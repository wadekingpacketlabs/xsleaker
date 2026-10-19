use std::collections::HashSet;
use std::net::SocketAddr;
use std::println;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::WebSocketStream;

use crate::error::WsError;
use crate::jsonrpc::{self, Id, Request, Response, RpcError};
use crate::sock_handler::SockHandler;

type WsSink = Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>;
type WsSource = SplitStream<WebSocketStream<TcpStream>>;
type PendingRequests = Arc<RwLock<HashSet<Id>>>;

pub struct WsHandler {
    accept_handle: JoinHandle<()>,
}

impl WsHandler {
    pub async fn new(host: String, port: u16, sock_handler: Arc<SockHandler>) -> Result<Self, WsError> {
        let addr_str = host + ":" + &port.to_string();
        let listener = TcpListener::bind(&addr_str).await.map_err(WsError::Bind)?;
        println!("websocket listener bound to {addr_str}...");

        let next_id = Arc::new(AtomicU64::new(0));

        let accept_handle = tokio::spawn(async move {
            loop {
                let (tcp_stream, addr) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        eprintln!("failed to accept tcp connection: {e}");
                        continue;
                    }
                };

                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let sock_handler = sock_handler.clone();

                tokio::spawn(async move {
                    let ws_stream = match accept_async(tcp_stream).await {
                        Ok(ws) => ws,
                        Err(e) => {
                            eprintln!("websocket handshake with {addr} failed: {e}");
                            return;
                        }
                    };

                    println!("websocket connection established with {addr}");

                    let (sink, source) = ws_stream.split();

                    let connection = Arc::new(ClientConnection {
                        id,
                        addr,
                        sink: Arc::new(Mutex::new(sink)),
                        pending: Arc::new(RwLock::new(HashSet::new())),
                        sock_handler,
                    });

                    connection.run(source).await;
                });
            }
        });

        Ok(WsHandler { accept_handle })
    }
}

impl Drop for WsHandler {
    fn drop(&mut self) {
        self.accept_handle.abort();
    }
}

struct ClientConnection {
    #[allow(dead_code)]
    id: u64,
    addr: SocketAddr,
    sink: WsSink,
    pending: PendingRequests,
    sock_handler: Arc<SockHandler>,
}

impl ClientConnection {
    async fn run(self: Arc<Self>, mut source: WsSource) {
        loop {
            let msg = source.next().await;

            match msg {
                Some(Ok(Message::Text(text))) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_request(text).await;
                    });
                }
                Some(Ok(Message::Ping(payload))) => {
                    if let Err(e) = self.sink.lock().await.send(Message::Pong(payload)).await {
                        eprintln!("failed to send pong to {}: {e}", self.addr);
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => {
                    println!("{} closed the connection", self.addr);
                    break;
                }
                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                Some(Err(e)) => {
                    eprintln!("error reading from {}: {e}", self.addr);
                    break;
                }
                None => {
                    println!("{} disconnected", self.addr);
                    break;
                }
            }
        }
    }

    async fn handle_request(&self, text: Utf8Bytes) {
        let request: Request = match serde_json::from_str(text.as_str()) {
            Ok(request) => request,
            Err(e) => {
                eprintln!("invalid json-rpc request from {}: {e}", self.addr);
                self.send_response(Response::error(None, jsonrpc::PARSE_ERROR, "Parse error")).await;
                return;
            }
        };

        if !self.pending.write().await.insert(request.id.clone()) {
            eprintln!("duplicate in-flight request id from {}: {:?}", self.addr, request.id);
            self.send_response(Response::error(
                Some(request.id),
                jsonrpc::INVALID_REQUEST,
                "request id already in flight",
            ))
            .await;
            return;
        }

        let response = match self.dispatch(&request.method, request.params).await {
            Ok(result) => Response::success(request.id.clone(), result),
            Err(err) => Response::error(Some(request.id.clone()), err.code, err.message),
        };

        self.pending.write().await.remove(&request.id);
        self.send_response(response).await;
    }

    async fn dispatch(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        match method {
            "closeSockets" => self.close_sockets(params).await,
            "connectionCount" => self.connection_count().await,
            _ => Err(RpcError { code: jsonrpc::METHOD_NOT_FOUND, message: format!("method not found: {method}") }),
        }
    }

    async fn close_sockets(&self, params: Value) -> Result<Value, RpcError> {
        #[derive(Deserialize)]
        struct CloseSocketsParams {
            count: usize,
        }

        let params: CloseSocketsParams = serde_json::from_value(params).map_err(|e| RpcError {
            code: jsonrpc::INVALID_PARAMS,
            message: format!("invalid params: {e}"),
        })?;

        let closed = self.sock_handler.close(params.count).await;
        println!("connection closed");
        let remaining = self.sock_handler.len().await;

        Ok(serde_json::json!({ "closed": closed, "remaining": remaining }))
    }

    async fn connection_count(&self) -> Result<Value, RpcError> {
        let count = self.sock_handler.len().await;
        Ok(serde_json::json!({ "count": count }))
    }

    async fn send_response(&self, response: Response) {
        let text = match serde_json::to_string(&response) {
            Ok(text) => text,
            Err(e) => {
                eprintln!("failed to serialize json-rpc response: {e}");
                return;
            }
        };

        if let Err(e) = self.sink.lock().await.send(Message::text(text)).await {
            eprintln!("failed to send json-rpc response: {e}");
        }
    }
}
