use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, RwLock};
use tokio::task::JoinHandle;

use crate::error::SocketError;

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
// How long we'll wait for the peer to actually send request bytes before
// giving up on the connection. A real fetch() sends its request the instant
// the TCP handshake completes, so a socket that's still silent after this
// long is almost certainly an unused speculative/preconnect socket rather
// than a real request, and we drop it instead of holding/counting it.
const REQUEST_GRACE_PERIOD: Duration = Duration::from_millis(750);
// A header line with no terminating blank line after it, so the response
// headers never finish: the browser's fetch() keeps waiting on the socket
// instead of resolving, while the periodic write still resets any idle
// timeout an intervening proxy or Chrome itself might apply.
const STATUS_LINE: &[u8] = b"HTTP/1.1 200 OK\r\n";
// Lets page JS read the response (status, headers, timing) across origins
// instead of getting an opaque result, so it can actually measure timing.
const CORS_HEADER: &[u8] = b"Access-Control-Allow-Origin: *\r\n";
// Without this, cross-origin PerformanceResourceTiming entries are clamped
// to a coarse start/end time only — this unlocks the detailed timestamps
// (responseStart, header/body sizes, etc).
const TIMING_HEADER: &[u8] = b"Timing-Allow-Origin: *\r\n";
const KEEPALIVE_HEADER: &[u8] = b"X-Keepalive: 1\r\n";
// Declares the connection non-reusable and completes the response headers
// (the blank line), sent only when we deliberately finish a held connection.
const END_HEADERS: &[u8] = b"Connection: close\r\n\r\n";

/// Pulls the request path out of a buffer holding (at least the start of)
/// an HTTP request, e.g. b"GET /time HTTP/1.1\r\n..." -> Some("/time").
fn request_path(buf: &[u8]) -> Option<&str> {
    let line_end = buf.iter().position(|&b| b == b'\n')?;
    let line = std::str::from_utf8(&buf[..line_end]).ok()?.trim_end_matches('\r');
    line.split_whitespace().nth(1)
}

struct Connection {
    handle: JoinHandle<()>,
    finish: oneshot::Sender<()>,
}

pub struct SockHandler{
    connections: Arc<RwLock<Vec<Connection>>>,
    soc_recv_handle: JoinHandle<()>
}

impl SockHandler{
    pub async fn new(host: String, port: u16) -> Result<Self, SocketError>{
        let connections = Arc::new(RwLock::new(vec![]));
        let connections_copy = connections.clone();
        let addr_str = host + ":" + &port.to_string();
        let listener = TcpListener::bind(addr_str).await?;
        println!("listening...");
        let soc_recv_handle = tokio::spawn(async move{
            loop {
                let res = listener.accept().await;
                if let Ok((conn, _)) = res{
                    println!("received connection!");
                    let (finish_tx, finish_rx) = oneshot::channel();
                    let handle = tokio::spawn(Self::hold_connection(conn, finish_rx));
                    connections_copy.write().await.push(Connection { handle, finish: finish_tx });
                }
            }
        });
        Ok(SockHandler { connections, soc_recv_handle })
    }

    /// Holds a connection open indefinitely: writes a status line and then
    /// trickles a harmless header line every `KEEPALIVE_INTERVAL`, never
    /// sending the blank line that would complete the response. Concurrently
    /// drains and discards whatever the peer sends, so the kernel receive
    /// buffer never has unread data sitting in it.
    ///
    /// Ends either when the peer goes away, or when `finish` fires: `close`
    /// uses that to ask for a graceful end instead of yanking the socket
    /// out from under an in-flight response. Writing the blank line that
    /// completes the headers and then shutting down cleanly gives the
    /// browser a normal, fully-finished 200 response (connection-close
    /// framed, empty body) rather than a reset mid-response — Chrome treats
    /// a reset on a pending idempotent request as a retryable network error
    /// and silently reopens a replacement connection, which is what was
    /// making the open-connection count jump around on close.
    ///
    /// Exception: a request for `/time` is answered immediately with the
    /// current server time and closed normally — a probe endpoint that
    /// doesn't hang, for use with the one spare slot left after filling
    /// the pool.
    async fn hold_connection(conn: TcpStream, mut finish: oneshot::Receiver<()>) {
        let (mut reader, mut writer) = conn.into_split();
        let mut buf = [0u8; 1024];

        let n = match tokio::time::timeout(REQUEST_GRACE_PERIOD, reader.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => n,
            _ => {
                // Either the peer never sent anything (timed out) or it
                // closed before sending a request — not a real fetch().
                return;
            }
        };

        if request_path(&buf[..n]) == Some("/time") {
            let _ = Self::write_time_response(&mut writer).await;
            let _ = writer.shutdown().await;
            return;
        }

        if writer.write_all(STATUS_LINE).await.is_err() {
            return;
        }
        if writer.write_all(CORS_HEADER).await.is_err() {
            return;
        }
        if writer.write_all(TIMING_HEADER).await.is_err() {
            return;
        }

        let mut ticker = tokio::time::interval(KEEPALIVE_INTERVAL);

        loop {
            tokio::select! {
                read_result = reader.read(&mut buf) => {
                    match read_result {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                }
                _ = ticker.tick() => {
                    if writer.write_all(KEEPALIVE_HEADER).await.is_err() {
                        return;
                    }
                }
                _ = &mut finish => {
                    let _ = writer.write_all(END_HEADERS).await;
                    let _ = writer.shutdown().await;
                    return;
                }
            }
        }
    }

    /// Writes a complete, immediate 200 response carrying the current
    /// server time in milliseconds since the Unix epoch.
    async fn write_time_response(writer: &mut tokio::net::tcp::OwnedWriteHalf) -> std::io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let body = now.as_millis().to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nTiming-Allow-Origin: *\r\nConnection: close\r\n\r\n",
            body.len()
        );
        writer.write_all(head.as_bytes()).await?;
        writer.write_all(body.as_bytes()).await
    }

    /// Closes up to `count` currently open socket connections. Returns the number actually closed.
    pub async fn close(&self, count: usize) -> usize {
        let mut connections = self.connections.write().await;
        connections.retain(|c| !c.handle.is_finished());
        let n = count.min(connections.len());
        for conn in connections.drain(0..n) {
            let _ = conn.finish.send(());
        }
        n
    }

    pub async fn len(&self) -> usize {
        let mut connections = self.connections.write().await;
        connections.retain(|c| !c.handle.is_finished());
        connections.len()
    }
}

impl Drop for SockHandler{
    fn drop(&mut self) {
        self.soc_recv_handle.abort();
    }
}
