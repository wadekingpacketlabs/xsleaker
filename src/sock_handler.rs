use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::error::SocketError;

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
// A header line with no terminating blank line after it, so the response
// headers never finish: the browser's fetch() keeps waiting on the socket
// instead of resolving, while the periodic write still resets any idle
// timeout an intervening proxy or Chrome itself might apply.
const STATUS_LINE: &[u8] = b"HTTP/1.1 200 OK\r\n";
const KEEPALIVE_HEADER: &[u8] = b"X-Keepalive: 1\r\n";

pub struct SockHandler{
    connections: Arc<RwLock<Vec<JoinHandle<()>>>>,
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
                    let handle = tokio::spawn(Self::hold_connection(conn));
                    connections_copy.write().await.push(handle);
                }
            }
        });
        Ok(SockHandler { connections, soc_recv_handle })
    }

    /// Holds a connection open indefinitely: writes a status line and then
    /// trickles a harmless header line every `KEEPALIVE_INTERVAL`, never
    /// sending the blank line that would complete the response. Returns
    /// (letting the socket close) only once the peer goes away or this task
    /// is aborted by `close`.
    async fn hold_connection(mut conn: TcpStream) {
        if conn.write_all(STATUS_LINE).await.is_err() {
            return;
        }

        let mut ticker = tokio::time::interval(KEEPALIVE_INTERVAL);
        loop {
            ticker.tick().await;
            if conn.write_all(KEEPALIVE_HEADER).await.is_err() {
                return;
            }
        }
    }

    /// Closes up to `count` currently open socket connections. Returns the number actually closed.
    pub async fn close(&self, count: usize) -> usize {
        let mut connections = self.connections.write().await;
        connections.retain(|handle| !handle.is_finished());
        let n = count.min(connections.len());
        for handle in connections.drain(0..n) {
            handle.abort();
        }
        n
    }

    pub async fn len(&self) -> usize {
        let mut connections = self.connections.write().await;
        connections.retain(|handle| !handle.is_finished());
        connections.len()
    }
}

impl Drop for SockHandler{
    fn drop(&mut self) {
        self.soc_recv_handle.abort();
    }
}