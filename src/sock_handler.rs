use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{ RwLock};
use tokio::task::JoinHandle;
use std::sync::Arc;

use crate::error::SocketError;
pub struct SockHandler{
    sockets: Arc<RwLock<Vec<TcpStream>>>,
    soc_recv_handle: JoinHandle<()>
}

impl SockHandler{
    pub async fn new(host: String, port: u16) -> Result<Self, SocketError>{
        let sockets = Arc::new(RwLock::new(vec![]));
        let socks_copy = sockets.clone();
        let addr_str = host + ":" + &port.to_string();
        let listener = TcpListener::bind(addr_str).await?;
        println!("listening...");
        let soc_recv_handle = tokio::spawn(async move{
            loop {
                let res = listener.accept().await;
                if let Ok((conn, _)) = res{
                    println!("received connection!");
                    let mut socks_lock = socks_copy.write().await;
                    socks_lock.push(conn);
                }
            }
        });
        Ok(SockHandler { sockets, soc_recv_handle })
    }

    /// Closes up to `count` currently open socket connections. Returns the number actually closed.
    pub async fn close(&self, count: usize) -> usize {
        let mut sockets = self.sockets.write().await;
        let n = count.min(sockets.len());
        sockets.drain(0..n);
        n
    }

    pub async fn len(&self) -> usize {
        self.sockets.read().await.len()
    }
}

impl Drop for SockHandler{
    fn drop(&mut self) {
        self.soc_recv_handle.abort();
    }
}