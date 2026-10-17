#[derive(thiserror::Error, Debug)]
pub enum SocketError{
    #[error("Failed to bind to address {0}")]
    Bind(#[from] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum WsError{
    #[error("Failed to bind to address {0}")]
    Bind(std::io::Error),
    #[error("Websocket handshake failed: {0}")]
    Handshake(#[from] tokio_tungstenite::tungstenite::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum DnsError{
    #[error("Failed to bind to address {0}")]
    Bind(#[from] std::io::Error),
    #[error("DNS server error: {0}")]
    Server(#[from] hickory_server::net::NetError),
}

#[derive(thiserror::Error, Debug)]
pub enum Error{
    #[error("{0}")]
    Sock(#[from] SocketError),
    #[error("{0}")]
    Ws(#[from] WsError),
    #[error("{0}")]
    Dns(#[from] DnsError),
}