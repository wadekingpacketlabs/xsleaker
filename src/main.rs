use std::println;
use std::process::exit;
use std::sync::Arc;

use clap::Parser;
use tokio::signal;

use crate::dns_handler::DnsHandler;
use crate::error::{Error};
use crate::sock_handler::SockHandler;
use crate::ws_handler::WsHandler;
mod error;
mod jsonrpc;
mod sock_handler;
mod ws_handler;
mod dns_handler;

/// Simple program to greet a person
#[derive(Parser, Debug)]
struct Args {
    /// listening interface for incoming http connections
    #[arg(short, long, default_value_t = String::from("0.0.0.0"))]
    interface: String,

    /// listening port for incoming http connections
    #[arg(short, long, default_value_t = 8000)]
    port: u16,

    /// listening port for incoming websocket control connections
    #[arg(short = 'w', long, default_value_t = 8001)]
    ws_port: u16,

    /// listening port for incoming DNS connections
    #[arg(short, long, default_value_t = 5354)]
    dns_port: u16,

    /// resolved IP address for DNS requests
    #[arg(short, long, default_value_t = String::from("127.0.0.1"))]
    resolved_address: String,

    /// Suffix to listen for incoming DNS requests
    #[arg(short, long, default_value_t = String::from(""))]
    suffix_dns: String
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let dns_addr = args.resolved_address.split(".").map(|seg|{seg.parse::<u8>().unwrap_or(0)}).collect::<Vec<u8>>();
    if dns_addr.len() < 4{
        println!("Invalid DNS address {}", args.resolved_address);
        exit(1);
    }
    let dns_bind_addr = args.interface.clone() + ":" + &args.dns_port.to_string();
    let dns_handler = DnsHandler::new(&dns_bind_addr, args.suffix_dns, dns_addr.try_into().unwrap()).await?;
    let sock_handler = Arc::new(SockHandler::new(args.interface.clone(), args.port).await?);
    let ws_handler = WsHandler::new(args.interface.clone(), args.ws_port, sock_handler.clone()).await?;

    signal::ctrl_c().await.ok();
    Ok(())
}
