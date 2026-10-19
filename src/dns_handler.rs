use std::println;
use std::time::Duration;

use async_trait::async_trait;
use hickory_proto::op::{Header, HeaderCounts, Metadata, ResponseCode};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{RData, Record, RecordType};
use hickory_server::Server;
use hickory_server::net::runtime::Time;
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use hickory_server::zone_handler::MessageResponseBuilder;
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use crate::error::DnsError;

pub struct DnsHandler {
    serve_handle: JoinHandle<()>,
}

impl DnsHandler {
    pub async fn new(
        bind_addr: &str,
        listen_suffix: String,
        response_ip: [u8; 4],
    ) -> Result<Self, DnsError> {
        let request_handler = DnsRequestHandler {
            listen_suffix,
            response_ip,
        };
        let mut server = Server::new(request_handler);

        let udp = UdpSocket::bind(bind_addr).await?;
        server.register_socket(udp);

        let tcp = TcpListener::bind(bind_addr).await?;
        server.register_listener(tcp, Duration::from_secs(5), 1024);

        let serve_handle = tokio::spawn(async move {
            let _ = server.block_until_done().await;
        });

        Ok(Self { serve_handle })
    }
}

impl Drop for DnsHandler {
    fn drop(&mut self) {
        self.serve_handle.abort();
    }
}

struct DnsRequestHandler {
    listen_suffix: String,
    response_ip: [u8; 4],
}

impl DnsRequestHandler {
    fn serv_fail(request: &Request) -> ResponseInfo {
        let mut metadata = Metadata::response_from_request(&request.metadata);
        metadata.response_code = ResponseCode::ServFail;
        Header {
            metadata,
            counts: HeaderCounts::default(),
        }
        .into()
    }
}

#[async_trait]
impl RequestHandler for DnsRequestHandler {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        let builder = MessageResponseBuilder::from_message_request(request);

        let query = match request.request_info() {
            Ok(info) => info.query,
            Err(_) => {
                let response = builder.error_msg(&request.metadata, ResponseCode::FormErr);
                return response_handle
                    .send_response(response)
                    .await
                    .unwrap_or_else(|_| Self::serv_fail(request));
            }
        };

        let name = query.name().to_string();
        let record_type = query.query_type();

        // Serve the configured response IP for any query matching our listen suffix
        println!("{}", name);
        let suffix = String::from(self.listen_suffix.clone() + ".");
        if record_type == RecordType::A && name.ends_with(&suffix) {
            let mut metadata = Metadata::response_from_request(&request.metadata);
            metadata.response_code = ResponseCode::NoError;

            let [a, b, c, d] = self.response_ip;
            let record = Record::from_rdata(
                query.original().name().clone(),
                300, // TTL in seconds
                RData::A(A::new(a, b, c, d)),
            );

            let answers = vec![record];
            let empty: &[Record] = &[];
            let response = builder.build(metadata, &answers, empty, empty, empty);

            response_handle
                .send_response(response)
                .await
                .unwrap_or_else(|_| Self::serv_fail(request))
        } else {
            // Return NXDomain (Not Found) for anything outside our listen suffix
            let response = builder.error_msg(&request.metadata, ResponseCode::NXDomain);
            response_handle
                .send_response(response)
                .await
                .unwrap_or_else(|_| Self::serv_fail(request))
        }
    }
}
