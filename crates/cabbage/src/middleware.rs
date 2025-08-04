use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};

use futures::Future;
use futures::TryFutureExt as _;
use futures::stream::Stream;
use futures_util::StreamExt;
use lazy_static::lazy_static;
use redis_protocol::resp2::types::BytesFrame;
use tokio_util::bytes::Bytes;
use tower::Layer;
use tower::Service;
use uuid::Uuid;

lazy_static! {
    static ref DOC_REQUEST: BytesFrame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from_static(b"COMMAND")),
        BytesFrame::BulkString(Bytes::from_static(b"DOCS")),
    ]);
}

pub struct ProxyLoggerLayer<'conn> {
    connection_id: &'conn str,
}
impl<'conn> ProxyLoggerLayer<'conn> {
    pub fn new(connection_id: &'conn str) -> Self {
        Self { connection_id }
    }
}

impl<'conn, S> Layer<S> for ProxyLoggerLayer<'conn> {
    type Service = ProxyLogger<'conn, S>;

    fn layer(&self, service: S) -> Self::Service {
        ProxyLogger::new(service, self.connection_id)
    }
}

pub struct ProxyLogger<'conn, S> {
    resp2_service: S,
    connection_id: &'conn str,
    request_count: u64,
    response_count: Arc<AtomicU64>,
}

impl<'conn, S> ProxyLogger<'conn, S> {
    fn new(resp2_service: S, connection_id: &'conn str) -> Self {
        Self {
            resp2_service,
            connection_id,
            request_count: 0,
            response_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<'conn, S> Service<BytesFrame> for ProxyLogger<'conn, S>
where
    S: Service<BytesFrame>,
    S::Response: Stream<Item = BytesFrame> + Send + 'static,
    S::Error: Into<anyhow::Error> + 'static,
    S::Future: Send + 'static,
{
    type Response = Box<dyn Stream<Item = BytesFrame> + Send>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.resp2_service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: BytesFrame) -> Self::Future {
        self.request_count += 1;
        let req_num = self.request_count;
        let command_id = Uuid::new_v4();

        let is_doc_command = req == *DOC_REQUEST;
        log::info!(
            "Client -> Target: conn={} req#{} cmd={} - {:?}",
            self.connection_id,
            req_num,
            command_id,
            req
        );

        let fut = self.resp2_service.call(req);

        let conn_id = self.connection_id.to_string();
        let resp_count = self.response_count.clone();
        Box::pin(
            fut.map_ok(move |stream| {
                let logged = stream.inspect(move |frame| {
                    let n = resp_count.fetch_add(1, atomic::Ordering::Relaxed) + 1;

                    if is_doc_command {
                        log::info!(
                            "Target -> Client: conn={} resp#{} cmd={} - docs",
                            conn_id,
                            n,
                            command_id
                        );
                    } else {
                        log::info!(
                            "Target -> Client: conn={} resp#{} cmd={} - {:?}",
                            conn_id,
                            n,
                            command_id,
                            frame
                        );
                    }
                });

                Box::new(logged) as Self::Response
            })
            .map_err(Into::into),
        )
    }
}
