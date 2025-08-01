use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};

use futures::Future;
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

impl<'conn, S> Layer<S> for ProxyLoggerLayer<'conn>
where
    S: Service<BytesFrame, Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>>,
{
    type Service = ProxyLogger<'conn, S>;

    fn layer(&self, service: S) -> Self::Service {
        ProxyLogger::new(service, self.connection_id)
    }
}

pub struct ProxyLogger<
    'conn,
    S: Service<BytesFrame, Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>>,
> {
    resp2_service: S,
    connection_id: &'conn str,
    request_count: u64,
    response_count: Arc<AtomicU64>,
}

impl<'conn, S> ProxyLogger<'conn, S>
where
    S: Service<BytesFrame, Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>>,
{
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
    S: Service<BytesFrame, Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>>,
    S::Error: Into<anyhow::Error>,
    S::Future: Send + 'static,
{
    type Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.resp2_service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: BytesFrame) -> Self::Future {
        self.request_count += 1;
        let is_doc_command = req == *DOC_REQUEST;
        let command_id = Uuid::new_v4();
        let command_id_str = command_id.to_string();
        let connection_id = self.connection_id.to_string();

        log::info!(
            concat!("Client -> Target: ", "conn={} command={} - {:?}"),
            connection_id,
            command_id_str,
            req
        );

        let fut = self.resp2_service.call(req);

        let resp_count = self.response_count.clone();
        Box::pin(async move {
            let response_num = resp_count.load(atomic::Ordering::Relaxed);
            let base_stream = fut.await.map_err(Into::into)?;

            // Use Box::pin to make the mapped stream Unpin
            let mapped_stream = base_stream.map(move |resp| {
                if is_doc_command {
                    log::info!(
                        concat!(
                            "Target -> Client: ",
                            "conn={} response #{} (last command #{}) - documents"
                        ),
                        connection_id,
                        response_num,
                        command_id_str
                    );
                } else {
                    log::info!(
                        concat!(
                            "Target -> Client: ",
                            "conn={} response #{} (last command #{}) - {:?}"
                        ),
                        connection_id,
                        response_num,
                        command_id_str,
                        resp
                    );
                }
                resp
            });

            let resp_stream: Box<dyn Stream<Item = BytesFrame> + Send + Unpin> =
                Box::new(Box::pin(mapped_stream));
            Ok(resp_stream)
        })
    }
}
