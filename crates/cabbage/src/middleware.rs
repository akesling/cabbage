use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};

use futures::Future;
use futures::stream::Stream;
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

impl<'conn, S, St> Service<BytesFrame> for ProxyLogger<'conn, S>
where
    S: Service<BytesFrame, Response = St>,
    St: Stream<Item = BytesFrame> + Send + Unpin + 'static,
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
            resp_count.fetch_add(1, atomic::Ordering::Relaxed);
            let response_num = resp_count.load(atomic::Ordering::Relaxed);
            let base_stream = fut.await.map_err(Into::into)?;

            // Create a wrapper struct that implements Stream
            struct LoggingStream<S> {
                inner: S,
                is_doc: bool,
                connection_id: String,
                response_num: u64,
                command_id_str: String,
            }

            impl<S: Stream<Item = BytesFrame> + Unpin> Stream for LoggingStream<S> {
                type Item = BytesFrame;

                fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                    match Pin::new(&mut self.inner).poll_next(cx) {
                        Poll::Ready(Some(resp)) => {
                            if self.is_doc {
                                log::info!(
                                    concat!(
                                        "Target -> Client: ",
                                        "conn={} response #{} (last command #{}) - documents"
                                    ),
                                    self.connection_id,
                                    self.response_num,
                                    self.command_id_str
                                );
                            } else {
                                log::info!(
                                    concat!(
                                        "Target -> Client: ",
                                        "conn={} response #{} (last command #{}) - {:?}"
                                    ),
                                    self.connection_id,
                                    self.response_num,
                                    self.command_id_str,
                                    resp
                                );
                            }
                            Poll::Ready(Some(resp))
                        }
                        other => other,
                    }
                }
            }

            let logging_stream = LoggingStream {
                inner: base_stream,
                is_doc: is_doc_command,
                connection_id,
                response_num,
                command_id_str,
            };

            Ok(Box::new(logging_stream) as Box<dyn Stream<Item = BytesFrame> + Send + Unpin>)
        })
    }
}
