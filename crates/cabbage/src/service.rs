use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use anyhow::bail;
use futures::Future;
use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::Framed;
use tower::Service;

static MAX_OUTSTANDING_RESPONSE_STREAM_MESSAGES: usize = 100;
static MAX_OUTSTANDING_REQUEST_MESSAGES: usize = 100;

struct RequestMessage {
    frame: BytesFrame,
    response_sender: mpsc::Sender<BytesFrame>,
}

struct CloseMessage {
    conn_sender: tokio::sync::oneshot::Sender<Framed<TcpStream, Resp2>>,
}

enum Message {
    Request(RequestMessage),
    #[allow(dead_code)]
    Close(CloseMessage),
}

pub struct Resp2Backend {
    request_sender: mpsc::Sender<Message>,

    backend_running: Arc<AtomicBool>,
}

impl Resp2Backend {
    pub fn new(target_framed: Framed<TcpStream, Resp2>) -> Self {
        let (request_sender, request_receiver) =
            mpsc::channel::<Message>(MAX_OUTSTANDING_REQUEST_MESSAGES);

        // Would like to initilize as false. Initilized to true to avoid race condition when starting a connection.
        let backend_running = Arc::new(AtomicBool::new(true));

        tokio::spawn(backend_task(
            target_framed,
            request_receiver,
            backend_running.clone(),
        ));

        Self {
            request_sender,
            backend_running,
        }
    }
}

impl Service<BytesFrame> for Resp2Backend {
    type Response = Box<dyn Stream<Item = BytesFrame> + Unpin + Send>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.backend_running.load(Ordering::Relaxed) {
            return Poll::Ready(Err(anyhow::anyhow!("Backend task has died")));
        }

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BytesFrame) -> Self::Future {
        let request_sender = self.request_sender.clone();

        let fut = async move {
            let (response_sender, response_receiver) =
                mpsc::channel(MAX_OUTSTANDING_RESPONSE_STREAM_MESSAGES);

            let request = RequestMessage {
                frame: req,
                response_sender,
            };
            if let Err(e) = request_sender.send(Message::Request(request)).await {
                bail!("Failed to send request to handler: {}", e);
            }

            Ok(Box::new(ReceiverStream::new(response_receiver)) as Self::Response)
        };

        Box::pin(fut)
    }
}

async fn backend_task(
    target_framed: Framed<TcpStream, Resp2>,
    mut request_receiver: mpsc::Receiver<Message>,
    backend_running: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let (mut sender, mut receiver) = target_framed.split();
    let mut current_response_sender: Option<mpsc::Sender<BytesFrame>> = None;

    let mut response_next = Box::pin(receiver.next());
    let mut close_sender: Option<tokio::sync::oneshot::Sender<Framed<TcpStream, Resp2>>> = None;
    backend_running.store(true, Ordering::Relaxed);

    loop {
        tokio::select! {
            request = request_receiver.recv() => {
                match request {
                    Some(Message::Request(RequestMessage { frame, response_sender })) => {
                        current_response_sender = Some(response_sender);
                        if let Err(e) = sender.send(frame).await {
                            log::error!("Failed to send request to target: {}", e);
                            break;
                        }
                    }
                    Some(Message::Close(CloseMessage { conn_sender })) => {
                        close_sender = Some(conn_sender);
                        break;
                    }
                    None => {
                        log::info!("Request channel closed, shutting down connection handler");
                        break;
                    }
                }
            }
            response = &mut response_next => {
                match response {
                    Some(Ok(frame)) => {
                        if let Some(ref sender) = current_response_sender {
                            if sender.send(frame).await.is_err() {
                                drop(current_response_sender.take());
                            }
                        } else {
                            log::error!(
                                "Response received without a known request to associate: {:?}",
                                frame);
                        }

                        response_next = Box::pin(receiver.next());
                    }
                    Some(Err(e)) => {
                        log::error!("Error reading response from target: {}", e);
                        break;
                    }
                    None => {
                        log::info!("Target connection closed");
                        break;
                    }
                }
            }
        }
    }

    backend_running.store(false, Ordering::Relaxed);

    if let Some(conn_sender) = close_sender {
        let framed = sender.reunite(receiver)?;
        if conn_sender.send(framed).is_err() {
            bail!(concat!(
                "Failed to send owned Framed back on receipt of ",
                "close message in backend task"
            ))
        }
    }
    Ok(())
}
