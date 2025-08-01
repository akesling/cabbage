use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::bail;
use futures::Future;
use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tower::Service;

pub struct FrameStream {
    receiver: mpsc::Receiver<BytesFrame>,
}

impl FrameStream {
    fn new(buffer_size: usize) -> (mpsc::Sender<BytesFrame>, Self) {
        let (sender, receiver) = mpsc::channel(buffer_size);
        (sender, Self { receiver })
    }
}

impl Stream for FrameStream {
    type Item = BytesFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

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
}

impl Resp2Backend {
    pub fn new(target_framed: Framed<TcpStream, Resp2>) -> Self {
        let (request_sender, request_receiver) = mpsc::channel::<Message>(100);

        // Spawn background task that owns the connection
        tokio::spawn(backend_task(target_framed, request_receiver));

        Self { request_sender }
    }
}

impl Service<BytesFrame> for Resp2Backend {
    type Response = Box<dyn Stream<Item = BytesFrame> + Send + Unpin>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BytesFrame) -> Self::Future {
        let request_sender = self.request_sender.clone();

        let fut = async move {
            let (response_sender, responses) = FrameStream::new(100);

            let request = RequestMessage {
                frame: req,
                response_sender,
            };
            if let Err(e) = request_sender.send(Message::Request(request)).await {
                bail!("Failed to send request to handler: {}", e);
            }

            let r: Self::Response = Box::new(responses);
            Ok(r)
        };

        Box::pin(fut)
    }
}

async fn backend_task(
    target_framed: Framed<TcpStream, Resp2>,
    mut request_receiver: mpsc::Receiver<Message>,
) -> anyhow::Result<()> {
    let (mut sink, mut stream) = target_framed.split();
    let mut current_response_sender: Option<mpsc::Sender<BytesFrame>> = None;

    let mut response_next = Box::pin(stream.next());
    let mut close_sender: Option<tokio::sync::oneshot::Sender<Framed<TcpStream, Resp2>>> = None;
    loop {
        tokio::select! {
            // Handle new requests
            request = request_receiver.recv() => {
                match request {
                    Some(Message::Request(RequestMessage { frame, response_sender })) => {
                        // Update current response sender for this request
                        current_response_sender = Some(response_sender);

                        // Send request to target
                        if let Err(e) = sink.send(frame).await {
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

            // Handle responses from target
            response = &mut response_next => {
                match response {
                    Some(Ok(frame)) => {
                        if let Some(ref sender) = current_response_sender {
                            if sender.send(frame).await.is_err() {
                                // Response channel closed, stop sending responses for this request
                                drop(current_response_sender.take());
                            }
                        } else {
                            log::error!(
                                "Response received without a known request to associate: {:?}",
                                frame);
                        }

                        response_next = Box::pin(stream.next());
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

    if let Some(conn_sender) = close_sender {
        let framed = sink.reunite(stream)?;
        if conn_sender.send(framed).is_err() {
            bail!(concat!(
                "Failed to send owned Framed back on receipt of ",
                "close message in backend task"
            ))
        }
    }
    Ok(())
}
