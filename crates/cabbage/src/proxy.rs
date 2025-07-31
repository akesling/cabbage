use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::bail;
use futures::Future;
use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio_util::{bytes::Bytes, codec::Framed};
use tower::Service;
use uuid::Uuid;

lazy_static! {
    static ref DOC_REQUEST: BytesFrame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from_static(b"COMMAND")),
        BytesFrame::BulkString(Bytes::from_static(b"DOCS")),
    ]);
}

pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
    connection_id: Uuid,
) -> anyhow::Result<()> {
    // Connect to target server
    let target_socket = TcpStream::connect(&target_addr).await?;
    log::info!(
        "connection {connection_id}: connected with target at: {}",
        target_addr
    );

    let client_framed = Framed::new(client_socket, Resp2::default());
    let target_framed = Framed::new(target_socket, Resp2::default());

    let (client_sink, client_stream) = client_framed.split();
    let target_service = Resp2Service::new(target_framed);

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let command_count = Arc::new(AtomicU64::new(0));
    let response_count = Arc::new(AtomicU64::new(0));

    // Create a channel for response frames to be sent to the client
    let (response_tx, response_rx) = mpsc::channel::<BytesFrame>(100);

    // Spawn response forwarding task
    let forward_task = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::ReceiverStream::new(response_rx);
        stream.map(Ok).forward(client_sink).await
    });

    // Process client commands and route responses
    let cmd_count = command_count.clone();
    let resp_count = response_count.clone();
    let target_service = Arc::new(Mutex::new(target_service));
    let response_tx_for_drop = response_tx.clone();

    client_stream
        .for_each_concurrent(None, move |frame_result| {
            let cmd_count = cmd_count.clone();
            let resp_count = resp_count.clone();
            let response_tx = response_tx.clone();
            let target_service = target_service.clone();
            async move {
                match frame_result {
                    Ok(frame) => {
                        cmd_count.fetch_add(1, Ordering::Relaxed);
                        let command_id = Uuid::new_v4();
                        let command_id_str = command_id.to_string();

                        log::info!(
                            concat!(
                                "Client -> Target: ",
                                "conn={} command={} - {:?}"),
                            connection_id, command_id_str, frame);

                        let is_doc_command = frame == *DOC_REQUEST;

                        match target_service.lock().await.call(frame).await {
                            Ok(mut response_stream) => {
                                // Spawn a task to handle this response stream
                                tokio::spawn(async move {
                                    while let Some(response_frame) = response_stream.next().await {
                                        resp_count.fetch_add(1, Ordering::Relaxed);
                                        let response_num = resp_count.load(Ordering::Relaxed);

                                        if is_doc_command {
                                            log::info!(
                                                concat!(
                                                    "Target -> Client: ",
                                                    "conn={} response #{} (last command #{}) - documents"),
                                                connection_id, response_num, command_id_str);
                                        } else {
                                            log::info!(
                                                concat!(
                                                    "Target -> Client: ",
                                                    "conn={} response #{} (last command #{}) - {:?}"),
                                                connection_id, response_num, command_id_str, response_frame);
                                        }

                                        if response_tx.send(response_frame).await.is_err() {
                                            log::error!("Failed to send response to client channel");
                                            break;
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                log::error!("Failed to send command to backend: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading from client: {}", e);
                    }
                }
            }
        })
        .await;

    // Wait for the forward task to complete
    drop(response_tx_for_drop); // Close the channel to signal completion
    if let Err(e) = forward_task.await {
        log::error!("Forward task failed: {}", e);
    }

    log::info!("Connection closed");
    Ok(())
}

struct RequestMessage {
    frame: BytesFrame,
    response_sender: mpsc::Sender<BytesFrame>,
}

pub struct Resp2Service {
    request_sender: mpsc::Sender<Message>,
}

impl Resp2Service {
    pub fn new(target_framed: Framed<TcpStream, Resp2>) -> Self {
        let (request_sender, request_receiver) = mpsc::channel::<Message>(100);

        // Spawn background task that owns the connection
        tokio::spawn(backend_task(target_framed, request_receiver));

        Self { request_sender }
    }
}

struct CloseMessage {
    conn_sender: tokio::sync::oneshot::Sender<Framed<TcpStream, Resp2>>,
}

enum Message {
    Request(RequestMessage),
    #[allow(dead_code)]
    Close(CloseMessage),
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

impl Service<BytesFrame> for Resp2Service {
    type Response = FrameStream;
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

            Ok(responses)
        };

        Box::pin(fut)
    }
}

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
