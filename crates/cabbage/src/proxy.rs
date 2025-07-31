use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::bail;
use futures::Future;
use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
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

    let (mut client_sink, mut client_stream) = client_framed.split();
    let (mut target_sink, mut target_stream) = target_framed.split();

    let mut _command_count = 0u64;
    let mut last_command: Option<Uuid> = None;
    let mut response_count = 0u64;

    let mut client_next = Box::pin(client_stream.next());
    let mut target_next = Box::pin(target_stream.next());

    let mut is_doc_command = false;

    loop {
        tokio::select! {
            // Handle frames from client -> target
            frame_result = &mut client_next => {
                match frame_result {
                    Some(Ok(frame)) => {
                        _command_count += 1;
                        last_command = Some(Uuid::new_v4());

                        let command_id_str = last_command.map(
                            |u| u.to_string()).unwrap_or("UNKNOWN".to_string());
                        log::info!(
                            concat!(
                                "Client -> Target: ",
                                "conn={} command={} - {:?}"),
                            connection_id, command_id_str, frame);

                        is_doc_command = frame == *DOC_REQUEST;

                        if let Err(e) = target_sink.send(frame).await {
                            bail!("Failed to send command to target: {}", e);
                        }

                        client_next = Box::pin(client_stream.next());
                    }
                    Some(Err(e)) => {
                        bail!("Error reading from client: {}", e);
                    }
                    None => {
                        log::info!("Client connection closed");
                        break;
                    }
                }
            }

            // Handle frames from target -> client
            frame_result = &mut target_next => {
                match frame_result {
                    Some(Ok(frame)) => {
                        response_count += 1;
                        let command_id_str = last_command.map(
                            |u| u.to_string()).unwrap_or("UNKNOWN".to_string());

                        if is_doc_command {
                            log::info!(
                                concat!(
                                    "Target -> Client: ",
                                    "conn={} response #{} (last command #{}) - documents"),
                                connection_id, response_count, command_id_str);
                        } else {
                            log::info!(
                                concat!(
                                    "Target -> Client: ",
                                    "conn={} response #{} (last command #{}) - {:?}"),
                                connection_id, response_count, command_id_str, frame);
                        }

                        if let Err(e) = client_sink.send(frame).await {
                            bail!("Failed to send response to client: {}", e);
                        }
                        target_next = Box::pin(target_stream.next());
                    }
                    Some(Err(e)) => {
                        bail!("Error reading from target: {}", e);
                    }
                    None => {
                        log::info!("Target connection closed");
                        break;
                    }
                }
            }
        }
    }

    log::info!("Connection closed");
    Ok(())
}

struct RequestMessage {
    frame: BytesFrame,
    response_sender: mpsc::Sender<BytesFrame>,
}

pub struct Resp2ProxyService {
    request_sender: mpsc::Sender<ProxyMessage>,
}

impl Resp2ProxyService {
    pub fn new(target_framed: Framed<TcpStream, Resp2>) -> Self {
        let (request_sender, request_receiver) = mpsc::channel::<ProxyMessage>(100);

        // Spawn background task that owns the connection
        tokio::spawn(proxy_task(target_framed, request_receiver));

        Self { request_sender }
    }
}

struct ProxyCloseMessage {
    conn_sender: tokio::sync::oneshot::Sender<Framed<TcpStream, Resp2>>,
}

enum ProxyMessage {
    Request(RequestMessage),
    #[allow(dead_code)]
    Close(ProxyCloseMessage),
}

async fn proxy_task(
    target_framed: Framed<TcpStream, Resp2>,
    mut request_receiver: mpsc::Receiver<ProxyMessage>,
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
                    Some(ProxyMessage::Request(RequestMessage { frame, response_sender })) => {
                        // Update current response sender for this request
                        current_response_sender = Some(response_sender);

                        // Send request to target
                        if let Err(e) = sink.send(frame).await {
                            log::error!("Failed to send request to target: {}", e);
                            break;
                        }
                    }
                    Some(ProxyMessage::Close(ProxyCloseMessage { conn_sender })) => {
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
                "close message in proxy task"
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

impl Service<BytesFrame> for Resp2ProxyService {
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
            if let Err(e) = request_sender.send(ProxyMessage::Request(request)).await {
                bail!("Failed to send request to handler: {}", e);
            }

            Ok(responses)
        };

        Box::pin(fut)
    }
}

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
