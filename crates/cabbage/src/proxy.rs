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

struct RedisProxyService {}

struct FrameStream {
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

// TODO(akesling): Implement a Service<BytesFrame> -> BytesFrame
impl Service<BytesFrame> for RedisProxyService {
    type Response = FrameStream;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BytesFrame) -> Self::Future {
        // Create a channel for the response stream
        let (sender, stream) = FrameStream::new(10);

        // Handle the request in an async block
        let fut = async move {
            // For now, just echo back the request
            // In a real implementation, this would forward to Redis
            if let Err(e) = sender.send(req).await {
                log::error!("Failed to send response frame: {}", e);
            }

            Ok(stream)
        };

        Box::pin(fut)
    }
}

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
