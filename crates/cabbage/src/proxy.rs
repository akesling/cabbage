use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
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
use tower::Layer;
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

    let (client_sink, mut client_stream) = client_framed.split();
    let mut target_service = Resp2Backend::new(target_framed);

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let command_count = Arc::new(AtomicU64::new(0));
    let response_count = Arc::new(AtomicU64::new(0));

    // Create channel for response stream routing
    let (stream_tx, mut stream_rx) = mpsc::channel::<(
        Box<dyn Stream<Item = BytesFrame> + Send + Unpin>,
        Uuid,
        bool,
    )>(100);

    // Spawn single task to handle response streams and forward to client
    let forward_task = tokio::spawn(async move {
        let resp_count = response_count.clone();
        let mut client_sink = client_sink;

        // Process incoming response streams and forward to client
        while let Some((mut response_stream, command_id, is_doc_command)) = stream_rx.recv().await {
            // Process this response stream
            while let Some(response_frame) = response_stream.next().await {
                resp_count.fetch_add(1, Ordering::Relaxed);
                let response_num = resp_count.load(Ordering::Relaxed);
                let command_id_str = command_id.to_string();

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
                        response_frame
                    );
                }

                if client_sink.send(response_frame).await.is_err() {
                    log::error!("Failed to send response to client on connection {connection_id}");
                    return;
                }
            }
        }
    });

    // Process client commands and route responses
    let cmd_count = command_count.clone();

    loop {
        let Some(frame_result) = client_stream.next().await else {
            break; // End of stream
        };

        match frame_result {
            Ok(frame) => {
                cmd_count.fetch_add(1, Ordering::Relaxed);
                let command_id = Uuid::new_v4();
                let command_id_str = command_id.to_string();

                log::info!(
                    concat!("Client -> Target: ", "conn={} command={} - {:?}"),
                    connection_id,
                    command_id_str,
                    frame
                );

                let is_doc_command = frame == *DOC_REQUEST;

                match target_service.call(frame).await {
                    Ok(response_stream) => {
                        // Send the response stream to our single handler task
                        if stream_tx
                            .send((response_stream, command_id, is_doc_command))
                            .await
                            .is_err()
                        {
                            log::error!("Failed to send response stream to handler");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to send command to backend: {}", e);
                    }
                }
            }
            Err(e) => {
                log::error!("Error reading from client: {}", e);
                break;
            }
        }
    }

    // Close channel and wait for task to complete
    drop(stream_tx); // Close the stream channel to signal no more response streams
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

struct ProxyLoggerLayer<'conn> {
    connection_id: &'conn str,
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

struct ProxyLogger<
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

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
