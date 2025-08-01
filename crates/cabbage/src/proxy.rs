use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tower::Layer;
use tower::Service;
use uuid::Uuid;

// TODO(akesling): Add connection timeout, etc.
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
    let connection_id_string = connection_id.to_string();
    let logger = crate::middleware::ProxyLoggerLayer::new(&connection_id_string);
    let mut target_service = logger.layer(crate::service::Resp2Backend::new(target_framed));

    // Create channel for response stream routing
    let (stream_tx, mut stream_rx) =
        mpsc::channel::<Box<dyn Stream<Item = BytesFrame> + Send + Unpin>>(100);

    // Spawn single task to handle response streams and forward to client
    let forward_task = tokio::spawn(async move {
        let mut client_sink = client_sink;
        while let Some(mut response_stream) = stream_rx.recv().await {
            while let Some(response_frame) = response_stream.next().await {
                if client_sink.send(response_frame).await.is_err() {
                    log::error!("Failed to send response to client on connection {connection_id}");
                    return;
                }
            }
        }
    });

    loop {
        let Some(frame_result) = client_stream.next().await else {
            break; // End of stream
        };

        match frame_result {
            Ok(frame) => {
                match target_service.call(frame).await {
                    Ok(response_stream) => {
                        // Send the response stream to our single handler task
                        if stream_tx.send(response_stream).await.is_err() {
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

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
