use std::pin::Pin;

use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tower::Layer;
use tower::Service;
use uuid::Uuid;

static MAX_OUTSTANDING_RESPONSE_STREAMS: usize = 100;

// TODO(akesling): Add connection timeout, etc.
pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
    connection_id: Uuid,
) -> anyhow::Result<()> {
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

    let (response_forwarder_tx, mut response_forwarder_rx) = mpsc::channel::<
        Box<dyn Stream<Item = BytesFrame> + Send>,
    >(MAX_OUTSTANDING_RESPONSE_STREAMS);
    let forward_task_join_handle = tokio::spawn(async move {
        let mut client_sink = client_sink;
        // Flatten streams of responses --
        // they interleave as req > [ resp > resp > resp ] > req > ...  This takes the stream of
        // streams and flattens it.
        while let Some(response_stream) = response_forwarder_rx.recv().await {
            let mut pinned = Pin::from(response_stream);
            while let Some(response_frame) = pinned.as_mut().next().await {
                if client_sink.send(response_frame).await.is_err() {
                    log::error!("Failed to send response to client on connection {connection_id}");
                    return;
                }
            }
        }
    });

    while let Some(frame_result) = client_stream.next().await {
        match frame_result {
            Ok(frame) => {
                match target_service.call(frame).await {
                    Ok(response_stream) => {
                        // Response streams are flattened by the response forwarder
                        if response_forwarder_tx.send(response_stream).await.is_err() {
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
    drop(response_forwarder_tx);
    if let Err(e) = forward_task_join_handle.await {
        log::error!("Forward task failed: {}", e);
    }

    log::info!("Connection closed");
    Ok(())
}

// TODO(akesling): Implement a "serve()" function which takes a "Listener" and a MakeService
