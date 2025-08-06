use futures::stream::Stream;
use futures_util::{SinkExt, StreamExt};
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tower::{MakeService, Service};

static MAX_OUTSTANDING_RESPONSE_STREAMS: usize = 100;

pub async fn serve<M>(listenter: TcpListener, mut make_service: M) -> anyhow::Result<()>
where
    M: MakeService<std::net::SocketAddr, BytesFrame> + 'static,
    M::Service: Service<BytesFrame> + Send + 'static,
    M::MakeError: Into<anyhow::Error> + Send + 'static,
    M::Future: Send + 'static,
    <M::Service as Service<BytesFrame>>::Error: Into<anyhow::Error> + Send + 'static,
    <M::Service as Service<BytesFrame>>::Response: Stream<Item = BytesFrame> + Send + 'static,
    <M::Service as Service<BytesFrame>>::Future: Send + 'static,
{
    log::info!("serving...");

    loop {
        let (client_socket, client_addr) = listenter.accept().await?;

        match make_service.make_service(client_addr).await {
            Ok(service) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client_socket, service).await {
                        log::error!("Connection {} error: {}", client_addr, e);
                    }
                });
            }

            Err(e) => {
                log::error!(
                    "Failed to create service for connection {}: {}",
                    client_addr,
                    e.into()
                );
            }
        }
    }
}

pub async fn handle_connection<S>(client_socket: TcpStream, mut service: S) -> anyhow::Result<()>
where
    S: Service<BytesFrame>,
    S::Response: Stream<Item = BytesFrame> + Send + 'static,
    S::Error: Into<anyhow::Error>,
{
    let client_framed = Framed::new(client_socket, Resp2::default());
    let (client_sink, mut client_stream) = client_framed.split();

    let (response_forwarder_tx, mut response_forwarder_rx) =
        mpsc::channel::<S::Response>(MAX_OUTSTANDING_RESPONSE_STREAMS);

    let forward_task_join_handle = tokio::spawn(async move {
        let mut client_sink = client_sink;
        // Flatten streams of responses --
        // they interleave as req > [ resp > resp > resp ] > req > ...  This takes the stream of
        // streams and flattens it.
        while let Some(response_stream) = response_forwarder_rx.recv().await {
            let mut pinned = std::pin::pin!(response_stream);
            while let Some(response_frame) = pinned.as_mut().next().await {
                if client_sink.send(response_frame).await.is_err() {
                    log::error!("Failed to send response to client");
                    return;
                }
            }
        }
    });

    while let Some(frame_result) = client_stream.next().await {
        match frame_result {
            Ok(frame) => {
                match service.call(frame).await {
                    Ok(response_stream) => {
                        // Response streams are flattened by the response forwarder
                        if response_forwarder_tx.send(response_stream).await.is_err() {
                            log::error!("Failed to send response stream to handler");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to send command to backend: {}", e.into());
                    }
                }
            }
            Err(e) => {
                log::error!("Error reading from client: {}", e);
                break;
            }
        }
    }

    drop(response_forwarder_tx);
    if let Err(e) = forward_task_join_handle.await {
        log::error!("Forward task failed: {}", e);
    }

    log::info!("Connection closed");
    Ok(())
}
