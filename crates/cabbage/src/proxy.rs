use futures_util::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
) -> anyhow::Result<()> {
    // Connect to target server
    let target_socket = TcpStream::connect(&target_addr).await?;
    log::info!("connected with target at: {}", target_addr);

    let client_framed = Framed::new(client_socket, Resp2::default());
    let target_framed = Framed::new(target_socket, Resp2::default());

    let (mut client_sink, mut client_stream) = client_framed.split();
    let (mut target_sink, mut target_stream) = target_framed.split();

    let mut command_count = 0u64;
    let mut response_count = 0u64;

    let mut client_next = Box::pin(client_stream.next());
    let mut target_next = Box::pin(target_stream.next());

    loop {
        tokio::select! {
            // Handle frames from client -> target
            frame_result = &mut client_next => {
                match frame_result {
                    Some(Ok(frame)) => {
                        command_count += 1;
                        log::info!("Client -> Target: command #{} - {:?}", command_count, frame);

                        if let Err(e) = target_sink.send(frame).await {
                            log::error!("Failed to send command to target: {}", e);
                            break;
                        }
                        client_next = Box::pin(client_stream.next());
                    }
                    Some(Err(e)) => {
                        log::error!("Error reading from client: {}", e);
                        break;
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
                        log::info!("Target -> Client: response #{} - {:?}", response_count, frame);

                        if let Err(e) = client_sink.send(frame).await {
                            log::error!("Failed to send response to client: {}", e);
                            break;
                        }
                        target_next = Box::pin(target_stream.next());
                    }
                    Some(Err(e)) => {
                        log::error!("Error reading from target: {}", e);
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

    log::info!("Connection closed");
    Ok(())
}
