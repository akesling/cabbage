use futures_util::SinkExt;
use redis_protocol::codec::Resp2;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
) -> anyhow::Result<()> {
    // Connect to target server
    let target_socket = TcpStream::connect(&target_addr).await?;
    log::info!("connected with target at: {}", target_addr);

    let mut client_framed = Framed::new(client_socket, Resp2::default());
    let mut target_framed = Framed::new(target_socket, Resp2::default());

    let mut command_count = 0u64;
    let mut response_count = 0u64;

    loop {
        tokio::select! {
            // Handle frames from client -> target
            frame_result = client_framed.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
                        command_count += 1;
                        log::info!("Client -> Target: command #{} - {:?}", command_count, frame);

                        if let Err(e) = target_framed.send(frame).await {
                            log::error!("Failed to send command to target: {}", e);
                            break;
                        }
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
            frame_result = target_framed.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
                        response_count += 1;
                        log::info!("Target -> Client: response #{} - {:?}", response_count, frame);

                        if let Err(e) = client_framed.send(frame).await {
                            log::error!("Failed to send response to client: {}", e);
                            break;
                        }
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
