use futures_util::SinkExt;
use redis_protocol::codec::Resp2;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
) -> anyhow::Result<()> {
    // Connect to target server
    let target_socket = TcpStream::connect(&target_addr).await?;
    log::info!("connected with target at: {}", target_addr);

    let (client_read, client_write) = client_socket.into_split();
    let (target_read, target_write) = target_socket.into_split();

    let client_reader = FramedRead::new(client_read, Resp2::default());
    let client_writer = FramedWrite::new(client_write, Resp2::default());
    let target_reader = FramedRead::new(target_read, Resp2::default());
    let target_writer = FramedWrite::new(target_write, Resp2::default());

    let client_to_target = tokio::spawn(forward_frames(
        client_reader,
        target_writer,
        "Client -> Target",
        "command",
    ));

    let target_to_client = tokio::spawn(forward_frames(
        target_reader,
        client_writer,
        "Target -> Client",
        "response",
    ));

    let (client_result, target_result) = tokio::join!(client_to_target, target_to_client);

    if let Err(e) = client_result.and_then(|r| Ok(r)) {
        log::error!("Client to target error: {}", e);
    }

    if let Err(e) = target_result.and_then(|r| Ok(r)) {
        log::error!("Target to client error: {}", e);
    }

    log::info!("Connection closed");
    Ok(())
}

async fn forward_frames<R, W>(
    mut reader: FramedRead<R, Resp2>,
    mut writer: FramedWrite<W, Resp2>,
    direction: &str,
    frame_type: &str,
) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut frame_count = 0u64;

    while let Some(frame_result) = reader.next().await {
        match frame_result {
            Ok(frame) => {
                frame_count += 1;
                log::info!(
                    "{}: {} #{} - {:?}",
                    direction,
                    frame_type,
                    frame_count,
                    frame
                );

                if let Err(e) = writer.send(frame).await {
                    log::error!("Failed to send {}: {}", frame_type, e);
                    break;
                }
            }
            Err(e) => {
                log::error!("Error reading {}: {}", frame_type, e);
                break;
            }
        }
    }

    log::info!(
        "{} finished after {} {}s",
        direction,
        frame_count,
        frame_type
    );
    Ok(())
}
