use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn handle_connection(
    client_socket: TcpStream,
    target_addr: String,
) -> anyhow::Result<()> {
    // Connect to target server
    let target_socket = TcpStream::connect(&target_addr).await?;

    // Split sockets
    let (client_read, client_write) = client_socket.into_split();
    let (target_read, target_write) = target_socket.into_split();

    // Forward data from client to target
    let client_to_target =
        tokio::spawn(
            async move { copy_and_log(client_read, target_write, "Client -> Target").await },
        );

    let target_to_client =
        tokio::spawn(
            async move { copy_and_log(target_read, client_write, "Target -> Client").await },
        );

    tokio::select! {
        result = client_to_target => {
            if let Err(e) = result? {
                log::error!("Client to target error: {}", e);
            }
        }
        result = target_to_client => {
            if let Err(e) = result? {
                log::error!("Target to client error: {}", e);
            }
        }
    }

    log::info!("Connection closed");
    Ok(())
}

async fn copy_and_log<R, W>(mut reader: R, mut writer: W, direction: &str) -> anyhow::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buffer = [0u8; 4096];
    let mut total_bytes = 0u64;

    loop {
        match reader.read(&mut buffer).await? {
            0 => {
                // EOF reached
                println!(
                    "{}: Connection closed after {} bytes",
                    direction, total_bytes
                );
                break;
            }
            bytes_read => {
                writer.write_all(&buffer[..bytes_read]).await?;
                writer.flush().await?;

                total_bytes += bytes_read as u64;
                println!(
                    "{}: {} bytes (total: {})",
                    direction, bytes_read, total_bytes
                );
            }
        }
    }

    Ok(())
}
