use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use redis_protocol::{codec::Resp2, resp2::types::BytesFrame};
use tokio::net::TcpStream;
use tokio_util::{bytes::Bytes, codec::Framed};
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

    let mut command_count = 0u64;
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
                        command_count += 1;
                        let command_id = Uuid::new_v4();

                        log::info!(
                            "Client -> Target: command #{} id: {} - {:?}",
                            command_count, command_id, frame);

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

                        if is_doc_command {
                            log::info!(
                                "Target -> Client: response #{} - documents",
                                response_count);
                        } else {
                            log::info!(
                                "Target -> Client: response #{} - {:?}",
                                response_count, frame);
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
