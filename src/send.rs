use anyhow::{Context, Result};
use nym_sdk::mixnet::Recipient;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::network::NymNode;
use crate::protocol::{
    deserialize_message, hash_to_hex, serialize_file_chunk, serialize_file_info,
    serialize_transfer_offer, BinaryMessage, FileChunk, FileInfo, ResendRequest, TransferComplete,
    TransferOffer,
};
use crate::utils::format_bytes;

type ChunkCache = HashMap<u32, Vec<u8>>;

#[derive(Debug)]
enum ReceiverEvent {
    ResendRequest(ResendRequest),
    TransferComplete(TransferComplete),
}

pub async fn send_file(
    nym: Arc<tokio::sync::Mutex<NymNode>>,
    recipient: &Recipient,
    file_path: &str,
    chunk_size_bytes: usize,
    chunks_per_second: f64,
    running: &Arc<AtomicBool>,
) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        anyhow::bail!("File not found: {}", file_path);
    }

    let file_name = path
        .file_name()
        .context("Invalid file name")?
        .to_string_lossy()
        .to_string();

    let file_size = std::fs::metadata(path)?.len();
    let total_chunks =
        ((file_size + chunk_size_bytes as u64 - 1) / chunk_size_bytes as u64) as u32;

    println!(
        "[Sending] {} ({}, {} chunks of {} KiB)",
        file_name,
        format_bytes(file_size as i64),
        total_chunks,
        chunk_size_bytes / 1024
    );

    let sender_address = {
        let nym_lock = nym.lock().await;
        nym_lock.client.nym_address().to_string()
    };

    let offer = TransferOffer {
        file_name: file_name.clone(),
        size: file_size,
        sender_address: sender_address.clone(),
    };
    let offer_bytes = serialize_transfer_offer(&offer);

    {
        let mut nym_lock = nym.lock().await;
        nym_lock.send_bytes(recipient, &offer_bytes).await?;
    }

    println!("[Handshake] Send a contact request to the recipient...");

    let mut handshake_ok = false;
    let mut handshake_rejected = false;
    let handshake_timeout = Duration::from_secs(45);
    let handshake_start = Instant::now();

    while handshake_start.elapsed() < handshake_timeout {
        if !running.load(Ordering::Relaxed) {
            anyhow::bail!("[Aborted] Handshake aborted.");
        }

        let messages = {
            let mut nym_lock = nym.lock().await;
            nym_lock.receive().await.unwrap_or_default()
        };

        for (_sender_tag, raw_bytes) in messages {
            if let Ok(BinaryMessage::TransferAccept(acc)) = deserialize_message(&raw_bytes) {
                if acc.file_name == file_name {
                    if acc.accepted {
                        handshake_ok = true;
                    } else {
                        handshake_rejected = true;
                    }
                    break;
                }
            }
        }

        if handshake_ok || handshake_rejected {
            break;
        }

        sleep(Duration::from_millis(500)).await;
    }

    if handshake_rejected {
        anyhow::bail!(
            "Transfer rejected by recipient.\n"
        );
    }

    if !handshake_ok {
        anyhow::bail!("The recipient is offline or unavailable. Transfer canceled.");
    }

    println!("[Handshake] The recipient is online and ready. Start transfer...");

    let info = FileInfo {
        name: file_name.clone(),
        size: file_size,
        chunks: total_chunks,
        sender_address,
    };
    let info_bytes = serialize_file_info(&info);

    {
        let mut nym_lock = nym.lock().await;
        nym_lock.send_bytes(recipient, &info_bytes).await?;
    }

    println!("[Info] File metadata sent. Waiting 5s before chunks...");
    sleep(Duration::from_secs(5)).await;

    let mut file = File::open(path)?;
    let mut chunk_cache: ChunkCache = HashMap::with_capacity(total_chunks as usize);
    let mut buffer = vec![0u8; chunk_size_bytes];
    let mut chunk_index: u32 = 0;
    let mut bytes_read_total: u64 = 0;
    let mut resend_count = 0;
    let mut early_completion: Option<TransferComplete> = None;

    let send_interval = if chunks_per_second > 0.0 {
        Duration::from_secs_f64(1.0 / chunks_per_second)
    } else {
        Duration::from_millis(0)
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<ReceiverEvent>();
    let nym_clone = nym.clone();
    let running_clone = running.clone();
    let stop_rx = Arc::new(AtomicBool::new(false));
    let stop_rx_clone = stop_rx.clone();

    let task_handle = tokio::spawn(async move {
        while running_clone.load(Ordering::Relaxed) && !stop_rx_clone.load(Ordering::Relaxed) {
            let messages = {
                let mut lock = nym_clone.lock().await;
                lock.receive().await.unwrap_or_default()
            };

            for (_sender_tag, raw_bytes) in messages {
                match deserialize_message(&raw_bytes) {
                    Ok(BinaryMessage::ResendRequest(req)) => {
                        let _ = tx.send(ReceiverEvent::ResendRequest(req));
                    }
                    Ok(BinaryMessage::TransferComplete(tc)) => {
                        let _ = tx.send(ReceiverEvent::TransferComplete(tc));
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let start_time = Instant::now();

    loop {
        if !running.load(Ordering::Relaxed) {
            println!("\n[Aborted] Send cancelled.");
            stop_rx.store(true, Ordering::Relaxed);
            let _ = task_handle.await;
            return Ok(());
        }

        while let Ok(event) = rx.try_recv() {
            match event {
                ReceiverEvent::ResendRequest(req) => {
                    resend_count += 1;
                    println!(
                        "\n[Resend #{}] Received request for {} missing chunks of '{}'",
                        resend_count,
                        req.missing_indices.len(),
                        req.file_name
                    );
                    let mut nym_lock = nym.lock().await;
                    send_missing_chunks(&mut nym_lock, recipient, &req, &chunk_cache).await?;
                }
                ReceiverEvent::TransferComplete(tc) => {
                    if tc.file_name == file_name {
                        early_completion = Some(tc);
                    }
                }
            }
        }

        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let is_last = bytes_read_total + bytes_read as u64 >= file_size;
        let chunk = FileChunk {
            index: chunk_index,
            total: total_chunks,
            data: buffer[..bytes_read].to_vec(),
            is_last,
        };

        let chunk_bytes = serialize_file_chunk(&chunk);
        chunk_cache.insert(chunk_index, chunk_bytes.clone());

        {
            let mut nym_lock = nym.lock().await;
            nym_lock.send_bytes(recipient, &chunk_bytes).await?;
        }

        bytes_read_total += bytes_read as u64;
        chunk_index += 1;

        let progress = (chunk_index as f64 / total_chunks as f64 * 100.0).floor() as u32;
        print!(
            "\r\x1b[K[Progress] {}% ({}/{}) - {} sent",
            progress,
            chunk_index,
            total_chunks,
            format_bytes(bytes_read_total as i64)
        );
        std::io::stdout().flush().unwrap();

        if chunk_index < total_chunks && !send_interval.is_zero() {
            sleep(send_interval).await;
        }
    }

    println!(
        "\n[Sent] All {} chunks transmitted in {:.1}s.",
        total_chunks,
        start_time.elapsed().as_secs_f64()
    );

    if resend_count > 0 {
        println!(
            "[Resend] Handled {} resend request(s) during transmission.",
            resend_count
        );
    }

    let mut received_confirmation = false;

    if let Some(tc) = early_completion {
        let hex_hash = hash_to_hex(&tc.ripemd160_hash);
        println!();
        println!("============================================================");
        println!(" Thank you for your submission.");
        println!(" Your file hash is: {}", hex_hash);
        println!("============================================================");
        println!("[Verified] Receiver confirmed complete transfer (intercepted early).");
        received_confirmation = true;
    }

    if !received_confirmation {
        println!("[Info] Wait for the confirmation of receipt...");
        println!("[Info] Will wait up to 1 minute.");

        let completion_timeout = Duration::from_secs(60);
        let wait_start = Instant::now();
        let mut last_debug = Instant::now();

        while running.load(Ordering::Relaxed) {
            if wait_start.elapsed() > completion_timeout {
                println!(
                    "\n[Timeout] No completion notice received after {}s. Exiting.",
                    completion_timeout.as_secs()
                );
                break;
            }

            if last_debug.elapsed() >= Duration::from_secs(10) {
                let elapsed = wait_start.elapsed().as_secs();
                println!(
                    "\n[Info] Waiting for {}s... (timeout in {}s)",
                    elapsed,
                    completion_timeout.as_secs() - elapsed
                );
                last_debug = Instant::now();
            }

            while let Ok(event) = rx.try_recv() {
                match event {
                    ReceiverEvent::ResendRequest(req) => {
                        resend_count += 1;
                        println!(
                            "\n[Resend #{}] Received request for {} missing chunks of '{}'",
                            resend_count,
                            req.missing_indices.len(),
                            req.file_name
                        );
                        let mut nym_lock = nym.lock().await;
                        send_missing_chunks(&mut nym_lock, recipient, &req, &chunk_cache).await?;
                    }
                    ReceiverEvent::TransferComplete(tc) => {
                        if tc.file_name == file_name {
                            let hex_hash = hash_to_hex(&tc.ripemd160_hash);
                            println!();
                            println!("============================================================");
                            println!(" Thank you for your submission.");
                            println!(" Your file hash is: {}", hex_hash);
                            println!("============================================================");
                            println!("[Verified] Receiver confirmed complete transfer.");
                            received_confirmation = true;
                            break;
                        } else {
                            println!(
                                "\n[Warning] Received completion for different file: {}",
                                tc.file_name
                            );
                        }
                    }
                }
            }

            if received_confirmation {
                break;
            }

            sleep(Duration::from_secs(1)).await;
        }
    }

    if !received_confirmation {
        println!("\n[Warning] Transfer may be incomplete - no confirmation received from receiver.");
    }

    stop_rx.store(true, Ordering::Relaxed);
    let _ = task_handle.await;

    Ok(())
}

async fn send_missing_chunks(
    nym: &mut NymNode,
    recipient: &Recipient,
    req: &ResendRequest,
    chunk_cache: &ChunkCache,
) -> Result<()> {
    let mut sent = 0;
    let mut not_found = 0;

    for &index in &req.missing_indices {
        if let Some(chunk_bytes) = chunk_cache.get(&index) {
            nym.send_bytes(recipient, chunk_bytes).await?;
            sent += 1;
            println!("[Resend] Sent chunk {}", index);
            sleep(Duration::from_millis(200)).await;
        } else {
            not_found += 1;
            println!("[Warning] Chunk {} not in cache anymore", index);
        }
    }

    println!(
        "[Resend] Re-sent {} chunks ({} not in cache anymore)",
        sent, not_found
    );

    Ok(())
}
