use anyhow::{Context, Result};
use nym_sdk::mixnet::Recipient;
use ripemd::{Digest, Ripemd160};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::network::NymNode;
use crate::protocol::{
    deserialize_message, hash_to_hex, serialize_resend_request, serialize_transfer_accept,
    serialize_transfer_complete, BinaryMessage, FileChunk, FileInfo, ResendRequest,
    TransferAccept,
};
use crate::utils::format_bytes;

const RESEND_TIMEOUT_SECS: u64 = 60;
const MAX_RESEND_ROUNDS: u32 = 10;
const MIB: u64 = 1024 * 1024;

#[derive(Deserialize)]
pub struct Whitelist {
    pub allowed_senders: Vec<String>,
}

pub fn load_whitelist(path: &str) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read whitelist file: {}", path))?;
    let whitelist: Whitelist =
        serde_json::from_str(&content).context("Failed to parse whitelist file")?;
    Ok(whitelist.allowed_senders)
}

pub struct ReceivedChunk {
    pub data: Vec<u8>,
    pub is_last: bool,
}

pub struct ReceiveState {
    pub current_file: Option<File>,
    pub current_file_name: String,
    pub current_file_size: u64,
    pub total_chunks: u32,
    pub next_expected_index: u32,
    pub chunks_written: u32,
    pub received_chunks: HashMap<u32, ReceivedChunk>,
    pub last_progress: i32,
    pub last_activity: Instant,
    pub sender_address: Option<String>,
    pub resend_rounds: u32,
    pub hasher: Ripemd160,
    pub sender_id: String,
}

impl ReceiveState {
    pub fn new(sender_id: String) -> Self {
        Self {
            current_file: None,
            current_file_name: String::new(),
            current_file_size: 0,
            total_chunks: 0,
            next_expected_index: 0,
            chunks_written: 0,
            received_chunks: HashMap::new(),
            last_progress: -1,
            last_activity: Instant::now(),
            sender_address: None,
            resend_rounds: 0,
            hasher: Ripemd160::new(),
            sender_id,
        }
    }
}

pub fn handle_file_info(info: FileInfo, output_dir: &str, state: &mut ReceiveState) {
    state.current_file = None;
    println!(
        "\n[Receiving] {} ({}, {} chunks) from {}",
        info.name,
        format_bytes(info.size as i64),
        info.chunks,
        state.sender_id
    );
    state.current_file_name = info.name.clone();
    state.current_file_size = info.size;
    state.total_chunks = info.chunks;
    state.next_expected_index = 0;
    state.chunks_written = 0;
    state.last_progress = -1;
    state.received_chunks.clear();
    state.last_activity = Instant::now();
    state.resend_rounds = 0;
    state.hasher = Ripemd160::new();
    state.sender_address = Some(info.sender_address.clone());

    if info.name.is_empty() {
        eprintln!("[Error] Empty filename received");
        return;
    }

    let file_path = PathBuf::from(output_dir).join(&info.name);
    if let Some(parent) = file_path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            eprintln!("[Error] Failed to create directory: {}", e);
            return;
        }
    }
    match File::create(&file_path) {
        Ok(f) => state.current_file = Some(f),
        Err(e) => eprintln!("[Error] Failed to create file: {}", e),
    }
}

pub fn handle_file_chunk(chunk: FileChunk, _output_dir: &str, state: &mut ReceiveState) {
    if state.current_file.is_none() {
        return;
    }
    state.last_activity = Instant::now();

    state.received_chunks.insert(
        chunk.index,
        ReceivedChunk {
            data: chunk.data,
            is_last: chunk.is_last,
        },
    );

    let file = state.current_file.as_mut().unwrap();
    while let Some(stored) = state.received_chunks.remove(&state.next_expected_index) {
        state.hasher.update(&stored.data);
        if let Err(e) = file.write_all(&stored.data) {
            eprintln!(
                "\n[Error] Failed to write chunk {}: {}",
                state.next_expected_index, e
            );
            return;
        }
        state.chunks_written += 1;
        state.next_expected_index += 1;

        let progress = if state.total_chunks > 0 {
            (state.chunks_written as f64 / state.total_chunks as f64 * 100.0).floor() as i32
        } else {
            0
        };
        if progress != state.last_progress {
            print!(
                "\r\x1b[K[Progress {}] {}% ({}/{})",
                state.sender_id,
                progress,
                state.chunks_written,
                state.total_chunks
            );
            std::io::stdout().flush().unwrap();
            state.last_progress = progress;
        }

        if stored.is_last || state.chunks_written == state.total_chunks {
            state.current_file = None;
            if state.chunks_written == state.total_chunks {
                println!(
                    "[Complete {}] {} ({})\n",
                    state.sender_id,
                    state.current_file_name,
                    format_bytes(state.current_file_size as i64)
                );
            } else {
                println!(
                    "[Warning {}] Incomplete: {} ({}/{} chunks)\n",
                    state.sender_id,
                    state.current_file_name,
                    state.chunks_written,
                    state.total_chunks
                );
            }
            state.received_chunks.clear();
            break;
        }
    }
}

pub async fn check_and_request_resend(
    state: &mut ReceiveState,
    nym: &mut NymNode,
) -> Result<()> {
    if state.current_file.is_none() {
        return Ok(());
    }
    if state.chunks_written >= state.total_chunks {
        return Ok(());
    }
    if state.last_activity.elapsed() < Duration::from_secs(RESEND_TIMEOUT_SECS) {
        return Ok(());
    }

    if state.resend_rounds >= MAX_RESEND_ROUNDS {
        eprintln!(
            "\n[Timeout {}] Gave up after {} resend rounds. File '{}' is incomplete.",
            state.sender_id,
            state.resend_rounds,
            state.current_file_name
        );
        state.current_file = None;
        state.received_chunks.clear();
        return Ok(());
    }

    let mut missing_indices = Vec::new();
    for i in state.next_expected_index..state.total_chunks {
        if !state.received_chunks.contains_key(&i) {
            missing_indices.push(i);
        }
    }
    if missing_indices.is_empty() {
        return Ok(());
    }

    state.resend_rounds += 1;
    println!(
        "\n[Timeout {}] {} chunks missing after {}s (round {}). Requesting resend...",
        state.sender_id,
        missing_indices.len(),
        RESEND_TIMEOUT_SECS,
        state.resend_rounds
    );

    if let Some(ref sender_addr) = state.sender_address {
        match sender_addr.parse::<Recipient>() {
            Ok(recipient) => {
                let req = ResendRequest {
                    file_name: state.current_file_name.clone(),
                    missing_indices,
                };
                let data = serialize_resend_request(&req);
                if let Err(e) = nym.send_bytes(&recipient, &data).await {
                    eprintln!("[Error] Failed to send resend request: {}", e);
                } else {
                    println!("[Resend {}] Request sent to sender via mixnet.", state.sender_id);
                }
            }
            Err(e) => {
                eprintln!("[Error] Invalid sender address '{}': {}", sender_addr, e);
            }
        }
    } else {
        eprintln!("[Warning] No sender address known, cannot request resend.");
    }

    state.last_activity = Instant::now();
    Ok(())
}

pub async fn send_transfer_complete(
    state: &mut ReceiveState,
    nym: &mut NymNode,
) -> Result<()> {
    let sender_addr = match &state.sender_address {
        Some(addr) => addr.clone(),
        None => {
            eprintln!("[Warning] Cannot send completion notice: no sender address");
            return Ok(());
        }
    };
    let recipient = match sender_addr.parse::<Recipient>() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[Error] Invalid sender address: {}", e);
            return Ok(());
        }
    };

    let hash_result = state.hasher.clone().finalize();
    let mut ripemd160_hash = [0u8; 20];
    ripemd160_hash.copy_from_slice(&hash_result);

    let tc = crate::protocol::TransferComplete {
        file_name: state.current_file_name.clone(),
        ripemd160_hash,
    };
    let data = serialize_transfer_complete(&tc);
    match nym.send_bytes(&recipient, &data).await {
        Ok(_) => {
            println!(
                "[Verified {}] Transfer complete notice sent to sender.",
                state.sender_id
            );
            println!(
                "[Verified {}] File hash: {}",
                state.sender_id,
                hash_to_hex(&ripemd160_hash)
            );
        }
        Err(e) => {
            eprintln!("[Error] Failed to send transfer complete notice: {}", e);
        }
    }
    Ok(())
}

pub async fn run_receive_mode(
    nym: Arc<tokio::sync::Mutex<NymNode>>,
    output_dir: &str,
    running: Arc<AtomicBool>,
    whitelist_path: Option<&str>,
    quota_mib: Option<u64>,
) -> Result<()> {
    fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    let whitelist = if let Some(path) = whitelist_path {
        println!("[Config] Loading whitelist from: {}", path);
        Some(load_whitelist(path)?)
    } else {
        None
    };

    let quota_bytes: Option<u64> = quota_mib.map(|m| m * MIB);

    println!("Listening for files. Saving to: {}", output_dir);
    if whitelist.is_some() {
        println!("[Config] Whitelist mode active - only allowed senders will be accepted");
    } else {
        println!("[Config] No whitelist - accepting all senders");
    }
    if let Some(q) = quota_mib {
        println!(
            "[Config] Quota active - max file size: {} MiB ({} bytes)",
            q,
            q * MIB
        );
    } else {
        println!("[Config] No quota - accepting any file size");
    }
    println!("Press Ctrl+C to exit\n");

    let mut states: HashMap<String, ReceiveState> = HashMap::new();
    let mut sender_counter = 0;

    while running.load(Ordering::Relaxed) {
        {
            let mut nym_lock = nym.lock().await;
            for state in states.values_mut() {
                check_and_request_resend(state, &mut nym_lock).await?;
            }
        }

        states.retain(|_, state| state.current_file.is_some());

        let messages = {
            let mut nym_lock = nym.lock().await;
            match nym_lock.receive().await {
                Ok(msgs) => msgs,
                Err(e) => {
                    eprintln!("[Error] Receive error: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        };

        if !running.load(Ordering::Relaxed) {
            break;
        }

        for (sender_tag, raw_bytes) in messages {
            match deserialize_message(&raw_bytes) {
                Ok(BinaryMessage::TransferOffer(offer)) => {
                    let allowed_by_whitelist = match &whitelist {
                        Some(list) => list.contains(&offer.sender_address),
                        None => true,
                    };

                    let allowed_by_quota = match quota_bytes {
                        Some(max_bytes) => offer.size <= max_bytes,
                        None => true,
                    };

                    let is_allowed = allowed_by_whitelist && allowed_by_quota;

                    if is_allowed {
                        println!(
                            "\n[Request] '{}' ({}) wants to send '{}'.",
                            offer.sender_address,
                            format_bytes(offer.size as i64),
                            offer.file_name
                        );
                        let accept = TransferAccept {
                            file_name: offer.file_name.clone(),
                            accepted: true,
                        };
                        let accept_bytes = serialize_transfer_accept(&accept);
                        match offer.sender_address.parse::<Recipient>() {
                            Ok(sender_recipient) => {
                                let mut nym_lock = nym.lock().await;
                                if let Err(e) =
                                    nym_lock.send_bytes(&sender_recipient, &accept_bytes).await
                                {
                                    eprintln!("[Error] Unable to send confirmation: {}", e);
                                } else {
                                    println!("[Handshake] Accepted. Waiting for file data...");
                                }
                            }
                            Err(e) => eprintln!("[Error] Invalid sender address: {}", e),
                        }
                    } else {
                        let reason = if !allowed_by_whitelist && !allowed_by_quota {
                            format!(
                                "not in whitelist AND exceeds quota (max {} MiB)",
                                quota_mib.unwrap()
                            )
                        } else if !allowed_by_whitelist {
                            "not in whitelist".to_string()
                        } else {
                            format!(
                                "exceeds quota of {} MiB (offered: {})",
                                quota_mib.unwrap(),
                                format_bytes(offer.size as i64)
                            )
                        };

                        println!(
                            "\n[Denied] '{}' rejected for '{}': {}.",
                            offer.sender_address, offer.file_name, reason
                        );

                        let accept = TransferAccept {
                            file_name: offer.file_name.clone(),
                            accepted: false,
                        };
                        let accept_bytes = serialize_transfer_accept(&accept);
                        match offer.sender_address.parse::<Recipient>() {
                            Ok(sender_recipient) => {
                                let mut nym_lock = nym.lock().await;
                                if let Err(e) =
                                    nym_lock.send_bytes(&sender_recipient, &accept_bytes).await
                                {
                                    eprintln!("[Error] Unable to send rejection: {}", e);
                                } else {
                                    println!("[Handshake] Rejected. Sender notified.");
                                }
                            }
                            Err(e) => eprintln!("[Error] Invalid sender address: {}", e),
                        }
                    }
                }
                Ok(BinaryMessage::FileInfo(info)) => {
                    sender_counter += 1;
                    let sender_id = format!("S{}", sender_counter);
                    let state = states
                        .entry(sender_tag.clone())
                        .or_insert_with(|| ReceiveState::new(sender_id.clone()));
                    state.sender_address = Some(info.sender_address.clone());
                    handle_file_info(info, output_dir, state);
                }
                Ok(BinaryMessage::FileChunk(chunk)) => {
                    if let Some(state) = states.get_mut(&sender_tag) {
                        handle_file_chunk(chunk, output_dir, state);
                        if state.current_file.is_none()
                            && state.chunks_written == state.total_chunks
                            && state.total_chunks > 0
                        {
                            sleep(Duration::from_millis(500)).await;
                            let mut nym_lock = nym.lock().await;
                            send_transfer_complete(state, &mut nym_lock).await?;
                            states.remove(&sender_tag);
                        }
                    } else {
                        eprintln!(
                            "[Warning] Received chunk from unknown sender tag: {}",
                            sender_tag
                        );
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}
