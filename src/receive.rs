use nym_sdk::mixnet::{self, AnonymousSenderTag, MixnetMessageSender, StoragePaths};
use ripemd::{Digest, Ripemd160};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::signal;

const APP_TAG_LEN: usize = 22;
const TRANSFER_TIMEOUT_SECS: u64 = 600;
const CHUNK_SIZE: usize = 64 * 1024;
const HANDSHAKE_CHUNK_IDX: u32 = 0xFFFFFFFF;
const RESEND_INACTIVITY_SECS: u64 = 45;
const RESEND_CHECK_INTERVAL_SECS: u64 = 90;
const MAX_RESEND_ATTEMPTS: usize = 3;

struct FileTransfer {
    filename: String,
    app_tag: String,
    total_chunks: usize,
    original_file_size: u64,
    received_chunks: HashSet<usize>,
    target_file: File,
    last_activity: Instant,
    sender_tag: Option<AnonymousSenderTag>,
    last_resend_check: Instant,
    resend_attempts: HashMap<usize, usize>,
}

impl FileTransfer {
    fn new(filename: String, app_tag: String, total_chunks: usize, original_file_size: u64, receive_path: &PathBuf) -> io::Result<Self> {
        let target_path = receive_path.join(&filename);
        let target_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&target_path)?;
        let expected_size = (total_chunks as u64) * (CHUNK_SIZE as u64);
        target_file.set_len(expected_size)?;
        Ok(Self {
            filename,
            app_tag,
            total_chunks,
            original_file_size,
            received_chunks: HashSet::new(),
            target_file,
            last_activity: Instant::now(),
            sender_tag: None,
            last_resend_check: Instant::now(),
            resend_attempts: HashMap::new(),
        })
    }

    fn save_chunk(&mut self, chunk_idx: usize, chunk_data: &[u8]) -> io::Result<()> {
        let offset = chunk_idx as u64 * CHUNK_SIZE as u64;
        self.target_file.seek(SeekFrom::Start(offset))?;
        self.target_file.write_all(chunk_data)?;
        self.target_file.sync_data()?;
        self.received_chunks.insert(chunk_idx);
        self.last_activity = Instant::now();
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.received_chunks.len() == self.total_chunks
    }

    fn get_missing_chunks(&self) -> Vec<usize> {
        (0..self.total_chunks)
            .filter(|idx| !self.received_chunks.contains(idx))
            .collect()
    }

    fn should_check_resend(&self) -> bool {
        let time_since_last_chunk = self.last_activity.elapsed().as_secs();
        let time_since_last_check = self.last_resend_check.elapsed().as_secs();
        time_since_last_chunk >= RESEND_INACTIVITY_SECS && 
        time_since_last_check >= RESEND_CHECK_INTERVAL_SECS
    }

    fn can_request_resend(&self, chunk_idx: usize) -> bool {
        let attempts = self.resend_attempts.get(&chunk_idx).copied().unwrap_or(0);
        attempts < MAX_RESEND_ATTEMPTS
    }

    fn mark_resend_requested(&mut self, chunk_idx: usize) {
        let attempts = self.resend_attempts.entry(chunk_idx).or_insert(0);
        *attempts += 1;
        self.last_resend_check = Instant::now();
    }
}

pub async fn receive_mode(
    custom_path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let receive_path = custom_path.unwrap_or_else(|| PathBuf::from("./received"));
    fs::create_dir_all(&receive_path)?;
    let base_dir = crate::config::get_base_dir();
    let config_dir = base_dir.join("nymx-config");
    let paths = StoragePaths::new_from_dir(config_dir.to_str().unwrap()).unwrap();
    let mut client = mixnet::MixnetClientBuilder::new_with_default_storage(paths)
        .await
        .unwrap()
        .build()
        .unwrap()
        .connect_to_mixnet()
        .await
        .unwrap();

    println!("Your Nym address: {}", client.nym_address());
    println!("Saving files to: {}", receive_path.display());
    println!("Transfer timeout: {} seconds", TRANSFER_TIMEOUT_SECS);
    println!("Direct append mode");
    println!("Waiting for anonymous incoming files...\n");

    let transfers: Arc<Mutex<HashMap<String, FileTransfer>>> = Arc::new(Mutex::new(HashMap::new()));
    let completed_transfers: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("\n\n[INFO] Graceful shutdown initiated (Ctrl+C detected).");
                let transfers_lock = transfers.lock().unwrap();
                let active_count = transfers_lock.len();
                if active_count == 0 {
                    println!("[INFO] No active transfers. Shutting down cleanly.");
                } else {
                    println!("[WARN] {} active transfer(s) still in progress.", active_count);
                    println!("[INFO] Because chunks are sync'd to disk immediately, data is safe.");
                    println!("[INFO] Finalizing client disconnect. Partial files can be resumed later.");
                }
                break;
            }
            messages_result = client.wait_for_messages() => {
                let messages = match messages_result {
                    Some(msgs) => msgs,
                    None => continue,
                };
                for message in messages {
                    if message.message.is_empty() {
                        continue;
                    }
                    let (chunk_idx, total_chunks, file_size, filename, app_tag, chunk_data) = match parse_chunk_payload(&message.message) {
                        Ok(parsed) => parsed,
                        Err(e) => {
                            println!("Failed to parse chunk: {}", e);
                            continue;
                        }
                    };

                    if chunk_idx == HANDSHAKE_CHUNK_IDX as usize {
                        println!("Handshake received");
                        if let Some(sender_tag) = &message.sender_tag {
                            let ready_payload = format!("READY:{}", app_tag).into_bytes();
                            let _ = client.send_reply(sender_tag.clone(), ready_payload).await;
                            println!("  ✓ Replied with READY");
                        } else {
                            println!("  ✗ No sender tag in handshake, cannot reply");
                        }
                        continue;
                    }

                    let completed_check = completed_transfers.lock().unwrap();
                    if completed_check.contains(&app_tag) {
                        continue;
                    }
                    drop(completed_check);

                    let sender_tag = message.sender_tag.clone();
                    let mut transfers_lock = transfers.lock().unwrap();
                    let now = Instant::now();
                    let timed_out: Vec<String> = transfers_lock
                        .iter()
                        .filter(|(_, transfer)| now.duration_since(transfer.last_activity).as_secs() > TRANSFER_TIMEOUT_SECS)
                        .map(|(app_tag, _)| app_tag.clone())
                        .collect();
                    for app_tag in timed_out {
                        if let Some(transfer) = transfers_lock.remove(&app_tag) {
                            println!("\nTransfer timed out for '{}'", transfer.filename);
                        }
                    }

                    let transfer = transfers_lock
                        .entry(app_tag.clone())
                        .or_insert_with(|| {
                            match FileTransfer::new(filename.clone(), app_tag.clone(), total_chunks, file_size, &receive_path) {
                                Ok(t) => {
                                    println!("New transfer started for '{}'", filename);
                                    println!("Chunks: {}", total_chunks);
                                    t
                                }
                                Err(e) => {
                                    eprintln!("Failed to create target file: {}", e);
                                    FileTransfer::new(filename.clone(), app_tag.clone(), total_chunks, file_size, &receive_path).unwrap()
                                }
                            }
                        });

                    if sender_tag.is_some() && transfer.sender_tag.is_none() {
                        transfer.sender_tag = sender_tag.clone();
                    }

                    let was_new = transfer.received_chunks.insert(chunk_idx);
                    if !was_new {
                        continue;
                    }

                    if let Err(e) = transfer.save_chunk(chunk_idx, chunk_data) {
                        eprintln!("Failed to save chunk {}: {}", chunk_idx + 1, e);
                        continue;
                    }

                    let received_count = transfer.received_chunks.len();
                    let progress = (received_count as f64 / total_chunks as f64) * 100.0;
                    print!("\x1b[1A\r\x1b[KSaved chunk {}/{} for '{}' | {:.1}%", chunk_idx + 1, total_chunks, filename, progress);
                    println!(); 
                    io::stdout().flush().unwrap();

                    if transfer.is_complete() {
                        println!("All chunks received! Finalizing file...");
                        let filename = transfer.filename.clone();
                        let app_tag = transfer.app_tag.clone();
                        let sender_tag = transfer.sender_tag.clone();
                        let original_file_size = transfer.original_file_size;
                        let final_path = receive_path.join(&filename);
                        
                        transfer.target_file.set_len(original_file_size)?;
                        transfer.target_file.sync_all()?;
                        
                        let completed_transfer = transfers_lock.remove(&app_tag).unwrap();
                        drop(completed_transfer);
                        
                        let mut completed_lock = completed_transfers.lock().unwrap();
                        completed_lock.insert(app_tag.clone());
                        drop(completed_lock);

                        let file_size = final_path.metadata()?.len() as u64;
                        println!("File saved to: {}", final_path.display());
                        println!("File size: {} MiB", file_size / 1024 / 1024);

                        let mut hasher = Ripemd160::new();
                        let mut file = File::open(&final_path)?;
                        let mut buffer = vec![0u8; 65536];
                        loop {
                            let bytes_read = std::io::Read::read(&mut file, &mut buffer)?;
                            if bytes_read == 0 {
                                break;
                            }
                            hasher.update(&buffer[..bytes_read]);
                        }
                        let hash = hex::encode(hasher.finalize());
                        println!("File hash: {}", hash);

                        let reply_data = build_reply_payload(&app_tag, &hash);
                        if let Some(tag) = sender_tag {
                            let _ = client.send_reply(tag, reply_data).await;
                            println!("Reply sent back to sender");
                        }
                        println!("Transfer completed successfully\n");
                    } else if transfer.should_check_resend() {
                        let missing = transfer.get_missing_chunks();
                        if !missing.is_empty() {
                            println!("\n{} chunks still missing after {}s inactivity, requesting resend...", 
                                     missing.len(), RESEND_INACTIVITY_SECS);
                            let sender_tag_clone = transfer.sender_tag.clone();
                            if let Some(sender_tag) = sender_tag_clone {
                                for chunk_idx in missing {
                                    if transfer.can_request_resend(chunk_idx) {
                                        let resend_request = format!("RESEND:{}", chunk_idx);
                                        let _ = client.send_reply(sender_tag.clone(), resend_request.into_bytes()).await;
                                        transfer.mark_resend_requested(chunk_idx);
                                        println!("  Requested resend for chunk {}", chunk_idx + 1);
                                    } else {
                                        println!("  ✗ Max resend attempts reached for chunk {}", chunk_idx + 1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("[INFO] Disconnecting from Nym Mixnet...");
    client.disconnect().await;
    println!("[INFO] Shutdown complete. Goodbye!");
    Ok(())
}

fn parse_chunk_payload(payload: &[u8]) -> Result<(usize, usize, u64, String, String, &[u8]), &'static str> {
    if payload.len() < 8 + 8 + 2 + APP_TAG_LEN {
        return Err("Chunk payload too short");
    }
    let chunk_idx = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let total_chunks = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize;
    let file_size = u64::from_be_bytes([
        payload[8], payload[9], payload[10], payload[11], 
        payload[12], payload[13], payload[14], payload[15]
    ]);
    let filename_len = u16::from_be_bytes([payload[16], payload[17]]) as usize;
    let filename_start = 18;
    let filename_end = filename_start + filename_len;
    if payload.len() < filename_end + APP_TAG_LEN {
        return Err("Chunk payload too short for header");
    }
    let filename = String::from_utf8_lossy(&payload[filename_start..filename_end]).to_string();
    let app_tag = String::from_utf8_lossy(&payload[filename_end..filename_end + APP_TAG_LEN]).to_string();
    let chunk_data = &payload[filename_end + APP_TAG_LEN..];
    Ok((chunk_idx, total_chunks, file_size, filename, app_tag, chunk_data))
}

fn build_reply_payload(app_tag: &str, hash: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(app_tag.as_bytes());
    payload.extend_from_slice(hash.as_bytes());
    payload
}
