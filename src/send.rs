use indicatif::{ProgressBar, ProgressStyle};
use nym_sdk::mixnet::{self, MixnetMessageSender, Recipient, StoragePaths};
use rand::Rng;
use ripemd::{Digest, Ripemd160};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const FILE_TAG_LEN: usize = 22;
const CHUNK_SIZE: usize = 64 * 1024;
const MAX_RETRIES: usize = 10;
const RETRY_DELAY_MS: u64 = 1000;
const REPLY_TIMEOUT_SECS: u64 = 120;
const HANDSHAKE_CHUNK_IDX: u32 = 0xFFFFFFFF;
const HANDSHAKE_TIMEOUT_SECS: u64 = 20;
const RESEND_REQUEST_PREFIX: &str = "RESEND:";

pub async fn send_mode(address: String, file_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
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

    let our_address = client.nym_address();
    println!("Your Nym address: {}", our_address);
    println!("Sending anonymously to: {}\n", address);

    let file_metadata = std::fs::metadata(&file_path)?;
    let file_size_bytes = file_metadata.len();
    let file_size_mib = file_size_bytes / 1024 / 1024;
    let total_chunks = ((file_size_bytes as f64) / (CHUNK_SIZE as f64)).ceil() as usize;
    let filename = file_path
        .file_name()
        .ok_or("Invalid filename")?
        .to_string_lossy()
        .to_string();

    println!("File size: {} MiB", file_size_mib);
    println!("Original filename: {}", filename);
    println!("Splitting into {} chunks of {} KB each", total_chunks, CHUNK_SIZE / 1024);

    let sender_hash = ripemd160_file(&file_path)?;
    println!("Sender hash: {}", sender_hash);

    let file_tag = generate_file_tag();
    let target_address: Recipient = match address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Error: Invalid Nym address format");
            eprintln!("Details: {}", e);
            eprintln!("\nA valid Nym address has the format:");
            eprintln!("  <Base58>.<Base58>@<Base58>");
            eprintln!("\nPlease check your nymx.json configuration.");
            client.disconnect().await;
            return Err("Invalid recipient address".into());
        }
    };

    println!("\nSending handshake to check if recipient is online...");
    let handshake_payload = build_handshake_payload(&file_tag);
    if let Err(e) = client.send_plain_message(target_address, handshake_payload).await {
        eprintln!("Failed to send handshake: {}", e);
        client.disconnect().await;
        let elapsed = start_time.elapsed();
        println!("Done!");
        println!("Total time: {}", format_duration(elapsed));
        return Ok(());
    }

    println!("Waiting up to {}s for recipient response...", HANDSHAKE_TIMEOUT_SECS);
    let handshake_result = timeout(Duration::from_secs(HANDSHAKE_TIMEOUT_SECS), async {
        loop {
            if let Some(msgs) = client.wait_for_messages().await {
                if let Some(msg) = msgs.into_iter().find(|m| !m.message.is_empty()) {
                    return msg;
                }
            }
        }
    })
    .await;

    let handshake_ok = match handshake_result {
        Ok(reply) => {
            let reply_str = String::from_utf8_lossy(&reply.message);
            let expected_prefix = format!("READY:{}", file_tag);
            if reply_str.starts_with(&expected_prefix) {
                println!("✓ Recipient is online and ready!\n");
                true
            } else {
                println!("✗ Unexpected response from recipient");
                false
            }
        }
        Err(_) => {
            println!("✗ No response within {}s", HANDSHAKE_TIMEOUT_SECS);
            false
        }
    };

    if !handshake_ok {
        println!("\nRecipient appears to be offline. Aborting before sending data.");
        client.disconnect().await;
        let elapsed = start_time.elapsed();
        println!("Done!");
        println!("Total time: {}", format_duration(elapsed));
        return Ok(());
    }

    let overall_pb = ProgressBar::new(total_chunks as u64);
    overall_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan/blue} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} chunks",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    overall_pb.set_message("Sending chunks");

    let mut failed_chunks = Vec::new();
    let mut file = File::open(&file_path)?;

    for chunk_idx in 0..total_chunks {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let bytes_read = file.read(&mut buffer)?;
        let chunk_data = &buffer[..bytes_read];

        for attempt in 0..MAX_RETRIES {
            let chunk_header = build_chunk_header(chunk_idx, total_chunks, file_size_bytes, &filename, &file_tag);
            let mut chunk_payload = chunk_header;
            chunk_payload.extend_from_slice(chunk_data);

            match client.send_plain_message(target_address, chunk_payload).await {
                Ok(_) => {
                    if attempt > 0 {
                        println!("\n✓ Chunk {} succeeded on attempt {}", chunk_idx + 1, attempt + 1);
                    }
                    break;
                }
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        eprintln!(
                            "\nChunk {} failed (attempt {}/{}): {}. Retrying...",
                            chunk_idx + 1,
                            attempt + 1,
                            MAX_RETRIES,
                            e
                        );
                        sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
                    } else {
                        eprintln!(
                            "\nChunk {} failed after {} attempts: {}",
                            chunk_idx + 1,
                            MAX_RETRIES,
                            e
                        );
                        failed_chunks.push(chunk_idx);
                    }
                }
            }
        }
        overall_pb.inc(1);
        sleep(Duration::from_millis(50)).await;
    }

    overall_pb.finish_with_message("All chunks sent");

    if !failed_chunks.is_empty() {
        println!("\nWarning: {} chunks failed to send:", failed_chunks.len());
        for chunk_idx in &failed_chunks {
            println!("  - Chunk {}", chunk_idx + 1);
        }
        println!("The receiver may have an incomplete file.");
    }

    println!("\nWaiting for reply or resend requests...\n");

    let file_path_clone = file_path.clone();
    let filename_clone = filename.clone();
    let file_tag_clone = file_tag.clone();
    let total_chunks_clone = total_chunks;
    let file_size_bytes_clone = file_size_bytes;
    let target_address_clone = target_address;
    let start_wait = Instant::now();
    let mut reply_message = None;

    while start_wait.elapsed() < Duration::from_secs(REPLY_TIMEOUT_SECS) {
        if let Some(msgs) = client.wait_for_messages().await {
            for msg in msgs {
                if msg.message.is_empty() {
                    continue;
                }
                let msg_str = String::from_utf8_lossy(&msg.message);
                if msg_str.starts_with(RESEND_REQUEST_PREFIX) {
                    if let Some(chunk_idx_str) = msg_str.strip_prefix(RESEND_REQUEST_PREFIX) {
                        if let Ok(chunk_idx) = chunk_idx_str.parse::<usize>() {
                            println!("Resend request received for chunk {}", chunk_idx + 1);
                            let open_result = File::open(&file_path_clone);
                            let seek_result = open_result.and_then(|mut f| {
                                let offset = chunk_idx as u64 * CHUNK_SIZE as u64;
                                f.seek(SeekFrom::Start(offset))?;
                                let mut buffer = vec![0u8; CHUNK_SIZE];
                                let bytes_read = f.read(&mut buffer)?;
                                Ok((buffer, bytes_read))
                            });
                            match seek_result {
                                Ok((buffer, bytes_read)) => {
                                    let chunk_data = &buffer[..bytes_read];
                                    let chunk_header = build_chunk_header(chunk_idx, total_chunks_clone, file_size_bytes_clone, &filename_clone, &file_tag_clone);
                                    let mut chunk_payload = chunk_header;
                                    chunk_payload.extend_from_slice(chunk_data);
                                    if let Err(e) = client.send_plain_message(target_address_clone, chunk_payload).await {
                                        eprintln!("Failed to resend chunk {}: {}", chunk_idx + 1, e);
                                    } else {
                                        println!("  ✓ Chunk {} resent", chunk_idx + 1);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to read chunk {} from file: {}", chunk_idx + 1, e);
                                }
                            }
                        }
                    }
                } else {
                    reply_message = Some(msg);
                    break;
                }
            }
            if reply_message.is_some() {
                break;
            }
        }
    }

    let reply_message = match reply_message {
        Some(msg) => msg,
        None => {
            println!("Timeout: No reply received. The receiver likely has an incomplete file.");
            client.disconnect().await;
            let elapsed = start_time.elapsed();
            println!("Done!");
            println!("Total time: {}", format_duration(elapsed));
            return Ok(());
        }
    };

    if let Some(tag) = &reply_message.sender_tag {
        println!("Reply Anonymous Sender Tag (SURB): {}", tag);
    }

    let (_reply_file_tag, reply_hash) = match parse_reply_payload(&reply_message.message) {
        Ok(parsed) => parsed,
        Err(e) => {
            println!("Failed to parse reply: {}", e);
            client.disconnect().await;
            let elapsed = start_time.elapsed();
            println!("Done!");
            println!("Total time: {}", format_duration(elapsed));
            return Ok(());
        }
    };

    println!("Receiver hash: {}", reply_hash);
    if sender_hash == reply_hash {
        println!("Hashes match!");
    } else {
        println!("Hashes do not match!");
    }

    client.disconnect().await;
    let elapsed = start_time.elapsed();
    println!("Done!");
    println!("Total time: {}", format_duration(elapsed));
    Ok(())
}

pub async fn send_parts_mode(address: String, prefix: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    let prefix_str = prefix.to_string_lossy().to_string();
    println!("=== Parts Mode ===");
    println!("Looking for files with prefix: {}", prefix_str);

    let expected_parts = parse_ripemd_file()?;
    if expected_parts.is_empty() {
        return Err("No parts found in ripemd-160.txt".into());
    }
    println!("Found {} expected parts in ripemd-160.txt\n", expected_parts.len());

    let mut part_files: Vec<(String, String, String)> = Vec::new();
    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap().to_string_lossy().to_string();
            if filename.starts_with(&prefix_str) {
                if let Some(expected_hash) = expected_parts.get(&filename) {
                    part_files.push((filename, expected_hash.clone(), path.to_string_lossy().to_string()));
                }
            }
        }
    }

    if part_files.is_empty() {
        return Err(format!("No files found matching prefix '{}'", prefix_str).into());
    }

    part_files.sort_by(|a, b| a.0.cmp(&b.0));
    println!("Found {} part files to send:\n", part_files.len());
    for (i, (filename, _, _)) in part_files.iter().enumerate() {
        println!("  {}. {}", i + 1, filename);
    }
    println!();

    let target_address: Recipient = match address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Error: Invalid Nym address format");
            eprintln!("Details: {}", e);
            return Err("Invalid recipient address".into());
        }
    };

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

    let our_address = client.nym_address();
    println!("Your Nym address: {}", our_address);
    println!("Sending anonymously to: {}\n", address);

    let mut successful_parts = 0;
    let mut failed_parts = Vec::new();

    for (i, (filename, expected_hash, file_path_str)) in part_files.iter().enumerate() {
        let part_start_time = Instant::now();
        println!("\n========================================");
        println!("Sending part {}/{}: {}", i + 1, part_files.len(), filename);
        println!("========================================\n");

        let file_path = PathBuf::from(file_path_str);
        match send_single_part(&mut client, target_address, &file_path, expected_hash).await {
            Ok(true) => {
                successful_parts += 1;
                let part_elapsed = part_start_time.elapsed();
                let total_elapsed = start_time.elapsed();
                println!("\n✓ Part {} completed successfully", i + 1);
                println!("  Part time: {}", format_duration(part_elapsed));
                println!("  Total elapsed: {}\n", format_duration(total_elapsed));
            }
            Ok(false) => {
                failed_parts.push((i + 1, filename.clone(), "Hash mismatch".to_string()));
                println!("\n✗ Part {} failed: Hash mismatch\n", i + 1);
            }
            Err(e) => {
                failed_parts.push((i + 1, filename.clone(), e.to_string()));
                println!("\n✗ Part {} failed: {}\n", i + 1, e);
            }
        }

        if i < part_files.len() - 1 {
            println!("Waiting 2 seconds before next part...");
            sleep(Duration::from_secs(2)).await;
        }
    }

    println!("\n========================================");
    println!("=== Parts Transfer Summary ===");
    println!("========================================");
    println!("Total parts: {}", part_files.len());
    println!("Successful: {}", successful_parts);
    println!("Failed: {}", failed_parts.len());

    if !failed_parts.is_empty() {
        println!("\nFailed parts:");
        for (num, filename, error) in &failed_parts {
            println!("  Part {}: {} - {}", num, filename, error);
        }
    }

    client.disconnect().await;
    let elapsed = start_time.elapsed();
    println!("\nDone!");
    println!("Total time: {}", format_duration(elapsed));
    Ok(())
}

async fn send_single_part(
    client: &mut nym_sdk::mixnet::MixnetClient,
    target_address: Recipient,
    file_path: &PathBuf,
    expected_hash: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let file_metadata = std::fs::metadata(file_path)?;
    let file_size_bytes = file_metadata.len();
    let file_size_mib = file_size_bytes / 1024 / 1024;
    let total_chunks = ((file_size_bytes as f64) / (CHUNK_SIZE as f64)).ceil() as usize;
    let filename = file_path
        .file_name()
        .ok_or("Invalid filename")?
        .to_string_lossy()
        .to_string();

    println!("File size: {} MiB", file_size_mib);
    println!("Original filename: {}", filename);
    println!("Splitting into {} chunks of {} KB each", total_chunks, CHUNK_SIZE / 1024);

    let sender_hash = ripemd160_file(file_path)?;
    println!("Sender hash: {}", sender_hash);
    println!("Expected hash: {}", expected_hash);

    if sender_hash != expected_hash {
        return Err("File hash does not match expected hash from ripemd-160.txt".into());
    }

    let file_tag = generate_file_tag();
    println!("\nSending handshake...");
    let handshake_payload = build_handshake_payload(&file_tag);
    if let Err(e) = client.send_plain_message(target_address, handshake_payload).await {
        return Err(format!("Failed to send handshake: {}", e).into());
    }

    println!("  Waiting up to {}s for recipient response...", HANDSHAKE_TIMEOUT_SECS);
    let handshake_result = timeout(Duration::from_secs(HANDSHAKE_TIMEOUT_SECS), async {
        loop {
            if let Some(msgs) = client.wait_for_messages().await {
                if let Some(msg) = msgs.into_iter().find(|m| !m.message.is_empty()) {
                    return msg;
                }
            }
        }
    })
    .await;

    let handshake_ok = match handshake_result {
        Ok(reply) => {
            let reply_str = String::from_utf8_lossy(&reply.message);
            let expected_prefix = format!("READY:{}", file_tag);
            if reply_str.starts_with(&expected_prefix) {
                println!("  ✓ Recipient is online and ready!\n");
                true
            } else {
                println!("  ✗ Unexpected response from recipient");
                false
            }
        }
        Err(_) => {
            println!("  ✗ No response within {}s", HANDSHAKE_TIMEOUT_SECS);
            false
        }
    };

    if !handshake_ok {
        return Err("Recipient appears to be offline".into());
    }

    let overall_pb = ProgressBar::new(total_chunks as u64);
    overall_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan/blue} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} chunks",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    overall_pb.set_message("Sending chunks");

    let mut failed_chunks = Vec::new();
    let mut file = File::open(file_path)?;

    for chunk_idx in 0..total_chunks {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let bytes_read = file.read(&mut buffer)?;
        let chunk_data = &buffer[..bytes_read];

        for attempt in 0..MAX_RETRIES {
            let chunk_header = build_chunk_header(chunk_idx, total_chunks, file_size_bytes, &filename, &file_tag);
            let mut chunk_payload = chunk_header;
            chunk_payload.extend_from_slice(chunk_data);

            match client.send_plain_message(target_address, chunk_payload).await {
                Ok(_) => {
                    if attempt > 0 {
                        println!("\n✓ Chunk {} succeeded on attempt {}", chunk_idx + 1, attempt + 1);
                    }
                    break;
                }
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        eprintln!(
                            "\nChunk {} failed (attempt {}/{}): {}. Retrying...",
                            chunk_idx + 1,
                            attempt + 1,
                            MAX_RETRIES,
                            e
                        );
                        sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
                    } else {
                        eprintln!(
                            "\nChunk {} failed after {} attempts: {}",
                            chunk_idx + 1,
                            MAX_RETRIES,
                            e
                        );
                        failed_chunks.push(chunk_idx);
                    }
                }
            }
        }
        overall_pb.inc(1);
        sleep(Duration::from_millis(50)).await;
    }

    overall_pb.finish_with_message("All chunks sent");

    if !failed_chunks.is_empty() {
        return Err(format!("{} chunks failed to send", failed_chunks.len()).into());
    }

    println!("\nWaiting for reply or resend requests...\n");

    let file_path_clone = file_path.clone();
    let filename_clone = filename.clone();
    let file_tag_clone = file_tag.clone();
    let total_chunks_clone = total_chunks;
    let file_size_bytes_clone = file_size_bytes;
    let target_address_clone = target_address;
    let start_wait = Instant::now();
    let mut reply_message = None;

    while start_wait.elapsed() < Duration::from_secs(REPLY_TIMEOUT_SECS) {
        if let Some(msgs) = client.wait_for_messages().await {
            for msg in msgs {
                if msg.message.is_empty() {
                    continue;
                }
                let msg_str = String::from_utf8_lossy(&msg.message);
                if msg_str.starts_with(RESEND_REQUEST_PREFIX) {
                    if let Some(chunk_idx_str) = msg_str.strip_prefix(RESEND_REQUEST_PREFIX) {
                        if let Ok(chunk_idx) = chunk_idx_str.parse::<usize>() {
                            println!("Resend request received for chunk {}", chunk_idx + 1);
                            let open_result = File::open(&file_path_clone);
                            let seek_result = open_result.and_then(|mut f| {
                                let offset = chunk_idx as u64 * CHUNK_SIZE as u64;
                                f.seek(SeekFrom::Start(offset))?;
                                let mut buffer = vec![0u8; CHUNK_SIZE];
                                let bytes_read = f.read(&mut buffer)?;
                                Ok((buffer, bytes_read))
                            });
                            match seek_result {
                                Ok((buffer, bytes_read)) => {
                                    let chunk_data = &buffer[..bytes_read];
                                    let chunk_header = build_chunk_header(chunk_idx, total_chunks_clone, file_size_bytes_clone, &filename_clone, &file_tag_clone);
                                    let mut chunk_payload = chunk_header;
                                    chunk_payload.extend_from_slice(chunk_data);
                                    if let Err(e) = client.send_plain_message(target_address_clone, chunk_payload).await {
                                        eprintln!("Failed to resend chunk {}: {}", chunk_idx + 1, e);
                                    } else {
                                        println!("  ✓ Chunk {} resent", chunk_idx + 1);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to read chunk {} from file: {}", chunk_idx + 1, e);
                                }
                            }
                        }
                    }
                } else {
                    reply_message = Some(msg);
                    break;
                }
            }
            if reply_message.is_some() {
                break;
            }
        }
    }

    let reply_message = match reply_message {
        Some(msg) => msg,
        None => {
            return Err("Timeout: No reply received".into());
        }
    };

    let (_reply_file_tag, reply_hash) = match parse_reply_payload(&reply_message.message) {
        Ok(parsed) => parsed,
        Err(e) => {
            return Err(format!("Failed to parse reply: {}", e).into());
        }
    };

    println!("Receiver hash: {}", reply_hash);
    if sender_hash == reply_hash {
        println!("✓ Hashes match!");
        Ok(true)
    } else {
        println!("✗ Hashes do not match!");
        Ok(false)
    }
}

fn parse_ripemd_file() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string("ripemd-160.txt")?;
    let mut parts = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("RIPEMD") || line.starts_with("Operation") || 
           line.starts_with("Original") || line.starts_with("Parts") || line.starts_with("---") {
            continue;
        }
        if let Some(colon_pos) = line.rfind(':') {
            let hash = line[colon_pos + 1..].trim().to_string();
            let before_colon = &line[..colon_pos];
            if let Some(paren_pos) = before_colon.rfind('(') {
                let filename = before_colon[..paren_pos].trim().to_string();
                parts.insert(filename, hash);
            }
        }
    }
    Ok(parts)
}

fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let minutes = total_secs / 60;
    let seconds = total_secs % 60;
    if minutes > 0 {
        format!("{}m {:02}s", minutes, seconds)
    } else {
        format!("{}.{:03}s", seconds, duration.subsec_millis())
    }
}

fn build_handshake_payload(file_tag: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&HANDSHAKE_CHUNK_IDX.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes());
    payload.extend_from_slice(&0u64.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(file_tag.as_bytes());
    payload
}

fn build_chunk_header(chunk_idx: usize, total_chunks: usize, file_size: u64, filename: &str, file_tag: &str) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&(chunk_idx as u32).to_be_bytes());
    header.extend_from_slice(&(total_chunks as u32).to_be_bytes());
    header.extend_from_slice(&file_size.to_be_bytes());
    let filename_bytes = filename.as_bytes();
    header.extend_from_slice(&(filename_bytes.len() as u16).to_be_bytes());
    header.extend_from_slice(filename_bytes);
    header.extend_from_slice(file_tag.as_bytes());
    header
}

fn parse_reply_payload(payload: &[u8]) -> Result<(String, String), &'static str> {
    if payload.len() < FILE_TAG_LEN {
        return Err("Reply payload too short");
    }
    let file_tag = String::from_utf8_lossy(&payload[..FILE_TAG_LEN]).to_string();
    let hash = String::from_utf8_lossy(&payload[FILE_TAG_LEN..]).to_string();
    Ok((file_tag, hash))
}

fn generate_file_tag() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..FILE_TAG_LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

fn ripemd160_file(path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut hasher = Ripemd160::new();
    let mut buffer = [0u8; 65536];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(hex::encode(hasher.finalize()))
}
