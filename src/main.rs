mod alias;
mod config;
mod get;
mod network;
mod protocol;
mod receive;
mod send;
mod ssh;
mod utils;

use anyhow::{bail, Context, Result};
use clap::Parser;
use nym_sdk::mixnet::Recipient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use alias::resolve_alias;
use config::load_config;
use network::NymNode;
use receive::run_receive_mode;
use send::send_file;

const VERSION: &str = "v0.1.2";
const COPYRIGHT: &str = "(c) 2026 Ch1ffr3punk";
const DEFAULT_CHUNK_SIZE_KB: usize = 64;
const DEFAULT_CHUNKS_PER_SEC: f64 = 1.0;

#[derive(Parser, Debug)]
#[command(
    name = "NymX",
    version = VERSION,
    about = "A data exchange tool for the Nym Mixnet"
)]
struct Cli {
    #[arg(short = 't', long = "receiver")]
    receiver_addr: Option<String>,
    #[arg(short = 'r', long = "receive")]
    receive_mode: bool,
    #[arg(short = 'g', long = "get-files", help = "Download files from ~/received/ on remote server via SSH")]
    get_files: bool,
    #[arg(short = 'o', long = "out", default_value = "./received")]
    output_dir: String,
    #[arg(short = 'c', long = "chunk-size", default_value_t = DEFAULT_CHUNK_SIZE_KB)]
    chunk_size_kb: usize,
    #[arg(short = 'R', long = "rate", default_value_t = DEFAULT_CHUNKS_PER_SEC)]
    chunks_per_second: f64,
    #[arg(short = 'a', long = "alias")]
    alias_name: Option<String>,
    #[arg(short = 'w', long = "whitelist")]
    whitelist: Option<String>,
    #[arg(short = 'q', long = "quota")]
    quota_mib: Option<u64>,
    #[arg(value_name = "FILE")]
    file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.get_files {
        if let Err(e) = get::run_get_mode().await {
            eprintln!("\n[Error] Get files operation failed: {}", e);
        }
        return Ok(());
    }

    let config = if !cli.receive_mode {
        load_config()
    } else {
        config::NymxConfig::default()
    };

    if !cli.receive_mode {
        if cli.chunk_size_kb < 1 {
            bail!("Chunk size must be at least 1 KiB");
        }
        if cli.chunk_size_kb > 1024 {
            eprintln!("[Warning] Chunk size above 1 MiB may cause issues");
        }
        if cli.chunks_per_second <= 0.0 {
            bail!("Rate must be positive");
        }
        if cli.quota_mib.is_some() {
            eprintln!("[Warning] -q/--quota is only effective in receive mode (-r)");
        }
    } else {
        if let Some(q) = cli.quota_mib {
            if q == 0 {
                bail!("Quota must be greater than 0 MiB");
            }
        }
    }

    let mut recipients: Vec<Recipient> = Vec::new();

    if let Some(ref addrs) = cli.receiver_addr {
        for addr in addrs.split(',') {
            let trimmed = addr.trim();
            if !trimmed.is_empty() {
                let recipient: Recipient = trimmed
                    .parse()
                    .with_context(|| format!("Invalid Nym recipient address: '{}'", trimmed))?;
                recipients.push(recipient);
            }
        }
    }

    if let Some(ref aliases) = cli.alias_name {
        for alias in aliases.split(',') {
            let trimmed = alias.trim();
            if !trimmed.is_empty() {
                let addr = resolve_alias(trimmed)?;
                println!("Resolved alias '{}' -> {}", trimmed, addr);
                let recipient: Recipient = addr
                    .parse()
                    .with_context(|| format!("Invalid Nym address from alias '{}'", trimmed))?;
                recipients.push(recipient);
            }
        }
    } else if cli.alias_name.is_none() && cli.receiver_addr.is_none() {
        if let Some(ref config_aliases) = config.aliases {
            for (alias_name, alias_addr) in config_aliases {
                println!("Resolved alias '{}' -> {}", alias_name, alias_addr);
                let recipient: Recipient = alias_addr
                    .parse()
                    .with_context(|| format!("Invalid Nym address from config alias '{}'", alias_name))?;
                recipients.push(recipient);
            }
        }
    }

    if !cli.receive_mode {
        let bandwidth = cli.chunk_size_kb as f64 * cli.chunks_per_second;
        println!("NymX {} {}", VERSION, COPYRIGHT);
        println!(
            "Configuration: Chunk size = {} KiB, Rate = {:.1} chunks/s ({:.1} KiB/s)",
            cli.chunk_size_kb, cli.chunks_per_second, bandwidth
        );
        println!("Total target recipients: {}", recipients.len());
    } else {
        println!("NymX {} {}", VERSION, COPYRIGHT);
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        ctrlc::set_handler(move || {
            eprintln!("\n[Shutdown] Interrupt received, stopping...");
            r.store(false, Ordering::SeqCst);
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_secs(3));
                std::process::exit(0);
            });
        })
        .context("Failed to set Ctrl+C handler")?;
    }

    let nym = Arc::new(tokio::sync::Mutex::new(NymNode::connect().await?));

    let result = if cli.receive_mode {
        run_receive_mode(
            nym.clone(),
            &cli.output_dir,
            running,
            cli.whitelist.as_deref(),
            cli.quota_mib,
        )
        .await
    } else if !recipients.is_empty() {
        let file_path = cli
            .file
            .as_ref()
            .context("Exactly one file must be specified for sending")?;
        let chunk_size_bytes = cli.chunk_size_kb * 1024;
        let mut final_res = Ok(());
        for (index, recipient) in recipients.iter().enumerate() {
            if !running.load(Ordering::Relaxed) {
                println!("[Aborted] Send loop canceled early.");
                break;
            }
            println!(
                "\nProgress: Sending to recipient {} of {}\n\
                Target: {}",
                index + 1,
                recipients.len(),
                recipient
            );
            let upload_res = send_file(
                nym.clone(),
                recipient,
                file_path,
                chunk_size_bytes,
                cli.chunks_per_second,
                &running,
            )
            .await;
            if let Err(e) = &upload_res {
                eprintln!("\n[Error] Transfer failed for recipient {}: {}", recipient, e);
                final_res = upload_res;
            }
        }
        final_res
    } else {
        println!("\nPlease specify one of:");
        println!("  -r [-w <file>] [-q <MiB>]    receive mode (optional whitelist & quota)");
        println!("  -t <addr1,addr2> <file>      send file to multiple addresses");
        println!("  -a <alias1,alias2> <file>    send file via multiple aliases");
        println!("  -g                           get files from remote server via SSH");
        println!("Run with --help for usage information.");
        Ok(())
    };

    if let Err(e) = result {
        eprintln!("\n[Error] Operation failed: {}", e);
    }

    let nym_node = match Arc::try_unwrap(nym) {
        Ok(mutex) => mutex.into_inner(),
        Err(_) => {
            eprintln!("[Warning] Could not cleanly disconnect - other references exist");
            return Ok(());
        }
    };
    nym_node.disconnect().await;

    Ok(())
}
