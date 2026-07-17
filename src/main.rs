mod config;
mod get;
mod receive;
mod send;
mod ssh;

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about = "NymX - Anonymous data exchange leveraging SURBs via the Nym Mixnet", long_about = None)]
struct Cli {
    #[arg(short = 'r')]
    receive: bool,
    #[arg(short = 's')]
    send: bool,
    #[arg(short = 'g')]
    get: bool,
    #[arg(short = 'p', long = "path")]
    path: Option<PathBuf>,
    #[arg(long = "part")]
    part: bool,
    #[arg(required_if_eq("send", "true"))]
    target: Option<String>,
    #[arg(required_if_eq("send", "true"))]
    file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.receive {
        receive::receive_mode(cli.path).await?;
    } else if cli.send {
        if let (Some(target), Some(file_path)) = (cli.target, cli.file) {
            let config = config::Config::load();
            let address = config.resolve(&target).unwrap_or(target);
            
            if cli.part {
                send::send_parts_mode(address, file_path).await?;
            } else {
                send::send_mode(address, file_path).await?;
            }
        }
    } else if cli.get {
        get::run_get_mode().await?;
    } else {
        print_help();
    }

    Ok(())
}

fn print_help() {
    println!("NymX - (c) 2026 Ch1ffr3punk");
    println!("Anonymous data exchange leveraging SURBs via the Nym Mixnet");
    println!();
    println!("Usage:");
    println!("  nymx -r [-p <path>]               - Receive mode: Listen for incoming files");
    println!("                                      -p: Path to save files (default: ./received)");
    println!("  nymx -s <target> <file>           - Send mode: Send a file anonymously");
    println!("  nymx -s --part <target> <prefix>  - Send parts mode: Send multiple parts sequentially");
    println!("  nymx -g                           - Get mode: Download files from SSH server via Tor");
    println!();
    println!("Examples:");
    println!("  nymx -r");
    println!("  nymx -r -p /var/spool/received");
    println!("  nymx -s AliceNymAddress document.pdf");
    println!("  nymx -s alice document.pdf (using alias from nymx.json)");
    println!("  nymx -s --part alice movie.mp4.part");
    println!("  nymx -g");
    println!();
    println!("Example nymx.json (for -s and -g):");
    println!("  {{");
    println!("    \"aliases\": {{");
    println!("      \"alice\": \"AliceNymAddress\",");
    println!("      \"bob\": \"BobNymAddress\",");
    println!("      \"carol\": \"CarolNymAddress\"");
    println!("    }},");
    println!("    \"ssh\": {{");
    println!("      \"host\": \"abcdef1234567890.onion\",");
    println!("      \"port\": 22,");
    println!("      \"username\": \"Ch1ffr3punk\",");
    println!("      \"socks5_proxy\": \"127.0.0.1:9050\"");
    println!("    }}");
    println!("  }}");
    println!();
    println!("Note: The receiver (-r) does NOT need nymx.json. Use -p to specify the save path.");
    println!("Note: For --part mode, ensure sam's ripemd-160.txt exists in the current directory.");
}
