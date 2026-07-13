use crate::config::{load_config};
use crate::ssh::{connect_ssh, parse_host};
use indicatif::{ProgressBar, ProgressStyle};
use ssh2::Session;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;

pub async fn run_get_mode() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config();

    let host_str = config.host.as_deref();
    let port = config.port.unwrap_or(22);
    let proxy_str = config.socks5_proxy.as_deref();

    let (host, mut username) = if let Some(h) = host_str {
        parse_host(h)
    } else {
        return Err("No host specified. Provide it via nymx.json".into());
    };

    if username.is_empty() {
        if let Some(u) = config.username {
            username = u;
        } else {
            print!("Username: ");
            std::io::stdout().flush()?;
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            username = input.trim().to_string();
        }
    }

    if username.is_empty() || host.is_empty() {
        return Err("Username and host are required".into());
    }

    let remote_dir = format!("/home/{}/received", username);
    print!("Password for {}@{}: ", username, host);
    std::io::stdout().flush()?;
    let password = rpassword::read_password()?;
    println!();

    let session = connect_ssh(&host, port, &username, &password, proxy_str)?;
    drop(password);

    let check_cmd = format!("test -d {} && echo 'exists' || echo 'not'", remote_dir);
    let output = run_remote_command(&session, &check_cmd).await?;
    if output.trim() != "exists" {
        println!("Directory {} does not exist on remote server", remote_dir);
        return Ok(());
    }

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}")
            .unwrap()
            .tick_chars("⠁⠁⠉⠙⠚⠒⠂⠂⠒⠲⠴⠤⠄⠄⠤⠠⠠⠤⠦⠖⠒⠐⠐⠒⠓⠋⠉⠈⠈")
    );
    spinner.set_message("Checking for files...");

    let in_use_files = get_in_use_files(&session, &remote_dir).await?;
    let files = list_remote_files(&session, &remote_dir).await?;
    spinner.finish_with_message("Ready");

    println!("Found {} files", files.len());
    if files.is_empty() {
        println!("No files to download");
        return Ok(());
    }

    let local_dir = PathBuf::from(".");
    let mut downloaded = 0;
    let mut failed = 0;
    let mut deleted = 0;
    let mut skipped = 0;

    let overall_pb = ProgressBar::new(files.len() as u64);
    overall_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({eta})"
        )
        .unwrap()
        .progress_chars("#>-")
    );
    overall_pb.set_message("Downloading files");

    for filename in &files {
        let is_in_use = in_use_files.iter().any(|path| path.ends_with(filename));
        if is_in_use {
            skipped += 1;
            overall_pb.inc(1);
            continue;
        }

        match download_file(&session, &remote_dir, &local_dir, filename).await {
            Ok(_) => {
                downloaded += 1;
                let remote_file = format!("{}/{}", remote_dir, filename);
                match delete_file_remote(&session, &remote_file).await {
                    Ok(_) => {
                        deleted += 1;
                    }
                    Err(e) => {
                        failed += 1;
                        eprintln!("\nFailed to delete {}: {}", filename, e);
                    }
                }
            }
            Err(e) => {
                failed += 1;
                eprintln!("\nFailed to download {}: {}", filename, e);
            }
        }
        overall_pb.inc(1);
        sleep(Duration::from_millis(50)).await;
    }

    overall_pb.finish_with_message("Done");
    println!("\nSummary:");
    println!("  Downloaded: {}", downloaded);
    println!("  Deleted: {}", deleted);
    println!("  Failed: {}", failed);
    println!("  Skipped: {}", skipped);
    println!("  Total: {}", files.len());

    Ok(())
}

async fn run_remote_command(
    session: &Session,
    command: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut channel = session.channel_session()?;
    channel.exec(command)?;
    let mut output = String::new();
    channel.read_to_string(&mut output)?;
    channel.wait_close()?;
    Ok(output)
}

async fn get_in_use_files(
    session: &Session,
    remote_path: &str,
) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let command = format!("lsof +D {} | awk 'NR >1 {{print $9}}' | sort -u", remote_path);
    let output = run_remote_command(session, &command).await?;
    let in_use_files: HashSet<String> = output
        .lines()
        .filter(|line| !line.is_empty() && line.starts_with('/'))
        .map(String::from)
        .collect();
    Ok(in_use_files)
}

async fn list_remote_files(
    session: &Session,
    remote_path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let command = format!("find {} -maxdepth 1 -type f -printf '%f\n'", remote_path);
    let output = run_remote_command(session, &command).await?;
    let files: Vec<String> = output
        .lines()
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect();
    Ok(files)
}

async fn get_file_size_remote(
    session: &Session,
    remote_path: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let command = format!("stat -c %s {}", remote_path);
    let output = run_remote_command(session, &command).await?;
    let size = output.trim().parse::<u64>()?;
    Ok(size)
}

async fn delete_file_remote(
    session: &Session,
    remote_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = format!("rm -f {}", remote_path);
    let _output = run_remote_command(session, &command).await?;
    Ok(())
}

async fn download_file(
    session: &Session,
    remote_path: &str,
    local_dir: &PathBuf,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let remote_full = format!("{}/{}", remote_path, filename);
    let local_path = local_dir.join(filename);
    let file_size = get_file_size_remote(session, &remote_full).await?;

    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})"
        )
        .unwrap()
        .progress_chars("#>-")
    );
    pb.set_message(format!("Downloading {}", filename));

    let (mut channel, _) = session.scp_recv(Path::new(&remote_full))?;
    let mut local_file = std::fs::File::create(&local_path)?;
    let mut buffer = vec![0u8; 32768];
    let mut downloaded = 0u64;

    loop {
        let n = channel.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        local_file.write_all(&buffer[..n])?;
        downloaded += n as u64;
        pb.set_position(downloaded);
    }

    channel.send_eof()?;
    channel.wait_eof()?;
    channel.close()?;
    channel.wait_close()?;
    pb.finish_with_message(format!("Downloaded {}", filename));
    Ok(())
}
