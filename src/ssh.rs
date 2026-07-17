use ssh2::Session;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

pub fn socks5_connect<A: ToSocketAddrs>(
    proxy_addr: A,
    target_host: &str,
    target_port: u16,
) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(proxy_addr)?;
    stream.write_all(&[0x05, 0x01, 0x00])?;
    let mut response = [0u8; 2];
    stream.read_exact(&mut response)?;
    if response[0] != 0x05 || response[1] != 0x00 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "SOCKS5 handshake failed",
        ));
    }
    let mut request = vec![0x05, 0x01, 0x00, 0x03];
    request.push(target_host.len() as u8);
    request.extend_from_slice(target_host.as_bytes());
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request)?;
    let mut response = [0u8; 4];
    stream.read_exact(&mut response)?;
    if response[0] != 0x05 || response[1] != 0x00 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "SOCKS5 CONNECT failed",
        ));
    }
    match response[3] {
        0x01 => {
            let mut buf = [0u8; 6];
            stream.read_exact(&mut buf)?;
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len)?;
            let mut buf = vec![0u8; len[0] as usize + 2];
            stream.read_exact(&mut buf)?;
        }
        0x04 => {
            let mut buf = [0u8; 18];
            stream.read_exact(&mut buf)?;
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid SOCKS5 response ATYP",
            ));
        }
    }
    Ok(stream)
}

pub fn connect_ssh(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    proxy: Option<&str>,
) -> Result<Session, Box<dyn std::error::Error>> {
    eprintln!("Connecting to {}@{}:{}...", username, host, port);
    let mut session = Session::new()?;
    if let Some(proxy_addr) = proxy {
        eprintln!("Using SOCKS5 proxy: {}", proxy_addr);
        let stream = socks5_connect(proxy_addr, host, port)?;
        session.set_tcp_stream(stream);
    } else {
        let tcp = TcpStream::connect((host, port))?;
        session.set_tcp_stream(tcp);
    }
    session.handshake()?;
    session.userauth_password(username, password)?;
    if !session.authenticated() {
        return Err("Authentication failed".into());
    }
    eprintln!("Connected successfully");
    Ok(session)
}
