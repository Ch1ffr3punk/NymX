use anyhow::{bail, Context, Result};

pub const MSG_TYPE_FILE_INFO: u8 = 0x01;
pub const MSG_TYPE_FILE_CHUNK: u8 = 0x02;
pub const MSG_TYPE_RESEND_REQUEST: u8 = 0x03;
pub const MSG_TYPE_TRANSFER_COMPLETE: u8 = 0x04;
pub const MSG_TYPE_TRANSFER_OFFER: u8 = 0x05;
pub const MSG_TYPE_TRANSFER_ACCEPT: u8 = 0x06;

pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub chunks: u32,
    pub sender_address: String,
}

pub struct FileChunk {
    pub index: u32,
    pub total: u32,
    pub data: Vec<u8>,
    pub is_last: bool,
}

#[derive(Debug)]
pub struct ResendRequest {
    pub file_name: String,
    pub missing_indices: Vec<u32>,
}

#[derive(Debug)]
pub struct TransferComplete {
    pub file_name: String,
    pub ripemd160_hash: [u8; 20],
}

pub struct TransferOffer {
    pub file_name: String,
    pub size: u64,
    pub sender_address: String,
}

pub struct TransferAccept {
    pub file_name: String,
    pub accepted: bool,
}

pub enum BinaryMessage {
    FileInfo(FileInfo),
    FileChunk(FileChunk),
    ResendRequest(ResendRequest),
    TransferComplete(TransferComplete),
    TransferOffer(TransferOffer),
    TransferAccept(TransferAccept),
}

fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    write_u32(buf, data.len() as u32);
    buf.extend_from_slice(data);
}

fn write_bool(buf: &mut Vec<u8>, value: bool) {
    buf.push(if value { 1 } else { 0 });
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > data.len() {
        bail!("Buffer underflow reading u32");
    }
    let value = u32::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ]);
    *offset += 4;
    Ok(value)
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64> {
    if *offset + 8 > data.len() {
        bail!("Buffer underflow reading u64");
    }
    let value = u64::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn read_string(data: &[u8], offset: &mut usize) -> Result<String> {
    let len = read_u32(data, offset)? as usize;
    if *offset + len > data.len() {
        bail!("Buffer underflow reading string");
    }
    let s = String::from_utf8(data[*offset..*offset + len].to_vec())
        .context("Invalid UTF-8 in string")?;
    *offset += len;
    Ok(s)
}

fn read_bytes(data: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
    let len = read_u32(data, offset)? as usize;
    if *offset + len > data.len() {
        bail!("Buffer underflow reading bytes");
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

fn read_bool(data: &[u8], offset: &mut usize) -> Result<bool> {
    if *offset >= data.len() {
        bail!("Buffer underflow reading bool");
    }
    let value = data[*offset] != 0;
    *offset += 1;
    Ok(value)
}

fn read_fixed_bytes<const N: usize>(data: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    if *offset + N > data.len() {
        bail!("Buffer underflow reading fixed bytes");
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&data[*offset..*offset + N]);
    *offset += N;
    Ok(arr)
}

pub fn serialize_file_info(info: &FileInfo) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_FILE_INFO);
    write_string(&mut buf, &info.name);
    write_u64(&mut buf, info.size);
    write_u32(&mut buf, info.chunks);
    write_string(&mut buf, &info.sender_address);
    buf
}

pub fn serialize_file_chunk(chunk: &FileChunk) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_FILE_CHUNK);
    write_u32(&mut buf, chunk.index);
    write_u32(&mut buf, chunk.total);
    write_bytes(&mut buf, &chunk.data);
    write_bool(&mut buf, chunk.is_last);
    buf
}

pub fn serialize_resend_request(req: &ResendRequest) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_RESEND_REQUEST);
    write_string(&mut buf, &req.file_name);
    write_u32(&mut buf, req.missing_indices.len() as u32);
    for &index in &req.missing_indices {
        write_u32(&mut buf, index);
    }
    buf
}

pub fn serialize_transfer_complete(tc: &TransferComplete) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_TRANSFER_COMPLETE);
    write_string(&mut buf, &tc.file_name);
    buf.extend_from_slice(&tc.ripemd160_hash);
    buf
}

pub fn serialize_transfer_offer(offer: &TransferOffer) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_TRANSFER_OFFER);
    write_string(&mut buf, &offer.file_name);
    write_u64(&mut buf, offer.size);
    write_string(&mut buf, &offer.sender_address);
    buf
}

pub fn serialize_transfer_accept(accept: &TransferAccept) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(MSG_TYPE_TRANSFER_ACCEPT);
    write_string(&mut buf, &accept.file_name);
    write_bool(&mut buf, accept.accepted);
    buf
}

pub fn hash_to_hex(hash: &[u8; 20]) -> String {
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn deserialize_message(data: &[u8]) -> Result<BinaryMessage> {
    if data.is_empty() {
        bail!("Empty message");
    }
    let msg_type = data[0];
    let mut offset = 1;

    match msg_type {
        MSG_TYPE_FILE_INFO => {
            let name = read_string(data, &mut offset)?;
            let size = read_u64(data, &mut offset)?;
            let chunks = read_u32(data, &mut offset)?;
            let sender_address = read_string(data, &mut offset)?;
            Ok(BinaryMessage::FileInfo(FileInfo {
                name,
                size,
                chunks,
                sender_address,
            }))
        }
        MSG_TYPE_FILE_CHUNK => {
            let index = read_u32(data, &mut offset)?;
            let total = read_u32(data, &mut offset)?;
            let chunk_data = read_bytes(data, &mut offset)?;
            let is_last = read_bool(data, &mut offset)?;
            Ok(BinaryMessage::FileChunk(FileChunk {
                index,
                total,
                data: chunk_data,
                is_last,
            }))
        }
        MSG_TYPE_RESEND_REQUEST => {
            let file_name = read_string(data, &mut offset)?;
            let count = read_u32(data, &mut offset)? as usize;
            let mut missing_indices = Vec::with_capacity(count);
            for _ in 0..count {
                missing_indices.push(read_u32(data, &mut offset)?);
            }
            Ok(BinaryMessage::ResendRequest(ResendRequest {
                file_name,
                missing_indices,
            }))
        }
        MSG_TYPE_TRANSFER_COMPLETE => {
            let file_name = read_string(data, &mut offset)?;
            let ripemd160_hash = read_fixed_bytes::<20>(data, &mut offset)?;
            Ok(BinaryMessage::TransferComplete(TransferComplete {
                file_name,
                ripemd160_hash,
            }))
        }
        MSG_TYPE_TRANSFER_OFFER => {
            let file_name = read_string(data, &mut offset)?;
            let size = read_u64(data, &mut offset)?;
            let sender_address = read_string(data, &mut offset)?;
            Ok(BinaryMessage::TransferOffer(TransferOffer {
                file_name,
                size,
                sender_address,
            }))
        }
        MSG_TYPE_TRANSFER_ACCEPT => {
            let file_name = read_string(data, &mut offset)?;
            let accepted = read_bool(data, &mut offset)?;
            Ok(BinaryMessage::TransferAccept(TransferAccept {
                file_name,
                accepted,
            }))
        }
        _ => bail!("Unknown message type: {}", msg_type),
    }
}
