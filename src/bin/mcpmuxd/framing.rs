//! Message framing. MCP stdio is newline-delimited JSON; LSP prefixes every
//! message with `Content-Length` headers. Client and backend sides of one
//! daemon always use the same framing.

use crate::log;
use serde_json::Value;
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

pub const MSG_LIMIT: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy)]
pub enum Framing {
    Lines,
    Lsp,
}

impl Framing {
    pub fn encode(self, msg: &Value) -> Vec<u8> {
        let mut body = serde_json::to_vec(msg).expect("Value always serialises");
        match self {
            Framing::Lines => {
                body.push(b'\n');
                body
            }
            Framing::Lsp => {
                let mut out = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
                out.append(&mut body);
                out
            }
        }
    }

    /// Next message, `Ok(None)` on clean EOF. Unparseable bodies are logged
    /// and skipped, the stream stays usable.
    pub async fn read<R: AsyncBufRead + Unpin>(
        self,
        reader: &mut R,
        who: &str,
    ) -> io::Result<Option<Value>> {
        match self {
            Framing::Lines => read_line_msg(reader, who).await,
            Framing::Lsp => read_lsp_msg(reader, who).await,
        }
    }
}

fn too_big(len: usize) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("message of {} bytes over limit", len))
}

async fn read_line_msg<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    who: &str,
) -> io::Result<Option<Value>> {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        if reader.read_until(b'\n', &mut buf).await? == 0 {
            return Ok(None);
        }
        if buf.len() > MSG_LIMIT {
            return Err(too_big(buf.len()));
        }
        let line = buf.trim_ascii();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice(line) {
            Ok(msg) => return Ok(Some(msg)),
            Err(_) => {
                let head = String::from_utf8_lossy(&line[..line.len().min(120)]);
                log(&format!("{}: non-JSON line dropped: {:?}", who, head));
            }
        }
    }
}

async fn read_lsp_msg<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    who: &str,
) -> io::Result<Option<Value>> {
    let mut line = Vec::new();
    loop {
        let mut length: Option<usize> = None;
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line).await? == 0 {
                return Ok(None);
            }
            let header = line.trim_ascii();
            if header.is_empty() {
                if length.is_some() {
                    break;
                }
                continue; // stray blank line between messages
            }
            let text = String::from_utf8_lossy(header);
            if let Some((name, value)) = text.split_once(':') {
                if name.trim().eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().ok();
                }
            }
        }
        let length = length.unwrap_or(0);
        if length > MSG_LIMIT {
            return Err(too_big(length));
        }
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).await?;
        match serde_json::from_slice(&body) {
            Ok(msg) => return Ok(Some(msg)),
            Err(e) => log(&format!("{}: unparseable LSP body dropped: {}", who, e)),
        }
    }
}
