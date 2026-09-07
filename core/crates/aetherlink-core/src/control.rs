//! Control-channel messages, protobuf-encoded on stream 0.
//!
//! Low volume and schema-evolving, so protobuf earns its place here. The bulk
//! data path uses the fixed binary header instead and never touches this.
//!
//! Types are declared with `prost` derives rather than generated from a `.proto`
//! file, so the build needs no `protoc`. The wire encoding is identical; the
//! equivalent schema is in `docs/PRD-v1.1.md` §5.4.

use aetherlink_proto::frame::{FrameHeader, MsgType, HEADER_LEN, MAX_FRAME_PAYLOAD};
use prost::Message as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;

#[derive(Clone, PartialEq, prost::Message)]
pub struct FileEntry {
    #[prost(uint64, tag = "1")]
    pub file_id: u64,
    #[prost(string, tag = "2")]
    pub relative_path: String,
    #[prost(uint64, tag = "3")]
    pub size_bytes: u64,
    #[prost(string, tag = "4")]
    pub mime_type: String,
    /// BLAKE3 root over the ordered chunk hashes.
    #[prost(bytes = "vec", tag = "5")]
    pub root_hash: Vec<u8>,
    /// Concatenated 32-byte chunk hashes, in index order. Committing to these
    /// up front is what lets the receiver verify each chunk as it lands instead
    /// of re-reading the whole file afterwards. 76 KB for a 10 GB file.
    #[prost(bytes = "vec", tag = "6")]
    pub chunk_hashes: Vec<u8>,
    #[prost(int64, tag = "7")]
    pub modified_unix_ms: i64,
    #[prost(int64, tag = "8")]
    pub created_unix_ms: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Hello {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub session_id: Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub stream_count: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ManifestOffer {
    #[prost(uint32, tag = "1")]
    pub chunk_size: u32,
    #[prost(message, repeated, tag = "2")]
    pub files: Vec<FileEntry>,
}

/// Per-file resume state the receiver already holds. An empty bitmap means
/// "send everything".
#[derive(Clone, PartialEq, prost::Message)]
pub struct FileProgress {
    #[prost(uint64, tag = "1")]
    pub file_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub have_bitmap: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ManifestAccept {
    #[prost(uint64, repeated, tag = "1")]
    pub accepted_file_ids: Vec<u64>,
    #[prost(message, repeated, tag = "2")]
    pub progress: Vec<FileProgress>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TransferComplete {
    #[prost(uint64, tag = "1")]
    pub bytes_written: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Abort {
    #[prost(string, tag = "1")]
    pub reason: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ControlMessage {
    #[prost(oneof = "Body", tags = "1, 2, 3, 4, 5")]
    pub body: Option<Body>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Body {
    #[prost(message, tag = "1")]
    Hello(Hello),
    #[prost(message, tag = "2")]
    Offer(ManifestOffer),
    #[prost(message, tag = "3")]
    Accept(ManifestAccept),
    #[prost(message, tag = "4")]
    Complete(TransferComplete),
    #[prost(message, tag = "5")]
    Abort(Abort),
}

impl ControlMessage {
    pub fn new(body: Body) -> Self {
        Self { body: Some(body) }
    }

    fn into_body(self) -> Result<Body, Error> {
        self.body
            .ok_or_else(|| Error::Protocol("control message has no body".into()))
    }
}

/// Writes a control message as a `Control` frame: fixed header, then the
/// protobuf payload.
pub async fn write_control<W>(w: &mut W, msg: &ControlMessage) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let payload = msg.encode_to_vec();
    if payload.len() > MAX_FRAME_PAYLOAD as usize {
        return Err(Error::Protocol(format!(
            "control message of {} bytes exceeds the frame maximum",
            payload.len()
        )));
    }
    let header = FrameHeader::new(MsgType::Control, 0, 0, payload.len() as u32);
    w.write_all(&header.to_bytes()).await?;
    w.write_all(&payload).await?;
    w.flush().await?;
    Ok(())
}

/// Reads one control message. The frame header's length is bounds-checked
/// during decode, so the allocation below is bounded by `MAX_FRAME_PAYLOAD`.
pub async fn read_control<R>(r: &mut R) -> Result<Body, Error>
where
    R: AsyncRead + Unpin,
{
    let mut header_buf = [0u8; HEADER_LEN];
    r.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::Control {
        return Err(Error::Protocol(format!(
            "expected a control frame, got {:?}",
            header.msg_type
        )));
    }
    let mut payload = vec![0u8; header.length as usize];
    r.read_exact(&mut payload).await?;
    ControlMessage::decode(payload.as_slice())
        .map_err(|e| Error::Protocol(format!("decoding control message: {e}")))?
        .into_body()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample_offer() -> ControlMessage {
        ControlMessage::new(Body::Offer(ManifestOffer {
            chunk_size: 4 * 1024 * 1024,
            files: vec![FileEntry {
                file_id: 42,
                relative_path: "DCIM/IMG_0001.HEIC".into(),
                size_bytes: 10_000_000_000,
                mime_type: "image/heic".into(),
                root_hash: vec![7u8; 32],
                chunk_hashes: vec![9u8; 64],
                modified_unix_ms: 1_700_000_000_000,
                created_unix_ms: 1_600_000_000_000,
            }],
        }))
    }

    #[tokio::test]
    async fn control_frames_roundtrip() {
        let msg = sample_offer();
        let mut buf = Vec::new();
        write_control(&mut buf, &msg).await.unwrap();

        // Header is written before the payload, and is a Control frame.
        let header = FrameHeader::decode(&buf[..HEADER_LEN]).unwrap();
        assert_eq!(header.msg_type, MsgType::Control);
        assert_eq!(header.length as usize, buf.len() - HEADER_LEN);

        let body = read_control(&mut Cursor::new(buf)).await.unwrap();
        assert_eq!(body, msg.body.unwrap());
    }

    #[tokio::test]
    async fn every_variant_roundtrips() {
        let cases = vec![
            Body::Hello(Hello {
                protocol_version: 1,
                session_id: vec![3u8; 16],
                stream_count: 8,
            }),
            Body::Accept(ManifestAccept {
                accepted_file_ids: vec![1, 2, 3],
                progress: vec![FileProgress {
                    file_id: 1,
                    have_bitmap: vec![0xF0, 0x0F],
                }],
            }),
            Body::Complete(TransferComplete {
                bytes_written: 10_000_000_000,
            }),
            Body::Abort(Abort {
                reason: "user cancelled".into(),
            }),
        ];
        for body in cases {
            let mut buf = Vec::new();
            write_control(&mut buf, &ControlMessage::new(body.clone()))
                .await
                .unwrap();
            assert_eq!(read_control(&mut Cursor::new(buf)).await.unwrap(), body);
        }
    }

    #[tokio::test]
    async fn rejects_a_data_frame_on_the_control_channel() {
        let header = FrameHeader::new(MsgType::FileData, 1, 0, 0);
        let mut buf = header.to_bytes().to_vec();
        buf.extend_from_slice(&[]);
        assert!(matches!(
            read_control(&mut Cursor::new(buf)).await,
            Err(Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn rejects_an_empty_body() {
        let mut buf = Vec::new();
        write_control(&mut buf, &ControlMessage { body: None })
            .await
            .unwrap();
        assert!(matches!(
            read_control(&mut Cursor::new(buf)).await,
            Err(Error::Protocol(_))
        ));
    }
}
