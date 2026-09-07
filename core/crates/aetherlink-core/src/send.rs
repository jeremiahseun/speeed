//! Sender: offers a manifest, then drives N parallel TLS streams until every
//! chunk the receiver asked for has gone out.
//!
//! Streams exist to saturate cores with AEAD work and keep the send queue deep,
//! not to dodge head-of-line blocking (PRD §5.1). Each stream owns one *whole
//! chunk* at a time, which is what lets the receiver verify on arrival without
//! reassembling across streams.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use aetherlink_proto::frame::{FrameHeader, MsgType, HEADER_LEN};
use aetherlink_proto::{ChunkBitmap, SessionId};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

use crate::control::{self, Body, ControlMessage, FileEntry, Hello, ManifestOffer};
use crate::io::SourceFile;
use crate::tls;
use crate::{Config, Error, TransferStats};

/// One file the caller wants to send.
pub struct OutgoingFile {
    pub path: PathBuf,
    pub relative_path: String,
    pub mime_type: String,
}

/// A chunk assigned to whichever stream picks it up next.
#[derive(Debug, Clone, Copy)]
struct WorkItem {
    file_index: usize,
    chunk_index: u64,
}

pub async fn send(
    addr: &str,
    pinned: tls::Fingerprint,
    files: Vec<OutgoingFile>,
    config: &Config,
) -> Result<TransferStats, Error> {
    let connector = TlsConnector::from(tls::client_config(pinned)?);
    let server_name = rustls::pki_types::ServerName::try_from(tls::SERVER_NAME)
        .map_err(|e| Error::Tls(format!("invalid server name: {e}")))?;

    // Map and hash every source before offering, so the manifest can commit to
    // the chunk hashes the receiver verifies against.
    let mut sources = Vec::with_capacity(files.len());
    let mut entries = Vec::with_capacity(files.len());
    for (i, f) in files.iter().enumerate() {
        let src = Arc::new(SourceFile::open(&f.path, config.chunk_size)?);
        let hashes = src.chunk_hashes()?;
        let root = aetherlink_proto::chunk::root_from_chunk_hashes(&hashes);
        entries.push(FileEntry {
            file_id: i as u64,
            relative_path: f.relative_path.clone(),
            size_bytes: src.size(),
            mime_type: f.mime_type.clone(),
            root_hash: root.to_vec(),
            chunk_hashes: hashes.concat(),
            modified_unix_ms: 0,
            created_unix_ms: 0,
        });
        sources.push(src);
    }

    let session_id = SessionId(rand_bytes());

    // --- control stream -----------------------------------------------------
    let mut control = connect(&connector, &server_name, addr).await?;
    control::write_control(
        &mut control,
        &ControlMessage::new(Body::Hello(Hello {
            protocol_version: aetherlink_proto::PROTOCOL_VERSION as u32,
            session_id: session_id.0.to_vec(),
            stream_count: config.stream_count as u32,
        })),
    )
    .await?;
    control::write_control(
        &mut control,
        &ControlMessage::new(Body::Offer(ManifestOffer {
            chunk_size: config.chunk_size,
            files: entries.clone(),
        })),
    )
    .await?;

    let accept = match control::read_control(&mut control).await? {
        Body::Accept(a) => a,
        Body::Abort(a) => return Err(Error::PeerAborted(a.reason)),
        other => return Err(Error::Protocol(format!("expected Accept, got {other:?}"))),
    };

    // --- build the work queue from what the receiver still needs ------------
    let mut queue = VecDeque::new();
    let mut bytes_to_send = 0u64;
    for (file_index, entry) in entries.iter().enumerate() {
        if !accept.accepted_file_ids.contains(&entry.file_id) {
            continue;
        }
        let layout = sources[file_index].layout();
        let have = accept
            .progress
            .iter()
            .find(|p| p.file_id == entry.file_id)
            .map(|p| ChunkBitmap::from_bytes(&p.have_bitmap, layout.count()))
            .transpose()?
            .unwrap_or_else(|| ChunkBitmap::new(layout.count()));

        // Only the zero bits travel. On a fresh transfer that is everything; on
        // a resume it is whatever the interruption left behind.
        for (start, end) in have.missing_ranges() {
            for chunk_index in start..end {
                bytes_to_send += layout.range(chunk_index)?.1 as u64;
                queue.push_back(WorkItem {
                    file_index,
                    chunk_index,
                });
            }
        }
    }

    let started = std::time::Instant::now();
    let queue = Arc::new(Mutex::new(queue));
    let sources = Arc::new(sources);

    // --- data streams -------------------------------------------------------
    let mut workers = Vec::with_capacity(config.stream_count as usize);
    for stream_index in 1..=config.stream_count {
        let mut stream = connect(&connector, &server_name, addr).await?;
        control::write_control(
            &mut stream,
            &ControlMessage::new(Body::Hello(Hello {
                protocol_version: aetherlink_proto::PROTOCOL_VERSION as u32,
                session_id: session_id.0.to_vec(),
                stream_count: stream_index as u32,
            })),
        )
        .await?;

        let queue = queue.clone();
        let sources = sources.clone();
        let frame_size = config.frame_size;
        workers.push(tokio::spawn(async move {
            let mut header_buf = [0u8; HEADER_LEN];
            loop {
                let item = { queue.lock().await.pop_front() };
                let Some(item) = item else { break };

                let src = &sources[item.file_index];
                let (chunk_offset, _) = src.layout().range(item.chunk_index)?;
                let payload = src.chunk(item.chunk_index)?;

                // One chunk goes out as a run of frames on this stream, in
                // order, so the receiver can accumulate without cross-stream
                // bookkeeping.
                for (n, part) in payload.chunks(frame_size as usize).enumerate() {
                    let offset = chunk_offset + (n as u64 * frame_size as u64);
                    FrameHeader::new(
                        MsgType::FileData,
                        item.file_index as u64,
                        offset,
                        part.len() as u32,
                    )
                    .encode(&mut header_buf);
                    stream.write_all(&header_buf).await?;
                    stream.write_all(part).await?;
                }
            }
            stream.flush().await?;
            stream.shutdown().await?;

            // Symmetric close: read to EOF so the peer's close_notify and FIN
            // are consumed before the socket is dropped. Dropping a socket that
            // still holds unread bytes sends RST, which discards whatever the
            // peer has not yet read.
            let mut drained = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut drained)
                .await
                .ok();
            Ok::<(), Error>(())
        }));
    }

    for worker in workers {
        worker
            .await
            .map_err(|e| Error::Io(format!("stream worker panicked: {e}")))??;
    }

    // --- wait for the receiver to confirm ----------------------------------
    match control::read_control(&mut control).await? {
        Body::Complete(c) => {
            let elapsed = started.elapsed();
            control.shutdown().await.ok();
            Ok(TransferStats {
                bytes: c.bytes_written.max(bytes_to_send),
                elapsed,
            })
        }
        Body::Abort(a) => Err(Error::PeerAborted(a.reason)),
        other => Err(Error::Protocol(format!("expected Complete, got {other:?}"))),
    }
}

async fn connect(
    connector: &TlsConnector,
    server_name: &rustls::pki_types::ServerName<'static>,
    addr: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, Error> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| Error::Io(format!("connecting to {addr}: {e}")))?;
    // Bulk transfer: never let Nagle hold a partial frame.
    tcp.set_nodelay(true).ok();
    connector
        .connect(server_name.clone(), tcp)
        .await
        .map_err(|e| Error::Tls(format!("handshake with {addr}: {e}")))
}

/// Session nonce. Not a secret — the pinned certificate is what authenticates
/// the peer — so the system clock plus the address of a local allocation is
/// sufficient entropy to distinguish concurrent sessions.
fn rand_bytes() -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    hasher.update(&now.as_nanos().to_le_bytes());
    let local = 0u8;
    hasher.update(&(&local as *const u8 as usize).to_le_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    out
}
