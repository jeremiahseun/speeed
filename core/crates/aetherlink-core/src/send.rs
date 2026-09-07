//! Sender: offers a manifest, then drives N parallel TLS streams until every
//! chunk the receiver asked for has gone out.
//!
//! Streams exist to saturate cores with AEAD work and keep the send queue deep,
//! not to dodge head-of-line blocking (PRD §5.1). Each stream owns one *whole
//! chunk* at a time, which is what lets the receiver verify on arrival without
//! reassembling across streams.

use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use aetherlink_proto::frame::{FrameHeader, MsgType, HEADER_LEN};
use aetherlink_proto::{ChunkBitmap, SessionId};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::control::{self, Body, ControlMessage, FileEntry, Hello, ManifestOffer};
use crate::io::SourceFile;
use crate::link::{Acceptor, Dialer, StreamSource};
use crate::progress::{NoProgress, ProgressSink};
use crate::tls::{Fingerprint, HostIdentity};
use crate::{Config, Error, TransferStats};

/// One file the caller wants to send.
pub struct OutgoingFile {
    pub path: PathBuf,
    pub relative_path: String,
    pub mime_type: String,
}

/// Tracks which chunks of one file are still being read, so pages behind
/// *every* stream's cursor can be released.
///
/// Chunks leave the queue in order but finish out of order, so the safe
/// boundary is the **lowest index still in flight**, not the highest
/// dispatched. Releasing to the latter would drop pages a slower stream is
/// still reading — which is not a correctness bug on a read-only mapping, since
/// the page simply re-faults, but it would undo the work by thrashing.
#[derive(Debug, Default)]
struct ReadCursor {
    in_flight: BTreeSet<u64>,
    /// One past the highest index handed out so far.
    next_dispatch: u64,
    /// Byte offset already released.
    released_to: u64,
}

impl ReadCursor {
    /// Registers a chunk as being read. **Must be called while still holding
    /// the work queue lock**: if a chunk could be popped but not yet
    /// registered, a faster stream finishing a later chunk would compute a
    /// boundary past it and release pages it is about to read.
    fn begin(&mut self, index: u64) {
        self.in_flight.insert(index);
        self.next_dispatch = self.next_dispatch.max(index + 1);
    }

    /// Marks a chunk done and returns the byte range that has become safe to
    /// release, if enough has accumulated to be worth a syscall.
    fn finish(
        &mut self,
        index: u64,
        batch: u64,
        chunk_size: u64,
        file_size: u64,
    ) -> Option<(u64, u64)> {
        self.in_flight.remove(&index);
        // Empty means everything dispatched is done, so the boundary is the
        // dispatch point itself.
        let boundary_chunk = self
            .in_flight
            .first()
            .copied()
            .unwrap_or(self.next_dispatch);
        let boundary = boundary_chunk.saturating_mul(chunk_size).min(file_size);

        if boundary.saturating_sub(self.released_to) < batch {
            return None;
        }
        let range = (self.released_to, boundary);
        self.released_to = boundary;
        Some(range)
    }
}

/// A chunk assigned to whichever stream picks it up next.
#[derive(Debug, Clone, Copy)]
struct WorkItem {
    file_index: usize,
    chunk_index: u64,
}

/// Sends as the device that **joined** the network — it dials the host.
/// This is the iOS-to-Android direction.
pub async fn send(
    addr: &str,
    pinned: Fingerprint,
    files: Vec<OutgoingFile>,
    config: &Config,
) -> Result<TransferStats, Error> {
    send_with_progress(addr, pinned, files, config, Arc::new(NoProgress)).await
}

/// As [`send`], reporting progress as chunks go out. Wrap the sink in
/// [`crate::Throttled`] before passing it.
pub async fn send_with_progress(
    addr: &str,
    pinned: Fingerprint,
    files: Vec<OutgoingFile>,
    config: &Config,
    progress: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    let mut source = Dialer::new(addr, pinned, config)?;
    run(&mut source, files, config, progress).await
}

/// Sends as the device that **hosts** the network — it accepts, and the peer
/// dials in. This is the Android-to-iOS direction, and it is the common one:
/// iOS cannot host a network, so Android holds the known address even when it
/// is the one transmitting.
pub async fn send_as_host(
    listener: &TcpListener,
    identity: &HostIdentity,
    files: Vec<OutgoingFile>,
    config: &Config,
) -> Result<TransferStats, Error> {
    send_as_host_with_progress(listener, identity, files, config, Arc::new(NoProgress)).await
}

/// As [`send_as_host`], with progress reporting.
pub async fn send_as_host_with_progress(
    listener: &TcpListener,
    identity: &HostIdentity,
    files: Vec<OutgoingFile>,
    config: &Config,
    progress: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    let mut source = Acceptor::new(listener, identity, config)?;
    run(&mut source, files, config, progress).await
}

/// The transfer itself, independent of who dialled whom.
async fn run<S: StreamSource>(
    source: &mut S,
    files: Vec<OutgoingFile>,
    config: &Config,
    progress: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    // Map and hash every source before offering, so the manifest can commit to
    // the chunk hashes the receiver verifies against.
    let mut sources = Vec::with_capacity(files.len());
    let mut entries = Vec::with_capacity(files.len());
    for (i, f) in files.iter().enumerate() {
        let src = Arc::new(SourceFile::open(&f.path, config.chunk_size)?);
        let hashes = src.chunk_hashes(config.release_read_pages)?;
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
    let mut control = source.next_stream().await?;
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
    let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let queue = Arc::new(Mutex::new(queue));
    let cursors: Arc<Vec<std::sync::Mutex<ReadCursor>>> = Arc::new(
        (0..sources.len())
            .map(|_| std::sync::Mutex::new(ReadCursor::default()))
            .collect(),
    );
    let sources = Arc::new(sources);

    // --- data streams -------------------------------------------------------
    let mut workers = Vec::with_capacity(config.stream_count as usize);
    for stream_index in 1..=config.stream_count {
        let mut stream = source.next_stream().await?;
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
        let progress = progress.clone();
        let sent = sent.clone();
        let cursors = cursors.clone();
        let release_pages = config.release_read_pages;
        workers.push(tokio::spawn(async move {
            let mut header_buf = [0u8; HEADER_LEN];
            loop {
                // Popping and registering happen together under the queue
                // lock, so no chunk is ever dispatched-but-unregistered — see
                // `ReadCursor::begin`.
                let item = {
                    let mut queue = queue.lock().await;
                    let item = queue.pop_front();
                    if let Some(item) = item {
                        if release_pages {
                            cursors[item.file_index]
                                .lock()
                                .unwrap()
                                .begin(item.chunk_index);
                        }
                    }
                    item
                };
                let Some(item) = item else { break };

                let src = &sources[item.file_index];
                let (chunk_offset, chunk_len) = src.layout().range(item.chunk_index)?;

                // The borrow of the mapping is scoped so it has ended before
                // any page release below. Holding it across the release would
                // break the contract on `SourceFile::release_range`.
                {
                    let payload = src.chunk(item.chunk_index)?;

                    // One chunk goes out as a run of frames on this stream, in
                    // order, so the receiver can accumulate without
                    // cross-stream bookkeeping.
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

                if release_pages {
                    let chunk_size = src.layout().chunk_size() as u64;
                    let released = cursors[item.file_index].lock().unwrap().finish(
                        item.chunk_index,
                        crate::io::RELEASE_BATCH_BYTES.max(chunk_size),
                        chunk_size,
                        src.size(),
                    );
                    if let Some((start, end)) = released {
                        // SAFETY: `finish` returns only the range below the
                        // lowest chunk still in flight, so every chunk it
                        // covers has been fully written to a socket and its
                        // borrow dropped. The scope above ensures this
                        // worker's own borrow has ended too.
                        unsafe { src.release_range(start, end) };
                    }
                }

                let done = sent.fetch_add(chunk_len as u64, std::sync::atomic::Ordering::Relaxed)
                    + chunk_len as u64;
                progress.on_progress(done, bytes_to_send);
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

#[cfg(test)]
mod tests {
    use super::*;

    const CS: u64 = 4 * 1024 * 1024;
    const FILE: u64 = CS * 100;

    fn finish(c: &mut ReadCursor, index: u64) -> Option<(u64, u64)> {
        c.finish(index, CS, CS, FILE)
    }

    #[test]
    fn releases_behind_a_single_stream_as_it_advances() {
        let mut c = ReadCursor::default();
        c.begin(0);
        assert_eq!(finish(&mut c, 0), Some((0, CS)));
        c.begin(1);
        assert_eq!(finish(&mut c, 1), Some((CS, 2 * CS)));
    }

    #[test]
    fn never_releases_a_chunk_that_is_still_in_flight() {
        let mut c = ReadCursor::default();
        // Four streams take chunks 0..4; the last one finishes first.
        for i in 0..4 {
            c.begin(i);
        }
        assert_eq!(
            finish(&mut c, 3),
            None,
            "chunk 0 is still being read, so nothing may be released"
        );
        assert_eq!(finish(&mut c, 2), None);
        assert_eq!(finish(&mut c, 1), None);

        // Only once the slowest stream finishes does the boundary move — and
        // then it jumps past everything that completed while it was blocked.
        assert_eq!(finish(&mut c, 0), Some((0, 4 * CS)));
    }

    #[test]
    fn the_boundary_follows_the_slowest_stream_not_the_fastest() {
        let mut c = ReadCursor::default();
        for i in 0..3 {
            c.begin(i);
        }
        assert_eq!(
            finish(&mut c, 0),
            Some((0, CS)),
            "chunk 1 is now the slowest"
        );
        c.begin(3);
        assert_eq!(finish(&mut c, 2), None, "chunk 1 still holds the boundary");
        // Only when the slow one lands does the boundary jump past 2 as well.
        assert_eq!(finish(&mut c, 1), Some((CS, 3 * CS)));
    }

    #[test]
    fn holds_back_until_a_whole_chunk_is_worth_releasing() {
        // A file whose chunks are shorter than one release batch: the boundary
        // advances but no syscall is made until it clears the threshold.
        let mut c = ReadCursor::default();
        c.begin(0);
        assert_eq!(
            c.finish(0, CS, CS, CS / 4),
            None,
            "a partial chunk is not worth a syscall"
        );
    }

    #[test]
    fn released_offsets_never_overlap_or_go_backwards() {
        let mut c = ReadCursor::default();
        let mut last_end = 0u64;
        // Dispatch four at a time and complete them in reverse, repeatedly —
        // the pattern most likely to produce an overlapping range.
        for round in 0..10u64 {
            let base = round * 4;
            for i in 0..4 {
                c.begin(base + i);
            }
            for i in (0..4).rev() {
                if let Some((start, end)) = finish(&mut c, base + i) {
                    assert_eq!(
                        start, last_end,
                        "ranges must be contiguous, not overlapping"
                    );
                    assert!(end > start);
                    last_end = end;
                }
            }
        }
        assert_eq!(
            last_end,
            40 * CS,
            "everything dispatched should end up released"
        );
    }

    #[test]
    fn a_released_range_never_extends_past_the_end_of_the_file() {
        // Three chunks, the last one short. Releasing past the mapping would
        // be a `madvise` on memory we do not own.
        let file_size = CS * 2 + 1234;
        let mut c = ReadCursor::default();
        for i in 0..3 {
            c.begin(i);
        }
        let mut last_end = 0;
        for i in 0..3 {
            if let Some((_, end)) = c.finish(i, CS, CS, file_size) {
                assert!(end <= file_size, "released past the end of the mapping");
                last_end = end;
            }
        }
        // The 1234-byte tail stays mapped: below the release threshold, and
        // the mapping is dropped moments later anyway.
        assert_eq!(last_end, 2 * CS);
        assert!(
            file_size - last_end < CS,
            "at most one chunk is ever left behind"
        );
    }
}
