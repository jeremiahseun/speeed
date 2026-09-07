//! Receiver: accepts the manifest, then verifies and writes chunks as they land.
//!
//! Verification is not a post-transfer pass. Each chunk is checked against the
//! hash the manifest committed to, on a blocking worker, before it reaches the
//! file — so the root is settled the moment the last frame arrives and a
//! corrupt chunk never touches disk.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aetherlink_proto::chunk::{self, ChunkLayout, Hash};
use aetherlink_proto::frame::{FrameHeader, MsgType, HEADER_LEN};
use aetherlink_proto::{sanitize_relative_path, ChunkBitmap};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::control::{self, Body, ControlMessage, FileProgress, ManifestAccept, TransferComplete};
use crate::io::{verify_and_write, SinkFile};
use crate::link::{Acceptor, Dialer, StreamSource};
use crate::progress::{NoProgress, ProgressSink};
use crate::resume::{self, StateKey};
use crate::tls::{Fingerprint, HostIdentity};
use crate::{Config, Error, TransferStats};

/// Per-file receive state, shared by every stream worker.
struct FileState {
    sink: SinkFile,
    layout: ChunkLayout,
    chunk_hashes: Vec<Hash>,
    bitmap: Mutex<ChunkBitmap>,
    /// Identity this file's resume record must match.
    key: StateKey,
    /// Bytes written since the last checkpoint.
    since_checkpoint: AtomicU64,
    /// Held for the duration of a checkpoint so concurrent workers do not
    /// stack fsyncs on the same file.
    checkpointing: Mutex<()>,
}

/// Receives as the device that **hosts** the network — it accepts, and the peer
/// dials in. This is the iOS-to-Android direction.
pub async fn receive(
    listener: &TcpListener,
    identity: &HostIdentity,
    output_dir: &Path,
    config: &Config,
) -> Result<TransferStats, Error> {
    receive_with_progress(listener, identity, output_dir, config, Arc::new(NoProgress)).await
}

/// As [`receive`], reporting progress as chunks land.
///
/// Wrap the sink in [`crate::Throttled`] before passing it: stream workers call
/// it on every completed chunk, which at 1 GB/s is 250 events a second.
pub async fn receive_with_progress(
    listener: &TcpListener,
    identity: &HostIdentity,
    output_dir: &Path,
    config: &Config,
    progress_sink: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    let mut source = Acceptor::new(listener, identity, config)?;
    run(&mut source, output_dir, config, progress_sink).await
}

/// Receives as the device that **joined** the network — it dials the host.
/// This is the Android-to-iOS direction, and the common one: Android holds the
/// known address even when it is the sender, so iOS dials in to be sent to.
pub async fn receive_as_client(
    addr: &str,
    pinned: Fingerprint,
    output_dir: &Path,
    config: &Config,
) -> Result<TransferStats, Error> {
    receive_as_client_with_progress(addr, pinned, output_dir, config, Arc::new(NoProgress)).await
}

/// As [`receive_as_client`], with progress reporting.
pub async fn receive_as_client_with_progress(
    addr: &str,
    pinned: Fingerprint,
    output_dir: &Path,
    config: &Config,
    progress_sink: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    let mut source = Dialer::new(addr, pinned, config)?;
    run(&mut source, output_dir, config, progress_sink).await
}

/// The receive itself, independent of who dialled whom.
async fn run<S: StreamSource>(
    source: &mut S,
    output_dir: &Path,
    config: &Config,
    progress_sink: Arc<dyn ProgressSink>,
) -> Result<TransferStats, Error> {
    // Control first, then data, on both sides — so a dialer on one end pairs
    // with an acceptor on the other without either needing to know which it is.
    let mut control = source.next_stream().await?;
    let stream_count = match control::read_control(&mut control).await? {
        Body::Hello(h) => {
            if h.protocol_version != aetherlink_proto::PROTOCOL_VERSION as u32 {
                let reason = format!("peer speaks protocol v{}", h.protocol_version);
                abort(&mut control, &reason).await;
                return Err(Error::Protocol(reason));
            }
            // Peer-controlled, and it drives an accept loop — bound it or a
            // hostile Hello parks us accepting connections forever.
            if h.stream_count == 0 || h.stream_count > config.max_streams as u32 {
                let reason = format!(
                    "peer requested {} streams, outside the accepted range 1..={}",
                    h.stream_count, config.max_streams
                );
                abort(&mut control, &reason).await;
                return Err(Error::Protocol(reason));
            }
            h.stream_count
        }
        other => return Err(Error::Protocol(format!("expected Hello, got {other:?}"))),
    };

    let offer = match control::read_control(&mut control).await? {
        Body::Offer(o) => o,
        other => return Err(Error::Protocol(format!("expected Offer, got {other:?}"))),
    };

    // Everything below treats the offer as hostile input.
    let mut files: HashMap<u64, Arc<FileState>> = HashMap::new();
    let mut accepted = Vec::new();
    let mut progress = Vec::new();
    let mut seen_paths = std::collections::HashSet::new();
    let mut total_bytes = 0u64;
    let mut resumed_bytes = 0u64;

    for entry in &offer.files {
        let safe_path = match sanitize_relative_path(&entry.relative_path) {
            Ok(p) => p,
            Err(e) => {
                let reason = e.to_string();
                abort(&mut control, &reason).await;
                return Err(e.into());
            }
        };
        if !seen_paths.insert(safe_path.clone()) {
            let reason = format!("manifest resolves two entries to {safe_path}");
            abort(&mut control, &reason).await;
            return Err(Error::Protocol(reason));
        }

        let layout = ChunkLayout::new(entry.size_bytes, offer.chunk_size)?;
        if entry.size_bytes > config.max_file_bytes {
            let reason = format!(
                "{safe_path} is {} bytes, above the accepted maximum",
                entry.size_bytes
            );
            abort(&mut control, &reason).await;
            return Err(Error::Protocol(reason));
        }
        let chunk_hashes = split_hashes(&entry.chunk_hashes, layout.count())?;

        // The offer must be internally consistent: the root it claims has to be
        // the root of the chunk hashes it sent. Otherwise a sender could commit
        // to one file and deliver another.
        let computed = chunk::root_from_chunk_hashes(&chunk_hashes);
        if entry.root_hash.as_slice() != computed.as_slice() {
            let reason = format!("root hash does not match the chunk hashes for {safe_path}");
            abort(&mut control, &reason).await;
            return Err(Error::Protocol(reason));
        }

        let dest: PathBuf = output_dir.join(&safe_path);
        let key = StateKey {
            root_hash: computed,
            file_size: entry.size_bytes,
            chunk_size: offer.chunk_size,
        };

        // Whether the staging file predates this session decides how hard we
        // work to find out what it holds. `SinkFile::create` would make it
        // either way, so ask first.
        let preexisting = dest.exists();
        let sink = SinkFile::create(&dest, entry.size_bytes, offer.chunk_size)?;

        let bitmap = if !config.resume {
            ChunkBitmap::new(layout.count())
        } else if let Some(recorded) = resume::load(&dest, &key) {
            // The sidecar validated against this exact manifest.
            recorded
        } else if preexisting {
            // Something is on disk but we have no usable record of it — a
            // sidecar lost to a hard kill, or a transfer from a build that did
            // not write one. Hashing what is there costs a full read but
            // cannot be misled.
            resume::rebuild_by_verification(&sink, &chunk_hashes)?
        } else {
            ChunkBitmap::new(layout.count())
        };

        // Short final chunk means this can overshoot; clamped below.
        resumed_bytes += bitmap.set_count() * offer.chunk_size as u64;
        progress.push(FileProgress {
            file_id: entry.file_id,
            have_bitmap: bitmap.as_bytes().to_vec(),
        });
        accepted.push(entry.file_id);
        total_bytes += entry.size_bytes;
        files.insert(
            entry.file_id,
            Arc::new(FileState {
                sink,
                layout,
                chunk_hashes,
                bitmap: Mutex::new(bitmap),
                key,
                since_checkpoint: AtomicU64::new(0),
                checkpointing: Mutex::new(()),
            }),
        );
    }

    control::write_control(
        &mut control,
        &ControlMessage::new(Body::Accept(ManifestAccept {
            accepted_file_ids: accepted,
            progress,
        })),
    )
    .await?;

    // --- data streams -------------------------------------------------------
    let started = std::time::Instant::now();
    // Seeded with the resumed bytes so a resumed transfer's progress bar picks
    // up where it left off instead of restarting at zero.
    let bytes_written = Arc::new(AtomicU64::new(resumed_bytes.min(total_bytes)));
    let files = Arc::new(files);
    let mut workers = Vec::new();

    for _ in 0..stream_count {
        let mut stream = source.next_stream().await?;
        match control::read_control(&mut stream).await? {
            Body::Hello(_) => {}
            other => return Err(Error::Protocol(format!("expected Hello, got {other:?}"))),
        }
        let files = files.clone();
        let counter = bytes_written.clone();
        let chunk_size = offer.chunk_size;
        let progress_sink = progress_sink.clone();
        let checkpoint_bytes = config.checkpoint_bytes;
        workers.push(tokio::spawn(async move {
            drain_stream(
                &mut stream,
                files,
                counter,
                chunk_size,
                progress_sink,
                total_bytes,
                checkpoint_bytes,
            )
            .await
        }));
    }

    let mut first_error: Option<Error> = None;
    for worker in workers {
        let outcome = match worker.await {
            Ok(result) => result,
            Err(e) => Err(Error::Io(format!("stream worker panicked: {e}"))),
        };
        if let Err(e) = outcome {
            first_error.get_or_insert(e);
        }
    }

    // A dropped connection is precisely what resume exists for, so checkpoint
    // every file before propagating the error. Without this the next attempt
    // starts from byte zero and the whole feature is decorative.
    if let Some(error) = first_error {
        for state in files.values() {
            checkpoint(state).await.ok();
        }
        return Err(error);
    }

    // --- completeness -------------------------------------------------------
    for state in files.values() {
        if !state.bitmap.lock().await.is_complete() {
            checkpoint(state).await.ok();
            let reason = format!("{} is incomplete", state.sink.path().display());
            abort(&mut control, &reason).await;
            return Err(Error::Incomplete(reason));
        }
        state.sink.sync()?;
        // The file is done. A stale record here would tell a later transfer to
        // the same path that it has nothing left to do.
        resume::discard(state.sink.path());
    }

    let bytes = bytes_written.load(Ordering::Relaxed);
    control::write_control(
        &mut control,
        &ControlMessage::new(Body::Complete(TransferComplete {
            bytes_written: bytes,
        })),
    )
    .await?;
    control.shutdown().await.ok();

    Ok(TransferStats {
        bytes,
        elapsed: started.elapsed(),
    })
}

/// Reads frames until the peer closes the stream, accumulating whole chunks.
#[allow(clippy::too_many_arguments)]
async fn drain_stream<S: AsyncRead + Unpin>(
    stream: &mut S,
    files: Arc<HashMap<u64, Arc<FileState>>>,
    counter: Arc<AtomicU64>,
    chunk_size: u32,
    progress_sink: Arc<dyn ProgressSink>,
    total_bytes: u64,
    checkpoint_bytes: u64,
) -> Result<(), Error> {
    let mut header_buf = [0u8; HEADER_LEN];
    let mut buf: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    // (file_id, chunk_index, chunk_start_offset, expected_len)
    let mut current: Option<(u64, u64, u64, u32)> = None;

    loop {
        match stream.read_exact(&mut header_buf).await {
            Ok(_) => {}
            // A clean close between chunks is how a stream ends.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(format!("reading frame header: {e}"))),
        }
        let header = FrameHeader::decode(&header_buf)?;
        match header.msg_type {
            MsgType::FileData => {}
            MsgType::Keepalive => continue,
            other => {
                return Err(Error::Protocol(format!(
                    "unexpected {other:?} on a data stream"
                )))
            }
        }

        let state = files
            .get(&header.file_id)
            .ok_or_else(|| Error::Protocol(format!("unknown file id {}", header.file_id)))?;

        // Start of a new chunk?
        if current.is_none() {
            let chunk_index = state.layout.index_of(header.offset)?;
            let (start, expected_len) = state.layout.range(chunk_index)?;
            if header.offset != start {
                return Err(Error::Protocol(format!(
                    "chunk must begin at its own offset; got {} expected {start}",
                    header.offset
                )));
            }
            buf.clear();
            current = Some((header.file_id, chunk_index, start, expected_len));
        }

        let (file_id, chunk_index, start, expected_len) = current.unwrap();
        if header.file_id != file_id {
            return Err(Error::Protocol("stream interleaved files mid-chunk".into()));
        }
        // Frames within a chunk must be contiguous and in order.
        if header.offset != start + buf.len() as u64 {
            return Err(Error::Protocol(format!(
                "out-of-order frame at offset {}, expected {}",
                header.offset,
                start + buf.len() as u64
            )));
        }
        if buf.len() + header.length as usize > expected_len as usize {
            return Err(Error::Protocol("frame overruns its chunk".into()));
        }

        let base = buf.len();
        buf.resize(base + header.length as usize, 0);
        stream
            .read_exact(&mut buf[base..])
            .await
            .map_err(|e| Error::Io(format!("reading frame payload: {e}")))?;

        if buf.len() == expected_len as usize {
            let state = state.clone();
            let file = state.sink.handle();
            let expected = state.chunk_hashes[chunk_index as usize];
            let payload = std::mem::take(&mut buf);

            // BLAKE3 over 4 MB is ~2 ms of CPU; it does not belong on a reactor
            // thread. The buffer is handed back so it can be reused.
            let (payload, result) = tokio::task::spawn_blocking(move || {
                let r = verify_and_write(&file, start, &payload, &expected);
                (payload, r)
            })
            .await
            .map_err(|e| Error::Io(format!("disk worker panicked: {e}")))?;
            result?;

            let complete = {
                let mut bitmap = state.bitmap.lock().await;
                bitmap.set(chunk_index)?;
                bitmap.is_complete()
            };
            let done =
                counter.fetch_add(expected_len as u64, Ordering::Relaxed) + expected_len as u64;
            progress_sink.on_progress(done, total_bytes);
            if complete {
                progress_sink.on_file_completed(file_id, &state.sink.path().to_string_lossy());
            }

            checkpoint_if_due(&state, expected_len as u64, checkpoint_bytes).await?;

            buf = payload;
            buf.clear();
            current = None;
        }
    }

    if current.is_some() {
        return Err(Error::Incomplete("stream closed mid-chunk".into()));
    }
    Ok(())
}

/// Checkpoints once enough has been written since the last one.
///
/// Ordering is the whole correctness argument. The bitmap is snapshotted
/// **before** the fsync, so every bit in the snapshot corresponds to a write
/// that had already completed — the fsync therefore guarantees those bytes are
/// on the device. A bit set after the snapshot is simply not recorded yet,
/// which costs at most one re-sent chunk. Snapshotting after the fsync would
/// invert this and could record a chunk that is still only in the page cache.
async fn checkpoint_if_due(
    state: &Arc<FileState>,
    added: u64,
    threshold: u64,
) -> Result<(), Error> {
    if threshold == 0 {
        return Ok(());
    }
    let before = state.since_checkpoint.fetch_add(added, Ordering::Relaxed);
    if before + added < threshold {
        return Ok(());
    }
    // One checkpoint at a time per file. A worker that loses the race keeps
    // transferring rather than queueing behind an fsync.
    let Ok(_guard) = state.checkpointing.try_lock() else {
        return Ok(());
    };
    state.since_checkpoint.store(0, Ordering::Relaxed);
    checkpoint(state).await
}

/// Flushes the file and records what is durably present.
async fn checkpoint(state: &Arc<FileState>) -> Result<(), Error> {
    let snapshot = state.bitmap.lock().await.clone();
    if snapshot.set_count() == 0 {
        return Ok(());
    }
    let file = state.sink.handle();
    let staging = state.sink.path().to_path_buf();
    let key = state.key;

    tokio::task::spawn_blocking(move || -> Result<(), Error> {
        file.sync_data()
            .map_err(|e| Error::Io(format!("syncing {}: {e}", staging.display())))?;
        resume::persist(&staging, &key, &snapshot)
    })
    .await
    .map_err(|e| Error::Io(format!("checkpoint worker panicked: {e}")))?
}

/// Best-effort notice to the peer before we drop the session. The local error
/// is what the caller sees; failing to deliver this does not change it.
async fn abort<W: AsyncWrite + Unpin>(control: &mut W, reason: &str) {
    let msg = ControlMessage::new(Body::Abort(control::Abort {
        reason: reason.to_string(),
    }));
    let _ = control::write_control(control, &msg).await;
}

fn split_hashes(concatenated: &[u8], count: u64) -> Result<Vec<Hash>, Error> {
    if concatenated.len() as u64 != count * 32 {
        return Err(Error::Protocol(format!(
            "manifest carries {} hash bytes, expected {} for {count} chunks",
            concatenated.len(),
            count * 32
        )));
    }
    Ok(concatenated
        .chunks_exact(32)
        .map(|c| {
            let mut h = [0u8; 32];
            h.copy_from_slice(c);
            h
        })
        .collect())
}

/// Convenience wrapper: bind an ephemeral port and return it with the identity
/// the peer must pin.
pub async fn bind(port: u16) -> Result<TcpListener, Error> {
    TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| Error::Io(format!("binding port {port}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_hashes_rejects_a_length_that_disagrees_with_the_chunk_count() {
        assert!(split_hashes(&[0u8; 64], 2).is_ok());
        assert!(split_hashes(&[0u8; 63], 2).is_err());
        assert!(split_hashes(&[0u8; 96], 2).is_err());
        assert!(split_hashes(&[], 0).is_ok());
    }
}
