//! Resume across reconnect, end to end.
//!
//! The unit tests in `resume.rs` cover the state record in isolation. These
//! prove the thing that actually matters: after an interruption, only the
//! missing bytes travel, and the file that lands is byte-for-byte correct.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aetherlink_core::{recv, resume, send, Config, HostIdentity, OutgoingFile, ProgressSink};

/// Compares by digest. Asserting on multi-megabyte `Vec`s directly buries the
/// failure message under the contents.
#[track_caller]
fn assert_same(actual: &[u8], expected: &[u8], what: &str) {
    assert_eq!(actual.len(), expected.len(), "{what}: length differs");
    assert_eq!(
        blake3::hash(actual),
        blake3::hash(expected),
        "{what}: contents differ"
    );
}

#[track_caller]
fn assert_differs(actual: &[u8], expected: &[u8], what: &str) {
    assert_ne!(blake3::hash(actual), blake3::hash(expected), "{what}");
}

const CHUNK: u32 = 64 * 1024;
const CHUNKS: usize = 24;

fn config() -> Config {
    Config {
        stream_count: 4,
        frame_size: 16 * 1024,
        chunk_size: CHUNK,
        // Checkpoint every chunk, so an interruption always leaves a record.
        checkpoint_bytes: CHUNK as u64,
        ..Config::default()
    }
}

#[derive(Debug)]
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "aetherlink-resume-it-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// Captures the `bytes_total` the sender reports, which is exactly the work it
/// queued after subtracting whatever the receiver said it already had.
#[derive(Default)]
struct WorkObserver {
    queued: AtomicU64,
    seen: AtomicU64,
}

impl ProgressSink for WorkObserver {
    fn on_progress(&self, bytes_done: u64, bytes_total: u64) {
        self.queued.store(bytes_total, Ordering::Relaxed);
        self.seen.fetch_max(bytes_done, Ordering::Relaxed);
    }
    fn on_file_completed(&self, _file_id: u64, _path: &str) {}
}

fn payload() -> Vec<u8> {
    payload_of(CHUNKS)
}

fn payload_of(chunks: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunks * CHUNK as usize);
    let mut x: u32 = 0x1234_5678;
    for _ in 0..out.capacity() {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        out.push((x >> 24) as u8);
    }
    out
}

/// Runs one full transfer, returning the bytes the sender had to queue.
async fn run_transfer(src: &Path, out_dir: &Path, cfg: Config) -> Result<u64, String> {
    let identity = HostIdentity::generate().unwrap();
    let fingerprint = identity.fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let out = out_dir.to_path_buf();
    let server = tokio::spawn(async move { recv::receive(&listener, &identity, &out, &cfg).await });

    let observer = Arc::new(WorkObserver::default());
    let files = vec![OutgoingFile {
        path: src.to_path_buf(),
        relative_path: "payload.bin".into(),
        mime_type: "application/octet-stream".into(),
    }];
    let sent = send::send_with_progress(&addr, fingerprint, files, &cfg, observer.clone()).await;
    let received = server.await.unwrap();

    sent.map_err(|e| format!("send: {e}"))?;
    received.map_err(|e| format!("recv: {e}"))?;
    Ok(observer.queued.load(Ordering::Relaxed))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_destination_transfers_only_the_missing_chunks() {
    let src_dir = TempDir::new("src");
    let dst_dir = TempDir::new("dst");
    let data = payload();
    let src = src_dir.0.join("payload.bin");
    std::fs::write(&src, &data).unwrap();

    // First pass: everything travels.
    let queued = run_transfer(&src, &dst_dir.0, config()).await.unwrap();
    assert_eq!(
        queued,
        data.len() as u64,
        "a fresh transfer sends the whole file"
    );

    let dest = dst_dir.0.join("payload.bin");
    assert!(
        !resume::state_path(&dest).exists(),
        "a completed file must not leave a resume record behind"
    );

    // Damage two chunks in place, keeping the file's length. With no sidecar
    // present, the receiver has to work out what survived by hashing.
    let mut on_disk = std::fs::read(&dest).unwrap();
    let cs = CHUNK as usize;
    on_disk[cs..cs * 2].fill(0);
    on_disk[cs * 3..cs * 4].fill(0);
    std::fs::write(&dest, &on_disk).unwrap();

    let queued = run_transfer(&src, &dst_dir.0, config()).await.unwrap();
    assert_eq!(
        queued,
        2 * CHUNK as u64,
        "only the two damaged chunks should travel"
    );
    assert_same(&std::fs::read(&dest).unwrap(), &data, "repaired file");
}

/// A TCP relay that forwards ciphertext to the real receiver and then drops
/// every connection once `budget` bytes have passed through.
///
/// This is how a transfer really dies — the peer walks out of range, or iOS
/// freezes the app — and it cuts at a byte count rather than a wall-clock
/// moment, so the test does not race the transfer.
async fn cutting_relay(upstream: String, budget: u64) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let forwarded = Arc::new(AtomicU64::new(0));

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };
            let Ok(outbound) = tokio::net::TcpStream::connect(&upstream).await else {
                break;
            };
            let (ri, wi) = inbound.into_split();
            let (ro, wo) = outbound.into_split();

            // Only the client-to-server direction is metered; that is where the
            // payload flows.
            tokio::spawn(pump(ri, wo, Some(forwarded.clone()), budget));
            tokio::spawn(pump(ro, wi, None, budget));
        }
    });
    addr
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    meter: Option<Arc<AtomicU64>>,
    budget: u64,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let Ok(n) = from.read(&mut buf).await else {
            break;
        };
        if n == 0 {
            break;
        }
        if let Some(meter) = &meter {
            if meter.fetch_add(n as u64, Ordering::Relaxed) + n as u64 > budget {
                // Drop both halves without a shutdown: a severed link, not a
                // polite close.
                break;
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_transfer_resumes_from_its_checkpoint() {
    let src_dir = TempDir::new("int-src");
    let dst_dir = TempDir::new("int-dst");
    let data = payload_of(64);
    let src = src_dir.0.join("payload.bin");
    std::fs::write(&src, &data).unwrap();

    let cfg = Config {
        stream_count: 1,
        ..config()
    };

    // --- pass one, cut mid-transfer ----------------------------------------
    let identity = HostIdentity::generate().unwrap();
    let fingerprint = identity.fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let real_addr = listener.local_addr().unwrap().to_string();

    let out = dst_dir.0.clone();
    let server = tokio::spawn(async move { recv::receive(&listener, &identity, &out, &cfg).await });

    // Enough for the handshake and manifest plus a few chunks, well short of
    // the whole file.
    let relay_addr = cutting_relay(real_addr, 8 * CHUNK as u64).await;
    let files = vec![OutgoingFile {
        path: src.clone(),
        relative_path: "payload.bin".into(),
        mime_type: "application/octet-stream".into(),
    }];
    let sent = send::send(&relay_addr, fingerprint, files, &cfg).await;
    let received = tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("receiver hung after the link was cut")
        .unwrap();

    assert!(sent.is_err(), "a severed link must fail the send");
    assert!(received.is_err(), "a severed link must fail the receive");

    let dest = dst_dir.0.join("payload.bin");
    assert!(
        resume::state_path(&dest).exists(),
        "an interrupted transfer must leave a resume record, or the next attempt starts over"
    );
    assert_differs(
        &std::fs::read(&dest).unwrap(),
        &data,
        "the interrupted file should still be incomplete",
    );

    // --- pass two, resuming over a healthy link -----------------------------
    let queued = run_transfer(&src, &dst_dir.0, cfg).await.unwrap();

    assert!(
        queued < data.len() as u64,
        "resume queued {queued} of {} bytes — it restarted from zero",
        data.len()
    );
    assert_same(&std::fs::read(&dest).unwrap(), &data, "resumed file");
    assert!(
        !resume::state_path(&dest).exists(),
        "the record must be cleared on completion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recorded_bitmap_is_honoured_exactly() {
    // The interruption test above proves resume works end to end but cannot
    // pin down *how much* is skipped, because where it stops depends on
    // timing. This one writes the record itself, so the saving is exact.
    let src_dir = TempDir::new("rec-src");
    let dst_dir = TempDir::new("rec-dst");
    let data = payload();
    let src = src_dir.0.join("payload.bin");
    std::fs::write(&src, &data).unwrap();

    run_transfer(&src, &dst_dir.0, config()).await.unwrap();
    let dest = dst_dir.0.join("payload.bin");

    // Claim the first 10 chunks are present and the rest are not.
    let hashes: Vec<[u8; 32]> = data
        .chunks(CHUNK as usize)
        .map(aetherlink_proto::chunk::hash_chunk)
        .collect();
    let key = resume::StateKey {
        root_hash: aetherlink_proto::chunk::root_from_chunk_hashes(&hashes),
        file_size: data.len() as u64,
        chunk_size: CHUNK,
    };
    let mut bitmap = aetherlink_proto::ChunkBitmap::new(CHUNKS as u64);
    for i in 0..10 {
        bitmap.set(i).unwrap();
    }
    resume::persist(&dest, &key, &bitmap).unwrap();

    let queued = run_transfer(&src, &dst_dir.0, config()).await.unwrap();
    assert_eq!(
        queued,
        (CHUNKS as u64 - 10) * CHUNK as u64,
        "exactly the chunks the record marked missing should travel"
    );
    assert_same(&std::fs::read(&dest).unwrap(), &data, "destination");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_can_be_switched_off() {
    let src_dir = TempDir::new("off-src");
    let dst_dir = TempDir::new("off-dst");
    let data = payload();
    let src = src_dir.0.join("payload.bin");
    std::fs::write(&src, &data).unwrap();

    run_transfer(&src, &dst_dir.0, config()).await.unwrap();

    let no_resume = Config {
        resume: false,
        ..config()
    };
    let queued = run_transfer(&src, &dst_dir.0, no_resume).await.unwrap();
    assert_eq!(
        queued,
        data.len() as u64,
        "with resume off, an identical destination is still re-sent in full"
    );
    assert_same(
        &std::fs::read(dst_dir.0.join("payload.bin")).unwrap(),
        &data,
        "destination",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_destination_transfers_nothing() {
    let src_dir = TempDir::new("done-src");
    let dst_dir = TempDir::new("done-dst");
    let data = payload();
    let src = src_dir.0.join("payload.bin");
    std::fs::write(&src, &data).unwrap();

    run_transfer(&src, &dst_dir.0, config()).await.unwrap();
    let queued = run_transfer(&src, &dst_dir.0, config()).await.unwrap();

    assert_eq!(queued, 0, "an already-complete file needs no bytes at all");
    assert_same(
        &std::fs::read(dst_dir.0.join("payload.bin")).unwrap(),
        &data,
        "destination",
    );
}
