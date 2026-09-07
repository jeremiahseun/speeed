//! Drives a real transfer through the FFI surface, exactly as Kotlin and Swift
//! will. Bindings generating cleanly proves nothing about whether the API
//! works; this does.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aetherlink_ffi::{
    EngineError, EngineSession, FileItem, SessionState, TransferConfig, TransferObserver,
};

#[derive(Default)]
struct Recorder {
    states: Mutex<Vec<SessionState>>,
    progress: Mutex<Vec<(u64, u64, f64)>>,
    completed_files: Mutex<Vec<String>>,
    finished: Mutex<Option<(u64, u64)>>,
    errors: Mutex<Vec<String>>,
}

impl TransferObserver for Recorder {
    fn on_state_changed(&self, state: SessionState) {
        self.states.lock().unwrap().push(state);
    }
    fn on_progress(&self, done: u64, total: u64, mbps: f64) {
        self.progress.lock().unwrap().push((done, total, mbps));
    }
    fn on_file_completed(&self, path: String) {
        self.completed_files.lock().unwrap().push(path);
    }
    fn on_finished(&self, bytes: u64, elapsed_millis: u64) {
        *self.finished.lock().unwrap() = Some((bytes, elapsed_millis));
    }
    fn on_error(&self, message: String) {
        self.errors.lock().unwrap().push(message);
    }
}

impl Recorder {
    fn wait_for_terminal(&self, timeout: Duration) -> Option<SessionState> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(s) = self.states.lock().unwrap().iter().rev().find(|s| {
                matches!(
                    s,
                    SessionState::Completed | SessionState::Failed | SessionState::Cancelled
                )
            }) {
                return Some(*s);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        None
    }
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "aetherlink-ffi-{tag}-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
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

fn small_config() -> TransferConfig {
    TransferConfig {
        stream_count: 2,
        frame_size_bytes: 16 * 1024,
        chunk_size_bytes: 64 * 1024,
        ..TransferConfig::default()
    }
}

#[test]
fn a_transfer_runs_end_to_end_through_the_ffi_surface() {
    let src_dir = TempDir::new("src");
    let dst_dir = TempDir::new("dst");
    let payload: Vec<u8> = (0..(64 * 1024 * 3 + 99)).map(|i| (i % 251) as u8).collect();
    let src = src_dir.0.join("movie.mp4");
    std::fs::write(&src, &payload).unwrap();

    let host_obs = Arc::new(Recorder::default());
    let host = EngineSession::new(small_config(), host_obs.clone()).unwrap();
    let handle = host
        .start_host(0, dst_dir.0.to_string_lossy().into_owned())
        .expect("host should bind");

    assert_ne!(
        handle.port, 0,
        "an ephemeral bind must report its real port"
    );
    assert_eq!(handle.fingerprint_hex.len(), 64);
    assert_eq!(host.state(), SessionState::Listening);

    let client_obs = Arc::new(Recorder::default());
    let client = EngineSession::new(small_config(), client_obs.clone()).unwrap();
    client
        .start_client(
            "127.0.0.1".into(),
            handle.port,
            handle.fingerprint_hex.clone(),
            vec![FileItem {
                path: src.to_string_lossy().into_owned(),
                relative_path: "movie.mp4".into(),
                mime_type: "video/mp4".into(),
            }],
        )
        .expect("client should start");

    assert_eq!(
        host_obs.wait_for_terminal(Duration::from_secs(30)),
        Some(SessionState::Completed),
        "host errors: {:?}",
        host_obs.errors.lock().unwrap()
    );
    assert_eq!(
        client_obs.wait_for_terminal(Duration::from_secs(30)),
        Some(SessionState::Completed),
        "client errors: {:?}",
        client_obs.errors.lock().unwrap()
    );

    // The file actually landed, byte for byte.
    assert_eq!(std::fs::read(dst_dir.0.join("movie.mp4")).unwrap(), payload);

    // The observer saw the things a UI needs.
    let finished = host_obs.finished.lock().unwrap();
    assert_eq!(finished.map(|(b, _)| b), Some(payload.len() as u64));
    assert!(
        !host_obs.progress.lock().unwrap().is_empty(),
        "at least one progress update must reach the UI"
    );
    assert_eq!(host_obs.completed_files.lock().unwrap().len(), 1);
    assert!(host_obs.errors.lock().unwrap().is_empty());
}

#[test]
fn the_session_is_reusable_after_a_transfer_completes() {
    let dst = TempDir::new("reuse");
    let obs = Arc::new(Recorder::default());
    let session = EngineSession::new(small_config(), obs.clone()).unwrap();

    let first = session
        .start_host(0, dst.0.to_string_lossy().into_owned())
        .unwrap();
    session.cancel();

    // A cancelled session must hand its slot back, or every session object is
    // single-use and the app has to rebuild one per transfer.
    let second = session.start_host(0, dst.0.to_string_lossy().into_owned());
    assert!(second.is_ok(), "session should be reusable, got {second:?}");
    assert_ne!(first.port, second.unwrap().port);
    session.cancel();
}

#[test]
fn a_second_concurrent_start_is_refused() {
    let dst = TempDir::new("busy");
    let obs = Arc::new(Recorder::default());
    let session = EngineSession::new(small_config(), obs).unwrap();

    session
        .start_host(0, dst.0.to_string_lossy().into_owned())
        .unwrap();
    let second = session.start_host(0, dst.0.to_string_lossy().into_owned());
    assert!(matches!(second, Err(EngineError::Busy)), "got {second:?}");
    session.cancel();
}

#[test]
fn a_malformed_fingerprint_is_rejected_before_any_socket_is_opened() {
    let obs = Arc::new(Recorder::default());
    let session = EngineSession::new(small_config(), obs).unwrap();

    for bad in ["", "abc", &"z".repeat(64), &"aa".repeat(31)] {
        let r = session.start_client("127.0.0.1".into(), 1234, bad.to_string(), vec![]);
        assert!(
            matches!(r, Err(EngineError::BadFingerprint(_))),
            "{bad:?} -> {r:?}"
        );
    }
    // A rejected fingerprint must not have consumed the session.
    assert_eq!(session.state(), SessionState::Idle);
}

#[test]
fn zero_stream_count_resolves_against_the_core_count() {
    let obs = Arc::new(Recorder::default());
    let session = EngineSession::new(TransferConfig::default(), obs).unwrap();
    let n = session.resolved_stream_count();
    assert!(
        (2..=8).contains(&n),
        "resolved {n}, expected the 2..=8 clamp"
    );
}

#[test]
fn cancelling_an_idle_session_is_harmless() {
    let obs = Arc::new(Recorder::default());
    let session = EngineSession::new(small_config(), obs.clone()).unwrap();
    session.cancel();
    session.cancel();
    assert_eq!(session.state(), SessionState::Idle);
    assert!(obs.errors.lock().unwrap().is_empty());
}
