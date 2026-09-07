//! UniFFI surface for Android (Kotlin) and iOS (Swift).
//!
//! Two rules here are requirements of UniFFI rather than preferences, and PRD
//! v1.0 got both wrong:
//!
//! * exported methods take `&self`, never `&mut self`, so all mutable state
//!   lives behind interior mutability;
//! * foreign-implemented traits arrive as `Arc<dyn Trait>`, not `Box`.
//!
//! Every `start_*` call returns immediately. The transfer runs on an owned
//! Tokio runtime and reports through the observer, so the platform's main
//! thread is never blocked.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aetherlink_core::{recv, send, Config, HostIdentity, OutgoingFile, ProgressSink, Throttled};

uniffi::setup_scaffolding!();

// ---------------------------------------------------------------- data types

#[derive(Debug, Clone, uniffi::Record)]
pub struct TransferConfig {
    /// Parallel data streams. **Zero means derive from the core count**, which
    /// is what the Sprint 1 benchmark says to do: four streams beat eight on a
    /// four-core machine and sixteen was clearly worse, because streams exist
    /// to saturate cores with AEAD work.
    #[uniffi(default = 0)]
    pub stream_count: u8,
    #[uniffi(default = 262144)]
    pub frame_size_bytes: u32,
    #[uniffi(default = 4194304)]
    pub chunk_size_bytes: u32,
    /// Ceiling on the stream count a peer may request of us.
    #[uniffi(default = 32)]
    pub max_streams: u8,
    /// Largest single file we will pre-allocate for.
    #[uniffi(default = 2199023255552)]
    pub max_file_bytes: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            stream_count: 0,
            frame_size_bytes: 262_144,
            chunk_size_bytes: 4 * 1024 * 1024,
            max_streams: 32,
            max_file_bytes: 2 << 40,
        }
    }
}

impl TransferConfig {
    fn resolve(&self) -> Config {
        Config {
            stream_count: if self.stream_count == 0 {
                default_stream_count()
            } else {
                self.stream_count
            },
            frame_size: self.frame_size_bytes,
            chunk_size: self.chunk_size_bytes,
            max_streams: self.max_streams,
            max_file_bytes: self.max_file_bytes,
        }
    }
}

/// Streams track available parallelism. Clamped to 2..=8: one stream cannot
/// keep the pipe full, and past eight the scheduling overhead outweighs the
/// added throughput even on a big-core phone.
fn default_stream_count() -> u8 {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    cores.clamp(2, 8) as u8
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct FileItem {
    /// Absolute path on this device. On Android, resolve a `content://` URI to
    /// a real path or an fd-backed path before passing it here.
    pub path: String,
    /// Path the receiver should recreate, relative to its output directory.
    /// Sanitized on arrival, so `../` here is rejected rather than honoured.
    pub relative_path: String,
    pub mime_type: String,
}

/// What the host must publish in its QR payload.
#[derive(Debug, Clone, uniffi::Record)]
pub struct HostHandle {
    pub port: u16,
    /// 64 hex characters. The client pins this; without it the TLS handshake
    /// fails, which is what stops anyone who merely joined the Wi-Fi group.
    pub fingerprint_hex: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SessionState {
    Idle,
    Listening,
    Transferring,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
#[uniffi(flat_error)]
pub enum EngineError {
    #[error("a session is already running")]
    Busy,
    #[error("invalid fingerprint: {0}")]
    BadFingerprint(String),
    #[error("{0}")]
    Engine(String),
}

impl From<aetherlink_core::Error> for EngineError {
    fn from(e: aetherlink_core::Error) -> Self {
        EngineError::Engine(e.to_string())
    }
}

// ----------------------------------------------------------------- observer

/// Implemented on the platform side. Calls arrive on engine threads, not the
/// main thread — hop to the UI thread before touching views.
///
/// `on_progress` is rate-limited to about ten calls a second, so it is safe to
/// use directly for a progress bar.
#[uniffi::export(with_foreign)]
pub trait TransferObserver: Send + Sync {
    fn on_state_changed(&self, state: SessionState);
    fn on_progress(&self, bytes_done: u64, bytes_total: u64, megabytes_per_second: f64);
    fn on_file_completed(&self, path: String);
    fn on_finished(&self, bytes: u64, elapsed_millis: u64);
    fn on_error(&self, message: String);
}

/// Adapts the engine's [`ProgressSink`] to the foreign observer, adding the
/// throughput figure the UI wants but the engine does not track.
struct ObserverBridge {
    observer: Arc<dyn TransferObserver>,
    started: Instant,
}

impl ProgressSink for ObserverBridge {
    fn on_progress(&self, bytes_done: u64, bytes_total: u64) {
        let secs = self.started.elapsed().as_secs_f64();
        let mbps = if secs > 0.0 {
            (bytes_done as f64 / 1_000_000.0) / secs
        } else {
            0.0
        };
        self.observer.on_progress(bytes_done, bytes_total, mbps);
    }

    fn on_file_completed(&self, _file_id: u64, path: &str) {
        self.observer.on_file_completed(path.to_string());
    }
}

// ------------------------------------------------------------------ session

#[derive(uniffi::Object)]
pub struct EngineSession {
    config: TransferConfig,
    observer: Arc<dyn TransferObserver>,
    runtime: tokio::runtime::Runtime,
    state: Mutex<SessionState>,
    running: Arc<AtomicBool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[uniffi::export]
impl EngineSession {
    #[uniffi::constructor]
    pub fn new(
        config: TransferConfig,
        observer: Arc<dyn TransferObserver>,
    ) -> Result<Arc<Self>, EngineError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(default_stream_count() as usize)
            .enable_all()
            .thread_name("aetherlink")
            .build()
            .map_err(|e| EngineError::Engine(format!("starting runtime: {e}")))?;

        Ok(Arc::new(Self {
            config,
            observer,
            runtime,
            state: Mutex::new(SessionState::Idle),
            running: Arc::new(AtomicBool::new(false)),
            task: Mutex::new(None),
        }))
    }

    /// Binds a listener and returns what the QR payload needs. The transfer
    /// itself runs in the background; results arrive on the observer.
    ///
    /// Pass `port = 0` to let the OS choose; the assigned port comes back in
    /// the handle.
    pub fn start_host(&self, port: u16, output_dir: String) -> Result<HostHandle, EngineError> {
        self.claim()?;

        let identity = HostIdentity::generate().map_err(EngineError::from)?;
        let fingerprint_hex = identity.fingerprint_hex();

        // Binding is effectively instant, and the caller needs the real port
        // before it can render a QR code, so this one step is synchronous.
        let listener = match self.runtime.block_on(recv::bind(port)) {
            Ok(l) => l,
            Err(e) => {
                // Hand the slot back; a failed bind is not a running session.
                self.running.store(false, Ordering::SeqCst);
                return Err(e.into());
            }
        };
        let bound_port = listener
            .local_addr()
            .map_err(|e| EngineError::Engine(format!("reading bound port: {e}")))?
            .port();

        let config = self.config.resolve();
        let observer = self.observer.clone();
        let sink: Arc<dyn ProgressSink> =
            Arc::new(Throttled::with_default_interval(Arc::new(ObserverBridge {
                observer: observer.clone(),
                started: Instant::now(),
            })));
        let out = PathBuf::from(output_dir);

        self.set_state(SessionState::Listening);
        self.spawn(async move {
            recv::receive_with_progress(&listener, &identity, &out, &config, sink).await
        });

        Ok(HostHandle {
            port: bound_port,
            fingerprint_hex,
        })
    }

    /// Connects to a host, pinning `fingerprint_hex` from its QR payload, and
    /// sends `files`.
    pub fn start_client(
        &self,
        host_ip: String,
        port: u16,
        fingerprint_hex: String,
        files: Vec<FileItem>,
    ) -> Result<(), EngineError> {
        let pinned = parse_fingerprint(&fingerprint_hex)?;
        self.claim()?;

        let outgoing: Vec<OutgoingFile> = files
            .into_iter()
            .map(|f| OutgoingFile {
                path: PathBuf::from(f.path),
                relative_path: f.relative_path,
                mime_type: f.mime_type,
            })
            .collect();

        let addr = format!("{host_ip}:{port}");
        let config = self.config.resolve();
        let sink: Arc<dyn ProgressSink> =
            Arc::new(Throttled::with_default_interval(Arc::new(ObserverBridge {
                observer: self.observer.clone(),
                started: Instant::now(),
            })));

        self.set_state(SessionState::Transferring);
        self.spawn(async move {
            send::send_with_progress(&addr, pinned, outgoing, &config, sink).await
        });

        Ok(())
    }

    /// Stops the session. Safe to call at any time, including when idle.
    pub fn cancel(&self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
        if self.running.swap(false, Ordering::SeqCst) {
            self.set_state(SessionState::Cancelled);
        }
    }

    pub fn state(&self) -> SessionState {
        *self.state.lock().unwrap()
    }

    /// Streams this session will actually open, after resolving `0` against the
    /// core count. Exposed so the UI can show it in diagnostics.
    pub fn resolved_stream_count(&self) -> u8 {
        self.config.resolve().stream_count
    }
}

impl EngineSession {
    fn claim(&self) -> Result<(), EngineError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(EngineError::Busy);
        }
        Ok(())
    }

    fn set_state(&self, state: SessionState) {
        *self.state.lock().unwrap() = state;
        self.observer.on_state_changed(state);
    }

    /// Runs a transfer future, reporting its outcome exactly once.
    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<
                Output = Result<aetherlink_core::TransferStats, aetherlink_core::Error>,
            > + Send
            + 'static,
    {
        // The task cannot borrow `self`, so it owns what it needs to report and
        // to release the session slot.
        let reporter = Reporter {
            observer: self.observer.clone(),
            running: self.running.clone(),
        };
        let handle = self.runtime.spawn(async move {
            match fut.await {
                Ok(stats) => reporter.finished(stats),
                Err(e) => reporter.failed(&e.to_string()),
            }
        });
        *self.task.lock().unwrap() = Some(handle);
    }
}

/// Owns just enough to report an outcome from a detached task and hand the
/// session back. Without releasing `running`, a second transfer after a
/// successful one would be refused as Busy forever.
struct Reporter {
    observer: Arc<dyn TransferObserver>,
    running: Arc<AtomicBool>,
}

impl Reporter {
    fn finished(&self, stats: aetherlink_core::TransferStats) {
        self.running.store(false, Ordering::SeqCst);
        self.observer.on_state_changed(SessionState::Completed);
        self.observer
            .on_finished(stats.bytes, stats.elapsed.as_millis() as u64);
    }

    fn failed(&self, message: &str) {
        self.running.store(false, Ordering::SeqCst);
        self.observer.on_state_changed(SessionState::Failed);
        self.observer.on_error(message.to_string());
    }
}

fn parse_fingerprint(hex: &str) -> Result<[u8; 32], EngineError> {
    if hex.len() != 64 {
        return Err(EngineError::BadFingerprint(format!(
            "expected 64 hex characters, got {}",
            hex.len()
        )));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| EngineError::BadFingerprint(format!("bad hex at position {}", i * 2)))?;
    }
    Ok(out)
}

/// Protocol version this build speaks. Exposed so a QR payload can carry it,
/// and so a peer mismatch can be reported in the UI rather than as a socket
/// error.
#[uniffi::export]
pub fn protocol_version() -> u8 {
    aetherlink_core::PROTOCOL_VERSION
}
