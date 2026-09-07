//! AetherLink transfer engine.
//!
//! Multi-stream TCP with TLS 1.3, a single-copy I/O pipeline, and BLAKE3
//! verification that completes with the last packet. See `docs/PRD-v1.1.md`.
//!
//! The transport choice is deliberate and inverts PRD v1.0: on a one-hop link,
//! TCP gets kernel segmentation offload and a mature congestion controller for
//! free, while userspace QUIC pays a syscall per datagram on platforms where
//! UDP GSO is unreliable (Android) or absent (iOS).

pub mod control;
pub mod io;
pub mod recv;
pub mod send;
pub mod tls;

pub use recv::receive;
pub use send::{send, OutgoingFile};
pub use tls::{Fingerprint, HostIdentity};

use aetherlink_proto::chunk::DEFAULT_CHUNK_SIZE;
use aetherlink_proto::frame::DEFAULT_FRAME_PAYLOAD;

/// Tuning knobs. Defaults come from PRD §5.1 and §5.3.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Parallel data streams. Sized to saturate cores with AEAD work, not to
    /// avoid head-of-line blocking.
    pub stream_count: u8,
    /// Wire frame payload. Large enough to keep the socket buffer full.
    pub frame_size: u32,
    /// Verification and resume granularity.
    pub chunk_size: u32,
    /// Most streams a peer may ask us to accept. `stream_count` arrives in the
    /// peer's Hello and drives an accept loop, so it needs a ceiling.
    pub max_streams: u8,
    /// Largest single file we will pre-allocate for. Stops a hostile manifest
    /// from filling the disk before the user has seen a prompt.
    pub max_file_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stream_count: 8,
            frame_size: DEFAULT_FRAME_PAYLOAD,
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_streams: 32,
            max_file_bytes: 2 << 40, // 2 TiB
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TransferStats {
    pub bytes: u64,
    pub elapsed: std::time::Duration,
}

impl TransferStats {
    pub fn megabytes_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        (self.bytes as f64 / 1_000_000.0) / secs
    }

    pub fn megabits_per_second(&self) -> f64 {
        self.megabytes_per_second() * 8.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("tls: {0}")]
    Tls(String),
    #[error("io: {0}")]
    Io(String),
    #[error(transparent)]
    Wire(#[from] aetherlink_proto::Error),
    #[error("chunk at offset {offset} failed verification")]
    ChunkVerificationFailed { offset: u64 },
    #[error("transfer incomplete: {0}")]
    Incomplete(String),
    #[error("peer aborted: {0}")]
    PeerAborted(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}
