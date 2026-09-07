//! AetherLink wire protocol: framing, chunk geometry, resume bitmaps, and the
//! transfer manifest.
//!
//! This crate is pure logic with no I/O and no async runtime, so it can be
//! exhaustively unit-tested and reused unchanged by the engine, the CLI
//! harness, and any future conformance tooling.
//!
//! See `docs/PRD-v1.1.md` §5 and §8.

#![forbid(unsafe_code)]

pub mod bitmap;
pub mod chunk;
pub mod frame;
pub mod manifest;

pub use bitmap::ChunkBitmap;
pub use chunk::{ChunkHashSet, ChunkLayout, Hash, DEFAULT_CHUNK_SIZE};
pub use frame::{Flags, FrameHeader, MsgType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};
pub use manifest::{sanitize_relative_path, FileMetadata, Manifest, SessionId};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("frame header truncated: got {0} bytes, need {HEADER_LEN}")]
    ShortHeader(usize),
    #[error("bad frame magic 0x{0:04X}, expected 0x{MAGIC:04X}")]
    BadMagic(u16),
    #[error("unsupported protocol version {0}, this build speaks {PROTOCOL_VERSION}")]
    UnsupportedVersion(u8),
    #[error("unknown message type {0}")]
    UnknownMessageType(u8),
    #[error("unknown frame flags set: 0x{0:04X}")]
    UnknownFlags(u16),
    #[error("reserved header field is not zero")]
    ReservedNotZero,
    #[error("frame payload of {0} bytes exceeds the maximum")]
    FrameTooLarge(u32),
    #[error("keepalive frame carries a {0}-byte payload")]
    KeepaliveWithPayload(u32),

    #[error("invalid chunk size {0}: must be a power of two within the accepted band")]
    InvalidChunkSize(u32),
    #[error("chunk index {index} out of range for a file of {count} chunks")]
    ChunkOutOfRange { index: u64, count: u64 },
    #[error("offset {offset} out of range for a file of {file_size} bytes")]
    OffsetOutOfRange { offset: u64, file_size: u64 },
    #[error("conflicting hashes recorded for chunk {0}")]
    ChunkHashConflict(u64),

    #[error("bitmap is {got} bytes, expected {expected} for this file")]
    BitmapLengthMismatch { got: usize, expected: usize },
    #[error("bitmap has padding bits set past the final chunk")]
    BitmapPaddingSet,

    #[error("manifest offers {0} files, exceeding the maximum")]
    ManifestTooLarge(usize),
    #[error("manifest repeats file id {0}")]
    DuplicateFileId(u64),
    #[error("manifest resolves two entries to the same path: {0}")]
    DuplicatePath(String),
    #[error("unsafe path {path:?}: {reason}")]
    UnsafePath { path: String, reason: String },
}
