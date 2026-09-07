//! Fixed-size binary frame header for the data streams.
//!
//! Control messages travel as `MsgType::Control` payloads on stream 0; bulk file
//! bytes travel as `MsgType::FileData` on the remaining streams. There is no
//! per-frame checksum: every frame is carried inside a TLS 1.3 record, which is
//! already AEAD-authenticated, so a second MAC here would be pure overhead.

use crate::Error;

/// Sentinel identifying an AetherLink frame.
pub const MAGIC: u16 = 0xAE10;

/// Wire protocol version. Bump on any incompatible header or control change.
pub const PROTOCOL_VERSION: u8 = 1;

/// Header length in bytes. 32 rather than the 24 named in PRD v1.1 §5.3 — the
/// fields the spec lists sum to 28, and 32 restores the 16-byte alignment the
/// original design asked for.
pub const HEADER_LEN: usize = 32;

/// Default bulk payload size. Large enough to keep the socket buffer full,
/// small enough that one stalled stream does not hold megabytes hostage.
pub const DEFAULT_FRAME_PAYLOAD: u32 = 256 * 1024;

/// Upper bound accepted from a peer. Guards against a hostile length field
/// driving an unbounded allocation before we have read the body.
pub const MAX_FRAME_PAYLOAD: u32 = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    /// Length-delimited control message (see `control` module).
    Control = 0,
    /// Bulk file bytes at `offset` within `file_id`.
    FileData = 1,
    /// Sender has transmitted every byte it holds for `file_id`.
    FileEnd = 2,
    /// Idle-link probe. Carries no payload.
    Keepalive = 3,
}

impl MsgType {
    fn from_u8(v: u8) -> Result<Self, Error> {
        Ok(match v {
            0 => MsgType::Control,
            1 => MsgType::FileData,
            2 => MsgType::FileEnd,
            3 => MsgType::Keepalive,
            other => return Err(Error::UnknownMessageType(other)),
        })
    }
}

/// Frame flags. Bits outside this set are rejected so that a future sender
/// cannot silently change semantics we do not implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags(u16);

impl Flags {
    pub const NONE: Flags = Flags(0);
    /// Final frame of this file on this stream.
    pub const END_OF_FILE: Flags = Flags(1 << 0);

    const KNOWN: u16 = 1 << 0;

    pub fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }
    pub fn union(self, other: Flags) -> Flags {
        Flags(self.0 | other.0)
    }
    pub fn bits(self) -> u16 {
        self.0
    }
}

/// ```text
///  0..2   magic u16          8..16  file_id u64
///  2      version u8        16..24  offset  u64
///  3      msg_type u8       24..28  length  u32
///  4..6   flags u16         28..32  reserved u32
///  6..8   reserved u16
/// ```
/// All fields little-endian; both target architectures are LE, so there is no
/// byte-swap on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub msg_type: MsgType,
    pub flags: Flags,
    pub file_id: u64,
    pub offset: u64,
    pub length: u32,
}

impl FrameHeader {
    pub fn new(msg_type: MsgType, file_id: u64, offset: u64, length: u32) -> Self {
        Self {
            msg_type,
            flags: Flags::NONE,
            file_id,
            offset,
            length,
        }
    }

    pub fn with_flags(mut self, flags: Flags) -> Self {
        self.flags = flags;
        self
    }

    pub fn encode(&self, out: &mut [u8; HEADER_LEN]) {
        out[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        out[2] = PROTOCOL_VERSION;
        out[3] = self.msg_type as u8;
        out[4..6].copy_from_slice(&self.flags.bits().to_le_bytes());
        out[6..8].copy_from_slice(&0u16.to_le_bytes());
        out[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.offset.to_le_bytes());
        out[24..28].copy_from_slice(&self.length.to_le_bytes());
        out[28..32].copy_from_slice(&0u32.to_le_bytes());
    }

    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        self.encode(&mut buf);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < HEADER_LEN {
            return Err(Error::ShortHeader(buf.len()));
        }
        let magic = u16::from_le_bytes([buf[0], buf[1]]);
        if magic != MAGIC {
            return Err(Error::BadMagic(magic));
        }
        let version = buf[2];
        if version != PROTOCOL_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let msg_type = MsgType::from_u8(buf[3])?;

        let raw_flags = u16::from_le_bytes([buf[4], buf[5]]);
        if raw_flags & !Flags::KNOWN != 0 {
            return Err(Error::UnknownFlags(raw_flags));
        }
        // Reserved fields must be zero so we can assign them later without
        // colliding with a peer that filled them with junk.
        if u16::from_le_bytes([buf[6], buf[7]]) != 0
            || u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]) != 0
        {
            return Err(Error::ReservedNotZero);
        }

        let length = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]);
        if length > MAX_FRAME_PAYLOAD {
            return Err(Error::FrameTooLarge(length));
        }
        // A keepalive carrying a payload is a peer we do not understand.
        if msg_type == MsgType::Keepalive && length != 0 {
            return Err(Error::KeepaliveWithPayload(length));
        }

        Ok(FrameHeader {
            msg_type,
            flags: Flags(raw_flags),
            file_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            length,
        })
    }

    /// End offset of this frame's payload, or `None` on overflow.
    pub fn end_offset(&self) -> Option<u64> {
        self.offset.checked_add(self.length as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FrameHeader {
        FrameHeader::new(
            MsgType::FileData,
            0x0123_4567_89AB_CDEF,
            4 * 1024 * 1024,
            262_144,
        )
        .with_flags(Flags::END_OF_FILE)
    }

    #[test]
    fn roundtrip_preserves_every_field() {
        let h = sample();
        assert_eq!(FrameHeader::decode(&h.to_bytes()).unwrap(), h);
    }

    #[test]
    fn header_is_exactly_32_bytes() {
        assert_eq!(sample().to_bytes().len(), HEADER_LEN);
        assert_eq!(HEADER_LEN % 16, 0, "header must stay 16-byte aligned");
    }

    #[test]
    fn encodes_little_endian_magic_first() {
        let bytes = sample().to_bytes();
        assert_eq!(&bytes[0..2], &[0x10, 0xAE]);
        assert_eq!(bytes[2], PROTOCOL_VERSION);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = sample().to_bytes();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::BadMagic(_))
        ));
    }

    #[test]
    fn rejects_future_version() {
        let mut bytes = sample().to_bytes();
        bytes[2] = PROTOCOL_VERSION + 1;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn rejects_unknown_message_type() {
        let mut bytes = sample().to_bytes();
        bytes[3] = 99;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::UnknownMessageType(99))
        ));
    }

    #[test]
    fn rejects_unknown_flag_bits() {
        let mut bytes = sample().to_bytes();
        bytes[4..6].copy_from_slice(&0x8000u16.to_le_bytes());
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::UnknownFlags(_))
        ));
    }

    #[test]
    fn rejects_nonzero_reserved() {
        let mut bytes = sample().to_bytes();
        bytes[6] = 1;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::ReservedNotZero)
        ));

        let mut bytes = sample().to_bytes();
        bytes[31] = 1;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::ReservedNotZero)
        ));
    }

    #[test]
    fn rejects_oversized_length_before_allocating() {
        let mut bytes = sample().to_bytes();
        bytes[24..28].copy_from_slice(&(MAX_FRAME_PAYLOAD + 1).to_le_bytes());
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(Error::FrameTooLarge(_))
        ));
    }

    #[test]
    fn rejects_keepalive_carrying_payload() {
        let h = FrameHeader::new(MsgType::Keepalive, 0, 0, 64);
        assert!(matches!(
            FrameHeader::decode(&h.to_bytes()),
            Err(Error::KeepaliveWithPayload(64))
        ));
    }

    #[test]
    fn rejects_truncated_buffer() {
        let bytes = sample().to_bytes();
        assert!(matches!(
            FrameHeader::decode(&bytes[..HEADER_LEN - 1]),
            Err(Error::ShortHeader(31))
        ));
    }

    #[test]
    fn end_offset_reports_overflow_rather_than_wrapping() {
        let h = FrameHeader::new(MsgType::FileData, 1, u64::MAX - 4, 128);
        assert_eq!(h.end_offset(), None);
    }
}
