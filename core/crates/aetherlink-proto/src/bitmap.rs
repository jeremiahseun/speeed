//! Resume bitmap: one bit per chunk, LSB-first within each byte (PRD §8).
//!
//! A bit is set only once its chunk has been hash-verified *and* fsynced. That
//! ordering is the whole point — setting it on write completion alone would let
//! a power loss leave a chunk marked present that resume then never repairs.
//!
//! A 10 GB file at 4 MB chunks needs 2385 bits: 299 bytes on the wire.

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkBitmap {
    bits: Vec<u8>,
    count: u64,
    set_count: u64,
}

impl ChunkBitmap {
    pub fn new(count: u64) -> Self {
        Self {
            bits: vec![0u8; count.div_ceil(8) as usize],
            count,
            set_count: 0,
        }
    }

    /// Parses a peer-supplied bitmap. Rejects a byte length that disagrees with
    /// `count`, and any padding bit set past the end — either means the peer is
    /// describing a different file than we are.
    pub fn from_bytes(bytes: &[u8], count: u64) -> Result<Self, Error> {
        let expected = count.div_ceil(8) as usize;
        if bytes.len() != expected {
            return Err(Error::BitmapLengthMismatch {
                got: bytes.len(),
                expected,
            });
        }
        let trailing = (expected as u64 * 8) - count;
        if trailing > 0 {
            if let Some(&last) = bytes.last() {
                let padding_mask = !(0xFFu8 >> trailing);
                if last & padding_mask != 0 {
                    return Err(Error::BitmapPaddingSet);
                }
            }
        }
        let set_count = bytes.iter().map(|b| b.count_ones() as u64).sum();
        Ok(Self {
            bits: bytes.to_vec(),
            count,
            set_count,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn set_count(&self) -> u64 {
        self.set_count
    }
    pub fn is_complete(&self) -> bool {
        self.set_count == self.count
    }

    pub fn get(&self, index: u64) -> Result<bool, Error> {
        if index >= self.count {
            return Err(Error::ChunkOutOfRange {
                index,
                count: self.count,
            });
        }
        Ok(self.bits[(index / 8) as usize] & (1 << (index % 8)) != 0)
    }

    /// Marks a chunk verified. Idempotent, so a duplicate frame after a resume
    /// does not corrupt the count.
    pub fn set(&mut self, index: u64) -> Result<(), Error> {
        if index >= self.count {
            return Err(Error::ChunkOutOfRange {
                index,
                count: self.count,
            });
        }
        let byte = &mut self.bits[(index / 8) as usize];
        let mask = 1u8 << (index % 8);
        if *byte & mask == 0 {
            *byte |= mask;
            self.set_count += 1;
        }
        Ok(())
    }

    /// Contiguous runs of *missing* chunks as half-open `[start, end)` ranges.
    /// This is what the sender iterates on resume, so it transmits long runs
    /// rather than re-deciding per chunk.
    pub fn missing_ranges(&self) -> Vec<(u64, u64)> {
        let mut ranges = Vec::new();
        let mut run_start: Option<u64> = None;
        for i in 0..self.count {
            let present = self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0;
            match (present, run_start) {
                (false, None) => run_start = Some(i),
                (true, Some(start)) => {
                    ranges.push((start, i));
                    run_start = None;
                }
                _ => {}
            }
        }
        if let Some(start) = run_start {
            ranges.push((start, self.count));
        }
        ranges
    }

    /// Bytes still to transfer, given the file's chunk geometry.
    pub fn remaining_bytes(&self, layout: &crate::chunk::ChunkLayout) -> Result<u64, Error> {
        let mut total = 0u64;
        for (start, end) in self.missing_ranges() {
            for i in start..end {
                total += layout.range(i)?.1 as u64;
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ChunkLayout, DEFAULT_CHUNK_SIZE};

    #[test]
    fn set_and_get_roundtrip() {
        let mut b = ChunkBitmap::new(20);
        assert!(!b.get(7).unwrap());
        b.set(7).unwrap();
        assert!(b.get(7).unwrap());
        assert!(!b.get(8).unwrap());
        assert_eq!(b.set_count(), 1);
    }

    #[test]
    fn setting_twice_does_not_double_count() {
        let mut b = ChunkBitmap::new(10);
        b.set(3).unwrap();
        b.set(3).unwrap();
        assert_eq!(b.set_count(), 1);
    }

    #[test]
    fn is_lsb_first_within_each_byte() {
        let mut b = ChunkBitmap::new(16);
        b.set(0).unwrap();
        b.set(9).unwrap();
        assert_eq!(b.as_bytes(), &[0b0000_0001, 0b0000_0010]);
    }

    #[test]
    fn ten_gigabyte_bitmap_is_299_bytes() {
        let layout = ChunkLayout::new(10_000_000_000, DEFAULT_CHUNK_SIZE).unwrap();
        let b = ChunkBitmap::new(layout.count());
        assert_eq!(layout.count(), 2385);
        assert_eq!(b.as_bytes().len(), 299);
    }

    #[test]
    fn rejects_a_bitmap_sized_for_a_different_file() {
        assert!(matches!(
            ChunkBitmap::from_bytes(&[0u8; 4], 20),
            Err(Error::BitmapLengthMismatch {
                got: 4,
                expected: 3
            })
        ));
    }

    #[test]
    fn rejects_padding_bits_set_past_the_end() {
        // count 20 -> 3 bytes, top 4 bits of byte 2 are padding.
        assert!(matches!(
            ChunkBitmap::from_bytes(&[0xFF, 0xFF, 0b1111_1111], 20),
            Err(Error::BitmapPaddingSet)
        ));
        // Same bytes with the padding cleared parse fine.
        let ok = ChunkBitmap::from_bytes(&[0xFF, 0xFF, 0b0000_1111], 20).unwrap();
        assert!(ok.is_complete());
    }

    #[test]
    fn roundtrips_through_bytes_preserving_the_count() {
        let mut b = ChunkBitmap::new(100);
        for i in [0, 5, 63, 64, 99] {
            b.set(i).unwrap();
        }
        let parsed = ChunkBitmap::from_bytes(b.as_bytes(), 100).unwrap();
        assert_eq!(parsed, b);
        assert_eq!(parsed.set_count(), 5);
    }

    #[test]
    fn missing_ranges_coalesces_runs() {
        let mut b = ChunkBitmap::new(10);
        for i in [0, 1, 2, 6] {
            b.set(i).unwrap();
        }
        assert_eq!(b.missing_ranges(), vec![(3, 6), (7, 10)]);
    }

    #[test]
    fn missing_ranges_handles_the_empty_and_full_extremes() {
        let empty = ChunkBitmap::new(5);
        assert_eq!(empty.missing_ranges(), vec![(0, 5)]);

        let mut full = ChunkBitmap::new(5);
        for i in 0..5 {
            full.set(i).unwrap();
        }
        assert!(full.missing_ranges().is_empty());
        assert!(full.is_complete());
    }

    #[test]
    fn remaining_bytes_accounts_for_the_short_final_chunk() {
        let layout = ChunkLayout::new(10 * 1024 * 1024, DEFAULT_CHUNK_SIZE).unwrap();
        let mut b = ChunkBitmap::new(layout.count());
        b.set(0).unwrap();
        // Chunks 1 (4 MB) and 2 (2 MB) remain.
        assert_eq!(b.remaining_bytes(&layout).unwrap(), 6 * 1024 * 1024);
    }
}
