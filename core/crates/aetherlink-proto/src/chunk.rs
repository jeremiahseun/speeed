//! Chunk geometry and BLAKE3 verification.
//!
//! Two sizes, deliberately distinct (PRD §5.3): the *frame* is the wire unit,
//! the *chunk* is the verification and resume unit. A chunk is hashed
//! independently so it can be verified the moment it lands, and the file root
//! commits to the ordered list of chunk hashes — so verification finishes with
//! the last packet rather than after a second full pass over the file.

use crate::Error;

/// Verification and resume granularity.
pub const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

/// Smallest chunk we accept from a peer. A tiny value would inflate the bitmap
/// and the per-chunk hashing overhead without bound.
pub const MIN_CHUNK_SIZE: u32 = 64 * 1024;
pub const MAX_CHUNK_SIZE: u32 = 64 * 1024 * 1024;

/// Domain separator, so a chunk hash can never be mistaken for a file root.
const ROOT_DOMAIN: &[u8] = b"aetherlink.file-root.v1";

pub type Hash = [u8; 32];

/// Chunk geometry for one file. Validated on construction so the rest of the
/// engine can index without re-checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLayout {
    file_size: u64,
    chunk_size: u32,
    count: u64,
}

impl ChunkLayout {
    pub fn new(file_size: u64, chunk_size: u32) -> Result<Self, Error> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(Error::InvalidChunkSize(chunk_size));
        }
        if !chunk_size.is_power_of_two() {
            return Err(Error::InvalidChunkSize(chunk_size));
        }
        let cs = chunk_size as u64;
        // Ceiling division; an empty file has exactly zero chunks.
        let count = file_size.div_ceil(cs);
        Ok(Self {
            file_size,
            chunk_size,
            count,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }
    /// Number of chunks. Zero for an empty file.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Byte range covered by `index`. The final chunk is short unless the file
    /// divides evenly.
    pub fn range(&self, index: u64) -> Result<(u64, u32), Error> {
        if index >= self.count {
            return Err(Error::ChunkOutOfRange {
                index,
                count: self.count,
            });
        }
        let offset = index * self.chunk_size as u64;
        let len = (self.file_size - offset).min(self.chunk_size as u64) as u32;
        Ok((offset, len))
    }

    /// Chunk containing `offset`.
    pub fn index_of(&self, offset: u64) -> Result<u64, Error> {
        if offset >= self.file_size {
            return Err(Error::OffsetOutOfRange {
                offset,
                file_size: self.file_size,
            });
        }
        Ok(offset / self.chunk_size as u64)
    }
}

pub fn hash_chunk(bytes: &[u8]) -> Hash {
    *blake3::hash(bytes).as_bytes()
}

/// File root over chunk hashes in index order.
pub fn root_from_chunk_hashes(chunk_hashes: &[Hash]) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ROOT_DOMAIN);
    hasher.update(&(chunk_hashes.len() as u64).to_le_bytes());
    for h in chunk_hashes {
        hasher.update(h);
    }
    *hasher.finalize().as_bytes()
}

/// Accumulates chunk hashes that arrive **out of order**, which is the normal
/// case with eight parallel streams. The root is only computable once every
/// slot is filled.
#[derive(Debug, Clone)]
pub struct ChunkHashSet {
    hashes: Vec<Option<Hash>>,
    filled: u64,
}

impl ChunkHashSet {
    pub fn new(count: u64) -> Self {
        Self {
            hashes: vec![None; count as usize],
            filled: 0,
        }
    }

    /// Records a chunk hash. Re-recording an identical hash is a no-op (a
    /// duplicate frame after a resume); a *conflicting* hash is an error, since
    /// it means the two peers disagree about the file's contents.
    pub fn insert(&mut self, index: u64, hash: Hash) -> Result<(), Error> {
        let count = self.hashes.len() as u64;
        let slot = self
            .hashes
            .get_mut(index as usize)
            .ok_or(Error::ChunkOutOfRange { index, count })?;
        match slot {
            Some(existing) if *existing == hash => Ok(()),
            Some(_) => Err(Error::ChunkHashConflict(index)),
            None => {
                *slot = Some(hash);
                self.filled += 1;
                Ok(())
            }
        }
    }

    pub fn is_complete(&self) -> bool {
        self.filled == self.hashes.len() as u64
    }

    /// Root over all chunk hashes, or `None` while any are still missing.
    pub fn root(&self) -> Option<Hash> {
        if !self.is_complete() {
            return None;
        }
        let ordered: Vec<Hash> = self.hashes.iter().map(|h| h.unwrap()).collect();
        Some(root_from_chunk_hashes(&ordered))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_covers_the_file_exactly() {
        let layout = ChunkLayout::new(10 * 1024 * 1024, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(layout.count(), 3);
        assert_eq!(layout.range(0).unwrap(), (0, DEFAULT_CHUNK_SIZE));
        assert_eq!(layout.range(2).unwrap(), (8 * 1024 * 1024, 2 * 1024 * 1024));

        let total: u64 = (0..layout.count())
            .map(|i| layout.range(i).unwrap().1 as u64)
            .sum();
        assert_eq!(
            total,
            layout.file_size(),
            "chunks must tile the file with no gap or overlap"
        );
    }

    #[test]
    fn exact_multiple_has_no_short_tail() {
        let layout = ChunkLayout::new(8 * 1024 * 1024, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(layout.count(), 2);
        assert_eq!(
            layout.range(1).unwrap(),
            (4 * 1024 * 1024, DEFAULT_CHUNK_SIZE)
        );
    }

    #[test]
    fn empty_file_has_zero_chunks() {
        let layout = ChunkLayout::new(0, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(layout.count(), 0);
        assert!(layout.range(0).is_err());
    }

    #[test]
    fn ten_gigabytes_indexes_without_overflow() {
        let ten_gb = 10_000_000_000u64;
        let layout = ChunkLayout::new(ten_gb, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(layout.count(), 2385);
        let (offset, len) = layout.range(layout.count() - 1).unwrap();
        assert_eq!(offset + len as u64, ten_gb);
    }

    #[test]
    fn rejects_chunk_sizes_outside_the_accepted_band() {
        assert!(ChunkLayout::new(1024, 1024).is_err());
        assert!(ChunkLayout::new(1024, MAX_CHUNK_SIZE * 2).is_err());
        assert!(
            ChunkLayout::new(1024, 3 * 1024 * 1024).is_err(),
            "must be a power of two"
        );
    }

    #[test]
    fn index_of_maps_offsets_to_chunks() {
        let layout = ChunkLayout::new(10 * 1024 * 1024, DEFAULT_CHUNK_SIZE).unwrap();
        assert_eq!(layout.index_of(0).unwrap(), 0);
        assert_eq!(layout.index_of(DEFAULT_CHUNK_SIZE as u64 - 1).unwrap(), 0);
        assert_eq!(layout.index_of(DEFAULT_CHUNK_SIZE as u64).unwrap(), 1);
        assert!(layout.index_of(10 * 1024 * 1024).is_err());
    }

    #[test]
    fn root_is_order_dependent() {
        let a = hash_chunk(b"alpha");
        let b = hash_chunk(b"beta");
        assert_ne!(
            root_from_chunk_hashes(&[a, b]),
            root_from_chunk_hashes(&[b, a])
        );
    }

    #[test]
    fn root_is_domain_separated_from_a_bare_chunk_hash() {
        let a = hash_chunk(b"alpha");
        assert_ne!(root_from_chunk_hashes(&[a]), a);
    }

    #[test]
    fn out_of_order_arrival_produces_the_in_order_root() {
        let hashes: Vec<Hash> = (0..5u8).map(|i| hash_chunk(&[i; 16])).collect();
        let expected = root_from_chunk_hashes(&hashes);

        let mut set = ChunkHashSet::new(5);
        for &i in &[3usize, 0, 4, 1, 2] {
            assert!(
                set.root().is_none(),
                "root must not resolve while incomplete"
            );
            set.insert(i as u64, hashes[i]).unwrap();
        }
        assert_eq!(set.root(), Some(expected));
    }

    #[test]
    fn duplicate_chunk_is_accepted_but_a_conflicting_one_is_not() {
        let mut set = ChunkHashSet::new(2);
        let h = hash_chunk(b"x");
        set.insert(0, h).unwrap();
        set.insert(0, h).unwrap();
        assert!(matches!(
            set.insert(0, hash_chunk(b"y")),
            Err(Error::ChunkHashConflict(0))
        ));
    }
}
