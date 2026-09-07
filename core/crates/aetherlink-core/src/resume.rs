//! Resume across reconnect (PRD §8).
//!
//! Two ways to learn what a staging file already holds:
//!
//! * the `.aether_state` sidecar, written as the transfer runs — cheap;
//! * re-verifying every chunk against the manifest — costs a full read and
//!   hash, but needs no prior state and cannot be fooled.
//!
//! The sidecar is used when it validates against the manifest, and the
//! verification path is the fallback. Either way the invariant holds: **a bit
//! is set only for a chunk whose hash matched and whose bytes are on disk.**
//! Setting a bit on write completion alone lets a power loss leave a chunk
//! marked present that resume then never repairs — a silently corrupt file.

use std::path::{Path, PathBuf};

use aetherlink_proto::chunk::{self, ChunkLayout, Hash};
use aetherlink_proto::ChunkBitmap;

use crate::io::SinkFile;
use crate::Error;

/// `AETHRSM` + format version.
const MAGIC: &[u8; 8] = b"AETHRSM1";

/// Bytes written between checkpoints. Each one costs an fsync, so this trades
/// resume granularity against write throughput: at 64 MB a worst-case
/// interruption re-sends 64 MB.
pub const DEFAULT_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;

/// Header is magic + chunk_size + chunk_count + file_size + root_hash.
const HEADER_LEN: usize = 8 + 4 + 8 + 8 + 32;

/// Sidecar path for a staging file. Kept beside it so both are removed
/// together when a transfer is abandoned.
pub fn state_path(staging: &Path) -> PathBuf {
    let mut name = staging.file_name().unwrap_or_default().to_os_string();
    name.push(".aether_state");
    staging.with_file_name(name)
}

/// Identity of the file a state record describes. A sidecar only applies if
/// every field matches the manifest — otherwise it belongs to a different
/// transfer that happened to use the same path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateKey {
    pub root_hash: Hash,
    pub file_size: u64,
    pub chunk_size: u32,
}

/// Serializes the bitmap with its identity and a checksum.
///
/// Written to a temporary file and renamed, so a crash mid-write leaves either
/// the previous record or none — never a torn one.
pub fn persist(staging: &Path, key: &StateKey, bitmap: &ChunkBitmap) -> Result<(), Error> {
    let mut buf = Vec::with_capacity(HEADER_LEN + bitmap.as_bytes().len() + 32);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&key.chunk_size.to_le_bytes());
    buf.extend_from_slice(&bitmap.count().to_le_bytes());
    buf.extend_from_slice(&key.file_size.to_le_bytes());
    buf.extend_from_slice(&key.root_hash);
    buf.extend_from_slice(bitmap.as_bytes());
    let checksum = blake3::hash(&buf);
    buf.extend_from_slice(checksum.as_bytes());

    let final_path = state_path(staging);
    let tmp_path = final_path.with_extension("tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| Error::Io(format!("creating {}: {e}", tmp_path.display())))?;
        f.write_all(&buf)
            .map_err(|e| Error::Io(format!("writing {}: {e}", tmp_path.display())))?;
        // The rename is only atomic with respect to content that has actually
        // reached the device.
        f.sync_all()
            .map_err(|e| Error::Io(format!("syncing {}: {e}", tmp_path.display())))?;
    }
    std::fs::rename(&tmp_path, &final_path)
        .map_err(|e| Error::Io(format!("renaming into {}: {e}", final_path.display())))?;
    Ok(())
}

/// Reads a sidecar, returning its bitmap only if it is intact and describes
/// exactly this file. Every failure is a `None`, not an error: a missing,
/// stale, or damaged record simply means "start from nothing", which is always
/// safe.
pub fn load(staging: &Path, key: &StateKey) -> Option<ChunkBitmap> {
    let raw = std::fs::read(state_path(staging)).ok()?;
    if raw.len() < HEADER_LEN + 32 {
        return None;
    }
    let (body, stored_checksum) = raw.split_at(raw.len() - 32);
    if blake3::hash(body) != blake3::Hash::from_slice(stored_checksum).ok()? {
        return None;
    }
    if &body[0..8] != MAGIC {
        return None;
    }

    let chunk_size = u32::from_le_bytes(body[8..12].try_into().ok()?);
    let chunk_count = u64::from_le_bytes(body[12..20].try_into().ok()?);
    let file_size = u64::from_le_bytes(body[20..28].try_into().ok()?);
    let mut root_hash = [0u8; 32];
    root_hash.copy_from_slice(&body[28..60]);

    // Belongs to a different transfer.
    if chunk_size != key.chunk_size || file_size != key.file_size || root_hash != key.root_hash {
        return None;
    }
    // The staging file must still be the size we reserved; if something
    // truncated it, the recorded offsets mean nothing.
    if std::fs::metadata(staging).ok()?.len() != file_size {
        return None;
    }

    ChunkBitmap::from_bytes(&body[HEADER_LEN..], chunk_count).ok()
}

/// Rebuilds a bitmap by hashing what is actually on disk. Slower than the
/// sidecar — a full read of the staging file — but it depends on no prior
/// state and cannot be misled by one.
pub fn rebuild_by_verification(
    sink: &SinkFile,
    chunk_hashes: &[Hash],
) -> Result<ChunkBitmap, Error> {
    let layout: &ChunkLayout = sink.layout();
    let mut bitmap = ChunkBitmap::new(layout.count());
    for index in 0..layout.count() {
        let bytes = match sink.read_chunk(index) {
            Ok(b) => b,
            // A short or unreadable region just means "not present yet".
            Err(_) => continue,
        };
        if chunk::hash_chunk(&bytes) == chunk_hashes[index as usize] {
            bitmap.set(index)?;
        }
    }
    Ok(bitmap)
}

/// Removes the sidecar. Called once a file is complete, so a later transfer to
/// the same path does not find a record for a file that is already done.
pub fn discard(staging: &Path) {
    std::fs::remove_file(state_path(staging)).ok();
    std::fs::remove_file(state_path(staging).with_extension("tmp")).ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetherlink_proto::chunk::MIN_CHUNK_SIZE;

    struct Fixture {
        dir: PathBuf,
        staging: PathBuf,
        key: StateKey,
    }

    impl Fixture {
        fn new(tag: &str, chunks: u64) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "aetherlink-resume-{tag}-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let staging = dir.join("file.bin");
            let file_size = chunks * MIN_CHUNK_SIZE as u64;
            std::fs::write(&staging, vec![0u8; file_size as usize]).unwrap();
            Self {
                dir,
                staging,
                key: StateKey {
                    root_hash: [9u8; 32],
                    file_size,
                    chunk_size: MIN_CHUNK_SIZE,
                },
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn bitmap_with(count: u64, set: &[u64]) -> ChunkBitmap {
        let mut b = ChunkBitmap::new(count);
        for &i in set {
            b.set(i).unwrap();
        }
        b
    }

    #[test]
    fn a_persisted_bitmap_loads_back_identically() {
        let f = Fixture::new("roundtrip", 20);
        let bitmap = bitmap_with(20, &[0, 1, 5, 19]);
        persist(&f.staging, &f.key, &bitmap).unwrap();

        let loaded = load(&f.staging, &f.key).expect("should load");
        assert_eq!(loaded, bitmap);
        assert_eq!(loaded.set_count(), 4);
    }

    #[test]
    fn no_sidecar_means_start_from_nothing() {
        let f = Fixture::new("absent", 10);
        assert!(load(&f.staging, &f.key).is_none());
    }

    #[test]
    fn a_record_for_a_different_file_is_ignored() {
        let f = Fixture::new("mismatch", 10);
        persist(&f.staging, &f.key, &bitmap_with(10, &[0, 1, 2])).unwrap();

        // Same path, different content: the manifest root differs.
        let other = StateKey {
            root_hash: [1u8; 32],
            ..f.key
        };
        assert!(
            load(&f.staging, &other).is_none(),
            "a different file must not resume"
        );

        // Different chunking of the same bytes.
        let rechunked = StateKey {
            chunk_size: MIN_CHUNK_SIZE * 2,
            ..f.key
        };
        assert!(load(&f.staging, &rechunked).is_none());

        // Different length.
        let resized = StateKey {
            file_size: f.key.file_size + 1,
            ..f.key
        };
        assert!(load(&f.staging, &resized).is_none());
    }

    #[test]
    fn a_corrupted_record_is_ignored_rather_than_trusted() {
        let f = Fixture::new("corrupt", 16);
        persist(&f.staging, &f.key, &bitmap_with(16, &[0, 1, 2, 3])).unwrap();

        let path = state_path(&f.staging);
        let mut raw = std::fs::read(&path).unwrap();
        // Flip a bitmap bit without updating the checksum — exactly what a
        // torn write or bit rot would look like.
        let n = raw.len();
        raw[n - 34] ^= 0x01;
        std::fs::write(&path, &raw).unwrap();

        assert!(
            load(&f.staging, &f.key).is_none(),
            "a damaged record must never be trusted; re-transfer is cheap, corruption is not"
        );
    }

    #[test]
    fn a_truncated_staging_file_invalidates_the_record() {
        let f = Fixture::new("truncated", 10);
        persist(&f.staging, &f.key, &bitmap_with(10, &[0, 1])).unwrap();

        // Something shrank the file; recorded offsets no longer mean anything.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&f.staging)
            .unwrap();
        file.set_len(f.key.file_size / 2).unwrap();
        drop(file);

        assert!(load(&f.staging, &f.key).is_none());
    }

    #[test]
    fn persisting_twice_leaves_no_temporary_behind() {
        let f = Fixture::new("atomic", 8);
        persist(&f.staging, &f.key, &bitmap_with(8, &[0])).unwrap();
        persist(&f.staging, &f.key, &bitmap_with(8, &[0, 1, 2])).unwrap();

        assert_eq!(load(&f.staging, &f.key).unwrap().set_count(), 3);
        assert!(!state_path(&f.staging).with_extension("tmp").exists());
    }

    #[test]
    fn discard_removes_the_record() {
        let f = Fixture::new("discard", 8);
        persist(&f.staging, &f.key, &bitmap_with(8, &[0])).unwrap();
        assert!(state_path(&f.staging).exists());

        discard(&f.staging);
        assert!(!state_path(&f.staging).exists());
        assert!(load(&f.staging, &f.key).is_none());
    }

    #[test]
    fn verification_finds_exactly_the_chunks_that_are_really_there() {
        let f = Fixture::new("verify", 4);
        let cs = MIN_CHUNK_SIZE as usize;

        // Chunks 0 and 2 hold real content; 1 and 3 are still the zeroed
        // pre-allocation.
        let good_a = vec![0xAAu8; cs];
        let good_c = vec![0xCCu8; cs];
        let mut on_disk = vec![0u8; cs * 4];
        on_disk[0..cs].copy_from_slice(&good_a);
        on_disk[cs * 2..cs * 3].copy_from_slice(&good_c);
        std::fs::write(&f.staging, &on_disk).unwrap();

        let expected = vec![
            chunk::hash_chunk(&good_a),
            chunk::hash_chunk(&vec![0xBBu8; cs]),
            chunk::hash_chunk(&good_c),
            chunk::hash_chunk(&vec![0xDDu8; cs]),
        ];

        let sink = SinkFile::create(&f.staging, f.key.file_size, MIN_CHUNK_SIZE).unwrap();
        let bitmap = rebuild_by_verification(&sink, &expected).unwrap();

        assert!(bitmap.get(0).unwrap());
        assert!(!bitmap.get(1).unwrap());
        assert!(bitmap.get(2).unwrap());
        assert!(!bitmap.get(3).unwrap());
        assert_eq!(bitmap.missing_ranges(), vec![(1, 2), (3, 4)]);
    }
}
