//! The single-copy I/O pipeline (PRD §5.5).
//!
//! Not zero-copy, and the name matters: `sendfile`/`splice` cannot carry TLS
//! without kernel TLS, which Android ships disabled and Darwin lacks entirely.
//! Encrypted bulk transfer must traverse userspace, so the honest goal is *one*
//! copy — out of the mapped source region, into the TLS record — with no
//! intermediate `Vec` allocation per frame.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aetherlink_proto::chunk::{self, ChunkLayout, Hash};
use memmap2::{Mmap, MmapOptions};

use crate::Error;

/// A file being sent, mapped once and shared across every stream worker.
pub struct SourceFile {
    map: Mmap,
    layout: ChunkLayout,
}

impl SourceFile {
    pub fn open(path: &Path, chunk_size: u32) -> Result<Self, Error> {
        let file =
            File::open(path).map_err(|e| Error::Io(format!("opening {}: {e}", path.display())))?;
        let size = file
            .metadata()
            .map_err(|e| Error::Io(format!("stat {}: {e}", path.display())))?
            .len();
        let layout = ChunkLayout::new(size, chunk_size)?;

        // SAFETY: mapping a file we hold open. The usual mmap caveat applies —
        // external truncation would fault — which is why the receiver writes to
        // a staging path nothing else touches, and the sender maps read-only.
        let map = unsafe { MmapOptions::new().len(size as usize).map(&file) }
            .map_err(|e| Error::Io(format!("mapping {}: {e}", path.display())))?;

        // Tell the kernel how we intend to read, so it prefetches ahead of the
        // stream workers rather than faulting on every page.
        let _ = map.advise(memmap2::Advice::Sequential);
        let _ = map.advise(memmap2::Advice::WillNeed);

        Ok(Self { map, layout })
    }

    pub fn layout(&self) -> &ChunkLayout {
        &self.layout
    }

    pub fn size(&self) -> u64 {
        self.layout.file_size()
    }

    /// Borrows a chunk's bytes directly out of the page cache. No copy happens
    /// here; the one copy occurs when this slice is encrypted into a TLS record.
    pub fn chunk(&self, index: u64) -> Result<&[u8], Error> {
        let (offset, len) = self.layout.range(index)?;
        Ok(&self.map[offset as usize..offset as usize + len as usize])
    }

    /// Hashes every chunk in order. Called once before offering a manifest, so
    /// the offer can commit to the chunk hashes the receiver verifies against.
    pub fn chunk_hashes(&self) -> Result<Vec<Hash>, Error> {
        (0..self.layout.count())
            .map(|i| Ok(chunk::hash_chunk(self.chunk(i)?)))
            .collect()
    }
}

/// A file being received: pre-allocated, then written at absolute offsets.
///
/// `pwrite` rather than `mmap` on this side, reversing the v1.0 design. Large
/// sequential mmap writes stall unpredictably on dirty-page writeback; a
/// positional write with a deep queue is as fast and far more predictable.
pub struct SinkFile {
    file: Arc<File>,
    path: PathBuf,
    layout: ChunkLayout,
}

impl SinkFile {
    /// Creates (or reopens, for resume) the staging file and reserves its space.
    pub fn create(path: &Path, size: u64, chunk_size: u32) -> Result<Self, Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(format!("creating {}: {e}", parent.display())))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|e| Error::Io(format!("creating {}: {e}", path.display())))?;

        // Reserve real disk blocks, not just a length, so the filesystem does
        // not fragment the file under sustained write load. Falls back to
        // ftruncate where the filesystem cannot pre-allocate.
        crate::platform::preallocate(&file, size)?;

        Ok(Self {
            file: Arc::new(file),
            path: path.to_path_buf(),
            layout: ChunkLayout::new(size, chunk_size)?,
        })
    }

    pub fn layout(&self) -> &ChunkLayout {
        &self.layout
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Clonable handle for use from a blocking worker.
    pub fn handle(&self) -> Arc<File> {
        self.file.clone()
    }

    pub fn sync(&self) -> Result<(), Error> {
        self.file
            .sync_all()
            .map_err(|e| Error::Io(format!("syncing {}: {e}", self.path.display())))
    }

    /// Reads a chunk back for verification. Used on resume to rebuild the
    /// bitmap from a staging file left behind by an interrupted session.
    pub fn read_chunk(&self, index: u64) -> Result<Vec<u8>, Error> {
        let (offset, len) = self.layout.range(index)?;
        let mut buf = vec![0u8; len as usize];
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(|e| Error::Io(format!("reading chunk {index}: {e}")))?;
        Ok(buf)
    }
}

/// Verifies a chunk against its committed hash and writes it at the right
/// offset. Runs on a blocking worker: BLAKE3 over 4 MB is ~2 ms of CPU, which
/// has no business on an async reactor thread.
///
/// Verification happens **before** the write, so a corrupt chunk never reaches
/// the file at all.
pub fn verify_and_write(
    file: &File,
    offset: u64,
    bytes: &[u8],
    expected: &Hash,
) -> Result<(), Error> {
    let actual = chunk::hash_chunk(bytes);
    if actual != *expected {
        return Err(Error::ChunkVerificationFailed { offset });
    }
    file.write_all_at(bytes, offset)
        .map_err(|e| Error::Io(format!("writing at offset {offset}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetherlink_proto::chunk::MIN_CHUNK_SIZE;

    fn tmpdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aetherlink-io-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn source_chunks_tile_the_file_and_hash_to_the_root() {
        let dir = tmpdir();
        let path = dir.join("src.bin");
        let data: Vec<u8> = (0..MIN_CHUNK_SIZE as usize * 2 + 1234)
            .map(|i| i as u8)
            .collect();
        std::fs::write(&path, &data).unwrap();

        let src = SourceFile::open(&path, MIN_CHUNK_SIZE).unwrap();
        assert_eq!(src.size(), data.len() as u64);
        assert_eq!(src.layout().count(), 3);

        let mut reassembled = Vec::new();
        for i in 0..src.layout().count() {
            reassembled.extend_from_slice(src.chunk(i).unwrap());
        }
        assert_eq!(
            reassembled, data,
            "chunks must reconstruct the file exactly"
        );

        let hashes = src.chunk_hashes().unwrap();
        assert_eq!(hashes.len(), 3);
        assert_eq!(
            hashes[0],
            chunk::hash_chunk(&data[..MIN_CHUNK_SIZE as usize])
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_source_has_no_chunks() {
        let dir = tmpdir();
        let path = dir.join("empty.bin");
        std::fs::write(&path, b"").unwrap();
        let src = SourceFile::open(&path, MIN_CHUNK_SIZE).unwrap();
        assert_eq!(src.layout().count(), 0);
        assert!(src.chunk_hashes().unwrap().is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn sink_preallocates_and_accepts_out_of_order_writes() {
        let dir = tmpdir();
        let path = dir.join("sink.bin");
        let size = MIN_CHUNK_SIZE as u64 * 2;
        let sink = SinkFile::create(&path, size, MIN_CHUNK_SIZE).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            size,
            "space reserved up front"
        );

        let second = vec![0xBBu8; MIN_CHUNK_SIZE as usize];
        let first = vec![0xAAu8; MIN_CHUNK_SIZE as usize];
        let file = sink.handle();

        // Deliberately out of order, as parallel streams would deliver them.
        verify_and_write(
            &file,
            MIN_CHUNK_SIZE as u64,
            &second,
            &chunk::hash_chunk(&second),
        )
        .unwrap();
        verify_and_write(&file, 0, &first, &chunk::hash_chunk(&first)).unwrap();
        sink.sync().unwrap();

        let written = std::fs::read(&path).unwrap();
        assert_eq!(&written[..MIN_CHUNK_SIZE as usize], &first[..]);
        assert_eq!(&written[MIN_CHUNK_SIZE as usize..], &second[..]);
        assert_eq!(sink.read_chunk(1).unwrap(), second);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_corrupt_chunk_is_rejected_before_it_reaches_the_file() {
        let dir = tmpdir();
        let path = dir.join("guard.bin");
        let sink = SinkFile::create(&path, MIN_CHUNK_SIZE as u64, MIN_CHUNK_SIZE).unwrap();
        let file = sink.handle();

        let good = vec![0x11u8; MIN_CHUNK_SIZE as usize];
        let expected = chunk::hash_chunk(&good);
        let tampered = vec![0x22u8; MIN_CHUNK_SIZE as usize];

        assert!(matches!(
            verify_and_write(&file, 0, &tampered, &expected),
            Err(Error::ChunkVerificationFailed { offset: 0 })
        ));
        assert!(
            std::fs::read(&path).unwrap().iter().all(|&b| b == 0),
            "nothing may be written when verification fails"
        );

        std::fs::remove_file(&path).ok();
    }
}
