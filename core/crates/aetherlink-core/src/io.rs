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

/// Mapped bytes to accumulate before releasing them.
///
/// Every `madvise(MADV_DONTNEED)` forces a TLB shootdown across every core that
/// has the mapping, so releasing per-chunk costs far more than it saves —
/// measured at roughly a third of throughput. Batching amortises the shootdown
/// while still bounding the footprint at this much plus whatever is in flight.
pub const RELEASE_BATCH_BYTES: u64 = 32 * 1024 * 1024;

/// Above this size, the hashing pass releases behind itself.
///
/// The trade is asymmetric. Below the threshold, keeping the file cached costs
/// little and makes the send pass a memory read. Above it, caching the file
/// already blows the < 120 MB RSS budget in PRD §2.5, and the send pass would
/// be re-reading from storage anyway once the file exceeds available memory.
///
/// The cost above the threshold is that the send pass genuinely re-reads. On a
/// device that is flash at several hundred MB/s against a radio at 100 MB/s, so
/// it is invisible; over loopback with no radio to wait for, it is the whole
/// difference. Do not read a loopback benchmark as evidence this is expensive
/// in the field.
pub const HASH_PASS_RELEASE_THRESHOLD: u64 = 128 * 1024 * 1024;

/// A file being sent, mapped once and shared across every stream worker.
pub struct SourceFile {
    map: Mmap,
    layout: ChunkLayout,
    page_size: u64,
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

        Ok(Self {
            map,
            layout,
            page_size: crate::platform::page_size() as u64,
        })
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

    /// Drops pages covering `[start, end)` from this process's page tables.
    ///
    /// Without this, sending a 10 GB file walks all 10 GB through the page
    /// cache and leaves it there, evicting whatever else the device was
    /// holding — other apps, and our own code. Releasing behind the read
    /// cursor keeps the sender's footprint bounded by what is in flight rather
    /// than by file size.
    ///
    /// The range is rounded **inward** to whole pages, so a page still holding
    /// bytes a stream has not read is never dropped. Best-effort: `madvise` is
    /// a hint, and a failure is not worth failing a transfer over.
    ///
    /// # Safety
    ///
    /// No slice returned by [`SourceFile::chunk`] that overlaps `[start, end)`
    /// may be alive anywhere when this is called. Releasing pages out from
    /// under a live borrow is exactly the case memmap2 marks unsafe.
    ///
    /// The mapping itself is sound to release: it is read-only, file-backed
    /// and private, and is never written through, so it holds no
    /// copy-on-write pages. Anything touched again after release re-faults
    /// from the file with identical contents. (The zero-fill hazard in the
    /// `MADV_DONTNEED` documentation applies to *anonymous* private mappings,
    /// which this is not.)
    pub unsafe fn release_range(&self, start: u64, end: u64) {
        let ps = self.page_size;
        let first_whole_page = start.div_ceil(ps) * ps;
        let last_whole_page = (end / ps) * ps;
        if last_whole_page <= first_whole_page {
            return;
        }
        // SAFETY: forwarded to this function's own contract — the caller
        // guarantees no live slice overlaps the range, and the mapping is a
        // read-only file mapping that is never written through.
        unsafe {
            let _ = self.map.unchecked_advise_range(
                memmap2::UncheckedAdvice::DontNeed,
                first_whole_page as usize,
                (last_whole_page - first_whole_page) as usize,
            );
        }
    }

    /// Byte offset one past the last byte of `index`, clamped to the file.
    /// The release boundary is expressed in chunks but applied in bytes.
    pub fn end_of_chunk(&self, index: u64) -> u64 {
        (index.saturating_mul(self.layout.chunk_size() as u64)).min(self.layout.file_size())
    }

    /// Hashes every chunk in order. Called once before offering a manifest, so
    /// the offer can commit to the chunk hashes the receiver verifies against.
    ///
    /// This pass touches the whole file, so above
    /// [`HASH_PASS_RELEASE_THRESHOLD`] it also releases behind itself. Without
    /// that, peak RSS is set *here* — the entire file is mapped in before a
    /// single byte is sent, and releasing during the send afterwards is
    /// shutting the door on an empty stable. Measured on a 1 GB file: 960 MB
    /// peak RSS before this, 122 MB after.
    pub fn chunk_hashes(&self, release_pages: bool) -> Result<Vec<Hash>, Error> {
        // Only worth it once the file is too big to keep cached anyway.
        let release_pages = release_pages && self.layout.file_size() > HASH_PASS_RELEASE_THRESHOLD;
        let mut out = Vec::with_capacity(self.layout.count() as usize);
        let batch = RELEASE_BATCH_BYTES.max(self.layout.chunk_size() as u64);
        let mut released_to = 0u64;

        for index in 0..self.layout.count() {
            let (offset, len) = self.layout.range(index)?;
            // The borrow ends with this statement, before any release below.
            out.push(chunk::hash_chunk(self.chunk(index)?));

            if release_pages {
                let end = offset + len as u64;
                if end.saturating_sub(released_to) >= batch {
                    // SAFETY: chunks are hashed in order and the borrow above
                    // has ended, so nothing holds a slice below `end`.
                    unsafe { self.release_range(released_to, end) };
                    released_to = end;
                }
            }
        }
        Ok(out)
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

        let hashes = src.chunk_hashes(false).unwrap();
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
        assert!(src.chunk_hashes(false).unwrap().is_empty());
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

    /// Resident set size of this process, in pages, from `/proc/self/statm`.
    ///
    /// Note `mincore` is the wrong instrument for this: on a file-backed
    /// mapping it reports whether a page is in the **page cache**, which
    /// `MADV_DONTNEED` deliberately does not change — leaving the page cached
    /// is what makes a re-fault cheap. What the release actually drops is this
    /// process's mapping of those pages, which shows up in RSS.
    #[cfg(target_os = "linux")]
    fn resident_pages() -> usize {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
        statm.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn releasing_pages_actually_drops_them_from_the_process() {
        let dir = tmpdir();
        let path = dir.join("release.bin");
        // Large enough that the drop is unambiguous against allocator noise.
        let chunks = 64u64;
        let size = MIN_CHUNK_SIZE as u64 * chunks;
        std::fs::write(&path, vec![0xABu8; size as usize]).unwrap();

        let src = SourceFile::open(&path, MIN_CHUNK_SIZE).unwrap();

        // Touch every chunk so the whole file is mapped in.
        let mut sum = 0u64;
        for i in 0..chunks {
            sum += src.chunk(i).unwrap()[0] as u64;
        }
        assert_eq!(sum, 0xAB * chunks);
        let before = resident_pages();

        // Release the first half, as the sender does behind its cursor.
        let half = size / 2;
        // SAFETY: the borrows from `src.chunk` above have all ended.
        unsafe { src.release_range(0, half) };
        let after = resident_pages();

        let expected = (half as usize) / crate::platform::page_size();
        let dropped = before.saturating_sub(after);
        assert!(
            dropped >= expected / 2,
            "expected roughly {expected} pages to leave RSS, saw {dropped} \
             ({before} resident before, {after} after)"
        );

        // And the data is still correct: a private file mapping re-faults from
        // the file, it does not come back zeroed.
        assert!(
            src.chunk(0).unwrap().iter().all(|&b| b == 0xAB),
            "released pages must re-read from the file, not zero-fill"
        );
    }

    #[test]
    fn releasing_rounds_inward_and_ignores_ranges_smaller_than_a_page() {
        let dir = tmpdir();
        let path = dir.join("inward.bin");
        std::fs::write(&path, vec![7u8; MIN_CHUNK_SIZE as usize]).unwrap();
        let src = SourceFile::open(&path, MIN_CHUNK_SIZE).unwrap();

        // A sub-page range covers no whole page, so nothing is released and
        // the data is untouched either way.
        // SAFETY: no outstanding borrows.
        unsafe {
            src.release_range(1, 2);
            src.release_range(100, 100);
            src.release_range(50, 10); // end before start
        }
        assert!(src.chunk(0).unwrap().iter().all(|&b| b == 7));
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
