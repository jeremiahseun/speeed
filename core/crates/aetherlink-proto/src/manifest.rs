//! Transfer manifest: what the sender is offering, and what the receiver agrees
//! to write.
//!
//! The receiver writes files under paths chosen by the *peer*, so every path is
//! hostile input until proved otherwise. `sanitize_relative_path` is the single
//! gate; nothing in the engine may join a peer-supplied path to a local
//! directory without passing through it.

use crate::chunk::{ChunkLayout, Hash};
use crate::Error;

/// 16 bytes, matching the control-message width. Random per session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, std::hash::Hash)]
pub struct SessionId(pub [u8; 16]);

impl SessionId {
    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Longest single path component we accept. Most filesystems cap at 255 bytes.
const MAX_COMPONENT_LEN: usize = 255;
/// Longest whole relative path we accept.
const MAX_PATH_LEN: usize = 1024;
/// Cap on files per manifest, so a hostile offer cannot exhaust memory before
/// the user has even seen the prompt.
pub const MAX_FILES_PER_MANIFEST: usize = 20_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub file_id: u64,
    /// Sender-supplied, and therefore untrusted. Always route through
    /// [`sanitize_relative_path`] before touching the filesystem.
    pub relative_path: String,
    pub size_bytes: u64,
    pub mime_type: String,
    pub root_hash: Hash,
    pub modified_unix_ms: i64,
    /// Preserved so files sort correctly in Photos and Gallery rather than all
    /// landing at the moment of transfer.
    pub created_unix_ms: i64,
}

impl FileMetadata {
    pub fn layout(&self, chunk_size: u32) -> Result<ChunkLayout, Error> {
        ChunkLayout::new(self.size_bytes, chunk_size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub session_id: SessionId,
    pub chunk_size: u32,
    pub files: Vec<FileMetadata>,
}

impl Manifest {
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size_bytes).sum()
    }

    pub fn file(&self, file_id: u64) -> Option<&FileMetadata> {
        self.files.iter().find(|f| f.file_id == file_id)
    }

    /// Validates a manifest received from a peer. Checks structural limits,
    /// duplicate ids, and every path. Returns the sanitized paths in the same
    /// order as `files`, so the caller need not re-derive them.
    pub fn validate(&self) -> Result<Vec<String>, Error> {
        if self.files.len() > MAX_FILES_PER_MANIFEST {
            return Err(Error::ManifestTooLarge(self.files.len()));
        }
        // Chunk size validity is enforced by ChunkLayout; check it once here so
        // a bad manifest fails before any file is created.
        ChunkLayout::new(0, self.chunk_size)?;

        let mut seen_ids = std::collections::HashSet::with_capacity(self.files.len());
        let mut seen_paths = std::collections::HashSet::with_capacity(self.files.len());
        let mut sanitized = Vec::with_capacity(self.files.len());

        for f in &self.files {
            if !seen_ids.insert(f.file_id) {
                return Err(Error::DuplicateFileId(f.file_id));
            }
            let path = sanitize_relative_path(&f.relative_path)?;
            // Two entries resolving to one path would race on the same fd.
            if !seen_paths.insert(path.clone()) {
                return Err(Error::DuplicatePath(path));
            }
            sanitized.push(path);
        }
        Ok(sanitized)
    }
}

/// Reduces a peer-supplied path to something safe to join to a local directory.
///
/// Rejects, rather than silently rewriting: absolute paths, `..` traversal,
/// Windows drive letters and UNC prefixes, NUL and control bytes, backslash
/// separators (which some filesystems treat as ordinary characters, letting
/// `..\..\x` slip past a naive `/`-only check), and reserved names. Returns the
/// path with redundant separators and `.` components collapsed.
pub fn sanitize_relative_path(raw: &str) -> Result<String, Error> {
    let bad = |why: &str| {
        Err(Error::UnsafePath {
            path: raw.to_string(),
            reason: why.to_string(),
        })
    };

    if raw.is_empty() {
        return bad("empty");
    }
    if raw.len() > MAX_PATH_LEN {
        return bad("exceeds maximum length");
    }
    if raw.contains('\0') {
        return bad("contains a NUL byte");
    }
    if raw.chars().any(|c| c.is_control()) {
        return bad("contains a control character");
    }
    if raw.contains('\\') {
        return bad("contains a backslash separator");
    }
    if raw.starts_with('/') {
        return bad("is absolute");
    }
    // C: or \\server — rejected even on Unix, since the manifest may be
    // replayed onto a Windows receiver later.
    if raw.len() >= 2 && raw.as_bytes()[1] == b':' {
        return bad("contains a drive letter");
    }

    let mut parts = Vec::new();
    for component in raw.split('/') {
        match component {
            // Collapse `a//b` and `a/./b`.
            "" | "." => continue,
            ".." => return bad("contains a parent-directory traversal"),
            c => {
                if c.len() > MAX_COMPONENT_LEN {
                    return bad("has an over-long component");
                }
                // Trailing dots and spaces are stripped by Windows, which would
                // let "foo." and "foo" collide after our duplicate check.
                if c.ends_with('.') || c.ends_with(' ') {
                    return bad("has a component ending in a dot or space");
                }
                if is_reserved_device_name(c) {
                    return bad("uses a reserved device name");
                }
                parts.push(c);
            }
        }
    }
    if parts.is_empty() {
        return bad("resolves to nothing");
    }
    Ok(parts.join("/"))
}

/// Windows reserved device names, compared against the stem and
/// case-insensitively — `con.txt` is as reserved as `CON`.
fn is_reserved_device_name(component: &str) -> bool {
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let stem = component.split('.').next().unwrap_or(component);
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(stem))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64, path: &str) -> FileMetadata {
        FileMetadata {
            file_id: id,
            relative_path: path.to_string(),
            size_bytes: 1024,
            mime_type: "application/octet-stream".into(),
            root_hash: [0u8; 32],
            modified_unix_ms: 0,
            created_unix_ms: 0,
        }
    }

    fn manifest(files: Vec<FileMetadata>) -> Manifest {
        Manifest {
            session_id: SessionId([7u8; 16]),
            chunk_size: 4 * 1024 * 1024,
            files,
        }
    }

    #[test]
    fn accepts_ordinary_paths_and_preserves_structure() {
        assert_eq!(sanitize_relative_path("photo.jpg").unwrap(), "photo.jpg");
        assert_eq!(
            sanitize_relative_path("DCIM/Camera/IMG_0001.HEIC").unwrap(),
            "DCIM/Camera/IMG_0001.HEIC"
        );
        assert_eq!(
            sanitize_relative_path("a b/c d.mp4").unwrap(),
            "a b/c d.mp4"
        );
        assert_eq!(
            sanitize_relative_path("café/naïve.txt").unwrap(),
            "café/naïve.txt"
        );
    }

    #[test]
    fn collapses_redundant_separators_and_dot_components() {
        assert_eq!(sanitize_relative_path("a//b/./c.txt").unwrap(), "a/b/c.txt");
        assert_eq!(sanitize_relative_path("./x.txt").unwrap(), "x.txt");
        assert_eq!(sanitize_relative_path("a/b/").unwrap(), "a/b");
    }

    #[test]
    fn rejects_traversal_in_every_position() {
        for path in [
            "../etc/passwd",
            "a/../../b",
            "a/..",
            "..",
            "../../../../etc/shadow",
        ] {
            assert!(sanitize_relative_path(path).is_err(), "must reject {path}");
        }
    }

    #[test]
    fn rejects_absolute_and_windows_style_paths() {
        for path in [
            "/etc/passwd",
            "C:/Windows/system32",
            "c:x",
            "a\\..\\..\\b",
            "\\\\server\\share",
        ] {
            assert!(sanitize_relative_path(path).is_err(), "must reject {path}");
        }
    }

    #[test]
    fn rejects_control_bytes_and_nul() {
        assert!(sanitize_relative_path("a\0b").is_err());
        assert!(sanitize_relative_path("a\nb").is_err());
        assert!(sanitize_relative_path("a\tb").is_err());
    }

    #[test]
    fn rejects_names_that_collide_after_windows_normalization() {
        assert!(sanitize_relative_path("report.").is_err());
        assert!(sanitize_relative_path("report ").is_err());
        assert!(sanitize_relative_path("CON").is_err());
        assert!(sanitize_relative_path("con.txt").is_err());
        assert!(sanitize_relative_path("dir/NUL.dat").is_err());
    }

    #[test]
    fn rejects_empty_and_over_long_paths() {
        assert!(sanitize_relative_path("").is_err());
        assert!(sanitize_relative_path("///").is_err());
        assert!(sanitize_relative_path(&"a".repeat(MAX_PATH_LEN + 1)).is_err());
        assert!(
            sanitize_relative_path(&format!("dir/{}", "a".repeat(MAX_COMPONENT_LEN + 1))).is_err()
        );
    }

    #[test]
    fn validate_accepts_a_well_formed_manifest() {
        let m = manifest(vec![meta(1, "a/./b.txt"), meta(2, "c.txt")]);
        assert_eq!(m.validate().unwrap(), vec!["a/b.txt", "c.txt"]);
        assert_eq!(m.total_bytes(), 2048);
    }

    #[test]
    fn validate_rejects_duplicate_file_ids() {
        let m = manifest(vec![meta(1, "a.txt"), meta(1, "b.txt")]);
        assert!(matches!(m.validate(), Err(Error::DuplicateFileId(1))));
    }

    #[test]
    fn validate_rejects_paths_that_collide_after_sanitization() {
        // "a/b.txt" and "a/./b.txt" are distinct strings but the same file.
        let m = manifest(vec![meta(1, "a/b.txt"), meta(2, "a/./b.txt")]);
        assert!(matches!(m.validate(), Err(Error::DuplicatePath(_))));
    }

    #[test]
    fn validate_rejects_a_bad_chunk_size_before_creating_anything() {
        let mut m = manifest(vec![meta(1, "a.txt")]);
        m.chunk_size = 3;
        assert!(m.validate().is_err());
    }

    #[test]
    fn session_id_hex_is_stable() {
        assert_eq!(SessionId([0xAB; 16]).to_hex(), "ab".repeat(16));
    }
}
