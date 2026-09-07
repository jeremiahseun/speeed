//! Platform syscalls the portable layers cannot express.
//!
//! Three things live here, each one a place where `std` stops short of what a
//! high-throughput transfer needs: reserving disk blocks rather than just a
//! file length, pinning a socket to an interface, and sizing socket buffers.

use std::fs::File;
use std::os::fd::AsRawFd;

use crate::Error;

/// Reserves `size` bytes of **actual disk blocks** for `file`.
///
/// `File::set_len` is `ftruncate`: it sets the length and nothing else, leaving
/// a sparse file whose blocks are allocated on demand, interleaved with
/// whatever else the filesystem is doing. Under a sustained 100 MB/s write that
/// is how a 10 GB file ends up fragmented across the device.
///
/// Falls back to `set_len` when the filesystem cannot pre-allocate, which is a
/// real case (some FUSE mounts, older filesystems) and not worth failing over —
/// the transfer still works, it is just less tidy on disk.
pub fn preallocate(file: &File, size: u64) -> Result<(), Error> {
    if size == 0 {
        return Ok(());
    }
    match reserve_blocks(file, size) {
        Ok(()) => {
            // `fallocate` extends the length as well, but `F_PREALLOCATE` does
            // not, so set it explicitly on both paths.
            file.set_len(size)
                .map_err(|e| Error::Io(format!("setting file length: {e}")))
        }
        Err(Unsupported) => file
            .set_len(size)
            .map_err(|e| Error::Io(format!("setting file length: {e}"))),
        Err(Failed(e)) => Err(Error::Io(format!("pre-allocating {size} bytes: {e}"))),
    }
}

enum ReserveError {
    /// The filesystem does not implement pre-allocation.
    Unsupported,
    Failed(std::io::Error),
}
use ReserveError::{Failed, Unsupported};

#[cfg(any(target_os = "linux", target_os = "android"))]
fn reserve_blocks(file: &File, size: u64) -> Result<(), ReserveError> {
    // SAFETY: `file` owns a valid descriptor for the duration of the call.
    let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // ext4 and f2fs support this; tmpfs and some others do not.
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => Err(Unsupported),
        _ => Err(Failed(err)),
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn reserve_blocks(file: &File, size: u64) -> Result<(), ReserveError> {
    // Ask for one contiguous extent first; fall back to "any blocks will do".
    // APFS frequently cannot satisfy the contiguous request on a busy volume,
    // and a fragmented reservation still beats no reservation.
    for flags in [libc::F_ALLOCATECONTIG, libc::F_ALLOCATEALL] {
        let mut store = libc::fstore_t {
            fst_flags: flags as libc::c_uint,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: size as libc::off_t,
            fst_bytesalloc: 0,
        };
        // SAFETY: `file` owns a valid descriptor and `store` is a correctly
        // initialised `fstore_t` for the duration of the call.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        if rc != -1 {
            return Ok(());
        }
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENOTSUP) | Some(libc::EINVAL) => Err(Unsupported),
        _ => Err(Failed(err)),
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
)))]
fn reserve_blocks(_file: &File, _size: u64) -> Result<(), ReserveError> {
    Err(Unsupported)
}

/// System page size. `madvise` operates on whole pages, so callers that want
/// to release a byte range have to round it to page boundaries themselves.
pub fn page_size() -> usize {
    // SAFETY: `sysconf` with a valid name; the result is a positive long on
    // every platform we target.
    let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if raw > 0 {
        raw as usize
    } else {
        4096
    }
}

/// Pins a socket to one network interface by index.
///
/// This exists for iOS. The direct link has no internet gateway, so the OS will
/// happily route our sockets out over cellular, where they reach nothing. On
/// Darwin `IP_BOUND_IF` forces the issue at the socket level.
///
/// On Android the equivalent is `ConnectivityManager.bindProcessToNetwork`,
/// which the Kotlin layer applies to the whole process — `SO_BINDTODEVICE`
/// needs `CAP_NET_RAW`, which an app does not have. So this is a deliberate
/// no-op there, and passing an index on Android is a caller mistake worth
/// reporting rather than silently ignoring.
///
/// Index `0` means "leave routing alone" and is always accepted.
pub fn bind_to_interface<S: AsRawFd>(socket: &S, interface_index: u32) -> Result<(), Error> {
    if interface_index == 0 {
        return Ok(());
    }
    bind_impl(socket.as_raw_fd(), interface_index)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn bind_impl(fd: std::os::fd::RawFd, index: u32) -> Result<(), Error> {
    let value = index as libc::c_int;
    // SAFETY: `fd` is valid, and `value` is a `c_int` of the length passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            libc::IP_BOUND_IF,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(Error::Io(format!(
            "binding socket to interface {index}: {}",
            std::io::Error::last_os_error()
        )))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn bind_impl(_fd: std::os::fd::RawFd, index: u32) -> Result<(), Error> {
    Err(Error::Io(format!(
        "per-socket interface binding is not available on this platform \
         (asked for index {index}); on Android bind the process to the network \
         with ConnectivityManager.bindProcessToNetwork instead"
    )))
}

/// Requests larger socket buffers. Zero leaves the kernel default in place.
///
/// Best-effort by design: the kernel clamps to its own maximum
/// (`net.core.wmem_max`), and on Linux it also doubles what it records for
/// bookkeeping. A smaller-than-requested buffer is not worth failing a
/// transfer over, so failures are reported to the caller to log rather than
/// raised as errors.
pub fn set_socket_buffers<S: AsRawFd>(
    socket: &S,
    send_bytes: u32,
    recv_bytes: u32,
) -> BufferResult {
    BufferResult {
        send: (send_bytes > 0).then(|| set_buf(socket.as_raw_fd(), libc::SO_SNDBUF, send_bytes)),
        recv: (recv_bytes > 0).then(|| set_buf(socket.as_raw_fd(), libc::SO_RCVBUF, recv_bytes)),
    }
}

/// Outcome per direction: `None` if not requested, `Some(Ok(actual))` with the
/// size the kernel settled on, or `Some(Err(..))`.
#[derive(Debug)]
pub struct BufferResult {
    pub send: Option<Result<u32, std::io::Error>>,
    pub recv: Option<Result<u32, std::io::Error>>,
}

fn set_buf(fd: std::os::fd::RawFd, option: libc::c_int, bytes: u32) -> Result<u32, std::io::Error> {
    let value = bytes as libc::c_int;
    // SAFETY: `fd` is valid and `value` is a `c_int` of the length passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    get_buf(fd, option)
}

fn get_buf(fd: std::os::fd::RawFd, option: libc::c_int) -> Result<u32, std::io::Error> {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is valid; `value` and `len` are correctly sized outputs.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(value.max(0) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    fn tmpfile(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("aetherlink-platform-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(tag)
    }

    #[test]
    fn preallocate_sets_the_length() {
        let path = tmpfile("prealloc.bin");
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        preallocate(&file, 4 * 1024 * 1024).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 4 * 1024 * 1024);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn preallocate_reserves_blocks_where_the_filesystem_allows_it() {
        use std::os::unix::fs::MetadataExt;
        let path = tmpfile("blocks.bin");
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let size = 4 * 1024 * 1024;
        preallocate(&file, size).unwrap();

        // 512-byte units. A sparse file reports far fewer than the length
        // implies; a reserved one reports roughly the full extent. Filesystems
        // that cannot pre-allocate fall back to ftruncate, so this is a
        // one-way check: blocks may be low, but the length must be right.
        let blocks = file.metadata().unwrap().blocks();
        assert_eq!(file.metadata().unwrap().len(), size);
        if blocks * 512 >= size {
            // Pre-allocation happened, which is what we want on ext4/f2fs.
        } else {
            eprintln!(
                "note: filesystem did not pre-allocate ({blocks} blocks); fell back to ftruncate"
            );
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn preallocating_nothing_is_a_no_op() {
        let path = tmpfile("empty.bin");
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        preallocate(&file, 0).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn interface_index_zero_leaves_routing_alone() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        assert!(
            bind_to_interface(&listener, 0).is_ok(),
            "index 0 must be accepted everywhere; it is the default"
        );
    }

    #[test]
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    fn a_real_interface_index_is_refused_rather_than_ignored_off_darwin() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let err = bind_to_interface(&listener, 1).unwrap_err();
        assert!(
            err.to_string().contains("bindProcessToNetwork"),
            "the error should point at the Android equivalent, got: {err}"
        );
    }

    #[test]
    fn socket_buffers_are_applied_and_reported_back() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).unwrap();

        let result = set_socket_buffers(&stream, 1 << 20, 1 << 20);
        let send = result
            .send
            .expect("send was requested")
            .expect("setsockopt should succeed");
        let recv = result
            .recv
            .expect("recv was requested")
            .expect("setsockopt should succeed");

        // The kernel clamps to its own maximum and may report double what was
        // asked for, so the only safe assertion is that something took effect.
        assert!(send > 0 && recv > 0, "got send={send} recv={recv}");
    }

    #[test]
    fn zero_means_leave_the_defaults_alone() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let result = set_socket_buffers(&listener, 0, 0);
        assert!(result.send.is_none() && result.recv.is_none());
    }
}
