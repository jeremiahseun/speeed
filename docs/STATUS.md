# AetherLink — build status and handoff

**Updated:** end of the cloud-side Sprint 1 work.
**Branch:** `claude/serene-dijkstra-xbsnix`
**Spec:** [`PRD-v1.1.md`](./PRD-v1.1.md) · **Benchmarks:** [`benchmarks.md`](./benchmarks.md)

This file is the single source of truth for what exists and what comes next.
Update it at the end of each work session.

---

## Where the work happens

| Phase | Machine | Why |
|---|---|---|
| Rust core, protocol, engine, FFI | **cloud** (done) | No SDKs needed; loopback benchmarks are meaningful |
| Android app, iOS app, on-device builds | **local** | Needs Android Studio + NDK, and Xcode |
| Sprint 0 hardware spike | **local + phones** | Needs real radios |

**We are at the handoff point.** Everything below the line marked *cloud-side
complete* is done and verified. The next step needs a local machine.

---

## Done

### `core/crates/aetherlink-proto` — wire protocol · 44 tests
Pure logic, no I/O, no async runtime.

- `frame` — 32-byte header, strict decode. Rejects bad magic, future versions,
  unknown flag bits, non-zero reserved fields, and an oversized length *before*
  allocating.
- `chunk` — chunk geometry and BLAKE3. `ChunkHashSet` accepts out-of-order
  arrivals and proves the root matches in-order hashing.
- `bitmap` — resume bitmap, LSB-first. 10 GB → 2385 chunks → 299 bytes.
- `manifest` — `sanitize_relative_path`, the single gate on peer-supplied paths.

### `core/crates/aetherlink-core` — engine · 17 unit + 7 loopback tests
- `tls` — TLS 1.3 only, server certificate pinned to the QR fingerprint.
  Session tickets disabled (see *Traps*, below).
- `control` — protobuf on stream 0 via prost derives, so the build needs no
  `protoc`. The offer commits to every chunk hash up front.
- `io` — mmap source with sequential/willneed advice; `pwrite` sink; verify
  before write.
- `send` / `recv` — one whole chunk per stream, so no cross-stream reassembly.
- `progress` — `ProgressSink` plus `Throttled`, rate-limiting UI updates to
  ~10 Hz while never dropping the first or last.

### `core/crates/aetherlink-cli` — harness
`aetherlink recv | send | bench`. The `bench` subcommand settles PRD §2.4.

### `core/crates/aetherlink-ffi` — UniFFI surface · 6 tests
`EngineSession` with `start_host`, `start_client`, `cancel`, `state`,
`resolved_stream_count`; `TransferObserver` callback trait; records for config,
files, and the host handle. Kotlin and Swift bindings generate cleanly, and the
tests drive a real transfer through the surface — generating is not the same as
working.

Two UniFFI requirements PRD v1.0 got wrong, now correct here: exported methods
take `&self` (state lives behind interior mutability), and foreign traits arrive
as `Arc<dyn Trait>`, not `Box`.

### Verified results
- **74 tests green**, clippy clean at `-D warnings`, CI on every push.
- **1.24 GB/s peak** over loopback with full TLS + BLAKE3 — meets the 1.2 GB/s
  software-ceiling gate. Caveats in `benchmarks.md`.

---

## ▸ Cloud-side complete — next steps need a local machine

## To do

### 1. Sprint 0 — hardware spike ⟵ *do this first, it gates everything*
Nothing below is worth building at scale until this number exists.

- [ ] `WifiP2pManager.createGroup` with `GROUP_OWNER_BAND_5GHZ` on ≥3 Android
      devices from different OEMs
- [ ] **Log `WifiP2pGroup.getFrequency()` every time** — how often a 5 GHz
      request silently lands on 2.4 GHz decides whether §4.5's fall-up path is
      a corner case or the common one
- [ ] Join from ≥2 iPhones via `NEHotspotConfiguration`
- [ ] `iperf3` both directions; record the tier reached per pair
- [ ] Run LocalSend on the same pairs for a like-for-like baseline

**Decision point:** if real-world Tier B lands near 25 MB/s rather than 55, the
targets and possibly the transport choice change. Do not skip this.

### 2. Wire the FFI into both platforms
- [ ] `./scripts/build-android.sh` — needs `cargo-ndk` and `ANDROID_NDK_HOME`
- [ ] `./scripts/build-ios.sh` — macOS only
- [ ] Confirm `EngineSession` round-trips from Kotlin and from Swift against
      the CLI on the same LAN (`aetherlink recv` on a laptop, app as client)

### 3. Android app
- [ ] Wi-Fi Direct group owner + LOHS fallback + **band-aware selection (§4.5)**
- [ ] QR generation and CameraX scanning
- [ ] Foreground service (`dataSync`) + wake lock + `WIFI_MODE_FULL_HIGH_PERF`
- [ ] `ConnectivityManager.bindProcessToNetwork` — without it Android routes
      our sockets to mobile data and the transfer fails silently
- [ ] Permissions incl. `POST_NOTIFICATIONS` and `NEARBY_WIFI_DEVICES`
- [ ] MediaStore ingestion with `IS_PENDING`, shown as its own progress state

### 4. iOS app
- [ ] `NEHotspotConfiguration` join with **polled** readiness (never a fixed
      sleep), `alreadyAssociated` treated as success
- [ ] QR scanning; `NSLocalNetworkUsageDescription` in Info.plist
- [ ] `IP_BOUND_IF` pinning wired through the FFI's `bound_interface_index`
- [ ] Checkpoint on background, auto-resume on foreground
- [ ] PhotoKit ingestion with `shouldMoveFile = true`, preserving `creationDate`

### 5. Engine work still outstanding
- [ ] **Resume across reconnect.** The bitmap and the protocol are done and the
      sender already transmits only missing chunks. What is missing: rebuilding
      a bitmap from a staging file left by an interrupted session (verify each
      chunk against the manifest, set bits for those that pass), and persisting
      `.aether_state` between runs.
- [ ] **Real pre-allocation.** `io::SinkFile::create` uses `set_len`, which
      reserves size but not blocks. Needs `fallocate` on Android and
      `F_PREALLOCATE` on iOS; both want a `libc` dependency.
- [ ] **`bound_interface_index` is in `Config` but unused.** iOS needs it
      applied via `setsockopt(IP_BOUND_IF)` on every socket.
- [ ] Bidirectional transfer (currently sender-connects, receiver-listens only)
- [ ] Socket buffer tuning (`SO_SNDBUF`/`SO_RCVBUF`) — needs `socket2`
- [ ] Adaptive stream count and frame size under thermal pressure

### 6. Spec corrections to fold back into PRD-v1.1
- [ ] §5.3 says the frame header is 24 bytes. Its own fields sum to 28; the
      implementation uses **32**, restoring 16-byte alignment.
- [ ] §5.1 hardcodes 8 streams. The benchmark says derive from core count —
      4 beat 8 on a 4-core box, 16 was clearly worse. The FFI already does this
      (`stream_count = 0` means auto); the prose should match.

---

## Traps — things that cost real debugging time

**TLS session tickets caused truncated transfers.** The rustls server sends
`NewSessionTicket` after the handshake. The sender never reads it, so closing
its socket with unread data sends RST, discarding payload still in flight. Four
loopback tests failed on this. Fixed by `send_tls13_tickets = 0` plus a
symmetric close that drains to EOF. **If you ever re-enable resumption, this
comes back.**

**Progress throttling swallowed the first update.** Initialising "last emit
time" to zero makes the first update look like it already happened, so a UI
sits blank for a whole interval — or for the entire transfer, if it is shorter.
Needs a distinct never-emitted sentinel.

**Two peer-controlled values needed bounds.** `stream_count` arrives in the
peer's Hello and drives an accept loop; file size drives pre-allocation. Both
are capped in `Config`. If you add another value that comes from the peer,
assume it is hostile.

**`Cargo.lock` is gitignored.** Fine for a library workspace, but pin it before
shipping binaries so builds are reproducible.

---

## Running things

```sh
cd core
cargo test --workspace                       # 74 tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check

cargo build --release -p aetherlink-cli
./target/release/aetherlink bench --size-mib 512 --runs 3 --streams 4

# Two real machines on one LAN:
./target/release/aetherlink recv --port 52080 --out ./received   # prints fingerprint
./target/release/aetherlink send 192.168.1.42:52080 --fingerprint <hex> ./movie.mp4
```

```sh
./scripts/build-android.sh    # needs cargo-ndk + ANDROID_NDK_HOME
./scripts/build-ios.sh        # macOS + Xcode
```

---

## Using the engine from the apps

The shape is the same on both platforms: build a config, implement the
observer, construct a session, start a role. Every `start_*` returns
immediately — the transfer runs on the engine's own threads.

**Observer callbacks arrive on engine threads, not the main thread.** Hop before
touching UI. `onProgress` is already rate-limited to ~10 Hz, so it is safe to
bind straight to a progress bar.

### Kotlin

```kotlin
class Transfers : TransferObserver {
    private val session = EngineSession(
        // streamCount = 0 means "derive from the core count" — leave it alone.
        config = TransferConfig(),
        observer = this,
    )

    fun host(destination: File): HostHandle =
        // Port 0 lets the OS choose; the real port comes back in the handle.
        session.startHost(port = 0u, outputDir = destination.absolutePath)
            .also { qr.encode(it.fingerprintHex, it.port) }

    fun send(hostIp: String, port: UShort, fingerprint: String, files: List<FileItem>) =
        session.startClient(hostIp, port, fingerprint, files)

    override fun onProgress(bytesDone: ULong, bytesTotal: ULong, megabytesPerSecond: Double) {
        mainHandler.post { progressBar.setProgress(bytesDone, bytesTotal) }
    }
    override fun onStateChanged(state: SessionState) { /* ... */ }
    override fun onFileCompleted(path: String) { /* queue MediaStore ingestion */ }
    override fun onFinished(bytes: ULong, elapsedMillis: ULong) { /* ... */ }
    override fun onError(message: String) { /* ... */ }
}
```

### Swift

```swift
final class Transfers: TransferObserver {
    private lazy var session = try! EngineSession(config: TransferConfig(), observer: self)

    func host(destination: URL) throws -> HostHandle {
        let handle = try session.startHost(port: 0, outputDir: destination.path)
        qr.encode(fingerprint: handle.fingerprintHex, port: handle.port)
        return handle
    }

    func onProgress(bytesDone: UInt64, bytesTotal: UInt64, megabytesPerSecond: Double) {
        Task { @MainActor in progress.update(bytesDone, of: bytesTotal) }
    }
    func onStateChanged(state: SessionState) { /* ... */ }
    func onFileCompleted(path: String) { /* queue PhotoKit ingestion */ }
    func onFinished(bytes: UInt64, elapsedMillis: UInt64) { /* ... */ }
    func onError(message: String) { /* ... */ }
}
```

### Notes

- `FileItem.path` must be a real filesystem path. On Android, resolve a
  `content://` URI first — the engine mmaps it and cannot take a URI.
- `FileItem.relativePath` is what the receiver recreates. It is sanitized on
  arrival, so a `../` here is rejected rather than honoured.
- A session is reusable: after a transfer completes, fails, or is cancelled, it
  can start another. Only one at a time — a second concurrent `start_*` throws
  `EngineException.Busy`.
