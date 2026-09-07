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
- `resume` — durable `.aether_state` sidecar, plus a rebuild-by-verification
  fallback. **Resume across reconnect works end to end**: an interrupted
  transfer leaves a record, and the next attempt sends only what is missing.
- `platform` — `fallocate` / `F_PREALLOCATE`, `IP_BOUND_IF` interface pinning,
  and socket buffer sizing.
- `link` — `StreamSource`, which separates *who dials* from *who sends*, so the
  network host can transmit as well as receive.

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
- **99 tests green**, clippy clean at `-D warnings`, CI on every push.
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
- [ ] **Full-duplex.** Both directions work, but not simultaneously — one
      session carries one direction. PRD §9.3 defers this to v2; nothing in the
      MVP needs it.
- [ ] Adaptive stream count and frame size under thermal pressure (Sprint 4;
      needs thermal signals from the platform layer)
- [ ] `MADV_DONTNEED` behind the sender's read cursor, so a 10 GB file does not
      evict the page cache (PRD §5.5 asks for it; not yet implemented)

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

**Resume trusts a record only when it validates.** The `.aether_state` sidecar
is rejected — silently, falling back to a full transfer — if its checksum fails,
if its root hash, file size or chunk size disagree with the manifest, or if the
staging file is no longer the length we reserved. Re-transferring is cheap;
trusting a stale record produces a corrupt file that nothing later repairs. If
you add a field to the record, add it to the identity check too.

**Checkpoint ordering is the correctness argument, not an optimisation.** The
bitmap is snapshotted *before* the fsync, so every bit in the snapshot is a
write that had already completed. Snapshotting after the fsync would invert
this and could record a chunk still sitting in the page cache — which is
exactly the silently-corrupt-file failure the design exists to prevent.

**Interface pinning is iOS-only, by design.** `bind_to_interface` uses
`IP_BOUND_IF`, which is Darwin. On Android the equivalent is
`ConnectivityManager.bindProcessToNetwork` in Kotlin — `SO_BINDTODEVICE` needs
`CAP_NET_RAW`, which an app does not have. Passing a non-zero index on Android
returns an error naming the alternative rather than silently doing nothing.
**Without one or the other, Android routes our sockets to mobile data and the
transfer fails silently** — the direct link has no gateway.

**`Cargo.lock` is gitignored.** Fine for a library workspace, but pin it before
shipping binaries so builds are reproducible.

---

## Running things

```sh
cd core
cargo test --workspace                       # 99 tests
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

    // Android hosts the network either way, so it always has a HostHandle to
    // put in the QR. Port 0 lets the OS choose; the real port comes back.
    fun hostAndReceive(destination: File): HostHandle =
        session.startHostReceiving(port = 0u, outputDir = destination.absolutePath)
            .also { qr.encode(it.fingerprintHex, it.port) }

    fun hostAndSend(files: List<FileItem>): HostHandle =
        session.startHostSending(port = 0u, files = files)
            .also { qr.encode(it.fingerprintHex, it.port) }

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

    // iOS always joins, never hosts, so it is always a client — but it can be
    // on either end of the data flow.
    func sendToHost(_ files: [FileItem], at handle: ScannedQR) throws {
        try session.startClientSending(
            hostIp: handle.ip, port: handle.port,
            fingerprintHex: handle.fingerprint, files: files)
    }

    func receiveFromHost(_ handle: ScannedQR, into destination: URL) throws {
        try session.startClientReceiving(
            hostIp: handle.ip, port: handle.port,
            fingerprintHex: handle.fingerprint, outputDir: destination.path)
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

### The four entry points

Transport role is fixed by the topology — **iOS always dials, Android always
accepts**, because only Android can host the network and therefore only Android
has an address the other side knows in advance. Data direction is independent
of that, which is why there are four methods rather than two:

| | Android (hosts the network) | iOS (joins it) |
|---|---|---|
| **Android → iOS** | `startHostSending` | `startClientReceiving` |
| **iOS → Android** | `startHostReceiving` | `startClientSending` |

iOS must also set `boundInterfaceIndex` to `if_nametoindex("en0")`, or the OS
routes the sockets over cellular where they reach nothing.

### Notes

- `FileItem.path` must be a real filesystem path. On Android, resolve a
  `content://` URI first — the engine mmaps it and cannot take a URI.
- `FileItem.relativePath` is what the receiver recreates. It is sanitized on
  arrival, so a `../` here is rejected rather than honoured.
- A session is reusable: after a transfer completes, fails, or is cancelled, it
  can start another. Only one at a time — a second concurrent `start_*` throws
  `EngineException.Busy`.
- **Resume is on by default and needs nothing from the app.** Point a retry at
  the same `outputDir` with the same files and only the missing chunks travel.
  On iOS this is what turns backgrounding from a lost transfer into a pause:
  checkpoint on background, start again on return.
- `checkpointBytes` (default 64 MB) sets how much is re-sent after an
  interruption. Each checkpoint costs an fsync, so lowering it trades write
  throughput for finer resume granularity.
