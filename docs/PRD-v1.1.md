# AetherLink — Product Requirements & Technical Specification

**Version:** 1.1.0 (supersedes 1.0.0)
**Scope:** v1.0 MVP — native Android, native iOS. **No server component.**
**Platforms:** Android 10+ (API 29+), iOS 16.0+

---

## 0. What changed from v1.0, and why

v1.0 was architecturally coherent but rested on several platform assumptions that do not
hold on shipping hardware. This revision corrects them. Every change below is a
correction of fact, not a change of ambition — the goal is still the fastest
cross-platform local transfer app that exists.

| # | v1.0 assumption | Reality | v1.1 decision |
|---|---|---|---|
| 1 | `startLocalOnlyHotspot(config, …)` forces 5 GHz | That overload is `@SystemApi`, gated on `NETWORK_SETTINGS`. Third-party apps get the no-config overload and whatever band the OEM picks — often 2.4 GHz. | **Wi-Fi Direct Group Owner is the primary link**, where `setGroupOperatingBand(5GHZ)` *is* public API. LOHS demoted to fallback. |
| 2 | 6 GHz is a target band | SoftAP/P2P on 6 GHz requires WPA3-SAE and Wi-Fi 6E on both ends; `NEHotspotConfiguration` does not reliably join SAE-only networks. | **6 GHz dropped from v1.** Revisit when Wi-Fi Aware/6E pairing matures. |
| 3 | ≥100 MB/s sustained is the headline target | 2×2 80 MHz Wi-Fi 5 caps around 55–70 MB/s real. Android P2P GO interfaces frequently run 1×1 or 80 MHz when the STA chain is also up. | **Tiered targets** (§2). 100+ MB/s is a Tier-A stretch goal on Wi-Fi 6 pairs, not the baseline gate. |
| 4 | QUIC primary, multi-TCP fallback | Userspace QUIC on mobile is syscall- and CPU-bound (no reliable UDP GSO/GRO on Android/iOS); typically 30–60 MB/s on a phone core. HOL blocking is irrelevant for bulk transfer where all bytes are required anyway. | **Inverted: multi-stream TCP + TLS 1.3 is primary.** QUIC becomes a measured experiment behind a flag. |
| 5 | Zero-copy via `sendfile`/`splice` | `sendfile` cannot carry TLS without kTLS. Android kernels ship it disabled; Darwin has no equivalent. Encrypted bulk transfer *must* traverse userspace. | **"Single-copy" pipeline**: mmap source → encrypt in-place into a pooled buffer → `writev`. Honest naming, same performance ceiling. |
| 6 | Noise XX handshake | XX authenticates nobody from out-of-band data. The QR already carries the host's static public key, which is exactly the input `IK`/`NK` want. | **TLS 1.3 (rustls) with raw public keys pinned to the QR fingerprint.** Hardware AES-GCM, one dependency, far less bespoke crypto. |
| 7 | BLAKE3 checksum on every wire frame | Every record is already AEAD-authenticated by TLS. A second 32-byte MAC per frame is pure overhead. | Per-frame checksum removed. BLAKE3 retained for **content integrity and resume**, at chunk granularity. |
| 8 | Silent-audio background keepalive | App Store Review Guideline 2.5.4 — a documented rejection vector. | **Removed.** Transfers are foreground-only with an explicit UX contract. |
| 9 | <3.5 s QR-scan-to-socket | iOS shows a non-suppressible system join prompt; GO bring-up is 1.5–3 s; DHCP adds 1–3 s. | Restated to **≤12 s cold, P50**, measured and owned as a real number. |
| 10 | `SO_BINDTODEVICE` on iOS | Linux-only. Darwin's equivalent is `IP_BOUND_IF` / `NWParameters.requiredInterfaceType`. | Corrected in §6.2. |
| 11 | — (absent) | iOS 14+ requires **Local Network** permission for *any* local-subnet traffic; Android 13+ requires `POST_NOTIFICATIONS` for the foreground service. | Added to §6. |
| 12 | Server component | The product is peer-to-peer by definition; a backend adds cost, privacy surface, and no MVP capability. | **No server in v1.** See §9 for what a v2 backend would be for. |

---

## 1. Product Vision

AetherLink is an ad-free, account-free, peer-to-peer file transfer app for Android and
iOS. It creates a direct radio link between two phones — no router, no cloud, no
account — and moves multi-gigabyte payloads at the physical limit of that link.

The three things that make it faster than the alternatives:

1. **It owns the radio.** A Wi-Fi Direct 5 GHz group, not the user's congested 2.4 GHz
   router. This removes the double-hop and roughly quadruples the ceiling versus
   LocalSend-class tools.
2. **It owns the transport.** Eight parallel TLS streams over TCP, tuned buffers, and
   a framing format designed for bulk throughput rather than RPC generality.
3. **It owns the I/O.** Memory-mapped reads, pre-allocated writes, SIMD hashing on
   worker threads, and a staging pipeline that keeps OS media indexers off the hot path.

**Non-goals for v1:** internet/WAN transfer, accounts, sync, backup, desktop clients,
transfer to devices without the app (see §9.1).

---

## 2. Performance Targets (Tiered)

Throughput is a property of the radio pair, not of our code, once the software stops
being the bottleneck. So targets are stated per hardware tier. **The engineering
commitment is that software is never the limiting factor** — validated by the
loopback benchmark in §2.2.

### 2.1 Throughput tiers

| Tier | Hardware | Link | Target sustained | Gate |
|---|---|---|---|---|
| **A** | Wi-Fi 6, 2×2, 80 MHz both ends (e.g. Pixel 8 / S23 ↔ iPhone 14 Pro) | 5 GHz P2P GO | **85–110 MB/s** | Stretch |
| **B** | Wi-Fi 5, 2×2, 80 MHz | 5 GHz P2P GO | **45–70 MB/s** | **Ship gate** |
| **C** | 5 GHz, 1×1 or 40 MHz | 5 GHz P2P GO | 20–35 MB/s | Must not regress |
| **D** | 2.4 GHz fallback | LOHS / 2.4 GHz GO | 8–16 MB/s | Must show UI banner |

Measurement condition for all tiers: single 10 GB file, 1 m line of sight, both devices
>50% battery, not thermally throttled, airplane-mode-off but no other Wi-Fi client
associated.

### 2.2 Software-ceiling gate (the number that is actually ours)

Over loopback / a wired 10 GbE link between two dev machines, `aetherlink-core` must
sustain **≥ 1.2 GB/s** encrypted with full BLAKE3 verification. On device, over
`localhost`, **≥ 400 MB/s**. If the engine can do 10× the radio, the radio is honestly
the limit and Tier B/C results are the hardware's fault, not ours.

### 2.3 Other KPIs

| Metric | Target | Note |
|---|---|---|
| Cold connect (QR framed → first byte) | **≤ 12 s P50, ≤ 20 s P95** | Includes non-suppressible iOS join prompt (~3–6 s) and DHCP |
| Warm reconnect (known peer, resume) | ≤ 5 s P50 | |
| BLE discovery → UI prompt | ≤ 3 s P50 | Was 1.5 s; GATT connect + MTU negotiation alone is ~1–2 s |
| Peak RSS, sustained streaming | < 120 MB | Was 80 MB; 8 streams × 2 MB buffers + mmap accounting is realistic at 120 |
| CPU, sustained Tier-B | < 220% of one core (≈27% of 8) | AES-GCM on ARMv8 crypto extensions is ~2–5 GB/s/core, not the bottleneck |
| Post-transfer verification lag | **0 s** | Merkle hashing completes with the last packet |
| Crash-free session rate | > 99.9% | |
| Resume correctness | 100% | 500 forced mid-transfer disconnects, zero corrupt files |

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Presentation          Android: Jetpack Compose                 │
│                        iOS:     SwiftUI                         │
├─────────────────────────────────────────────────────────────────┤
│  Platform Link Layer   (the part that cannot be shared)         │
│   Android: WifiP2pManager (GO, 5 GHz) │ LOHS fallback           │
│            BLE peripheral, CameraX QR, MediaStore ingest        │
│   iOS:     NEHotspotConfiguration │ CoreBluetooth central       │
│            AVFoundation QR scan, PhotoKit ingest                │
├─────────────────────────────────────────────────────────────────┤
│  UniFFI bridge — generated Kotlin + Swift bindings              │
├─────────────────────────────────────────────────────────────────┤
│  aetherlink-core (Rust, ~85% of total logic)                    │
│   ┌────────────────┬─────────────────┬───────────────────────┐  │
│   │ Multi-stream   │ TLS 1.3 (rustls)│ Single-copy I/O       │  │
│   │ TCP scheduler  │ raw-PK pinning  │ mmap read / pwrite    │  │
│   ├────────────────┼─────────────────┼───────────────────────┤  │
│   │ BLAKE3 Merkle  │ Resume state    │ Manifest & framing    │  │
│   │ (SIMD workers) │ machine + bitmap│ (protobuf control)    │  │
│   └────────────────┴─────────────────┴───────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

The rule that keeps this honest: **anything that can live in Rust, does.** The native
layers own only what the OS refuses to expose to a library — radio configuration,
permissions, camera, media libraries, and UI.

---

## 4. Link Layer

### 4.1 Topology

The asymmetry is forced by iOS: a third-party iOS app can *join* a Wi-Fi network but
can never *create* one. **Therefore Android is always the network host**, regardless of
transfer direction. An iOS→Android send still has Android hosting the group; only the
data direction flips.

**iOS↔iOS is not supported in v1** and is not a small gap to close — it requires
Multipeer Connectivity / AWDL, which is a different transport with a different (and
much lower, ~10–25 MB/s) ceiling. Scoped out explicitly.

### 4.2 Android host: three-tier link strategy

Attempt in order, fall back on failure, surface the achieved tier in the UI:

**Tier 1 — Wi-Fi Direct Group Owner, 5 GHz (primary):**
```kotlin
val config = WifiP2pConfig.Builder()
    .setNetworkName("DIRECT-AL-$sessionTag")   // must begin with "DIRECT-"
    .setPassphrase(generatePassphrase())        // 8–63 chars, ours to choose
    .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
    .enablePersistentMode(false)
    .build()
manager.createGroup(channel, config, listener)
```
Public API since API 29. Gives us a **known SSID and passphrase** (unlike LOHS, where
they are generated for us) and **real band control**. The group is a standard WPA2-PSK
network — iOS joins it with `NEHotspotConfiguration` like any other Wi-Fi network. GO
address is fixed at `192.168.49.1`.

*Known constraints:* some OEMs (certain Xiaomi/Honor builds) silently ignore the band
request; P2P is not permitted on DFS channels, so the group lands on 36–48 or 149–165.
Both are acceptable. Detect the actual channel via `WifiP2pGroup.getFrequency()` and
record it in telemetry.

**Tier 2 — `startLocalOnlyHotspot` (fallback):** No band control for third-party apps.
Read the assigned SSID/passphrase from `reservation.softApConfiguration` (API 30+).
Check the resulting band; if 2.4 GHz, show the Tier-D banner.

**Tier 3 — Existing shared LAN:** Both devices already on the same router. Slowest
(double-hop) but zero-friction. Discovery via mDNS (`_aetherlink._tcp`). This is the
LocalSend model and belongs in the product as a graceful floor, not as an embarrassment.

### 4.3 iOS client: joining

```swift
let config = NEHotspotConfiguration(ssid: ssid, passphrase: psk, isWEP: false)
config.joinOnce = true          // torn down when the app exits
try await NEHotspotConfigurationManager.shared.apply(config)
```

Requirements and gotchas that must be designed around, not discovered late:

- Requires the `com.apple.developer.networking.HotspotConfiguration` entitlement.
- iOS displays a **system join prompt the app cannot suppress**. This is the single
  largest contributor to connect latency and is why §2.3 says 12 s, not 3.5 s.
- `apply` returning success **does not mean the link is usable.** DHCP may still be
  negotiating. Do not sleep a fixed 1.2 s (v1.0's approach) — **poll**: attempt a TCP
  connect to `192.168.49.1:PORT` every 250 ms with a 15 s deadline.
- `joinOnce` configs are removed on app termination. If the app is backgrounded
  mid-transfer, iOS may drop the network. See §6.2.
- Error `NEHotspotConfigurationError.alreadyAssociated` is a *success* case — handle it.

### 4.4 Interface pinning (no WAN on this link)

The P2P group has no internet gateway. Both OSes will try to route around it.

- **iOS:** `NWParameters` with `requiredInterfaceType = .wifi` and
  `prohibitedInterfaceTypes = [.cellular]`. For the BSD socket path used by the Rust
  core, `setsockopt(fd, IPPROTO_IP, IP_BOUND_IF, &ifIndex, …)` — **not**
  `SO_BINDTODEVICE`, which does not exist on Darwin. The interface index comes from
  `if_nametoindex("en0")`, passed down through FFI.
- **Android:** `ConnectivityManager.bindProcessToNetwork(network)` using the `Network`
  object from the P2P group's `NetworkRequest`. Without this, Android routes our
  sockets to mobile data and the transfer silently fails.

---

## 5. Transport & Engine

### 5.1 Transport selection

**Primary: 8 parallel TCP connections, each wrapped in TLS 1.3.**

Rationale — this is the correction that matters most. On a one-hop 5 GHz link:
- TCP gets kernel TSO/GSO, GRO, and a mature congestion controller for free.
- QUIC on mobile pays a syscall per datagram; `UDP_SEGMENT`/GSO is unavailable or
  unreliable on Android vendor kernels and absent on iOS. Measured ceilings for
  userspace QUIC on phone-class cores land at 30–60 MB/s — *below our Tier-B target.*
- Head-of-line blocking, QUIC's headline advantage, is irrelevant here: for a bulk file
  transfer every byte is required before the file is usable, so there is no head to block.

Parallel streams exist not for HOL avoidance but to **saturate multiple CPU cores with
AEAD work and keep the send queue deep enough to hide per-syscall latency.** Eight is
a starting point; §5.4 tunes it.

**Experimental: QUIC (quinn) behind a build flag.** Benchmarked in Sprint 1 against
multi-TCP on real hardware. Promoted only if it wins. Expected outcome: it loses on
throughput and wins on reconnect latency — which may make it the right choice for the
*control* channel later.

### 5.2 Security

TLS 1.3 via `rustls`, with **raw public keys (RFC 7250)** or a self-signed certificate
whose SPKI fingerprint is pinned to the value carried in the QR/BLE payload.

- Cipher suite preference: `TLS13_AES_128_GCM_SHA256` first (ARMv8 crypto extensions
  give 2–5 GB/s/core), `TLS13_CHACHA20_POLY1305_SHA256` fallback for anything without
  hardware AES.
- The out-of-band QR fingerprint provides responder authentication and makes the whole
  exchange resistant to an on-path attacker who joined the group. This is what Noise
  `IK` would have given us — but rustls is one well-audited dependency rather than a
  bespoke handshake, and it is hardware-accelerated on both platforms.
- The Wi-Fi passphrase in the QR is ephemeral and per-session. Anyone who photographs
  the screen can join the radio group; they still cannot complete the TLS handshake
  without the pinned key. Threat model documented, not hand-waved.

### 5.3 Wire format

Two layers, cleanly separated — v1.0 mixed three serialization formats.

**Control channel (stream 0):** length-prefixed protobuf. Low volume, schema evolution
matters here.

**Data channel (streams 1..N):** fixed 24-byte binary header, then raw payload bytes.
No protobuf, no per-frame checksum (TLS already authenticates every record).

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Magic 0xAE10 |  Ver  | Type  |          Flags                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          File ID (u64)                        |
+                                                               +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Byte Offset (u64)                      |
+                                                               +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length (u32)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Payload bytes …                        |
```

All fields little-endian (both target architectures are LE; no byte-swap cost).

**Two distinct sizes, which v1.0 conflated:**
- **Wire frame:** 256 KB. Sized to fill the socket buffer without stalling.
- **Verification chunk:** 4 MB. The unit of BLAKE3 hashing and of the resume bitmap.

### 5.4 Control protobuf schema

```protobuf
syntax = "proto3";
package aetherlink.wire;

message FileMetadata {
  uint64 file_id            = 1;
  string relative_path      = 2;   // preserves directory structure
  uint64 size_bytes         = 3;
  string mime_type          = 4;
  bytes  blake3_root_hash   = 5;   // bytes, not string
  int64  modified_unix_ms   = 6;
  int64  created_unix_ms    = 7;   // needed for correct Photos/Gallery ordering
}

message ManifestOffer {
  bytes  session_id   = 1;         // 16-byte UUID, matches header width
  uint64 total_bytes  = 2;
  uint32 total_files  = 3;
  repeated FileMetadata files = 4;
}

message ManifestAccept {
  bytes  session_id            = 1;
  repeated uint64 accepted_ids = 2;   // receiver may decline individual files
}

message ResumeRequest {
  bytes  session_id  = 1;
  uint64 file_id     = 2;
  bytes  chunk_bitmap = 3;         // 1 bit per 4 MB chunk, LSB-first
}

message TransferProgress {
  bytes  session_id       = 1;
  uint64 bytes_transferred = 2;
  double throughput_mbps   = 3;
}
```

### 5.5 I/O pipeline (single-copy, honestly named)

**Sender:**
1. `mmap(fd, MAP_PRIVATE)` + `madvise(MADV_SEQUENTIAL | MADV_WILLNEED)`.
2. Encrypt directly from the mapped region into a pooled output buffer — one copy,
   unavoidable while TLS is in userspace.
3. `writev` header + ciphertext. Buffers return to the pool; **no per-frame allocation**.
4. `madvise(MADV_DONTNEED)` behind the read cursor so a 10 GB file does not evict the
   page cache.

**Receiver:**
1. On manifest accept, pre-allocate: `fallocate()` on Android,
   `fcntl(F_PREALLOCATE)` + `ftruncate()` on iOS. Prevents fragmentation under
   sustained write load.
2. **`pwrite` at the frame's byte offset — not mmap.** Large sequential mmap writes
   cause unpredictable dirty-page writeback stalls; `pwrite` with a deep queue is as
   fast and far more predictable. (v1.0 specified mmap on both sides; this is a
   deliberate reversal on the receive path only.)
3. Hash chunks on a `rayon` worker pool as they complete — BLAKE3 with NEON, ~1–3 GB/s
   per core. The Merkle root is finished when the last frame lands: **zero-second
   verification**, as v1.0 correctly intended.
4. `sync_file_range` periodically; `F_FULLFSYNC` once at completion.

### 5.6 Threading

```
UI thread ──FFI (non-blocking)──▶ Tokio multi-thread runtime
                                   ├─ 1 control task
                                   ├─ 8 stream tasks (socket + AEAD)
                                   ├─ rayon pool: BLAKE3 (n_cpus/2)
                                   └─ blocking pool: pwrite / fallocate / fsync
```

Backpressure is explicit: bounded `mpsc` channels between network and disk. If the
flash cannot keep up with the radio, the sockets stall — never an unbounded queue,
which is how you get the OOM crashes the KPI table forbids.

---

## 6. FFI Surface

UniFFI-generated. Two corrections to v1.0's sketch that are hard requirements of the
tool, not preferences:

- Exported object methods take **`&self`**, never `&mut self`. State lives behind
  `Mutex`/atomics inside.
- Callback interfaces are **`Arc<dyn Trait>`**, not `Box<dyn Trait>`.

```rust
#[derive(uniffi::Record)]
pub struct TransferConfig {
    pub bind_port: u16,
    pub parallel_streams: u8,      // default 8
    pub frame_size_bytes: u32,     // default 262_144
    pub chunk_size_bytes: u32,     // default 4_194_304
    pub staging_directory: String,
    pub bound_interface_index: u32, // 0 = unbound; iOS passes if_nametoindex("en0")
}

#[uniffi::export(callback_interface)]
pub trait TransferObserver: Send + Sync {
    fn on_state_changed(&self, state: SessionState);
    fn on_progress(&self, bytes_done: u64, bytes_total: u64, mbps: f32, eta_secs: u32);
    fn on_file_completed(&self, file_id: u64, staged_path: String);
    fn on_error(&self, code: ErrorCode, message: String, recoverable: bool);
}

#[uniffi::export]
impl EngineSession {
    #[uniffi::constructor]
    pub fn new(config: TransferConfig, observer: Arc<dyn TransferObserver>) -> Arc<Self>;

    pub fn start_host(&self) -> Result<HostHandle, EngineError>;   // returns port + pubkey fingerprint
    pub fn start_client(&self, host_ip: String, port: u16, pinned_fp: Vec<u8>) -> Result<(), EngineError>;
    pub fn offer_files(&self, items: Vec<FileItem>) -> Result<(), EngineError>;
    pub fn accept_offer(&self, file_ids: Vec<u64>) -> Result<(), EngineError>;
    pub fn pause(&self);
    pub fn resume(&self);
    pub fn cancel(&self);
}
```

**Toolchain:** `cargo-ndk` for `aarch64-linux-android` + `x86_64-linux-android`;
`aarch64-apple-ios` + `aarch64-apple-ios-sim` packaged as an XCFramework. Both wired
into CI so a Rust change cannot silently break either app.

---

## 7. Platform Layers

### 7.1 Android

**Permissions.** v1.0's list plus two that were missing:
```xml
<uses-permission android:name="android.permission.POST_NOTIFICATIONS" />   <!-- API 33+, FGS is invisible without it -->
<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION"
    android:maxSdkVersion="32" />                                          <!-- superseded by NEARBY_WIFI_DEVICES -->
<uses-permission android:name="android.permission.NEARBY_WIFI_DEVICES"
    android:usesPermissionFlags="neverForLocation" />
```
Plus `CHANGE_WIFI_STATE`, `ACCESS_WIFI_STATE`, `BLUETOOTH_ADVERTISE/SCAN/CONNECT`,
`FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_DATA_SYNC`, `WAKE_LOCK`.

**Survival against OEM killers.** Foreground service (`dataSync` type) + a
`PARTIAL_WAKE_LOCK` + `WifiManager.createWifiLock(WIFI_MODE_FULL_HIGH_PERF)`. On
Xiaomi/Oppo/Vivo, additionally deep-link to the autostart settings page on first run —
without it, these OEMs kill the process regardless of correct API usage.

**Ingestion.** Two-phase, as v1.0 specified, with one addition: the Phase-2 move into
`MediaStore` is a **ContentResolver stream copy, not a rename** — budget ~20 s for
10 GB on UFS. Show it as a distinct "Saving to Gallery" progress state rather than
letting the transfer appear to hang at 100%. Use `IS_PENDING=1` during the write.
`MediaScannerConnection` is legacy and unnecessary on API 29+.

### 7.2 iOS

**Entitlements & Info.plist:**
- `com.apple.developer.networking.HotspotConfiguration`
- `com.apple.developer.networking.multicast` (only if mDNS discovery for Tier 3)
- `NSLocalNetworkUsageDescription` — **mandatory since iOS 14**; without it every
  local-subnet socket fails. Absent from v1.0 and would have been a launch blocker.
- `NSBonjourServices` = `_aetherlink._tcp` if using mDNS.
- `NSCameraUsageDescription`, `NSPhotoLibraryAddUsageDescription`, `NSBluetoothAlwaysUsageDescription`.

**Background execution — the honest position.** iOS gives a third-party app roughly
30 s via `beginBackgroundTask` and nothing more. v1.0's silent-audio workaround
violates Guideline 2.5.4 and will be rejected. Therefore:
- `isIdleTimerDisabled = true` for the duration of a transfer.
- Clear UI contract: "Keep AetherLink open — iOS pauses transfers in the background."
- On backgrounding, checkpoint the resume bitmap immediately and enter `PAUSED`.
- On foregrounding, auto-resume from the bitmap. **Resume is what makes this
  acceptable UX** — it turns a hard failure into a 3-second hiccup.

**Ingestion.** `PHAssetCreationRequest.addResource(with:fileURL:options:)` with
`shouldMoveFile = true` — a move, no copy, so iOS ingestion is genuinely fast where
Android's is not. Set `creationDate` from `FileMetadata.created_unix_ms` and preserve
original EXIF. Live Photos require both the `.photo` and `.pairedVideo` resources added
to a **single** request.

---

## 8. State Machine & Resume

```
DISCONNECTED ──link established──▶ HANDSHAKING ──TLS + pin verified──▶ NEGOTIATING
                                        │                                   │
                                        │ pin mismatch / timeout            │ manifest accepted
                                        ▼                                   ▼
                                     FAILED                            TRANSFERRING ◀─┐
                                                                            │         │
                            ┌───────────────────────────────────────────────┤         │
                            │ socket dropped / iOS backgrounded             │ all     │
                            ▼                                               │ chunks  │ resumed
                        INTERRUPTED ──peer back in range──▶ RESUMING ───────┘ verified│
                            │  (bitmap checkpointed)                                  │
                            │ 60 s timeout                                            ▼
                            ▼                                                    INGESTING
                        DISCONNECTED                                                  │
                                                                                      ▼
                                                                                 COMPLETED
```

**Resume protocol.** Both peers persist `.aether_state` (session id, per-file chunk
bitmap, 1 bit per 4 MB) fsynced every 64 MB. On reconnect the receiver sends
`ResumeRequest` with its bitmap; the sender transmits only zero bits. A 10 GB file
needs a 320-byte bitmap.

**Correctness requirement:** a chunk's bit is set only *after* its BLAKE3 hash matches
the manifest **and** the containing region is fsynced. Setting it on write completion
alone would let a power loss produce a silently corrupt file that resume refuses to fix.

---

## 9. Explicitly Deferred

### 9.1 Web client mode (v2)
Embedded `axum` server + a browser download URL, for receivers without the app. Real
value, but it forces a second, unencrypted-at-app-layer path through the engine and its
own security review. Not MVP.

### 9.2 Backend (v2 — decided out of v1)
There is no server in v1, by decision. The two cases that would justify one later:
- **Telemetry.** Aggregating tier/throughput/failure data across real hardware is the
  only way to know whether §2.1's tiers are right. Highest-value backend, smallest scope.
- **WAN relay.** Rendezvous + TURN-style relay for off-LAN transfer. This is a second
  product, not a feature.

### 9.3 Also deferred
iOS↔iOS (§4.1), full-duplex simultaneous bidirectional transfer, desktop clients,
6 GHz, Wi-Fi Aware discovery.

---

## 10. Roadmap

**Sprint 0 — Hardware truth (1 week).** Before writing engine code, prove the radio.
Bring up a Wi-Fi Direct 5 GHz GO on three Android devices, join from two iPhones, and
measure raw `iperf3` throughput. **This single number determines whether the rest of
the plan is worth building as specified.** If real-world Tier-B lands at 25 MB/s rather
than 55, the targets and possibly the transport choice change.

**Sprint 1 — Core engine.** `aetherlink-core`: multi-TCP + rustls, framing, mmap/pwrite
pipeline, BLAKE3 Merkle, resume state machine. CLI harness. Gate: §2.2 software ceiling.

**Sprint 2 — Link automation.** Android GO + LOHS + QR generation; iOS join + polling
readiness detection + QR scan; BLE as secondary discovery; interface pinning both sides.
Gate: end-to-end 1 GB transfer, phone to phone.

**Sprint 3 — FFI & ingestion.** UniFFI bindings, CI for both toolchains, two-phase
ingestion into Photos and MediaStore with correct timestamps, resume across real
disconnects.

**Sprint 4 — UI, hardening, thermals.** Compose + SwiftUI. Adaptive stream count and
frame size under thermal pressure. OEM-killer mitigations. 2,000-small-file batch test.

---

## 11. Definition of Done

- [ ] Tier B (Wi-Fi 5, 2×2) sustains **≥ 45 MB/s**; Tier A reaches **≥ 85 MB/s**
- [ ] Software ceiling ≥ 400 MB/s device-loopback, ≥ 1.2 GB/s desktop (§2.2)
- [ ] 10 GB single file completes with zero corruption, verified by independent BLAKE3
- [ ] 2,000 mixed small files (100 KB–5 MB): no crash, no OOM, no leaked fds
- [ ] 500 forced mid-transfer disconnects: 100% resume, 0 corrupt outputs
- [ ] Cold connect ≤ 12 s P50 across 5 Android × 3 iOS device pairs
- [ ] 2.4 GHz fallback works and shows the speed-limit banner
- [ ] Files land in Photos and Gallery with correct creation timestamps and EXIF intact
- [ ] iOS: backgrounding pauses and foregrounding resumes, no data loss, no 2.5.4 violation
- [ ] Android: survives 30-minute transfer on Xiaomi/Samsung/Pixel without process death
- [ ] Peak RSS < 120 MB throughout
