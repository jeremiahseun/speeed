# Benchmark log

## Sprint 1 — software-ceiling gate (PRD §2.4)

**Question this answers:** is the engine ever the bottleneck? If it moves an
order of magnitude more than the radio can, then Tier A–D results are the
hardware's limit and not ours.

### Environment

Cloud container, **4 cores**, 15 GB RAM, Rust 1.94, `--release` (thin LTO,
`codegen-units = 1`). Source and destination both on `/dev/shm` (tmpfs).

Two caveats that make these numbers *conservative*:

1. **Both endpoints share the same 4 cores.** A real transfer has a sender and
   a receiver on separate devices, each with its own CPU. Here they contend.
2. **Both endpoints share the same RAM.** The tmpfs source, the tmpfs
   destination, and the page cache all compete for the same 15 GB.

### Results — 512 MiB payload, 3 runs each

Stream count, at 256 KiB frames and 4 MiB chunks:

| Streams | Best | Mean |
|---|---|---|
| 2 | 910 MB/s | 673 MB/s |
| **4** | **1130 MB/s** | **849 MB/s** |
| 8 | 994 MB/s | 859 MB/s |
| 16 | 700 MB/s | 559 MB/s |

Frame size, at 4 streams:

| Frame | Best | Mean |
|---|---|---|
| 64 KiB | 1221 MB/s | 1172 MB/s |
| 256 KiB | 1244 MB/s | 1068 MB/s |
| 1 MiB | **1288 MB/s** | 1004 MB/s |

### Results — payload scaling, 4 streams

| Payload | Best | Mean |
|---|---|---|
| 512 MiB | 1244 MB/s | 1068 MB/s |
| 1 GiB | 1080 MB/s | 513 MB/s |
| 2 GiB | 341 MB/s | 273 MB/s |

### Reading

**The gate is met at 512 MiB–1 GiB: 1.24 GB/s peak against a 1.2 GB/s target**,
with full TLS 1.3 AES-GCM and BLAKE3 verification of every chunk in the path.

**The 2 GiB fall-off is the container, not the engine.** A 2 GiB source plus a
2 GiB destination on tmpfs is 4 GiB of RAM before page cache, on a box also
running both endpoints. It is memory pressure on a shared host, and it should
not be read as a scaling limit — but it does mean the >1 GB/s figure is only
demonstrated up to 1 GiB here, and wants re-running on a real workstation.

**Four streams beat eight on a 4-core box, and sixteen is clearly worse.** That
is the expected shape: streams exist to saturate cores with AEAD work, so the
useful count tracks available parallelism. On an 8-core phone the PRD's default
of 8 is likely right, but `stream_count` should be derived from
`available_parallelism()` rather than hardcoded. Worth a follow-up.

**Frame size barely matters between 64 KiB and 1 MiB.** The 256 KiB default is
fine; there is no reason to tune it further before the radio is in the picture.

### What this does not tell us

Nothing about the radio. Loopback has no packet loss, no contention, no
retransmits, and effectively infinite bandwidth. **Sprint 0 on real hardware is
still the number that decides whether the product hits Tier B.** This result
only establishes that when the radio delivers 45–110 MB/s, the engine will not
be what caps it — it has roughly 10× of headroom.

---

## After real pre-allocation and role decoupling

Re-run of the same configuration once `fallocate`/`F_PREALLOCATE` replaced
`ftruncate` on the receive path.

| Payload | Best | Mean | Was (best) |
|---|---|---|---|
| 512 MiB, 4 streams | **2188 MB/s** | 1270–1968 MB/s | 1288 MB/s |

**Pre-allocation roughly doubled peak throughput.** `ftruncate` sets a length
and nothing else, so every page of the destination was faulted in on first
write, on the hot path, while the socket waited. Reserving the blocks up front
moves that work out of the transfer. The effect is exaggerated here because the
destination is tmpfs — page allocation *is* the write — but the same mechanism
applies to a real filesystem, which is why PRD §5.5 asked for it.

Mean stays well below best because the first run of each batch is cold. Read
the best figure as the ceiling and the mean as a reminder that the first
transfer after boot pays for warming the cache.

CLI check of the host-sends direction, 40 MB over loopback:

```
sent        1099.88 MB/s   received    1095.22 MB/s   bytes match
```

---

## Page release behind the sender's cursor

`MADV_DONTNEED` behind the read cursor, so sending a large file does not walk
its whole length through the page cache. Measured on a 1 GB file over loopback,
sender in its own process, peak `VmRSS` sampled during the transfer.

| | Peak RSS | Throughput |
|---|---|---|
| Release **off** | **960 MB** every run | 231 / 821 / 642 MB/s |
| Release **on** | 631 / 268 / 211 MB | 604 / 322 / 336 MB/s |

**The property that matters is not the average, it is the shape.** With release
off, peak RSS equals file size, every time — a 10 GB transfer maps 10 GB. With
it on, it does not track file size at all. The spread (211–631 MB) is how fast
pages fault in versus how fast the 32 MB batches release them, which depends on
cache warmth.

Throughput differs by more than 2× *in both directions* across runs, so this
container cannot resolve a throughput effect. Do not read one from the table.

### Three measurement mistakes worth recording

Getting this number right took three tries, and each wrong answer was
confidently wrong.

1. **`mincore` showed nothing.** On a file-backed mapping it reports whether a
   page is in the *page cache*, which `MADV_DONTNEED` deliberately does not
   change — leaving the page cached is what makes a re-fault cheap. RSS is the
   metric; `/proc/self/statm` is the instrument.
2. **`VmHWM` showed nothing.** It is a high-water mark, so it records the peak
   *before* any release and never comes down. Instantaneous `VmRSS` has to be
   sampled during the transfer.
3. **The first A/B showed a 3× slowdown that did not exist.** Runs were ordered
   on-then-off, so the first paid to read the file from disk and the second
   inherited a warm cache. Equalising cache state before every run (`cat file >
   /dev/null`) made the difference vanish.

The genuine bug the exercise did find: `chunk_hashes` touches the whole file to
build the manifest *before* any byte is sent, so peak RSS was set there and
releasing during the send changed nothing. The hashing pass now releases behind
itself too, above a 128 MB threshold — below that, keeping the file cached is
cheap and makes the send pass a memory read.

### Reproducing

```sh
cd core
cargo build --release -p aetherlink-cli
./target/release/aetherlink bench --size-mib 512 --runs 3 --streams 4
```
