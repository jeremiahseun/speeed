# aetherlink-core

Shared Rust engine for AetherLink. Everything that can live here does; the
Android and iOS layers own only what the OS refuses to expose to a library.

See [`docs/PRD-v1.1.md`](../docs/PRD-v1.1.md) for the specification.

## Crates

| Crate | Status | Contents |
|---|---|---|
| `aetherlink-proto` | **implemented** | Frame header, chunk geometry, BLAKE3 verification, resume bitmap, manifest validation. Pure logic, no I/O. |
| `aetherlink-core` | not started | Transport (multi-stream TCP + rustls), file I/O pipeline, session state machine. |
| `aetherlink-cli` | not started | Test harness: `send`, `recv`, `bench`. |

## Developing

```sh
cd core
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

## Notes on the implementation

**Frame header is 32 bytes, not the 24 named in PRD §5.3.** The fields the spec
lists sum to 28; 32 restores the 16-byte alignment the original design asked
for. The spec text should be corrected to match.

**Path sanitization is the receiver's main attack surface.** A sender chooses
the paths a receiver writes to, so `manifest::sanitize_relative_path` is a hard
gate: nothing may join a peer-supplied path to a local directory without passing
through it. It rejects rather than rewrites, and covers traversal, absolute
paths, drive letters, backslash separators, control bytes, and names that
collide only after Windows normalization (`report.` vs `report`).

**Bitmap ordering is a durability requirement, not a detail.** A chunk's bit is
set only after its hash verifies *and* the region is fsynced. Setting it on
write completion alone lets a power loss leave a chunk marked present that
resume will then never repair.
