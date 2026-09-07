//! End-to-end transfers over loopback: the real TLS handshake, the real framing,
//! the real disk pipeline. These are the tests that would catch a protocol bug
//! that every unit test passes.

use std::path::PathBuf;

use aetherlink_core::{recv, send, Config, Error, HostIdentity, OutgoingFile};

fn test_config() -> Config {
    Config {
        stream_count: 4,
        frame_size: 16 * 1024,
        // 64 KiB chunks keep these tests fast while still exercising the
        // multi-chunk, multi-frame paths.
        chunk_size: 64 * 1024,
        ..Config::default()
    }
}

#[derive(Debug)]
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "aetherlink-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// Deterministic pseudo-random bytes, so a failure is reproducible.
fn payload(len: usize, seed: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed as u32 | 1;
    for _ in 0..len {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        out.push((x >> 24) as u8);
    }
    out
}

async fn transfer(
    files: Vec<(&str, Vec<u8>)>,
    config: Config,
) -> Result<(TempDir, Vec<(String, Vec<u8>)>), Error> {
    let src_dir = TempDir::new("src");
    let dst_dir = TempDir::new("dst");

    let mut outgoing = Vec::new();
    for (name, bytes) in &files {
        let path = src_dir.join(&name.replace('/', "_"));
        std::fs::write(&path, bytes).unwrap();
        outgoing.push(OutgoingFile {
            path,
            relative_path: name.to_string(),
            mime_type: "application/octet-stream".into(),
        });
    }

    let identity = HostIdentity::generate().unwrap();
    let fingerprint = identity.fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let out_path = dst_dir.0.clone();
    let server =
        tokio::spawn(async move { recv::receive(&listener, &identity, &out_path, &config).await });

    let send_result = send::send(&addr, fingerprint, outgoing, &config).await;
    let recv_result = server.await.unwrap();

    // Surface whichever side failed; the sender's error is usually the clearer one.
    send_result?;
    recv_result?;

    let received = files
        .iter()
        .map(|(name, _)| {
            (
                name.to_string(),
                std::fs::read(dst_dir.join(name)).unwrap_or_default(),
            )
        })
        .collect();
    Ok((dst_dir, received))
}

#[tokio::test]
async fn transfers_a_multi_chunk_file_byte_for_byte() {
    // 10 chunks and change, so the short final chunk is exercised.
    let data = payload(64 * 1024 * 10 + 777, 7);
    let (_dst, got) = transfer(vec![("video.mp4", data.clone())], test_config())
        .await
        .unwrap();
    assert_eq!(got[0].1.len(), data.len());
    assert_eq!(
        got[0].1, data,
        "received bytes must match the source exactly"
    );
}

#[tokio::test]
async fn transfers_many_files_preserving_directory_structure() {
    let files = vec![
        ("DCIM/Camera/a.jpg", payload(64 * 1024 * 3, 1)),
        ("DCIM/Camera/b.jpg", payload(1234, 2)),
        ("Documents/notes.txt", payload(64 * 1024 + 1, 3)),
    ];
    let (dst, got) = transfer(files.clone(), test_config()).await.unwrap();
    for ((name, expected), (_, actual)) in files.iter().zip(got.iter()) {
        assert_eq!(actual, expected, "{name} did not round-trip");
        assert!(
            dst.join(name).exists(),
            "{name} must land at its relative path"
        );
    }
}

#[tokio::test]
async fn transfers_an_empty_file() {
    let (_dst, got) = transfer(vec![("empty.bin", Vec::new())], test_config())
        .await
        .unwrap();
    assert!(got[0].1.is_empty());
}

#[tokio::test]
async fn single_stream_works_as_well_as_eight() {
    let data = payload(64 * 1024 * 5, 9);
    let config = Config {
        stream_count: 1,
        ..test_config()
    };
    let (_dst, got) = transfer(vec![("solo.bin", data.clone())], config)
        .await
        .unwrap();
    assert_eq!(got[0].1, data);
}

#[tokio::test]
async fn more_streams_than_chunks_still_completes() {
    // 16 streams, 2 chunks: most workers find an empty queue and exit cleanly.
    let data = payload(64 * 1024 * 2, 11);
    let config = Config {
        stream_count: 16,
        ..test_config()
    };
    let (_dst, got) = transfer(vec![("small.bin", data.clone())], config)
        .await
        .unwrap();
    assert_eq!(got[0].1, data);
}

#[tokio::test]
async fn a_wrong_fingerprint_is_refused() {
    let src_dir = TempDir::new("pin-src");
    let dst_dir = TempDir::new("pin-dst");
    let path = src_dir.join("f.bin");
    std::fs::write(&path, payload(4096, 5)).unwrap();

    let identity = HostIdentity::generate().unwrap();
    // A different host's fingerprint: exactly the on-path attacker the pin exists for.
    let attacker = HostIdentity::generate().unwrap().fingerprint();
    assert_ne!(identity.fingerprint(), attacker);

    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = test_config();
    let out = dst_dir.0.clone();
    let server =
        tokio::spawn(async move { recv::receive(&listener, &identity, &out, &config).await });

    let result = send::send(
        &addr,
        attacker,
        vec![OutgoingFile {
            path,
            relative_path: "f.bin".into(),
            mime_type: "application/octet-stream".into(),
        }],
        &config,
    )
    .await;

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
    server.abort();
}

#[tokio::test]
async fn a_traversal_path_in_the_manifest_is_rejected() {
    let result = transfer(vec![("../../etc/passwd", payload(1024, 4))], test_config()).await;
    match result {
        Err(Error::Wire(aetherlink_proto::Error::UnsafePath { .. }))
        | Err(Error::PeerAborted(_)) => {}
        other => panic!("traversal must be refused, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn releasing_pages_behind_the_cursor_does_not_corrupt_the_stream() {
    // The failure this guards against is a race: one stream releasing pages
    // that a slower stream is still reading. It needs many chunks spread
    // across many streams to surface, so this is deliberately larger and
    // wider than the other loopback tests.
    let data = payload(64 * 1024 * 200 + 4321, 31);
    let config = Config {
        stream_count: 8,
        chunk_size: 64 * 1024,
        frame_size: 16 * 1024,
        release_read_pages: true,
        ..Config::default()
    };

    let (_dst, got) = transfer(vec![("big.bin", data.clone())], config)
        .await
        .unwrap();
    assert_eq!(got[0].1.len(), data.len());
    assert_eq!(
        blake3::hash(&got[0].1),
        blake3::hash(&data),
        "page release corrupted the transfer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn releasing_pages_changes_nothing_observable() {
    // Same payload with the release turned off: the bytes on the wire must be
    // identical either way. It is a memory hint, not a protocol change.
    let data = payload(64 * 1024 * 40 + 99, 32);
    let base = Config {
        stream_count: 4,
        chunk_size: 64 * 1024,
        frame_size: 16 * 1024,
        ..Config::default()
    };

    let (_a, with) = transfer(
        vec![("f.bin", data.clone())],
        Config {
            release_read_pages: true,
            ..base
        },
    )
    .await
    .unwrap();
    let (_b, without) = transfer(
        vec![("f.bin", data.clone())],
        Config {
            release_read_pages: false,
            ..base
        },
    )
    .await
    .unwrap();

    assert_eq!(blake3::hash(&with[0].1), blake3::hash(&data));
    assert_eq!(blake3::hash(&without[0].1), blake3::hash(&with[0].1));
}
