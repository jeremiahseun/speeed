//! Transport role is fixed by the topology; data direction is not.
//!
//! iOS can join a Wi-Fi network but never create one, so Android always hosts
//! and always owns the known address — it is the TCP server whichever way the
//! bytes flow. These tests cover both directions over that one fixed topology.

use std::path::PathBuf;

use aetherlink_core::{recv, send, Config, HostIdentity, OutgoingFile};

fn config() -> Config {
    Config {
        stream_count: 4,
        frame_size: 16 * 1024,
        chunk_size: 64 * 1024,
        ..Config::default()
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "aetherlink-roles-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn payload(len: usize, seed: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed as u32 | 1;
    for _ in 0..len {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        out.push((x >> 24) as u8);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_network_host_can_be_the_sender() {
    // Android hosts the group and sends photos; the iPhone joins and receives.
    // This is the common direction, and the one a client-dials-and-sends
    // design cannot express at all.
    let src_dir = TempDir::new("host-send-src");
    let dst_dir = TempDir::new("host-send-dst");
    let data = payload(64 * 1024 * 5 + 313, 21);
    let src = src_dir.0.join("photo.heic");
    std::fs::write(&src, &data).unwrap();

    let identity = HostIdentity::generate().unwrap();
    let fingerprint = identity.fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let cfg = config();
    let files = vec![OutgoingFile {
        path: src.clone(),
        relative_path: "photo.heic".into(),
        mime_type: "image/heic".into(),
    }];
    // The host accepts and transmits.
    let host =
        tokio::spawn(async move { send::send_as_host(&listener, &identity, files, &cfg).await });

    // The joining device dials in to be sent to.
    let received = recv::receive_as_client(&addr, fingerprint, &dst_dir.0, &cfg).await;

    received.expect("client should receive");
    host.await.unwrap().expect("host should send");

    assert_eq!(
        std::fs::read(dst_dir.0.join("photo.heic")).unwrap(),
        data,
        "the file must arrive intact in the host-sends direction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_joining_device_can_be_the_sender() {
    // The other direction, over the same topology: the iPhone joins and sends,
    // Android hosts and receives.
    let src_dir = TempDir::new("client-send-src");
    let dst_dir = TempDir::new("client-send-dst");
    let data = payload(64 * 1024 * 3, 42);
    let src = src_dir.0.join("clip.mp4");
    std::fs::write(&src, &data).unwrap();

    let identity = HostIdentity::generate().unwrap();
    let fingerprint = identity.fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let cfg = config();
    let out = dst_dir.0.clone();
    let host = tokio::spawn(async move { recv::receive(&listener, &identity, &out, &cfg).await });

    let files = vec![OutgoingFile {
        path: src.clone(),
        relative_path: "clip.mp4".into(),
        mime_type: "video/mp4".into(),
    }];
    send::send(&addr, fingerprint, files, &cfg)
        .await
        .expect("client should send");
    host.await.unwrap().expect("host should receive");

    assert_eq!(std::fs::read(dst_dir.0.join("clip.mp4")).unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinning_still_holds_when_the_host_is_the_sender() {
    // The pin authenticates the network host, which is the TLS server in both
    // directions. Reversing the data flow must not weaken that.
    let src_dir = TempDir::new("pin-src");
    let dst_dir = TempDir::new("pin-dst");
    let src = src_dir.0.join("f.bin");
    std::fs::write(&src, payload(4096, 3)).unwrap();

    let identity = HostIdentity::generate().unwrap();
    let attacker = HostIdentity::generate().unwrap().fingerprint();
    let listener = recv::bind(0).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let cfg = config();
    let files = vec![OutgoingFile {
        path: src,
        relative_path: "f.bin".into(),
        mime_type: "application/octet-stream".into(),
    }];
    let host =
        tokio::spawn(async move { send::send_as_host(&listener, &identity, files, &cfg).await });

    let result = recv::receive_as_client(&addr, attacker, &dst_dir.0, &cfg).await;
    assert!(
        matches!(result, Err(aetherlink_core::Error::Tls(_))),
        "a wrong fingerprint must fail the handshake, got {result:?}"
    );
    host.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_works_in_the_host_sends_direction_too() {
    let src_dir = TempDir::new("hs-resume-src");
    let dst_dir = TempDir::new("hs-resume-dst");
    let data = payload(64 * 1024 * 6, 77);
    let src = src_dir.0.join("big.bin");
    std::fs::write(&src, &data).unwrap();

    let run = || {
        let src = src.clone();
        let out = dst_dir.0.clone();
        async move {
            let identity = HostIdentity::generate().unwrap();
            let fingerprint = identity.fingerprint();
            let listener = recv::bind(0).await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let cfg = config();
            let files = vec![OutgoingFile {
                path: src,
                relative_path: "big.bin".into(),
                mime_type: "application/octet-stream".into(),
            }];
            let host =
                tokio::spawn(
                    async move { send::send_as_host(&listener, &identity, files, &cfg).await },
                );
            let received = recv::receive_as_client(&addr, fingerprint, &out, &cfg).await;
            let sent = host.await.unwrap();
            (sent, received)
        }
    };

    let (sent, received) = run().await;
    sent.unwrap();
    received.unwrap();

    // Damage a chunk, then repeat: the receiver rebuilds by verification and
    // asks for only what is missing, regardless of transport role.
    let dest = dst_dir.0.join("big.bin");
    let mut on_disk = std::fs::read(&dest).unwrap();
    on_disk[64 * 1024..64 * 1024 * 2].fill(0);
    std::fs::write(&dest, &on_disk).unwrap();

    let (sent, received) = run().await;
    sent.unwrap();
    received.unwrap();
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        data,
        "the repaired file must match"
    );
}
