//! Test harness for `aetherlink-core`.
//!
//! `bench` is the one that matters: it settles the software-ceiling gate in
//! PRD §2.4. If the engine can move an order of magnitude more than the radio,
//! then Tier A–D results are the hardware's limit and not ours.

use std::path::PathBuf;
use std::time::Instant;

use aetherlink_core::{recv, send, Config, HostIdentity, OutgoingFile, TransferStats};
use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "aetherlink", about = "AetherLink transfer engine harness")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Host the link and receive. Prints the fingerprint the peer must pin.
    Recv {
        #[arg(long, default_value_t = 52080)]
        port: u16,
        #[arg(long, default_value = "./received")]
        out: PathBuf,
        #[command(flatten)]
        tuning: Tuning,
    },
    /// Join a host's link and send to it.
    Send {
        addr: String,
        /// Fingerprint printed by `recv`, as 64 hex characters.
        #[arg(long)]
        fingerprint: String,
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[command(flatten)]
        tuning: Tuning,
    },
    /// Host the link and send. The peer dials in to be sent to — this is the
    /// Android-to-iOS direction, where the host owns the known address but is
    /// the one transmitting.
    HostSend {
        #[arg(long, default_value_t = 52080)]
        port: u16,
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[command(flatten)]
        tuning: Tuning,
    },
    /// Join a host's link and receive what it sends.
    ClientRecv {
        addr: String,
        #[arg(long)]
        fingerprint: String,
        #[arg(long, default_value = "./received")]
        out: PathBuf,
        #[command(flatten)]
        tuning: Tuning,
    },
    /// Loopback benchmark: both roles in one process.
    Bench {
        /// Payload size in MiB.
        #[arg(long, default_value_t = 1024)]
        size_mib: u64,
        /// Working directory for the generated and received files.
        #[arg(long, default_value = "/dev/shm/aetherlink-bench")]
        dir: PathBuf,
        /// Repeat count; the best run is reported alongside the mean.
        #[arg(long, default_value_t = 3)]
        runs: u32,
        #[command(flatten)]
        tuning: Tuning,
    },
}

#[derive(Args, Clone)]
struct Tuning {
    #[arg(long, default_value_t = 8)]
    streams: u8,
    #[arg(long, default_value_t = 256 * 1024)]
    frame_bytes: u32,
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_bytes: u32,
    /// Keep the sender's pages mapped after transmitting them. For measuring
    /// what releasing them is worth; not a setting to ship with.
    #[arg(long)]
    keep_read_pages: bool,
}

impl Tuning {
    fn config(&self) -> Config {
        Config {
            stream_count: self.streams,
            frame_size: self.frame_bytes,
            chunk_size: self.chunk_bytes,
            release_read_pages: !self.keep_read_pages,
            ..Config::default()
        }
    }
}

fn report(label: &str, stats: &TransferStats) {
    println!(
        "{label:<10} {:>8.2} MB/s  ({:>7.0} Mbps)  {:>8.3} s  {:>6.2} GB",
        stats.megabytes_per_second(),
        stats.megabits_per_second(),
        stats.elapsed.as_secs_f64(),
        stats.bytes as f64 / 1e9,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Recv { port, out, tuning } => {
            let identity = HostIdentity::generate()?;
            let listener = recv::bind(port).await?;
            println!("listening on {}", listener.local_addr()?);
            println!("fingerprint  {}", identity.fingerprint_hex());
            let stats = recv::receive(&listener, &identity, &out, &tuning.config()).await?;
            report("received", &stats);
        }
        Command::Send {
            addr,
            fingerprint,
            files,
            tuning,
        } => {
            let pinned = parse_fingerprint(&fingerprint)?;
            let stats = send::send(&addr, pinned, to_outgoing(files), &tuning.config()).await?;
            report("sent", &stats);
        }
        Command::HostSend {
            port,
            files,
            tuning,
        } => {
            let identity = HostIdentity::generate()?;
            let listener = recv::bind(port).await?;
            println!("listening on {}", listener.local_addr()?);
            println!("fingerprint  {}", identity.fingerprint_hex());
            let stats =
                send::send_as_host(&listener, &identity, to_outgoing(files), &tuning.config())
                    .await?;
            report("sent", &stats);
        }
        Command::ClientRecv {
            addr,
            fingerprint,
            out,
            tuning,
        } => {
            let pinned = parse_fingerprint(&fingerprint)?;
            let stats = recv::receive_as_client(&addr, pinned, &out, &tuning.config()).await?;
            report("received", &stats);
        }
        Command::Bench {
            size_mib,
            dir,
            runs,
            tuning,
        } => bench(size_mib, &dir, runs, tuning.config()).await?,
    }
    Ok(())
}

fn to_outgoing(files: Vec<PathBuf>) -> Vec<OutgoingFile> {
    files
        .into_iter()
        .map(|path| {
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "file.bin".into());
            OutgoingFile {
                path,
                relative_path: name,
                mime_type: "application/octet-stream".into(),
            }
        })
        .collect()
}

fn parse_fingerprint(hex: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(hex.len() == 64, "fingerprint must be 64 hex characters");
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("invalid hex at position {}", i * 2))?;
    }
    Ok(out)
}

async fn bench(size_mib: u64, dir: &PathBuf, runs: u32, config: Config) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let src = dir.join("bench-source.bin");
    let out_dir = dir.join("out");
    let bytes = size_mib * 1024 * 1024;

    if std::fs::metadata(&src).map(|m| m.len()).ok() != Some(bytes) {
        println!("generating {size_mib} MiB source at {}", src.display());
        let t = Instant::now();
        write_source(&src, bytes)?;
        println!("  generated in {:.2}s", t.elapsed().as_secs_f64());
    }

    println!(
        "\nstreams={}  frame={} KiB  chunk={} MiB  cores={}\n",
        config.stream_count,
        config.frame_size / 1024,
        config.chunk_size / (1024 * 1024),
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    );

    let mut results = Vec::new();
    for run in 1..=runs {
        std::fs::remove_dir_all(&out_dir).ok();
        let identity = HostIdentity::generate()?;
        let fingerprint = identity.fingerprint();
        let listener = recv::bind(0).await?;
        let addr = listener.local_addr()?.to_string();

        let out = out_dir.clone();
        let cfg = config;
        let server =
            tokio::spawn(async move { recv::receive(&listener, &identity, &out, &cfg).await });

        let outgoing = vec![OutgoingFile {
            path: src.clone(),
            relative_path: "bench.bin".into(),
            mime_type: "application/octet-stream".into(),
        }];
        send::send(&addr, fingerprint, outgoing, &config).await?;
        let stats = server.await??;

        report(&format!("run {run}"), &stats);
        results.push(stats.megabytes_per_second());
    }

    let best = results.iter().cloned().fold(f64::MIN, f64::max);
    let mean = results.iter().sum::<f64>() / results.len() as f64;
    println!(
        "\nbest {best:.2} MB/s   mean {mean:.2} MB/s   ({:.0} Mbps peak)",
        best * 8.0
    );

    std::fs::remove_dir_all(&out_dir).ok();
    Ok(())
}

/// Incompressible pseudo-random payload, so nothing downstream can cheat by
/// compressing it.
fn write_source(path: &PathBuf, bytes: u64) -> Result<()> {
    use std::io::Write;
    let file = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::with_capacity(1 << 20, file);
    let mut block = vec![0u8; 1 << 20];
    let mut x: u64 = 0x243F_6A88_85A3_08D3;
    let mut written = 0u64;
    while written < bytes {
        for c in block.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            c.copy_from_slice(&x.to_le_bytes()[..c.len()]);
        }
        let n = block.len().min((bytes - written) as usize);
        w.write_all(&block[..n])?;
        written += n as u64;
    }
    w.flush()?;
    Ok(())
}
