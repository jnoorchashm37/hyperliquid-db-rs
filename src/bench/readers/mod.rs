mod current_impl;
mod existing_fs_reader;
use std::{
    env,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process,
    sync::mpsc,
    thread,
    time::{Duration, Instant}
};

use hyperliquid_db::fs_watchers::{directory::OutData, types::HyperliquidDataDirKind};

const BENCHMARK_DIRECTORY: &str = "/var/lib/hyperliquid/hl/data/replica_cmds";

type SpawnReader = fn(HyperliquidDataDirKind, &Path) -> eyre::Result<mpsc::Receiver<OutData>>;

pub fn run_fs_readers_bench() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        process::exit(1);
    }
}

fn run() -> eyre::Result<()> {
    let config = BenchConfig::from_env()?;

    println!("fs_watching benchmark");
    println!("runs: {}", config.runs);
    println!("chunks/run: {}", config.chunks);
    println!("chunk bytes: {}", config.chunk_bytes);
    println!("bytes/run: {}", config.bytes_per_run());
    println!("ready delay: {:.3} ms", ms(config.ready_delay));
    println!("recv timeout/run: {:.3} ms", ms(config.recv_timeout));
    println!();

    let current = benchmark_reader("current_impl", current_impl::spawn_file_reader, &config)?;
    let existing =
        benchmark_reader("existing_fs_reader", existing_fs_reader::spawn_file_reader, &config)?;

    print_report(&[current, existing]);

    Ok(())
}

#[derive(Clone, Copy)]
struct BenchConfig {
    runs:         usize,
    chunks:       usize,
    chunk_bytes:  usize,
    ready_delay:  Duration,
    recv_timeout: Duration
}

impl BenchConfig {
    fn from_env() -> eyre::Result<Self> {
        let runs = env_usize("FS_WATCH_BENCH_RUNS", 10)?;
        let chunks = env_usize("FS_WATCH_BENCH_CHUNKS", 1_000)?;
        let chunk_bytes = env_usize("FS_WATCH_BENCH_CHUNK_BYTES", 1_024)?;
        let ready_delay = Duration::from_millis(env_u64("FS_WATCH_BENCH_READY_MS", 100)?);
        let recv_timeout =
            Duration::from_millis(env_u64("FS_WATCH_BENCH_RECV_TIMEOUT_MS", 10_000)?);

        if runs == 0 {
            return Err(eyre::eyre!("FS_WATCH_BENCH_RUNS must be greater than 0"));
        }
        if chunks == 0 {
            return Err(eyre::eyre!("FS_WATCH_BENCH_CHUNKS must be greater than 0"));
        }
        if chunk_bytes == 0 {
            return Err(eyre::eyre!("FS_WATCH_BENCH_CHUNK_BYTES must be greater than 0"));
        }

        Ok(Self { runs, chunks, chunk_bytes, ready_delay, recv_timeout })
    }

    fn bytes_per_run(self) -> usize {
        self.chunks * self.chunk_bytes
    }
}

struct ReaderReport {
    name:          String,
    bytes_per_run: usize,
    samples:       Vec<Sample>
}

struct Sample {
    total:    Duration,
    write:    Duration,
    messages: usize
}

fn benchmark_reader(
    name: &str,
    spawn_reader: SpawnReader,
    config: &BenchConfig
) -> eyre::Result<ReaderReport> {
    let temp_dir = tempfile::Builder::new().prefix(name).tempdir()?;
    let rx = spawn_reader(HyperliquidDataDirKind::ReplicaCmds, temp_dir.path())?;
    thread::sleep(config.ready_delay);

    let mut samples = Vec::with_capacity(config.runs);
    for run_idx in 0..config.runs {
        drain_stale_messages(&rx);

        let path = temp_dir.path().join(format!("sample-{run_idx:04}.log"));
        let started = Instant::now();
        let write = write_sample_file(&path, run_idx, config)?;
        let messages = recv_expected_bytes(&rx, config.bytes_per_run(), config.recv_timeout)
            .map_err(|err| eyre::eyre!("{name} run {run_idx} failed: {err}"))?;
        let total = started.elapsed();

        samples.push(Sample { total, write, messages });
    }

    Ok(ReaderReport { name: name.to_owned(), bytes_per_run: config.bytes_per_run(), samples })
}

fn write_sample_file(
    path: &PathBuf,
    run_idx: usize,
    config: &BenchConfig
) -> eyre::Result<Duration> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let started = Instant::now();

    for chunk_idx in 0..config.chunks {
        let chunk = make_chunk(run_idx, chunk_idx, config.chunk_bytes);
        file.write_all(&chunk)?;
        file.flush()?;
    }

    Ok(started.elapsed())
}

fn make_chunk(run_idx: usize, chunk_idx: usize, chunk_bytes: usize) -> Vec<u8> {
    let mut chunk = vec![b'x'; chunk_bytes];
    let prefix = format!("{run_idx:08}:{chunk_idx:08}:");
    let prefix = prefix.as_bytes();
    let prefix_len = prefix.len().min(chunk_bytes);
    chunk[..prefix_len].copy_from_slice(&prefix[..prefix_len]);
    chunk[chunk_bytes - 1] = b'\n';
    chunk
}

fn recv_expected_bytes(
    rx: &mpsc::Receiver<OutData>,
    expected_bytes: usize,
    timeout: Duration
) -> eyre::Result<usize> {
    let deadline = Instant::now() + timeout;
    let mut received_bytes = 0;
    let mut messages = 0;

    while received_bytes < expected_bytes {
        let now = Instant::now();
        if now >= deadline {
            return Err(eyre::eyre!(
                "timed out after receiving {received_bytes}/{expected_bytes} bytes"
            ));
        }

        let chunk = match rx.recv_timeout(deadline - now) {
            Ok(chunk) => chunk,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(eyre::eyre!(
                    "timed out after receiving {received_bytes}/{expected_bytes} bytes"
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(eyre::eyre!(
                    "reader channel disconnected after receiving \
                     {received_bytes}/{expected_bytes} bytes"
                ));
            }
        };

        received_bytes += chunk.bytes.len();
        messages += 1;
    }

    if received_bytes != expected_bytes {
        return Err(eyre::eyre!(
            "received {received_bytes} bytes, expected exactly {expected_bytes}"
        ));
    }

    Ok(messages)
}

fn drain_stale_messages(rx: &mpsc::Receiver<OutData>) {
    while rx.try_recv().is_ok() {}
}

fn print_report(reports: &[ReaderReport]) {
    println!(
        "{:<22} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "reader", "avg ms", "median ms", "min ms", "p95 ms", "lag ms", "MiB/s"
    );

    for report in reports {
        let summary = Summary::new(report);
        println!(
            "{:<22} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>12.2}",
            report.name,
            ms(summary.avg_total),
            ms(summary.median_total),
            ms(summary.min_total),
            ms(summary.p95_total),
            ms(summary.avg_lag),
            summary.throughput_mib_s,
        );
    }

    println!();
    println!("{:<22} {:>12}", "reader", "avg messages");
    for report in reports {
        let avg_messages = report
            .samples
            .iter()
            .map(|sample| sample.messages as f64)
            .sum::<f64>()
            / report.samples.len() as f64;
        println!("{:<22} {:>12.1}", report.name, avg_messages);
    }
}

struct Summary {
    avg_total:        Duration,
    median_total:     Duration,
    min_total:        Duration,
    p95_total:        Duration,
    avg_lag:          Duration,
    throughput_mib_s: f64
}

impl Summary {
    fn new(report: &ReaderReport) -> Self {
        let mut totals: Vec<Duration> = report.samples.iter().map(|sample| sample.total).collect();
        totals.sort_unstable();

        let avg_total = average_duration(totals.iter().copied());
        let median_total = percentile(&totals, 0.50);
        let min_total = totals[0];
        let p95_total = percentile(&totals, 0.95);
        let avg_lag = average_duration(
            report
                .samples
                .iter()
                .map(|sample| sample.total.saturating_sub(sample.write))
        );
        let throughput_mib_s = bytes_to_mib(report.bytes_per_run) / avg_total.as_secs_f64();

        Self { avg_total, median_total, min_total, p95_total, avg_lag, throughput_mib_s }
    }
}

fn average_duration(samples: impl Iterator<Item = Duration>) -> Duration {
    let mut total_nanos = 0_u128;
    let mut len = 0_u128;

    for sample in samples {
        total_nanos += sample.as_nanos();
        len += 1;
    }

    Duration::from_nanos((total_nanos / len) as u64)
}

fn percentile(samples: &[Duration], percentile: f64) -> Duration {
    let idx = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
    samples[idx]
}

fn bytes_to_mib(bytes: usize) -> f64 {
    bytes as f64 / 1_048_576.0
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn env_usize(name: &str, default: usize) -> eyre::Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|err| eyre::eyre!("invalid {name}={value:?}: {err}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err.into())
    }
}

fn env_u64(name: &str, default: u64) -> eyre::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|err| eyre::eyre!("invalid {name}={value:?}: {err}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err.into())
    }
}
