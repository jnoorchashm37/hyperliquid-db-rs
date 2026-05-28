// #![cfg(target_os = "linux")]

mod current_impl;
mod existing_fs_reader;

use std::{
    path::Path,
    process,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant}
};

use hyperliquid_db::fs_watchers::{directory::OutData, types::HyperliquidDataDirKind};

const BENCHMARK_DIRECTORY: &str = "/var/lib/hyperliquid/hl/data/node_slow_block_times";
const BENCHMARK_RUNS: usize = 10;
const BENCHMARK_CHUNKS: usize = 100;
const BENCHMARK_CHUNK_BYTES: usize = 1_024;
const BENCHMARK_READY_MS: u64 = 100;
const BENCHMARK_RECV_TIMEOUT_MS: u64 = 1_000_000;

type SpawnReader = fn(HyperliquidDataDirKind, &Path) -> eyre::Result<mpsc::Receiver<OutData>>;

pub fn run_fs_readers_bench() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        process::exit(1);
    }
}

fn run() -> eyre::Result<()> {
    let config = BenchConfig::default();
    let directory = Path::new(BENCHMARK_DIRECTORY);
    if !directory.is_dir() {
        return Err(eyre::eyre!(
            "BENCHMARK_DIRECTORY does not exist or is not a directory: {}",
            directory.display()
        ));
    }

    println!("fs_watching benchmark");
    println!("directory: {}", directory.display());
    println!("runs: {}", config.runs);
    println!("target chunks/run: {}", config.chunks);
    println!("target chunk bytes: {}", config.chunk_bytes);
    println!("target bytes/run: {}", config.bytes_per_run());
    println!("ready delay: {:.3} ms", ms(config.ready_delay));
    println!("recv timeout/run: {:.3} ms", ms(config.recv_timeout));
    println!();

    let collectors = vec![
        spawn_reader_collector("current_impl", current_impl::spawn_file_reader, directory)?,
        spawn_reader_collector(
            "existing_fs_reader",
            existing_fs_reader::spawn_file_reader,
            directory
        )?,
    ];
    thread::sleep(config.ready_delay);

    let reports = benchmark_collectors(collectors, &config)?;
    print_report(&reports);

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
    fn bytes_per_run(self) -> usize {
        self.chunks * self.chunk_bytes
    }
}

impl Default for BenchConfig {
    fn default() -> Self {
        let runs = BENCHMARK_RUNS;
        let chunks = BENCHMARK_CHUNKS;
        let chunk_bytes = BENCHMARK_CHUNK_BYTES;
        let ready_delay = Duration::from_millis(BENCHMARK_READY_MS);
        let recv_timeout = Duration::from_millis(BENCHMARK_RECV_TIMEOUT_MS);
        Self { runs, chunks, chunk_bytes, ready_delay, recv_timeout }
    }
}

struct ReaderCollector {
    name:       &'static str,
    command_tx: mpsc::Sender<CollectorCommand>,
    result_rx:  mpsc::Receiver<Result<Sample, String>>
}

enum CollectorCommand {
    Sample { target_bytes: usize, timeout: Duration, barrier: Arc<Barrier> },
    Stop
}

struct ReaderReport {
    name:                 &'static str,
    target_bytes_per_run: usize,
    samples:              Vec<Sample>
}

#[derive(Clone, Copy)]
struct Sample {
    total:    Duration,
    bytes:    usize,
    messages: usize
}

fn spawn_reader_collector(
    name: &'static str,
    spawn_reader: SpawnReader,
    directory: &Path
) -> eyre::Result<ReaderCollector> {
    let rx = spawn_reader(HyperliquidDataDirKind::ReplicaCmds, directory)?;
    let (command_tx, command_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();

    thread::spawn(move || {
        while let Ok(command) = command_rx.recv() {
            match command {
                CollectorCommand::Sample { target_bytes, timeout, barrier } => {
                    drain_stale_messages(&rx);
                    barrier.wait();

                    let result = recv_target_bytes(&rx, target_bytes, timeout)
                        .map_err(|err| format!("{err:?}"));
                    if result_tx.send(result).is_err() {
                        break;
                    }
                }
                CollectorCommand::Stop => break
            }
        }
    });

    Ok(ReaderCollector { name, command_tx, result_rx })
}

fn benchmark_collectors(
    collectors: Vec<ReaderCollector>,
    config: &BenchConfig
) -> eyre::Result<Vec<ReaderReport>> {
    let mut reports: Vec<ReaderReport> = collectors
        .iter()
        .map(|collector| ReaderReport {
            name:                 collector.name,
            target_bytes_per_run: config.bytes_per_run(),
            samples:              Vec::with_capacity(config.runs)
        })
        .collect();

    for run_idx in 0..config.runs {
        let barrier = Arc::new(Barrier::new(collectors.len() + 1));
        for collector in &collectors {
            collector
                .command_tx
                .send(CollectorCommand::Sample {
                    target_bytes: config.bytes_per_run(),
                    timeout:      config.recv_timeout,
                    barrier:      barrier.clone()
                })
                .map_err(|_| eyre::eyre!("{} collector stopped", collector.name))?;
        }

        barrier.wait();

        for (collector, report) in collectors.iter().zip(reports.iter_mut()) {
            let sample = collector
                .result_rx
                .recv()
                .map_err(|_| eyre::eyre!("{} collector did not return a sample", collector.name))?
                .map_err(|err| eyre::eyre!("{} run {run_idx} failed: {err}", collector.name))?;
            report.samples.push(sample);
        }
    }

    for collector in &collectors {
        let _ = collector.command_tx.send(CollectorCommand::Stop);
    }

    Ok(reports)
}

fn recv_target_bytes(
    rx: &mpsc::Receiver<OutData>,
    target_bytes: usize,
    timeout: Duration
) -> eyre::Result<Sample> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut received_bytes = 0;
    let mut messages = 0;

    while received_bytes < target_bytes {
        let now = Instant::now();
        if now >= deadline {
            return Err(eyre::eyre!(
                "timed out after receiving {received_bytes}/{target_bytes} bytes"
            ));
        }

        let chunk = match rx.recv_timeout(deadline - now) {
            Ok(chunk) => chunk,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(eyre::eyre!(
                    "timed out after receiving {received_bytes}/{target_bytes} bytes"
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(eyre::eyre!(
                    "reader channel disconnected after receiving {received_bytes}/{target_bytes} \
                     bytes"
                ));
            }
        };

        received_bytes += chunk.bytes.len();
        messages += 1;
    }

    Ok(Sample { total: started.elapsed(), bytes: received_bytes, messages })
}

fn drain_stale_messages(rx: &mpsc::Receiver<OutData>) {
    while rx.try_recv().is_ok() {}
}

fn print_report(reports: &[ReaderReport]) {
    println!(
        "{:<22} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "reader", "avg ms", "median ms", "min ms", "p95 ms", "MiB/s", "avg MiB"
    );

    for report in reports {
        let summary = Summary::new(report);
        println!(
            "{:<22} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>12.2} {:>12.3}",
            report.name,
            ms(summary.avg_total),
            ms(summary.median_total),
            ms(summary.min_total),
            ms(summary.p95_total),
            summary.throughput_mib_s,
            bytes_to_mib(summary.avg_bytes as usize),
        );
    }

    println!();
    println!("{:<22} {:>12} {:>12}", "reader", "target MiB", "avg messages");
    for report in reports {
        let summary = Summary::new(report);
        println!(
            "{:<22} {:>12.3} {:>12.1}",
            report.name,
            bytes_to_mib(report.target_bytes_per_run),
            summary.avg_messages,
        );
    }
}

struct Summary {
    avg_total:        Duration,
    median_total:     Duration,
    min_total:        Duration,
    p95_total:        Duration,
    avg_bytes:        f64,
    avg_messages:     f64,
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
        let avg_bytes = report
            .samples
            .iter()
            .map(|sample| sample.bytes as f64)
            .sum::<f64>()
            / report.samples.len() as f64;
        let avg_messages = report
            .samples
            .iter()
            .map(|sample| sample.messages as f64)
            .sum::<f64>()
            / report.samples.len() as f64;
        let throughput_mib_s = bytes_to_mib(avg_bytes as usize) / avg_total.as_secs_f64();

        Self {
            avg_total,
            median_total,
            min_total,
            p95_total,
            avg_bytes,
            avg_messages,
            throughput_mib_s
        }
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
