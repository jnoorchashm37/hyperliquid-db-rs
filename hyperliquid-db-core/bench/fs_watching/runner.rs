mod current_impl;
mod existing_fs_reader;

use std::{
    path::Path,
    process,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant}
};

use eyre::WrapErr;
use hyperliquid_db_core::{
    HYPERLIQUID_DATA_DIR,
    fs_handlers::types::FsOutData,
    hl_fs::{
        HyperliquidDirData, HyperliquidDirKind,
        parsers::{HyperliquidDataParser, NodeFillsParser}
    },
    processors::{HyperliquidDataProcessorHandle, TradeDeriver},
    types::HyperliquidData,
    utils::{NS_PER_MS, NS_PER_SEC, unix_timestamp}
};

const BENCHMARK_RUNS: usize = 10;
const BENCHMARK_CHUNKS: usize = 100;
const BENCHMARK_CHUNK_BYTES: usize = 256;
const BENCHMARK_READY_MS: u64 = 100;
const BENCHMARK_RECV_TIMEOUT_MS: u64 = 100_000;

type SpawnReader =
    fn(HyperliquidDirKind, &Path) -> eyre::Result<mpsc::Receiver<eyre::Result<FsOutData>>>;

pub fn run_fs_readers_bench() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        process::exit(1);
    }
}

fn run() -> eyre::Result<()> {
    let config = BenchConfig::default();
    let directory = Path::new(HYPERLIQUID_DATA_DIR);
    if !directory.is_dir() {
        return Err(eyre::eyre!(
            "BENCHMARK_DIRECTORY does not exist or is not a directory: {}",
            directory.display()
        ));
    }

    println!("fs_watching benchmark");
    println!("directory: {}", directory.display());
    println!("runs: {}", config.runs);
    println!("target nominal chunks/run: {}", config.chunks);
    println!("target nominal chunk bytes: {}", config.chunk_bytes);
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

struct Sample {
    total:  Duration,
    bytes:  usize,
    chunks: usize,
    stages: StageSamples
}

#[derive(Default)]
struct StageSamples {
    trade_time_to_row_local:       Vec<f64>,
    block_time_to_row_local:       Vec<f64>,
    row_local_to_notification:     Vec<f64>,
    notification_to_drain_file:    Vec<f64>,
    drain_file:                    Vec<f64>,
    drain_file_to_drain_new_bytes: Vec<f64>,
    drain_new_bytes:               Vec<f64>,
    notification_to_file_bytes:    Vec<f64>,
    file_bytes_to_parsed_row:      Vec<f64>,
    parsed_row_to_derived_trade:   Vec<f64>,
    channel_send_to_receive:       Vec<f64>,
    notification_to_channel:       Vec<f64>
}

impl StageSamples {
    fn append(&mut self, mut other: StageSamples) {
        self.trade_time_to_row_local
            .append(&mut other.trade_time_to_row_local);
        self.block_time_to_row_local
            .append(&mut other.block_time_to_row_local);
        self.row_local_to_notification
            .append(&mut other.row_local_to_notification);
        self.notification_to_drain_file
            .append(&mut other.notification_to_drain_file);
        self.drain_file.append(&mut other.drain_file);
        self.drain_file_to_drain_new_bytes
            .append(&mut other.drain_file_to_drain_new_bytes);
        self.drain_new_bytes.append(&mut other.drain_new_bytes);
        self.notification_to_file_bytes
            .append(&mut other.notification_to_file_bytes);
        self.file_bytes_to_parsed_row
            .append(&mut other.file_bytes_to_parsed_row);
        self.parsed_row_to_derived_trade
            .append(&mut other.parsed_row_to_derived_trade);
        self.channel_send_to_receive
            .append(&mut other.channel_send_to_receive);
        self.notification_to_channel
            .append(&mut other.notification_to_channel);
    }
}

fn spawn_reader_collector(
    name: &'static str,
    spawn_reader: SpawnReader,
    directory: &Path
) -> eyre::Result<ReaderCollector> {
    let rx = spawn_reader(HyperliquidDirKind::NodeFills, directory)?;
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
    rx: &mpsc::Receiver<eyre::Result<FsOutData>>,
    target_bytes: usize,
    timeout: Duration
) -> eyre::Result<Sample> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut received_bytes = 0;
    let mut chunks = 0;
    let mut stages = StageSamples::default();
    let mut row_profiler = NodeFillsRowProfiler::default();

    while received_bytes < target_bytes {
        let now = Instant::now();
        if now >= deadline {
            return Err(eyre::eyre!(
                "timed out after receiving {received_bytes}/{target_bytes} bytes"
            ));
        }

        let chunk = match rx.recv_timeout(deadline - now) {
            Ok(chunk) => chunk?,
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

        let channel_received_at_ns = unix_timestamp().as_nanos();
        stages.append(chunk_pipeline_stages(&chunk, channel_received_at_ns));
        stages.append(row_profiler.profile_chunk(&chunk)?);

        received_bytes += chunk.bytes.len();
        chunks += 1;
    }

    Ok(Sample { total: started.elapsed(), bytes: received_bytes, chunks, stages })
}

fn chunk_pipeline_stages(chunk: &FsOutData, channel_received_at_ns: u128) -> StageSamples {
    let mut stages = StageSamples::default();
    let pipeline = chunk.pipeline;

    stages.notification_to_drain_file.push(delta_ms(
        pipeline.drain_file_started_at_ns,
        pipeline.notification_batch_received_at_ns
    ));
    stages
        .drain_file
        .push(delta_ms(pipeline.drain_file_finished_at_ns, pipeline.drain_file_started_at_ns));
    stages
        .drain_file_to_drain_new_bytes
        .push(delta_ms(pipeline.drain_new_bytes_started_at_ns, pipeline.drain_file_started_at_ns));
    stages.drain_new_bytes.push(delta_ms(
        pipeline.drain_new_bytes_finished_at_ns,
        pipeline.drain_new_bytes_started_at_ns
    ));
    stages
        .notification_to_file_bytes
        .push(delta_ms(pipeline.file_bytes_read_at_ns, pipeline.notification_batch_received_at_ns));
    stages
        .channel_send_to_receive
        .push(delta_ms(channel_received_at_ns, pipeline.channel_send_started_at_ns));
    stages
        .notification_to_channel
        .push(delta_ms(channel_received_at_ns, pipeline.notification_batch_received_at_ns));

    stages
}

#[derive(Default)]
struct NodeFillsRowProfiler {
    deriver:     TradeDeriver,
    line_buffer: Vec<u8>
}

impl NodeFillsRowProfiler {
    fn profile_chunk(&mut self, chunk: &FsOutData) -> eyre::Result<StageSamples> {
        let mut buffer = std::mem::take(&mut self.line_buffer);
        buffer.extend_from_slice(&chunk.bytes);

        let mut stages = StageSamples::default();
        let mut line_start = 0;
        let mut consumed_len = 0;

        for newline_idx in buffer
            .iter()
            .enumerate()
            .filter_map(|(idx, byte)| (*byte == b'\n').then_some(idx))
        {
            let line = &buffer[line_start..newline_idx];
            if !is_blank_line(line) {
                stages.append(self.profile_row(line, chunk)?);
            }
            line_start = newline_idx + 1;
            consumed_len = line_start;
        }

        if consumed_len > 0 {
            buffer.drain(..consumed_len);
        }

        self.line_buffer = buffer;
        Ok(stages)
    }

    fn profile_row(&mut self, line: &[u8], chunk: &FsOutData) -> eyre::Result<StageSamples> {
        let row = NodeFillsParser::parse_raw_type(line).wrap_err_with(|| {
            format!("failed to parse node_fills_streaming row from {}", chunk.path)
        })?;
        let parsed_at_ns = unix_timestamp().as_nanos();
        let local_time_ns = parse_node_timestamp_ns(&row.local_time)
            .wrap_err_with(|| format!("failed to parse local_time {}", row.local_time))?;
        let block_time_ns = parse_node_timestamp_ns(&row.block_time)
            .wrap_err_with(|| format!("failed to parse block_time {}", row.block_time))?;

        let trades = match self
            .deriver
            .handle_data(&HyperliquidDirData::NodeFills(vec![row]))?
        {
            Some(HyperliquidData::Trades(trades)) => trades,
            None => Vec::new()
        };
        let derived_at_ns = unix_timestamp().as_nanos();

        let mut stages = StageSamples::default();
        stages
            .block_time_to_row_local
            .push(delta_ms(local_time_ns, block_time_ns));
        stages
            .row_local_to_notification
            .push(delta_ms(chunk.pipeline.notification_batch_received_at_ns, local_time_ns));
        stages
            .file_bytes_to_parsed_row
            .push(delta_ms(parsed_at_ns, chunk.pipeline.file_bytes_read_at_ns));
        stages
            .parsed_row_to_derived_trade
            .push(delta_ms(derived_at_ns, parsed_at_ns));
        stages.trade_time_to_row_local.extend(
            trades
                .iter()
                .map(|trade| delta_ms(local_time_ns, trade.time as u128 * NS_PER_MS))
        );

        Ok(stages)
    }
}

fn is_blank_line(line: &[u8]) -> bool {
    line.strip_suffix(b"\r")
        .unwrap_or(line)
        .iter()
        .all(|byte| byte.is_ascii_whitespace())
}

fn parse_node_timestamp_ns(value: &str) -> eyre::Result<u128> {
    let (date, time) = value
        .split_once('T')
        .ok_or_else(|| eyre::eyre!("timestamp missing T separator"))?;
    let mut date_parts = date.split('-');
    let year = parse_next_i32(&mut date_parts, "year")?;
    let month = parse_next_u32(&mut date_parts, "month")?;
    let day = parse_next_u32(&mut date_parts, "day")?;
    if date_parts.next().is_some() {
        return Err(eyre::eyre!("timestamp date has too many fields"));
    }

    let (time, fractional) = time.split_once('.').unwrap_or((time, ""));
    let mut time_parts = time.split(':');
    let hour = parse_next_u32(&mut time_parts, "hour")?;
    let minute = parse_next_u32(&mut time_parts, "minute")?;
    let second = parse_next_u32(&mut time_parts, "second")?;
    if time_parts.next().is_some() {
        return Err(eyre::eyre!("timestamp time has too many fields"));
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return Err(eyre::eyre!("timestamp is before Unix epoch"));
    }

    let seconds =
        days as u128 * 86_400 + hour as u128 * 3_600 + minute as u128 * 60 + second as u128;
    Ok(seconds * NS_PER_SEC + parse_fractional_ns(fractional)?)
}

fn parse_next_i32<'a>(parts: &mut impl Iterator<Item = &'a str>, name: &str) -> eyre::Result<i32> {
    parts
        .next()
        .ok_or_else(|| eyre::eyre!("timestamp missing {name}"))?
        .parse()
        .wrap_err_with(|| format!("timestamp has invalid {name}"))
}

fn parse_next_u32<'a>(parts: &mut impl Iterator<Item = &'a str>, name: &str) -> eyre::Result<u32> {
    parts
        .next()
        .ok_or_else(|| eyre::eyre!("timestamp missing {name}"))?
        .parse()
        .wrap_err_with(|| format!("timestamp has invalid {name}"))
}

fn parse_fractional_ns(fractional: &str) -> eyre::Result<u128> {
    if fractional.len() > 9 {
        return Err(eyre::eyre!("timestamp fractional seconds exceed nanosecond precision"));
    }

    let mut nanos = 0_u128;
    for byte in fractional.bytes() {
        if !byte.is_ascii_digit() {
            return Err(eyre::eyre!("timestamp fractional seconds contain a non-digit"));
        }
        nanos = nanos * 10 + u128::from(byte - b'0');
    }

    for _ in fractional.len()..9 {
        nanos *= 10;
    }

    Ok(nanos)
}

fn days_from_civil(mut year: i32, month: u32, day: u32) -> i64 {
    year -= i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u32;
    let month = month as i32;
    let day = day as i32;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era =
        year_of_era as i32 * 365 + year_of_era as i32 / 4 - year_of_era as i32 / 100 + day_of_year;

    i64::from(era) * 146_097 + i64::from(day_of_era) - 719_468
}

fn drain_stale_messages(rx: &mpsc::Receiver<eyre::Result<FsOutData>>) {
    while rx.try_recv().is_ok() {}
}

fn print_report(reports: &[ReaderReport]) {
    println!(
        "{:<22} {:>12} {:>12} {:>12} {:>12} {:>16} {:>16}",
        "reader", "wall avg", "wall p95", "MiB/s", "avg MiB", "notify->rx avg", "notify->rx p95"
    );

    for report in reports {
        let summary = Summary::new(report);
        println!(
            "{:<22} {:>12.3} {:>12.3} {:>12.2} {:>12.3} {:>16} {:>16}",
            report.name,
            ms(summary.avg_total),
            ms(summary.p95_total),
            summary.throughput_mib_s,
            bytes_to_mib(summary.avg_bytes as usize),
            format_ms(summary.notification_to_channel.avg_ms),
            format_ms(summary.notification_to_channel.p95_ms),
        );
    }

    println!();
    println!("{:<22} {:>12} {:>12}", "reader", "target MiB", "avg chunks");
    for report in reports {
        let summary = Summary::new(report);
        println!(
            "{:<22} {:>12.3} {:>12.1}",
            report.name,
            bytes_to_mib(report.target_bytes_per_run),
            summary.avg_chunks,
        );
    }

    println!();
    println!(
        "{:<22} {:<34} {:>12} {:>12} {:>10}",
        "reader", "stage", "avg ms", "p95 ms", "samples"
    );
    for report in reports {
        let summary = Summary::new(report);
        for (name, stage) in summary.stage_rows() {
            println!(
                "{:<22} {:<34} {:>12} {:>12} {:>10}",
                report.name,
                name,
                format_ms(stage.avg_ms),
                format_ms(stage.p95_ms),
                stage.samples
            );
        }
    }
}

struct Summary {
    avg_total:               Duration,
    p95_total:               Duration,
    notification_to_channel: StageSummary,
    avg_bytes:               f64,
    avg_chunks:              f64,
    throughput_mib_s:        f64,
    stages:                  StageSummaries
}

impl Summary {
    fn new(report: &ReaderReport) -> Self {
        let mut totals: Vec<Duration> = report.samples.iter().map(|sample| sample.total).collect();
        totals.sort_unstable();

        let avg_total = average_duration(totals.iter().copied());
        let p95_total = percentile(&totals, 0.95);
        let stages = StageSummaries::new(report);
        let notification_to_channel = stages.notification_to_channel;
        let avg_bytes = report
            .samples
            .iter()
            .map(|sample| sample.bytes as f64)
            .sum::<f64>()
            / report.samples.len() as f64;
        let avg_chunks = report
            .samples
            .iter()
            .map(|sample| sample.chunks as f64)
            .sum::<f64>()
            / report.samples.len() as f64;
        let throughput_mib_s = bytes_to_mib(avg_bytes as usize) / avg_total.as_secs_f64();

        Self {
            avg_total,
            p95_total,
            notification_to_channel,
            avg_bytes,
            avg_chunks,
            throughput_mib_s,
            stages
        }
    }

    fn stage_rows(&self) -> [(&'static str, StageSummary); 12] {
        [
            ("trade time -> row local_time", self.stages.trade_time_to_row_local),
            ("block_time -> row local_time", self.stages.block_time_to_row_local),
            ("row local_time -> notify", self.stages.row_local_to_notification),
            ("notify -> drain_file", self.stages.notification_to_drain_file),
            ("drain_file duration", self.stages.drain_file),
            ("drain_file -> drain_new_bytes", self.stages.drain_file_to_drain_new_bytes),
            ("drain_new_bytes duration", self.stages.drain_new_bytes),
            ("notify -> file bytes read", self.stages.notification_to_file_bytes),
            ("file bytes read -> parsed row", self.stages.file_bytes_to_parsed_row),
            ("parsed row -> derived trade", self.stages.parsed_row_to_derived_trade),
            ("channel send -> receive", self.stages.channel_send_to_receive),
            ("notify -> channel receive", self.stages.notification_to_channel)
        ]
    }
}

#[derive(Clone, Copy)]
struct StageSummary {
    avg_ms:  Option<f64>,
    p95_ms:  Option<f64>,
    samples: usize
}

impl StageSummary {
    fn new(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self { avg_ms: None, p95_ms: None, samples: 0 };
        }

        let mut sorted = samples.to_vec();
        sorted.sort_by(|left, right| left.total_cmp(right));

        Self {
            avg_ms:  Some(samples.iter().sum::<f64>() / samples.len() as f64),
            p95_ms:  Some(percentile_f64(&sorted, 0.95)),
            samples: samples.len()
        }
    }
}

struct StageSummaries {
    trade_time_to_row_local:       StageSummary,
    block_time_to_row_local:       StageSummary,
    row_local_to_notification:     StageSummary,
    notification_to_drain_file:    StageSummary,
    drain_file:                    StageSummary,
    drain_file_to_drain_new_bytes: StageSummary,
    drain_new_bytes:               StageSummary,
    notification_to_file_bytes:    StageSummary,
    file_bytes_to_parsed_row:      StageSummary,
    parsed_row_to_derived_trade:   StageSummary,
    channel_send_to_receive:       StageSummary,
    notification_to_channel:       StageSummary
}

impl StageSummaries {
    fn new(report: &ReaderReport) -> Self {
        let mut stages = StageSamples::default();
        for sample in &report.samples {
            stages
                .trade_time_to_row_local
                .extend(&sample.stages.trade_time_to_row_local);
            stages
                .block_time_to_row_local
                .extend(&sample.stages.block_time_to_row_local);
            stages
                .row_local_to_notification
                .extend(&sample.stages.row_local_to_notification);
            stages
                .notification_to_drain_file
                .extend(&sample.stages.notification_to_drain_file);
            stages.drain_file.extend(&sample.stages.drain_file);
            stages
                .drain_file_to_drain_new_bytes
                .extend(&sample.stages.drain_file_to_drain_new_bytes);
            stages
                .drain_new_bytes
                .extend(&sample.stages.drain_new_bytes);
            stages
                .notification_to_file_bytes
                .extend(&sample.stages.notification_to_file_bytes);
            stages
                .file_bytes_to_parsed_row
                .extend(&sample.stages.file_bytes_to_parsed_row);
            stages
                .parsed_row_to_derived_trade
                .extend(&sample.stages.parsed_row_to_derived_trade);
            stages
                .channel_send_to_receive
                .extend(&sample.stages.channel_send_to_receive);
            stages
                .notification_to_channel
                .extend(&sample.stages.notification_to_channel);
        }

        Self {
            trade_time_to_row_local:       StageSummary::new(&stages.trade_time_to_row_local),
            block_time_to_row_local:       StageSummary::new(&stages.block_time_to_row_local),
            row_local_to_notification:     StageSummary::new(&stages.row_local_to_notification),
            notification_to_drain_file:    StageSummary::new(&stages.notification_to_drain_file),
            drain_file:                    StageSummary::new(&stages.drain_file),
            drain_file_to_drain_new_bytes: StageSummary::new(&stages.drain_file_to_drain_new_bytes),
            drain_new_bytes:               StageSummary::new(&stages.drain_new_bytes),
            notification_to_file_bytes:    StageSummary::new(&stages.notification_to_file_bytes),
            file_bytes_to_parsed_row:      StageSummary::new(&stages.file_bytes_to_parsed_row),
            parsed_row_to_derived_trade:   StageSummary::new(&stages.parsed_row_to_derived_trade),
            channel_send_to_receive:       StageSummary::new(&stages.channel_send_to_receive),
            notification_to_channel:       StageSummary::new(&stages.notification_to_channel)
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

fn percentile_f64(samples: &[f64], percentile: f64) -> f64 {
    let idx = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
    samples[idx]
}

fn bytes_to_mib(bytes: usize) -> f64 {
    bytes as f64 / 1_048_576.0
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn delta_ms(later_ns: u128, earlier_ns: u128) -> f64 {
    (later_ns as f64 - earlier_ns as f64) / NS_PER_MS as f64
}

fn format_ms(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.3}"))
}
