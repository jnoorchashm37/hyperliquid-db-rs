use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::RecvTimeoutError
    },
    thread::JoinHandle,
    time::Duration
};

use hyperliquid_db_core::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta},
    processors::{HyperliquidDataProcessorHandle, TradeDeriver},
    types::{HyperliquidData, Trade},
    utils::{NS_PER_MS, NS_PER_SEC, unix_timestamp}
};
use serde::Deserialize;

use crate::utils::{set_hl_websocket_read_timeout, spawn_hl_trades_websocket, spawn_hl_watcher};

const TIMEOUT_SECS: u64 = 60;
const PUBLIC_WS_READ_TIMEOUT_MS: u64 = 100;
const TRADES_COIN: &str = "BTC";
static IS_RUNNING: AtomicBool = AtomicBool::new(true);

pub fn run_trades_ws_bench() -> eyre::Result<()> {
    println!("subscribing to public trades websocket for {TRADES_COIN}");

    let public_ws_stream_handle = run_public_ws_stream();
    let implemented_stream_handle = run_implemented_stream();

    let timeout = std::env::var("TIMEOUT_SECS")
        .unwrap_or_else(|_| TIMEOUT_SECS.to_string())
        .parse()
        .unwrap();
    println!("sleeping for {timeout} seconds");
    std::thread::sleep(Duration::from_secs(timeout));
    IS_RUNNING.store(false, Ordering::Release);

    let public_ws_stream = public_ws_stream_handle
        .join()
        .map_err(|_| eyre::eyre!("public websocket stream thread panicked"))??;
    let implemented_stream = implemented_stream_handle
        .join()
        .map_err(|_| eyre::eyre!("implemented stream thread panicked"))??;

    let comparison =
        TradeTimeComparionMetrics::compare_trade_caches(public_ws_stream, implemented_stream);

    comparison.pretty_print();

    Ok(())
}

fn run_public_ws_stream() -> JoinHandle<eyre::Result<TradeCache>> {
    std::thread::spawn(move || {
        let mut public_ws_stream = spawn_hl_trades_websocket(TRADES_COIN)?;
        set_hl_websocket_read_timeout(
            &mut public_ws_stream,
            Some(Duration::from_millis(PUBLIC_WS_READ_TIMEOUT_MS))
        )?;

        let mut cache = TradeCache::new("public ws");

        loop {
            let message = match public_ws_stream.read() {
                Ok(message) => message,
                Err(tungstenite::Error::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    if !IS_RUNNING.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
                Err(error) => return Err(error.into())
            };
            if message.is_text() {
                let rx_timestamp_ns = unix_timestamp().as_nanos();
                let message: WsMessage = serde_json::from_str(message.to_text()?)?;
                if message.channel == "trades" {
                    let trades = serde_json::from_value(message.data)?;
                    cache.new_trades(trades, rx_timestamp_ns, None);
                }
            }

            if !IS_RUNNING.load(Ordering::Relaxed) {
                break
            }
        }

        Ok(cache)
    })
}

fn run_implemented_stream() -> JoinHandle<eyre::Result<TradeCache>> {
    std::thread::spawn(move || {
        let implemented_stream = spawn_hl_watcher()?;
        let mut cache = TradeCache::new("file reader");

        let mut deriver = TradeDeriver::new();

        loop {
            let data = match implemented_stream
                .recv_timeout(Duration::from_millis(PUBLIC_WS_READ_TIMEOUT_MS))
            {
                Ok(data) => data?,
                Err(RecvTimeoutError::Timeout) => {
                    if !IS_RUNNING.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(eyre::eyre!("implemented stream channel disconnected"));
                }
            };

            let channel_received_at_ns = unix_timestamp().as_nanos();
            let pipeline_meta = data.pipeline_meta.clone();
            let rx_timestamp_ns = pipeline_meta.notification_received_at_ns;
            let pipeline = pipeline_meta.pipeline;
            let HyperliquidDirData::NodeFills(rows) = data.data;

            for row in rows {
                let row_local_time_ns = parse_node_timestamp_ns(&row.local_time)?;
                let row_block_time_ns = parse_node_timestamp_ns(&row.block_time)?;
                let row_data = HyperliquidDirDataWithMeta {
                    data:          HyperliquidDirData::NodeFills(vec![row]),
                    pipeline_meta: pipeline_meta.clone()
                };
                let trades = match deriver.handle_data(&row_data)? {
                    Some(HyperliquidData::Trades(trades)) => trades,
                    None => Vec::new()
                };
                let derived_at_ns = unix_timestamp().as_nanos();
                let fs_timing = FsTradeTiming {
                    row_local_time_ns,
                    row_block_time_ns,
                    file_bytes_read_at_ns: pipeline.file_bytes_read_at_ns,
                    channel_send_started_at_ns: pipeline.channel_send_started_at_ns,
                    channel_received_at_ns,
                    derived_at_ns
                };

                let trades = trades.into_iter().map(|trade| trade.data).collect();
                cache.new_trades(trades, rx_timestamp_ns, Some(fs_timing));
            }

            if !IS_RUNNING.load(Ordering::Relaxed) {
                break
            }
        }
        Ok(cache)
    })
}

#[derive(Deserialize)]
struct WsMessage {
    channel: String,
    #[serde(default)]
    data:    serde_json::Value
}

#[derive(Debug)]
struct TradeCache {
    name:   &'static str,
    trades: Vec<TimestampedTrade>
}

impl TradeCache {
    fn new(name: &'static str) -> Self {
        Self { name, trades: Vec::new() }
    }

    fn new_trades(
        &mut self,
        trades: Vec<Trade>,
        rx_timestamp_ns: u128,
        fs_timing: Option<FsTradeTiming>
    ) {
        trades.into_iter().for_each(|trade| {
            if &trade.coin == TRADES_COIN {
                self.trades
                    .push(TimestampedTrade { rx_timestamp_ns, trade, fs_timing });
            }
        });
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TimestampedTrade {
    rx_timestamp_ns: u128,
    trade:           Trade,
    fs_timing:       Option<FsTradeTiming>
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct FsTradeTiming {
    row_local_time_ns:          u128,
    row_block_time_ns:          u128,
    file_bytes_read_at_ns:      u128,
    channel_send_started_at_ns: u128,
    channel_received_at_ns:     u128,
    derived_at_ns:              u128
}

#[derive(Debug)]
struct TradeTimeComparionMetrics {
    cache0: &'static str,
    cache1: &'static str,
    trades0: usize,
    trades1: usize,
    total_similiar_trades: usize,
    latency_lag0_ms: LatencyStats,
    latency_lag1_ms: LatencyStats,
    diff_latency_lag_ms: LatencyStats,
    min_max_first_rx_time0_ns: (u128, u128),
    min_max_first_rx_time1_ns: (u128, u128),
    min_max_first_trade_time0_ms: (u64, u64),
    min_max_first_trade_time1_ms: (u64, u64),
    matched_min_max_rx_time0_ns: (u128, u128),
    matched_min_max_rx_time1_ns: (u128, u128),
    matched_min_max_trade_time0_ms: (u64, u64),
    matched_min_max_trade_time1_ms: (u64, u64),
    file_reader_stats: Option<FileReaderStats>
}

#[derive(Debug, Clone, Copy)]
struct LatencyStats {
    avg_ms:              f64,
    p50_ms:              f64,
    p95_ms:              f64,
    min_ms:              f64,
    max_ms:              f64,
    first_window_avg_ms: f64,
    last_window_avg_ms:  f64,
    window_samples:      usize
}

impl LatencyStats {
    fn new(mut values: Vec<f64>) -> Self {
        assert!(!values.is_empty(), "latency stats need at least one sample");

        let avg_ms = values.iter().sum::<f64>() / values.len() as f64;
        let window_samples = values.len().div_ceil(10).clamp(1, 100);
        let first_window_avg_ms = avg_f64(&values[..window_samples]);
        let last_window_avg_ms = avg_f64(&values[values.len() - window_samples..]);
        values.sort_by(|left, right| left.total_cmp(right));

        Self {
            avg_ms,
            p50_ms: percentile_f64(&values, 0.50),
            p95_ms: percentile_f64(&values, 0.95),
            min_ms: *values.first().expect("values is non-empty"),
            max_ms: *values.last().expect("values is non-empty"),
            first_window_avg_ms,
            last_window_avg_ms,
            window_samples
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FileReaderStats {
    trade_time_to_row_local:        LatencyStats,
    row_block_time_to_row_local:    LatencyStats,
    row_local_to_notify:            LatencyStats,
    notify_to_file_bytes_read:      LatencyStats,
    notify_to_channel_send:         LatencyStats,
    channel_send_to_receive:        LatencyStats,
    notify_to_bench_receive:        LatencyStats,
    bench_receive_to_derived_trade: LatencyStats,
    trade_time_to_bench_receive:    LatencyStats,
    trade_time_to_derived_trade:    LatencyStats
}

impl FileReaderStats {
    fn new(similiar_trades: &[(TimestampedTrade, TimestampedTrade)]) -> Option<Self> {
        let mut trade_time_to_row_local = Vec::new();
        let mut row_block_time_to_row_local = Vec::new();
        let mut row_local_to_notify = Vec::new();
        let mut notify_to_file_bytes_read = Vec::new();
        let mut notify_to_channel_send = Vec::new();
        let mut channel_send_to_receive = Vec::new();
        let mut notify_to_bench_receive = Vec::new();
        let mut bench_receive_to_derived_trade = Vec::new();
        let mut trade_time_to_bench_receive = Vec::new();
        let mut trade_time_to_derived_trade = Vec::new();

        for (_, file_trade) in similiar_trades {
            let Some(fs_timing) = file_trade.fs_timing else {
                continue;
            };
            let trade_time_ns = file_trade.trade.time as u128 * NS_PER_MS;

            trade_time_to_row_local.push(delta_ms(fs_timing.row_local_time_ns, trade_time_ns));
            row_block_time_to_row_local
                .push(delta_ms(fs_timing.row_local_time_ns, fs_timing.row_block_time_ns));
            row_local_to_notify
                .push(delta_ms(file_trade.rx_timestamp_ns, fs_timing.row_local_time_ns));
            notify_to_file_bytes_read
                .push(delta_ms(fs_timing.file_bytes_read_at_ns, file_trade.rx_timestamp_ns));
            notify_to_channel_send
                .push(delta_ms(fs_timing.channel_send_started_at_ns, file_trade.rx_timestamp_ns));
            channel_send_to_receive.push(delta_ms(
                fs_timing.channel_received_at_ns,
                fs_timing.channel_send_started_at_ns
            ));
            notify_to_bench_receive
                .push(delta_ms(fs_timing.channel_received_at_ns, file_trade.rx_timestamp_ns));
            bench_receive_to_derived_trade
                .push(delta_ms(fs_timing.derived_at_ns, fs_timing.channel_received_at_ns));
            trade_time_to_bench_receive
                .push(delta_ms(fs_timing.channel_received_at_ns, trade_time_ns));
            trade_time_to_derived_trade.push(delta_ms(fs_timing.derived_at_ns, trade_time_ns));
        }

        if trade_time_to_row_local.is_empty() {
            return None;
        }

        Some(Self {
            trade_time_to_row_local:        LatencyStats::new(trade_time_to_row_local),
            row_block_time_to_row_local:    LatencyStats::new(row_block_time_to_row_local),
            row_local_to_notify:            LatencyStats::new(row_local_to_notify),
            notify_to_file_bytes_read:      LatencyStats::new(notify_to_file_bytes_read),
            notify_to_channel_send:         LatencyStats::new(notify_to_channel_send),
            channel_send_to_receive:        LatencyStats::new(channel_send_to_receive),
            notify_to_bench_receive:        LatencyStats::new(notify_to_bench_receive),
            bench_receive_to_derived_trade: LatencyStats::new(bench_receive_to_derived_trade),
            trade_time_to_bench_receive:    LatencyStats::new(trade_time_to_bench_receive),
            trade_time_to_derived_trade:    LatencyStats::new(trade_time_to_derived_trade)
        })
    }

    fn stage_rows(&self) -> [(&'static str, LatencyStats); 10] {
        [
            ("trade time -> row local_time", self.trade_time_to_row_local),
            ("row block_time -> row local_time", self.row_block_time_to_row_local),
            ("row local_time -> notify", self.row_local_to_notify),
            ("notify -> file bytes read", self.notify_to_file_bytes_read),
            ("notify -> channel send", self.notify_to_channel_send),
            ("channel send -> bench receive", self.channel_send_to_receive),
            ("notify -> bench receive", self.notify_to_bench_receive),
            ("bench receive -> derived trade", self.bench_receive_to_derived_trade),
            ("trade time -> bench receive", self.trade_time_to_bench_receive),
            ("trade time -> derived trade", self.trade_time_to_derived_trade)
        ]
    }
}

impl TradeTimeComparionMetrics {
    fn compare_trade_caches(cache0: TradeCache, cache1: TradeCache) -> Self {
        let mut cache0_trades_by_key = cache0
            .trades
            .iter()
            .map(|trade| (trade.trade.clone(), trade.clone()))
            .collect::<HashMap<_, _>>();

        let mut similiar_trades = Vec::new();

        cache1.trades.iter().for_each(|trade| {
            if let Some(cach0_trade) = cache0_trades_by_key.remove(&trade.trade) {
                // assert_eq!(&trade.trade, cach0_trade);
                similiar_trades.push((cach0_trade.clone(), trade.clone()));
            }
        });

        let similiar_trades_len = similiar_trades.len();
        assert!(similiar_trades_len > 0, "no comparable trades found");

        let mut latency_lag0_ms = Vec::with_capacity(similiar_trades_len);
        let mut latency_lag1_ms = Vec::with_capacity(similiar_trades_len);
        let mut diff_latency_lag_ms = Vec::with_capacity(similiar_trades_len);
        for (trade0, trade1) in &similiar_trades {
            let lag0_ms = (trade0.rx_timestamp_ns as f64
                - (trade0.trade.time as u128 * NS_PER_MS) as f64)
                / NS_PER_MS as f64;
            let lag1_ms = (trade1.rx_timestamp_ns as f64
                - (trade1.trade.time as u128 * NS_PER_MS) as f64)
                / NS_PER_MS as f64;

            latency_lag0_ms.push(lag0_ms);
            latency_lag1_ms.push(lag1_ms);
            diff_latency_lag_ms.push(lag0_ms - lag1_ms);
        }

        let min_first_rx_time0_ns = cache0
            .trades
            .iter()
            .map(|trade| trade.rx_timestamp_ns)
            .min()
            .unwrap();
        let max_first_rx_time0_ns = cache0
            .trades
            .iter()
            .map(|trade| trade.rx_timestamp_ns)
            .max()
            .unwrap();
        let min_first_rx_time1_ns = cache1
            .trades
            .iter()
            .map(|trade| trade.rx_timestamp_ns)
            .min()
            .unwrap();
        let max_first_rx_time1_ns = cache1
            .trades
            .iter()
            .map(|trade| trade.rx_timestamp_ns)
            .max()
            .unwrap();

        let min_first_trade_time0_ms = cache0
            .trades
            .iter()
            .map(|trade| trade.trade.time)
            .min()
            .unwrap();
        let max_first_trade_time0_ms = cache0
            .trades
            .iter()
            .map(|trade| trade.trade.time)
            .max()
            .unwrap();
        let min_first_trade_time1_ms = cache1
            .trades
            .iter()
            .map(|trade| trade.trade.time)
            .min()
            .unwrap();
        let max_first_trade_time1_ms = cache1
            .trades
            .iter()
            .map(|trade| trade.trade.time)
            .max()
            .unwrap();

        let matched_min_first_rx_time0_ns = similiar_trades
            .iter()
            .map(|(trade0, _)| trade0.rx_timestamp_ns)
            .min()
            .unwrap();
        let matched_max_first_rx_time0_ns = similiar_trades
            .iter()
            .map(|(trade0, _)| trade0.rx_timestamp_ns)
            .max()
            .unwrap();
        let matched_min_first_rx_time1_ns = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.rx_timestamp_ns)
            .min()
            .unwrap();
        let matched_max_first_rx_time1_ns = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.rx_timestamp_ns)
            .max()
            .unwrap();
        let matched_min_first_trade_time0_ms = similiar_trades
            .iter()
            .map(|(trade0, _)| trade0.trade.time)
            .min()
            .unwrap();
        let matched_max_first_trade_time0_ms = similiar_trades
            .iter()
            .map(|(trade0, _)| trade0.trade.time)
            .max()
            .unwrap();
        let matched_min_first_trade_time1_ms = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.trade.time)
            .min()
            .unwrap();
        let matched_max_first_trade_time1_ms = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.trade.time)
            .max()
            .unwrap();

        TradeTimeComparionMetrics {
            cache0: cache0.name,
            cache1: cache1.name,
            trades0: cache0.trades.len(),
            trades1: cache1.trades.len(),
            total_similiar_trades: similiar_trades.len(),
            latency_lag0_ms: LatencyStats::new(latency_lag0_ms),
            latency_lag1_ms: LatencyStats::new(latency_lag1_ms),
            diff_latency_lag_ms: LatencyStats::new(diff_latency_lag_ms),
            min_max_first_rx_time0_ns: (min_first_rx_time0_ns, max_first_rx_time0_ns),
            min_max_first_rx_time1_ns: (min_first_rx_time1_ns, max_first_rx_time1_ns),
            min_max_first_trade_time0_ms: (min_first_trade_time0_ms, max_first_trade_time0_ms),
            min_max_first_trade_time1_ms: (min_first_trade_time1_ms, max_first_trade_time1_ms),
            matched_min_max_rx_time0_ns: (
                matched_min_first_rx_time0_ns,
                matched_max_first_rx_time0_ns
            ),
            matched_min_max_rx_time1_ns: (
                matched_min_first_rx_time1_ns,
                matched_max_first_rx_time1_ns
            ),
            matched_min_max_trade_time0_ms: (
                matched_min_first_trade_time0_ms,
                matched_max_first_trade_time0_ms
            ),
            matched_min_max_trade_time1_ms: (
                matched_min_first_trade_time1_ms,
                matched_max_first_trade_time1_ms
            ),
            file_reader_stats: FileReaderStats::new(&similiar_trades)
        }
    }

    fn pretty_print(&self) {
        let comparable_trades = self.trades0.min(self.trades1);
        let match_rate = if comparable_trades == 0 {
            0.0
        } else {
            (self.total_similiar_trades as f64 / comparable_trades as f64) * 100.0
        };

        println!("trade websocket latency comparison");
        println!(
            "{:<18} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "stream", "trades", "avg ms", "p50 ms", "p95 ms", "max ms"
        );
        println!(
            "{:<18} {:>12} {:>12.3} {:>12.3} {:>12.3} {:>12.3}",
            self.cache0,
            self.trades0,
            self.latency_lag0_ms.avg_ms,
            self.latency_lag0_ms.p50_ms,
            self.latency_lag0_ms.p95_ms,
            self.latency_lag0_ms.max_ms
        );
        println!(
            "{:<18} {:>12} {:>12.3} {:>12.3} {:>12.3} {:>12.3}",
            self.cache1,
            self.trades1,
            self.latency_lag1_ms.avg_ms,
            self.latency_lag1_ms.p50_ms,
            self.latency_lag1_ms.p95_ms,
            self.latency_lag1_ms.max_ms
        );
        println!();
        println!("matched trades: {} ({match_rate:.2}%)", self.total_similiar_trades);
        println!(
            "avg lag delta ({} - {}): {:.3} ms",
            self.cache0, self.cache1, self.diff_latency_lag_ms.avg_ms
        );
        println!(
            "lag delta p50/p95/min/max: {:.3} / {:.3} / {:.3} / {:.3} ms",
            self.diff_latency_lag_ms.p50_ms,
            self.diff_latency_lag_ms.p95_ms,
            self.diff_latency_lag_ms.min_ms,
            self.diff_latency_lag_ms.max_ms
        );
        println!();
        println!(
            "first/last matched window avg lag ({} samples per stream)",
            self.latency_lag0_ms.window_samples
        );
        println!(
            "{:<18} {:>12.3} -> {:>12.3} ms",
            self.cache0,
            self.latency_lag0_ms.first_window_avg_ms,
            self.latency_lag0_ms.last_window_avg_ms
        );
        println!(
            "{:<18} {:>12.3} -> {:>12.3} ms",
            self.cache1,
            self.latency_lag1_ms.first_window_avg_ms,
            self.latency_lag1_ms.last_window_avg_ms
        );
        println!(
            "{:<18} {:>12.3} -> {:>12.3} ms",
            "delta",
            self.diff_latency_lag_ms.first_window_avg_ms,
            self.diff_latency_lag_ms.last_window_avg_ms
        );
        if let Some(file_reader_stats) = self.file_reader_stats {
            println!();
            println!("file reader pipeline for matched trades");
            println!(
                "{:<34} {:>12} {:>12} {:>12} {:>12} {:>25}",
                "stage", "avg ms", "p50 ms", "p95 ms", "max ms", "first -> last avg ms"
            );
            for (name, stats) in file_reader_stats.stage_rows() {
                println!(
                    "{:<34} {:>12.3} {:>12.3} {:>12.3} {:>12.3} {:>10.3} -> {:>10.3}",
                    name,
                    stats.avg_ms,
                    stats.p50_ms,
                    stats.p95_ms,
                    stats.max_ms,
                    stats.first_window_avg_ms,
                    stats.last_window_avg_ms
                );
            }
        }
        println!();
        println!("all observed trades");
        println!("{:<18} {:>35} {:>35}", "stream", "rx time range ns", "trade time range ms");
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache0,
            format!("{} - {}", self.min_max_first_rx_time0_ns.0, self.min_max_first_rx_time0_ns.1),
            format!(
                "{} - {}",
                self.min_max_first_trade_time0_ms.0, self.min_max_first_trade_time0_ms.1
            )
        );
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache1,
            format!("{} - {}", self.min_max_first_rx_time1_ns.0, self.min_max_first_rx_time1_ns.1),
            format!(
                "{} - {}",
                self.min_max_first_trade_time1_ms.0, self.min_max_first_trade_time1_ms.1
            )
        );
        println!();
        println!("matched trades only");
        println!("{:<18} {:>35} {:>35}", "stream", "rx time range ns", "trade time range ms");
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache0,
            format!(
                "{} - {}",
                self.matched_min_max_rx_time0_ns.0, self.matched_min_max_rx_time0_ns.1
            ),
            format!(
                "{} - {}",
                self.matched_min_max_trade_time0_ms.0, self.matched_min_max_trade_time0_ms.1
            )
        );
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache1,
            format!(
                "{} - {}",
                self.matched_min_max_rx_time1_ns.0, self.matched_min_max_rx_time1_ns.1
            ),
            format!(
                "{} - {}",
                self.matched_min_max_trade_time1_ms.0, self.matched_min_max_trade_time1_ms.1
            )
        );
    }
}

fn percentile_f64(samples: &[f64], percentile: f64) -> f64 {
    let idx = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
    samples[idx]
}

fn avg_f64(samples: &[f64]) -> f64 {
    samples.iter().sum::<f64>() / samples.len() as f64
}

fn delta_ms(later_ns: u128, earlier_ns: u128) -> f64 {
    (later_ns as f64 - earlier_ns as f64) / NS_PER_MS as f64
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
        .map_err(Into::into)
}

fn parse_next_u32<'a>(parts: &mut impl Iterator<Item = &'a str>, name: &str) -> eyre::Result<u32> {
    parts
        .next()
        .ok_or_else(|| eyre::eyre!("timestamp missing {name}"))?
        .parse()
        .map_err(Into::into)
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
