use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::atomic::{AtomicBool, Ordering},
    thread::JoinHandle,
    time::Duration
};

use hyperliquid_db_core::{
    types::{
        HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, ParsedDataPipelineMeta,
        Trade
    },
    utils::{NS_PER_MS, unix_timestamp}
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
                    cache.new_public_trades(trades, rx_timestamp_ns);
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
        let mut implemented_stream = spawn_hl_watcher(HyperliquidDataKind::Trades)?;
        let mut cache = TradeCache::new("pipeline");

        loop {
            let data = match implemented_stream.blocking_recv() {
                Ok(data) => data
                    .as_ref()
                    .as_ref()
                    .map_err(|e| eyre::eyre!("{e:?}"))?
                    .clone(),
                Err(err) => {
                    return Err(eyre::eyre!("implemented stream channel disconnected - {err:?}"));
                }
            };

            let stream_received_at_ns = unix_timestamp().as_nanos();
            let HyperliquidData::Trades(trades) = data;
            cache.new_pipeline_trades(trades, stream_received_at_ns);

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

    fn new_public_trades(&mut self, trades: Vec<Trade>, rx_timestamp_ns: u128) {
        trades.into_iter().for_each(|trade| {
            if &trade.coin == TRADES_COIN {
                self.trades.push(TimestampedTrade {
                    rx_timestamp_ns,
                    trade: HyperliquidDataWithMeta {
                        data:          trade,
                        pipeline_meta: ParsedDataPipelineMeta::default()
                    },
                    has_pipeline_meta: false
                });
            }
        });
    }

    fn new_pipeline_trades(
        &mut self,
        trades: Vec<HyperliquidDataWithMeta<Trade>>,
        stream_received_at_ns: u128
    ) {
        trades.into_iter().for_each(|trade| {
            if &trade.data.coin == TRADES_COIN {
                self.trades.push(TimestampedTrade {
                    rx_timestamp_ns: stream_received_at_ns,
                    trade,
                    has_pipeline_meta: true
                });
            }
        });
    }
}

#[derive(Debug, Clone)]
struct TimestampedTrade {
    rx_timestamp_ns:   u128,
    trade:             HyperliquidDataWithMeta<Trade>,
    has_pipeline_meta: bool
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
    pipeline_stats: Option<PipelineStats>
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
struct PipelineStats {
    trade_time_to_latest_notify:    LatencyStats,
    latest_notify_to_processing:    LatencyStats,
    processing_duration:            LatencyStats,
    processed_to_bench_receive:     LatencyStats,
    latest_notify_to_bench_receive: LatencyStats,
    trade_time_to_processed:        LatencyStats,
    trade_time_to_bench_receive:    LatencyStats
}

impl PipelineStats {
    fn new(similiar_trades: &[(TimestampedTrade, TimestampedTrade)]) -> Option<Self> {
        let mut trade_time_to_latest_notify = Vec::new();
        let mut latest_notify_to_processing = Vec::new();
        let mut processing_duration = Vec::new();
        let mut processed_to_bench_receive = Vec::new();
        let mut latest_notify_to_bench_receive = Vec::new();
        let mut trade_time_to_processed = Vec::new();
        let mut trade_time_to_bench_receive = Vec::new();

        for (_, pipeline_trade) in similiar_trades {
            if !pipeline_trade.has_pipeline_meta {
                continue;
            }

            let trade_time_ns = pipeline_trade.trade.data.time as u128 * NS_PER_MS;
            let pipeline_meta = &pipeline_trade.trade.pipeline_meta;

            trade_time_to_latest_notify
                .push(delta_ms(pipeline_meta.latest_notification_received_at_ns, trade_time_ns));
            latest_notify_to_processing.push(delta_ms(
                pipeline_meta.processing_data_at_ns,
                pipeline_meta.latest_notification_received_at_ns
            ));
            processing_duration.push(delta_ms(
                pipeline_meta.processed_data_at_ns,
                pipeline_meta.processing_data_at_ns
            ));
            processed_to_bench_receive
                .push(delta_ms(pipeline_trade.rx_timestamp_ns, pipeline_meta.processed_data_at_ns));
            latest_notify_to_bench_receive.push(delta_ms(
                pipeline_trade.rx_timestamp_ns,
                pipeline_meta.latest_notification_received_at_ns
            ));
            trade_time_to_processed
                .push(delta_ms(pipeline_meta.processed_data_at_ns, trade_time_ns));
            trade_time_to_bench_receive
                .push(delta_ms(pipeline_trade.rx_timestamp_ns, trade_time_ns));
        }

        if trade_time_to_latest_notify.is_empty() {
            return None;
        }

        Some(Self {
            trade_time_to_latest_notify:    LatencyStats::new(trade_time_to_latest_notify),
            latest_notify_to_processing:    LatencyStats::new(latest_notify_to_processing),
            processing_duration:            LatencyStats::new(processing_duration),
            processed_to_bench_receive:     LatencyStats::new(processed_to_bench_receive),
            latest_notify_to_bench_receive: LatencyStats::new(latest_notify_to_bench_receive),
            trade_time_to_processed:        LatencyStats::new(trade_time_to_processed),
            trade_time_to_bench_receive:    LatencyStats::new(trade_time_to_bench_receive)
        })
    }

    fn stage_rows(&self) -> [(&'static str, LatencyStats); 7] {
        [
            ("trade time -> latest notify", self.trade_time_to_latest_notify),
            ("latest notify -> processing", self.latest_notify_to_processing),
            ("processing duration", self.processing_duration),
            ("processed -> bench receive", self.processed_to_bench_receive),
            ("latest notify -> bench receive", self.latest_notify_to_bench_receive),
            ("trade time -> processed", self.trade_time_to_processed),
            ("trade time -> bench receive", self.trade_time_to_bench_receive)
        ]
    }
}

impl TradeTimeComparionMetrics {
    fn compare_trade_caches(cache0: TradeCache, cache1: TradeCache) -> Self {
        let mut cache0_trades_by_key = cache0
            .trades
            .iter()
            .map(|trade| (trade.trade.data.clone(), trade.clone()))
            .collect::<HashMap<_, _>>();

        let mut similiar_trades = Vec::new();

        cache1.trades.iter().for_each(|trade| {
            if let Some(cach0_trade) = cache0_trades_by_key.remove(&trade.trade.data) {
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
                - (trade0.trade.data.time as u128 * NS_PER_MS) as f64)
                / NS_PER_MS as f64;
            let lag1_ms = (trade1.rx_timestamp_ns as f64
                - (trade1.trade.data.time as u128 * NS_PER_MS) as f64)
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
            .map(|trade| trade.trade.data.time)
            .min()
            .unwrap();
        let max_first_trade_time0_ms = cache0
            .trades
            .iter()
            .map(|trade| trade.trade.data.time)
            .max()
            .unwrap();
        let min_first_trade_time1_ms = cache1
            .trades
            .iter()
            .map(|trade| trade.trade.data.time)
            .min()
            .unwrap();
        let max_first_trade_time1_ms = cache1
            .trades
            .iter()
            .map(|trade| trade.trade.data.time)
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
            .map(|(trade0, _)| trade0.trade.data.time)
            .min()
            .unwrap();
        let matched_max_first_trade_time0_ms = similiar_trades
            .iter()
            .map(|(trade0, _)| trade0.trade.data.time)
            .max()
            .unwrap();
        let matched_min_first_trade_time1_ms = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.trade.data.time)
            .min()
            .unwrap();
        let matched_max_first_trade_time1_ms = similiar_trades
            .iter()
            .map(|(_, trade1)| trade1.trade.data.time)
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
            pipeline_stats: PipelineStats::new(&similiar_trades)
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
        if let Some(pipeline_stats) = self.pipeline_stats {
            println!();
            println!("implemented pipeline for matched trades");
            println!(
                "{:<34} {:>12} {:>12} {:>12} {:>12} {:>25}",
                "stage", "avg ms", "p50 ms", "p95 ms", "max ms", "first -> last avg ms"
            );
            for (name, stats) in pipeline_stats.stage_rows() {
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
