use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc
    },
    thread::JoinHandle
};

use hyperliquid_db::{
    constructed_data::{HyperliquidDataDeriver, TradeDeriver, types::Trade},
    hl_fs::HyperliquidDataDirKind
};
use itertools::Itertools;

use crate::ws_streams::utils::{spawn_hl_watcher, spawn_hl_websocket, timestamp_utc};

const TIMEOUT_SECS: u64 = 30;
static IS_RUNNING: AtomicBool = AtomicBool::new(true);

pub fn run_trades_ws_bench() {
    let public_ws_stream_handle = run_public_ws_stream();
    let implemented_stream_handle = run_implemented_stream();

    std::thread::sleep(std::time::Duration::from_secs(TIMEOUT_SECS));
    IS_RUNNING.store(false, Ordering::Release);

    let public_ws_stream = public_ws_stream_handle.join().unwrap().unwrap();
    let implemented_stream = implemented_stream_handle.join().unwrap().unwrap();

    let comparison =
        TradeTimeComparionMetrics::compare_trade_caches(public_ws_stream, implemented_stream);

    comparison.pretty_print();
}

fn run_public_ws_stream() -> JoinHandle<eyre::Result<TradeCache>> {
    std::thread::spawn(move || {
        let mut public_ws_stream = spawn_hl_websocket("trades")?;

        let mut cache = TradeCache::new("public ws");

        loop {
            let value = public_ws_stream.read()?;
            if value.is_text() {
                let trades: Vec<Trade> = serde_json::from_str(&value.to_text()?)?;
                cache.new_trades(trades);
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
            let data = implemented_stream.recv()??;

            let trades = match data.name {
                HyperliquidDataDirKind::NodeFills => deriver.handle_raw_data(data)?,
                _ => unreachable!()
            };

            cache.new_trades(trades);

            if !IS_RUNNING.load(Ordering::Relaxed) {
                break
            }
        }
        Ok(cache)
    })
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

    fn new_trade(&mut self, trade: Trade) {
        self.trades
            .push(TimestampedTrade { rx_timestamp_ms: timestamp_utc().as_millis(), trade });
    }

    fn new_trades(&mut self, trades: Vec<Trade>) {
        let rx_timestamp_ms = timestamp_utc().as_millis();
        trades.into_iter().for_each(|trade| {
            self.trades
                .push(TimestampedTrade { rx_timestamp_ms, trade });
        });
    }
}

#[derive(Debug)]
struct TimestampedTrade {
    rx_timestamp_ms: u128,
    trade:           Trade
}

#[derive(Debug)]
struct TradeTimeComparionMetrics {
    cache0:                  &'static str,
    cache1:                  &'static str,
    trades0:                 usize,
    trades1:                 usize,
    total_similiar_trades:   usize,
    avg_latency_lag0_ms:     f64,
    avg_latency_lag1_ms:     f64,
    avg_diff_latency_lag_ms: f64
}

impl TradeTimeComparionMetrics {
    fn compare_trade_caches(cache0: TradeCache, cache1: TradeCache) -> Self {
        let mut cache0_trades_by_key = cache0
            .trades
            .iter()
            .map(|trade| (trade.trade.hash.clone(), trade.clone()))
            .collect::<HashMap<_, _>>();

        let mut similiar_trades = Vec::new();

        cache1.trades.iter().for_each(|trade| {
            if let Some(cach0_trade) = cache0_trades_by_key.remove(&trade.trade.hash) {
                assert_eq!(trade.trade, cach0_trade.trade);
                similiar_trades.push((cach0_trade.clone(), trade.clone()));
            }
        });

        let similiar_trades_len = similiar_trades.len();
        let (avg_latency_lag0_ms, avg_latency_lag1_ms, avg_diff_latency_lag_ms) =
            similiar_trades.iter().fold(
                (0.0, 0.0, 0.0),
                |(
                    mut avg_latency_lag0_ms,
                    mut avg_latency_lag1_ms,
                    mut avg_diff_latency_lag_ms
                ),
                 (trade0, trade1)| {
                    let latency_lag0_ms =
                        (trade0.rx_timestamp_ms - trade0.trade.time as u128) as f64;
                    let latency_lag1_ms =
                        (trade1.rx_timestamp_ms - trade1.trade.time as u128) as f64;
                    let diff_latency_lag_ms = (latency_lag0_ms - latency_lag1_ms);

                    avg_latency_lag0_ms += (latency_lag0_ms / similiar_trades_len as f64);
                    avg_latency_lag1_ms += (latency_lag1_ms / similiar_trades_len as f64);
                    avg_diff_latency_lag_ms += (diff_latency_lag_ms / similiar_trades_len as f64);

                    (avg_latency_lag0_ms, avg_latency_lag1_ms, avg_diff_latency_lag_ms)
                }
            );

        TradeTimeComparionMetrics {
            cache0: cache0.name,
            cache1: cache1.name,
            trades0: cache0.trades.len(),
            trades1: cache1.trades.len(),
            total_similiar_trades: similiar_trades.len(),
            avg_latency_lag0_ms,
            avg_latency_lag1_ms,
            avg_diff_latency_lag_ms
        }
    }

    fn pretty_print(&self) {
        println!("{self:?}")
    }
}
