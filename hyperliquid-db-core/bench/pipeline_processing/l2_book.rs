use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::atomic::{AtomicBool, Ordering},
    thread::JoinHandle,
    time::Duration
};

use hyperliquid_db_core::{
    types::{
        HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, L2Book, L2BookLevel,
        ParsedDataPipelineMeta
    },
    utils::{NS_PER_MS, unix_timestamp}
};
use serde::Deserialize;
use tokio::sync::broadcast::error::TryRecvError;

use crate::utils::{set_hl_websocket_read_timeout, spawn_hl_trades_websocket, spawn_hl_watcher};

const TIMEOUT_SECS: u64 = 60;
const PUBLIC_WS_READ_TIMEOUT_MS: u64 = 100;
const L2_BOOK_COIN: &str = "BTC";
const L2_COMPARISON_DEPTH: usize = 20;
const L2_MATCH_TIME_TOLERANCE_MS: u64 = 1_000;
const DECIMAL_MULTIPLIER: f64 = 100_000_000.0;
const PIPELINE_EMPTY_LOG_POLLS: u64 = 10;
static IS_RUNNING: AtomicBool = AtomicBool::new(true);

pub fn run_l2_book_ws_bench() -> eyre::Result<()> {
    IS_RUNNING.store(true, Ordering::Release);
    println!("subscribing to public l2 book websocket for {L2_BOOK_COIN}");

    let public_ws_stream_handle = run_public_ws_stream();
    let implemented_stream_handle = run_implemented_stream();

    let timeout = std::env::var("TIMEOUT_SECS")
        .unwrap_or_else(|_| TIMEOUT_SECS.to_string())
        .parse()
        .unwrap();
    println!("collecting samples for {timeout} seconds (override with TIMEOUT_SECS)");
    std::thread::sleep(Duration::from_secs(timeout));
    IS_RUNNING.store(false, Ordering::Release);
    println!("collection window elapsed; stopping streams");

    let public_ws_stream = public_ws_stream_handle
        .join()
        .map_err(|_| eyre::eyre!("public websocket stream thread panicked"))??;
    let implemented_stream = implemented_stream_handle
        .join()
        .map_err(|_| eyre::eyre!("implemented stream thread panicked"))??;

    println!(
        "collected {} public ws l2 books and {} pipeline l2 books",
        public_ws_stream.l2_books.len(),
        implemented_stream.l2_books.len()
    );

    let comparison =
        L2BookTimeComparionMetrics::compare_l2_book_caches(public_ws_stream, implemented_stream);

    comparison.pretty_print();

    Ok(())
}

fn run_public_ws_stream() -> JoinHandle<eyre::Result<L2BookCache>> {
    std::thread::spawn(move || {
        println!("[l2 bench][public ws] connecting websocket");
        let subscription = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": L2_BOOK_COIN
            }
        });
        let mut public_ws_stream = spawn_hl_trades_websocket(L2_BOOK_COIN, subscription)?;
        set_hl_websocket_read_timeout(
            &mut public_ws_stream,
            Some(Duration::from_millis(PUBLIC_WS_READ_TIMEOUT_MS))
        )?;
        println!("[l2 bench][public ws] subscribed to {L2_BOOK_COIN} l2Book");

        let mut cache = L2BookCache::new("public ws");

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
                if message.channel == "l2Book" {
                    let l2_book = serde_json::from_value(message.data)?;
                    cache.new_public_l2_book(l2_book, rx_timestamp_ns);
                    let count = cache.l2_books.len();
                    if count == 1 || count % 10 == 0 {
                        let latest_time = cache
                            .l2_books
                            .last()
                            .map(|l2_book| l2_book.l2_book.data.time);
                        println!(
                            "[l2 bench][public ws] collected {count} {L2_BOOK_COIN} books \
                             latest_time={latest_time:?}"
                        );
                    }
                }
            }

            if !IS_RUNNING.load(Ordering::Relaxed) {
                break
            }
        }

        println!(
            "[l2 bench][public ws] stopped with {} {L2_BOOK_COIN} books",
            cache.l2_books.len()
        );
        Ok(cache)
    })
}

fn run_implemented_stream() -> JoinHandle<eyre::Result<L2BookCache>> {
    std::thread::spawn(move || {
        println!("[l2 bench][pipeline] spawning HyperliquidDataManager watcher for L2Book");
        let mut implemented_stream = spawn_hl_watcher(HyperliquidDataKind::L2Book)?;
        println!(
            "[l2 bench][pipeline] watcher subscribed; waiting for derived {L2_BOOK_COIN} l2 books"
        );
        let mut cache = L2BookCache::new("pipeline");
        let mut empty_polls = 0;
        let mut received_messages = 0;
        let mut non_l2_messages = 0;

        loop {
            let data = match implemented_stream.try_recv() {
                Ok(data) => {
                    empty_polls = 0;
                    received_messages += 1;
                    data.as_ref()
                        .as_ref()
                        .map_err(|e| eyre::eyre!("{e:?}"))?
                        .clone()
                }
                Err(TryRecvError::Empty) => {
                    if !IS_RUNNING.load(Ordering::Relaxed) {
                        break;
                    }
                    empty_polls += 1;
                    if empty_polls == 1 || empty_polls % PIPELINE_EMPTY_LOG_POLLS == 0 {
                        println!(
                            "[l2 bench][pipeline] no derived messages yet after {:.1}s; needs \
                             live /root/hl/data order-status/raw-book-diff writes and snapshot \
                             service on localhost:3001",
                            empty_polls as f64 * PUBLIC_WS_READ_TIMEOUT_MS as f64 / 1_000.0
                        );
                    }
                    std::thread::sleep(Duration::from_millis(PUBLIC_WS_READ_TIMEOUT_MS));
                    continue;
                }
                Err(TryRecvError::Closed) => {
                    return Err(eyre::eyre!("implemented stream channel disconnected"));
                }
                Err(TryRecvError::Lagged(skipped)) => {
                    return Err(eyre::eyre!(
                        "implemented stream lagged and skipped {skipped} messages"
                    ));
                }
            };

            let stream_received_at_ns = unix_timestamp().as_nanos();
            let HyperliquidData::L2Book(l2_books) = data else {
                non_l2_messages += 1;
                println!(
                    "[l2 bench][pipeline] received non-L2 pipeline message \
                     non_l2_messages={non_l2_messages}"
                );
                continue;
            };
            println!(
                "[l2 bench][pipeline] received L2 batch with {} books \
                 received_messages={received_messages}",
                l2_books.len()
            );
            cache.new_pipeline_l2_books(l2_books, stream_received_at_ns);
            let count = cache.l2_books.len();
            if count == 0 {
                println!(
                    "[l2 bench][pipeline] L2 batch did not contain {L2_BOOK_COIN}; cached count \
                     remains 0"
                );
            } else if count == 1 || count % 10 == 0 {
                let latest_time = cache
                    .l2_books
                    .last()
                    .map(|l2_book| l2_book.l2_book.data.time);
                println!(
                    "[l2 bench][pipeline] cached {count} {L2_BOOK_COIN} books \
                     latest_time={latest_time:?}"
                );
            }

            if !IS_RUNNING.load(Ordering::Relaxed) {
                break
            }
        }
        println!(
            "[l2 bench][pipeline] stopped with {} {L2_BOOK_COIN} books from {received_messages} \
             pipeline messages ({non_l2_messages} non-L2)",
            cache.l2_books.len()
        );
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
struct L2BookCache {
    name:     &'static str,
    l2_books: Vec<TimestampedL2Book>
}

impl L2BookCache {
    fn new(name: &'static str) -> Self {
        Self { name, l2_books: Vec::new() }
    }

    fn new_public_l2_book(&mut self, l2_book: L2Book, rx_timestamp_ns: u128) {
        if &l2_book.coin == L2_BOOK_COIN {
            self.l2_books.push(TimestampedL2Book {
                rx_timestamp_ns,
                l2_book: HyperliquidDataWithMeta {
                    data:          l2_book,
                    pipeline_meta: ParsedDataPipelineMeta::default()
                },
                has_pipeline_meta: false
            });
        }
    }

    fn new_pipeline_l2_books(
        &mut self,
        l2_books: Vec<HyperliquidDataWithMeta<L2Book>>,
        stream_received_at_ns: u128
    ) {
        l2_books.into_iter().for_each(|l2_book| {
            if &l2_book.data.coin == L2_BOOK_COIN {
                self.l2_books.push(TimestampedL2Book {
                    rx_timestamp_ns: stream_received_at_ns,
                    l2_book,
                    has_pipeline_meta: true
                });
            }
        });
    }
}

#[derive(Debug, Clone)]
struct TimestampedL2Book {
    rx_timestamp_ns:   u128,
    l2_book:           HyperliquidDataWithMeta<L2Book>,
    has_pipeline_meta: bool
}

#[derive(Debug)]
struct L2BookTimeComparionMetrics {
    cache0: &'static str,
    cache1: &'static str,
    l2_books0: usize,
    l2_books1: usize,
    total_similiar_l2_books: usize,
    latency_lag0_ms: LatencyStats,
    latency_lag1_ms: LatencyStats,
    diff_latency_lag_ms: LatencyStats,
    min_max_first_rx_time0_ns: (u128, u128),
    min_max_first_rx_time1_ns: (u128, u128),
    min_max_first_l2_book_time0_ms: (u64, u64),
    min_max_first_l2_book_time1_ms: (u64, u64),
    matched_min_max_rx_time0_ns: (u128, u128),
    matched_min_max_rx_time1_ns: (u128, u128),
    matched_min_max_l2_book_time0_ms: (u64, u64),
    matched_min_max_l2_book_time1_ms: (u64, u64),
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
    l2_book_time_to_latest_notify:  LatencyStats,
    latest_notify_to_processing:    LatencyStats,
    processing_duration:            LatencyStats,
    processed_to_bench_receive:     LatencyStats,
    latest_notify_to_bench_receive: LatencyStats,
    l2_book_time_to_processed:      LatencyStats,
    l2_book_time_to_bench_receive:  LatencyStats
}

impl PipelineStats {
    fn new(similiar_l2_books: &[(TimestampedL2Book, TimestampedL2Book)]) -> Option<Self> {
        let mut l2_book_time_to_latest_notify = Vec::new();
        let mut latest_notify_to_processing = Vec::new();
        let mut processing_duration = Vec::new();
        let mut processed_to_bench_receive = Vec::new();
        let mut latest_notify_to_bench_receive = Vec::new();
        let mut l2_book_time_to_processed = Vec::new();
        let mut l2_book_time_to_bench_receive = Vec::new();

        for (_, pipeline_l2_book) in similiar_l2_books {
            if !pipeline_l2_book.has_pipeline_meta {
                continue;
            }

            let l2_book_time_ns = pipeline_l2_book.l2_book.data.time as u128 * NS_PER_MS;
            let pipeline_meta = &pipeline_l2_book.l2_book.pipeline_meta;

            l2_book_time_to_latest_notify
                .push(delta_ms(pipeline_meta.latest_notification_received_at_ns, l2_book_time_ns));
            latest_notify_to_processing.push(delta_ms(
                pipeline_meta.processing_data_at_ns,
                pipeline_meta.latest_notification_received_at_ns
            ));
            processing_duration.push(delta_ms(
                pipeline_meta.processed_data_at_ns,
                pipeline_meta.processing_data_at_ns
            ));
            processed_to_bench_receive.push(delta_ms(
                pipeline_l2_book.rx_timestamp_ns,
                pipeline_meta.processed_data_at_ns
            ));
            latest_notify_to_bench_receive.push(delta_ms(
                pipeline_l2_book.rx_timestamp_ns,
                pipeline_meta.latest_notification_received_at_ns
            ));
            l2_book_time_to_processed
                .push(delta_ms(pipeline_meta.processed_data_at_ns, l2_book_time_ns));
            l2_book_time_to_bench_receive
                .push(delta_ms(pipeline_l2_book.rx_timestamp_ns, l2_book_time_ns));
        }

        if l2_book_time_to_latest_notify.is_empty() {
            return None;
        }

        Some(Self {
            l2_book_time_to_latest_notify:  LatencyStats::new(l2_book_time_to_latest_notify),
            latest_notify_to_processing:    LatencyStats::new(latest_notify_to_processing),
            processing_duration:            LatencyStats::new(processing_duration),
            processed_to_bench_receive:     LatencyStats::new(processed_to_bench_receive),
            latest_notify_to_bench_receive: LatencyStats::new(latest_notify_to_bench_receive),
            l2_book_time_to_processed:      LatencyStats::new(l2_book_time_to_processed),
            l2_book_time_to_bench_receive:  LatencyStats::new(l2_book_time_to_bench_receive)
        })
    }

    fn stage_rows(&self) -> [(&'static str, LatencyStats); 7] {
        [
            ("l2 book time -> latest notify", self.l2_book_time_to_latest_notify),
            ("latest notify -> processing", self.latest_notify_to_processing),
            ("processing duration", self.processing_duration),
            ("processed -> bench receive", self.processed_to_bench_receive),
            ("latest notify -> bench receive", self.latest_notify_to_bench_receive),
            ("l2 book time -> processed", self.l2_book_time_to_processed),
            ("l2 book time -> bench receive", self.l2_book_time_to_bench_receive)
        ]
    }
}

impl L2BookTimeComparionMetrics {
    fn compare_l2_book_caches(cache0: L2BookCache, cache1: L2BookCache) -> Self {
        let print_fn = |full_book: &L2BookCache| {
            for book in &full_book.l2_books {
                println!("{}\n\n", book.l2_book.data.format_pretty());
            }
        };

        println!("\n\n\n\n\n\n\n\n----------- CACHE 0 -----------");
        print_fn(&cache0);
        println!("\n\n\n\n\n\n\n\n----------- CACHE 1 -----------");
        print_fn(&cache1);

        panic!();

        let mut cache0_l2_books_by_key = l2_books_by_comparison_key(&cache0.l2_books);

        let mut similiar_l2_books = Vec::new();

        cache1.l2_books.iter().for_each(|l2_book| {
            if let Some(cach0_l2_book) = remove_l2_book_match(&mut cache0_l2_books_by_key, l2_book)
            {
                similiar_l2_books.push((cach0_l2_book.clone(), l2_book.clone()));
            }
        });

        let cache0_live_l2_books_len = live_l2_book_count(&cache0.l2_books);
        let cache1_live_l2_books_len = live_l2_book_count(&cache1.l2_books);
        let similiar_l2_books_len = similiar_l2_books.len();
        assert!(
            similiar_l2_books_len > 0,
            "no comparable live l2 books found with {} ms time tolerance - {} count: {}, live \
             comparable count: {}, time range: {:?}, live time range: {:?}, sample: {:?};\n\n {} \
             count: {}, live comparable count: {}, time range: {:?}, live time range: {:?}, \
             sample: {:?}",
            L2_MATCH_TIME_TOLERANCE_MS,
            cache0.name,
            cache0.l2_books.len(),
            cache0_live_l2_books_len,
            l2_book_time_range(&cache0.l2_books),
            live_l2_book_time_range(&cache0.l2_books),
            cache0.l2_books.first().map(|l2_book| &l2_book.l2_book.data),
            cache1.name,
            cache1.l2_books.len(),
            cache1_live_l2_books_len,
            l2_book_time_range(&cache1.l2_books),
            live_l2_book_time_range(&cache1.l2_books),
            cache1.l2_books.first().map(|l2_book| &l2_book.l2_book.data),
        );

        let mut latency_lag0_ms = Vec::with_capacity(similiar_l2_books_len);
        let mut latency_lag1_ms = Vec::with_capacity(similiar_l2_books_len);
        let mut diff_latency_lag_ms = Vec::with_capacity(similiar_l2_books_len);
        for (l2_book0, l2_book1) in &similiar_l2_books {
            let lag0_ms = (l2_book0.rx_timestamp_ns as f64
                - (l2_book0.l2_book.data.time as u128 * NS_PER_MS) as f64)
                / NS_PER_MS as f64;
            let lag1_ms = (l2_book1.rx_timestamp_ns as f64
                - (l2_book1.l2_book.data.time as u128 * NS_PER_MS) as f64)
                / NS_PER_MS as f64;

            latency_lag0_ms.push(lag0_ms);
            latency_lag1_ms.push(lag1_ms);
            diff_latency_lag_ms.push(lag0_ms - lag1_ms);
        }

        let min_first_rx_time0_ns = cache0
            .l2_books
            .iter()
            .map(|l2_book| l2_book.rx_timestamp_ns)
            .min()
            .unwrap();
        let max_first_rx_time0_ns = cache0
            .l2_books
            .iter()
            .map(|l2_book| l2_book.rx_timestamp_ns)
            .max()
            .unwrap();
        let min_first_rx_time1_ns = cache1
            .l2_books
            .iter()
            .map(|l2_book| l2_book.rx_timestamp_ns)
            .min()
            .unwrap();
        let max_first_rx_time1_ns = cache1
            .l2_books
            .iter()
            .map(|l2_book| l2_book.rx_timestamp_ns)
            .max()
            .unwrap();

        let min_first_l2_book_time0_ms = cache0
            .l2_books
            .iter()
            .map(|l2_book| l2_book.l2_book.data.time)
            .min()
            .unwrap();
        let max_first_l2_book_time0_ms = cache0
            .l2_books
            .iter()
            .map(|l2_book| l2_book.l2_book.data.time)
            .max()
            .unwrap();
        let min_first_l2_book_time1_ms = cache1
            .l2_books
            .iter()
            .map(|l2_book| l2_book.l2_book.data.time)
            .min()
            .unwrap();
        let max_first_l2_book_time1_ms = cache1
            .l2_books
            .iter()
            .map(|l2_book| l2_book.l2_book.data.time)
            .max()
            .unwrap();

        let matched_min_first_rx_time0_ns = similiar_l2_books
            .iter()
            .map(|(l2_book0, _)| l2_book0.rx_timestamp_ns)
            .min()
            .unwrap();
        let matched_max_first_rx_time0_ns = similiar_l2_books
            .iter()
            .map(|(l2_book0, _)| l2_book0.rx_timestamp_ns)
            .max()
            .unwrap();
        let matched_min_first_rx_time1_ns = similiar_l2_books
            .iter()
            .map(|(_, l2_book1)| l2_book1.rx_timestamp_ns)
            .min()
            .unwrap();
        let matched_max_first_rx_time1_ns = similiar_l2_books
            .iter()
            .map(|(_, l2_book1)| l2_book1.rx_timestamp_ns)
            .max()
            .unwrap();
        let matched_min_first_l2_book_time0_ms = similiar_l2_books
            .iter()
            .map(|(l2_book0, _)| l2_book0.l2_book.data.time)
            .min()
            .unwrap();
        let matched_max_first_l2_book_time0_ms = similiar_l2_books
            .iter()
            .map(|(l2_book0, _)| l2_book0.l2_book.data.time)
            .max()
            .unwrap();
        let matched_min_first_l2_book_time1_ms = similiar_l2_books
            .iter()
            .map(|(_, l2_book1)| l2_book1.l2_book.data.time)
            .min()
            .unwrap();
        let matched_max_first_l2_book_time1_ms = similiar_l2_books
            .iter()
            .map(|(_, l2_book1)| l2_book1.l2_book.data.time)
            .max()
            .unwrap();

        L2BookTimeComparionMetrics {
            cache0: cache0.name,
            cache1: cache1.name,
            l2_books0: cache0.l2_books.len(),
            l2_books1: cache1.l2_books.len(),
            total_similiar_l2_books: similiar_l2_books.len(),
            latency_lag0_ms: LatencyStats::new(latency_lag0_ms),
            latency_lag1_ms: LatencyStats::new(latency_lag1_ms),
            diff_latency_lag_ms: LatencyStats::new(diff_latency_lag_ms),
            min_max_first_rx_time0_ns: (min_first_rx_time0_ns, max_first_rx_time0_ns),
            min_max_first_rx_time1_ns: (min_first_rx_time1_ns, max_first_rx_time1_ns),
            min_max_first_l2_book_time0_ms: (
                min_first_l2_book_time0_ms,
                max_first_l2_book_time0_ms
            ),
            min_max_first_l2_book_time1_ms: (
                min_first_l2_book_time1_ms,
                max_first_l2_book_time1_ms
            ),
            matched_min_max_rx_time0_ns: (
                matched_min_first_rx_time0_ns,
                matched_max_first_rx_time0_ns
            ),
            matched_min_max_rx_time1_ns: (
                matched_min_first_rx_time1_ns,
                matched_max_first_rx_time1_ns
            ),
            matched_min_max_l2_book_time0_ms: (
                matched_min_first_l2_book_time0_ms,
                matched_max_first_l2_book_time0_ms
            ),
            matched_min_max_l2_book_time1_ms: (
                matched_min_first_l2_book_time1_ms,
                matched_max_first_l2_book_time1_ms
            ),
            pipeline_stats: PipelineStats::new(&similiar_l2_books)
        }
    }

    fn pretty_print(&self) {
        let comparable_l2_books = self.l2_books0.min(self.l2_books1);
        let match_rate = if comparable_l2_books == 0 {
            0.0
        } else {
            (self.total_similiar_l2_books as f64 / comparable_l2_books as f64) * 100.0
        };

        println!("l2 book websocket latency comparison");
        println!(
            "{:<18} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "stream", "l2 books", "avg ms", "p50 ms", "p95 ms", "max ms"
        );
        println!(
            "{:<18} {:>12} {:>12.3} {:>12.3} {:>12.3} {:>12.3}",
            self.cache0,
            self.l2_books0,
            self.latency_lag0_ms.avg_ms,
            self.latency_lag0_ms.p50_ms,
            self.latency_lag0_ms.p95_ms,
            self.latency_lag0_ms.max_ms
        );
        println!(
            "{:<18} {:>12} {:>12.3} {:>12.3} {:>12.3} {:>12.3}",
            self.cache1,
            self.l2_books1,
            self.latency_lag1_ms.avg_ms,
            self.latency_lag1_ms.p50_ms,
            self.latency_lag1_ms.p95_ms,
            self.latency_lag1_ms.max_ms
        );
        println!();
        println!("matched l2 books: {} ({match_rate:.2}%)", self.total_similiar_l2_books);
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
            println!("implemented pipeline for matched l2 books");
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
        println!("all observed l2 books");
        println!("{:<18} {:>35} {:>35}", "stream", "rx time range ns", "l2 book time range ms");
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache0,
            format!("{} - {}", self.min_max_first_rx_time0_ns.0, self.min_max_first_rx_time0_ns.1),
            format!(
                "{} - {}",
                self.min_max_first_l2_book_time0_ms.0, self.min_max_first_l2_book_time0_ms.1
            )
        );
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache1,
            format!("{} - {}", self.min_max_first_rx_time1_ns.0, self.min_max_first_rx_time1_ns.1),
            format!(
                "{} - {}",
                self.min_max_first_l2_book_time1_ms.0, self.min_max_first_l2_book_time1_ms.1
            )
        );
        println!();
        println!("matched l2 books only");
        println!("{:<18} {:>35} {:>35}", "stream", "rx time range ns", "l2 book time range ms");
        println!(
            "{:<18} {:>35} {:>35}",
            self.cache0,
            format!(
                "{} - {}",
                self.matched_min_max_rx_time0_ns.0, self.matched_min_max_rx_time0_ns.1
            ),
            format!(
                "{} - {}",
                self.matched_min_max_l2_book_time0_ms.0, self.matched_min_max_l2_book_time0_ms.1
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
                self.matched_min_max_l2_book_time1_ms.0, self.matched_min_max_l2_book_time1_ms.1
            )
        );
    }
}

fn percentile_f64(samples: &[f64], percentile: f64) -> f64 {
    let idx = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
    samples[idx]
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct L2BookComparisonKey {
    coin:   String,
    levels: [Vec<L2BookComparisonLevel>; 2]
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct L2BookComparisonLevel {
    px: u64,
    sz: u64,
    n:  u64
}

fn l2_book_comparison_key(l2_book: &L2Book) -> Option<L2BookComparisonKey> {
    if l2_book.time == 0 {
        return None;
    }

    Some(L2BookComparisonKey {
        coin:   l2_book.coin.clone(),
        levels: [
            l2_book_comparison_levels(l2_book.bids())?,
            l2_book_comparison_levels(l2_book.asks())?
        ]
    })
}

fn l2_books_by_comparison_key(
    l2_books: &[TimestampedL2Book]
) -> HashMap<L2BookComparisonKey, Vec<TimestampedL2Book>> {
    let mut by_key = HashMap::new();

    for l2_book in l2_books {
        let Some(key) = l2_book_comparison_key(&l2_book.l2_book.data) else {
            continue;
        };
        by_key
            .entry(key)
            .or_insert_with(Vec::new)
            .push(l2_book.clone());
    }

    by_key
}

fn remove_l2_book_match(
    candidates_by_key: &mut HashMap<L2BookComparisonKey, Vec<TimestampedL2Book>>,
    target: &TimestampedL2Book
) -> Option<TimestampedL2Book> {
    let key = l2_book_comparison_key(&target.l2_book.data)?;
    let target_time = target.l2_book.data.time;

    let (matched, remove_key) = {
        let candidates = candidates_by_key.get_mut(&key)?;
        let (idx, _) = candidates
            .iter()
            .enumerate()
            .filter_map(|(idx, candidate)| {
                let time_diff = candidate.l2_book.data.time.abs_diff(target_time);
                (time_diff <= L2_MATCH_TIME_TOLERANCE_MS).then_some((idx, time_diff))
            })
            .min_by_key(|(idx, time_diff)| (*time_diff, *idx))?;

        let matched = candidates.remove(idx);
        (matched, candidates.is_empty())
    };

    if remove_key {
        candidates_by_key.remove(&key);
    }

    Some(matched)
}

fn l2_book_comparison_levels(levels: &[L2BookLevel]) -> Option<Vec<L2BookComparisonLevel>> {
    levels
        .iter()
        .take(L2_COMPARISON_DEPTH)
        .map(|level| {
            Some(L2BookComparisonLevel {
                px: parse_decimal_comparison_value(&level.px)?,
                sz: parse_decimal_comparison_value(&level.sz)?,
                n:  level.n
            })
        })
        .collect()
}

fn parse_decimal_comparison_value(value: &str) -> Option<u64> {
    Some((value.parse::<f64>().ok()? * DECIMAL_MULTIPLIER).round() as u64)
}

fn avg_f64(samples: &[f64]) -> f64 {
    samples.iter().sum::<f64>() / samples.len() as f64
}

fn delta_ms(later_ns: u128, earlier_ns: u128) -> f64 {
    (later_ns as f64 - earlier_ns as f64) / NS_PER_MS as f64
}

fn l2_book_time_range(l2_books: &[TimestampedL2Book]) -> Option<(u64, u64)> {
    let min = l2_books
        .iter()
        .map(|l2_book| l2_book.l2_book.data.time)
        .min()?;
    let max = l2_books
        .iter()
        .map(|l2_book| l2_book.l2_book.data.time)
        .max()?;
    Some((min, max))
}

fn live_l2_book_time_range(l2_books: &[TimestampedL2Book]) -> Option<(u64, u64)> {
    let min = l2_books
        .iter()
        .filter_map(|l2_book| {
            (l2_book_comparison_key(&l2_book.l2_book.data).is_some())
                .then_some(l2_book.l2_book.data.time)
        })
        .min()?;
    let max = l2_books
        .iter()
        .filter_map(|l2_book| {
            (l2_book_comparison_key(&l2_book.l2_book.data).is_some())
                .then_some(l2_book.l2_book.data.time)
        })
        .max()?;
    Some((min, max))
}

fn live_l2_book_count(l2_books: &[TimestampedL2Book]) -> usize {
    l2_books
        .iter()
        .filter(|l2_book| l2_book_comparison_key(&l2_book.l2_book.data).is_some())
        .count()
}
