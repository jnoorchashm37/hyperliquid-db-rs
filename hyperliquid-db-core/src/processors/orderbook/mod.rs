use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::Arc
};

pub mod book;
pub mod snapshots;
pub mod types;
pub mod utils;

use self::snapshots::{StateSnapshot, StateSnapshotFetcher};
use crate::{
    fs_handlers::types::FsOutData,
    hl_fs::{
        HyperliquidDirData, HyperliquidDirDataWithMeta,
        schemas::{NodeOrderStatusesRows, NodeRawBookDiffsRows}
    },
    processors::{
        HyperliquidDataProcessorHandle,
        orderbook::{
            book::OrderBook,
            types::{Coin, InnerL4Order, Sz},
            utils::coin_to_book_updates
        }
    },
    types::{
        HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, L2Book, L4Book, L4BookDiff,
        L4OrderDiff, L4OrderStatus, ParsedDataPipelineMeta
    },
    utils::unix_timestamp
};

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const PRICE_MULTIPLIER: f64 = 100_000_000.0;
const FETCH_SNAPSHOT_SLEEP_TIME_SEC: u64 = 5;
// Streaming rows for a block can arrive after later blocks are observed.
const STREAMING_FINALIZATION_BLOCK_DELAY: u64 = 64;

#[derive(Default)]
pub struct OrderBookDeriver {
    order_status_cache:          BatchQueue<L4OrderStatus>,
    book_diff_cache:             BatchQueue<L4BookDiff>,
    streaming_block_cache:       StreamingBlockCache,
    order_books:                 BTreeMap<String, OrderBook>,
    state_snapshot:              StateSnapshotFetcher,
    snapshot_height:             Option<u64>,
    book_time:                   u64,
    snapshots_pending:           bool,
    ignore_spot:                 bool,
    logged_waiting_for_snapshot: bool,
    outputs:                     OrderBookOutputs
}

impl OrderBookDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn for_data_kinds(data_kinds: &[HyperliquidDataKind]) -> Self {
        Self { outputs: OrderBookOutputs::for_data_kinds(data_kinds), ..Default::default() }
    }

    pub fn with_ignore_spot(ignore_spot: bool) -> Self {
        Self { ignore_spot, ..Default::default() }
    }

    pub fn order_count(&self) -> usize {
        self.order_books.values().map(OrderBook::order_count).sum()
    }

    pub fn pending_batch_count(&self) -> usize {
        self.pending_order_status_batch_count() + self.pending_book_diff_batch_count()
    }

    fn pending_order_status_batch_count(&self) -> usize {
        self.order_status_cache.len() + self.streaming_block_cache.order_status_batch_count()
    }

    fn pending_book_diff_batch_count(&self) -> usize {
        self.book_diff_cache.len() + self.streaming_block_cache.book_diff_batch_count()
    }

    pub fn is_ready(&self) -> bool {
        self.snapshot_height.is_some()
    }

    pub fn compute_snapshots(&self) -> Option<Vec<L4Book>> {
        self.snapshot_height.map(|height| {
            self.order_books
                .iter()
                .map(|(coin, book)| L4Book::Snapshot {
                    coin: coin.clone(),
                    time: self.book_time,
                    height,
                    levels: book.to_l4_snapshot()
                })
                .collect()
        })
    }

    pub fn compute_l2_snapshots(&self) -> Option<Vec<L2Book>> {
        self.snapshot_height.map(|_| {
            self.order_books
                .iter()
                .map(|(coin, book)| L2Book {
                    coin:   coin.clone(),
                    time:   self.book_time,
                    levels: book.to_l2_snapshot()
                })
                .collect()
        })
    }

    fn try_initialize_from_snapshot(&mut self) -> eyre::Result<bool> {
        if self.is_ready() {
            return Ok(true);
        }

        let Some(StateSnapshot { height, snapshots }) = self.state_snapshot.write(Option::take)?
        else {
            if !self.logged_waiting_for_snapshot {
                println!(
                    "[orderbook deriver] waiting for state snapshot before emitting books; \
                     pending_order_status_batches={} pending_book_diff_batches={}",
                    self.pending_order_status_batch_count(),
                    self.pending_book_diff_batch_count()
                );
                self.logged_waiting_for_snapshot = true;
            }
            return Ok(false);
        };
        self.logged_waiting_for_snapshot = false;
        println!(
            "[orderbook deriver] initializing from snapshot height={height}; \
             pending_order_status_batches={} pending_book_diff_batches={}",
            self.pending_order_status_batch_count(),
            self.pending_book_diff_batch_count()
        );

        self.order_books = snapshots.into_orderbooks(self.ignore_spot)?;
        self.snapshot_height = Some(height);
        self.book_time = 0;
        self.push_ready_streaming_batches()?;

        if let Err(error) = self.apply_cached_batches() {
            println!(
                "[orderbook deriver] failed to apply cached batches after snapshot \
                 height={height}; waiting for next snapshot: {error:?}"
            );
            tracing::info!(
                ?error,
                "Failed to apply updates to this book (likely missing older updates). Waiting for \
                 next snapshot."
            );
            self.reset_after_apply_error();
            return Ok(false);
        }

        self.snapshots_pending = true;
        tracing::info!(
            snapshot_height = height,
            current_height = self.snapshot_height.unwrap_or(height),
            order_count = self.order_count(),
            "l4 order book ready"
        );
        println!(
            "[orderbook deriver] ready snapshot_height={} order_count={} outputs_l2={} \
             outputs_l4={}",
            self.snapshot_height.unwrap_or(height),
            self.order_count(),
            self.outputs.l2,
            self.outputs.l4
        );
        Ok(true)
    }

    fn reset_after_apply_error(&mut self) {
        self.order_status_cache = Default::default();
        self.book_diff_cache = Default::default();
        self.streaming_block_cache = Default::default();
        self.order_books.clear();
        self.snapshot_height = None;
        self.book_time = 0;
        self.snapshots_pending = false;
        self.logged_waiting_for_snapshot = false;
        self.state_snapshot.fetch_new();
    }

    fn receive_order_statuses(
        &mut self,
        rows: &[NodeOrderStatusesRows],
        fs_data: &Arc<FsOutData>
    ) -> eyre::Result<()> {
        for row in rows {
            let events = row
                .events
                .iter()
                .cloned()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?;
            self.streaming_block_cache.push_order_statuses(
                row.block_number,
                row.block_time_unix_ms()?,
                events,
                fs_data
            );
        }

        self.push_ready_streaming_batches()?;
        Ok(())
    }

    fn receive_book_diffs(
        &mut self,
        rows: &[NodeRawBookDiffsRows],
        fs_data: &Arc<FsOutData>
    ) -> eyre::Result<()> {
        for row in rows {
            self.streaming_block_cache.push_book_diffs(
                row.block_number,
                row.block_time_unix_ms()?,
                row.events
                    .iter()
                    .cloned()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                fs_data
            );
        }

        self.push_ready_streaming_batches()?;
        Ok(())
    }

    fn push_ready_streaming_batches(&mut self) -> eyre::Result<()> {
        let Some(snapshot_height) = self.snapshot_height else {
            return Ok(());
        };
        let next_block = snapshot_height
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("l4 order book snapshot height overflow"))?;
        self.streaming_block_cache.start_at(next_block);

        for (order_statuses, book_diffs) in self.streaming_block_cache.pop_ready_batches() {
            self.order_status_cache.push(order_statuses)?;
            self.book_diff_cache.push(book_diffs)?;
        }

        Ok(())
    }

    fn pop_cache(&mut self) -> Option<(CachedBatch<L4OrderStatus>, CachedBatch<L4BookDiff>)> {
        while let Some(diff_batch) = self.book_diff_cache.front() {
            let Some(status_batch) = self.order_status_cache.front() else {
                break;
            };

            match diff_batch.block_number.cmp(&status_batch.block_number) {
                Ordering::Less => {
                    self.book_diff_cache.pop_front();
                }
                Ordering::Equal => {
                    let status_batch = self.order_status_cache.pop_front()?;
                    let diff_batch = self.book_diff_cache.pop_front()?;
                    return Some((status_batch, diff_batch));
                }
                Ordering::Greater => {
                    self.order_status_cache.pop_front();
                }
            }
        }

        None
    }

    fn process_ready_batches(
        &mut self,
        processing_data_at_ns: u128
    ) -> eyre::Result<ProcessedOrderBookData> {
        let mut out = ProcessedOrderBookData::default();

        while let Some((order_statuses, book_diffs)) = self.pop_cache() {
            let applied = match self.apply_cached_batch(&order_statuses, &book_diffs) {
                Ok(applied) => applied,
                Err(error) => {
                    println!(
                        "[orderbook deriver] failed to apply live batch block={} \
                         current_height={:?}; waiting for next snapshot: {error:?}",
                        order_statuses.block_number, self.snapshot_height
                    );
                    tracing::info!(
                        ?error,
                        "Failed to apply updates to this book. Waiting for next snapshot."
                    );
                    self.reset_after_apply_error();
                    return Ok(ProcessedOrderBookData::default());
                }
            };

            if !applied {
                continue;
            }

            let mut pipeline_meta = order_statuses.pipeline_meta;
            pipeline_meta.latest_notification_received_at_ns = pipeline_meta
                .latest_notification_received_at_ns
                .max(book_diffs.pipeline_meta.latest_notification_received_at_ns);

            pipeline_meta.processing_data_at_ns = processing_data_at_ns;
            pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

            let l2_before = out.l2.len();
            let l4_before = out.l4.len();

            if self.outputs.l2 {
                out.l2.extend(self.l2_updates_for_book_diffs(
                    &book_diffs.events,
                    book_diffs.time,
                    pipeline_meta.clone()
                ));
            }

            if self.outputs.l4 {
                for update in coin_to_book_updates(
                    order_statuses.events,
                    book_diffs.events,
                    book_diffs.time,
                    book_diffs.block_number
                ) {
                    out.l4.push(HyperliquidDataWithMeta {
                        data:          L4Book::Updates(update),
                        pipeline_meta: pipeline_meta.clone()
                    });
                }
            }

            let l2_added = out.l2.len() - l2_before;
            let l4_added = out.l4.len() - l4_before;
            if l2_added > 0 || l4_added > 0 {
                println!(
                    "[orderbook deriver] applied block={} time={} l2_books_added={} \
                     l4_updates_added={} pending_order_status_batches={} \
                     pending_book_diff_batches={}",
                    book_diffs.block_number,
                    book_diffs.time,
                    l2_added,
                    l4_added,
                    self.order_status_cache.len(),
                    self.book_diff_cache.len()
                );
            }
        }

        Ok(out)
    }

    fn l2_updates_for_book_diffs(
        &self,
        book_diffs: &[L4BookDiff],
        time: u64,
        pipeline_meta: ParsedDataPipelineMeta
    ) -> Vec<HyperliquidDataWithMeta<L2Book>> {
        let mut coins = BTreeSet::new();
        for diff in book_diffs {
            if self.ignore_spot && Coin::new(&diff.coin).is_spot() {
                continue;
            }
            coins.insert(diff.coin.clone());
        }

        coins
            .into_iter()
            .filter_map(|coin| {
                self.order_books
                    .get(&coin)
                    .map(|book| HyperliquidDataWithMeta {
                        data:          L2Book { coin, levels: book.to_l2_snapshot(), time },
                        pipeline_meta: pipeline_meta.clone()
                    })
            })
            .collect()
    }

    fn process_pending_snapshots(
        &mut self,
        fs_data: &Arc<FsOutData>,
        processing_data_at_ns: u128
    ) -> ProcessedOrderBookData {
        if !self.snapshots_pending {
            return ProcessedOrderBookData::default();
        }
        self.snapshots_pending = false;

        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(fs_data);
        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        let mut out = ProcessedOrderBookData::default();

        if self.outputs.l4 {
            if let Some(snapshots) = self.compute_snapshots() {
                out.l4.extend(
                    snapshots
                        .into_iter()
                        .map(|snapshot| HyperliquidDataWithMeta {
                            data:          snapshot,
                            pipeline_meta: pipeline_meta.clone()
                        })
                );
            }
        }

        if self.outputs.l2 {
            if let Some(snapshots) = self.compute_l2_snapshots() {
                out.l2.extend(
                    snapshots
                        .into_iter()
                        .map(|snapshot| HyperliquidDataWithMeta {
                            data:          snapshot,
                            pipeline_meta: pipeline_meta.clone()
                        })
                );
            }
        }

        out
    }

    fn apply_cached_batches(&mut self) -> eyre::Result<()> {
        while let Some((order_statuses, book_diffs)) = self.pop_cache() {
            self.apply_cached_batch(&order_statuses, &book_diffs)?;
        }

        Ok(())
    }

    fn apply_cached_batch(
        &mut self,
        order_statuses: &CachedBatch<L4OrderStatus>,
        book_diffs: &CachedBatch<L4BookDiff>
    ) -> eyre::Result<bool> {
        if order_statuses.block_number != book_diffs.block_number {
            return Err(eyre::eyre!("expected synchronized order status and book diff batches"));
        }

        let Some(current_height) = self.snapshot_height else {
            return Err(eyre::eyre!("cannot apply l4 order book batch before snapshot"));
        };

        if order_statuses.block_number <= current_height {
            return Ok(false);
        }

        let expected_height = current_height
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("l4 order book snapshot height overflow"))?;
        if order_statuses.block_number != expected_height {
            return Err(eyre::eyre!(
                "expecting block {}, got block {}",
                expected_height,
                order_statuses.block_number
            ));
        }

        self.apply_updates(&order_statuses.events, &book_diffs.events)?;
        self.snapshot_height = Some(order_statuses.block_number);
        self.book_time = order_statuses.time;

        Ok(true)
    }

    fn apply_updates(
        &mut self,
        order_statuses: &[L4OrderStatus],
        book_diffs: &[L4BookDiff]
    ) -> eyre::Result<()> {
        let mut order_map = order_statuses
            .iter()
            .filter(|order_status| order_status.is_inserted_into_book())
            .map(|order_status| (order_status.order.oid, order_status))
            .collect::<HashMap<_, _>>();

        for diff in book_diffs {
            if self.ignore_spot && Coin::new(&diff.coin).is_spot() {
                continue;
            }

            match &diff.raw_book_diff {
                L4OrderDiff::New { sz } => {
                    let Some(order_status) = order_map.remove(&diff.oid) else {
                        return Err(eyre::eyre!("unable to find order opening status: {diff:?}"));
                    };
                    let mut order = InnerL4Order::try_from((
                        order_status.user.clone(),
                        order_status.order.clone()
                    ))?;
                    order.modify_sz(Sz::new_f64(*sz));
                    order.convert_trigger(order_status.time_unix_ms()?);
                    self.order_books
                        .entry(order.coin.value())
                        .or_default()
                        .add_order(order)?;
                }
                L4OrderDiff::Update { new_sz, .. } => {
                    if !self
                        .order_books
                        .get_mut(&diff.coin)
                        .is_some_and(|book| book.modify_sz(diff.oid, Sz::new_f64(*new_sz)))
                    {
                        return Err(eyre::eyre!("unable to find order on the book: {diff:?}"));
                    }
                }
                L4OrderDiff::Remove => {
                    if !self
                        .order_books
                        .get_mut(&diff.coin)
                        .is_some_and(|book| book.cancel_order(diff.oid))
                    {
                        return Err(eyre::eyre!("unable to find order on the book: {diff:?}"));
                    }
                }
            }
        }

        Ok(())
    }
}

impl HyperliquidDataProcessorHandle for OrderBookDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        match &data.data {
            HyperliquidDirData::NodeOrderStatuses(rows) => {
                self.receive_order_statuses(rows, &data.pipeline_meta)?;
                println!(
                    "[orderbook deriver] received {} order-status rows; \
                     pending_order_status_batches={} pending_book_diff_batches={}",
                    rows.len(),
                    self.pending_order_status_batch_count(),
                    self.pending_book_diff_batch_count()
                );
            }
            HyperliquidDirData::NodeRawBookDiffs(rows) => {
                self.receive_book_diffs(rows, &data.pipeline_meta)?;
                println!(
                    "[orderbook deriver] received {} raw-book-diff rows; \
                     pending_order_status_batches={} pending_book_diff_batches={}",
                    rows.len(),
                    self.pending_order_status_batch_count(),
                    self.pending_book_diff_batch_count()
                );
            }
            _ => return Ok(Vec::new())
        }

        if !self.try_initialize_from_snapshot()? {
            return Ok(Vec::new());
        }

        let mut out = self.process_pending_snapshots(&data.pipeline_meta, processing_data_at_ns);
        out.extend(self.process_ready_batches(processing_data_at_ns)?);

        if !self.is_ready() {
            return Ok(Vec::new());
        }

        if out.is_empty() { Ok(Vec::new()) } else { Ok(out.into_hyperliquid_data()) }
    }
}

#[derive(Default)]
struct ProcessedOrderBookData {
    l4: Vec<HyperliquidDataWithMeta<L4Book>>,
    l2: Vec<HyperliquidDataWithMeta<L2Book>>
}

impl ProcessedOrderBookData {
    fn extend(&mut self, other: Self) {
        self.l4.extend(other.l4);
        self.l2.extend(other.l2);
    }

    fn is_empty(&self) -> bool {
        self.l4.is_empty() && self.l2.is_empty()
    }

    fn into_hyperliquid_data(self) -> Vec<HyperliquidData> {
        let mut out = Vec::new();

        if !self.l4.is_empty() {
            out.push(HyperliquidData::L4Book(self.l4));
        }
        if !self.l2.is_empty() {
            out.push(HyperliquidData::L2Book(self.l2));
        }

        out
    }
}

#[derive(Clone)]
struct CachedBatch<T> {
    block_number:  u64,
    time:          u64,
    events:        Vec<T>,
    pipeline_meta: ParsedDataPipelineMeta
}

impl<T> CachedBatch<T> {
    fn from_meta(
        block_number: u64,
        time: u64,
        events: Vec<T>,
        pipeline_meta: ParsedDataPipelineMeta
    ) -> Self {
        Self { block_number, time, events, pipeline_meta }
    }
}

#[derive(Default)]
struct StreamingBlockCache {
    order_statuses:            BTreeMap<u64, AccumulatedBatch<L4OrderStatus>>,
    book_diffs:                BTreeMap<u64, AccumulatedBatch<L4BookDiff>>,
    latest_order_status_block: Option<u64>,
    latest_book_diff_block:    Option<u64>,
    next_block_to_finalize:    Option<u64>,
    last_finalized_time:       u64
}

impl StreamingBlockCache {
    fn push_order_statuses(
        &mut self,
        block_number: u64,
        time: u64,
        events: Vec<L4OrderStatus>,
        fs_data: &FsOutData
    ) {
        if self.is_finalized(block_number) {
            return;
        }

        self.latest_order_status_block = Some(
            self.latest_order_status_block
                .map_or(block_number, |latest| latest.max(block_number))
        );
        self.order_statuses
            .entry(block_number)
            .or_insert_with(|| AccumulatedBatch::new(time))
            .extend(time, events, fs_data);
    }

    fn push_book_diffs(
        &mut self,
        block_number: u64,
        time: u64,
        events: Vec<L4BookDiff>,
        fs_data: &FsOutData
    ) {
        if self.is_finalized(block_number) {
            return;
        }

        self.latest_book_diff_block = Some(
            self.latest_book_diff_block
                .map_or(block_number, |latest| latest.max(block_number))
        );
        self.book_diffs
            .entry(block_number)
            .or_insert_with(|| AccumulatedBatch::new(time))
            .extend(time, events, fs_data);
    }

    fn pop_ready_batches(&mut self) -> Vec<(CachedBatch<L4OrderStatus>, CachedBatch<L4BookDiff>)> {
        let Some(ready_upper_exclusive) = self.ready_upper_exclusive() else {
            return Vec::new();
        };
        let Some(mut block_number) = self
            .next_block_to_finalize
            .or_else(|| self.first_pending_block())
        else {
            return Vec::new();
        };

        let mut out = Vec::new();
        while block_number < ready_upper_exclusive {
            let order_statuses = self.order_statuses.remove(&block_number);
            let book_diffs = self.book_diffs.remove(&block_number);
            let time = order_statuses
                .as_ref()
                .map(|batch| batch.time)
                .or_else(|| book_diffs.as_ref().map(|batch| batch.time))
                .unwrap_or(self.last_finalized_time);
            let order_status_meta = order_statuses
                .as_ref()
                .map(|batch| batch.pipeline_meta.clone())
                .or_else(|| book_diffs.as_ref().map(|batch| batch.pipeline_meta.clone()))
                .unwrap_or_default();
            let book_diff_meta = book_diffs
                .as_ref()
                .map(|batch| batch.pipeline_meta.clone())
                .or_else(|| {
                    order_statuses
                        .as_ref()
                        .map(|batch| batch.pipeline_meta.clone())
                })
                .unwrap_or_default();
            let order_status_events = order_statuses.map(|batch| batch.events).unwrap_or_default();
            let book_diff_events = book_diffs.map(|batch| batch.events).unwrap_or_default();

            out.push((
                CachedBatch::from_meta(block_number, time, order_status_events, order_status_meta),
                CachedBatch::from_meta(block_number, time, book_diff_events, book_diff_meta)
            ));

            self.last_finalized_time = time;
            block_number += 1;
        }

        self.next_block_to_finalize = Some(block_number);
        out
    }

    fn start_at(&mut self, block_number: u64) {
        let next_block = self
            .next_block_to_finalize
            .map_or(block_number, |next_block| next_block.max(block_number));
        self.next_block_to_finalize = Some(next_block);
        self.order_statuses
            .retain(|block_number, _| *block_number >= next_block);
        self.book_diffs
            .retain(|block_number, _| *block_number >= next_block);
    }

    fn order_status_batch_count(&self) -> usize {
        self.order_statuses.len()
    }

    fn book_diff_batch_count(&self) -> usize {
        self.book_diffs.len()
    }

    fn is_finalized(&self, block_number: u64) -> bool {
        self.next_block_to_finalize
            .is_some_and(|next_block| block_number < next_block)
    }

    fn ready_upper_exclusive(&self) -> Option<u64> {
        let lower_watermark = self
            .latest_order_status_block?
            .min(self.latest_book_diff_block?);

        lower_watermark.checked_sub(STREAMING_FINALIZATION_BLOCK_DELAY - 1)
    }

    fn first_pending_block(&self) -> Option<u64> {
        self.order_statuses
            .keys()
            .chain(self.book_diffs.keys())
            .min()
            .copied()
    }
}

struct AccumulatedBatch<T> {
    time:          u64,
    events:        Vec<T>,
    pipeline_meta: ParsedDataPipelineMeta
}

impl<T> AccumulatedBatch<T> {
    fn new(time: u64) -> Self {
        Self { time, events: Vec::new(), pipeline_meta: ParsedDataPipelineMeta::default() }
    }

    fn extend(&mut self, time: u64, events: Vec<T>, fs_data: &FsOutData) {
        self.time = time;
        self.events.extend(events);
        self.pipeline_meta.modify_with_fs_data(fs_data);
    }
}

struct BatchQueue<T> {
    deque:      VecDeque<CachedBatch<T>>,
    last_block: Option<u64>
}

impl<T> Default for BatchQueue<T> {
    fn default() -> Self {
        Self { deque: VecDeque::new(), last_block: None }
    }
}

impl<T> BatchQueue<T> {
    fn push(&mut self, batch: CachedBatch<T>) -> eyre::Result<()> {
        if self
            .last_block
            .is_some_and(|last_block| last_block >= batch.block_number)
        {
            return Ok(());
        }

        self.last_block = Some(batch.block_number);
        self.deque.push_back(batch);

        Ok(())
    }

    fn pop_front(&mut self) -> Option<CachedBatch<T>> {
        self.deque.pop_front()
    }

    fn front(&self) -> Option<&CachedBatch<T>> {
        self.deque.front()
    }

    fn len(&self) -> usize {
        self.deque.len()
    }
}

#[derive(Clone, Copy)]
struct OrderBookOutputs {
    l4: bool,
    l2: bool
}

impl Default for OrderBookOutputs {
    fn default() -> Self {
        Self { l4: true, l2: false }
    }
}

impl OrderBookOutputs {
    fn for_data_kinds(data_kinds: &[HyperliquidDataKind]) -> Self {
        Self {
            l4: data_kinds.contains(&HyperliquidDataKind::L4Book),
            l2: data_kinds.contains(&HyperliquidDataKind::L2Book)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::{
        OrderBookDeriver,
        snapshots::{StateSnapshot, StateSnapshotFetcher},
        types::{Coin, InnerL4Order, Px, Snapshot, Snapshots, Sz}
    };
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{NodeOrderStatusesRows, NodeRawBookDiffsRows}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{HyperliquidData, HyperliquidDataKind, L4Book, L4OrderDiff, Side}
    };

    #[test]
    fn waits_for_snapshot_before_processing_batches() {
        let mut deriver = deriver_without_snapshot();

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap()
            .into_iter()
            .next();

        assert!(first.is_none());
        assert_eq!(deriver.pending_batch_count(), 1);

        let second = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap()
            .into_iter()
            .next();

        assert!(second.is_none());
        assert_eq!(deriver.pending_batch_count(), 2);
        assert_eq!(deriver.order_count(), 0);
    }

    #[test]
    fn waits_for_matching_status_and_diff_batches() {
        let mut deriver = deriver_with_snapshot(1019927124);

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap()
            .into_iter()
            .next();

        assert!(first.is_none());
        assert_eq!(deriver.pending_batch_count(), 1);

        let second = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap()
            .into_iter()
            .next();

        assert!(second.is_none());

        let out = flush_block(&mut deriver, 1019927125)
            .into_iter()
            .next()
            .expect("matched batches should emit an l4 update after both streams advance");

        let HyperliquidData::L4Book(books) = out else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(deriver.order_count(), 1);

        let L4Book::Updates(update) = &books[0].data else { unreachable!() };
        assert_eq!(update.height, 1019927125);
        assert_eq!(update.order_statuses.len(), 1);
        assert_eq!(update.book_diffs.len(), 1);
        assert!(matches!(update.book_diffs[0].raw_book_diff, L4OrderDiff::New { .. }));
    }

    #[test]
    fn emits_l4_snapshots_after_initializing_from_state() {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            Coin::new("BTC"),
            Snapshot::new([vec![inner_order(1, Side::Bid, 100.0, 1.25)], vec![]])
        );
        let mut deriver = deriver_with_state_snapshot(1019927124, Snapshots::new(snapshots));

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap()
            .into_iter()
            .next()
            .expect("initialization should emit l4 snapshots");

        let HyperliquidData::L4Book(books) = out else { unreachable!() };
        assert_eq!(books.len(), 1);

        let L4Book::Snapshot { coin, time, height, levels } = &books[0].data else {
            unreachable!()
        };
        assert_eq!(coin, "BTC");
        assert_eq!(*time, 0);
        assert_eq!(*height, 1019927124);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(levels[0][0].oid, 1);
        assert_eq!(levels[0][0].sz, 1.25);
    }

    #[test]
    fn emits_l2_updates_after_matched_batch() {
        let mut deriver = deriver_with_snapshot(1019927124);
        request_all_orderbook_outputs(&mut deriver);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        assert!(out.is_empty());
        let out = flush_block(&mut deriver, 1019927125);

        assert_eq!(out.len(), 2);

        let HyperliquidData::L2Book(books) = &out[1] else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].data.coin, "JTO");
        assert_eq!(books[0].data.time, 1780394399398);
        assert!(books[0].data.bids().is_empty());

        let asks = books[0].data.asks();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, "0.65313");
        assert_eq!(asks[0].sz, "1096");
        assert_eq!(asks[0].n, 1);
    }

    #[test]
    fn emits_l2_snapshots_after_initializing_from_state() {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            Coin::new("BTC"),
            Snapshot::new([
                vec![
                    inner_order(1, Side::Bid, 100.0, 1.25),
                    inner_order(2, Side::Bid, 100.0, 0.75),
                ],
                vec![inner_order(3, Side::Ask, 101.0, 2.0)]
            ])
        );
        let mut deriver = deriver_with_state_snapshot(1019927124, Snapshots::new(snapshots));
        request_all_orderbook_outputs(&mut deriver);

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        assert_eq!(out.len(), 2);

        let HyperliquidData::L2Book(books) = &out[1] else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].data.coin, "BTC");
        assert_eq!(books[0].data.time, 0);

        let bids = books[0].data.bids();
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, "100");
        assert_eq!(bids[0].sz, "2");
        assert_eq!(bids[0].n, 2);

        let asks = books[0].data.asks();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, "101");
        assert_eq!(asks[0].sz, "2");
        assert_eq!(asks[0].n, 1);
    }

    #[test]
    fn only_emits_requested_orderbook_outputs() {
        let mut deriver = deriver_with_snapshot(1019927124);
        deriver.outputs = super::OrderBookOutputs::for_data_kinds(&[HyperliquidDataKind::L2Book]);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        assert!(out.is_empty());
        let out = flush_block(&mut deriver, 1019927125);

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], HyperliquidData::L2Book(_)));
    }

    #[test]
    fn l2_output_waits_for_matching_order_statuses() {
        let mut deriver = deriver_with_snapshot(1019927124);
        deriver.outputs = super::OrderBookOutputs::for_data_kinds(&[HyperliquidDataKind::L2Book]);

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        assert!(first.is_empty());
        assert_eq!(deriver.pending_batch_count(), 1);

        let second = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        assert!(second.is_empty());
        let second = flush_block(&mut deriver, 1019927125);

        assert_eq!(second.len(), 1);
        let HyperliquidData::L2Book(books) = &second[0] else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].data.coin, "JTO");
        assert_eq!(books[0].data.time, 1780394399398);
        assert!(books[0].data.bids().is_empty());

        let asks = books[0].data.asks();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, "0.65313");
        assert_eq!(asks[0].sz, "1096");
        assert_eq!(asks[0].n, 1);
    }

    #[test]
    fn zero_size_update_removes_order_from_l2_book() {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            Coin::new("JTO"),
            Snapshot::new([
                vec![],
                vec![inner_order_for_coin("JTO", 452916385917, Side::Ask, 0.65313, 1096.0)]
            ])
        );

        let mut deriver = deriver_with_state_snapshot(1019927124, Snapshots::new(snapshots));
        deriver.outputs = super::OrderBookOutputs::for_data_kinds(&[HyperliquidDataKind::L2Book]);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![zero_size_update_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        assert!(out.is_empty());
        let out = flush_block(&mut deriver, 1019927125);

        assert_eq!(out.len(), 1);
        let HyperliquidData::L2Book(books) = &out[0] else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].data.coin, "JTO");
        assert!(books[0].data.bids().is_empty());
        assert!(books[0].data.asks().is_empty());
    }

    fn request_all_orderbook_outputs(deriver: &mut OrderBookDeriver) {
        deriver.outputs = super::OrderBookOutputs::for_data_kinds(&[
            HyperliquidDataKind::L4Book,
            HyperliquidDataKind::L2Book
        ]);
    }

    #[test]
    fn resets_ready_book_after_apply_failure() {
        let mut deriver = ready_deriver(1019927124);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![remove_diff_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap()
            .into_iter()
            .next();

        assert!(out.is_none());
        let out = flush_block(&mut deriver, 1019927125).into_iter().next();

        assert!(out.is_none());
        assert!(!deriver.is_ready());
        assert_eq!(deriver.order_count(), 0);
    }

    #[test]
    fn merges_multiple_streaming_rows_for_same_block_before_applying() {
        let mut deriver = ready_deriver(1019927124);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        let out = flush_block(&mut deriver, 1019927125);

        let HyperliquidData::L4Book(books) = &out[0] else { unreachable!() };
        let L4Book::Updates(update) = &books[0].data else { unreachable!() };
        assert_eq!(update.height, 1019927125);
        assert_eq!(update.order_statuses.len(), 1);
        assert_eq!(update.book_diffs.len(), 1);
        assert_eq!(deriver.order_count(), 1);
    }

    #[test]
    fn advances_across_empty_streaming_block_gaps() {
        let mut deriver = ready_deriver(1019927124);

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row_for_block(
                    1019927127
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row_for_block(
                    1019927127
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        let out = flush_block(&mut deriver, 1019927127);

        assert_eq!(deriver.order_count(), 1);
        let HyperliquidData::L4Book(books) = &out[0] else { unreachable!() };
        let L4Book::Updates(update) = &books[0].data else { unreachable!() };
        assert_eq!(update.height, 1019927127);
    }

    fn deriver_without_snapshot() -> OrderBookDeriver {
        OrderBookDeriver {
            order_status_cache:          Default::default(),
            book_diff_cache:             Default::default(),
            streaming_block_cache:       Default::default(),
            order_books:                 Default::default(),
            state_snapshot:              StateSnapshotFetcher::empty(),
            snapshot_height:             None,
            book_time:                   0,
            snapshots_pending:           false,
            ignore_spot:                 false,
            logged_waiting_for_snapshot: false,
            outputs:                     Default::default()
        }
    }

    fn deriver_with_snapshot(height: u64) -> OrderBookDeriver {
        deriver_with_state_snapshot(height, Snapshots::new(HashMap::new()))
    }

    fn deriver_with_state_snapshot(
        height: u64,
        snapshots: Snapshots<InnerL4Order>
    ) -> OrderBookDeriver {
        OrderBookDeriver {
            order_status_cache:          Default::default(),
            book_diff_cache:             Default::default(),
            streaming_block_cache:       Default::default(),
            order_books:                 Default::default(),
            state_snapshot:              StateSnapshotFetcher::with_snapshot(StateSnapshot {
                height,
                snapshots
            }),
            snapshot_height:             None,
            book_time:                   0,
            snapshots_pending:           false,
            ignore_spot:                 false,
            logged_waiting_for_snapshot: false,
            outputs:                     Default::default()
        }
    }

    fn ready_deriver(height: u64) -> OrderBookDeriver {
        OrderBookDeriver {
            order_status_cache:          Default::default(),
            book_diff_cache:             Default::default(),
            streaming_block_cache:       Default::default(),
            order_books:                 Default::default(),
            state_snapshot:              StateSnapshotFetcher::empty(),
            snapshot_height:             Some(height),
            book_time:                   0,
            snapshots_pending:           false,
            ignore_spot:                 false,
            logged_waiting_for_snapshot: false,
            outputs:                     Default::default()
        }
    }

    fn status_row() -> NodeOrderStatusesRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-02T10:00:00.854461191",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":1019927125,
                "events":[{
                    "time":"2026-06-02T09:59:59.398971110",
                    "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                    "hash":"0xhash",
                    "builder":null,
                    "status":"open",
                    "order":{
                        "coin":"JTO",
                        "side":"A",
                        "limitPx":"0.65313",
                        "sz":"1096.0",
                        "oid":452916385917,
                        "timestamp":1780394399398,
                        "triggerCondition":"N/A",
                        "isTrigger":false,
                        "triggerPx":"0.0",
                        "children":[],
                        "isPositionTpsl":false,
                        "reduceOnly":false,
                        "orderType":"Limit",
                        "origSz":"1096.0",
                        "tif":"Gtc",
                        "cloid":null
                    }
                }]
            }"#
        )
        .unwrap()
    }

    fn diff_row() -> NodeRawBookDiffsRows {
        serde_json::from_str(
            r#"{
                "local_time":"2026-06-02T10:00:00.854322306",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":1019927125,
                "events":[{
                    "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                    "oid":452916385917,
                    "coin":"JTO",
                    "side":"A",
                    "px":"0.65313",
                    "raw_book_diff":{"new":{"sz":"1096.0"}}
                }]
            }"#
        )
        .unwrap()
    }

    fn status_row_for_block(block_number: u64) -> NodeOrderStatusesRows {
        let mut row = status_row();
        row.block_number = block_number;
        row
    }

    fn diff_row_for_block(block_number: u64) -> NodeRawBookDiffsRows {
        let mut row = diff_row();
        row.block_number = block_number;
        row
    }

    fn empty_status_row(block_number: u64) -> NodeOrderStatusesRows {
        serde_json::from_str(&format!(
            r#"{{
                "local_time":"2026-06-02T10:00:00.854461191",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":{block_number},
                "events":[]
            }}"#
        ))
        .unwrap()
    }

    fn empty_diff_row(block_number: u64) -> NodeRawBookDiffsRows {
        serde_json::from_str(&format!(
            r#"{{
                "local_time":"2026-06-02T10:00:00.854322306",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":{block_number},
                "events":[]
            }}"#
        ))
        .unwrap()
    }

    fn remove_diff_row(block_number: u64) -> NodeRawBookDiffsRows {
        serde_json::from_str(&format!(
            r#"{{
                "local_time":"2026-06-02T10:00:00.854322306",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":{block_number},
                "events":[{{
                    "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                    "oid":999,
                    "coin":"JTO",
                    "side":"A",
                    "px":"0.65313",
                    "raw_book_diff":"remove"
                }}]
            }}"#
        ))
        .unwrap()
    }

    fn zero_size_update_row(block_number: u64) -> NodeRawBookDiffsRows {
        serde_json::from_str(&format!(
            r#"{{
                "local_time":"2026-06-02T10:00:00.854322306",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":{block_number},
                "events":[{{
                    "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                    "oid":452916385917,
                    "coin":"JTO",
                    "side":"A",
                    "px":"0.65313",
                    "raw_book_diff":{{"update":{{"origSz":"1096.0","newSz":"0.0"}}}}
                }}]
            }}"#
        ))
        .unwrap()
    }

    fn inner_order(oid: u64, side: Side, limit_px: f64, sz: f64) -> InnerL4Order {
        inner_order_for_coin("BTC", oid, side, limit_px, sz)
    }

    fn inner_order_for_coin(
        coin: &str,
        oid: u64,
        side: Side,
        limit_px: f64,
        sz: f64
    ) -> InnerL4Order {
        InnerL4Order {
            user: "0x0000000000000000000000000000000000000000".to_string(),
            coin: Coin::new(coin),
            side,
            limit_px: Px::new_f64(limit_px),
            sz: Sz::new_f64(sz),
            oid,
            timestamp: 0,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: 0.0,
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None
        }
    }

    fn fs_data(kind: HyperliquidDirKind) -> Arc<FsOutData> {
        Arc::new(FsOutData {
            name: kind,
            bytes: vec![],
            path: String::new(),
            chunk_len: 0,
            notification_received_at_ns: 0,
            pipeline: Default::default()
        })
    }

    fn flush_block(deriver: &mut OrderBookDeriver, block_number: u64) -> Vec<HyperliquidData> {
        let mut out = Vec::new();

        for offset in 1..=super::STREAMING_FINALIZATION_BLOCK_DELAY {
            let next_block = block_number + offset;
            deriver
                .handle_data(&HyperliquidDirDataWithMeta {
                    data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                        next_block
                    )]),
                    pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
                })
                .unwrap();
            out = deriver
                .handle_data(&HyperliquidDirDataWithMeta {
                    data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                        next_block
                    )]),
                    pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
                })
                .unwrap();
        }

        out
    }
}
