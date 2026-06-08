use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc
};

pub mod book;
pub mod processing;
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
            processing::*,
            types::{Coin, InnerL4Order, Sz},
            utils::coin_to_book_updates
        }
    },
    types::*,
    utils::unix_timestamp
};

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const PRICE_MULTIPLIER: f64 = 100_000_000.0;
const FETCH_SNAPSHOT_SLEEP_TIME_SEC: u64 = 5;

#[derive(Default)]
pub struct OrderBookDeriver {
    order_status_cache:          BatchQueue<L4OrderStatus>,
    book_diff_cache:             BatchQueue<L4BookDiff>,
    streaming_block_cache:       StreamingBlockCache,
    order_books:                 BTreeMap<String, OrderBook>,
    state_snapshot:              StateSnapshotFetcher,
    pending_snapshot:            Option<StateSnapshot>,
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

        if self.pending_snapshot.is_none() {
            let Some(snapshot) = self.state_snapshot.write(Option::take)? else {
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
                "[orderbook deriver] initializing from snapshot height={}; \
                 pending_order_status_batches={} pending_book_diff_batches={}",
                snapshot.height,
                self.pending_order_status_batch_count(),
                self.pending_book_diff_batch_count()
            );
            self.pending_snapshot = Some(snapshot);
        }

        self.try_initialize_pending_snapshot()
    }

    fn try_initialize_pending_snapshot(&mut self) -> eyre::Result<bool> {
        let Some(StateSnapshot { height, snapshots }) = self.pending_snapshot.take() else {
            return Ok(false);
        };

        self.push_ready_streaming_batches_from_height(height)?;
        if !self.has_cached_pair() {
            self.pending_snapshot = Some(StateSnapshot { height, snapshots });
            return Ok(false);
        }

        let mut candidate_order_books = snapshots.into_orderbooks(self.ignore_spot)?;
        let mut candidate_height = height;
        let mut candidate_book_time = 0;
        let mut replayed_batches = 0;

        while let Some((order_statuses, book_diffs)) = self.pop_cache() {
            match Self::apply_cached_batch_to_state(
                &mut candidate_order_books,
                &mut candidate_height,
                &mut candidate_book_time,
                self.ignore_spot,
                &order_statuses,
                &book_diffs
            ) {
                Ok(applied) => {
                    if applied {
                        replayed_batches += 1;
                    }
                }
                Err(error) => {
                    println!(
                        "[orderbook deriver] failed to apply cached batches after snapshot \
                         height={height}; waiting for next snapshot: {error:?}"
                    );
                    tracing::info!(
                        ?error,
                        "Failed to apply updates to this book (likely missing older updates). \
                         Waiting for next snapshot."
                    );
                    self.reset_after_apply_error();
                    return Ok(false);
                }
            }
        }

        if replayed_batches == 0 {
            self.reset_after_apply_error();
            return Ok(false);
        }

        self.order_books = candidate_order_books;
        self.snapshot_height = Some(candidate_height);
        self.book_time = candidate_book_time;
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
        self.pending_snapshot = None;
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
        self.push_ready_streaming_batches_from_height(snapshot_height)
    }

    fn push_ready_streaming_batches_from_height(
        &mut self,
        snapshot_height: u64
    ) -> eyre::Result<()> {
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

    fn has_cached_pair(&mut self) -> bool {
        loop {
            let Some(diff_block) = self.book_diff_cache.front().map(|batch| batch.block_number)
            else {
                return false;
            };
            let Some(status_block) = self
                .order_status_cache
                .front()
                .map(|batch| batch.block_number)
            else {
                return false;
            };

            match diff_block.cmp(&status_block) {
                Ordering::Less => {
                    self.book_diff_cache.pop_front();
                }
                Ordering::Equal => return true,
                Ordering::Greater => {
                    self.order_status_cache.pop_front();
                }
            }
        }
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
        // Gap-filled blocks with no real block time would emit books stamped
        // time=0, which are meaningless downstream. Skip them.
        if time == 0 {
            return Vec::new();
        }

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

        // A freshly initialized book may not have a real block time yet when the
        // finalize range began on empty gap-filled blocks (their time falls back
        // to 0). Snapshots stamped time=0 are useless downstream, so hold the
        // pending snapshot until a real block time is known.
        if self.book_time == 0 {
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

    fn apply_cached_batch(
        &mut self,
        order_statuses: &CachedBatch<L4OrderStatus>,
        book_diffs: &CachedBatch<L4BookDiff>
    ) -> eyre::Result<bool> {
        let Some(mut current_height) = self.snapshot_height else {
            return Err(eyre::eyre!("cannot apply l4 order book batch before snapshot"));
        };

        let applied = Self::apply_cached_batch_to_state(
            &mut self.order_books,
            &mut current_height,
            &mut self.book_time,
            self.ignore_spot,
            order_statuses,
            book_diffs
        )?;
        self.snapshot_height = Some(current_height);

        Ok(applied)
    }

    fn apply_cached_batch_to_state(
        order_books: &mut BTreeMap<String, OrderBook>,
        current_height: &mut u64,
        book_time: &mut u64,
        ignore_spot: bool,
        order_statuses: &CachedBatch<L4OrderStatus>,
        book_diffs: &CachedBatch<L4BookDiff>
    ) -> eyre::Result<bool> {
        if order_statuses.block_number != book_diffs.block_number {
            return Err(eyre::eyre!("expected synchronized order status and book diff batches"));
        }

        if order_statuses.block_number <= *current_height {
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

        Self::apply_updates_to_books(
            order_books,
            ignore_spot,
            &order_statuses.events,
            &book_diffs.events
        )?;
        *current_height = order_statuses.block_number;
        *book_time = order_statuses.time;

        Ok(true)
    }

    fn apply_updates_to_books(
        order_books: &mut BTreeMap<String, OrderBook>,
        ignore_spot: bool,
        order_statuses: &[L4OrderStatus],
        book_diffs: &[L4BookDiff]
    ) -> eyre::Result<()> {
        let mut order_map = order_statuses
            .iter()
            .filter(|order_status| order_status.is_inserted_into_book())
            .map(|order_status| (order_status.order.oid, order_status))
            .collect::<HashMap<_, _>>();

        for diff in book_diffs {
            if ignore_spot && Coin::new(&diff.coin).is_spot() {
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
                    order_books
                        .entry(order.coin.value())
                        .or_default()
                        .add_order(order)?;
                }
                L4OrderDiff::Update { new_sz, .. } => {
                    if !order_books
                        .get_mut(&diff.coin)
                        .is_some_and(|book| book.modify_sz(diff.oid, Sz::new_f64(*new_sz)))
                    {
                        return Err(eyre::eyre!("unable to find order on the book: {diff:?}"));
                    }
                }
                L4OrderDiff::Remove => {
                    if !order_books
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
        let mut deriver = ready_deriver(1019927124);

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

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        assert!(first.is_empty());
        assert!(!deriver.is_ready());

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();
        assert!(!deriver.is_ready());

        let out = flush_block(&mut deriver, 1019927125)
            .into_iter()
            .next()
            .expect("initialization should emit l4 snapshots");

        let HyperliquidData::L4Book(books) = out else { unreachable!() };
        assert_eq!(books.len(), 1);

        let L4Book::Snapshot { coin, time, height, levels } = &books[0].data else {
            unreachable!()
        };
        assert_eq!(coin, "BTC");
        assert_eq!(*time, 1780394399398);
        assert_eq!(*height, 1019927125);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(levels[0][0].oid, 1);
        assert_eq!(levels[0][0].sz, 1.25);
    }

    #[test]
    fn emits_l2_updates_after_matched_batch() {
        let mut deriver = ready_deriver(1019927124);
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

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        assert!(first.is_empty());
        assert!(!deriver.is_ready());

        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();
        assert!(!deriver.is_ready());

        let out = flush_block(&mut deriver, 1019927125);

        assert_eq!(out.len(), 2);

        let HyperliquidData::L2Book(books) = &out[1] else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].data.coin, "BTC");
        assert_eq!(books[0].data.time, 1780394399398);

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
    fn rejects_bad_post_snapshot_replay_before_emitting_snapshot() {
        let mut deriver = deriver_with_snapshot(1019927124);

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
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![remove_diff_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

        let out = flush_block(&mut deriver, 1019927125);

        assert!(out.is_empty());
        assert!(!deriver.is_ready());
        assert_eq!(deriver.order_count(), 0);
    }

    #[test]
    fn only_emits_requested_orderbook_outputs() {
        let mut deriver = ready_deriver(1019927124);
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
        let mut deriver = ready_deriver(1019927124);
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
    fn zero_size_update_keeps_order_on_book_with_zero_size() {
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

        // The order stays on the book at zero size (mirroring order_book_server);
        // it is only removed by an explicit `Remove` diff or by being matched.
        let asks = books[0].data.asks();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, "0.65313");
        assert_eq!(asks[0].sz, "0");
        assert_eq!(asks[0].n, 1);
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
    fn stops_at_missing_block_instead_of_synthesizing_empty() {
        let mut deriver = ready_deriver(1019927124);

        // Blocks 1019927125 and 1019927127 arrive in both streams, but 1019927126
        // never does. With lockstep replay the hole must halt finalization rather
        // than synthesize an empty 1019927126 and march past the gap (which would
        // drop any opens carried by blocks still in flight).
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
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                    1019927125
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();
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

        // Even with a successor for 1019927127 delivered, the missing 1019927126
        // blocks the contiguous run, so the open in 1019927127 is never applied.
        let out = flush_block(&mut deriver, 1019927127);
        assert!(out.is_empty());
        assert_eq!(deriver.order_count(), 0);

        // Once 1019927126 arrives the run is contiguous and replay catches up
        // through 1019927127.
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    1019927126
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();
        let out = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                    1019927126
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

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
            pending_snapshot:            None,
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
            pending_snapshot:            None,
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
            pending_snapshot:            None,
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
        // A block is finalized only once its successor has arrived in both
        // streams (which proves the block is complete), so deliver an empty
        // successor block to trigger finalization of `block_number` in tests.
        let next_block = block_number + 1;
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![empty_status_row(
                    next_block
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();
        deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![empty_diff_row(
                    next_block
                )]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap()
    }
}
