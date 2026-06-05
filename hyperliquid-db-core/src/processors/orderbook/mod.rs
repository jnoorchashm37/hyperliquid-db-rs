use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
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
        HyperliquidData, HyperliquidDataWithMeta, L4Book, L4BookDiff, L4OrderDiff, L4OrderStatus,
        ParsedDataPipelineMeta
    },
    utils::unix_timestamp
};

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const PRICE_MULTIPLIER: f64 = 100_000_000.0;
const FETCH_SNAPSHOT_SLEEP_TIME_SEC: u64 = 5;

#[derive(Default)]
pub struct OrderBookDeriver {
    order_status_cache: BatchQueue<L4OrderStatus>,
    book_diff_cache:    BatchQueue<L4BookDiff>,
    order_books:        BTreeMap<String, OrderBook>,
    state_snapshot:     StateSnapshotFetcher,
    snapshot_height:    Option<u64>,
    book_time:          u64,
    snapshots_pending:  bool,
    ignore_spot:        bool
}

impl OrderBookDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ignore_spot(ignore_spot: bool) -> Self {
        Self { ignore_spot, ..Default::default() }
    }

    pub fn order_count(&self) -> usize {
        self.order_books.values().map(OrderBook::order_count).sum()
    }

    pub fn pending_batch_count(&self) -> usize {
        self.order_status_cache.len() + self.book_diff_cache.len()
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

    fn try_initialize_from_snapshot(&mut self) -> eyre::Result<bool> {
        if self.is_ready() {
            return Ok(true);
        }

        let Some(StateSnapshot { height, snapshots }) = self.state_snapshot.write(Option::take)?
        else {
            return Ok(false);
        };

        self.order_books = snapshots.into_orderbooks(self.ignore_spot)?;
        self.snapshot_height = Some(height);
        self.book_time = 0;

        if let Err(error) = self.apply_cached_batches() {
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
        Ok(true)
    }

    fn reset_after_apply_error(&mut self) {
        self.order_books.clear();
        self.snapshot_height = None;
        self.book_time = 0;
        self.snapshots_pending = false;
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
            self.order_status_cache.push(CachedBatch::new(
                row.block_number,
                row.block_time_unix_ms()?,
                events,
                fs_data
            ))?;
        }

        Ok(())
    }

    fn receive_book_diffs(
        &mut self,
        rows: &[NodeRawBookDiffsRows],
        fs_data: &Arc<FsOutData>
    ) -> eyre::Result<()> {
        for row in rows {
            self.book_diff_cache.push(CachedBatch::new(
                row.block_number,
                row.block_time_unix_ms()?,
                row.events
                    .iter()
                    .cloned()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                fs_data
            ))?;
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
    ) -> eyre::Result<Vec<HyperliquidDataWithMeta<L4Book>>> {
        let mut out = Vec::new();

        while let Some((order_statuses, book_diffs)) = self.pop_cache() {
            let applied = match self.apply_cached_batch(&order_statuses, &book_diffs) {
                Ok(applied) => applied,
                Err(error) => {
                    tracing::info!(
                        ?error,
                        "Failed to apply updates to this book. Waiting for next snapshot."
                    );
                    self.reset_after_apply_error();
                    return Ok(Vec::new());
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

            for update in coin_to_book_updates(
                order_statuses.events,
                book_diffs.events,
                book_diffs.time,
                book_diffs.block_number
            ) {
                out.push(HyperliquidDataWithMeta {
                    data:          L4Book::Updates(update),
                    pipeline_meta: pipeline_meta.clone()
                });
            }
        }

        Ok(out)
    }

    fn process_pending_snapshots(
        &mut self,
        fs_data: &Arc<FsOutData>,
        processing_data_at_ns: u128
    ) -> Vec<HyperliquidDataWithMeta<L4Book>> {
        if !self.snapshots_pending {
            return Vec::new();
        }
        self.snapshots_pending = false;

        let Some(snapshots) = self.compute_snapshots() else {
            return Vec::new();
        };

        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(fs_data);
        pipeline_meta.processing_data_at_ns = processing_data_at_ns;
        pipeline_meta.processed_data_at_ns = unix_timestamp().as_nanos();

        snapshots
            .into_iter()
            .map(|snapshot| HyperliquidDataWithMeta {
                data:          snapshot,
                pipeline_meta: pipeline_meta.clone()
            })
            .collect()
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
            }
            HyperliquidDirData::NodeRawBookDiffs(rows) => {
                self.receive_book_diffs(rows, &data.pipeline_meta)?;
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

        if out.is_empty() { Ok(Vec::new()) } else { Ok(vec![HyperliquidData::L4Book(out)]) }
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
    fn new(block_number: u64, time: u64, events: Vec<T>, fs_data: &FsOutData) -> Self {
        let mut pipeline_meta = ParsedDataPipelineMeta::default();
        pipeline_meta.modify_with_fs_data(fs_data);
        Self { block_number, time, events, pipeline_meta }
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
        types::{HyperliquidData, L4Book, L4OrderDiff, Side}
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
            .next()
            .expect("matched batches should emit an l4 update");

        let HyperliquidData::L4Book(books) = second else { unreachable!() };
        assert_eq!(books.len(), 1);
        assert_eq!(deriver.pending_batch_count(), 0);
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
        assert!(!deriver.is_ready());
        assert_eq!(deriver.order_count(), 0);
    }

    fn deriver_without_snapshot() -> OrderBookDeriver {
        OrderBookDeriver {
            order_status_cache: Default::default(),
            book_diff_cache:    Default::default(),
            order_books:        Default::default(),
            state_snapshot:     StateSnapshotFetcher::empty(),
            snapshot_height:    None,
            book_time:          0,
            snapshots_pending:  false,
            ignore_spot:        false
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
            order_status_cache: Default::default(),
            book_diff_cache:    Default::default(),
            order_books:        Default::default(),
            state_snapshot:     StateSnapshotFetcher::with_snapshot(StateSnapshot {
                height,
                snapshots
            }),
            snapshot_height:    None,
            book_time:          0,
            snapshots_pending:  false,
            ignore_spot:        false
        }
    }

    fn ready_deriver(height: u64) -> OrderBookDeriver {
        OrderBookDeriver {
            order_status_cache: Default::default(),
            book_diff_cache:    Default::default(),
            order_books:        Default::default(),
            state_snapshot:     StateSnapshotFetcher::empty(),
            snapshot_height:    Some(height),
            book_time:          0,
            snapshots_pending:  false,
            ignore_spot:        false
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

    fn inner_order(oid: u64, side: Side, limit_px: f64, sz: f64) -> InnerL4Order {
        InnerL4Order {
            user: "0x0000000000000000000000000000000000000000".to_string(),
            coin: Coin::new("BTC"),
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
}
