use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc
};

pub mod book;
pub mod snapshots;
pub mod types;

use serde_json::Value;

use self::{
    snapshots::{StateSnapshot, StateSnapshotFetcher},
    types::Snapshots as StateSnapshots
};
use crate::{
    fs_handlers::types::FsOutData,
    hl_fs::{
        HyperliquidDirData, HyperliquidDirDataWithMeta,
        schemas::{NodeOrderStatusesRows, NodeRawBookDiffsRows}
    },
    processors::{
        HyperliquidDataProcessorHandle,
        l4_orderbook::{book::OrderBook, types::InnerL4Order}
    },
    types::{
        HyperliquidData, HyperliquidDataWithMeta, L4Book, L4BookDiff, L4BookUpdates, L4Order,
        L4OrderBuilder, L4OrderDiff, L4OrderStatus, ParsedDataPipelineMeta, Side
    },
    utils::unix_timestamp
};

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const PRICE_MULTIPLIER: f64 = 100_000_000.0;
const FETCH_SNAPSHOT_SLEEP_TIME_SEC: u64 = 5;

#[derive(Default)]
pub struct L4BookDeriver {
    order_status_cache: BatchQueue<L4OrderStatus>,
    book_diff_cache:    BatchQueue<L4BookDiff>,
    order_books:        HashMap<String, OrderBook>,
    state_snapshot:     StateSnapshotFetcher,
    snapshot_height:    Option<u64>
}

impl L4BookDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn order_count(&self) -> usize {
        self.order_books.values().map(OrderBook::order_count).sum()
    }

    pub fn pending_batch_count(&self) -> usize {
        self.order_status_cache.len() + self.book_diff_cache.len()
    }

    fn is_ready(&self) -> bool {
        self.snapshot_height.is_some()
    }

    fn try_initialize_from_snapshot(&mut self) -> eyre::Result<bool> {
        if self.is_ready() {
            return Ok(true);
        }

        let Some(StateSnapshot { height, snapshots }) = self.state_snapshot.write(Option::take)?
        else {
            return Ok(false);
        };

        self.order_books = order_books_from_snapshot(snapshots)?;
        self.snapshot_height = Some(height);

        if let Err(error) = self.apply_cached_batches() {
            tracing::info!(
                ?error,
                "Failed to apply updates to this book (likely missing older updates). Waiting for \
                 next snapshot."
            );
            self.order_books.clear();
            self.snapshot_height = None;
            self.state_snapshot.fetch_new();
            return Ok(false);
        }

        tracing::info!(
            snapshot_height = height,
            current_height = self.snapshot_height.unwrap_or(height),
            order_count = self.order_count(),
            "l4 order book ready"
        );
        Ok(true)
    }

    fn receive_order_statuses(
        &mut self,
        rows: &[NodeOrderStatusesRows],
        fs_data: &Arc<FsOutData>
    ) -> eyre::Result<()> {
        for row in rows {
            self.order_status_cache
                .push(order_status_batch(row, fs_data)?)?;
        }

        Ok(())
    }

    fn receive_book_diffs(
        &mut self,
        rows: &[NodeRawBookDiffsRows],
        fs_data: &Arc<FsOutData>
    ) -> eyre::Result<()> {
        for row in rows {
            self.book_diff_cache.push(book_diff_batch(row, fs_data)?)?;
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
            if !self.apply_cached_batch(&order_statuses, &book_diffs)? {
                continue;
            }

            let mut pipeline_meta =
                combine_pipeline_meta(order_statuses.pipeline_meta, book_diffs.pipeline_meta);
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

        Ok(true)
    }

    fn apply_updates(
        &mut self,
        order_statuses: &[L4OrderStatus],
        book_diffs: &[L4BookDiff]
    ) -> eyre::Result<()> {
        let mut order_map = order_statuses
            .iter()
            .filter(|order_status| is_inserted_into_book(order_status))
            .map(|order_status| (order_status.order.oid, order_status))
            .collect::<HashMap<_, _>>();

        for diff in book_diffs {
            match &diff.raw_book_diff {
                L4OrderDiff::New { sz } => {
                    let Some(order_status) = order_map.remove(&diff.oid) else {
                        return Err(eyre::eyre!("unable to find order opening status: {diff:?}"));
                    };
                    let mut order = order_status.order.clone();
                    order.user = Some(order_status.user.clone());
                    order.sz = sz.clone();
                    convert_trigger(&mut order, &order_status.time)?;
                    self.order_books
                        .entry(order.coin.clone())
                        .or_default()
                        .add_order(order)?;
                }
                L4OrderDiff::Update { new_sz, .. } => {
                    if !self
                        .order_books
                        .get_mut(&diff.coin)
                        .is_some_and(|book| book.modify_sz(diff.oid, new_sz.clone()))
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

impl HyperliquidDataProcessorHandle for L4BookDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Option<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        match &data.data {
            HyperliquidDirData::NodeOrderStatuses(rows) => {
                self.receive_order_statuses(rows, &data.pipeline_meta)?;
            }
            HyperliquidDirData::NodeRawBookDiffs(rows) => {
                self.receive_book_diffs(rows, &data.pipeline_meta)?;
            }
            _ => return Ok(None)
        }

        if !self.try_initialize_from_snapshot()? {
            return Ok(None);
        }

        let updates = self.process_ready_batches(processing_data_at_ns)?;
        if updates.is_empty() { Ok(None) } else { Ok(Some(HyperliquidData::L4Book(updates))) }
    }
}

#[derive(Clone)]
struct CachedBatch<T> {
    block_number:  u64,
    time:          u64,
    events:        Vec<T>,
    pipeline_meta: ParsedDataPipelineMeta
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

fn order_books_from_snapshot(
    snapshots: StateSnapshots<InnerL4Order>
) -> eyre::Result<HashMap<String, OrderBook>> {
    let mut order_books = HashMap::new();

    for (coin, snapshot) in snapshots.value() {
        let mut order_book = OrderBook::default();
        for order in snapshot.into_levels().into_iter().flatten() {
            order_book.add_order(order_from_snapshot_order(order))?;
        }
        order_books.insert(coin.value(), order_book);
    }

    Ok(order_books)
}

fn order_from_snapshot_order(order: InnerL4Order) -> L4Order {
    L4Order {
        user:              Some(order.user),
        coin:              order.coin.value(),
        side:              order.side,
        limit_px:          order.limit_px.to_str(),
        sz:                order.sz.to_str(),
        oid:               order.oid,
        timestamp:         order.timestamp,
        trigger_condition: order.trigger_condition,
        is_trigger:        order.is_trigger,
        trigger_px:        order.trigger_px,
        is_position_tpsl:  order.is_position_tpsl,
        reduce_only:       order.reduce_only,
        order_type:        order.order_type,
        tif:               order.tif,
        cloid:             order.cloid
    }
}

fn order_status_batch(
    row: &NodeOrderStatusesRows,
    fs_data: &Arc<FsOutData>
) -> eyre::Result<CachedBatch<L4OrderStatus>> {
    let row = serde_json::to_value(row)?;
    let block_number = u64_field(&row, "block_number")?;
    let time = timestamp_millis(string_field(&row, "block_time")?)?;
    let events = array_field(&row, "events")?
        .iter()
        .map(order_status_from_value)
        .collect::<eyre::Result<Vec<_>>>()?;

    Ok(CachedBatch { block_number, time, events, pipeline_meta: pipeline_meta(fs_data) })
}

fn book_diff_batch(
    row: &NodeRawBookDiffsRows,
    fs_data: &Arc<FsOutData>
) -> eyre::Result<CachedBatch<L4BookDiff>> {
    let row = serde_json::to_value(row)?;
    let block_number = u64_field(&row, "block_number")?;
    let time = timestamp_millis(string_field(&row, "block_time")?)?;
    let events = array_field(&row, "events")?
        .iter()
        .map(book_diff_from_value)
        .collect::<eyre::Result<Vec<_>>>()?;

    Ok(CachedBatch { block_number, time, events, pipeline_meta: pipeline_meta(fs_data) })
}

fn order_status_from_value(value: &Value) -> eyre::Result<L4OrderStatus> {
    Ok(L4OrderStatus {
        time:    string_field(value, "time")?.to_string(),
        user:    string_field(value, "user")?.to_string(),
        hash:    optional_string_field(value, "hash")?,
        builder: optional_object_field(value, "builder")?
            .map(builder_from_value)
            .transpose()?,
        status:  string_field(value, "status")?.to_string(),
        order:   order_from_value(json_field(value, "order")?)?
    })
}

fn builder_from_value(value: &Value) -> eyre::Result<L4OrderBuilder> {
    Ok(L4OrderBuilder { b: string_field(value, "b")?.to_string(), f: u64_field(value, "f")? })
}

fn book_diff_from_value(value: &Value) -> eyre::Result<L4BookDiff> {
    Ok(L4BookDiff {
        user:          string_field(value, "user")?.to_string(),
        oid:           u64_field(value, "oid")?,
        coin:          string_field(value, "coin")?.to_string(),
        side:          optional_string_field(value, "side")?
            .map(|side| side_from_str(&side))
            .transpose()?,
        px:            decimal_string(json_field(value, "px")?)?,
        raw_book_diff: order_diff_from_value(json_field(value, "raw_book_diff")?)?
    })
}

fn order_from_value(value: &Value) -> eyre::Result<L4Order> {
    Ok(L4Order {
        user:              optional_string_field(value, "user")?,
        coin:              string_field(value, "coin")?.to_string(),
        side:              side_from_str(string_field(value, "side")?)?,
        limit_px:          decimal_string(json_field(value, "limitPx")?)?,
        sz:                decimal_string(json_field(value, "sz")?)?,
        oid:               u64_field(value, "oid")?,
        timestamp:         u64_field(value, "timestamp")?,
        trigger_condition: string_field(value, "triggerCondition")?.to_string(),
        is_trigger:        bool_field(value, "isTrigger")?,
        trigger_px:        decimal_string(json_field(value, "triggerPx")?)?,
        is_position_tpsl:  bool_field(value, "isPositionTpsl")?,
        reduce_only:       bool_field(value, "reduceOnly")?,
        order_type:        string_field(value, "orderType")?.to_string(),
        tif:               optional_string_field(value, "tif")?,
        cloid:             optional_string_field(value, "cloid")?
    })
}

fn order_diff_from_value(value: &Value) -> eyre::Result<L4OrderDiff> {
    if value.as_str() == Some("remove") {
        return Ok(L4OrderDiff::Remove);
    }

    let object = value
        .as_object()
        .ok_or_else(|| eyre::eyre!("expected raw book diff object"))?;
    if let Some(new) = object.get("new") {
        return Ok(L4OrderDiff::New { sz: decimal_string(json_field(new, "sz")?)? });
    }
    if let Some(update) = object.get("update") {
        return Ok(L4OrderDiff::Update {
            orig_sz: decimal_string(json_field(update, "origSz")?)?,
            new_sz:  decimal_string(json_field(update, "newSz")?)?
        });
    }

    Err(eyre::eyre!("unknown raw book diff variant: {value}"))
}

fn coin_to_book_updates(
    order_statuses: Vec<L4OrderStatus>,
    book_diffs: Vec<L4BookDiff>,
    time: u64,
    height: u64
) -> Vec<L4BookUpdates> {
    let mut updates = BTreeMap::<String, L4BookUpdates>::new();

    for diff in book_diffs {
        updates
            .entry(diff.coin.clone())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .book_diffs
            .push(diff);
    }

    for status in order_statuses {
        updates
            .entry(status.order.coin.clone())
            .or_insert_with(|| L4BookUpdates::new(time, height))
            .order_statuses
            .push(status);
    }

    updates.into_values().collect()
}

fn is_inserted_into_book(order_status: &L4OrderStatus) -> bool {
    (order_status.status == "open"
        && !order_status.order.is_trigger
        && order_status.order.tif.as_deref() != Some("Ioc"))
        || (order_status.order.is_trigger && order_status.status == "triggered")
}

fn convert_trigger(order: &mut L4Order, status_time: &str) -> eyre::Result<()> {
    if order.is_trigger {
        order.trigger_px = "0.0".to_string();
        order.trigger_condition = "Triggered".to_string();
        order.is_trigger = false;
        order.timestamp = timestamp_millis(status_time)?;
        order.tif = Some("Gtc".to_string());
    }

    Ok(())
}

fn side_from_str(side: &str) -> eyre::Result<Side> {
    match side {
        "A" => Ok(Side::Ask),
        "B" => Ok(Side::Bid),
        _ => Err(eyre::eyre!("invalid L4 side: {side}"))
    }
}

fn timestamp_millis(value: &str) -> eyre::Result<u64> {
    let (date, time) = value
        .split_once('T')
        .ok_or_else(|| eyre::eyre!("invalid timestamp: {value}"))?;
    let mut date_parts = date.split('-');
    let year = parse_i64(date_parts.next(), "year", value)?;
    let month = parse_i64(date_parts.next(), "month", value)?;
    let day = parse_i64(date_parts.next(), "day", value)?;
    if date_parts.next().is_some() {
        return Err(eyre::eyre!("invalid timestamp date: {value}"));
    }

    let (time, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut time_parts = time.split(':');
    let hour = parse_i64(time_parts.next(), "hour", value)?;
    let minute = parse_i64(time_parts.next(), "minute", value)?;
    let second = parse_i64(time_parts.next(), "second", value)?;
    if time_parts.next().is_some() {
        return Err(eyre::eyre!("invalid timestamp time: {value}"));
    }

    let millis = days_from_civil(year, month, day)
        .checked_mul(86_400_000)
        .and_then(|millis| millis.checked_add(hour.checked_mul(3_600_000)?))
        .and_then(|millis| millis.checked_add(minute.checked_mul(60_000)?))
        .and_then(|millis| millis.checked_add(second.checked_mul(1_000)?))
        .and_then(|millis| millis.checked_add(fraction_millis(fraction)?))
        .ok_or_else(|| eyre::eyre!("timestamp overflow: {value}"))?;
    if millis < 0 {
        return Err(eyre::eyre!("negative timestamp is unsupported: {value}"));
    }

    Ok(millis as u64)
}

fn parse_i64(value: Option<&str>, field: &str, timestamp: &str) -> eyre::Result<i64> {
    value
        .ok_or_else(|| eyre::eyre!("missing {field} in timestamp: {timestamp}"))?
        .parse()
        .map_err(Into::into)
}

fn fraction_millis(fraction: &str) -> Option<i64> {
    let mut millis = 0_i64;
    for (idx, byte) in fraction.as_bytes().iter().take(3).enumerate() {
        if !byte.is_ascii_digit() {
            return None;
        }
        let digit = i64::from(byte - b'0');
        millis += digit
            * match idx {
                0 => 100,
                1 => 10,
                _ => 1
            };
    }

    Some(millis)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn pipeline_meta(fs_data: &Arc<FsOutData>) -> ParsedDataPipelineMeta {
    let mut pipeline_meta = ParsedDataPipelineMeta::default();
    pipeline_meta.modify_with_fs_data(fs_data);
    pipeline_meta
}

fn combine_pipeline_meta(
    mut left: ParsedDataPipelineMeta,
    right: ParsedDataPipelineMeta
) -> ParsedDataPipelineMeta {
    left.latest_notification_received_at_ns = left
        .latest_notification_received_at_ns
        .max(right.latest_notification_received_at_ns);
    left
}

fn json_field<'a>(value: &'a Value, field: &str) -> eyre::Result<&'a Value> {
    value
        .get(field)
        .ok_or_else(|| eyre::eyre!("missing field `{field}` in {value}"))
}

fn array_field<'a>(value: &'a Value, field: &str) -> eyre::Result<&'a [Value]> {
    json_field(value, field)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| eyre::eyre!("field `{field}` is not an array in {value}"))
}

fn optional_object_field<'a>(value: &'a Value, field: &str) -> eyre::Result<Option<&'a Value>> {
    match value.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) if value.is_object() => Ok(Some(value)),
        Some(value) => Err(eyre::eyre!("field `{field}` is not an object or null in {value}"))
    }
}

fn string_field<'a>(value: &'a Value, field: &str) -> eyre::Result<&'a str> {
    json_field(value, field)?
        .as_str()
        .ok_or_else(|| eyre::eyre!("field `{field}` is not a string in {value}"))
}

fn optional_string_field(value: &Value, field: &str) -> eyre::Result<Option<String>> {
    match value.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(value) => Err(eyre::eyre!("field `{field}` is not a string or null in {value}"))
    }
}

fn bool_field(value: &Value, field: &str) -> eyre::Result<bool> {
    json_field(value, field)?
        .as_bool()
        .ok_or_else(|| eyre::eyre!("field `{field}` is not a bool in {value}"))
}

fn u64_field(value: &Value, field: &str) -> eyre::Result<u64> {
    json_field(value, field)?
        .as_u64()
        .ok_or_else(|| eyre::eyre!("field `{field}` is not an unsigned integer in {value}"))
}

fn decimal_string(value: &Value) -> eyre::Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => value
            .as_f64()
            .map(format_decimal)
            .ok_or_else(|| eyre::eyre!("number is not representable as f64: {value}")),
        _ => Err(eyre::eyre!("expected decimal string or number, got {value}"))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::{
        L4BookDeriver,
        snapshots::{StateSnapshot, StateSnapshotFetcher},
        types::Snapshots
    };
    use crate::{
        fs_handlers::types::FsOutData,
        hl_fs::{
            HyperliquidDirData, HyperliquidDirDataWithMeta, HyperliquidDirKind,
            schemas::{NodeOrderStatusesRows, NodeRawBookDiffsRows}
        },
        processors::HyperliquidDataProcessorHandle,
        types::{HyperliquidData, L4Book, L4OrderDiff}
    };

    #[test]
    fn waits_for_snapshot_before_processing_batches() {
        let mut deriver = deriver_without_snapshot();

        let first = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeOrderStatuses(vec![status_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeOrderStatuses)
            })
            .unwrap();

        assert!(first.is_none());
        assert_eq!(deriver.pending_batch_count(), 1);

        let second = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap();

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
            .unwrap();

        assert!(first.is_none());
        assert_eq!(deriver.pending_batch_count(), 1);

        let second = deriver
            .handle_data(&HyperliquidDirDataWithMeta {
                data:          HyperliquidDirData::NodeRawBookDiffs(vec![diff_row()]),
                pipeline_meta: fs_data(HyperliquidDirKind::NodeRawBookDiffs)
            })
            .unwrap()
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

    fn deriver_without_snapshot() -> L4BookDeriver {
        L4BookDeriver {
            order_status_cache: Default::default(),
            book_diff_cache:    Default::default(),
            order_books:        Default::default(),
            state_snapshot:     StateSnapshotFetcher::empty(),
            snapshot_height:    None
        }
    }

    fn deriver_with_snapshot(height: u64) -> L4BookDeriver {
        L4BookDeriver {
            order_status_cache: Default::default(),
            book_diff_cache:    Default::default(),
            order_books:        Default::default(),
            state_snapshot:     StateSnapshotFetcher::with_snapshot(StateSnapshot {
                height,
                snapshots: Snapshots::new(HashMap::new())
            }),
            snapshot_height:    None
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
