use std::collections::{BTreeMap, VecDeque};

use crate::{
    fs_handlers::types::FsOutData,
    processors::orderbook::STREAMING_FINALIZATION_BLOCK_DELAY,
    types::{
        HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, L2Book, L4Book, L4BookDiff,
        L4OrderStatus, ParsedDataPipelineMeta
    }
};

#[derive(Default)]
pub struct ProcessedOrderBookData {
    pub l4: Vec<HyperliquidDataWithMeta<L4Book>>,
    pub l2: Vec<HyperliquidDataWithMeta<L2Book>>
}

impl ProcessedOrderBookData {
    pub fn extend(&mut self, other: Self) {
        self.l4.extend(other.l4);
        self.l2.extend(other.l2);
    }

    pub fn is_empty(&self) -> bool {
        self.l4.is_empty() && self.l2.is_empty()
    }

    pub fn into_hyperliquid_data(self) -> Vec<HyperliquidData> {
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
pub struct CachedBatch<T> {
    pub block_number:  u64,
    pub time:          u64,
    pub events:        Vec<T>,
    pub pipeline_meta: ParsedDataPipelineMeta
}

impl<T> CachedBatch<T> {
    pub fn from_meta(
        block_number: u64,
        time: u64,
        events: Vec<T>,
        pipeline_meta: ParsedDataPipelineMeta
    ) -> Self {
        Self { block_number, time, events, pipeline_meta }
    }
}

#[derive(Default)]
pub struct StreamingBlockCache {
    pub order_statuses:            BTreeMap<u64, AccumulatedBatch<L4OrderStatus>>,
    pub book_diffs:                BTreeMap<u64, AccumulatedBatch<L4BookDiff>>,
    pub latest_order_status_block: Option<u64>,
    pub latest_book_diff_block:    Option<u64>,
    pub next_block_to_finalize:    Option<u64>,
    pub last_finalized_time:       u64
}

impl StreamingBlockCache {
    pub fn push_order_statuses(
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

    pub fn push_book_diffs(
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

    pub fn pop_ready_batches(
        &mut self
    ) -> Vec<(CachedBatch<L4OrderStatus>, CachedBatch<L4BookDiff>)> {
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

    pub fn start_at(&mut self, block_number: u64) {
        let next_block = self
            .next_block_to_finalize
            .map_or(block_number, |next_block| next_block.max(block_number));
        self.next_block_to_finalize = Some(next_block);
        self.order_statuses
            .retain(|block_number, _| *block_number >= next_block);
        self.book_diffs
            .retain(|block_number, _| *block_number >= next_block);
    }

    pub fn order_status_batch_count(&self) -> usize {
        self.order_statuses.len()
    }

    pub fn book_diff_batch_count(&self) -> usize {
        self.book_diffs.len()
    }

    pub fn is_finalized(&self, block_number: u64) -> bool {
        self.next_block_to_finalize
            .is_some_and(|next_block| block_number < next_block)
    }

    pub fn ready_upper_exclusive(&self) -> Option<u64> {
        let lower_watermark = self
            .latest_order_status_block?
            .min(self.latest_book_diff_block?);

        lower_watermark.checked_sub(STREAMING_FINALIZATION_BLOCK_DELAY.saturating_sub(1))
    }

    pub fn first_pending_block(&self) -> Option<u64> {
        self.order_statuses
            .keys()
            .chain(self.book_diffs.keys())
            .min()
            .copied()
    }
}

pub struct AccumulatedBatch<T> {
    pub time:          u64,
    pub events:        Vec<T>,
    pub pipeline_meta: ParsedDataPipelineMeta
}

impl<T> AccumulatedBatch<T> {
    pub fn new(time: u64) -> Self {
        Self { time, events: Vec::new(), pipeline_meta: ParsedDataPipelineMeta::default() }
    }

    pub fn extend(&mut self, time: u64, events: Vec<T>, fs_data: &FsOutData) {
        self.time = time;
        self.events.extend(events);
        self.pipeline_meta.modify_with_fs_data(fs_data);
    }
}

pub struct BatchQueue<T> {
    pub deque:      VecDeque<CachedBatch<T>>,
    pub last_block: Option<u64>
}

impl<T> Default for BatchQueue<T> {
    fn default() -> Self {
        Self { deque: VecDeque::new(), last_block: None }
    }
}

impl<T> BatchQueue<T> {
    pub fn push(&mut self, batch: CachedBatch<T>) -> eyre::Result<()> {
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

    pub fn pop_front(&mut self) -> Option<CachedBatch<T>> {
        self.deque.pop_front()
    }

    pub fn front(&self) -> Option<&CachedBatch<T>> {
        self.deque.front()
    }

    pub fn len(&self) -> usize {
        self.deque.len()
    }
}

#[derive(Clone, Copy)]
pub struct OrderBookOutputs {
    pub l4: bool,
    pub l2: bool
}

impl Default for OrderBookOutputs {
    fn default() -> Self {
        Self { l4: true, l2: false }
    }
}

impl OrderBookOutputs {
    pub fn for_data_kinds(data_kinds: &[HyperliquidDataKind]) -> Self {
        Self {
            l4: data_kinds.contains(&HyperliquidDataKind::L4Book),
            l2: data_kinds.contains(&HyperliquidDataKind::L2Book)
        }
    }
}
