use std::collections::{BTreeMap, VecDeque};

use crate::{
    fs_handlers::types::FsOutData,
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
    pub order_statuses:         BTreeMap<u64, AccumulatedBatch<L4OrderStatus>>,
    pub book_diffs:             BTreeMap<u64, AccumulatedBatch<L4BookDiff>>,
    pub next_block_to_finalize: Option<u64>
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

        self.book_diffs
            .entry(block_number)
            .or_insert_with(|| AccumulatedBatch::new(time))
            .extend(time, events, fs_data);
    }

    pub fn pop_ready_batches(
        &mut self
    ) -> Vec<(CachedBatch<L4OrderStatus>, CachedBatch<L4BookDiff>)> {
        let Some(mut block_number) = self
            .next_block_to_finalize
            .or_else(|| self.first_pending_block())
        else {
            return Vec::new();
        };

        // Finalize the longest contiguous run of blocks that BOTH streams have
        // delivered, mirroring order_book_server's lockstep replay. A block is
        // only finalized once its immediate successor has also arrived in both
        // streams: that proves the block is complete (all of its possibly
        // multi-row events have been merged) and lets us stop at the first
        // not-yet-delivered block instead of synthesizing an empty one.
        // Synthesizing empties for blocks still in flight dropped the opens they
        // carried, which made later Remove/Update diffs reference orders that
        // were never added to the book.
        let mut out = Vec::new();
        while self.has_block(block_number) && self.has_block(block_number + 1) {
            let order_statuses = self
                .order_statuses
                .remove(&block_number)
                .expect("order statuses present per has_block");
            let book_diffs = self
                .book_diffs
                .remove(&block_number)
                .expect("book diffs present per has_block");

            let time = order_statuses.time;
            out.push((
                CachedBatch::from_meta(
                    block_number,
                    time,
                    order_statuses.events,
                    order_statuses.pipeline_meta
                ),
                CachedBatch::from_meta(
                    block_number,
                    time,
                    book_diffs.events,
                    book_diffs.pipeline_meta
                )
            ));

            block_number += 1;
        }

        self.next_block_to_finalize = Some(block_number);
        out
    }

    fn has_block(&self, block_number: u64) -> bool {
        self.order_statuses.contains_key(&block_number)
            && self.book_diffs.contains_key(&block_number)
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
