use std::collections::{HashMap, VecDeque};

use crate::{
    fs_handlers::types::FsOutData,
    hl_fs::schemas::{NodeOrderStatusesRows, NodeRawBookDiffsRows},
    types::{
        HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, L2Book, L4Book,
        ParsedDataPipelineMeta
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

/// A single block fully assembled from both streaming feeds, ready to apply.
pub struct CompleteBlock {
    pub block_number:  u64,
    pub time:          u64,
    pub status_rows:   Vec<NodeOrderStatusesRows>,
    pub diff_rows:     Vec<NodeRawBookDiffsRows>,
    pub pipeline_meta: ParsedDataPipelineMeta
}

/// Reassembles the two independent node streams (order statuses and raw book
/// diffs) into ordered, complete, per-block batches.
///
/// The node writes each stream incrementally, and a block only appears in a
/// stream if it carried events for that stream - so the streams are sparse and
/// skewed relative to each other. A block is provably complete once BOTH
/// streams have delivered a strictly later block: events are written in block
/// order within a stream, so once a stream has moved on, any block it never
/// wrote a row for is genuinely empty rather than still in flight.
///
/// `try_process` therefore finalizes every block below
/// `min(order_status_watermark, raw_book_diff_watermark)`, empty-filling the
/// gaps. This replaces the old `has_block(N) && has_block(N+1)` lockstep, which
/// stalled forever the first time a block was empty in one stream.
#[derive(Default)]
pub struct BlockQueueCache {
    pub current_block:   u64,
    pub last_block_time: u64,
    pub order_statuses:  SingleBlockQueueCache<NodeOrderStatusesRows>,
    pub raw_book_diffs:  SingleBlockQueueCache<NodeRawBookDiffsRows>,
    pub block_meta:      HashMap<u64, ParsedDataPipelineMeta>
}

impl BlockQueueCache {
    pub fn new_order_statuses(&mut self, rows: Vec<NodeOrderStatusesRows>, fs_data: &FsOutData) {
        for row in rows {
            let block_number = row.block_number;
            self.order_statuses
                .block_values
                .entry(block_number)
                .or_default()
                .push(row);
            self.order_statuses.latest_block = self.order_statuses.latest_block.max(block_number);
            self.block_meta
                .entry(block_number)
                .or_default()
                .modify_with_fs_data(fs_data);
        }
    }

    pub fn new_raw_book_diffs(&mut self, rows: Vec<NodeRawBookDiffsRows>, fs_data: &FsOutData) {
        for row in rows {
            let block_number = row.block_number;
            self.raw_book_diffs
                .block_values
                .entry(block_number)
                .or_default()
                .push(row);
            self.raw_book_diffs.latest_block = self.raw_book_diffs.latest_block.max(block_number);
            self.block_meta
                .entry(block_number)
                .or_default()
                .modify_with_fs_data(fs_data);
        }
    }

    /// Finalize, in order, every block below the min watermark of the two
    /// streams. A block absent from a stream is emitted with no events for that
    /// side; a block absent from both inherits the previous block's time so it
    /// never stamps `time = 0` (which would suppress snapshot/L2 emission).
    pub fn try_process(&mut self) -> eyre::Result<Vec<CompleteBlock>> {
        let prev_block = self.current_block;
        self.current_block = self
            .order_statuses
            .latest_block
            .min(self.raw_book_diffs.latest_block);

        // The first observation only establishes the baseline watermark (we
        // cannot emit `[0, current)`); a stalled watermark means nothing new is
        // complete yet.
        if prev_block == 0 || self.current_block == prev_block {
            return Ok(Vec::new());
        }

        let mut blocks = Vec::with_capacity((self.current_block - prev_block) as usize);
        for block_number in prev_block..self.current_block {
            let status_rows = self
                .order_statuses
                .block_values
                .remove(&block_number)
                .unwrap_or_default();
            let diff_rows = self
                .raw_book_diffs
                .block_values
                .remove(&block_number)
                .unwrap_or_default();
            let pipeline_meta = self.block_meta.remove(&block_number).unwrap_or_default();

            let time = if let Some(row) = status_rows.first() {
                row.block_time_unix_ms()?
            } else if let Some(row) = diff_rows.first() {
                row.block_time_unix_ms()?
            } else {
                self.last_block_time
            };
            self.last_block_time = time;

            blocks.push(CompleteBlock {
                block_number,
                time,
                status_rows,
                diff_rows,
                pipeline_meta
            });
        }

        // Drop any stragglers still below the watermark (e.g. an initial
        // multi-block batch observed before the baseline was set). Both streams
        // have moved past them, so they will never complete.
        let cutoff = self.current_block;
        self.order_statuses
            .block_values
            .retain(|block_number, _| *block_number >= cutoff);
        self.raw_book_diffs
            .block_values
            .retain(|block_number, _| *block_number >= cutoff);
        self.block_meta
            .retain(|block_number, _| *block_number >= cutoff);

        Ok(blocks)
    }
}

pub struct SingleBlockQueueCache<T> {
    pub latest_block: u64,
    pub block_values: HashMap<u64, Vec<T>>
}

impl<T> Default for SingleBlockQueueCache<T> {
    fn default() -> Self {
        Self { latest_block: 0, block_values: HashMap::new() }
    }
}
