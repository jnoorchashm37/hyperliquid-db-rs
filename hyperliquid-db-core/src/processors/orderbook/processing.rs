use std::collections::VecDeque;

use crate::types::{
    HyperliquidData, HyperliquidDataKind, HyperliquidDataWithMeta, L2Book, L4Book,
    ParsedDataPipelineMeta
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

//
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
