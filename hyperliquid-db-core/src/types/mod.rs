mod trades;

mod l4_book;
pub use l4_book::{
    L4Book, L4BookDiff, L4BookUpdates, L4Order, L4OrderBuilder, L4OrderDiff, L4OrderStatus
};

mod common;
pub use common::*;
use serde::{Deserialize, Serialize};
pub use trades::{PendingTrade, Trade};
mod l2_book;
pub use l2_book::{L2Book, L2BookLevel};
use strum::IntoEnumIterator;

use crate::{
    fs_handlers::types::FsOutData, hl_fs::HyperliquidDirKind,
    processors::HyperliquidDataProcessorKind
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct HyperliquidDataWithMeta<D> {
    pub data:          D,
    pub pipeline_meta: ParsedDataPipelineMeta
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidData {
    Trades(Vec<HyperliquidDataWithMeta<Trade>>),
    L4Book(Vec<HyperliquidDataWithMeta<L4Book>>),
    L2Book(Vec<HyperliquidDataWithMeta<L2Book>>)
}

impl HyperliquidData {
    pub fn kind(&self) -> HyperliquidDataKind {
        match self {
            HyperliquidData::Trades(_) => HyperliquidDataKind::Trades,
            HyperliquidData::L4Book(_) => HyperliquidDataKind::L4Book,
            HyperliquidData::L2Book(_) => HyperliquidDataKind::L2Book
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter)]
pub enum HyperliquidDataKind {
    Trades,
    L4Book,
    L2Book
}

impl HyperliquidDataKind {
    pub fn all() -> Vec<Self> {
        Self::iter().collect()
    }

    pub fn required_dirs(&self) -> Vec<HyperliquidDirKind> {
        match self {
            HyperliquidDataKind::Trades => vec![HyperliquidDirKind::NodeFills],
            HyperliquidDataKind::L4Book => {
                vec![HyperliquidDirKind::NodeOrderStatuses, HyperliquidDirKind::NodeRawBookDiffs]
            }
            HyperliquidDataKind::L2Book => vec![HyperliquidDirKind::NodeRawBookDiffs]
        }
    }

    pub fn processor_kind(&self) -> HyperliquidDataProcessorKind {
        match self {
            HyperliquidDataKind::Trades => HyperliquidDataProcessorKind::Trades,
            HyperliquidDataKind::L4Book | HyperliquidDataKind::L2Book => {
                HyperliquidDataProcessorKind::Orderbook
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ParsedDataPipelineMeta {
    /// latest `notification_received_at_ns` from the filesystem reciever
    /// `FsOutData`
    pub latest_notification_received_at_ns: u128,
    pub processing_data_at_ns:              u128,
    pub processed_data_at_ns:               u128
}

impl ParsedDataPipelineMeta {
    pub fn modify_with_fs_data(&mut self, fs_data: &FsOutData) {
        self.latest_notification_received_at_ns = std::cmp::max(
            fs_data.notification_received_at_ns,
            self.latest_notification_received_at_ns
        );
    }
}
