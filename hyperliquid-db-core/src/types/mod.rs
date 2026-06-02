mod trades;

use serde::{Deserialize, Serialize};
pub use trades::{PendingTrade, Trade, TradeSide};
mod l2_orderbook;
pub use l2_orderbook::L2Book;
use strum::IntoEnumIterator;

use crate::{fs_handlers::types::FsOutData, hl_fs::HyperliquidDirKind};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct HyperliquidDataWithMeta<D> {
    pub data:          D,
    pub pipeline_meta: ParsedDataPipelineMeta
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidData {
    Trades(Vec<HyperliquidDataWithMeta<Trade>>)
}

impl HyperliquidData {
    pub fn kind(&self) -> HyperliquidDataKind {
        match self {
            HyperliquidData::Trades(_) => HyperliquidDataKind::Trades
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter)]
pub enum HyperliquidDataKind {
    Trades
}

impl HyperliquidDataKind {
    pub fn all() -> Vec<Self> {
        Self::iter().collect()
    }

    pub fn required_dirs(&self) -> Vec<HyperliquidDirKind> {
        match self {
            HyperliquidDataKind::Trades => vec![HyperliquidDirKind::NodeFills]
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
