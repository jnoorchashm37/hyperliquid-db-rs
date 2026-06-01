mod trades;
use serde::{Deserialize, Serialize};
pub use trades::{PendingTrade, Trade, TradeSide};
mod all_mids;
pub use all_mids::AllMids;
use strum::IntoEnumIterator;

use crate::hl_fs::HyperliquidDirKind;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidData {
    Trades(Vec<Trade>)
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
