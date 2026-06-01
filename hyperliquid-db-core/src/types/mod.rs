mod trades;
pub use trades::*;
mod all_mids;
pub use all_mids::*;
use strum::IntoEnumIterator;

use crate::hl_fs::HyperliquidDirKind;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum HyperliquidData {
    Trades(Vec<Trade>)
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
