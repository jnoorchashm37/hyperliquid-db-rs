use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc
};

use strum::IntoEnumIterator;

use crate::{
    HYPERLIQUID_DATA_DIR,
    fs_handlers::types::FsOutData,
    hl_fs::schemas::{NodeFillsRow, NodeOrderStatusesRows, NodeRawBookDiffsRows}
};

#[derive(Debug, Clone)]
pub struct HyperliquidDirDataWithMeta {
    pub data:          HyperliquidDirData,
    pub pipeline_meta: Arc<FsOutData>
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum HyperliquidDirData {
    NodeOrderStatuses(Vec<NodeOrderStatusesRows>),
    NodeRawBookDiffs(Vec<NodeRawBookDiffsRows>),
    NodeFills(Vec<NodeFillsRow>)
}

impl HyperliquidDirData {
    pub fn kind(&self) -> HyperliquidDirKind {
        match self {
            HyperliquidDirData::NodeFills(_) => HyperliquidDirKind::NodeFills,
            HyperliquidDirData::NodeOrderStatuses(_) => HyperliquidDirKind::NodeOrderStatuses,
            HyperliquidDirData::NodeRawBookDiffs(_) => HyperliquidDirKind::NodeRawBookDiffs
        }
    }
}

impl From<Vec<NodeFillsRow>> for HyperliquidDirData {
    fn from(value: Vec<NodeFillsRow>) -> Self {
        Self::NodeFills(value)
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, strum::EnumIter)]
pub enum HyperliquidDirKind {
    NodeOrderStatuses,
    NodeRawBookDiffs,
    NodeFills
}

impl HyperliquidDirKind {
    pub fn dir_path(&self) -> PathBuf {
        let base_dir = Path::new(HYPERLIQUID_DATA_DIR);
        let ext_dir = match self {
            HyperliquidDirKind::NodeOrderStatuses => "node_order_statuses_streaming",
            HyperliquidDirKind::NodeRawBookDiffs => "node_raw_book_diffs_streaming",
            HyperliquidDirKind::NodeFills => "node_fills_streaming"
        };

        base_dir.join(ext_dir)
    }

    pub fn all() -> Vec<Self> {
        Self::iter().collect()
    }
}

impl FromStr for HyperliquidDirKind {
    type Err = eyre::ErrReport;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "node_order_statuses_streaming" => Ok(Self::NodeOrderStatuses),
            "node_raw_book_diffs_streaming" => Ok(Self::NodeRawBookDiffs),
            "node_fills_streaming" => Ok(Self::NodeFills),
            _ => Err(eyre::eyre!("invalid `HyperliquidDirKind`: {s}"))
        }
    }
}

impl fmt::Display for HyperliquidDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            HyperliquidDirKind::NodeOrderStatuses => "node_order_statuses_streaming",
            HyperliquidDirKind::NodeRawBookDiffs => "node_raw_book_diffs_streaming",
            HyperliquidDirKind::NodeFills => "node_fills_streaming"
        };

        fmt::Display::fmt(s, f)
    }
}

impl fmt::Debug for HyperliquidDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
