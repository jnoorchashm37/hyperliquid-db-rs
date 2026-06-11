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
    hl_fs::schemas::{
        Hip3OracleUpdatesRows, MiscEventsRows, NodeFillsRow, NodeOrderStatusesRows,
        NodeRawBookDiffsRows, ReplicaCmdsRows
    }
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
    NodeFills(Vec<NodeFillsRow>),
    Hip3OracleUpdates(Vec<Hip3OracleUpdatesRows>),
    MiscEvents(Vec<MiscEventsRows>),
    ReplicaCmds(Vec<ReplicaCmdsRows>)
}

impl HyperliquidDirData {
    pub fn kind(&self) -> HyperliquidDirKind {
        match self {
            HyperliquidDirData::NodeFills(_) => HyperliquidDirKind::NodeFills,
            HyperliquidDirData::NodeOrderStatuses(_) => HyperliquidDirKind::NodeOrderStatuses,
            HyperliquidDirData::NodeRawBookDiffs(_) => HyperliquidDirKind::NodeRawBookDiffs,
            HyperliquidDirData::Hip3OracleUpdates(_) => HyperliquidDirKind::Hip3OracleUpdates,
            HyperliquidDirData::MiscEvents(_) => HyperliquidDirKind::MiscEvents,
            HyperliquidDirData::ReplicaCmds(_) => HyperliquidDirKind::ReplicaCmds
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, strum::EnumIter)]
pub enum HyperliquidDirKind {
    NodeOrderStatuses,
    NodeRawBookDiffs,
    NodeFills,
    Hip3OracleUpdates,
    MiscEvents,
    ReplicaCmds
}

impl HyperliquidDirKind {
    pub fn dir_path(&self) -> PathBuf {
        let base_dir = Path::new(HYPERLIQUID_DATA_DIR);
        let ext_dir = match self {
            HyperliquidDirKind::NodeOrderStatuses => "node_order_statuses_streaming",
            HyperliquidDirKind::NodeRawBookDiffs => "node_raw_book_diffs_streaming",
            HyperliquidDirKind::NodeFills => "node_fills_streaming",
            HyperliquidDirKind::Hip3OracleUpdates => "hip3_oracle_updates_streaming",
            HyperliquidDirKind::MiscEvents => "misc_events_streaming",
            HyperliquidDirKind::ReplicaCmds => "replica_cmds"
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
            "hip3_oracle_updates_streaming" => Ok(Self::Hip3OracleUpdates),
            "misc_events_streaming" => Ok(Self::MiscEvents),
            "replica_cmds" => Ok(Self::ReplicaCmds),
            _ => Err(eyre::eyre!("invalid `HyperliquidDirKind`: {s}"))
        }
    }
}

impl fmt::Display for HyperliquidDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            HyperliquidDirKind::NodeOrderStatuses => "node_order_statuses_streaming",
            HyperliquidDirKind::NodeRawBookDiffs => "node_raw_book_diffs_streaming",
            HyperliquidDirKind::NodeFills => "node_fills_streaming",
            HyperliquidDirKind::Hip3OracleUpdates => "hip3_oracle_updates_streaming",
            HyperliquidDirKind::MiscEvents => "misc_events_streaming",
            HyperliquidDirKind::ReplicaCmds => "replica_cmds"
        };

        fmt::Display::fmt(s, f)
    }
}

impl fmt::Debug for HyperliquidDirKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
