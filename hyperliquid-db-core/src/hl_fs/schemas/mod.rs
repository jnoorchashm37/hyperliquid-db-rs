mod node_fills;
pub use node_fills::*;
mod node_order_statuses;
pub use node_order_statuses::*;
mod hip3_oracle_updates;
pub use hip3_oracle_updates::*;
mod node_raw_book_diffs;
pub use node_raw_book_diffs::*;

mod misc_events;
pub use misc_events::*;

mod replica_cmds;
pub use replica_cmds::*;

use crate::hl_fs::HyperliquidDirData;

pub const NODE_DATA_DATE_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.f";

impl From<Vec<Hip3OracleUpdatesRows>> for HyperliquidDirData {
    fn from(value: Vec<Hip3OracleUpdatesRows>) -> Self {
        Self::Hip3OracleUpdates(value)
    }
}

impl From<Vec<NodeFillsRow>> for HyperliquidDirData {
    fn from(value: Vec<NodeFillsRow>) -> Self {
        Self::NodeFills(value)
    }
}

impl From<Vec<NodeOrderStatusesRows>> for HyperliquidDirData {
    fn from(value: Vec<NodeOrderStatusesRows>) -> Self {
        Self::NodeOrderStatuses(value)
    }
}

impl From<Vec<NodeRawBookDiffsRows>> for HyperliquidDirData {
    fn from(value: Vec<NodeRawBookDiffsRows>) -> Self {
        Self::NodeRawBookDiffs(value)
    }
}

impl From<Vec<ReplicaCmdsRows>> for HyperliquidDirData {
    fn from(value: Vec<ReplicaCmdsRows>) -> Self {
        Self::ReplicaCmds(value)
    }
}

impl From<Vec<MiscEventsRows>> for HyperliquidDirData {
    fn from(value: Vec<MiscEventsRows>) -> Self {
        Self::MiscEvents(value)
    }
}
