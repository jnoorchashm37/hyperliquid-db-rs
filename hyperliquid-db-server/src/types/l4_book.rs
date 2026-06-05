use hyperliquid_db_core::types::{
    L4Book, L4BookDiff, L4BookUpdates, L4Order, L4OrderBuilder, L4OrderDiff, L4OrderStatus
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RpcL4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<RpcL4Order>; 2] },
    Updates(RpcL4BookUpdates)
}

impl TryFrom<L4Book> for RpcL4Book {
    type Error = eyre::ErrReport;

    fn try_from(value: L4Book) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RpcL4BookUpdates {
    pub time:           u64,
    pub height:         u64,
    pub order_statuses: Vec<RpcL4OrderStatus>,
    pub book_diffs:     Vec<RpcL4BookDiff>
}

impl TryFrom<L4BookUpdates> for RpcL4BookUpdates {
    type Error = eyre::ErrReport;

    fn try_from(value: L4BookUpdates) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RpcL4OrderStatus {
    pub time:    String,
    pub user:    String,
    #[serde(default)]
    pub hash:    Option<String>,
    #[serde(default)]
    pub builder: Option<RpcL4OrderBuilder>,
    pub status:  String,
    pub order:   RpcL4Order
}

impl TryFrom<L4OrderStatus> for RpcL4OrderStatus {
    type Error = eyre::ErrReport;

    fn try_from(value: L4OrderStatus) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RpcL4OrderBuilder {
    pub b: String,
    pub f: u64
}

impl TryFrom<L4OrderBuilder> for RpcL4OrderBuilder {
    type Error = eyre::ErrReport;

    fn try_from(value: L4OrderBuilder) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RpcL4BookDiff {
    pub user:          String,
    pub oid:           u64,
    pub coin:          String,
    #[serde(default)]
    pub side:          Option<String>,
    pub px:            String,
    pub raw_book_diff: RpcL4OrderDiff
}

impl TryFrom<L4BookDiff> for RpcL4BookDiff {
    type Error = eyre::ErrReport;

    fn try_from(value: L4BookDiff) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcL4Order {
    pub user:              Option<String>,
    pub coin:              String,
    pub side:              String,
    pub limit_px:          String,
    pub sz:                String,
    pub oid:               u64,
    pub timestamp:         u64,
    pub trigger_condition: String,
    pub is_trigger:        bool,
    pub trigger_px:        String,
    pub is_position_tpsl:  bool,
    pub reduce_only:       bool,
    pub order_type:        String,
    pub tif:               Option<String>,
    pub cloid:             Option<String>
}

impl TryFrom<L4Order> for RpcL4Order {
    type Error = eyre::ErrReport;

    fn try_from(value: L4Order) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RpcL4OrderDiff {
    New { sz: String },
    Update { orig_sz: String, new_sz: String },
    Remove
}

impl TryFrom<L4OrderDiff> for RpcL4OrderDiff {
    type Error = eyre::ErrReport;

    fn try_from(value: L4OrderDiff) -> Result<Self, Self::Error> {
        todo!()
    }
}
