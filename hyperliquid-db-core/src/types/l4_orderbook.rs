use serde::{Deserialize, Serialize};

use crate::types::Side;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct L4BookUpdates {
    pub time:           u64,
    pub height:         u64,
    pub order_statuses: Vec<L4OrderStatus>,
    pub book_diffs:     Vec<L4BookDiff>
}

impl L4BookUpdates {
    pub const fn new(time: u64, height: u64) -> Self {
        Self { time, height, order_statuses: Vec::new(), book_diffs: Vec::new() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct L4OrderStatus {
    pub time:    String,
    pub user:    String,
    #[serde(default)]
    pub hash:    Option<String>,
    #[serde(default)]
    pub builder: Option<L4OrderBuilder>,
    pub status:  String,
    pub order:   L4Order
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct L4OrderBuilder {
    pub b: String,
    pub f: u64
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct L4BookDiff {
    pub user:          String,
    pub oid:           u64,
    pub coin:          String,
    #[serde(default)]
    pub side:          Option<Side>,
    pub px:            String,
    pub raw_book_diff: L4OrderDiff
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L4Order {
    pub user:              Option<String>,
    pub coin:              String,
    pub side:              Side,
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

impl L4Order {
    pub fn order_size(&self) -> eyre::Result<f64> {
        Ok(self.sz.parse()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum L4OrderDiff {
    New { sz: String },
    Update { orig_sz: String, new_sz: String },
    Remove
}
