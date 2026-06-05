use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};

use crate::{
    hl_fs::schemas::{
        NODE_DATA_DATE_TIME_FORMAT, NodeOrderStatusesBuilder, NodeOrderStatusesEvent,
        NodeRawBookDiffsEvent, NodeRawBookDiffsRawBookDiff
    },
    types::Side
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates)
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
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

impl L4OrderStatus {
    pub fn is_inserted_into_book(&self) -> bool {
        (self.status == "open"
            && !self.order.is_trigger
            && self.order.tif.as_deref() != Some("Ioc"))
            || (self.order.is_trigger && self.status == "triggered")
    }

    pub fn time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }
}

impl TryFrom<NodeOrderStatusesEvent> for L4OrderStatus {
    type Error = eyre::ErrReport;

    fn try_from(value: NodeOrderStatusesEvent) -> Result<Self, Self::Error> {
        let order = value.clone().try_into()?;
        Ok(Self {
            time: value.time,
            user: value.user,
            hash: value.hash,
            builder: value.builder.map(TryInto::try_into).transpose()?,
            status: value.status,
            order
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct L4OrderBuilder {
    pub b: String,
    pub f: u64
}

impl TryFrom<NodeOrderStatusesBuilder> for L4OrderBuilder {
    type Error = eyre::ErrReport;

    fn try_from(value: NodeOrderStatusesBuilder) -> Result<Self, Self::Error> {
        Ok(Self { b: value.b, f: value.f })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct L4BookDiff {
    pub user:          String,
    pub oid:           u64,
    pub coin:          String,
    pub side:          Side,
    pub px:            f64,
    pub raw_book_diff: L4OrderDiff
}

impl TryFrom<NodeRawBookDiffsEvent> for L4BookDiff {
    type Error = eyre::ErrReport;

    fn try_from(value: NodeRawBookDiffsEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            user:          value.user,
            oid:           value.oid,
            coin:          value.coin,
            side:          value.side,
            px:            value.px,
            raw_book_diff: value.raw_book_diff.try_into()?
        })
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L4Order {
    pub user:              Option<String>,
    pub coin:              String,
    pub side:              Side,
    #[serde_as(as = "DisplayFromStr")]
    pub limit_px:          f64,
    #[serde_as(as = "DisplayFromStr")]
    pub sz:                f64,
    pub oid:               u64,
    pub timestamp:         u64,
    pub trigger_condition: String,
    pub is_trigger:        bool,
    #[serde_as(as = "DisplayFromStr")]
    pub trigger_px:        f64,
    pub is_position_tpsl:  bool,
    pub reduce_only:       bool,
    pub order_type:        String,
    pub tif:               Option<String>,
    pub cloid:             Option<String>
}

impl TryFrom<NodeOrderStatusesEvent> for L4Order {
    type Error = eyre::ErrReport;

    fn try_from(value: NodeOrderStatusesEvent) -> Result<Self, Self::Error> {
        let (user, value) = (value.user, value.order);
        Ok(Self {
            user:              Some(user),
            coin:              value.coin,
            side:              value.side,
            limit_px:          value.limit_px,
            sz:                value.sz,
            oid:               value.oid,
            timestamp:         value.timestamp,
            trigger_condition: value.trigger_condition,
            is_trigger:        value.is_trigger,
            trigger_px:        value.trigger_px,
            is_position_tpsl:  value.is_position_tpsl,
            reduce_only:       value.reduce_only,
            order_type:        value.order_type,
            tif:               value.tif,
            cloid:             value.cloid
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum L4OrderDiff {
    New { sz: f64 },
    Update { orig_sz: f64, new_sz: f64 },
    Remove
}

impl TryFrom<NodeRawBookDiffsRawBookDiff> for L4OrderDiff {
    type Error = eyre::ErrReport;

    fn try_from(value: NodeRawBookDiffsRawBookDiff) -> Result<Self, Self::Error> {
        match value {
            NodeRawBookDiffsRawBookDiff::New { sz } => Ok(Self::New { sz }),
            NodeRawBookDiffsRawBookDiff::Update { orig_sz, new_sz } => {
                Ok(Self::Update { orig_sz, new_sz })
            }
            NodeRawBookDiffsRawBookDiff::Remove => Ok(Self::Remove)
        }
    }
}
