use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeOrderStatusesRows {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<NodeOrderStatusesEvent>
}

impl<'de> Deserialize<'de> for NodeOrderStatusesRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::NodeOrderStatusesRowsRaw::deserialize(deserializer)?;

        let events = raw
            .events
            .into_iter()
            .map(|raw_event| {
                Ok(NodeOrderStatusesEvent {
                    time:    raw_event.time,
                    user:    raw_event.user,
                    hash:    raw_event.hash,
                    builder: raw_event.builder.map(NodeOrderStatusesBuilder::from),
                    status:  raw_event.status,
                    order:   parse_order(raw_event.order)?
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            local_time: raw.local_time,
            block_time: raw.block_time,
            block_number: raw.block_number,
            events
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeOrderStatusesEvent {
    pub time:    String,
    pub user:    String,
    pub hash:    String,
    pub builder: Option<NodeOrderStatusesBuilder>,
    pub status:  String,
    pub order:   NodeOrderStatusesOrder
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeOrderStatusesBuilder {
    pub b: String,
    pub f: u64
}

impl From<_private::NodeOrderStatusesBuilderRaw> for NodeOrderStatusesBuilder {
    fn from(value: _private::NodeOrderStatusesBuilderRaw) -> Self {
        Self { b: value.b, f: value.f }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeOrderStatusesOrder {
    pub coin:              String,
    pub side:              NodeOrderStatusesSide,
    pub limit_px:          f64,
    pub sz:                f64,
    pub oid:               u64,
    pub timestamp:         u64,
    pub trigger_condition: String,
    pub is_trigger:        bool,
    pub trigger_px:        f64,
    pub children:          Vec<NodeOrderStatusesOrder>,
    pub is_position_tpsl:  bool,
    pub reduce_only:       bool,
    pub order_type:        String,
    pub orig_sz:           f64,
    pub tif:               Option<String>,
    pub cloid:             Option<String>
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NodeOrderStatusesSide {
    A,
    B
}

fn parse_order<E>(raw: _private::NodeOrderStatusesOrderRaw) -> Result<NodeOrderStatusesOrder, E>
where
    E: serde::de::Error
{
    Ok(NodeOrderStatusesOrder {
        coin:              raw.coin,
        side:              raw.side,
        limit_px:          raw.limit_px.parse().map_err(serde::de::Error::custom)?,
        sz:                raw.sz.parse().map_err(serde::de::Error::custom)?,
        oid:               raw.oid,
        timestamp:         raw.timestamp,
        trigger_condition: raw.trigger_condition,
        is_trigger:        raw.is_trigger,
        trigger_px:        raw.trigger_px.parse().map_err(serde::de::Error::custom)?,
        children:          raw
            .children
            .into_iter()
            .map(parse_order::<E>)
            .collect::<Result<Vec<_>, _>>()?,
        is_position_tpsl:  raw.is_position_tpsl,
        reduce_only:       raw.reduce_only,
        order_type:        raw.order_type,
        orig_sz:           raw.orig_sz.parse().map_err(serde::de::Error::custom)?,
        tif:               raw.tif,
        cloid:             raw.cloid
    })
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::NodeOrderStatusesSide;

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeOrderStatusesRowsRaw {
        pub local_time:   String,
        pub block_time:   String,
        pub block_number: u64,
        pub events:       Vec<NodeOrderStatusesEventRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeOrderStatusesEventRaw {
        pub time:    String,
        pub user:    String,
        pub hash:    String,
        pub builder: Option<NodeOrderStatusesBuilderRaw>,
        pub status:  String,
        pub order:   NodeOrderStatusesOrderRaw
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeOrderStatusesBuilderRaw {
        pub b: String,
        pub f: u64
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct NodeOrderStatusesOrderRaw {
        pub coin:              String,
        pub side:              NodeOrderStatusesSide,
        pub limit_px:          String,
        pub sz:                String,
        pub oid:               u64,
        pub timestamp:         u64,
        pub trigger_condition: String,
        pub is_trigger:        bool,
        pub trigger_px:        String,
        pub children:          Vec<NodeOrderStatusesOrderRaw>,
        pub is_position_tpsl:  bool,
        pub reduce_only:       bool,
        pub order_type:        String,
        pub orig_sz:           String,
        pub tif:               Option<String>,
        pub cloid:             Option<String>
    }
}
