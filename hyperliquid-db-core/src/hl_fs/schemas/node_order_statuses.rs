use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{hl_fs::schemas::NODE_DATA_DATE_TIME_FORMAT, types::Side};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeOrderStatusesRows {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<NodeOrderStatusesEvent>
}

impl NodeOrderStatusesRows {
    pub fn local_time_unix(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.local_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp() as u64)
    }

    pub fn block_time_unix(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.block_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp() as u64)
    }
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
    pub hash:    Option<String>,
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
    pub side:              Side,
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

    use super::Side;

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
        pub hash:    Option<String>,
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
        pub side:              Side,
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

#[cfg(test)]
mod tests {
    use super::NodeOrderStatusesRows;
    use crate::types::Side;

    #[test]
    fn deserializes_builder_and_child_order_sample_shapes() {
        let row: NodeOrderStatusesRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-02T10:00:00.914047560",
                "block_time":"2026-06-02T09:59:59.678571270",
                "block_number":1019927129,
                "events":[
                    {
                        "time":"2026-06-02T09:59:59.457306449",
                        "user":"0x8f308355ce0604dd4fd872309d5761167655b07c",
                        "hash":"0xab41fe01a96f0058acbb043ccada5601b00015e744621f2a4f0aa9546862da43",
                        "builder":{"b":"0xb84c7fb41ee7d8781e2b0d59eed2accd2ae99533","f":15},
                        "status":"open",
                        "order":{
                            "coin":"km:USOIL",
                            "side":"B",
                            "limitPx":"129.78",
                            "sz":"106.796",
                            "oid":452916386246,
                            "timestamp":1780394399457,
                            "triggerCondition":"N/A",
                            "isTrigger":false,
                            "triggerPx":"0.0",
                            "children":[],
                            "isPositionTpsl":false,
                            "reduceOnly":false,
                            "orderType":"Limit",
                            "origSz":"106.796",
                            "tif":"Gtc",
                            "cloid":"0xaaa497eb2cabe32e233204e17d3350c1"
                        }
                    },
                    {
                        "time":"2026-06-02T09:59:59.678571270",
                        "user":"0x55684b989f23978dc2ad4926736c463e225ea5cc",
                        "hash":"0xba9ec888d37a4cf5bc18043ccada590201aa006e6e7d6bc75e6773db927e26e0",
                        "builder":null,
                        "status":"open",
                        "order":{
                            "coin":"xyz:XYZ100",
                            "side":"B",
                            "limitPx":"30505.0",
                            "sz":"0.0093",
                            "oid":452916388462,
                            "timestamp":1780394399678,
                            "triggerCondition":"N/A",
                            "isTrigger":false,
                            "triggerPx":"0.0",
                            "children":[{
                                "coin":"xyz:XYZ100",
                                "side":"A",
                                "limitPx":"26934.0",
                                "sz":"0.0093",
                                "oid":452916388462,
                                "timestamp":1780394399678,
                                "triggerCondition":"Price below 29927",
                                "isTrigger":true,
                                "triggerPx":"29927.0",
                                "children":[],
                                "isPositionTpsl":false,
                                "reduceOnly":true,
                                "orderType":"Stop Market",
                                "origSz":"0.0093",
                                "tif":null,
                                "cloid":"0x5b931285742037753c624a2fcd1177b3"
                            }],
                            "isPositionTpsl":false,
                            "reduceOnly":false,
                            "orderType":"Limit",
                            "origSz":"0.0093",
                            "tif":"Alo",
                            "cloid":"0x5b931285742037753c624a2fcd1177b3"
                        }
                    }
                ]
            }"#
        )
        .unwrap();

        assert_eq!(row.local_time, "2026-06-02T10:00:00.914047560");
        assert_eq!(row.block_time, "2026-06-02T09:59:59.678571270");
        assert_eq!(row.block_number, 1019927129);
        assert_eq!(row.events.len(), 2);

        let builder_event = &row.events[0];
        let builder = builder_event.builder.as_ref().unwrap();
        assert_eq!(builder.b, "0xb84c7fb41ee7d8781e2b0d59eed2accd2ae99533");
        assert_eq!(builder.f, 15);
        assert_eq!(builder_event.order.side, Side::Bid);
        assert!((builder_event.order.limit_px - 129.78).abs() < f64::EPSILON);
        assert!((builder_event.order.sz - 106.796).abs() < f64::EPSILON);
        assert_eq!(builder_event.order.tif.as_deref(), Some("Gtc"));
        assert_eq!(
            builder_event.order.cloid.as_deref(),
            Some("0xaaa497eb2cabe32e233204e17d3350c1")
        );

        let parent = &row.events[1].order;
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].side, Side::Ask);
        assert!(parent.children[0].is_trigger);
        assert!(parent.children[0].reduce_only);
        assert!((parent.children[0].trigger_px - 29927.0).abs() < f64::EPSILON);
        assert_eq!(parent.children[0].order_type, "Stop Market");
        assert_eq!(parent.children[0].tif, None);
    }

    #[test]
    fn deserializes_null_event_hash() {
        let row: NodeOrderStatusesRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-02T10:00:00.854461191",
                "block_time":"2026-06-02T09:59:59.398971110",
                "block_number":1019927125,
                "events":[{
                    "time":"2026-06-02T09:59:59.398971110",
                    "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                    "hash":null,
                    "builder":null,
                    "status":"canceled",
                    "order":{
                        "coin":"JTO",
                        "side":"A",
                        "limitPx":"0.65313",
                        "sz":"1096.0",
                        "oid":452916385917,
                        "timestamp":1780394399398,
                        "triggerCondition":"N/A",
                        "isTrigger":false,
                        "triggerPx":"0.0",
                        "children":[],
                        "isPositionTpsl":false,
                        "reduceOnly":false,
                        "orderType":"Limit",
                        "origSz":"1096.0",
                        "tif":"Alo",
                        "cloid":null
                    }
                }]
            }"#
        )
        .unwrap();

        assert_eq!(row.events.len(), 1);
        assert_eq!(row.events[0].hash, None);
    }

    #[test]
    fn parses_timestrings_to_unix_timestamps() {
        let row = NodeOrderStatusesRows {
            local_time:   "2026-06-05T00:00:00.003243951".to_string(),
            block_time:   "2026-06-05T00:00:01.999999999".to_string(),
            block_number: 0,
            events:       Vec::new()
        };

        assert_eq!(row.local_time_unix().unwrap(), 1_780_617_600);
        assert_eq!(row.block_time_unix().unwrap(), 1_780_617_601);
    }
}
