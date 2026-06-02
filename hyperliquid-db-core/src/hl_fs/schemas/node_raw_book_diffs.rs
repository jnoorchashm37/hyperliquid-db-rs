use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeRawBookDiffsRows {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<NodeRawBookDiffsEvent>
}

impl<'de> Deserialize<'de> for NodeRawBookDiffsRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::NodeRawBookDiffsRowsRaw::deserialize(deserializer)?;

        let events = raw
            .events
            .into_iter()
            .map(|raw_event| {
                Ok(NodeRawBookDiffsEvent {
                    user:          raw_event.user,
                    oid:           raw_event.oid,
                    coin:          raw_event.coin,
                    side:          raw_event.side,
                    px:            raw_event.px.parse().map_err(serde::de::Error::custom)?,
                    raw_book_diff: parse_raw_book_diff(raw_event.raw_book_diff)?
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
pub struct NodeRawBookDiffsEvent {
    pub user:          String,
    pub oid:           u64,
    pub coin:          String,
    pub side:          NodeRawBookDiffsSide,
    pub px:            f64,
    pub raw_book_diff: NodeRawBookDiffsRawBookDiff
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NodeRawBookDiffsSide {
    A,
    B
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum NodeRawBookDiffsRawBookDiff {
    New { sz: f64 },
    Update { orig_sz: f64, new_sz: f64 },
    Remove
}

fn parse_raw_book_diff<E>(
    raw: _private::NodeRawBookDiffsRawBookDiffRaw
) -> Result<NodeRawBookDiffsRawBookDiff, E>
where
    E: serde::de::Error
{
    match raw {
        _private::NodeRawBookDiffsRawBookDiffRaw::New { sz } => {
            Ok(NodeRawBookDiffsRawBookDiff::New {
                sz: sz.parse().map_err(serde::de::Error::custom)?
            })
        }
        _private::NodeRawBookDiffsRawBookDiffRaw::Update { orig_sz, new_sz } => {
            Ok(NodeRawBookDiffsRawBookDiff::Update {
                orig_sz: orig_sz.parse().map_err(serde::de::Error::custom)?,
                new_sz:  new_sz.parse().map_err(serde::de::Error::custom)?
            })
        }
        _private::NodeRawBookDiffsRawBookDiffRaw::Remove => Ok(NodeRawBookDiffsRawBookDiff::Remove)
    }
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::NodeRawBookDiffsSide;

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeRawBookDiffsRowsRaw {
        pub local_time:   String,
        pub block_time:   String,
        pub block_number: u64,
        pub events:       Vec<NodeRawBookDiffsEventRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeRawBookDiffsEventRaw {
        pub user:          String,
        pub oid:           u64,
        pub coin:          String,
        pub side:          NodeRawBookDiffsSide,
        pub px:            String,
        pub raw_book_diff: NodeRawBookDiffsRawBookDiffRaw
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
    pub enum NodeRawBookDiffsRawBookDiffRaw {
        New { sz: String },
        Update { orig_sz: String, new_sz: String },
        Remove
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeRawBookDiffsRawBookDiff, NodeRawBookDiffsRows, NodeRawBookDiffsSide};

    #[test]
    fn deserializes_new_update_and_remove_sample_shapes() {
        let row: NodeRawBookDiffsRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-02T10:00:00.865603615",
                "block_time":"2026-06-02T09:59:59.457306449",
                "block_number":1019927126,
                "events":[
                    {
                        "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                        "oid":452916385917,
                        "coin":"JTO",
                        "side":"A",
                        "px":"0.65313",
                        "raw_book_diff":{"new":{"sz":"1096.0"}}
                    },
                    {
                        "user":"0x1c1c270b573d55b68b3d14722b5d5d401511bed0",
                        "oid":452916245881,
                        "coin":"BTC",
                        "side":"A",
                        "px":"69409.0",
                        "raw_book_diff":{"update":{"origSz":"0.12246","newSz":"0.0"}}
                    },
                    {
                        "user":"0x31ca8395cf837de08b24da3f660e77761dfb974b",
                        "oid":452916042519,
                        "coin":"STX",
                        "side":"B",
                        "px":"0.22495",
                        "raw_book_diff":"remove"
                    }
                ]
            }"#
        )
        .unwrap();

        assert_eq!(row.local_time, "2026-06-02T10:00:00.865603615");
        assert_eq!(row.block_time, "2026-06-02T09:59:59.457306449");
        assert_eq!(row.block_number, 1019927126);
        assert_eq!(row.events.len(), 3);

        assert_eq!(row.events[0].side, NodeRawBookDiffsSide::A);
        assert!((row.events[0].px - 0.65313).abs() < f64::EPSILON);
        match row.events[0].raw_book_diff {
            NodeRawBookDiffsRawBookDiff::New { sz } => {
                assert!((sz - 1096.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected new raw book diff")
        }

        assert_eq!(row.events[1].coin, "BTC");
        assert!((row.events[1].px - 69409.0).abs() < f64::EPSILON);
        match row.events[1].raw_book_diff {
            NodeRawBookDiffsRawBookDiff::Update { orig_sz, new_sz } => {
                assert!((orig_sz - 0.12246).abs() < f64::EPSILON);
                assert!((new_sz - 0.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected update raw book diff")
        }

        assert_eq!(row.events[2].side, NodeRawBookDiffsSide::B);
        assert!((row.events[2].px - 0.22495).abs() < f64::EPSILON);
        assert!(matches!(row.events[2].raw_book_diff, NodeRawBookDiffsRawBookDiff::Remove));
    }
}
