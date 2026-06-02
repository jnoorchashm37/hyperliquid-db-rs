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
