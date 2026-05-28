use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct NodeFillsRow {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<NodeFillsFill>
}

impl<'de> Deserialize<'de> for NodeFillsRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::NodeFillsRowRaw::deserialize(deserializer)?;

        let events = raw
            .events
            .into_iter()
            .map(|raw_event| {
                let (user, event) = (raw_event.0, raw_event.1);
                Ok(NodeFillsFill {
                    user,
                    coin: event.coin,
                    px: event.px.parse().map_err(serde::de::Error::custom)?,
                    sz: event.sz.parse().map_err(serde::de::Error::custom)?,
                    side: event.side,
                    time: event.time,
                    start_position: event
                        .start_position
                        .parse()
                        .map_err(serde::de::Error::custom)?,
                    dir: event.dir,
                    closed_pnl: event.closed_pnl.parse().map_err(serde::de::Error::custom)?,
                    hash: event.hash,
                    oid: event.oid,
                    crossed: event.crossed,
                    fee: event.fee.parse().map_err(serde::de::Error::custom)?,
                    builder_fee: event
                        .builder_fee
                        .map(|builder_fee| builder_fee.parse().map_err(serde::de::Error::custom))
                        .transpose()?,
                    tid: event.tid,
                    cloid: event.cloid,
                    fee_token: event.fee_token,
                    builder: event.builder,
                    twap_id: event.twap_id,
                    deployer_fee: event
                        .deployer_fee
                        .map(|deployer_fee| deployer_fee.parse().map_err(serde::de::Error::custom))
                        .transpose()?
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
#[serde(rename_all = "camelCase")]
pub struct NodeFillsFill {
    pub user:           String,
    pub coin:           String,
    pub px:             f64,
    pub sz:             f64,
    pub side:           NodeFillsSide,
    pub time:           u64,
    pub start_position: f64,
    pub dir:            String,
    pub closed_pnl:     f64,
    pub hash:           String,
    pub oid:            u64,
    pub crossed:        bool,
    pub fee:            f64,
    pub builder_fee:    Option<f64>,
    pub tid:            u64,
    pub cloid:          Option<String>,
    pub fee_token:      String,
    pub builder:        Option<String>,
    pub twap_id:        Option<u64>,
    pub deployer_fee:   Option<f64>
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum NodeFillsSide {
    A,
    B
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::NodeFillsSide;

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeFillsRowRaw {
        pub local_time:   String,
        pub block_time:   String,
        pub block_number: u64,
        pub events:       Vec<NodeFillsEventRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct NodeFillsEventRaw(pub String, pub NodeFillsFillRaw);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct NodeFillsFillRaw {
        pub coin:           String,
        pub px:             String,
        pub sz:             String,
        pub side:           NodeFillsSide,
        pub time:           u64,
        pub start_position: String,
        pub dir:            String,
        pub closed_pnl:     String,
        pub hash:           String,
        pub oid:            u64,
        pub crossed:        bool,
        pub fee:            String,
        pub builder_fee:    Option<String>,
        pub tid:            u64,
        pub cloid:          Option<String>,
        pub fee_token:      String,
        pub builder:        Option<String>,
        pub twap_id:        Option<u64>,
        pub deployer_fee:   Option<String>
    }
}
