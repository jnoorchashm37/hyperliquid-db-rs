use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::NODE_DATA_DATE_TIME_FORMAT;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct Hip3OracleUpdatesRows {
    pub local_time:   String,
    pub block_time:   String,
    pub block_number: u64,
    pub events:       Vec<Hip3OracleUpdatesEvent>
}

impl Hip3OracleUpdatesRows {
    pub fn local_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.local_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }

    pub fn block_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.block_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }
}

impl<'de> Deserialize<'de> for Hip3OracleUpdatesRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw = _private::Hip3OracleUpdatesRowsRaw::deserialize(deserializer)?;

        let events = raw
            .events
            .into_iter()
            .map(|raw_event| {
                Ok(Hip3OracleUpdatesEvent {
                    update_class:            raw_event.update_class,
                    mark_px_inputs:          parse_px_inputs(raw_event.mark_px_inputs)?,
                    spot_px_inputs:          parse_px_inputs(raw_event.spot_px_inputs)?,
                    external_perp_px_inputs: parse_px_inputs(raw_event.external_perp_px_inputs)?,
                    oracle_pxs:              Hip3OracleUpdatesOraclePxs {
                        coin_to_mark_px:          parse_coin_pxs(
                            raw_event.oracle_pxs.coin_to_mark_px
                        )?,
                        coin_to_oracle_px:        parse_coin_pxs(
                            raw_event.oracle_pxs.coin_to_oracle_px
                        )?,
                        coin_to_external_perp_px: parse_coin_pxs(
                            raw_event.oracle_pxs.coin_to_external_perp_px
                        )?
                    }
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
pub struct Hip3OracleUpdatesEvent {
    pub update_class:            Hip3OracleUpdatesUpdateClass,
    pub mark_px_inputs:          Vec<Hip3OracleUpdatesPxInput>,
    pub spot_px_inputs:          Vec<Hip3OracleUpdatesPxInput>,
    pub external_perp_px_inputs: Vec<Hip3OracleUpdatesPxInput>,
    pub oracle_pxs:              Hip3OracleUpdatesOraclePxs
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Hip3OracleUpdatesUpdateClass {
    Deployer,
    Fallback
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct Hip3OracleUpdatesPxInput {
    pub coin: String,
    pub px:   f64
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct Hip3OracleUpdatesOraclePxs {
    pub coin_to_mark_px:          Vec<Hip3OracleUpdatesCoinPx>,
    pub coin_to_oracle_px:        Vec<Hip3OracleUpdatesCoinPx>,
    pub coin_to_external_perp_px: Vec<Hip3OracleUpdatesCoinPx>
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub struct Hip3OracleUpdatesCoinPx {
    pub coin:             String,
    pub px:               f64,
    pub last_update_time: String,
    pub daily_px:         f64
}

fn parse_px_inputs<E>(
    raw: Vec<_private::Hip3OracleUpdatesPxInputRaw>
) -> Result<Vec<Hip3OracleUpdatesPxInput>, E>
where
    E: serde::de::Error
{
    raw.into_iter()
        .map(|raw_input| {
            Ok(Hip3OracleUpdatesPxInput {
                coin: raw_input.0,
                px:   raw_input.1.parse().map_err(serde::de::Error::custom)?
            })
        })
        .collect()
}

fn parse_coin_pxs<E>(
    raw: Vec<_private::Hip3OracleUpdatesCoinPxRaw>
) -> Result<Vec<Hip3OracleUpdatesCoinPx>, E>
where
    E: serde::de::Error
{
    raw.into_iter()
        .map(|raw_coin_px| {
            let (coin, px) = (raw_coin_px.0, raw_coin_px.1);
            Ok(Hip3OracleUpdatesCoinPx {
                coin,
                px: px.px.parse().map_err(serde::de::Error::custom)?,
                last_update_time: px.last_update_time,
                daily_px: px.daily_px.parse().map_err(serde::de::Error::custom)?
            })
        })
        .collect()
}

mod _private {
    use serde::{Deserialize, Serialize};

    use super::Hip3OracleUpdatesUpdateClass;

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesRowsRaw {
        pub local_time:   String,
        pub block_time:   String,
        pub block_number: u64,
        pub events:       Vec<Hip3OracleUpdatesEventRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesEventRaw {
        pub update_class:            Hip3OracleUpdatesUpdateClass,
        pub mark_px_inputs:          Vec<Hip3OracleUpdatesPxInputRaw>,
        pub spot_px_inputs:          Vec<Hip3OracleUpdatesPxInputRaw>,
        pub external_perp_px_inputs: Vec<Hip3OracleUpdatesPxInputRaw>,
        pub oracle_pxs:              Hip3OracleUpdatesOraclePxsRaw
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesPxInputRaw(pub String, pub String);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesOraclePxsRaw {
        pub coin_to_mark_px:          Vec<Hip3OracleUpdatesCoinPxRaw>,
        pub coin_to_oracle_px:        Vec<Hip3OracleUpdatesCoinPxRaw>,
        pub coin_to_external_perp_px: Vec<Hip3OracleUpdatesCoinPxRaw>
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesCoinPxRaw(pub String, pub Hip3OracleUpdatesOraclePxRaw);

    #[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Hip3OracleUpdatesOraclePxRaw {
        pub px:               String,
        pub last_update_time: String,
        pub daily_px:         String
    }
}

#[cfg(test)]
mod tests {
    use super::{Hip3OracleUpdatesRows, Hip3OracleUpdatesUpdateClass};

    #[test]
    fn deserializes_deployer_sample_shape() {
        let row: Hip3OracleUpdatesRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T11:00:00.710597964",
                "block_time":"2026-06-11T11:00:00.103069128",
                "block_number":1031299617,
                "events":[{
                    "update_class":"Deployer",
                    "mark_px_inputs":[["flx:BTC","90608.6"],["flx:COIN","158.42"]],
                    "spot_px_inputs":[["flx:BTC","63171.0"],["flx:COIN","158.33"]],
                    "external_perp_px_inputs":[["flx:BTC","63138.0"],["flx:COIN","154.02"]],
                    "oracle_pxs":{
                        "coin_to_mark_px":[["flx:BTC",{"px":"90608.6","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"90608.6"}]],
                        "coin_to_oracle_px":[["flx:BTC",{"px":"63171.0","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"61521.0"}]],
                        "coin_to_external_perp_px":[["flx:BTC",{"px":"63138.0","last_update_time":"2026-06-11T11:00:00.103069128","daily_px":"61485.0"}]]
                    }
                }]
            }"#
        )
        .unwrap();

        assert_eq!(row.local_time, "2026-06-11T11:00:00.710597964");
        assert_eq!(row.block_time, "2026-06-11T11:00:00.103069128");
        assert_eq!(row.block_number, 1031299617);
        assert_eq!(row.events.len(), 1);

        let event = &row.events[0];
        assert_eq!(event.update_class, Hip3OracleUpdatesUpdateClass::Deployer);

        assert_eq!(event.mark_px_inputs.len(), 2);
        assert_eq!(event.mark_px_inputs[0].coin, "flx:BTC");
        assert!((event.mark_px_inputs[0].px - 90608.6).abs() < f64::EPSILON);
        assert_eq!(event.mark_px_inputs[1].coin, "flx:COIN");
        assert!((event.mark_px_inputs[1].px - 158.42).abs() < f64::EPSILON);

        assert!((event.spot_px_inputs[0].px - 63171.0).abs() < f64::EPSILON);
        assert!((event.external_perp_px_inputs[1].px - 154.02).abs() < f64::EPSILON);

        let mark_px = &event.oracle_pxs.coin_to_mark_px[0];
        assert_eq!(mark_px.coin, "flx:BTC");
        assert!((mark_px.px - 90608.6).abs() < f64::EPSILON);
        assert_eq!(mark_px.last_update_time, "2026-06-11T11:00:00.103069128");
        assert!((mark_px.daily_px - 90608.6).abs() < f64::EPSILON);

        let oracle_px = &event.oracle_pxs.coin_to_oracle_px[0];
        assert!((oracle_px.px - 63171.0).abs() < f64::EPSILON);
        assert!((oracle_px.daily_px - 61521.0).abs() < f64::EPSILON);

        let external_perp_px = &event.oracle_pxs.coin_to_external_perp_px[0];
        assert!((external_perp_px.px - 63138.0).abs() < f64::EPSILON);
        assert!((external_perp_px.daily_px - 61485.0).abs() < f64::EPSILON);
    }

    #[test]
    fn deserializes_fallback_sample_with_empty_external_perp_inputs() {
        let row: Hip3OracleUpdatesRows = serde_json::from_str(
            r#"{
                "local_time":"2026-06-11T11:00:06.153753569",
                "block_time":"2026-06-11T11:00:06.016924723",
                "block_number":1031299704,
                "events":[{
                    "update_class":"Fallback",
                    "mark_px_inputs":[["abcd:USA500","6980.0"]],
                    "spot_px_inputs":[["abcd:USA500","6980.0"]],
                    "external_perp_px_inputs":[],
                    "oracle_pxs":{
                        "coin_to_mark_px":[["abcd:USA500",{"px":"6980.0","last_update_time":"2026-06-11T11:00:06.016924723","daily_px":"6980.0"}]],
                        "coin_to_oracle_px":[["abcd:USA500",{"px":"6980.0","last_update_time":"2026-06-11T11:00:06.016924723","daily_px":"6980.0"}]],
                        "coin_to_external_perp_px":[["abcd:USA500",{"px":"6980.0","last_update_time":"1970-01-01T00:00:00","daily_px":"6980.0"}]]
                    }
                }]
            }"#
        )
        .unwrap();

        assert_eq!(row.events.len(), 1);

        let event = &row.events[0];
        assert_eq!(event.update_class, Hip3OracleUpdatesUpdateClass::Fallback);
        assert!(event.external_perp_px_inputs.is_empty());
        assert_eq!(
            event.oracle_pxs.coin_to_external_perp_px[0].last_update_time,
            "1970-01-01T00:00:00"
        );
    }

    #[test]
    fn parses_timestrings_to_unix_timestamps() {
        let row = Hip3OracleUpdatesRows {
            local_time:   "2026-06-11T11:00:00.710597964".to_string(),
            block_time:   "2026-06-11T11:00:00.103069128".to_string(),
            block_number: 0,
            events:       Vec::new()
        };

        assert_eq!(row.local_time_unix_ms().unwrap(), 1_781_175_600_710);
        assert_eq!(row.block_time_unix_ms().unwrap(), 1_781_175_600_103);
    }
}
