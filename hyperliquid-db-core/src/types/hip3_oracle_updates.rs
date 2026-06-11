use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::{
    Hip3OracleUpdatesCoinPx, Hip3OracleUpdatesEvent, Hip3OracleUpdatesOraclePxs,
    Hip3OracleUpdatesPxInput, Hip3OracleUpdatesUpdateClass, NODE_DATA_DATE_TIME_FORMAT
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Hip3OracleUpdate {
    pub time:                    u64,
    pub height:                  u64,
    pub update_class:            Hip3OracleUpdateClass,
    pub mark_px_inputs:          Vec<Hip3OracleUpdatePxInput>,
    pub spot_px_inputs:          Vec<Hip3OracleUpdatePxInput>,
    pub external_perp_px_inputs: Vec<Hip3OracleUpdatePxInput>,
    pub oracle_pxs:              Hip3OracleUpdateOraclePxs
}

impl Hip3OracleUpdate {
    pub fn from_event(time: u64, height: u64, event: Hip3OracleUpdatesEvent) -> Self {
        Self {
            time,
            height,
            update_class: event.update_class.into(),
            mark_px_inputs: event.mark_px_inputs.into_iter().map(Into::into).collect(),
            spot_px_inputs: event.spot_px_inputs.into_iter().map(Into::into).collect(),
            external_perp_px_inputs: event
                .external_perp_px_inputs
                .into_iter()
                .map(Into::into)
                .collect(),
            oracle_pxs: event.oracle_pxs.into()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Hip3OracleUpdateClass {
    Deployer,
    Fallback
}

impl From<Hip3OracleUpdatesUpdateClass> for Hip3OracleUpdateClass {
    fn from(value: Hip3OracleUpdatesUpdateClass) -> Self {
        match value {
            Hip3OracleUpdatesUpdateClass::Deployer => Self::Deployer,
            Hip3OracleUpdatesUpdateClass::Fallback => Self::Fallback
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Hip3OracleUpdatePxInput {
    pub coin: String,
    pub px:   f64
}

impl From<Hip3OracleUpdatesPxInput> for Hip3OracleUpdatePxInput {
    fn from(value: Hip3OracleUpdatesPxInput) -> Self {
        Self { coin: value.coin, px: value.px }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Hip3OracleUpdateOraclePxs {
    pub coin_to_mark_px:          Vec<Hip3OracleUpdateCoinPx>,
    pub coin_to_oracle_px:        Vec<Hip3OracleUpdateCoinPx>,
    pub coin_to_external_perp_px: Vec<Hip3OracleUpdateCoinPx>
}

impl From<Hip3OracleUpdatesOraclePxs> for Hip3OracleUpdateOraclePxs {
    fn from(value: Hip3OracleUpdatesOraclePxs) -> Self {
        Self {
            coin_to_mark_px:          value.coin_to_mark_px.into_iter().map(Into::into).collect(),
            coin_to_oracle_px:        value
                .coin_to_oracle_px
                .into_iter()
                .map(Into::into)
                .collect(),
            coin_to_external_perp_px: value
                .coin_to_external_perp_px
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Hip3OracleUpdateCoinPx {
    pub coin:             String,
    pub px:               f64,
    pub last_update_time: String,
    pub daily_px:         f64
}

impl Hip3OracleUpdateCoinPx {
    pub fn last_update_time_unix_ms(&self) -> eyre::Result<u64> {
        Ok(NaiveDateTime::parse_from_str(&self.last_update_time, NODE_DATA_DATE_TIME_FORMAT)?
            .and_utc()
            .timestamp_millis() as u64)
    }
}

impl From<Hip3OracleUpdatesCoinPx> for Hip3OracleUpdateCoinPx {
    fn from(value: Hip3OracleUpdatesCoinPx) -> Self {
        Self {
            coin:             value.coin,
            px:               value.px,
            last_update_time: value.last_update_time,
            daily_px:         value.daily_px
        }
    }
}
