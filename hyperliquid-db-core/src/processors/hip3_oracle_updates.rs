use crate::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataWithMeta},
    processors::HyperliquidDataProcessorHandle,
    types::HyperliquidData,
    utils::unix_timestamp
};

#[derive(Default)]
pub struct Hip3OracleUpdatesDeriver {}

impl Hip3OracleUpdatesDeriver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl HyperliquidDataProcessorHandle for Hip3OracleUpdatesDeriver {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>> {
        let processing_data_at_ns = unix_timestamp().as_nanos();

        let hip3_updates = match &data.data {
            HyperliquidDirData::Hip3OracleUpdates(items) => items.clone(),
            _ => return Ok(Vec::new())
        };

        Ok(Vec::new())
    }
}
