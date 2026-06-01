use std::sync::Arc;

use tokio::sync::broadcast;

use crate::{
    hl_fs::HyperliquidDirData, processors::HyperliquidDataProcessorHandle, types::HyperliquidData
};

pub struct HyperliquidDataClient<P: HyperliquidDataProcessorHandle> {
    processor: P,
    data_rx:   broadcast::Receiver<Arc<eyre::Result<HyperliquidDirData>>>
}

impl<P: HyperliquidDataProcessorHandle> HyperliquidDataClient<P> {
    pub fn new(
        processor: P,
        data_rx: broadcast::Receiver<Arc<eyre::Result<HyperliquidDirData>>>
    ) -> Self {
        Self { processor, data_rx }
    }

    pub async fn recv(&mut self) -> eyre::Result<Option<HyperliquidData>> {
        let data = self.data_rx.recv().await?;
        let data = match data.as_ref() {
            Ok(data) => data.clone(),
            Err(error) => return Err(eyre::eyre!("{error:?}"))
        };

        self.processor.handle_data(&data)
    }
}
