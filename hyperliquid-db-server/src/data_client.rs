use std::sync::Arc;

use hyperliquid_db_core::types::{HyperliquidData, HyperliquidDataKind};
use tokio::sync::broadcast;

pub struct HyperliquidDataClient {
    processor_kind: HyperliquidDataKind,
    data_rx:        broadcast::Receiver<Arc<eyre::Result<HyperliquidData>>>
}

impl HyperliquidDataClient {
    pub fn new(
        processor_kind: HyperliquidDataKind,
        data_rx: broadcast::Receiver<Arc<eyre::Result<HyperliquidData>>>
    ) -> Self {
        Self { processor_kind, data_rx }
    }

    pub async fn recv(&mut self) -> eyre::Result<Option<HyperliquidData>> {
        let data = self.data_rx.recv().await?;
        let data = match data.as_ref() {
            Ok(data) => data.clone(),
            Err(error) => return Err(eyre::eyre!("{error:?}"))
        };

        if self.processor_kind == data.kind() { Ok(Some(data)) } else { Ok(None) }
    }
}
