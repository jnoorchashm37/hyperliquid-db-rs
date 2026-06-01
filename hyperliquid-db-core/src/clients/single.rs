use std::sync::Arc;

use tokio::sync::broadcast;

use crate::{
    hl_fs::{HyperliquidDirData, HyperliquidDirDataManager},
    processors::HyperliquidDataProcessorHandle,
    types::HyperliquidDataKind
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
}
