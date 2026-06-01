use crate::{
    hl_fs::HyperliquidDirData,
    processors::{HyperliquidDataProcessorHandle, TradeDeriver},
    types::{HyperliquidData, HyperliquidDataKind}
};

pub struct HyperliquidDataProcessorGroup {
    processors: Vec<Box<dyn HyperliquidDataProcessorHandle>>
}

impl HyperliquidDataProcessorGroup {
    pub fn new(kinds: &[HyperliquidDataKind]) -> Self {
        let processors = kinds.iter().copied().map(kind_to_processor).collect();
        Self { processors }
    }

    pub fn handle_data(&mut self, data: HyperliquidDirData) -> eyre::Result<Vec<HyperliquidData>> {
        Ok(self
            .processors
            .iter_mut()
            .map(|processor| processor.handle_data(&data))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect())
    }
}

fn kind_to_processor(kind: HyperliquidDataKind) -> Box<dyn HyperliquidDataProcessorHandle> {
    match kind {
        HyperliquidDataKind::Trades => Box::new(TradeDeriver::new())
    }
}
