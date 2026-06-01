mod trades;
pub use trades::TradeDeriver;

use crate::{hl_fs::HyperliquidDirData, types::HyperliquidData};

pub trait HyperliquidDataProcessorHandle: Send {
    fn handle_data(&mut self, data: &HyperliquidDirData) -> eyre::Result<Option<HyperliquidData>>;
}
