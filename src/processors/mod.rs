mod trades;
pub use trades::*;

use crate::{hl_fs::HyperliquidDirData, types::HyperliquidData};

pub trait HyperliquidDataProcessor {
    fn handle_data(&mut self, data: HyperliquidDirData) -> eyre::Result<Option<HyperliquidData>>;
}
