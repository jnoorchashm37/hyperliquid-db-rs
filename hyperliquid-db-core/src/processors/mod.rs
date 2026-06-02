mod trades;
pub use trades::TradeDeriver;

use crate::{hl_fs::HyperliquidDirDataWithMeta, types::HyperliquidData};

pub trait HyperliquidDataProcessorHandle: Send {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Option<HyperliquidData>>;
}
