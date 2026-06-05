mod trades;
pub use trades::TradeDeriver;

mod orderbook;
pub use orderbook::L4BookDeriver;

use crate::{hl_fs::HyperliquidDirDataWithMeta, types::HyperliquidData};

pub trait HyperliquidDataProcessorHandle: Send {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>>;
}
