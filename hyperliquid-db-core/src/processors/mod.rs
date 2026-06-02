mod trades;
pub use trades::TradeDeriver;

mod l4_orderbook;
pub use l4_orderbook::L4BookDeriver;

use crate::{hl_fs::HyperliquidDirDataWithMeta, types::HyperliquidData};

pub trait HyperliquidDataProcessorHandle: Send {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Option<HyperliquidData>>;
}
