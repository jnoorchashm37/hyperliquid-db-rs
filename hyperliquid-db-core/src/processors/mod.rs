mod trades;
pub use trades::TradeDeriver;

mod orderbook;
pub use orderbook::OrderBookDeriver;

mod hip3_oracle_updates;
pub use hip3_oracle_updates::Hip3OracleUpdatesDeriver;

use crate::{hl_fs::HyperliquidDirDataWithMeta, types::HyperliquidData};

pub trait HyperliquidDataProcessorHandle: Send {
    fn handle_data(
        &mut self,
        data: &HyperliquidDirDataWithMeta
    ) -> eyre::Result<Vec<HyperliquidData>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumIter)]
pub enum HyperliquidDataProcessorKind {
    Trades,
    Orderbook,
    Hip3OracleUpdates
}
