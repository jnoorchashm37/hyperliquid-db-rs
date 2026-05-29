mod trades;
pub use trades::*;
mod all_mids;
pub use all_mids::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HyperliquidDataKind {
    Trades
}
