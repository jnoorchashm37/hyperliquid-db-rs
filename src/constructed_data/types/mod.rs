mod trades;
pub use trades::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConstructedHyperliquidDataKind {
    Trades
}
