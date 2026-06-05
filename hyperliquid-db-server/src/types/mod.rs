mod l4_book;
pub use l4_book::*;

mod trades;
use serde::{Deserialize, Serialize};
pub use trades::*;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidRpcData {
    Trades(Vec<RpcTrade>),
    L4Book(Vec<RpcL4Book>)
}
