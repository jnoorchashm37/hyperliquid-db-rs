use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::NodeFillsFill;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TradeSide {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Trade {
    pub coin:  String,
    pub side:  TradeSide,
    pub px:    String,
    pub sz:    String,
    pub hash:  String,
    pub time:  u64,
    pub tid:   u64,
    pub users: [String; 2]
}

#[derive(Default)]
pub struct PendingTrade {
    pub ask: Option<NodeFillsFill>,
    pub bid: Option<NodeFillsFill>
}

impl PendingTrade {
    pub fn is_complete(&self) -> bool {
        self.ask.is_some() && self.bid.is_some()
    }

    pub fn into_trade(self) -> eyre::Result<Trade> {
        let ask = self.ask.expect("complete trade has ask fill");
        let bid = self.bid.expect("complete trade has bid fill");

        if ask.coin != bid.coin {
            return Err(eyre::eyre!(
                "matched fills for tid {} have different coins: ask={}, bid={}",
                ask.tid,
                ask.coin,
                bid.coin
            ));
        }
        if ask.px != bid.px {
            return Err(eyre::eyre!(
                "matched fills for tid {} have different prices: ask={}, bid={}",
                ask.tid,
                ask.px,
                bid.px
            ));
        }
        if ask.sz != bid.sz {
            return Err(eyre::eyre!(
                "matched fills for tid {} have different sizes: ask={}, bid={}",
                ask.tid,
                ask.sz,
                bid.sz
            ));
        }

        let side = if ask.crossed { TradeSide::Ask } else { TradeSide::Bid };

        Ok(Trade {
            coin: ask.coin,
            side,
            px: decimal_string(ask.px),
            sz: decimal_string(ask.sz),
            hash: ask.hash,
            time: ask.time,
            tid: ask.tid,
            users: [bid.user, ask.user]
        })
    }
}

fn decimal_string(value: f64) -> String {
    let mut value = format!("{value:.8}");
    while value.contains('.') && value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.push('0');
    }

    value
}
