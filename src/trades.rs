use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hl_fs::schemas::{NodeFillsFill, NodeFillsRow, NodeFillsSide};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TradeSide {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
pub struct TradeDeriver {
    pending_fills: HashMap<u64, PendingTrade>
}

impl TradeDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_fill_data(&mut self, data: NodeFillsRow) -> eyre::Result<()> {
        for fill in data.events {
            self.new_fill(fill)?;
        }

        Ok(())
    }

    pub fn pending_fill_count(&self) -> usize {
        self.pending_fills.len()
    }

    fn new_fill(&mut self, fill: NodeFillsFill) -> eyre::Result<()> {
        let tid = fill.tid;
        let pending_trade = self.pending_fills.entry(tid).or_default();

        match fill.side {
            NodeFillsSide::A => pending_trade.ask = Some(fill),
            NodeFillsSide::B => pending_trade.bid = Some(fill)
        }

        if !pending_trade.is_complete() {
            return Ok(());
        }

        let pending_trade = self
            .pending_fills
            .remove(&tid)
            .expect("pending trade exists");
        let trade = pending_trade.into_trade()?;
        println!("{}", serde_json::to_string(&trade)?);

        Ok(())
    }
}

#[derive(Default)]
struct PendingTrade {
    ask: Option<NodeFillsFill>,
    bid: Option<NodeFillsFill>
}

impl PendingTrade {
    fn is_complete(&self) -> bool {
        self.ask.is_some() && self.bid.is_some()
    }

    fn into_trade(self) -> eyre::Result<Trade> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_order_book_server_trade_shape() {
        let trade = PendingTrade { ask: Some(ask_fill(false)), bid: Some(bid_fill(true)) }
            .into_trade()
            .unwrap();

        assert_eq!(
            serde_json::to_string(&trade).unwrap(),
            r#"{"coin":"BTC","side":"B","px":"106296.0","sz":"0.00017","hash":"0xhash","time":1751430933565,"tid":293353986402527,"users":["0xbuyer","0xseller"]}"#
        );
    }

    #[test]
    fn matches_fills_across_updates() {
        let mut deriver = TradeDeriver::new();

        deriver.new_fill_data(row(vec![ask_fill(false)])).unwrap();
        assert_eq!(deriver.pending_fill_count(), 1);

        deriver.new_fill_data(row(vec![bid_fill(true)])).unwrap();
        assert_eq!(deriver.pending_fill_count(), 0);
    }

    #[test]
    fn ask_taker_produces_ask_side_trade() {
        let trade = PendingTrade { ask: Some(ask_fill(true)), bid: Some(bid_fill(false)) }
            .into_trade()
            .unwrap();

        assert_eq!(trade.side, TradeSide::Ask);
    }

    fn row(events: Vec<NodeFillsFill>) -> NodeFillsRow {
        NodeFillsRow {
            local_time: "2025-06-24T02:56:36.172847427".to_string(),
            block_time: "2025-06-24T02:56:36.172847427".to_string(),
            block_number: 1,
            events
        }
    }

    fn ask_fill(crossed: bool) -> NodeFillsFill {
        fill("0xseller", NodeFillsSide::A, crossed)
    }

    fn bid_fill(crossed: bool) -> NodeFillsFill {
        fill("0xbuyer", NodeFillsSide::B, crossed)
    }

    fn fill(user: &str, side: NodeFillsSide, crossed: bool) -> NodeFillsFill {
        NodeFillsFill {
            user: user.to_string(),
            coin: "BTC".to_string(),
            px: 106296.0,
            sz: 0.00017,
            side,
            time: 1751430933565,
            start_position: 0.0,
            dir: "Open Long".to_string(),
            closed_pnl: 0.0,
            hash: "0xhash".to_string(),
            oid: 1,
            crossed,
            fee: 0.0,
            builder_fee: None,
            tid: 293353986402527,
            cloid: None,
            fee_token: "USDC".to_string(),
            builder: None,
            twap_id: None,
            deployer_fee: None
        }
    }
}
