use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    constructed_data::{
        HyperliquidDataDeriver,
        types::{PendingTrade, Trade}
    },
    hl_fs::schemas::{NodeFillsFill, NodeFillsRow, NodeFillsSide}
};

#[derive(Default)]
pub struct TradeDeriver {
    pending_fills: HashMap<u64, PendingTrade>
}

impl TradeDeriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_fill_count(&self) -> usize {
        self.pending_fills.len()
    }

    fn new_fill(&mut self, fill: NodeFillsFill) -> eyre::Result<Option<Trade>> {
        let tid = fill.tid;
        let pending_trade = self.pending_fills.entry(tid).or_default();

        match fill.side {
            NodeFillsSide::A => pending_trade.ask = Some(fill),
            NodeFillsSide::B => pending_trade.bid = Some(fill)
        }

        if !pending_trade.is_complete() {
            return Ok(None);
        }

        let pending_trade = self
            .pending_fills
            .remove(&tid)
            .expect("pending trade exists");
        let trade = pending_trade.into_trade()?;
        // println!("{}", serde_json::to_string(&trade)?);

        Ok(Some(trade))
    }
}

impl HyperliquidDataDeriver for TradeDeriver {
    type ParsedType = Trade;
    type RawType = NodeFillsRow;

    fn parse_raw_type(data: &[u8]) -> eyre::Result<Self::RawType> {
        Ok(serde_json::from_slice::<Self::RawType>(data)?)
    }

    fn construct_data(&mut self, data: Self::RawType) -> eyre::Result<Vec<Self::ParsedType>> {
        let mut trades = Vec::new();
        for fill in data.events {
            if let Some(trade) = self.new_fill(fill)? {
                trades.push(trade);
            }
        }

        Ok(trades)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constructed_data::types::*;

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

        deriver.construct_data(row(vec![ask_fill(false)])).unwrap();
        assert_eq!(deriver.pending_fill_count(), 1);

        deriver.construct_data(row(vec![bid_fill(true)])).unwrap();
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
