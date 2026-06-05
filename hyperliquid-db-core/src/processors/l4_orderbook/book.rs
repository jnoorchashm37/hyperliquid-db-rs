use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::{
    processors::l4_orderbook::types::{InnerL4Order, Px, Sz},
    types::{L4Order, Side}
};

#[derive(Clone, Default)]
pub struct OrderBook {
    oid_to_side_px: HashMap<u64, (Side, Px)>,
    bids:           BTreeMap<Px, VecDeque<InnerL4Order>>,
    asks:           BTreeMap<Px, VecDeque<InnerL4Order>>
}

impl OrderBook {
    pub fn order_count(&self) -> usize {
        self.oid_to_side_px.len()
    }

    pub fn add_order(&mut self, mut order: InnerL4Order) -> eyre::Result<()> {
        let filled_oids = self.match_order(&mut order);
        for oid in filled_oids {
            self.oid_to_side_px.remove(&oid);
        }

        if order.sz().is_positive() {
            let oid = order.oid;
            let side = order.side();
            let px = order.limit_px();
            self.oid_to_side_px.insert(oid, (side, px));
            match side {
                Side::Ask => self.asks.entry(px).or_default().push_back(order),
                Side::Bid => self.bids.entry(px).or_default().push_back(order)
            }
        }

        Ok(())
    }

    pub fn cancel_order(&mut self, oid: u64) -> bool {
        let Some((side, px)) = self.oid_to_side_px.remove(&oid) else {
            return false;
        };

        let map = match side {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids
        };

        let Some(orders) = map.get_mut(&px) else {
            return false;
        };

        let Some(idx) = orders.iter().position(|order| order.oid == oid) else {
            return false;
        };

        orders.remove(idx);
        if orders.is_empty() {
            map.remove(&px);
        }

        true
    }

    pub fn modify_sz(&mut self, oid: u64, sz: Sz) -> bool {
        let Some((side, px)) = self.oid_to_side_px.get(&oid).copied() else {
            return false;
        };

        let map = match side {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids
        };

        let Some(orders) = map.get_mut(&px) else {
            return false;
        };

        let Some(order) = orders.iter_mut().find(|order| order.oid == oid) else {
            return false;
        };

        order.modify_sz(sz);
        true
    }

    pub fn to_l4_snapshot(&self) -> [Vec<L4Order>; 2] {
        [
            self.bids
                .iter()
                .rev()
                .flat_map(|(_, level)| level.iter().cloned().map(Into::into))
                .collect(),
            self.asks
                .iter()
                .flat_map(|(_, level)| level.iter().cloned().map(Into::into))
                .collect()
        ]
    }

    fn match_order(&mut self, taker_order: &mut InnerL4Order) -> Vec<u64> {
        let limit_px = taker_order.limit_px();
        match taker_order.side() {
            Side::Ask => {
                let keys = self
                    .bids
                    .range(limit_px..)
                    .rev()
                    .map(|(px, _)| *px)
                    .collect();
                self.match_orders_at_prices(taker_order, keys, Side::Bid)
            }
            Side::Bid => {
                let keys = self.asks.range(..=limit_px).map(|(px, _)| *px).collect();
                self.match_orders_at_prices(taker_order, keys, Side::Ask)
            }
        }
    }

    fn match_orders_at_prices(
        &mut self,
        taker_order: &mut InnerL4Order,
        prices: Vec<Px>,
        side: Side
    ) -> Vec<u64> {
        let maker_orders = match side {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids
        };
        let mut filled_oids = Vec::new();

        for price in prices {
            if taker_order.sz().is_zero() {
                break;
            }

            let mut empty_level = false;
            if let Some(level) = maker_orders.get_mut(&price) {
                while taker_order.sz().is_positive() {
                    let Some(maker_order) = level.front_mut() else {
                        break;
                    };

                    taker_order.fill(maker_order);

                    if maker_order.sz().is_zero() {
                        filled_oids.push(maker_order.oid);
                        level.pop_front();
                    }
                }
                empty_level = level.is_empty();
            }

            if empty_level {
                maker_orders.remove(&price);
            }
        }

        filled_oids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processors::l4_orderbook::types::{Coin, Px, Sz};

    #[test]
    fn matching_uses_fixed_point_size_math() {
        let mut book = OrderBook::default();

        book.add_order(order(1, Side::Bid, 1.0, 0.3)).unwrap();
        book.add_order(order(2, Side::Ask, 1.0, 0.1)).unwrap();
        book.add_order(order(3, Side::Ask, 1.0, 0.2)).unwrap();

        assert_eq!(book.order_count(), 0);
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());
    }

    fn order(oid: u64, side: Side, limit_px: f64, sz: f64) -> InnerL4Order {
        InnerL4Order {
            user: "0x0000000000000000000000000000000000000000".to_string(),
            coin: Coin::new("BTC"),
            side,
            limit_px: Px::new_f64(limit_px),
            sz: Sz::new_f64(sz),
            oid,
            timestamp: 0,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: 0.0,
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None
        }
    }
}
