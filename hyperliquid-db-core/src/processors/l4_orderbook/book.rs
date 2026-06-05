use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::{
    processors::l4_orderbook::PRICE_MULTIPLIER,
    types::{L4Order, Side}
};

#[derive(Default)]
pub struct OrderBook {
    pub oid_to_side_px: HashMap<u64, (Side, i64)>,
    pub bids:           BTreeMap<i64, VecDeque<L4Order>>,
    pub asks:           BTreeMap<i64, VecDeque<L4Order>>
}

impl OrderBook {
    pub fn order_count(&self) -> usize {
        self.oid_to_side_px.len()
    }

    pub fn add_order(&mut self, mut order: L4Order) -> eyre::Result<()> {
        let filled_oids = self.match_order(&mut order)?;
        for oid in filled_oids {
            self.oid_to_side_px.remove(&oid);
        }

        if order.sz > 0.0 {
            let oid = order.oid;
            let side = order.side;
            let px = (order.limit_px * PRICE_MULTIPLIER).round() as i64;
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

    pub fn modify_sz(&mut self, oid: u64, sz: f64) -> bool {
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

        order.sz = sz;
        true
    }

    pub fn match_order(&mut self, taker_order: &mut L4Order) -> eyre::Result<Vec<u64>> {
        let limit_px = (taker_order.limit_px * PRICE_MULTIPLIER).round() as i64;
        let filled_oids = match taker_order.side {
            Side::Ask => {
                let keys = self
                    .bids
                    .range(limit_px..)
                    .rev()
                    .map(|(px, _)| *px)
                    .collect();
                self.match_orders_at_prices(taker_order, keys, Side::Bid)?
            }
            Side::Bid => {
                let keys = self.asks.range(..=limit_px).map(|(px, _)| *px).collect();
                self.match_orders_at_prices(taker_order, keys, Side::Ask)?
            }
        };

        Ok(filled_oids)
    }

    fn match_orders_at_prices(
        &mut self,
        taker_order: &mut L4Order,
        prices: Vec<i64>,
        side: Side
    ) -> eyre::Result<Vec<u64>> {
        let maker_orders = match side {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids
        };
        let mut filled_oids = Vec::new();

        for price in prices {
            if taker_order.sz <= 0.0 {
                break;
            }

            let mut empty_level = false;
            if let Some(level) = maker_orders.get_mut(&price) {
                while taker_order.sz > 0.0 {
                    let Some(maker_order) = level.front_mut() else {
                        break;
                    };

                    let match_sz = taker_order.sz.min(maker_order.sz);
                    taker_order.sz = (taker_order.sz - match_sz).max(0.0);
                    maker_order.sz = (maker_order.sz - match_sz).max(0.0);

                    if maker_order.sz <= 0.0 {
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

        Ok(filled_oids)
    }
}
