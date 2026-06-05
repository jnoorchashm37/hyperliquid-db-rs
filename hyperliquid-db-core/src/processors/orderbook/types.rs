use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{self, Formatter}
};

use crate::{
    processors::orderbook::{PRICE_MULTIPLIER, book::OrderBook},
    types::{L4Order, Side}
};

pub struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

impl<O> Snapshots<O> {
    pub const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

impl Snapshots<InnerL4Order> {
    pub fn into_orderbooks(self, ignore_spot: bool) -> eyre::Result<BTreeMap<String, OrderBook>> {
        let mut order_books = BTreeMap::new();

        for (coin, mut snapshot) in self.value() {
            if ignore_spot && coin.is_spot() {
                continue;
            }

            snapshot.remove_triggers();
            let mut order_book = OrderBook::default();
            for order in snapshot.into_levels().into_iter().flatten() {
                order_book.add_order(order)?;
            }
            order_books.insert(coin.value(), order_book);
        }

        Ok(order_books)
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot<O>([Vec<O>; 2]);

impl<O> Snapshot<O> {
    pub fn new(val: [Vec<O>; 2]) -> Self {
        Self(val)
    }

    pub fn into_levels(self) -> [Vec<O>; 2] {
        self.0
    }
}

impl Snapshot<InnerL4Order> {
    fn remove_triggers(&mut self) {
        let bid_oids = self.0[0]
            .iter()
            .map(|order| order.oid)
            .collect::<HashSet<_>>();
        let ask_oids = self.0[1]
            .iter()
            .map(|order| order.oid)
            .collect::<HashSet<_>>();

        for orders in &mut self.0 {
            while let Some(order) = orders.last() {
                if bid_oids.contains(&order.oid) && ask_oids.contains(&order.oid) {
                    orders.pop();
                } else {
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InnerL4Order {
    pub user:              String,
    pub coin:              Coin,
    pub side:              Side,
    pub limit_px:          Px,
    pub sz:                Sz,
    pub oid:               u64,
    pub timestamp:         u64,
    pub trigger_condition: String,
    pub is_trigger:        bool,
    pub trigger_px:        f64,
    pub is_position_tpsl:  bool,
    pub reduce_only:       bool,
    pub order_type:        String,
    pub tif:               Option<String>,
    pub cloid:             Option<String>
}

impl InnerL4Order {
    pub fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn limit_px(&self) -> Px {
        self.limit_px
    }

    pub fn sz(&self) -> Sz {
        self.sz
    }

    pub fn decrement_sz(&mut self, dec: Sz) {
        self.sz.decrement_sz(dec.value());
    }

    pub fn modify_sz(&mut self, sz: Sz) {
        self.sz = sz;
    }

    pub fn fill(&mut self, maker_order: &mut Self) -> Sz {
        let match_sz = self.sz().min(maker_order.sz());
        self.decrement_sz(match_sz);
        maker_order.decrement_sz(match_sz);
        match_sz
    }

    pub fn convert_trigger(&mut self, ts: u64) {
        if self.is_trigger {
            self.trigger_px = 0.0;
            self.trigger_condition = "Triggered".to_string();
            self.is_trigger = false;
            self.timestamp = ts;
            self.tif = Some("Gtc".to_string());
        }
    }
}

impl TryFrom<(String, L4Order)> for InnerL4Order {
    type Error = eyre::ErrReport;

    fn try_from(value: (String, L4Order)) -> eyre::Result<Self> {
        let L4Order {
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
            ..
        } = value.1;
        let user = value.0;
        let limit_px = Px::new_f64(limit_px);
        let sz = Sz::new_f64(sz);
        Ok(Self {
            user,
            coin: Coin::new(&coin),
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid
        })
    }
}

impl From<InnerL4Order> for L4Order {
    fn from(value: InnerL4Order) -> Self {
        let InnerL4Order {
            user,
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid
        } = value;
        Self {
            user: Some(user),
            coin: coin.value(),
            side,
            limit_px: limit_px.to_f64(),
            sz: sz.to_f64(),
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Oid(u64);

impl Oid {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Coin(String);

impl Coin {
    pub fn new(coin: &str) -> Self {
        Self(coin.to_string())
    }

    pub fn value(&self) -> String {
        self.0.clone()
    }

    pub fn is_spot(&self) -> bool {
        self.0.starts_with('@') || self.0 == "PURR/USDC"
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Px(u64);

impl Px {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn new_f64(value: f64) -> Self {
        let value = (value * PRICE_MULTIPLIER).round() as u64;
        Self::new(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / PRICE_MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    pub const fn to_f64(self) -> f64 {
        self.value() as f64 / PRICE_MULTIPLIER
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub fn num_digits(self) -> u32 {
        if self.value() == 0 { 1 } else { (self.value() as f64).log10().floor() as u32 + 1 }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sz(u64);

impl Sz {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn new_f64(value: f64) -> Self {
        let value = (value * PRICE_MULTIPLIER).round() as u64;
        Self::new(value)
    }

    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn decrement_sz(&mut self, dec: u64) {
        self.0 = self.0.saturating_sub(dec);
    }

    #[must_use]
    pub fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / PRICE_MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    pub const fn to_f64(self) -> f64 {
        self.value() as f64 / PRICE_MULTIPLIER
    }
}

impl fmt::Debug for Px {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / PRICE_MULTIPLIER))
    }
}

impl fmt::Debug for Sz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / PRICE_MULTIPLIER))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_loader_filters_trailing_trigger_placeholders() {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            Coin::new("BTC"),
            Snapshot::new([
                vec![order(1, Side::Bid, 1.0), trigger_placeholder(2, Side::Bid)],
                vec![order(3, Side::Ask, 2.0), trigger_placeholder(2, Side::Ask)]
            ])
        );

        let order_books = Snapshots::new(snapshots).into_orderbooks(false).unwrap();

        assert_eq!(order_books.get("BTC").unwrap().order_count(), 2);
    }

    #[test]
    fn snapshot_loader_can_ignore_spot_books() {
        let mut snapshots = HashMap::new();
        snapshots.insert(Coin::new("@1"), Snapshot::new([vec![order(1, Side::Bid, 1.0)], vec![]]));

        let order_books = Snapshots::new(snapshots).into_orderbooks(true).unwrap();

        assert!(order_books.is_empty());
    }

    fn trigger_placeholder(oid: u64, side: Side) -> InnerL4Order {
        let mut order = order(oid, side, if side == Side::Bid { 0.5 } else { 2.5 });
        order.is_trigger = true;
        order
    }

    fn order(oid: u64, side: Side, limit_px: f64) -> InnerL4Order {
        InnerL4Order {
            user: "0x0000000000000000000000000000000000000000".to_string(),
            coin: Coin::new("BTC"),
            side,
            limit_px: Px::new_f64(limit_px),
            sz: Sz::new_f64(1.0),
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
