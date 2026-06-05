use std::{
    collections::HashMap,
    fmt::{self, Formatter}
};

use crate::{
    processors::l4_orderbook::PRICE_MULTIPLIER,
    types::{L4Order, Side}
};

pub struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

impl<O> Snapshots<O> {
    pub const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub const fn as_ref(&self) -> &HashMap<Coin, Snapshot<O>> {
        &self.0
    }

    pub fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub trigger_px:        String,
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
            self.trigger_px = "0.0".to_string();
            self.trigger_condition = "Triggered".to_string();
            self.is_trigger = false;
            self.timestamp = ts;
            self.tif = Some("Gtc".to_string());
        }
    }

    fn coin(&self) -> Coin {
        self.coin.clone()
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
        let limit_px = Px::parse_from_str(&limit_px)?;
        let sz = Sz::parse_from_str(&sz)?;
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
        let limit_px = limit_px.to_str();
        let sz = sz.to_str();
        Self {
            user: Some(user),
            coin: coin.value(),
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

    pub const fn value(self) -> u64 {
        self.0
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub fn parse_from_str(value: &str) -> eyre::Result<Self> {
        let value = (value.parse::<f64>()? * PRICE_MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / PRICE_MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
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

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub fn parse_from_str(value: &str) -> eyre::Result<Self> {
        let value = (value.parse::<f64>()? * PRICE_MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / PRICE_MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
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
