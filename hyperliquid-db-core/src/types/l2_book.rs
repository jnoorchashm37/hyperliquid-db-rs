use std::fmt::Write;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Hash)]
pub struct L2Book {
    pub coin:   String,
    pub levels: [Vec<L2BookLevel>; 2],
    pub time:   u64
}

impl L2Book {
    pub fn bids(&self) -> &[L2BookLevel] {
        &self.levels[0]
    }

    pub fn asks(&self) -> &[L2BookLevel] {
        &self.levels[1]
    }

    pub fn format_pretty(&self) -> String {
        let price_width = self
            .bids()
            .iter()
            .chain(self.asks().iter())
            .map(|level| level.px.len())
            .max()
            .unwrap_or_default()
            .max("PRICE".len());
        let size_width = self
            .bids()
            .iter()
            .chain(self.asks().iter())
            .map(|level| level.sz.len())
            .max()
            .unwrap_or_default()
            .max("SIZE".len());
        let order_count_width = self
            .bids()
            .iter()
            .chain(self.asks().iter())
            .map(|level| level.n.to_string().len())
            .max()
            .unwrap_or_default()
            .max("ORDERS".len());

        let separator = format!(
            "{}-+-{}-+-{}-+-{}",
            "-".repeat("SIDE".len()),
            "-".repeat(price_width),
            "-".repeat(size_width),
            "-".repeat(order_count_width)
        );

        let mut pretty = String::new();
        let _ = writeln!(pretty, "{} L2 book @ {}", self.coin, self.time);
        let _ = writeln!(
            pretty,
            "{:<side_width$} | {:>price_width$} | {:>size_width$} | {:>order_count_width$}",
            "SIDE",
            "PRICE",
            "SIZE",
            "ORDERS",
            side_width = "SIDE".len()
        );
        let _ = writeln!(pretty, "{separator}");

        for ask in self.asks().iter().rev() {
            let _ = writeln!(
                pretty,
                "{:<side_width$} | {:>price_width$} | {:>size_width$} | {:>order_count_width$}",
                "ASK",
                ask.px,
                ask.sz,
                ask.n,
                side_width = "SIDE".len()
            );
        }

        let _ = writeln!(pretty, "{separator}");

        for bid in self.bids() {
            let _ = writeln!(
                pretty,
                "{:<side_width$} | {:>price_width$} | {:>size_width$} | {:>order_count_width$}",
                "BID",
                bid.px,
                bid.sz,
                bid.n,
                side_width = "SIDE".len()
            );
        }

        pretty
    }
}

#[cfg(test)]
mod tests {
    use super::{L2Book, L2BookLevel};

    #[test]
    fn formats_orderbook_with_aligned_columns() {
        let book = L2Book {
            coin:   "BTC".to_string(),
            levels: [
                vec![
                    L2BookLevel { px: "100.5".to_string(), sz: "2".to_string(), n: 1 },
                    L2BookLevel { px: "99.75".to_string(), sz: "12.345".to_string(), n: 20 },
                ],
                vec![
                    L2BookLevel { px: "101".to_string(), sz: "3.4".to_string(), n: 2 },
                    L2BookLevel { px: "102.25".to_string(), sz: "10".to_string(), n: 1 },
                ]
            ],
            time:   1_780_394_399_398
        };

        assert_eq!(
            book.format_pretty(),
            concat!(
                "BTC L2 book @ 1780394399398\n",
                "SIDE |  PRICE |   SIZE | ORDERS\n",
                "-----+--------+--------+-------\n",
                "ASK  | 102.25 |     10 |      1\n",
                "ASK  |    101 |    3.4 |      2\n",
                "-----+--------+--------+-------\n",
                "BID  |  100.5 |      2 |      1\n",
                "BID  |  99.75 | 12.345 |     20\n",
            )
        );
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Serialize, Deserialize, Hash)]
pub struct L2BookLevel {
    pub px: String,
    pub sz: String,
    pub n:  u64
}
