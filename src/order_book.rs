use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{BookChange, PriceLevel, Side};

#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopOfBook {
    #[serde(with = "rust_decimal::serde::str")]
    pub bid: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask_size: Decimal,
}

impl OrderBook {
    pub fn apply_snapshot(&mut self, bids: &[PriceLevel], asks: &[PriceLevel]) {
        self.bids.clear();
        self.asks.clear();

        for level in bids {
            if level.size > Decimal::ZERO {
                self.bids.insert(level.price, level.size);
            }
        }

        for level in asks {
            if level.size > Decimal::ZERO {
                self.asks.insert(level.price, level.size);
            }
        }
    }

    pub fn apply_changes(&mut self, changes: &[BookChange]) {
        for change in changes {
            let side = match change.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };

            if change.size == Decimal::ZERO {
                side.remove(&change.price);
            } else {
                side.insert(change.price, change.size);
            }
        }
    }

    #[must_use]
    pub fn top(&self) -> Option<TopOfBook> {
        let (&bid, &bid_size) = self.bids.last_key_value()?;
        let (&ask, &ask_size) = self.asks.first_key_value()?;

        Some(TopOfBook {
            bid,
            bid_size,
            ask,
            ask_size,
        })
    }

    #[must_use]
    pub fn is_crossed(&self) -> bool {
        self.top().is_some_and(|top| top.bid >= top.ask)
    }

    #[must_use]
    pub fn bid_levels(&self) -> usize {
        self.bids.len()
    }

    #[must_use]
    pub fn ask_levels(&self) -> usize {
        self.asks.len()
    }
}
