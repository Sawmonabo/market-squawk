use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::order_book::TopOfBook;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OnlineFeatures {
    #[serde(with = "rust_decimal::serde::str")]
    pub mid_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub spread: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub micro_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub book_imbalance: Decimal,
}

impl OnlineFeatures {
    #[must_use]
    pub fn from_top(top: &TopOfBook) -> Option<Self> {
        if top.bid <= Decimal::ZERO
            || top.ask <= Decimal::ZERO
            || top.ask < top.bid
            || top.bid_size < Decimal::ZERO
            || top.ask_size < Decimal::ZERO
        {
            return None;
        }

        let two = Decimal::from(2_u32);
        let ten_thousand = Decimal::from(10_000_u32);
        let mid_price = (top.bid + top.ask) / two;
        let spread = top.ask - top.bid;
        let spread_bps = if mid_price == Decimal::ZERO {
            Decimal::ZERO
        } else {
            spread / mid_price * ten_thousand
        };

        let total_depth = top.bid_size + top.ask_size;
        let (micro_price, book_imbalance) = if total_depth == Decimal::ZERO {
            (mid_price, Decimal::ZERO)
        } else {
            (
                (top.ask * top.bid_size + top.bid * top.ask_size) / total_depth,
                (top.bid_size - top.ask_size) / total_depth,
            )
        };

        Some(Self {
            mid_price,
            spread,
            spread_bps,
            micro_price,
            book_imbalance,
        })
    }
}
