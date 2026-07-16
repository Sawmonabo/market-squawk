use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawEnvelope {
    pub event_id: Uuid,
    pub source: String,
    pub connection_id: Uuid,
    pub source_sequence: Option<u64>,
    pub exchange_at: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub payload: Vec<u8>,
}

impl RawEnvelope {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            source: source.into(),
            connection_id,
            source_sequence,
            exchange_at,
            received_at: Utc::now(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PriceLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookChange {
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketEvent {
    BookSnapshot {
        source: String,
        product: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        received_at: DateTime<Utc>,
    },
    BookDelta {
        source: String,
        product: String,
        changes: Vec<BookChange>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
    },
    Trade {
        source: String,
        product: String,
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        size: Decimal,
        /// Coinbase reports the resting maker order side, not aggressor direction.
        maker_side: Side,
        trade_id: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
    },
    Heartbeat {
        source: String,
        product: String,
        sequence: u64,
        last_trade_id: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
    },
    SourceStatus {
        source: String,
        status: String,
        detail: Option<String>,
        received_at: DateTime<Utc>,
    },
}

impl MarketEvent {
    #[must_use]
    pub fn product(&self) -> Option<&str> {
        match self {
            Self::BookSnapshot { product, .. }
            | Self::BookDelta { product, .. }
            | Self::Trade { product, .. }
            | Self::Heartbeat { product, .. } => Some(product),
            Self::SourceStatus { .. } => None,
        }
    }

    #[must_use]
    pub fn received_at(&self) -> DateTime<Utc> {
        match self {
            Self::BookSnapshot { received_at, .. }
            | Self::BookDelta { received_at, .. }
            | Self::Trade { received_at, .. }
            | Self::Heartbeat { received_at, .. }
            | Self::SourceStatus { received_at, .. } => *received_at,
        }
    }
}
