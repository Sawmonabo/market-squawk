use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::{
    domain::{BookChange, MarketEvent, PriceLevel, RawEnvelope, Side},
    journal::JournalSink,
    source::MarketSource,
};

#[derive(Debug, Clone)]
pub struct MockSource {
    product: String,
    events: usize,
}

impl MockSource {
    #[must_use]
    pub fn new(product: impl Into<String>, events: usize) -> Self {
        Self {
            product: product.into(),
            events,
        }
    }
}

#[async_trait]
impl MarketSource for MockSource {
    async fn run(
        self: Box<Self>,
        journal: JournalSink,
        output: mpsc::Sender<MarketEvent>,
        cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        let source = "mock".to_owned();
        let connection_id = Uuid::new_v4();
        let received_at = Utc::now();
        let bids = vec![PriceLevel {
            price: Decimal::from(100_u32),
            size: Decimal::from(2_u32),
        }];
        let asks = vec![PriceLevel {
            price: Decimal::from(101_u32),
            size: Decimal::from(2_u32),
        }];
        let snapshot_payload = serde_json::to_vec(&json!({
            "kind": "mock_snapshot",
            "product": self.product.as_str(),
            "bids": [["100", "2"]],
            "asks": [["101", "2"]]
        }))?;
        journal
            .append(RawEnvelope::new(
                &source,
                connection_id,
                Some(1),
                Some(received_at),
                snapshot_payload,
            ))
            .await?;
        output
            .send(MarketEvent::BookSnapshot {
                source: source.clone(),
                product: self.product.clone(),
                bids,
                asks,
                received_at,
            })
            .await?;

        for index in 0..self.events {
            if *cancel.borrow() {
                break;
            }
            let sequence = u64::try_from(index)?.saturating_add(2);
            let price = Decimal::from(100_u32) + Decimal::new(i64::try_from(index % 25)?, 2);
            let size = Decimal::from(1_u32) + Decimal::new(i64::try_from(index % 10)?, 2);
            let side = if index % 2 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let received_at = Utc::now();
            let raw = serde_json::to_vec(&json!({
                "kind": "mock_delta",
                "sequence": sequence,
                "product": self.product.as_str(),
                "side": side,
                "price": price.to_string(),
                "size": size.to_string()
            }))?;
            journal
                .append(RawEnvelope::new(
                    &source,
                    connection_id,
                    Some(sequence),
                    Some(received_at),
                    raw,
                ))
                .await?;
            output
                .send(MarketEvent::BookDelta {
                    source: source.clone(),
                    product: self.product.clone(),
                    changes: vec![BookChange { side, price, size }],
                    exchange_at: Some(received_at),
                    received_at,
                })
                .await?;
        }

        journal.flush().await?;
        Ok(())
    }
}
