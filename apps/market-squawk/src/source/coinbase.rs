use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::{
    domain::{BookChange, MarketEvent, PriceLevel, Side},
    source::{CaptureContext, MarketSource, SourceRunOutcome},
};

const SOURCE_NAME: &str = "coinbase-exchange";
const DEFAULT_URL: &str = "wss://ws-feed.exchange.coinbase.com";

#[derive(Debug, Clone)]
pub struct CoinbaseSource {
    products: Vec<String>,
    url: String,
}

impl CoinbaseSource {
    #[must_use]
    pub fn new(mut products: Vec<String>) -> Self {
        products.sort();
        products.dedup();
        Self {
            products,
            url: DEFAULT_URL.to_owned(),
        }
    }

    #[must_use]
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[async_trait]
impl MarketSource for CoinbaseSource {
    async fn run_session(
        &mut self,
        capture: CaptureContext,
        events: mpsc::Sender<MarketEvent>,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<SourceRunOutcome> {
        if *cancel.borrow() {
            return Ok(SourceRunOutcome::Cancelled);
        }
        let result = self.run_connection(&capture, &events, &mut cancel).await;
        if *cancel.borrow() {
            return Ok(SourceRunOutcome::Cancelled);
        }
        let detail = result.err().map(|error| format!("{error:#}"));
        events
            .send(MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "disconnected".to_owned(),
                detail,
                received_at: Utc::now(),
            })
            .await?;
        Ok(SourceRunOutcome::ReconnectRequired)
    }
}

impl CoinbaseSource {
    async fn run_connection(
        &self,
        capture: &CaptureContext,
        events: &mpsc::Sender<MarketEvent>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        validate_products(&self.products)?;
        let source_label: Arc<str> = Arc::from(SOURCE_NAME);
        events
            .send(MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "connecting".to_owned(),
                detail: Some(self.url.clone()),
                received_at: Utc::now(),
            })
            .await?;

        let (socket, _) = connect_async(self.url.as_str())
            .await
            .with_context(|| format!("failed to connect to {}", self.url))?;
        let (mut write, mut read) = socket.split();

        let subscription = json!({
            "type": "subscribe",
            "product_ids": &self.products,
            "channels": ["level2", "heartbeat", "matches"]
        });
        write
            .send(Message::Text(subscription.to_string().into()))
            .await?;

        events
            .send(MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "connected".to_owned(),
                detail: None,
                received_at: Utc::now(),
            })
            .await?;

        loop {
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        let _ = write.send(Message::Close(None)).await;
                        return Ok(());
                    }
                }
                message = read.next() => {
                    let message = match message {
                        Some(message) => message,
                        None => bail!("Coinbase WebSocket stream ended"),
                    };
                    match message? {
                        Message::Text(text) => {
                            let received_at = Utc::now();
                            let payload: Bytes = text.into();
                            let _capture_receipt = capture.publish(
                                Uuid::new_v4(),
                                Arc::clone(&source_label),
                                None,
                                None,
                                received_at,
                                payload.clone(),
                            )?;
                            let parsed = serde_json::from_slice::<Value>(&payload);
                            let parsed = parsed.context("Coinbase sent invalid JSON")?;
                            if let Some(event) = decode_message(&parsed, received_at)? {
                                let is_error = matches!(
                                    event,
                                    MarketEvent::SourceStatus { ref status, .. } if status == "error"
                                );
                                events.send(event).await?;
                                if is_error {
                                    bail!("Coinbase returned an error message");
                                }
                            }
                        }
                        Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                        Message::Close(frame) => {
                            bail!("Coinbase closed the connection: {frame:?}");
                        }
                        Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                    }
                }
            }
        }
    }
}

fn validate_products(products: &[String]) -> Result<()> {
    if products.is_empty() {
        bail!("at least one Coinbase product is required");
    }
    if products.len() > 100 {
        bail!("a single local Coinbase connection supports at most 100 configured products");
    }
    for product in products {
        if product.is_empty()
            || product.len() > 64
            || !product
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!("invalid Coinbase product identifier: {product:?}");
        }
    }
    Ok(())
}

pub fn decode_message(value: &Value, received_at: DateTime<Utc>) -> Result<Option<MarketEvent>> {
    let message_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match message_type {
        "snapshot" => {
            let product = required_string(value, "product_id")?;
            let bids = parse_levels(value.get("bids"), "bids")?;
            let asks = parse_levels(value.get("asks"), "asks")?;
            Ok(Some(MarketEvent::BookSnapshot {
                source: SOURCE_NAME.to_owned(),
                product,
                bids,
                asks,
                received_at,
            }))
        }
        "l2update" => {
            let product = required_string(value, "product_id")?;
            let changes = parse_changes(value.get("changes"))?;
            Ok(Some(MarketEvent::BookDelta {
                source: SOURCE_NAME.to_owned(),
                product,
                changes,
                exchange_at: parse_optional_time(value.get("time"))?,
                received_at,
            }))
        }
        "heartbeat" => Ok(Some(MarketEvent::Heartbeat {
            source: SOURCE_NAME.to_owned(),
            product: required_string(value, "product_id")?,
            sequence: value
                .get("sequence")
                .and_then(Value::as_u64)
                .context("heartbeat missing sequence")?,
            last_trade_id: value.get("last_trade_id").and_then(Value::as_u64),
            exchange_at: parse_optional_time(value.get("time"))?,
            received_at,
        })),
        "match" | "last_match" => Ok(Some(MarketEvent::Trade {
            source: SOURCE_NAME.to_owned(),
            product: required_string(value, "product_id")?,
            price: positive_decimal(value.get("price"), "price")?,
            size: positive_decimal(value.get("size"), "size")?,
            maker_side: parse_side(value.get("side"))?,
            trade_id: value.get("trade_id").and_then(Value::as_u64),
            exchange_at: parse_optional_time(value.get("time"))?,
            received_at,
        })),
        "error" => Ok(Some(MarketEvent::SourceStatus {
            source: SOURCE_NAME.to_owned(),
            status: "error".to_owned(),
            detail: value
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_owned),
            received_at,
        })),
        "subscriptions" => Ok(Some(MarketEvent::SourceStatus {
            source: SOURCE_NAME.to_owned(),
            status: "subscribed".to_owned(),
            detail: None,
            received_at,
        })),
        _ => Ok(None),
    }
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("message missing string field {field}"))
}

fn parse_decimal(value: Option<&Value>, field: &str) -> Result<Decimal> {
    let raw = value
        .and_then(Value::as_str)
        .with_context(|| format!("message missing decimal field {field}"))?;
    Decimal::from_str(raw).with_context(|| format!("invalid decimal in {field}: {raw}"))
}

fn positive_decimal(value: Option<&Value>, field: &str) -> Result<Decimal> {
    let parsed = parse_decimal(value, field)?;
    if parsed <= Decimal::ZERO {
        bail!("{field} must be positive");
    }
    Ok(parsed)
}

fn non_negative_decimal(value: Option<&Value>, field: &str) -> Result<Decimal> {
    let parsed = parse_decimal(value, field)?;
    if parsed < Decimal::ZERO {
        bail!("{field} must not be negative");
    }
    Ok(parsed)
}

fn parse_side(value: Option<&Value>) -> Result<Side> {
    match value.and_then(Value::as_str) {
        Some("buy") => Ok(Side::Buy),
        Some("sell") => Ok(Side::Sell),
        other => bail!("invalid side: {other:?}"),
    }
}

fn parse_levels(value: Option<&Value>, field: &str) -> Result<Vec<PriceLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .with_context(|| format!("message missing {field} array"))?;
    rows.iter()
        .map(|row| {
            let values = row
                .as_array()
                .with_context(|| format!("{field} row is not an array"))?;
            Ok(PriceLevel {
                price: positive_decimal(values.first(), "level.price")?,
                size: positive_decimal(values.get(1), "level.size")?,
            })
        })
        .collect()
}

fn parse_changes(value: Option<&Value>) -> Result<Vec<BookChange>> {
    let rows = value
        .and_then(Value::as_array)
        .context("message missing changes array")?;
    rows.iter()
        .map(|row| {
            let values = row.as_array().context("change row is not an array")?;
            Ok(BookChange {
                side: parse_side(values.first())?,
                price: positive_decimal(values.get(1), "change.price")?,
                size: non_negative_decimal(values.get(2), "change.size")?,
            })
        })
        .collect()
}

fn parse_optional_time(value: Option<&Value>) -> Result<Option<DateTime<Utc>>> {
    let raw = match value {
        Some(raw) => raw,
        None => return Ok(None),
    };
    let raw = raw.as_str().context("exchange time must be a string")?;
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid RFC 3339 timestamp: {raw}"))?;
    Ok(Some(parsed.with_timezone(&Utc)))
}
