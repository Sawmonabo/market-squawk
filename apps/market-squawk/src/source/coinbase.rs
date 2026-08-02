use std::{future::Future, str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Sink, SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    domain::{BookChange, MarketEvent, PriceLevel, Side},
    source::{CaptureContext, MarketSource, SourceRunOutcome, send_event_until_cancelled},
};

const SOURCE_NAME: &str = "coinbase-exchange";
const DEFAULT_URL: &str = "wss://ws-feed.exchange.coinbase.com";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancellableOperation<T> {
    Completed(T),
    Cancelled,
}

#[derive(Debug)]
enum ControlMessageDisposition {
    Data(Message),
    Continue,
    ProviderClosed,
    Cancelled,
}

async fn await_or_cancel<T>(
    cancellation: &CancellationToken,
    operation: impl Future<Output = T>,
) -> CancellableOperation<T> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => CancellableOperation::Cancelled,
        output = operation => CancellableOperation::Completed(output),
    }
}

async fn handle_control_message<S, E>(
    message: Message,
    write: &mut S,
    cancellation: &CancellationToken,
) -> Result<ControlMessageDisposition>
where
    S: Sink<Message, Error = E> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    match message {
        Message::Ping(payload) => {
            match await_or_cancel(cancellation, write.send(Message::Pong(payload))).await {
                CancellableOperation::Completed(result) => result.map_err(anyhow::Error::new)?,
                CancellableOperation::Cancelled => {
                    return Ok(ControlMessageDisposition::Cancelled);
                }
            }
            Ok(ControlMessageDisposition::Continue)
        }
        Message::Close(frame) => {
            match await_or_cancel(cancellation, write.send(Message::Close(frame))).await {
                CancellableOperation::Completed(result) => result.map_err(anyhow::Error::new)?,
                CancellableOperation::Cancelled => {
                    return Ok(ControlMessageDisposition::Cancelled);
                }
            }
            Ok(ControlMessageDisposition::ProviderClosed)
        }
        message => Ok(ControlMessageDisposition::Data(message)),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionExit {
    Cancelled,
}

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
        cancellation: CancellationToken,
    ) -> Result<SourceRunOutcome> {
        if cancellation.is_cancelled() {
            return Ok(SourceRunOutcome::Cancelled);
        }
        let result = self.run_connection(&capture, &events, &cancellation).await;
        match result {
            Ok(ConnectionExit::Cancelled) => return Ok(SourceRunOutcome::Cancelled),
            Err(_error) if cancellation.is_cancelled() => return Ok(SourceRunOutcome::Cancelled),
            Err(_error) => {}
        }
        if !send_event_until_cancelled(
            &events,
            MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "disconnected".to_owned(),
                detail: Some("Coinbase Exchange session ended; reconnect required".to_owned()),
                received_at: Utc::now(),
            },
            &cancellation,
        )
        .await?
        {
            return Ok(SourceRunOutcome::Cancelled);
        }
        Ok(SourceRunOutcome::ReconnectRequired)
    }
}

impl CoinbaseSource {
    async fn run_connection(
        &self,
        capture: &CaptureContext,
        events: &mpsc::Sender<MarketEvent>,
        cancellation: &CancellationToken,
    ) -> Result<ConnectionExit> {
        validate_products(&self.products)?;
        let source_label: Arc<str> = Arc::from(SOURCE_NAME);
        if !send_event_until_cancelled(
            events,
            MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "connecting".to_owned(),
                detail: Some("Coinbase Exchange WebSocket connection".to_owned()),
                received_at: Utc::now(),
            },
            cancellation,
        )
        .await?
        {
            return Ok(ConnectionExit::Cancelled);
        }

        let (socket, _) =
            match await_or_cancel(cancellation, connect_async(self.url.as_str())).await {
                CancellableOperation::Completed(result) => {
                    result.with_context(|| format!("failed to connect to {}", self.url))?
                }
                CancellableOperation::Cancelled => return Ok(ConnectionExit::Cancelled),
            };
        let (mut write, mut read) = socket.split();

        let subscription = json!({
            "type": "subscribe",
            "product_ids": &self.products,
            "channels": ["level2", "heartbeat", "matches"]
        });
        match await_or_cancel(
            cancellation,
            write.send(Message::Text(subscription.to_string().into())),
        )
        .await
        {
            CancellableOperation::Completed(result) => result?,
            CancellableOperation::Cancelled => return Ok(ConnectionExit::Cancelled),
        }

        if !send_event_until_cancelled(
            events,
            MarketEvent::SourceStatus {
                source: SOURCE_NAME.to_owned(),
                status: "connected".to_owned(),
                detail: None,
                received_at: Utc::now(),
            },
            cancellation,
        )
        .await?
        {
            return Ok(ConnectionExit::Cancelled);
        }

        loop {
            let message = match await_or_cancel(cancellation, read.next()).await {
                CancellableOperation::Completed(Some(message)) => message?,
                CancellableOperation::Completed(None) => {
                    bail!("Coinbase WebSocket stream ended");
                }
                CancellableOperation::Cancelled => return Ok(ConnectionExit::Cancelled),
            };
            let message = match handle_control_message(message, &mut write, cancellation).await? {
                ControlMessageDisposition::Data(message) => message,
                ControlMessageDisposition::Continue => continue,
                ControlMessageDisposition::ProviderClosed => {
                    bail!("Coinbase closed the connection");
                }
                ControlMessageDisposition::Cancelled => {
                    return Ok(ConnectionExit::Cancelled);
                }
            };
            match message {
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
                        if !send_event_until_cancelled(events, event, cancellation).await? {
                            return Ok(ConnectionExit::Cancelled);
                        }
                        if is_error {
                            bail!("Coinbase returned an error message");
                        }
                    }
                }
                Message::Binary(_)
                | Message::Ping(_)
                | Message::Pong(_)
                | Message::Close(_)
                | Message::Frame(_) => {}
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

#[cfg(test)]
mod cancellation_tests {
    use std::{
        io,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use futures_util::Sink;
    use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
    use tokio_util::sync::CancellationToken;

    use super::{
        CancellableOperation, ControlMessageDisposition, await_or_cancel, handle_control_message,
    };

    #[derive(Debug)]
    struct StalledMessageSink {
        ready_polls: Arc<AtomicUsize>,
    }

    impl Sink<Message> for StalledMessageSink {
        type Error = io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.ready_polls.fetch_add(1, Ordering::AcqRel);
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn cancellation_preempts_a_stalled_transport_or_channel_operation() {
        let cancellation = CancellationToken::new();
        let operation = await_or_cancel(&cancellation, std::future::pending::<()>());
        tokio::pin!(operation);
        cancellation.cancel();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), operation).await,
            Ok(CancellableOperation::Cancelled)
        );
    }

    async fn assert_stalled_control_write_is_cancelled(
        incoming: Message,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let ready_polls = Arc::new(AtomicUsize::new(0));
        let task_ready_polls = Arc::clone(&ready_polls);
        let task = tokio::spawn(async move {
            let mut sink = StalledMessageSink {
                ready_polls: task_ready_polls,
            };
            handle_control_message(incoming, &mut sink, &task_cancellation).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while ready_polls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        cancellation.cancel();
        let disposition = tokio::time::timeout(Duration::from_secs(1), task).await???;

        assert!(matches!(disposition, ControlMessageDisposition::Cancelled));
        assert!(ready_polls.load(Ordering::Acquire) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn stalled_pong_write_is_cancelled_at_the_transport_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_stalled_control_write_is_cancelled(Message::Ping(vec![1, 2, 3].into())).await
    }

    #[tokio::test]
    async fn stalled_provider_close_reply_is_cancelled_at_the_transport_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_stalled_control_write_is_cancelled(Message::Close(Some(CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: "provider shutdown".into(),
        })))
        .await
    }
}
