//! Bounded canonical detailed-result encoding.

use std::io;

use serde::Serialize;

use crate::{AccountingReconciliation, BacktestRequest, BacktestRun, BacktestServiceError};

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWire {
    schema_version: u16,
    dataset_identity: String,
    object_graph_digest: String,
    execution_assumption_digest: String,
    seed: u64,
    result_digest: String,
    accounting_reconciliation: &'static str,
    no_action_count: usize,
    sharpe: f64,
    return_observations: usize,
    return_skewness: f64,
    return_excess_kurtosis: f64,
    fills: Vec<FillWire>,
    portfolio: PortfolioWire,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct FillWire {
    order_id: String,
    instrument_id: String,
    signal_at_unix_nanos: i64,
    executed_at_unix_nanos: i64,
    side: &'static str,
    quantity_lots: i64,
    price_ticks: i64,
    fee_amount: String,
    fee_currency: String,
    partial: bool,
    execution_assumption_digest: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct PortfolioWire {
    revision_id: String,
    account_id: String,
    base_currency: String,
    cash: String,
    market_value: String,
    gross_exposure: String,
    marked_equity: String,
    realized_gain: String,
    realized_loss: String,
    fees: String,
    positions: Vec<PositionWire>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct PositionWire {
    instrument_id: String,
    quantity: String,
    market_value: String,
    market_value_currency: String,
}

pub(super) fn encode(
    request: &BacktestRequest,
    run: &BacktestRun,
    maximum_bytes: usize,
) -> Result<Vec<u8>, BacktestServiceError> {
    let fills = run
        .fills()
        .iter()
        .map(|fill| FillWire {
            order_id: fill.order_id().as_uuid().to_string(),
            instrument_id: fill.instrument_id().as_uuid().to_string(),
            signal_at_unix_nanos: fill.signal_at().unix_nanos(),
            executed_at_unix_nanos: fill.executed_at().unix_nanos(),
            side: match fill.side() {
                market_squawk_domain::OrderSide::Buy => "buy",
                market_squawk_domain::OrderSide::Sell => "sell",
            },
            quantity_lots: fill.quantity().get(),
            price_ticks: fill.price().get(),
            fee_amount: fill.fee().amount().to_string(),
            fee_currency: fill.fee().currency().as_str().to_owned(),
            partial: fill.partial(),
            execution_assumption_digest: hex(fill.assumption_digest().bytes()),
        })
        .collect();
    let portfolio = run.portfolio();
    let positions = portfolio
        .positions()
        .iter()
        .map(|position| PositionWire {
            instrument_id: position.instrument_id().as_uuid().to_string(),
            quantity: position.quantity().to_string(),
            market_value: position.market_value().amount().to_string(),
            market_value_currency: position.market_value().currency().as_str().to_owned(),
        })
        .collect();
    let wire = ArtifactWire {
        schema_version: 2,
        dataset_identity: hex(request.dataset_identity().bytes()),
        object_graph_digest: hex(request.dataset.object_graph_digest().bytes()),
        execution_assumption_digest: hex(request.assumption_digest().bytes()),
        seed: request.seed(),
        result_digest: hex(run.result_digest().bytes()),
        accounting_reconciliation: match run.accounting_reconciliation() {
            AccountingReconciliation::Independent => "independent",
        },
        no_action_count: run.no_action_count(),
        sharpe: run.performance().sharpe,
        return_observations: run.performance().observations,
        return_skewness: run.performance().skewness,
        return_excess_kurtosis: run.performance().excess_kurtosis,
        fills,
        portfolio: PortfolioWire {
            revision_id: hex(portfolio.token().bytes()),
            account_id: portfolio.account_id().as_uuid().to_string(),
            base_currency: portfolio.base_currency().as_str().to_owned(),
            cash: portfolio.cash().amount().to_string(),
            market_value: portfolio.market_value().amount().to_string(),
            gross_exposure: portfolio.gross_exposure().amount().to_string(),
            marked_equity: portfolio.marked_equity().amount().to_string(),
            realized_gain: portfolio.realized_gain().amount().to_string(),
            realized_loss: portfolio.realized_loss().amount().to_string(),
            fees: portfolio.fees().amount().to_string(),
            positions,
        },
    };
    let mut writer = BoundedBuffer::new(maximum_bytes);
    serde_json::to_writer(&mut writer, &wire)
        .map_err(|_| BacktestServiceError::ArtifactEncoding)?;
    writer.finish()
}

#[derive(Debug)]
struct BoundedBuffer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedBuffer {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn finish(self) -> Result<Vec<u8>, BacktestServiceError> {
        if self.bytes.is_empty() {
            Err(BacktestServiceError::ArtifactEncoding)
        } else {
            Ok(self.bytes)
        }
    }
}

impl io::Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("backtest artifact limit exceeded"))?;
        if next > self.maximum {
            return Err(io::Error::other("backtest artifact limit exceeded"));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| io::Error::other("backtest artifact allocation failed"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}
