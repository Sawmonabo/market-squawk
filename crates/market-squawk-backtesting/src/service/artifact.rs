//! Bounded canonical detailed-result encoding.

use std::io;

use serde::Serialize;

use crate::{AccountingReconciliation, BacktestRequest, BacktestRun, BacktestServiceError};

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWire<'a> {
    schema_version: u16,
    dataset_identity: String,
    object_graph_digest: String,
    execution_assumption_digest: String,
    run_input_digest: String,
    seed: u64,
    result_digest: String,
    accounting_reconciliation: &'static str,
    no_action_count: usize,
    sharpe: f64,
    return_observations: usize,
    return_skewness: f64,
    return_excess_kurtosis: f64,
    fills: FillSequence<'a>,
    equity_marks: MarkSequence<'a>,
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
    receivable_value: String,
    cash_entitlements: Vec<CashEntitlementWire>,
    realized_gain: BasisWire,
    realized_loss: BasisWire,
    fees: String,
    positions: Vec<PositionWire>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum BasisWire {
    Complete { amount: String, currency: String },
    Incomplete,
}
impl From<market_squawk_portfolio::BasisMeasurement> for BasisWire {
    fn from(value: market_squawk_portfolio::BasisMeasurement) -> Self {
        match value {
            market_squawk_portfolio::BasisMeasurement::Complete(value) => Self::Complete {
                amount: value.amount().to_string(),
                currency: value.currency().as_str().to_owned(),
            },
            market_squawk_portfolio::BasisMeasurement::Incomplete => Self::Incomplete,
        }
    }
}
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct CashEntitlementWire {
    action_evidence: String,
    instrument_id: String,
    entitled_at_unix_nanos: i64,
    amount: String,
    currency: String,
    payable_date: Option<String>,
    simulated_settlement_at_unix_nanos: Option<i64>,
    settled: bool,
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
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<tempfile::NamedTempFile, BacktestServiceError> {
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
        schema_version: 3,
        dataset_identity: hex(request.dataset_identity().bytes()),
        object_graph_digest: hex(request.dataset.object_graph_digest().bytes()),
        execution_assumption_digest: hex(request.assumption_digest().bytes()),
        run_input_digest: hex(request.run_input_digest().bytes()),
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
        fills: FillSequence(run),
        equity_marks: MarkSequence(run),
        portfolio: PortfolioWire {
            revision_id: hex(portfolio.token().bytes()),
            account_id: portfolio.account_id().as_uuid().to_string(),
            base_currency: portfolio.base_currency().as_str().to_owned(),
            cash: portfolio.cash().amount().to_string(),
            market_value: portfolio.market_value().amount().to_string(),
            gross_exposure: portfolio.gross_exposure().amount().to_string(),
            marked_equity: portfolio.marked_equity().amount().to_string(),
            receivable_value: portfolio.receivable_value().amount().to_string(),
            cash_entitlements: portfolio
                .cash_entitlements()
                .iter()
                .map(|value| CashEntitlementWire {
                    action_evidence: hex(value.action_evidence().bytes()),
                    instrument_id: value.instrument().as_uuid().to_string(),
                    entitled_at_unix_nanos: value.entitled_at().unix_nanos(),
                    amount: value.amount().amount().to_string(),
                    currency: value.amount().currency().as_str().to_owned(),
                    payable_date: value.payable_date().map(|date| date.to_string()),
                    simulated_settlement_at_unix_nanos: value
                        .simulated_settlement_at()
                        .map(|at| at.unix_nanos()),
                    settled: value.settled(),
                })
                .collect(),
            realized_gain: portfolio.realized_gain().into(),
            realized_loss: portfolio.realized_loss().into(),
            fees: portfolio.fees().amount().to_string(),
            positions,
        },
    };
    let scratch = request.dataset.observations.operation_scratch();
    let file = match scratch.as_ref() {
        Some(owner) => tempfile::NamedTempFile::new_in(owner.path()),
        None => tempfile::NamedTempFile::new(),
    }
    .map_err(|_| BacktestServiceError::ArtifactEncoding)?;
    let mut writer = BoundedBuffer::new(file, maximum_bytes, cancellation);
    serde_json::to_writer(&mut writer, &wire)
        .map_err(|_| BacktestServiceError::ArtifactEncoding)?;
    writer.finish()
}

#[derive(Debug)]
struct BoundedBuffer<'a> {
    file: tempfile::NamedTempFile,
    bytes: usize,
    maximum: usize,
    cancellation: &'a tokio_util::sync::CancellationToken,
}
impl<'a> BoundedBuffer<'a> {
    fn new(
        file: tempfile::NamedTempFile,
        maximum: usize,
        cancellation: &'a tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            file,
            bytes: 0,
            maximum,
            cancellation,
        }
    }
    fn finish(self) -> Result<tempfile::NamedTempFile, BacktestServiceError> {
        if self.bytes == 0 {
            return Err(BacktestServiceError::ArtifactEncoding);
        }
        self.file
            .as_file()
            .sync_all()
            .map_err(|_| BacktestServiceError::ArtifactEncoding)?;
        Ok(self.file)
    }
}
impl io::Write for BoundedBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::other("backtest cancelled"));
        }
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.maximum)
            .ok_or_else(|| io::Error::other("backtest artifact limit exceeded"))?;
        self.file.as_file_mut().write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.as_file_mut().flush()
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

#[derive(Debug)]
struct FillSequence<'a>(&'a BacktestRun);
impl Serialize for FillSequence<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(Some(self.0.fill_count()))?;
        for fill in self.0.fill_iter() {
            let fill = fill.map_err(serde::ser::Error::custom)?;
            let wire = FillWire {
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
            };
            sequence.serialize_element(&wire)?;
        }
        sequence.end()
    }
}
#[derive(Debug)]
struct MarkSequence<'a>(&'a BacktestRun);
impl Serialize for MarkSequence<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(None)?;
        for mark in self.0.equity_marks() {
            sequence.serialize_element(&mark.map_err(serde::ser::Error::custom)?.to_string())?;
        }
        sequence.end()
    }
}
