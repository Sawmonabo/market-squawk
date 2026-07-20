//! Bounded paper state images and strict opaque recovery checkpoints.

use std::collections::BTreeMap;
use std::io::{self, Write};

use market_squawk_domain::{
    AccountId, ClientOrderId, Money, OrderId, PriceTicks, QuantityLots, Timestamp,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::ledger::LedgerRecoveryWire;
use crate::order::{PaperOrder, PaperOrderRecoveryWire};
use crate::{
    LiquidityRole, PaperAccountRiskSnapshot, PaperCashBalance, PaperLedger, PaperOrderState,
    PaperPosition,
};

/// Immutable public order state image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperOrderSnapshot {
    order_id: OrderId,
    account_id: AccountId,
    state: PaperOrderState,
    requested: QuantityLots,
    cumulative_filled: QuantityLots,
    average_fill_price: Option<PriceTicks>,
    cumulative_fees: Money,
    accepted_at: Timestamp,
    eligible_at: Timestamp,
    expires_at: Timestamp,
    revision: u64,
}

impl PaperOrderSnapshot {
    pub(crate) fn from_order(order: &PaperOrder) -> Self {
        Self {
            order_id: order.order_id,
            account_id: order.account_id,
            state: order.lifecycle.state(),
            requested: order.quantity,
            cumulative_filled: order.lifecycle.cumulative_filled(),
            average_fill_price: order.average_fill_price(),
            cumulative_fees: order.cumulative_fee,
            accepted_at: order.accepted_at,
            eligible_at: order.eligible_at,
            expires_at: order.expires_at,
            revision: order.lifecycle.revision(),
        }
    }

    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    pub const fn state(&self) -> PaperOrderState {
        self.state
    }
    pub const fn requested(&self) -> QuantityLots {
        self.requested
    }
    pub const fn cumulative_filled(&self) -> QuantityLots {
        self.cumulative_filled
    }
    pub const fn average_fill_price(&self) -> Option<PriceTicks> {
        self.average_fill_price
    }
    pub const fn cumulative_fees(&self) -> Money {
        self.cumulative_fees
    }
    pub const fn accepted_at(&self) -> Timestamp {
        self.accepted_at
    }
    pub const fn eligible_at(&self) -> Timestamp {
        self.eligible_at
    }
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// One immutable fill retained for accounting and recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperFillSnapshot {
    sequence: u64,
    order_id: OrderId,
    event_at: Timestamp,
    quantity: QuantityLots,
    average_price: PriceTicks,
    notional: Money,
    fee: Money,
    liquidity: LiquidityRole,
}

impl PaperFillSnapshot {
    #[allow(
        clippy::too_many_arguments,
        reason = "fill evidence binds every independent accounting value"
    )]
    pub(crate) const fn new(
        sequence: u64,
        order_id: OrderId,
        event_at: Timestamp,
        quantity: QuantityLots,
        average_price: PriceTicks,
        notional: Money,
        fee: Money,
        liquidity: LiquidityRole,
    ) -> Self {
        Self {
            sequence,
            order_id,
            event_at,
            quantity,
            average_price,
            notional,
            fee,
            liquidity,
        }
    }
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }
    pub const fn event_at(self) -> Timestamp {
        self.event_at
    }
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
    pub const fn average_price(self) -> PriceTicks {
        self.average_price
    }
    pub const fn notional(self) -> Money {
        self.notional
    }
    pub const fn fee(self) -> Money {
        self.fee
    }
    pub const fn liquidity(self) -> LiquidityRole {
        self.liquidity
    }
}

/// Complete bounded point-in-time image returned by the paper worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperExecutionSnapshot {
    configuration_digest: [u8; 32],
    sequence: u64,
    complete: bool,
    reconciliation_required: bool,
    orders: Box<[PaperOrderSnapshot]>,
    fills: Box<[PaperFillSnapshot]>,
    accounts: Box<[PaperAccountRiskSnapshot]>,
    cash: Box<[PaperCashBalance]>,
    positions: Box<[PaperPosition]>,
}

impl PaperExecutionSnapshot {
    pub(crate) fn from_state(
        configuration_digest: [u8; 32],
        sequence: u64,
        reconciliation_required: bool,
        orders: &BTreeMap<OrderId, PaperOrder>,
        fills: &[PaperFillSnapshot],
        ledger: &PaperLedger,
    ) -> Self {
        Self {
            configuration_digest,
            sequence,
            complete: true,
            reconciliation_required,
            orders: orders
                .values()
                .map(PaperOrderSnapshot::from_order)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            fills: fills.to_vec().into_boxed_slice(),
            accounts: ledger.account_risk_snapshot().into_boxed_slice(),
            cash: ledger.cash_snapshot().into_boxed_slice(),
            positions: ledger.position_snapshot().into_boxed_slice(),
        }
    }

    pub const fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    pub const fn complete(&self) -> bool {
        self.complete
    }
    pub const fn reconciliation_required(&self) -> bool {
        self.reconciliation_required
    }
    pub const fn orders(&self) -> &[PaperOrderSnapshot] {
        &self.orders
    }
    pub const fn fills(&self) -> &[PaperFillSnapshot] {
        &self.fills
    }
    pub const fn accounts(&self) -> &[PaperAccountRiskSnapshot] {
        &self.accounts
    }
    pub const fn cash(&self) -> &[PaperCashBalance] {
        &self.cash
    }
    pub const fn positions(&self) -> &[PaperPosition] {
        &self.positions
    }
}

/// Versioned complete checkpoint with private construction and strict same-config restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperExecutionCheckpoint {
    pub(crate) schema_version: u32,
    pub(crate) configuration_digest: [u8; 32],
    pub(crate) complete: bool,
    pub(crate) sequence: u64,
    pub(crate) reconciliation_required: bool,
    pub(crate) orders: BTreeMap<OrderId, PaperOrder>,
    pub(crate) fills: Vec<PaperFillSnapshot>,
    pub(crate) ledger: PaperLedger,
    pub(crate) idempotency: BTreeMap<(AccountId, ClientOrderId), OrderId>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckpointWire {
    schema_version: u32,
    configuration_digest: [u8; 32],
    complete: bool,
    sequence: u64,
    reconciliation_required: bool,
    orders: Vec<PaperOrderRecoveryWire>,
    fills: Vec<FillRecoveryWire>,
    ledger: LedgerRecoveryWire,
    idempotency: Vec<IdempotencyRecoveryWire>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FillRecoveryWire {
    sequence: u64,
    order_id: OrderId,
    event_at: Timestamp,
    quantity: QuantityLots,
    average_price: PriceTicks,
    notional: Money,
    fee: Money,
    liquidity: LiquidityRole,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IdempotencyRecoveryWire {
    account_id: AccountId,
    client_order_id: ClientOrderId,
    order_id: OrderId,
}

impl PaperExecutionCheckpoint {
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }
    pub const fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }
    pub const fn complete(&self) -> bool {
        self.complete
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Encodes a complete checkpoint without filesystem access under a caller-selected byte cap.
    pub fn encode(&self, maximum_bytes: usize) -> Result<Vec<u8>, PaperCheckpointError> {
        if maximum_bytes == 0 || !self.complete {
            return Err(PaperCheckpointError::InvalidHeader);
        }
        let wire = self.recovery_wire();
        let mut output = BoundedCheckpointWriter::new(maximum_bytes)?;
        serde_json::to_writer(&mut output, &wire).map_err(PaperCheckpointError::Encoding)?;
        Ok(output.into_inner())
    }

    pub(crate) fn recovery_input_digest(&self) -> Result<[u8; 32], PaperCheckpointError> {
        let mut output = CheckpointDigestWriter(Sha256::new());
        serde_json::to_writer(&mut output, &self.recovery_wire())
            .map_err(PaperCheckpointError::Encoding)?;
        Ok(output.0.finalize().into())
    }

    fn recovery_wire(&self) -> CheckpointWire {
        CheckpointWire {
            schema_version: self.schema_version,
            configuration_digest: self.configuration_digest,
            complete: self.complete,
            sequence: self.sequence,
            reconciliation_required: self.reconciliation_required,
            orders: self
                .orders
                .values()
                .map(PaperOrder::recovery_wire)
                .collect(),
            fills: self
                .fills
                .iter()
                .map(|fill| FillRecoveryWire {
                    sequence: fill.sequence,
                    order_id: fill.order_id,
                    event_at: fill.event_at,
                    quantity: fill.quantity,
                    average_price: fill.average_price,
                    notional: fill.notional,
                    fee: fill.fee,
                    liquidity: fill.liquidity,
                })
                .collect(),
            ledger: self.ledger.recovery_wire(),
            idempotency: self
                .idempotency
                .iter()
                .map(
                    |((account_id, client_order_id), order_id)| IdempotencyRecoveryWire {
                        account_id: *account_id,
                        client_order_id: client_order_id.clone(),
                        order_id: *order_id,
                    },
                )
                .collect(),
        }
    }

    /// Decodes and fully revalidates a checkpoint against exact current configuration and bounds.
    pub fn decode(
        config: crate::PaperExecutionConfig,
        bytes: &[u8],
        maximum_bytes: usize,
    ) -> Result<Self, PaperCheckpointError> {
        if bytes.is_empty() || bytes.len() > maximum_bytes || maximum_bytes == 0 {
            return Err(PaperCheckpointError::TooLarge);
        }
        let wire: CheckpointWire =
            serde_json::from_slice(bytes).map_err(PaperCheckpointError::Encoding)?;
        let limits = config.input();
        if wire.schema_version != crate::PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION
            || wire.configuration_digest != config.digest()
            || !wire.complete
            || wire.orders.len() > limits.maximum_orders.get()
            || wire.fills.len() > limits.maximum_fills.get()
            || wire.idempotency.len() > limits.maximum_idempotency_keys.get()
        {
            return Err(PaperCheckpointError::InvalidHeader);
        }
        let mut orders = BTreeMap::new();
        for order_wire in wire.orders {
            let order = PaperOrder::try_from_recovery_wire(order_wire)
                .map_err(|_| PaperCheckpointError::InvalidOrder)?;
            if order.lifecycle.last_sequence() > wire.sequence
                || orders.insert(order.order_id, order).is_some()
            {
                return Err(PaperCheckpointError::InvalidOrder);
            }
        }
        let fills = validate_fills(wire.fills, wire.sequence, &orders)?;
        let ledger = PaperLedger::try_from_recovery_wire(config.ledger_config(), wire.ledger)
            .map_err(PaperCheckpointError::Ledger)?;
        validate_reservation_shape(&orders, &ledger)?;
        let idempotency = validate_idempotency(wire.idempotency, &orders)?;
        Ok(Self {
            schema_version: wire.schema_version,
            configuration_digest: wire.configuration_digest,
            complete: wire.complete,
            sequence: wire.sequence,
            reconciliation_required: wire.reconciliation_required,
            orders,
            fills,
            ledger,
            idempotency,
        })
    }
}

#[derive(Debug)]
struct CheckpointDigestWriter(Sha256);

impl Write for CheckpointDigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct BoundedCheckpointWriter {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedCheckpointWriter {
    fn new(maximum_bytes: usize) -> Result<Self, PaperCheckpointError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve(maximum_bytes.min(8 * 1024))
            .map_err(|_| PaperCheckpointError::Allocation)?;
        Ok(Self {
            bytes,
            maximum_bytes,
        })
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedCheckpointWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("paper checkpoint byte bound overflowed"))?;
        if next_len > self.maximum_bytes {
            return Err(io::Error::other("paper checkpoint byte bound exceeded"));
        }
        self.bytes
            .try_reserve(buffer.len())
            .map_err(|_| io::Error::other("paper checkpoint allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_fills(
    wire_fills: Vec<FillRecoveryWire>,
    checkpoint_sequence: u64,
    orders: &BTreeMap<OrderId, PaperOrder>,
) -> Result<Vec<PaperFillSnapshot>, PaperCheckpointError> {
    let mut fills = Vec::new();
    fills
        .try_reserve_exact(wire_fills.len())
        .map_err(|_| PaperCheckpointError::Allocation)?;
    let mut totals: BTreeMap<OrderId, (i64, i128, Decimal)> = BTreeMap::new();
    let mut last_sequence = 0_u64;
    for fill in wire_fills {
        let order = orders
            .get(&fill.order_id)
            .ok_or(PaperCheckpointError::InvalidFill)?;
        if fill.sequence <= last_sequence
            || fill.sequence > checkpoint_sequence
            || fill.quantity.get() == 0
            || fill.average_price.get() <= 0
            || fill.event_at < order.eligible_at
            || fill.event_at > order.expires_at
            || fill.notional.currency() != order.terms.quote_currency()
            || fill.notional.amount() <= Decimal::ZERO
            || fill.fee.currency() != order.terms.quote_currency()
            || fill.fee.amount().is_sign_negative()
        {
            return Err(PaperCheckpointError::InvalidFill);
        }
        let total = totals.entry(fill.order_id).or_insert((0, 0, Decimal::ZERO));
        total.0 = total
            .0
            .checked_add(fill.quantity.get())
            .ok_or(PaperCheckpointError::InvalidFill)?;
        total.1 = total
            .1
            .checked_add(
                i128::from(fill.average_price.get())
                    .checked_mul(i128::from(fill.quantity.get()))
                    .ok_or(PaperCheckpointError::InvalidFill)?,
            )
            .ok_or(PaperCheckpointError::InvalidFill)?;
        total.2 = total
            .2
            .checked_add(fill.fee.amount())
            .ok_or(PaperCheckpointError::InvalidFill)?;
        last_sequence = fill.sequence;
        fills.push(PaperFillSnapshot::new(
            fill.sequence,
            fill.order_id,
            fill.event_at,
            fill.quantity,
            fill.average_price,
            fill.notional,
            fill.fee,
            fill.liquidity,
        ));
    }
    for order in orders.values() {
        let (quantity, weighted, fees) =
            totals
                .get(&order.order_id)
                .copied()
                .unwrap_or((0, 0, Decimal::ZERO));
        if quantity != order.lifecycle.cumulative_filled().get()
            || weighted != order.weighted_fill_ticks
            || fees != order.cumulative_fee.amount()
        {
            return Err(PaperCheckpointError::InvalidFill);
        }
    }
    Ok(fills)
}

fn validate_reservation_shape(
    orders: &BTreeMap<OrderId, PaperOrder>,
    ledger: &PaperLedger,
) -> Result<(), PaperCheckpointError> {
    let mut required_count = 0_usize;
    for order in orders.values() {
        let requires_reservation = matches!(
            order.lifecycle.state(),
            PaperOrderState::Accepted
                | PaperOrderState::PartiallyFilled
                | PaperOrderState::CancelPending
        );
        if requires_reservation {
            required_count = required_count
                .checked_add(1)
                .ok_or(PaperCheckpointError::InvalidReservation)?;
        }
        if ledger.has_reservation(order.order_id) != requires_reservation {
            return Err(PaperCheckpointError::InvalidReservation);
        }
    }
    if ledger.reservation_count() != required_count {
        return Err(PaperCheckpointError::InvalidReservation);
    }
    Ok(())
}

fn validate_idempotency(
    entries: Vec<IdempotencyRecoveryWire>,
    orders: &BTreeMap<OrderId, PaperOrder>,
) -> Result<BTreeMap<(AccountId, ClientOrderId), OrderId>, PaperCheckpointError> {
    if entries.len() != orders.len() {
        return Err(PaperCheckpointError::InvalidIdempotency);
    }
    let mut idempotency = BTreeMap::new();
    for entry in entries {
        let order = orders
            .get(&entry.order_id)
            .ok_or(PaperCheckpointError::InvalidIdempotency)?;
        if order.account_id != entry.account_id
            || order.client_order_id != entry.client_order_id
            || idempotency
                .insert((entry.account_id, entry.client_order_id), entry.order_id)
                .is_some()
        {
            return Err(PaperCheckpointError::InvalidIdempotency);
        }
    }
    Ok(idempotency)
}

/// Strict bounded checkpoint codec or invariant failure.
#[derive(Debug, Error)]
pub enum PaperCheckpointError {
    #[error("paper checkpoint exceeds its byte bound")]
    TooLarge,
    #[error("paper checkpoint header, schema, completeness, or configuration is invalid")]
    InvalidHeader,
    #[error("paper checkpoint order state is invalid")]
    InvalidOrder,
    #[error("paper checkpoint fill evidence is invalid")]
    InvalidFill,
    #[error("paper checkpoint reservation state is invalid")]
    InvalidReservation,
    #[error("paper checkpoint idempotency state is invalid")]
    InvalidIdempotency,
    #[error("paper checkpoint bounded allocation failed")]
    Allocation,
    #[error("paper checkpoint JSON encoding or decoding failed")]
    Encoding(#[source] serde_json::Error),
    #[error("paper checkpoint ledger is invalid")]
    Ledger(#[source] crate::PaperLedgerError),
}
