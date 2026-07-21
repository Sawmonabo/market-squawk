//! Bounded paper state images and strict opaque recovery checkpoints.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use market_squawk_domain::{
    AccountId, ClientOrderId, Money, OrderId, PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_execution::{ReconciliationBatchBinding, ReconciliationBatchId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::ledger::{LedgerRecoveryWire, checked_notional};
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
    maximum_execution_price: PriceTicks,
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
            maximum_execution_price: order.execution_price_bound.maximum_price(),
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
    pub const fn maximum_execution_price(&self) -> PriceTicks {
        self.maximum_execution_price
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
    active_orders: Box<[PaperOrderSnapshot]>,
    archived_orders: Box<[PaperOrderSnapshot]>,
    fills: Box<[PaperFillSnapshot]>,
    accounts: Box<[PaperAccountRiskSnapshot]>,
    cash: Box<[PaperCashBalance]>,
    positions: Box<[PaperPosition]>,
}

impl PaperExecutionSnapshot {
    #[expect(
        clippy::too_many_arguments,
        reason = "a snapshot binds eight independent persisted state components at one consistency boundary"
    )]
    pub(crate) fn from_state(
        configuration_digest: [u8; 32],
        sequence: u64,
        reconciliation_required: bool,
        orders: &BTreeMap<OrderId, PaperOrder>,
        fills: &[PaperFillSnapshot],
        archived_orders: &BTreeMap<OrderId, PaperOrder>,
        archived_fills: &[PaperFillSnapshot],
        ledger: &PaperLedger,
    ) -> Self {
        let active_orders = orders
            .values()
            .map(PaperOrderSnapshot::from_order)
            .collect::<Vec<_>>();
        let archived_order_snapshots = archived_orders
            .values()
            .map(PaperOrderSnapshot::from_order)
            .collect::<Vec<_>>();
        let mut all_orders = active_orders.clone();
        all_orders.extend(archived_order_snapshots.iter().cloned());
        all_orders.sort_unstable_by_key(PaperOrderSnapshot::order_id);
        let mut all_fills = fills.to_vec();
        all_fills.extend_from_slice(archived_fills);
        all_fills.sort_unstable_by_key(|fill| fill.sequence);
        Self {
            configuration_digest,
            sequence,
            complete: true,
            reconciliation_required,
            orders: all_orders.into_boxed_slice(),
            active_orders: active_orders.into_boxed_slice(),
            archived_orders: archived_order_snapshots.into_boxed_slice(),
            fills: all_fills.into_boxed_slice(),
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
    pub const fn active_orders(&self) -> &[PaperOrderSnapshot] {
        &self.active_orders
    }
    pub const fn archived_orders(&self) -> &[PaperOrderSnapshot] {
        &self.archived_orders
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
    pub(crate) archived_orders: BTreeMap<OrderId, PaperOrder>,
    pub(crate) archived_fills: Vec<PaperFillSnapshot>,
    pub(crate) durable_sequence: u64,
    pub(crate) reconciled_orders: BTreeSet<OrderId>,
    pub(crate) acknowledged_reconciliation_batches: Vec<ReconciliationBatchBinding>,
    pub(crate) ledger: PaperLedger,
    pub(crate) idempotency: BTreeMap<(AccountId, ClientOrderId), OrderId>,
}

/// Exact checkpoint identity supplied with dispatcher-minted persistence authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperCheckpointPersistenceEvidence {
    pub(crate) configuration_digest: [u8; 32],
    pub(crate) sequence: u64,
    pub(crate) recovery_digest: [u8; 32],
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
    archived_orders: Vec<PaperOrderRecoveryWire>,
    archived_fills: Vec<FillRecoveryWire>,
    durable_sequence: u64,
    reconciled_orders: Vec<OrderId>,
    acknowledged_reconciliation_batches: Vec<AcknowledgedReconciliationBatchWire>,
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AcknowledgedReconciliationBatchWire {
    batch_id: [u8; 32],
    binding_digest: [u8; 32],
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

    /// Binds caller-confirmed persisted bytes to this exact complete checkpoint image.
    pub fn persistence_evidence(
        &self,
        persisted_bytes: &[u8],
    ) -> Result<PaperCheckpointPersistenceEvidence, PaperCheckpointError> {
        if persisted_bytes.is_empty()
            || self.encode(persisted_bytes.len())?.as_slice() != persisted_bytes
        {
            return Err(PaperCheckpointError::InvalidPersistenceEvidence);
        }
        Ok(PaperCheckpointPersistenceEvidence {
            configuration_digest: self.configuration_digest,
            sequence: self.sequence,
            recovery_digest: self.recovery_input_digest()?,
        })
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
            archived_orders: self
                .archived_orders
                .values()
                .map(PaperOrder::recovery_wire)
                .collect(),
            archived_fills: self
                .archived_fills
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
            durable_sequence: self.durable_sequence,
            reconciled_orders: self.reconciled_orders.iter().copied().collect(),
            acknowledged_reconciliation_batches: self
                .acknowledged_reconciliation_batches
                .iter()
                .map(|binding| AcknowledgedReconciliationBatchWire {
                    batch_id: *binding.batch_id().as_bytes(),
                    binding_digest: binding.binding_digest(),
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
        let maximum_archived_fills = limits
            .maximum_archived_orders
            .get()
            .checked_mul(limits.maximum_fills.get())
            .ok_or(PaperCheckpointError::InvalidHeader)?;
        let maximum_reconciled_orders = limits
            .maximum_orders
            .get()
            .checked_add(limits.maximum_archived_orders.get())
            .ok_or(PaperCheckpointError::InvalidHeader)?;
        if wire.schema_version != crate::PaperExecutionConfig::CHECKPOINT_SCHEMA_VERSION
            || wire.configuration_digest != config.digest()
            || !wire.complete
            || wire.orders.len() > limits.maximum_orders.get()
            || wire.fills.len() > limits.maximum_fills.get()
            || wire.archived_orders.len() > limits.maximum_archived_orders.get()
            || wire.archived_fills.len() > maximum_archived_fills
            || wire.idempotency.len() > limits.maximum_idempotency_keys.get()
            || wire.acknowledged_reconciliation_batches.len() > maximum_reconciled_orders
            || wire.durable_sequence > wire.sequence
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
        let mut archived_orders = BTreeMap::new();
        for order_wire in wire.archived_orders {
            let order = PaperOrder::try_from_recovery_wire(order_wire)
                .map_err(|_| PaperCheckpointError::InvalidOrder)?;
            if order.lifecycle.last_sequence() > wire.durable_sequence
                || !is_terminal(order.lifecycle.state())
                || orders.contains_key(&order.order_id)
                || archived_orders.insert(order.order_id, order).is_some()
            {
                return Err(PaperCheckpointError::InvalidOrder);
            }
        }
        let (fills, fill_totals) =
            validate_fills(wire.fills, wire.sequence, &orders, limits.fee_schedule)?;
        let (archived_fills, _) = validate_fills(
            wire.archived_fills,
            wire.durable_sequence,
            &archived_orders,
            limits.fee_schedule,
        )?;
        let ledger = PaperLedger::try_from_recovery_wire(config.ledger_config(), wire.ledger)
            .map_err(PaperCheckpointError::Ledger)?;
        validate_reservation_shape(&orders, &ledger, &fill_totals)?;
        let idempotency = validate_idempotency(wire.idempotency, &orders)?;
        validate_archive_identities(&orders, &idempotency, &archived_orders)?;
        let reconciled_orders = wire.reconciled_orders.into_iter().collect::<BTreeSet<_>>();
        if reconciled_orders.len() > maximum_reconciled_orders
            || archived_orders
                .keys()
                .any(|order_id| !reconciled_orders.contains(order_id))
            || reconciled_orders.iter().any(|order_id| {
                orders
                    .get(order_id)
                    .or_else(|| archived_orders.get(order_id))
                    .is_none_or(|order| !is_terminal(order.lifecycle.state()))
            })
        {
            return Err(PaperCheckpointError::InvalidArchive);
        }
        let mut acknowledged_reconciliation_batches = Vec::new();
        acknowledged_reconciliation_batches
            .try_reserve_exact(wire.acknowledged_reconciliation_batches.len())
            .map_err(|_| PaperCheckpointError::Allocation)?;
        for persisted in wire.acknowledged_reconciliation_batches {
            let batch_id = ReconciliationBatchId::try_from_bytes(persisted.batch_id)
                .map_err(|_| PaperCheckpointError::InvalidArchive)?;
            let binding = ReconciliationBatchBinding::try_new(batch_id, persisted.binding_digest)
                .map_err(|_| PaperCheckpointError::InvalidArchive)?;
            if acknowledged_reconciliation_batches.iter().any(
                |candidate: &ReconciliationBatchBinding| candidate.batch_id() == binding.batch_id(),
            ) {
                return Err(PaperCheckpointError::InvalidArchive);
            }
            acknowledged_reconciliation_batches.push(binding);
        }
        Ok(Self {
            schema_version: wire.schema_version,
            configuration_digest: wire.configuration_digest,
            complete: wire.complete,
            sequence: wire.sequence,
            reconciliation_required: wire.reconciliation_required,
            orders,
            fills,
            archived_orders,
            archived_fills,
            durable_sequence: wire.durable_sequence,
            reconciled_orders,
            acknowledged_reconciliation_batches,
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

#[derive(Clone, Copy, Debug, Default)]
struct FillTotals {
    quantity: i64,
    weighted_ticks: i128,
    fees: Decimal,
    maker_notional: Decimal,
    taker_notional: Decimal,
}

fn validate_fills(
    wire_fills: Vec<FillRecoveryWire>,
    checkpoint_sequence: u64,
    orders: &BTreeMap<OrderId, PaperOrder>,
    fee_schedule: crate::FeeSchedule,
) -> Result<(Vec<PaperFillSnapshot>, BTreeMap<OrderId, FillTotals>), PaperCheckpointError> {
    let mut fills = Vec::new();
    fills
        .try_reserve_exact(wire_fills.len())
        .map_err(|_| PaperCheckpointError::Allocation)?;
    let mut totals: BTreeMap<OrderId, FillTotals> = BTreeMap::new();
    let mut last_sequence = 0_u64;
    for fill in wire_fills {
        let order = orders
            .get(&fill.order_id)
            .ok_or(PaperCheckpointError::InvalidFill)?;
        let expected_notional = checked_notional(order.terms, fill.average_price, fill.quantity)
            .map_err(|_| PaperCheckpointError::InvalidFill)?;
        if fill.sequence <= last_sequence
            || fill.sequence > checkpoint_sequence
            || fill.sequence > order.lifecycle.last_sequence()
            || fill.quantity.get() == 0
            || fill.average_price.get() <= 0
            || !order.execution_price_bound.permits(fill.average_price)
            || fill.event_at < order.eligible_at
            || fill.event_at > order.expires_at
            || fill.notional.currency() != order.terms.quote_currency()
            || fill.notional.amount() <= Decimal::ZERO
            || fill.notional != expected_notional
            || fill.fee.currency() != order.terms.quote_currency()
            || fill.fee.amount().is_sign_negative()
        {
            return Err(PaperCheckpointError::InvalidFill);
        }
        let total = totals.entry(fill.order_id).or_default();
        let prior_fees = total.fees;
        total.quantity = total
            .quantity
            .checked_add(fill.quantity.get())
            .ok_or(PaperCheckpointError::InvalidFill)?;
        total.weighted_ticks = total
            .weighted_ticks
            .checked_add(
                i128::from(fill.average_price.get())
                    .checked_mul(i128::from(fill.quantity.get()))
                    .ok_or(PaperCheckpointError::InvalidFill)?,
            )
            .ok_or(PaperCheckpointError::InvalidFill)?;
        total.fees = total
            .fees
            .checked_add(fill.fee.amount())
            .ok_or(PaperCheckpointError::InvalidFill)?;
        let role_notional = match fill.liquidity {
            LiquidityRole::Maker => &mut total.maker_notional,
            LiquidityRole::Taker => &mut total.taker_notional,
        };
        *role_notional = role_notional
            .checked_add(fill.notional.amount())
            .ok_or(PaperCheckpointError::InvalidFill)?;
        let expected_cumulative_fee = fee_schedule
            .charge_cumulative(
                Money::new(total.maker_notional, order.terms.quote_currency()),
                Money::new(total.taker_notional, order.terms.quote_currency()),
            )
            .map_err(|_| PaperCheckpointError::InvalidFill)?;
        let expected_increment = expected_cumulative_fee
            .amount()
            .checked_sub(prior_fees)
            .ok_or(PaperCheckpointError::InvalidFill)?;
        if fill.fee.amount() != expected_increment {
            return Err(PaperCheckpointError::InvalidFill);
        }
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
        let total = totals.get(&order.order_id).copied().unwrap_or_default();
        if total.quantity != order.lifecycle.cumulative_filled().get()
            || total.weighted_ticks != order.weighted_fill_ticks
            || total.fees != order.cumulative_fee.amount()
        {
            return Err(PaperCheckpointError::InvalidFill);
        }
    }
    Ok((fills, totals))
}

fn validate_reservation_shape(
    orders: &BTreeMap<OrderId, PaperOrder>,
    ledger: &PaperLedger,
    fill_totals: &BTreeMap<OrderId, FillTotals>,
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
        if requires_reservation {
            let totals = fill_totals
                .get(&order.order_id)
                .copied()
                .unwrap_or_default();
            let remaining = order
                .remaining()
                .map_err(|_| PaperCheckpointError::InvalidReservation)?;
            let adverse = crate::slippage::adverse_bound(
                order.reference_price,
                order.side,
                order.maximum_slippage,
            )
            .map_err(|_| PaperCheckpointError::InvalidReservation)?;
            let reservation_price = match (order.side, order.limit_price) {
                (market_squawk_domain::OrderSide::Buy, Some(limit)) => adverse.min(limit),
                (market_squawk_domain::OrderSide::Sell, Some(limit)) => adverse.max(limit),
                (_, None) => adverse,
            };
            if !ledger.reservation_matches(
                order.order_id,
                order.account_id,
                order.terms,
                order.side,
                order.quantity,
                reservation_price,
                remaining,
                Money::new(totals.maker_notional, order.terms.quote_currency()),
                Money::new(totals.taker_notional, order.terms.quote_currency()),
                order.cumulative_fee,
            ) {
                return Err(PaperCheckpointError::InvalidReservation);
            }
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

fn validate_archive_identities(
    active_orders: &BTreeMap<OrderId, PaperOrder>,
    active_idempotency: &BTreeMap<(AccountId, ClientOrderId), OrderId>,
    archived_orders: &BTreeMap<OrderId, PaperOrder>,
) -> Result<(), PaperCheckpointError> {
    let mut identities = active_idempotency.keys().cloned().collect::<BTreeSet<_>>();
    for order in archived_orders.values() {
        if active_orders.contains_key(&order.order_id)
            || !identities.insert((order.account_id, order.client_order_id.clone()))
        {
            return Err(PaperCheckpointError::InvalidArchive);
        }
    }
    Ok(())
}

const fn is_terminal(state: PaperOrderState) -> bool {
    matches!(
        state,
        PaperOrderState::Filled
            | PaperOrderState::Canceled
            | PaperOrderState::Rejected
            | PaperOrderState::Expired
    )
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
    #[error("paper checkpoint archive state is invalid")]
    InvalidArchive,
    #[error("paper checkpoint persistence evidence does not match encoded bytes")]
    InvalidPersistenceEvidence,
    #[error("paper checkpoint bounded allocation failed")]
    Allocation,
    #[error("paper checkpoint JSON encoding or decoding failed")]
    Encoding(#[source] serde_json::Error),
    #[error("paper checkpoint ledger is invalid")]
    Ledger(#[source] crate::PaperLedgerError),
}
