//! Dispatcher-owned authoritative account-replacement preparation.

use market_squawk_domain::{AccountId, Currency, OrderId, QuantityLots, Timestamp};
use sha2::{Digest, Sha256};

use crate::account::{
    AccountReplacementCandidate, AccountReplacementReservationBinding, AccountStateReplacementBatch,
};
use crate::{
    ExecutionDispatchError, ExecutionPriceBound, ExecutionState, OrderIntentDigest,
    ReconciledOrder, ReconciledOrderStatus,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct ReconciliationRecordBinding {
    pub(super) account_id: AccountId,
    pub(super) order_id: OrderId,
    pub(super) intent_digest: OrderIntentDigest,
    pub(super) account_revision: u64,
    pub(super) requested_quantity: QuantityLots,
    pub(super) execution_price_bound: ExecutionPriceBound,
    pub(super) settlement_currency: Option<Currency>,
    pub(super) previous: Option<ReconciledOrder>,
    pub(super) was_reconciliation: bool,
    pub(super) recovered: bool,
}

#[derive(Debug)]
pub(super) struct PreparedAccountReplacement {
    batch: AccountStateReplacementBatch,
    affected_accounts: Box<[AccountId]>,
}

impl PreparedAccountReplacement {
    pub(super) fn into_parts(self) -> (AccountStateReplacementBatch, Box<[AccountId]>) {
        (self.batch, self.affected_accounts)
    }
}

pub(super) fn prepare_account_replacement(
    state: &ExecutionState,
    records: &[ReconciliationRecordBinding],
    invoked_at: Timestamp,
) -> Result<Option<PreparedAccountReplacement>, ExecutionDispatchError> {
    let recovered = records.iter().filter(|record| record.recovered).count();
    if recovered != 0 {
        if recovered != records.len() || state.reconciliation_required() {
            return Err(ExecutionDispatchError::AccountReplacementRejected);
        }
        for record in records {
            let observed = observed_order(state, record.order_id)
                .ok_or(ExecutionDispatchError::AccountReplacementRejected)?;
            validate_order(record, observed)?;
        }
        return Ok(None);
    }
    let mut affected_accounts = Vec::new();
    affected_accounts
        .try_reserve_exact(records.len())
        .map_err(|_| ExecutionDispatchError::Allocation)?;
    for record in records {
        let Some(observed) = observed_order(state, record.order_id) else {
            continue;
        };
        validate_order(record, observed)?;
        if record.was_reconciliation || has_financial_effect(observed) {
            affected_accounts.push(record.account_id);
        }
    }
    affected_accounts.sort_unstable();
    affected_accounts.dedup();

    let has_replacement_evidence = !state.accounts().is_empty() || state.source_binding().is_some();
    if !has_replacement_evidence {
        return Ok(None);
    }
    if state.reconciliation_required() || state.source_binding().is_none() {
        return Err(ExecutionDispatchError::AccountReplacementRejected);
    }

    if affected_accounts.is_empty() {
        return if state.accounts().is_empty() {
            Ok(None)
        } else {
            Err(ExecutionDispatchError::AccountReplacementRejected)
        };
    }
    if state.accounts().is_empty() {
        return Err(ExecutionDispatchError::AccountReplacementRejected);
    }

    let mut returned_accounts = Vec::new();
    returned_accounts
        .try_reserve_exact(state.accounts().len())
        .map_err(|_| ExecutionDispatchError::Allocation)?;
    returned_accounts.extend(state.accounts().iter().map(|account| account.account_id()));
    returned_accounts.sort_unstable();
    if returned_accounts != affected_accounts {
        return Err(ExecutionDispatchError::AccountReplacementRejected);
    }
    let mut account_closure_is_terminal = true;
    for record in records
        .iter()
        .filter(|record| affected_accounts.binary_search(&record.account_id).is_ok())
    {
        let observed = observed_order(state, record.order_id)
            .ok_or(ExecutionDispatchError::AccountReplacementRejected)?;
        validate_order(record, observed)?;
        account_closure_is_terminal &= is_terminal(observed.status());
    }
    if !account_closure_is_terminal {
        return Ok(None);
    }
    let source = state
        .source_binding()
        .ok_or(ExecutionDispatchError::AccountReplacementRejected)?;

    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(affected_accounts.len())
        .map_err(|_| ExecutionDispatchError::Allocation)?;
    for account_id in &affected_accounts {
        let state_account = state
            .accounts()
            .iter()
            .find(|candidate| candidate.account_id() == *account_id)
            .cloned()
            .ok_or(ExecutionDispatchError::AccountReplacementRejected)?;
        let mut reservations = Vec::new();
        let expected_revision = records
            .iter()
            .find(|record| record.account_id == *account_id)
            .map(|record| record.account_revision)
            .ok_or(ExecutionDispatchError::AccountReplacementRejected)?;
        for record in records
            .iter()
            .filter(|record| record.account_id == *account_id)
        {
            if record.account_revision != expected_revision {
                return Err(ExecutionDispatchError::AccountReplacementRejected);
            }
            reservations
                .try_reserve(1)
                .map_err(|_| ExecutionDispatchError::Allocation)?;
            reservations.push(AccountReplacementReservationBinding::new(
                record.order_id,
                record.intent_digest,
                record.account_revision,
            ));
        }
        candidates.push(
            AccountReplacementCandidate::try_new(state_account, expected_revision, reservations)
                .map_err(|_| ExecutionDispatchError::AccountReplacementRejected)?,
        );
    }

    let invocation_digest = reconciliation_digest(state, records, invoked_at);
    let batch = AccountStateReplacementBatch::try_new(source, invocation_digest, candidates)
        .map_err(|_| ExecutionDispatchError::AccountReplacementRejected)?;
    Ok(Some(PreparedAccountReplacement {
        batch,
        affected_accounts: affected_accounts.into_boxed_slice(),
    }))
}

fn observed_order(state: &ExecutionState, order_id: OrderId) -> Option<ReconciledOrder> {
    state
        .orders()
        .iter()
        .copied()
        .find(|order| order.order_id() == order_id)
}

fn validate_order(
    record: &ReconciliationRecordBinding,
    observed: ReconciledOrder,
) -> Result<(), ExecutionDispatchError> {
    let cumulative_regression = record.previous.is_some_and(|previous| {
        observed.cumulative_filled().get() < previous.cumulative_filled().get()
            || observed.cumulative_fees().currency() != previous.cumulative_fees().currency()
            || observed.cumulative_fees().amount() < previous.cumulative_fees().amount()
            || observed.maximum_fill_price() < previous.maximum_fill_price()
    });
    let filled = observed.cumulative_filled().get();
    let requested = record.requested_quantity.get();
    let invalid = cumulative_regression
        || filled < 0
        || filled > requested
        || record.settlement_currency != Some(observed.cumulative_fees().currency())
        || observed
            .maximum_fill_price()
            .is_some_and(|price| !record.execution_price_bound.permits(price))
        || matches!(observed.status(), ReconciledOrderStatus::Filled) && filled != requested
        || matches!(observed.status(), ReconciledOrderStatus::PartiallyFilled)
            && (filled <= 0 || filled >= requested)
        || matches!(observed.status(), ReconciledOrderStatus::Unknown);
    if invalid {
        return Err(ExecutionDispatchError::AccountReplacementRejected);
    }
    Ok(())
}

fn has_financial_effect(order: ReconciledOrder) -> bool {
    order.cumulative_filled().get() != 0 || !order.cumulative_fees().amount().is_zero()
}

const fn is_terminal(status: ReconciledOrderStatus) -> bool {
    matches!(
        status,
        ReconciledOrderStatus::Filled
            | ReconciledOrderStatus::Canceled
            | ReconciledOrderStatus::Rejected
            | ReconciledOrderStatus::Expired
    )
}

pub(super) fn reconciliation_digest(
    state: &ExecutionState,
    records: &[ReconciliationRecordBinding],
    invoked_at: Timestamp,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/dispatcher-account-reconciliation/v3\0");
    digest.update(invoked_at.unix_nanos().to_be_bytes());
    if let Some(source) = state.source_binding() {
        digest.update([1]);
        digest.update(source.schema_version().to_be_bytes());
        digest.update(source.configuration_digest());
        digest.update(source.snapshot_sequence().get().to_be_bytes());
        digest.update(source.snapshot_digest());
    } else {
        digest.update([0]);
    }
    digest.update((records.len() as u64).to_be_bytes());
    for record in records {
        digest.update(record.account_id.as_uuid().as_bytes());
        digest.update(record.order_id.as_uuid().as_bytes());
        digest.update(record.intent_digest.as_bytes());
        digest.update(record.account_revision.to_be_bytes());
        digest.update(record.requested_quantity.get().to_be_bytes());
        digest.update(
            record
                .execution_price_bound
                .maximum_price()
                .get()
                .to_be_bytes(),
        );
        match record.settlement_currency {
            Some(currency) => {
                digest.update([1]);
                digest.update(currency.as_str().as_bytes());
            }
            None => digest.update([0]),
        }
        digest.update([u8::from(record.was_reconciliation)]);
        match record.previous {
            Some(previous) => {
                digest.update([1]);
                digest_order(&mut digest, previous);
            }
            None => digest.update([0]),
        }
        if let Some(order) = observed_order(state, record.order_id) {
            digest.update([1]);
            digest_order(&mut digest, order);
        } else {
            digest.update([0]);
        }
    }
    digest.update((state.accounts().len() as u64).to_be_bytes());
    for account in state.accounts() {
        digest.update(account.account_id().as_uuid().as_bytes());
        digest.update(account.revision().get().to_be_bytes());
        digest.update([u8::from(account.eligible())]);
        digest_money(&mut digest, account.cash());
        digest_money(&mut digest, account.settled_capital());
        digest_money(&mut digest, account.marked_equity());
        digest_money(&mut digest, account.peak_marked_equity());
        digest_money(&mut digest, account.marked_gross_exposure());
        digest_money(&mut digest, account.unrealized_pnl());
        digest_money(&mut digest, account.drawdown());
        digest.update(account.mark_digest());
        digest_money(&mut digest, account.realized_pnl());
        digest_money(&mut digest, account.realized_loss());
        digest.update((account.positions().len() as u64).to_be_bytes());
        for (instrument_id, lots) in account.positions() {
            digest.update(instrument_id.as_uuid().as_bytes());
            digest.update(lots.to_be_bytes());
        }
        digest.update((account.position_cost_basis().len() as u64).to_be_bytes());
        for (instrument_id, basis) in account.position_cost_basis() {
            digest.update(instrument_id.as_uuid().as_bytes());
            digest_money(&mut digest, *basis);
        }
    }
    digest.finalize().into()
}

fn digest_order(digest: &mut Sha256, order: ReconciledOrder) {
    digest.update(order.order_id().as_uuid().as_bytes());
    digest.update([match order.status() {
        ReconciledOrderStatus::Open => 0,
        ReconciledOrderStatus::PartiallyFilled => 1,
        ReconciledOrderStatus::Filled => 2,
        ReconciledOrderStatus::Canceled => 3,
        ReconciledOrderStatus::Rejected => 4,
        ReconciledOrderStatus::Expired => 5,
        ReconciledOrderStatus::Unknown => 6,
    }]);
    digest.update(order.cumulative_filled().get().to_be_bytes());
    match order.average_fill_price() {
        Some(price) => {
            digest.update([1]);
            digest.update(price.get().to_be_bytes());
        }
        None => digest.update([0]),
    }
    match order.maximum_fill_price() {
        Some(price) => {
            digest.update([1]);
            digest.update(price.get().to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest_money(digest, order.cumulative_fees());
}

fn digest_money(digest: &mut Sha256, money: market_squawk_domain::Money) {
    digest.update(money.currency().as_str().as_bytes());
    digest.update(money.amount().mantissa().to_be_bytes());
    digest.update(money.amount().scale().to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use market_squawk_domain::{AccountId, Currency, Money, OrderId, PriceTicks, QuantityLots};
    use rust_decimal::Decimal;

    use super::{ReconciliationRecordBinding, validate_order};
    use crate::{
        ExecutionDispatchError, ExecutionPriceBound, OrderIntentDigest, ReconciledOrder,
        ReconciledOrderStatus,
    };

    #[test]
    fn maximum_individual_fill_above_approved_ceiling_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let usd = Currency::try_from("USD")?;
        let record = ReconciliationRecordBinding {
            account_id: AccountId::from_str("50000000-0000-0000-0000-000000000001")?,
            order_id: OrderId::from_str("20000000-0000-0000-0000-000000000001")?,
            intent_digest: OrderIntentDigest::from_bytes([7; 32]),
            account_revision: 1,
            requested_quantity: QuantityLots::new(2)?,
            execution_price_bound: ExecutionPriceBound::try_new(PriceTicks::new(10_000))?,
            settlement_currency: Some(usd),
            previous: None,
            was_reconciliation: true,
            recovered: false,
        };
        let observed = ReconciledOrder::try_new(
            record.order_id,
            ReconciledOrderStatus::Filled,
            record.requested_quantity,
            Some(PriceTicks::new(9_900)),
            Some(PriceTicks::new(10_001)),
            Money::new(Decimal::ZERO, usd),
        )?;

        assert_eq!(
            validate_order(&record, observed),
            Err(ExecutionDispatchError::AccountReplacementRejected)
        );
        Ok(())
    }
}
