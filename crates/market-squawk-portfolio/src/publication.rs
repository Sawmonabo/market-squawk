//! Atomic replay materialization, valuation, identity, and retained-size checks.

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use market_squawk_data::{AdjustmentStep, CorporateActionPlan};
use market_squawk_domain::{
    AccountId, Currency, Money, RevisionNumber, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::accounting::{ReplayState, step_index};
use crate::evidence::{
    CashBalance, CorporateActionBinding, FeatureBinding, PortfolioRevision, PortfolioRevisionId,
    Position, RevisionEvidence, ValuationSet,
};
use crate::lots::{Lot, LotDirection, LotSelection};
use crate::transaction::{LedgerEntry, LedgerEntryKind};
use crate::{PortfolioError, PortfolioLimits, checked_decimal_add, checked_decimal_sub};

#[allow(
    clippy::too_many_arguments,
    reason = "revision publication binds every immutable candidate explicitly"
)]
pub(crate) fn build_revision(
    account_id: AccountId,
    base_currency: Currency,
    limits: PortfolioLimits,
    active_entries: &BTreeMap<SourceIdentifier, LedgerEntry>,
    seen_revisions: &BTreeSet<(SourceIdentifier, u32)>,
    plans: &[CorporateActionPlan],
    previous_revision_id: Option<PortfolioRevisionId>,
    valuation: ValuationSet,
    evidence: RevisionEvidence,
) -> Result<PortfolioRevision, PortfolioError> {
    let mut ordered = active_entries.values().cloned().collect::<Vec<_>>();
    ordered.sort_unstable_by(|left, right| {
        left.account_id
            .cmp(&right.account_id)
            .then_with(|| left.occurred_at.cmp(&right.occurred_at))
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| {
                left.transaction
                    .transaction_id
                    .cmp(&right.transaction.transaction_id)
            })
    });
    let mut state = ReplayState::default();
    let mut operations = ordered
        .iter()
        .map(ReplayOperation::Entry)
        .collect::<Vec<_>>();
    for plan in plans {
        ReplayState::validate_plan(plan)?;
        for step in plan.steps() {
            operations.push(ReplayOperation::Action { plan, step });
        }
    }
    operations.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
    for operation in operations {
        match operation {
            ReplayOperation::Entry(entry) => state.apply_entry(entry)?,
            ReplayOperation::Action { plan, step } => state.apply_step(plan, step)?,
        }
        if state.lots.len() > limits.max_lots {
            return Err(PortfolioError::LimitExceeded {
                resource: "lots",
                observed: state.lots.len(),
                limit: limits.max_lots,
            });
        }
    }
    let positions = build_positions(&state.lots, &valuation, limits)?;
    let cash = state.cash.total(&valuation)?;
    let cash_balances = state
        .cash
        .0
        .iter()
        .map(|(currency, amount)| CashBalance {
            currency: *currency,
            amount: Money::new(*amount, *currency),
        })
        .collect::<Vec<_>>();
    let zero = || Money::new(Decimal::ZERO, base_currency);
    let market_value = positions.iter().try_fold(zero(), |total, position| {
        total
            .checked_add(position.market_value)
            .map_err(|_| PortfolioError::Arithmetic)
    })?;
    let cost_basis = positions.iter().try_fold(zero(), |total, position| {
        total
            .checked_add(position.cost_basis)
            .map_err(|_| PortfolioError::Arithmetic)
    })?;
    let unrealized_gain = positions.iter().try_fold(zero(), |total, position| {
        total
            .checked_add(position.unrealized_gain)
            .map_err(|_| PortfolioError::Arithmetic)
    })?;
    let corporate_actions = plans
        .iter()
        .map(CorporateActionBinding::from_plan)
        .collect::<Vec<_>>();
    let id = revision_id(
        account_id,
        previous_revision_id,
        &ordered,
        &corporate_actions,
        &valuation,
        &evidence,
    );
    let retained_bytes = estimate_retained(
        &positions,
        &cash_balances,
        active_entries,
        seen_revisions,
        plans,
        &evidence,
    )?;
    if retained_bytes > limits.max_retained_bytes {
        return Err(PortfolioError::RetainedBytesExceeded {
            observed: retained_bytes,
            limit: limits.max_retained_bytes,
        });
    }
    Ok(PortfolioRevision {
        id,
        previous_revision_id,
        account_id,
        base_currency,
        cash,
        cash_balances,
        positions,
        market_value,
        cost_basis,
        realized_gain: state.realized_gain.total(&valuation)?,
        unrealized_gain,
        income: state.income.total(&valuation)?,
        withholding: state.withholding.total(&valuation)?,
        fees: state.fees.total(&valuation)?,
        return_of_capital: state.return_of_capital.total(&valuation)?,
        evidence,
        corporate_actions,
        retained_bytes,
        active_entries: active_entries.clone(),
        seen_revisions: seen_revisions.clone(),
        plans: plans.to_vec(),
        limits,
    })
}

enum ReplayOperation<'a> {
    Entry(&'a LedgerEntry),
    Action {
        plan: &'a CorporateActionPlan,
        step: &'a AdjustmentStep,
    },
}

impl ReplayOperation<'_> {
    fn key(&self) -> (Timestamp, &str, u8) {
        match self {
            Self::Entry(entry) => (entry.occurred_at, entry.source.as_str(), 0),
            Self::Action { plan, step } => {
                let record = plan.admitted().get(step_index(step));
                let at = record
                    .and_then(|record| {
                        record
                            .observation()
                            .context()
                            .time()
                            .effective()
                            .exact_timestamp()
                    })
                    .unwrap_or(plan.valuation_cutoff());
                let source = record.map_or("", |record| {
                    record
                        .observation()
                        .context()
                        .provenance()
                        .source_identifier()
                        .as_str()
                });
                (at, source, 1)
            }
        }
    }
}

fn build_positions(
    lots: &[Lot],
    valuation: &ValuationSet,
    limits: PortfolioLimits,
) -> Result<Vec<Position>, PortfolioError> {
    let instruments = lots
        .iter()
        .map(|lot| lot.instrument_id)
        .collect::<BTreeSet<_>>();
    if instruments.len() > limits.max_instruments {
        return Err(PortfolioError::LimitExceeded {
            resource: "instruments",
            observed: instruments.len(),
            limit: limits.max_instruments,
        });
    }
    let mut positions = Vec::new();
    positions
        .try_reserve_exact(instruments.len())
        .map_err(|_| PortfolioError::AllocationFailed)?;
    for instrument_id in instruments {
        let instrument_lots = lots
            .iter()
            .filter(|lot| lot.instrument_id == instrument_id)
            .cloned()
            .collect::<Vec<_>>();
        let quantity = instrument_lots
            .iter()
            .try_fold(Decimal::ZERO, |total, lot| match lot.direction {
                LotDirection::Long => checked_decimal_add(total, lot.quantity),
                LotDirection::Short => checked_decimal_sub(total, lot.quantity),
            })?;
        let cost_basis = instrument_lots.iter().try_fold(
            Money::new(Decimal::ZERO, valuation.base_currency),
            |total, lot| {
                total
                    .checked_add(valuation.convert(lot.basis)?)
                    .map_err(|_| PortfolioError::Arithmetic)
            },
        )?;
        let market_value = valuation.market_value(instrument_id, quantity)?;
        let unrealized_gain = instrument_lots.iter().try_fold(
            Money::new(Decimal::ZERO, valuation.base_currency),
            |total, lot| {
                let lot_market = valuation.market_value(
                    instrument_id,
                    match lot.direction {
                        LotDirection::Long => lot.quantity,
                        LotDirection::Short => -lot.quantity,
                    },
                )?;
                let lot_basis = valuation.convert(lot.basis)?;
                let gain = match lot.direction {
                    LotDirection::Long => lot_market
                        .checked_sub(lot_basis)
                        .map_err(|_| PortfolioError::Arithmetic)?,
                    LotDirection::Short => lot_basis
                        .checked_add(lot_market)
                        .map_err(|_| PortfolioError::Arithmetic)?,
                };
                total
                    .checked_add(gain)
                    .map_err(|_| PortfolioError::Arithmetic)
            },
        )?;
        positions.push(Position {
            instrument_id,
            quantity,
            cost_basis,
            market_value,
            unrealized_gain,
            basis_complete: instrument_lots.iter().all(Lot::basis_complete),
            lots: instrument_lots,
        });
    }
    Ok(positions)
}

fn revision_id(
    account_id: AccountId,
    previous: Option<PortfolioRevisionId>,
    entries: &[LedgerEntry],
    actions: &[CorporateActionBinding],
    valuation: &ValuationSet,
    evidence: &RevisionEvidence,
) -> PortfolioRevisionId {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk-portfolio-revision-v1\0");
    digest.update(account_id.as_uuid().as_bytes());
    digest.update(previous.map_or([0_u8; 32], |revision_id| revision_id.0));
    digest.update(evidence.as_of.unix_nanos().to_be_bytes());
    digest.update(evidence.dataset.dataset_id().as_str().as_bytes());
    digest.update(evidence.dataset.manifest_version().to_be_bytes());
    digest.update(evidence.dataset.schema().fingerprint());
    digest.update(evidence.dataset.content_hash().bytes());
    digest.update(evidence.point_in_time_content.bytes());
    digest.update(evidence.point_in_time_audit.bytes());
    for source in &evidence.sources {
        hash_bytes(&mut digest, source.as_str().as_bytes());
    }
    for feature in &evidence.features {
        hash_bytes(&mut digest, feature.key.name().as_bytes());
        digest.update(feature.key.version().get().to_be_bytes());
        digest.update(feature.semantic_digest.as_bytes());
    }
    for entry in entries {
        hash_bytes(
            &mut digest,
            entry.transaction.transaction_id.as_str().as_bytes(),
        );
        digest.update(entry.transaction.revision.get().to_be_bytes());
        digest.update(
            entry
                .transaction
                .supersedes
                .map_or(0_u32, RevisionNumber::get)
                .to_be_bytes(),
        );
        digest.update(entry.occurred_at.unix_nanos().to_be_bytes());
        hash_bytes(&mut digest, entry.source.as_str().as_bytes());
        match &entry.kind {
            LedgerEntryKind::Trade(trade) => {
                digest.update([trade.side as u8]);
                digest.update(trade.instrument_id.as_uuid().as_bytes());
                hash_decimal(&mut digest, trade.quantity);
                hash_money(&mut digest, trade.price);
                hash_money(&mut digest, trade.fee);
                match &trade.lot_selection {
                    LotSelection::Fifo => digest.update([0]),
                    LotSelection::SpecificIdentification(ids) => {
                        digest.update([1]);
                        for id in ids {
                            hash_bytes(&mut digest, id.as_str().as_bytes());
                        }
                    }
                }
            }
            LedgerEntryKind::CashFlow(flow) => {
                digest.update([10_u8.saturating_add(flow.kind as u8)]);
                hash_money(&mut digest, flow.amount);
                digest.update(
                    flow.instrument_id
                        .map_or([0_u8; 16], |instrument| *instrument.as_uuid().as_bytes()),
                );
            }
        }
    }
    for action in actions {
        digest.update(action.policy_version.to_be_bytes());
        digest.update(action.content_identity.bytes());
        digest.update(action.audit_identity.bytes());
        digest.update(action.knowledge_cutoff.unix_nanos().to_be_bytes());
        digest.update(action.valuation_cutoff.unix_nanos().to_be_bytes());
    }
    for price in valuation.prices.values() {
        digest.update(price.instrument_id.as_uuid().as_bytes());
        hash_money(&mut digest, price.price);
        digest.update(price.as_of.unix_nanos().to_be_bytes());
        hash_bytes(&mut digest, price.source.as_str().as_bytes());
    }
    for fx in valuation.fx_rates.values() {
        hash_bytes(&mut digest, fx.from.as_str().as_bytes());
        hash_bytes(&mut digest, fx.to.as_str().as_bytes());
        hash_decimal(&mut digest, fx.rate);
        digest.update(fx.as_of.unix_nanos().to_be_bytes());
        hash_bytes(&mut digest, fx.source.as_str().as_bytes());
    }
    PortfolioRevisionId(digest.finalize().into())
}

fn hash_money(digest: &mut Sha256, money: Money) {
    hash_decimal(digest, money.amount());
    hash_bytes(digest, money.currency().as_str().as_bytes());
}

fn hash_decimal(digest: &mut Sha256, value: Decimal) {
    hash_bytes(digest, value.normalize().to_string().as_bytes());
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update(value.len().to_be_bytes());
    digest.update(value);
}

fn estimate_retained(
    positions: &[Position],
    cash_balances: &[CashBalance],
    active_entries: &BTreeMap<SourceIdentifier, LedgerEntry>,
    seen_revisions: &BTreeSet<(SourceIdentifier, u32)>,
    plans: &[CorporateActionPlan],
    evidence: &RevisionEvidence,
) -> Result<usize, PortfolioError> {
    let lot_count = positions.iter().try_fold(0_usize, |total, position| {
        total
            .checked_add(position.lots.capacity())
            .ok_or(PortfolioError::Arithmetic)
    })?;
    let mut retained = size_of::<PortfolioRevision>();
    for bytes in [
        positions
            .len()
            .checked_mul(size_of::<Position>())
            .ok_or(PortfolioError::Arithmetic)?,
        cash_balances
            .len()
            .checked_mul(size_of::<CashBalance>())
            .ok_or(PortfolioError::Arithmetic)?,
        lot_count
            .checked_mul(size_of::<Lot>())
            .ok_or(PortfolioError::Arithmetic)?,
        active_entries
            .len()
            .checked_mul(size_of::<(SourceIdentifier, LedgerEntry)>())
            .ok_or(PortfolioError::Arithmetic)?,
        seen_revisions
            .len()
            .checked_mul(size_of::<(SourceIdentifier, u32)>())
            .ok_or(PortfolioError::Arithmetic)?,
        evidence
            .sources
            .capacity()
            .checked_mul(size_of::<SourceIdentifier>())
            .ok_or(PortfolioError::Arithmetic)?,
        evidence
            .features
            .capacity()
            .checked_mul(size_of::<FeatureBinding>())
            .ok_or(PortfolioError::Arithmetic)?,
    ] {
        retained = retained
            .checked_add(bytes)
            .ok_or(PortfolioError::Arithmetic)?;
    }
    for plan in plans {
        retained = retained
            .checked_add(plan.retained_bytes())
            .ok_or(PortfolioError::Arithmetic)?;
    }
    for id in active_entries.keys() {
        retained = retained
            .checked_add(id.retained_bytes())
            .ok_or(PortfolioError::Arithmetic)?;
    }
    Ok(retained)
}
