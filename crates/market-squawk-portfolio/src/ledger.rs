//! Deterministic immutable portfolio revision publication.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use crate::evidence::{CorporateActionBinding, PortfolioRevision, RevisionEvidence, ValuationSet};
use crate::publication::build_revision;
use crate::transaction::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, Task10EconomicKind,
    Task10TransactionInstruction, TransactionRevision,
};
use crate::{PortfolioError, PortfolioLimits};
use market_squawk_data::CorporateActionPlan;
use market_squawk_domain::{AccountId, Currency, Money, SourceIdentifier, TransactionObservation};

/// Deterministic immutable-revision publisher for one canonical account.
#[derive(Clone, Debug)]
pub struct PortfolioLedger {
    pub(crate) account_id: AccountId,
    pub(crate) base_currency: Currency,
    pub(crate) limits: PortfolioLimits,
    pub(crate) active_entries: BTreeMap<SourceIdentifier, LedgerEntry>,
    pub(crate) seen_revisions: BTreeSet<(SourceIdentifier, u32)>,
    pub(crate) plans: Vec<CorporateActionPlan>,
    pub(crate) history: Vec<PortfolioRevision>,
}

impl PortfolioLedger {
    /// Constructs an empty bounded account ledger.
    ///
    /// # Errors
    ///
    /// Returns a typed error when account capacity is unavailable.
    pub fn try_new(
        account_id: AccountId,
        base_currency: Currency,
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if limits.max_accounts < 1 {
            return Err(PortfolioError::InvalidLimits);
        }
        Ok(Self {
            account_id,
            base_currency,
            limits,
            active_entries: BTreeMap::new(),
            seen_revisions: BTreeSet::new(),
            plans: Vec::new(),
            history: Vec::new(),
        })
    }

    /// Applies normalized entries and an optional Task 11 action plan, publishing atomically.
    ///
    /// Inputs are ordered by account, economic time, source identity, logical transaction, and
    /// revision before replay. Corrections replace only the active logical revision; all prior
    /// immutable portfolio revisions remain unchanged. The ledger mutates only after every
    /// financial, evidence, and resource invariant passes.
    ///
    /// # Errors
    ///
    /// Rejects duplicate/corrupt lineage, insufficient inventory, missing prices/FX, unresolved
    /// corporate actions, checked arithmetic failure, or exceeded work/memory limits.
    pub fn try_apply(
        &mut self,
        mut entries: Vec<LedgerEntry>,
        corporate_actions: Option<&CorporateActionPlan>,
        valuation: ValuationSet,
        evidence: RevisionEvidence,
    ) -> Result<PortfolioRevision, PortfolioError> {
        self.validate_bindings(&entries, corporate_actions, &valuation, &evidence)?;
        if entries.len() > self.limits.max_transactions {
            return Err(PortfolioError::LimitExceeded {
                resource: "transactions",
                observed: entries.len(),
                limit: self.limits.max_transactions,
            });
        }
        entries.sort_unstable_by(|left, right| {
            left.account_id
                .cmp(&right.account_id)
                .then_with(|| left.occurred_at.cmp(&right.occurred_at))
                .then_with(|| left.source.cmp(&right.source))
                .then_with(|| {
                    left.transaction
                        .transaction_id
                        .cmp(&right.transaction.transaction_id)
                })
                .then_with(|| {
                    left.transaction
                        .revision
                        .get()
                        .cmp(&right.transaction.revision.get())
                })
        });
        let mut candidate_entries = self.active_entries.clone();
        let mut candidate_seen = self.seen_revisions.clone();
        for entry in entries {
            admit_entry(
                &mut candidate_entries,
                &mut candidate_seen,
                entry,
                self.limits,
            )?;
        }
        let mut candidate_plans = self.plans.clone();
        if let Some(plan) = corporate_actions {
            let binding = CorporateActionBinding::from_plan(plan);
            if candidate_plans
                .iter()
                .any(|prior| prior.content_hash() == binding.content_identity)
            {
                return Err(PortfolioError::EvidenceMismatch);
            }
            candidate_plans.push(plan.clone());
            candidate_plans.sort_unstable_by_key(CorporateActionPlan::valuation_cutoff);
        }
        let previous_revision_id = self.history.last().map(PortfolioRevision::id);
        let revision = build_revision(
            self.account_id,
            self.base_currency,
            self.limits,
            &candidate_entries,
            &candidate_seen,
            &candidate_plans,
            previous_revision_id,
            valuation,
            evidence,
        )?;
        if self.history.len() >= self.limits.max_history {
            return Err(PortfolioError::LimitExceeded {
                resource: "revision history",
                observed: self.history.len().saturating_add(1),
                limit: self.limits.max_history,
            });
        }
        self.active_entries = candidate_entries;
        self.seen_revisions = candidate_seen;
        self.plans = candidate_plans;
        self.history.push(revision.clone());
        Ok(revision)
    }

    /// Applies normalized Task 10 transactions under explicit revision/economic instructions.
    ///
    /// Task 10 intentionally preserves source classifications that do not distinguish dividend
    /// from interest or buy from cover. Therefore every normalized broker transaction requires one
    /// matching instruction. Corporate-action markers are not interpreted economically; their
    /// economics must be supplied by the Task 11 plan passed to this method.
    ///
    /// # Errors
    ///
    /// Rejects absent/duplicate/extraneous instructions, classification conflicts, ambiguous zero
    /// amounts, missing Task 11 action plans, or any error from [`Self::try_apply`].
    pub fn try_apply_import(
        &mut self,
        transactions: &[TransactionObservation],
        instructions: &[Task10TransactionInstruction],
        corporate_actions: Option<&CorporateActionPlan>,
        valuation: ValuationSet,
        evidence: RevisionEvidence,
    ) -> Result<PortfolioRevision, PortfolioError> {
        if transactions.len() != instructions.len()
            || instructions.len() > self.limits.max_transactions
        {
            return Err(PortfolioError::AmbiguousNormalizedRecord);
        }
        let instruction_map = instructions
            .iter()
            .map(|instruction| (instruction.broker_transaction_id(), instruction))
            .collect::<BTreeMap<_, _>>();
        if instruction_map.len() != instructions.len() {
            return Err(PortfolioError::AmbiguousNormalizedRecord);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(transactions.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for transaction in transactions {
            let instruction = instruction_map
                .get(transaction.source_record_id())
                .copied()
                .ok_or(PortfolioError::AmbiguousNormalizedRecord)?;
            if let Some(entry) = translate_task10(transaction, instruction, corporate_actions)? {
                entries.push(entry);
            }
        }
        self.try_apply(entries, corporate_actions, valuation, evidence)
    }

    /// Returns immutable published revision history in publication order.
    pub fn history(&self) -> &[PortfolioRevision] {
        &self.history
    }

    fn validate_bindings(
        &self,
        entries: &[LedgerEntry],
        plan: Option<&CorporateActionPlan>,
        valuation: &ValuationSet,
        evidence: &RevisionEvidence,
    ) -> Result<(), PortfolioError> {
        if valuation.base_currency != self.base_currency
            || valuation.as_of != evidence.as_of
            || valuation.dataset != evidence.dataset
            || valuation.point_in_time_content != evidence.point_in_time_content
            || entries.iter().any(|entry| {
                entry.account_id != self.account_id || entry.occurred_at > evidence.as_of
            })
            || plan.is_some_and(|candidate| candidate.valuation_cutoff() > evidence.as_of)
        {
            return Err(PortfolioError::EvidenceMismatch);
        }
        match (plan, evidence.corporate_action) {
            (Some(plan), Some(binding)) if binding == CorporateActionBinding::from_plan(plan) => {}
            (None, None) => {}
            _ => return Err(PortfolioError::EvidenceMismatch),
        }
        Ok(())
    }
}

fn translate_task10(
    transaction: &TransactionObservation,
    instruction: &Task10TransactionInstruction,
    corporate_actions: Option<&CorporateActionPlan>,
) -> Result<Option<LedgerEntry>, PortfolioError> {
    if instruction.broker_transaction_id() != transaction.source_record_id()
        || instruction.revision() != transaction.context().time().revision()
    {
        return Err(PortfolioError::AmbiguousNormalizedRecord);
    }
    let classification = transaction.transaction_type().as_str();
    let context_instrument = transaction.context().provenance().instrument_id();
    let kind = match (classification, instruction.economic_kind()) {
        ("trade", Task10EconomicKind::Trade { trade })
            if context_instrument == Some(trade.instrument_id()) =>
        {
            LedgerEntryKind::Trade(trade.clone())
        }
        ("cash_transfer", Task10EconomicKind::CashTransfer { amount })
            if context_instrument.is_none() =>
        {
            if amount.amount().is_zero() {
                return Err(PortfolioError::AmbiguousNormalizedRecord);
            }
            let flow_kind = if amount.amount().is_sign_positive() {
                CashFlowKind::Deposit
            } else {
                CashFlowKind::Withdrawal
            };
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                flow_kind,
                Money::new(amount.amount().abs(), amount.currency()),
                None,
            )?)
        }
        (
            "income",
            Task10EconomicKind::Dividend {
                amount,
                instrument_id,
            },
        ) if context_instrument == Some(*instrument_id) && !amount.amount().is_zero() => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Dividend,
                *amount,
                Some(*instrument_id),
            )?)
        }
        (
            "income",
            Task10EconomicKind::Interest {
                amount,
                instrument_id,
            },
        ) if context_instrument == *instrument_id && !amount.amount().is_zero() => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Interest,
                *amount,
                *instrument_id,
            )?)
        }
        (
            "income",
            Task10EconomicKind::Withholding {
                amount,
                instrument_id,
            },
        ) if context_instrument == *instrument_id && !amount.amount().is_zero() => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Withholding,
                *amount,
                *instrument_id,
            )?)
        }
        (
            "fee",
            Task10EconomicKind::Fee {
                amount,
                instrument_id,
            },
        ) if context_instrument == *instrument_id && !amount.amount().is_zero() => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Fee,
                *amount,
                *instrument_id,
            )?)
        }
        ("corporate_action", Task10EconomicKind::CorporateActionMarker) => {
            if corporate_actions.is_none() {
                return Err(PortfolioError::AmbiguousNormalizedRecord);
            }
            return Ok(None);
        }
        _ => return Err(PortfolioError::AmbiguousNormalizedRecord),
    };
    let account_id = AccountId::from_str(transaction.account_id().as_str())
        .map_err(|_| PortfolioError::AmbiguousNormalizedRecord)?;
    let occurred_at = transaction
        .context()
        .time()
        .effective()
        .exact_timestamp()
        .ok_or(PortfolioError::AmbiguousNormalizedRecord)?;
    Ok(Some(LedgerEntry::try_new(
        account_id,
        TransactionRevision::try_new(
            transaction.source_record_id().clone(),
            instruction.revision(),
            instruction.supersedes(),
        )?,
        occurred_at,
        transaction
            .context()
            .provenance()
            .source_identifier()
            .clone(),
        kind,
    )?))
}

fn admit_entry(
    active: &mut BTreeMap<SourceIdentifier, LedgerEntry>,
    seen: &mut BTreeSet<(SourceIdentifier, u32)>,
    entry: LedgerEntry,
    limits: PortfolioLimits,
) -> Result<(), PortfolioError> {
    let key = (
        entry.transaction.transaction_id.clone(),
        entry.transaction.revision.get(),
    );
    if !seen.insert(key) {
        return Err(PortfolioError::DuplicateTransactionRevision);
    }
    match active.get(&entry.transaction.transaction_id) {
        Some(current) => {
            let current_revision = current.transaction.revision;
            match entry.transaction.supersedes {
                None => return Err(PortfolioError::SupersessionRequired),
                Some(prior) if prior != current_revision => {
                    return Err(PortfolioError::SupersessionMismatch);
                }
                Some(_) if entry.transaction.revision.get() <= current_revision.get() => {
                    return Err(PortfolioError::NonIncreasingRevision);
                }
                Some(_) => {}
            }
        }
        None if entry.transaction.supersedes.is_some() => {
            return Err(PortfolioError::SupersessionMismatch);
        }
        None => {}
    }
    if active.len() >= limits.max_transactions
        && !active.contains_key(&entry.transaction.transaction_id)
    {
        return Err(PortfolioError::LimitExceeded {
            resource: "logical transactions",
            observed: active.len().saturating_add(1),
            limit: limits.max_transactions,
        });
    }
    active.insert(entry.transaction.transaction_id.clone(), entry);
    Ok(())
}
