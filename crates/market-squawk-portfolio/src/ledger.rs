//! Deterministic immutable portfolio revision publication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[path = "snapshot.rs"]
pub(crate) mod snapshot;
use snapshot::LedgerSnapshot;
use tokio_util::sync::CancellationToken;

use crate::evidence::{CorporateActionBinding, PortfolioRevision, RevisionEvidence, ValuationSet};
use crate::publication::build_revision;
use crate::transaction::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, Task10EconomicKind,
    Task10TransactionInstruction, Trade, TradeSide, TransactionRevision,
};
use crate::{PortfolioError, PortfolioLimits, checked_decimal_div};
use market_squawk_data::CorporateActionPlan;
use market_squawk_domain::{
    AccountId, Currency, Money, NormalizedPortfolioLotMethod, NormalizedPortfolioTransactionClass,
    NormalizedPortfolioTransactionEvidence, SourceIdentifier,
};
use rust_decimal::Decimal;

/// Deterministic immutable-revision publisher for one canonical account.
#[derive(Clone, Debug)]
pub struct PortfolioLedger {
    pub(crate) account_id: AccountId,
    pub(crate) base_currency: Currency,
    pub(crate) limits: PortfolioLimits,
    pub(crate) snapshot: LedgerSnapshot,
    pub(crate) plan: Option<CorporateActionPlan>,
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
            snapshot: LedgerSnapshot::empty(),
            plan: None,
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
        if let Some((scratch, spill_bytes)) = self.snapshot.disk_policy() {
            return self.try_apply_stream(
                entries.into_iter().map(Ok),
                corporate_actions,
                valuation,
                evidence,
                scratch,
                spill_bytes,
                &CancellationToken::new(),
            );
        }
        let candidate_plan = next_plan(self.plan.as_ref(), corporate_actions)?;
        self.validate_bindings(&entries, candidate_plan.as_ref(), &valuation, &evidence)?;
        if entries.len() > self.limits.max_transactions {
            return Err(PortfolioError::LimitExceeded {
                resource: "transactions",
                observed: entries.len(),
                limit: self.limits.max_transactions,
            });
        }
        entries.sort_unstable_by(snapshot::compare_entries);
        let (mut active, mut seen) = self.snapshot.memory_candidate()?;
        for entry in entries {
            admit_entry(&mut active, &mut seen, entry, self.limits)?;
        }
        let candidate = LedgerSnapshot::from_memory(active, seen, self.limits)?;
        self.publish(
            candidate,
            candidate_plan,
            valuation,
            evidence,
            &CancellationToken::new(),
        )
    }

    /// Applies a fallible entry stream using private disk-backed transaction and revision indexes.
    /// The complete immutable index stays owned by the returned revision and its restored ledger.
    /// The original accounting engine and canonical revision hash are shared with `try_apply`.
    ///
    /// # Errors
    /// Rejects the same financial/evidence errors as `try_apply`, input failures, cancellation,
    /// exhausted temporary disk capacity, and corrupt or unavailable scratch storage.
    #[allow(clippy::too_many_arguments)]
    pub fn try_apply_stream(
        &mut self,
        entries: impl IntoIterator<Item = Result<LedgerEntry, PortfolioError>>,
        corporate_actions: Option<&CorporateActionPlan>,
        valuation: ValuationSet,
        evidence: RevisionEvidence,
        scratch: Arc<market_squawk_data::OperationScratchDirectory>,
        max_spill_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioRevision, PortfolioError> {
        let candidate_plan = next_plan(self.plan.as_ref(), corporate_actions)?;
        self.validate_bindings(&[], candidate_plan.as_ref(), &valuation, &evidence)?;
        let candidate = self.snapshot.apply_disk(
            entries,
            self.account_id,
            evidence.as_of,
            self.limits,
            scratch,
            max_spill_bytes,
            cancellation,
        )?;
        self.publish(candidate, candidate_plan, valuation, evidence, cancellation)
    }

    fn publish(
        &mut self,
        candidate: LedgerSnapshot,
        candidate_plan: Option<CorporateActionPlan>,
        valuation: ValuationSet,
        evidence: RevisionEvidence,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioRevision, PortfolioError> {
        let revision = build_revision(
            self.account_id,
            self.base_currency,
            self.limits,
            &candidate,
            candidate_plan.as_ref(),
            self.history.last(),
            valuation,
            evidence,
            cancellation,
        )?;
        if self.history.len() >= self.limits.max_history {
            return Err(PortfolioError::LimitExceeded {
                resource: "revision history",
                observed: self.history.len().saturating_add(1),
                limit: self.limits.max_history,
            });
        }
        snapshot::check_cancelled(cancellation)?;
        self.snapshot = candidate;
        self.plan = candidate_plan;
        self.history.push(revision.clone());
        Ok(revision)
    }

    /// Applies checked normalized Task 10 evidence under ambiguity-only caller policies.
    ///
    /// Source amount, quantity, instrument, occurrence, lot method, raw digest, and correction
    /// lineage are accepted only from the shared evidence contract. Instructions may resolve only
    /// income classification and buy/sell versus short/cover lifecycle ambiguity.
    ///
    /// # Errors
    ///
    /// Rejects absent/duplicate/extraneous instructions, classification conflicts, ambiguous zero
    /// amounts, missing Task 11 action plans, or any error from [`Self::try_apply`].
    pub fn try_apply_import(
        &mut self,
        transactions: &[NormalizedPortfolioTransactionEvidence],
        instructions: &[Task10TransactionInstruction],
        corporate_actions: Option<&CorporateActionPlan>,
        valuation: ValuationSet,
        evidence: RevisionEvidence,
    ) -> Result<PortfolioRevision, PortfolioError> {
        if transactions.len() > self.limits.max_transactions
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
        let mut used_instructions = BTreeSet::new();
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(transactions.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for transaction in transactions {
            let instruction = instruction_map
                .get(transaction.broker_transaction_id())
                .copied();
            if instruction.is_some() {
                used_instructions.insert(transaction.broker_transaction_id());
            }
            let supersedes = self.import_supersession(transaction)?;
            if let Some(entry) = translate_task10(
                transaction,
                instruction,
                supersedes,
                corporate_actions.or(self.plan.as_ref()),
            )? {
                entries.push(entry);
            }
        }
        if used_instructions.len() != instruction_map.len() {
            return Err(PortfolioError::AmbiguousNormalizedRecord);
        }
        self.try_apply(entries, corporate_actions, valuation, evidence)
    }

    fn import_supersession(
        &self,
        evidence: &NormalizedPortfolioTransactionEvidence,
    ) -> Result<Option<market_squawk_domain::RevisionNumber>, PortfolioError> {
        let Some(prior_source_revision) = evidence.supersedes_source_revision() else {
            return Ok(None);
        };
        let current = self
            .snapshot
            .get(evidence.broker_transaction_id())?
            .ok_or(PortfolioError::SupersessionMismatch)?;
        let current_evidence = current
            .normalized_evidence
            .as_ref()
            .ok_or(PortfolioError::SupersessionMismatch)?;
        if current_evidence.logical_record_id() != evidence.logical_record_id()
            || current_evidence.source_revision() != prior_source_revision
        {
            return Err(PortfolioError::SupersessionMismatch);
        }
        Ok(Some(current.transaction.revision))
    }

    /// Releases older full materializations after their immutable publication owner has retained
    /// any readers. The exact latest predecessor and complete active transaction lineage remain.
    pub fn retain_latest_materialization(&mut self) {
        if self.history.len() > 1 {
            self.history.drain(..self.history.len() - 1);
        }
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
            || plan.is_some_and(|candidate| {
                candidate.knowledge_cutoff() > evidence.knowledge_cutoff
                    || candidate.valuation_cutoff() > evidence.as_of
            })
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

fn next_plan(
    current: Option<&CorporateActionPlan>,
    supplied: Option<&CorporateActionPlan>,
) -> Result<Option<CorporateActionPlan>, PortfolioError> {
    let Some(candidate) = supplied else {
        return Ok(current.cloned());
    };
    let Some(current) = current else {
        return Ok(Some(candidate.clone()));
    };
    if candidate == current {
        return Ok(Some(current.clone()));
    }
    if candidate.policy() != current.policy()
        || candidate.knowledge_cutoff() < current.knowledge_cutoff()
        || candidate.valuation_cutoff() < current.valuation_cutoff()
        || current
            .admitted()
            .iter()
            .any(|prior| !candidate.admitted().contains(prior))
    {
        return Err(PortfolioError::EvidenceMismatch);
    }
    Ok(Some(candidate.clone()))
}

fn translate_task10(
    transaction: &NormalizedPortfolioTransactionEvidence,
    instruction: Option<&Task10TransactionInstruction>,
    supersedes: Option<market_squawk_domain::RevisionNumber>,
    corporate_actions: Option<&CorporateActionPlan>,
) -> Result<Option<LedgerEntry>, PortfolioError> {
    if instruction
        .is_some_and(|policy| policy.broker_transaction_id() != transaction.broker_transaction_id())
    {
        return Err(PortfolioError::AmbiguousNormalizedRecord);
    }
    let amount = transaction.amount();
    let magnitude = Money::new(amount.amount().abs(), amount.currency());
    let kind = match (
        transaction.classification(),
        instruction.map(|value| value.economic_kind()),
    ) {
        (
            NormalizedPortfolioTransactionClass::Trade,
            Some(Task10EconomicKind::Trade {
                side,
                lot_selection,
            }),
        ) => {
            let signed_quantity = transaction
                .quantity()
                .ok_or(PortfolioError::AmbiguousNormalizedRecord)?;
            let side_matches_sign = if signed_quantity.is_sign_positive() {
                matches!(side, TradeSide::Buy | TradeSide::BuyToCover)
            } else {
                matches!(side, TradeSide::Sell | TradeSide::SellShort)
            };
            // Opening a long/short lot does not dispose any prior inventory. Its source lot
            // method remains retained in normalized evidence, but it cannot require fabricated
            // specific-lot identifiers; only a disposal applies the selection policy.
            let method_matches = if matches!(side, TradeSide::Buy | TradeSide::SellShort) {
                true
            } else {
                match transaction.lot_method() {
                    Some(NormalizedPortfolioLotMethod::Fifo) => {
                        matches!(lot_selection, crate::LotSelection::Fifo)
                    }
                    Some(NormalizedPortfolioLotMethod::SpecificIdentification) => {
                        matches!(
                            lot_selection,
                            crate::LotSelection::SpecificIdentification(_)
                        )
                    }
                    Some(NormalizedPortfolioLotMethod::Lifo) => {
                        matches!(lot_selection, crate::LotSelection::Lifo)
                    }
                    Some(NormalizedPortfolioLotMethod::AverageCost) => {
                        matches!(lot_selection, crate::LotSelection::AverageCost)
                    }
                    None => false,
                }
            };
            if !side_matches_sign || !method_matches {
                return Err(PortfolioError::AmbiguousNormalizedRecord);
            }
            let quantity = signed_quantity.abs();
            let price = Money::new(
                checked_decimal_div(magnitude.amount(), quantity)?,
                amount.currency(),
            );
            LedgerEntryKind::Trade(Trade::try_new(
                *side,
                transaction
                    .instrument_id()
                    .ok_or(PortfolioError::AmbiguousNormalizedRecord)?,
                quantity,
                price,
                Money::new(Decimal::ZERO, amount.currency()),
                lot_selection.clone(),
            )?)
        }
        (NormalizedPortfolioTransactionClass::CashTransfer, None) => {
            if amount.amount().is_zero() {
                return Err(PortfolioError::AmbiguousNormalizedRecord);
            }
            let flow_kind = if amount.amount().is_sign_positive() {
                CashFlowKind::Deposit
            } else {
                CashFlowKind::Withdrawal
            };
            LedgerEntryKind::CashFlow(CashFlow::try_new(flow_kind, magnitude, None)?)
        }
        (NormalizedPortfolioTransactionClass::Income, Some(Task10EconomicKind::Dividend)) => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Dividend,
                magnitude,
                transaction.instrument_id(),
            )?)
        }
        (NormalizedPortfolioTransactionClass::Income, Some(Task10EconomicKind::Interest)) => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Interest,
                magnitude,
                transaction.instrument_id(),
            )?)
        }
        (NormalizedPortfolioTransactionClass::Income, Some(Task10EconomicKind::Withholding)) => {
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Withholding,
                magnitude,
                transaction.instrument_id(),
            )?)
        }
        (NormalizedPortfolioTransactionClass::Fee, None) => LedgerEntryKind::CashFlow(
            CashFlow::try_new(CashFlowKind::Fee, magnitude, transaction.instrument_id())?,
        ),
        (NormalizedPortfolioTransactionClass::CorporateAction, None) => {
            if corporate_actions.is_none() {
                return Err(PortfolioError::AmbiguousNormalizedRecord);
            }
            return Ok(None);
        }
        _ => return Err(PortfolioError::AmbiguousNormalizedRecord),
    };
    Ok(Some(LedgerEntry::from_normalized_evidence(
        TransactionRevision::try_new(
            transaction.broker_transaction_id().clone(),
            transaction.revision(),
            supersedes,
        )?,
        kind,
        transaction.clone(),
    )))
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
    validate_supersession(
        active
            .get(&entry.transaction.transaction_id)
            .map(|value| value.transaction.revision),
        &entry,
    )?;
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

fn validate_supersession(
    current: Option<market_squawk_domain::RevisionNumber>,
    entry: &LedgerEntry,
) -> Result<(), PortfolioError> {
    match current {
        Some(current) => match entry.transaction.supersedes {
            None => Err(PortfolioError::SupersessionRequired),
            Some(prior) if prior != current => Err(PortfolioError::SupersessionMismatch),
            Some(_) if entry.transaction.revision.get() <= current.get() => {
                Err(PortfolioError::NonIncreasingRevision)
            }
            Some(_) => Ok(()),
        },
        None if entry.transaction.supersedes.is_some() => Err(PortfolioError::SupersessionMismatch),
        None => Ok(()),
    }
}
