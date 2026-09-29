//! One retained publisher, fed only by actual paper worker execution evidence.
use crate::application::{
    SourceAppliedCorporateActionPlanReference, SourceAppliedCorporateActionReadCapability,
};
use market_squawk_adapter_paper::{
    PaperControlContext, PaperExecutionAdapter, PaperExecutionConfig, PaperExecutionSnapshot,
    PaperPortfolioReplay,
};
use market_squawk_data::{
    CorporateActionPlan, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    AccountId, Money, OrderSide, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_execution::{
    PortfolioReadCapability, PortfolioReadLimits, PortfolioServicePublisher,
    portfolio_execution_state,
};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, CorporateActionBinding, LedgerEntry, LedgerEntryKind, LotSelection,
    PortfolioLedger, PortfolioLimits, PortfolioService, PortfolioServiceLimitInput,
    PortfolioServiceLimits, PriceEvidence, RevisionEvidence, Trade, TradeSide, TransactionRevision,
    ValuationSet,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct PaperPortfolioPublication {
    publisher: PortfolioServicePublisher,
    ledger: PortfolioLedger,
    account: AccountId,
    limits: PortfolioLimits,
    maximum_instruments: NonZeroUsize,
    retained_bytes: NonZeroUsize,
    initialized: bool,
    last_sequence: Option<u64>,
    canonical: Option<(
        crate::portfolio_application::PaperPortfolioPublishCapability,
        PaperExecutionConfig,
    )>,
    pub(super) sources: Option<Arc<SourceAppliedCorporateActionReadCapability>>,
}
impl std::fmt::Debug for PaperPortfolioPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperPortfolioPublication")
            .field("account", &self.account)
            .field("last_sequence", &self.last_sequence)
            .finish_non_exhaustive()
    }
}
impl PaperPortfolioPublication {
    pub(super) fn new(
        publisher: PortfolioServicePublisher,
        ledger: PortfolioLedger,
        account: AccountId,
        limits: PortfolioLimits,
        maximum_instruments: NonZeroUsize,
        retained_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            publisher,
            ledger,
            account,
            limits,
            maximum_instruments,
            retained_bytes,
            initialized: false,
            last_sequence: None,
            canonical: None,
            sources: None,
        }
    }
    pub(super) fn bind_canonical(
        &mut self,
        publisher: crate::portfolio_application::PaperPortfolioPublishCapability,
        configuration: PaperExecutionConfig,
    ) -> anyhow::Result<PortfolioReadCapability> {
        if self.canonical.is_some() || self.initialized {
            anyhow::bail!("paper canonical publication already bound");
        }
        let current = publisher.current_revision(self.account)?;
        let service = PortfolioService::try_new(
            current.into_iter().collect(),
            Vec::new(),
            PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
                max_accounts: NonZeroUsize::MIN,
                max_history_per_account: NonZeroUsize::new(2)
                    .ok_or_else(|| anyhow::anyhow!("paper history bound"))?,
                max_results: self.maximum_instruments,
                max_retained_bytes: self.retained_bytes,
            })?,
        )?;
        let (execution_publisher, reader) = portfolio_execution_state(
            service,
            PortfolioReadLimits::new(
                self.maximum_instruments,
                self.retained_bytes,
                NonZeroUsize::new(4096)
                    .ok_or_else(|| anyhow::anyhow!("paper retired history bound"))?,
            ),
        )?;
        self.publisher = execution_publisher;
        self.canonical = Some((publisher, configuration));
        Ok(reader)
    }
    pub(super) fn bind_financial_fence(
        &mut self,
        fence: market_squawk_execution::AccountRiskReconciliationFence,
    ) -> anyhow::Result<()> {
        self.publisher.bind_financial_fence(fence)?;
        Ok(())
    }
    pub(super) async fn reconcile(
        &mut self,
        adapter: &PaperExecutionAdapter,
        cancellation: CancellationToken,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| anyhow::anyhow!("paper publication deadline overflow"))?;
        let replay = if let Some((_, configuration)) = &self.canonical {
            let checkpoint = adapter
                .portfolio_checkpoint(PaperControlContext::try_new_before(
                    deadline.into(),
                    cancellation.clone(),
                )?)
                .await?;
            Some(PaperPortfolioReplay::capture(
                &checkpoint,
                configuration,
                8 * 1024 * 1024,
            )?)
        } else {
            None
        };
        let snapshot = match &replay {
            Some(replay) => replay.snapshot().clone(),
            None => {
                adapter
                    .snapshot(PaperControlContext::try_new_before(
                        deadline.into(),
                        cancellation.clone(),
                    )?)
                    .await?
            }
        };
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        validate_paper_valuation_clock(&snapshot, self.account, Timestamp::from_unix_nanos(nanos))?;
        if self.last_sequence == Some(snapshot.sequence()) {
            return Ok(());
        }
        let plan = if let Some(bytes) = snapshot.action_source_reference() {
            if bytes.len() > 64 * 1024 {
                anyhow::bail!("paper action source recipe exceeds bound");
            }
            let reference: SourceAppliedCorporateActionPlanReference =
                serde_json::from_slice(bytes)?;
            let sources = self
                .sources
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("paper action source reader unavailable"))?;
            let source = sources
                .read_reference(&reference, deadline, cancellation.clone())
                .await
                .map_err(|error| anyhow::anyhow!("paper action source reopen failed: {error:?}"))?
                .ok_or_else(|| anyhow::anyhow!("paper action source no longer available"))?;
            let plan = source
                .into_covered_accounting_plan()
                .map_err(|error| anyhow::anyhow!("paper action source coverage: {error:?}"))?;
            adapter
                .reopen_corporate_actions(
                    plan.clone(),
                    bytes.to_vec(),
                    PaperControlContext::try_new_before(deadline.into(), cancellation.clone())?,
                )
                .await?;
            Some(plan)
        } else {
            None
        };
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            anyhow::bail!("paper publication cancelled or expired");
        }
        if let (Some((publisher, _)), Some(replay)) = (&self.canonical, replay) {
            let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
            let revision = publisher
                .publish(
                    replay,
                    self.account,
                    plan,
                    Timestamp::from_unix_nanos(nanos),
                    deadline,
                    cancellation,
                )
                .await?;
            self.publish_revision(snapshot, revision)
        } else {
            self.publish(snapshot, plan.as_ref())
        }
    }
    fn publish(
        &mut self,
        snapshot: PaperExecutionSnapshot,
        plan: Option<&CorporateActionPlan>,
    ) -> anyhow::Result<()> {
        let nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
        let as_of = Timestamp::from_unix_nanos(nanos);
        let prior = self.ledger.history().last();
        let revision = build_paper_revision(
            &snapshot,
            self.account,
            plan,
            prior,
            self.last_sequence,
            as_of,
            self.limits,
            [0; 32],
        )?;
        self.publish_revision(snapshot, revision)
    }
    fn publish_revision(
        &mut self,
        snapshot: PaperExecutionSnapshot,
        revision: market_squawk_portfolio::PortfolioRevision,
    ) -> anyhow::Result<()> {
        let candidate = revision.clone().into_ledger()?;
        let service = PortfolioService::try_new(
            vec![revision],
            Vec::new(),
            PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
                max_accounts: NonZeroUsize::MIN,
                max_history_per_account: NonZeroUsize::new(2)
                    .ok_or_else(|| anyhow::anyhow!("paper history bound"))?,
                max_results: self.maximum_instruments,
                max_retained_bytes: self.retained_bytes,
            })?,
        )?;
        self.publisher
            .publish_reconciled(service, snapshot.sequence())?;
        self.ledger = candidate;
        self.initialized = true;
        self.last_sequence = Some(snapshot.sequence());
        Ok(())
    }
}

/// Replays one original native state through the same ledger calculation used for live publication.
/// The supplied clock is the retained publication clock, never the restart clock.
pub(crate) fn build_paper_revision(
    snapshot: &PaperExecutionSnapshot,
    account: AccountId,
    plan: Option<&CorporateActionPlan>,
    prior: Option<&market_squawk_portfolio::PortfolioRevision>,
    previous_sequence: Option<u64>,
    as_of: Timestamp,
    limits: PortfolioLimits,
    original_checkpoint_digest: [u8; 32],
) -> anyhow::Result<market_squawk_portfolio::PortfolioRevision> {
    let currency = snapshot
        .cash()
        .iter()
        .find(|cash| cash.account_id() == account)
        .ok_or_else(|| anyhow::anyhow!("paper native cash missing"))?
        .balance()
        .currency();
    if !snapshot.complete() || snapshot.simulation().allow_short() {
        anyhow::bail!("paper long-account publication requires complete original state");
    }
    let source = SourceIdentifier::try_from("paper-worker-original-execution-evidence")?;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/paper-portfolio-worker-state/v1\0");
    hash.update(snapshot.configuration_digest());
    hash.update(original_checkpoint_digest);
    hash.update(snapshot.sequence().to_be_bytes());
    hash.update(as_of.unix_nanos().to_be_bytes());
    for fill in snapshot.fills() {
        hash.update(fill.sequence().to_be_bytes());
        hash.update(fill.event_at().unix_nanos().to_be_bytes());
        hash.update(fill.order_id().as_uuid().as_bytes());
        hash.update(fill.notional().amount().normalize().to_string());
        hash.update(fill.fee().amount().normalize().to_string());
    }
    for mark in snapshot.executable_marks() {
        hash.update(mark.evidence_digest());
    }
    if let Some(plan) = plan {
        hash.update(plan.audit_hash().bytes());
    }
    let identity = Sha256Digest::new(hash.finalize().into());
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("paper-worker-financial-state")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        identity,
    )?;
    let mut entries = Vec::new();
    entries.try_reserve_exact(
        snapshot
            .fills()
            .len()
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("paper fill count overflow"))?,
    )?;
    if previous_sequence.is_none() {
        let (opened_at, accounts) = snapshot
            .original_accounts()
            .ok_or_else(|| anyhow::anyhow!("paper original bootstrap unavailable"))?;
        let original = accounts
            .iter()
            .find(|original| original.account_id == account)
            .ok_or_else(|| anyhow::anyhow!("paper original account mismatch"))?;
        if !original.positions.is_empty() || original.cash.len() != 1 {
            anyhow::bail!("paper bootstrap requires original position evidence");
        }
        let cash = original.cash[0];
        if cash.currency() != currency {
            anyhow::bail!("paper original currency differs");
        }
        entries.push(LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(
                SourceIdentifier::try_from("paper-sandbox-initial-cash-deposit")?,
                RevisionNumber::new(if prior.is_some() { 2 } else { 1 })?,
                if prior.is_some() {
                    Some(RevisionNumber::new(1)?)
                } else {
                    None
                },
            )?,
            opened_at,
            source.clone(),
            LedgerEntryKind::CashFlow(CashFlow::try_new(CashFlowKind::Deposit, cash, None)?),
        )?);
    }
    for fill in snapshot.fills() {
        if previous_sequence.is_some_and(|sequence| fill.sequence() <= sequence) {
            continue;
        }
        let order = snapshot
            .orders()
            .iter()
            .find(|order| order.order_id() == fill.order_id())
            .ok_or_else(|| anyhow::anyhow!("paper original fill order unavailable"))?;
        if order.account_id() != account {
            continue;
        }
        let terms = order.execution_terms();
        if terms.contract_multiplier() != Decimal::ONE {
            anyhow::bail!("paper instrument requires explicit derivative portfolio basis");
        }
        let units = Decimal::from(fill.quantity().get())
            .checked_mul(terms.lot_size().as_decimal())
            .ok_or_else(|| anyhow::anyhow!("paper unit arithmetic overflow"))?;
        let price = terms
            .price_tick()
            .as_decimal()
            .checked_mul(Decimal::from(fill.average_price().get()))
            .ok_or_else(|| anyhow::anyhow!("paper price arithmetic overflow"))?;
        entries.push(LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(
                SourceIdentifier::try_from(format!("paper-fill-{:020}", fill.sequence()).as_str())?,
                RevisionNumber::new(1)?,
                None,
            )?,
            fill.event_at(),
            source.clone(),
            LedgerEntryKind::Trade(Trade::try_from_execution(
                if order.side() == OrderSide::Buy {
                    TradeSide::Buy
                } else {
                    TradeSide::Sell
                },
                terms.instrument_id(),
                units,
                Money::new(price, terms.quote_currency()),
                fill.fee(),
                LotSelection::AverageCost,
                fill.notional(),
            )?),
        )?);
    }
    validate_paper_valuation_clock(snapshot, account, as_of)?;
    let mut prices = Vec::new();
    for position in snapshot
        .positions()
        .iter()
        .filter(|position| position.account_id() == account)
    {
        if position.lots() <= 0 {
            anyhow::bail!("paper position direction is unsupported by long publication");
        }
        let mark = snapshot
            .executable_marks()
            .iter()
            .find(|mark| mark.execution_terms().instrument_id() == position.instrument_id())
            .ok_or_else(|| anyhow::anyhow!("paper executable mark missing"))?;
        let price = mark
            .execution_terms()
            .price_tick()
            .as_decimal()
            .checked_mul(Decimal::from(mark.best_bid().get()))
            .ok_or_else(|| anyhow::anyhow!("paper mark arithmetic overflow"))?;
        prices.push(PriceEvidence::try_new(
            position.instrument_id(),
            Money::new(price, mark.execution_terms().quote_currency()),
            mark.observed_at(),
            source.clone(),
        )?);
    }
    let mut candidate = match prior {
        Some(revision) => revision.clone().into_ledger()?,
        None => PortfolioLedger::try_new(account, currency, limits)?,
    };
    candidate.retain_latest_materialization();
    let revision = candidate.try_apply(
        entries,
        plan,
        ValuationSet::try_new_with_observation_times(
            currency,
            as_of,
            dataset.clone(),
            identity,
            prices,
            Vec::new(),
            limits,
        )?,
        RevisionEvidence::try_new(
            as_of,
            dataset,
            identity,
            identity,
            vec![source],
            Vec::new(),
            plan.map(CorporateActionBinding::from_plan),
        )?,
    )?;
    let cash = snapshot
        .cash()
        .iter()
        .find(|cash| cash.account_id() == account && cash.balance().currency() == currency)
        .ok_or_else(|| anyhow::anyhow!("paper native cash missing"))?;
    if revision.cash() != cash.balance() {
        anyhow::bail!("paper portfolio cash fails native ledger reconciliation");
    }
    let native_positions = snapshot
        .positions()
        .iter()
        .filter(|position| position.account_id() == account)
        .collect::<Vec<_>>();
    if native_positions.len() != revision.positions().len() {
        anyhow::bail!("paper portfolio positions differ");
    }
    for native in native_positions {
        let mark = snapshot
            .executable_marks()
            .iter()
            .find(|mark| mark.execution_terms().instrument_id() == native.instrument_id())
            .ok_or_else(|| anyhow::anyhow!("paper native mark missing"))?;
        let units = Decimal::from(native.lots())
            .checked_mul(mark.execution_terms().lot_size().as_decimal())
            .ok_or_else(|| anyhow::anyhow!("paper native units overflow"))?;
        if revision
            .position(native.instrument_id())
            .is_none_or(|position| position.quantity() != units)
        {
            anyhow::bail!("paper portfolio inventory differs");
        }
    }
    let native_claims = snapshot
        .cash_entitlements()
        .iter()
        .filter(|claim| claim.account_id() == account && !claim.amount().amount().is_zero())
        .collect::<Vec<_>>();
    // A source action on zero inventory creates no payable claim in the worker. The
    // portfolio keeps that exact zero as action audit evidence; it is not source absence.
    if native_claims.len()
        != revision
            .cash_entitlements()
            .iter()
            .filter(|claim| !claim.amount().amount().is_zero())
            .count()
        || native_claims.iter().any(|claim| {
            !revision.cash_entitlements().iter().any(|other| {
                other.action_evidence().bytes() == claim.evidence()
                    && other.amount() == claim.amount()
                    && other.settled() == claim.settled()
                    && other.entitled_at() == claim.entitled_at()
                    && other.simulated_settlement_at() == claim.simulated_settlement_at()
            })
        })
    {
        anyhow::bail!("paper entitlement reconciliation differs");
    }
    let native_account = snapshot
        .accounts()
        .iter()
        .find(|native| native.account_id() == account)
        .ok_or_else(|| anyhow::anyhow!("paper native account missing"))?;
    if revision.marked_equity() != native_account.marked_equity() {
        anyhow::bail!("paper marked equity fails native ledger reconciliation");
    }
    Ok(revision)
}

fn validate_paper_valuation_clock(
    snapshot: &PaperExecutionSnapshot,
    account: AccountId,
    as_of: Timestamp,
) -> anyhow::Result<()> {
    for position in snapshot
        .positions()
        .iter()
        .filter(|position| position.account_id() == account)
    {
        let mark = snapshot
            .executable_marks()
            .iter()
            .find(|mark| mark.execution_terms().instrument_id() == position.instrument_id())
            .ok_or_else(|| anyhow::anyhow!("paper executable mark missing"))?;
        let age = i128::from(as_of.unix_nanos()) - i128::from(mark.observed_at().unix_nanos());
        if age < 0 || age >= i128::from(snapshot.simulation().maximum_mark_age_nanos()) {
            anyhow::bail!("paper executable mark stale");
        }
    }
    Ok(())
}
