//! Exact economic-date ordinary fields joined to the existing all-family source snapshot.
//!
//! The guarantee is source scoped: every Tiingo native daily cash/split field in the requested
//! interval, plus every returned applicable Alpaca lifecycle disposition in the retained query.
//! It does not turn Alpaca process-date coverage into historical economic-date coverage, assert
//! that an unobserved reorganization cannot exist, or provide a currency for an untyped amount.

use super::*;
use crate::application::research::ingest::{
    TiingoCompletedEodActionRead, TiingoCompletedEodHistoryReference,
};
use crate::application::research::market_history::NativeSessionHistory;
use market_squawk_domain::DigestAlgorithm;
use sha2::{Digest as _, Sha256};

pub(crate) use market_squawk_data::{
    OrdinaryActionCoverageGap, OrdinaryActionField, ReconciledOrdinaryAction,
};

/// Private construction: authority comes only from actual complete-history/native replay reads.
/// This is part of SourceAppliedCorporateActionPlan, not a second selector or registry.
pub(crate) struct OrdinaryActionCoverage {
    reads: Vec<(TiingoCompletedEodActionRead, SourcePlanCalendar)>,
    gaps: Box<[OrdinaryActionCoverageGap]>,
    reconciled: Box<[ReconciledOrdinaryAction]>,
    digest: EvidenceDigest,
    timestamped: Vec<super::anchor::AlpacaHistoryReference>,
}
impl OrdinaryActionCoverage {
    pub(crate) fn reads(&self) -> &[(TiingoCompletedEodActionRead, SourcePlanCalendar)] {
        &self.reads
    }
    pub(crate) fn gaps(&self) -> &[OrdinaryActionCoverageGap] {
        &self.gaps
    }
    pub(crate) fn reconciled(&self) -> &[ReconciledOrdinaryAction] {
        &self.reconciled
    }
    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OrdinaryHistoryReference {
    history: TiingoCompletedEodHistoryReference,
    calendar: crate::application::market_calendar::CompletedMarketSessionReference,
    pub(super) timestamped: Option<super::anchor::AlpacaHistoryReference>,
}

impl SourceAppliedCorporateActionPlan {
    /// Adds real economic-date coverage to the existing finite-query plan. Every instrument must
    /// have exactly one complete native history and its exact listing calendar at the SAME actual
    /// knowledge cutoff. Unavailable fields remain represented; only covered_accounting_plan may
    /// admit the resulting source-scoped financial calculation.
    pub(crate) fn with_complete_ordinary_history<C: Into<SourcePlanCalendar>>(
        mut self,
        reads: Vec<(TiingoCompletedEodActionRead, C)>,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if self.ordinary.is_some()
            || self.current_ordinary.is_some()
            || reads.is_empty()
            || reads.len() > 32
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let mut reads: Vec<_> = reads
            .into_iter()
            .map(|(read, calendar)| (read, calendar.into()))
            .collect();
        reads.sort_by_key(|(read, _)| read.history().selection().receipt().instrument_id());
        let source_reads: Vec<_> = reads
            .iter()
            .map(|(read, calendar)| {
                (
                    read.source_history().as_ref(),
                    calendar.source_action_calendar().as_ref(),
                )
            })
            .collect();
        self.plan = CorporateActionPlan::try_from_source_reads(
            &self.source,
            &self.query_identity,
            self.calendar.source_action_calendar(),
            &source_reads,
            &self.requested_instruments.iter().copied().collect(),
            self.interval,
            self.plan.policy(),
            self.payment_policy,
            self.plan.valuation_cutoff(),
            self.evaluated_at,
            limits,
            deadline,
            cancellation,
        )
        .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        self.retain_ordinary_history(reads, deadline, cancellation)
    }

    /// Stores original physical history custody and the final selector's actual coverage.
    pub(super) fn retain_ordinary_history(
        mut self,
        reads: Vec<(TiingoCompletedEodActionRead, SourcePlanCalendar)>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        let coverage = self
            .plan
            .source_coverage()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        self.unresolved = coverage.unresolved().to_vec().into_boxed_slice();
        self.outside_window = coverage.outside_window().to_vec().into_boxed_slice();
        let gaps = coverage.ordinary_gaps().to_vec();
        let reconciled = coverage.reconciled().to_vec();
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/source-known-ordinary-action-coverage/v1\0");
        hasher.update(self.source.receipt_digest().bytes());
        hasher.update(self.plan.content_hash().bytes());
        hasher.update(self.plan.audit_hash().bytes());
        let references: Vec<_> = reads
            .iter()
            .map(|(read, calendar)| OrdinaryHistoryReference {
                history: read.reference().clone(),
                calendar: calendar.reference().clone(),
                timestamped: None,
            })
            .collect();
        hasher.update(
            serde_json::to_vec(&(self.interval, &self.requested_instruments, &references))
                .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?,
        );
        for overlap in &reconciled {
            hasher.update(overlap.applied_alpaca_record.bytes());
            hasher.update(overlap.corroborating_tiingo_record.bytes());
        }
        self.ordinary = Some(OrdinaryActionCoverage {
            reads,
            gaps: gaps.into_boxed_slice(),
            reconciled: reconciled.into_boxed_slice(),
            digest: digest(hasher.finalize().into()),
            timestamped: Vec::new(),
        });
        check(deadline, cancellation)?;
        Ok(self)
    }

    /// Complete ordinary fields over the stated economic-date interval and no unresolved returned
    /// lifecycle action in the exact Alpaca source snapshot. This is deliberately not a claim of
    /// global completeness for corporate reorganizations absent from both source datasets.
    pub(crate) fn covered_accounting_plan(
        &self,
    ) -> Result<&CorporateActionPlan, ApplicableActionPlanError> {
        if self.current_ordinary.is_none() {
            let coverage = self
                .ordinary
                .as_ref()
                .ok_or(ApplicableActionPlanError::IncompleteOrdinaryCoverage)?;
            if !coverage.gaps.is_empty() {
                return Err(ApplicableActionPlanError::IncompleteOrdinaryCoverage);
            }
        }
        let plan = self.accounting_plan()?;
        if plan.source_admission().is_none() {
            return Err(ApplicableActionPlanError::IncompleteOrdinaryCoverage);
        }
        Ok(plan)
    }
    /// Validates exact retained histories against this already physically reopened source plan.
    /// Saved references or general decoded plans cannot construct this authority.
    pub(crate) fn validate_accounting_histories<H: NativeSessionHistory>(
        &self,
        histories: &[&H],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if histories.is_empty() || histories.len() > 32 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let cutoff = self.covered_accounting_plan()?.knowledge_cutoff();
        self.validate_histories_at_cutoff(histories, cutoff, deadline, cancellation)
    }

    fn validate_histories_at_cutoff<H: NativeSessionHistory>(
        &self,
        histories: &[&H],
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ApplicableActionPlanError> {
        let coverage = self
            .ordinary_coverage()
            .ok_or(ApplicableActionPlanError::IncompleteOrdinaryCoverage)?;
        if !coverage.timestamped.is_empty() {
            for history in histories {
                check(deadline, cancellation)?;
                if history.read_receipt().knowledge_cutoff() != cutoff
                    || history.native_sessions().is_none()
                    || !coverage
                        .timestamped
                        .contains(&super::anchor::timestamped_history_reference(*history)?)
                {
                    return Err(ApplicableActionPlanError::InvalidEvidence);
                }
            }
            return Ok(());
        }
        for history in histories {
            check(deadline, cancellation)?;
            let receipt = history.selection().receipt();
            let matching = coverage.reads().iter().find(|(read, _)| {
                read.history().selection().receipt().instrument_id() == receipt.instrument_id()
            });
            let Some((original, _)) = matching else {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            };
            let original = original.history();
            let expected = original.selection().receipt();
            if history.read_receipt().knowledge_cutoff() != cutoff
                || original.read_receipt().knowledge_cutoff() != cutoff
                || receipt.origin_manifest() != expected.origin_manifest()
                || receipt.receipt_digest() != expected.receipt_digest()
                || receipt.binding_digest() != expected.binding_digest()
                || receipt.date_windows() != expected.date_windows()
                || receipt.bar_set_digest() != expected.bar_set_digest()
                || history.read_receipt().history_content_digest()
                    != original.read_receipt().history_content_digest()
                || !same_retained_rows(history.bars(), original.bars())?
                || !same_retained_rows(history.source_actions(), original.source_actions())?
                || history.native_sessions().is_none()
                || !history
                    .native_sessions()
                    .ok_or(ApplicableActionPlanError::InvalidEvidence)?
                    .sessions()
                    .same_rows(
                        original
                            .native_sessions()
                            .ok_or(ApplicableActionPlanError::InvalidEvidence)?
                            .sessions(),
                    )
                    .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?
            {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
        }
        check(deadline, cancellation)?;
        Ok(())
    }

    /// Narrow source-known share adjustment only. Missing cash units remain in the original
    /// source reference/audit and cannot be consumed as total-return or income authority.
    pub(crate) fn covered_price_plan(
        &self,
    ) -> Result<&CorporateActionPlan, ApplicableActionPlanError> {
        if self.ordinary.is_none() || self.plan.source_split_admission().is_none() {
            return Err(ApplicableActionPlanError::IncompleteOrdinaryCoverage);
        }
        Ok(&self.plan)
    }
    pub(crate) fn price_reference(
        &self,
    ) -> Result<SourceAppliedCorporateActionPlanReference, ApplicableActionPlanError> {
        self.covered_price_plan()?;
        self.source_reference()
    }
    pub(crate) fn into_covered_price_plan(
        self,
    ) -> Result<CorporateActionPlan, ApplicableActionPlanError> {
        self.covered_price_plan()?;
        Ok(self.plan)
    }
    pub(crate) const fn ordinary_coverage(&self) -> Option<&OrdinaryActionCoverage> {
        self.ordinary.as_ref()
    }
    pub(super) fn ordinary_references(&self) -> Vec<OrdinaryHistoryReference> {
        self.ordinary
            .as_ref()
            .map(|coverage| {
                coverage
                    .reads
                    .iter()
                    .map(|(read, calendar)| OrdinaryHistoryReference {
                        history: read.reference().clone(),
                        calendar: calendar.reference().clone(),
                        timestamped: coverage
                            .timestamped
                            .iter()
                            .find(|history| {
                                history.instrument_id()
                                    == read.history().selection().receipt().instrument_id()
                            })
                            .cloned(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Reopens an owning complete history from the exact source-plan custody. A current action
    /// recipe alone owns no price history and therefore cannot supply one through this read.
    pub(crate) async fn read_history_reference(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        instrument: InstrumentId,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<market_squawk_data::CompleteMarketBarHistoryCursor>, ApplicableActionPlanError>
    {
        check(deadline, &cancellation)?;
        let Some(plan) = self
            .read_reference_with_job_context(reference, deadline, cancellation.clone(), job)
            .await?
        else {
            return Ok(None);
        };
        let Some(ordinary) = plan.ordinary_coverage() else {
            return Ok(None);
        };
        if !ordinary.timestamped.is_empty() {
            let mut matching = ordinary
                .timestamped
                .iter()
                .filter(|history| history.instrument_id() == instrument);
            let Some(original) = matching.next() else {
                return Ok(None);
            };
            if matching.next().is_some() {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
            let history = self
                .reopen_timestamped_history(
                    original,
                    reference.knowledge_cutoff(),
                    deadline,
                    &cancellation,
                    job,
                )
                .await?;
            plan.validate_histories_at_cutoff(
                &[&history],
                reference.knowledge_cutoff(),
                deadline,
                &cancellation,
            )?;
            return Ok(Some(history));
        }
        let mut matching = ordinary
            .reads()
            .iter()
            .filter(|(read, _)| read.history().selection().receipt().instrument_id() == instrument);
        let Some((original, calendar)) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let history = self
            .research
            .read_tiingo_eod_history_reference(original.reference(), deadline, &cancellation)
            .await
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
        let history = match calendar {
            SourcePlanCalendar::Live(calendar) => {
                self.research
                    .rejoin_market_history_native_sessions_with_calendar_with_job_context(
                        history,
                        calendar,
                        deadline,
                        &cancellation,
                        job,
                    )
                    .await
            }
            SourcePlanCalendar::Retained(calendar) => {
                self.research
                    .rejoin_market_history_native_sessions_with_retained_calendar_with_job_context(
                        history,
                        calendar,
                        deadline,
                        &cancellation,
                        job,
                    )
                    .await
            }
        }
        .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
        plan.validate_histories_at_cutoff(
            &[&history],
            reference.knowledge_cutoff(),
            deadline,
            &cancellation,
        )?;
        check(deadline, &cancellation)?;
        Ok(Some(history))
    }
    /// Reopens original source authority and binds a consumer's exact complete history owners to
    /// that same source pool and frozen cutoff. This never performs source acquisition.
    pub(crate) async fn read_reference_for_histories<H: NativeSessionHistory>(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        histories: &[&H],
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<SourceAppliedCorporateActionPlan>, ApplicableActionPlanError> {
        check(deadline, &cancellation)?;
        if histories.is_empty() || histories.len() > 32 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let Some(plan) = self
            .read_reference_with_job_context(reference, deadline, cancellation.clone(), job)
            .await?
        else {
            return Ok(None);
        };
        plan.validate_accounting_histories(histories, deadline, &cancellation)?;
        Ok(Some(plan))
    }

    /// Same original-history custody check with explicitly price-only source admission.
    pub(crate) async fn read_price_reference_for_histories<H: NativeSessionHistory>(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        histories: &[&H],
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<SourceAppliedCorporateActionPlan>, ApplicableActionPlanError> {
        check(deadline, &cancellation)?;
        if histories.is_empty() || histories.len() > 32 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let Some(plan) = self
            .read_reference_with_job_context(reference, deadline, cancellation.clone(), job)
            .await?
        else {
            return Ok(None);
        };
        plan.covered_price_plan()?;
        plan.validate_histories_at_cutoff(
            histories,
            reference.knowledge_cutoff(),
            deadline,
            &cancellation,
        )?;
        Ok(Some(plan))
    }

    pub(super) async fn reopen_ordinary(
        &self,
        references: &[OrdinaryHistoryReference],
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Vec<(TiingoCompletedEodActionRead, SourcePlanCalendar)>, ApplicableActionPlanError>
    {
        if references.is_empty() || references.len() > 32 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let mut reads = Vec::new();
        reads
            .try_reserve_exact(references.len())
            .map_err(|_| ApplicableActionPlanError::SourceRead(ServiceError::ResourceExhausted))?;
        for reference in references {
            check(deadline, cancellation)?;
            let calendar = self
                .calendars
                .read_reference_with_job_context(
                    &reference.calendar,
                    cutoff,
                    deadline,
                    cancellation.clone(),
                    job,
                )
                .await
                .map_err(|error| ApplicableActionPlanError::SourceRead(map_calendar_error(error)))?
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            let history = match &calendar {
                SourcePlanCalendar::Live(calendar) => self.research
                    .read_tiingo_eod_history_action_reference_with_calendar_with_job_context(
                        &reference.history, calendar, deadline, cancellation, job,
                    ).await,
                SourcePlanCalendar::Retained(calendar) => {
                    let history = self.research
                        .read_tiingo_eod_history_reference(&reference.history, deadline, cancellation)
                        .await
                        .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
                    let history = self.research
                        .rejoin_market_history_native_sessions_with_retained_calendar_with_job_context(
                            history, calendar, deadline, cancellation, job,
                        ).await
                        .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
                    self.research.rejoin_tiingo_eod_history_actions_with_job_context(
                        history, deadline, cancellation, job,
                    ).await
                }
            }.map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
            reads.push((history, calendar));
        }
        Ok(reads)
    }
}

fn digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

impl SourceAppliedCorporateActionPlan {
    /// The timestamp prices and nominal economic-date action pool remain distinct originals.
    pub(crate) fn with_completed_timestamp_price_histories<H: NativeSessionHistory>(
        self,
        histories: &[&H],
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        if histories.is_empty() || histories.len() > 2 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let originals = histories
            .iter()
            .map(|history| {
                Ok((
                    super::anchor::timestamped_history_reference(*history)?,
                    history
                        .ordinary_evidence()
                        .map_err(|error| map_source_plan_error(error, deadline, cancellation))?,
                ))
            })
            .collect::<Result<Vec<_>, ApplicableActionPlanError>>()?;
        self.with_timestamp_price_proofs(originals, limits, deadline, cancellation)
    }

    pub(super) fn with_timestamp_price_proofs(
        mut self,
        originals: Vec<(
            super::anchor::AlpacaHistoryReference,
            market_squawk_data::CompletedOrdinaryHistoryEvidence,
        )>,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        let ordinary = self
            .ordinary
            .as_mut()
            .ok_or(ApplicableActionPlanError::IncompleteOrdinaryCoverage)?;
        if !ordinary.timestamped.is_empty()
            || self.current_ordinary.is_some()
            || self.anchor.is_some()
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        self.plan = self
            .plan
            .try_bind_completed_history_evidence(
                self.calendar.source_action_calendar(),
                &originals
                    .iter()
                    .map(|(_, proof)| proof.clone())
                    .collect::<Vec<_>>(),
                limits,
                deadline,
                cancellation,
            )
            .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        let coverage = self
            .plan
            .source_coverage()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        self.unresolved = coverage.unresolved().to_vec().into_boxed_slice();
        self.outside_window = coverage.outside_window().to_vec().into_boxed_slice();
        ordinary.timestamped = originals
            .into_iter()
            .map(|(reference, _)| reference)
            .collect();
        ordinary
            .timestamped
            .sort_by_key(super::anchor::AlpacaHistoryReference::instrument_id);
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/original-timestamp-prices-with-nominal-action-proof/v1\0");
        hash.update(ordinary.digest.bytes());
        hash.update(self.plan.content_hash().bytes());
        hash.update(self.plan.audit_hash().bytes());
        hash.update(
            serde_json::to_vec(&ordinary.timestamped)
                .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?,
        );
        ordinary.digest = digest(hash.finalize().into());
        check(deadline, cancellation)?;
        Ok(self)
    }
}
impl SourceAppliedCorporateActionReadCapability {
    pub(super) async fn reopen_timestamp_price_proofs(
        &self,
        references: &[OrdinaryHistoryReference],
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<
        Vec<(
            super::anchor::AlpacaHistoryReference,
            market_squawk_data::CompletedOrdinaryHistoryEvidence,
        )>,
        ApplicableActionPlanError,
    > {
        if references.len() > 32 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let mut proofs = Vec::new();
        let mut unique = std::collections::BTreeSet::new();
        for reference in references
            .iter()
            .filter_map(|reference| reference.timestamped.as_ref())
        {
            if !unique.insert(reference.instrument_id()) {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
            let history = self
                .reopen_timestamped_history(reference, cutoff, deadline, cancellation, job)
                .await?;
            let proof = history
                .ordinary_evidence()
                .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
            proofs.push((reference.clone(), proof));
            drop(history);
        }
        Ok(proofs)
    }
}

fn same_retained_rows<T: PartialEq>(
    left: impl IntoIterator<Item = Result<T, market_squawk_data::AnalyticalReadError>>,
    right: impl IntoIterator<Item = Result<T, market_squawk_data::AnalyticalReadError>>,
) -> Result<bool, ApplicableActionPlanError> {
    let mut right = right.into_iter();
    for left in left {
        let left = left
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?;
        let Some(row) = right.next() else {
            return Ok(false);
        };
        let row = row
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?;
        if left != row {
            return Ok(false);
        }
    }
    match right.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(error)) => Err(ApplicableActionPlanError::SourceRead(map_analytical_error(
            error,
        ))),
    }
}
