//! A completed timestamp price history may reuse an independently admitted nominal action pool.

use super::*;
use crate::corporate_actions::current_ordinary::CompletedOrdinaryHistoryEvidence;

impl CorporateActionPlan {
    /// Binds original timestamp price histories to an already admitted complete nominal ordinary
    /// action pool. Original action/calendar parents remain proof inputs; only the exact original
    /// timestamp generations become price inputs. Decoded plans cannot supply this authority.
    pub fn try_bind_completed_history_evidence(
        mut self,
        calendar: &RetainedCorporateActionCalendar,
        histories: &[CompletedOrdinaryHistoryEvidence],
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Self, CorporateActionError> {
        super::super::source_plan::check(deadline, cancellation)?;
        let invalid = || CorporateActionError::InvalidApplication;
        let coverage = self.source_split_admission().ok_or_else(invalid)?;
        let native_close = calendar
            .native_session_bounds(coverage.interval)
            .ok_or_else(invalid)?
            .1;
        if histories.is_empty()
            || histories.len() > 32
            || histories.len() != coverage.instruments.len()
            || coverage.current_us_equity_dates
            || !coverage.completed_histories.is_empty()
            || coverage.history_inputs.is_empty()
            || !coverage.roots.contains(calendar.manifest())
            || coverage.source_calendar_identity
                != (
                    calendar.manifest().content_hash(),
                    calendar.binding_digest(),
                    calendar.evidence_digest(),
                )
            || calendar.venue_id().as_str() != "iex"
            || calendar.knowledge_cutoff() != coverage.knowledge_cutoff
            || self.knowledge_cutoff() != coverage.knowledge_cutoff
            || self.valuation_cutoff() != native_close
            || coverage.valuation_bound != native_close
        {
            return Err(invalid());
        }
        let terminal_completion = histories[0].terminal_completion();
        let mut seen = BTreeSet::new();
        for history in histories {
            let instrument = history.require_calendar_scope(
                calendar,
                coverage.interval,
                coverage.knowledge_cutoff,
                terminal_completion,
                deadline,
                cancellation,
            )?;
            if !seen.insert(instrument)
                || !coverage.instruments.contains(&instrument)
                || coverage.application_starts_at(instrument)
                    != calendar
                        .native_session_bounds(coverage.interval)
                        .map(|bounds| bounds.0)
                || coverage.history_inputs.contains(history.manifest())
            {
                return Err(invalid());
            }
        }
        if coverage.projection_records.len() > limits.max_actions().get() {
            return Err(CorporateActionError::ActionLimitExceeded {
                limit: limits.max_actions().get(),
                observed: coverage.projection_records.len(),
            });
        }
        let policy = self.policy();
        // A fresh source pool is consumed. A projected/shared pool cannot silently acquire new
        // authority for other retained plans that still depend on the old source coordinates.
        let mut coverage = Arc::try_unwrap(self.source_coverage.take().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        drop(self);
        super::super::retained::require_retained_limit(
            coverage.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        let original_digest = coverage.digest;
        let mut audit = AuditBuffer {
            bytes: Vec::new(),
            limit: limits.max_retained_bytes().get(),
            deadline,
            cancellation,
        };
        audit
            .write_all(&coverage.source_audit)
            .map_err(|_| audit.failure())?;
        audit.json(&(
            "original-timestamp-prices-with-nominal-action-proof/v1",
            original_digest,
            calendar.binding_digest(),
            calendar.evidence_digest(),
            coverage.knowledge_cutoff,
            coverage.interval,
            native_close,
            terminal_completion,
        ))?;
        let mut roots = coverage.roots.into_vec();
        let mut history_inputs = Vec::new();
        let mut completed = Vec::new();
        super::super::retained::try_reserve_exact(&mut history_inputs, histories.len())?;
        super::super::retained::try_reserve_exact(&mut completed, histories.len())?;
        super::super::retained::try_reserve_exact(&mut roots, histories.len().saturating_mul(2))?;
        // Use the source-owned instrument ordering so caller slice order cannot change identity.
        for instrument in &coverage.instruments {
            super::super::source_plan::check(deadline, cancellation)?;
            let history = histories
                .iter()
                .find(|history| history.instrument_id() == *instrument)
                .ok_or_else(invalid)?;
            audit.json(&(
                "completed-timestamp-history",
                history.evidence_digest().bytes(),
            ))?;
            for root in [history.manifest(), history.origin_manifest()] {
                push_manifest(&mut roots, root)?;
            }
            push_manifest(&mut history_inputs, history.manifest())?;
            completed.push(history.clone());
        }
        for root in &roots {
            audit.json(&(
                root.dataset_id().as_str(),
                root.manifest_version(),
                root.schema().name(),
                root.schema_version().get(),
                root.schema().fingerprint(),
                root.content_hash().bytes(),
            ))?;
        }
        coverage.roots = roots.into_boxed_slice();
        coverage.history_inputs = history_inputs.into_boxed_slice();
        coverage.completed_histories = completed.into_boxed_slice();
        coverage.valuation_bound = terminal_completion;
        coverage.source_audit = audit.bytes.into_boxed_slice();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/source-admitted-timestamp-history-nominal-actions/v1\0");
        hash.update(&coverage.source_audit);
        coverage.digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        coverage.retained_bytes = coverage.checked_retained_bytes()?;
        super::super::retained::require_retained_limit(
            coverage.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        let mut records = Vec::new();
        super::super::retained::try_reserve_exact(&mut records, coverage.projection_records.len())?;
        records.extend(coverage.projection_records.iter().cloned());
        let mut plan = Self::try_build(
            policy,
            coverage.knowledge_cutoff,
            terminal_completion,
            records,
            limits,
        )?;
        plan.retained_bytes = add(plan.retained_bytes, coverage.retained_bytes)?;
        super::super::retained::require_retained_limit(
            plan.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        plan.source_coverage = Some(Arc::new(coverage));
        if plan.source_split_admission().is_none() {
            return Err(invalid());
        }
        super::super::source_plan::check(deadline, cancellation)?;
        Ok(plan)
    }
}

fn push_manifest(
    roots: &mut Vec<DatasetManifestRef>,
    candidate: &DatasetManifestRef,
) -> Result<(), CorporateActionError> {
    if let Some(retained) = roots.iter().find(|root| {
        root.dataset_id() == candidate.dataset_id()
            && root.manifest_version() == candidate.manifest_version()
    }) {
        return if retained == candidate {
            Ok(())
        } else {
            Err(CorporateActionError::InvalidApplication)
        };
    }
    roots.push(candidate.clone());
    Ok(())
}
