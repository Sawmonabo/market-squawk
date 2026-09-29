//! Private live admission retained by the existing plan, separate from its recovery values.

mod completed_history;
mod current_ordinary;

use super::*;
use crate::{
    CorporateActionQueryIdentitySelection, CorporateActionSourceSnapshot, DatasetManifestRef,
};
use market_squawk_adapter_tiingo::TiingoEodActionFieldDisposition;
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, io::Write as _, mem::size_of, sync::Arc, time::Instant};

/// Exact source-known coverage, constructed only by the real source/calendar read constructor.
/// It retains full source audit bytes and original applied records without retaining another
/// complete price history in every projected dataset plan. Its fields cannot be deserialized.
#[derive(Debug)]
pub struct CorporateActionSourceCoverage {
    interval: (CalendarDate, CalendarDate),
    instruments: Box<[InstrumentId]>,
    knowledge_cutoff: Timestamp,
    valuation_bound: Timestamp,
    source_calendar_identity: (crate::Sha256Digest, EvidenceDigest, EvidenceDigest),
    projection_records: Box<[CorporateActionRecord]>,
    application_starts: Box<[(InstrumentId, Timestamp)]>,
    roots: Box<[DatasetManifestRef]>,
    history_inputs: Box<[DatasetManifestRef]>,
    completed_histories: Box<[super::current_ordinary::CompletedOrdinaryHistoryEvidence]>,
    source_audit: Box<[u8]>,
    ordinary_present: bool,
    current_us_equity_dates: bool,
    outside_window: Box<[SourceIdentifier]>,
    unresolved: Box<[ApplicableActionGap]>,
    gaps: Box<[OrdinaryActionCoverageGap]>,
    reconciled: Box<[ReconciledOrdinaryAction]>,
    digest: EvidenceDigest,
    pub(super) retained_bytes: usize,
}
impl PartialEq for CorporateActionSourceCoverage {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}
impl Eq for CorporateActionSourceCoverage {}

impl CorporateActionSourceCoverage {
    /// True only after original US IEX calendar and exact economic-date reads are joined.
    pub const fn uses_us_equity_dates(&self) -> bool {
        self.current_us_equity_dates
    }
    pub const fn interval(&self) -> (CalendarDate, CalendarDate) {
        self.interval
    }
    pub fn instruments(&self) -> &[InstrumentId] {
        &self.instruments
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    /// First actual source-calendar application boundary for this admitted instrument.
    /// This borrows retained native session evidence and performs no date-to-time conversion.
    pub fn application_starts_at(&self, instrument: InstrumentId) -> Option<Timestamp> {
        self.application_starts
            .iter()
            .find_map(|(id, starts_at)| (*id == instrument).then_some(*starts_at))
    }
    pub fn source_manifests(&self) -> &[DatasetManifestRef] {
        &self.roots
    }
    /// Only these selected history generations enter feature PIT rows. Other source_manifests
    /// are proof/rights parents and must not be decoded a second time as price candidates.
    pub fn history_input_manifests(&self) -> &[DatasetManifestRef] {
        &self.history_inputs
    }
    /// Genuine timestamp history span, admitted through original receipt and native calendar.
    pub fn covers_timestamp_history_span(
        &self,
        instrument: InstrumentId,
        manifest: &DatasetManifestRef,
        knowledge: Timestamp,
        left: Timestamp,
        right: Timestamp,
    ) -> bool {
        knowledge == self.knowledge_cutoff
            && self
                .completed_histories
                .iter()
                .any(|history| history.covers_span(instrument, manifest, knowledge, left, right))
    }
    pub(crate) fn admits_completed_history(
        &self,
        history: &super::current_ordinary::CompletedOrdinaryHistoryEvidence,
    ) -> bool {
        self.completed_histories.contains(history)
    }
    /// Checked shared proof heap, charged once when several projected plans share this marker.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.digest
    }
    pub fn outside_window(&self) -> &[SourceIdentifier] {
        &self.outside_window
    }
    pub fn unresolved(&self) -> &[ApplicableActionGap] {
        &self.unresolved
    }
    pub fn ordinary_gaps(&self) -> &[OrdinaryActionCoverageGap] {
        &self.gaps
    }
    pub fn reconciled(&self) -> &[ReconciledOrdinaryAction] {
        &self.reconciled
    }
    /// Full retained source summary/observations, query identity and native zero/missing/action
    /// rows. These audit bytes do not recreate this marker or confer serving authority.
    pub fn retained_source_audit(&self) -> &[u8] {
        &self.source_audit
    }
    fn require_projection(
        &self,
        instrument: InstrumentId,
        knowledge_cutoff: Timestamp,
        valuation_cutoff: Timestamp,
    ) -> Result<(), CorporateActionError> {
        if knowledge_cutoff != self.knowledge_cutoff
            || valuation_cutoff > self.valuation_bound
            || valuation_cutoff > knowledge_cutoff
            || !self.instruments.contains(&instrument)
            || self
                .application_starts
                .iter()
                .find(|(id, _)| *id == instrument)
                .is_none_or(|(_, start)| valuation_cutoff < *start)
        {
            Err(CorporateActionError::InvalidApplication)
        } else {
            Ok(())
        }
    }
    fn complete(&self) -> bool {
        self.ordinary_present && self.unresolved.is_empty() && self.gaps.is_empty()
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_retain(
        source: &CorporateActionSourceSnapshot,
        query: &CorporateActionQueryIdentitySelection,
        calendar: &RetainedCorporateActionCalendar,
        reads: &[(
            &RetainedTiingoEodActionHistory,
            &RetainedCorporateActionCalendar,
        )],
        instruments: &BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        evaluated_at: Timestamp,
        valuation_bound: Timestamp,
        payment_policy: CorporateActionPaymentPolicy,
        projection_records: Box<[CorporateActionRecord]>,
        outside_window: Vec<SourceIdentifier>,
        unresolved: Vec<ApplicableActionGap>,
        gaps: Vec<OrdinaryActionCoverageGap>,
        reconciled: Vec<ReconciledOrdinaryAction>,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Self, CorporateActionError> {
        let mut audit = AuditBuffer {
            bytes: Vec::new(),
            limit: limits.max_retained_bytes().get(),
            deadline,
            cancellation,
        };
        audit.json(&(
            interval,
            instruments,
            source.knowledge_cutoff(),
            evaluated_at,
            valuation_bound,
            payment_policy,
        ))?;
        audit.json(&(
            source.receipt_digest(),
            source.capture_page_received_at(),
            source.summary(),
            source.source_actions(),
            query.retained(),
        ))?;
        for definition in query.selected_definitions() {
            audit.json(&(
                definition.definition(),
                definition.revision_digest(),
                definition.revision_sequence(),
                definition.published_at(),
            ))?;
        }
        for record in source.actions() {
            audit.record(record)?;
        }
        let mut roots = vec![source.manifest().clone(), calendar.manifest().clone()];
        audit.json(&(
            calendar.binding_digest(),
            calendar.evidence_digest(),
            calendar.knowledge_cutoff(),
        ))?;
        let mut history_inputs = Vec::new();
        for (read, calendar) in reads {
            let history = read.history();
            let selected = history.selection().pinned().manifest();
            if !history_inputs.contains(selected) {
                history_inputs.push(selected.clone());
            }
            audit.json(&(
                "feature-pit-input",
                selected.dataset_id().as_str(),
                selected.manifest_version(),
                selected.content_hash().bytes(),
            ))?;
            let receipt = history.selection().receipt();
            let graph = receipt
                .date_windows()
                .ok_or(CorporateActionError::InvalidApplication)?;
            audit.json(&(
                read.evidence_digest(),
                receipt.instrument_id(),
                graph.venue_id(),
                graph.requested_dates(),
                calendar.binding_digest(),
                calendar.evidence_digest(),
            ))?;
            for row in read.action_rows() {
                let row = row.map_err(|_| CorporateActionError::InvalidApplication)?;
                audit.json(&(
                    row.history_page_index,
                    row.provider_row_index,
                    row.date,
                    row.row_digest,
                    row.cash_dividend,
                    row.split_factor,
                    field(row.cash),
                    field(row.shares),
                ))?;
            }
            if let Some(unit) = read.actions().cash_unit() {
                audit.json(&(
                    unit.instrument(),
                    unit.contract_identity(),
                    unit.currency(),
                    unit.assertion(),
                    unit.available_at(),
                ))?;
            }
            for record in read.records() {
                let record = record.map_err(|_| CorporateActionError::InvalidApplication)?;
                audit.record(&record)?;
            }
            for root in [
                history.selection().pinned().manifest(),
                history.read_receipt().origin_manifest(),
                calendar.manifest(),
            ] {
                if !roots.contains(root) {
                    roots.push(root.clone());
                }
            }
        }
        for overlap in &reconciled {
            audit.json(&(
                overlap.instrument,
                overlap.date,
                match overlap.field {
                    OrdinaryActionField::Shares => 0_u8,
                    OrdinaryActionField::Cash => 1,
                },
                overlap.applied_alpaca_record,
                overlap.corroborating_tiingo_record,
            ))?;
        }
        for record in &projection_records {
            audit.record(record)?;
        }
        // Immutable manifest schema/generation identity is part of the same admission digest.
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
        let source_audit = audit.bytes.into_boxed_slice();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/source-admitted-corporate-action-plan/v1\0");
        hash.update(&source_audit);
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        let mut value = Self {
            interval,
            instruments: instruments.iter().copied().collect(),
            knowledge_cutoff: source.knowledge_cutoff(),
            valuation_bound,
            source_calendar_identity: (
                calendar.manifest().content_hash(),
                calendar.binding_digest(),
                calendar.evidence_digest(),
            ),
            projection_records,
            application_starts: reads
                .iter()
                .filter_map(|(read, calendar)| {
                    calendar.native_session_bounds(interval).map(|(start, _)| {
                        (read.history().selection().receipt().instrument_id(), start)
                    })
                })
                .collect(),
            roots: roots.into_boxed_slice(),
            history_inputs: history_inputs.into_boxed_slice(),
            completed_histories: Box::new([]),
            source_audit,
            ordinary_present: !reads.is_empty(),
            current_us_equity_dates: false,
            outside_window: outside_window.into_boxed_slice(),
            unresolved: unresolved.into_boxed_slice(),
            gaps: gaps.into_boxed_slice(),
            reconciled: reconciled.into_boxed_slice(),
            digest,
            retained_bytes: 0,
        };
        value.retained_bytes = value.checked_retained_bytes()?;
        super::retained::require_retained_limit(
            value.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        Ok(value)
    }
    fn checked_retained_bytes(&self) -> Result<usize, CorporateActionError> {
        let mut bytes = size_of::<Self>() + 2 * size_of::<usize>(); // Arc header, owned once per plan.
        for (count, width) in [
            (self.instruments.len(), size_of::<InstrumentId>()),
            (
                self.application_starts.len(),
                size_of::<(InstrumentId, Timestamp)>(),
            ),
            (
                self.projection_records.len(),
                size_of::<CorporateActionRecord>(),
            ),
            (self.roots.len(), size_of::<DatasetManifestRef>()),
            (self.history_inputs.len(), size_of::<DatasetManifestRef>()),
            (
                self.completed_histories.len(),
                size_of::<super::current_ordinary::CompletedOrdinaryHistoryEvidence>(),
            ),
            (self.source_audit.len(), 1),
            (self.outside_window.len(), size_of::<SourceIdentifier>()),
            (self.unresolved.len(), size_of::<ApplicableActionGap>()),
            (self.gaps.len(), size_of::<OrdinaryActionCoverageGap>()),
            (self.reconciled.len(), size_of::<ReconciledOrdinaryAction>()),
        ] {
            bytes = add(
                bytes,
                count
                    .checked_mul(width)
                    .ok_or(CorporateActionError::RetainedSizeOverflow)?,
            )?;
        }
        for record in &self.projection_records {
            bytes = add(bytes, super::retained::record_dynamic_bytes(record)?)?;
        }
        for root in self.roots.iter().chain(self.history_inputs.iter()) {
            bytes = add(bytes, root.dataset_id().as_str().len())?;
            bytes = add(bytes, root.schema().name().len())?;
        }
        for history in &self.completed_histories {
            for manifest in [history.manifest(), history.origin_manifest()] {
                bytes = add(bytes, manifest.dataset_id().as_str().len())?;
                bytes = add(bytes, manifest.schema().name().len())?;
            }
        }
        for action in &self.outside_window {
            bytes = add(bytes, action.retained_bytes())?;
        }
        for gap in &self.unresolved {
            let dynamic = match gap {
                ApplicableActionGap::SourceDisposition { action, .. }
                | ApplicableActionGap::MissingEconomicDate { action }
                | ApplicableActionGap::MissingEffectiveSession { action, .. }
                | ApplicableActionGap::InvalidPayableOrder { action, .. } => {
                    action.retained_bytes()
                }
                ApplicableActionGap::SameDateEconomicOrdering { .. } => 0,
            };
            bytes = add(bytes, dynamic)?;
        }
        Ok(bytes)
    }
}
impl CorporateActionPlan {
    /// Source-known scope and explicit gaps, including finite queries without interval coverage.
    pub fn source_coverage(&self) -> Option<&CorporateActionSourceCoverage> {
        self.source_coverage.as_deref()
    }
    /// Complete native ordinary fields and resolved returned lifecycle events. General builders
    /// and recovery decoding cannot supply this authority, even when the action vector is empty.
    pub fn source_admission(&self) -> Option<&CorporateActionSourceCoverage> {
        self.source_coverage().filter(|coverage| {
            coverage.complete()
                && self.conflicts.is_empty()
                && self.exclusions.iter().all(|excluded| {
                    excluded.reason() == CorporateActionExclusionReason::FutureEffectiveTime
                })
        })
    }
    /// Derives the exact requested vector shape and same shared proof heap under existing fixed
    /// process ceilings. Earlier-cutoff exclusions are included; this does not raise a global cap.
    pub fn source_projection_limits(
        &self,
        policy: CorporateActionPolicy,
        instrument: InstrumentId,
        knowledge_cutoff: Timestamp,
        valuation_cutoff: Timestamp,
    ) -> Result<CorporateActionLimits, CorporateActionError> {
        let coverage = self
            .source_admission()
            .ok_or(CorporateActionError::InvalidApplication)?;
        coverage.require_projection(instrument, knowledge_cutoff, valuation_cutoff)?;
        let selected = || {
            coverage.projection_records.iter().filter(|record| {
                record.observation().context().provenance().instrument_id() == Some(instrument)
            })
        };
        let count = selected().count();
        let shape = super::retained::plan_shape_for_records(
            policy,
            knowledge_cutoff,
            valuation_cutoff,
            selected(),
        )?;
        let retained = add(shape.minimum_retained_bytes, coverage.retained_bytes)?;
        let canonical_work = selected().try_fold(0_usize, |sum, record| {
            add(
                sum,
                super::canonical::canonical_record_bytes(record)?.capacity(),
            )
        })?;
        let bytes = retained.max(canonical_work);
        CorporateActionLimits::try_new(
            std::num::NonZeroUsize::new(count.max(1)).ok_or(CorporateActionError::InvalidLimits)?,
            std::num::NonZeroUsize::new(bytes).ok_or(CorporateActionError::InvalidLimits)?,
        )
    }
    /// Projects the existing admitted source pool; immutable knowledge and native scope remain
    /// unchanged. Future economic records stay audited exclusions at the earlier valuation.
    pub fn try_project_source_action_plan(
        &self,
        policy: CorporateActionPolicy,
        instrument: InstrumentId,
        knowledge_cutoff: Timestamp,
        valuation_cutoff: Timestamp,
        limits: CorporateActionLimits,
    ) -> Result<Self, CorporateActionError> {
        let required =
            self.source_projection_limits(policy, instrument, knowledge_cutoff, valuation_cutoff)?;
        if required.max_actions().get() > limits.max_actions().get() {
            return Err(CorporateActionError::ActionLimitExceeded {
                limit: limits.max_actions().get(),
                observed: required.max_actions().get(),
            });
        }
        super::retained::require_retained_limit(
            required.max_retained_bytes().get(),
            limits.max_retained_bytes().get(),
        )?;
        let coverage = self
            .source_admission()
            .ok_or(CorporateActionError::InvalidApplication)?;
        let mut records = Vec::new();
        super::retained::try_reserve_exact(&mut records, required.max_actions().get())?;
        for record in coverage.projection_records.iter().filter(|record| {
            record.observation().context().provenance().instrument_id() == Some(instrument)
        }) {
            records.push(record.clone());
        }
        let mut plan =
            Self::try_build(policy, knowledge_cutoff, valuation_cutoff, records, limits)?;
        let retained_bytes = add(plan.retained_bytes, coverage.retained_bytes)?;
        super::retained::require_retained_limit(retained_bytes, limits.max_retained_bytes().get())?;
        plan.retained_bytes = retained_bytes;
        plan.source_coverage = self.source_coverage.as_ref().map(Arc::clone);
        Ok(plan)
    }
    /// Source-known share-adjustment coverage only. Cash gaps remain in the same immutable
    /// source audit and never satisfy source_admission or grant total-return accounting.
    pub fn source_split_admission(&self) -> Option<&CorporateActionSourceCoverage> {
        self.source_coverage().filter(|coverage| {
            coverage.ordinary_present
                && coverage.unresolved.is_empty()
                && coverage.gaps.iter().all(|gap| matches!(gap,
                    OrdinaryActionCoverageGap::MissingField { field: OrdinaryActionField::Cash, .. }
                    | OrdinaryActionCoverageGap::SourceDisagreement { field: OrdinaryActionField::Cash, .. }
                    | OrdinaryActionCoverageGap::EconomicSourceUnavailable { field: OrdinaryActionField::Cash, disposition: market_squawk_domain::CorporateActionSourceDisposition::MissingCurrency, .. }
                ))
                && self.conflicts.is_empty()
                && self.exclusions.iter().all(|excluded| {
                    excluded.reason() == CorporateActionExclusionReason::FutureEffectiveTime
                })
        })
    }
    /// Exact retained pool bounds for a split-only price projection. Knowledge and native
    /// interval authority remain unchanged; missing share fields and lifecycle gaps reject.
    pub fn source_split_projection_limits(
        &self,
        policy: CorporateActionPolicy,
        instrument: InstrumentId,
        knowledge_cutoff: Timestamp,
        valuation_cutoff: Timestamp,
    ) -> Result<CorporateActionLimits, CorporateActionError> {
        if policy.adjustment() != CorporateActionAdjustment::SplitAdjusted {
            return Err(CorporateActionError::InvalidApplication);
        }
        let coverage = self
            .source_split_admission()
            .ok_or(CorporateActionError::InvalidApplication)?;
        coverage.require_projection(instrument, knowledge_cutoff, valuation_cutoff)?;
        let selected = || {
            coverage.projection_records.iter().filter(|record| {
                record.observation().context().provenance().instrument_id() == Some(instrument)
            })
        };
        let count = selected().count();
        let shape = super::retained::plan_shape_for_records(
            policy,
            knowledge_cutoff,
            valuation_cutoff,
            selected(),
        )?;
        let retained = add(shape.minimum_retained_bytes, coverage.retained_bytes)?;
        let canonical_work = selected().try_fold(0_usize, |sum, record| {
            add(
                sum,
                super::canonical::canonical_record_bytes(record)?.capacity(),
            )
        })?;
        let bytes = retained.max(canonical_work);
        CorporateActionLimits::try_new(
            std::num::NonZeroUsize::new(count.max(1)).ok_or(CorporateActionError::InvalidLimits)?,
            std::num::NonZeroUsize::new(bytes).ok_or(CorporateActionError::InvalidLimits)?,
        )
    }
    /// Projects the existing admitted source pool; immutable knowledge and native scope remain
    /// unchanged. Future economic records stay audited exclusions at the earlier valuation.
    pub fn try_project_source_split_plan(
        &self,
        policy: CorporateActionPolicy,
        instrument: InstrumentId,
        knowledge_cutoff: Timestamp,
        valuation_cutoff: Timestamp,
        limits: CorporateActionLimits,
    ) -> Result<Self, CorporateActionError> {
        let required = self.source_split_projection_limits(
            policy,
            instrument,
            knowledge_cutoff,
            valuation_cutoff,
        )?;
        if required.max_actions().get() > limits.max_actions().get() {
            return Err(CorporateActionError::ActionLimitExceeded {
                limit: limits.max_actions().get(),
                observed: required.max_actions().get(),
            });
        }
        super::retained::require_retained_limit(
            required.max_retained_bytes().get(),
            limits.max_retained_bytes().get(),
        )?;
        let coverage = self
            .source_split_admission()
            .ok_or(CorporateActionError::InvalidApplication)?;
        let mut records = Vec::new();
        super::retained::try_reserve_exact(&mut records, required.max_actions().get())?;
        for record in coverage.projection_records.iter().filter(|record| {
            record.observation().context().provenance().instrument_id() == Some(instrument)
        }) {
            records.push(record.clone());
        }
        let mut plan =
            Self::try_build(policy, knowledge_cutoff, valuation_cutoff, records, limits)?;
        let retained_bytes = add(plan.retained_bytes, coverage.retained_bytes)?;
        super::retained::require_retained_limit(retained_bytes, limits.max_retained_bytes().get())?;
        plan.retained_bytes = retained_bytes;
        plan.source_coverage = self.source_coverage.as_ref().map(Arc::clone);
        Ok(plan)
    }
}
fn add(a: usize, b: usize) -> Result<usize, CorporateActionError> {
    a.checked_add(b)
        .ok_or(CorporateActionError::RetainedSizeOverflow)
}
fn field(value: TiingoEodActionFieldDisposition) -> (u8, usize) {
    match value {
        TiingoEodActionFieldDisposition::ExplicitNoEvent => (0, 0),
        TiingoEodActionFieldDisposition::MissingField => (1, 0),
        TiingoEodActionFieldDisposition::MissingMonetaryUnit => (2, 0),
        TiingoEodActionFieldDisposition::RatioOutsideCanonicalPrecision => (3, 0),
        TiingoEodActionFieldDisposition::Normalized { observation_index } => (4, observation_index),
    }
}
struct AuditBuffer<'a> {
    bytes: Vec<u8>,
    limit: usize,
    deadline: Instant,
    cancellation: &'a tokio_util::sync::CancellationToken,
}
impl AuditBuffer<'_> {
    fn failure(&self) -> CorporateActionError {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            CorporateActionError::SourceReadInterrupted
        } else {
            CorporateActionError::RetainedByteLimitExceeded {
                limit: self.limit,
                required: self.limit.saturating_add(1),
            }
        }
    }
    fn json(&mut self, value: &impl serde::Serialize) -> Result<(), CorporateActionError> {
        serde_json::to_writer(&mut *self, value).map_err(|_| self.failure())?;
        self.write_all(b"\n").map_err(|_| self.failure())
    }
    fn record(&mut self, record: &CorporateActionRecord) -> Result<(), CorporateActionError> {
        let bytes = super::canonical::canonical_record_bytes(record)?;
        self.write_all(&(bytes.len() as u64).to_be_bytes())
            .and_then(|_| self.write_all(&bytes))
            .map_err(|_| self.failure())
    }
}
impl std::io::Write for AuditBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|len| len > self.limit)
        {
            return Err(std::io::Error::other("source audit bound exceeded"));
        }
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(std::io::Error::other("source audit interrupted"));
        }
        let required = self.bytes.len() + bytes.len();
        if required > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(required)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(std::io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
