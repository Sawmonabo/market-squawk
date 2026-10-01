//! Projects genuine source snapshots through independently retained calendars.

mod anchor;
mod backup;
mod continuity;
mod current_ordinary;
mod forecast_outcome;
mod ordinary;
pub(crate) use forecast_outcome::SourceForecastOutcomeEvidence;
mod schema;
pub(crate) use continuity::{
    SourceForecastUnitContinuity, SourceForecastUnitContinuityError,
    SourceForecastUnitContinuityReference,
};
pub(crate) use ordinary::{
    OrdinaryActionCoverage, OrdinaryActionCoverageGap, OrdinaryActionField,
    ReconciledOrdinaryAction,
};

use super::source_errors::{
    map_analytical_error, map_calendar_error, map_ingest_error, map_query_identity_error,
    map_research_error,
};
use crate::application::market_calendar::{
    CompletedMarketSessionDateReceipt, CompletedMarketSessionRead,
    RetainedMarketSessionDateReceipt, RetainedMarketSessionRead,
};
use market_squawk_data::{
    CorporateActionLimits, CorporateActionPaymentPolicy, CorporateActionPlan,
    CorporateActionPolicy, CorporateActionQueryIdentitySelection, CorporateActionSourceSnapshot,
};
use market_squawk_domain::{
    CalendarDate, CorporateActionSourcePayload, EvidenceDigest, InstrumentId, SourceIdentifier,
    Timestamp,
};
use market_squawk_services::ServiceError;
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

pub(crate) use market_squawk_data::ApplicableActionGap;

/// Source-plan projection keeps live currentness checks separate from original backup replay.
/// Neither the retained variant nor its date receipts exposes execution/publication authority.
pub(crate) enum SourcePlanCalendar {
    Live(CompletedMarketSessionRead),
    Retained(RetainedMarketSessionRead),
}
impl From<CompletedMarketSessionRead> for SourcePlanCalendar {
    fn from(read: CompletedMarketSessionRead) -> Self {
        Self::Live(read)
    }
}
impl SourcePlanCalendar {
    pub(crate) fn reference(
        &self,
    ) -> &crate::application::market_calendar::CompletedMarketSessionReference {
        match self {
            Self::Live(read) => read.reference(),
            Self::Retained(read) => read.reference(),
        }
    }
    pub(crate) fn source_action_calendar(
        &self,
    ) -> &Arc<market_squawk_data::RetainedCorporateActionCalendar> {
        match self {
            Self::Live(read) => read.source_action_calendar(),
            Self::Retained(read) => read.source_action_calendar(),
        }
    }
    pub(crate) fn native_session_replay(
        &self,
    ) -> &Arc<market_squawk_adapter_alpaca::AlpacaRetainedCalendarSessions> {
        match self {
            Self::Live(read) => read.native_session_replay(),
            Self::Retained(read) => read.native_session_replay(),
        }
    }
    pub(crate) fn venue_id(&self) -> &market_squawk_domain::VenueId {
        match self {
            Self::Live(read) => read.venue_id(),
            Self::Retained(read) => read.venue_id(),
        }
    }
    pub(crate) fn date_session_on(
        &self,
        date: CalendarDate,
        cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<SourcePlanCalendarDate> {
        match self {
            Self::Live(read) => read
                .date_session_on(date, cutoff, evaluated_at)
                .map(SourcePlanCalendarDate::Live),
            Self::Retained(read) => read
                .date_session_on(date, cutoff, evaluated_at)
                .map(SourcePlanCalendarDate::Retained),
        }
    }
}
pub(crate) enum SourcePlanCalendarDate {
    Live(CompletedMarketSessionDateReceipt),
    Retained(RetainedMarketSessionDateReceipt),
}
impl SourcePlanCalendarDate {
    pub(crate) fn date(&self) -> CalendarDate {
        match self {
            Self::Live(read) => read.date(),
            Self::Retained(read) => read.date(),
        }
    }
    pub(crate) fn opens_at(&self) -> Timestamp {
        match self {
            Self::Live(read) => read.opens_at(),
            Self::Retained(read) => read.opens_at(),
        }
    }
    pub(crate) fn closes_at_exclusive(&self) -> Timestamp {
        match self {
            Self::Live(read) => read.closes_at_exclusive(),
            Self::Retained(read) => read.closes_at_exclusive(),
        }
    }
    pub(crate) fn evidence_digest(&self) -> EvidenceDigest {
        match self {
            Self::Live(read) => read.evidence_digest(),
            Self::Retained(read) => read.evidence_digest(),
        }
    }
    pub(crate) fn provider_period(&self) -> Option<&market_squawk_domain::BarTimeSemantics> {
        match self {
            Self::Live(read) => read.provider_period(),
            Self::Retained(read) => read.provider_period(),
        }
    }
}
#[derive(Clone)]
enum SourcePlanCalendarReader {
    Live(crate::application::market_calendar::CompletedMarketSessionReadCapability),
    Retained(Arc<crate::application::market_calendar::RetainedMarketSessionReadCapability>),
}
impl SourcePlanCalendarReader {
    async fn read_reference_with_job_context(
        &self,
        reference: &crate::application::market_calendar::CompletedMarketSessionReference,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<
        Option<SourcePlanCalendar>,
        crate::application::market_calendar::CompletedMarketSessionError,
    > {
        match self {
            Self::Live(reader) => reader
                .read_reference_with_job_context(reference, as_of, deadline, cancellation, job)
                .await
                .map(|read| read.map(SourcePlanCalendar::Live)),
            Self::Retained(reader) => reader
                .read_reference_with_job_context(reference, as_of, deadline, cancellation, job)
                .await
                .map(|read| read.map(SourcePlanCalendar::Retained)),
        }
    }
}

/// An opaque projection of the exact observed source query, not a claim that every action in the
/// world or every process-date history was available. The full query and outside-window rows
/// remain retained. A real empty query may produce an empty economic plan but cannot certify
/// an unrelated historical effective interval.
pub(crate) struct SourceAppliedCorporateActionPlan {
    source: Arc<CorporateActionSourceSnapshot>,
    query_identity: CorporateActionQueryIdentitySelection,
    calendar: SourcePlanCalendar,
    interval: (CalendarDate, CalendarDate),
    requested_instruments: Box<[InstrumentId]>,
    outside_window: Box<[SourceIdentifier]>,
    unresolved: Box<[ApplicableActionGap]>,
    plan: CorporateActionPlan,
    evaluated_at: Timestamp,
    payment_policy: CorporateActionPaymentPolicy,
    ordinary: Option<OrdinaryActionCoverage>,
    current_ordinary: Option<current_ordinary::CurrentOrdinaryCoverage>,
    // The two retained cursors belong to an optional anchor, not every by-value plan/result.
    anchor: Option<Box<anchor::AlpacaOriginAdjustmentAnchor>>,
}

impl SourceAppliedCorporateActionPlan {
    /// Reuses the existing planner after joining exact immutable source and calendar receipts.
    /// Selection/reopening of each source belongs to its existing owner; this method fetches
    /// nothing and accepts no caller-authored action list or completeness flag.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_from_read(
        source: Arc<CorporateActionSourceSnapshot>,
        query_identity: CorporateActionQueryIdentitySelection,
        calendar: impl Into<SourcePlanCalendar>,
        requested_instruments: BTreeSet<InstrumentId>,
        interval: (CalendarDate, CalendarDate),
        policy: CorporateActionPolicy,
        payment_policy: CorporateActionPaymentPolicy,
        valuation_cutoff: Timestamp,
        evaluated_at: Timestamp,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        let calendar = calendar.into();
        // Native event dates and retained catalog timestamps are separate evidence. Reopen
        // their original relationship through the genuine calendar before admitting the plan,
        // including identities for rows outside the requested economic projection window.
        for (date, identity) in source.event_identity_dates() {
            check(deadline, cancellation)?;
            let selection = &identity.selection;
            let session = calendar
                .date_session_on(date, selection.knowledge_at, selection.knowledge_at)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            let definition = query_identity
                .event_definition(identity)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            let validity = definition.definition().effective_interval();
            if validity.starts_at() > session.opens_at()
                || validity
                    .ends_at()
                    .is_some_and(|end| end < session.closes_at_exclusive())
                || selection.knowledge_at > source.knowledge_cutoff()
                || selection.effective_at != session.opens_at()
                || selection.provider_identity_validity.starts_at() > session.opens_at()
                || selection
                    .provider_identity_validity
                    .ends_at()
                    .is_some_and(|end| end < session.closes_at_exclusive())
            {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
        }
        let plan = CorporateActionPlan::try_from_source_reads(
            &source,
            &query_identity,
            calendar.source_action_calendar(),
            &[],
            &requested_instruments,
            interval,
            policy,
            payment_policy,
            valuation_cutoff,
            evaluated_at,
            limits,
            deadline,
            cancellation,
        )
        .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        let coverage = plan
            .source_coverage()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let outside_window = coverage.outside_window().to_vec();
        let unresolved = coverage.unresolved().to_vec();
        Ok(Self {
            source,
            query_identity,
            calendar,
            interval,
            requested_instruments: requested_instruments.into_iter().collect(),
            outside_window: outside_window.into_boxed_slice(),
            unresolved: unresolved.into_boxed_slice(),
            plan,
            evaluated_at,
            payment_policy,
            ordinary: None,
            current_ordinary: None,
            anchor: None,
        })
    }

    /// Gives accounting the existing plan only when every returned applicable source event is
    /// resolved. Consumers must separately enforce their required source-query/history policy.
    pub(crate) fn accounting_plan(
        &self,
    ) -> Result<&CorporateActionPlan, ApplicableActionPlanError> {
        if self
            .ordinary
            .as_ref()
            .is_some_and(|coverage| !coverage.gaps().is_empty())
            || !self.unresolved.is_empty()
            || !self.plan.conflicts().is_empty()
            || !self.plan.exclusions().is_empty()
        {
            return Err(ApplicableActionPlanError::UnresolvedApplicableActions);
        }
        Ok(&self.plan)
    }
    pub(crate) fn source(&self) -> &CorporateActionSourceSnapshot {
        &self.source
    }
    pub(crate) const fn query_identity(&self) -> &CorporateActionQueryIdentitySelection {
        &self.query_identity
    }
    pub(crate) const fn calendar(&self) -> &SourcePlanCalendar {
        &self.calendar
    }
    pub(crate) const fn interval(&self) -> (CalendarDate, CalendarDate) {
        self.interval
    }
    pub(crate) fn requested_instruments(&self) -> &[InstrumentId] {
        &self.requested_instruments
    }
    pub(crate) fn outside_window(&self) -> &[SourceIdentifier] {
        &self.outside_window
    }
    pub(crate) fn unresolved(&self) -> &[ApplicableActionGap] {
        &self.unresolved
    }
    pub(crate) fn source_receipt_digest(&self) -> EvidenceDigest {
        self.source.receipt_digest()
    }
}

/// Inert durable identity. Decoding does not recreate source/calendar read authority.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(
    try_from = "ApplicablePlanReferenceWire",
    into = "ApplicablePlanReferenceWire"
)]
pub struct SourceAppliedCorporateActionPlanReference(ApplicablePlanReferenceWire);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicablePlanReferenceWire {
    version: u16,
    source_origin_content: EvidenceDigest,
    source_binding: EvidenceDigest,
    source_snapshot: EvidenceDigest,
    calendar: crate::application::market_calendar::CompletedMarketSessionReference,
    requested_instruments: Vec<InstrumentId>,
    interval: (CalendarDate, CalendarDate),
    knowledge_cutoff: Timestamp,
    valuation_cutoff: Timestamp,
    evaluated_at: Timestamp,
    adjustment: u8,
    policy_version: u32,
    payment_policy: CorporateActionPaymentPolicy,
    content_hash: EvidenceDigest,
    audit_hash: EvidenceDigest,
    ordinary: Vec<ordinary::OrdinaryHistoryReference>,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    current_ordinary: Option<current_ordinary::CurrentOrdinaryRecipeReference>,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    current_ordinary_digest: Option<EvidenceDigest>,
    ordinary_coverage_digest: Option<EvidenceDigest>,
    anchor: Option<anchor::AlpacaOriginAdjustmentAnchorReference>,
}
impl TryFrom<ApplicablePlanReferenceWire> for SourceAppliedCorporateActionPlanReference {
    type Error = &'static str;
    fn try_from(wire: ApplicablePlanReferenceWire) -> Result<Self, Self::Error> {
        if wire.version != 1
            || wire
                .anchor
                .as_ref()
                .is_some_and(|anchor| !anchor.bounded_page_clocks())
            || (wire.current_ordinary.is_some()
                && (wire.anchor.is_some()
                    || wire
                        .ordinary
                        .iter()
                        .any(|reference| reference.timestamped.is_some())))
            || (wire.current_ordinary.is_none() != wire.current_ordinary_digest.is_none())
            || wire
                .current_ordinary
                .as_ref()
                .is_some_and(|reference| !reference.valid())
            || wire.current_ordinary_digest.is_some_and(|digest| {
                digest.algorithm() != market_squawk_domain::DigestAlgorithm::Sha256
                    || digest.bytes() == [0; 32]
            })
            || wire.ordinary.len() > 32
            || (wire.ordinary.is_empty() != wire.ordinary_coverage_digest.is_none())
            || wire.ordinary_coverage_digest.is_some_and(|digest| {
                digest.algorithm() != market_squawk_domain::DigestAlgorithm::Sha256
                    || digest.bytes() == [0; 32]
            })
            || wire.adjustment > 2
            || wire.policy_version == 0
            || wire.requested_instruments.is_empty()
            || wire.requested_instruments.len() > 32
            || wire
                .requested_instruments
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || wire.interval.0 > wire.interval.1
            || wire.evaluated_at < wire.knowledge_cutoff
            || wire.valuation_cutoff > wire.knowledge_cutoff
            || [
                wire.source_origin_content,
                wire.source_binding,
                wire.source_snapshot,
                wire.content_hash,
                wire.audit_hash,
            ]
            .into_iter()
            .any(|digest| {
                digest.algorithm() != market_squawk_domain::DigestAlgorithm::Sha256
                    || digest.bytes() == [0; 32]
            })
        {
            return Err("corporate-action plan reference is invalid");
        }
        Ok(Self(wire))
    }
}
impl SourceAppliedCorporateActionPlanReference {
    /// Exact controlled source-recipe dependency for existing backup/restore traversal.
    pub(crate) fn current_recipe_artifact(
        &self,
    ) -> Result<Option<market_squawk_services::ArtifactReference>, ApplicableActionPlanError> {
        self.0
            .current_ordinary
            .as_ref()
            .map(current_ordinary::CurrentOrdinaryRecipeReference::artifact)
            .transpose()
    }
    /// Original source snapshot cutoff. This inert coordinate grants no read authority.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.0.knowledge_cutoff
    }
    /// Original economic projection cutoff, distinct from source acquisition time.
    pub const fn valuation_cutoff(&self) -> Timestamp {
        self.0.valuation_cutoff
    }
    /// Source-known native economic date interval retained by the original producer.
    pub const fn interval(&self) -> (CalendarDate, CalendarDate) {
        self.0.interval
    }
    /// Exact canonical subjects, including genuine zero-event source query selections.
    pub fn requested_instruments(&self) -> &[InstrumentId] {
        &self.0.requested_instruments
    }
}
impl From<SourceAppliedCorporateActionPlanReference> for ApplicablePlanReferenceWire {
    fn from(value: SourceAppliedCorporateActionPlanReference) -> Self {
        value.0
    }
}
impl SourceAppliedCorporateActionPlan {
    /// Creates a durable reference only from an accounting-usable joined projection. The source
    /// snapshot reference remains mandatory even for a genuinely empty response.
    pub(crate) fn reference(
        &self,
    ) -> Result<SourceAppliedCorporateActionPlanReference, ApplicableActionPlanError> {
        if self.ordinary.is_some() || self.current_ordinary.is_some() {
            self.covered_accounting_plan()?;
        } else {
            self.accounting_plan()?;
        }
        self.source_reference()
    }

    /// Moves the already admitted financial plan to its consumer. Its private shared source
    /// marker is retained; no action decoding or secondary authority constructor is involved.
    pub(crate) fn into_covered_accounting_plan(
        self,
    ) -> Result<CorporateActionPlan, ApplicableActionPlanError> {
        self.covered_accounting_plan()?;
        Ok(self.plan)
    }

    /// Retains exact source reconstruction coordinates even when a missing cash unit prevents
    /// accounting. This value is not a financial-coverage or unit-continuity authority.
    pub(crate) fn source_reference(
        &self,
    ) -> Result<SourceAppliedCorporateActionPlanReference, ApplicableActionPlanError> {
        let plan = &self.plan;
        if self
            .current_ordinary
            .as_ref()
            .is_some_and(|coverage| coverage.recipe.is_none())
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        use market_squawk_data::CorporateActionAdjustment;
        let digest =
            |bytes| EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256, bytes);
        SourceAppliedCorporateActionPlanReference::try_from(ApplicablePlanReferenceWire {
            version: 1,
            source_origin_content: digest(self.source.manifest().content_hash().bytes()),
            source_binding: self.source.binding_digest(),
            source_snapshot: self.source.receipt_digest(),
            calendar: self.calendar.reference().clone(),
            requested_instruments: self.requested_instruments.to_vec(),
            interval: self.interval,
            knowledge_cutoff: plan.knowledge_cutoff(),
            valuation_cutoff: plan.valuation_cutoff(),
            evaluated_at: self.evaluated_at,
            adjustment: match plan.policy().adjustment() {
                CorporateActionAdjustment::Raw => 0,
                CorporateActionAdjustment::SplitAdjusted => 1,
                CorporateActionAdjustment::TotalReturn => 2,
            },
            policy_version: plan.policy().version().get(),
            payment_policy: self.payment_policy,
            content_hash: digest(plan.content_hash().bytes()),
            audit_hash: digest(plan.audit_hash().bytes()),
            ordinary: self.ordinary_references(),
            current_ordinary: self
                .current_ordinary
                .as_ref()
                .and_then(|coverage| coverage.recipe.clone()),
            current_ordinary_digest: self
                .current_ordinary
                .as_ref()
                .map(|coverage| coverage.digest),
            ordinary_coverage_digest: self.ordinary.as_ref().map(OrdinaryActionCoverage::digest),
            anchor: self
                .anchor
                .as_deref()
                .map(anchor::AlpacaOriginAdjustmentAnchor::reference)
                .transpose()?,
        })
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)
    }
}

/// Composes existing source/calendar readers. It owns no registry, cache, publisher or selector.
#[derive(Clone)]
pub(crate) struct SourceAppliedCorporateActionReadCapability {
    research: Arc<crate::ResearchService>,
    calendars: SourcePlanCalendarReader,
    artifacts: Option<Arc<dyn market_squawk_services::ArtifactRepository>>,
}
impl SourceAppliedCorporateActionReadCapability {
    pub(crate) fn new(
        research: Arc<crate::ResearchService>,
        calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    ) -> Self {
        Self {
            research,
            calendars: SourcePlanCalendarReader::Live(calendars),
            artifacts: None,
        }
    }
    /// Reopens the original exact source and calendar roots and recomputes every plan identity.
    pub(crate) async fn read_reference(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<SourceAppliedCorporateActionPlan>, ApplicableActionPlanError> {
        self.read_reference_with_job_context(reference, deadline, cancellation, None)
            .await
    }

    pub(crate) async fn read_reference_for_job(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        deadline: Instant,
        cancellation: CancellationToken,
        job: &market_squawk_jobs::JobRunContext,
    ) -> Result<Option<SourceAppliedCorporateActionPlan>, ApplicableActionPlanError> {
        self.read_reference_with_job_context(reference, deadline, cancellation, Some(job))
            .await
    }

    pub(crate) async fn read_reference_with_job_context(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<SourceAppliedCorporateActionPlan>, ApplicableActionPlanError> {
        check(deadline, &cancellation)?;
        let wire = &reference.0;
        let manifest = self
            .research
            .analytical_reader()
            .provider_capture_origin(
                wire.source_binding,
                market_squawk_data::Sha256Digest::new(wire.source_origin_content.bytes()),
                wire.knowledge_cutoff,
                deadline,
                &cancellation,
            )
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };
        let generation = self
            .research
            .read_provider_capture_generation_with_job_context(
                job,
                manifest,
                deadline,
                &cancellation,
                |generation, _, _, _, _| Ok(generation.clone()),
            )
            .await
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
        let source = self
            .research
            .analytical_reader()
            .read_corporate_action_source_snapshot(
                &generation,
                wire.knowledge_cutoff,
                deadline,
                cancellation.clone(),
            )
            .await
            .map_err(|error| map_source_read_error(error, deadline, &cancellation))?;
        if source.receipt_digest() != wire.source_snapshot
            || source.binding_digest() != wire.source_binding
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let query_identity = self
            .research
            .market_data_instruments()
            .reopen_corporate_action_query_identities(&source, deadline, &cancellation)
            .map_err(|error| {
                ApplicableActionPlanError::SourceRead(map_query_identity_error(error))
            })?;
        let Some(calendar) = self
            .calendars
            .read_reference_with_job_context(
                &wire.calendar,
                wire.knowledge_cutoff,
                deadline,
                cancellation.clone(),
                job,
            )
            .await
            .map_err(|error| ApplicableActionPlanError::SourceRead(map_calendar_error(error)))?
        else {
            return Ok(None);
        };
        use market_squawk_data::CorporateActionAdjustment;
        let adjustment = match wire.adjustment {
            0 => CorporateActionAdjustment::Raw,
            1 => CorporateActionAdjustment::SplitAdjusted,
            2 => CorporateActionAdjustment::TotalReturn,
            _ => return Err(ApplicableActionPlanError::InvalidEvidence),
        };
        let limits = CorporateActionLimits::try_new(
            std::num::NonZeroUsize::new(16_000)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
            std::num::NonZeroUsize::new(64 * 1024 * 1024)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let initial_valuation = if wire
            .ordinary
            .iter()
            .any(|reference| reference.timestamped.is_some())
        {
            calendar
                .source_action_calendar()
                .native_session_bounds(wire.interval)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?
                .1
        } else {
            wire.valuation_cutoff
        };
        let plan = SourceAppliedCorporateActionPlan::try_from_read(
            Arc::new(source),
            query_identity,
            calendar,
            wire.requested_instruments.iter().copied().collect(),
            wire.interval,
            CorporateActionPolicy::new(
                adjustment,
                std::num::NonZeroU32::new(wire.policy_version)
                    .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
            ),
            wire.payment_policy,
            initial_valuation,
            wire.evaluated_at,
            limits,
            deadline,
            &cancellation,
        )?;
        let ordinary_reads = if wire.ordinary.is_empty() {
            Vec::new()
        } else {
            self.reopen_ordinary(
                &wire.ordinary,
                wire.knowledge_cutoff,
                deadline,
                &cancellation,
                job,
            )
            .await?
        };
        let plan = if let Some(recipe) = &wire.current_ordinary {
            let reads = self
                .reopen_current_recipe(recipe, wire, deadline, &cancellation, job)
                .await?;
            let mut plan = if ordinary_reads.is_empty() {
                plan.with_current_ordinary_reads(reads, limits, deadline, &cancellation)?
            } else {
                plan.with_hybrid_ordinary_reads(
                    ordinary_reads,
                    reads,
                    limits,
                    deadline,
                    &cancellation,
                )?
            };
            let coverage = plan
                .current_ordinary
                .as_mut()
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            coverage.recipe = Some(recipe.clone());
            coverage.references = Vec::new();
            plan
        } else if ordinary_reads.is_empty() {
            plan
        } else {
            plan.with_complete_ordinary_history(ordinary_reads, limits, deadline, &cancellation)?
        };
        let plan = if wire
            .ordinary
            .iter()
            .any(|reference| reference.timestamped.is_some())
        {
            let proofs = self
                .reopen_timestamp_price_proofs(
                    &wire.ordinary,
                    wire.knowledge_cutoff,
                    deadline,
                    &cancellation,
                    job,
                )
                .await?;
            plan.with_timestamp_price_proofs(proofs, limits, deadline, &cancellation)?
        } else {
            plan
        };
        let plan = if let Some(anchor) = &wire.anchor {
            let (raw, split) = self
                .reopen_anchor(anchor, wire.knowledge_cutoff, deadline, &cancellation, job)
                .await?;
            self.with_fresh_alpaca_split_anchor_with_job_context(
                plan,
                raw,
                split,
                deadline,
                &cancellation,
                job,
            )
            .await?
        } else {
            plan
        };
        if plan.source_reference()? != *reference {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        check(deadline, &cancellation)?;
        Ok(Some(plan))
    }
}

fn check(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ApplicableActionPlanError> {
    if cancellation.is_cancelled() {
        Err(ApplicableActionPlanError::SourceRead(
            ServiceError::Cancelled,
        ))
    } else if Instant::now() >= deadline {
        Err(ApplicableActionPlanError::SourceRead(
            ServiceError::DeadlineExceeded,
        ))
    } else {
        Ok(())
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum ApplicableActionPlanError {
    InvalidEvidence,
    SourceRead(market_squawk_services::ServiceError),
    UnresolvedApplicableActions,
    IncompleteOrdinaryCoverage,
    Interrupted,
}

fn map_source_plan_error(
    error: market_squawk_data::CorporateActionError,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> ApplicableActionPlanError {
    use market_squawk_data::CorporateActionError as E;
    match error {
        E::SourceReadInterrupted => {
            check(deadline, cancellation)
                .err()
                .unwrap_or(ApplicableActionPlanError::SourceRead(
                    ServiceError::Internal,
                ))
        }
        E::InvalidLimits
        | E::ActionLimitExceeded { .. }
        | E::RetainedByteLimitExceeded { .. }
        | E::RetainedSizeOverflow
        | E::AllocationFailed
        | E::CanonicalEncodingOverflow => {
            ApplicableActionPlanError::SourceRead(ServiceError::ResourceExhausted)
        }
        E::InvalidApplication | E::RecoveryCodec | E::MissingInstrument => {
            ApplicableActionPlanError::InvalidEvidence
        }
    }
}

fn map_source_read_error(
    error: market_squawk_data::CorporateActionSourceReadError,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> ApplicableActionPlanError {
    use market_squawk_data::{CorporateActionSourceReadError as E, IngestError};
    let error = match error {
        E::Interrupted => {
            return check(deadline, cancellation).err().unwrap_or(
                ApplicableActionPlanError::SourceRead(ServiceError::Internal),
            );
        }
        E::InvalidEvidence | E::Arrow(_) => ServiceError::InvalidResult,
        E::FutureEvidence => ServiceError::Unavailable,
        E::ResourceBound => ServiceError::ResourceExhausted,
        E::Manifest(error) => map_ingest_error(IngestError::Manifest(error)),
        E::Parquet(error) => map_ingest_error(IngestError::Parquet(error)),
    };
    ApplicableActionPlanError::SourceRead(error)
}

// The generic research query mapper treats some nested failures as source absence. Preserve
// typed source replay failures here while reusing existing nested source-owner classifications.
