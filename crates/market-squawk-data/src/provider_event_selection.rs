//! Bounded provider-neutral point-in-time selection over immutable market-event publications.

use std::sync::Arc;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, LiveEventClass, LiveEvidenceScope, MarketEvent,
    ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::catalog::CatalogReadSnapshot;
use crate::{
    CatalogError, DatasetId, ManifestCatalogError, MarketEventCommitRef,
    PersistedProviderPublicationEvidence, ProviderMarketEventArrowBatch,
    ProviderMarketEventPublicationKind,
};

const SELECTION_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/provider-market-event-point-in-time-selection/v1";

/// Maximum exact event rows one point-in-time request may retain across source surfaces.
pub const MAX_PROVIDER_MARKET_EVENT_POINT_IN_TIME_CANDIDATES: usize = 256;

/// Explicit event-time clock applied to the as-of cutoff.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderMarketEventEffectiveTimeBasis {
    /// Use the provider-authored source timestamp and exclude rows where it is absent.
    SourceTimestamp,
    /// Use the local socket-boundary receive timestamp.
    ReceivedAt,
}

/// Tie handling after selecting each source's newest eligible effective time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderMarketEventTiePolicy {
    /// Retain every exact row at that effective time for historical point-in-time analysis.
    AllNewestEffectiveTimeTies,
    /// Retain only the newest local receive observation, preserving every equal-receive tie.
    LatestReceivedObservation,
}

/// Bounded immutable provider-market-event point-in-time request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventPointInTimeRequest {
    dataset: DatasetId,
    scope: LiveEvidenceScope,
    venue_id: VenueId,
    event_kind: LiveEventClass,
    as_of_cutoff: Timestamp,
    knowledge_cutoff: Timestamp,
    effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
    maximum_candidates: usize,
    tie_policy: ProviderMarketEventTiePolicy,
    exact_commit: Option<MarketEventCommitRef>,
    exact_source_surface: Option<SourceId>,
    exact_product: Option<ProviderProduct>,
    exact_channel: Option<ProviderChannel>,
}

impl ProviderMarketEventPointInTimeRequest {
    /// Builds a request that resolves the newest eligible logical commit at the cutoff.
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact financial key, clocks, basis, and work ceiling remain explicit"
    )]
    pub fn try_latest(
        dataset: DatasetId,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        event_kind: LiveEventClass,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        exact_source_surface: Option<SourceId>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        Self::try_new(
            dataset,
            instrument_id,
            venue_id,
            event_kind,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            None,
            exact_source_surface,
        )
    }

    /// Builds a request pinned to one complete logical commit.
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact financial key, clocks, basis, and work ceiling remain explicit"
    )]
    pub fn try_exact(
        dataset: DatasetId,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        event_kind: LiveEventClass,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        exact_commit: MarketEventCommitRef,
        exact_source_surface: Option<SourceId>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        Self::try_new(
            dataset,
            instrument_id,
            venue_id,
            event_kind,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            Some(exact_commit),
            exact_source_surface,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exact financial key, clocks, basis, and work ceiling remain explicit"
    )]
    fn try_new(
        dataset: DatasetId,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        event_kind: LiveEventClass,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        exact_commit: Option<MarketEventCommitRef>,
        exact_source_surface: Option<SourceId>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        if event_kind == LiveEventClass::Screener {
            return Err(ProviderMarketEventSelectionError::InvalidRequest);
        }
        Self::try_scoped(
            dataset,
            LiveEvidenceScope::Instrument(instrument_id),
            venue_id,
            event_kind,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            exact_commit,
            exact_source_surface,
            None,
            None,
        )
    }

    /// Selects one exact source cohort from the newest eligible logical commit.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact cohort scope and bounds are mandatory"
    )]
    pub fn try_cohort_latest(
        dataset: DatasetId,
        cohort_key: SourceIdentifier,
        venue_id: VenueId,
        source: SourceId,
        product: ProviderProduct,
        channel: ProviderChannel,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        Self::try_scoped(
            dataset,
            LiveEvidenceScope::SourceCohort(cohort_key),
            venue_id,
            LiveEventClass::Screener,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            None,
            Some(source),
            Some(product),
            Some(channel),
        )
    }

    /// Pins an exact source cohort to an immutable commit, including genuinely empty cohorts.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact cohort scope and bounds are mandatory"
    )]
    pub fn try_cohort_exact(
        dataset: DatasetId,
        cohort_key: SourceIdentifier,
        venue_id: VenueId,
        source: SourceId,
        product: ProviderProduct,
        channel: ProviderChannel,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        exact_commit: MarketEventCommitRef,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        Self::try_scoped(
            dataset,
            LiveEvidenceScope::SourceCohort(cohort_key),
            venue_id,
            LiveEventClass::Screener,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            Some(exact_commit),
            Some(source),
            Some(product),
            Some(channel),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "closed selection identity remains atomic"
    )]
    fn try_scoped(
        dataset: DatasetId,
        scope: LiveEvidenceScope,
        venue_id: VenueId,
        event_kind: LiveEventClass,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        exact_commit: Option<MarketEventCommitRef>,
        exact_source_surface: Option<SourceId>,
        exact_product: Option<ProviderProduct>,
        exact_channel: Option<ProviderChannel>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        if matches!(&scope, LiveEvidenceScope::SourceCohort(_))
            != (event_kind == LiveEventClass::Screener)
            || (matches!(&scope, LiveEvidenceScope::SourceCohort(_))
                && (exact_source_surface.is_none()
                    || exact_product.is_none()
                    || exact_channel.is_none()))
        {
            return Err(ProviderMarketEventSelectionError::InvalidRequest);
        }
        if maximum_candidates == 0
            || maximum_candidates > MAX_PROVIDER_MARKET_EVENT_POINT_IN_TIME_CANDIDATES
            || exact_commit
                .as_ref()
                .is_some_and(|commit| commit.dataset_id() != &dataset)
        {
            return Err(ProviderMarketEventSelectionError::InvalidRequest);
        }
        if let Some(commit) = exact_commit.as_ref() {
            let registered = crate::schema::DatasetSchemaRegistry::local()
                .canonical_market_events()
                .map_err(|_| ProviderMarketEventSelectionError::InvalidRequest)?;
            if commit.schema() != &registered {
                return Err(ProviderMarketEventSelectionError::InvalidRequest);
            }
        }
        Ok(Self {
            dataset,
            scope,
            venue_id,
            event_kind,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            tie_policy: ProviderMarketEventTiePolicy::AllNewestEffectiveTimeTies,
            exact_commit,
            exact_source_surface,
            exact_product,
            exact_channel,
        })
    }

    /// Returns the exact analytical dataset identity.
    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    /// Returns the internal canonical instrument identity.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        match &self.scope {
            LiveEvidenceScope::Instrument(id) => Some(*id),
            LiveEvidenceScope::SourceCohort(_) => None,
        }
    }

    /// Returns the closed instrument or source-cohort scope.
    pub const fn scope(&self) -> &LiveEvidenceScope {
        &self.scope
    }

    /// Returns the exact venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the required canonical event class.
    pub const fn event_kind(&self) -> LiveEventClass {
        self.event_kind
    }

    /// Returns the cutoff applied to the selected effective-time basis.
    pub const fn as_of_cutoff(&self) -> Timestamp {
        self.as_of_cutoff
    }

    /// Returns the maximum local-knowledge time admitted by this request.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the explicitly selected effective-time basis.
    pub const fn effective_time_basis(&self) -> ProviderMarketEventEffectiveTimeBasis {
        self.effective_time_basis
    }

    /// Sets the explicit tie policy without changing either cutoff or effective-time clock.
    #[must_use]
    pub const fn with_tie_policy(mut self, policy: ProviderMarketEventTiePolicy) -> Self {
        self.tie_policy = policy;
        self
    }

    /// Returns the digest-bound policy applied within each newest effective-time cohort.
    pub const fn tie_policy(&self) -> ProviderMarketEventTiePolicy {
        self.tie_policy
    }

    /// Returns the complete cross-source candidate ceiling.
    pub const fn maximum_candidates(&self) -> usize {
        self.maximum_candidates
    }

    /// Returns the caller-selected logical commit when supplied.
    pub const fn exact_commit(&self) -> Option<&MarketEventCommitRef> {
        self.exact_commit.as_ref()
    }

    /// Returns the exact source surface when provider selection occurred upstream.
    pub const fn exact_source_surface(&self) -> Option<&SourceId> {
        self.exact_source_surface.as_ref()
    }

    /// Returns the exact cohort key; instrument requests never match a cohort.
    pub fn cohort_key(&self) -> Option<&SourceIdentifier> {
        match &self.scope {
            LiveEvidenceScope::SourceCohort(key) => Some(key),
            LiveEvidenceScope::Instrument(_) => None,
        }
    }

    /// Returns the mandatory product of a cohort request.
    pub const fn exact_product(&self) -> Option<&ProviderProduct> {
        self.exact_product.as_ref()
    }

    /// Returns the mandatory channel of a cohort request.
    pub const fn exact_channel(&self) -> Option<&ProviderChannel> {
        self.exact_channel.as_ref()
    }

    fn restart_request(
        &self,
        commit: MarketEventCommitRef,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        Self::try_scoped(
            self.dataset.clone(),
            self.scope.clone(),
            self.venue_id.clone(),
            self.event_kind,
            self.as_of_cutoff,
            self.knowledge_cutoff,
            self.effective_time_basis,
            self.maximum_candidates,
            Some(commit),
            self.exact_source_surface.clone(),
            self.exact_product.clone(),
            self.exact_channel.clone(),
        )
        .map(|request| request.with_tie_policy(self.tie_policy))
    }
}

/// Exact immutable provider publication selected for one row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventExactPublication {
    digest: EvidenceDigest,
    kind: ProviderMarketEventPublicationKind,
}

impl ProviderMarketEventExactPublication {
    pub(crate) const fn from_catalog(
        digest: EvidenceDigest,
        kind: ProviderMarketEventPublicationKind,
    ) -> Self {
        Self { digest, kind }
    }

    /// Returns the exact kind-qualified publication digest.
    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }

    /// Returns the closed durable publication kind.
    pub const fn kind(self) -> ProviderMarketEventPublicationKind {
        self.kind
    }
}

/// Response or stream component carrying one row of a possibly composite publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderMarketEventComponentKind {
    /// Canonical event decoded from a sealed provider response.
    Response,
    /// Canonical event decoded from a sealed live-event microbatch.
    Stream,
}

/// Exact durable catalog row coordinate for one selected typed event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventSelectionCoordinate {
    publication: ProviderMarketEventExactPublication,
    publication_row_ordinal: u32,
    component_kind: ProviderMarketEventComponentKind,
    component_binding_digest: EvidenceDigest,
    component_row_ordinal: u32,
    canonical_event_digest: EvidenceDigest,
    source_surface: SourceId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    scope: LiveEvidenceScope,
    venue_id: VenueId,
    event_kind: LiveEventClass,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    connection_generation: u64,
    source_sequence: Option<u64>,
    provider_event_id: SourceIdentifier,
    coordinate_digest: EvidenceDigest,
    origin_committed_at: Timestamp,
}

impl ProviderMarketEventSelectionCoordinate {
    /// Returns the exact publication identity.
    pub const fn publication(&self) -> ProviderMarketEventExactPublication {
        self.publication
    }

    /// Returns the row ordinal in the complete publication batch.
    pub const fn publication_row_ordinal(&self) -> u32 {
        self.publication_row_ordinal
    }

    /// Returns the response or stream component kind.
    pub const fn component_kind(&self) -> ProviderMarketEventComponentKind {
        self.component_kind
    }

    /// Returns the exact component binding digest.
    pub const fn component_binding_digest(&self) -> EvidenceDigest {
        self.component_binding_digest
    }

    /// Returns the canonical row ordinal within the component.
    pub const fn component_row_ordinal(&self) -> u32 {
        self.component_row_ordinal
    }

    /// Returns the canonical typed-event JSON digest.
    pub const fn canonical_event_digest(&self) -> EvidenceDigest {
        self.canonical_event_digest
    }

    /// Returns the exact source surface represented by the row.
    pub const fn source_surface(&self) -> &SourceId {
        &self.source_surface
    }

    /// Returns the internal canonical instrument identity.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        match &self.scope {
            LiveEvidenceScope::Instrument(id) => Some(*id),
            LiveEvidenceScope::SourceCohort(_) => None,
        }
    }

    /// Returns the closed instrument or source-cohort scope.
    pub const fn scope(&self) -> &LiveEvidenceScope {
        &self.scope
    }

    /// Returns the exact venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the canonical event class.
    pub const fn event_kind(&self) -> LiveEventClass {
        self.event_kind
    }

    /// Returns the provider-authored timestamp when supplied.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns the local socket-boundary receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when this row became locally knowable.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the immutable ingestion time.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the exact nonzero connection generation.
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    /// Returns the provider sequence when supplied.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    /// Returns the provider-native event or record identity.
    pub const fn provider_event_id(&self) -> &SourceIdentifier {
        &self.provider_event_id
    }

    /// Returns the catalog coordinate-record digest.
    pub const fn coordinate_digest(&self) -> EvidenceDigest {
        self.coordinate_digest
    }

    /// Returns when the publication first entered an logical event commit.
    pub const fn origin_committed_at(&self) -> Timestamp {
        self.origin_committed_at
    }
}

/// One selected event with exact reconstructed Parquet and provider-native evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventSelectedCandidate {
    coordinate: ProviderMarketEventSelectionCoordinate,
    event: MarketEvent,
    evidence: Arc<PersistedProviderPublicationEvidence>,
}

impl ProviderMarketEventSelectedCandidate {
    /// Returns the exact immutable row coordinate.
    pub const fn coordinate(&self) -> &ProviderMarketEventSelectionCoordinate {
        &self.coordinate
    }

    /// Returns the reconstructed typed canonical event.
    pub const fn event(&self) -> &MarketEvent {
        &self.event
    }

    /// Returns complete persisted raw and provider-native publication evidence.
    pub fn evidence(&self) -> &PersistedProviderPublicationEvidence {
        &self.evidence
    }
}

/// Newest effective-time candidates retained by the request tie policy for one exact source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventSourceSelection {
    source_surface: SourceId,
    effective_at: Timestamp,
    tied_candidates: Box<[ProviderMarketEventSelectedCandidate]>,
}

impl ProviderMarketEventSourceSelection {
    /// Returns the exact source surface. The data layer does not rank this against other sources.
    pub const fn source_surface(&self) -> &SourceId {
        &self.source_surface
    }

    /// Returns the newest admissible effective time for this source.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns every policy-admitted tie in deterministic evidence order.
    pub fn tied_candidates(&self) -> &[ProviderMarketEventSelectedCandidate] {
        &self.tied_candidates
    }
}

/// Bounded exclusion counts retained with a selection receipt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProviderMarketEventExclusionCounts {
    missing_source_timestamp: u64,
    after_as_of: u64,
    available_after_knowledge: u64,
    ingested_after_knowledge: u64,
    origin_published_after_knowledge: u64,
    superseded_effective_time: u64,
    superseded_received_observation: u64,
}

impl ProviderMarketEventExclusionCounts {
    pub(crate) const fn from_catalog(
        missing_source_timestamp: u64,
        after_as_of: u64,
        available_after_knowledge: u64,
        ingested_after_knowledge: u64,
        origin_published_after_knowledge: u64,
        superseded_effective_time: u64,
        superseded_received_observation: u64,
    ) -> Self {
        Self {
            missing_source_timestamp,
            after_as_of,
            available_after_knowledge,
            ingested_after_knowledge,
            origin_published_after_knowledge,
            superseded_effective_time,
            superseded_received_observation,
        }
    }

    /// Returns rows excluded because source-time mode cannot fall back to receipt time.
    pub const fn missing_source_timestamp(self) -> u64 {
        self.missing_source_timestamp
    }

    /// Returns rows whose selected effective time is after the as-of cutoff.
    pub const fn after_as_of(self) -> u64 {
        self.after_as_of
    }

    /// Returns rows not available by the knowledge cutoff.
    pub const fn available_after_knowledge(self) -> u64 {
        self.available_after_knowledge
    }

    /// Returns rows not durably ingested by the knowledge cutoff.
    pub const fn ingested_after_knowledge(self) -> u64 {
        self.ingested_after_knowledge
    }

    /// Returns rows whose first immutable origin generation was published too late.
    pub const fn origin_published_after_knowledge(self) -> u64 {
        self.origin_published_after_knowledge
    }

    /// Returns otherwise eligible rows older than their source's retained newest effective time.
    pub const fn superseded_effective_time(self) -> u64 {
        self.superseded_effective_time
    }

    /// Returns newest-effective-time rows superseded by a later admitted receive observation.
    pub const fn superseded_received_observation(self) -> u64 {
        self.superseded_received_observation
    }
}

/// Completeness of the bounded candidate enumeration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderMarketEventSelectionCompleteness {
    /// The complete eligible candidate set fit inside the caller's explicit ceiling.
    Complete,
}

/// Exact immutable provider-neutral PIT result. Sources remain separate for application ranking.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMarketEventPointInTimeSelection {
    request: ProviderMarketEventPointInTimeRequest,
    commit: MarketEventCommitRef,
    commit_available_at: Timestamp,
    sources: Box<[ProviderMarketEventSourceSelection]>,
    exclusions: ProviderMarketEventExclusionCounts,
    completeness: ProviderMarketEventSelectionCompleteness,
    selection_digest: EvidenceDigest,
}

impl ProviderMarketEventPointInTimeSelection {
    /// Returns the semantic request. The resolved commit is separately retained below.
    pub const fn request(&self) -> &ProviderMarketEventPointInTimeRequest {
        &self.request
    }

    /// Returns the exact logical commit used by every selected row.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }

    /// Returns when the selected generation became queryable.
    pub const fn commit_available_at(&self) -> Timestamp {
        self.commit_available_at
    }

    /// Returns bounded latest candidates separated by exact source surface.
    pub fn sources(&self) -> &[ProviderMarketEventSourceSelection] {
        &self.sources
    }

    /// Returns exclusion counts retained as part of the digest-bound receipt.
    pub const fn exclusions(&self) -> ProviderMarketEventExclusionCounts {
        self.exclusions
    }

    /// Returns whether candidate enumeration is complete.
    pub const fn completeness(&self) -> ProviderMarketEventSelectionCompleteness {
        self.completeness
    }

    /// Returns the digest of the semantic request, commit, exclusions, ties, and evidence rows.
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    /// Returns an exact-commit request suitable for a restart replay.
    pub fn exact_restart_request(
        &self,
    ) -> Result<ProviderMarketEventPointInTimeRequest, ProviderMarketEventSelectionError> {
        self.request.restart_request(self.commit.clone())
    }

    /// Requires a restarted exact-commit replay to reproduce the complete original receipt.
    pub fn verify_restart_replay(
        &self,
        replay: &Self,
    ) -> Result<(), ProviderMarketEventSelectionError> {
        if replay.request.exact_commit() != Some(&self.commit)
            || replay.commit != self.commit
            || replay.commit_available_at != self.commit_available_at
            || replay.sources != self.sources
            || replay.exclusions != self.exclusions
            || replay.completeness != self.completeness
            || replay.selection_digest != self.selection_digest
        {
            return Err(ProviderMarketEventSelectionError::RestartMismatch);
        }
        Ok(())
    }

    pub(crate) fn try_from_reconstructed(
        request: ProviderMarketEventPointInTimeRequest,
        plan: ProviderMarketEventCatalogPlan,
        reconstructed: Vec<ProviderMarketEventSelectedCandidate>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        if reconstructed.len() != plan.candidates.len()
            || reconstructed.len() > request.maximum_candidates
            || plan.commit.dataset_id() != request.dataset()
            || plan.commit_available_at > request.knowledge_cutoff()
            || request
                .exact_commit()
                .is_some_and(|exact| exact != &plan.commit)
            || plan.candidates.windows(2).any(|pair| {
                pair[0].source_surface > pair[1].source_surface
                    || (pair[0].source_surface == pair[1].source_surface
                        && pair[0].effective_at != pair[1].effective_at)
            })
        {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        for (candidate, expected) in reconstructed.iter().zip(&plan.candidates) {
            if !expected.matches_selected(&candidate.coordinate, &request) {
                return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
            }
        }

        let mut sources = Vec::new();
        sources
            .try_reserve_exact(reconstructed.len())
            .map_err(|_| ProviderMarketEventSelectionError::Allocation)?;
        let mut rows = reconstructed.into_iter().peekable();
        while let Some(first) = rows.next() {
            let source_surface = first.coordinate.source_surface.clone();
            let effective_at = effective_time(&first.coordinate, request.effective_time_basis)?;
            let mut tied = Vec::new();
            tied.try_reserve_exact(
                plan.candidates
                    .iter()
                    .filter(|candidate| candidate.source_surface == source_surface)
                    .count(),
            )
            .map_err(|_| ProviderMarketEventSelectionError::Allocation)?;
            tied.push(first);
            while rows
                .peek()
                .is_some_and(|candidate| candidate.coordinate.source_surface == source_surface)
            {
                let candidate = rows
                    .next()
                    .ok_or(ProviderMarketEventSelectionError::EvidenceMismatch)?;
                if effective_time(&candidate.coordinate, request.effective_time_basis)?
                    != effective_at
                    || (request.tie_policy
                        == ProviderMarketEventTiePolicy::LatestReceivedObservation
                        && candidate.coordinate.received_at != tied[0].coordinate.received_at)
                {
                    return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
                }
                tied.push(candidate);
            }
            sources.push(ProviderMarketEventSourceSelection {
                source_surface,
                effective_at,
                tied_candidates: tied.into_boxed_slice(),
            });
        }
        let mut selection = Self {
            request,
            commit: plan.commit,
            commit_available_at: plan.commit_available_at,
            sources: sources.into_boxed_slice(),
            exclusions: plan.exclusions,
            completeness: ProviderMarketEventSelectionCompleteness::Complete,
            selection_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        selection.selection_digest = selection_digest(&selection)?;
        Ok(selection)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderMarketEventCatalogCandidate {
    pub(crate) publication: ProviderMarketEventExactPublication,
    pub(crate) publication_row_ordinal: u32,
    pub(crate) coordinate_digest: EvidenceDigest,
    pub(crate) source_surface: SourceId,
    pub(crate) effective_at: Timestamp,
    pub(crate) origin_committed_at: Timestamp,
}

impl ProviderMarketEventCatalogCandidate {
    fn matches_selected(
        &self,
        coordinate: &ProviderMarketEventSelectionCoordinate,
        request: &ProviderMarketEventPointInTimeRequest,
    ) -> bool {
        let Ok(effective_at) = effective_time(coordinate, request.effective_time_basis) else {
            return false;
        };
        self.publication == coordinate.publication
            && self.publication_row_ordinal == coordinate.publication_row_ordinal
            && self.coordinate_digest == coordinate.coordinate_digest
            && self.source_surface == coordinate.source_surface
            && self.effective_at == effective_at
            && self.origin_committed_at == coordinate.origin_committed_at
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderMarketEventCatalogPlan {
    pub(crate) commit: MarketEventCommitRef,
    pub(crate) commit_available_at: Timestamp,
    pub(crate) candidates: Vec<ProviderMarketEventCatalogCandidate>,
    pub(crate) exclusions: ProviderMarketEventExclusionCounts,
}

impl ProviderMarketEventCatalogPlan {
    pub(crate) fn try_new(
        commit: MarketEventCommitRef,
        commit_available_at: Timestamp,
        candidates: Vec<ProviderMarketEventCatalogCandidate>,
        exclusions: ProviderMarketEventExclusionCounts,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        if commit_available_at != commit.available_at() {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        if candidates.len() > MAX_PROVIDER_MARKET_EVENT_POINT_IN_TIME_CANDIDATES {
            return Err(ProviderMarketEventSelectionError::CandidateLimitExceeded);
        }
        Ok(Self {
            commit,
            commit_available_at,
            candidates,
            exclusions,
        })
    }
}

impl ProviderMarketEventSelectedCandidate {
    pub(crate) fn try_from_reopened_publication(
        request: &ProviderMarketEventPointInTimeRequest,
        planned: &ProviderMarketEventCatalogCandidate,
        snapshot: &CatalogReadSnapshot,
        batch: &ProviderMarketEventArrowBatch,
        evidence: Arc<PersistedProviderPublicationEvidence>,
    ) -> Result<Self, ProviderMarketEventSelectionError> {
        if batch.publication_digest() != planned.publication.digest
            || batch.publication_kind() != publication_kind_name(planned.publication.kind)
            || evidence.publication_digest() != planned.publication.digest
            || evidence.publication_kind() != publication_kind_name(planned.publication.kind)
        {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        evidence.verify_integrity()?;
        let publication_rows = snapshot.publication_coordinates(planned.publication.digest)?;
        let indexed = publication_rows
            .get(
                usize::try_from(planned.publication_row_ordinal)
                    .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?,
            )
            .ok_or(ProviderMarketEventSelectionError::EvidenceMismatch)?;
        if indexed.publication_digest() != planned.publication.digest
            || indexed.publication_kind() != publication_kind_name(planned.publication.kind)
            || indexed.publication_row_ordinal() != planned.publication_row_ordinal
            || indexed.coordinate_digest() != planned.coordinate_digest
        {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        let event = batch
            .events()
            .get(
                usize::try_from(planned.publication_row_ordinal)
                    .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?,
            )
            .ok_or(ProviderMarketEventSelectionError::EvidenceMismatch)?
            .clone();
        indexed.revalidate_reconstructed_event(&event)?;
        let component_kind = match indexed.component_kind() {
            "response" => ProviderMarketEventComponentKind::Response,
            "stream" => ProviderMarketEventComponentKind::Stream,
            _ => return Err(ProviderMarketEventSelectionError::EvidenceMismatch),
        };
        let component_ordinal = usize::try_from(indexed.component_row_ordinal())
            .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?;
        let component_matches = match component_kind {
            ProviderMarketEventComponentKind::Response => {
                evidence.response().is_some_and(|response| {
                    response.binding_digest() == indexed.component_binding_digest()
                        && response.rows().get(component_ordinal).is_some_and(|row| {
                            row.canonical_row_ordinal() == indexed.component_row_ordinal()
                                && row.canonical_event_digest() == indexed.canonical_event_digest()
                        })
                })
            }
            ProviderMarketEventComponentKind::Stream => evidence.event().is_some_and(|event| {
                event.binding_digest() == indexed.component_binding_digest()
                    && event.rows().get(component_ordinal).is_some_and(|row| {
                        row.canonical_row_ordinal() == indexed.component_row_ordinal()
                            && row.canonical_event_digest() == indexed.canonical_event_digest()
                    })
            }),
        };
        if !component_matches {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        let coordinate = ProviderMarketEventSelectionCoordinate {
            publication: planned.publication,
            publication_row_ordinal: indexed.publication_row_ordinal(),
            component_kind,
            component_binding_digest: indexed.component_binding_digest(),
            component_row_ordinal: indexed.component_row_ordinal(),
            canonical_event_digest: indexed.canonical_event_digest(),
            source_surface: SourceId::try_from(indexed.source_id().to_owned())
                .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?,
            scope: indexed.scope().clone(),
            provider_product: indexed.provider_product().clone(),
            provider_channel: indexed.provider_channel().clone(),
            venue_id: VenueId::try_from(indexed.venue_id().to_owned())
                .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?,
            event_kind: indexed.event_kind(),
            source_timestamp: indexed.source_timestamp(),
            received_at: indexed.received_at(),
            available_at: indexed.available_at(),
            ingested_at: indexed.ingested_at(),
            connection_generation: indexed.connection_generation(),
            source_sequence: indexed.source_sequence(),
            provider_event_id: SourceIdentifier::try_from(indexed.provider_event_id().to_owned())
                .map_err(|_| ProviderMarketEventSelectionError::EvidenceMismatch)?,
            coordinate_digest: indexed.coordinate_digest(),
            origin_committed_at: planned.origin_committed_at,
        };
        if !planned.matches_selected(&coordinate, request)
            || coordinate.scope != request.scope
            || request
                .exact_product
                .as_ref()
                .is_some_and(|product| product != &coordinate.provider_product)
            || request
                .exact_channel
                .as_ref()
                .is_some_and(|channel| channel != &coordinate.provider_channel)
            || coordinate.venue_id != request.venue_id
            || coordinate.event_kind != request.event_kind
            || coordinate.available_at > request.knowledge_cutoff
            || coordinate.ingested_at > request.knowledge_cutoff
            || coordinate.origin_committed_at > request.knowledge_cutoff
            || effective_time(&coordinate, request.effective_time_basis)? > request.as_of_cutoff
            || request
                .exact_source_surface
                .as_ref()
                .is_some_and(|source| source != &coordinate.source_surface)
        {
            return Err(ProviderMarketEventSelectionError::EvidenceMismatch);
        }
        Ok(Self {
            coordinate,
            event,
            evidence,
        })
    }
}

fn effective_time(
    coordinate: &ProviderMarketEventSelectionCoordinate,
    basis: ProviderMarketEventEffectiveTimeBasis,
) -> Result<Timestamp, ProviderMarketEventSelectionError> {
    match basis {
        ProviderMarketEventEffectiveTimeBasis::SourceTimestamp => coordinate
            .source_timestamp
            .ok_or(ProviderMarketEventSelectionError::EvidenceMismatch),
        ProviderMarketEventEffectiveTimeBasis::ReceivedAt => Ok(coordinate.received_at),
    }
}

fn selection_digest(
    selection: &ProviderMarketEventPointInTimeSelection,
) -> Result<EvidenceDigest, ProviderMarketEventSelectionError> {
    let request = &selection.request;
    let mut hash = Sha256::new();
    hash_field(&mut hash, SELECTION_DIGEST_DOMAIN)?;
    hash_field(&mut hash, request.dataset.as_str().as_bytes())?;
    hash_scope(&mut hash, &request.scope)?;
    hash_optional_text(
        &mut hash,
        request
            .exact_product
            .as_ref()
            .map(|value| value.as_source_identifier().as_str()),
    )?;
    hash_optional_text(
        &mut hash,
        request
            .exact_channel
            .as_ref()
            .map(|value| value.as_source_identifier().as_str()),
    )?;
    hash_field(&mut hash, request.venue_id.as_str().as_bytes())?;
    hash_field(&mut hash, event_kind_name(request.event_kind).as_bytes())?;
    hash.update(request.as_of_cutoff.unix_nanos().to_be_bytes());
    hash.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
    hash.update([match request.effective_time_basis {
        ProviderMarketEventEffectiveTimeBasis::SourceTimestamp => 1,
        ProviderMarketEventEffectiveTimeBasis::ReceivedAt => 2,
    }]);
    hash.update([match request.tie_policy {
        ProviderMarketEventTiePolicy::AllNewestEffectiveTimeTies => 1,
        ProviderMarketEventTiePolicy::LatestReceivedObservation => 2,
    }]);
    hash.update(
        u64::try_from(request.maximum_candidates)
            .map_err(|_| ProviderMarketEventSelectionError::DigestOverflow)?
            .to_be_bytes(),
    );
    hash_optional_text(
        &mut hash,
        request.exact_source_surface.as_ref().map(SourceId::as_str),
    )?;
    hash_commit(&mut hash, &selection.commit)?;
    hash.update(selection.commit_available_at.unix_nanos().to_be_bytes());
    hash.update(selection.exclusions.missing_source_timestamp.to_be_bytes());
    hash.update(selection.exclusions.after_as_of.to_be_bytes());
    hash.update(selection.exclusions.available_after_knowledge.to_be_bytes());
    hash.update(selection.exclusions.ingested_after_knowledge.to_be_bytes());
    hash.update(
        selection
            .exclusions
            .origin_published_after_knowledge
            .to_be_bytes(),
    );
    hash.update(selection.exclusions.superseded_effective_time.to_be_bytes());
    hash.update(
        selection
            .exclusions
            .superseded_received_observation
            .to_be_bytes(),
    );
    hash.update([1]);
    hash.update(
        u64::try_from(selection.sources.len())
            .map_err(|_| ProviderMarketEventSelectionError::DigestOverflow)?
            .to_be_bytes(),
    );
    for source in &selection.sources {
        hash_field(&mut hash, source.source_surface.as_str().as_bytes())?;
        hash.update(source.effective_at.unix_nanos().to_be_bytes());
        hash.update(
            u64::try_from(source.tied_candidates.len())
                .map_err(|_| ProviderMarketEventSelectionError::DigestOverflow)?
                .to_be_bytes(),
        );
        for candidate in &source.tied_candidates {
            hash_coordinate(&mut hash, &candidate.coordinate)?;
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_commit(
    hash: &mut Sha256,
    commit: &MarketEventCommitRef,
) -> Result<(), ProviderMarketEventSelectionError> {
    hash_field(hash, commit.dataset_id().as_str().as_bytes())?;
    hash.update(commit.sequence().to_be_bytes());
    hash_field(hash, commit.schema().name().as_bytes())?;
    hash.update(commit.schema().version().get().to_be_bytes());
    hash.update(commit.schema().fingerprint());
    hash.update(commit.content_hash().bytes());
    hash.update(commit.publication_digest().bytes());
    hash.update(commit.row_count().to_be_bytes());
    Ok(())
}

fn hash_coordinate(
    hash: &mut Sha256,
    coordinate: &ProviderMarketEventSelectionCoordinate,
) -> Result<(), ProviderMarketEventSelectionError> {
    hash.update(coordinate.publication.digest.bytes());
    hash_field(
        hash,
        publication_kind_name(coordinate.publication.kind).as_bytes(),
    )?;
    hash.update(coordinate.publication_row_ordinal.to_be_bytes());
    hash.update([match coordinate.component_kind {
        ProviderMarketEventComponentKind::Response => 1,
        ProviderMarketEventComponentKind::Stream => 2,
    }]);
    hash.update(coordinate.component_binding_digest.bytes());
    hash.update(coordinate.component_row_ordinal.to_be_bytes());
    hash.update(coordinate.canonical_event_digest.bytes());
    hash_field(hash, coordinate.source_surface.as_str().as_bytes())?;
    hash_scope(hash, &coordinate.scope)?;
    hash_field(
        hash,
        coordinate
            .provider_product
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_field(
        hash,
        coordinate
            .provider_channel
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_field(hash, coordinate.venue_id.as_str().as_bytes())?;
    hash_field(hash, event_kind_name(coordinate.event_kind).as_bytes())?;
    hash_optional_timestamp(hash, coordinate.source_timestamp);
    hash.update(coordinate.received_at.unix_nanos().to_be_bytes());
    hash.update(coordinate.available_at.unix_nanos().to_be_bytes());
    hash.update(coordinate.ingested_at.unix_nanos().to_be_bytes());
    hash.update(coordinate.connection_generation.to_be_bytes());
    hash_optional_u64(hash, coordinate.source_sequence);
    hash_field(hash, coordinate.provider_event_id.as_str().as_bytes())?;
    hash.update(coordinate.coordinate_digest.bytes());
    hash.update(coordinate.origin_committed_at.unix_nanos().to_be_bytes());
    Ok(())
}

fn hash_optional_text(
    hash: &mut Sha256,
    value: Option<&str>,
) -> Result<(), ProviderMarketEventSelectionError> {
    match value {
        Some(value) => {
            hash.update([1]);
            hash_field(hash, value.as_bytes())
        }
        None => {
            hash.update([0]);
            Ok(())
        }
    }
}

fn hash_optional_timestamp(hash: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.unix_nanos().to_be_bytes());
        }
        None => hash.update([0]),
    }
}

fn hash_optional_u64(hash: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.to_be_bytes());
        }
        None => hash.update([0]),
    }
}

fn hash_field(hash: &mut Sha256, value: &[u8]) -> Result<(), ProviderMarketEventSelectionError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| ProviderMarketEventSelectionError::DigestOverflow)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

pub(crate) const fn publication_kind_name(
    kind: ProviderMarketEventPublicationKind,
) -> &'static str {
    match kind {
        ProviderMarketEventPublicationKind::ResponseMarketEvent => "response_market_event",
        ProviderMarketEventPublicationKind::EventMicrobatch => "event_microbatch",
        ProviderMarketEventPublicationKind::CompositeResponseEvent => "composite_response_event",
    }
}

pub(crate) const fn event_kind_name(kind: LiveEventClass) -> &'static str {
    match kind {
        LiveEventClass::Trade => "trade",
        LiveEventClass::Quote => "quote",
        LiveEventClass::BookSnapshot => "book_snapshot",
        LiveEventClass::BookDelta => "book_delta",
        LiveEventClass::Auction => "auction",
        LiveEventClass::TradingHalt => "trading_halt",
        LiveEventClass::InstrumentStatus => "instrument_status",
        LiveEventClass::CorporateAction => "corporate_action",
        LiveEventClass::Chart => "chart",
        LiveEventClass::Screener => "screener",
    }
}

/// Provider-market-event PIT request, selection, or restart failure.
#[derive(Debug, Error)]
pub enum ProviderMarketEventSelectionError {
    /// A required identity, schema, exact commit, or work ceiling is invalid.
    #[error("provider market-event point-in-time request is invalid")]
    InvalidRequest,
    /// More eligible exact rows exist than the caller authorized retaining.
    #[error("provider market-event point-in-time candidate ceiling was exceeded")]
    CandidateLimitExceeded,
    /// Catalog coordinates, Parquet events, or persisted provider evidence disagree.
    #[error("provider market-event point-in-time evidence does not match")]
    EvidenceMismatch,
    /// An exact-commit restart did not reproduce the original selection receipt.
    #[error("provider market-event point-in-time restart replay differs")]
    RestartMismatch,
    /// Fallible bounded result allocation failed.
    #[error("provider market-event point-in-time allocation failed")]
    Allocation,
    /// Canonical selection-digest length accounting overflowed.
    #[error("provider market-event point-in-time digest accounting overflowed")]
    DigestOverflow,
    /// The immutable analytical commit catalog rejected the selection.
    #[error("provider market-event commit selection failed")]
    Manifest(#[from] ManifestCatalogError),
    /// The provider evidence catalog rejected the selected coordinate.
    #[error("provider market-event evidence selection failed")]
    Catalog(#[from] CatalogError),
}

impl From<rusqlite::Error> for ProviderMarketEventSelectionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Manifest(ManifestCatalogError::Sqlite(error))
    }
}

fn hash_scope(
    hash: &mut Sha256,
    scope: &LiveEvidenceScope,
) -> Result<(), ProviderMarketEventSelectionError> {
    match scope {
        LiveEvidenceScope::Instrument(id) => {
            hash.update([1]);
            hash.update(id.as_uuid().as_bytes());
        }
        LiveEvidenceScope::SourceCohort(key) => {
            hash.update([2]);
            hash_field(hash, key.as_str().as_bytes())?;
        }
    }
    Ok(())
}
