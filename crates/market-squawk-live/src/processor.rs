//! Instrument-owned current-observation application and sole authority boundary.

#![allow(
    dead_code,
    reason = "the actor runtime is the sole production caller of this crate-private processor"
)]

use std::collections::HashMap;
use std::time::Duration;

use market_squawk_domain::{
    ConnectionGeneration, DataQuality, InstrumentDefinition, LiveEventClass, MarketEvent,
    QualificationAssessment, Timestamp, TradingStatus,
};
use market_squawk_sources::{
    CurrentDecodedProviderBatch, CurrentObservationIter, CurrentProviderObservation,
    CurrentStreamKey,
};
#[path = "processor/error.rs"]
mod error;
#[path = "processor/event.rs"]
mod event;
#[path = "processor/generation.rs"]
mod generation;
#[path = "processor/snapshot.rs"]
mod snapshot;
#[path = "processor/status.rs"]
mod status;
#[path = "processor/stream.rs"]
mod stream;

pub(crate) use error::LiveApplyError;
pub(crate) use event::{delta_canonical_vector_peak_bytes, snapshot_canonical_vector_peak_bytes};
#[allow(
    unused_imports,
    reason = "control-plane binding consumes the registry and exit handle"
)]
pub(crate) use generation::{
    GenerationAdmission, GenerationAuthorityRegistry, GenerationRegistryExitHandle,
};
use snapshot::build_snapshot_seed;
pub(crate) use snapshot::{ProcessorSnapshotLimits, ProcessorSnapshotSeed};
use status::{SharedStatus, StatusBook, StatusKey};
use stream::{StreamState, preview_stream};

use crate::authority::{
    AppliedObservationAuthority, AuthorityGate, ClockReading, RuntimeLease, ShardLease,
    SystemTrustedClock, TrustedClock,
};
use crate::provider_book::{BookProcessingScratch, ProviderBook};
use crate::qualification::build_qualified_event;
use crate::{AuthorityError, ConsumedLiveAuthority, DepthLimit, LiveExecutionCapability};

/// Hard bound for independently keyed source/product/channel streams per instrument owner.
pub(crate) const MAX_STREAMS_PER_INSTRUMENT: usize = 64;

/// Inline persistent ownership charged once per independently keyed stream.
///
/// The concrete state includes sequence, checksum, provenance, generation/revision authority,
/// scaled/exact book containers, the stream hash entry, and its separately keyed status entry.
pub(crate) const fn persistent_stream_inline_bytes() -> usize {
    std::mem::size_of::<(CurrentStreamKey, StreamState)>()
        + std::mem::size_of::<(StatusKey, SharedStatus)>()
}

/// Exact shard-owned runtime liveness bindings used by every capability.
///
/// Positive invalidation ownership remains with the actor/supervisor. The processor receives only
/// validation/degradation leases and never constructs a private per-instrument incarnation.
#[derive(Clone, Debug)]
pub(crate) struct ProcessorLivenessBinding {
    shard: ShardLease,
    runtime: RuntimeLease,
}

impl ProcessorLivenessBinding {
    pub(crate) fn new(shard: ShardLease, runtime: RuntimeLease) -> Self {
        Self { shard, runtime }
    }

    fn validate(&self) -> Result<(), AuthorityError> {
        self.shard.validate().map_err(AuthorityError::from)?;
        self.runtime.validate().map_err(AuthorityError::from)
    }
}

/// Owned wire-order cursor. Only intact current observations can be applied.
#[derive(Debug)]
pub(crate) struct CurrentBatchCursor {
    key_venue: market_squawk_domain::VenueId,
    key_instrument: market_squawk_domain::InstrumentId,
    observations: CurrentObservationIter,
    admission: GenerationAdmission,
}

/// Canonical event, audit assessment, and optional current-state authority seed.
#[derive(Debug)]
pub(crate) struct AppliedLiveObservation {
    pub(crate) stream: CurrentStreamKey,
    pub(crate) generation: ConnectionGeneration,
    pub(crate) event: MarketEvent,
    pub(crate) assessment: QualificationAssessment,
    pub(crate) binding_digest: [u8; 32],
    pub(crate) committed_state_revision: u64,
    pub(crate) authority: Option<AppliedObservationAuthority>,
}

/// Single-writer state for one instrument across independently keyed provider streams.
#[derive(Debug)]
pub(crate) struct InstrumentLiveProcessor<C: TrustedClock> {
    definition: InstrumentDefinition,
    depth: DepthLimit,
    streams: HashMap<CurrentStreamKey, StreamState>,
    source_generations: HashMap<market_squawk_domain::SourceId, ConnectionGeneration>,
    statuses: StatusBook,
    max_streams: usize,
    max_sources: usize,
    liveness: ProcessorLivenessBinding,
    authority: AuthorityGate,
    clock: C,
    maximum_capability_lifetime: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceGenerationTransition {
    Current,
    Insert,
    Replace,
}

impl InstrumentLiveProcessor<SystemTrustedClock> {
    /// Constructs the production processor using runtime-owned liveness and the sealed clock.
    #[allow(
        clippy::too_many_arguments,
        reason = "all startup bounds and external liveness bindings are explicit"
    )]
    pub(crate) fn new_system(
        definition: InstrumentDefinition,
        depth: DepthLimit,
        max_streams: usize,
        max_sources: usize,
        nonce_capacity: usize,
        nonce_reclaim_budget: usize,
        maximum_capability_lifetime: Duration,
        liveness: ProcessorLivenessBinding,
    ) -> Result<Self, LiveApplyError> {
        Self::try_new(
            definition,
            depth,
            max_streams,
            max_sources,
            nonce_capacity,
            nonce_reclaim_budget,
            maximum_capability_lifetime,
            liveness,
            SystemTrustedClock,
        )
    }
}

impl<C: TrustedClock> InstrumentLiveProcessor<C> {
    #[allow(
        clippy::too_many_arguments,
        reason = "startup bounds, external liveness, and sealed clock are explicit"
    )]
    pub(crate) fn try_new(
        definition: InstrumentDefinition,
        depth: DepthLimit,
        max_streams: usize,
        max_sources: usize,
        nonce_capacity: usize,
        nonce_reclaim_budget: usize,
        maximum_capability_lifetime: Duration,
        liveness: ProcessorLivenessBinding,
        clock: C,
    ) -> Result<Self, LiveApplyError> {
        if max_streams == 0 || max_streams > MAX_STREAMS_PER_INSTRUMENT {
            return Err(LiveApplyError::InvalidStreamCapacity {
                requested: max_streams,
                maximum: MAX_STREAMS_PER_INSTRUMENT,
            });
        }
        if max_sources == 0 || max_sources > MAX_STREAMS_PER_INSTRUMENT {
            return Err(LiveApplyError::InvalidGenerationCapacity);
        }
        if maximum_capability_lifetime.is_zero() {
            return Err(LiveApplyError::InvalidCapabilityLifetime);
        }
        liveness.validate()?;
        let mut streams = HashMap::new();
        streams
            .try_reserve(max_streams)
            .map_err(|_| LiveApplyError::Allocation)?;
        let mut source_generations = HashMap::new();
        source_generations
            .try_reserve(max_sources)
            .map_err(|_| LiveApplyError::Allocation)?;
        Ok(Self {
            definition,
            depth,
            streams,
            source_generations,
            statuses: StatusBook::try_new(max_streams)?,
            max_streams,
            max_sources,
            liveness,
            authority: AuthorityGate::new(nonce_capacity, nonce_reclaim_budget)?,
            clock,
            maximum_capability_lifetime,
        })
    }

    /// Validates an owned homogeneous batch and preserves its intact current observations.
    pub(crate) fn accept_batch(
        &self,
        batch: CurrentDecodedProviderBatch,
        admission: &GenerationAdmission,
    ) -> Result<CurrentBatchCursor, LiveApplyError> {
        let now = self.clock.now()?;
        batch.validate_at(now.wall())?;
        admission.validate_at(now.wall())?;
        if !batch
            .current_lease()
            .binding()
            .shares_allocation_with(admission.source().binding())
        {
            return Err(LiveApplyError::GenerationAdmissionTransplant);
        }
        if batch.key().instrument() != self.definition.instrument_id() {
            return Err(LiveApplyError::InstrumentMismatch);
        }
        if !self
            .definition
            .venue_mappings()
            .iter()
            .any(|mapping| mapping.venue_id() == batch.key().venue())
        {
            return Err(LiveApplyError::VenueMismatch);
        }
        let key_venue = batch.key().venue().clone();
        let key_instrument = batch.key().instrument();
        Ok(CurrentBatchCursor {
            key_venue,
            key_instrument,
            observations: batch.into_observations(),
            admission: admission.clone(),
        })
    }

    /// Applies and qualifies exactly one wire-order observation using candidate-and-commit state.
    pub(crate) fn apply_next(
        &mut self,
        cursor: &mut CurrentBatchCursor,
        scratch: &mut BookProcessingScratch,
    ) -> Result<Option<AppliedLiveObservation>, LiveApplyError> {
        let Some(current) = cursor.observations.next() else {
            return Ok(None);
        };
        let now = self.clock.now()?;
        self.validate_observation(&current, cursor, now.wall())?;
        let key = current.stream_key().clone();
        self.prepare_generation_and_stream(&current, &cursor.admission)?;
        let staged_status = match self
            .statuses
            .stage(&current, self.definition.trading_status())
        {
            Ok(value) => value,
            Err(error) => {
                self.quarantine_rejected(&key, &current, now.wall());
                return Err(error);
            }
        };
        let mut state = self
            .streams
            .remove(&key)
            .ok_or(LiveApplyError::StreamStateMissing)?;
        let outcome = (|| {
            let mut candidate = preview_stream(
                &mut state,
                &current,
                &self.definition,
                staged_status.status(),
                now.wall(),
                scratch,
            )?;
            let prepared = candidate.take_prepared()?;
            let qualified = build_qualified_event(
                &current,
                candidate.qualification(),
                now.wall(),
                move |provenance| prepared.build(provenance),
            )?;
            let capability_deadline =
                monotonic_deadline(now, qualified.valid_until, self.maximum_capability_lifetime)?;

            // Recheck every external allocation with a fresh sealed reading while the RAII delta
            // is still reversible; qualification work may have crossed a wall deadline.
            let commit_now = self.clock.now()?;
            self.validate_observation(&current, cursor, commit_now.wall())?;
            if commit_now.wall() > qualified.valid_until
                || commit_now.monotonic() > capability_deadline
            {
                return Err(LiveApplyError::CapabilityExpired);
            }
            self.liveness.validate()?;
            candidate
                .generation_lease()
                .validate()
                .map_err(AuthorityError::from)?;
            self.statuses.validate_staged(&staged_status)?;
            let committed = candidate.commit()?;
            Ok::<_, LiveApplyError>((qualified, capability_deadline, committed))
        })();
        let (qualified, capability_deadline, committed) = match outcome {
            Ok(value) => value,
            Err(error) => {
                state.quarantine_rejected(&current, now.wall());
                self.streams.insert(key, state);
                return Err(error);
            }
        };
        self.streams.insert(key.clone(), state);
        let status = self.statuses.commit(staged_status);
        let authority = if qualified.assessment.recorded_quality() == DataQuality::DirectVerified
            && committed.trading_status == TradingStatus::Active
            && execution_enabled(current.observation().event_class())
        {
            Some(AppliedObservationAuthority::new(
                cursor.admission.source().clone(),
                committed.generation,
                self.liveness.shard.clone(),
                self.liveness.runtime.clone(),
                status.allocation,
                committed.revision,
                committed.expected_revision,
                status.revision,
                status.expected_revision,
                qualified.assessment.assessment_id().clone(),
                qualified.assessment.binding().clone(),
                qualified.binding_digest,
                qualified.valid_until,
                capability_deadline,
                qualified.assessment.recorded_quality(),
            ))
        } else {
            None
        };
        Ok(Some(AppliedLiveObservation {
            stream: key,
            generation: cursor.admission.source().binding().connection_generation(),
            event: qualified.event,
            assessment: qualified.assessment,
            binding_digest: qualified.binding_digest,
            committed_state_revision: committed.expected_revision,
            authority,
        }))
    }

    pub(crate) fn committed_book(
        &self,
        stream: &CurrentStreamKey,
    ) -> Result<&ProviderBook, LiveApplyError> {
        self.streams
            .get(stream)
            .map(StreamState::book)
            .ok_or(LiveApplyError::StreamStateMissing)
    }

    /// Returns immutable reference-master execution terms owned by this route processor.
    pub(crate) const fn execution_terms(&self) -> market_squawk_domain::InstrumentExecutionTerms {
        self.definition.execution_terms()
    }

    /// Revalidates exact applied authority before feature and strategy evaluation.
    pub(crate) fn validate_applied_current(
        &self,
        applied: &AppliedObservationAuthority,
    ) -> Result<(), AuthorityError> {
        self.authority
            .validate_applied_current(applied, self.clock.now()?)
    }

    /// Mints the sole opaque capability after revalidating exact committed authority.
    pub(crate) fn issue(
        &mut self,
        applied: &AppliedObservationAuthority,
    ) -> Result<LiveExecutionCapability, AuthorityError> {
        self.authority.issue(applied, self.clock.now()?)
    }

    /// Consumes an opaque capability exactly once before risk/dispatch validation.
    pub(crate) fn consume(
        &mut self,
        capability: LiveExecutionCapability,
    ) -> Result<ConsumedLiveAuthority, AuthorityError> {
        self.authority.consume(capability, self.clock.now()?)
    }

    /// Returns bounded immutable state for snapshot publication, including quarantined streams.
    pub(crate) fn snapshot_seed(
        &self,
        limits: ProcessorSnapshotLimits,
    ) -> Result<ProcessorSnapshotSeed, LiveApplyError> {
        build_snapshot_seed(
            self.definition.instrument_id(),
            self.depth.get(),
            &self.streams,
            &self.statuses,
            limits,
        )
    }

    /// Release-invalidates processor-owned authority while the runtime retains liveness ownership.
    pub(crate) fn invalidate_for_exit(&mut self) {
        for state in self.streams.values_mut() {
            state.quarantine();
        }
        self.statuses.invalidate_all();
    }

    fn validate_observation(
        &self,
        current: &CurrentProviderObservation,
        cursor: &CurrentBatchCursor,
        at: Timestamp,
    ) -> Result<(), LiveApplyError> {
        current.current_lease().validate_at(at)?;
        cursor.admission.validate_at(at)?;
        self.liveness.validate()?;
        if !current
            .frame_evidence()
            .binding()
            .shares_allocation_with(cursor.admission.source().binding())
        {
            return Err(LiveApplyError::GenerationAdmissionTransplant);
        }
        validate_current_identity(
            current,
            &cursor.key_venue,
            cursor.key_instrument,
            self.definition.instrument_id(),
        )
    }

    fn prepare_generation_and_stream(
        &mut self,
        current: &CurrentProviderObservation,
        admission: &GenerationAdmission,
    ) -> Result<(), LiveApplyError> {
        let key = current.stream_key();
        let source_id = key.source_id();
        let generation = current.frame_evidence().binding().connection_generation();
        let source_transition = match self.source_generations.get(source_id).copied() {
            Some(existing) if existing > generation => {
                return Err(LiveApplyError::GenerationNotAdvanced);
            }
            Some(existing) if existing == generation => SourceGenerationTransition::Current,
            Some(_) => SourceGenerationTransition::Replace,
            None if self.source_generations.len() >= self.max_sources => {
                return Err(LiveApplyError::GenerationCapacityExhausted);
            }
            None => SourceGenerationTransition::Insert,
        };
        if let Some(existing) = self.streams.get(key) {
            if generation == existing.connection_generation()
                && source_transition == SourceGenerationTransition::Current
            {
                if existing
                    .generation_lease()
                    .shares_allocation_with(&admission.generation())
                {
                    return Ok(());
                }
                return Err(LiveApplyError::GenerationAdmissionTransplant);
            }
            if generation < existing.connection_generation() {
                return Err(LiveApplyError::GenerationNotAdvanced);
            }
        }

        let retained_streams = if source_transition == SourceGenerationTransition::Replace {
            self.streams
                .keys()
                .filter(|candidate| candidate.source_id() != source_id)
                .count()
        } else if self.streams.contains_key(key) {
            self.streams.len().saturating_sub(1)
        } else {
            self.streams.len()
        };
        if retained_streams >= self.max_streams {
            return Err(LiveApplyError::StreamCapacityExhausted);
        }

        // Construct every fallible replacement before revoking the committed generation. This
        // keeps a source cutover and all of its stream invalidation at one single-writer commit
        // point; capacity or protocol-resolution failure leaves the former state untouched.
        let state = StreamState::new(
            generation,
            admission.generation(),
            current.policy().protocol(),
            self.depth,
        )?;
        if source_transition != SourceGenerationTransition::Current {
            // Revalidate immediately before the now-infallible generation commit. Same-generation
            // stream additions already passed top-level validation and need no extra clock read.
            admission.validate_at(self.clock.now()?.wall())?;
        }
        if source_transition == SourceGenerationTransition::Replace {
            self.streams.retain(|key, state| {
                if key.source_id() == source_id {
                    state.quarantine();
                    false
                } else {
                    true
                }
            });
            self.statuses.invalidate_source(source_id);
        } else if let Some(existing) = self.streams.get_mut(key) {
            existing.quarantine();
        }
        if source_transition != SourceGenerationTransition::Current {
            self.source_generations
                .insert(source_id.clone(), generation);
        }
        self.streams.insert(key.clone(), state);
        Ok(())
    }

    fn quarantine_rejected(
        &mut self,
        key: &CurrentStreamKey,
        current: &CurrentProviderObservation,
        evaluated_at: Timestamp,
    ) {
        if let Some(state) = self.streams.get_mut(key) {
            state.quarantine_rejected(current, evaluated_at);
        }
    }
}

impl<C: TrustedClock> Drop for InstrumentLiveProcessor<C> {
    fn drop(&mut self) {
        self.invalidate_for_exit();
    }
}

fn validate_current_identity(
    current: &CurrentProviderObservation,
    batch_venue: &market_squawk_domain::VenueId,
    batch_instrument: market_squawk_domain::InstrumentId,
    definition_instrument: market_squawk_domain::InstrumentId,
) -> Result<(), LiveApplyError> {
    let observation = current.observation();
    let stream = current.stream_key();
    let policy = current.policy();
    let frame = current.frame_evidence();
    if current.key().venue() != batch_venue
        || current.key().instrument() != batch_instrument
        || observation.venue() != batch_venue
        || observation.instrument() != batch_instrument
        || observation.instrument() != definition_instrument
        || stream.venue() != batch_venue
        || stream.instrument() != batch_instrument
        || stream.source_id() != frame.binding().source_id()
        || policy.coverage().source_id() != frame.binding().source_id()
        || policy.coverage().metadata_revision() != frame.binding().metadata_revision()
        || stream.provider_product() != policy.provider_product()
        || stream.provider_channel() != policy.provider_channel()
        || policy.coverage().event_class() != observation.event_class()
        || policy.coverage().depth() != observation.depth()
    {
        return Err(LiveApplyError::BindingMismatch);
    }
    Ok(())
}

fn execution_enabled(event_class: LiveEventClass) -> bool {
    matches!(
        event_class,
        LiveEventClass::Trade
            | LiveEventClass::Quote
            | LiveEventClass::BookSnapshot
            | LiveEventClass::BookDelta
    )
}

fn monotonic_deadline(
    now: ClockReading,
    wall_deadline: Timestamp,
    maximum_lifetime: Duration,
) -> Result<std::time::Instant, LiveApplyError> {
    let remaining = i128::from(wall_deadline.unix_nanos()) - i128::from(now.wall().unix_nanos());
    if remaining < 0 {
        return Err(LiveApplyError::CapabilityExpired);
    }
    let remaining = u128::try_from(remaining).map_err(|_| LiveApplyError::CapabilityExpired)?;
    let bounded = remaining.min(maximum_lifetime.as_nanos());
    let nanos = u64::try_from(bounded).map_err(|_| LiveApplyError::CapabilityExpired)?;
    now.monotonic()
        .checked_add(Duration::from_nanos(nanos))
        .ok_or(LiveApplyError::CapabilityExpired)
}

#[cfg(test)]
#[path = "processor/tests.rs"]
mod tests;
