//! Fail-safe candidate-and-commit mutation for one provider product/channel stream.

use market_squawk_domain::{
    BookStateBinding, ChecksumCapability, ChecksumEvidence, ChecksumScope, ChecksumValue,
    ConnectionGeneration, InstrumentDefinition, SnapshotApplicability, SourceIdentifier, Timestamp,
    TradingStatus,
};
use market_squawk_sources::{
    ChecksumValidationProfile, CurrentProviderObservation, ProviderChecksumEvidence,
    ProviderObservationPayload, ProviderTimestampEvidence,
};

use super::LiveApplyError;
use super::event::{PreparedEvent, digest_book, normalized_changes, prepare_non_book};
use crate::authority::{AuthorityError, GenerationLease, StreamRevisionLease, StreamRevisionOwner};
use crate::provider_book::{ProviderBook, ProviderBookDeltaTransaction};
use crate::qualification::{CommittedQualificationEvidence, SnapshotOrigin, canonical_digest};
use crate::{
    DepthLimit, GenerationPhase, GenerationStateMachine, ResolvedChecksumValidator, SequenceTracker,
};

#[derive(Debug)]
pub(super) struct StreamState {
    connection_generation: ConnectionGeneration,
    generation: GenerationLease,
    phase: GenerationStateMachine,
    sequence: SequenceTracker,
    checksum: Option<ResolvedChecksumValidator>,
    book: ProviderBook,
    snapshot_origin: Option<SnapshotOrigin>,
    revision: StreamRevisionOwner,
    health_epoch: u64,
    source_valid_until: Option<Timestamp>,
    source_timestamp: Option<Timestamp>,
    received_at: Option<Timestamp>,
    evaluated_at: Option<Timestamp>,
}

impl StreamState {
    pub(super) fn new(
        generation: ConnectionGeneration,
        generation_lease: GenerationLease,
        protocol: &market_squawk_sources::LiveProtocolProfile,
        depth: DepthLimit,
    ) -> Result<Self, LiveApplyError> {
        let checksum = match protocol.checksum() {
            ChecksumValidationProfile::Unsupported { .. } => None,
            profile @ ChecksumValidationProfile::Provided { .. } => {
                Some(ResolvedChecksumValidator::resolve(profile, depth.get())?)
            }
        };
        let mut phase = GenerationStateMachine::new();
        phase.begin_generation(generation)?;
        Ok(Self {
            connection_generation: generation,
            generation: generation_lease,
            phase,
            sequence: SequenceTracker::new(generation, protocol.sequence()),
            checksum,
            book: ProviderBook::new(depth),
            snapshot_origin: None,
            revision: StreamRevisionOwner::new(),
            health_epoch: 0,
            source_valid_until: None,
            source_timestamp: None,
            received_at: None,
            evaluated_at: None,
        })
    }

    pub(super) const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }
    pub(super) const fn phase(&self) -> GenerationPhase {
        self.phase.phase()
    }
    pub(super) fn generation_lease(&self) -> GenerationLease {
        self.generation.clone()
    }
    pub(super) fn revision(&self) -> u64 {
        self.revision.diagnostic_revision()
    }
    pub(super) const fn sequence(&self) -> &SequenceTracker {
        &self.sequence
    }
    pub(super) const fn book(&self) -> &ProviderBook {
        &self.book
    }
    pub(super) const fn snapshot_origin(&self) -> Option<&SnapshotOrigin> {
        self.snapshot_origin.as_ref()
    }
    pub(super) const fn health_epoch(&self) -> u64 {
        self.health_epoch
    }
    pub(super) const fn source_valid_until(&self) -> Option<Timestamp> {
        self.source_valid_until
    }
    pub(super) const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }
    pub(super) const fn received_at(&self) -> Option<Timestamp> {
        self.received_at
    }
    pub(super) const fn evaluated_at(&self) -> Option<Timestamp> {
        self.evaluated_at
    }

    pub(super) fn quarantine(&mut self) {
        self.generation.invalidate();
        self.revision.invalidate();
        self.phase.quarantine();
    }

    /// Quarantines a rejected observation while retaining complete, truthful diagnostics.
    ///
    /// A newly allocated stream has no committed market state. Its first rejected observation is
    /// therefore the only truthful provenance available for the quarantined diagnostic record.
    /// An established stream keeps the provenance of its last committed state so a rejected
    /// candidate can never relabel an older book with newer, uncommitted evidence.
    pub(super) fn quarantine_rejected(
        &mut self,
        current: &CurrentProviderObservation,
        evaluated_at: Timestamp,
    ) {
        if self.received_at.is_none() {
            self.health_epoch = current.current_lease().health_epoch();
            self.source_valid_until = Some(current.current_lease().valid_until());
            self.source_timestamp = match current.observation().timestamp() {
                ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
                ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
            };
            self.received_at = Some(current.frame_evidence().received_at());
            self.evaluated_at = Some(evaluated_at);
        }
        self.quarantine();
    }

    #[cfg(test)]
    pub(super) fn set_revision_for_test(&mut self, revision: u64) {
        self.revision.invalidate();
        self.revision = StreamRevisionOwner::new_for_test(revision);
    }
}

#[derive(Debug)]
enum BookMutation<'a> {
    Unchanged,
    Snapshot {
        target: &'a mut ProviderBook,
        candidate: ProviderBook,
    },
    Delta(ProviderBookDeltaTransaction<'a>),
}

/// Fully validated next state. A dropped delta candidate automatically restores last-good book.
#[derive(Debug)]
pub(super) struct StreamCandidate<'a> {
    phase_target: &'a mut GenerationStateMachine,
    sequence_target: &'a mut SequenceTracker,
    snapshot_origin_target: &'a mut Option<SnapshotOrigin>,
    revision_target: &'a mut StreamRevisionOwner,
    book_mutation: BookMutation<'a>,
    phase: GenerationStateMachine,
    sequence: SequenceTracker,
    snapshot_origin: Option<SnapshotOrigin>,
    prepared: Option<PreparedEvent>,
    qualification: CommittedQualificationEvidence,
    next_revision: u64,
    trading_status: TradingStatus,
    generation: GenerationLease,
    health_epoch_target: &'a mut u64,
    source_valid_until_target: &'a mut Option<Timestamp>,
    source_timestamp_target: &'a mut Option<Timestamp>,
    received_at_target: &'a mut Option<Timestamp>,
    evaluated_at_target: &'a mut Option<Timestamp>,
    health_epoch: u64,
    source_valid_until: Timestamp,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    evaluated_at: Timestamp,
}

impl StreamCandidate<'_> {
    pub(super) fn take_prepared(&mut self) -> Result<PreparedEvent, LiveApplyError> {
        self.prepared
            .take()
            .ok_or(LiveApplyError::CandidateEventAlreadyBuilt)
    }
    pub(super) fn qualification(&self) -> CommittedQualificationEvidence {
        self.qualification.clone()
    }
    pub(super) const fn next_revision(&self) -> u64 {
        self.next_revision
    }
    pub(super) fn generation_lease(&self) -> GenerationLease {
        self.generation.clone()
    }

    /// Commits revision, candidate book, cursors, and provenance in one single-writer step.
    pub(super) fn commit(self) -> Result<CommittedState, LiveApplyError> {
        let revision = self
            .revision_target
            .advance()
            .map_err(AuthorityError::from)?;
        if revision != self.next_revision {
            self.revision_target.invalidate();
            return Err(LiveApplyError::StateRevisionConflict);
        }
        match self.book_mutation {
            BookMutation::Unchanged => {}
            BookMutation::Snapshot { target, candidate } => *target = candidate,
            BookMutation::Delta(transaction) => transaction.commit(),
        }
        *self.phase_target = self.phase;
        *self.sequence_target = self.sequence;
        *self.snapshot_origin_target = self.snapshot_origin;
        *self.health_epoch_target = self.health_epoch;
        *self.source_valid_until_target = Some(self.source_valid_until);
        *self.source_timestamp_target = self.source_timestamp;
        *self.received_at_target = Some(self.received_at);
        *self.evaluated_at_target = Some(self.evaluated_at);
        Ok(CommittedState {
            generation: self.generation,
            revision: self.revision_target.lease(),
            expected_revision: revision,
            trading_status: self.trading_status,
        })
    }
}

#[derive(Debug)]
pub(super) struct CommittedState {
    pub(super) generation: GenerationLease,
    pub(super) revision: StreamRevisionLease,
    pub(super) expected_revision: u64,
    pub(super) trading_status: TradingStatus,
}

/// Builds a complete next-state candidate without publishing cursor/revision/provenance changes.
pub(super) fn preview_stream<'a>(
    state: &'a mut StreamState,
    current: &CurrentProviderObservation,
    definition: &InstrumentDefinition,
    trading_status: TradingStatus,
    evaluated_at: Timestamp,
) -> Result<StreamCandidate<'a>, LiveApplyError> {
    if state.phase.phase() == GenerationPhase::Quarantined {
        return Err(LiveApplyError::Quarantined);
    }
    let next_revision = state
        .revision
        .diagnostic_revision()
        .checked_add(1)
        .ok_or(LiveApplyError::StateRevisionExhausted)?;
    let mut phase = state.phase.clone();
    let mut sequence = state.sequence.clone();
    let mut snapshot_origin = state.snapshot_origin.clone();
    let observation = current.observation();
    let source_timestamp = match observation.timestamp() {
        ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
    };

    let (prepared, canonical_state_digest, book_state, sequence_evidence, checksum, book_mutation) =
        match observation.payload() {
            ProviderObservationPayload::BookSnapshot(snapshot) => {
                phase.begin_snapshot()?;
                let sequence_evidence = sequence.validate_snapshot(observation.sequence())?;
                let mut candidate = ProviderBook::new(state.book.scaled_depth());
                let computed = candidate.replace_snapshot(
                    snapshot.bids(),
                    snapshot.asks(),
                    definition.tick_size(),
                    definition.lot_size(),
                    state
                        .checksum
                        .as_ref()
                        .map(|validator| (validator, observation.checksum())),
                )?;
                let digest = digest_book(&candidate)?;
                let identity = state_identity(state.connection_generation, next_revision)?;
                snapshot_origin = Some(SnapshotOrigin {
                    identity: identity.clone(),
                    digest: digest.clone(),
                    initialized_at: evaluated_at,
                    sequence: sequence_evidence.observed_sequence(),
                    state_revision: next_revision,
                });
                let book_state = BookStateBinding::new(snapshot.depth(), identity, digest.clone());
                phase.commit_snapshot()?;
                let checksum = checksum_evidence(current, computed)?;
                let prepared = PreparedEvent::BookSnapshot {
                    depth: snapshot.depth(),
                    bids: candidate.bid_levels()?,
                    asks: candidate.ask_levels()?,
                    sequence: sequence_evidence.observed_sequence(),
                };
                (
                    prepared,
                    digest,
                    Some(book_state),
                    sequence_evidence,
                    checksum,
                    BookMutation::Snapshot {
                        target: &mut state.book,
                        candidate,
                    },
                )
            }
            ProviderObservationPayload::BookDelta(delta) => {
                require_healthy(&phase)?;
                let sequence_evidence = sequence.validate_delta(observation.sequence())?;
                let transaction = state.book.begin_delta(
                    delta.changes(),
                    definition.tick_size(),
                    definition.lot_size(),
                    state
                        .checksum
                        .as_ref()
                        .map(|validator| (validator, observation.checksum())),
                )?;
                let computed = transaction.computed_checksum();
                let candidate_book = transaction.candidate();
                let late_result = (|| {
                    let digest = digest_book(candidate_book)?;
                    let identity = state_identity(state.connection_generation, next_revision)?;
                    let origin = snapshot_origin
                        .as_ref()
                        .ok_or(LiveApplyError::SnapshotRequired)?;
                    let book_state = BookStateBinding::new_with_snapshot_origin(
                        delta.depth(),
                        identity,
                        digest.clone(),
                        origin.identity.clone(),
                        origin.digest.clone(),
                    );
                    let checksum = checksum_evidence(current, computed)?;
                    let changes = normalized_changes(delta.changes(), definition)?;
                    Ok::<_, LiveApplyError>((
                        PreparedEvent::BookDelta {
                            depth: delta.depth(),
                            changes,
                            sequence: sequence_evidence.observed_sequence(),
                        },
                        digest,
                        Some(book_state),
                        checksum,
                    ))
                })();
                let (prepared, digest, book_state, checksum) = late_result?;
                (
                    prepared,
                    digest,
                    book_state,
                    sequence_evidence,
                    checksum,
                    BookMutation::Delta(transaction),
                )
            }
            payload => {
                establish_non_book(&mut phase, current)?;
                let sequence_evidence = sequence.validate_non_book(observation.sequence())?;
                let checksum = checksum_evidence(current, None)?;
                let prepared = prepare_non_book(payload, definition)?;
                let digest = canonical_digest(&prepared.canonical_bytes()?)?;
                (
                    prepared,
                    digest,
                    None,
                    sequence_evidence,
                    checksum,
                    BookMutation::Unchanged,
                )
            }
        };
    let qualification_snapshot = book_state.as_ref().and(snapshot_origin.clone());
    Ok(StreamCandidate {
        phase_target: &mut state.phase,
        sequence_target: &mut state.sequence,
        snapshot_origin_target: &mut state.snapshot_origin,
        revision_target: &mut state.revision,
        book_mutation,
        phase,
        sequence,
        snapshot_origin,
        prepared: Some(prepared),
        qualification: CommittedQualificationEvidence {
            canonical_state_digest,
            book_state,
            snapshot_origin: qualification_snapshot,
            sequence: sequence_evidence,
            checksum,
            trading_status,
            state_revision: next_revision,
        },
        next_revision,
        trading_status,
        generation: state.generation.clone(),
        health_epoch_target: &mut state.health_epoch,
        source_valid_until_target: &mut state.source_valid_until,
        source_timestamp_target: &mut state.source_timestamp,
        received_at_target: &mut state.received_at,
        evaluated_at_target: &mut state.evaluated_at,
        health_epoch: current.current_lease().health_epoch(),
        source_valid_until: current.current_lease().valid_until(),
        source_timestamp,
        received_at: current.frame_evidence().received_at(),
        evaluated_at,
    })
}

fn require_healthy(phase: &GenerationStateMachine) -> Result<(), LiveApplyError> {
    if phase.phase() == GenerationPhase::Healthy {
        Ok(())
    } else {
        Err(LiveApplyError::SnapshotRequired)
    }
}

fn establish_non_book(
    phase: &mut GenerationStateMachine,
    current: &CurrentProviderObservation,
) -> Result<(), LiveApplyError> {
    if !matches!(
        current.policy().rule().snapshot_applicability(),
        SnapshotApplicability::NotApplicable { .. }
    ) {
        return Err(LiveApplyError::SnapshotPolicyMismatch);
    }
    match phase.phase() {
        GenerationPhase::AwaitingSnapshot => phase.establish_snapshot_not_applicable()?,
        GenerationPhase::Healthy => {}
        _ => return Err(LiveApplyError::Quarantined),
    }
    Ok(())
}

fn checksum_evidence(
    current: &CurrentProviderObservation,
    computed: Option<u32>,
) -> Result<ChecksumEvidence, LiveApplyError> {
    let generation = current.frame_evidence().binding().connection_generation();
    match current.policy().protocol().checksum() {
        ChecksumValidationProfile::Unsupported { .. } => {
            if !matches!(
                current.observation().checksum(),
                ProviderChecksumEvidence::Unsupported { .. }
            ) {
                return Err(LiveApplyError::ChecksumProfileMismatch);
            }
            Ok(ChecksumEvidence::unsupported(generation))
        }
        ChecksumValidationProfile::Provided {
            rule,
            scope,
            book_scope: Some(book_scope),
            ..
        } => {
            let Some(computed) = computed else {
                return Err(LiveApplyError::UnsupportedPayloadChecksum);
            };
            let ProviderChecksumEvidence::Provided { value, .. } = current.observation().checksum()
            else {
                return Err(LiveApplyError::ChecksumProfileMismatch);
            };
            let expected = value
                .as_str()
                .parse::<u64>()
                .map_err(|_| LiveApplyError::InvalidChecksumValue)?;
            let level_count = book_scope
                .level_count()
                .ok_or(LiveApplyError::ChecksumProfileMismatch)?;
            Ok(ChecksumEvidence::validate_book(
                ChecksumCapability::Provided,
                Some(rule.clone()),
                generation,
                Some(ChecksumScope::new(
                    book_scope.depth(),
                    u32::from(level_count.get()),
                    scope.clone(),
                )?),
                Some(ChecksumValue::new(expected)),
                Some(ChecksumValue::new(u64::from(computed))),
            )?)
        }
        ChecksumValidationProfile::Provided {
            book_scope: None, ..
        } => Err(LiveApplyError::UnsupportedPayloadChecksum),
    }
}

fn state_identity(
    generation: ConnectionGeneration,
    revision: u64,
) -> Result<SourceIdentifier, LiveApplyError> {
    Ok(SourceIdentifier::try_from(format!(
        "book-g{}-r{}",
        generation.get(),
        revision
    ))?)
}
