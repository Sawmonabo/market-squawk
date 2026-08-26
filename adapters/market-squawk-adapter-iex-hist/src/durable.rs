//! Restart-safe provider-local IEX HIST evidence state.
//!
//! This state deliberately stops before shared object sealing, canonical mapping, immutable
//! generation publication, or point-in-time reads. A stored capture or decode summary is audit and
//! adoption evidence only: restart may reuse it only after the root data plane independently
//! reverifies the corresponding opaque artifact.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::decode::{DecodeError, DecodeSummary};
use crate::model::Sha256Digest;
use crate::planning::{
    ColdJobPlan, IexHistPlanner, IexHistTrustedClockReading, PlanError,
};
use crate::receipt::{
    CaptureChronologyDisposition, CaptureError, PcapMaterializationReceipt,
};

// This provider envelope v3 embeds and revalidates the immutable cold-plan envelope v2 and its
// decoder-contract-bound plan identity v3. Greenfield state rejects every older local envelope.
const DURABLE_SCHEMA_VERSION: u16 = 3;
const MAX_DURABLE_CHECKPOINT_BYTES: usize = 256 * 1024;

/// Minimal crash-safe state-store seam implemented by the shared application control plane.
pub trait IexHistCheckpointStore {
    /// Loads the exact latest durable payload.
    fn load(&self) -> Result<Option<Vec<u8>>, IexHistCheckpointStoreError>;

    /// Atomically compares the current exact payload identity and stores the next payload.
    fn compare_and_swap(
        &self,
        expected_payload_sha256: Option<Sha256Digest>,
        next_payload: &[u8],
    ) -> Result<(), IexHistCheckpointStoreError>;
}

/// Shared-store failure reduced to provider-local meaning.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IexHistCheckpointStoreError {
    /// Durable state is unavailable, unsafe, corrupt, or cannot be atomically replaced.
    #[error("IEX HIST durable checkpoint store is unavailable")]
    Unavailable,
    /// Another writer changed the exact state before this transition committed.
    #[error("IEX HIST durable checkpoint compare-and-swap conflicted")]
    Conflict,
}

/// Latest provider-local evidence phase for one immutable selected-file plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistJobPhase {
    /// The complete immutable plan is restorable; no capture evidence is retained.
    Planned,
    /// Complete transport/materialization evidence is retained, but no durable object is claimed.
    CaptureEvidence,
    /// Terminal transactional decode evidence is retained, but no analytical generation exists.
    DecodeEvidence,
    /// Deterministic evidence is quarantined pending explicit root-authority review.
    Quarantined,
    /// Required evidence or authority was unavailable for this attempt.
    Unavailable,
}

/// Honest recovery action authorized by provider-local state alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistRecoveryAction {
    /// Acquire a new attempt and repeat the selected-file transfer from byte zero.
    RestartWholeFileTransfer,
    /// Root may adopt only after independently reverifying an opaque durable artifact; otherwise
    /// the transfer must restart from byte zero.
    RequireSharedArtifactAdoptionOrRestartWholeFile,
    /// Terminal evidence must be reviewed under the recorded reactivation requirement.
    AwaitReactivation,
}

/// Phase in which a terminal condition was observed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistTerminalPhase {
    /// Plan restoration or attempt admission.
    Recovery,
    /// Provider response, representation, expansion, or PCAP materialization.
    Capture,
    /// PCAP/IEX-TP decode or transactional sink commit.
    Decode,
}

/// Closed provider-local terminal classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistTerminalDisposition {
    /// Deterministic evidence must not be retried automatically.
    Quarantined,
    /// Evidence or authority was unavailable and may be retried under a new admitted attempt.
    Unavailable,
}

/// Stable error classification retained without serializing implementation error strings.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistTerminalError {
    /// Trusted response or completion chronology was quarantined.
    CaptureClockAnomaly,
    /// Provider object, gzip, PCAP, or checksum evidence was corrupt or incomplete.
    CorruptRawEvidence,
    /// Selected feed, transport, representation, or schema version was unsupported.
    UnsupportedVersion,
    /// Packet, session, sequence, or stream-offset continuity failed.
    ContinuityFault,
    /// A typed message or transactional sink could not be completed.
    DecodeFailure,
    /// Shared provider, storage, capacity, or clock authority was unavailable.
    AuthorityUnavailable,
    /// Restored state or its referenced evidence could not be revalidated.
    RecoveryEvidenceUnavailable,
}

/// Whether and when a terminal provider-local job may be attempted again.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "retry")]
pub enum IexHistRetryDisposition {
    /// No automatic retry is permitted.
    Never,
    /// A new application-authority attempt may restart the whole file no earlier than this time.
    ReacquireAndRestartWholeFile { not_before_unix_nanos: i64 },
}

/// Authority boundary required before terminal state may be superseded.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IexHistReactivationRequirement {
    /// Root authority must review and explicitly bind a permit to the terminal evidence identity.
    RootAuthorityReview,
    /// Root capacity authority must mint a new selected-plan attempt after the retry instant.
    NewAuthorityAttempt,
}

/// Exact optional decoder coordinate retained with a terminal condition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistTerminalCoordinate {
    packet_ordinal: Option<u64>,
    sequence: Option<i64>,
    stream_offset: Option<i64>,
}

impl IexHistTerminalCoordinate {
    /// Creates bounded optional packet/sequence/offset evidence.
    pub fn try_new(
        packet_ordinal: Option<u64>,
        sequence: Option<i64>,
        stream_offset: Option<i64>,
    ) -> Result<Self, IexHistCheckpointError> {
        if sequence.is_some_and(|value| value <= 0)
            || stream_offset.is_some_and(|value| value < 0)
        {
            return Err(IexHistCheckpointError::InvalidTerminalEvidence);
        }
        Ok(Self {
            packet_ordinal,
            sequence,
            stream_offset,
        })
    }

    /// Returns the zero-based PCAP packet ordinal, when known.
    #[must_use]
    pub const fn packet_ordinal(self) -> Option<u64> { self.packet_ordinal }
    /// Returns the exact IEX-TP sequence coordinate, when known.
    #[must_use]
    pub const fn sequence(self) -> Option<i64> { self.sequence }
    /// Returns the exact IEX-TP stream offset, when known.
    #[must_use]
    pub const fn stream_offset(self) -> Option<i64> { self.stream_offset }
}

/// Typed durable terminal evidence for one exact immutable plan state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IexHistTerminalEvidence {
    disposition: IexHistTerminalDisposition,
    failed_phase: IexHistTerminalPhase,
    error: IexHistTerminalError,
    coordinate: IexHistTerminalCoordinate,
    attempt_sha256: Option<Sha256Digest>,
    observed_clock: DurableClockEvidence,
    retry: IexHistRetryDisposition,
    reactivation: IexHistReactivationRequirement,
    prior_evidence_sha256: Sha256Digest,
    evidence_sha256: Sha256Digest,
}

impl IexHistTerminalEvidence {
    /// Returns the closed terminal classification.
    #[must_use]
    pub const fn disposition(&self) -> IexHistTerminalDisposition { self.disposition }
    /// Returns the phase that failed.
    #[must_use]
    pub const fn failed_phase(&self) -> IexHistTerminalPhase { self.failed_phase }
    /// Returns the stable error classification.
    #[must_use]
    pub const fn error(&self) -> IexHistTerminalError { self.error }
    /// Returns optional packet/sequence/offset evidence.
    #[must_use]
    pub const fn coordinate(&self) -> IexHistTerminalCoordinate { self.coordinate }
    /// Returns the exact producing attempt when one was known.
    #[must_use]
    pub const fn attempt_sha256(&self) -> Option<Sha256Digest> { self.attempt_sha256 }
    /// Returns the trusted wall-clock instant at which the terminal condition was observed.
    #[must_use]
    pub const fn observed_at_unix_nanos(&self) -> i64 { self.observed_clock.unix_nanos }
    /// Returns the trusted UTC offset retained with the terminal observation.
    #[must_use]
    pub const fn observed_utc_offset_seconds(&self) -> i32 {
        self.observed_clock.utc_offset_seconds
    }
    /// Returns the exact trusted local calendar date retained with the terminal observation.
    #[must_use]
    pub fn observed_date(&self) -> &str { &self.observed_clock.observed_date }
    /// Returns the retry policy recorded at the terminal CAS boundary.
    #[must_use]
    pub const fn retry(&self) -> IexHistRetryDisposition { self.retry }
    /// Returns the authority required to supersede this terminal state.
    #[must_use]
    pub const fn reactivation(&self) -> IexHistReactivationRequirement { self.reactivation }
    /// Returns the complete terminal evidence identity.
    #[must_use]
    pub const fn evidence_sha256(&self) -> Sha256Digest { self.evidence_sha256 }
    /// Returns the exact predecessor evidence identity bound into this terminal result.
    #[must_use]
    pub const fn prior_evidence_sha256(&self) -> Sha256Digest {
        self.prior_evidence_sha256
    }
}

/// Sole owner of one selected job's provider-local durable evidence progression.
pub struct IexHistDurableJob<S> {
    store: S,
    plan: ColdJobPlan,
    envelope: DurableEnvelope,
    payload_sha256: Sha256Digest,
}

impl<S> std::fmt::Debug for IexHistDurableJob<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let phase = match &self.envelope.phase {
            DurablePhase::Planned => IexHistJobPhase::Planned,
            DurablePhase::CaptureEvidence { .. } => IexHistJobPhase::CaptureEvidence,
            DurablePhase::DecodeEvidence { .. } => IexHistJobPhase::DecodeEvidence,
            DurablePhase::Terminal { terminal, .. } => match terminal.disposition {
                IexHistTerminalDisposition::Quarantined => IexHistJobPhase::Quarantined,
                IexHistTerminalDisposition::Unavailable => IexHistJobPhase::Unavailable,
            },
        };
        formatter
            .debug_struct("IexHistDurableJob")
            .field("state_version", &self.envelope.state_version)
            .field("phase", &phase)
            .field("plan_sha256", &self.plan.plan_sha256())
            .finish_non_exhaustive()
    }
}

impl<S: IexHistCheckpointStore> IexHistDurableJob<S> {
    /// Creates a checkpoint or independently restores its complete immutable plan.
    pub fn try_open(plan: &ColdJobPlan, store: S) -> Result<Self, IexHistCheckpointError> {
        let loaded = store.load()?;
        if let Some(payload) = loaded {
            let value = Self::decode_existing(store, payload)?;
            if value.plan.plan_sha256() != plan.plan_sha256() {
                return Err(IexHistCheckpointError::PlanMismatch);
            }
            return Ok(value);
        }
        Self::create(plan, store)
    }

    /// Restores from the closed durable envelope without an original in-memory plan.
    pub fn restore(store: S) -> Result<Self, IexHistCheckpointError> {
        let payload = store.load()?.ok_or(IexHistCheckpointError::MissingState)?;
        Self::decode_existing(store, payload)
    }

    fn create(plan: &ColdJobPlan, store: S) -> Result<Self, IexHistCheckpointError> {
        let plan_envelope = plan.durable_envelope()?;
        let mut envelope = DurableEnvelope {
            schema_version: DURABLE_SCHEMA_VERSION,
            state_version: 1,
            plan_sha256: plan.plan_sha256(),
            decode_contract_sha256: plan.decode_contract().contract_sha256(),
            plan_envelope,
            phase: DurablePhase::Planned,
            envelope_sha256: Sha256Digest::of(b"pending-envelope-identity"),
        };
        envelope.envelope_sha256 = envelope_identity(&envelope)?;
        envelope.validate(plan)?;
        let payload = encode_envelope(&envelope)?;
        store.compare_and_swap(None, &payload)?;
        Ok(Self {
            store,
            plan: plan.clone(),
            envelope,
            payload_sha256: Sha256Digest::of(&payload),
        })
    }

    fn decode_existing(store: S, payload: Vec<u8>) -> Result<Self, IexHistCheckpointError> {
        if payload.is_empty() || payload.len() > MAX_DURABLE_CHECKPOINT_BYTES {
            return Err(IexHistCheckpointError::InvalidState);
        }
        let envelope: DurableEnvelope =
            serde_json::from_slice(&payload).map_err(IexHistCheckpointError::Decode)?;
        let plan = IexHistPlanner::restore(&envelope.plan_envelope)?;
        envelope.validate(&plan)?;
        Ok(Self {
            store,
            plan,
            envelope,
            payload_sha256: Sha256Digest::of(&payload),
        })
    }

    /// Returns the independently restored immutable plan.
    #[must_use]
    pub const fn plan(&self) -> &ColdJobPlan { &self.plan }

    /// Returns the monotonically increasing durable state version.
    #[must_use]
    pub const fn state_version(&self) -> u64 { self.envelope.state_version }

    /// Returns the latest provider-local evidence phase.
    #[must_use]
    pub const fn phase(&self) -> IexHistJobPhase {
        match &self.envelope.phase {
            DurablePhase::Planned => IexHistJobPhase::Planned,
            DurablePhase::CaptureEvidence { .. } => IexHistJobPhase::CaptureEvidence,
            DurablePhase::DecodeEvidence { .. } => IexHistJobPhase::DecodeEvidence,
            DurablePhase::Terminal { terminal, .. } => match terminal.disposition {
                IexHistTerminalDisposition::Quarantined => IexHistJobPhase::Quarantined,
                IexHistTerminalDisposition::Unavailable => IexHistJobPhase::Unavailable,
            },
        }
    }

    /// Returns the only recovery action provider-local state can authorize.
    #[must_use]
    pub const fn recovery_action(&self) -> IexHistRecoveryAction {
        match &self.envelope.phase {
            DurablePhase::Planned => IexHistRecoveryAction::RestartWholeFileTransfer,
            DurablePhase::CaptureEvidence { .. } | DurablePhase::DecodeEvidence { .. } => {
                IexHistRecoveryAction::RequireSharedArtifactAdoptionOrRestartWholeFile
            }
            DurablePhase::Terminal { .. } => IexHistRecoveryAction::AwaitReactivation,
        }
    }

    /// Returns retained capture evidence for audit or root-owned external reverification only.
    #[must_use]
    pub fn capture_evidence(&self) -> Option<&PcapMaterializationReceipt> {
        phase_capture(&self.envelope.phase)
    }

    /// Returns retained transactional decode evidence; it is not a publication receipt.
    #[must_use]
    pub fn decode_evidence(&self) -> Option<&DecodeSummary> {
        match &self.envelope.phase {
            DurablePhase::DecodeEvidence { decode, .. } => Some(decode.as_ref()),
            DurablePhase::Terminal { prior, .. } => match prior.as_ref() {
                DurableNonTerminalPhase::DecodeEvidence { decode, .. } => Some(decode.as_ref()),
                DurableNonTerminalPhase::Planned | DurableNonTerminalPhase::CaptureEvidence { .. } => None,
            },
            DurablePhase::Planned | DurablePhase::CaptureEvidence { .. } => None,
        }
    }

    /// Returns typed terminal evidence when the generation is closed.
    #[must_use]
    pub fn terminal_evidence(&self) -> Option<&IexHistTerminalEvidence> {
        match &self.envelope.phase {
            DurablePhase::Terminal { terminal, .. } => Some(terminal.as_ref()),
            DurablePhase::Planned
            | DurablePhase::CaptureEvidence { .. }
            | DurablePhase::DecodeEvidence { .. } => None,
        }
    }

    /// Commits complete provider-local capture evidence.
    ///
    /// Quarantined chronology is atomically recorded as terminal evidence instead of becoming
    /// decode-authorizing state.
    pub fn record_capture(
        &mut self,
        plan: &ColdJobPlan,
        capture: PcapMaterializationReceipt,
        observed_clock: IexHistTrustedClockReading,
    ) -> Result<(), IexHistCheckpointError> {
        self.validate_plan(plan)?;
        capture.validate_against(plan)?;
        if let CaptureChronologyDisposition::Quarantined(_) = capture.chronology_disposition() {
            return self.commit_terminal(
                observed_clock,
                IexHistTerminalDisposition::Quarantined,
                IexHistTerminalPhase::Capture,
                IexHistTerminalError::CaptureClockAnomaly,
                IexHistTerminalCoordinate::try_new(None, None, None)?,
                Some(capture.attempt_sha256()),
                IexHistRetryDisposition::Never,
                Some(capture),
            );
        }
        match &self.envelope.phase {
            DurablePhase::Planned => self.commit_phase(DurablePhase::CaptureEvidence {
                capture: Box::new(capture),
            }),
            DurablePhase::CaptureEvidence { capture: current } if current.as_ref() == &capture => Ok(()),
            DurablePhase::DecodeEvidence { capture: current, .. } if current.as_ref() == &capture => Ok(()),
            DurablePhase::Terminal { .. }
            | DurablePhase::CaptureEvidence { .. }
            | DurablePhase::DecodeEvidence { .. } => Err(IexHistCheckpointError::InvalidTransition),
        }
    }

    /// Commits terminal transactional decode accounting above the exact capture parent.
    pub fn record_decoded(
        &mut self,
        plan: &ColdJobPlan,
        capture: &PcapMaterializationReceipt,
        decode: DecodeSummary,
    ) -> Result<(), IexHistCheckpointError> {
        self.validate_plan(plan)?;
        capture.validate_against(plan)?;
        if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
            return Err(IexHistCheckpointError::InvalidTransition);
        }
        decode.validate_against(plan, capture, decode.decode_attempt_evidence)?;
        match &self.envelope.phase {
            DurablePhase::CaptureEvidence { capture: current } if current.as_ref() == capture => {
                self.commit_phase(DurablePhase::DecodeEvidence {
                    capture: Box::new(capture.clone()),
                    decode: Box::new(decode),
                })
            }
            DurablePhase::DecodeEvidence { capture: current, decode: existing }
                if current.as_ref() == capture && existing.as_ref() == &decode => Ok(()),
            DurablePhase::Planned
            | DurablePhase::CaptureEvidence { .. }
            | DurablePhase::DecodeEvidence { .. }
            | DurablePhase::Terminal { .. } => Err(IexHistCheckpointError::InvalidTransition),
        }
    }

    /// Atomically closes the current provider-local job with exact typed recovery evidence.
    #[allow(clippy::too_many_arguments, reason = "terminal recovery retains complete typed evidence")]
    pub fn record_terminal(
        &mut self,
        plan: &ColdJobPlan,
        observed_clock: IexHistTrustedClockReading,
        disposition: IexHistTerminalDisposition,
        failed_phase: IexHistTerminalPhase,
        error: IexHistTerminalError,
        coordinate: IexHistTerminalCoordinate,
        retry: IexHistRetryDisposition,
    ) -> Result<(), IexHistCheckpointError> {
        self.validate_plan(plan)?;
        let attempt_sha256 = phase_attempt_sha256(&self.envelope.phase);
        self.commit_terminal(
            observed_clock,
            disposition,
            failed_phase,
            error,
            coordinate,
            attempt_sha256,
            retry,
            None,
        )
    }

    fn commit_terminal(
        &mut self,
        observed_clock: IexHistTrustedClockReading,
        disposition: IexHistTerminalDisposition,
        failed_phase: IexHistTerminalPhase,
        error: IexHistTerminalError,
        coordinate: IexHistTerminalCoordinate,
        attempt_sha256: Option<Sha256Digest>,
        retry: IexHistRetryDisposition,
        replacement_capture: Option<PcapMaterializationReceipt>,
    ) -> Result<(), IexHistCheckpointError> {
        let prior = match &self.envelope.phase {
            DurablePhase::Planned => DurableNonTerminalPhase::Planned,
            DurablePhase::CaptureEvidence { capture } => DurableNonTerminalPhase::CaptureEvidence {
                capture: capture.clone(),
            },
            DurablePhase::DecodeEvidence { capture, decode } => DurableNonTerminalPhase::DecodeEvidence {
                capture: capture.clone(),
                decode: decode.clone(),
            },
            DurablePhase::Terminal { .. } => return Err(IexHistCheckpointError::InvalidTransition),
        };
        let prior = replacement_capture.map_or(prior, |capture| {
            DurableNonTerminalPhase::CaptureEvidence { capture: Box::new(capture) }
        });
        let prior_evidence_sha256 = nonterminal_identity(&prior);
        let observed_clock = DurableClockEvidence::from_trusted(observed_clock);
        let reactivation = match disposition {
            IexHistTerminalDisposition::Quarantined => IexHistReactivationRequirement::RootAuthorityReview,
            IexHistTerminalDisposition::Unavailable => IexHistReactivationRequirement::NewAuthorityAttempt,
        };
        let evidence_sha256 = terminal_identity(
            disposition,
            failed_phase,
            error,
            coordinate,
            attempt_sha256,
            &observed_clock,
            retry,
            reactivation,
            prior_evidence_sha256,
        );
        let terminal = IexHistTerminalEvidence {
            disposition,
            failed_phase,
            error,
            coordinate,
            attempt_sha256,
            observed_clock,
            retry,
            reactivation,
            prior_evidence_sha256,
            evidence_sha256,
        };
        terminal.validate()?;
        self.commit_phase(DurablePhase::Terminal {
            prior: Box::new(prior),
            terminal: Box::new(terminal),
        })
    }

    fn validate_plan(&self, plan: &ColdJobPlan) -> Result<(), IexHistCheckpointError> {
        if self.plan.plan_sha256() != plan.plan_sha256() {
            Err(IexHistCheckpointError::PlanMismatch)
        } else {
            Ok(())
        }
    }

    fn commit_phase(&mut self, phase: DurablePhase) -> Result<(), IexHistCheckpointError> {
        let state_version = self.envelope.state_version.checked_add(1)
            .ok_or(IexHistCheckpointError::StateVersionExhausted)?;
        let mut next = DurableEnvelope {
            schema_version: DURABLE_SCHEMA_VERSION,
            state_version,
            plan_sha256: self.envelope.plan_sha256,
            decode_contract_sha256: self.envelope.decode_contract_sha256,
            plan_envelope: self.envelope.plan_envelope.clone(),
            phase,
            envelope_sha256: Sha256Digest::of(b"pending-envelope-identity"),
        };
        next.envelope_sha256 = envelope_identity(&next)?;
        next.validate(&self.plan)?;
        let payload = encode_envelope(&next)?;
        self.store.compare_and_swap(Some(self.payload_sha256), &payload)?;
        self.payload_sha256 = Sha256Digest::of(&payload);
        self.envelope = next;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableEnvelope {
    schema_version: u16,
    state_version: u64,
    plan_sha256: Sha256Digest,
    decode_contract_sha256: Sha256Digest,
    plan_envelope: Vec<u8>,
    phase: DurablePhase,
    envelope_sha256: Sha256Digest,
}

impl DurableEnvelope {
    fn validate(&self, plan: &ColdJobPlan) -> Result<(), IexHistCheckpointError> {
        if self.schema_version != DURABLE_SCHEMA_VERSION
            || self.state_version == 0
            || self.plan_sha256 != plan.plan_sha256()
            || self.decode_contract_sha256 != plan.decode_contract().contract_sha256()
            || IexHistPlanner::restore(&self.plan_envelope)?.plan_sha256() != self.plan_sha256
            || envelope_identity(self)? != self.envelope_sha256
        {
            return Err(IexHistCheckpointError::InvalidState);
        }
        validate_phase(plan, &self.phase)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "phase")]
enum DurablePhase {
    Planned,
    CaptureEvidence { capture: Box<PcapMaterializationReceipt> },
    DecodeEvidence {
        capture: Box<PcapMaterializationReceipt>,
        decode: Box<DecodeSummary>,
    },
    Terminal {
        prior: Box<DurableNonTerminalPhase>,
        terminal: Box<IexHistTerminalEvidence>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "phase")]
enum DurableNonTerminalPhase {
    Planned,
    CaptureEvidence { capture: Box<PcapMaterializationReceipt> },
    DecodeEvidence {
        capture: Box<PcapMaterializationReceipt>,
        decode: Box<DecodeSummary>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableClockEvidence {
    unix_nanos: i64,
    utc_offset_seconds: i32,
    observed_date: String,
}

impl DurableClockEvidence {
    fn from_trusted(value: IexHistTrustedClockReading) -> Self {
        Self {
            unix_nanos: value.unix_nanos(),
            utc_offset_seconds: value.utc_offset_seconds(),
            observed_date: value.observed_date().compact(),
        }
    }

    fn validate(&self) -> Result<(), IexHistCheckpointError> {
        let date = crate::model::TradeDate::parse(&self.observed_date)
            .map_err(|_| IexHistCheckpointError::InvalidTerminalEvidence)?;
        IexHistTrustedClockReading::try_new(self.unix_nanos, self.utc_offset_seconds, date)
            .map_err(|_| IexHistCheckpointError::InvalidTerminalEvidence)?;
        Ok(())
    }
}

impl IexHistTerminalEvidence {
    fn validate(&self) -> Result<(), IexHistCheckpointError> {
        IexHistTerminalCoordinate::try_new(
            self.coordinate.packet_ordinal,
            self.coordinate.sequence,
            self.coordinate.stream_offset,
        )?;
        self.observed_clock.validate()?;
        let valid_policy = matches!(
            (self.disposition, self.retry, self.reactivation),
            (
                IexHistTerminalDisposition::Quarantined,
                IexHistRetryDisposition::Never,
                IexHistReactivationRequirement::RootAuthorityReview,
            ) | (
                IexHistTerminalDisposition::Unavailable,
                IexHistRetryDisposition::ReacquireAndRestartWholeFile { .. },
                IexHistReactivationRequirement::NewAuthorityAttempt,
            )
        );
        if !valid_policy
            || matches!(self.retry, IexHistRetryDisposition::ReacquireAndRestartWholeFile {
                not_before_unix_nanos
            } if not_before_unix_nanos < self.observed_clock.unix_nanos)
            || terminal_identity(
                self.disposition,
                self.failed_phase,
                self.error,
                self.coordinate,
                self.attempt_sha256,
                &self.observed_clock,
                self.retry,
                self.reactivation,
                self.prior_evidence_sha256,
            ) != self.evidence_sha256
        {
            return Err(IexHistCheckpointError::InvalidTerminalEvidence);
        }
        Ok(())
    }
}

fn validate_phase(plan: &ColdJobPlan, phase: &DurablePhase) -> Result<(), IexHistCheckpointError> {
    match phase {
        DurablePhase::Planned => Ok(()),
        DurablePhase::CaptureEvidence { capture } => {
            capture.validate_against(plan)?;
            if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
                return Err(IexHistCheckpointError::InvalidState);
            }
            Ok(())
        }
        DurablePhase::DecodeEvidence { capture, decode } => {
            capture.validate_against(plan)?;
            if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
                return Err(IexHistCheckpointError::InvalidState);
            }
            decode.validate_against(plan, capture, decode.decode_attempt_evidence)?;
            Ok(())
        }
        DurablePhase::Terminal { prior, terminal } => {
            validate_nonterminal(plan, prior)?;
            terminal.validate()?;
            if terminal.prior_evidence_sha256 != nonterminal_identity(prior)
                || terminal.attempt_sha256 != nonterminal_attempt_sha256(prior)
            {
                return Err(IexHistCheckpointError::InvalidState);
            }
            Ok(())
        }
    }
}

fn validate_nonterminal(
    plan: &ColdJobPlan,
    phase: &DurableNonTerminalPhase,
) -> Result<(), IexHistCheckpointError> {
    match phase {
        DurableNonTerminalPhase::Planned => Ok(()),
        DurableNonTerminalPhase::CaptureEvidence { capture } => {
            capture.validate_against(plan).map_err(Into::into)
        }
        DurableNonTerminalPhase::DecodeEvidence { capture, decode } => {
            capture.validate_against(plan)?;
            if capture.chronology_disposition() != CaptureChronologyDisposition::Admitted {
                return Err(IexHistCheckpointError::InvalidState);
            }
            decode.validate_against(plan, capture, decode.decode_attempt_evidence)?;
            Ok(())
        }
    }
}

fn phase_capture(phase: &DurablePhase) -> Option<&PcapMaterializationReceipt> {
    match phase {
        DurablePhase::CaptureEvidence { capture } | DurablePhase::DecodeEvidence { capture, .. } => {
            Some(capture.as_ref())
        }
        DurablePhase::Terminal { prior, .. } => match prior.as_ref() {
            DurableNonTerminalPhase::CaptureEvidence { capture }
            | DurableNonTerminalPhase::DecodeEvidence { capture, .. } => Some(capture.as_ref()),
            DurableNonTerminalPhase::Planned => None,
        },
        DurablePhase::Planned => None,
    }
}

fn phase_attempt_sha256(phase: &DurablePhase) -> Option<Sha256Digest> {
    match phase {
        DurablePhase::Planned => None,
        DurablePhase::CaptureEvidence { capture } => Some(capture.attempt_sha256()),
        DurablePhase::DecodeEvidence { decode, .. } => Some(decode.decode_attempt_sha256),
        DurablePhase::Terminal { prior, .. } => nonterminal_attempt_sha256(prior),
    }
}

fn nonterminal_attempt_sha256(phase: &DurableNonTerminalPhase) -> Option<Sha256Digest> {
    match phase {
        DurableNonTerminalPhase::Planned => None,
        DurableNonTerminalPhase::CaptureEvidence { capture } => {
            Some(capture.attempt_sha256())
        }
        DurableNonTerminalPhase::DecodeEvidence { decode, .. } => {
            Some(decode.decode_attempt_sha256)
        }
    }
}

fn nonterminal_identity(phase: &DurableNonTerminalPhase) -> Sha256Digest {
    match phase {
        DurableNonTerminalPhase::Planned => crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-durable-prior/v2",
            b"planned",
        ]),
        DurableNonTerminalPhase::CaptureEvidence { capture } => crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-durable-prior/v2",
            b"capture_evidence",
            capture.receipt_sha256().as_bytes(),
        ]),
        DurableNonTerminalPhase::DecodeEvidence { capture, decode } => crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-durable-prior/v2",
            b"decode_evidence",
            capture.receipt_sha256().as_bytes(),
            decode.decoder_contract_sha256.as_bytes(),
            decode.decode_attempt_evidence_sha256.as_bytes(),
            decode.summary_sha256().as_bytes(),
        ]),
    }
}

fn terminal_identity(
    disposition: IexHistTerminalDisposition,
    phase: IexHistTerminalPhase,
    error: IexHistTerminalError,
    coordinate: IexHistTerminalCoordinate,
    attempt_sha256: Option<Sha256Digest>,
    clock: &DurableClockEvidence,
    retry: IexHistRetryDisposition,
    reactivation: IexHistReactivationRequirement,
    prior_evidence_sha256: Sha256Digest,
) -> Sha256Digest {
    let packet = coordinate.packet_ordinal.unwrap_or(u64::MAX);
    let sequence = coordinate.sequence.unwrap_or(i64::MIN);
    let stream_offset = coordinate.stream_offset.unwrap_or(i64::MIN);
    let attempt = attempt_sha256.unwrap_or_else(|| Sha256Digest::of(b"no-attempt"));
    let (retry_tag, retry_at) = match retry {
        IexHistRetryDisposition::Never => (0_u8, i64::MIN),
        IexHistRetryDisposition::ReacquireAndRestartWholeFile { not_before_unix_nanos } => {
            (1, not_before_unix_nanos)
        }
    };
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-terminal-evidence/v2",
        &[disposition as u8],
        &[phase as u8],
        &[error as u8],
        &packet.to_le_bytes(),
        &sequence.to_le_bytes(),
        &stream_offset.to_le_bytes(),
        attempt.as_bytes(),
        &clock.unix_nanos.to_le_bytes(),
        &clock.utc_offset_seconds.to_le_bytes(),
        clock.observed_date.as_bytes(),
        &[retry_tag],
        &retry_at.to_le_bytes(),
        &[reactivation as u8],
        prior_evidence_sha256.as_bytes(),
    ])
}

fn envelope_identity(envelope: &DurableEnvelope) -> Result<Sha256Digest, IexHistCheckpointError> {
    let phase_identity = match &envelope.phase {
        DurablePhase::Planned => Sha256Digest::of(b"planned"),
        DurablePhase::CaptureEvidence { capture } => capture.receipt_sha256(),
        DurablePhase::DecodeEvidence { decode, .. } => decode.summary_sha256(),
        DurablePhase::Terminal { terminal, .. } => terminal.evidence_sha256,
    };
    Ok(crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-durable-envelope/v3",
        &envelope.schema_version.to_le_bytes(),
        &envelope.state_version.to_le_bytes(),
        envelope.plan_sha256.as_bytes(),
        envelope.decode_contract_sha256.as_bytes(),
        Sha256Digest::of(&envelope.plan_envelope).as_bytes(),
        phase_identity.as_bytes(),
    ]))
}

fn encode_envelope(envelope: &DurableEnvelope) -> Result<Vec<u8>, IexHistCheckpointError> {
    let payload = serde_json::to_vec(envelope).map_err(IexHistCheckpointError::Encode)?;
    if payload.is_empty() || payload.len() > MAX_DURABLE_CHECKPOINT_BYTES {
        return Err(IexHistCheckpointError::InvalidState);
    }
    Ok(payload)
}

/// Fail-closed durable-job recovery or transition error.
#[derive(Debug, Error)]
pub enum IexHistCheckpointError {
    /// Shared crash-safe state authority failed.
    #[error("IEX HIST durable checkpoint store failed: {0}")]
    Store(#[from] IexHistCheckpointStoreError),
    /// Closed immutable plan restoration failed.
    #[error("IEX HIST durable plan restoration failed: {0}")]
    Plan(#[from] PlanError),
    /// Durable JSON could not be decoded through the closed schema.
    #[error("IEX HIST durable checkpoint could not be decoded")]
    Decode(serde_json::Error),
    /// Durable JSON could not be encoded.
    #[error("IEX HIST durable checkpoint could not be encoded")]
    Encode(serde_json::Error),
    /// No durable state exists to restore.
    #[error("IEX HIST durable checkpoint does not exist")]
    MissingState,
    /// The caller supplied a different immutable plan.
    #[error("IEX HIST durable checkpoint plan does not match")]
    PlanMismatch,
    /// State version, plan envelope, phase evidence, or payload bounds were invalid.
    #[error("IEX HIST durable checkpoint state is invalid")]
    InvalidState,
    /// A phase was skipped, replaced with conflicting evidence, or moved backward.
    #[error("IEX HIST durable checkpoint transition is invalid")]
    InvalidTransition,
    /// Terminal phase, coordinates, clock, retry, or reactivation evidence was invalid.
    #[error("IEX HIST terminal recovery evidence is invalid")]
    InvalidTerminalEvidence,
    /// Monotonic durable state version overflowed.
    #[error("IEX HIST durable checkpoint state version is exhausted")]
    StateVersionExhausted,
    /// Exact transport/materialization evidence was invalid.
    #[error("IEX HIST durable capture evidence is invalid: {0}")]
    Capture(#[from] CaptureError),
    /// Exact terminal decode evidence was invalid.
    #[error("IEX HIST durable decode evidence is invalid: {0}")]
    DecodeEvidence(#[from] DecodeError),
}
