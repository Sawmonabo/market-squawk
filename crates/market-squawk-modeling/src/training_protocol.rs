//! Closed validation for the bounded Python training-worker NDJSON protocol.

use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Wire schema accepted by this release.
pub const TRAINING_WORKER_SCHEMA_VERSION: u32 = 1;
/// Maximum bytes in one NDJSON frame, excluding its newline delimiter.
pub const MAX_TRAINING_WORKER_EVENT_BYTES: usize = 16 * 1024;
/// Maximum bytes in one complete stdout protocol stream.
pub const MAX_TRAINING_WORKER_STREAM_BYTES: usize = 256 * 1024;
/// Maximum frames in one worker generation.
pub const MAX_TRAINING_WORKER_EVENTS: usize = 64;
/// Maximum captured stderr bytes accepted from the process supervisor.
pub const MAX_TRAINING_WORKER_STDERR_BYTES: usize = 64 * 1024;
const MAX_MESSAGE_BYTES: usize = 192;
const MAX_PATH_BYTES: usize = 1024;
const MAX_REVISION_BYTES: usize = 128;
const MAX_OBJECTIVE_UNITS: u64 = 1_000_000_000;

/// A Python-produced candidate identity. It grants no registration or runtime authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrainingWorkerCandidate {
    admission_request_sha256: [u8; 32],
    candidate_directory: Box<str>,
    metadata_sha256: [u8; 32],
    artifact_sha256: [u8; 32],
    training_run_sha256: [u8; 32],
    authority_sha256: [u8; 32],
    dataset_export_sha256: [u8; 32],
    dataset_selection_sha256: [u8; 32],
    catalog_identity_sha256: [u8; 32],
    training_environment_sha256: [u8; 32],
    training_code_revision: Box<str>,
}

impl TrainingWorkerCandidate {
    /// Returns the claimed admission-request digest for controlled artifact resolution.
    #[must_use]
    pub const fn admission_request_sha256(&self) -> [u8; 32] {
        self.admission_request_sha256
    }

    /// Returns the untrusted candidate-directory coordinate for controlled artifact resolution.
    #[must_use]
    pub fn candidate_directory(&self) -> &str {
        &self.candidate_directory
    }

    /// Returns the claimed metadata digest that Rust must independently verify.
    #[must_use]
    pub const fn metadata_sha256(&self) -> [u8; 32] {
        self.metadata_sha256
    }

    /// Returns the claimed model-artifact digest that Rust must independently verify.
    #[must_use]
    pub const fn artifact_sha256(&self) -> [u8; 32] {
        self.artifact_sha256
    }

    /// Returns the claimed training-run receipt digest that Rust must independently verify.
    #[must_use]
    pub const fn training_run_sha256(&self) -> [u8; 32] {
        self.training_run_sha256
    }

    /// Returns the claimed independent authority digest that Rust must independently verify.
    #[must_use]
    pub const fn authority_sha256(&self) -> [u8; 32] {
        self.authority_sha256
    }

    /// Returns the claimed immutable dataset-export digest.
    #[must_use]
    pub const fn dataset_export_sha256(&self) -> [u8; 32] {
        self.dataset_export_sha256
    }

    /// Returns the claimed point-in-time dataset-selection digest.
    #[must_use]
    pub const fn dataset_selection_sha256(&self) -> [u8; 32] {
        self.dataset_selection_sha256
    }

    /// Returns the claimed hardened catalog identity.
    #[must_use]
    pub const fn catalog_identity_sha256(&self) -> [u8; 32] {
        self.catalog_identity_sha256
    }

    /// Returns the claimed sealed training-environment identity.
    #[must_use]
    pub const fn training_environment_sha256(&self) -> [u8; 32] {
        self.training_environment_sha256
    }

    /// Returns the claimed source-closure revision.
    #[must_use]
    pub fn training_code_revision(&self) -> &str {
        &self.training_code_revision
    }

    /// Revalidates the Python claim against the currently sealed Rust-verified environment.
    ///
    /// This check is necessary but not sufficient for admission. The service must also resolve the
    /// controlled candidate/request coordinates and call [`crate::verify_model_candidate`] before
    /// the single runtime admission authority is invoked.
    ///
    /// # Errors
    ///
    /// Rejects a receipt or source-closure identity mismatch.
    pub fn verify_environment(
        &self,
        environment: &crate::VerifiedTrainingEnvironment,
    ) -> Result<(), TrainingWorkerProtocolError> {
        if self.training_environment_sha256 != environment.receipt_sha256()
            || self.training_code_revision() != environment.training_code_revision()
        {
            return Err(TrainingWorkerProtocolError::CandidateEvidenceMismatch);
        }
        Ok(())
    }
}

/// Safe evidence about captured worker stderr; raw text is never retained or exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrainingWorkerStderrEvidence {
    captured_bytes: u32,
    sha256: [u8; 32],
}

impl TrainingWorkerStderrEvidence {
    /// Redacts a supervisor-bounded stderr buffer into length and digest evidence.
    ///
    /// # Errors
    ///
    /// Rejects a buffer larger than [`MAX_TRAINING_WORKER_STDERR_BYTES`].
    pub fn capture(bytes: &[u8]) -> Result<Self, TrainingWorkerProtocolError> {
        if bytes.len() > MAX_TRAINING_WORKER_STDERR_BYTES {
            return Err(TrainingWorkerProtocolError::StderrTooLarge);
        }
        Ok(Self {
            captured_bytes: u32::try_from(bytes.len())
                .map_err(|_| TrainingWorkerProtocolError::StderrTooLarge)?,
            sha256: Sha256::digest(bytes).into(),
        })
    }

    /// Returns the number of bounded bytes reduced into this redacted evidence.
    #[must_use]
    pub const fn captured_bytes(self) -> u32 {
        self.captured_bytes
    }

    /// Returns the digest of the captured bytes without exposing their contents.
    #[must_use]
    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

/// Closed phase names accepted from the Python worker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TrainingWorkerPhase {
    /// Input and environment validation.
    Validation,
    /// Deterministic fitting.
    Training,
    /// Candidate evaluation.
    Evaluation,
    /// Candidate and receipt export.
    Export,
    /// Successful candidate production.
    Complete,
    /// Explicit cancellation terminal.
    Cancelled,
    /// Failed candidate-production terminal.
    Failed,
}

/// Bounded progress that the durable job service may publish.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrainingWorkerProgress {
    phase: TrainingWorkerPhase,
    message: Box<str>,
    completed_units: u64,
    total_units: u64,
}

impl TrainingWorkerProgress {
    /// Returns the closed training phase.
    #[must_use]
    pub const fn phase(&self) -> TrainingWorkerPhase {
        self.phase
    }

    /// Returns a bounded code-owned safe message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns completed objective units.
    #[must_use]
    pub const fn completed_units(&self) -> u64 {
        self.completed_units
    }

    /// Returns total objective units.
    #[must_use]
    pub const fn total_units(&self) -> u64 {
        self.total_units
    }
}

/// One validated event from a worker generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrainingWorkerEvent {
    /// Safe bounded progress suitable for durable publication.
    Progress(TrainingWorkerProgress),
    /// Candidate evidence was staged but remains unavailable until clean process completion.
    CandidateStaged,
    /// A typed error terminal was staged.
    ErrorStaged {
        /// Bounded code-owned diagnostic code.
        diagnostic_code: Box<str>,
    },
}

/// Closed training-worker protocol rejection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TrainingWorkerProtocolError {
    /// Expected run or generation identity is invalid.
    #[error("training worker identity is invalid")]
    InvalidIdentity,
    /// A frame is malformed, non-canonical in shape, or contains an unknown field.
    #[error("training worker frame is invalid")]
    InvalidFrame,
    /// A frame exceeded its byte ceiling.
    #[error("training worker frame exceeded its byte ceiling")]
    EventTooLarge,
    /// The complete stdout protocol stream exceeded its byte ceiling.
    #[error("training worker stream exceeded its byte ceiling")]
    StreamTooLarge,
    /// The stream exceeded its event-count ceiling.
    #[error("training worker stream exceeded its event ceiling")]
    EventLimit,
    /// Run identity, generation, or sequence did not match this session.
    #[error("training worker event ordering is invalid")]
    Sequence,
    /// The stream attempted to continue after one terminal event.
    #[error("training worker stream is already terminal")]
    Terminal,
    /// A clean worker exit omitted a terminal event.
    #[error("training worker terminal event is missing")]
    MissingTerminal,
    /// The worker emitted one typed error terminal.
    #[error("training worker rejected candidate production")]
    WorkerRejected,
    /// The process failed, timed out, or was cancelled; any candidate evidence was destroyed.
    #[error("training worker process did not complete cleanly")]
    ProcessFailed,
    /// Captured stderr exceeded the process boundary's agreed ceiling.
    #[error("training worker stderr exceeded its byte ceiling")]
    StderrTooLarge,
    /// Candidate claims differ from independently verified local authority.
    #[error("training worker candidate evidence does not match local authority")]
    CandidateEvidenceMismatch,
}

/// Fail-closed validator for one run/generation-bound NDJSON stdout stream.
#[derive(Debug)]
pub struct TrainingWorkerProtocolSession {
    run_id: Box<str>,
    generation: u64,
    next_sequence: u64,
    event_count: usize,
    stream_bytes: usize,
    completed_units: u64,
    total_units: Option<u64>,
    terminal: Option<Terminal>,
    failed: bool,
}

#[derive(Debug)]
enum Terminal {
    Result(Box<TrainingWorkerCandidate>),
    Error,
}

impl TrainingWorkerProtocolSession {
    /// Begins a worker stream for one canonical UUID and positive job generation.
    ///
    /// # Errors
    ///
    /// Rejects a non-canonical UUID or zero generation.
    pub fn try_new(run_id: &str, generation: u64) -> Result<Self, TrainingWorkerProtocolError> {
        if !canonical_uuid(run_id) || generation == 0 {
            return Err(TrainingWorkerProtocolError::InvalidIdentity);
        }
        Ok(Self {
            run_id: run_id.into(),
            generation,
            next_sequence: 0,
            event_count: 0,
            stream_bytes: 0,
            completed_units: 0,
            total_units: None,
            terminal: None,
            failed: false,
        })
    }

    /// Validates one newline-stripped protocol frame.
    ///
    /// # Errors
    ///
    /// Rejects invalid UTF-8/JSON, unknown fields, identity or sequence mismatches, invalid event
    /// shapes, byte/count overflow, and every event after a terminal frame.
    pub fn accept_line(
        &mut self,
        line: &[u8],
    ) -> Result<TrainingWorkerEvent, TrainingWorkerProtocolError> {
        if self.failed || self.terminal.is_some() {
            self.fail();
            return Err(TrainingWorkerProtocolError::Terminal);
        }
        if line.is_empty() || line.len() > MAX_TRAINING_WORKER_EVENT_BYTES {
            let error = if line.len() > MAX_TRAINING_WORKER_EVENT_BYTES {
                TrainingWorkerProtocolError::EventTooLarge
            } else {
                TrainingWorkerProtocolError::InvalidFrame
            };
            self.fail();
            return Err(error);
        }
        let framed = line
            .len()
            .checked_add(1)
            .and_then(|bytes| self.stream_bytes.checked_add(bytes));
        let Some(framed) = framed else {
            self.fail();
            return Err(TrainingWorkerProtocolError::StreamTooLarge);
        };
        if framed > MAX_TRAINING_WORKER_STREAM_BYTES {
            self.fail();
            return Err(TrainingWorkerProtocolError::StreamTooLarge);
        }
        let Some(next_count) = self.event_count.checked_add(1) else {
            self.fail();
            return Err(TrainingWorkerProtocolError::EventLimit);
        };
        if next_count > MAX_TRAINING_WORKER_EVENTS {
            self.fail();
            return Err(TrainingWorkerProtocolError::EventLimit);
        }
        let wire: EventWire = match serde_json::from_slice(line) {
            Ok(wire) => wire,
            Err(_) => {
                self.fail();
                return Err(TrainingWorkerProtocolError::InvalidFrame);
            }
        };
        if wire.schema_version != TRAINING_WORKER_SCHEMA_VERSION
            || wire.run_id != self.run_id.as_ref()
            || wire.generation != self.generation
            || wire.sequence != self.next_sequence
        {
            self.fail();
            return Err(TrainingWorkerProtocolError::Sequence);
        }
        if wire.completed_units < self.completed_units
            || self
                .total_units
                .is_some_and(|total| total != wire.total_units)
        {
            self.fail();
            return Err(TrainingWorkerProtocolError::Sequence);
        }
        let completed_units = wire.completed_units;
        let total_units = wire.total_units;
        let validated = match validate_event(wire) {
            Ok(validated) => validated,
            Err(error) => {
                self.fail();
                return Err(error);
            }
        };
        let Some(next_sequence) = self.next_sequence.checked_add(1) else {
            self.fail();
            return Err(TrainingWorkerProtocolError::Sequence);
        };
        self.next_sequence = next_sequence;
        self.event_count = next_count;
        self.stream_bytes = framed;
        self.completed_units = completed_units;
        self.total_units = Some(total_units);
        self.terminal = validated.terminal;
        Ok(validated.event)
    }

    /// Releases candidate evidence only after one successful process exit and result terminal.
    ///
    /// # Errors
    ///
    /// Failed/cancelled/timed-out processes, worker error terminals, or incomplete streams destroy
    /// any staged candidate and return a closed failure.
    pub fn finish(
        &mut self,
        process_succeeded: bool,
    ) -> Result<TrainingWorkerCandidate, TrainingWorkerProtocolError> {
        if !process_succeeded || self.failed {
            self.fail();
            return Err(TrainingWorkerProtocolError::ProcessFailed);
        }
        match self.terminal.take() {
            Some(Terminal::Result(candidate)) => {
                self.failed = true;
                Ok(*candidate)
            }
            Some(Terminal::Error) => {
                self.failed = true;
                Err(TrainingWorkerProtocolError::WorkerRejected)
            }
            None => {
                self.failed = true;
                Err(TrainingWorkerProtocolError::MissingTerminal)
            }
        }
    }

    #[cfg(test)]
    fn staged_candidate(&self) -> Option<&TrainingWorkerCandidate> {
        match &self.terminal {
            Some(Terminal::Result(candidate)) if !self.failed => Some(candidate.as_ref()),
            Some(Terminal::Result(_) | Terminal::Error) | None => None,
        }
    }

    fn fail(&mut self) {
        self.failed = true;
        self.terminal = None;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventWire {
    schema_version: u32,
    run_id: String,
    generation: u64,
    sequence: u64,
    kind: EventKind,
    phase: TrainingWorkerPhase,
    message: String,
    completed_units: u64,
    total_units: u64,
    unit: String,
    diagnostic_code: Option<String>,
    result: Option<CandidateWire>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EventKind {
    Progress,
    Result,
    Error,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CandidateWire {
    admission_request_sha256: String,
    candidate_directory: String,
    metadata_sha256: String,
    artifact_sha256: String,
    training_run_sha256: String,
    authority_sha256: String,
    dataset_export_sha256: String,
    dataset_selection_sha256: String,
    catalog_identity_sha256: String,
    training_environment_sha256: String,
    training_code_revision: String,
}

struct ValidatedEvent {
    event: TrainingWorkerEvent,
    terminal: Option<Terminal>,
}

fn validate_event(wire: EventWire) -> Result<ValidatedEvent, TrainingWorkerProtocolError> {
    if wire.unit != "steps"
        || wire.total_units == 0
        || wire.total_units > MAX_OBJECTIVE_UNITS
        || wire.completed_units > wire.total_units
        || !bounded_safe_ascii(&wire.message, MAX_MESSAGE_BYTES)
    {
        return Err(TrainingWorkerProtocolError::InvalidFrame);
    }
    match (wire.kind, wire.phase, wire.diagnostic_code, wire.result) {
        (
            EventKind::Progress,
            TrainingWorkerPhase::Validation
            | TrainingWorkerPhase::Training
            | TrainingWorkerPhase::Evaluation
            | TrainingWorkerPhase::Export,
            None,
            None,
        ) => Ok(ValidatedEvent {
            event: TrainingWorkerEvent::Progress(TrainingWorkerProgress {
                phase: wire.phase,
                message: wire.message.into(),
                completed_units: wire.completed_units,
                total_units: wire.total_units,
            }),
            terminal: None,
        }),
        (EventKind::Result, TrainingWorkerPhase::Complete, None, Some(result))
            if wire.completed_units == wire.total_units =>
        {
            Ok(ValidatedEvent {
                event: TrainingWorkerEvent::CandidateStaged,
                terminal: Some(Terminal::Result(Box::new(validate_candidate(result)?))),
            })
        }
        (
            EventKind::Error,
            TrainingWorkerPhase::Cancelled | TrainingWorkerPhase::Failed,
            Some(code),
            None,
        ) if diagnostic_code(&code) => Ok(ValidatedEvent {
            event: TrainingWorkerEvent::ErrorStaged {
                diagnostic_code: code.into(),
            },
            terminal: Some(Terminal::Error),
        }),
        _ => Err(TrainingWorkerProtocolError::InvalidFrame),
    }
}

fn validate_candidate(
    wire: CandidateWire,
) -> Result<TrainingWorkerCandidate, TrainingWorkerProtocolError> {
    if !bounded_safe_ascii(&wire.candidate_directory, MAX_PATH_BYTES)
        || !bounded_safe_ascii(&wire.training_code_revision, MAX_REVISION_BYTES)
    {
        return Err(TrainingWorkerProtocolError::InvalidFrame);
    }
    Ok(TrainingWorkerCandidate {
        admission_request_sha256: parse_hex(&wire.admission_request_sha256)?,
        candidate_directory: wire.candidate_directory.into(),
        metadata_sha256: parse_hex(&wire.metadata_sha256)?,
        artifact_sha256: parse_hex(&wire.artifact_sha256)?,
        training_run_sha256: parse_hex(&wire.training_run_sha256)?,
        authority_sha256: parse_hex(&wire.authority_sha256)?,
        dataset_export_sha256: parse_hex(&wire.dataset_export_sha256)?,
        dataset_selection_sha256: parse_hex(&wire.dataset_selection_sha256)?,
        catalog_identity_sha256: parse_hex(&wire.catalog_identity_sha256)?,
        training_environment_sha256: parse_hex(&wire.training_environment_sha256)?,
        training_code_revision: wire.training_code_revision.into(),
    })
}

fn parse_hex(value: &str) -> Result<[u8; 32], TrainingWorkerProtocolError> {
    if value.len() != 64
        || value
            .as_bytes()
            .iter()
            .any(|byte| !byte.is_ascii_hexdigit())
    {
        return Err(TrainingWorkerProtocolError::InvalidFrame);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(TrainingWorkerProtocolError::InvalidFrame)?;
        let low = hex_nibble(pair[1]).ok_or(TrainingWorkerProtocolError::InvalidFrame)?;
        digest[index] = (high << 4) | low;
    }
    if digest == [0; 32] {
        return Err(TrainingWorkerProtocolError::InvalidFrame);
    }
    Ok(digest)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn bounded_safe_ascii(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii() && !byte.is_ascii_control())
}

fn diagnostic_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f')
            }
        })
}

#[cfg(test)]
mod tests {
    use super::{TrainingWorkerProtocolError, TrainingWorkerProtocolSession};

    const RUN_ID: &str = "018f3c2a-91ab-7ccd-b3de-123456789abc";
    const RESULT: &str = r#"{"schemaVersion":1,"runId":"018f3c2a-91ab-7ccd-b3de-123456789abc","generation":7,"sequence":1,"kind":"result","phase":"complete","message":"Model candidate produced for Rust validation.","completedUnits":2,"totalUnits":2,"unit":"steps","diagnosticCode":null,"result":{"admissionRequestSha256":"9999999999999999999999999999999999999999999999999999999999999999","candidateDirectory":"models/fixture-v1/candidate","metadataSha256":"1111111111111111111111111111111111111111111111111111111111111111","artifactSha256":"2222222222222222222222222222222222222222222222222222222222222222","trainingRunSha256":"3333333333333333333333333333333333333333333333333333333333333333","authoritySha256":"4444444444444444444444444444444444444444444444444444444444444444","datasetExportSha256":"5555555555555555555555555555555555555555555555555555555555555555","datasetSelectionSha256":"6666666666666666666666666666666666666666666666666666666666666666","catalogIdentitySha256":"7777777777777777777777777777777777777777777777777777777777777777","trainingEnvironmentSha256":"8888888888888888888888888888888888888888888888888888888888888888","trainingCodeRevision":"fixture-revision"}}"#;

    #[test]
    fn ordered_terminal_result_is_released_only_after_clean_exit()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = TrainingWorkerProtocolSession::try_new(RUN_ID, 7)?;
        session
            .accept_line(br#"{"schemaVersion":1,"runId":"018f3c2a-91ab-7ccd-b3de-123456789abc","generation":7,"sequence":0,"kind":"progress","phase":"validation","message":"Training request validated.","completedUnits":1,"totalUnits":2,"unit":"steps","diagnosticCode":null,"result":null}"#)
            ?;
        session.accept_line(RESULT.as_bytes())?;
        assert!(session.staged_candidate().is_some());
        let candidate = session.finish(true)?;
        assert_eq!(candidate.artifact_sha256(), [0x22; 32]);
        Ok(())
    }

    #[test]
    fn identity_sequence_shape_and_post_terminal_events_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = TrainingWorkerProtocolSession::try_new(RUN_ID, 7)?;
        let unknown = br#"{"schemaVersion":1,"runId":"018f3c2a-91ab-7ccd-b3de-123456789abc","generation":7,"sequence":0,"kind":"progress","phase":"validation","message":"Training request validated.","completedUnits":1,"totalUnits":2,"unit":"steps","diagnosticCode":null,"result":null,"extra":true}"#;
        assert_eq!(
            session.accept_line(unknown),
            Err(TrainingWorkerProtocolError::InvalidFrame)
        );
        assert!(session.staged_candidate().is_none());

        let mut session = TrainingWorkerProtocolSession::try_new(RUN_ID, 7)?;
        assert_eq!(
            session.accept_line(RESULT.as_bytes()),
            Err(TrainingWorkerProtocolError::Sequence)
        );
        assert!(session.staged_candidate().is_none());
        Ok(())
    }

    #[test]
    fn cancelled_or_failed_process_never_releases_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut session = TrainingWorkerProtocolSession::try_new(RUN_ID, 7)?;
        session
            .accept_line(br#"{"schemaVersion":1,"runId":"018f3c2a-91ab-7ccd-b3de-123456789abc","generation":7,"sequence":0,"kind":"error","phase":"cancelled","message":"Training was cancelled.","completedUnits":0,"totalUnits":4,"unit":"steps","diagnosticCode":"TRAINING_CANCELLED","result":null}"#)
            ?;
        assert_eq!(
            session.finish(false),
            Err(TrainingWorkerProtocolError::ProcessFailed)
        );
        assert!(session.staged_candidate().is_none());
        Ok(())
    }
}
