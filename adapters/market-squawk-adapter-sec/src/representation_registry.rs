//! Crash-safe durable truth for retrieved HTTP representations.

mod storage;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cap_std::fs::Dir;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use market_squawk_sources::{SEC_EDGAR_SOURCE_ID, route_filing_taxonomy_physical_locator};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;

#[cfg(test)]
use crate::evidence_store::PublicationCommitTestProbe;
use storage::{Snapshot, cleanup_old_snapshots, load_latest, persist_snapshot};

const SNAPSHOT_SCHEMA_VERSION: u16 = 1;
const SNAPSHOT_PREFIX: &str = "sec-source-qualified-representations-v1-";
const SNAPSHOT_SUFFIX: &str = ".json";
const STAGING_PREFIX: &str = ".sec-source-qualified-representations-v1-";
const SNAPSHOT_CHECKSUM_DOMAIN: &[u8] =
    b"market-squawk/sec-source-qualified-representation-snapshot/v1";
const MAX_REPRESENTATIONS: usize = 4_096;
const MAX_COMMITTED_SNAPSHOTS: usize = 8;
const RETAINED_SNAPSHOTS: usize = 2;
const MAX_VALIDATOR_BYTES: usize = 1_024;
const MAX_LOCATOR_BYTES: usize = 2_048;
const MAX_BULK_REPRESENTATION_BYTES: u64 = 1024 * 1024 * 1024;

/// Runtime ceilings for durable SEC representation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecRepresentationLimits {
    max_representations: usize,
    max_snapshot_bytes: u64,
    max_representation_bytes: u64,
}

impl SecRepresentationLimits {
    /// Conservative local production ceilings.
    pub const fn production_defaults() -> Self {
        Self {
            max_representations: MAX_REPRESENTATIONS,
            max_snapshot_bytes: 8 * 1024 * 1024,
            max_representation_bytes: MAX_BULK_REPRESENTATION_BYTES,
        }
    }

    /// Constructs explicit nonzero ceilings within hard parser bounds.
    pub const fn try_new(
        max_representations: usize,
        max_snapshot_bytes: u64,
        max_representation_bytes: u64,
    ) -> Result<Self, SecRepresentationError> {
        if max_representations == 0
            || max_representations > MAX_REPRESENTATIONS
            || max_snapshot_bytes == 0
            || max_snapshot_bytes > 8 * 1024 * 1024
            || max_representation_bytes == 0
        {
            return Err(SecRepresentationError::InvalidLimits);
        }
        Ok(Self {
            max_representations,
            max_snapshot_bytes,
            max_representation_bytes,
        })
    }
}

/// Opaque HTTP validators retained only for conditional representation requests.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecHttpValidators {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
}

impl SecHttpValidators {
    /// Validates bounded opaque ETag and Last-Modified header values.
    pub fn try_new(
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<Self, SecRepresentationError> {
        Ok(Self {
            etag: etag.map(validate_validator).transpose()?,
            last_modified: last_modified.map(validate_validator).transpose()?,
        })
    }

    /// Returns the opaque ETag for an `If-None-Match` request.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the opaque modification date for an `If-Modified-Since` request.
    pub fn last_modified(&self) -> Option<&str> {
        self.last_modified.as_deref()
    }

    fn is_empty(&self) -> bool {
        self.etag.is_none() && self.last_modified.is_none()
    }

    fn merged_with(&self, update: Self) -> Self {
        Self {
            etag: update.etag.or_else(|| self.etag.clone()),
            last_modified: update.last_modified.or_else(|| self.last_modified.clone()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecHttpValidatorsWire {
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
}

impl<'de> Deserialize<'de> for SecHttpValidators {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SecHttpValidatorsWire::deserialize(deserializer)?;
        Self::try_new(wire.etag.as_deref(), wire.last_modified.as_deref())
            .map_err(serde::de::Error::custom)
    }
}

/// Durable identity and first-observed availability for one exact URL representation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecRepresentation {
    source_id: SourceId,
    locator: String,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    validators: SecHttpValidators,
    retrieval_revision: u64,
}

impl SecRepresentation {
    /// Returns the exact publisher authority that produced this representation.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact authorized URL.
    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Returns the exact decoded representation digest.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns decoded bytes retained under the digest.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the trusted first local observation of this exact representation.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns the monotonic retrieval revision for this locator.
    pub const fn retrieval_revision(&self) -> u64 {
        self.retrieval_revision
    }

    /// Returns opaque conditional-request validators.
    pub const fn validators(&self) -> &SecHttpValidators {
        &self.validators
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecRepresentationWire {
    source_id: SourceId,
    locator: String,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    validators: SecHttpValidators,
    retrieval_revision: u64,
}

impl SecRepresentationWire {
    fn validate(
        self,
        limits: SecRepresentationLimits,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        validate_source_locator(&self.source_id, &self.locator)?;
        if self.evidence.algorithm() != DigestAlgorithm::Sha256
            || self.size_bytes > limits.max_representation_bytes
            || self.retrieval_revision == 0
        {
            return Err(SecRepresentationError::InvalidRecord);
        }
        Ok(SecRepresentation {
            source_id: self.source_id,
            locator: self.locator,
            evidence: self.evidence,
            size_bytes: self.size_bytes,
            first_observed_at: self.first_observed_at,
            validators: self.validators,
            retrieval_revision: self.retrieval_revision,
        })
    }
}

trait RepresentationClock: Send + Sync {
    fn now(&self) -> Result<Timestamp, SecRepresentationError>;
}

#[derive(Debug)]
struct SystemRepresentationClock;

impl RepresentationClock for SystemRepresentationClock {
    fn now(&self) -> Result<Timestamp, SecRepresentationError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SecRepresentationError::ClockUnavailable)?;
        let nanos = i64::try_from(elapsed.as_nanos())
            .map_err(|_| SecRepresentationError::ClockUnavailable)?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }
}

#[derive(Debug)]
struct RepresentationState {
    generation: u64,
    entries: BTreeMap<(SourceId, String), SecRepresentation>,
}

/// Capability-scoped crash-safe current-representation registry.
pub struct SecRepresentationRegistry {
    directory: Dir,
    limits: SecRepresentationLimits,
    state: Mutex<RepresentationState>,
    clock: Arc<dyn RepresentationClock>,
    #[cfg(test)]
    publication_probe: Option<Arc<PublicationCommitTestProbe>>,
}

impl std::fmt::Debug for SecRepresentationRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecRepresentationRegistry")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SecRepresentationRegistry {
    /// Opens a dedicated capability directory and validates every committed snapshot.
    pub fn open(
        directory: Dir,
        limits: SecRepresentationLimits,
    ) -> Result<Self, SecRepresentationError> {
        let state = load_latest(&directory, limits)?;
        Ok(Self {
            directory,
            limits,
            state: Mutex::new(state),
            clock: Arc::new(SystemRepresentationClock),
            #[cfg(test)]
            publication_probe: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_with_publication_probe(
        directory: Dir,
        limits: SecRepresentationLimits,
        publication_probe: Arc<PublicationCommitTestProbe>,
    ) -> Result<Self, SecRepresentationError> {
        let state = load_latest(&directory, limits)?;
        Ok(Self {
            directory,
            limits,
            state: Mutex::new(state),
            clock: Arc::new(SystemRepresentationClock),
            publication_probe: Some(publication_probe),
        })
    }

    /// Returns conditional validators for an already retained locator.
    pub fn conditional_request(
        &self,
        locator: &str,
    ) -> Result<Option<SecHttpValidators>, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.conditional_request_for_source(&source_id, locator)
    }

    /// Returns conditional validators for one exact source-qualified locator.
    pub fn conditional_request_for_source(
        &self,
        source_id: &SourceId,
        locator: &str,
    ) -> Result<Option<SecHttpValidators>, SecRepresentationError> {
        validate_source_locator(source_id, locator)?;
        let state = self
            .state
            .lock()
            .map_err(|_| SecRepresentationError::StatePoisoned)?;
        Ok(state
            .entries
            .get(&(source_id.clone(), locator.to_owned()))
            .and_then(|entry| (!entry.validators.is_empty()).then(|| entry.validators.clone())))
    }

    /// Returns the retained value for one exact locator without granting publication authority.
    ///
    /// This lookup does not recreate a provider capture receipt or a physical seal. Consumers must
    /// still rejoin the returned coordinates to the exact captured and sealed response.
    pub fn representation(
        &self,
        locator: &str,
    ) -> Result<Option<SecRepresentation>, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.representation_for_source(&source_id, locator)
    }

    /// Returns one exact source-qualified representation without publication authority.
    pub fn representation_for_source(
        &self,
        source_id: &SourceId,
        locator: &str,
    ) -> Result<Option<SecRepresentation>, SecRepresentationError> {
        validate_source_locator(source_id, locator)?;
        let state = self
            .state
            .lock()
            .map_err(|_| SecRepresentationError::StatePoisoned)?;
        Ok(state
            .entries
            .get(&(source_id.clone(), locator.to_owned()))
            .cloned())
    }

    /// Records a successful decoded response under trusted local receipt time.
    ///
    /// Identical bytes preserve object identity and first availability. Changed bytes advance the
    /// locator revision only after a later trusted observation has been durably published.
    pub fn record_success(
        &self,
        locator: &str,
        evidence: EvidenceDigest,
        size_bytes: u64,
        validators: SecHttpValidators,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.record_source_success_cancellable(
            &source_id,
            locator,
            evidence,
            size_bytes,
            validators,
            &CancellationToken::new(),
        )
    }

    /// Records a successful response with cooperative persistence cancellation.
    pub fn record_success_cancellable(
        &self,
        locator: &str,
        evidence: EvidenceDigest,
        size_bytes: u64,
        validators: SecHttpValidators,
        cancellation: &CancellationToken,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.record_source_success_cancellable(
            &source_id,
            locator,
            evidence,
            size_bytes,
            validators,
            cancellation,
        )
    }

    /// Records one successful source-qualified response with cooperative persistence
    /// cancellation.
    pub fn record_source_success_cancellable(
        &self,
        source_id: &SourceId,
        locator: &str,
        evidence: EvidenceDigest,
        size_bytes: u64,
        validators: SecHttpValidators,
        cancellation: &CancellationToken,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        check_cancelled(cancellation)?;
        validate_source_locator(source_id, locator)?;
        if evidence.algorithm() != DigestAlgorithm::Sha256
            || size_bytes > self.limits.max_representation_bytes
        {
            return Err(SecRepresentationError::InvalidRecord);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| SecRepresentationError::StatePoisoned)?;
        let key = (source_id.clone(), locator.to_owned());
        if let Some(existing) = state.entries.get(&key)
            && existing.evidence == evidence
            && existing.size_bytes == size_bytes
        {
            if existing.validators == validators {
                return Ok(existing.clone());
            }
            let mut updated = existing.clone();
            updated.validators = validators;
            publish_update(self, &mut state, updated.clone(), cancellation)?;
            return Ok(updated);
        }
        if state.entries.len() >= self.limits.max_representations
            && !state.entries.contains_key(&key)
        {
            return Err(SecRepresentationError::RepresentationLimitExceeded);
        }
        let observed_at = self.clock.now()?;
        let revision = match state.entries.get(&key) {
            Some(existing) => {
                if observed_at <= existing.first_observed_at {
                    return Err(SecRepresentationError::ClockRegression);
                }
                existing
                    .retrieval_revision
                    .checked_add(1)
                    .ok_or(SecRepresentationError::RevisionOverflow)?
            }
            None => 1,
        };
        let representation = SecRepresentation {
            source_id: source_id.clone(),
            locator: locator.to_owned(),
            evidence,
            size_bytes,
            first_observed_at: observed_at,
            validators,
            retrieval_revision: revision,
        };
        publish_update(self, &mut state, representation.clone(), cancellation)?;
        Ok(representation)
    }

    /// Resolves a `304 Not Modified` only against already retained exact bytes.
    pub fn record_not_modified(
        &self,
        locator: &str,
        response_validators: SecHttpValidators,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.record_source_not_modified_cancellable(
            &source_id,
            locator,
            response_validators,
            &CancellationToken::new(),
        )
    }

    /// Resolves a `304 Not Modified` with cooperative persistence cancellation.
    pub fn record_not_modified_cancellable(
        &self,
        locator: &str,
        response_validators: SecHttpValidators,
        cancellation: &CancellationToken,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecRepresentationError::InvalidSource)?;
        self.record_source_not_modified_cancellable(
            &source_id,
            locator,
            response_validators,
            cancellation,
        )
    }

    /// Resolves a source-qualified `304 Not Modified` with cooperative cancellation.
    pub fn record_source_not_modified_cancellable(
        &self,
        source_id: &SourceId,
        locator: &str,
        response_validators: SecHttpValidators,
        cancellation: &CancellationToken,
    ) -> Result<SecRepresentation, SecRepresentationError> {
        check_cancelled(cancellation)?;
        validate_source_locator(source_id, locator)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| SecRepresentationError::StatePoisoned)?;
        let existing = state
            .entries
            .get(&(source_id.clone(), locator.to_owned()))
            .cloned()
            .ok_or(SecRepresentationError::NotModifiedWithoutRepresentation)?;
        let validators = existing.validators.merged_with(response_validators);
        if validators == existing.validators {
            return Ok(existing);
        }
        let mut updated = existing;
        updated.validators = validators;
        publish_update(self, &mut state, updated.clone(), cancellation)?;
        Ok(updated)
    }
}

fn publish_update(
    registry: &SecRepresentationRegistry,
    state: &mut RepresentationState,
    representation: SecRepresentation,
    cancellation: &CancellationToken,
) -> Result<(), SecRepresentationError> {
    check_cancelled(cancellation)?;
    let generation = state
        .generation
        .checked_add(1)
        .ok_or(SecRepresentationError::GenerationOverflow)?;
    let mut entries = state.entries.clone();
    entries.insert(
        (
            representation.source_id.clone(),
            representation.locator.clone(),
        ),
        representation,
    );
    let snapshot = Snapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        generation,
        entries: entries.values().cloned().collect(),
    };
    persist_snapshot(
        &registry.directory,
        registry.limits,
        &snapshot,
        cancellation,
        #[cfg(test)]
        registry.publication_probe.as_deref(),
    )?;
    state.generation = generation;
    state.entries = entries;
    cleanup_old_snapshots(&registry.directory, generation)?;
    Ok(())
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), SecRepresentationError> {
    if cancellation.is_cancelled() {
        Err(SecRepresentationError::Cancelled)
    } else {
        Ok(())
    }
}

fn validate_source_locator(
    source_id: &SourceId,
    locator: &str,
) -> Result<(), SecRepresentationError> {
    if locator.is_empty() || locator.len() > MAX_LOCATOR_BYTES {
        return Err(SecRepresentationError::InvalidLocator);
    }
    let parsed = Url::parse(locator).map_err(|_| SecRepresentationError::InvalidLocator)?;
    if parsed.scheme() != "https"
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.as_str() != locator
    {
        return Err(SecRepresentationError::InvalidLocator);
    }
    if source_id.as_str() == SEC_EDGAR_SOURCE_ID {
        if !matches!(
            parsed.host_str(),
            Some("data.sec.gov" | "www.sec.gov" | "xbrl.sec.gov")
        ) {
            return Err(SecRepresentationError::InvalidLocator);
        }
        return Ok(());
    }
    let authority = route_filing_taxonomy_physical_locator(locator)
        .map_err(|_| SecRepresentationError::InvalidLocator)?;
    if authority.source_id() != source_id.as_str() {
        return Err(SecRepresentationError::InvalidSource);
    }
    Ok(())
}

fn validate_validator(value: &str) -> Result<String, SecRepresentationError> {
    if value.is_empty()
        || value.len() > MAX_VALIDATOR_BYTES
        || HeaderValue::from_str(value).is_err()
    {
        Err(SecRepresentationError::InvalidValidator)
    } else {
        Ok(value.to_owned())
    }
}

/// Durable SEC representation-registry failure.
#[derive(Debug, Error)]
pub enum SecRepresentationError {
    /// The caller cancelled before immutable publication completed.
    #[error("SEC representation operation was cancelled")]
    Cancelled,
    #[error("SEC representation limits are invalid")]
    InvalidLimits,
    #[error("SEC representation source authority is invalid")]
    InvalidSource,
    #[error("SEC representation locator is invalid")]
    InvalidLocator,
    #[error("SEC representation validator is invalid")]
    InvalidValidator,
    #[error("SEC representation record is invalid")]
    InvalidRecord,
    #[error("SEC representation capacity is exhausted")]
    RepresentationLimitExceeded,
    #[error("SEC representation snapshot capacity is exhausted")]
    SnapshotLimitExceeded,
    #[error("SEC representation snapshot exceeds its byte bound")]
    SnapshotTooLarge,
    #[error("SEC representation bounded allocation failed")]
    AllocationFailed,
    #[error("SEC representation snapshot name is invalid")]
    InvalidSnapshotName,
    #[error("SEC representation snapshot is invalid")]
    InvalidSnapshot,
    #[error("SEC representation snapshot digest mismatches its name")]
    SnapshotDigestMismatch,
    #[error("SEC representation snapshot content conflicts")]
    SnapshotConflict,
    #[error("SEC representation snapshot generation is duplicated")]
    DuplicateGeneration,
    #[error("SEC representation locator is duplicated")]
    DuplicateLocator,
    #[error("SEC 304 response has no retained representation")]
    NotModifiedWithoutRepresentation,
    #[error("SEC representation registry state is poisoned")]
    StatePoisoned,
    #[error("trusted SEC receipt clock is unavailable")]
    ClockUnavailable,
    #[error("trusted SEC receipt clock did not advance")]
    ClockRegression,
    #[error("SEC representation revision overflowed")]
    RevisionOverflow,
    #[error("SEC representation generation overflowed")]
    GenerationOverflow,
    #[error("SEC representation snapshot path is not a regular file")]
    NotRegularFile,
    #[error("SEC representation I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("SEC representation JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}
