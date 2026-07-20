//! Immutable, operation-specific source-rights evidence and ingest identities.

use std::collections::BTreeSet;
use std::fmt;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const MAX_TERMS_URL_BYTES: usize = 2_048;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;

/// Composition-held capability for durable rights registration.
pub(crate) struct RightsRegistrar {
    pub(crate) catalog_id: Uuid,
}

impl fmt::Debug for RightsRegistrar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RightsRegistrar([SEALED CATALOG AUTHORITY])")
    }
}

/// Sealed handle to one durable rights grant admitted by this catalog session.
#[derive(Clone, Eq, PartialEq)]
pub struct RegisteredRightsGrant {
    pub(crate) catalog_id: Uuid,
    pub(crate) rights_id: [u8; 32],
}

impl RegisteredRightsGrant {
    /// Returns the durable grant identity for local control-plane persistence.
    pub const fn rights_id(&self) -> [u8; 32] {
        self.rights_id
    }
}

impl fmt::Debug for RegisteredRightsGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredRightsGrant([SEALED GRANT])")
    }
}

/// An operation independently admitted by retained source-rights evidence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SourceOperation {
    /// Retrieve bytes from the declared source.
    Retrieve,
    /// Display observations to the authorized local user.
    Display,
    /// Persist normalized observations or source payloads.
    Persist,
    /// Retain a reusable local cache.
    Cache,
    /// Redistribute data outside the authorized local installation.
    Redistribute,
    /// Use observations for model training.
    Train,
}

impl SourceOperation {
    pub(crate) const fn mask(self) -> u8 {
        match self {
            Self::Retrieve => 1 << 0,
            Self::Display => 1 << 1,
            Self::Persist => 1 << 2,
            Self::Cache => 1 << 3,
            Self::Redistribute => 1 << 4,
            Self::Train => 1 << 5,
        }
    }

    pub(crate) const fn database_name(self) -> &'static str {
        match self {
            Self::Retrieve => "retrieve",
            Self::Display => "display",
            Self::Persist => "persist",
            Self::Cache => "cache",
            Self::Redistribute => "redistribute",
            Self::Train => "train",
        }
    }

    pub(crate) fn from_database_name(value: &str) -> Option<Self> {
        match value {
            "retrieve" => Some(Self::Retrieve),
            "display" => Some(Self::Display),
            "persist" => Some(Self::Persist),
            "cache" => Some(Self::Cache),
            "redistribute" => Some(Self::Redistribute),
            "train" => Some(Self::Train),
            _ => None,
        }
    }
}

/// Complete evidence input for one immutable rights decision.
#[derive(Clone, Debug)]
pub struct RightsDecisionInput {
    /// Source namespace covered by the decision.
    pub source_id: SourceId,
    /// Exact retrieved payload covered by the decision.
    pub payload_digest: EvidenceDigest,
    /// Time the covered payload was retrieved.
    pub retrieved_at: Timestamp,
    /// Exact HTTPS terms or license reference reviewed for the decision.
    pub terms_url: String,
    /// Digest of the exact terms or license content.
    pub terms_digest: EvidenceDigest,
    /// Digest of the authorization, entitlement, or ownership evidence.
    pub authorization_evidence: EvidenceDigest,
    /// Exclusive authorization expiry, when the evidence is time-limited.
    pub authorization_expires_at: Option<Timestamp>,
    /// Operations independently admitted by the evidence.
    pub permitted_operations: Vec<SourceOperation>,
}

/// Immutable rights evidence that cannot be widened by extraction output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceRightsDecision {
    source_id: SourceId,
    payload_digest: EvidenceDigest,
    retrieved_at: Timestamp,
    terms_url: String,
    terms_digest: EvidenceDigest,
    authorization_evidence: EvidenceDigest,
    authorization_expires_at: Option<Timestamp>,
    permitted_operations: BTreeSet<SourceOperation>,
    fingerprint: [u8; 32],
}

impl SourceRightsDecision {
    /// Validates exact terms, authorization, time, and operation evidence.
    ///
    /// # Errors
    ///
    /// Rejects non-HTTPS or oversized terms references, empty/duplicate operation sets, and an
    /// authorization expiry that is not after retrieval.
    pub(crate) fn try_new(input: RightsDecisionInput) -> Result<Self, RightsError> {
        if input.terms_url.len() > MAX_TERMS_URL_BYTES
            || input
                .terms_url
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(RightsError::InvalidTermsReference);
        }
        if input.permitted_operations.len() > 6 {
            return Err(RightsError::DuplicateOperation);
        }
        let terms_url =
            Url::parse(&input.terms_url).map_err(|_| RightsError::InvalidTermsReference)?;
        if terms_url.scheme() != "https"
            || terms_url.host_str().is_none()
            || !terms_url.username().is_empty()
            || terms_url.password().is_some()
            || terms_url.fragment().is_some()
            || terms_url.cannot_be_a_base()
            || terms_url.as_str() != input.terms_url
        {
            return Err(RightsError::InvalidTermsReference);
        }
        if input
            .authorization_expires_at
            .is_some_and(|expiry| expiry <= input.retrieved_at)
        {
            return Err(RightsError::InvalidAuthorizationExpiry);
        }
        let permitted_operations: BTreeSet<_> =
            input.permitted_operations.iter().copied().collect();
        if permitted_operations.is_empty() {
            return Err(RightsError::NoPermittedOperation);
        }
        if permitted_operations.len() != input.permitted_operations.len() {
            return Err(RightsError::DuplicateOperation);
        }
        let mut decision = Self {
            source_id: input.source_id,
            payload_digest: input.payload_digest,
            retrieved_at: input.retrieved_at,
            terms_url: input.terms_url,
            terms_digest: input.terms_digest,
            authorization_evidence: input.authorization_evidence,
            authorization_expires_at: input.authorization_expires_at,
            permitted_operations,
            fingerprint: [0; 32],
        };
        decision.fingerprint = decision.compute_fingerprint();
        Ok(decision)
    }

    pub(crate) fn validate_at(&self, admitted_at: Timestamp) -> Result<(), RightsError> {
        if admitted_at < self.retrieved_at {
            return Err(RightsError::AdmissionBeforeRetrieval);
        }
        if self
            .authorization_expires_at
            .is_some_and(|expiry| admitted_at >= expiry)
        {
            return Err(RightsError::AuthorizationExpired);
        }
        Ok(())
    }

    pub(crate) fn authorize_at(
        &self,
        request: &IngestIdentity,
        admitted_at: Timestamp,
    ) -> Result<(), RightsError> {
        if self.source_id != request.source_id {
            return Err(RightsError::SourceMismatch);
        }
        if self.payload_digest != request.payload_digest {
            return Err(RightsError::PayloadMismatch);
        }
        self.validate_at(admitted_at)?;
        if !self.permitted_operations.contains(&request.operation) {
            return Err(RightsError::OperationDenied);
        }
        Ok(())
    }

    /// Returns the covered source namespace.
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact covered payload digest.
    pub(crate) const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns payload retrieval time.
    pub(crate) const fn retrieved_at(&self) -> Timestamp {
        self.retrieved_at
    }

    /// Returns the exact retained terms URL.
    pub(crate) fn terms_url(&self) -> &str {
        &self.terms_url
    }

    /// Returns the exact terms-content digest.
    pub(crate) const fn terms_digest(&self) -> EvidenceDigest {
        self.terms_digest
    }

    /// Returns the authorization-evidence digest.
    pub(crate) const fn authorization_evidence(&self) -> EvidenceDigest {
        self.authorization_evidence
    }

    /// Returns exclusive authorization expiry, when present.
    pub(crate) const fn authorization_expires_at(&self) -> Option<Timestamp> {
        self.authorization_expires_at
    }

    /// Returns the compact operation set used for durable admission.
    pub(crate) fn operation_mask(&self) -> u8 {
        self.permitted_operations
            .iter()
            .fold(0, |mask, operation| mask | operation.mask())
    }

    pub(crate) const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    fn compute_fingerprint(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        update_length_prefixed(&mut hasher, self.source_id.as_str().as_bytes());
        update_digest(&mut hasher, self.payload_digest);
        hasher.update(self.retrieved_at.unix_nanos().to_be_bytes());
        update_length_prefixed(&mut hasher, self.terms_url.as_bytes());
        update_digest(&mut hasher, self.terms_digest);
        update_digest(&mut hasher, self.authorization_evidence);
        match self.authorization_expires_at {
            Some(expiry) => {
                hasher.update([1]);
                hasher.update(expiry.unix_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update([self.operation_mask()]);
        hasher.finalize().into()
    }
}

/// Stable identity of one rights-gated ingest attempt.
#[derive(Clone, Eq, PartialEq)]
pub struct IngestIdentity {
    source_id: SourceId,
    payload_digest: EvidenceDigest,
    operation: SourceOperation,
    idempotency_key: String,
}

impl fmt::Debug for IngestIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestIdentity")
            .field("source_id", &self.source_id)
            .field("payload_digest", &self.payload_digest)
            .field("operation", &self.operation)
            .field("idempotency_key", &"[REDACTED OPAQUE KEY]")
            .finish()
    }
}

impl IngestIdentity {
    /// Constructs a bounded idempotency identity for one source operation.
    pub fn try_new(
        source_id: SourceId,
        payload_digest: EvidenceDigest,
        operation: SourceOperation,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, RightsError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty()
            || idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES
            || idempotency_key.chars().any(char::is_control)
        {
            return Err(RightsError::InvalidIdempotencyKey);
        }
        Ok(Self {
            source_id,
            payload_digest,
            operation,
            idempotency_key,
        })
    }

    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the independently authorized operation.
    pub const fn operation(&self) -> SourceOperation {
        self.operation
    }

    /// Returns the bounded idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Rights construction or admission failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RightsError {
    /// Terms must use a bounded, explicit HTTPS reference.
    #[error("rights terms reference is invalid")]
    InvalidTermsReference,
    /// Authorization expiry must be strictly after retrieval.
    #[error("rights authorization expiry is invalid")]
    InvalidAuthorizationExpiry,
    /// At least one operation must be independently permitted.
    #[error("rights decision permits no operation")]
    NoPermittedOperation,
    /// Duplicate operation evidence is ambiguous.
    #[error("rights decision repeats an operation")]
    DuplicateOperation,
    /// The idempotency identity is empty, oversized, or contains control characters.
    #[error("ingest idempotency key is invalid")]
    InvalidIdempotencyKey,
    /// Rights evidence belongs to a different source.
    #[error("rights source does not match ingest source")]
    SourceMismatch,
    /// Rights evidence covers a different payload.
    #[error("rights payload does not match ingest payload")]
    PayloadMismatch,
    /// Trusted admission cannot precede retrieval of its covered payload.
    #[error("rights admission precedes payload retrieval evidence")]
    AdmissionBeforeRetrieval,
    /// Authorization was expired at request time.
    #[error("rights authorization is expired")]
    AuthorizationExpired,
    /// The requested operation was not granted.
    #[error("rights evidence does not permit the requested operation")]
    OperationDenied,
}

fn update_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(digest.bytes());
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}
