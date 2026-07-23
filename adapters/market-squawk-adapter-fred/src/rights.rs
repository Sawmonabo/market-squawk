use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

/// A checked SHA-256 digest.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Constructs a digest from its exact bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses 64 lowercase hexadecimal characters.
    pub fn from_lower_hex(value: &str) -> Result<Self, FredRightsError> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(FredRightsError::InvalidDigest);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
        }
        Ok(Self(bytes))
    }

    /// Returns the exact digest bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    fn is_zero(self) -> bool {
        self.0 == [0; 32]
    }

    fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

fn hex_nibble(value: u8) -> Result<u8, FredRightsError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FredRightsError::InvalidDigest),
    }
}

/// A separately authorized operation over FRED or ALFRED material.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FredOperation {
    /// Transient retrieval without durable retention.
    RetrieveEphemeral,
    /// Durable local persistence.
    Persist,
    /// Local cache retention.
    Cache,
    /// Archival retention.
    Archive,
    /// Redistribution to another party.
    Redistribute,
    /// Display to a user.
    Display,
    /// Model training or fitting.
    Train,
}

/// The fail-closed outcome of a rights assessment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FredRightsDisposition {
    /// Every requested operation is affirmatively authorized.
    Permitted,
    /// No current exact-series grant establishes the requested durable use.
    BlockedUnknownRights,
    /// A grant exists but does not include every requested operation.
    BlockedOperationScope,
    /// The terms evidence has crossed its local revalidation deadline.
    BlockedStaleTerms,
}

const TERMS_BUNDLE_DOMAIN: &[u8] = b"market-squawk:fred-terms-bundle:v1\0";
/// Maximum exact bytes accepted for one reviewed FRED terms document.
pub const MAX_FRED_TERMS_DOCUMENT_BYTES: usize = 2 * 1024 * 1024;

/// Required role of one exact document in the FRED terms bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum FredTermsDocumentRole {
    /// FRED API terms governing API access and incorporated terms.
    ApiTerms,
    /// FRED services and legal terms incorporated by the API terms.
    FredServicesLegalTerms,
    /// Federal Reserve Bank of St. Louis online privacy notice.
    PrivacyPolicy,
}

impl FredTermsDocumentRole {
    const ALL: [Self; 3] = [
        Self::ApiTerms,
        Self::FredServicesLegalTerms,
        Self::PrivacyPolicy,
    ];

    const fn tag(self) -> u8 {
        match self {
            Self::ApiTerms => 1,
            Self::FredServicesLegalTerms => 2,
            Self::PrivacyPolicy => 3,
        }
    }

    const fn canonical_url(self) -> &'static str {
        match self {
            Self::ApiTerms => "https://fred.stlouisfed.org/docs/api/terms_of_use.html",
            Self::FredServicesLegalTerms => "https://fred.stlouisfed.org/legal/",
            Self::PrivacyPolicy => {
                "https://www.stlouisfed.org/about-us/privacy-policy/online-notice"
            }
        }
    }
}

/// Exact caller-supplied bytes for one reviewed FRED terms document.
#[derive(Clone, Copy)]
pub struct FredTermsDocumentBytes<'a> {
    role: FredTermsDocumentRole,
    bytes: &'a [u8],
}

impl<'a> FredTermsDocumentBytes<'a> {
    /// Binds non-empty, bounded exact document bytes to their required role.
    ///
    /// # Errors
    ///
    /// Returns [`FredRightsError::InvalidTermsDocumentBytes`] for empty input or input above
    /// [`MAX_FRED_TERMS_DOCUMENT_BYTES`].
    pub fn try_new(role: FredTermsDocumentRole, bytes: &'a [u8]) -> Result<Self, FredRightsError> {
        if bytes.is_empty() || bytes.len() > MAX_FRED_TERMS_DOCUMENT_BYTES {
            return Err(FredRightsError::InvalidTermsDocumentBytes);
        }
        Ok(Self { role, bytes })
    }

    /// Returns the document role.
    pub const fn role(&self) -> FredTermsDocumentRole {
        self.role
    }

    /// Returns the exact supplied bytes.
    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl std::fmt::Debug for FredTermsDocumentBytes<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredTermsDocumentBytes")
            .field("role", &self.role)
            .field("byte_length", &self.bytes.len())
            .finish()
    }
}

/// Verified identity of one exact document in the FRED terms bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredTermsDocumentEvidence {
    role: FredTermsDocumentRole,
    url: String,
    digest: Sha256Digest,
    byte_length: usize,
}

impl FredTermsDocumentEvidence {
    /// Returns the required document role.
    pub const fn role(&self) -> FredTermsDocumentRole {
        self.role
    }

    /// Returns the exact canonical document URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the verified exact document digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the verified exact document byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }
}

/// Exact reviewed terms-document bundle and its local revalidation window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredTermsEvidence {
    documents: BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>,
    bundle_digest: Sha256Digest,
    assessed_at: Timestamp,
    review_required_by: Timestamp,
}

impl FredTermsEvidence {
    /// Returns the deterministic digest of all exact reviewed terms documents.
    pub const fn bundle_digest(&self) -> Sha256Digest {
        self.bundle_digest
    }

    /// Returns the verified documents in deterministic role order.
    pub fn documents(&self) -> impl ExactSizeIterator<Item = &FredTermsDocumentEvidence> {
        self.documents.values()
    }

    /// Returns evidence for one required document role.
    pub fn document(&self, role: FredTermsDocumentRole) -> Option<&FredTermsDocumentEvidence> {
        self.documents.get(&role)
    }

    /// Returns when the local assessment was made.
    pub const fn assessed_at(&self) -> Timestamp {
        self.assessed_at
    }

    /// Returns the local fail-closed revalidation deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.review_required_by
    }
}

/// A release evidence artifact describing the reviewed FRED rights state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredRightsArtifact {
    terms: FredTermsEvidence,
    operations: Vec<FredOperation>,
    disposition: FredRightsDisposition,
}

impl FredRightsArtifact {
    /// Parses the release artifact and verifies every exact reviewed terms document.
    ///
    /// # Errors
    ///
    /// Returns a typed rights error when the artifact, required document set, exact bytes,
    /// deterministic bundle digest, source evidence, or fail-closed decision is invalid.
    pub fn parse(
        bytes: &[u8],
        terms_bytes: &[FredTermsDocumentBytes<'_>],
    ) -> Result<Self, FredRightsError> {
        if bytes.len() > 256 * 1024 {
            return Err(FredRightsError::ArtifactTooLarge);
        }
        let wire: ArtifactWire = serde_json::from_slice(bytes)?;
        if wire.schema_version != 2 || wire.series_scope != "unresolved" {
            return Err(FredRightsError::InvalidArtifact("schema or series scope"));
        }
        let documents = verify_terms_documents(&wire.terms_documents, terms_bytes)?;
        let terms = FredTermsEvidence {
            bundle_digest: Sha256Digest::from_lower_hex(&wire.terms_bundle_digest)?,
            documents,
            assessed_at: Timestamp::from_unix_nanos(wire.assessed_at_unix_nanos),
            review_required_by: Timestamp::from_unix_nanos(wire.review_required_by_unix_nanos),
        };
        validate_terms(&terms)?;
        if terms.bundle_digest != terms_bundle_digest(&terms.documents)? {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
        validate_operations(&wire.operations)?;
        if wire.operations.contains(&FredOperation::RetrieveEphemeral)
            || wire.disposition != FredRightsDisposition::BlockedUnknownRights
            || wire.confirmed_facts.is_empty()
            || wire.engineering_inferences.is_empty()
            || wire.sources.is_empty()
        {
            return Err(FredRightsError::InvalidArtifact("rights decision"));
        }
        for source in &wire.sources {
            validate_https_url(&source.url)?;
        }
        let all_documents_confirmed = terms.documents().all(|document| {
            wire.sources.iter().any(|source| {
                source.url == document.url
                    && source
                        .sha256
                        .as_deref()
                        .and_then(|value| Sha256Digest::from_lower_hex(value).ok())
                        == Some(document.digest)
                    && source.byte_length == Some(document.byte_length)
                    && source.evidence_class == "confirmed"
                    && !source.accessed_on.is_empty()
            })
        });
        if !all_documents_confirmed {
            return Err(FredRightsError::InvalidArtifact("terms source evidence"));
        }
        Ok(Self {
            terms,
            operations: wire.operations,
            disposition: wire.disposition,
        })
    }

    /// Returns the reviewed terms evidence.
    pub const fn terms_evidence(&self) -> &FredTermsEvidence {
        &self.terms
    }

    /// Returns the assessment timestamp.
    pub const fn assessed_at(&self) -> Timestamp {
        self.terms.assessed_at
    }

    /// Returns the local fail-closed review deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.terms.review_required_by
    }

    /// Returns the operations assessed by the release artifact.
    pub fn operations(&self) -> &[FredOperation] {
        &self.operations
    }

    /// Returns the release disposition.
    pub const fn disposition(&self) -> FredRightsDisposition {
        self.disposition
    }
}

/// Independently verified exact owner-authorization evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredOwnerAuthorizationEvidence {
    url: String,
    digest: Sha256Digest,
    byte_length: usize,
}

impl FredOwnerAuthorizationEvidence {
    /// Verifies declared evidence identity against the exact supplied authorization bytes.
    pub fn try_new(
        url: String,
        digest: Sha256Digest,
        byte_length: usize,
        authorization_bytes: &[u8],
    ) -> Result<Self, FredRightsError> {
        validate_https_url(&url)?;
        if digest.is_zero()
            || byte_length == 0
            || byte_length != authorization_bytes.len()
            || digest != Sha256Digest::of(authorization_bytes)
        {
            return Err(FredRightsError::OwnerEvidenceMismatch);
        }
        Ok(Self {
            url,
            digest,
            byte_length,
        })
    }

    /// Returns the exact evidence URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the verified authorization digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the verified authorization byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }
}

/// An exact-series authorization supplied by the series owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredSeriesRightsGrant {
    series: SourceIdentifier,
    owner: SourceIdentifier,
    authorization: FredOwnerAuthorizationEvidence,
    terms_bundle_digest: Sha256Digest,
    operations: BTreeSet<FredOperation>,
    effective_at: Timestamp,
    expires_at: Timestamp,
}

impl FredSeriesRightsGrant {
    /// Builds a bounded exact-series grant; wildcard grants are not supported.
    #[allow(
        clippy::too_many_arguments,
        reason = "all evidence fields bind rights authority"
    )]
    pub fn try_new(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        authorization: FredOwnerAuthorizationEvidence,
        terms_bundle_digest: Sha256Digest,
        operations: Vec<FredOperation>,
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_operations(&operations)?;
        if terms_bundle_digest.is_zero()
            || effective_at >= expires_at
            || series.as_str().contains('*')
        {
            return Err(FredRightsError::InvalidGrant);
        }
        Ok(Self {
            series,
            owner,
            authorization,
            terms_bundle_digest,
            operations: operations.into_iter().collect(),
            effective_at,
            expires_at,
        })
    }

    /// Returns the exact covered series.
    pub const fn series(&self) -> &SourceIdentifier {
        &self.series
    }

    /// Returns the series-owner identity bound to the grant.
    pub const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    /// Returns the exact owner-authorization evidence URL.
    pub fn authorization_url(&self) -> &str {
        self.authorization.url()
    }

    /// Returns the digest of the owner-authorization evidence bytes.
    pub const fn authorization_digest(&self) -> Sha256Digest {
        self.authorization.digest()
    }

    /// Returns the verified owner-authorization evidence byte length.
    pub const fn authorization_byte_length(&self) -> usize {
        self.authorization.byte_length()
    }

    /// Returns the complete FRED terms-bundle digest to which this grant is bound.
    pub const fn terms_bundle_digest(&self) -> Sha256Digest {
        self.terms_bundle_digest
    }

    /// Returns the operations affirmatively granted.
    pub fn operations(&self) -> impl Iterator<Item = FredOperation> + '_ {
        self.operations.iter().copied()
    }

    /// Returns the inclusive grant start instant.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the exclusive grant expiry instant.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Runtime fail-closed FRED rights authority.
#[derive(Clone, Debug)]
pub struct FredRightsPolicy {
    terms: FredTermsEvidence,
    grants: BTreeMap<SourceIdentifier, FredSeriesRightsGrant>,
}

impl FredRightsPolicy {
    /// Builds a policy with at most one exact grant per series.
    pub fn try_new(
        terms: FredTermsEvidence,
        grants: Vec<FredSeriesRightsGrant>,
    ) -> Result<Self, FredRightsError> {
        validate_terms(&terms)?;
        let mut by_series = BTreeMap::new();
        for grant in grants {
            if grant.terms_bundle_digest != terms.bundle_digest
                || by_series.insert(grant.series.clone(), grant).is_some()
            {
                return Err(FredRightsError::InvalidGrant);
            }
        }
        Ok(Self {
            terms,
            grants: by_series,
        })
    }

    /// Assesses exact operations for one exact series at a caller-supplied time.
    pub fn assess(
        &self,
        series: &SourceIdentifier,
        operations: &[FredOperation],
        at: Timestamp,
    ) -> Result<FredRightsDecision, FredRightsError> {
        validate_operations(operations)?;
        let disposition = if at < self.terms.assessed_at || at >= self.terms.review_required_by {
            FredRightsDisposition::BlockedStaleTerms
        } else if operations == [FredOperation::RetrieveEphemeral] {
            FredRightsDisposition::Permitted
        } else if operations.contains(&FredOperation::RetrieveEphemeral) {
            FredRightsDisposition::BlockedOperationScope
        } else if let Some(grant) = self.grants.get(series) {
            if at < grant.effective_at || at >= grant.expires_at {
                FredRightsDisposition::BlockedUnknownRights
            } else if operations
                .iter()
                .all(|operation| grant.operations.contains(operation))
            {
                FredRightsDisposition::Permitted
            } else {
                FredRightsDisposition::BlockedOperationScope
            }
        } else {
            FredRightsDisposition::BlockedUnknownRights
        };
        Ok(FredRightsDecision {
            terms_bundle_digest: self.terms.bundle_digest,
            operations: operations.to_vec(),
            review_required_by: self.terms.review_required_by,
            disposition,
        })
    }
}

/// A complete result of one runtime rights assessment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredRightsDecision {
    terms_bundle_digest: Sha256Digest,
    operations: Vec<FredOperation>,
    review_required_by: Timestamp,
    disposition: FredRightsDisposition,
}

impl FredRightsDecision {
    /// Returns the exact complete terms-bundle digest assessed.
    pub const fn terms_bundle_digest(&self) -> Sha256Digest {
        self.terms_bundle_digest
    }

    /// Returns the exact requested operations.
    pub fn operations(&self) -> &[FredOperation] {
        &self.operations
    }

    /// Returns the local terms revalidation deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.review_required_by
    }

    /// Returns the fail-closed disposition.
    pub const fn disposition(&self) -> FredRightsDisposition {
        self.disposition
    }
}

/// A FRED rights evidence or policy validation failure.
#[derive(Debug, Error)]
pub enum FredRightsError {
    /// The artifact exceeds its fixed parser budget.
    #[error("FRED rights artifact exceeds the byte budget")]
    ArtifactTooLarge,
    /// The artifact is not valid JSON in the exact supported schema.
    #[error("invalid FRED rights artifact JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A digest is not non-zero lowercase SHA-256 hexadecimal.
    #[error("invalid SHA-256 digest")]
    InvalidDigest,
    /// A structured artifact invariant failed.
    #[error("invalid FRED rights artifact: {0}")]
    InvalidArtifact(&'static str),
    /// Terms evidence is internally inconsistent or stale at construction.
    #[error("invalid FRED terms evidence")]
    InvalidTerms,
    /// One caller-supplied terms document is empty or exceeds its fixed byte ceiling.
    #[error("invalid FRED terms document bytes")]
    InvalidTermsDocumentBytes,
    /// Supplied terms bytes do not match the artifact's declared exact evidence.
    #[error("FRED terms bytes do not match the release artifact")]
    TermsEvidenceMismatch,
    /// Supplied owner-authorization bytes do not match their declared exact evidence.
    #[error("FRED owner authorization bytes do not match declared evidence")]
    OwnerEvidenceMismatch,
    /// An exact-series grant is invalid or duplicates another grant.
    #[error("invalid FRED series rights grant")]
    InvalidGrant,
    /// The operation set is empty or contains duplicates.
    #[error("invalid FRED operation set")]
    InvalidOperations,
    /// Evidence URL is not a valid HTTPS URL.
    #[error("rights evidence URL must be valid HTTPS")]
    InvalidUrl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWire {
    schema_version: u16,
    series_scope: String,
    terms_bundle_digest: String,
    terms_documents: Vec<TermsDocumentWire>,
    assessed_at_unix_nanos: i64,
    review_required_by_unix_nanos: i64,
    operations: Vec<FredOperation>,
    disposition: FredRightsDisposition,
    confirmed_facts: Vec<String>,
    engineering_inferences: Vec<String>,
    sources: Vec<SourceWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TermsDocumentWire {
    role: FredTermsDocumentRole,
    url: String,
    sha256: String,
    byte_length: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceWire {
    url: String,
    accessed_on: String,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    byte_length: Option<usize>,
    evidence_class: String,
}

fn verify_terms_documents(
    declared: &[TermsDocumentWire],
    supplied: &[FredTermsDocumentBytes<'_>],
) -> Result<BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>, FredRightsError> {
    if declared.len() != FredTermsDocumentRole::ALL.len()
        || supplied.len() != FredTermsDocumentRole::ALL.len()
    {
        return Err(FredRightsError::TermsEvidenceMismatch);
    }
    let mut supplied_by_role = BTreeMap::new();
    for document in supplied {
        if document.bytes.is_empty()
            || supplied_by_role
                .insert(document.role, document.bytes)
                .is_some()
        {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
    }
    let mut documents = BTreeMap::new();
    for document in declared {
        if document.url != document.role.canonical_url() || document.byte_length == 0 {
            return Err(FredRightsError::InvalidArtifact("terms document identity"));
        }
        let digest = Sha256Digest::from_lower_hex(&document.sha256)?;
        let exact_bytes = supplied_by_role
            .get(&document.role)
            .ok_or(FredRightsError::TermsEvidenceMismatch)?;
        if digest.is_zero()
            || document.byte_length != exact_bytes.len()
            || digest != Sha256Digest::of(exact_bytes)
        {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
        let evidence = FredTermsDocumentEvidence {
            role: document.role,
            url: document.url.clone(),
            digest,
            byte_length: document.byte_length,
        };
        if documents.insert(document.role, evidence).is_some() {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
    }
    if FredTermsDocumentRole::ALL
        .iter()
        .any(|role| !documents.contains_key(role) || !supplied_by_role.contains_key(role))
    {
        return Err(FredRightsError::TermsEvidenceMismatch);
    }
    Ok(documents)
}

fn terms_bundle_digest(
    documents: &BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>,
) -> Result<Sha256Digest, FredRightsError> {
    if documents.len() != FredTermsDocumentRole::ALL.len() {
        return Err(FredRightsError::InvalidTerms);
    }
    let document_count =
        u32::try_from(documents.len()).map_err(|_| FredRightsError::InvalidTerms)?;
    let mut hasher = Sha256::new();
    hasher.update(TERMS_BUNDLE_DOMAIN);
    hasher.update(document_count.to_be_bytes());
    for role in FredTermsDocumentRole::ALL {
        let document = documents.get(&role).ok_or(FredRightsError::InvalidTerms)?;
        if document.role != role || document.url != role.canonical_url() {
            return Err(FredRightsError::InvalidTerms);
        }
        let url_length =
            u32::try_from(document.url.len()).map_err(|_| FredRightsError::InvalidTerms)?;
        let byte_length =
            u64::try_from(document.byte_length).map_err(|_| FredRightsError::InvalidTerms)?;
        hasher.update([role.tag()]);
        hasher.update(url_length.to_be_bytes());
        hasher.update(document.url.as_bytes());
        hasher.update(byte_length.to_be_bytes());
        hasher.update(document.digest.bytes());
    }
    Ok(Sha256Digest(hasher.finalize().into()))
}

fn validate_terms(terms: &FredTermsEvidence) -> Result<(), FredRightsError> {
    if terms.bundle_digest.is_zero()
        || terms.assessed_at >= terms.review_required_by
        || terms.bundle_digest != terms_bundle_digest(&terms.documents)?
    {
        return Err(FredRightsError::InvalidTerms);
    }
    Ok(())
}

fn validate_operations(operations: &[FredOperation]) -> Result<(), FredRightsError> {
    if operations.is_empty()
        || operations.iter().copied().collect::<BTreeSet<_>>().len() != operations.len()
    {
        return Err(FredRightsError::InvalidOperations);
    }
    Ok(())
}

fn validate_https_url(value: &str) -> Result<(), FredRightsError> {
    let url = Url::parse(value).map_err(|_| FredRightsError::InvalidUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
    {
        return Err(FredRightsError::InvalidUrl);
    }
    Ok(())
}
