use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use serde::Deserialize;
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
}

fn hex_nibble(value: u8) -> Result<u8, FredRightsError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FredRightsError::InvalidDigest),
    }
}

/// A separately authorized operation over FRED or ALFRED material.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

/// Exact reviewed terms bytes and their local revalidation window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredTermsEvidence {
    url: String,
    digest: Sha256Digest,
    byte_length: usize,
    assessed_at: Timestamp,
    review_required_by: Timestamp,
}

impl FredTermsEvidence {
    /// Returns the digest of the exact terms bytes reviewed.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the reviewed terms URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the reviewed byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
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
    /// Parses and validates the machine-readable release evidence artifact.
    pub fn parse(bytes: &[u8]) -> Result<Self, FredRightsError> {
        if bytes.len() > 256 * 1024 {
            return Err(FredRightsError::ArtifactTooLarge);
        }
        let wire: ArtifactWire = serde_json::from_slice(bytes)?;
        if wire.schema_version != 1 || wire.series_scope != "unresolved" {
            return Err(FredRightsError::InvalidArtifact("schema or series scope"));
        }
        validate_https_url(&wire.terms_url)?;
        let terms = FredTermsEvidence {
            url: wire.terms_url,
            digest: Sha256Digest::from_lower_hex(&wire.terms_digest)?,
            byte_length: wire.terms_bytes,
            assessed_at: Timestamp::from_unix_nanos(wire.assessed_at_unix_nanos),
            review_required_by: Timestamp::from_unix_nanos(wire.review_required_by_unix_nanos),
        };
        validate_terms(&terms)?;
        validate_operations(&wire.operations)?;
        if wire.operations.contains(&FredOperation::RetrieveEphemeral)
            || wire.disposition != FredRightsDisposition::BlockedUnknownRights
            || wire.confirmed_facts.is_empty()
            || wire.engineering_inferences.is_empty()
            || wire.sources.is_empty()
        {
            return Err(FredRightsError::InvalidArtifact("rights decision"));
        }
        let matching_source = wire.sources.iter().any(|source| {
            source.url == terms.url
                && source.sha256.as_deref() == Some(wire.terms_digest.as_str())
                && source.byte_length == Some(terms.byte_length)
                && source.evidence_class == "confirmed"
                && !source.accessed_on.is_empty()
        });
        if !matching_source {
            return Err(FredRightsError::InvalidArtifact("terms source evidence"));
        }
        for source in &wire.sources {
            validate_https_url(&source.url)?;
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

/// An exact-series authorization supplied by the series owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredSeriesRightsGrant {
    series: SourceIdentifier,
    owner: SourceIdentifier,
    authorization_url: String,
    authorization_digest: Sha256Digest,
    terms_digest: Sha256Digest,
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
        authorization_url: String,
        authorization_digest: Sha256Digest,
        terms_digest: Sha256Digest,
        operations: Vec<FredOperation>,
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_https_url(&authorization_url)?;
        validate_operations(&operations)?;
        if authorization_digest.is_zero()
            || terms_digest.is_zero()
            || effective_at >= expires_at
            || series.as_str().contains('*')
        {
            return Err(FredRightsError::InvalidGrant);
        }
        Ok(Self {
            series,
            owner,
            authorization_url,
            authorization_digest,
            terms_digest,
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
        &self.authorization_url
    }

    /// Returns the digest of the owner-authorization evidence bytes.
    pub const fn authorization_digest(&self) -> Sha256Digest {
        self.authorization_digest
    }

    /// Returns the FRED terms digest to which this grant is bound.
    pub const fn terms_digest(&self) -> Sha256Digest {
        self.terms_digest
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
            if grant.terms_digest != terms.digest
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
            terms_digest: self.terms.digest,
            operations: operations.to_vec(),
            review_required_by: self.terms.review_required_by,
            disposition,
        })
    }
}

/// A complete result of one runtime rights assessment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredRightsDecision {
    terms_digest: Sha256Digest,
    operations: Vec<FredOperation>,
    review_required_by: Timestamp,
    disposition: FredRightsDisposition,
}

impl FredRightsDecision {
    /// Returns the exact terms digest assessed.
    pub const fn terms_digest(&self) -> Sha256Digest {
        self.terms_digest
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
    terms_url: String,
    terms_digest: String,
    terms_bytes: usize,
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
struct SourceWire {
    url: String,
    accessed_on: String,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    byte_length: Option<usize>,
    evidence_class: String,
}

fn validate_terms(terms: &FredTermsEvidence) -> Result<(), FredRightsError> {
    validate_https_url(&terms.url)?;
    if terms.digest.is_zero()
        || terms.byte_length == 0
        || terms.assessed_at >= terms.review_required_by
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
