//! Public bounded XBRL extraction result types.

use std::sync::Arc;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision, SourceId,
    SourceIdentifier, Timestamp, XbrlFactEvidence, XbrlOccurrenceRelationships, XbrlQualifiedName,
    XbrlTaxonomySet, XbrlText,
};
use market_squawk_sources::ProviderCaptureMaterial;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::SecXbrlError;
use crate::{RawEvidenceStore, RetrievedSecBytes};

const TAXONOMY_REGISTRY_RULESET: &str = "sec-xbrl-taxonomy-registry-v1";
const MAX_TAXONOMY_ARTIFACTS: usize = 64;
const MAX_TAXONOMY_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TAXONOMY_SET_BYTES: u64 = 64 * 1024 * 1024;

/// Code-owned validator for a bounded set of exact, captured official taxonomy artifacts.
#[derive(Clone, Copy, Debug, Default)]
pub struct SecXbrlTaxonomyRegistry {
    _private: (),
}

impl SecXbrlTaxonomyRegistry {
    /// Returns the sole code-owned taxonomy-set validation ruleset.
    pub const fn code_owned() -> Self {
        Self { _private: () }
    }

    /// Reopens exact captured artifacts and produces a non-cloneable pending admission.
    pub(crate) fn try_admit_captured(
        &self,
        raw_store: Arc<RawEvidenceStore>,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        mut artifacts: Vec<RetrievedSecBytes>,
        cancellation: &CancellationToken,
    ) -> Result<SecPendingValidatedXbrlTaxonomySet, SecXbrlError> {
        if artifacts.is_empty() || artifacts.len() > MAX_TAXONOMY_ARTIFACTS {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        artifacts.sort_unstable_by(|left, right| {
            left.locator()
                .cmp(&right.locator())
                .then_with(|| left.evidence().bytes().cmp(&right.evidence().bytes()))
        });
        if artifacts
            .windows(2)
            .any(|pair| pair[0].locator() == pair[1].locator())
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let descriptors = validate_taxonomy_artifacts(
            &raw_store,
            source_id,
            metadata_revision,
            &artifacts,
            cancellation,
        )?;
        let mut artifact_set = Sha256::new();
        hash_taxonomy_field(&mut artifact_set, b"sec-xbrl-exact-artifact-set-v1");
        artifact_set.update(
            u64::try_from(descriptors.len())
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                .to_be_bytes(),
        );
        for artifact in &descriptors {
            hash_taxonomy_field(&mut artifact_set, artifact.locator.as_str().as_bytes());
            artifact_set.update(artifact.evidence.bytes());
            artifact_set.update(artifact.size_bytes.to_be_bytes());
        }
        let artifact_set =
            EvidenceDigest::new(DigestAlgorithm::Sha256, artifact_set.finalize().into());
        let fingerprint = taxonomy_registry_fingerprint(artifact_set);
        let version = SourceIdentifier::try_from(format!(
            "sec-xbrl-taxonomy-set.{}",
            digest_prefix(fingerprint, 16)
        ))?;
        Ok(SecPendingValidatedXbrlTaxonomySet {
            validated: SecValidatedXbrlTaxonomySet {
                version,
                artifact_set,
                fingerprint,
                artifacts: descriptors.into_boxed_slice(),
            },
            raw_store,
            source_id: source_id.clone(),
            metadata_revision: metadata_revision.clone(),
            artifacts,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecXbrlTaxonomyArtifact {
    locator: SourceIdentifier,
    evidence: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
}

/// Opaque exact taxonomy-set identity minted only from captured official artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecValidatedXbrlTaxonomySet {
    version: SourceIdentifier,
    artifact_set: EvidenceDigest,
    fingerprint: EvidenceDigest,
    artifacts: Box<[SecXbrlTaxonomyArtifact]>,
}

/// Non-cloneable captured taxonomy evidence awaiting the common physical-seal transition.
pub(crate) struct SecPendingValidatedXbrlTaxonomySet {
    validated: SecValidatedXbrlTaxonomySet,
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    artifacts: Vec<RetrievedSecBytes>,
}

impl SecPendingValidatedXbrlTaxonomySet {
    pub(crate) const fn validated(&self) -> &SecValidatedXbrlTaxonomySet {
        &self.validated
    }

    pub(crate) fn revalidate(&self, cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
        let descriptors = validate_taxonomy_artifacts(
            &self.raw_store,
            &self.source_id,
            &self.metadata_revision,
            &self.artifacts,
            cancellation,
        )?;
        if descriptors.as_slice() != self.validated.artifacts.as_ref() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        Ok(())
    }

    pub(crate) fn into_sealing_parts(
        self,
        cancellation: &CancellationToken,
    ) -> Result<(Self, Vec<ProviderCaptureMaterial>), SecXbrlError> {
        self.revalidate(cancellation)?;
        let mut materials = Vec::new();
        materials
            .try_reserve_exact(self.artifacts.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        for artifact in &self.artifacts {
            if cancellation.is_cancelled() {
                return Err(SecXbrlError::Cancelled);
            }
            materials.push(
                artifact
                    .capture_material()
                    .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
                    .ok_or(SecXbrlError::InvalidTaxonomySet)?,
            );
        }
        Ok((self, materials))
    }
}

impl SecValidatedXbrlTaxonomySet {
    /// Returns the exact set-derived version identity.
    pub const fn version(&self) -> &SourceIdentifier {
        &self.version
    }

    /// Returns the canonical digest of every exact captured artifact in the accepted set.
    pub const fn artifact_set(&self) -> EvidenceDigest {
        self.artifact_set
    }

    /// Returns the code-owned ruleset fingerprint of the accepted artifact set.
    pub const fn fingerprint(&self) -> EvidenceDigest {
        self.fingerprint
    }

    pub(crate) fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        self.artifacts
            .len()
            .checked_mul(std::mem::size_of::<SecXbrlTaxonomyArtifact>())?
            .checked_add(self.version.retained_bytes())?
            .checked_add(self.artifacts.iter().try_fold(0usize, |total, artifact| {
                total.checked_add(artifact.locator.retained_bytes())
            })?)
    }

    pub(crate) fn domain_set(&self) -> XbrlTaxonomySet {
        XbrlTaxonomySet::declared(self.artifact_set, self.version.clone())
    }
}

fn taxonomy_registry_fingerprint(artifact_set: EvidenceDigest) -> EvidenceDigest {
    let mut fingerprint = Sha256::new();
    hash_taxonomy_field(&mut fingerprint, TAXONOMY_REGISTRY_RULESET.as_bytes());
    fingerprint.update(artifact_set.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, fingerprint.finalize().into())
}

fn validate_taxonomy_artifacts(
    raw_store: &RawEvidenceStore,
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    artifacts: &[RetrievedSecBytes],
    cancellation: &CancellationToken,
) -> Result<Vec<SecXbrlTaxonomyArtifact>, SecXbrlError> {
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(artifacts.len())
        .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
    let mut total_bytes = 0_u64;
    for artifact in artifacts {
        if cancellation.is_cancelled() {
            return Err(SecXbrlError::Cancelled);
        }
        let locator = artifact.locator().ok_or(SecXbrlError::InvalidTaxonomySet)?;
        validate_taxonomy_locator(locator)?;
        let receipt = artifact
            .capture_receipt()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if receipt.source_id() != source_id || receipt.metadata_revision() != metadata_revision {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        artifact
            .capture_material()
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let size_bytes =
            u64::try_from(artifact.bytes().len()).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        if size_bytes == 0 || size_bytes > MAX_TAXONOMY_ARTIFACT_BYTES {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        total_bytes = total_bytes
            .checked_add(size_bytes)
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if total_bytes > MAX_TAXONOMY_SET_BYTES {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let reopened = raw_store
            .read_verified_bounded_cancellable(&artifact.evidence(), size_bytes, cancellation)
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        if reopened.as_slice() != artifact.bytes().as_ref() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        descriptors.push(SecXbrlTaxonomyArtifact {
            locator: SourceIdentifier::try_from(locator)?,
            evidence: artifact.evidence(),
            size_bytes,
            first_observed_at: artifact.received_at(),
            retrieval_revision: artifact
                .retrieval_revision()
                .ok_or(SecXbrlError::InvalidTaxonomySet)?,
        });
    }
    Ok(descriptors)
}

fn validate_taxonomy_locator(locator: &str) -> Result<(), SecXbrlError> {
    let parsed = Url::parse(locator).map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
    let official_host = matches!(
        parsed.host_str(),
        Some("www.sec.gov" | "xbrl.sec.gov" | "xbrl.fasb.org" | "www.xbrl.org" | "www.w3.org")
    );
    if parsed.scheme() != "https"
        || !official_host
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !(parsed.path().ends_with(".xsd") || parsed.path().ends_with(".xml"))
    {
        return Err(SecXbrlError::InvalidTaxonomySet);
    }
    Ok(())
}

fn digest_prefix(digest: EvidenceDigest, bytes: usize) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.saturating_mul(2));
    for byte in digest.bytes().into_iter().take(bytes) {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn hash_taxonomy_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .map_or(u64::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(value);
}

/// Immutable document-level evidence shared by every parsed occurrence.
#[derive(Clone, Debug)]
pub struct XbrlDocumentContext {
    pub(super) accession: SourceIdentifier,
    pub(super) expected_cik: Option<SourceIdentifier>,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
}

impl XbrlDocumentContext {
    /// Binds parser output to accession, taxonomy set, exact payload, and evaluation time.
    pub const fn new(
        accession: SourceIdentifier,
        taxonomy_set: XbrlTaxonomySet,
        source_payload: ExactPayloadEvidence,
        evaluated_at: Timestamp,
    ) -> Self {
        Self {
            accession,
            expected_cik: None,
            taxonomy_set,
            source_payload,
            evaluated_at,
        }
    }

    /// Binds parser output to a freshly revalidated captured taxonomy admission.
    pub(crate) fn from_validated_taxonomy(
        accession: SourceIdentifier,
        expected_cik: SourceIdentifier,
        taxonomy_set: &SecPendingValidatedXbrlTaxonomySet,
        source_payload: ExactPayloadEvidence,
        evaluated_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecXbrlError> {
        taxonomy_set.revalidate(cancellation)?;
        Ok(Self {
            accession,
            expected_cik: Some(expected_cik),
            taxonomy_set: taxonomy_set.validated().domain_set(),
            source_payload,
            evaluated_at,
        })
    }
}

/// One exact normalized numeric fact plus its full occurrence evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XbrlNumericFact {
    pub(super) concept: SourceIdentifier,
    pub(super) unit: SourceIdentifier,
    pub(super) value: Decimal,
    pub(super) evidence: XbrlFactEvidence,
}

impl XbrlNumericFact {
    /// Returns the qualified concept identity.
    pub const fn concept(&self) -> &SourceIdentifier {
        &self.concept
    }
    /// Returns the normalized unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
    /// Returns the exact normalized decimal.
    pub const fn value(&self) -> Decimal {
        self.value
    }
    /// Returns occurrence-level audit evidence.
    pub const fn evidence(&self) -> &XbrlFactEvidence {
        &self.evidence
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        SourceIdentifier,
        Decimal,
        XbrlFactEvidence,
    ) {
        (self.concept, self.unit, self.value, self.evidence)
    }
}

/// Nil or nonnumeric occurrence retained without fabricating a Decimal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XbrlNonnumericOccurrence {
    pub(super) occurrence_id: SourceIdentifier,
    pub(super) accession: SourceIdentifier,
    pub(super) concept: XbrlQualifiedName,
    pub(super) context_id: SourceIdentifier,
    pub(super) lexical_value: XbrlText,
    pub(super) nil: bool,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) occurrence_relationships: XbrlOccurrenceRelationships,
}

impl XbrlNonnumericOccurrence {
    /// Returns the source or deterministic occurrence identity.
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }

    /// Returns the exact filing accession carrying this occurrence.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the exact XBRL context identity referenced by this occurrence.
    pub const fn context_id(&self) -> &SourceIdentifier {
        &self.context_id
    }

    /// Returns the source lexical and resolved concept QName.
    pub const fn concept(&self) -> &XbrlQualifiedName {
        &self.concept
    }

    /// Returns the exact bounded text, empty only for an explicit nil occurrence.
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    /// Returns whether the occurrence carried explicit nil semantics.
    pub const fn is_nil(&self) -> bool {
        self.nil
    }

    /// Returns nesting, continuation, and explanatory relationship evidence.
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }

    /// Returns the exact filing payload evidence carrying this occurrence.
    pub const fn source_payload(&self) -> &ExactPayloadEvidence {
        &self.source_payload
    }
}

/// Parsed XBRL output preserving numeric and nonnumeric occurrence families separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedXbrlDocument {
    pub(super) accession: SourceIdentifier,
    pub(super) expected_cik: Option<SourceIdentifier>,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
    pub(super) retained_output_upper_bound: usize,
    pub(super) numeric_facts: Vec<XbrlNumericFact>,
    pub(super) nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
}

impl ParsedXbrlDocument {
    pub(crate) fn matches_document_context(
        &self,
        accession: &SourceIdentifier,
        expected_cik: &SourceIdentifier,
        taxonomy_set: &SecValidatedXbrlTaxonomySet,
        source_payload: EvidenceDigest,
    ) -> bool {
        &self.accession == accession
            && self.expected_cik.as_ref() == Some(expected_cik)
            && self.taxonomy_set == taxonomy_set.domain_set()
            && self.source_payload.content_digest() == source_payload
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    pub(crate) const fn retained_output_upper_bound(&self) -> usize {
        self.retained_output_upper_bound
    }

    pub(crate) fn into_families(
        self,
    ) -> (Vec<XbrlNumericFact>, Vec<XbrlNonnumericOccurrence>, usize) {
        (
            self.numeric_facts,
            self.nonnumeric_occurrences,
            self.retained_output_upper_bound,
        )
    }

    /// Returns normalized numeric facts.
    pub fn numeric_facts(&self) -> &[XbrlNumericFact] {
        &self.numeric_facts
    }
    /// Returns nil and nonnumeric occurrences.
    pub fn nonnumeric_occurrences(&self) -> &[XbrlNonnumericOccurrence] {
        &self.nonnumeric_occurrences
    }
}
