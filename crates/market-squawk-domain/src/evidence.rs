//! Exact payload evidence for source-authored identity assertions.

use serde::{Deserialize, Serialize};

use crate::{EvidenceDigest, MetadataRevision, SourceIdentifier};

/// A bounded source-object locator paired with explicit source-supplied version-pin metadata.
///
/// This remains retrieval metadata. The content digest required by [`ExactPayloadEvidence`] is the
/// authoritative payload identity, so a locator can never qualify evidence by itself. This type
/// preserves the supplied version pin but does not independently prove its immutability.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionPinnedSourceLocator {
    reference: SourceIdentifier,
    version: SourceIdentifier,
}

impl VersionPinnedSourceLocator {
    /// Constructs a source locator with a separately retained version identity.
    pub const fn new(reference: SourceIdentifier, version: SourceIdentifier) -> Self {
        Self { reference, version }
    }

    /// Returns the source object locator used for retrieval or explanation.
    pub const fn reference(&self) -> &SourceIdentifier {
        &self.reference
    }

    /// Returns the bounded caller/source-supplied object or record version pin.
    pub const fn version(&self) -> &SourceIdentifier {
        &self.version
    }

    /// Returns checked heap bytes retained by this locator's bounded identities.
    ///
    /// The exhaustive field binding intentionally makes every future retained field an explicit
    /// allocation-accounting decision.
    pub fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self { reference, version } = self;
        reference
            .retained_bytes()
            .checked_add(version.retained_bytes())
    }
}

/// Mandatory algorithm-qualified content evidence for one exact source payload.
///
/// A content digest is always present. A version-pinned locator is optional retrieval metadata and
/// cannot replace the digest; a moving URL or bare source reference is therefore structurally
/// insufficient to construct or deserialize this type.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExactPayloadEvidence {
    content_digest: EvidenceDigest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_pinned_locator: Option<VersionPinnedSourceLocator>,
}

impl ExactPayloadEvidence {
    /// Constructs exact payload evidence from an algorithm-qualified content digest.
    pub const fn from_content_digest(content_digest: EvidenceDigest) -> Self {
        Self {
            content_digest,
            version_pinned_locator: None,
        }
    }

    /// Constructs exact evidence with optional retrieval metadata carrying its own version.
    pub const fn with_version_pinned_locator(
        content_digest: EvidenceDigest,
        version_pinned_locator: VersionPinnedSourceLocator,
    ) -> Self {
        Self {
            content_digest,
            version_pinned_locator: Some(version_pinned_locator),
        }
    }

    /// Returns the mandatory algorithm-qualified content digest.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns optional version-pinned retrieval metadata.
    pub const fn version_pinned_locator(&self) -> Option<&VersionPinnedSourceLocator> {
        self.version_pinned_locator.as_ref()
    }

    /// Returns checked heap bytes retained by optional retrieval metadata.
    ///
    /// The digest is fixed-width inline storage. The exhaustive field binding intentionally makes
    /// every future retained field an explicit allocation-accounting decision.
    pub fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            content_digest: _,
            version_pinned_locator,
        } = self;
        version_pinned_locator
            .as_ref()
            .map_or(Some(0), VersionPinnedSourceLocator::dynamic_retained_bytes)
    }
}

/// An authoritative metadata revision atomically bound to its exact source payload evidence.
///
/// Consumers cannot retain the revision claim without also retaining the content digest of the
/// payload that established it.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionBoundPayloadEvidence {
    metadata_revision: MetadataRevision,
    payload_evidence: ExactPayloadEvidence,
}

impl RevisionBoundPayloadEvidence {
    /// Binds a typed source-metadata revision to exact payload evidence.
    pub const fn new(
        metadata_revision: MetadataRevision,
        payload_evidence: ExactPayloadEvidence,
    ) -> Self {
        Self {
            metadata_revision,
            payload_evidence,
        }
    }

    /// Returns the source metadata revision established by this payload.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact content evidence establishing the revision.
    pub const fn payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.payload_evidence
    }
}
