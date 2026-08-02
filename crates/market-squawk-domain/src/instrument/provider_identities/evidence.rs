//! Content-addressed provider assertion evidence and retrieval metadata.

use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize};

use super::{BoundedVec, ProviderIdentityCollection};
use crate::{EvidenceDigest, InstrumentError, SourceIdentifier};

/// Retrieval metadata containing a provider object locator and caller-supplied version pin.
///
/// Both fields are bounded identifiers. This type does not prove that a provider's version is
/// immutable; the separately required digest in [`ProviderIdentityEvidence`] is the authoritative
/// evidence identity. The version-pinned locator is retained only for retrieval and explanation.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentityLocator {
    reference: SourceIdentifier,
    version: SourceIdentifier,
}

impl ProviderIdentityLocator {
    /// Constructs a bounded provider locator with caller/provider-supplied version-pin metadata.
    pub const fn new(reference: SourceIdentifier, version: SourceIdentifier) -> Self {
        Self { reference, version }
    }

    /// Returns the provider object or record locator.
    pub const fn reference(&self) -> &SourceIdentifier {
        &self.reference
    }

    /// Returns the caller/provider-supplied object or record version pin.
    pub const fn version(&self) -> &SourceIdentifier {
        &self.version
    }
}

/// Immutable, algorithm-qualified content evidence for a provider identity assertion.
///
/// A content digest is mandatory. Version-pinned locators are optional retrieval metadata and can
/// never replace the digest, so a bare URL or mutable object name is not representable as evidence.
/// Locators are retained in deterministic sorted/deduplicated order and do not participate in
/// assertion content equivalence.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ProviderIdentityEvidence {
    content_digest: EvidenceDigest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    locators: Vec<ProviderIdentityLocator>,
}

impl ProviderIdentityEvidence {
    /// Maximum unique retrieval locators retained for one content digest.
    pub const MAX_LOCATORS: usize = 64;

    /// Constructs provider evidence from an algorithm-qualified content digest.
    pub const fn from_content_digest(content_digest: EvidenceDigest) -> Self {
        Self {
            content_digest,
            locators: Vec::new(),
        }
    }

    /// Constructs provider evidence with one version-pinned retrieval locator.
    pub fn with_version_pinned_locator(
        content_digest: EvidenceDigest,
        version_pinned_locator: ProviderIdentityLocator,
    ) -> Self {
        Self {
            content_digest,
            locators: vec![version_pinned_locator],
        }
    }

    /// Constructs provider evidence with bounded, canonical retrieval metadata.
    ///
    /// Duplicate locators are ignored before enforcing the unique-locator limit. Input is consumed
    /// incrementally, so an unbounded iterator cannot cause unbounded collection growth.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::ProviderIdentityCapacityExceeded`] when more than
    /// [`Self::MAX_LOCATORS`] unique locators are supplied.
    pub fn try_with_locators<I>(
        content_digest: EvidenceDigest,
        locators: I,
    ) -> Result<Self, InstrumentError>
    where
        I: IntoIterator<Item = ProviderIdentityLocator>,
    {
        let mut evidence = Self::from_content_digest(content_digest);
        for locator in locators {
            evidence.insert_locator(locator)?;
        }
        Ok(evidence)
    }

    /// Returns the mandatory algorithm-qualified content digest.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns canonical retrieval locators in reference/version order.
    pub fn locators(&self) -> &[ProviderIdentityLocator] {
        &self.locators
    }

    pub(super) fn content_equivalent(&self, other: &Self) -> bool {
        self.content_digest == other.content_digest
    }

    pub(super) fn merge_locator_metadata(&mut self, other: &Self) -> Result<(), InstrumentError> {
        for locator in &other.locators {
            self.insert_locator(locator.clone())?;
        }
        Ok(())
    }

    fn insert_locator(&mut self, locator: ProviderIdentityLocator) -> Result<(), InstrumentError> {
        if self.locators.contains(&locator) {
            return Ok(());
        }
        if self.locators.len() == Self::MAX_LOCATORS {
            return Err(InstrumentError::ProviderIdentityCapacityExceeded {
                collection: ProviderIdentityCollection::Locators,
                max: Self::MAX_LOCATORS,
            });
        }
        self.locators.push(locator);
        self.locators.sort_by(compare_locators);
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderIdentityEvidenceWire {
    content_digest: EvidenceDigest,
    #[serde(default)]
    locators: BoundedVec<ProviderIdentityLocator, { ProviderIdentityEvidence::MAX_LOCATORS }>,
}

impl<'de> Deserialize<'de> for ProviderIdentityEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderIdentityEvidenceWire::deserialize(deserializer)?;
        Self::try_with_locators(wire.content_digest, wire.locators.into_inner())
            .map_err(serde::de::Error::custom)
    }
}

pub(super) fn compare_locator_slices(
    left: &[ProviderIdentityLocator],
    right: &[ProviderIdentityLocator],
) -> Ordering {
    for (left_locator, right_locator) in left.iter().zip(right) {
        let ordering = compare_locators(left_locator, right_locator);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn compare_locators(left: &ProviderIdentityLocator, right: &ProviderIdentityLocator) -> Ordering {
    left.reference()
        .cmp(right.reference())
        .then_with(|| left.version().cmp(right.version()))
}
