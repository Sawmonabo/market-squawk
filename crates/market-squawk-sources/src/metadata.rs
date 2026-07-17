//! Immutable, evidence-bound source metadata.

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule,
    LiveEventClass, MarketDepth, MetadataRevision, ProviderChannel, ProviderProduct,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SequenceValidationRule,
    SnapshotApplicability, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::num::NonZeroU16;
use thiserror::Error;

use crate::bounded::BoundedVec;
use crate::{EndpointPolicy, ProviderBudgetPolicy};

const MAX_ASSET_CLASSES: usize = 32;
const MAX_VENUES: usize = 256;
const MAX_INSTRUMENTS: usize = 4_096;
const MAX_LIVE_COVERAGE_RULES: usize = 32;

/// Broad operational class of a configured source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceClass {
    /// Direct trading-venue interface.
    Exchange,
    /// User-authorized broker interface.
    Broker,
    /// Government or central-bank publication source.
    OfficialAgency,
    /// Regulatory filing repository.
    RegulatoryFiling,
    /// User-controlled local file or database export.
    LocalFile,
    /// User-controlled portfolio or transaction export.
    PortfolioExport,
    /// Licensed local dataset whose access rights are configured by the user.
    LicensedDataset,
    /// Public on-chain protocol data.
    OnChain,
}

/// Basis under which the configured source may lawfully be accessed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationMode {
    /// Published public interface used according to its declared terms.
    PublicInterface,
    /// Interface accessed with credentials belonging to and authorized by the user.
    UserAuthorized,
    /// Locally available dataset covered by a configured license or entitlement.
    Licensed,
    /// Local user-owned file requiring no remote provider access.
    UserOwnedLocal,
}

/// Whether this source may perform any network I/O.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkAccessPolicy {
    /// Network access is structurally denied.
    Denied,
    /// Only targets accepted by the explicit endpoint policy may be contacted.
    Allowlisted(EndpointPolicy),
}

impl NetworkAccessPolicy {
    /// Authorizes a final target or fails closed when networking is denied.
    ///
    /// # Errors
    ///
    /// Returns a typed endpoint denial for [`Self::Denied`] or delegates full structural
    /// authorization to the configured allowlist.
    pub fn authorize(&self, target: &str) -> Result<(), crate::NetworkPolicyError> {
        match self {
            Self::Denied => Err(crate::NetworkPolicyError::EndpointDenied {
                reason: crate::EndpointDenialReason::NotAllowlisted,
            }),
            Self::Allowlisted(policy) => policy.authorize(target),
        }
    }
}

/// Evidence and effective interval for one configured authorization basis.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationGrant {
    mode: AuthorizationMode,
    basis: AuthorizationBasis,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
}

impl AuthorizationGrant {
    /// Constructs an immutable, evidence-backed authorization declaration.
    pub const fn new(
        mode: AuthorizationMode,
        basis: AuthorizationBasis,
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
    ) -> Self {
        Self {
            mode,
            basis,
            evidence,
            effective,
        }
    }

    /// Returns the authorization mode.
    pub const fn mode(&self) -> AuthorizationMode {
        self.mode
    }

    /// Returns the audited authorization basis.
    pub const fn basis(&self) -> &AuthorizationBasis {
        &self.basis
    }

    /// Returns the exact evidence for the authorization declaration.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the authorization effective interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective
    }

    /// Returns whether the declaration is effective at `at`.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        interval_contains(self.effective, at)
    }

    /// Converts the half-open authorization end into an inclusive runtime deadline.
    pub fn inclusive_authorization_deadline(&self) -> Option<Timestamp> {
        self.effective
            .ends_at()
            .and_then(|end| end.checked_sub_nanos(1).ok())
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            mode: _,
            basis,
            evidence,
            effective: _,
        } = self;
        basis
            .as_source_identifier()
            .retained_bytes()
            .checked_add(evidence.dynamic_retained_bytes()?)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoverageTopologyKind {
    SingleVenue,
    PartialVenues,
    Consolidated,
    NotApplicable,
}

/// Venue scope without conflating venue consolidation with market depth or fair-value hierarchy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageTopology {
    kind: CoverageTopologyKind,
    venues: BoundedVec<VenueId, MAX_VENUES>,
}

impl CoverageTopology {
    /// Constructs coverage for one explicitly identified venue.
    pub fn single_venue(venue: VenueId) -> Self {
        Self {
            kind: CoverageTopologyKind::SingleVenue,
            venues: BoundedVec::singleton(venue),
        }
    }

    /// Constructs explicitly partial multi-venue coverage.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, or oversized venue set.
    pub fn partial_venues(venues: Vec<VenueId>) -> Result<Self, SourceMetadataError> {
        Self::try_from_parts(CoverageTopologyKind::PartialVenues, venues)
    }

    /// Constructs an explicitly consolidated multi-venue product scope.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, or oversized venue set.
    pub fn consolidated(venues: Vec<VenueId>) -> Result<Self, SourceMetadataError> {
        Self::try_from_parts(CoverageTopologyKind::Consolidated, venues)
    }

    /// Constructs a non-venue research scope.
    pub fn not_applicable() -> Self {
        Self {
            kind: CoverageTopologyKind::NotApplicable,
            venues: BoundedVec::empty(),
        }
    }

    fn try_from_parts(
        kind: CoverageTopologyKind,
        venues: Vec<VenueId>,
    ) -> Result<Self, SourceMetadataError> {
        if venues.is_empty() {
            return Err(SourceMetadataError::EmptyCollection { field: "venues" });
        }
        if contains_duplicates(&venues) {
            return Err(SourceMetadataError::DuplicateValue { field: "venues" });
        }
        let venues = BoundedVec::try_new(venues).map_err(|error| {
            SourceMetadataError::CollectionTooLarge {
                field: "venues",
                max: error.max,
            }
        })?;
        Ok(Self { kind, venues })
    }

    /// Returns all explicitly covered venues.
    pub fn venues(&self) -> &[VenueId] {
        self.venues.as_slice()
    }

    /// Returns whether the coverage is explicitly consolidated.
    pub const fn is_consolidated(&self) -> bool {
        matches!(self.kind, CoverageTopologyKind::Consolidated)
    }

    /// Returns whether venue coverage is explicitly partial.
    pub const fn is_partial(&self) -> bool {
        matches!(self.kind, CoverageTopologyKind::PartialVenues)
    }

    /// Returns whether the coverage applies to exactly one venue.
    pub const fn is_single_venue(&self) -> bool {
        matches!(self.kind, CoverageTopologyKind::SingleVenue)
    }

    /// Returns whether venue identity is inapplicable to the source.
    pub const fn is_not_applicable(&self) -> bool {
        matches!(self.kind, CoverageTopologyKind::NotApplicable)
    }

    /// Returns whether the exact venue is inside the declared topology.
    pub fn contains_venue(&self, venue: &VenueId) -> bool {
        self.venues.as_slice().contains(venue)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageTopologyWire {
    kind: CoverageTopologyKind,
    venues: BoundedVec<VenueId, MAX_VENUES>,
}

impl<'de> Deserialize<'de> for CoverageTopology {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CoverageTopologyWire::deserialize(deserializer)?;
        let venues = wire.venues.as_slice();
        let valid = match wire.kind {
            CoverageTopologyKind::SingleVenue => venues.len() == 1,
            CoverageTopologyKind::PartialVenues | CoverageTopologyKind::Consolidated => {
                !venues.is_empty() && !contains_duplicates(venues)
            }
            CoverageTopologyKind::NotApplicable => venues.is_empty(),
        };
        if !valid {
            return Err(serde::de::Error::custom(
                SourceMetadataError::InvalidCoverageTopology,
            ));
        }
        Ok(Self {
            kind: wire.kind,
            venues: wire.venues,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum InstrumentCoverageKind {
    AllDeclared,
    Partial,
    Enumerated,
}

/// Evidence strength for one instrument membership lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentCoverageMembership {
    /// The instrument appears in the exact bounded instrument set.
    Enumerated,
    /// Metadata evidence attests to the provider product's complete universe.
    EvidenceBackedUniverse,
    /// Coverage is explicitly partial and cannot establish membership.
    PartialUnproven,
    /// An enumerated set explicitly excludes the instrument.
    Outside,
}

include!("metadata/coverage.rs");
include!("metadata/protocol.rs");
include!("metadata/source.rs");
