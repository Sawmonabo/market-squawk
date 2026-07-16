//! Source-metadata-backed coverage declarations for live observations.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    AssessmentValidity, LiveEventClass, LiveEvidenceBinding, MarketDepth, MetadataRevision,
    ProviderChannel, ProviderProduct,
};
use crate::{SourceId, Timestamp, VenueId};

/// Provider-declared delivery delay for a coverage scope.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum CoverageDelay {
    /// Provider metadata declares real-time delivery.
    RealTime,
    /// Provider metadata declares a positive delay in nanoseconds.
    Delayed(u64),
}

/// Venue consolidation represented by a provider product.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageConsolidation {
    /// The product covers the bound venue only.
    SingleVenue,
    /// The product explicitly covers only a subset of represented venues.
    Partial,
    /// The product is an explicitly consolidated view.
    Consolidated,
}

/// Compact result derived from an authoritative scoped coverage declaration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    /// The scoped declaration is sufficient for this observation.
    Sufficient,
    /// The scoped declaration is partial, delayed, expired, or otherwise insufficient.
    Insufficient,
    /// Coverage has not been established.
    Unknown,
}

/// Exact provider metadata scope underlying one coverage result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageScope {
    source_id: SourceId,
    venue_id: VenueId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    delay: CoverageDelay,
    consolidation: CoverageConsolidation,
    effective_from: Timestamp,
    effective_until: Option<Timestamp>,
    metadata_revision: MetadataRevision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageScopeWire {
    source_id: SourceId,
    venue_id: VenueId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    delay: CoverageDelay,
    consolidation: CoverageConsolidation,
    effective_from: Timestamp,
    effective_until: Option<Timestamp>,
    metadata_revision: MetadataRevision,
}

impl<'de> Deserialize<'de> for CoverageScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CoverageScopeWire::deserialize(deserializer)?;
        Self::new(
            wire.source_id,
            wire.venue_id,
            wire.provider_product,
            wire.provider_channel,
            wire.event_class,
            wire.depth,
            wire.delay,
            wire.consolidation,
            wire.effective_from,
            wire.effective_until,
            wire.metadata_revision,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl CoverageScope {
    /// Constructs a checked, effective source coverage scope.
    ///
    /// # Errors
    ///
    /// Rejects empty delay values, reversed effective intervals, and book scopes without depth.
    #[expect(
        clippy::too_many_arguments,
        reason = "coverage metadata scope is an atomic provider declaration"
    )]
    pub fn new(
        source_id: SourceId,
        venue_id: VenueId,
        provider_product: ProviderProduct,
        provider_channel: ProviderChannel,
        event_class: LiveEventClass,
        depth: Option<MarketDepth>,
        delay: CoverageDelay,
        consolidation: CoverageConsolidation,
        effective_from: Timestamp,
        effective_until: Option<Timestamp>,
        metadata_revision: MetadataRevision,
    ) -> Result<Self, CoverageError> {
        if matches!(delay, CoverageDelay::Delayed(0)) {
            return Err(CoverageError::ZeroDelay);
        }
        if effective_until.is_some_and(|until| until < effective_from) {
            return Err(CoverageError::InvalidEffectiveInterval);
        }
        if event_class.requires_book_state() && depth.is_none() {
            return Err(CoverageError::MissingBookDepth);
        }
        if !event_class.requires_book_state() && depth.is_some() {
            return Err(CoverageError::UnexpectedNonBookDepth);
        }
        Ok(Self {
            source_id,
            venue_id,
            provider_product,
            provider_channel,
            event_class,
            depth,
            delay,
            consolidation,
            effective_from,
            effective_until,
            metadata_revision,
        })
    }

    /// Returns the covered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the covered venue.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
    /// Returns the covered provider product.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    /// Returns the covered provider channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    /// Returns the covered event class.
    pub const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }
    /// Returns the covered depth when applicable.
    pub const fn depth(&self) -> Option<MarketDepth> {
        self.depth
    }
    /// Returns the declared delivery delay.
    pub const fn delay(&self) -> CoverageDelay {
        self.delay
    }
    /// Returns the consolidation class.
    pub const fn consolidation(&self) -> CoverageConsolidation {
        self.consolidation
    }
    /// Returns the first effective instant.
    pub const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }
    /// Returns the inclusive final effective instant, if bounded.
    pub const fn effective_until(&self) -> Option<Timestamp> {
        self.effective_until
    }
    /// Returns the source-metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns whether the scope is effective at an instant.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        at >= self.effective_from
            && match self.effective_until {
                Some(until) => at <= until,
                None => true,
            }
    }
}

/// Coverage result that has been relationally checked against a complete live binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCoverageRecord {
    binding: LiveEvidenceBinding,
    scope: CoverageScope,
    status: CoverageStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceCoverageRecordWire {
    binding: LiveEvidenceBinding,
    scope: CoverageScope,
    status: CoverageStatus,
}

impl<'de> Deserialize<'de> for SourceCoverageRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceCoverageRecordWire::deserialize(deserializer)?;
        Self::new(wire.binding, wire.scope, wire.status).map_err(serde::de::Error::custom)
    }
}

impl SourceCoverageRecord {
    /// Validates an independently sourced scope against every duplicated binding dimension.
    ///
    /// # Errors
    ///
    /// Rejects a venue, product, event, depth, or metadata-revision transplant. A sufficient
    /// result also rejects delayed or explicitly partial coverage.
    pub fn new(
        binding: LiveEvidenceBinding,
        scope: CoverageScope,
        status: CoverageStatus,
    ) -> Result<Self, CoverageError> {
        if scope.source_id != *binding.source_id() {
            return Err(CoverageError::BindingMismatch(CoverageDimension::Source));
        }
        if scope.venue_id != *binding.venue_id() {
            return Err(CoverageError::BindingMismatch(CoverageDimension::Venue));
        }
        if scope.provider_product != *binding.provider_product() {
            return Err(CoverageError::BindingMismatch(CoverageDimension::Product));
        }
        if scope.provider_channel != *binding.provider_channel() {
            return Err(CoverageError::BindingMismatch(CoverageDimension::Channel));
        }
        if scope.event_class != binding.event_class() {
            return Err(CoverageError::BindingMismatch(
                CoverageDimension::EventClass,
            ));
        }
        if scope.metadata_revision != *binding.metadata_revision() {
            return Err(CoverageError::BindingMismatch(
                CoverageDimension::MetadataRevision,
            ));
        }
        let bound_depth = binding.book_state().map(super::BookStateBinding::depth);
        if scope.depth != bound_depth {
            return Err(CoverageError::BindingMismatch(CoverageDimension::Depth));
        }
        if status == CoverageStatus::Sufficient
            && (!matches!(scope.delay, CoverageDelay::RealTime)
                || scope.consolidation == CoverageConsolidation::Partial)
        {
            return Err(CoverageError::ContradictorySufficientStatus);
        }
        Ok(Self {
            binding,
            scope,
            status,
        })
    }

    /// Returns the complete observation binding.
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }
    /// Returns the independently validated coverage scope.
    pub const fn scope(&self) -> &CoverageScope {
        &self.scope
    }
    /// Returns the compact coverage result.
    pub const fn status(&self) -> CoverageStatus {
        self.status
    }

    /// Returns the status at an instant, downgrading an out-of-effect declaration.
    pub fn status_at(&self, at: Timestamp) -> CoverageStatus {
        if self.scope.is_effective_at(at) {
            self.status
        } else {
            CoverageStatus::Insufficient
        }
    }
}

impl AssessmentValidity for SourceCoverageRecord {}

/// Coverage dimension implicated in a transplant attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageDimension {
    /// Source mismatch.
    Source,
    /// Venue mismatch.
    Venue,
    /// Provider product mismatch.
    Product,
    /// Provider channel mismatch.
    Channel,
    /// Event class mismatch.
    EventClass,
    /// Market depth mismatch.
    Depth,
    /// Source metadata revision mismatch.
    MetadataRevision,
}

/// Failure to construct or bind source coverage metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageError {
    /// A delayed declaration used a zero delay.
    ZeroDelay,
    /// Effective end precedes effective start.
    InvalidEffectiveInterval,
    /// A book scope omitted depth.
    MissingBookDepth,
    /// A non-book scope supplied market depth.
    UnexpectedNonBookDepth,
    /// A scope field does not match the live binding.
    BindingMismatch(CoverageDimension),
    /// A sufficient status contradicts delayed or explicitly partial metadata.
    ContradictorySufficientStatus,
}

impl fmt::Display for CoverageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDelay => formatter.write_str("delayed coverage must have a positive delay"),
            Self::InvalidEffectiveInterval => {
                formatter.write_str("coverage effective interval is reversed")
            }
            Self::MissingBookDepth => formatter.write_str("book-event coverage requires depth"),
            Self::UnexpectedNonBookDepth => {
                formatter.write_str("non-book coverage must not declare market depth")
            }
            Self::BindingMismatch(dimension) => write!(
                formatter,
                "coverage {dimension:?} does not match evidence binding"
            ),
            Self::ContradictorySufficientStatus => {
                formatter.write_str("sufficient coverage cannot be delayed or partial")
            }
        }
    }
}

impl std::error::Error for CoverageError {}
