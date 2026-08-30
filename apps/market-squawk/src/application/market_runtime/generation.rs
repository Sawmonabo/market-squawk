//! Versioned identity for one exact account-backed runtime-group incarnation.

use std::sync::Arc;

use market_squawk_domain::{
    ConnectionGeneration, DigestAlgorithm, EvidenceDigest, SourceIdentifier,
};
use market_squawk_live::ShardKey;
use market_squawk_services::ServiceError;
use market_squawk_sources::{InstrumentCoverageMembership, SourceMetadata};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    provider_activation::{PreparedMarketProviderConfiguration, PreparedSchwabMarketRuntimeStart},
    provider_onboarding::ProviderActivationLease,
};

use super::configuration::PreparedMarketProviderConfigurationRequest;

const GROUP_GENERATION_DOMAIN: &[u8] = b"market-squawk/market-runtime-group-generation/v2\0";
const LIVE_SURFACE_GENERATION_DOMAIN: &[u8] = b"market-squawk/live-market-surface-generation/v1\0";

/// Lifecycle identity for either one source connection or one independently supervised group.
///
/// Child connection generations remain visible in source snapshots. A group identity is stable
/// across one child's ordinary reconnect and changes only when the whole runtime surface is
/// replaced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketSourceRuntimeGeneration {
    Scalar(ConnectionGeneration),
    Group(MarketRuntimeGroupGeneration),
}

impl MarketSourceRuntimeGeneration {
    pub(crate) const fn connection_generation(self) -> Option<ConnectionGeneration> {
        match self {
            Self::Scalar(generation) => Some(generation),
            Self::Group(_) => None,
        }
    }

    pub(crate) const fn group_generation(self) -> Option<MarketRuntimeGroupGeneration> {
        match self {
            Self::Scalar(_) => None,
            Self::Group(generation) => Some(generation),
        }
    }

    pub(crate) const fn runtime_generation_digest(self) -> Option<EvidenceDigest> {
        match self {
            Self::Scalar(_) => None,
            Self::Group(generation) => Some(generation.digest()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketRuntimeTopologyMode {
    Scalar,
    Group(MarketRuntimeGroupGeneration),
}

/// Exact source and route topology for one scalar-readable market surface.
#[derive(Clone, Debug)]
pub(super) struct MarketRuntimeTopology {
    metadata: Arc<[SourceMetadata]>,
    routes: Box<[MarketRuntimeRouteTopology]>,
    mode: MarketRuntimeTopologyMode,
}

#[derive(Clone, Debug)]
pub(super) struct MarketRuntimeRouteTopology {
    route: ShardKey,
    source_indexes: Box<[usize]>,
}

impl MarketRuntimeRouteTopology {
    pub(super) const fn route(&self) -> &ShardKey {
        &self.route
    }

    pub(super) fn source_indexes(&self) -> &[usize] {
        &self.source_indexes
    }
}

impl MarketRuntimeTopology {
    pub(super) fn try_new(
        surface_id: &SourceIdentifier,
        metadata: Arc<[SourceMetadata]>,
        routes: Arc<[ShardKey]>,
    ) -> Result<Self, ServiceError> {
        if metadata.is_empty() || routes.is_empty() {
            return Err(ServiceError::Unavailable);
        }

        let mut source_indexes = Vec::new();
        source_indexes
            .try_reserve_exact(metadata.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        source_indexes.extend(0..metadata.len());
        source_indexes.sort_by(|left, right| {
            let left = &metadata[*left];
            let right = &metadata[*right];
            left.source_id().cmp(right.source_id()).then_with(|| {
                left.revision()
                    .as_source_identifier()
                    .cmp(right.revision().as_source_identifier())
            })
        });
        if source_indexes
            .windows(2)
            .any(|pair| metadata[pair[0]].source_id() == metadata[pair[1]].source_id())
            || source_indexes
                .iter()
                .any(|index| metadata[*index].coverage().live().is_none())
        {
            return Err(ServiceError::Unavailable);
        }

        let mut canonical_routes = Vec::new();
        canonical_routes
            .try_reserve_exact(routes.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        canonical_routes.extend(routes.iter().cloned());
        canonical_routes.sort_by(|left, right| {
            left.venue()
                .cmp(right.venue())
                .then_with(|| left.instrument().cmp(&right.instrument()))
        });
        if canonical_routes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ServiceError::Unavailable);
        }

        let mut used_sources = Vec::new();
        used_sources
            .try_reserve_exact(metadata.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        used_sources.resize(metadata.len(), false);
        let mut route_topology = Vec::new();
        route_topology
            .try_reserve_exact(canonical_routes.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for route in canonical_routes {
            let mut expected_sources = Vec::new();
            expected_sources
                .try_reserve_exact(metadata.len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            for source_index in &source_indexes {
                if metadata_covers_route(&metadata[*source_index], &route) {
                    expected_sources.push(*source_index);
                    used_sources[*source_index] = true;
                }
            }
            if expected_sources.is_empty() {
                return Err(ServiceError::Unavailable);
            }
            route_topology.push(MarketRuntimeRouteTopology {
                route,
                source_indexes: expected_sources.into_boxed_slice(),
            });
        }
        if used_sources.iter().any(|used| !used) {
            return Err(ServiceError::Unavailable);
        }

        let routes = route_topology.into_boxed_slice();
        let mode = if metadata.len() == 1 {
            MarketRuntimeTopologyMode::Scalar
        } else {
            MarketRuntimeTopologyMode::Group(MarketRuntimeGroupGeneration::try_from_live_topology(
                surface_id,
                &metadata,
                &routes,
                uuid::Uuid::new_v4(),
            )?)
        };
        Ok(Self {
            metadata,
            routes,
            mode,
        })
    }

    pub(super) const fn metadata(&self) -> &Arc<[SourceMetadata]> {
        &self.metadata
    }

    pub(super) fn routes(&self) -> &[MarketRuntimeRouteTopology] {
        &self.routes
    }

    pub(super) fn generation(
        &self,
        scalar_generation: Option<ConnectionGeneration>,
    ) -> Result<MarketSourceRuntimeGeneration, ServiceError> {
        match (self.mode, scalar_generation) {
            (MarketRuntimeTopologyMode::Scalar, Some(generation)) => {
                Ok(MarketSourceRuntimeGeneration::Scalar(generation))
            }
            (MarketRuntimeTopologyMode::Group(generation), None) => {
                Ok(MarketSourceRuntimeGeneration::Group(generation))
            }
            (MarketRuntimeTopologyMode::Scalar, None)
            | (MarketRuntimeTopologyMode::Group(_), Some(_)) => Err(ServiceError::Unavailable),
        }
    }

    pub(super) const fn group_generation(&self) -> Option<MarketRuntimeGroupGeneration> {
        match self.mode {
            MarketRuntimeTopologyMode::Scalar => None,
            MarketRuntimeTopologyMode::Group(generation) => Some(generation),
        }
    }
}

/// SHA-256 identity of one fresh runtime incarnation, exact account lease, and prepared children.
///
/// This is deliberately not a [`market_squawk_domain::ConnectionGeneration`]. Each child source
/// mints and retains its real connection generation independently.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct MarketRuntimeGroupGeneration(EvidenceDigest);

impl MarketRuntimeGroupGeneration {
    pub(crate) const fn digest(self) -> EvidenceDigest {
        self.0
    }

    /// Reconstructs an expected group identity for compare-and-set only.
    pub(crate) fn try_from_expected_digest(digest: EvidenceDigest) -> Result<Self, ServiceError> {
        if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
            Err(ServiceError::InvalidRequest)
        } else {
            Ok(Self(digest))
        }
    }

    pub(super) fn try_from_prepared(
        request: PreparedMarketProviderConfigurationRequest,
        prepared: &PreparedMarketProviderConfiguration,
        runtime_incarnation: Uuid,
    ) -> Result<Self, ServiceError> {
        let lease = match prepared {
            PreparedMarketProviderConfiguration::AlpacaBasic(value) => value.lease(),
            PreparedMarketProviderConfiguration::KrakenLevel3(value) => value.lease(),
        };
        let metadata = match prepared {
            PreparedMarketProviderConfiguration::AlpacaBasic(value) => {
                let optional = value.options_config().map(|config| config.metadata());
                return Self::try_from_account_parts(
                    request,
                    lease,
                    &[value.iex_config().metadata()],
                    optional,
                    runtime_incarnation,
                );
            }
            PreparedMarketProviderConfiguration::KrakenLevel3(value) => [value.config().metadata()],
        };
        Self::try_from_account_parts(request, lease, &metadata, None, runtime_incarnation)
    }

    pub(super) fn try_from_schwab(
        request: PreparedMarketProviderConfigurationRequest,
        prepared: &PreparedSchwabMarketRuntimeStart,
        runtime_incarnation: Uuid,
    ) -> Result<Self, ServiceError> {
        Self::try_from_account_parts(
            request,
            prepared.activation_lease(),
            &[prepared.metadata()],
            None,
            runtime_incarnation,
        )
    }

    fn try_from_account_parts(
        request: PreparedMarketProviderConfigurationRequest,
        lease: &ProviderActivationLease,
        required_metadata: &[&SourceMetadata],
        optional_metadata: Option<&SourceMetadata>,
        runtime_incarnation: Uuid,
    ) -> Result<Self, ServiceError> {
        if runtime_incarnation.is_nil() {
            return Err(ServiceError::Unavailable);
        }
        let mut hasher = Sha256::new();
        hasher.update(GROUP_GENERATION_DOMAIN);
        hasher.update(runtime_incarnation.as_bytes());
        update_text(&mut hasher, request.surface().surface_id())?;
        hasher.update(request.onboarding_session_id().as_bytes());
        update_evidence(&mut hasher, request.expected_public_configuration_digest());
        update_evidence(
            &mut hasher,
            request.expected_runtime_verification_receipt_digest(),
        );
        hasher.update(request.expected_credential_generation().get().to_be_bytes());
        update_lease(&mut hasher, lease);
        match (required_metadata, optional_metadata) {
            ([required], optional) => update_optional_metadata(&mut hasher, required, optional)?,
            (_, None) => update_metadata(&mut hasher, required_metadata)?,
            (_, Some(_)) => return Err(ServiceError::InvalidRequest),
        }
        let bytes: [u8; 32] = hasher.finalize().into();
        if bytes == [0; 32] {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)))
    }

    fn try_from_live_topology(
        surface_id: &SourceIdentifier,
        metadata: &[SourceMetadata],
        routes: &[MarketRuntimeRouteTopology],
        runtime_incarnation: Uuid,
    ) -> Result<Self, ServiceError> {
        if runtime_incarnation.is_nil() || metadata.len() < 2 || routes.is_empty() {
            return Err(ServiceError::Unavailable);
        }
        let mut hasher = Sha256::new();
        hasher.update(LIVE_SURFACE_GENERATION_DOMAIN);
        hasher.update(runtime_incarnation.as_bytes());
        update_text(&mut hasher, surface_id.as_str())?;

        let metadata_count =
            u64::try_from(metadata.len()).map_err(|_| ServiceError::InvalidRequest)?;
        hasher.update(metadata_count.to_be_bytes());
        let mut source_indexes = Vec::new();
        source_indexes
            .try_reserve_exact(metadata.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        source_indexes.extend(0..metadata.len());
        source_indexes.sort_by(|left, right| {
            metadata[*left]
                .source_id()
                .cmp(metadata[*right].source_id())
        });
        for source_index in source_indexes {
            let source = &metadata[source_index];
            let live = source.coverage().live().ok_or(ServiceError::Unavailable)?;
            update_text(&mut hasher, source.source_id().as_str())?;
            update_text(
                &mut hasher,
                source.revision().as_source_identifier().as_str(),
            )?;
            update_evidence(
                &mut hasher,
                source
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest(),
            );
            update_text(&mut hasher, source.provider().as_str())?;
            update_text(
                &mut hasher,
                live.provider_product().as_source_identifier().as_str(),
            )?;
            update_text(
                &mut hasher,
                live.provider_channel().as_source_identifier().as_str(),
            )?;
        }

        let route_count = u64::try_from(routes.len()).map_err(|_| ServiceError::InvalidRequest)?;
        hasher.update(route_count.to_be_bytes());
        for route in routes {
            update_text(&mut hasher, route.route().venue().as_str())?;
            hasher.update(route.route().instrument().as_uuid().as_bytes());
            let source_count = u64::try_from(route.source_indexes().len())
                .map_err(|_| ServiceError::InvalidRequest)?;
            hasher.update(source_count.to_be_bytes());
            for source_index in route.source_indexes() {
                let source = metadata
                    .get(*source_index)
                    .ok_or(ServiceError::Unavailable)?;
                update_text(&mut hasher, source.source_id().as_str())?;
            }
        }
        let bytes: [u8; 32] = hasher.finalize().into();
        if bytes == [0; 32] {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)))
    }
}

fn metadata_covers_route(metadata: &SourceMetadata, route: &ShardKey) -> bool {
    let coverage = metadata.coverage();
    coverage.topology().venues().contains(route.venue())
        && matches!(
            coverage.instruments().membership(route.instrument()),
            InstrumentCoverageMembership::Enumerated
                | InstrumentCoverageMembership::EvidenceBackedUniverse
        )
}

fn update_optional_metadata(
    hasher: &mut Sha256,
    required: &SourceMetadata,
    optional: Option<&SourceMetadata>,
) -> Result<(), ServiceError> {
    match optional {
        Some(optional) => update_metadata(hasher, &[required, optional]),
        None => update_metadata(hasher, &[required]),
    }
}

fn update_metadata(hasher: &mut Sha256, metadata: &[&SourceMetadata]) -> Result<(), ServiceError> {
    let metadata_count = u64::try_from(metadata.len()).map_err(|_| ServiceError::InvalidRequest)?;
    hasher.update(metadata_count.to_be_bytes());
    for source in metadata {
        update_text(hasher, source.source_id().as_str())?;
        update_text(hasher, source.revision().as_source_identifier().as_str())?;
        update_text(hasher, source.provider().as_str())?;
        hasher.update([quality_tag(source.quality_ceiling())]);
    }
    Ok(())
}

fn update_lease(hasher: &mut Sha256, lease: &ProviderActivationLease) {
    update_evidence(hasher, lease.capability_digest());
    update_evidence(hasher, lease.rights_decision_digest());
    update_evidence(hasher, lease.runtime_evidence_digest());
    update_optional_evidence(hasher, lease.account_digest());
    update_optional_evidence(hasher, lease.verification_evidence_digest());
}

fn update_evidence(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([digest_algorithm_tag(digest.algorithm())]);
    hasher.update(digest.bytes());
}

fn update_optional_evidence(hasher: &mut Sha256, digest: Option<EvidenceDigest>) {
    match digest {
        Some(value) => {
            hasher.update([1]);
            update_evidence(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn update_text(hasher: &mut Sha256, value: &str) -> Result<(), ServiceError> {
    let length = u64::try_from(value.len()).map_err(|_| ServiceError::InvalidRequest)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

const fn digest_algorithm_tag(value: DigestAlgorithm) -> u8 {
    match value {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

const fn quality_tag(value: market_squawk_domain::DataQuality) -> u8 {
    use market_squawk_domain::DataQuality;
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}
