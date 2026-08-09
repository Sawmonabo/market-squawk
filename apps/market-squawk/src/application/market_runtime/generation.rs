//! Deterministic identity for one exact account-backed runtime group configuration.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;
use sha2::{Digest as _, Sha256};

use crate::{
    provider_activation::PreparedMarketProviderConfiguration,
    provider_onboarding::ProviderActivationLease,
};

use super::configuration::PreparedMarketProviderConfigurationRequest;

const GROUP_GENERATION_DOMAIN: &[u8] = b"market-squawk/market-runtime-group-generation/v1\0";

/// SHA-256 identity of one exact account lease and its prepared child-source configurations.
///
/// This is deliberately not a [`market_squawk_domain::ConnectionGeneration`]. Each child source
/// mints and retains its real connection generation independently.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct MarketRuntimeGroupGeneration(EvidenceDigest);

impl MarketRuntimeGroupGeneration {
    pub(crate) const fn digest(self) -> EvidenceDigest {
        self.0
    }

    pub(super) fn try_from_prepared(
        request: PreparedMarketProviderConfigurationRequest,
        prepared: &PreparedMarketProviderConfiguration,
    ) -> Result<Self, ServiceError> {
        let mut hasher = Sha256::new();
        hasher.update(GROUP_GENERATION_DOMAIN);
        update_text(&mut hasher, request.surface().surface_id())?;
        hasher.update(request.onboarding_session_id().as_bytes());
        update_evidence(&mut hasher, request.expected_public_configuration_digest());
        let lease = match prepared {
            PreparedMarketProviderConfiguration::AlpacaBasic(value) => value.lease(),
            PreparedMarketProviderConfiguration::Tradier(value) => value.lease(),
            PreparedMarketProviderConfiguration::KrakenLevel3(value) => value.lease(),
        };
        update_lease(&mut hasher, lease);
        match prepared {
            PreparedMarketProviderConfiguration::AlpacaBasic(value) => {
                let optional = value.options_config().map(|config| config.metadata());
                update_optional_metadata(&mut hasher, value.iex_config().metadata(), optional)?;
            }
            PreparedMarketProviderConfiguration::Tradier(value) => {
                let optional = value
                    .derived_index_rest_config()
                    .map(|config| config.metadata());
                update_tradier_metadata(
                    &mut hasher,
                    value.consolidated_stream_config().metadata(),
                    value.consolidated_rest_config().metadata(),
                    optional,
                )?;
            }
            PreparedMarketProviderConfiguration::KrakenLevel3(value) => {
                update_metadata(&mut hasher, &[value.config().metadata()])?;
            }
        }
        let bytes: [u8; 32] = hasher.finalize().into();
        if bytes == [0; 32] {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)))
    }
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

fn update_tradier_metadata(
    hasher: &mut Sha256,
    stream: &SourceMetadata,
    snapshots: &SourceMetadata,
    derived_indexes: Option<&SourceMetadata>,
) -> Result<(), ServiceError> {
    match derived_indexes {
        Some(derived_indexes) => update_metadata(hasher, &[stream, snapshots, derived_indexes]),
        None => update_metadata(hasher, &[stream, snapshots]),
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
