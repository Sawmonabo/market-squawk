//! Exact account-scoped metadata for the two admitted Instruments detail lookups.

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, CoverageTopology, EndpointPolicy, FreshnessPolicy,
    HistoricalCapability, InstrumentCoverage, NetworkAccessPolicy, SourceCapabilities, SourceClass,
    SourceCoverage, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use sha2::{Digest as _, Sha256};

use super::super::SchwabMarketDataAccountActivation;

pub(super) const PROFILE: &str = "schwab.trader-api-market-data.instruments";
pub(super) const SOURCE: &str = "schwab-trader-api-instruments";
pub(super) const DATASET: &str = "schwab.instruments.detail";

pub(super) fn metadata(
    activation: &SchwabMarketDataAccountActivation,
) -> Result<SourceMetadata, ServiceError> {
    let lease = activation.lease();
    let effective = EffectiveInterval::new(
        lease.authority_effective_at(),
        lease.verification_expires_at(),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    // This digest identifies the code-owned acquisition declaration. Actual provider assertions
    // are supplied later by the original sealed response, never by this metadata digest.
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/schwab-instruments-detail-source/v1\0");
    hash.update(include_bytes!("metadata.rs"));
    hash.update(lease.capability_digest().bytes());
    hash.update(lease.public_configuration_digest().bytes());
    hash.update(lease.rights_decision_digest().bytes());
    hash.update(activation.account_binding().verification_evidence().bytes());
    let declaration = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ));
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(activation.account_binding().subject().clone()),
        ExactPayloadEvidence::from_content_digest(
            activation.account_binding().verification_evidence(),
        ),
        effective,
    );
    let source = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(SOURCE).map_err(|_| ServiceError::Internal)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(
                SourceIdentifier::try_from(format!(
                    "schwab-instruments-detail-{}",
                    lower_hex(&declaration.content_digest().bytes())
                ))
                .map_err(|_| ServiceError::Internal)?,
            ),
            declaration.clone(),
        ),
        SourceClass::Broker,
        SourceIdentifier::try_from("schwab-trader-api").map_err(|_| ServiceError::Internal)?,
        authorization,
        SourceCoverage::try_instrument(
            declaration,
            effective,
            vec![AssetClass::Fund],
            CoverageTopology::single_venue(
                VenueId::try_from("ARCX").map_err(|_| ServiceError::Internal)?,
            ),
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::NotApplicable,
            DeliveryEvidence::AuthorizedBroker,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Allowlisted(
            EndpointPolicy::try_new([
                "https://api.schwabapi.com/marketdata/v1/instruments/78462F103",
                "https://api.schwabapi.com/marketdata/v1/instruments/922908769",
            ])
            .map_err(|_| ServiceError::InvalidResult)?,
        ),
        FreshnessPolicy::try_new(
            86_400_000_000_000,
            86_400_000_000_000,
            86_400_000_000_000,
            86_400_000_000_000,
            1_000_000_000,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        Some(
            activation
                .provider_rate_declaration()
                .map_err(|_| ServiceError::Unauthorized)?
                .policy()
                .clone(),
        ),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| ServiceError::InvalidResult)?;
    if !activation.account_binding().validates_metadata(&source)
        || !source.is_effective_at(lease.authority_effective_at())
    {
        return Err(ServiceError::Unauthorized);
    }
    Ok(source)
}

pub(super) fn timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?,
    ))
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(64);
    for byte in bytes {
        text.push(char::from(HEX[usize::from(byte >> 4)]));
        text.push(char::from(HEX[usize::from(byte & 15)]));
    }
    text
}
