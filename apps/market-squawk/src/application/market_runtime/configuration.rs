//! Typed, secret-free configuration resolution for account-backed market runtime groups.

use std::time::Instant;

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_platform::SecretGeneration;
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::provider_activation::PreparedMarketProviderConfiguration;

/// Alpaca Basic account lifecycle surface.
pub(crate) const ALPACA_BASIC_SURFACE_ID: &str = "alpaca.basic-market-data";
/// Authenticated Kraken order-level lifecycle surface.
pub(crate) const KRAKEN_LEVEL3_SURFACE_ID: &str = "kraken.spot-authenticated-level3-market-data";

/// Closed V1 account-backed market surface set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum AccountMarketSurface {
    AlpacaBasic,
    KrakenLevel3,
}

impl AccountMarketSurface {
    pub(crate) const fn surface_id(self) -> &'static str {
        match self {
            Self::AlpacaBasic => ALPACA_BASIC_SURFACE_ID,
            Self::KrakenLevel3 => KRAKEN_LEVEL3_SURFACE_ID,
        }
    }

    pub(crate) fn parse(surface_id: &str) -> Option<Self> {
        match surface_id {
            ALPACA_BASIC_SURFACE_ID => Some(Self::AlpacaBasic),
            KRAKEN_LEVEL3_SURFACE_ID => Some(Self::KrakenLevel3),
            _ => None,
        }
    }
}

/// Exact non-secret authority required to resolve one prepared account-market configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedMarketProviderConfigurationRequest {
    surface: AccountMarketSurface,
    onboarding_session_id: Uuid,
    expected_public_configuration_digest: EvidenceDigest,
    expected_runtime_verification_receipt_digest: EvidenceDigest,
    expected_credential_generation: SecretGeneration,
}

impl PreparedMarketProviderConfigurationRequest {
    /// Constructs an exact request without accepting an unqualified or empty digest.
    pub(crate) fn try_new(
        surface: AccountMarketSurface,
        onboarding_session_id: Uuid,
        expected_public_configuration_digest: EvidenceDigest,
        expected_runtime_verification_receipt_digest: EvidenceDigest,
        expected_credential_generation: SecretGeneration,
    ) -> Result<Self, ServiceError> {
        if onboarding_session_id.is_nil()
            || expected_public_configuration_digest.algorithm() != DigestAlgorithm::Sha256
            || expected_public_configuration_digest.bytes() == [0; 32]
            || expected_runtime_verification_receipt_digest.algorithm() != DigestAlgorithm::Sha256
            || expected_runtime_verification_receipt_digest.bytes() == [0; 32]
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            surface,
            onboarding_session_id,
            expected_public_configuration_digest,
            expected_runtime_verification_receipt_digest,
            expected_credential_generation,
        })
    }

    pub(crate) const fn surface(self) -> AccountMarketSurface {
        self.surface
    }

    pub(crate) const fn onboarding_session_id(self) -> Uuid {
        self.onboarding_session_id
    }

    pub(crate) const fn expected_public_configuration_digest(self) -> EvidenceDigest {
        self.expected_public_configuration_digest
    }

    pub(crate) const fn expected_runtime_verification_receipt_digest(self) -> EvidenceDigest {
        self.expected_runtime_verification_receipt_digest
    }

    pub(crate) const fn expected_credential_generation(self) -> SecretGeneration {
        self.expected_credential_generation
    }
}

/// Application authority that resolves exact prepared configuration without exposing secrets.
///
/// The implementation must recover an already admitted onboarding lease and canonical instrument
/// bindings. It may perform bounded public reference/identity resolution through its owned
/// official Nasdaq reference and repository-owned catalog authorities. It must not read credentials,
/// activate a provider market-data
/// account, contact a credentialed market-data endpoint, or weaken the request's exact
/// surface/session/configuration binding.
#[async_trait]
pub(crate) trait PreparedMarketProviderConfigurationResolver: Send + Sync + 'static {
    async fn resolve(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedMarketProviderConfiguration, ServiceError>;

    /// Stops admitting new identity/configuration work before runtime children are drained.
    fn begin_shutdown(&self);

    /// Reaps the resolver's extraction and publication authorities under the registry deadline.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Revalidates a resolver result before any credential activation or child startup begins.
pub(super) fn validate_resolved_configuration(
    request: PreparedMarketProviderConfigurationRequest,
    prepared: &PreparedMarketProviderConfiguration,
) -> Result<(), ServiceError> {
    let (surface, lease) = match prepared {
        PreparedMarketProviderConfiguration::AlpacaBasic(value) => {
            (AccountMarketSurface::AlpacaBasic, value.lease())
        }
        PreparedMarketProviderConfiguration::KrakenLevel3(value) => {
            (AccountMarketSurface::KrakenLevel3, value.lease())
        }
    };
    if surface != request.surface()
        || lease.surface_id().as_str() != request.surface().surface_id()
        || lease.session_id() != request.onboarding_session_id()
        || lease.public_configuration_digest() != request.expected_public_configuration_digest()
        || lease.runtime_evidence_digest() != request.expected_runtime_verification_receipt_digest()
        || lease.generation() != Some(request.expected_credential_generation())
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(())
}
