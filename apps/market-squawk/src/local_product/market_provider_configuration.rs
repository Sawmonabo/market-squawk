//! Installed account-market configuration resolution.
//!
//! This boundary corroborates the small code-owned overview set against repository-owned identity
//! and official reference evidence. It never turns a listing symbol or external identifier into
//! an internal identity, never creates execution terms for listed securities, and never reads an
//! account credential. Credential activation and provider connections remain owned by the
//! market-runtime registry after this resolver returns.

use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_adapter_alpaca::AlpacaTransportLimits;
use market_squawk_adapter_kraken::{KrakenL3ClientTier, KrakenL3Depth};
use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::{
    EffectiveInterval, ExactPayloadEvidence, ProviderInstrumentId, SourceId, Timestamp,
};
use market_squawk_platform::AppConfig;
use market_squawk_services::ServiceError;
use market_squawk_sources::FreshnessPolicy;
use tokio_util::sync::CancellationToken;

use crate::application::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
    PreparedMarketProviderConfigurationResolver,
};
use crate::provider_activation::nasdaq_reference::NasdaqReferenceUniverseService;
use crate::provider_activation::{
    AlpacaBasicMarketConfigurationInput, BoundedMarketDataInstrumentSet,
    BoundedMarketInstrumentSet, KrakenL3MarketConfigurationInput, MarketDataInstrumentBinding,
    MarketInstrumentBinding, MarketReferenceIdentityAuthority, MarketReferenceIdentityRequest,
    MarketReferenceIdentityResolution, MarketSourceEvidence, MarketSubscriptionPriority,
    PreparedMarketProviderConfiguration, ProviderMarketConfigurationRequest,
};
use crate::{ProviderAdapterActivation, ProviderOnboardingService, ResearchService};

const ALPACA_IEX_SOURCE: &str = "alpaca-basic-iex-market-data";
const KRAKEN_LEVEL3_SOURCE: &str = "kraken-authenticated-level3-market-data";

const SECOND_NANOS: u64 = 1_000_000_000;
const MAX_PROVIDER_FRAME_BYTES: usize = 1024 * 1024;

/// Production resolver shared by lifecycle restoration and foreground source activation.
pub(super) struct ProductionMarketProviderConfigurationResolver {
    config: AppConfig,
    onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    reference_identity: MarketReferenceIdentityAuthority,
    market_data_instruments: MarketDataInstrumentReadCapability,
}

impl ProductionMarketProviderConfigurationResolver {
    pub(super) fn new(
        config: AppConfig,
        onboarding: Arc<ProviderOnboardingService>,
        provider_activation: Arc<ProviderAdapterActivation>,
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        research: &ResearchService,
    ) -> Arc<Self> {
        let market_data_instruments = research.market_data_instruments();
        let reference_identity = MarketReferenceIdentityAuthority::new(
            Arc::clone(&nasdaq),
            market_data_instruments.clone(),
        );
        Arc::new(Self {
            config,
            onboarding,
            provider_activation,
            nasdaq,
            reference_identity,
            market_data_instruments,
        })
    }

    async fn resolve_display_bindings(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoundedMarketDataInstrumentSet, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let listings = self
            .nasdaq
            .default_overview_current_listings(deadline, cancellation)
            .await
            .map_err(|error| map_reference_error(error, deadline, cancellation))?;
        if listings.is_empty() {
            return Err(ServiceError::Unavailable);
        }
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(listings.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for listing in listings {
            let resolution = self
                .reference_identity
                .resolve(
                    MarketReferenceIdentityRequest::new(
                        listing.key().symbol().clone(),
                        listing.key().mic().clone(),
                    ),
                    deadline,
                    cancellation,
                )
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "repository-owned market identity resolution failed");
                    request_state_error(deadline, cancellation)
                })?;
            let MarketReferenceIdentityResolution::Available(approval) = resolution else {
                tracing::debug!(
                    symbol = listing.key().symbol().as_str(),
                    venue = listing.key().mic().as_str(),
                    "default market listing has no exact canonical identity approval"
                );
                continue;
            };
            let Some(record) = self
                .market_data_instruments
                .latest(approval.instrument_id(), deadline, cancellation)
                .map_err(|error| {
                    tracing::error!(%error, "repository-owned market identity read failed");
                    request_state_error(deadline, cancellation)
                })?
            else {
                continue;
            };
            bindings.push(
                MarketDataInstrumentBinding::try_from_nasdaq_session_listing(
                    MarketSubscriptionPriority::Benchmark,
                    record,
                    listing.key().symbol().clone(),
                    listing,
                    &approval,
                )
                .map_err(|error| {
                    tracing::error!(%error, "market display symbol binding failed");
                    ServiceError::InvalidResult
                })?,
            );
        }
        BoundedMarketDataInstrumentSet::try_new(bindings).map_err(|error| {
            tracing::warn!(%error, "no exact default U.S. market identities were admitted");
            ServiceError::Unavailable
        })
    }

    async fn resolve_alpaca(
        &self,
        lease: crate::ProviderActivationLease,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedMarketProviderConfiguration, ServiceError> {
        let instruments = self
            .resolve_display_bindings(deadline, cancellation)
            .await?;
        let configured_at = system_timestamp()?;
        let request =
            ProviderMarketConfigurationRequest::AlpacaBasic(AlpacaBasicMarketConfigurationInput {
                configured_at,
                iex_evidence: source_evidence(&lease, ALPACA_IEX_SOURCE)?,
                options_evidence: None,
                iex_instruments: instruments,
                option_instruments: None,
                transport_limits: AlpacaTransportLimits::try_new(
                    MAX_PROVIDER_FRAME_BYTES,
                    Duration::from_secs(5),
                    Duration::from_secs(10),
                )
                .map_err(|error| {
                    tracing::error!(%error, "Alpaca transport policy is invalid");
                    ServiceError::Internal
                })?,
            });
        self.provider_activation
            .try_construct_staged_market_provider_configuration(lease, request)
            .map_err(|error| {
                tracing::warn!(%error, "Alpaca market configuration resolution failed");
                request_state_error(deadline, cancellation)
            })
    }

    fn resolve_kraken(
        &self,
        lease: crate::ProviderActivationLease,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedMarketProviderConfiguration, ServiceError> {
        ensure_before(deadline, cancellation)?;
        let configured = self.config.kraken().ok_or(ServiceError::Unavailable)?;
        let provider_symbol = ProviderInstrumentId::try_from(configured.symbol())
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let binding = MarketInstrumentBinding::try_new_provisional_kraken(
            MarketSubscriptionPriority::Benchmark,
            configured.definition().clone(),
            provider_symbol,
        )
        .map_err(|error| {
            tracing::error!(%error, "configured Kraken symbol cannot be bound to exact terms");
            ServiceError::InvalidResult
        })?;
        let instruments = BoundedMarketInstrumentSet::try_new(vec![binding]).map_err(|error| {
            tracing::error!(%error, "configured Kraken level-3 set is invalid");
            ServiceError::InvalidResult
        })?;
        let configured_at = system_timestamp()?;
        let request =
            ProviderMarketConfigurationRequest::KrakenLevel3(KrakenL3MarketConfigurationInput {
                configured_at,
                evidence: source_evidence(&lease, KRAKEN_LEVEL3_SOURCE)?,
                instruments,
                retained_depth: KrakenL3Depth::OneHundred,
                client_tier: KrakenL3ClientTier::Standard,
                max_message_bytes: NonZeroUsize::new(MAX_PROVIDER_FRAME_BYTES)
                    .ok_or(ServiceError::Internal)?,
            });
        self.provider_activation
            .try_construct_staged_market_provider_configuration(lease, request)
            .map_err(|error| {
                tracing::warn!(%error, "Kraken level-3 configuration resolution failed");
                request_state_error(deadline, cancellation)
            })
    }
}

#[async_trait]
impl PreparedMarketProviderConfigurationResolver for ProductionMarketProviderConfigurationResolver {
    async fn resolve(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedMarketProviderConfiguration, ServiceError> {
        ensure_before(deadline, &cancellation)?;
        let lease = self
            .onboarding
            .activation_lease(request.onboarding_session_id())
            .or_else(|_| {
                self.onboarding
                    .prepared_activation_lease(request.onboarding_session_id())
            })
            .map_err(|error| {
                tracing::warn!(%error, "account-market activation lease is unavailable");
                ServiceError::Unauthorized
            })?;
        if lease.surface_id().as_str() != request.surface().surface_id()
            || lease.public_configuration_digest() != request.expected_public_configuration_digest()
            || lease.runtime_evidence_digest()
                != request.expected_runtime_verification_receipt_digest()
            || lease.generation() != Some(request.expected_credential_generation())
        {
            return Err(ServiceError::InvalidRequest);
        }
        match request.surface() {
            AccountMarketSurface::AlpacaBasic => {
                self.resolve_alpaca(lease, deadline, &cancellation).await
            }
            AccountMarketSurface::KrakenLevel3 => {
                self.resolve_kraken(lease, deadline, &cancellation)
            }
            AccountMarketSurface::SchwabMarketData => Err(ServiceError::InvalidRequest),
        }
    }

    fn begin_shutdown(&self) {}

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        if Instant::now() >= deadline {
            Err(ServiceError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

fn source_evidence(
    lease: &crate::ProviderActivationLease,
    source: &str,
) -> Result<MarketSourceEvidence, ServiceError> {
    let effective = EffectiveInterval::new(
        lease.authority_effective_at(),
        lease.verification_expires_at(),
    )
    .map_err(|_error| ServiceError::InvalidRequest)?;
    Ok(MarketSourceEvidence::new(
        SourceId::try_from(source).map_err(|_error| ServiceError::Internal)?,
        ExactPayloadEvidence::from_content_digest(lease.capability_digest()),
        effective,
        live_freshness()?,
    ))
}

fn live_freshness() -> Result<FreshnessPolicy, ServiceError> {
    FreshnessPolicy::try_new(
        30 * SECOND_NANOS,
        5 * SECOND_NANOS,
        5 * SECOND_NANOS,
        5 * SECOND_NANOS,
        SECOND_NANOS,
    )
    .map_err(|_error| ServiceError::Internal)
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Internal)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ServiceError::Internal)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_before(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn request_state_error(deadline: Instant, cancellation: &CancellationToken) -> ServiceError {
    if cancellation.is_cancelled() {
        ServiceError::Cancelled
    } else if Instant::now() >= deadline {
        ServiceError::DeadlineExceeded
    } else {
        ServiceError::Unavailable
    }
}

fn map_reference_error(
    error: impl std::fmt::Display,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> ServiceError {
    tracing::warn!(%error, "default U.S. market reference resolution failed");
    request_state_error(deadline, cancellation)
}
