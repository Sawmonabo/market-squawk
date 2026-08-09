//! Installed account-market configuration resolution.
//!
//! This boundary enriches only the small code-owned overview set with stable, public FIGI
//! identity. It never turns a listing symbol into an internal identity, never creates execution
//! terms for listed securities, and never reads an account credential. Credential activation and
//! provider connections remain owned by the market-runtime registry after this resolver returns.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_adapter_alpaca::AlpacaTransportLimits;
use market_squawk_adapter_kraken::{KrakenL3ClientTier, KrakenL3Depth};
use market_squawk_adapter_nasdaq_symbols::NASDAQ_SYMBOL_DIRECTORY_VENUES;
use market_squawk_adapter_tradier::TradierTransportLimits;
use market_squawk_data::MarketDataInstrumentReadCapability;
use market_squawk_domain::{
    Currency, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_platform::AppConfig;
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    FreshnessPolicy, HttpRequestBounds, ProviderRateAuthority, install_ring_tls_provider,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
    PreparedMarketProviderConfigurationResolver,
};
use crate::provider_activation::nasdaq_reference::{
    NasdaqCurrentListing, NasdaqListingKey, NasdaqReferenceUniverseService,
};
use crate::provider_activation::openfigi_identity::{
    EvidenceBoundQuoteCurrency, OpenFigiIdentityPublicationResult,
    OpenFigiIdentityPublicationStatus, OpenFigiIdentityPublisher, OpenFigiPublicTermsEvidence,
    OpenFigiQuoteCurrencyPolicy,
};
use crate::provider_activation::{
    AlpacaBasicMarketConfigurationInput, BoundedMarketDataInstrumentSet,
    BoundedMarketInstrumentSet, KrakenL3MarketConfigurationInput, MarketDataInstrumentBinding,
    MarketInstrumentBinding, MarketSourceEvidence, MarketSubscriptionPriority,
    PreparedMarketProviderConfiguration, ProviderMarketConfigurationRequest,
    TradierMarketConfigurationInput,
};
use crate::{ProviderAdapterActivation, ProviderOnboardingService, ResearchService};

const OPENFIGI_TERMS_URL: &str = "https://www.openfigi.com/docs/terms-of-service";
const OPENFIGI_API_DOCUMENTATION_URL: &str = "https://www.openfigi.com/api/documentation";
const OPENFIGI_TERMS_VERSION: &str = "last-updated-2018-11-27-reviewed-2026-08-09";
const OPENFIGI_API_VERSION: &str = "openfigi-v3-reviewed-2026-08-09";
const OPENFIGI_REVIEWED_FROM_UNIX_NANOS: i64 = 1_786_233_600_000_000_000;
const OPENFIGI_REVIEWED_UNTIL_UNIX_NANOS: i64 = 1_817_769_600_000_000_000;

const ECFR_RULE_612_URL: &str =
    "https://www.ecfr.gov/api/versioner/v1/full/2026-08-06/title-17.xml?section=242.612";
const ECFR_RULE_612_VERSION: &str = "2026-08-06";
const ECFR_RULE_612_SHA256: [u8; 32] = [
    0x99, 0x20, 0xa7, 0x90, 0xa5, 0x74, 0xb5, 0xe8, 0x92, 0xb7, 0xee, 0x83, 0xe8, 0xbc, 0x43, 0xaa,
    0xc9, 0x2a, 0xe9, 0xb0, 0x64, 0xbc, 0xb8, 0x3f, 0x3e, 0xeb, 0x1c, 0xba, 0x51, 0x50, 0x69, 0x9b,
];

const ALPACA_IEX_SOURCE: &str = "alpaca-basic-iex-market-data";
const TRADIER_STREAM_SOURCE: &str = "tradier-consolidated-stream-market-data";
const TRADIER_REST_SOURCE: &str = "tradier-consolidated-rest-market-data";
const KRAKEN_LEVEL3_SOURCE: &str = "kraken-authenticated-level3-market-data";

const SECOND_NANOS: u64 = 1_000_000_000;
const MAX_PROVIDER_FRAME_BYTES: usize = 1024 * 1024;
const MAX_PROVIDER_HTTP_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;

/// Production resolver shared by lifecycle restoration and foreground source activation.
pub(super) struct ProductionMarketProviderConfigurationResolver {
    config: AppConfig,
    onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    openfigi: Arc<OpenFigiIdentityPublisher>,
    market_data_instruments: MarketDataInstrumentReadCapability,
}

impl ProductionMarketProviderConfigurationResolver {
    pub(super) fn try_new(
        config: AppConfig,
        onboarding: Arc<ProviderOnboardingService>,
        provider_activation: Arc<ProviderAdapterActivation>,
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        research: &ResearchService,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Arc<Self>, ServiceError> {
        let public_terms = reviewed_openfigi_terms()?;
        let quote_currency_policy: Arc<dyn OpenFigiQuoteCurrencyPolicy> =
            Arc::new(UsListedUsdQuoteCurrencyPolicy::try_new()?);
        let openfigi = OpenFigiIdentityPublisher::try_new(
            Arc::clone(&nasdaq),
            provider_rate,
            install_ring_tls_provider().map_err(|error| {
                tracing::error!(%error, "OpenFIGI TLS authority construction failed");
                ServiceError::Unavailable
            })?,
            public_terms,
            research.market_data_instrument_synchronization(),
            research.market_data_instruments(),
            quote_currency_policy,
        )
        .map_err(|error| {
            tracing::error!(%error, "OpenFIGI identity authority construction failed");
            ServiceError::Unavailable
        })?;
        Ok(Arc::new(Self {
            config,
            onboarding,
            provider_activation,
            nasdaq,
            openfigi: Arc::new(openfigi),
            market_data_instruments: research.market_data_instruments(),
        }))
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
        let keys = sorted_listing_keys(&listings)?;
        let receipt = self
            .openfigi
            .resolve_selected_and_publish(keys, deadline, cancellation)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "bounded OpenFIGI identity resolution failed");
                request_state_error(deadline, cancellation)
            })?;
        tracing::debug!(
            provider_batches = receipt.provider_receipts().len(),
            catalog_published = receipt.catalog_receipt().is_some(),
            resolved_listings = receipt.results().len(),
            "bounded OpenFIGI identity resolution completed"
        );
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(receipt.results().len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for result in receipt.results() {
            let OpenFigiIdentityPublicationStatus::Exact { instrument_id, .. } = result.status()
            else {
                continue;
            };
            let listing = listing_for_result(&listings, result)
                .ok_or(ServiceError::InvalidResult)?
                .clone();
            let record = self
                .market_data_instruments
                .latest(*instrument_id, deadline, cancellation)
                .map_err(|error| {
                    tracing::error!(%error, "FIGI-backed market identity read failed");
                    request_state_error(deadline, cancellation)
                })?
                .ok_or(ServiceError::InvalidResult)?;
            bindings.push(
                MarketDataInstrumentBinding::try_from_nasdaq_session_listing(
                    MarketSubscriptionPriority::Benchmark,
                    record.definition().clone(),
                    listing.key().symbol().clone(),
                    listing,
                    result,
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
            .try_construct_market_provider_configuration(lease, request)
            .map_err(|error| {
                tracing::warn!(%error, "Alpaca market configuration resolution failed");
                request_state_error(deadline, cancellation)
            })
    }

    async fn resolve_tradier(
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
            ProviderMarketConfigurationRequest::Tradier(TradierMarketConfigurationInput {
                configured_at,
                consolidated_stream_evidence: source_evidence(&lease, TRADIER_STREAM_SOURCE)?,
                consolidated_rest_evidence: source_evidence(&lease, TRADIER_REST_SOURCE)?,
                derived_index_rest_evidence: None,
                consolidated_instruments: instruments,
                derived_indexes: None,
                transport_limits: tradier_transport_limits()?,
            });
        self.provider_activation
            .try_construct_market_provider_configuration(lease, request)
            .map_err(|error| {
                tracing::warn!(%error, "Tradier market configuration resolution failed");
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
            .try_construct_market_provider_configuration(lease, request)
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
            .map_err(|error| {
                tracing::warn!(%error, "account-market activation lease is unavailable");
                ServiceError::Unauthorized
            })?;
        if lease.surface_id().as_str() != request.surface().surface_id()
            || lease.public_configuration_digest() != request.expected_public_configuration_digest()
        {
            return Err(ServiceError::InvalidRequest);
        }
        match request.surface() {
            AccountMarketSurface::AlpacaBasic => {
                self.resolve_alpaca(lease, deadline, &cancellation).await
            }
            AccountMarketSurface::Tradier => {
                self.resolve_tradier(lease, deadline, &cancellation).await
            }
            AccountMarketSurface::KrakenLevel3 => {
                self.resolve_kraken(lease, deadline, &cancellation)
            }
        }
    }

    fn begin_shutdown(&self) {
        self.openfigi.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.openfigi
            .finish_shutdown(deadline)
            .await
            .map_err(|error| {
                tracing::error!(%error, "OpenFIGI identity authority shutdown failed");
                if Instant::now() >= deadline {
                    ServiceError::DeadlineExceeded
                } else {
                    ServiceError::Unavailable
                }
            })
    }
}

#[derive(Clone)]
struct UsListedUsdQuoteCurrencyPolicy {
    value: EvidenceBoundQuoteCurrency,
}

impl UsListedUsdQuoteCurrencyPolicy {
    fn try_new() -> Result<Self, ServiceError> {
        let evidence = ExactPayloadEvidence::with_version_pinned_locator(
            EvidenceDigest::new(DigestAlgorithm::Sha256, ECFR_RULE_612_SHA256),
            VersionPinnedSourceLocator::new(
                identifier(ECFR_RULE_612_URL)?,
                identifier(ECFR_RULE_612_VERSION)?,
            ),
        );
        Ok(Self {
            value: EvidenceBoundQuoteCurrency::try_new(
                Currency::try_from("USD").map_err(|_error| ServiceError::Internal)?,
                evidence,
            )
            .map_err(|error| {
                tracing::error!(%error, "U.S. listed quote-currency policy is invalid");
                ServiceError::Internal
            })?,
        })
    }
}

impl OpenFigiQuoteCurrencyPolicy for UsListedUsdQuoteCurrencyPolicy {
    fn quote_currency_for(&self, listing: &NasdaqListingKey) -> Option<EvidenceBoundQuoteCurrency> {
        NASDAQ_SYMBOL_DIRECTORY_VENUES
            .iter()
            .any(|mic| listing.mic().as_str() == *mic)
            .then(|| self.value.clone())
    }
}

fn reviewed_openfigi_terms() -> Result<OpenFigiPublicTermsEvidence, ServiceError> {
    let effective = EffectiveInterval::new(
        Timestamp::from_unix_nanos(OPENFIGI_REVIEWED_FROM_UNIX_NANOS),
        Some(Timestamp::from_unix_nanos(
            OPENFIGI_REVIEWED_UNTIL_UNIX_NANOS,
        )),
    )
    .map_err(|_error| ServiceError::Internal)?;
    OpenFigiPublicTermsEvidence::try_new(
        reviewed_manifest_evidence(
            b"market-squawk/openfigi-figi-public-domain-review/v1\0",
            OPENFIGI_TERMS_URL,
            OPENFIGI_TERMS_VERSION,
        )?,
        reviewed_manifest_evidence(
            b"market-squawk/openfigi-public-api-contract-review/v1\0",
            OPENFIGI_API_DOCUMENTATION_URL,
            OPENFIGI_API_VERSION,
        )?,
        effective,
    )
    .map_err(|error| {
        tracing::error!(%error, "reviewed OpenFIGI authority evidence is invalid");
        ServiceError::Internal
    })
}

fn reviewed_manifest_evidence(
    domain: &[u8],
    reference: &str,
    version: &str,
) -> Result<ExactPayloadEvidence, ServiceError> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    update_text(&mut hasher, reference)?;
    update_text(&mut hasher, version)?;
    Ok(ExactPayloadEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into()),
        VersionPinnedSourceLocator::new(identifier(reference)?, identifier(version)?),
    ))
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

fn tradier_transport_limits() -> Result<TradierTransportLimits, ServiceError> {
    let http = HttpRequestBounds::try_new(
        nonzero_u64(5 * SECOND_NANOS)?,
        nonzero_u64(10 * SECOND_NANOS)?,
        nonzero_u64(15 * SECOND_NANOS)?,
        0,
        nonzero_u64(MAX_PROVIDER_HTTP_RESPONSE_BYTES)?,
    )
    .map_err(|_error| ServiceError::Internal)?;
    TradierTransportLimits::try_new(MAX_PROVIDER_FRAME_BYTES, Duration::from_secs(10), http)
        .map_err(|error| {
            tracing::error!(%error, "Tradier transport policy is invalid");
            ServiceError::Internal
        })
}

fn sorted_listing_keys(
    listings: &[NasdaqCurrentListing],
) -> Result<Vec<NasdaqListingKey>, ServiceError> {
    let mut keys = Vec::new();
    keys.try_reserve_exact(listings.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    keys.extend(listings.iter().map(|listing| listing.key().clone()));
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Err(ServiceError::Unavailable);
    }
    Ok(keys)
}

fn listing_for_result<'a>(
    listings: &'a [NasdaqCurrentListing],
    result: &OpenFigiIdentityPublicationResult,
) -> Option<&'a NasdaqCurrentListing> {
    listings
        .iter()
        .find(|listing| listing.key() == result.listing())
}

fn identifier(value: &str) -> Result<SourceIdentifier, ServiceError> {
    SourceIdentifier::try_from(value).map_err(|_error| ServiceError::Internal)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, ServiceError> {
    NonZeroU64::new(value).ok_or(ServiceError::Internal)
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

fn update_text(hasher: &mut Sha256, value: &str) -> Result<(), ServiceError> {
    let length = u64::try_from(value.len()).map_err(|_error| ServiceError::Internal)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
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
