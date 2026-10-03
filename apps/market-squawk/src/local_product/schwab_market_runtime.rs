//! Installed resolution of the exact one-use Schwab market runtime package.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_data::{
    ListingReferenceReadCapability, MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
    OfficialIssuerInstrumentReference,
};
use market_squawk_domain::{AssetClass, ProviderIdentityRecord, Timestamp, VenueId};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ResearchService,
    application::{
        AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
        PreparedSchwabMarketRuntimeResolver,
    },
    provider_activation::{
        MarketDataInstrumentBinding, MarketInstrumentReferenceBinding,
        MarketReferenceIdentityApprovalV1, MarketReferenceIdentityAuthority,
        MarketReferenceIdentityRequest, MarketReferenceIdentityResolution,
        MarketSubscriptionPriority, PreparedSchwabMarketRuntimeStart,
        SchwabMarketDataAccountActivation,
        SchwabQuoteReferenceBinding,
        nasdaq_reference::{NasdaqListingKey, NasdaqReferenceUniverseService},
    },
};

use super::cli_provider::ProviderResearchActivationService;

const MAXIMUM_SCHWAB_INSTRUMENTS: usize = 50;

pub(super) struct ProductionSchwabMarketRuntimeResolver {
    onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    reference_identity: MarketReferenceIdentityAuthority,
    listing_reference: Option<ListingReferenceReadCapability>,
    market_data_instruments: MarketDataInstrumentReadCapability,
    portal: OnceLock<Arc<ProviderResearchActivationService>>,
    accepting: AtomicBool,
}

impl ProductionSchwabMarketRuntimeResolver {
    pub(super) fn new(
        onboarding: Arc<ProviderOnboardingService>,
        provider_activation: Arc<ProviderAdapterActivation>,
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        research: &ResearchService,
    ) -> Arc<Self> {
        let market_data_instruments = research.market_data_instruments();
        Arc::new(Self {
            onboarding,
            provider_activation,
            reference_identity: MarketReferenceIdentityAuthority::new(
                Arc::clone(&nasdaq),
                market_data_instruments.clone(),
            ),
            listing_reference: nasdaq.listing_reference_reader(),
            nasdaq,
            market_data_instruments,
            portal: OnceLock::new(),
            accepting: AtomicBool::new(true),
        })
    }

    pub(super) fn bind_portal(
        &self,
        portal: Arc<ProviderResearchActivationService>,
    ) -> Result<(), ServiceError> {
        self.portal
            .set(portal)
            .map_err(|_| ServiceError::InvalidRequest)
    }

    async fn bootstrap_instrument_references(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<MarketDataInstrumentRecord>, ServiceError> {
        let reader = self
            .listing_reference
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let mut records = Vec::new();
        for issuer in OfficialIssuerInstrumentReference::predeclared_benchmarks().map_err(|_| ServiceError::InvalidResult)? {
            ensure_before(&self.accepting, deadline, cancellation)?;
            let key = NasdaqListingKey::new(
                market_squawk_domain::ProviderInstrumentId::try_from(issuer.symbol().as_str())
                    .map_err(|_| ServiceError::Internal)?,
                issuer.venue().clone(),
            );
            let listings = self
                .nasdaq
                .selected_current_listings(&[key], deadline, cancellation)
                .await
                .map_err(|_| request_state_error(deadline, cancellation))?;
            if listings.len() != 1 {
                return Err(ServiceError::Unavailable);
            }
            let listing = reader
                .exact_current(issuer.symbol().as_str(), issuer.venue(), deadline, cancellation)
                .map_err(|_| request_state_error(deadline, cancellation))?
                .ok_or(ServiceError::Unavailable)?;
            let at = system_timestamp()?;
            let existing = self
                .market_data_instruments
                .resolve_exact_as_of(issuer.cusip().as_str(), at, at, deadline, cancellation)
                .map_err(|_| request_state_error(deadline, cancellation))?;
            if existing.has_more() || existing.matches().len() > 1 {
                return Err(ServiceError::Unavailable);
            }
            let expected = existing
                .matches()
                .first()
                .map(|matched| matched.record().clone());
            let record = self
                .provider_activation
                .publish_schwab_instrument_reference(
                    activation,
                    &issuer,
                    listing,
                    expected,
                    deadline,
                    cancellation.child_token(),
                )
                .await?;
            records.push(record);
        }
        Ok(records)
    }

    async fn resolve_bindings(
        &self,
        records: Vec<MarketDataInstrumentRecord>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedSchwabBindings, ServiceError> {
        ensure_before(&self.accepting, deadline, cancellation)?;
        if records.is_empty() || records.len() > MAXIMUM_SCHWAB_INSTRUMENTS {
            return Err(ServiceError::Unavailable);
        }
        let mut quotes = Vec::new();
        let mut display = Vec::new();
        let mut approvals = Vec::new();
        let issuers = OfficialIssuerInstrumentReference::predeclared_benchmarks().map_err(|_| ServiceError::InvalidResult)?;
        let reader = self
            .listing_reference
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        for record in records {
            ensure_before(&self.accepting, deadline, cancellation)?;
            let at = system_timestamp()?;
            let definition = record.definition();
            let identity = exact_provider_identity(definition, at)?;
            let issuer = issuers
                .iter()
                .find(|issuer| {
                    issuer.symbol().as_str() == identity.provider_instrument_id().as_str()
                })
                .ok_or(ServiceError::Unavailable)?;
            if definition.asset_class() != AssetClass::Fund {
                return Err(ServiceError::Unavailable);
            }
            let (listing, official) = self
                .exact_current_listing(&identity, issuer.venue(), reader, deadline, cancellation)
                .await?;
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
                .map_err(|_| request_state_error(deadline, cancellation))?;
            let MarketReferenceIdentityResolution::Available(approval) = resolution else {
                return Err(ServiceError::Unavailable);
            };
            display.push(
                MarketDataInstrumentBinding::try_from_nasdaq_session_listing(
                    MarketSubscriptionPriority::Benchmark,
                    record.clone(),
                    listing.key().symbol().clone(),
                    listing,
                    &approval,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            );
            quotes.push(
                SchwabQuoteReferenceBinding::try_new(
                    record,
                    identity,
                    MarketInstrumentReferenceBinding::NasdaqListing(official),
                    MarketSubscriptionPriority::Benchmark,
                    at,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            );
            approvals.push(approval);
        }
        Ok(ResolvedSchwabBindings {
            quotes,
            display,
            approvals,
            uses_listing_reference: true,
        })
    }

    async fn exact_current_listing(
        &self,
        provider_identity: &ProviderIdentityRecord,
        venue: &VenueId,
        listing_reader: &ListingReferenceReadCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            crate::provider_activation::nasdaq_reference::NasdaqCurrentListing,
            market_squawk_data::ListingReferenceRecord,
        ),
        ServiceError,
    > {
        let symbol = provider_identity.provider_instrument_id();
        let keys = [NasdaqListingKey::new(symbol.clone(), venue.clone())];
        let listings = self
            .nasdaq
            .selected_current_listings(&keys, deadline, cancellation)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "official Schwab listing identity is unavailable");
                request_state_error(deadline, cancellation)
            })?;
        if listings.len() != 1 {
            return Err(ServiceError::Unavailable);
        }
        let listing = listings
            .into_iter()
            .next()
            .ok_or(ServiceError::Unavailable)?;
        let official = listing_reader
            .exact_current(symbol.as_str(), listing.key().mic(), deadline, cancellation)
            .map_err(|error| {
                tracing::warn!(%error, "durable Schwab listing reference is unavailable");
                request_state_error(deadline, cancellation)
            })?
            .ok_or(ServiceError::Unavailable)?;
        Ok((listing, official))
    }
}

#[async_trait]
impl PreparedSchwabMarketRuntimeResolver for ProductionSchwabMarketRuntimeResolver {
    async fn resolve(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedSchwabMarketRuntimeStart, ServiceError> {
        ensure_before(&self.accepting, deadline, &cancellation)?;
        if request.surface() != AccountMarketSurface::SchwabMarketData {
            return Err(ServiceError::InvalidRequest);
        }
        let lease = self
            .onboarding
            .activation_lease(request.onboarding_session_id())
            .map_err(|error| {
                tracing::warn!(%error, "active Schwab onboarding lease is unavailable");
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
        let portal = self.portal.get().ok_or(ServiceError::Unavailable)?;
        let oauth = portal
            .schwab_market_authority(request.onboarding_session_id(), cancellation.child_token())
            .await
            .map_err(|error| {
                tracing::warn!(%error, "active Schwab OAuth market authority is unavailable");
                ServiceError::Unauthorized
            })?;
        let activation = self
            .provider_activation
            .activate_schwab_market_data_account(lease, oauth, cancellation.child_token())
            .await
            .map_err(|error| {
                tracing::warn!(%error, "Schwab market-data account activation failed");
                request_state_error(deadline, &cancellation)
            })?;
        let activation = Arc::new(activation);
        let records = self
            .bootstrap_instrument_references(&activation, deadline, &cancellation)
            .await?;
        let resolved = self
            .resolve_bindings(records, deadline, &cancellation)
            .await?;
        let streamer_admitted = activation
            .doctor_receipt()
            .observation()
            .families
            .iter()
            .any(|family| {
                family.family == market_squawk_sources::SchwabMarketDataFamily::LevelOneEquities
                    && matches!(
                        family.disposition,
                        market_squawk_sources::RuntimeCapabilityDisposition::Available
                            | market_squawk_sources::RuntimeCapabilityDisposition::Degraded
                    )
            });
        if streamer_admitted {
            return self
                .provider_activation
                .prepare_schwab_streamer_market_runtime_start(
                    activation,
                    resolved.quotes,
                    resolved.display,
                    resolved.approvals,
                    self.market_data_instruments.clone(),
                    self.listing_reference.clone(),
                    deadline,
                    cancellation,
                )
                .await;
        }
        let generation = self
            .provider_activation
            .register_schwab_quote_generation(&activation, &resolved.quotes)
            .await?;
        let reference_identity = resolved
            .uses_listing_reference
            .then_some(self.reference_identity.clone());
        let listing_reference = if resolved.uses_listing_reference {
            self.listing_reference.clone()
        } else {
            None
        };
        let preparation_cancellation = cancellation.clone();
        let retained_generation = generation.clone();
        let prepared = self
            .provider_activation
            .prepare_schwab_market_runtime_start(
                activation,
                generation,
                resolved.quotes,
                resolved.display,
                reference_identity,
                listing_reference,
                resolved.approvals,
                deadline,
                cancellation,
            )
            .await
            .map_err(|error| {
                tracing::warn!(%error, "Schwab market runtime preparation failed");
                request_state_error(deadline, &preparation_cancellation)
            });
        if prepared.is_err() {
            self.provider_activation
                .revoke_research_runtime(&retained_generation)
                .await
                .map_err(|_| ServiceError::Unavailable)?;
        }
        prepared
    }

    fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        if Instant::now() >= deadline {
            Err(ServiceError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

struct ResolvedSchwabBindings {
    quotes: Vec<SchwabQuoteReferenceBinding>,
    display: Vec<MarketDataInstrumentBinding>,
    approvals: Vec<MarketReferenceIdentityApprovalV1>,
    uses_listing_reference: bool,
}

fn exact_provider_identity(
    definition: &market_squawk_domain::MarketDataInstrumentDefinition,
    at: Timestamp,
) -> Result<ProviderIdentityRecord, ServiceError> {
    let mut exact = definition.provider_identities().iter().filter(|identity| {
        identity.source_id().as_str() == "schwab-trader-api-instruments"
            && definition.provider_identity_at(
                identity.source_id(),
                identity.provider_instrument_id(),
                at,
            ) == Some(*identity)
    });
    let identity = exact.next().cloned().ok_or(ServiceError::Unavailable)?;
    if exact.next().is_some() {
        return Err(ServiceError::Unavailable);
    }
    Ok(identity)
}

fn ensure_before(
    accepting: &AtomicBool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if !accepting.load(Ordering::Acquire) {
        Err(ServiceError::Unavailable)
    } else if cancellation.is_cancelled() {
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

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
