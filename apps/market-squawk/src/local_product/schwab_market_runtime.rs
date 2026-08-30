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
    InstrumentDefinitionReadCapability, ListingReferenceReadCapability,
    MarketDataInstrumentReadCapability,
};
use market_squawk_domain::{AssetClass, InstrumentDefinition, ProviderIdentityRecord, Timestamp};
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ResearchService,
    application::{
        AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
        PreparedSchwabMarketRuntimeResolver,
    },
    provider_activation::{
        BoundedMarketInstrumentSet, MarketDataInstrumentBinding, MarketInstrumentBinding,
        MarketInstrumentReferenceBinding, MarketReferenceIdentityApprovalV1,
        MarketReferenceIdentityAuthority, MarketReferenceIdentityRequest,
        MarketReferenceIdentityResolution, MarketSubscriptionPriority,
        PreparedSchwabMarketRuntimeStart,
        nasdaq_reference::{NasdaqListingKey, NasdaqReferenceUniverseService},
    },
};

use super::cli_provider::ProviderResearchActivationService;

const MAXIMUM_SCHWAB_INSTRUMENTS: usize = 50;
const MAXIMUM_EXACT_LISTING_MATCHES: usize = 16;

pub(super) struct ProductionSchwabMarketRuntimeResolver {
    onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    reference_identity: MarketReferenceIdentityAuthority,
    listing_reference: Option<ListingReferenceReadCapability>,
    instrument_definitions: InstrumentDefinitionReadCapability,
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
            instrument_definitions: research.instrument_definitions(),
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

    async fn resolve_bindings(
        &self,
        metadata: &SourceMetadata,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedSchwabBindings, ServiceError> {
        ensure_before(&self.accepting, deadline, cancellation)?;
        let instrument_ids = metadata.coverage().instruments().instruments();
        if instrument_ids.is_empty() || instrument_ids.len() > MAXIMUM_SCHWAB_INSTRUMENTS {
            return Err(ServiceError::Unavailable);
        }
        let definitions = self
            .instrument_definitions
            .latest(
                instrument_ids,
                MAXIMUM_SCHWAB_INSTRUMENTS,
                deadline,
                cancellation,
            )
            .map_err(|error| {
                tracing::warn!(%error, "canonical Schwab instrument definitions are unavailable");
                request_state_error(deadline, cancellation)
            })?;
        if definitions.len() != instrument_ids.len() {
            return Err(ServiceError::Unavailable);
        }
        let evaluated_at = system_timestamp()?;
        let mut strict = Vec::new();
        let mut display = Vec::new();
        let mut approvals = Vec::new();
        strict
            .try_reserve_exact(definitions.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        display
            .try_reserve_exact(definitions.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        approvals
            .try_reserve_exact(definitions.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut uses_listing_reference = false;
        for definition in definitions {
            ensure_before(&self.accepting, deadline, cancellation)?;
            let provider_identity = exact_provider_identity(&definition, metadata, evaluated_at)?;
            let market_data = self
                .market_data_instruments
                .latest(definition.instrument_id(), deadline, cancellation)
                .map_err(|error| {
                    tracing::warn!(%error, "canonical Schwab market-data identity is unavailable");
                    request_state_error(deadline, cancellation)
                })?
                .ok_or(ServiceError::Unavailable)?;
            match definition.asset_class() {
                AssetClass::Equity | AssetClass::Fund => {
                    let listing_reader = self
                        .listing_reference
                        .as_ref()
                        .ok_or(ServiceError::Unavailable)?;
                    let (listing, official) = self
                        .exact_current_listing(
                            &definition,
                            &provider_identity,
                            listing_reader,
                            deadline,
                            cancellation,
                        )
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
                        .map_err(|error| {
                            tracing::warn!(%error, "Schwab reference identity approval failed");
                            request_state_error(deadline, cancellation)
                        })?;
                    let MarketReferenceIdentityResolution::Available(approval) = resolution else {
                        return Err(ServiceError::Unavailable);
                    };
                    display.push(
                        MarketDataInstrumentBinding::try_from_nasdaq_session_listing(
                            MarketSubscriptionPriority::Benchmark,
                            market_data.clone(),
                            listing.key().symbol().clone(),
                            listing,
                            &approval,
                        )
                        .map_err(|error| {
                            tracing::warn!(%error, "Schwab display identity binding failed");
                            ServiceError::InvalidResult
                        })?,
                    );
                    strict.push(
                        MarketInstrumentBinding::try_new_with_market_data_definition(
                            MarketSubscriptionPriority::Benchmark,
                            definition,
                            provider_identity,
                            MarketInstrumentReferenceBinding::NasdaqListing(official),
                            market_data,
                        )
                        .map_err(|error| {
                            tracing::warn!(%error, "Schwab execution identity binding failed");
                            ServiceError::InvalidResult
                        })?,
                    );
                    approvals.push(approval);
                    uses_listing_reference = true;
                }
                AssetClass::Option | AssetClass::Index | AssetClass::Crypto => {
                    let (strict_binding, display_binding) =
                        exact_assigned_binding(definition, market_data, provider_identity)?;
                    strict.push(strict_binding);
                    display.push(display_binding);
                }
                AssetClass::FixedIncome
                | AssetClass::Future
                | AssetClass::ForeignExchange
                | AssetClass::Commodity
                | AssetClass::Cash => return Err(ServiceError::Unavailable),
            }
        }
        let strict = BoundedMarketInstrumentSet::try_new(strict).map_err(|error| {
            tracing::warn!(%error, "bounded Schwab instrument set is invalid");
            ServiceError::InvalidResult
        })?;
        Ok(ResolvedSchwabBindings {
            strict,
            display,
            approvals,
            uses_listing_reference,
        })
    }

    async fn exact_current_listing(
        &self,
        definition: &InstrumentDefinition,
        provider_identity: &ProviderIdentityRecord,
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
        let mut keys = Vec::new();
        for mapping in definition
            .venue_mappings()
            .iter()
            .filter(|mapping| mapping.venue_symbol().as_str() == symbol.as_str())
        {
            keys.push(NasdaqListingKey::new(
                symbol.clone(),
                mapping.venue_id().clone(),
            ));
        }
        keys.sort();
        keys.dedup();
        if keys.is_empty() {
            return Err(ServiceError::Unavailable);
        }
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
        let page = listing_reader
            .search(
                symbol.as_str(),
                MAXIMUM_EXACT_LISTING_MATCHES,
                deadline,
                cancellation,
            )
            .map_err(|error| {
                tracing::warn!(%error, "durable Schwab listing reference is unavailable");
                request_state_error(deadline, cancellation)
            })?;
        if page.has_more() {
            return Err(ServiceError::Unavailable);
        }
        let mut matches = page.matches().iter().filter(|matched| {
            matched.record().provider_symbol() == symbol.as_str()
                && matched.record().listing_venue() == listing.key().mic()
        });
        let official = matches
            .next()
            .map(|matched| matched.record().clone())
            .ok_or(ServiceError::Unavailable)?;
        if matches.next().is_some() {
            return Err(ServiceError::Unavailable);
        }
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
        let profile =
            market_squawk_domain::SourceIdentifier::try_from(request.surface().surface_id())
                .map_err(|_| ServiceError::Internal)?;
        let generation = self
            .provider_activation
            .research_runtime_generation(&profile)
            .map_err(|error| {
                tracing::warn!(%error, "registered Schwab research generation is unavailable");
                ServiceError::Unavailable
            })?
            .ok_or(ServiceError::Unavailable)?;
        let resolved = self
            .resolve_bindings(generation.metadata(), deadline, &cancellation)
            .await?;
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
        let reference_identity = resolved
            .uses_listing_reference
            .then_some(self.reference_identity.clone());
        let listing_reference = if resolved.uses_listing_reference {
            self.listing_reference.clone()
        } else {
            None
        };
        let preparation_cancellation = cancellation.clone();
        self.provider_activation
            .prepare_schwab_market_runtime_start(
                activation,
                generation,
                resolved.strict,
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
            })
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
    strict: BoundedMarketInstrumentSet,
    display: Vec<MarketDataInstrumentBinding>,
    approvals: Vec<MarketReferenceIdentityApprovalV1>,
    uses_listing_reference: bool,
}

fn exact_provider_identity(
    definition: &InstrumentDefinition,
    metadata: &SourceMetadata,
    at: Timestamp,
) -> Result<ProviderIdentityRecord, ServiceError> {
    let mut exact = definition.provider_identities().iter().filter(|identity| {
        identity.source_id() == metadata.source_id()
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

fn exact_assigned_binding(
    definition: InstrumentDefinition,
    market_data: market_squawk_data::MarketDataInstrumentRecord,
    provider_identity: ProviderIdentityRecord,
) -> Result<(MarketInstrumentBinding, MarketDataInstrumentBinding), ServiceError> {
    let mut selected = None;
    for identifier in definition.identifiers() {
        let strict = MarketInstrumentBinding::try_new_with_market_data_definition(
            MarketSubscriptionPriority::Benchmark,
            definition.clone(),
            provider_identity.clone(),
            MarketInstrumentReferenceBinding::AssignedExternalIdentifier(identifier.clone()),
            market_data.clone(),
        );
        let display = MarketDataInstrumentBinding::try_from_assigned_identifier(
            MarketSubscriptionPriority::Benchmark,
            market_data.clone(),
            provider_identity.provider_instrument_id().clone(),
            identifier.clone(),
        );
        if let (Ok(strict), Ok(display)) = (strict, display) {
            if selected.replace((strict, display)).is_some() {
                return Err(ServiceError::Unavailable);
            }
        }
    }
    selected.ok_or(ServiceError::Unavailable)
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
