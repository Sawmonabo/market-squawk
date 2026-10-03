//! Startup acquisition of genuine Alpaca native asset identities before IEX registration.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_alpaca::{AlpacaAssetReferenceClient, AlpacaError};
use market_squawk_data::{
    AlpacaAssetReferenceAdmission, ListingReferenceRecord, MarketDataInstrumentRecord,
};
use market_squawk_domain::{ProviderInstrumentId, VenueId, VenueSymbol};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    HttpRequestBounds, ProviderNativeIdentityRequest, SharedProviderBudget,
};
use tokio_util::sync::CancellationToken;

use crate::{
    ResearchService, application::ResearchProviderPublicationOperation,
    provider_activation::AlpacaBasicAccountActivation,
};

/// Published canonical position and genuine native coordinates for one selected IEX route.
pub(crate) struct AlpacaNativeAssetRoute {
    pub(crate) after: MarketDataInstrumentRecord,
    pub(crate) native: ProviderNativeIdentityRequest,
}

/// Acquires exactly one authenticated Paper asset response, seals its original body, then
/// atomically creates or enriches its canonical identity against the current official listing.
/// The returned coordinates require current catalog selection before live session startup.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_alpaca_iex_asset_reference(
    activation: &AlpacaBasicAccountActivation,
    research: &Arc<ResearchService>,
    operation: &ResearchProviderPublicationOperation,
    budget: &SharedProviderBudget,
    bounds: HttpRequestBounds,
    before: Option<MarketDataInstrumentRecord>,
    listing: ListingReferenceRecord,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AlpacaNativeAssetRoute, ServiceError> {
    if cancellation.is_cancelled()
        || Instant::now() >= deadline
        || operation.source().source_id() != operation.rights().source_id()
        || before.as_ref().is_some_and(|record| {
            !matches!(
                record.definition().asset_class(),
                market_squawk_domain::AssetClass::Equity | market_squawk_domain::AssetClass::Fund
            )
        })
    {
        return Err(ServiceError::Unavailable);
    }
    let symbol = listing.provider_symbol().to_owned();
    operation
        .validate_precommit()
        .map_err(|_| ServiceError::Unavailable)?;
    activation
        .require_prepared_or_active()
        .await
        .map_err(|_| ServiceError::Unavailable)?;
    let client = AlpacaAssetReferenceClient::try_new(operation.source().clone(), bounds)
        .map_err(|_| ServiceError::Unavailable)?;
    let store = research.provider_capture_store();
    let custody_owner = Arc::clone(research);
    let (asset, capture) = client
        .acquire(
            activation.credentials().as_ref(),
            budget,
            &symbol,
            deadline,
            cancellation,
            move |pending| {
                let store = Arc::clone(&store);
                let custody_owner = Arc::clone(&custody_owner);
                async move {
                    let custody_deadline = Instant::now()
                        .checked_add(Duration::from_secs(120))
                        .ok_or(AlpacaError::DeadlineExceeded)?;
                    let finish = CancellationToken::new();
                    custody_owner
                        .run_owned_research_io(custody_deadline, &finish, move |worker| {
                            if worker.is_cancelled() || Instant::now() >= custody_deadline {
                                return Err(AlpacaError::CaptureMaterial);
                            }
                            let (rejoin, seal) = pending.into_seal_parts()?;
                            let sealed = seal
                                .seal(&store)
                                .map_err(|_| AlpacaError::CaptureMaterial)?;
                            rejoin.try_rejoin(sealed)
                        })
                        .await
                        .map_err(|_| AlpacaError::CaptureMaterial)?
                }
            },
        )
        .await
        .map_err(|error| {
            tracing::warn!(%error, symbol, stage = "asset_reference_acquisition", "native asset reference unavailable");
            ServiceError::Unavailable
        })?;
    operation
        .validate_precommit()
        .map_err(|_| ServiceError::Unavailable)?;
    let native_uuid = asset.id();
    let native_symbol = asset.symbol().to_owned();
    let rights = operation.rights().decision(
        asset.capture().capture().observation_digest(),
        asset.received_at(),
    )?;
    let after = research
        .publish_alpaca_asset_reference(
            AlpacaAssetReferenceAdmission {
                source: operation.source().clone(),
                rights,
                capture,
                asset,
                official_listing: listing,
                expected_current: before,
            },
            operation.precommit_authority(),
            deadline,
            cancellation.clone(),
        )
        .await
        .map_err(|error| {
            tracing::warn!(%error, symbol, stage = "asset_reference_publication", "native asset reference unavailable");
            ServiceError::Unavailable
        })?;
    // Exact replays retain their original publication time. Selection uses today's knowledge
    // cutoff without changing the native observation or catalog revision timestamp.
    let cutoff = market_squawk_domain::Timestamp::from_unix_nanos(
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ServiceError::Unavailable)?
                .as_nanos(),
        )
        .map_err(|_| ServiceError::Unavailable)?,
    );
    let native = ProviderNativeIdentityRequest {
        namespace: operation.source().source_id().clone(),
        provider_instrument_id: ProviderInstrumentId::try_from(native_uuid.to_string())
            .map_err(|_| ServiceError::Unavailable)?,
        instrument: after.definition().instrument_id(),
        venue: VenueId::try_from("iex").map_err(|_| ServiceError::Unavailable)?,
        venue_symbol: VenueSymbol::try_from(native_symbol)
            .map_err(|_| ServiceError::Unavailable)?,
        knowledge_at: cutoff,
        effective_at: cutoff,
    };
    Ok(AlpacaNativeAssetRoute { after, native })
}
