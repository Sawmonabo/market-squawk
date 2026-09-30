//! Startup acquisition of genuine Alpaca native asset identities before IEX registration.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_adapter_alpaca::{AlpacaAssetReferenceClient, AlpacaError};
use market_squawk_data::{AlpacaAssetReferenceAdmission, MarketDataInstrumentRecord};
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

/// Before/after canonical position and genuine native coordinates for one configured IEX route.
pub(crate) struct AlpacaNativeAssetRoute {
    pub(crate) before: MarketDataInstrumentRecord,
    pub(crate) after: MarketDataInstrumentRecord,
    pub(crate) native: ProviderNativeIdentityRequest,
}

/// Acquires exactly one authenticated Paper asset response, seals its original body, then
/// atomically adds its UUID to the existing instrument catalog. The returned coordinates still
/// require the source registry's current catalog selection before session startup.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_alpaca_iex_asset_reference(
    activation: &AlpacaBasicAccountActivation,
    research: &Arc<ResearchService>,
    operation: &ResearchProviderPublicationOperation,
    budget: &SharedProviderBudget,
    bounds: HttpRequestBounds,
    before: MarketDataInstrumentRecord,
    symbol: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AlpacaNativeAssetRoute, ServiceError> {
    if cancellation.is_cancelled()
        || Instant::now() >= deadline
        || operation.source().source_id() != operation.rights().source_id()
        || !matches!(
            before.definition().asset_class(),
            market_squawk_domain::AssetClass::Equity | market_squawk_domain::AssetClass::Fund
        )
    {
        return Err(ServiceError::Unavailable);
    }
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
            symbol,
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
                expected_current: before.clone(),
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
    let cutoff = after.published_at();
    let native = ProviderNativeIdentityRequest {
        namespace: operation.source().source_id().clone(),
        provider_instrument_id: ProviderInstrumentId::try_from(native_uuid.to_string())
            .map_err(|_| ServiceError::Unavailable)?,
        instrument: before.definition().instrument_id(),
        venue: VenueId::try_from("iex").map_err(|_| ServiceError::Unavailable)?,
        venue_symbol: VenueSymbol::try_from(native_symbol)
            .map_err(|_| ServiceError::Unavailable)?,
        knowledge_at: cutoff,
        effective_at: cutoff,
    };
    Ok(AlpacaNativeAssetRoute {
        before,
        after,
        native,
    })
}
