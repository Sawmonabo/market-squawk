//! Asynchronous FRED seal-first publication used by startup composition.

use std::{sync::Arc, time::Instant};

use market_squawk_domain::SourceIdentifier;
use market_squawk_services::{
    JsonContractError, JsonStructureLimits, RequestContext, RequestId, ServiceLimits,
    ServiceLimitsError,
};
use market_squawk_sources::FRED_ALFRED_API_SURFACE_ID;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::ResearchService;
use crate::application::{FredPublishedGenerationHandoff, ProductionResearchIngestCoordinator};

/// Reopens the latest exact FRED generation without provider reacquisition.
///
/// `Ok(None)` means the configured analytical dataset has never been published. Present but
/// invalid durable evidence is an error and cannot fall through to another manifest or a live
/// append path.
pub(crate) fn reopen_fred_latest_known(
    research: &ResearchService,
    provider_dataset: SourceIdentifier,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Option<FredPublishedGenerationHandoff>, FredPublicationActivationError> {
    FredPublishedGenerationHandoff::try_reopen_latest(
        research,
        provider_dataset,
        deadline,
        &cancellation,
    )
    .map_err(|error| FredPublicationActivationError::ExistingPublication(error.to_string()))
}

/// Acquires, seals, publishes, and restart-verifies the exact configured FRED dataset.
pub(crate) async fn publish_fred_latest_known(
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    provider_dataset: SourceIdentifier,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<FredPublishedGenerationHandoff, FredPublicationActivationError> {
    let profile = SourceIdentifier::try_from(FRED_ALFRED_API_SURFACE_ID)
        .map_err(|_| FredPublicationActivationError::InvalidCodeOwnedIdentity)?;
    let context = RequestContext::new(
        RequestId::String(Arc::from("startup.fred.latest-known-publication")),
        cancellation,
        deadline,
        startup_limits()?,
    );
    let sealed = coordinator
        .acquire_and_seal_fred_dataset(&profile, &provider_dataset, &context)
        .await
        .map_err(|error| FredPublicationActivationError::Publication(error.to_string()))?;
    coordinator
        .publish_sealed_fred_dataset(sealed, &context)
        .await
        .map_err(|error| FredPublicationActivationError::Publication(error.to_string()))
}

fn startup_limits() -> Result<ServiceLimits, FredPublicationActivationError> {
    let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 4096, 4096)?;
    ServiceLimits::try_new(64 * 1024, 32, 1024 * 1024, 1024, structure).map_err(Into::into)
}

/// Failure before a restart-verified FRED publication can reach the typed operation.
#[derive(Debug, Error)]
pub(crate) enum FredPublicationActivationError {
    /// A code-owned profile identifier could not be constructed.
    #[error("the code-owned FRED profile identity is invalid")]
    InvalidCodeOwnedIdentity,
    /// Startup request limits are invalid.
    #[error("the FRED startup publication limits are invalid")]
    Limits(#[from] ServiceLimitsError),
    /// Startup JSON structure limits are invalid.
    #[error("the FRED startup publication structure limits are invalid")]
    Structure(#[from] JsonContractError),
    /// A present immutable generation or provider binding did not reopen exactly.
    #[error("an existing FRED generation failed exact reopening: {0}")]
    ExistingPublication(String),
    /// Seal-first acquisition, publication, or restart verification failed.
    #[error("FRED startup publication failed: {0}")]
    Publication(String),
}
