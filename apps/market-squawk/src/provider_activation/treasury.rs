//! Asynchronous Treasury seal-first publication used by startup composition.

use std::{num::NonZeroU16, sync::Arc, time::Instant};

use market_squawk_adapter_treasury::TreasurySurface;
use market_squawk_domain::SourceIdentifier;
use market_squawk_services::{
    JsonContractError, JsonStructureLimits, RequestContext, RequestId, ServiceLimits,
    ServiceLimitsError,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::application::{
    ProductionResearchIngestCoordinator, ResearchProviderRuntimeGeneration,
    TreasuryApplicationClosure, TreasuryMacroPublicationReceipt, TreasurySelectedObjectRequest,
};

const MAXIMUM_DISCOVERY_OBJECTS: u16 = 64;

/// Closed restart result for one exact configured Treasury surface.
#[derive(Debug)]
pub(crate) enum TreasuryDurableRecovery {
    /// Every configured dataset reopened from its latest exact durable generation.
    Complete {
        /// Restart-verified receipts for the complete configured dataset set.
        receipts: Vec<TreasuryMacroPublicationReceipt>,
    },
    /// Only the listed datasets lack a durable generation; existing receipts remain exact.
    Missing {
        /// Restart-verified receipts that must be preserved without reacquisition.
        existing_receipts: Vec<TreasuryMacroPublicationReceipt>,
        /// Exact configured provider datasets that alone may enter first publication.
        provider_datasets: Vec<SourceIdentifier>,
    },
}

/// Reopens every configured Treasury dataset from its latest durable exact generation.
///
/// Missing datasets are returned separately from exact existing receipts. Invalid
/// manifest/raw/native evidence is an error and must remain unavailable rather than falling
/// through to reacquisition or another generation.
pub(crate) async fn reopen_treasury_latest_known(
    closure: Arc<TreasuryApplicationClosure>,
    surface: TreasurySurface,
    provider_datasets: Vec<SourceIdentifier>,
    generation: ResearchProviderRuntimeGeneration,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<TreasuryDurableRecovery, TreasuryPublicationActivationError> {
    validate_configured_datasets(&provider_datasets)?;
    let configured_dataset_count = provider_datasets.len();
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(provider_datasets.len())
        .map_err(|_error| TreasuryPublicationActivationError::InvalidConfiguredDatasets)?;
    let mut missing = Vec::new();
    missing
        .try_reserve_exact(provider_datasets.len())
        .map_err(|_error| TreasuryPublicationActivationError::InvalidConfiguredDatasets)?;
    for provider_dataset in provider_datasets {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(TreasuryPublicationActivationError::ExistingPublicationUnavailable);
        }
        let receipt = closure
            .reopen_latest_published(
                surface,
                &provider_dataset,
                &generation,
                deadline,
                &cancellation,
            )
            .await
            .map_err(|error| {
                TreasuryPublicationActivationError::ExistingPublication(error.to_string())
            })?;
        if let Some(receipt) = receipt {
            receipts.push(receipt);
        } else {
            missing.push(provider_dataset);
        }
    }
    if missing.is_empty() {
        if receipts.len() != configured_dataset_count {
            return Err(TreasuryPublicationActivationError::ExistingPublicationUnavailable);
        }
        return Ok(TreasuryDurableRecovery::Complete { receipts });
    }
    if receipts
        .len()
        .checked_add(missing.len())
        .filter(|count| *count == configured_dataset_count)
        .is_none()
    {
        return Err(TreasuryPublicationActivationError::ExistingPublicationUnavailable);
    }
    Ok(TreasuryDurableRecovery::Missing {
        existing_receipts: receipts,
        provider_datasets: missing,
    })
}

/// Publishes every exact configured dataset and returns only restart-verified receipts.
pub(crate) async fn publish_treasury_latest_known(
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    closure: Arc<TreasuryApplicationClosure>,
    surface: TreasurySurface,
    provider_datasets: Vec<SourceIdentifier>,
    generation: ResearchProviderRuntimeGeneration,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Vec<TreasuryMacroPublicationReceipt>, TreasuryPublicationActivationError> {
    validate_configured_datasets(&provider_datasets)?;
    let configured_dataset_count = provider_datasets.len();
    let profile = SourceIdentifier::try_from(surface.profile_id())
        .map_err(|_| TreasuryPublicationActivationError::InvalidCodeOwnedIdentity)?;
    let context = RequestContext::new(
        RequestId::String(Arc::from(match surface {
            TreasurySurface::FiscalData => "startup.treasury-fiscal.latest-known-publication",
            TreasurySurface::DailyRatesXml => "startup.treasury-daily.latest-known-publication",
        })),
        cancellation,
        deadline,
        startup_limits()?,
    );
    let maximum_objects = NonZeroU16::new(MAXIMUM_DISCOVERY_OBJECTS)
        .ok_or(TreasuryPublicationActivationError::InvalidCodeOwnedLimit)?;
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(configured_dataset_count)
        .map_err(|_error| TreasuryPublicationActivationError::InvalidConfiguredDatasets)?;
    for provider_dataset in provider_datasets {
        if surface == TreasurySurface::DailyRatesXml {
            let receipt = loop {
                if context.cancellation().is_cancelled() || Instant::now() >= deadline {
                    return Err(TreasuryPublicationActivationError::ExistingPublicationUnavailable);
                }
                if let Some(receipt) = closure
                    .publish_daily_rates_all_history(&generation, &provider_dataset, &context)
                    .await
                    .map_err(|error| {
                        TreasuryPublicationActivationError::Publication(error.to_string())
                    })?
                {
                    break receipt;
                }
            };
            receipts.push(receipt);
            continue;
        }
        let discovery = coordinator
            .discover_registered_objects(
                &profile,
                &provider_dataset,
                None,
                maximum_objects,
                &context,
            )
            .await
            .map_err(|error| TreasuryPublicationActivationError::Publication(error.to_string()))?;
        if discovery.objects().is_empty() {
            return Err(TreasuryPublicationActivationError::IncompleteDiscovery);
        }
        let mut latest_receipt = None;
        for object in discovery.objects() {
            let selected = TreasurySelectedObjectRequest::fiscal_data(
                provider_dataset.clone(),
                object.source_object().object_id().clone(),
                object.discovery_receipt().to_owned(),
            )
            .map_err(|error| TreasuryPublicationActivationError::Publication(error.to_string()))?;
            let sealed = closure
                .acquire_and_seal(selected, &context, deadline)
                .await
                .map_err(|error| {
                    TreasuryPublicationActivationError::Publication(error.to_string())
                })?;
            latest_receipt = Some(closure.publish(sealed, &context).await.map_err(|error| {
                TreasuryPublicationActivationError::Publication(error.to_string())
            })?);
        }
        receipts
            .push(latest_receipt.ok_or(TreasuryPublicationActivationError::IncompleteDiscovery)?);
    }
    if receipts.len() != configured_dataset_count {
        return Err(TreasuryPublicationActivationError::IncompleteDiscovery);
    }
    Ok(receipts)
}

fn validate_configured_datasets(
    provider_datasets: &[SourceIdentifier],
) -> Result<(), TreasuryPublicationActivationError> {
    if provider_datasets.is_empty()
        || provider_datasets.len() > 32
        || provider_datasets
            .iter()
            .enumerate()
            .any(|(ordinal, dataset)| provider_datasets[..ordinal].contains(dataset))
    {
        return Err(TreasuryPublicationActivationError::InvalidConfiguredDatasets);
    }
    Ok(())
}

fn startup_limits() -> Result<ServiceLimits, TreasuryPublicationActivationError> {
    let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 4096, 4096)?;
    ServiceLimits::try_new(64 * 1024, 32, 1024 * 1024, 1024, structure).map_err(Into::into)
}

/// Failure before restart-verified Treasury receipts reach a typed operation.
#[derive(Debug, Error)]
pub(crate) enum TreasuryPublicationActivationError {
    #[error("the configured Treasury dataset set is invalid")]
    InvalidConfiguredDatasets,
    #[error("the code-owned Treasury profile identity is invalid")]
    InvalidCodeOwnedIdentity,
    #[error("the code-owned Treasury discovery limit is invalid")]
    InvalidCodeOwnedLimit,
    #[error("Treasury discovery returned no complete publication object")]
    IncompleteDiscovery,
    #[error("an existing Treasury generation is unavailable before exact reopening completes")]
    ExistingPublicationUnavailable,
    #[error("an existing Treasury generation failed exact reopening: {0}")]
    ExistingPublication(String),
    #[error("the Treasury startup publication limits are invalid")]
    Limits(#[from] ServiceLimitsError),
    #[error("the Treasury startup publication structure limits are invalid")]
    Structure(#[from] JsonContractError),
    #[error("Treasury startup publication failed: {0}")]
    Publication(String),
}
