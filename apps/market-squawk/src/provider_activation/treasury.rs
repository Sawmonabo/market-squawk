//! Asynchronous Treasury seal-first publication used by startup composition.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_treasury::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasurySurface,
};
use market_squawk_domain::SourceIdentifier;
use market_squawk_services::{
    JsonContractError, JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
    ServiceLimitsError,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::application::{
    ResearchProviderRuntimeGeneration, TreasuryApplicationClosure, TreasuryMacroPublicationReceipt,
};

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
    validate_configured_datasets(surface, &provider_datasets, true)?;
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
        ensure_startup_live(deadline, &cancellation)?;
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
                TreasuryPublicationActivationError::ExistingPublication(Box::new(error))
            })?;
        if let Some(receipt) = receipt {
            receipts.push(receipt);
        } else {
            missing.push(provider_dataset);
        }
    }
    ensure_startup_live(deadline, &cancellation)?;
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
    closure: Arc<TreasuryApplicationClosure>,
    surface: TreasurySurface,
    provider_datasets: Vec<SourceIdentifier>,
    generation: ResearchProviderRuntimeGeneration,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Vec<TreasuryMacroPublicationReceipt>, TreasuryPublicationActivationError> {
    validate_configured_datasets(surface, &provider_datasets, false)?;
    let configured_dataset_count = provider_datasets.len();
    let context = RequestContext::new(
        RequestId::String(Arc::from(match surface {
            TreasurySurface::FiscalData => "startup.treasury-fiscal.latest-known-publication",
            TreasurySurface::DailyRatesXml => "startup.treasury-daily.latest-known-publication",
        })),
        cancellation,
        deadline,
        startup_limits()?,
    );
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(configured_dataset_count)
        .map_err(|_error| TreasuryPublicationActivationError::InvalidConfiguredDatasets)?;
    for provider_dataset in provider_datasets {
        ensure_startup_live(deadline, context.cancellation())?;
        let receipt = loop {
            ensure_startup_live(deadline, context.cancellation())?;
            if let Some(receipt) = closure
                .publish_all_history(surface, &generation, &provider_dataset, &context)
                .await
                .map_err(|error| TreasuryPublicationActivationError::Publication(Box::new(error)))?
            {
                break receipt;
            }
        };
        receipts.push(receipt);
    }
    ensure_startup_live(deadline, context.cancellation())?;
    if receipts.len() != configured_dataset_count {
        return Err(TreasuryPublicationActivationError::IncompletePublication);
    }
    Ok(receipts)
}

fn ensure_startup_live(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), TreasuryPublicationActivationError> {
    if cancellation.is_cancelled() {
        return Err(TreasuryPublicationActivationError::Publication(Box::new(
            ServiceError::Cancelled,
        )));
    }
    if Instant::now() >= deadline {
        return Err(TreasuryPublicationActivationError::ExistingPublicationUnavailable);
    }
    Ok(())
}

fn validate_configured_datasets(
    surface: TreasurySurface,
    provider_datasets: &[SourceIdentifier],
    require_complete: bool,
) -> Result<(), TreasuryPublicationActivationError> {
    let expected = match surface {
        TreasurySurface::FiscalData => vec![
            SourceIdentifier::try_from("treasury:fiscal-data:average-interest-rates-v2:all")
                .map_err(|_| TreasuryPublicationActivationError::InvalidCodeOwnedIdentity)?,
        ],
        TreasurySurface::DailyRatesXml => TreasuryDailyRateFamily::ALL
            .into_iter()
            .map(|family| {
                TreasuryDailyRateQuery::all_history(family)
                    .map(|query| query.dataset().clone())
                    .map_err(|_| TreasuryPublicationActivationError::InvalidCodeOwnedIdentity)
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    if provider_datasets.is_empty()
        || (require_complete && provider_datasets.len() != expected.len())
        || provider_datasets
            .iter()
            .enumerate()
            .any(|(ordinal, dataset)| {
                !expected.contains(dataset) || provider_datasets[..ordinal].contains(dataset)
            })
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
    #[error("Treasury publication did not return the complete configured dataset set")]
    IncompletePublication,
    #[error("an existing Treasury generation is unavailable before exact reopening completes")]
    ExistingPublicationUnavailable,
    #[error("an existing Treasury generation failed exact reopening: {0}")]
    ExistingPublication(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("the Treasury startup publication limits are invalid")]
    Limits(#[from] ServiceLimitsError),
    #[error("the Treasury startup publication structure limits are invalid")]
    Structure(#[from] JsonContractError),
    #[error("Treasury startup publication failed: {0}")]
    Publication(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl TreasuryPublicationActivationError {
    pub(crate) fn is_terminal_cancellation(&self) -> bool {
        match self {
            Self::Publication(error) | Self::ExistingPublication(error) => {
                matches!(
                    error.downcast_ref::<ServiceError>(),
                    Some(ServiceError::Cancelled)
                ) || TreasuryApplicationClosure::is_terminal_cancellation(error.as_ref())
            }
            _ => false,
        }
    }
}
