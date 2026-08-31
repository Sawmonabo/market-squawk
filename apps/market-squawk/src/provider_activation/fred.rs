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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, ffi::OsString, time::Duration};

    use chrono::Utc;
    use market_squawk_domain::{CalendarDate, Timestamp};
    use market_squawk_platform::{ConfigOverrides, ConfigSources, SecretValue};
    use serde_json::Value;

    use super::*;
    use crate::{AppConfig, LocalProduct, ProviderPortalActivationRequest, StartOnboardingRequest};

    const FRED_CURRENT_DATASET: &str = "fred:series-observations:UNRATE:1776-07-04:9999-12-31";
    const ALFRED_VINTAGE_DATASET: &str = "alfred:series-observations:UNRATE:2025-01-01:2026-08-31";
    const LIVE_JOURNEY_TIMEOUT: Duration = Duration::from_secs(5 * 60);

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    /// Credential-gated production proof for the two FRED namespaces retained by one adapter.
    ///
    /// This remains ignored so routine tests and CI never contact the provider. Run it explicitly
    /// with `FRED_API_KEY` supplied by the owner-managed credential environment.
    #[tokio::test]
    #[ignore = "requires an explicit live FRED credential and external-network authorization"]
    async fn live_fred_and_alfred_publish_read_and_reopen() -> TestResult {
        let api_key = env::var("FRED_API_KEY")?;
        for provider_dataset in [FRED_CURRENT_DATASET, ALFRED_VINTAGE_DATASET] {
            exercise_live_dataset(provider_dataset, &api_key).await?;
        }
        Ok(())
    }

    async fn exercise_live_dataset(provider_dataset: &str, api_key: &str) -> TestResult {
        let temporary = tempfile::tempdir()?;
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?;
        let dataset = SourceIdentifier::try_from(provider_dataset)?;
        let product = LocalProduct::try_new(config.clone())?;
        let onboarding = product.provider_onboarding();
        onboarding
            .unlock_encrypted_file_fallback(
                SecretValue::new("fred-live-journey-local-store".to_owned())?,
                CancellationToken::new(),
            )
            .await?;
        let started = onboarding
            .start(
                StartOnboardingRequest::try_new(FRED_ALFRED_API_SURFACE_ID, None, None)?,
                CancellationToken::new(),
            )
            .await?;
        let imported = onboarding
            .submit_secret(
                started.session_id(),
                SecretValue::new(api_key.to_owned())?,
                CancellationToken::new(),
            )
            .await?;
        if !imported.credential_stored() {
            return Err("FRED credential was not retained by the protected store".into());
        }
        product
            .provider_portal_activation()
            .activate(
                imported.session_id(),
                ProviderPortalActivationRequest::FredAlfred {
                    provider_dataset: dataset.clone(),
                },
                CancellationToken::new(),
            )
            .await?;
        let deadline = Instant::now() + LIVE_JOURNEY_TIMEOUT;
        let cancellation = CancellationToken::new();
        let context = RequestContext::new(
            RequestId::String(Arc::from("test.live.fred.publish-read-reopen")),
            cancellation.clone(),
            deadline,
            startup_limits()?,
        );
        let profile = SourceIdentifier::try_from(FRED_ALFRED_API_SURFACE_ID)?;
        let coordinator = product.research_ingest();
        let sealed = coordinator
            .acquire_and_seal_fred_dataset(&profile, &dataset, &context)
            .await?;
        let provider_rows = sealed.provider_row_count();
        let page_count = sealed.page_count();
        if provider_rows == 0 || page_count == 0 {
            return Err("live FRED acquisition returned no durable rows".into());
        }
        let published = coordinator
            .publish_sealed_fred_dataset(sealed, &context)
            .await?;
        let manifest = published.manifest().clone();
        let durable_bindings = published
            .restart_selector()
            .verify(product.research().as_ref())?;
        if durable_bindings.len() != page_count {
            return Err("published FRED page count changed during durable verification".into());
        }
        let (capability, generation) = published.into_operation_parts();
        let pinned = capability.try_pin_generation(&generation)?;
        if pinned != manifest {
            return Err("published FRED generation did not pin its exact manifest".into());
        }
        let knowledge_cutoff = now_timestamp()?;
        let effective_cutoff = CalendarDate::new(2026, 7, 1)?;
        let first_read = capability
            .read_latest_known(
                pinned,
                knowledge_cutoff,
                effective_cutoff,
                knowledge_cutoff,
                deadline,
                cancellation.clone(),
            )
            .await?;
        let first_read = serde_json::to_value(first_read)?;
        assert_latest_unemployment_read(&first_read, provider_dataset)?;

        if !product
            .application()
            .shutdown(Instant::now() + Duration::from_secs(10))
            .await
            .is_complete()
        {
            return Err("FRED live product did not shut down completely".into());
        }
        drop(product);

        let reopened_product = LocalProduct::try_new(config)?;
        let reopened = reopen_fred_latest_known(
            reopened_product.research().as_ref(),
            dataset,
            Instant::now() + LIVE_JOURNEY_TIMEOUT,
            CancellationToken::new(),
        )?
        .ok_or("durable FRED generation was absent after reopen")?;
        if reopened.manifest() != &manifest
            || reopened
                .restart_selector()
                .verify(reopened_product.research().as_ref())?
                .len()
                != page_count
        {
            return Err("durable FRED generation changed after reopen".into());
        }
        let (reopened_capability, reopened_generation) = reopened.into_operation_parts();
        let reopened_read = reopened_capability
            .read_latest_known(
                reopened_capability.try_pin_generation(&reopened_generation)?,
                knowledge_cutoff,
                effective_cutoff,
                knowledge_cutoff,
                Instant::now() + LIVE_JOURNEY_TIMEOUT,
                CancellationToken::new(),
            )
            .await?;
        let reopened_read = serde_json::to_value(reopened_read)?;
        if reopened_read != first_read {
            return Err("typed FRED point-in-time result changed after reopen".into());
        }
        eprintln!(
            "live durable macro proof: dataset={provider_dataset} rows={provider_rows} pages={page_count} manifest_version={}",
            manifest.manifest_version()
        );
        if !reopened_product
            .application()
            .shutdown(Instant::now() + Duration::from_secs(10))
            .await
            .is_complete()
        {
            return Err("reopened FRED product did not shut down completely".into());
        }
        Ok(())
    }

    fn assert_latest_unemployment_read(read: &Value, provider_dataset: &str) -> TestResult {
        let binding = read
            .pointer("/binding/provider/providerDatasetId")
            .and_then(Value::as_str);
        let series = read
            .pointer("/observation/seriesId")
            .and_then(Value::as_str);
        let effective = read
            .pointer("/observation/effectiveDate")
            .and_then(Value::as_str);
        let value = read
            .pointer("/observation/value/decimal")
            .and_then(Value::as_str);
        if binding != Some(provider_dataset)
            || series != Some("UNRATE")
            || effective != Some("2026-07-01")
            || value != Some("4.1")
            || read.pointer("/selection/complete").and_then(Value::as_bool) != Some(true)
        {
            return Err(
                "typed FRED point-in-time read did not match the admitted UNRATE fact".into(),
            );
        }
        Ok(())
    }

    fn now_timestamp() -> TestResult<Timestamp> {
        let nanos = Utc::now()
            .timestamp_nanos_opt()
            .ok_or("current time is outside the application timestamp range")?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }
}
