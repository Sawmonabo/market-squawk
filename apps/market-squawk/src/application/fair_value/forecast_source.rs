//! Installed forecast valuation source reconstruction using existing model and data authorities.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_data::{
    AnalyticalDataService, AuthorizedResearchUse, ResearchUse, ResearchUseLimits,
    ResearchUseRequest,
};
use market_squawk_domain::Timestamp;
use market_squawk_services::{ArtifactReadContext, ServiceError};
use market_squawk_valuation::{
    FairValueError, ForecastValuationReference, ForecastValuationResolver, ForecastValuationSource,
};

use crate::{
    ResearchService,
    application::{
        model::{
            ForecastApplicationError, ModelDomainService, map_forecast_selection_error,
            forecast::{ForecastEvidenceReadContext, ForecastEvidenceReader, LatestValidForecast},
            forecast_preparation::ForecastEvidenceReadError,
        },
        research::reopen_forecast_serving_output,
    },
};

const MAXIMUM_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;

fn map_source_service_error(error: ServiceError) -> FairValueError {
    match error {
        ServiceError::Cancelled => FairValueError::Cancelled,
        ServiceError::DeadlineExceeded => FairValueError::DeadlineExceeded,
        ServiceError::ResourceExhausted => FairValueError::ResourceExhausted,
        ServiceError::InvalidResult | ServiceError::Internal => FairValueError::CorruptPersistence,
        // A missing, invalid or unauthorized original source cannot supply producer evidence.
        ServiceError::InvalidRequest | ServiceError::NotFound | ServiceError::Unauthorized => {
            FairValueError::InvalidProducerEvidence
        }
        ServiceError::Unavailable => FairValueError::Persistence,
    }
}

fn map_model_source_error(error: ForecastApplicationError) -> FairValueError {
    map_source_service_error(map_forecast_selection_error(error))
}

fn map_serving_read_error(error: ForecastEvidenceReadError) -> FairValueError {
    match error {
        ForecastEvidenceReadError::Cancelled => FairValueError::Cancelled,
        ForecastEvidenceReadError::DeadlineExceeded => FairValueError::DeadlineExceeded,
        ForecastEvidenceReadError::Capacity => FairValueError::ResourceExhausted,
        ForecastEvidenceReadError::InvalidEvidence => FairValueError::CorruptPersistence,
        ForecastEvidenceReadError::Unavailable => FairValueError::Persistence,
    }
}

/// References the installed authorities without adding a runtime, registry, or retained cache.
#[derive(Clone)]
pub(crate) struct ForecastValuationSourceFactory {
    model: Arc<ModelDomainService>,
    research: Arc<ResearchService>,
}

impl ForecastValuationSourceFactory {
    pub(crate) fn new(model: Arc<ModelDomainService>, research: Arc<ResearchService>) -> Self {
        Self { model, research }
    }

    /// Creates request-scoped recovery using the existing operation cancellation and deadline.
    pub(crate) fn resolver(
        &self,
        context: ArtifactReadContext,
    ) -> InstalledForecastValuationResolver<'_> {
        Self::borrowed_resolver(&self.model, self.research.analytical(), context)
    }

    /// Uses the exact staged authorities during restore without reopening another catalog.
    pub(crate) fn borrowed_resolver<'a>(
        model: &'a ModelDomainService,
        analytical: &'a AnalyticalDataService,
        context: ArtifactReadContext,
    ) -> InstalledForecastValuationResolver<'a> {
        InstalledForecastValuationResolver {
            model,
            analytical,
            context,
        }
    }

    /// Reopens the exact original serving selection for an already authenticated selected forecast.
    pub(crate) async fn source_for_selected_forecast(
        &self,
        selected: &LatestValidForecast,
        context: &ArtifactReadContext,
    ) -> Result<ForecastValuationSource, FairValueError> {
        source_for_selected_forecast(self.research.analytical(), selected, context).await
    }
}

async fn source_for_selected_forecast(
    analytical: &AnalyticalDataService,
    selected: &LatestValidForecast,
    context: &ArtifactReadContext,
) -> Result<ForecastValuationSource, FairValueError> {
    context
        .ensure_live()
        .map_err(|error| map_model_source_error(ForecastApplicationError::Artifact(error)))?;
    let distribution = selected
        .selected_distribution()
        .ok_or(FairValueError::InvalidProducerEvidence)?;
    let serving = distribution.serving_binding();
    if serving.financial_epoch().is_some() {
        return ForecastValuationSource::try_from_financial_distribution(
            distribution.native_output().clone(),
            serving,
            serving.knowledge_cutoff(),
            Timestamp::from_unix_nanos(selected.selection_receipt().as_of_unix_nanos()),
        );
    }
    if serving.current_price_epoch().is_some() {
        return ForecastValuationSource::try_from_current_price_distribution(
            distribution.native_output().clone(),
            serving,
            serving.knowledge_cutoff(),
            Timestamp::from_unix_nanos(selected.selection_receipt().as_of_unix_nanos()),
        );
    }
    let output = reopen_forecast_serving_output(
        &analytical.analytical_reader(),
        serving,
        distribution.native_output().instrument_id(),
        context.deadline(),
        context.cancellation().clone(),
    )
    .await
    .map_err(map_serving_read_error)?;
    context
        .ensure_live()
        .map_err(|error| map_model_source_error(ForecastApplicationError::Artifact(error)))?;
    ForecastValuationSource::try_from_distribution(
        distribution.native_output().clone(),
        serving,
        &output,
        serving.knowledge_cutoff(),
        Timestamp::from_unix_nanos(selected.selection_receipt().as_of_unix_nanos()),
    )
}

/// Exact immutable forecast recovery scoped to one live installed operation.
pub(crate) struct InstalledForecastValuationResolver<'a> {
    model: &'a ModelDomainService,
    analytical: &'a AnalyticalDataService,
    context: ArtifactReadContext,
}

impl ForecastValuationResolver for InstalledForecastValuationResolver<'_> {
    fn resolve<'a>(
        &'a self,
        reference: &'a ForecastValuationReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (ForecastValuationSource, AuthorizedResearchUse),
                        FairValueError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.context
                .ensure_live()
                .map_err(|error| map_model_source_error(ForecastApplicationError::Artifact(error)))?;
            let selected = self
                .model
                .exact_distribution_for_identity(
                    reference.vintage_id(),
                    reference.instrument_id(),
                    reference.selected_at(),
                    ForecastEvidenceReadContext::new(
                        self.context.clone(),
                        NonZeroUsize::new(MAXIMUM_ARTIFACT_BYTES)
                            .ok_or(FairValueError::Arithmetic)?,
                    ),
                )
                .await
                .map_err(map_model_source_error)?;
            let source =
                source_for_selected_forecast(self.analytical, &selected, &self.context).await?;
            if source.reference() != reference {
                return Err(FairValueError::CorruptPersistence);
            }
            self.context
                .ensure_live()
                .map_err(|error| map_model_source_error(ForecastApplicationError::Artifact(error)))?;
            let remaining = self
                .context
                .deadline()
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5));
            if remaining.is_zero() {
                self.context
                    .ensure_live()
                    .map_err(|error| {
                        map_model_source_error(ForecastApplicationError::Artifact(error))
                    })?;
                return Err(FairValueError::DeadlineExceeded);
            }
            let limits = ResearchUseLimits::try_new(
                64,
                4096,
                8192,
                4096,
                4 * 1024 * 1024,
                remaining,
                Duration::from_secs(300),
            )
            .map_err(|_| FairValueError::Arithmetic)?;
            let authorization = self
                .analytical
                .authorize_research_use(
                    ResearchUseRequest::try_new(
                        reference.parent_manifests().to_vec(),
                        ResearchUse::LocalAnalysis,
                        limits,
                    )
                    .map_err(|error| {
                        map_source_service_error(
                            crate::application::research::map_research_use_error(error),
                        )
                    })?,
                    self.context.cancellation(),
                )
                .map_err(|error| {
                    map_source_service_error(
                        crate::application::research::map_research_use_error(error),
                    )
                })?;
            self.context
                .ensure_live()
                .map_err(|error| map_model_source_error(ForecastApplicationError::Artifact(error)))?;
            // The recovery owner checks exact graph membership and consumes this one-use permit.
            Ok((source, authorization))
        })
    }
}
