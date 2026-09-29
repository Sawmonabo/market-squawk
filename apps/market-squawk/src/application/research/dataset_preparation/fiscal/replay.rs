//! Original source/model reconstruction for retained historical fiscal inference.
use super::*;
use crate::application::{
    InstrumentContextReadCapability,
    model::{
        ForecastStudyRuntimeReference, SelectedForecastRuntime, runtime::ProductionModelRuntime,
    },
    research::prepare_fixed_current_population,
};
use market_squawk_data::{
    AnalyticalFeatureDataset, DatasetBuildSpecDigest, FeatureDatasetInputCoordinate,
    FeatureDatasetInputEpochOutput, QueryLimits,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Inert immutable-generation coordinates. Only the catalog's exact source read admits them.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DatasetReference {
    dataset_id: String,
    build_spec: [u8; 32],
    manifest_version: u64,
    manifest_hash: [u8; 32],
}
impl DatasetReference {
    fn from_source(dataset: &AnalyticalFeatureDataset) -> Result<Self, ServiceError> {
        let generation = dataset.generation();
        let manifest = generation.manifest();
        Ok(Self {
            dataset_id: manifest.dataset_id().as_str().to_owned(),
            build_spec: generation
                .build_spec_digest()
                .ok_or(ServiceError::InvalidResult)?
                .digest()
                .bytes(),
            manifest_version: manifest.manifest_version(),
            manifest_hash: manifest.content_hash().bytes(),
        })
    }
    async fn read(
        &self,
        research: &crate::ResearchService,
        contract: FeatureDatasetProductContract,
        context: &RequestContext,
    ) -> Result<FeatureDatasetInputEpochOutput, ServiceError> {
        ensure_request(context)?;
        let dataset_id = DatasetId::try_from(self.dataset_id.as_str())
            .map_err(|_| ServiceError::InvalidRequest)?;
        let build = DatasetBuildSpecDigest::try_new(self.build_spec)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let reader = research.analytical_reader();
        let dataset = reader
            .feature_dataset_for_build(
                contract,
                &dataset_id,
                build,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::super::super::map_read_error)?
            .ok_or(ServiceError::Unavailable)?;
        let manifest = dataset.generation().manifest();
        if manifest.manifest_version() != self.manifest_version
            || manifest.content_hash().bytes() != self.manifest_hash
        {
            return Err(ServiceError::InvalidResult);
        }
        let limits = QueryLimits::try_new_with_inline_bytes(
            32768,
            32 * 1024 * 1024,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
            1,
            128,
            128,
            Duration::from_secs(30),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        reader
            .feature_dataset_input_epochs(
                contract,
                manifest,
                limits,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(super::super::super::map_read_error)
    }
}

/// Every field is a replay recipe; none is authority or a deserialized monetary amount.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalFiscalForecastReference {
    target_id: String,
    price_inputs: DatasetReference,
    price_example_id: String,
    price_origin_identity: [u8; 32],
    financial_inputs: DatasetReference,
    runtime: ForecastStudyRuntimeReference,
    distribution_identity: [u8; 32],
}
impl HistoricalFiscalForecastReference {
    pub(super) fn from_source(
        target_id: String,
        price: FeatureDatasetInputCoordinate<'_>,
        financial: &FeatureDatasetInputEpochOutput,
        runtime: &SelectedForecastRuntime,
        distribution: Sha256Digest,
        origin: Sha256Digest,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            target_id,
            price_inputs: DatasetReference::from_source(price.dataset())?,
            price_example_id: price.epoch().example_id().to_owned(),
            price_origin_identity: origin.bytes(),
            financial_inputs: DatasetReference::from_source(financial.dataset())?,
            runtime: runtime.reference().clone(),
            distribution_identity: distribution.bytes(),
        })
    }
}

/// Existing owners only. Restart reopens exact datasets and model, then recomputes native inference.
pub(crate) struct HistoricalFiscalForecastReadCapability {
    research: Arc<crate::ResearchService>,
    datasets: Arc<DatasetPreparationAuthority>,
    identities: Arc<InstrumentContextReadCapability>,
    runtime: Arc<ProductionModelRuntime>,
    artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
}
impl HistoricalFiscalForecastReadCapability {
    pub(crate) fn new(
        research: Arc<crate::ResearchService>,
        datasets: Arc<DatasetPreparationAuthority>,
        identities: Arc<InstrumentContextReadCapability>,
        runtime: Arc<ProductionModelRuntime>,
        artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
    ) -> Self {
        Self {
            research,
            datasets,
            identities,
            runtime,
            artifacts,
        }
    }
}

/// A failed actual source preparation, retained as an original replay recipe rather than an
/// invented monetary forecast. Reopening must independently reproduce source unavailability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalFiscalUnavailableReference {
    target_id: String,
    price_inputs: DatasetReference,
    price_example_id: String,
    price_origin_identity: [u8; 32],
}
impl HistoricalFiscalUnavailableReference {
    pub(crate) fn from_source(
        target_id: &str,
        price: FeatureDatasetInputCoordinate<'_>,
        error: ServiceError,
    ) -> Result<Self, ServiceError> {
        if !matches!(error, ServiceError::Unavailable | ServiceError::NotFound)
            || !crate::application::research::fiscal_projection::fiscal_projection_targets()
                .iter()
                .any(|target| target.target_id == target_id)
        {
            return Err(error);
        }
        Ok(Self {
            target_id: target_id.to_owned(),
            price_inputs: DatasetReference::from_source(price.dataset())?,
            price_example_id: price.epoch().example_id().to_owned(),
            price_origin_identity: Sha256::digest(
                price
                    .epoch()
                    .canonical_bytes()
                    .map_err(map_fiscal_build_error)?,
            )
            .into(),
        })
    }
    pub(crate) fn origin_identity(&self) -> [u8; 32] {
        self.price_origin_identity
    }
    pub(crate) fn target_id(&self) -> &str {
        &self.target_id
    }
}
impl HistoricalFiscalForecastReference {
    pub(crate) fn target_id(&self) -> &str {
        &self.target_id
    }
}

mod artifact;
pub(crate) use artifact::{
    HISTORICAL_FISCAL_MAXIMUM_ORIGINS, HISTORICAL_FISCAL_MAXIMUM_PAGES,
    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
    HISTORICAL_FISCAL_PAGE_SIZE, HistoricalFiscalCompletedJobs, HistoricalFiscalJobReference,
    HistoricalFiscalOriginDescriptor, HistoricalFiscalPageDescriptor,
    HistoricalFiscalPageReference, HistoricalFiscalRecipeReference,
    HistoricalFiscalSourceSelection, HistoricalFiscalStudyBinding,
};
