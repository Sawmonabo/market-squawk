//! Production forecast evidence from a separately admitted AnalysisV1 dataset paired to the
//! immutable TrainingV1 selection retained by model metadata.

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::{
    AnalyticalFeatureDataset, AnalyticalMarketBarReadLimit, AnalyticalMarketBarReadRequest,
    AnalyticalReadCapability, AnalyticalReadLimit, ComponentAdjustmentEvidence, ComponentKind,
    ComponentScope, ComponentValue, CorporateActionSensitivity, DatasetId, DatasetManifestRef,
    DatasetSplit, FeatureDatasetProductContract, ForecastDatasetEvidence,
    ForecastDatasetReadLimits, ForecastFeatureRow, ForecastFeatureRows, ForecastFeatureValue,
    GenerationParentRelation, MarketBarEffectiveRange, ObservationFamilyKey, QueryLimits,
    Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, ResearchTemporalCoordinate, Timestamp,
};
use market_squawk_modeling::{
    ForecastMeasurement, ForecastObservedPoint, ForecastTargetMeaning, ForecastValue,
    ModelMetadata, TrainingDatasetIdentity, has_price_return_macro_context_feature_order_v1,
};
use market_squawk_services::ServiceError;
use rust_decimal::{Decimal, prelude::ToPrimitive as _};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::model::forecast_preparation::{
    ForecastDatasetPairingReceipt, ForecastEvidenceCatalogRequest, ForecastEvidenceCatalogSnapshot,
    ForecastEvidenceDataset, ForecastEvidenceMaterializationRequest, ForecastEvidencePolicy,
    ForecastEvidenceReadError, ForecastEvidenceReader, ForecastEvidenceRevalidation,
    ForecastInstrumentAvailability, ForecastServingInputFence, PreparedForecastEvidence,
};

use super::macro_context::MacroContextReadCapability;
use super::macro_features::{MacroFeatureVector, MacroRateRegime, read_macro_feature_vector};

const DATASET_PAGE: usize = 64;
const MAX_DATASETS: usize = 4_096;
const MAX_BYTES: usize = 256 * 1024 * 1024;
const MAX_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;
const MINIMUM_HISTORY: usize = 3;
const FLOAT_RETURN_SCALE: u8 = 8;
const MAX_EVIDENCE_PAIRS: usize = 4_096;
const MAX_SERVING_BARS: u32 = 4_096;
const SERVING_QUERY_BYTES: u64 = 64 * 1024 * 1024;
const SERVING_QUERY_DURATION: Duration = Duration::from_secs(30);
const SERVING_LOOKBACK_NANOS: i64 = 10 * 366 * 24 * 60 * 60 * 1_000_000_000;

/// Analytical implementation of the model-owned forecast evidence contract.
#[derive(Clone)]
pub(crate) struct AnalyticalForecastEvidenceReader {
    analytical: AnalyticalReadCapability,
    macro_context: Option<MacroContextReadCapability>,
    calendar: Option<crate::application::market_calendar::CompletedMarketSessionReadCapability>,
}

impl AnalyticalForecastEvidenceReader {
    pub(crate) const fn new(
        analytical: AnalyticalReadCapability,
        macro_context: Option<MacroContextReadCapability>,
        calendar: Option<crate::application::market_calendar::CompletedMarketSessionReadCapability>,
    ) -> Self {
        Self {
            analytical,
            macro_context,
            calendar,
        }
    }

    async fn exact_training(
        &self,
        metadata: &ModelMetadata,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<ForecastDatasetEvidence>, ForecastEvidenceReadError> {
        let identity = metadata.dataset();
        let (training_contract, _) = dataset_contracts(metadata)?;
        let evidence = match self
            .analytical
            .forecast_dataset_evidence(
                training_contract,
                identity.manifest(),
                identity.selection_as_of(),
                evidence_limits()?,
                deadline,
                cancellation,
            )
            .await
        {
            Ok(evidence) => evidence,
            Err(market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable) => {
                return Ok(None);
            }
            Err(error) => return Err(map_read_error(error)),
        };
        let dataset = evidence.dataset();
        let generation = dataset.generation();
        let fence = evidence.fence();
        if dataset.product_contract() != training_contract
            || generation.manifest() != identity.manifest()
            || generation.build_spec_digest() != Some(identity.build_spec_digest())
            || dataset.universe_digest() != identity.universe_digest()
            || dataset.policy_digest() != identity.policy_digest()
            || dataset.universe_id() != metadata.universe_id()
            || fence.catalog_identity() != identity.catalog_identity()
            || fence.export_sha256() != identity.export_digest()
            || fence.selection_sha256() != identity.selection_digest()
            || fence.as_of() != identity.selection_as_of()
            || fence.selected_rows() != identity.selected_component_rows()
            || dataset.split_policy() != identity.split_policy()
            || dataset.study_policy() != identity.study_policy()
            || dataset.source_snapshot_digest() != identity.source_snapshot_digest()
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(Some(evidence))
    }

    async fn exact_analysis(
        &self,
        metadata: &ModelMetadata,
        manifest: &DatasetManifestRef,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<ForecastDatasetEvidence>, ForecastEvidenceReadError> {
        match self
            .analytical
            .forecast_dataset_evidence(
                dataset_contracts(metadata)?.1,
                manifest,
                as_of,
                evidence_limits()?,
                deadline,
                cancellation,
            )
            .await
        {
            Ok(evidence) => Ok(Some(evidence)),
            Err(market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable) => Ok(None),
            Err(error) => Err(map_read_error(error)),
        }
    }

    fn analysis_catalog(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<AnalyticalFeatureDataset>, ForecastEvidenceReadError> {
        let limit = AnalyticalReadLimit::try_new(DATASET_PAGE)
            .map_err(|_| ForecastEvidenceReadError::Capacity)?;
        let mut datasets = Vec::new();
        for contract in analysis_contracts() {
            let mut after: Option<DatasetId> = None;
            loop {
                check_control(deadline, cancellation)?;
                let page = self
                    .analytical
                    .feature_datasets(contract, after.as_ref(), limit, deadline, cancellation)
                    .map_err(map_read_error)?;
                if page.datasets().is_empty() {
                    if page.has_more() {
                        return Err(ForecastEvidenceReadError::InvalidEvidence);
                    }
                    break;
                }
                let retained_dataset_count = datasets
                    .len()
                    .checked_add(page.datasets().len())
                    .ok_or(ForecastEvidenceReadError::Capacity)?;
                if retained_dataset_count > MAX_DATASETS {
                    return Err(ForecastEvidenceReadError::Capacity);
                }
                datasets
                    .try_reserve_exact(page.datasets().len())
                    .map_err(|_| ForecastEvidenceReadError::Capacity)?;
                for dataset in page.datasets() {
                    datasets.push(dataset.clone());
                }
                after = page
                    .datasets()
                    .last()
                    .map(|dataset| dataset.generation().manifest().dataset_id().clone());
                if !page.has_more() {
                    break;
                }
            }
        }
        Ok(datasets)
    }
}

impl fmt::Debug for AnalyticalForecastEvidenceReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalForecastEvidenceReader")
            .field("analytical", &self.analytical)
            .field("pairing", &"[SEALED TRAINING_V1 -> ANALYSIS_V1]")
            .finish()
    }
}

#[async_trait]
impl ForecastEvidenceReader for AnalyticalForecastEvidenceReader {
    async fn financial_input(
        &self,
        manifest: &DatasetManifestRef,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_data::FeatureDatasetInputEpochOutput, ForecastEvidenceReadError> {
        crate::application::model::forecast::reopen_financial_input(
            &self.analytical,
            manifest,
            deadline,
            cancellation,
        )
        .await
        .map_err(map_serving_error)
    }

    async fn catalog(
        &self,
        request: ForecastEvidenceCatalogRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastEvidenceCatalogSnapshot, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        let analysis_catalog = self.analysis_catalog(deadline, &cancellation)?;
        let current_output = if let Some(input) = request.current_feature_input() {
            Some(
                crate::application::model::forecast::reopen_current_price_input(
                    &self.analytical,
                    input.manifest(),
                    deadline,
                    cancellation.child_token(),
                )
                .await
                .map_err(map_serving_error)?,
            )
        } else {
            None
        };
        let mut datasets = Vec::new();
        let maximum_pairs = request
            .models()
            .len()
            .checked_mul(analysis_catalog.len())
            .ok_or(ForecastEvidenceReadError::Capacity)?;
        if maximum_pairs > MAX_EVIDENCE_PAIRS {
            return Err(ForecastEvidenceReadError::Capacity);
        }
        datasets
            .try_reserve_exact(maximum_pairs)
            .map_err(|_| ForecastEvidenceReadError::Capacity)?;
        let mut retained_bytes = datasets
            .capacity()
            .checked_mul(std::mem::size_of::<ForecastEvidenceDataset>())
            .ok_or(ForecastEvidenceReadError::Capacity)?;
        if retained_bytes > MAX_BYTES {
            return Err(ForecastEvidenceReadError::Capacity);
        }
        for model in request.models() {
            check_control(deadline, &cancellation)?;
            if model.runtime_generation_sha256() != request.runtime_generation_sha256() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            let metadata = model.metadata();
            if matches!(
                metadata.output_binding().target(),
                ForecastTargetMeaning::FixedHorizonEvent { .. }
            ) && current_output.is_none()
            {
                continue;
            }
            if let (Some(output), Some(input)) = (&current_output, request.current_feature_input())
            {
                let index = crate::application::model::forecast::current_price_coordinate_index(
                    output,
                    input.example_id(),
                )
                .map_err(map_serving_error)?;
                let coordinate = output
                    .coordinate(index)
                    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
                let epoch = coordinate.epoch();
                let compatible = matches!(metadata.output_binding().target(), ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos, origin_basis } | ForecastTargetMeaning::FixedHorizonEvent { horizon_nanos, origin_basis, .. }
                    if Some(origin_basis) == epoch.fixed_horizon_origin_basis()
                    && epoch.target_at().zip(epoch.target_origin()).is_some_and(|(target, origin)| target.unix_nanos().checked_sub(origin.unix_nanos()) == i64::try_from(horizon_nanos.get()).ok()));
                if !compatible {
                    continue;
                }
            }
            let Some(horizon) = admitted_forecast_horizon(metadata) else {
                continue;
            };
            let Some(training) = self
                .exact_training(metadata, deadline, cancellation.child_token())
                .await?
            else {
                continue;
            };

            for candidate in analysis_catalog
                .iter()
                .filter(|candidate| pairable_summary(metadata, &training, candidate))
            {
                check_control(deadline, &cancellation)?;
                let Some(analysis) = self
                    .exact_analysis(
                        metadata,
                        candidate.generation().manifest(),
                        metadata.dataset().selection_as_of(),
                        deadline,
                        cancellation.child_token(),
                    )
                    .await?
                else {
                    continue;
                };
                let pairing = pair(metadata, &training, &analysis)?;
                let historical_instruments = instrument_inventory(
                    metadata,
                    analysis.rows(),
                    analysis.fence().as_of(),
                    horizon,
                    deadline,
                    &cancellation,
                )?;
                // OOS labels qualify the trained estimator; only genuine current sources supply catalog origins.
                let mut instruments = Vec::new();
                if let (Some(output), Some(input)) =
                    (&current_output, request.current_feature_input())
                {
                    let index =
                        crate::application::model::forecast::current_price_coordinate_index(
                            output,
                            input.example_id(),
                        )
                        .map_err(map_serving_error)?;
                    let coordinate = output
                        .coordinate(index)
                        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
                    if historical_instruments
                        .iter()
                        .any(|value| value.instrument_id() == coordinate.epoch().instrument_id())
                    {
                        let prepared = current_price_materialization(
                            metadata,
                            output,
                            index,
                            request.knowledge_cutoff(),
                            input.session_cohort(),
                        )?;
                        let calendar = self.calendar.clone().map(crate::application::market_calendar::ForecastSessionReadCapability::Current);
                        let origin =
                            crate::application::model::forecast::current_price_session_origin(
                                calendar.as_ref(),
                                input.session_cohort(),
                                coordinate,
                                deadline,
                                cancellation.child_token(),
                            )
                            .await
                            .map_err(map_serving_error)?;
                        let mut availability = ForecastInstrumentAvailability::try_new(
                            coordinate.epoch().instrument_id(),
                            prepared.observed_cutoff,
                            prepared.observed_cutoff,
                            prepared.available_at,
                            NonZeroUsize::MIN,
                            prepared.decimal_scale,
                        )?;
                        if let Some(origin) = origin {
                            availability = availability.with_session_origin(origin)?;
                        }
                        instruments.push(availability);
                    }
                } else {
                    for historical in historical_instruments {
                        match serving_materialization(
                            &self.analytical,
                            &analysis,
                            historical.instrument_id(),
                            request.knowledge_cutoff(),
                            None,
                            deadline,
                            cancellation.child_token(),
                        )
                        .await
                        {
                            Ok(current) => {
                                if current
                                    .observed_cutoff
                                    .checked_add_nanos(
                                        i64::try_from(horizon.get()).map_err(|_| {
                                            ForecastEvidenceReadError::InvalidEvidence
                                        })?,
                                    )
                                    .is_ok_and(|target| target > request.knowledge_cutoff())
                                {
                                    instruments.push(ForecastInstrumentAvailability::try_new(
                                        historical.instrument_id(),
                                        current
                                            .observed_history
                                            .first()
                                            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
                                            .observed_at(),
                                        current.observed_cutoff,
                                        current.available_at,
                                        NonZeroUsize::new(current.observed_history.len())
                                            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
                                        current.decimal_scale,
                                    )?);
                                }
                            }
                            Err(ForecastEvidenceReadError::Unavailable) => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
                if instruments.is_empty() {
                    continue;
                }
                let policy = ForecastEvidencePolicy::try_new(
                    NonZeroU16::MIN,
                    horizon,
                    NonZeroU64::new(MAX_VALIDITY_NANOS)
                        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
                    NonZeroUsize::new(if current_output.is_some() {
                        1
                    } else {
                        MINIMUM_HISTORY
                    })
                    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
                )?;
                let mut policies = Vec::new();
                policies
                    .try_reserve_exact(1)
                    .map_err(|_| ForecastEvidenceReadError::Capacity)?;
                policies.push(policy);
                let dataset = ForecastEvidenceDataset::try_new(
                    metadata.model_id(),
                    metadata.bundle_id().clone(),
                    metadata.bundle_version(),
                    pairing,
                    instruments,
                    policies,
                )?;
                let dataset_bytes = dataset.retained_dynamic_bytes()?;
                retained_bytes = retained_bytes
                    .checked_add(dataset_bytes)
                    .ok_or(ForecastEvidenceReadError::Capacity)?;
                if retained_bytes > MAX_BYTES {
                    return Err(ForecastEvidenceReadError::Capacity);
                }
                datasets.push(dataset);
            }
        }
        datasets.sort_unstable_by(|left, right| {
            left.model_id()
                .cmp(&right.model_id())
                .then_with(|| left.bundle_id().as_str().cmp(right.bundle_id().as_str()))
                .then_with(|| left.bundle_version().cmp(&right.bundle_version()))
                .then_with(|| {
                    left.analysis_manifest()
                        .dataset_id()
                        .as_str()
                        .cmp(right.analysis_manifest().dataset_id().as_str())
                })
                .then_with(|| {
                    left.analysis_manifest()
                        .manifest_version()
                        .cmp(&right.analysis_manifest().manifest_version())
                })
        });
        let mut authority = Sha256::new();
        authority.update(b"market-squawk/current-forecast-analysis-pairing-catalog/v2\0");
        authority.update(request.knowledge_cutoff().unix_nanos().to_be_bytes());
        if let Some(output) = &current_output {
            authority.update(output.query_output().result_digest().bytes());
        }
        if let Some(input) = request.current_feature_input() {
            hash_bytes(&mut authority, input.example_id().as_bytes())?;
            hash_bytes(
                &mut authority,
                &serde_json::to_vec(&input.session_cohort())
                    .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?,
            )?;
        }
        authority.update(request.runtime_generation_sha256().bytes());
        for dataset in &datasets {
            authority.update(dataset.pairing().pairing_sha256().bytes());
        }
        ForecastEvidenceCatalogSnapshot::try_new(
            Sha256Digest::new(authority.finalize().into()),
            datasets,
        )
    }

    async fn prepare(
        &self,
        request: ForecastEvidenceMaterializationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        if request.selection().dataset_manifest() != request.pairing().training().manifest()
            || request.selection().analysis_manifest()
                != request.pairing().analysis_fence().manifest()
            || request.pairing().training() != request.model().metadata().dataset()
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let training = self
            .exact_training(
                request.model().metadata(),
                deadline,
                cancellation.child_token(),
            )
            .await?
            .ok_or(ForecastEvidenceReadError::Unavailable)?;
        let analysis = self
            .exact_analysis(
                request.model().metadata(),
                request.pairing().analysis_fence().manifest(),
                request.model().metadata().dataset().selection_as_of(),
                deadline,
                cancellation.child_token(),
            )
            .await?
            .ok_or(ForecastEvidenceReadError::Unavailable)?;
        if pair(request.model().metadata(), &training, &analysis)? != *request.pairing() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        materialize(
            request,
            &analysis,
            &self.analytical,
            self.macro_context.as_ref(),
            self.calendar.as_ref(),
            None,
            deadline,
            cancellation,
        )
        .await
    }

    async fn revalidate(
        &self,
        expected: &ForecastEvidenceRevalidation,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastEvidenceReadError> {
        let request = expected.request().clone();
        let training = self
            .exact_training(
                request.model().metadata(),
                deadline,
                cancellation.child_token(),
            )
            .await?
            .ok_or(ForecastEvidenceReadError::Unavailable)?;
        let analysis = self
            .exact_analysis(
                request.model().metadata(),
                request.pairing().analysis_fence().manifest(),
                request.model().metadata().dataset().selection_as_of(),
                deadline,
                cancellation.child_token(),
            )
            .await?
            .ok_or(ForecastEvidenceReadError::Unavailable)?;
        if pair(request.model().metadata(), &training, &analysis)? != *request.pairing() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let prepared = materialize(
            request,
            &analysis,
            &self.analytical,
            self.macro_context.as_ref(),
            self.calendar.as_ref(),
            Some(expected.serving_input()),
            deadline,
            cancellation,
        )
        .await?;
        if prepared.evidence_sha256() != expected.evidence_sha256() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(())
    }
}

fn evidence_limits() -> Result<ForecastDatasetReadLimits, ForecastEvidenceReadError> {
    ForecastDatasetReadLimits::try_new(usize::MAX, MAX_BYTES)
        .map_err(|_| ForecastEvidenceReadError::Capacity)
}

fn analysis_contracts() -> [FeatureDatasetProductContract; 4] {
    use FeatureDatasetProductContract::*;
    [
        PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
        PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1,
        PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1,
        PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1,
    ]
}

fn dataset_contracts(
    metadata: &ModelMetadata,
) -> Result<(FeatureDatasetProductContract, FeatureDatasetProductContract), ForecastEvidenceReadError>
{
    use FeatureDatasetProductContract::*;
    use market_squawk_data::ProbabilityEventTarget::*;
    match metadata.output_binding().target() {
        ForecastTargetMeaning::FixedHorizonTerminal { .. }
            if metadata.output_binding().measurement() == ForecastMeasurement::Return =>
        {
            Ok((
                PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
                PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
            ))
        }
        ForecastTargetMeaning::FixedHorizonEvent { event, .. }
            if metadata.output_binding().measurement() == ForecastMeasurement::Probability
                && event.validate().is_ok()
                && metadata.label().name() == event.label_component_name() =>
        {
            Ok(match event {
                PriceHigher => (
                    PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1,
                    PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1,
                ),
                BenchmarkOutperformance { .. } => (
                    PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1,
                    PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1,
                ),
                ProfitAfterCosts { .. } => (
                    PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1,
                    PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1,
                ),
            })
        }
        _ => Err(ForecastEvidenceReadError::Unavailable),
    }
}

fn admitted_forecast_horizon(metadata: &ModelMetadata) -> Option<NonZeroU64> {
    match (
        metadata.output_binding().measurement(),
        metadata.output_binding().target(),
    ) {
        (
            ForecastMeasurement::Return,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos,
                origin_basis:
                    market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose
                    | market_squawk_data::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
            },
        ) => Some(horizon_nanos),
        (
            ForecastMeasurement::Probability,
            ForecastTargetMeaning::FixedHorizonEvent {
                horizon_nanos,
                origin_basis:
                    market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose
                    | market_squawk_data::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar,
                event,
            },
        ) if event.validate().is_ok()
            && metadata.label().name() == event.label_component_name()
            && metadata.probability_calibration().is_some() =>
        {
            Some(horizon_nanos)
        }
        _ => None,
    }
}

fn pairable_summary(
    metadata: &ModelMetadata,
    training: &ForecastDatasetEvidence,
    analysis: &AnalyticalFeatureDataset,
) -> bool {
    let training_dataset = training.dataset();
    let training_generation = training_dataset.generation();
    let analysis_generation = analysis.generation();
    analysis.product_contract()
        == match dataset_contracts(metadata) {
            Ok((_, contract)) => contract,
            Err(_) => return false,
        }
        && analysis_generation.build_spec_digest().is_some()
        && analysis_generation.build_spec_digest() != training_generation.build_spec_digest()
        && analysis_generation.manifest() != training_generation.manifest()
        && analysis_generation.parents() == training_generation.parents()
        && analysis_generation.row_count() == training_generation.row_count()
        && analysis.policy_digest() == training_dataset.policy_digest()
        && analysis.universe_digest() == training_dataset.universe_digest()
        && analysis.universe_id() == training_dataset.universe_id()
        && analysis.split_counts() == training_dataset.split_counts()
        && analysis.source_ids() == training_dataset.source_ids()
}

fn pair(
    metadata: &ModelMetadata,
    training: &ForecastDatasetEvidence,
    analysis: &ForecastDatasetEvidence,
) -> Result<ForecastDatasetPairingReceipt, ForecastEvidenceReadError> {
    if !pairable_summary(metadata, training, analysis.dataset())
        || training.fence().catalog_identity() != analysis.fence().catalog_identity()
        || training.fence().as_of() != analysis.fence().as_of()
        || training.fence().as_of() != metadata.dataset().selection_as_of()
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    let analysis_identity = TrainingDatasetIdentity::try_new(
        analysis.fence().manifest().clone(),
        analysis
            .dataset()
            .generation()
            .build_spec_digest()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
        analysis.dataset().universe_digest(),
        analysis.dataset().policy_digest(),
        analysis.fence().catalog_identity(),
        analysis.fence().export_sha256(),
        analysis.fence().selection_sha256(),
        analysis.fence().as_of(),
        analysis.fence().selected_rows(),
        analysis.dataset().split_policy(),
        analysis.dataset().study_policy().copied(),
        analysis.dataset().source_snapshot_digest(),
    )
    .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?;
    let fixed_horizon_nanos =
        admitted_forecast_horizon(metadata).ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let training_compatibility = shared_compatibility_digest(training, fixed_horizon_nanos)?;
    let analysis_compatibility = shared_compatibility_digest(analysis, fixed_horizon_nanos)?;
    if training_compatibility != analysis_compatibility {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    ForecastDatasetPairingReceipt::try_new(
        metadata.dataset().clone(),
        analysis_identity,
        training.fence().clone(),
        analysis.fence().clone(),
        training
            .dataset()
            .production_receipt()
            .production_identity(),
        training.dataset().production_receipt().receipt_sha256(),
        analysis
            .dataset()
            .production_receipt()
            .production_identity(),
        analysis.dataset().production_receipt().receipt_sha256(),
        fixed_horizon_nanos,
        training_compatibility,
    )
}

fn shared_compatibility_digest(
    evidence: &ForecastDatasetEvidence,
    fixed_horizon_nanos: NonZeroU64,
) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    let dataset = evidence.dataset();
    let split_counts = dataset.split_counts();
    let parent_graph = parent_graph_digest(dataset.generation().parents())?;
    let rows = evidence.rows().sorted_row_digests(
        |row| shared_row_digest(row).map(Sha256Digest::bytes),
        map_read_error,
    )?;

    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-training-analysis-compatibility/v1\0");
    digest.update(fixed_horizon_nanos.get().to_be_bytes());
    hash_admitted_feature_order(&mut digest)?;
    digest.update(dataset.policy_digest().bytes());
    digest.update(dataset.universe_digest().bytes());
    hash_bytes(&mut digest, dataset.universe_id().as_str().as_bytes())?;
    digest.update(parent_graph.bytes());
    digest.update(dataset.generation().row_count().to_be_bytes());
    digest.update(
        u64::try_from(split_counts.train_examples())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(split_counts.validation_examples())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(split_counts.test_examples())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(
        u64::try_from(dataset.source_ids().len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    for source_id in dataset.source_ids() {
        hash_bytes(&mut digest, source_id.as_str().as_bytes())?;
    }
    digest.update(evidence.fence().as_of().unix_nanos().to_be_bytes());
    digest.update(evidence.fence().selected_rows().get().to_be_bytes());
    digest.update(
        u64::try_from(evidence.rows().len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    for row in rows.iter() {
        digest.update(row.map_err(map_read_error)?);
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_admitted_feature_order(digest: &mut Sha256) -> Result<(), ForecastEvidenceReadError> {
    hash_bytes(digest, b"research.price-return")?;
    let contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1;
    digest.update(
        u64::try_from(contract.macro_components().len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    for (position, component) in contract.macro_components().iter().enumerate() {
        if usize::from(component.position()) != position {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        digest.update([component.position()]);
        hash_bytes(digest, component.component_name().as_bytes())?;
        hash_bytes(digest, component.unit().as_bytes())?;
    }
    Ok(())
}

fn shared_row_digest(row: &ForecastFeatureRow) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-training-analysis-compatible-row/v1\0");
    digest.update(row.instrument_id().as_uuid().as_bytes());
    digest.update(row.source_selection_as_of().unix_nanos().to_be_bytes());
    digest.update(
        row.decision_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_optional_timestamp(&mut digest, row.label_selection_as_of());
    hash_optional_timestamp(&mut digest, row.observed_effective_at());
    hash_optional_timestamp(&mut digest, row.label_effective_at());
    digest.update([row.target_coordinate_kind(), split_tag(row.split())]);
    digest.update([row.component_kind()]);
    hash_bytes(&mut digest, row.component_name().as_bytes())?;
    digest.update(row.component_version().to_be_bytes());
    match row.value() {
        ForecastFeatureValue::Float(value) => {
            if !value.is_finite() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            digest.update([1]);
            digest.update(value.to_bits().to_be_bytes());
        }
        ForecastFeatureValue::Decimal { mantissa, scale } => {
            digest.update([2]);
            digest.update(mantissa.to_be_bytes());
            digest.update([*scale]);
        }
        ForecastFeatureValue::Missing => digest.update([3]),
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

const fn split_tag(split: DatasetSplit) -> u8 {
    match split {
        DatasetSplit::Train => 1,
        DatasetSplit::Validation => 2,
        DatasetSplit::Test => 3,
    }
}

fn parent_graph_digest(
    parents: &[market_squawk_data::GenerationParent],
) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    if parents.is_empty() {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-shared-parent-graph/v1\0");
    digest.update(
        u64::try_from(parents.len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    for parent in parents {
        digest.update([match parent.relation() {
            GenerationParentRelation::AppendPredecessor => 1,
            GenerationParentRelation::CompactionPredecessor => 2,
            GenerationParentRelation::DerivedInput => 3,
        }]);
        hash_manifest(&mut digest, parent.manifest())?;
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn instrument_inventory(
    metadata: &ModelMetadata,
    rows: &ForecastFeatureRows,
    available_at: Timestamp,
    horizon: NonZeroU64,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ForecastInstrumentAvailability>, ForecastEvidenceReadError> {
    let mut inventory = Vec::new();
    let mut previous_instrument = None;
    for row in rows.iter_component(
        None,
        2,
        metadata.label().name(),
        metadata.label().version().get(),
    ) {
        check_control(deadline, cancellation)?;
        let row = row.map_err(map_read_error)?;
        let instrument = row.instrument_id();
        if previous_instrument == Some(instrument) {
            continue;
        }
        previous_instrument = Some(instrument);
        let Some(origin_label) = latest_oos_origin(metadata, rows, instrument, horizon)? else {
            continue;
        };
        let (origin, target) = exact_terminal_coordinates(&origin_label, horizon)?;
        let Some((history_count, observed_from, observed_through, scale)) =
            historical_label_summary(metadata, rows, instrument, target, horizon)?
        else {
            continue;
        };
        if history_count < MINIMUM_HISTORY {
            continue;
        }
        if observed_through != target || available_at < target {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        coefficient_row(
            metadata,
            rows,
            instrument,
            origin,
            target,
            origin_label.source_selection_as_of(),
            origin_label.split(),
        )?;
        inventory
            .try_reserve(1)
            .map_err(|_| ForecastEvidenceReadError::Capacity)?;
        inventory.push(ForecastInstrumentAvailability::try_new(
            instrument,
            observed_from,
            observed_through,
            available_at,
            NonZeroUsize::new(history_count).ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
            scale,
        )?);
    }
    Ok(inventory)
}

#[allow(
    clippy::too_many_arguments,
    reason = "source and calendar owners retain independent exact evidence"
)]
async fn materialize(
    request: ForecastEvidenceMaterializationRequest,
    evidence: &ForecastDatasetEvidence,
    analytical: &AnalyticalReadCapability,
    macro_context: Option<&MacroContextReadCapability>,
    calendar: Option<&crate::application::market_calendar::CompletedMarketSessionReadCapability>,
    expected_serving: Option<&ForecastServingInputFence>,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
    let metadata = request.model().metadata();
    let horizon =
        admitted_forecast_horizon(metadata).ok_or(ForecastEvidenceReadError::Unavailable)?;
    if request.selection().dataset_manifest() != request.pairing().training().manifest()
        || request.selection().analysis_manifest() != request.pairing().analysis_fence().manifest()
        || request.pairing().training() != metadata.dataset()
        || request.selection().horizon().points() != NonZeroU16::MIN
        || request.selection().horizon().step_nanos() != Some(horizon)
        || request.pairing().fixed_horizon_nanos() != horizon
        || !has_price_return_macro_context_feature_order_v1(metadata)
    {
        return Err(ForecastEvidenceReadError::Unavailable);
    }
    let instrument = request.selection().instrument_id();
    let historical_oos = latest_oos_origin(metadata, evidence.rows(), instrument, horizon)?
        .ok_or(ForecastEvidenceReadError::Unavailable)?;
    let (historical_origin, historical_target) =
        exact_terminal_coordinates(&historical_oos, horizon)?;
    if evidence.fence().as_of() < historical_target {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    coefficient_row(
        metadata,
        evidence.rows(),
        instrument,
        historical_origin,
        historical_target,
        historical_oos.source_selection_as_of(),
        historical_oos.split(),
    )?;
    let knowledge_cutoff = request.knowledge_cutoff();
    if let Some(input) = request.selection().current_feature_input() {
        let output = crate::application::model::forecast::reopen_current_price_input(
            analytical,
            input.manifest(),
            deadline,
            cancellation.child_token(),
        )
        .await
        .map_err(map_serving_error)?;
        let index = crate::application::model::forecast::current_price_coordinate_index(
            &output,
            input.example_id(),
        )
        .map_err(map_serving_error)?;
        let coordinate = output
            .coordinate(index)
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
        if coordinate.epoch().instrument_id() != instrument {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let calendar = calendar
            .cloned()
            .map(crate::application::market_calendar::ForecastSessionReadCapability::Current);
        crate::application::model::forecast::current_price_session_origin(
            calendar.as_ref(),
            input.session_cohort(),
            coordinate,
            deadline,
            cancellation.child_token(),
        )
        .await
        .map_err(map_serving_error)?;
        let serving = current_price_materialization(
            metadata,
            &output,
            index,
            knowledge_cutoff,
            input.session_cohort(),
        )?;
        if expected_serving.is_some_and(|expected| expected != &serving.fence) {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let inputs =
            crate::application::model::forecast::current_price_feature_values(metadata, coordinate)
                .map_err(map_serving_error)?;
        return PreparedForecastEvidence::try_new(
            request,
            serving.fence,
            serving.observed_cutoff,
            serving.available_at,
            serving.decimal_scale,
            serving.observed_history,
            vec![inputs.into_boxed_slice()],
        );
    }
    if matches!(
        metadata.output_binding().target(),
        ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) {
        return Err(ForecastEvidenceReadError::Unavailable);
    }
    let effective_date_cutoff = request.macro_effective_date_cutoff();
    let serving = serving_materialization(
        analytical,
        evidence,
        instrument,
        knowledge_cutoff,
        expected_serving,
        deadline,
        cancellation.child_token(),
    )
    .await?;
    let macro_context = macro_context.ok_or(ForecastEvidenceReadError::Unavailable)?;
    let macro_features = read_macro_feature_vector(
        macro_context,
        knowledge_cutoff,
        effective_date_cutoff,
        deadline,
        cancellation.child_token(),
    )
    .await
    .map_err(map_macro_feature_error)?;
    let (serving, input) =
        enrich_serving_materialization(serving, macro_features, effective_date_cutoff)?;
    PreparedForecastEvidence::try_new(
        request,
        serving.fence,
        serving.observed_cutoff,
        serving.available_at,
        serving.decimal_scale,
        serving.observed_history,
        vec![input],
    )
}

fn enrich_serving_materialization(
    mut serving: ServingMaterialization,
    macro_features: MacroFeatureVector,
    effective_date_cutoff: CalendarDate,
) -> Result<(ServingMaterialization, Box<[f64]>), ForecastEvidenceReadError> {
    if macro_features.knowledge_cutoff() != serving.fence.knowledge_cutoff()
        || macro_features.effective_date_cutoff() != effective_date_cutoff
        || macro_features.parent_manifests().is_empty()
        || macro_features.evidence_digest().algorithm() != DigestAlgorithm::Sha256
        || macro_features.evidence_digest().bytes() == [0; 32]
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    if macro_features.downstream_evidence_digest().algorithm() != DigestAlgorithm::Sha256
        || macro_features.downstream_evidence_digest().bytes() == [0; 32]
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }

    let descriptors =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1
            .macro_components();
    if macro_features.components().len() != descriptors.len()
        || macro_features.component_cutoffs().count() != descriptors.len()
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }

    let mut input = Vec::new();
    input
        .try_reserve_exact(descriptors.len() + 1)
        .map_err(|_| ForecastEvidenceReadError::Capacity)?;
    input.push(serving.current_return);

    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-serving-price-macro-vector/v1\0");
    digest.update(serving.fence.feature_sha256().bytes());
    digest.update(macro_features.evidence_digest().bytes());
    digest.update(macro_features.downstream_evidence_digest().bytes());
    if let Some(investment) = macro_features.investment_context() {
        if investment.knowledge_cutoff() != macro_features.knowledge_cutoff()
            || investment.effective_date_cutoff() != effective_date_cutoff
            || investment.parent_manifests() != macro_features.parent_manifests()
            || investment.evidence_digest().algorithm() != DigestAlgorithm::Sha256
            || investment.evidence_digest().bytes() == [0; 32]
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let regime = investment.regime();
        let valuation_rates = investment.valuation_rates();
        if regime.effective() != valuation_rates.effective()
            || regime.evidence_digest().algorithm() != DigestAlgorithm::Sha256
            || regime.evidence_digest().bytes() == [0; 32]
            || valuation_rates.evidence_digest().algorithm() != DigestAlgorithm::Sha256
            || valuation_rates.evidence_digest().bytes() == [0; 32]
            || valuation_rates.unit().as_str() != "percent_per_year"
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        digest.update([1]);
        digest.update(investment.evidence_digest().bytes());
        digest.update(regime.evidence_digest().bytes());
        digest.update(valuation_rates.evidence_digest().bytes());
        digest.update([match regime.regime() {
            MacroRateRegime::UpwardSloping => 1,
            MacroRateRegime::Flat => 2,
            MacroRateRegime::Inverted => 3,
            MacroRateRegime::Mixed => 4,
        }]);
        for value in [
            regime.three_month_to_ten_year_spread(),
            regime.two_year_to_ten_year_spread(),
            valuation_rates.ten_year_government_yield(),
            valuation_rates.thirty_year_government_yield(),
        ] {
            digest.update(value.mantissa().to_be_bytes());
            digest.update(value.scale().to_be_bytes());
        }
    } else {
        digest.update([0]);
    }
    digest.update(macro_features.knowledge_cutoff().unix_nanos().to_be_bytes());
    digest.update(effective_date_cutoff.year().to_be_bytes());
    digest.update([effective_date_cutoff.month(), effective_date_cutoff.day()]);
    digest.update(
        u64::try_from(macro_features.parent_manifests().len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    for parent in macro_features.parent_manifests() {
        hash_manifest(&mut digest, parent)?;
    }

    for (position, (component, descriptor)) in macro_features
        .components()
        .iter()
        .zip(descriptors)
        .enumerate()
    {
        let specification = component.spec();
        if usize::from(descriptor.position()) != position
            || specification.kind() != ComponentKind::Feature
            || specification.scope() != ComponentScope::Global
            || specification.corporate_actions() != CorporateActionSensitivity::NotApplicable
            || specification.name() != descriptor.component_name()
            || specification.version() != std::num::NonZeroU32::MIN
            || component.label_selection_effective_cutoff().is_some()
            || component.adjustment() != &ComponentAdjustmentEvidence::NotApplicable
            || component.selectors().len() != 1
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let selector = component
            .selectors()
            .first()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
        let ObservationFamilyKey::Macro { effective, .. } = selector.family() else {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        };
        if effective != component.selection_effective_cutoff() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let ComponentValue::Decimal {
            value,
            unit: Some(unit),
            currency: None,
        } = component.value()
        else {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        };
        if unit.as_str() != descriptor.unit() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let finite_value = value
            .to_f64()
            .filter(|value| value.is_finite())
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
        input.push(finite_value);

        digest.update([descriptor.position()]);
        hash_bytes(&mut digest, descriptor.component_name().as_bytes())?;
        hash_bytes(&mut digest, descriptor.unit().as_bytes())?;
        digest.update(selector.identity().bytes());
        hash_temporal_coordinate(&mut digest, effective)?;
        digest.update(value.mantissa().to_be_bytes());
        digest.update(value.scale().to_be_bytes());
    }

    let composite_feature_sha256 = Sha256Digest::new(digest.finalize().into());
    let mut serving_parents = serving.fence.parent_manifests().to_vec();
    for parent in macro_features.parent_manifests() {
        if !serving_parents.contains(parent) {
            serving_parents.push(parent.clone());
        }
    }
    let composite_fence = ForecastServingInputFence::try_new(
        serving.fence.manifest().clone(),
        serving.fence.source_id().clone(),
        serving.fence.object_graph_sha256(),
        serving.fence.selection_sha256(),
        serving.fence.result_sha256(),
        serving.fence.knowledge_cutoff(),
        serving
            .fence
            .prior_observed_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
        serving.fence.observed_through(),
        composite_feature_sha256,
    )?
    .with_origin_bar(serving.fence.origin_bar().cloned())?
    .with_parent_manifests(serving_parents)?;
    serving.available_at = serving.available_at.max(macro_features.knowledge_cutoff());
    serving.fence = composite_fence;
    Ok((serving, input.into_boxed_slice()))
}

fn hash_temporal_coordinate(
    digest: &mut Sha256,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), ForecastEvidenceReadError> {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        digest.update([1]);
        digest.update(timestamp.unix_nanos().to_be_bytes());
    } else if let Some(date) = coordinate.calendar_date_value() {
        digest.update([2]);
        digest.update(date.year().to_be_bytes());
        digest.update([date.month(), date.day()]);
    } else {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    Ok(())
}

const fn map_macro_feature_error(error: ServiceError) -> ForecastEvidenceReadError {
    match error {
        ServiceError::Cancelled => ForecastEvidenceReadError::Cancelled,
        ServiceError::DeadlineExceeded => ForecastEvidenceReadError::DeadlineExceeded,
        ServiceError::ResourceExhausted => ForecastEvidenceReadError::Capacity,
        ServiceError::InvalidResult | ServiceError::InvalidRequest => {
            ForecastEvidenceReadError::InvalidEvidence
        }
        ServiceError::NotFound
        | ServiceError::Unauthorized
        | ServiceError::Unavailable
        | ServiceError::Internal => ForecastEvidenceReadError::Unavailable,
    }
}

fn current_price_materialization(
    metadata: &ModelMetadata,
    output: &market_squawk_data::FeatureDatasetInputEpochOutput,
    index: usize,
    knowledge_cutoff: Timestamp,
    cohort: Option<&crate::application::market_calendar::ForecastSessionCohortReference>,
) -> Result<ServingMaterialization, ForecastEvidenceReadError> {
    let coordinate = output
        .coordinate(index)
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let epoch = coordinate.epoch();
    if epoch.source_selection_as_of() != knowledge_cutoff {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    let inputs =
        crate::application::model::forecast::current_price_feature_values(metadata, coordinate)
            .map_err(map_serving_error)?;
    let current_return = *inputs
        .first()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let fence = ForecastServingInputFence::from_current_price_output(output, index, cohort)?;
    let observed_cutoff = epoch
        .target_origin()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let observed = crate::application::model::forecast::current_price_observed_point(
        coordinate,
        current_return,
    )
    .map_err(map_serving_error)?;
    Ok(ServingMaterialization {
        fence,
        observed_cutoff,
        available_at: knowledge_cutoff,
        decimal_scale: FLOAT_RETURN_SCALE,
        observed_history: if matches!(
            metadata.output_binding().target(),
            ForecastTargetMeaning::FixedHorizonEvent { .. }
        ) {
            Vec::new()
        } else {
            vec![observed]
        },
        current_return,
    })
}

struct ServingMaterialization {
    fence: ForecastServingInputFence,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: Vec<ForecastObservedPoint>,
    current_return: f64,
}

async fn serving_materialization(
    analytical: &AnalyticalReadCapability,
    evidence: &ForecastDatasetEvidence,
    instrument: InstrumentId,
    knowledge_cutoff: Timestamp,
    expected: Option<&ForecastServingInputFence>,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<ServingMaterialization, ForecastEvidenceReadError> {
    check_control(deadline, &cancellation)?;
    if expected.is_some_and(|expected| expected.knowledge_cutoff() != knowledge_cutoff) {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }

    let parents = evidence.dataset().generation().parents();
    let mut manifests = Vec::new();
    manifests
        .try_reserve_exact(parents.len())
        .map_err(|_| ForecastEvidenceReadError::Capacity)?;
    if let Some(expected) = expected {
        if !parents
            .iter()
            .any(|parent| parent.manifest().dataset_id() == expected.manifest().dataset_id())
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        manifests.push(expected.manifest().clone());
    } else {
        for parent in parents {
            check_control(deadline, &cancellation)?;
            if manifests.iter().any(|manifest: &DatasetManifestRef| {
                manifest.dataset_id() == parent.manifest().dataset_id()
            }) {
                continue;
            }
            let Some(latest) = analytical
                .latest(parent.manifest().dataset_id(), deadline, &cancellation)
                .map_err(map_read_error)?
            else {
                continue;
            };
            manifests.push(latest.manifest().clone());
        }
    }

    let mut selected = None;
    for manifest in manifests {
        check_control(deadline, &cancellation)?;
        let Some(output) = read_serving_market_bars(
            analytical,
            manifest,
            instrument,
            knowledge_cutoff,
            deadline,
            cancellation.child_token(),
        )
        .await?
        else {
            continue;
        };
        let Some(candidate) = derive_serving_materialization(&output, knowledge_cutoff)? else {
            continue;
        };
        if selected.is_some() {
            return Err(ForecastEvidenceReadError::Unavailable);
        }
        selected = Some(candidate);
    }
    let selected = selected.ok_or(ForecastEvidenceReadError::Unavailable)?;
    // The retained fence includes the macro vector. Revalidate the exact bar selection here;
    // `revalidate` compares the complete evidence after the same macro vector is reconstructed.
    if expected.is_some_and(|expected| {
        expected.manifest() != selected.fence.manifest()
            || expected.source_id() != selected.fence.source_id()
            || expected.object_graph_sha256() != selected.fence.object_graph_sha256()
            || expected.selection_sha256() != selected.fence.selection_sha256()
            || expected.result_sha256() != selected.fence.result_sha256()
            || expected.knowledge_cutoff() != selected.fence.knowledge_cutoff()
            || expected.prior_observed_at() != selected.fence.prior_observed_at()
            || expected.observed_through() != selected.fence.observed_through()
            || expected.origin_bar() != selected.fence.origin_bar()
    }) {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    Ok(selected)
}

async fn read_serving_market_bars(
    analytical: &AnalyticalReadCapability,
    manifest: DatasetManifestRef,
    instrument: InstrumentId,
    knowledge_cutoff: Timestamp,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Option<market_squawk_data::AnalyticalMarketBarOutput>, ForecastEvidenceReadError> {
    check_control(deadline, &cancellation)?;
    let start = knowledge_cutoff
        .checked_sub_nanos(SERVING_LOOKBACK_NANOS)
        .map_err(|_| ForecastEvidenceReadError::Unavailable)?;
    let range = MarketBarEffectiveRange::try_new(start, knowledge_cutoff)
        .map_err(|_| ForecastEvidenceReadError::Unavailable)?;
    let read_limit = AnalyticalMarketBarReadLimit::try_new(MAX_SERVING_BARS)
        .map_err(|_| ForecastEvidenceReadError::Capacity)?;
    let query_limits = QueryLimits::try_new_with_inline_bytes(
        u64::from(MAX_SERVING_BARS),
        SERVING_QUERY_BYTES,
        SERVING_QUERY_BYTES,
        SERVING_QUERY_BYTES * 2,
        4,
        512,
        512,
        SERVING_QUERY_DURATION,
    )
    .map_err(|_| ForecastEvidenceReadError::Capacity)?;

    let request = match AnalyticalMarketBarReadRequest::try_new(
        manifest,
        instrument,
        knowledge_cutoff,
        Some(range),
        read_limit,
    ) {
        Ok(request) => request,
        Err(market_squawk_data::AnalyticalReadError::InvalidObservationSchema) => return Ok(None),
        Err(error) => return Err(map_read_error(error)),
    };
    analytical
        .read_market_bars(request, query_limits, deadline, cancellation)
        .await
        .map(Some)
        .map_err(map_read_error)
}

/// Reopens the exact original serving query authenticated by the controlled forecast artifact.
pub(crate) async fn reopen_forecast_serving_output(
    analytical: &AnalyticalReadCapability,
    binding: &market_squawk_modeling::AuthenticatedForecastServingBinding,
    instrument: InstrumentId,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<market_squawk_data::AnalyticalMarketBarOutput, ForecastEvidenceReadError> {
    let output = read_serving_market_bars(
        analytical,
        binding.manifest().clone(),
        instrument,
        binding.knowledge_cutoff(),
        deadline,
        cancellation.child_token(),
    )
    .await?
    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let selected = derive_serving_materialization(&output, binding.knowledge_cutoff())?
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let fence = selected.fence;
    if fence.manifest() != binding.manifest()
        || fence.source_id() != binding.source_id()
        || fence.object_graph_sha256() != binding.object_graph_sha256()
        || fence.selection_sha256() != binding.selection_sha256()
        || fence.result_sha256() != binding.result_sha256()
        || fence.knowledge_cutoff() != binding.knowledge_cutoff()
        || fence.prior_observed_at() != binding.prior_observed_at()
        || Some(fence.observed_through()) != binding.observed_through()
        || fence.origin_bar() != binding.origin_bar()
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    check_control(deadline, &cancellation)?;
    Ok(output)
}

fn derive_serving_materialization(
    output: &market_squawk_data::AnalyticalMarketBarOutput,
    knowledge_cutoff: Timestamp,
) -> Result<Option<ServingMaterialization>, ForecastEvidenceReadError> {
    let bars = output.bars();
    if bars.len() < MINIMUM_HISTORY + 1 || bars.len() >= MAX_SERVING_BARS as usize {
        return Ok(None);
    }
    let first = bars
        .first()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    if first.adjustment() != MarketBarAdjustment::Split
        || bars.windows(2).any(|pair| {
            !same_serving_series(&pair[0], &pair[1])
                || pair[0].completed_at() >= pair[1].completed_at()
        })
    {
        return Ok(None);
    }
    let mut observed_history = Vec::new();
    observed_history
        .try_reserve_exact(bars.len() - 1)
        .map_err(|_| ForecastEvidenceReadError::Capacity)?;
    let object_graph = sha256_evidence(output.output().object_graph_digest())?;
    let selection = sha256_evidence(output.output().query_identity())?;
    let result = sha256_evidence(output.output().result_digest())?;
    let mut latest_feature = None;
    let mut maximum_available_at = None;
    for pair in bars.windows(2) {
        let prior = &pair[0];
        let current = &pair[1];
        let observed_at = current
            .completed_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
        if observed_at > knowledge_cutoff {
            continue;
        }
        let available_at =
            conservative_available_at(prior)?.max(conservative_available_at(current)?);
        let value = simple_return(prior, current)?;
        let feature_sha256 =
            serving_feature_digest(object_graph, selection, result, prior, current, value)?;
        let forecast_value = forecast_return(value)?;
        observed_history.push(
            ForecastObservedPoint::try_new(
                observed_at,
                available_at,
                forecast_value,
                feature_sha256,
                DataQuality::Aggregated,
            )
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?,
        );
        maximum_available_at = Some(
            maximum_available_at.map_or(available_at, |retained: Timestamp| {
                retained.max(available_at)
            }),
        );
        latest_feature = Some((prior, current, value, feature_sha256));
    }
    if observed_history.len() < MINIMUM_HISTORY {
        return Ok(None);
    }
    let (prior, current, current_value, feature_sha256) =
        latest_feature.ok_or(ForecastEvidenceReadError::Unavailable)?;
    let observed_cutoff = current
        .completed_at()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let available_at = maximum_available_at.ok_or(ForecastEvidenceReadError::Unavailable)?;
    let current_return = current_value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let decimal_scale = observed_history
        .first()
        .map(|point| point.value().scale())
        .ok_or(ForecastEvidenceReadError::Unavailable)?;
    if observed_history
        .iter()
        .any(|point| point.value().scale() != decimal_scale)
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    let fence = ForecastServingInputFence::try_new(
        output.output().manifest().clone(),
        output.source_id().clone(),
        object_graph,
        selection,
        result,
        knowledge_cutoff,
        prior
            .completed_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
        observed_cutoff,
        feature_sha256,
    )?
    .with_origin_bar(Some(current.clone()))?;
    Ok(Some(ServingMaterialization {
        fence,
        observed_cutoff,
        available_at,
        decimal_scale,
        observed_history,
        current_return,
    }))
}

fn same_serving_series(left: &MarketBarObservation, right: &MarketBarObservation) -> bool {
    let left_provenance = left.context().provenance();
    let right_provenance = right.context().provenance();
    left_provenance.source_id() == right_provenance.source_id()
        && left_provenance.venue_id() == right_provenance.venue_id()
        && left.provider_instrument_id() == right.provider_instrument_id()
        && left.feed() == right.feed()
        && left.interval() == right.interval()
        && left.adjustment() == right.adjustment()
        && left.currency() == right.currency()
        && left.time_semantics().timestamp_basis() == right.time_semantics().timestamp_basis()
        && left.time_semantics().session() == right.time_semantics().session()
}

fn conservative_available_at(
    bar: &MarketBarObservation,
) -> Result<Timestamp, ForecastEvidenceReadError> {
    bar.context()
        .provenance()
        .availability()
        .conservative_available_at()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)
}

fn simple_return(
    prior: &MarketBarObservation,
    current: &MarketBarObservation,
) -> Result<Decimal, ForecastEvidenceReadError> {
    let mut value = current
        .close()
        .amount()
        .checked_div(prior.close().amount())
        .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
        .map(|value| value.round_dp(u32::from(FLOAT_RETURN_SCALE)))
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    value.rescale(u32::from(FLOAT_RETURN_SCALE));
    Ok(value)
}

fn forecast_return(value: Decimal) -> Result<ForecastValue, ForecastEvidenceReadError> {
    ForecastValue::try_new(value.mantissa(), value.scale() as u8)
        .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)
}

fn serving_feature_digest(
    object_graph: Sha256Digest,
    selection: Sha256Digest,
    result: Sha256Digest,
    prior: &MarketBarObservation,
    current: &MarketBarObservation,
    value: Decimal,
) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/forecast-serving-price-return/v1\0");
    digest.update(b"CompletedBarClose\0");
    digest.update(
        prior
            .completed_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
            .unix_nanos()
            .to_be_bytes(),
    );
    digest.update(
        current
            .completed_at()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
            .unix_nanos()
            .to_be_bytes(),
    );
    digest.update(object_graph.bytes());
    digest.update(selection.bytes());
    digest.update(result.bytes());
    digest.update(
        prior
            .time_semantics()
            .provider_timestamp()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
            .unix_nanos()
            .to_be_bytes(),
    );
    digest.update(
        current
            .time_semantics()
            .provider_timestamp()
            .ok_or(ForecastEvidenceReadError::InvalidEvidence)?
            .unix_nanos()
            .to_be_bytes(),
    );
    hash_bytes(&mut digest, prior.close().amount().to_string().as_bytes())?;
    hash_bytes(&mut digest, current.close().amount().to_string().as_bytes())?;
    hash_bytes(&mut digest, value.to_string().as_bytes())?;
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn sha256_evidence(value: EvidenceDigest) -> Result<Sha256Digest, ForecastEvidenceReadError> {
    if value.algorithm() != DigestAlgorithm::Sha256 || value.bytes() == [0; 32] {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    Ok(Sha256Digest::new(value.bytes()))
}

fn exact_terminal_coordinates(
    row: &ForecastFeatureRow,
    horizon: NonZeroU64,
) -> Result<(Timestamp, Timestamp), ForecastEvidenceReadError> {
    match (
        row.target_coordinate_kind(),
        row.observed_effective_at(),
        row.label_effective_at(),
        row.decision_at(),
    ) {
        (3 | 5, Some(observed), Some(label), Some(decision))
            if observed <= decision
                && decision < label
                && label.unix_nanos().checked_sub(observed.unix_nanos())
                    == i64::try_from(horizon.get()).ok() =>
        {
            Ok((observed, label))
        }
        _ => Err(ForecastEvidenceReadError::InvalidEvidence),
    }
}

fn latest_oos_origin(
    metadata: &ModelMetadata,
    rows: &ForecastFeatureRows,
    instrument: InstrumentId,
    horizon: NonZeroU64,
) -> Result<Option<ForecastFeatureRow>, ForecastEvidenceReadError> {
    let training_end = metadata
        .training_period()
        .end()
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let mut selected: Option<ForecastFeatureRow> = None;
    for row in rows.iter_component(
        Some(instrument),
        2,
        metadata.label().name(),
        metadata.label().version().get(),
    ) {
        let row = row.map_err(map_read_error)?;
        if matches!(row.value(), ForecastFeatureValue::Missing) {
            continue;
        }
        let (origin, _) = exact_terminal_coordinates(&row, horizon)?;
        if row.split() == DatasetSplit::Train || origin < training_end {
            continue;
        }
        match selected.as_ref() {
            Some(current) => {
                let (current_origin, _) = exact_terminal_coordinates(current, horizon)?;
                if origin == current_origin {
                    return Err(ForecastEvidenceReadError::InvalidEvidence);
                }
                if origin > current_origin {
                    selected = Some(row);
                }
            }
            None => selected = Some(row),
        }
    }
    Ok(selected)
}

fn historical_label_summary(
    metadata: &ModelMetadata,
    rows: &ForecastFeatureRows,
    instrument: InstrumentId,
    through: Timestamp,
    horizon: NonZeroU64,
) -> Result<Option<(usize, Timestamp, Timestamp, u8)>, ForecastEvidenceReadError> {
    let mut summary: Option<(usize, Timestamp, Timestamp, u8)> = None;
    for row in rows.iter_component(
        Some(instrument),
        2,
        metadata.label().name(),
        metadata.label().version().get(),
    ) {
        let row = row.map_err(map_read_error)?;
        if matches!(row.value(), ForecastFeatureValue::Missing) {
            continue;
        }
        let (_, target) = exact_terminal_coordinates(&row, horizon)?;
        if target > through {
            continue;
        }
        let scale = observed_value(&row)?.scale();
        summary = Some(match summary {
            None => (1, target, target, scale),
            Some((count, first, previous, expected_scale)) => {
                if previous >= target || scale != expected_scale {
                    return Err(ForecastEvidenceReadError::InvalidEvidence);
                }
                (
                    count
                        .checked_add(1)
                        .ok_or(ForecastEvidenceReadError::Capacity)?,
                    first,
                    target,
                    scale,
                )
            }
        });
    }
    Ok(summary)
}

fn observed_value(row: &ForecastFeatureRow) -> Result<ForecastValue, ForecastEvidenceReadError> {
    match row.value() {
        ForecastFeatureValue::Decimal { mantissa, scale } => {
            ForecastValue::try_new(*mantissa, *scale)
                .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)
        }
        ForecastFeatureValue::Float(value) => {
            if !value.is_finite() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            let factor = 10_u64.pow(u32::from(FLOAT_RETURN_SCALE)) as f64;
            let scaled = *value * factor;
            if !scaled.is_finite() || scaled.abs() > i128::MAX as f64 {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            ForecastValue::try_new(scaled.round() as i128, FLOAT_RETURN_SCALE)
                .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)
        }
        ForecastFeatureValue::Missing => Err(ForecastEvidenceReadError::InvalidEvidence),
    }
}

fn coefficient_row(
    metadata: &ModelMetadata,
    rows: &ForecastFeatureRows,
    instrument: InstrumentId,
    origin: Timestamp,
    target: Timestamp,
    cutoff: Timestamp,
    split: DatasetSplit,
) -> Result<Vec<f64>, ForecastEvidenceReadError> {
    metadata
        .features()
        .iter()
        .map(|binding| {
            let mut selected = None;
            for row in rows.iter_component(
                Some(instrument),
                1,
                binding.key().name(),
                binding.key().version().get(),
            ) {
                let row = row.map_err(map_read_error)?;
                if row.source_selection_as_of() == cutoff
                    && row.observed_effective_at() == Some(origin)
                    && row.label_effective_at() == Some(target)
                    && matches!(row.target_coordinate_kind(), 3 | 5)
                    && row.split() == split
                {
                    if selected.is_some() {
                        return Err(ForecastEvidenceReadError::InvalidEvidence);
                    }
                    selected = Some(row);
                }
            }
            finite_value(&selected.ok_or(ForecastEvidenceReadError::Unavailable)?)
        })
        .collect()
}

fn finite_value(row: &ForecastFeatureRow) -> Result<f64, ForecastEvidenceReadError> {
    let value = match row.value() {
        ForecastFeatureValue::Float(value) => *value,
        ForecastFeatureValue::Decimal { mantissa, scale } => {
            (*mantissa as f64) / 10_f64.powi(i32::from(*scale))
        }
        ForecastFeatureValue::Missing => return Err(ForecastEvidenceReadError::Unavailable),
    };
    value
        .is_finite()
        .then_some(value)
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)
}

fn hash_manifest(
    digest: &mut Sha256,
    manifest: &DatasetManifestRef,
) -> Result<(), ForecastEvidenceReadError> {
    hash_bytes(digest, manifest.dataset_id().as_str().as_bytes())?;
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(digest, manifest.schema().name().as_bytes())?;
    digest.update(manifest.schema_version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    Ok(())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), ForecastEvidenceReadError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| ForecastEvidenceReadError::Capacity)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ForecastEvidenceReadError> {
    if cancellation.is_cancelled() {
        Err(ForecastEvidenceReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ForecastEvidenceReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_read_error(error: market_squawk_data::AnalyticalReadError) -> ForecastEvidenceReadError {
    match error {
        market_squawk_data::AnalyticalReadError::InvalidLimit => {
            ForecastEvidenceReadError::Capacity
        }
        market_squawk_data::AnalyticalReadError::Query(
            market_squawk_data::QueryError::SpillStorageExhausted
            | market_squawk_data::QueryError::MemoryLimitExceeded { .. },
        ) => ForecastEvidenceReadError::Capacity,
        market_squawk_data::AnalyticalReadError::Query(
            market_squawk_data::QueryError::Cancelled,
        ) => ForecastEvidenceReadError::Cancelled,
        market_squawk_data::AnalyticalReadError::Query(
            market_squawk_data::QueryError::DeadlineExceeded,
        ) => ForecastEvidenceReadError::DeadlineExceeded,
        market_squawk_data::AnalyticalReadError::Manifest(_)
        | market_squawk_data::AnalyticalReadError::PythonDataset(_) => {
            ForecastEvidenceReadError::InvalidEvidence
        }
        market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable => {
            ForecastEvidenceReadError::Unavailable
        }
        _ => ForecastEvidenceReadError::Unavailable,
    }
}

fn map_serving_error(error: ServiceError) -> ForecastEvidenceReadError {
    match error {
        ServiceError::Cancelled => ForecastEvidenceReadError::Cancelled,
        ServiceError::DeadlineExceeded => ForecastEvidenceReadError::DeadlineExceeded,
        ServiceError::ResourceExhausted => ForecastEvidenceReadError::Capacity,
        ServiceError::Unavailable | ServiceError::NotFound => {
            ForecastEvidenceReadError::Unavailable
        }
        _ => ForecastEvidenceReadError::InvalidEvidence,
    }
}
