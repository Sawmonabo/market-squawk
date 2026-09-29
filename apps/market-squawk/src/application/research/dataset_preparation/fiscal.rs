//! Native fiscal datasets from the same authenticated source, population and publisher.

use super::*;
use crate::application::{
    analytical_profile::ValidatedAnalyticalProfile,
    research::financial_targets::read_native_financial_series,
};
use market_squawk_data::{
    CurrentListedPopulation, CurrentListedPopulationPartition, DatasetTargetHorizon,
    FinancialAmountSelection, FinancialDatasetSeries,
};
use market_squawk_domain::FundamentalCadence;
use market_squawk_services::RequestContext;
use std::num::NonZeroU16;

mod historical;
mod replay;
pub(crate) use historical::{
    HistoricalFiscalDatasetExpectation, HistoricalFiscalTrainingAuthority,
    HistoricalOriginFinancialForecast, PreparedHistoricalFiscalDatasets,
};
pub(crate) use replay::{
    HISTORICAL_FISCAL_MAXIMUM_ORIGINS, HISTORICAL_FISCAL_MAXIMUM_PAGES,
    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
    HISTORICAL_FISCAL_PAGE_SIZE, HistoricalFiscalCompletedJobs, HistoricalFiscalJobReference,
    HistoricalFiscalPageDescriptor, HistoricalFiscalPageReference, HistoricalFiscalStudyBinding,
    HistoricalFiscalForecastReadCapability, HistoricalFiscalForecastReference,
    HistoricalFiscalRecipeReference, HistoricalFiscalSourceSelection,
    HistoricalFiscalUnavailableReference,
};

const MAXIMUM_FISCAL_PERIODS: usize = 1024;
const MAXIMUM_FISCAL_PARENT_ROWS: usize = 65_536;
const MAXIMUM_FISCAL_BUILD_BYTES: usize = 64 * 1024 * 1024;

/// Requested meaning and genuine current issuer cohort; no financial values are accepted.
#[derive(Clone, Debug)]
pub(crate) struct FiscalDatasetPreparationRequest {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) knowledge_cutoff: Timestamp,
    pub(crate) effective_date_cutoff: CalendarDate,
    pub(crate) measurement: FinancialAmountSelection,
    pub(crate) cadence: FundamentalCadence,
    pub(crate) periods_ahead: NonZeroU16,
    pub(crate) population: CurrentListedPopulation,
}

/// Ordinary producer requests consumed sequentially by the existing dataset job runner.
pub(crate) struct PreparedFiscalDatasetPair {
    training: PreparedFeatureDatasetBuild,
    study_inputs: PreparedFeatureDatasetBuild,
    study_example_id: String,
}
impl PreparedFiscalDatasetPair {
    pub(crate) fn into_parts(
        self,
    ) -> (
        PreparedFeatureDatasetBuild,
        PreparedFeatureDatasetBuild,
        String,
    ) {
        (self.training, self.study_inputs, self.study_example_id)
    }
}

impl DatasetPreparationAuthority {
    /// Predeclares native partitions and purges crossing labels. Both generations retain the
    /// declared present-day fixed cohort and all retrospective limitations. The latest fiscal
    /// period is reserved exclusively for serving, strictly after the fit/calibration windows.
    pub(crate) async fn prepare_financial_datasets(
        &self,
        request: FiscalDatasetPreparationRequest,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedFiscalDatasetPair, ServiceError> {
        self.prepare_financial_datasets_at_origin(request, profile, None, context)
            .await
    }

    async fn prepare_financial_datasets_at_origin(
        &self,
        request: FiscalDatasetPreparationRequest,
        profile: &ValidatedAnalyticalProfile,
        historical_origin: Option<&market_squawk_data::FeatureDatasetInputEpoch>,
        context: &RequestContext,
    ) -> Result<PreparedFiscalDatasetPair, ServiceError> {
        ensure_request(context)?;
        if request.population.instrument_ids() != [request.instrument_id]
            || request.population.membership_as_of() != request.knowledge_cutoff
            || request.population.financial_profile_digest().bytes()
                != decode_sha256(&profile.resolution().configuration_digest)
                    .ok_or(ServiceError::InvalidRequest)?
            || !profile
                .recommendation_policy()
                .parameters()
                .allow_retrospective_studies
            || !matches!(
                request.cadence,
                FundamentalCadence::Annual | FundamentalCadence::Quarterly
            )
        {
            return Err(ServiceError::InvalidRequest);
        }
        let series = read_native_financial_series(
            &self.research,
            request.instrument_id,
            request.knowledge_cutoff,
            request.effective_date_cutoff,
            request.measurement,
            request.cadence,
            context,
        )
        .await?;
        if series.is_empty() || series.len() > MAXIMUM_FISCAL_PERIODS {
            return Err(ServiceError::Unavailable);
        }
        let target = DatasetTargetHorizon::FiscalPeriods {
            cadence: request.cadence,
            periods_ahead: request.periods_ahead,
        };
        let policy = |purpose| {
            DatasetStudyPolicy::try_new(
                HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
                purpose,
                request.knowledge_cutoff,
                Some(Duration::ZERO),
                target,
            )
            .map_err(|_| ServiceError::InvalidRequest)
        };
        let training_policy = policy(DatasetBuildPurpose::Training)?;
        let current_policy = policy(DatasetBuildPurpose::StudyInputs)?;
        let partitions = native_partitions(&series, request.periods_ahead)?;
        // Historical scoring owns a separate native serving origin. Its training examples were
        // already purged through partitions.ends[2], so widening only the evaluation envelope
        // admits that unlabeled origin without adding a fitting or calibration observation.
        let evaluation_end = if historical_origin.is_some() {
            series
                .observed_period(
                    u32::try_from(series.len() - 1).map_err(|_| ServiceError::ResourceExhausted)?,
                )
                .ok_or(ServiceError::InvalidResult)?
                .end()
        } else {
            partitions.ends[2]
        };
        let training_split = ChronologicalSplitPolicy::try_fiscal(
            partitions.ends[0],
            partitions.ends[1],
            evaluation_end,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let base_identity = fiscal_preparation_identity(&request, &series, &partitions.ends)?;
        let identity = match historical_origin {
            Some(epoch) => {
                let mut hash = Sha256::new();
                hash.update(b"market-squawk/historical-fiscal-origin-preparation/v1\0");
                hash.update(base_identity.bytes());
                hash.update(epoch.canonical_bytes().map_err(map_fiscal_build_error)?);
                Sha256Digest::new(hash.finalize().into())
            }
            None => base_identity,
        };
        let mut examples = Vec::new();
        examples
            .try_reserve_exact(partitions.origins.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for ordinal in partitions.origins {
            ensure_request(context)?;
            let period = series
                .observed_period(ordinal)
                .ok_or(ServiceError::InvalidResult)?;
            examples.push(
                series
                    .try_example(
                        &format!("fiscal-{}-{ordinal:04}", short_hex(identity)),
                        ordinal,
                        &training_policy,
                        request.knowledge_cutoff,
                        Some(request.knowledge_cutoff),
                        ResearchTemporalCoordinate::calendar_date(period.end()),
                    )
                    .map_err(map_fiscal_build_error)?,
            );
        }
        let current_ordinal =
            u32::try_from(series.len() - 1).map_err(|_| ServiceError::ResourceExhausted)?;
        let current_period = series
            .observed_period(current_ordinal)
            .ok_or(ServiceError::InvalidResult)?;
        let study_example_id = format!("fiscal-{}-current", short_hex(identity));
        let current = series
            .try_example(
                &study_example_id,
                current_ordinal,
                &current_policy,
                request.knowledge_cutoff,
                None,
                ResearchTemporalCoordinate::calendar_date(current_period.end()),
            )
            .map_err(map_fiscal_build_error)?;
        // The current row occupies the final native partition; no timestamp is fabricated from
        // a fiscal date. Its independent generation contains no training labels or observations.
        let current_split = ChronologicalSplitPolicy::try_fiscal(
            partitions.ends[0],
            partitions.ends[1],
            current_period.end(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let partitions = request
            .population
            .partitions()
            .map_err(|_| ServiceError::InvalidResult)?;
        let [population] = partitions.as_ref() else {
            return Err(ServiceError::InvalidResult);
        };
        let training = prepare_native_build(
            population,
            series.source_manifest(),
            examples,
            training_policy,
            training_split,
            identity,
        )?;
        let study_inputs = prepare_native_build(
            population,
            series.source_manifest(),
            vec![current],
            current_policy,
            current_split,
            identity,
        )?;
        for prepared in [&training, &study_inputs] {
            ensure_request(context)?;
            self.research
                .analytical()
                .dataset_builder()
                .validate_request_authority(&prepared.request, context.cancellation())
                .map_err(map_fiscal_build_error)?;
        }
        Ok(PreparedFiscalDatasetPair {
            training,
            study_inputs,
            study_example_id,
        })
    }
}

struct FiscalPartitions {
    ends: [CalendarDate; 3],
    origins: Vec<u32>,
}

/// Partition selection uses only authentic source ordinals and periods, before looking at labels.
fn native_partitions(
    series: &FinancialDatasetSeries,
    horizon: NonZeroU16,
) -> Result<FiscalPartitions, ServiceError> {
    // Reserve latest origin from every training/calibration/evaluation interval.
    let count = series
        .len()
        .checked_sub(1)
        .ok_or(ServiceError::Unavailable)?;
    let first = count / 3;
    let second = count
        .checked_mul(2)
        .ok_or(ServiceError::ResourceExhausted)?
        / 3;
    if first == 0 || second <= first || second >= count {
        return Err(ServiceError::Unavailable);
    }
    let indices = [first - 1, second - 1, count - 1];
    let mut dates = Vec::with_capacity(3);
    for index in indices {
        dates.push(
            series
                .observed_period(u32::try_from(index).map_err(|_| ServiceError::ResourceExhausted)?)
                .ok_or(ServiceError::InvalidResult)?
                .end(),
        );
    }
    let mut origins = Vec::new();
    origins
        .try_reserve_exact(count)
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let mut counts = [0_usize; 3];
    for current in 0..count {
        let terminal = current
            .checked_add(usize::from(horizon.get()))
            .ok_or(ServiceError::ResourceExhausted)?;
        let split = if current < first {
            0
        } else if current < second {
            1
        } else {
            2
        };
        if terminal <= indices[split] {
            origins.push(u32::try_from(current).map_err(|_| ServiceError::ResourceExhausted)?);
            counts[split] += 1;
        }
    }
    if counts[0] < 2 || counts[1] == 0 || counts[2] == 0 {
        return Err(ServiceError::Unavailable);
    }
    Ok(FiscalPartitions {
        ends: [dates[0], dates[1], dates[2]],
        origins,
    })
}

fn prepare_native_build(
    population: &CurrentListedPopulationPartition,
    source_manifest: &DatasetManifestRef,
    examples: Vec<DatasetExample>,
    study: DatasetStudyPolicy,
    split: ChronologicalSplitPolicy,
    identity: Sha256Digest,
) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
    let training = study.purpose() == DatasetBuildPurpose::Training;
    let contract = if training {
        FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1
    } else {
        FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
    };
    let use_case = if training {
        DatasetPreparationUse::Train
    } else {
        DatasetPreparationUse::LocalAnalysis
    };
    // Copy registered component specs from actual source examples. The same publisher
    // independently verifies their closed financial recipe at final publication.
    let specs = examples
        .first()
        .ok_or(ServiceError::InvalidResult)?
        .components()
        .iter()
        .map(|component| component.spec().clone())
        .collect();
    let example_count = examples.len();
    let component_count = if training { 2 } else { 1 };
    let inputs = DatasetBuildInputs::try_new_for_current_population(
        vec![source_manifest.clone()],
        population.clone(),
        specs,
        examples,
        Vec::new(),
    )
    .map_err(map_fiscal_build_error)?;
    let policy = DatasetBuildPolicy::new(
        split,
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
            .map_err(|_| ServiceError::InvalidResult)?,
        CorporateActionPolicy::new(CorporateActionAdjustment::Raw, NonZeroU32::MIN),
        MissingValuePolicy::Reject,
        SourceIdentifier::try_from(contract.implementation_revision())
            .map_err(|_| ServiceError::InvalidResult)?,
        Some(study),
    );
    let output = format!(
        "prepared.fiscal.{}.{}",
        short_hex(identity),
        if training { "train" } else { "inputs" }
    );
    let request = DatasetBuildRequest::try_new(
        DatasetId::try_from(output.as_str()).map_err(|_| ServiceError::InvalidResult)?,
        inputs,
        policy,
        use_case.domain(),
        ResearchUseLimits::try_new(
            1,
            4096,
            8192,
            4096,
            16 * 1024 * 1024,
            Duration::from_secs(30),
            Duration::from_secs(300),
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        derived_authorization(identity, use_case).map_err(ServiceError::from)?,
        DatasetBuildLimits::try_new(
            MAXIMUM_FISCAL_PARENT_ROWS,
            example_count,
            component_count,
            example_count
                .checked_mul(component_count)
                .ok_or(ServiceError::ResourceExhausted)?,
            MAXIMUM_FISCAL_BUILD_BYTES,
            BUILD_DURATION,
            PointInTimeLimits::try_new(
                MAXIMUM_FISCAL_PARENT_ROWS,
                MAXIMUM_FISCAL_PARENT_ROWS,
                256,
                MAXIMUM_FISCAL_PARENT_ROWS,
                MAXIMUM_FISCAL_BUILD_BYTES,
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            UniverseLimits::try_new(1024, 4 * 1024 * 1024)
                .map_err(|_| ServiceError::InvalidResult)?,
            CorporateActionLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroUsize::new(1024).ok_or(ServiceError::InvalidResult)?,
            )
            .map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(map_fiscal_build_error)?,
    )
    .map_err(map_fiscal_build_error)?;
    Ok(PreparedFeatureDatasetBuild {
        finalizer: FeatureDatasetProductionFinalizer {
            contract,
            build_spec: request.build_spec_digest().digest(),
            evidence: None,
            maximum_currentness_expires_at: None,
        },
        request,
    })
}

fn fiscal_preparation_identity(
    request: &FiscalDatasetPreparationRequest,
    series: &FinancialDatasetSeries,
    partitions: &[CalendarDate; 3],
) -> Result<Sha256Digest, ServiceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/native-fiscal-default-preparation/v1\0");
    hash_manifest(&mut hash, series.source_manifest());
    hash.update(request.instrument_id.as_uuid().as_bytes());
    hash.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
    hash.update(
        request
            .effective_date_cutoff
            .days_since_unix_epoch()
            .to_be_bytes(),
    );
    hash.update(request.population.content_digest().bytes());
    hash.update(request.population.audit_digest().bytes());
    hash.update(request.population.financial_profile_digest().bytes());
    hash.update(request.periods_ahead.get().to_be_bytes());
    hash.update(
        serde_json::to_vec(&(request.measurement, request.cadence))
            .map_err(|_| ServiceError::InvalidResult)?,
    );
    for date in partitions {
        hash.update(date.days_since_unix_epoch().to_be_bytes());
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}
fn ensure_request(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn map_fiscal_build_error(error: market_squawk_data::DatasetBuildError) -> ServiceError {
    match error {
        market_squawk_data::DatasetBuildError::Cancelled => ServiceError::Cancelled,
        market_squawk_data::DatasetBuildError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        market_squawk_data::DatasetBuildError::LimitExceeded => ServiceError::ResourceExhausted,
        market_squawk_data::DatasetBuildError::EmptyDataset
        | market_squawk_data::DatasetBuildError::MissingValueRejected
        | market_squawk_data::DatasetBuildError::TemporalLeakage
        | market_squawk_data::DatasetBuildError::ComponentEvidenceMismatch => {
            ServiceError::Unavailable
        }
        _ => ServiceError::InvalidResult,
    }
}
