//! Source-coordinate recipes shared by the existing composition-owned feature publisher.

mod stock;
pub(super) use stock::prepare_investment_dataset;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use market_squawk_backtesting::RECOMMENDATION_TARGET_HORIZON_NANOS_V1;
use market_squawk_data::Sha256Digest;
use sha2::{Digest as _, Sha256};

use super::{DatasetPreparationError, MAXIMUM_EXAMPLES, MarketSeriesPoint};

pub(super) struct DatasetRecipeCoordinates {
    pub(super) identity: Sha256Digest,
    pub(super) label: &'static str,
    pub(super) coordinates: Vec<[usize; 3]>,
    pub(super) split_counts: [usize; 3],
    pub(super) observed_points: usize,
}

/// Both recipes select coordinates before inspecting return values. The annual recipe requires
/// exact terminal timestamps and purges every label crossing the preselected time partitions.
pub(super) fn recipes(
    series: Sha256Digest,
    points: &[MarketSeriesPoint],
) -> Result<Vec<DatasetRecipeCoordinates>, DatasetPreparationError> {
    let mut recipes = Vec::with_capacity(2);
    if let Some(adjacent) = adjacent_recipe(series, points) {
        recipes.push(adjacent);
    }
    if let Some(annual) = annual_recipe(series, points)? {
        recipes.push(annual);
    }
    Ok(recipes)
}

fn adjacent_recipe(
    identity: Sha256Digest,
    points: &[MarketSeriesPoint],
) -> Option<DatasetRecipeCoordinates> {
    let mut horizons: BTreeMap<u64, Vec<[usize; 3]>> = BTreeMap::new();
    for (chunk_index, triple) in points.chunks_exact(3).enumerate() {
        let horizon = triple[2]
            .effective
            .unix_nanos()
            .checked_sub(triple[1].effective.unix_nanos())
            .and_then(|value| u64::try_from(value).ok());
        let Some(horizon) = horizon.filter(|value| *value > 0) else {
            continue;
        };
        let start = chunk_index * 3;
        horizons
            .entry(horizon)
            .or_default()
            .push([start, start + 1, start + 2]);
    }
    let (_, coordinates) =
        horizons
            .into_iter()
            .fold(None, |selected, candidate| match selected {
                None => Some(candidate),
                Some(current) if candidate.1.len() > current.1.len() => Some(candidate),
                Some(current)
                    if candidate.1.len() == current.1.len() && candidate.0 < current.0 =>
                {
                    Some(candidate)
                }
                Some(current) => Some(current),
            })?;
    let count = coordinates.len();
    if count < 3 {
        return None;
    }
    Some(DatasetRecipeCoordinates {
        identity,
        label: "Price returns with economic context",
        coordinates,
        split_counts: [count / 3, count / 3, count - 2 * (count / 3)],
        observed_points: count * 3,
    })
}

fn annual_recipe(
    series: Sha256Digest,
    points: &[MarketSeriesPoint],
) -> Result<Option<DatasetRecipeCoordinates>, DatasetPreparationError> {
    let mut candidates = Vec::new();
    for (current_index, current) in points.iter().enumerate().skip(1) {
        let Ok(target_at) = current
            .effective
            .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
        else {
            continue;
        };
        let Ok(terminal_index) = points.binary_search_by_key(&target_at, |point| point.effective)
        else {
            continue;
        };
        let terminal = &points[terminal_index];
        // Local acquisition after the historical target cannot manufacture a historical input.
        // The unchanged dataset builder additionally reselects every source at these real clocks.
        if terminal_index <= current_index
            || current.available_at >= target_at
            || current.available_at >= terminal.available_at
        {
            continue;
        }
        candidates.push([current_index - 1, current_index, terminal_index]);
    }
    let (Some(first), Some(last)) = (candidates.first(), candidates.last()) else {
        return Ok(None);
    };
    let starts_at = i128::from(points[first[1]].available_at.unix_nanos());
    let ends_at = i128::from(points[last[2]].available_at.unix_nanos());
    let span = ends_at - starts_at;
    if span <= 0 {
        return Ok(None);
    }
    let train_boundary = starts_at + span / 3;
    let validation_boundary = starts_at + 2 * span / 3;
    let mut partitions: [Vec<[usize; 3]>; 3] = std::array::from_fn(|_| Vec::new());
    for coordinate in candidates {
        let origin = i128::from(points[coordinate[1]].available_at.unix_nanos());
        let label_available = i128::from(points[coordinate[2]].available_at.unix_nanos());
        let partition = if label_available <= train_boundary {
            Some(0)
        } else if origin > train_boundary && label_available <= validation_boundary {
            Some(1)
        } else if origin > validation_boundary && label_available <= ends_at {
            Some(2)
        } else {
            None
        };
        if let Some(partition) = partition {
            partitions[partition].push(coordinate);
        }
    }
    if partitions.iter().any(Vec::is_empty) {
        return Ok(None);
    }
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(MAXIMUM_EXAMPLES)
        .map_err(|_| DatasetPreparationError::Capacity)?;
    let mut split_counts = [0; 3];
    for (partition, candidates) in partitions.into_iter().enumerate() {
        let maximum = MAXIMUM_EXAMPLES / 3 + usize::from(partition < MAXIMUM_EXAMPLES % 3);
        let retained = candidates.len().min(maximum);
        split_counts[partition] = retained;
        // Even deterministic sampling retains the complete time extent, without choosing on
        // labels, performance, or a later study result.
        for index in 0..retained {
            let selected = if retained == 1 {
                0
            } else {
                index * (candidates.len() - 1) / (retained - 1)
            };
            coordinates.push(candidates[selected]);
        }
    }
    let observed_points = coordinates
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .len();
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/exact-365-day-purged-time-thirds-recipe/v1\0");
    identity.update(series.bytes());
    identity.update(RECOMMENDATION_TARGET_HORIZON_NANOS_V1.to_be_bytes());
    Ok(Some(DatasetRecipeCoordinates {
        identity: Sha256Digest::new(identity.finalize().into()),
        label: "365-day price returns with economic context",
        coordinates,
        split_counts,
        observed_points,
    }))
}

use super::{
    CanonicalSupport, DatasetPreparationAuthority, DatasetPreparationUse, EvidencePart,
    FeatureDatasetProductionFinalizer, MAXIMUM_GENERATIONS, MAXIMUM_OBSERVATIONS_PER_GENERATION,
    MAXIMUM_QUERY_BYTES, PreparedFeatureDatasetBuild, PreparedProductionEvidence, action_plan,
    adjustment_evidence, aggregate_evidence, check_control, component_content_evidence,
    dataset_request, evidence_digest, instrument_population_query_evidence, market_bar_family,
    membership_evidence, plan_audit_evidence, push_parent, read_macro_feature_vector,
    return_component, return_kernel_evidence, short_hex, split_adjusted_return,
    timestamp_calendar_date,
};
use market_squawk_data::{
    AnalyticalObservationTemplate, AnalyticalReadLimit, CanonicalMarketBarHistoryRequest,
    ChronologicalSplitPolicy, CompleteMarketBarHistoryCursor, ComponentKind, ComponentScope,
    ComponentValue, CorporateActionAdjustment, CorporateActionPolicy, CorporateActionSensitivity,
    DatasetBuildInputs, DatasetBuildPolicy, DatasetBuildPurpose, DatasetExample,
    DatasetManifestRef, DatasetSchemaRegistry, DatasetStudyPolicy, FeatureDatasetProductContract,
    FeatureLabelComponentSpec, MarketHistorySelectionPolicy, MissingValuePolicy,
    PointInTimeCandidate, PointInTimeLimits, PointInTimePolicy, PointInTimeRequest,
    PointInTimeRevisionMode, PointInTimeService,
};
use market_squawk_domain::{
    CalendarDate, HistoricalStudyBasis, InstrumentId, MarketBarAdjustment, ResearchObservation,
    ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

const MAXIMUM_COHORT_EXAMPLES: usize = 2 * MAXIMUM_OBSERVATIONS_PER_GENERATION;
const MAXIMUM_COHORT_SOURCE_BYTES: usize = 96 * 1024 * 1024;
const MAXIMUM_COHORT_MACRO_BYTES: usize = 32 * 1024 * 1024;
const MAXIMUM_COHORT_COMPONENT_BYTES: usize = 96 * 1024 * 1024;

/// Predeclared source, economic population, and partitions. These are requests only; the existing
/// data authority reselects every actual source and mints the sealed input epochs after building.
#[derive(Clone, Debug)]
pub(crate) struct RecommendationCohortPreparationRequest {
    pub(crate) subject_instrument: InstrumentId,
    pub(crate) subject_manifest: DatasetManifestRef,
    pub(crate) primary_benchmark_instrument: InstrumentId,
    pub(crate) primary_benchmark_manifest: DatasetManifestRef,
    pub(crate) study: DatasetStudyPolicy,
    pub(crate) population_starts_at: Timestamp,
    /// Inclusive authentic completed-close population anchor, shared across every purpose/fold.
    pub(crate) population_ends_at: Timestamp,
    pub(crate) split: ChronologicalSplitPolicy,
    pub(crate) source_action_reference: Option<
        crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
    >,
}

pub(super) struct CohortPreparationRequest {
    pub(super) subject_instrument: InstrumentId,
    pub(super) subject_manifest: DatasetManifestRef,
    pub(super) benchmark: Option<(InstrumentId, DatasetManifestRef)>,
    pub(super) study: DatasetStudyPolicy,
    pub(super) population_starts_at: Timestamp,
    pub(super) population_ends_at: Timestamp,
    pub(super) split: ChronologicalSplitPolicy,
    pub(super) source_action_reference: Option<
        crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
    >,
    pub(super) event: Option<market_squawk_data::ProbabilityEventTarget>,
    pub(super) probability_subject: Option<market_squawk_data::ProbabilityEventTarget>,
    pub(super) costs: Option<Arc<market_squawk_backtesting::AllOriginRoundTripEvaluationV1>>,
}

pub(super) struct PreparedSourceCohort {
    identity: Sha256Digest,
    inputs: DatasetBuildInputs,
    policy: DatasetBuildPolicy,
    examples: usize,
    evidence: PreparedProductionEvidence,
    pub(super) coverage: super::probability::ProbabilityCohortCoverage,
}

impl DatasetPreparationAuthority {
    /// Prepares one genuine subject/primary-benchmark cohort through the existing publisher.
    /// StudyInputs retains all common source origins, without requiring or exposing target labels.
    pub(crate) async fn prepare_recommendation_cohort(
        &self,
        request: RecommendationCohortPreparationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        if request
            .study
            .target_horizon()
            .exact_elapsed()
            .map(|horizon| horizon.as_nanos())
            != Some(RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u128)
        {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        let training = request.study.purpose() == DatasetBuildPurpose::Training;
        let contract = if training {
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
        } else {
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
        };
        let cohort = self
            .prepare_source_cohort(
                CohortPreparationRequest {
                    subject_instrument: request.subject_instrument,
                    subject_manifest: request.subject_manifest,
                    benchmark: Some((
                        request.primary_benchmark_instrument,
                        request.primary_benchmark_manifest,
                    )),
                    study: request.study,
                    population_starts_at: request.population_starts_at,
                    population_ends_at: request.population_ends_at,
                    split: request.split,
                    source_action_reference: request.source_action_reference,
                    event: None,
                    probability_subject: None,
                    costs: None,
                },
                deadline,
                &cancellation,
            )
            .await?;
        self.finalize_source_cohort(
            &cohort,
            if training {
                DatasetPreparationUse::Train
            } else {
                DatasetPreparationUse::LocalAnalysis
            },
            contract,
            deadline,
            &cancellation,
        )
    }

    pub(super) async fn prepare_source_cohort(
        &self,
        request: CohortPreparationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedSourceCohort, DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let boundaries = request
            .split
            .timestamp_boundaries()
            .ok_or(DatasetPreparationError::InvalidSelection)?;
        if request
            .benchmark
            .as_ref()
            .is_some_and(|(instrument, _)| request.subject_instrument == *instrument)
            || request.population_starts_at >= boundaries[0]
            || request.population_starts_at >= request.population_ends_at
            || request.population_ends_at > request.study.snapshot_as_of()
            || boundaries[2] > request.study.snapshot_as_of()
            || request
                .study
                .target_horizon()
                .exact_elapsed()
                .is_none_or(|horizon| horizon.is_zero() || horizon.as_nanos() > i64::MAX as u128)
        {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        let limit = AnalyticalReadLimit::try_new(MAXIMUM_GENERATIONS)
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let page = self
            .reader
            .datasets(None, limit, deadline, &cancellation)
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        if page.has_more() {
            return Err(DatasetPreparationError::Capacity);
        }
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let mut retained_bytes = 0_usize;
        let mut generations = Vec::new();
        for generation in page
            .generations()
            .iter()
            .filter(|value| value.manifest().schema() == &canonical)
        {
            check_control(deadline, &cancellation)?;
            let mut templates = vec![
                (
                    AnalyticalObservationTemplate::UniverseMembership,
                    Vec::new(),
                ),
                (
                    AnalyticalObservationTemplate::CorporateAction,
                    std::iter::once(request.subject_instrument)
                        .chain(
                            request
                                .benchmark
                                .as_ref()
                                .map(|(instrument, _)| *instrument),
                        )
                        .collect(),
                ),
            ];
            let mut market_instruments = Vec::new();
            if generation.manifest() == &request.subject_manifest {
                market_instruments.push(request.subject_instrument);
            }
            if let Some((instrument, manifest)) = &request.benchmark {
                if generation.manifest() == manifest {
                    market_instruments.push(*instrument);
                }
            }
            if !market_instruments.is_empty() {
                templates.push((AnalyticalObservationTemplate::MarketBar, market_instruments));
            }
            let mut observations = Vec::new();
            for (template, instruments) in templates {
                let (mut selected, observation_bytes) = self
                    .observation_selection(
                        generation,
                        template,
                        instruments,
                        deadline,
                        cancellation.child_token(),
                    )
                    .await?;
                retained_bytes = retained_bytes
                    .checked_add(observation_bytes)
                    .filter(|bytes| *bytes <= MAXIMUM_COHORT_SOURCE_BYTES)
                    .ok_or(DatasetPreparationError::Capacity)?;
                observations.append(&mut selected);
            }
            generations.push((generation.clone(), observations));
        }
        let support = CanonicalSupport::from_generations(&generations)?;
        let mut sources = Vec::with_capacity(2);
        for (instrument, manifest) in
            std::iter::once((request.subject_instrument, &request.subject_manifest)).chain(
                request
                    .benchmark
                    .as_ref()
                    .map(|(instrument, manifest)| (*instrument, manifest)),
            )
        {
            let (_, observations) = generations
                .iter()
                .find(|(generation, _)| generation.manifest() == manifest)
                .ok_or(DatasetPreparationError::StaleCatalog)?;
            sources.push(
                select_series(
                    self,
                    observations,
                    manifest,
                    instrument,
                    request.study.snapshot_as_of(),
                    request.study.basis(),
                    deadline,
                    &cancellation,
                )
                .await?,
            );
        }
        drop(generations);
        if let Some(reference) = &request.source_action_reference {
            let reader = &self.source_actions;
            for (lane, source) in sources.iter_mut().enumerate() {
                if source.nominal_history.is_some() {
                    continue;
                }
                let instrument = if lane == 0 {
                    request.subject_instrument
                } else {
                    request
                        .benchmark
                        .as_ref()
                        .ok_or(DatasetPreparationError::InvalidEvidence)?
                        .0
                };
                let history = reader
                    .read_history_reference(
                        reference,
                        instrument,
                        deadline,
                        cancellation.child_token(),
                        None,
                    )
                    .await
                    .map_err(super::map_source_action_error)?
                    .ok_or(DatasetPreparationError::Unavailable)?;
                if history.read_receipt().knowledge_cutoff() != request.study.snapshot_as_of()
                    || history.selection().receipt().date_windows().is_some()
                    || history.bar_count() != source.points.len()
                {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                let mut bars = history.bars();
                for point in &source.points {
                    let bar = bars
                        .next()
                        .transpose()
                        .map_err(|_| DatasetPreparationError::InvalidEvidence)?
                        .ok_or(DatasetPreparationError::InvalidEvidence)?;
                    if bar != point.observation {
                        return Err(DatasetPreparationError::InvalidEvidence);
                    }
                }
                if bars
                    .next()
                    .transpose()
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?
                    .is_some()
                {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                drop(bars);
                source.completed_history = Some(history);
            }
        }
        let native = sources
            .iter()
            .filter_map(|source| {
                source
                    .nominal_history
                    .as_ref()
                    .or(source.completed_history.as_ref())
            })
            .collect::<Vec<_>>();
        let source_plan = if native.is_empty() {
            None
        } else {
            let reference = request
                .source_action_reference
                .as_ref()
                .ok_or(DatasetPreparationError::Unavailable)?;
            if reference.knowledge_cutoff() != request.study.snapshot_as_of() {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            let reader = &self.source_actions;
            let source = reader
                .read_price_reference_for_histories(
                    reference,
                    &native,
                    deadline,
                    cancellation.child_token(),
                    None,
                )
                .await
                .map_err(super::map_source_action_error)?
                .ok_or(DatasetPreparationError::Unavailable)?;
            let plan = source
                .into_covered_price_plan()
                .map_err(super::map_source_action_error)?;
            retained_bytes
                .checked_add(plan.retained_bytes())
                .filter(|bytes| *bytes <= MAXIMUM_COHORT_SOURCE_BYTES)
                .ok_or(DatasetPreparationError::Capacity)?;
            Some(Arc::new(plan))
        };
        drop(native);
        let output = build_cohort(
            self,
            &request,
            &sources,
            &support,
            None,
            source_plan.as_ref(),
            deadline,
            &cancellation,
        )
        .await?;
        Ok(output)
    }

    pub(super) fn finalize_source_cohort(
        &self,
        cohort: &PreparedSourceCohort,
        use_case: DatasetPreparationUse,
        contract: FeatureDatasetProductContract,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let build = dataset_request(
            cohort.identity,
            use_case,
            cohort.inputs.clone(),
            cohort.policy.clone(),
            cohort.examples,
        )?;
        for parent in build.parent_manifests() {
            let latest = self
                .reader
                .latest(parent.dataset_id(), deadline, cancellation)
                .map_err(|_| DatasetPreparationError::Unavailable)?
                .ok_or(DatasetPreparationError::StaleCatalog)?;
            if latest.manifest() != parent {
                return Err(DatasetPreparationError::StaleCatalog);
            }
        }
        self.research
            .analytical()
            .dataset_builder()
            .validate_request_authority(&build, cancellation)
            .map_err(|_| DatasetPreparationError::Authority)?;
        Ok(PreparedFeatureDatasetBuild {
            finalizer: FeatureDatasetProductionFinalizer {
                contract,
                build_spec: build.build_spec_digest().digest(),
                evidence: Some(cohort.evidence.clone()),
                maximum_currentness_expires_at: None,
            },
            request: build,
        })
    }
}

/// The actual selector resolves revisions under the declared snapshot, never by row order.
async fn select_series(
    authority: &DatasetPreparationAuthority,
    observations: &[ResearchObservation],
    manifest: &DatasetManifestRef,
    instrument: InstrumentId,
    snapshot: Timestamp,
    basis: HistoricalStudyBasis,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CohortSeries, DatasetPreparationError> {
    let candidates: Vec<_> = observations
        .iter()
        .filter(|value| {
            matches!(value,
        ResearchObservation::MarketBar(bar) if bar.adjustment() == MarketBarAdjustment::Raw
            && bar.context().provenance().instrument_id() == Some(instrument))
        })
        .cloned()
        .map(|value| PointInTimeCandidate::new(value, manifest.clone()))
        .collect();
    if candidates.iter().any(|candidate| {
        matches!(candidate.observation(),
        ResearchObservation::MarketBar(bar) if bar.time_semantics().nominal_daily_date().is_some())
    }) {
        return select_nominal_series(
            authority,
            &candidates,
            manifest,
            instrument,
            snapshot,
            basis,
            deadline,
            cancellation,
        )
        .await;
    }
    let count = candidates.len().max(1);
    if count > MAXIMUM_OBSERVATIONS_PER_GENERATION {
        return Err(DatasetPreparationError::Capacity);
    }
    let policy = PointInTimePolicy::try_new(
        std::num::NonZeroU32::MIN,
        PointInTimeRevisionMode::LatestKnown,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let limits =
        PointInTimeLimits::try_new(count, count, count.min(256), count, MAXIMUM_QUERY_BYTES)
            .map_err(|_| DatasetPreparationError::Capacity)?;
    let request = PointInTimeRequest::try_new(
        policy,
        snapshot,
        None,
        ResearchTemporalCoordinate::exact(snapshot),
        None,
        limits,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let selection = PointInTimeService::new()
        .select(&request, &candidates, cancellation, deadline)
        .await
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let mut points = Vec::with_capacity(selection.records().len());
    for record in selection.records() {
        let ResearchObservation::MarketBar(bar) = record.candidate().observation() else {
            return Err(DatasetPreparationError::InvalidEvidence);
        };
        let provenance = bar.context().provenance();
        let available_at = provenance
            .availability()
            .conservative_available_at()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let effective = bar
            .completed_at()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if provenance.received_at() > snapshot
            || provenance.ingested_at() > snapshot
            || effective > snapshot
            || available_at < effective
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        points.push(MarketSeriesPoint {
            observation: bar.clone(),
            effective,
            available_at,
            manifest: manifest.clone(),
            session_evidence: bar
                .time_semantics()
                .session()
                .ok_or(DatasetPreparationError::InvalidEvidence)?
                .evidence(),
        });
    }
    points.sort_unstable_by_key(|point| point.effective);
    if points.len() < 2
        || points.windows(2).any(|pair| {
            pair[0].effective == pair[1].effective
                || pair[0].observation.interval() != pair[1].observation.interval()
                || pair[0].observation.feed() != pair[1].observation.feed()
                || pair[0].observation.currency() != pair[1].observation.currency()
                || pair[0].observation.time_semantics().session()
                    != pair[1].observation.time_semantics().session()
        })
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    if basis == HistoricalStudyBasis::HistoricalAsKnown {
        for point in &mut points {
            let family = market_bar_family(point)?;
            let earliest = candidates
                .iter()
                .filter(|candidate| candidate.family_key().is_ok_and(|value| value == family))
                .filter_map(|candidate| {
                    let ResearchObservation::MarketBar(bar) = candidate.observation() else {
                        return None;
                    };
                    let available = bar
                        .context()
                        .provenance()
                        .availability()
                        .conservative_available_at()?;
                    (available <= snapshot).then_some((available, bar))
                })
                .min_by_key(|(available, _)| *available)
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
            point.observation = earliest.1.clone();
            point.available_at = earliest.0;
        }
    }
    drop(selection);
    Ok(CohortSeries {
        points,
        candidates,
        nominal_history: None,
        completed_history: None,
        nominal_calendar_manifest: None,
        nominal_snapshot: None,
    })
}

/// Reads nominal daily bars through the actual canonical history/calendar authority. The
/// provider date remains on every observation; only the independent session supplies an instant.
async fn select_nominal_series(
    authority: &DatasetPreparationAuthority,
    candidates: &[PointInTimeCandidate],
    manifest: &DatasetManifestRef,
    instrument: InstrumentId,
    snapshot: Timestamp,
    basis: HistoricalStudyBasis,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CohortSeries, DatasetPreparationError> {
    if basis != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
        || candidates.len() > MAXIMUM_OBSERVATIONS_PER_GENERATION
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    let mut dates = BTreeSet::new();
    for candidate in candidates {
        let ResearchObservation::MarketBar(bar) = candidate.observation() else {
            return Err(DatasetPreparationError::InvalidEvidence);
        };
        let date = bar
            .time_semantics()
            .nominal_daily_date()
            .ok_or(DatasetPreparationError::InvalidEvidence)?
            .date();
        dates.insert(date);
    }
    let (&start, &end) = dates
        .first()
        .zip(dates.last())
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let request = CanonicalMarketBarHistoryRequest::try_exact_nominal(
        instrument,
        start,
        end,
        MarketHistorySelectionPolicy::COMPLETE_DAILY_RAW_V1,
        snapshot,
        manifest.clone(),
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let history = authority
        .reader
        .read_canonical_market_bar_history_cursor(request, deadline, cancellation.child_token())
        .await
        .map_err(|_| DatasetPreparationError::Unavailable)?
        .ok_or(DatasetPreparationError::Unavailable)?;
    if history.selection().pinned().manifest() != manifest
        || history.selection().receipt().instrument_id() != instrument
        || history.bar_count() > MAXIMUM_OBSERVATIONS_PER_GENERATION
        || history.bar_count() < 2
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    nominal_series_from_history(authority, history, instrument, snapshot, deadline, cancellation)
        .await
}

async fn nominal_series_from_history(
    authority: &DatasetPreparationAuthority,
    history: CompleteMarketBarHistoryCursor,
    instrument: InstrumentId,
    snapshot: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CohortSeries, DatasetPreparationError> {
    let manifest = history.selection().pinned().manifest().clone();
    let (history, calendar) = authority
        .rejoin_nominal_history(history, deadline, cancellation)
        .await?;
    let native = history
        .native_sessions()
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let mut points = Vec::with_capacity(history.bar_count());
    for bar in history.bars() {
        check_control(deadline, cancellation)?;
        let bar = bar.map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let date = bar
            .time_semantics()
            .nominal_daily_date()
            .ok_or(DatasetPreparationError::InvalidEvidence)?
            .date();
        let session = native
            .sessions()
            .find_date(date)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let available_at = bar
            .context()
            .provenance()
            .availability()
            .conservative_available_at()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if !session.bar_present()
            || session.provider_timestamp().is_some()
            || session.provider_period().is_some()
            || bar.completed_at().is_some()
            || session.opens_at() >= session.closes_at_exclusive()
            || session.closes_at_exclusive() > snapshot
            || available_at > snapshot
            || available_at < session.closes_at_exclusive()
            || bar.context().time().effective().calendar_date_value() != Some(date)
            || bar.adjustment() != MarketBarAdjustment::Raw
            || bar.context().provenance().instrument_id() != Some(instrument)
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let date_text = date.to_string();
        let session_evidence = evidence_digest(
            b"market-squawk/cohort-original-nominal-session/v1",
            &[
                EvidencePart::Manifest(&manifest),
                EvidencePart::Sha256(history.read_receipt().result_digest()),
                EvidencePart::Sha256(history.read_receipt().history_content_digest()),
                EvidencePart::Sha256(history.selection().receipt().receipt_digest()),
                EvidencePart::Digest(native.mapping_digest()),
                EvidencePart::Digest(native.source_replay_digest()),
                EvidencePart::Digest(native.capture_receipt_digest()),
                EvidencePart::Text(&date_text),
                EvidencePart::Timestamp(session.opens_at()),
                EvidencePart::Timestamp(session.closes_at_exclusive()),
            ],
        );
        points.push(MarketSeriesPoint {
            observation: bar.clone(),
            manifest: manifest.clone(),
            session_evidence,
            effective: session.closes_at_exclusive(),
            available_at,
        });
    }
    if points.windows(2).any(|pair| {
        pair[0].effective >= pair[1].effective
            || source_date(&pair[0]).ok() >= source_date(&pair[1]).ok()
    }) {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    // Nominal selections already retain the exact complete history and source-issued epochs.
    // No duplicate revision-candidate array is needed by their coordinate lookup.
    Ok(CohortSeries {
        points,
        candidates: Vec::new(),
        nominal_history: Some(history),
        completed_history: None,
        nominal_calendar_manifest: Some(calendar.source_action_calendar().manifest().clone()),
        nominal_snapshot: Some(snapshot),
    })
}

fn source_date(point: &MarketSeriesPoint) -> Result<CalendarDate, DatasetPreparationError> {
    match point.observation.time_semantics().nominal_daily_date() {
        Some(date) => Ok(date.date()),
        None => timestamp_calendar_date(point.effective),
    }
}

fn source_coordinate(point: &MarketSeriesPoint) -> ResearchTemporalCoordinate {
    match point.observation.time_semantics().nominal_daily_date() {
        Some(date) => ResearchTemporalCoordinate::calendar_date(date.date()),
        None => ResearchTemporalCoordinate::exact(point.effective),
    }
}

struct CohortSeries {
    points: Vec<MarketSeriesPoint>,
    candidates: Vec<PointInTimeCandidate>,
    nominal_history: Option<CompleteMarketBarHistoryCursor>,
    completed_history: Option<CompleteMarketBarHistoryCursor>,
    nominal_calendar_manifest: Option<DatasetManifestRef>,
    nominal_snapshot: Option<Timestamp>,
}

impl std::ops::Deref for CohortSeries {
    type Target = [MarketSeriesPoint];
    fn deref(&self) -> &Self::Target {
        &self.points
    }
}

/// Resolves exactly the selected families again at the common cohort knowledge cutoff. The
/// observed values used in return arithmetic are these real selected revisions.
async fn select_coordinate_sources(
    series: &CohortSeries,
    indices: &[usize],
    cutoff: Timestamp,
    origin: Timestamp,
    terminal: Option<Timestamp>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<MarketSeriesPoint>, DatasetPreparationError> {
    if series.nominal_history.is_some() {
        if series.nominal_snapshot != Some(cutoff) {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        return indices
            .iter()
            .map(|index| {
                series
                    .points
                    .get(*index)
                    .cloned()
                    .ok_or(DatasetPreparationError::InvalidEvidence)
            })
            .collect();
    }
    let families = indices
        .iter()
        .map(|index| market_bar_family(&series[*index]))
        .collect::<Result<Vec<_>, _>>()?;
    let candidates = series
        .candidates
        .iter()
        .filter(|candidate| {
            candidate
                .family_key()
                .is_ok_and(|family| families.contains(&family))
        })
        .cloned()
        .collect::<Vec<_>>();
    let count = candidates.len().max(1);
    let policy = PointInTimePolicy::try_new(
        std::num::NonZeroU32::MIN,
        PointInTimeRevisionMode::LatestKnown,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let limits =
        PointInTimeLimits::try_new(count, count, count.min(256), count, MAXIMUM_QUERY_BYTES)
            .map_err(|_| DatasetPreparationError::Capacity)?;
    let request = PointInTimeRequest::try_new(
        policy,
        cutoff,
        None,
        ResearchTemporalCoordinate::exact(origin),
        terminal.map(ResearchTemporalCoordinate::exact),
        limits,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let selected = PointInTimeService::new()
        .select(&request, &candidates, cancellation, deadline)
        .await
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let mut result = Vec::with_capacity(indices.len());
    for (index, family) in indices.iter().zip(families) {
        let mut matches = selected.records().iter().filter(|record| {
            record
                .candidate()
                .family_key()
                .is_ok_and(|value| value == family)
        });
        let record = matches
            .next()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if matches.next().is_some() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let ResearchObservation::MarketBar(bar) = record.candidate().observation() else {
            return Err(DatasetPreparationError::InvalidEvidence);
        };
        if bar.completed_at() != Some(series[*index].effective) {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        result.push(MarketSeriesPoint {
            observation: bar.clone(),
            effective: series[*index].effective,
            manifest: series[*index].manifest.clone(),
            session_evidence: series[*index].session_evidence,
            available_at: bar
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .ok_or(DatasetPreparationError::InvalidEvidence)?,
        });
    }
    Ok(result)
}

#[allow(
    clippy::too_many_arguments,
    reason = "historical and fixed population authority share the existing source recipe"
)]
async fn build_cohort(
    authority: &DatasetPreparationAuthority,
    request: &CohortPreparationRequest,
    sources: &[CohortSeries],
    support: &CanonicalSupport,
    population: Option<&market_squawk_data::CurrentListedPopulationPartition>,
    source_plan: Option<&Arc<market_squawk_data::CorporateActionPlan>>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PreparedSourceCohort, DatasetPreparationError> {
    let horizon = request
        .study
        .target_horizon()
        .exact_elapsed()
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .filter(|value| *value > 0)
        .ok_or(DatasetPreparationError::InvalidSelection)?;
    let training = request.study.purpose() == DatasetBuildPurpose::Training;
    let contract = if training {
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
    } else {
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
    };
    let feature_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        contract.feature_component_name(),
        std::num::NonZeroU32::MIN,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let label_spec = training
        .then(|| {
            FeatureLabelComponentSpec::try_new(
                ComponentKind::Label,
                ComponentScope::Instrument,
                CorporateActionSensitivity::RequiresAdjustment,
                contract.label_component_name(),
                std::num::NonZeroU32::MIN,
            )
        })
        .transpose()
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let pit = PointInTimePolicy::try_new(
        std::num::NonZeroU32::MIN,
        PointInTimeRevisionMode::LatestKnown,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let adjustment = CorporateActionPolicy::new(
        CorporateActionAdjustment::SplitAdjusted,
        std::num::NonZeroU32::MIN,
    );
    let boundaries = request
        .split
        .timestamp_boundaries()
        .ok_or(DatasetPreparationError::InvalidSelection)?;
    let instruments = std::iter::once(request.subject_instrument)
        .chain(
            request
                .benchmark
                .as_ref()
                .map(|(instrument, _)| *instrument),
        )
        .collect::<Vec<_>>();
    if sources.len() != instruments.len() || sources.is_empty() {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    if request.event.is_some()
        && sources.len() == 2
        && sources[0].nominal_history.is_some() != sources[1].nominal_history.is_some()
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    let mut coverage = super::probability::ProbabilityCohortCoverage::default();
    let mut all_parents = Vec::new();
    push_parent(&mut all_parents, &request.subject_manifest)?;
    if let Some((_, manifest)) = &request.benchmark {
        push_parent(&mut all_parents, manifest)?;
    }
    if let Some(evaluation) = &request.costs {
        push_parent(&mut all_parents, evaluation.dataset_manifest())?;
        for row in evaluation.results() {
            push_parent(&mut all_parents, row.source_manifest())?;
        }
    }
    for source in sources {
        match (&source.nominal_history, &source.nominal_calendar_manifest) {
            (Some(history), Some(calendar)) => {
                push_parent(&mut all_parents, history.read_receipt().origin_manifest())?;
                push_parent(&mut all_parents, calendar)?;
            }
            (None, None) => {}
            _ => return Err(DatasetPreparationError::InvalidEvidence),
        }
    }
    if let Some(plan) = source_plan {
        let coverage = plan
            .source_split_admission()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        for manifest in coverage.source_manifests() {
            push_parent(&mut all_parents, manifest)?;
        }
    }
    let actions = instruments
        .iter()
        .map(|instrument| support.actions_for(*instrument))
        .collect::<Vec<_>>();
    for candidate in actions.iter().flatten() {
        push_parent(&mut all_parents, candidate.source_manifest())?;
    }
    let mut coordinates = Vec::new();
    for (subject_index, current) in sources[0].iter().enumerate().skip(1) {
        if current.effective < request.population_starts_at
            || current.effective > request.population_ends_at
        {
            continue;
        }
        let benchmark_index = sources
            .get(1)
            .and_then(|source| {
                source
                    .binary_search_by_key(&current.effective, |point| point.effective)
                    .ok()
            })
            .filter(|index| *index > 0);
        if request.event.is_none() && sources.len() == 2 && benchmark_index.is_none() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let decision = match request.study.basis() {
            HistoricalStudyBasis::HistoricalAsKnown => {
                if request.event.is_none() {
                    benchmark_index.map_or(current.available_at, |index| {
                        current.available_at.max(sources[1][index].available_at)
                    })
                } else {
                    current.available_at
                }
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => current
                .effective
                .checked_add_nanos(
                    i64::try_from(
                        request
                            .study
                            .decision_lag()
                            .ok_or(DatasetPreparationError::InvalidEvidence)?
                            .as_nanos(),
                    )
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
                )
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        };
        if decision > request.study.snapshot_as_of() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let target = current
            .effective
            .checked_add_nanos(horizon)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        if decision >= target {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        coordinates.push(([Some(subject_index), benchmark_index], decision, target));
        if coordinates.len() > MAXIMUM_COHORT_EXAMPLES / 2 {
            return Err(DatasetPreparationError::Capacity);
        }
    }
    if coordinates.is_empty() {
        return Err(if request.event.is_some() {
            DatasetPreparationError::Unavailable
        } else {
            DatasetPreparationError::InvalidEvidence
        });
    }
    coverage.original_origins = coordinates.len();
    if let Some(evaluation) = &request.costs {
        if evaluation.results().len() != coordinates.len() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let mut observed_origins = BTreeSet::new();
        for row in evaluation.results() {
            check_control(deadline, cancellation)?;
            let index = coordinates
                .binary_search_by_key(&Some(row.target_origin()), |(indices, _, _)| {
                    indices[0]
                        .and_then(|index| sources[0].get(index))
                        .map(|point| point.effective)
                })
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            if !observed_origins.insert(row.target_origin())
                || row.instrument_id() != request.subject_instrument
                || row.source_manifest() != &request.subject_manifest
                || row.decision_at() != coordinates[index].1
                || row.target_at() != coordinates[index].2
            {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
        }
    }
    let first_source = match request.study.basis() {
        HistoricalStudyBasis::HistoricalAsKnown => coordinates
            .iter()
            .map(|coordinate| coordinate.1)
            .min()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot => request.study.snapshot_as_of(),
    };
    let last_source = match request.study.basis() {
        HistoricalStudyBasis::HistoricalAsKnown => coordinates
            .iter()
            .map(|coordinate| coordinate.1)
            .max()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot => request.study.snapshot_as_of(),
    };
    let mut memberships = Vec::new();
    if let Some(population) = population {
        if population.instrument_ids() != instruments
            || population.population().membership_as_of() != request.study.snapshot_as_of()
            || request.study.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
    } else {
        for instrument in instruments.iter().copied() {
            let membership = membership_evidence(
                &support.memberships,
                instrument,
                first_source,
                last_source,
                cancellation,
            )?
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
            push_parent(&mut all_parents, &membership.manifest)?;
            memberships.push(membership);
        }
        if memberships
            .iter()
            .any(|membership| membership.universe_id != memberships[0].universe_id)
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
    }
    let mut macro_parents = Vec::new();
    let mut macros = BTreeMap::new();
    let mut macro_bytes = 0_usize;
    // Freeze macro dependencies across the complete population before purging Training rows.
    // Both purposes consequently retain the same source snapshot, including censored origins.
    for (indices, decision, _) in &coordinates {
        check_control(deadline, cancellation)?;
        let cutoff = if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
            *decision
        } else {
            request.study.snapshot_as_of()
        };
        let key = (
            cutoff,
            source_date(&sources[0][indices[0].ok_or(DatasetPreparationError::InvalidEvidence)?])?,
        );
        if !macros.contains_key(&key) {
            let vector = read_macro_feature_vector(
                &authority.macro_context,
                cutoff,
                key.1,
                deadline,
                cancellation.child_token(),
            )
            .await
            .map_err(|_| DatasetPreparationError::Unavailable)?;
            for parent in vector.parent_manifests() {
                push_parent(&mut macro_parents, parent)?;
                push_parent(&mut all_parents, parent)?;
            }
            let component_bytes =
                vector
                    .components()
                    .iter()
                    .try_fold(0_usize, |bytes, component| {
                        bytes
                            .checked_add(
                                component
                                    .retained_bytes()
                                    .map_err(|_| DatasetPreparationError::Capacity)?,
                            )
                            .ok_or(DatasetPreparationError::Capacity)
                    })?;
            macro_bytes = macro_bytes
                .checked_add(component_bytes)
                .and_then(|bytes| bytes.checked_add(256))
                .filter(|bytes| *bytes <= MAXIMUM_COHORT_MACRO_BYTES)
                .ok_or(DatasetPreparationError::Capacity)?;
            macros.insert(
                key,
                (
                    vector.components().to_vec(),
                    vector.downstream_evidence_digest(),
                    component_bytes,
                ),
            );
        }
    }
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/predeclared-recommendation-cohort/v1");
    identity.update(horizon.to_be_bytes());
    identity.update(request.subject_instrument.as_uuid().as_bytes());
    if let Some((instrument, _)) = &request.benchmark {
        identity.update(instrument.as_uuid().as_bytes());
    }
    if let Some(event) = request.event {
        identity.update(b"probability-event/v1");
        identity.update(event.digest().bytes());
        for (indices, decision, target) in &coordinates {
            identity.update(
                sources[0][indices[0].ok_or(DatasetPreparationError::InvalidEvidence)?]
                    .effective
                    .unix_nanos()
                    .to_be_bytes(),
            );
            identity.update(decision.unix_nanos().to_be_bytes());
            identity.update(target.unix_nanos().to_be_bytes());
        }
        if let Some(evaluation) = &request.costs {
            identity.update(evaluation.cohort_digest().bytes());
        }
    }
    if let Some(event) = request.probability_subject {
        identity.update(b"probability-subject/v1");
        identity.update(event.digest().bytes());
    }
    if let Some(partition) = population {
        let population = partition.population();
        identity.update(b"present-day-fixed-cohort/v1");
        identity.update(population.content_digest().bytes());
        identity.update(population.audit_digest().bytes());
        identity.update(population.financial_profile_digest().bytes());
    }
    identity.update(request.population_starts_at.unix_nanos().to_be_bytes());
    identity.update(request.population_ends_at.unix_nanos().to_be_bytes());
    identity.update(request.study.snapshot_as_of().unix_nanos().to_be_bytes());
    for boundary in boundaries {
        identity.update(boundary.unix_nanos().to_be_bytes());
    }
    identity.update([match request.study.basis() {
        HistoricalStudyBasis::HistoricalAsKnown => 1,
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot => 2,
    }]);
    identity.update(
        request
            .study
            .decision_lag()
            .map_or(0, |lag| lag.as_nanos())
            .to_be_bytes(),
    );
    for parent in &all_parents {
        super::hash_manifest(&mut identity, parent);
    }
    let identity = Sha256Digest::new(identity.finalize().into());
    let mut examples = Vec::new();
    let mut component_bytes = 0_usize;
    let mut feature_content = Vec::new();
    let mut feature_audit = Vec::new();
    let mut label_content = Vec::new();
    let mut label_audit = Vec::new();
    let mut macro_evidence = Vec::new();
    let mut sessions = Vec::new();
    let mut returns = Vec::new();
    let mut split_counts = [0_usize; 3];
    for (coordinate, (indices, decision, target)) in coordinates.iter().enumerate() {
        check_control(deadline, cancellation)?;
        let source_cutoff = if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
            *decision
        } else {
            request.study.snapshot_as_of()
        };
        let partition = match boundaries.iter().position(|end| decision <= end) {
            Some(partition) => partition,
            // The common population and all parents have already been frozen. A model's own
            // Training export contains only its predeclared partitions; StudyInputs keeps every
            // original coordinate and therefore must admit its complete source-known interval.
            None if training => {
                coverage.outside_partitions += 1;
                continue;
            }
            None => return Err(DatasetPreparationError::InvalidEvidence),
        };
        if sources.len() == 2
            && (indices[1].is_none()
                || (request.event.is_some()
                    && (sources[1][indices[1].ok_or(DatasetPreparationError::InvalidEvidence)?]
                        .available_at
                        > source_cutoff
                        || sources[1]
                            [indices[1].ok_or(DatasetPreparationError::InvalidEvidence)? - 1]
                            .available_at
                            > source_cutoff
                        || sources[0]
                            [indices[0].ok_or(DatasetPreparationError::InvalidEvidence)?]
                        .observation
                        .currency()
                            != sources[1]
                                [indices[1].ok_or(DatasetPreparationError::InvalidEvidence)?]
                            .observation
                            .currency())))
        {
            coverage.missing_benchmark += 1;
            continue;
        }
        let terminals = (0..sources.len())
            .map(|lane| {
                if training {
                    sources[lane]
                        .binary_search_by_key(target, |point| point.effective)
                        .ok()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let cost = if let Some(evaluation) = &request.costs {
            let origin =
                sources[0][indices[0].ok_or(DatasetPreparationError::InvalidEvidence)?].effective;
            let Some(value) = super::probability::cost_attestation(
                evaluation,
                request.subject_instrument,
                origin,
                *target,
                *decision,
                source_cutoff,
            )?
            else {
                coverage.unavailable_cost_outcome += 1;
                continue;
            };
            Some(value)
        } else {
            None
        };
        let label_cutoff = if training {
            let Some(subject_terminal) = terminals[0] else {
                coverage.missing_subject_terminal += 1;
                continue;
            };
            if terminals.iter().skip(1).any(Option::is_none) {
                coverage.missing_benchmark += 1;
                continue;
            }
            let mut actual_known = sources[0][subject_terminal].available_at;
            for lane in 1..sources.len() {
                actual_known = actual_known.max(
                    sources[lane]
                        [terminals[lane].ok_or(DatasetPreparationError::InvalidEvidence)?]
                    .available_at,
                );
            }
            if let Some(value) = &cost {
                actual_known = actual_known.max(value.label_available_at);
            }
            let known = if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
                actual_known
            } else {
                request.study.snapshot_as_of()
            };
            let window_end =
                if let Some(market_squawk_data::ProbabilityEventTarget::ProfitAfterCosts {
                    policy,
                }) = request.event
                {
                    target
                        .checked_add_nanos(policy.maximum_exit_lag_nanos)
                        .map_err(|_| DatasetPreparationError::InvalidEvidence)?
                } else {
                    *target
                };
            let purge = if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
                known.max(window_end)
            } else {
                window_end
            };
            if purge > boundaries[partition]
                || *target > request.study.snapshot_as_of()
                || known > request.study.snapshot_as_of()
            {
                coverage.boundary_censored += 1;
                continue;
            }
            Some(known)
        } else {
            None
        };
        let mut coordinate_examples = Vec::with_capacity(sources.len());
        let mut comparison_currency = None;
        for lane in 0..sources.len() {
            let index = indices[lane].ok_or(DatasetPreparationError::InvalidEvidence)?;
            let selected = select_coordinate_sources(
                &sources[lane],
                &[index - 1, index],
                source_cutoff,
                sources[lane][index].effective,
                None,
                deadline,
                cancellation,
            )
            .await?;
            let prior = &selected[0];
            let current = &selected[1];
            if request.event.is_some() {
                let currency = current.observation.currency();
                if prior.observation.currency() != currency
                    || comparison_currency.is_some_and(|value| value != currency)
                {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                comparison_currency = Some(currency);
            }
            let valuation_cutoff = if sources[lane].completed_history.is_some() {
                current.effective
            } else if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
                source_cutoff
            } else {
                *decision
            };
            let feature_plan = if source_plan.is_some() {
                super::project_source_price_plan(
                    source_plan.ok_or(DatasetPreparationError::InvalidEvidence)?,
                    instruments[lane],
                    source_cutoff,
                    valuation_cutoff,
                    prior,
                    current,
                    deadline,
                    cancellation,
                )?
            } else {
                action_plan(
                    &actions[lane],
                    pit,
                    adjustment,
                    valuation_cutoff,
                    source_cutoff,
                    ResearchTemporalCoordinate::exact(*decision),
                    None,
                    deadline,
                    cancellation,
                )
                .await?
            };
            let feature_return = split_adjusted_return(prior, current, &feature_plan)?;
            let feature = return_component(
                feature_spec.clone(),
                feature_return,
                vec![market_bar_family(prior)?, market_bar_family(current)?],
                source_coordinate(current),
                None,
                adjustment_evidence(&feature_plan)?,
            )?;
            let vector = macros
                .get(&(source_cutoff, source_date(current)?))
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
            component_bytes = component_bytes
                .checked_add(
                    feature
                        .retained_bytes()
                        .map_err(|_| DatasetPreparationError::Capacity)?,
                )
                .and_then(|bytes| bytes.checked_add(vector.2))
                .filter(|bytes| *bytes <= MAXIMUM_COHORT_COMPONENT_BYTES)
                .ok_or(DatasetPreparationError::Capacity)?;
            let mut components = vec![feature.clone()];
            components.extend(vector.0.iter().cloned());
            if let (Some(spec), Some(known), Some(terminal_index)) =
                (&label_spec, label_cutoff, terminals[lane])
            {
                let selected_terminal = select_coordinate_sources(
                    &sources[lane],
                    &[terminal_index],
                    known,
                    current.effective,
                    Some(*target),
                    deadline,
                    cancellation,
                )
                .await?;
                let terminal = &selected_terminal[0];
                if request.event.is_some()
                    && terminal.observation.currency() != current.observation.currency()
                {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                let valuation_cutoff =
                    if request.study.basis() == HistoricalStudyBasis::HistoricalAsKnown {
                        known
                    } else {
                        *target
                    };
                let plan = if source_plan.is_some() {
                    super::project_source_price_plan(
                        source_plan.ok_or(DatasetPreparationError::InvalidEvidence)?,
                        instruments[lane],
                        known,
                        valuation_cutoff,
                        current,
                        terminal,
                        deadline,
                        cancellation,
                    )?
                } else {
                    action_plan(
                        &actions[lane],
                        pit,
                        adjustment,
                        valuation_cutoff,
                        known,
                        ResearchTemporalCoordinate::exact(*target),
                        None,
                        deadline,
                        cancellation,
                    )
                    .await?
                };
                let value = split_adjusted_return(current, terminal, &plan)?;
                let label = return_component(
                    spec.clone(),
                    value,
                    vec![market_bar_family(terminal)?],
                    source_coordinate(current),
                    Some(source_coordinate(terminal)),
                    adjustment_evidence(&plan)?,
                )?;
                label_content.push(component_content_evidence(&label));
                label_audit.push(plan_audit_evidence(&plan));
                returns.push(return_kernel_evidence(
                    feature_return,
                    value,
                    &feature_plan,
                    &plan,
                ));
                component_bytes = component_bytes
                    .checked_add(
                        label
                            .retained_bytes()
                            .map_err(|_| DatasetPreparationError::Capacity)?,
                    )
                    .filter(|bytes| *bytes <= MAXIMUM_COHORT_COMPONENT_BYTES)
                    .ok_or(DatasetPreparationError::Capacity)?;
                components.push(label);
            } else {
                returns.push(evidence_digest(
                    b"market-squawk/study-feature-return-output/v1",
                    &[
                        EvidencePart::Text(&feature_return.normalize().to_string()),
                        EvidencePart::Sha256(feature_plan.content_hash()),
                        EvidencePart::Sha256(feature_plan.audit_hash()),
                    ],
                ));
            }
            let example_id = format!("cohort-{}-{coordinate:05}-{lane}", short_hex(identity));
            let example = if let Some(history) = &sources[lane].nominal_history {
                let source_plan = source_plan.ok_or(DatasetPreparationError::InvalidEvidence)?;
                history
                    .try_nominal_daily_dataset_example(
                        &example_id,
                        source_date(current)?,
                        request.study,
                        components,
                        deadline,
                        cancellation,
                    )
                    .and_then(|example| example.try_with_source_price_plan(Arc::clone(source_plan)))
            } else if let Some(history) = &sources[lane].completed_history {
                let source_plan = source_plan.ok_or(DatasetPreparationError::InvalidEvidence)?;
                history
                    .try_timestamp_history_dataset_example(
                        &example_id,
                        request.study,
                        *decision,
                        *target,
                        components,
                        deadline,
                        cancellation,
                    )
                    .and_then(|example| example.try_with_source_price_plan(Arc::clone(source_plan)))
            } else {
                DatasetExample::try_new_with_temporal_cutoffs(
                    example_id,
                    instruments[lane],
                    source_cutoff,
                    label_cutoff,
                    *decision,
                    ResearchTemporalCoordinate::exact(current.effective),
                    ResearchTemporalCoordinate::exact(*target),
                    components,
                )
            }
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            coordinate_examples.push(example);
            feature_content.push(component_content_evidence(&feature));
            feature_audit.push(plan_audit_evidence(&feature_plan));
            macro_evidence.push(vector.1);
            sessions.push(evidence_digest(
                b"market-squawk/study-completed-source-session/v1",
                &[
                    EvidencePart::Timestamp(current.effective),
                    EvidencePart::Timestamp(*decision),
                    EvidencePart::Timestamp(*target),
                    EvidencePart::Digest(current.session_evidence),
                ],
            ));
        }
        if let Some(event) = request.event {
            let benchmark = coordinate_examples.get(1).cloned();
            let subject = coordinate_examples
                .into_iter()
                .next()
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
            let example = subject
                .try_derive_probability_label(event, benchmark.as_ref(), cost)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            let label = example
                .components()
                .iter()
                .find(|value| value.spec().kind() == ComponentKind::Label)
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
            label_content.push(component_content_evidence(label));
            returns.push(evidence_digest(
                b"market-squawk/original-probability-event-output/v1",
                &[
                    EvidencePart::Sha256(event.digest()),
                    EvidencePart::Digest(component_content_evidence(label)),
                ],
            ));
            let features = example
                .components()
                .iter()
                .filter(|value| value.spec().kind() == ComponentKind::Feature)
                .collect::<Vec<_>>();
            let expected_names = std::iter::once(contract.feature_component_name())
                .chain(
                    contract
                        .macro_components()
                        .iter()
                        .map(|value| value.component_name()),
                )
                .collect::<Vec<_>>();
            if features.len() != expected_names.len()
                || expected_names.iter().any(|name| {
                    features
                        .iter()
                        .filter(|value| value.spec().name() == *name)
                        .count()
                        != 1
                })
            {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            coverage.feature_count = expected_names.len();
            if features
                .iter()
                .all(|value| matches!(value.value(), ComponentValue::Decimal { .. }))
            {
                let class = match label.value() {
                    ComponentValue::Decimal { value, .. } if *value == Decimal::ZERO => 0,
                    ComponentValue::Decimal { value, .. } if *value == Decimal::ONE => 1,
                    _ => return Err(DatasetPreparationError::InvalidEvidence),
                };
                coverage.complete_classes[partition][class] += 1;
            } else {
                coverage.missing_features += 1;
            }
            examples.push(example);
            split_counts[partition] += 1;
        } else {
            split_counts[partition] += coordinate_examples.len();
            examples.extend(coordinate_examples);
        }
    }
    coverage.retained_examples = examples.len();
    coverage.split_counts = split_counts;
    if request.event.is_some() {
        let counts = [
            coverage.original_origins,
            coverage.outside_partitions,
            coverage.missing_subject_terminal,
            coverage.missing_benchmark,
            coverage.unavailable_cost_outcome,
            coverage.boundary_censored,
            coverage.retained_examples,
        ];
        if counts[1..].iter().sum::<usize>() != coverage.original_origins {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let mut bytes = Vec::with_capacity(counts.len() * 8);
        for count in counts
            .into_iter()
            .chain(coverage.complete_classes.into_iter().flatten())
            .chain([coverage.missing_features, coverage.feature_count])
        {
            bytes.extend_from_slice(&(count as u64).to_be_bytes());
        }
        returns.push(evidence_digest(
            b"market-squawk/probability-original-cohort-coverage/v1",
            &[EvidencePart::Bytes(&bytes)],
        ));
    }
    if examples.is_empty() || split_counts.contains(&0) {
        return Err(if request.event.is_some() {
            DatasetPreparationError::Unavailable
        } else {
            DatasetPreparationError::InvalidEvidence
        });
    }
    let mut specs = vec![feature_spec];
    for descriptor in contract.macro_components() {
        specs.push(
            FeatureLabelComponentSpec::try_new(
                ComponentKind::Feature,
                ComponentScope::Global,
                CorporateActionSensitivity::NotApplicable,
                descriptor.component_name(),
                std::num::NonZeroU32::MIN,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        );
    }
    if let Some(spec) = label_spec {
        specs.push(if let Some(event) = request.event {
            FeatureLabelComponentSpec::try_new(
                ComponentKind::Label,
                ComponentScope::Instrument,
                CorporateActionSensitivity::RequiresAdjustment,
                event.label_component_name(),
                std::num::NonZeroU32::MIN,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?
        } else {
            spec
        });
    }
    let example_count = examples.len();
    let inputs = if let Some(population) = population {
        DatasetBuildInputs::try_new_for_current_population(
            all_parents,
            population.clone(),
            specs,
            examples,
            Vec::new(),
        )
    } else {
        DatasetBuildInputs::try_new(
            all_parents,
            memberships[0].universe_id.clone(),
            memberships
                .iter()
                .map(|membership| membership.value.clone())
                .collect(),
            specs,
            examples,
        )
    }
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let inputs = match request.probability_subject {
        Some(event) => inputs
            .try_with_probability_subject(event, request.subject_instrument)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        None => inputs,
    };
    let policy = DatasetBuildPolicy::new(
        request.split,
        pit,
        adjustment,
        if request.event.is_some() || request.probability_subject.is_some() {
            MissingValuePolicy::Preserve
        } else {
            MissingValuePolicy::Reject
        },
        SourceIdentifier::try_from(
            request
                .event
                .map_or(contract, |event| {
                    super::probability::event_contract(event, DatasetPreparationUse::Train)
                })
                .implementation_revision(),
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        Some(request.study),
    );
    let aggregate = |domain, values: &Vec<_>| aggregate_evidence(domain, values);
    let mut evidence = PreparedProductionEvidence {
        universe_membership_content: aggregate_evidence(
            b"market-squawk/cohort-universe-content/v1",
            &memberships
                .iter()
                .map(|value| value.content)
                .collect::<Vec<_>>(),
        ),
        universe_membership_audit: aggregate_evidence(
            b"market-squawk/cohort-universe-audit/v1",
            &memberships
                .iter()
                .map(|value| value.audit)
                .collect::<Vec<_>>(),
        ),
        instrument_population_query: aggregate_evidence(
            b"market-squawk/cohort-population-query/v1",
            &instruments
                .iter()
                .map(|instrument| {
                    instrument_population_query_evidence(*instrument, first_source, last_source)
                })
                .collect::<Vec<_>>(),
        ),
        instrument_population_receipt: aggregate_evidence(
            b"market-squawk/cohort-population-receipt/v1",
            &memberships
                .iter()
                .map(|value| value.receipt)
                .collect::<Vec<_>>(),
        ),
        completed_session_request: aggregate(b"market-squawk/cohort-session-request/v1", &sessions),
        completed_session_receipt: aggregate(b"market-squawk/cohort-session-receipt/v1", &sessions),
        feature_point_in_time_content: aggregate(
            b"market-squawk/feature-pit-content-set/v1",
            &feature_content,
        ),
        feature_point_in_time_audit: aggregate(
            b"market-squawk/feature-pit-audit-set/v1",
            &feature_audit,
        ),
        macro_context_evidence: aggregate(
            b"market-squawk/macro-context-evidence-set/v1",
            &macro_evidence,
        ),
        macro_parent_manifests: macro_parents.into_boxed_slice(),
        label_point_in_time_content: training
            .then(|| aggregate(b"market-squawk/label-pit-content-set/v1", &label_content)),
        label_point_in_time_audit: training
            .then(|| aggregate(b"market-squawk/label-pit-audit-set/v1", &label_audit)),
        return_kernel_output: aggregate(b"market-squawk/return-kernel-output-set/v1", &returns),
    };
    if let Some(partition) = population {
        let population = partition.population();
        evidence.universe_membership_content = super::EvidenceDigest::new(
            super::DigestAlgorithm::Sha256,
            population.content_digest().bytes(),
        );
        evidence.universe_membership_audit = super::EvidenceDigest::new(
            super::DigestAlgorithm::Sha256,
            population.audit_digest().bytes(),
        );
        evidence.instrument_population_query = super::EvidenceDigest::new(
            super::DigestAlgorithm::Sha256,
            population.source_population_digest().bytes(),
        );
        evidence.instrument_population_receipt = evidence_digest(
            b"market-squawk/native-stock-fixed-population/v1",
            &[
                EvidencePart::Sha256(population.content_digest()),
                EvidencePart::Sha256(population.audit_digest()),
                EvidencePart::Sha256(population.financial_profile_digest()),
                EvidencePart::Sha256(population.official_directory_digest()),
                EvidencePart::Bytes(&partition.descriptor().partition_digest()),
                EvidencePart::Timestamp(population.membership_as_of()),
            ],
        );
    }
    Ok(PreparedSourceCohort {
        identity,
        inputs,
        policy,
        examples: example_count,
        evidence,
        coverage,
    })
}
