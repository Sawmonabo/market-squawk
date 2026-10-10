//! Durable saved-screen preparation over the complete exact current feature population.

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_analytics::{FeatureCompatibility, FeatureKey, FeatureRegistry, StatisticalF64};
use market_squawk_data::{
    AnalyticalReadCapability, CurrentListedPopulation, CurrentPopulationInputUnavailable,
    DatasetBuildPurpose, DatasetId, DatasetManifestRef, DatasetPopulationBasis,
    DatasetPopulationPartition, DatasetSchemaRef, DatasetSchemaRegistry, FeatureDatasetInputEpoch,
    FeatureDatasetInputEpochOutput, FeatureDatasetProductContract, ForecastFeatureRow,
    ForecastFeatureValue, QueryLimits, ResearchUse, ResearchUseLimits, ResearchUseRequest,
    Sha256Digest,
};
use market_squawk_decisions::{
    AsOfSemantics, CandidateFlag, CandidateId, CandidateInput, ComparisonOperator,
    DecisionContentDigest, NullPolicy, RankingDirection, SavedScreen, ScreenConstraints,
    ScreenFeatureBinding, ScreenFeatureObservation, ScreenId, ScreenPredicate, ScreenRanking,
    ScreenRevision, ScreenRun, ScreenRunId,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, HistoricalStudyBasis, RevisionNumber,
    SchemaVersion, SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::DecisionApplicationError;
use super::codec::candidate::CandidateInputWire;
use super::codec::screen::RunWire;
use crate::ResearchService;
use crate::application::{
    analytical_profile::ValidatedAnalyticalProfile,
    market_calendar::{
        CompletedMarketSessionRead, CompletedMarketSessionReference,
        CompletedMarketSessionResolution,
    },
    research::{CurrentFindScreenPartition, FindPopulationReference},
};

const MAXIMUM_SCREEN_DATASET_ROWS: usize = 100_000;
const MAXIMUM_SCREEN_DATASET_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_SCREEN_OBSERVATIONS: usize = 262_144;
const LIQUIDITY_FEATURE_NAME: &str = "liquidity.available-quantity";
const SCREEN_INPUT_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-job-input/v1\0";
const SCREEN_DATASET_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-dataset-evidence/v1\0";
const SCREEN_CANDIDATE_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-candidate-evidence/v1\0";
const SCREEN_RUN_ID_DOMAIN: &[u8] = b"market-squawk/screen-run-id/v1\0";

/// Predeclared candidate discovery by observed price movement. The resulting score is neither
/// forecast return nor investment confidence; complete deep analysis ranks final opportunities.
/// The supplied dataset is a genuine reopened publication, not a caller-authored universe digest.
pub(crate) fn prepare_price_movement_find_screen(
    partitions: &[CurrentFindScreenPartition],
    maximum_candidates: usize,
    registry: &FeatureRegistry,
) -> Result<SavedScreen, ScreenWorkflowError> {
    let population = validate_current_partition_set(partitions)?;
    if !matches!(maximum_candidates, 8 | 16 | 32) {
        return Err(ScreenWorkflowError::InvalidRequest);
    }
    let key = FeatureKey::try_new(
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
            .feature_component_name(),
        NonZeroU32::MIN,
    )
    .map_err(|_| ScreenWorkflowError::InvalidRequest)?;
    let metadata = registry
        .try_resolve(&key, FeatureCompatibility::PointInTime)
        .map_err(|_| ScreenWorkflowError::InvalidRequest)?;
    let binding = ScreenFeatureBinding::new(key, metadata.semantic_digest());
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/find-price-movement-screen/v1\0");
    identity.update(population.content_digest().bytes());
    identity.update(binding.semantic_digest().as_bytes());
    identity.update((maximum_candidates as u64).to_be_bytes());
    let identity: [u8; 32] = identity.finalize().into();
    let revision = ScreenRevision::new(
        ScreenId::try_new(format!("find-price-movement.{}", hex(&identity)))
            .map_err(|_| ScreenWorkflowError::InvalidRequest)?,
        RevisionNumber::new(1).map_err(|_| ScreenWorkflowError::InvalidRequest)?,
    );
    let finite =
        |value| StatisticalF64::try_new(value).map_err(|_| ScreenWorkflowError::InvalidRequest);
    SavedScreen::try_new(
        revision,
        content_identity(population.content_digest().bytes())?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        // Positive source prices imply simple return >= -1. This neutral predicate retains
        // negative observed returns as well as positive ones; missing values remain excluded.
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThanOrEqual,
            finite(-1.0)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(binding, RankingDirection::Descending),
        NonZeroUsize::new(maximum_candidates).ok_or(ScreenWorkflowError::InvalidRequest)?,
        ScreenConstraints::try_new(finite(1.0)?, finite(0.0)?, vec![DataQuality::Modeled])
            .map_err(|_| ScreenWorkflowError::InvalidRequest)?,
        registry,
    )
    .map_err(|_| ScreenWorkflowError::InvalidRequest)
}

/// Distinct original authorities entering the same saved-screen pipeline.
/// Current listed snapshots require the complete source-issued partition set.
#[derive(Clone, Debug)]
pub(crate) enum ScreenDatasetSelection {
    PublishedDataset(DatasetManifestRef),
    CurrentFind(Box<[CurrentFindScreenPartition]>),
}
impl ScreenDatasetSelection {
    fn manifests(&self) -> Vec<&DatasetManifestRef> {
        match self {
            Self::PublishedDataset(manifest) => vec![manifest],
            Self::CurrentFind(partitions) => partitions
                .iter()
                .filter_map(|value| {
                    value
                        .dataset()
                        .map(|dataset| dataset.generation().manifest())
                })
                .collect(),
        }
    }
    fn current_partitions(&self) -> Option<&[CurrentFindScreenPartition]> {
        match self {
            Self::CurrentFind(partitions) => Some(partitions),
            Self::PublishedDataset(_) => None,
        }
    }
}

/// Minimal presentation request for a service-owned saved-screen execution.
#[derive(Clone)]
pub struct ScreenJobRequest {
    screen_id: ScreenId,
    screen_revision: RevisionNumber,
    datasets: ScreenDatasetSelection,
    as_of: Timestamp,
    calendar: CompletedMarketSessionRead,
    profile: ValidatedAnalyticalProfile,
    research: Arc<ResearchService>,
}

impl fmt::Debug for ScreenJobRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScreenJobRequest")
            .field("screen_id", &self.screen_id)
            .field("as_of", &self.as_of)
            .field("calendar", self.calendar.reference())
            .field("profile", self.profile.resolution())
            .finish_non_exhaustive()
    }
}

impl ScreenJobRequest {
    /// Selects original feature publications with their actual calendar, profile and rights reader.
    #[must_use]
    pub(crate) const fn new(
        screen_id: ScreenId,
        screen_revision: RevisionNumber,
        datasets: ScreenDatasetSelection,
        as_of: Timestamp,
        calendar: CompletedMarketSessionRead,
        profile: ValidatedAnalyticalProfile,
        research: Arc<ResearchService>,
    ) -> Self {
        Self {
            screen_id,
            screen_revision,
            datasets,
            as_of,
            calendar,
            profile,
            research,
        }
    }

    pub(super) const fn screen_id(&self) -> &ScreenId {
        &self.screen_id
    }

    pub(super) const fn screen_revision(&self) -> RevisionNumber {
        self.screen_revision
    }

    pub(super) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
}

/// Durable locator returned only after the complete immutable job input is committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedScreenJob {
    input_identity: SourceIdentifier,
    input_digest: EvidenceDigest,
    run_id: ScreenRunId,
    population_member_count: usize,
    population_unavailable: Box<[CurrentPopulationInputUnavailable]>,
}

impl AdmittedScreenJob {
    /// Exact durable screen-input identity used by the job authority.
    #[must_use]
    pub const fn input_identity(&self) -> &SourceIdentifier {
        &self.input_identity
    }

    /// Commitment to the complete service-derived screen input.
    #[must_use]
    pub const fn input_digest(&self) -> EvidenceDigest {
        self.input_digest
    }

    /// Exact immutable run that will be published by the decision authority.
    #[must_use]
    pub const fn run_id(&self) -> &ScreenRunId {
        &self.run_id
    }

    /// Full source-qualified population, before missing features and financial ranking.
    pub const fn population_member_count(&self) -> usize {
        self.population_member_count
    }

    /// Actual source-input gaps retained independently of the ranked candidates.
    pub fn population_unavailable(&self) -> &[CurrentPopulationInputUnavailable] {
        &self.population_unavailable
    }
}

/// Screen preparation or durable-input lookup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScreenWorkflowError {
    /// The selected screen, cutoff, or supported constraint policy is invalid.
    InvalidRequest,
    /// The retained screen or exact prepared input does not exist.
    NotFound,
    /// The pinned dataset is unavailable, malformed, or does not match the screen universe.
    DatasetUnavailable,
    /// A stable run or input identity names different content.
    Conflict,
    /// A fixed preparation or retained-memory bound was exceeded.
    Capacity,
    /// The durable decision application could not complete the operation.
    Application(DecisionApplicationError),
}

impl fmt::Display for ScreenWorkflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest => formatter.write_str("screen job request is invalid"),
            Self::NotFound => formatter.write_str("screen job input was not found"),
            Self::DatasetUnavailable => {
                formatter.write_str("screen feature dataset is unavailable or inconsistent")
            }
            Self::Conflict => formatter.write_str("screen job input conflicts with retained state"),
            Self::Capacity => formatter.write_str("screen job preparation capacity is exhausted"),
            Self::Application(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for ScreenWorkflowError {}

impl From<DecisionApplicationError> for ScreenWorkflowError {
    fn from(error: DecisionApplicationError) -> Self {
        Self::Application(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScreenPublicationFence {
    manifest: DatasetManifestRef,
    object_graph_digest: EvidenceDigest,
    export_sha256: Sha256Digest,
    query_sha256: Sha256Digest,
    result_sha256: Sha256Digest,
    selected_rows: NonZeroU64,
    policy_sha256: Sha256Digest,
    universe_sha256: Sha256Digest,
    population_partition: Option<DatasetPopulationPartition>,
    research_decision: [u8; 32],
    research_graph: [u8; 32],
    research_expires_at: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ScreenDatasetFence {
    publications: Box<[ScreenPublicationFence]>,
    calendar_reference: CompletedMarketSessionReference,
    profile_digest: Sha256Digest,
    population_basis: DatasetPopulationBasis,
    population_member_count: usize,
    population_unavailable: Box<[CurrentPopulationInputUnavailable]>,
    population_partitions: Box<[DatasetPopulationPartition]>,
    population_reference: Option<FindPopulationReference>,
    research_expires_at: Timestamp,
    current_session_expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ScreenJobPlan {
    run: ScreenRun,
    candidates: Vec<CandidateInput>,
    selected_at: Timestamp,
    dataset: ScreenDatasetFence,
    input_digest: EvidenceDigest,
}

impl ScreenJobPlan {
    pub(super) const fn run(&self) -> &ScreenRun {
        &self.run
    }

    pub(super) const fn input_digest(&self) -> EvidenceDigest {
        self.input_digest
    }

    pub(super) fn into_execution(self) -> (ScreenRun, Vec<CandidateInput>, Timestamp) {
        (self.run, self.candidates, self.selected_at)
    }

    pub(super) fn admitted(&self) -> Result<AdmittedScreenJob, ScreenWorkflowError> {
        Ok(AdmittedScreenJob {
            input_identity: SourceIdentifier::try_from(self.run.id().as_str())
                .map_err(|_error| ScreenWorkflowError::InvalidRequest)?,
            input_digest: self.input_digest,
            run_id: self.run.id().clone(),
            population_member_count: self.dataset.population_member_count,
            population_unavailable: self.dataset.population_unavailable.clone(),
        })
    }

    pub(super) fn matches_request(&self, screen: &SavedScreen, request: &ScreenJobRequest) -> bool {
        expected_run_id(screen, request).is_ok_and(|expected| expected == *self.run.id())
    }
}

struct LatestFeatureRows<'a> {
    source_selection_as_of: Timestamp,
    decision_at: Timestamp,
    example_id: &'a str,
    components: BTreeMap<(&'a str, u32), &'a ForecastFeatureRow>,
}

pub(super) async fn prepare(
    screen: &SavedScreen,
    request: &ScreenJobRequest,
    reader: &AnalyticalReadCapability,
    selected_at: Timestamp,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<ScreenJobPlan, ScreenWorkflowError> {
    if screen.revision().id() != request.screen_id()
        || screen.revision().revision() != request.screen_revision()
        || request.as_of() > selected_at
        || (screen.constraints().minimum_liquidity().get() > 0.0
            && screen
                .feature_bindings()
                .iter()
                .filter(|binding| binding.key().name() == LIQUIDITY_FEATURE_NAME)
                .count()
                != 1)
        || !screen
            .constraints()
            .admitted_data_qualities()
            .contains(&DataQuality::Modeled)
    {
        return Err(ScreenWorkflowError::InvalidRequest);
    }
    ensure_screen_control(deadline, &cancellation)?;
    let current_parts = request.datasets.current_partitions();
    let population = current_parts
        .map(validate_current_partition_set)
        .transpose()?;
    if population.is_some_and(|population| {
        population.membership_as_of() != request.as_of()
            || population.financial_profile_digest().bytes()
                != analytical_profile_digest(&request.profile)
                    .map_or([0; 32], |value| value.bytes())
            || content_identity(population.content_digest().bytes()).ok()
                != Some(screen.universe_identity())
    }) {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    let manifests = request.datasets.manifests();
    // An entirely unavailable preparation retains coverage in its producer receipt. It cannot
    // mint an analytical publication or a successful screen without an admitted input generation.
    if manifests.is_empty()
        || manifests.len() > maximum_population_partitions()
        || manifests
            .iter()
            .enumerate()
            .any(|(index, manifest)| manifests[index + 1..].contains(manifest))
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    let run = ScreenRun::try_new(
        expected_run_id(screen, request)?,
        screen.revision().clone(),
        request.as_of(),
        dataset_identity(request)?,
        screen.universe_identity(),
        screen.feature_bindings().to_vec(),
    )
    .map_err(|_| ScreenWorkflowError::InvalidRequest)?;
    let mut screen_permits = Vec::new();
    screen_permits
        .try_reserve_exact(manifests.len())
        .map_err(|_| ScreenWorkflowError::Capacity)?;
    let mut publications = Vec::new();
    publications
        .try_reserve_exact(manifests.len())
        .map_err(|_| ScreenWorkflowError::Capacity)?;
    let mut candidates = Vec::new();
    let mut population_unavailable = Vec::new();
    let mut population_partitions = Vec::new();
    if let Some(parts) = current_parts {
        population_partitions
            .try_reserve_exact(parts.len())
            .map_err(|_| ScreenWorkflowError::Capacity)?;
        population_unavailable
            .try_reserve_exact(population.map_or(0, |p| p.instrument_ids().len()))
            .map_err(|_| ScreenWorkflowError::Capacity)?;
        for part in parts {
            population_partitions.push(part.partition().descriptor().clone());
            population_unavailable.extend_from_slice(part.unavailable());
        }
    }
    let mut member_count = population.map_or(0, |population| population.instrument_ids().len());
    let mut total_rows = 0_usize;
    let mut research_expires_at = Timestamp::from_unix_nanos(i64::MAX);
    let mut current_session_expires_at = research_expires_at;
    // Read and drop one partition at a time. Only the compact exact observations and publication
    // fences survive; retained raw histories and complete Arrow results never accumulate here.
    for manifest in manifests {
        ensure_screen_control(deadline, &cancellation)?;
        let authorization_duration = deadline
            .saturating_duration_since(Instant::now())
            .min(std::time::Duration::from_secs(5));
        if authorization_duration.is_zero() {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let roots = vec![manifest.clone()];
        let authorization = request
            .research
            .analytical()
            .authorize_research_use(
                ResearchUseRequest::try_new(
                    roots.clone(),
                    ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(
                        roots.len(),
                        4096,
                        8192,
                        4096,
                        4 * 1024 * 1024,
                        authorization_duration,
                        std::time::Duration::from_secs(300),
                    )
                    .map_err(|_| ScreenWorkflowError::Capacity)?,
                )
                .map_err(|_| ScreenWorkflowError::InvalidRequest)?,
                &cancellation,
            )
            .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?;
        if authorization.graph().roots().len() != roots.len()
            || !authorization
                .graph()
                .roots()
                .iter()
                .all(|root| roots.contains(root))
            || authorization.research_use() != ResearchUse::LocalAnalysis
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let research_decision = authorization.decision_digest().bytes();
        let research_graph = authorization.graph().digest().bytes();
        let publication_research_expires_at = authorization.expires_at();
        research_expires_at = research_expires_at.min(publication_research_expires_at);
        screen_permits.push(authorization.into_permit());
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        let limits = QueryLimits::try_new_with_inline_bytes(
            MAXIMUM_SCREEN_DATASET_ROWS as u64,
            MAXIMUM_SCREEN_DATASET_BYTES as u64,
            MAXIMUM_SCREEN_DATASET_BYTES as u64,
            MAXIMUM_SCREEN_DATASET_BYTES as u64,
            2,
            512,
            512,
            remaining.min(std::time::Duration::from_secs(60)),
        )
        .map_err(|_| ScreenWorkflowError::Capacity)?;
        let evidence = reader
            .feature_dataset_input_epochs(
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                manifest,
                limits,
                deadline,
                cancellation.clone(),
            )
            .await
            .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?;
        let dataset = evidence.dataset();
        let study = dataset
            .study_policy()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        if dataset.generation().manifest() != manifest
            || study.basis() != HistoricalStudyBasis::HistoricalAsKnown
            || study.purpose() != DatasetBuildPurpose::StudyInputs
            || study.snapshot_as_of() > request.as_of()
            || (current_parts.is_none()
                && content_identity(dataset.universe_digest().bytes())?
                    != screen.universe_identity())
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        match current_parts {
            Some(parts) => {
                let original = parts
                    .iter()
                    .find(|part| {
                        part.dataset()
                            .is_some_and(|dataset| dataset.generation().manifest() == manifest)
                    })
                    .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
                let expected = original
                    .dataset()
                    .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
                if dataset.population_basis() != DatasetPopulationBasis::CurrentListedSnapshot
                    || dataset.population_partition() != Some(original.partition().descriptor())
                    || dataset.population_member_count() != member_count
                    || dataset.population_unavailable() != original.unavailable()
                    || dataset.python_export_sha256() != expected.python_export_sha256()
                    || dataset.policy_digest() != expected.policy_digest()
                    || study.snapshot_as_of() != request.as_of()
                {
                    return Err(ScreenWorkflowError::DatasetUnavailable);
                }
            }
            None => {
                // A plain manifest cannot replace the original current population authority.
                if dataset.population_basis()
                    != DatasetPopulationBasis::PublishedHistoricalMembership
                    || dataset.population_partition().is_some()
                    || !dataset.population_unavailable().is_empty()
                {
                    return Err(ScreenWorkflowError::DatasetUnavailable);
                }
                member_count = dataset.population_member_count();
            }
        }
        total_rows = total_rows
            .checked_add(evidence.rows().len())
            .ok_or(ScreenWorkflowError::Capacity)?;
        if total_rows > MAXIMUM_SCREEN_DATASET_ROWS.saturating_mul(maximum_population_partitions())
        {
            return Err(ScreenWorkflowError::Capacity);
        }
        let calculated_at = screen_clock()?;
        if calculated_at < selected_at || calculated_at >= research_expires_at {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let current = current_epochs(&evidence, request, calculated_at, deadline, &cancellation)?;
        let mut partition_candidates =
            candidate_inputs(&run, screen, &evidence, &current, deadline, &cancellation)?;
        let combined = candidates
            .len()
            .checked_add(partition_candidates.len())
            .ok_or(ScreenWorkflowError::Capacity)?;
        if combined > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS
            || combined
                .checked_mul(screen.feature_bindings().len())
                .is_none_or(|count| count > MAXIMUM_SCREEN_OBSERVATIONS)
        {
            return Err(ScreenWorkflowError::Capacity);
        }
        current_session_expires_at = current_session_expires_at.min(
            current
                .values()
                .map(|(_, _, expiry)| *expiry)
                .min()
                .ok_or(ScreenWorkflowError::DatasetUnavailable)?,
        );
        candidates
            .try_reserve_exact(partition_candidates.len())
            .map_err(|_| ScreenWorkflowError::Capacity)?;
        candidates.append(&mut partition_candidates);
        publications.push(ScreenPublicationFence {
            manifest: manifest.clone(),
            object_graph_digest: evidence.query_output().object_graph_digest(),
            export_sha256: dataset.python_export_sha256(),
            query_sha256: Sha256Digest::new(evidence.query_output().query_identity().bytes()),
            result_sha256: Sha256Digest::new(evidence.query_output().result_digest().bytes()),
            selected_rows: NonZeroU64::new(evidence.rows().len() as u64)
                .ok_or(ScreenWorkflowError::DatasetUnavailable)?,
            policy_sha256: dataset.policy_digest(),
            universe_sha256: dataset.universe_digest(),
            population_partition: dataset.population_partition().cloned(),
            research_decision,
            research_graph,
            research_expires_at: publication_research_expires_at,
        });
    }
    candidates.sort_unstable_by_key(CandidateInput::instrument_id);
    population_unavailable.sort_unstable_by_key(CurrentPopulationInputUnavailable::instrument_id);
    let dataset = ScreenDatasetFence {
        publications: publications.into_boxed_slice(),
        calendar_reference: request.calendar.reference().clone(),
        profile_digest: analytical_profile_digest(&request.profile)?,
        population_basis: if population.is_some() {
            DatasetPopulationBasis::CurrentListedSnapshot
        } else {
            DatasetPopulationBasis::PublishedHistoricalMembership
        },
        population_member_count: member_count,
        population_unavailable: population_unavailable.into_boxed_slice(),
        population_partitions: population_partitions.into_boxed_slice(),
        population_reference: current_parts
            .and_then(|parts| parts.first())
            .map(|part| part.population_reference().clone()),
        research_expires_at,
        current_session_expires_at,
    };
    validate_population_inputs(&dataset, &candidates)?;
    ensure_screen_control(deadline, &cancellation)?;
    let completed_at = screen_clock()?;
    if completed_at < selected_at
        || completed_at >= research_expires_at
        || completed_at >= current_session_expires_at
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    let input_digest = plan_digest(&run, &candidates, completed_at, &dataset)?;
    Ok(ScreenJobPlan {
        run,
        candidates,
        selected_at: completed_at,
        dataset,
        input_digest,
    })
}

pub(super) fn expected_request_run_id(
    screen: &SavedScreen,
    request: &ScreenJobRequest,
) -> Result<ScreenRunId, ScreenWorkflowError> {
    expected_run_id(screen, request)
}

fn expected_run_id(
    screen: &SavedScreen,
    request: &ScreenJobRequest,
) -> Result<ScreenRunId, ScreenWorkflowError> {
    let mut hash = Sha256::new();
    hash.update(SCREEN_RUN_ID_DOMAIN);
    hash_bytes(&mut hash, screen.revision().id().as_str().as_bytes())?;
    hash.update(screen.revision().revision().get().to_be_bytes());
    hash_selection(&mut hash, &request.datasets)?;
    hash.update(request.as_of().unix_nanos().to_be_bytes());
    hash.update(analytical_profile_digest(&request.profile)?.bytes());
    hash_bytes(
        &mut hash,
        &serde_json::to_vec(request.calendar.reference())
            .map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    let digest: [u8; 32] = hash.finalize().into();
    ScreenRunId::try_new(format!("run.{}", hex(&digest)))
        .map_err(|_error| ScreenWorkflowError::Capacity)
}

fn current_epochs<'a>(
    evidence: &'a FeatureDatasetInputEpochOutput,
    request: &ScreenJobRequest,
    evaluated_at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    BTreeMap<
        market_squawk_domain::InstrumentId,
        (&'a FeatureDatasetInputEpoch, EvidenceDigest, Timestamp),
    >,
    ScreenWorkflowError,
> {
    let mut latest = BTreeMap::<_, (usize, &FeatureDatasetInputEpoch)>::new();
    for (index, epoch) in evidence.epochs().iter().enumerate() {
        ensure_screen_control(deadline, cancellation)?;
        let origin = epoch
            .target_origin()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        let decision = epoch
            .decision_at()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.basis() != HistoricalStudyBasis::HistoricalAsKnown
            || epoch.snapshot_as_of() > request.as_of()
            || epoch.source_selection_as_of() > decision
            || decision > request.as_of()
            || origin > decision
            || epoch.calculated_at() > evaluated_at
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let selected = latest.entry(epoch.instrument_id()).or_insert((index, epoch));
        let key = (decision, epoch.source_selection_as_of());
        let prior_key = (
            selected.1
                .decision_at()
                .ok_or(ScreenWorkflowError::DatasetUnavailable)?,
            selected.1.source_selection_as_of(),
        );
        if key > prior_key {
            *selected = (index, epoch);
        } else if key == prior_key && selected.1.example_id() != epoch.example_id() {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
    }
    if latest.is_empty() || latest.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    // The existing cohort authority binds source-authenticated nominal epochs to the original
    // reopened calendar. It preserves native regular closes separately from timestamp periods.
    let nominal_cohort = if latest.values().any(|(_, epoch)| epoch.named_session_origin().is_some()) {
        let horizon = request.profile.horizon().step_nanos()
            .and_then(|step| i64::try_from(step.get()).ok())
            .ok_or(ScreenWorkflowError::InvalidRequest)?;
        Some(request.calendar.latest_forecast_session_cohort(
            request.as_of(), horizon, evaluated_at, deadline, cancellation,
        ).map_err(|_| ScreenWorkflowError::DatasetUnavailable)?
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?)
    } else {
        None
    };
    let mut qualified = BTreeMap::new();
    for (instrument, (index, epoch)) in latest {
        ensure_screen_control(deadline, cancellation)?;
        let bar = epoch
            .market_bar()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        let age = request
            .as_of()
            .unix_nanos()
            .checked_sub(epoch.target_origin().ok_or(ScreenWorkflowError::DatasetUnavailable)?.unix_nanos())
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        // This is a completed feature-input age, not the live quote's separate sixty-second gate.
        if age < 0
            || age
                > request
                    .profile
                    .recommendation_policy()
                    .parameters()
                    .forecast_max_age_nanos
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        if epoch.named_session_origin().is_some() {
            let cohort = nominal_cohort.as_ref().ok_or(ScreenWorkflowError::DatasetUnavailable)?;
            let coordinate = evidence.coordinate(index).ok_or(ScreenWorkflowError::DatasetUnavailable)?;
            let origin = cohort.bind_input_epoch(coordinate)
                .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?
                .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
            let expires_at = request.calendar.currentness_expires_at();
            if !cohort.matches_origin(&origin) || expires_at <= evaluated_at {
                return Err(ScreenWorkflowError::DatasetUnavailable);
            }
            let mut digest = Sha256::new();
            digest.update(b"market-squawk/screen-nominal-session/v1\0");
            digest.update(cohort.reference().digest()
                .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?.bytes());
            digest.update(origin.source_origin_evidence_digest().bytes());
            qualified.insert(instrument, (epoch, EvidenceDigest::new(
                DigestAlgorithm::Sha256, digest.finalize().into()), expires_at));
            continue;
        }
        if epoch.fixed_horizon_origin_basis() != Some(market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose)
            || bar.completed_at() != epoch.target_origin() {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let venue = bar
            .context()
            .provenance()
            .venue_id()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        let session_request = request
            .calendar
            .authority()
            .request_for(
                venue,
                bar.interval(),
                request.as_of(),
                request.as_of(),
                evaluated_at,
            )
            .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        let CompletedMarketSessionResolution::Available(session) = request
            .calendar
            .authority()
            .resolve(session_request)
            .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?
        else {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        };
        if Some(session.period()) != bar.time_semantics().timestamped_period()
            || session.expires_at() <= evaluated_at
            || session.calendar_available_at() > request.as_of()
            || session.knowledge_available_at() > request.as_of()
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        qualified.insert(instrument, (epoch, session.digest(), session.expires_at()));
    }
    Ok(qualified)
}

pub(super) fn ensure_screen_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ScreenWorkflowError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(ScreenWorkflowError::DatasetUnavailable)
    } else {
        Ok(())
    }
}

fn screen_clock() -> Result<Timestamp, ScreenWorkflowError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(elapsed.as_nanos()).map_err(|_| ScreenWorkflowError::DatasetUnavailable)?,
    ))
}

fn candidate_inputs(
    run: &ScreenRun,
    screen: &SavedScreen,
    evidence: &FeatureDatasetInputEpochOutput,
    current: &BTreeMap<
        market_squawk_domain::InstrumentId,
        (&FeatureDatasetInputEpoch, EvidenceDigest, Timestamp),
    >,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<CandidateInput>, ScreenWorkflowError> {
    let mut latest = BTreeMap::<_, LatestFeatureRows<'_>>::new();
    for row in evidence.rows() {
        ensure_screen_control(deadline, cancellation)?;
        if row.component_kind() != 1 {
            continue;
        }
        let decision_at = row
            .decision_at()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        if row.component_version() == 0
            || row.label_selection_as_of().is_some()
            || row.source_selection_as_of() > decision_at
            || decision_at > run.as_of()
            || row
                .observed_effective_at()
                .is_some_and(|origin| origin > decision_at)
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let coordinate = (decision_at, row.source_selection_as_of());
        let entry = latest
            .entry(row.instrument_id())
            .or_insert_with(|| LatestFeatureRows {
                source_selection_as_of: row.source_selection_as_of(),
                decision_at,
                example_id: row.example_id(),
                components: BTreeMap::new(),
            });
        let selected_coordinate = (entry.decision_at, entry.source_selection_as_of);
        if coordinate > selected_coordinate {
            entry.source_selection_as_of = row.source_selection_as_of();
            entry.decision_at = decision_at;
            entry.example_id = row.example_id();
            entry.components.clear();
        }
        if coordinate == (entry.decision_at, entry.source_selection_as_of)
            && (row.example_id() != entry.example_id
                || entry
                    .components
                    .insert((row.component_name(), row.component_version()), row)
                    .is_some())
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
    }
    if latest.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS {
        return Err(ScreenWorkflowError::Capacity);
    }
    if latest.is_empty() {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    if latest
        .len()
        .checked_mul(screen.feature_bindings().len())
        .is_none_or(|observations| observations > MAXIMUM_SCREEN_OBSERVATIONS)
    {
        return Err(ScreenWorkflowError::Capacity);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(latest.len())
        .map_err(|_error| ScreenWorkflowError::Capacity)?;
    for (instrument_id, rows) in latest {
        let (epoch, session_digest, _) = current
            .get(&instrument_id)
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        if epoch.example_id() != rows.example_id
            || epoch.decision_at() != Some(rows.decision_at)
            || epoch.source_selection_as_of() != rows.source_selection_as_of
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(screen.feature_bindings().len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?;
        let mut present = 0_usize;
        let mut liquidity = None;
        let mut candidate_hash = Sha256::new();
        candidate_hash.update(SCREEN_CANDIDATE_DIGEST_DOMAIN);
        candidate_hash.update(run.dataset_identity().evidence_digest().bytes());
        candidate_hash.update(instrument_id.as_uuid().as_bytes());
        candidate_hash.update(session_digest.bytes());
        candidate_hash.update(rows.source_selection_as_of.unix_nanos().to_be_bytes());
        candidate_hash.update(rows.decision_at.unix_nanos().to_be_bytes());
        hash_bytes(&mut candidate_hash, rows.example_id.as_bytes())?;
        for binding in screen.feature_bindings() {
            hash_bytes(&mut candidate_hash, binding.key().name().as_bytes())?;
            candidate_hash.update(binding.key().version().get().to_be_bytes());
            candidate_hash.update(binding.semantic_digest().as_bytes());
            let value = match rows
                .components
                .get(&(binding.key().name(), binding.key().version().get()))
            {
                Some(row) => match row.value() {
                    ForecastFeatureValue::Float(value) => {
                        let value = StatisticalF64::try_new(*value)
                            .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?;
                        candidate_hash.update([1]);
                        candidate_hash.update(row.lineage_sha256().bytes());
                        candidate_hash.update(value.get().to_bits().to_be_bytes());
                        present = present
                            .checked_add(1)
                            .ok_or(ScreenWorkflowError::Capacity)?;
                        Some(value)
                    }
                    ForecastFeatureValue::Missing => {
                        candidate_hash.update([0]);
                        candidate_hash.update(row.lineage_sha256().bytes());
                        None
                    }
                    ForecastFeatureValue::Decimal { mantissa, scale } => {
                        // The producer retains the exact decimal. This is the same finite
                        // statistical projection used by model inputs, never an executable price.
                        let value = StatisticalF64::try_new(
                            *mantissa as f64 / 10_f64.powi(i32::from(*scale)),
                        )
                        .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?;
                        candidate_hash.update([2]);
                        candidate_hash.update(row.lineage_sha256().bytes());
                        candidate_hash.update(mantissa.to_be_bytes());
                        candidate_hash.update([*scale]);
                        candidate_hash.update(value.get().to_bits().to_be_bytes());
                        present = present
                            .checked_add(1)
                            .ok_or(ScreenWorkflowError::Capacity)?;
                        Some(value)
                    }
                },
                None => {
                    candidate_hash.update([0]);
                    candidate_hash.update([0; 32]);
                    None
                }
            };
            if binding.key().name() == LIQUIDITY_FEATURE_NAME {
                if liquidity.is_some() {
                    return Err(ScreenWorkflowError::InvalidRequest);
                }
                liquidity = value;
            }
            observations.push(ScreenFeatureObservation::new(binding.clone(), value));
        }
        let coverage =
            StatisticalF64::try_new(present as f64 / screen.feature_bindings().len() as f64)
                .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?;
        let evidence_identity = content_identity(candidate_hash.finalize().into())?;
        let mut id_hasher = Sha256::new();
        id_hasher.update(b"market-squawk/screen-candidate-id/v1\0");
        hash_bytes(&mut id_hasher, run.id().as_str().as_bytes())?;
        id_hasher.update(instrument_id.as_uuid().as_bytes());
        id_hasher.update(rows.source_selection_as_of.unix_nanos().to_be_bytes());
        id_hasher.update(rows.decision_at.unix_nanos().to_be_bytes());
        hash_bytes(&mut id_hasher, rows.example_id.as_bytes())?;
        let id_hash: [u8; 32] = id_hasher.finalize().into();
        let candidate_id = CandidateId::try_new(format!("candidate.{}", hex(&id_hash)))
            .map_err(|_error| ScreenWorkflowError::Capacity)?;
        candidates.push(
            CandidateInput::try_new(
                candidate_id,
                instrument_id,
                observations,
                coverage,
                liquidity,
                DataQuality::Modeled,
                None,
                Vec::new(),
                evidence_identity,
            )
            .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?,
        );
    }
    Ok(candidates)
}

fn dataset_identity(
    request: &ScreenJobRequest,
) -> Result<DecisionContentDigest, ScreenWorkflowError> {
    let mut hash = Sha256::new();
    hash.update(SCREEN_DATASET_DIGEST_DOMAIN);
    hash_selection(&mut hash, &request.datasets)?;
    hash.update(request.as_of().unix_nanos().to_be_bytes());
    hash.update(analytical_profile_digest(&request.profile)?.bytes());
    hash_bytes(
        &mut hash,
        &serde_json::to_vec(request.calendar.reference())
            .map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    content_identity(hash.finalize().into())
}

fn hash_selection(
    hash: &mut Sha256,
    selection: &ScreenDatasetSelection,
) -> Result<(), ScreenWorkflowError> {
    match selection {
        ScreenDatasetSelection::PublishedDataset(manifest) => {
            hash.update([0]);
            hash_manifest(hash, manifest)?;
        }
        ScreenDatasetSelection::CurrentFind(parts) => {
            validate_current_partition_set(parts)?;
            hash.update([1]);
            hash.update((parts.len() as u64).to_be_bytes());
            for part in parts {
                hash_bytes(
                    hash,
                    &serde_json::to_vec(part.population_reference())
                        .map_err(|_| ScreenWorkflowError::Capacity)?,
                )?;
                hash_bytes(
                    hash,
                    &serde_json::to_vec(part.partition().descriptor())
                        .map_err(|_| ScreenWorkflowError::Capacity)?,
                )?;
                hash_bytes(
                    hash,
                    &serde_json::to_vec(part.unavailable())
                        .map_err(|_| ScreenWorkflowError::Capacity)?,
                )?;
                match part.dataset() {
                    Some(dataset) => {
                        hash.update([1]);
                        hash_manifest(hash, dataset.generation().manifest())?;
                    }
                    None => hash.update([0]),
                }
            }
        }
    }
    Ok(())
}

fn plan_digest(
    run: &ScreenRun,
    candidates: &[CandidateInput],
    selected_at: Timestamp,
    dataset: &ScreenDatasetFence,
) -> Result<EvidenceDigest, ScreenWorkflowError> {
    let mut hash = Sha256::new();
    hash.update(SCREEN_INPUT_DIGEST_DOMAIN);
    hash_bytes(&mut hash, run.id().as_str().as_bytes())?;
    hash_bytes(&mut hash, run.screen().id().as_str().as_bytes())?;
    hash.update(run.screen().revision().get().to_be_bytes());
    hash.update(run.as_of().unix_nanos().to_be_bytes());
    hash.update(run.dataset_identity().evidence_digest().bytes());
    hash.update(run.universe_identity().evidence_digest().bytes());
    hash.update(
        u64::try_from(run.feature_bindings().len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    for binding in run.feature_bindings() {
        hash_bytes(&mut hash, binding.key().name().as_bytes())?;
        hash.update(binding.key().version().get().to_be_bytes());
        hash.update(binding.semantic_digest().as_bytes());
    }
    hash.update(selected_at.unix_nanos().to_be_bytes());
    hash.update((dataset.publications.len() as u64).to_be_bytes());
    for publication in &dataset.publications {
        hash_manifest(&mut hash, &publication.manifest)?;
        hash.update(publication.object_graph_digest.bytes());
        hash.update(publication.export_sha256.bytes());
        hash.update(publication.query_sha256.bytes());
        hash.update(publication.result_sha256.bytes());
        hash.update(publication.selected_rows.get().to_be_bytes());
        hash.update(publication.policy_sha256.bytes());
        hash.update(publication.universe_sha256.bytes());
        hash.update(publication.research_decision);
        hash.update(publication.research_graph);
        hash.update(publication.research_expires_at.unix_nanos().to_be_bytes());
        hash_bytes(
            &mut hash,
            &serde_json::to_vec(&publication.population_partition)
                .map_err(|_| ScreenWorkflowError::Capacity)?,
        )?;
    }
    hash.update(dataset.profile_digest.bytes());
    hash_bytes(
        &mut hash,
        &serde_json::to_vec(&dataset.calendar_reference)
            .map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    hash_population(
        &mut hash,
        dataset.population_basis,
        dataset.population_member_count,
        &dataset.population_unavailable,
    )?;
    hash_bytes(
        &mut hash,
        &serde_json::to_vec(&dataset.population_partitions)
            .map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    hash_bytes(
        &mut hash,
        &serde_json::to_vec(&dataset.population_reference)
            .map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    hash.update(dataset.research_expires_at.unix_nanos().to_be_bytes());
    hash.update(
        dataset
            .current_session_expires_at
            .unix_nanos()
            .to_be_bytes(),
    );
    hash.update(
        u64::try_from(candidates.len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    for candidate in candidates {
        hash_bytes(&mut hash, candidate.id().as_str().as_bytes())?;
        hash.update(candidate.instrument_id().as_uuid().as_bytes());
        hash.update(candidate.coverage().get().to_bits().to_be_bytes());
        match candidate.liquidity() {
            Some(value) => {
                hash.update([1]);
                hash.update(value.get().to_bits().to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update([data_quality_tag(candidate.data_quality())]);
        match candidate.portfolio_impact() {
            Some(portfolio) => {
                hash.update([1]);
                hash.update(portfolio.bytes());
            }
            None => hash.update([0]),
        }
        hash.update(
            u64::try_from(candidate.flags().len())
                .map_err(|_error| ScreenWorkflowError::Capacity)?
                .to_be_bytes(),
        );
        for flag in candidate.flags() {
            hash.update([candidate_flag_tag(*flag)]);
        }
        hash.update(candidate.evidence_identity().evidence_digest().bytes());
        for observation in candidate.observations() {
            hash_bytes(&mut hash, observation.binding().key().name().as_bytes())?;
            hash.update(observation.binding().key().version().get().to_be_bytes());
            hash.update(observation.binding().semantic_digest().as_bytes());
            match observation.value() {
                Some(value) => {
                    hash.update([1]);
                    hash.update(value.get().to_bits().to_be_bytes());
                }
                None => hash.update([0]),
            }
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_population(
    hash: &mut Sha256,
    basis: DatasetPopulationBasis,
    member_count: usize,
    unavailable: &[CurrentPopulationInputUnavailable],
) -> Result<(), ScreenWorkflowError> {
    hash_bytes(
        hash,
        &serde_json::to_vec(&basis).map_err(|_| ScreenWorkflowError::Capacity)?,
    )?;
    hash.update(
        u64::try_from(member_count)
            .map_err(|_| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    hash_bytes(
        hash,
        &serde_json::to_vec(unavailable).map_err(|_| ScreenWorkflowError::Capacity)?,
    )
}

fn validate_current_partition_set(
    partitions: &[CurrentFindScreenPartition],
) -> Result<&CurrentListedPopulation, ScreenWorkflowError> {
    let first = partitions
        .first()
        .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
    let population = first.partition().population();
    if population.instrument_ids().is_empty()
        || population.instrument_ids().len()
            > crate::application::market_selection::product::MAXIMUM_PRODUCT_MARKET_POPULATION
        || partitions.len() != first.partition().partition_count()
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    let mut members = std::collections::BTreeSet::new();
    for (ordinal, value) in partitions.iter().enumerate() {
        let part = value.partition();
        if part.ordinal() != ordinal
            || part.partition_count() != partitions.len()
            || part.population().content_digest() != population.content_digest()
            || part.population().audit_digest() != population.audit_digest()
            || part.population().membership_as_of() != population.membership_as_of()
            || value.population_reference() != first.population_reference()
            || part.descriptor().member_ids() != part.instrument_ids()
            || part.instrument_ids().iter().any(|id| !members.insert(*id))
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
    }
    if members.len() != population.instrument_ids().len()
        || population
            .instrument_ids()
            .iter()
            .any(|id| !members.contains(id))
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    Ok(population)
}

fn validate_population_inputs(
    dataset: &ScreenDatasetFence,
    candidates: &[CandidateInput],
) -> Result<(), ScreenWorkflowError> {
    if dataset
        .publications
        .iter()
        .map(|publication| publication.research_expires_at)
        .min()
        != Some(dataset.research_expires_at)
        || dataset.publications.is_empty()
        || dataset.publications.len() > maximum_population_partitions()
        || candidates
            .len()
            .checked_mul(
                candidates
                    .first()
                    .map_or(0, |candidate| candidate.observations().len()),
            )
            .is_none_or(|count| count > MAXIMUM_SCREEN_OBSERVATIONS)
        || dataset
            .publications
            .iter()
            .any(|publication| publication.selected_rows.get() > MAXIMUM_SCREEN_DATASET_ROWS as u64)
        || dataset
            .publications
            .iter()
            .enumerate()
            .any(|(index, publication)| {
                dataset.publications[index + 1..]
                    .iter()
                    .any(|other| other.manifest == publication.manifest)
            })
        || candidates
            .windows(2)
            .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
        || dataset
            .population_unavailable
            .windows(2)
            .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    if dataset.population_basis == DatasetPopulationBasis::PresentDayFixedCohort {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    if dataset.population_basis == DatasetPopulationBasis::CurrentListedSnapshot {
        let mut declared = std::collections::BTreeSet::new();
        let first = dataset
            .population_partitions
            .first()
            .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
        if dataset
            .population_reference
            .as_ref()
            .is_none_or(|reference| {
                reference
                    .canonical_population_count()
                    .is_none_or(|count| count < dataset.population_member_count)
                    || reference.financial_profile_digest() != hex(&dataset.profile_digest.bytes())
            })
            || first.partition_count() != dataset.population_partitions.len()
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        for (ordinal, part) in dataset.population_partitions.iter().enumerate() {
            part.validate(dataset.population_member_count)
                .map_err(|_| ScreenWorkflowError::DatasetUnavailable)?;
            if part.ordinal() != ordinal
                || part.partition_count() != dataset.population_partitions.len()
                || part.full_population_digest() != first.full_population_digest()
                || part.member_ids().iter().any(|id| !declared.insert(*id))
            {
                return Err(ScreenWorkflowError::DatasetUnavailable);
            }
        }
        let actual = candidates
            .iter()
            .map(CandidateInput::instrument_id)
            .chain(
                dataset
                    .population_unavailable
                    .iter()
                    .map(CurrentPopulationInputUnavailable::instrument_id),
            )
            .collect::<std::collections::BTreeSet<_>>();
        if dataset.population_member_count == 0
            || dataset.population_member_count
                > crate::application::market_selection::product::MAXIMUM_PRODUCT_MARKET_POPULATION
            || actual.len() != candidates.len() + dataset.population_unavailable.len()
            || actual.len() != dataset.population_member_count
            || actual != declared
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let mut published = std::collections::BTreeSet::new();
        let mut prior_ordinal = None;
        for publication in &dataset.publications {
            let part = publication
                .population_partition
                .as_ref()
                .ok_or(ScreenWorkflowError::DatasetUnavailable)?;
            if dataset.population_partitions.get(part.ordinal()) != Some(part)
                || prior_ordinal.is_some_and(|ordinal| part.ordinal() <= ordinal)
                || !published.insert(part.ordinal())
            {
                return Err(ScreenWorkflowError::DatasetUnavailable);
            }
            prior_ordinal = Some(part.ordinal());
        }
        for part in &dataset.population_partitions {
            let available = part.member_ids().iter().any(|instrument| {
                candidates
                    .binary_search_by_key(instrument, CandidateInput::instrument_id)
                    .is_ok()
            });
            if published.contains(&part.ordinal()) != available {
                return Err(ScreenWorkflowError::DatasetUnavailable);
            }
        }
    } else if !dataset.population_unavailable.is_empty()
        || !dataset.population_partitions.is_empty()
        || dataset.population_reference.is_some()
        || dataset.publications.len() != 1
        || dataset
            .publications
            .iter()
            .any(|value| value.population_partition.is_some())
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    Ok(())
}

fn hash_manifest(
    hash: &mut Sha256,
    manifest: &DatasetManifestRef,
) -> Result<(), ScreenWorkflowError> {
    hash_bytes(hash, manifest.dataset_id().as_str().as_bytes())?;
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(hash, manifest.schema().name().as_bytes())?;
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    Ok(())
}

fn hash_bytes(hash: &mut Sha256, value: &[u8]) -> Result<(), ScreenWorkflowError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

fn analytical_profile_digest(
    profile: &ValidatedAnalyticalProfile,
) -> Result<Sha256Digest, ScreenWorkflowError> {
    let text = &profile.resolution().configuration_digest;
    if text.len() != 64
        || !text
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
    {
        return Err(ScreenWorkflowError::InvalidRequest);
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| ScreenWorkflowError::InvalidRequest)?;
    }
    if digest == [0; 32] {
        return Err(ScreenWorkflowError::InvalidRequest);
    }
    Ok(Sha256Digest::new(digest))
}

fn content_identity(bytes: [u8; 32]) -> Result<DecisionContentDigest, ScreenWorkflowError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)
}

fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScreenJobPlanWire {
    run: RunWire,
    candidates: Vec<CandidateInputWire>,
    selected_at: Timestamp,
    dataset: ScreenDatasetFenceWire,
    input_digest: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScreenDatasetFenceWire {
    publications: Box<[ScreenPublicationFenceWire]>,
    calendar_reference: CompletedMarketSessionReference,
    profile_digest: [u8; 32],
    population_basis: DatasetPopulationBasis,
    population_member_count: usize,
    population_unavailable: Box<[CurrentPopulationInputUnavailable]>,
    population_partitions: Box<[DatasetPopulationPartition]>,
    population_reference: Option<FindPopulationReference>,
    research_expires_at: Timestamp,
    current_session_expires_at: Timestamp,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScreenPublicationFenceWire {
    manifest: ManifestWire,
    object_graph_digest: EvidenceDigest,
    export_sha256: [u8; 32],
    query_sha256: [u8; 32],
    result_sha256: [u8; 32],
    selected_rows: u64,
    policy_sha256: [u8; 32],
    universe_sha256: [u8; 32],
    population_partition: Option<DatasetPopulationPartition>,
    research_decision: [u8; 32],
    research_graph: [u8; 32],
    research_expires_at: Timestamp,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
}

impl ScreenJobPlanWire {
    pub(super) fn key(&self) -> &str {
        self.run.key()
    }

    pub(super) fn from_plan(plan: &ScreenJobPlan) -> Self {
        Self {
            run: (&plan.run).into(),
            candidates: plan.candidates.iter().map(Into::into).collect(),
            selected_at: plan.selected_at,
            dataset: ScreenDatasetFenceWire {
                publications: plan
                    .dataset
                    .publications
                    .iter()
                    .map(|publication| ScreenPublicationFenceWire {
                        manifest: ManifestWire::from_manifest(&publication.manifest),
                        object_graph_digest: publication.object_graph_digest,
                        export_sha256: publication.export_sha256.bytes(),
                        query_sha256: publication.query_sha256.bytes(),
                        result_sha256: publication.result_sha256.bytes(),
                        selected_rows: publication.selected_rows.get(),
                        policy_sha256: publication.policy_sha256.bytes(),
                        universe_sha256: publication.universe_sha256.bytes(),
                        population_partition: publication.population_partition.clone(),
                        research_decision: publication.research_decision,
                        research_graph: publication.research_graph,
                        research_expires_at: publication.research_expires_at,
                    })
                    .collect(),
                calendar_reference: plan.dataset.calendar_reference.clone(),
                profile_digest: plan.dataset.profile_digest.bytes(),
                population_basis: plan.dataset.population_basis,
                population_member_count: plan.dataset.population_member_count,
                population_unavailable: plan.dataset.population_unavailable.clone(),
                population_partitions: plan.dataset.population_partitions.clone(),
                population_reference: plan.dataset.population_reference.clone(),
                research_expires_at: plan.dataset.research_expires_at,
                current_session_expires_at: plan.dataset.current_session_expires_at,
            },
            input_digest: plan.input_digest,
        }
    }

    pub(super) fn decode(
        &self,
        registry: &FeatureRegistry,
    ) -> Result<ScreenJobPlan, DecisionApplicationError> {
        if self.dataset.publications.len() > maximum_population_partitions()
            || self.dataset.population_partitions.len() > maximum_population_partitions()
            || self.candidates.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS
            || self.dataset.population_unavailable.len()
                > crate::application::market_selection::product::MAXIMUM_PRODUCT_MARKET_POPULATION
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let run = self.run.decode(registry)?;
        let candidates = self
            .candidates
            .iter()
            .map(|candidate| candidate.decode(run.feature_bindings(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        let publications = self
            .dataset
            .publications
            .iter()
            .map(|value| {
                if value.object_graph_digest
                    != EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        value.object_graph_digest.bytes(),
                    )
                {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                Ok(ScreenPublicationFence {
                    manifest: value.manifest.decode()?,
                    object_graph_digest: EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        nonzero_sha256(value.object_graph_digest.bytes())?.bytes(),
                    ),
                    export_sha256: nonzero_sha256(value.export_sha256)?,
                    query_sha256: nonzero_sha256(value.query_sha256)?,
                    result_sha256: nonzero_sha256(value.result_sha256)?,
                    selected_rows: NonZeroU64::new(value.selected_rows)
                        .ok_or(DecisionApplicationError::InvalidPersistentState)?,
                    policy_sha256: nonzero_sha256(value.policy_sha256)?,
                    universe_sha256: nonzero_sha256(value.universe_sha256)?,
                    population_partition: value.population_partition.clone(),
                    research_decision: nonzero_sha256(value.research_decision)?.bytes(),
                    research_graph: nonzero_sha256(value.research_graph)?.bytes(),
                    research_expires_at: value.research_expires_at,
                })
            })
            .collect::<Result<Box<[_]>, DecisionApplicationError>>()?;
        let dataset = ScreenDatasetFence {
            publications,
            calendar_reference: self.dataset.calendar_reference.clone(),
            profile_digest: nonzero_sha256(self.dataset.profile_digest)?,
            population_basis: self.dataset.population_basis,
            population_member_count: self.dataset.population_member_count,
            population_unavailable: self.dataset.population_unavailable.clone(),
            population_partitions: self.dataset.population_partitions.clone(),
            population_reference: self.dataset.population_reference.clone(),
            research_expires_at: self.dataset.research_expires_at,
            current_session_expires_at: self.dataset.current_session_expires_at,
        };
        validate_population_inputs(&dataset, &candidates)
            .map_err(|_| DecisionApplicationError::InvalidPersistentState)?;
        let expected = plan_digest(&run, &candidates, self.selected_at, &dataset)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if dataset.population_partitions.first().is_some_and(|part| {
            part.full_population_digest() != run.universe_identity().evidence_digest().bytes()
        }) || expected != self.input_digest
            || self.selected_at < run.as_of()
            || self.selected_at >= dataset.research_expires_at
            || self.selected_at >= dataset.current_session_expires_at
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(ScreenJobPlan {
            run,
            candidates,
            selected_at: self.selected_at,
            dataset,
            input_digest: self.input_digest,
        })
    }
}

impl ManifestWire {
    fn from_manifest(manifest: &DatasetManifestRef) -> Self {
        Self {
            dataset_id: manifest.dataset_id().as_str().to_owned(),
            manifest_version: manifest.manifest_version(),
            schema_name: manifest.schema().name().to_owned(),
            schema_version: manifest.schema_version().get(),
            schema_fingerprint: manifest.schema().fingerprint(),
            content_hash: manifest.content_hash().bytes(),
        }
    }

    fn decode(&self) -> Result<DatasetManifestRef, DecisionApplicationError> {
        let schema = DatasetSchemaRef::try_new(
            &self.schema_name,
            SchemaVersion::new(self.schema_version)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.schema_fingerprint,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str())
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.manifest_version,
            schema,
            nonzero_sha256(self.content_hash)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

fn nonzero_sha256(bytes: [u8; 32]) -> Result<Sha256Digest, DecisionApplicationError> {
    if bytes == [0; 32] {
        Err(DecisionApplicationError::InvalidPersistentState)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn candidate_flag_tag(flag: CandidateFlag) -> u8 {
    match flag {
        CandidateFlag::MissingFeatureIncluded => 1,
        CandidateFlag::ModelDependent => 2,
        CandidateFlag::PortfolioImpactBound => 3,
        CandidateFlag::NonDirectData => 4,
    }
}

pub(super) fn validate_fence(
    plan: &ScreenJobPlan,
    screen: &SavedScreen,
) -> Result<(), ScreenWorkflowError> {
    validate_population_inputs(&plan.dataset, &plan.candidates)?;
    if plan.run.screen() != screen.revision()
        || plan.run.universe_identity() != screen.universe_identity()
        || plan.run.feature_bindings() != screen.feature_bindings()
        || plan
            .dataset
            .population_reference
            .as_ref()
            .is_some_and(|reference| {
                reference.maximum_deep_analyses() != screen.maximum_results().get()
                    || reference.source_cutoff() > plan.run.as_of()
            })
        || plan.selected_at < plan.run.as_of()
        || plan.candidates.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS
        || plan.selected_at >= plan.dataset.research_expires_at
        || plan.selected_at >= plan.dataset.current_session_expires_at
        || plan
            .dataset
            .population_partitions
            .first()
            .is_some_and(|part| {
                part.full_population_digest()
                    != plan.run.universe_identity().evidence_digest().bytes()
            })
    {
        return Err(ScreenWorkflowError::Conflict);
    }
    Ok(())
}

const fn maximum_population_partitions() -> usize {
    super::current_find::MAXIMUM_CURRENT_FIND_PARTITIONS
}

/// Recheck cancellation and the original source-use/session leases at the journal publication lock.
pub(super) fn validate_publication_control(
    plan: &ScreenJobPlan,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ScreenWorkflowError> {
    ensure_screen_control(deadline, cancellation)?;
    let now = screen_clock()?;
    if now < plan.selected_at
        || now >= plan.dataset.research_expires_at
        || now >= plan.dataset.current_session_expires_at
    {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    Ok(())
}
