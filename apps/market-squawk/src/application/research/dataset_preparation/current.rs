//! Current, label-free Find features over the existing source selectors and dataset publisher.

mod packing;

use std::{collections::BTreeMap, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use market_squawk_backtesting::RECOMMENDATION_TARGET_HORIZON_NANOS_V1;
use market_squawk_data::{
    AnalyticalFeatureDataset, AnalyticalObservationTemplate, AnalyticalReadLimit,
    ChronologicalSplitPolicy, ComponentKind, ComponentScope, CorporateActionAdjustment,
    CorporateActionPolicy, CorporateActionSensitivity, CurrentListedPopulation,
    CurrentListedPopulationPartition, CurrentPopulationInputUnavailable,
    CurrentPopulationInputUnavailableReason, DatasetBuildInputs, DatasetBuildPolicy,
    DatasetBuildPurpose, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DatasetPopulationBasis, DatasetPopulationPartition, DatasetSchemaRegistry, DatasetStudyPolicy,
    DatasetTargetHorizon, DerivedGenerationParents, FeatureDatasetProductContract,
    FeatureLabelComponentSpec, MarketDataInstrumentReadCapability, MissingValuePolicy,
    NominalDailyCurrentSource,
    PointInTimePolicy, PointInTimeRevisionMode, ResearchUse, ResearchUseCatalogError,
    ResearchUseLimits, ResearchUseRequest, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, HistoricalStudyBasis, InstrumentId,
    MarketBarAdjustment, MarketBarObservation, ResearchTemporalCoordinate,
    SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use std::sync::Arc;
use crate::application::research::corporate_actions::{SourceAppliedCorporateActionReadCapability, SourcePlanCalendar};
use crate::application::decision::current_find::RetainedCurrentFindSources;

use super::{
    DatasetPreparationAuthority, DatasetPreparationError, DatasetPreparationUse,
    EvidencePart, FeatureDatasetProductionFinalizer, MarketSeriesPoint,
    PreparedFeatureDatasetBuild, PreparedProductionEvidence, adjustment_evidence,
    aggregate_evidence, check_control, component_content_evidence, dataset_request,
    evidence_digest, hash_manifest, market_bar_family, plan_audit_evidence, push_parent,
    return_component, short_hex, split_adjusted_return, timestamp_calendar_date,
};
use crate::application::{
    analytical_profile::ValidatedAnalyticalProfile,
    market_calendar::{CompletedMarketSessionRead, CompletedMarketSessionResolution},
    market_selection::product::MAXIMUM_PRODUCT_MARKET_POPULATION,
    research::{
        FindPopulationReference, PreparedFindPopulation,
        macro_features::MacroFeatureVector,
    },
};

const MAXIMUM_CURRENT_CANDIDATES: usize = MAXIMUM_PRODUCT_MARKET_POPULATION;
const MAXIMUM_CURRENT_SOURCE_BYTES: usize = 16 * 1024 * 1024;
// Only one at-most128 partition retains component recipes while its existing child runs.
const MAXIMUM_CURRENT_COMPONENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct CurrentFindCandidateCoordinates {
    instrument_id: InstrumentId,
    quote_currency: Currency,
}

/// Actual coverage survives when every member lacks required current source evidence.
/// Empty data is never promoted into a completed feature dataset.
pub(crate) struct PreparedCurrentFindFeaturePartition {
    partition: CurrentListedPopulationPartition,
    population_reference: FindPopulationReference,
    build: Option<PreparedFeatureDatasetBuild>,
    unavailable: Box<[CurrentPopulationInputUnavailable]>,
}

impl PreparedCurrentFindFeaturePartition {
    pub(crate) const fn partition(&self) -> &CurrentListedPopulationPartition {
        &self.partition
    }

    pub(crate) fn unavailable(&self) -> &[CurrentPopulationInputUnavailable] {
        &self.unavailable
    }

    /// Transfers the sole dataset finalizer to the existing child job while retaining its exact
    /// source-issued coverage and build commitment for that job's eventual publication.
    pub(crate) fn into_job_parts(
        self,
    ) -> (
        CurrentFindPartitionPreparationEvidence,
        Option<PreparedFeatureDatasetBuild>,
    ) {
        let expected_dataset_id = self
            .build
            .as_ref()
            .map(|build| build.request.output_dataset().clone());
        let expected_build_spec = self
            .build
            .as_ref()
            .map(|build| build.request.build_spec_digest().digest());
        (
            CurrentFindPartitionPreparationEvidence {
                partition: self.partition,
                population_reference: self.population_reference,
                unavailable: self.unavailable,
                expected_build_spec,
                expected_dataset_id,
            },
            self.build,
        )
    }
}

/// Complete original source topology, with no retained history, feature rows, or build recipes.
/// Each fixed partition is prepared and published before the next partition allocates inputs.
#[derive(Debug)]
pub(crate) struct PreparedCurrentFindFeatures {
    population: CurrentListedPopulation,
    population_reference: FindPopulationReference,
    partitions: Box<[CurrentListedPopulationPartition]>,
    candidates: Box<[CurrentFindCandidateCoordinates]>,
    source_actions: RetainedCurrentFindSources,
}
impl PreparedCurrentFindFeatures {
    pub(crate) fn source_page_references(&self) -> &[crate::application::decision::current_find::CurrentFindSourcePageReference] {
        self.source_actions.pages()
    }
    pub(crate) fn partitions(&self) -> &[CurrentListedPopulationPartition] {
        &self.partitions
    }
    /// A parent-limit cut needs a new source group; all members of one group share its roots.
    /// Remaining cuts consume128 members. This conservative bound preserves every member.
    pub(crate) const fn maximum_partition_count(population_count: usize, source_groups: usize) -> usize {
        let bound = population_count.div_ceil(128).saturating_add(source_groups);
        if bound < population_count { bound } else { population_count }
    }

    pub(crate) fn partition_ends(&self) -> Box<[usize]> {
        let mut end = 0;
        self.partitions.iter().map(|partition| { end += partition.instrument_ids().len(); end }).collect()
    }
    pub(crate) const fn population_reference(&self) -> &FindPopulationReference {
        &self.population_reference
    }
    pub(crate) fn analytical_cutoff(&self) -> Timestamp {
        self.population.membership_as_of()
    }
}

/// Inert restart reference. Deserialization validates structure and never grants membership,
/// source absence, or publication authority; only the retained DecisionJournal row can rebind it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "CurrentFindPartitionEvidenceWire",
    into = "CurrentFindPartitionEvidenceWire"
)]
pub(crate) struct CurrentFindPartitionEvidenceReference {
    population_reference: FindPopulationReference,
    partition: DatasetPopulationPartition,
    population_member_count: usize,
    analytical_cutoff: Timestamp,
    unavailable: Box<[CurrentPopulationInputUnavailable]>,
    expected_dataset_id: Option<Box<str>>,
    expected_build_spec: Option<[u8; 32]>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CurrentFindPartitionEvidenceWire {
    population_reference: FindPopulationReference,
    partition: DatasetPopulationPartition,
    population_member_count: usize,
    analytical_cutoff: Timestamp,
    unavailable: Box<[CurrentPopulationInputUnavailable]>,
    expected_dataset_id: Option<Box<str>>,
    expected_build_spec: Option<[u8; 32]>,
}
impl From<CurrentFindPartitionEvidenceReference> for CurrentFindPartitionEvidenceWire {
    fn from(value: CurrentFindPartitionEvidenceReference) -> Self {
        Self {
            population_reference: value.population_reference,
            partition: value.partition,
            population_member_count: value.population_member_count,
            analytical_cutoff: value.analytical_cutoff,
            unavailable: value.unavailable,
            expected_dataset_id: value.expected_dataset_id,
            expected_build_spec: value.expected_build_spec,
        }
    }
}
impl TryFrom<CurrentFindPartitionEvidenceWire> for CurrentFindPartitionEvidenceReference {
    type Error = &'static str;
    fn try_from(value: CurrentFindPartitionEvidenceWire) -> Result<Self, Self::Error> {
        let result = Self {
            population_reference: value.population_reference,
            partition: value.partition,
            population_member_count: value.population_member_count,
            analytical_cutoff: value.analytical_cutoff,
            unavailable: value.unavailable,
            expected_dataset_id: value.expected_dataset_id,
            expected_build_spec: value.expected_build_spec,
        };
        result.validate()?;
        Ok(result)
    }
}
impl CurrentFindPartitionEvidenceReference {
    pub(crate) const fn population_reference(&self) -> &FindPopulationReference {
        &self.population_reference
    }
    pub(crate) const fn partition(&self) -> &DatasetPopulationPartition {
        &self.partition
    }
    pub(crate) const fn analytical_cutoff(&self) -> Timestamp {
        self.analytical_cutoff
    }
    pub(crate) fn unavailable(&self) -> &[CurrentPopulationInputUnavailable] {
        &self.unavailable
    }
    pub(crate) fn expected_dataset_id(&self) -> Option<&str> {
        self.expected_dataset_id.as_deref()
    }
    pub(crate) const fn expected_build_spec(&self) -> Option<[u8; 32]> {
        self.expected_build_spec
    }
    fn validate(&self) -> Result<(), &'static str> {
        self.partition
            .validate(self.population_member_count)
            .map_err(|_| "invalid current partition")?;
        if self.population_reference.source_cutoff() > self.analytical_cutoff
            || self.population_reference.coverage()
                != crate::application::research::FindPopulationCoverage::Complete
            || self.unavailable.len() > self.partition.member_ids().len()
            || self
                .unavailable
                .windows(2)
                .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
            || self.unavailable.iter().any(|value| {
                self.partition
                    .member_ids()
                    .binary_search(&value.instrument_id())
                    .is_err()
            })
        {
            return Err("invalid current partition coverage");
        }
        match (&self.expected_dataset_id, self.expected_build_spec) {
            (Some(dataset), Some(digest))
                if digest != [0; 32]
                    && self.unavailable.len() < self.partition.member_ids().len() =>
            {
                DatasetId::try_from(dataset.as_ref())
                    .map_err(|_| "invalid current dataset identity")?;
            }
            (None, None) if self.unavailable.len() == self.partition.member_ids().len() => {}
            _ => return Err("invalid current publication coordinates"),
        }
        Ok(())
    }
}

/// Process-owned custody from actual source preparation through the existing dataset job.
/// Neither parsed references nor a caller manifest list can construct this authority.
#[derive(Clone, Debug)]
pub(crate) struct CurrentFindPartitionPreparationEvidence {
    partition: CurrentListedPopulationPartition,
    population_reference: FindPopulationReference,
    unavailable: Box<[CurrentPopulationInputUnavailable]>,
    expected_build_spec: Option<Sha256Digest>,
    expected_dataset_id: Option<DatasetId>,
}
impl CurrentFindPartitionPreparationEvidence {
    pub(crate) fn reference(&self) -> CurrentFindPartitionEvidenceReference {
        CurrentFindPartitionEvidenceReference {
            population_reference: self.population_reference.clone(),
            partition: self.partition.descriptor().clone(),
            population_member_count: self.partition.population().instrument_ids().len(),
            analytical_cutoff: self.partition.population().membership_as_of(),
            unavailable: self.unavailable.clone(),
            expected_dataset_id: self
                .expected_dataset_id
                .as_ref()
                .map(|id| Box::from(id.as_str())),
            expected_build_spec: self.expected_build_spec.map(Sha256Digest::bytes),
        }
    }

    pub(crate) const fn population_reference(&self) -> &FindPopulationReference {
        &self.population_reference
    }
    pub(crate) const fn partition(&self) -> &CurrentListedPopulationPartition {
        &self.partition
    }
    pub(crate) fn unavailable(&self) -> &[CurrentPopulationInputUnavailable] {
        &self.unavailable
    }
    pub(crate) const fn expected_build_spec(&self) -> Option<Sha256Digest> {
        self.expected_build_spec
    }
    /// The supplied summary comes only from the native exact publication reader. A missing
    /// child publication cannot become a fabricated missing-input assessment.
    pub(crate) fn complete(
        self,
        dataset: Option<AnalyticalFeatureDataset>,
    ) -> Result<CurrentFindScreenPartition, DatasetPreparationError> {
        match (&dataset, self.expected_build_spec) {
            (Some(dataset), Some(expected)) => {
                let study = dataset
                    .study_policy()
                    .ok_or(DatasetPreparationError::InvalidEvidence)?;
                if Some(dataset.generation().manifest().dataset_id()) != self.expected_dataset_id.as_ref()
                    || dataset.generation().build_spec_digest().map(|value| value.digest()) != Some(expected)
                    || dataset.product_contract() != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
                    || dataset.population_basis() != DatasetPopulationBasis::CurrentListedSnapshot
                    || dataset.universe_id() != self.partition.population().universe_id()
                    || dataset.population_partition() != Some(self.partition.descriptor())
                    || dataset.population_member_count() != self.partition.population().instrument_ids().len()
                    || dataset.population_unavailable() != self.unavailable.as_ref()
                    || study.basis() != HistoricalStudyBasis::HistoricalAsKnown
                    || study.purpose() != DatasetBuildPurpose::StudyInputs
                    || study.snapshot_as_of() != self.partition.population().membership_as_of()
                {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
            }
            (None, None) if self.expected_dataset_id.is_none() => validate_population_coverage(
                self.partition.instrument_ids(),
                &[],
                &self.unavailable,
            )?,
            _ => return Err(DatasetPreparationError::Unavailable),
        }
        Ok(CurrentFindScreenPartition {
            partition: self.partition,
            population_reference: self.population_reference,
            dataset,
            unavailable: self.unavailable,
        })
    }
}

/// The original source partition joined to its genuine completed feature publication or its
/// original wholly unavailable preparation. Shared ScreenRun admission consumes the full set.
#[derive(Clone, Debug)]
pub(crate) struct CurrentFindScreenPartition {
    partition: CurrentListedPopulationPartition,
    population_reference: FindPopulationReference,
    dataset: Option<AnalyticalFeatureDataset>,
    unavailable: Box<[CurrentPopulationInputUnavailable]>,
}
impl CurrentFindScreenPartition {
    pub(crate) const fn population_reference(&self) -> &FindPopulationReference {
        &self.population_reference
    }
    pub(crate) const fn partition(&self) -> &CurrentListedPopulationPartition {
        &self.partition
    }
    pub(crate) const fn dataset(&self) -> Option<&AnalyticalFeatureDataset> {
        self.dataset.as_ref()
    }
    pub(crate) fn unavailable(&self) -> &[CurrentPopulationInputUnavailable] {
        &self.unavailable
    }
}

enum CurrentSourceSelection {
    Available(SelectedCurrentSource),
    Unavailable(CurrentPopulationInputUnavailableReason),
}

enum CurrentMacroSelection {
    Available { vector: MacroFeatureVector, expires_at: Timestamp },
    Unavailable(CurrentPopulationInputUnavailableReason),
}

struct SelectedCurrentSource {
    instrument: InstrumentId,
    manifest: DatasetManifestRef,
    prior: MarketSeriesPoint,
    current: MarketSeriesPoint,
    pit_content: EvidenceDigest,
    pit_audit: EvidenceDigest,
    session_request: EvidenceDigest,
    session_receipt: EvidenceDigest,
    expires_at: Timestamp,
    retained_bytes: usize,
    nominal_source: Option<NominalDailyCurrentSource>,
    source_action_plan: Option<Arc<market_squawk_data::CorporateActionPlan>>,
    additional_parents: Vec<DatasetManifestRef>,
}

impl DatasetPreparationAuthority {
    /// Produces current features over the entire qualified source population. Missing inputs
    /// remain explicit members in coverage; no replacement candidate or invented value is used.
    #[allow(
        clippy::too_many_arguments,
        reason = "all original source authorities stay explicit"
    )]
    pub(crate) async fn prepare_current_find_features(
        &self,
        population: &PreparedFindPopulation,
        market_reader: &MarketDataInstrumentReadCapability,
        profile: &ValidatedAnalyticalProfile,
        source_cutoff: Timestamp,
        source_actions: &RetainedCurrentFindSources,
        retained_partition_ends: Option<&[usize]>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedCurrentFindFeatures, DatasetPreparationError> {
        check_control(deadline, &cancellation)?;
        let evaluated_at = current_time()?;
        let original = population
            .current_population()
            .ok_or(DatasetPreparationError::Capacity)?;
        if !population.complete()
            || original.instrument_ids().len() > MAXIMUM_CURRENT_CANDIDATES
            || population.source_cutoff() > source_cutoff
            || population.validated_at() > evaluated_at
            || source_cutoff > evaluated_at
            || population.reference().financial_profile_digest()
                != profile.resolution().configuration_digest
        {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        let population_seal = market_reader
            .revalidate_current_listed_population(original, source_cutoff, deadline, &cancellation)
            .map_err(|error| {
                map_current_service_error(
                    crate::application::research::map_current_population_error(error),
                )
            })?;
        if population_seal.membership_as_of() != source_cutoff {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        if population.exclusions().iter().any(|excluded| {
            population_seal.instrument_ids().binary_search(&excluded.instrument_id()).is_ok()
        }) {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let candidates = population
            .candidates()
            .iter()
            .map(|candidate| CurrentFindCandidateCoordinates {
                instrument_id: candidate.instrument_id(),
                quote_currency: candidate.context().quote_currency(),
            })
            .collect::<Vec<_>>();
        let all_ids = candidates
            .iter()
            .map(|candidate| candidate.instrument_id)
            .collect::<std::collections::BTreeSet<_>>();
        if all_ids.len() != candidates.len()
            || !all_ids
                .iter()
                .copied()
                .eq(population_seal.instrument_ids().iter().copied())
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let mut action_ids = candidates.iter().map(|candidate| candidate.instrument_id).collect::<Vec<_>>();
        action_ids.sort_unstable();
        if !source_actions.matches_instruments(&action_ids).map_err(|_| DatasetPreparationError::InvalidEvidence)? {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let partitions = match retained_partition_ends {
            Some(ends) => population_seal.partitions_with_ends(ends).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            None => self.pack_current_partitions(&population_seal, source_actions, source_cutoff, deadline, &cancellation).await?,
        };
        Ok(PreparedCurrentFindFeatures {
            population: population_seal,
            population_reference: population.reference().clone(),
            partitions,
            candidates: candidates.into_boxed_slice(),
            source_actions: source_actions.clone(),
        })
    }

    /// Prepares one original retained partition through the existing complete-history selector.
    /// Callers await the actual dataset child and retain only its exact receipt before advancing.
    #[allow(
        clippy::too_many_arguments,
        reason = "source authorities and control remain explicit"
    )]
    pub(crate) async fn prepare_current_find_feature_partition(
        &self,
        prepared: &PreparedCurrentFindFeatures,
        ordinal: usize,
        _calendar: &CompletedMarketSessionRead,
        profile: &ValidatedAnalyticalProfile,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedCurrentFindFeaturePartition, DatasetPreparationError> {
        check_control(deadline, &cancellation)?;
        if prepared.population_reference.financial_profile_digest()
            != profile.resolution().configuration_digest
        {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        let source_cutoff = prepared.analytical_cutoff();
        let partition = prepared
            .partitions
            .get(ordinal)
            .ok_or(DatasetPreparationError::InvalidSelection)?
            .clone();
        let mut unavailable = Vec::new();
        let mut selected = Vec::new();
        let mut selected_bytes = 0_usize;
        selected
            .try_reserve_exact(partition.instrument_ids().len())
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let action_reader = SourceAppliedCorporateActionReadCapability::new(Arc::clone(&self.research), self.calendar.clone());
        for (index, _) in prepared.source_actions.pages().iter().enumerate().filter(|(_, page)| page.intersects(partition.instrument_ids())) {
            check_control(deadline, &cancellation)?;
            let reference = prepared.source_actions.read(index).map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            if !reference.requested_instruments().iter().any(|id| partition.instrument_ids().binary_search(id).is_ok()) { continue; }
            let action_source = action_reader.read_reference(&reference, deadline, cancellation.child_token()).await
                .map_err(|error| super::map_source_action_error(error))?.ok_or(DatasetPreparationError::Unavailable)?;
            let covered = action_source.covered_price_plan().map_err(super::map_source_action_error)?;
            let coverage = covered.source_split_admission().ok_or(DatasetPreparationError::InvalidEvidence)?;
            let source_parents = DerivedGenerationParents::try_new(coverage.source_manifests().to_vec())
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?.as_slice().to_vec();
            let duration = deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(5));
            if duration.is_zero() { return Err(DatasetPreparationError::Cancelled); }
            let authorization = match self.research.analytical().authorize_research_use(
                ResearchUseRequest::try_new(source_parents.clone(), ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(source_parents.len(), 16_384, 65_536, 4_096,
                        16 * 1024 * 1024, duration, Duration::from_secs(300))
                        .map_err(|_| DatasetPreparationError::Capacity)?)
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?, &cancellation) {
                Ok(value) => value,
                Err(ResearchUseCatalogError::Denied { .. }) => {
                    for id in reference.requested_instruments().iter().filter(|id| partition.instrument_ids().binary_search(id).is_ok()) {
                        unavailable.push(CurrentPopulationInputUnavailable::new(*id, CurrentPopulationInputUnavailableReason::SourceRightsUnavailable));
                    }
                    continue;
                }
                Err(error) => return Err(map_current_service_error(crate::application::research::map_research_use_error(error))),
            };
            if authorization.graph().roots() != source_parents || authorization.research_use() != ResearchUse::LocalAnalysis {
                return Err(DatasetPreparationError::Authority);
            }
            let rights_expiry = authorization.expires_at();
            let _permit = authorization.into_permit();
            let ordinary = action_source.ordinary_coverage().ok_or(DatasetPreparationError::InvalidEvidence)?;
            let mut batch = Vec::new();
            for candidate in prepared.candidates.iter().filter(|candidate| partition.instrument_ids().binary_search(&candidate.instrument_id).is_ok()
                && reference.requested_instruments().contains(&candidate.instrument_id)) {
                let (read, calendar) = ordinary.reads().iter().find(|(read, _)| read.history().selection().receipt().instrument_id() == candidate.instrument_id)
                    .ok_or(DatasetPreparationError::InvalidEvidence)?;
                // Current selection needs the original live source's actual currentness.
                // A backup-only retained calendar cannot supply that authority.
                let SourcePlanCalendar::Live(calendar) = calendar else {
                    return Err(DatasetPreparationError::InvalidEvidence);
                };
                let history = read.history();
                if history.read_receipt().knowledge_cutoff() != source_cutoff {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                let source = history.try_current_nominal_daily_source(source_cutoff, deadline, &cancellation)
                    .map_err(|_| check_control(deadline, &cancellation).err().unwrap_or(DatasetPreparationError::InvalidEvidence))?;
                match select_current_nominal_source(source, history, calendar, profile, source_cutoff, deadline, &cancellation)? {
                    CurrentSourceSelection::Unavailable(reason) => unavailable.push(CurrentPopulationInputUnavailable::new(candidate.instrument_id, reason)),
                    CurrentSourceSelection::Available(mut value) => {
                        if value.current.observation.currency() != candidate.quote_currency { return Err(DatasetPreparationError::InvalidEvidence); }
                        if current_time()? >= rights_expiry { return Err(DatasetPreparationError::Expired); }
                        value.expires_at = value.expires_at.min(rights_expiry);
                        for parent in &source_parents { push_parent(&mut value.additional_parents, parent)?; }
                        let parent_bytes = value.additional_parents.iter().try_fold(0_usize, |bytes, parent| {
                            bytes.checked_add(4 * std::mem::size_of::<DatasetManifestRef>())
                                .and_then(|bytes| bytes.checked_add(4 * parent.dataset_id().as_str().len()))
                                .and_then(|bytes| bytes.checked_add(4 * parent.schema().name().len()))
                                .and_then(|bytes| bytes.checked_add(512)).ok_or(DatasetPreparationError::Capacity)
                        })?;
                        value.retained_bytes = value.retained_bytes.checked_add(parent_bytes).ok_or(DatasetPreparationError::Capacity)?;
                        selected_bytes = selected_bytes.checked_add(value.retained_bytes)
                            .filter(|bytes| *bytes <= MAXIMUM_CURRENT_SOURCE_BYTES).ok_or(DatasetPreparationError::Capacity)?;
                        batch.push(value);
                    }
                }
            }
            // One shared original proof pool per acquisition batch. The owning raw histories drop
            // here; each selected source retains only its compact actual pair and the shared plan.
            let plan = Arc::new(action_source.into_covered_price_plan().map_err(super::map_source_action_error)?);
            selected_bytes = selected_bytes.checked_add(plan.retained_bytes())
                .filter(|bytes| *bytes <= MAXIMUM_CURRENT_SOURCE_BYTES).ok_or(DatasetPreparationError::Capacity)?;
            for mut value in batch { value.source_action_plan = Some(Arc::clone(&plan)); selected.push(value); }
        }
        let selected_refs = selected.iter().collect::<Vec<_>>();
        validate_population_coverage(partition.instrument_ids(), &selected_refs, &unavailable)?;
        if selected.is_empty() {
            return all_current_inputs_unavailable(&prepared.population_reference, partition, &[], unavailable,
                CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable);
        }
        drop(selected_refs);
        let mut macro_inputs = BTreeMap::new();
        let mut macro_bytes = 0_usize;
        for source in &mut selected {
            let date = current_source_date(&source.current)?;
            if !macro_inputs.contains_key(&date) {
                let input = self.read_current_macro(date, source_cutoff, deadline, &cancellation).await?;
                if let CurrentMacroSelection::Available { vector, .. } = &input {
                    // Retain only the fixed vector; the potentially large raw snapshot is dropped
                    // before reading the next origin date.
                    for component in vector.components() {
                        macro_bytes = macro_bytes.checked_add(component.retained_bytes()
                            .map_err(|_| DatasetPreparationError::Capacity)?)
                            .filter(|bytes| *bytes <= MAXIMUM_CURRENT_COMPONENT_BYTES)
                            .ok_or(DatasetPreparationError::Capacity)?;
                    }
                    let parent_bytes = vector.parent_manifests().iter().try_fold(0_usize, |bytes, parent| {
                        bytes.checked_add(4 * std::mem::size_of::<DatasetManifestRef>())
                            .and_then(|bytes| bytes.checked_add(4 * parent.dataset_id().as_str().len()))
                            .and_then(|bytes| bytes.checked_add(4 * parent.schema().name().len()))
                            .and_then(|bytes| bytes.checked_add(512))
                            .ok_or(DatasetPreparationError::Capacity)
                    })?;
                    macro_bytes = macro_bytes.checked_add(parent_bytes)
                        .and_then(|bytes| bytes.checked_add(16_384))
                        .filter(|bytes| *bytes <= MAXIMUM_CURRENT_COMPONENT_BYTES)
                        .ok_or(DatasetPreparationError::Capacity)?;
                }
                macro_inputs.insert(date, input);
            }
            if let Some(CurrentMacroSelection::Available { expires_at, .. }) = macro_inputs.get(&date) {
                source.expires_at = source.expires_at.min(*expires_at);
            }
        }

        let selected_refs = selected.iter().collect::<Vec<_>>();
        self.prepare_current_find_partition(
            &prepared.population_reference,
            partition,
            &selected_refs,
            &macro_inputs,
            unavailable,
            profile,
            source_cutoff,
            &mut 0,
            deadline,
            &cancellation,
        )
        .await
    }

    /// Reads and authorizes one macro snapshot before retaining only its bounded feature vector.
    async fn read_current_macro(
        &self,
        date: CalendarDate,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CurrentMacroSelection, DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let snapshot = match self.macro_context.read_latest_known(
            cutoff, date, deadline, cancellation.child_token(),
        ).await {
            Ok(snapshot) => snapshot,
            Err(ServiceError::Unavailable) => return Ok(CurrentMacroSelection::Unavailable(
                CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable,
            )),
            Err(error) => return Err(map_current_service_error(error)),
        };
        let parents = snapshot.evidence().consumed_parent_manifests().to_vec();
        if parents.is_empty() {
            return Ok(CurrentMacroSelection::Unavailable(
                CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable,
            ));
        }
        let duration = deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(5));
        if duration.is_zero() { return Err(DatasetPreparationError::Cancelled); }
        let authorization = match self.research.analytical().authorize_research_use(
            ResearchUseRequest::try_new(parents.clone(), ResearchUse::LocalAnalysis,
                ResearchUseLimits::try_new(parents.len(), 16_384, 65_536, 4_096,
                    16 * 1024 * 1024, duration, Duration::from_secs(300))
                    .map_err(|_| DatasetPreparationError::Capacity)?,
            ).map_err(|_| DatasetPreparationError::InvalidEvidence)?, cancellation,
        ) {
            Ok(authorization) => authorization,
            Err(ResearchUseCatalogError::Denied { .. }) => return Ok(CurrentMacroSelection::Unavailable(
                CurrentPopulationInputUnavailableReason::SourceRightsUnavailable,
            )),
            Err(error) => return Err(map_current_service_error(
                crate::application::research::map_research_use_error(error),
            )),
        };
        if authorization.graph().roots() != parents || authorization.research_use() != ResearchUse::LocalAnalysis {
            return Err(DatasetPreparationError::Authority);
        }
        let expires_at = authorization.expires_at();
        let _permit = authorization.into_permit();
        let vector = match MacroFeatureVector::try_from_snapshot(&snapshot) {
            Ok(vector) => vector,
            Err(ServiceError::Unavailable) => return Ok(CurrentMacroSelection::Unavailable(
                CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable,
            )),
            Err(error) => return Err(map_current_service_error(error)),
        };
        check_control(deadline, cancellation)?;
        if current_time()? >= expires_at { return Err(DatasetPreparationError::Expired); }
        Ok(CurrentMacroSelection::Available { vector, expires_at })
    }

    /// Rebinds only custody loaded by the existing DecisionJournal to the reopened original seal.
    /// Parsed references alone cannot invoke this authority boundary.
    pub(crate) fn rebind_current_find_partition(
        &self,
        prepared: &PreparedCurrentFindFeatures,
        retained: &crate::application::decision::current_find::RetainedCurrentFindPartition,
    ) -> Result<CurrentFindPartitionPreparationEvidence, DatasetPreparationError> {
        let reference = retained.evidence_reference();
        reference
            .validate()
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let partition = prepared
            .partitions
            .get(reference.partition.ordinal())
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if reference.population_reference != prepared.population_reference
            || reference.analytical_cutoff != prepared.analytical_cutoff()
            || reference.population_member_count != prepared.population.instrument_ids().len()
            || &reference.partition != partition.descriptor()
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        Ok(CurrentFindPartitionPreparationEvidence {
            partition: partition.clone(),
            population_reference: reference.population_reference.clone(),
            unavailable: reference.unavailable.clone(),
            expected_dataset_id: reference
                .expected_dataset_id
                .as_ref()
                .map(|id| {
                    DatasetId::try_from(id.as_ref())
                        .map_err(|_| DatasetPreparationError::InvalidEvidence)
                })
                .transpose()?,
            expected_build_spec: reference.expected_build_spec.map(Sha256Digest::new),
        })
    }

    /// Reopens one original journal-owned partition output without reconstructing the population.
    /// This is output custody only; it does not mint a screen population or new feature authority.
    pub(crate) fn read_current_find_partition_dataset(
        &self,
        retained: &crate::application::decision::current_find::RetainedCurrentFindPartition,
        deadline: Instant, cancellation: &CancellationToken,
    ) -> Result<Option<market_squawk_data::AnalyticalFeatureDataset>, DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let reference = retained.evidence_reference();
        reference.validate().map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        match (reference.expected_dataset_id(), reference.expected_build_spec()) {
            (None, None) => Ok(None),
            (Some(id), Some(build)) => self.reader.feature_dataset_for_build(
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                &DatasetId::try_from(id).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
                DatasetBuildSpecDigest::try_new(build).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
                deadline, cancellation,
            ).map_err(map_current_analytical_error)?.ok_or(DatasetPreparationError::Unavailable).map(Some),
            _ => Err(DatasetPreparationError::InvalidEvidence),
        }
    }

    /// Reopens the unique actual child publication by its source-issued output and build digest.
    /// A missing child never becomes an all-unavailable partition assessment.
    pub(crate) fn read_current_find_partition(
        &self,
        evidence: CurrentFindPartitionPreparationEvidence,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CurrentFindScreenPartition, DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let dataset = match (
            evidence.expected_dataset_id.as_ref(),
            evidence.expected_build_spec,
        ) {
            (Some(dataset_id), Some(build_spec)) => self
                .reader
                .feature_dataset_for_build(
                    FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                    dataset_id,
                    DatasetBuildSpecDigest::try_new(build_spec.bytes())
                        .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
                    deadline,
                    cancellation,
                )
                .map_err(map_current_analytical_error)?
                .ok_or(DatasetPreparationError::Unavailable)
                .map(Some)?,
            (None, None) => None,
            _ => return Err(DatasetPreparationError::InvalidEvidence),
        };
        evidence.complete(dataset)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "same source, partition and current-use authorities remain explicit"
    )]
    async fn prepare_current_find_partition(
        &self,
        population_reference: &FindPopulationReference,
        partition: CurrentListedPopulationPartition,
        selected: &[&SelectedCurrentSource],
        macro_inputs: &BTreeMap<CalendarDate, CurrentMacroSelection>,
        mut unavailable: Vec<CurrentPopulationInputUnavailable>,
        profile: &ValidatedAnalyticalProfile,
        source_cutoff: Timestamp,
        component_bytes: &mut usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedCurrentFindFeaturePartition, DatasetPreparationError> {
        unavailable.sort_unstable_by_key(CurrentPopulationInputUnavailable::instrument_id);
        validate_population_coverage(partition.instrument_ids(), selected, &unavailable)?;
        if selected.is_empty() {
            return all_current_inputs_unavailable(
                population_reference,
                partition,
                selected,
                unavailable,
                CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable,
            );
        }
        let mut admitted = Vec::new();
        let mut vectors = BTreeMap::new();
        for source in selected {
            let date = current_source_date(&source.current)?;
            match macro_inputs.get(&date).ok_or(DatasetPreparationError::InvalidEvidence)? {
                CurrentMacroSelection::Available { vector, .. } => {
                    admitted.push(*source);
                    vectors.insert(date, vector);
                }
                CurrentMacroSelection::Unavailable(reason) => unavailable.push(
                    CurrentPopulationInputUnavailable::new(source.instrument, *reason),
                ),
            }
        }
        unavailable.sort_unstable_by_key(CurrentPopulationInputUnavailable::instrument_id);
        let selected = admitted.as_slice();
        validate_population_coverage(partition.instrument_ids(), selected, &unavailable)?;
        if selected.is_empty() {
            return all_current_inputs_unavailable(population_reference, partition, selected,
                unavailable, CurrentPopulationInputUnavailableReason::RequiredFeatureUnavailable);
        }
        let current_population = partition.population();
        let mut parents = Vec::new();
        for source in selected {
            push_parent(&mut parents, &source.manifest)?;
            for parent in &source.additional_parents {
                push_parent(&mut parents, parent)?;
            }
        }
        for vector in vectors.values() {
            for manifest in vector.parent_manifests() {
                push_parent(&mut parents, manifest)?;
            }
        }
        if parents.len() > market_squawk_data::MAX_DERIVED_GENERATION_PARENTS {
            return Err(DatasetPreparationError::Capacity);
        }
        let parents = DerivedGenerationParents::try_new(parents)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let parents = parents.as_slice().to_vec();
        let duration = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(5));
        if duration.is_zero() {
            return Err(DatasetPreparationError::Cancelled);
        }
        let authorization = match self.research.analytical().authorize_research_use(
            ResearchUseRequest::try_new(
                parents.clone(),
                ResearchUse::LocalAnalysis,
                ResearchUseLimits::try_new(
                    parents.len(),
                    16_384,
                    65_536,
                    4_096,
                    16 * 1024 * 1024,
                    duration,
                    Duration::from_secs(300),
                )
                .map_err(|_| DatasetPreparationError::Capacity)?,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            &cancellation,
        ) {
            Ok(authorization) => authorization,
            Err(ResearchUseCatalogError::Denied { .. }) => {
                return all_current_inputs_unavailable(
                    population_reference,
                    partition,
                    selected,
                    unavailable,
                    CurrentPopulationInputUnavailableReason::SourceRightsUnavailable,
                );
            }
            Err(ResearchUseCatalogError::Cancelled | ResearchUseCatalogError::DeadlineExceeded) => {
                return Err(DatasetPreparationError::Cancelled);
            }
            Err(ResearchUseCatalogError::LimitExceeded) => {
                return Err(DatasetPreparationError::Capacity);
            }
            Err(_) => return Err(DatasetPreparationError::Authority),
        };
        if authorization.graph().roots() != parents
            || authorization.research_use() != ResearchUse::LocalAnalysis
        {
            return Err(DatasetPreparationError::Authority);
        }
        let rights_decision = authorization.decision_digest();
        let rights_graph = authorization.graph().digest();
        let rights_expiry = authorization.expires_at();
        let _permit = authorization.into_permit();
        let contract =
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1;
        let feature_spec = FeatureLabelComponentSpec::try_new(
            ComponentKind::Feature,
            ComponentScope::Instrument,
            CorporateActionSensitivity::RequiresAdjustment,
            contract.feature_component_name(),
            std::num::NonZeroU32::MIN,
        )
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
        let study = DatasetStudyPolicy::try_new(
            HistoricalStudyBasis::HistoricalAsKnown,
            DatasetBuildPurpose::StudyInputs,
            source_cutoff,
            None,
            DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(
                RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u64,
            )),
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let mut identity = Sha256::new();
        identity.update(b"market-squawk/current-find-study-inputs/v1\0");
        identity.update(population_reference.selection_digest());
        identity.update(current_population.content_digest().bytes());
        identity.update(current_population.audit_digest().bytes());
        identity.update(partition.descriptor().partition_digest());
        identity.update(profile.resolution().configuration_digest.as_bytes());
        identity.update(source_cutoff.unix_nanos().to_be_bytes());
        for source in selected {
            identity.update(source.instrument.as_uuid().as_bytes());
            identity.update(source.current.effective.unix_nanos().to_be_bytes());
            identity.update(source.current.session_evidence.bytes());
        }
        for parent in &parents {
            hash_manifest(&mut identity, parent);
        }
        let identity = Sha256Digest::new(identity.finalize().into());
        let mut examples = Vec::new();
        let mut feature_content = Vec::new();
        let mut feature_audit = Vec::new();
        let mut returns = Vec::new();
        for (ordinal, source) in selected.iter().enumerate() {
            check_control(deadline, &cancellation)?;
            let origin = source.current.effective;
            let target = origin.checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            if source_cutoff >= target {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            let vector = vectors.get(&current_source_date(&source.current)?)
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
            if current_time()? >= rights_expiry {
                return Err(DatasetPreparationError::Expired);
            }
            let original_plan = source.source_action_plan.as_ref().ok_or(DatasetPreparationError::InvalidEvidence)?;
            let plan = super::project_source_price_plan(original_plan, source.instrument, source_cutoff,
                source.current.effective, &source.prior, &source.current, deadline, cancellation)?;
            let value = split_adjusted_return(&source.prior, &source.current, &plan)?;
            let feature = return_component(
                feature_spec.clone(),
                value,
                vec![
                    market_bar_family(&source.prior)?,
                    market_bar_family(&source.current)?,
                ],
                current_source_coordinate(&source.current),
                None,
                adjustment_evidence(&plan)?,
            )?;
            let mut components = Vec::with_capacity(vector.components().len() + 1);
            components.push(feature.clone());
            components.extend(vector.components().iter().cloned());
            for component in &components {
                *component_bytes = component_bytes
                    .checked_add(
                        component
                            .retained_bytes()
                            .map_err(|_| DatasetPreparationError::Capacity)?,
                    )
                    .filter(|bytes| *bytes <= MAXIMUM_CURRENT_COMPONENT_BYTES)
                    .ok_or(DatasetPreparationError::Capacity)?;
            }
            let example_id = format!("current-{}-{ordinal:02}", short_hex(identity));
            let example = if let Some(nominal) = &source.nominal_source {
                nominal.try_dataset_example(&example_id, study, components, deadline, cancellation)
                    .and_then(|example| example.try_with_source_price_plan(Arc::clone(original_plan)))
            } else {
                market_squawk_data::DatasetExample::try_new_with_temporal_cutoffs(
                    example_id, source.instrument, source_cutoff, None, source_cutoff,
                    ResearchTemporalCoordinate::exact(origin),
                    ResearchTemporalCoordinate::exact(target), components,
                )
            }.map_err(|_| check_control(deadline, cancellation).err()
                .unwrap_or(DatasetPreparationError::InvalidEvidence))?;
            examples.push(example);
            feature_content.push(aggregate_evidence(
                b"market-squawk/current-feature-source-content/v1",
                &[source.pit_content, component_content_evidence(&feature)],
            ));
            feature_audit.push(aggregate_evidence(
                b"market-squawk/current-feature-source-audit/v1",
                &[source.pit_audit, plan_audit_evidence(&plan)],
            ));
            returns.push(evidence_digest(
                b"market-squawk/study-feature-return-output/v1",
                &[
                    EvidencePart::Text(&value.normalize().to_string()),
                    EvidencePart::Sha256(plan.content_hash()),
                    EvidencePart::Sha256(plan.audit_hash()),
                ],
            ));
        }
        let first_vector = vectors.values().next().ok_or(DatasetPreparationError::InvalidEvidence)?;
        let mut specs = vec![feature_spec];
        specs.extend(
            first_vector
                .components()
                .iter()
                .map(|component| component.spec().clone()),
        );
        let example_count = examples.len();
        let population_content = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            current_population.content_digest().bytes(),
        );
        let population_audit = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            current_population.audit_digest().bytes(),
        );
        let inputs = DatasetBuildInputs::try_new_for_current_population(
            parents,
            partition.clone(),
            specs,
            examples,
            unavailable.clone(),
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        // The earlier empty partitions are bookkeeping only. This feature-only publication is
        // current inference evidence, and supplies no training labels or held-out scoring claim.
        let split = ChronologicalSplitPolicy::try_new(
            source_cutoff
                .checked_add_nanos(-2)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            source_cutoff
                .checked_add_nanos(-1)
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            source_cutoff,
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let policy = DatasetBuildPolicy::new(
            split,
            pit,
            adjustment,
            MissingValuePolicy::Reject,
            SourceIdentifier::try_from(contract.implementation_revision())
                .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
            Some(study),
        );
        let request = dataset_request(
            identity,
            DatasetPreparationUse::LocalAnalysis,
            inputs,
            policy,
            example_count,
        )?;
        for parent in request.parent_manifests() {
            let generation = self
                .reader
                .latest(parent.dataset_id(), deadline, &cancellation)
                .map_err(|_| DatasetPreparationError::Unavailable)?
                .ok_or(DatasetPreparationError::StaleCatalog)?;
            if generation.manifest() != parent {
                return Err(DatasetPreparationError::StaleCatalog);
            }
        }
        self.research
            .analytical()
            .dataset_builder()
            .validate_request_authority(&request, &cancellation)
            .map_err(|_| DatasetPreparationError::Authority)?;
        check_control(deadline, &cancellation)?;
        let completed_at = current_time()?;
        if completed_at >= rights_expiry
            || selected
                .iter()
                .any(|source| completed_at >= source.expires_at)
        {
            return Err(DatasetPreparationError::Expired);
        }
        let evidence = PreparedProductionEvidence {
            universe_membership_content: population_content,
            universe_membership_audit: population_audit,
            instrument_population_query: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                population_reference.source_population_digest(),
            ),
            instrument_population_receipt: evidence_digest(
                b"market-squawk/current-population-use/v1",
                &[
                    EvidencePart::Bytes(&population_reference.selection_digest()),
                    EvidencePart::Bytes(&rights_decision.bytes()),
                    EvidencePart::Bytes(&rights_graph.bytes()),
                    EvidencePart::Timestamp(rights_expiry),
                    EvidencePart::Timestamp(completed_at),
                ],
            ),
            completed_session_request: aggregate_evidence(
                b"market-squawk/current-session-request/v1",
                &selected
                    .iter()
                    .map(|value| value.session_request)
                    .collect::<Vec<_>>(),
            ),
            completed_session_receipt: aggregate_evidence(
                b"market-squawk/current-session-receipt/v1",
                &selected
                    .iter()
                    .map(|value| value.session_receipt)
                    .collect::<Vec<_>>(),
            ),
            feature_point_in_time_content: aggregate_evidence(
                b"market-squawk/feature-pit-content-set/v1",
                &feature_content,
            ),
            feature_point_in_time_audit: aggregate_evidence(
                b"market-squawk/feature-pit-audit-set/v1",
                &feature_audit,
            ),
            macro_context_evidence: aggregate_evidence(
                b"market-squawk/current-origin-macro-evidence/v1",
                &vectors.values().map(|vector| vector.downstream_evidence_digest()).collect::<Vec<_>>(),
            ),
            macro_parent_manifests: {
                let mut parents = Vec::new();
                for vector in vectors.values() {
                    for parent in vector.parent_manifests() { push_parent(&mut parents, parent)?; }
                }
                parents.into_boxed_slice()
            },
            label_point_in_time_content: None,
            label_point_in_time_audit: None,
            return_kernel_output: aggregate_evidence(
                b"market-squawk/return-kernel-output-set/v1",
                &returns,
            ),
        };
        Ok(PreparedCurrentFindFeaturePartition {
            partition,
            population_reference: population_reference.clone(),
            build: Some(PreparedFeatureDatasetBuild {
                finalizer: FeatureDatasetProductionFinalizer {
                    contract,
                    build_spec: request.build_spec_digest().digest(),
                    evidence: Some(evidence),
                    maximum_currentness_expires_at: Some(
                        selected.iter().fold(rights_expiry, |expiry, source| {
                            expiry.min(source.expires_at)
                        }),
                    ),
                },
                request,
            }),
            unavailable: unavailable.into_boxed_slice(),
        })
    }
}

fn all_current_inputs_unavailable(
    population_reference: &FindPopulationReference,
    partition: CurrentListedPopulationPartition,
    selected: &[&SelectedCurrentSource],
    mut unavailable: Vec<CurrentPopulationInputUnavailable>,
    reason: CurrentPopulationInputUnavailableReason,
) -> Result<PreparedCurrentFindFeaturePartition, DatasetPreparationError> {
    unavailable.extend(
        selected
            .iter()
            .map(|value| CurrentPopulationInputUnavailable::new(value.instrument, reason)),
    );
    unavailable.sort_unstable_by_key(CurrentPopulationInputUnavailable::instrument_id);
    validate_population_coverage(partition.instrument_ids(), &[], &unavailable)?;
    Ok(PreparedCurrentFindFeaturePartition {
        partition,
        population_reference: population_reference.clone(),
        build: None,
        unavailable: unavailable.into_boxed_slice(),
    })
}

fn map_current_service_error(error: ServiceError) -> DatasetPreparationError {
    match error {
        ServiceError::Cancelled | ServiceError::DeadlineExceeded => {
            DatasetPreparationError::Cancelled
        }
        ServiceError::ResourceExhausted => DatasetPreparationError::Capacity,
        ServiceError::Unauthorized => DatasetPreparationError::Unauthorized,
        ServiceError::InvalidRequest | ServiceError::InvalidResult => {
            DatasetPreparationError::InvalidEvidence
        }
        _ => DatasetPreparationError::Unavailable,
    }
}

fn map_current_analytical_error(
    error: market_squawk_data::AnalyticalReadError,
) -> DatasetPreparationError {
    use market_squawk_data::AnalyticalReadError;
    let service_error = match error {
        AnalyticalReadError::Manifest(error) => {
            crate::application::research::map_manifest_error(error)
        }
        AnalyticalReadError::Query(error) => crate::application::research::map_query_error(error),
        AnalyticalReadError::Parquet(error) => {
            crate::application::research::source_errors::map_parquet_error(error)
        }
        AnalyticalReadError::PythonDataset(error) => {
            crate::application::research::map_python_dataset_error(error)
        }
        AnalyticalReadError::NativeSessionControl(error) => match error {
            market_squawk_platform::ResearchObjectControlError::Cancelled => {
                ServiceError::Cancelled
            }
            market_squawk_platform::ResearchObjectControlError::DeadlineExceeded => {
                ServiceError::DeadlineExceeded
            }
            market_squawk_platform::ResearchObjectControlError::Unavailable => {
                ServiceError::Unavailable
            }
        },
        _ => ServiceError::InvalidResult,
    };
    map_current_service_error(service_error)
}

fn validate_population_coverage(
    member_ids: &[InstrumentId],
    selected: &[&SelectedCurrentSource],
    unavailable: &[CurrentPopulationInputUnavailable],
) -> Result<(), DatasetPreparationError> {
    let actual = selected
        .iter()
        .map(|value| value.instrument)
        .chain(
            unavailable
                .iter()
                .map(CurrentPopulationInputUnavailable::instrument_id),
        )
        .collect::<std::collections::BTreeSet<_>>();
    if actual.len() != selected.len() + unavailable.len()
        || actual.len() != member_ids.len()
        || member_ids.iter().any(|id| !actual.contains(id))
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    Ok(())
}

/// Current nominal input consumes a source-issued pair and the original replayed calendar.
/// Native dates stay on source bars; the independently retained session supplies each close.
fn select_current_nominal_source(
    source: NominalDailyCurrentSource,
    history: &market_squawk_data::CompleteMarketBarHistoryOutput,
    calendar: &CompletedMarketSessionRead,
    profile: &ValidatedAnalyticalProfile,
    cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CurrentSourceSelection, DatasetPreparationError> {
    check_control(deadline, cancellation)?;
    let prior = source.prior_bar();
    let current = source.current_bar();
    let prior_date = prior.time_semantics().nominal_daily_date()
        .ok_or(DatasetPreparationError::InvalidEvidence)?.date();
    let current_date = current.time_semantics().nominal_daily_date()
        .ok_or(DatasetPreparationError::InvalidEvidence)?.date();
    if source.source_cutoff() != cutoff
        || source.manifest() != history.selection().pinned().manifest()
        || !same_series(prior, current)
        || source.prior_close() >= source.current_close()
        || source.current_close() > cutoff
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    let age = cutoff.unix_nanos().checked_sub(source.current_close().unix_nanos())
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    if age > profile.recommendation_policy().parameters().forecast_max_age_nanos {
        return Ok(CurrentSourceSelection::Unavailable(
            CurrentPopulationInputUnavailableReason::FreshnessUnavailable,
        ));
    }
    let mut latest = calendar.native_session_replay().sessions().iter().rev()
        .filter(|session| session.closes_at_exclusive() <= cutoff);
    if latest.next().is_none_or(|session| session.date() != current_date)
        || latest.next().is_none_or(|session| session.date() != prior_date)
    {
        return Ok(CurrentSourceSelection::Unavailable(
            CurrentPopulationInputUnavailableReason::CalendarUnavailable,
        ));
    }
    let evaluated_at = current_time()?;
    let mut receipts = Vec::with_capacity(2);
    for (date, expected_close) in [(prior_date, source.prior_close()), (current_date, source.current_close())] {
        let Some(receipt) = calendar.date_session_on(date, cutoff, evaluated_at) else {
            return Ok(CurrentSourceSelection::Unavailable(
                CurrentPopulationInputUnavailableReason::CalendarUnavailable,
            ));
        };
        if receipt.closes_at_exclusive() != expected_close || receipt.available_at() > cutoff {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        receipts.push(evidence_digest(
            b"market-squawk/current-nominal-calendar-session/v1",
            &[
                EvidencePart::Digest(receipt.evidence_digest()),
                EvidencePart::Text(&date.to_string()),
                EvidencePart::Timestamp(receipt.opens_at()),
                EvidencePart::Timestamp(receipt.closes_at_exclusive()),
            ],
        ));
    }
    let native = history.native_sessions().ok_or(DatasetPreparationError::InvalidEvidence)?;
    let content = evidence_digest(b"market-squawk/current-nominal-price-source/v1", &[
        EvidencePart::Manifest(source.manifest()),
        EvidencePart::Sha256(history.read_receipt().history_content_digest()),
        EvidencePart::Sha256(history.read_receipt().result_digest()),
        EvidencePart::Digest(native.mapping_digest()),
    ]);
    let observed_availability = |bar: &MarketBarObservation| {
        bar.context().provenance().availability().conservative_available_at()
            .filter(|available| *available <= cutoff)
            .ok_or(DatasetPreparationError::InvalidEvidence)
    };
    let prior_point = MarketSeriesPoint {
        observation: prior.clone(), manifest: source.manifest().clone(),
        session_evidence: receipts[0], effective: source.prior_close(),
        available_at: observed_availability(prior)?.max(native.received_at()),
    };
    let current_point = MarketSeriesPoint {
        observation: current.clone(), manifest: source.manifest().clone(),
        session_evidence: receipts[1], effective: source.current_close(),
        available_at: observed_availability(current)?.max(native.received_at()),
    };
    let retained_bytes = selected_price_bytes(prior, current)?.checked_add(source.retained_bytes())
        .filter(|bytes| *bytes <= MAXIMUM_CURRENT_SOURCE_BYTES)
        .ok_or(DatasetPreparationError::Capacity)?;
    let mut additional_parents = Vec::new();
    push_parent(&mut additional_parents, history.read_receipt().origin_manifest())?;
    push_parent(&mut additional_parents, calendar.source_action_calendar().manifest())?;
    let selected = SelectedCurrentSource {
        instrument: history.selection().receipt().instrument_id(),
        manifest: source.manifest().clone(), prior: prior_point, current: current_point,
        pit_content: content,
        pit_audit: evidence_digest(b"market-squawk/current-nominal-source-cutoff/v1", &[
            EvidencePart::Digest(content), EvidencePart::Timestamp(cutoff),
            EvidencePart::Digest(native.source_replay_digest()),
        ]),
        session_request: evidence_digest(b"market-squawk/current-nominal-session-request/v1", &[
            EvidencePart::Digest(calendar.reference().origin_content_digest()),
            EvidencePart::Digest(calendar.reference().capture_binding_digest()),
            EvidencePart::Timestamp(cutoff),
        ]),
        session_receipt: aggregate_evidence(b"market-squawk/current-nominal-session-receipts/v1", &receipts),
        expires_at: calendar.currentness_expires_at(), retained_bytes,
        nominal_source: Some(source), source_action_plan: None, additional_parents,
    };
    check_control(deadline, cancellation)?;
    Ok(CurrentSourceSelection::Available(selected))
}

/// Matches the original raw price series, preserving native-date rulesets separately from sessions.
fn same_series(left: &MarketBarObservation, right: &MarketBarObservation) -> bool {
    let left_context = left.context().provenance();
    let right_context = right.context().provenance();
    left_context.source_id() == right_context.source_id()
        && left_context.instrument_id() == right_context.instrument_id()
        && left_context.venue_id() == right_context.venue_id()
        && left.provider_instrument_id() == right.provider_instrument_id()
        && left.feed() == right.feed()
        && left.interval() == right.interval()
        && left.currency() == right.currency()
        && left.adjustment() == MarketBarAdjustment::Raw
        && left.adjustment() == right.adjustment()
        && left.time_semantics().timestamp_basis() == right.time_semantics().timestamp_basis()
        && left.time_semantics().session() == right.time_semantics().session()
        && left.time_semantics().nominal_daily_date().map(|date| date.ruleset())
            == right.time_semantics().nominal_daily_date().map(|date| date.ruleset())
}

fn current_source_date(point: &MarketSeriesPoint) -> Result<CalendarDate, DatasetPreparationError> {
    match point.observation.time_semantics().nominal_daily_date() {
        Some(date) => Ok(date.date()),
        None => timestamp_calendar_date(point.effective),
    }
}

fn current_source_coordinate(point: &MarketSeriesPoint) -> ResearchTemporalCoordinate {
    match point.observation.time_semantics().nominal_daily_date() {
        Some(date) => ResearchTemporalCoordinate::calendar_date(date.date()),
        None => ResearchTemporalCoordinate::exact(point.effective),
    }
}

fn selected_price_bytes(
    prior: &MarketBarObservation,
    current: &MarketBarObservation,
) -> Result<usize, DatasetPreparationError> {
    // The canonical decoder charges static observations plus doubled serialized payload size.
    // This selected-pair copy conservatively doubles that dynamic allowance once more and
    // reserves the source manifest/selection metadata, without retaining serialized copies.
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|total| *total <= MAXIMUM_CURRENT_SOURCE_BYTES)
                .ok_or_else(|| std::io::Error::other("selected source byte bound"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, prior).map_err(|_| DatasetPreparationError::Capacity)?;
    serde_json::to_writer(&mut counter, current).map_err(|_| DatasetPreparationError::Capacity)?;
    counter
        .0
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SelectedCurrentSource>()))
        .and_then(|bytes| bytes.checked_add(4_096))
        .ok_or(DatasetPreparationError::Capacity)
}

fn current_time() -> Result<Timestamp, DatasetPreparationError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DatasetPreparationError::Unavailable)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| DatasetPreparationError::Unavailable)?,
    ))
}
