//! Immutable current-screen input custody in the existing decision journal.
//!
//! Only small original-source references and partition receipts live here. Feature recipes remain
//! with one running dataset child. Reads load one bounded record from the existing journal.

mod completion;
pub(crate) mod member;
mod sources;
pub(crate) use sources::{CurrentFindSourcePageReference, CurrentFindSourcePageRecord, RetainedCurrentFindSources};
pub(crate) use completion::{CurrentFindCompletionRecord, CurrentFindCompletionReference};

use std::collections::BTreeMap;

use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{AdmittedScreenJob, DecisionApplication, DecisionApplicationError};
use crate::application::{
    analytical_profile::{AnalyticalProfileResolution, ValidatedAnalyticalProfile},
    market_calendar::{
        CompletedMarketSessionReference, ForecastSessionCohort, ForecastSessionCohortReference,
    },
    research::{
        CurrentFindPartitionEvidenceReference, CurrentFindPartitionPreparationEvidence,
        FindPopulationExclusion, FindPopulationReference, PreparedCurrentFindFeatures,
        PreparedFindPopulation,
    },
};

const MAXIMUM_RECORD_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAXIMUM_CURRENT_FIND_PARTITIONS: usize =
    PreparedCurrentFindFeatures::maximum_partition_count(
        market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS,
        sources::MAXIMUM_SOURCE_PAGES,
    );

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindPreparationRecord {
    pub(crate) preparation_id: Uuid,
    workspace_id: Uuid,
    client_id: Uuid,
    pub(crate) request_sha256: [u8; 32],
    pub(crate) profile: AnalyticalProfileResolution,
    pub(crate) population: FindPopulationReference,
    pub(crate) calendar: Option<CompletedMarketSessionReference>,
    pub(crate) forecast_cohort: Option<ForecastSessionCohortReference>,
    pub(crate) source_pages: Box<[CurrentFindSourcePageReference]>,
    pub(crate) analytical_cutoff: Timestamp,
    pub(crate) partition_count: usize,
    pub(crate) partition_ends: Box<[usize]>,
    pub(crate) population_count: usize,
    pub(crate) population_content_digest: [u8; 32],
    pub(crate) exclusions: Box<[FindPopulationExclusion]>,
}

impl CurrentFindPreparationRecord {
    pub(crate) fn from_source(
        preparation_id: Uuid,
        request_sha256: [u8; 32],
        population: &PreparedFindPopulation,
        prepared: &PreparedCurrentFindFeatures,
        profile: &ValidatedAnalyticalProfile,
        calendar: Option<CompletedMarketSessionReference>,
        forecast_cohort: Option<&ForecastSessionCohort>,
        sources: &RetainedCurrentFindSources,
        context: &RequestContext,
    ) -> Result<Self, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let source = population
            .current_population()
            .ok_or(ServiceError::Unavailable)?;
        if !population.complete() || prepared.population_reference() != population.reference()
            || prepared.source_page_references() != sources.pages() {
            return Err(ServiceError::InvalidResult);
        }
        if forecast_cohort.is_some_and(|cohort| {
            cohort.reference().horizon_nanos().ok()
                != profile
                    .horizon()
                    .step_nanos()
                    .and_then(|step| i64::try_from(step.get()).ok())
        }) {
            return Err(ServiceError::InvalidResult);
        }
        let record = Self {
            preparation_id,
            workspace_id: origin.workspace_id(),
            client_id: origin.client_id(),
            request_sha256,
            profile: profile.resolution().clone(),
            population: population.reference().clone(),
            calendar,
            forecast_cohort: forecast_cohort.map(|cohort| cohort.reference().clone()),
            source_pages: sources.pages().to_vec().into_boxed_slice(),
            analytical_cutoff: prepared.analytical_cutoff(),
            partition_count: prepared.partitions().len(),
            partition_ends: prepared.partition_ends(),
            population_count: source.instrument_ids().len(),
            population_content_digest: source.content_digest().bytes(),
            exclusions: population.exclusions().to_vec().into_boxed_slice(),
        };
        record.validate().map_err(|_| ServiceError::InvalidResult)?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), DecisionApplicationError> {
        if self.partition_ends.len() != self.partition_count
            || self.partition_ends.last().copied().unwrap_or(0) != self.population_count {
            return Err(invalid());
        }
        let mut previous_end = 0;
        for end in &self.partition_ends {
            if *end <= previous_end || *end > self.population_count || *end - previous_end > 128 {
                return Err(invalid());
            }
            previous_end = *end;
        }
        if sources::validate_references(&self.source_pages, self.analytical_cutoff)? != self.population.selected_count() {
            return Err(invalid());
        }
        if let Some(cohort) = &self.forecast_cohort {
            cohort.validate().map_err(|_| invalid())?;
            if self.calendar.as_ref() != Some(cohort.calendar())
                || cohort.knowledge_cutoff().map_err(|_| invalid())? != self.analytical_cutoff
            {
                return Err(invalid());
            }
        }
        if self
            .population
            .selected_count()
            .checked_add(self.exclusions.len())
            != self.population.canonical_population_count()
            || self.population.selected_count() != self.population_count
        {
            return Err(invalid());
        }
        if self.preparation_id.is_nil()
            || self.workspace_id.is_nil()
            || self.client_id.is_nil()
            || self.request_sha256 == [0; 32]
            || self.population_content_digest == [0; 32]
            || self.population_count > market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS
            || self.partition_count < self.population_count.div_ceil(128)
            || self.partition_count > self.population_count
            || self.partition_count > PreparedCurrentFindFeatures::maximum_partition_count(self.population_count, self.source_pages.len())
            || self.partition_count > MAXIMUM_CURRENT_FIND_PARTITIONS
            || self.calendar.is_none() != (self.partition_count == 0)
            || self.population.financial_profile_digest() != self.profile.configuration_digest
            || self.population.source_cutoff() > self.analytical_cutoff
            || self.analytical_cutoff.unix_nanos() <= 0
            || !matches!(self.population.maximum_deep_analyses(), 8 | 16 | 32)
            || self.exclusions.len() > 65_536
            || self
                .exclusions
                .windows(2)
                .any(|v| v[0].instrument_id() >= v[1].instrument_id())
            || self.population.coverage()
                != crate::application::research::FindPopulationCoverage::Complete
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<[u8; 32], DecisionApplicationError> {
        digest(self)
    }

    pub(crate) fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if self.workspace_id != origin.workspace_id() || self.client_id != origin.client_id() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindPartitionRecord {
    preparation_id: Uuid,
    preparation_sha256: [u8; 32],
    ordinal: usize,
    evidence: CurrentFindPartitionEvidenceReference,
}

/// Constructor-private evidence read from the sole journal and matched to its original parent.
#[derive(Clone, Debug)]
pub(crate) struct RetainedCurrentFindPartition {
    record: CurrentFindPartitionRecord,
}

impl RetainedCurrentFindPartition {
    pub(crate) const fn evidence_reference(&self) -> &CurrentFindPartitionEvidenceReference {
        &self.record.evidence
    }
    pub(crate) const fn ordinal(&self) -> usize {
        self.record.ordinal
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindDatasetJob {
    pub(crate) ordinal: usize,
    pub(crate) job_id: Uuid,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindScreenRecord {
    pub(crate) preparation_id: Uuid,
    preparation_sha256: [u8; 32],
    pub(crate) dataset_jobs: Box<[CurrentFindDatasetJob]>,
    pub(crate) input_identity: SourceIdentifier,
    pub(crate) input_digest: EvidenceDigest,
    pub(crate) run_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(in crate::application::decision) enum CurrentFindCustodyRecord {
    Preparation(Box<CurrentFindPreparationRecord>),
    Partition(Box<CurrentFindPartitionRecord>),
    Screen(Box<CurrentFindScreenRecord>),
    Completion(Box<CurrentFindCompletionRecord>),
    SourcePage(Box<CurrentFindSourcePageRecord>),
    MemberUnavailable(Box<member::FindMemberUnavailableRecord>),
    Results(Box<super::find_results::FindResultsRecord>),
}

impl CurrentFindCustodyRecord {
    pub(super) fn key(&self) -> String {
        match self {
            Self::MemberUnavailable(value) => value.key(),
            Self::Results(value) => value.key(),
            Self::Preparation(value) => preparation_key(value.preparation_id),
            Self::Partition(value) => partition_key(value.preparation_id, value.ordinal),
            Self::Screen(value) => screen_key(value.preparation_id),
            Self::Completion(value) => completion::key(value.preparation_id, value.ordinal),
            Self::SourcePage(value) => value.key(),
        }
    }

    pub(super) fn validate(&self) -> Result<(), DecisionApplicationError> {
        if serde_json::to_vec(self).map_err(|_| invalid())?.len() > MAXIMUM_RECORD_BYTES {
            return Err(DecisionApplicationError::Capacity);
        }
        match self {
            Self::MemberUnavailable(value) => value.validate(),
            Self::Results(value) => value.validate(),
            Self::Preparation(value) => value.validate(),
            Self::Completion(value) => value.validate(),
            Self::SourcePage(value) => value.validate(),
            Self::Partition(value) => {
                if value.preparation_id.is_nil()
                    || value.preparation_sha256 == [0; 32]
                    || value.ordinal >= MAXIMUM_CURRENT_FIND_PARTITIONS
                    || value.evidence.partition().ordinal() != value.ordinal
                    || value.evidence.unavailable().len() > 128
                {
                    return Err(invalid());
                }
                Ok(())
            }
            Self::Screen(value) => {
                if value.preparation_id.is_nil()
                    || value.preparation_sha256 == [0; 32]
                    || value.dataset_jobs.is_empty()
                    || value.dataset_jobs.len() > MAXIMUM_CURRENT_FIND_PARTITIONS
                    || value
                        .dataset_jobs
                        .windows(2)
                        .any(|v| v[0].ordinal >= v[1].ordinal)
                    || value.dataset_jobs.iter().any(|job| {
                        job.job_id.is_nil()
                            || job.generation == 0
                            || job.ordinal >= MAXIMUM_CURRENT_FIND_PARTITIONS
                    })
                    || value.input_digest.bytes() == [0; 32]
                    || value.run_id.is_empty()
                {
                    return Err(invalid());
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct CurrentFindRecoverySummary {
    digest: [u8; 32],
    workspace_id: Uuid,
    profile_sha256: [u8; 32],
    screen_run_id: Option<String>,
    results_published: bool,
    member_assessments: BTreeMap<String, ([u8; 32],[u8;32])>,
    population: FindPopulationReference,
    cutoff: Timestamp,
    partition_count: usize,
    partition_ends: Box<[usize]>,
    pub(super) population_count: usize,
    pub(super) population_content_digest: [u8; 32],
    next_ordinal: usize,
    built_ordinals: Vec<usize>,
    completed_jobs: Vec<CurrentFindDatasetJob>,
    completed_count: usize,
    completion_digest: Option<[u8; 32]>,
    pending_partition_digest: Option<[u8; 32]>,
}

pub(super) fn recover(
    record: &CurrentFindCustodyRecord,
    parents: &mut BTreeMap<Uuid, CurrentFindRecoverySummary>,
    source_pages: &mut BTreeMap<String, CurrentFindSourcePageReference>,
) -> Result<(), DecisionApplicationError> {
    record.validate()?;
    match record {
        CurrentFindCustodyRecord::SourcePage(value) => {
            let reference = value.reference()?;
            if source_pages.insert(value.key(), reference).is_some() { return Err(invalid()); }
        }
        CurrentFindCustodyRecord::Preparation(value) => {
            if value.source_pages.iter().any(|page| source_pages.get(&sources::key(page)) != Some(page)) { return Err(invalid()); }
            if parents.contains_key(&value.preparation_id) {
                return Err(invalid());
            }
            parents.insert(
                value.preparation_id,
                CurrentFindRecoverySummary {
                    digest: value.digest()?,
                    workspace_id: value.workspace_id,
                    profile_sha256: super::investment_request::parse_digest(
                        &value.profile.configuration_digest,
                    )
                    .map_err(|_| invalid())?
                    .bytes(),
                    screen_run_id: None,
                    results_published: false,
                    member_assessments: BTreeMap::new(),
                    population: value.population.clone(),
                    cutoff: value.analytical_cutoff,
                    partition_count: value.partition_count,
                    partition_ends: value.partition_ends.clone(),
                    population_count: value.population_count,
                    population_content_digest: value.population_content_digest,
                    next_ordinal: 0,
                    built_ordinals: Vec::new(),
                    completed_jobs: Vec::new(),
                    completed_count: 0,
                    completion_digest: None,
                    pending_partition_digest: None,
                },
            );
        }
        CurrentFindCustodyRecord::Partition(value) => {
            let parent = parents.get_mut(&value.preparation_id).ok_or_else(invalid)?;
            if value.preparation_sha256 != parent.digest
                || value.ordinal != parent.next_ordinal
                || value.ordinal != parent.completed_count
                || parent.pending_partition_digest.is_some()
                || value.evidence.population_reference() != &parent.population
                || value.evidence.analytical_cutoff() != parent.cutoff
                || value.evidence.partition().partition_count() != parent.partition_count
                || value.evidence.partition().member_ids().len() != partition_size(&parent.partition_ends, value.ordinal)?
                || value.evidence.partition().full_population_digest()
                    != parent.population_content_digest
            {
                return Err(invalid());
            }
            if value.evidence.expected_build_spec().is_some() {
                parent.built_ordinals.push(value.ordinal);
            }
            parent.next_ordinal += 1;
            parent.pending_partition_digest = Some(digest(value.as_ref())?);
        }
        CurrentFindCustodyRecord::Completion(value) => {
            let parent = parents.get_mut(&value.preparation_id).ok_or_else(invalid)?;
            if value.preparation_sha256 != parent.digest
                || value.ordinal != parent.completed_count
                || value.ordinal >= parent.next_ordinal
                || value.previous_sha256 != parent.completion_digest
                || Some(value.partition_sha256) != parent.pending_partition_digest
                || value.dataset_job.is_some() != parent.built_ordinals.contains(&value.ordinal)
            { return Err(invalid()); }
            if let Some(job) = &value.dataset_job { parent.completed_jobs.push(job.clone()); }
            parent.completed_count += 1;
            parent.pending_partition_digest = None;
            parent.completion_digest = Some(value.digest()?);
        }
        CurrentFindCustodyRecord::Screen(value) => {
            let parent = parents.get_mut(&value.preparation_id).ok_or_else(invalid)?;
            if value.preparation_sha256 != parent.digest
                || parent.next_ordinal != parent.partition_count
                || parent.completed_count != parent.partition_count
                || value.dataset_jobs.as_ref() != parent.completed_jobs.as_slice()
                || !value
                    .dataset_jobs
                    .iter()
                    .map(|job| job.ordinal)
                    .eq(parent.built_ordinals.iter().copied())
            {
                return Err(invalid());
            }
            if parent.screen_run_id.replace(value.run_id.clone()).is_some() {
                return Err(invalid());
            }
        }
        CurrentFindCustodyRecord::MemberUnavailable(value) => {
            let parent = parents
                .get_mut(&value.member.preparation_id)
                .ok_or_else(invalid)?;
            value.validate_parent(parent)?;
            if parent.member_assessments.len() >= parent.population.maximum_deep_analyses()
                || parent
                    .member_assessments
                    .insert(value.member.candidate_id.clone(), (value.digest()?,digest(&value.value()?)?))
                    .is_some()
            {
                return Err(invalid());
            }
        }
        CurrentFindCustodyRecord::Results(value) => {
            let parent = parents.get_mut(&value.preparation_id).ok_or_else(invalid)?;
            if parent.results_published
                || value.preparation_sha256 != parent.digest
                || value.workspace_id != parent.workspace_id
                || value.profile_sha256 != parent.profile_sha256
                || parent.completed_count != parent.partition_count
                || parent.next_ordinal != parent.partition_count
                || value.screen_run_id != parent.screen_run_id
                || (parent.screen_run_id.is_none() && !parent.built_ordinals.is_empty())
            {
                return Err(invalid());
            }
            for row in &value.rows {
                let actual = parent.member_assessments.get(&row.reference.candidate_id);
                match &row.member_unavailable {
                    Some(member) if actual == Some(&(super::investment_request::parse_digest(&member.assessment_sha256).map_err(|_|invalid())?.bytes(),digest(member)?)) => {}
                    None if actual.is_none() => {}
                    _ => return Err(invalid()),
                }
            }
            parent.results_published = true;
        }
    }
    Ok(())
}

impl DecisionApplication {
    pub(crate) fn current_find_preparation(
        &self,
        id: Uuid,
    ) -> Result<Option<CurrentFindPreparationRecord>, DecisionApplicationError> {
        let state = self.reader()?;
        match state.journal.current_find_record(&preparation_key(id))? {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::Preparation(value)) => Ok(Some(*value)),
            Some(_) => Err(invalid()),
        }
    }

    pub(crate) fn retain_current_find_preparation(
        &self,
        record: CurrentFindPreparationRecord,
        context: &RequestContext,
    ) -> Result<CurrentFindPreparationRecord, DecisionApplicationError> {
        let value = CurrentFindCustodyRecord::Preparation(Box::new(record.clone()));
        value.validate()?;
        let encoded = super::codec::current_find_custody(&value)?;
        let state = self.writer()?;
        for page in &record.source_pages {
            check_control(context)?;
            let Some(CurrentFindCustodyRecord::SourcePage(actual)) = state.journal.current_find_record(&sources::key(page))?
                else { return Err(invalid()); };
            actual.validate()?;
            if actual.reference()? != *page { return Err(invalid()); }
        }
        check_control(context)?;
        state.journal.append(&encoded)?;
        Ok(record)
    }

    pub(crate) fn current_find_partition(
        &self,
        parent: &CurrentFindPreparationRecord,
        ordinal: usize,
    ) -> Result<Option<RetainedCurrentFindPartition>, DecisionApplicationError> {
        if ordinal >= parent.partition_count {
            return Err(invalid());
        }
        let state = self.reader()?;
        match state
            .journal
            .current_find_record(&partition_key(parent.preparation_id, ordinal))?
        {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::Partition(record)) => {
                validate_partition_parent(&record, parent, parent.digest()?)?;
                Ok(Some(RetainedCurrentFindPartition { record: *record }))
            }
            Some(_) => Err(invalid()),
        }
    }

    /// Reads the committed prefix while hashing the original source record only once.
    pub(crate) fn current_find_partitions(
        &self,
        parent: &CurrentFindPreparationRecord,
    ) -> Result<Vec<RetainedCurrentFindPartition>, DecisionApplicationError> {
        let digest = parent.digest()?;
        let state = self.reader()?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(parent.partition_count)
            .map_err(|_| DecisionApplicationError::Allocation)?;
        for ordinal in 0..parent.partition_count {
            match state
                .journal
                .current_find_record(&partition_key(parent.preparation_id, ordinal))?
            {
                None => break,
                Some(CurrentFindCustodyRecord::Partition(record)) => {
                    validate_partition_parent(&record, parent, digest)?;
                    records.push(RetainedCurrentFindPartition { record: *record });
                }
                Some(_) => return Err(invalid()),
            }
        }
        Ok(records)
    }

    pub(crate) fn retain_current_find_partition(
        &self,
        parent: &CurrentFindPreparationRecord,
        evidence: &CurrentFindPartitionPreparationEvidence,
        context: &RequestContext,
    ) -> Result<RetainedCurrentFindPartition, DecisionApplicationError> {
        let ordinal = evidence.partition().ordinal();
        let record = CurrentFindPartitionRecord {
            preparation_id: parent.preparation_id,
            preparation_sha256: parent.digest()?,
            ordinal,
            evidence: evidence.reference(),
        };
        validate_partition_parent(&record, parent, parent.digest()?)?;
        let custody = CurrentFindCustodyRecord::Partition(Box::new(record.clone()));
        custody.validate()?;
        let encoded = super::codec::current_find_custody(&custody)?;
        let state = self.writer()?;
        let Some(CurrentFindCustodyRecord::Preparation(actual)) = state
            .journal
            .current_find_record(&preparation_key(parent.preparation_id))?
        else {
            return Err(invalid());
        };
        if *actual != *parent
            || (ordinal > 0
                && state
                    .journal
                    .current_find_record(&completion::key(parent.preparation_id, ordinal - 1))?
                    .is_none())
        {
            return Err(invalid());
        }
        check_control(context)?;
        state.journal.append(&encoded)?;
        Ok(RetainedCurrentFindPartition { record })
    }

    pub(crate) fn current_find_screen(
        &self,
        parent: &CurrentFindPreparationRecord,
    ) -> Result<Option<CurrentFindScreenRecord>, DecisionApplicationError> {
        match self
            .reader()?
            .journal
            .current_find_record(&screen_key(parent.preparation_id))?
        {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::Screen(record))
                if record.preparation_sha256 == parent.digest()? =>
            {
                Ok(Some(*record))
            }
            Some(_) => Err(invalid()),
        }
    }

    pub(crate) fn retain_current_find_screen(
        &self,
        parent: &CurrentFindPreparationRecord,
        dataset_jobs: Box<[CurrentFindDatasetJob]>,
        admitted: &AdmittedScreenJob,
        context: &RequestContext,
    ) -> Result<(), DecisionApplicationError> {
        let expected_jobs = dataset_jobs.to_vec();
        let record = CurrentFindCustodyRecord::Screen(Box::new(CurrentFindScreenRecord {
            preparation_id: parent.preparation_id,
            preparation_sha256: parent.digest()?,
            dataset_jobs,
            input_identity: admitted.input_identity().clone(),
            input_digest: admitted.input_digest(),
            run_id: admitted.run_id().as_str().to_owned(),
        }));
        record.validate()?;
        let encoded = super::codec::current_find_custody(&record)?;
        let state = self.writer()?;
        let parent_digest = parent.digest()?;
        let plan = state
            .screen_job_inputs
            .get(admitted.run_id().as_str())
            .ok_or_else(invalid)?;
        if plan.run().universe_identity().evidence_digest().bytes()
            != parent.population_content_digest
            || admitted.population_member_count() != parent.population_count
        {
            return Err(invalid());
        }
        let mut actual_jobs = Vec::new();
        for ordinal in 0..parent.partition_count {
            let Some(CurrentFindCustodyRecord::Partition(partition)) = state
                .journal
                .current_find_record(&partition_key(parent.preparation_id, ordinal))?
            else {
                return Err(invalid());
            };
            validate_partition_parent(&partition, parent, parent_digest)?;
            let Some(CurrentFindCustodyRecord::Completion(completed)) = state.journal.current_find_record(&completion::key(parent.preparation_id, ordinal))?
                else { return Err(invalid()); };
            completion::validate_join(&completed, &partition)?;
            if let Some(job) = completed.dataset_job { actual_jobs.push(job); }
        }
        if actual_jobs != expected_jobs {
            return Err(invalid());
        }
        check_control(context)?;
        state.journal.append(&encoded)?;
        Ok(())
    }
}

fn validate_partition_parent(
    record: &CurrentFindPartitionRecord,
    parent: &CurrentFindPreparationRecord,
    parent_digest: [u8; 32],
) -> Result<(), DecisionApplicationError> {
    if record.preparation_id != parent.preparation_id
        || record.preparation_sha256 != parent_digest
        || record.ordinal >= parent.partition_count
        || record.evidence.population_reference() != &parent.population
        || record.evidence.analytical_cutoff() != parent.analytical_cutoff
        || record.evidence.partition().partition_count() != parent.partition_count
        || record.evidence.partition().member_ids().len() != partition_size(&parent.partition_ends, record.ordinal)?
        || record.evidence.partition().full_population_digest() != parent.population_content_digest
    {
        return Err(invalid());
    }
    Ok(())
}

fn preparation_key(id: Uuid) -> String {
    format!("current-find:preparation:{id}")
}
fn partition_key(id: Uuid, ordinal: usize) -> String {
    format!("current-find:partition:{id}:{ordinal}")
}
fn screen_key(id: Uuid) -> String {
    format!("current-find:screen:{id}")
}
fn invalid() -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}
fn digest(value: &impl Serialize) -> Result<[u8; 32], DecisionApplicationError> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    if bytes.len() > MAXIMUM_RECORD_BYTES {
        return Err(DecisionApplicationError::Capacity);
    }
    Ok(Sha256::digest(bytes).into())
}

fn check_control(context: &RequestContext) -> Result<(), DecisionApplicationError> {
    if context.cancellation().is_cancelled() || std::time::Instant::now() >= context.deadline() {
        return Err(DecisionApplicationError::Unavailable);
    }
    Ok(())
}

fn partition_size(ends: &[usize], ordinal: usize) -> Result<usize, DecisionApplicationError> {
    let end = *ends.get(ordinal).ok_or_else(invalid)?;
    let start = if ordinal == 0 { 0 } else { *ends.get(ordinal - 1).ok_or_else(invalid)? };
    end.checked_sub(start).filter(|size| (1..=128).contains(size)).ok_or_else(invalid)
}

impl CurrentFindSourcePageReference {
    pub(crate) fn key(&self) -> String {
        sources::key(self)
    }
}
