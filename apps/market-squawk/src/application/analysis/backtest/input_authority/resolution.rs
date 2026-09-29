//! Fresh point-in-time materialization and immutable evidence comparison.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use market_squawk_backtesting::{BacktestDataset, BacktestRequest, ResearchExecutionAssumptions};
use market_squawk_data::{
    AnalyticalReadCapability, CompleteMarketBarHistoryOutput, CorporateActionPlan,
    DatasetManifestRef, FeatureDatasetInputEpochOutput, FeatureDatasetProductContract,
    PinnedDataset, PinnedInstrumentDefinitions, ResearchQueryEngine, Sha256Digest,
};
use market_squawk_domain::Timestamp;
use market_squawk_services::ServiceError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    PinnedBacktestInput, ResearchService,
    backtest_service::{
        PinnedBacktestCohort, PinnedBacktestCohortCandidate, PinnedBacktestCohortMember,
    },
};

use crate::application::research::corporate_actions::SourcePlanCalendar;

use super::recipe::{
    CohortMemberEvidence, ExpectedEvidence, InputCoreWire, ManifestAuthorityWire,
    sort_manifest_authorities,
};

const HARD_MAXIMUM_MANIFEST_NODES: usize = 4_096;

pub(super) struct MaterializedInput {
    body: MaterializedBody,
    pub(super) evidence: ExpectedEvidence,
}

enum MaterializedBody {
    Generic(PinnedBacktestInput),
    Study {
        epochs: FeatureDatasetInputEpochOutput,
        definitions: PinnedInstrumentDefinitions,
        actions: CorporateActionPlan,
        assumptions: ResearchExecutionAssumptions,
        limits: market_squawk_backtesting::BacktestLimits,
        histories: Vec<CompleteMarketBarHistoryOutput>,
        admitted_at: Timestamp,
    },
}

#[derive(Debug)]
pub(super) struct RecommendationInputFacts {
    pub(super) dataset_identity: Sha256Digest,
    pub(super) study_qualification: market_squawk_backtesting::BacktestStudyQualification,
    pub(super) raw_price_evidence_digest: Sha256Digest,
    pub(super) corporate_action_content_digest: Sha256Digest,
    pub(super) corporate_action_audit_digest: Sha256Digest,
    pub(super) corporate_action_valuation_cutoff: Timestamp,
    pub(super) admitted_at: Timestamp,
}

impl MaterializedInput {
    pub(super) fn into_generic(self) -> Result<PinnedBacktestInput, ServiceError> {
        match self.body {
            MaterializedBody::Generic(input) => Ok(input),
            _ => Err(ServiceError::InvalidRequest),
        }
    }
    fn generic_mut(&mut self) -> Result<&mut PinnedBacktestInput, ServiceError> {
        match &mut self.body {
            MaterializedBody::Generic(input) => Ok(input),
            _ => Err(ServiceError::InvalidRequest),
        }
    }
    pub(super) fn has_daily_history(&self) -> bool {
        matches!(&self.body, MaterializedBody::Study { .. })
    }
    pub(super) fn validate_registration(
        self,
    ) -> Result<Option<RecommendationInputFacts>, ServiceError> {
        let admitted_at = match &self.body {
            MaterializedBody::Study { admitted_at, .. } => Some(*admitted_at),
            MaterializedBody::Generic(_) => None,
        };
        if let Some(admitted_at) = admitted_at {
            let (dataset, actions, _) = self.into_recommendation_dataset()?;
            return Ok(Some(RecommendationInputFacts {
                dataset_identity: dataset.identity(),
                study_qualification: dataset
                    .study_qualification()
                    .ok_or(ServiceError::InvalidResult)?,
                raw_price_evidence_digest: dataset
                    .raw_execution_history_digest()
                    .ok_or(ServiceError::InvalidResult)?,
                corporate_action_content_digest: actions.content_hash(),
                corporate_action_audit_digest: actions.audit_hash(),
                corporate_action_valuation_cutoff: actions.valuation_cutoff(),
                admitted_at,
            }));
        }
        validate_pinned_input(self.into_generic()?)?;
        Ok(None)
    }
    pub(super) fn into_recommendation_dataset(
        self,
    ) -> Result<
        (
            BacktestDataset,
            CorporateActionPlan,
            ResearchExecutionAssumptions,
        ),
        ServiceError,
    > {
        match self.body {
            MaterializedBody::Study {
                epochs,
                definitions,
                actions,
                assumptions,
                limits,
                histories,
                admitted_at,
            } => {
                if actions.knowledge_cutoff() > admitted_at {
                    return Err(ServiceError::InvalidResult);
                }
                let dataset = BacktestDataset::try_from_study_input_epochs(
                    epochs,
                    definitions,
                    histories,
                    admitted_at,
                    limits,
                )
                .map_err(|_| ServiceError::InvalidResult)?;
                Ok((dataset, actions, assumptions))
            }
            MaterializedBody::Generic(input) => {
                if input.cohort.is_some() {
                    return Err(ServiceError::InvalidResult);
                }
                let actions = input.corporate_actions.ok_or(ServiceError::Unavailable)?;
                let dataset = BacktestDataset::try_from_pinned_query(
                    input.query,
                    input.instrument_definitions,
                    input.limits,
                )
                .map_err(|_| ServiceError::InvalidResult)?;
                Ok((dataset, actions, input.execution_assumptions))
            }
        }
    }
}

fn validate_pinned_input(input: PinnedBacktestInput) -> Result<(), ServiceError> {
    let PinnedBacktestInput {
        query,
        instrument_definitions,
        execution_assumptions,
        portfolio,
        corporate_actions,
        sources,
        seed,
        limits,
        experiment: _,
        cohort,
    } = input;
    let dataset = BacktestDataset::try_from_pinned_query(query, instrument_definitions, limits)
        .map_err(|_| ServiceError::InvalidRequest)?;
    BacktestRequest::try_new(
        dataset,
        execution_assumptions,
        portfolio,
        corporate_actions,
        sources,
        seed,
        limits,
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    if let Some(cohort) = cohort {
        for member in cohort.members {
            if member.input.cohort.is_some() {
                return Err(ServiceError::InvalidRequest);
            }
            validate_pinned_input(member.input)?;
        }
    }
    Ok(())
}

pub(super) struct BacktestInputMaterializer {
    research: Arc<ResearchService>,
    maximum_manifest_nodes: usize,
    pub(super) source_actions: Option<
        crate::application::research::corporate_actions::SourceAppliedCorporateActionReadCapability,
    >,
}

impl std::fmt::Debug for BacktestInputMaterializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BacktestInputMaterializer")
            .field("maximum_manifest_nodes", &self.maximum_manifest_nodes)
            .finish_non_exhaustive()
    }
}

impl BacktestInputMaterializer {
    pub(super) fn try_new(
        research: Arc<ResearchService>,
        maximum_manifest_nodes: usize,
    ) -> Result<Self, ServiceError> {
        if maximum_manifest_nodes == 0 || maximum_manifest_nodes > HARD_MAXIMUM_MANIFEST_NODES {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            research,
            maximum_manifest_nodes,
            source_actions: None,
        })
    }

    /// Uses the same immutable transitive graph resolver as registration; preparation cannot
    /// supply an unrelated source list or silently omit an action/history parent.
    pub(super) async fn source_ids_for_roots(
        &self,
        roots: Vec<DatasetManifestRef>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Vec<market_squawk_domain::SourceId>, ServiceError> {
        let reader = self.research.analytical_reader();
        let maximum = self.maximum_manifest_nodes;
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let graph = manifest_graph(&reader, roots, maximum, deadline, &worker_cancellation)?;
            let mut sources = graph
                .into_iter()
                .map(|entry| entry.source_id)
                .collect::<Vec<_>>();
            sources.sort_unstable();
            sources.dedup();
            Ok(sources)
        });
        await_blocking(worker, &cancellation, deadline).await
    }

    pub(super) async fn materialize(
        &self,
        core: &InputCoreWire,
        repository_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializedInput, ServiceError> {
        let mut materialized = self
            .materialize_single(
                core,
                repository_permit.clone(),
                cancellation.clone(),
                deadline,
            )
            .await?;
        let Some(cohort) = core.cohort().map_err(|_| ServiceError::InvalidResult)? else {
            return Ok(materialized);
        };
        let member_cores = cohort
            .member_cores(core)
            .map_err(|_| ServiceError::InvalidResult)?;
        let mut members = Vec::new();
        let mut evidence = Vec::new();
        members
            .try_reserve_exact(member_cores.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        evidence
            .try_reserve_exact(member_cores.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (member_id, member_core) in member_cores {
            let member = self
                .materialize_single(
                    &member_core,
                    repository_permit.clone(),
                    cancellation.clone(),
                    deadline,
                )
                .await?;
            let MaterializedInput {
                body,
                evidence: member_evidence,
            } = member;
            let MaterializedBody::Generic(input) = body else {
                return Err(ServiceError::InvalidResult);
            };
            evidence.push(CohortMemberEvidence {
                member_id: member_id.clone(),
                evidence: Box::new(member_evidence),
            });
            members.push(PinnedBacktestCohortMember { member_id, input });
        }
        materialized.evidence.cohort_members = evidence;
        materialized.generic_mut()?.cohort = Some(PinnedBacktestCohort {
            generator_version: cohort.generator_version().clone(),
            generator_parameters: cohort.generator_parameters(),
            members,
            folds: cohort
                .folds()
                .into_iter()
                .map(|fold| {
                    fold.into_iter()
                        .map(|(in_sample_member_id, out_of_sample_member_id)| {
                            PinnedBacktestCohortCandidate {
                                in_sample_member_id,
                                out_of_sample_member_id,
                            }
                        })
                        .collect()
                })
                .collect(),
            selection_member_ids: cohort.selection_member_ids(),
        });
        Ok(materialized)
    }

    async fn materialize_single(
        &self,
        core: &InputCoreWire,
        repository_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializedInput, ServiceError> {
        ensure_live(&cancellation, deadline)?;
        let manifest = core.manifest().map_err(|_| ServiceError::InvalidResult)?;
        let mut roots = core
            .corporate_action_manifests()
            .map_err(|_| ServiceError::InvalidResult)?;
        if let Some(history) = core.daily_history() {
            roots.extend(
                history
                    .manifests()
                    .map_err(|_| ServiceError::InvalidResult)?,
            );
        }
        let original_actions = if let Some(history) = core.daily_history() {
            let reader = self
                .source_actions
                .as_ref()
                .ok_or(ServiceError::Unavailable)?;
            let source = reader
                .read_reference(
                    history.source_action_reference(),
                    deadline,
                    cancellation.clone(),
                )
                .await
                .map_err(|_| lifecycle_error(&cancellation, deadline))?
                .ok_or(ServiceError::Unavailable)?;
            let coverage = source
                .covered_accounting_plan()
                .map_err(|_| ServiceError::Unavailable)?
                .source_admission()
                .ok_or(ServiceError::Unavailable)?;
            roots.extend(coverage.source_manifests().iter().cloned());
            roots.extend(coverage.history_input_manifests().iter().cloned());
            Some(source)
        } else {
            None
        };
        roots.push(manifest.clone());
        let graph_reader = self.research.analytical_reader();
        let graph_cancellation = cancellation.clone();
        let maximum_manifest_nodes = self.maximum_manifest_nodes;
        let graph_permit = repository_permit.clone();
        let graph_worker = tokio::task::spawn_blocking(move || {
            let _repository_permit = graph_permit;
            manifest_graph(
                &graph_reader,
                roots,
                maximum_manifest_nodes,
                deadline,
                &graph_cancellation,
            )
        });
        let manifests = await_blocking(graph_worker, &cancellation, deadline).await?;

        let study_epochs = if core.daily_history().is_some() {
            let reader = self.research.analytical_reader();
            Some(reader
                .feature_dataset_input_epochs(
                    FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                    &manifest,
                    core.query_limits()
                        .map_err(|_| ServiceError::InvalidResult)?,
                    deadline,
                    cancellation.clone(),
                )
                .await
                .map_err(|_| lifecycle_error(&cancellation, deadline))?)
        } else {
            None
        };
        let definitions = self.research.instrument_definitions();
        let instruments = core.instruments().to_vec();
        let definition_limit = core
            .definition_limit()
            .map_err(|_| ServiceError::InvalidResult)?;
        let definition_as_of = match &study_epochs {
            Some(epochs) => epochs
                .dataset()
                .study_policy()
                .ok_or(ServiceError::InvalidResult)?
                .snapshot_as_of(),
            None => core.definition_as_of(),
        };
        let definition_cancellation = cancellation.clone();
        let definition_permit = repository_permit.clone();
        let definition_worker = tokio::task::spawn_blocking(move || {
            let _repository_permit = definition_permit;
            definitions
                .pin(
                    &instruments,
                    definition_as_of,
                    definition_limit,
                    deadline,
                    &definition_cancellation,
                )
                .map_err(|_| lifecycle_error(&definition_cancellation, deadline))
        });
        let instrument_definitions =
            await_blocking(definition_worker, &cancellation, deadline).await?;
        if let Some(history) = core.daily_history() {
            let reader = self.research.analytical_reader();
            let epochs = study_epochs.ok_or(ServiceError::InvalidResult)?;
            let evidence = ExpectedEvidence::from_query(
                epochs.query_output(),
                &instrument_definitions,
                manifests,
            );
            let mut histories = Vec::new();
            histories
                .try_reserve_exact(history.selections().len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            for selection in history.selections() {
                ensure_live(&cancellation, deadline)?;
                let request = selection
                    .exact_request()
                    .map_err(|_| ServiceError::InvalidResult)?;
                let output = reader
                    .read_complete_market_bar_history(request, deadline, cancellation.clone())
                    .await
                    .map_err(|_| lifecycle_error(&cancellation, deadline))?
                    .ok_or(ServiceError::Unavailable)?;
                let source = original_actions.as_ref().ok_or(ServiceError::Unavailable)?;
                let ordinary = source
                    .ordinary_coverage()
                    .ok_or(ServiceError::Unavailable)?;
                let (_, calendar) = ordinary
                    .reads()
                    .iter()
                    .find(|(read, _)| {
                        read.history().selection().receipt().instrument_id()
                            == output.selection().receipt().instrument_id()
                    })
                    .ok_or(ServiceError::Unavailable)?;
                let output = match calendar {
                    SourcePlanCalendar::Live(calendar) => self
                        .research
                        .rejoin_market_history_native_sessions_with_calendar(
                            output, calendar, deadline, &cancellation,
                        )
                        .await,
                    SourcePlanCalendar::Retained(calendar) => self
                        .research
                        .rejoin_market_history_native_sessions_with_retained_calendar_with_job_context(
                            output, calendar, deadline, &cancellation, None,
                        )
                        .await,
                }
                .map_err(|_| lifecycle_error(&cancellation, deadline))?;
                if !selection
                    .matches(&output)
                    .map_err(|_| ServiceError::InvalidResult)?
                {
                    return Err(ServiceError::InvalidResult);
                }
                histories.push(output);
            }
            let references = histories.iter().collect::<Vec<_>>();
            let replayed = original_actions.ok_or(ServiceError::Unavailable)?;
            replayed
                .validate_accounting_histories(&references, deadline, &cancellation)
                .map_err(|_| lifecycle_error(&cancellation, deadline))?;
            let actions = replayed
                .into_covered_accounting_plan()
                .map_err(|_| ServiceError::Unavailable)?;
            let saved = core
                .corporate_actions()
                .map_err(|_| ServiceError::InvalidResult)?
                .ok_or(ServiceError::Unavailable)?;
            // The serialized plan is comparison evidence only. Only the fresh physical replay
            // supplies source admission, including genuine no-event and cash-unit coverage.
            if actions.content_hash() != saved.content_hash()
                || actions.audit_hash() != saved.audit_hash()
                || actions.policy() != saved.policy()
                || actions.knowledge_cutoff() != saved.knowledge_cutoff()
                || actions.valuation_cutoff() != saved.valuation_cutoff()
            {
                return Err(ServiceError::InvalidResult);
            }
            ensure_live(&cancellation, deadline)?;
            return Ok(MaterializedInput {
                evidence,
                body: MaterializedBody::Study {
                    epochs,
                    definitions: instrument_definitions,
                    histories,
                    admitted_at: history.admitted_at(),
                    actions,
                    assumptions: core
                        .execution_assumptions()
                        .map_err(|_| ServiceError::InvalidResult)?,
                    limits: core.limits().map_err(|_| ServiceError::InvalidResult)?,
                },
            });
        }
        let pinned_research = Arc::clone(&self.research);
        let pinned_manifest = manifest.clone();
        let pinned_permit = repository_permit.clone();
        let pinned_worker = tokio::task::spawn_blocking(move || {
            let _repository_permit = pinned_permit;
            pinned_research
                .analytical()
                .pinned(&pinned_manifest)
                .map_err(|_| ServiceError::InvalidResult)
        });
        let pinned = await_blocking(pinned_worker, &cancellation, deadline).await?;
        let query = self
            .query(core, pinned, cancellation.clone(), deadline)
            .await?;

        let input = PinnedBacktestInput {
            query,
            instrument_definitions,
            execution_assumptions: core
                .execution_assumptions()
                .map_err(|_| ServiceError::InvalidResult)?,
            portfolio: core.portfolio().map_err(|_| ServiceError::InvalidResult)?,
            corporate_actions: core
                .corporate_actions()
                .map_err(|_| ServiceError::InvalidResult)?,
            sources: core.sources().map_err(|_| ServiceError::InvalidResult)?,
            seed: core.seed(),
            limits: core.limits().map_err(|_| ServiceError::InvalidResult)?,
            experiment: core.experiment().map_err(|_| ServiceError::InvalidResult)?,
            cohort: None,
        };
        let evidence = ExpectedEvidence::from_input(&input, manifests);
        ensure_live(&cancellation, deadline)?;
        Ok(MaterializedInput {
            body: MaterializedBody::Generic(input),
            evidence,
        })
    }

    async fn query(
        &self,
        core: &InputCoreWire,
        pinned: PinnedDataset,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<market_squawk_data::PinnedQueryOutput, ServiceError> {
        let engine = ResearchQueryEngine::from_pinned_dataset(
            pinned,
            core.table_name(),
            self.research.analytical().object_store(),
            cancellation.clone(),
        )
        .await
        .map_err(|_| lifecycle_error(&cancellation, deadline))?;
        let request = core
            .query_request()
            .map_err(|_| ServiceError::InvalidResult)?;
        let limits = core
            .query_limits()
            .map_err(|_| ServiceError::InvalidResult)?;
        let execution = engine.query_pinned(request, limits, cancellation.clone());
        tokio::pin!(execution);
        let deadline_wait = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(deadline_wait);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                let _ignored = execution.as_mut().await;
                Err(ServiceError::Cancelled)
            }
            _ = deadline_wait.as_mut() => {
                cancellation.cancel();
                let _ignored = execution.as_mut().await;
                Err(ServiceError::DeadlineExceeded)
            }
            result = execution.as_mut() => {
                result.map_err(|_| lifecycle_error(&cancellation, deadline))
            }
        }
    }
}

fn manifest_graph(
    reader: &AnalyticalReadCapability,
    roots: Vec<DatasetManifestRef>,
    maximum_nodes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ManifestAuthorityWire>, ServiceError> {
    ensure_live(cancellation, deadline)?;
    let mut pending = roots;
    let mut resolved =
        BTreeMap::<(String, u64), (DatasetManifestRef, ManifestAuthorityWire)>::new();
    while let Some(manifest) = pending.pop() {
        ensure_live(cancellation, deadline)?;
        let coordinate = (
            manifest.dataset_id().as_str().to_owned(),
            manifest.manifest_version(),
        );
        if let Some((existing, _)) = resolved.get(&coordinate) {
            if existing != &manifest {
                return Err(ServiceError::InvalidResult);
            }
            continue;
        }
        if resolved.len() >= maximum_nodes {
            return Err(ServiceError::ResourceExhausted);
        }
        let generation = reader
            .exact(&manifest, deadline, cancellation)
            .map_err(|_| lifecycle_error(cancellation, deadline))?;
        pending.extend(
            generation
                .parents()
                .iter()
                .map(|parent| parent.manifest().clone()),
        );
        resolved.insert(
            coordinate,
            (
                manifest.clone(),
                ManifestAuthorityWire::new(&manifest, generation.source_id().clone()),
            ),
        );
    }
    let mut authorities = resolved
        .into_values()
        .map(|(_, authority)| authority)
        .collect::<Vec<_>>();
    sort_manifest_authorities(&mut authorities);
    Ok(authorities)
}

async fn await_blocking<T>(
    mut worker: JoinHandle<Result<T, ServiceError>>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, ServiceError> {
    tokio::select! {
        biased;
        result = &mut worker => result.map_err(|_| ServiceError::Internal)?,
        () = cancellation.cancelled() => {
            let _ignored = worker.await;
            Err(ServiceError::Cancelled)
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            cancellation.cancel();
            let _ignored = worker.await;
            Err(ServiceError::DeadlineExceeded)
        }
    }
}

fn ensure_live(cancellation: &CancellationToken, deadline: Instant) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn lifecycle_error(cancellation: &CancellationToken, deadline: Instant) -> ServiceError {
    if cancellation.is_cancelled() {
        ServiceError::Cancelled
    } else if Instant::now() >= deadline {
        ServiceError::DeadlineExceeded
    } else {
        ServiceError::InvalidResult
    }
}
