//! Fresh point-in-time materialization and immutable evidence comparison.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use market_squawk_backtesting::{BacktestDataset, BacktestRequest};
use market_squawk_data::{
    AnalyticalReadCapability, DatasetManifestRef, PinnedDataset, ResearchQueryEngine,
};
use market_squawk_services::ServiceError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    PinnedBacktestInput, ResearchService,
    backtest_service::{
        PinnedBacktestCohort, PinnedBacktestCohortCandidate, PinnedBacktestCohortMember,
    },
};

use super::recipe::{
    CohortMemberEvidence, ExpectedEvidence, InputCoreWire, ManifestAuthorityWire,
    sort_manifest_authorities,
};

const HARD_MAXIMUM_MANIFEST_NODES: usize = 4_096;

pub(super) struct MaterializedInput {
    pub(super) input: PinnedBacktestInput,
    pub(super) evidence: ExpectedEvidence,
}

impl MaterializedInput {
    pub(super) fn validate_registration(self) -> Result<(), ServiceError> {
        validate_pinned_input(self.input)
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

#[derive(Debug)]
pub(super) struct BacktestInputMaterializer {
    research: Arc<ResearchService>,
    maximum_manifest_nodes: usize,
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
        })
    }

    pub(super) async fn materialize(
        &self,
        core: &InputCoreWire,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializedInput, ServiceError> {
        let mut materialized = self
            .materialize_single(core, cancellation.clone(), deadline)
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
                .materialize_single(&member_core, cancellation.clone(), deadline)
                .await?;
            evidence.push(CohortMemberEvidence {
                member_id: member_id.clone(),
                evidence: Box::new(member.evidence),
            });
            members.push(PinnedBacktestCohortMember {
                member_id,
                input: member.input,
            });
        }
        materialized.evidence.cohort_members = evidence;
        materialized.input.cohort = Some(PinnedBacktestCohort {
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
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializedInput, ServiceError> {
        ensure_live(&cancellation, deadline)?;
        let manifest = core.manifest().map_err(|_| ServiceError::InvalidResult)?;
        let mut roots = core
            .corporate_action_manifests()
            .map_err(|_| ServiceError::InvalidResult)?;
        roots.push(manifest.clone());
        let graph_reader = self.research.analytical_reader();
        let graph_cancellation = cancellation.clone();
        let maximum_manifest_nodes = self.maximum_manifest_nodes;
        let graph_manifest = manifest.clone();
        let graph_worker = tokio::task::spawn_blocking(move || {
            manifest_graph(
                &graph_reader,
                &graph_manifest,
                roots,
                maximum_manifest_nodes,
                deadline,
                &graph_cancellation,
            )
        });
        let manifests = await_blocking(graph_worker, &cancellation, deadline).await?;

        let pinned_research = Arc::clone(&self.research);
        let pinned_manifest = manifest.clone();
        let pinned_worker = tokio::task::spawn_blocking(move || {
            pinned_research
                .analytical()
                .pinned(&pinned_manifest)
                .map_err(|_| ServiceError::InvalidResult)
        });
        let pinned = await_blocking(pinned_worker, &cancellation, deadline).await?;
        let query = self
            .query(core, pinned, cancellation.clone(), deadline)
            .await?;

        let definitions = self.research.instrument_definitions();
        let instruments = core.instruments().to_vec();
        let definition_limit = core
            .definition_limit()
            .map_err(|_| ServiceError::InvalidResult)?;
        let definition_as_of = core.definition_as_of();
        let definition_cancellation = cancellation.clone();
        let definition_worker = tokio::task::spawn_blocking(move || {
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
        Ok(MaterializedInput { input, evidence })
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
    primary: &DatasetManifestRef,
    roots: Vec<DatasetManifestRef>,
    maximum_nodes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ManifestAuthorityWire>, ServiceError> {
    ensure_live(cancellation, deadline)?;
    let latest = reader
        .latest(primary.dataset_id(), deadline, cancellation)
        .map_err(|_| lifecycle_error(cancellation, deadline))?
        .ok_or(ServiceError::NotFound)?;
    if latest.manifest() != primary {
        return Err(ServiceError::InvalidResult);
    }
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
