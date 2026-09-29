//! Actual later event publication through the same original source and durable dataset owners.
use super::*;
use crate::application::{
    ForecastOutcomeSourcePreparation, SourceActionPreparationCapability,
    job::{JobApplication, JobApplicationError},
    model::{
        ModelDomainService,
        forecast::{
            ForecastEvidenceReadContext, ForecastEvidenceReader, MAXIMUM_FORECAST_ARTIFACT_BYTES,
        },
    },
};
use crate::jobs::PhaseOneDerivedGenerationJobRunner;
use market_squawk_data::ForecastDatasetReadLimits;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_jobs::{
    JobOrigin, JobStartAdmission, JobStartBinding, JobStartState, JobState, SqliteJobRepository,
};
use market_squawk_services::{ArtifactReadContext, RequestId};
use std::num::NonZeroUsize;
use uuid::Uuid;

pub(super) struct OutcomePublication {
    sources: SourceActionPreparationCapability,
    model: Arc<ModelDomainService>,
    runner: Arc<PhaseOneDerivedGenerationJobRunner>,
    jobs: JobApplication<SqliteJobRepository>,
}
impl InstalledProbabilityPreparation {
    /// Injects existing owners once during installed composition; no callback into this service
    /// from the model domain, and no second scheduler or publication capability.
    pub(in crate::service) fn with_outcome_publication(
        mut self,
        sources: SourceActionPreparationCapability,
        model: Arc<ModelDomainService>,
        runner: Arc<PhaseOneDerivedGenerationJobRunner>,
        jobs: &InstalledJobAuthority,
    ) -> Self {
        self.outcome = Some(OutcomePublication {
            sources,
            model,
            runner,
            jobs: JobApplication::new(jobs.repository(), jobs.authority()),
        });
        self
    }
    /// Called only after the original model operation reports a mature missing event outcome.
    /// Success means a real Analysis publication has completed and reopened, never a label claim.
    pub(in crate::service) async fn ensure_matured_event_publication(
        &self,
        token: Uuid,
        account: Option<AccountId>,
        context: &RequestContext,
    ) -> Result<bool, ServiceError> {
        ensure_live(context)?;
        let owner = self.outcome.as_ref().ok_or(ServiceError::Unavailable)?;
        let reader = self.research.analytical_reader();
        let evidence_context = ForecastEvidenceReadContext::new(
            ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            NonZeroUsize::new(MAXIMUM_FORECAST_ARTIFACT_BYTES).ok_or(ServiceError::Internal)?,
        );
        let Some(origin) = owner
            .model
            .outcome_preparation_origin(token, &reader, evidence_context)
            .await
            .map_err(crate::application::model::map_forecast_selection_error)?
        else {
            return Ok(false);
        };
        let Some(event) = origin.event_target() else {
            return Ok(false);
        };
        if matches!(event, ProbabilityEventTarget::ProfitAfterCosts { .. }) && account.is_none() {
            return Ok(false);
        }
        let saved_bar = origin.origin_bar().clone();
        let subject = origin.instrument_id();
        let saved_origin = origin.origin_at();
        let target = origin.target_at();
        let horizon = target
            .unix_nanos()
            .checked_sub(saved_origin.unix_nanos())
            .filter(|v| *v > 0)
            .ok_or(ServiceError::InvalidResult)?;
        let maturity = match event {
            ProbabilityEventTarget::ProfitAfterCosts { policy } => target
                .checked_add_nanos(policy.maximum_exit_lag_nanos)
                .map_err(|_| ServiceError::InvalidResult)?,
            _ => target,
        };
        let now =
            super::super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        if now < maturity {
            return Ok(false);
        }
        let original = reader
            .forecast_dataset_evidence(
                analysis_contract(event),
                origin.analysis_evidence().manifest(),
                now,
                ForecastDatasetReadLimits::try_new(100_000, 256 * 1024 * 1024)
                    .map_err(|_| ServiceError::Internal)?,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(crate::application::map_source_analytical_error)?;
        if original.probability_event_target() != Some(event)
            || original
                .dataset()
                .production_receipt()
                .production_identity()
                != origin.analysis_evidence().production_identity_sha256()
            || original.dataset().production_receipt().receipt_sha256()
                != origin.analysis_evidence().production_receipt_sha256()
        {
            return Err(ServiceError::InvalidResult);
        }
        let original_study = *original
            .dataset()
            .study_policy()
            .ok_or(ServiceError::InvalidResult)?;
        if original_study
            .target_horizon()
            .exact_elapsed()
            .is_none_or(|v| v.as_nanos() != horizon as u128)
        {
            return Err(ServiceError::InvalidResult);
        }
        let original_boundaries = original
            .dataset()
            .split_policy()
            .timestamp_boundaries()
            .ok_or(ServiceError::InvalidResult)?;
        let benchmark = if let ProbabilityEventTarget::BenchmarkOutperformance {
            benchmark_instrument_id,
            benchmark_definition,
        } = event
        {
            let Some(selected) = self.benchmarks.select_comparison(
                Some(benchmark_instrument_id),
                original_study.snapshot_as_of(),
                original_study.snapshot_as_of(),
                context.deadline(),
                context.cancellation(),
            )?
            else {
                return Ok(false);
            };
            if selected.reference_revision_digest() != benchmark_definition {
                return Ok(false);
            }
            Some(selected)
        } else {
            None
        };
        drop(original);
        let prepared = match owner
            .sources
            .prepare_outcome_measurement(origin, context)
            .await?
        {
            ForecastOutcomeSourcePreparation::Prepared(value) => value,
            ForecastOutcomeSourcePreparation::NotYetCompleted
            | ForecastOutcomeSourcePreparation::SourceUnavailable
            | ForecastOutcomeSourcePreparation::UnsupportedHistory => return Ok(false),
        };
        let cutoff = prepared.cutoff();
        let reference = prepared.reference().clone();
        if cutoff < maturity || reference.knowledge_cutoff() != cutoff {
            return Err(ServiceError::InvalidResult);
        }
        let Some(history) = self
            .actions
            .read_history_reference(
                &reference,
                subject,
                context.deadline(),
                context.cancellation().clone(),
                None,
            )
            .await
            .map_err(|error| source_error(error, context))?
        else {
            return Ok(false);
        };
        if history.selection().pinned().manifest() != prepared.manifest() {
            return Err(ServiceError::InvalidResult);
        }
        let sessions = history
            .native_sessions()
            .ok_or(ServiceError::InvalidResult)?;
        let first = sessions
            .sessions()
            .first()
            .map_err(crate::application::map_source_analytical_error)?
            .ok_or(ServiceError::Unavailable)?
            .closes_at_exclusive();
        let mut last = None;
        for session in sessions.sessions().iter() {
            let close = session
                .map_err(crate::application::map_source_analytical_error)?
                .closes_at_exclusive();
            if close <= cutoff {
                last = Some(last.map_or(close, |previous: Timestamp| previous.max(close)));
            }
        }
        let last = last.ok_or(ServiceError::Unavailable)?;
        if first >= original_boundaries[0] || last < maturity || last <= original_boundaries[1] {
            return Ok(false);
        }
        // Original train/calibration boundaries stay immutable. Only the later terminal window
        // extends to genuine source coverage, declared before any new outcome is derived.
        let split =
            ChronologicalSplitPolicy::try_new(original_boundaries[0], original_boundaries[1], last)
                .map_err(|_| ServiceError::InvalidResult)?;
        let study = |purpose| {
            DatasetStudyPolicy::try_new(
                original_study.basis(),
                purpose,
                cutoff,
                original_study.decision_lag(),
                original_study.target_horizon(),
            )
            .map_err(|_| ServiceError::InvalidResult)
        };
        let subject_request = ProbabilitySubjectInputRequest {
            event,
            subject_instrument: subject,
            subject_manifest: prepared.manifest().clone(),
            study: study(DatasetBuildPurpose::StudyInputs)?,
            population_starts_at: first,
            population_ends_at: last,
            split,
            source_action_reference: Some(reference.clone()),
        };
        let subject_prepared = match self
            .datasets
            .prepare_probability_subject_inputs(
                subject_request,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(crate::application::DatasetPreparationError::Unavailable) => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        let dataset = self
            .publish_outcome_dataset(
                owner,
                token,
                subject_prepared,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                context,
            )
            .await?;
        let costs = if let ProbabilityEventTarget::ProfitAfterCosts { policy } = event {
            let Some(account) = account else {
                return Ok(false);
            };
            let Some(actions) = self
                .actions
                .read_reference_for_histories(
                    &reference,
                    &[&history],
                    context.deadline(),
                    context.cancellation().clone(),
                    None,
                )
                .await
                .map_err(|error| source_error(error, context))?
            else {
                return Ok(false);
            };
            let actions = actions
                .into_covered_accounting_plan()
                .map_err(|error| source_error(error, context))?;
            Some(Arc::new(
                self.inputs
                    .prepare_probability_evaluation(
                        dataset,
                        &history,
                        actions,
                        reference.clone(),
                        policy,
                        account,
                        context,
                    )
                    .await?,
            ))
        } else {
            None
        };
        let benchmark = if let Some(selection) = benchmark {
            let Some(history) = self
                .actions
                .read_history_reference(
                    &reference,
                    selection.instrument_id(),
                    context.deadline(),
                    context.cancellation().clone(),
                    None,
                )
                .await
                .map_err(|error| source_error(error, context))?
            else {
                return Ok(false);
            };
            Some(ProbabilityBenchmarkSource {
                selection,
                manifest: history.selection().pinned().manifest().clone(),
            })
        } else {
            None
        };
        let pair = match self
            .datasets
            .prepare_probability_cohort(
                ProbabilityCohortPreparationRequest {
                    subject_instrument: subject,
                    subject_manifest: prepared.manifest().clone(),
                    benchmark,
                    event,
                    costs,
                    study: study(DatasetBuildPurpose::Training)?,
                    population_starts_at: first,
                    population_ends_at: last,
                    split,
                    source_action_reference: Some(reference),
                },
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(crate::application::DatasetPreparationError::Unavailable) => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        let published = self
            .publish_outcome_dataset(
                owner,
                token,
                pair.analysis,
                analysis_contract(event),
                context,
            )
            .await?;
        let read_at =
            super::super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let evidence = reader
            .forecast_dataset_evidence(
                analysis_contract(event),
                published.generation().manifest(),
                read_at,
                ForecastDatasetReadLimits::try_new(100_000, 256 * 1024 * 1024)
                    .map_err(|_| ServiceError::Internal)?,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(crate::application::map_source_analytical_error)?;
        Ok(evidence
            .select_probability_outcome(event, subject, saved_origin, target)
            .map_err(crate::application::map_source_analytical_error)?
            .is_some_and(|outcome| outcome.matches_origin_observation(&saved_bar)))
    }
    async fn publish_outcome_dataset(
        &self,
        owner: &OutcomePublication,
        token: Uuid,
        prepared: PreparedFeatureDatasetBuild,
        contract: FeatureDatasetProductContract,
        context: &RequestContext,
    ) -> Result<AnalyticalFeatureDataset, ServiceError> {
        ensure_live(context)?;
        let build = prepared.build_spec_digest();
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/saved-event-dataset-job/v1\0");
        hash.update(token.as_bytes());
        hash.update(build.digest().bytes());
        let digest: [u8; 32] = hash.finalize().into();
        let id = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let binding = JobStartBinding::new(
            JobOrigin::new(
                SourceIdentifier::try_from(origin.workspace_id().to_string())
                    .map_err(|_| ServiceError::Unauthorized)?,
                SourceIdentifier::try_from(origin.client_id().to_string())
                    .map_err(|_| ServiceError::Unauthorized)?,
            ),
            RequestId::try_string(format!("probability-outcome-{id}"))
                .map_err(|_| ServiceError::Internal)?,
            SourceIdentifier::try_from(START_PROBABILITY_DATASET)
                .map_err(|_| ServiceError::Internal)?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        );
        let (job, generation) = match owner.jobs.begin_start(&binding).await.map_err(map_job)? {
            JobStartAdmission::Existing(existing) => {
                let snapshot = existing.snapshot().ok_or(ServiceError::Unavailable)?;
                if snapshot.spec().input().digest().bytes() != build.digest().bytes() {
                    return Err(ServiceError::InvalidResult);
                }
                (
                    snapshot.id().as_uuid().to_string(),
                    snapshot.generation().get(),
                )
            }
            JobStartAdmission::Execute(permit) => {
                let admitted_at = super::super::runtime::current_timestamp()
                    .map_err(|_| ServiceError::Unavailable)?;
                let admission = match owner.runner.admit_prepared(prepared, admitted_at) {
                    Ok(value) => value,
                    Err(error) => {
                        owner.jobs.cancel_start(&binding).await.map_err(map_job)?;
                        return Err(super::super::tool_services::map_research_admission(error));
                    }
                };
                match owner
                    .jobs
                    .start_reserved(admission.clone(), &permit, admitted_at)
                    .await
                {
                    Ok(receipt) => (
                        receipt.job_id().as_uuid().to_string(),
                        receipt.generation().get(),
                    ),
                    Err(error) => {
                        let disposition =
                            owner.jobs.cancel_start(&binding).await.map_err(map_job)?;
                        if disposition.state() == JobStartState::NotAdmitted
                            && disposition.job().is_none()
                        {
                            owner
                                .runner
                                .revoke(&admission)
                                .map_err(super::super::tool_services::map_research_admission)?;
                        }
                        return Err(map_job(error));
                    }
                }
            }
        };
        loop {
            let snapshot = self.training.snapshot(&job, generation, context).await?;
            match snapshot.state() {
                JobState::Completed => {
                    let selection = self
                        .training
                        .reopen_prepared_dataset(&snapshot, Some(contract), context)
                        .await?;
                    if selection.identity().build_spec_digest() != build
                        || snapshot.spec().input().digest().bytes() != build.digest().bytes()
                    {
                        return Err(ServiceError::InvalidResult);
                    }
                    let result = self
                        .research
                        .analytical_reader()
                        .feature_dataset_for_build(
                            contract,
                            selection.identity().manifest().dataset_id(),
                            build,
                            context.deadline(),
                            context.cancellation(),
                        )
                        .map_err(crate::application::map_source_analytical_error)?
                        .ok_or(ServiceError::Unavailable)?;
                    if result.generation().manifest() != selection.identity().manifest() {
                        return Err(ServiceError::InvalidResult);
                    }
                    return Ok(result);
                }
                JobState::Failed | JobState::Cancelled | JobState::Interrupted => {
                    return Err(ServiceError::Unavailable);
                }
                JobState::AwaitingConfirmation => return Err(ServiceError::InvalidResult),
                _ => {}
            }
            tokio::select! {biased; _=context.cancellation().cancelled()=>return Err(ServiceError::Cancelled),
            _=tokio::time::sleep_until(context.deadline().into())=>return Err(ServiceError::DeadlineExceeded),
            _=tokio::time::sleep(Duration::from_millis(100))=>{}}
        }
    }
}
fn map_job(error: JobApplicationError) -> ServiceError {
    match error {
        JobApplicationError::NotFound => ServiceError::NotFound,
        JobApplicationError::Contract => ServiceError::InvalidResult,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}
fn analysis_contract(event: ProbabilityEventTarget) -> FeatureDatasetProductContract {
    use FeatureDatasetProductContract::*;
    match event {
        ProbabilityEventTarget::PriceHigher => {
            PriceReturnMacroContextFixedHorizonPriceHigherAnalysisV1
        }
        ProbabilityEventTarget::BenchmarkOutperformance { .. } => {
            PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceAnalysisV1
        }
        ProbabilityEventTarget::ProfitAfterCosts { .. } => {
            PriceReturnMacroContextFixedHorizonProfitAfterCostsAnalysisV1
        }
    }
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
