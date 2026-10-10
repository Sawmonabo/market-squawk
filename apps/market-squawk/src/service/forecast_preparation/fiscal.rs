//! Current fiscal planning and exact completed jobs use the existing dataset and forecast owners.
use super::*;
use crate::{
    application::{
        DatasetPreparationAuthority, FiscalDatasetPreparationRequest, FiscalProjectionTarget,
        PreparedFeatureDatasetBuild, PreparedFiscalDatasetPair,
        analytical_profile::ValidatedAnalyticalProfile, fiscal_projection_policy_value,
        fiscal_projection_targets, model::runtime::ModelAdmissionReceipt,
        prepare_fixed_current_population,
    },
    jobs::TrainingJobRunner,
    service::InstalledProductTraining,
};
use market_squawk_data::{
    CurrentListedPopulation, FeatureDatasetProductContract, FeatureLabelMeasurement,
    PythonDatasetSelection, Sha256Digest,
};
use std::num::NonZeroU64;

pub(in crate::service) const GET_FISCAL_PREPARATION_PLAN: &str =
    "Analysis.GetFiscalPreparationPlan";
pub(in crate::service) const START_FISCAL_DATASET_BUILD: &str = "Analysis.StartFiscalDatasetBuild";
pub(in crate::service) const START_FISCAL_FORECAST: &str = "Model.StartFiscalForecast";

impl InstalledForecastPreparation {
    /// Readiness is established by the actual native recipe, not the presence of a source label.
    pub(in crate::service) async fn fiscal_plan(
        &self,
        datasets: &DatasetPreparationAuthority,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.name() != GET_FISCAL_PREPARATION_PLAN {
            return Err(ServiceError::InvalidRequest);
        }
        let input: FiscalPlanInput =
            decode(&super::super::business_arguments(request.arguments()))?;
        let cutoff = input.cutoff(context)?;
        let profile = self
            .revalidate_profile(&input.financial_profile, context)
            .await?;
        let (population, reason) = if !profile
            .recommendation_policy()
            .parameters()
            .allow_retrospective_studies
        {
            (None, Some("retrospective_studies_disabled"))
        } else {
            match self
                .fiscal_population(input.instrument_id, cutoff, &profile, context)
                .await
            {
                Ok(population) => (Some(population), None),
                Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                    (None, Some("source_population_unavailable"))
                }
                Err(error) => return Err(error),
            }
        };
        let mut targets = Vec::with_capacity(fiscal_projection_targets().len());
        for target in fiscal_projection_targets() {
            ensure_live(context)?;
            let unavailable = match &population {
                Some(population) => match Self::prepare_fiscal_pair(
                    datasets,
                    input.instrument_id,
                    cutoff,
                    &target,
                    population.clone(),
                    &profile,
                    context,
                )
                .await
                {
                    Ok(_) => None,
                    Err(ServiceError::Unavailable | ServiceError::NotFound) => {
                        Some("required_fiscal_history_unavailable")
                    }
                    Err(error) => return Err(error),
                },
                None => reason,
            };
            targets.push(json!({"target":target,"availability":{
                "state":if unavailable.is_some() {"unavailable"} else {"ready"},
                "reason":unavailable,
            }}));
        }
        ensure_live(context)?;
        TypedToolResult::try_new(
            json!({"instrumentId":input.instrument_id,
                "sourceCutoffUnixNanos":cutoff.unix_nanos().to_string(),
                "financialProfileDigest":profile.resolution().configuration_digest,
                "policy":fiscal_projection_policy_value(),"targets":targets}),
            targets.len(),
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    /// Reconstructs the selected recipe on every start; a readiness response grants no authority.
    pub(in crate::service) async fn prepare_fiscal_dataset(
        &self,
        datasets: &DatasetPreparationAuthority,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedFeatureDatasetBuild, ServiceError> {
        if request.name() != START_FISCAL_DATASET_BUILD {
            return Err(ServiceError::InvalidRequest);
        }
        let input: FiscalDatasetInput =
            decode(&super::super::business_arguments(request.arguments()))?;
        let target = fiscal_target(&input.target_id)?;
        let plan = FiscalPlanInput {
            instrument_id: input.instrument_id,
            source_cutoff_unix_nanos: input.source_cutoff_unix_nanos,
            financial_profile: input.financial_profile,
        };
        let cutoff = plan.cutoff(context)?;
        let profile = self
            .revalidate_profile(&plan.financial_profile, context)
            .await?;
        require_retrospective(&profile)?;
        let population = self
            .fiscal_population(plan.instrument_id, cutoff, &profile, context)
            .await?;
        let (training, study_inputs, _) = Self::prepare_fiscal_pair(
            datasets,
            plan.instrument_id,
            cutoff,
            &target,
            population,
            &profile,
            context,
        )
        .await?
        .into_parts();
        ensure_live(context)?;
        match input.purpose.as_str() {
            "training" => Ok(training),
            "studyInputs" => Ok(study_inputs),
            _ => Err(ServiceError::InvalidRequest),
        }
    }

    /// Opens both exact job generations and checks their original current fiscal recipe.
    pub(in crate::service) async fn prepare_fiscal_forecast(
        &self,
        datasets: &DatasetPreparationAuthority,
        training: &InstalledProductTraining,
        runner: &TrainingJobRunner,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedForecastJobInput, ServiceError> {
        if request.name() != START_FISCAL_FORECAST {
            return Err(ServiceError::InvalidRequest);
        }
        let input: FiscalForecastInput =
            decode(&super::super::business_arguments(request.arguments()))?;
        let profile = self
            .revalidate_profile(&input.financial_profile, context)
            .await?;
        require_retrospective(&profile)?;
        let input_job = training
            .snapshot(
                &input.input_dataset_job_id,
                input.input_dataset_job_generation,
                context,
            )
            .await?;
        let selection = training
            .reopen_prepared_dataset(
                &input_job,
                Some(FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1),
                context,
            )
            .await?;
        let output = crate::application::model::forecast::reopen_financial_input(
            &self.research.analytical_reader(),
            selection.identity().manifest(),
            context.deadline(),
            context.cancellation().child_token(),
        )
        .await?;
        let [epoch] = output.epochs() else {
            return Err(ServiceError::InvalidResult);
        };
        let target = target_for_epoch(epoch)?;
        let population = self
            .fiscal_population(
                epoch.instrument_id(),
                epoch.source_selection_as_of(),
                &profile,
                context,
            )
            .await?;
        let (expected_training, expected_inputs, example_id) = Self::prepare_fiscal_pair(
            datasets,
            epoch.instrument_id(),
            epoch.source_selection_as_of(),
            &target,
            population,
            &profile,
            context,
        )
        .await?
        .into_parts();
        let (expected_inputs, _) = expected_inputs.into_parts();
        if epoch.example_id() != example_id
            || selection.as_of() != epoch.source_selection_as_of()
            || selection.identity().build_spec_digest() != expected_inputs.build_spec_digest()
            || input_job.spec().input().digest().bytes()
                != expected_inputs.build_spec_digest().digest().bytes()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let (expected_training, _) = expected_training.into_parts();
        let dataset = self
            .research
            .analytical_reader()
            .feature_dataset_for_build(
                FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1,
                expected_training.output_dataset(),
                expected_training.build_spec_digest(),
                context.deadline(),
                context.cancellation(),
            )
            .map_err(crate::application::map_source_analytical_error)?
            .ok_or(ServiceError::Unavailable)?;
        let training_selection = training
            .verify_dataset_export(
                dataset.python_export_sha256(),
                dataset.product_contract(),
                selection.as_of(),
                context,
            )
            .await?;
        if training_selection.identity().manifest() != dataset.generation().manifest()
            || training_selection.identity().build_spec_digest()
                != expected_training.build_spec_digest()
        {
            return Err(ServiceError::InvalidResult);
        }
        let expected = runner
            .prepare_product(
                &training_selection,
                training_selection.identity().manifest(),
                &profile,
                None,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::super::tool_services::map_training_admission)?;
        let training_job = training
            .snapshot(
                &input.training_job_id,
                input.training_job_generation,
                context,
            )
            .await?;
        if training_job.spec().input().digest() != expected.commitment() {
            return Err(ServiceError::InvalidRequest);
        }
        let receipt = runner
            .resolve_completed(&training_job)
            .map_err(super::super::tool_services::map_training_admission)?;
        if receipt.dataset_selection_sha256() != training_selection.selection_sha256() {
            return Err(ServiceError::InvalidResult);
        }
        self.prepare_financial_completed(&receipt, &selection, &output, &profile, context)
            .await
    }

    async fn fiscal_population(
        &self,
        instrument: InstrumentId,
        cutoff: Timestamp,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<CurrentListedPopulation, ServiceError> {
        ensure_live(context)?;
        let identities = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
        let population = prepare_fixed_current_population(
            &self.research,
            Arc::new(identities.clone()),
            vec![instrument],
            Sha256Digest::new(
                super::super::jobs::parse_sha256(&profile.resolution().configuration_digest)?
                    .bytes(),
            ),
            cutoff,
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        let [member] = population.members() else {
            return Err(ServiceError::Unavailable);
        };
        if !profile.admits_investment(
            member.canonical_record().definition().asset_class(),
            member.listing_record().is_etf(),
        ) {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(population)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "native source, population and profile remain explicit"
    )]
    async fn prepare_fiscal_pair(
        datasets: &DatasetPreparationAuthority,
        instrument_id: InstrumentId,
        cutoff: Timestamp,
        target: &FiscalProjectionTarget,
        population: CurrentListedPopulation,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedFiscalDatasetPair, ServiceError> {
        datasets
            .prepare_financial_datasets(
                FiscalDatasetPreparationRequest {
                    instrument_id,
                    knowledge_cutoff: cutoff,
                    effective_date_cutoff: cutoff
                        .utc_calendar_date()
                        .map_err(|_| ServiceError::InvalidRequest)?,
                    measurement: target.measurement(),
                    cadence: target.cadence,
                    periods_ahead: target.periods_ahead,
                    population,
                },
                profile,
                context,
            )
            .await
    }

    pub(in crate::service) async fn prepare_financial_completed(
        &self,
        receipt: &ModelAdmissionReceipt,
        selection: &PythonDatasetSelection,
        output: &market_squawk_data::FeatureDatasetInputEpochOutput,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedForecastJobInput, ServiceError> {
        ensure_live(context)?;
        if selection.product_contract()
            != FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
        {
            return Err(ServiceError::InvalidRequest);
        }
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let [epoch] = output.epochs() else {
            return Err(ServiceError::InvalidResult);
        };
        target_for_epoch(epoch)?;
        if epoch.source_selection_as_of() != selection.as_of() {
            return Err(ServiceError::InvalidRequest);
        }
        let identity = resolve_product_identity(
            self.instruments.as_ref().ok_or(ServiceError::Unavailable)?,
            epoch.instrument_id(),
            epoch.source_selection_as_of(),
            epoch.source_selection_as_of(),
            context,
        )?;
        let validity = u64::try_from(
            profile
                .recommendation_policy()
                .parameters()
                .financial_model_max_age_nanos,
        )
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(ServiceError::InvalidRequest)?;
        let digest =
            super::super::jobs::parse_sha256(&profile.resolution().configuration_digest)?.bytes();
        authority
            .prepare_financial_job(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                receipt,
                selection.identity().manifest(),
                epoch.example_id(),
                identity,
                digest,
                validity,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(map_preparation)
    }
}

fn fiscal_target(id: &str) -> Result<FiscalProjectionTarget, ServiceError> {
    fiscal_projection_targets()
        .into_iter()
        .find(|target| target.target_id == id)
        .ok_or(ServiceError::InvalidRequest)
}

fn target_for_epoch(
    epoch: &market_squawk_data::FeatureDatasetInputEpoch,
) -> Result<FiscalProjectionTarget, ServiceError> {
    let period = epoch
        .financial_period()
        .ok_or(ServiceError::InvalidResult)?;
    let FeatureLabelMeasurement::FinancialAmount {
        role,
        basis,
        share_convention,
        ..
    } = epoch
        .financial_measurement()
        .ok_or(ServiceError::InvalidResult)?
    else {
        return Err(ServiceError::InvalidResult);
    };
    let offset = period
        .target_ordinal()
        .checked_sub(period.observed_ordinal())
        .ok_or(ServiceError::InvalidResult)?;
    fiscal_projection_targets()
        .into_iter()
        .find(|target| {
            target.role == role
                && target.basis == basis
                && target.share_convention == share_convention
                && target.cadence == period.cadence()
                && u32::from(target.periods_ahead.get()) == offset
        })
        .ok_or(ServiceError::InvalidRequest)
}

fn require_retrospective(profile: &ValidatedAnalyticalProfile) -> Result<(), ServiceError> {
    if profile
        .recommendation_policy()
        .parameters()
        .allow_retrospective_studies
    {
        Ok(())
    } else {
        Err(ServiceError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FiscalPlanInput {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
}

impl FiscalPlanInput {
    fn cutoff(&self, context: &RequestContext) -> Result<Timestamp, ServiceError> {
        ensure_live(context)?;
        context.origin().ok_or(ServiceError::Unauthorized)?;
        let nanos = self
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let cutoff = Timestamp::from_unix_nanos(nanos);
        if nanos.to_string() != self.source_cutoff_unix_nanos
            || cutoff
                > super::super::runtime::current_timestamp()
                    .map_err(|_| ServiceError::Unavailable)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(cutoff)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FiscalDatasetInput {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    target_id: String,
    purpose: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FiscalForecastInput {
    training_job_id: String,
    training_job_generation: u64,
    input_dataset_job_id: String,
    input_dataset_job_generation: u64,
    financial_profile: AnalyticalProfileResolution,
}
