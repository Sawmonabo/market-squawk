//! Profile selection over the existing model/data owner's admitted forecast catalogue.

use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_modeling::{ForecastHorizon, ModelOutputSemantics};
use serde_json::{Value, json};

use super::{AnalyticalModelBundlePolicy, AnalyticalProfileError, ValidatedAnalyticalProfile, hex};
use crate::application::model::{
    ForecastModelEvidenceState,
    forecast_preparation::{
        ForecastEvidenceDataset, ForecastEvidencePolicy, ForecastModelSummary,
        ForecastPreparationCatalog, ForecastPreparationSelection,
    },
};

impl ValidatedAnalyticalProfile {
    /// Selects an actual exact producer request with no copied history, tensor, or model authority.
    /// The existing preparation authority still revalidates the tuple before issuing its permit.
    pub(crate) fn select_forecast(
        &self,
        catalog: &ForecastPreparationCatalog,
        instrument_id: InstrumentId,
        source_cutoff: Timestamp,
        cohort: Option<&crate::application::market_calendar::ForecastSessionCohort>,
    ) -> Result<ForecastPreparationSelection, AnalyticalProfileError> {
        self.select_forecast_ranked(catalog, instrument_id, source_cutoff, cohort)
            .map(|(selection, _)| selection)
    }

    /// Applies the same total ordering within and across immutable inventory pages.
    pub(crate) fn select_forecast_ranked(
        &self,
        catalog: &ForecastPreparationCatalog,
        instrument_id: InstrumentId,
        source_cutoff: Timestamp,
        cohort: Option<&crate::application::market_calendar::ForecastSessionCohort>,
    ) -> Result<
        (
            ForecastPreparationSelection,
            (Timestamp, Timestamp, u64, [u8; 32], [u8; 32], u64),
        ),
        AnalyticalProfileError,
    > {
        let horizon = self.horizon();
        let mut selected: Option<(
            &ForecastModelSummary,
            &ForecastEvidenceDataset,
            ForecastEvidencePolicy,
        )> = None;
        for model in catalog.models() {
            if !qualifies(model, horizon) || !self.selects_model(model) {
                continue;
            }
            for dataset in catalog.evidence().datasets() {
                if !matches_model(dataset, model)
                    || dataset.dataset().selection_as_of() > source_cutoff
                    || dataset.pairing().analysis().selection_as_of() > source_cutoff
                    || Some(dataset.pairing().fixed_horizon_nanos()) != horizon.step_nanos()
                {
                    continue;
                }
                let Some(instrument) = dataset.instruments().iter().find(|entry| {
                    entry.instrument_id() == instrument_id
                        && entry.available_at() <= source_cutoff
                        && entry.observed_through() <= source_cutoff
                        && cohort.is_none_or(|cohort| {
                            cohort.reference().horizon_nanos().ok()
                                == horizon
                                    .step_nanos()
                                    .and_then(|n| i64::try_from(n.get()).ok())
                                && entry
                                    .session_origin()
                                    .is_some_and(|origin| cohort.matches_origin(origin))
                        })
                }) else {
                    continue;
                };
                for policy in dataset.policies().iter().copied().filter(|policy| {
                    admits_horizon(*policy, horizon)
                        && instrument.observed_points() >= policy.minimum_observed_points()
                }) {
                    if selected.is_none_or(|(prior_model, prior_dataset, prior_policy)| {
                        selection_key(model, dataset, policy)
                            > selection_key(prior_model, prior_dataset, prior_policy)
                    }) {
                        selected = Some((model, dataset, policy));
                    }
                }
            }
        }
        let (model, dataset, policy) = selected.ok_or(AnalyticalProfileError::ModelUnavailable)?;
        let maximum_age = u64::try_from(
            self.recommendation_policy()
                .parameters()
                .forecast_max_age_nanos,
        )
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
        let rank = selection_key(model, dataset, policy);
        let selection = ForecastPreparationSelection::try_new(
            model.model_id(),
            model.bundle_id().clone(),
            model.bundle_version(),
            dataset.dataset().manifest().clone(),
            dataset.analysis_manifest().clone(),
            instrument_id,
            horizon,
            policy.maximum_validity_nanos().get().min(maximum_age),
        )
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
        Ok((selection, rank))
    }

    fn selects_model(&self, model: &ForecastModelSummary) -> bool {
        match self.resolution().configuration.model_bundle_policy {
            AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1 => true,
            AnalyticalModelBundlePolicy::Exact { model_token } => {
                model.product_evidence().model_token() == model_token
            }
        }
    }
}

pub(super) fn validate_model_policy(
    selection: AnalyticalModelBundlePolicy,
    catalog: Option<&ForecastPreparationCatalog>,
    horizon: ForecastHorizon,
) -> Result<Value, AnalyticalProfileError> {
    match selection {
        AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1 => Ok(json!({
            "selection": selection,
            "output": "calibrated_exact_horizon_conditional_mean",
            "order": "latest_training_cutoff_then_analysis_cutoff_then_bundle_version_then_exact_digest",
            "allowFallback": false,
        })),
        AnalyticalModelBundlePolicy::Exact { model_token } => {
            if model_token.is_nil() {
                return Err(AnalyticalProfileError::InvalidPolicy);
            }
            let catalog = catalog.ok_or(AnalyticalProfileError::ModelUnavailable)?;
            let mut matches = catalog.models().iter().filter(|model| {
                model.product_evidence().model_token() == model_token
                    && qualifies(model, horizon)
                    && has_compatible_data(catalog, model, horizon)
            });
            let model = matches
                .next()
                .ok_or(AnalyticalProfileError::ModelUnavailable)?;
            if matches.next().is_some() {
                return Err(AnalyticalProfileError::IdentityMismatch);
            }
            Ok(json!({"selection": selection, "model": model_identity(model)}))
        }
    }
}

pub(crate) fn model_choices(
    catalog: &ForecastPreparationCatalog,
    horizon: ForecastHorizon,
) -> Vec<Value> {
    catalog.models().iter()
        .filter(|model| qualifies(model, horizon) && has_compatible_data(catalog, model, horizon))
        .map(|model| json!({
            "id": model.product_evidence().model_token(),
            "label": "Calibrated one-year forecast",
            "selection": AnalyticalModelBundlePolicy::Exact { model_token: model.product_evidence().model_token() },
            "identity": model_identity(model),
        }))
        .collect()
}

fn qualifies(model: &ForecastModelSummary, horizon: ForecastHorizon) -> bool {
    let binding = model.output_binding();
    let exact_target = binding
        .expected_terminal_price_horizon_nanos()
        .or_else(|| binding.expected_arithmetic_return_horizon_nanos());
    model.output_semantics() == ModelOutputSemantics::Regression
        && model.has_calibrated_intervals()
        && model.product_evidence().pit_inputs() == ForecastModelEvidenceState::Sufficient
        && model.product_evidence().out_of_sample() == ForecastModelEvidenceState::Sufficient
        && horizon.points().get() == 1
        && horizon
            .step_nanos()
            .is_some_and(|step| exact_target == Some(step))
}

fn has_compatible_data(
    catalog: &ForecastPreparationCatalog,
    model: &ForecastModelSummary,
    horizon: ForecastHorizon,
) -> bool {
    catalog.evidence().datasets().iter().any(|dataset| {
        matches_model(dataset, model)
            && Some(dataset.pairing().fixed_horizon_nanos()) == horizon.step_nanos()
            && dataset
                .policies()
                .iter()
                .any(|policy| admits_horizon(*policy, horizon))
    })
}

fn matches_model(dataset: &ForecastEvidenceDataset, model: &ForecastModelSummary) -> bool {
    dataset.model_id() == model.model_id()
        && dataset.bundle_id() == model.bundle_id()
        && dataset.bundle_version() == model.bundle_version()
        && dataset.dataset().manifest() == model.dataset_manifest()
}

fn admits_horizon(policy: ForecastEvidencePolicy, horizon: ForecastHorizon) -> bool {
    policy.maximum_horizon_points() >= horizon.points()
        && Some(policy.horizon_step_nanos()) == horizon.step_nanos()
}

fn selection_key(
    model: &ForecastModelSummary,
    dataset: &ForecastEvidenceDataset,
    policy: ForecastEvidencePolicy,
) -> (Timestamp, Timestamp, u64, [u8; 32], [u8; 32], u64) {
    (
        dataset.dataset().selection_as_of(),
        dataset.pairing().analysis().selection_as_of(),
        model.bundle_version().get(),
        model.metadata_sha256().bytes(),
        dataset.pairing().pairing_sha256().bytes(),
        policy.maximum_validity_nanos().get(),
    )
}

fn model_identity(model: &ForecastModelSummary) -> Value {
    json!({
        "modelToken": model.product_evidence().model_token(),
        "metadataSha256": hex(model.metadata_sha256().bytes()),
        "artifactSha256": hex(model.artifact_sha256().bytes()),
        "datasetExportSha256": hex(model.dataset_export_sha256().bytes()),
        "datasetPolicySha256": hex(model.dataset_policy_sha256().bytes()),
        "featureCount": model.feature_count(),
    })
}
