//! Product preference units are converted exactly; financial admission belongs to the backend.

use std::{collections::HashSet, sync::Arc};

use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use tokio_util::sync::CancellationToken;

use crate::application::analytical_workflow::host::{
    InvocationAuthority, WorkflowGeneration, WorkflowState, invoke_analytical_operation,
};

use super::{AnalyticalControllerResponse, AnalyticalProfileComponent, WorkflowError};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct FinancialConfiguration(Value);

impl FinancialConfiguration {
    pub(super) fn default_v1() -> Result<Self, WorkflowError> {
        let default = crate::AnalyticalProfileConfiguration::default_v1()
            .map_err(|_error| WorkflowError::internal())?;
        serde_json::to_value(default)
            .map(Self)
            .map_err(|_error| WorkflowError::internal())
    }

    pub(super) fn validate_shape(&self) -> Result<(), WorkflowError> {
        serde_json::from_value::<crate::AnalyticalProfileConfiguration>(self.0.clone())
            .map(|_configuration| ())
            .map_err(|_error| invalid_preferences())
    }

    pub(super) fn value(&self) -> &Value {
        &self.0
    }

    pub(super) fn differences_from(&self, default: &Self) -> Vec<AnalyticalProfileComponent> {
        [
            (
                "supportedInvestmentPolicy",
                AnalyticalProfileComponent::SupportedInvestmentPolicy,
            ),
            (
                "historicalDatasetPolicy",
                AnalyticalProfileComponent::HistoricalDatasetPolicy,
            ),
            (
                "requiredFeatureSet",
                AnalyticalProfileComponent::RequiredFeatureSet,
            ),
            (
                "modelBundlePolicy",
                AnalyticalProfileComponent::ModelBundlePolicy,
            ),
            (
                "trainingCalibrationPolicy",
                AnalyticalProfileComponent::TrainingCalibrationPolicy,
            ),
            (
                "forecastHorizonPolicy",
                AnalyticalProfileComponent::ForecastHorizonPolicy,
            ),
            (
                "valuationPolicy",
                AnalyticalProfileComponent::ValuationPolicy,
            ),
            (
                "backtestCostPolicy",
                AnalyticalProfileComponent::BacktestCostPolicy,
            ),
            (
                "recommendationPolicyParameters",
                AnalyticalProfileComponent::RecommendationPolicy,
            ),
            (
                "riskFreshnessAbstentionPolicy",
                AnalyticalProfileComponent::RiskFreshnessAbstentionPolicy,
            ),
        ]
        .into_iter()
        .filter_map(|(key, family)| {
            let changed = self.0.get(key) != default.0.get(key)
                || matches!(family, AnalyticalProfileComponent::HistoricalDatasetPolicy)
                    && self
                        .0
                        .get("recommendationPolicyParameters")
                        .and_then(|parameters| parameters.get("allow_retrospective_studies"))
                        != default
                            .0
                            .get("recommendationPolicyParameters")
                            .and_then(|parameters| parameters.get("allow_retrospective_studies"))
                || matches!(
                    family,
                    AnalyticalProfileComponent::RiskFreshnessAbstentionPolicy
                ) && [
                    "portfolioRiskMinimumDailyReturns",
                    "portfolioRiskMaximumDailyReturns",
                ]
                .iter()
                .any(|property| self.0.get(*property) != default.0.get(*property));
            changed.then_some(family)
        })
        .collect()
    }

    pub(super) fn preferences(&self) -> Result<FinancialPreferences, WorkflowError> {
        let coverage = match self
            .0
            .get("supportedInvestmentPolicy")
            .and_then(Value::as_str)
        {
            Some("listed_equities_and_etfs_v1") => InvestmentCoverage::StocksAndEtfs,
            Some("listed_equities_v1") => InvestmentCoverage::Stocks,
            _ => return Err(WorkflowError::internal()),
        };
        let model = self
            .0
            .get("modelBundlePolicy")
            .ok_or_else(WorkflowError::internal)?;
        let model_choice = match model.get("kind").and_then(Value::as_str) {
            Some("best_admitted_calibrated_mean_v1") => "recommended".to_owned(),
            Some("exact") => model
                .get("modelToken")
                .and_then(Value::as_str)
                .ok_or_else(WorkflowError::internal)?
                .to_owned(),
            _ => return Err(WorkflowError::internal()),
        };
        let fields = preference_fields()
            .into_iter()
            .map(|field| {
                let value = field
                    .configuration_property
                    .map_or_else(
                        || {
                            self.0
                                .get("recommendationPolicyParameters")?
                                .get(field.parameter)
                        },
                        |property| self.0.get(property),
                    )
                    .and_then(|value| field.index.map_or(Some(value), |index| value.get(index)))
                    .ok_or_else(WorkflowError::internal)?;
                Ok(PreferenceField {
                    key: field.key(),
                    label: field.label,
                    group: field.group,
                    value: field.unit.present(value)?,
                    unit: field.unit.label(),
                    choices: if matches!(field.unit, PreferenceUnit::Rounding) {
                        rounding_choices()
                    } else {
                        Vec::new()
                    },
                })
            })
            .collect::<Result<Vec<_>, WorkflowError>>()?;
        Ok(FinancialPreferences {
            coverage,
            model_choice,
            allow_retrospective_studies: self
                .0
                .get("recommendationPolicyParameters")
                .and_then(|parameters| parameters.get("allow_retrospective_studies"))
                .and_then(Value::as_bool)
                .ok_or_else(WorkflowError::internal)?,
            fields,
        })
    }

    pub(super) fn with_preferences(
        &self,
        preferences: FinancialPreferencesInput,
    ) -> Result<Self, WorkflowError> {
        let definitions = preference_fields();
        if preferences.fields.len() != definitions.len() {
            return Err(invalid_preferences());
        }
        let mut keys = HashSet::with_capacity(preferences.fields.len());
        if preferences
            .fields
            .iter()
            .any(|field| !keys.insert(field.key.as_str()))
        {
            return Err(invalid_preferences());
        }
        let mut candidate = self.0.clone();
        let object = candidate
            .as_object_mut()
            .ok_or_else(WorkflowError::internal)?;
        object.insert(
            "supportedInvestmentPolicy".to_owned(),
            json!(match preferences.coverage {
                InvestmentCoverage::StocksAndEtfs => "listed_equities_and_etfs_v1",
                InvestmentCoverage::Stocks => "listed_equities_v1",
            }),
        );
        let model = if preferences.model_choice == "recommended" {
            json!({ "kind": "best_admitted_calibrated_mean_v1" })
        } else {
            let token = preferences
                .model_choice
                .parse::<uuid::Uuid>()
                .ok()
                .filter(|token| !token.is_nil())
                .ok_or_else(invalid_preferences)?;
            json!({ "kind": "exact", "modelToken": token })
        };
        object.insert("modelBundlePolicy".to_owned(), model);
        object
            .get_mut("recommendationPolicyParameters")
            .and_then(Value::as_object_mut)
            .ok_or_else(WorkflowError::internal)?
            .insert(
                "allow_retrospective_studies".to_owned(),
                Value::Bool(preferences.allow_retrospective_studies),
            );
        for definition in definitions {
            let supplied = preferences
                .fields
                .iter()
                .find(|field| field.key == definition.key())
                .ok_or_else(invalid_preferences)?;
            let value = definition.unit.parse(&supplied.value)?;
            let (parameters, parameter) = if let Some(property) = definition.configuration_property
            {
                (&mut *object, property)
            } else {
                (
                    object
                        .get_mut("recommendationPolicyParameters")
                        .and_then(Value::as_object_mut)
                        .ok_or_else(WorkflowError::internal)?,
                    definition.parameter,
                )
            };
            if let Some(index) = definition.index {
                let item = parameters
                    .get_mut(parameter)
                    .and_then(Value::as_array_mut)
                    .and_then(|values| values.get_mut(index))
                    .ok_or_else(WorkflowError::internal)?;
                *item = value;
            } else {
                parameters.insert(parameter.to_owned(), value);
            }
        }
        let configured = Self(candidate);
        configured.validate_shape()?;
        Ok(configured)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum InvestmentCoverage {
    StocksAndEtfs,
    Stocks,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FinancialPreferences {
    coverage: InvestmentCoverage,
    model_choice: String,
    allow_retrospective_studies: bool,
    fields: Vec<PreferenceField>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FinancialPreferencesInput {
    coverage: InvestmentCoverage,
    #[serde(deserialize_with = "deserialize_preference_text::<_, 64>")]
    model_choice: String,
    allow_retrospective_studies: bool,
    #[serde(deserialize_with = "deserialize_preference_fields")]
    fields: Vec<PreferenceInput>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreferenceInput {
    #[serde(deserialize_with = "deserialize_preference_text::<_, 128>")]
    key: String,
    #[serde(deserialize_with = "deserialize_preference_text::<_, 32>")]
    value: String,
}

fn deserialize_preference_fields<'de, D>(deserializer: D) -> Result<Vec<PreferenceInput>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Fields;
    impl<'de> serde::de::Visitor<'de> for Fields {
        type Value = Vec<PreferenceInput>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("the complete bounded set of analysis preferences")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let maximum = preference_fields().len();
            if sequence.size_hint().is_some_and(|size| size > maximum) {
                return Err(serde::de::Error::custom("too many analysis preferences"));
            }
            let mut fields = Vec::new();
            fields.try_reserve_exact(maximum).map_err(|_error| {
                serde::de::Error::custom("analysis preferences are unavailable")
            })?;
            while let Some(field) = sequence.next_element()? {
                if fields.len() == maximum {
                    return Err(serde::de::Error::custom("too many analysis preferences"));
                }
                fields.push(field);
            }
            if fields.len() != maximum {
                return Err(serde::de::Error::custom(
                    "analysis preferences are incomplete",
                ));
            }
            Ok(fields)
        }
    }
    deserializer.deserialize_seq(Fields)
}

fn deserialize_preference_text<'de, D, const MAXIMUM: usize>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Text<const MAXIMUM: usize>;
    impl<const MAXIMUM: usize> serde::de::Visitor<'_> for Text<MAXIMUM> {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded analysis preference")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAXIMUM {
                return Err(E::custom("analysis preference is too long"));
            }
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAXIMUM {
                return Err(E::custom("analysis preference is too long"));
            }
            Ok(value)
        }
    }
    deserializer.deserialize_string(Text::<MAXIMUM>)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreferenceField {
    key: String,
    label: &'static str,
    group: &'static str,
    value: String,
    unit: &'static str,
    choices: Vec<PreferenceChoice>,
}

#[derive(Clone, Debug, Serialize)]
struct PreferenceChoice {
    value: &'static str,
    label: &'static str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct FinancialResolution(Value);

impl FinancialResolution {
    pub(super) fn admits_configuration(&self, configuration: &FinancialConfiguration) -> bool {
        self.binds_retained_configuration(configuration) && self.valid_shape()
    }

    /// Retention and cancellation do not admit an obsolete financial schema. Every current
    /// producer/activation path additionally requires the backend-owned `valid_shape` below.
    pub(super) fn binds_retained_configuration(
        &self,
        configuration: &FinancialConfiguration,
    ) -> bool {
        self.0.get("configuration") == Some(configuration.value())
    }

    fn configuration(&self) -> Result<FinancialConfiguration, WorkflowError> {
        self.0
            .get("configuration")
            .cloned()
            .map(FinancialConfiguration)
            .ok_or_else(WorkflowError::internal)
    }

    fn typed(&self) -> Result<crate::AnalyticalProfileResolution, WorkflowError> {
        serde_json::from_value(self.0.clone()).map_err(|_error| WorkflowError::internal())
    }

    pub(super) fn valid_shape(&self) -> bool {
        use crate::AnalyticalProfileComponentFamily as Family;
        let families = [
            Family::SupportedInvestmentPolicy,
            Family::HistoricalDatasetPolicy,
            Family::RequiredFeatureSet,
            Family::ModelBundlePolicy,
            Family::TrainingCalibrationPolicy,
            Family::ForecastHorizonPolicy,
            Family::ValuationPolicy,
            Family::BacktestCostPolicy,
            Family::RecommendationPolicy,
            Family::RiskFreshnessAbstentionPolicy,
        ];
        let Ok(resolution) = self.typed() else {
            return false;
        };
        super::valid_digest(&resolution.configuration_digest)
            && super::valid_digest(&resolution.recommendation_policy_digest)
            && resolution
                .components
                .iter()
                .zip(families)
                .all(|(receipt, family)| {
                    receipt.family == family
                        && super::valid_identifier(&receipt.identity, 128)
                        && super::valid_identifier(&receipt.version, 64)
                        && super::valid_digest(&receipt.digest)
                        && super::valid_display_name(&receipt.label)
                })
    }
}

pub(super) async fn validate_profile(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    profile_token: &str,
    profile_state_token: &str,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    let (configuration, digest) = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation
            .analytical_controller()
            .profile_configuration_for_validation(profile_token, profile_state_token)?
    };
    let resolution = resolve_configuration(generation, &configuration, &digest)
        .await
        .ok();
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    generation
        .analytical_controller()
        .retain_profile_validation(profile_token, profile_state_token, resolution)
}

/// A validation records the exact model and policy commitments. Activation must check them again:
/// a removed model or changed financial component cannot be admitted by an old profile token.
pub(super) async fn activate_profile(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
    profile_token: &str,
    profile_state_token: &str,
    validation_token: &str,
    activation_token: &str,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    let expected = {
        let _fence = generation.analytical_retirement_fence().await;
        state.admit_current(generation)?;
        generation
            .analytical_controller()
            .validation_for_activation(
                profile_token,
                profile_state_token,
                validation_token,
                activation_token,
            )?
    };
    let actual = resolve_configuration(
        generation,
        &expected.financial_resolution.configuration()?,
        &expected.config_digest,
    )
    .await?;
    let _fence = generation.analytical_retirement_fence().await;
    state.admit_current(generation)?;
    if actual != expected.financial_resolution {
        return Err(WorkflowError::new(
            "profile_validation_required",
            "The available analysis settings have changed. Validate this profile again before using it.",
        ));
    }
    generation.analytical_controller().activate_custom(
        profile_token,
        profile_state_token,
        validation_token,
        activation_token,
    )
}

pub(super) async fn resolve_configuration(
    generation: &Arc<WorkflowGeneration>,
    configuration: &FinancialConfiguration,
    profile_digest: &str,
) -> Result<FinancialResolution, WorkflowError> {
    let identity = RequestId::try_string(format!("desktop-profile-{profile_digest}"))
        .map_err(|_error| WorkflowError::internal())?;
    let arguments = json!({ "configuration": configuration.value() })
        .as_object()
        .cloned()
        .ok_or_else(WorkflowError::internal)?;
    let response = invoke_analytical_operation(
        generation,
        "AnalyticalProfile.Resolve",
        arguments,
        InvocationAuthority::ReadOnly,
        identity,
        CancellationToken::new(),
    )
    .await?;
    response
        .get("data")
        .cloned()
        .and_then(|value| serde_json::from_value::<FinancialResolution>(value).ok())
        .filter(|resolution| resolution.admits_configuration(configuration))
        .ok_or_else(WorkflowError::internal)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FinancialProfileOptions {
    benchmark_choices: Vec<BenchmarkChoice>,
    model_choices: Vec<ModelChoice>,
    fixed_settings: Vec<FixedSetting>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkChoice {
    instrument_id: uuid::Uuid,
    display_name: String,
    symbol: String,
    comparison_description: String,
    is_default: bool,
}

#[derive(Debug, Serialize)]
struct ModelChoice {
    token: String,
    label: String,
}

#[derive(Debug, Serialize)]
struct FixedSetting {
    label: String,
    value: String,
    explanation: String,
}

pub(super) async fn profile_options(
    state: &WorkflowState,
    generation: &Arc<WorkflowGeneration>,
) -> Result<AnalyticalControllerResponse, WorkflowError> {
    state.admit_current(generation)?;
    let request =
        RequestId::try_string(format!("desktop-profile-options-{}", uuid::Uuid::new_v4()))
            .map_err(|_error| WorkflowError::internal())?;
    let response = invoke_analytical_operation(
        generation,
        "AnalyticalProfile.GetCatalog",
        serde_json::Map::new(),
        InvocationAuthority::ReadOnly,
        request,
        CancellationToken::new(),
    )
    .await?;
    state.admit_current(generation)?;
    let data = response.get("data").ok_or_else(WorkflowError::internal)?;
    let choices = data.get("benchmarkChoices").and_then(Value::as_array)
        .filter(|choices| choices.len() <= 3).ok_or_else(WorkflowError::internal)?;
    let mut benchmark_choices = Vec::with_capacity(choices.len());
    let mut benchmark_ids = HashSet::new();
    let mut has_default = false;
    for choice in choices {
        let instrument_id = choice.get("instrumentId").and_then(Value::as_str)
            .and_then(|id| id.parse::<uuid::Uuid>().ok()).filter(|id| !id.is_nil())
            .ok_or_else(WorkflowError::internal)?;
        let is_default = choice.get("isDefault").and_then(Value::as_bool)
            .ok_or_else(WorkflowError::internal)?;
        if !benchmark_ids.insert(instrument_id) || is_default && has_default {
            return Err(WorkflowError::internal());
        }
        has_default |= is_default;
        benchmark_choices.push(BenchmarkChoice {
            instrument_id,
            display_name: product_text(choice, "displayName", 512)?,
            symbol: product_text(choice, "symbol", 32)?,
            comparison_description: product_text(choice, "comparisonDescription", 523)?,
            is_default,
        });
    }
    let expected = FinancialConfiguration::default_v1()?;
    if data.get("defaultConfiguration") != Some(expected.value()) {
        return Err(WorkflowError::internal());
    }
    let resolution = data
        .get("defaultResolution")
        .cloned()
        .and_then(|value| serde_json::from_value::<FinancialResolution>(value).ok())
        .filter(|resolution| resolution.admits_configuration(&expected))
        .ok_or_else(WorkflowError::internal)?;
    let models = data
        .get("models")
        .and_then(Value::as_array)
        .filter(|models| models.len() <= 1_000)
        .ok_or_else(WorkflowError::internal)?;
    let mut model_choices = vec![ModelChoice {
        token: "recommended".to_owned(),
        label: "Recommended calibrated forecast".to_owned(),
    }];
    let mut tokens = HashSet::new();
    for model in models {
        let token = model
            .get("id")
            .and_then(Value::as_str)
            .and_then(|id| id.parse::<uuid::Uuid>().ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(WorkflowError::internal)?;
        if !tokens.insert(token) {
            return Err(WorkflowError::internal());
        }
        if model.get("selection") != Some(&json!({ "kind": "exact", "modelToken": token })) {
            return Err(WorkflowError::internal());
        }
        model_choices.push(ModelChoice {
            token: token.to_string(),
            label: product_text(model, "label", 256)?,
        });
    }
    let components = data
        .get("components")
        .and_then(Value::as_array)
        .filter(|components| components.len() == 10)
        .ok_or_else(WorkflowError::internal)?;
    let mut fixed_settings = Vec::with_capacity(8);
    let resolution = resolution.typed()?;
    for (component, receipt) in components.iter().zip(&resolution.components) {
        let family =
            serde_json::to_value(receipt.family).map_err(|_error| WorkflowError::internal())?;
        if component.get("family") != Some(&family) {
            return Err(WorkflowError::internal());
        }
        if matches!(
            component.get("family").and_then(Value::as_str),
            Some("supported_investment_policy" | "model_bundle_policy")
        ) {
            continue;
        }
        let choice = component
            .get("choices")
            .and_then(Value::as_array)
            .filter(|choices| choices.len() == 1)
            .and_then(|choices| choices.first())
            .ok_or_else(WorkflowError::internal)?;
        let mut explanation = product_text(component, "description", 1024)?;
        if receipt.family == crate::AnalyticalProfileComponentFamily::BacktestCostPolicy {
            explanation.push(' ');
            explanation.push_str(&historical_cost_description(data)?);
        }
        fixed_settings.push(FixedSetting {
            label: product_text(component, "label", 64)?,
            value: product_text(choice, "label", 256)?,
            explanation,
        });
    }
    Ok(AnalyticalControllerResponse::ProfileOptions {
        options: FinancialProfileOptions {
            benchmark_choices,
            model_choices,
            fixed_settings,
        },
    })
}

fn historical_cost_description(data: &Value) -> Result<String, WorkflowError> {
    let costs = data
        .get("historicalCosts")
        .ok_or_else(WorkflowError::internal)?;
    if costs.get("appliesToEntryAndExit").and_then(Value::as_bool) != Some(true) {
        return Err(WorkflowError::internal());
    }
    let percent = |field| {
        costs
            .get(field)
            .ok_or_else(WorkflowError::internal)
            .and_then(|value| PreferenceUnit::PercentBps.present(value))
    };
    let fees = percent("feesBps")?;
    let slippage = percent("slippageBps")?;
    let random_slippage = percent("maximumRandomSlippageBps")?;
    let participation = percent("maximumParticipationBps")?;
    let latency = costs
        .get("latencyNanos")
        .ok_or_else(WorkflowError::internal)
        .and_then(|value| PreferenceUnit::Seconds.present(value))?;
    let partial_fills = match costs.get("allowPartialFills").and_then(Value::as_bool) {
        Some(true) => "Partial fills are allowed.",
        Some(false) => "Orders must fill completely.",
        None => return Err(WorkflowError::internal()),
    };
    Ok(format!(
        "Each entry and exit includes {fees}% fees, {slippage}% slippage and up to {random_slippage}% additional random slippage. Maximum volume participation is {participation}%, with {latency} seconds of delay. {partial_fills}"
    ))
}

fn product_text(value: &Value, field: &str, maximum: usize) -> Result<String, WorkflowError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| {
            !text.is_empty() && text.len() <= maximum && !text.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .ok_or_else(WorkflowError::internal)
}

fn rounding_choices() -> Vec<PreferenceChoice> {
    [
        ("nearest_even", "Nearest, ties to even"),
        ("away_from_zero", "Away from zero"),
        ("toward_zero", "Toward zero"),
        ("floor", "Down"),
        ("ceiling", "Up"),
    ]
    .into_iter()
    .map(|(value, label)| PreferenceChoice { value, label })
    .collect()
}

#[derive(Clone, Copy)]
enum PreferenceUnit {
    Count,
    DailyReturns,
    PercentPpm,
    PercentBps,
    Seconds,
    Rounding,
}

impl PreferenceUnit {
    const fn label(self) -> &'static str {
        match self {
            Self::Count => "",
            Self::DailyReturns => "daily returns",
            Self::PercentPpm | Self::PercentBps => "%",
            Self::Seconds => "seconds",
            Self::Rounding => "",
        }
    }

    const fn decimal_places(self) -> u32 {
        match self {
            Self::PercentPpm => 4,
            Self::PercentBps => 2,
            Self::Seconds => 9,
            Self::Count | Self::DailyReturns | Self::Rounding => 0,
        }
    }

    fn present(self, value: &Value) -> Result<String, WorkflowError> {
        if matches!(self, Self::Rounding) {
            return value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(WorkflowError::internal);
        }
        let integer = if matches!(self, Self::Seconds) {
            value.as_str().and_then(|value| value.parse::<i64>().ok())
        } else {
            value.as_i64()
        }
        .ok_or_else(WorkflowError::internal)?;
        let divisor = 10_i128.pow(self.decimal_places());
        let magnitude = i128::from(integer).abs();
        let whole = magnitude / divisor;
        let fraction = magnitude % divisor;
        let sign = if integer < 0 { "-" } else { "" };
        if fraction == 0 {
            return Ok(format!("{sign}{whole}"));
        }
        let fraction = format!("{fraction:0width$}", width = self.decimal_places() as usize);
        Ok(format!("{sign}{whole}.{}", fraction.trim_end_matches('0')))
    }

    fn parse(self, value: &str) -> Result<Value, WorkflowError> {
        if matches!(self, Self::Rounding) {
            return rounding_choices()
                .iter()
                .any(|choice| choice.value == value)
                .then(|| json!(value))
                .ok_or_else(invalid_preferences);
        }
        if value.is_empty() || value.len() > 32 || value.trim() != value {
            return Err(invalid_preferences());
        }
        let (negative, magnitude) = value
            .strip_prefix('-')
            .map_or((false, value), |value| (true, value));
        let (whole, fraction) = magnitude.split_once('.').unwrap_or((magnitude, ""));
        if whole.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            || fraction.len() > self.decimal_places() as usize
        {
            return Err(invalid_preferences());
        }
        let factor = 10_i128.pow(self.decimal_places());
        let whole = whole
            .parse::<i128>()
            .map_err(|_error| invalid_preferences())?;
        let fractional = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse::<i128>()
                .map_err(|_error| invalid_preferences())?
                * 10_i128.pow(self.decimal_places() - fraction.len() as u32)
        };
        let combined = whole
            .checked_mul(factor)
            .and_then(|whole| whole.checked_add(fractional))
            .ok_or_else(invalid_preferences)?;
        let integer = i64::try_from(if negative { -combined } else { combined })
            .map_err(|_error| invalid_preferences())?;
        Ok(if matches!(self, Self::Seconds) {
            json!(integer.to_string())
        } else {
            json!(integer)
        })
    }
}

struct FieldDefinition {
    parameter: &'static str,
    configuration_property: Option<&'static str>,
    index: Option<usize>,
    label: &'static str,
    group: &'static str,
    unit: PreferenceUnit,
}

impl FieldDefinition {
    fn key(&self) -> String {
        self.index.map_or_else(
            || self.parameter.to_owned(),
            |index| format!("{}.{index}", self.parameter),
        )
    }
}

fn preference_fields() -> Vec<FieldDefinition> {
    use PreferenceUnit::{Count, PercentBps, PercentPpm, Rounding, Seconds};
    let mut fields: Vec<_> = [
        (
            "proposal_lifetime_nanos",
            "Recommendation lifetime",
            "Freshness",
            Seconds,
        ),
        (
            "market_max_age_nanos",
            "Maximum market information age",
            "Freshness",
            Seconds,
        ),
        (
            "forecast_max_age_nanos",
            "Maximum forecast age",
            "Freshness",
            Seconds,
        ),
        (
            "valuation_max_age_nanos",
            "Maximum valuation age",
            "Freshness",
            Seconds,
        ),
        (
            "financial_model_max_age_nanos",
            "Maximum financial model age",
            "Freshness",
            Seconds,
        ),
        (
            "backtest_max_age_nanos",
            "Maximum historical test age",
            "Freshness",
            Seconds,
        ),
        (
            "out_of_sample_max_age_nanos",
            "Maximum held-out test age",
            "Freshness",
            Seconds,
        ),
        (
            "harmonic_pattern_max_age_nanos",
            "Maximum price-pattern age",
            "Freshness",
            Seconds,
        ),
        (
            "liquidity_max_age_nanos",
            "Maximum liquidity information age",
            "Freshness",
            Seconds,
        ),
        (
            "portfolio_risk_max_age_nanos",
            "Maximum portfolio risk information age",
            "Freshness",
            Seconds,
        ),
        (
            "bullish_threshold",
            "Positive expected-return threshold",
            "Recommendation thresholds",
            PercentBps,
        ),
        (
            "bearish_threshold",
            "Negative expected-return threshold",
            "Recommendation thresholds",
            PercentBps,
        ),
        (
            "minimum_forecast_outcomes",
            "Minimum completed forecast outcomes",
            "Forecast validation",
            Count,
        ),
        (
            "minimum_nominal_forecast_coverage_ppm",
            "Minimum stated interval coverage",
            "Forecast validation",
            PercentPpm,
        ),
        (
            "maximum_nominal_forecast_coverage_ppm",
            "Maximum stated interval coverage",
            "Forecast validation",
            PercentPpm,
        ),
        (
            "minimum_realized_forecast_coverage_ppm",
            "Minimum observed interval coverage",
            "Forecast validation",
            PercentPpm,
        ),
        (
            "maximum_forecast_calibration_error_ppm",
            "Maximum calibration error",
            "Forecast validation",
            PercentPpm,
        ),
        (
            "minimum_backtest_observations",
            "Minimum historical observations",
            "Historical validation",
            Count,
        ),
        (
            "minimum_backtest_trials",
            "Minimum historical trials",
            "Historical validation",
            Count,
        ),
        (
            "minimum_backtest_stability_ppm",
            "Minimum test stability",
            "Historical validation",
            PercentPpm,
        ),
        (
            "minimum_oos_completion_coverage_ppm",
            "Minimum held-out completion",
            "Historical validation",
            PercentPpm,
        ),
        (
            "minimum_cost_adjusted_return",
            "Minimum return after costs",
            "Historical validation",
            PercentBps,
        ),
        (
            "maximum_backtest_drawdown",
            "Maximum historical drawdown",
            "Risk limits",
            PercentBps,
        ),
        (
            "maximum_liquidity_spread",
            "Maximum quoted spread",
            "Risk limits",
            PercentBps,
        ),
        (
            "minimum_liquidity_capacity_ppm",
            "Minimum liquidity capacity",
            "Risk limits",
            PercentPpm,
        ),
        (
            "minimum_portfolio_risk_capacity_ppm",
            "Minimum portfolio risk capacity",
            "Risk limits",
            PercentPpm,
        ),
        (
            "minimum_confidence_ppm",
            "Minimum evidence reliability",
            "Recommendation thresholds",
            PercentPpm,
        ),
        (
            "forecast_base_weight_bps",
            "Forecast contribution",
            "Price estimates",
            PercentBps,
        ),
        (
            "valuation_weight_bps",
            "Valuation contribution",
            "Price estimates",
            PercentBps,
        ),
        (
            "price_scale",
            "Price decimal places",
            "Price estimates",
            Count,
        ),
        (
            "rounding_policy",
            "Price rounding",
            "Price estimates",
            Rounding,
        ),
    ]
    .into_iter()
    .map(|(parameter, label, group, unit)| FieldDefinition {
        parameter,
        configuration_property: None,
        index: None,
        label,
        group,
        unit,
    })
    .collect();
    for (parameter, property, label) in [
        (
            "portfolio_risk_minimum_daily_returns",
            "portfolioRiskMinimumDailyReturns",
            "Minimum portfolio history",
        ),
        (
            "portfolio_risk_maximum_daily_returns",
            "portfolioRiskMaximumDailyReturns",
            "Maximum portfolio history",
        ),
    ] {
        fields.push(FieldDefinition {
            parameter,
            configuration_property: Some(property),
            index: None,
            label,
            group: "Portfolio risk history",
            unit: PreferenceUnit::DailyReturns,
        });
    }
    for (index, label) in [
        "Forecast calibration weight",
        "Valuation agreement weight",
        "Historical stability weight",
        "Market integrity weight",
        "Liquidity capacity weight",
        "Portfolio risk capacity weight",
    ]
    .into_iter()
    .enumerate()
    {
        fields.push(FieldDefinition {
            parameter: "confidence_weights_ppm",
            configuration_property: None,
            index: Some(index),
            label,
            group: "Evidence reliability weights",
            unit: PercentPpm,
        });
    }
    for (index, label) in [
        "Exit lower bound: lower-price weight",
        "Exit upper bound: lower-price weight",
        "Add lower bound: lower-price weight",
        "Add upper bound: lower-price weight",
        "Entry lower bound: lower-price weight",
        "Entry upper bound: lower-price weight",
        "Trim lower bound: lower-price weight",
        "Trim upper bound: lower-price weight",
        "Add target: lower-bound weight",
    ]
    .into_iter()
    .enumerate()
    {
        fields.push(FieldDefinition {
            parameter: "price_range_weights_bps",
            configuration_property: None,
            index: Some(index),
            label,
            group: "Price-range weights",
            unit: PercentBps,
        });
    }
    fields
}

fn invalid_preferences() -> WorkflowError {
    WorkflowError::invalid_request(
        "One or more analysis preferences are invalid. Review the values and try again.",
    )
}
