//! Financial component admission shared by clients and the existing analytical producers.
//!
//! This is a projection of the production constructors, not a second policy or model registry.
//! Profile naming, activation, history, discovery breadth, and workflow ordering belong to clients.

mod catalog;
mod forecast;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk_analytics::KnownFeatureImplementation;
use market_squawk_backtesting::{
    RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1, RECOMMENDATION_OOS_FOLD_COUNT_V1,
    RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1, RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
    ResearchExecutionAssumptions, recommendation_conservative_execution_assumptions_v1,
};
use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionPolicy, FeatureDatasetProductContract,
    MissingValuePolicy, PointInTimePolicy, PointInTimeRevisionMode,
};
use market_squawk_decisions::RecommendationPolicy;
use market_squawk_domain::AssetClass;
use market_squawk_modeling::ForecastHorizon;
use market_squawk_valuation::AutomaticValuationMethod;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::decision::RecommendationPolicyParametersWire;
use super::model::forecast_preparation::ForecastPreparationCatalog;

pub(crate) use catalog::catalog;
pub(crate) use forecast::model_choices;

const MINIMUM_PORTFOLIO_DAILY_RETURNS: usize = 252;
const MAXIMUM_PORTFOLIO_DAILY_RETURNS: usize = 1_260;

/// Finite investment scope consumed by identity selection before analysis.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedInvestmentPolicy {
    /// Listed shares and exchange-traded funds.
    ListedEquitiesAndEtfsV1,
    /// Listed shares, excluding exchange-traded funds.
    ListedEquitiesV1,
}

macro_rules! fixed_component {
    ($name:ident, $variant:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            #[doc = $description]
            $variant,
        }
    };
}

fixed_component!(
    HistoricalDatasetPolicy,
    SplitAdjustedQualifiedHistoryV1,
    "Qualified historical observations, evidenced split adjustment, and complete required inputs."
);
fixed_component!(
    RequiredFeatureSet,
    PriceReturnMacroAndPatternsV1,
    "The production price-return and macro feature vector plus causal price-pattern evidence."
);
fixed_component!(
    TrainingCalibrationPolicy,
    ChronologicalRollingOriginV1,
    "Chronological held-out evaluation and calibrated rolling-origin fixed-horizon outcomes."
);
/// Exact recommendation horizon shared by forecasting and historical evaluation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ForecastHorizonPolicy {
    /// One terminal outcome exactly 365 elapsed days after its forecast origin.
    #[serde(rename = "elapsed_365_days_v1")]
    Elapsed365DaysV1,
}
fixed_component!(
    AnalyticalValuationPolicy,
    AllAdmittedAutomaticMethodsV1,
    "Every automatic financial method is enabled and requires its own admitted method-specific inputs."
);
fixed_component!(
    BacktestCostPolicy,
    ConservativeRoundTripV1,
    "The production conservative cost and fill assumptions on both legs of the historical outcome."
);
fixed_component!(
    RiskFreshnessAbstentionPolicy,
    MandatoryEvidenceV1,
    "Every mandatory evidence family must satisfy the selected policy; missing evidence prevents action."
);

/// Selection rule over the existing admitted model inventory.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AnalyticalModelBundlePolicy {
    /// Select the newest compatible training vintage, then deterministic exact identity order.
    BestAdmittedCalibratedMeanV1,
    /// Pin the existing immutable model token; no replacement is allowed after removal.
    Exact {
        /// Opaque immutable token returned by the existing model inventory.
        #[serde(rename = "modelToken")]
        model_token: Uuid,
    },
}

/// Complete financial settings. This contains neither client workflow state nor financial data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticalProfileConfiguration {
    /// Eligible investment population.
    pub supported_investment_policy: SupportedInvestmentPolicy,
    /// Immutable-data and adjustment semantics.
    pub historical_dataset_policy: HistoricalDatasetPolicy,
    /// Mandatory analytical feature families.
    pub required_feature_set: RequiredFeatureSet,
    /// Model choice over the existing admitted inventory.
    pub model_bundle_policy: AnalyticalModelBundlePolicy,
    /// Held-out model evaluation interpretation.
    pub training_calibration_policy: TrainingCalibrationPolicy,
    /// Exact elapsed target horizon.
    pub forecast_horizon_policy: ForecastHorizonPolicy,
    /// Method-specific automatic valuation admission.
    pub valuation_policy: AnalyticalValuationPolicy,
    /// Historical entry/exit cost interpretation.
    pub backtest_cost_policy: BacktestCostPolicy,
    /// The sole canonical copy of editable recommendation, calibration, risk, and age parameters.
    pub recommendation_policy_parameters: RecommendationPolicyParametersWire,
    /// Mandatory evidence and research-only abstention semantics.
    pub risk_freshness_abstention_policy: RiskFreshnessAbstentionPolicy,
    /// Minimum complete daily return observations used for portfolio scenarios.
    pub portfolio_risk_minimum_daily_returns: usize,
    /// Maximum daily return observations retained for portfolio scenarios.
    pub portfolio_risk_maximum_daily_returns: usize,
}

impl AnalyticalProfileConfiguration {
    /// Constructs financial defaults directly from the production policy authority.
    ///
    /// This performs no I/O and claims no data/model readiness. Actual model selection and every
    /// evidence read are admitted by their existing producers at the requested analysis cutoff.
    pub fn default_v1() -> Result<Self, AnalyticalProfileError> {
        let recommendation =
            RecommendationPolicy::v1().map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
        Ok(Self {
            supported_investment_policy: SupportedInvestmentPolicy::ListedEquitiesAndEtfsV1,
            historical_dataset_policy: HistoricalDatasetPolicy::SplitAdjustedQualifiedHistoryV1,
            required_feature_set: RequiredFeatureSet::PriceReturnMacroAndPatternsV1,
            model_bundle_policy: AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1,
            training_calibration_policy: TrainingCalibrationPolicy::ChronologicalRollingOriginV1,
            forecast_horizon_policy: ForecastHorizonPolicy::Elapsed365DaysV1,
            valuation_policy: AnalyticalValuationPolicy::AllAdmittedAutomaticMethodsV1,
            backtest_cost_policy: BacktestCostPolicy::ConservativeRoundTripV1,
            recommendation_policy_parameters: recommendation.parameters().into(),
            risk_freshness_abstention_policy: RiskFreshnessAbstentionPolicy::MandatoryEvidenceV1,
            portfolio_risk_minimum_daily_returns: MINIMUM_PORTFOLIO_DAILY_RETURNS,
            portfolio_risk_maximum_daily_returns: MAXIMUM_PORTFOLIO_DAILY_RETURNS,
        })
    }
}

/// The ten financial component families. Their order is stable and independent of UI layout.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticalProfileComponentFamily {
    /// Supported investment population.
    SupportedInvestmentPolicy,
    /// Historical dataset basis and source qualification.
    HistoricalDatasetPolicy,
    /// Features and price patterns.
    RequiredFeatureSet,
    /// Admitted model selection.
    ModelBundlePolicy,
    /// Training and calibration admission.
    TrainingCalibrationPolicy,
    /// Forecast horizon.
    ForecastHorizonPolicy,
    /// Valuation method admission.
    ValuationPolicy,
    /// Historical costs and fills.
    BacktestCostPolicy,
    /// Action and confidence semantics.
    RecommendationPolicy,
    /// Risk, age, and abstention limits.
    RiskFreshnessAbstentionPolicy,
}

/// Deterministic commitment to actual financial semantics, never a data/execution permission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticalProfileComponentReceipt {
    /// Exact financial family.
    pub family: AnalyticalProfileComponentFamily,
    /// Stable component identifier.
    pub identity: String,
    /// Financial interpretation version.
    pub version: String,
    /// Lowercase hexadecimal SHA-256 of the complete component semantics.
    pub digest: String,
    /// Financial description suitable for ordinary product controls.
    pub label: String,
}

/// Retainable exact financial configuration and its component commitments.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticalProfileResolution {
    /// All selected financial values.
    pub configuration: AnalyticalProfileConfiguration,
    /// Complete configuration and all production component commitments.
    pub configuration_digest: String,
    /// Exactly one receipt for each financial family.
    pub components: [AnalyticalProfileComponentReceipt; 10],
    /// Exact digest accepted by the proposal and shared alpha authorities.
    pub recommendation_policy_digest: String,
}

/// Constructor-admitted financial settings; deserialization cannot create this value.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedAnalyticalProfile {
    resolution: AnalyticalProfileResolution,
    recommendation: RecommendationPolicy,
    execution: ResearchExecutionAssumptions,
    horizon: ForecastHorizon,
    portfolio_risk_minimum_daily_returns: NonZeroUsize,
    portfolio_risk_maximum_daily_returns: NonZeroUsize,
}

impl ValidatedAnalyticalProfile {
    pub(crate) const fn resolution(&self) -> &AnalyticalProfileResolution {
        &self.resolution
    }

    pub(crate) const fn recommendation_policy(&self) -> &RecommendationPolicy {
        &self.recommendation
    }

    pub(crate) const fn execution_assumptions(&self) -> ResearchExecutionAssumptions {
        self.execution
    }

    pub(crate) const fn horizon(&self) -> ForecastHorizon {
        self.horizon
    }

    pub(crate) const fn portfolio_risk_minimum_daily_returns(&self) -> NonZeroUsize {
        self.portfolio_risk_minimum_daily_returns
    }

    pub(crate) const fn portfolio_risk_maximum_daily_returns(&self) -> NonZeroUsize {
        self.portfolio_risk_maximum_daily_returns
    }

    pub(crate) fn admits_investment(&self, asset: AssetClass, exchange_traded_fund: bool) -> bool {
        match self.resolution.configuration.supported_investment_policy {
            SupportedInvestmentPolicy::ListedEquitiesAndEtfsV1 => {
                asset == AssetClass::Equity || exchange_traded_fund
            }
            SupportedInvestmentPolicy::ListedEquitiesV1 => {
                asset == AssetClass::Equity && !exchange_traded_fund
            }
        }
    }

    pub(crate) fn point_in_time_policy(&self) -> Result<PointInTimePolicy, AnalyticalProfileError> {
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
            .map_err(|_| AnalyticalProfileError::InvalidPolicy)
    }

    pub(crate) const fn corporate_action_policy(&self) -> CorporateActionPolicy {
        CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN)
    }

    pub(crate) const fn missing_value_policy(&self) -> MissingValuePolicy {
        MissingValuePolicy::Reject
    }

    pub(crate) const fn dataset_contracts(&self) -> [FeatureDatasetProductContract; 3] {
        dataset_contracts()
    }

    pub(crate) const fn valuation_methods(&self) -> [AutomaticValuationMethod; 4] {
        [
            AutomaticValuationMethod::DiscountedCashFlow,
            AutomaticValuationMethod::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome,
            AutomaticValuationMethod::ForecastDistribution,
        ]
    }
}

/// Validates numeric semantics using the same constructor used by actual recommendation generation.
/// Exact model pins additionally require membership in the current admitted compatible inventory.
pub(crate) fn resolve(
    configuration: Option<AnalyticalProfileConfiguration>,
    models: Option<&ForecastPreparationCatalog>,
) -> Result<ValidatedAnalyticalProfile, AnalyticalProfileError> {
    let configuration =
        configuration.map_or_else(AnalyticalProfileConfiguration::default_v1, Ok)?;
    let risk_minimum = configuration.portfolio_risk_minimum_daily_returns;
    let risk_maximum = configuration.portfolio_risk_maximum_daily_returns;
    if risk_minimum < MINIMUM_PORTFOLIO_DAILY_RETURNS
        || risk_minimum > risk_maximum
        || risk_maximum > MAXIMUM_PORTFOLIO_DAILY_RETURNS
    {
        return Err(AnalyticalProfileError::InvalidPolicy);
    }
    let portfolio_risk_minimum_daily_returns =
        NonZeroUsize::new(risk_minimum).ok_or(AnalyticalProfileError::InvalidPolicy)?;
    let portfolio_risk_maximum_daily_returns =
        NonZeroUsize::new(risk_maximum).ok_or(AnalyticalProfileError::InvalidPolicy)?;
    let recommendation =
        RecommendationPolicy::try_new(configuration.recommendation_policy_parameters.into())
            .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let execution = recommendation_conservative_execution_assumptions_v1()
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let horizon_nanos = u64::try_from(recommendation.horizon_nanos())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(AnalyticalProfileError::InvalidPolicy)?;
    if recommendation.horizon_nanos() != RECOMMENDATION_TARGET_HORIZON_NANOS_V1 {
        return Err(AnalyticalProfileError::InvalidPolicy);
    }
    let horizon = ForecastHorizon::try_new(NonZeroU16::MIN, horizon_nanos)
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let model_binding =
        forecast::validate_model_policy(configuration.model_bundle_policy, models, horizon)?;
    let components = component_receipts(&configuration, &recommendation, execution, model_binding)?;
    let configuration_digest = semantic_digest(
        b"market-squawk/financial-configuration/v1\0",
        &json!({
            "configuration": configuration,
            "components": components.each_ref().map(|receipt| json!({
                "family": receipt.family, "identity": receipt.identity,
                "version": receipt.version, "digest": receipt.digest,
            })),
        }),
    )?;
    let recommendation_policy_digest = hex(recommendation.digest().bytes());
    let validated = ValidatedAnalyticalProfile {
        resolution: AnalyticalProfileResolution {
            configuration,
            configuration_digest,
            components,
            recommendation_policy_digest,
        },
        recommendation,
        execution,
        horizon,
        portfolio_risk_minimum_daily_returns,
        portfolio_risk_maximum_daily_returns,
    };
    validated.point_in_time_policy()?;
    Ok(validated)
}

/// Rebuilds all semantic commitments rather than trusting a caller or persisted digest.
pub(crate) fn revalidate(
    expected: &AnalyticalProfileResolution,
    models: Option<&ForecastPreparationCatalog>,
) -> Result<ValidatedAnalyticalProfile, AnalyticalProfileError> {
    let actual = resolve(Some(expected.configuration.clone()), models)?;
    if &actual.resolution != expected {
        return Err(AnalyticalProfileError::IdentityMismatch);
    }
    Ok(actual)
}

const fn dataset_contracts() -> [FeatureDatasetProductContract; 3] {
    [
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1,
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1,
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
    ]
}

fn component_receipts(
    configuration: &AnalyticalProfileConfiguration,
    policy: &RecommendationPolicy,
    execution: ResearchExecutionAssumptions,
    model_binding: Value,
) -> Result<[AnalyticalProfileComponentReceipt; 10], AnalyticalProfileError> {
    use AnalyticalProfileComponentFamily as Family;
    let p = configuration.recommendation_policy_parameters;
    let contracts = dataset_contracts();
    let harmonic = KnownFeatureImplementation::BatchHarmonicPatterns
        .implementation_digest()
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let returns = KnownFeatureImplementation::BatchReturns
        .implementation_digest()
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let macro_kernel = KnownFeatureImplementation::BatchMacro
        .implementation_digest()
        .map_err(|_| AnalyticalProfileError::InvalidPolicy)?;
    let macro_components = contracts[0]
        .macro_components()
        .iter()
        .map(|component| {
            json!({
                "position": component.position(), "name": component.component_name(),
                "indicator": component.indicator_id(), "unit": component.unit(),
            })
        })
        .collect::<Vec<_>>();
    let semantics = [
        (
            Family::SupportedInvestmentPolicy,
            "Investment coverage",
            json!(configuration.supported_investment_policy),
        ),
        (
            Family::HistoricalDatasetPolicy,
            "Historical data",
            json!({
                "selection": configuration.historical_dataset_policy,
                "contracts": contracts.map(FeatureDatasetProductContract::identity),
                "implementation": contracts[0].implementation_revision(),
                "revision": "latest_known", "adjustment": "split_adjusted", "missing": "reject",
                "allowRetrospectiveStudies": p.allow_retrospective_studies,
            }),
        ),
        (
            Family::RequiredFeatureSet,
            "Features and price patterns",
            json!({
                "selection": configuration.required_feature_set,
                "priceReturn": contracts[0].feature_component_name(),
                "label": contracts[0].label_component_name(),
                "macroComponents": macro_components,
                "returnImplementation": hex(returns.as_bytes()),
                "macroImplementation": hex(macro_kernel.as_bytes()),
                "patternImplementation": hex(harmonic.as_bytes()),
            }),
        ),
        (Family::ModelBundlePolicy, "Forecast model", model_binding),
        (
            Family::TrainingCalibrationPolicy,
            "Training and calibration",
            json!({
                "selection": configuration.training_calibration_policy,
                "trainingContract": contracts[1].identity(),
                "minimumOutcomes": p.minimum_forecast_outcomes,
                "minimumNominalCoveragePpm": p.minimum_nominal_forecast_coverage_ppm,
                "maximumNominalCoveragePpm": p.maximum_nominal_forecast_coverage_ppm,
                "minimumRealizedCoveragePpm": p.minimum_realized_forecast_coverage_ppm,
                "maximumCalibrationErrorPpm": p.maximum_forecast_calibration_error_ppm,
            }),
        ),
        (
            Family::ForecastHorizonPolicy,
            "Investment horizon",
            json!({
                "selection": configuration.forecast_horizon_policy,
                "points": 1, "stepNanos": policy.horizon_nanos().to_string(),
            }),
        ),
        (
            Family::ValuationPolicy,
            "Financial valuation",
            json!({
                "selection": configuration.valuation_policy,
                "methods": ["discounted_cash_flow", "comparable_companies", "residual_income", "forecast_distribution"],
                "maximumAgeNanos": p.valuation_max_age_nanos.to_string(),
                "financialModelMaximumAgeNanos": p.financial_model_max_age_nanos.to_string(),
                "methodSpecificInputsRequired": true,
                "nativeFinancialProjection": super::research::fiscal_projection::fiscal_projection_policy_value(),
            }),
        ),
        (
            Family::BacktestCostPolicy,
            "Modeled trading costs",
            json!({
                "selection": configuration.backtest_cost_policy,
                "currentSizingInterpretation": "conditional_current_side_depth;max_declared_jitter;after_cost_cash_reserve;single_target_change;not_broker_fees;not_future_exit_costs",
                "executionAssumptionDigest": hex(execution.digest().bytes()),
                "folds": RECOMMENDATION_OOS_FOLD_COUNT_V1,
                "foldHorizonNanos": RECOMMENDATION_OOS_FOLD_HORIZON_NANOS_V1.to_string(),
                "evaluationHorizonNanos": RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1.to_string(),
                "minimumObservations": p.minimum_backtest_observations,
                "minimumTrials": p.minimum_backtest_trials,
                "minimumStabilityPpm": p.minimum_backtest_stability_ppm,
                "minimumCompletionPpm": p.minimum_oos_completion_coverage_ppm,
                "minimumCostAdjustedReturnBps": p.minimum_cost_adjusted_return,
                "maximumDrawdownBps": p.maximum_backtest_drawdown,
                "allowRetrospectiveStudies": p.allow_retrospective_studies,
            }),
        ),
        (
            Family::RecommendationPolicy,
            "Recommendation rules",
            json!({
                "version": policy.version().get(), "digest": hex(policy.digest().bytes()),
                "parameters": p,
            }),
        ),
        (
            Family::RiskFreshnessAbstentionPolicy,
            "Risk and evidence freshness",
            json!({
                "selection": configuration.risk_freshness_abstention_policy,
                "proposalLifetimeNanos": p.proposal_lifetime_nanos.to_string(),
                "marketMaxAgeNanos": p.market_max_age_nanos.to_string(),
                "forecastMaxAgeNanos": p.forecast_max_age_nanos.to_string(),
                "valuationMaxAgeNanos": p.valuation_max_age_nanos.to_string(),
                "financialModelMaxAgeNanos": p.financial_model_max_age_nanos.to_string(),
                "backtestMaxAgeNanos": p.backtest_max_age_nanos.to_string(),
                "outOfSampleMaxAgeNanos": p.out_of_sample_max_age_nanos.to_string(),
                "harmonicMaxAgeNanos": p.harmonic_pattern_max_age_nanos.to_string(),
                "liquidityMaxAgeNanos": p.liquidity_max_age_nanos.to_string(),
                "portfolioRiskMaxAgeNanos": p.portfolio_risk_max_age_nanos.to_string(),
                "portfolioRiskMinimumDailyReturns": configuration.portfolio_risk_minimum_daily_returns,
                "portfolioRiskMaximumDailyReturns": configuration.portfolio_risk_maximum_daily_returns,
                "portfolioRiskTailConfidencePpm": 950_000,
                "portfolioRiskHorizon": "one_trading_session",
                "maximumSpreadBps": p.maximum_liquidity_spread,
                "minimumLiquidityCapacityPpm": p.minimum_liquidity_capacity_ppm,
                "minimumPortfolioRiskCapacityPpm": p.minimum_portfolio_risk_capacity_ppm,
                "minimumConfidencePpm": p.minimum_confidence_ppm,
                "allEvidenceRequired": true, "executionAuthority": false,
            }),
        ),
    ];
    let mut receipts = Vec::with_capacity(10);
    for (family, label, semantics) in semantics {
        let family_value =
            serde_json::to_value(family).map_err(|_| AnalyticalProfileError::Encoding)?;
        let family_name = family_value
            .as_str()
            .ok_or(AnalyticalProfileError::Encoding)?;
        receipts.push(AnalyticalProfileComponentReceipt {
            family,
            identity: format!("market-squawk.financial-component.{family_name}"),
            version: "1".to_owned(),
            digest: semantic_digest(
                b"market-squawk/financial-component/v1\0",
                &json!({
                    "family": family, "version": 1, "semantics": semantics,
                }),
            )?,
            label: label.to_owned(),
        });
    }
    receipts
        .try_into()
        .map_err(|_| AnalyticalProfileError::Encoding)
}

fn semantic_digest(domain: &[u8], value: &Value) -> Result<String, AnalyticalProfileError> {
    let encoded = serde_json::to_vec(value).map_err(|_| AnalyticalProfileError::Encoding)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(
        u64::try_from(encoded.len())
            .map_err(|_| AnalyticalProfileError::Encoding)?
            .to_be_bytes(),
    );
    digest.update(encoded);
    Ok(hex(digest.finalize().into()))
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(64);
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    text
}

/// Closed refusal reasons for financial settings and exact model selection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AnalyticalProfileError {
    /// A parameter violates the existing financial authority's invariants.
    #[error("financial settings are invalid")]
    InvalidPolicy,
    /// A pinned or required model is absent or no longer eligible.
    #[error("a compatible calibrated forecast model is unavailable")]
    ModelUnavailable,
    /// Retained component commitments do not match the reconstructed semantics.
    #[error("financial settings have changed")]
    IdentityMismatch,
    /// Financial settings could not be encoded within the closed representation.
    #[error("financial settings could not be represented")]
    Encoding,
}
