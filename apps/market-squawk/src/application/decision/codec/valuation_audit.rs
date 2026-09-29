//! Strict saved method-set mirrors. Recovery reconstructs every domain invariant and digest.
use super::super::DecisionApplicationError;
use super::proposal::RequiredOption;
use market_squawk_domain::{AccountId, Currency, EvidenceDigest, InstrumentId, Money, Timestamp};
use market_squawk_valuation::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

fn invalid<T>(_: T) -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}

macro_rules! wire_enum {
    ($wire:ident, $domain:ident, $($variant:ident),+ $(,)?) => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        #[serde(rename_all="snake_case")]
        enum $wire { $($variant),+ }
        impl From<$domain> for $wire { fn from(value: $domain) -> Self {
            match value { $($domain::$variant => Self::$variant),+ }
        }}
        impl From<$wire> for $domain { fn from(value: $wire) -> Self {
            match value { $($wire::$variant => Self::$variant),+ }
        }}
    }
}
wire_enum!(
    Method,
    AutomaticValuationMethod,
    DiscountedCashFlow,
    ComparableCompanies,
    ResidualIncome,
    ForecastDistribution
);
wire_enum!(
    Stage,
    AutomaticValuationStage,
    SourceSelection,
    NativeFiscalSources,
    AnnualDiscountSources,
    CalculationPublication,
    NativeCalculationPublication
);
wire_enum!(
    Failure,
    AutomaticValuationFailure,
    InvalidRequest,
    NotFound,
    Unauthorized,
    ResourceExhausted,
    Unavailable,
    InvalidResult,
    Internal
);
wire_enum!(
    Purpose,
    AutomaticValuationForecastPurpose,
    PriceDistribution,
    NativeFinancial
);
wire_enum!(
    Basis,
    ValuationAmountBasis,
    PerInstrumentUnit,
    ReportingEntityTotal,
    PositionTotal,
    TotalCommonEquity
);

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MethodSetWire {
    account: AccountId,
    instrument: InstrumentId,
    source_cutoff: Timestamp,
    market_cutoff: Timestamp,
    profile: EvidenceDigest,
    attempts: [Attempt; 4],
    forecast_reads: Vec<ForecastRead>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    selected_measurement: RequiredOption<[u8; 32]>,
    completed_at: Timestamp,
    identity: EvidenceDigest,
}
impl From<&AutomaticValuationMethodSetAudit> for MethodSetWire {
    fn from(v: &AutomaticValuationMethodSetAudit) -> Self {
        Self {
            account: v.account_id(),
            instrument: v.instrument_id(),
            source_cutoff: v.source_cutoff(),
            market_cutoff: v.market_cutoff(),
            profile: v.profile_identity(),
            attempts: v.attempts().each_ref().map(Into::into),
            forecast_reads: v.forecast_reads().iter().map(Into::into).collect(),
            selected_measurement: RequiredOption(v.selected_measurement_id().map(|id| id.bytes())),
            completed_at: v.completed_at(),
            identity: v.identity(),
        }
    }
}
impl MethodSetWire {
    pub(super) fn decode(
        self,
    ) -> Result<AutomaticValuationMethodSetAudit, DecisionApplicationError> {
        if self.forecast_reads.len() > 17 {
            return Err(invalid(()));
        }
        let attempts = self
            .attempts
            .into_iter()
            .map(Attempt::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let reads = self
            .forecast_reads
            .into_iter()
            .map(ForecastRead::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let value = AutomaticValuationMethodSetAudit::try_new(
            self.account,
            self.instrument,
            self.source_cutoff,
            self.market_cutoff,
            self.profile,
            attempts.try_into().map_err(invalid)?,
            reads.into_boxed_slice(),
            self.selected_measurement
                .0
                .map(id::<MeasurementId>)
                .transpose()
                .map_err(invalid)?,
            self.completed_at,
        )
        .map_err(invalid)?;
        if value.identity() != self.identity {
            return Err(invalid(()));
        }
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    method: Method,
    started_at: Timestamp,
    completed_at: Timestamp,
    stage: Stage,
    outcome: Result<Calculation, Failure>,
}
impl From<&AutomaticValuationAttemptAudit> for Attempt {
    fn from(v: &AutomaticValuationAttemptAudit) -> Self {
        Self {
            method: v.method().into(),
            started_at: v.started_at(),
            completed_at: v.completed_at(),
            stage: v.stage().into(),
            outcome: match v.outcome() {
                Ok(v) => Ok(v.into()),
                Err(e) => Err((*e).into()),
            },
        }
    }
}
impl Attempt {
    fn decode(self) -> Result<AutomaticValuationAttemptAudit, DecisionApplicationError> {
        let outcome = match self.outcome {
            Ok(v) => Ok(v.decode()?),
            Err(e) => Err(e.into()),
        };
        AutomaticValuationAttemptAudit::try_new(
            self.method.into(),
            self.started_at,
            self.completed_at,
            self.stage.into(),
            outcome,
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Amount {
    amount: Decimal,
    currency: Currency,
    scale: u8,
    basis: Basis,
}
impl From<ValuationAmount> for Amount {
    fn from(v: ValuationAmount) -> Self {
        Self {
            amount: v.money().amount(),
            currency: v.money().currency(),
            scale: v.scale(),
            basis: v.basis().into(),
        }
    }
}
impl Amount {
    fn decode(self) -> Result<ValuationAmount, DecisionApplicationError> {
        ValuationAmount::try_new(
            Money::new(self.amount, self.currency),
            self.scale,
            self.basis.into(),
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Calculation {
    measurement: [u8; 32],
    calculation: [u8; 32],
    inputs: [u8; 32],
    range: [Amount; 3],
    calculated_at: Timestamp,
    expires_at: Timestamp,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    terminal_growth: RequiredOption<TerminalGrowth>,
    #[serde(deserialize_with = "RequiredOption::deserialize")]
    residual_terminal: RequiredOption<ResidualTerminal>,
    recommendation: Recommendation,
}
impl From<&AutomaticValuationResultAudit> for Calculation {
    fn from(v: &AutomaticValuationResultAudit) -> Self {
        Self {
            measurement: v.measurement_id().bytes(),
            calculation: v.calculation_identity().bytes(),
            inputs: v.input_set_identity().bytes(),
            range: [
                v.range().lower().into(),
                v.range().central().into(),
                v.range().upper().into(),
            ],
            calculated_at: v.calculated_at(),
            expires_at: v.expires_at(),
            terminal_growth: RequiredOption(v.terminal_growth().map(Into::into)),
            residual_terminal: RequiredOption(v.residual_terminal().map(Into::into)),
            recommendation: v.recommendation().into(),
        }
    }
}
impl Calculation {
    fn decode(self) -> Result<AutomaticValuationResultAudit, DecisionApplicationError> {
        let [lower, central, upper] = self.range;
        AutomaticValuationResultAudit::try_recover(
            id::<MeasurementId>(self.measurement)?,
            id::<AutomaticValuationIdentity>(self.calculation)?,
            id::<AutomaticValuationInputSetIdentity>(self.inputs)?,
            AutomaticValuationRange::try_new(lower.decode()?, central.decode()?, upper.decode()?)
                .map_err(invalid)?,
            self.calculated_at,
            self.expires_at,
            self.terminal_growth
                .0
                .map(TerminalGrowth::decode)
                .transpose()?,
            self.residual_terminal
                .0
                .map(ResidualTerminal::decode)
                .transpose()?,
            self.recommendation.decode(),
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TerminalGrowth {
    final_explicit_input: [u8; 32],
    next_period_input: [u8; 32],
    annual_reference: EvidenceDigest,
    annual_rate: EvidenceDigest,
    uncapped: Decimal,
    cap: Decimal,
    applied: Decimal,
    assumption: EvidenceDigest,
}
impl From<&DcfTerminalGrowthAudit> for TerminalGrowth {
    fn from(v: &DcfTerminalGrowthAudit) -> Self {
        Self {
            final_explicit_input: v.final_explicit_input().bytes(),
            next_period_input: v.next_period_input().bytes(),
            annual_reference: v.annual_reference_identity(),
            annual_rate: v.annual_rate_identity(),
            uncapped: v.uncapped_growth(),
            cap: v.nominal_risk_free_cap(),
            applied: v.applied_growth(),
            assumption: v.assumption_identity(),
        }
    }
}
impl TerminalGrowth {
    fn decode(self) -> Result<DcfTerminalGrowthAudit, DecisionApplicationError> {
        DcfTerminalGrowthAudit::try_recover(
            id::<InputId>(self.final_explicit_input)?,
            id::<InputId>(self.next_period_input)?,
            self.annual_reference,
            self.annual_rate,
            self.uncapped,
            self.cap,
            self.applied,
            self.assumption,
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResidualTerminal {
    convention: String,
    terminal_period: u32,
    current_book: [u8; 32],
    final_income: [u8; 32],
    final_book: [u8; 32],
    annual_rate: EvidenceDigest,
    cost_of_equity: Decimal,
    sensitivity: Decimal,
    identity: EvidenceDigest,
}
impl From<&ResidualIncomeTerminalAudit> for ResidualTerminal {
    fn from(v: &ResidualIncomeTerminalAudit) -> Self {
        Self {
            convention: "zero_abnormal_earnings_after_explicit_horizon".into(),
            terminal_period: v.terminal_period().get(),
            current_book: v.current_book_input().bytes(),
            final_income: v.final_income_input().bytes(),
            final_book: v.final_opening_book_input().bytes(),
            annual_rate: v.annual_rate_identity(),
            cost_of_equity: v.annual_cost_of_equity(),
            sensitivity: v.continuing_value_sensitivity(),
            identity: v.identity(),
        }
    }
}
impl ResidualTerminal {
    fn decode(self) -> Result<ResidualIncomeTerminalAudit, DecisionApplicationError> {
        if self.convention != "zero_abnormal_earnings_after_explicit_horizon" {
            return Err(invalid(()));
        }
        ResidualIncomeTerminalAudit::try_recover(
            ResidualIncomeTerminalConvention::ZeroAbnormalEarningsAfterExplicitHorizon,
            NonZeroU32::new(self.terminal_period).ok_or_else(|| invalid(()))?,
            id::<InputId>(self.current_book)?,
            id::<InputId>(self.final_income)?,
            id::<InputId>(self.final_book)?,
            self.annual_rate,
            self.cost_of_equity,
            self.sensitivity,
            self.identity,
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Recommendation {
    assessed_at: Timestamp,
    outcome: RecommendationOutcome,
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RecommendationOutcome {
    Selected,
    NotPerInstrumentUnit,
    ShareUnitBasisUnproven,
    NotCheckedAfterSelection,
    AdmissionFailed(Failure),
}
impl From<AutomaticValuationRecommendationAudit> for Recommendation {
    fn from(v: AutomaticValuationRecommendationAudit) -> Self {
        Self {
            assessed_at: v.assessed_at(),
            outcome: match v.outcome() {
                AutomaticValuationRecommendationOutcome::Selected => {
                    RecommendationOutcome::Selected
                }
                AutomaticValuationRecommendationOutcome::NotPerInstrumentUnit => {
                    RecommendationOutcome::NotPerInstrumentUnit
                }
                AutomaticValuationRecommendationOutcome::ShareUnitBasisUnproven => {
                    RecommendationOutcome::ShareUnitBasisUnproven
                }
                AutomaticValuationRecommendationOutcome::NotCheckedAfterSelection => {
                    RecommendationOutcome::NotCheckedAfterSelection
                }
                AutomaticValuationRecommendationOutcome::AdmissionFailed(e) => {
                    RecommendationOutcome::AdmissionFailed(e.into())
                }
            },
        }
    }
}
impl Recommendation {
    fn decode(self) -> AutomaticValuationRecommendationAudit {
        AutomaticValuationRecommendationAudit::new(
            self.assessed_at,
            match self.outcome {
                RecommendationOutcome::Selected => {
                    AutomaticValuationRecommendationOutcome::Selected
                }
                RecommendationOutcome::NotPerInstrumentUnit => {
                    AutomaticValuationRecommendationOutcome::NotPerInstrumentUnit
                }
                RecommendationOutcome::ShareUnitBasisUnproven => {
                    AutomaticValuationRecommendationOutcome::ShareUnitBasisUnproven
                }
                RecommendationOutcome::NotCheckedAfterSelection => {
                    AutomaticValuationRecommendationOutcome::NotCheckedAfterSelection
                }
                RecommendationOutcome::AdmissionFailed(e) => {
                    AutomaticValuationRecommendationOutcome::AdmissionFailed(e.into())
                }
            },
        )
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ForecastRead {
    purpose: Purpose,
    vintage: EvidenceDigest,
    artifact: EvidenceDigest,
    selection: EvidenceDigest,
    started_at: Timestamp,
    completed_at: Timestamp,
    outcome: Result<EvidenceDigest, SourceFailure>,
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceFailure {
    InvalidProducerEvidence,
    Arithmetic,
    Persistence,
}
impl From<&AutomaticValuationForecastReadAudit> for ForecastRead {
    fn from(v: &AutomaticValuationForecastReadAudit) -> Self {
        Self {
            purpose: v.purpose().into(),
            vintage: v.vintage_identity(),
            artifact: v.artifact_identity(),
            selection: v.selection_identity(),
            started_at: v.started_at(),
            completed_at: v.completed_at(),
            outcome: match v.outcome() {
                Ok(v) => Ok(*v),
                Err(FairValueError::InvalidProducerEvidence) => {
                    Err(SourceFailure::InvalidProducerEvidence)
                }
                Err(FairValueError::Arithmetic) => Err(SourceFailure::Arithmetic),
                Err(FairValueError::Persistence) => Err(SourceFailure::Persistence),
                Err(_) => unreachable!(
                    "validated valuation source audit contains only the closed error set"
                ),
            },
        }
    }
}
impl ForecastRead {
    fn decode(self) -> Result<AutomaticValuationForecastReadAudit, DecisionApplicationError> {
        AutomaticValuationForecastReadAudit::try_new(
            self.purpose.into(),
            self.vintage,
            self.artifact,
            self.selection,
            self.started_at,
            self.completed_at,
            self.outcome.map_err(|e| match e {
                SourceFailure::InvalidProducerEvidence => FairValueError::InvalidProducerEvidence,
                SourceFailure::Arithmetic => FairValueError::Arithmetic,
                SourceFailure::Persistence => FairValueError::Persistence,
            }),
        )
        .map_err(invalid)
    }
}

fn id<T: std::str::FromStr>(bytes: [u8; 32]) -> Result<T, DecisionApplicationError> {
    crate::application::domain_support::encode_hex(bytes)
        .parse()
        .map_err(invalid)
}
