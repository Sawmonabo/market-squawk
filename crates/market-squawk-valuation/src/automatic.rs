//! Evidence-closed, deterministic automatic valuation calculations.
//!
//! This module deliberately stops before fair-value measurement construction. A completed
//! calculation is research-only: it is not a crate::ValuationMeasurement, classification,
//! approval, recommendation, position, order, or execution authority. The serialized adapter
//! creates a genuine derived crate::ValuationInput before the existing measurement,
//! classification, independent-approval, and latest-valid-selection authorities may be used.

use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_data::{
    CompanySecurityIdentityDisposition, CompanySecurityIdentitySelectionReceipt,
    DatasetManifestRef, FinancialAmountBasis, FinancialAmountRole, MarketEventCommitRef,
    MarketEventUseInput, ResearchUseDecisionDigest, ResearchUseGraphDigest,
};
use market_squawk_domain::{
    AccountId, Currency, DigestAlgorithm, EvidenceDigest, FundamentalCadence, FundamentalPeriod,
    IdentifierEntitlement, InstrumentId, Money, RoundingPolicy, Timestamp,
};
use rust_decimal::{Decimal, RoundingStrategy};
use thiserror::Error;

use crate::{
    ActorId, CanonicalHasher, EvidenceOrigin, EvidenceVerification, FinancialModelMacroAssumptions,
    ForecastValuationValueSelection, InputId, InputInstrumentRelation, ValuationAmount,
    ValuationAmountBasis, ValuationInput,
};

mod common_shares;
pub use common_shares::{
    CommonShareFilingEvidence, CommonShareValuationBasis, REPORTED_COMMON_SHARE_ASSUMPTION,
};
mod current_share;
pub use current_share::CurrentShareValuationProjection;

mod equity_premium;
pub use equity_premium::{
    AnnualEquityPremiumArithmetic, EQUITY_PREMIUM_ESTIMATOR, EQUITY_PREMIUM_SAMPLE_YEARS,
    ModeledGovernmentAnnualReturn,
};
mod method_set;
pub use method_set::{
    AutomaticValuationAttemptAudit, AutomaticValuationFailure, AutomaticValuationForecastPurpose,
    AutomaticValuationForecastReadAudit, AutomaticValuationMethodSetAudit,
    AutomaticValuationRecommendationAudit, AutomaticValuationRecommendationOutcome,
    AutomaticValuationResultAudit, AutomaticValuationStage, DcfTerminalGrowthAudit,
    ResidualIncomeTerminalAudit,
};
mod terminal;
pub use terminal::DcfTerminalGrowthPolicy;
mod residual;
pub use residual::{ResidualIncomeTerminalConvention, ResidualIncomeTerminalReceipt};

const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_METHOD_INPUTS: usize = 512;
const MAX_INPUT_MANIFESTS: usize = 4096;
const MAX_ASSUMPTIONS: usize = 128;
const MAX_DCF_PERIODS: usize = 128;
const MAX_COMPARABLES: usize = 256;
const MAX_RESIDUAL_PERIODS: usize = 128;
const MAX_FORECAST_POINTS: usize = 512;
const PROBABILITY_PARTS_PER_MILLION: u32 = 1_000_000;

digest_id!(
    /// SHA-256 identity of one complete automatic valuation calculation receipt.
    AutomaticValuationIdentity
);
digest_id!(
    /// SHA-256 identity of the exact point-in-time input set used by a calculation.
    AutomaticValuationInputSetIdentity
);

/// Required evidence or authority that was not usable at the requested cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationUnavailable {
    /// Exact current company/security identity was not available.
    Identity,
    /// Local-analysis rights were absent, wrong-use, or expired.
    Rights,
    /// Exact current market evidence was not available.
    CurrentMarket,
    /// A required method input was absent, stale, or unusable.
    MethodInput,
    /// A required economic assumption was absent, stale, or unusable.
    Assumption,
}

/// Independently supplied coordinates that did not agree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationConflict {
    /// Company/security identity evidence was ambiguous or internally inconsistent.
    Identity,
    /// Currency coordinates disagreed.
    Currency,
    /// Monetary-unit or economic-basis coordinates disagreed.
    AmountBasis,
    /// Immutable evidence or point-in-time selection coordinates disagreed.
    Evidence,
    /// The explicit comparable-company set was duplicate or otherwise contradictory.
    PeerSet,
    /// Explicit probability or peer-weight mass did not equal one.
    ProbabilityMass,
    /// Explicit uncertainty bounds did not contain the calculated central value.
    Uncertainty,
}

/// Fail-closed automatic valuation calculation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AutomaticValuationError {
    /// A required input or authority was unavailable.
    #[error("automatic valuation is unavailable: {0:?}")]
    Unavailable(AutomaticValuationUnavailable),
    /// Independently supplied inputs conflict.
    #[error("automatic valuation inputs conflict: {0:?}")]
    Conflict(AutomaticValuationConflict),
    /// A bounded identifier, time window, method shape, or digest was malformed.
    #[error("automatic valuation contract is invalid")]
    InvalidContract,
    /// Checked decimal, integer, or collection arithmetic failed.
    #[error("automatic valuation checked arithmetic failed")]
    Arithmetic,
}

/// Closed research-only calculation method.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AutomaticValuationMethod {
    /// Discount explicit forecast cash flows and an explicit terminal value.
    DiscountedCashFlow,
    /// Apply an explicit weighted peer multiple to an explicit subject metric.
    ComparableCompanies,
    /// Add discounted explicit residual-income forecasts to current book value.
    ResidualIncome,
    /// Calculate an expectation from explicit forecast outcomes and probabilities.
    ForecastDistribution,
}

/// Caller-selected deterministic arithmetic policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuationArithmeticPolicy {
    rounding: RoundingPolicy,
    maximum_periods: usize,
}

impl ValuationArithmeticPolicy {
    /// Constructs an explicit rounding policy and bounded period ceiling.
    pub fn try_new(
        rounding: RoundingPolicy,
        maximum_periods: usize,
    ) -> Result<Self, AutomaticValuationError> {
        if maximum_periods == 0 || maximum_periods > MAX_FORECAST_POINTS {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            rounding,
            maximum_periods,
        })
    }

    /// Returns the explicit final-output rounding rule.
    pub const fn rounding(self) -> RoundingPolicy {
        self.rounding
    }

    /// Returns the caller-selected hard period ceiling.
    pub const fn maximum_periods(self) -> usize {
        self.maximum_periods
    }
}

/// A genuine valuation input plus its exact point-in-time selection receipt and lifetime.
///
/// The wrapped ValuationInput must already have been constructed by an existing producer
/// boundary. This type cannot turn a scalar or digest into producer evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointInTimeValuationInput {
    input: ValuationInput,
    selection_receipt: EvidenceDigest,
    rights_input_digest: EvidenceDigest,
    knowledge_at: Timestamp,
    expires_at: Timestamp,
}

impl PointInTimeValuationInput {
    /// Binds one genuine producer input to an exact point-in-time selection.
    pub fn try_new(
        input: ValuationInput,
        selection_receipt: EvidenceDigest,
        rights_input_digest: EvidenceDigest,
        knowledge_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        let evidence = input.evidence();
        let forecast_source = match evidence.origin() {
            EvidenceOrigin::ForecastDistribution { evidence } => Some(evidence.source()),
            _ => None,
        };
        let publication_ceiling =
            forecast_source.map_or(knowledge_at, |source| source.reference().selected_at());
        if !valid_sha256(rights_input_digest)
            || !valid_sha256(selection_receipt)
            || evidence.verification() != EvidenceVerification::Verified
            || !evidence.producer_verification_is_current_at(knowledge_at)
            || evidence.available_at().is_none()
            || evidence
                .available_at()
                .is_some_and(|available_at| available_at > publication_ceiling)
            || evidence.ingested_at() > publication_ceiling
            || forecast_source.is_some_and(|source| {
                source.reference().knowledge_at() != knowledge_at
                    || source.distribution().expires_at() < expires_at
            })
            || expires_at <= knowledge_at
            || evidence
                .automatic_selection_binding()
                .is_some_and(|binding| binding != (selection_receipt, knowledge_at))
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            input,
            selection_receipt,
            rights_input_digest,
            knowledge_at,
            expires_at,
        })
    }

    /// Returns the genuine immutable producer-derived input.
    pub const fn input(&self) -> &ValuationInput {
        &self.input
    }

    /// Returns the exact point-in-time selection receipt.
    pub const fn selection_receipt(&self) -> EvidenceDigest {
        self.selection_receipt
    }

    /// Returns the exact physical and logical rights inputs that admitted this selected input.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.rights_input_digest
    }

    /// Returns the exact knowledge cutoff used by the selection.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    /// Returns the exclusive selection lifetime.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Closed economic-assumption role. This module supplies no value for any role.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AutomaticValuationAssumptionKind {
    /// Per-period DCF discount rate, expressed as an exact decimal rate.
    DiscountRate,
    /// Explicit comparable-company weight, expressed as a decimal fraction.
    ComparableWeight,
    /// Per-period residual-income cost of equity, expressed as an exact decimal rate.
    CostOfEquity,
    /// Explicit forecast-outcome probability, expressed as a decimal fraction.
    ForecastProbability,
    /// Lower uncertainty amount in the calculation output unit.
    UncertaintyLower,
    /// Upper uncertainty amount in the calculation output unit.
    UncertaintyUpper,
    /// Explicit annual perpetual growth of common-equity cash flow after the final forecast year.
    TerminalGrowth,
}

/// Evidence-bound, finite-lived, caller-supplied economic assumption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationAssumption {
    kind: AutomaticValuationAssumptionKind,
    identifier: Box<str>,
    value: Decimal,
    evidence: EvidenceDigest,
    available_at: Timestamp,
    expires_at: Timestamp,
}

impl AutomaticValuationAssumption {
    /// Constructs an assumption without interpreting or defaulting its economic value.
    pub fn try_new(
        kind: AutomaticValuationAssumptionKind,
        identifier: &str,
        value: Decimal,
        evidence: EvidenceDigest,
        available_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        if !valid_identifier(identifier) || !valid_sha256(evidence) || expires_at <= available_at {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            kind,
            identifier: identifier.into(),
            value: value.normalize(),
            evidence,
            available_at,
            expires_at,
        })
    }

    /// Returns the assumption role.
    pub const fn kind(&self) -> AutomaticValuationAssumptionKind {
        self.kind
    }

    /// Returns the caller-owned stable assumption identity.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns the exact caller-supplied value.
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns the exact assumption evidence identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns conservative assumption availability.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns exclusive assumption expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Evidence-bound lower and upper uncertainty assumptions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationUncertainty {
    lower: AutomaticValuationAssumption,
    upper: AutomaticValuationAssumption,
}

impl AutomaticValuationUncertainty {
    /// Constructs an ordered uncertainty contract in the calculation output unit.
    pub fn try_new(
        lower: AutomaticValuationAssumption,
        upper: AutomaticValuationAssumption,
    ) -> Result<Self, AutomaticValuationError> {
        if lower.kind() != AutomaticValuationAssumptionKind::UncertaintyLower
            || upper.kind() != AutomaticValuationAssumptionKind::UncertaintyUpper
            || lower.value() > upper.value()
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self { lower, upper })
    }

    /// Returns the evidence-bound lower amount assumption.
    pub const fn lower(&self) -> &AutomaticValuationAssumption {
        &self.lower
    }

    /// Returns the evidence-bound upper amount assumption.
    pub const fn upper(&self) -> &AutomaticValuationAssumption {
        &self.upper
    }
}

mod rights;
pub use rights::{ValuationEventRightsAdmission, ValuationRightsReceipt};
use rights::{combined_rights_input_digest, hash_event_admission};

/// Common exact authority, identity, market, unit, and time coordinates.
#[derive(Debug)]
pub struct AutomaticValuationInput {
    /// Reporting account to which a later governed measurement would belong.
    pub account_id: AccountId,
    /// Exact current company/security identity receipt.
    pub company_security: CompanySecurityIdentitySelectionReceipt,
    /// Exact security being valued.
    pub instrument_id: InstrumentId,
    /// Exact output currency.
    pub currency: Currency,
    /// Exact output economic unit.
    pub amount_basis: ValuationAmountBasis,
    /// Genuine current market evidence selected at the same cutoff.
    pub current_market: PointInTimeValuationInput,
    /// Catalog-issued single-use local-analysis authority.
    pub rights: ValuationRightsReceipt,
    /// Point-in-time valuation and knowledge cutoff.
    pub measurement_at: Timestamp,
    /// Calculation completion time.
    pub calculated_at: Timestamp,
    /// Exclusive calculation-result expiry selected by caller policy.
    pub expires_at: Timestamp,
    /// Exact calculation actor identity; this is not an approval identity.
    pub calculated_by: ActorId,
    /// Exact declared decimal scale for output amounts.
    pub output_scale: u8,
    /// Explicit rounding and bounded-period policy.
    pub arithmetic_policy: ValuationArithmeticPolicy,
}

/// One explicitly forecast DCF cash flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DcfCashFlow {
    /// One-based period offset.
    pub period: NonZeroU32,
    /// Exact producer-derived cash-flow input and point-in-time selection.
    pub cash_flow: PointInTimeValuationInput,
}

/// Complete discounted-cash-flow request.
#[derive(Debug)]
pub struct DiscountedCashFlowValuationRequest {
    /// Exact count of equal financial periods per year; the supplied rate is per such period.
    /// Annual cash flows and annual rates use one, without assuming a fixed civil-year duration.
    pub periods_per_year: NonZeroU32,
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Explicit cash-flow forecast; the calculator performs no forecasting.
    pub cash_flows: Vec<DcfCashFlow>,
    /// Exact annual government reference and independently evidenced equity risk premium.
    pub macro_assumptions: FinancialModelMacroAssumptions,
    /// One-based terminal period.
    pub terminal_period: NonZeroU32,
    /// Genuine next-period common FCFE (N+1); it is not also an explicit flow at N.
    pub terminal_cash_flow: PointInTimeValuationInput,
    /// Evidence-bound uncertainty bounds.
    pub uncertainty: AutomaticValuationUncertainty,
}

/// One exact caller-selected comparable company.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparableCompanyInput {
    /// Exact peer company/security identity receipt.
    pub company_security: CompanySecurityIdentitySelectionReceipt,
    /// Exact peer security.
    pub instrument_id: InstrumentId,
    /// Explicit peer metric denominator.
    pub metric: PointInTimeValuationInput,
    /// Explicit peer value numerator.
    pub value: PointInTimeValuationInput,
    /// Explicit nonzero peer weight in parts per million.
    pub weight_ppm: u32,
    /// Evidence receipt whose decimal value must equal the exact weight.
    pub weight_assumption: AutomaticValuationAssumption,
}

/// Complete comparable-companies request.
#[derive(Debug)]
pub struct ComparableCompaniesValuationRequest {
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Exact subject metric to which the weighted peer multiple is applied.
    pub subject_metric: PointInTimeValuationInput,
    /// Explicit peer set; the calculator performs no peer discovery.
    pub comparables: Vec<ComparableCompanyInput>,
    /// Evidence-bound uncertainty bounds.
    pub uncertainty: AutomaticValuationUncertainty,
}

/// One checked peer contribution. This arithmetic result grants no source or approval authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComparablePeerArithmetic {
    multiple: Decimal,
    weight: Decimal,
    contribution: Decimal,
}

impl ComparablePeerArithmetic {
    /// Exact peer value divided by its selected metric.
    pub const fn multiple(self) -> Decimal {
        self.multiple
    }
    /// Exact parts-per-million weight represented as a decimal.
    pub const fn weight(self) -> Decimal {
        self.weight
    }
    /// Exact weighted multiple before final output rounding.
    pub const fn contribution(self) -> Decimal {
        self.contribution
    }
}

/// Shared comparable-method arithmetic, independent of accounting and study admission clocks.
///
/// Callers must authenticate their inputs separately. This value contains no evidence receipt,
/// historical qualification, or approval and cannot construct an accounting valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparableValueArithmetic {
    peers: Box<[ComparablePeerArithmetic]>,
    weighted_multiple: Decimal,
    raw_value: Decimal,
    lower: Decimal,
    upper: Decimal,
}

impl ComparableValueArithmetic {
    /// Evaluates `(peer value, peer metric, weight ppm)` in the supplied deterministic order.
    pub fn calculate(
        subject_metric: Decimal,
        peers: &[(Decimal, Decimal, u32)],
    ) -> Result<Self, AutomaticValuationError> {
        if peers.is_empty() || peers.len() > MAX_COMPARABLES {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::MethodInput,
            ));
        }
        let mut calculations = reserved_vec(peers.len())?;
        let mut weighted_multiple = Decimal::ZERO;
        let mut weight_sum = 0_u32;
        let mut lower: Option<Decimal> = None;
        let mut upper: Option<Decimal> = None;
        for &(value, metric, weight_ppm) in peers {
            if weight_ppm == 0 || weight_ppm > PROBABILITY_PARTS_PER_MILLION {
                return Err(AutomaticValuationError::Conflict(
                    AutomaticValuationConflict::PeerSet,
                ));
            }
            if metric == Decimal::ZERO {
                return Err(AutomaticValuationError::Unavailable(
                    AutomaticValuationUnavailable::MethodInput,
                ));
            }
            let weight = probability_decimal(weight_ppm)?;
            let multiple = value
                .checked_div(metric)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            let contribution = multiple
                .checked_mul(weight)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            weighted_multiple = weighted_multiple
                .checked_add(contribution)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            weight_sum = weight_sum
                .checked_add(weight_ppm)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            let implied = subject_metric
                .checked_mul(multiple)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            lower = Some(lower.map_or(implied, |prior| prior.min(implied)));
            upper = Some(upper.map_or(implied, |prior| prior.max(implied)));
            calculations.push(ComparablePeerArithmetic {
                multiple,
                weight,
                contribution,
            });
        }
        if weight_sum != PROBABILITY_PARTS_PER_MILLION {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::ProbabilityMass,
            ));
        }
        Ok(Self {
            peers: calculations.into_boxed_slice(),
            weighted_multiple,
            raw_value: subject_metric
                .checked_mul(weighted_multiple)
                .ok_or(AutomaticValuationError::Arithmetic)?,
            lower: lower.ok_or(AutomaticValuationError::InvalidContract)?,
            upper: upper.ok_or(AutomaticValuationError::InvalidContract)?,
        })
    }

    /// Original ordered peer arithmetic for retained method intermediates.
    pub fn peers(&self) -> &[ComparablePeerArithmetic] {
        &self.peers
    }
    /// Exact sum of the peer contributions.
    pub const fn weighted_multiple(&self) -> Decimal {
        self.weighted_multiple
    }
    /// Exact subject metric multiplied by the weighted peer multiple.
    pub const fn raw_value(&self) -> Decimal {
        self.raw_value
    }
    /// Smallest observed peer-implied subject value, before outward rounding.
    pub const fn lower(&self) -> Decimal {
        self.lower
    }
    /// Largest observed peer-implied subject value, before outward rounding.
    pub const fn upper(&self) -> Decimal {
        self.upper
    }
}

/// One explicitly forecast residual-income period.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidualIncomePeriod {
    /// One-based period offset.
    pub period: NonZeroU32,
    /// Explicit forecast net income.
    pub net_income: PointInTimeValuationInput,
    /// Explicit opening book value used for the equity charge.
    pub opening_book_value: PointInTimeValuationInput,
}

/// Complete residual-income request.
#[derive(Debug)]
pub struct ResidualIncomeValuationRequest {
    /// Exact count of equal financial periods per year; the supplied rate is per such period.
    /// Annual cash flows and annual rates use one, without assuming a fixed civil-year duration.
    pub periods_per_year: NonZeroU32,
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Exact current book value.
    pub current_book_value: PointInTimeValuationInput,
    /// Explicit forecast periods; the calculator performs no forecasting.
    pub periods: Vec<ResidualIncomePeriod>,
    /// Exact annual government reference and independently evidenced equity risk premium.
    pub macro_assumptions: FinancialModelMacroAssumptions,
    /// Evidence-bound uncertainty bounds.
    pub uncertainty: AutomaticValuationUncertainty,
}

/// One explicit forecast outcome and probability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastDistributionPoint {
    /// Caller-owned stable outcome identity.
    pub point_id: Box<str>,
    /// Explicit producer-derived terminal value.
    pub terminal_value: PointInTimeValuationInput,
    /// Exact terminal instant asserted by the producer evidence.
    pub terminal_at: Timestamp,
    /// Explicit nonzero probability in parts per million.
    pub probability_ppm: u32,
    /// Evidence receipt whose decimal value must equal the exact probability.
    pub probability_assumption: AutomaticValuationAssumption,
}

/// Complete forecast-distribution request.
#[derive(Debug)]
pub struct ForecastDistributionValuationRequest {
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Explicit positive forecast horizon.
    pub horizon_nanos: NonZeroU64,
    /// Explicit probability distribution; calibration intervals are not accepted.
    pub points: Vec<ForecastDistributionPoint>,
    /// Exact upstream forecast-selection receipt.
    pub forecast_selection_receipt: EvidenceDigest,
    /// Evidence-bound uncertainty bounds.
    pub uncertainty: AutomaticValuationUncertainty,
}

/// Closed material intermediate-calculation role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutomaticValuationIntermediateKind {
    /// One discounted explicit DCF cash flow.
    DiscountedCashFlow,
    /// The discounted explicit DCF terminal value.
    DiscountedTerminalValue,
    /// One peer's explicit weight applied to its calculated multiple.
    WeightedComparableMultiple,
    /// The weighted peer multiple applied to the subject metric.
    ComparableSubjectValue,
    /// One discounted residual-income contribution.
    DiscountedResidualIncome,
    /// One explicit probability-weighted forecast outcome.
    ProbabilityWeightedForecast,
}

/// Material operands and result for one deterministic method step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationIntermediate {
    kind: AutomaticValuationIntermediateKind,
    sequence: u32,
    instrument_id: InstrumentId,
    primary_input: InputId,
    secondary_input: Option<InputId>,
    amount: Decimal,
    adjustment: Decimal,
    factor: Decimal,
    result: Decimal,
    evidence: EvidenceDigest,
}

impl AutomaticValuationIntermediate {
    /// Restores material operands for complete receipt recovery.
    ///
    /// This does not admit a calculation. The enclosing receipt must recompute every operand,
    /// intermediate result, output amount, and canonical identity before accepting this value.
    #[allow(
        clippy::too_many_arguments,
        reason = "durable arithmetic operands remain explicit"
    )]
    pub(crate) fn try_recover(
        kind: AutomaticValuationIntermediateKind,
        sequence: u32,
        instrument_id: InstrumentId,
        primary_input: InputId,
        secondary_input: Option<InputId>,
        amount: Decimal,
        adjustment: Decimal,
        factor: Decimal,
        result: Decimal,
        evidence: EvidenceDigest,
    ) -> Result<Self, AutomaticValuationError> {
        if sequence == 0 || !valid_sha256(evidence) {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(intermediate(
            kind,
            sequence,
            instrument_id,
            primary_input,
            secondary_input,
            amount,
            adjustment,
            factor,
            result,
            evidence,
        ))
    }
    /// Returns the method-specific calculation role.
    pub const fn kind(&self) -> AutomaticValuationIntermediateKind {
        self.kind
    }
    /// Returns the period or stable one-based method order.
    pub const fn sequence(&self) -> u32 {
        self.sequence
    }
    /// Returns the security whose inputs produced this step.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the primary immutable valuation input.
    pub const fn primary_input(&self) -> InputId {
        self.primary_input
    }
    /// Returns the optional second immutable valuation input.
    pub const fn secondary_input(&self) -> Option<InputId> {
        self.secondary_input
    }
    /// Returns the primary exact amount operand.
    pub const fn amount(&self) -> Decimal {
        self.amount
    }
    /// Returns the exact calculated adjustment or multiple.
    pub const fn adjustment(&self) -> Decimal {
        self.adjustment
    }
    /// Returns the exact discount divisor, probability, or peer weight.
    pub const fn factor(&self) -> Decimal {
        self.factor
    }
    /// Returns the exact contribution produced by this step.
    pub const fn result(&self) -> Decimal {
        self.result
    }
    /// Returns the method-specific supporting receipt identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }
}

/// Exact inclusive output range in one currency, scale, and economic unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutomaticValuationRange {
    lower: ValuationAmount,
    central: ValuationAmount,
    upper: ValuationAmount,
}

impl AutomaticValuationRange {
    /// Restores an ordered, same-currency, same-scale result in one economic unit.
    pub fn try_new(
        lower: ValuationAmount,
        central: ValuationAmount,
        upper: ValuationAmount,
    ) -> Result<Self, AutomaticValuationError> {
        if [lower, upper].into_iter().any(|amount| {
            amount.money().currency() != central.money().currency()
                || amount.basis() != central.basis()
                || amount.scale() != central.scale()
        }) || lower.money().amount() > central.money().amount()
            || central.money().amount() > upper.money().amount()
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            lower,
            central,
            upper,
        })
    }
    /// Returns the evidence-bound lower value.
    pub const fn lower(self) -> ValuationAmount {
        self.lower
    }
    /// Returns the calculated central value.
    pub const fn central(self) -> ValuationAmount {
        self.central
    }
    /// Returns the evidence-bound upper value.
    pub const fn upper(self) -> ValuationAmount {
        self.upper
    }
}

/// Immutable, evidence-closed research calculation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationMethodReceipt {
    id: AutomaticValuationIdentity,
    input_set_id: AutomaticValuationInputSetIdentity,
    method: AutomaticValuationMethod,
    periods_per_year: Option<NonZeroU32>,
    account_id: AccountId,
    instrument_id: InstrumentId,
    company_security: CompanySecurityIdentitySelectionReceipt,
    peer_identities: Box<[CompanySecurityIdentitySelectionReceipt]>,
    rights_decision: ResearchUseDecisionDigest,
    rights_graph: ResearchUseGraphDigest,
    rights_input_digest: EvidenceDigest,
    admitted_event_inputs: Box<[ValuationEventRightsAdmission]>,
    rights_expires_at: Timestamp,
    admitted_input_manifests: Box<[DatasetManifestRef]>,
    current_market_input: InputId,
    method_base_input: Option<InputId>,
    inputs: Box<[PointInTimeValuationInput]>,
    assumptions: Box<[AutomaticValuationAssumption]>,
    macro_assumptions: Option<FinancialModelMacroAssumptions>,
    residual_terminal: Option<ResidualIncomeTerminalReceipt>,
    intermediates: Box<[AutomaticValuationIntermediate]>,
    range: AutomaticValuationRange,
    arithmetic_policy: ValuationArithmeticPolicy,
    method_selection_receipt: Option<EvidenceDigest>,
    forecast_horizon_nanos: Option<NonZeroU64>,
    forecast_terminal_at: Option<Timestamp>,
    measurement_at: Timestamp,
    calculated_at: Timestamp,
    calculated_by: ActorId,
    expires_at: Timestamp,
}

/// Complete untrusted persisted calculation, accepted only after deterministic recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AutomaticValuationRecoveryInput {
    /// Expected complete calculation commitment.
    pub expected_id: AutomaticValuationIdentity,
    /// Expected exact input-set commitment.
    pub expected_input_set_id: AutomaticValuationInputSetIdentity,
    /// Closed calculation method.
    pub method: AutomaticValuationMethod,
    /// Explicit financial-period frequency for discounting methods.
    pub periods_per_year: Option<NonZeroU32>,
    /// Exact reporting account.
    pub account_id: AccountId,
    /// Exact valued instrument.
    pub instrument_id: InstrumentId,
    /// Full subject identity receipt, decoded by its source authority.
    pub company_security: CompanySecurityIdentitySelectionReceipt,
    /// Full ordered peer identity receipts.
    pub peer_identities: Box<[CompanySecurityIdentitySelectionReceipt]>,
    /// Original research-use authorization decision.
    pub rights_decision: ResearchUseDecisionDigest,
    /// Original transitive source graph.
    pub rights_graph: ResearchUseGraphDigest,
    /// Combined exact physical and logical input identity.
    pub rights_input_digest: EvidenceDigest,
    /// Original event-use admissions; recovery does not grant renewed use.
    pub admitted_event_inputs: Box<[ValuationEventRightsAdmission]>,
    /// Original exclusive authorization expiry.
    pub rights_expires_at: Timestamp,
    /// Exact input manifests admitted against the authentic authorization graph.
    pub admitted_input_manifests: Box<[DatasetManifestRef]>,
    /// Original selected market input.
    pub current_market_input: InputId,
    /// Explicit subject metric or current book value when required.
    pub method_base_input: Option<InputId>,
    /// Complete source-authority-decoded input receipts.
    pub inputs: Box<[PointInTimeValuationInput]>,
    /// Complete economic and uncertainty assumptions.
    pub assumptions: Box<[AutomaticValuationAssumption]>,
    /// Exact source reference and premium consumed by annual discounting methods.
    pub macro_assumptions: Option<FinancialModelMacroAssumptions>,
    /// Exact conditional terminal interpretation, required only for residual income.
    pub residual_terminal: Option<ResidualIncomeTerminalReceipt>,
    /// Complete method intermediates, still untrusted until recomputed.
    pub intermediates: Box<[AutomaticValuationIntermediate]>,
    /// Claimed output amounts, still untrusted until recomputed.
    pub range: AutomaticValuationRange,
    /// Exact admitted arithmetic policy.
    pub arithmetic_policy: ValuationArithmeticPolicy,
    /// Exact upstream method selector when required.
    pub method_selection_receipt: Option<EvidenceDigest>,
    /// Exact forecast-distribution horizon when required.
    pub forecast_horizon_nanos: Option<NonZeroU64>,
    /// Exact forecast-distribution terminal instant when required.
    pub forecast_terminal_at: Option<Timestamp>,
    /// Original analytical source cutoff.
    pub measurement_at: Timestamp,
    /// Original calculation completion.
    pub calculated_at: Timestamp,
    /// Original calculation actor.
    pub calculated_by: ActorId,
    /// Exclusive result expiry.
    pub expires_at: Timestamp,
}

impl AutomaticValuationMethodReceipt {
    /// Recomputes one persisted receipt without issuing or spending research-use authority.
    pub(crate) fn try_recover(
        input: AutomaticValuationRecoveryInput,
    ) -> Result<Self, AutomaticValuationError> {
        let value = Self {
            id: input.expected_id,
            input_set_id: input.expected_input_set_id,
            method: input.method,
            periods_per_year: input.periods_per_year,
            account_id: input.account_id,
            instrument_id: input.instrument_id,
            company_security: input.company_security,
            peer_identities: input.peer_identities,
            rights_decision: input.rights_decision,
            rights_graph: input.rights_graph,
            rights_input_digest: input.rights_input_digest,
            admitted_event_inputs: input.admitted_event_inputs,
            rights_expires_at: input.rights_expires_at,
            admitted_input_manifests: input.admitted_input_manifests,
            current_market_input: input.current_market_input,
            method_base_input: input.method_base_input,
            inputs: input.inputs,
            assumptions: input.assumptions,
            macro_assumptions: input.macro_assumptions,
            residual_terminal: input.residual_terminal,
            intermediates: input.intermediates,
            range: input.range,
            arithmetic_policy: input.arithmetic_policy,
            method_selection_receipt: input.method_selection_receipt,
            forecast_horizon_nanos: input.forecast_horizon_nanos,
            forecast_terminal_at: input.forecast_terminal_at,
            measurement_at: input.measurement_at,
            calculated_at: input.calculated_at,
            calculated_by: input.calculated_by,
            expires_at: input.expires_at,
        };
        verify_recovered_receipt(&value)?;
        if input_set_identity(&value.inputs)? != value.input_set_id
            || receipt_identity(&value)? != value.id
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        Ok(value)
    }

    /// Exact conditional finite-horizon interpretation and terminal sensitivity.
    pub const fn residual_terminal(&self) -> Option<&ResidualIncomeTerminalReceipt> {
        self.residual_terminal.as_ref()
    }

    /// Charges the complete retained receipt and every dynamic source/identity payload.
    pub(crate) fn retained_bytes(&self) -> Result<usize, crate::FairValueError> {
        use std::mem::{size_of, size_of_val};
        let mut total = crate::checked_add(size_of::<Self>(), self.calculated_by.retained_bytes())?;
        total = crate::checked_add(
            total,
            company_receipt_dynamic_bytes(&self.company_security)?,
        )?;
        total = crate::checked_add(total, size_of_val(&*self.peer_identities))?;
        for peer in &self.peer_identities {
            total = crate::checked_add(total, company_receipt_dynamic_bytes(peer)?)?;
        }
        total = crate::checked_add(total, size_of_val(&*self.admitted_input_manifests))?;
        for manifest in &self.admitted_input_manifests {
            total = crate::checked_add(total, crate::evidence::manifest_retained_bytes(manifest)?)?;
        }
        total = crate::checked_add(total, size_of_val(&*self.admitted_event_inputs))?;
        for admission in &self.admitted_event_inputs {
            total = crate::checked_add(
                total,
                crate::evidence::market_event_commit_retained_bytes(&admission.commit)?,
            )?;
            total = crate::checked_add(total, size_of_val(&*admission.inputs))?;
            for input in &admission.inputs {
                total = crate::checked_add(total, input.source_id().as_str().len())?;
            }
        }
        total = crate::checked_add(total, size_of_val(&*self.inputs))?;
        for input in &self.inputs {
            total = crate::checked_add(
                total,
                input
                    .input()
                    .retained_bytes()
                    .checked_sub(size_of::<ValuationInput>())
                    .ok_or(crate::FairValueError::Arithmetic)?,
            )?;
        }
        total = crate::checked_add(total, size_of_val(&*self.assumptions))?;
        for assumption in &self.assumptions {
            total = crate::checked_add(total, assumption.identifier().len())?;
        }
        if let Some(binding) = &self.macro_assumptions {
            total = crate::checked_add(total, binding.premium().identifier().len())?;
            total = crate::checked_add(total, binding.assumption().identifier().len())?;
            total = crate::checked_add(
                total,
                binding.premium_source_reference().map_or(0, <[u8]>::len),
            )?;
            total = crate::checked_add(total, size_of_val(binding.premium_parent_manifests()))?;
            for manifest in binding.premium_parent_manifests() {
                total =
                    crate::checked_add(total, crate::evidence::manifest_retained_bytes(manifest)?)?;
            }
        }
        crate::checked_add(total, size_of_val(&*self.intermediates))
    }
    /// Returns the complete calculation identity.
    pub const fn id(&self) -> AutomaticValuationIdentity {
        self.id
    }
    /// Returns the exact point-in-time input-set identity.
    pub const fn input_set_id(&self) -> AutomaticValuationInputSetIdentity {
        self.input_set_id
    }
    /// Returns the exact calculation method.
    pub const fn method(&self) -> AutomaticValuationMethod {
        self.method
    }
    /// Returns the model's explicit financial-period frequency, absent for non-discounting methods.
    pub const fn periods_per_year(&self) -> Option<NonZeroU32> {
        self.periods_per_year
    }
    /// Returns the exact reporting account.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }
    /// Returns the exact subject security.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the complete subject company/security selection receipt.
    pub const fn company_security(&self) -> &CompanySecurityIdentitySelectionReceipt {
        &self.company_security
    }
    /// Returns complete explicit peer identity receipts, empty for non-peer methods.
    pub fn peer_identities(&self) -> &[CompanySecurityIdentitySelectionReceipt] {
        &self.peer_identities
    }
    /// Returns the authorized research-use decision identity.
    pub const fn rights_decision(&self) -> ResearchUseDecisionDigest {
        self.rights_decision
    }
    /// Returns the authorized exact transitive graph identity.
    pub const fn rights_graph(&self) -> ResearchUseGraphDigest {
        self.rights_graph
    }
    /// Returns the combined exact physical and logical rights input identity.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.rights_input_digest
    }
    /// Returns exact original event admissions, requiring fresh grants for subsequent use.
    pub fn admitted_event_inputs(&self) -> &[ValuationEventRightsAdmission] {
        &self.admitted_event_inputs
    }
    /// Returns the original exclusive research authorization lifetime.
    pub const fn rights_expires_at(&self) -> Timestamp {
        self.rights_expires_at
    }
    /// Returns the exact canonical set of input manifests admitted by the source graph.
    pub fn admitted_input_manifests(&self) -> &[DatasetManifestRef] {
        &self.admitted_input_manifests
    }
    /// Returns the explicit subject metric or opening book input when the method needs one.
    pub const fn method_base_input(&self) -> Option<InputId> {
        self.method_base_input
    }
    /// Returns the exact current-market input identity.
    pub const fn current_market_input(&self) -> InputId {
        self.current_market_input
    }
    /// Returns all complete producer and point-in-time input receipts.
    pub fn inputs(&self) -> &[PointInTimeValuationInput] {
        &self.inputs
    }
    /// Returns all explicit economic and uncertainty assumptions.
    pub fn assumptions(&self) -> &[AutomaticValuationAssumption] {
        &self.assumptions
    }
    /// Returns the exact retained reference, premium and rate used by a discounting method.
    pub const fn macro_assumptions(&self) -> Option<&FinancialModelMacroAssumptions> {
        self.macro_assumptions.as_ref()
    }
    /// Returns every material intermediate calculation in method order.
    pub fn intermediates(&self) -> &[AutomaticValuationIntermediate] {
        &self.intermediates
    }
    /// Returns the exact result range and central value.
    pub const fn range(&self) -> AutomaticValuationRange {
        self.range
    }
    /// Returns the explicit arithmetic policy.
    pub const fn arithmetic_policy(&self) -> ValuationArithmeticPolicy {
        self.arithmetic_policy
    }
    /// Returns an exact method-selection receipt when the method requires one.
    pub const fn method_selection_receipt(&self) -> Option<EvidenceDigest> {
        self.method_selection_receipt
    }
    /// Returns the explicit forecast horizon for forecast-distribution calculations.
    pub const fn forecast_horizon_nanos(&self) -> Option<NonZeroU64> {
        self.forecast_horizon_nanos
    }
    /// Returns the exact terminal instant shared by every forecast outcome.
    pub const fn forecast_terminal_at(&self) -> Option<Timestamp> {
        self.forecast_terminal_at
    }
    /// Returns the point-in-time valuation cutoff.
    pub const fn measurement_at(&self) -> Timestamp {
        self.measurement_at
    }
    /// Returns calculation completion time.
    pub const fn calculated_at(&self) -> Timestamp {
        self.calculated_at
    }
    /// Returns the calculation actor, which is not an approver.
    pub const fn calculated_by(&self) -> &ActorId {
        &self.calculated_by
    }
    /// Returns exclusive result expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Completed research-only calculation.
///
/// This value intentionally exposes no conversion to a fair-value measurement and grants no
/// classification, approval, recommendation, selection, or execution authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutomaticValuationCalculation {
    receipt: AutomaticValuationMethodReceipt,
}

impl AutomaticValuationCalculation {
    /// Seals the actual completion clock after the bounded calculation has returned.
    ///
    /// Source cutoffs stay unchanged. The producer must call this before publication so the
    /// result never claims to have existed at the earlier request or source-selection time.
    pub fn completed_at(
        mut self,
        completed_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        if completed_at < self.receipt.calculated_at || completed_at >= self.receipt.expires_at {
            return Err(AutomaticValuationError::InvalidContract);
        }
        self.receipt.calculated_at = completed_at;
        verify_recovered_receipt(&self.receipt)?;
        self.receipt.id = receipt_identity(&self.receipt)?;
        Ok(self)
    }

    /// Returns the exact calculated central amount.
    pub const fn amount(&self) -> ValuationAmount {
        self.receipt.range.central
    }
    /// Returns the complete evidence-closed calculation receipt.
    pub const fn receipt(&self) -> &AutomaticValuationMethodReceipt {
        &self.receipt
    }
    /// Consumes the calculation into its receipt.
    pub fn into_receipt(self) -> AutomaticValuationMethodReceipt {
        self.receipt
    }
}

/// Calculates a DCF from explicit cash flows, terminal value, rate, and evidence.
pub fn calculate_discounted_cash_flow(
    mut request: DiscountedCashFlowValuationRequest,
) -> Result<AutomaticValuationCalculation, AutomaticValuationError> {
    validate_common(&request.common)?;
    validate_derived_assumption(
        request.macro_assumptions.assumption(),
        AutomaticValuationAssumptionKind::DiscountRate,
        &request.common,
    )?;
    if request.cash_flows.is_empty()
        || request.cash_flows.len()
            > MAX_DCF_PERIODS.min(request.common.arithmetic_policy.maximum_periods())
        || usize::try_from(request.terminal_period.get())
            .map_err(|_| AutomaticValuationError::Arithmetic)?
            > request.common.arithmetic_policy.maximum_periods()
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    validate_method_input(
        &request.terminal_cash_flow,
        &request.common,
        request.common.instrument_id,
    )?;
    let terminal_offset = request
        .terminal_period
        .get()
        .checked_add(1)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let source_anchor = native_financial_input(
        &request.terminal_cash_flow,
        FinancialAmountRole::CommonEquityCashFlow,
        terminal_offset,
    )?;
    if source_anchor.identity != request.common.company_security.receipt_digest()
        || request.periods_per_year != NonZeroU32::MIN
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let discount_base = Decimal::ONE
        .checked_add(request.macro_assumptions.assumption().value())
        .ok_or(AutomaticValuationError::Arithmetic)?;
    if discount_base <= Decimal::ZERO {
        return Err(AutomaticValuationError::InvalidContract);
    }

    request.cash_flows.sort_by_key(|value| value.period);
    if request
        .cash_flows
        .windows(2)
        .any(|pair| pair[0].period == pair[1].period)
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }
    let method_capacity = request
        .cash_flows
        .len()
        .checked_add(1)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let mut raw_value = Decimal::ZERO;
    let mut inputs = reserved_vec(method_capacity)?;
    let mut intermediates = reserved_vec(method_capacity)?;
    for (index, value) in request.cash_flows.into_iter().enumerate() {
        if usize::try_from(value.period.get()).ok() != index.checked_add(1)
            || value.period > request.terminal_period
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        validate_method_input(
            &value.cash_flow,
            &request.common,
            request.common.instrument_id,
        )?;
        if native_financial_input(
            &value.cash_flow,
            FinancialAmountRole::CommonEquityCashFlow,
            value.period.get(),
        )? != source_anchor
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let amount = input_decimal(&value.cash_flow);
        let (divisor, present_value) = discount(amount, discount_base, value.period)?;
        raw_value = raw_value
            .checked_add(present_value)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        intermediates.push(intermediate(
            AutomaticValuationIntermediateKind::DiscountedCashFlow,
            value.period.get(),
            request.common.instrument_id,
            value.cash_flow.input().id(),
            None,
            amount,
            Decimal::ZERO,
            divisor,
            present_value,
            request.macro_assumptions.assumption().evidence(),
        ));
        inputs.push(value.cash_flow);
    }
    if intermediates.last().map(|step| step.sequence) != Some(request.terminal_period.get()) {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let growth = DcfTerminalGrowthPolicy::from_inputs(
        inputs
            .last()
            .ok_or(AutomaticValuationError::InvalidContract)?,
        &request.terminal_cash_flow,
        request.terminal_period,
        &request.macro_assumptions,
        request.common.calculated_at,
        request.common.expires_at,
    )?;
    validate_derived_assumption(
        growth.assumption(),
        AutomaticValuationAssumptionKind::TerminalGrowth,
        &request.common,
    )?;
    let terminal_amount = input_decimal(&request.terminal_cash_flow);
    let (terminal_divisor, terminal_present_value) = terminal_fcfe_discount(
        terminal_amount,
        request.macro_assumptions.assumption().value(),
        growth.assumption().value(),
        request.terminal_period,
    )?;
    raw_value = raw_value
        .checked_add(terminal_present_value)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    intermediates.push(intermediate(
        AutomaticValuationIntermediateKind::DiscountedTerminalValue,
        request.terminal_period.get(),
        request.common.instrument_id,
        request.terminal_cash_flow.input().id(),
        Some(growth.final_explicit_input()),
        terminal_amount,
        growth.assumption().value(),
        terminal_divisor,
        terminal_present_value,
        request.macro_assumptions.assumption().evidence(),
    ));
    inputs.push(request.terminal_cash_flow);

    let mut assumptions = reserved_vec(2)?;
    assumptions.push(request.macro_assumptions.assumption().clone());
    assumptions.push(growth.assumption().clone());
    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::DiscountedCashFlow,
        macro_assumptions: Some(request.macro_assumptions),
        residual_terminal: None,
        periods_per_year: Some(request.periods_per_year),
        method_base_input: None,
        raw_value,
        uncertainty: request.uncertainty,
        assumptions,
        inputs,
        intermediates,
        peer_identities: Vec::new(),
        method_selection_receipt: None,
        forecast_horizon_nanos: None,
        forecast_terminal_at: None,
    })
}

/// Calculates a comparable-company value from an explicit peer set and exact weights.
pub fn calculate_comparable_companies(
    mut request: ComparableCompaniesValuationRequest,
) -> Result<AutomaticValuationCalculation, AutomaticValuationError> {
    validate_common(&request.common)?;
    validate_method_input(
        &request.subject_metric,
        &request.common,
        request.common.instrument_id,
    )?;
    if request.comparables.is_empty() || request.comparables.len() > MAX_COMPARABLES {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    request
        .comparables
        .sort_by_key(|comparable| comparable.instrument_id);
    if request.comparables.windows(2).any(|pair| {
        pair[0].instrument_id == pair[1].instrument_id
            || pair[0].instrument_id == request.common.instrument_id
    }) || request
        .comparables
        .last()
        .is_some_and(|value| value.instrument_id == request.common.instrument_id)
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::PeerSet,
        ));
    }

    let comparable_count = request.comparables.len();
    let input_capacity = comparable_count
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let intermediate_capacity = comparable_count
        .checked_add(1)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let mut arithmetic_inputs = reserved_vec(comparable_count)?;
    for comparable in &request.comparables {
        arithmetic_inputs.push((
            input_decimal(&comparable.value),
            input_decimal(&comparable.metric),
            comparable.weight_ppm,
        ));
    }
    let subject_metric = input_decimal(&request.subject_metric);
    let arithmetic = ComparableValueArithmetic::calculate(subject_metric, &arithmetic_inputs)?;
    let mut inputs = reserved_vec(input_capacity)?;
    let mut assumptions = reserved_vec(comparable_count)?;
    let mut intermediates = reserved_vec(intermediate_capacity)?;
    let mut peer_identities = reserved_vec(comparable_count)?;
    for (index, comparable) in request.comparables.into_iter().enumerate() {
        validate_company_security(
            &comparable.company_security,
            comparable.instrument_id,
            request.common.measurement_at,
            request.common.expires_at,
        )?;
        validate_method_input(
            &comparable.metric,
            &request.common,
            comparable.instrument_id,
        )?;
        validate_method_input(&comparable.value, &request.common, comparable.instrument_id)?;
        validate_derived_assumption(
            &comparable.weight_assumption,
            AutomaticValuationAssumptionKind::ComparableWeight,
            &request.common,
        )?;
        if comparable.instrument_id == request.common.instrument_id {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::PeerSet,
            ));
        }
        let peer_arithmetic = arithmetic.peers()[index];
        let weight = peer_arithmetic.weight();
        if comparable.weight_assumption.value() != weight {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let peer_value = input_decimal(&comparable.value);
        let multiple = peer_arithmetic.multiple();
        let contribution = peer_arithmetic.contribution();
        intermediates.push(intermediate(
            AutomaticValuationIntermediateKind::WeightedComparableMultiple,
            u32::try_from(
                index
                    .checked_add(1)
                    .ok_or(AutomaticValuationError::Arithmetic)?,
            )
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
            comparable.instrument_id,
            comparable.value.input().id(),
            Some(comparable.metric.input().id()),
            peer_value,
            multiple,
            weight,
            contribution,
            comparable.weight_assumption.evidence(),
        ));
        inputs.push(comparable.metric);
        inputs.push(comparable.value);
        assumptions.push(comparable.weight_assumption);
        peer_identities.push(comparable.company_security);
    }
    let weighted_multiple = arithmetic.weighted_multiple();
    let raw_value = arithmetic.raw_value();
    intermediates.push(intermediate(
        AutomaticValuationIntermediateKind::ComparableSubjectValue,
        u32::try_from(
            intermediates
                .len()
                .checked_add(1)
                .ok_or(AutomaticValuationError::Arithmetic)?,
        )
        .map_err(|_| AutomaticValuationError::Arithmetic)?,
        request.common.instrument_id,
        request.subject_metric.input().id(),
        None,
        subject_metric,
        weighted_multiple,
        Decimal::ONE,
        raw_value,
        request.common.company_security.receipt_digest(),
    ));
    let method_base_input = Some(request.subject_metric.input().id());
    inputs.push(request.subject_metric);

    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ComparableCompanies,
        macro_assumptions: None,
        residual_terminal: None,
        periods_per_year: None,
        method_base_input,
        raw_value,
        uncertainty: request.uncertainty,
        assumptions,
        inputs,
        intermediates,
        peer_identities,
        method_selection_receipt: None,
        forecast_horizon_nanos: None,
        forecast_terminal_at: None,
    })
}

/// Calculates residual income from explicit forecasts and an explicit cost of equity.
pub fn calculate_residual_income(
    mut request: ResidualIncomeValuationRequest,
) -> Result<AutomaticValuationCalculation, AutomaticValuationError> {
    validate_common(&request.common)?;
    validate_method_input(
        &request.current_book_value,
        &request.common,
        request.common.instrument_id,
    )?;
    let source_anchor = native_financial_input(
        &request.current_book_value,
        FinancialAmountRole::CommonBookEquity,
        0,
    )?;
    if source_anchor.identity != request.common.company_security.receipt_digest()
        || request.periods_per_year != NonZeroU32::MIN
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    validate_derived_assumption(
        request.macro_assumptions.assumption(),
        AutomaticValuationAssumptionKind::CostOfEquity,
        &request.common,
    )?;
    if request.periods.is_empty()
        || request.periods.len()
            > MAX_RESIDUAL_PERIODS.min(request.common.arithmetic_policy.maximum_periods())
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    let discount_base = Decimal::ONE
        .checked_add(request.macro_assumptions.assumption().value())
        .ok_or(AutomaticValuationError::Arithmetic)?;
    if discount_base <= Decimal::ZERO {
        return Err(AutomaticValuationError::InvalidContract);
    }
    request.periods.sort_by_key(|value| value.period);
    if request
        .periods
        .windows(2)
        .any(|pair| pair[0].period == pair[1].period)
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }

    let final_period = request
        .periods
        .last()
        .ok_or(AutomaticValuationError::InvalidContract)?;
    let residual_terminal = ResidualIncomeTerminalReceipt::from_inputs(
        &request.current_book_value,
        &final_period.net_income,
        &final_period.opening_book_value,
        final_period.period,
        request.macro_assumptions.assumption(),
    )?;
    let period_count = request.periods.len();
    let input_capacity = period_count
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let mut raw_value = input_decimal(&request.current_book_value);
    let mut inputs = reserved_vec(input_capacity)?;
    let mut intermediates = reserved_vec(period_count)?;
    for (index, period) in request.periods.into_iter().enumerate() {
        if usize::try_from(period.period.get()).ok() != index.checked_add(1) {
            return Err(AutomaticValuationError::InvalidContract);
        }
        if usize::try_from(period.period.get()).map_err(|_| AutomaticValuationError::Arithmetic)?
            > request.common.arithmetic_policy.maximum_periods()
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        validate_method_input(
            &period.net_income,
            &request.common,
            request.common.instrument_id,
        )?;
        validate_method_input(
            &period.opening_book_value,
            &request.common,
            request.common.instrument_id,
        )?;
        let income_anchor = native_financial_input(
            &period.net_income,
            FinancialAmountRole::CommonNetIncome,
            period.period.get(),
        )?;
        let opening_anchor = native_financial_input(
            &period.opening_book_value,
            FinancialAmountRole::CommonBookEquity,
            period
                .period
                .get()
                .checked_sub(1)
                .ok_or(AutomaticValuationError::Arithmetic)?,
        )?;
        if income_anchor != source_anchor
            || opening_anchor != source_anchor
            || (period.period.get() == 1
                && period.opening_book_value.input().id()
                    != request.current_book_value.input().id())
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let net_income = input_decimal(&period.net_income);
        let equity_charge = input_decimal(&period.opening_book_value)
            .checked_mul(request.macro_assumptions.assumption().value())
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let residual_income = net_income
            .checked_sub(equity_charge)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let (divisor, present_value) = discount(residual_income, discount_base, period.period)?;
        raw_value = raw_value
            .checked_add(present_value)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        intermediates.push(intermediate(
            AutomaticValuationIntermediateKind::DiscountedResidualIncome,
            period.period.get(),
            request.common.instrument_id,
            period.net_income.input().id(),
            Some(period.opening_book_value.input().id()),
            net_income,
            equity_charge,
            divisor,
            present_value,
            request.macro_assumptions.assumption().evidence(),
        ));
        inputs.push(period.net_income);
        inputs.push(period.opening_book_value);
    }
    let method_base_input = Some(request.current_book_value.input().id());
    inputs.push(request.current_book_value);

    let assumptions = single_vec(request.macro_assumptions.assumption().clone())?;
    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ResidualIncome,
        macro_assumptions: Some(request.macro_assumptions),
        residual_terminal: Some(residual_terminal),
        periods_per_year: Some(request.periods_per_year),
        method_base_input,
        raw_value,
        uncertainty: request.uncertainty,
        assumptions,
        inputs,
        intermediates,
        peer_identities: Vec::new(),
        method_selection_receipt: None,
        forecast_horizon_nanos: None,
        forecast_terminal_at: None,
    })
}

/// One probability-weighted outcome, without source or valuation admission authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastOutcomeArithmetic {
    probability: Decimal,
    contribution: Decimal,
}

impl ForecastOutcomeArithmetic {
    /// Exact normalized probability of this outcome.
    pub const fn probability(self) -> Decimal {
        self.probability
    }
    /// Terminal amount multiplied by its probability.
    pub const fn contribution(self) -> Decimal {
        self.contribution
    }
}

/// Shared expectation arithmetic for live and study forecast valuations.
///
/// This numeric result authenticates no input, source, historical basis, or approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastValueArithmetic {
    outcomes: Box<[ForecastOutcomeArithmetic]>,
    raw_value: Decimal,
    lower: Decimal,
    upper: Decimal,
}

impl ForecastValueArithmetic {
    /// Evaluates positive terminal amounts and exact parts-per-million masses in input order.
    pub fn calculate(points: &[(Decimal, u32)]) -> Result<Self, AutomaticValuationError> {
        if points.is_empty() || points.len() > MAX_FORECAST_POINTS {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::MethodInput,
            ));
        }
        let mut outcomes = reserved_vec(points.len())?;
        let mut raw_value = Decimal::ZERO;
        let mut mass = 0_u32;
        let mut lower: Option<Decimal> = None;
        let mut upper: Option<Decimal> = None;
        for &(amount, probability_ppm) in points {
            if amount <= Decimal::ZERO {
                return Err(AutomaticValuationError::Unavailable(
                    AutomaticValuationUnavailable::MethodInput,
                ));
            }
            if probability_ppm == 0 || probability_ppm > PROBABILITY_PARTS_PER_MILLION {
                return Err(AutomaticValuationError::Conflict(
                    AutomaticValuationConflict::ProbabilityMass,
                ));
            }
            let probability = probability_decimal(probability_ppm)?;
            let contribution = amount
                .checked_mul(probability)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            raw_value = raw_value
                .checked_add(contribution)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            mass = mass
                .checked_add(probability_ppm)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            lower = Some(lower.map_or(amount, |prior| prior.min(amount)));
            upper = Some(upper.map_or(amount, |prior| prior.max(amount)));
            outcomes.push(ForecastOutcomeArithmetic {
                probability,
                contribution,
            });
        }
        if mass != PROBABILITY_PARTS_PER_MILLION {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::ProbabilityMass,
            ));
        }
        Ok(Self {
            outcomes: outcomes.into_boxed_slice(),
            raw_value,
            lower: lower.ok_or(AutomaticValuationError::InvalidContract)?,
            upper: upper.ok_or(AutomaticValuationError::InvalidContract)?,
        })
    }

    /// Probability and contribution of each input in its original order.
    pub fn outcomes(&self) -> &[ForecastOutcomeArithmetic] {
        &self.outcomes
    }
    /// Exact probability-weighted terminal expectation, before final rounding.
    pub const fn raw_value(&self) -> Decimal {
        self.raw_value
    }
    /// Smallest support amount, before outward rounding.
    pub const fn lower(&self) -> Decimal {
        self.lower
    }
    /// Largest support amount, before outward rounding.
    pub const fn upper(&self) -> Decimal {
        self.upper
    }
}

/// Calculates an expectation from explicit terminal values and exact probability mass.
pub fn calculate_forecast_distribution(
    mut request: ForecastDistributionValuationRequest,
) -> Result<AutomaticValuationCalculation, AutomaticValuationError> {
    validate_common(&request.common)?;
    let horizon_nanos = i64::try_from(request.horizon_nanos.get())
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    let terminal_at = request
        .common
        .measurement_at
        .checked_add_nanos(horizon_nanos)
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    if request.points.is_empty()
        || request.points.len()
            > MAX_FORECAST_POINTS.min(request.common.arithmetic_policy.maximum_periods())
        || !valid_sha256(request.forecast_selection_receipt)
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    request
        .points
        .sort_by(|left, right| left.point_id.cmp(&right.point_id));
    if request
        .points
        .iter()
        .any(|point| !valid_identifier(&point.point_id))
        || request
            .points
            .windows(2)
            .any(|pair| pair[0].point_id == pair[1].point_id)
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }

    let point_count = request.points.len();
    let mut arithmetic_inputs = reserved_vec(point_count)?;
    arithmetic_inputs.extend(
        request
            .points
            .iter()
            .map(|point| (input_decimal(&point.terminal_value), point.probability_ppm)),
    );
    let arithmetic = ForecastValueArithmetic::calculate(&arithmetic_inputs)?;
    let mut inputs = reserved_vec(point_count)?;
    let mut assumptions = reserved_vec(point_count)?;
    let mut intermediates = reserved_vec(point_count)?;
    for (index, point) in request.points.into_iter().enumerate() {
        if point.terminal_at != terminal_at
            || point.terminal_value.input().evidence().effective_at() != Some(point.terminal_at)
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        validate_method_input(
            &point.terminal_value,
            &request.common,
            request.common.instrument_id,
        )?;
        let EvidenceOrigin::ForecastDistribution { evidence } =
            point.terminal_value.input().evidence().origin()
        else {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::MethodInput,
            ));
        };
        let source = evidence.source();
        let native = source
            .distribution()
            .points()
            .get(
                evidence
                    .ordinal()
                    .ok_or(AutomaticValuationError::InvalidContract)?,
            )
            .ok_or(AutomaticValuationError::InvalidContract)?;
        if source.reference().identity() != request.forecast_selection_receipt
            || source.distribution().points().len() != point_count
            || native.probability_ppm().get() != point.probability_ppm
            || point.probability_assumption.kind()
                != AutomaticValuationAssumptionKind::ForecastProbability
            || point.probability_assumption.evidence() != source.reference().identity()
            || point.probability_assumption.available_at() != source.distribution().published_at()
            || point.probability_assumption.available_at() > request.common.calculated_at
            || point.probability_assumption.expires_at() < request.common.expires_at
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        if point.probability_assumption.identifier() != &*point.point_id {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::ProbabilityMass,
            ));
        }
        let outcome = arithmetic.outcomes()[index];
        let probability = outcome.probability();
        if point.probability_assumption.value() != probability {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let terminal_value = input_decimal(&point.terminal_value);
        intermediates.push(intermediate(
            AutomaticValuationIntermediateKind::ProbabilityWeightedForecast,
            u32::try_from(
                index
                    .checked_add(1)
                    .ok_or(AutomaticValuationError::Arithmetic)?,
            )
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
            request.common.instrument_id,
            point.terminal_value.input().id(),
            None,
            terminal_value,
            Decimal::ZERO,
            probability,
            outcome.contribution(),
            point.probability_assumption.evidence(),
        ));
        inputs.push(point.terminal_value);
        assumptions.push(point.probability_assumption);
    }
    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ForecastDistribution,
        macro_assumptions: None,
        residual_terminal: None,
        periods_per_year: None,
        method_base_input: None,
        raw_value: arithmetic.raw_value(),
        uncertainty: request.uncertainty,
        assumptions,
        inputs,
        intermediates,
        peer_identities: Vec::new(),
        method_selection_receipt: Some(request.forecast_selection_receipt),
        forecast_horizon_nanos: Some(request.horizon_nanos),
        forecast_terminal_at: Some(terminal_at),
    })
}

struct FinishInput {
    macro_assumptions: Option<FinancialModelMacroAssumptions>,
    residual_terminal: Option<ResidualIncomeTerminalReceipt>,
    common: AutomaticValuationInput,
    method: AutomaticValuationMethod,
    periods_per_year: Option<NonZeroU32>,
    raw_value: Decimal,
    method_base_input: Option<InputId>,
    uncertainty: AutomaticValuationUncertainty,
    assumptions: Vec<AutomaticValuationAssumption>,
    inputs: Vec<PointInTimeValuationInput>,
    intermediates: Vec<AutomaticValuationIntermediate>,
    peer_identities: Vec<CompanySecurityIdentitySelectionReceipt>,
    method_selection_receipt: Option<EvidenceDigest>,
    forecast_horizon_nanos: Option<NonZeroU64>,
    forecast_terminal_at: Option<Timestamp>,
}

fn finish(
    mut request: FinishInput,
) -> Result<AutomaticValuationCalculation, AutomaticValuationError> {
    if request
        .assumptions
        .len()
        .checked_add(2)
        .is_none_or(|value| value > MAX_ASSUMPTIONS)
        || request
            .inputs
            .len()
            .checked_add(1)
            .is_none_or(|value| value > MAX_METHOD_INPUTS)
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    if request.method != AutomaticValuationMethod::ForecastDistribution {
        validate_derived_assumption(
            request.uncertainty.lower(),
            AutomaticValuationAssumptionKind::UncertaintyLower,
            &request.common,
        )?;
        validate_derived_assumption(
            request.uncertainty.upper(),
            AutomaticValuationAssumptionKind::UncertaintyUpper,
            &request.common,
        )?;
    }
    request
        .assumptions
        .try_reserve_exact(2)
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    request
        .inputs
        .try_reserve_exact(1)
        .map_err(|_| AutomaticValuationError::Arithmetic)?;

    let rounded = round(
        request.raw_value,
        request.common.output_scale,
        request.common.arithmetic_policy.rounding(),
    );
    let lower = request.uncertainty.lower().value();
    let upper = request.uncertainty.upper().value();
    if lower.scale() > u32::from(request.common.output_scale)
        || upper.scale() > u32::from(request.common.output_scale)
        || lower > rounded
        || rounded > upper
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Uncertainty,
        ));
    }
    let central = valuation_amount(&request.common, rounded)?;
    let range = AutomaticValuationRange {
        lower: valuation_amount(&request.common, lower)?,
        central,
        upper: valuation_amount(&request.common, upper)?,
    };

    request.assumptions.push(request.uncertainty.lower);
    request.assumptions.push(request.uncertainty.upper);
    request.assumptions.sort_by(|left, right| {
        left.kind()
            .cmp(&right.kind())
            .then_with(|| left.identifier().cmp(right.identifier()))
    });
    if request.assumptions.windows(2).any(|pair| {
        pair[0].kind() == pair[1].kind() && pair[0].identifier() == pair[1].identifier()
    }) {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }

    let current_market_input = request.common.current_market.input().id();
    request.inputs.push(request.common.current_market);
    request.inputs.sort_by_key(|value| value.input().id());
    for pair in request.inputs.windows(2) {
        if pair[0].input().id() == pair[1].input().id() && pair[0] != pair[1] {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
    }
    request
        .inputs
        .dedup_by(|left, right| left.input().id() == right.input().id());
    let input_set_id = input_set_identity(&request.inputs)?;

    let rights_decision = request.common.rights.decision_digest();
    let rights_graph = request.common.rights.graph_digest();
    let rights_input_digest = request.common.rights.rights_input_digest();
    let rights_expires_at = request.common.rights.expires_at();
    if request
        .inputs
        .iter()
        .any(|selected| !request.common.rights.admits(selected.input()))
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    if request.macro_assumptions.as_ref().is_some_and(|binding| {
        binding.premium_source_reference().is_none()
            || binding.premium_parent_manifests().iter().any(|manifest| {
                !request
                    .common
                    .rights
                    .authorization
                    .graph()
                    .nodes()
                    .iter()
                    .any(|node| node.manifest() == manifest)
            })
    }) {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    let admitted_input_manifests =
        exact_input_manifests(&request.inputs, request.macro_assumptions.as_ref())?
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
            .into_boxed_slice();
    let admitted_event_inputs = request.common.rights.event_admissions;
    let _consumed_event_authorities = request.common.rights.event_authorizations;
    let _consumed_single_use_permit = request.common.rights.authorization.into_permit();

    let mut receipt = AutomaticValuationMethodReceipt {
        id: AutomaticValuationIdentity([0; 32]),
        input_set_id,
        method: request.method,
        periods_per_year: request.periods_per_year,
        account_id: request.common.account_id,
        instrument_id: request.common.instrument_id,
        company_security: request.common.company_security,
        peer_identities: request.peer_identities.into_boxed_slice(),
        rights_decision,
        rights_graph,
        rights_input_digest,
        admitted_event_inputs,
        rights_expires_at,
        admitted_input_manifests,
        current_market_input,
        method_base_input: request.method_base_input,
        inputs: request.inputs.into_boxed_slice(),
        assumptions: request.assumptions.into_boxed_slice(),
        macro_assumptions: request.macro_assumptions,
        residual_terminal: request.residual_terminal,
        intermediates: request.intermediates.into_boxed_slice(),
        range,
        arithmetic_policy: request.common.arithmetic_policy,
        method_selection_receipt: request.method_selection_receipt,
        forecast_horizon_nanos: request.forecast_horizon_nanos,
        forecast_terminal_at: request.forecast_terminal_at,
        measurement_at: request.common.measurement_at,
        calculated_at: request.common.calculated_at,
        calculated_by: request.common.calculated_by,
        expires_at: request.common.expires_at,
    };
    verify_recovered_receipt(&receipt)?;
    receipt.id = receipt_identity(&receipt)?;
    Ok(AutomaticValuationCalculation { receipt })
}

fn receipt_identity(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<AutomaticValuationIdentity, AutomaticValuationError> {
    let mut hash = CanonicalHasher::new(b"market-squawk/automatic-valuation-calculation/v1");
    hash.u8(method_tag(receipt.method));
    match receipt.periods_per_year {
        Some(value) => {
            hash.u8(1);
            hash.u32(value.get());
        }
        None => hash.u8(0),
    }
    hash.bytes(receipt.account_id.as_uuid().as_bytes());
    hash.bytes(receipt.instrument_id.as_uuid().as_bytes());
    hash.fixed(receipt.company_security.receipt_digest().bytes());
    hash.fixed(receipt.rights_decision.bytes());
    hash.fixed(receipt.rights_graph.bytes());
    hash.fixed(receipt.rights_input_digest.bytes());
    hash.u64(receipt.admitted_event_inputs.len() as u64);
    for admission in &receipt.admitted_event_inputs {
        hash_event_admission(&mut hash, admission);
    }
    hash.i64(receipt.rights_expires_at.unix_nanos());
    hash.u64(
        u64::try_from(receipt.admitted_input_manifests.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for manifest in &receipt.admitted_input_manifests {
        crate::evidence::hash_manifest(&mut hash, manifest);
    }
    hash.fixed(receipt.current_market_input.bytes());
    match receipt.method_base_input {
        Some(id) => {
            hash.u8(1);
            hash.fixed(id.bytes());
        }
        None => hash.u8(0),
    }
    match &receipt.residual_terminal {
        Some(condition) => {
            hash.u8(1);
            hash.fixed(condition.identity().bytes());
        }
        None => hash.u8(0),
    }
    hash.fixed(receipt.input_set_id.bytes());
    receipt.range.central.hash_into(&mut hash);
    receipt.range.lower.hash_into(&mut hash);
    receipt.range.upper.hash_into(&mut hash);
    hash.u8(rounding_tag(receipt.arithmetic_policy.rounding()));
    hash.u64(
        u64::try_from(receipt.arithmetic_policy.maximum_periods())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    hash.u64(
        u64::try_from(receipt.peer_identities.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for receipt in &receipt.peer_identities {
        hash.fixed(receipt.receipt_digest().bytes());
    }
    hash.u64(
        u64::try_from(receipt.assumptions.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for assumption in &receipt.assumptions {
        hash_assumption(&mut hash, assumption);
    }
    if let Some(binding) = &receipt.macro_assumptions {
        hash.bytes(b"annual-government-reference-and-premium/v1");
        let reference = binding.reference();
        hash.u8(match reference.maturity() {
            crate::MacroRateMaturity::TenYear => 1,
            crate::MacroRateMaturity::ThirtyYear => 2,
        });
        hash.bytes(&reference.annual_yield_percent().mantissa().to_be_bytes());
        hash.u32(reference.annual_yield_percent().scale());
        hash.fixed(reference.context_identity().bytes());
        hash.fixed(reference.evidence_identity().bytes());
        hash.i64(reference.knowledge_cutoff().unix_nanos());
        hash.bytes(&reference.effective_date_cutoff().year().to_be_bytes());
        hash.u8(reference.effective_date_cutoff().month());
        hash.u8(reference.effective_date_cutoff().day());
        hash.i64(reference.available_at().unix_nanos());
        hash.i64(reference.expires_at().unix_nanos());
        hash_assumption(&mut hash, binding.premium());
        hash_assumption(&mut hash, binding.assumption());
        match binding.premium_source_reference() {
            Some(bytes) => {
                hash.u8(1);
                hash.bytes(bytes);
            }
            None => hash.u8(0),
        }
        hash.u64(
            u64::try_from(binding.premium_parent_manifests().len())
                .map_err(|_| AutomaticValuationError::Arithmetic)?,
        );
        for manifest in binding.premium_parent_manifests() {
            crate::evidence::hash_manifest(&mut hash, manifest);
        }
    }
    hash.u64(
        u64::try_from(receipt.intermediates.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for value in &receipt.intermediates {
        hash_intermediate(&mut hash, value);
    }
    hash_optional_digest(&mut hash, receipt.method_selection_receipt);
    match receipt.forecast_horizon_nanos {
        Some(value) => {
            hash.u8(1);
            hash.u64(value.get());
        }
        None => hash.u8(0),
    }
    match receipt.forecast_terminal_at {
        Some(value) => {
            hash.u8(1);
            hash.i64(value.unix_nanos());
        }
        None => hash.u8(0),
    }
    hash.i64(receipt.measurement_at.unix_nanos());
    hash.i64(receipt.calculated_at.unix_nanos());
    hash.bytes(receipt.calculated_by.as_str().as_bytes());
    hash.i64(receipt.expires_at.unix_nanos());

    Ok(AutomaticValuationIdentity(hash.finish()))
}

fn company_receipt_dynamic_bytes(
    receipt: &CompanySecurityIdentitySelectionReceipt,
) -> Result<usize, crate::FairValueError> {
    use std::mem::size_of_val;
    let mut total = crate::checked_add(
        size_of_val(receipt.ordered_candidates()),
        size_of_val(receipt.ordered_exclusions()),
    )?;
    for entry in receipt
        .ordered_candidates()
        .iter()
        .chain(receipt.ordered_exclusions().iter().map(|(entry, _)| entry))
    {
        for length in [
            entry.company_source_id().as_str().len(),
            entry.provider_company_id().as_str().len(),
            entry.rights_policy_id().as_str().len(),
            entry.rights_terms_reference().as_str().len(),
        ] {
            total = crate::checked_add(total, length)?;
        }
    }
    Ok(total)
}

fn recovered_input(
    receipt: &AutomaticValuationMethodReceipt,
    id: InputId,
) -> Result<&PointInTimeValuationInput, AutomaticValuationError> {
    receipt
        .inputs
        .binary_search_by_key(&id, |value| value.input().id())
        .ok()
        .and_then(|index| receipt.inputs.get(index))
        .ok_or(AutomaticValuationError::InvalidContract)
}

fn recovered_assumption(
    receipt: &AutomaticValuationMethodReceipt,
    kind: AutomaticValuationAssumptionKind,
) -> Result<&AutomaticValuationAssumption, AutomaticValuationError> {
    let mut matches = receipt
        .assumptions
        .iter()
        .filter(|value| value.kind() == kind);
    let value = matches
        .next()
        .ok_or(AutomaticValuationError::InvalidContract)?;
    if matches.next().is_some() {
        return Err(AutomaticValuationError::InvalidContract);
    }
    Ok(value)
}

fn verify_recovered_receipt(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<(), AutomaticValuationError> {
    let invalid = AutomaticValuationError::InvalidContract;
    if receipt.inputs.is_empty()
        || receipt.inputs.len() > MAX_METHOD_INPUTS
        || receipt.admitted_input_manifests.is_empty()
        || receipt.admitted_input_manifests.len() > MAX_INPUT_MANIFESTS
        || receipt.assumptions.len() < 3
        || receipt.assumptions.len() > MAX_ASSUMPTIONS
        || receipt.intermediates.is_empty()
        || receipt.intermediates.len() > MAX_FORECAST_POINTS
        || receipt.peer_identities.len() > MAX_COMPARABLES
        || receipt.calculated_at < receipt.measurement_at
        || receipt.expires_at <= receipt.calculated_at
        || receipt.rights_expires_at < receipt.expires_at
        || receipt.rights_decision.bytes() == [0; 32]
        || receipt.rights_graph.bytes() == [0; 32]
        || receipt.admitted_event_inputs.is_empty()
        || receipt.admitted_event_inputs.len() > MAX_METHOD_INPUTS
        || receipt.rights_input_digest
            != combined_rights_input_digest(receipt.rights_graph, &receipt.admitted_event_inputs)
        || receipt
            .admitted_event_inputs
            .windows(2)
            .any(|pair| pair[0].rights_input_digest.bytes() >= pair[1].rights_input_digest.bytes())
        || receipt.admitted_event_inputs.iter().any(|admission| {
            admission.evaluated_at > receipt.calculated_at
                || admission.expires_at < receipt.rights_expires_at
        })
        || receipt
            .inputs
            .windows(2)
            .any(|pair| pair[0].input().id() >= pair[1].input().id())
        || receipt.assumptions.windows(2).any(|pair| {
            (pair[0].kind(), pair[0].identifier()) >= (pair[1].kind(), pair[1].identifier())
        })
    {
        return Err(invalid);
    }
    residual::verify_residual_terminal(receipt)?;
    if !exact_input_manifests(&receipt.inputs, receipt.macro_assumptions.as_ref())?
        .into_iter()
        .eq(receipt.admitted_input_manifests.iter())
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }
    verify_event_admission_coverage(&receipt.inputs, &receipt.admitted_event_inputs)?;
    validate_company_security(
        &receipt.company_security,
        receipt.instrument_id,
        receipt.measurement_at,
        receipt.expires_at,
    )?;
    let market = recovered_input(receipt, receipt.current_market_input)?;
    if market.knowledge_at() < receipt.measurement_at
        || market.knowledge_at() > receipt.calculated_at
        || market.input().subject_instrument_id() != receipt.instrument_id
        || market.input().reference_instrument_id() != receipt.instrument_id
        || market.input().relationship() != InputInstrumentRelation::Identical
        || market.input().amount().basis() != ValuationAmountBasis::PerInstrumentUnit
        || !is_published_market_input(market)
        || market
            .input()
            .evidence()
            .source_timestamp()
            .is_none_or(|time| time > market.knowledge_at())
        || market
            .input()
            .market_access_assessment()
            .is_some_and(|value| value.account_id() != receipt.account_id)
    {
        return Err(invalid);
    }
    for selected in &receipt.inputs {
        let input = selected.input();
        let expected_cutoff = if is_published_market_input(selected) {
            market.knowledge_at()
        } else {
            receipt.measurement_at
        };
        if selected.knowledge_at() != expected_cutoff
            || selected.expires_at() < receipt.expires_at
            || selected.rights_input_digest() != receipt.rights_input_digest
            || !derived_input_completed_by(input, receipt.calculated_at)
            || !input
                .evidence()
                .producer_verification_is_current_at(selected.knowledge_at())
            || input.amount().money().currency() != receipt.range.central.money().currency()
            || input.relationship() != InputInstrumentRelation::Identical
            || input.subject_instrument_id() != input.reference_instrument_id()
            || (input.id() != receipt.current_market_input
                && input.amount().basis() != receipt.range.central.basis())
            || (input.id() != receipt.current_market_input
                && Some(input.id()) != receipt.method_base_input
                && !receipt.intermediates.iter().any(|step| {
                    step.primary_input == input.id() || step.secondary_input == Some(input.id())
                }))
        {
            return Err(invalid);
        }
    }
    let lower = recovered_assumption(receipt, AutomaticValuationAssumptionKind::UncertaintyLower)?;
    let upper = recovered_assumption(receipt, AutomaticValuationAssumptionKind::UncertaintyUpper)?;
    if lower.value() != receipt.range.lower.money().amount()
        || upper.value() != receipt.range.upper.money().amount()
    {
        return Err(invalid);
    }
    for assumption in &receipt.assumptions {
        let ceiling = receipt.calculated_at;
        if assumption.available_at() > ceiling || assumption.expires_at() < receipt.expires_at {
            return Err(invalid);
        }
    }
    if receipt.method != AutomaticValuationMethod::ComparableCompanies
        && !receipt.peer_identities.is_empty()
    {
        return Err(invalid);
    }
    if receipt.method != AutomaticValuationMethod::ForecastDistribution
        && (receipt.method_selection_receipt.is_some()
            || receipt.forecast_horizon_nanos.is_some()
            || receipt.forecast_terminal_at.is_some())
    {
        return Err(invalid);
    }
    match (receipt.method, receipt.macro_assumptions.as_ref()) {
        (
            AutomaticValuationMethod::DiscountedCashFlow | AutomaticValuationMethod::ResidualIncome,
            Some(binding),
        ) => {
            let kind = if receipt.method == AutomaticValuationMethod::DiscountedCashFlow {
                AutomaticValuationAssumptionKind::DiscountRate
            } else {
                AutomaticValuationAssumptionKind::CostOfEquity
            };
            let recovered = binding.revalidated()?;
            if recovered != *binding
                || binding.premium_source_reference().is_none()
                || receipt.periods_per_year != Some(NonZeroU32::MIN)
                || receipt.range.central.money().currency().as_str() != "USD"
                || binding.reference().knowledge_cutoff() > receipt.measurement_at
                || recovered_assumption(receipt, kind)? != binding.assumption()
            {
                return Err(invalid);
            }
        }
        (
            AutomaticValuationMethod::ComparableCompanies
            | AutomaticValuationMethod::ForecastDistribution,
            None,
        ) => {}
        _ => return Err(invalid),
    }
    let raw_value = match receipt.method {
        AutomaticValuationMethod::DiscountedCashFlow | AutomaticValuationMethod::ResidualIncome => {
            verify_recovered_discounting(receipt)?
        }
        AutomaticValuationMethod::ComparableCompanies => verify_recovered_comparables(receipt)?,
        AutomaticValuationMethod::ForecastDistribution => verify_recovered_distribution(receipt)?,
    };
    let rounded = round(
        raw_value,
        receipt.range.central.scale(),
        receipt.arithmetic_policy.rounding(),
    );
    if rounded != receipt.range.central.money().amount() {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }
    Ok(())
}

fn verify_recovered_discounting(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<Decimal, AutomaticValuationError> {
    let invalid = AutomaticValuationError::InvalidContract;
    let dcf = receipt.method == AutomaticValuationMethod::DiscountedCashFlow;
    let role = if dcf {
        AutomaticValuationAssumptionKind::DiscountRate
    } else {
        AutomaticValuationAssumptionKind::CostOfEquity
    };
    if receipt.periods_per_year != Some(NonZeroU32::MIN)
        || receipt.assumptions.len() != if dcf { 4 } else { 3 }
    {
        return Err(invalid);
    }
    let terminal_policy = receipt.terminal_growth_policy()?;
    let rate = recovered_assumption(receipt, role)?;
    let base = Decimal::ONE
        .checked_add(rate.value())
        .ok_or(AutomaticValuationError::Arithmetic)?;
    if base <= Decimal::ZERO {
        return Err(invalid);
    }
    let residual_anchor = if dcf {
        let terminal = receipt.intermediates.last().ok_or(invalid)?;
        let source = recovered_input(receipt, terminal.primary_input)?;
        let anchor = native_financial_input(
            source,
            FinancialAmountRole::CommonEquityCashFlow,
            terminal.sequence.checked_add(1).ok_or(invalid)?,
        )?;
        if anchor.identity != receipt.company_security.receipt_digest() {
            return Err(invalid);
        }
        Some(anchor)
    } else {
        let current = recovered_input(receipt, receipt.method_base_input.ok_or(invalid)?)?;
        let anchor = native_financial_input(current, FinancialAmountRole::CommonBookEquity, 0)?;
        if anchor.identity != receipt.company_security.receipt_digest()
            || receipt.periods_per_year != Some(NonZeroU32::MIN)
        {
            return Err(invalid);
        }
        Some(anchor)
    };
    let mut total = if dcf {
        if receipt.method_base_input.is_some()
            || receipt.intermediates.len() < 2
            || receipt.intermediates.len() - 1
                > MAX_DCF_PERIODS.min(receipt.arithmetic_policy.maximum_periods())
        {
            return Err(invalid);
        }
        Decimal::ZERO
    } else {
        if receipt.intermediates.len()
            > MAX_RESIDUAL_PERIODS.min(receipt.arithmetic_policy.maximum_periods())
        {
            return Err(invalid);
        }
        let value = recovered_input(receipt, receipt.method_base_input.ok_or(invalid)?)?;
        if value.input().subject_instrument_id() != receipt.instrument_id {
            return Err(invalid);
        }
        input_decimal(value)
    };
    let terminal_sequence = if dcf {
        receipt.intermediates.last().ok_or(invalid)?.sequence
    } else {
        0
    };
    let mut previous = 0;
    for (index, step) in receipt.intermediates.iter().enumerate() {
        let terminal = dcf && index + 1 == receipt.intermediates.len();
        let kind = if terminal {
            AutomaticValuationIntermediateKind::DiscountedTerminalValue
        } else if dcf {
            AutomaticValuationIntermediateKind::DiscountedCashFlow
        } else {
            AutomaticValuationIntermediateKind::DiscountedResidualIncome
        };
        if step.kind != kind
            || step.instrument_id != receipt.instrument_id
            || step.sequence == 0
            || usize::try_from(step.sequence).map_err(|_| invalid)?
                > receipt.arithmetic_policy.maximum_periods()
            || (!terminal && step.sequence <= previous)
            || (!terminal && usize::try_from(step.sequence).ok() != index.checked_add(1))
            || (dcf && step.sequence > terminal_sequence)
        {
            return Err(invalid);
        }
        if !terminal {
            previous = step.sequence;
        }
        let primary = recovered_input(receipt, step.primary_input)?;
        if primary.input().subject_instrument_id() != receipt.instrument_id
            || primary.input().amount().basis() != receipt.range.central.basis()
        {
            return Err(invalid);
        }
        let amount = input_decimal(primary);
        let adjustment = if dcf {
            if (!terminal && step.secondary_input.is_some())
                || (terminal
                    && step.secondary_input
                        != terminal_policy
                            .as_ref()
                            .map(DcfTerminalGrowthPolicy::final_explicit_input))
            {
                return Err(invalid);
            }
            let offset = step
                .sequence
                .checked_add(u32::from(terminal))
                .ok_or(invalid)?;
            if Some(native_financial_input(
                primary,
                FinancialAmountRole::CommonEquityCashFlow,
                offset,
            )?) != residual_anchor
            {
                return Err(invalid);
            }
            if terminal {
                terminal_policy
                    .as_ref()
                    .ok_or(invalid)?
                    .assumption()
                    .value()
            } else {
                Decimal::ZERO
            }
        } else {
            let opening = recovered_input(receipt, step.secondary_input.ok_or(invalid)?)?;
            if opening.input().subject_instrument_id() != receipt.instrument_id
                || opening.input().amount().basis() != receipt.range.central.basis()
            {
                return Err(invalid);
            }
            let income_anchor = native_financial_input(
                primary,
                FinancialAmountRole::CommonNetIncome,
                step.sequence,
            )?;
            let opening_anchor = native_financial_input(
                opening,
                FinancialAmountRole::CommonBookEquity,
                step.sequence.checked_sub(1).ok_or(invalid)?,
            )?;
            if Some(income_anchor) != residual_anchor
                || Some(opening_anchor) != residual_anchor
                || (step.sequence == 1 && Some(opening.input().id()) != receipt.method_base_input)
            {
                return Err(invalid);
            }
            input_decimal(opening)
                .checked_mul(rate.value())
                .ok_or(AutomaticValuationError::Arithmetic)?
        };
        let period = NonZeroU32::new(step.sequence).ok_or(invalid)?;
        let (factor, result) = if terminal {
            if previous != step.sequence {
                return Err(invalid);
            }
            terminal_fcfe_discount(amount, rate.value(), adjustment, period)?
        } else {
            let discounted = amount
                .checked_sub(adjustment)
                .ok_or(AutomaticValuationError::Arithmetic)?;
            discount(discounted, base, period)?
        };
        let expected = intermediate(
            kind,
            step.sequence,
            receipt.instrument_id,
            step.primary_input,
            step.secondary_input,
            amount,
            adjustment,
            factor,
            result,
            rate.evidence(),
        );
        if &expected != step {
            return Err(invalid);
        }
        total = total
            .checked_add(result)
            .ok_or(AutomaticValuationError::Arithmetic)?;
    }
    Ok(total)
}

fn verify_recovered_comparables(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<Decimal, AutomaticValuationError> {
    let invalid = AutomaticValuationError::InvalidContract;
    let count = receipt.peer_identities.len();
    if receipt.periods_per_year.is_some()
        || count == 0
        || receipt.intermediates.len() != count + 1
        || receipt.assumptions.len() != count + 2
        || receipt.assumptions[..count]
            .iter()
            .any(|value| value.kind() != AutomaticValuationAssumptionKind::ComparableWeight)
    {
        return Err(invalid);
    }
    let subject = recovered_input(receipt, receipt.method_base_input.ok_or(invalid)?)?;
    if subject.input().subject_instrument_id() != receipt.instrument_id
        || subject.input().amount().basis() != receipt.range.central.basis()
    {
        return Err(invalid);
    }
    let mut multiple_sum = Decimal::ZERO;
    let mut weight_sum = 0_u32;
    let mut previous = None;
    for (index, identity) in receipt.peer_identities.iter().enumerate() {
        let step = &receipt.intermediates[index];
        validate_company_security(
            identity,
            step.instrument_id,
            receipt.measurement_at,
            receipt.expires_at,
        )?;
        if step.instrument_id == receipt.instrument_id
            || previous.is_some_and(|id| id >= step.instrument_id)
            || step.kind != AutomaticValuationIntermediateKind::WeightedComparableMultiple
            || step.sequence != u32::try_from(index + 1).map_err(|_| invalid)?
        {
            return Err(invalid);
        }
        previous = Some(step.instrument_id);
        let numerator = recovered_input(receipt, step.primary_input)?;
        let denominator = recovered_input(receipt, step.secondary_input.ok_or(invalid)?)?;
        if [numerator, denominator].into_iter().any(|value| {
            value.input().subject_instrument_id() != step.instrument_id
                || value.input().amount().basis() != receipt.range.central.basis()
        }) || input_decimal(denominator).is_zero()
        {
            return Err(invalid);
        }
        let weight = exact_probability_ppm(step.factor)?;
        if !receipt.assumptions[..count]
            .iter()
            .any(|value| value.evidence() == step.evidence && value.value() == step.factor)
        {
            return Err(invalid);
        }
        let amount = input_decimal(numerator);
        let multiple = amount
            .checked_div(input_decimal(denominator))
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let contribution = multiple
            .checked_mul(step.factor)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let expected = intermediate(
            step.kind,
            step.sequence,
            step.instrument_id,
            step.primary_input,
            step.secondary_input,
            amount,
            multiple,
            step.factor,
            contribution,
            step.evidence,
        );
        if &expected != step {
            return Err(invalid);
        }
        weight_sum = weight_sum
            .checked_add(weight)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        multiple_sum = multiple_sum
            .checked_add(contribution)
            .ok_or(AutomaticValuationError::Arithmetic)?;
    }
    if weight_sum != PROBABILITY_PARTS_PER_MILLION {
        return Err(invalid);
    }
    let amount = input_decimal(subject);
    let raw = amount
        .checked_mul(multiple_sum)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let expected = intermediate(
        AutomaticValuationIntermediateKind::ComparableSubjectValue,
        u32::try_from(count + 1).map_err(|_| invalid)?,
        receipt.instrument_id,
        subject.input().id(),
        None,
        amount,
        multiple_sum,
        Decimal::ONE,
        raw,
        receipt.company_security.receipt_digest(),
    );
    if receipt.intermediates.last() != Some(&expected) {
        return Err(invalid);
    }
    Ok(raw)
}

fn verify_recovered_distribution(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<Decimal, AutomaticValuationError> {
    let invalid = AutomaticValuationError::InvalidContract;
    let count = receipt.intermediates.len();
    let horizon = receipt.forecast_horizon_nanos.ok_or(invalid)?;
    let terminal = receipt
        .measurement_at
        .checked_add_nanos(i64::try_from(horizon.get()).map_err(|_| invalid)?)
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    if receipt.periods_per_year.is_some()
        || receipt.method_base_input.is_some()
        || receipt.forecast_terminal_at != Some(terminal)
        || !receipt.method_selection_receipt.is_some_and(valid_sha256)
        || count > MAX_FORECAST_POINTS.min(receipt.arithmetic_policy.maximum_periods())
        || receipt.assumptions.len() != count + 2
    {
        return Err(invalid);
    }
    let mut total = Decimal::ZERO;
    let mut probability_sum = 0_u32;
    let mut ordinals = Vec::with_capacity(count);
    let first = recovered_input(
        receipt,
        receipt.intermediates.first().ok_or(invalid)?.primary_input,
    )?;
    let EvidenceOrigin::ForecastDistribution {
        evidence: first_evidence,
    } = first.input().evidence().origin()
    else {
        return Err(invalid);
    };
    let source = first_evidence.source();
    if source.distribution().points().len() != count
        || Some(source.reference().identity()) != receipt.method_selection_receipt
        || source.distribution().target_at() != Some(terminal)
    {
        return Err(invalid);
    }
    for (index, step) in receipt.intermediates.iter().enumerate() {
        let assumption = &receipt.assumptions[index];
        let value = recovered_input(receipt, step.primary_input)?;
        if assumption.kind() != AutomaticValuationAssumptionKind::ForecastProbability
            || step.kind != AutomaticValuationIntermediateKind::ProbabilityWeightedForecast
            || step.sequence != u32::try_from(index + 1).map_err(|_| invalid)?
            || step.instrument_id != receipt.instrument_id
            || step.secondary_input.is_some()
            || value.input().subject_instrument_id() != receipt.instrument_id
            || value.input().amount().basis() != receipt.range.central.basis()
            || value.input().evidence().effective_at() != Some(terminal)
        {
            return Err(invalid);
        }
        let probability = exact_probability_ppm(assumption.value())?;
        let EvidenceOrigin::ForecastDistribution { evidence } = value.input().evidence().origin()
        else {
            return Err(invalid);
        };
        let native = source
            .distribution()
            .points()
            .get(
                evidence
                    .ordinal()
                    .ok_or(AutomaticValuationError::InvalidContract)?,
            )
            .ok_or(invalid)?;
        if evidence.source().reference() != source.reference()
            || native.probability_ppm().get() != probability
            || assumption.evidence() != source.reference().identity()
            || assumption.available_at() != source.distribution().published_at()
        {
            return Err(invalid);
        }
        ordinals.push(evidence.ordinal().ok_or(invalid)?);
        let amount = input_decimal(value);
        let result = amount
            .checked_mul(assumption.value())
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let expected = intermediate(
            step.kind,
            step.sequence,
            step.instrument_id,
            step.primary_input,
            None,
            amount,
            Decimal::ZERO,
            assumption.value(),
            result,
            assumption.evidence(),
        );
        if &expected != step {
            return Err(invalid);
        }
        probability_sum = probability_sum
            .checked_add(probability)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        total = total
            .checked_add(result)
            .ok_or(AutomaticValuationError::Arithmetic)?;
    }
    if probability_sum != PROBABILITY_PARTS_PER_MILLION {
        return Err(invalid);
    }
    ordinals.sort_unstable();
    if ordinals.iter().copied().ne(0..count) {
        return Err(invalid);
    }
    let lower = recovered_assumption(receipt, AutomaticValuationAssumptionKind::UncertaintyLower)?;
    let upper = recovered_assumption(receipt, AutomaticValuationAssumptionKind::UncertaintyUpper)?;
    let scale = u32::from(receipt.range.central.scale());
    for (assumption, ordinal, rounding) in [
        (lower, 0, RoundingStrategy::ToNegativeInfinity),
        (upper, count - 1, RoundingStrategy::ToPositiveInfinity),
    ] {
        let expected = source
            .amount(ordinal)
            .map_err(|_| invalid)?
            .money()
            .amount()
            .round_dp_with_strategy(scale, rounding);
        if assumption.value() != expected
            || assumption.evidence() != source.reference().identity()
            || assumption.available_at() != source.distribution().published_at()
        {
            return Err(invalid);
        }
    }
    Ok(total)
}

fn exact_probability_ppm(value: Decimal) -> Result<u32, AutomaticValuationError> {
    let scaled = value
        .checked_mul(Decimal::from(PROBABILITY_PARTS_PER_MILLION))
        .ok_or(AutomaticValuationError::Arithmetic)?
        .normalize();
    if scaled.scale() != 0 {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let result =
        u32::try_from(scaled.mantissa()).map_err(|_| AutomaticValuationError::InvalidContract)?;
    if result == 0
        || result > PROBABILITY_PARTS_PER_MILLION
        || probability_decimal(result)? != value
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    Ok(result)
}

fn validate_common(common: &AutomaticValuationInput) -> Result<(), AutomaticValuationError> {
    validate_company_security(
        &common.company_security,
        common.instrument_id,
        common.measurement_at,
        common.expires_at,
    )?;
    if common.calculated_at < common.measurement_at
        || common.expires_at <= common.calculated_at
        || u32::from(common.output_scale) > Decimal::MAX_SCALE
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    if common.rights.expires_at() < common.expires_at {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    let market = &common.current_market;
    let input = market.input();
    if !common.rights.admits(input) {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    if market.knowledge_at() < common.measurement_at
        || market.knowledge_at() > common.calculated_at
        || market.expires_at() < common.expires_at
        || market.rights_input_digest() != common.rights.rights_input_digest()
        || input.subject_instrument_id() != common.instrument_id
        || input.reference_instrument_id() != common.instrument_id
        || input.relationship() != InputInstrumentRelation::Identical
        || input.amount().money().currency() != common.currency
        || input.amount().basis() != ValuationAmountBasis::PerInstrumentUnit
        || !is_published_market_input(market)
        || !input
            .evidence()
            .producer_verification_is_current_at(market.knowledge_at())
        || input
            .evidence()
            .source_timestamp()
            .is_none_or(|value| value > market.knowledge_at())
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::CurrentMarket,
        ));
    }
    if input
        .market_access_assessment()
        .is_some_and(|value| value.account_id() != common.account_id)
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }
    Ok(())
}

fn is_published_market_input(selected: &PointInTimeValuationInput) -> bool {
    let evidence = selected.input().evidence();
    matches!(
        evidence.origin(),
        EvidenceOrigin::Market {
            publication: Some(_),
            ..
        } | EvidenceOrigin::PublishedMarket { .. }
    ) && evidence.automatic_selection_binding()
        == Some((selected.selection_receipt(), selected.knowledge_at()))
}

fn automatic_event_input(
    input: &ValuationInput,
) -> Option<(&MarketEventCommitRef, EvidenceDigest, u32, EvidenceDigest)> {
    match input.evidence().origin() {
        EvidenceOrigin::Market {
            publication: Some(publication),
            ..
        } => Some((
            &publication.commit,
            publication.publication_digest,
            publication.publication_row,
            publication.canonical_event_digest,
        )),
        EvidenceOrigin::PublishedMarket { evidence } => Some((
            &evidence.commit,
            evidence.publication_digest,
            evidence.publication_row,
            evidence.canonical_event_digest,
        )),
        _ => None,
    }
}

fn matches_event_input(
    input: &ValuationInput,
    admission: &ValuationEventRightsAdmission,
    event: &MarketEventUseInput,
) -> bool {
    let Some((commit, publication, row, canonical_digest)) = automatic_event_input(input) else {
        return false;
    };
    let (origin, coordinate) = match input.evidence().origin() {
        EvidenceOrigin::Market {
            publication: Some(publication),
            ..
        } => (
            publication.origin_committed_at,
            Some(publication.coordinate_digest),
        ),
        EvidenceOrigin::PublishedMarket { evidence } => (evidence.origin_committed_at, None),
        _ => return false,
    };
    admission.commit() == commit
        && event.publication_digest() == publication
        && event.row_ordinal() == row
        && event.canonical_event_digest() == canonical_digest
        && event.source_id() == input.evidence().source_id()
        && event.origin_committed_at() == origin
        && coordinate.is_none_or(|value| value == event.coordinate_digest())
}

fn verify_event_admission_coverage(
    inputs: &[PointInTimeValuationInput],
    admissions: &[ValuationEventRightsAdmission],
) -> Result<(), AutomaticValuationError> {
    let input_count = admissions
        .iter()
        .try_fold(0usize, |count, admission| {
            count.checked_add(admission.inputs().len())
        })
        .ok_or(AutomaticValuationError::InvalidContract)?;
    if input_count > MAX_METHOD_INPUTS {
        return Err(AutomaticValuationError::InvalidContract);
    }
    for input in inputs {
        if automatic_event_input(input.input()).is_some()
            && !admissions.iter().any(|admission| {
                admission
                    .inputs()
                    .iter()
                    .any(|event| matches_event_input(input.input(), admission, event))
            })
        {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::Rights,
            ));
        }
    }
    for admission in admissions {
        for event in admission.inputs() {
            if !inputs
                .iter()
                .any(|input| matches_event_input(input.input(), admission, event))
            {
                return Err(AutomaticValuationError::Conflict(
                    AutomaticValuationConflict::Evidence,
                ));
            }
        }
    }
    Ok(())
}

fn automatic_input_manifests(input: &ValuationInput) -> &[DatasetManifestRef] {
    match input.evidence().origin() {
        EvidenceOrigin::Market {
            publication: Some(_),
            ..
        }
        | EvidenceOrigin::PublishedMarket { .. } => &[],
        EvidenceOrigin::Research { manifest, .. }
        | EvidenceOrigin::Analytics { manifest, .. }
        | EvidenceOrigin::Fundamental { manifest, .. } => std::slice::from_ref(manifest),
        EvidenceOrigin::ForecastDistribution { evidence } => {
            evidence.source().reference().parent_manifests()
        }
        EvidenceOrigin::Market {
            publication: None, ..
        }
        | EvidenceOrigin::Portfolio { .. }
        | EvidenceOrigin::AutomaticValuation { .. } => &[],
    }
}

fn exact_input_manifests<'a>(
    inputs: &'a [PointInTimeValuationInput],
    macro_assumptions: Option<&'a FinancialModelMacroAssumptions>,
) -> Result<Vec<&'a DatasetManifestRef>, AutomaticValuationError> {
    if inputs.is_empty() || inputs.len() > MAX_METHOD_INPUTS {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let mut manifests = Vec::new();
    manifests
        .try_reserve_exact(inputs.len())
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    for input in inputs {
        let source_manifests = automatic_input_manifests(input.input());
        if source_manifests.is_empty() && automatic_event_input(input.input()).is_none() {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::Rights,
            ));
        }
        for manifest in source_manifests {
            if manifests.contains(&manifest) {
                continue;
            }
            if manifests.len() >= MAX_INPUT_MANIFESTS {
                return Err(AutomaticValuationError::InvalidContract);
            }
            manifests
                .try_reserve(1)
                .map_err(|_| AutomaticValuationError::Arithmetic)?;
            manifests.push(manifest);
        }
    }
    if let Some(binding) = macro_assumptions {
        for manifest in binding.premium_parent_manifests() {
            if !manifests.contains(&manifest) {
                if manifests.len() == MAX_INPUT_MANIFESTS {
                    return Err(AutomaticValuationError::InvalidContract);
                }
                manifests
                    .try_reserve_exact(1)
                    .map_err(|_| AutomaticValuationError::Arithmetic)?;
                manifests.push(manifest);
            }
        }
    }
    manifests.sort_by(|left, right| {
        left.dataset_id()
            .as_str()
            .cmp(right.dataset_id().as_str())
            .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
            .then_with(|| left.schema().cmp(right.schema()))
            .then_with(|| left.content_hash().cmp(&right.content_hash()))
    });
    manifests.dedup();
    Ok(manifests)
}

fn validate_company_security(
    receipt: &CompanySecurityIdentitySelectionReceipt,
    instrument_id: InstrumentId,
    measurement_at: Timestamp,
    expires_at: Timestamp,
) -> Result<(), AutomaticValuationError> {
    if receipt.disposition() == CompanySecurityIdentityDisposition::Conflict {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Identity,
        ));
    }
    if receipt.disposition() != CompanySecurityIdentityDisposition::Complete
        || receipt.ordered_candidates().len() != 1
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Identity,
        ));
    }
    if receipt.knowledge_at() != measurement_at
        || !valid_sha256(receipt.query_digest())
        || !valid_sha256(receipt.receipt_digest())
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Identity,
        ));
    }
    let candidate = &receipt.ordered_candidates()[0];
    let digests_current = candidate.current_company_observation_digest()
        == Some(candidate.linked_company_observation_digest())
        && candidate.current_market_revision_digest()
            == Some(candidate.linked_market_revision_digest());
    let current_times_available = candidate
        .current_company_available_at()
        .is_some_and(|value| value <= measurement_at)
        && candidate
            .current_company_ingested_at()
            .is_some_and(|value| value <= measurement_at)
        && candidate
            .current_company_completed_at()
            .is_some_and(|value| value <= measurement_at)
        && candidate
            .current_market_published_at()
            .is_some_and(|value| value <= measurement_at)
        && candidate
            .current_market_effective_start()
            .is_some_and(|value| value <= measurement_at);
    let current_through_expiry = candidate
        .effective_end()
        .is_none_or(|value| value >= expires_at)
        && candidate
            .market_effective_end()
            .is_none_or(|value| value >= expires_at)
        && candidate
            .current_market_effective_end()
            .is_none_or(|value| value >= expires_at);
    if candidate.instrument_id() != instrument_id
        || !valid_sha256(candidate.link_digest())
        || !digests_current
        || !current_times_available
        || !current_through_expiry
        || candidate.company_ingested_at() > measurement_at
        || candidate.company_completed_at() > measurement_at
        || candidate.market_published_at() > measurement_at
        || candidate.link_available_at() > measurement_at
        || candidate.link_ingested_at() > measurement_at
        || candidate.link_published_at() > measurement_at
        || candidate.effective_start() > measurement_at
        || candidate
            .effective_end()
            .is_some_and(|value| value <= measurement_at)
        || candidate.rights_entitlement() == IdentifierEntitlement::UnknownOrRestricted
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Identity,
        ));
    }
    Ok(())
}

fn validate_method_input(
    value: &PointInTimeValuationInput,
    common: &AutomaticValuationInput,
    instrument_id: InstrumentId,
) -> Result<(), AutomaticValuationError> {
    let expected_cutoff = if is_published_market_input(value) {
        common.current_market.knowledge_at()
    } else {
        common.measurement_at
    };
    if value.knowledge_at() != expected_cutoff || value.expires_at() < common.expires_at {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    if value.rights_input_digest() != common.rights.rights_input_digest()
        || !common.rights.admits(value.input())
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    let input = value.input();
    if !derived_input_completed_by(input, common.calculated_at) {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    if !input
        .evidence()
        .producer_verification_is_current_at(value.knowledge_at())
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    if input.subject_instrument_id() != instrument_id
        || input.reference_instrument_id() != instrument_id
        || input.relationship() != InputInstrumentRelation::Identical
    {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Identity,
        ));
    }
    if input.amount().money().currency() != common.currency {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Currency,
        ));
    }
    if input.amount().basis() != common.amount_basis {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::AmountBasis,
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativeFinancialAnchor {
    annual_period: FundamentalPeriod,
    identity: EvidenceDigest,
    source_selection: EvidenceDigest,
}

fn native_financial_input(
    value: &PointInTimeValuationInput,
    required_role: FinancialAmountRole,
    offset: u32,
) -> Result<NativeFinancialAnchor, AutomaticValuationError> {
    let invalid = AutomaticValuationError::Unavailable(AutomaticValuationUnavailable::MethodInput);
    let EvidenceOrigin::ForecastDistribution { evidence } = value.input().evidence().origin()
    else {
        return Err(invalid);
    };
    let source = evidence.source();
    let epoch = source.financial_epoch().ok_or(invalid)?;
    let binding = epoch.financial_period().ok_or(invalid)?;
    let Some(market_squawk_data::FeatureLabelMeasurement::FinancialAmount {
        currency,
        role,
        basis,
        share_convention,
    }) = epoch.financial_measurement()
    else {
        return Err(invalid);
    };
    let selection = if offset == 0 {
        ForecastValuationValueSelection::FinancialOrigin
    } else {
        ForecastValuationValueSelection::ConditionalMean
    };
    if evidence.selection() != selection
        || role != required_role
        || basis != FinancialAmountBasis::TotalCommonEquity
        || share_convention.is_some()
        || currency != value.input().amount().money().currency()
        || value.input().amount().basis() != ValuationAmountBasis::TotalCommonEquity
        || binding.cadence() != FundamentalCadence::Annual
        || epoch.source_selection_as_of() != value.knowledge_at()
        || source.reference().knowledge_at() != value.knowledge_at()
        || source.distribution().financial_target() != Some(binding)
        || (offset > 0
            && binding
                .target_ordinal()
                .checked_sub(binding.observed_ordinal())
                != Some(offset))
        || (offset == 0 && required_role != FinancialAmountRole::CommonBookEquity)
        || matches!(binding.observed_period(), FundamentalPeriod::Instant { .. })
            != (required_role == FinancialAmountRole::CommonBookEquity)
    {
        return Err(invalid);
    }
    let observed_end = binding.observed_period().end();
    let mut annual_period = None;
    for row in binding.duration_chain() {
        let period = row.fact_context().period();
        if period.end() != observed_end {
            continue;
        }
        if !matches!(period, FundamentalPeriod::Duration { .. })
            || annual_period.is_some_and(|existing| existing != period)
        {
            return Err(invalid);
        }
        annual_period = Some(period);
    }
    Ok(NativeFinancialAnchor {
        annual_period: annual_period.ok_or(invalid)?,
        identity: binding.identity_receipt_digest(),
        source_selection: binding.source_selection_digest(),
    })
}

fn derived_input_completed_by(input: &ValuationInput, calculated_at: Timestamp) -> bool {
    match input.evidence().origin() {
        EvidenceOrigin::ForecastDistribution { evidence } => {
            evidence.source().reference().selected_at() <= calculated_at
                && evidence.source().distribution().published_at() <= calculated_at
        }
        _ => true,
    }
}

fn validate_derived_assumption(
    value: &AutomaticValuationAssumption,
    kind: AutomaticValuationAssumptionKind,
    common: &AutomaticValuationInput,
) -> Result<(), AutomaticValuationError> {
    if value.kind() != kind
        || value.available_at() > common.calculated_at
        || value.expires_at() < common.expires_at
    {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Assumption,
        ));
    }
    Ok(())
}

/// Checked annual-period arithmetic only; these numbers do not mint valuation or source evidence.
pub struct AnnualEquityArithmetic;
impl AnnualEquityArithmetic {
    /// Recomputes a conditional FCFE-ratio/cap scenario without granting source authority.
    pub fn conditional_terminal_growth(
        final_fcfe: Decimal,
        next_fcfe: Decimal,
        nominal_risk_free_cap: Decimal,
        annual_cost_of_equity: Decimal,
    ) -> Result<(Decimal, Decimal), AutomaticValuationError> {
        terminal::conditional_terminal_growth(
            final_fcfe,
            next_fcfe,
            nominal_risk_free_cap,
            annual_cost_of_equity,
        )
    }
    /// Discounts one genuine annual amount at the supplied explicit annual rate.
    pub fn discounted_amount(
        amount: Decimal,
        annual_rate: Decimal,
        period: NonZeroU32,
    ) -> Result<Decimal, AutomaticValuationError> {
        let base = Decimal::ONE
            .checked_add(annual_rate)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(AutomaticValuationError::InvalidContract)?;
        discount(amount, base, period).map(|(_, value)| value)
    }
    /// Values genuine next-period common FCFE with a separately supplied continuation assumption.
    pub fn discounted_terminal_fcfe(
        next_period_fcfe: Decimal,
        annual_cost_of_equity: Decimal,
        annual_growth: Decimal,
        terminal_period: NonZeroU32,
    ) -> Result<Decimal, AutomaticValuationError> {
        terminal_fcfe_discount(
            next_period_fcfe,
            annual_cost_of_equity,
            annual_growth,
            terminal_period,
        )
        .map(|(_, value)| value)
    }
}

fn terminal_fcfe_discount(
    next_period_fcfe: Decimal,
    annual_cost_of_equity: Decimal,
    annual_growth: Decimal,
    terminal_period: NonZeroU32,
) -> Result<(Decimal, Decimal), AutomaticValuationError> {
    if next_period_fcfe <= Decimal::ZERO
        || annual_growth <= -Decimal::ONE
        || annual_growth >= annual_cost_of_equity
    {
        return Err(AutomaticValuationError::InvalidContract);
    }
    let continuation = annual_cost_of_equity
        .checked_sub(annual_growth)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let base = Decimal::ONE
        .checked_add(annual_cost_of_equity)
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(AutomaticValuationError::InvalidContract)?;
    let (discount_factor, _) = discount(Decimal::ONE, base, terminal_period)?;
    let divisor = continuation
        .checked_mul(discount_factor)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let value = next_period_fcfe
        .checked_div(divisor)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    Ok((divisor.normalize(), value.normalize()))
}

fn discount(
    amount: Decimal,
    base: Decimal,
    period: NonZeroU32,
) -> Result<(Decimal, Decimal), AutomaticValuationError> {
    let mut divisor = Decimal::ONE;
    for _ in 0..period.get() {
        divisor = divisor
            .checked_mul(base)
            .ok_or(AutomaticValuationError::Arithmetic)?;
    }
    let present_value = amount
        .checked_div(divisor)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    Ok((divisor.normalize(), present_value.normalize()))
}

fn probability_decimal(value: u32) -> Result<Decimal, AutomaticValuationError> {
    Decimal::from(value)
        .checked_div(Decimal::from(PROBABILITY_PARTS_PER_MILLION))
        .map(|value| value.normalize())
        .ok_or(AutomaticValuationError::Arithmetic)
}

fn input_decimal(value: &PointInTimeValuationInput) -> Decimal {
    value.input().amount().money().amount()
}

fn reserved_vec<T>(capacity: usize) -> Result<Vec<T>, AutomaticValuationError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| AutomaticValuationError::Arithmetic)?;
    Ok(values)
}

fn single_vec<T>(value: T) -> Result<Vec<T>, AutomaticValuationError> {
    let mut values = reserved_vec(1)?;
    values.push(value);
    Ok(values)
}

#[allow(clippy::too_many_arguments)]
fn intermediate(
    kind: AutomaticValuationIntermediateKind,
    sequence: u32,
    instrument_id: InstrumentId,
    primary_input: InputId,
    secondary_input: Option<InputId>,
    amount: Decimal,
    adjustment: Decimal,
    factor: Decimal,
    result: Decimal,
    evidence: EvidenceDigest,
) -> AutomaticValuationIntermediate {
    AutomaticValuationIntermediate {
        kind,
        sequence,
        instrument_id,
        primary_input,
        secondary_input,
        amount: amount.normalize(),
        adjustment: adjustment.normalize(),
        factor: factor.normalize(),
        result: result.normalize(),
        evidence,
    }
}

fn valuation_amount(
    common: &AutomaticValuationInput,
    value: Decimal,
) -> Result<ValuationAmount, AutomaticValuationError> {
    ValuationAmount::try_new(
        Money::new(value, common.currency),
        common.output_scale,
        common.amount_basis,
    )
    .map_err(|_| AutomaticValuationError::InvalidContract)
}

fn input_set_identity(
    inputs: &[PointInTimeValuationInput],
) -> Result<AutomaticValuationInputSetIdentity, AutomaticValuationError> {
    let mut hash = CanonicalHasher::new(b"market-squawk/automatic-valuation-input-set/v1");
    hash.u64(u64::try_from(inputs.len()).map_err(|_| AutomaticValuationError::Arithmetic)?);
    for value in inputs {
        hash.fixed(value.input().id().bytes());
        hash.fixed(value.input().evidence().hash().bytes());
        hash.fixed(value.selection_receipt().bytes());
        hash.fixed(value.rights_input_digest().bytes());
        hash.i64(value.knowledge_at().unix_nanos());
        hash.i64(value.expires_at().unix_nanos());
    }
    Ok(AutomaticValuationInputSetIdentity(hash.finish()))
}

fn hash_assumption(hash: &mut CanonicalHasher, value: &AutomaticValuationAssumption) {
    hash.u8(assumption_tag(value.kind()));
    hash.bytes(value.identifier().as_bytes());
    hash_decimal(hash, value.value());
    hash.fixed(value.evidence().bytes());
    hash.i64(value.available_at().unix_nanos());
    hash.i64(value.expires_at().unix_nanos());
}

fn hash_intermediate(hash: &mut CanonicalHasher, value: &AutomaticValuationIntermediate) {
    hash.u8(intermediate_tag(value.kind()));
    hash.u32(value.sequence());
    hash.bytes(value.instrument_id().as_uuid().as_bytes());
    hash.fixed(value.primary_input().bytes());
    match value.secondary_input() {
        Some(input) => {
            hash.u8(1);
            hash.fixed(input.bytes());
        }
        None => hash.u8(0),
    }
    hash_decimal(hash, value.amount());
    hash_decimal(hash, value.adjustment());
    hash_decimal(hash, value.factor());
    hash_decimal(hash, value.result());
    hash.fixed(value.evidence().bytes());
}

fn hash_optional_digest(hash: &mut CanonicalHasher, value: Option<EvidenceDigest>) {
    match value {
        Some(value) => {
            hash.u8(1);
            hash.fixed(value.bytes());
        }
        None => hash.u8(0),
    }
}

fn hash_decimal(hash: &mut CanonicalHasher, value: Decimal) {
    let value = value.normalize();
    hash.bytes(&value.mantissa().to_be_bytes());
    hash.u32(value.scale());
}

fn round(value: Decimal, scale: u8, policy: RoundingPolicy) -> Decimal {
    value
        .round_dp_with_strategy(u32::from(scale), rounding_strategy(policy))
        .normalize()
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_sha256(value: EvidenceDigest) -> bool {
    value.algorithm() == DigestAlgorithm::Sha256 && value.bytes() != [0; 32]
}

const fn method_tag(value: AutomaticValuationMethod) -> u8 {
    match value {
        AutomaticValuationMethod::DiscountedCashFlow => 1,
        AutomaticValuationMethod::ComparableCompanies => 2,
        AutomaticValuationMethod::ResidualIncome => 3,
        AutomaticValuationMethod::ForecastDistribution => 4,
    }
}

const fn assumption_tag(value: AutomaticValuationAssumptionKind) -> u8 {
    match value {
        AutomaticValuationAssumptionKind::DiscountRate => 1,
        AutomaticValuationAssumptionKind::ComparableWeight => 2,
        AutomaticValuationAssumptionKind::CostOfEquity => 3,
        AutomaticValuationAssumptionKind::ForecastProbability => 4,
        AutomaticValuationAssumptionKind::UncertaintyLower => 5,
        AutomaticValuationAssumptionKind::UncertaintyUpper => 6,
        AutomaticValuationAssumptionKind::TerminalGrowth => 7,
    }
}

const fn intermediate_tag(value: AutomaticValuationIntermediateKind) -> u8 {
    match value {
        AutomaticValuationIntermediateKind::DiscountedCashFlow => 1,
        AutomaticValuationIntermediateKind::DiscountedTerminalValue => 2,
        AutomaticValuationIntermediateKind::WeightedComparableMultiple => 3,
        AutomaticValuationIntermediateKind::ComparableSubjectValue => 4,
        AutomaticValuationIntermediateKind::DiscountedResidualIncome => 5,
        AutomaticValuationIntermediateKind::ProbabilityWeightedForecast => 6,
    }
}

const fn rounding_tag(value: RoundingPolicy) -> u8 {
    match value {
        RoundingPolicy::NearestEven => 1,
        RoundingPolicy::AwayFromZero => 2,
        RoundingPolicy::TowardZero => 3,
        RoundingPolicy::Floor => 4,
        RoundingPolicy::Ceiling => 5,
    }
}

const fn rounding_strategy(value: RoundingPolicy) -> RoundingStrategy {
    match value {
        RoundingPolicy::NearestEven => RoundingStrategy::MidpointNearestEven,
        RoundingPolicy::AwayFromZero => RoundingStrategy::AwayFromZero,
        RoundingPolicy::TowardZero => RoundingStrategy::ToZero,
        RoundingPolicy::Floor => RoundingStrategy::ToNegativeInfinity,
        RoundingPolicy::Ceiling => RoundingStrategy::ToPositiveInfinity,
    }
}
