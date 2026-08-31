//! Evidence-closed, deterministic automatic valuation calculation prerequisites.
//!
//! This module deliberately stops before fair-value measurement construction. A completed
//! calculation is research-only: it is not a crate::ValuationMeasurement, classification,
//! approval, recommendation, position, order, or execution authority. A future serialized adapter
//! must create a genuine derived crate::ValuationInput before the existing measurement,
//! classification, independent-approval, and latest-valid-selection authorities may be used.

use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_data::{
    CompanySecurityIdentityDisposition, CompanySecurityIdentitySelectionReceipt, ResearchUse,
    ResearchUseDecisionDigest, ResearchUseGraphDigest, ResearchUsePermit,
};
use market_squawk_domain::{
    AccountId, Currency, DigestAlgorithm, EvidenceDigest, IdentifierEntitlement, InstrumentId,
    Money, RoundingPolicy, Timestamp,
};
use rust_decimal::{Decimal, RoundingStrategy};
use thiserror::Error;

use crate::{
    ActorId, CanonicalHasher, EvidenceOrigin, EvidenceVerification, InputId,
    InputInstrumentRelation, ValuationAmount, ValuationAmountBasis, ValuationInput,
};

const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_METHOD_INPUTS: usize = 512;
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
    rights_graph: ResearchUseGraphDigest,
    knowledge_at: Timestamp,
    expires_at: Timestamp,
}

impl PointInTimeValuationInput {
    /// Binds one genuine producer input to an exact point-in-time selection.
    pub fn try_new(
        input: ValuationInput,
        selection_receipt: EvidenceDigest,
        rights_graph: ResearchUseGraphDigest,
        knowledge_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        let evidence = input.evidence();
        if !valid_sha256(selection_receipt)
            || evidence.verification() != EvidenceVerification::Verified
            || !evidence.producer_verification_is_current_at(knowledge_at)
            || evidence.available_at().is_none()
            || evidence
                .available_at()
                .is_some_and(|available_at| available_at > knowledge_at)
            || evidence.ingested_at() > knowledge_at
            || expires_at <= knowledge_at
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            input,
            selection_receipt,
            rights_graph,
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

    /// Returns the exact rights graph that admitted this selected input.
    pub const fn rights_graph(&self) -> ResearchUseGraphDigest {
        self.rights_graph
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

/// Single-use local-analysis authority retained until calculation completes.
#[derive(Debug)]
pub struct ValuationRightsReceipt {
    permit: ResearchUsePermit,
    decision_digest: ResearchUseDecisionDigest,
    graph_digest: ResearchUseGraphDigest,
    expires_at: Timestamp,
}

impl ValuationRightsReceipt {
    /// Consumes a catalog-issued permit into this one calculation request.
    pub fn try_from_permit(permit: ResearchUsePermit) -> Result<Self, AutomaticValuationError> {
        if permit.research_use() != ResearchUse::LocalAnalysis {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::Rights,
            ));
        }
        Ok(Self {
            decision_digest: permit.decision_digest(),
            graph_digest: permit.graph_digest(),
            expires_at: permit.expires_at(),
            permit,
        })
    }

    /// Returns the durable authorization decision identity.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.decision_digest
    }

    /// Returns the exact transitive source graph identity.
    pub const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.graph_digest
    }

    /// Returns exclusive permit expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

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
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Explicit cash-flow forecast; the calculator performs no forecasting.
    pub cash_flows: Vec<DcfCashFlow>,
    /// Caller-supplied per-period discount rate.
    pub discount_rate: AutomaticValuationAssumption,
    /// One-based terminal period.
    pub terminal_period: NonZeroU32,
    /// Explicit producer-derived terminal value; no growth rate is inferred.
    pub terminal_value: PointInTimeValuationInput,
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
    /// Shared identity, rights, current-market, unit, and time coordinates.
    pub common: AutomaticValuationInput,
    /// Exact current book value.
    pub current_book_value: PointInTimeValuationInput,
    /// Explicit forecast periods; the calculator performs no forecasting.
    pub periods: Vec<ResidualIncomePeriod>,
    /// Caller-supplied per-period cost of equity.
    pub cost_of_equity: AutomaticValuationAssumption,
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
    account_id: AccountId,
    instrument_id: InstrumentId,
    company_security: CompanySecurityIdentitySelectionReceipt,
    peer_identities: Box<[CompanySecurityIdentitySelectionReceipt]>,
    rights_decision: ResearchUseDecisionDigest,
    rights_graph: ResearchUseGraphDigest,
    current_market_input: InputId,
    inputs: Box<[PointInTimeValuationInput]>,
    assumptions: Box<[AutomaticValuationAssumption]>,
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

impl AutomaticValuationMethodReceipt {
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
    validate_assumption(
        &request.discount_rate,
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
        &request.terminal_value,
        &request.common,
        request.common.instrument_id,
    )?;
    let discount_base = Decimal::ONE
        .checked_add(request.discount_rate.value())
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
    for value in request.cash_flows {
        if value.period > request.terminal_period {
            return Err(AutomaticValuationError::InvalidContract);
        }
        validate_method_input(
            &value.cash_flow,
            &request.common,
            request.common.instrument_id,
        )?;
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
            request.discount_rate.evidence(),
        ));
        inputs.push(value.cash_flow);
    }
    let terminal_amount = input_decimal(&request.terminal_value);
    let (terminal_divisor, terminal_present_value) =
        discount(terminal_amount, discount_base, request.terminal_period)?;
    raw_value = raw_value
        .checked_add(terminal_present_value)
        .ok_or(AutomaticValuationError::Arithmetic)?;
    intermediates.push(intermediate(
        AutomaticValuationIntermediateKind::DiscountedTerminalValue,
        request.terminal_period.get(),
        request.common.instrument_id,
        request.terminal_value.input().id(),
        None,
        terminal_amount,
        Decimal::ZERO,
        terminal_divisor,
        terminal_present_value,
        request.discount_rate.evidence(),
    ));
    inputs.push(request.terminal_value);

    let assumptions = single_vec(request.discount_rate)?;
    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::DiscountedCashFlow,
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
    let mut weighted_multiple = Decimal::ZERO;
    let mut weight_sum = 0_u32;
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
        validate_assumption(
            &comparable.weight_assumption,
            AutomaticValuationAssumptionKind::ComparableWeight,
            &request.common,
        )?;
        if comparable.instrument_id == request.common.instrument_id
            || comparable.weight_ppm == 0
            || comparable.weight_ppm > PROBABILITY_PARTS_PER_MILLION
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::PeerSet,
            ));
        }
        let weight = probability_decimal(comparable.weight_ppm)?;
        if comparable.weight_assumption.value() != weight {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let metric = input_decimal(&comparable.metric);
        if metric == Decimal::ZERO {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::MethodInput,
            ));
        }
        let peer_value = input_decimal(&comparable.value);
        let multiple = peer_value
            .checked_div(metric)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        let contribution = multiple
            .checked_mul(weight)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        weighted_multiple = weighted_multiple
            .checked_add(contribution)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        weight_sum = weight_sum
            .checked_add(comparable.weight_ppm)
            .ok_or(AutomaticValuationError::Arithmetic)?;
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
    if weight_sum != PROBABILITY_PARTS_PER_MILLION {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::ProbabilityMass,
        ));
    }
    let subject_metric = input_decimal(&request.subject_metric);
    let raw_value = subject_metric
        .checked_mul(weighted_multiple)
        .ok_or(AutomaticValuationError::Arithmetic)?;
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
    inputs.push(request.subject_metric);

    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ComparableCompanies,
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
    validate_assumption(
        &request.cost_of_equity,
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
        .checked_add(request.cost_of_equity.value())
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

    let period_count = request.periods.len();
    let input_capacity = period_count
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or(AutomaticValuationError::Arithmetic)?;
    let mut raw_value = input_decimal(&request.current_book_value);
    let mut inputs = reserved_vec(input_capacity)?;
    let mut intermediates = reserved_vec(period_count)?;
    for period in request.periods {
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
        let net_income = input_decimal(&period.net_income);
        let equity_charge = input_decimal(&period.opening_book_value)
            .checked_mul(request.cost_of_equity.value())
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
            request.cost_of_equity.evidence(),
        ));
        inputs.push(period.net_income);
        inputs.push(period.opening_book_value);
    }
    inputs.push(request.current_book_value);

    let assumptions = single_vec(request.cost_of_equity)?;
    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ResidualIncome,
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
    let mut raw_value = Decimal::ZERO;
    let mut total_probability = 0_u32;
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
        validate_assumption(
            &point.probability_assumption,
            AutomaticValuationAssumptionKind::ForecastProbability,
            &request.common,
        )?;
        if point.probability_ppm == 0
            || point.probability_ppm > PROBABILITY_PARTS_PER_MILLION
            || point.probability_assumption.identifier() != &*point.point_id
        {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::ProbabilityMass,
            ));
        }
        let probability = probability_decimal(point.probability_ppm)?;
        if point.probability_assumption.value() != probability {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        let terminal_value = input_decimal(&point.terminal_value);
        let contribution = terminal_value
            .checked_mul(probability)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        raw_value = raw_value
            .checked_add(contribution)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        total_probability = total_probability
            .checked_add(point.probability_ppm)
            .ok_or(AutomaticValuationError::Arithmetic)?;
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
            contribution,
            point.probability_assumption.evidence(),
        ));
        inputs.push(point.terminal_value);
        assumptions.push(point.probability_assumption);
    }
    if total_probability != PROBABILITY_PARTS_PER_MILLION {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::ProbabilityMass,
        ));
    }

    finish(FinishInput {
        common: request.common,
        method: AutomaticValuationMethod::ForecastDistribution,
        raw_value,
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
    common: AutomaticValuationInput,
    method: AutomaticValuationMethod,
    raw_value: Decimal,
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
    validate_assumption(
        request.uncertainty.lower(),
        AutomaticValuationAssumptionKind::UncertaintyLower,
        &request.common,
    )?;
    validate_assumption(
        request.uncertainty.upper(),
        AutomaticValuationAssumptionKind::UncertaintyUpper,
        &request.common,
    )?;
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
    let _consumed_single_use_permit = request.common.rights.permit;
    let mut hash = CanonicalHasher::new(b"market-squawk/automatic-valuation-calculation/v1");
    hash.u8(method_tag(request.method));
    hash.bytes(request.common.account_id.as_uuid().as_bytes());
    hash.bytes(request.common.instrument_id.as_uuid().as_bytes());
    hash.fixed(request.common.company_security.receipt_digest().bytes());
    hash.fixed(rights_decision.bytes());
    hash.fixed(rights_graph.bytes());
    hash.fixed(current_market_input.bytes());
    hash.fixed(input_set_id.bytes());
    central.hash_into(&mut hash);
    range.lower.hash_into(&mut hash);
    range.upper.hash_into(&mut hash);
    hash.u8(rounding_tag(request.common.arithmetic_policy.rounding()));
    hash.u64(
        u64::try_from(request.common.arithmetic_policy.maximum_periods())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    hash.u64(
        u64::try_from(request.peer_identities.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for receipt in &request.peer_identities {
        hash.fixed(receipt.receipt_digest().bytes());
    }
    hash.u64(
        u64::try_from(request.assumptions.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for assumption in &request.assumptions {
        hash_assumption(&mut hash, assumption);
    }
    hash.u64(
        u64::try_from(request.intermediates.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?,
    );
    for value in &request.intermediates {
        hash_intermediate(&mut hash, value);
    }
    hash_optional_digest(&mut hash, request.method_selection_receipt);
    match request.forecast_horizon_nanos {
        Some(value) => {
            hash.u8(1);
            hash.u64(value.get());
        }
        None => hash.u8(0),
    }
    match request.forecast_terminal_at {
        Some(value) => {
            hash.u8(1);
            hash.i64(value.unix_nanos());
        }
        None => hash.u8(0),
    }
    hash.i64(request.common.measurement_at.unix_nanos());
    hash.i64(request.common.calculated_at.unix_nanos());
    hash.bytes(request.common.calculated_by.as_str().as_bytes());
    hash.i64(request.common.expires_at.unix_nanos());

    Ok(AutomaticValuationCalculation {
        receipt: AutomaticValuationMethodReceipt {
            id: AutomaticValuationIdentity(hash.finish()),
            input_set_id,
            method: request.method,
            account_id: request.common.account_id,
            instrument_id: request.common.instrument_id,
            company_security: request.common.company_security,
            peer_identities: request.peer_identities.into_boxed_slice(),
            rights_decision,
            rights_graph,
            current_market_input,
            inputs: request.inputs.into_boxed_slice(),
            assumptions: request.assumptions.into_boxed_slice(),
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
        },
    })
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
    if market.knowledge_at() != common.measurement_at
        || market.expires_at() < common.expires_at
        || market.rights_graph() != common.rights.graph_digest()
        || input.subject_instrument_id() != common.instrument_id
        || input.reference_instrument_id() != common.instrument_id
        || input.relationship() != InputInstrumentRelation::Identical
        || input.amount().money().currency() != common.currency
        || input.amount().basis() != ValuationAmountBasis::PerInstrumentUnit
        || !matches!(input.evidence().origin(), EvidenceOrigin::Market { .. })
        || !input
            .evidence()
            .producer_verification_is_current_at(common.measurement_at)
        || input
            .evidence()
            .source_timestamp()
            .is_none_or(|value| value > common.measurement_at)
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
    if value.knowledge_at() != common.measurement_at || value.expires_at() < common.expires_at {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::MethodInput,
        ));
    }
    if value.rights_graph() != common.rights.graph_digest() {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Rights,
        ));
    }
    let input = value.input();
    if !input
        .evidence()
        .producer_verification_is_current_at(common.measurement_at)
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

fn validate_assumption(
    value: &AutomaticValuationAssumption,
    kind: AutomaticValuationAssumptionKind,
    common: &AutomaticValuationInput,
) -> Result<(), AutomaticValuationError> {
    if value.kind() != kind {
        return Err(AutomaticValuationError::Conflict(
            AutomaticValuationConflict::Evidence,
        ));
    }
    if value.available_at() > common.measurement_at || value.expires_at() < common.expires_at {
        return Err(AutomaticValuationError::Unavailable(
            AutomaticValuationUnavailable::Assumption,
        ));
    }
    Ok(())
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
        hash.fixed(value.rights_graph().bytes());
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
