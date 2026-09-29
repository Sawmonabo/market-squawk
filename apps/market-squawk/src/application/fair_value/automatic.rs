//! Bounded source selection, observed comparable valuation, and durable calculation publication.

mod financial;
mod investment;
pub(crate) use investment::{
    AutomaticInvestmentValuationEvaluation, AutomaticInvestmentValuationRequest,
    AutomaticInvestmentValuationSources,
};
mod forecast;
mod historical;
mod macro_context;
pub(crate) use forecast::AutomaticForecastValuationRequest;
pub(crate) use historical::{
    HistoricalForecastValuationAssumption, HistoricalForecastValuationReceipt,
    HistoricalStudyValuationReadCapability, HistoricalValuationMethodEvaluation,
};
pub(crate) use macro_context::ReopenedAutomaticMacroContext;

use std::{sync::Arc, time::Duration};

use market_squawk_data::{
    CompanySecurityIdentityDisposition, CompanySecurityIdentityQuery,
    CompanySecurityIdentitySelectionReceipt, DatasetManifestRef, IndustryClassificationScheme,
    IndustryClassificationVersion, IndustryCohortCompleteness, PointInTimeLimits,
    PointInTimeRevisionMode, ResearchUse, ResearchUseLimits, ResearchUseRequest, SecResearchFamily,
    SecResearchIdentityOutcome, SecResearchIdentityReadRequest,
};
use market_squawk_domain::{
    CalendarDate, CompanyIdentitySurface, DigestAlgorithm, EvidenceDigest, FundamentalCadence,
    FundamentalObservation, FundamentalPeriod, ResearchObservation, ResearchTemporalCoordinate,
    RoundingPolicy, SourceId,
};
use market_squawk_valuation::{
    AutomaticValuationAssumption, AutomaticValuationAssumptionKind, AutomaticValuationInput,
    AutomaticValuationMethodReceipt, AutomaticValuationUncertainty,
    ComparableCompaniesValuationRequest, ComparableCompanyInput, ComparableValueArithmetic,
    PointInTimeValuationInput, ValuationArithmeticPolicy, ValuationRightsReceipt,
    calculate_comparable_companies,
};
use sha2::{Digest as _, Sha256};

use super::*;
use crate::ResearchService;
use crate::application::market_selection::{
    MarketInvestmentReadCapability, MarketInvestmentReadReceipt,
};

const MAXIMUM_PEERS: usize = 16;
const SOURCE_READ_BYTES: usize = 16 * 1024 * 1024;
const SOURCE_READ_ROWS: usize = 100_000;
const POLICY: &[u8] = b"market-squawk/observed-annual-diluted-eps-comparables/v1";

/// Independent financial primitive; profile and workflow orchestration stay with its caller.
#[derive(Clone, Debug)]
pub(crate) struct ObservedComparableValuationRequest {
    pub(crate) account_id: AccountId,
    pub(crate) subject: InstrumentId,
    /// Empty selects the complete bounded industry cohort; an explicit set remains exact.
    pub(crate) peers: Vec<InstrumentId>,
    pub(crate) knowledge_at: Timestamp,
    pub(crate) effective_date: CalendarDate,
    pub(crate) expires_at: Timestamp,
    pub(crate) calculated_by: ActorId,
}

/// Exact retained calculation and its separate, unapproved accounting classification.
#[derive(Clone, Debug)]
pub(crate) struct AutomaticValuationPublication {
    pub(crate) receipt: AutomaticValuationMethodReceipt,
    pub(crate) measurement_id: MeasurementId,
    pub(crate) decision_id: DecisionId,
    pub(crate) product: Value,
}

/// Reproducible method cases and sensitivity from a retained calculation.
#[derive(Clone, Debug)]
pub(crate) struct AutomaticValuationModelCases {
    calculation_identity: market_squawk_valuation::AutomaticValuationIdentity,
    scenarios: market_squawk_decisions::TargetPriceCases,
    scenario_identity: market_squawk_decisions::DecisionContentDigest,
    sensitivity_range: market_squawk_decisions::TargetPriceRange,
    sensitivity_identity: market_squawk_decisions::DecisionContentDigest,
}

impl AutomaticValuationModelCases {
    pub(crate) const fn calculation_identity(
        &self,
    ) -> market_squawk_valuation::AutomaticValuationIdentity {
        self.calculation_identity
    }
    pub(crate) const fn scenarios(&self) -> market_squawk_decisions::TargetPriceCases {
        self.scenarios
    }
    pub(crate) const fn scenario_identity(&self) -> market_squawk_decisions::DecisionContentDigest {
        self.scenario_identity
    }
    pub(crate) const fn sensitivity_range(&self) -> market_squawk_decisions::TargetPriceRange {
        self.sensitivity_range
    }
    pub(crate) const fn sensitivity_identity(
        &self,
    ) -> market_squawk_decisions::DecisionContentDigest {
        self.sensitivity_identity
    }
}

/// Recomputes method cases and sensitivity without a new source read.
///
/// Comparables apply each peer to the actual subject metric and remove one peer for sensitivity.
/// Forecast distributions use their empirical support and remove one outcome for sensitivity.
/// Each remaining set retains its original relative weights, normalized to one.
pub(crate) fn automatic_valuation_model_cases(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<AutomaticValuationModelCases, ServiceError> {
    if receipt.method() == market_squawk_valuation::AutomaticValuationMethod::ForecastDistribution {
        return forecast::forecast_model_cases(receipt);
    }
    use market_squawk_valuation::{
        AutomaticValuationIntermediateKind as Step, AutomaticValuationMethod,
    };
    if receipt.method() != AutomaticValuationMethod::ComparableCompanies
        || receipt.range().central().basis() != ValuationAmountBasis::PerInstrumentUnit
    {
        return Err(ServiceError::Unavailable);
    }
    let subject = receipt
        .intermediates()
        .iter()
        .filter(|value| value.kind() == Step::ComparableSubjectValue)
        .collect::<Vec<_>>();
    let [subject] = subject.as_slice() else {
        return Err(ServiceError::InvalidResult);
    };
    let peers = receipt
        .intermediates()
        .iter()
        .filter(|value| value.kind() == Step::WeightedComparableMultiple)
        .collect::<Vec<_>>();
    if !(2..=MAXIMUM_PEERS).contains(&peers.len()) {
        return Err(ServiceError::Unavailable);
    }
    let sum = peers.iter().try_fold(Decimal::ZERO, |sum, peer| {
        sum.checked_add(peer.result())
            .ok_or(ServiceError::InvalidResult)
    })?;
    let mass = peers.iter().try_fold(Decimal::ZERO, |sum, peer| {
        sum.checked_add(peer.factor())
            .ok_or(ServiceError::InvalidResult)
    })?;
    if mass != Decimal::ONE
        || sum != subject.adjustment()
        || subject.instrument_id() != receipt.instrument_id()
        || subject.amount() <= Decimal::ZERO
    {
        return Err(ServiceError::InvalidResult);
    }
    let currency = receipt.range().central().money().currency();
    let mut scenario_values = Vec::with_capacity(peers.len());
    let mut sensitivity_values = Vec::with_capacity(peers.len());
    let mut scenario_hash = Sha256::new();
    scenario_hash.update(b"market-squawk/comparable-observed-peer-cases/v1");
    scenario_hash.update(receipt.id().bytes());
    let mut sensitivity_hash = Sha256::new();
    sensitivity_hash.update(b"market-squawk/comparable-leave-one-peer-out/v1");
    sensitivity_hash.update(receipt.id().bytes());
    for peer in peers {
        if peer.factor() <= Decimal::ZERO || peer.factor() >= Decimal::ONE {
            return Err(ServiceError::InvalidResult);
        }
        let implied = subject
            .amount()
            .checked_mul(peer.adjustment())
            .ok_or(ServiceError::InvalidResult)?;
        let remaining = sum
            .checked_sub(peer.result())
            .ok_or(ServiceError::InvalidResult)?;
        let remaining_mass = Decimal::ONE
            .checked_sub(peer.factor())
            .ok_or(ServiceError::InvalidResult)?;
        let sensitivity = remaining
            .checked_div(remaining_mass)
            .and_then(|value| value.checked_mul(subject.amount()))
            .ok_or(ServiceError::InvalidResult)?;
        let implied = round_method_value(receipt, implied);
        let sensitivity = round_method_value(receipt, sensitivity);
        for (hash, value) in [
            (&mut scenario_hash, implied),
            (&mut sensitivity_hash, sensitivity),
        ] {
            hash.update(peer.primary_input().bytes());
            hash.update(value.mantissa().to_be_bytes());
            hash.update(value.scale().to_be_bytes());
        }
        scenario_values.push(implied);
        sensitivity_values.push(sensitivity);
    }
    let scenario_lower = *scenario_values
        .iter()
        .min()
        .ok_or(ServiceError::Unavailable)?;
    let scenario_upper = *scenario_values
        .iter()
        .max()
        .ok_or(ServiceError::Unavailable)?;
    let sensitivity_lower = *sensitivity_values
        .iter()
        .min()
        .ok_or(ServiceError::Unavailable)?;
    let sensitivity_upper = *sensitivity_values
        .iter()
        .max()
        .ok_or(ServiceError::Unavailable)?;
    let scenarios = market_squawk_decisions::TargetPriceCases::try_new(
        Money::new(scenario_lower, currency),
        receipt.range().central().money(),
        Money::new(scenario_upper, currency),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let sensitivity_range = market_squawk_decisions::TargetPriceRange::try_new(
        Money::new(sensitivity_lower, currency),
        Money::new(sensitivity_upper, currency),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let identity = |hash: Sha256| {
        market_squawk_decisions::DecisionContentDigest::try_new(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hash.finalize().into(),
        ))
        .map_err(|_| ServiceError::InvalidResult)
    };
    Ok(AutomaticValuationModelCases {
        calculation_identity: receipt.id(),
        scenarios,
        scenario_identity: identity(scenario_hash)?,
        sensitivity_range,
        sensitivity_identity: identity(sensitivity_hash)?,
    })
}

fn round_method_value(receipt: &AutomaticValuationMethodReceipt, value: Decimal) -> Decimal {
    use rust_decimal::RoundingStrategy;
    let strategy = match receipt.arithmetic_policy().rounding() {
        RoundingPolicy::NearestEven => RoundingStrategy::MidpointNearestEven,
        RoundingPolicy::AwayFromZero => RoundingStrategy::AwayFromZero,
        RoundingPolicy::TowardZero => RoundingStrategy::ToZero,
        RoundingPolicy::Floor => RoundingStrategy::ToNegativeInfinity,
        RoundingPolicy::Ceiling => RoundingStrategy::ToPositiveInfinity,
    };
    value.round_dp_with_strategy(u32::from(receipt.range().central().scale()), strategy)
}

enum ComparableMarketRead<'a> {
    Borrowed(&'a MarketInvestmentReadReceipt),
    Selected(MarketInvestmentReadReceipt),
}
impl ComparableMarketRead<'_> {
    fn receipt(&self) -> &MarketInvestmentReadReceipt {
        match self {
            Self::Borrowed(value) => value,
            Self::Selected(value) => value,
        }
    }
}
struct SelectedComparable<'a> {
    fundamentals: SelectedComparableFundamentals,
    market: ValuationInput,
    market_selection: ComparableMarketRead<'a>,
}

struct SelectedComparableFundamentals {
    identity: CompanySecurityIdentitySelectionReceipt,
    metric: ValuationInput,
    fact: FundamentalObservation,
    period: FundamentalPeriod,
    industry: Box<str>,
    industry_evidence: EvidenceDigest,
    industry_source: SourceId,
    industry_surface: CompanyIdentitySurface,
    roots: Vec<DatasetManifestRef>,
}

impl FairValueDomainService {
    /// Calculates from actual annual facts, compatible peers, and exact published market prices.
    ///
    /// Weights are deterministic equal parts; bounds are the observed peer-implied minimum and
    /// maximum, not a confidence interval. Missing facts, source rights, exact price publication,
    /// industry evidence, compatible reporting periods, or positive earnings fail closed.
    pub(crate) async fn calculate_observed_comparables(
        &self,
        research: Arc<ResearchService>,
        market_reader: &MarketInvestmentReadCapability,
        final_market: &MarketInvestmentReadReceipt,
        mut request: ObservedComparableValuationRequest,
        context: &RequestContext,
    ) -> Result<AutomaticValuationPublication, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        if (!request.peers.is_empty() && !(2..=MAXIMUM_PEERS).contains(&request.peers.len()))
            || request.expires_at <= request.knowledge_at
            || i64::from(request.effective_date.days_since_unix_epoch())
                > request
                    .knowledge_at
                    .unix_nanos()
                    .div_euclid(86_400_000_000_000)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let market_cutoff = final_market.reference().source_cutoff()?;
        if final_market.reference().instrument_id() != request.subject
            || market_cutoff < request.knowledge_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        request.peers.sort_unstable();
        if request.peers.iter().any(|peer| *peer == request.subject)
            || request.peers.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(ServiceError::InvalidRequest);
        }
        let subject = self
            .select_comparable(
                &research,
                ComparableMarketRead::Borrowed(final_market),
                request.subject,
                &request,
                context,
            )
            .await?;
        let cohort_evidence = if request.peers.is_empty() {
            let (peers, evidence) =
                discover_peers(&research, &subject.fundamentals, &request, context).await?;
            request.peers = peers;
            Some(evidence)
        } else {
            None
        };
        let mut peers = Vec::with_capacity(request.peers.len());
        for peer in &request.peers {
            let market = market_reader
                .read(
                    *peer,
                    market_cutoff,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?
                .ok_or(ServiceError::Unavailable)?;
            let selected = self
                .select_comparable(
                    &research,
                    ComparableMarketRead::Selected(market),
                    *peer,
                    &request,
                    context,
                )
                .await?;
            if selected.fundamentals.industry != subject.fundamentals.industry
                || selected.fundamentals.period != subject.fundamentals.period
                || selected.fundamentals.metric.amount().money().currency()
                    != subject.fundamentals.metric.amount().money().currency()
            {
                return Err(ServiceError::Unavailable);
            }
            peers.push(selected);
        }
        let mut roots = Vec::new();
        let mut expires_at = request.expires_at;
        for selected in std::iter::once(&subject).chain(peers.iter()) {
            market_reader
                .recheck(
                    selected.market_selection.receipt(),
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?;
            expires_at = expires_at.min(
                selected
                    .market_selection
                    .receipt()
                    .observation()
                    .map_err(|_| ServiceError::InvalidResult)?
                    .mark()
                    .fresh_until()
                    .ok_or(ServiceError::Unavailable)?,
            );
            for root in &selected.fundamentals.roots {
                if !roots.contains(root) {
                    roots.push(root.clone());
                }
            }
            for identity in selected.fundamentals.identity.ordered_candidates() {
                for end in [
                    identity.effective_end(),
                    identity.market_effective_end(),
                    identity.current_market_effective_end(),
                ]
                .into_iter()
                .flatten()
                {
                    expires_at = expires_at.min(end);
                }
            }
        }
        ensure_request_live(context, &self.lifecycle)?;
        let limits = ResearchUseLimits::try_new(
            64,
            4096,
            8192,
            4096,
            4 * 1024 * 1024,
            context
                .deadline()
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5)),
            Duration::from_secs(300),
        )
        .map_err(|_| ServiceError::Internal)?;
        let authorization = research
            .authorize_research_use(
                ResearchUseRequest::try_new(roots, ResearchUse::LocalAnalysis, limits)
                    .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| map_research_use_worker_error(error, context))?
            .map_err(|error| map_research_use_error(error, context))?;
        expires_at = expires_at.min(authorization.expires_at());
        let rights = ValuationRightsReceipt::try_from_authorization(authorization)
            .map_err(|_| ServiceError::Unavailable)?;
        let graph = rights.graph_digest();
        let subject_value = subject.fundamentals.metric.amount().money().amount();
        let output_scale = subject.market.amount().scale();
        let peer_count = u32::try_from(peers.len()).map_err(|_| ServiceError::Internal)?;
        let arithmetic = observed_comparable_arithmetic(
            subject_value,
            peers.iter().map(|peer| {
                (
                    peer.market.amount().money().amount(),
                    peer.fundamentals.metric.amount().money().amount(),
                )
            }),
        )?;
        let assumption_at = calculation_clock()?;
        let mut comparables = Vec::with_capacity(peers.len());
        let mut policy = Sha256::new();
        policy.update(POLICY);
        policy.update(subject.fundamentals.identity.receipt_digest().bytes());
        policy.update(subject.fundamentals.metric.id().bytes());
        policy.update(subject.fundamentals.industry_evidence.bytes());
        if let Some(evidence) = cohort_evidence {
            policy.update([1]);
            policy.update(evidence.bytes());
        } else {
            policy.update([0]);
        }
        for (index, peer) in peers.into_iter().enumerate() {
            let ordinal = u32::try_from(index).map_err(|_| ServiceError::Internal)?;
            let weight_ppm = 1_000_000 / peer_count + u32::from(ordinal < 1_000_000 % peer_count);
            let mut weight_evidence = Sha256::new();
            weight_evidence.update(POLICY);
            if let Some(evidence) = cohort_evidence {
                weight_evidence.update([1]);
                weight_evidence.update(evidence.bytes());
            } else {
                weight_evidence.update([0]);
            }
            weight_evidence.update(subject.fundamentals.identity.receipt_digest().bytes());
            weight_evidence.update(peer.fundamentals.identity.receipt_digest().bytes());
            weight_evidence.update(peer.fundamentals.metric.id().bytes());
            weight_evidence.update(peer.market.id().bytes());
            weight_evidence.update(peer.fundamentals.industry_evidence.bytes());
            weight_evidence.update(peer_count.to_be_bytes());
            weight_evidence.update(ordinal.to_be_bytes());
            weight_evidence.update(weight_ppm.to_be_bytes());
            let digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, weight_evidence.finalize().into());
            policy.update(digest.bytes());
            comparables.push(ComparableCompanyInput {
                instrument_id: peer.fundamentals.metric.reference_instrument_id(),
                company_security: peer.fundamentals.identity,
                metric: point_input(
                    peer.fundamentals.metric,
                    graph,
                    request.knowledge_at,
                    expires_at,
                )?,
                value: point_input(peer.market, graph, market_cutoff, expires_at)?,
                weight_ppm,
                weight_assumption: assumption(
                    AutomaticValuationAssumptionKind::ComparableWeight,
                    &format!("equal_peer_weight_{ordinal}"),
                    Decimal::from_i128_with_scale(i128::from(weight_ppm), 6),
                    digest,
                    assumption_at,
                    expires_at,
                )?,
            });
        }
        let lower = arithmetic.lower();
        let upper = arithmetic.upper();
        policy.update(b"outward-interval-rounding-floor-ceiling/v1");
        policy.update([output_scale]);
        for bound in [lower, upper] {
            policy.update(bound.mantissa().to_be_bytes());
            policy.update(bound.scale().to_be_bytes());
        }
        let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, policy.finalize().into());
        let lower = lower.round_dp_with_strategy(
            u32::from(output_scale),
            rust_decimal::RoundingStrategy::ToNegativeInfinity,
        );
        let upper = upper.round_dp_with_strategy(
            u32::from(output_scale),
            rust_decimal::RoundingStrategy::ToPositiveInfinity,
        );
        let uncertainty = AutomaticValuationUncertainty::try_new(
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyLower,
                "observed_peer_minimum",
                lower,
                evidence,
                assumption_at,
                expires_at,
            )?,
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyUpper,
                "observed_peer_maximum",
                upper,
                evidence,
                assumption_at,
                expires_at,
            )?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let calculated_at = calculation_clock()?;
        if expires_at <= calculated_at {
            return Err(ServiceError::Unavailable);
        }
        ensure_request_live(context, &self.lifecycle)?;
        let currency = subject.market.amount().money().currency();
        let calculation = calculate_comparable_companies(ComparableCompaniesValuationRequest {
            common: AutomaticValuationInput {
                account_id: request.account_id,
                company_security: subject.fundamentals.identity,
                instrument_id: request.subject,
                currency,
                amount_basis: ValuationAmountBasis::PerInstrumentUnit,
                current_market: point_input(subject.market, graph, market_cutoff, expires_at)?,
                rights,
                measurement_at: request.knowledge_at,
                calculated_at,
                expires_at,
                calculated_by: request.calculated_by,
                output_scale,
                arithmetic_policy: ValuationArithmeticPolicy::try_new(
                    RoundingPolicy::NearestEven,
                    MAXIMUM_PEERS,
                )
                .map_err(|_| ServiceError::Internal)?,
            },
            subject_metric: point_input(
                subject.fundamentals.metric,
                graph,
                request.knowledge_at,
                expires_at,
            )?,
            comparables,
            uncertainty,
        })
        .map_err(|_| ServiceError::Unavailable)?;
        self.publish_automatic_calculation(calculation, context)
            .await
    }

    async fn publish_automatic_calculation(
        &self,
        calculation: market_squawk_valuation::AutomaticValuationCalculation,
        context: &RequestContext,
    ) -> Result<AutomaticValuationPublication, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let calculation = calculation
            .completed_at(calculation_clock()?)
            .map_err(|_| ServiceError::Unavailable)?;
        let receipt = calculation.receipt().clone();
        let input =
            ValuationInput::from_automatic_calculation(calculation, InputSignificance::Significant)
                .map_err(map_fair_value_error)?;
        let measurement = ValuationMeasurement::try_new(ValuationMeasurementSpec {
            account_id: receipt.account_id(),
            instrument_id: receipt.instrument_id(),
            amount: receipt.range().central(),
            // This accounting measurement exists when the calculation completes. Its full method
            // receipt separately retains the original source knowledge cutoff.
            measurement_at: receipt.calculated_at(),
            prepared_at: receipt.calculated_at(),
            prepared_by: receipt.calculated_by().clone(),
            method: match receipt.method() {
                market_squawk_valuation::AutomaticValuationMethod::ComparableCompanies => {
                    ValuationMethod::MarketApproach
                }
                market_squawk_valuation::AutomaticValuationMethod::DiscountedCashFlow
                | market_squawk_valuation::AutomaticValuationMethod::ResidualIncome
                | market_squawk_valuation::AutomaticValuationMethod::ForecastDistribution => {
                    ValuationMethod::IncomeApproach
                }
            },
            inputs: vec![input],
        })
        .map_err(map_fair_value_error)?;
        let mut state = self.lock_state(context).await?;
        if calculation_clock()? >= receipt.expires_at() {
            return Err(ServiceError::Unavailable);
        }
        let decision = state
            .classify(measurement, self.ruleset.clone())
            .map_err(map_fair_value_error)?;
        let retained = state
            .measurement(decision.measurement_id())
            .ok_or(ServiceError::Internal)?;
        drop(state);
        let mut tokens = self.workflow_tokens.lock().await;
        let product = product_measurement_detail(&mut tokens, &retained, Some(&decision), &[])?;
        Ok(AutomaticValuationPublication {
            receipt,
            measurement_id: retained.id(),
            decision_id: decision.id(),
            product,
        })
    }

    /// Admits one exact retained research measurement for fresh recommendation analysis.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact retained source and consumer coordinates remain explicit"
    )]
    pub(crate) async fn read_research_valuation(
        &self,
        research: &ResearchService,
        measurement_id: MeasurementId,
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: market_squawk_domain::Currency,
        horizon_at: Timestamp,
        context: &RequestContext,
    ) -> Result<market_squawk_decisions::ValuationEvidence, ServiceError> {
        let state = self.lock_state(context).await?;
        let measurement = state
            .measurement(measurement_id)
            .ok_or(ServiceError::NotFound)?;
        drop(state);
        let [input] = measurement.inputs() else {
            return Err(ServiceError::InvalidResult);
        };
        let EvidenceOrigin::AutomaticValuation { receipt } = input.evidence().origin() else {
            return Err(ServiceError::NotFound);
        };
        ensure_request_live(context, &self.lifecycle)?;
        if calculation_clock()? >= receipt.expires_at() {
            return Err(ServiceError::Unavailable);
        }
        if receipt.admitted_input_manifests().len() > market_squawk_data::MAX_RESEARCH_USE_ROOTS {
            return Err(ServiceError::ResourceExhausted);
        }
        let remaining = context
            .deadline()
            .checked_duration_since(std::time::Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let limits = ResearchUseLimits::try_new(
            market_squawk_data::MAX_RESEARCH_USE_ROOTS,
            4096,
            8192,
            4096,
            4 * 1024 * 1024,
            remaining.min(Duration::from_secs(5)),
            Duration::from_secs(300),
        )
        .map_err(|_| ServiceError::Internal)?;
        let authorization = research
            .authorize_research_use(
                ResearchUseRequest::try_new(
                    receipt.admitted_input_manifests().to_vec(),
                    ResearchUse::LocalAnalysis,
                    limits,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| map_research_use_worker_error(error, context))?
            .map_err(|error| map_research_use_error(error, context))?;
        ensure_request_live(context, &self.lifecycle)?;
        let admitted_at = calculation_clock()?;
        let evidence = market_squawk_decisions::ValuationEvidence::try_from_automatic_measurement(
            &measurement,
            account_id,
            instrument_id,
            currency,
            horizon_at,
            admitted_at,
            receipt.expires_at().min(authorization.expires_at()),
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let _consumed = authorization.into_permit();
        Ok(evidence)
    }

    /// Reopens one exact source-owned calculation into the two existing investment evidence roles.
    #[allow(
        clippy::too_many_arguments,
        reason = "genuine source capabilities and exact consumer identities stay explicit"
    )]
    pub(crate) async fn read_automatic_investment_evidence(
        &self,
        research: &ResearchService,
        macro_reader: &crate::application::research::MacroContextReadCapability,
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        measurement_id: MeasurementId,
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: market_squawk_domain::Currency,
        horizon_at: Timestamp,
        context: &RequestContext,
    ) -> Result<
        (
            market_squawk_decisions::ValuationEvidence,
            market_squawk_decisions::FinancialModelEvidence,
        ),
        ServiceError,
    > {
        let valuation = self
            .read_research_valuation(
                research,
                measurement_id,
                account_id,
                instrument_id,
                currency,
                horizon_at,
                context,
            )
            .await?;
        let receipt = self
            .read_automatic_valuation(measurement_id, context)
            .await?;
        let cases = automatic_valuation_model_cases(&receipt)?;
        let reopened_macro = self
            .read_automatic_macro_context(
                measurement_id,
                macro_reader,
                calendars,
                research,
                context,
            )
            .await?;
        let expires_at = reopened_macro
            .as_ref()
            .map_or(valuation.window().expires_at(), |source| {
                valuation.window().expires_at().min(source.expires_at())
            });
        let window = market_squawk_decisions::ProposalEvidenceWindow::try_from_derived(
            receipt.measurement_at(),
            receipt.measurement_at(),
            receipt.calculated_at(),
            expires_at,
            market_squawk_decisions::DecisionContentDigest::try_new(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                receipt.id().bytes(),
            ))
            .map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let model = crate::application::decision::recommendation::adapt_financial_model_evidence(
            &receipt,
            reopened_macro.as_ref().map(|value| value.context()),
            receipt.macro_assumptions().cloned(),
            cases.scenarios(),
            cases.scenario_identity(),
            cases.sensitivity_range(),
            cases.sensitivity_identity(),
            horizon_at,
            window,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        ensure_request_live(context, &self.lifecycle)?;
        if calculation_clock()? >= expires_at {
            return Err(ServiceError::Unavailable);
        }
        Ok((valuation, model))
    }

    async fn select_comparable<'a>(
        &self,
        research: &ResearchService,
        market_selection: ComparableMarketRead<'a>,
        instrument_id: InstrumentId,
        request: &ObservedComparableValuationRequest,
        context: &RequestContext,
    ) -> Result<SelectedComparable<'a>, ServiceError> {
        let mut fundamentals = self
            .select_comparable_fundamentals(research, instrument_id, request, context)
            .await?;
        if market_selection.receipt().reference().instrument_id() != instrument_id {
            return Err(ServiceError::InvalidRequest);
        }
        fundamentals
            .roots
            .push(market_selection.receipt().publication().manifest().clone());
        let market = ValuationInput::from_published_market_selection(
            market_selection.receipt().publication(),
            market_selection.receipt().market_definitions(),
            market_selection.receipt().instrument_definitions(),
            InputSignificance::Significant,
        )
        .map_err(map_fair_value_error)?;
        if market.amount().money().currency() != fundamentals.metric.amount().money().currency()
            || market.amount().money().amount() <= Decimal::ZERO
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(SelectedComparable {
            fundamentals,
            market,
            market_selection,
        })
    }

    async fn select_comparable_fundamentals(
        &self,
        research: &ResearchService,
        instrument_id: InstrumentId,
        request: &ObservedComparableValuationRequest,
        context: &RequestContext,
    ) -> Result<SelectedComparableFundamentals, ServiceError> {
        let reader = research.analytical().sec_research_reader();
        let store = research.provider_capture_store();
        let pit = PointInTimeLimits::try_new(
            SOURCE_READ_ROWS,
            SOURCE_READ_ROWS,
            1024,
            SOURCE_READ_ROWS,
            SOURCE_READ_BYTES,
        )
        .map_err(|_| ServiceError::Internal)?;
        let read_request = |family| {
            SecResearchIdentityReadRequest::try_new(
                instrument_id,
                family,
                request.knowledge_at,
                ResearchTemporalCoordinate::calendar_date(request.effective_date),
                PointInTimeRevisionMode::LatestKnown,
                pit,
                SOURCE_READ_BYTES,
            )
            .map_err(|_| ServiceError::InvalidRequest)
        };
        let facts = reader
            .select_by_identity(
                read_request(SecResearchFamily::CompanyFacts)?,
                &store,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_sec_research_error)?;
        let SecResearchIdentityOutcome::Exact(selected) = facts.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        let (row, observation) = select_annual_eps(selected)?;
        let period = observation.fact_context().period();
        let fact = observation.clone();
        let metric =
            ValuationInput::from_selected_fundamental(&facts, row, InputSignificance::Significant)
                .map_err(map_fair_value_error)?;
        let identity = facts.identity().receipt().clone();
        let company_id = selected
            .company_identity()
            .observation()
            .provider_company_id()
            .clone();
        let mut roots = vec![selected.origin().manifest().clone()];
        drop(facts);
        let filings = reader
            .select_by_identity(
                read_request(SecResearchFamily::Submissions)?,
                &store,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_sec_research_error)?;
        let SecResearchIdentityOutcome::Exact(selected) = filings.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        let company = selected.company_identity();
        if company.observation().provider_company_id() != &company_id {
            return Err(ServiceError::InvalidResult);
        }
        let industry = company
            .observation()
            .sic()
            .filter(|value| value.len() == 4 && value.as_bytes().iter().all(u8::is_ascii_digit))
            .ok_or(ServiceError::Unavailable)?
            .into();
        let industry_evidence = company.observation_digest();
        let industry_source = company.observation().source_id().clone();
        let industry_surface = company.observation().surface();
        roots.push(selected.origin().manifest().clone());
        drop(filings);
        Ok(SelectedComparableFundamentals {
            identity,
            metric,
            fact,
            period,
            industry,
            industry_evidence,
            industry_source,
            industry_surface,
            roots,
        })
    }
}

async fn discover_peers(
    research: &ResearchService,
    subject: &SelectedComparableFundamentals,
    request: &ObservedComparableValuationRequest,
    context: &RequestContext,
) -> Result<(Vec<InstrumentId>, EvidenceDigest), ServiceError> {
    let reader = research
        .analytical()
        .company_identities()
        .security_relationships();
    let industry_source = subject.industry_source.clone();
    let industry_surface = subject.industry_surface;
    let industry = subject.industry.clone();
    let industry_evidence = subject.industry_evidence;
    let knowledge_at = request.knowledge_at;
    let subject_id = request.subject;
    let deadline = context.deadline();
    research
        .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
            let cohort = reader
                .industry_cohort_as_of(
                    &industry_source,
                    industry_surface,
                    knowledge_at,
                    IndustryClassificationScheme::SecSic,
                    IndustryClassificationVersion::SecSicCurrentV1,
                    &industry,
                    MAXIMUM_PEERS + 1,
                    deadline,
                    &cancellation,
                )
                .map_err(map_company_identity_error)?;
            // A bounded subset cannot silently stand in for a complete automatically selected peer set.
            if cohort.receipt().completeness() != IndustryCohortCompleteness::Complete
                || cohort.exclusions().iter().any(|excluded| {
                    excluded.reason()
                        != market_squawk_data::IndustryClassificationExclusionReason::StaleParent
                })
                || !cohort
                    .members()
                    .iter()
                    .any(|member| member.company_observation_digest() == industry_evidence)
            {
                return Err(ServiceError::Unavailable);
            }
            let mut peers = Vec::with_capacity(cohort.members().len());
            let mut digest = Sha256::new();
            digest.update(b"market-squawk/observed-comparables-complete-cohort/v1");
            digest.update(cohort.receipt().receipt_digest().bytes());
            for member in cohort.members() {
                let selected = reader
                    .as_of(
                        &CompanySecurityIdentityQuery::new(
                            member.company_source_id().clone(),
                            member.provider_company_id().clone(),
                            member.company_surface(),
                            None,
                            true,
                        ),
                        knowledge_at,
                        deadline,
                        &cancellation,
                    )
                    .map_err(map_company_identity_error)?;
                let [candidate] = selected.candidates() else {
                    return Err(ServiceError::Unavailable);
                };
                if selected.disposition() != CompanySecurityIdentityDisposition::Complete
                    || candidate.link().company_observation_digest()
                        != member.company_observation_digest()
                {
                    return Err(ServiceError::Unavailable);
                }
                digest.update(selected.receipt().receipt_digest().bytes());
                if candidate.link().instrument_id() != subject_id {
                    peers.push(candidate.link().instrument_id());
                }
            }
            peers.sort_unstable();
            if !(2..=MAXIMUM_PEERS).contains(&peers.len())
                || peers.windows(2).any(|pair| pair[0] == pair[1])
            {
                return Err(ServiceError::Unavailable);
            }
            Ok((
                peers,
                EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
            ))
        })
        .await
        .map_err(|error| map_research_use_worker_error(error, context))?
}

fn select_annual_eps(
    selected: &market_squawk_data::SecResearchSelection,
) -> Result<(u32, &FundamentalObservation), ServiceError> {
    let mut latest: Option<(u32, &FundamentalObservation)> = None;
    for row in selected.selected() {
        let ordinal = row.row().row_ordinal();
        let Some(ResearchObservation::Fundamental(value)) =
            selected.decoded_rows().get(ordinal as usize)
        else {
            continue;
        };
        if value.concept().as_str() != "us-gaap:EarningsPerShareDiluted"
            || value.fact_context().cadence() != FundamentalCadence::Annual
            || !value.unit().as_str().ends_with("/shares")
            || value.value() <= Decimal::ZERO
            || !is_complete_annual_period(value.fact_context().period())
        {
            continue;
        }
        match latest {
            Some((_, previous))
                if previous.fact_context().period().end() > value.fact_context().period().end() => {
            }
            Some((_, previous))
                if previous.fact_context().period().end()
                    == value.fact_context().period().end() =>
            {
                // No row-order preference can hide differing filing, dimension or value evidence.
                if previous != value {
                    return Err(ServiceError::Unavailable);
                }
            }
            _ => latest = Some((ordinal, value)),
        }
    }
    latest.ok_or(ServiceError::Unavailable)
}

fn observed_comparable_arithmetic(
    subject_metric: Decimal,
    peers: impl ExactSizeIterator<Item = (Decimal, Decimal)>,
) -> Result<ComparableValueArithmetic, ServiceError> {
    let count = peers.len();
    if !(2..=MAXIMUM_PEERS).contains(&count) {
        return Err(ServiceError::Unavailable);
    }
    let count = u32::try_from(count).map_err(|_| ServiceError::InvalidResult)?;
    let mut values = Vec::with_capacity(count as usize);
    for (index, (value, metric)) in peers.enumerate() {
        let index = u32::try_from(index).map_err(|_| ServiceError::InvalidResult)?;
        let weight = 1_000_000 / count + u32::from(index < 1_000_000 % count);
        values.push((value, metric, weight));
    }
    ComparableValueArithmetic::calculate(subject_metric, &values)
        .map_err(|_| ServiceError::InvalidResult)
}

fn is_complete_annual_period(period: FundamentalPeriod) -> bool {
    let FundamentalPeriod::Duration { start, end } = period else {
        return false;
    };
    // Fiscal-year labels describe the filing and can also accompany a fourth-quarter fact.
    // Admit actual whole-year durations, including calendar and 52/53-week financial years.
    let days =
        i64::from(end.days_since_unix_epoch()) - i64::from(start.days_since_unix_epoch()) + 1;
    (364..=371).contains(&days)
}

fn point_input(
    input: ValuationInput,
    graph: market_squawk_data::ResearchUseGraphDigest,
    knowledge_at: Timestamp,
    expires_at: Timestamp,
) -> Result<PointInTimeValuationInput, ServiceError> {
    let (selection, cutoff) = input
        .evidence()
        .automatic_selection_binding()
        .ok_or(ServiceError::InvalidResult)?;
    if cutoff != knowledge_at {
        return Err(ServiceError::InvalidResult);
    }
    PointInTimeValuationInput::try_new(input, selection, graph, knowledge_at, expires_at)
        .map_err(|_| ServiceError::Unavailable)
}

fn assumption(
    kind: AutomaticValuationAssumptionKind,
    identifier: &str,
    value: Decimal,
    evidence: EvidenceDigest,
    available_at: Timestamp,
    expires_at: Timestamp,
) -> Result<AutomaticValuationAssumption, ServiceError> {
    AutomaticValuationAssumption::try_new(
        kind,
        identifier,
        value,
        evidence,
        available_at,
        expires_at,
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn calculation_clock() -> Result<Timestamp, ServiceError> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}

fn map_sec_research_error(error: market_squawk_data::SecResearchReadError) -> ServiceError {
    use crate::application::research::corporate_actions::map_research_error;
    use market_squawk_data::{ArrowConversionError as A, IngestError, SecResearchReadError as E};
    use market_squawk_platform::ResearchObjectControlError as C;
    match error {
        E::InvalidRequest => ServiceError::InvalidRequest,
        E::Cancelled => ServiceError::Cancelled,
        E::DeadlineExceeded => ServiceError::DeadlineExceeded,
        E::AuthorityUnavailable => ServiceError::Unavailable,
        E::ObjectBudgetExceeded => ServiceError::ResourceExhausted,
        E::OriginMismatch
        | E::ProviderBindingMismatch
        | E::PointInTimeSelection
        | E::RestartMismatch
        | E::DigestEncoding => ServiceError::InvalidResult,
        E::CompanySecurity(error) => map_company_identity_error(error),
        E::Manifest(error) => map_research_error(crate::ResearchServiceError::Manifest(error)),
        E::Catalog(error) => map_research_error(crate::ResearchServiceError::Catalog(error)),
        E::Parquet(error) => {
            map_research_error(crate::ResearchServiceError::Ingest(IngestError::Parquet(error)))
        }
        E::RawStore(error) => {
            map_research_error(crate::ResearchServiceError::ProviderCaptureStore(error))
        }
        E::Arrow(error) => match error {
            A::ObjectControl(C::Cancelled) => ServiceError::Cancelled,
            A::ObjectControl(C::DeadlineExceeded) => ServiceError::DeadlineExceeded,
            A::ObjectControl(C::Unavailable) => ServiceError::Internal,
            A::RetainedLimitExceeded { .. }
            | A::RecordLimitExceeded { .. }
            | A::AllocationFailure
            | A::RetainedSizeOverflow => ServiceError::ResourceExhausted,
            A::InvalidRetainedByteLimit { .. } | A::InvalidRecordLimit { .. } => {
                ServiceError::InvalidRequest
            }
            A::EmptyBatch
            | A::ExtractionBindingMismatch
            | A::RevisionAssignmentMismatch
            | A::ProviderCaptureRequired
            | A::RevisionAuthority(_)
            | A::PayloadContractEncoding(_)
            | A::PayloadContractMismatch
            | A::Research(_)
            | A::RequestDigestNotSha256
            | A::InvalidSchemaMetadata
            | A::UnsupportedSchemaVersion { .. }
            | A::InvalidSchema
            | A::UnexpectedDatasetSchema
            | A::InvalidFeatureLabelRow
            | A::InvalidMarketEventRow
            | A::InvalidOptionMarketRow
            | A::ProjectionMismatch
            | A::DecimalScale(_)
            | A::Arrow(_)
            | A::Json(_)
            | A::DatasetSchema(_) => ServiceError::InvalidResult,
        },
    }
}

fn map_company_identity_error(
    error: market_squawk_data::CompanySecurityIdentityCatalogError,
) -> ServiceError {
    use market_squawk_data::CompanySecurityIdentityCatalogError as E;
    match error {
        E::Cancelled => ServiceError::Cancelled,
        E::DeadlineExceeded => ServiceError::DeadlineExceeded,
        E::HistoryLimitExceeded | E::ResultLimitExceeded => ServiceError::ResourceExhausted,
        E::InvalidInput => ServiceError::InvalidRequest,
        E::UnverifiedIdentityAuthority => ServiceError::Unauthorized,
        E::ParentUnavailable | E::AmbiguousParent | E::AuthorityUnavailable => {
            ServiceError::Unavailable
        }
        E::TransitionConflict | E::CorruptCatalog | E::Serialization(_) => {
            ServiceError::InvalidResult
        }
        E::Storage(_) => ServiceError::Internal,
    }
}

fn map_research_use_worker_error(
    error: crate::ResearchServiceError,
    context: &RequestContext,
) -> ServiceError {
    if context.cancellation().is_cancelled() {
        return ServiceError::Cancelled;
    }
    if Instant::now() >= context.deadline() {
        return ServiceError::DeadlineExceeded;
    }
    match error {
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::Cancelled) => {
            ServiceError::Cancelled
        }
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::DeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        _ => ServiceError::Internal,
    }
}
fn map_research_use_error(
    error: market_squawk_data::ResearchUseCatalogError,
    context: &RequestContext,
) -> ServiceError {
    if context.cancellation().is_cancelled() {
        return ServiceError::Cancelled;
    }
    if Instant::now() >= context.deadline() {
        return ServiceError::DeadlineExceeded;
    }
    match error {
        market_squawk_data::ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
        market_squawk_data::ResearchUseCatalogError::DeadlineExceeded => {
            ServiceError::DeadlineExceeded
        }
        market_squawk_data::ResearchUseCatalogError::LimitExceeded => {
            ServiceError::ResourceExhausted
        }
        market_squawk_data::ResearchUseCatalogError::CorruptCatalog => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}
