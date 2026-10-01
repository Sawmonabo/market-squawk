//! Real estimator-distribution valuation through the shared durable publication path.

use std::num::NonZeroU64;

use market_squawk_domain::CommonEquitySuitability;
use market_squawk_valuation::{
    ForecastDistributionPoint, ForecastDistributionValuationRequest, ForecastValuationSource,
    calculate_forecast_distribution,
};

use super::*;

/// Empirical support cases and exact leave-one-outcome-out reweighted expectation sensitivity.
pub(super) fn forecast_model_cases(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<AutomaticValuationModelCases, ServiceError> {
    use market_squawk_valuation::AutomaticValuationIntermediateKind;
    let steps = receipt.intermediates();
    if steps.is_empty()
        || steps.iter().any(|step| {
            step.kind() != AutomaticValuationIntermediateKind::ProbabilityWeightedForecast
        })
        || receipt.range().central().basis() != ValuationAmountBasis::PerInstrumentUnit
    {
        return Err(ServiceError::InvalidResult);
    }
    let total = steps.iter().try_fold(Decimal::ZERO, |sum, step| {
        sum.checked_add(step.result())
            .ok_or(ServiceError::InvalidResult)
    })?;
    let mass = steps.iter().try_fold(Decimal::ZERO, |sum, step| {
        sum.checked_add(step.factor())
            .ok_or(ServiceError::InvalidResult)
    })?;
    if mass != Decimal::ONE {
        return Err(ServiceError::InvalidResult);
    }
    let mut lower = receipt.range().central().money().amount();
    let mut upper = lower;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/forecast-leave-one-outcome-out/v1");
    hash.update(receipt.id().bytes());
    for step in steps {
        if step.factor() <= Decimal::ZERO || step.factor() > Decimal::ONE {
            return Err(ServiceError::InvalidResult);
        }
        let value = if steps.len() == 1 {
            total
        } else {
            total
                .checked_sub(step.result())
                .and_then(|remaining| {
                    Decimal::ONE
                        .checked_sub(step.factor())
                        .and_then(|mass| remaining.checked_div(mass))
                })
                .ok_or(ServiceError::InvalidResult)?
        };
        let value = round_method_value(receipt, value);
        lower = lower.min(value);
        upper = upper.max(value);
        hash.update(step.primary_input().bytes());
        hash.update(value.mantissa().to_be_bytes());
        hash.update(value.scale().to_be_bytes());
    }
    let currency = receipt.range().central().money().currency();
    let scenarios = market_squawk_decisions::TargetPriceCases::try_new(
        receipt.range().lower().money(),
        receipt.range().central().money(),
        receipt.range().upper().money(),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let sensitivity_range = market_squawk_decisions::TargetPriceRange::try_new(
        Money::new(lower, currency),
        Money::new(upper, currency),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let identity = |bytes| {
        market_squawk_decisions::DecisionContentDigest::try_new(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            bytes,
        ))
        .map_err(|_| ServiceError::InvalidResult)
    };
    let mut scenario = Sha256::new();
    scenario.update(b"market-squawk/forecast-empirical-support-cases/v1");
    scenario.update(receipt.id().bytes());
    Ok(AutomaticValuationModelCases {
        calculation_identity: receipt.id(),
        scenarios,
        scenario_identity: identity(scenario.finalize().into())?,
        sensitivity_range,
        sensitivity_identity: identity(hash.finalize().into())?,
    })
}

/// Policy/actor coordinates for an authentic artifact-backed distribution calculation.
#[derive(Clone, Debug)]
pub(crate) struct AutomaticForecastValuationRequest {
    pub(crate) account_id: AccountId,
    pub(crate) expires_at: Timestamp,
    pub(crate) calculated_by: ActorId,
}

impl FairValueDomainService {
    /// Calculates all explicit estimator outcomes with their actual normalized probability mass.
    pub(crate) async fn calculate_forecast_valuation(
        &self,
        research: Arc<ResearchService>,
        market_reader: &MarketInvestmentReadCapability,
        market_selection: &MarketInvestmentReadReceipt,
        source: ForecastValuationSource,
        request: AutomaticForecastValuationRequest,
        context: &RequestContext,
    ) -> Result<AutomaticValuationPublication, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let source = Arc::new(source);
        let reference = source.reference();
        let instrument_id = reference.instrument_id();
        let knowledge_at = reference.knowledge_at();
        if market_selection.reference().instrument_id() != instrument_id
            || market_selection.reference().source_cutoff()? < knowledge_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        let identity_reader = research
            .analytical()
            .company_identities()
            .security_relationships();
        let deadline = context.deadline();
        let identity = research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                identity_reader
                    .instrument_company_as_of(
                        instrument_id,
                        &SourceId::try_from("sec-edgar").map_err(|_| ServiceError::Internal)?,
                        CompanyIdentitySurface::SecCompanyFacts,
                        knowledge_at,
                        CommonEquitySuitability::SuitableIssuerCommonEquity,
                        deadline,
                        &cancellation,
                    )
                    .map_err(map_company_identity_error)
            })
            .await
            .map_err(|error| map_research_use_worker_error(error, context))??;
        if identity.receipt().disposition() != CompanySecurityIdentityDisposition::Complete
            || identity.receipt().ordered_candidates().len() != 1
        {
            return Err(ServiceError::Unavailable);
        }
        let market = ValuationInput::from_published_market_selection(
            market_selection.publication(),
            market_selection.market_definitions(),
            market_selection.instrument_definitions(),
            InputSignificance::Significant,
        )
        .map_err(map_fair_value_error)?;
        let mut expires_at = request
            .expires_at
            .min(source.distribution().expires_at())
            .min(
                market_selection
                    .observation()
                    .map_err(|_| ServiceError::InvalidResult)?
                    .mark()
                    .fresh_until()
                    .ok_or(ServiceError::Unavailable)?,
            );
        for candidate in identity.receipt().ordered_candidates() {
            for end in [
                candidate.effective_end(),
                candidate.market_effective_end(),
                candidate.current_market_effective_end(),
            ]
            .into_iter()
            .flatten()
            {
                expires_at = expires_at.min(end);
            }
        }
        let roots = reference.parent_manifests().to_vec();
        let authorization = research
            .authorize_research_use(
                ResearchUseRequest::try_new(
                    roots,
                    ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(
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
                    .map_err(|_| ServiceError::Internal)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| map_research_use_worker_error(error, context))?
            .map_err(|error| map_research_use_error(error, context))?;
        let (event_authorization, _) = market_reader
            .authorize_local_analysis(
                market_selection.publication(),
                context.deadline(),
                context.cancellation(),
            )
            .await?;
        let rights = ValuationRightsReceipt::try_from_authorization(
            authorization,
            vec![event_authorization],
        )
        .map_err(|_| ServiceError::Unavailable)?;
        expires_at = expires_at.min(rights.expires_at());
        let rights_input_digest = rights.rights_input_digest();
        let output_scale = market.amount().scale();
        let currency = market.amount().money().currency();
        let evidence = reference.identity();
        let published_at = source.distribution().published_at();
        let count = source.distribution().points().len();
        if count == 0 || count > 126 {
            return Err(ServiceError::Unavailable);
        }
        let lower = source
            .amount(0)
            .map_err(map_fair_value_error)?
            .money()
            .amount()
            .round_dp_with_strategy(
                u32::from(output_scale),
                rust_decimal::RoundingStrategy::ToNegativeInfinity,
            );
        let upper = source
            .amount(count - 1)
            .map_err(map_fair_value_error)?
            .money()
            .amount()
            .round_dp_with_strategy(
                u32::from(output_scale),
                rust_decimal::RoundingStrategy::ToPositiveInfinity,
            );
        let uncertainty = AutomaticValuationUncertainty::try_new(
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyLower,
                "empirical_support_minimum",
                lower,
                evidence,
                published_at,
                expires_at,
            )?,
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyUpper,
                "empirical_support_maximum",
                upper,
                evidence,
                published_at,
                expires_at,
            )?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let mut points = Vec::with_capacity(count);
        for (ordinal, native) in source.distribution().points().iter().enumerate() {
            let name = format!("forecast_outcome_{ordinal:03}");
            let input = ValuationInput::from_forecast_distribution_point(
                Arc::clone(&source),
                ordinal,
                InputSignificance::Significant,
            )
            .map_err(map_fair_value_error)?;
            points.push(ForecastDistributionPoint {
                point_id: name.clone().into_boxed_str(),
                terminal_value: point_input(input, rights_input_digest, knowledge_at, expires_at)?,
                terminal_at: source
                    .distribution()
                    .target_at()
                    .ok_or(ServiceError::Unavailable)?,
                probability_ppm: native.probability_ppm().get(),
                probability_assumption: assumption(
                    AutomaticValuationAssumptionKind::ForecastProbability,
                    &name,
                    Decimal::from_i128_with_scale(i128::from(native.probability_ppm().get()), 6),
                    evidence,
                    published_at,
                    expires_at,
                )?,
            });
        }
        let horizon_nanos = source
            .distribution()
            .target_at()
            .ok_or(ServiceError::Unavailable)?
            .unix_nanos()
            .checked_sub(knowledge_at.unix_nanos())
            .and_then(|value| u64::try_from(value).ok())
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidResult)?;
        market_reader
            .recheck(
                market_selection,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
        ensure_request_live(context, &self.lifecycle)?;
        let calculated_at = calculation_clock()?;
        if expires_at <= calculated_at {
            return Err(ServiceError::Unavailable);
        }
        let calculation = calculate_forecast_distribution(ForecastDistributionValuationRequest {
            common: AutomaticValuationInput {
                account_id: request.account_id,
                company_security: identity.receipt().clone(),
                instrument_id,
                currency,
                amount_basis: ValuationAmountBasis::PerInstrumentUnit,
                current_market: point_input(
                    market,
                    rights_input_digest,
                    market_selection.reference().source_cutoff()?,
                    expires_at,
                )?,
                rights,
                measurement_at: knowledge_at,
                calculated_at,
                expires_at,
                calculated_by: request.calculated_by,
                output_scale,
                arithmetic_policy: ValuationArithmeticPolicy::try_new(
                    RoundingPolicy::NearestEven,
                    count,
                )
                .map_err(|_| ServiceError::Internal)?,
            },
            horizon_nanos,
            points,
            forecast_selection_receipt: evidence,
            uncertainty,
        })
        .map_err(|_| ServiceError::Unavailable)?;
        self.publish_automatic_calculation(calculation, context)
            .await
    }
}
