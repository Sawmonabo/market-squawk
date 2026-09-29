//! Native fiscal amount assembly through the actual valuation calculators and durable owner.

use super::*;
use crate::application::research::fiscal_projection::FISCAL_PROJECTION_EXPLICIT_PERIODS;
use market_squawk_data::{FeatureLabelMeasurement, FinancialAmountBasis, FinancialAmountRole};
use market_squawk_domain::CommonEquitySuitability;
use market_squawk_valuation::{
    AnnualEquityArithmetic, AutomaticValuationMethod, DcfCashFlow,
    DiscountedCashFlowValuationRequest, FinancialModelMacroAssumptions, ForecastValuationSource,
    ResidualIncomePeriod, ResidualIncomeValuationRequest, calculate_discounted_cash_flow,
    calculate_residual_income,
};
use std::num::NonZeroU32;

struct NativeMethodInputs {
    method: AutomaticValuationMethod,
    /// Current original common book followed by income1..N and book1..N-1, or FCFE1..N+1.
    inputs: Vec<ValuationInput>,
    sources: Vec<Arc<ForecastValuationSource>>,
}

impl NativeMethodInputs {
    fn select(
        sources: &[Arc<ForecastValuationSource>],
        method: AutomaticValuationMethod,
        instrument: InstrumentId,
        cutoff: Timestamp,
    ) -> Result<Self, ServiceError> {
        let n = usize::from(FISCAL_PROJECTION_EXPLICIT_PERIODS);
        if sources.len() > 16 || n == 0 || n > 8 {
            return Err(ServiceError::InvalidRequest);
        }
        let mut selected = Vec::new();
        let mut inputs = Vec::new();
        let mut select = |role, offset: usize, origin: bool| -> Result<(), ServiceError> {
            let mut matches = sources.iter().filter(|source| {
                let Some(epoch) = source.financial_epoch() else {
                    return false;
                };
                let Some(binding) = epoch.financial_period() else {
                    return false;
                };
                let Some(FeatureLabelMeasurement::FinancialAmount {
                    role: actual_role,
                    basis: FinancialAmountBasis::TotalCommonEquity,
                    share_convention: None,
                    ..
                }) = epoch.financial_measurement()
                else {
                    return false;
                };
                actual_role == role
                    && binding.cadence() == FundamentalCadence::Annual
                    && source.reference().instrument_id() == instrument
                    && source.reference().knowledge_at() == cutoff
                    && binding
                        .target_ordinal()
                        .checked_sub(binding.observed_ordinal())
                        == u32::try_from(offset).ok()
            });
            let source = matches.next().ok_or(ServiceError::Unavailable)?;
            if matches.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            let input = if origin {
                ValuationInput::from_forecast_financial_origin(
                    Arc::clone(source),
                    InputSignificance::Significant,
                )
            } else {
                ValuationInput::from_forecast_distribution_central(
                    Arc::clone(source),
                    InputSignificance::Significant,
                )
            }
            .map_err(map_fair_value_error)?;
            selected.push(Arc::clone(source));
            inputs.push(input);
            Ok(())
        };
        match method {
            AutomaticValuationMethod::DiscountedCashFlow => {
                for offset in 1..=n + 1 {
                    select(FinancialAmountRole::CommonEquityCashFlow, offset, false)?;
                }
            }
            AutomaticValuationMethod::ResidualIncome => {
                // Book0 is the exact reported/derived source amount underlying the Book1 vintage.
                select(FinancialAmountRole::CommonBookEquity, 1, true)?;
                for offset in 1..=n {
                    select(FinancialAmountRole::CommonNetIncome, offset, false)?;
                }
                for offset in 1..n {
                    select(FinancialAmountRole::CommonBookEquity, offset, false)?;
                }
            }
            _ => return Err(ServiceError::InvalidRequest),
        }
        Ok(Self {
            method,
            inputs,
            sources: selected,
        })
    }

    /// Only actual empirical model support is varied, one input at a time. No joint probability is asserted.
    fn range(
        &self,
        rate: Decimal,
        nominal_risk_free_cap: Option<Decimal>,
    ) -> Result<(Decimal, Decimal, EvidenceDigest), ServiceError> {
        let values: Vec<_> = self
            .inputs
            .iter()
            .map(|input| input.amount().money().amount())
            .collect();
        let central = self.value(&values, rate, nominal_risk_free_cap)?;
        let mut lower = central;
        let mut upper = central;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/native-financial-one-input-support/v1");
        hash.update(FISCAL_PROJECTION_EXPLICIT_PERIODS.to_be_bytes());
        match self.method {
            AutomaticValuationMethod::DiscountedCashFlow => {
                hash.update(b"conditional_fcfe_ratio_same_nominal_rate_cap");
            }
            AutomaticValuationMethod::ResidualIncome => {
                use market_squawk_valuation::ResidualIncomeTerminalConvention;
                let convention =
                    ResidualIncomeTerminalConvention::ZeroAbnormalEarningsAfterExplicitHorizon;
                hash.update(convention.identifier().as_bytes());
            }
            AutomaticValuationMethod::ComparableCompanies
            | AutomaticValuationMethod::ForecastDistribution => {
                return Err(ServiceError::InvalidResult);
            }
        }

        for (index, source) in self.sources.iter().enumerate() {
            hash.update(source.reference().identity().bytes());
            if self.method == AutomaticValuationMethod::ResidualIncome && index == 0 {
                continue;
            }
            let count = source.distribution().points().len();
            if count == 0 {
                return Err(ServiceError::Unavailable);
            }
            for ordinal in [0, count - 1] {
                let mut scenario = values.clone();
                scenario[index] = source
                    .amount(ordinal)
                    .map_err(map_fair_value_error)?
                    .money()
                    .amount();
                let result = self.value(&scenario, rate, nominal_risk_free_cap)?;
                lower = lower.min(result);
                upper = upper.max(result);
                hash.update((index as u64).to_be_bytes());
                hash.update((ordinal as u64).to_be_bytes());
                hash.update(result.mantissa().to_be_bytes());
                hash.update(result.scale().to_be_bytes());
            }
        }
        hash.update(rate.mantissa().to_be_bytes());
        hash.update(rate.scale().to_be_bytes());
        if let Some(growth) = nominal_risk_free_cap {
            hash.update(growth.mantissa().to_be_bytes());
            hash.update(growth.scale().to_be_bytes());
        }
        Ok((
            lower,
            upper,
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        ))
    }

    fn value(
        &self,
        amounts: &[Decimal],
        rate: Decimal,
        nominal_risk_free_cap: Option<Decimal>,
    ) -> Result<Decimal, ServiceError> {
        let n = usize::from(FISCAL_PROJECTION_EXPLICIT_PERIODS);
        let period = |i| {
            NonZeroU32::new(u32::try_from(i).map_err(|_| ServiceError::InvalidResult)?)
                .ok_or(ServiceError::InvalidResult)
        };
        let mut value = if self.method == AutomaticValuationMethod::ResidualIncome {
            amounts[0]
        } else {
            Decimal::ZERO
        };
        for offset in 1..=n {
            let amount = if self.method == AutomaticValuationMethod::ResidualIncome {
                let opening = if offset == 1 {
                    amounts[0]
                } else {
                    amounts[n + offset - 1]
                };
                amounts[offset]
                    .checked_sub(
                        opening
                            .checked_mul(rate)
                            .ok_or(ServiceError::InvalidResult)?,
                    )
                    .ok_or(ServiceError::InvalidResult)?
            } else {
                amounts[offset - 1]
            };
            value = value
                .checked_add(
                    AnnualEquityArithmetic::discounted_amount(amount, rate, period(offset)?)
                        .map_err(|_| ServiceError::InvalidResult)?,
                )
                .ok_or(ServiceError::InvalidResult)?;
        }
        if self.method == AutomaticValuationMethod::DiscountedCashFlow {
            value = value
                .checked_add(
                    AnnualEquityArithmetic::discounted_terminal_fcfe(
                        amounts[n],
                        rate,
                        AnnualEquityArithmetic::conditional_terminal_growth(
                            amounts[n - 1],
                            amounts[n],
                            nominal_risk_free_cap.ok_or(ServiceError::Unavailable)?,
                            rate,
                        )
                        .map_err(|_| ServiceError::Unavailable)?
                        .1,
                        period(n)?,
                    )
                    .map_err(|_| ServiceError::InvalidResult)?,
                )
                .ok_or(ServiceError::InvalidResult)?;
        }
        Ok(value)
    }
}

impl FairValueDomainService {
    /// Selects true source-owned native monetary roles before running either fundamental method.
    pub(super) async fn calculate_native_financial_valuation(
        &self,
        research: &ResearchService,
        market: &MarketInvestmentReadReceipt,
        sources: &[Arc<ForecastValuationSource>],
        method: AutomaticValuationMethod,
        macro_assumptions: FinancialModelMacroAssumptions,
        request: &super::investment::AutomaticInvestmentValuationRequest,
        context: &RequestContext,
    ) -> Result<AutomaticValuationPublication, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let native = NativeMethodInputs::select(
            sources,
            method,
            request.instrument_id,
            request.knowledge_at,
        )?;
        let identity_reader = research
            .analytical()
            .company_identities()
            .security_relationships();
        let instrument = request.instrument_id;
        let cutoff = request.knowledge_at;
        let deadline = context.deadline();
        let identity = research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                identity_reader
                    .instrument_company_as_of(
                        instrument,
                        &SourceId::try_from("sec-edgar").map_err(|_| ServiceError::Internal)?,
                        CompanyIdentitySurface::SecCompanyFacts,
                        cutoff,
                        CommonEquitySuitability::SuitableIssuerCommonEquity,
                        deadline,
                        &cancellation,
                    )
                    .map_err(map_company_identity_error)
            })
            .await
            .map_err(|error| map_research_use_worker_error(error, context))??;
        if native.sources.iter().any(|source| {
            source
                .financial_epoch()
                .and_then(|epoch| epoch.financial_period())
                .is_none_or(|binding| {
                    binding.identity_receipt_digest() != identity.receipt().receipt_digest()
                })
        }) {
            return Err(ServiceError::Unavailable);
        }
        let current_market = ValuationInput::from_published_market_selection(
            market.publication(),
            market.market_definitions(),
            market.instrument_definitions(),
            InputSignificance::Significant,
        )
        .map_err(map_fair_value_error)?;
        let mut expires_at = request
            .expires_at
            .min(macro_assumptions.assumption().expires_at())
            .min(
                market
                    .observation()
                    .map_err(|_| ServiceError::InvalidResult)?
                    .mark()
                    .fresh_until()
                    .ok_or(ServiceError::Unavailable)?,
            );
        let mut roots = macro_assumptions.premium_parent_manifests().to_vec();
        for source in &native.sources {
            expires_at = expires_at.min(source.distribution().expires_at());
            for manifest in source.reference().parent_manifests() {
                if !roots.contains(manifest) {
                    roots.push(manifest.clone());
                }
            }
        }
        if !roots.contains(market.publication().manifest()) {
            roots.push(market.publication().manifest().clone());
        }
        let authorization = research
            .authorize_research_use(
                ResearchUseRequest::try_new(
                    roots,
                    ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(
                        256,
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
                    .map_err(|_| ServiceError::InvalidRequest)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|error| map_research_use_worker_error(error, context))?
            .map_err(|error| map_research_use_error(error, context))?;
        expires_at = expires_at.min(authorization.expires_at());
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
        let nominal_risk_free_cap = if method == AutomaticValuationMethod::DiscountedCashFlow {
            if macro_assumptions.reference().maturity()
                != market_squawk_valuation::MacroRateMaturity::TenYear
                || current_market.amount().money().currency().as_str() != "USD"
            {
                return Err(ServiceError::Unavailable);
            }
            Some(
                macro_assumptions
                    .reference()
                    .annual_yield_percent()
                    .checked_div(Decimal::from(100_u32))
                    .ok_or(ServiceError::InvalidResult)?,
            )
        } else {
            None
        };
        let (lower, upper, uncertainty_id) = native.range(
            macro_assumptions.assumption().value(),
            nominal_risk_free_cap,
        )?;
        let calculated_at = calculation_clock()?;
        if calculated_at >= expires_at {
            return Err(ServiceError::Unavailable);
        }
        let (lower_identifier, upper_identifier) = match method {
            AutomaticValuationMethod::DiscountedCashFlow => (
                "native_one_input_support_minimum_conditional_fcfe_growth",
                "native_one_input_support_maximum_conditional_fcfe_growth",
            ),
            AutomaticValuationMethod::ResidualIncome => (
                "native_one_input_support_minimum_conditional_finite_residual",
                "native_one_input_support_maximum_conditional_finite_residual",
            ),
            AutomaticValuationMethod::ComparableCompanies
            | AutomaticValuationMethod::ForecastDistribution => {
                return Err(ServiceError::InvalidResult);
            }
        };
        let uncertainty = AutomaticValuationUncertainty::try_new(
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyLower,
                lower_identifier,
                lower.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToNegativeInfinity),
                uncertainty_id,
                calculated_at,
                expires_at,
            )?,
            assumption(
                AutomaticValuationAssumptionKind::UncertaintyUpper,
                upper_identifier,
                upper.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToPositiveInfinity),
                uncertainty_id,
                calculated_at,
                expires_at,
            )?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let rights = ValuationRightsReceipt::try_from_authorization(authorization)
            .map_err(|_| ServiceError::Unavailable)?;
        let graph = rights.graph_digest();
        let common = AutomaticValuationInput {
            account_id: request.account_id,
            company_security: identity.receipt().clone(),
            instrument_id: request.instrument_id,
            currency: current_market.amount().money().currency(),
            amount_basis: ValuationAmountBasis::TotalCommonEquity,
            current_market: point_input(
                current_market,
                graph,
                market.reference().source_cutoff()?,
                expires_at,
            )?,
            rights,
            measurement_at: request.knowledge_at,
            calculated_at,
            expires_at,
            calculated_by: request.calculated_by.clone(),
            output_scale: 2,
            arithmetic_policy: ValuationArithmeticPolicy::try_new(RoundingPolicy::NearestEven, 16)
                .map_err(|_| ServiceError::Internal)?,
        };
        let mut points = native
            .inputs
            .into_iter()
            .map(|input| point_input(input, graph, request.knowledge_at, expires_at))
            .collect::<Result<Vec<_>, _>>()?;
        let n = usize::from(FISCAL_PROJECTION_EXPLICIT_PERIODS);
        let calculation = if method == AutomaticValuationMethod::ResidualIncome {
            let current_book_value = points[0].clone();
            let mut periods = Vec::with_capacity(n);
            for offset in 1..=n {
                periods.push(ResidualIncomePeriod {
                    period: NonZeroU32::new(
                        u32::try_from(offset).map_err(|_| ServiceError::InvalidResult)?,
                    )
                    .ok_or(ServiceError::Internal)?,
                    net_income: points[offset].clone(),
                    opening_book_value: if offset == 1 {
                        current_book_value.clone()
                    } else {
                        points[n + offset - 1].clone()
                    },
                });
            }
            calculate_residual_income(ResidualIncomeValuationRequest {
                periods_per_year: NonZeroU32::MIN,
                common,
                current_book_value,
                periods,
                macro_assumptions,
                uncertainty,
            })
        } else {
            let terminal_cash_flow = points.pop().ok_or(ServiceError::InvalidResult)?;
            let cash_flows = points
                .into_iter()
                .enumerate()
                .map(|(index, cash_flow)| {
                    Ok(DcfCashFlow {
                        period: NonZeroU32::new(
                            u32::try_from(index + 1).map_err(|_| ServiceError::InvalidResult)?,
                        )
                        .ok_or(ServiceError::Internal)?,
                        cash_flow,
                    })
                })
                .collect::<Result<Vec<_>, ServiceError>>()?;
            calculate_discounted_cash_flow(DiscountedCashFlowValuationRequest {
                periods_per_year: NonZeroU32::MIN,
                common,
                cash_flows,
                macro_assumptions,
                terminal_period: NonZeroU32::new(
                    u32::try_from(n).map_err(|_| ServiceError::Internal)?,
                )
                .ok_or(ServiceError::Internal)?,
                terminal_cash_flow,
                uncertainty,
            })
        }
        .map_err(|_| ServiceError::Unavailable)?;
        self.publish_automatic_calculation(calculation, context)
            .await
    }
}

pub(crate) const MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES: usize = 128 * 1024;

/// Exact filing and action locators. Decoding these coordinates grants no source authority.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FundamentalShareSourceReference {
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    filing_result: EvidenceDigest,
    company_identity: EvidenceDigest,
    action: crate::application::SourceAppliedCorporateActionPlanReference,
}
#[derive(Clone)]
pub(crate) struct FundamentalShareProjectionSources {
    pub(crate) bases: Vec<market_squawk_valuation::CommonShareValuationBasis>,
    pub(crate) reference: Box<[u8]>,
}

async fn selected_share_filing(
    research: &ResearchService,
    instrument: InstrumentId,
    knowledge_at: Timestamp,
    context: &RequestContext,
) -> Result<market_squawk_data::SecResearchIdentitySelection, ServiceError> {
    let pit = PointInTimeLimits::try_new(
        SOURCE_READ_ROWS,
        SOURCE_READ_ROWS,
        1024,
        SOURCE_READ_ROWS,
        SOURCE_READ_BYTES,
    )
    .map_err(|_| ServiceError::Internal)?;
    let request = SecResearchIdentityReadRequest::try_new(
        instrument,
        SecResearchFamily::FilingXbrl,
        knowledge_at,
        ResearchTemporalCoordinate::calendar_date(
            knowledge_at
                .utc_calendar_date()
                .map_err(|_| ServiceError::InvalidRequest)?,
        ),
        PointInTimeRevisionMode::LatestKnown,
        pit,
        SOURCE_READ_BYTES,
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    let selection = research
        .analytical()
        .sec_research_reader()
        .select_by_identity(
            request,
            &research.provider_capture_store(),
            context.deadline(),
            context.cancellation().clone(),
        )
        .await
        .map_err(map_sec_research_error)?;
    let SecResearchIdentityOutcome::Exact(selected) = selection.outcome() else {
        return Err(ServiceError::Unavailable);
    };
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
    .map_err(|_| ServiceError::InvalidRequest)?;
    // Retained local source use still needs actual current LocalAnalysis rights.
    let authorization = research
        .authorize_research_use(
            ResearchUseRequest::try_new(
                vec![selected.origin().manifest().clone()],
                ResearchUse::LocalAnalysis,
                limits,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            context.deadline(),
            context.cancellation(),
        )
        .await
        .map_err(|e| map_research_use_worker_error(e, context))?
        .map_err(|e| map_research_use_error(e, context))?;
    let _permit = authorization.into_permit();
    Ok(selection)
}

/// Compact source-derived preparation across acquisition; no complete filing payload is retained.
pub(crate) struct FundamentalShareRequirements {
    filings: Vec<(
        market_squawk_valuation::CommonShareFilingEvidence,
        EvidenceDigest,
        EvidenceDigest,
    )>,
    required: Vec<(InstrumentId, CalendarDate)>,
}
impl FundamentalShareRequirements {
    pub(crate) fn requirements(&self) -> &[(InstrumentId, CalendarDate)] {
        &self.required
    }
}

impl FairValueDomainService {
    /// Resolve genuine filing and cohort facts before any final valuation quote is sampled.
    pub(crate) async fn prepare_common_share_requirements(
        research: &ResearchService,
        subject: InstrumentId,
        knowledge_at: Timestamp,
        context: &RequestContext,
    ) -> Result<FundamentalShareRequirements, ServiceError> {
        let filing = compact_share_filing(research, subject, knowledge_at, context).await?;
        let mut result = FundamentalShareRequirements {
            required: vec![(subject, filing.0.reported_on())],
            filings: vec![filing],
        };
        // Missing comparable sources do not discard a genuine native-method denominator.
        let comparable = async {
            let date = knowledge_at
                .utc_calendar_date()
                .map_err(|_| ServiceError::InvalidRequest)?;
            let subject_facts = Self::select_comparable_fundamentals(
                research,
                subject,
                knowledge_at,
                date,
                context,
            )
            .await?;
            let (peers, _) =
                discover_peers(research, &subject_facts, subject, knowledge_at, context).await?;
            let FundamentalPeriod::Duration { start, .. } = subject_facts.period else {
                return Err(ServiceError::Unavailable);
            };
            let mut peer_filings = Vec::with_capacity(peers.len());
            let mut requirements = vec![(subject, start.min(result.filings[0].0.reported_on()))];
            for peer in peers {
                let facts = Self::select_comparable_fundamentals(
                    research,
                    peer,
                    knowledge_at,
                    date,
                    context,
                )
                .await?;
                if facts.period != subject_facts.period
                    || facts.industry != subject_facts.industry
                    || facts.metric.amount().money().currency()
                        != subject_facts.metric.amount().money().currency()
                {
                    return Err(ServiceError::Unavailable);
                }
                let filing = compact_share_filing(research, peer, knowledge_at, context).await?;
                requirements.push((peer, start.min(filing.0.reported_on())));
                peer_filings.push(filing);
            }
            Ok::<_, ServiceError>((peer_filings, requirements))
        }
        .await;
        match comparable {
            Ok((peers, required)) => {
                result.filings.extend(peers);
                result.required = required;
            }
            Err(
                ServiceError::Unavailable | ServiceError::NotFound | ServiceError::Unauthorized,
            ) => {}
            Err(error) => return Err(error),
        }
        Ok(result)
    }
}

async fn compact_share_filing(
    research: &ResearchService,
    instrument: InstrumentId,
    knowledge_at: Timestamp,
    context: &RequestContext,
) -> Result<
    (
        market_squawk_valuation::CommonShareFilingEvidence,
        EvidenceDigest,
        EvidenceDigest,
    ),
    ServiceError,
> {
    let selection = selected_share_filing(research, instrument, knowledge_at, context).await?;
    let filing = market_squawk_valuation::CommonShareFilingEvidence::try_from_filing(&selection)
        .map_err(|_| ServiceError::Unavailable)?;
    let SecResearchIdentityOutcome::Exact(selected) = selection.outcome() else {
        return Err(ServiceError::Unavailable);
    };
    Ok((
        filing,
        selected.receipt().result_digest(),
        selection.identity().receipt().receipt_digest(),
    ))
}

/// Bind prepared filing identities to the exact ordered plans finished after final quote sampling.
/// The serialized result is inert; generation must physically reopen it before using a denominator.
pub(crate) fn finish_common_share_sources(
    requirements: FundamentalShareRequirements,
    actions: Vec<crate::application::SourceAppliedCorporateActionPlanReference>,
) -> Result<String, ServiceError> {
    let subject = requirements
        .filings
        .first()
        .ok_or(ServiceError::InvalidResult)?
        .0
        .instrument_id();
    if actions.is_empty()
        || actions.len() > requirements.filings.len()
        || actions
            .first()
            .is_none_or(|action| action.requested_instruments() != [subject])
    {
        return Err(ServiceError::InvalidResult);
    }
    // Missing peer sources leave the subject usable by native equity models. The
    // comparable receipt still requires its complete original cohort at admission.
    let mut actions = actions.into_iter().peekable();
    let mut sources = Vec::with_capacity(actions.len());
    for (filing, filing_result, company_identity) in requirements.filings {
        if actions
            .peek()
            .is_some_and(|action| action.requested_instruments() == [filing.instrument_id()])
        {
            sources.push(FundamentalShareSourceReference {
                instrument_id: filing.instrument_id(),
                knowledge_at: filing.knowledge_at(),
                filing_result,
                company_identity,
                action: actions.next().ok_or(ServiceError::InvalidResult)?,
            });
        }
    }
    // Unknown, duplicated, or reordered references cannot be rebound to another filing.
    if actions.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let reference = serde_json::to_string(&sources).map_err(|_| ServiceError::InvalidResult)?;
    validate_fundamental_share_sources(reference.as_bytes())?;
    Ok(reference)
}

/// Select only the original method's exact source set from the bounded final preparation carrier.
pub(super) async fn select_prepared_fundamental_share_sources(
    receipt: &AutomaticValuationMethodReceipt,
    prepared: Option<&str>,
    research: &ResearchService,
    actions: &crate::application::SourceAppliedCorporateActionReadCapability,
    context: &RequestContext,
) -> Result<FundamentalShareProjectionSources, ServiceError> {
    let sources =
        decode_fundamental_share_sources(prepared.ok_or(ServiceError::Unavailable)?.as_bytes())?;
    let instruments = receipt
        .common_share_instruments()
        .map_err(|_| ServiceError::InvalidResult)?;
    let selected = instruments
        .iter()
        .map(|instrument| {
            sources
                .iter()
                .find(|source| source.instrument_id == *instrument)
                .cloned()
                .ok_or(ServiceError::Unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let reference = serde_json::to_vec(&selected).map_err(|_| ServiceError::InvalidResult)?;
    replay_fundamental_share_sources(receipt, &reference, research, actions, context).await
}

pub(crate) fn validate_fundamental_share_sources(reference: &[u8]) -> Result<(), ServiceError> {
    decode_fundamental_share_sources(reference).map(|_| ())
}
fn decode_fundamental_share_sources(
    reference: &[u8],
) -> Result<Vec<FundamentalShareSourceReference>, ServiceError> {
    if reference.is_empty() || reference.len() > MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES {
        return Err(ServiceError::InvalidResult);
    }
    let sources: Vec<FundamentalShareSourceReference> =
        serde_json::from_slice(reference).map_err(|_| ServiceError::InvalidResult)?;
    if sources.is_empty()
        || sources.len() > 17
        || serde_json::to_vec(&sources).map_err(|_| ServiceError::InvalidResult)? != reference
        || sources.iter().enumerate().any(|(index, source)| {
            sources[..index]
                .iter()
                .any(|prior| prior.instrument_id == source.instrument_id)
                || source.action.requested_instruments() != [source.instrument_id]
                || source.action.knowledge_cutoff() < source.knowledge_at
                || [source.filing_result, source.company_identity]
                    .iter()
                    .any(|digest| {
                        digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
                    })
        })
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(sources)
}

pub(crate) async fn replay_fundamental_share_sources(
    receipt: &AutomaticValuationMethodReceipt,
    reference: &[u8],
    research: &ResearchService,
    actions: &crate::application::SourceAppliedCorporateActionReadCapability,
    context: &RequestContext,
) -> Result<FundamentalShareProjectionSources, ServiceError> {
    let sources = decode_fundamental_share_sources(reference)?;
    let instruments = receipt
        .common_share_instruments()
        .map_err(|_| ServiceError::InvalidResult)?;
    if sources.len() != instruments.len() {
        return Err(ServiceError::InvalidResult);
    }
    let mut bases = Vec::with_capacity(sources.len());
    for (source, instrument) in sources.iter().zip(instruments) {
        if source.instrument_id != instrument || source.knowledge_at != receipt.measurement_at() {
            return Err(ServiceError::InvalidResult);
        }
        let selection =
            selected_share_filing(research, instrument, source.knowledge_at, context).await?;
        let SecResearchIdentityOutcome::Exact(selected) = selection.outcome() else {
            return Err(ServiceError::Unavailable);
        };
        if selected.receipt().result_digest() != source.filing_result
            || selection.identity().receipt().receipt_digest() != source.company_identity
        {
            return Err(ServiceError::InvalidResult);
        }
        let filing =
            market_squawk_valuation::CommonShareFilingEvidence::try_from_filing(&selection)
                .map_err(|_| ServiceError::Unavailable)?;
        let requirement = receipt
            .common_share_source_requirement(filing)
            .map_err(|_| ServiceError::InvalidResult)?;
        let plan = actions
            .read_reference(
                &source.action,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(crate::application::decision::current_share::source_error)?
            .ok_or(ServiceError::Unavailable)?;
        bases.push(
            market_squawk_valuation::CommonShareValuationBasis::try_from_source_plan(
                filing,
                plan.share_plan()
                    .map_err(crate::application::decision::current_share::source_error)?,
                requirement.1,
                requirement.2,
            )
            .map_err(|_| ServiceError::Unavailable)?,
        );
    }
    Ok(FundamentalShareProjectionSources {
        bases,
        reference: reference.into(),
    })
}

pub(super) fn native_model_cases(
    receipt: &AutomaticValuationMethodReceipt,
) -> Result<AutomaticValuationModelCases, ServiceError> {
    let mut sources = Vec::<Arc<ForecastValuationSource>>::new();
    for input in receipt.inputs() {
        if let EvidenceOrigin::ForecastDistribution { evidence } = input.input().evidence().origin()
            && !sources
                .iter()
                .any(|source| source.reference() == evidence.source().reference())
        {
            sources.push(Arc::new(evidence.source().clone()));
        }
    }
    let native = NativeMethodInputs::select(
        &sources,
        receipt.method(),
        receipt.instrument_id(),
        receipt.measurement_at(),
    )?;
    let assumptions = receipt
        .macro_assumptions()
        .ok_or(ServiceError::InvalidResult)?;
    let cap = if receipt.method() == AutomaticValuationMethod::DiscountedCashFlow {
        Some(
            assumptions
                .reference()
                .annual_yield_percent()
                .checked_div(Decimal::from(100))
                .ok_or(ServiceError::InvalidResult)?,
        )
    } else {
        None
    };
    let (lower, upper, identity) = native.range(assumptions.assumption().value(), cap)?;
    let lower = lower.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToNegativeInfinity);
    let upper = upper.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToPositiveInfinity);
    if lower != receipt.range().lower().money().amount()
        || upper != receipt.range().upper().money().amount()
        || !receipt
            .assumptions()
            .iter()
            .filter(|a| {
                matches!(
                    a.kind(),
                    AutomaticValuationAssumptionKind::UncertaintyLower
                        | AutomaticValuationAssumptionKind::UncertaintyUpper
                )
            })
            .all(|a| a.evidence() == identity)
    {
        return Err(ServiceError::InvalidResult);
    }
    let identity = market_squawk_decisions::DecisionContentDigest::try_new(identity)
        .map_err(|_| ServiceError::InvalidResult)?;
    let (sensitivity_lower, sensitivity_upper) = receipt
        .original_share_projection_sensitivity()
        .map_err(|_| ServiceError::InvalidResult)?;
    Ok(AutomaticValuationModelCases {
        calculation_identity: receipt.id(),
        scenarios: market_squawk_decisions::TargetPriceCases::try_new(
            receipt.range().lower().money(),
            receipt.range().central().money(),
            receipt.range().upper().money(),
        )
        .map_err(|_| ServiceError::Unavailable)?,
        scenario_identity: identity,
        sensitivity_range: market_squawk_decisions::TargetPriceRange::try_new(
            sensitivity_lower,
            sensitivity_upper,
        )
        .map_err(|_| ServiceError::Unavailable)?,
        sensitivity_identity: identity,
    })
}

/// Controlled artifact dependencies of the same immutable source recipe used by replay.
pub(crate) fn fundamental_share_recipe_artifacts(
    reference: &[u8],
) -> Result<Vec<market_squawk_services::ArtifactReference>, ServiceError> {
    let sources = decode_fundamental_share_sources(reference)?;
    sources
        .iter()
        .filter_map(|source| source.action.current_recipe_artifact().transpose())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ServiceError::InvalidResult)
}
