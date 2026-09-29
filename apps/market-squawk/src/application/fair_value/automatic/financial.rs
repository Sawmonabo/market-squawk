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
