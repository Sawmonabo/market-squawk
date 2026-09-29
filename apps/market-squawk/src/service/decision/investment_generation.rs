//! Investment publication from exact source and completed-job references.
//! Source acquisition, research jobs and workflow sequencing remain independent operations.

use super::super::{jobs::InstalledJobOperations, portfolio_analysis::InstalledPortfolioAnalysis};
use super::{ensure_live, map_application};
use crate::{
    ResearchService,
    application::{
        MacroContextReadCapability, MarketHistoryReadCapability, MarketHistoryUnavailableReason,
        analysis::{
            GovernedRecommendationBacktestReferenceV1, ProductionGovernedBacktestInputAuthority,
            ProductionGovernedBacktestRepository,
        },
        analytical_profile::{AnalyticalProfileResolution, revalidate},
        decision::{
            DecisionApplication,
            investment_request::{
                ForecastReference, GenerateRequest, digest, parse_digest, timestamp, uuid,
                validate_canonical_request,
            },
            recommendation::{adapt_price_forecast_evidence, adapt_recommendation_backtest_v1},
        },
        fair_value::{
            AutomaticInvestmentValuationRequest, AutomaticInvestmentValuationSources,
            FairValueDomainService, ForecastValuationSourceFactory,
        },
        market_calendar::CompletedMarketSessionReadCapability,
        market_selection::{
            MarketInvestmentReadCapability, MarketInvestmentReadReceipt,
        },
        model::forecast::{
                ExactHorizonPriceForecastEvidence, ForecastEvidenceReadContext,
                ForecastEvidenceReader, ForecastPriceEvidence, LatestValidForecast,
                replay_price_history_inputs,
            },
        SourceAppliedCorporateActionReadCapability,
    },
    jobs::ForecastJobRunner,
    portfolio_application::{
        PortfolioAnalysisCurrentPosition, PortfolioAnalysisLiquidityCapacityAvailability,
        PortfolioAnalysisMarketAvailability, PortfolioAnalysisPrerequisiteResolution,
        PortfolioRecommendationEvidence,
    },
};
use market_squawk_data::Sha256Digest;
use market_squawk_decisions::{
    CandidateId, CurrentShareMarketAdmission, InvestmentAnalysisEvidence, InvestmentAnalysisEvidenceInput,
    InvestmentAnalysisRequestProvenance, InvestmentProbabilityEvidence, LiquidityEvidence,
    PortfolioPositionState,
    PortfolioRiskEvidence, PreparedPublishedInvestmentAnalysis,
    ProbabilityEventEvidence, ProbabilityEventKind, ProbabilityForecastEvidence,
    ProbabilityForecastReference, ProbabilityUnavailableReason, ProposalEvidenceWindow,
    ScreenRunId,
};
use market_squawk_domain::{InstrumentId, Money, Timestamp};
use market_squawk_services::{
    ArtifactReadContext, RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_valuation::ActorId;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

pub(in crate::service) const GENERATE_INVESTMENT_ANALYSIS: &str = "Decision.GenerateInvestmentAnalysis";

/// Reuses existing authorities. It owns no source fetch, model runner, account selector or store.
pub(in crate::service) struct InvestmentGenerationOperations {
    decisions: Arc<DecisionApplication>,
    research: Arc<ResearchService>,
    market: MarketInvestmentReadCapability,
    macro_reader: MacroContextReadCapability,
    calendars: CompletedMarketSessionReadCapability,
    history: MarketHistoryReadCapability,
    forecasts: Arc<dyn ForecastEvidenceReader>,
    fair_value: Arc<FairValueDomainService>,
    valuation_sources: ForecastValuationSourceFactory,
    study_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
    studies: Arc<ProductionGovernedBacktestRepository>,
    historical_reader: Option<Arc<crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability>,
    >,
    maximum_forecast_artifact_bytes: NonZeroUsize,
    source_actions: SourceAppliedCorporateActionReadCapability,
}
impl InvestmentGenerationOperations {
    /// Shares existing read authorities only; chart reads cannot acquire or publish new inputs.
    pub(super) fn chart_reader(&self,
    ) -> super::investment_analysis::chart::SavedInvestmentChartReader {
        super::investment_analysis::chart::SavedInvestmentChartReader {
            research: Arc::clone(&self.research),
            calendars: self.calendars.clone(),
            history: self.history.clone(),
            forecasts: Arc::clone(&self.forecasts),
            maximum_forecast_artifact_bytes: self.maximum_forecast_artifact_bytes,
        }
    }


    #[allow(
        clippy::too_many_arguments,
        reason = "existing independent authorities retain their ownership"
    )]
    pub(in crate::service) fn new(
        decisions: Arc<DecisionApplication>,
        research: Arc<ResearchService>,
        market: MarketInvestmentReadCapability,
        macro_reader: MacroContextReadCapability,
        calendars: CompletedMarketSessionReadCapability,
        history: MarketHistoryReadCapability,
        forecasts: Arc<dyn ForecastEvidenceReader>,
        fair_value: Arc<FairValueDomainService>,
        valuation_sources: ForecastValuationSourceFactory,
        study_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
        studies: Arc<ProductionGovernedBacktestRepository>,
        historical_reader: Option<Arc<crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability>,
        >,
        maximum_forecast_artifact_bytes: NonZeroUsize,
        source_actions: SourceAppliedCorporateActionReadCapability,
    ) -> Self {
        Self {
            decisions,
            research,
            market,
            macro_reader,
            calendars,
            history,
            forecasts,
            fair_value,
            valuation_sources,
            study_inputs,
            studies,
            historical_reader,
            maximum_forecast_artifact_bytes,
            source_actions,
        }
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        jobs: &InstalledJobOperations,
        runner: &ForecastJobRunner,
        portfolios: &InstalledPortfolioAnalysis,
        preparation: &super::super::forecast_preparation::InstalledForecastPreparation,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.name() != GENERATE_INVESTMENT_ANALYSIS {
            return Err(ServiceError::NotFound);
        }
        ensure_live(context)?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let input: GenerateRequest = serde_json::from_value(Value::Object(
            super::super::business_arguments(request.arguments()),
        ))
        .map_err(invalid)?;
        if input.financial_forecasts.len() > 16 {
            return Err(ServiceError::ResourceExhausted);
        }
        let workflow = input.workflow.domain()?;
        let canonical_request = serde_json::to_vec(&input).map_err(invalid)?;
        validate_canonical_request(&canonical_request)?;
        let request_provenance = InvestmentAnalysisRequestProvenance::try_new(
            *origin.workspace_id().as_bytes(),
            canonical_request.into_boxed_slice(),
        )
        .map_err(invalid)?;
        let request_digest = request_provenance.request_digest();
        // A completed exact retry returns the original durable bundle before current reads.
        if let Some(bundle) = self
            .decisions
            .get_generated_investment_analysis(&workflow, request_digest)
            .map_err(map_application)?
        {
            return self.result(&bundle, context);
        }
        let models = preparation.financial_profile_catalog(context).await?;
        let validated =
            revalidate(&input.financial_profile, models.as_ref()).map_err(ServiceError::from)?;
        let profile = input.analytical_profile.domain()?;
        let source_cutoff = timestamp(&input.source_cutoff_unix_nanos)?;
        let portfolio_cutoff = input
            .portfolio
            .prerequisites()
            .source_cutoff()
            .map_err(|e| e.as_service_error())?;
        let now = current_time()?;
        let instrument = input.portfolio.prerequisites().candidate_instrument_id();
        let market_cutoff = portfolio_cutoff;
        let account = input.portfolio.prerequisites().account_id();
        if source_cutoff > market_cutoff || market_cutoff > now {
            return Err(ServiceError::InvalidRequest);
        }
        let maximum_age = u64::try_from(
            validated
                .recommendation_policy()
                .parameters()
                .market_max_age_nanos,
        )
        .map_err(invalid)?;
        let market_reader = self.market.with_maximum_mark_age_nanos(maximum_age)?;
        let market = match &input.market {
            Some(reference) => {
                if reference.source_cutoff()? != market_cutoff
                    || reference.instrument_id() != instrument
                    || reference.maximum_mark_age_nanos()? != maximum_age
                {
                    return Err(ServiceError::InvalidRequest);
                }
                Some(
                    market_reader
                        .read_reference(
                            reference,
                            context.deadline(),
                            context.cancellation().clone(),
                        )
                        .await?,
                )
            }
            None => {
                // Null is a request to recheck absence, never caller-issued missing evidence.
                if market_reader
                    .read(
                        instrument,
                        market_cutoff,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await?
                    .is_some()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                None
            }
        };
        let currency = match &market {
            Some(market) => market
                .observation()
                .map_err(|_| ServiceError::Unavailable)?
                .mark()
                .currency(),
            None => {
                let portfolio = portfolios
                    .read_reference(
                        &input.portfolio,
                        &input.financial_profile,
                        models.as_ref(),
                        context,
                    )
                    .await?;
                match portfolio {
                    PortfolioAnalysisPrerequisiteResolution::Evaluated(value) => {
                        value.portfolio().reporting_currency()
                    }
                    PortfolioAnalysisPrerequisiteResolution::Unavailable(value) => {
                        value.portfolio().reporting_currency()
                    }
                    PortfolioAnalysisPrerequisiteResolution::SetupRequired { .. } => {
                        return Err(ServiceError::Unavailable);
                    }
                }
            }
        };
        let horizon = NonZeroU64::new(
            u64::try_from(validated.recommendation_policy().horizon_nanos()).map_err(invalid)?,
        )
        .ok_or_else(|| invalid(()))?;
        let price = match &input.price_forecast {
            Some(reference) => Some(
                self.forecast(
                    reference,
                    instrument,
                    &input.financial_profile,
                    jobs,
                    runner,
                    context,
                )
                .await?,
            ),
            None => None,
        };
        let mut financial = Vec::new();
        financial
            .try_reserve_exact(input.financial_forecasts.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (index, reference) in input.financial_forecasts.iter().enumerate() {
            if input.financial_forecasts[..index]
                .iter()
                .any(|prior| prior.forecast_token == reference.forecast_token)
                || input
                    .price_forecast
                    .as_ref()
                    .is_some_and(|price| price.forecast_token == reference.forecast_token)
            {
                return Err(ServiceError::InvalidRequest);
            }
            financial.push(
                self.forecast(
                    reference,
                    instrument,
                    &input.financial_profile,
                    jobs,
                    runner,
                    context,
                )
                .await?,
            );
        }
        for selected in price.iter().chain(financial.iter()) {
            let serving = match selected.price_evidence() {
                ForecastPriceEvidence::Available(value) => value.serving_evidence(),
                ForecastPriceEvidence::Unavailable(value) => value.serving_evidence(),
            };
            if serving.knowledge_cutoff() > source_cutoff {
                return Err(ServiceError::InvalidRequest);
            }
        }
        let expected_cost_policy = if input.probability_forecasts.profit_after_costs.is_some() {
            Some(
                self.study_inputs
                    .probability_cost_policy(currency, validated.execution_assumptions())?,
            )
        } else {
            None
        };
        let probability_references = input.probability_forecasts.entries();
        let mut probability_tokens = std::collections::BTreeSet::new();
        let mut probability_events = Vec::with_capacity(3);
        for (kind, reference) in [
            ProbabilityEventKind::PriceHigher,
            ProbabilityEventKind::BenchmarkOutperformance,
            ProbabilityEventKind::ProfitAfterCosts,
        ]
        .into_iter()
        .zip(probability_references)
        {
            if let Some(reference) = reference {
                if !probability_tokens.insert(reference.forecast_token.as_str())
                    || input
                        .price_forecast
                        .as_ref()
                        .is_some_and(|value| value.forecast_token == reference.forecast_token)
                    || input
                        .financial_forecasts
                        .iter()
                        .any(|value| value.forecast_token == reference.forecast_token)
                {
                    return Err(ServiceError::InvalidRequest);
                }
                probability_events.push(
                    self.probability_forecast(
                        kind,
                        reference,
                        instrument,
                        horizon,
                        source_cutoff,
                        input.benchmark_instrument_id,
                        expected_cost_policy,
                        &input.financial_profile,
                        jobs,
                        runner,
                        context,
                    )
                    .await?,
                );
            } else {
                probability_events.push(ProbabilityEventEvidence::Unavailable {
                    kind,
                    target: None,
                    reference: None,
                    reason: ProbabilityUnavailableReason::ForecastEvidenceUnavailable,
                });
            }
        }
        let mut forecast_evidence = None;
        let mut observed_through = source_cutoff;
        if let Some(price) = &price {
            match price
                .exact_horizon_price_projection(horizon)
                .map_err(|_| ServiceError::InvalidResult)?
            {
                ExactHorizonPriceForecastEvidence::Available(projection) => {
                    if projection.currency() != currency {
                        return Err(ServiceError::InvalidRequest);
                    }
                    observed_through = projection.observed_through();
                    forecast_evidence = Some(
                        adapt_price_forecast_evidence(
                            projection,
                            validated.recommendation_policy(),
                            source_cutoff,
                            current_time()?,
                        )
                        .map_err(|_| ServiceError::InvalidResult)?,
                    );
                }
                ExactHorizonPriceForecastEvidence::Unavailable(_) => {}
            }
        }
        let expected_probability_origin = if forecast_evidence.is_some() {
            Some(observed_through)
        } else {
            probability_events.iter().find_map(|value| match value {
                ProbabilityEventEvidence::Ready(value) => Some(value.record().observed_at),
                _ => None,
            })
        };
        for event in &mut probability_events {
            if let ProbabilityEventEvidence::Ready(value) = event {
                if expected_probability_origin
                    .is_some_and(|origin| origin != value.record().observed_at)
                {
                    *event = ProbabilityEventEvidence::Unavailable {
                        kind: value.kind(),
                        target: Some(value.target()),
                        reference: Some(value.reference()),
                        reason: ProbabilityUnavailableReason::OriginMismatch,
                    };
                }
            }
        }
        let [price_probability, benchmark_probability, cost_probability]: [ProbabilityEventEvidence;
            3] = probability_events
            .try_into()
            .map_err(|_| ServiceError::Internal)?;
        let probabilities = InvestmentProbabilityEvidence::try_new(
            instrument,
            horizon,
            price_probability,
            benchmark_probability,
            cost_probability,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let benchmark_comparison = crate::application::saved_benchmark::prepare(
            &self.research,
            &self.history,
            &self.calendars,
            crate::application::saved_benchmark::SavedBenchmarkRequest {
                subject: instrument,
                requested: input.benchmark_instrument_id,
                currency,
                source_cutoff,
                observed_through,
            },
            context,
        )
        .await?;
        let study = match &input.historical_study {
            Some(reference) => optional_source(
                self.studies
                    .read_recommendation(
                        &self.study_inputs,
                        GovernedRecommendationBacktestReferenceV1 {
                            request_digest: Sha256Digest::new(
                                parse_digest(&reference.request_digest)?.bytes(),
                            ),
                            evidence_digest: Sha256Digest::new(
                                parse_digest(&reference.evidence_digest)?.bytes(),
                            ),
                        },
                        current_time()?,
                        self.historical_reader
                            .as_deref()
                            .ok_or(ServiceError::Unavailable)?,
                        context,
                    )
                    .await,
            )?,
            None => None,
        };
        let (backtest, out_of_sample) = match &study {
            Some(study) => match adapt_recommendation_backtest_v1(study) {
                Ok(value) => {
                    if value.historical_test.simulation_cutoff_at() > source_cutoff {
                        return Err(ServiceError::InvalidRequest);
                    }
                    (Some(value.historical_test), Some(value.out_of_sample))
                }
                Err(_) => (None, None),
            },
            None => (None, None),
        };
        let selected_candidate = match &input.selected_candidate {
            Some(selected) => Some(
                self.decisions
                    .resolve_selected_candidate(
                        &CandidateId::try_new(selected.candidate_id.clone()).map_err(invalid)?,
                        &ScreenRunId::try_new(selected.screen_run_id.clone()).map_err(invalid)?,
                        digest(parse_digest(&selected.evidence_digest)?)?,
                    )
                    .map_err(map_application)?,
            ),
            None => None,
        };
        let forecast_chart = match (
            price.as_ref(),
            forecast_evidence.as_ref(),
            input.source_action_reference.as_ref(),
        ) {
            (Some(selected), Some(_), Some(reference)) => {
                let ForecastPriceEvidence::Available(price) = selected.price_evidence() else {
                    return Err(ServiceError::InvalidResult);
                };
                replay_price_history_inputs(
                    price,
                    &self.research,
                    &crate::application::market_calendar::ForecastSessionReadCapability::Current(self.calendars.clone()),
                    &self.source_actions,
                    reference,
                    None,
                    context,
                )
                .await?
            }
            _ => None,
        };
        let Some(market) = market else {
            // Reopen the same account revision and original cutoff after the completed-job
            // reads. Neither a missing mark nor historical forecasts establish current risk.
            let portfolio = portfolios
                .read_reference(
                    &input.portfolio,
                    &input.financial_profile,
                    models.as_ref(),
                    context,
                )
                .await?;
            let original_portfolio = match &portfolio {
                PortfolioAnalysisPrerequisiteResolution::Evaluated(value) => value.portfolio(),
                PortfolioAnalysisPrerequisiteResolution::Unavailable(value) => value.portfolio(),
                PortfolioAnalysisPrerequisiteResolution::SetupRequired { .. } => {
                    return Err(ServiceError::Unavailable);
                }
            };
            if original_portfolio.account_id() != account
                || original_portfolio.reporting_currency() != currency
            {
                return Err(ServiceError::InvalidResult);
            }
            if market_reader
                .read(
                    instrument,
                    market_cutoff,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?
                .is_some()
            {
                return Err(ServiceError::Unavailable);
            }
            ensure_live(context)?;
            let mut evidence = InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
                instrument_id: instrument,
                currency,
                account_id: account,
                as_of: source_cutoff,
                admitted_at: current_time()?,
                market: None,
                price_forecast: forecast_evidence,
                valuation: None,
                financial_model: None,
                backtest,
                out_of_sample,
                harmonic_pattern: None,
                liquidity: None,
                portfolio_risk: None,
            });
            evidence = evidence
                .try_with_benchmark_comparison(benchmark_comparison.clone())
                .map_err(|_| ServiceError::InvalidResult)?;
            evidence = evidence
                .try_with_probabilities(probabilities.clone())
                .map_err(|_| ServiceError::InvalidResult)?;
            if let Some(history) = &forecast_chart {
                evidence = evidence
                    .try_with_forecast_chart(history.saved.clone())
                    .map_err(|_| ServiceError::InvalidResult)?;
            }
            if let Some(selected) = selected_candidate {
                evidence = evidence
                    .try_with_selected_candidate(selected)
                    .map_err(|_| ServiceError::InvalidRequest)?;
            }
            // The original pure authority produces MissingEvidence(Market), preserving real
            // forecasts/studies. No all-method valuation attempt or action is fabricated.
            let bundle = self.decisions.generate_published_investment_analysis(
                evidence,
                validated.recommendation_policy().clone(),
                profile,
                workflow,
                request_provenance,
                None,
                context.deadline(),
                context.cancellation(),
            );
            ensure_live(context)?;
            return self.result(&bundle.map_err(map_application)?, context);
        };
        let observation = market
            .observation()
            .map_err(|_| ServiceError::Unavailable)?;
        let horizon_at = observed_through
            .checked_add_nanos(validated.recommendation_policy().horizon_nanos())
            .map_err(invalid)?;
        let expires_at = observation
            .mark()
            .fresh_until()
            .ok_or(ServiceError::Unavailable)?
            .checked_add_nanos(1)
            .map_err(invalid)?
            .min(market.authorization_expires_at());
        if expires_at <= current_time()? {
            return Err(ServiceError::Unavailable);
        }
        let premium = self
            .macro_reader
            .read_default_equity_premium_from_store(
                &self.research,
                &self.calendars,
                source_cutoff,
                source_cutoff,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|e| e.into_service_error());
        let valuation = self
            .fair_value
            .evaluate_automatic_investment_valuations(
                Arc::clone(&self.research),
                &self.macro_reader,
                &self.calendars,
                &self.market,
                &market,
                &self.valuation_sources,
                AutomaticInvestmentValuationSources {
                    price_forecast: price.as_ref(),
                    financial_forecasts: &financial,
                    equity_premium: premium.as_ref().map_err(Clone::clone),
                },
                AutomaticInvestmentValuationRequest {
                    account_id: account,
                    instrument_id: instrument,
                    knowledge_at: source_cutoff,
                    horizon_at,
                    expires_at,
                    calculated_by: ActorId::try_from("installed-investment-analysis")
                        .map_err(invalid)?,
                    profile_identity: parse_digest(&input.financial_profile.configuration_digest)?,
                },
                context,
            )
            .await?;
        let selected_receipt = valuation.selected_receipt().cloned();
        let (method_set, selected_valuation) = valuation.into_parts();
        let (valuation, financial_model) =
            selected_valuation.map_or((None, None), |(v, f)| (Some(v), Some(f)));
        let harmonic_result = if let Some(history) = &forecast_chart {
            Some(
                self.history
                    .read_forecast_basis_harmonic(
                        &self.research,
                        &history.history,
                        market.execution_terms().map(|terms| terms.price_tick()),
                        None,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await,
            )
        } else {
            None
        };
        ensure_live(context)?;
        let harmonic = match harmonic_result {
            Some(Ok(value)) => Some(value),
            None => None,
            Some(Err(MarketHistoryUnavailableReason::Cancelled)) => {
                return Err(ServiceError::Cancelled);
            }
            Some(Err(MarketHistoryUnavailableReason::DeadlineExceeded)) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            Some(Err(MarketHistoryUnavailableReason::CapacityExceeded)) => {
                return Err(ServiceError::ResourceExhausted);
            }
            Some(Err(MarketHistoryUnavailableReason::StorageUnavailable)) => {
                return Err(ServiceError::Unavailable);
            }
            Some(Err(MarketHistoryUnavailableReason::IntegrityUnproven)) => {
                return Err(ServiceError::InvalidResult);
            }
        };
        // Reopen current prerequisites after potentially long valuation/study reads.
        let portfolio = portfolios
            .read_reference(
                &input.portfolio,
                &input.financial_profile,
                models.as_ref(),
                context,
            )
            .await?;
        let (liquidity, portfolio_risk) = match &portfolio {
            PortfolioAnalysisPrerequisiteResolution::Evaluated(value) => {
                if value.portfolio().account_id() != account
                    || value.portfolio().reporting_currency() != currency
                {
                    return Err(ServiceError::InvalidRequest);
                }
                current_portfolio_evidence(value, &market)?
            }
            PortfolioAnalysisPrerequisiteResolution::Unavailable(value) => {
                if value.portfolio().account_id() != account
                    || value.portfolio().reporting_currency() != currency
                {
                    return Err(ServiceError::InvalidRequest);
                }
                (None, None)
            }
            PortfolioAnalysisPrerequisiteResolution::SetupRequired { .. } => {
                return Err(ServiceError::Unavailable);
            }
        };
        self.market
            .recheck(&market, context.deadline(), context.cancellation().clone())
            .await?;
        ensure_live(context)?;
        let admitted_at = current_time()?;
        let sizing_inputs = super::investment_projection::current_sizing_inputs(
            &portfolio,
            &market,
            admitted_at,
            &validated,
            context,
        )?;
        let mut evidence = InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
            instrument_id: instrument,
            currency,
            account_id: account,
            as_of: source_cutoff,
            admitted_at,
            market: Some(crate::application::decision::current_share::market_evidence(&market, market.authorization_expires_at())?),
            price_forecast: forecast_evidence,
            valuation,
            financial_model,
            backtest,
            out_of_sample,
            harmonic_pattern: harmonic
                .as_ref()
                .and_then(|value| value.pattern_receipt())
                .cloned(),
            liquidity,
            portfolio_risk,
        })
        .try_with_valuation_method_set(method_set)
        .map_err(|_| ServiceError::InvalidResult)?;
        evidence = evidence
            .try_with_benchmark_comparison(benchmark_comparison)
            .map_err(|_| ServiceError::InvalidResult)?;
        evidence = evidence
            .try_with_probabilities(probabilities)
            .map_err(|_| ServiceError::InvalidResult)?;
        if let Some(history) = &forecast_chart {
            evidence = evidence
                .try_with_forecast_chart(history.saved.clone())
                .map_err(|_| ServiceError::InvalidResult)?;
        }
        if let Some(harmonic) = &harmonic {
            evidence = evidence
                .try_with_harmonic_history(harmonic.audit().clone())
                .map_err(|_| ServiceError::InvalidResult)?;
        }
        if let Some(selected) = selected_candidate {
            evidence = evidence
                .try_with_selected_candidate(selected)
                .map_err(|_| ServiceError::InvalidRequest)?;
        }
        if let (Some(history), Some(receipt), Some(reference)) = (
            forecast_chart.as_ref(), selected_receipt.as_ref(), input.current_share_action_reference.as_ref(),
        ) {
            let conversion = self.source_actions.read_current_forecast_share_conversion(
                reference, &history.epoch, &history.history, &history.original_plan, &market,
                u32::from(receipt.range().central().scale()), context.deadline(), context.cancellation().clone(),
            ).await.map_err(crate::application::decision::current_share::source_error)?;
            if let Some(conversion) = conversion {
                let valuation_projection = receipt.project_current_share_units(&history.epoch, &conversion, admitted_at)
                    .map_err(|_| ServiceError::InvalidResult)?;
                let admission = CurrentShareMarketAdmission {
                    market: *evidence.market().ok_or(ServiceError::InvalidResult)?,
                    authorized_at: market.authorized_at(),
                    authorization_expires_at: market.authorization_expires_at(),
                    authorization_decision_digest: digest(market_squawk_domain::EvidenceDigest::new(
                        market_squawk_domain::DigestAlgorithm::Sha256, market.authorization_decision_digest(),
                    ))?,
                };
                evidence = evidence.try_project_current_share_units(conversion, valuation_projection,
                    market.publication(), admission).map_err(|_| ServiceError::InvalidResult)?;
            }
        }
        let bundle = self.decisions.generate_published_investment_analysis(
            evidence,
            validated.recommendation_policy().clone(),
            profile,
            workflow,
            request_provenance,
            sizing_inputs,
            context.deadline(),
            context.cancellation(),
        );
        // Publication may have committed before the caller stopped waiting. Preserve its
        // durable identity for exact retry while reporting the real request lifetime.
        ensure_live(context)?;
        let bundle = bundle.map_err(map_application)?;
        self.result(&bundle, context)
    }

    async fn forecast(
        &self,
        reference: &ForecastReference,
        instrument: InstrumentId,
        profile: &AnalyticalProfileResolution,
        jobs: &InstalledJobOperations,
        runner: &ForecastJobRunner,
        context: &RequestContext,
    ) -> Result<LatestValidForecast, ServiceError> {
        let token = uuid(&reference.forecast_token)?;
        let result = jobs
            .read_forecast_reference(runner, &reference.job_id, reference.generation, context)
            .await?;
        if result.request_sha256 != parse_digest(&reference.request_sha256)?.bytes()
            || result.financial_profile_digest
                != Some(parse_digest(&profile.configuration_digest)?.bytes())
            || result
                .result
                .structured_content()
                .get("forecastToken")
                .and_then(Value::as_str)
                != Some(reference.forecast_token.as_str())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let selected = self
            .forecasts
            .exact_distribution_for_vintage(
                token,
                instrument,
                current_time()?,
                ForecastEvidenceReadContext::new(
                    ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
                    self.maximum_forecast_artifact_bytes,
                ),
            )
            .await;
        ensure_live(context)?;
        selected.map_err(|error| {
            use crate::application::model::forecast::ForecastApplicationError;

            match error {
                ForecastApplicationError::InvalidRecord => ServiceError::InvalidResult,
                ForecastApplicationError::InvalidLimits => ServiceError::Internal,
                error => crate::application::model::map_forecast_selection_error(error),
            }
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "original job, event and source authority remain explicit"
    )]
    async fn probability_forecast(
        &self,
        kind: ProbabilityEventKind,
        reference: &ForecastReference,
        instrument: InstrumentId,
        horizon: NonZeroU64,
        source_cutoff: Timestamp,
        benchmark_instrument_id: Option<InstrumentId>,
        expected_cost_policy: Option<market_squawk_data::ProbabilityCostPolicyV1>,
        profile: &AnalyticalProfileResolution,
        jobs: &InstalledJobOperations,
        runner: &ForecastJobRunner,
        context: &RequestContext,
    ) -> Result<ProbabilityEventEvidence, ServiceError> {
        let token = uuid(&reference.forecast_token)?;
        let result = jobs
            .read_forecast_reference(runner, &reference.job_id, reference.generation, context)
            .await?;
        if result.request_sha256 != parse_digest(&reference.request_sha256)?.bytes()
            || result.financial_profile_digest
                != Some(parse_digest(&profile.configuration_digest)?.bytes())
            || result
                .result
                .structured_content()
                .get("forecastToken")
                .and_then(Value::as_str)
                != Some(reference.forecast_token.as_str())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let source_reference = ProbabilityForecastReference::try_new(
            *uuid(&reference.job_id)?.as_bytes(),
            NonZeroU64::new(reference.generation).ok_or(ServiceError::InvalidRequest)?,
            *token.as_bytes(),
            digest(parse_digest(&reference.request_sha256)?)?,
            digest(parse_digest(&profile.configuration_digest)?)?,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let selected = match self
            .forecasts
            .exact_probability_for_vintage(
                token,
                current_time()?,
                ForecastEvidenceReadContext::new(
                    ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
                    self.maximum_forecast_artifact_bytes,
                ),
            )
            .await
        {
            Ok(value) => value,
            Err(crate::application::model::forecast::ForecastApplicationError::NotFound) => {
                return Ok(ProbabilityEventEvidence::Unavailable {
                    kind,
                    target: None,
                    reference: Some(source_reference),
                    reason: ProbabilityUnavailableReason::ForecastEvidenceUnavailable,
                });
            }
            Err(error) => {
                return Err(crate::application::model::map_forecast_selection_error(
                    error,
                ));
            }
        };
        ensure_live(context)?;
        let original = selected.vintage();
        let target = original.path().output_binding().target();
        if original.path().instrument_id() != instrument
            || selected.source_knowledge_cutoff() > source_cutoff
            || ProbabilityEventKind::from_target(target).map_err(|_| ServiceError::InvalidResult)?
                != kind
        {
            return Err(ServiceError::InvalidRequest);
        }
        if !matches!(target, market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent {horizon_nanos,..} if horizon_nanos==horizon)
        {
            return Ok(ProbabilityEventEvidence::Unavailable {
                kind,
                target: Some(target),
                reference: Some(source_reference),
                reason: ProbabilityUnavailableReason::HorizonMismatch,
            });
        }
        if kind == ProbabilityEventKind::BenchmarkOutperformance {
            let selection =
                crate::application::RecommendationBenchmarkSelectionReadCapability::new(
                    self.research.market_data_instruments(),
                )
                .select_comparison(
                    benchmark_instrument_id,
                    source_cutoff,
                    source_cutoff,
                    context.deadline(),
                    context.cancellation(),
                )?;
            let matches = selection.is_some_and(|selected| matches!(target,
                market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent {
                    event: market_squawk_data::ProbabilityEventTarget::BenchmarkOutperformance {
                        benchmark_instrument_id, benchmark_definition }, .. }
                if benchmark_instrument_id == selected.instrument_id() && benchmark_definition == selected.reference_revision_digest()));
            if !matches {
                return Ok(ProbabilityEventEvidence::Unavailable {
                    kind,
                    target: Some(target),
                    reference: Some(source_reference),
                    reason: ProbabilityUnavailableReason::BenchmarkEvidenceUnavailable,
                });
            }
        }
        if kind == ProbabilityEventKind::ProfitAfterCosts
            && !matches!(target,
            market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent {
                event: market_squawk_data::ProbabilityEventTarget::ProfitAfterCosts {policy},.. }
                if Some(policy) == expected_cost_policy)
        {
            return Ok(ProbabilityEventEvidence::Unavailable {
                kind,
                target: Some(target),
                reference: Some(source_reference),
                reason: ProbabilityUnavailableReason::CostEvidenceUnavailable,
            });
        }
        let evidence = ProbabilityForecastEvidence::try_from_forecast(
            source_reference,
            original,
            selected.model_metadata(),
            selected.source_knowledge_cutoff(),
            digest(market_squawk_domain::EvidenceDigest::new(
                market_squawk_domain::DigestAlgorithm::Sha256,
                selected.source_selection_sha256().bytes(),
            ))?,
            digest(market_squawk_domain::EvidenceDigest::new(
                market_squawk_domain::DigestAlgorithm::Sha256,
                selected.source_feature_sha256().bytes(),
            ))?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        Ok(ProbabilityEventEvidence::Ready(evidence))
    }

    fn result(
        &self,
        bundle: &PreparedPublishedInvestmentAnalysis,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let decision = bundle.decision();
        let token = self
            .decisions
            .investment_analysis_product_token(decision.analysis_id())
            .map_err(map_application)?;
        TypedToolResult::try_new(json!({"actionToken":token,"analysisId":hex(decision.analysis_id().bytes()),
            "explanationDigest":hex(bundle.explanation().explanation_digest().bytes()),
            "publishedAtUnixNanos":bundle.publication().published_at().unix_nanos().to_string(),
            "valuationMethodSetIdentity":decision.evidence().valuation_method_set().map(|value|hex(value.identity().bytes())),
            "outcomeProjection":bundle.outcome_projection().map(super::investment_analysis::outcome_projection_value),
            "sizing":bundle.sizing_projection().map(super::investment_analysis::sizing_projection_value)
                .unwrap_or_else(||super::investment_analysis::unavailable_sizing_value(bundle.sizing_price_scale_unavailable(),decision.proposal_id().is_some())),
            "expectedReturn":bundle.outcome_projection()
                .map(|value|super::investment_analysis::expected_return_value(value.expected_return()))
                .unwrap_or_else(super::investment_analysis::unavailable_expected_return_value),
        }),1,ToolResultMetadata::complete_not_applicable(),context.limits()).map_err(Into::into)
    }
}


fn current_portfolio_evidence(
    value: &PortfolioRecommendationEvidence,
    market: &MarketInvestmentReadReceipt,
) -> Result<(Option<LiquidityEvidence>, Option<PortfolioRiskEvidence>), ServiceError> {
    let instrument = market.reference().instrument_id();
    let entry = value
        .markets()
        .entry(instrument)
        .ok_or(ServiceError::InvalidResult)?;
    let PortfolioAnalysisMarketAvailability::Available {
        market: selected,
        liquidity: source_liquidity,
    } = entry.availability()
    else {
        return Ok((None, None));
    };
    let observed = market
        .observation()
        .map_err(|_| ServiceError::Unavailable)?;
    let capacity = value.liquidity_capacity();
    if selected.selection().receipt_digest() != observed.selection_digest()
        || capacity.market_selection_digest() != observed.selection_digest()
        || capacity.market_observation_digest() != selected.observation().observation_digest()
        || selected.observation().unit_mark()
            != Money::new(observed.mark().value(), observed.mark().currency())
        || selected.observation().observed_at() != observed.timestamps().effective_at()
        || selected.observation().available_at() != observed.timestamps().available_at()
    {
        return Err(ServiceError::InvalidResult);
    }
    let expires = value
        .marked_portfolio()
        .holdings()
        .iter()
        .map(|holding| holding.fresh_until())
        .fold(selected.observation().fresh_until(), Timestamp::min)
        .min(market.authorization_expires_at());
    let cutoff = value.evaluated_at();
    let window = |identity| {
        ProposalEvidenceWindow::try_from_derived(
            cutoff,
            cutoff,
            value.calculated_at(),
            expires,
            digest(identity)?,
        )
        .map_err(|_| ServiceError::InvalidResult)
    };
    let side = |value: &PortfolioAnalysisLiquidityCapacityAvailability| match value {
        PortfolioAnalysisLiquidityCapacityAvailability::Available(value) => {
            Some(value.capacity_ppm())
        }
        PortfolioAnalysisLiquidityCapacityAvailability::Unavailable(_) => None,
    };
    let liquidity = capacity
        .quoted_spread_basis_points()
        .map(|spread| {
            LiquidityEvidence::try_new(
                instrument,
                value.portfolio().reporting_currency(),
                spread,
                side(capacity.buy_add()),
                side(capacity.trim_sell()),
                selected.observation().quality(),
                digest(capacity.evidence_digest())?,
                ProposalEvidenceWindow::try_from_derived(
                    source_liquidity
                        .observed_at()
                        .ok_or(ServiceError::InvalidResult)?,
                    cutoff,
                    value.calculated_at(),
                    source_liquidity
                        .fresh_until()
                        .unwrap_or(expires)
                        .min(expires),
                    digest(capacity.evidence_digest())?,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            )
            .map_err(|_| ServiceError::InvalidResult)
        })
        .transpose()?;
    let position = match value.marked_portfolio().current_position() {
        PortfolioAnalysisCurrentPosition::NoPosition => PortfolioPositionState::NoPosition,
        PortfolioAnalysisCurrentPosition::Position {
            quantity,
            marked_value,
        } => {
            let upper = value
                .marked_portfolio()
                .marked_equity()
                .amount()
                .checked_mul(Decimal::from(
                    value
                        .portfolio()
                        .setup()
                        .setup()
                        .profile()
                        .preferred_position_weight_upper_bps(),
                ))
                .and_then(|value| value.checked_mul(Decimal::new(1, 4)))
                .ok_or(ServiceError::InvalidResult)?;
            // Local allocation policy authorizes research consideration only; current exposure
            // and actual risk/capacity remain independently checked by the sole proposal authority.
            PortfolioPositionState::Position {
                add_allowed: quantity > Decimal::ZERO && marked_value.amount() < upper,
                trim_allowed: quantity > Decimal::ZERO,
                exit_allowed: quantity > Decimal::ZERO,
            }
        }
    };
    let risk = PortfolioRiskEvidence::try_new(
        instrument,
        value.portfolio().account_id(),
        value.portfolio().reporting_currency(),
        value.portfolio().revision().clone(),
        position,
        value.risk().risk_capacity_ppm(),
        digest(value.risk().evidence_digest())?,
        window(value.risk().evidence_digest())?,
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    Ok((liquidity, Some(risk)))
}

fn hex(value: [u8; 32]) -> String {
    crate::application::model::forecast_preparation::hex(Sha256Digest::new(value))
}
fn invalid<T>(_: T) -> ServiceError {
    ServiceError::InvalidRequest
}
fn current_time() -> Result<Timestamp, ServiceError> {
    super::super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)
}

fn optional_source<T>(result: Result<T, ServiceError>) -> Result<Option<T>, ServiceError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ServiceError::NotFound | ServiceError::Unavailable) => Ok(None),
        Err(error) => Err(error),
    }
}
