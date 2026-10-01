//! Finite four-method execution over already authenticated product sources.

use super::*;
use crate::application::fair_value::forecast_source::ForecastValuationSourceFactory;
use crate::application::{
    decision::recommendation::derive_default_financial_model_macro_assumptions,
    model::forecast::LatestValidForecast,
    research::{HistoricalEquityPremiumRead, MacroContextReadCapability},
};
use market_squawk_decisions::{FinancialModelEvidence, ValuationEvidence};
use market_squawk_services::ArtifactReadContext;
use market_squawk_valuation::{
    AutomaticValuationAttemptAudit, AutomaticValuationFailure, AutomaticValuationForecastPurpose,
    AutomaticValuationForecastReadAudit, AutomaticValuationMethod,
    AutomaticValuationMethodSetAudit, AutomaticValuationRecommendationAudit,
    AutomaticValuationRecommendationOutcome, AutomaticValuationResultAudit,
    AutomaticValuationStage, ForecastValuationSource,
};
use std::num::NonZeroU64;

/// Values are actual source reads. A failed default premium read remains its real service error.
pub(crate) struct AutomaticInvestmentValuationSources<'a> {
    pub(crate) price_forecast: Option<&'a LatestValidForecast>,
    pub(crate) financial_forecasts: &'a [LatestValidForecast],
    pub(crate) equity_premium: Result<&'a HistoricalEquityPremiumRead, ServiceError>,
}

/// Admitted generation coordinates; no financial values or model target periods enter here.
pub(crate) struct AutomaticInvestmentValuationRequest {
    pub(crate) account_id: AccountId,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) knowledge_at: Timestamp,
    pub(crate) horizon_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) calculated_by: ActorId,
    pub(crate) profile_identity: EvidenceDigest,
}

/// One actually executed method, including its precise source-stage failure.
#[derive(Clone, Debug)]
struct AutomaticInvestmentValuationAttempt {
    method: AutomaticValuationMethod,
    /// Exact source clock captured before this method evaluates its admitted inputs.
    source_knowledge_at: Timestamp,
    started_at: Timestamp,
    completed_at: Timestamp,
    stage: AutomaticValuationStage,
    result: Result<AutomaticValuationPublication, ServiceError>,
}

pub(crate) struct AutomaticInvestmentValuationEvaluation {
    completion: AutomaticValuationMethodSetAudit,
    selected: Option<(ValuationEvidence, FinancialModelEvidence)>,
    selected_receipt: Option<market_squawk_valuation::AutomaticValuationMethodReceipt>,
    selected_share_sources: Option<super::FundamentalShareProjectionSources>,
}
impl AutomaticInvestmentValuationEvaluation {
    pub(crate) const fn completion(&self) -> &AutomaticValuationMethodSetAudit {
        &self.completion
    }
    pub(crate) const fn selected_receipt(
        &self,
    ) -> Option<&market_squawk_valuation::AutomaticValuationMethodReceipt> {
        self.selected_receipt.as_ref()
    }
    pub(crate) fn selected_share_sources(
        &self,
    ) -> Option<&super::FundamentalShareProjectionSources> {
        self.selected_share_sources.as_ref()
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        AutomaticValuationMethodSetAudit,
        Option<(ValuationEvidence, FinancialModelEvidence)>,
    ) {
        (self.completion, self.selected)
    }
}

impl FairValueDomainService {
    /// Executes every method, retaining actual failures; controls never become fabricated outcomes.
    pub(crate) async fn evaluate_automatic_investment_valuations(
        &self,
        research: Arc<ResearchService>,
        macro_reader: &MacroContextReadCapability,
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        market_reader: &MarketInvestmentReadCapability,
        market: &MarketInvestmentReadReceipt,
        source_factory: &ForecastValuationSourceFactory,
        prepared_share_sources: Option<&str>,
        source_actions: &crate::application::SourceAppliedCorporateActionReadCapability,
        sources: AutomaticInvestmentValuationSources<'_>,
        request: AutomaticInvestmentValuationRequest,
        context: &RequestContext,
    ) -> Result<AutomaticInvestmentValuationEvaluation, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        if sources.financial_forecasts.len() > 16
            || request.profile_identity.algorithm() != DigestAlgorithm::Sha256
            || request.profile_identity.bytes() == [0; 32]
            || market.reference().instrument_id() != request.instrument_id
            || request.knowledge_at > market.reference().source_cutoff()?
            || request.expires_at <= calculation_clock()?
            || request.horizon_at <= request.expires_at
        {
            return Err(ServiceError::InvalidRequest);
        }
        let artifact_context =
            ArtifactReadContext::new(context.cancellation().clone(), context.deadline());
        // Calendar date is only a UTC query bound. Native fiscal periods remain source-owned.
        let query_date = request
            .knowledge_at
            .utc_calendar_date()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let mut fiscal = Vec::<Arc<ForecastValuationSource>>::new();
        let mut forecast_reads = Vec::with_capacity(sources.financial_forecasts.len() + 1);
        for selected in sources.financial_forecasts {
            let started_at = calculation_clock()?;
            let result = source_factory
                .source_for_selected_forecast(selected, &artifact_context)
                .await;
            ensure_request_live(context, &self.lifecycle)?;
            forecast_reads.push(forecast_read_audit(
                selected,
                AutomaticValuationForecastPurpose::NativeFinancial,
                started_at,
                calculation_clock()?,
                &result,
            )?);
            match result {
                Ok(source) => fiscal.push(Arc::new(source)),
                Err(error) => {
                    // Only an actual method/source rejection may leave this source absent.
                    // Capacity and integrity failures abort the enclosing evaluation.
                    audit_service_failure(map_fair_value_error(error))?;
                }
            }
        }
        let macro_context = macro_reader
            .read_investment_context(
                request.knowledge_at,
                query_date,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await;
        let mut attempts = Vec::with_capacity(4);
        for method in [
            AutomaticValuationMethod::DiscountedCashFlow,
            AutomaticValuationMethod::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome,
            AutomaticValuationMethod::ForecastDistribution,
        ] {
            ensure_request_live(context, &self.lifecycle)?;
            let started_at = calculation_clock()?;
            let mut stage = AutomaticValuationStage::SourceSelection;
            let mut source_knowledge_at = request.knowledge_at;
            let result = match method {
                AutomaticValuationMethod::ComparableCompanies => {
                    self.calculate_observed_comparables(
                        Arc::clone(&research),
                        market_reader,
                        market,
                        ObservedComparableValuationRequest {
                            account_id: request.account_id,
                            subject: request.instrument_id,
                            peers: Vec::new(),
                            knowledge_at: request.knowledge_at,
                            effective_date: query_date,
                            expires_at: request.expires_at,
                            calculated_by: request.calculated_by.clone(),
                        },
                        context,
                    )
                    .await
                }
                AutomaticValuationMethod::ForecastDistribution => {
                    match sources.price_forecast {
                        Some(selected) => {
                            let read_started_at = calculation_clock()?;
                            let result = source_factory
                                .source_for_selected_forecast(selected, &artifact_context)
                                .await;
                            ensure_request_live(context, &self.lifecycle)?;
                            forecast_reads.push(forecast_read_audit(
                                selected,
                                AutomaticValuationForecastPurpose::PriceDistribution,
                                read_started_at,
                                calculation_clock()?,
                                &result,
                            )?);
                            match result {
                                Ok(source) => {
                                    // Find retains its original forecast cutoff even when the
                                    // enclosing research and market reads happened later.
                                    source_knowledge_at = source.reference().knowledge_at();
                                    if source_knowledge_at > request.knowledge_at {
                                        return Err(ServiceError::InvalidRequest);
                                    }
                                    stage = AutomaticValuationStage::CalculationPublication;
                                    self.calculate_forecast_valuation(
                                        Arc::clone(&research),
                                        market_reader,
                                        market,
                                        source,
                                        AutomaticForecastValuationRequest {
                                            account_id: request.account_id,
                                            expires_at: request.expires_at,
                                            calculated_by: request.calculated_by.clone(),
                                        },
                                        context,
                                    )
                                    .await
                                }
                                Err(error) => Err(map_fair_value_error(error)),
                            }
                        }
                        // The completed-job owner found no authenticated price forecast.
                        // Preserve absence at source selection without inventing a read identity.
                        None => Err(ServiceError::Unavailable),
                    }
                }
                AutomaticValuationMethod::DiscountedCashFlow
                | AutomaticValuationMethod::ResidualIncome => {
                    let rate_kind = if method == AutomaticValuationMethod::DiscountedCashFlow {
                        AutomaticValuationAssumptionKind::DiscountRate
                    } else {
                        AutomaticValuationAssumptionKind::CostOfEquity
                    };
                    stage = AutomaticValuationStage::AnnualDiscountSources;
                    let binding = match (macro_context.as_ref(), sources.equity_premium.as_ref()) {
                        (Ok(macro_context), Ok(premium)) => {
                            derive_default_financial_model_macro_assumptions(
                                macro_context,
                                premium,
                                rate_kind,
                                "source_grounded_annual_market_equity_rate",
                                NonZeroU64::new(30 * 86_400 * 1_000_000_000)
                                    .ok_or(ServiceError::Internal)?,
                            )
                            .map_err(|_| ServiceError::Unavailable)
                        }
                        (Err(error), _) | (_, Err(error)) => Err(*error),
                    };
                    match binding {
                        Ok(binding) => {
                            stage = AutomaticValuationStage::NativeCalculationPublication;
                            self.calculate_native_financial_valuation(
                                &research,
                                market_reader,
                                market,
                                &fiscal,
                                method,
                                binding,
                                &request,
                                context,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            ensure_request_live(context, &self.lifecycle)?;
            if let Err(error) = result {
                audit_service_failure(error)?;
            }
            attempts.push(AutomaticInvestmentValuationAttempt {
                method,
                source_knowledge_at,
                started_at,
                completed_at: calculation_clock()?,
                stage,
                result,
            });
        }
        let mut selected = None;
        let mut selected_id = None;
        let mut selected_receipt = None;
        let mut selected_share_sources = None;
        let mut recommendation = [None; 4];
        // Every successful calculation gets a separate truthful recommendation-admission outcome.
        for method in [
            AutomaticValuationMethod::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome,
            AutomaticValuationMethod::DiscountedCashFlow,
            AutomaticValuationMethod::ForecastDistribution,
        ] {
            let Some((index, publication)) = attempts
                .iter()
                .enumerate()
                .find(|(_, attempt)| attempt.method == method)
                .and_then(|(index, attempt)| {
                    attempt
                        .result
                        .as_ref()
                        .ok()
                        .map(|publication| (index, publication))
                })
            else {
                continue;
            };
            let assessed_at;
            let outcome = if selected_id.is_some() {
                assessed_at = calculation_clock()?;
                AutomaticValuationRecommendationOutcome::NotCheckedAfterSelection
            } else {
                let share_sources = if method == AutomaticValuationMethod::ForecastDistribution {
                    Ok(None)
                } else {
                    super::financial::select_prepared_fundamental_share_sources(
                        &publication.receipt,
                        prepared_share_sources,
                        &research,
                        source_actions,
                        context,
                    )
                    .await
                    .map(Some)
                };
                let result = self
                    .read_automatic_investment_evidence(
                        &research,
                        macro_reader,
                        calendars,
                        publication.measurement_id,
                        request.account_id,
                        request.instrument_id,
                        publication.receipt.range().central().money().currency(),
                        request.horizon_at,
                        context,
                    )
                    .await;
                ensure_request_live(context, &self.lifecycle)?;
                assessed_at = calculation_clock()?;
                let result = result.and_then(|evidence| {
                    share_sources.as_ref().map_err(|error| *error)?;
                    if assessed_at >= evidence.0.window().expires_at()
                        || assessed_at >= evidence.1.window().expires_at()
                    {
                        Err(ServiceError::Unavailable)
                    } else {
                        Ok(evidence)
                    }
                });
                match result {
                    Ok(evidence) => {
                        selected_share_sources = share_sources?;
                        selected_receipt = Some(publication.receipt.clone());
                        selected = Some(evidence);
                        selected_id = Some(publication.measurement_id);
                        AutomaticValuationRecommendationOutcome::Selected
                    }
                    Err(error) => AutomaticValuationRecommendationOutcome::AdmissionFailed(
                        audit_service_failure(error)?,
                    ),
                }
            };
            recommendation[index] = Some(AutomaticValuationRecommendationAudit::new(
                assessed_at,
                outcome,
            ));
        }
        ensure_request_live(context, &self.lifecycle)?;
        let completed_at = calculation_clock()?;
        let audit = attempts
            .into_iter()
            .enumerate()
            .map(|(index, attempt)| {
                let outcome = match attempt.result {
                    Ok(publication) => {
                        if publication.receipt.account_id() != request.account_id
                            || publication.receipt.instrument_id() != request.instrument_id
                            || publication.receipt.measurement_at() != attempt.source_knowledge_at
                            || publication.receipt.method() != attempt.method
                        {
                            return Err(ServiceError::InvalidResult);
                        }
                        Ok(AutomaticValuationResultAudit::from_receipt(
                            publication.measurement_id,
                            &publication.receipt,
                            recommendation[index].ok_or(ServiceError::InvalidResult)?,
                        )
                        .map_err(|_| ServiceError::InvalidResult)?)
                    }
                    Err(error) => Err(audit_service_failure(error)?),
                };
                AutomaticValuationAttemptAudit::try_new(
                    attempt.method,
                    attempt.started_at,
                    attempt.completed_at,
                    attempt.stage,
                    outcome,
                )
                .map_err(|_| ServiceError::InvalidResult)
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        Ok(AutomaticInvestmentValuationEvaluation {
            completion: AutomaticValuationMethodSetAudit::try_new(
                request.account_id,
                request.instrument_id,
                request.knowledge_at,
                market.reference().source_cutoff()?,
                request.profile_identity,
                audit.try_into().map_err(|_| ServiceError::Internal)?,
                forecast_reads.into_boxed_slice(),
                selected_id,
                completed_at,
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            selected,
            selected_receipt,
            selected_share_sources,
        })
    }
}

fn forecast_read_audit(
    selected: &LatestValidForecast,
    purpose: AutomaticValuationForecastPurpose,
    started_at: Timestamp,
    completed_at: Timestamp,
    result: &Result<ForecastValuationSource, market_squawk_valuation::FairValueError>,
) -> Result<AutomaticValuationForecastReadAudit, ServiceError> {
    let outcome = match result {
        Ok(source) => Ok(source.reference().identity()),
        Err(error) => {
            // Operational failures cannot enter the closed persisted source-failure set.
            audit_service_failure(map_fair_value_error(error.clone()))?;
            Err(error.clone())
        }
    };
    AutomaticValuationForecastReadAudit::try_from_reference(
        purpose,
        selected.selection_receipt().selected_vintage_id(),
        selected.forecast_artifact().sha256(),
        selected.selection_receipt().receipt_digest(),
        started_at,
        completed_at,
        outcome,
    )
    .map_err(|_| ServiceError::InvalidResult)
}
fn audit_service_failure(error: ServiceError) -> Result<AutomaticValuationFailure, ServiceError> {
    Ok(match error {
        ServiceError::InvalidRequest => AutomaticValuationFailure::InvalidRequest,
        ServiceError::NotFound => AutomaticValuationFailure::NotFound,
        ServiceError::Unauthorized => AutomaticValuationFailure::Unauthorized,
        ServiceError::Unavailable => AutomaticValuationFailure::Unavailable,
        ServiceError::ResourceExhausted
        | ServiceError::InvalidResult
        | ServiceError::Internal
        | ServiceError::Cancelled
        | ServiceError::DeadlineExceeded => return Err(error),
    })
}
