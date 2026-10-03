//! Provider-neutral financial portfolio prerequisites over the existing independent authorities.

use std::sync::Arc;

use market_squawk_data::AnalyticalReadCapability;
use market_squawk_domain::{AccountId, InstrumentId, Money, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};

use crate::{
    application::{
        PortfolioCandidateResolutionFactory,
        analytical_profile::{AnalyticalProfileResolution, revalidate},
        market_calendar::{
            CompletedMarketSessionError, CompletedMarketSessionReadCapability,
            CompletedMarketSessionReference,
        },
        model::forecast_preparation::ForecastPreparationCatalog,
        recommendation::{RecommendationSetupAuthority, SetupRequiredKind},
    },
    portfolio_application::{
        PortfolioAccountCatalogReadCapability, PortfolioAnalysisCurrentPosition,
        PortfolioAnalysisMarkedPortfolioEvidence, PortfolioAnalysisPrerequisitePolicy,
        PortfolioAnalysisPrerequisiteReadCapability, PortfolioAnalysisPrerequisiteResolution,
        PortfolioAnalysisPrerequisiteUnavailableReason, PortfolioAnalysisReadReference,
        PortfolioAnalysisRiskEvidence, PortfolioAnalysisRiskUnavailableReason,
        PortfolioApplicationServiceError, PortfolioCandidateImpactReadCapability,
    },
};

pub(super) const SELECT: &str = "Portfolio.SelectAnalysisPrerequisites";
pub(super) const READ: &str = "Portfolio.ReadAnalysisPrerequisites";

/// This adapter owns no account choice, paper runtime, receipt registry, or financial defaults.
pub(super) struct InstalledPortfolioAnalysis {
    factory: PortfolioCandidateResolutionFactory,
    setup: Arc<RecommendationSetupAuthority>,
    catalog: PortfolioAccountCatalogReadCapability,
    portfolios: PortfolioCandidateImpactReadCapability,
    history: AnalyticalReadCapability,
    research: Arc<crate::ResearchService>,
    completed_sessions: CompletedMarketSessionReadCapability,
}

impl InstalledPortfolioAnalysis {
    pub(super) const fn new(
        factory: PortfolioCandidateResolutionFactory,
        setup: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
        portfolios: PortfolioCandidateImpactReadCapability,
        history: AnalyticalReadCapability,
        research: Arc<crate::ResearchService>,
        completed_sessions: CompletedMarketSessionReadCapability,
    ) -> Self {
        Self {
            factory,
            setup,
            catalog,
            portfolios,
            history,
            research,
            completed_sessions,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, SELECT | READ)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        models: Option<&ForecastPreparationCatalog>,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = serde_json::Value::Object(super::business_arguments(request.arguments()));
        let (profile, selection) = match request.name() {
            SELECT => {
                let input: SelectRequest =
                    serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
                (
                    input.financial_profile,
                    Selection::Current(
                        input.instrument_id,
                        parse_cutoff(&input.source_cutoff_unix_nanos)?,
                    ),
                )
            }
            READ => {
                let input: ReadRequest =
                    serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
                (
                    input.financial_profile,
                    Selection::Reference(input.reference),
                )
            }
            _ => return Err(ServiceError::NotFound),
        };
        let resolved = match self.resolve(&profile, selection, models, context).await {
            Ok(resolved) => resolved,
            Err(ResolutionReadError::Changed {
                instrument_id,
                source_cutoff,
                summary,
            }) => {
                return changed_result(
                    instrument_id,
                    source_cutoff,
                    &profile.configuration_digest,
                    summary,
                    context,
                );
            }
            Err(ResolutionReadError::Service(error)) => return Err(error),
        };
        let response = project(
            resolved.instrument_id,
            resolved.source_cutoff,
            &profile.configuration_digest,
            &resolved.evidence,
            resolved.calendar,
        )?;
        result(response, context)
    }

    /// Reopens typed prerequisites for final investment composition through the same exact
    /// calendar, profile, portfolio and source path used by the transport operation. Changed
    /// evidence remains unavailable; a new calculation clock never authorizes replacement inputs.
    pub(super) async fn read_reference(
        &self,
        reference: &PortfolioAnalysisReadReference,
        profile: &AnalyticalProfileResolution,
        models: Option<&ForecastPreparationCatalog>,
        context: &RequestContext,
    ) -> Result<PortfolioAnalysisPrerequisiteResolution, ServiceError> {
        let resolved = self
            .resolve(
                profile,
                Selection::Reference(reference.clone()),
                models,
                context,
            )
            .await;
        ensure_live(context)?;
        resolved
            .map(|resolved| resolved.evidence)
            .map_err(ResolutionReadError::into_service_error)
    }

    async fn resolve(
        &self,
        profile: &AnalyticalProfileResolution,
        selection: Selection,
        models: Option<&ForecastPreparationCatalog>,
        context: &RequestContext,
    ) -> Result<ResolvedPrerequisites, ResolutionReadError> {
        ensure_live(context)?;
        let validated = revalidate(profile, models).map_err(ServiceError::from)?;
        let policy = PortfolioAnalysisPrerequisitePolicy::try_new(
            validated.portfolio_risk_minimum_daily_returns(),
            validated.portfolio_risk_maximum_daily_returns(),
        )
        .map_err(|error| error.as_service_error())?
        .bind_financial_configuration(&profile.configuration_digest)
        .map_err(|error| error.as_service_error())?;
        let maximum_age = u64::try_from(
            validated
                .recommendation_policy()
                .parameters()
                .market_max_age_nanos,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let authority = self
            .factory
            .with_maximum_mark_age_nanos(maximum_age)?
            .bind(Arc::clone(&self.setup), self.catalog.clone());
        // Resolve the actual retained calendar once per analysis. Exact reopening never
        // substitutes a newly selected calendar for the original source publication.
        let selected_sessions = match &selection {
            Selection::Current(_, cutoff) => self
                .completed_sessions
                .select(*cutoff, context.deadline(), context.cancellation().clone())
                .await
                .map_err(calendar_error)?,
            Selection::Reference(reference) => match reference.calendar() {
                Some(calendar) => {
                    let cutoff = reference
                        .prerequisites()
                        .source_cutoff()
                        .map_err(|error| error.as_service_error())?;
                    let selected = self
                        .completed_sessions
                        .read_reference(
                            calendar,
                            cutoff,
                            context.deadline(),
                            context.cancellation().clone(),
                        )
                        .await
                        .map_err(calendar_error)?;
                    if selected
                        .as_ref()
                        .is_none_or(|selected| selected.reference() != calendar)
                    {
                        return Err(ResolutionReadError::Changed {
                            instrument_id: reference.prerequisites().candidate_instrument_id(),
                            source_cutoff: cutoff,
                            summary: "The saved trading-calendar evidence is no longer current. Start a fresh investment analysis.",
                        });
                    }
                    selected
                }
                // Retain the original absence instead of upgrading a saved unavailable result
                // with calendar evidence that arrived after its frozen source cutoff.
                None => None,
            },
        };
        let reader = PortfolioAnalysisPrerequisiteReadCapability::new(
            authority,
            self.portfolios.clone(),
            self.history.clone(),
            Arc::clone(&self.research),
            self.completed_sessions.clone(),
            selected_sessions
                .as_ref()
                .map(|selected| Arc::clone(selected.authority())),
        );
        let (instrument_id, source_cutoff, evidence) = match selection {
            Selection::Current(instrument_id, cutoff) => {
                let evidence = match reader
                    .read(
                        instrument_id,
                        policy,
                        cutoff,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                {
                    Ok(evidence) => evidence,
                    Err(PortfolioApplicationServiceError::StateChanged) => {
                        return Err(ResolutionReadError::Changed {
                            instrument_id,
                            source_cutoff: cutoff,
                            summary: "Your portfolio or current market and trading-calendar evidence changed during calculation. Start a fresh investment analysis.",
                        });
                    }
                    Err(error) => return Err(error.as_service_error().into()),
                };
                (instrument_id, cutoff, evidence)
            }
            Selection::Reference(reference) => {
                let reference = reference.prerequisites();
                let cutoff = reference
                    .source_cutoff()
                    .map_err(|error| error.as_service_error())?;
                let evidence = match reader
                    .read_reference(
                        reference,
                        policy,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                {
                    Ok(evidence) => evidence,
                    Err(PortfolioApplicationServiceError::StateChanged) => {
                        ensure_live(context)?;
                        return Err(ResolutionReadError::Changed {
                            instrument_id: reference.candidate_instrument_id(),
                            source_cutoff: cutoff,
                            summary: "Your portfolio or selected analysis evidence has changed. Start a fresh investment analysis.",
                        });
                    }
                    Err(error) => return Err(error.as_service_error().into()),
                };
                (reference.candidate_instrument_id(), cutoff, evidence)
            }
        };
        ensure_live(context)?;
        Ok(ResolvedPrerequisites {
            instrument_id,
            source_cutoff,
            evidence,
            calendar: selected_sessions
                .as_ref()
                .map(|selected| selected.reference().clone()),
        })
    }
}

struct ResolvedPrerequisites {
    instrument_id: InstrumentId,
    source_cutoff: Timestamp,
    evidence: PortfolioAnalysisPrerequisiteResolution,
    calendar: Option<CompletedMarketSessionReference>,
}

enum ResolutionReadError {
    Service(ServiceError),
    Changed {
        instrument_id: InstrumentId,
        source_cutoff: Timestamp,
        summary: &'static str,
    },
}

impl From<ServiceError> for ResolutionReadError {
    fn from(error: ServiceError) -> Self {
        Self::Service(error)
    }
}

impl ResolutionReadError {
    fn into_service_error(self) -> ServiceError {
        match self {
            Self::Service(error) => error,
            Self::Changed { .. } => ServiceError::Unavailable,
        }
    }
}

impl std::fmt::Debug for InstalledPortfolioAnalysis {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledPortfolioAnalysis")
            .field(
                "authorities",
                &"[EXPLICIT PORTFOLIO, CURRENT MARKET AND IMMUTABLE HISTORY READS]",
            )
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SelectRequest {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadRequest {
    reference: PortfolioAnalysisReadReference,
    financial_profile: AnalyticalProfileResolution,
}

enum Selection {
    Current(InstrumentId, Timestamp),
    Reference(PortfolioAnalysisReadReference),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PortfolioAnalysisResponse {
    status: &'static str,
    summary: &'static str,
    instrument_id: InstrumentId,
    account_id: Option<AccountId>,
    portfolio_as_of_unix_nanos: Option<String>,
    source_cutoff_unix_nanos: String,
    calculated_at_unix_nanos: Option<String>,
    financial_configuration_digest: String,
    reference: Option<PortfolioAnalysisReadReference>,
    marked_portfolio: Option<MarkedPortfolio>,
    historical_risk: Option<HistoricalRisk>,
    analysis_only: bool,
    execution_authority: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Amount {
    amount: String,
    currency: String,
}
impl From<Money> for Amount {
    fn from(value: Money) -> Self {
        Self {
            amount: value.amount().normalize().to_string(),
            currency: value.currency().as_str().to_owned(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MarkedPortfolio {
    equity: Amount,
    cash: Amount,
    receivables: Amount,
    holding_count: usize,
    candidate_quantity: Option<String>,
    candidate_value: Option<Amount>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HistoricalRisk {
    basis: &'static str,
    interpretation: &'static str,
    confidence_basis_points: u16,
    value_at_risk_return: String,
    expected_shortfall_return: String,
    remaining_downside_budget_ppm: u32,
    observations: usize,
    source_observations: usize,
    holdings: usize,
    sample_start_unix_nanos: Option<String>,
    sample_end_unix_nanos: Option<String>,
    current_session_coverage: &'static str,
    cash_assumption: &'static str,
}

fn project(
    instrument_id: InstrumentId,
    source_cutoff: Timestamp,
    configuration_digest: &str,
    evidence: &PortfolioAnalysisPrerequisiteResolution,
    calendar: Option<CompletedMarketSessionReference>,
) -> Result<PortfolioAnalysisResponse, ServiceError> {
    let reference = evidence
        .reference(instrument_id)
        .map_err(|error| error.as_service_error())?
        .map(|prerequisites| PortfolioAnalysisReadReference::new(prerequisites, calendar));
    let mut response = PortfolioAnalysisResponse {
        status: "setup_required",
        summary: "Choose a portfolio and confirm your investment preferences.",
        instrument_id,
        account_id: None,
        portfolio_as_of_unix_nanos: None,
        source_cutoff_unix_nanos: source_cutoff.unix_nanos().to_string(),
        calculated_at_unix_nanos: None,
        financial_configuration_digest: configuration_digest.to_owned(),
        reference,
        marked_portfolio: None,
        historical_risk: None,
        analysis_only: true,
        execution_authority: "none",
    };
    match evidence {
        PortfolioAnalysisPrerequisiteResolution::SetupRequired { requirement, .. } => {
            response.summary = match requirement.kind() {
                SetupRequiredKind::NoDefaultAccount | SetupRequiredKind::AmbiguousAccounts => {
                    "Choose the portfolio to use for this investment analysis."
                }
                SetupRequiredKind::PortfolioEvidenceUnavailable => {
                    "Add current holdings and cash before calculating your portfolio impact."
                }
                SetupRequiredKind::ProfileReviewRequired => {
                    "Review your investment preferences before calculating personalized risk."
                }
            };
        }
        PortfolioAnalysisPrerequisiteResolution::Unavailable(evidence) => {
            response.status = "unavailable";
            response.summary = unavailable_summary(evidence.reason());
            response.account_id = Some(evidence.portfolio().account_id());
            response.portfolio_as_of_unix_nanos =
                Some(evidence.portfolio().effective_at().unix_nanos().to_string());
            response.calculated_at_unix_nanos =
                Some(evidence.calculated_at().unix_nanos().to_string());
            response.marked_portfolio = evidence.marked_portfolio().map(marked_portfolio);
            response.historical_risk = evidence.historical_scenario().map(historical_risk);
        }
        PortfolioAnalysisPrerequisiteResolution::Evaluated(evidence) => {
            response.status = "evaluated";
            response.summary =
                "Your portfolio value and analytical risk context have been calculated.";
            response.account_id = Some(evidence.portfolio().account_id());
            response.portfolio_as_of_unix_nanos =
                Some(evidence.portfolio().effective_at().unix_nanos().to_string());
            response.calculated_at_unix_nanos =
                Some(evidence.calculated_at().unix_nanos().to_string());
            response.marked_portfolio = Some(marked_portfolio(evidence.marked_portfolio()));
            response.historical_risk = Some(historical_risk(evidence.risk()));
        }
    }
    Ok(response)
}

fn marked_portfolio(evidence: &PortfolioAnalysisMarkedPortfolioEvidence) -> MarkedPortfolio {
    let (candidate_quantity, candidate_value) = match evidence.current_position() {
        PortfolioAnalysisCurrentPosition::NoPosition => (None, None),
        PortfolioAnalysisCurrentPosition::Position {
            quantity,
            marked_value,
        } => (
            Some(quantity.normalize().to_string()),
            Some(marked_value.into()),
        ),
    };
    MarkedPortfolio {
        equity: evidence.marked_equity().into(),
        cash: evidence.source_cash_balance().into(),
        receivables: evidence.source_receivable_value().into(),
        holding_count: evidence.holdings().len(),
        candidate_quantity,
        candidate_value,
    }
}

fn historical_risk(evidence: &PortfolioAnalysisRiskEvidence) -> HistoricalRisk {
    let scenario = evidence.historical_scenario();
    let cash_only = scenario.is_some_and(|scenario| scenario.is_cash_only());
    HistoricalRisk {
        basis: evidence.basis_name(),
        interpretation: if cash_only && scenario.is_some_and(|value| !value.receivables().amount().is_zero()) {
            "Cash and amounts due are held constant in these market-price scenarios. Amounts due are not spendable cash; payment delays and defaults are not estimated by this scenario."
        } else if cash_only {
            "Reporting-currency cash has no exposure to the modeled market-price shocks."
        } else {
            "Historical one-session adjusted price changes applied to your current holdings at current prices. These are scenarios, not your experienced returns or a forecast."
        },
        confidence_basis_points: evidence.confidence_basis_points(),
        value_at_risk_return: evidence.value_at_risk().normalize().to_string(),
        expected_shortfall_return: evidence.expected_shortfall().normalize().to_string(),
        remaining_downside_budget_ppm: evidence.risk_capacity_ppm(),
        observations: scenario.map_or_else(
            || evidence.returns().len(),
            |scenario| scenario.observations(),
        ),
        source_observations: scenario.map_or(0, |scenario| scenario.source_observations()),
        holdings: scenario.map_or(0, |scenario| scenario.holdings()),
        sample_start_unix_nanos: scenario
            .and_then(|scenario| scenario.sample_start())
            .map(|value| value.unix_nanos().to_string()),
        sample_end_unix_nanos: scenario
            .and_then(|scenario| scenario.sample_end())
            .map(|value| value.unix_nanos().to_string()),
        current_session_coverage: if cash_only {
            "not_applicable"
        } else if scenario.is_some_and(|scenario| scenario.current_session_covered()) {
            "complete"
        } else {
            "unavailable"
        },
        cash_assumption: "Cash and receivables stay constant in the reporting currency. Receivables remain separate from spendable cash; interest, inflation, foreign-exchange changes and collection risk are excluded.",
    }
}

fn unavailable_summary(reason: &PortfolioAnalysisPrerequisiteUnavailableReason) -> &'static str {
    match reason {
        PortfolioAnalysisPrerequisiteUnavailableReason::CurrentMarket { .. } => {
            "Current prices are missing for this investment or one of your holdings."
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::HistoricalRisk(reason) => match reason {
            PortfolioAnalysisRiskUnavailableReason::CurrentSessionCoverageUnavailable => {
                "Historical risk scenarios are calculated, but coverage through the latest completed trading session is not confirmed."
            }
            PortfolioAnalysisRiskUnavailableReason::InsufficientHistory { .. } => {
                "There are not enough matching daily observations across your holdings to estimate historical risk."
            }
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryUnavailable { .. } => {
                "Complete daily history is missing for one of your holdings."
            }
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryCurrencyMismatch { .. }
            | PortfolioAnalysisRiskUnavailableReason::ReportingCurrencyMismatch { .. } => {
                "Historical risk needs prices in your portfolio's reporting currency."
            }
            PortfolioAnalysisRiskUnavailableReason::MarketHistoryAdjustmentUnsupported {
                ..
            } => "Historical risk needs prices with supported corporate-action adjustments.",
            _ => "Portfolio history is incomplete for this risk calculation.",
        },
        PortfolioAnalysisPrerequisiteUnavailableReason::ReportingCurrencyMismatch { .. } => {
            "Portfolio valuation needs prices in the reporting currency."
        }
        PortfolioAnalysisPrerequisiteUnavailableReason::HoldingExecutionTermsMismatch {
            ..
        } => "A holding's units cannot be reconciled with its current investment terms.",
        PortfolioAnalysisPrerequisiteUnavailableReason::NonPositiveMarkedEquity => {
            "Positive net portfolio value is required for percentage-based risk and allocation analysis."
        }
    }
}

fn changed_result(
    instrument_id: InstrumentId,
    source_cutoff: Timestamp,
    configuration_digest: &str,
    summary: &'static str,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    ensure_live(context)?;
    result(
        PortfolioAnalysisResponse {
            status: "unavailable",
            summary,
            instrument_id,
            account_id: None,
            portfolio_as_of_unix_nanos: None,
            source_cutoff_unix_nanos: source_cutoff.unix_nanos().to_string(),
            calculated_at_unix_nanos: None,
            financial_configuration_digest: configuration_digest.to_owned(),
            reference: None,
            marked_portfolio: None,
            historical_risk: None,
            analysis_only: true,
            execution_authority: "none",
        },
        context,
    )
}

fn calendar_error(error: CompletedMarketSessionError) -> ServiceError {
    match error {
        CompletedMarketSessionError::InvalidRequest => ServiceError::InvalidRequest,
        CompletedMarketSessionError::InvalidEvidence => ServiceError::InvalidResult,
        CompletedMarketSessionError::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        CompletedMarketSessionError::Unavailable => ServiceError::Unavailable,
        CompletedMarketSessionError::Cancelled => ServiceError::Cancelled,
        CompletedMarketSessionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
    }
}

fn result(
    response: PortfolioAnalysisResponse,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        serde_json::to_value(response).map_err(|_| ServiceError::InvalidResult)?,
        1,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(Into::into)
}

fn parse_cutoff(value: &str) -> Result<Timestamp, ServiceError> {
    let nanos = value
        .parse::<i64>()
        .map_err(|_| ServiceError::InvalidRequest)?;
    let now = super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
    if nanos <= 0 || nanos.to_string() != value || nanos > now.unix_nanos() {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
