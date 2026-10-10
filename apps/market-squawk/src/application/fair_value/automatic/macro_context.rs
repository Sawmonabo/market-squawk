//! Exact retained annual-rate context replay for research valuation consumption.

use super::*;
use crate::application::research::{MacroContextReadCapability, MacroInvestmentContext};
use market_squawk_valuation::{AutomaticValuationMethod, MacroRateMaturity};

pub(crate) struct ReopenedAutomaticMacroContext {
    context: MacroInvestmentContext,
    expires_at: Timestamp,
}
impl ReopenedAutomaticMacroContext {
    pub(crate) const fn context(&self) -> &MacroInvestmentContext {
        &self.context
    }
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

impl FairValueDomainService {
    /// Replays exact retained government and equity-premium sources before active research use.
    pub(crate) async fn read_automatic_macro_context(
        &self,
        measurement_id: MeasurementId,
        reader: &MacroContextReadCapability,
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        research: &ResearchService,
        context: &RequestContext,
    ) -> Result<Option<ReopenedAutomaticMacroContext>, ServiceError> {
        let receipt = self
            .read_automatic_valuation(measurement_id, context)
            .await?;
        let Some(binding) = receipt.macro_assumptions() else {
            return if matches!(
                receipt.method(),
                AutomaticValuationMethod::ComparableCompanies
                    | AutomaticValuationMethod::ForecastDistribution
            ) {
                Ok(None)
            } else {
                Err(ServiceError::InvalidResult)
            };
        };
        let reference = binding.reference();
        ensure_request_live(context, &self.lifecycle)?;
        if calculation_clock()? >= receipt.expires_at() {
            return Err(ServiceError::Unavailable);
        }
        let reopened = reader
            .read_investment_context(
                reference.knowledge_cutoff(),
                reference.effective_date_cutoff(),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
        let selected = match reference.maturity() {
            MacroRateMaturity::TenYear => reopened.valuation_rates().ten_year_reference(),
            MacroRateMaturity::ThirtyYear => reopened.valuation_rates().thirty_year_reference(),
        };
        let maximum_age =
            std::num::NonZeroU64::new(30 * 86_400 * 1_000_000_000).ok_or(ServiceError::Internal)?;
        if reopened.knowledge_cutoff() != reference.knowledge_cutoff()
            || reopened.effective_date_cutoff() != reference.effective_date_cutoff()
            || reopened.evidence_digest() != reference.context_identity()
            || selected.evidence_digest() != reference.evidence_identity()
            || selected.annual_yield_percent() != reference.annual_yield_percent()
            || selected.available_at() != reference.available_at()
            || reference.expires_at() > selected.expires_at(maximum_age)?
            || reopened.parent_manifests().is_empty()
            || reopened.parent_manifests().len() > 64
        {
            return Err(ServiceError::Unavailable);
        }
        let benchmarks =
            crate::application::research::RecommendationBenchmarkSelectionReadCapability::new(
                research.market_data_instruments(),
            );
        let premium_result = reader
            .read_equity_premium_reference_bytes(
                binding
                    .premium_source_reference()
                    .ok_or(ServiceError::Unavailable)?,
                research,
                calendars,
                &benchmarks,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await;
        ensure_request_live(context, &self.lifecycle)?;
        let premium = premium_result.map_err(|error| error.into_service_error())?;
        let recomputed = crate::application::decision::recommendation::derive_default_financial_model_macro_assumptions(
            &reopened,&premium,binding.assumption().kind(),binding.assumption().identifier(),maximum_age
        ).map_err(|_| ServiceError::Unavailable)?;
        if &recomputed != binding {
            return Err(ServiceError::Unavailable);
        }
        ensure_request_live(context, &self.lifecycle)?;
        let remaining = context
            .deadline()
            .checked_duration_since(std::time::Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let limits = ResearchUseLimits::try_new(
            64,
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
                    binding.premium_parent_manifests().to_vec(),
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
        let expires_at = receipt.expires_at().min(authorization.expires_at());
        ensure_request_live(context, &self.lifecycle)?;
        if calculation_clock()? >= expires_at {
            return Err(ServiceError::Unavailable);
        }
        let _consumed = authorization.into_permit();
        Ok(Some(ReopenedAutomaticMacroContext {
            context: reopened,
            expires_at,
        }))
    }
}
