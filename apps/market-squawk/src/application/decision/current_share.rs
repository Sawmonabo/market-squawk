//! Physical replay of the original monetary projection; saved coordinates grant no source authority.

use std::{num::NonZeroUsize, sync::Arc};

use market_squawk_data::Sha256Digest;
use market_squawk_decisions::{CurrentShareDecisionProjection, CurrentShareMarketAdmission, InvestmentAnalysisEvidence, MarketReferenceEvidence, MarketReferenceAdjustmentBasis, MarketReferencePriceKind, ProposalEvidenceWindow};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Money, Timestamp};
use market_squawk_services::{ArtifactReadContext, RequestContext, ServiceError};
use serde::{Deserialize, Serialize};

use crate::{ResearchService, application::{
    SourceAppliedCorporateActionReadCapability,
    fair_value::{FairValueAutomaticReadCapability, ForecastValuationSourceFactory},
    market_calendar::ForecastSessionReadCapability,
    market_selection::{MarketInvestmentReadCapability, MarketInvestmentReadReceipt, MarketInvestmentMarkBasis},
    model::forecast::{ForecastEvidenceReadContext, ForecastEvidenceReader, ForecastPriceEvidence, replay_price_history_inputs},
    research::corporate_actions::ApplicableActionPlanError,
}};
use super::investment_request::{GenerateRequest, digest, validate_canonical_request};

/// Strict inert coordinates, independently compared against all three reconstructed identities.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CurrentShareReplayRecipe {
    projection_identity: [u8; 32],
    original_monetary_identity: [u8; 32],
    conversion_identity: [u8; 32],
    valuation_projection_identity: [u8; 32],
    authorized_at: Timestamp,
    authorization_expires_at: Timestamp,
    authorization_decision_digest: EvidenceDigest,
    output_scale: u32,
}
impl From<&CurrentShareDecisionProjection> for CurrentShareReplayRecipe {
    fn from(value: &CurrentShareDecisionProjection) -> Self {
        let admission = value.market_admission();
        Self {
            projection_identity: value.identity().evidence_digest().bytes(),
            original_monetary_identity: value.original_monetary_identity().evidence_digest().bytes(),
            conversion_identity: value.conversion().identity().bytes(),
            valuation_projection_identity: value.valuation_projection().identity().bytes(),
            authorized_at: admission.authorized_at,
            authorization_expires_at: admission.authorization_expires_at,
            authorization_decision_digest: admission.authorization_decision_digest.evidence_digest(),
            output_scale: value.conversion().output_scale(),
        }
    }
}

/// Existing bounded read owners only; no acquisition, latest selector, or valuation publisher.
#[derive(Clone)]
pub(crate) struct CurrentShareReplayCapability {
    pub(crate) research: Arc<ResearchService>,
    pub(crate) calendars: ForecastSessionReadCapability,
    pub(crate) market: MarketInvestmentReadCapability,
    pub(crate) forecasts: Arc<dyn ForecastEvidenceReader>,
    pub(crate) valuations: FairValueAutomaticReadCapability,
    pub(crate) valuation_sources: ForecastValuationSourceFactory,
    pub(crate) source_actions: SourceAppliedCorporateActionReadCapability,
    pub(crate) maximum_forecast_artifact_bytes: NonZeroUsize,
}
impl CurrentShareReplayCapability {
    /// Reopen exact retained source bytes under current read rights, preserving financial clocks.
    pub(crate) async fn replay(
        &self,
        original: InvestmentAnalysisEvidence,
        canonical_request: &[u8],
        policy: &market_squawk_decisions::RecommendationPolicy,
        recipe: &CurrentShareReplayRecipe,
        context: &RequestContext,
    ) -> Result<InvestmentAnalysisEvidence, ServiceError> {
        validate_canonical_request(canonical_request)?;
        let request: GenerateRequest = serde_json::from_slice(canonical_request)
            .map_err(|_| ServiceError::InvalidResult)?;
        let invalid = ServiceError::InvalidResult;
        if original.current_share_projection().is_some()
            || recipe.output_scale > 28
            || recipe.authorized_at > original.admitted_at()
            || original.admitted_at() >= recipe.authorization_expires_at
            || recipe.authorization_decision_digest.algorithm() != DigestAlgorithm::Sha256
            || [recipe.projection_identity, recipe.original_monetary_identity,
                recipe.conversion_identity, recipe.valuation_projection_identity,
                recipe.authorization_decision_digest.bytes()].contains(&[0; 32])
        { return Err(invalid); }
        let method = original.valuation_method_set().ok_or(invalid)?;
        let receipt = self.valuations.read_automatic_valuation(method.selected_measurement_id().ok_or(invalid)?, context).await?;
        let mut source_references = receipt.inputs().iter().filter_map(|input| match input.input().evidence().origin() {
            market_squawk_valuation::EvidenceOrigin::ForecastDistribution { evidence } => Some(evidence.source().reference()),
            _ => None,
        });
        let source_reference = source_references.next().ok_or(invalid)?;
        if source_references.any(|reference| reference != source_reference)
            || source_reference.selected_at() > original.admitted_at() { return Err(invalid); }
        let saved = original.forecast_chart().ok_or(invalid)?;
        let forecast = original.price_forecast().ok_or(invalid)?;
        let selected = self.forecasts.exact_distribution_for_identity(
            Sha256Digest::new(forecast.vintage_id().bytes()), original.instrument_id(), source_reference.selected_at(),
            ForecastEvidenceReadContext::new(ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
                self.maximum_forecast_artifact_bytes),
        ).await.map_err(crate::application::model::map_forecast_selection_error)?;
        let ForecastPriceEvidence::Available(price) = selected.price_evidence() else { return Err(ServiceError::Unavailable); };
        let horizon = std::num::NonZeroU64::new(u64::try_from(policy.horizon_nanos()).map_err(|_| invalid)?).ok_or(invalid)?;
        let crate::application::model::forecast::ExactHorizonPriceForecastEvidence::Available(projection) = selected
            .exact_horizon_price_projection(horizon).map_err(|_| invalid)? else { return Err(invalid); };
        let authenticated_forecast = super::recommendation::adapt_price_forecast_evidence(
            projection, policy, original.as_of(), original.admitted_at(),
        ).map_err(|_| invalid)?;
        if authenticated_forecast != *forecast { return Err(invalid); }

        let history = replay_price_history_inputs(price, &self.research, &self.calendars, &self.source_actions,
            request.source_action_reference.as_ref().ok_or(invalid)?, Some(saved), context)
            .await?.ok_or(ServiceError::Unavailable)?;
        let market = self.market.read_reference(request.market.as_ref().ok_or(invalid)?,
            context.deadline(), context.cancellation().clone()).await?;
        let original_market = *original.market().ok_or(invalid)?;
        if market_evidence(&market, recipe.authorization_expires_at)? != original_market { return Err(invalid); }
        let original_model = original.financial_model().ok_or(invalid)?;
        let cases = crate::application::fair_value::automatic_valuation_model_cases(&receipt)?;
        let authenticated_model = super::recommendation::adapt_financial_model_evidence(
            &receipt, None, receipt.macro_assumptions().cloned(), cases.scenarios(),
            cases.scenario_identity(), cases.sensitivity_range(), cases.sensitivity_identity(),
            original_model.horizon_at(), original_model.window(),
        ).map_err(|_| invalid)?;
        if authenticated_model != *original_model { return Err(invalid); }
        let conversion = self.source_actions.read_retained_forecast_share_conversion(
            request.current_share_action_reference.as_ref().ok_or(invalid)?, &history.epoch, &history.history,
            &history.original_plan, &market, original.admitted_at(), recipe.authorized_at,
            recipe.authorization_expires_at, Sha256Digest::new(recipe.conversion_identity), recipe.output_scale,
            context.deadline(), context.cancellation().clone(),
        ).await.map_err(source_error)?.ok_or(ServiceError::Unavailable)?;
        let valuation = receipt.project_current_share_units(&history.epoch, &conversion, original.admitted_at())
            .map_err(|_| invalid)?;
        let source = self.valuation_sources.source_for_selected_forecast(&selected,
            &ArtifactReadContext::new(context.cancellation().clone(), context.deadline())).await
            .map_err(|error| match error {
                market_squawk_valuation::FairValueError::Cancelled => ServiceError::Cancelled,
                market_squawk_valuation::FairValueError::DeadlineExceeded => ServiceError::DeadlineExceeded,
                market_squawk_valuation::FairValueError::ResourceExhausted => ServiceError::ResourceExhausted,
                _ => ServiceError::InvalidResult,
            })?;
        if source.reference() != source_reference || source.reference().identity() != valuation.source_identity() { return Err(invalid); }
        let admission = CurrentShareMarketAdmission {
            market: original_market, authorized_at: recipe.authorized_at,
            authorization_expires_at: recipe.authorization_expires_at,
            authorization_decision_digest: digest(recipe.authorization_decision_digest)?,
        };
        let projected = original.try_project_current_share_units(conversion, valuation, market.publication(), admission)
            .map_err(|_| invalid)?;
        if CurrentShareReplayRecipe::from(projected.current_share_projection().ok_or(invalid)?) != *recipe {
            return Err(invalid);
        }
        Ok(projected)
    }
}

pub(crate) fn source_error(error: ApplicableActionPlanError) -> ServiceError {
    match error {
        ApplicableActionPlanError::SourceRead(error) => error,
        ApplicableActionPlanError::IncompleteOrdinaryCoverage
        | ApplicableActionPlanError::UnresolvedApplicableActions => ServiceError::Unavailable,
        ApplicableActionPlanError::InvalidEvidence => ServiceError::InvalidResult,
        ApplicableActionPlanError::Interrupted => ServiceError::Internal,
    }
}

/// Retains the startup/restore operation's deadline and cancellation for existing read adapters.
pub(crate) fn recovery_request_context(
    context: &ArtifactReadContext,
) -> Result<RequestContext, ServiceError> {
    let structure = market_squawk_services::JsonStructureLimits::try_new(8, 4096, 128, 16)
        .map_err(|_| ServiceError::Internal)?;
    let limits = market_squawk_services::ServiceLimits::try_new(4096, 1, 4096, 1, structure)
        .map_err(|_| ServiceError::Internal)?;
    Ok(RequestContext::new(market_squawk_services::RequestId::Integer(1),
        context.cancellation().clone(), context.deadline(), limits))
}

/// Canonical market evidence; retained replay keeps the original admission expiry.
pub(crate) fn market_evidence(
    market: &MarketInvestmentReadReceipt,
    authorization_expires_at: Timestamp,
) -> Result<MarketReferenceEvidence, ServiceError> {
    let observation = market
        .observation()
        .map_err(|_| ServiceError::Unavailable)?;
    let mark = observation.mark();
    let source = market.reference().source_cutoff()?;
    let expires = mark
        .fresh_until()
        .ok_or(ServiceError::Unavailable)?
        .checked_add_nanos(1)
        .map_err(|_| ServiceError::InvalidResult)?
        .min(authorization_expires_at);
    let window = ProposalEvidenceWindow::try_from_derived(
        observation.timestamps().effective_at(),
        source,
        source,
        expires,
        digest(market.evidence_digest())?,
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    MarketReferenceEvidence::try_new(
        observation.instrument_id(),
        Money::new(mark.value(), mark.currency()),
        observation.quality(),
        match mark.basis() {
            MarketInvestmentMarkBasis::FreshLastTrade => MarketReferencePriceKind::LastTrade,
            MarketInvestmentMarkBasis::FreshBidAskMidpoint => {
                MarketReferencePriceKind::CheckedBidAskMidpoint
            }
        },
        MarketReferenceAdjustmentBasis::UnadjustedSpot,
        digest(observation.selection_digest())?,
        digest(mark.evidence_identity())?,
        window,
    )
    .map_err(|_| ServiceError::InvalidResult)
}
