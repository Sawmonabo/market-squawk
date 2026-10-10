//! Strict reference-only request provenance shared by publication and durable recovery.
use crate::{
    application::{
        analytical_profile::AnalyticalProfileResolution,
        market_selection::MarketInvestmentReadReference,
    },
    portfolio_application::PortfolioAnalysisReadReference,
};
use market_squawk_decisions::{
    AnalyticalProfileBindingReference, CandidateId, DecisionContentDigest,
    InvestmentAnalysisWorkflowReference, ScreenRunId,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use uuid::Uuid;
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GenerateRequest {
    pub(crate) source_cutoff_unix_nanos: String,
    pub(crate) financial_profile: AnalyticalProfileResolution,
    pub(crate) analytical_profile: ProfileBinding,
    pub(crate) workflow: WorkflowBinding,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) market: Option<MarketInvestmentReadReference>,
    pub(crate) portfolio: PortfolioAnalysisReadReference,
    pub(crate) price_forecast: Option<ForecastReference>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) source_action_reference: Option<
        crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
    >,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) current_share_action_reference: Option<
        crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
    >,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) fundamental_share_sources: Option<String>,
    pub(crate) probability_forecasts: ProbabilityForecastReferences,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) benchmark_instrument_id: Option<InstrumentId>,
    pub(crate) financial_forecasts: Vec<ForecastReference>,
    pub(crate) historical_study: Option<StudyReference>,
    pub(crate) selected_candidate: Option<CandidateReference>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ForecastReference {
    pub(crate) job_id: String,
    pub(crate) generation: u64,
    pub(crate) forecast_token: String,
    pub(crate) request_sha256: String,
}
/// Three independently nullable original forecast references; values and policies are re-admitted.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProbabilityForecastReferences {
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) price_higher: Option<ForecastReference>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) benchmark_outperformance: Option<ForecastReference>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) profit_after_costs: Option<ForecastReference>,
}
impl ProbabilityForecastReferences {
    pub(crate) fn entries(&self) -> [Option<&ForecastReference>; 3] {
        [
            self.price_higher.as_ref(),
            self.benchmark_outperformance.as_ref(),
            self.profit_after_costs.as_ref(),
        ]
    }
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StudyReference {
    pub(crate) request_digest: String,
    pub(crate) evidence_digest: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CandidateReference {
    pub(crate) candidate_id: String,
    pub(crate) screen_run_id: String,
    pub(crate) evidence_digest: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProfileBinding {
    pub(crate) profile_id: String,
    pub(crate) revision: u32,
    pub(crate) content_sha256: String,
}
impl ProfileBinding {
    pub(crate) fn domain(&self) -> Result<AnalyticalProfileBindingReference, ServiceError> {
        uuid(&self.profile_id)?;
        Ok(AnalyticalProfileBindingReference::new(
            SourceIdentifier::try_from(self.profile_id.clone()).map_err(invalid)?,
            NonZeroU32::new(self.revision).ok_or_else(|| invalid(()))?,
            digest(parse_digest(&self.content_sha256)?)?,
        ))
    }
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowBinding {
    pub(crate) workflow_id: String,
    pub(crate) revision: u32,
    pub(crate) content_sha256: String,
}
impl WorkflowBinding {
    pub(crate) fn domain(&self) -> Result<InvestmentAnalysisWorkflowReference, ServiceError> {
        uuid(&self.workflow_id)?;
        Ok(InvestmentAnalysisWorkflowReference::new(
            SourceIdentifier::try_from(self.workflow_id.clone()).map_err(invalid)?,
            NonZeroU32::new(self.revision).ok_or_else(|| invalid(()))?,
            digest(parse_digest(&self.content_sha256)?)?,
        ))
    }
}
pub(crate) fn uuid(value: &str) -> Result<Uuid, ServiceError> {
    let value_uuid = Uuid::parse_str(value).map_err(invalid)?;
    if value_uuid.is_nil() || value_uuid.to_string() != value {
        return Err(invalid(()));
    }
    Ok(value_uuid)
}
pub(crate) fn timestamp(value: &str) -> Result<Timestamp, ServiceError> {
    let nanos = value.parse::<i64>().map_err(invalid)?;
    if nanos <= 0 || nanos.to_string() != value {
        return Err(invalid(()));
    }
    Ok(Timestamp::from_unix_nanos(nanos))
}
pub(crate) fn digest(value: EvidenceDigest) -> Result<DecisionContentDigest, ServiceError> {
    DecisionContentDigest::try_new(value).map_err(invalid)
}
pub(crate) fn parse_digest(value: &str) -> Result<EvidenceDigest, ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(invalid)?;
        bytes[index] = u8::from_str_radix(pair, 16).map_err(invalid)?;
    }
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

/// Saved bytes are audit references, never a substitute for current source admission.
pub(crate) fn validate_canonical_request(bytes: &[u8]) -> Result<(), ServiceError> {
    if bytes.is_empty()
        || bytes.len() > market_squawk_decisions::MAX_INVESTMENT_ANALYSIS_REQUEST_BYTES
    {
        return Err(ServiceError::InvalidRequest);
    }
    let input: GenerateRequest = serde_json::from_slice(bytes).map_err(invalid)?;
    if input.financial_forecasts.len() > 16 || serde_json::to_vec(&input).map_err(invalid)? != bytes
    {
        return Err(ServiceError::InvalidRequest);
    }
    if let Some(sources) = &input.fundamental_share_sources {
        crate::application::fair_value::validate_fundamental_share_sources(sources.as_bytes())
            .map_err(invalid)?;
    }
    if input
        .benchmark_instrument_id
        .is_some_and(|id| id.as_uuid().is_nil())
    {
        return Err(ServiceError::InvalidRequest);
    }
    input.workflow.domain()?;
    input.analytical_profile.domain()?;
    let cutoff = timestamp(&input.source_cutoff_unix_nanos)?;
    let market_cutoff = input
        .portfolio
        .prerequisites()
        .source_cutoff()
        .map_err(|e| e.as_service_error())?;
    if cutoff > market_cutoff {
        return Err(ServiceError::InvalidRequest);
    }
    if let Some(reference) = &input.source_action_reference {
        if reference.knowledge_cutoff() > cutoff
            || !reference
                .requested_instruments()
                .contains(&input.portfolio.prerequisites().candidate_instrument_id())
            || input.price_forecast.is_none()
        {
            return Err(ServiceError::InvalidRequest);
        }
    }
    if let Some(reference) = &input.current_share_action_reference {
        if input.market.is_none()
            || input.price_forecast.is_none()
            || input.source_action_reference.is_none()
            || reference.knowledge_cutoff() < market_cutoff
            || !reference
                .requested_instruments()
                .contains(&input.portfolio.prerequisites().candidate_instrument_id())
        {
            return Err(ServiceError::InvalidRequest);
        }
    }
    if let Some(market) = &input.market {
        if market.source_cutoff()? != market_cutoff
            || market.instrument_id() != input.portfolio.prerequisites().candidate_instrument_id()
        {
            return Err(ServiceError::InvalidRequest);
        }
    }
    for forecast in input
        .price_forecast
        .iter()
        .chain(input.financial_forecasts.iter())
        .chain(input.probability_forecasts.entries().into_iter().flatten())
    {
        uuid(&forecast.job_id)?;
        uuid(&forecast.forecast_token)?;
        parse_digest(&forecast.request_sha256)?;
        if forecast.generation == 0 {
            return Err(ServiceError::InvalidRequest);
        }
    }
    if let Some(study) = input.historical_study {
        parse_digest(&study.request_digest)?;
        parse_digest(&study.evidence_digest)?;
    }
    if let Some(candidate) = input.selected_candidate {
        CandidateId::try_new(candidate.candidate_id).map_err(invalid)?;
        ScreenRunId::try_new(candidate.screen_run_id).map_err(invalid)?;
        parse_digest(&candidate.evidence_digest)?;
    }
    Ok(())
}
fn invalid<T>(_: T) -> ServiceError {
    ServiceError::InvalidRequest
}

/// Rechecks the request's semantic identity against its saved financial publication, without
/// using any request field as source or investment authority.
pub(crate) fn validate_request_publication(
    bytes: &[u8],
    bundle: &market_squawk_decisions::PreparedPublishedInvestmentAnalysis,
) -> Result<(), ServiceError> {
    validate_canonical_request(bytes)?;
    let input: GenerateRequest = serde_json::from_slice(bytes).map_err(invalid)?;
    let evidence = bundle.decision().evidence();
    if bundle.publication().workflow() != &input.workflow.domain()?
        || bundle.publication().analytical_profile() != &input.analytical_profile.domain()?
        || evidence.instrument_id() != input.portfolio.prerequisites().candidate_instrument_id()
        || evidence.account_id() != input.portfolio.prerequisites().account_id()
        || evidence.as_of() != timestamp(&input.source_cutoff_unix_nanos)?
    {
        return Err(ServiceError::InvalidResult);
    }
    if let Some(chart) = evidence.forecast_chart() {
        let saved = crate::application::model::forecast::SavedForecastChart::decode(chart)?;
        if input.source_action_reference.as_ref() != Some(saved.source_action_reference()) {
            return Err(ServiceError::InvalidResult);
        }
    }
    let comparison = evidence
        .benchmark_comparison()
        .ok_or(ServiceError::InvalidResult)?;
    let saved = crate::application::saved_benchmark::SavedBenchmarkComparison::decode(comparison)?;
    if saved.requested() != input.benchmark_instrument_id {
        return Err(ServiceError::InvalidResult);
    }
    let probabilities = evidence
        .probabilities()
        .ok_or(ServiceError::InvalidResult)?;
    if i64::try_from(probabilities.horizon_nanos().get()).ok()
        != Some(bundle.decision().policy().horizon_nanos())
    {
        return Err(ServiceError::InvalidResult);
    }
    if let (Some(expected), market_squawk_decisions::ProbabilityEventEvidence::Ready(actual)) = (
        input.benchmark_instrument_id,
        probabilities.benchmark_outperformance(),
    ) {
        if !matches!(actual.target(), market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent {
            event: market_squawk_data::ProbabilityEventTarget::BenchmarkOutperformance { benchmark_instrument_id, .. }, ..
        } if benchmark_instrument_id == expected)
        {
            return Err(ServiceError::InvalidResult);
        }
    }
    for (requested, retained) in input
        .probability_forecasts
        .entries()
        .into_iter()
        .zip(probabilities.events())
    {
        match (requested, retained.reference()) {
            (None, None) => {}
            (Some(expected), Some(actual))
                if *uuid(&expected.job_id)?.as_bytes() == actual.job_id()
                    && expected.generation == actual.generation().get()
                    && *uuid(&expected.forecast_token)?.as_bytes() == actual.forecast_token()
                    && parse_digest(&expected.request_sha256)?
                        == actual.request_identity().evidence_digest()
                    && parse_digest(&input.financial_profile.configuration_digest)?
                        == actual.profile_identity().evidence_digest() => {}
            _ => return Err(ServiceError::InvalidResult),
        }
    }
    match &input.market {
        Some(market) => {
            let audit = evidence
                .valuation_method_set()
                .ok_or(ServiceError::InvalidResult)?;
            if audit.market_cutoff() != market.source_cutoff()?
                || audit.profile_identity()
                    != parse_digest(&input.financial_profile.configuration_digest)?
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        None => {
            // This narrow application outcome records an actual missing current mark. It cannot
            // omit the original audit for any computed valuation or turn into an action on replay.
            if evidence.market().is_some()
                || evidence.valuation().is_some()
                || evidence.financial_model().is_some()
                || evidence.valuation_method_set().is_some()
                || evidence.liquidity().is_some()
                || evidence.portfolio_risk().is_some()
                || !matches!(bundle.decision(),
                    market_squawk_decisions::InvestmentProposalDecision::Unavailable(value)
                    if value.reason() == market_squawk_decisions::ProposalUnavailableReason::MissingEvidence(
                        market_squawk_decisions::RecommendationEvidenceKind::Market))
            {
                return Err(ServiceError::InvalidResult);
            }
        }
    }
    match (input.selected_candidate, evidence.selected_candidate()) {
        (None, None) => {}
        (Some(expected), Some(actual))
            if expected.candidate_id == actual.candidate_id().as_str()
                && expected.screen_run_id == actual.screen_run_id().as_str()
                && parse_digest(&expected.evidence_digest)?
                    == actual.evidence_digest().evidence_digest() => {}
        _ => return Err(ServiceError::InvalidResult),
    }
    Ok(())
}
