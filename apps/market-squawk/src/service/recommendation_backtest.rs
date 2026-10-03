//! Installed exact-reference reads of governed recommendation studies.

use std::{sync::Arc, time::Instant};

use market_squawk_data::Sha256Digest;
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::application::analysis::{
    GovernedRecommendationBacktestReferenceV1, ProductionGovernedBacktestInputAuthority,
    ProductionGovernedBacktestRepository,
};
use crate::application::decision::{
    DecisionApplication, recommendation::adapt_recommendation_backtest_v1,
};

pub(super) const GET_RECOMMENDATION_BACKTEST: &str = "Analysis.GetRecommendationBacktest";

/// Keeps historical reports accessible through their exact retained request and evidence identity.
/// A report read cannot issue a signal, replace a selected study, or grant proposal authority.
pub(super) struct InstalledRecommendationBacktestReadOperations {
    inputs: Arc<ProductionGovernedBacktestInputAuthority>,
    repository: Arc<ProductionGovernedBacktestRepository>,
    decisions: Arc<DecisionApplication>,
    runtime: RuntimeIdentity,
    historical_reader: Option<Arc<crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability>>,
}

impl InstalledRecommendationBacktestReadOperations {
    pub(super) const fn new(
        inputs: Arc<ProductionGovernedBacktestInputAuthority>,
        repository: Arc<ProductionGovernedBacktestRepository>,
        decisions: Arc<DecisionApplication>,
        runtime: RuntimeIdentity,
        historical_reader: Option<Arc<crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability>>,
    ) -> Self {
        Self {
            inputs,
            repository,
            decisions,
            runtime,
            historical_reader,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        operation == GET_RECOMMENDATION_BACKTEST
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if !Self::owns(request.name()) {
            return Err(ServiceError::NotFound);
        }
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid() {
            return Err(ServiceError::Unauthorized);
        }
        ensure_live(context)?;
        let arguments: RecommendationBacktestReadRequest = serde_json::from_value(Value::Object(
            super::business_arguments(request.arguments()),
        ))
        .map_err(|_| ServiceError::InvalidRequest)?;
        let as_of = super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let retained = match arguments {
            RecommendationBacktestReadRequest::Analysis(arguments) => {
                let token = Uuid::parse_str(&arguments.action_token)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                if token.is_nil() || token.to_string() != arguments.action_token {
                    return Err(ServiceError::InvalidRequest);
                }
                let analysis_id = self
                    .decisions
                    .resolve_investment_analysis_product_token(token)
                    .map_err(super::decision::map_application)?;
                let analysis = self
                    .decisions
                    .read_investment_analysis(analysis_id)
                    .map_err(super::decision::map_application)?;
                let evidence = analysis.decision.evidence();
                let expected_backtest = evidence.backtest().ok_or(ServiceError::NotFound)?;
                let expected_oos = evidence.out_of_sample().ok_or(ServiceError::NotFound)?;
                // The saved report identity was minted by this same adapter. Resolve the
                // request through the durable terminal index; command identity is a signal
                // plan identity and cannot be substituted for that retained request.
                let retained = self
                    .repository
                    .read_recommendation_receipt(
                        &self.inputs,
                        Sha256Digest::new(
                            expected_backtest
                                .report_identity()
                                .evidence_digest()
                                .bytes(),
                        ),
                        as_of,
                        self.historical_reader.as_deref().ok_or(ServiceError::Unavailable)?,
                        context,
                    )
                    .await?;
                let restored = adapt_recommendation_backtest_v1(&retained.evidence)
                    .map_err(|_| ServiceError::InvalidResult)?;
                if restored.historical_test != *expected_backtest
                    || restored.out_of_sample != *expected_oos
                {
                    return Err(ServiceError::InvalidResult);
                }
                retained
            }
            RecommendationBacktestReadRequest::Exact(arguments) => {
                let reference = GovernedRecommendationBacktestReferenceV1 {
                    request_digest: parse_digest(&arguments.request_digest)?,
                    evidence_digest: parse_digest(&arguments.evidence_digest)?,
                };
                self.repository
                    .read_recommendation_report(
                        &self.inputs,
                        reference,
                        as_of,
                        self.historical_reader.as_deref().ok_or(ServiceError::Unavailable)?,
                        context,
                    )
                    .await?
            }
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            retained.report(),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

impl std::fmt::Debug for InstalledRecommendationBacktestReadOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledRecommendationBacktestReadOperations")
            .field("inputs", &"[EXACT GOVERNED INPUT AUTHORITY]")
            .field("repository", &"[GOVERNED STUDY REPOSITORY]")
            .field("decisions", &"[RETAINED INVESTMENT ANALYSIS AUTHORITY]")
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RecommendationBacktestReadRequest {
    Analysis(RecommendationBacktestAnalysisRequest),
    Exact(RecommendationBacktestExactRequest),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecommendationBacktestAnalysisRequest {
    action_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecommendationBacktestExactRequest {
    request_digest: String,
    evidence_digest: String,
}

fn parse_digest(value: &str) -> Result<Sha256Digest, ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (destination, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| ServiceError::InvalidRequest)?;
        *destination = u8::from_str_radix(pair, 16).map_err(|_| ServiceError::InvalidRequest)?;
    }
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(Sha256Digest::new(bytes))
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
