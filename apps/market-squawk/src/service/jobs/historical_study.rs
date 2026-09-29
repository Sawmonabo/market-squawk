//! Exact completed historical study results through the existing installed jobs authority.

use super::*;

pub(in crate::service) const GET_RECOMMENDATION_BACKTEST_JOB_RESULT: &str =
    "Analysis.GetRecommendationBacktestJobResult";

impl InstalledJobOperations {
    pub(in crate::service) async fn read_recommendation_backtest_result(
        &self,
        runner: &crate::jobs::BacktestJobRunner,
        historical_reader: &crate::application::analysis::HistoricalRecommendationAlphaProducerReadCapability,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let origin = authenticated_origin(context)?;
        let input: GetRequest = decode(&super::super::business_arguments(request.arguments()))?;
        let id = parse_id(&input.job_id)?;
        let generation = parse_generation(input.generation)?;
        let snapshot = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
            result = self.repository.get(id, generation) => result.map_err(|_| ServiceError::Unavailable)?,
        };
        if snapshot.spec().origin() != &origin {
            return Err(ServiceError::Unauthorized);
        }
        let receipt = runner
            .read_recommendation_result(&snapshot, historical_reader, context)
            .await?;
        ensure_live(context)?;
        TypedToolResult::try_new(
            json!({"job": JobReceipt::from_snapshot(&snapshot), "backtest": receipt.report(),
                "requestDigest": digest_text(receipt.reference.request_digest.bytes()),
                "evidenceDigest": digest_text(receipt.reference.evidence_digest.bytes()),
            }),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}
