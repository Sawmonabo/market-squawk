//! Bounded comparison reads over the original authenticated projection.

use super::SavedInvestmentChartReader;
use crate::application::saved_benchmark::SavedBenchmarkComparison;
use market_squawk_decisions::InvestmentAnalysisEvidence;
use market_squawk_services::{RequestContext, ServiceError};
use serde_json::{Value, json};

impl SavedInvestmentChartReader {
    pub(super) async fn benchmark_viewport(
        &self,
        evidence: &InvestmentAnalysisEvidence,
        start: Option<i64>,
        end: Option<i64>,
        point_limit: usize,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let Some(comparison) = evidence.benchmark_comparison() else {
            return Ok(
                json!({"state":"unavailable", "basis":"split_adjusted_price_index",
                "members":[{"role":"subject","instrumentId":evidence.instrument_id(),"label":"Investment"}],
                "reason":"selection_unavailable", "summary":"No saved comparison is available."}),
            );
        };
        let saved = SavedBenchmarkComparison::decode(comparison)?;
        saved
            .read_projection(&self.research, start, end, point_limit, context)
            .await
    }
}
