//! Provider-neutral projection of exact retained, backend-indexed comparison histories.

use super::{SavedInvestmentChartReader, nanos, quality};
use crate::application::{
    benchmark::{BenchmarkHistoryDisposition, BenchmarkHistoryEvaluation},
    saved_benchmark::{SavedBenchmarkComparison, SavedBenchmarkReplay, SavedBenchmarkUnavailable},
};
use market_squawk_decisions::InvestmentAnalysisEvidence;
use market_squawk_services::{RequestContext, ServiceError};
use serde_json::{Value, json};

impl SavedInvestmentChartReader {
    pub(super) async fn benchmark(
        &self,
        evidence: &InvestmentAnalysisEvidence,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let mut members = vec![
            json!({"role":"subject", "instrumentId":evidence.instrument_id(), "label":"Investment"}),
        ];
        let comparison = evidence
            .benchmark_comparison()
            .ok_or(ServiceError::InvalidResult)?;
        let saved = SavedBenchmarkComparison::decode(comparison)?;
        if let Some(selected) = saved.selected() {
            members.push(json!({"role":"selected", "instrumentId":selected.instrument_id(), "label":selected.label()}));
        } else if let Some(requested) = saved.requested() {
            members.push(
                json!({"role":"selected", "instrumentId":requested, "label":"Selected comparison"}),
            );
        }
        if let Some(accompanying) = saved.accompanying() {
            members.push(json!({"role":"accompanying", "instrumentId":accompanying.instrument_id(), "label":accompanying.label()}));
        }
        let actual = saved
            .replay(&self.research, &self.history, &self.calendars, context)
            .await?;
        super::super::ensure_live(context)?;
        let actual = match actual {
            SavedBenchmarkReplay::Evaluated(value) => value,
            SavedBenchmarkReplay::Unavailable(reason) => {
                let (reason, summary) = match reason {
                    SavedBenchmarkUnavailable::Selection => (
                        "selection_unavailable",
                        "The original comparison identity is unavailable. The saved choice has not been replaced.",
                    ),
                    SavedBenchmarkUnavailable::Storage => (
                        "storage_unavailable",
                        "The original comparison history is unavailable. Newer history has not been substituted.",
                    ),
                    SavedBenchmarkUnavailable::Integrity => (
                        "integrity_unproven",
                        "The original comparison history could not be verified.",
                    ),
                };
                return Ok(unavailable(members, reason, summary));
            }
        };
        match actual.disposition() {
            BenchmarkHistoryDisposition::MissingSubject => Ok(unavailable(
                members,
                "missing_subject",
                "No comparable price history was retained for this investment at the original cutoff.",
            )),
            BenchmarkHistoryDisposition::MissingSelectedComparison => Ok(unavailable(
                members,
                "missing_selected_comparison",
                "No comparable price history was retained for the selected comparison at the original cutoff.",
            )),
            BenchmarkHistoryDisposition::NoCommonObservation => Ok(unavailable(
                members,
                "no_common_observation",
                "The retained histories have no shared eligible observation on the same trading session.",
            )),
            BenchmarkHistoryDisposition::Available => available(members, &actual),
        }
    }
}

fn unavailable(members: Vec<Value>, reason: &str, summary: &str) -> Value {
    json!({"state":"unavailable", "basis":"split_adjusted_price_index", "members":members,
        "reason":reason, "summary":summary})
}

fn available(
    members: Vec<Value>,
    actual: &BenchmarkHistoryEvaluation,
) -> Result<Value, ServiceError> {
    let baseline = actual.baseline().ok_or(ServiceError::InvalidResult)?;
    if !(2..=3).contains(&members.len()) || actual.points().is_empty() {
        return Err(ServiceError::InvalidResult);
    }
    let mut points = Vec::new();
    points
        .try_reserve_exact(actual.points().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for point in actual.points() {
        if point.observations.len() != members.len() {
            return Err(ServiceError::InvalidResult);
        }
        let observations = point.observations.iter().map(|observation| observation.as_ref().map(|value| {
            json!({"close":value.close.normalize().to_string(), "priceIndex":value.price_index.normalize().to_string(),
                "availableAtUnixNanos":nanos(value.available_at),
                "providerCompletedAtUnixNanos":value.provider_completed_at.map(nanos), "quality":quality(value.quality)})
        })).collect::<Vec<_>>();
        points.push(json!({"coordinate":{"date":point.coordinate.date.to_string(),
            "sessionCloseUnixNanos":nanos(point.coordinate.session_close)}, "observations":observations}));
    }
    Ok(
        json!({"state":"available", "basis":"split_adjusted_price_index", "members":members,
        "summary":"Split-adjusted closing prices start at 100 on the same original trading session. Cash distributions are excluded; gaps indicate missing observations.",
        "baseline":{"date":baseline.date.to_string(), "sessionCloseUnixNanos":nanos(baseline.session_close)}, "points":points}),
    )
}
