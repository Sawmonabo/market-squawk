//! Viewport reads over persisted, original investment chart evidence.

mod action_ranges;
mod benchmark;

use crate::{ResearchService, application::model::forecast::SavedForecastChart};
use market_squawk_analytics::{HarmonicDirection, HarmonicPatternKind};
use market_squawk_decisions::{
    HarmonicHistoryDisposition, InvestmentAnalysisEvidence, InvestmentProposalDecision,
};
use market_squawk_domain::{PriceTicks, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ChartViewportRequest {
    pub(super) action_token: String,
    start_unix_nanos: Option<String>,
    end_unix_nanos: Option<String>,
    point_limit: Option<usize>,
    #[serde(default)]
    layer: ChartLayer,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChartLayer {
    History,
    Forecast,
    Benchmark,
    PricePattern,
    ActionRanges,
    #[default]
    All,
}
impl ChartLayer {
    const fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Forecast => "forecast",
            Self::Benchmark => "benchmark",
            Self::PricePattern => "price_pattern",
            Self::ActionRanges => "action_ranges",
            Self::All => "all",
        }
    }
    const fn wants(self, layer: Self) -> bool {
        matches!(self, Self::All) || self as u8 == layer as u8
    }
}

pub(super) struct ChartViewport {
    start: Option<i64>,
    end: Option<i64>,
    point_limit: usize,
    layer: ChartLayer,
}
impl ChartViewportRequest {
    pub(super) fn viewport(&self) -> Result<ChartViewport, ServiceError> {
        let parse = |value: &str| {
            value
                .parse::<i64>()
                .ok()
                .filter(|number| number.to_string() == value)
                .ok_or(ServiceError::InvalidRequest)
        };
        let start = self.start_unix_nanos.as_deref().map(parse).transpose()?;
        let end = self.end_unix_nanos.as_deref().map(parse).transpose()?;
        let point_limit = self.point_limit.unwrap_or(1000);
        if !(8..=4096).contains(&point_limit)
            || start.zip(end).is_some_and(|(start, end)| start > end)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(ChartViewport {
            start,
            end,
            point_limit,
            layer: self.layer,
        })
    }
}

pub(in crate::service) struct SavedInvestmentChartReader {
    pub(in crate::service) research: Arc<ResearchService>,
}

impl SavedInvestmentChartReader {
    pub(super) async fn read_viewport(
        &self,
        decision: &InvestmentProposalDecision,
        viewport: ChartViewport,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        super::ensure_live(context)?;
        let evidence = decision.evidence();
        let projection = if !matches!(viewport.layer, ChartLayer::Benchmark) {
            if let Some(saved) = evidence.forecast_chart() {
                let saved = SavedForecastChart::decode(saved)?;
                let read = if viewport.layer.wants(ChartLayer::History) {
                    saved
                        .read_projection(
                            &self.research,
                            viewport.start,
                            viewport.end,
                            viewport.point_limit,
                            context,
                        )
                        .await
                } else {
                    saved.read_metadata(&self.research, context).await
                };
                match read {
                    Ok(value) => Some(value),
                    Err(ServiceError::NotFound | ServiceError::Unavailable) => None,
                    Err(error) => return Err(error),
                }
            } else {
                None
            }
        } else {
            None
        };
        let frame = projection.as_ref().map(|value| &value.frame);
        if let (Some(frame), Some(audit)) = (frame, evidence.harmonic_history()) {
            let input = audit.input();
            if input.instrument_id != frame.instrument_id()
                || input.currency != frame.origin_price().currency()
                || input.source_cutoff != frame.source_cutoff()
                || input.observed_through != frame.origin_at()
                || input.adjustment_identity.bytes() != frame.basis_identity().bytes()
                || input.completeness_identity.bytes() != frame.history_identity().bytes()
                || input.source_identity.bytes() != frame.source_read_identity().bytes()
                || input.calendar_identity.bytes() != frame.calendar_identity().bytes()
                || input.selected_manifest.bytes()
                    != frame.selected_manifest()?.content_hash().bytes()
                || input.origin_manifest.bytes() != frame.origin_manifest()?.content_hash().bytes()
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        let history = if viewport.layer.wants(ChartLayer::History) {
            projection
                .as_ref()
                .map(|value| value.history.clone())
                .unwrap_or_else(|| unavailable_history())
        } else {
            deferred_history()
        };
        let forecast = if viewport.layer.wants(ChartLayer::Forecast) {
            let mut value = projection
                .as_ref()
                .map(|value| value.forecast.clone())
                .unwrap_or_else(|| unavailable_forecast());
            if let Some(points) = value.get_mut("points").and_then(Value::as_array_mut) {
                for point in points.iter() {
                    if point
                        .get("timeUnixNanos")
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<i64>().ok())
                        .is_none()
                    {
                        return Err(ServiceError::InvalidResult);
                    }
                }
                points.retain(|point| {
                    point
                        .get("timeUnixNanos")
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<i64>().ok())
                        .is_some_and(|at| {
                            viewport.start.is_none_or(|start| at >= start)
                                && viewport.end.is_none_or(|end| at <= end)
                        })
                });
            }
            value
        } else {
            unavailable_forecast()
        };
        let price_pattern = if viewport.layer.wants(ChartLayer::PricePattern) {
            pattern(evidence, frame)?
        } else {
            let mut value = pattern(evidence, None)?;
            value["summary"] = json!("Open the price-pattern layer to load its saved evidence.");
            value
        };
        let action_ranges = if viewport.layer.wants(ChartLayer::ActionRanges) {
            action_ranges::value(decision, frame)?
        } else {
            json!({"state":"unavailable","basis":"split_adjusted_price","reason":"not_requested",
                "summary":"Open the action-range layer to load its saved evidence.",
                "informationCurrentThroughUnixNanos":nanos(evidence.as_of()),
                "admittedAtUnixNanos":nanos(evidence.admitted_at()),
                "expiresAtUnixNanos":nanos(decision.expires_at().min(decision.horizon_at())),"ranges":[]})
        };
        let benchmark = if viewport.layer.wants(ChartLayer::Benchmark) {
            self.benchmark_viewport(
                evidence,
                viewport.start,
                viewport.end,
                viewport.point_limit,
                context,
            )
            .await?
        } else {
            json!({"state":"unavailable","basis":"split_adjusted_price_index","members":[],
            "reason":"not_requested","summary":"Open the comparison layer to load its saved evidence."})
        };
        let display = projection
            .as_ref()
            .and_then(|value| value.history.get("display"))
            .or_else(|| benchmark.get("display"));
        let full_start = display
            .and_then(|value| value.get("firstTimeUnixNanos"))
            .cloned()
            .unwrap_or(Value::Null);
        let full_end = display
            .and_then(|value| value.get("lastTimeUnixNanos"))
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .map(|last| last.max(decision.horizon_at().unix_nanos()))
            .map(|last| Value::String(last.to_string()))
            .unwrap_or(Value::Null);
        super::ensure_live(context)?;
        Ok(json!({
            "informationCurrentThroughUnixNanos":nanos(evidence.as_of()),
            "history":history, "forecast":forecast, "pricePattern":price_pattern,
            "benchmark":benchmark, "actionRanges":action_ranges,
            "viewport":{"startUnixNanos":viewport.start.map(|v|v.to_string()),
                "endUnixNanos":viewport.end.map(|v|v.to_string()),"pointLimit":viewport.point_limit,"layer":viewport.layer.name(),
                "fullStartUnixNanos":full_start,"fullEndUnixNanos":full_end},
            "basisExplanation":"Saved history, price patterns and the forecast origin retain their original split-adjusted share units. Comparison values retain the original base-100 anchor. Displayed subsets do not replace the complete analytical inputs or establish current trading eligibility."
        }))
    }
}
fn unavailable_history() -> Value {
    json!({"state":"unavailable","summary":"The original saved price history cannot currently be opened.",
        "basis":"split_adjusted_price","points":[]})
}
fn deferred_history() -> Value {
    json!({"state":"unavailable","summary":"Open the history layer to load its saved evidence.",
        "basis":"split_adjusted_price","points":[]})
}
fn unavailable_forecast() -> Value {
    json!({"state":"unavailable","summary":"No saved price forecast is loaded for this layer.",
        "basis":"saved_price_projection","observedThroughUnixNanos":null,
        "origin":{"state":"unavailable","summary":"No original observed price is loaded for this layer."},"points":[]})
}

fn pattern(
    evidence: &InvestmentAnalysisEvidence,
    proof: Option<&SavedForecastChart>,
) -> Result<Value, ServiceError> {
    let audit = proof.and_then(|_| evidence.harmonic_history());
    let receipt = proof.and_then(|_| evidence.harmonic_pattern());
    let status = match audit.map(|audit| audit.input().disposition) {
        None => "unavailable",
        Some(HarmonicHistoryDisposition::Pattern) => "confirmed",
        Some(HarmonicHistoryDisposition::InsufficientBars) => "insufficient_bars",
        Some(HarmonicHistoryDisposition::InsufficientPivots) => "insufficient_pivots",
        Some(HarmonicHistoryDisposition::NoMatchingPattern) => "no_matching_pattern",
        Some(HarmonicHistoryDisposition::Expired) => "expired",
        Some(HarmonicHistoryDisposition::Invalidated) => "invalidated",
    };
    let summary = match status {
        "confirmed" => {
            "This pattern was confirmed using information available for the saved analysis. A pattern alone is not a trading recommendation."
        }
        "expired" => {
            "The pattern had expired by the analysis date. No active pattern shape was saved."
        }
        "invalidated" => {
            "The pattern had been invalidated by prices available at the analysis date. No active pattern shape was saved."
        }
        "insufficient_bars" => "Too few daily prices were available to evaluate a pattern.",
        "insufficient_pivots" => "The price history had too few confirmed turning points.",
        "no_matching_pattern" => "No qualifying pattern was found in the saved price history.",
        _ if evidence.harmonic_history().is_some() => {
            "The saved price-pattern evidence cannot currently be reopened."
        }
        _ => "No saved price-pattern analysis is available.",
    };
    let mut pivots = Vec::new();
    let mut ratios = Vec::new();
    let mut targets = Vec::new();
    let mut reversal = None;
    let mut invalidation = None;
    if let (Some(audit), Some(receipt)) = (audit, receipt) {
        let input = audit.input();
        let geometry = input.geometry.as_ref().ok_or(ServiceError::InvalidResult)?;
        let price = |ticks: PriceTicks| {
            ticks
                .checked_to_decimal(input.analytical_tick)
                .map(|value| value.normalize().to_string())
                .map_err(|_| ServiceError::InvalidResult)
        };
        for (name, pivot) in ["X", "A", "B", "C", "D"].into_iter().zip(geometry.pivots) {
            pivots.push(
                json!({"name":name,"kind":if pivot.high {"high"} else {"low"},
                "observedAtUnixNanos":nanos(pivot.observed_at),
                "availableAtUnixNanos":nanos(pivot.available_at),
                "confirmedAtUnixNanos":nanos(pivot.confirmed_at),"value":price(pivot.price)?}),
            );
        }
        for (name, ratio) in [
            "AB/XA", "BC/AB", "CD/BC", "CD/AB", "AD/XA", "XC/XA", "CD/XC",
        ]
        .into_iter()
        .zip(geometry.ratios)
        {
            if let Some((numerator, denominator)) = ratio {
                ratios.push(json!({"name":name,"numerator":numerator.to_string(),"denominator":denominator.get().to_string()}));
            }
        }
        let (lower, upper) = (receipt.completion_lower(), receipt.completion_upper());
        reversal = Some(json!({"lower":price(lower)?,"upper":price(upper)?}));
        invalidation = Some(price(receipt.invalidation())?);
        for target in receipt.targets() {
            targets.push(price(target)?);
        }
    } else if receipt.is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let mut interpretation = vec!["A pattern alone does not establish a probability, confidence level or ability to trade.".to_owned(),
        "Confirmation dates show when each turning point became known. Developing patterns are not shown.".to_owned()];
    let original_forecast = evidence
        .current_share_projection()
        .map(|projection| projection.original_forecast())
        .or_else(|| evidence.price_forecast().copied());
    if let (Some(pattern), Some(forecast), Some(proof)) = (receipt, original_forecast, proof) {
        // The original proof origin and saved forecast share one split-adjusted unit basis.
        // A current market mark is unadjusted and cannot supply this comparison after a split.
        let change = forecast
            .cases()
            .base()
            .amount()
            .cmp(&proof.origin_price().amount());
        let conflict = matches!(
            (pattern.direction(), change),
            (HarmonicDirection::Bullish, std::cmp::Ordering::Less)
                | (HarmonicDirection::Bearish, std::cmp::Ordering::Greater)
        );
        interpretation.push(if conflict {
            "The pattern direction conflicts with the saved central forecast in the same share units. Compare valuation and risk separately."
        } else {
            "The pattern does not conflict with the saved central forecast direction. Compare valuation and risk separately; agreement alone does not increase confidence."
        }.to_owned());
    }
    Ok(
        json!({"status":status,"summary":summary,"basis":"split_adjusted_price",
        "kind":receipt.map(|r| kind(r.kind())),
        "direction":receipt.map(|r|match r.direction(){HarmonicDirection::Bullish=>"bullish",HarmonicDirection::Bearish=>"bearish"}),
        "pivots":pivots,"ratios":ratios,"reversalZone":reversal,"invalidation":invalidation,
        "targets":targets,"expiresAtUnixNanos":receipt.map(|r|nanos(r.expires_at())),
        "observationCutoffUnixNanos":receipt.map(|r|nanos(r.observation_cutoff())),
        "confirmationCutoffUnixNanos":receipt.map(|r|nanos(r.confirmation_cutoff())),
        "interpretation":interpretation}),
    )
}
fn nanos(value: Timestamp) -> String {
    value.unix_nanos().to_string()
}
fn kind(value: HarmonicPatternKind) -> &'static str {
    match value {
        HarmonicPatternKind::AbCd => "ab_cd",
        HarmonicPatternKind::Gartley => "gartley",
        HarmonicPatternKind::Bat => "bat",
        HarmonicPatternKind::Butterfly => "butterfly",
        HarmonicPatternKind::Crab => "crab",
        HarmonicPatternKind::DeepCrab => "deep_crab",
        HarmonicPatternKind::Cypher => "cypher",
        HarmonicPatternKind::Shark => "shark",
    }
}
