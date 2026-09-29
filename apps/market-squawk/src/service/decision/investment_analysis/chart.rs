//! Saved, read-only chart projection. All economic values come from retained native authorities.

mod action_ranges;
mod benchmark;

use crate::{
    ResearchService,
    application::{
        MarketHistoryReadCapability, MarketHistoryUnavailableReason,
        market_calendar::CompletedMarketSessionReadCapability,
        model::forecast::{
            ExactHorizonPriceForecastEvidence, ForecastEvidenceReadContext, ForecastEvidenceReader,
            ForecastPriceEvidence, LatestValidForecast, SavedForecastChart,
            SelectedPriceForecast, SelectedPriceInterval, replay_price_history,
        },
        SourceAppliedCorporateActionReadCapability,
    },
};
use market_squawk_analytics::{HarmonicDirection, HarmonicPatternKind};
use market_squawk_data::{ForecastBasisHistory, Sha256Digest};
use market_squawk_decisions::{
    HarmonicHistoryDisposition, InvestmentAnalysisEvidence, InvestmentProposalDecision,
};
use market_squawk_domain::{DataQuality, PriceTicks, Timestamp};
use market_squawk_modeling::ForecastValue;
use market_squawk_services::{ArtifactReadContext, RequestContext, ServiceError};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, sync::Arc};

pub(in crate::service) struct SavedInvestmentChartReader {
    pub(in crate::service) research: Arc<ResearchService>,
    pub(in crate::service) calendars: CompletedMarketSessionReadCapability,
    pub(in crate::service) history: MarketHistoryReadCapability,
    pub(in crate::service) forecasts: Arc<dyn ForecastEvidenceReader>,
    pub(in crate::service) maximum_forecast_artifact_bytes: NonZeroUsize,
}

impl SavedInvestmentChartReader {
    pub(super) async fn read(
        &self,
        decision: &InvestmentProposalDecision,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let evidence = decision.evidence();
        super::ensure_live(context)?;
        let selected = self.selected_forecast(evidence, context).await?;
        let price = selected.as_ref().and_then(|selected| match selected.price_evidence() {
            ForecastPriceEvidence::Available(price) => Some(price.as_ref()),
            ForecastPriceEvidence::Unavailable(_) => None,
        });
        let proof = self.history(evidence, price, context).await?;
        let pattern_verified = if let (Some(proof), Some(saved)) = (&proof, evidence.harmonic_history()) {
            match self.history
                .read_forecast_basis_harmonic(
                    &self.research,
                    proof,
                    saved.input().execution_tick,
                    Some(saved),
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await
            {
                Ok(_) => true,
                Err(MarketHistoryUnavailableReason::StorageUnavailable) => false,
                Err(reason) => return Err(map_history_unavailable(reason)),
            }
        } else {
            false
        };
        let history = history_value(proof.as_ref());
        let forecast = forecast_value(evidence, selected.as_ref(), proof.as_ref())?;
        let benchmark = self.benchmark(evidence, context).await?;
        super::ensure_live(context)?;
        Ok(json!({
            "informationCurrentThroughUnixNanos": nanos(evidence.as_of()),
            "history": history,
            "forecast": forecast,
            "pricePattern": pattern(evidence, proof.as_ref().filter(|_| pattern_verified))?,
            "benchmark": benchmark,
            "actionRanges": action_ranges::value(decision, proof.as_ref())?,
            "basisExplanation":"Saved history, price patterns and the forecast origin use the original split-adjusted share units when their exact source evidence can be reopened. The benchmark uses a separate base-100 comparison. Available saved action ranges are converted into those same original share units and shown only within their original admission and expiry interval. They do not establish current trading eligibility."
        }))
    }

    async fn history(
        &self,
        evidence: &InvestmentAnalysisEvidence,
        price: Option<&SelectedPriceForecast>,
        context: &RequestContext,
    ) -> Result<Option<ForecastBasisHistory>, ServiceError> {
        let (Some(saved), Some(price)) = (evidence.forecast_chart(), price) else {
            return Ok(None);
        };
        let saved = SavedForecastChart::decode(saved)?;
        let source_actions = SourceAppliedCorporateActionReadCapability::new(
            Arc::clone(&self.research),
            self.calendars.clone(),
        );
        let result = replay_price_history(
            price,
            &self.research,
            &self.calendars,
            &source_actions,
            saved.source_action_reference(),
            evidence.forecast_chart(),
            context,
        )
        .await;
        super::ensure_live(context)?;
        let result = match result {
            Ok(value) => value,
            Err(ServiceError::Unavailable | ServiceError::NotFound) => None,
            Err(error) => return Err(error),
        };
        Ok(result.map(|(proof, _)| proof))
    }

    async fn selected_forecast(
        &self,
        evidence: &InvestmentAnalysisEvidence,
        context: &RequestContext,
    ) -> Result<Option<LatestValidForecast>, ServiceError> {
        let Some(saved) = evidence.price_forecast() else {
            return Ok(None);
        };
        let selected = self
            .forecasts
            .exact_distribution_for_identity(
                Sha256Digest::new(saved.vintage_id().bytes()),
                evidence.instrument_id(),
                evidence.admitted_at(),
                ForecastEvidenceReadContext::new(
                    ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
                    self.maximum_forecast_artifact_bytes,
                ),
            )
            .await;
        super::ensure_live(context)?;
        let selected = match selected {
            Ok(value) => value,
            Err(error) => {
                let error = crate::application::model::map_forecast_selection_error(error);
                return match error {
                    ServiceError::NotFound | ServiceError::Unavailable => Ok(None),
                    error => Err(error),
                };
            }
        };
        let ForecastPriceEvidence::Available(price) = selected.price_evidence() else {
            return Err(ServiceError::InvalidResult);
        };
        let ExactHorizonPriceForecastEvidence::Available(projection) = selected
            .exact_horizon_price_projection(price.terminal_horizon_nanos())
            .map_err(|_| ServiceError::InvalidResult)?
        else {
            return Err(ServiceError::InvalidResult);
        };
        if price.instrument_id() != evidence.instrument_id()
            || price.currency() != evidence.currency()
            || price.vintage_id().bytes() != saved.vintage_id().bytes()
            || projection.price_derivation_identity().bytes()
                != saved.output_binding_identity().evidence_digest().bytes()
            || projection.calibration_identity().bytes()
                != saved.calibration_identity().evidence_digest().bytes()
            || projection.calibration().residuals_hash().bytes()
                != saved.outcome_set_identity().evidence_digest().bytes()
            || projection.terminal_at() != saved.horizon_at()
            || price.observed_through() != saved.window().observed_at()
            || projection.source_knowledge_cutoff() != saved.window().source_knowledge_cutoff()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Some(selected))
    }
}

fn history_value(proof: Option<&ForecastBasisHistory>) -> Value {
    let Some(proof) = proof else {
        return json!({"state":"unavailable","summary":"The original saved price history cannot currently be opened.",
            "basis":"split_adjusted_price","points":[]});
    };
    let points = proof.rows().iter().map(|row| {
        json!({
            "coordinate":if row.provider_timestamp.is_some() {
                json!({"kind":"timestamp","timeUnixNanos":nanos(row.observed_at)})
            } else {
                json!({"kind":"session_date","date":row.native_date.to_string(),
                    "sessionCloseUnixNanos":nanos(row.session_close)})
            },
            "availableAtUnixNanos":row.available_at.map(nanos),
            "value":row.prices.as_ref().map(|prices|prices.close.amount().normalize().to_string()),
            "quality":row.quality.map(quality),
        })
    }).collect::<Vec<_>>();
    json!({"state":"available","basis":"split_adjusted_price","points":points,
        "summary":"Original daily closing prices in the saved forecast's split-adjusted share units. Empty observations are genuine gaps; dates identify market sessions."})
}

fn forecast_value(
    evidence: &InvestmentAnalysisEvidence,
    selected: Option<&LatestValidForecast>,
    proof: Option<&ForecastBasisHistory>,
) -> Result<Value, ServiceError> {
    let Some(selected) = selected else {
        let summary = if evidence.price_forecast().is_some() {
            "The saved price forecast cannot currently be opened."
        } else {
            "No saved price forecast is available for this analysis."
        };
        return Ok(json!({"state":"unavailable","summary":summary,
            "basis":"saved_price_projection","observedThroughUnixNanos":null,
            "origin":unavailable_origin(),"points":[]}));
    };
    let ForecastPriceEvidence::Available(price) = selected.price_evidence() else {
        return Err(ServiceError::InvalidResult);
    };
    let [point] = price.points() else {
        return Err(ServiceError::InvalidResult);
    };
    let points = vec![json!({
        "timeUnixNanos":nanos(point.target_at()),
        "central":amount(point.central())?,
        "interval50":point.intervals().map(|v| interval(v.interval_50())).transpose()?,
        "interval80":point.intervals().map(|v| interval(v.interval_80())).transpose()?,
        "interval95":point.intervals().map(|v| interval(v.interval_95())).transpose()?,
    })];
    Ok(json!({"state":"available","basis":"saved_price_projection",
        "observedThroughUnixNanos":nanos(price.observed_through()),
        "origin":forecast_origin(price, proof)?,"points":points,
        "summary":"One saved expected price at the selected horizon with calibrated ranges. No intermediate future path was estimated."}))
}

fn map_history_unavailable(reason: MarketHistoryUnavailableReason) -> ServiceError {
    match reason {
        MarketHistoryUnavailableReason::Cancelled => ServiceError::Cancelled,
        MarketHistoryUnavailableReason::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketHistoryUnavailableReason::CapacityExceeded => ServiceError::ResourceExhausted,
        MarketHistoryUnavailableReason::IntegrityUnproven => ServiceError::InvalidResult,
        MarketHistoryUnavailableReason::StorageUnavailable => ServiceError::Unavailable,
    }
}

/// Uses the authenticated final history close when available. Older saved forecasts can still
/// expose their revalidated return-to-price anchor without selecting a current quote.
fn forecast_origin(
    price: &SelectedPriceForecast,
    proof: Option<&ForecastBasisHistory>,
) -> Result<Value, ServiceError> {
    if let Some(proof) = proof {
        let origin = proof.rows().last().ok_or(ServiceError::InvalidResult)?;
        let close = origin.prices.as_ref().ok_or(ServiceError::InvalidResult)?.close;
        let source_quality = origin.quality.ok_or(ServiceError::InvalidResult)?;
        if proof.instrument_id() != price.instrument_id()
            || proof.origin_at() != price.observed_through()
            || origin.observed_at != proof.origin_at()
            || close != proof.origin_price()
            || close.currency() != price.currency()
        {
            return Err(ServiceError::InvalidResult);
        }
        let coordinate = if origin.provider_timestamp.is_some() {
            json!({"kind":"timestamp","timeUnixNanos":nanos(origin.observed_at)})
        } else {
            json!({"kind":"session_date","date":origin.native_date.to_string(),
                "sessionCloseUnixNanos":nanos(origin.session_close)})
        };
        return Ok(json!({"state":"available","basis":"split_adjusted_price",
            "coordinate":coordinate,"value":close.amount().normalize().to_string(),
            "quality":quality(source_quality),
            "summary":"Original observed close in the same split-adjusted share units as the saved forecast. Cash dividends are excluded."}));
    }
    if price
        .model_metadata()
        .output_binding()
        .expected_arithmetic_return_horizon_nanos()
        != Some(price.terminal_horizon_nanos())
    {
        return Ok(unavailable_origin());
    }
    let serving = price.serving_evidence();
    let Some(origin) = serving.origin_bar() else {
        return Ok(unavailable_origin());
    };
    if origin.context().provenance().instrument_id().is_none() {
        return Ok(unavailable_origin());
    }
    if serving.observed_through() != Some(price.observed_through())
        || origin.context().provenance().instrument_id() != Some(price.instrument_id())
        || origin.currency() != price.currency()
    {
        return Err(ServiceError::InvalidResult);
    }
    // This is the same choice used by the original price::project transformation. The current
    // input's raw source, exact split plan and named-session coordinate were independently
    // reopened by read_forecast_index_selection; legacy split bars are artifact-validated.
    let value = if let Some(current) = serving.current_price_input() {
        current.current_unit_price
    } else {
        market_squawk_modeling::validate_forecast_price_origin(
            origin,
            serving.source_id(),
            price.observed_through(),
            serving.knowledge_cutoff(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        origin.close()
    };
    if value.currency() != price.currency() || value.amount() <= rust_decimal::Decimal::ZERO {
        return Err(ServiceError::InvalidResult);
    }
    Ok(json!({
        "state":"available",
        "basis":"split_adjusted_price",
        "coordinate":match origin.time_semantics().nominal_daily_date() {
            Some(date) => json!({"kind":"session_date","date":date.date().to_string(),
                "sessionCloseUnixNanos":nanos(price.observed_through())}),
            None => json!({"kind":"timestamp","timeUnixNanos":nanos(price.observed_through())}),
        },
        "value":value.amount().normalize().to_string(),
        "quality":quality(origin.context().provenance().quality()),
        "summary":"Original closing price in the same share units as this forecast. Split adjustments are included; cash dividends are excluded."
    }))
}

fn unavailable_origin() -> Value {
    json!({"state":"unavailable",
        "summary":"The saved forecast does not establish a matching observed price."})
}

fn pattern(
    evidence: &InvestmentAnalysisEvidence,
    proof: Option<&ForecastBasisHistory>,
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
        "insufficient_bars" => {
            "Too few daily prices were available to evaluate a pattern."
        }
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
    if let (Some(pattern), Some(forecast), Some(proof)) =
        (receipt, original_forecast, proof)
    {
        // The original proof origin and saved forecast share one split-adjusted unit basis.
        // A current market mark is unadjusted and cannot supply this comparison after a split.
        let change = forecast.cases().base().amount().cmp(&proof.origin_price().amount());
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
fn amount(value: ForecastValue) -> Result<String, ServiceError> {
    rust_decimal::Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
        .map(|value| value.normalize().to_string())
        .map_err(|_| ServiceError::InvalidResult)
}
fn interval(value: SelectedPriceInterval) -> Result<Value, ServiceError> {
    Ok(json!({"lower":amount(value.lower())?,"upper":amount(value.upper())?}))
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
fn quality(value: DataQuality) -> &'static str {
    match value {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}
