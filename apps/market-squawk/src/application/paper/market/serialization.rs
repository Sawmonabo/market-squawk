//! Market JSON, source-coverage, and quality rendering.

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_domain::{DataQuality, InstrumentId, Timestamp};
use market_squawk_live::{
    LastTradeSnapshot, SnapshotDimension, StreamPhaseSnapshot, StreamSnapshot,
};
use market_squawk_services::ServiceError;
use serde_json::{Value, json};

use super::{MarketFilters, StreamView};
use crate::application::domain_support::encode_hex;
use crate::application::market_runtime::{
    MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease, MarketSourceSnapshotFailure,
    MarketSourceSnapshotFailureKind,
};

const MAXIMUM_LISTED_EVIDENCE_IDENTITIES: usize = 8;

pub(super) fn identity_value(view: &StreamView<'_>) -> Value {
    json!({
        "sourceId": view.stream.source().as_str(),
        "venueId": view.route.route().venue().as_str(),
        "instrumentId": view.route.route().instrument().to_string(),
        "providerProduct": view
            .stream
            .provider_product()
            .as_source_identifier()
            .as_str(),
        "providerChannel": view
            .stream
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "connectionGeneration": view.stream.connection_generation().get().to_string(),
        "stateRevision": view.stream.state_revision().to_string(),
        "shardId": view.shard.shard_id().to_string(),
        "shardSnapshotRevision": view.shard.snapshot_revision().get().to_string()
    })
}

pub(super) fn snapshot_value(
    view: &StreamView<'_>,
    level_limit: usize,
    reference_at: Timestamp,
) -> Value {
    let mut value = identity_value(view);
    value["phase"] = json!(view.stream.phase());
    value["lastSequence"] = json!(view.stream.last_sequence().map(|value| value.get()));
    value["snapshotOriginRevision"] = json!(view.stream.snapshot_origin_revision());
    value["snapshotInitialized"] = Value::Bool(view.stream.snapshot_initialized());
    value["generationCurrent"] = Value::Bool(view.stream.generation_current());
    value["healthEpoch"] = Value::from(view.stream.health_epoch());
    value["sourceTimestamp"] = json!(view.stream.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(view.stream.received_at()));
    value["evaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["publishedAt"] = Value::String(timestamp_value(view.shard.published_at()));
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["sourceValidUntil"] = Value::String(timestamp_value(view.stream.source_valid_until()));
    value["freshAtReference"] = Value::Bool(reference_at <= view.stream.source_valid_until());
    value["tradingStatus"] = json!(view.stream.trading_status());
    value["tradingStatusRevision"] = json!(view.stream.trading_status_revision());
    value["book"] = book_value(view.stream, level_limit);
    value["lastTrade"] = view
        .stream
        .last_trade()
        .map(|trade| trade_value(view, trade, reference_at))
        .unwrap_or(Value::Null);
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

pub(super) fn quality_value(view: &StreamView<'_>, reference_at: Timestamp) -> Value {
    let best_bid = view.stream.bids().first().map(|level| level.price());
    let best_ask = view.stream.asks().first().map(|level| level.price());
    let mut value = identity_value(view);
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["phase"] = json!(view.stream.phase());
    value["generationCurrent"] = Value::Bool(view.stream.generation_current());
    value["snapshotInitialized"] = Value::Bool(view.stream.snapshot_initialized());
    value["lastSequence"] = json!(view.stream.last_sequence().map(|value| value.get()));
    value["sourceTimestamp"] = json!(view.stream.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(view.stream.received_at()));
    value["evaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["sourceValidUntil"] = Value::String(timestamp_value(view.stream.source_valid_until()));
    value["referenceAt"] = Value::String(timestamp_value(reference_at));
    value["freshAtReference"] = Value::Bool(reference_at <= view.stream.source_valid_until());
    value["tradingStatus"] = json!(view.stream.trading_status());
    value["tradingStatusRevision"] = json!(view.stream.trading_status_revision());
    value["stateBidDepth"] = Value::from(view.stream.state_bid_depth());
    value["stateAskDepth"] = Value::from(view.stream.state_ask_depth());
    value["bidDimension"] = dimension_value(view.stream.bid_dimension());
    value["askDimension"] = dimension_value(view.stream.ask_dimension());
    value["crossedBook"] = Value::Bool(best_bid.zip(best_ask).is_some_and(|(bid, ask)| bid >= ask));
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

pub(super) fn trade_value(
    view: &StreamView<'_>,
    trade: &LastTradeSnapshot,
    reference_at: Timestamp,
) -> Value {
    let mut value = identity_value(view);
    value["sourceIdentifier"] = Value::String(trade.source_identifier().as_str().to_owned());
    value["stableTradeId"] = Value::String(trade.stable_trade_id().as_str().to_owned());
    value["tradeConnectionGeneration"] =
        Value::String(trade.connection_generation().get().to_string());
    value["priceTicks"] = Value::String(trade.price().get().to_string());
    value["quantityLots"] = Value::String(trade.quantity().get().to_string());
    value["aggressorSide"] = json!(trade.aggressor_side());
    value["takerOrderType"] = json!(trade.taker_order_type());
    value["sourceTimestamp"] = json!(trade.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(trade.received_at()));
    value["availableAt"] = Value::String(timestamp_value(trade.available_at()));
    value["ingestedAt"] = Value::String(timestamp_value(trade.ingested_at()));
    value["recordedQuality"] = json!(trade.recorded_quality());
    value["currentDisplayQuality"] = json!(current_trade_display_quality(trade, reference_at));
    value["recordedCoverage"] = json!(trade.recorded_coverage());
    value["assessmentId"] = Value::String(trade.assessment_id().as_str().to_owned());
    value["qualificationEvaluatedAt"] =
        Value::String(timestamp_value(trade.qualification_evaluated_at()));
    value["qualificationValidUntil"] =
        Value::String(timestamp_value(trade.qualification_valid_until()));
    value["freshAtReference"] = Value::Bool(reference_at <= trade.qualification_valid_until());
    value["payloadDigest"] = json!({
        "algorithm": trade.payload_digest().algorithm(),
        "bytes": encode_hex(trade.payload_digest().bytes())
    });
    value["bindingDigest"] = Value::String(encode_hex(trade.binding_digest()));
    value["tradeTradingStatus"] = json!(trade.trading_status());
    value["committedStateRevision"] = Value::String(trade.committed_state_revision().to_string());
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

pub(super) fn quote_value(view: &StreamView<'_>, reference_at: Timestamp) -> Value {
    let bid = view.stream.bids().first().copied();
    let ask = view.stream.asks().first().copied();
    let mut value = identity_value(view);
    value["bid"] = bid.map(level_value).unwrap_or(Value::Null);
    value["ask"] = ask.map(level_value).unwrap_or(Value::Null);
    value["sourceTimestamp"] = Value::Null;
    value["asOf"] = Value::String(timestamp_value(view.shard.published_at()));
    value["stateEvaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["crossed"] = Value::Bool(
        bid.zip(ask)
            .is_some_and(|(bid, ask)| bid.price() >= ask.price()),
    );
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

pub(super) fn comparison_value(
    instrument: InstrumentId,
    candidates: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    count: usize,
) -> Value {
    let observations = candidates
        .iter()
        .filter(|view| {
            filters.matches_time(view.stream.evaluated_at())
                && !(view.stream.bids().is_empty() && view.stream.asks().is_empty())
        })
        .map(|view| {
            let bid = view.stream.bids().first().copied();
            let ask = view.stream.asks().first().copied();
            let midpoint = bid.zip(ask).map(|(bid, ask)| {
                json!({
                    "numeratorTicks": (
                        i128::from(bid.price().get()) + i128::from(ask.price().get())
                    ).to_string(),
                    "denominator": "2"
                })
            });
            json!({
                "sourceId": view.stream.source().as_str(),
                "venueId": view.route.route().venue().as_str(),
                "providerProduct": view
                    .stream
                    .provider_product()
                    .as_source_identifier()
                    .as_str(),
                "providerChannel": view
                    .stream
                    .provider_channel()
                    .as_source_identifier()
                    .as_str(),
                "bid": bid.map(level_value),
                "ask": ask.map(level_value),
                "midpoint": midpoint,
                "asOf": timestamp_value(view.shard.published_at()),
                "stateEvaluatedAt": timestamp_value(view.stream.evaluated_at()),
                "recordedQuality": view.stream.quality(),
                "currentDisplayQuality": current_display_quality(view.stream, reference_at)
            })
        })
        .collect::<Vec<_>>();
    json!({
        "instrumentId": instrument.to_string(),
        "observationCount": count,
        "comparable": count >= 2,
        "observations": observations,
        "authority": "not_exposed"
    })
}

pub(super) fn book_value(stream: &StreamSnapshot, level_limit: usize) -> Value {
    let (bid_count, ask_count) =
        balanced_level_counts(stream.bids().len(), stream.asks().len(), level_limit);
    let bids = stream.bids()[..bid_count]
        .iter()
        .copied()
        .map(level_value)
        .collect::<Vec<_>>();
    let asks = stream.asks()[..ask_count]
        .iter()
        .copied()
        .map(level_value)
        .collect::<Vec<_>>();
    json!({
        "configuredDepth": stream.configured_depth(),
        "stateBidDepth": stream.state_bid_depth(),
        "stateAskDepth": stream.state_ask_depth(),
        "snapshotBidDimension": dimension_value(stream.bid_dimension()),
        "snapshotAskDimension": dimension_value(stream.ask_dimension()),
        "resultBidDimension": count_dimension(stream.bids().len(), bid_count, level_limit),
        "resultAskDimension": count_dimension(stream.asks().len(), ask_count, level_limit),
        "bids": bids,
        "asks": asks
    })
}

fn balanced_level_counts(bids: usize, asks: usize, limit: usize) -> (usize, usize) {
    let mut bid_count = bids.min(limit.saturating_add(1) / 2);
    let mut ask_count = asks.min(limit.saturating_sub(bid_count));
    bid_count = bids.min(limit.saturating_sub(ask_count));
    ask_count = asks.min(limit.saturating_sub(bid_count));
    (bid_count, ask_count)
}

fn count_dimension(available: usize, returned: usize, configured_limit: usize) -> Value {
    let completeness = if returned == available {
        "complete"
    } else if returned == 0 {
        "unavailable"
    } else {
        "truncated"
    };
    json!({
        "completeness": completeness,
        "available": available,
        "returned": returned,
        "configuredLimit": configured_limit
    })
}

fn dimension_value(dimension: &SnapshotDimension) -> Value {
    json!({
        "completeness": dimension.completeness(),
        "available": dimension.available(),
        "returned": dimension.returned(),
        "configuredLimit": dimension.configured_limit()
    })
}

fn level_value(level: market_squawk_live::BookLevelSnapshot) -> Value {
    json!({
        "priceTicks": level.price().get().to_string(),
        "quantityLots": level.quantity().get().to_string()
    })
}

pub(super) fn source_coverage_value(
    streams: &[StreamView<'_>],
    failures: &[MarketSourceSnapshotFailure],
    filters: &MarketFilters<'_>,
    display: &[&MarketDisplaySnapshotLease],
    kraken: &[&MarketKrakenPriceProjectionLease],
) -> Value {
    let mut sources = streams
        .iter()
        .map(|view| view.stream.source().as_str().to_owned())
        .collect::<Vec<_>>();
    sources.extend(
        display
            .iter()
            .map(|snapshot| snapshot.metadata().source_id().as_str().to_owned()),
    );
    sources.extend(
        kraken
            .iter()
            .map(|snapshot| snapshot.metadata().source_id().as_str().to_owned()),
    );
    sources.sort_unstable();
    sources.dedup();
    let source_count = sources.len();
    sources.truncate(MAXIMUM_LISTED_EVIDENCE_IDENTITIES);

    let mut venues = streams
        .iter()
        .map(|view| view.route.route().venue().as_str().to_owned())
        .collect::<Vec<_>>();
    venues.extend(
        display
            .iter()
            .map(|snapshot| snapshot.lease().key().venue_id().as_str().to_owned()),
    );
    venues.extend(
        kraken
            .iter()
            .map(|snapshot| snapshot.key().venue_id().as_str().to_owned()),
    );
    venues.sort_unstable();
    venues.dedup();
    let venue_count = venues.len();
    venues.truncate(MAXIMUM_LISTED_EVIDENCE_IDENTITIES);

    let failed_source_count = failures.len();
    let failed_sources = failures
        .iter()
        .take(MAXIMUM_LISTED_EVIDENCE_IDENTITIES)
        .map(|failure| {
            json!({
                "surfaceId": failure.surface_id().as_str(),
                "reason": match failure.kind() {
                    MarketSourceSnapshotFailureKind::ResourceExhausted => "resource_exhausted",
                    MarketSourceSnapshotFailureKind::Unavailable => "unavailable",
                }
            })
        })
        .collect::<Vec<_>>();

    json!({
        "mode": if display.is_empty() && kraken.is_empty() {
            "current_live_runtime"
        } else {
            "unified_current_market_runtime"
        },
        "consistency": if failed_source_count == 0 {
            "per_shard_current_non_atomic"
        } else {
            "partial_provider_set"
        },
        "historicalDataset": Value::Null,
        "requestedSourceCount": filters.sources.len(),
        "listedRequestedSources": filters
            .sources
            .iter()
            .take(MAXIMUM_LISTED_EVIDENCE_IDENTITIES)
            .copied()
            .collect::<Vec<_>>(),
        "listedRequestedSourcesComplete":
            filters.sources.len() <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "observedSourceCount": source_count,
        "listedSources": sources,
        "listedSourcesComplete": source_count <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "observedVenueCount": venue_count,
        "listedVenues": venues,
        "listedVenuesComplete": venue_count <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "failedSourceCount": failed_source_count,
        "failedSources": failed_sources,
        "listedFailedSourcesComplete":
            failed_source_count <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "streamIdentityScope": "complete",
        "bookDepthScope": "per_record_explicit",
        "displayObservationCount": display.len(),
        "krakenOrderLevelProjectionCount": kraken.len()
    })
}

pub(super) fn with_availability(mut coverage: Value, available: usize) -> Value {
    coverage["availability"] = Value::String(if available == 0 {
        "no_current_observation".to_owned()
    } else {
        "current".to_owned()
    });
    coverage
}

pub(super) struct QualitySummary {
    reference_at: Timestamp,
    recorded: [usize; 9],
    current: [usize; 9],
    fresh: usize,
    stale: usize,
}

impl QualitySummary {
    pub(super) const fn new(reference_at: Timestamp) -> Self {
        Self {
            reference_at,
            recorded: [0; 9],
            current: [0; 9],
            fresh: 0,
            stale: 0,
        }
    }

    pub(super) fn observe_stream(&mut self, stream: &StreamSnapshot) {
        let current = current_display_quality(stream, self.reference_at);
        self.recorded[quality_index(stream.quality())] += 1;
        self.current[quality_index(current)] += 1;
        if self.reference_at <= stream.source_valid_until() {
            self.fresh += 1;
        } else {
            self.stale += 1;
        }
    }

    pub(super) fn observe_trade(&mut self, trade: &LastTradeSnapshot) {
        let current = current_trade_display_quality(trade, self.reference_at);
        self.recorded[quality_index(trade.recorded_quality())] += 1;
        self.current[quality_index(current)] += 1;
        if self.reference_at <= trade.qualification_valid_until() {
            self.fresh += 1;
        } else {
            self.stale += 1;
        }
    }

    pub(super) fn into_value(self) -> Value {
        json!({
            "referenceAt": timestamp_value(self.reference_at),
            "recordedClassifications": quality_counts(&self.recorded),
            "currentDisplayClassifications": quality_counts(&self.current),
            "freshObservations": self.fresh,
            "staleObservations": self.stale,
            "authority": "not_exposed"
        })
    }
}

fn quality_counts(counts: &[usize; 9]) -> Value {
    const QUALITIES: [DataQuality; 9] = [
        DataQuality::DirectVerified,
        DataQuality::DirectUnverified,
        DataQuality::OfficialDelayed,
        DataQuality::Aggregated,
        DataQuality::Indicative,
        DataQuality::Modeled,
        DataQuality::Estimated,
        DataQuality::Stale,
        DataQuality::Quarantined,
    ];
    Value::Array(
        QUALITIES
            .into_iter()
            .zip(counts)
            .filter(|(_, count)| **count > 0)
            .map(|(quality, count)| json!({"quality": quality, "count": count}))
            .collect(),
    )
}

const fn quality_index(quality: DataQuality) -> usize {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

pub(super) fn require_observable_top(stream: &StreamSnapshot) -> Result<(), ServiceError> {
    if [stream.bid_dimension(), stream.ask_dimension()]
        .into_iter()
        .any(|dimension| dimension.available() > 0 && dimension.returned() == 0)
    {
        Err(ServiceError::Unavailable)
    } else {
        Ok(())
    }
}

fn current_trade_display_quality(
    trade: &LastTradeSnapshot,
    reference_at: Timestamp,
) -> DataQuality {
    if reference_at <= trade.qualification_valid_until() {
        trade.recorded_quality()
    } else {
        DataQuality::Stale
    }
}

pub(super) fn current_display_quality(
    stream: &StreamSnapshot,
    reference_at: Timestamp,
) -> DataQuality {
    if !stream.generation_current()
        || stream.phase() != StreamPhaseSnapshot::Healthy
        || stream.quality() == DataQuality::Quarantined
    {
        DataQuality::Quarantined
    } else if reference_at > stream.source_valid_until() {
        DataQuality::Stale
    } else {
        stream.quality()
    }
}

pub(super) fn timestamp_value(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}
