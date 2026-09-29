//! Provider-qualified decimal book, chart and source-cohort observations.
//! These observations retain supplied economics and clocks without acquiring execution authority.

use super::{MarketDataEventError, MarketDataReference};
use crate::{
    LiveEventClass, LiveProvenance, MarketDepth, Money, ProviderInstrumentId, SourceIdentifier,
    Timestamp,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;

/// Local retention bounds, never provider capacity promises.
pub const MAX_MARKET_DATA_BOOK_LEVELS: usize = 2048;
pub const MAX_MARKET_DATA_BOOK_PARTICIPANTS: usize = 8192;
pub const MAX_MARKET_DATA_SCREENER_ITEMS: usize = 1024;

/// Distinguishes missing, explicit null, and supplied provider values.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MarketDataReported<T> {
    Absent,
    Null,
    Value(T),
}
impl<T> MarketDataReported<T> {
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Absent | Self::Null => None,
        }
    }
}

/// Economic units require an explicit source contract; no lot conversion is implied.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataSizeUnit {
    Unspecified,
    Shares,
    Contracts,
    CurrencyPairs,
}

/// Provider-authored participant detail at a price level.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataBookParticipant {
    pub identifier: SourceIdentifier,
    pub size: Decimal,
    /// Original millisecond value; no epoch is inferred when the source does not establish one.
    pub quote_time_millis: MarketDataReported<u64>,
}

/// Exact provider-authored price level; participant totals are not silently substituted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataBookLevel {
    pub price: Money,
    pub aggregate_size: Decimal,
    pub participant_count: u64,
    pub participants: Vec<MarketDataBookParticipant>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataBookInput {
    pub provenance: LiveProvenance,
    pub reference: MarketDataReference,
    pub size_unit: MarketDataSizeUnit,
    pub bids: Vec<MarketDataBookLevel>,
    pub asks: Vec<MarketDataBookLevel>,
}

/// Whole source book image; price-level coverage is provider qualified, never consolidated depth.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MarketDataBookEvent(MarketDataBookInput);
impl MarketDataBookEvent {
    pub fn try_new(input: MarketDataBookInput) -> Result<Self, MarketDataEventError> {
        validate_instrument(
            &input.provenance,
            &input.reference,
            LiveEventClass::BookSnapshot,
        )?;
        if input
            .provenance
            .binding()
            .book_state()
            .is_none_or(|state| state.depth() != MarketDepth::PriceLevel)
            || input
                .bids
                .len()
                .checked_add(input.asks.len())
                .is_none_or(|n| n > MAX_MARKET_DATA_BOOK_LEVELS)
        {
            return Err(MarketDataEventError::Binding);
        }
        let mut participants = 0usize;
        for side in [&input.bids, &input.asks] {
            for level in side {
                if level.price.currency() != input.reference.currency() {
                    return Err(MarketDataEventError::Currency);
                }
                if level.price.amount() <= Decimal::ZERO
                    || level.aggregate_size < Decimal::ZERO
                    || level.participant_count
                        != u64::try_from(level.participants.len())
                            .map_err(|_| MarketDataEventError::Size)?
                {
                    return Err(MarketDataEventError::Size);
                }
                participants = participants
                    .checked_add(level.participants.len())
                    .ok_or(MarketDataEventError::Size)?;
                if participants > MAX_MARKET_DATA_BOOK_PARTICIPANTS {
                    return Err(MarketDataEventError::Size);
                }
                let mut names = BTreeSet::new();
                for participant in &level.participants {
                    if participant.size < Decimal::ZERO || !names.insert(&participant.identifier) {
                        return Err(MarketDataEventError::Size);
                    }
                }
            }
        }
        // Preserve provider order and crossed whole images; neither is normalized into executable depth.
        Ok(Self(input))
    }
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.0.provenance
    }
    pub const fn reference(&self) -> &MarketDataReference {
        &self.0.reference
    }
    pub const fn input(&self) -> &MarketDataBookInput {
        &self.0
    }
}
impl<'de> Deserialize<'de> for MarketDataBookEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_new(MarketDataBookInput::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

/// A timestamp may identify a candle without proving its exact period boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataChartTimestampBasis {
    Unspecified,
    PeriodStart,
    PeriodEnd,
}

/// A Streamer chart observation does not attest completed historical-bar authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataChartCompletion {
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataChartInput {
    pub provenance: LiveProvenance,
    pub reference: MarketDataReference,
    pub interval_nanos: u64,
    pub timestamp_basis: MarketDataChartTimestampBasis,
    pub completion: MarketDataChartCompletion,
    pub open: Money,
    pub high: Money,
    pub low: Money,
    pub close: Money,
    pub volume: Decimal,
    pub volume_unit: MarketDataSizeUnit,
    pub provider_sequence: MarketDataReported<u64>,
    pub provider_day: MarketDataReported<i64>,
}

/// Original source chart update, explicitly distinct from a completed research MarketBar.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MarketDataChartEvent(MarketDataChartInput);
impl MarketDataChartEvent {
    pub fn try_new(input: MarketDataChartInput) -> Result<Self, MarketDataEventError> {
        validate_instrument(&input.provenance, &input.reference, LiveEventClass::Chart)?;
        if input.interval_nanos == 0
            || input.interval_nanos > 86_400_000_000_000
            || input.volume < Decimal::ZERO
        {
            return Err(MarketDataEventError::Size);
        }
        for value in [input.open, input.high, input.low, input.close] {
            if value.currency() != input.reference.currency() {
                return Err(MarketDataEventError::Currency);
            }
        }
        if input.low.amount() > input.high.amount()
            || input.open.amount() < input.low.amount()
            || input.open.amount() > input.high.amount()
            || input.close.amount() < input.low.amount()
            || input.close.amount() > input.high.amount()
        {
            return Err(MarketDataEventError::Size);
        }
        Ok(Self(input))
    }
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.0.provenance
    }
    pub const fn reference(&self) -> &MarketDataReference {
        &self.0.reference
    }
    pub const fn input(&self) -> &MarketDataChartInput {
        &self.0
    }
}
impl<'de> Deserialize<'de> for MarketDataChartEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_new(MarketDataChartInput::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

/// Source screen sort semantics; percentages remain percentages, never fractional returns.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataScreenerSort {
    Volume,
    Trades,
    PercentChangeUp,
    PercentChangeDown,
    AveragePercentVolume,
}

/// An original ranked member, optionally joined to an exact independently admitted identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataScreenerItem {
    pub symbol: ProviderInstrumentId,
    pub reference: Option<MarketDataReference>,
    pub description: MarketDataReported<String>,
    /// Decimal source quotation; currency is known only when an exact reference establishes it.
    pub last_price: MarketDataReported<Decimal>,
    pub market_share_percent: MarketDataReported<Decimal>,
    pub net_change: MarketDataReported<Decimal>,
    pub net_percent_change: MarketDataReported<Decimal>,
    pub total_volume: MarketDataReported<u64>,
    pub trades: MarketDataReported<u64>,
    pub volume: MarketDataReported<u64>,
    pub volume_unit: MarketDataSizeUnit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataScreenerInput {
    pub provenance: LiveProvenance,
    pub cohort_key: SourceIdentifier,
    pub sort: MarketDataScreenerSort,
    /// None means the source's all-day period, never a made-up midnight interval.
    pub frequency_minutes: Option<u16>,
    pub items: Vec<MarketDataScreenerItem>,
}

/// Whole bounded source cohort, including a genuinely empty returned list.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MarketDataScreenerEvent(MarketDataScreenerInput);
impl MarketDataScreenerEvent {
    pub fn try_new(input: MarketDataScreenerInput) -> Result<Self, MarketDataEventError> {
        if input.provenance.binding().event_class() != LiveEventClass::Screener
            || input.provenance.instrument_id().is_some()
            || input.provenance.source_identifier() != &input.cohort_key
            || !matches!(input.provenance.binding().scope(), crate::LiveEvidenceScope::SourceCohort(key) if key == &input.cohort_key)
            || input.provenance.source_timestamp().is_none()
            || input
                .provenance
                .source_timestamp()
                .is_some_and(|time| time > input.provenance.received_at())
            || input.items.len() > MAX_MARKET_DATA_SCREENER_ITEMS
            || input
                .frequency_minutes
                .is_some_and(|value| !matches!(value, 1 | 5 | 10 | 30 | 60))
        {
            return Err(MarketDataEventError::Binding);
        }
        let mut symbols = BTreeSet::new();
        for item in &input.items {
            if !symbols.insert(&item.symbol)
                || item
                    .description
                    .value()
                    .is_some_and(|value| value.len() > 2048 || value.contains('\0'))
                || item
                    .last_price
                    .value()
                    .is_some_and(|value| *value < Decimal::ZERO)
                || item
                    .market_share_percent
                    .value()
                    .is_some_and(|value| *value < Decimal::ZERO || *value > Decimal::from(100u32))
            {
                return Err(MarketDataEventError::Size);
            }
            if let Some(reference) = &item.reference {
                reference.validate_at(input.provenance.received_at())?;
                if reference.source_symbol() != &item.symbol {
                    return Err(MarketDataEventError::Reference);
                }
            }
        }
        Ok(Self(input))
    }
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.0.provenance
    }
    pub const fn input(&self) -> &MarketDataScreenerInput {
        &self.0
    }
}
impl<'de> Deserialize<'de> for MarketDataScreenerEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_new(MarketDataScreenerInput::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

fn validate_instrument(
    provenance: &LiveProvenance,
    reference: &MarketDataReference,
    class: LiveEventClass,
) -> Result<(), MarketDataEventError> {
    reference.validate_at(provenance.received_at())?;
    if provenance.instrument_id() != Some(reference.instrument_id())
        || provenance.binding().event_class() != class
        || provenance.source_identifier().as_str() != reference.source_symbol().as_str()
        || provenance.source_timestamp().is_none()
        || provenance
            .source_timestamp()
            .is_some_and(|time| time > provenance.received_at())
    {
        return Err(MarketDataEventError::Binding);
    }
    Ok(())
}
