//! Atomic source-ranked market snapshots; these are not investment recommendations.

use std::fmt;

use rust_decimal::Decimal;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::intraday_candle::{
    MarketSnapshotError, MarketSnapshotVolume, MarketSourceText, validate_market_raw_context,
    validate_snapshot_context,
};
use crate::{
    EvidenceDigest, InstrumentId, MetadataRevision, Money, ProviderChannel, ProviderProduct,
    ResearchContext, RevisionNumber, SourceIdentifier, Timestamp, VenueId,
};

/// Application ceiling for one atomic ranking, not a provider entitlement or universe size.
pub const MAX_RANKED_MARKET_ITEMS: usize = 1_024;

/// Source-symbol byte ceiling, independent of the larger bounded description allowance.
pub const MAX_RANKED_MARKET_SYMBOL_BYTES: usize = 256;

/// Financial source time and local observation time are different evidence classes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "basis", rename_all = "snake_case")]
pub enum RankedMarketTime {
    /// The source supplies an as-of instant and an independent update/envelope instant.
    SourceReported {
        /// Financial instant of the whole source-ranked snapshot.
        snapshot_at: Timestamp,
        /// Source message/update instant.
        updated_at: Timestamp,
    },
    /// The source supplies neither instant; only receipt of this result is established.
    LocalObserved {
        /// Exact local receipt instant, never represented as a provider as-of timestamp.
        observed_at: Timestamp,
    },
}

impl RankedMarketTime {
    /// Returns the effective selector coordinate together with its separately retained basis.
    pub const fn effective_at(self) -> Timestamp {
        match self {
            Self::SourceReported { snapshot_at, .. } => snapshot_at,
            Self::LocalObserved { observed_at } => observed_at,
        }
    }
}

/// Source-neutral financial scope of a ranking; native universe lineage is retained separately.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "value",
    rename_all = "snake_case"
)]
pub enum RankedMarketUniverse {
    /// The source's eligible equities, not a claim of complete listed-equity coverage.
    Equities,
    /// The source's eligible options.
    Options,
    /// The source's eligible call options.
    CallOptions,
    /// The source's eligible put options.
    PutOptions,
    /// Equities scoped to one resolved canonical venue.
    EquityVenue(VenueId),
    /// Equities scoped to a resolved canonical index.
    EquityIndex {
        /// Canonical index identity selected at the snapshot knowledge boundary.
        instrument_id: InstrumentId,
        /// Exact reference revision used by the index resolver.
        reference_revision: MetadataRevision,
        /// Nonzero index-selection evidence digest.
        evidence: EvidenceDigest,
        /// Earliest conservative availability of the selected index mapping.
        available_at: Timestamp,
    },
    /// Source index scope retained before exact canonical index identity is established.
    UnresolvedEquityIndex,
}

/// Source ranking rule; order is retained exactly and never recomputed from display values.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankedMarketSort {
    /// Source ranks by traded volume in the selected frequency.
    Volume,
    /// Source ranks by trade count in the selected frequency.
    Trades,
    /// Source ranks positive percentage changes.
    PercentChangeUp,
    /// Source ranks negative percentage changes.
    PercentChangeDown,
    /// Source ranks its reported average-percent-volume measure.
    AveragePercentVolume,
}

/// Source-defined ranking window, without manufacturing a UTC start for an all-day session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankedMarketFrequency {
    /// Provider-defined trading day; no calendar/timezone is inferred.
    AllDay,
    /// One-minute ranking window.
    OneMinute,
    /// Five-minute ranking window.
    FiveMinutes,
    /// Ten-minute ranking window.
    TenMinutes,
    /// Thirty-minute ranking window.
    ThirtyMinutes,
    /// Sixty-minute ranking window.
    SixtyMinutes,
}

/// Completeness of the returned ranked set, independently of its always-subset universe coverage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankedMarketCompleteness {
    /// Every item in this source ranking response was retained.
    Complete,
    /// An explicitly partial source ranking was retained.
    Partial,
    /// An explicitly truncated ranking was retained.
    Truncated,
    /// The source did not establish response completeness.
    Unknown,
}

/// A reported financial component or explicit source absence; missing never becomes zero.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    content = "value",
    rename_all = "snake_case"
)]
pub enum RankedMarketMetric<T> {
    /// Exact source-reported component.
    Observed(T),
    /// The snapshot contains no value for this component.
    SourceMissing,
}

impl<T> RankedMarketMetric<T> {
    /// Returns only a genuinely observed component.
    pub const fn observed(&self) -> Option<&T> {
        match self {
            Self::Observed(value) => Some(value),
            Self::SourceMissing => None,
        }
    }
}

/// Monetary price evidence with a closed unresolved-currency state for unresolved source symbols.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    content = "value",
    rename_all = "snake_case"
)]
pub enum RankedMarketPrice {
    /// Price/change whose currency is established by exact source/reference evidence.
    Monetary(Money),
    /// Exact source decimal whose currency cannot yet be established; not admitted as money.
    CurrencyUnresolved(Decimal),
}

impl RankedMarketPrice {
    fn normalized(self) -> Self {
        match self {
            Self::Monetary(value) => Self::Monetary(value),
            Self::CurrencyUnresolved(value) => Self::CurrencyUnresolved(value.normalize()),
        }
    }
}

/// Canonical resolution at the observation's knowledge boundary; native identity is never lost.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "state", rename_all = "snake_case")]
pub enum RankedMarketIdentity {
    /// One exact resolver-selected instrument and its reference evidence.
    Resolved {
        /// Canonical instrument identity.
        instrument_id: InstrumentId,
        /// Exact represented venue when established; some instruments are venue-independent.
        venue_id: Option<VenueId>,
        /// Exact canonical reference revision used by the resolver.
        reference_revision: MetadataRevision,
        /// Nonzero identity-selection evidence digest.
        evidence: EvidenceDigest,
        /// Earliest conservative availability of the selected mapping.
        available_at: Timestamp,
    },
    /// No exact mapping exists at the retained cutoff.
    Unresolved,
    /// Multiple mappings exist; the ranking must not choose the first one.
    Ambiguous {
        /// Number of competing mappings established by the bounded resolver.
        candidate_count: u32,
    },
}

/// Exact source components for one ranked member; percentage fields use percent, not fractions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RankedMarketItemInput {
    /// One-based position in the source's ordered array.
    pub rank: u32,
    /// Source-native symbol retained whether or not canonical identity resolves.
    pub provider_instrument_id: MarketSourceText,
    /// Exact, unresolved, or ambiguous canonical mapping disposition.
    pub identity: RankedMarketIdentity,
    /// Bounded source description when supplied; not canonical identity.
    pub description: Option<MarketSourceText>,
    /// Last source price, with exact or unresolved currency state.
    pub last_price: RankedMarketMetric<RankedMarketPrice>,
    /// Source net change, which may be negative.
    pub net_change: RankedMarketMetric<RankedMarketPrice>,
    /// Signed source net change in percentage points; 1 means one percent.
    pub net_percent_change: RankedMarketMetric<Decimal>,
    /// Source market-share percentage in [0, 100], with its source scope retained.
    pub market_share_percent: RankedMarketMetric<Decimal>,
    /// Total source volume for its trading day, not the selected frequency.
    pub total_volume: RankedMarketMetric<MarketSnapshotVolume>,
    /// Source volume for the selected ranking frequency.
    pub volume: RankedMarketMetric<MarketSnapshotVolume>,
    /// Integral source trade count for the selected frequency.
    pub trades: RankedMarketMetric<u64>,
}

/// One source-ranked member; unresolved identities retain their position rather than disappearing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RankedMarketItemInput")]
pub struct RankedMarketItem {
    rank: u32,
    provider_instrument_id: MarketSourceText,
    identity: RankedMarketIdentity,
    description: Option<MarketSourceText>,
    last_price: RankedMarketMetric<RankedMarketPrice>,
    net_change: RankedMarketMetric<RankedMarketPrice>,
    net_percent_change: RankedMarketMetric<Decimal>,
    market_share_percent: RankedMarketMetric<Decimal>,
    total_volume: RankedMarketMetric<MarketSnapshotVolume>,
    volume: RankedMarketMetric<MarketSnapshotVolume>,
    trades: RankedMarketMetric<u64>,
}

impl RankedMarketItem {
    /// Validates retained position, monetary consistency, percentages, and identity disposition.
    pub fn try_new(input: RankedMarketItemInput) -> Result<Self, MarketSnapshotError> {
        if input.rank == 0 || u64::from(input.rank) > MAX_RANKED_MARKET_ITEMS as u64 {
            return Err(MarketSnapshotError::InvalidRankedItems);
        }
        if input.provider_instrument_id.as_str().len() > MAX_RANKED_MARKET_SYMBOL_BYTES {
            return Err(MarketSnapshotError::InvalidNativeText);
        }
        match &input.identity {
            RankedMarketIdentity::Resolved { evidence, .. } if evidence.bytes() == [0; 32] => {
                return Err(MarketSnapshotError::InvalidResolution);
            }
            RankedMarketIdentity::Ambiguous { candidate_count } if *candidate_count < 2 => {
                return Err(MarketSnapshotError::InvalidResolution);
            }
            _ => {}
        }
        if let (Some(last), Some(change)) =
            (input.last_price.observed(), input.net_change.observed())
        {
            match (last, change) {
                (RankedMarketPrice::Monetary(last), RankedMarketPrice::Monetary(change))
                    if last.currency() == change.currency() => {}
                (
                    RankedMarketPrice::CurrencyUnresolved(_),
                    RankedMarketPrice::CurrencyUnresolved(_),
                ) => {}
                _ => return Err(MarketSnapshotError::CurrencyMismatch),
            }
        }
        if input
            .market_share_percent
            .observed()
            .is_some_and(|value| *value < Decimal::ZERO || *value > Decimal::ONE_HUNDRED)
        {
            return Err(MarketSnapshotError::InvalidPercentage);
        }
        if let (Some(total), Some(volume)) =
            (input.total_volume.observed(), input.volume.observed())
            && total.unit() != volume.unit()
        {
            return Err(MarketSnapshotError::InvalidRankedItems);
        }
        Ok(Self {
            rank: input.rank,
            provider_instrument_id: input.provider_instrument_id,
            identity: input.identity,
            description: input.description,
            last_price: normalize_price(input.last_price),
            net_change: normalize_price(input.net_change),
            net_percent_change: normalize_decimal(input.net_percent_change),
            market_share_percent: normalize_decimal(input.market_share_percent),
            total_volume: input.total_volume,
            volume: input.volume,
            trades: input.trades,
        })
    }

    /// Returns the retained one-based source rank.
    pub const fn rank(&self) -> u32 {
        self.rank
    }
    /// Returns native identifier lineage, including unresolved members.
    pub const fn provider_instrument_id(&self) -> &MarketSourceText {
        &self.provider_instrument_id
    }
    /// Returns exact canonical identity or its explicit unresolved disposition.
    pub const fn identity(&self) -> &RankedMarketIdentity {
        &self.identity
    }
    /// Returns the source description when supplied.
    pub const fn description(&self) -> Option<&MarketSourceText> {
        self.description.as_ref()
    }
    /// Returns last-price evidence and currency state.
    pub const fn last_price(&self) -> RankedMarketMetric<RankedMarketPrice> {
        self.last_price
    }
    /// Returns signed net monetary-change evidence.
    pub const fn net_change(&self) -> RankedMarketMetric<RankedMarketPrice> {
        self.net_change
    }
    /// Returns signed net percentage change, where 1 means one percent.
    pub const fn net_percent_change(&self) -> RankedMarketMetric<Decimal> {
        self.net_percent_change
    }
    /// Returns market-share percentage in its retained source scope.
    pub const fn market_share_percent(&self) -> RankedMarketMetric<Decimal> {
        self.market_share_percent
    }
    /// Returns source day volume and its exact unit state.
    pub const fn total_volume(&self) -> RankedMarketMetric<MarketSnapshotVolume> {
        self.total_volume
    }
    /// Returns source frequency volume and its exact unit state.
    pub const fn volume(&self) -> RankedMarketMetric<MarketSnapshotVolume> {
        self.volume
    }
    /// Returns source frequency trade count or explicit absence.
    pub const fn trades(&self) -> RankedMarketMetric<u64> {
        self.trades
    }
}

impl TryFrom<RankedMarketItemInput> for RankedMarketItem {
    type Error = MarketSnapshotError;
    fn try_from(value: RankedMarketItemInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

/// Complete input for one atomic source ranking; the vector is never a full-universe claim.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RankedMarketSnapshotObservationInput {
    /// Set-wide context with no singular instrument/venue and exact raw content evidence.
    pub context: ResearchContext,
    /// Exact provider product.
    pub provider_product: ProviderProduct,
    /// Exact source channel/service.
    pub provider_channel: ProviderChannel,
    /// Source-neutral financial universe scope.
    pub universe: RankedMarketUniverse,
    /// Native universe key before sort/frequency, retained for source-qualified natural identity.
    pub provider_universe: SourceIdentifier,
    /// Exact source ranking rule.
    pub sort: RankedMarketSort,
    /// Exact source ranking window.
    pub frequency: RankedMarketFrequency,
    /// Exact source clocks, or explicitly local observation when the source supplies no clocks.
    pub time: RankedMarketTime,
    /// Completeness of the returned ranked set only, never the eligible universe.
    pub completeness: RankedMarketCompleteness,
    /// Total ranked members reported by the source, absent when not supplied.
    pub reported_total: Option<u32>,
    /// Every retained member in source order, including unresolved identities.
    #[serde(deserialize_with = "deserialize_items")]
    pub items: Vec<RankedMarketItem>,
}

/// One immutable, atomic ranking of a source-qualified subset at an exact snapshot time.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RankedMarketSnapshotObservationInput")]
pub struct RankedMarketSnapshotObservation {
    context: ResearchContext,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    universe: RankedMarketUniverse,
    provider_universe: SourceIdentifier,
    sort: RankedMarketSort,
    frequency: RankedMarketFrequency,
    time: RankedMarketTime,
    completeness: RankedMarketCompleteness,
    reported_total: Option<u32>,
    items: Box<[RankedMarketItem]>,
}

impl RankedMarketSnapshotObservation {
    /// Admits a bounded ranking atomically, preserving source order, membership, and all clocks.
    pub fn try_new(
        input: RankedMarketSnapshotObservationInput,
    ) -> Result<Self, MarketSnapshotError> {
        if input.context.provenance().instrument_id().is_some()
            || input.context.provenance().venue_id().is_some()
        {
            return Err(MarketSnapshotError::InvalidSnapshotScope);
        }
        match input.time {
            RankedMarketTime::SourceReported {
                snapshot_at,
                updated_at,
            } => {
                validate_snapshot_context(&input.context, updated_at)?;
                if snapshot_at > updated_at {
                    return Err(MarketSnapshotError::InvalidChronology);
                }
            }
            RankedMarketTime::LocalObserved { observed_at } => {
                validate_market_raw_context(&input.context)?;
                if input.context.provenance().source_timestamp().is_some()
                    || input.context.time().published().is_some()
                    || observed_at != input.context.provenance().received_at()
                    || input
                        .context
                        .provenance()
                        .availability()
                        .conservative_available_at()
                        .is_none_or(|available| available < observed_at)
                {
                    return Err(MarketSnapshotError::InvalidChronology);
                }
            }
        }
        if input.context.time().effective().exact_timestamp() != Some(input.time.effective_at()) {
            return Err(MarketSnapshotError::InvalidChronology);
        }
        if input.items.len() > MAX_RANKED_MARKET_ITEMS {
            return Err(MarketSnapshotError::LimitExceeded);
        }
        if let Some(reported) = input.reported_total {
            let returned = input.items.len() as u32;
            if reported < returned
                || (input.completeness == RankedMarketCompleteness::Complete
                    && reported != returned)
                || (input.completeness == RankedMarketCompleteness::Truncated
                    && reported == returned)
            {
                return Err(MarketSnapshotError::InvalidRankedItems);
            }
        }
        let available = input
            .context
            .provenance()
            .availability()
            .conservative_available_at()
            .ok_or(MarketSnapshotError::InvalidChronology)?;
        if let RankedMarketUniverse::EquityIndex {
            evidence,
            available_at,
            ..
        } = &input.universe
            && (evidence.bytes() == [0; 32] || *available_at > available)
        {
            return Err(MarketSnapshotError::InvalidResolution);
        }
        let mut native_ids = Vec::new();
        native_ids
            .try_reserve_exact(input.items.len())
            .map_err(|_| MarketSnapshotError::AllocationFailed)?;
        for (index, item) in input.items.iter().enumerate() {
            if item.rank != (index + 1) as u32 {
                return Err(MarketSnapshotError::InvalidRankedItems);
            }
            native_ids.push(item.provider_instrument_id.as_str());
            if let RankedMarketIdentity::Resolved { available_at, .. } = &item.identity
                && *available_at > available
            {
                return Err(MarketSnapshotError::InvalidResolution);
            }
        }
        native_ids.sort_unstable();
        if native_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(MarketSnapshotError::InvalidRankedItems);
        }
        drop(native_ids);
        Ok(Self {
            context: input.context,
            provider_product: input.provider_product,
            provider_channel: input.provider_channel,
            universe: input.universe,
            provider_universe: input.provider_universe,
            sort: input.sort,
            frequency: input.frequency,
            time: input.time,
            completeness: input.completeness,
            reported_total: input.reported_total,
            items: input.items.into_boxed_slice(),
        })
    }

    /// Returns source-qualified raw lineage, local clocks, and canonical revision.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }
    /// Returns the exact source product.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    /// Returns the exact source channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    /// Returns the financial scope from which the source selected this subset.
    pub const fn universe(&self) -> &RankedMarketUniverse {
        &self.universe
    }
    /// Returns the native universe lineage used to keep different source scopes separate.
    pub const fn provider_universe(&self) -> &SourceIdentifier {
        &self.provider_universe
    }
    /// Returns the retained source ranking rule.
    pub const fn sort(&self) -> RankedMarketSort {
        self.sort
    }
    /// Returns the source ranking window.
    pub const fn frequency(&self) -> RankedMarketFrequency {
        self.frequency
    }
    /// Returns the temporal evidence class without upgrading local receipt to a source clock.
    pub const fn time(&self) -> RankedMarketTime {
        self.time
    }
    /// Returns the source snapshot instant only when supplied by the source.
    pub const fn snapshot_at(&self) -> Option<Timestamp> {
        match self.time {
            RankedMarketTime::SourceReported { snapshot_at, .. } => Some(snapshot_at),
            RankedMarketTime::LocalObserved { .. } => None,
        }
    }
    /// Returns the independent provider message/update timestamp.
    pub const fn updated_at(&self) -> Option<Timestamp> {
        match self.time {
            RankedMarketTime::SourceReported { updated_at, .. } => Some(updated_at),
            RankedMarketTime::LocalObserved { .. } => None,
        }
    }
    /// Returns response completeness, always limited to a ranked subset of the universe.
    pub const fn completeness(&self) -> RankedMarketCompleteness {
        self.completeness
    }
    /// Returns the source-reported ranking cardinality, without inventing the universe size.
    pub const fn reported_total(&self) -> Option<u32> {
        self.reported_total
    }
    /// Returns every member in source rank order; consumers must not rerank after identity filtering.
    pub fn items(&self) -> &[RankedMarketItem] {
        &self.items
    }

    /// Rebinds only the durable revision, preserving the complete atomic snapshot.
    pub fn with_revision(&self, revision: RevisionNumber) -> Self {
        Self {
            context: self.context.with_revision(revision),
            ..self.clone()
        }
    }
}

impl TryFrom<RankedMarketSnapshotObservationInput> for RankedMarketSnapshotObservation {
    type Error = MarketSnapshotError;
    fn try_from(value: RankedMarketSnapshotObservationInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

fn normalize_price(
    value: RankedMarketMetric<RankedMarketPrice>,
) -> RankedMarketMetric<RankedMarketPrice> {
    match value {
        RankedMarketMetric::Observed(value) => RankedMarketMetric::Observed(value.normalized()),
        RankedMarketMetric::SourceMissing => RankedMarketMetric::SourceMissing,
    }
}

fn normalize_decimal(value: RankedMarketMetric<Decimal>) -> RankedMarketMetric<Decimal> {
    match value {
        RankedMarketMetric::Observed(value) => RankedMarketMetric::Observed(value.normalize()),
        RankedMarketMetric::SourceMissing => RankedMarketMetric::SourceMissing,
    }
}

fn deserialize_items<'de, D>(deserializer: D) -> Result<Vec<RankedMarketItem>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ItemsVisitor;
    impl<'de> Visitor<'de> for ItemsVisitor {
        type Value = Vec<RankedMarketItem>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_RANKED_MARKET_ITEMS} ordered ranking members"
            )
        }
        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|length| length > MAX_RANKED_MARKET_ITEMS)
            {
                return Err(serde::de::Error::custom(MarketSnapshotError::LimitExceeded));
            }
            let mut items = Vec::new();
            items
                .try_reserve_exact(
                    sequence
                        .size_hint()
                        .unwrap_or(0)
                        .min(MAX_RANKED_MARKET_ITEMS),
                )
                .map_err(|_| serde::de::Error::custom(MarketSnapshotError::AllocationFailed))?;
            while items.len() < MAX_RANKED_MARKET_ITEMS {
                let Some(item) = sequence.next_element()? else {
                    return Ok(items);
                };
                items
                    .try_reserve(1)
                    .map_err(|_| serde::de::Error::custom(MarketSnapshotError::AllocationFailed))?;
                items.push(item);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(MarketSnapshotError::LimitExceeded));
            }
            Ok(items)
        }
    }
    deserializer.deserialize_seq(ItemsVisitor)
}
