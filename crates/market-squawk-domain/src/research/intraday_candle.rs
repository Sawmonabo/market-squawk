//! Exact source candle updates, including minutes whose completion is not established.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    Money, PayloadReference, ProviderChannel, ProviderInstrumentId, ProviderProduct,
    ResearchContext, RevisionNumber, SequenceNumber, SourceIdentifier, Timestamp,
};

/// Hard ceiling for an intraday aggregation interval; daily/session bars use their own contract.
pub const MAX_INTRADAY_CANDLE_INTERVAL_SECONDS: u32 = 86_399;

/// An invariant failure at the canonical candle or ranked-snapshot boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketSnapshotError {
    /// Required instrument or venue identity is absent.
    MissingIdentity,
    /// A set-wide observation incorrectly claims one instrument or venue.
    InvalidSnapshotScope,
    /// Exact financial, provider, receive, or availability clocks disagree.
    InvalidChronology,
    /// Raw lineage is absent or is not a nonzero content digest.
    InvalidLineage,
    /// The candle duration is zero, daily, or cannot be represented at its timestamp.
    InvalidInterval,
    /// A finality assertion lacks an evidenced completed interval.
    InvalidFinality,
    /// Price components have inconsistent currency or currency-resolution states.
    CurrencyMismatch,
    /// OHLC values violate the exact low/high envelope.
    InvalidPriceRange,
    /// A source quantity is negative.
    NegativeVolume,
    /// A reported percentage is outside its quantity's admissible range.
    InvalidPercentage,
    /// Ordered members or retained completeness counts disagree.
    InvalidRankedItems,
    /// Canonical identity resolution is internally inconsistent.
    InvalidResolution,
    /// A bounded collection exceeds its code-owned ceiling.
    LimitExceeded,
    /// A bounded allocation could not be reserved.
    AllocationFailed,
    /// Native text is empty, exceeds its byte ceiling, or contains control characters.
    InvalidNativeText,
}

impl fmt::Display for MarketSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingIdentity => "market snapshot requires exact instrument and venue identity",
            Self::InvalidSnapshotScope => "ranked snapshot must retain its set-wide scope",
            Self::InvalidChronology => "market snapshot clocks are inconsistent",
            Self::InvalidLineage => "market snapshot requires nonzero raw content evidence",
            Self::InvalidInterval => "intraday candle interval is invalid or unrepresentable",
            Self::InvalidFinality => "candle finality does not establish interval completion",
            Self::CurrencyMismatch => "market snapshot price currencies are inconsistent",
            Self::InvalidPriceRange => "candle prices violate the low/high envelope",
            Self::NegativeVolume => "market snapshot volume must not be negative",
            Self::InvalidPercentage => "ranked market percentage is outside its valid range",
            Self::InvalidRankedItems => "ranked snapshot members or completeness are inconsistent",
            Self::InvalidResolution => "ranked market identity evidence is inconsistent",
            Self::LimitExceeded => "ranked snapshot exceeds its member limit",
            Self::AllocationFailed => "market observation allocation could not be reserved",
            Self::InvalidNativeText => "native market text violates its content or byte bound",
        })
    }
}

impl std::error::Error for MarketSnapshotError {}

/// Bounded native market text that preserves meaningful internal and trailing whitespace.
///
/// This is source evidence, not a canonical identifier. In particular, padded option symbols
/// and human-readable descriptions must not pass through whitespace-rejecting identity types.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MarketSourceText(String);

impl MarketSourceText {
    /// Maximum retained UTF-8 bytes for one native market evidence field.
    pub const MAX_BYTES: usize = 1_024;

    /// Retains exact nonblank text without trimming or normalizing whitespace.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, MarketSnapshotError> {
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > Self::MAX_BYTES
            || value.chars().all(char::is_whitespace)
            || value.chars().any(char::is_control)
        {
            return Err(MarketSnapshotError::InvalidNativeText);
        }
        let mut retained = String::new();
        retained
            .try_reserve_exact(value.len())
            .map_err(|_| MarketSnapshotError::AllocationFailed)?;
        retained.push_str(value);
        Ok(Self(retained))
    }

    /// Returns the exact original text, including source padding.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for MarketSourceText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TextVisitor;
        impl serde::de::Visitor<'_> for TextVisitor {
            type Value = MarketSourceText;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bounded nonblank market text without control characters")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                MarketSourceText::try_new(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

/// Explicit unit of a reported market volume; source silence is never interpreted as shares.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSnapshotVolumeUnit {
    /// Individual security shares, where source/reference evidence establishes that unit.
    Shares,
    /// Derivative contracts, where source/reference evidence establishes that unit.
    Contracts,
    /// Units of the instrument's canonical base asset.
    BaseAssetUnits,
    /// The source reports volume without establishing its financial unit.
    SourceUnspecified,
}

/// Nonnegative exact volume with an explicit established or unestablished unit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(try_from = "MarketSnapshotVolumeWire")]
pub struct MarketSnapshotVolume {
    value: Decimal,
    unit: MarketSnapshotVolumeUnit,
}

impl MarketSnapshotVolume {
    /// Retains an exact source volume without changing its unit.
    pub fn try_new(
        value: Decimal,
        unit: MarketSnapshotVolumeUnit,
    ) -> Result<Self, MarketSnapshotError> {
        if value < Decimal::ZERO {
            return Err(MarketSnapshotError::NegativeVolume);
        }
        Ok(Self {
            value: value.normalize(),
            unit,
        })
    }

    /// Returns the exact normalized amount.
    pub const fn value(self) -> Decimal {
        self.value
    }

    /// Returns the retained unit, including source-unspecified evidence.
    pub const fn unit(self) -> MarketSnapshotVolumeUnit {
        self.unit
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketSnapshotVolumeWire {
    value: Decimal,
    unit: MarketSnapshotVolumeUnit,
}

impl TryFrom<MarketSnapshotVolumeWire> for MarketSnapshotVolume {
    type Error = MarketSnapshotError;

    fn try_from(value: MarketSnapshotVolumeWire) -> Result<Self, Self::Error> {
        Self::try_new(value.value, value.unit)
    }
}

/// Meaning of the native candle timestamp, independently of the message update timestamp.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleTimestampBasis {
    /// The source explicitly identifies the inclusive period start.
    PeriodStart,
    /// The source explicitly identifies the exclusive period end.
    PeriodEnd,
    /// The source does not establish which interval boundary the timestamp identifies.
    Unspecified,
}

/// Source-established completion; elapsed wall time alone never upgrades an update to final.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "state", rename_all = "snake_case")]
pub enum IntradayCandleFinality {
    /// No source proof of completion was supplied, even if the nominal interval has elapsed.
    Unconfirmed,
    /// The source proves this update is final for the retained interval end.
    ProviderFinal {
        /// Source-evidenced exclusive period end.
        period_end_exclusive: Timestamp,
        /// Source instant at which this finality assertion became true.
        confirmed_at: Timestamp,
        /// Source field, record, or versioned contract establishing finality.
        evidence: SourceIdentifier,
    },
}

/// Complete input for one immutable observation of a possibly mutable source candle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IntradayCandleObservationInput {
    /// Exact canonical instrument/venue, raw content, local clocks, and canonical revision.
    pub context: ResearchContext,
    /// Native instrument key admitted against the canonical identity.
    pub provider_instrument_id: ProviderInstrumentId,
    /// Exact provider product; it is provenance rather than ordinary product presentation.
    pub provider_product: ProviderProduct,
    /// Exact source channel/service.
    pub provider_channel: ProviderChannel,
    /// Source-authored candle timestamp, retained without inventing its boundary meaning.
    pub candle_at: Timestamp,
    /// Source-authored timestamp of this message/update, distinct from candle time.
    pub updated_at: Timestamp,
    /// Explicit aggregation duration in seconds; one-minute source candles use exactly 60.
    pub interval_seconds: u32,
    /// Established or unknown native timestamp boundary semantics.
    pub timestamp_basis: CandleTimestampBasis,
    /// Source price at the start of the represented observations.
    pub open: Money,
    /// Greatest source price so far in this candle.
    pub high: Money,
    /// Least source price so far in this candle.
    pub low: Money,
    /// Last source price so far, not an assertion of interval completion.
    pub close: Money,
    /// Source-reported volume and its actual unit state.
    pub volume: MarketSnapshotVolume,
    /// Source sequence, whose scope and meaning remain channel-specific.
    pub sequence: Option<SequenceNumber>,
    /// Native source revision when independently supplied; canonical revision is in context.
    pub source_revision: Option<SourceIdentifier>,
    /// Explicit source completion evidence or unconfirmed state.
    pub finality: IntradayCandleFinality,
}

/// Immutable captured revision of a source candle; it is never a completed session bar.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "IntradayCandleObservationInput")]
pub struct IntradayCandleObservation {
    context: ResearchContext,
    provider_instrument_id: ProviderInstrumentId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    candle_at: Timestamp,
    updated_at: Timestamp,
    interval_seconds: u32,
    timestamp_basis: CandleTimestampBasis,
    open: Money,
    high: Money,
    low: Money,
    close: Money,
    volume: MarketSnapshotVolume,
    sequence: Option<SequenceNumber>,
    source_revision: Option<SourceIdentifier>,
    finality: IntradayCandleFinality,
}

impl IntradayCandleObservation {
    /// Validates exact clocks, identity, interval, OHLC, raw evidence, and optional finality.
    ///
    /// Signed prices remain representable for instruments that admit them. Instrument-specific
    /// price rules and canonical identity/currency authority belong to the producing resolver.
    pub fn try_new(input: IntradayCandleObservationInput) -> Result<Self, MarketSnapshotError> {
        if input.context.provenance().instrument_id().is_none()
            || input.context.provenance().venue_id().is_none()
        {
            return Err(MarketSnapshotError::MissingIdentity);
        }
        validate_snapshot_context(&input.context, input.updated_at)?;
        if input.context.time().effective().exact_timestamp() != Some(input.candle_at) {
            return Err(MarketSnapshotError::InvalidChronology);
        }
        if input.interval_seconds == 0
            || input.interval_seconds > MAX_INTRADAY_CANDLE_INTERVAL_SECONDS
        {
            return Err(MarketSnapshotError::InvalidInterval);
        }
        let duration = i64::from(input.interval_seconds) * 1_000_000_000;
        let known_end = match input.timestamp_basis {
            CandleTimestampBasis::PeriodStart => {
                if input.updated_at < input.candle_at {
                    return Err(MarketSnapshotError::InvalidChronology);
                }
                Some(
                    input
                        .candle_at
                        .checked_add_nanos(duration)
                        .map_err(|_| MarketSnapshotError::InvalidInterval)?,
                )
            }
            CandleTimestampBasis::PeriodEnd => {
                let start = input
                    .candle_at
                    .checked_sub_nanos(duration)
                    .map_err(|_| MarketSnapshotError::InvalidInterval)?;
                if input.updated_at < start {
                    return Err(MarketSnapshotError::InvalidChronology);
                }
                Some(input.candle_at)
            }
            CandleTimestampBasis::Unspecified => {
                // An unknown anchor may be an exclusive end, but cannot identify an interval
                // lying wholly after this source update. This does not choose a boundary.
                let earliest_start = input
                    .candle_at
                    .checked_sub_nanos(duration)
                    .map_err(|_| MarketSnapshotError::InvalidInterval)?;
                if input.updated_at < earliest_start {
                    return Err(MarketSnapshotError::InvalidChronology);
                }
                None
            }
        };
        if let IntradayCandleFinality::ProviderFinal {
            period_end_exclusive,
            confirmed_at,
            ..
        } = &input.finality
        {
            let start = period_end_exclusive
                .checked_sub_nanos(duration)
                .map_err(|_| MarketSnapshotError::InvalidFinality)?;
            if *confirmed_at < *period_end_exclusive
                || *confirmed_at > input.updated_at
                || known_end.is_some_and(|end| end != *period_end_exclusive)
                || input.candle_at < start
                || input.candle_at > *period_end_exclusive
            {
                return Err(MarketSnapshotError::InvalidFinality);
            }
        }
        if [input.high, input.low, input.close]
            .iter()
            .any(|price| price.currency() != input.open.currency())
        {
            return Err(MarketSnapshotError::CurrencyMismatch);
        }
        if input.low.amount() > input.high.amount()
            || [input.open, input.close].iter().any(|price| {
                price.amount() < input.low.amount() || price.amount() > input.high.amount()
            })
        {
            return Err(MarketSnapshotError::InvalidPriceRange);
        }
        Ok(Self {
            context: input.context,
            provider_instrument_id: input.provider_instrument_id,
            provider_product: input.provider_product,
            provider_channel: input.provider_channel,
            candle_at: input.candle_at,
            updated_at: input.updated_at,
            interval_seconds: input.interval_seconds,
            timestamp_basis: input.timestamp_basis,
            open: input.open,
            high: input.high,
            low: input.low,
            close: input.close,
            volume: input.volume,
            sequence: input.sequence,
            source_revision: input.source_revision,
            finality: input.finality,
        })
    }

    /// Returns canonical identity, exact raw evidence, local knowledge clocks, and revision.
    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }
    /// Returns the retained source instrument key.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
    /// Returns the source product identity.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    /// Returns the exact source channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    /// Returns native candle time without rebasing it to an assumed minute boundary.
    pub const fn candle_at(&self) -> Timestamp {
        self.candle_at
    }
    /// Returns the provider's timestamp for this update.
    pub const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
    /// Returns the explicit aggregation duration.
    pub const fn interval_seconds(&self) -> u32 {
        self.interval_seconds
    }
    /// Returns the established or unspecified timestamp boundary semantics.
    pub const fn timestamp_basis(&self) -> CandleTimestampBasis {
        self.timestamp_basis
    }
    /// Returns exact open price.
    pub const fn open(&self) -> Money {
        self.open
    }
    /// Returns exact high price so far.
    pub const fn high(&self) -> Money {
        self.high
    }
    /// Returns exact low price so far.
    pub const fn low(&self) -> Money {
        self.low
    }
    /// Returns exact close/last price so far without asserting finality.
    pub const fn close(&self) -> Money {
        self.close
    }
    /// Returns exact volume and its unit state.
    pub const fn volume(&self) -> MarketSnapshotVolume {
        self.volume
    }
    /// Returns the source sequence without treating it as canonical revision authority.
    pub const fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence
    }
    /// Returns the source-authored revision when supplied.
    pub const fn source_revision(&self) -> Option<&SourceIdentifier> {
        self.source_revision.as_ref()
    }
    /// Returns source finality evidence.
    pub const fn finality(&self) -> &IntradayCandleFinality {
        &self.finality
    }

    /// Rebinds only the canonical durable revision, preserving captured payload and clocks.
    pub fn with_revision(&self, revision: RevisionNumber) -> Self {
        Self {
            context: self.context.with_revision(revision),
            ..self.clone()
        }
    }
}

impl TryFrom<IntradayCandleObservationInput> for IntradayCandleObservation {
    type Error = MarketSnapshotError;
    fn try_from(value: IntradayCandleObservationInput) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

pub(super) fn validate_snapshot_context(
    context: &ResearchContext,
    source_timestamp: Timestamp,
) -> Result<(), MarketSnapshotError> {
    let provenance = context.provenance();
    if provenance.source_timestamp() != Some(source_timestamp)
        || source_timestamp > provenance.received_at()
        || provenance
            .availability()
            .conservative_available_at()
            .is_none_or(|available| available < source_timestamp)
    {
        return Err(MarketSnapshotError::InvalidChronology);
    }
    validate_market_raw_context(context)
}

pub(super) fn validate_market_raw_context(
    context: &ResearchContext,
) -> Result<(), MarketSnapshotError> {
    if context
        .provenance()
        .availability()
        .conservative_available_at()
        .is_none()
    {
        return Err(MarketSnapshotError::InvalidChronology);
    }
    if !matches!(context.provenance().payload_reference(), PayloadReference::ContentHash(hash)
        if hash.digest() != [0; 32])
    {
        return Err(MarketSnapshotError::InvalidLineage);
    }
    Ok(())
}
