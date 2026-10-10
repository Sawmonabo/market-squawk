//! Authority-free bounded feature snapshot data-transfer objects.

use std::num::NonZeroU32;

use market_squawk_analytics::{
    FeatureOutputType, FeatureUnit, FeatureValidity, RequiredLiveFeature,
};
use market_squawk_domain::{
    ConnectionGeneration, EvidenceDigest, InstrumentId, ProviderChannel, ProviderProduct, SourceId,
    Timestamp, VenueId,
};
use serde::{Serialize, Serializer};

use super::SnapshotDimension;

/// Closed serializable representation of a live feature scalar.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum LiveFeatureScalarSnapshot {
    PriceTicks(i64),
    HalfTickPrice(i128),
    QuantityLots(i64),
    BasisPoints(i32),
    SignedInteger(i128),
    UnsignedInteger(u128),
    ExactRatio { numerator: i128, denominator: u128 },
    StatisticalBits(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LiveFeatureValiditySnapshot {
    Ready,
    WarmingUp,
    Unavailable,
    Overflow,
    TimestampRegression,
    Stale,
}

impl From<FeatureValidity> for LiveFeatureValiditySnapshot {
    fn from(value: FeatureValidity) -> Self {
        match value {
            FeatureValidity::Ready => Self::Ready,
            FeatureValidity::WarmingUp => Self::WarmingUp,
            FeatureValidity::Unavailable => Self::Unavailable,
            FeatureValidity::Overflow => Self::Overflow,
            FeatureValidity::TimestampRegression => Self::TimestampRegression,
            FeatureValidity::Stale => Self::Stale,
        }
    }
}

impl From<LiveFeatureValiditySnapshot> for FeatureValidity {
    fn from(value: LiveFeatureValiditySnapshot) -> Self {
        match value {
            LiveFeatureValiditySnapshot::Ready => Self::Ready,
            LiveFeatureValiditySnapshot::WarmingUp => Self::WarmingUp,
            LiveFeatureValiditySnapshot::Unavailable => Self::Unavailable,
            LiveFeatureValiditySnapshot::Overflow => Self::Overflow,
            LiveFeatureValiditySnapshot::TimestampRegression => Self::TimestampRegression,
            LiveFeatureValiditySnapshot::Stale => Self::Stale,
        }
    }
}

impl LiveFeatureValiditySnapshot {
    pub(crate) const fn digest_tag(self) -> u8 {
        match self {
            Self::Ready => 1,
            Self::WarmingUp => 2,
            Self::Unavailable => 3,
            Self::Overflow => 4,
            Self::TimestampRegression => 5,
            Self::Stale => 6,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveFeatureOutputTypeSnapshot(FeatureOutputType);

impl From<FeatureOutputType> for LiveFeatureOutputTypeSnapshot {
    fn from(value: FeatureOutputType) -> Self {
        Self(value)
    }
}

impl From<LiveFeatureOutputTypeSnapshot> for FeatureOutputType {
    fn from(value: LiveFeatureOutputTypeSnapshot) -> Self {
        value.0
    }
}

impl LiveFeatureOutputTypeSnapshot {
    pub(crate) const fn digest_tag(self) -> u8 {
        match self.0 {
            FeatureOutputType::PriceTicks => 1,
            FeatureOutputType::HalfTickPrice => 2,
            FeatureOutputType::QuantityLots => 3,
            FeatureOutputType::BasisPoints => 4,
            FeatureOutputType::SignedInteger => 5,
            FeatureOutputType::UnsignedInteger => 6,
            FeatureOutputType::ExactRatio => 7,
            FeatureOutputType::StatisticalF64 => 8,
            FeatureOutputType::Decimal => 9,
            FeatureOutputType::Money => 10,
        }
    }

    const fn serialized_name(self) -> &'static str {
        match self.0 {
            FeatureOutputType::PriceTicks => "price_ticks",
            FeatureOutputType::HalfTickPrice => "half_tick_price",
            FeatureOutputType::QuantityLots => "quantity_lots",
            FeatureOutputType::BasisPoints => "basis_points",
            FeatureOutputType::SignedInteger => "signed_integer",
            FeatureOutputType::UnsignedInteger => "unsigned_integer",
            FeatureOutputType::ExactRatio => "exact_ratio",
            FeatureOutputType::StatisticalF64 => "statistical_f64",
            FeatureOutputType::Decimal => "decimal",
            FeatureOutputType::Money => "money",
        }
    }
}

impl Serialize for LiveFeatureOutputTypeSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.serialized_name())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveFeatureUnitSnapshot(FeatureUnit);

impl From<FeatureUnit> for LiveFeatureUnitSnapshot {
    fn from(value: FeatureUnit) -> Self {
        Self(value)
    }
}

impl From<LiveFeatureUnitSnapshot> for FeatureUnit {
    fn from(value: LiveFeatureUnitSnapshot) -> Self {
        value.0
    }
}

impl LiveFeatureUnitSnapshot {
    pub(crate) const fn digest_tag(self) -> u8 {
        match self.0 {
            FeatureUnit::PriceTicks => 1,
            FeatureUnit::QuantityLots => 2,
            FeatureUnit::BasisPoints => 3,
            FeatureUnit::Ratio => 4,
            FeatureUnit::Return => 5,
            FeatureUnit::Volatility => 6,
            FeatureUnit::LotsPerSecond => 7,
            FeatureUnit::Count => 8,
            FeatureUnit::Nanoseconds => 9,
            FeatureUnit::Unitless => 10,
            FeatureUnit::Rate => 11,
            FeatureUnit::CurrencyAmount => 12,
        }
    }

    const fn serialized_name(self) -> &'static str {
        match self.0 {
            FeatureUnit::PriceTicks => "price_ticks",
            FeatureUnit::QuantityLots => "quantity_lots",
            FeatureUnit::BasisPoints => "basis_points",
            FeatureUnit::Ratio => "ratio",
            FeatureUnit::Return => "return",
            FeatureUnit::Volatility => "volatility",
            FeatureUnit::LotsPerSecond => "lots_per_second",
            FeatureUnit::Count => "count",
            FeatureUnit::Nanoseconds => "nanoseconds",
            FeatureUnit::Unitless => "unitless",
            FeatureUnit::Rate => "rate",
            FeatureUnit::CurrencyAmount => "currency_amount",
        }
    }
}

impl Serialize for LiveFeatureUnitSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.serialized_name())
    }
}

/// One timestamped required feature value with stale payload exclusion preserved.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeatureValueSnapshot {
    pub(crate) name: String,
    pub(crate) version: NonZeroU32,
    pub(crate) semantic_digest: [u8; 32],
    pub(crate) implementation_digest: [u8; 32],
    pub(crate) output_type: LiveFeatureOutputTypeSnapshot,
    pub(crate) unit: LiveFeatureUnitSnapshot,
    pub(crate) observed_at: Timestamp,
    pub(crate) validity: LiveFeatureValiditySnapshot,
    pub(crate) scalar: Option<LiveFeatureScalarSnapshot>,
}

impl LiveFeatureValueSnapshot {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable value binds identity, metadata, time, validity, and scalar evidence"
    )]
    pub(crate) fn new(
        name: String,
        version: NonZeroU32,
        semantic_digest: [u8; 32],
        implementation_digest: [u8; 32],
        output_type: FeatureOutputType,
        unit: FeatureUnit,
        observed_at: Timestamp,
        validity: FeatureValidity,
        scalar: Option<LiveFeatureScalarSnapshot>,
    ) -> Self {
        Self {
            name,
            version,
            semantic_digest,
            implementation_digest,
            output_type: output_type.into(),
            unit: unit.into(),
            observed_at,
            validity: validity.into(),
            scalar,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }

    /// Returns the SHA-256 commitment to this feature version's complete semantics.
    pub const fn semantic_digest(&self) -> [u8; 32] {
        self.semantic_digest
    }

    /// Returns the SHA-256 identity of the code-owned feature implementation.
    pub const fn implementation_digest(&self) -> [u8; 32] {
        self.implementation_digest
    }

    /// Returns the closed scalar representation required to interpret this value.
    pub fn output_type(&self) -> FeatureOutputType {
        self.output_type.into()
    }

    /// Returns the financial or statistical unit required to interpret this value.
    pub fn unit(&self) -> FeatureUnit {
        self.unit.into()
    }

    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    pub fn validity(&self) -> FeatureValidity {
        self.validity.into()
    }

    pub const fn scalar(&self) -> Option<&LiveFeatureScalarSnapshot> {
        self.scalar.as_ref()
    }

    pub(crate) const fn output_type_digest_tag(&self) -> u8 {
        self.output_type.digest_tag()
    }

    pub(crate) const fn unit_digest_tag(&self) -> u8 {
        self.unit.digest_tag()
    }

    pub(crate) const fn validity_digest_tag(&self) -> u8 {
        self.validity.digest_tag()
    }
}

/// Complete required feature set for one exact provider stream generation.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeatureSetSnapshot {
    pub(crate) source: SourceId,
    pub(crate) venue: VenueId,
    pub(crate) instrument: InstrumentId,
    pub(crate) provider_product: ProviderProduct,
    pub(crate) provider_channel: ProviderChannel,
    pub(crate) connection_generation: ConnectionGeneration,
    pub(crate) available_at: Timestamp,
    pub(crate) content_digest: EvidenceDigest,
    pub(crate) values: Box<[LiveFeatureValueSnapshot]>,
    pub(crate) value_dimension: SnapshotDimension,
}

impl LiveFeatureSetSnapshot {
    pub const fn source(&self) -> &SourceId {
        &self.source
    }

    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns when this exact immutable set was admitted for local publication.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the deterministic digest of the complete identity, timing, metadata, and values.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    pub fn values(&self) -> &[LiveFeatureValueSnapshot] {
        &self.values
    }

    pub fn feature(&self, feature: RequiredLiveFeature) -> Option<&LiveFeatureValueSnapshot> {
        self.values
            .iter()
            .find(|value| value.name == feature.name())
    }

    pub const fn value_dimension(&self) -> &SnapshotDimension {
        &self.value_dimension
    }
}

/// Independently bounded feature dimension for one route snapshot.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeatureSnapshot {
    pub(crate) sets: Box<[LiveFeatureSetSnapshot]>,
    pub(crate) set_dimension: SnapshotDimension,
    pub(crate) retained_bytes: u64,
}

impl LiveFeatureSnapshot {
    #[cfg(test)]
    pub(crate) fn empty(limit: usize) -> Result<Self, super::SnapshotBuildError> {
        Ok(Self {
            sets: Box::default(),
            set_dimension: SnapshotDimension::from_counts(0, 0, limit)?,
            retained_bytes: std::mem::size_of::<Self>() as u64,
        })
    }

    pub fn sets(&self) -> &[LiveFeatureSetSnapshot] {
        &self.sets
    }

    pub const fn set_dimension(&self) -> &SnapshotDimension {
        &self.set_dimension
    }

    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}
