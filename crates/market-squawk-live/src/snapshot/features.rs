//! Authority-free bounded feature snapshot data-transfer objects.

use std::num::NonZeroU32;

use market_squawk_analytics::{FeatureValidity, RequiredLiveFeature};
use market_squawk_domain::{
    ConnectionGeneration, InstrumentId, ProviderChannel, ProviderProduct, SourceId, Timestamp,
    VenueId,
};
use serde::Serialize;

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

/// One timestamped required feature value with stale payload exclusion preserved.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeatureValueSnapshot {
    pub(crate) name: String,
    pub(crate) version: NonZeroU32,
    pub(crate) observed_at: Timestamp,
    pub(crate) validity: LiveFeatureValiditySnapshot,
    pub(crate) scalar: Option<LiveFeatureScalarSnapshot>,
}

impl LiveFeatureValueSnapshot {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn version(&self) -> NonZeroU32 {
        self.version
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
