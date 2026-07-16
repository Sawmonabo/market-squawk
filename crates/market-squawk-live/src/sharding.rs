//! Versioned, architecture-independent routing for instrument-owned shard state.

use std::fmt;
use std::num::NonZeroU16;

use market_squawk_domain::{InstrumentId, VenueId};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const V1_DOMAIN: &[u8; 9] = b"MSQKSHARD";
const V1_TAG: u8 = 1;
const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stable algorithm revision persisted with shard-owned diagnostics and snapshots.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRoutingVersion {
    /// Domain-separated FNV-1a over the frozen V1 byte encoding.
    V1,
}

/// Nonzero shard cardinality embedded in every routed shard identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ShardCount(NonZeroU16);

impl ShardCount {
    /// Constructs a nonzero shard count.
    ///
    /// # Errors
    ///
    /// Returns [`ShardRoutingError::ZeroShardCount`] when `count` is zero.
    pub fn new(count: u16) -> Result<Self, ShardRoutingError> {
        NonZeroU16::new(count)
            .map(Self)
            .ok_or(ShardRoutingError::ZeroShardCount)
    }

    /// Returns the configured shard cardinality.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl fmt::Display for ShardCount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// Zero-based shard index bound to the cardinality under which it was routed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShardId {
    index: u16,
    count: ShardCount,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardIdWire {
    index: u16,
    count: ShardCount,
}

impl<'de> Deserialize<'de> for ShardId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ShardIdWire::deserialize(deserializer)?;
        Self::from_count(wire.index, wire.count).map_err(serde::de::Error::custom)
    }
}

impl ShardId {
    /// Constructs a checked shard identity from primitive wire values.
    ///
    /// # Errors
    ///
    /// Rejects a zero count and any index greater than or equal to that count.
    pub fn new(index: u16, count: u16) -> Result<Self, ShardRoutingError> {
        Self::from_count(index, ShardCount::new(count)?)
    }

    fn from_count(index: u16, count: ShardCount) -> Result<Self, ShardRoutingError> {
        if index >= count.get() {
            return Err(ShardRoutingError::IndexOutOfRange {
                index,
                count: count.get(),
            });
        }
        Ok(Self { index, count })
    }

    /// Returns the zero-based shard index.
    pub const fn index(self) -> u16 {
        self.index
    }

    /// Returns the shard cardinality used to produce this identity.
    pub const fn count(self) -> ShardCount {
        self.count
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.index, self.count)
    }
}

/// Stable routing key independent of provider product/channel identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShardKey {
    venue: VenueId,
    instrument: InstrumentId,
}

impl ShardKey {
    /// Constructs the exact venue/instrument ownership key.
    pub const fn new(venue: VenueId, instrument: InstrumentId) -> Self {
        Self { venue, instrument }
    }

    /// Returns the venue namespace participating in the V1 preimage.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the stable instrument UUID participating in the V1 preimage.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
}

/// Immutable deterministic router for one exact version and shard count.
#[derive(Clone, Debug)]
pub struct ShardRouter {
    version: ShardRoutingVersion,
    count: ShardCount,
}

impl ShardRouter {
    /// Constructs the V1 router.
    ///
    /// # Errors
    ///
    /// Returns [`ShardRoutingError::ZeroShardCount`] when `count` is zero.
    pub fn v1(count: u16) -> Result<Self, ShardRoutingError> {
        Ok(Self {
            version: ShardRoutingVersion::V1,
            count: ShardCount::new(count)?,
        })
    }

    /// Routes a key using the router's frozen version and count.
    pub fn route(&self, key: &ShardKey) -> ShardId {
        let hash = match self.version {
            ShardRoutingVersion::V1 => v1_hash(key),
        };
        let index = (hash % u64::from(self.count.get())) as u16;
        ShardId {
            index,
            count: self.count,
        }
    }

    /// Returns the frozen algorithm revision.
    pub const fn version(&self) -> ShardRoutingVersion {
        self.version
    }

    /// Returns the configured nonzero shard cardinality.
    pub const fn count(&self) -> ShardCount {
        self.count
    }
}

fn v1_hash(key: &ShardKey) -> u64 {
    let venue = key.venue.as_str().as_bytes();
    // VenueId's invariant is substantially narrower than u16::MAX. Keeping the cast here makes
    // the exact two-byte V1 wire representation visible rather than depending on usize width.
    let venue_length = venue.len() as u16;
    let mut hash = FNV1A_OFFSET_BASIS;
    for byte in V1_DOMAIN
        .iter()
        .copied()
        .chain([V1_TAG])
        .chain(venue_length.to_be_bytes())
        .chain(venue.iter().copied())
        .chain(key.instrument.as_uuid().as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV1A_PRIME);
    }
    hash
}

/// Invalid shard cardinality or identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ShardRoutingError {
    /// A router cannot own zero shards.
    #[error("shard count must be nonzero")]
    ZeroShardCount,
    /// A shard index must lie in `[0, count)`.
    #[error("shard index {index} is outside shard count {count}")]
    IndexOutOfRange { index: u16, count: u16 },
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use market_squawk_domain::{InstrumentId, VenueId};

    use super::{ShardId, ShardKey, ShardRouter, ShardRoutingVersion, v1_hash};

    #[test]
    fn v1_matches_the_frozen_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
        let key = ShardKey::new(
            VenueId::try_from("coinbase")?,
            InstrumentId::from_str("018f0000-0000-7000-8000-000000000001")?,
        );
        let router = ShardRouter::v1(16)?;

        assert_eq!(v1_hash(&key), 0x28ed_ee9c_b185_2659);
        assert_eq!(router.route(&key), ShardId::new(9, 16)?);
        assert_eq!(router.version(), ShardRoutingVersion::V1);
        Ok(())
    }

    #[test]
    fn zero_count_is_rejected() {
        assert!(ShardRouter::v1(0).is_err());
        assert!(ShardId::new(0, 0).is_err());
    }

    #[test]
    fn shard_id_rejects_an_index_outside_its_count() {
        assert!(ShardId::new(16, 16).is_err());
        assert!(ShardId::new(u16::MAX, 1).is_err());
    }
}
