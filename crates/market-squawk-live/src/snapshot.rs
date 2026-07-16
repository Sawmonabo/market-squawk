//! Authority-free, bounded immutable live-state snapshot contracts.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use market_squawk_domain::{
    ConnectionGeneration, InstrumentId, PriceTicks, ProviderChannel, ProviderProduct, QuantityLots,
    SequenceNumber, SourceId, Timestamp, TradingStatus, VenueId,
};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

use crate::{ShardCount, ShardId, ShardKey, ShardRoutingVersion};

#[path = "snapshot/store.rs"]
mod store;

pub(crate) use store::{
    SnapshotPlaneBundle, SnapshotPublishError, SnapshotPublisher, create_snapshot_plane,
};

/// Hard bound aligned with one shard's preallocated route table.
pub(crate) const MAX_SNAPSHOT_ROUTES: usize = 64;
/// Hard bound aligned with Task 7 stream/status capacity per route.
pub(crate) const MAX_SNAPSHOT_STREAMS_PER_ROUTE: usize = 64;
/// Hard bound aligned with Task 7 stream/status capacity per route.
pub(crate) const MAX_SNAPSHOT_STATUSES_PER_ROUTE: usize = 64;
/// Hard bound aligned with the live book and decoder depth contract.
pub(crate) const MAX_SNAPSHOT_LEVELS_PER_SIDE: u32 = 10_000;
/// Hard upper bound for one immutable shard snapshot.
pub(crate) const MAX_SNAPSHOT_RETAINED_BYTES: u32 = 64 * 1024 * 1024;

/// Whether one independently bounded snapshot dimension is complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCompleteness {
    /// Every available item is represented.
    Complete,
    /// The configured output limit omitted one or more available items.
    Truncated,
    /// No truthful value was available for this dimension.
    Unavailable,
}

/// Counts and configured output policy for one independently bounded dimension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDimension {
    completeness: SnapshotCompleteness,
    available: u32,
    returned: u32,
    configured_limit: u32,
}

impl SnapshotDimension {
    pub(crate) fn from_counts(
        available: usize,
        returned: usize,
        configured_limit: usize,
    ) -> Result<Self, SnapshotBuildError> {
        let available = u32::try_from(available).map_err(|_| SnapshotBuildError::CountOverflow)?;
        let returned = u32::try_from(returned).map_err(|_| SnapshotBuildError::CountOverflow)?;
        let configured_limit =
            u32::try_from(configured_limit).map_err(|_| SnapshotBuildError::CountOverflow)?;
        if returned > available || returned > configured_limit {
            return Err(SnapshotBuildError::DimensionInvariant);
        }
        let completeness = if available == 0 && returned == 0 {
            SnapshotCompleteness::Complete
        } else if returned == 0 {
            SnapshotCompleteness::Unavailable
        } else if returned == available {
            SnapshotCompleteness::Complete
        } else {
            SnapshotCompleteness::Truncated
        };
        Ok(Self {
            completeness,
            available,
            returned,
            configured_limit,
        })
    }

    /// Returns whether this dimension is complete, truncated, or unavailable.
    pub const fn completeness(&self) -> SnapshotCompleteness {
        self.completeness
    }

    /// Returns the number of available values before output bounding.
    pub const fn available(&self) -> u32 {
        self.available
    }

    /// Returns the number of values retained in this DTO.
    pub const fn returned(&self) -> u32 {
        self.returned
    }

    /// Returns the configured maximum for this dimension.
    pub const fn configured_limit(&self) -> u32 {
        self.configured_limit
    }
}

/// Actor lifecycle at one exact shard publication revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardLifecycleSnapshot {
    Starting,
    Ready,
    Degraded,
    Stopping,
    Stopped,
}

/// Synchronization phase of one source/product/channel stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamPhaseSnapshot {
    Disconnected,
    AwaitingSnapshot,
    Synchronizing,
    Healthy,
    Quarantined,
}

/// One scaled integer price level in an immutable diagnostic snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BookLevelSnapshot {
    price: PriceTicks,
    quantity: QuantityLots,
}

impl BookLevelSnapshot {
    pub(crate) const fn new(price: PriceTicks, quantity: QuantityLots) -> Self {
        Self { price, quantity }
    }

    pub const fn price(self) -> PriceTicks {
        self.price
    }

    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// Cross-channel trading status retained separately from stream state.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSnapshot {
    pub(crate) source: SourceId,
    pub(crate) venue: VenueId,
    pub(crate) instrument: InstrumentId,
    pub(crate) connection_generation: ConnectionGeneration,
    pub(crate) trading_status: TradingStatus,
    pub(crate) status_revision: u64,
}

impl StatusSnapshot {
    pub const fn source(&self) -> &SourceId {
        &self.source
    }
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }
    pub const fn trading_status(&self) -> TradingStatus {
        self.trading_status
    }
    pub const fn status_revision(&self) -> u64 {
        self.status_revision
    }
}

/// Complete bounded view of one independently synchronized provider stream.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamSnapshot {
    pub(crate) source: SourceId,
    pub(crate) venue: VenueId,
    pub(crate) instrument: InstrumentId,
    pub(crate) provider_product: ProviderProduct,
    pub(crate) provider_channel: ProviderChannel,
    pub(crate) connection_generation: ConnectionGeneration,
    pub(crate) phase: StreamPhaseSnapshot,
    pub(crate) state_revision: u64,
    pub(crate) last_sequence: Option<SequenceNumber>,
    pub(crate) snapshot_origin_revision: Option<u64>,
    pub(crate) snapshot_initialized: bool,
    pub(crate) generation_current: bool,
    pub(crate) health_epoch: u64,
    pub(crate) source_valid_until: Timestamp,
    pub(crate) source_timestamp: Option<Timestamp>,
    pub(crate) received_at: Timestamp,
    pub(crate) evaluated_at: Timestamp,
    pub(crate) trading_status: Option<TradingStatus>,
    pub(crate) trading_status_revision: Option<u64>,
    pub(crate) configured_depth: u32,
    pub(crate) state_bid_depth: usize,
    pub(crate) state_ask_depth: usize,
    pub(crate) bids: Box<[BookLevelSnapshot]>,
    pub(crate) asks: Box<[BookLevelSnapshot]>,
    pub(crate) bid_dimension: SnapshotDimension,
    pub(crate) ask_dimension: SnapshotDimension,
}

impl StreamSnapshot {
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
    pub const fn phase(&self) -> StreamPhaseSnapshot {
        self.phase
    }
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }
    pub const fn last_sequence(&self) -> Option<SequenceNumber> {
        self.last_sequence
    }
    pub const fn snapshot_origin_revision(&self) -> Option<u64> {
        self.snapshot_origin_revision
    }
    pub const fn snapshot_initialized(&self) -> bool {
        self.snapshot_initialized
    }
    pub const fn generation_current(&self) -> bool {
        self.generation_current
    }
    pub const fn health_epoch(&self) -> u64 {
        self.health_epoch
    }
    pub const fn source_valid_until(&self) -> Timestamp {
        self.source_valid_until
    }
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    pub const fn trading_status(&self) -> Option<TradingStatus> {
        self.trading_status
    }
    pub const fn trading_status_revision(&self) -> Option<u64> {
        self.trading_status_revision
    }
    pub const fn configured_depth(&self) -> u32 {
        self.configured_depth
    }
    pub const fn state_bid_depth(&self) -> usize {
        self.state_bid_depth
    }
    pub const fn state_ask_depth(&self) -> usize {
        self.state_ask_depth
    }
    pub fn bids(&self) -> &[BookLevelSnapshot] {
        &self.bids
    }
    pub fn asks(&self) -> &[BookLevelSnapshot] {
        &self.asks
    }
    pub const fn bid_dimension(&self) -> &SnapshotDimension {
        &self.bid_dimension
    }
    pub const fn ask_dimension(&self) -> &SnapshotDimension {
        &self.ask_dimension
    }
}

/// Bounded state for one venue/instrument owner.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSnapshot {
    pub(crate) route: ShardKey,
    pub(crate) streams: Box<[StreamSnapshot]>,
    pub(crate) statuses: Box<[StatusSnapshot]>,
    pub(crate) stream_dimension: SnapshotDimension,
    pub(crate) status_dimension: SnapshotDimension,
}

impl RouteSnapshot {
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }
    pub fn streams(&self) -> &[StreamSnapshot] {
        &self.streams
    }
    pub fn statuses(&self) -> &[StatusSnapshot] {
        &self.statuses
    }
    pub const fn stream_dimension(&self) -> &SnapshotDimension {
        &self.stream_dimension
    }
    pub const fn status_dimension(&self) -> &SnapshotDimension {
        &self.status_dimension
    }
}

/// Complete immutable publication from one single-writer shard actor.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSnapshot {
    pub(crate) routing_version: ShardRoutingVersion,
    pub(crate) shard_count: ShardCount,
    pub(crate) runtime_incarnation: NonZeroU64,
    pub(crate) shard_id: ShardId,
    pub(crate) snapshot_revision: NonZeroU64,
    pub(crate) health_revision: u64,
    pub(crate) lifecycle: ShardLifecycleSnapshot,
    pub(crate) evaluated_at: Timestamp,
    pub(crate) published_at: Timestamp,
    pub(crate) routes: Box<[RouteSnapshot]>,
    pub(crate) route_dimension: SnapshotDimension,
    pub(crate) retained_bytes: u64,
}

impl ShardSnapshot {
    pub const fn routing_version(&self) -> ShardRoutingVersion {
        self.routing_version
    }
    pub const fn shard_count(&self) -> ShardCount {
        self.shard_count
    }
    pub const fn runtime_incarnation(&self) -> NonZeroU64 {
        self.runtime_incarnation
    }
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    pub const fn snapshot_revision(&self) -> NonZeroU64 {
        self.snapshot_revision
    }
    pub const fn health_revision(&self) -> u64 {
        self.health_revision
    }
    pub const fn lifecycle(&self) -> ShardLifecycleSnapshot {
        self.lifecycle
    }
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
    pub fn routes(&self) -> &[RouteSnapshot] {
        &self.routes
    }
    pub const fn route_dimension(&self) -> &SnapshotDimension {
        &self.route_dimension
    }
    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

/// Caller-selected snapshot bounds validated before actor construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotLimits {
    maximum_routes: NonZeroUsize,
    maximum_streams_per_route: NonZeroUsize,
    maximum_statuses_per_route: NonZeroUsize,
    maximum_levels_per_side: NonZeroU32,
    maximum_retained_bytes: NonZeroU32,
}

impl SnapshotLimits {
    /// Constructs locally bounded snapshot dimensions.
    pub fn try_new(
        maximum_routes: usize,
        maximum_streams_per_route: usize,
        maximum_statuses_per_route: usize,
        maximum_levels_per_side: u32,
        maximum_retained_bytes: u32,
    ) -> Result<Self, SnapshotLimitsError> {
        let minimum_retained_bytes = u32::try_from(std::mem::size_of::<ShardSnapshot>())
            .map_err(|_| SnapshotLimitsError::TruthfulBaseUnrepresentable)?;
        if maximum_retained_bytes < minimum_retained_bytes {
            return Err(SnapshotLimitsError::BelowTruthfulBase {
                value: maximum_retained_bytes,
                minimum: minimum_retained_bytes,
            });
        }
        Ok(Self {
            maximum_routes: checked_usize("maximum_routes", maximum_routes, MAX_SNAPSHOT_ROUTES)?,
            maximum_streams_per_route: checked_usize(
                "maximum_streams_per_route",
                maximum_streams_per_route,
                MAX_SNAPSHOT_STREAMS_PER_ROUTE,
            )?,
            maximum_statuses_per_route: checked_usize(
                "maximum_statuses_per_route",
                maximum_statuses_per_route,
                MAX_SNAPSHOT_STATUSES_PER_ROUTE,
            )?,
            maximum_levels_per_side: checked_u32(
                "maximum_levels_per_side",
                maximum_levels_per_side,
                MAX_SNAPSHOT_LEVELS_PER_SIDE,
            )?,
            maximum_retained_bytes: checked_u32(
                "maximum_retained_bytes",
                maximum_retained_bytes,
                MAX_SNAPSHOT_RETAINED_BYTES,
            )?,
        })
    }

    pub const fn maximum_routes(self) -> NonZeroUsize {
        self.maximum_routes
    }
    pub const fn maximum_streams_per_route(self) -> NonZeroUsize {
        self.maximum_streams_per_route
    }
    pub const fn maximum_statuses_per_route(self) -> NonZeroUsize {
        self.maximum_statuses_per_route
    }
    pub const fn maximum_levels_per_side(self) -> NonZeroU32 {
        self.maximum_levels_per_side
    }
    pub const fn maximum_retained_bytes(self) -> NonZeroU32 {
        self.maximum_retained_bytes
    }
}

/// Non-cloneable retained-reader lease for one immutable shard publication.
#[derive(Debug)]
pub struct LiveSnapshotLease {
    snapshot: Arc<ShardSnapshot>,
    _permit: OwnedSemaphorePermit,
}

impl LiveSnapshotLease {
    pub(crate) fn new(snapshot: Arc<ShardSnapshot>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            snapshot,
            _permit: permit,
        }
    }

    /// Returns the immutable DTO guarded by this retained-reader permit.
    pub fn snapshot(&self) -> &ShardSnapshot {
        &self.snapshot
    }
}

/// Revision metadata for one shard in a non-atomic cross-shard read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSnapshotRevision {
    shard_id: ShardId,
    snapshot_revision: NonZeroU64,
    evaluated_at: Timestamp,
    published_at: Timestamp,
}

impl ShardSnapshotRevision {
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    pub const fn snapshot_revision(&self) -> NonZeroU64 {
        self.snapshot_revision
    }
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Non-cloneable retained-reader lease for a sorted cross-shard revision vector.
#[derive(Debug)]
pub struct LiveRuntimeSnapshotLease {
    snapshots: Box<[Arc<ShardSnapshot>]>,
    revisions: Box<[ShardSnapshotRevision]>,
    _permit: OwnedSemaphorePermit,
}

impl LiveRuntimeSnapshotLease {
    pub(crate) fn new(
        snapshots: Box<[Arc<ShardSnapshot>]>,
        revisions: Box<[ShardSnapshotRevision]>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            snapshots,
            revisions,
            _permit: permit,
        }
    }

    /// Iterates borrowed DTOs while this one retained-reader permit remains held.
    ///
    /// The underlying runtime-owned `Arc` values are intentionally never exposed, so callers
    /// cannot clone them and retain unbounded historical publications after dropping this lease.
    pub fn snapshots(&self) -> impl ExactSizeIterator<Item = &ShardSnapshot> {
        self.snapshots.iter().map(Arc::as_ref)
    }
    pub fn revisions(&self) -> &[ShardSnapshotRevision] {
        &self.revisions
    }
}

/// Cloneable read-only access to current immutable shard publications.
#[derive(Clone, Debug)]
pub struct LiveSnapshotReader {
    pub(crate) plane: Arc<store::SnapshotPlane>,
}

impl LiveSnapshotReader {
    /// Loads one current shard snapshot without blocking publication.
    pub fn try_load(&self, shard: ShardId) -> Result<LiveSnapshotLease, SnapshotReadError> {
        self.plane.try_load(shard)
    }

    /// Loads a sorted cross-shard revision vector without claiming global atomicity.
    pub fn try_load_all(&self) -> Result<LiveRuntimeSnapshotLease, SnapshotReadError> {
        self.plane.try_load_all()
    }
}

fn checked_usize(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<NonZeroUsize, SnapshotLimitsError> {
    let value = NonZeroUsize::new(value).ok_or(SnapshotLimitsError::Zero { field })?;
    if value.get() > maximum {
        return Err(SnapshotLimitsError::ExceedsHardLimit {
            field,
            value: value.get() as u64,
            maximum: maximum as u64,
        });
    }
    Ok(value)
}

fn checked_u32(
    field: &'static str,
    value: u32,
    maximum: u32,
) -> Result<NonZeroU32, SnapshotLimitsError> {
    let value = NonZeroU32::new(value).ok_or(SnapshotLimitsError::Zero { field })?;
    if value.get() > maximum {
        return Err(SnapshotLimitsError::ExceedsHardLimit {
            field,
            value: u64::from(value.get()),
            maximum: u64::from(maximum),
        });
    }
    Ok(value)
}

/// Invalid public snapshot output bounds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SnapshotLimitsError {
    #[error("snapshot limit {field} must be nonzero")]
    Zero { field: &'static str },
    #[error("snapshot limit {field} value {value} exceeds hard maximum {maximum}")]
    ExceedsHardLimit {
        field: &'static str,
        value: u64,
        maximum: u64,
    },
    #[error("snapshot retained-byte limit {value} is below truthful base size {minimum}")]
    BelowTruthfulBase { value: u32, minimum: u32 },
    #[error("truthful base snapshot size cannot be represented")]
    TruthfulBaseUnrepresentable,
}

/// Snapshot construction invariant or checked-retained-size failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SnapshotBuildError {
    #[error("snapshot count cannot be represented")]
    CountOverflow,
    #[error("snapshot dimension counts violate configured bounds")]
    DimensionInvariant,
    #[error("snapshot retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    #[error("snapshot revision exhausted")]
    RevisionExhausted,
    #[error("system clock is outside the supported timestamp range")]
    ClockRange,
    #[error("committed stream seed omitted required provenance time")]
    IncompleteStreamProvenance,
}

/// Bounded retained-reader or shard lookup failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SnapshotReadError {
    #[error("all configured retained snapshot reader permits are in use")]
    ReaderLimitReached,
    #[error("snapshot shard identity is not part of this runtime incarnation")]
    UnknownShard,
    #[error("snapshot reader plane is closed")]
    Closed,
}
