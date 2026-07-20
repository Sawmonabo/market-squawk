//! Deterministic instrument-owned live state and current execution authority.

mod action;
mod authority;
mod book;
mod cross_venue;
mod features;
mod integrity;
mod normalization;
mod processor;
mod provider_book;
mod qualification;
mod runtime;
mod sharding;
mod snapshot;
mod state;

pub use action::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext,
    CommittedMarketReference, CurrentAuthorityGate, CurrentAuthorityGateError, LiveActionHook,
    LiveActionHookError, MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION, RouteActionHook,
    RouteActionHookError,
};
pub use authority::{
    AuthorityError, ConsumedLiveAuthority, ConsumedLiveEvidence, LiveExecutionCapability,
};
pub use book::{BookError, BookSide, DepthLimit, LevelUpdate, MAX_BOOK_MESSAGE_ITEMS, ScaledBook};
pub use cross_venue::{
    CrossVenueFeatureError, CrossVenueFeatureHub, CrossVenueFeatureSnapshot, CrossVenueUpdate,
    CrossVenueVenueSnapshot,
};
pub use features::{FeatureInvalidationReason, RouteFeatureError, RouteFeatureState};
pub use integrity::{
    ChecksumValidationError, KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID,
    ResolvedChecksumValidator, SequenceTracker, SequenceValidationError, kraken_v2_crc32,
};
pub use normalization::{
    NormalizationError, normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};
pub use runtime::{
    BoundShardIngress, DormantRouteIngress, LiveIngressBindError, LiveIngressError,
    LiveRouteConfig, LiveRouteConfigInput, LiveRuntime, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput, LiveRuntimeHealthEvent, LiveRuntimeHealthKind, LiveRuntimeIngress,
    LiveRuntimeReplaceError, LiveRuntimeShutdown, LiveRuntimeStartError,
    MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT, RegistrationFailure, ShardShutdownOutcome,
    ShardShutdownStatus,
};
pub use sharding::{
    ShardCount, ShardId, ShardKey, ShardRouter, ShardRoutingError, ShardRoutingVersion,
};
pub use snapshot::{
    BookLevelSnapshot, LiveFeatureScalarSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot,
    LiveFeatureValueSnapshot, LiveRuntimeSnapshotLease, LiveSnapshotLease, LiveSnapshotReader,
    RouteSnapshot, ShardLifecycleSnapshot, ShardSnapshot, ShardSnapshotRevision,
    SnapshotCompleteness, SnapshotDimension, SnapshotLimits, SnapshotLimitsError,
    SnapshotReadError, StatusSnapshot, StreamPhaseSnapshot, StreamSnapshot,
};
pub use state::{GenerationPhase, GenerationStateError, GenerationStateMachine};
