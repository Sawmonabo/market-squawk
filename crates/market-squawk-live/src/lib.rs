//! Deterministic instrument-owned live state and current execution authority.

mod action;
mod authority;
mod book;
mod cross_venue;
mod features;
mod integrity;
mod order_level;
mod processor;
mod provider_book;
mod qualification;
mod qualified_export;
mod runtime;
mod sharding;
mod snapshot;
mod state;

pub(crate) use action::ActionHookActivationLease;
pub use action::{
    ActionAuthorityIssueLimit, ActionHookDisposition, ActiveLiveActionHookGroup,
    CommittedActionContext, CommittedMarketReference, CurrentAuthorityGate,
    CurrentAuthorityGateError, DisabledLiveActionHookGroup, LiveActionControlError,
    LiveActionControlRejection, LiveActionHook, LiveActionHookActivationError, LiveActionHookError,
    LiveActionHookGeneration, LiveActionHookReapReceipt,
    MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION, PreparedLiveActionHookGroup, RouteActionHook,
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
pub use market_squawk_sources::{
    DirectBookLimits, DirectOrderBook, DirectOrderBookError, DirectPublishedBook,
    DirectPublishedLevel, DirectSyncPhase, NormalizationError, normalize_delta_quantity,
    normalize_positive_quantity, normalize_price,
};
pub use order_level::{
    MAX_ORDER_LEVEL_ORDERS, OrderLevelBatch, OrderLevelBatchError, OrderLevelBatchInput,
    OrderLevelBatchKind, OrderLevelBatchPayload, OrderLevelBook, OrderLevelBookError,
    OrderLevelCommit, OrderLevelDeleteQuantity, OrderLevelEntry, OrderLevelEvent,
    OrderLevelLimitError, OrderLevelLimits, OrderLevelModelError, OrderLevelOperation,
    OrderLevelPhase, OrderLevelPriceProjection, OrderLevelPriority, OrderLevelPriorityUpdate,
    OrderLevelProjectionError, OrderLevelQuarantineReason, OrderLevelRoute, OrderLevelVisibleOrder,
    PriceLevelProjection, SequencedProviderConversionError, UnknownOrderDisposition,
    provider_order, provider_snapshot_orders, sequenced_provider_event,
};
pub use qualification::{CommittedQualifiedMarketObservation, QualifiedMarketPrice};
pub use qualified_export::{
    QualifiedMarketExportError, QualifiedMarketObservationLease,
    QualifiedMarketObservationReceiver, RouteQualifiedMarketExport,
};
pub use runtime::{
    BoundShardIngress, DormantRouteIngress, LiveIngressBindError, LiveIngressError,
    LiveIngressRevokeError, LiveRouteConfig, LiveRouteConfigInput, LiveRuntime, LiveRuntimeConfig,
    LiveRuntimeConfigError, LiveRuntimeConfigInput, LiveRuntimeHealthEvent, LiveRuntimeHealthKind,
    LiveRuntimeIngress, LiveRuntimeReplaceError, LiveRuntimeShutdown, LiveRuntimeStartError,
    MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT, RegistrationFailure, ShardShutdownOutcome,
    ShardShutdownStatus,
};
pub use sharding::{
    ShardCount, ShardId, ShardKey, ShardRouter, ShardRoutingError, ShardRoutingVersion,
};
pub use snapshot::{
    BookLevelSnapshot, LastTradeSnapshot, LiveFeatureScalarSnapshot, LiveFeatureSetSnapshot,
    LiveFeatureSnapshot, LiveFeatureValueSnapshot, LiveRuntimeSnapshotLease, LiveSnapshotLease,
    LiveSnapshotReader, RouteSnapshot, ShardLifecycleSnapshot, ShardSnapshot,
    ShardSnapshotRevision, SnapshotCompleteness, SnapshotDimension, SnapshotLimits,
    SnapshotLimitsError, SnapshotReadError, SourceRuntimeEvidenceSnapshot, StatusSnapshot,
    StreamPhaseSnapshot, StreamSnapshot,
};
pub use state::{GenerationPhase, GenerationStateError, GenerationStateMachine};
