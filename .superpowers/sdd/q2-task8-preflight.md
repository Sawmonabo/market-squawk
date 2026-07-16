# Q2 Task 8 Runtime Integration Preflight and Implementation Map

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this map task-by-task. Use
> `superpowers:test-driven-development` for every behavior change and
> `superpowers:verification-before-completion` before the Quarter 2 checkpoint. Formal review is
> intentionally grouped after the exact integrated Tasks 5-8 head, not after an individual task.

**Goal:** Add deterministic, versioned single-writer shard routing, count-and-byte bounded live
ingress, fail-closed actor supervision, and bounded immutable snapshots without allowing legacy,
replayed, serialized, or caller-authored data to acquire current execution authority.

**Architecture:** The source registry issues an opaque current lease before a configured source
generation starts feeding data. Task 8 binds that lease and one exact route to a producer handle
whose generation invalidator is minted by the owning actor and shared with the Task 7 processor;
the producer can then
perform nonblocking count-and-byte admission without a map lookup on failure. One runtime-wide
incarnation allocation and one actor-wide shard-liveness allocation are injected into every
processor they govern. Actors own all mutable state and publish complete bounded DTOs through
crate-private `ArcSwap` cells after the action decision boundary.

**Tech Stack:** Rust 1.97.0 stable, Edition 2024, Tokio 1.52.3 as currently locked, Tokio
`mpsc`/`Semaphore`/`JoinSet`, Tokio Util `CancellationToken`, ArcSwap 1.9.2, Serde for bounded
diagnostic DTOs only, Task 5 current-source leases, and Task 7 opaque execution capability leases.

## Global constraints

- No paid software, paid API, cloud service, external database service, mandatory container
  runtime, or mandatory telemetry infrastructure.
- No identity/account rotation to evade limits, browser or TLS fingerprint spoofing, CAPTCHA or
  anti-bot bypass, proxy rotation intended to defeat blocking, or distributed quota evasion.
- No SQLite, DataFusion, Parquet, Python, MCP, LLM, arbitrary filesystem operation, unrelated
  network request, or unbounded write in the event-to-action path.
- The only production actor ingress payload is Task 5's owned, non-Serde
  `CurrentDecodedProviderBatch`; bare decoder values, canonical events, audit DTOs, and replay
  values are rejected structurally.
- Only state derived as `DirectVerified` can receive Task 7's opaque current capability. Task 8
  never infers quality or authority from a snapshot.
- Queue count, queue bytes, individual message bytes, route count, source generations, instrument
  state, book depth, snapshot output, notification count, retained readers, and shutdown duration
  are all validated and bounded.
- Every saturation, closure, accounting failure, runtime replacement, and actor exit publishes
  one-way Release invalidation before the failing API returns or the actor exits. Best-effort
  health events are diagnostics, never the safety transition.
- Rust library errors remain typed `thiserror` values. Production code does not use `unwrap`,
  `expect`, `panic!`, unsafe Rust, unchecked arithmetic, or an unspecified/randomized routing hash.
- Files remain cohesive and normally below 500-700 lines. `runtime.rs` is a facade over focused
  runtime submodules rather than a monolith.
- The repository remains runnable. The old app engine is quarantined as diagnostic/replay
  compatibility; it is never converted into a `CurrentDecodedProviderBatch` and never enters the
  production runtime.

---

## Evidence and scope

This is an implementation preflight, not a formal code review.

- Exact integrated root inspected: `349aa084f365878ccb43e11d24ec4b57d49d28ee`
  (`test(capture): harden bridge verification`).
- Root Task 5/6 capture bridge inspected through
  `crates/market-squawk-domain/src/capture.rs`,
  `crates/market-squawk-sources/src/registry/{authority,current_batch}.rs`,
  `crates/market-squawk-platform/src/capture.rs`, and the three Q2 implementation reports.
- In-progress Task 7 inspected read-only in `.worktrees/q2-task7-live` at branch head `806cd22` plus
  its uncommitted `market-squawk-live` tree. That tree was actively changing, so every Task 7 item
  below is either an observed signature or an explicitly marked integration requirement.
- The ignored Task 8 brief was read from the root checkout at
  `.superpowers/sdd/task-8-brief.md`; task briefs do not appear in ordinary committed worktrees.
- The complete frozen contract was read from `docs/plans/q2-live-readiness-audit.md`.
- Local Tokio 1.52.3 and ArcSwap 1.9.2 sources were inspected. Tokio's
  `Semaphore::try_acquire_many_owned` accepts `u32`, even though `Semaphore::MAX_PERMITS` is a
  larger `usize` bound. Task 8 must therefore cap both aggregate byte permits and per-command byte
  permits at `u32::MAX` before calling Tokio.
- The committed lockfile on `349aa08` does not yet include the integrated ArcSwap/platform graph.
  `cargo build --workspace --all-features --locked` correctly failed with exit 101 because the
  lockfile needs regeneration. Root owns the one final integrated lockfile; this report-only branch
  did not modify it.

## Upstream contracts observed

### Task 5 current batches

At integrated root, `CurrentDecodedProviderBatch` is an owned, nonempty homogeneous route batch
with `key()`, checked `retained_bytes()`, `validate_at()`, and consuming
`into_observations()`. Each intact `CurrentProviderObservation` retains its exact frame evidence,
stream key, compact policy, and `CurrentSourceAuthorityLease`. Mixed provider frames are grouped by
`(venue, instrument)` while preserving first-key and per-key wire order.

The Task 7 worktree adds two required source-side changes:

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CurrentStreamKey { /* private exact stream identity */ }

impl CurrentDecodedProviderBatch {
    pub fn current_lease(&self) -> &CurrentSourceAuthorityLease;
}
```

The final pre-feed control seam must also exist on the already registry-validated current authority
view. The precise name may follow source-crate conventions, but the ownership and checks are fixed:

```rust
impl ValidatedCurrentSourceAuthority<'_> {
    pub fn try_current_lease(&self) -> Result<CurrentSourceAuthorityLease, RegistryError>;
}

impl CurrentSourceAuthorityLease {
    pub fn health_epoch(&self) -> u64;
    pub fn valid_until(&self) -> Timestamp;
}
```

`try_current_lease` rechecks the exact current session allocation, health epoch, capture allocation,
and inclusive deadline inherited from the registry validation. It returns the same opaque,
non-Serde, O(1)-clone validation lease later embedded in routed batches. It accepts no loose source,
session, generation, quality, health, metadata, or capture values.

The two lease getters expose bounded audit data needed by the owner-built snapshot seed. They do not
return the underlying allocation, provide mutation, or replace `validate_at`; serialized snapshots
cannot reconstruct the lease.

This method removes the first-batch paradox: the app/source supervisor binds Task 8 generation
ingress before the adapter opens or feeds the data queue. A health-epoch refresh returns a fresh
source lease but reuses the same Task 7 generation invalidator; a connection-generation rollover
allocates a new invalidator and revokes the old one.

### Task 6 capture

The integrated platform queue is generic over a complete domain `CaptureAuthorityBundle`. For the
production source bundle it queues the exact `RawMarketFrame` and returns the exact Task 5
`CaptureAdmissionReceipt`. Saturation and writer faults already degrade the exact capture
allocation. Task 8 must consume only receipt-validated current batches produced after that bridge;
it does not duplicate capture authority or accept `RawCaptureRecord`/MSJ1 diagnostics.

### Task 7 authority as observed

The in-progress Task 7 tree currently provides the following useful shapes:

```rust
pub struct LiveExecutionCapability { /* private, non-Clone, non-Serde */ }
pub struct ConsumedLiveAuthority { /* private, movable */ }

impl ConsumedLiveAuthority {
    pub fn validate_current(&self) -> Result<(), AuthorityError>;
}

pub(crate) struct InstrumentLiveProcessor<C: TrustedClock> { /* owner state */ }
pub(crate) struct CurrentBatchCursor { /* owned wire-order iterator */ }
pub(crate) struct AppliedLiveObservation {
    pub(crate) event: MarketEvent,
    pub(crate) assessment: QualificationAssessment,
    pub(crate) authority: Option<AppliedObservationAuthority>,
}

impl<C: TrustedClock> InstrumentLiveProcessor<C> {
    pub(crate) fn accept_batch(
        &mut self,
        batch: CurrentDecodedProviderBatch,
        admission: &GenerationAdmission,
    ) -> Result<CurrentBatchCursor, LiveApplyError>;

    pub(crate) fn apply_next(
        &mut self,
        cursor: &mut CurrentBatchCursor,
    ) -> Result<Option<AppliedLiveObservation>, LiveApplyError>;

    pub(crate) fn issue(
        &mut self,
        applied: &AppliedObservationAuthority,
    ) -> Result<LiveExecutionCapability, AuthorityError>;

    pub(crate) fn consume(
        &mut self,
        capability: LiveExecutionCapability,
    ) -> Result<ConsumedLiveAuthority, AuthorityError>;
}
```

Task 7 already validates source, generation, shard, runtime, status, state revision, status
revision, wall deadline, monotonic deadline, nonce identity, and binding before and after issuance
and consumption. `ConsumedLiveAuthority::validate_current` is the later risk/dispatch recheck.

The near-completion worktree now injects `ProcessorLivenessBinding` and has split transactional
stream/status/snapshot modules. Its snapshot seed already includes deterministically sorted streams,
statuses, book levels, generation phase, revision, sequence, and currentness. It still uses one
untyped `OneWayLease` for multiple authority dimensions, keeps the generation owner map and
`register_generation` inside each processor, and omits health epoch, snapshot-origin revision,
observed/evaluated timestamps, configured/output depth metadata, and checked retained bytes from
the seed. It also lacks an actor-callable sealed-clock `validate_applied_current`. The following
seam changes are required before Task 8 is considered integrated; they are not optional alternate
designs.

---

## Required Task 7 integration seams

### 1. Separate the bounded generation registry from processor state

Move the `SourceGenerationKey`/generation-owner map out of one instrument processor and into a
crate-private bounded route registry owned by the shard actor. The source supervisor sends one
bounded/cancellable control command before opening the data feed; the actor registers or refreshes
the exact source lease and returns a cloneable `GenerationAdmission` over `oneshot`. The actor exit
guard owns registry invalidation:

```rust
pub(crate) struct GenerationAuthorityRegistry {
    entries: HashMap<SourceId, GenerationAuthorityEntry>,
    maximum_sources: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct GenerationAdmission {
    source: CurrentSourceAuthorityLease,
    generation: GenerationExecutionLease,
}

impl GenerationAuthorityRegistry {
    pub(crate) fn try_new(maximum_sources: usize) -> Result<Self, AuthorityError>;

    pub(crate) fn bind(
        &mut self,
        source: CurrentSourceAuthorityLease,
    ) -> Result<GenerationAdmission, AuthorityError>;

    pub(crate) fn invalidate_all(&mut self);
}

impl GenerationAdmission {
    pub(crate) fn validate_at(&self, at: Timestamp) -> Result<(), AuthorityError>;

    pub(crate) fn matches(&self, source: &CurrentSourceAuthorityLease) -> bool;

    pub(crate) fn invalidate_on_admission_failure(&self);

    pub(crate) fn retained_bytes(&self) -> Result<usize, AuthorityError>;
}
```

The registry is keyed by the currently authoritative Task 5 source allocation for one route, not by
caller-provided strings. Rebinding a new health epoch for the same metadata/session/connection
generation replaces only the `source` lease in the returned handle and reuses the exact same
`OneWayLease`. A strictly newer Task 5 connection generation invalidates/replaces the prior entry.
A stale/equal-but-different allocation fails. The bounded registry replaces per-source entries
rather than accumulating every reconnect until capacity exhaustion.

`BoundShardIngress::try_publish` stores one returned `GenerationAdmission`; therefore its failure
path never sends control work, locks, or scans the registry. Registration occurs only during the
source-supervisor bind/rebind handshake, not per market frame. If the control channel closes,
saturates, is cancelled, or exceeds its bounded registration deadline before an admission is
returned, no new execution-generation handle exists and the source feed remains unopened.

### 2. Make every one-way lease dimension statically distinct

The current Task 7 candidate uses the same `OneWayLease` type for generation, status, shard, and
runtime fields. Field names are insufficient: those leases can be swapped accidentally inside
crate-private constructors and still compile. Replace the untyped surface with a private generic
marker implementation or four opaque newtypes:

```rust
pub(crate) struct GenerationExecutionLease(/* private marker-typed lease */);
pub(crate) struct StatusExecutionLease(/* private marker-typed lease */);
pub(crate) struct ShardLivenessLease(/* private marker-typed lease */);
pub(crate) struct RuntimeIncarnationLease(/* private marker-typed lease */);

pub(crate) struct GenerationExecutionOwner(/* private owner */);
pub(crate) struct StatusExecutionOwner(/* private owner */);
pub(crate) struct ShardLivenessOwner(/* private owner */);
pub(crate) struct RuntimeIncarnationOwner(/* private owner */);
```

Each owner can mint only its corresponding lease. No `From`, `Into`, common public constructor, raw
allocation getter, or type-erased one-way lease crosses the authority modules. Add compile-time/unit
tests that constructor parameter swaps do not type-check and runtime transplant tests that distinct
allocations fail even within one dimension. State and status revision leases should receive
distinct markers as well so their expected revisions cannot be exchanged.

### 3. Inject Task 8 authority allocations

Task 8 owns one runtime-incarnation allocation shared by all shards in an incarnation and one
shard-liveness allocation shared by every processor in one actor. Task 7 processors accept the
validation/degradation lease clones; they must not invent independent per-instrument allocations:

```rust
pub(crate) struct ProcessorAuthorityContext {
    pub(crate) shard: ShardLivenessLease,
    pub(crate) runtime: RuntimeIncarnationLease,
}

impl InstrumentLiveProcessor<SystemTrustedClock> {
    pub(crate) fn new_system(
        definition: InstrumentDefinition,
        depth: DepthLimit,
        nonce_capacity: usize,
        nonce_reclaim_budget: usize,
        maximum_capability_lifetime: Duration,
        authority: ProcessorAuthorityContext,
    ) -> Result<Self, LiveApplyError>;
}
```

Task 8 retains the sole runtime/shard owners and can invalidate them before any processor cleanup.
The injected marker-typed leases may retain degradation-only `invalidate()` because fail-closed
holders are allowed to reduce authority; they expose no activation/reset operation and cannot be
converted between dimensions.

### 4. Add explicit pre-feature/pre-strategy revalidation

The actor must not inspect private fields or duplicate Task 7's lease logic. Add one crate-private
processor method that uses the sealed trusted clock and exact applied authority:

```rust
impl<C: TrustedClock> InstrumentLiveProcessor<C> {
    pub(crate) fn validate_applied_current(
        &self,
        applied: &AppliedObservationAuthority,
    ) -> Result<(), AuthorityError>;
}
```

Task 8 calls this method before feature work and again before strategy/issuance. `issue` and
`consume` retain their existing before/after nonce-transition validation. Later risk and final
dispatch call public `ConsumedLiveAuthority::validate_current` independently.

### 5. Expose a bounded snapshot seed, not mutable state

Task 8 cannot build truthful snapshots from `AppliedLiveObservation` alone: non-executable and
quarantined states may have no `AppliedObservationAuthority`, and current generation phase, status,
revision, snapshot-origin revision, book depth, and health epoch are private. Add a crate-private,
owned, bounded DTO built by the processor:

```rust
#[derive(Debug)]
pub(crate) struct ProcessorSnapshotSeed {
    pub(crate) streams: Box<[StreamSnapshotSeed]>,
    pub(crate) statuses: Box<[StatusSnapshotSeed]>,
    pub(crate) retained_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct StreamSnapshotSeed {
    pub(crate) stream: CurrentStreamKey,
    pub(crate) connection_generation: ConnectionGeneration,
    pub(crate) phase: crate::snapshot::StreamPhaseSnapshot,
    pub(crate) state_revision: u64,
    pub(crate) snapshot_origin_revision: Option<u64>,
    pub(crate) health_epoch: u64,
    pub(crate) source_valid_until: Timestamp,
    pub(crate) source_timestamp: Option<Timestamp>,
    pub(crate) received_at: Timestamp,
    pub(crate) evaluated_at: Timestamp,
    pub(crate) configured_depth: u32,
    pub(crate) state_bid_depth: usize,
    pub(crate) state_ask_depth: usize,
    pub(crate) bids: Box<[ProcessorLevelSeed]>,
    pub(crate) asks: Box<[ProcessorLevelSeed]>,
}

#[derive(Debug)]
pub(crate) struct StatusSnapshotSeed {
    pub(crate) source: SourceId,
    pub(crate) venue: VenueId,
    pub(crate) instrument: InstrumentId,
    pub(crate) connection_generation: ConnectionGeneration,
    pub(crate) trading_status: TradingStatus,
    pub(crate) status_revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessorLevelSeed {
    pub(crate) price: PriceTicks,
    pub(crate) quantity: QuantityLots,
}

impl<C: TrustedClock> InstrumentLiveProcessor<C> {
    pub(crate) fn snapshot_seed(
        &self,
        limits: ProcessorSnapshotLimits,
    ) -> Result<ProcessorSnapshotSeed, LiveApplyError>;
}
```

Construction happens on the owning actor from one committed state, uses deterministic stream/book
ordering, performs checked retained-byte accounting, and records exact per-dimension available and
returned counts. Shared cross-channel status remains a separate seed keyed by
`(source, venue, instrument, generation)` rather than being duplicated as channel-local mutable
state. It exposes no lease, owner, issuer, nonce, capability, `ArcSwap`, or mutable book. If Task 7
uses different internal names, Task 8 must preserve these semantics and fields.

---

## File and responsibility map

### Live crate

- Create `crates/market-squawk-live/src/sharding.rs`
  - `ShardRoutingVersion`, `ShardCount`, `ShardId`, `ShardKey`, `ShardRouter`.
  - V1 preimage encoding and FNV-1a implementation only.
  - No actor, Tokio, snapshot, source-health, or application behavior.
- Create `crates/market-squawk-live/src/runtime.rs`
  - Public runtime configuration/start/shutdown facade and typed errors.
  - Re-exports only safe ingress, lifecycle, health, and snapshot-reader handles.
- Create `crates/market-squawk-live/src/runtime/admission.rs`
  - Route-bound producer handle, private command enum, checked retained-cost calculation, count and
    byte permit acquisition, exact-generation invalidation, and bounded health mirror.
- Create `crates/market-squawk-live/src/runtime/actor.rs`
  - Single-writer event loop, route-owned processors, apply/action/snapshot ordering, queue drain,
    and actor drop guard.
- Create `crates/market-squawk-live/src/runtime/config.rs`
  - Validated count/byte/message/snapshot/instrument/nonce/shutdown bounds and per-route definition
    validation.
- Create `crates/market-squawk-live/src/runtime/lifecycle.rs`
  - Complete-startup ready barrier, partial-startup cleanup, cancellation, runtime replacement,
    deadline join, abort-and-await, and aggregate shutdown outcome.
- Create `crates/market-squawk-live/src/runtime/memory.rs`
  - Checked conservative peak-retained model and constants for mailbox, processing candidate,
    processor capacity, snapshot construction/current publication, notifications, and retained
    reader allowance.
- Create `crates/market-squawk-live/src/snapshot.rs`
  - Public immutable bounded DTOs, completeness types, sorted cross-shard revision vector, and
    private publication/reader facade.
- Create `crates/market-squawk-live/src/snapshot/store.rs`
  - Crate-private `ArcSwap`, checked snapshot revision, bounded reader permits, coalescing
    notification channel, and nonblocking store/load operations.
- Modify `crates/market-squawk-live/src/lib.rs`
  - Export routing/config/ingress/snapshot DTO/lifecycle error types.
  - Keep processor, issuer, authority owners, admission registry, snapshot cell, and actor command
    crate-private.
- Modify `crates/market-squawk-live/src/authority.rs`
  - Add bounded generation registry, externally injected runtime/shard lease context, and applied
    revalidation seam.
- Modify `crates/market-squawk-live/src/authority/lease.rs`
  - Replace interchangeable one-way/state-revision leases with private marker-typed generation,
    status, shard, runtime, instrument-state revision, and status-revision owners/leases.
- Modify the focused Task 7 processor modules after Task 7's final split
  - Consume injected authority, accept exact `GenerationAdmission`, expose bounded snapshot seed,
    and remove private per-processor runtime/shard owner construction.
- Modify `crates/market-squawk-sources/src/registry/authority.rs`
  - Add the pre-feed current-lease method with allocation/currentness tests.
- Modify `crates/market-squawk-sources/src/registry/current_batch.rs`
  - Retain Task 7's `CurrentStreamKey: Hash` and batch `current_lease()` getter with non-Serde and
    transplant regressions.

### Tests

- Create `crates/market-squawk-live/tests/sharding.rs`
  - Golden vectors, zero/count bounds, byte encoding, Unicode byte length, delimiter ambiguity,
    process/architecture independence, and every valid shard result `< shard_count`.
- Create `crates/market-squawk-live/tests/overflow.rs`
  - Real Task 5 registry/capture/current-batch fixture; count-full, byte-full, overweight,
    checked-cost, closed, lease transplant, health rebind, rollover, permit release, and
    invalidation-before-return tests.
- Create `crates/market-squawk-live/tests/snapshot_isolation.rs`
  - Atomic N/N+1 snapshots, slow-reader nonblocking publication, reader bound, exact truncation,
    deterministic order, stale snapshot nonauthority, and no fabricated global `as_of`.
- Create `crates/market-squawk-live/tests/runtime_lifecycle.rs`
  - Complete startup, partial startup, unexpected exit, cancel/receive race, runtime replacement,
    queued drain, shutdown deadline, abort-and-await, and no detached actor evidence.
- Add private unit tests beside `runtime/actor.rs` and `runtime/admission.rs`
  - Deterministic barriers around apply, feature, strategy, issue, consume, risk-validation, and
    dispatch-validation boundaries; private fault injection does not create a production token.
- Modify `apps/market-squawk/tests/engine.rs`
  - Rename imports and assertions to the diagnostic compatibility engine; assert it exposes no
    production runtime ingress or capability surface.
- Create `apps/market-squawk/tests/live_runtime_composition.rs`
  - Build the real runtime facade from checked route definitions, prove startup-before-source and
    reverse shutdown ordering, and prove legacy app events cannot enter `BoundShardIngress`.

### Application compatibility quarantine

- Create `apps/market-squawk/src/live_runtime.rs`
  - The real Task 8 composition boundary: convert validated app configuration and instrument
    definitions into `LiveRuntimeConfig`, start the runtime before sources, expose only bound
    ingress and bounded snapshot readers, and perform explicit bounded shutdown.
- Move `apps/market-squawk/src/engine.rs` to
  `apps/market-squawk/src/diagnostic_engine.rs`
  - Preserve runnable mock/current CLI/MCP/replay compatibility.
  - Rename public types to `DiagnosticEngine`, `DiagnosticEngineSnapshot`, and
    `SharedDiagnosticEngine`.
  - Remove wording that implies execution quality/current authority. Its paper calculation remains
    a compatibility diagnostic and cannot call Task 7 issuance, risk, or dispatch.
- Modify `apps/market-squawk/src/replay.rs`
  - Consume only `DiagnosticEngine`; retain explicit replay ineligibility.
- Modify `apps/market-squawk/src/mcp.rs`
  - Existing compatibility tools consume only the diagnostic snapshot until Task 13 services
    replace them. No Task 8 ingress, issuer, or snapshot cell is exposed to MCP.
- Modify `apps/market-squawk/src/main.rs`
  - Preserve the current diagnostic source path and compose the real runtime independently where
    configured definitions/current sources exist. Never translate legacy app `MarketEvent` into a
    current batch.
- Modify `apps/market-squawk/src/lib.rs`
  - Export explicitly named diagnostic compatibility types and the safe real-runtime composition
    facade. Remove the ambiguous `Engine`/`SharedEngine` production-facing names.

The compatibility engine deletion trigger is exact: Task 11 production adapters must emit receipt-
validated `CurrentDecodedProviderBatch` values and bind generation ingress before feed; Task 13
application services must consume Task 8 immutable live snapshots. Once both consumers are
integrated and the mock/replay tests use explicitly typed diagnostic sinks, the compatibility
engine and its app-local event/book/quality path are deleted. Until then it remains runnable but
structurally quarantined, not silently promoted and not vaguely deferred.

---

## Public and crate-private API map

### Versioned deterministic routing

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRoutingVersion {
    V1,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ShardCount(NonZeroU16);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ShardId {
    index: u16,
    count: ShardCount,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ShardKey {
    venue: VenueId,
    instrument: InstrumentId,
}

#[derive(Clone, Debug)]
pub struct ShardRouter {
    version: ShardRoutingVersion,
    count: ShardCount,
}

impl ShardRouter {
    pub fn v1(count: u16) -> Result<Self, ShardRoutingError>;
    pub fn route(&self, key: &ShardKey) -> ShardId;
    pub const fn version(&self) -> ShardRoutingVersion;
    pub const fn count(&self) -> ShardCount;
}
```

V1 hashes these exact bytes:

```text
ASCII "MSQKSHARD"
0x01
venue UTF-8 byte length as big-endian u16
venue UTF-8 bytes exactly as stored in VenueId
InstrumentId UUID 16 RFC/network-order bytes
```

FNV-1a uses offset `0xcbf29ce484222325` and prime `0x00000100000001b3` with wrapping
multiplication specified by the algorithm. Venue `coinbase` plus UUID
`018f0000-0000-7000-8000-000000000001` hashes to `0x28edee9cb1852659` and routes to shard 9 of 16.
No display text, UUID serialization, native endian, delimiter concatenation, normalization,
`DefaultHasher`, or dependency-specified hash participates.

### Validated runtime configuration

```rust
#[derive(Clone, Debug)]
pub struct LiveRuntimeConfig {
    routing_version: ShardRoutingVersion,
    shard_count: ShardCount,
    mailbox_count_per_shard: NonZeroUsize,
    mailbox_bytes_per_shard: NonZeroU32,
    maximum_message_bytes: NonZeroU32,
    maximum_routes_per_shard: NonZeroUsize,
    maximum_sources_per_route: NonZeroUsize,
    registration_control_capacity: NonZeroUsize,
    registration_deadline: Duration,
    health_event_capacity: NonZeroUsize,
    snapshot_event_budget: NonZeroUsize,
    snapshot_interval: Duration,
    snapshot_limits: SnapshotLimits,
    maximum_retained_snapshot_readers: NonZeroU32,
    shutdown_deadline: Duration,
}

#[derive(Clone, Debug)]
pub struct LiveRouteConfig {
    route: ShardKey,
    definition: InstrumentDefinition,
    depth: DepthLimit,
    nonce_capacity: NonZeroUsize,
    nonce_reclaim_budget: NonZeroUsize,
    maximum_capability_lifetime: Duration,
}
```

Construction rejects zero duration/capacity, `maximum_message_bytes > mailbox_bytes_per_shard`,
either byte limit above `u32::MAX`, Tokio permit incompatibility, duplicate route, route venue not
present in the definition, route instrument mismatch, a route landing outside configured shards,
too many routes per shard, checked memory-model overflow, and a configured peak above the explicit
runtime memory ceiling.

### Runtime and ingress

```rust
#[derive(Debug)]
pub struct LiveRuntime { /* owns JoinSet, authority owners, senders, cells */ }

#[derive(Clone, Debug)]
pub struct LiveRuntimeIngress { /* bind/rebind only */ }

#[derive(Clone, Debug)]
pub struct BoundShardIngress { /* exact route + exact generation */ }

impl LiveRuntime {
    pub async fn start(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeStartError>;

    pub fn ingress(&self) -> LiveRuntimeIngress;
    pub fn snapshots(&self) -> LiveSnapshotReader;
    pub fn try_next_health(&mut self) -> Option<LiveRuntimeHealthEvent>;

    pub async fn replace(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeReplaceError>;

    pub async fn shutdown(self) -> LiveRuntimeShutdown;
}

impl LiveRuntimeIngress {
    pub async fn bind_generation(
        &self,
        route: ShardKey,
        source: CurrentSourceAuthorityLease,
        cancellation: CancellationToken,
    ) -> Result<BoundShardIngress, LiveIngressBindError>;
}

impl BoundShardIngress {
    pub fn try_publish(
        &self,
        batch: CurrentDecodedProviderBatch,
    ) -> Result<(), LiveIngressError>;
}
```

There is deliberately no unbound `LiveRuntimeIngress::try_publish`, async enqueue, bare-event
enqueue, caller-provided retained-size trait, loose-generation constructor, ingress-to-processor
map lookup, or public actor sender.

`bind_generation` is bounded/cancellable source-supervisor control-plane work. It checks the
runtime/shard leases and exact configured route, sends the exact Task 5 lease over the owning
actor's separate bounded registration channel, and awaits a typed result only to the configured
deadline. The actor's route registry performs the current-lease registration. On health-epoch
refresh it returns a new bound handle with a fresh source lease and the same Task 7 generation
allocation. On connection rollover it Release-invalidates the old allocation before returning the
successor. The adapter/feed starts only after this future returns success.

`BoundShardIngress::try_publish` follows this exact order:

```text
Acquire runtime, shard, source, and generation leases
-> verify batch route and batch current lease share the bound allocation
-> compute private checked command retained bytes
-> reject overweight or u32 conversion failure
-> try_acquire_many_owned exact byte permits
-> try_send(command owning batch + admission + permit)
-> return
```

Every failure after a generation handle exists first calls
`GenerationAdmission::invalidate_on_admission_failure` with Release semantics. Dropping the private
command releases its owned byte permit exactly once. Queue saturation never attempts to enqueue a
quarantine message.

### Actor flow and authority boundary

Each actor owns a prevalidated route table keyed by `ShardKey`, one processor per configured route,
one shard-liveness owner, route generation registries, snapshot construction state, local health
counters, and local future strategy/risk state. No other shard shares mutable state or a state
mutex.

One dequeued command executes:

```text
Acquire runtime + shard + source + generation
-> reject invalid queued command without mutating processor state
-> processor.accept_batch(batch, exact GenerationAdmission)
-> for each wire-order observation:
     processor.apply_next
     -> processor.validate_applied_current before feature work
     -> bounded feature work (Task 9 integration point)
     -> processor.validate_applied_current before strategy/issue
     -> issue once per typed intent, if any
     -> consume before risk, then validate_current at risk
     -> validate_current again at final dispatch
     -> decide action/no-action
     -> update coalesced snapshot schedule
-> drop command and byte permit
```

Task 8 has no production strategy yet, so its current action decision is explicitly `NoStrategy`
and it does not mint an unused capability. Deterministic private tests exercise issue/consume and
the risk/dispatch recheck boundaries using Task 7's real authority methods; they do not add a mock
production issuer. Tasks 9 and 10 replace the no-strategy decision with the typed feature/strategy/
risk pipeline without changing the admission or snapshot order.

An invalid queued command may increment bounded/saturating diagnostic counters, but it does not call
`accept_batch`, mutate books, advance state revision, compute features, invoke strategy, issue a
nonce, enter risk, or dispatch.

### Snapshot DTO and publication

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotCompleteness {
    Complete,
    Truncated,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDimension {
    completeness: SnapshotCompleteness,
    available: u32,
    returned: u32,
    configured_limit: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ShardLifecycleSnapshot {
    Starting,
    Ready,
    Degraded,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StreamPhaseSnapshot {
    Disconnected,
    AwaitingSnapshot,
    Synchronizing,
    Healthy,
    Quarantined,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookLevelSnapshot {
    price: PriceTicks,
    quantity: QuantityLots,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardSnapshot {
    routing_version: ShardRoutingVersion,
    shard_count: ShardCount,
    runtime_incarnation: NonZeroU64,
    shard_id: ShardId,
    snapshot_revision: NonZeroU64,
    health_revision: u64,
    lifecycle: ShardLifecycleSnapshot,
    evaluated_at: Timestamp,
    published_at: Timestamp,
    routes: Box<[RouteSnapshot]>,
    route_dimension: SnapshotDimension,
    retained_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteSnapshot {
    route: ShardKey,
    streams: Box<[StreamSnapshot]>,
    statuses: Box<[StatusSnapshot]>,
    stream_dimension: SnapshotDimension,
    status_dimension: SnapshotDimension,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    source: SourceId,
    venue: VenueId,
    instrument: InstrumentId,
    connection_generation: ConnectionGeneration,
    trading_status: TradingStatus,
    status_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamSnapshot {
    source: SourceId,
    venue: VenueId,
    instrument: InstrumentId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    connection_generation: ConnectionGeneration,
    phase: StreamPhaseSnapshot,
    state_revision: u64,
    snapshot_origin_revision: Option<u64>,
    health_epoch: u64,
    source_valid_until: Timestamp,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    evaluated_at: Timestamp,
    configured_depth: u32,
    state_bid_depth: usize,
    state_ask_depth: usize,
    bids: Box<[BookLevelSnapshot]>,
    asks: Box<[BookLevelSnapshot]>,
    bid_dimension: SnapshotDimension,
    ask_dimension: SnapshotDimension,
}

#[derive(Debug)]
pub struct LiveSnapshotLease { /* non-Clone Arc DTO + reader permit */ }

#[derive(Clone, Debug)]
pub struct LiveSnapshotReader { /* cells remain private */ }

impl LiveSnapshotReader {
    pub fn try_load(&self, shard: ShardId) -> Result<LiveSnapshotLease, SnapshotReadError>;
    pub fn try_load_all(&self) -> Result<LiveRuntimeSnapshotLease, SnapshotReadError>;
}
```

The actor builds the entire next `ShardSnapshot` from one committed owner state after the action
decision and before calling `ArcSwap::store`. Readers observe all of N or all of N+1. The cell and
store operation are crate-private. A separate capacity-one `mpsc::Sender<()>` uses `try_send` as a
coalescing hint; full notification drops increment a saturating counter and never delay store.

Reader permits enforce the configured trusted-reader retention allowance. `LiveSnapshotLease` is
non-Clone and releases one permit on drop. A held N lease cannot block publication of N+1. External
code can serialize/copy a DTO under its own application-service response bounds, but it cannot hold
unbounded runtime-owned historical `Arc` generations through the official reader.

`try_load_all` returns shard snapshots sorted by `ShardId` and a sorted vector of
`(ShardId, snapshot_revision, evaluated_at, published_at)`. It does not expose a single global
`as_of`, because cross-shard reads are not atomic.

Snapshot revision overflow, checked retained-size overflow, or inability to construct a bounded
truthful snapshot invalidates shard liveness and exits the actor. Truncation is explicit and
dimension-specific; it is not a fatal error when it follows configured output limits.

### Peak memory model

Runtime construction computes and records a conservative checked peak:

```text
preallocated route/processor/nonce capacity
+ bounded book/provider-lexeme state at configured depth
+ mailbox byte permits (queued plus currently processing commands)
+ one bounded message candidate and rollback journal per shard
+ one bounded snapshot under construction per shard
+ one current published snapshot per shard
+ maximum_retained_snapshot_readers * maximum snapshot bytes
+ bounded health events and capacity-one notification hints
+ fixed actor/task/channel/control overhead
```

The private command cost includes the Task 5 batch's already checked deep retained charge plus the
command, admission, permit, and conservative shared-allocation charges. The same allocation is
never omitted merely because it is behind an `Arc`. All additions/multiplications are checked. A
runtime whose model overflows or exceeds its explicit configured memory ceiling does not start.

### Lifecycle and replacement

`LiveRuntime::start` creates a fresh nonzero checked incarnation, allocates all route tables,
mailboxes, semaphores, leases, snapshot cells, and actor tasks, then awaits one ready result from
every configured shard. Ingress is returned only after all shards are ready. Any startup failure
Release-invalidates runtime/shard/generation authority, cancels, closes/drains queues, and joins or
aborts-and-awaits every actor already spawned.

The actor drop guard owns marker-typed degradation-only shard/runtime/generation handles and
invalidates in this
order on normal completion, error, cancellation, or unwind:

```text
shard liveness
-> shared runtime incarnation
-> all route generation allocations
-> route processor/status/state authority
-> terminal diagnostic publication best-effort
```

`LiveRuntime::shutdown` invalidates runtime authority before closing ingress, cancels actors,
drains queued commands so every byte permit drops, and waits to one total deadline. At the deadline
it calls `JoinSet::abort_all` and continues `join_next` until every aborted task is observed. The
returned outcome records complete, actor-error, panic/join-error, or deadline-aborted status for
every configured shard. `Drop` is a fail-closed fallback that invalidates and aborts the owned
`JoinSet`; normal application composition must call and inspect async `shutdown` so aborts are
awaited.

`LiveRuntime::replace` consumes the old runtime. It first invalidates the old incarnation and all
old ingress, performs bounded shutdown, then starts a new incarnation from the new routing
version/count. It never remaps live owner state. The app treats the result as `reconnect_required`
and obtains fresh Task 5 sessions, capture allocations, source leases, and snapshots.

---

## TDD implementation sequence

### Task 8.1: Freeze Task 5/7 seams

**Files:**

- Modify `crates/market-squawk-sources/src/registry/authority.rs`
- Modify `crates/market-squawk-sources/src/registry/current_batch.rs`
- Modify `crates/market-squawk-live/src/authority.rs`
- Modify the final focused Task 7 processor modules
- Test in source/live crate unit and integration suites

- [ ] Add failing tests proving a registry-validated authority can issue an owned current lease,
  deserialized DTOs cannot, health refresh changes the source lease while retaining generation
  identity, and rollover revokes the former lease.
- [ ] Run the exact focused tests and require missing-method/type failures.
- [ ] Add `try_current_lease`, `GenerationAuthorityRegistry`, injected
  `ProcessorAuthorityContext`, marker-typed authority dimensions, `validate_applied_current`, and
  the complete bounded snapshot seed APIs with no new public minting surface.
- [ ] Run source/live tests and strict Clippy.
- [ ] Commit the seam separately so Task 8 starts from a compiling exact API.

### Task 8.2: Implement stable routing

**Files:**

- Create `crates/market-squawk-live/src/sharding.rs`
- Create `crates/market-squawk-live/tests/sharding.rs`
- Modify `crates/market-squawk-live/src/lib.rs`

- [ ] Write the approved golden vector, zero/count bounds, delimiter ambiguity, byte-length, and
  repeatability tests.
- [ ] Run `cargo test -p market-squawk-live --test sharding --locked` and require missing-type
  failures.
- [ ] Implement the exact V1 encoding/FNV algorithm and typed routing values.
- [ ] Run the sharding test, live crate test suite, and strict Clippy.
- [ ] Commit `feat(live): add versioned deterministic shard routing`.

### Task 8.3: Implement configuration, memory model, and route partitioning

**Files:**

- Create `crates/market-squawk-live/src/runtime/config.rs`
- Create `crates/market-squawk-live/src/runtime/memory.rs`
- Create runtime unit tests beside both modules

- [ ] Write failing tables for every zero, `u32`, Tokio permit, duplicate route, mapping,
  per-shard route, duration, arithmetic-overflow, and memory-ceiling boundary.
- [ ] Implement validated config and deterministic partitioning with pre-reservation.
- [ ] Test boundary values at exact maximum and maximum plus one.
- [ ] Run live tests/Clippy and commit `feat(live): validate bounded shard runtime capacity`.

### Task 8.4: Implement pre-bound count-and-byte admission

**Files:**

- Create `crates/market-squawk-live/src/runtime/admission.rs`
- Create `crates/market-squawk-live/tests/overflow.rs`
- Reuse a real Task 5 registry/capture fixture

- [ ] Write failing count-full, byte-full, overweight, checked-cost, closed, wrong-route,
  source-lease transplant, rebind, rollover, and permit-drop tests.
- [ ] Add deterministic assertions that generation validation fails before each error is observable
  to the caller.
- [ ] Implement private checked command cost, owned byte permit, bound ingress, and bounded health
  mirror.
- [ ] Run overflow/live/source tests and strict Clippy.
- [ ] Commit `feat(live): add exact-generation bounded shard ingress`.

### Task 8.5: Implement actor ownership and linearization

**Files:**

- Create `crates/market-squawk-live/src/runtime/actor.rs`
- Add private barrier/model tests beside actor/authority modules

- [ ] Write failing deterministic tests for invalidation at dequeue, pre-feature, pre-strategy,
  after nonce registration, pre-consume, post-consume risk validation, final dispatch validation,
  and actor exit.
- [ ] Implement preallocated route-owned processors and the exact apply/action ordering.
- [ ] Prove queued invalidated commands do not change processor revision or issue an action.
- [ ] Run live tests/Clippy and commit `feat(live): own market state in single-writer shard actors`.

### Task 8.6: Implement immutable bounded snapshots

**Files:**

- Create `crates/market-squawk-live/src/snapshot.rs`
- Create `crates/market-squawk-live/src/snapshot/store.rs`
- Create `crates/market-squawk-live/tests/snapshot_isolation.rs`

- [ ] Write failing atomic N/N+1, slow-reader, reader-cap, notification-full, truncation,
  deterministic-order, revision-overflow, and cross-shard revision-vector tests.
- [ ] Implement checked DTO construction, crate-private ArcSwap publication, reader permits, and
  capacity-one coalesced hints.
- [ ] Prove serialized/stale snapshots cannot satisfy any capability/risk input type.
- [ ] Run snapshot/live tests/Clippy and commit `feat(live): publish bounded immutable snapshots`.

### Task 8.7: Implement complete startup and bounded shutdown

**Files:**

- Create `crates/market-squawk-live/src/runtime.rs`
- Create `crates/market-squawk-live/src/runtime/lifecycle.rs`
- Create `crates/market-squawk-live/tests/runtime_lifecycle.rs`

- [ ] Write failing ready-gate, partial-startup, unexpected-exit, cancellation/receive,
  runtime-replacement, queue-drain, deadline, abort-and-await, and exact-actor-count tests.
- [ ] Implement `LiveRuntime::{start,replace,shutdown}`, actor guards, and structured outcomes.
- [ ] Prove ingress is unavailable before complete readiness and invalid before shutdown returns.
- [ ] Run lifecycle/live tests/Clippy and commit `feat(live): supervise bounded shard lifecycle`.

### Task 8.8: Compose the app and quarantine compatibility

**Files:**

- Create `apps/market-squawk/src/live_runtime.rs`
- Move `apps/market-squawk/src/engine.rs` to
  `apps/market-squawk/src/diagnostic_engine.rs`
- Modify `apps/market-squawk/src/{lib,main,mcp,replay}.rs`
- Modify `apps/market-squawk/tests/engine.rs`
- Create `apps/market-squawk/tests/live_runtime_composition.rs`

- [ ] First change app tests to require explicit diagnostic names and no legacy-to-current bridge;
  run them and require compile failures.
- [ ] Rename/quarantine the engine and migrate its existing consumers without changing runnable
  diagnostic behavior.
- [ ] Compose real Task 8 runtime startup/snapshot/shutdown independently of legacy event handling.
- [ ] Add exact Task 11/13 removal-trigger documentation in rustdoc and current-state docs.
- [ ] Run app tests, offline mock, MCP smoke, live tests, and strict Clippy.
- [ ] Commit `refactor(app): quarantine legacy engine beside live shard runtime`.

### Task 8.9: Integrate manifests, lockfile, and evidence

**Files:**

- Modify `crates/market-squawk-live/Cargo.toml`
- Modify `apps/market-squawk/Cargo.toml`
- Modify root-owned `Cargo.lock`
- Update `.superpowers/sdd/progress.md` only after verification
- Update architecture/gap/current-state documents with exact implemented status

- [ ] Add live production dependencies `arc-swap.workspace`, `serde.workspace`,
  `tokio.workspace`, and `tokio-util.workspace`; retain source/domain dependencies and inherited
  lints. Add app dependencies on live and sources only where the composition root consumes them.
- [ ] Regenerate the single integrated root lockfile after Task 7 and every Task 8 manifest are
  present. Review the direct/transitive diff and update the exact duplicate dependency inventory
  only when the resolved graph proves it necessary.
- [ ] Run `cargo metadata --locked`, boundary checks, brand checks, generated-artifact checks, and
  `git diff --check` before the full gate.
- [ ] Run every required locked workspace command and security audit listed below.
- [ ] Commit the exact lockfile/evidence update separately.

---

## Manifest and lock implications

`crates/market-squawk-live/Cargo.toml` must add:

```toml
[dependencies]
arc-swap.workspace = true
market-squawk-domain = { path = "../market-squawk-domain" }
market-squawk-sources = { path = "../market-squawk-sources" }
serde.workspace = true
tokio.workspace = true
tokio-util.workspace = true
```

Existing `crc32fast`, `rust_decimal`, `sha2`, and `thiserror` dependencies remain. Tokio is required
for bounded `mpsc`, owned byte permits, actor tasks, time, and `JoinSet`; Tokio Util is required for
hierarchical cancellation; ArcSwap is the snapshot value store; Serde is only for bounded
authority-free snapshot/health DTOs.

`apps/market-squawk/Cargo.toml` adds path dependencies on `market-squawk-live` and
`market-squawk-sources` for real runtime composition and source-registry control. It must not gain
access to crate-private issuer/owner/cell types.

The root lock must be generated once after all Task 5-8 manifests are integrated. The currently
committed lock's absence of ArcSwap despite the platform manifest is already known; no Task 8 lane
should commit a divergent lock. Locked verification starts only after root regenerates and reviews
that union.

## Compile-risk ledger

| Risk | Evidence | Required resolution |
| --- | --- | --- |
| Pre-feed source lease is absent from integrated root | `ValidatedCurrentSourceAuthority` has no owned-lease method | Add/test the exact allocation-bound `try_current_lease` seam before Task 8 source supervision |
| Batch lease getter and stream-key hash are only uncommitted in Task 7 | Task 7 diff adds both | Integrate them with Task 7 and retain non-Serde/transplant tests |
| Generation invalidator is currently minted through `&mut InstrumentLiveProcessor` | In-progress `register_generation` owns a per-processor map | Extract bounded generation registry so control-plane bind and actor capability share one exact allocation |
| One-way authority dimensions are interchangeable | Near-completion candidate uses `OneWayLease` for generation, status, shard, and runtime, including inside `ProcessorLivenessBinding` | Introduce distinct marker-typed owners/leases and compile-time constructor-swap coverage |
| Processor liveness is injected but not statically dimension-safe | Near-completion `ProcessorLivenessBinding` accepts two values of the same lease type | Inject exact Task 8 `ShardLivenessLease` and `RuntimeIncarnationLease` types |
| Snapshot seed exists but cannot yet satisfy the frozen snapshot contract | Candidate seed has sorted stream/status/book/generation/revision data but lacks health epoch, snapshot-origin revision, observed/evaluated times, configured/output depth metadata, and checked retained bytes | Complete the bounded crate-private seed before Task 8 snapshot publication |
| Pre-feature/pre-strategy recheck is implicit rather than callable by actor | `issue` rechecks, but future work can occur before it | Add one sealed-clock `validate_applied_current` method and call at both boundaries |
| Task 7 is still changing and was not a clean committed compile target during preflight | New live crate and source edits are uncommitted | Re-read exact Task 7 head/signatures immediately before Task 8 implementation; do not code against this snapshot blindly |
| Legacy engine removal conflicts with current app consumers and staged adapter/service ownership | `main`, `mcp`, `replay`, `lib`, tests consume `Engine`; Task 11/13 own replacements | Apply the approved diagnostic compatibility quarantine and exact Task 11/13 deletion trigger; never forge a current batch |
| Committed lock is stale | Locked baseline exits 101 | Root regenerates one integrated lock after Task 8 manifests; all acceptance commands use `--locked` |
| Tokio byte permit API narrows to `u32` | Local Tokio 1.52.3 source | Reject capacities/messages above `u32::MAX` before semaphore construction/acquisition |
| ArcSwap does not itself bound old `Arc` retention | Slow readers can retain historical DTOs | Official reader returns non-Clone lease guarded by a bounded retained-reader permit |

## Quarter 2 acceptance and review checkpoint

Formal review begins only after Tasks 5-8 are integrated into one exact root commit. Two independent
reviewers should inspect the same commit: one for authority/concurrency/lifecycle, and one for
routing/memory/snapshot/app boundaries. Remediate all Critical, Important, and Minor findings before
the checkpoint is approved.

Run fresh on the exact corrected head:

```bash
cargo fmt --all --check

cargo clippy \
  --workspace \
  --all-targets \
  --all-features \
  --locked \
  -- \
  -D warnings

cargo test \
  --workspace \
  --all-features \
  --locked

cargo build \
  --workspace \
  --all-features \
  --release \
  --locked

RUSTDOCFLAGS='-D warnings' cargo doc \
  --workspace \
  --all-features \
  --no-deps \
  --locked

python3 scripts/check_brand.py
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
python3 scripts/check_generated_artifacts.py
python3 scripts/smoke_mcp.py
gitleaks dir --redact --no-banner .
cargo deny check
git diff --check
```

The checkpoint evidence must state the exact commit, toolchain, resolved Tokio/ArcSwap versions,
test counts, lockfile diff disposition, memory configuration used in adversarial tests, and every
review remediation commit. It must make no throughput or latency claim; measured performance
belongs to the later benchmark stage.

## Self-review

- Spec coverage: this map covers versioned routing, count and byte bounds, exact-generation
  synchronous invalidation, actor ownership, authority rechecks, snapshots, notifications, memory,
  startup/shutdown/replacement, app composition, manifest/lock integration, and the Quarter 2
  review gate.
- Placeholder scan: the map contains no `TBD`, implementation placeholder, unchecked "handle edge
  cases" instruction, or reference to an undefined neighboring interface.
- Type consistency: source lease flows from registry to generation registry to bound ingress to
  processor/capability; runtime/shard leases flow from Task 8 owners into every processor; snapshots
  contain data-only DTOs and never flow back into authority.
- Safety: no evasion, concealment, quota bypass, risk bypass, replay promotion, or legacy-event
  promotion is present.
