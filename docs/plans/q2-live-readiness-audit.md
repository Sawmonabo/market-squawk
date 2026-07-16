# Q2 live-runtime readiness audit

**Audit date:** 2026-07-16  
**Frozen root reviewed:** `4c8d72cbf01179e568b2d2dff4b71aab5ad06ffb`
**Disposition:** Task 7 is implementation-ready after the Task 5 P0 contracts below are closed;
Task 8 remains blocked on Task 7's lease and capability linearization contracts.

This preflight reviewed the Task 5/6 work in progress, the controlling Stage 1 plan, the domain
identity and live-evidence APIs, and the locally resolved Tokio 1.52.3, ArcSwap 1.9.2, and UUID
1.24.0 sources. It made no production-code changes. The controlling plan and local Task 7/8 briefs
were corrected after the audit.

## Verified deterministic routing vector

Routing V1 hashes these exact bytes:

```text
4d53514b5348415244010008636f696e62617365018f0000000070008000000000000001
```

The encoding is ASCII `MSQKSHARD`, version byte `01`, big-endian venue UTF-8 byte length `0008`,
the bytes for `coinbase`, and the UUID's exact 16 RFC/network-order bytes. FNV-1a 64-bit with offset
`0xcbf29ce484222325` and prime `0x00000100000001b3` produces
`0x28edee9cb1852659`; modulo 16 is shard 9.

V1 must hash `VenueId::as_str().as_bytes()` and `InstrumentId::as_uuid().as_bytes()` directly. It
must not use Display/Serde UUID text, native-endian integers, locale transforms, Unicode
normalization, `DefaultHasher`, or a dependency-defined unstable hash. Routing version and shard
count define an immutable runtime incarnation and are persisted in diagnostics and snapshots.

## Required contracts before Task 8

1. Task 5 must produce an owned, `Send + 'static`, non-Serde `CurrentDecodedProviderBatch` for actor
   ingress. It retains exact session/current-health/subscription/capture allocation identity. A
   borrowing proof, bare decoded batch, canonical event, or replay value cannot cross production
   ingress.
2. Capture uses a registry-owned one-way allocation per generation. Platform receives a non-Clone
   admission issuer and degradation-only handle. Its cloneable publisher cannot promote or rotate
   health. Successful admission returns a non-Serde receipt bound to exact frame evidence; all
   loss/failure paths synchronously degrade the allocation.
3. Shard mailboxes are bounded by admitted message count, retained bytes, and per-message bytes.
   A private command enum computes checked deep retained size. Count and byte permits are acquired
   without awaiting and retained through processing/discard.
4. Overflow cannot enqueue its own quarantine into the full mailbox. The bound ingress handle
   synchronously invalidates an exact one-way generation execution lease before returning any
   count-full, byte-full, overweight, checked-cost, or closed error.
5. Actors recheck current leases before state application and before features/strategy/issuance.
   Already queued commands for an invalidated generation are diagnostic-only and cannot mutate a
   replacement generation or produce action.
6. Task 7 capability issuance binds source-generation, capture/current-health, shard-liveness,
   runtime-incarnation, and checked state-revision allocations. Issue checks before and after nonce
   registration. Consume, risk, and dispatch recheck. Release invalidation and Acquire checks define
   the concurrency linearization boundary.
7. Snapshot publication uses crate-private `ArcSwap`, not Tokio `watch` as a value store. Tokio's
   watch receiver borrow holds a read guard that can block the producer. Optional notifications are
   independent bounded/coalescing hints.
8. The supervisor owns exactly the configured actors, gates ingress on complete startup, invalidates
   authority before every exit/shutdown, releases all permits, and joins or aborts-and-awaits every
   task to a deadline. No actor is detached.

## Snapshot and memory semantics

Snapshots are built completely from one committed owner state after the action decision and at a
bounded coalesced cadence. They include routing version/count, runtime incarnation, shard ID,
source/session generation, state/snapshot/health revisions, exact times, configured/state/output
depth, and dimension-specific completeness metadata. A cross-shard response reports a sorted
bounded revision vector; it does not claim a single-instant global `as_of`.

Peak live memory is documented as the sum of bounded instrument state, bounded mailbox bytes, one
bounded in-progress candidate, bounded snapshot construction, and a bounded trusted-reader
retention allowance. External services receive bounded DTOs, never snapshot cells, leases, issuers,
nonces, capabilities, or arbitrary subscription creation.

## Mandatory concurrency evidence

- Count-full, byte-full, overweight, checked-cost overflow, receiver closure, and permit release on
  every failure/cancellation/drop path.
- Invalidation before admission error return even when callers ignore the error; queued same-
  generation commands cannot reach strategy, issuer, risk, or dispatch.
- Deterministic barrier/model tests for issue, nonce registration, consume, risk, and dispatch
  interleaved with overflow, generation rollover, capture degradation, cancellation, and actor exit.
- Atomic complete snapshot N/N+1 observations, slow-reader nonblocking publication, exact
  truncation, and stale-snapshot inability to establish current authority.
- Partial-startup cleanup, unexpected actor exit, cancellation/receive races, shutdown permit
  release, deadline abort-and-await, and absence of detached tasks.

The stable V1 routing vector and single-writer actor direction are approved. Implementation is not
approved until the authority, byte-bounding, snapshot, and lifecycle contracts above exist and are
covered by the specified adversarial evidence.

## Task 5 P0 prerequisites for Task 7

The Task 5 worktree compiled and its complete source-crate test suite passed during this audit. Its
decoder now has bounded typed book/trade/quote payloads, exact decimal lexemes, typed checksum book
scope, independent market/transport/source/clock limits, and checked retained-byte accounting. The
following contracts still block a correct Task 7 implementation:

This is a frozen preflight finding list. Later worktree changes may close individual findings, but
acceptance is determined by the required consuming API and tests below, not by deleting the
historical finding.

1. The latest `crates/market-squawk-sources/src/registry.rs::validate_decoded_batch_owned` now
   retains a `Box<[ValidatedLiveScope]>` aligned with observations, but the shape is not yet a safe
   hot-path handoff. `ValidatedLiveScope` does not expose its newly retained quality ceiling/static
   authorization/static coverage, is not consumable with the observation as one intact value, and
   does not provide a compact cloneable validation-only lease for a capability that outlives the
   large batch. It also clones the entire `SourceCoverage`—including a potentially 4,096-instrument
   vector—once per observation. `CurrentDecodedProviderBatch::retained_bytes` charges only the
   decoded batch, not the boxed authorities or their deep metadata clones. Replace this with shared
   compact per-scope policy and exact retained-byte accounting; Task 7 must be able to construct the
   complete Task 4 policy/coverage evidence without retaining the frame-sized batch.
2. `crates/market-squawk-sources/src/decoder.rs::ProviderObservationPayload` is not yet exhaustive
   with respect to the canonical domain constructors. Typed trade aggressor evidence is now present.
   Auction still lacks a canonical phase and requires a non-optional paired quantity; halt and
   instrument status retain only opaque provider status text; corporate action lacks effective time
   and typed action. The source-specific decoder must interpret provider codes. The generic live
   crate must never string-match provider status/action values.
3. `DecodedProviderBatch::try_new` permits an empty batch and observations for multiple
   `(venue, instrument)` keys. Task 8 routes on that exact pair, so registry validation must produce
   bounded, nonempty, homogeneous current batches before ingress. A multi-instrument wire frame must
   be grouped without losing wire order within a key, exact frame/capture identity, or per-observation
   live scope.

Resolved during the audit: freshness now has independent market, transport, source, future-skew,
and connection-idle bounds; trade retains typed aggressor evidence; capture admission now explicitly
means successful bounded queue enqueue rather than a disk acknowledgement; and the capture receipt
also binds the exact frame ID.
5. Session binding, receive time, and payload digest do not uniquely identify one raw frame. The
   current session must assign a checked, nonzero, never-reused frame ordinal within each connection
   generation. `RawMarketFrame`, decoder evidence, and the admission receipt retain that exact
   frame identity. Ordinal exhaustion terminally invalidates the generation before returning an
   error; two byte-identical frames received at the same timestamp cannot exchange receipts.
6. `SourceHealthSnapshot` is a Serde audit DTO and must never be accepted directly to establish
   current authority. A non-Serde, process-local health update/reporting proof binds the exact
   session allocation, metadata freshness policy, and observation. The registry consumes that proof
   and may emit a snapshot for audit. Deserializing a healthy-looking snapshot cannot establish or
   restore current health authority. All five freshness limits must equal current metadata, and a
   source timestamp tolerated within future skew cannot extend real validity past the conservative
   receive/observation-time deadline.

No remaining root-domain P0 was found at `4c8d72c`. In particular, exact tick/lot conversion,
current-versus-snapshot book binding, independent timing limits, checksum targets, relational
qualification, and audit-only provenance provide the primitives Task 7 needs.

## Required Task 5 consuming API

These are opaque, non-Serde production types. All fields and constructors remain private to the
source registry. `CurrentLivePolicy` is a compact validated projection; it must not clone an entire
4,096-instrument coverage declaration into every observation. Shared allocations are immutable and
their deep retained bytes are charged conservatively by each routed batch.

```rust
// Signature contract; implementation bodies are intentionally omitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentBatchKey {
    venue: VenueId,
    instrument: InstrumentId,
}

impl CurrentBatchKey {
    pub fn venue(&self) -> &VenueId;
    pub fn instrument(&self) -> InstrumentId;
}

#[derive(Debug)]
pub struct CurrentDecodedProviderBatches { /* bounded, private */ }

pub struct CurrentBatchIter { /* private, bounded */ }

impl IntoIterator for CurrentDecodedProviderBatches {
    type Item = CurrentDecodedProviderBatch;
    type IntoIter = CurrentBatchIter;
}

#[derive(Debug)]
pub struct CurrentDecodedProviderBatch { /* nonempty and homogeneous */ }

impl CurrentDecodedProviderBatch {
    pub fn key(&self) -> &CurrentBatchKey;
    pub fn retained_bytes(&self) -> Result<usize, DecodeError>;
    pub fn into_observations(self) -> CurrentObservationIter;
}

pub struct CurrentObservationIter { /* private, bounded, wire ordered */ }

impl Iterator for CurrentObservationIter {
    type Item = CurrentProviderObservation;
}

impl ExactSizeIterator for CurrentObservationIter {}

#[derive(Debug)]
pub struct CurrentProviderObservation {
    /* exact observation + exact per-observation policy + current lease */
}

impl CurrentProviderObservation {
    pub fn key(&self) -> &CurrentBatchKey;
    pub fn stream_key(&self) -> &CurrentStreamKey;
    pub fn observation(&self) -> &ProviderNormalizedObservation;
    pub fn policy(&self) -> &CurrentLivePolicy;
    pub fn current_lease(&self) -> &CurrentSourceAuthorityLease;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentStreamKey {
    /* source, venue, instrument, provider product, and provider channel */
}

#[derive(Clone, Debug)]
pub struct CurrentSourceAuthorityLease { /* private Arc allocation identity */ }

impl CurrentSourceAuthorityLease {
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError>;
    pub fn binding(&self) -> &FrameSessionBinding;
}

#[derive(Clone, Debug)]
pub struct CurrentCoveragePolicy { /* private compact exact-scope projection */ }

impl CurrentCoveragePolicy {
    pub fn source_id(&self) -> &SourceId;
    pub fn venue(&self) -> &VenueId;
    pub fn provider_product(&self) -> &ProviderProduct;
    pub fn provider_channel(&self) -> &ProviderChannel;
    pub fn event_class(&self) -> LiveEventClass;
    pub fn depth(&self) -> Option<MarketDepth>;
    pub fn delay(&self) -> CoverageDelay;
    pub fn consolidation(&self) -> CoverageConsolidation;
    pub fn effective_from(&self) -> Timestamp;
    pub fn effective_until(&self) -> Option<Timestamp>;
    pub fn metadata_revision(&self) -> &MetadataRevision;
}

#[derive(Clone, Debug)]
pub struct CurrentLivePolicy { /* private compact projection */ }

impl CurrentLivePolicy {
    pub fn quality_ceiling(&self) -> DataQuality;
    pub fn authorization_grant(&self) -> &AuthorizationGrant;
    pub fn authorization_health(&self) -> &AuthorizationHealth;
    pub fn coverage(&self) -> &CurrentCoveragePolicy;
    pub fn coverage_health(&self) -> &CoverageHealth;
    pub fn live_rule(&self) -> &LiveCoverageRule;
    pub fn protocol(&self) -> &LiveProtocolProfile;
    pub fn freshness(&self) -> FreshnessPolicy;
    pub fn valid_until(&self) -> Timestamp;
    pub fn universe_evidence(&self) -> Option<&ExactPayloadEvidence>;
}

impl ValidatedCurrentSourceAuthority<'_> {
    pub fn validate_decoded_batch_owned(
        &self,
        batch: DecodedProviderBatch,
        receipt: CaptureAdmissionReceipt,
    ) -> Result<CurrentDecodedProviderBatches, RegistryError>;
}
```

`validate_decoded_batch_owned` should consume one decoded frame and capture receipt, build a
`CurrentProviderObservation` from every `ValidatedLiveScope`, group by `CurrentBatchKey`, and return
`CurrentDecodedProviderBatches`. There is deliberately no public `into_parts` that separates a bare
observation from its policy and lease. Task 7 consumes the intact value and rechecks the lease at
apply and issuance time.

`CurrentBatchKey(venue, instrument)` is only the deterministic Task 8 routing key. Mutable Task 7
stream state is keyed by `CurrentStreamKey(source, venue, instrument, provider product, provider
channel)`, with revision/session/generation bound by the one-way current-authority lease. Two
authorized sources or channels routed to the same instrument shard must retain independent books,
sequence/snapshot state, health, and capability authority.

The remaining provider payload shapes should be equivalent to:

```rust
Trade {
    trade_id: SourceIdentifier,
    price: ProviderPrice,
    quantity: ProviderQuantity,
    aggressor_side: AggressorSide,
}
Auction {
    provider_code: SourceIdentifier,
    interpretation_rule: IntegrityRule,
    phase: AuctionPhase,
    indicative_price: Option<ProviderPrice>,
    paired_quantity: ProviderQuantity,
}
TradingHalt {
    provider_status: SourceIdentifier,
    interpretation_rule: IntegrityRule,
    transition: HaltTransition,
    reason: SourceIdentifier,
}
InstrumentStatus {
    provider_status: SourceIdentifier,
    interpretation_rule: IntegrityRule,
    status: TradingStatus,
}
CorporateAction {
    action_id: SourceIdentifier,
    interpretation_rule: IntegrityRule,
    effective_at: Timestamp,
    action: CorporateActionKind,
}
```

## Task 7 module and API layout

Keep the public surface small and split implementation files before they approach the project's
500–700 line guidance:

```text
src/lib.rs                    exports safe views/tokens and composes the processor
src/book.rs                   DepthLimit and pure scaled-book invariants
src/normalization.rs          exact provider decimal -> tick/lot conversion
src/integrity.rs              sequence, snapshot, timing, checksum, canonical digest
src/integrity/checksum.rs     closed supported canonicalizer/algorithm dispatch
src/state.rs                  generation transition table and atomic message application
src/qualification.rs          Task 4 binding/evidence/assessment/event construction
src/authority.rs              sole issuer and current-authority gate
src/authority/lease.rs        one-way generation/shard/state-revision leases
src/authority/nonce.rs        fixed-capacity nonce registry and reclamation
```

Only opaque authority tokens are public for Task 10. Processor, issuer, gate, clock, and applied
authority remain crate-private so Task 8 can use them without creating a dependent-crate minting
surface:

```rust
// Signature contract; implementation bodies are intentionally omitted.
pub struct LiveExecutionCapability { /* private; Send, !Sync, !Clone, non-Serde */ }
pub struct ConsumedLiveAuthority { /* private; moved into risk/approved order */ }

impl ConsumedLiveAuthority {
    pub fn validate_current(&self) -> Result<(), AuthorityError>;
    pub fn assessment_id(&self) -> &QualificationAssessmentId;
    pub fn binding(&self) -> &LiveEvidenceBinding;
}

pub(crate) trait TrustedClock: sealed::Sealed {
    fn now(&self) -> Result<ClockReading, ClockError>;
}

pub(crate) struct InstrumentLiveProcessor<C: TrustedClock> {
    /* definition, state, leases, issuer, nonce gate, trusted clock */
}

pub(crate) struct CurrentBatchCursor { /* private owned CurrentObservationIter */ }

impl<C: TrustedClock> InstrumentLiveProcessor<C> {
    pub(crate) fn accept_batch(
        &mut self,
        batch: CurrentDecodedProviderBatch,
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

pub(crate) struct AppliedLiveObservation {
    pub(crate) event: MarketEvent,
    pub(crate) assessment: QualificationAssessment,
    pub(crate) authority: Option<AppliedObservationAuthority>,
}

pub(crate) struct AppliedObservationAuthority {
    /* small source/generation/shard/state leases and exact committed revision */
}
```

`apply_next` exists so Task 8 performs `apply -> features -> strategy -> issue per intent` before
processing the next wire-order observation. Applying an entire multi-observation batch first would
make every earlier observation's state revision stale before its strategy runs. Production time is
read from a sealed wall-plus-monotonic clock. No production method accepts a caller-authored
evaluation instant, quality, deadline, assessment, status result, metadata object, or bare decoded
observation.

## Exact conversion and atomic application

For every price use
`PriceTicks::try_from_decimal(provider.value().decimal(), definition.tick_size())`; for every
quantity use
`QuantityLots::try_from_decimal(provider.value().decimal(), definition.lot_size())`. Snapshot,
quote, and trade quantities must be positive. Delta quantity zero is the only delete operation.
Inexact ratios, negative quantities, overflow, an instrument/key mismatch, duplicate snapshot
prices, invalid side ordering, or a crossed candidate quarantine the exact generation.

Snapshots build new bid/ask maps and provider checksum material off to the side. Deltas first
convert the complete message, then apply it under a bounded rollback journal that records every
original touched or depth-evicted level. Validate sequence, provider checksum over the candidate,
depth, and uncrossed state before discarding the rollback journal. Failure restores the last good
book, retains a bounded diagnostic failure, publishes Release invalidation, and enters
`Quarantined`; the rejected candidate is never externally visible. This avoids cloning the full
book per delta while preserving provider-message atomicity.

Checksum state must retain the exact provider decimal lexeme for every live level as well as the
scaled maps. Kraken canonicalization is defined over provider representations; a map containing
only ticks/lots is insufficient evidence. Configured retained depth must be at least the provider's
checksum level count. Resolve metadata to a closed internal canonicalizer once per stream; an
unknown algorithm/canonicalization/scope combination fails configuration or remains non-executable,
never falls back to an invented checksum.

The generation state machine is:

```text
Disconnected -> AwaitingSnapshot -> Synchronizing -> Healthy
       any integrity/precision/status/overflow failure -> Quarantined
Quarantined -> AwaitingSnapshot only through a new source connection generation/allocation
```

The one-way allocation is never reset or reactivated. Heartbeats update connection liveness only;
the latest successfully applied market observation controls market freshness. A checked state-
revision overflow invalidates and quarantines before wrap. The last good diagnostic state remains
readable but cannot issue authority.

## Evidence construction order

One observation follows this order without I/O, SQLite, DataFusion, Parquet, Python, MCP, or an
unbounded write:

1. Acquire-revalidate source/current-health/capture, shard, runtime, and generation leases.
2. Verify batch key, frame binding, policy scope, instrument definition, and current generation.
3. Convert all numeric payload fields exactly and create a complete candidate message.
4. Validate snapshot relationship and sequence progression against generation-owned state.
5. Apply the candidate transactionally; compute the provider checksum from candidate provider
   lexemes and the canonical state digest from versioned deterministic bytes.
6. Validate book consistency, effective trading status, timing, freshness, coverage, authorization,
   delivery, capture, and stream integrity.
7. Checked-increment the state revision and build `BookStateBinding`. A snapshot sets current and
   snapshot-origin identity/digest; a delta changes only current identity/digest.
8. Build `LiveEvidenceBinding`, `SourcePolicyAssessment`, `CoverageScope`, sequence/snapshot/checksum/
   timing evidence, all `BoundAssessment` values, and `QualificationAssessment` from the exact same
   binding.
9. Build recorded `LiveProvenance` referencing that assessment and then the typed `MarketEvent`.
10. Acquire-revalidate every lease and expected revision, commit once, and expose a private applied-
    observation authority only when the derived quality is `DirectVerified`, status is active, and
    the event class is explicitly execution-enabled.

The inclusive capability deadline is the minimum of timing's market/source deadline, static
authorization and coverage ends, runtime authorization/subscription/universe deadline, current
health deadline, and configured maximum capability lifetime. Generation, shard, incarnation, and
state leases add immediate revocation rather than invented clock deadlines. Stage 1 Coinbase
remains `DirectUnverified`, produces audit events only, and cannot receive a capability.

## Authority, nonce, and Task 8 linearization

Each capability retains the exact Task 5 current-authority lease, generation execution allocation,
shard-liveness allocation, runtime incarnation, state-revision lease and expected revision, binding
digest, assessment identity, inclusive wall deadline, monotonic deadline, fixed-capacity slot index,
slot epoch, and globally non-reused nonzero nonce. Its fields and constructor are private.

The nonce registry is a fixed-size startup allocation with an O(1) free list and a bounded
incremental expiry cursor. Issuance pops one slot, checked-advances nonce and slot epoch, records the
exact binding, then rechecks every lease and revision with Acquire. Any changed authority retires
the slot and fails. Nonce/slot counter overflow invalidates the issuer's generation and fails rather
than wrapping. No producer path scans nonce state.

Consumption moves the capability, verifies the exact issued slot/binding/deadline/leases/revision,
marks the slot consumed before returning, and performs a final Acquire recheck. A failed consumed
capability is retired and cannot be retried. `ConsumedLiveAuthority` remains non-Clone and moves
through Task 10 risk into the approved order; risk and final dispatch call `validate_current` again.

All invalidators are one-way and publish with Release. Validation loads use Acquire. Task 8 binds a
producer handle to one exact generation invalidator, so overflow needs no map lookup or collection
scan:

```text
try_publish success: validate -> acquire byte permit -> try_send(command + permit)
try_publish failure: Release-invalidate exact generation -> release permit -> return error
actor dequeue:       Acquire source/generation/shard -> apply
before strategy:     Acquire source/generation/shard/state revision
issue/consume:       Acquire before and after nonce transition
actor exit:          Release shard first -> Release all owned generations -> drain permits -> exit
```

An operation whose final Acquire linearizes before invalidation may complete that boundary, but the
next risk/dispatch validation observes the revocation. Any operation beginning after an overflow or
closed-ingress API has returned must observe the earlier Release. Runtime routing-version/count
changes allocate a new incarnation after invalidating every old shard and generation; old
allocations are never promoted.

## Concrete Task 7 test gate

- `tests/conversion.rs`: exact tick/lot tables, inexact ratios, negative/zero/overflow boundaries,
  and every provider payload variant.
- `tests/book.rs`: snapshot-before-delta, delete-zero, strict ordering, duplicates, depth, extrema,
  multi-change rollback, crossed candidates, last-good diagnostics, and snapshot-origin retention.
- `tests/book_properties.rs`: generated snapshots/deltas preserve ordering, bounds, extrema,
  delete semantics, atomicity, and no executable crossed/quarantined state.
- `tests/checksum.rs`: Kraken golden snapshots/deltas, top-N side ordering, exact lexemes, leading/
  trailing zero rules, deletion, insufficient retained depth, mismatch quarantine, and unknown
  canonicalizer refusal.
- `tests/state_machine.rs`: exhaustive transition table, no same-allocation reset, heartbeat versus
  market freshness, generation rollover, status/halt restriction, and revision-overflow quarantine.
- `tests/qualification.rs`: complete evidence mapping, checksum unsupported versus unchecked,
  coverage/delivery/authorization/capture failures, deadline intersection and `+1ns`, binding-
  dimension mutation, assessment provenance, and Coinbase non-eligibility.
- Authority unit tests: fixed-capacity exhaustion, bounded expiry reclamation, nonce/slot overflow,
  issue invalidation during registration, duplicate consumption, stale state revision, and exact
  source/generation/shard/runtime transplant rejection. Deterministic private clock/hooks remain
  unit-test-only and cannot mint production-accepted alternate token types.
- Task 8 linearization tests: barriers around issue/consume/risk/dispatch versus count/byte overflow,
  capture degradation, generation rollover, runtime replacement, and actor exit; Release must occur
  before admission error return.
- Trybuild: dependent code cannot construct, clone, serialize, deserialize, or field-update a
  capability; cannot substitute `QualificationAssessment`; and cannot access issuer, nonce, lease
  invalidator, applied authority, or a loose-time minting API.

Task 7 verification remains the controlling plan's locked test, Clippy, compatibility-test, and
`git diff --check` commands. Passing unit tests without closing the P0 Task 5 contracts above is not
implementation readiness.

## Task 6 lifecycle and journal acceptance additions

Positive capture control belongs only to the app-owned source supervisor. Adapter-facing capture
contexts carry an immutable generation key plus admission/degradation functionality; they cannot
initialize or rotate an allocation. A typed reconnect result returns control to the supervisor,
which advances the connection generation and creates a fresh context. Every supervisor exit path,
including normal completion, cancellation, adapter error, rotation failure, and task abortion,
Release-invalidates the active generation before returning or dropping.

Journal collection limits are not writer-reopen limits. Startup validation is streaming and must
not make a legitimate journal permanently unwritable merely because it exceeded the reader's
default 512 MiB aggregate collection cap. Reopen validation retains per-record, count, framing,
CRC, offset-overflow, final-symlink, locking, and torn-tail fail-closed checks while using a distinct
documented operational bound or streaming to checked EOF. Final and intermediate symlink
substitution tests exercise the capability-confined directory handles.
