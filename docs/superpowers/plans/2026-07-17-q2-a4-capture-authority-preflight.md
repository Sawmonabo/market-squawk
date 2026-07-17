# Q2 A4 Capture Authority Preflight

> Status: implementation blueprint only. No A4 implementation may begin until the replacement A3
> authority candidate is approved, integrated, and green on its full exact-head gate.

## Audit anchors

- Root audited at `e5808801df88b222a956edba8b1f8ec7ffad6866`.
- The active authority lane advanced during the audit through
  `a2a6e5c6609f2ea981cf2d8f8d60072a4a133d5f` and remained under remediation.
- These commits are audit anchors, not approval claims.
- Refresh every path, signature, call-site count, and retained-size formula against the approved A3
  integration commit before starting A4.

## Hard dependency barrier

A4 depends directly on two unresolved A3 contracts:

1. I02 must provide the sole session-global terminal authority latch and durable terminal writer.
   Trusted-time discontinuity must reuse that mechanism rather than introduce a competing store
   transaction or lock order.
2. I03 must close the reachable durability-memory graph. A4-TIME adds registry-specific continuity
   ownership, so the final charge must be refreshed at the frozen A4 seed.

The required start condition is therefore:

```text
approved A3 exact head
-> integrated root
-> focused and full replacement gates green
-> A4 path/interface refresh
-> mechanical platform split
-> frozen A4.0 contract seed
-> parallel TIME and MEM worktrees
```

## A4.0: required capture retained-size contract

Add a required, non-default method to
`crates/market-squawk-domain/src/capture.rs`:

```rust
fn checked_retained_bytes(&self) -> Option<usize>;
```

The value is the conservative complete retained charge of one capture authority generation/bundle,
applied once per queued capture record. It includes:

- Inline capability storage.
- Capacity-sensitive owned buffers.
- Every unique shared pointee and control block.
- Synchronization state.
- Binding and session identity.
- Lease, initializer, admission, and degradation state.
- A4-TIME continuity state after that field is introduced.

It excludes platform queue-record and frame storage, which the platform adds separately. `None`
means arithmetic overflow and must fail closed.

### Current implementation inventory

The seed must update all four implementations in one runnable commit:

1. `crates/market-squawk-sources/src/capture.rs` — `CaptureGenerationCapabilities`
2. `crates/market-squawk-platform/src/capture/diagnostic.rs` — `DiagnosticCaptureBundle`
3. `crates/market-squawk-platform/tests/capture_authority_bridge.rs` — `TestBundle`
4. `crates/market-squawk-domain/tests/capture_authority_contract.rs` — `TestBundle`

Add `CaptureAuthorityIdentity::checked_dynamic_retained_bytes()` in the domain capture module. It
must use exhaustive destructuring and charge the actual capacities of `source_id`,
`metadata_revision`, and `session_identifier`; `connection_generation` is inline.

### Production source formula before A4-TIME

Exhaustively destructure, without `..`:

- `CaptureGenerationCapabilities { binding, lease, initialization, admission, degradation }`
- `CaptureInitializationControl { lease, not_sync }`
- `CaptureAdmissionIssuer { binding, lease, not_sync }`
- `CaptureDegradationCapability { lease }`

Verify that every nested binding/lease shares the constructor's exact allocation. Any internal
ownership mismatch returns `None`. Charge each unique allocation once:

```text
size_of::<CaptureGenerationCapabilities>()
+ FrameSessionBinding shared allocation
  - size_of::<FrameSessionIdentity>()
  - conservative Arc control-block charge
  - SourceId capacity
  - MetadataRevision/SourceIdentifier capacity
  - SessionId/SourceIdentifier capacity
+ CaptureGenerationLease shared allocation
  - size_of::<CaptureGenerationState>()
  - conservative Arc control-block charge
```

A4-TIME extends the lease term with one unique continuity allocation and control block.

### Diagnostic formula

Exhaustively destructure the diagnostic bundle and charge:

```text
size_of::<DiagnosticCaptureBundle>()
+ dynamic capacities of bundle.identity
+ dynamic capacities of admission.identity (a distinct clone/allocation)
+ size_of::<AtomicU8>()
+ conservative Arc<AtomicU8> control-block charge
```

Initializer, admission, and degradation handles share the same atomic allocation. Test bundles may
use a checked declared-charge fixture, but must still destructure every field and support overflow.

### Make the contract an enforced seam

The seed must consume the method rather than merely compile it:

- Precompute the charge before consuming a bundle.
- Change `raw_capture_channel` to return
  `Result<(Publisher, Control, Writer), CaptureGenerationError>` with no compatibility overload.
- Add `CaptureGenerationError::RetainedSizeOverflow`.
- On overflow, consume and degrade the rejected bundle before returning, without creating or
  activating a channel.
- Store `complete_generation_retained_bytes` in every `GenerationCaptureState`.
- Have `GenerationCaptureState::try_new` add the bundle charge to its platform wrapper, Arc control
  block, and platform-owned `Arc<CaptureAuthorityIdentity>` pointee/control/dynamic capacity.
- Precompute and validate a rotation successor before publication. Degrade only a rejected
  successor; do not disturb the current generation.

The preflight found 37 `raw_capture_channel` call sites across these nine files; refresh the count
at implementation time and migrate every site in the seed without `unwrap` or `expect`:

- `apps/market-squawk/src/main.rs`
- `apps/market-squawk/tests/coinbase_source.rs`
- `apps/market-squawk/tests/source_supervisor.rs`
- `crates/market-squawk-platform/tests/capture_authority_bridge.rs`
- `crates/market-squawk-platform/tests/capture_authority_bridge/cases.rs`
- `crates/market-squawk-platform/tests/capture_authority_bridge/writer_cases.rs`
- `crates/market-squawk-platform/tests/capture_lifecycle.rs`
- `crates/market-squawk-platform/tests/capture_lifecycle/deadline_cases.rs`
- `crates/market-squawk-sources/tests/capture_bridge.rs`

### Required mechanical split

`crates/market-squawk-platform/src/capture.rs` was already 741 lines at preflight. Before adding the
seed behavior, move `CaptureMessage`, `QueueByteReservation`, `GenerationCaptureState`, publisher
errors, publisher implementation, and charge helpers into `capture/admission.rs` in a behavior-free
commit. Keep state/channel/health ownership in `capture.rs`; `control.rs` imports the new internals.

### A4.0 RED/GREEN contract

RED evidence:

1. A domain contract test calls `checked_retained_bytes` before it exists and fails with `E0599`.
2. A fake implementation omits the method and fails to compile, proving there is no default.
3. A bundle returning `None` makes initial construction and rotation fail and degrades only that
   bundle.

GREEN evidence:

- Exact production formula using maximum-capacity, short-length source/revision/session values.
- Exact diagnostic formula including both distinct identity allocations.
- All four implementations compile.
- Exact successful initial and successor precomputation.
- Successor overflow leaves the healthy predecessor current.
- Exhaustive destructuring provides new-field compile pressure.

## A4-TIME: source-owned trusted receipt time

Run this only after the A4.0 seed in a grouped source-only worktree.

### Module and file ownership

Create `crates/market-squawk-sources/src/authority_time.rs` and register it privately from `lib.rs`.
Move registry clock primitives out of `registry.rs`. Recommended private types:

- `RawRegistryClockSource`
- `RegistryMonotonicInstant(u64)`
- `TrustedRegistryTime { wall, monotonic }`
- `AuthorityTimeContinuity` / `AuthorityTimeContinuityState`
- `SealedRegistryClock`
- `TrustedReceiptObservation`

Use one short mutex-protected paired cursor. Wall and monotonic observations must compare and advance
at one linearization point. Source failure, unrepresentable time, cursor poison, wall rollback, or
monotonic rollback permanently latches the continuity generation before returning. Equal wall with
advancing monotonic is valid. A later larger observation cannot recover a latched generation.

Split session lifecycle from the 691-line `registry/catalog.rs` into
`registry/catalog/session.rs`. Put new clock tests in `registry/tests/time_cases.rs`; do not grow the
existing large temporal test module.

TIME owns source/live files only after the seed. It must not modify domain, platform, or application
production files.

### Receipt flow and invariants

1. Registry construction samples one paired observation, validates/opens A3 durability with its wall
   value, and anchors a new sealed continuity allocation to that same pair.
2. Session, capture, registry, health, current-batch, and registry-scoped budget capabilities retain
   that exact continuity allocation and include its latch in O(1) currentness checks.
3. Remove the caller-authored timestamp from `RawFrameFactory::try_frame` without overload:

   ```rust
   pub fn try_frame(
       &mut self,
       transport: TransportFrameKind,
       payload: Bytes,
   ) -> Result<RawMarketFrame, SourceError>;
   ```

4. The factory samples and seals the paired receipt, rechecks authority, consumes a never-reused
   ordinal, and embeds a private `TrustedReceiptObservation`. Public `received_at()` exposes only
   the trusted wall value.
5. Serialized/replayed frames have no private receipt and remain execution-authority-free.
6. Session validation requires the exact binding allocation, exact continuity allocation, a live
   latch, receipt at/after session start, and receipt no later than the current paired high-water.
7. Decoder evidence and capture admission receipts carry the opaque observation and continuity
   identity. Owned batch validation requires exact receipt/continuity agreement and revalidates it
   before grouping.
8. Replace raw `Instant` deadline comparisons with registry-owned monotonic values so clock
   generations cannot be mixed.

### Discontinuity and A3 terminal semantics

Every retained capability must reject after one latch: frame production, health, live scope,
capture admission, decoded/capture receipts, current/queued batches, registry mutation, later
connection generations, and registry-scoped budget availability.

The hot/live-adjacent path must not perform persistence I/O. Reuse A3 in two stages:

1. Immediately latch continuity and the shared A3 authority/session availability atomic.
2. Preserve the already-durable `InUse` marker and reject every later transaction and clean close.
3. Allow a later control-plane terminal flush only through I02's one central terminal writer; TIME
   must not introduce another store mutex/order or availability-restoring path.

A same-registry generation cannot recover. A new registry receives a new continuity allocation, and
a durable restart must still satisfy the saved high-water and unclean-run rules.

Keep registry continuity on registry-scoped budget wrappers/leases, not one process-global budget
allocation that could incorrectly share one registry's clock authority with another registry.

### TIME RED matrix

- Old three-argument `try_frame` signature is a compile failure; the new signature has a positive
  doctest.
- The embedded wall value equals the sealed internal sample; adapters have no timestamp input.
- Replayed, forged-missing, and wrong-continuity receipts reject.
- Wall and monotonic rollback compare against the latest high-water, latch permanently, and cannot
  recover after a later greater sample.
- Clock unavailability and cursor poison latch permanently.
- Buffered frames, live/current/queued capabilities, health, capture admission, and budget
  availability all reject after the same latch.
- A same-registry replacement generation rejects; a fresh ephemeral registry succeeds; a durable
  same-store restart rejects the `InUse` predecessor.
- A deterministic barrier test proves paired high-water observations cannot tear.
- Retained-size tests include the continuity allocation/control block exactly once.

The preflight found 23 `try_frame` calls across 12 files. Refresh and migrate the full inventory at
implementation time, deriving test timestamps from the returned frame or the private manual clock
rather than reintroducing caller-authored receipt time.

## A4-MEM: platform-owned capture admission

Run this in parallel with TIME from the frozen A4.0 seed, in a grouped platform-only worktree.

### Closed reservation formula

Compute in `capture/admission.rs` with checked arithmetic:

```text
queue_record_inline = size_of::<CaptureMessage<B>>()
frame_dynamic = frame.retained_bytes() - size_of::<B::Frame>()
complete_generation_bundle = active.complete_generation_retained_bytes
conversion_peak = exact platform-owned diagnostic conversion allocation

reservation = queue_record_inline
            + frame_dynamic
            + complete_generation_bundle
            + conversion_peak
```

The subtraction avoids double-charging a frame already stored inline in `CaptureMessage`. Reject if
the frame charge is smaller than its inline type, the bundle charge is invalid, or any arithmetic
overflows.

Hold `QueueByteReservation` through the complete `append_frame` call. Do not release at dequeue.
One blocked in-flight write plus every queued message must remain inside the same byte ceiling. One
RAII path must release on success, conversion/append/flush failure, cancellation, shutdown,
issuance race, full/disconnected channel, drain, handle drop, and pending-owner drop.

Cover the platform's own conversion overlap. The diagnostic conversion currently copies payload
twice; introduce a normalized internal constructor so it copies once, then charge the output
payload, copied source allocation/control, record inline state, and bounded UUID scratch retained
before the sink call.

MEM owns platform capture/admission/writer/raw-record code and platform/app capture tests only. It
must not edit source/domain production files after the seed.

### MEM RED matrix

- Exact-limit admission succeeds; one byte below fails and degrades.
- Arithmetic overflow in frame, bundle, or conversion terms returns the typed overflow.
- Capacity, not length, changes the identity charge.
- A blocked sink retains the in-flight reservation after receiver dequeue.
- Rotation through multiple generations with one retained record each charges every distinct
  generation.
- Multiple records from one generation deliberately charge the full shared generation per message;
  document this conservative overcharge.
- Every success/error/cancellation/shutdown/drop/drain path returns queued bytes to exactly zero
  with zero invariant failures.
- Successor overflow degrades only the successor and leaves the predecessor current.

## Parallel implementation and integration order

1. Approve and integrate A3; restore a clean root.
2. Commit the mechanical `capture/admission.rs` split with unchanged tests.
3. Implement and freeze A4.0, including the required trait, four implementations, fail-closed
   construction seam, all call migrations, and exact tests.
4. Create exactly two grouped worktrees from that seed:
   - `.worktrees/q2-a4-time` / `feat/q2-a4-trusted-time`
   - `.worktrees/q2-a4-memory` / `feat/q2-a4-capture-memory`
5. Run TIME and MEM concurrently under the ownership boundaries above.
6. Integrate TIME first and run domain/source/live focused gates.
7. Integrate MEM second and run platform/application focused gates.
8. Run the combined gate, transfer evidence, remove both clean worktrees normally, and prune
   metadata. Retain branches until the normal completion decision.
9. Refresh checkpoint truth, freeze one exact Q2 candidate, run the repository-wide gate, then run
   the grouped three-review Q2 checkpoint.

## Verification

Each lane runs formatting, all-target/all-feature tests, doctests, strict Clippy, and release builds
for its affected packages. The combined candidate additionally runs boundary and duplicate
dependency checks, locked metadata, rustdoc with warnings denied, `git diff --check`, and clean
status. The formal Q2 checkpoint then runs the complete exact-head gate defined in the master plan,
including `verify.sh`, Cargo deny/audit, Gitleaks, brand/generated-artifact checks, unchanged HEAD,
and three independent grouped reviews.

No lane or focused gate constitutes Q2 approval on its own.
