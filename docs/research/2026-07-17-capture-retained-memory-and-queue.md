# Capture retained-memory accounting and queue architecture

Date: 2026-07-17

Status: Q2 A4 Wave 0 decision research; not implementation approval

Exact audit base: ab3f7c19000884357c38702edf6b4acc6a80c483

Target: aarch64-apple-darwin

Rust: 1.97.0, commit 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3

## Table of contents

- [Decision](#decision)
- [Execution base and optional hosted evidence](#execution-base-and-optional-hosted-evidence)
- [Scope and proof boundary](#scope-and-proof-boundary)
- [Evidence classes](#evidence-classes)
- [Version and repository anchors](#version-and-repository-anchors)
- [Current retained-memory defects](#current-retained-memory-defects)
- [Closed capture-memory model](#closed-capture-memory-model)
- [Authority-bundle contract](#authority-bundle-contract)
- [Frame and payload contract](#frame-and-payload-contract)
- [Complete generation construction](#complete-generation-construction)
- [Queue alternatives](#queue-alternatives)
- [Platform-owned safe ring](#platform-owned-safe-ring)
- [Reservation lifetime and accounting integrity](#reservation-lifetime-and-accounting-integrity)
- [Conversion, journal, and sink bounds](#conversion-journal-and-sink-bounds)
- [Typed failure taxonomy](#typed-failure-taxonomy)
- [TDD matrix](#tdd-matrix)
- [Implementation DAG and grouped worktrees](#implementation-dag-and-grouped-worktrees)
- [Verification and performance evidence](#verification-and-performance-evidence)
- [Re-audit gates](#re-audit-gates)
- [Sources](#sources)

## Decision

Q2-I08 requires a closed structural retained-memory model, not only a byte counter around occupied
messages. The hardened design is:

1. Add required, non-default, `Result`-returning retained-size methods to every capture authority
   bundle and raw frame. Arithmetic overflow and invalid authority graphs are distinct domain
   errors; platform detection of an underreported retained graph is a third, distinct failure.
2. Normalize both production frame implementations and committed `RawCaptureRecord` payloads to a
   deliberate application-owned `Arc<[u8]>` representation, with an explicit empty form and one
   checked pinned-layout helper at the domain memory seam. Preserve the committed compatibility
   ceiling while live frame owners retain the stricter 4 MiB limit, and expose an ownership-preserving
   frame payload accessor so conversion does not copy.
3. Separate fixed capture infrastructure, RAII resident-generation tokens, and dynamic record
   reservations. Health events share the resident-generation token; there are no health-event or
   generation-transition reservation categories. Expose components only through a bounded coherent
   transition/epoch snapshot, never unrelated atomic loads.
4. Replace the opaque standard-library channel at this authority boundary with a narrow,
   platform-owned, fixed-capacity safe ring whose actual backing capacity is known and charged.
   Requested logical depth is distinct from allocator-observed capacity. Sender count, close, and
   shutdown wake state are mutex-authoritative; producer duplication is fallible.
5. Keep each record reservation alive through conversion, sink append, policy-driven flush, and
   every error, cancellation, shutdown, and drain path.
6. Remove the journal's payload-scale serialization vector through deterministic two-pass
   streaming.
7. Make every retaining sink explicitly bounded and separately accounted, and replace the growing
   process-global destination map with a fallibly preallocated process-lifetime registry ledger.
8. Carry `capture_queue_capacity` and `capture_memory_ceiling_bytes` through the validated library
   policy and the normal defaults/file/environment/CLI configuration precedence, removing the
   misleading journal-capacity name without a legacy alias.
9. Fail closed with typed errors for arithmetic overflow, invalid ownership graphs, infeasible
   fixed storage, allocation failure, queue saturation, queue contention, closure, poison, and
   accounting-integrity failure.

No evaluated third-party queue exposes the stable retained-allocation contract required at this
boundary. A mature queue implementation can provide strong concurrency behavior, but its private
allocation layout remains outside Market Squawk's checked memory contract. For this specific
authority boundary, a small safe owned ring is preferable. It adds no repository unsafe code and
no dependency.

This decision is conditional on measured performance. The mutex-based ring preserves nonblocking
publication by using try-lock behavior; it must meet the project's live-path target on documented
hardware before Q2 approval. No performance claim is made by this report.

## Execution base and optional hosted evidence

This report is independently useful Wave 0 research. It is not approval of A4 or Q2-I08. The exact
audit base is the locally approved A3 production tree recorded in
[project memory](../project-memory.md): `ab3f7c19000884357c38702edf6b4acc6a80c483` passed the clean
local exact-head verifier and independent exact-hash review with no unresolved finding. The report
still must be refreshed after Wave 0 documentation is integrated because paths, interfaces, and
the lock graph can change even when the production tree does not.

The Q2 A4 preflight requires:

~~~text
locally approved A3 production tree at ab3f7c1
-> reviewed and integrated Wave 0 documentation descendant
-> mandatory A4 path/interface/evidence refresh
-> mechanical platform split
-> clean standard-channel code/harness A4_BASELINE_CODE_HEAD
-> reviewed baseline report A4_BASELINE_EVIDENCE_HEAD naming the measured code head
-> frozen fixed-ring/final-API A4_SEED_HEAD
-> parallel TIME and MEM worktrees
~~~

GitHub Actions run
[29564138664](https://github.com/Sawmonabo/market-squawk/actions/runs/29564138664) assigned no
runner and executed no checkout or project step because of an account-level billing/spending
condition. It is an optional hosted-portability evidence gap, not an implementation or approval
barrier and not a Market Squawk test result. Market Squawk has no mandatory cloud-service
dependency. The clean local exact-head gate remains mandatory, and neither local nor hosted
evidence may be inferred from the other.

Before implementation begins, the integration owner must refresh the reviewed and integrated Wave
0 descendant against the approved production anchor and record whether its production tree still
matches `ab3f7c1`. The refresh covers:

- paths and line anchors;
- trait and constructor signatures;
- bundle and frame implementation inventories;
- queue and writer ownership;
- all size formulas;
- the current Cargo lock graph;
- the 39 current raw-capture-channel invocations;
- the 23 current raw-frame-factory invocations;
- the ten direct publisher clones, `CaptureContext` derive, two positive publisher-`Clone` static
  assertions, and 18 unbounded/default memory-sink constructions;
- a fresh, clean local exact-head baseline; and
- optional hosted evidence separately, if a runner becomes available.

The base commit is both this report's audit anchor and the locally approved A3 production tree. It
does not approve this report, its future implementation, or a later documentation descendant.

## Scope and proof boundary

This document defines conservative structural retained-byte accounting for:

- the compiled target;
- Rust 1.97.0;
- the exact locked dependency versions;
- the repository ownership graph;
- configured queue and payload bounds; and
- the capture channel, generation, frame, conversion, writer, journal, and retaining-sink
  lifetimes.

It does not claim byte-exact allocator or operating-system memory use.

Structural retained bytes include Rust-visible inline storage, observed collection capacity,
modeled shared-allocation pointees and control blocks, and bounded simultaneously live conversion
objects. Structural accounting deliberately excludes unknown allocator metadata, size-class
rounding, fragmentation, guard pages, thread stacks, executable mappings, unrelated process
allocations, and operating-system page residency.

Accordingly:

~~~text
structural retained-byte ceiling != allocator usable size != RSS
~~~

RSS measurement is useful supplemental regression evidence. It cannot replace the ownership
formula because RSS includes unrelated and shared pages, while an allocator may retain freed pages
or round an allocation above the Rust-visible capacity. Conversely, a structural formula can
prove that the application cannot retain an unbounded number of owned values even when an RSS
sample happens to remain low.

The formula charges a complete generation once for every resident generation identity and shares
that RAII charge across state, records, messages, snapshots, health events, and identity-bearing
errors. Record reservations cover only proven unique frame dynamic and conversion-peak bytes. Within
one authority bundle graph, each unique shared allocation is counted once and ownership mismatches
fail closed.

## Evidence classes

Claims in this report use four evidence classes.

| Class | Meaning | Permitted use |
| --- | --- | --- |
| Normative API guarantee | Behavior documented by Rust 1.97 or an exact crate version's public API | Stable input to the formula for the pinned release |
| Pinned implementation fact | Private layout or allocation behavior inspected in the exact Rust or crate source | Current formula input with a mandatory version-change re-audit |
| Conservative structural inference | Worst-case coexistence derived from the repository's ownership and lifecycle graph | Admission and construction ceiling for the audited design |
| Supplemental measurement | Compiled size, capacity, latency, throughput, or process-memory observations on a named fixture and host | Regression and acceptance evidence, never a portability proof |

Examples:

- Vec reservation providing at least the requested capacity is normative.
- The actual Vec capacity after allocation is observable and must be charged.
- Rust 1.97's private Arc header and standard-channel slot layout are pinned implementation facts.
- Charging every current, retired, prepared, or externally retained generation once until its final
  accounted identity handle drops is a conservative structural inference.
- A custom bounded latency collector's percentiles and peak RSS are supplemental measurements.

## Version and repository anchors

The pinned toolchain and exact lockfile at the audit base establish:

| Component | Version | Relevance |
| --- | --- | --- |
| Rust toolchain | 1.97.0 | Arc, Vec, Mutex, Condvar, and Layout behavior; pinned by `rust-toolchain.toml`, not `Cargo.lock` |
| bytes | 1.12.1 | Current source and diagnostic payload representation |
| Tokio | 1.52.4 | Existing asynchronous runtime and evaluated MPSC alternative |
| Loom | 0.7.2 | Existing deterministic concurrency-model test dependency |
| serde_json | 1.0.150 | Current journal record serialization |
| uuid | 1.24.0 | Diagnostic generation and event identifiers |

Crossbeam channel, Flume, Thingbuf, and Criterion are evaluated or planned components, not
dependencies in the audited lockfile. Criterion 0.8.2 is a proposed seed dev-dependency and must
not be described as locked until the manifest and lockfile change is integrated.

Repository implementation anchors:

- The required bundle and frame traits are in
  [domain capture](../../crates/market-squawk-domain/src/capture.rs).
- Capacity-sensitive domain strings are in
  [domain identity](../../crates/market-squawk-domain/src/identity.rs).
- The production source authority bundle and lease are in
  [source capture](../../crates/market-squawk-sources/src/capture.rs).
- The source frame, binding, and session identity are in
  [source live](../../crates/market-squawk-sources/src/live.rs).
- Bounded source bytes are in
  [source bounded values](../../crates/market-squawk-sources/src/bounded.rs).
- Platform channel, generation, reservation, and publisher state are in
  [platform capture](../../crates/market-squawk-platform/src/capture.rs).
- Rotation is in
  [capture control](../../crates/market-squawk-platform/src/capture/control.rs).
- Diagnostic bundle and frame code are in
  [capture diagnostic](../../crates/market-squawk-platform/src/capture/diagnostic.rs).
- Reservation release and conversion occur in
  [capture writer](../../crates/market-squawk-platform/src/capture/writer.rs).
- The raw record representation is in
  [raw record](../../crates/market-squawk-platform/src/raw_record.rs).
- Sink ownership is defined in
  [capture sink](../../crates/market-squawk-platform/src/capture/writer/sink.rs).
- Process-global destination fencing is in
  [capture destination](../../crates/market-squawk-platform/src/capture/writer/destination.rs).
- Journal framing is in
  [journal](../../crates/market-squawk-platform/src/journal.rs).
- Queue capacity configuration is in
  [platform configuration](../../crates/market-squawk-platform/src/config.rs).

At this base, there are:

- four CaptureAuthorityBundle implementations;
- exactly four RawCaptureFrameView implementations: production `RawMarketFrame` and
  `DiagnosticCaptureFrame`, platform `TestFrame`, and domain `TestFrame`;
- 39 raw_capture_channel function invocations across nine calling file groups; and
- 23 .try_frame method invocations across twelve calling file groups;
- ten direct publisher clone expressions, one `CaptureContext` `Clone` derive, and two positive static
  publisher-`Clone` assertions that must migrate to the fallible duplication contract; and
- 18 `MemoryCaptureSink::default`/`Default` call sites that must migrate to explicit bounded
  construction.

The counts are refresh inputs, not permanent architecture constants.

## Current retained-memory defects

### Authority bundle is not an enforced accounting seam

CaptureAuthorityBundle has no required checked retained-size method. Platform construction can
therefore consume a generation without proving that its complete source-specific authority graph
fits the memory ceiling.

### Source frame undercounts retained memory

RawMarketFrame currently charges:

- its inline size;
- source, revision, and session lengths; and
- payload retained length.

It omits or weakens:

- capacity instead of length for owned identity strings;
- the frame-session binding's shared pointee and control block;
- an explicit closed contract for Bytes shared backing; and
- typed arithmetic overflow.

### Diagnostic frame undercounts retained memory

DiagnosticCaptureFrame currently charges only its inline size and payload length. It omits dynamic
identity capacities and a closed payload allocation/control term.

### Queue count and queue bytes are disconnected

The platform currently creates std::sync::mpsc::sync_channel queues. The Rust 1.97 pinned
implementation creates a boxed array containing one slot per configured message at channel
construction. Its channel and reference counter have additional allocations.

Current configuration permits:

~~~text
default queue capacity = 16,384 messages
maximum queue capacity = 1,048,576 messages
capture dynamic byte budget = 64 MiB
~~~

An empty maximum-capacity queue can therefore retain a large slot array without consuming the
dynamic byte counter. The lower bound alone is:

~~~text
capacity * size_of::<queue slot>
~~~

At 1,048,576 slots, a 64-byte slot consumes 64 MiB before authority, payload, conversion, channel
core, synchronization, writer, health, or sink allocations are counted.

### Current publication formula double-counts one term and omits others

The current publisher adds the complete frame retained size to `size_of` the capture message. The
message already contains the frame inline, so the inline frame is double-counted. At the same time,
the unified account omits fixed channel backing, the separately resident generation graph, and
conversion overlap.

### Reservation is released before the bounded work ends

The writer explicitly drops QueueByteReservation before append_frame. The admitted bytes can
therefore return to the available counter while frame conversion, sink serialization, sink I/O,
and policy-triggered flush still retain the admitted record graph.

### Diagnostic conversion copies payload twice

Diagnostic conversion first copies the payload into Bytes, then RawCaptureRecord::try_new_live
normalizes or copies it again. Both allocations can overlap. The platform must either charge that
current peak during the migration and then replace both frame/record payloads with one shared
normalized `Arc<[u8]>` allocation so conversion introduces no payload copy.

### Journal creates a payload-scale vector

The journal uses serde_json::to_vec(record) before writing its framed body. That additional Vec can
approach the maximum serialized record size while the frame and converted raw record remain live.
It is outside the current proposed conversion formula.

### Public memory sink is unbounded

MemoryCaptureSink clones every appended CapturedRawRecord into an unbounded Vec. Thus the sink API
permits record allocations to remain retained after append returns. A reservation held only through
append does not bound that sink graph.

### In-band wake is not reliable at saturation

The current CaptureMessage includes a Wake variant. A full bounded channel can reject the wake
record needed to stop or reconfigure the writer. Closure and cancellation must be out-of-band
state plus notification, not an item competing for a full record slot.

## Closed capture-memory model

Each capture channel has exactly three accounting terms: fixed infrastructure, resident-generation
tokens, and record reservations. One authoritative checked total governs all channel admission.
Component counters are diagnostics only and must never be independently summed to make an admission
decision. The process-global destination registry has its own immutable process-lifetime fixed ledger
because no individual channel owns or may release its backing allocation; that separate ledger never
substitutes for or lends bytes to a channel total.

### Fixed capture infrastructure

Fixed bytes exist even with no resident generation and no record:

~~~text
fixed_capture_bytes
    = accounting_core_base_bytes
    + sum(live fixed-infrastructure reservations)

accounting_core_base_bytes
    = complete Arc<CaptureMemoryAccounting> allocation

channel_state_fixed_bytes
    = complete Arc<CaptureState<B>> allocation
    + complete Arc<QueueCore<CaptureMessage<B>>> allocation
    + record_slots.capacity() * size_of::<Option<CaptureMessage<B>>>()
    + complete Arc<QueueCore<CaptureHealthEvent>> allocation
    + health_slots.capacity() * size_of::<Option<CaptureHealthEvent>>()
    + complete Arc<WriterLifecycleCore> allocation

writer_start_fixed_bytes
    = observed capacities of fixed UUID/source/event-name scratch
    + complete destination-lease allocation and bounded destination identity
    + bounded writer-thread name allocation
    + pinned std::thread spawn-packet/JoinHandle control upper bound
    + any stable writer allocation not embedded in WriterLifecycleCore

destination_registry_process_bytes
    = size_of::<OnceLock<DestinationFenceRegistryInitializationState>>()
    + destination_slots.capacity() * size_of::<Option<DestinationFenceEntry>>()
~~~

`CaptureState` includes its inline lifecycle mutex, `ArcSwap`, completion accounting, lifecycle
state, bounded diagnostics, and fixed queue handles. `WriterLifecycleCore` coalesces the current
separate cancellation flag, shutdown-deadline mutex, completion notification, final-report mutex,
and other stable writer-control pointees into one closed allocation. Its clones are inline `Arc`
handles, not additional allocation charges. The writer-start fixed scratch term includes the UUID
generation and source/event-name buffers currently allocated during every conversion; they are
allocated once at writer start, never grow, and are charged at their observed capacities. An
implementation that retains separate writer-control `Arc` allocations instead must enumerate and
charge every one of them rather than hiding them under a generic writer term.

The sink value moved into the worker is covered by its own declared sink ledger, not by
`writer_start_fixed_bytes`. The pinned Rust 1.97 thread-spawn packet, `Thread`/`JoinHandle` shared
control, closure capture storage, and bounded thread-name allocation are one fixed writer-runtime
class whose conservative source-derived upper bound is recorded with the compiled target. The native
thread handle, kernel bookkeeping, and stack remain host/RSS evidence. If the seed cannot close the
Rust-owned thread-runtime upper bound for the pinned implementation, it must reject the structural
writer-memory claim rather than omit the class.

The destination-fence registry has a different owner and lifetime from an individual writer. Replace
its current process-global growing `HashMap` with an explicitly initialized, fallibly preallocated,
never-growing `Vec<Option<DestinationFenceEntry>>`. The requested logical entry limit remains 1,024;
`try_reserve_exact` is called for that request, exactly 1,024 logical slots are initialized, and the
observed `Vec::capacity()` is charged. Spare allocator capacity is never an additional logical entry.
The registry's process ledger is admitted against a separate nonzero process-infrastructure ceiling
before any capture channel or writer handle is published and remains charged for the initialized
registry's process lifetime. Entry removal clears one logical slot but never releases or reassigns the
fixed backing ledger. Individual writer-start receipts charge only their distinct destination lease;
they never borrow the registry ledger and never release it when a writer exits.

The one exact global owner is:

~~~rust
static CAPTURE_DESTINATION_FENCES:
    OnceLock<DestinationFenceRegistryInitializationState> = OnceLock::new();

enum DestinationFenceRegistryInitializationState {
    Ready {
        admitted_limits: CaptureProcessInfrastructureLimits,
        registry: Mutex<CaptureDestinationFenceRegistry>,
    },
    Failed {
        attempted_limits: CaptureProcessInfrastructureLimits,
        error: DestinationFenceRegistryPermanentInitializationError,
    },
}
~~~

Both variants are entirely inline in that `OnceLock`; there is no second static, `Arc`, retained
error allocation, or lazily allocated side ledger. A first initialization attempt is permanent. A
successful first attempt stores the checked process-byte result inside the registry diagnostics and
returns an allocation-free `CaptureProcessInfrastructure` containing only a private `&'static` proof
of the matching `Ready` state. A failed first attempt returns its stored typed permanent error and
can never publish that proof. A later call whose limits byte-for-byte equal `admitted_limits` or
`attempted_limits`, as applicable, returns the same proof or wraps the same stored failure in
`DestinationFenceRegistryInitializationError::Permanent`; different limits return
`AlreadyInitializedWithDifferentLimits`, including after a failed first attempt. The proof itself
owns no heap allocation. If a future proof handle retains storage, that storage must enter this
formula before the type changes.

Initialization computes and checks
`size_of::<OnceLock<DestinationFenceRegistryInitializationState>>()` before attempting the vector,
uses `try_reserve_exact`, resizes only to the 1,024 requested logical entries, and, for `Ready`, adds
the observed backing-capacity term. Arithmetic, fixed-budget, and allocation failures are stored in
`Failed`; no valid registry or process proof is published. Static failed-state storage is the inline
`OnceLock` term and has no vector-backing term. Stable Rust allocation for a later per-writer lease
`Arc` remains an explicit process-allocator OOM boundary; the design must not invent a recoverable
error for it.

Registry tests cover concurrent first initialization, successful and failed same-limit replay,
successful and failed different-limit refusal, requested-versus-observed capacity, exact/one-under
process ceiling, allocation refusal, 1,024 live destinations, one-over refusal, churn without growth,
removal without ledger release, terminal mutex poison, and the state after the final writer-start
reservation drops. `capture/writer/destination.rs` is serialized seed ownership because no parallel
lane can safely change this process-global authority.

~~~rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestinationFenceRegistryInitializationError {
    Permanent(DestinationFenceRegistryPermanentInitializationError),
    AlreadyInitializedWithDifferentLimits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestinationFenceRegistryPermanentInitializationError {
    ArithmeticOverflow,
    AllocationFailed { requested_entries: usize },
    FixedStorageBudgetExceeded { required: usize, limit: usize },
}
~~~

The `Vec` header is inline in `QueueState`, and observed capacity after `try_reserve_exact` is charged
because the allocator may expose more capacity than requested. The health queue uses the same
fixed-storage rule; a health event carries a shared accounted-generation handle and never creates an
event-specific reservation. Returned publisher, control, producer, consumer, writer, and pending-
writer handles contain inline fields and shared handles only; any future handle-owned heap allocation
requires a formula and re-audit.

The accounting-core base is installed before any other reservation and remains until the last
accounted identity or reservation drops. `CaptureState` owns the RAII
`channel_state_fixed_bytes` reservation; it releases that reservation when the final state/queue
owner drops even if an external accounted identity keeps the accounting core alive. Writer start
computes and admits `writer_start_fixed_bytes` against the same authoritative total before marking
the writer running or publishing its handle. Its RAII owner spans the worker, ordinary handle,
pending-reap owner, final report, and destination fence and releases only after the worker is joined
and the final lifecycle owner drops. Budget refusal is typed and leaves the generation uninitialized;
thread-creation failure releases the prepared fixed reservation. The one bounded OS thread's kernel
objects and stack are recorded as supplemental RSS/host evidence rather than misrepresented as Rust
heap-layout terms.

`WriterFixedStorageReceipt` is the only writer-start quote. It stores every observed UUID/source/
event-name scratch capacity, the complete destination-lease charge, bounded thread-name charge, the
pinned spawn-packet/closure/`Thread`/`JoinHandle` control upper bound, every other stable writer
allocation, the compiled target, and a proof-artifact hash. The proof artifact is persisted under
`docs/reports/performance/` and records the pinned Rust source revision, target, compiled type sizes,
closure-capture inventory, formula, and fixture hash. Writer start constructs every term, reserves
their checked sum once, and only then may spawn or publish health. Scratch refusal, destination
refusal, proof mismatch, thread-name refusal, or thread creation drops the prepared receipt. A
successful receipt remains owned through worker, ordinary handle, pending reap, final report, join,
and destination-fence teardown. Exact/one-under and forced-drop-order tests independently rebuild
each receipt term; no generic `writer overhead` term is accepted.

Sink-owned fixed buffers and records retained after `append` return belong to the sink's separately
declared budget. They are never funded by releasing or borrowing a channel record reservation.
The journal fixed sink formula is `size_of::<JournalWriter>() + journal_path.capacity() +
BufWriter::capacity()`. Construction checks the requested buffer and path bounds, creates the buffer,
reads both observed capacities, and publishes no journal sink when the checked fixed sum exceeds its
separate sink ceiling. The inline writer is charged once, not per append. The journal fixed sum
remains charged until journal-sink drop; it is not included in `fixed_capture_bytes` and cannot borrow
channel or memory-sink budget.

### Resident-generation tokens

Each complete generation is charged exactly once while any platform-owned or externally returned
handle can keep it reachable. The private ownership seam is:

~~~rust
struct AccountedGenerationIdentity {
    identity: CaptureAuthorityIdentity,
    _resident: ResidentGenerationReservation,
}

struct GenerationCaptureState<B> {
    accounted_identity: Arc<AccountedGenerationIdentity>,
    // authority bundle capabilities and other generation state
}

pub struct CaptureIdentitySnapshot {
    accounted: Arc<AccountedGenerationIdentity>,
}
~~~

`ResidentGenerationReservation` is acquired from the one accounting core before a generation can
become prepared or externally visible, moved into `AccountedGenerationIdentity`, and released only
by its `Drop`. Current state, retired state, prepared successors, record messages,
`CapturedRawRecord` wrappers, health events, control snapshots, and any public identity snapshot
carry clones of the same private `Arc<AccountedGenerationIdentity>`. Public APIs may borrow the
identity or return a typed accounted snapshot; they must not extract an unaccounted
`Arc<CaptureAuthorityIdentity>`.

`CapturedRawRecord`, health events, snapshots, and identity-bearing errors preserve their public
value semantics with explicit `Clone`, `Debug`, `Eq`, and `PartialEq` implementations over audit
identity and public evidence fields. Clone shares the accounted handle; formatting and equality do
not expose or compare accounting-core internals. The token is runtime ownership metadata and is
never serialized.

The resident token conservatively charges the complete generation graph even after only an
external identity handle remains. This intentional overcharge eliminates graph reachability races:
there is no manual predecessor release, no active-to-retired accounting conversion, and no
per-record generation charge. A prepared successor and its predecessor are both resident until
their own final accounted handles drop.

### Record reservations

Generic subtraction of `size_of::<B::Frame>()` from a complete frame total is invalid because a
frame may share some dynamic allocations with the already-admitted generation and retain other
dynamic allocations uniquely. The domain contract therefore exposes a checked decomposition:

~~~rust
pub struct CaptureFrameFootprint {
    inline_slot_funded_bytes: usize,
    resident_shared_bytes: usize,
    unique_frame_dynamic_bytes: usize,
}
~~~

The frame footprint declares the complete structural decomposition, but the frame is not the
authority that may subtract a resident-shared allocation. The active admission capability provides
the required, no-default pointer-proof seam:

~~~rust
pub trait CaptureAdmission<Frame> {
    // Existing receipt, preflight, issuance, and validation methods remain required.
    fn checked_resident_shared_frame_bytes(
        &self,
        frame: &Frame,
    ) -> Result<usize, CaptureRetainedSizeError>;
}
~~~

Production admission proves the frame's `FrameSessionBinding` is `Arc::ptr_eq` to the active
admission binding and returns the exact checked allocation. TIME extends the same exhaustive method
with the pointer-proven continuity allocation. Diagnostic admission returns zero. All production
and test admission implementations must implement the method explicitly; an omission compile
fixture prevents a permissive default from erasing the proof obligation.

All fields are private, checked at construction, and exposed through accessors. Their checked sum is
the frame's complete conservative structural footprint. `inline_slot_funded_bytes` must equal
`size_of::<Self>()`; a smaller or different report is platform underreporting. The fixed queue slot
funds that inline value. `resident_shared_bytes` is excluded from a record reservation only after the
active admission capability has proved that each advertised shared pointee is pointer-identical to an
allocation already covered by the active `AccountedGenerationIdentity`. Successful value equality is
not enough. The publisher requires
`admission.checked_resident_shared_frame_bytes(frame) == footprint.resident_shared_bytes`; an error
or mismatch refuses publication before reservation and is never converted into a smaller charge.
Any frame implementation that cannot obtain that active pointer proof reports zero resident-shared
bytes and includes the allocation in `unique_frame_dynamic_bytes`.

The publisher's order is mandatory:

1. load and retain the active `GenerationCaptureState`;
2. obtain the active admission lock and complete its binding/pointer preflight;
3. obtain the frame's checked footprint, call the required active-admission
   `checked_resident_shared_frame_bytes` proof, and require their exact equality;
4. reserve only the unique frame dynamic and conversion peak; and
5. enqueue the frame with that same retained active accounted identity.

Rotation after step 1 does not invalidate the accounting proof because the message retains that
exact accounted generation. A preflight mismatch refuses publication before any resident-shared
subtraction.

For `RawMarketFrame`, the inline frame is slot-funded, the complete shared
`FrameSessionBinding` allocation is resident-shared only after `Arc::ptr_eq` against the active
generation admission binding, and the nonempty `CapturePayload` allocation is unique frame dynamic.
For `DiagnosticCaptureFrame`, value-equal identity fields are distinct owned allocations rather than
pointer-shared generation allocations: `resident_shared_bytes` is zero and actual identity capacities
plus the nonempty payload allocation are unique frame dynamic.

The record reservation is then:

~~~text
record_reservation_bytes
    = footprint.unique_frame_dynamic_bytes
      .checked_add(conversion_peak_bytes)

frame_complete_bytes
    = footprint.inline_slot_funded_bytes
      .checked_add(footprint.resident_shared_bytes)
      .checked_add(footprint.unique_frame_dynamic_bytes)
~~~

The queue slot already owns the inline `CaptureMessage`, frame, accounted-identity `Arc` handle, and
reservation handle, so adding their inline sizes again would double-count fixed slots. The resident
token already owns proven shared generation allocations, so adding those to each record would
double-charge them. A malformed decomposition or total below its structural minima is the distinct
platform `RetainedSizeUnderreported` failure. Domain arithmetic overflow remains
`CaptureRetainedSizeError::Overflow`; a false or mismatched sharing claim is
`InvalidAuthorityGraph` or an authority binding mismatch, never a smaller charge.

The reservation is acquired before insertion and stays alive through dequeue, validation,
conversion, sink append, policy-driven flush, cancellation, failure, and drain. It does not include
the generation graph: the message and resulting `CapturedRawRecord` wrapper share its resident
token.

### Admission invariant

One atomic total is initialized with the accounting-core base and channel-state fixed reservation.
Later fixed writer-start reservations, resident tokens, and record reservations all admit with the
same checked compare-exchange against the same ceiling:

~~~text
authoritative_total_accounted_bytes
    = fixed_capture_bytes
    + sum(live resident-generation tokens)
    + sum(live record reservations)
    <= configured_capture_memory_ceiling_bytes
~~~

Construction fails before handle publication unless initial fixed storage and the initial resident
token fit. Writer start, successor preparation, and record publication use the same total. All
current, retired, prepared, message-held, event-held, record-held, error-held, and externally held
generation identities remain in the resident sum until final drop.

Diagnostics may expose `fixed_capture_bytes`, `resident_generation_bytes`,
`record_reservation_bytes`, `total_accounted_bytes`, and `accounting_invariant_failures`. These
components explain the total; they are never separate admission authorities. After record drain,
record reservations are zero. Resident bytes may remain for current or externally retained retired
generations. Writer-start fixed bytes disappear after joined lifecycle/fence teardown; channel-state
fixed bytes disappear after the final state owner; the accounting-core base remains until its final
reservation or accounted identity drops.

Those values are never exposed as unrelated atomic loads. `CaptureMemoryAccounting` also owns
`active_transitions: AtomicUsize`, monotonically checked `completed_epoch: AtomicU64`, and
`accounting_status: AtomicU8`, all inline in the already charged accounting-core allocation. The private
closed encoding is `Healthy = 0`, `TransitionOverflow = 1`, `EpochOverflow = 2`, and
`InvariantViolated = 3`; no other byte is accepted as healthy. The first terminal reason wins through
a checked compare-exchange and is durable for the accounting core's remaining lifetime. A later
failure may increment the checked invariant-failure counter but cannot erase or replace that first
reason.

All mutable accounting atomics—poison reason, transition count, completed epoch, authoritative
total, diagnostic components, and invariant-failure count—use `Ordering::SeqCst`; the configured
ceiling is immutable after core construction. This intentionally conservative ordering is part of the Q2 proof contract; a
weaker ordering requires a new documented Rust-memory-model proof, benchmark evidence, and Loom
re-review before it can replace the frozen protocol. Every fixed, resident, or record reserve/release:

1. loads and rejects a non-healthy poison reason;
2. enters with a checked SeqCst compare-exchange on `active_transitions` before the first byte-counter
   mutation, publishing `TransitionOverflow` instead of wrapping;
3. rechecks poison after entry, so a concurrent terminal failure cannot authorize another mutation;
4. performs the authoritative-total and matching component changes with checked SeqCst
   compare-exchange operations;
5. advances `completed_epoch` with a checked SeqCst compare-exchange only after the byte counters are
   mutually consistent, publishing `EpochOverflow` instead of wrapping; and
6. leaves with a checked SeqCst decrement. Zero-before-leave, component/total disagreement, checked
   arithmetic failure after entry, or an impossible RAII drop publishes `InvariantViolated`.

Once poison is published, no new admission or ordinary release transition begins. An already-entered
transition completes only the minimal checked reconciliation needed to leave; if it cannot preserve a
consistent total/component state, poison makes every snapshot and later admission fail closed. The
transition RAII guard has an explicit successful `finish` path; its `Drop` fallback performs only the
checked leave/poison sequence and cannot silently abandon an active count. These rules make overflow
and impossible-drop failures observable even though Rust `Drop` itself cannot return a `Result`.

The only public diagnostic read is bounded and coherent:

~~~rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureAccountingSnapshot {
    completed_epoch: u64,
    fixed_capture_bytes: usize,
    resident_generation_bytes: usize,
    record_reservation_bytes: usize,
    total_accounted_bytes: usize,
    accounting_invariant_failures: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureAccountingSnapshotError {
    Contended { attempts: NonZeroUsize },
    TransitionOverflow,
    EpochOverflow,
    InvariantViolated,
}

#[repr(u8)]
enum CaptureAccountingStatus {
    Healthy = 0,
    TransitionOverflow = 1,
    EpochOverflow = 2,
    InvariantViolated = 3,
}
~~~

Snapshot fields remain private and have read-only `const` accessors; callers cannot construct or
alter a value that did not pass the coherence proof.

`try_accounting_snapshot(max_attempts: NonZeroUsize)` never blocks and never retries beyond the
caller-supplied bound. Every load in one attempt is SeqCst and occurs in this exact order:

~~~text
poison_before
completed_epoch_before
active_before
fixed_capture_bytes
resident_generation_bytes
record_reservation_bytes
total_accounted_bytes
accounting_invariant_failures
configured_ceiling
active_after_components
completed_epoch_after
active_final
poison_after
~~~

An attempt immediately returns the exact durable `TransitionOverflow`, `EpochOverflow`, or
`InvariantViolated` reason when either poison read is non-healthy or contains an unknown encoding. It
accepts only when all three active reads are zero, both epoch reads match, both poison reads are
healthy, checked component addition equals the total, and the total is within the observed configured
ceiling. Any other nonterminal observation consumes one attempt and eventually returns
`Contended { attempts: max_attempts }`; it is never fabricated as an invariant failure. The final
active load closes the validation window around the epoch read, while the final poison load prevents
a concurrently discovered terminal failure from being reported as a healthy sample. A transition
that begins and completes entirely during the read changes the epoch, preventing total/component ABA
from being accepted. A transition that begins only after all byte components were read linearizes
after the coherent older snapshot; SeqCst ordering prevents it from being observed as a torn mixture.

All CLI, health, tests, reports, and benchmarks consume accepted `CaptureAccountingSnapshot` values;
none independently loads or reconciles component atomics. Sampling contention is reported separately
from accounting poison and cannot be relabeled as an invariant failure. A benchmark run with no
accepted structural snapshot is invalid. Deterministic barriers and Loom cover a writer transition
at every gap in the exact read sequence, concurrent fixed/resident/record reserve and release,
overlapping writers, a complete reserve/release ABA, transition and epoch overflow injection,
first-poison-reason durability, impossible guard drop, bounded retry exhaustion, and a final
post-drain accepted snapshot. The Loom model uses the same SeqCst operations and load order as
production; a reduced model with weaker or reordered operations is not acceptance evidence.

### Validated construction policy and application configuration

The library boundary accepts one invariant-preserving value:

~~~rust
pub struct CaptureChannelLimits {
    capture_queue_capacity: NonZeroUsize,
    capture_memory_ceiling_bytes: NonZeroUsize,
}
~~~

Both exact fields flow through `ConfigOverrides`, file configuration, `AppConfig`, environment
parsing, CLI composition, redacted/debug output, and precedence tests:

~~~text
file keys: capture_queue_capacity, capture_memory_ceiling_bytes
environment: MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY
             MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES
CLI: --capture-queue-capacity
     --capture-memory-ceiling-bytes
~~~

The safe defaults remain 16,384 record slots and 64 MiB. The current
`journal_queue_capacity` field/file key and `MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY` environment
variable are removed without legacy aliases because version 0.1 has no compatibility promise and
the old name misstates what it controls. When the new CLI control is added, no
`--journal-queue-capacity` alias is introduced. Unknown legacy configuration fails through the
normal unknown-key or unknown-option path; it must not silently fall back. All 39 channel call
sites pass an explicit validated `CaptureChannelLimits`.

## Authority-bundle contract

Add a required method with no default implementation:

~~~rust
pub enum CaptureRetainedSizeError {
    Overflow { component: CaptureRetainedComponent },
    InvalidAuthorityGraph { component: CaptureRetainedComponent },
}

fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError>;
~~~

The method allocates nothing, uses checked arithmetic and actual owned capacities, counts each
unique shared allocation once within the bundle, verifies promised pointer identity, and
exhaustively destructures authority-bearing fields without a rest pattern. Arithmetic maps to
`Overflow`; pointer/ownership inconsistencies map to `InvalidAuthorityGraph`. Platform validation
that a successful total is below a known inline or constructed minimum maps separately to
`RetainedSizeUnderreported` and is never collapsed into either domain error.

### Identity dynamic charge

`CaptureAuthorityIdentity::checked_dynamic_retained_bytes` returns the same typed `Result` and
exhaustively charges the actual capacities of `SourceId`, metadata-revision `SourceIdentifier`, and
session `SourceIdentifier`. Connection generation is inline. Tests use short strings backed by
maximum-capacity allocations so a length-based implementation fails.

### Production source bundle

`CaptureGenerationCapabilities` retains this sharing graph:

~~~text
bundle
├── binding
├── lease
├── initialization ── lease
├── admission ── binding, lease
└── degradation ── lease
~~~

The bundle/admission bindings must be `Arc::ptr_eq`; bundle, initializer, admission, and
degradation leases must all be `Arc::ptr_eq`. Every mismatch returns
`InvalidAuthorityGraph { component: ... }`, not an overflow surrogate.

Before A4-TIME, the source formula is the inline bundle plus one complete
`FrameSessionBinding` allocation, its identity string capacities, and one complete
`CaptureGenerationLease` allocation. A4-TIME adds exactly one continuity pointee allocation. The
exhaustive destructure must change in the TIME lane; no rest pattern may hide it.

### Diagnostic bundle

The diagnostic bundle's direct identity and admission identity are distinct allocations and both
are charged. Initializer, admission, and degradation share one `Arc<AtomicU8>`; all handles must be
pointer-equal, and the allocation is charged once. Test bundles may declare a deterministic charge
but must still exhaustively destructure their fields and return typed `Result` failures.

### Shared Arc allocation helpers

Place dependency-neutral, checked allocation-layout helpers in the domain capture/memory seam.
For sized `Arc<T>`, compose the pinned Rust 1.97 header layout (two `AtomicUsize` values) with
`Layout::new::<T>()`; for `Arc<[u8]>`, compose the same header with
`Layout::array::<u8>(payload_len)`. Use `Layout::extend` and `pad_to_align`, map every layout or
arithmetic failure to `Overflow`, and use no unsafe code. A bare sum of header and pointee sizes can
miss alignment padding.

These helpers model the allocation retained by an owning graph; an `Arc` handle's inline bytes are
charged by its containing fixed slot or object. The layout is a pinned implementation fact, not a
public ABI. Every toolchain or target change triggers source-layout re-audit and boundary fixtures.

## Frame and payload contract

`RawCaptureFrameView` must expose both the borrowed wire bytes used by hashing/serialization and the
owned normalized payload used by frame-to-record conversion. Returning only `&[u8]` forces the
writer to allocate a replacement payload and defeats the shared-allocation design. The required
domain seam has no ownership-erasing default:

~~~rust
pub trait RawCaptureFrameView: Clone + Send + Sync + 'static {
    // Existing identity, ordinal, and time accessors remain required.
    fn payload(&self) -> &[u8];
    fn capture_payload(&self) -> &CapturePayload;
    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError>;
}
~~~

The private raw-record conversion constructor accepts `CapturePayload`, and the writer passes
`frame.capture_payload().clone()`. The clone shares the same `Arc<[u8]>`; it never reconstructs from
`payload()`. `payload()` remains the canonical borrowed byte view and must equal
`capture_payload().as_bytes()`. Trybuild fixtures fail when any production or test frame omits either
accessor or the checked footprint method.

Frames produce `InvalidAuthorityGraph` only when they advertise and validate a promised sharing
graph; arithmetic failures are `Overflow`. The publisher preserves the domain error and degrades the
affected generation. A malformed footprint or complete total below a platform-known structural
minimum becomes the distinct platform underreporting error.

### Deliberate payload ownership

Define one application-owned bounded representation in the domain capture/memory seam and replace
`Bytes` in `RawMarketFrame`, `DiagnosticCaptureFrame`, and committed `RawCaptureRecord` with it:

~~~rust
pub struct CapturePayload(PayloadStorage);

enum PayloadStorage {
    Empty,
    Shared(Arc<[u8]>),
}
~~~

The fields remain private and constructors enforce a frozen two-tier limit:

~~~text
COMMITTED_JOURNAL_BODY_CEILING_BYTES = 64 * 1024 * 1024 = 67,108,864
MAX_COMMITTED_COMPATIBILITY_PAYLOAD_BYTES
    = (COMMITTED_JOURNAL_BODY_CEILING_BYTES - 2) / 2
    = 33,554,431
MAX_LIVE_CAPTURE_PAYLOAD_BYTES = 4 * 1024 * 1024 = 4,194,304
~~~

The compatibility payload value is the exact maximum number of byte elements that could possibly
fit in the committed JSON-array representation: two bytes are required for brackets and every
element requires at least one digit plus a separator. It is a deserialization allocation ceiling,
not a promise that every value sequence of that length fits the whole record. The existing first-pass
serialized-body check remains authoritative for the complete 64 MiB journal frame.

`CapturePayload::try_from_live` enforces the 4 MiB owner limit and is used by `RawMarketFrame`,
`DiagnosticCaptureFrame`, and newly captured `RawCaptureRecord` construction. Journal
deserialization and the explicitly named compatibility constructor use
`CapturePayload::try_from_committed_wire`, accept payloads greater than 4 MiB when the complete
historical record is valid, and reject above 33,554,431 bytes before unbounded allocation. Live
publication always re-applies the 4 MiB check, so a compatibility record cannot be promoted into live
capture merely because it deserialized successfully. This preserves every valid existing current or
legacy committed-journal record while keeping new live owners at the established 4 MiB boundary.

`Empty` has no heap allocation and contributes zero dynamic bytes. A nonempty boundary slice is
copied exactly once into a right-sized `Arc<[u8]>`; sliced or spare-capacity backing storage cannot
enter the graph. Cloning shares the same allocation. Serialization borrows `&[u8]`, preserving the
existing wire schema. Internal frame-to-record conversion clones the `CapturePayload` instead of
copying payload bytes, so the committed record and frame deliberately share one allocation. Public
raw-record construction from an external slice still performs exactly one checked boundary copy.

The constructors reject length and retained-layout overflow before attempting the copy, but stable
Rust 1.97 does not expose a recoverably fallible `Arc<[u8]>` boundary allocation. Allocator OOM at
that already bounded allocation remains a process boundary. No error variant or test may claim that
this allocation returns a recoverable refusal; only the ring, registry, memory-sink, and scratch
`Vec::try_reserve_exact` paths make that claim.

The checked `Arc<[u8]>` layout helper charges header, alignment, and payload length as one complete
allocation. Tests cover empty payloads; exact 4 MiB live success and one-byte-over live refusal in
all three live owners; successful read and round trip of a canonical valid historical journal payload
above 4 MiB; exact compatibility-constructor success and one-byte-over refusal; complete-record
64 MiB enforcement; pointer-equal clones; boundary-copy isolation after caller-buffer mutation;
unchanged serialization; arithmetic overflow; and a conversion fixture proving no second payload
allocation. The historical compatibility fixture must be committed or deterministically generated
without embedding an unreviewable binary artifact.

### Complete frame formulas

`RawMarketFrame` reports its inline size, one complete resident-shared `FrameSessionBinding`
allocation with actual source/revision/session capacities, and the unique nonempty payload
allocation. Its complete graph validator deduplicates promised shared pointers or fails
`InvalidAuthorityGraph`; record admission excludes the binding only after the active pointer proof
defined above.

`DiagnosticCaptureFrame` reports its inline size, zero resident-shared bytes, actual unique dynamic
identity capacities, and the unique nonempty payload allocation. These are closed conservative
structural upper bounds for the pinned graph, not allocator usable-size or RSS claims.

## Complete generation construction

The platform prepares a successor fully before publication or predecessor revocation:

~~~rust
struct PreparedCaptureGeneration<B> {
    state: Arc<GenerationCaptureState<B>>,
}
~~~

Preparation first obtains typed bundle/identity sizes, constructs and validates the complete graph,
checks the result against known structural minima, reserves the complete resident bytes in the one
accounting total, and moves that RAII token into the private
`Arc<AccountedGenerationIdentity>`. The state and every derivative handle clone that accounted
identity. Any failure drops the not-yet-published token automatically.

The conservative resident charge is:

~~~text
bundle checked retained graph
+ GenerationCaptureState Arc allocation
+ AccountedGenerationIdentity Arc allocation
+ platform identity dynamic capacities
~~~

The charge may conservatively retain complete-generation bytes after only an identity snapshot
remains, but it is never duplicated per record and never manually transferred between prepared,
active, or retired categories.

`raw_capture_channel` changes from an infallible tuple to a typed `Result`; retained-size failure
marks only the rejected bundle incomplete and publishes no handle. Rotation prepares and reserves
the successor, acquires lifecycle serialization, revalidates session/generation/writer state,
initializes it, marks the predecessor incomplete only at the established commit point, and
atomically publishes the successor. Every existing authority, authority-busy, ordering, and
writer-publication error remains distinct. Successor overflow, invalid graph, underreporting, or
budget refusal must never disturb the healthy predecessor.

## Queue alternatives

### Evaluation criteria

A production queue at this boundary needs:

- bounded, fixed-capacity storage;
- a checked construction path;
- an observable retained-allocation formula;
- nonblocking producer admission;
- one explicitly owned consumer;
- FIFO behavior;
- explicit close and drain;
- reliable out-of-band shutdown;
- timeout/cancellation support;
- typed full, contention, closure, poison, and allocation failures;
- RAII release for queued reservations; and
- no repository unsafe code.

### Comparison

| Option | Strengths | Retained-memory and lifecycle limitations | Decision |
| --- | --- | --- | --- |
| Rust std sync_channel | No dependency; bounded count; MPSC; try_send; timeout receive; normal disconnect | Rust 1.97 privately preallocates Box<[Slot<T>]> and a separate counter allocation; no retained-size API; construction is infallible; private waker layout; in-band Wake can fail when full | Reject for this proof unless every private term is pinned, persisted, and budgeted |
| Crossbeam channel 0.5.15 | Mature, performant bounded channels; try_send and timeout receive | Private allocation layout and implementation-level unsafe; no stable retained-size API or explicit close contract tailored to this lifecycle | Good general channel, not a closed memory boundary |
| Tokio MPSC 1.52.4 | Already locked; bounded API; explicit `Receiver::close` and drain guidance; async integration | Internal block allocation is explicitly an implementation detail; no retained-byte API; dedicated blocking-writer integration adds runtime concerns | Reject for closed structural accounting |
| Flume 0.12 | Bounded, try-send, and timeout operations; its crate documentation states that it contains no unsafe code | Private `Mutex`, `VecDeque`, and waiter-hook allocation behavior; logical capacity is not a stable allocation contract | Useful comparison, not a closed allocation proof |
| Thingbuf 0.1.6 | Preallocated array shape; blocking and async APIs; no per-message allocation after creation | Private slot/recycle layout; no retained-size API; high mostly-empty capacities are documented as inefficient; internal implementation details require pinning | Useful benchmark/reference candidate, not the default authority boundary |
| Platform-owned safe fixed ring | Actual slot capacity observable; no dependency; no repository unsafe; explicit single-consumer, close, drain, poison, and accounting semantics | Mutex contention must be measured; implementation and concurrency tests are repository responsibilities | Selected, conditional on benchmark acceptance |

The standard channel's Rust 1.97 private source is based on a preallocated bounded MPMC array. The
source contains:

- Slot<T> with an atomic stamp and MaybeUninit<T>;
- Channel<T> with Box<[Slot<T>]>;
- padded head and tail atomics;
- sender and receiver wakers; and
- a separately boxed sender/receiver counter around the channel.

These are pinned facts and explain the current undercount. They are not public standard-library
layout promises.

No examined mature dependency exposes a public retained-allocation receipt sufficient for
Q2-I08. Monetary cost and asymptotic throughput are separate from proof quality; all evaluated
crates are available without a paid runtime, but none supplies the required memory contract.

## Platform-owned safe ring

### Shape and closure linearization

A narrow internal implementation can use:

~~~rust
struct QueueCore<T> {
    state: Mutex<QueueState<T>>,
    not_empty: Condvar,
    closed_hint: AtomicBool,
}

struct QueueState<T> {
    slots: Vec<Option<T>>,
    head: usize,
    len: usize,
    sender_count: usize,
    closed: bool,
    receiver_alive: bool,
    poisoned: bool,
    terminal_cleanup_claimed: bool,
}
~~~

The tail is derived with checked modular arithmetic from `head` and `len`. Capacity is immutable.
There is exactly one initial producer and one unique non-`Clone` consumer. Producer duplication is a
fallible `try_clone`, not an infallible `Clone` implementation: mutex poison and `usize` sender-count
overflow cannot be represented by `Clone`, and production code may not panic or saturate around
either condition. `try_clone` locks the state, returns `QueueClosed` or `QueuePoisoned` without
creating a handle when terminal, uses `checked_add(1)`, returns `SenderCountOverflow` without mutation on
overflow, increments under the mutex, and only then returns the new producer. Application publisher
cloning exposes the same typed fallible contract. Clone/drop are setup and lifecycle operations; no
event publication creates or destroys a producer handle, and `try_push` remains the only producer
hot-path operation.

`QueueState::closed` under the mutex is authoritative. `closed_hint` is only an early-rejection
hint and can never authorize insertion. `try_push` may reject immediately when the hint is true,
using an Acquire load; after `try_lock` succeeds it rechecks authoritative closure and inserts only
while `state.closed == false && state.receiver_alive == true`. Explicit close, last-sender closure,
and receiver drop acquire or recover the same mutex, set authoritative closure, publish the hint with
Release ordering, and notify. Close and insertion therefore linearize under one mutex; a stale
atomic observation cannot insert after close.

Producer `Drop` acquires the same mutex. A positive count decrements exactly once; transition from
one to zero sets authoritative closure before releasing the guard, then stores `closed_hint = true`
with Release ordering and notifies all waiters. Observing zero before decrement is an invariant
failure: cleanup marks the queue poisoned and closed, increments the bounded invariant counter,
publishes the hint, and notifies. If the state mutex is already poisoned, clone/send/receive do not
resume service. Drop and explicit terminal cleanup recover ownership with
`PoisonError::into_inner`, mark `poisoned = true` and `closed = true`, decrement a still-positive
sender count only for the dropping handle, drain owned messages exactly once when the unique cleanup
owner atomically changes `terminal_cleanup_claimed` from false to true, publish the hint, and notify.
Without allocating a second collection, the cleanup owner repeatedly takes at most one occupied slot
under the mutex, releases the guard, drops that message, and reacquires or recovers the terminal mutex
until empty. Every later cleanup observer sees the claimed flag and cannot take those messages. Only
`closed_hint` is atomic; sender count and the cleanup claim are never read or modified outside the
mutex.

### Checked construction

Construction:

1. Accept validated nonzero queue-capacity and memory-ceiling values.
2. Check capacity multiplied by `size_of::<Option<T>>()`.
3. Reject a slot lower bound already above the ceiling.
4. Allocate an empty `Vec` and call `try_reserve_exact(capacity)`.
5. Map `TryReserveError` to a typed queue-allocation failure.
6. Record the observed `Vec::capacity()`, then fill exactly the requested logical slots with `None`
   through `resize_with(capacity, || None)`; never resize to observed spare capacity.
7. Initialize `sender_count = 1`, `receiver_alive = true`, and all terminal/cleanup flags false before
   either handle is visible.
8. Charge the observed backing capacity while deriving every ring index and Full decision only from
   the requested logical `slots.len()`.
9. Add every checked accounting-core, channel-state, record/health `QueueCore`, observed slot,
   `WriterLifecycleCore`, and fixed scratch term enumerated by the fixed-infrastructure formula.
10. Initialize the authoritative total to the complete fixed charge.
11. Prepare and reserve the initial resident generation.
12. Publish handles only after the total is within the ceiling and every check succeeds.

Rust permits `try_reserve_exact` to expose more capacity than requested, so actual capacity is
charged without becoming usable queue depth. Do not convert to a boxed slice through a potentially
allocating infallible shrink path; a never-grown `Vec` has an observable capacity and simple checked
formula. An injected over-capacity fixture proves logical capacity `N` accepts exactly `N` records,
returns `Full` for `N + 1`, never indexes allocator spare capacity, and still charges the complete
observed backing allocation.

Stable Rust does not provide recoverable allocation for every small `Arc` and synchronization
object. Dominant slot allocation is fallible and typed, while process-wide allocator OOM remains
outside ordinary application recovery. The claim is checked arithmetic, recoverable dominant
allocation failure, and a closed conservative structural upper bound for the pinned implementation,
not byte-exact allocator usage or RSS.

### Producer contract

Publication is nonblocking and uses `Mutex::try_lock`. Results remain distinct:

~~~text
Enqueued
Full
Contended
Closed
Poisoned
~~~

`Contended` is not `Full`. Any result that cannot prove capture degrades the affected generation
with the precise typed reason. Mutex contention is an implementation risk to measure before Q2
approval; failure of the acceptance target requires a new retained-layout design decision, not
silent abandonment of accounting.

### Consumer, close, and cancellation contract

The consumer waits on a `Condvar` predicate covering queue nonempty, authoritative closure,
`sender_count == 0`, receiver liveness, and poison. Shutdown never relies on a separately stored
cancellation flag plus a bare notification: `request_shutdown` acquires or recovers the same
`QueueState` mutex, sets authoritative `closed = true`, publishes `closed_hint` with Release ordering,
releases the guard, and calls `notify_all`. The consumer therefore observes shutdown through the same
mutex predicate used by `Condvar::wait`; it never consumes a record slot with an in-band wake message.
Writer-lifecycle cancellation/deadline state may still classify why shutdown occurred, but it cannot
be the sole wake predicate.

Explicit close prevents new sends and drains items admitted before its mutex linearization point.
Last-sender drop uses the same close path. Receiver drop acquires or recovers the lock, sets
`receiver_alive = false` and `closed = true`, drains all pending messages while owning state,
publishes `closed_hint = true`, and notifies waiters. Poison is terminal for normal service;
`PoisonError::into_inner` is permitted only to take and drop owned messages during cleanup.

Deterministic barrier tests force close-vs-insert and receiver-drop-vs-insert at every lock
boundary. Loom tests use a reduced queue/control model to explore producer, explicit-close,
fallible-clone, clone-overflow refusal, last-sender-drop, receiver-drop, poison recovery, stale-hint,
shutdown-after-predicate-before-wait, full-queue shutdown, and drain interleavings. Every history must
linearize to an admitted pre-close item or a typed terminal result; no lost shutdown wake,
post-close insertion, sender-count wrap, normal service after poison, or double-drain is valid.

### Fixed health queue with shared resident identity

Replace the current health channel with the same fixed queue shape and charge its slot/control
graph at construction. A health event stores a clone of the generation's
`Arc<AccountedGenerationIdentity>`; event clones share the already-admitted resident token. There
is no health-event reservation or separate heap admission. Bounded diagnostic overflow follows its
documented drop/counter policy and never changes authority state.

## Reservation lifetime and accounting integrity

### Required lexical lifetime

`RecordReservation` remains owned by `CaptureMessage::Record` through deadline, generation, and
issuance validation; frame-to-record conversion; sink append; policy-triggered flush; every error;
cancellation; shutdown; pending ownership; and drain.

~~~rust
let CaptureMessage::Record {
    accounted_identity,
    frame,
    reservation: _reservation,
} = message;

validate(&accounted_identity, ...)?;
let record = convert(accounted_identity, frame)?;
sink.append(&record)?;
flush_if_required(...)?;
~~~

Every early `Result` return drops the reservation exactly once. Failed nonblocking send returns
message ownership, and dropping it releases both record reservation and identity handle. A dequeued
record blocked in a sink remains charged.

### One accounting core

Fixed-infrastructure, resident, and record reservations retain an `Arc` to one narrow accounting core
rather than a `Weak` reference to `CaptureState`. The core starts with its base plus the initial
channel-state reservation and remains alive until the last admitted owner is destroyed without
cycling through publisher, writer, or queue state.

Admission enters the frozen checked transition bracket, loads the authoritative total, checks
addition and ceiling, reserves with the documented `SeqCst` compare-exchange protocol, publishes
the matching component and epoch, then finishes the transition and constructs the typed RAII token.
Release uses the same `SeqCst` transition protocol and never wraps or panics. If a
token exceeds the total or would reduce it below fixed bytes, accounting is poisoned, the total is
moved to a terminal safe value, the invariant-failure counter increments, all later publication is
rejected, and the generation degrades.

Ordinary budget refusal is not poison. Overflow, underflow, double release, token mismatch, or a
stored charge below a validated graph is an integrity failure. Diagnostic component counters move
with successful total transitions and reconcile to the total, but they are not independent
admission authorities. Their transition bracket and completed epoch do not serialize admission or
replace the authoritative total CAS; they exist only so bounded readers can reject a torn diagnostic
view. No live publisher waits for a snapshot reader, and snapshot contention never authorizes or
refuses a record.

## Conversion, journal, and sink bounds

### Single-copy raw-record conversion

Diagnostic conversion currently produces two payload copies. The normalized payload contract
removes both conversion copies: the frame already owns a checked `CapturePayload`, and the private
raw-record constructor clones its `Arc<[u8]>`. The only payload copy is the original adapter or
diagnostic boundary copy. Public record construction from a caller-owned slice retains its own
single checked boundary-copy contract.

After moving UUID scratch to fixed writer storage, the current conversion peak has one dynamic term:

~~~text
conversion_peak_bytes
    = complete Arc<str> allocation for the raw-record source label
~~~

UUIDs, timestamps, options, the raw-record value, and `CapturePayload`/accounted-identity handles are
inline; UUID input scratch is already in fixed writer bytes. The peak explicitly excludes a new
payload allocation. The input frame, shared payload, accounted generation identity, raw-record source
allocation, and fixed scratch can coexist until sink return, so the record reservation covers the
complete checked overlap. Any later conversion buffer extends this formula and triggers re-audit
before merge. Tests assert pointer equality between frame and record payloads and use allocation
instrumentation to reject a second payload allocation.

### UUID input scratch

Diagnostic UUID construction currently creates generation-name and event-name Vec values. Their
maximum inputs are derivable from bounded identity fields and fixed binary fields. Prefer:

- a fixed-capacity writer-owned buffer charged in fixed writer bytes; or
- a fixed-size inline buffer when the complete maximum is practical.

Do not create multiple payload-dependent vectors. Exact boundary tests must cover the maximum
source, revision, session, generation, ordinal, and timestamp representation.

### Two-pass streaming journal

The journal needs payload length and CRC before writing the framed body. It can avoid
serde_json::to_vec with deterministic two-pass serialization:

1. Serialize through serde_json::to_writer into a CountingCrcWriter that stores no body.
2. Check the counted length against the maximum record size.
3. Write the journal header containing length and CRC.
4. Serialize the same record again directly into the journal BufWriter.
5. Flush according to policy while the capture reservation remains live.

The serializer settings and record value are immutable between passes, so both passes are
deterministic. The runtime-only accounted-generation handle remains on `CapturedRawRecord`; only
its inner `RawCaptureRecord` uses the committed wire serializer, so the journal schema does not
gain an accounting field. Tests compare the second-pass body, count, and CRC against a canonical
fixture.

A failure during the second pass can leave a truncated tail, as can an underlying write failure in
the present framed design. Startup/recovery must retain its existing rule of accepting complete
frames and truncating or rejecting an incomplete final frame. No partially written frame is
reported as a durable append.

The BufWriter allocation is persistent sink state and belongs to the fixed sink ledger.

### Bounded retaining sink

`MemoryCaptureSink` remains a supported bounded diagnostic capability, not test-only scaffolding.
Its only constructor is fallible:

~~~rust
MemoryCaptureSink::try_new(
    max_records: NonZeroUsize,
    max_retained_bytes: NonZeroUsize,
)
~~~

Construction checks `max_records * size_of::<CapturedRawRecord>()`, calls
`Vec::try_reserve_exact(max_records)`, records the observed capacity, and charges that entire fixed
slot term before returning. The vector never grows: append rejects at the configured record count,
which is no greater than the preallocated observed capacity. `Default`, zero/unbounded construction,
and any internal grow-on-demand path are removed.

The separate sink ledger uses checked arithmetic and one explicit formula:

~~~text
sink_fixed_bytes
    = size_of::<MemoryCaptureSink>()
    + records.capacity() * size_of::<CapturedRawRecord>()

retained_record_dynamic_bytes(record)
    = complete source Arc<str> allocation
    + complete nonempty CapturePayload Arc<[u8]> allocation

sink_total_accounted_bytes
    = sink_fixed_bytes
    + sum(retained_record_dynamic_bytes(record))
    <= max_retained_bytes
~~~

The `MemoryCaptureSink` inline term charges its `Vec` header, inline destination, limits, and counters
once. The current sink has no other fixed allocation, and the current record schema has no other
dynamic field. Any schema or sink-metadata addition must extend the formula before it can enter the
retaining sink. Source and payload terms use the checked Arc-layout helpers.
The declared policy conservatively charges each retained record's dynamic allocation even if a
caller presents two record clones sharing it; this may refuse early but cannot undercount and avoids
an allocation-growing pointer-deduplication index inside the diagnostic sink.

The inline `CapturedRawRecord`, its inline raw-record fields, and its accounted-generation `Arc`
handle are already funded by the preallocated slot and are never added per append. The handle
continues to share the channel's resident-generation token; retaining it cannot release that token
prematurely, and the sink does not duplicate the complete generation or resident token in its own
byte ledger. Append computes the candidate count, dynamic charge, and candidate total before cloning
or mutating the vector. Exact-limit insertion succeeds. One additional record or byte fails with no
collection or counter change.

Construction distinguishes arithmetic overflow, allocation failure, and fixed-budget infeasibility.
Append distinguishes record-limit exhaustion, retained-byte exhaustion, retained-size calculation
failure, and accounting-invariant failure. `clear` and `Drop` release every dynamic sink charge;
fixed capacity remains charged until sink drop. Counter underflow, mismatch with recomputation, or
mutation after a refused append is terminal `CaptureSinkError::AccountingInvariant`, not ordinary
capacity exhaustion.

CaptureSink documentation states that a production sink may not retain a record after append
returns unless that retained graph is admitted against an explicit separate sink budget. A sink
that retains data cannot rely on the queue reservation after append returns.

## Typed failure taxonomy

Domain retained-size failures remain reusable and exact:

~~~rust
pub enum CaptureRetainedSizeError {
    Overflow { component: CaptureRetainedComponent },
    InvalidAuthorityGraph { component: CaptureRetainedComponent },
}
~~~

Platform construction and preparation add platform-owned validation without erasing the domain
cause:

~~~rust
pub enum CaptureChannelError {
    GenerationPreparation(CaptureGenerationPreparationError),
    FixedStorageBudgetExceeded {
        required: usize,
        limit: usize,
    },
    QueueAllocationFailed {
        queue: CaptureQueueKind,
        requested_slots: usize,
    },
}
~~~

Writer start has its own recovery boundary. Its complete nested error surface is:

~~~rust
pub enum CaptureDestinationFenceError {
    Busy { destination: CaptureDestination },
    Capacity { limit: usize },
    RegistryPoisoned,
}

pub enum WriterRuntimeProofError {
    CompiledTargetMismatch,
    FormulaRevisionMismatch { expected: u32, actual: u32 },
    ArtifactHashMismatch { expected: [u8; 32], actual: [u8; 32] },
}

pub enum CaptureWriterSpawnError {
    FixedStorageBudgetExceeded { required: usize, limit: usize },
    ScratchAllocationFailed { requested_bytes: usize },
    DestinationFence(CaptureDestinationFenceError),
    RuntimeProof(WriterRuntimeProofError),
    ThreadNameLimitExceeded { actual: usize, limit: usize },
    ThreadSpawnFailed { source: std::io::Error },
}
~~~

Destination busy, registry capacity, and terminal registry poison retain different recovery actions;
none is mislabeled as a byte-budget or operating-system spawn failure. A poisoned registry mutex is
recovered only to preserve/drop owned entries during terminal cleanup and never resumes acquisition.
The runtime-proof variants carry only fixed-size, non-secret evidence. The scratch variant comes from
recoverably fallible fixed `Vec` preparation; the thread variant preserves the operating-system spawn
cause without pretending that stable `Arc`, thread-name, or standard-library spawn-packet allocation
itself is recoverable.

Every writer-start failure releases the prepared writer fixed reservation and any acquired
destination lease exactly once, creates no published thread or handle, and never marks the generation
healthy. Fixed journal-sink refusal remains a sink-construction error rather than being collapsed into
thread creation. These names and nested variants are canonical; no stringly catch-all or alias may
replace them.

Construction and rotation share only the nested preparation taxonomy:

~~~rust
pub enum CaptureGenerationPreparationError {
    RetainedSize(CaptureRetainedSizeError),
    RetainedSizeUnderreported {
        component: CaptureRetainedComponent,
        reported: usize,
        minimum: usize,
    },
    CaptureMemoryBudgetExceeded {
        required: usize,
        available: usize,
    },
}

pub enum CaptureGenerationError {
    Activation(CaptureAuthorityError),
    BindingMismatch {
        current: CaptureIdentitySnapshot,
        received: CaptureIdentitySnapshot,
    },
    GenerationOrder,
    WriterLifecycle,
    Preparation(CaptureGenerationPreparationError),
    RetainedSize(CaptureRetainedSizeError),
    RetainedSizeUnderreported {
        component: CaptureRetainedComponent,
        reported: usize,
        minimum: usize,
    },
    CaptureMemoryBudgetExceeded {
        required: usize,
        available: usize,
    },
    AccountingInvariant,
}
~~~

`CaptureChannelError` is returned only by initial construction.
`CaptureGenerationError` is returned by activation and rotation. `Activation` preserves the typed
authority cause; `BindingMismatch` preserves both accounted identities; and `GenerationOrder` and
`WriterLifecycle` remain distinct bounded classes rather than strings or aliases for one another.
Identity-bearing payloads use the accounted snapshot wrapper so an error cannot outlive its resident
token. This preserves different caller recovery actions while reusing the exact graph-validation
failure type.

Publication distinguishes:

~~~rust
pub enum CapturePublishError {
    // Existing authority-bearing and lifecycle failures remain distinct.
    Authority(CaptureAuthorityError),
    AuthorityBusy,
    WriterUnavailable,
    RetainedSize(CaptureRetainedSizeError),
    RetainedSizeUnderreported {
        component: CaptureRetainedComponent,
        reported: usize,
        minimum: usize,
    },
    InvalidPayloadView,
    CaptureMemoryBudgetExceeded {
        required: usize,
        available: usize,
    },
    QueueFull,
    QueueContended,
    QueueClosed,
    QueuePoisoned,
    AccountingInvariant,
}
~~~

Producer duplication also remains fallible and distinct:

~~~rust
pub enum CapturePublisherCloneError {
    QueueClosed,
    QueuePoisoned,
    SenderCountOverflow,
}
~~~

It neither aliases `CapturePublishError` nor marks `Full`: cloning does not attempt record
publication. These Rust-shaped names are canonical: initial channel and writer budget refusal use
`FixedStorageBudgetExceeded`; clone/queue terminal cases use `QueueClosed` and `QueuePoisoned`;
producer-count overflow uses `SenderCountOverflow`; unified channel refusal uses
`CaptureMemoryBudgetExceeded`; accounting poison uses `AccountingInvariant`; and borrowed payload
disagreement uses `CapturePublishError::InvalidPayloadView`. No alternate aliases are introduced for
these contracts.

The existing `Authority`, `AuthorityBusy`, and `WriterUnavailable` publication variants are not
folded into queue or accounting failures. Existing generation binding, ordering, authority, and
writer variants likewise survive; identity-bearing error payloads use the accounted snapshot
wrapper. Current `Saturated` and `Closed` publication cases are refined into
the queue/budget variants above. Capture-health reasons gain matching bounded non-secret classes so
operators can distinguish overflow, invalid graph, underreporting, memory refusal, full,
contention, close, poison, and accounting poison.

Post-publication writer failures remain a separate closed surface:

~~~rust
pub enum CaptureWriterError {
    Deadline,
    Authority(CaptureAuthorityError),
    InvalidPayloadSharing,
    DiagnosticConversion,
    Sink(CaptureSinkError),
    AccountingInvariant,
}
~~~

`Deadline` and `DiagnosticConversion` retain only bounded non-secret timing/schema evidence;
`Authority` preserves its typed source; `Sink` preserves the complete sink taxonomy below. No
variant collapses into an arbitrary writer message.

Memory-sink construction and runtime failures remain distinct from journal I/O:

~~~rust
pub enum MemoryCaptureSinkRetainedComponent {
    FixedRecordSlots,
    RecordSource,
    RecordPayload,
    Total,
}

pub enum MemoryCaptureSinkConstructionError {
    ArithmeticOverflow { component: MemoryCaptureSinkRetainedComponent },
    AllocationFailed { requested_records: usize },
    FixedStorageBudgetExceeded { required: usize, limit: usize },
}

pub enum JournalSinkConstructionError {
    FixedStorageBudgetExceeded { required: usize, limit: usize },
    ArithmeticOverflow,
    Journal(JournalError),
}

pub enum CaptureSinkError {
    RetainedSize(CaptureRetainedSizeError),
    RetainedSizeUnderreported,
    RecordLimitExceeded { limit: usize },
    RetainedByteLimitExceeded { required: usize, limit: usize },
    SerializationLimitExceeded { required: usize, limit: usize },
    SerializationFailure,
    WriteFailure,
    FlushFailure,
    ShutdownDeadlineExceeded,
    AccountingInvariant,
}
~~~

These cases have different operator actions. They must not collapse into one generic capacity or
disconnection message.

### Exhaustive bounded health mapping

Every active-generation failure maps exhaustively to a bounded non-secret `CaptureHealthReason`.
Adding an error variant without updating this match is a compile failure. Health events may carry the
accounted identity snapshot, integrity state, reason class, and bounded numeric `required`/`limit`
evidence. They never contain payload bytes, source frames, URLs, filesystem paths, arbitrary provider
text, credentials, serialized lower-level errors, or allocator/debug dumps.

| Failure | Health reason | Integrity effect |
| --- | --- | --- |
| `CaptureAuthorityError::GenerationNotReady` | `AuthorityNotReady` | Refuse publication and mark the misused generation incomplete |
| `CaptureAuthorityError::GenerationIncomplete` | `AuthorityIncomplete` | Terminal for that generation |
| `CaptureAuthorityError::FrameBindingMismatch` | `FrameBindingMismatch` | Refuse the foreign frame; do not corrupt the current generation |
| `CaptureAuthorityError::FrameRejected` | `AuthorityRejected` | Refuse and mark the affected generation incomplete |
| publication authority mutex busy | `AuthorityBusy` | Refuse and mark the affected generation incomplete |
| writer absent or stopped | `WriterUnavailable` | Terminal capture incompleteness |
| supervised writer exits through an unexpected worker/storage path | `WriterFailed` | Terminal capture incompleteness |
| supervised writer completes its normal stop transition | `WriterStopped` | Capture authority ends with writer lifetime |
| generation activation refusal | `GenerationActivation` | Refuse the successor or active transition without weakening its predecessor |
| generation identity binding mismatch | `BindingMismatch` | Refuse the foreign identity while retaining its accounted evidence |
| generation ordering refusal | `GenerationOrder` | Refuse a non-successor without mutating current authority |
| generation writer-lifecycle refusal | `WriterLifecycle` | Refuse or terminate the affected generation according to its lifecycle state |
| generation preparation refusal | `GenerationPreparation` | Refuse the unpublished successor and release its prepared reservations |
| frame processing deadline | `FrameDeadlineExceeded` | Refuse the record and preserve existing late-write accounting |
| frame-to-record timestamp/schema conversion refusal | `DiagnosticConversion` | Writer failure; record is not reported complete |
| sole positive capture supervisor exits or drops | `SupervisorStopped` | Terminal capture incompleteness |
| retained arithmetic overflow | `RetainedSizeOverflow` | Terminal accounting refusal for the affected generation |
| invalid promised sharing graph | `InvalidAuthorityGraph` | Terminal preparation/publication refusal |
| structural underreporting | `RetainedSizeUnderreported` | Terminal preparation/publication refusal |
| borrowed frame payload differs from owned `CapturePayload` | `InvalidPayloadView` | Refuse publication before reservation reaches the sink |
| converted record does not share the admitted payload allocation | `InvalidPayloadSharing` | Writer failure; sink never observes the record |
| ordinary unified-memory refusal | `CaptureMemoryBudgetExceeded` | Mark the affected generation incomplete; accounting core is not poisoned |
| fixed record queue full | `QueueFull` | Mark the affected generation incomplete; not mutex contention |
| record queue lock contended | `QueueContended` | Mark the affected generation incomplete; not queue full |
| queue explicitly/last-sender/receiver closed | `QueueClosed` | Terminal capture incompleteness |
| state mutex poison or queue invariant failure | `QueuePoisoned` | Terminal; cleanup only |
| producer clone after close | `QueueClosed` | Refuse lifecycle composition; no handle created |
| producer clone after poison | `QueuePoisoned` | Terminal; no handle created |
| producer sender-count overflow | `SenderCountOverflow` | Typed lifecycle refusal without counter mutation |
| accounting overflow/underflow/double release/token mismatch | `AccountingInvariant` | Terminal; all later admission refused |
| memory-sink record refusal | `SinkRecordLimit` | Writer failure; retained record is not reported complete |
| memory-sink retained-byte refusal | `SinkRetainedByteLimit` | Writer failure; retained record is not reported complete |
| memory-sink retained-size overflow/invalid graph/underreporting | `RetainedSizeOverflow` / `InvalidAuthorityGraph` / `RetainedSizeUnderreported` | Writer failure with the original exact class |
| memory-sink counter invariant failure | `AccountingInvariant` | Terminal writer/accounting failure |
| journal body above committed ceiling | `SerializationLimit` | Writer failure |
| deterministic serialization failure | `SerializationFailure` | Writer failure |
| journal write failure | `WriteFailure` | Writer failure |
| policy-triggered or final flush failure | `FlushFailure` | Writer failure |
| shutdown deadline | `ShutdownDeadlineExceeded` | Terminal capture incompleteness with existing late-write accounting |

Initial channel, sink, or writer construction failures that occur before any active handle or health
queue exists return their typed construction error and structured redacted local log class; they do
not fabricate a health event. Rejected successor preparation marks only the not-yet-published
successor incomplete and emits at most a bounded event carrying its accounted snapshot; it never
rewrites predecessor health.

## TDD matrix

Implementation follows RED, GREEN, focused verification, and self-review within each lane.

### Serialized A4 contract and final-API seed

RED:

- A domain contract test calls the `Result`-returning `checked_retained_bytes` before it exists and
  fails with E0599.
- A fake bundle omitting the method fails to compile, proving there is no default.
- Fake frame implementations omitting borrowed `payload`, ownership-preserving `capture_payload`, or
  `checked_retained_footprint` methods fail to compile.
- A new bundle field without an exhaustive accounting update fails to compile.
- Synthetic overflow, pointer mismatch, and platform underreporting fixtures require three distinct
  errors and fail against the current collapsed contract.

GREEN:

- All four bundle implementations compile.
- Maximum-capacity, short-length source, revision, and session values charge capacities.
- Source bundle counts one shared binding and one shared lease.
- Mismatched binding pointers return `InvalidAuthorityGraph`.
- Each mismatched initializer, admission, or degradation lease returns `InvalidAuthorityGraph`.
- Diagnostic bundle counts both distinct identity allocations.
- Mismatched diagnostic `AtomicU8` handles return `InvalidAuthorityGraph`.
- Arithmetic returns typed `Overflow` without saturation.
- A successful report below the platform-known structural minimum returns
  `RetainedSizeUnderreported`, not a domain error.
- Initial channel construction degrades the rejected bundle and creates no active channel.
- Every successor preparation failure leaves predecessor identity, health, and admission unchanged.
- Existing authority, authority-busy, binding, ordering, and writer-unavailable errors retain their
  variants.
- All 39 `raw_capture_channel` invocations migrate to the `Result` contract without `unwrap` or
  `expect`.
- Every channel call supplies explicit validated queue and memory limits.
- `capture_queue_capacity` and `capture_memory_ceiling_bytes` obey defaults/file/environment/CLI
  precedence with the exact documented environment and CLI names.
- Zero, overflow, and malformed values fail closed; the old journal field, environment variable,
  and CLI spelling are unknown rather than aliased.

### Payload and frame accounting

- RawMarketFrame footprint reports actual source, revision, and session capacities as
  resident-shared and excludes them from record reservation only after active binding pointer proof.
- A value-equal but pointer-distinct RawMarketFrame binding fails preflight before shared bytes can be
  excluded.
- DiagnosticCaptureFrame reports zero resident-shared bytes and charges both distinct identity and
  payload allocations as unique frame dynamic.
- Empty payload uses the explicit empty representation and charges no payload allocation.
- Nonempty production and diagnostic payloads use `Arc<[u8]>`; committed raw records clone the same
  allocation.
- `payload()` equals `capture_payload().as_bytes()` for every frame implementation and fixture.
- Production RawMarketFrame/DiagnosticCaptureFrame and both platform/domain TestFrame
  implementations provide all three required payload/footprint methods; the inventory is exactly
  four at this base and a changed count triggers refresh.
- Caller-buffer mutation after construction cannot change the captured payload.
- Clone fixtures are pointer-equal and introduce no payload copy or additional allocation charge.
- Serialization bytes and schemas remain identical to canonical fixtures.
- Frame-to-record conversion has no new payload allocation; instrumentation and pointer equality
  enforce it.
- A malformed inline/resident/unique decomposition or inline term different from `size_of` its frame
  type is rejected as platform underreporting.
- Exact 4,194,304-byte live payload succeeds in RawMarketFrame, DiagnosticCaptureFrame, and new-live
  raw-record construction; 4,194,305 bytes fails in each owner.
- A canonical valid historical journal payload greater than 4 MiB still reads and round trips.
- The compatibility constructor accepts 33,554,431 payload bytes for allocation-bound purposes and
  rejects 33,554,432 before allocation growth; complete serialization still enforces the exact
  67,108,864-byte journal-body ceiling.
- Arc slice-layout arithmetic overflow becomes a typed domain overflow.
- Toolchain, target, or owned-payload representation changes invalidate the pinned fixture.

### Fixed queue

- Capacity one accepts one record and reports Full on the next.
- FIFO ordering survives multiple wraparounds.
- Multiple producers and one consumer preserve the model order.
- Producer handles use typed fallible `try_clone`; the consumer handle is not cloneable.
- Negative static/trybuild assertions prove both the internal producer and public publisher no longer
  implement `Clone`; all ten direct clone expressions, the `CaptureContext` derive, and both prior
  positive static Clone assertions migrate deliberately.
- Clone after close/poison and sender-count overflow return distinct errors without count mutation or
  handle creation.
- Producer send never blocks.
- A locked queue returns Contended, not Full.
- Explicit close and insertion serialize under the state mutex; later sends cannot insert even when
  `closed_hint` is stale.
- Last producer drop wakes the consumer.
- Producer drop decrements exactly once under the mutex; zero-before-decrement poisons and closes.
- Receiver drop serializes with insertion, closes, and drains exactly once.
- A full queue plus shutdown wakes without an in-band record.
- Receive timeout is deterministic.
- Poison is terminal for normal operations.
- Poison cleanup drains all owned messages and reservations.
- Capacity multiplication overflow is typed.
- Dominant storage allocation failure is typed.
- Actual Vec capacity greater than requested is charged but does not increase logical queue depth;
  an over-capacity fixture accepts exactly the requested count and returns Full for the next item.
- Fixed storage exactly equal to the limit succeeds.
- Fixed storage one byte above the limit fails before handle publication.
- A high message count whose fixed slots exceed the byte ceiling fails before allocation when the
  lower bound is sufficient to prove failure.
- Property tests compare enqueue, dequeue, wraparound, close, and drain against `VecDeque`.
- Deterministic barrier tests cover close-vs-insert and receiver-drop-vs-insert at each lock edge.
- Deterministic and Loom tests cover shutdown after predicate evaluation but before Condvar sleep and
  full-queue shutdown; shutdown closes under the queue mutex and cannot lose its wake.
- Loom reduced-model tests cover producer/close/fallible-clone/drop/poison/hint/drain races and prove
  no post-close insert, sender-count wrap, normal post-poison service, or double drain.

### One total and record reservations

- Fixed plus all resident tokens plus all record reservations exactly equal to the ceiling succeeds.
- A ceiling one byte below the required total fails.
- Accounting-core base, channel-state infrastructure, writer-lifecycle core, both actual-capacity
  rings, every fixed scratch capacity, and writer-start destination allocation appear in the fixed
  component with named owners.
- The process destination registry fallibly preallocates exactly 1,024 logical slots, charges its
  observed backing capacity once for process lifetime, never grows, and remains charged after the
  final per-writer reservation drops.
- The one `OnceLock<DestinationFenceRegistryInitializationState>` charges its exact inline size;
  `Ready` and `Failed` first initialization are permanent, same-limit replay is deterministic,
  different-limit replay is typed, and only `Ready` can create the allocation-free process proof.
- Concurrent initialization publishes exactly one state; allocation/budget failure has no vector
  term or usable registry, and no second static or retained failure allocation exists.
- Destination busy, capacity, and terminal registry poison remain distinct
  `CaptureDestinationFenceError` cases; poison permits cleanup but never later acquisition.
- Dropping final channel state releases the channel-state fixed reservation while an external
  accounted snapshot keeps the core base and resident bytes alive; dropping that final snapshot
  releases the resident token and permits core destruction.
- Writer-start budget, scratch, destination busy/capacity/poison, compiled-target/formula/hash proof,
  thread-name, and thread-creation failures retain their canonical nested variants, release the
  prepared writer fixed reservation and lease exactly once, and publish no running handle.
- `WriterFixedStorageReceipt` independently proves scratch, destination lease, thread name,
  spawn/closure/control upper bound, target proof hash, exact/one-under refusal, and final lifecycle
  release.
- Frame and conversion arithmetic overflows remain component-typed.
- Maximum-capacity short identity values affect the charge.
- A dequeued record blocked in the sink remains charged.
- Releasing the sink returns record-reservation bytes to zero.
- Conversion failure releases exactly once.
- Append failure releases exactly once.
- Flush failure releases exactly once.
- Deadline rejection releases exactly once.
- Full, contended, closed, and poisoned send failures release exactly once.
- Cancellation drains and releases exactly once.
- Writer drop drains and releases exactly once.
- Pending-owner drop releases exactly once.
- Multiple records from one generation share one resident token and reserve only proven unique frame
  dynamic plus conversion peak.
- Only accepted bounded `CaptureAccountingSnapshot` values expose diagnostic components; concurrent
  reserve/release and complete ABA cannot yield a torn accepted sum.
- Every production accounting coordination load/RMW uses the frozen SeqCst transition protocol; Loom
  places a writer at every gap in the exact poison/epoch/active/components/active/epoch/final-active/
  poison read sequence.
- Transition/epoch overflow and invariant violation publish one durable inline first-poison reason;
  snapshots return that exact reason after intervening drops, bounded contention remains nonterminal,
  no accepted sample is an invalid benchmark, and diagnostic counters cannot authorize admission.
- Underflow or double release poisons accounting and prevents later admission.
- Normal budget exhaustion does not poison accounting.

### Resident generations and shared health identity

- Empty-record-queue construction charges the complete initial generation.
- Fixed plus initial generation one byte over the ceiling publishes no handle.
- Prepared predecessor and successor simultaneously own separate resident tokens.
- Rejected successor preparation drops only its RAII token and leaves the predecessor unchanged.
- Successful publication moves no accounting category and requires no manual release.
- Current state, retired state, messages, `CapturedRawRecord` wrappers, health events, control
  snapshots, external identity snapshots, and every identity-bearing error clone the same
  accounted-generation identity.
- A `CaptureGenerationError::BindingMismatch` retained across rotation and after channel-state drop
  keeps each referenced generation charged; dropping the last error/snapshot handle releases each
  token exactly once under forced drop-order permutations.
- A predecessor remains resident across rotation until its final handle drops; forced drop-order
  permutations prove final-handle release exactly once.
- Health-event clones add no reservation and keep the resident token alive.
- Clone/equality/debug tests preserve public value semantics without exposing or comparing the
  accounting token.
- Health-slot overflow increments the diagnostic drop counter without changing integrity.
- Exhaustive tests map every authority, retained-size, budget, queue, accounting, sink, journal, and
  deadline error to its exact bounded health reason and assert forbidden secret/evidence payloads are
  absent.
- All current, retired, prepared, and externally retained generations appear in the resident
  diagnostic counter; overflow or underflow is terminal.

### Generation and TIME interaction

- Complete initial generation precomputation is a closed conservative structural upper bound for
  the declared graph.
- Complete successor precomputation occurs before predecessor revocation.
- TIME adds one unique continuity pointee/control allocation.
- The continuity allocation is counted once within the source bundle.
- Exhaustive matching requires a formula change when continuity is added.
- No MEM production patch edits source/domain contracts after the frozen seed.

### MEM refinement boundary

- MEM begins with every queue, accounting-core, resident-identity, frame-footprint admission,
  coherent-snapshot, destination-registry, and reserve/release lifecycle test green and immutable.
- Its conversion RED proves only that the seed's explicitly charged compatibility payload copy still
  exists, allocation identity is not yet enforced at conversion, or the reservation does not yet
  span append/record-triggered flush. It does not expect footprint construction, admission limits,
  or queue failure release to be absent.
- Its writer RED covers the complete `WriterFixedStorageReceipt` and proof artifact behind the seed's
  frozen fixed-component API; it does not change total arithmetic or component ownership.
- Its journal RED covers two-pass streaming and the separate journal fixed ledger.
- Its memory-sink RED names the exact conservative seed dynamic quote and the internal refinement
  that remains; count bounds, fallible fixed allocation, no-growth, and the public constructor are
  already green seed capabilities.
- A MEM test that requires changing a public queue, accounting, publisher, frame, registry, or sink
  contract is a seed-design correction and stops the lane rather than being implemented in parallel.

### Journal

- First-pass count equals the actual second-pass body length.
- First-pass CRC equals the actual second-pass body CRC.
- Streamed bytes equal a canonical JSON fixture.
- Maximum valid serialized record succeeds.
- One-byte oversize record fails before body output.
- Second-pass write failure never reports durable append.
- Recovery accepts complete prior frames and handles a truncated final frame.
- No payload-scale serialization Vec remains in the production append path.
- Reservation remains live through policy-triggered flush.
- Observed `BufWriter::capacity()` is charged to the separate fixed journal-sink ledger; exact fit
  succeeds, one-byte-under budget fails construction, and the charge remains until sink drop.

### Retaining sink

- Constructor arithmetic overflow is typed.
- `try_reserve_exact` failure is typed and publishes no sink.
- Observed capacity, even above `max_records`, is charged as fixed storage.
- Fixed storage one byte above the sink ceiling fails construction.
- Exact record count succeeds.
- One additional record fails.
- Exact retained-byte limit succeeds.
- One-byte-under retained limit fails.
- Fixed inline slots are not charged per append, and accounted generation/resident bytes are never
  copied into the sink ledger.
- Dynamic source and payload charges follow the exact documented per-record conservative formula;
  arithmetic/refusal leaves records and counters unchanged.
- The vector never grows after construction.
- Retained records remain charged to the sink after queue reservation release.
- Sink clear/drop returns retained accounting to zero without invariant failures.
- Public construction cannot select an unbounded default.
- Negative static/trybuild assertions prove `MemoryCaptureSink: !Default`, and all 18 current default
  call sites pass explicit nonzero count/byte limits and propagate construction failure.

### Performance

Before queue replacement, but after every queue-independent retained contract, `CapturePayload`/
frame/raw-record ownership change, conversion compatibility quote, mechanical module split, and the
final endpoint/collector harness are committed, the serialized owner adds Criterion 0.8.2 and commits
that still-standard-channel code as a clean `A4_BASELINE_CODE_HEAD`. The benchmark executes from that
unchanged clean head rather than only compiling. Only after execution does the owner write and review
the nonempty report under `docs/reports/performance/`, then commit that report in the distinct
descendant `A4_BASELINE_EVIDENCE_HEAD`. The report names `A4_BASELINE_CODE_HEAD` as the measured
commit; the evidence commit must not claim it was the clean code SHA that it necessarily postdates.
`A4_SEED_HEAD` descends from the reviewed evidence head. A baseline reconstructed after queue
replacement is not evidence.

The benchmark source is hash-partitioned so intentional backend replacement cannot be confused with
fixture drift. The baseline commit freezes these canonical partitions:

~~~text
comparable contract modules
    collector              deterministic sample collection and quantiles
    endpoints              the five named timing boundaries
    fixture                matrix/full/sustained definitions and validity rules
    producer_inventory     producer derivation and operation quotas
    schema                 outcome reconciliation and result schema
    workload               accounting-snapshot and RSS execution rules

backend modules
    standard          std::sync::mpsc producer/consumer adapter
    candidate         production fallible fixed-ring adapter, including the
                      benchmark-only forced-lock barrier and QueueContended assertion
~~~

The manifest records the exact `immutable_module_sha256` object with separate `collector`,
`endpoints`, `fixture`, `producer_inventory`, `schema`, and `workload` members. It separately records
`entrypoint_sha256`, `backend`, and `backend_sha256`; the candidate-only forced-lock code is inside,
and therefore covered by, the separately hashed candidate backend rather than an allegedly immutable
module. The manifest also records the exact expected fixture set, result schema, relative
evidence-local executable path and digest, measured code head, production libraries linked into the
executable, and host, toolchain, and release-profile fingerprints. Baseline and candidate
comparability requires an identical `immutable_module_sha256` object, fixture parameters, producer
inventory, result schema, host/toolchain/release-profile fingerprints, payloads, depths, operation
quotas, endpoints, collector, timers, and `entrypoint_sha256`. The entrypoint remains immutable after
the standard baseline; the candidate lane may replace only the separately hashed `backend` module.
The backend hash must differ; executable hashes are expected
to differ and are never hidden inside a falsely equal "whole harness" hash. Later
zero-copy/journal/sink work may affect writer endpoints, so reports compare each endpoint by name and
never attribute an overall writer delta solely to the ring.

Every invocation sets `CAPTURE_BENCH_BACKEND` explicitly. The baseline accepts only `standard`; the
candidate accepts only `candidate`; missing, unknown, or head-incompatible values exit nonzero before
measurement. The standard invocation requires
`CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,sustained_rss`; the candidate requires
`CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss`. Any duplicate,
missing, extra, or reordered fixture fails before measurement. Candidate startup also requires
`CAPTURE_BENCH_BASELINE_MANIFEST`, recomputes every individual immutable-module hash, and exits before
measurement unless each hash and the host, toolchain, and release-profile fingerprints match the
persisted standard manifest. Wave 1A puts producer duplication behind one benchmark-only fallible
factory: the standard implementation wraps its then-infallible clone in `Ok`, while the candidate
fixed-ring implementation calls production `try_clone`. Workload and outcome accounting stay
identical while the backend's authority contract changes.

The committed fixed-operation matrix is exact:

- payload lengths: 0, 1,024, and 4,194,304 bytes;
- queue depths: 1, 64, and the production default 16,384;
- numeric producer cases: 1, 2, 4, and 8; and
- one separately reported representative fan-in equal to the nonzero sum of producer tasks in the
  committed adapter fixture. The report lists every contributing task and the derived integer,
  deduplicates execution when it equals 1/2/4/8 while retaining both labels, and rejects zero,
  overflow, or an undocumented value.

At this audit base, the representative inventory is exactly one producer: `run_source` creates one
`SupervisedSourceTask`, which owns one sequential `MarketSource::run_session`; Coinbase and mock
capture publication occurs inline in that task; event analysis and the writer own no publisher; and
Coinbase creates no production child capture producer. The deterministic Coinbase fixture likewise
moves its only publisher into one source task. The checked representative sum is therefore `1` and
is executed once while retaining both the numeric-1 and `representative` result labels. A changed
inventory is a mandatory refresh, not permission to use `available_parallelism` or another host-
dependent substitute.

Eight is the explicit synthetic fan-in stress case, not an assertion about current production
adapter count. Each throughput case requests
`max(1_000_000, producer_count * 100_000)` operations. Each producer has a checked nonzero quota; the
harness rejects overflow while deriving the aggregate. The throughput/latency matrix is
operation-bounded, not epoch-bounded. A case starts when its barrier releases, terminates only after
every requested operation has one typed outcome, the consumer has accounted for every success, and
every producer joins, and uses that exact interval for throughput. It does not inherit the sustained
RSS epochs. Criterion comparison groups may partition these fixed operations into a recorded sample
schedule but may not silently substitute a time quota. Requested, completed, successful, and every
typed refusal count reconcile exactly.

Timing endpoints are named and never conflated:

- queue push latency: immediately before `try_push` through its returned `Enqueued` or typed refusal;
- queue pop latency: immediately before `try_pop` through its returned item or typed empty/terminal
  result;
- capture-admission latency: immediately before `RawCapturePublisher::try_publish` through returned
  receipt or typed publication refusal; it excludes writer and sink completion;
- writer-append latency: admitted-message dequeue through `CaptureSink::append` return; and
- flush-inclusive writer latency: admitted-message dequeue through the policy-triggered `flush`
  return, reported separately from append-only latency.

None of these is called overall event-to-decision latency. The Q2-A4 sub-millisecond threshold applies
only to warmed capture admission; the product event-to-decision target requires a later end-to-end
fixture covering decode, validation, sharding, features, strategy/inference, and risk.

The harness uses fallibly preallocated per-producer latency arrays with a checked aggregate capacity
of exactly 1,000,000 samples per case. Because every requested operation count is known before the
barrier, each producer computes its deterministic nonzero sampling stride before execution and
records that stride; it never fills an early prefix and starts dropping only after saturation.
Producers write only declared stride samples into disjoint slices. The harness joins every producer
before reading or merging counters and samples. Collector overflow, an out-of-stride write, an
unjoined producer, zero requested or completed operations, zero samples, count mismatch, or a
zero-duration interval invalidates the run. Collector allocation is outside capture structural
counters, but its exact preallocated bytes are reported and included in process RSS. Criterion's
default statistics never substitute for project p50/p95/p99/maximum.

One externally selected repetition is one indivisible fixture sequence—not one independently repeated
subfixture:

~~~text
standard repetition
    one complete fixed-operation matrix
    + one comparable deterministic depth-one QueueFull fixture
    + one representative sustained RSS fixture

candidate repetition
    one complete fixed-operation matrix
    + the identical comparable deterministic depth-one QueueFull fixture
    + one candidate-only deterministic forced-lock QueueContended fixture
    + the identical representative sustained RSS fixture
~~~

The baseline full fixture and candidate full fixture share every immutable-module and fixture hash,
gate the consumer until a depth-one queue refuses, and are invalid unless `QueueFull > 0`. Only the
candidate fixed-ring backend has the internal lock authority needed for the forced-lock fixture; its
separately hashed backend code is invalid unless `QueueContended > 0` and is noncomparative
structural evidence. The
unsaturated 1,024-byte, depth-16,384 representative case is a labeled case within the matrix, not a
fourth separately repeated benchmark. For that unsaturated representative case, candidate acceptance
requires at least 100,000 successful admissions/second, warmed capture-admission p99 strictly below
one millisecond, nonzero successes and samples, zero publication refusals, zero accounting invariant
failures, and an accepted post-drain `CaptureAccountingSnapshot` with record reservation zero.
Snapshot contention is counted separately and retried outside latency endpoints; no accepted
snapshot invalidates the repetition. Every fixture reconciles attempts, typed outcomes, consumer
successes, and post-drain ledgers exactly.

Accepted structural snapshots are primary memory evidence. RSS is supplemental but mandatory for the
host-performance claim. The sustained fixture runs only the representative 1,024-byte, depth-16,384
case in a dedicated process: two five-second warm-up epochs followed by ten ten-second measured
burst/drain epochs, with process RSS sampled every 100 milliseconds through the documented platform
API and page size. Each epoch completes and joins its current checked batch, drains, obtains one
accepted snapshot, and then records post-drain RSS. Record pre-warm, per-epoch peak, immediate
post-drain, final RSS, and the accepted snapshot after every drain. The time-bounded batch count is
never reported as the matrix operation quota. The final post-drain RSS must be no more than
`max(8 MiB, 5% of first_measured_post_drain_rss)` above the first measured post-drain value, and the
final five measured post-drain values must not each establish a new strict maximum. Failure requires
investigation; allocator retention is not silently relabeled as a pass. An unavailable RSS source or
sampling perturbation beyond the recorded allowance leaves the host-memory claim unapproved.

Every clean baseline and candidate evidence repetition—not only the sustained child process—runs on
the same documented otherwise-idle host. The integration owner first pauses every other
implementation/review agent. Before preflight, the owner acquires the repository's exclusive
capture-benchmark evidence lock and retains it through postflight; failure to acquire the lock
forbids an evidence run. The manifest records an idle-host preflight and postflight: load, a complete
competing-process inventory (including any other `cargo`, `rustc`, or benchmark process), power mode,
thermal state when available, CPU affinity or scheduler policy when configured, and any deviation. A
known competing workload, another build/benchmark process, active repository agent, thermal
throttling, power-mode change, sleep/wake, host change, or lost evidence lock invalidates comparative
and absolute performance claims; the run is retained only as diagnostic output. Dirty-tree
exploration has no evidence status.

An externally supplied `CAPTURE_BENCH_REPETITION` executes exactly one sequence above. When the
selector is absent, the harness may orchestrate exactly five repetitions once; an outer five-run loop
and internal five-run mode must never be combined. The manifest records selection mode and rejects a
duplicate or missing repetition identifier.

Raw evidence is written directly to one absolute, ignored integration-owner artifact root derived
from the common Git directory's repository root, never to an expendable lane worktree's relative
`target/`. The owner copies the exact built executable into the unique evidence directory as
`capture_admission-exe`, verifies its hash and executable bit, and runs that immutable copy so a
later build cannot replace the measured binary. Every repetition writes a unique directory; no run
overwrites another. Before lane handoff, the producer writes a nonempty top-level manifest and
path-sorted `SHA256SUMS`, then the integration owner verifies every digest, executable bit,
repetition count, fixture inventory, measured clean code head, and module/input hash. The handoff
records the absolute artifact reference and manifest digest in the committed report and canonical PR
comment. A lane worktree cannot be removed until that verified artifact root is reachable from the
integration worktree and no running process uses it.

A later exact-head gate may reuse raw evidence only when checksums pass; the benchmark executable,
production-library, entrypoint, every individual immutable-module, backend-module, fixture,
producer-inventory,
result-schema, toolchain, target, release-profile, and host hashes are all identical; and a recorded
tree diff proves that intervening commits touch only `docs/architecture/`, `docs/plans/`,
`docs/reports/`, or `docs/project-memory.md`. The report names both the measured code head and the
later code-equivalent candidate and never relabels the latter as the measured head. Any other path,
source, dependency, feature, compiler, target, fixture, backend, executable, or code-affecting
integration change—including a rebase that changes the executable—requires a fresh five-repetition
run. Missing raw artifacts, a stale relative path, an unverifiable digest, or an incomplete handoff
forbids reuse.

## Implementation DAG and grouped worktrees

Do not create one worktree per test or small task. Use one serialized two-barrier seed sequence and
two parallel grouped implementation worktrees only after its final barrier freezes.

### Wave 0: research and refresh

This report is the Wave 0 research artifact. The integration owner:

1. retains `ab3f7c1` as the locally approved A3 production anchor and records the hosted billing
   condition only as optional evidence;
2. reviews and integrates the Wave 0 documentation on a clean descendant;
3. refreshes code inventories, formulas, paths, interfaces, lock state, and baseline evidence on
   that exact descendant;
4. publishes file ownership and integration order; and
5. keeps shared manifests, lockfile, production application composition, and authority handoff
   serialized.

Wave 0 exits on a reviewed documentation descendant whose production-tree equivalence to the
approved A3 anchor is recorded and whose fresh local baseline is green. Hosted runner availability
is not an exit condition.

### Wave 1A: serialized standard-channel baseline barrier

One owner and one grouped worktree first own the contract changes that can coexist with the current
standard channel:

- behavior-preserving capture/admission module split;
- typed domain retained-size contracts and distinct platform underreporting;
- domain identity helper;
- checked sized-Arc and `Arc<[u8]>` structural layout helpers;
- all four bundle implementations;
- deliberate frame/raw-record `CapturePayload` ownership, the final `RawCaptureFrameView`
  accessor/footprint contract, the required active-admission
  `checked_resident_shared_frame_bytes` seam, and all frame/admission implementations;
- Criterion 0.8.2 dev-dependency addition through the serialized manifests/lockfile owner; and
- the complete bounded benchmark harness while the production queue is still the standard channel.

This list is ordered as one queue-independent barrier: contracts, payload ownership, frame formulas,
the conservative compatibility-copy reservation quote, module split, and the final complete harness
all land and pass before measurement. The standard channel remains unchanged throughout that barrier.
Measuring immediately after adding only the harness and then changing payload/contracts is forbidden
because it would not isolate the queue candidate.

The owner then runs focused tests and commits the unchanged standard-channel backend plus complete harness
as clean `A4_BASELINE_CODE_HEAD`. The actual release benchmark runs from that exact unchanged head.
The owner then persists and reviews the report, verifies that it names and hashes the measured code
head, and commits only the evidence artifact as the distinct descendant
`A4_BASELINE_EVIDENCE_HEAD`. Compiling with `--no-run` is necessary but not sufficient. No fixed-ring
production code may land before both baseline barriers.

### Wave 1B: serialized final API and migration seed

The same owner then replaces the standard channel and owns all shared API/composition changes:

- the final platform-owned fixed ring, mutex-authoritative sender count, fallible producer clone,
  requested logical capacity distinct from observed backing capacity, mutex-linearized out-of-band
  shutdown close, drain, poison, and deterministic/Loom queue tests;
- the fallibly preallocated never-growing process destination-fence registry, its process-lifetime
  ledger, `capture/writer/destination.rs`, and all registry initialization/churn/drop tests;
- final `CaptureChannelLimits`, fallible channel construction, preparation/rotation error surfaces,
  and full configuration precedence;
- the complete channel accounting core and authoritative checked total, including the fixed channel
  reservation admitted before handle publication;
- resident-generation reservations, private RAII accounted identities, preparation/rotation
  lifetimes, and identity-bearing error/drop behavior;
- publisher enforcement of the frozen active-admission pointer proof, checked frame-footprint record
  reservations, and reservation transfer through queue ownership;
- fixed/resident/record/total diagnostics, accounting-invariant counters, construction/rotation/
  publication/drain/drop lifecycle tests, bounded coherent accounting snapshots with transition/
  epoch ABA tests, and accounting poison behavior;
- the final publisher, writer, sink, and bounded `MemoryCaptureSink::try_new` public APIs;
- final construction/count bounds for the never-growing memory sink;
- complete typed errors and exhaustive health-reason variants;
- all refreshed channel, publisher-clone, sink-constructor, application composition, and test call
  migrations, including the currently inventoried 39 channel calls, ten direct publisher clones,
  one `CaptureContext` Clone derive, two positive publisher-Clone static assertions, and 18 default
  memory-sink constructions;
- Criterion/Loom manifests and lockfile; and
- seed RED/GREEN tests covering the frozen APIs.

The source and domain frame fixes are seed work because the later MEM lane is platform-owned. The
approximately 741-line platform capture module is split by moving existing admission/generation
behavior into a focused module before adding new behavior. No empty scaffolding crate or module is
created.

Freeze `A4_SEED_HEAD` only after the fixed ring and bounded sink are real production capabilities,
every migrated call compiles, focused domain/source/platform/app and Loom gates pass, and the exact
standard-channel report still names `A4_BASELINE_CODE_HEAD`. `A4_SEED_HEAD` must descend from
`A4_BASELINE_EVIDENCE_HEAD`; TIME and MEM branch only from this later clean seed. The baseline code,
baseline evidence, and seed heads are intentionally distinct serialized commits.

### Wave 2: parallel TIME and MEM

Create exactly two grouped worktrees from the frozen seed.

TIME owns:

- source authority-time state;
- registry session/time separation;
- removal of caller-authored received time;
- all 23 .try_frame invocation migrations;
- continuity allocation accounting; and
- source/live TIME tests.

MEM owns:

- the complete `WriterFixedStorageReceipt`, compiled-target proof artifact, writer-start reservation/
  lifetime, and fixed conversion/serialization scratch behind the frozen Wave 1B accounting and
  writer APIs; the process destination registry itself remains serialized Wave 1B ownership;
- shared-Arc zero-copy frame-to-record conversion using the frozen frame-footprint reservation;
- journal two-pass streaming and its complete fixed sink ledger;
- exact memory-sink dynamic ledger and invariant hardening behind its frozen bounded API;
- platform/app writer, conversion, journal, sink, and integrated-memory tests that consume—not
  redefine—the Wave 1B accounting API; and
- fixed-ring/capture candidate benchmarks using the persisted baseline harness.

Wave 1A freezes the admission pointer-proof contract and its source/diagnostic implementations.
Wave 1B is then the only owner of the platform channel accounting core, fixed channel reservation,
resident-generation/accounted-identity RAII, publisher enforcement of that pointer proof,
frame-footprint record reservations, accounting counters/snapshot protocol, and their
construction/publication/rotation/drain/drop lifecycle tests. MEM may exercise those frozen
capabilities in integration fixtures but may not reimplement, rename, or move them. This makes the
serialized seed a real accounting capability rather than an API shell whose authority is completed
concurrently.

MEM RED tests are limited to behavior that the safe seed deliberately has not finalized: the
compatibility-copy term is nonzero until allocation identity is proved, writer-start has no complete
receipt/proof until MEM installs it, journal append still lacks two-pass streaming, and the bounded
memory sink still needs its explicitly named dynamic-ledger refinement. Seed-owned footprint
construction, admission exact/one-over, authoritative reserve/release, accounting snapshots,
identity-token drop, queue close/drain, and registry tests are immutable green prerequisites in MEM;
MEM may add integration assertions over them but may not claim they are absent or rewrite their
authority. The seed documents any conservative temporary memory-sink dynamic quote precisely so the
seed remains safely bounded before MEM refines it.

TIME does not edit platform MEM production files. MEM does not edit domain or source production
contracts. Production application composition, manifests, lockfile, and integration conflict
resolution remain integration-owner hotspots. The seed owner performs every configuration rename,
dependency change, final fixed-ring/sink API decision, and production call migration before
branching; MEM may edit only the embedded app-main test module after the seed freezes production
composition. If MEM discovers that accounting or journal hardening requires a public queue, publisher,
or sink API change, it stops at a documented seed-design correction rather than creating a second
conflicting authority surface.

Integrate TIME first so its new continuity field updates the exhaustive source formula. Integrate
MEM second onto that result. Run combined focused gates after each transfer.

A third implementation writer is not safe because remaining files are shared authority contracts,
configuration, application composition, or integration tests. A remaining concurrency slot may be
used for read-only review, fixture evaluation, or benchmark observation without overlapping
writes.

### Wave 3: Q2 checkpoint

After integration:

1. update current-state, target-state, gap-analysis, and implementation-plan truth;
2. freeze one clean exact Q2 candidate;
3. run the full local exact-head gate;
4. record hosted evidence separately if available, without making it a local approval prerequisite;
5. run the grouped quarter-checkpoint specialist reviews;
6. union and deduplicate findings before remediation;
7. resolve every substantiated Critical, Important, and Minor finding;
8. re-review the exact remediation head;
9. rerun unchanged exact-head verification; and
10. remove clean integrated lane worktrees normally and prune metadata.

Fresh independent reviews occur at the quarter checkpoint, not after every small task. A re-review
that closes a rejected checkpoint's findings is required remediation, not a new task-level review
round.

## Verification and performance evidence

Focused seed commands include:

~~~bash
cargo test -p market-squawk-domain --all-features --locked
cargo test -p market-squawk-sources --all-features --locked
cargo test -p market-squawk-platform --all-features --locked
cargo test -p market-squawk --all-features --locked

cargo clippy \
  -p market-squawk-domain \
  -p market-squawk-sources \
  -p market-squawk-platform \
  -p market-squawk \
  --all-targets \
  --all-features \
  --locked \
  -- \
  -D warnings
~~~

At the Wave 1A barrier, the standard-channel owner first records clean
`A4_BASELINE_CODE_HEAD` only after the queue-independent contracts, payload/formula work, mechanical
split, and final hash-partitioned harness are committed, then executes the benchmark with the explicit
`standard` backend without changing that checkout. `--no-run` is a compile gate only and never
substitutes for execution. The nonempty report is written afterward, reviewed against the measured
code-head, executable, comparable-module, backend, fixture, schema, host, and artifact hashes, and
committed as distinct descendant `A4_BASELINE_EVIDENCE_HEAD`. At MEM/candidate verification, the
explicit `candidate` backend (the production fixed ring) emits its report after every producer joins.
Each standard repetition contains matrix plus comparable full plus sustained; each candidate
repetition contains those same fixtures plus the forced-lock fixture covered by the separately
hashed candidate backend. Both run on the documented otherwise-idle host and write directly to the
persistent absolute integration-owner artifact root.

The integrated exact candidate runs:

~~~bash
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
~~~

It also runs the repository's dependency, vulnerability, license, credential, generated-artifact,
fuzz-compilation, documentation, and exact-head cleanliness gates.

Focused lane evidence is not checkpoint approval. Performance acceptance requires:

- hardware model and memory;
- operating system and target;
- exact Rust and dependency versions;
- measured exact commit and any separately named code-equivalent documentation descendant;
- release profile;
- explicit backend, executable hash, the exact `immutable_module_sha256` object, backend-module hash,
  fixture/result-schema hashes, and verified artifact-manifest digest;
- baseline and candidate fixture plus requested and completed producer-operation counts;
- queue capacity and byte ceiling;
- producer count and payload distribution;
- successful throughput plus separate full, contended, closed, and rejected counts;
- custom-collector p50, p95, p99, and maximum latency;
- accepted coherent structural-snapshot peak plus snapshot-contention count;
- supplemental RSS peak; and
- sustained-burst and post-drain memory behavior; and
- idle-host preflight/postflight plus the persistent artifact handoff/reuse decision.

Criterion 0.8.2 is planned rather than locked at this audit base. Wave 1A must add it before recording
the standard-channel code head so baseline and candidate share the exact comparable-module, fixture,
endpoint, collector, schema, release-profile, and host hashes while reporting their backend hashes
separately. Criterion supplies controlled iteration and throughput measurement; the project-owned
bounded preallocated collector supplies the required latency quantiles and maximum.
`A4_BASELINE_CODE_HEAD`, `A4_BASELINE_EVIDENCE_HEAD`, `A4_SEED_HEAD`, and the exact candidate head
remain distinct evidence anchors. Full saturation and candidate-only mutex contention are separate
outcomes, producer-side counts are read only after every producer joins, and all validity, threshold,
idle-host, persistent-handoff, and RSS rules in the Performance section are checkpoint gates rather
than optional metadata. The fixed-operation matrix and long representative RSS fixture retain their
different termination rules even though one external repetition executes each exactly once; a
command or report that conflates their quotas, duplicates a subfixture, omits the comparable full
fixture, or loses the raw artifact handoff is invalid.

## Re-audit gates

Refresh the formula and research when any of these changes:

- Rust toolchain or target architecture;
- Arc private layout assumption;
- standard channel source used by comparison fixtures;
- legacy `bytes` version before migration, or the owned payload representation afterward;
- Tokio, serde_json, or UUID version used in the capture graph;
- queue slot type or CaptureMessage fields;
- queue implementation, capacity, or storage container;
- queue core synchronization fields;
- requested logical queue depth versus observed backing capacity;
- sender-count ownership, producer-clone contract, or last-sender linearization;
- accounting transition/epoch/snapshot representation or sampling policy;
- CaptureAuthorityBundle fields or implementation count;
- FrameSessionIdentity, FrameSessionBinding, or lease state;
- TIME continuity ownership;
- RawCaptureFrameView fields, footprint decomposition, or implementation count;
- live, compatibility-payload, or committed journal-body ceilings;
- identity maximum lengths or backing representation;
- generation wrapper fields;
- accounted-generation identity or reservation-token ownership;
- writer conversion sequence or UUID derivation;
- writer fixed receipt, thread-runtime proof artifact, destination lease, or process destination
  registry representation/capacity;
- journal encoding, checksum, framing, or BufWriter ownership;
- sink trait ownership, sink fixed/dynamic formula, or any sink that retains records;
- health/control queue representation or error-to-health mapping;
- capture queue-capacity or memory-ceiling configuration semantics;
- Criterion version, benchmark fixture/harness/endpoints, producer matrix, collector, or RSS method;
- compiler optimization or feature set that changes compiled size; or
- allocator/RSS claims.

Every re-audit records:

- old and new exact versions;
- affected ownership graph;
- recalculated checked formulas;
- new compiled size/capacity fixtures;
- boundary tests;
- performance comparison; and
- the exact verified commit.

Dependency automation must not merge a payload, runtime, or serialization update solely because
ordinary tests pass. The retained-layout and conversion-overlap gates are required.

## Sources

### Repository sources

- [Q2 A4 capture authority preflight](../superpowers/plans/2026-07-17-q2-a4-capture-authority-preflight.md)
- [Q2 checkpoint review](../reports/q2-checkpoint-review.md)
- [Current architecture](../architecture/current-state.md)
- [Target architecture](../architecture/target-state.md)
- [Gap analysis](../plans/gap-analysis.md)
- [Project memory](../project-memory.md)
- [Hosted Actions account-gate audit](../audits/2026-07-17-hosted-actions-account-gate.md)
- [Lane B live-memory accounting](2026-07-16-live-memory-accounting.md)
- [Source-authority memory reservation](2026-07-17-source-authority-memory-reservation.md)
- [Domain capture contracts](../../crates/market-squawk-domain/src/capture.rs)
- [Domain capacity-sensitive identity](../../crates/market-squawk-domain/src/identity.rs)
- [Source capture authority](../../crates/market-squawk-sources/src/capture.rs)
- [Source live frame and binding](../../crates/market-squawk-sources/src/live.rs)
- [Platform capture admission](../../crates/market-squawk-platform/src/capture.rs)
- [Capture writer](../../crates/market-squawk-platform/src/capture/writer.rs)
- [Capture sinks](../../crates/market-squawk-platform/src/capture/writer/sink.rs)
- [Capture destination registry](../../crates/market-squawk-platform/src/capture/writer/destination.rs)
- [Journal](../../crates/market-squawk-platform/src/journal.rs)

### Rust normative documentation

- [Rust 1.97 Vec try_reserve_exact](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.try_reserve_exact)
- [Rust 1.97 Vec guarantees](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#guarantees)
- [Rust 1.97 Vec into_boxed_slice](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.into_boxed_slice)
- [Rust 1.97 Arc](https://doc.rust-lang.org/1.97.0/std/sync/struct.Arc.html)
- [Rust 1.97 Layout](https://doc.rust-lang.org/1.97.0/std/alloc/struct.Layout.html)
- [Rust 1.97 Mutex try_lock](https://doc.rust-lang.org/1.97.0/std/sync/struct.Mutex.html#method.try_lock)
- [Rust 1.97 mutex poisoning](https://doc.rust-lang.org/1.97.0/std/sync/struct.Mutex.html#poisoning)
- [Rust 1.97 Condvar](https://doc.rust-lang.org/1.97.0/std/sync/struct.Condvar.html)
- [Rust 1.97 BufWriter capacity](https://doc.rust-lang.org/1.97.0/std/io/struct.BufWriter.html#method.capacity)
- [Rust 1.97 PathBuf capacity](https://doc.rust-lang.org/1.97.0/std/path/struct.PathBuf.html#method.capacity)
- [Rust 1.97 sync_channel](https://doc.rust-lang.org/1.97.0/std/sync/mpsc/fn.sync_channel.html)
- [Rust checked usize addition](https://doc.rust-lang.org/1.97.0/std/primitive.usize.html#method.checked_add)
- [Rust checked usize multiplication](https://doc.rust-lang.org/1.97.0/std/primitive.usize.html#method.checked_mul)

### Pinned implementation evidence

- [Rust 1.97 ArcInner source](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/alloc/src/sync.rs#L388-L396)
- [Rust 1.97 standard MPMC array channel source](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/std/src/sync/mpmc/array.rs)
- [Rust 1.97 standard channel counter source](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/std/src/sync/mpmc/counter.rs)
- [Rust 1.97 thread runtime source](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/std/src/thread/mod.rs)
- [Dmitry Vyukov bounded MPMC queue referenced by Rust](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue)

### Locked crate documentation

- [Bytes 1.12.1](https://docs.rs/bytes/1.12.1/bytes/struct.Bytes.html)
- [Tokio 1.52.4 bounded MPSC](https://docs.rs/tokio/1.52.4/tokio/sync/mpsc/index.html)
- [Tokio 1.52.4 MPSC allocation behavior](https://docs.rs/tokio/1.52.4/tokio/sync/mpsc/index.html#allocation-behavior)
- [Tokio 1.52.4 clean shutdown](https://docs.rs/tokio/1.52.4/tokio/sync/mpsc/index.html#clean-shutdown)
- [Loom 0.7.2 model documentation](https://docs.rs/loom/0.7.2/loom/model/fn.model.html)
- [serde_json 1.0.150 to_writer](https://docs.rs/serde_json/1.0.150/serde_json/fn.to_writer.html)
- [UUID 1.24.0 new_v5](https://docs.rs/uuid/1.24.0/uuid/struct.Uuid.html#method.new_v5)

### Planned benchmark tooling, not locked at the audit base

- [Criterion 0.8.2 documentation](https://docs.rs/criterion/0.8.2/criterion/)
- [Criterion measurement-time guidance](https://criterion-rs.github.io/criterion.rs/book/user_guide/advanced_configuration.html)

### Evaluated queue alternatives

- [Crossbeam channel documentation](https://docs.rs/crossbeam-channel/0.5.15/crossbeam_channel/)
- [Crossbeam repository](https://github.com/crossbeam-rs/crossbeam)
- [Crossbeam channel advisory fixed in 0.5.15](https://github.com/advisories/GHSA-pg9f-39pc-qf8g)
- [Flume 0.12 documentation](https://docs.rs/flume/0.12.0/flume/)
- [Flume 0.12 source](https://docs.rs/flume/0.12.0/src/flume/lib.rs.html)
- [Thingbuf 0.1.6 documentation](https://docs.rs/thingbuf/0.1.6/thingbuf/)

### Optional hosted status evidence

- [GitHub Actions run 29564138664](https://github.com/Sawmonabo/market-squawk/actions/runs/29564138664)
