# Q2 A4 Capture Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to execute this plan. Every implementation lane also
> uses `superpowers:test-driven-development`; every handoff uses
> `superpowers:verification-before-completion`.

**Goal:** Close the historical Q2/A4 capture-authority audit identity as Quarter 1 of 4 with
source-owned trusted receipt time and a complete, checked, bounded capture-memory graph that remains
outside the event-to-action path.

**Architecture:** A documentation-only Wave 0 head descends from the locally approved A3 commit and
changes no production tree. A mandatory clean-head refresh then freezes the actual inventory,
formulas, tools, and lockfile graph before implementation. Exactly three grouped implementation
worktrees are used: one serialized A4.0 seed, then disjoint TIME and MEM lanes from the frozen seed.
The seed owns dependencies, the ownership-preserving payload seam, the safe fixed rings, fallible
construction, all shared call migration, and both closed benchmark backend implementations. TIME
integrates first, MEM rebases onto TIME, and the first clean fully integrated descendant passes the
reviewed standard-reference freeze barrier before one unchanged candidate receives the grouped
Quarter 1 of 4 checkpoint.

**Tech Stack:** Rust 1.97.1, Edition 2024, checked `Layout`/`usize` arithmetic, invariant-preserving
`CapturePayload` ownership over `Arc<[u8]>`, RAII reservations, `Mutex`/`Condvar`, Loom, Serde
streaming, Criterion 0.8.2, Cargo, and optional GitHub Actions portability evidence.

Rust 1.97.0 is forbidden for implementation, benchmark, checkpoint, or release evidence because
of its upstream critical LLVM miscompilation. Every command result and private-layout assumption
recorded under 1.97.0 is historical only and must be regenerated or explicitly revalidated under
the exact 1.97.1 production toolchain before A4 approval.

### Thin-test integration correction (2026-07-18)

The expected-RED classifier, its fixture matrix, verification-script parser tests, synthetic Loom
wrapper tests, and synthetic rustdoc-parser tests were development-process scaffolding and are not
release gates. They were removed during canonical integration. Later plan steps that name those
artifacts are historical execution notes, not instructions to recreate them. The retained release
gates execute the real compile-fail contracts, compiler-derived implementation inventory, candidate
backend build, Loom models, security tools, and end-to-end behavior directly.

### Binding measurement-scope correction (2026-07-17)

#### Reviewed standard-reference rebaseline (binding remediation)

This subsection is the authoritative A4 benchmark provenance and sequencing contract. Its reviewed
design base is exact commit `6d0f71ce0c836feb3522fffa360b8adcf85fc55d`. That commit is an audit
and design base only; it is **not** a standard-reference head, measured-code head, performance
baseline, or approved candidate. Do not substitute a fabricated or abbreviated future SHA.

All earlier A4 artifacts that labeled the production `FixedQueue` transport as `standard`, described
it as a historical standard-channel run, or derived a comparison from that mislabeled identity are
invalid. They cannot supply a threshold, manifest, lock, hash, host fingerprint, sample, approval
claim, or carry-forward input. Delete or quarantine such generated artifacts before the authoritative
run. In the corrected contract, `standard` means only the benchmark-only reference implementation
backed by `std::sync::mpsc::sync_channel`. It is not a claim about historical production behavior and
is not a production capture transport. It is compiled only with the `capture-benchmark` feature and
the closed standard backend selector.

The standard reference wraps `sync_channel` with one combined atomic lifecycle word whose closed
bit and checked active send/clone count share a single modification order. Send and clone first
register through a CAS that rejects the closed bit; close and receiver drop set that bit atomically,
reject new registration, and wait for the encoded count to reach zero. An admitted operation may
finish before close returns, while an operation that loses the registration race returns closed; no
send or clone succeeds after a completed close/receiver drop. Deterministic pre-CAS and
post-registration interleavings plus model checks cover send/clone/receiver-drop linearizability.
Split lifecycle-state and active-operation atomics are not an acceptable reference implementation.

No authoritative measurement may run during the dirty seed remediation or from the design base. The
future `A4_STANDARD_REFERENCE_HEAD` is the first clean descendant of the design base that:

1. contains the reviewed queue, writer, benchmark, TIME, and MEM integration and every accepted
   remediation;
2. contains both closed benchmark backend sources and gates the standard transport behind
   `capture-benchmark`;
3. passes the exact-head Rust 1.97.1 full verification and independent reference-freeze review; and
4. has its complete 40-hex commit and clean tree rederived immediately before the standard run.

That descendant becomes a reference only after those gates pass and the integration owner assigns
its rederived full commit to `A4_STANDARD_REFERENCE_HEAD`. The review records that exact value in the
standard manifest and standard lock; this plan intentionally does not predict it. A dirty tree,
unreviewed descendant, abbreviated revision, placeholder, or artifact generated before the freeze
barrier has zero evidence authority.

At `A4_STANDARD_REFERENCE_HEAD`, the following files are the closed immutable comparison harness and
must be hashed independently under the stated manifest identities:

```text
benchmark_identity  crates/market-squawk-platform/benches/capture_admission/benchmark_identity.rs
collector           crates/market-squawk-platform/benches/capture_admission/collector.rs
endpoints           crates/market-squawk-platform/benches/capture_admission/endpoints.rs
evidence_io         crates/market-squawk-platform/benches/capture_admission/evidence_io.rs
fixture             crates/market-squawk-platform/benches/capture_admission/fixture.rs
producer_inventory  crates/market-squawk-platform/benches/capture_admission/producer_inventory.rs
schema              crates/market-squawk-platform/benches/capture_admission/schema.rs
workload            crates/market-squawk-platform/benches/capture_admission/workload.rs
```

The freeze also records separate hashes for
`crates/market-squawk-platform/benches/capture_admission.rs`,
`crates/market-squawk-platform/benches/capture_admission_criterion.rs`,
`crates/market-squawk-platform/src/capture/benchmark_support/observer.rs`, the backend dispatcher,
the selected standard backend source, all evidence/build/host-gate tools, linked production-library
sources, lockfile, toolchain, release profile, fixture, executable, host, and controlled artifacts.
`crates/market-squawk-platform/benches/capture_admission/backend/standard.rs` must remain the explicit
`sync_channel` reference. The separately hashed candidate backend is selected from
`crates/market-squawk-platform/benches/capture_admission/backend/candidate.rs`; its selected-backend
digest must differ without changing the dispatcher or immutable harness.

The candidate-delta rule is closed. After the standard run, the first candidate evidence head is a
clean descendant of `A4_STANDARD_REFERENCE_HEAD` whose source-tree delta is exactly the standard
reference report and its machine-readable lock:

```text
docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md
docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json
```

The report must call the run a benchmark-only standard reference, not a historical production
baseline. The candidate executable is rebuilt from that report-only descendant with the closed
candidate selector. All immutable, entrypoint, Criterion, observer, tool, production-source,
fixture, toolchain, release-profile, and host fingerprints must equal the frozen reference values;
only the selected backend identity/source digest and the evidence fields that necessarily bind the
distinct clean candidate head may differ. Any other code, build, fixture, schema, workload, tool, or
environment delta invalidates the reference and requires a new clean reference head, a fresh
independent freeze review, and fresh standard and candidate runs. The later exact seven-file
documentation/evidence-only Q2 truth commit may reuse the already verified candidate artifacts only
under Wave 3's unchanged-code checks.

This correction explicitly supersedes the pre-ring sequencing and provenance language in seed Step
3C, seed Steps 3D/3E, seed Step 5, and MEM Step 5. Those sections remain useful only for their
behavioral test, workload, and evidence-field requirements. They do not authorize a pre-ring or
reconstructed historical baseline, do not make an already-installed `FixedQueue` a standard
transport, and do not permit a standard run before the future clean integrated reference barrier.

#### Reviewed fixed-ring design and formula rebaseline (binding remediation)

The same review replaces the former single-`QueueState`-mutex/`Vec<Option<T>>` ring contract. A live
producer must never wait for queue capacity or another valid producer/consumer critical section, and
ordinary unsaturated overlap must never become a real `QueueContended` refusal. The production ring
is therefore a safe bounded MPSC sequence ring: modular atomic enqueue/dequeue positions, one
preallocated `QueueSlot<T>` per logical slot, atomic sender count, one combined atomic lifecycle word
for receiver closure and the checked active-operation count, and a per-slot readiness atomic plus
`Mutex<Option<T>>` used only after the slot sequence grants exclusive ownership. No unsafe code is
permitted. Correct sequence/readiness publication makes the owned slot mutex uncontended in every
valid execution; a would-block or poisoned slot lock is a terminal internal invariant failure, not
normal queue contention.

This rebaseline supersedes every later statement requiring one mutex-authoritative `QueueState`, a
non-atomic sender count, a `Condvar`, or `Vec<Option<T>>` slot accounting. The exact replacement
formula and linearization contract appear below. The future standard-reference freeze must hash this
rebaselined implementation and its updated accounting/schema/tests. No artifact produced with the
superseded formula can contribute evidence.

The pre-change standard-channel run and current preparer/host artifacts are diagnostic, not an
independent approval baseline. Do not freeze, publish, or cite them as production performance
evidence. A4 implementation proceeds directly through the fixed-ring/accounting/TIME/MEM work.
After integration, the reviewed reference head and its report-only candidate descendant run paired
standard-reference-versus-ring measurements
under direct Rust 1.97.1, locked dependencies, exact source/fixture/executable hashes, bounded host
process supervision, an independent RSS observer, and documented host state. Identical final source,
fixtures, host, and collection rules are mandatory for both backends. The clean exact-head full gate
and grouped independent Quarter 1 of 4 review—not a self-declared lock or signature
literal—authorize the performance claim.

The measurement trust model excludes a malicious same-UID compiler/build-script adversary and does
not claim byte-reproducible build or supply-chain attestation. Existing bounded I/O, process, schema,
and no-clobber tests remain useful diagnostic hardening; they must not be described as proving that
broader threat model. Unfinished hermetic snapshot/provenance work is excluded from the candidate.

Authoritative tool references used by the seed are Cargo's profile inheritance rules—where the
built-in `bench` profile inherits `release`—at
<https://doc.rust-lang.org/cargo/reference/profiles.html>, and Clippy's feature-name guidance at
<https://rust-lang.github.io/rust-clippy/master/#redundant_feature_names>. The production support
feature is therefore named `capture-benchmark`, and evidence labels the active profile as Cargo
bench inheriting the exact bound release profile rather than pretending `cargo bench` selected a
different profile.

## Global constraints

- Audit anchor: `ab3f7c19000884357c38702edf6b4acc6a80c483`. It is locally approved A3
  authority code, not proof of hosted execution.
- GitHub Actions run `29564138664` was blocked before runner assignment by the account billing gate.
  Hosted Ubuntu/macOS/Windows results are optional portability evidence. Pending or unavailable
  hosted evidence cannot block A4 implementation or approval and must never be reported as passing.
- Wave 0 produces one clean `WAVE0_HEAD` that is a descendant of `ab3f7c1` and changes only `docs/**`.
  A clean refresh commit descends from it and records the exact implementation inventory; the
  serialized A4.0 seed forks from that exact `REFRESH_HEAD`, not from a dirty documentation
  worktree or an unrefreshed audit base.
- A3 owns the sole session-global terminal authority latch and terminal writer. TIME reuses it and
  adds no second store transaction, terminal mutex, lock order, or availability-restoring path.
- The live publisher performs no persistence, journal serialization, flush, filesystem, database,
  analytical, Python, MCP, LLM, or unrelated network operation.
- Every retained-size boundary uses typed `Result`; arithmetic overflow, underreporting, invalid
  pointer graphs, allocation refusal, poison, or limit violations fail closed.
- Fixed queue/control storage, all still-reachable generation graphs, and record/conversion
  reservations share one authoritative total. Component counters are diagnostic reconciliations,
  never independent authority or separate permissive ceilings.
- No production sink retains a record after `append` unless it enforces a separate explicit bounded
  sink budget. No queue reservation is borrowed to justify post-append sink retention.
- Shared manifests, `Cargo.lock`, domain contracts, production application composition, and
  integration conflict resolution remain serialized.
- All shell constants shown below are assigned in the shell that consumes them. Do not depend on
  exported state from an earlier shell, agent, or commentary message.
- Every Bash block starts with `set -euo pipefail`. A mandatory command whose expected output is
  empty is first assigned by a standalone simple assignment and only then tested; never nest it in
  `test -z "$(...)"`, which can mask command failure. Expected-RED commands capture status/logs
  independently, assert nonzero plus the intended missing-contract diagnostic, and continue only
  after proving the failure is not an unrelated environment/toolchain error. A Cargo target/filter
  name, source filename, module name, or broad domain word is never sufficient RED evidence. Every
  RED log passes the tested `scripts/assert_expected_red.sh` classifier with either an exact
  purpose-built `MSQ_A4_RED_*` assertion sentinel or both an allowed Rust error code and an exact
  missing trait/type/method symbol. The classifier rejects dependency resolution/download, network,
  lockfile, manifest, missing toolchain/target/component, linker, permission, storage-exhaustion,
  malformed-trybuild, and unrelated syntax/delimiter failures before it considers intended evidence.
- Every dirty-tree gate checks tracked unstaged changes, staged changes, and untracked files. Every
  exact-head gate checks `HEAD`, clean status, and staged/untracked emptiness again after the gate.
- There are no accounting waivers. A known undercount, manual early release, unbounded sink,
  unresolved race, failed performance criterion, or unmeasured claim blocks historical A4/Q2
  approval at the Quarter 1 of 4 checkpoint.
- Provider-access evasion mechanisms remain permanently excluded.

### Canonical GitHub publication target

Every lookup and comment in this plan targets the same repository and PR explicitly; branch-relative
`gh pr view` is forbidden from Wave 0, seed, TIME, or MEM worktrees:

```text
repository:          Sawmonabo/market-squawk
canonical PR:        1
canonical PR branch: feat/stage-1-foundation
base branch:         main
expected state:      OPEN and draft through Quarter 1 of 4
historical Q2/A4 predecessor: ab3f7c19000884357c38702edf6b4acc6a80c483
```

Every publication step uses `gh pr view 1 --repo Sawmonabo/market-squawk ...` and
`gh pr comment 1 --repo Sawmonabo/market-squawk ...`; it never infers a PR from the checked-out
branch. In the same shell immediately before every comment, rederive every reported value and
verify PR number, head branch, head OID, base branch, open state, and draft state. The only allowed
head-OID change is the final fast-forward promotion of the already reviewed Quarter 1 candidate.

---

## Frozen A4.0 contracts

### One dependency-neutral pinned `Arc` layout helper

Create `crates/market-squawk-domain/src/retained.rs` and re-export it from
`crates/market-squawk-domain/src/lib.rs`. It owns the repository's only capture-visible model of the
Rust 1.97 `Arc` allocation layout:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainedLayoutError {
    LayoutOverflow,
    DynamicAllocationOverflow,
}

pub fn checked_arc_value_allocation_bytes<T>(
    pointee_dynamic_bytes: usize,
) -> Result<usize, RetainedLayoutError>;

pub fn checked_arc_bytes_allocation_bytes(
    length: usize,
) -> Result<usize, RetainedLayoutError>;
```

The implementation composes a proxy header containing two `AtomicUsize` counters with the pointee
through checked `std::alloc::Layout::extend`/`pad_to_align`, then adds checked owned dynamic bytes.
The byte-slice helper composes the same header with `Layout::array::<u8>(length)`. It uses no unsafe
code and does not claim allocator metadata, size-class, fragmentation, or RSS exactness. Rust,
target-layout, or payload-representation changes trigger a formula re-audit. Remove the private
sources-only duplicate; domain, sources, and platform use this one helper.

### Typed retained-size contracts

Modify `crates/market-squawk-domain/src/capture.rs` with no defaults or compatibility overloads:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureRetainedComponent {
    Identity,
    SessionBinding,
    CaptureLease,
    Continuity,
    Payload,
    Frame,
    Bundle,
    DiagnosticState,
    PlatformGeneration,
    PlatformIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureRetainedSizeError {
    Overflow {
        component: CaptureRetainedComponent,
    },
    InvalidAuthorityGraph {
        component: CaptureRetainedComponent,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureFrameFootprint {
    inline_slot_funded_bytes: usize,
    resident_shared_bytes: usize,
    unique_frame_dynamic_bytes: usize,
}

impl CaptureFrameFootprint {
    pub fn try_new(
        inline_slot_funded_bytes: usize,
        resident_shared_bytes: usize,
        unique_frame_dynamic_bytes: usize,
    ) -> Result<Self, CaptureRetainedSizeError>;

    pub const fn inline_slot_funded_bytes(self) -> usize;
    pub const fn resident_shared_bytes(self) -> usize;
    pub const fn unique_frame_dynamic_bytes(self) -> usize;
    pub fn checked_complete_bytes(self) -> Result<usize, CaptureRetainedSizeError>;
}

pub trait RawCaptureFrameView: Clone + Send + Sync + 'static {
    // Existing identity, time, and payload accessors remain required.
    fn capture_payload(&self) -> &CapturePayload;
    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError>;
}

pub trait CaptureAuthorityBundle: fmt::Debug + Send + Sized + 'static {
    // Existing associated types, identity(), and into_parts() remain required.
    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError>;
}

pub trait CaptureAdmission<Frame>: fmt::Debug + Send + 'static {
    // Existing receipt, preflight, issuance, and validation methods remain required.
    type Receipt: CaptureRetainedReceipt;

    fn checked_resident_shared_frame_bytes(
        &self,
        frame: &Frame,
    ) -> Result<usize, CaptureRetainedSizeError>;

    fn issue_after_enqueue(
        &mut self,
        frame: &Frame,
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError>;
}
```

`CaptureResidentGenerationLease` is a dependency-neutral opaque wrapper over one
`Arc<dyn CaptureResidentToken>`. It is non-`Clone`; its public generic constructor and borrowed
`shares_allocation_with` proof perform only allocation-free Arc unsizing and expose no inner
accessor, detaching conversion, manual release, or authority operation. Every concrete associated
receipt implements the required `CaptureRetainedReceipt` contract, stores the lease privately, and
reports its additional dynamic retained bytes through a no-default checked method. The platform
constructs a lease from the exact active `Arc<AccountedGenerationIdentity>`, moves it into
`issue_after_enqueue`, then requires the issued receipt to prove both pointer identity with that
active token and zero additional dynamic bytes before return. Pointer substitution or any nonzero
unreserved receipt allocation terminally degrades the affected generation with typed
`InvalidAuthorityGraph(CaptureLease)` and returns no receipt. The receipt/lease remain non-`Clone`
and non-Serde, with no consuming extraction API. This closes the case where an old source receipt
outlives all platform state while retaining bundle-counted binding/lease allocations after the
resident charge was released. Receipt-only-after-rotation drop-order tests require the predecessor
charge to remain until the concrete receipt drops and then reconcile exactly. Future nonzero
receipt-owned dynamic storage requires an explicit reservation design; it cannot be admitted by
changing the receipt implementation alone.

The superseded pre-ring baseline sequence must not be executed. In Steps 1 through 3, the platform
creates this transitional lease over the exact active
`Arc<GenerationCaptureState<B>>`, validates receipt pointer identity against that same Arc, and uses
it solely as a lifetime proof for the already reachable source binding/lease graph. No evidence run
may describe that transitional anchor as a memory-accounting token. Step 5 atomically replaces
the anchor with the exact `Arc<AccountedGenerationIdentity>` when resident accounting is installed;
the same pointer and zero-additional-dynamic validations then become accounting-authoritative. The
standard reference is frozen and measured only after this replacement and full integration.

Add the inherent method with a complete body in implementation:

```text
CaptureAuthorityIdentity::checked_dynamic_retained_bytes(&self)
    -> Result<usize, CaptureRetainedSizeError>
```

Bundle `checked_retained_bytes` values include `size_of_val(self)`. A frame instead returns the
closed `CaptureFrameFootprint` decomposition. Its private-field constructor checks the complete sum;
the publisher independently requires `inline_slot_funded_bytes == size_of_val(frame)` and requires
the active admission's pointer-proven resident byte count to equal `resident_shared_bytes`. The
dependency-neutral domain contract reports arithmetic overflow or an invalid authority graph; the
platform returns the distinct `RetainedSizeUnderreported` preparation/publication error when a
successful report is below an independently known structural minimum. Dynamic identity bytes
exclude inline fields, so zero is valid. Exhaustive destructuring without `..` makes every new
authority field a compile-time accounting obligation.

The required `capture_payload` method is the ownership-preserving conversion seam. It is not a
default and is not replaceable by the existing borrowed `payload() -> &[u8]` view. The seed adds a
trybuild omission fixture for this method and runtime tests proving that platform generic
frame-to-record conversion clones this exact `CapturePayload` allocation. This prevents a generic
writer from falling back to `Bytes::copy_from_slice(frame.payload())` or another hidden copy.

The required admission method separates allocations already resident in the active generation
from frame-exclusive allocations. Production sources prove the frame's `FrameSessionBinding` Arc is
pointer-equal to admission's binding and return that exact checked allocation; TIME later adds the
pointer-proven `AuthorityTimeContinuity` allocation. Diagnostic admission returns zero because its
frame owns distinct identity capacities rather than a resident-shared binding. All admission/test
implementations are exhaustive and a trybuild omission fixture makes this accounting seam
mandatory.

### Exact payload and frame formulas

Replace capture-plane `Bytes` payload ownership with one invariant-preserving domain type used by:

- `RawMarketFrame`;
- `DiagnosticCaptureFrame`; and
- `RawCaptureRecord`.

The representation is private and admits only the complete closed graph:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturePayload(PayloadStorage);

#[derive(Clone, Debug, Eq, PartialEq)]
enum PayloadStorage {
    Empty,
    Shared(Arc<[u8]>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapturePayloadError {
    TooLarge { actual: usize, maximum: NonZeroUsize },
    RetainedLayout(RetainedLayoutError),
}

impl CapturePayload {
    pub fn try_from_live(input: &[u8]) -> Result<Self, CapturePayloadError>;

    pub fn try_from_committed_wire(input: &[u8]) -> Result<Self, CapturePayloadError>;

    pub fn as_bytes(&self) -> &[u8];

    pub fn checked_retained_allocation_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError>;

    pub fn shares_allocation_with(&self, other: &Self) -> bool;
}
```

The dependency-neutral named constructors apply the frozen live or committed-wire ceiling and
reject oversize input before allocation; callers cannot supply a permissive maximum. Empty input
becomes allocation-free `PayloadStorage::Empty`;
nonempty input performs the one permitted bounded boundary copy into a right-sized `Arc<[u8]>`. No
sliced or spare-capacity backing allocation can enter the graph. Public payload views return
`&[u8]`; serialization preserves the existing wire. `shares_allocation_with` returns true only for
two empty payloads or pointer-equal shared payload allocations. Internal frame-to-record conversion
clones the exact `CapturePayload` returned by `RawCaptureFrameView::capture_payload`, sharing the
pointee/control allocation without a second payload copy. Stable Rust 1.97 cannot make the
`Arc<[u8]>` allocation recoverably fallible; oversize input and retained-layout arithmetic are typed,
while process-wide allocator OOM remains an explicit process boundary.

Payload limits deliberately have two tiers:

```text
live producer/frame construction:
    MAX_RAW_FRAME_BYTES == 4 * 1024 * 1024

historical current/legacy committed-journal read compatibility:
    MAX_COMPATIBILITY_PAYLOAD_BYTES == 33_554_431
        == (MAX_SERIALIZED_RECORD_BYTES - 2) / 2
    MAX_SERIALIZED_RECORD_BYTES == 64 * 1024 * 1024
```

Every live source and diagnostic constructor applies the 4 MiB bound before creating a payload.
Historical deserialization may normalize a payload larger than 4 MiB but no larger than the
existing compatibility bound so old local journals remain readable. The explicitly named
committed-wire constructor is public because the platform raw-record owner is a separate crate, but
every live frame constructor re-applies the live bound and accepts no already-normalized bypass; a
compatibility payload therefore cannot construct a live executable frame. Tests
round-trip a historical payload just above 4 MiB, reject the same bytes at every live constructor,
and reject compatibility one-over without allocating. The structural formulas are:

```text
RawMarketFrame =
    size_of::<RawMarketFrame>()
  + checked Arc<FrameSessionIdentity> allocation
      (pointee + actual source/revision/session capacities)
  + payload.checked_retained_allocation_bytes()

DiagnosticCaptureFrame =
    size_of::<DiagnosticCaptureFrame>()
  + identity.checked_dynamic_retained_bytes()
  + payload.checked_retained_allocation_bytes()

RawCaptureRecord =
    size_of::<RawCaptureRecord>()
  + checked Arc<str> source allocation
  + payload.checked_retained_allocation_bytes()
```

`RawCaptureRecord` gains a crate-visible checked retained-size method for conversion and bounded-sink
accounting. Maximum-capacity short identities and payload boundary tests must fail length-only or
missing-control-block implementations.

### Per-frame reservation and allocation-identity proof

The platform never adds the complete frame and complete record totals, which would double-charge
the shared payload. Before admission it consumes the frame's checked decomposition:

```text
footprint = frame.checked_retained_footprint()
require footprint.inline_slot_funded_bytes() == size_of_val(frame)
require admission.checked_resident_shared_frame_bytes(frame)
    == footprint.resident_shared_bytes()

conversion_source_allocation =
    checked Arc<str> allocation for the exact normalized source identifier

record_reservation =
    footprint.unique_frame_dynamic_bytes()
  + conversion_source_allocation
```

Every construction/addition is checked. A footprint with an incorrect inline term or a resident
term that the active admission cannot prove is rejected before reservation as underreporting or an
invalid authority graph; arithmetic overflow is typed. A frame includes its unique payload
allocation in `unique_frame_dynamic_bytes`, so the shared frame-to-record payload is charged once
without being decomposed and re-added by platform code. After conversion, before append, the writer
proves:

```text
frame.capture_payload()
    .shares_allocation_with(record.capture_payload())
```

and independently verifies that the record's complete retained total decomposes into its inline
value, the same shared payload allocation, and the exact source allocation. The borrowed
`frame.payload()` bytes must also equal `frame.capture_payload().as_bytes()` before admission.
Borrowed-view mismatch is `InvalidPayloadView`; allocation mismatch after conversion is
`InvalidPayloadSharing`; in either case the sink never observes the record. Tests cover empty and
nonempty pointer equality, a malicious frame whose borrowed view differs from its owned payload,
maximum source capacity, footprint-sum overflow, inline/resident mismatch, and a test conversion
that attempts a second payload copy. Thus one allocation is charged exactly once while frame and
record coexist.

For `RawMarketFrame`, the frame's resident-shared term is the pointer-proven
`FrameSessionBinding` allocation in seed and binding plus continuity after TIME; these bytes remain
in `resident_generation_bytes` and are never charged per record. Its payload is unique/shared only
between frame and converted record and is charged once in `record_reservation`. For
`DiagnosticCaptureFrame`, resident-shared is zero, so its distinct identity dynamic capacities and
payload remain frame-exclusive/payload terms. Tests fail if a source returns the correct byte count
for the wrong binding pointer, omits continuity, or subtracts diagnostic identity as resident.

`conversion_peak_bytes` is exactly the checked source `Arc<str>` allocation. UUID/event-name
formatting uses fixed writer-owned scratch; timestamps, UUID values, the raw-record inline value,
CRC state, and counting-writer state are inline in already charged queue/writer/sink storage. Any
future heap-backed conversion temporary must be added to this formula before it can compile through
an exhaustive `ConversionAllocationBreakdown` test.

### Exact bundle formulas

All four implementations are mandatory in the seed: production sources, diagnostic platform,
platform test bundle, and domain test bundle.

Before TIME, `CaptureGenerationCapabilities` exhaustively proves the top-level/admission binding
pointer equality and all four lease pointer equalities, then charges:

```text
size_of::<CaptureGenerationCapabilities>()
+ checked Arc<FrameSessionIdentity> allocation once
    (pointee + actual source/revision/session capacities)
+ checked Arc<CaptureGenerationState> allocation once
```

TIME adds one exact `AuthorityTimeContinuity` pointee/control allocation after proving every
time-bearing handle is pointer-equal. Any mismatch returns `InvalidAuthorityGraph` for its exact
component.

`DiagnosticCaptureBundle` proves initializer, admission, and degradation share the same
`Arc<AtomicU8>`, then charges:

```text
size_of::<DiagnosticCaptureBundle>()
+ bundle identity dynamic capacities
+ distinct admission identity dynamic capacities
+ checked Arc<AtomicU8> allocation once
```

`DiagnosticCaptureFrame` uses the frame formula above. Test bundles exhaustively destructure every
field, prove their shared test-state pointers, accept a deterministic declared total, and support
typed overflow, underreporting, and invalid-graph fixtures.

### Fallible initial channel and configuration

Use these public shapes with full rustdoc and `Debug` on every public type:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureChannelLimits {
    capture_queue_capacity: NonZeroUsize,
    capture_memory_ceiling_bytes: NonZeroUsize,
}

impl CaptureChannelLimits {
    pub const fn new(
        capture_queue_capacity: NonZeroUsize,
        capture_memory_ceiling_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            capture_queue_capacity,
            capture_memory_ceiling_bytes,
        }
    }

    pub const fn capture_queue_capacity(self) -> NonZeroUsize {
        self.capture_queue_capacity
    }

    pub const fn capture_memory_ceiling_bytes(self) -> NonZeroUsize {
        self.capture_memory_ceiling_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureProcessInfrastructureLimits {
    destination_registry_memory_ceiling_bytes: NonZeroUsize,
}

impl CaptureProcessInfrastructureLimits {
    pub const fn new(destination_registry_memory_ceiling_bytes: NonZeroUsize) -> Self {
        Self {
            destination_registry_memory_ceiling_bytes,
        }
    }

    pub const fn destination_registry_memory_ceiling_bytes(self) -> NonZeroUsize {
        self.destination_registry_memory_ceiling_bytes
    }
}

pub struct CaptureProcessInfrastructure {
    // Allocation-free private reference to the ready arm of the process-global state.
}

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

pub fn initialize_capture_process_infrastructure(
    limits: CaptureProcessInfrastructureLimits,
) -> Result<CaptureProcessInfrastructure, DestinationFenceRegistryInitializationError>;
```

```text
raw_capture_channel<B: CaptureAuthorityBundle>(
    process: &CaptureProcessInfrastructure,
    limits: CaptureChannelLimits,
    bundle: B,
) -> Result<
    (RawCapturePublisher<B>, RawCaptureControl<B>, RawCaptureWriter<B>),
    CaptureChannelError,
>
```

`CaptureChannelError` owns only initial construction errors: typed generation preparation,
fixed-budget rejection, and recoverable dominant ring-slot allocation refusal.
`CaptureGenerationError` owns activation, binding, generation ordering, writer lifecycle, and typed
successor preparation. `CapturePublishError` preserves `Authority(CaptureAuthorityError)`,
`AuthorityBusy`, and `WriterUnavailable`, and separately names typed retained-size, unified-budget,
queue-full, queue-contention, queue-closed, queue-poison, and accounting-invariant failures. Sink
errors separately name retained-size overflow, retained-byte limit, record limit, serialization
limit/failure, write failure, and flush failure.

Rename configuration at v0.1 with no legacy alias:

```text
file: capture_queue_capacity
env:  MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY
CLI:  --capture-queue-capacity

file: capture_memory_ceiling_bytes
env:  MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES
CLI:  --capture-memory-ceiling-bytes

file: capture_destination_registry_memory_ceiling_bytes
env:  MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES
CLI:  --capture-destination-registry-memory-ceiling-bytes
```

Rename `journal_queue_capacity`/`MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY` everywhere. The old file key,
environment key, getter, and CLI spelling fail closed as unknown; there is no alias. Defaults remain
16,384 records and 64 MiB per channel; the process destination-registry ceiling defaults to 1 MiB.
Defaults, file, environment, and CLI flow through `ConfigOverrides`,
`FileConfig`, `AppConfig`, debug output, accessors, `load_config`, precedence tests, and every channel
composition call. Application composition initializes the process infrastructure exactly once before
any channel handle is published and passes the proof handle to every channel. The first outcome is
permanent. Success makes same-limit calls idempotent and returns an allocation-free proof referencing
the `Ready` arm; different limits return `AlreadyInitializedWithDifferentLimits`. Failure drops any
temporary vector, stores attempted limits plus a closed copyable error inline in `Failed`, replays
that exact error for same-limit calls, and rejects different limits with
`AlreadyInitializedWithDifferentLimits`. No failed call publishes a proof and concurrent
initialization has one `OnceLock` winner. Tests and non-application consumers explicitly initialize
the process handle; no hidden lazy default, retry, or allocation-bearing proof bypass exists.

---

## Frozen seed-owned authority and accounting model

### One authoritative total and one resident-generation token

The serialized seed creates a narrow `Arc<CaptureMemoryAccounting>` that outlives `CaptureState`
whenever an
external accounted value remains. It owns checked diagnostic counters:

```text
fixed_capture_bytes
resident_generation_bytes
record_reservation_bytes
total_accounted_bytes
accounting_invariant_failures
active_transitions
completed_epoch
accounting_status
```

Outside an in-progress compare/exchange transition:

```text
total_accounted_bytes
    == fixed_capture_bytes
     + resident_generation_bytes
     + record_reservation_bytes
    <= configured_capture_memory_ceiling_bytes
```

There is no per-record generation term and no manual resident-generation release.

The first four byte counters are never read independently by diagnostics. Freeze one bounded,
nonblocking snapshot seam:

```rust
pub struct CaptureAccountingSnapshot {
    completed_epoch: u64,
    fixed_capture_bytes: usize,
    resident_generation_bytes: usize,
    record_reservation_bytes: usize,
    total_accounted_bytes: usize,
    accounting_invariant_failures: u64,
}

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
```

`CaptureAccountingStatus` is stored inline as `AtomicU8` in the already charged accounting core.
The first terminal reason wins through checked compare/exchange and remains observable for the
core's lifetime. Every later reservation, release, admission, and snapshot checks it and fails
closed; no counter operation resumes normal service after terminal status.

Freeze one deliberately conservative `SeqCst` protocol rather than leaving atomics to
implementation interpretation. Every fixed/resident/record reserve or release checks `Healthy`,
enters `active_transitions` through a checked `SeqCst` compare/exchange before its first counter
mutation, checks `Healthy` again, performs the authoritative-total transition and matching component
transition with `SeqCst`, publishes the checked next `completed_epoch` with `SeqCst`, and leaves the
transition with a checked `SeqCst` decrement. Transition-count overflow, epoch overflow,
underflow, or reconciliation failure publishes the exact first terminal status before returning;
an operation that entered before poison still releases its active-transition guard but cannot make
the core healthy again.

The transition guard has one explicit successful `finish` path. Its `Drop` fallback performs only
the checked `SeqCst` active-count leave and first-wins `InvariantViolated` poison sequence; it cannot
silently abandon a live transition, wrap a counter, resume service, or claim successful component/
total reconciliation. Deterministic and production-ordering Loom tests deliberately abandon a live
guard, prove the checked decrement, prove durable first-poison semantics, reject later admission,
and require `try_accounting_snapshot` to return `InvariantViolated` rather than `Contended`.

The only diagnostic API is
`try_accounting_snapshot(max_attempts: NonZeroUsize)`. One bounded attempt loads the atomics with
`SeqCst`, in exact order: status, epoch, active count, every component and authoritative total,
invariant-failure count, the immutable configured ceiling, active count, epoch, active count again,
and status again. It accepts only when both status reads are `Healthy`,
all three active reads are zero, both epoch reads match, and checked component addition equals the
total within the ceiling. The final active read closes the validation window; a transition entirely
inside the read changes the epoch, preventing reserve/release ABA from being accepted. Terminal
status maps durably to `TransitionOverflow`, `EpochOverflow`, or `InvariantViolated`; only bounded
healthy racing returns `Contended`. It never fabricates a sample or authorizes admission. Health
diagnostics, tests, benchmark peaks, and post-drain assertions consume only an accepted
`CaptureAccountingSnapshot`. Deterministic barriers and the production-ordering Loom model force a
transition between every pair of snapshot loads, overlapping writers, fixed/resident/record
reserve/release, complete ABA, every terminal poison publication, overflow injection, retry
exhaustion, and the final accepted post-drain snapshot.

The accounting Arc's own checked allocation is an immutable base in `fixed_capture_bytes` for the
entire accounting-core lifetime. `CaptureState` separately owns an RAII fixed-infrastructure
reservation for record/health rings, channel state, and `WriterLifecycleCore`; that portion releases
when the final channel-state owner drops even if an external accounted generation identity keeps the
accounting core alive. Writer runtime ownership separately retains the writer-start fixed
reservation through final joined lifecycle drop. The remaining core base disappears only with the
accounting Arc itself, when no future admission or observable counter exists.

Successor preparation first reserves the complete generation charge and embeds that one RAII
reservation in:

```rust
#[derive(Debug)]
struct AccountedGenerationIdentity {
    identity: CaptureAuthorityIdentity,
    complete_retained_bytes: usize,
    resident: ResidentGenerationReservation,
}
```

The stored conservative generation charge is evaluated once with checked arithmetic:

```text
complete_generation_bytes =
    bundle.checked_retained_bytes()
  + checked Arc<GenerationCaptureState<B>> allocation
  + checked Arc<AccountedGenerationIdentity> allocation
      (platform identity dynamic capacities + inline resident token/account handle)
```

The bundle term may conservatively retain initializer/inline capability storage after a capability
moves or drops; it must never omit a reachable allocation. This is a closed structural upper bound,
not allocator/RSS exactness.

The same `Arc<AccountedGenerationIdentity>` is shared through `GenerationCaptureState`, publisher
and control identity snapshots, health snapshots/events, queued messages, and
`CapturedRawRecord`, and the opaque non-clone resident lease embedded in every issued receipt. Public
wrappers expose `&CaptureAuthorityIdentity` but retain that exact Arc. No receipt API can extract a
bare concrete receipt while dropping the resident lease early.
The RAII token releases the resident charge only when the final
state/message/event/snapshot/record/receipt reference drops. Rotation creates the successor
token while the predecessor remains charged; a rejected successor drops its token and degrades only
itself; successful publication needs no charge transfer or predecessor release call.

Health slots are fixed queue storage. Health events share the generation token instead of cloning a
new identity allocation or owning a second health-byte reservation. Diagnostic-event refusal may
increment a bounded drop counter but never restore integrity. A sink-retained `CapturedRawRecord`
keeps its generation resident in the channel accounting core until the record's final clone drops.

### Fixed storage and per-record reservation

The seed freezes these exact owned types and rejects construction unless every term can be computed
and reserved. `QueueCore<T>` contains one never-growing `Vec<QueueSlot<T>>`, atomic enqueue/dequeue
positions, the capacity-aligned position modulus, sender count, and one combined atomic lifecycle
word whose high bit is the one-way receiver-closed state and whose remaining bits are the checked
active send/clone count, plus the consumer/terminal-drain serialization gate, bounded
receiver-wakeup registration, and test-only coordination compiled for the exact target.
`QueueSlot<T>` contains its sequence number, readiness atomic, and `Mutex<Option<T>>` inline.
`CaptureState<B>` owns the two queue Arcs, accounting Arc, and a coalesced `WriterLifecycleCore` Arc
containing shutdown deadlines, completion, final-report, destination-fence, and lifecycle-control
state. At MEM writer start, `CaptureWriterRuntime<B>` owns the fallibly preallocated formatting
scratch and a writer-start fixed reservation behind the seed's frozen fixed-component API.
Caller-owned inline publisher, control, and writer handle values are not heap allocations and are
not charged. There are no hidden receiver/control Arcs outside the exhaustively named types; any
retained separate pointee must be
named and charged before construction can publish handles.

At channel construction, use observed capacities after `try_reserve_exact` and compute:

```text
record_slot_bytes =
    record_slots.capacity() * size_of::<QueueSlot<CaptureMessage<B>>>()

health_slot_bytes =
    health_slots.capacity() * size_of::<QueueSlot<CaptureHealthEvent>>()

record_queue_allocation =
    checked_arc_value_allocation_bytes::<QueueCore<CaptureMessage<B>>>(record_slot_bytes)

health_queue_allocation =
    checked_arc_value_allocation_bytes::<QueueCore<CaptureHealthEvent>>(health_slot_bytes)

accounting_core_base_bytes =
    checked_arc_value_allocation_bytes::<CaptureMemoryAccounting>(0)

capture_state_allocation =
    checked_arc_value_allocation_bytes::<CaptureState<B>>(0)

writer_lifecycle_core_allocation =
    checked_arc_value_allocation_bytes::<WriterLifecycleCore>(0)

channel_state_fixed_bytes =
    record_queue_allocation
  + health_queue_allocation
  + capture_state_allocation
  + writer_lifecycle_core_allocation

writer_start_fixed_bytes =
    observed fixed UUID/source/event scratch capacities
  + complete per-writer destination-lease allocation and bounded destination identity
  + bounded writer-thread name allocation
  + pinned Rust-owned thread spawn/control upper bound
  + every stable writer allocation not embedded in WriterLifecycleCore

fixed_capture_bytes =
    accounting_core_base_bytes
  + channel_state_fixed_bytes
  + active writer_start_fixed_bytes
```

The Arc helper's dynamic argument already incorporates each ring's observed slot backing; slot
bytes are not added a second time. `CaptureFixedStorageReceipt` stores each channel term, observed
record/health capacity, lifecycle-core term, and checked sum. `WriterFixedStorageReceipt` stores the
exact observed scratch capacities, destination/thread/runtime terms, compiled-target proof
identifier, and RAII fixed-component reservation. Tests independently reconstruct every term from
actual objects and reject omitted, duplicated, overflowed, or hidden terms. Both rings use
`Vec::try_reserve_exact`, initialize exactly the requested logical count with sequence-owned empty
`QueueSlot` values, derive ring modulus/fullness only from that requested length, charge the
independently observed backing capacity at `size_of::<QueueSlot<T>>()`, never address allocator spare
capacity, never grow, and return
`CaptureChannelError::QueueAllocationFailed { queue, requested_slots }` on recoverable allocation
refusal. The record ring's requested logical length is the configured nonzero
`capture_queue_capacity`; the health ring's separately named requested logical length is the fixed
nonzero value 64. Neither logical length is ever replaced by its observed `Vec` capacity. The
accounting base plus `channel_state_fixed_bytes` and the initial resident-generation
token must fit before any handle becomes visible. Writer start then prepares every quoted term,
uses `try_reserve_exact` for conversion scratch, recomputes from observed capacities, atomically
reserves the complete `writer_start_fixed_bytes`, and becomes healthy only after reservation,
allocation, and thread creation succeed. Failed writer start releases its fixed-component
reservation; successful start retains it across worker, pending-reap, final-report, and destination
fence ownership until the joined lifecycle finally drops. The pinned Rust 1.97 spawn packet,
`Thread`/`JoinHandle` shared control, closure capture, and thread-name allocation require a persisted
compiled-target conservative upper-bound proof. If that Rust-owned class cannot be closed, reject
the structural-memory claim. Native stack, thread handle, allocator metadata, scheduler/kernel
bookkeeping, and fragmentation remain supplemental RSS/host evidence and are never mislabeled as
exact Rust graph terms.

The process-global destination fence is a fourth, separate lifetime class and is never temporarily
charged to a writer. Seed replaces the current retained-capacity `HashMap` with one fallibly
preallocated, never-growing `Vec<Option<CaptureDestinationFenceEntry>>` of exactly
`MAX_ACTIVE_CAPTURE_DESTINATIONS` logical entries. Linear scan is acceptable on writer-start/drop
control paths and avoids an unaccountable private hash-table layout. The process-lifetime ledger
charges both static registry storage and the complete observed vector backing:

```text
destination_registry_process_bytes =
    size_of::<OnceLock<DestinationFenceRegistryInitializationState>>()
  + ready.entries.capacity() * size_of::<Option<CaptureDestinationFenceEntry>>()
```

Initialization calls `try_reserve_exact`, resizes length to exactly the configured compile-time
logical entry count, reads and charges observed capacity, compares it with the explicit production
registry ceiling, and permanently stores the exact `Ready` or `Failed` state before any writer can
acquire a lease. A failed state charges only the inline `OnceLock`/enum storage because every
temporary vector is dropped before publication; a ready state additionally charges its observed
vector backing. `CaptureProcessInfrastructure` is an allocation-free reference proof and adds no
process term. Removal clears a slot but never shrinks; the backing charge
persists for process lifetime. It is exposed in process diagnostics and benchmark RSS metadata but
cannot be borrowed by the per-channel total or per-sink ledgers. Tests use an injectable constructor
to cover exact/one-under ceilings, allocation refusal, 1,024 distinct active destinations, churn and
reuse, duplicate fencing, no growth, logical capacity despite allocator spare capacity, poison,
and the state after the final per-writer lease/reservation drops. Tests also cover concurrent first
initialization, same/different limits after ready and failed outcomes, exact permanent-failure
replay, no proof publication after refusal, and exact process-byte reporting for both terminal arms.

For a record:

```text
record_reservation =
    footprint.unique_frame_dynamic_bytes()
  + conversion_source_allocation

admit iff
    fixed_capture_bytes
  + resident_generation_bytes
  + record_reservation_bytes
  + record_reservation
  <= configured_capture_memory_ceiling_bytes
```

The fixed record slot's inline `Mutex<Option<CaptureMessage<B>>>` already owns the inline
message/frame/identity-Arc/reservation handles, so those inline values are in `record_slot_bytes`;
only their reachable dynamic allocations are in the record reservation. Zero dynamic bytes are
valid. `QueueByteReservation` stays lexically alive through deadline and authority revalidation,
conversion, sink append, and policy-triggered flush. It is installed before the enqueue claim and
remains inside the owned message on every send failure. RAII releases exactly once on success,
conversion/append/flush error, full/closed/invariant send, cancellation, shutdown, drain, writer
drop, and pending-owner drop.

### Queue linearization and Loom contract

Use one safe bounded sequence ring per record/health queue. Each slot starts with its logical ordinal
as the sequence and an explicit `ready = false` publication state. A producer first enters the
checked active-operation set through a CAS on the combined lifecycle word, claims one enqueue
position by CAS, and may touch only the slot whose sequence proves that exact position is empty. It
then `try_lock`s that uniquely owned slot, verifies `ready = false`, installs the message, releases
the guard, and publishes `ready = true` with `Release`. The single consumer claims the next dequeue
position only when
`Acquire` observes the exact sequence and `ready = true`, removes the message under that uniquely
owned slot lock, clears readiness, and publishes the next empty-cycle sequence with `Release`.
The next producer's sequence `Acquire` therefore observes the cleared readiness and removed value
before reusing the slot. The separate readiness state is required even at logical capacity one,
where the published and next-free sequence values would otherwise alias. Full and empty observations
derive only from position/sequence/readiness state and the exact logical capacity. The readiness
atomic is part of `size_of::<QueueSlot<CaptureMessage<B>>>()` and therefore part of exact fixed-slot
and queue-private-storage accounting; it is not an uncharged side allocation. Construction sets
`position_modulus = usize::MAX - (usize::MAX % logical_capacity)`, requires it to exceed the logical
capacity, and advances positions modulo that capacity-aligned bound so the position-to-slot mapping
is preserved across rollover without relying on ambiguous native integer overflow. Test-only
near-modulus seeding proves rollover without indexing allocator spare capacity.

Valid producer execution never waits on a queue mutex, condition variable, capacity permit, or
another producer/consumer critical section. The per-slot mutex is a safe storage cell rather than a
contention policy: sequence ownership guarantees its immediate acquisition. A would-block slot,
poisoned slot, impossible empty/filled value, counter overflow, or ownership mismatch is a distinct
terminal queue-invariant result, closes the stream, and releases the rejected message reservation;
it is never reported as `QueueContended`. Production acceptance and performance evidence require
zero such invariant failures and no ordinary contention outcome.

Send linearizes when the owned slot's `ready = true` state is published. Close/receiver drop sets the
closed bit through an atomic read-modify-write on the same lifecycle word that admits operations,
rejects subsequent send/clone registration, and waits until that word's active-operation count is
zero before notifying or draining under the declared cleanup owner. An operation guard's checked
decrement can never clear the closed bit. A send admitted before close either publishes a drainable
message or returns the intact value while close waits; one that observes the closed bit is rejected.
Receiver drop uses this same close transition before draining, without a second competing
receiver-alive authority flag. `try_clone` holds the same active-operation guard around its checked
sender-count CAS, so close cannot return before an admitted clone completes; last-sender drop closes
and notifies. No post-close send or clone can succeed, no in-band `Wake` variant exists, and no user
destructor runs while a slot lock is held.

Benchmark timeout and Drop paths may issue a nonblocking close-registration request so reporting
cannot wait behind an admitted producer. Once shutdown is requested, however, the writer must not
treat a transient `Empty` observation as terminal while the combined lifecycle count is nonzero. It
retries/yields, consumes any message published by an operation admitted before close, and exits the
drain only on terminal `Closed` or the configured shutdown deadline. A deterministic paused-send
fixture proves the writer cannot terminate between operation registration and publication and that
the late accepted message or intact refusal reconciles its reservation exactly.

The unique consumer uses a stored `Thread` wake token outside the publisher's latency authority. Its
registration protocol publishes the waiter, rechecks queue and terminal sequence state, and only
then parks. A producer/closer that cannot immediately acquire the registration cell is racing that
pre-park publication and may skip the wake only because the receiver's mandatory recheck will see
the transition; a wake after the recheck leaves an `unpark` token. This exact handshake, rather than
an unrelated lifecycle flag, closes the check-before-sleep lost-wakeup window.

Deterministic barriers, sustained multi-producer stress, and platform Loom models cover slot
ownership, capacity-one readiness/sequence alias regression, arbitrary-capacity full/empty/FIFO
behavior, counter wraparound, send-vs-close,
send-vs-receiver-drop, clone-vs-close, receiver-drop-vs-send, last-sender-vs-wait,
consumer-registration-vs-notify, full-queue shutdown, deadline/reap, poison/invariant cleanup, and
exactly-once reservation release.

Freeze and rustdoc one lock rank for every path that can touch more than one authority object:
lifecycle transition, record queue, health queue, accounting transition, then destination registry.
Prefer releasing one rank before acquiring the next. No user/generic destructor, sink operation,
health-event publication, tracing formatter, or allocation is invoked while any queue/accounting/
registry mutex is held; cleanup moves owned values into a local bounded batch, unlocks, then drops
or publishes. Tests use reentrant `Drop`/sink/health fixtures that call back into diagnostics and
shutdown to prove the implementation neither deadlocks nor observes a partially published state.

`RawCapturePublisher` deliberately does not implement `Clone`. Its typed
`try_clone(&self) -> Result<Self, CapturePublisherCloneError>` uses a checked sender-count CAS,
rejects terminal close and `usize` overflow, then rechecks terminal state and rolls back if close won
the race. Sender `Drop` atomically decrements and the last drop closes/notifies. The seed migrates all ten audited
publisher `.clone()` calls, removes the `CaptureContext` derived `Clone`, replaces two positive
publisher `Clone` assertions with negative compile-time assertions plus `Send + Sync`, and removes
the hidden inner-sender clone during channel construction. Tests and Loom models assert clone/drop
races, overflow at `usize::MAX`, last-sender wakeup,
receiver-drop drain, queue-full close, invariant cleanup, and no send after the linearized last-close
transition. Poison recovery may take owned messages only for terminal cleanup; it never resumes
normal queue service.

### Conversion, journal, and bounded sink

Move UUID name scratch to fixed writer-owned storage charged at construction. Conversion creates
one `RawCaptureRecord` source allocation and clones the frame's `CapturePayload`; it allocates no
second payload. Its peak includes the simultaneously live record object and all remaining
conversion scratch and stays charged until append and any record-triggered flush return.

`JournalWriter::append` serializes twice without `serde_json::to_vec`: first into a bounded
counting/CRC writer, then—after length validation and header write—directly into `BufWriter<File>`.
Second-pass failure remains a rejected/truncated tail under the existing reader/startup contract;
no partial frame is reported as a successful append.

The journal owns a separate bounded sink ledger. It never borrows channel reservations and never
claims the OS/file-system page cache as Rust-retained memory. Construction uses
`BufWriter::with_capacity(JOURNAL_BUFFER_CAPACITY_BYTES, file)` and records the observed
`BufWriter::capacity()`. The exact fixed formula is:

```text
journal_sink_fixed_bytes =
    size_of::<JournalWriter>()
  + journal_path.capacity()
  + buf_writer.capacity() * size_of::<u8>()
```

The Rust 1.97 `PathBuf`/`OsString` capacity is charged in its documented byte-capacity unit; tests
run on Unix and Windows targets and use actual capacity, not display length. The bounded counting/CRC
writer and second-pass serializer retain no heap payload. A typed
`JournalSinkLimits { buffer_capacity: NonZeroUsize, retained_byte_ceiling: NonZeroUsize }` is
validated before opening/publishing the sink; observed fixed bytes must fit its separate ceiling.
The standard library's `BufWriter` allocation is not fallible through stable Rust 1.97. The bounded
`Arc<[u8]>`/`Arc<str>` and synchronization allocations are likewise process-allocator OOM
boundaries. Dominant ring and memory-sink `Vec` allocation is recoverably fallible through
`try_reserve_exact`; all allocation sizes are prebounded and included in the conservative graph.
Tests prove exact and one-under fixed ceilings, observed capacity, bounded streaming, no
`serde_json::to_vec`, and no record retention after successful or failed append. The plan does not
invent recoverable allocation errors for stable standard-library operations that cannot return
them.

`MemoryCaptureSink::try_new(max_records: NonZeroUsize, max_retained_bytes: NonZeroUsize)` is frozen
in the seed and is the only constructor. It:

1. validates nonzero limits;
2. uses `Vec::try_reserve_exact(max_records)`;
3. computes its exact fixed storage before becoming visible;
4. rejects when fixed storage already exceeds its separate byte limit;
5. checks count and complete retained clone bytes before every append; and
6. never pushes at `len == max_records` and therefore never grows after construction.

```text
memory_sink_fixed_bytes =
    size_of::<MemoryCaptureSink>()
  + records.capacity() * size_of::<CapturedRawRecord>()

record_dynamic_bytes =
    complete checked source Arc<str> allocation
  + complete nonempty CapturePayload Arc<[u8]> allocation

memory_sink_total_bytes =
    memory_sink_fixed_bytes
  + sum(record_dynamic_bytes for every retained record clone)
```

The sink formula intentionally charges each retained clone's complete dynamic graph even if a test
inserts the same Arc-backed payload twice; this conservative per-record charge can overcount but
cannot undercount. The generation identity's resident reservation remains authoritative in channel
accounting and is not presented as sink-owned bytes. MEM may harden the sink's dynamic-ledger
internals behind this API but must not alter the typed seed constructor or reintroduce `Default`.

Exact-limit insertion succeeds; one-over count/bytes and arithmetic/allocation failures are typed.
Remove `Default` and every unbounded public constructor.

### Identity-bearing errors and complete health mapping

No public error, health event, snapshot, queued record, or sink record may clone a bare
`CaptureAuthorityIdentity` and lose the resident token. Freeze one private
`CaptureIdentitySnapshot(Arc<AccountedGenerationIdentity>)`; public accessors return a borrowed
identity. Binding/order/rotation errors that retain expected or observed identities store this
wrapper. `Debug`, `Display`, `Eq`, and source chaining must not allocate a second identity graph or
expose secrets. Tests retain each error after publisher/control/writer drop and prove the resident
charge remains until the final error is dropped.

Freeze the full error taxonomy; broad string errors and catch-all writer failures are forbidden:

```text
CaptureChannelError:
  GenerationPreparation | FixedStorageBudgetExceeded | QueueAllocationFailed

CaptureWriterSpawnError:
  FixedStorageBudgetExceeded | ScratchAllocationFailed
  | DestinationFence(CaptureDestinationFenceError)
  | RuntimeProof(WriterRuntimeProofError)
  | ThreadNameLimitExceeded { actual, limit }
  | ThreadSpawnFailed { source }

CaptureDestinationFenceError:
  Busy | Capacity | RegistryPoisoned

WriterRuntimeProofError:
  CompiledTargetMismatch | FormulaRevisionMismatch | ArtifactHashMismatch

DestinationFenceRegistryInitializationError:
  Permanent(DestinationFenceRegistryPermanentInitializationError)
  | AlreadyInitializedWithDifferentLimits

DestinationFenceRegistryPermanentInitializationError:
  ArithmeticOverflow | AllocationFailed | FixedStorageBudgetExceeded

CaptureGenerationError:
  Activation | BindingMismatch | GenerationOrder | WriterLifecycle | Preparation
  | RetainedSize | RetainedSizeUnderreported | CaptureMemoryBudgetExceeded | AccountingInvariant

CapturePublisherCloneError:
  QueueClosed | SenderCountOverflow

CapturePublishError:
  Authority | AuthorityBusy | WriterUnavailable | RetainedSize | RetainedSizeUnderreported
  | InvalidPayloadView | CaptureMemoryBudgetExceeded | QueueFull
  | QueueClosed | QueuePoisoned | QueueInvariant | AccountingInvariant

CaptureWriterError:
  Deadline | Authority | InvalidPayloadSharing
  | DiagnosticConversion | Sink(CaptureSinkError) | AccountingInvariant

CaptureSinkError:
  RetainedSize | RetainedSizeUnderreported | RecordLimitExceeded | RetainedByteLimitExceeded
  | SerializationLimitExceeded | SerializationFailure | WriteFailure | FlushFailure
  | ShutdownDeadlineExceeded | AccountingInvariant

MemoryCaptureSinkConstructionError:
  FixedStorageBudgetExceeded | ArithmeticOverflow | AllocationFailed

JournalSinkConstructionError:
  FixedStorageBudgetExceeded | ArithmeticOverflow | Journal(JournalError)

CaptureAccountingSnapshotError:
  Contended { attempts } | TransitionOverflow | EpochOverflow | InvariantViolated

JournalError (existing compatibility/path taxonomy remains exhaustive):
  UnsupportedMagic | InvalidWriterExtension | InvalidSourceFilename | LegacyFormatReadOnly
  | SymlinkNotAllowed | DirectoryDurabilityUnsupported | AlreadyLocked | RecordLimitExceeded
  | AggregateLimitExceeded | Io { phase, source } | InvalidRecord | Json | InvalidRawRecord
  | LengthOverflow | RecordTooLarge
```

Destination-fence registry poison is terminal and never recovered into normal lease service.
Destination busy/capacity/poison, compiled-target/formula/artifact proof mismatches, bounded
thread-name refusal, fixed/scratch refusal, and OS thread creation remain distinct, bounded,
non-secret writer-start outcomes. Failures before writer publication return only the typed
constructor error and release the prepared receipt and destination slot exactly once; only failures
after publication map to health events. Exhaustive no-wildcard conversions and forced drop-order
tests cover every nested variant.

`Io.phase` is a closed non-secret enum for open, metadata, lock, header write/flush/sync,
directory sync, record write, flush, and read operations; replace free-form context strings. The
`CaptureSink` implementation maps the exact journal phase to serialization/write/flush rather than
collapsing it into a catch-all. Existing extension, symlink, lock, legacy-read-only, bounded-read,
CRC/framing, and recovery errors remain public and tested.

Every runtime rejection has one exhaustive, compile-checked `CaptureHealthReason` mapping:

| Cause | Health reason |
| --- | --- |
| `CaptureAuthorityError::GenerationNotReady` | `AuthorityNotReady`; refuse and mark the misused generation incomplete |
| `CaptureAuthorityError::GenerationIncomplete` | `AuthorityIncomplete`; terminal for that generation |
| `CaptureAuthorityError::FrameBindingMismatch` | `FrameBindingMismatch`; refuse the foreign frame without corrupting current authority |
| `CaptureAuthorityError::FrameRejected` | `AuthorityRejected`; refuse and mark the affected generation incomplete |
| authority mutex busy / writer unavailable | `AuthorityBusy` / `WriterUnavailable` |
| unexpected writer/storage exit | `WriterFailed`; terminal capture incompleteness |
| normal supervised writer stop | `WriterStopped`; capture authority ends with writer lifetime |
| sole positive capture supervisor exit/drop | `SupervisorStopped`; terminal capture incompleteness |
| generation activation / binding / order / lifecycle / preparation | `GenerationActivation` / `BindingMismatch` / `GenerationOrder` / `WriterLifecycle` / `GenerationPreparation` |
| producer clone closed / count overflow | `QueueClosed` / `SenderCountOverflow` |
| retained overflow / invalid graph / underreport | `RetainedSizeOverflow` / `InvalidAuthorityGraph` / `RetainedSizeUnderreported` |
| payload view / allocation mismatch | `InvalidPayloadView` / `InvalidPayloadSharing` |
| unified memory refusal | `CaptureMemoryBudgetExceeded` |
| queue full / closed / poison / impossible slot ownership | `QueueFull` / `QueueClosed` / `QueuePoisoned` / `QueueInvariant` |
| accounting reconciliation failure | `AccountingInvariant` |
| diagnostic/source conversion failure | `DiagnosticConversion` |
| sink record / byte / serialization limits | `SinkRecordLimit` / `SinkRetainedByteLimit` / `SerializationLimit` |
| serialization / write / flush failure | `SerializationFailure` / `WriteFailure` / `FlushFailure` |
| frame / shutdown deadline | `FrameDeadlineExceeded` / `ShutdownDeadlineExceeded` |

Initial construction has no published health channel, so its typed error and rejected-bundle
degradation are the complete observable outcome. Exhaustive `match` expressions without wildcards,
table-driven tests for every variant, and secret-redaction assertions make additions fail to compile
until operator semantics are chosen.

---

## Dependency DAG and ownership

| Wave | Lane | Start barrier | Exclusive ownership | Exit gate | Order |
| --- | --- | --- | --- | --- | --- |
| 0 | Documentation/evidence | A3 audit anchor | `docs/**` only | Clean `REFRESH_HEAD`, production tree unchanged, current inventory/formulas/tools/lock published | First |
| 1 | Grouped A4.0 seed | Frozen `REFRESH_HEAD` | Domain/shared contracts, payload types, configuration/app composition, manifests/lockfile, all channel/sink calls, legacy baseline, safe rings/fallible construction, queue Loom | Clean unchanged seed exact head | Serialized |
| 2 | TIME | Frozen seed | Sources/live authority-time production and tests | Clean unchanged TIME exact head | Integrate first |
| 2 | MEM | Same frozen seed | Platform writer-start/conversion/journal/sink hardening behind seed-owned record tokens, platform/app tests, acceptance benchmark | Clean unchanged MEM exact head | Rebase onto TIME, then integrate |
| 3 | Quarter 1 of 4 checkpoint (historical Q2/A4 audit identity) | TIME then rebased MEM integrated | Truth/evidence owner, exact candidate, grouped reviewers | Clean exact-head gate and zero findings | Last |

Create exactly these three grouped implementation worktrees; never create a worktree per checklist
task:

```text
.worktrees/q2-a4-seed    feat/q2-a4-seed
.worktrees/q2-a4-time    feat/q2-a4-trusted-time
.worktrees/q2-a4-memory  feat/q2-a4-capture-memory
```

The seed worktree is the integration owner. After it freezes, create TIME and MEM from the exact
`A4_SEED_HEAD`. Neither Wave 2 lane edits `Cargo.toml`, `Cargo.lock`, domain contracts, scripts,
production app composition, or checkpoint evidence. MEM may edit only the embedded `#[cfg(test)]`
module in app `main.rs`; TIME never edits platform or app files. TIME integrates first. MEM then
rebases with `--onto` onto that exact integration commit before its final gate.

## Wave 0: freeze the documentation head

### Task 0: Commit the preflight without changing production

**Files:**

- Modify/Create: the already scoped Wave 0 files under `docs/**`
- Create: `docs/reports/2026-07-17-q2-a4-wave0-refresh.md`
- Modify: no production, manifest, lockfile, script, workflow, or application file

**Interfaces:**

- Consumes: locally approved A3 `ab3f7c19000884357c38702edf6b4acc6a80c483`
- Produces: one clean `WAVE0_HEAD` and truthful optional-hosted status

- [ ] **Step 1: Verify the current worktree base and documentation-only diff**

  ```bash
  set -euo pipefail
  A3_HEAD=ab3f7c19000884357c38702edf6b4acc6a80c483
  test "$(git rev-parse HEAD)" = "$A3_HEAD"
  git diff --check
  git diff --cached --check
  EMPTY_OUTPUT="$(git diff --name-only | awk '$0 !~ /^docs\// { print }')"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git diff --cached --name-only | awk '$0 !~ /^docs\// { print }')"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git ls-files --others --exclude-standard | awk '$0 !~ /^docs\// { print }')"
  test -z "$EMPTY_OUTPUT"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json number --jq .number)" = 1
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json headRefName --jq .headRefName)" = \
    feat/stage-1-foundation
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json headRefOid --jq .headRefOid)" = \
    "$A3_HEAD"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json baseRefName --jq .baseRefName)" = main
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json state --jq .state)" = OPEN
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json isDraft --jq .isDraft)" = true
  ```

  Expected: only intended `docs/**` artifacts are dirty/untracked.

- [ ] **Step 2: Review and commit the exact Wave 0 artifacts**

  Stage only the reviewed documentation files, commit intentionally, and record:

  ```bash
  set -euo pipefail
  A3_HEAD=ab3f7c19000884357c38702edf6b4acc6a80c483
  git add docs/project-memory.md \
    docs/audits/2026-07-17-hosted-actions-account-gate.md \
    docs/research/2026-07-17-capture-retained-memory-and-queue.md \
    docs/superpowers/plans/2026-07-17-q2-a4-capture-authority-preflight.md
  test "$(git diff --cached --name-only)" = "$(printf '%s\n' \
    docs/audits/2026-07-17-hosted-actions-account-gate.md \
    docs/project-memory.md \
    docs/research/2026-07-17-capture-retained-memory-and-queue.md \
    docs/superpowers/plans/2026-07-17-q2-a4-capture-authority-preflight.md)"
  git diff --cached --check
  git commit -m "docs: freeze hardened q2 a4 capture plan"
  WAVE0_HEAD="$(git rev-parse HEAD)"
  git merge-base --is-ancestor "$A3_HEAD" "$WAVE0_HEAD"
  EMPTY_OUTPUT="$(git diff --name-only "$A3_HEAD..$WAVE0_HEAD" | awk '$0 !~ /^docs\// { print }')"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

- [ ] **Step 3: Verify the unchanged production tree at the committed head**

  ```bash
  set -euo pipefail
  WAVE0_HEAD="$(git rev-parse HEAD)"
  ./scripts/verify.sh
  test "$(git rev-parse HEAD)" = "$WAVE0_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

  Expected: the documentation descendant passes locally at one unchanged clean commit.

- [ ] **Step 4: Classify optional hosted evidence without conflating states**

  Query the exact SHA and classify all four outcomes: no workflow run; a run with zero runner
  assignments/executed steps (including unassigned job skeleton objects); assigned but
  incomplete/failed jobs; or complete success. A no-runner run is called an account-gated run only
  when its run URL/annotation matches the separately persisted account-gate audit; absence of a run
  is never called billing failure.

  ```bash
  set -euo pipefail
  A3_HEAD=ab3f7c19000884357c38702edf6b4acc6a80c483
  REPO=Sawmonabo/market-squawk
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EVIDENCE_DIR="$REPO_ROOT/target/q2-a4-hosted/a3"
  rm -rf "$EVIDENCE_DIR"
  mkdir -p "$EVIDENCE_DIR"
  RUN_LIST_JSON="$(gh run list --repo "$REPO" --workflow CI --commit "$A3_HEAD" \
    --json databaseId,headSha,createdAt,url,status,conclusion --limit 100)"
  printf '%s\n' "$RUN_LIST_JSON" | tee "$EVIDENCE_DIR/run-list.json" >/dev/null
  RUN_ID="$(printf '%s\n' "$RUN_LIST_JSON" | jq -r \
    --arg sha "$A3_HEAD" \
    'map(select(.headSha == $sha)) | sort_by(.createdAt) | reverse |
      .[0].databaseId // empty')"
  if test -z "$RUN_ID"; then
    HOSTED_CLASS=no_run_for_exact_sha
  else
    RUN_JSON="$(gh run view "$RUN_ID" --repo "$REPO" \
      --json url,status,conclusion,headSha,jobs)"
    printf '%s\n' "$RUN_JSON" | tee "$EVIDENCE_DIR/run.json" >/dev/null
    RUN_HEAD="$(printf '%s\n' "$RUN_JSON" | jq -r .headSha)"
    test "$RUN_HEAD" = "$A3_HEAD"
    JOBS_ENDPOINT="repos/$REPO/actions/runs/$RUN_ID/jobs?filter=all&per_page=100"
    JOBS_JSON="$(gh api "$JOBS_ENDPOINT")"
    printf '%s\n' "$JOBS_JSON" | tee "$EVIDENCE_DIR/jobs.json" >/dev/null
    JOB_OBJECT_COUNT="$(printf '%s\n' "$JOBS_JSON" | jq '.jobs | length')"
    ASSIGNED_JOB_COUNT="$(printf '%s\n' "$JOBS_JSON" | jq \
      '[.jobs[] | select(
        (.runner_id // 0) != 0 or
        ((.runner_name // "") | length) != 0 or
        any((.steps // [])[];
          .started_at != null or
          .status == "in_progress" or
          .status == "completed" or
          .conclusion != null)
      )] | length')"
    if test "$JOB_OBJECT_COUNT" -eq 0 || test "$ASSIGNED_JOB_COUNT" -eq 0; then
      HOSTED_CLASS=run_exists_zero_assigned_jobs
    else
      VERIFY_OK="$(printf '%s\n' "$JOBS_JSON" | jq \
        '[.jobs[] | select(.name == "verify" and .conclusion == "success")] | length == 1')"
      MACOS_OK="$(printf '%s\n' "$JOBS_JSON" | jq \
        '[.jobs[] | select(.name == "macos" and .conclusion == "success")] | length == 1')"
      WINDOWS_OK="$(printf '%s\n' "$JOBS_JSON" | jq \
        '[.jobs[] | select(.name == "windows" and .conclusion == "success")] | length == 1')"
      if test "$VERIFY_OK" = true && test "$MACOS_OK" = true && test "$WINDOWS_OK" = true; then
        HOSTED_CLASS=assigned_jobs_success
      else
        HOSTED_CLASS=assigned_jobs_failed_or_incomplete
      fi
    fi
  fi
  printf '%s\n' "$HOSTED_CLASS" | tee "$EVIDENCE_DIR/class.txt"
  ```

  Persist the exact class and supporting URL/JSON. `assigned_jobs_failed_or_incomplete` is never
  reported as an account gate or success. Optional hosted status does not block the seed or local
  approval. Do not rerun Actions merely to advance this plan.

- [ ] **Step 5: Publish the verified Wave 0 head**

  ```bash
  set -euo pipefail
  WAVE0_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push origin "HEAD:refs/heads/docs/q2-a4-wave0"
  test "$(git rev-parse HEAD)" = "$WAVE0_HEAD"
  ```

  Comment on canonical PR 1 explicitly:

  ```bash
  set -euo pipefail
  WAVE0_HEAD="$(git rev-parse HEAD)"
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  HOSTED_CLASS="$(sed -n '1p' "$REPO_ROOT/target/q2-a4-hosted/a3/class.txt")"
  test -n "$HOSTED_CLASS"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 Wave 0 published at ${WAVE0_HEAD}; local verifier passed; hosted class: ${HOSTED_CLASS}; raw evidence: root target/q2-a4-hosted/a3.")"
  test -n "$COMMENT_URL"
  ```

- [ ] **Step 6: Create the grouped seed worktree and write RED refresh evidence**

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  WAVE0_HEAD="$(git rev-parse HEAD)"
  git worktree add "$REPO_ROOT/.worktrees/q2-a4-seed" \
    -b feat/q2-a4-seed "$WAVE0_HEAD"
  cd "$REPO_ROOT/.worktrees/q2-a4-seed"
  test "$(git rev-parse HEAD)" = "$WAVE0_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  test ! -e docs/reports/2026-07-17-q2-a4-wave0-refresh.md
  ```

  Expected RED: the mandatory refresh report does not exist. Create it with the exact SHA and raw
  command output for: all 39 `raw_capture_channel` calls in nine files; all 23 `try_frame` calls in
  12 files; all 18 `MemoryCaptureSink::default()` calls in eight files; all ten publisher clone
  calls, the `CaptureContext` derived-Clone expectation, two positive publisher `Clone` assertions,
  and the inner-sender clone; all four production/test `RawCaptureFrameView` impls and every bundle;
  every fixed/resident/record/sink formula owner; `rustc`, `cargo`, `cargo-deny`, `cargo-audit`,
  `gitleaks`, and `gh` versions; `shasum -a 256 Cargo.lock`; and `cargo metadata --locked`.
  Use `rg` commands whose complete output is embedded in the report. If any count or owner differs
  from this plan, stop, update/review the plan, and do not begin implementation.

  The inventory distinguishes method invocations from the `try_frame` definition and matches both
  imported and fully qualified trait implementations:

  ```bash
  set -euo pipefail
  test "$(rg -n 'raw_capture_channel\(' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 39
  test "$(rg -l 'raw_capture_channel\(' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 9
  test "$(rg -n '\.try_frame\(' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 23
  test "$(rg -l '\.try_frame\(' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 12
  test "$(rg -n 'MemoryCaptureSink::default\(\)' apps crates --glob '*.rs' | \
    wc -l | tr -d ' ')" -eq 18
  test "$(rg -n '\b[a-zA-Z_]*publisher\.clone\(\)|\.publisher\.clone\(\)' \
    apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 10
  test "$(rg -n 'impl (market_squawk_domain::)?RawCaptureFrameView for' \
    apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 4
  test "$(rg -n 'impl (market_squawk_domain::)?CaptureAuthorityBundle for' \
    apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 4
  ```

- [ ] **Step 7: Validate, commit, and publish the exact refresh head**

  ```bash
  set -euo pipefail
  WAVE0_HEAD="$(git rev-parse HEAD)"
  git diff --check
  git diff --cached --check
  test "$(git status --short)" = \
    "?? docs/reports/2026-07-17-q2-a4-wave0-refresh.md"
  EMPTY_OUTPUT="$(git diff --name-only "$WAVE0_HEAD" -- . ':!docs/reports/2026-07-17-q2-a4-wave0-refresh.md')"
  test -z "$EMPTY_OUTPUT"
  git add docs/reports/2026-07-17-q2-a4-wave0-refresh.md
  git diff --cached --check
  git commit -m "docs: freeze q2 a4 implementation inventory"
  REFRESH_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git diff --name-only "$WAVE0_HEAD..$REFRESH_HEAD" | \
    awk '$0 != "docs/reports/2026-07-17-q2-a4-wave0-refresh.md" { print }')"
  test -z "$EMPTY_OUTPUT"
  ./scripts/verify.sh
  test "$(git rev-parse HEAD)" = "$REFRESH_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push -u origin feat/q2-a4-seed
  test "$(git rev-parse HEAD)" = "$REFRESH_HEAD"
  ```

  Comment on canonical PR 1 with the exact refresh head, lockfile digest, and unchanged-production
  assertion. This is the published seed start barrier:

  ```bash
  set -euo pipefail
  REFRESH_HEAD="$(git rev-parse HEAD)"
  LOCK_DIGEST="$(shasum -a 256 Cargo.lock | awk '{ print $1 }')"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 refresh published at ${REFRESH_HEAD}; production tree remains equal to Wave 0; Cargo.lock SHA-256 ${LOCK_DIGEST}; evidence: docs/reports/2026-07-17-q2-a4-wave0-refresh.md.")"
  test -n "$COMMENT_URL"
  ```

## Wave 1: one serialized grouped A4.0 seed

### Task 1: Establish every cross-lane contract and migration

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/market-squawk-domain/Cargo.toml`
- Modify: `crates/market-squawk-domain/src/lib.rs`
- Create: `crates/market-squawk-domain/src/retained.rs`
- Modify: `crates/market-squawk-domain/src/capture.rs`
- Modify: `crates/market-squawk-domain/tests/capture_authority_contract.rs`
- Create: `crates/market-squawk-domain/tests/capture_contract_compile_fail.rs`
- Create: `crates/market-squawk-domain/tests/ui/capture_bundle_missing_retained_bytes.rs`
- Create: `crates/market-squawk-domain/tests/ui/capture_bundle_missing_retained_bytes.stderr`
- Create: `crates/market-squawk-domain/tests/ui/raw_frame_missing_retained_footprint.rs`
- Create: `crates/market-squawk-domain/tests/ui/raw_frame_missing_retained_footprint.stderr`
- Create: `crates/market-squawk-domain/tests/ui/raw_frame_missing_capture_payload.rs`
- Create: `crates/market-squawk-domain/tests/ui/raw_frame_missing_capture_payload.stderr`
- Create: `crates/market-squawk-domain/tests/ui/admission_missing_resident_frame_bytes.rs`
- Create: `crates/market-squawk-domain/tests/ui/admission_missing_resident_frame_bytes.stderr`
- Create: `crates/market-squawk-domain/tests/ui/capture_receipt_missing_retention.rs`
- Create: `crates/market-squawk-domain/tests/ui/capture_receipt_missing_retention.stderr`
- Create: `crates/market-squawk-domain/tests/ui/capture_receipt_missing_dynamic_size.rs`
- Create: `crates/market-squawk-domain/tests/ui/capture_receipt_missing_dynamic_size.stderr`
- Modify: `crates/market-squawk-sources/src/lib.rs`
- Modify: `crates/market-squawk-sources/src/bounded.rs`
- Modify: `crates/market-squawk-sources/src/live.rs`
- Modify: `crates/market-squawk-sources/src/capture.rs`
- Modify: `crates/market-squawk-sources/tests/contracts.rs`
- Modify: `crates/market-squawk-sources/tests/registry_authority.rs`
- Modify: `crates/market-squawk-platform/Cargo.toml`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `crates/market-squawk-platform/src/capture.rs`
- Create: `crates/market-squawk-platform/src/capture/admission.rs`
- Modify: `crates/market-squawk-platform/src/capture/control.rs`
- Modify: `crates/market-squawk-platform/src/capture/diagnostic.rs`
- Create: `crates/market-squawk-platform/src/capture/queue.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer/lifecycle.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer/destination.rs`
- Modify: `crates/market-squawk-platform/src/raw_record.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer/sink.rs`
- Modify: `crates/market-squawk-platform/src/config.rs`
- Modify: `crates/market-squawk-platform/tests/config_precedence.rs`
- Modify: `crates/market-squawk-platform/tests/capture_authority_bridge.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/src/source/mod.rs`
- Modify: `apps/market-squawk/src/source_supervisor.rs`
- Modify: all remaining channel, publisher-clone, and memory-sink call files in the inventories below
- Create: `crates/market-squawk-platform/benches/capture_admission.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/backend.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/collector.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/endpoints.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/fixture.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/producer_inventory.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/schema.rs`
- Create: `crates/market-squawk-platform/benches/capture_admission/workload.rs`
- Create: `scripts/check_capture_queue_loom.sh`
- Create: `scripts/assert_expected_red.sh`
- Create: `scripts/capture_benchmark_host_gate.sh`
- Create: `scripts/classify_hosted_run.sh`
- Create: `scripts/tests/test_assert_expected_red.sh`
- Create: `scripts/tests/expected-red/valid-runtime.log`
- Create: `scripts/tests/expected-red/valid-compiler.log`
- Create: `scripts/tests/expected-red/invalid-filter-only.log`
- Create: `scripts/tests/expected-red/invalid-unrelated-rust.log`
- Create: `scripts/tests/expected-red/invalid-environment-matrix.tsv`
- Create: `scripts/tests/expected-red/invalid-mixed.log`
- Create: `scripts/tests/expected-red/invalid-success.log`
- Create: `scripts/tests/expected-red/invalid-sentinel-assertion.log`
- Create: `scripts/tests/expected-red/invalid-sentinel-rust.log`
- Create: `scripts/tests/expected-red/invalid-uncorrelated-compiler.log`
- Create: `scripts/tests/test_capture_benchmark_host_gate.py`
- Create: `scripts/tests/test_classify_hosted_run.py`
- Modify: `scripts/verify.sh`
- Create: `docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md`

**Interfaces:**

- Consumes: exact clean published `REFRESH_HEAD`
- Produces: one clean seed with typed retained errors, one Arc helper, one shared `CapturePayload`
  contract used by all three owners, exact formulas, renamed configuration, fallible channel,
  prevalidated rotation, safe sequence-owned fixed rings, fallible publisher cloning, 39
  channel migrations, 18 bounded-sink migrations, ten direct publisher-clone migrations plus all
  three type-level Clone expectations, the authoritative total/resident identity/frame reservation
  lifecycle, trybuild/Loom enforcement, and closed standard-reference/candidate benchmark backends

The following implementation barriers are serialized. The reviewed rebaseline correction above
supersedes the former pre-ring measurement barrier: implement and integrate the ring first, and do
not measure either backend before the future clean `A4_STANDARD_REFERENCE_HEAD` freeze.

- [ ] **Step 0A: RED bootstrap inventory and add pinned development dependencies**

  Start in the grouped seed worktree at the published refresh head:

  ```bash
  set -euo pipefail
  REFRESH_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  test -f docs/reports/2026-07-17-q2-a4-wave0-refresh.md
  test ! -f crates/market-squawk-platform/benches/capture_admission.rs
  ! rg -n '^criterion\s*=' Cargo.toml >/dev/null
  rg -n '^trybuild\s*=\s*"1\.0"$' Cargo.toml
  rg -n '^loom\s*=\s*"=0\.7\.2"$' Cargo.toml
  ```

  Expected RED: Criterion and the harness are absent. Preserve the already reviewed workspace
  `trybuild = "1.0"` lock resolution and exact `loom = "=0.7.2"`; add
  `criterion = "=0.8.2"`, the domain `trybuild` dev-dependency, and platform Criterion plus
  target-`cfg(loom)` dev-dependencies. Do not register the benchmark target or run `--benches` until
  Step 3 creates its source. Do not run any `--locked` build until the lockfile is updated.

  Create `scripts/assert_expected_red.sh` and deterministic shell fixtures/tests in
  `scripts/tests/expected-red/`. Its closed CLI is `LOG EXACT_SENTINEL ALLOWED_RUST_ERROR_REGEX
  REQUIRED_SYMBOL_REGEX`. It must inspect a nonempty regular file, reject the complete unrelated
  failure taxonomy above first, then accept only (a) the exact sentinel as a standalone diagnostic
  line or (b) at least one allowed Rust error code and at least one required exact symbol. Empty
  arguments, broad catch-all patterns, target/filter/module/file-name-only matches, missing logs,
  mixed intended-plus-environment failures, and zero-error success logs fail closed. Fixtures cover
  every rejected environment class, a same-filter-name unrelated assertion, an unrelated Rust error
  beside the required symbol, sentinel-only runtime failure, intended compiler failure, and a mixed
  intended/environment failure. The script itself is strict-mode shell, has no network behavior,
  and is syntax-checked plus exercised before any substantive RED phase:

  ```bash
  set -euo pipefail
  bash -n scripts/assert_expected_red.sh scripts/tests/test_assert_expected_red.sh
  ./scripts/tests/test_assert_expected_red.sh
  rg -F './scripts/tests/test_assert_expected_red.sh' scripts/verify.sh
  ```

  Wire the shell test into `scripts/verify.sh` in this same step; it is a mandatory deterministic
  default-suite gate, not an ad hoc bootstrap check. The test owns the closed fixture set enumerated
  in Task 1's file list, expands every row of `invalid-environment-matrix.tsv` into an isolated
  temporary log, and asserts the classifier's exact exit status and diagnostics. Any later change
  to the classifier, test, fixture matrix, or verify wiring remains seed-owned and must pass Steps 8
  and 9 at the exact committed head.

  Update the lock graph deliberately, then prove the lock works:

  ```bash
  set -euo pipefail
  cargo check -p market-squawk-domain --tests --all-features
  cargo check -p market-squawk-platform --tests --all-features
  cargo metadata --format-version 1 --locked >/dev/null
  cargo tree -p market-squawk-platform --locked | rg 'criterion v0\.8\.2'
  git diff -- Cargo.toml Cargo.lock crates/market-squawk-domain/Cargo.toml \
    crates/market-squawk-platform/Cargo.toml
  git diff --check
  git diff --cached --check
  ```

- [ ] **Step 0B: Keep bootstrap changes uncommitted until the queue-independent baseline barrier**

  Dependency/lock changes remain in the grouped seed worktree while Steps 1 through 3 install every
  queue-independent retained contract, payload representation, frame/bundle implementation,
  mechanical module split, and the final shared benchmark collector/endpoints. Do not register a
  bench target before its source exists. Do not create an intermediate dependency-only or
  trait-only commit. The first seed commit is the complete, full-workspace-green
  `A4_BASELINE_CODE_HEAD` defined after Step 3; it still uses the standard channel.

- [ ] **Step 1: RED the complete domain payload and ownership-preserving trait surface**

  Add checked method calls and complete `CapturePayload` constructor/limit/identity tests before the
  types and traits expose them, then capture the compile failure. This purpose-built compiler RED
  deliberately runs before the trybuild runner exists: an unaccepted trybuild stderr produces
  `wip/*.stderr` diagnostics that the mandatory classifier correctly rejects as malformed evidence.
  Do not weaken that rejection. After the exact missing-contract RED is independently classified,
  implement the domain surface, add the trybuild runner/fixtures proving a frame cannot omit
  `capture_payload` or retained-size, an admission cannot omit resident-shared frame accounting,
  a concrete receipt cannot omit either resident-generation retention or its checked additional
  dynamic-size declaration, and a bundle cannot omit retained-size. Generate each stderr in
  isolation, inspect it for the exact missing method, accept it, and require the complete trybuild
  suite to pass. Add typed
  overflow, underreport, invalid-graph, zero-dynamic, maximum-capacity identity, Arc value layout,
  Arc byte-slice layout, and over-aligned pointee tests.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/domain-payload
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-domain --test capture_authority_contract \
    --all-features --locked \
    >"$RED_DIR/test.log" 2>&1; then
    printf '%s\n' 'expected domain payload RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/test.log" \
    'MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT' \
    'error\[E(0046|0407|0432|0599)\]' \
    'CapturePayload|capture_payload|checked_retained_bytes|checked_retained_footprint'
  ```

  Expected RED: checked/ownership types and methods are absent. Record the exact diagnostics before
  implementation. After the domain surface exists, create each omission fixture, run
  `TRYBUILD=overwrite cargo test -p market-squawk-domain --test capture_contract_compile_fail
  --all-features --locked`, inspect every generated stderr for `error[E0046]` and its exact required
  method name and no unrelated diagnostic, then rerun without `TRYBUILD=overwrite`. Overwrite mode
  writes each reviewed stderr directly beside its `tests/ui/*.rs` fixture; there is no `wip/` move
  in this flow.
  Run `python3 scripts/check_brand.py`, documentation-contract tests, `git diff --check`, and
  `git diff --cached --check` after this execution correction and at the completed domain barrier.

- [ ] **Step 2: GREEN the complete domain payload seam without an intermediate commit**

  Implement the complete empty/shared `CapturePayload` representation, both named constructors,
  live/committed-wire limits, allocation-sharing accessor, checked retained formula, Arc layout
  helper, and every required trait/interface/formula above. Keep the former sources-private helper
  migration for the external-owner cutover in Step 3. Accept generated trybuild stderr only after
  confirming each failure is the
  missing required method, not an unrelated syntax/import failure. Run:

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-domain --all-targets --all-features --locked
  cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
  git diff --check
  git diff --cached --check
  ```

  Review the domain/helper/trybuild diff, but do not commit it yet. Required no-default methods would
  break the external sources/platform implementations until Step 3 migrates them. Keep this one
  intentional TDD worktree dirty and proceed directly to the atomic queue-independent cutover.

- [ ] **Step 3A: Write and run RED external-owner payload migration tests**

  First add failing tests for empty allocation-free storage, nonempty pointer-sharing, a copied
  conversion pointer mismatch, borrowed-view mismatch, 4 MiB live exact/one-over, 33,554,431-byte
  compatibility exact/one-over, historical `> 4 MiB` read-only round-trip, source Arc capacity, and
  resident-shared `FrameSessionBinding` exact/wrong-pointer/omitted allocation, diagnostic zero
  resident-shared bytes, and every invalid bundle edge. Run the focused domain/sources/platform
  tests and record their RED failures:

  ```bash
  set -euo pipefail
  cargo test -p market-squawk-domain --test capture_authority_contract \
    --all-features --locked
  RED_DIR=target/q2-a4-red/external-payload-owners
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-sources --test contracts --all-features --locked \
    >"$RED_DIR/sources.log" 2>&1; then
    printf '%s\n' 'expected sources payload-owner RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/sources.log" \
    'MSQ_A4_RED_SOURCE_PAYLOAD_OWNER' \
    'error\[E(0046|0308|0599)\]' \
    'capture_payload|checked_retained_bytes|checked_retained_footprint|FrameSessionBinding'
  if cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked >"$RED_DIR/platform.log" 2>&1; then
    printf '%s\n' 'expected platform payload-owner RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/platform.log" \
    'MSQ_A4_RED_PLATFORM_PAYLOAD_OWNER' \
    'error\[E(0046|0308|0599)\]' \
    'CapturedRawRecord|capture_payload|checked_retained_bytes|checked_retained_footprint'
  ```

  Expected RED: the complete domain `CapturePayload` and constructors are already green. External
  production/diagnostic frame owners, raw records, bundle implementations, the former sources Arc
  helper, historical round trip, borrowed-view identity, and compatibility-copy behavior have not
  yet migrated and therefore fail their exact assertions or required trait implementations.

- [ ] **Step 3B: Implement two-tier payload ownership**

  Migrate the former sources-private Arc helper to the domain helper. Move `RawMarketFrame`,
  `DiagnosticCaptureFrame`, and `RawCaptureRecord` to normalized
  `CapturePayload`; preserve wire schemas and borrowed payload access. Implement its private
  empty/shared representation, checked maximum constructor, ownership-preserving clone seam,
  two-tier live/compatibility policy, exact frame/bundle formulas, and direct payload clone-pointer
  tests. Leave writer conversion on the explicitly over-reserved compatibility-copy quote until MEM
  installs and proves the generic zero-copy conversion; do not undercharge the temporary copy.

- [ ] **Step 3C: GREEN every queue-independent contract and define the final harness**

  This step defines and tests the closed harness, but does not freeze an evidence head or authorize a
  run. The exact file hashes freeze later at the reviewed `A4_STANDARD_REFERENCE_HEAD` barrier.

  Complete the behavior-preserving `capture.rs`/`capture/admission.rs` split without changing the
  standard channel. Register two deliberately separate targets. `capture_admission_evidence` is the
  authoritative fixed-quota executable and is the only target allowed to emit baseline/candidate
  evidence. `capture_admission_criterion` is a genuine adaptive Criterion engineering target over
  the same five closed production-operation seams, but is permanently labeled
  `exploratory_zero_authority` and can never establish or compare an evidence baseline. Partition
  the authoritative target into immutable `benchmark_identity`, `collector`, `endpoints`,
  `evidence_io`, `fixture`, `producer_inventory`, `schema`, and `workload` modules plus one
  separately hashed closed `backend` adapter. The benchmark abstraction
  freezes one producer-duplication factory: standard-channel duplication returns `Ok(clone)`;
  candidate duplication later calls production `try_clone`. It freezes identical workload,
  outcome accounting, endpoints, and collector code for both backends.

  The production representative capture-producer inventory is a checked sum of exactly one current
  producer: the single `SupervisedSourceTask` created by `run_source` owns the one sequential
  `MarketSource::run_session`; Coinbase and mock capture publication occurs inline in that task;
  event analysis and the capture writer own no publisher; Coinbase has no production child capture
  producer. The deterministic Coinbase fixture likewise moves its only publisher into one source
  task. The harness and refresh report list these contributors, compute `1` with checked nonzero
  addition, deduplicate it against numeric case 1 while retaining the `representative` label, and
  reject zero, overflow, or an undocumented contributor. `available_parallelism` is host metadata,
  never fan-in authority.

  Freeze all five endpoint families in rustdoc and code: queue push, queue pop, capture admission,
  writer append, and flush-inclusive writer latency. The fixed-operation matrix uses payloads
  `{0, 1_024, 4_194_304}`, depths `{1, 64, 16_384}` for queue/admission endpoints, depth `{64}` only
  for writer endpoints, producer cases `{1, 2, 4, 8}` plus the labeled representative case, and
  a checked payload-aware quota: `max(1_000_000, producers * 100_000)` successful operations for
  the 0-byte and 1,024-byte cells, and exactly 10,000 successful operations for every 4 MiB stress
  cell in each repetition. The five repetitions therefore retain at least 50,000 maximum-payload
  observations per cell without pretending a consumer system can hash roughly 400 GB/s. Queue
  permits and barriers remain outside
  the named latency interval; the overall case timer includes producer/thread/barrier/permit wait.
  Queue-pop has exactly one production receiver owner with producer fan-in. Preallocate disjoint
  producer-local slices totaling the cell's exact declared operation/sample quota, join every producer before
  aggregation, and reject collector overflow, lost outcome, malformed result, zero duration, or any
  requested/completed/sample count mismatch. Record configured depth separately from the exact
  production effective-capacity quote. The matrix does not run sustained time epochs.

  Separately freeze the representative sustained RSS fixture: 1,024-byte payload, depth 16,384,
  representative fan-in, two five-second warm epochs, ten ten-second measured burst/drain epochs,
  and exactly 100 typed RSS samples in every measured epoch at the 100 ms cadence. Every sample
  records epoch ownership, target offset, observed monotonic offset, and bytes; a missed deadline
  outside the fixed 25 ms tolerance fails rather than backfilling observations. Sustained offered load
  uses the real unthrottled bounded production queue and requires both accepted operations and
  refusals; it does not use capture admission because admission poison on deliberate saturation is
  a different contract. A deterministic comparable-full fixture is separate from the matrix and
  runs for both backends; it gates the consumer until a depth-one queue refuses and requires exactly
  one recorded `QueueFull`. The candidate backend adapter additionally exposes a test/benchmark-only
  adversarial forced-lock fixture. It uses a real `CaptureMessage` and reports measured accounting
  reconciliation, or emits an explicit typed-unavailable result when the production sequence
  protocol makes the probe inapplicable. It is non-authoritative, noncomparative, never enters
  endpoint or acceptance results, and never requires a production `QueueContended` outcome. Neither
  case is conflated with unsaturated acceptance, which requires
  zero refusals. One externally selected standard repetition is exactly matrix + comparable full +
  sustained RSS. One externally selected candidate repetition is that identical set plus the
  separately named noncomparative forced-lock fixture.

  Every manifest records the exact `immutable_module_sha256` object with separate
  `benchmark_identity`, `collector`, `endpoints`, `evidence_io`, `fixture`, `producer_inventory`,
  `schema`, and `workload` members; a separate
  `entrypoint_sha256`; a separate `backend_sha256`; selected `backend`; exact expected fixture set;
  result schema; relative evidence-local executable path and digest; measured code head; hashes of
  production libraries linked into the executable; host fingerprint; toolchain fingerprint; and
  release-profile fingerprint. It also binds the ordered five repetition digests, every controlled
  artifact digest, the exact build command, sanitized build-environment policy and digest, Cargo
  executable digest, the exact real pinned-toolchain Cargo/rustc binaries, the explicitly selected
  target linker and its required SDK/linker inputs, bound Git executable/config environment, build
  script and bounded build-helper modules, every host-gate helper, and every separately split
  build-evidence preparer module. The preparer owns the exact Cargo invocation under a minimal
  constructed tool PATH, rejects loader injection, discovered Cargo config, and unowned compiler/
  profile/target override surfaces, captures bounded Cargo JSON itself, and no-clobber publishes the exact
  artifact. `build.rs` independently requires the closed-build policy, exact feature/profile facts,
  clean Git head, and command/environment digests. Candidate startup requires
  `CAPTURE_BENCH_BASELINE_MANIFEST`,
  recomputes every immutable module hash, and fails before measurement unless each equals the
  persisted standard manifest. Host, toolchain, and release-profile fingerprints must also equal
  the baseline. The backend hash must differ; no whole-harness equality claim includes it.

  Create `scripts/capture_benchmark_host_gate.sh` plus its capability-confined Python helper and a
  deterministic closed adversarial matrix. `measure` owns preflight, all five exact repetitions,
  continuous bounded monitoring, and postflight while the lock remains held. It creates a private
  mode-0500 evidence-local execution copy from the descriptor-verified runner, then binds the
  original runner, execution copy, and build evidence by device, inode, size, and SHA-256 before,
  throughout, and after every repetition. The no-other-agent attestation is an explicit residual
  same-UID threat boundary; periodic pathname checks are not described as mathematically race-free.
  `preflight`
  acquires and attests the already-created exclusive evidence lock, rejects any other active
  repository agent, examines a complete `ps` process inventory with PID/PPID/state/comm/full argv
  transiently, rejects competing Cargo/rustc/full-command benchmark processes, and atomically records host ID,
  boot/session ID, uptime/load, power mode, thermal/throttling state when available, CPU affinity,
  scheduler/nice policy, toolchain, target, release profile, Git head, and a lock-owner nonce.
  `postflight` repeats that inventory while the lock is still held and exits nonzero on host/boot,
  power, thermal, affinity/scheduler, toolchain/target/profile, Git-head, lock-nonce, sleep/wake, or
  competitor/agent deviation. Preflight requires normalized one-minute load no greater than 0.10
  per logical CPU; unavailable load or CPU-count evidence is a typed gate failure. Postflight records
  normalized load diagnostically but never compares it with the idle threshold or preflight value,
  because the measured workload itself remains in the one-minute load average. Wall elapsed versus
  monotonic elapsed may differ by at most two seconds, which detects sleep/wake without requiring an
  impossible identical raw load sample. Full argv matching—not the truncated Linux `comm` name—identifies
  `capture_admission`, but raw argv is never persisted because it may contain credentials. Persisted
  process evidence contains only PID, PPID, state, bounded `comm`, a redacted command class, and a
  digest of the transient canonical inventory in a mode-0600 file under the canonical controlled
  evidence root. All file access requires descriptor-relative no-follow primitives, exact bounded
  read/write loops, private ownership/mode, stable descriptor/path identity, single links, strict
  duplicate-free JSON schemas, and no-clobber fsync publication. Release requires both the exact
  preflight ticket and caller-supplied lock/owner/nonce identity, verifies the lock contains only its
  owner before unlink, and preserves the lock on mismatch. Tests include secret-bearing argv
  fixtures and prove neither stdout, stderr, nor persisted JSON contains the secret. The integration
  owner supplies a persisted `no-other-active-agents`
  attestation only after pausing every subagent. Fake Darwin/Linux fixtures cover all success and
  invalidation branches, including a valid high postflight load caused by the benchmark plus real
  competitor, thermal, power, sleep/wake, root/output/attestation/owner replacement, partial I/O,
  replace/restore, and lock-loss failures. Preflight creates only `owner.json` inside the
  already-exclusive lock; postflight proves the same identities and nonce, and explicit release—not
  a force-cleanup trap—removes only the caller-bound exact owner before `rmdir`.
  After postflight, the benchmark executable's finalize-only mode consumes
  those immutable JSON files, verifies their digests and valid comparison, and only then writes the
  top-level manifest. The finalizer requires exact root and host-directory allowlists, exact
  configured/effective capacity, throughput, refusal and sustained/RSS invariants, lowercase SHA
  values, a full lowercase Git object ID, and `CAPTURE_BENCH_FINALIZE_ONLY=1`; it then self-reads the
  no-clobber manifest and requires typed equality. The manifest binds preflight, monitor,
  postflight, and comparison SHA-256 values;
  missing or stale host evidence makes the run diagnostic-only.

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test --workspace --all-targets --all-features --locked
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  cargo build --workspace --all-features --locked
  cargo bench -p market-squawk-platform \
    --bench capture_admission_evidence \
    --bench capture_admission_criterion \
    --all-features --locked --no-run
  python3 -m unittest \
    scripts.tests.test_capture_benchmark_host_gate \
    scripts.tests.test_capture_benchmark_prepare_build_evidence
  git diff --check
  git diff --cached --check
  ```

  Review the exact bootstrap/domain/source/platform/module-split/harness diff. Prove no ring backend,
  public channel-result, publisher-Clone, configuration, or application call-site change exists.
  Stage that exact reviewed set, commit as
  `feat(capture): freeze queue-independent capture contracts`, and record:

  ```bash
  set -euo pipefail
  git diff --cached --check
  git commit -m "feat(capture): freeze queue-independent capture contracts"
  A4_BASELINE_CODE_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  rg -n 'std::sync::mpsc::sync_channel|mpsc::sync_channel' \
    crates/market-squawk-platform/src/capture.rs
  ./scripts/verify.sh
  cargo bench -p market-squawk-platform \
    --bench capture_admission_evidence \
    --bench capture_admission_criterion \
    --all-features --locked --no-run
  test "$(git rev-parse HEAD)" = "$A4_BASELINE_CODE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

- [ ] **Step 3D: SUPERSEDED here; retain the standard-reference run template**

  Do not execute this step at its historical pre-ring location. Apply its workload and host-gate
  requirements only after the binding reviewed rebaseline barrier, substituting the exact future
  `A4_STANDARD_REFERENCE_HEAD` and the benchmark-only `sync_channel` standard reference.

  Execute five independent fixed-operation matrix repetitions plus five independent representative
  sustained/RSS repetitions at the exact clean `A4_BASELINE_CODE_HEAD`. The fixed matrix ends at
  its declared operation quota; only the representative sustained fixture runs the 110 seconds of
  warm/measured epochs. The integration owner pauses all other implementation/review agents, proves
  no other `cargo`, `rustc`, or benchmark process is running, holds the exclusive evidence lock for
  the complete run, and records hardware, power policy, and toolchain. Preserve output in the root
  worktree's ignored `target/` so later temporary-worktree cleanup cannot delete it.

  ```bash
  set -euo pipefail
  A4_BASELINE_CODE_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  RUN_DIR="$EVIDENCE_ROOT/standard-$A4_BASELINE_CODE_HEAD"
  HOST_EVIDENCE_DIR="$RUN_DIR/host-gate"
  umask 077
  mkdir -p "$EVIDENCE_ROOT"
  chmod 700 "$EVIDENCE_ROOT"
  mkdir "$RUN_DIR"
  chmod 700 "$RUN_DIR"
  scripts/capture_benchmark_prepare_build_evidence.py --run-dir "$RUN_DIR"
  test -s "$RUN_DIR/capture-bench-build.json"
  test -x "$RUN_DIR/capture_admission_evidence-exe"
  test -s "$RUN_DIR/build-evidence.json"

  # Pause every other agent and competing build/benchmark process before this attestation.
  printf '%s\n' no-other-active-agents > "$RUN_DIR/active-agent-attestation.txt"
  chmod 600 "$RUN_DIR/active-agent-attestation.txt"
  mkdir "$EVIDENCE_ROOT/.exclusive-lock"
  chmod 700 "$EVIDENCE_ROOT/.exclusive-lock"
  scripts/capture_benchmark_host_gate.sh measure \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
    --output-dir "$HOST_EVIDENCE_DIR" \
    --runner "$RUN_DIR/capture_admission_evidence-exe" \
    --build-evidence "$RUN_DIR/build-evidence.json"
  env CAPTURE_BENCH_BACKEND=standard \
    CAPTURE_BENCH_FINALIZE_ONLY=1 \
    CAPTURE_BENCH_BUILD_EVIDENCE="$RUN_DIR/build-evidence.json" \
    CAPTURE_BENCH_HOST_EVIDENCE="$HOST_EVIDENCE_DIR/comparison.json" \
    CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
    "$RUN_DIR/capture_admission_evidence-exe" --bench
  test -s "$RUN_DIR/manifest.json"
  jq -e '.backend == "standard" and
    .evidence_mode == "diagnostic_fixed_quota" and
    .criterion_evidence_mode == "exploratory_zero_authority" and
    .benchmark_support_feature == "capture-benchmark" and
    .fixtures == ["matrix", "comparable_full", "sustained_rss"] and
    .repetitions == [1, 2, 3, 4, 5] and
    .executable_path == "./capture_admission_evidence-exe" and
    (.immutable_module_sha256 | keys | sort) ==
      ["benchmark_identity", "collector", "endpoints", "evidence_io", "fixture",
       "producer_inventory", "schema", "workload"] and
    (.repetition_sha256 | keys | sort) ==
      ["repetition-1.json", "repetition-2.json", "repetition-3.json",
       "repetition-4.json", "repetition-5.json"] and
    .build_environment_policy == "sanitized-cargo-bench-v1" and
    (.build_command_sha256 | length == 64) and
    (.build_environment_sha256 | length == 64) and
    (.cargo_executable_sha256 | length == 64) and
    (.entrypoint_sha256 | type == "string" and length == 64) and
    (.backend_sha256 | type == "string" and length == 64) and
    (.host_fingerprint_sha256 | type == "string" and length == 64) and
    (.toolchain_fingerprint_sha256 | type == "string" and length == 64) and
    (.release_profile_sha256 | type == "string" and length == 64) and
    .host_gate.valid == true and
    (.host_gate.preflight_sha256 | length == 64) and
    (.host_gate.monitor_sha256 | length == 64) and
    (.host_gate.postflight_sha256 | length == 64) and
    (.host_gate.comparison_sha256 | length == 64)' \
    "$RUN_DIR/manifest.json"
  PREFLIGHT="$HOST_EVIDENCE_DIR/preflight.json"
  scripts/capture_benchmark_host_gate.sh release \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --release-ticket "$PREFLIGHT" \
    --expected-lock-device "$(jq -r '.lock_identity.device' "$PREFLIGHT")" \
    --expected-lock-inode "$(jq -r '.lock_identity.inode' "$PREFLIGHT")" \
    --expected-owner-device "$(jq -r '.owner_identity.device' "$PREFLIGHT")" \
    --expected-owner-inode "$(jq -r '.owner_identity.inode' "$PREFLIGHT")" \
    --expected-nonce-sha256 "$(jq -r '.lock_nonce_sha256' "$PREFLIGHT")"
  test ! -e "$EVIDENCE_ROOT/.exclusive-lock"
  test "$(git rev-parse HEAD)" = "$A4_BASELINE_CODE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

  Create `docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md` with the exact measured code
  SHA, clean proof, harness/fixture digests, producer-task derivation, hardware/OS/toolchain/timer,
  release profile, commands, requested/completed outcomes, every endpoint's p50/p95/p99/max,
  exact payload-aware sample quotas and their statistical rationale, throughput,
  configured/effective capacities, structural counters,
  all 1,000 measured RSS samples, raw artifact references under the persistent root evidence
  directory, every immutable-module/tool/artifact/repetition SHA, separate standard backend SHA,
  exact fixture/repetition manifest, and absence of any candidate threshold claim.

- [ ] **Step 3E: SUPERSEDED ring barrier; retain the report-only candidate barrier**

  The ring is already integrated before this step under the reviewed rebaseline. After the future
  standard-reference run, commit only the two exact report/lock paths named in the candidate-delta
  rule. That clean report-only descendant is the candidate evidence head; no implementation delta is
  allowed between the paired runs.

  ```bash
  set -euo pipefail
  A4_BASELINE_CODE_HEAD="$(git rev-parse HEAD)"
  git diff --check
  git diff --cached --check
  test "$(git status --short)" = \
    $'?? docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json\n?? docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md'
  git add \
    docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json \
    docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md
  git diff --cached --check
  git commit -m "docs: record q2 a4 standard channel baseline"
  A4_BASELINE_EVIDENCE_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  test "$(git diff --name-only "$A4_BASELINE_CODE_HEAD..$A4_BASELINE_EVIDENCE_HEAD")" = \
    $'docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json\ndocs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md'
  ```

  The report-inclusive evidence head is not relabeled as standard-reference measured code. The ring
  has already been installed. The candidate build changes only the closed compile-time backend
  selection; payload ownership, production sources, endpoints, collectors, producer inventory,
  operation quotas, benchmark fixture semantics, and every other frozen input remain byte-identical.

- [ ] **Step 4: RED final ring construction, publisher cloning, and rotation**

  Prove initial bundle overflow/underreport/invalid graph, fixed-plus-generation one-over, and
  allocation refusal return dedicated `CaptureChannelError` values, degrade the rejected bundle,
  and publish no handle. Prove a rejected fully prepared successor leaves predecessor identity,
  health, admission, and generation order unchanged.

  Add deterministic tests for fixed channel storage's four exact allocation terms, the frozen
  fallible fixed-component quote/reservation API used later by writer start, observed record/health
  backing capacity versus exact requested logical length, injected allocator spare capacity, queue
  allocation refusal, record/health exact-full, near-wrap position/sequence cycles, valid-operation
  slot-lock non-contention, impossible slot ownership, close, receiver drop, poison cleanup,
  sender-count `try_clone`, overflow, close races, and last-close; and
  exactly-once reservation release. Add the complete process-lifetime destination-registry tests,
  receiver-registration shutdown/check-before-sleep barrier, and bounded accounting-snapshot tests
  including fixed/resident/record transitions, multiple writers, ABA, epoch overflow, poison, and
  snapshot contention. Deliberately abandon an entered transition guard in deterministic and Loom
  fixtures and assert checked leave, durable first `InvariantViolated` poison, later-admission
  refusal, and the exact terminal snapshot error. Cover initial resident generation, successor overlap and
  failure, old snapshot/health/error/queued-record references retaining the predecessor charge,
  every identity-bearing error, exhaustive publish/generation/lifecycle health mapping, and
  reconciliation after every drop/drain path. Table-driven no-wildcard tests separately force an
  unexpected writer/storage exit, normal supervised writer stop, and sole supervisor exit/drop and
  assert exact `WriterFailed`, `WriterStopped`, and `SupervisorStopped` reasons plus terminal
  integrity effects. Add static assertions that `RawCapturePublisher` is `Send + Sync` and does not
  implement `Clone`.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/capture-ring
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-platform --lib --all-features --locked \
    'capture::queue' >"$RED_DIR/library.log" 2>&1; then
    printf '%s\n' 'expected capture-ring library RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/library.log" \
    'MSQ_A4_RED_CAPTURE_RING_AUTHORITY' \
    'error\[E(0061|0277|0308|0432|0599)\]' \
    'CaptureChannelLimits|CaptureFixedStorageReceipt|try_reserve_fixed_component|try_clone'
  if cargo test -p market-squawk-platform --test capture_lifecycle --all-features --locked \
    >"$RED_DIR/lifecycle.log" 2>&1; then
    printf '%s\n' 'expected capture lifecycle RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/lifecycle.log" \
    'MSQ_A4_RED_CAPTURE_LIFECYCLE_AUTHORITY' \
    'error\[E(0277|0308|0432|0599)\]' \
    'CaptureAccountingSnapshot|CaptureDestinationFenceEntry|InvariantViolated|SupervisorStopped'
  ```

  Expected RED: the old standard channel is infallible, publisher still implements `Clone`, fixed
  receipts do not exist, and required typed variants are absent.

- [ ] **Step 5: GREEN the final safe fixed ring without a public-cutover commit**

  This implementation step precedes the standard-reference freeze. It must not consume, generate, or
  claim a historical standard baseline; the binding reviewed rebaseline governs later evidence.

  Complete the already-started message/generation/publisher split in `capture/admission.rs` and put
  the fixed ring in `capture/queue.rs`. Replace the process-global destination `HashMap` in
  `capture/writer/destination.rs` with the frozen fallibly preallocated fixed registry. Implement the exact fixed formula,
  sequence-owned per-slot state, fallible slots, `CaptureMemoryAccounting`, resident generation token,
  `AccountedGenerationIdentity`, identity-bearing errors/snapshots/events/queued records, complete
  generation/publish health mappings, frame-footprint reservation ownership, typed
  `RawCapturePublisher::try_clone`, initial/generation errors, exact pre-consumption validation,
  fail-closed rejection, and successor preparation before predecessor revocation. The queue message
  owns the reservation and the writer lifecycle consumes it without exposing a manual release.
  This is the final queue/accounting/RAII design; MEM does not reimplement any authoritative total,
  resident token, identity wrapper, record reservation, or ring.

  Until MEM installs the allocation-identity-proven conversion, the seed's runnable compatibility
  converter is conservatively charged for a second payload allocation as well as the complete
  frame footprint and source allocation. It may overcount but cannot undercount. The frozen
  `RecordReservationQuote` distinguishes `compatibility_copy_allocation` from the final shared
  payload term; MEM can set the former to zero only after its pointer proof passes. This temporary
  safe compatibility term is tested and is never used in the final candidate formula.

  Create one shared `scripts/run_exact_loom_gate.sh` used by both authority and queue wrappers. It
  rejects ambient `LOOM_*`, `RUSTFLAGS`, encoded/build/target Rust flags, target/profile overrides,
  `RUSTC`, and compiler wrappers by environment name before Cargo runs; derives exactly one pinned
  host target; then owns exactly `RUSTFLAGS=--cfg loom` and `CARGO_INCREMENTAL=0`. Listing, Clippy,
  and execution use the same package, library target, all features, release profile, lockfile, and
  target. The runner lists the complete reserved `::loom_model` namespace, byte-compares its sorted
  nonempty unique inventory with the wrapper's closed full-path list, and invokes each full path
  separately with `--exact --test-threads=1`. Missing, renamed, duplicate, extra, or zero models
  fail. Every model explicitly sets every `loom::model::Builder` environment/default-sensitive
  field (`max_threads`, `max_branches`, `max_permutations`, `max_duration`, `preemption_bound`,
  `checkpoint_file`, `checkpoint_interval`, `expect_explicit_explore`, `location`, and `log`). A
  network-free fake-Cargo policy test covers exact success and every inventory/list/execution/
  forbidden-environment failure and is wired into `scripts/verify.sh`.

  Create `scripts/check_capture_queue_loom.sh` with that exact runner and separately named models for
  clone/drop/overflow/poison/last-close, explicit live-transition-guard abandonment and checked
  Drop fallback, coherent-snapshot ABA, shutdown-before-wait, and every send/close/drain race in the
  frozen contract. Add that script to `scripts/verify.sh`; failure must propagate nonzero. Run:

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-platform --lib --all-features --locked 'capture::queue'
  ./scripts/check_capture_queue_loom.sh
  bash -n scripts/check_capture_queue_loom.sh scripts/verify.sh
  git diff --check
  git diff --cached --check
  ```

  Review the ring/accounting/admission/destination/script diff, but do not commit the public
  `Result`, limits, or no-`Clone` cutover while 39 channel callers, ten clone callers, three
  Clone expectations, configuration, and sink callers still use the old API. Continue directly to
  Steps 6 and 7 in this same intentional dirty TDD worktree. Platform integration and app targets
  remain deliberately RED after the breaking boundary is introduced; Step 5 claims GREEN only for
  the private queue/accounting library model and Loom. The complete integration graph becomes GREEN
  only after the one atomic Step 7 migration.

- [ ] **Step 6: RED configuration, sink-construction, and call migration**

  Test exact configuration precedence and rejection for all three new file/env/CLI fields. Assert every
  old `journal_queue_capacity`, `MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY`, and legacy CLI spelling is
  rejected at v0.1. Add failing memory-sink exact/one-over count/bytes, allocation failure,
  no-growth, and removed-`Default` tests. Change the publisher clone assertions/calls to require
  `try_clone`, then run the platform/app compile tests and record the expected failures before
  implementation.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/public-cutover
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-platform --test config_precedence --all-features --locked \
    >"$RED_DIR/config.log" 2>&1; then
    printf '%s\n' 'expected configuration-cutover RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/config.log" \
    'MSQ_A4_RED_CAPTURE_CONFIG_CUTOVER' \
    'error\[E(0061|0560|0599|0609)\]' \
    'capture_queue_capacity|capture_memory_ceiling_bytes|capture_destination_registry_memory_ceiling_bytes'
  if cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_sink >"$RED_DIR/sink.log" 2>&1; then
    printf '%s\n' 'expected sink-construction RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/sink.log" \
    'MSQ_A4_RED_MEMORY_SINK_CONSTRUCTION' \
    'error\[E(0061|0277|0599)\]' \
    'MemoryCaptureSink::try_new|MemoryCaptureSinkConstructionError|FixedStorageBudgetExceeded'
  if cargo test -p market-squawk --all-targets --all-features --locked \
    >"$RED_DIR/application.log" 2>&1; then
    printf '%s\n' 'expected application-cutover RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/application.log" \
    'MSQ_A4_RED_APPLICATION_CAPTURE_CUTOVER' \
    'error\[E(0061|0277|0308|0432|0599)\]' \
    'CaptureProcessInfrastructure|CaptureChannelLimits|RawCapturePublisher::try_clone'
  ```

  Expected RED: new config fields/typed sink constructor/clone errors are absent and old calls no
  longer satisfy the intended compile-negative contracts.

- [ ] **Step 7: GREEN all shared configuration and call migrations**

  Implement `CaptureChannelLimits`, `CaptureProcessInfrastructureLimits`, and the one-time process
  initialization proof; rename/plumb queue, channel-memory, and destination-registry-memory
  configuration through the entire precedence/composition chain; freeze
  `MemoryCaptureSink::try_new(NonZeroUsize, NonZeroUsize)`, remove `Default`, and migrate every
  inventoried call with typed propagation.

  Migrate all 39 channel calls:

  | File | Calls |
  | --- | ---: |
  | `apps/market-squawk/src/main.rs` | 2 |
  | `apps/market-squawk/tests/coinbase_source.rs` | 2 |
  | `apps/market-squawk/tests/source_supervisor.rs` | 2 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge.rs` | 3 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge/cases.rs` | 5 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge/writer_cases.rs` | 7 |
  | `crates/market-squawk-platform/tests/capture_lifecycle.rs` | 11 |
  | `crates/market-squawk-platform/tests/capture_lifecycle/deadline_cases.rs` | 6 |
  | `crates/market-squawk-sources/tests/capture_bridge.rs` | 1 |

  The inventory contains nine files, not “the remaining eight files.” Every call passes the explicit
  initialized `CaptureProcessInfrastructure` proof plus `CaptureChannelLimits` and propagates
  `Result` with `?`/typed mapping. No `unwrap`, `expect`, hidden constant, lazy default, infallible
  adapter, or overload remains.

  Migrate all 18 `MemoryCaptureSink::default()` calls:

  | File | Calls |
  | --- | ---: |
  | `apps/market-squawk/src/main.rs` | 1 |
  | `apps/market-squawk/tests/source_supervisor.rs` | 2 |
  | `apps/market-squawk/tests/coinbase_source.rs` | 1 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge.rs` | 2 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge/cases.rs` | 5 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge/writer_cases.rs` | 3 |
  | `crates/market-squawk-platform/tests/capture_lifecycle.rs` | 3 |
  | `crates/market-squawk-sources/tests/capture_bridge.rs` | 1 |

  Migrate all ten publisher clones to typed `try_clone` propagation:

  | File | Calls |
  | --- | ---: |
  | `apps/market-squawk/src/main.rs` | 1 |
  | `apps/market-squawk/src/source_supervisor.rs` | 1 |
  | `apps/market-squawk/tests/source_supervisor.rs` | 6 |
  | `crates/market-squawk-platform/tests/capture_authority_bridge/cases.rs` | 2 |

  Remove `#[derive(Clone)]` from `CaptureContext` in `apps/market-squawk/src/source/mod.rs`,
  remove/replace the two positive `Clone` assertions in `capture_authority_bridge.rs` and
  `capture_lifecycle.rs`, and restructure the inner sender clone in `capture.rs` channel
  construction so the initial authoritative sender count matches actual handles. `SourceSupervisor`
  construction/run becomes fallible where it needs another producer; no `.ok()`, default, or silent
  drop may hide a clone failure.

  Create `scripts/classify_hosted_run.sh` as a bounded, read-only GitHub evidence classifier with
  explicit `--repo`, `--sha`, `--workflow`, `--poll-attempts`, `--poll-interval-seconds`, and
  `--output-dir` arguments. It atomically writes the observed class plus raw run/jobs JSON to the
  controlled ignored output directory and prints exactly that class to stdout after publication.
  It validates a full lowercase 40-hex SHA, positive attempts, nonnegative interval, and an output
  directory under the common repository root's controlled `target/q2-a4-hosted/` subtree. It
  rejects symlinks at every existing path component and canonical escape/traversal, caps total poll
  wait, API attempts, pages per endpoint, runs/jobs/steps per page and in aggregate, response JSON
  bytes, diagnostic text, and the exact number and size of output files. Boundary tests cover every
  exact cap and one-over case plus traversal, an intermediate symlink, a final symlink, and an
  output root outside the controlled subtree. A job
  counts as assigned/started only when `runner_id != 0`,
  `runner_name` is nonempty, or at least one step has a non-null `started_at`, status
  `in_progress`/`completed`, or non-null conclusion; a mere step skeleton is not execution.
  Deterministic fake-`gh` tests cover all four classes, delayed run appearance, incomplete jobs,
  pagination, exact SHA filtering, malformed JSON, and nonzero API failure. Add them to
  `scripts/verify.sh`.

  ```bash
  set -euo pipefail
  LEGACY_CONFIG_NAMES="$(mktemp)"
  trap 'rm -f "$LEGACY_CONFIG_NAMES"' EXIT
  set +e
  rg -n 'journal_queue_capacity|MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY|--journal-queue-capacity' \
    apps crates --glob '*.rs' >"$LEGACY_CONFIG_NAMES"
  LEGACY_CONFIG_STATUS=$?
  set -e
  test "$LEGACY_CONFIG_STATUS" -eq 1
  test ! -s "$LEGACY_CONFIG_NAMES"
  rg -n 'capture_queue_capacity|MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY|--capture-queue-capacity' \
    apps crates --glob '*.rs'
  test "$(rg -n 'raw_capture_channel\(' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 39
  test "$(rg -n 'MemoryCaptureSink::default\(\)' apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 0
  test "$(rg -n '\b[a-zA-Z_]*publisher\.clone\(\)|\.publisher\.clone\(\)' \
    apps crates --glob '*.rs' | wc -l | tr -d ' ')" -eq 0
  cargo fmt --all --check
  cargo test -p market-squawk-platform --all-targets --all-features --locked
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk --all-targets --all-features --locked
  ./scripts/tests/test_assert_expected_red.sh
  rg -F './scripts/tests/test_assert_expected_red.sh' scripts/verify.sh
  python3 -m unittest scripts.tests.test_capture_benchmark_host_gate \
    scripts.tests.test_classify_hosted_run
  bash -n scripts/assert_expected_red.sh scripts/tests/test_assert_expected_red.sh \
    scripts/capture_benchmark_host_gate.sh scripts/classify_hosted_run.sh \
    scripts/check_capture_queue_loom.sh scripts/verify.sh
  git diff --check
  git diff --cached --check
  ```

  Run `./scripts/verify.sh`, review the complete ring/accounting/destination/configuration/app/sink/
  call-site/script cutover as one atomic workspace-green diff, stage that exact set, and commit with
  message `feat(capture): install bounded capture authority`. Record `A4_SEED_HEAD` and prove clean
  status. No commit between `A4_BASELINE_EVIDENCE_HEAD` and this atomic cutover may expose a broken
  public signature or unmigrated caller.

- [ ] **Step 8: Run the complete seed candidate gate**

  Before approval, a scoped production-source gate excludes the intentionally preserved benchmark
  standard backend and proves that `crates/market-squawk-platform/src/capture.rs` plus
  `src/capture/**/*.rs` contain no standard `sync_channel`/`SyncSender`/`mpsc::Receiver` and no
  `CaptureMessage::Wake`. Pair those negative checks with positive `QueueCore`, `QueueSlot<T>`,
  `Vec<QueueSlot<T>>`, atomic enqueue/dequeue positions, sender count, one combined atomic lifecycle
  word with receiver-closed-bit and active-operation-count masks, and per-slot
  `ready: AtomicBool` plus `Mutex<Option<T>>` evidence in
  `capture/queue.rs`. The gate distinguishes ripgrep no-match status 1
  from search failure and never masks failures with `|| true` or command substitution. Separately,
  the standard benchmark backend must retain its explicit benchmark-only `sync_channel`
  implementation. Its digest is persisted only at the future reviewed standard-reference freeze.

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-domain --all-targets --all-features --locked
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-platform --all-targets --all-features --locked
  cargo test -p market-squawk --all-targets --all-features --locked
  cargo clippy -p market-squawk-domain -p market-squawk-sources \
    -p market-squawk-platform -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-domain -p market-squawk-sources \
    -p market-squawk-platform -p market-squawk --all-features --release --locked
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run
  ./scripts/check_capture_queue_loom.sh
  ./scripts/tests/test_assert_expected_red.sh
  rg -F './scripts/tests/test_assert_expected_red.sh' scripts/verify.sh
  bash -n scripts/assert_expected_red.sh scripts/tests/test_assert_expected_red.sh \
    scripts/capture_benchmark_host_gate.sh scripts/classify_hosted_run.sh \
    scripts/check_capture_queue_loom.sh scripts/verify.sh
  git diff --check
  git diff --cached --check
  git status --short
  ```

  Expected: green candidate evidence. If any intended seed change remains, review the exact owned
  diff, commit it intentionally, and rerun this whole step; do not freeze a dirty seed.

- [ ] **Step 9: Run the unchanged clean seed gate and fork both lanes**

  ```bash
  set -euo pipefail
  A4_SEED_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ./scripts/verify.sh
  ./scripts/tests/test_assert_expected_red.sh
  rg -F './scripts/tests/test_assert_expected_red.sh' scripts/verify.sh
  bash -n scripts/assert_expected_red.sh scripts/tests/test_assert_expected_red.sh \
    scripts/capture_benchmark_host_gate.sh scripts/classify_hosted_run.sh \
    scripts/check_capture_queue_loom.sh scripts/verify.sh
  ./scripts/check_capture_queue_loom.sh
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run
  test "$(git rev-parse HEAD)" = "$A4_SEED_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push origin feat/q2-a4-seed
  test "$(git rev-parse HEAD)" = "$A4_SEED_HEAD"
  ```

  Create both Wave 2 worktrees from exactly `A4_SEED_HEAD`:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  A4_SEED_HEAD="$(git rev-parse HEAD)"
  git worktree add "$REPO_ROOT/.worktrees/q2-a4-time" \
    -b feat/q2-a4-trusted-time "$A4_SEED_HEAD"
  git worktree add "$REPO_ROOT/.worktrees/q2-a4-memory" \
    -b feat/q2-a4-capture-memory "$A4_SEED_HEAD"
  test "$(git -C "$REPO_ROOT/.worktrees/q2-a4-time" rev-parse HEAD)" = "$A4_SEED_HEAD"
  test "$(git -C "$REPO_ROOT/.worktrees/q2-a4-memory" rev-parse HEAD)" = "$A4_SEED_HEAD"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT/.worktrees/q2-a4-time" status --short)"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT/.worktrees/q2-a4-memory" status --short)"
  test -z "$EMPTY_OUTPUT"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 seed ${A4_SEED_HEAD} passed its exact-head gate. Grouped worktrees active: seed integration owner, TIME lane, and MEM lane; both lanes start at the same seed SHA.")"
  test -n "$COMMENT_URL"
  ```

  The original Wave 0 documentation worktree is now clean and handed off. The integration owner
  first releases its agent, records that disposition, verifies no process has the worktree open,
  removes it normally, and prunes. Never force-remove it:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  WAVE0_WORKTREE="$REPO_ROOT/.worktrees/q2-a4-wave0"
  test "$(pwd -P)" = "$REPO_ROOT/.worktrees/q2-a4-seed"
  EMPTY_OUTPUT="$(git -C "$WAVE0_WORKTREE" status --short)"
  test -z "$EMPTY_OUTPUT"
  RELEASE_EVIDENCE="$REPO_ROOT/target/q2-a4-worktree-release/wave0.txt"
  mkdir -p "$(dirname "$RELEASE_EVIDENCE")"
  printf '%s\n' no-active-agent-or-process > "$RELEASE_EVIDENCE"
  command -v pgrep >/dev/null
  set +e
  PROCESS_OUTPUT="$(pgrep -f "$WAVE0_WORKTREE" 2>"${RELEASE_EVIDENCE}.pgrep.err")"
  PROCESS_STATUS=$?
  set -e
  test "$PROCESS_STATUS" -eq 0 || test "$PROCESS_STATUS" -eq 1
  test ! -s "${RELEASE_EVIDENCE}.pgrep.err"
  test -z "$PROCESS_OUTPUT"
  if command -v lsof >/dev/null 2>&1; then
    set +e
    LSOF_OUTPUT="$(lsof -t +D "$WAVE0_WORKTREE" 2>"${RELEASE_EVIDENCE}.lsof.err")"
    LSOF_STATUS=$?
    set -e
    test "$LSOF_STATUS" -eq 0 || test "$LSOF_STATUS" -eq 1
    test ! -s "${RELEASE_EVIDENCE}.lsof.err"
    test -z "$LSOF_OUTPUT"
  fi
  git worktree remove "$WAVE0_WORKTREE"
  git worktree prune
  test ! -d "$WAVE0_WORKTREE"
  ```

## Wave 2A: TIME from the frozen seed

### Task 2: Make receipt time source-owned and continuity-sealed

**Files:**

- Create: `crates/market-squawk-sources/src/authority_time.rs`
- Modify: `crates/market-squawk-sources/src/lib.rs`
- Modify: `crates/market-squawk-sources/src/live.rs`
- Modify: `crates/market-squawk-sources/src/capture.rs`
- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/**`
- Modify: `crates/market-squawk-sources/tests/**`
- Modify: `crates/market-squawk-live/src/**` test/fixture call sites only
- Modify: `crates/market-squawk-live/tests/**`
- Create: `crates/market-squawk-sources/src/registry/tests/time_cases.rs`
- Create: `crates/market-squawk-sources/src/registry/catalog/session.rs`

**Interfaces:**

- Consumes: exact `A4_SEED_HEAD`
- Produces: one paired sealed clock, opaque trusted receipt, two-argument frame factory, one terminal
  continuity latch, continuity-bound capabilities, and the exact added continuity charge

- [ ] **Step 1: Write TIME RED tests**

  Cover paired wall/monotonic rollback, source failure, cursor poison, equal wall with advancing
  monotonic, permanent latch, torn-observation barrier, wrong/missing continuity, retained
  capabilities rejecting after one latch, same-registry replacement rejection, fresh ephemeral
  success, durable `InUse` restart rejection, and the new continuity graph mismatch/charge.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/trusted-time
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-sources --lib --all-features --locked \
    'registry::tests::time_cases' >"$RED_DIR/library.log" 2>&1; then
    printf '%s\n' 'expected trusted-time library RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/library.log" \
    'MSQ_A4_RED_TRUSTED_TIME_LIBRARY' \
    'error\[E(0061|0412|0422|0432|0599)\]' \
    'TrustedReceiptObservation|AuthorityTimeContinuity|SealedRegistryClock'
  if cargo test -p market-squawk-sources --test registry_authority \
    --all-features --locked >"$RED_DIR/integration.log" 2>&1; then
    printf '%s\n' 'expected trusted-time integration RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/integration.log" \
    'MSQ_A4_RED_TRUSTED_TIME_INTEGRATION' \
    'error\[E(0061|0308|0412|0432|0599)\]' \
    'TrustedReceipt|AuthorityTimeContinuity|try_frame'
  ```

  Expected RED: paired sealed time/continuity types and the two-argument factory do not exist. Save
  exact failing test names; do not weaken fixtures merely to obtain a failure.

- [ ] **Step 2: Implement paired sealed time behind the unchanged public boundary**

  Create private `RawRegistryClockSource`, `RegistryMonotonicInstant`, `TrustedRegistryTime`,
  `AuthorityTimeContinuity`, `AuthorityTimeContinuityState`, `SealedRegistryClock`, and
  `TrustedReceiptObservation`. Compare/update the pair under one short mutex. Latch continuity and
  A3 authority immediately; terminal persistence runs only later through A3's central writer.

  Add the private clock/continuity core and its focused tests, but keep the existing public frame
  factory signature, frame representation, binding graph, and accounting formulas unchanged while
  this worktree remains intentionally dirty. Do not commit a private unused scaffold or a
  trait/signature change that leaves the 23 callers or retained formula behind. The atomic public
  cutover happens in Step 3.

  The final frame-factory interface, installed only as part of that atomic cutover, is:

  ```text
  RawFrameFactory::try_frame(
      &mut self,
      transport: TransportFrameKind,
      payload: Bytes,
  ) -> Result<RawMarketFrame, SourceError>
  ```

  The final factory normalizes through the checked `CapturePayload` boundary, samples/seals receipt
  time, consumes a never-reused ordinal, and embeds opaque continuity. First make the private
  focused time tests green without changing the public factory, then continue directly to Step 3
  with the worktree dirty; there is no `TIME_CORE_HEAD` or intermediate commit.

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-sources --lib --all-features --locked \
    'registry::tests::time_cases'
  cargo test -p market-squawk-sources --test registry_authority \
    --all-features --locked
  git diff --check
  git diff --cached --check
  ```

- [ ] **Step 3: Atomically cut over the factory, all callers, authority binding, and formula**

  Require exact binding/continuity allocations, live latch, receipt at/after session start, and no
  receipt beyond the paired high-water in session, decoder, capture, current/queued batch, health,
  live scope, and registry-scoped budget validation. Use registry monotonic deadlines. Add the one
  continuity Arc allocation to the exhaustive bundle formula and to
  `checked_resident_shared_frame_bytes`; prove both binding and continuity pointer equality and
  reject every mismatched pointer.
  Migrate exactly 23 audited calls across these 12 files without a timestamp overload:

  | File | Calls |
  | --- | ---: |
  | `crates/market-squawk-live/src/qualification/tests.rs` | 1 |
  | `crates/market-squawk-live/tests/support/current_source.rs` | 2 |
  | `crates/market-squawk-live/tests/processor/tests/fixture.rs` | 1 |
  | `crates/market-squawk-live/tests/processor/snapshot/tests/fixture.rs` | 1 |
  | `crates/market-squawk-sources/tests/contracts.rs` | 4 |
  | `crates/market-squawk-sources/tests/authority_persistence.rs` | 1 |
  | `crates/market-squawk-sources/tests/capture_bridge.rs` | 1 |
  | `crates/market-squawk-sources/tests/registry_authority.rs` | 4 |
  | `crates/market-squawk-sources/src/registry/tests.rs` | 1 |
  | `crates/market-squawk-sources/tests/registry_authority/pre_feed_cases.rs` | 2 |
  | `crates/market-squawk-sources/src/registry/tests/temporal_cases.rs` | 2 |
  | `crates/market-squawk-sources/tests/registry_authority/current_scope_cases.rs` | 3 |

  ```bash
  set -euo pipefail
  test "$(rg -n '\.try_frame\(' crates/market-squawk-sources \
    crates/market-squawk-live --glob '*.rs' | wc -l | tr -d ' ')" -eq 23
  cargo fmt --all --check
  cargo test -p market-squawk-sources --lib --all-features --locked \
    'registry::tests::time_cases' -- --list | rg 'time_cases'
  cargo test -p market-squawk-sources --test registry_authority \
    --all-features --locked -- --list | rg 'authority|time|continuity'
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-live --all-targets --all-features --locked
  cargo clippy -p market-squawk-sources -p market-squawk-live \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-sources -p market-squawk-live \
    --all-features --release --locked
  ./scripts/check_authority_lifecycle_loom.sh
  git diff --check
  git diff --cached --check
  ```

  Review the one atomic diff containing the private core, public two-argument factory, all 23 caller
  migrations, continuity embedding, pointer-equality validation, and exact accounting-formula
  update. Commit only that complete TIME-owned cutover as
  `feat(sources): bind live authority to trusted time`. Record `TIME_HEAD`; prove clean. A commit
  containing only the trait/signature, only the callers, or only the formula is forbidden.

- [ ] **Step 4: Audit the clean TIME cutover and commit any complete remediation atomically**

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-live --all-targets --all-features --locked
  cargo clippy -p market-squawk-sources -p market-squawk-live \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-sources -p market-squawk-live \
    --all-features --release --locked
  ./scripts/check_authority_lifecycle_loom.sh
  git diff --check
  git diff --cached --check
  git status --short
  ```

  Expected: clean status at `TIME_HEAD`. If review finds a defect, implement the complete
  cross-file remediation, rerun every gate above, commit it intentionally, and replace `TIME_HEAD`;
  do not hand off dirty or partially migrated state.

- [ ] **Step 5: Rerun at the unchanged clean TIME head**

  ```bash
  set -euo pipefail
  TIME_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-live --all-targets --all-features --locked
  cargo clippy -p market-squawk-sources -p market-squawk-live \
    --all-targets --all-features --locked -- -D warnings
  ./scripts/check_authority_lifecycle_loom.sh
  test "$(git rev-parse HEAD)" = "$TIME_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push -u origin feat/q2-a4-trusted-time
  test "$(git rev-parse HEAD)" = "$TIME_HEAD"
  ```

  Comment on canonical PR 1 explicitly. This is a lane handoff, not the Quarter 1 of 4 review:

  ```bash
  set -euo pipefail
  TIME_HEAD="$(git rev-parse HEAD)"
  A4_SEED_HEAD="$(git merge-base "$TIME_HEAD" feat/q2-a4-seed)"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 TIME lane ${TIME_HEAD} handed off from seed ${A4_SEED_HEAD}; owned source/live authority-time files only; sources/live tests, clippy, release build, and authority Loom passed.")"
  test -n "$COMMENT_URL"
  ```

## Wave 2B: MEM from the same frozen seed

### Task 3: Implement the complete bounded capture graph

**Files:**

- Modify: `crates/market-squawk-platform/src/capture/writer.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer/lifecycle.rs`
- Modify: `crates/market-squawk-platform/src/capture/writer/sink.rs`
- Modify: `crates/market-squawk-platform/src/raw_record.rs`
- Modify: `crates/market-squawk-platform/src/journal.rs`
- Modify: `crates/market-squawk-platform/src/paths.rs`
- Modify: `crates/market-squawk-platform/tests/capture_authority_bridge.rs`
- Modify: `crates/market-squawk-platform/tests/capture_authority_bridge/**`
- Modify: `crates/market-squawk-platform/tests/capture_lifecycle.rs`
- Modify: `crates/market-squawk-platform/tests/capture_lifecycle/**`
- Modify: `apps/market-squawk/src/main.rs` `#[cfg(test)]` module only
- Modify: `crates/market-squawk-platform/benches/capture_admission/backend.rs`
- Create: `docs/reports/performance/2026-07-17-q2-a4-writer-runtime-proof.md`

**Interfaces:**

- Consumes: exact `A4_SEED_HEAD`
- Produces: writer-start fixed reservation/scratch behind the seed's frozen accounting APIs;
  lexical reservation consumption with allocation-identity proof; single-copy conversion;
  streaming journal with a separate bounded sink ledger; memory-sink dynamic-ledger hardening behind
  the seeded constructor; writer record-lifecycle Loom/integration coverage; and acceptance
  benchmarks

- [ ] **Step 1: RED the complete writer-start fixed receipt**

  Treat the seed's accounting/identity/ring tests as immutable prerequisites. Add failing tests for
  every `WriterFixedStorageReceipt` term independently: observed UUID/source/event scratch,
  per-writer destination lease/identity, bounded thread name, pinned spawn packet/closure/
  `Thread`/`JoinHandle` control upper bound, any other stable allocation, compiled target, and proof
  artifact hash. Cover exact/one-over reservation, scratch allocation refusal, proof mismatch,
  destination refusal, thread-name/creation failure, start-vs-shutdown,
  failed start releasing only its writer reservation, successful start keeping it until final writer
  runtime drop, no healthy transition before reservation/allocation, and no second writer scratch.
  Extend the existing Loom invocation only with writer ownership-transfer/drop behavior; do not edit
  queue authority or accounting types.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/writer-start
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_writer_start >"$RED_DIR/writer.log" 2>&1; then
    printf '%s\n' 'expected writer-start receipt RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/writer.log" \
    'MSQ_A4_RED_WRITER_FIXED_STORAGE' \
    'error\[E(0061|0412|0422|0432|0599)\]' \
    'WriterFixedStorageReceipt|WriterRuntimeProofError|CaptureDestinationFenceError'
  if ./scripts/check_capture_queue_loom.sh >"$RED_DIR/loom.log" 2>&1; then
    printf '%s\n' 'expected writer-ownership Loom RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/loom.log" \
    'MSQ_A4_RED_WRITER_OWNERSHIP_LOOM' \
    'error\[E(0061|0412|0432|0599)\]' \
    'WriterFixedStorageReceipt|WriterRuntimeProofError|CaptureDestinationFenceError'
  ```

  Expected RED: the complete receipt/proof contract is absent; a scratch-only quote cannot satisfy
  reconstruction or lifetime tests.

- [ ] **Step 2: GREEN and commit writer-runtime fixed storage**

  Use the seed's frozen `try_reserve_fixed_component`/RAII APIs. Create
  `docs/reports/performance/2026-07-17-q2-a4-writer-runtime-proof.md` with pinned Rust source revision,
  compiled target, current-target type sizes, closure-capture inventory, exact source-derived upper
  bound, formula revision, and fixture hash. Embed and verify its SHA-256/formula revision in the
  receipt. Prepare scratch with `try_reserve_exact`, read every observed capacity, acquire the
  separately accounted process-registry lease, validate the bounded thread name and compiled-target
  proof, construct one complete `WriterFixedStorageReceipt`, and reserve its checked sum once before
  thread creation or health publication. Stable-Rust allocations without fallible APIs remain honest
  process-OOM boundaries; do not invent an allocation error. Retain the receipt across worker,
  ordinary handle, pending reap, final report, join, and destination-fence teardown. Every
  pre-publication failure releases it. No change to resident identity, record reservation, sender
  state, ring storage, snapshot protocol, or authoritative total arithmetic is permitted.

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  set -o pipefail
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_writer_start -- --list | rg 'capture_writer_start.*: test$'
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_writer_start
  ./scripts/check_capture_queue_loom.sh
  git diff --check
  git diff --cached --check
  ```

  Review/commit only writer/runtime tests and implementation as
  `feat(capture): reserve writer fixed storage`. Record `MEM_WRITER_HEAD`; prove clean.

- [ ] **Step 3A: Write and run RED conversion-reservation tests**

  Treat seed footprint construction, admission limits, accounting snapshots, queue failures, and
  reserve/release lifecycle as immutable green prerequisites. Add focused RED tests only for the
  remaining MEM refinement: the seed's explicitly charged `compatibility_copy_allocation` is still
  nonzero; frame/record empty and nonempty payload allocation identity is not yet enforced; a
  borrowed-view mismatch or deliberately copying converter must fail; the maximum source `Arc<str>`
  conversion term is exact; and the already-owned reservation must remain lexically live through
  conversion, `append`, and record-triggered `flush` before its existing RAII release paths run.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/capture-conversion
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_conversion >"$RED_DIR/bridge.log" 2>&1; then
    printf '%s\n' 'expected capture-conversion bridge RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/bridge.log" \
    'MSQ_A4_RED_CAPTURE_CONVERSION_IDENTITY' \
    'error\[E(0308|0412|0432|0599)\]' \
    'InvalidPayloadSharing|compatibility_copy_allocation|shares_allocation_with'
  if cargo test -p market-squawk-platform --test capture_lifecycle --all-features --locked \
    >"$RED_DIR/lifecycle.log" 2>&1; then
    printf '%s\n' 'expected capture-conversion lifecycle RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/lifecycle.log" \
    'MSQ_A4_RED_CAPTURE_RESERVATION_LIFETIME' \
    'error\[E(0308|0412|0432|0599)\]' \
    'QueueByteReservation|compatibility_copy_allocation|shares_allocation_with'
  ```

  Expected RED: the conservative compatibility copy remains and conversion allocation identity/
  lexical append-plus-flush lifetime is not yet proven. Seed-owned admission/accounting tests remain
  green and are never represented as MEM RED evidence.

- [ ] **Step 3B: Implement exact zero-copy conversion**

  Consume the seed-owned reservation intact in `append_frame`; bind it until
  authority/deadline checks, conversion identity proof, append, and record-triggered flush finish.
  Replace the compatibility-copy quote with the final zero-copy quote only after cloning the exact
  `capture_payload` and proving allocation identity. The queue/send/drain ownership remains seed
  code and is not edited by MEM.

- [ ] **Step 3C: Run GREEN conversion gates and commit**

  ```bash
  set -euo pipefail
  set -o pipefail
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_conversion -- --list | rg 'capture_conversion'
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_conversion
  cargo test -p market-squawk-platform --test capture_lifecycle \
    --all-features --locked
  ./scripts/check_capture_queue_loom.sh
  cargo fmt --all --check
  git diff --check
  git diff --cached --check
  ```

  Review/commit the green conversion/reservation change as
  `feat(capture): reserve exact frame conversion graph`. Record `MEM_RESERVATION_HEAD`; prove clean.

- [ ] **Step 4A: Write and run RED journal and remaining sink-refinement tests**

  First add failing tests for journal fixed exact/one-under budget, observed buffer/path capacity,
  direct streaming, length/CRC equality, truncated second pass, serialization/write/flush errors,
  33,554,431-byte historical compatibility, 4 MiB live rejection, and no retained record after
  append. The seed already owns `MemoryCaptureSink::try_new`, explicit count/byte limits, fallible
  preallocation, no-growth, and basic fixed/dynamic accounting. Keep those tests green. Add RED only
  for the remaining sink refinements: the exact conservative dynamic quote for its retained graph,
  repeated-Arc charging policy, allocation-identity mismatch rejection, retained-clone lifetime,
  arithmetic boundaries not already frozen by seed, and final-token release.

  ```bash
  set -euo pipefail
  RED_DIR=target/q2-a4-red/journal-sink
  rm -rf "$RED_DIR"
  mkdir -p "$RED_DIR"
  if cargo test -p market-squawk-platform --test journal_compatibility \
    --all-features --locked >"$RED_DIR/journal.log" 2>&1; then
    printf '%s\n' 'expected bounded-journal RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/journal.log" \
    'MSQ_A4_RED_BOUNDED_JOURNAL' \
    'error\[E(0061|0412|0422|0432|0599)\]' \
    'JournalWriter|JournalSinkLimits|JournalSinkConstructionError|SerializationLimitExceeded'
  if cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_sink >"$RED_DIR/sink.log" 2>&1; then
    printf '%s\n' 'expected retained-sink refinement RED but target passed' >&2
    exit 1
  fi
  ./scripts/assert_expected_red.sh \
    "$RED_DIR/sink.log" \
    'MSQ_A4_RED_RETAINED_SINK_REFINEMENT' \
    'error\[E(0308|0412|0432|0599)\]' \
    'RetainedByteLimitExceeded|InvalidPayloadSharing|shares_allocation_with'
  ```

  Expected RED: bounded journal construction/streaming and only the named dynamic sink refinements
  do not exist or fail their exact-limit assertions. Seed-owned constructor, fixed preallocation,
  count limit, no-growth, and basic ledger tests remain green and are not claimed as MEM RED.

- [ ] **Step 4B: Implement separate bounded sink ledgers**

  Use fixed UUID scratch, clone the frame's `CapturePayload`, implement the two-pass
  counting/CRC serializer, construct the explicitly bounded `BufWriter`, and harden the seed's
  fallibly preallocated never-growing memory sink. Preserve every distinct typed construction,
  limit, serialization, write, flush, shutdown, and accounting error plus existing current/legacy
  committed-journal recovery semantics.

- [ ] **Step 4C: Run GREEN journal/sink gates and commit**

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  set -o pipefail
  cargo test -p market-squawk-platform --test journal_compatibility \
    --all-features --locked -- --list | rg 'journal'
  cargo test -p market-squawk-platform --test journal_compatibility \
    --all-features --locked
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_sink -- --list | rg 'capture_sink'
  cargo test -p market-squawk-platform --test capture_authority_bridge \
    --all-features --locked capture_sink
  cargo test -p market-squawk --all-targets --all-features --locked
  git diff --check
  git diff --cached --check
  ```

  Review/commit writer/journal/sink work as
  `feat(capture): bound journal and retained sinks`. Record `MEM_SINK_HEAD`; prove clean.

- [ ] **Step 5: Validate both closed backends before the future reference freeze**

  Do not redesign, expand, or relabel the closed harness during MEM. The dispatcher already selects
  either the benchmark-only standard `sync_channel` source or production
  `RawCapturePublisher::try_clone`; validate both feature-gated builds, but perform no authoritative
  measurement from the dirty lane. The five connected endpoint modules become immutable at the
  future `A4_STANDARD_REFERENCE_HEAD`, and production code is observed only through those closed
  boundaries. Keep candidate-only forced-lock instrumentation inside the separately hashed candidate
  adapter, using real `CaptureMessage` accounting or an explicit typed-unavailable result. The
  standard and candidate result groups remain separately named; no artifact may be presented as a
  historical pre-replacement baseline. Payload ownership, fixtures, operation quotas, endpoint
  boundaries, collector, producer inventory, result schema, entrypoint, Criterion target, observer,
  and evidence tools must satisfy the exact hash equality required by the reviewed rebaseline.

  The exact fixed-operation matrix remains:

  ```text
  payload bytes:           0, 1_024, 4_194_304
  queue depths:            1, 64, 16_384
  numeric producers:       1, 2, 4, 8
  representative fan-in:   checked production task inventory, currently exactly 1
  requested operations:    max(1_000_000, producers * 100_000) for 0/1,024-byte cells;
                           exactly 10,000 for every 4 MiB stress cell
  latency sample capacity: exactly the checked per-cell operation quota
  endpoint families:       queue push; queue pop; capture admission; writer append;
                           flush-inclusive writer
  ```

  The representative value is the checked sum documented by seed: one `SupervisedSourceTask`, one
  sequential `run_session`, Coinbase/mock publication inline, and no child capture producer. Numeric
  value `1` executes once but retains both `numeric-1` and `representative` labels in metadata.
  `available_parallelism` remains recorded host metadata and never chooses the production fixture.
  Every matrix case ends only at its checked fixed-operation quota; it never inherits the sustained
  fixture's time epochs.

  Compute each producer's deterministic nonzero sampling stride from its quota before the start
  barrier. Fallibly preallocate disjoint producer-local slices whose checked total is exactly
  the cell's exact checked quota, sample only declared stride ordinals across the full population, join all
  producers, and then aggregate. Out-of-stride writes, collector overflow, an unjoined producer,
  lost/double outcomes, count mismatch, zero duration, zero successes, zero samples, or malformed
  results exit nonzero. The five frozen endpoints preserve their distinct start/end boundaries and
  are never described as end-to-end event-to-decision latency.

  Each external candidate repetition is exactly the fixed matrix, deterministic comparable
  depth-one full fixture, separately named noncomparative forced-lock fixture, and representative
  sustained RSS fixture. The standard repetition is the identical set without forced-lock.
  Unsaturated representative acceptance inside the matrix requires zero typed
  publication refusals and an accepted coherent post-drain `CaptureAccountingSnapshot` with record
  reservation zero. The full fixture is invalid unless `QueueFull > 0`. The forced-lock diagnostic
  is accepted only when its typed status and every applicable real-message accounting field
  reconcile; an unavailable field is never encoded as numeric zero. It has no performance threshold
  and is excluded from performance acceptance. All fixture outcome counts must equal attempts
  exactly, every consumer count must equal successes, and all post-drain ledgers must reconcile.

  Only the representative sustained fixture uses two five-second warm epochs followed by ten
  ten-second measured burst/drain epochs. Every epoch completes and joins its current checked batch,
  drains the queue, obtains one accepted bounded-retry accounting snapshot, and then records
  post-drain RSS. The internal sampler uses the documented platform API/page size at 100 ms only to
  capture epoch peak and diagnostics; the growth gate compares the first measured post-drain value
  with the final value and the sequence of the final five measured post-drain values—not arbitrary
  interval samples. Final post-drain RSS must be no more than
  `max(8 MiB, 5% of first_measured_post_drain_rss)` above the first measured post-drain value, and
  the final five post-drain values must not each establish a new strict maximum. `/usr/bin/time -l`
  on macOS or `/usr/bin/time -v` on Linux records supplemental whole-process peak RSS. An unavailable
  platform RSS source is a typed failure of the host-memory claim.

  Exactly one candidate acceptance cell per repetition—capture admission, 1,024-byte payload,
  depth 16,384, and the labeled representative fan-in—requires at least 100,000
  successful capture admissions/second, warmed capture-admission p99 strictly below 1 ms, nonzero
  successes/samples, zero refusals, zero accounting invariant failures, and record reservation zero
  in the accepted post-drain snapshot. Criterion may schedule the frozen fixed-operation cases, but
  project p50/p95/p99/maximum come from the bounded project collector.

  One externally selected `CAPTURE_BENCH_REPETITION` executes exactly the backend's declared fixture
  set once. When the selector is absent the harness may orchestrate exactly five repetitions once;
  an outer loop and internal five-run mode must never be combined. Run one selector-driven dirty-tree
  repetition here only as non-evidence design validation, writing it to a distinct ignored
  `exploratory/` directory:

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  BASELINE_MANIFEST="$REPO_ROOT/target/q2-a4-capture-benchmark/standard/manifest.json"
  test -s "$BASELINE_MANIFEST"
  rm -rf target/q2-a4-capture-benchmark/exploratory
  mkdir -p target/q2-a4-capture-benchmark/exploratory
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run \
    --message-format=json > target/q2-a4-capture-bench-build.json
  BENCH_EXE="$(sed -n 's/.*"executable":"\([^"]*capture_admission[^"]*\)".*/\1/p' \
    target/q2-a4-capture-bench-build.json | tail -n 1)"
  test -n "$BENCH_EXE"
  test -x "$BENCH_EXE"
  set -o pipefail
  case "$(uname -s)" in
    Darwin)
      { /usr/bin/time -l env CAPTURE_BENCH_BACKEND=candidate \
          CAPTURE_BENCH_REPETITION=1 \
          CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
          CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
          CAPTURE_BENCH_OUTPUT=target/q2-a4-capture-benchmark/exploratory \
          "$BENCH_EXE" --bench; } 2>&1 | tee \
        target/q2-a4-capture-benchmark/exploratory/repetition-1.log
      ;;
    Linux)
      { /usr/bin/time -v env CAPTURE_BENCH_BACKEND=candidate \
          CAPTURE_BENCH_REPETITION=1 \
          CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
          CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
          CAPTURE_BENCH_OUTPUT=target/q2-a4-capture-benchmark/exploratory \
          "$BENCH_EXE" --bench; } 2>&1 | tee \
        target/q2-a4-capture-benchmark/exploratory/repetition-1.log
      ;;
    *)
      exit 1
      ;;
  esac
  ```

  Preserve but do not commit raw exploratory output. If the safe fixed ring misses a validity or
  acceptance criterion, reject MEM before integration and return to a documented bounded-queue
  design review; do not waive the result, tune away failures, or silently swap in an unaccounted
  implementation.

- [ ] **Step 6: Run dirty MEM gates, review, and commit**

  ```bash
  set -euo pipefail
  cargo fmt --all --check
  cargo test -p market-squawk-platform --all-targets --all-features --locked
  cargo test -p market-squawk --all-targets --all-features --locked
  cargo clippy -p market-squawk-platform -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-platform -p market-squawk --all-features --release --locked
  ./scripts/check_capture_queue_loom.sh
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run
  git diff --check
  git diff --cached --check
  git status --short
  ```

  Review only MEM-owned files, benchmark acceptance, and Loom evidence; stage those exact files,
  commit as `feat(capture): complete bounded capture memory`, and prove clean status. Raw ignored
  benchmark artifacts are evidence-transfer inputs, never committed generated output.

- [ ] **Step 7: Run the five-repetition evidence gate at the unchanged clean MEM head**

  Schedule the measured portion only after other implementation agents and repository build/test
  processes are idle. Hold the same root evidence lock and record the same host/power-policy fields
  as baseline; parallel planning or read-only work may continue only when it does not consume the
  benchmark host.

  ```bash
  set -euo pipefail
  MEM_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  cargo test -p market-squawk-platform --all-targets --all-features --locked
  cargo test -p market-squawk --all-targets --all-features --locked
  cargo clippy -p market-squawk-platform -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-platform -p market-squawk \
    --all-features --release --locked
  ./scripts/check_capture_queue_loom.sh
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  BASELINE_MANIFEST="$EVIDENCE_ROOT/standard/manifest.json"
  RUN_DIR="$EVIDENCE_ROOT/pre-rebase-candidate"
  HOST_EVIDENCE_DIR="$RUN_DIR/host-gate"
  test -s "$BASELINE_MANIFEST"
  (cd "$EVIDENCE_ROOT/standard" && shasum -a 256 -c SHA256SUMS)
  mkdir -p "$EVIDENCE_ROOT"
  mkdir "$EVIDENCE_ROOT/.exclusive-lock"
  trap 'rm -f "$EVIDENCE_ROOT/.exclusive-lock/owner.json"; rmdir "$EVIDENCE_ROOT/.exclusive-lock"' EXIT
  rm -rf "$RUN_DIR"
  mkdir -p "$RUN_DIR"
  printf '%s\n' no-other-active-agents > "$RUN_DIR/active-agent-attestation.txt"
  scripts/capture_benchmark_host_gate.sh preflight \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
    --output-dir "$HOST_EVIDENCE_DIR"
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run \
    --message-format=json > target/q2-a4-capture-bench-build.json
  BENCH_EXE="$(sed -n 's/.*"executable":"\([^"]*capture_admission[^"]*\)".*/\1/p' \
    target/q2-a4-capture-bench-build.json | tail -n 1)"
  test -n "$BENCH_EXE"
  test -x "$BENCH_EXE"
  cp "$BENCH_EXE" "$RUN_DIR/capture_admission-exe"
  BENCH_EXE="$RUN_DIR/capture_admission-exe"
  test -x "$BENCH_EXE"
  set -o pipefail
  for REPETITION in 1 2 3 4 5; do
    case "$(uname -s)" in
      Darwin)
        { /usr/bin/time -l env CAPTURE_BENCH_BACKEND=candidate \
            CAPTURE_BENCH_REPETITION="$REPETITION" \
            CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
            CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
            CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
            "$BENCH_EXE" --bench; } 2>&1 | tee \
          "$RUN_DIR/repetition-${REPETITION}.log"
        ;;
      Linux)
        { /usr/bin/time -v env CAPTURE_BENCH_BACKEND=candidate \
            CAPTURE_BENCH_REPETITION="$REPETITION" \
            CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
            CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
            CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
            "$BENCH_EXE" --bench; } 2>&1 | tee \
          "$RUN_DIR/repetition-${REPETITION}.log"
        ;;
      *)
        exit 1
        ;;
    esac
  done
  scripts/capture_benchmark_host_gate.sh postflight \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
    --output-dir "$HOST_EVIDENCE_DIR"
  env CAPTURE_BENCH_BACKEND=candidate \
    CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
    CAPTURE_BENCH_FINALIZE_ONLY=1 \
    CAPTURE_BENCH_HOST_EVIDENCE="$HOST_EVIDENCE_DIR/comparison.json" \
    CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
    "$BENCH_EXE" --bench
  test -s "$RUN_DIR/manifest.json"
  jq -e -s '.[0].backend == "candidate" and
    .[0].fixtures == ["matrix", "comparable_full", "forced_lock", "sustained_rss"] and
    .[0].repetitions == [1, 2, 3, 4, 5] and
    .[0].executable_path == "./capture_admission-exe" and
    .[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 != .[1].backend_sha256 and
    .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
    .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
    .[0].release_profile_sha256 == .[1].release_profile_sha256 and
    .[0].host_gate.valid == true and
    (.[0].host_gate.preflight_sha256 | length == 64) and
    (.[0].host_gate.postflight_sha256 | length == 64) and
    (.[0].host_gate.comparison_sha256 | length == 64)' \
    "$RUN_DIR/manifest.json" "$BASELINE_MANIFEST"
  (cd "$RUN_DIR" && \
    find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | \
      while IFS= read -r FILE; do shasum -a 256 "$FILE"; done > SHA256SUMS)
  test -s "$RUN_DIR/SHA256SUMS"
  (cd "$RUN_DIR" && shasum -a 256 -c SHA256SUMS)
  test "$(git rev-parse HEAD)" = "$MEM_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push -u origin feat/q2-a4-capture-memory
  test "$(git rev-parse HEAD)" = "$MEM_HEAD"
  ```

  Comment on canonical PR 1 explicitly. This is a lane handoff, not the Quarter 1 of 4 review:

  ```bash
  set -euo pipefail
  MEM_HEAD="$(git rev-parse HEAD)"
  A4_SEED_HEAD="$(git merge-base "$MEM_HEAD" feat/q2-a4-seed)"
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  MANIFEST="$REPO_ROOT/target/q2-a4-capture-benchmark/pre-rebase-candidate/manifest.json"
  ARTIFACT_DIGEST="$(shasum -a 256 \
    "$MANIFEST" | awk '{print $1}')"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 MEM lane ${MEM_HEAD} handed off from seed ${A4_SEED_HEAD}; platform/app MEM-owned files only; platform/app tests, clippy, clean-head release build, queue Loom, and five-repetition benchmark passed. Manifest SHA-256 ${ARTIFACT_DIGEST} at root target/q2-a4-capture-benchmark/pre-rebase-candidate/manifest.json (local ignored evidence).")"
  test -n "$COMMENT_URL"
  ```

## Ordered integration

### Task 4: Integrate TIME, rebase MEM, then integrate MEM

- [ ] **Step 1: Integrate and verify TIME first**

  In the seed integration worktree, fast-forward only from the frozen seed to the exact handed-off
  TIME head:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  cd "$REPO_ROOT/.worktrees/q2-a4-seed"
  A4_SEED_HEAD="$(git rev-parse HEAD)"
  TIME_HEAD="$(git rev-parse feat/q2-a4-trusted-time)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git merge-base --is-ancestor "$A4_SEED_HEAD" "$TIME_HEAD"
  git merge --ff-only "$TIME_HEAD"
  TIME_INTEGRATED_HEAD="$(git rev-parse HEAD)"
  test "$TIME_INTEGRATED_HEAD" = "$TIME_HEAD"
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-live --all-targets --all-features --locked
  ./scripts/check_authority_lifecycle_loom.sh
  test "$(git rev-parse HEAD)" = "$TIME_INTEGRATED_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push origin feat/q2-a4-seed
  test "$(git rev-parse HEAD)" = "$TIME_INTEGRATED_HEAD"
  ```

  Comment on canonical PR 1 with an explicit target:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  cd "$REPO_ROOT/.worktrees/q2-a4-seed"
  TIME_INTEGRATED_HEAD="$(git rev-parse HEAD)"
  TIME_HEAD="$(git rev-parse feat/q2-a4-trusted-time)"
  A4_SEED_HEAD="$(git merge-base "$TIME_HEAD" feat/q2-a4-capture-memory)"
  test "$TIME_INTEGRATED_HEAD" = "$TIME_HEAD"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 TIME integrated: seed ${A4_SEED_HEAD}, TIME ${TIME_HEAD}, integrated head ${TIME_INTEGRATED_HEAD}; fast-forward equality and sources/live/authority-Loom gates passed.")"
  test -n "$COMMENT_URL"
  ```

- [ ] **Step 2: Rebase clean MEM onto exact integrated TIME**

  Rebase with an explicit old-base/new-base range; never plain `git rebase`:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  cd "$REPO_ROOT/.worktrees/q2-a4-memory"
  git fetch origin feat/q2-a4-capture-memory
  OLD_MEM_HEAD="$(git rev-parse HEAD)"
  test "$OLD_MEM_HEAD" = "$(git rev-parse origin/feat/q2-a4-capture-memory)"
  TIME_INTEGRATED_HEAD="$(git rev-parse feat/q2-a4-seed)"
  A4_SEED_HEAD="$(git merge-base "$OLD_MEM_HEAD" "$TIME_INTEGRATED_HEAD")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  mkdir -p "$EVIDENCE_ROOT"
  jq -n --arg old_mem_head "$OLD_MEM_HEAD" \
    --arg time_integrated_head "$TIME_INTEGRATED_HEAD" \
    --arg a4_seed_head "$A4_SEED_HEAD" \
    '{old_mem_head: $old_mem_head, time_integrated_head: $time_integrated_head,
      a4_seed_head: $a4_seed_head}' | tee "$EVIDENCE_ROOT/rebase-inputs.json"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git merge-base --is-ancestor "$A4_SEED_HEAD" "$OLD_MEM_HEAD"
  git merge-base --is-ancestor "$A4_SEED_HEAD" "$TIME_INTEGRATED_HEAD"
  git rebase --onto "$TIME_INTEGRATED_HEAD" "$A4_SEED_HEAD" \
    feat/q2-a4-capture-memory
  REBASED_MEM_HEAD="$(git rev-parse HEAD)"
  git merge-base --is-ancestor "$TIME_INTEGRATED_HEAD" "$REBASED_MEM_HEAD"
  EMPTY_OUTPUT="$(git diff --name-only "$TIME_INTEGRATED_HEAD..$REBASED_MEM_HEAD" -- \
    crates/market-squawk-sources crates/market-squawk-live)"
  test -z "$EMPTY_OUTPUT"
  git range-diff "$A4_SEED_HEAD..$OLD_MEM_HEAD" \
    "$TIME_INTEGRATED_HEAD..$REBASED_MEM_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

  Any conflict in TIME-owned files rejects the rebase for integration-owner resolution; do not
  auto-resolve, drop, or duplicate TIME contracts. Do not push the rewritten MEM branch until the
  next exact-head gate passes.

- [ ] **Step 3: Verify the rebased MEM exact head**

  The fresh measured portion is an integration-owner barrier: all other implementation/review agents
  and repository builds are idle, hardware and power policy match the baseline manifest, and the
  exclusive root evidence lock is held for all five repetitions.

  ```bash
  set -euo pipefail
  REBASED_MEM_HEAD="$(git rev-parse HEAD)"
  TIME_INTEGRATED_HEAD="$(git rev-parse feat/q2-a4-seed)"
  OLD_MEM_HEAD="$(git rev-parse origin/feat/q2-a4-capture-memory)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git merge-base --is-ancestor "$TIME_INTEGRATED_HEAD" "$REBASED_MEM_HEAD"
  cargo test -p market-squawk-sources --test capture_bridge --all-features --locked
  cargo test -p market-squawk-sources --all-targets --all-features --locked
  cargo test -p market-squawk-live --all-targets --all-features --locked
  cargo test -p market-squawk-platform --all-targets --all-features --locked
  cargo test -p market-squawk --all-targets --all-features --locked
  cargo clippy -p market-squawk-sources -p market-squawk-live \
    -p market-squawk-platform -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo build -p market-squawk-sources -p market-squawk-live \
    -p market-squawk-platform -p market-squawk --all-features --release --locked
  ./scripts/check_authority_lifecycle_loom.sh
  ./scripts/check_capture_queue_loom.sh
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  BASELINE_MANIFEST="$EVIDENCE_ROOT/standard/manifest.json"
  RUN_DIR="$EVIDENCE_ROOT/rebased-candidate"
  HOST_EVIDENCE_DIR="$RUN_DIR/host-gate"
  test -s "$BASELINE_MANIFEST"
  (cd "$EVIDENCE_ROOT/standard" && shasum -a 256 -c SHA256SUMS)
  mkdir -p "$EVIDENCE_ROOT"
  mkdir "$EVIDENCE_ROOT/.exclusive-lock"
  trap 'rm -f "$EVIDENCE_ROOT/.exclusive-lock/owner.json"; rmdir "$EVIDENCE_ROOT/.exclusive-lock"' EXIT
  rm -rf "$RUN_DIR"
  mkdir -p "$RUN_DIR"
  printf '%s\n' no-other-active-agents > "$RUN_DIR/active-agent-attestation.txt"
  scripts/capture_benchmark_host_gate.sh preflight \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
    --output-dir "$HOST_EVIDENCE_DIR"
  cargo bench -p market-squawk-platform --bench capture_admission --locked --no-run \
    --message-format=json > target/q2-a4-capture-bench-build.json
  BENCH_EXE="$(sed -n 's/.*"executable":"\([^"]*capture_admission[^"]*\)".*/\1/p' \
    target/q2-a4-capture-bench-build.json | tail -n 1)"
  test -n "$BENCH_EXE"
  test -x "$BENCH_EXE"
  cp "$BENCH_EXE" "$RUN_DIR/capture_admission-exe"
  BENCH_EXE="$RUN_DIR/capture_admission-exe"
  test -x "$BENCH_EXE"
  set -o pipefail
  for REPETITION in 1 2 3 4 5; do
    case "$(uname -s)" in
      Darwin)
        { /usr/bin/time -l env CAPTURE_BENCH_BACKEND=candidate \
            CAPTURE_BENCH_REPETITION="$REPETITION" \
            CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
            CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
            CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
            "$BENCH_EXE" --bench; } 2>&1 | tee \
          "$RUN_DIR/repetition-${REPETITION}.log"
        ;;
      Linux)
        { /usr/bin/time -v env CAPTURE_BENCH_BACKEND=candidate \
            CAPTURE_BENCH_REPETITION="$REPETITION" \
            CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
            CAPTURE_BENCH_EXPECTED_FIXTURES=matrix,comparable_full,forced_lock,sustained_rss \
            CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
            "$BENCH_EXE" --bench; } 2>&1 | tee \
          "$RUN_DIR/repetition-${REPETITION}.log"
        ;;
      *)
        exit 1
        ;;
    esac
  done
  scripts/capture_benchmark_host_gate.sh postflight \
    --lock-dir "$EVIDENCE_ROOT/.exclusive-lock" \
    --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
    --output-dir "$HOST_EVIDENCE_DIR"
  env CAPTURE_BENCH_BACKEND=candidate \
    CAPTURE_BENCH_BASELINE_MANIFEST="$BASELINE_MANIFEST" \
    CAPTURE_BENCH_FINALIZE_ONLY=1 \
    CAPTURE_BENCH_HOST_EVIDENCE="$HOST_EVIDENCE_DIR/comparison.json" \
    CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
    "$BENCH_EXE" --bench
  test -s "$RUN_DIR/manifest.json"
  jq -e -s '.[0].backend == "candidate" and
    .[0].fixtures == ["matrix", "comparable_full", "forced_lock", "sustained_rss"] and
    .[0].repetitions == [1, 2, 3, 4, 5] and
    .[0].executable_path == "./capture_admission-exe" and
    .[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 != .[1].backend_sha256 and
    .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
    .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
    .[0].release_profile_sha256 == .[1].release_profile_sha256 and
    .[0].host_gate.valid == true and
    (.[0].host_gate.preflight_sha256 | length == 64) and
    (.[0].host_gate.postflight_sha256 | length == 64) and
    (.[0].host_gate.comparison_sha256 | length == 64)' \
    "$RUN_DIR/manifest.json" "$BASELINE_MANIFEST"
  (cd "$RUN_DIR" && \
    find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | \
      while IFS= read -r FILE; do shasum -a 256 "$FILE"; done > SHA256SUMS)
  test -s "$RUN_DIR/SHA256SUMS"
  (cd "$RUN_DIR" && shasum -a 256 -c SHA256SUMS)
  test "$(git rev-parse HEAD)" = "$REBASED_MEM_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push --force-with-lease=refs/heads/feat/q2-a4-capture-memory:"$OLD_MEM_HEAD" \
    origin feat/q2-a4-capture-memory
  test "$(git rev-parse HEAD)" = "$REBASED_MEM_HEAD"
  ```

  This is a mandatory fresh five-repetition run because TIME integration is code-affecting. The
  harness writes a nonempty manifest naming `REBASED_MEM_HEAD`, the benchmark executable SHA-256,
  every production/fixture input hash, baseline harness/fixture hashes, repetition set, and raw
  artifact digests. Later documentation-only candidate verification may reuse this raw run only
  while all those hashes remain identical.

  Comment on canonical PR 1 with the explicit rebased facts:

  ```bash
  set -euo pipefail
  REBASED_MEM_HEAD="$(git rev-parse HEAD)"
  TIME_INTEGRATED_HEAD="$(git rev-parse feat/q2-a4-seed)"
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  MANIFEST="$REPO_ROOT/target/q2-a4-capture-benchmark/rebased-candidate/manifest.json"
  REBASE_INPUTS="$REPO_ROOT/target/q2-a4-capture-benchmark/rebase-inputs.json"
  OLD_MEM_HEAD="$(jq -er '.old_mem_head' "$REBASE_INPUTS")"
  test "$(jq -er '.measured_code_head' "$MANIFEST")" = "$REBASED_MEM_HEAD"
  test "$(jq -er '.time_integrated_head' "$REBASE_INPUTS")" = "$TIME_INTEGRATED_HEAD"
  ARTIFACT_DIGEST="$(shasum -a 256 \
    "$MANIFEST" | awk '{print $1}')"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 MEM rebased from ${OLD_MEM_HEAD} onto TIME ${TIME_INTEGRATED_HEAD} as ${REBASED_MEM_HEAD}; range-diff/ancestry and combined exact-head gates passed; fresh five-repetition manifest SHA-256 ${ARTIFACT_DIGEST}.")"
  test -n "$COMMENT_URL"
  ```

- [ ] **Step 4: Integrate rebased MEM and clean lane worktrees**

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  REBASED_MEM_HEAD="$(git -C "$REPO_ROOT/.worktrees/q2-a4-memory" rev-parse HEAD)"
  cd "$REPO_ROOT/.worktrees/q2-a4-seed"
  TIME_INTEGRATED_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git merge-base --is-ancestor "$TIME_INTEGRATED_HEAD" "$REBASED_MEM_HEAD"
  git merge --ff-only "$REBASED_MEM_HEAD"
  INTEGRATED_HEAD="$(git rev-parse HEAD)"
  test "$INTEGRATED_HEAD" = "$REBASED_MEM_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push origin feat/q2-a4-seed
  test "$(git rev-parse HEAD)" = "$INTEGRATED_HEAD"
  ```

  Comment on canonical PR 1 with the explicit integration equality and handoff:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  cd "$REPO_ROOT/.worktrees/q2-a4-seed"
  INTEGRATED_HEAD="$(git rev-parse HEAD)"
  REBASED_MEM_HEAD="$(git rev-parse origin/feat/q2-a4-capture-memory)"
  TIME_INTEGRATED_HEAD="$(git rev-parse feat/q2-a4-trusted-time)"
  test "$INTEGRATED_HEAD" = "$REBASED_MEM_HEAD"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Q2 A4 combined integration head ${INTEGRATED_HEAD}; TIME parent ${TIME_INTEGRATED_HEAD}, rebased MEM ${REBASED_MEM_HEAD}, and fast-forward equality verified. Fresh benchmark evidence is handed to Wave 3.")"
  test -n "$COMMENT_URL"
  ```

  Verify TIME and MEM worktrees have
  empty staged, unstaged, and untracked status and no agent/process uses them, then remove normally:

  ```bash
  set -euo pipefail
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT/.worktrees/q2-a4-time" status --short)"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT/.worktrees/q2-a4-memory" status --short)"
  test -z "$EMPTY_OUTPUT"
  git worktree remove "$REPO_ROOT/.worktrees/q2-a4-time"
  git worktree remove "$REPO_ROOT/.worktrees/q2-a4-memory"
  git worktree prune
  test ! -d "$REPO_ROOT/.worktrees/q2-a4-time"
  test ! -d "$REPO_ROOT/.worktrees/q2-a4-memory"
  ```

  Never force-remove dirty/active state. Retain branches through checkpoint publication.

## Wave 3: one exact Quarter 1 of 4 checkpoint

### Task 5: Commit truth, freeze, verify, review, and publish

**Files:**

- Modify: `docs/architecture/current-state.md`
- Modify: `docs/architecture/target-state.md`
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Modify: `docs/project-memory.md`
- Create: `docs/reports/2026-07-17-q2-a4-evidence-lock.json`
- Create: `docs/reports/2026-07-17-q2-a4-verification.md`

- [ ] **Step 1: Update truth and evidence before candidate freeze**

  Record implemented versus remaining capability, exact lane/integration commits, TDD/Loom gates,
  benchmark hardware/results, structural/RSS limits, and optional hosted status. Do not claim hosted
  portability while runner assignment is absent. Copy/digest raw MEM artifacts into the exact
  verification report; do not commit generated Criterion directories. Run documentation/dirty
  gates, `git diff --check`, `git diff --cached --check`, and `git status --short`; review the exact
  seven-file truth diff, stage it explicitly, commit, and prove clean before freezing the candidate.
  The committed evidence-lock JSON has a closed, unknown-field-denying schema and records the exact
  measured code head, standard and rebased-candidate manifest SHA-256 values, both `SHA256SUMS`
  file digests, executable digest, immutable-module object, entrypoint digest, backend digests, and
  both standard/candidate host-gate objects. Its exact keys are `schema_version`,
  `measured_code_head`, `standard_manifest_sha256`, `candidate_manifest_sha256`,
  `standard_checksums_sha256`, `candidate_checksums_sha256`, `executable_sha256`,
  `immutable_module_sha256`, `entrypoint_sha256`, `standard_backend_sha256`,
  `candidate_backend_sha256`, `standard_host_gate`, and `candidate_host_gate`. The verification
  report renders the same values for humans; the JSON
  is the machine authority used again at final publication.
  The memory update records integrated candidate facts, hosted class, evidence paths, clean
  worktree disposition, and the immediate canonical Stage 2 planning/Wave barrier. It records that
  implementation continues through the complete release plan under four quarter-checkpoint reviews.
  It does not award projected credit. Because a file cannot contain its own commit hash, it
  identifies its authority as the exact commit containing the entry; the canonical PR comment later
  binds that literal SHA. The first canonical Stage 2 planning/freeze commit resolves the literal
  approved historical Q2/A4 SHA. There is no tracked post-review memory edit at Quarter 1 of 4.

- [ ] **Step 2: Run the unchanged clean exact-head local gate**

  ```bash
  set -euo pipefail
  CANDIDATE_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ./scripts/verify.sh
  ./scripts/check_capture_queue_loom.sh
  cargo deny check
  cargo audit --deny warnings
  gitleaks dir --no-banner --redact --config .gitleaks.toml .
  gitleaks git --no-banner --redact --config .gitleaks.toml .
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  STANDARD_DIR="$EVIDENCE_ROOT/standard"
  RUN_DIR="$EVIDENCE_ROOT/rebased-candidate"
  BASELINE_MANIFEST="$STANDARD_DIR/manifest.json"
  MANIFEST="$RUN_DIR/manifest.json"
  EVIDENCE_LOCK=docs/reports/2026-07-17-q2-a4-evidence-lock.json
  MEASURED_CODE_HEAD="$(jq -er '.measured_code_head' "$MANIFEST")"
  RELATIVE_BENCH_EXE="$(jq -er '.executable_path' "$MANIFEST")"
  case "$RELATIVE_BENCH_EXE" in
    ./*) ;;
    *) exit 1 ;;
  esac
  BENCH_EXE="$RUN_DIR/${RELATIVE_BENCH_EXE#./}"
  test -n "$MEASURED_CODE_HEAD"
  test -x "$BENCH_EXE"
  EMPTY_OUTPUT="$(git diff --name-only "$MEASURED_CODE_HEAD..$CANDIDATE_HEAD" | \
    awk '$0 !~ /^docs\/(architecture\/|plans\/|reports\/|project-memory\.md$)/ { print }')"
  test -z "$EMPTY_OUTPUT"
  (cd "$STANDARD_DIR" && shasum -a 256 -c SHA256SUMS)
  (cd "$RUN_DIR" && shasum -a 256 -c SHA256SUMS)
  jq -e -s '.[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 != .[1].backend_sha256 and
    .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
    .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
    .[0].release_profile_sha256 == .[1].release_profile_sha256 and
    .[0].host_gate.valid == true' \
    "$MANIFEST" "$BASELINE_MANIFEST"
  test "$(jq -er '.measured_code_head' "$EVIDENCE_LOCK")" = "$MEASURED_CODE_HEAD"
  test "$(jq -er '.standard_manifest_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$BASELINE_MANIFEST" | awk '{print $1}')"
  test "$(jq -er '.candidate_manifest_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$MANIFEST" | awk '{print $1}')"
  test "$(jq -er '.standard_checksums_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$STANDARD_DIR/SHA256SUMS" | awk '{print $1}')"
  test "$(jq -er '.candidate_checksums_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$RUN_DIR/SHA256SUMS" | awk '{print $1}')"
  jq -e '(.schema_version == 1) and
    ((keys | sort) == ["candidate_backend_sha256", "candidate_checksums_sha256",
      "candidate_host_gate", "candidate_manifest_sha256", "entrypoint_sha256",
      "executable_sha256", "immutable_module_sha256", "measured_code_head",
      "schema_version", "standard_backend_sha256", "standard_checksums_sha256",
      "standard_host_gate", "standard_manifest_sha256"])' "$EVIDENCE_LOCK"
  jq -e -s '.[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 == .[1].candidate_backend_sha256 and
    .[2].backend_sha256 == .[1].standard_backend_sha256 and
    .[0].executable_sha256 == .[1].executable_sha256 and
    .[0].host_gate == .[1].candidate_host_gate and
    .[2].host_gate == .[1].standard_host_gate' \
    "$MANIFEST" "$EVIDENCE_LOCK" "$BASELINE_MANIFEST"
  test "$(jq -er '.executable_sha256' "$MANIFEST")" = \
    "$(shasum -a 256 "$BENCH_EXE" | awk '{print $1}')"
  test "$(git rev-parse HEAD)" = "$CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

  Expected: every command succeeds at one unchanged clean commit. The candidate reuses the mandatory
  fresh rebased-head benchmark only because its sole descendant change is the reviewed seven-file
  documentation/evidence commit and the executable, production inputs, fixture, harness, and raw
  artifact hashes are identical. The verification report truthfully names `MEASURED_CODE_HEAD`;
  any code-affecting change requires a fresh five-repetition run instead of this reuse path.

- [ ] **Step 3: Record optional hosted portability evidence**

  Push the exact locally verified candidate, then use the Wave 0 four-state query with
  `CANDIDATE_HEAD` substituted for `A3_HEAD`. Persist `no_run_for_exact_sha`,
  `run_exists_zero_assigned_jobs`, `assigned_jobs_failed_or_incomplete`, or
  `assigned_jobs_success`; never infer billing from no run and never turn assigned failure into an
  account gate.

  ```bash
  set -euo pipefail
  CANDIDATE_HEAD="$(git rev-parse HEAD)"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git push origin feat/q2-a4-seed
  git fetch origin feat/q2-a4-seed
  test "$(git rev-parse origin/feat/q2-a4-seed)" = "$CANDIDATE_HEAD"
  test "$(git rev-parse HEAD)" = "$CANDIDATE_HEAD"
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  PRE_PROMOTION_EVIDENCE_DIR="$REPO_ROOT/target/q2-a4-hosted/q2-pre-promotion"
  PRE_PROMOTION_HOSTED_CLASS="$(scripts/classify_hosted_run.sh \
    --repo Sawmonabo/market-squawk \
    --sha "$CANDIDATE_HEAD" \
    --workflow CI \
    --poll-attempts 1 \
    --poll-interval-seconds 1 \
    --output-dir "$PRE_PROMOTION_EVIDENCE_DIR")"
  test "$PRE_PROMOTION_HOSTED_CLASS" = \
    "$(sed -n '1p' "$PRE_PROMOTION_EVIDENCE_DIR/class.txt")"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\tab3f7c19000884357c38702edf6b4acc6a80c483\tmain\tOPEN\ttrue')"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Historical Q2/A4 candidate ${CANDIDATE_HEAD} published for Quarter 1 of 4 review; approval is pending. Local exact-head gates and rebased benchmark evidence are recorded in docs/reports/2026-07-17-q2-a4-verification.md. Pre-promotion hosted class: ${PRE_PROMOTION_HOSTED_CLASS}; raw evidence: root target/q2-a4-hosted/q2-pre-promotion.")"
  test -n "$COMMENT_URL"
  ```

  Optional hosted state does not alter the local approval criteria or permit a false portability
  claim.

- [ ] **Step 4: Dispatch grouped Quarter 1 of 4 reviewers on one SHA**

  Give the same `CANDIDATE_HEAD` to independent non-mutating reviewers for TIME/authority,
  memory/queue/sink/journal, and integration/tests/performance. Do not change the candidate between
  batches. Union and deduplicate all findings before remediation.

- [ ] **Step 5: Decide and promote the reviewed SHA without a tracked post-review edit**

  Any substantiated Critical, Important, or Minor finding rejects the checkpoint and requires
  remediation, complete exact-head verification, and re-review. On approval, make no tracked file
  edit: push the already reviewed seed SHA if needed, fast-forward the canonical
  `feat/stage-1-foundation` PR head to that exact SHA, and comment on canonical PR 1 with local
  evidence, benchmark facts, optional hosted status, worktree disposition, and remaining work. A
  later text or evidence correction is a new candidate commit and invalidates the former review.

  The integration owner receives the literal full SHA approved by all three reviewers and assigns it
  once as `REVIEWED_CANDIDATE_HEAD`; do not re-derive it from a mutable checkout after review. Use
  explicit repository, PR, branch, ancestry, predecessor, and equality guards. The main worktree and
  remote PR branch must still be at the recorded predecessor before promotion; any divergence stops
  for integration-owner review. The lease-protected push below is an atomic expected-old guard after
  fast-forward ancestry is proven, not authorization to rewrite divergent history:

  ```bash
  set -euo pipefail
  REVIEWED_CANDIDATE_HEAD='<full-reviewed-sha>'
  Q2_PREDECESSOR=ab3f7c19000884357c38702edf6b4acc6a80c483
  PR_HEAD_BRANCH=feat/stage-1-foundation
  test "$(printf '%s' "$REVIEWED_CANDIDATE_HEAD" | wc -c | tr -d ' ')" -eq 40
  printf '%s\n' "$REVIEWED_CANDIDATE_HEAD" | rg '^[0-9a-f]{40}$'
  test "$(git branch --show-current)" = feat/q2-a4-seed
  test "$(git rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git fetch origin feat/q2-a4-seed "$PR_HEAD_BRANCH"
  test "$(git rev-parse origin/feat/q2-a4-seed)" = "$REVIEWED_CANDIDATE_HEAD"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json number --jq .number)" = 1
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json headRefName --jq .headRefName)" = \
    "$PR_HEAD_BRANCH"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json headRefOid --jq .headRefOid)" = \
    "$Q2_PREDECESSOR"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json baseRefName --jq .baseRefName)" = main
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json state --jq .state)" = OPEN
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk --json isDraft --jq .isDraft)" = true
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  test "$(git -C "$REPO_ROOT" branch --show-current)" = "$PR_HEAD_BRANCH"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT" status --short)"
  test -z "$EMPTY_OUTPUT"
  git -C "$REPO_ROOT" fetch origin "$PR_HEAD_BRANCH"
  test "$(git -C "$REPO_ROOT" rev-parse HEAD)" = "$Q2_PREDECESSOR"
  test "$(git -C "$REPO_ROOT" rev-parse "$PR_HEAD_BRANCH")" = "$Q2_PREDECESSOR"
  test "$(git -C "$REPO_ROOT" rev-parse "origin/$PR_HEAD_BRANCH")" = "$Q2_PREDECESSOR"
  git -C "$REPO_ROOT" merge-base --is-ancestor \
    "$Q2_PREDECESSOR" "$REVIEWED_CANDIDATE_HEAD"
  git -C "$REPO_ROOT" merge --ff-only "$REVIEWED_CANDIDATE_HEAD"
  test "$(git -C "$REPO_ROOT" rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  test "$(git -C "$REPO_ROOT" rev-parse "$PR_HEAD_BRANCH")" = \
    "$REVIEWED_CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT" status --short)"
  test -z "$EMPTY_OUTPUT"
  git -C "$REPO_ROOT" push \
    --force-with-lease="refs/heads/${PR_HEAD_BRANCH}:${Q2_PREDECESSOR}" \
    origin \
    "${REVIEWED_CANDIDATE_HEAD}:refs/heads/${PR_HEAD_BRANCH}"
  git -C "$REPO_ROOT" fetch origin "$PR_HEAD_BRANCH"
  test "$(git -C "$REPO_ROOT" rev-parse "origin/$PR_HEAD_BRANCH")" = \
    "$REVIEWED_CANDIDATE_HEAD"
  test "$(git -C "$REPO_ROOT" rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\t%s\tmain\tOPEN\ttrue' \
      "$REVIEWED_CANDIDATE_HEAD")"
  POST_PROMOTION_EVIDENCE_DIR="$REPO_ROOT/target/q2-a4-hosted/q2-post-promotion"
  POST_PROMOTION_HOSTED_CLASS="$( \
    "$REPO_ROOT/scripts/classify_hosted_run.sh" \
      --repo Sawmonabo/market-squawk \
      --sha "$REVIEWED_CANDIDATE_HEAD" \
      --workflow CI \
      --poll-attempts 12 \
      --poll-interval-seconds 5 \
      --output-dir "$POST_PROMOTION_EVIDENCE_DIR")"
  test "$POST_PROMOTION_HOSTED_CLASS" = \
    "$(sed -n '1p' "$POST_PROMOTION_EVIDENCE_DIR/class.txt")"
  test "$(git rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  test "$(git rev-parse origin/feat/q2-a4-seed)" = "$REVIEWED_CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  EMPTY_OUTPUT="$(git -C "$REPO_ROOT" status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

  After post-promotion classification and confirmation no process uses the seed worktree, verify the
  seed and newly promoted main worktree are clean, remove the seed normally, prune, and only then
  publish the final Quarter 1 of 4 approval comment. Never force-remove.
  Retain the published branches until the normal PR/branch completion decision.

  ```bash
  set -euo pipefail
  REVIEWED_CANDIDATE_HEAD='<same-full-reviewed-sha>'
  COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
  REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
  SEED_WORKTREE="$REPO_ROOT/.worktrees/q2-a4-seed"
  EMPTY_OUTPUT="$(git -C "$SEED_WORKTREE" status --short)"
  test -z "$EMPTY_OUTPUT"
  cd "$REPO_ROOT"
  test "$(git rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  git worktree remove "$SEED_WORKTREE"
  git worktree prune
  test ! -d "$SEED_WORKTREE"
  test ! -d "$REPO_ROOT/.worktrees/q2-a4-wave0"
  test ! -d "$REPO_ROOT/.worktrees/q2-a4-time"
  test ! -d "$REPO_ROOT/.worktrees/q2-a4-memory"
  POST_PROMOTION_EVIDENCE_DIR="$REPO_ROOT/target/q2-a4-hosted/q2-post-promotion"
  POST_PROMOTION_HOSTED_CLASS="$(sed -n '1p' \
    "$POST_PROMOTION_EVIDENCE_DIR/class.txt")"
  EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
  STANDARD_DIR="$EVIDENCE_ROOT/standard"
  CANDIDATE_DIR="$EVIDENCE_ROOT/rebased-candidate"
  BASELINE_MANIFEST="$STANDARD_DIR/manifest.json"
  MANIFEST="$CANDIDATE_DIR/manifest.json"
  EVIDENCE_LOCK=docs/reports/2026-07-17-q2-a4-evidence-lock.json
  MEASURED_CODE_HEAD="$(jq -er '.measured_code_head' "$MANIFEST")"
  STANDARD_MANIFEST_DIGEST="$(shasum -a 256 "$BASELINE_MANIFEST" | awk '{print $1}')"
  ARTIFACT_DIGEST="$(shasum -a 256 "$MANIFEST" | awk '{print $1}')"
  test -n "$POST_PROMOTION_HOSTED_CLASS"
  test -n "$MEASURED_CODE_HEAD"
  (cd "$STANDARD_DIR" && shasum -a 256 -c SHA256SUMS)
  (cd "$CANDIDATE_DIR" && shasum -a 256 -c SHA256SUMS)
  test "$(jq -er '.standard_manifest_sha256' "$EVIDENCE_LOCK")" = \
    "$STANDARD_MANIFEST_DIGEST"
  test "$(jq -er '.candidate_manifest_sha256' "$EVIDENCE_LOCK")" = "$ARTIFACT_DIGEST"
  test "$(jq -er '.standard_checksums_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$STANDARD_DIR/SHA256SUMS" | awk '{print $1}')"
  test "$(jq -er '.candidate_checksums_sha256' "$EVIDENCE_LOCK")" = \
    "$(shasum -a 256 "$CANDIDATE_DIR/SHA256SUMS" | awk '{print $1}')"
  test "$(jq -er '.measured_code_head' "$EVIDENCE_LOCK")" = "$MEASURED_CODE_HEAD"
  EMPTY_OUTPUT="$(git diff --name-only "$MEASURED_CODE_HEAD..$REVIEWED_CANDIDATE_HEAD" | \
    awk '$0 !~ /^docs\/(architecture\/|plans\/|reports\/|project-memory\.md$)/ { print }')"
  test -z "$EMPTY_OUTPUT"
  jq -e -s '.[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 != .[1].backend_sha256 and
    .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
    .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
    .[0].release_profile_sha256 == .[1].release_profile_sha256 and
    .[0].host_gate.valid == true' "$MANIFEST" "$BASELINE_MANIFEST"
  jq -e -s '.[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
    .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
    .[0].backend_sha256 == .[1].candidate_backend_sha256 and
    .[2].backend_sha256 == .[1].standard_backend_sha256 and
    .[0].executable_sha256 == .[1].executable_sha256 and
    .[0].host_gate == .[1].candidate_host_gate and
    .[2].host_gate == .[1].standard_host_gate' \
    "$MANIFEST" "$EVIDENCE_LOCK" "$BASELINE_MANIFEST"
  test "$(gh pr view 1 --repo Sawmonabo/market-squawk \
    --json number,headRefName,headRefOid,baseRefName,state,isDraft \
    --jq '[.number,.headRefName,.headRefOid,.baseRefName,.state,.isDraft] | @tsv')" = \
    "$(printf '1\tfeat/stage-1-foundation\t%s\tmain\tOPEN\ttrue' \
      "$REVIEWED_CANDIDATE_HEAD")"
  COMMENT_URL="$(gh pr comment 1 --repo Sawmonabo/market-squawk --body \
    "Historical Q2/A4 approved as Quarter 1 of 4 at exact candidate ${REVIEWED_CANDIDATE_HEAD}; measured code ${MEASURED_CODE_HEAD}; local verifier, queue Loom, audits, and capture-admission benchmark passed; standard manifest SHA-256 ${STANDARD_MANIFEST_DIGEST}; candidate manifest SHA-256 ${ARTIFACT_DIGEST}. Post-promotion hosted class: ${POST_PROMOTION_HOSTED_CLASS}; raw evidence: root target/q2-a4-hosted/q2-post-promotion. Wave0, TIME, MEM, and seed worktrees were removed normally; canonical root retained clean. Next barrier is canonical Stage 2 planning and its reviewed Wave sequence; implementation continues through Stages 2-7.")"
  test -n "$COMMENT_URL"
  printf '%s\n' "$COMMENT_URL" | rg '^https://github\.com/Sawmonabo/market-squawk/'
  test "$(git rev-parse HEAD)" = "$REVIEWED_CANDIDATE_HEAD"
  EMPTY_OUTPUT="$(git status --short)"
  test -z "$EMPTY_OUTPUT"
  ```

- [ ] **Step 6: Close Quarter 1 of 4 and continue through the canonical Stage/Wave plan**

  Report the Quarter 1 outcome, exact frozen commit, evidence paths, hosted class, worktree
  disposition, remaining Stage 2-7 blockers, and the next barrier. Immediately return to the
  canonical Stage 2 plan and its reviewed Wave sequence, then continue through Stage 3 research
  storage, Stage 4 Python/modeling/analytics/portfolio, Stage 5 strategy/risk/execution, Stage 6
  valuation and complete typed MCP, and Stage 7 release hardening. The four quarter checkpoints are
  the grouped review barriers; no intermediate percentage milestone pauses implementation.
  Continue until the complete release definition of done is satisfied. Do not award projected,
  contract-only, scaffold, or unverified credit in progress reporting.

No lane result, dirty gate, benchmark subset, hosted result, or documentation claim constitutes
Quarter 1 of 4 approval by itself.
