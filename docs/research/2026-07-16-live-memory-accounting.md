# Lane B live-memory accounting: source basis and proof boundary

Date: 2026-07-16

Research basis: Rust 1.97.0 (`rustc 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3`),
`arc-swap` 1.9.2, and Tokio 1.52.4

Implementation scope: Quarter 2 Lane B book-processing and snapshot-generation memory accounting

## Table of contents

- [Conclusion](#conclusion)
- [Evidence classes](#evidence-classes)
- [Version and commit anchors](#version-and-commit-anchors)
- [Primary-source findings](#primary-source-findings)
- [B1: closed book-processing inventory](#b1-closed-book-processing-inventory)
- [B2: snapshot generations and reader leases](#b2-snapshot-generations-and-reader-leases)
- [Checked arithmetic and fail-closed construction](#checked-arithmetic-and-fail-closed-construction)
- [What the tests prove](#what-the-tests-prove)
- [Why allocator observation cannot replace the formula](#why-allocator-observation-cannot-replace-the-formula)
- [Platform- and allocator-dependent remainder](#platform--and-allocator-dependent-remainder)
- [Revalidation gates](#revalidation-gates)
- [Primary sources](#primary-sources)

## Conclusion

Lane B replaces open-ended allocation assumptions with a closed, checked inventory of the Rust
objects that can coexist in the supported live-processing paths. It does so by changing the
production object graph, not merely by applying larger constants to the former implementation:

- provider books now use fixed-length boxed slot arrays with reusable active/candidate images;
- shard normalization scratch is allocated before actor processing and reused;
- production mutation no longer owns `BTreeMap` nodes or rollback journals;
- canonical `Vec` outputs are normalized through `Box<[T]>`, and the structural model charges a
  possible old/new allocation overlap rather than assuming that `try_reserve_exact` is exact;
- snapshot publication accounts for a predecessor and successor per publishing shard, plus one
  additional retained publication per weighted reader permit;
- aggregate-reader arrays, lease metadata, and the pinned Rust 1.97 `Arc` control-block structure
  are included with checked arithmetic.

This is a conservative **structural retained-byte model for the compiled target and pinned
dependencies**. It is not a universal statement about allocator block sizes, heap metadata,
fragmentation, resident-set size, or future Rust/dependency layouts. In particular, a boxed slice
has a fixed API-visible element count, but the global allocator may still reserve more physical
space than the element payload. No claim in this document treats allocator behavior as universally
byte-exact.

## Evidence classes

Every claim below belongs to one of these classes. Mixing the classes would overstate the proof.

| Class | Meaning | Permitted use |
|---|---|---|
| **Normative API guarantee** | A contract documented by the versioned Rust 1.97 API/Reference or by the exact crate version pinned in `Cargo.lock`. | Establishes behavior callers may rely on for that pinned release. |
| **Pinned implementation fact** | A private implementation detail verified in the exact Rust compiler source or exact crate source. It is not a stable API or ABI promise. | Supports the current formula, with an explicit re-audit gate on toolchain/dependency change. |
| **Conservative structural inference** | A worst-case coexistence or byte term derived from the repository's ownership graph, bounded configuration, and the preceding contracts/facts. | Establishes the repository's structural admission formula, subject to its stated scope. |
| **Allocator-observed supplemental measurement** | API-visible capacity/size observations or platform allocator instrumentation from a concrete build and workload. | Detects regressions and validates examples; never substitutes for the structural formula or proves portability. |

The Lane B tests currently observe Rust-visible capacities, compiled `size_of` values, and retained
generation behavior. They do **not** call a platform `malloc_usable_size` equivalent and therefore
do not report physical allocator block size. If allocator-level instrumentation is added, its
results remain supplemental and must identify allocator, target, compiler, profile, and fixture.

## Version and commit anchors

The lane commits and integrated-root commits have identical stable patch IDs:

| Capability | Lane commit | Integrated root commit | Stable patch ID |
|---|---|---|---|
| B1 book-processing memory closure | `de2123abc3b93e7d1ccfec6cbe1115fbf182a002` | `280fb0f7254da3060b1dca634cff20e141d0d3da` | `b86dd77349c63aea413276cd0d5a84b4fcbcb193` |
| B2 snapshot generations and leases | `366690b6b516ed23ea94e57d6b96dc2eed1bbf02` | `0f9b8cce9c1a5ed20e034554987dd66148e6b07d` | `4b05f63320fbd4d2524ec48a2ccc03cdc329034f` |

The lane exact-head evidence was produced at
`366690b6b516ed23ea94e57d6b96dc2eed1bbf02` with Rust 1.97.0. `Cargo.lock` pins
`arc-swap` 1.9.2 and Tokio 1.52.4. An integrated-root review must still verify the unchanged root
head; patch identity shows that the two Lane B changes were transferred unchanged, but it is not a
substitute for a clean quarter-head gate.

## Primary-source findings

### `Vec` reservation is a lower bound, not exact capacity

**Normative API guarantee.** Rust 1.97 documents that a successful
[`Vec::try_reserve_exact`](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.try_reserve_exact)
provides capacity of at least `len + additional`. It also explicitly allows the allocator to give
the collection more space than requested, so the resulting capacity cannot be assumed to be
minimal. The broader [`Vec` guarantees](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#guarantees)
likewise say that the allocator may return an allocation larger than the exact request and that
`Vec` does not promise a stable ABI or a particular growth strategy.

**Implementation consequence.** Neither provider-book storage nor canonical output construction
uses `try_reserve_exact(n)` as proof that `capacity() == n`. Code that rejected a successful
reservation merely because the allocator exposed additional capacity would reject valid Rust
behavior and would make source availability depend on allocator policy.

### `Box<[T]>` closes the logical element count

**Normative API guarantee.** Rust 1.97 documents that
[`Vec::into_boxed_slice`](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.into_boxed_slice)
discards excess capacity before conversion. The documented example converts the box back to a
vector and observes capacity equal to its length. Conversely, the boxed-slice-to-vector conversion
transfers ownership of the existing heap allocation.

**Conservative structural inference.** A `Box<[T]>` exposes a slice length, not a mutable vector
capacity or growth operation. Market Squawk can therefore make the boxed slot count the fixed
logical backing used by the live state machine. Converting a normalized box back into `Vec<T>`
gives the canonical output an API-visible capacity equal to its length for this documented
conversion path.

This does not imply that the allocator's physical block contains exactly `len * size_of::<T>()`
bytes. The allocator may retain hidden rounding or bookkeeping not represented by the slice length
or a vector's reported capacity.

### `Arc` ownership can retain old generations

**Normative API guarantee.** [`Arc<T>`](https://doc.rust-lang.org/1.97.0/std/sync/struct.Arc.html)
provides shared ownership of one heap allocation. Cloning it creates another owner of the same
allocation, and the inner value remains alive until the last strong owner is destroyed. A full
`Arc` loaded from an `ArcSwap` can therefore keep a superseded publication alive.

[`ArcSwap::load_full`](https://docs.rs/arc-swap/1.9.2/arc_swap/type.ArcSwap.html#method.load_full)
returns another copy of the held reference-counted pointer. Its
[`store`](https://docs.rs/arc-swap/1.9.2/arc_swap/type.ArcSwap.html#method.store) operation replaces
the stored value so subsequent loads yield the new one. These are the pinned `arc-swap` 1.9.2 API
contracts used by the snapshot plane.

**Pinned implementation fact.** At the exact Rust 1.97 compiler commit, private
[`ArcInner<T>`](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/alloc/src/sync.rs#L388-L396)
is `repr(C, align(2))` and contains strong and weak `Atomic<usize>` counters followed by `T`.
Rust 1.97's
[`Atomic<T>` documentation](https://doc.rust-lang.org/1.97.0/std/sync/atomic/struct.Atomic.html)
states that the generic atomic has the same size as its underlying type and alignment equal to its
size. The public `Arc` API does not promise this private layout.

**Conservative structural inference.** For the pinned implementation, Lane B charges two
`usize`-sized counters plus as much as `align_of::<T>() - 1` bytes before the pointee. The padding
term is intentionally a worst case. It must be re-audited on every Rust toolchain change and is not
an `Arc` ABI guarantee.

### Owned semaphore permits bound retention authority

**Normative pinned-crate guarantee.** Tokio 1.52.4 documents that
[`try_acquire_owned`](https://docs.rs/tokio/1.52.4/tokio/sync/struct.Semaphore.html#method.try_acquire_owned)
returns one owned permit and that
[`try_acquire_many_owned`](https://docs.rs/tokio/1.52.4/tokio/sync/struct.Semaphore.html#method.try_acquire_many_owned)
returns one owned permit representing the requested number of permits. The exact crate source
returns those permits on `OwnedSemaphorePermit::drop`; its public API also exposes the number held
through [`num_permits`](https://docs.rs/tokio/1.52.4/tokio/sync/struct.OwnedSemaphorePermit.html#method.num_permits).

**Implementation consequence.** A single-shard lease consumes one permit. An all-shard lease
consumes exactly `shard_count` permits before loading publications. The aggregate lease cannot turn
one permit into `shard_count` uncharged retained generations.

### Checked arithmetic is an overflow signal

**Normative API guarantee.** Rust 1.97's
[`u64::checked_add`](https://doc.rust-lang.org/1.97.0/std/primitive.u64.html#method.checked_add)
and [`u64::checked_mul`](https://doc.rust-lang.org/1.97.0/std/primitive.u64.html#method.checked_mul)
return `None` on overflow. The Lane B formula converts that signal into typed capacity errors rather
than wrapping a structural byte count.

## B1: closed book-processing inventory

The implementation proof is distributed across
[`provider_book/storage.rs`](../../crates/market-squawk-live/src/provider_book/storage.rs),
[`provider_book.rs`](../../crates/market-squawk-live/src/provider_book.rs),
[`processor/event.rs`](../../crates/market-squawk-live/src/processor/event.rs), and
[`runtime/memory.rs`](../../crates/market-squawk-live/src/runtime/memory.rs).

Let:

- `D` be the configured retained depth per side;
- `M` be the conservative maximum book-item count derivable from the admitted message-byte ceiling,
  capped by the decoder hard limit;
- `U = size_of::<Option<UnifiedBookLevel>>()`;
- `N = size_of::<Option<NormalizedChange>>()`;
- `L = size_of::<BookLevel>()`;
- `C = size_of::<BookChange>()`;
- `A` be the pinned structural allocation term for one exact-level `Arc` pointee, including the
  two-counter/padding term described above.

### Persistent storage

**Conservative structural inference.** Each provider book owns active and candidate fixed buffers
for both bids and asks. The fixed boxed backing is therefore:

```text
provider_book_buffer_bytes = 4 * D * U
```

The committed image can retain at most `2 * D` exact-level pointees:

```text
active_exact_bytes = 2 * D * A
```

`ExactProviderLevel` stores both provider decimal lexemes in fixed inline byte arrays bounded by
the domain identifier limit. It does not retain provider `String` capacity behind the `Arc`; this
is what makes `A` a closed structural pointee term rather than another allocator-dependent string
inventory.

These terms are charged as persistent stream ownership. The estimator does not charge them again
as event-temporary memory.

### Per-shard reusable scratch and transaction peak

Every actor constructs one `BookProcessingScratch` before processing. Its fixed boxed backing is:

```text
shard_scratch_bytes = M * N
```

During a transaction, the active image remains committed while the inactive candidate image is
filled. At most `min(2 * D, M)` candidate levels can be newly represented by exact pointees:

```text
candidate_exact_bytes = min(2 * D, M) * A
```

Unchanged levels clone an existing `Arc`, so they share the already charged pointee; changed levels
can allocate new pointees, which the candidate term covers. The `Arc` handle stored in each slot is
already included in `U`.

Canonical snapshot vectors retain both final sides while one side may be in the pre-box and
post-box conversion overlap:

```text
snapshot_canonical_bytes = (min(2 * D, M) + min(D, M)) * L
```

A maximum delta vector may have the pre-normalization and normalized allocations coexist:

```text
delta_canonical_bytes = 2 * M * C
```

The per-shard event-specific peak is therefore:

```text
book_processing_additional =
    shard_scratch_bytes
    + candidate_exact_bytes
    + max(snapshot_canonical_bytes, delta_canonical_bytes)
```

The runtime creates scratch for every shard, including one that currently owns no route, and adds
the transaction-only portion once for every route-owning shard at that shard's maximum configured
route depth. This closes the all-shard concurrency case without multiplying every route by every
shard.

The decoded command itself is excluded from this term because the bounded mailbox byte semaphore
already retains and charges the admitted command and nested allocations. Including it here would be
double counting rather than conservatism.

### Why the tree and rollback assumptions were removed

Before B1, production book state used `BTreeMap` storage and transaction rollback structures. A
portable byte formula for private tree nodes, allocator rounding, and rollback-vector growth would
have depended on implementation details unrelated to the configured market depth. B1 instead
made the production object graph match the desired bound:

- `FixedBuffer<T>` owns `Box<[Option<T>]>` with exactly the configured logical slot count;
- `SideBuffers` owns one active and one reusable candidate buffer;
- a successful transaction swaps the buffers and clears the old image;
- an error or dropped transaction clears only the inactive candidate, leaving the active image
  unchanged;
- normalization uses the shard's preallocated scratch rather than an event-owned rollback vector;
- merge is linear over sorted active levels and normalized changes.

`BTreeMap` remains in property tests as a correctness oracle; it is not production retained state
and is therefore absent from the runtime formula. Likewise, rollback behavior remains as a
semantic requirement, but it is achieved through inactive-buffer isolation rather than a retained
rollback journal.

### Why spare-capacity assumptions were removed

`try_reserve_exact` is allowed to return a larger capacity. B1 therefore follows a successful
reserve with `into_boxed_slice`; fixed storage keeps that box, while canonical event construction
converts the normalized box back into a vector. The memory formula charges the possible conversion
overlap because shrinking may reallocate. It does not assume that the allocator will shrink
in-place, and it does not fail a valid market-data operation merely because the allocator selected
a larger class.

## B2: snapshot generations and reader leases

The implementation proof is in
[`snapshot.rs`](../../crates/market-squawk-live/src/snapshot.rs),
[`snapshot/store.rs`](../../crates/market-squawk-live/src/snapshot/store.rs), and
[`runtime/memory.rs`](../../crates/market-squawk-live/src/runtime/memory.rs).

Let:

- `S` be `shard_count`;
- `R` be the runtime-wide maximum retained-reader permit count;
- `P` be the configured maximum retained bytes of one `ShardSnapshot`, including its pointee and
  nested boxed slices/strings;
- `H(T) = 2 * size_of::<usize>() + align_of::<T>() - 1`, the pinned conservative `Arc` control and
  pre-pointee-padding term.

### Publication generations

**Conservative structural inference.** While a shard replaces its current publication, its new
`Arc<ShardSnapshot>` is allocated before the former current owner is released. An `ArcSwap` guard or
reader may also retain a predecessor. The structural model therefore permits a predecessor and a
successor per concurrently publishing shard. Each official reader permit can retain one additional
distinct old per-shard publication:

```text
publication_count = 2 * S + R
publication_bytes = P + H(ShardSnapshot)
```

`R` is not multiplied by `S`. A single-shard lease uses one permit for one `Arc`; an aggregate
lease first acquires `S` permits and then retains `S` `Arc` values. Public lease APIs return only
borrowed `&ShardSnapshot` views, and the lease types are not cloneable, so callers cannot obtain
unmetered clones of the internal publication owners.

### Reader metadata

A single-shard lease charges its compiled inline `size_of`. An aggregate lease charges:

```text
size_of::<LiveRuntimeSnapshotLease>()
    + S * size_of::<Arc<ShardSnapshot>>()
    + S * size_of::<ShardSnapshotRevision>()
    + max(
        S * size_of::<Arc<ShardSnapshot>>(),
        S * size_of::<ShardSnapshotRevision>()
      )
```

The final `max(...)` is one possible `Vec`-to-box conversion overlap while both final boxed arrays
are retained. For each complete group of `S` permits, the formula selects the larger of one
aggregate lease and `S` individual leases, then charges individual leases for the remainder. This
covers mixed use of the two official read APIs rather than assuming that all readers have the
smaller shape.

The complete additional snapshot term is:

```text
snapshot_additional =
    publication_count * publication_bytes
    + reader_metadata_peak_bytes(R, S)
```

The `Arc` handle bytes inside leases are metadata; the publication term separately charges the
shared allocation and its pinned control-block term. This separation avoids both omission and
double counting.

## Checked arithmetic and fail-closed construction

**Normative API guarantee plus implementation proof.** B1 and B2 use `checked_add`, `checked_mul`,
checked division/remainder, checked subtraction, and fallible integer conversion throughout the
formula. Overflow maps to `LiveRuntimeConfigError::CapacityOverflow` or
`SnapshotReadError::CapacityOverflow`.

The snapshot plane validates the complete reader-metadata shape before allocating the plane, checks
`maximum_readers` against Tokio's `Semaphore::MAX_PERMITS`, and repeats the aggregate shape check
before building aggregate-reader vectors. Fallible reservations map allocation failure to typed
errors. Actors are not started behind an overflowed or invalid structural estimate.

This proves fail-closed behavior for the modeled arithmetic. It does not turn `size_of` into a
cross-target constant and does not promise that the system allocator can never fail below a
configured logical ceiling.

## What the tests prove

The implementation-specific proof includes the following deterministic checks:

- [`provider_book/tests.rs`](../../crates/market-squawk-live/src/provider_book/tests.rs) runs the
  maximum derived delta shape concurrently on four shard workers and checks the observed Rust-level
  scratch, candidate-pointee, and canonical-vector shape against the structural peak. Property
  tests compare the linear merge to a `BTreeMap` reference implementation.
- [`runtime/tests/config_memory.rs`](../../crates/market-squawk-live/src/runtime/tests/config_memory.rs)
  proves that the former wire-byte multiplier undercharges the structural delta peak, checks exact
  ceiling acceptance and one-byte-under rejection, verifies `2 * S + R`, verifies all-shard
  coexistence, and checks route-order independence.
- [`snapshot/store/tests.rs`](../../crates/market-squawk-live/src/snapshot/store/tests.rs) proves one
  permit per retained shard generation, all-shard permit weighting, permit restoration on drop,
  retention of multiple distinct old generations while publication continues, reader exhaustion,
  and modeled metadata greater than or equal to the Rust-visible observed metadata.

The Lane B handoff ran formatting, the `market-squawk-live` all-target/all-feature locked test and
strict-Clippy gates, a locked release build, and `git diff --check` on the clean exact lane head.
Those results support the named commit only. Quarter approval still requires the repository's full
clean, unchanged, exact-root-head verification.

These tests establish object-count, ownership, formula-boundary, and target-compiled structural
properties. They do not observe hidden allocator metadata or prove an operating-system RSS bound.

## Why allocator observation cannot replace the formula

An allocator trace covers one allocator, target, build profile, schedule, and fixture. Even a large
stress run may miss the precise coexistence of all publishing shards, old reader generations,
candidate exact pointees, and a canonical conversion allocation. A low observed peak therefore
cannot justify deleting a structurally reachable term.

Conversely, the formula establishes a closed Rust object inventory but cannot derive undocumented
allocator size classes, per-allocation headers, quarantine behavior, fragmentation, or resident
page accounting. Allocator instrumentation is valuable as a regression and calibration layer:

1. the structural formula proves which supported objects and generations may coexist;
2. compiled `size_of`, capacity, and lifecycle tests verify that the implementation still matches
   the formula;
3. allocator/RSS measurements quantify platform-specific overhead and inform operational headroom.

The layers answer different questions. A measurement does not replace the formula, and the formula
does not authorize a universal physical-byte claim.

## Platform- and allocator-dependent remainder

The following remain target, compiler, dependency, or allocator dependent:

- `usize` width and primitive alignment are target-specific; the Rust Reference explicitly notes
  that `usize` is address-sized and primitive alignment is platform-specific.
- Repository structs and enums use Rust's default representation unless annotated; the Reference
  gives only limited soundness guarantees for that representation and no general stable ABI.
- `size_of::<Option<UnifiedBookLevel>>()`, `size_of::<NormalizedChange>()`, lease inline sizes, and
  revision-array element sizes are therefore values of the actual compiled target, not portable
  constants.
- `ArcInner` is private Rust implementation structure. Its two counters and field order are pinned
  facts for compiler commit `2d8144b78`, not public guarantees for a later toolchain.
- `ArcSwap` protection strategy and Tokio permit representation may change when their pinned
  versions change, even if their public ownership semantics remain similar.
- A `Vec` or boxed slice's API-visible capacity/length does not expose allocator block rounding,
  allocator metadata, fragmentation, cache/quarantine retention, virtual-memory mappings, or RSS.
- Optimizer choices and thread schedules can reduce an observed peak, but the admission model keeps
  structurally possible coexistence terms rather than relying on that reduction.

Accordingly, `maximum_runtime_bytes` is a structural admission ceiling for the modeled live
ownership graph. A deployment that requires a hard physical-process or RSS limit must additionally
document its target allocator and apply measured operational headroom or an external memory limit;
it must not reinterpret this formula as allocator-independent byte exactness.

## Revalidation gates

Re-run the source and implementation audit whenever any of these changes:

- `rust-toolchain.toml`, target triple, compiler code-generation options, or Rust representation
  annotations;
- the pinned `arc-swap` or Tokio version in `Cargo.lock`;
- provider-book slot types, exact-level pointee types, canonical domain event types, snapshot DTOs,
  lease types, or revision metadata;
- active/candidate ownership, scratch ownership, canonical conversion order, publication order, or
  reader-permit weighting;
- a public API begins returning cloneable internal `Arc` owners;
- a new allocator or operating system is included in physical-memory acceptance claims.

The gate must recompute target `size_of`/`align_of` values, inspect the pinned `ArcInner` source,
run exact-ceiling and one-under tests, exercise maximum concurrent generation retention, and record
allocator-specific measurements separately from the structural proof.

## Primary sources

All sources were accessed on 2026-07-16.

- [Rust 1.97 `Vec` guarantees](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#guarantees)
- [Rust 1.97 `Vec::try_reserve_exact`](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.try_reserve_exact)
- [Rust 1.97 `Vec::into_boxed_slice`](https://doc.rust-lang.org/1.97.0/std/vec/struct.Vec.html#method.into_boxed_slice)
- [Rust 1.97 `Arc`](https://doc.rust-lang.org/1.97.0/std/sync/struct.Arc.html)
- [Rust 1.97 `Atomic<T>`](https://doc.rust-lang.org/1.97.0/std/sync/atomic/struct.Atomic.html)
- [Rust 1.97 checked `u64` addition](https://doc.rust-lang.org/1.97.0/std/primitive.u64.html#method.checked_add)
- [Rust 1.97 checked `u64` multiplication](https://doc.rust-lang.org/1.97.0/std/primitive.u64.html#method.checked_mul)
- [Rust 1.97 Reference: type layout](https://doc.rust-lang.org/1.97.0/reference/type-layout.html)
- [Rust `ArcInner` at compiler commit `2d8144b78`](https://github.com/rust-lang/rust/blob/2d8144b7880597b6e6d3dfd63a9a9efae3f533d3/library/alloc/src/sync.rs#L388-L396)
- [`arc-swap` 1.9.2 `ArcSwap` API](https://docs.rs/arc-swap/1.9.2/arc_swap/type.ArcSwap.html)
- [Tokio 1.52.4 `Semaphore` API](https://docs.rs/tokio/1.52.4/tokio/sync/struct.Semaphore.html)
- [Tokio 1.52.4 `OwnedSemaphorePermit` API](https://docs.rs/tokio/1.52.4/tokio/sync/struct.OwnedSemaphorePermit.html)
