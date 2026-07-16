# Q2 Task 7 — Transactional Live State and Current Authority

Date: 2026-07-16

## Scope delivered

This lane implements the first production live-processing boundary over Task 5 current-source
authority. It does not bridge the legacy app replay/diagnostic order book or quality helpers into
execution authority. Those compatibility paths remain non-authoritative until the later adapter
and service tasks emit and consume real current batches and Task 8 snapshots.

- Scaled-integer price-level books with strict snapshot ordering, bounded depth, delete-on-zero,
  crossed-book rejection, and message-atomic mutations.
- Exact provider decimal lexeme retention for venue checksum rules.
- Closed Kraken WebSocket v2 CRC32 profile resolution and streaming checksum canonicalization.
- Full-key instrument stream ownership across source, venue, instrument, product, and channel.
- Sequence, snapshot, checksum, timing, precision, coverage, authorization, trading-status,
  capture, and stream-integrity qualification into the Task 4 relational evidence model.
- Versioned fixed-size assessment identity and execution digest bound to the exact Task 5 frame
  ordinal and committed stream revision as well as the full live evidence binding.
- Candidate-and-commit stream processing. Incremental deltas use a fail-safe RAII rollback guard;
  snapshots are built off-side. Sequence, book, revision, snapshot origin, status, and provenance
  remain at the exact last committed state on rejection.
- Marker-typed generation, shard, runtime, trading-status, stream-revision, and status-revision
  authority dimensions, preventing substitution at compile time.
- Non-cloneable, non-serializable, single-use `LiveExecutionCapability` issuance with fixed-capacity
  nonce tracking and independent source/generation/shard/runtime/status/revision/deadline checks.
- Transactional cross-channel trading-status rotation keyed by source/venue/instrument.
  Capability checks use allocation-local status revision leases, while diagnostic snapshots expose
  a separate checked monotonic allocation version so Active→Halted→Active publishes revisions
  1→2→3 and overflow fails closed without changing last-good state.
- A bounded pre-feed `GenerationAuthorityRegistry` that accepts only opaque Task 5 leases, reuses
  one invalidator across health refreshes, replaces strictly newer same-registry generations,
  rejects equal-visible-identity registry transplants, and invalidates ingress in O(1).
- Task 8-owned marker-typed shard/runtime liveness injection rather than per-instrument owners.
- A deterministic, explicitly limited snapshot seed with requested/output dimensions, exact
  completeness flags, conservative checked retained-byte charging, status join, source deadline,
  health epoch, distinct source/receive/evaluation timestamps, state/snapshot revisions, and no
  authority or lease exposure.
- Task 5 `ValidatedCurrentSourceAuthority::try_current_lease(at)` for pre-feed registration,
  including exact registry lineage, current health epoch, capture generation, and inclusive
  source deadline. Dropping the sole authoritative registry synchronously invalidates every active
  session and capture generation, so retained lease, raw-frame, and capture handles cannot outlive
  their authority owner.

## Integrity and performance decisions

The delta path does not clone a retained book. It allocates rollback journals proportional to the
decoded message (`2 * change_count` upper bound), retains the exact provider and scaled candidates
under one fail-safe guard, streams checksum bytes directly into CRC32, and streams canonical book
state directly into SHA-256. The current canonical digest remains O(retained depth); no benchmark
claim is made before Task 8/Task 15 measurements.

The Kraken implementation follows the official WebSocket v2 checksum ordering and decimal
canonicalization guidance:

- <https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2>

## Verification evidence

Final lane verification completed successfully:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --all-features --locked`
- `cargo build --workspace --all-features --release --locked`

The final live library suite contains 31 passing unit tests, including 11 processor authority and
transaction tests and 5 bounded snapshot tests. The source registry suite contains 8 passing
authority integration tests, including authoritative-registry Drop invalidation.

Focused gates also completed during implementation:

- `cargo clippy -p market-squawk-live --all-targets --all-features --locked -- -D warnings`
- `cargo clippy -p market-squawk-sources --all-targets --all-features --locked -- -D warnings`
- `cargo test -p market-squawk-sources --test registry_authority --locked`
- `cargo test -p market-squawk-sources --all-features --locked`
- `cargo test -p market-squawk-live --all-features --locked`
- `cargo test -p market-squawk-live --test authority_privacy --locked`
- `cargo test -p market-squawk-live --test book_properties --locked`
- `cargo build -p market-squawk-live --all-features --release --locked`

The deterministic coverage includes fixed-seed book properties, exact financial conversion,
Kraken official checksum vectors, sequence and state transitions, capture/current lease races,
assessment/provenance/deadline relations, identity collision resistance, marker-type separation,
trybuild opacity/non-Clone/non-Serde guarantees, rollback-journal scaling, streaming digest golden
equivalence, and corporate-action canonical-field binding. Processor-level generation, status,
snapshot, issuance, consumption, rollback, and fresh-clock races are recorded by the private
processor suite in this commit.

`Cargo.lock` was regenerated locally only to run `--locked` gates because the lane base lockfile
predated the integrated workspace dependency graph. It is deliberately excluded from both Task 7
commits; the root integration lane owns the sole lockfile update and full-workspace locked gates.

## Task 8 contract handoff

Task 8 should construct one marker-typed runtime-incarnation owner per live runtime and one
marker-typed shard-liveness owner per actor. It injects their validation leases through
`ProcessorLivenessBinding`, owns their positive invalidation, and calls the processor's sealed-clock
`validate_applied_current` before feature or strategy evaluation.

Before a producer opens or feeds a bounded queue, the source supervisor obtains a Task 5 current
lease with `try_current_lease(at)` and binds it through `GenerationAuthorityRegistry::bind_current`.
The returned `GenerationAdmission` is retained by the producer and accompanies intact current
batches. Any queue full/closed/overweight/checked-cost rejection synchronously calls
`invalidate_on_admission_failure`; the per-message path performs no registry scan or lock.

Task 8 publishes immutable views only from `snapshot_seed(ProcessorSnapshotLimits)`. It must retain
the seed's requested/output counts, depth/count dimensions, completeness flags, and retained-byte
charge rather than reconstructing private live-state semantics.
