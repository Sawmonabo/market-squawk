# Q2 Task 8 deterministic live-runtime implementation report

## Document control

- Date: 2026-07-16
- Scope: deterministic sharding, bounded live admission, single-writer actors, immutable snapshots,
  supervised lifecycle, and application-boundary quarantine
- Production implementation commits: `307cef7`, `902d94f`, `c7404ac`, `2b3aa57`, `2cc8094`,
  and `09f2b33`
- Deterministic test commits at report creation: `069ad58`, `5ea3bae`, `1bebffe`, `cc3c89d`,
  `5c2b08a`, `71f3db7`, `849ee8b`, `0a97160`, and `c1faab0`
- Status: implemented for the Task 8 contract; quarter-level acceptance remains the exact
  integrated Tasks 5-8 checkpoint

This report records working production behavior and deterministic evidence. It does not count
interfaces, synthetic fixtures, or the diagnostic compatibility engine as production live
capability.

## Outcome

The live crate now owns a production runtime in which an exact current Task 5 source allocation is
bound to one deterministic `(venue, instrument)` route before feed data is accepted. Each shard is
a single writer over its route processors. Ingress is nonblocking and bounded simultaneously by
message count, retained bytes, and individual message bytes. Any admission failure invalidates the
exact generation before returning. Actors publish authority-free immutable snapshots through a
private `ArcSwap` plane and are supervised through complete startup, bounded shutdown, and clean
replacement.

Task 8 deliberately makes the post-apply decision `NoStrategy`. It does not mint an unused
execution capability or pretend that Tasks 9 and 10 features, strategy, risk, or execution have
already been integrated. Those consumers attach at the actor's already-tested revalidation seams.

## Frozen deterministic routing contract

Routing V1 hashes this unambiguous byte sequence:

```text
ASCII "MSQKSHARD"
0x01
venue UTF-8 byte length as big-endian u16
venue UTF-8 bytes
InstrumentId UUID network-order bytes
```

The algorithm is FNV-1a 64-bit with fixed offset `0xcbf29ce484222325` and fixed prime
`0x00000100000001b3`. The frozen vector for venue `coinbase` and instrument
`018f0000-0000-7000-8000-000000000001` is hash `0x28edee9cb1852659`, which routes to shard 9 of
16. The implementation never relies on `DefaultHasher`, process-random map state, native endian,
display text, delimiter concatenation, Unicode normalization, or a dependency-defined hash.

## Production architecture

### Admission and ownership

`LiveRuntime::start` validates every capacity and route, calculates a checked conservative peak
memory bound, constructs every snapshot cell, starts exactly the configured shard actors, and
waits concurrently for every actor to publish its initial `Ready` snapshot. Ingress is not returned
until the complete readiness barrier succeeds and runtime/shard liveness is rechecked.

The public runtime ingress performs only a bounded, cancellable pre-feed registration handshake.
It has no unbound publish method. A successful handshake returns `BoundShardIngress`, which retains
the exact route, current source allocation, runtime lease, shard lease, and actor-minted generation
admission. Its `try_publish` path:

1. revalidates runtime, shard, source, and generation;
2. rejects route or source-allocation transplants;
3. computes private checked deep retained bytes;
4. rejects commands above the configured per-message ceiling;
5. attempts exact byte permits without waiting;
6. attempts the bounded count mailbox without waiting; and
7. transfers the batch, generation admission, and owned byte permit together.

Every failure after generation binding invalidates that exact generation synchronously before the
error returns. Health events are bounded best-effort mirrors and never perform the safety
transition.

### Actor processing and authority

The stable V1 route maps each `(venue, instrument)` to exactly one actor. The actor owns its
processors, generation registries, order books, revisions, snapshot construction, and future
feature/strategy state. No live mutable state is shared between actors.

For each command the actor rechecks current runtime/shard/source/generation authority before
calling the processor. The processor applies observations transactionally and the actor performs
two explicit applied-current rechecks: one at the future feature boundary and one at the future
strategy/issuer boundary. Task 8 then records `NoStrategy`. Any processing rejection invalidates
the exact admission. Irrecoverable capacity, revision, clock, nonce, allocation, or authority
invariant failures terminate the actor; normal source/generation/data-integrity rejection remains
route-local and cannot authorize action.

Actor exit invalidates shard liveness, runtime liveness, all route generations, and processor
authority before emitting one terminal health event and returning. Marker-typed leases prevent
generation, status, shard, runtime, instrument-revision, and status-revision dimensions from being
exchanged accidentally. Owner `Drop` is a Release invalidation fallback, not a reactivation path.

### Immutable snapshot plane

Each actor constructs a complete bounded DTO from one committed owner state after the action
decision boundary. The snapshot contains routing version/count, runtime incarnation, shard ID,
snapshot revision, lifecycle, exact publication/evaluation times, stream/status revisions,
generation and health information, book depth, provenance timestamps, and per-dimension
completeness/truncation metadata.

Publication uses crate-private `ArcSwap` cells. The public reader receives an authority-free lease,
never a cell, mutable state, source lease, issuer, nonce, or capability. Single-shard reads retain
one reader permit; aggregate reads retain one permit per returned shard generation. Aggregate views
return a deterministically sorted per-shard revision vector and do not fabricate a global `as_of`.
Slow readers can exhaust the configured reader-retention budget, but they cannot block publication.
Snapshot-change notifications are separate, keyed, bounded, coalescing hints.

### Lifecycle and memory bounds

Startup failure invalidates the runtime, closes channels and snapshots, aborts, and awaits every
partially started task. Graceful shutdown invalidates authority before draining. If the configured
deadline expires, every remaining task is aborted and awaited; no task is detached. Replacement
starts a fresh incarnation only after the former runtime reports complete shutdown.

The startup memory model charges independent bounded terms for actor/process state, route/source/
nonce state, order-book depth, count and byte mailboxes, the in-progress candidate, snapshot
construction and current publications, retained reader generations, control channels, health
events, and snapshot notifications. Configuration rejects zero values, incompatible limits,
checked-arithmetic overflow, Tokio permit incompatibility, invalid routes, duplicate routes, or an
estimate above the explicit ceiling before allocation.

## Application composition boundary

`apps/market-squawk/src/live_runtime.rs` is the production application owner. It exposes checked
startup, route-bound generation binding, immutable snapshot reads, runtime incarnation and memory
metrics, bounded health/notification polling, clean replacement, and explicit shutdown. It does
not expose unbound publication, live actor senders, snapshot cells, or authority internals.

The previous application `Engine` is renamed `DiagnosticEngine`; its module-level documentation
states that it cannot accept `CurrentDecodedProviderBatch`, mint live authority, or enter the Task
8 runtime. Its event types are private and re-exported only under explicit `Diagnostic*` names.
Replay, mock capture, the existing compatibility MCP tools, and the historical paper calculation
remain runnable but cannot be promoted into production ingress. CLI and MCP descriptions no longer
imply that this compatibility calculation controls production execution.

The deletion trigger is concrete: Task 11 adapters must produce receipt-validated current batches
after pre-feed binding, and Task 13 application services must consume Task 8 snapshots. At that
point the diagnostic engine and app-local event/book/quality path are deleted rather than converted
into execution authority.

## Deterministic evidence

The focused Task 8 suite covers:

- golden routing vectors, byte encoding, count bounds, Unicode byte length, and delimiter
  ambiguity;
- zero, overflow, partition, route, queue, message, snapshot, and memory configuration bounds;
- exact generation registration, refresh, rollover, transplant rejection, and one-way invalidation;
- count-full, byte-full, overweight, closed, retained-cost overflow, and permit-release admission;
- snapshot successor identity, isolation, truncation, deterministic ordering, reader accounting,
  notification coalescing, close behavior, and revision exhaustion;
- complete readiness, unordered readiness failure, partial-start cleanup, unexpected actor exit,
  graceful shutdown, deadline abort-and-await, replacement, and `Drop` fallback; and
- application compile/runtime separation between diagnostic events and production bound ingress.

The exact combined Task 8 live/application gate passed before report creation:

```text
cargo fmt --all --check
cargo clippy -p market-squawk-live -p market-squawk \
  --all-targets --all-features --locked -- -D warnings
cargo test -p market-squawk-live -p market-squawk --all-features --locked
cargo build -p market-squawk-live -p market-squawk \
  --all-features --release --locked
git diff --check
```

Every command exited zero against the regenerated integrated lock. The live library contains 74
passing unit tests, including 10 snapshot-store, 9 admission, and 8 lifecycle regressions. The
integration suite additionally passes compile-time authority privacy, book/property, official
Kraken checksum, exact conversion, real-registry overflow, sequence, 15 public sharding/config, and
state-machine tests. The complete application suite passes, including all 5 diagnostic/live-runtime
composition tests. The full-workspace quarter gate and grouped review still belong to the exact
integrated Tasks 5-8 checkpoint.

No throughput or latency claim is made. Task 8 supplies bounded runtime mechanics; the specified
100,000 events/s and sub-millisecond warmed p99 targets still require the documented Task 14
benchmark run on the integrated production pipeline.

## Persisted design references

The implementation was checked against the locally locked crate source. These official references
record the public behavior that informed the design:

- [Tokio 1.52.4 bounded `mpsc`](https://docs.rs/tokio/1.52.4/tokio/sync/mpsc/index.html) for
  bounded count admission and nonblocking `try_send`.
- [Tokio 1.52.4 `Semaphore`](https://docs.rs/tokio/1.52.4/tokio/sync/struct.Semaphore.html) for
  owned exact byte/reader permits and explicit closure behavior.
- [Tokio 1.52.4 `JoinSet`](https://docs.rs/tokio/1.52.4/tokio/task/struct.JoinSet.html) for
  supervised task ownership; the implementation explicitly aborts and then awaits tasks instead
  of detaching them.
- [`ArcSwap` 1.9.2](https://docs.rs/arc-swap/1.9.2/arc_swap/type.ArcSwap.html) for lock-free
  replacement of the current immutable snapshot generation.
- [Rust atomic ordering](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html) for the
  Release invalidation and Acquire validation boundary used by one-way authority leases.

## Remaining integrated-quarter work

These are not Task 8 claims and are not optional release exclusions:

- Task 9 attaches bounded online feature state at the actor's first applied-current recheck.
- Task 10 attaches typed strategy, issue/consume, risk, and dispatch rechecks at the second seam.
- Task 11 replaces diagnostic Coinbase ingestion with production Coinbase/Kraken adapters that
  emit exact current batches and use pre-feed binding.
- Task 12 adds live-kernel benchmarks and fuzz targets.
- Task 13 moves CLI/MCP live reads to shared bounded application services and completes the
  diagnostic-engine deletion trigger.
- Task 14 records integrated throughput, latency percentiles, sustained memory, and release
  hardening evidence.

The prohibited evasion requests remain excluded: there is no identity/account rotation, TLS or
browser fingerprint concealment, CAPTCHA bypass, blocking-evasion proxy rotation, or distributed
quota evasion in this implementation or its extension surfaces.
