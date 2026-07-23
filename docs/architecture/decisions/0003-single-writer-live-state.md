# 0003: Use Deterministic Single-Writer Live State

Status: Accepted

Decision date: 2026-07-16

## Context

Order books, sequence state, rolling features, strategies, and live authority must change in one
well-defined order for each venue/instrument route. Shared mutable maps and fine-grained locks would
make ordering, rollback, capability revocation, and event-to-action latency dependent on thread
interleavings. One task per instrument would avoid some locking but would create unbounded task and
queue topology as coverage grows.

The system also needs parallelism across instruments, bounded memory, stable routing across
processes, and immutable control-plane reads that do not block live mutation.

## Decision

Stable instrument shards provide deterministic single-writer ownership of mutable live state.

Routing V1 hashes the frozen domain-separated byte encoding of `(venue_id, instrument_id)` and takes
the result modulo the configured nonzero shard count. One supervised actor owns every route mapped
to its shard, including processors, generation registries, order books, feature state, strategy
hooks, and local action state. No mutable reference escapes the actor.

Ingress and registration queues are count- and byte-bounded. The actor applies a message,
qualifies committed state, runs feature/strategy/action handoff, and then publishes a complete
immutable shard snapshot. Snapshot readers receive bounded leases and a per-shard revision vector;
cross-shard reads do not claim one fabricated globally atomic instant. A shard-count change is a
restart-time rebuild decision, not live remapping.

## Consequences

- All state transitions for one route are serialized without route-level shared-state locks.
- Shards execute independently, allowing bounded parallelism across routes.
- Mailbox saturation or closure is an integrity event that invalidates affected authority; critical
  messages are not silently dropped.
- Actor exit invalidates shard, runtime, and generation leases before ownership disappears.
- Immutable snapshot publication decouples bounded readers from the writer.
- Hot-route skew can load one shard more heavily than others and must be addressed through measured
  shard count and route placement, not dynamic ownership changes.
- Changing shard count requires explicit restart and state reconstruction.

## Rejected alternatives

- A concurrent mutable order-book map protected by shared locks.
- Multiple writers applying the same instrument stream.
- One unbounded task or queue per instrument.
- Unbounded channels with best-effort loss under burst load.
- Dynamic live resharding that changes ownership while capabilities remain outstanding.
- Treating an aggregate snapshot as a globally atomic cross-shard state.

## Related architecture

- [Live execution plane](../live-execution-plane.md)
- [Building blocks](../building-blocks.md)
- [Quality attributes](../quality-attributes.md)
- [ADR 0002: Evidence-derived execution quality](0002-evidence-derived-execution-quality.md)

## Evidence and sources

- [Frozen shard routing](../../../crates/market-squawk-live/src/sharding.rs),
  [single-writer actor](../../../crates/market-squawk-live/src/runtime/actor.rs), and
  [bounded ingress](../../../crates/market-squawk-live/src/runtime/admission.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Immutable bounded snapshots](../../../crates/market-squawk-live/src/snapshot.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Tokio bounded MPSC documentation](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html),
  reviewed 2026-07-23, documents bounded multi-producer/single-consumer channels and backpressure.
- [ArcSwap documentation](https://docs.rs/arc-swap/latest/arc_swap/), reviewed 2026-07-23,
  documents atomic publication of immutable `Arc` values for read-mostly access.
