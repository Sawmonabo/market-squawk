# 0001: Separate Live and Research Planes

Status: Accepted

Decision date: 2026-07-16

## Context

Immediate market action and historical research have different authority, latency, time, storage,
and recovery requirements. A live observation must establish a current source session, stream
integrity, freshness, instrument state, and process-local execution capability without analytical
I/O. Research must preserve revisions, availability, effective time, immutable dataset generations,
and point-in-time lineage. Historical data may come from sources unrelated to the live feed.

Forcing both workloads through one event schema or one pipeline would either place storage and
research work in the event-to-action path or discard the temporal and revision semantics required
for defensible research.

## Decision

The live execution plane and research data plane are independent pipelines over shared domain
contracts.

The shared domain owns stable instrument identity, money and precision types, classification,
time, provenance building blocks, schemas, and pure mathematical kernels where semantics are
identical. The live plane owns protocol decoding, current-source qualification, deterministic
single-writer state, online features, strategy/model action handoff, and live capability issuance.
The research plane owns extraction, canonical research observations, Arrow exchange, manifest-bound
Parquet generations, DataFusion/Python analysis, revisions, and point-in-time selection.

Historical datasets do not have to originate from, reproduce, or mirror the live feed. Replay
remains diagnostic tooling and is not an architectural dependency between the planes.

## Consequences

- Live and extraction sources use distinct contracts and adapters.
- Live and research provenance retain different time and authority semantics.
- Filings, macro observations, portfolios, and market events are not forced into a universal event
  type.
- Research storage, Python, SQL, MCP, and filesystem work remain outside the live event-to-action
  path.
- Shared mathematical kernels require explicit semantic compatibility; sharing a type does not
  imply sharing a source or dataset.
- Cross-plane use requires a bounded, typed publication or evidence handoff rather than access to
  another plane's mutable state.

## Rejected alternatives

- One universal event type for live ticks, filings, macro data, and portfolio records.
- A single pipeline in which all live events are synchronously persisted before strategy action.
- Treating the live feed as the mandatory historical source of truth.
- Making replay a prerequisite for research, backtesting, or production execution.
- Sharing mutable runtime state between analytical queries and live shards.

## Related architecture

- [Architecture overview](../overview.md)
- [Live execution plane](../live-execution-plane.md)
- [Research data plane](../research-data-plane.md)
- [Data, time, and provenance](../data-time-and-provenance.md)

## Evidence and sources

- [Canonical live provenance](../../../crates/market-squawk-domain/src/provenance/live.rs) and
  [research provenance](../../../crates/market-squawk-domain/src/provenance/research.rs), reviewed
  at `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Live source API](../../../crates/market-squawk-sources/src/live.rs) and
  [extraction source API](../../../crates/market-squawk-sources/src/extraction/mod.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Live runtime](../../../crates/market-squawk-live/src/runtime.rs) and
  [research ingestion](../../../crates/market-squawk-data/src/ingest.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [W3C PROV-DM](https://www.w3.org/TR/prov-dm/), reviewed 2026-07-23, provides a
  domain-independent model for entities, activities, derivations, revisions, and responsibility
  while allowing domain-specific extensions.
