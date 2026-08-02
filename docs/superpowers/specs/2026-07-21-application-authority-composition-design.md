# Application Authority Composition Design

Status: approved design for Quarter 1 of 4 remediation  
Audit base: `ed137b39aea95c20cb1b8adb80786070f7671ef5`

## Outcome

Seal every live adapter generation to one registry allocation, make Kraken a real selectable
production source through the canonical capture/live/risk/paper graph, and make the shipping stdio
MCP command use the hardened transport with durable audit and controlled artifact ownership.

The existing `DirectUnverified` ceilings remain unchanged. This remediation adds no execution
eligibility.

## Exact-generation authority

`AuthoritativeSourceRegistry` is the only constructor of a non-clone, non-serializable
`LiveSourceGeneration`. The registry validates the exact `CurrentSourceSession`, internally issues
the sole raw-frame factory, and binds the resulting capability to the session lease, capture lease,
registry allocation, coordinated provider budget, source, metadata revision, session identifier,
and connection generation. Callers cannot assemble those pieces.

An adapter consumes `LiveSourceGeneration` at construction and consumes its one-use start authority
before endpoint authorization, budget acquisition, or network I/O. Start revalidates session
currentness and healthy capture admission. The adapter owns the raw-frame factory; the
`LiveMarketSource::run` signature accepts only a sink and cancellation token, so a successor or
cross-registry factory cannot be substituted later. Rollover, registry drop, capture degradation,
or a second run fails closed.

## Provider composition

The application retains one production supervisor and one capture-first sink. A typed sealed
provider profile selects Coinbase or Kraken and supplies only provider-specific metadata,
configuration, decoder, subscription identity, and source construction. Registry, capture,
health, live ingress, risk, dispatch, paper execution, reconnect, and shutdown remain shared.

Kraken configuration is explicit and bounded: exact production endpoint, public-interface rights
evidence, one enumerated Kraken venue symbol per source instance, checked instrument definition,
book depth, message ceiling, freshness, and shared budget policy. The CLI selects the provider with
a typed enum; it cannot supply an endpoint. A test-support connector exists only in debug builds
with the explicit feature and still enters the public source `run` path after endpoint-policy and
generation-authority validation.

The deterministic vertical starts a local WebSocket, application composition, capture writer,
Kraken decoder, canonical live ingress, risk hook, dispatcher, and paper ledger. It proves the
Kraken observation reaches canonical pre-authority risk as `DirectUnverified`, while approval,
dispatch, and paper mutation counts remain zero.

## Hardened MCP composition

The shipping `market-squawk mcp` command constructs transport-neutral `ToolServices` and serves
them only through `market-squawk-mcp::McpServer`. The legacy app-local JSON-RPC server is removed
from reachable production composition.

The application supplies a bounded durable audit sink and a capability-confined artifact
repository rooted in `LocalPaths::artifacts()`. Artifacts are content-addressed, size-bounded,
staged without following links, fsynced, atomically installed without overwrite, parent-fsynced,
and returned only as opaque references. Existing identical content is accepted only after complete
digest and length verification.

For a mutating tool, admission and terminal audit capacity are both reserved before calling the
service. Admission persistence completes before the effect. The terminal reservation is committed
after the service reaches its authoritative outcome, independently of stdout publication. Output
failure records separate output-unavailable evidence and cannot erase mutation completion.

The isolated SDK runs on an explicitly owned thread. Cooperative cancellation is followed by a
bounded join deadline. A timed-out thread is transferred to a pre-reserved bounded reaper that
retains and joins it; application shutdown drains the reaper. No code claims that aborting a Tokio
blocking-task handle stops its thread.

## Failure and shutdown rules

- Invalid or stale generation authority performs no budget or network operation.
- Capture remains before decode; capture failure quarantines the generation.
- Heartbeats update connection health only, never market freshness.
- Provider checksum, snapshot, sequence, and resynchronization policies remain provider-specific.
- MCP audit admission/reservation failure rejects mutation before the effect.
- Artifact or stdout failure never reports a successful publication and never erases terminal
  mutation evidence.
- Shutdown cancels source/MCP work, joins owned tasks within configured deadlines, and retains any
  still-running SDK thread in the bounded reaper until joined.

## Thin verification

1. One source-authority test covers stale generation, successor rollover, and same-valued
   cross-registry transplant rejection at the sole mint boundary.
2. One causal test per live adapter proves stale authority is rejected before budget/network work.
3. One Kraken local-WebSocket vertical proves capture-to-risk delivery and zero
   approval/dispatch/paper mutation.
4. One MCP lifecycle test proves pre-reserved mutation audit survives output failure and the SDK
   worker is retained/reaped.
5. The Coinbase fixture test validates authoritative URL, retrieval date, derivation, protocol
   revision, terms reference, and immutable fixture digests.

Focused package gates precede the clean exact-head workspace gate. Cargo execution remains
serialized through the integration owner.
