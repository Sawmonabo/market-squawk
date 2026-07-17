# Market Squawk Q3 Production Design

**Date:** 2026-07-16
**Base commit:** `834674aa40198656b4486c4c64dec1fa788eae29`
**Status:** Proposed for Q3 execution after Q2 approval
**Supersedes:** Tasks 9–12 of
`docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md`

## Purpose

This proposed design closes the production-boundary gaps between the existing Q2
live/source/platform
foundation and a complete Q3 live decision and paper-execution pipeline. It replaces the former
Tasks 9–12 where those tasks would underbuild the product specification, contradict existing
authority boundaries, or overstate diagnostic compatibility as production capability.

The Q3 result must provide:

- Every required live feature, with bounded state and explicit validity.
- Authoritative account-aware risk and one-time execution authorization.
- Typed decoder outcomes for data, control, ignored, resynchronization, and quarantine frames.
- A working production Coinbase source composition with an honest `DirectUnverified` ceiling.
- Realistic paper execution with fees, deterministic seeded latency, slippage, depth-aware partial
  fills, order states, cancellation races, balances, positions, reconciliation, and audit.
- An enforced dependency DAG and peak-memory model covering all new queues and retained state.

This design does not claim that Coinbase data is eligible for automated action. Execution-authority
tests use a test-only synthetic source through the real registry and runtime contracts. Synthetic
sources remain absent from production registration.

## Existing evidence and reasons for supersession

At the base commit, the actual workspace graph is:

```text
domain
platform  -> domain
sources   -> domain, platform
live      -> domain, sources
app       -> domain, live, platform, sources
```

The current live actor commits an observation and then contains comments for the future feature and
action hooks at `crates/market-squawk-live/src/runtime/actor.rs:508-510`. The live processor is the
sole owner of capability issuance and consumption at
`crates/market-squawk-live/src/processor.rs:358-370`; the underlying `AuthorityGate` is crate-private
at `crates/market-squawk-live/src/authority.rs:279-419`. A direct call from `live` to a future
`execution` crate would create a `live -> execution -> live` cycle.

`MarketDecoder::decode` currently returns a nonempty `DecodedProviderBatch` at
`crates/market-squawk-sources/src/decoder/batch.rs:241-250`. This cannot honestly represent
subscription acknowledgements, ping/pong, provider heartbeats, documented forward-compatible
extensions, or recovery/quarantine dispositions without fabricating a market observation.

The app still runs Coinbase through the diagnostic path at `apps/market-squawk/src/main.rs:290`.
`LiveRuntimeComposition` exists at `apps/market-squawk/src/live_runtime.rs:23`, but the Coinbase path
does not compose the authoritative registry, capture receipt, decoder, current-outcome validation,
or route-bound live ingress.

The existing feature, risk, and paper types are diagnostic compatibility types:

- `apps/market-squawk/src/features.rs:7` has only midpoint, spread, microprice, and imbalance.
- `apps/market-squawk/src/risk.rs:27` exposes a cloneable, serializable `Approved` enum variant and
  accepts caller-supplied current position and time.
- `apps/market-squawk/src/bot.rs:48` fills immediately at the intent limit and has no submission
  state machine, cash enforcement, cancellation, reconciliation, latency, slippage, or partial
  fills.

The former Stage-1 plan explicitly deferred complete features and realistic paper execution at
`docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md:1803-1808`. That deferral is
superseded here. A compatibility-only paper adapter may be retained as a migration oracle, but it is
never an acceptance target and must remain classified `Partial`.

## Considered approaches

### 1. Direct live-to-execution dependency

This would make the live actor call `RiskService` directly. It was rejected because execution must
depend on live authority types, creating a dependency cycle and coupling the live actor to broker
and paper backends.

### 2. App-side risk worker after a shard queue

This would export action candidates to an app worker. It was rejected because it moves strategy and
local-risk state outside deterministic instrument ownership, adds queue delay before capability
consumption, complicates expiry, and makes account and source transplants easier to express.

### 3. Live-owned synchronous hook implemented by execution — selected

The live crate defines a narrow synchronous hook and an actor-scoped authority façade. The execution
crate implements the hook, consumes authority through the façade, performs bounded deterministic
risk, and moves an approved order into a bounded execution handoff. The app supplies the hook during
composition. This preserves dependency direction, actor ownership, and a network/disk-free
event-to-decision path.

## Dependency and file-boundary design

The Q3 dependency DAG is:

```text
domain
├── platform
├── sources ───────────────> platform
├── analytics
├── live ──────────────────> sources, analytics
└── execution ─────────────> live, analytics

adapter-coinbase ──────────> domain, sources
adapter-paper ─────────────> domain, execution
app ───────────────────────> all composition dependencies
```

No reverse edge is allowed. In particular:

- `domain` has no workspace dependency.
- `analytics` has no Tokio, platform, source, live, execution, or adapter dependency.
- `live` has no execution or adapter dependency.
- `execution` has no platform, sources, or adapter dependency.
- Coinbase has no live, platform, analytics, execution, or paper dependency.
- Paper has no live, sources, platform, analytics, or Coinbase dependency.
- A production dependency may not enable a `test-support` feature.

`scripts/check_workspace_boundaries.py` must enforce this graph when the first Q3 crate lands. It
must reject unknown workspace packages, forbidden edges, production test-support features, adapter
cycles, and app business logic leaking into adapter crates.

Files already beyond the preferred 700-line ceiling must be split before Q3 behavior is appended:

| File | Base lines | Required action |
| --- | ---: | --- |
| `crates/market-squawk-live/src/runtime/actor.rs` | 769 | Split scheduling, processing, and snapshot publication. |
| `crates/market-squawk-sources/src/registry/current_batch.rs` | 781 | Put decode dispositions in a new module. |
| `crates/market-squawk-sources/src/policy/budget.rs` | 927 | Reuse its public shared budget; add no Coinbase logic. |
| `crates/market-squawk-sources/src/registry/catalog.rs` | 731 | Keep adapter registration logic out. |
| `crates/market-squawk-sources/tests/registry_authority.rs` | 1,004 | Extract focused fixture support before adding cases. |
| `crates/market-squawk-domain/src/instrument/provider_identities.rs` | 716 | Add execution identities in a new module. |
| `crates/market-squawk-platform/src/capture.rs` | 741 | Keep Q3 composition in focused app/platform modules. |

## Live feature architecture

### Pure kernels

`market-squawk-analytics` owns pure mathematical kernels and registry metadata. It performs no I/O,
uses no Tokio primitive, and does not own live mutable state. Exact price and quantity inputs use
scaled integers or exact rational/decimal arithmetic. Statistical `f64` output is allowed only
through an explicit conversion type and is never reused as an order price.

Every feature returns:

```rust
pub struct FeatureValue<T> {
    value: Option<T>,
    observed_at: Timestamp,
    validity: FeatureValidity,
}

pub enum FeatureValidity {
    Ready,
    WarmingUp,
    Unavailable,
    Overflow,
    TimestampRegression,
    Stale,
}
```

Required feature coverage is:

- Spread, midpoint, and microprice.
- Book imbalance and order-flow imbalance.
- Depth-weighted price.
- Trade aggressor imbalance.
- Rolling VWAP and volume velocity.
- Momentum.
- Rolling returns and volatility.
- Cross-venue divergence.
- Liquidity and slippage estimates.

Each registered feature records name/version, exact input schema, parameters, time semantics,
warm-up, null policy, output type and units, live compatibility, point-in-time compatibility, and
implementation revision. Duplicate `(name, version)` metadata conflicts fail closed.

### Route-owned state

The live route owner keeps bounded rolling windows and feature state for its exact
`(venue_id, instrument_id)` route. State updates occur only after canonical event commit. Generation
rollover, resynchronization, quarantine, trading-status invalidation, timestamp regression, and
source replacement reset or invalidate the affected feature state according to registered policy.

An unavailable, warming, overflowed, regressed, or stale required feature suppresses automated
action. `DirectUnverified` and other non-executable quality classes may produce display/research
features but never an authority capability.

### Cross-venue state

Cross-venue divergence cannot be calculated solely inside a route owner because identical
instruments on different venues may be assigned to different shards. A bounded single-writer
cross-venue hub receives compact coalescing updates, retains a configured maximum number of venues
per instrument, and publishes immutable snapshots. Queue saturation or missing/stale venues marks
the cross-venue feature unavailable. It never silently omits a venue and remains outside the mutable
route state of another shard.

All feature windows, coalescing queues, cross-venue retained state, hook objects, and published
feature snapshots are added to `LiveRuntimeConfig` and the checked peak-memory estimate.

## Live action and authority design

The live crate defines:

```rust
pub trait LiveActionHook: Send + std::fmt::Debug {
    fn on_committed(
        &mut self,
        context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition;

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError>;
}
```

`CommittedActionContext` is borrowed and allocation-free. It contains the committed canonical event,
qualification assessment identity, route identity, bounded market reference, bounded feature view,
validity, and event/source/receive timestamps. It does not contain a constructible authority or a
quality value usable as authority.

`CurrentAuthorityGate<'actor>` has private fields, is constructed only by the actor, cannot escape
the callback, and is `!Clone`, `!Serialize`, `!Deserialize`, `!Send`, and `!Sync`. It delegates:

```rust
pub fn issue(&mut self) -> Result<LiveExecutionCapability, AuthorityError>;

pub fn consume(
    &mut self,
    capability: LiveExecutionCapability,
) -> Result<ConsumedLiveAuthority, AuthorityError>;
```

The live processor remains the sole nonce issuer and consumer. Execution implements
`LiveActionHook`; live never names an execution type. The callback performs no filesystem access,
database query, analytical query, Python, MCP, LLM, network request, adapter future, or unbounded
queue write.

## Authoritative risk and execution design

The domain adds invariant-preserving `AccountId`, `StrategyId`, `ModelId`, `OrderId`,
`ClientOrderId`, and `ApprovalId` types in focused modules.

`OrderIntent` contains typed strategy/model identity, account, instrument, side, order type,
quantity, optional limit/stop prices, time in force, signal/expiration timestamps, reason codes,
maximum slippage, required quality, and idempotency identity. Its constructor rejects invalid order
type/price combinations, nonpositive quantity, invalid chronology, and inexact tick/lot values.

Risk uses no caller-authored current position, balance, account view, or action time. An
execution-owned `AccountRiskCoordinator` maintains authoritative account revision, balances,
positions, capital, exposure, leverage, losses, drawdown, rates, and duplicate state. It issues a
private, expiring `AccountRiskReservation` so concurrent shards cannot jointly exceed an account
limit. Accounts are deterministically partitioned into startup-sized single-writer critical sections;
the live hook uses nonblocking acquisition. Contention fails closed with a typed rejection and zero
mutation rather than blocking an instrument actor on an unbounded lock or response queue.

`RiskService::evaluate` receives an intent, bounded market reference, a
`LiveExecutionCapability` by value, and the actor-scoped `CurrentAuthorityGate`. It consumes the
capability exactly once, validates the authority, obtains an account reservation, evaluates all
typed rejection reasons, admits a bounded audit record, and privately constructs `ApprovedOrder`.

`ApprovedOrder` owns the consumed live authority and account reservation. It is private-field,
non-Serde, non-Clone, and not externally constructible. Its expiry is the minimum of every intent,
authority, authorization/coverage, account reservation, and risk-policy deadline.

`ExecutionDispatcher` owns the bounded one-time approval registry and bounded submission queue. It
moves the approval into an execution worker. Immediately before constructing private
`DispatchOrder`, the worker revalidates authority, account reservation, approval expiry, and audit
admission. Queue saturation, audit saturation, expiry, revocation, or reconciliation-required state
fails closed before the adapter call. An uncertain adapter result requires reconciliation and a new
risk decision; the dispatch value is never replayed.

Production time comes from sealed wall-plus-monotonic clocks. Public APIs do not accept a
caller-supplied decision or dispatch timestamp. Deterministic clocks remain test-only.

No strategy, model, app service, CLI command, MCP tool, or adapter can construct `ApprovedOrder` or
`DispatchOrder`, or call an adapter without the dispatcher.

## Decoder outcome design

`MarketDecoder` changes to:

```rust
pub trait MarketDecoder: SourceMetadataProvider {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError>;
}
```

`DecodeOutcome` is:

```rust
pub enum DecodeOutcome {
    Data(DecodedProviderBatch),
    Control(DecodedControlFrame),
    Ignored(DecodedIgnoredFrame),
    Resynchronize(DecodedRecoveryAction),
    Quarantine(DecodedQuarantineAction),
}
```

Every variant retains exact `DecoderEvidence`: shared session binding, frame ID, receive time,
SHA-256 payload digest, and decoder rule/revision. Non-data types use closed enums and bounded
provider product/channel/message identifiers; they do not retain another raw payload copy.

Expected provider faults become typed recovery or quarantine outcomes. `DecodeInternalError` is
reserved for allocation, retained-size, or implementation invariant failures. Stateful sequence,
snapshot, checksum, tick/lot, book, freshness, and trading-status qualification remains in live.

Capture binding and current coverage are intentionally separate authority upgrades. First,
`ValidatedSourceSession::validate_decode_outcome_owned` consumes any outcome and its exact
`CaptureAdmissionReceipt`, validates allocation identity and every frame/digest/rule dimension, and
returns a non-cloneable, non-Serde session-bound value:

```rust
pub enum ValidatedSessionDecodeOutcome {
    Data(CapturedDecodedProviderBatch),
    Control(SessionControlDisposition),
    Ignored(SessionIgnoredDisposition),
    Resynchronize(SessionRecoveryDisposition),
    Quarantine(SessionQuarantineDisposition),
}
```

The app-owned per-generation subscription state machine consumes session-bound control outcomes.
Only an exact acknowledgement for the configured product/channel set may establish current coverage
health. Data before that acknowledgement fails closed and cannot reach current authority or shard
ingress. After acknowledgement,
`ValidatedCurrentSourceAuthority::validate_data_outcome_owned(CapturedDecodedProviderBatch)` performs
the second upgrade and returns `CurrentDecodedProviderBatches`. Control frames may update
connection/transport health but never market freshness. Ignored frames produce bounded
audit/counters without live mutation. Recovery and quarantine dispositions stop market eligibility
and are acted on only by the app supervisor. Deserialized or replayed outcomes have no current
authority.

All outcomes report checked retained bytes. Non-data audit uses bounded count and byte queues. Audit
saturation invalidates the generation instead of silently losing an integrity decision.

## Coinbase production composition

The selected protocol is Coinbase Exchange WebSocket v1 for fixture continuity and direct-venue
semantics:

```text
endpoint: wss://ws-feed.exchange.coinbase.com
channels: level2, matches, heartbeat
coverage: one Coinbase venue and subscribed products/channels only
quality ceiling: DirectUnverified
level2 sequence qualification: unsupported by the selected contract
checksum qualification: unsupported
trade completeness: not guaranteed
heartbeat: connection/feed health only
```

Coinbase documents these channels and warns that `matches` messages can be dropped:

- <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels>
- <https://docs.cdp.coinbase.com/exchange/websocket-feed/best-practices>

Advanced Trade is not mixed into this implementation. If a later adapter selects it, it must be a
separate protocol profile using the official endpoint, channel subscriptions, five-second
subscription deadline, `sequence_num`, and batched `market_trades` semantics documented at:

- <https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/guides/websocket>
- <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels>
- <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview>

The adapter runs one exact connection generation. It does not reconnect using the same
`RawFrameFactory`. The app supervisor alone ends the session, applies the registry-issued shared
budget/backoff, starts a new session/generation, obtains a new frame factory and capture binding, and
binds new live ingress.

The adapter preserves exact bounded `ProviderDecimalLexeme` values. Authoritative tick/lot
normalization remains in the live processor against the current `InstrumentDefinition`; the adapter
does not duplicate it.

The app production composition owns the authoritative registry, source metadata/session/health,
shared budget, frame factory, capture publisher, decoder, two-stage session/current validators,
bounded per-generation subscription/control state, route-bound ingress, and supervisor.
Configuration includes explicit provider-product to internal-instrument mapping, venue, tick/lot
definition, subscription bounds, frame bounds, endpoint allowlist, and freshness policy. Production
IDs are not hardcoded in the adapter. The state machine does not declare current coverage before the
exact subscription acknowledgement; data-before-ack, acknowledgement mismatch, control/audit queue
saturation, or control-state overflow invalidates the generation. Ping/pong and heartbeat prove only
transport/feed liveness. Every control queue, retained identifier, transition counter, and audit
record participates in startup memory accounting.

Coinbase remains `DirectUnverified`; no test or documentation promotes it to execution eligibility.

## Realistic paper execution design

The required paper adapter is a deterministic, single-writer execution simulator, not the current
immediate-fill compatibility object. It implements:

```text
New
Accepted
PartiallyFilled
Filled
CancelPending
Canceled
Rejected
Expired
```

It owns:

- Bounded orders, fills, cancellation requests, and audit records.
- Cash balances, positions, reserved cash/quantity, fees, and realized cash flow.
- Checked decimal/integer ledger arithmetic with explicit currency and rounding.
- Configured maker/taker fee schedules.
- A deterministic seeded latency model.
- Side-aware bid/ask and configured slippage.
- Bounded depth-aware fill allocation and partial fills.
- Resting order processing through a bounded typed paper-market ingress.
- Intent/order idempotency and monotonic internal order revisions.
- Cancellation/fill race ordering by deterministic event time and sequence.
- Reconciliation snapshots and reconciliation-required uncertain states.

`DispatchOrder` carries a bounded immutable market reference with top/depth, observed time, quality,
and authority evidence. It also freezes the validated instrument-definition revision and exact
execution terms: price tick, lot size, quote and settlement currency, and positive exact contract
multiplier where applicable. Paper accounting converts ticks times lots times exact execution terms
to `Money` with checked decimal arithmetic; it never assumes raw ticks equal currency minor units.
Terms from another instrument/revision, an inexact scale, an unsupported settlement denomination,
or a currency mismatch fail before reservation or mutation. Resting orders consume subsequent
private `ExecutionMarketUpdate` values admitted by the execution layer; the adapter cannot construct
them from arbitrary app DTOs.

The simulator implements market, limit, stop, and stop-limit orders and Day, GTC, IOC, and FOK time
in force. Latency expires before eligibility. Crossing limits may take liquidity; non-crossing
limits rest with stable price/time priority. Stops trigger from the configured canonical reference;
stop-limits activate as limits. Day expiry uses a configured venue-session calendar/time zone rather
than process-local midnight. Each bounded market-liquidity update is allocated once across all
eligible competing orders, preventing two orders from consuming the same displayed quantity.
Maker/taker classification, partial-fill continuation, slippage, fees, and cancellation races are
deterministic and auditable.

Compatibility metadata may truthfully describe immediate/full-fill behavior as `Modeled`, but such
behavior remains `Partial` and is not the Q3 acceptance target.

## Error, saturation, and shutdown behavior

Every queue has count and byte bounds. Saturation is explicit:

- Feature/cross-venue saturation marks the affected feature unavailable and suppresses action.
- Capture or live-ingress saturation invalidates the exact source generation.
- Decision-audit or execution-handoff saturation rejects the order before adapter invocation.
- Paper command or market-update saturation rejects the new command, preserves prior state, and
  emits a bounded health/audit condition.

Shutdown order is:

```text
stop source producers
→ invalidate source/live authority
→ close action production
→ drain or reject bounded execution commands by deadline
→ reconcile paper/external adapter state
→ flush audit outside the live path
→ join workers
→ close snapshots and services
```

No worker or blocking task is detached after a deadline. Owned pending work remains inspectable and
reapable under the existing platform lifecycle pattern.

A post-commit feature arithmetic/capacity failure does not roll back or actor-fatally discard an
already committed market observation. The route transactionally publishes an unavailable/overflow
feature disposition, suppresses action for that observation, emits bounded health, and continues
processing. The paper worker and its audit-reader consumer are owned by Wave 3 application services;
shutdown closes production, drains/reconciles by deadline, flushes the controlled local audit, and
joins both tasks. No paper task is detached and no audit receiver is left unconsumed.

Platform provides a domain-agnostic controlled, bounded, checksum-framed audit journal and has no
dependency on execution or paper DTOs. The app validates and serializes canonical execution/risk/
paper records into that journal. A journal-writer failure revokes future action production and
forces reconciliation while the live event path remains nonblocking and never waits on disk.

Before action startup, the app validates journal/checkpoint version, monotonic sequence, framing,
and checksum/hash-chain continuity; applies an explicit torn-final-tail policy; and reconstructs
orders, fills, cash, positions, reservations, idempotency, and reconciliation state. Duplicate,
reordered, missing, corrupted-complete, unsupported-version, or inconsistent records fail closed.
Incomplete recovery publishes `ReconciliationRequired` and prevents every new submission; the
system never silently starts paper accounting from an empty state.

## Privacy, lawful access, and anti-evasion exclusions

Q3 preserves local-first operation, no mandatory cloud/database/container/telemetry dependency,
endpoint allowlists, secret redaction, local structured tracing, and controlled artifacts.

The following remain permanently `Unsafe` and are not designed, tested as positive behavior, or
exposed through configuration:

- Identity or account rotation to evade provider limits.
- Browser or TLS fingerprint spoofing for concealment.
- CAPTCHA or anti-bot bypass.
- Proxy rotation intended to defeat blocking.
- Distributed requests intended to evade quotas.

Rate limits and refusal responses use the registry-coordinated `SharedProviderBudget`, provider
backoff, `Retry-After`, cache/local persistence, coverage metadata, and source failover. Tests prove
there is no rotation/evasion surface.

## Verification and review design

Execution must start from the formally approved integrated Q2 commit, not automatically from the
base commit recorded above. Before Q3 work begins, the integration owner rebases the plan branch or
creates fresh implementation worktrees at that approved commit, refreshes every source line anchor
and base-commit reference, reruns the complete local baseline, and records the reviewed Q2 evidence.
Local command success is local evidence only; it does not prove that hosted CI ran or passed.

Every task uses red-green TDD, focused tests, strict Clippy, and a small commit. External network
tests remain ignored and opt-in; deterministic local WebSocket tests remain in the default suite.

Each integration wave runs:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_workspace_boundaries.py
git diff --check
```

The grouped Q3 review after the superseding Tasks 9–12 uses independent specialties:

- Authority and bypass reviewer.
- Concurrency, retained-memory, and shutdown reviewer.
- Provider protocol, coverage, and lawful-access reviewer.
- Financial accounting and paper-state reviewer.
- Dependency boundary and application entry-point reviewer.

Q3 is not accepted if Coinbase is described as `DirectVerified`, any submission path bypasses risk,
caller-authored time/account data can authorize action, a control frame becomes a market event, a
reconnect reuses a generation, a required feature is absent, paper lacks realistic behavior, or any
new retained state is missing from the peak-memory proof. It is also not accepted with incomplete
startup recovery, a detached/unconsumed paper or audit worker, a platform-to-execution dependency,
or any unresolved substantiated quarter-review finding. Hosted CI, when present, is recorded
separately and is not required for local approval.

## Proposed execution decisions

- The design uses a live-owned hook implemented by execution; no dependency cycle is allowed.
- Live remains the sole capability issuer/consumer; execution owns approval and dispatch one-time
  state.
- The selected Coinbase protocol is Exchange WebSocket v1 with a `DirectUnverified` ceiling.
- Complete live features and complete realistic paper execution are Q3 scope, not deferred work.
- The boundary checker and large-file splits are prerequisites, not release hardening follow-ups.
- Root manifests and `Cargo.lock` are owned by the integration lane, never by parallel feature lanes.
- Anti-evasion features remain prohibited.
