# 0005: Centralize Risk and Execution Authority

Status: Accepted

Decision date: 2026-07-16

## Context

Strategies and models need to express decisions without acquiring broker authority. Source quality,
freshness, instrument terms, portfolio state, account exposure, price/slippage, order rate,
duplicates, loss limits, intent expiry, and audit must be evaluated consistently regardless of
which strategy or local transport initiated work.

Allowing strategies, adapters, CLI, or MCP to construct broker-ready orders would duplicate policy,
make reservation races possible, and create bypass paths around live evidence and audit. A
serializable approval would also remain usable after its source, portfolio, account, or deadline
authority had changed.

## Decision

Strategies emit intents; central risk is mandatory; only the execution authority may call an
adapter.

A strategy receives authority-free committed market context and emits a fixed-capacity set of
validated `OrderIntent` values or an audited no-action fact. An intent carries identities,
revision-bound instrument terms, order parameters, signal and expiry, rationale, slippage, and a
`DirectVerified` requirement, but no execution capability.

The process-local `RiskService` is the sole constructor path for `ApprovedOrder`. It consumes the
current live capability, reserves audit capacity, applies current market and policy checks, binds
and rechecks the immutable portfolio revision, evaluates account limits, and atomically reserves
account capacity. Approval lifetime is the minimum of intent, live evidence, risk policy, account
reservation, and configured maximum lifetime.

The bounded dispatcher is the execution authority. It consumes each approval once, rechecks live,
portfolio, reservation, expiry, and idempotency state, and alone constructs the private
adapter-facing `DispatchOrder`. `ExecutionAdapter` accepts that type for submit and uses separately
typed cancel/reconcile operations. CLI, MCP, replay, models, strategies, and persisted records have
no direct adapter path.

## Consequences

- All automated orders cross the same live-evidence, portfolio, account, policy, and audit checks.
- Model or strategy failure produces no action rather than a guessed fallback order.
- Approval and dispatch values are private, non-cloneable or non-serializable where authority is
  retained, and bounded by current deadlines.
- Account capacity is reserved before adapter submission and released or reconciled through the
  execution lifecycle.
- Known rejection, known no-attempt failure, and uncertain adapter outcome remain distinct; an
  uncertain attempt requires reconciliation.
- Paper and configured live adapters are replaceable behind the same authority boundary.
- Central risk is a logical authority inside the local process; it does not require a network
  service or external database.

## Rejected alternatives

- Strategies or models calling an execution adapter directly.
- A CLI or MCP operation constructing an unchecked broker order.
- Risk implemented independently in every strategy or transport.
- Approving on a `DirectVerified` enum without consuming current live authority.
- Serializing and replaying `ApprovedOrder` or `DispatchOrder`.
- Reserving account capacity after the adapter call.
- Retrying uncertain adapter outcomes as if no attempt occurred.

## Related architecture

- [Live execution plane](../live-execution-plane.md)
- [Control plane](../control-plane.md)
- [Security and trust boundaries](../security-and-trust-boundaries.md)
- [ADR 0002: Evidence-derived execution quality](0002-evidence-derived-execution-quality.md)

## Evidence and sources

- [Strategy and bounded output](../../../crates/market-squawk-execution/src/strategy.rs) and
  [authority-free order intent](../../../crates/market-squawk-execution/src/intent.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Mandatory risk evaluation](../../../crates/market-squawk-execution/src/risk.rs) and
  [private approval construction](../../../crates/market-squawk-execution/src/approval.rs), reviewed
  at `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Bounded dispatcher](../../../crates/market-squawk-execution/src/dispatcher.rs) and
  [adapter-only dispatch contract](../../../crates/market-squawk-execution/src/adapter.rs), reviewed
  at `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Application transport composition](../../../apps/market-squawk/src/application.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Tokio bounded MPSC documentation](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html),
  reviewed 2026-07-23, supports the bounded single-consumer dispatcher handoff.
