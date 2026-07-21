# Paper Financial Recovery Design

Status: Approved implementation design

Audit base: `136b556`

## Objective

Make the production paper composition financially fail-closed between asynchronous adapter
mutation, risk reconciliation, durable restart, and final shutdown. The live event-to-action path
must retain atomic-only fencing and bounded queue handoff; reconciliation and persistence remain
outside that path.

## Chosen architecture

The application owns one bounded paper financial supervisor. The paper worker remains the sole
paper-state writer, the execution dispatcher remains the sole account-replacement authority, and
the account coordinator remains the sole pre-trade account authority.

Before a paper mutation can change authoritative account risk, the paper worker advances a shared
monotonic reconciliation fence. Risk admission compares the required and applied paper sequences
using atomics and rejects with `ReconciliationRequired` while a newer sequence is pending. After
the mutation, a bounded coalescing notification wakes the application supervisor. The supervisor
drives dispatcher reconciliation outside the live path and advances the applied fence only for the
exact backend sequence whose account/order image completed dispatcher and adapter acknowledgement.
Notifications may coalesce, but sequence comparison prevents lost work.

The production runtime owns the supervisor task and its cancellation. Startup reserves task
ownership before spawning. Shutdown closes live/action admission, quiesces and joins the dispatcher
submission worker, completes final reconciliation, snapshots the coordinator replay fences, writes
one current recovery manifest, acknowledges that exact persisted checkpoint to paper, shuts down
the paper worker, and drains transferred tasks. No complete result is possible when any earlier
barrier is incomplete.

## Exact financial arithmetic

Cash and exposure reservation totals remain separate. Both accumulation and the addition of the new
reservation are checked. Cash overflow produces `AccountRiskViolation::ArithmeticOverflow`; it can
never be treated as sufficient cash and can never mint a reservation or approval.

## Recovery repository

The checkpoint repository keeps immutable, content-addressed checkpoint objects and adds one fixed,
capability-relative current manifest. The manifest binds schema, paper configuration, stable
repository identity, monotonic generation, checkpoint reference and digest, checkpoint recovery
digest and sequence, plus every account replay/idempotency snapshot needed to reconstruct risk.

A fresh artifact root may initialize with no paper namespace. If the paper namespace or any paper
artifact exists without one valid current manifest, startup fails closed. Existing state is never
replaced by configured defaults. Publication writes and synchronizes a private staging manifest,
atomically replaces the fixed current manifest, synchronizes the containing directories, reopens
without following symlinks, bounds the read, and validates every binding before returning a receipt.
Restart reads only the fixed manifest and its exact referenced object; it never scans or selects a
file by timestamp or lexical order. The stable repository identity and generation are restored from
the manifest.

The recovered checkpoint supplies paper orders, ledger state, account revisions, and paper replay
state. The same manifest supplies account-coordinator idempotency snapshots. Open paper orders are
restored under dispatcher reconciliation ownership before live admission; they cannot be silently
forgotten or converted into new order authority.

## Rejected alternatives

- Adapter-owned coordinator mutation couples execution backends to risk ownership and bypasses the
  dispatcher reconciliation authority.
- Periodic polling without a pre-mutation fence leaves a stale-risk approval window.
- Test-only or CLI-triggered reconciliation does not provide production ownership.
- Directory scans and "latest file" selection make restart ambiguous after crashes or clock skew.
- Persisting before dispatcher quiescence can acknowledge a checkpoint older than an accepted
  in-flight submit.

## Thin causal verification

1. A cash addition overflow rejects with `ArithmeticOverflow`, retains no reservation, and leaves
   fee cash and gross exposure calculations independent.
2. A production paper fill advances the fence, the owned supervisor reconciles it, and the
   coordinator then reflects cash, position, capital, realized loss, and drawdown without a manual
   reconcile call.
3. A clean restart loads the exact current manifest, paper checkpoint, replay fence, account
   revision, and open-order reconciliation ownership; partial or corrupt recovery state fails.
4. A submit racing shutdown is either rejected before admission or included in the final reconciled
   checkpoint. A complete shutdown never reports a sequence older than the final paper snapshot.

