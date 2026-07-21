# Paper Risk and Lifecycle Remediation Design

Status: Approved implementation design

Audit base: `ed137b39aea95c20cb1b8adb80786070f7671ef5`

Scope: domain exact-financial kernels, execution authority/risk/dispatcher, and the paper adapter

## Objective

Close the Quarter 1 financial, authority, durability, and lifecycle findings without weakening the
bounded live path. The result must fail closed on inexact arithmetic, stale evidence, expired
authority, incomplete marks, ambiguous remote outcomes, unverifiable persistence, and unjoined
tasks.

## Chosen architecture

The remediation extends the existing typed authority and state projections instead of applying
independent local patches. Three rejected alternatives were: symptom-level fixes that leave
conflicting semantics, caller assertions for persistence or valuation, and disabling required paper
capabilities.

### Exact financial arithmetic

The domain crate exposes narrowly typed exact decimal-product and exact rational-rounding
operations. Stack-only factor cancellation remains the risk hot-path implementation. `BigUint` is
used only when an order-time result must be rounded explicitly; it is never used per market event.

Contract-multiplier notional and leverage comparisons reject unrepresentable exact results. Risk
fee reservation rounds in the adverse direction. Paper settlement fees round nearest-even at the
configured money scale. A BigInt oracle proves a case where `rust_decimal` would return a silently
rounded value.

### Qualified marking and account state

The paper ledger stores private-construction marks only from `DirectVerified` execution updates.
Each mark binds current instrument terms, venue/generation evidence, currency and multiplier,
assessment digest, trusted observation time, and non-heartbeat event identity. A mark must be fresh
at valuation time and definition-consistent. Missing, stale, regressed, or mismatched evidence makes
the affected account ineligible for authoritative replacement.

Long positions use the executable bid and short positions use the executable ask. Exact revaluation
keeps cash and settled capital separate from marked equity, unrealized profit and loss, marked gross
exposure, peak marked equity, and drawdown. Checkpoint, recovery, reconciliation, account
replacement, digests, and schema versions bind these dimensions and their mark evidence. After
reconciliation replaces the coordinator image, an adverse mark must reject the next risk admission.

### Authority deadlines and remote ambiguity

`valid_until` is exclusive for source freshness. `ApprovedOrderParts` retains the approval's
monotonic deadline. The dispatch attempt deadline is the minimum of authority, reservation, policy,
intent, and operation deadlines, and the same deadline travels in `DispatchOrder`.

The paper worker obtains trusted wall and monotonic time immediately before the first irreversible
ledger, idempotency, order, or audit-state mutation. Expiry before that point returns a known
non-attempt with zero mutation. Once a remote adapter future has been polled, timeout, cancellation,
or lost completion is an uncertain outcome requiring reconciliation.

### Controlled checkpoint publication

`PaperCheckpointRepository` owns an existing platform `ArtifactRoot` capability and a bounded
single-writer generation. It encodes canonically under a byte limit, creates private no-follow
same-directory staging with `create_new`, writes and synchronizes the file, publishes a
content-addressed final name without clobbering, accepts an existing object only after exact byte
verification, synchronizes the containing directory, reopens through the capability, performs a
bounded read-back, and verifies bytes, hash, and checkpoint decode.

Only that repository can construct the opaque, non-cloneable persistence receipt. The receipt binds
repository identity, generation, configuration, sequence, recovery digest, artifact digest, and
portable artifact reference. Paper compaction and dispatcher reconciliation-fence advancement occur
only after receipt validation and dispatcher finalization. Caller-created byte-comparison evidence
is removed.

### Task ownership and bounded work

Every paper, dispatcher, and isolated adapter task reserves process-lifetime reaper capacity before
spawn and retains the permit until join or transfer. Timeout and `Drop` abort and transfer the
pending handle non-fallibly to a runtime-independent bounded reaper; handles are never detached or
discarded. The application integration owner must drain that reaper during process shutdown.

Shipping adapters are registered as cooperative. Generic adapter submit, cancel, and reconcile
attempts run in owned isolated tasks so a non-cooperative implementation cannot stall dispatcher
ownership. A timed-out attempted call is uncertain. Paper configuration enforces fixed production
ceilings for orders and depth evaluations, and matching yields at a fixed cooperative quantum while
preserving deterministic price-time ordering.

## Thin behavioral proof

The remediation adds or strengthens only these critical proofs:

1. Independent-oracle exact product and fee rounding at the silent-rounding boundary.
2. Adverse qualified mark, reconciliation/account replacement, and rejection of the next order.
3. Freshness equality rejection with unchanged account and approval audit state.
4. Capacity-one open/close of instrument A followed by admission of instrument B.
5. Delayed paper submission expiry with zero mutation and attempted remote timeout as uncertain.
6. Checkpoint publication crash boundaries, verified-existing publication, and receipt-gated
   compaction.
7. Reaper transfer after timeout/Drop and hard-capacity refusal before spawn.

Application call-site changes caused by removal of caller-mintable persistence evidence are an
explicit root integration handoff because this lane may not edit the application crate.
