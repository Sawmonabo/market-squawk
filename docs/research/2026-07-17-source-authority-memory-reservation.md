# Source-authority memory reservation architecture

Date: 2026-07-17

Status: Binding implementation design for Q2 A3/I03

Audit anchor: authority remediation worktree base `e6df2d1000675e55d60c649ed1926f774fd3d834`
plus its uncommitted I02/I03 candidate

Approval status: design evidence only; not approval of that moving candidate

## Conclusion

Source-authority memory must be bounded by one composition-global reservation account, while
passive live leases retain only a small immutable/atomic availability projection. This is a
combined design:

- a global reservation is the correctness mechanism for a mutable persistent graph; and
- passive-lease decoupling is the latency and reachability mechanism that keeps persistence out of
  the live path.

Caching the session's current retained size is insufficient. A queued batch freezes its charge at
construction, but the shared session can grow later through registry, revision, budget-group, and
declaration mutations. The old batch would then retain more shared memory than its immutable charge
records. Conversely, recomputing the graph on every route acquires persistence locks, may invoke a
store callback, and is proportional to the graph size.

The governing invariant is:

```text
accepted live peak
    = live runtime peak excluding source durability
    + configured source-authority ceiling exactly once
    + bounded marginal queued-command bytes

actual committed mutable authority graph <= configured committed ceiling
actual authority mutation workspace     <= configured mutation ceiling
```

No live routing operation may lock or traverse the durability graph.

## Rejected alternatives

| Alternative | Rejection reason |
|---|---|
| Per-batch snapshot of current shared graph size | Later graph growth makes already queued charges stale. |
| O(1) cached current graph size per batch | Removes traversal cost but retains the same stale-charge defect. |
| Passive-lease split alone | Removes persistence from routing but leaves the registry/session graph outside `maximum_runtime_bytes`. |
| Seal the graph before runtime | Prevents legitimate dynamic source and revision registration. |
| Charge the theoretical maximum graph to every batch | Multiplies one shared graph by queues/routes and becomes operationally unusable. |

## Composition types

`market-squawk-sources` owns the account because it owns the graph and its mutation protocols.
`market-squawk-live` receives only opaque witnesses and attachments. `market-squawk-platform`
parses raw configuration and owns the concrete local durable store. The application remains the
composition root.

### Public source-side capabilities

- `SourceAuthorityMemoryLimits`: validated nonzero committed-graph and one-mutation workspace
  ceilings. Limits above the account's numeric representation are rejected.
- `SourceAuthorityMemoryBudget`: non-Serde, non-reconstructable `Arc`-backed composition account.
  It issues one session owner, cloneable validation witnesses, and at most one runtime attachment.
- `SourceAuthorityMemoryRuntimeWitness`: cloneable identity-and-limit witness with no ability to
  reserve or release memory.
- `SourceAuthorityMemoryRuntimeAttachment`: non-Clone RAII runtime attachment. A second concurrent
  runtime on the same domain fails startup; replacement attaches only after full former-runtime
  cleanup.

### Private ownership types

- `AuthoritySessionMemoryReservation`: owned by `AuthorityDurabilitySession`; releases only after
  graph-bearing session fields are dropped.
- `PendingAuthorityMemoryDelta`: non-Clone RAII token. Uncommitted growth rolls back on `Drop`;
  successful publication transfers the delta to the session reservation.
- `AuthorityMutationWorkspaceGuard`: non-Clone guard for candidate, canonicalization, payload, and
  replacement overlap workspace.
- `DurabilityAvailabilityState`: small atomic `Arc` projection with no envelope, store, registry,
  or persistence capability.
- `BudgetRuntimeCore`: stable policy/state/clock/generation/terminal ownership shared by passive
  leases.

`BudgetAvailabilityLease` retains `BudgetRuntimeCore`, generation, availability projection, and a
memory-domain witness. It must not retain `BudgetDurabilityBinding` or
`Arc<AuthorityDurabilitySession>`. Full `SharedProviderBudget` and `BudgetPermit` handles retain the
allocation/session because they can perform persistence and reconciliation.

## Reservation word and failure semantics

The account uses one `AtomicU64`:

```text
bit 63       accounting-poison sentinel
bits 0..62   currently committed bytes
```

Positive reservation uses checked addition and an `AcqRel` compare-exchange loop. Exceeding the
configured ceiling is a normal precommit capacity error: it changes no authority state and makes
no store attempt. Arithmetic overflow, underflow, reservation mismatch, double release, or
published retained size above the staged reservation is an integrity failure: poison the account,
terminalize associated authority, and reject subsequent use.

Shrinkage is released only after the replaced graph allocation has been dropped. Store detach,
clean close, terminalization, and final destruction must be idempotent and release each component
exactly once.

No live route or publish operation reads or mutates the reservation counter.

## Stable store contract

The store's mutable `retained_allocation_bytes()` snapshot is not an adequate production contract.
A backend could grow internal capacity outside an envelope mutation and evade the delta protocol.

Replace it with a lifetime-stable retained-allocation ceiling declared once at session open:

- charge the store value, heap allocations, and assigned shared control block once;
- reserve the complete ceiling, not current logical payload length;
- require load/store results to include a crate-private observed-retained receipt;
- terminalize and detach if any receipt exceeds the declared ceiling;
- keep the production store extension trait crate-private;
- keep the public durable constructor restricted to the concrete local store.

An adversarial test store may grow within its pre-reserved ceiling without increasing the session
charge. A store that reports even one byte above its ceiling must never publish a successful
authority mutation.

## Session opening protocol

1. Claim the sole active session owner from the memory budget.
2. Read and validate the immutable store ceiling.
3. Acquire mutation workspace for load, decode, restore, and in-use publication.
4. Load and validate the canonical envelope.
5. Calculate the initial committed session charge, including session/control allocation, complete
   capacity-sensitive envelope graph, full store ceiling, and assigned availability/account
   allocations.
6. Reserve that complete charge.
7. Construct and durably store the in-use envelope.
8. Validate the store receipt against its ceiling.
9. Publish `Arc<AuthorityDurabilitySession>` with the committed reservation.
10. Release temporary workspace.

If the committed ceiling cannot hold restored state, opening fails before the run is published in
use. Store failure rolls back the pending reservation and publishes no session.

## Mutation protocols

Persistence APIs distinguish fixed-shape and shape-changing transactions at the API boundary.

### Fixed-shape

Checkpoint counters/deadlines, terminal flags, run state, and wall high-water updates use lifecycle
admission, workspace, `envelope -> store` locking, store receipt validation, and fixed-size
publication. They perform no graph traversal or graph-delta reservation. Test-only assertions may
recompute size to prove a zero delta.

### Shape-changing

Registry replacement, source/revision additions, budget groups, declarations, allocation-bearing
policy changes, and close-time registry replacement use:

1. lifecycle operation admission;
2. bounded workspace guard;
3. envelope lock;
4. complete candidate construction without current-state mutation;
5. exhaustive, capacity-sensitive candidate charge;
6. checked positive-delta reservation;
7. canonicalization and bounded serialization;
8. store lock after envelope lock;
9. lifecycle admission revalidation;
10. durable store and receipt validation;
11. infallible in-memory swap immediately after store success;
12. old-graph drop;
13. pending growth commit or post-drop shrink release;
14. workspace release last.

There must be no fallible operation between successful durable store and candidate publication.

## Workspace bound

Committed graph bytes and transaction workspace are separate limits. The workspace ceiling covers
the maximum coexistence of current envelope, candidate, canonicalization/sort scratch, bounded
serialized payload, replaced graph, and concrete-store write scratch. Only one mutation workspace
guard is active for the authority domain.

Persistent allocation-bearing collections should have deterministic capacity semantics. Prefer
exact-length boxed slices where mutation patterns allow them. Where bounded vectors remain, charge
actual capacity after candidate construction and verify it against the staged bound before store.

This follows the evidence classes and structural-accounting limitations in
[Lane B live-memory accounting](2026-07-16-live-memory-accounting.md); it does not claim allocator
physical-block exactness.

## Stable live and batch charge

`CurrentSourceAuthorityLease` precomputes a stable shared charge containing only allocations whose
size cannot grow after lease minting: session lease state, capture generation, `BudgetRuntimeCore`,
policy allocation, clocks, and the small durability-availability projection.

It excludes the session, store, envelope, registry graph, durable budget groups/declarations, and
memory account already charged globally.

`CurrentDecodedProviderBatch.retained_bytes` then covers only marginal batch ownership:

- batch and exact observation backing;
- batch-key allocation;
- normalized observation/policy allocations;
- exact frame-evidence pointee/control allocation once;
- stable current-authority allocation once;
- inline handles already included structurally.

Routing may not invoke a store method, acquire envelope/store locks, traverse durability state,
wait for persistence, or observe a mutable graph-size snapshot.

## Live-runtime integration

Add raw platform/app configuration for:

```text
maximum_source_authority_retained_bytes
maximum_source_authority_mutation_bytes
```

The app constructs one account and passes capabilities, not duplicated byte values. The live
runtime retains the witness and adds `witness.maximum_total_accounted_bytes()` exactly once,
outside route, source, shard, mailbox, stream, and reader multipliers.

Runtime startup atomically attaches before allocating actors, checks the complete estimate against
`maximum_runtime_bytes`, and retains the attachment until all actors, queues, commands, snapshots,
and routes have been destroyed. Partial startup releases only after cleanup. Generation binding
requires source lease and runtime attachment to share the exact memory-domain identity.

## Typed errors

Normal capacity exhaustion remains distinct from integrity failure:

- `AuthorityMemoryCeilingExceeded`: precommit rejection, current authority unchanged.
- `AuthorityMemoryAccountingOverflow`: poison and terminalize.
- `AuthorityMemoryAccountingCorrupt`: poison and terminalize.
- `AuthorityMemoryBudgetUnavailable`: account already poisoned or closed.
- `AuthorityMemorySessionAlreadyOwned`: conflicting composition.
- `AuthorityStoreMemoryContractViolated`: terminalize and detach.
- `LiveRuntimeStartError::SourceAuthorityMemoryAlreadyAttached`.
- `LiveRuntimeStartError::SourceAuthorityMemoryUnavailable`.
- `LiveIngressBindError::SourceAuthorityMemoryDomainMismatch`.

Do not map capacity exhaustion to a store outage or accounting corruption to ordinary active work.

## Blocking RED/GREEN evidence

Implementation is not complete until deterministic tests prove:

1. routing never invokes a store retained-size callback;
2. routing completes while the envelope lock is held elsewhere;
3. routing performs zero durability-graph visits regardless of graph size;
4. old batch charge remains immutable while later graph growth consumes the global reservation;
5. store growth within its ceiling is already charged and above-ceiling receipt terminalizes;
6. exact ceiling succeeds and one byte over is failure-atomic;
7. overflow/underflow poisons and blocks later authority;
8. concurrent boundary reservations admit only a sum within the ceiling;
9. uncommitted RAII growth rolls back exactly;
10. store failure after staged growth leaks no reservation;
11. shrink releases only after an old-graph drop probe fires;
12. detach and final drop release each component exactly once;
13. full budget/permit owners retain the session, but a passive routed batch does not;
14. every nested collection is capacity-sensitive;
15. fixed-shape updates have zero delta and zero graph traversal;
16. actual-versus-staged mismatch poisons before publication;
17. the live estimator adds the total authority ceiling exactly once under every multiplier;
18. exact combined live ceiling succeeds and one byte under fails with the exact estimate;
19. one runtime attachment wins and a successor attaches only after full shutdown;
20. a witness/domain transplant is rejected before enqueue;
21. blocked control-plane persistence cannot delay `try_publish`;
22. runtime attachment outlives all queued commands.

Compile-fail tests must also prove that downstream callers cannot forge witnesses, attachments,
session owners, or delta guards; clone non-Clone capabilities; mutate reservation counters; inject
an arbitrary production store; or compose a runtime from an unrelated raw byte count.

## Implementation order and ownership

The packed lifecycle/operation-admission remediation is a start barrier because session close,
terminalization, store detach, and permit lifetime are shared hotspots. After that interface is
frozen:

1. freeze the C1/C2 and reservation RED tests;
2. add account, witness, attachment, reservation, delta, workspace, and poison primitives;
3. harden store ceiling/receipt and session open;
4. split fixed-shape and shape-changing mutation transactions;
5. split passive budget reachability from persistence ownership;
6. replace current-authority/batch accounting;
7. integrate the live estimator, attachment lifecycle, and domain check exactly once;
8. migrate platform/app configuration and composition;
9. run focused sources/live/app gates, then the full clean exact-head checkpoint gate.

One integration owner must serialize lifecycle, session mutation, store detach, and permit
ownership. Reservation primitives and disjoint RED tests may proceed in parallel after the
lifecycle contract is frozen.
