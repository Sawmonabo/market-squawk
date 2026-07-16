# Q2 live-runtime readiness audit

**Audit date:** 2026-07-16  
**Frozen root reviewed:** `5f7087480ffd1cc77bf18285a5c4c29af1dcec9c`  
**Disposition:** Task 8 is blocked on the contracts below; Tasks 5–7 must establish them first.

This preflight reviewed the Task 5/6 work in progress, the controlling Stage 1 plan, the domain
identity and live-evidence APIs, and the locally resolved Tokio 1.52.3, ArcSwap 1.9.2, and UUID
1.24.0 sources. It made no production-code changes. The controlling plan and local Task 7/8 briefs
were corrected after the audit.

## Verified deterministic routing vector

Routing V1 hashes these exact bytes:

```text
4d53514b5348415244010008636f696e62617365018f0000000070008000000000000001
```

The encoding is ASCII `MSQKSHARD`, version byte `01`, big-endian venue UTF-8 byte length `0008`,
the bytes for `coinbase`, and the UUID's exact 16 RFC/network-order bytes. FNV-1a 64-bit with offset
`0xcbf29ce484222325` and prime `0x00000100000001b3` produces
`0x28edee9cb1852659`; modulo 16 is shard 9.

V1 must hash `VenueId::as_str().as_bytes()` and `InstrumentId::as_uuid().as_bytes()` directly. It
must not use Display/Serde UUID text, native-endian integers, locale transforms, Unicode
normalization, `DefaultHasher`, or a dependency-defined unstable hash. Routing version and shard
count define an immutable runtime incarnation and are persisted in diagnostics and snapshots.

## Required contracts before Task 8

1. Task 5 must produce an owned, `Send + 'static`, non-Serde `CurrentDecodedProviderBatch` for actor
   ingress. It retains exact session/current-health/subscription/capture allocation identity. A
   borrowing proof, bare decoded batch, canonical event, or replay value cannot cross production
   ingress.
2. Capture uses a registry-owned one-way allocation per generation. Platform receives a non-Clone
   admission issuer and degradation-only handle. Its cloneable publisher cannot promote or rotate
   health. Successful admission returns a non-Serde receipt bound to exact frame evidence; all
   loss/failure paths synchronously degrade the allocation.
3. Shard mailboxes are bounded by admitted message count, retained bytes, and per-message bytes.
   A private command enum computes checked deep retained size. Count and byte permits are acquired
   without awaiting and retained through processing/discard.
4. Overflow cannot enqueue its own quarantine into the full mailbox. The bound ingress handle
   synchronously invalidates an exact one-way generation execution lease before returning any
   count-full, byte-full, overweight, checked-cost, or closed error.
5. Actors recheck current leases before state application and before features/strategy/issuance.
   Already queued commands for an invalidated generation are diagnostic-only and cannot mutate a
   replacement generation or produce action.
6. Task 7 capability issuance binds source-generation, capture/current-health, shard-liveness,
   runtime-incarnation, and checked state-revision allocations. Issue checks before and after nonce
   registration. Consume, risk, and dispatch recheck. Release invalidation and Acquire checks define
   the concurrency linearization boundary.
7. Snapshot publication uses crate-private `ArcSwap`, not Tokio `watch` as a value store. Tokio's
   watch receiver borrow holds a read guard that can block the producer. Optional notifications are
   independent bounded/coalescing hints.
8. The supervisor owns exactly the configured actors, gates ingress on complete startup, invalidates
   authority before every exit/shutdown, releases all permits, and joins or aborts-and-awaits every
   task to a deadline. No actor is detached.

## Snapshot and memory semantics

Snapshots are built completely from one committed owner state after the action decision and at a
bounded coalesced cadence. They include routing version/count, runtime incarnation, shard ID,
source/session generation, state/snapshot/health revisions, exact times, configured/state/output
depth, and dimension-specific completeness metadata. A cross-shard response reports a sorted
bounded revision vector; it does not claim a single-instant global `as_of`.

Peak live memory is documented as the sum of bounded instrument state, bounded mailbox bytes, one
bounded in-progress candidate, bounded snapshot construction, and a bounded trusted-reader
retention allowance. External services receive bounded DTOs, never snapshot cells, leases, issuers,
nonces, capabilities, or arbitrary subscription creation.

## Mandatory concurrency evidence

- Count-full, byte-full, overweight, checked-cost overflow, receiver closure, and permit release on
  every failure/cancellation/drop path.
- Invalidation before admission error return even when callers ignore the error; queued same-
  generation commands cannot reach strategy, issuer, risk, or dispatch.
- Deterministic barrier/model tests for issue, nonce registration, consume, risk, and dispatch
  interleaved with overflow, generation rollover, capture degradation, cancellation, and actor exit.
- Atomic complete snapshot N/N+1 observations, slow-reader nonblocking publication, exact
  truncation, and stale-snapshot inability to establish current authority.
- Partial-startup cleanup, unexpected actor exit, cancellation/receive races, shutdown permit
  release, deadline abort-and-await, and absence of detached tasks.

The stable V1 routing vector and single-writer actor direction are approved. Implementation is not
approved until the authority, byte-bounding, snapshot, and lifecycle contracts above exist and are
covered by the specified adversarial evidence.
