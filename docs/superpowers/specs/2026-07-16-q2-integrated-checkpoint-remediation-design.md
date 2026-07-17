# Q2 Integrated Checkpoint Remediation Design

## Document control

- Audit anchor: `651a01e120dfe27a598b9475296733d238d870b7`
- Anchor disposition: rejected by three independent read-only reviewers
- Findings: zero Critical, eleven Important, three Minor; thirteen deduplicated remediations
- This document is a design input, not checkpoint approval.
- Q2 approval requires a new clean exact-head gate and independent re-review after every finding is
  closed.

## Goal

Close the adjacent production-boundary defects found after the original Q2-R01–R15 remediation:
terminal authority behavior, canonical provider/account identity, restart-durable budget
enforcement, trusted receipt and wall-clock continuity, complete retained-memory ceilings, bounded
application framing and shutdown, truthful diagnostic terminology, and one coherent checkpoint
record.

The correction must preserve the already-closed Q2 invariants. In particular, no remedy may make
live authority serializable or clonable, move control-plane I/O into the event-to-action path,
weaken bounded queues, detach workers, or make provider restrictions avoidable.

## Security and provider-access boundary

Provider access is limited to user-authorized and published interfaces. Identity/account rotation,
browser or TLS fingerprint concealment, CAPTCHA or anti-bot bypass, proxy rotation intended to
defeat blocking, distributed quota evasion, stealth scraping, and access-control circumvention are
permanently prohibited.

Canonical identity exists to make one legitimate quota harder to multiply. It must not create or
select alternate identities. Retry-After, refusal, disable, and terminal states remain authoritative
fail-closed transitions.

## Remediation map

| ID | Severity | Contract to establish |
| --- | --- | --- |
| Q2-I01 | Important | Health-epoch exhaustion terminally invalidates the current session and every live/queued lease before returning. |
| Q2-I02 | Important | Budget scope is derived from registry-owned canonical endpoint and authorization evidence identity, never provider/account display aliases. |
| Q2-I03 | Important | Budget enforcement state survives a clean process restart and restores conservatively after an unclean or temporally ambiguous restart. |
| Q2-I04 | Important | Raw transport receipt time is sampled and sealed by registry/reader authority; adapters cannot provide it. |
| Q2-I05 | Important | The sealed registry clock retains a wall high-water and latches rollback/discontinuity fail closed. |
| Q2-I06 | Important | Live runtime memory covers every simultaneously live snapshot/delta processing allocation. |
| Q2-I07 | Important | Snapshot reader, publication, generation, permit, handle, and aggregate-lease metadata are included in the runtime ceiling. |
| Q2-I08 | Important | Capture admission accounts for frame identity and every uniquely retained generation/bundle allocation. |
| Q2-I09 | Important | Application source shutdown has a validated deadline, cancellation race, abort-and-await fallback, and typed outcome. |
| Q2-I10 | Important | MCP framing never materializes more than the configured maximum request plus bounded framing state. |
| Q2-I11 | Important | Architecture, gap, implementation, checkpoint, and progress artifacts describe one current candidate and disposition. |
| Q2-M01 | Minor | Persisted budget state is byte-deterministic across insertion orders. |
| Q2-M02 | Minor | Public app, CLI, MCP, and README wording cannot conflate diagnostic `VALID` with canonical `DirectVerified`. |

## Terminal health authority

Ordinary pre-publication validation remains failure atomic. Exhausting a revocation/authority epoch
is different: inability to mint the next epoch must never preserve the authority that an
invalidating update was meant to revoke.

The session lease therefore has a terminal latch. On health-epoch exhaustion it performs this
ordered transition before returning the typed error:

1. mark live qualification false;
2. close the validity interval;
3. mark the session non-current and terminal;
4. invalidate capture completeness and all current/live/queued validation paths; and
5. publish no replacement authority.

The latch never wraps or resets. Recovery requires registry-controlled replacement with a higher
source epoch and a new session/generation. If the source epoch is also exhausted, the source remains
terminal and must be reconfigured under a new audited identity; a local retry cannot reactivate it.

## Canonical provider and authorization identity

Human-readable provider names, account labels, authorization-basis strings, and source IDs are
metadata. They are not quota keys.

For remote sources, the registry derives an opaque `CanonicalBudgetIdentity` from invariant
evidence already inside the trusted configuration boundary:

- a canonical, sorted set of normalized allowlisted endpoint origins (scheme, IDNA-normalized host,
  and effective port); and
- for account-qualified access, a registry-owned authorization-subject capability minted from the
  exact authorization-evidence content digest, authorization mode, and stable credential or
  entitlement record held by the trusted composition boundary.

Locators and display labels do not affect the identity. Public access is keyed only by the
canonical endpoint-origin set. User-authorized and remotely licensed access additionally require
the opaque authorization-subject capability. Two declarations using different provider/account
aliases but the same endpoint and credential/entitlement record resolve to one identity and one
allocation, even when their display basis differs. A conflict between aliases or policies is
rejected without publication. Distinct account allocations require distinct evidence-backed
credential/entitlement records resolved by the trusted composition; a caller-supplied label or
digest alone can never create one.

The authorization-subject and budget-identity types have private fields. The subject capability is
non-serializable and non-clonable outside the trusted resolver; durable state stores only its
versioned stable subject identifier and evidence binding. Budget identity uses canonical
serialization and domain-separated SHA-256, with no constructor that accepts an already-computed
caller key. Source metadata retains display provenance separately. Deserialized legacy state is
rederived and checked rather than trusted.

This design prevents accidental or intentional alias multiplication within Market Squawk's trusted
configuration. It does not claim to identify credentials outside the configured credential store,
and it never probes, rotates, or compares credentials by making provider requests.

## Restart-durable provider budgets

Process-lifetime coordination is necessary but insufficient. The persisted authority envelope is
extended with versioned, bounded, canonically ordered budget checkpoints containing:

- canonical identity and policy;
- window start wall time and requests used;
- conservative in-flight count;
- cooldown wall deadline and refusal/backoff attempt;
- disabled, terminal, and poisoned state;
- availability generation; and
- the trusted wall high-water under which the snapshot was written.

Monotonic instants are never serialized. Restore observes a fresh paired wall/monotonic time and
converts future wall deadlines once. It rejects wall rollback, future snapshots, policy/identity
conflicts, invalid counters, arithmetic overflow, or unavailable persistence. On clean restart,
in-flight permits restore as consumed until the saved window closes; this intentionally
over-enforces rather than manufacturing capacity. Cooldowns, disablement, terminality, refusal
history, request usage, and availability generations never reset through restart.

Every authority-relevant mutation produces a new bounded checkpoint through a required durability
sink before newly available authority is returned. Failure to persist a restrictive transition
must still revoke in-memory authority and latch the allocation unavailable; failure to persist an
availability-increasing transition prevents the increase. Tests may use an explicit in-memory sink,
but production registry construction requires the production local durable store. The source crate
owns the state machine and a narrow persistence contract. The platform crate implements a
path-confined atomic local state file with an integrity-protected versioned envelope, write-and-sync
temporary file, atomic replacement, parent-directory sync where supported, single-writer lock, and
fail-closed recovery of interrupted writes. Filesystem I/O remains outside the live event path.

Subprocess tests use a temporary file-backed test sink to prove a fresh address space cannot reset
quota state. They do not depend on process-global test ordering.

## Trusted receive time and wall continuity

`RawFrameFactory` no longer accepts `received_at`. It samples the same sealed registry clock that
owns session time and embeds an opaque paired wall/monotonic receipt observation with the frame.
The public frame exposes the wall timestamp for provenance, but qualification validates the opaque
receipt against its session, registry clock identity, wall high-water, and monotonic bounds.

The clock keeps an atomic wall high-water. Any observation below the last successful wall value
latches a discontinuity. Once latched, it invalidates current qualification and refuses new frame,
health, and budget authority for that registry instance. It never silently accepts a later recovery
within the same clock generation. Recovery requires a new registry composition whose durable state
proves a non-regressing wall observation.

This catches rollback between the original acceptance wall and a later high-water, not merely
rollback below the original acceptance wall. Heartbeats remain connection-health evidence and do
not advance market-data freshness.

## Closed retained-memory ceilings

Memory accounting describes actual simultaneously reachable allocations, not a multiplier over
wire bytes. Every allocation-bearing type participating in admission exposes an exhaustive checked
retained-size calculation. Struct destructuring and exhaustive enum matches make new fields or
variants a compile-time accounting decision.

### Live book processing

The per-shard peak includes, separately for maximum-shape snapshot and delta paths:

- the admitted command and decoded nested allocations;
- normalized changes and exact lexeme clones;
- scaled level updates;
- scaled and exact rollback entries at their maximum capacities;
- candidate scaled/exact maps and conservative tree-node allocations;
- canonical `BookChange` construction;
- prior committed books retained until publication; and
- bounded sorting or snapshot scratch that overlaps the transaction.

The configuration uses the larger derived peak. Exact-boundary tests accept at the computed ceiling
and reject one byte below. Allocator-observed high-water tests validate the structural model under
all-shard concurrent maximum-shape fixtures; measurement supplements rather than replaces the
closed formula.

### Snapshot readers and publications

The runtime ceiling also includes:

- publication `Arc` pointees and control blocks;
- per-reader retained old generations;
- aggregate lease `Arc` handle and revision arrays, including allocation capacities;
- owned permit state; and
- all shard-generation combinations allowed by the configured reader count.

The worst case assumes slow readers retain distinct old publications while new generations are
published. Single-shard and aggregate exact-boundary tests cover maximum readers and multiple
generations.

### Capture queue

Capture queue accounting uses a closed retained-size contract on `CaptureAuthorityBundle` and the
generation state. A queued record charges its exact frame/payload allocation, the complete
capacity-sensitive session identity allocation, and a conservative complete generation/bundle
allocation. Charging the shared generation once per message is deliberately conservative and keeps
admission O(1); it avoids a separate reference-counted reservation state that could fail to release
on exceptional paths. The documented queue ceiling is therefore an upper bound even when a blocked
writer retains one frame from each of many rotated generations.

## Bounded source shutdown

Source shutdown has a validated nonzero configuration deadline independent of capture-writer
shutdown. The supervisor races cancellation against the entire session future, including connect,
subscription, status sends, event sends, control writes, close, and reconnect backoff.

At application shutdown:

1. revoke source/capture authority and signal cancellation;
2. wait until the configured source deadline;
3. abort a non-cooperative source task;
4. await the aborted `JoinHandle` so no task is detached; and
5. return a typed graceful, aborted-at-deadline, or task-failed outcome before reverse-order
   shutdown continues.

Adapter operations remain individually cancellation-aware so abort is a last-resort ownership
backstop. Tests cover a non-cooperative source, a full event channel, stalled setup/control writes,
and clean cooperative shutdown under paused deterministic time.

## Bounded MCP framing

MCP stdio reads into a reusable fixed-capacity buffer and scans incrementally for a newline. It
never calls `read_line`/`lines` on untrusted input. A frame may materialize at most
`MAX_MCP_LINE_BYTES` plus one detection byte and fixed reader scratch.

An exact-limit line is accepted. A maximum-plus-one line, with or without a later newline, produces
one bounded framing error and terminates the stdio session. Termination is intentional: it avoids an
unbounded drain and prevents ambiguous resynchronization after a protocol violation. Tests use an
instrumented reader to assert the maximum retained/requested buffer size, not only the returned
error.

## Diagnostic terminology

The compatibility application plane is named consistently as diagnostic and authority-free.
Public CLI help, MCP `tools/list`, README, and current-state documentation must say:

- Coinbase Exchange single-venue, partial coverage;
- diagnostic snapshot and app-local `QualityState`;
- diagnostic `VALID` is not canonical `DataQuality::DirectVerified`;
- no diagnostic value can mint production live authority; and
- paper simulation only, with no production order authority.

Policy tests reject unqualified claims of validated market quality on these surfaces.

## Checkpoint documentation coherence

The rejected `581d4fd` and `651a01e` reviews remain append-only history. Current-state,
gap-analysis, implementation-plan, checkpoint-ledger, and SDD-progress documents use one stable Q2
candidate identifier and one lifecycle vocabulary:

```text
rejected -> remediation in progress -> pending exact-head re-review -> approved exact head
```

The documents distinguish the historical findings, their code dispositions, the new adjacent
findings, later-stage product capabilities, and optional hosted-CI evidence. A deterministic parser
checks candidate identity, lifecycle status, prior-finding disposition, and prohibited stale phrases
without line-number pins.

Because a commit cannot contain its own hash, committed documents identify the stable candidate and
state that the review target is repository `HEAD`. The review command records the resolved hash
externally before any test runs. Final approval is attached to that unchanged commit with an
annotated local checkpoint tag; creating the tag does not mutate the reviewed commit. No document
commit is made after approval without invalidating it and triggering re-review.

## Hot-path and blast-radius constraints

The live event-to-action path remains free of SQLite, DataFusion, Parquet, Python, MCP, LLM,
filesystem operations, unrelated network requests, and unbounded writes. Budget persistence occurs
at control-plane mutations and source request admission, never per market event. Trusted receipt
sampling is constant-time and local. Memory accounting is startup/admission arithmetic, not an
allocator walk in the actor loop.

The remediation must audit source serialization, restore, capture bridge, current-batch admission,
live qualification, app composition, CLI/MCP schemas, README claims, all downstream fixtures, and
the future Q3 plan's refresh assumptions.
