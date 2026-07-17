# Q2 Integrated Checkpoint Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILLS: Use
> `superpowers:subagent-driven-development` for execution,
> `superpowers:test-driven-development` for every behavior change,
> `superpowers:systematic-debugging` for unexpected failures, and
> `superpowers:verification-before-completion` before handoff.

**Goal:** Close every substantiated finding from the rejected exact Q2 checkpoint at
`651a01e120dfe27a598b9475296733d238d870b7` without reopening Q2-R01–R15.

**Architecture:** A registry-owned identity/time/persistence boundary makes source authority and
provider budgets non-aliasable, non-resettable, and fail closed. Structural memory models cover
every live and capture allocation. Application framing and shutdown are bounded. Machine-checked
documentation preserves one truthful checkpoint state.

**Tech stack:** Rust 1.97, Edition 2024, Tokio, Serde, SHA-256, bounded collections, atomic and
mutex-protected control state, Python standard-library policy tests, Proptest, and deterministic
paused-time/concurrency tests.

**Controlling design:**
[`2026-07-16-q2-integrated-checkpoint-remediation-design.md`](../specs/2026-07-16-q2-integrated-checkpoint-remediation-design.md)

## Execution status — 2026-07-16

This is an implementation progress record, not checkpoint approval. Root integration is clean at
`0f9b8cc` while Lane A remains active:

| Work | State | Evidence |
| --- | --- | --- |
| Platform authority-state store | Integrated | `61c4292`; 8 MiB bounded envelope, exclusive lock, no-follow confinement, atomic Unix replacement, directory sync, canonical verification |
| Lane A A1 | Frozen in lane | `f171961`; terminal health-epoch exhaustion |
| Lane A A2 | Frozen in lane | `d211e67`; canonical provider-budget identity |
| Lane A A3 | In progress | durable source-owned budget protocol using the integrated platform store |
| Lane A A4 | Pending A3 | sealed receive time, wall discontinuity latch, capture retained-size contract |
| Lane B B1–B2 | Integrated | `280fb0f`, `0f9b8cc`; live processing and snapshot/publication/reader structural memory ceilings |
| Lane C C1–C3 | Integrated | `a36e624`, `9911d2e`, `89b220b`, `a7be854`; bounded MCP framing, owned shutdown, diagnostic terminology |
| Root D1 | Integrated before this plan | semantic documentation-contract markers and deterministic policy test |
| Root D2 and exact-head gate | Pending all lanes | lifecycle remains `remediation-in-progress`; no approval claim |

## Audit base and mandatory barriers

The audit anchor is rejected, not approved. Every implementation worktree starts from the commit
containing this plan. Before editing, each worker must confirm:

```bash
git status --short
git rev-parse HEAD
git diff --check
```

Workers must not edit root `Cargo.toml`, `Cargo.lock`, `.github/workflows/ci.yml`, root checkpoint
documents, or another lane's files. A required dependency or shared-file change is reported to the
integration owner and serialized.

Every task follows red, green, focused gate, self-review, commit. A passing test written after the
implementation is not accepted as TDD evidence. Tests may expose test-only constructors but must
not widen production authority.

## Dependency DAG and wave ownership

```text
Plan/design freeze
├── Lane A: authority/persistence/capture memory (A1 -> A2 -> A3 -> A4)
├── Lane B: live memory (B1 -> B2)
├── Lane C: app process boundaries (C1 and C2 -> C3)
└── Root: documentation contract red test -> final document refresh
              │
              └──────────── all lane commits integrated ────────────┐
                                                                    ▼
                                                   exact-head full verification
                                                                    ▼
                                            three fresh Q2 re-review lanes
                                                                    ▼
                                     no findings -> immutable approval tag
                                     findings -> deduplicated remediation loop
```

| Wave | Lane | Start barrier | Exclusive ownership | Focused gate | Merge |
| --- | --- | --- | --- | --- | --- |
| 0 | Root | clean `651a01e` | design, plan, rejection ledger, progress | documentation tests, brand, diff | plan base |
| 1 | A | plan commit | sources crate; platform capture; domain capture contract | sources/platform/domain tests | 1 |
| 1 | B | plan commit | live crate only | live all-target/all-feature tests | 2 |
| 1 | C | plan commit | app source/MCP/config/tests only | app all-target/all-feature tests | 3 |
| 1 | Root | plan commit | root docs, README, Python documentation policy | Python docs/brand tests | 4 |
| 2 | Root | A–D integrated | conflict resolution, formatting, shared manifests if justified | complete locked local gate and audits | frozen candidate |
| 3 | Reviewers | clean frozen candidate | read-only authority, memory/lifecycle, architecture/security scopes | independent reports | approval or remediation |

Shared hotspots reserved to root are workspace manifests/lockfile, checkpoint truth, cross-lane
conflict resolution, and final evidence. Lane A owns both sides of the capture retained-size
contract to avoid a domain/platform split. Lane C must not edit README; root owns public-document
wording.

## Lane A — authority, identity, persistence, time, and capture memory

### Task A1: Make health-epoch exhaustion terminal

**Files:**

- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/health_authority.rs`
- Modify: `crates/market-squawk-sources/src/registry/current_batch/validation.rs`
- Modify: `crates/market-squawk-sources/src/registry/tests/temporal_cases.rs`
- Modify: affected authority fixtures under `crates/market-squawk-sources/tests/` and
  `crates/market-squawk-live/tests/support/`

- [ ] Add a failing test that forces health epoch `u64::MAX`, submits an invalidating health update,
  and proves current, live, queued, capture, and raw-frame authority all fail immediately.
- [ ] Add a failing recovery test proving a same-session retry cannot recover and a registry-owned
  replacement source epoch plus new session/generation can recover.
- [ ] Add a source-epoch-exhausted test proving no wrap/revival path exists.
- [ ] Implement an ordered terminal session latch before returning `HealthEpochExhausted`; retain
  typed error detail that distinguishes terminal exhaustion from ordinary rejection.
- [ ] Audit every lease validator for terminal/current checks using exhaustive call-site search.
- [ ] Run focused temporal/current/live/capture tests, format, and `git diff --check`.
- [ ] Commit only Task A1.

### Task A2: Derive canonical budget identity from endpoint and authorization evidence

**Files:**

- Modify: `crates/market-squawk-sources/src/policy/endpoint.rs`
- Modify: `crates/market-squawk-sources/src/policy/budget.rs`
- Modify: `crates/market-squawk-sources/src/policy/budget/coordinator.rs`
- Modify: `crates/market-squawk-sources/src/metadata.rs`
- Modify: `crates/market-squawk-sources/src/metadata/source.rs`
- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Modify: `crates/market-squawk-sources/src/policy/tests.rs`
- Modify: `crates/market-squawk-sources/tests/network_policy.rs`
- Modify: `crates/market-squawk-sources/tests/registry_authority.rs`

- [ ] Add failing alias-matrix tests: same endpoint/evidence with different provider and account
  labels shares one allocation; same endpoint/evidence with a conflicting policy rejects; public
  aliases share; locator-only differences do not split; distinct account scopes require distinct
  exact evidence digests.
- [ ] Add failing canonical endpoint tests for scheme/host case, IDNA form, default/explicit port,
  sorted allowlist order, duplicates, paths, query, fragment, IP forms, and ambiguous origins.
- [ ] Introduce a private-field `CanonicalBudgetIdentity` derived inside metadata/registry
  validation with domain-separated SHA-256. Do not accept an arbitrary digest or display alias.
- [ ] Separate display provenance from the canonical coordinator key and rederive legacy wire state
  before accepting it.
- [ ] Make identity derivation and authorization-mode mapping exhaustive; adding a mode must fail to
  compile until its quota identity rule is declared.
- [ ] Prove one shared request window/cooldown across aliases and no change to legitimate distinct
  audited accounts.
- [ ] Run policy, metadata, registry, serialization, and concurrency tests, format, and diff check.
- [ ] Commit only Task A2.

### Task A3: Persist conservative budget enforcement across process restart

**Files:**

- Modify: `crates/market-squawk-sources/src/policy/budget.rs`
- Modify: `crates/market-squawk-sources/src/policy/budget/coordinator.rs`
- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Create: `crates/market-squawk-sources/src/policy/budget/persistence.rs`
- Create: `crates/market-squawk-platform/src/authority_state.rs`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Create: `crates/market-squawk-platform/tests/authority_state.rs`
- Modify/Create: focused persistence tests under `crates/market-squawk-sources/tests/`

- [ ] Add a failing child-process test executable/test mode that consumes request capacity, exits,
  starts a clean process with only persisted state, and proves capacity/cooldown/disablement did not
  reset.
- [ ] Add failing restore tests for in-flight permits, availability generation, terminal state,
  refusal attempt, canonical ordering, wall rollback, future snapshot, corrupt counters, conflicting
  identity/policy, truncated state, and persistence write failure.
- [ ] Add a narrow required durability contract and versioned bounded budget checkpoint. Implement
  the production path-confined local store in the platform crate with a versioned SHA-256 integrity
  envelope, exclusive writer lock, temporary write and sync, atomic replacement, parent sync where
  supported, bounded read, and interrupted-write/corruption recovery tests.
- [ ] Make the authority-bearing registry constructor require this durable composition. Keep an
  explicitly named ephemeral constructor test-only or diagnostic-only and prove it cannot mint
  restart-persistent provider-budget authority.
- [ ] Persist restrictive transitions even when the sink fails by latching in-memory unavailability;
  prevent availability-increasing transitions unless their checkpoint is durable.
- [ ] Re-anchor saved wall deadlines to a fresh monotonic observation exactly once. Treat restored
  in-flight capacity conservatively until the saved window ends.
- [ ] Sort every structured scope/state before serialization and prove byte-identical output across
  at least 100 insertion permutations.
- [ ] Run subprocess, restore, budget, registry, platform state-store, Serde, corruption, lock-
  contention, and concurrency tests on the repository's supported local platforms. Confirm no
  external network is used.
- [ ] Commit only Task A3.

### Task A4: Seal receipt time, latch wall rollback, and close capture retained memory

**Files:**

- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/current_batch.rs`
- Modify: `crates/market-squawk-sources/src/live.rs`
- Modify: `crates/market-squawk-sources/src/capture.rs`
- Modify: `crates/market-squawk-domain/src/capture.rs`
- Modify: `crates/market-squawk-platform/src/capture.rs`
- Modify: `crates/market-squawk-platform/src/capture/control.rs`
- Modify: source/platform capture bridge and lifecycle tests

- [ ] Add compile-fail or signature coverage proving `RawFrameFactory` accepts no caller timestamp.
- [ ] Add failing tests for forged/old buffered frames, wall rollback between acceptance and latest
  high-water, rollback after successful frame/health observations, and discontinuity-latched current,
  live, queued, capture, and future frame rejection.
- [ ] Implement registry-sealed paired receipt observations and a non-resetting wall high-water
  discontinuity latch. Public provenance may expose wall time but not the validation capability.
- [ ] Add failing exact retained-size tests for maximum-capacity/small-length session strings and
  every allocation-bearing capture authority bundle field.
- [ ] Add a blocked-writer rotation test with one queued frame per unique generation, exact-limit
  acceptance, and one-byte-over rejection.
- [ ] Add an exhaustive retained-size method to the authority-bundle contract. Conservatively charge
  a complete generation per queued message and document the intentional overcharge.
- [ ] Migrate every raw frame and capture call site; do not add a compatibility constructor with a
  caller timestamp.
- [ ] Run domain/sources/platform/live bridge tests, all-target lane tests, format, and diff check.
- [ ] Inspect `git diff` for authority escape or filesystem/network work in the live event path.
- [ ] Commit only Task A4 and provide the ordered A1–A4 commit range.

## Lane B — complete live runtime memory ceiling

### Task B1: Derive maximum snapshot/delta processing peaks from actual allocations

**Files:**

- Modify: `crates/market-squawk-live/src/book.rs`
- Modify: `crates/market-squawk-live/src/provider_book.rs`
- Modify: `crates/market-squawk-live/src/processor/event.rs`
- Modify: `crates/market-squawk-live/src/processor/stream.rs`
- Modify: `crates/market-squawk-live/src/runtime/memory.rs`
- Modify: `crates/market-squawk-live/src/runtime/config.rs`
- Modify: `crates/market-squawk-live/src/runtime/tests/config_memory.rs`
- Modify/Create: maximum-shape memory fixtures and allocator high-water tests in the live crate

- [x] Inventory every allocation simultaneously reachable on maximum snapshot and delta paths and
  encode that inventory as exhaustive checked structural helpers beside the owning types.
- [x] Add failing exact-boundary tests demonstrating that the old `2 * maximum_message_bytes` model
  accepts a non-conservative configuration.
- [x] Cover command allocations, fixed active/inactive level buffers, inline exact lexemes,
  prior/candidate exact-level `Arc` pointees, canonical vectors and their boxed-slice conversion
  overlap, and the reusable shard scratch. The resulting production state has no rollback vectors
  or tree-node allocation assumption.
- [x] Use the larger derived snapshot/delta peak per shard. Reject arithmetic overflow and a runtime
  ceiling one byte below the derived maximum.
- [x] Add an allocator-observed high-water harness for all shards processing maximum-shape fixtures
  concurrently. Assert observed allocation is at or below the structural ceiling; do not replace the
  structural proof with an unexplained multiplier.
- [x] Run live configuration, processing, snapshot/delta, property, and all-shard tests; format and
  diff check.
- [x] Commit only Task B1 as lane commit `de2123a` (integrated as `280fb0f`).

### Task B2: Charge snapshot readers, publications, generations, and aggregate lease metadata

**Files:**

- Modify: `crates/market-squawk-live/src/snapshot.rs`
- Modify: `crates/market-squawk-live/src/snapshot/store.rs`
- Modify: `crates/market-squawk-live/src/runtime/memory.rs`
- Modify: `crates/market-squawk-live/src/runtime/config.rs`
- Modify: `crates/market-squawk-live/src/snapshot/store/tests.rs`
- Modify: `crates/market-squawk-live/src/runtime/tests/config_memory.rs`

- [x] Add failing tests with maximum reader permits retaining distinct old generations while every
  shard republishes a near-limit snapshot.
- [x] Derive charges for publication pointees/control blocks, reader-retained old generations,
  aggregate `Arc`/revision boxed slices, capacities, and owned permits.
- [x] Add single-shard and aggregate exact-ceiling/one-byte-under tests across multiple generations.
- [x] Make reader-count and shard-count multiplication checked and bounded before allocation.
- [x] Rerun the B1 allocator harness with worst-case readers and prove the combined ceiling remains
  conservative.
- [x] Run `cargo test -p market-squawk-live --all-targets --all-features --locked`, strict package
  Clippy, release build, format, and diff check.
- [x] Self-review the complete B1–B2 diff for double charge versus undercharge and commit Task B2 as
  lane commit `366690b` (integrated as `0f9b8cc`).

## Lane C — application framing, shutdown, and public diagnostic contracts

### Task C1: Replace allocating MCP line reads with bounded framing

**Files:**

- Modify: `apps/market-squawk/src/mcp.rs`
- Modify/Create: MCP framing tests under `apps/market-squawk/src/` or
  `apps/market-squawk/tests/`
- Modify: `scripts/tests/test_smoke_mcp.py` only if its protocol fixture must change

- [x] Extract the framing reader behind an async-read-generic test seam without exposing arbitrary
  input capabilities through MCP tools.
- [x] Add failing tests for exact maximum, maximum plus one followed by newline, maximum plus one at
  EOF, fragmented input, multiple valid frames, CRLF policy, empty lines, and cancellation.
- [x] Instrument reads/buffer capacity and prove the implementation never materializes a complete
  oversized line or retains more than maximum plus fixed scratch.
- [x] Implement fixed-capacity incremental framing. On oversize, emit one bounded protocol error and
  terminate the stdio session; do not unboundedly drain or resume.
- [x] Run MCP unit/integration/smoke tests, app package Clippy, format, and diff check.
- [x] Commit only Task C1 as lane commit `55eeaa1` (integrated as `a36e624`).

### Task C2: Bound source shutdown and make adapter operations cancellation-aware

**Files:**

- Modify: `apps/market-squawk/src/source/mod.rs`
- Modify: `apps/market-squawk/src/source/coinbase.rs`
- Modify: `apps/market-squawk/src/source_supervisor.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: app configuration types/tests in `apps/market-squawk/src/` as applicable
- Modify: `apps/market-squawk/tests/source_supervisor.rs`
- Modify: `apps/market-squawk/tests/live_runtime_composition.rs`

- [x] Add validated nonzero source-shutdown deadline configuration and precedence tests.
- [x] Add failing paused-time tests for a non-cooperative source, full event channel, stalled setup,
  stalled status/control send, stalled Pong/provider-initiated Close reply, immediate WebSocket
  transport drop on client cancellation, reconnect backoff, cooperative completion, and source
  task failure.
- [x] Race cancellation around the complete session future and each adapter blocking point.
- [x] Add a single application shutdown helper that signals cancellation, waits to deadline, aborts,
  awaits the join, and returns a typed outcome. Never drop/detach the handle.
- [x] Prove reverse-order shutdown continues after deadline abort and capture/event tasks are reaped.
- [x] Run source-supervisor, Coinbase deterministic, composition, config, and app all-target tests,
  format, and diff check.
- [x] Commit Task C2 and its self-review follow-up as lane commits `a7f2d47` and `2d842f1`
  (integrated as `9911d2e` and `a7be854`).

### Task C3: Make app/CLI/MCP diagnostic terminology unambiguous

**Files:**

- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/src/mcp.rs`
- Modify: `apps/market-squawk/src/diagnostic_engine.rs` rustdoc only if needed
- Modify/Create: MCP `tools/list` and CLI-help contract tests under `apps/market-squawk/tests/`

- [x] Add failing assertions requiring diagnostic/authority-free/paper-only/single-venue partial
  coverage wording and forbidding unqualified `validated`, `VALID`, or market `quality` claims.
- [x] Update capture command, Market snapshot/quality, Bot, and Risk descriptions consistently.
- [x] Explicitly distinguish app-local `QualityState` from canonical `DataQuality` and say the former
  can never establish `DirectVerified`.
- [x] Run app all-target/all-feature tests, CLI help, MCP tools/list smoke, strict package Clippy,
  format, and diff check.
- [x] Self-review C1–C3 for protocol compatibility and public-claim accuracy; commit Task C3 as lane
  commit `83edeb6` (integrated as `89b220b`).

## Root lane — checkpoint truth and deterministic coherence

### Task D1: Add a failing documentation-coherence policy test

**Files:**

- Modify: `scripts/tests/test_documentation_contracts.py`
- Modify/Create: a standard-library parser/helper under `scripts/` only if the test would otherwise
  duplicate parsing logic

- [ ] Add a failing test that reads current-state, gap-analysis, implementation-plan,
  checkpoint-review, and SDD-progress documents.
- [ ] Require one stable current Q2 candidate ID and lifecycle status across all five documents.
- [ ] Require explicit separation of rejected `581d4fd`, rejected `651a01e`, closed R01–R15, current
  Q2-I01–I11/Q2-M01–M02 remediation, and later-stage missing product capabilities.
- [ ] Reject stale phrases saying the old three lanes are still active, root integration remains
  future, or R01–R15 are currently unsafe after their superseding closure entry.
- [ ] Parse semantic markers/fields; do not use brittle line-number allowances.
- [ ] Confirm the new test fails for the intended contradictions before editing documents.

### Task D2: Refresh every authoritative document and public README

**Files:**

- Modify: `docs/architecture/current-state.md`
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Append: `docs/reports/q2-checkpoint-review.md`
- Modify: `.superpowers/sdd/progress.md`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md` only to add the completed-plan link if absent

- [ ] Preserve both rejected reviews verbatim or append-only; do not rewrite their historical
  dispositions.
- [ ] Describe the integrated candidate after A–C merge, its stable ID, and `pending exact-head
  re-review`; do not claim approval.
- [ ] Reclassify each original R01–R15 from current code evidence and classify every new finding as
  remediated/pending review only after its code and tests are integrated.
- [ ] Keep all remaining research, adapters, analytics, execution, valuation, and MCP capabilities
  truthful as Partial/Missing; remediation of Q2 is not product completion.
- [ ] Add the diagnostic terminology required by Task C3 to README/current-state.
- [ ] Link the completed Q3 production plan as an independently delivered provisional plan with its
  mandatory approved-Q2 refresh barrier.
- [ ] Make the D1 test pass, then run all documentation, brand, generated-artifact, credential, and
  boundary policy tests plus `git diff --check`.
- [ ] Commit D1–D2 only after the three implementation lanes are integrated so the status is true.

## Integration protocol

### Task I1: Integrate lane commits without weakening ownership

- [ ] Require each lane to report base, ordered commits, focused red/green evidence, self-review,
  final status, and `git diff --check`.
- [ ] Cherry-pick Lane A A1–A4 in order. Resolve source/platform conflicts only in root and rerun its
  package gates immediately.
- [x] Cherry-pick Lane B B1–B2, rerun live tests and memory boundary fixtures.
- [x] Cherry-pick Lane C C1–C3, rerun app tests, CLI help, and MCP smoke.
- [ ] Inspect the combined dependency graph and `Cargo.lock`. Reject unexplained dependency or
  feature changes.
- [ ] Complete root D1–D2 against the integrated code and commit.
- [ ] Run `git diff --check`, confirm a clean worktree, record the exact candidate hash, and prohibit
  further edits while verification/review runs.

### Task I2: Run the clean exact-head gate

Run from a clean worktree and assert HEAD/status are unchanged before and after:

```bash
candidate="$(git rev-parse HEAD)"
test -z "$(git status --short)"

./scripts/verify.sh
cargo deny check
cargo audit --deny warnings
gitleaks dir --redact --no-banner --config .gitleaks.toml .
gitleaks git --redact --no-banner --config .gitleaks.toml

test "$(git rev-parse HEAD)" = "$candidate"
test -z "$(git status --short)"
git diff --check
```

The verification record must enumerate Python policy tests, strict locked all-target/all-feature
Clippy, workspace tests/doctests, release build, warning-denied rustdoc, offline CLI/MCP smoke,
dependency/license/advisory/credential/generated-artifact checks, and the new subprocess/memory/
framing/shutdown regressions. Hosted CI remains separate optional evidence.

### Task I3: Perform the Q2 exact-head re-review

Dispatch three fresh read-only specialist lanes against the same unchanged commit:

1. source authority, canonical identity, restart persistence, receipt time, wall continuity, and
   capture accounting;
2. live processing/snapshot memory, concurrency, cancellation, source shutdown, and bounded MCP
   framing; and
3. architecture boundaries, capability non-bypass, provider-access safety, documentation coherence,
   CLI/MCP terminology, audits, and exact-head evidence.

Reviewers must explicitly disposition Q2-I01–I11 and Q2-M01–M02 plus confirm R01–R15 remain closed.
Do not mutate the candidate between reviewer batches. Union and deduplicate all findings before any
remediation. Every substantiated Critical, Important, and Minor finding blocks approval.

If all three approve, create an annotated local tag pointing to the unchanged commit, for example:

```bash
git tag -a checkpoint/q2-approved-2026-07-16 "$(git rev-parse HEAD)" \
  -m "Q2 approved after exact-head gate and three independent re-reviews"
```

The tag is approval evidence without a post-review commit. If any finding remains, do not tag or
claim approval; write one deduplicated remediation delta and repeat the exact-head gate/re-review.

## Definition of this plan's completion

This plan is complete only when:

- all thirteen remediations have failing-before/passing-after tests;
- every lane is integrated with no ownership or manifest drift;
- all authoritative documents describe one candidate and truthful status;
- the complete exact-head local gate and audits pass on a clean unchanged commit;
- three independent re-review scopes report no unresolved severity; and
- the immutable approval tag points to that exact commit.

This closes Quarter 2. It does not claim the later research, adapters, analytics, modeling,
portfolio, risk/execution, valuation, full MCP, performance, fuzzing, or release capabilities are
already complete.
