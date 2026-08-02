# Paper Financial Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the frozen paper cash-overflow, asynchronous reconciliation, exact restart, and
final-checkpoint ordering findings as one production authority contract.

**Architecture:** A monotonic atomic fence closes risk admission before paper financial mutation,
then an application-owned bounded supervisor reconciles the exact sequence outside the live path.
A capability-confined fixed current manifest durably binds the paper checkpoint and account replay
state, and shutdown serializes quiescence, reconciliation, manifest publication, acknowledgement,
and worker termination.

**Tech Stack:** Rust 1.97.1, Tokio, cap-std, Serde, SHA-256, rust_decimal.

## Global Constraints

- Work only in `.worktrees/paper-financial-recovery` on `fix/paper-financial-recovery` from
  `136b556`.
- Do not edit README, project memory, checkpoint reports, workspace manifests, or `Cargo.lock`.
- Preserve bounded queues, nonblocking live handoffs, typed errors, private construction,
  one-use authorities, and checked financial arithmetic.
- Add only four causal proofs to existing relevant test suites.
- Run focused locked format, test, strict Clippy, and release build gates; root owns the workspace
  gate, GitHub, integration, and cleanup.

---

### Task 1: Cash overflow fails closed

**Files:**

- Modify: `crates/market-squawk-execution/src/account.rs`
- Test: `crates/market-squawk-execution/tests/risk_matrix.rs`

**Interfaces:**

- Preserve `AccountRiskCoordinator::try_reserve` and return
  `AccountRiskViolation::ArithmeticOverflow` when active cash plus the candidate cannot be
  represented.
- Keep `ReservationCalculation::cash` and `ReservationCalculation::exposure` independent.

- [ ] Add one risk-matrix test with individually valid cash reservations whose aggregate plus the
  candidate overflows `Decimal`.
- [ ] Run the exact test and confirm it fails because approval/reservation is minted or overflow is
  omitted.
- [ ] Replace the `is_ok_and` cash comparison with an explicit checked-add match that records
  `ArithmeticOverflow` on failure and `InsufficientCash` only for a representable excess.
- [ ] Rerun the exact test and affected execution suite.

### Task 2: Sequence-fenced production reconciliation

**Files:**

- Modify: `crates/market-squawk-execution/src/account.rs`
- Modify: `crates/market-squawk-execution/src/account/replacement.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher/lifecycle.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker/reconciliation.rs`
- Modify: `apps/market-squawk/src/paper_bot.rs`
- Test: `apps/market-squawk/tests/risk_dispatch_pipeline/paper.rs`

**Interfaces:**

- Produce a cloneable, private-mutation account reconciliation fence from the coordinator.
- Produce a bounded coalescing paper sequence reader owned by the application supervisor.
- Produce dispatcher quiesce and reconciliation status methods that do not reopen admission.

- [ ] Change the existing paper vertical to omit its manual reconciliation after a fill and wait
  for the production runtime to publish the exact reconciled account revision.
- [ ] Run that test and confirm it fails on stale coordinator state.
- [ ] Fence each paper account-risk mutation before commit and publish its sequence after commit.
- [ ] Start one bounded supervisor in production composition; coalesce notifications, drive
  dispatcher reconciliation until the fence catches up, and fail admission throughout any gap.
- [ ] Make cancellation and supervisor shutdown deterministic, then rerun the vertical.

### Task 3: Fixed-manifest restart recovery

**Files:**

- Modify: `adapters/market-squawk-adapter-paper/src/checkpoint_repository.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/snapshot.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Modify: `apps/market-squawk/src/paper_bot.rs`
- Modify: `apps/market-squawk/src/paper_bot/defaults.rs`
- Test: `adapters/market-squawk-adapter-paper/src/checkpoint_repository.rs`
- Test: `apps/market-squawk/tests/risk_dispatch_pipeline/paper.rs`

**Interfaces:**

- Repository startup returns either `Fresh` or one exact recovered checkpoint plus account replay
  snapshots from the fixed current manifest.
- Manifest persistence accepts the checkpoint and coordinator replay snapshots and returns the
  existing opaque receipt only after durable verified publication.

- [ ] Extend the repository durability test so a new repository instance restores the exact
  identity/generation/object and rejects partial namespace state.
- [ ] Extend the application paper vertical to restart from the published current manifest and
  prove account revision, replay fence, paper state, and open-order ownership survive.
- [ ] Run both tests and confirm missing recovery APIs/state cause failure.
- [ ] Implement bounded no-follow current-manifest read/publication and exact restore validation.
- [ ] Compose recovered paper and risk state before live/source admission and rerun both tests.

### Task 4: Final checkpoint linearizes after quiescence

**Files:**

- Modify: `crates/market-squawk-execution/src/dispatcher/lifecycle.rs`
- Modify: `apps/market-squawk/src/paper_bot.rs`
- Test: `apps/market-squawk/tests/risk_dispatch_pipeline/paper.rs`

**Interfaces:**

- Shutdown order is live/action close, dispatcher admission close and queue drain, final
  reconciliation, replay snapshot, current-manifest persistence, paper acknowledgement, paper
  shutdown, and reaper drain.
- `ProductionPaperBotShutdown::is_complete` additionally proves persisted and final paper sequences
  are identical.

- [ ] Add one in-flight submit versus shutdown test with a barrier-controlled adapter/paper event;
  assert a complete outcome contains the accepted submit in the final checkpoint sequence.
- [ ] Run it and confirm the current pre-dispatch checkpoint ordering fails.
- [ ] Split dispatcher quiescence from terminal shutdown, run final reconciliation after quiescence,
  bind replay snapshots into the current manifest, and compare receipt sequence to final snapshot.
- [ ] Rerun the test and the complete focused application suite.

### Task 5: Focused exact-head verification and handoff

**Files:** all paths modified by Tasks 1-4.

- [ ] Run `cargo fmt --all --check`.
- [ ] Run focused locked tests for `market-squawk-execution`,
  `market-squawk-adapter-paper`, and the application paper/risk pipeline.
- [ ] Run strict all-target/all-feature locked Clippy for those three packages with `-D warnings`.
- [ ] Run their locked all-feature release build.
- [ ] Run `git diff --check`, inspect the complete diff for financial/lifecycle ripple errors, and
  verify the exact committed head is clean.
- [ ] Return exact SHAs, changed paths, RED/GREEN evidence, and integration risks without pushing or
  mutating GitHub.

