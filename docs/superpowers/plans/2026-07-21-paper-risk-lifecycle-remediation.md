# Paper Risk and Lifecycle Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make paper execution exact, mark-aware, deadline-safe, durably checkpointed, and free of
detached task ownership.

**Architecture:** Extend the existing financial, approval, reconciliation, and checkpoint
capabilities as one ordered authority repair. Preserve deterministic single-writer paper state;
move filesystem publication and task reaping to bounded control-plane owners.

**Tech Stack:** Rust 1.97.1, `rust_decimal`, `num-bigint`, Tokio, cap-std, SHA-256, Serde.

## Global Constraints

- Work only in `.worktrees/paper-risk-lifecycle` on `fix/paper-risk-lifecycle` from audit base
  `ed137b39aea95c20cb1b8adb80786070f7671ef5`.
- Do not edit the root checkout, application, MCP, README, platform sources, workspace manifests,
  or `Cargo.lock`; root owns the narrow application/platform integration seam.
- Do not run Cargo until the integration owner grants the serialized slot. Run focused RED/GREEN
  gates first, then the scoped all-target/all-feature gates requested below.
- Keep `BigUint` out of per-market-event work. Exact risk products use stack-only cancellation;
  explicit fee settlement/reservation rounding may use the existing bounded BigUint boundary.
- Preserve bounded queues, private construction, deterministic ordering, audit identities, and
  zero mutation for known pre-attempt failure.
- Produce one cohesive implementation commit, not per-finding branches or commits.

---

### Task 1: Exact arithmetic, freshness, and flat-position invariants

**Files:**

- Modify: `crates/market-squawk-domain/src/financial.rs`
- Modify: `crates/market-squawk-domain/src/financial/exact_decimal.rs`
- Modify: `crates/market-squawk-domain/tests/financial_exactness.rs`
- Modify: `crates/market-squawk-execution/src/limits.rs`
- Modify: `crates/market-squawk-execution/src/risk.rs`
- Modify: `crates/market-squawk-execution/tests/risk_matrix.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/fees.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/ledger.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/ledger/recovery.rs`
- Modify: `adapters/market-squawk-adapter-paper/tests/paper_matrix.rs`

**Interfaces:**

- Produce `Money::checked_mul_decimal(Decimal) -> Result<Money, FinancialError>`.
- Produce `Money::checked_basis_points(BasisPoints, u32, RoundingPolicy) -> Result<Money,
  FinancialError>` using an exact pre-rounding rational.
- Risk reservation uses `RoundingPolicy::Ceiling` for nonnegative fees; settlement uses
  `RoundingPolicy::NearestEven`.

- [ ] Add one independent BigInt-oracle test proving exact multiplier rejection and adverse fee
  rounding where `Decimal::checked_mul` returns a rounded `Some`.

- [ ] Add one risk test that calls pre-authority evaluation at `now == market.valid_until()` and
  asserts `SourceStale`, unchanged account snapshot, and no approval audit event.

- [ ] Extend the existing paper ledger test: with `maximum_positions = 1`, open and close A, then
  reserve B; assert A's zero position and basis are absent.

- [ ] Request the serialized Cargo slot and run:

  ```bash
  cargo test -p market-squawk-domain --test financial_exactness --locked
  cargo test -p market-squawk-execution --test risk_matrix --locked
  cargo test -p market-squawk-adapter-paper --test paper_matrix --locked
  ```

  Expected RED: the oracle, equality, and capacity tests fail only because the new behavior is
  absent. Release the slot after recording the failures.

- [ ] Implement the two Money operations by reusing `exact_product` and a bounded exact-rational
  rounding helper; replace execution/paper multiplier and fee `Decimal` operations; fail leverage
  closed if either exact product is unrepresentable; change freshness to `now >= valid_until`;
  remove flat position and basis entries atomically and reject/compact them on recovery.

- [ ] Request Cargo and rerun the three commands. Expected GREEN: all three focused suites pass.

### Task 2: Qualified marks and complete account replacement

**Files:**

- Create: `adapters/market-squawk-adapter-paper/src/ledger/marks.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/ledger.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/ledger/account_state.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/ledger/recovery.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker/reconciliation.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/snapshot.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/config.rs`
- Modify: `crates/market-squawk-execution/src/adapter/account_state.rs`
- Modify: `crates/market-squawk-execution/src/account.rs`
- Modify: `crates/market-squawk-execution/src/account/contracts.rs`
- Modify: `crates/market-squawk-execution/src/account/replacement.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher/reconciliation.rs`
- Modify: `crates/market-squawk-execution/src/limits.rs`
- Test: `adapters/market-squawk-adapter-paper/tests/paper_matrix.rs`
- Test: `crates/market-squawk-execution/src/account/replacement/tests.rs`

**Interfaces:**

- `PaperMarkEvidence` has private construction from `ExecutionMarketUpdate` and retains terms,
  venue/generation binding, bid/ask, observation/freshness time, event class, and assessment digest.
- `ReconciledAccountState` schema version 3 carries settled capital, marked equity, peak marked
  equity, unrealized P&L, marked exposure, drawdown, and mark digest in addition to cash, realized
  P&L/loss, positions, and cost basis.
- `RiskLimits` checks marked equity/exposure/drawdown and combines realized loss with negative
  unrealized P&L without making the reversible unrealized component monotonic.

- [ ] Extend one existing end-to-end paper test: fill a long position, publish a current
  `DirectVerified` adverse bid/ask update, reconcile and replace the account, then assert the next
  risk evaluation rejects for capital/drawdown. Add one assertion that stale or mismatched evidence
  leaves the account ineligible and does not alter the last valid mark.

- [ ] Request Cargo and run the exact affected tests. Expected RED: mark projection fields or
  behavior are absent.

- [ ] Implement private mark admission and exact long-bid/short-ask revaluation; separate settled
  capital from marked equity; bind marks and valuation fields into configuration/checkpoint/account
  schema versions and reconciliation digests; validate replacement atomically and fail closed on
  incomplete marks.

- [ ] Request Cargo and rerun affected adapter/execution tests. Expected GREEN: adverse mark and
  replacement behavior pass with existing reconciliation tests.

### Task 3: Monotonic attempt authority and bounded task ownership

**Files:**

- Create: `crates/market-squawk-execution/src/task_reaper.rs`
- Modify: `crates/market-squawk-execution/src/lib.rs`
- Modify: `crates/market-squawk-execution/src/approval.rs`
- Modify: `crates/market-squawk-execution/src/adapter.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher/worker.rs`
- Modify: `crates/market-squawk-execution/src/dispatcher/lifecycle.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/config.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/matching.rs`
- Test: `crates/market-squawk-execution/tests/risk_matrix.rs`
- Test: `adapters/market-squawk-adapter-paper/tests/paper_matrix.rs`

**Interfaces:**

- `ApprovedOrderParts::monotonic_deadline` survives transfer; dispatcher constructs
  `ExecutionOperation` with `min(approval_deadline, operation_deadline)`.
- `ExecutionTaskReaper::try_reserve()` returns a non-cloneable permit acquired before spawn;
  `permit.transfer(JoinHandle)` is internal and non-fallible; `drain(deadline)` is the root-owned
  shutdown seam.
- Shipping paper calls remain cooperative; generic adapter attempts run in isolated owned tasks.

- [ ] Add a delayed paper-submit test whose approval expires after queueing but before the first
  paper mutation; assert no order, idempotency, ledger, sequence, or audit mutation.

- [ ] Add one execution lifecycle test where an adapter attempt has been polled and misses its
  deadline; assert `UncertainOutcome`, reconciliation-required state, retained reaper ownership,
  and refusal before spawn when the hard permit ceiling is exhausted.

- [ ] Request Cargo and run the affected tests. Expected RED: the monotonic deadline is discarded
  and handles are currently aborted then dropped.

- [ ] Preserve the deadline, isolate adapter calls, pre-reserve task ownership, transfer every
  timed-out/Drop handle, expose bounded drain status, cap paper orders/depth evaluations, and yield
  matching after a fixed work quantum without reordering candidates.

- [ ] Request Cargo and rerun affected execution/paper tests. Expected GREEN: expiry is zero
  mutation, attempted timeout is uncertain, and no handle is detached.

### Task 4: Capability-confined durable checkpoint receipts and final gates

**Files:**

- Create: `adapters/market-squawk-adapter-paper/src/checkpoint_repository.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/lib.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/snapshot.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/worker/reconciliation.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Modify: `adapters/market-squawk-adapter-paper/Cargo.toml`
- Test: `adapters/market-squawk-adapter-paper/tests/paper_matrix.rs`

**Interfaces:**

- `PaperCheckpointRepository::try_new(ArtifactRoot, maximum_bytes) -> Result<Self, _>` clones the
  retained directory capability and owns its repository identity/generation.
- `persist(&mut self, &PaperExecutionCheckpoint) -> Result<PaperCheckpointReceipt, _>` performs
  staged file/directory durability and bounded verified read-back.
- `PaperExecutionAdapter::acknowledge_persistence` accepts only the opaque non-Clone receipt plus
  dispatcher authority; `PaperExecutionCheckpoint::persistence_evidence` is removed.

- [ ] Replace caller-byte evidence in one existing checkpoint test with repository persistence;
  inject compact crash checkpoints around file sync, publication, directory sync, and read-back;
  assert no durable fence or compaction before a verified receipt and verified-existing publication
  accepts only identical bytes.

- [ ] Request Cargo and run the paper test. Expected RED: repository/receipt API is absent.

- [ ] Implement capability-relative private staging, content-addressed no-clobber publication,
  file and directory synchronization, bounded read-back/decode/hash verification, receipt binding,
  and receipt-gated acknowledgement/compaction. Request an exact platform patch from root only if
  the injected `ArtifactRoot::try_clone_directory` API is insufficient; do not edit platform.

- [ ] Request Cargo and run focused GREEN gates:

  ```bash
  cargo fmt --all --check
  cargo clippy -p market-squawk-domain -p market-squawk-execution -p market-squawk-adapter-paper --all-targets --all-features --locked -- -D warnings
  cargo test -p market-squawk-domain -p market-squawk-execution -p market-squawk-adapter-paper --all-features --locked
  cargo build -p market-squawk-domain -p market-squawk-execution -p market-squawk-adapter-paper --all-features --release --locked
  ```

  Expected: all commands exit 0 with no warnings. Root owns the workspace-wide exact-head gate.

- [ ] Run `git diff --check`, inspect the complete owned-file diff, commit one cohesive
  implementation change, and report the clean SHA plus RED/GREEN evidence.

- [ ] Hand root the exact application updates: inject `ArtifactRoot`, replace
  `persistence_evidence` calls with repository receipts, register/drain `ExecutionTaskReaper`, and
  update construction fields required by schema/config changes. Root also owns integration,
  push/GitHub issue closure, branch deletion, and worktree cleanup after acceptance.
