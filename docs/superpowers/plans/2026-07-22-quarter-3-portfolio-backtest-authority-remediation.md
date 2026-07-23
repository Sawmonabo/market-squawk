# Quarter 3 Portfolio and Backtest Authority Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the five substantiated Quarter 3 portfolio/backtest authority and resource-bound
defects without widening product scope or weakening the accepted model and fair-value slices.

**Architecture:** Run two disjoint product lanes. Portfolio analytics derives one immutable
evidence identity from an actual private-field `PortfolioRevision`, binds explicit policy and time
semantics, and admits work before allocation. Backtesting consumes a catalog-minted historical
instrument-definition receipt and validates the complete durable attempt namespace against the
canonical reservation before treating any attempt or terminal as authoritative.

**Tech Stack:** Rust 1.97.1, SQLite, Arrow query receipts, `rust_decimal`, SHA-256, cap-std, Serde.

## Global Constraints

- Exact plan/audit base: release commit `be886b9` on `release/market-squawk-v0.1.0`.
- Portfolio lane: `.worktrees/portfolio-pit-analytics-authority` on
  `feature/portfolio-pit-analytics-authority`.
- Backtest lane: `.worktrees/backtest-reference-authority` on
  `feature/backtest-reference-authority`.
- The lanes may run concurrently. Portfolio owns only `market-squawk-portfolio` plus its existing
  tests. Backtest owns data-catalog instrument receipt APIs, `market-squawk-backtesting`, and the
  application backtest seam. Root alone owns this plan, the delivery ledger, project memory,
  GitHub state, integration, and checkpoint evidence.
- Do not change `Cargo.toml`, `Cargo.lock`, dependencies, migration schemas, CLI/MCP surfaces,
  model/fair-value code, or `BacktestCohortPlan` inventory coupling.
- Reuse only existing consolidated test targets. Do not create a test file, test executable,
  checker script, prose test, snapshot test, or duplicate broad gate.
- All lane Cargo commands use `CARGO_INCREMENTAL=0` and the worktree-local default `target/`.
  `CARGO_TARGET_DIR`, `CARGO_BUILD_BUILD_DIR`, compiler wrappers, and cross-worktree caches are
  prohibited. Stop and clean a lane if its target reaches 10 GiB.
- Run focused RED/GREEN and affected-package gates only. Root runs the one workspace full gate only
  after the exact-candidate Quarter 3 rereview accepts the remediation.
- Preserve checked arithmetic, fallible reservations, deterministic ordering, private authority
  construction, stable historical trial wire compatibility, and the one-way
  `execution -> portfolio` dependency.

---

### Task 1: Portfolio point-in-time analytics authority and bounded work

**Files:**

- Create: `crates/market-squawk-portfolio/src/analytics_evidence.rs`
- Modify: `crates/market-squawk-portfolio/src/lib.rs`
- Modify: `crates/market-squawk-portfolio/src/evidence.rs`
- Modify: `crates/market-squawk-portfolio/src/ledger.rs`
- Modify: `crates/market-squawk-portfolio/src/performance.rs`
- Modify: `crates/market-squawk-portfolio/src/exposure.rs`
- Modify: `crates/market-squawk-portfolio/src/attribution.rs`
- Modify: `crates/market-squawk-portfolio/src/risk.rs`
- Test: `crates/market-squawk-portfolio/tests/accounting.rs`
- Test: `crates/market-squawk-portfolio/tests/analytics.rs`

**Interfaces:**

- Produce `AnalyticsPolicyBinding::try_new(id: SourceIdentifier, version: NonZeroU32)`.
- Produce `PortfolioAnalyticsEvidence::try_from_revision(revision: &PortfolioRevision,
  effective_through: Timestamp, available_through: Timestamp, valuation_policy:
  AnalyticsPolicyBinding, fx_policy: AnalyticsPolicyBinding, as_of_policy:
  AnalyticsPolicyBinding) -> Result<Self, PortfolioError>`.
- The evidence clones the exact revision dataset, point-in-time content/audit identities, and
  canonical sources; requires both time horizons no later than `revision.evidence().as_of()`; binds
  the revision ID, all three policy identities/versions, and those Task 11 identities into a
  versioned SHA-256 semantic digest; and has no public field constructor or deserializer.
- `PerformanceReport`, `ExposureReport`, `AttributionReport`, and `PortfolioRiskReport` require
  `&PortfolioAnalyticsEvidence`, reject a revision mismatch, validate their time horizon against
  it, and retain `analytics_evidence_digest: Sha256Digest`.
- `InstrumentClassification::try_new` and `ScenarioDefinition::try_new` receive
  `PortfolioLimits` and reject count/retained-byte excess before sorting or other materialization.
- Use `max_factors` for per-classification and per-scenario dimensions, `max_scenarios` for scenario
  count, `max_history` for return/loss/period history, `max_results` for checked aggregate
  factor/shock/work/result units, and `max_retained_bytes` for conservative preflight plus exact
  retained-result recheck. Do not add another limit vocabulary.

- [ ] **Step 1: Add the two critical RED cases to existing harnesses**

  In `accounting.rs`, construct a corporate-action plan whose `knowledge_cutoff` is later than the
  revision `as_of` while its valuation cutoff is not; assert `RevisionEvidence::try_new` rejects it
  with `PortfolioError::EvidenceMismatch`. In `analytics.rs`, add one case proving future analytics
  evidence is rejected and one deliberately small limit proving factor/shock/retained work fails
  before a report is produced. Update the existing successful analytics case to show the intended
  evidence-bearing API; do not duplicate its financial assertions.

- [ ] **Step 2: Run focused RED**

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-portfolio \
    --test portfolio --all-features --locked
  ```

  Expected: compilation or behavioral failure only because the evidence/bound interfaces and
  cutoff rejection are absent. Record the failure and target size.

- [ ] **Step 3: Enforce corporate-action cutoffs at both invariant boundaries**

  In `RevisionEvidence::try_new`, reject a binding when either `knowledge_cutoff > as_of` or
  `valuation_cutoff > as_of`. Retain the same two checks in `PortfolioLedger::validate_bindings` so
  a future internal construction path cannot bypass publication admission.

- [ ] **Step 4: Implement evidence-bound reports and pre-allocation limits**

  Add the immutable evidence type and canonical digest. Validate it before any report work. Use
  checked sums/products to bound factor occurrences, worst-case exposure lines, scenario shocks,
  `positions * shocks` scenario work, histories, temporary allocation rows, and output rows before
  allocating maps/vectors/strings. Use fallible reservations after admission. Calculate exact
  Rust-visible retained bytes for each final report and reject over the configured ceiling.

- [ ] **Step 5: Run portfolio GREEN and quality gates**

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-portfolio \
    --test portfolio --all-features --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-portfolio --all-features --locked
  CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-portfolio \
    --all-targets --all-features --locked -- -D warnings
  cargo fmt --all --check
  git diff --check
  ```

  Expected: existing accounting/analytics behavior remains intact; the new authority and limit
  cases pass; no other crate, dependency, or test target changes.

- [ ] **Step 6: Self-review and commit one cohesive lane**

  Inspect every public constructor/call site, report the exact test counts and target size, then
  commit with `fix(portfolio): bind point-in-time analytics authority`.

### Task 2: Historical instrument-definition and attempt-recovery authority

**Files:**

- Modify: `crates/market-squawk-data/src/catalog.rs`
- Modify: `crates/market-squawk-data/src/catalog/records.rs`
- Modify: `crates/market-squawk-data/src/catalog/types.rs`
- Modify: `crates/market-squawk-data/src/lib.rs`
- Test: `crates/market-squawk-data/tests/catalog.rs`
- Modify: `crates/market-squawk-backtesting/src/dataset.rs`
- Modify: `crates/market-squawk-backtesting/src/dataset/admission.rs`
- Modify: `crates/market-squawk-backtesting/src/experiments/inventory.rs`
- Test: `crates/market-squawk-backtesting/src/tests.rs`
- Modify: `apps/market-squawk/src/backtest_service.rs`
- Test: `apps/market-squawk/tests/backtest_vertical.rs`

**Interfaces:**

- Produce a non-serializable, private-field `PinnedInstrumentDefinitions` only from
  `Catalog::pin_instrument_definitions(instrument_ids: &[InstrumentId], as_of: Timestamp,
  limit: CatalogLimit) -> Result<PinnedInstrumentDefinitions, CatalogError>`.
- Each admitted definition binds its verified catalog row digest, `definition_revision`, and
  conservative availability/effective start equal to `observed_at`. For each instrument, the next
  strictly later observed revision closes the prior half-open effective interval. Missing coverage,
  duplicate IDs/times, non-increasing definition revisions, truncation, digest mismatch, or a
  receipt result over catalog count/byte limits fails closed.
- The receipt exposes read-only resolution of `InstrumentExecutionTerms` at a decision cutoff plus
  versioned content/audit identities and the requested as-of bound. It has no public constructor or
  deserializer. Existing `instrument_history` behavior remains available for compatibility.
- Replace `PinnedBacktestInput.execution_terms: Vec<InstrumentExecutionTerms>` with
  `instrument_definitions: PinnedInstrumentDefinitions`. Replace the dataset constructor argument
  likewise. Dataset admission resolves the exact terms for every `(instrument_id, decision_at)`,
  requires exact receipt coverage of used instruments, and binds both receipt identities into a
  versioned dataset identity. Trial V1/V2/V3 persisted schemas remain unchanged; new trials inherit
  the stronger dataset identity.
- Change `latest_attempt` to receive the digest of the actual canonical reservation. Count every
  entry, require a regular file and canonical 20-digit `.json` name, require filename/payload
  attempt equality, require every payload reservation digest to equal the actual reservation, and
  require the complete sequence `1..=latest` without gaps. Use fixed/bounded tracking rather than
  collecting an unbounded directory vector.

- [ ] **Step 1: Add three critical RED behaviors to existing harnesses**

  In `catalog.rs`, add one case with two monotonic definitions and assert resolution returns v1
  before v2's observed time and v2 at/after it, while a pre-history cutoff fails. In backtesting's
  existing `src/tests.rs`, admit two historical rows for one instrument and assert their execution
  terms/identity use the correct catalog revisions. Add one attempt-recovery case that independently
  corrupts a noncanonical name, filename/payload number, reservation digest, and sequence gap and
  asserts each namespace is rejected. Keep these as existing-harness tests, not new executables.

- [ ] **Step 2: Run focused RED**

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-data --test catalog --all-features --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-backtesting --lib --all-features --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk --test backtest_vertical \
    --all-features --locked
  ```

  Expected: failures identify only the absent catalog receipt, historical resolution, and strict
  attempt namespace behavior. Record the failures and target size.

- [ ] **Step 3: Implement the catalog-minted point-in-time receipt**

  Query verified instrument rows with `observed_at_ns`, charge the existing cumulative catalog
  result budget, detect limit truncation with a bounded extra-row check, validate definition and
  monotonic history invariants, derive conservative intervals, and compute separate content/audit
  identities. Do not change the SQLite schema or claim a provider-authored effective time that the
  catalog does not possess.

- [ ] **Step 4: Consume historical terms in dataset admission**

  Resolve after each row's instrument and cutoff are decoded, reject any uncovered row or unused
  receipt instrument, include receipt identities in `BacktestDatasetInput`, and bump only the
  internal dataset-identity domain tag. Update the sole application input seam and compile-contract
  test; do not add another backtest service.

- [ ] **Step 5: Validate the complete attempt namespace**

  Thread the actual reservation digest through every `latest_attempt` call. Validate every
  directory entry and decoded record before selecting the latest attempt. Preserve immutable
  attempt publication, bounded leases, terminal compatibility, and pending-artifact recovery.

- [ ] **Step 6: Run backtest/data GREEN and quality gates**

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-data --test catalog --all-features --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-backtesting --lib --all-features --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk --test backtest_vertical \
    --all-features --locked
  CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-data \
    -p market-squawk-backtesting -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo fmt --all --check
  git diff --check
  ```

  Expected: historical terms vary only at catalog-observed boundaries; experiment recovery rejects
  every noncanonical/unbound namespace; existing historical terminal compatibility remains green.

- [ ] **Step 7: Self-review and commit one cohesive lane**

  Inspect producer/consumer coverage, all `latest_attempt` callers, dataset/trial identity ripple,
  and wire compatibility. Report exact test counts and target size, then commit with
  `fix(backtest): bind historical reference authority`.

### Task 3: Exact integration, rereview, gate, and cleanup

**Files:**

- Modify after acceptance: `docs/plans/delivery-ledger.md`
- Modify after acceptance: `docs/project-memory.md`
- Modify ignored recovery map: `.superpowers/sdd/progress.md`

**Interfaces:**

- Root owns exact-range review, integration, GitHub issue/Project transitions, and cleanup.
- The Quarter 3 checkpoint remains rejected until one frozen exact-candidate rereview accepts the
  remediated Tasks 16–17 slice. Previously accepted slices need only a cross-plane regression spot
  check when untouched.

- [ ] **Step 1: Review each lane before integration**

  Require clean worktree status, exact base/head range, RED/GREEN evidence, affected Clippy, target
  size below 10 GiB, and no out-of-scope diff. Dispatch an independent reviewer for each exact range;
  remediate all substantiated Critical or Important findings before acceptance.

- [ ] **Step 2: Integrate and verify exact patch equivalence**

  Integrate Task 1 before Task 2, prove 1:1 range equivalence, run the focused portfolio then
  data/backtest/application gates on the release tree, push, and record exact heads on PR `#26` and
  issues `#21`/`#22`.

- [ ] **Step 3: Clean completed lane state immediately**

  After push and handoff, confirm no Cargo process uses a lane; remove its generated target and clean
  worktree; delete the merged/patch-equivalent local and origin feature branch; prune worktree and
  remote metadata. Preserve the three protected stashes, `bundle-backup`, and Dependabot refs.

- [ ] **Step 4: Run the grouped Quarter 3 rereview**

  Freeze one exact clean release commit. Review Tasks 16–17 plus the Task 11/12 and execution/fair-
  value boundary ripples. If any substantiated finding remains, keep issues open and return only the
  rejected slice to bounded remediation.

- [ ] **Step 5: Run one clean full gate only after review acceptance**

  Confirm no Cargo process is active, record disk state, clean the root generated target, and run:

  ```bash
  CARGO_INCREMENTAL=0 ./scripts/verify.sh
  ```

  Expected: the script enforces its own 20 GiB ceiling and every required locked format, Clippy,
  test, release, audit, security, product, CLI, and MCP smoke gate exits zero at the unchanged
  reviewed commit.

- [ ] **Step 6: Close the checkpoint truthfully**

  Only after the accepted rereview and full gate, close issues `#20`, `#21`, and `#22`, set their
  Project 5 items to Done, update the tracked ledger/memory and ignored recovery map, commit/push the
  status closeout, and resume Tasks 19, 19A, and 20.
