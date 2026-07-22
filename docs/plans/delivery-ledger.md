# Market Squawk Delivery Ledger

Last updated: 2026-07-22

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Current integration state

- Release branch: `release/market-squawk-v0.1.0`
- Current integrated capability code head: `7621552` (`fix(execution): harden portfolio risk authority`)
- Task 13 accepted feature and release head: `59ba05c`
- Task 16 accepted core head: `e124722`; accepted execution-binding and release head: `7621552`
- Task 12 code integration head: `9702556` (`fix(analytics): bind complete batch semantics`)
- Integrated and pushed hardening code head: `2d39b0a34eb818f817973210148355c88f8f4b52`
- Hardening owner: GitHub issue `#30`, Project 5
- Hardening status: implemented, verified, integrated, pushed, and cleaned up
- Product release status: still blocked on the mandatory capabilities listed in the README and
  canonical complete-release plan

## Product delivery closeout and next barrier

- Task 13 owner: GitHub issue `#18`, Project 5; closure follows this ledger commit and push.
- Delivered at exact accepted head `59ba05c`: capability-scoped immutable bundles; complete
  dataset, label, universe, feature, artifact, and code-revision validation; atomic bounded model
  generations; allocation-free deterministic native linear/logistic inference; execution-owned
  fail-closed model strategy; and durable typed no-action audit through the versioned paper-bot
  consumer.
- Task 13 verification: modeling 9/9, execution 32/32, paper-bot audit 2/2, strict affected-package
  Clippy, workspace boundaries, formatting, and diff checks passed. Independent review rejected
  three material implementation defects and two audit-consumer/wire ripples; each was fixed, and the
  final exact-head re-review reported no remaining Critical or Important finding.
- Task 16 owner: GitHub issue `#21`, Project 5, status `Done`.
- Delivered through Steps 1–5 at accepted head `e124722`: source-evidenced normalized portfolio
  transactions, immutable revisions, long/short lots, FIFO/specific identification, cash flows,
  exact gains, explicit incomplete-basis measurements, authoritative corporate-action snapshots,
  reconciliation, performance, exposure, attribution, risk, scenarios, and proposal-only
  rebalancing.
- Task 16 Steps 1–5 verification: full domain tests, four portfolio-adapter integrations, the single
  consolidated 8-test portfolio executable, strict affected-package Clippy, boundaries, formatting,
  and diff checks passed. Independent review rejected four financial/evidence defects; exact-head
  re-review at `e124722` confirmed all four closed with no remaining Critical or Important finding.
- Task 16 Step 6 is accepted at exact head `7621552`: execution owns an immutable portfolio read
  capability; risk derives settlement cash, position, gross exposure, marked and peak equity,
  realized/unrealized loss, leverage, and drawdown from the current complete portfolio projection;
  approvals bind the exact revision, snapshot digest, and monotonic publication generation; and the
  dispatcher rechecks that authority before its sole adapter call. Publication rejects rollback,
  sibling races, identity resurrection, and stale or revoked revisions.
- Task 16 Step 6 verification: portfolio 8/8, execution 34/34, application risk-dispatch 6/6,
  strict affected-package Clippy, workspace boundaries, formatting, and diff hygiene passed.
  Independent exact-head re-review confirmed all three earlier authority/concurrency/dispatch
  findings closed with no remaining Critical or Important finding.
- Current implementation barrier: Task 14's Python product is implemented on its feature lane but
  remains outside the release branch while confirmed producer/consumer provenance, bounded-input,
  destructive-path, and independent-admission findings are corrected and re-reviewed.

- Task 12 owner: GitHub issue `#17`, Project 5, status `Done`.
- Exact feature and fast-forwarded release code head: `9702556`.
- Delivered: complete pure-Rust batch returns, risk, factor, fundamental, macro, exposure,
  attribution, and scenario kernels; exact-rate and monetary-basis contracts; cadence-bound return
  series; typed statistical location/dispersion; scale-safe correlation; scaled streaming Givens QR
  factor regression; and a code-owned 43-entry batch registry whose semantic digests bind every
  execution-relevant input and policy.
- Verification: both consolidated analytics test executables passed all 24 focused tests; strict
  all-target/all-feature Clippy passed; three independent Quarter 2 reviewers reported no Critical
  or Important finding; and `CARGO_INCREMENTAL=0 ./scripts/verify.sh` passed the complete workspace,
  release, audit, documentation, offline-product, and MCP-smoke gate at exact head `9702556`.
- The former Task 13 serialization barrier is satisfied at accepted head `59ba05c`.

- Task 11 owner: GitHub issue `#16`, Project 5, status `Done`.
- Exact feature head: `95fbf0e`; exact release integration head: `8f03d87`.
- Delivered: durable provider/local revision assignment before publication; production revision
  plans for FRED/ALFRED, SEC, BLS, and Treasury; source-authored historical-universe evidence;
  conservative corporate-action and point-in-time selection; leakage-bounded feature/label dataset
  construction; authority-bound Arrow/Parquet publication and DataFusion query; and the application
  research service that owns source registration, rights admission, ingest reservation, ingestion,
  dataset construction, and analytical access.
- The checkpoint review approved the exact implementation after canonical-identity compatibility,
  aggregate retained-memory admission, and application-owned ingest-authority blockers were fixed.
- Final gates passed: the complete `market-squawk-data` suite, application control-plane suite,
  strict affected-package all-target/all-feature Clippy, full workspace all-target/all-feature
  compile, formatting, and diff hygiene. Focused verification also passed on the merged release tree.

## Rust development and test hardening delivered

- Removed 96 GiB of generated root Cargo output and 4 GiB from the preserved research worktree
  without deleting source, branches, commits, or uncommitted research changes.
- Retired `target/agent-shared`. Each worktree now owns one default local `target/`; verification
  rejects Cargo target/build-directory overrides and compiler wrappers.
- Routine dev/test profiles retain incremental feedback with line-table debug information and no
  dependency debug information. Agent, CI, benchmark, and approval gates are nonincremental. Full
  debugging is an explicit opt-in profile.
- The verifier enforces a 20 GiB target ceiling before and after its gate. CI cache writes are
  restricted to trusted mainline events.
- Consolidated existing integration tests from 115 separately linked executables to 41 without
  adding behavioral tests or removing the inventoried assertions, ignored network tests, Loom
  models, or Trybuild privacy checks.
- Scoped Rust 1.97's macOS compact-unwind linker diagnostic only at the three affected large test
  crates. The production binary and workspace-wide diagnostics remain unsuppressed; unsafe linker
  workarounds were not introduced.
- Corrected the stdio MCP smoke client's required initialization handshake and published bounded,
  namespaced machine-readable authority contracts from the transport-neutral tool descriptors.

## Verification evidence

The authoritative full gate ran at exact pushed head
`2d39b0a34eb818f817973210148355c88f8f4b52`:

```text
CARGO_INCREMENTAL=0 ./scripts/verify.sh
exit: 0
elapsed: 370.23 seconds
maximum resident set size: 1,675,378,688 bytes
```

The gate passed formatting, workspace-boundary policy, dependency/license/vulnerability checks,
current-tree and Git-history credential scans, strict all-target/all-feature Clippy, the complete
consolidated workspace tests, explicit UI/Trybuild tests, Loom models, locked all-feature release
build, rustdoc, compiler-derived capture-contract checks, offline product smoke, and stdio MCP
smoke. Authorized external-network tests remained explicitly opt-in.

Measurement host and artifacts:

```text
macOS 26.5.1 (25F80), Apple M1 Pro, 16 GiB RAM
rustc 1.97.1, LLVM 22.1.6, aarch64-apple-darwin
clean nonincremental pre-fix gate footprint: 8.7 GiB
exact-head checkpoint footprint after focused recompiles and the full gate: 12 GiB
exact-head target files: 29,662
exact-head executable files: 637
release application: 8,804,512 bytes
hard ceiling: 20 GiB
```

The 12 GiB checkpoint includes retained focused-build variants accumulated after the clean
baseline; it remained bounded below the enforced ceiling. It is generated compiler state, not
application size.

## Cleanup state

- Removed the completed hardening target: 29,662 files and 11.9 GiB.
- Removed `.worktrees/dev-test-hardening` and pruned worktree metadata.
- Deleted merged local and origin branch `feature/dev-test-hardening`.
- Preserved the root release worktree.
- Integrated derivative commit `16466db3410ccbccecbe47e8b5dedca1f07a2806`; removed its clean
  worktree; deleted local and origin branch `feature/derivatives-lifecycle-selection`; and pruned
  worktree/remote metadata.
- Removed the completed `.worktrees/research-analytics` worktree and its 18 GiB generated target;
  deleted merged local and origin branch `feature/research-analytics`; and pruned worktree and remote
  metadata.
- Removed the completed `.worktrees/analytics-feature-vertical` worktree after reclaiming its
  9.8 GiB generated target; deleted merged local and origin branch
  `feature/analytics-feature-vertical`; and pruned worktree and remote metadata. Issue `#17` is
  closed and its Project 5 item is `Done`.
- Removed the accepted model-bundle worktree after reclaiming 4.1 GiB and the accepted portfolio
  core worktree after reclaiming 2.8 GiB; deleted both merged local and origin product branches and
  pruned worktree/remote metadata. At that closeout, only the release worktree remained and issue
  `#18` was closed with its Project 5 item `Done`. Issue `#21` subsequently completed at `7621552`,
  was closed, and its Project 5 item was set to `Done`. Its generated target was cleaned, its clean
  worktree and merged local feature branch were removed, no matching origin branch remained, and
  worktree/remote metadata was pruned. The Python feature worktree is the only active feature lane.

The next delivery event is acceptance of Task 14's corrected Python financial analytics/training
product, followed by Task 15's bounded local ONNX inference. Completed Tasks 13 and 16 do not claim
that the Market Squawk product release is complete.
