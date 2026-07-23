# Market Squawk Delivery Ledger

Last updated: 2026-07-22

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Current integration state

- Release branch: `release/market-squawk-v0.1.0`
- Pre-status Quarter 3 capability-code head: `daf183a`
  (`fix(python): make native module the sealed package root`). This is a durable milestone, not the
  moving release-branch head.
- Current exact integration head: obtain it from `git rev-parse HEAD` and pull request `#26`.
  Tracked prose does not self-pin the commit that contains that prose.
- Quarter 3 status: Tasks 13–18 are integrated, but the frozen grouped review rejected the candidate
  with thirteen Important findings. Fair-value evidence-authority remediation passed independent
  exact-candidate closure review and is integrated through `6c114c7`. Model-runtime and
  backtest/portfolio closure reviews found four and two additional Important defects respectively;
  both focused remediation lanes are active. The checkpoint has not been accepted and the single
  clean full gate has not run on the remediated exact head.
- Task 14 accepted feature and fast-forwarded release head: `02ab5cd`
- Task 18 release merge head: `051ee3c`; reconciled lock head: `5c34b7d`
- Task 13 accepted feature and release head: `59ba05c`
- Task 16 accepted core head: `e124722`; accepted execution-binding and release head: `7621552`
- Task 12 code integration head: `9702556` (`fix(analytics): bind complete batch semantics`)
- Integrated and pushed hardening code head: `2d39b0a34eb818f817973210148355c88f8f4b52`
- Hardening owner: GitHub issue `#30`, Project 5
- Hardening status: implemented, verified, integrated, pushed, and cleaned up
- Documentation-system lane: the production documentation information architecture is approved and
  is a release blocker for the first complete local release. The canonical written design is
  [`2026-07-22-market-squawk-documentation-system-design.md`](../superpowers/specs/2026-07-22-market-squawk-documentation-system-design.md).
  It is awaiting written-spec review before implementation planning and documentation-tree
  migration. The active model-runtime remediation retains ownership of
  `docs/operations/onnx-runtime.md` until it is accepted and integrated.
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
- Task 18 owner: GitHub issue `#23`, Project 5, status `Done`.
- Delivered at accepted feature head `31de1a5`: nonforgeable producer receipts; point-in-time market
  activity and evidence admission; strict Level 1 classification; usable Level 2/Level 3 input
  judgments; non-promotable `Unclassified` evidence; durable dual-approved market access,
  overrides, approvals, revocations, audit chains, catalog CAS, bounded recovery, and global limits.
- Task 18 verification: the complete valuation package, four-case consolidated fair-value harness,
  bounded live-export route, catalog recovery/query regressions, strict affected-package Clippy,
  formatting, and diff hygiene passed on the integrated locked tree. Exact-head review rejected two
  point-in-time/override defects; remediation-only rereview accepted `31de1a5` with no remaining
  Critical or Important finding.
- Quarter 3 follow-up at reviewed candidate `e59dfca` closed stale-quality classification and
  legacy-v1 analytical-evidence recovery defects without changing historical identities. The exact
  two-commit series range-diffed 1:1 onto release commits `6a9a685` and `6c114c7`; the integrated
  valuation gate passed 8 unit and 4 consolidated integration tests before push and cleanup.
- Task 14 owner: GitHub issue `#19`, Project 5, status `Done` after this closeout push.
- Delivered at accepted and fast-forwarded release head `02ab5cd`: catalog-authorized Task 11
  point-in-time dataset access; fixed-width Arrow/Parquet schema-v2 validation; exact
  `decimal.Decimal` accounting inputs; bounded Rust financial kernels; visualization; deterministic
  native linear/logistic training; and finalized model-bundle publication. External authority v4
  independently binds final metadata, artifact, training-run, catalog, export, selection, feature,
  label, universe, split, code, and environment identities. The production API cannot select a
  validator executable; the adjacent Rust validator is size/type bounded and its pre/post hash must
  match the identity compiled into the native wheel.
- Task 14 verification: exact-head rereview accepted `02ab5cd` after the original catalog, memory,
  Decimal, runtime, cancellation, migration, schema, model-authority, and validator findings were
  closed. The single sealed offline release matrix admitted 357 source paths and passed 9/9 product
  contracts on CPython 3.12.12 and 9/9 on 3.13.7 with no retry. Exact evidence: release manifest
  `5403a73fbfe03d715b192e9da19cf9e7cfc8b7aa31f773bdd39586534b44618d`, project wheel
  `f19be320abd91ed73637f6d7edfa8df133ff5149cfaa8804663dadcd4134a25c`, validator
  `2b8576c3e6f219f34d958c863e08cf2b68599306faa2668ba8cf348f705e1b1c`, wheelhouse/source lock
  `92657a32099c7b309e9b73b674ae1ecee26f8c70d71e69e1a72f225a5e510f9a`, and sealed build
  environment `d0a9479dae9eb8024e5a4c6bfb1e5fa606a03e0858530ca3a1622b2580379931`.
- Task 15 owner: GitHub issue `#20`, Project 5, status `In Progress` until the Quarter 3 checkpoint
  closes. The integrated implementation provides the required self-contained tract ONNX backend,
  bounded helper-process/resource/deadline contracts, exact graph and warm-up admission, and
  no-action failure. The optional operator-supplied ONNX Runtime path is Linux-only, descriptor-
  verified, sealed in immutable memory, parity-checked, and cannot replace the required tract
  fallback.
- Task 17 owner: GitHub issue `#22`, Project 5, status `In Progress` until the Quarter 3 checkpoint
  closes. The integrated application-owned PIT backtesting service binds exact dataset partitions,
  executable/model/configuration identities, research execution assumptions, reconciled portfolio
  accounting, immutable success/failure terminals, artifacts, cohorts and overfitting diagnostics.
- The next barrier is remediation, closure review, and integration of the two rejected model and
  backtest candidates, followed by a grouped review of that exact integrated Quarter 3 head and one
  clean `CARGO_INCREMENTAL=0 ./scripts/verify.sh` run. Tasks 15/17 remain open until that evidence
  passes;
  focused lane gates and prior release artifacts do not substitute for the checkpoint.

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
  worktree/remote metadata was pruned.
- Fast-forwarded Task 14 to `02ab5cd`, removed the clean Python feature worktree and its 5.5 GiB
  target plus 1.2 GiB ignored release evidence, deleted merged local branch
  `feature/python-financial-training`, confirmed no matching origin branch existed, and pruned
  worktree/remote metadata. Only the release worktree remains.
- Integrated the reviewed Task 15/Python containment series by an exact 1:1 range-diff at
  `daf183a`; reclaimed its 7.1 GiB target; removed the clean
  `.worktrees/model-runtime-containment` worktree; deleted merged local branch
  `feature/model-runtime-containment`; confirmed no matching origin branch existed; and pruned
  worktree/remote metadata. The three protected stashes, `bundle-backup`, main/release branches and
  Dependabot branches remain. At that closeout boundary, `.worktrees` was empty; the three active
  Quarter 3 remediation worktrees were created afterward.
- Integrated the independently accepted fair-value evidence-authority series through release head
  `6c114c7`, pushed it, removed 5,496 generated files and 3.4 GiB from its target, removed the clean
  `.worktrees/fair-value-evidence-authority` worktree, deleted the patch-equivalent local feature
  branch, confirmed no matching origin branch existed, and pruned worktree/remote metadata. The
  model-runtime and backtest-experiment-integrity worktrees remain active and deliberately
  preserved while their rejected closure findings are remediated.

The next delivery event is closure review and integration of the two active Quarter 3 remediation
lanes, then one grouped exact-head rereview and one full gate. If those pass, issues `#20` and `#22`
can close and Tasks 19, 19A and 20 begin. Integrated Tasks 13–18 do not claim that the Market Squawk
product release is complete.
