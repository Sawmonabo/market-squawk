# Market Squawk Delivery Ledger

Last updated: 2026-07-22

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Current integration state

- Release branch: `release/market-squawk-v0.1.0`
- Task 12 code integration head: `9702556` (`fix(analytics): bind complete batch semantics`)
- Integrated and pushed hardening code head: `2d39b0a34eb818f817973210148355c88f8f4b52`
- Hardening owner: GitHub issue `#30`, Project 5
- Hardening status: implemented, verified, integrated, pushed, and cleaned up
- Product release status: still blocked on the mandatory capabilities listed in the README and
  canonical complete-release plan

## Product delivery closeout and next barrier

- Task 12 owner: GitHub issue `#17`, Project 5; closure follows this ledger commit and push.
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
- Current serialization barrier: Task 13 must consume these immutable feature contracts and Task 11
  dataset identities through complete model bundles and a bounded native Rust inference backend.

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

The next delivery event is Task 13's complete model registry, bundle, and native Rust inference
vertical. Task 12 completion is not a claim that the Market Squawk product release is complete.
