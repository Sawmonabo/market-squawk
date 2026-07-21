# Market Squawk Delivery Ledger

Last updated: 2026-07-21

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Current integration state

- Release branch: `release/market-squawk-v0.1.0`
- Exact pushed release head: `cff28fde945fab33eddaab5615320e785435ae2d`
- Integrated and pushed hardening code head: `2d39b0a34eb818f817973210148355c88f8f4b52`
- Hardening owner: GitHub issue `#30`, Project 5
- Hardening status: implemented, verified, integrated, pushed, and cleaned up
- Product release status: still blocked on the mandatory capabilities listed in the README and
  canonical complete-release plan

## Active product delivery

- Task 11 owner: GitHub issue `#16`, Project 5, status `In Progress`.
- Exact pushed integration head: `cd53368b97f366cc3db17e72fbd0d6cc88cbc296` on
  `feature/research-analytics`.
- Delivered on that head: durable provider/local revision assignment before publication; lineage-
  preserving revision rebinding; production revision plans for FRED/ALFRED, SEC, BLS and Treasury;
  conservative historical universe, corporate-action and point-in-time selection; explicit option,
  futures and roll lifecycle selection with snapshot-bound venue-calendar evidence.
- Focused gates passed on the integrated head: the source/durable-revision tests, publication
  recovery/replay, FRED, SEC, BLS and Treasury suites, derivative-universe tests, formatting, and
  strict affected-package all-target/all-feature Clippy.
- Active isolated lane: `feature/point-in-time-dataset-builder` in
  `.worktrees/point-in-time-dataset-builder`. It is not yet integrated or eligible for cleanup.
- Current serialization barrier: integrate and verify the dataset builder, then compose the
  application research service and producer-to-query vertical. Issue `#16` remains open until that
  runnable vertical is complete.

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
- Preserved the root release worktree and clean active `.worktrees/research-analytics` integration
  worktree.
- Integrated derivative commit `16466db3410ccbccecbe47e8b5dedca1f07a2806`; removed its clean
  worktree; deleted local and origin branch `feature/derivatives-lifecycle-selection`; and pruned
  worktree/remote metadata.
- Preserved the active dataset-builder worktree and branch because their commits are not yet
  integrated.

The next delivery event is Task 11's integrated dataset-builder and application research vertical.
Neither the hardening lane nor the current research checkpoint is a claim that the Market Squawk
product release is complete.
