# Market Squawk Delivery Ledger

Last updated: 2026-07-21

This is the compact operational handoff record required by
[`project-memory.md`](../project-memory.md). It records integration state; it does not replace
behavioral verification, review evidence, the README capability truth, or the canonical release
plan.

## Current integration state

- Integration branch: `feature/dev-test-hardening`
- Pushed head: `793498bcabafbd1621a3d41f0bf584c1ff3afdc3`
- Review state: Quarter 2 of 4 approved at that unchanged exact head
- GitHub owner: issue `#30`, Project 5, status `In Progress`
- Next release event: clean integrated measurement and conditional tool-pilot candidate for Quarter
  3 of 4
- Open blocker: Task 4 measurements and acceptance gates have not yet been completed

## Completed hardening work

- Removed 96 GiB of root Cargo output and 4 GiB from the preserved research worktree after dry-run,
  ignore, process, and source-state checks.
- Established worktree-local Cargo output, bounded routine debug information, nonincremental
  approval gates, a 20 GiB verifier ceiling, and trusted-main-only CI cache writes.
- Integrated core harness commit `788223b208322dd74227f357245b987f1778cb52` and service harness
  commit `a40a30dcd61045d2b295c7ec36e360f74443032b`.
- Reduced workspace integration-test executables from 115 to exactly 41 while preserving the
  inventoried tests, ignored-state inventory, and Trybuild fixtures.
- Explicitly retained domain, live, and execution UI verification outside the routine Cargo test
  selection.
- Quarter 2 received parallel core, service, and integration/storage-policy reviews with no
  substantiated Critical, Important, or Minor findings.
- Cleaned the completed lane targets (3.7 GiB and 1.5 GiB), removed both lane worktrees, deleted the
  merged local and origin lane branches, and pruned worktree metadata.

## Active worktrees

- Root release worktree: clean at `34b8cc17f6fcf66f3b4d7735b3d98a60e43ad076`.
- Dev/test hardening worktree: clean at the Quarter 2 approved head above before this ledger update.
- Research analytics worktree: active and intentionally dirty at
  `f92c7f13491bc9388d523024a9801db3c9caf0a0`; its unique source state must not be overwritten,
  cleaned, or removed.

## Remaining hardening work

1. Build a clean nonincremental integrated baseline and record hardware, toolchain, elapsed time,
   target size, file/executable counts, and peak memory.
2. Enforce the 15 GiB clean full-gate acceptance limit and compare normal developer clean, warm,
   and incremental feedback against the fixed-head baseline.
3. Evaluate pinned local-only `sccache` and `cargo-nextest` pilots against the plan's measured
   adoption thresholds; reject them if the thresholds are not met.
4. Run the unchanged exact-head full verification and release gates, obtain the remaining grouped
   reviews, integrate the accepted branch into the release branch, update GitHub evidence, and
   perform final cleanup.

Market Squawk's broader usable-release capability work remains governed by the canonical release
plan and README. Completion of this hardening lane is not a claim that the product release is
complete.
