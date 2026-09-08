# Market Squawk Implementation Plan

**Status: Current canonical entry point**

This stable repository path intentionally names exactly one executable delivery authority:

- [V1 installed-product experience implementation plan](../superpowers/plans/2026-08-01-market-squawk-v1-installed-product-experience.md)

That plan governs the complete installed-product capability contract, dependency-safe Stage/Wave
DAG, one per-user service, guided setup, permanent Desktop workspaces, CLI/MCP projections,
cross-platform owner-test packages, and exact-head verification. Its current barrier is Task 25
followed by Task 26: produce and verify an unpublished owner-test candidate, then hand that exact
candidate to the owner through the existing draft PR and project. It does not authorize public
release creation, a stable curl endpoint, merge to `main`, or release-branch integration.

Historical filenames, Q-prefixed checkpoints, and finding identifiers remain immutable audit
locators. They do not override the August 1 plan.

## Current execution rule

- Exact pushed heads, active worktrees, blockers, next release events, issue state, and cleanup
  disposition are maintained in the [delivery ledger](delivery-ledger.md).
- Work continues to the usable complete local release terminal condition in
  [`project-memory.md`](../project-memory.md).
- A Stage or Wave is an ordering boundary, not a partial-release stopping point.
- Task 26 freezes one clean exact feature commit for complete verification and the existing final
  Quarter 4 grouped review.
- No contracts, schemas, mocks, synthetic sources, diagnostic paths, plans, or focused tests count
  as a completed product vertical.
- Maintained product documentation is updated by the integration owner only from committed source
  and exact package evidence. Mutable head, blocker, issue, evidence, and cleanup state lives only
  in the delivery ledger.
- Public publication and merge decisions require separate user authority after owner testing.

## Historical plan disposition

The following documents are historical evidence and have no current execution authority:

- [2026-07-17 usable complete-release plan](../superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md)
- [2026-07-22 Quarter 3 portfolio/backtest remediation](../superpowers/plans/2026-07-22-quarter-3-portfolio-backtest-authority-remediation.md)
- [2026-07-29 complete installation and public release](../superpowers/plans/2026-07-29-complete-installation-and-public-release.md)
- [2026-07-16 complete remaining work](../superpowers/plans/2026-07-16-market-squawk-complete-remaining-work.md)
- [2026-07-16 Q3 production plan](../superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md)

Their Q-prefixed names and findings are preserved for traceability. Their old quarter terminology,
audit anchors, dependency assumptions, Python scope, and terminal conditions do not override the
canonical plan.
