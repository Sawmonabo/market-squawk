# Market Squawk Agent Instructions

These instructions apply to the entire repository.

Before planning, implementation, integration, or review work, read
[`docs/project-memory.md`](docs/project-memory.md). It contains binding project operating decisions
about production quality, safe parallelism, planning handoff, quarter checkpoints, exact-head
verification, and progress reporting.

In particular:

- Do not hold an independently useful plan or research artifact behind an unrelated implementation
  approval. Mark its audit base and refresh gate explicitly.
- Parallelize only along a documented dependency DAG with disjoint file ownership. Serialize shared
  manifests, lockfiles, application composition, and authority-critical hotspots.
- Group fresh independent reviews at the four delivery-quarter checkpoints. A re-review that closes
  findings from an already rejected checkpoint is required remediation, not a new per-task review
  round.
- Never integrate or approve a candidate that has unresolved substantiated Critical, Important, or
  Minor findings.
- Approval and performance claims require clean, unchanged, exact-head evidence. Focused lane tests
  are not release-gate approval.
- Preserve historical Q-prefixed checkpoint and finding identifiers as audit locators. New work uses
  Stage and Wave for dependency/ownership scheduling and exactly four numbered delivery-quarter
  checkpoints for grouped review.
- Continue through the usable complete-release terminal condition in project memory; a progress
  percentage, contracts, scaffolding, diagnostic paths, or a numbered Stage cannot authorize a
  halfway stop.
- Report progress by outcome, frozen commit, active lane, remaining blocker, and next barrier. A
  historical task number alone is not an adequate status report.
- Remove clean lane worktrees promptly after integration and handoff, then prune their worktree
  metadata. Never force-remove a dirty or still-active worktree; reconcile or preserve its
  uncommitted state first. Branches and commits may remain until normal branch completion.
