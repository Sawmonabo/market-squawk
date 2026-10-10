# Market Squawk Agent Instructions

These instructions apply to the entire repository.

Before planning, implementation, integration, or review work, read
[`docs/project-memory.md`](docs/project-memory.md). It contains binding project operating decisions
about production quality, safe parallelism, planning handoff, quarter checkpoints, exact-head
verification, and progress reporting.

Current scope and acceptance are defined by
[`docs/plans/v1-owner-test-goal.md`](docs/plans/v1-owner-test-goal.md). Current execution status and
assignments live in the delivery ledger. Explicit owner pauses remain controlling.

In particular:

- Tie engineering work to a required workflow, concrete defect or existing substantiated finding;
  do not expand completion through speculative hardening or general cleanup.
- Use the existing feature branch and primary worktree. New branches/worktrees need explicit owner
  approval. The lead alone owns Git, integration and build/test scheduling.
- Do not hold an independently useful plan or research artifact behind an unrelated implementation
  approval. Mark its audit base and refresh gate explicitly.
- Parallelize only along a documented dependency DAG with disjoint file ownership. Serialize shared
  manifests, lockfiles, application composition, and authority-critical hotspots.
- Group fresh independent reviews at the four delivery-quarter checkpoints. A re-review that closes
  findings from an already rejected checkpoint is required remediation, not a new per-task review
  round.
- Do not accept a checkpoint with unresolved substantiated findings in its changed behavior. All
  applicable Critical, Important and Minor findings still block final approval. Coherent ordinary
  checkpoints use relevant critical checks; they do not require a new quarter review or full CI.
- Approval and performance claims require clean, unchanged, exact-head evidence. Focused lane tests
  are not release-gate approval.
- Preserve historical Q-prefixed checkpoint and finding identifiers as audit locators. New work uses
  Stage and Wave for dependency/ownership scheduling and exactly four numbered delivery-quarter
  checkpoints for grouped review.
- On authorized execution, pursue the finite owner-test contract, distinguishing functional
  completion from final verification. Preserve explicit pauses and the no-publication/no-merge
  boundary; scaffolding and isolated tests are not completed workflows.
- Report progress by outcome, frozen commit, active lane, remaining blocker, and next barrier. A
  historical task number alone is not an adequate status report.
- For historical or explicitly approved exceptional worktrees, remove them promptly after
  integration and handoff once clean, then prune their worktree metadata. Never force-remove a dirty or still-active worktree; reconcile or preserve its
  uncommitted state first. Branches and commits may remain until normal branch completion.
