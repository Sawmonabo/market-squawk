# Market Squawk Agent Instructions

These instructions apply to the entire repository.

Before planning, implementation, integration, or review work, read
[`docs/project-memory.md`](docs/project-memory.md). It contains binding project operating decisions
about production quality, safe parallelism, planning handoff, review checkpoints, exact-head
verification, and progress reporting.

In particular:

- Do not hold an independently useful plan or research artifact behind an unrelated implementation
  approval. Mark its audit base and refresh gate explicitly.
- Parallelize only along a documented dependency DAG with disjoint file ownership. Serialize shared
  manifests, lockfiles, application composition, and authority-critical hotspots.
- Group fresh independent reviews at quarter checkpoints. A re-review that closes findings from an
  already rejected checkpoint is required remediation, not a new per-task review round.
- Never integrate or approve a candidate that has unresolved substantiated Critical, Important, or
  Minor findings.
- Approval and performance claims require clean, unchanged, exact-head evidence. Focused lane tests
  are not quarter approval.
- Report progress by outcome, frozen commit, active lane, remaining blocker, and next barrier. A
  historical task number alone is not an adequate status report.
- Keep all provider-access evasion mechanisms permanently excluded.

