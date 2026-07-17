# Contributing

Read and follow [`docs/project-memory.md`](docs/project-memory.md) before planning or implementation.
It defines the repository's binding parallel-ownership, review-checkpoint, exact-head verification,
and progress-reporting rules.

Current controlling artifacts are the
[Q2 integrated remediation plan](docs/superpowers/plans/2026-07-16-q2-integrated-checkpoint-remediation.md)
and the provisional, approved-base-gated
[Q3 production plan](docs/superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md).

1. Use the pinned Rust 1.97.1 toolchain, including the repository's `rustfmt` and Clippy components.
2. Add or update a failing test before implementation changes.
3. Keep source-specific schemas inside adapters.
4. Keep MCP, SQL, notebooks, and model training outside the live event-to-decision path.
5. Never silently discard raw data, sequence gaps, checksum failures, or risk rejections.
6. Run `./scripts/verify.sh` before committing.
7. Do not add credentialed live execution without a separate design, explicit authorization model, reconciliation, and adversarial review.
