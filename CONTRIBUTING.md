# Contributing

Read and follow [`docs/project-memory.md`](docs/project-memory.md) before planning or implementation.
It defines the repository's binding parallel-ownership, review-checkpoint, exact-head verification,
and progress-reporting rules.

The current controlling delivery artifact is the
[usable complete-release implementation plan](docs/superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md).
Older Q-prefixed plans remain historical audit records and have no current execution authority.

1. Use the pinned Rust 1.97.1 toolchain, including the repository's `rustfmt` and Clippy components.
2. Add or update a failing test before implementation changes.
3. Keep source-specific schemas inside adapters.
4. Keep MCP, SQL, notebooks, and model training outside the live event-to-decision path.
5. Never silently discard raw data, sequence gaps, checksum failures, or risk rejections.
6. Run `./scripts/verify.sh` before committing.
7. Do not add credentialed live execution without a separate design, explicit authorization model, reconciliation, and adversarial review.
