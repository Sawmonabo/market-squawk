# Contributing

Read and follow [`docs/project-memory.md`](docs/project-memory.md) before planning or implementation.
It defines the repository's binding parallel-ownership, review-checkpoint, exact-head verification,
and progress-reporting rules.

The current controlling delivery artifact is the
[usable complete-release implementation plan](docs/superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md).
Older Q-prefixed plans remain historical audit records and have no current execution authority.

Prepare and run the complete source-built desktop with:

```bash
cargo install just --version 1.57.0 --locked
just setup
just dev
```

`just --list` is the developer command index. Use `just check`, `just test-package <crate>`, and
`just test` for focused work. `just dev-web` starts only Vite and is not product-level evidence.

1. Use the pinned Rust 1.97.1 toolchain, including the repository's `rustfmt` and Clippy components.
2. Add or update a failing test before implementation changes.
3. Keep source-specific schemas inside adapters.
4. Keep MCP, SQL, notebooks, and model training outside the live event-to-decision path.
5. Never silently discard raw data, sequence gaps, checksum failures, or risk rejections.
6. Keep each checkout and worktree on its default local `target/`; do not set `CARGO_TARGET_DIR`,
   `CARGO_BUILD_BUILD_DIR`, `target-dir`, or `build-dir` for repository verification.
7. Normal `dev` and `test` commands are incremental and retain line tables. Use
   `cargo build --profile debugging` for full workspace debug information.
8. Run `./scripts/verify.sh` before submitting an integration candidate. It disables incremental
   compilation for gate evidence and enforces the local 20 GiB target ceiling.
9. Do not add credentialed live execution without a separate design, explicit authorization model,
   reconciliation, and adversarial review.
