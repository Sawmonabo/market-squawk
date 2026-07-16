# Contributing

1. Use the pinned Rust 1.97.0 toolchain, including the repository's `rustfmt` and Clippy components.
2. Add or update a failing test before implementation changes.
3. Keep source-specific schemas inside adapters.
4. Keep MCP, SQL, notebooks, and model training outside the live event-to-decision path.
5. Never silently discard raw data, sequence gaps, checksum failures, or risk rejections.
6. Run `./scripts/verify.sh` before committing.
7. Do not add credentialed live execution without a separate design, explicit authorization model, reconciliation, and adversarial review.
