# Q2 A3 I01 Live Fixture Migration

## Outcome

Commit `84c326f00d21502359638a2c33b24a48ccb146d1` replaces the removed generic
`AuthoritativeSourceRegistry::try_new()` call in four deterministic live test fixtures with the
explicit `AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()` constructor. The
fixture harness constructors retain their existing names and signatures. No compatibility alias,
production live code, source-authority code, manifest, or lockfile is included in the commit.

The RED evidence is the independent full-workspace compiler failure against rejected candidate
`9d9ce8e` after removal of the generic registry constructor.

## Verification

- `cargo fmt --all --check` — exit 1. After correcting all four owned files, the rerun reports only
  an unrelated rustfmt diff at `crates/market-squawk-sources/src/registry.rs:22`, which remains owned
  by the authority-remediation lane.
- `cargo check --workspace --all-targets --all-features --locked` — exit 101 before completing the
  workspace. Concurrent source-authority edits fail in
  `crates/market-squawk-sources/src/policy/budget/coordinator/tests.rs:20,24` because
  `AuthorityStateStoreError` is not in scope and in
  `crates/market-squawk-sources/tests/authority_persistence.rs:15` because the test still imports
  removed public `AuthorityStateStore` and `AuthorityStateStoreError` exports. No live-crate error
  was emitted.
- `git diff --check` — exit 0.
- `git diff --cached --check` — exit 0 immediately before commit.

## Remaining concern

The shared worktree is intentionally dirty with source-authority remediation outside this lane.
The integration owner must resolve those source-owned compile and formatting failures, then rerun
the clean exact-head quarter gate; this focused commit alone is not checkpoint approval evidence.
