# Rust development and test storage hardening

**Research date:** 2026-07-21

**Audit commit:** `34b8cc17f6fcf66f3b4d7735b3d98a60e43ad076`

**Status:** decision input for the
[Rust development and test storage hardening plan](../superpowers/plans/2026-07-21-rust-dev-test-storage-hardening.md);
not release approval or performance evidence

This report records the primary-source facts behind the development/test storage policy. Facts are
linked to Cargo, GitHub Actions, or the pinned cache action. Statements labeled **Decision** or
**Inference** are Market Squawk policy derived from those facts.

## Cargo profiles and artifact locality

Cargo's [profile reference](https://doc.rust-lang.org/cargo/reference/profiles.html) establishes the
following:

- `dev` is the normal build profile, while `test` inherits `dev` and is selected by `cargo test`.
- `debug = "line-tables-only"` preserves file/line backtrace information without variable or
  parameter debug information. `debug = "full"` is the full-debug setting.
- custom profiles require `inherits`; a custom profile has its own output directory and is selected
  with `--profile`.
- `[profile.<name>.package."*"]` applies to non-workspace dependencies. A named package override has
  higher precedence, so an existing named `argon2` override must state `debug = false` itself.
- profile `incremental` can be overridden globally by `CARGO_INCREMENTAL`.

Cargo's [environment-variable reference](https://doc.rust-lang.org/cargo/reference/environment-variables.html)
defines `CARGO_TARGET_DIR` as an output relocation, `CARGO_BUILD_BUILD_DIR` as the configuration
environment form of `build.build-dir`, and `CARGO_INCREMENTAL=0` as a forced incremental-compilation
disable. The [build-cache reference](https://doc.rust-lang.org/cargo/reference/build-cache.html)
describes `target/` as generated output and places incremental state beneath the active build
directory.

**Decision.** Each worktree owns the default `target/`; neither local verification nor CI redirects
the target or build directory. Normal developer `dev` and `test` profiles use line tables and
incremental compilation, while non-workspace dependencies have debug information disabled. The
opt-in `debugging` profile inherits `dev`, restores full workspace debug information, and disables
incremental compilation. Agents, CI, benchmarks, and approval gates force `CARGO_INCREMENTAL=0`.
No `split-debuginfo`, `codegen-units`, target/build directory, or compiler-wrapper policy is added.

**Decision.** The verification entry point rejects the two specified ambient directory overrides
before work starts and checks its local `target/` against a 20 GiB hard ceiling before and after the
gate. The integrated measurement task must later prove the tighter 15 GiB clean full-gate acceptance
criterion at one unchanged candidate; this report does not claim that result in advance.

## Test target selection and harness consolidation

The [`cargo test` reference](https://doc.rust-lang.org/cargo/commands/cargo-test.html) says an
ordinary test invocation compiles and executes unit, integration, and documentation tests. With no
target selector it also builds examples for compile coverage. `--all-targets` additionally selects
benches, and `--doc` runs only documentation tests; therefore an ordinary workspace test followed by
an explicit `--doc` pass repeats doctests.

Cargo's [target reference](https://doc.rust-lang.org/cargo/reference/cargo-targets.html) says each
integration-test target is a separate executable and explicitly recommends consolidating many
integration tests under one target with modules when compile overhead is significant. The libtest
harness still discovers module tests and can run them in parallel.

**Decision.** The authoritative workspace test is
`cargo test --workspace --all-features --locked`; strict Clippy retains `--all-targets`, so benches
and examples remain compiled under the warning-denied gate without making the test pass execute
benchmark targets. The later harness lanes may consolidate integration-test executables without
deleting behavioral tests, changing ignored status, or folding explicitly isolated UI tests into an
ordinary target.

## CI cache trust and deterministic cross-platform tests

The pinned
[`Swatinem/rust-cache` v2.9.1 source](https://github.com/Swatinem/rust-cache/tree/c19371144df3bb44fab255c43d04cbc2ab54d1c4)
documents `workspaces`, `cache-targets`, `cache-on-failure`, `cache-all-crates`,
`cache-workspace-crates`, and `save-if`. It normally caches dependency artifacts, removes
incremental output before persistence, and supports restore-only operation by making `save-if`
false. The action also sets `CARGO_INCREMENTAL=0`, but the workflow declares that policy itself so
the build contract does not depend on an action side effect.

GitHub's [dependency-cache security guidance](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
recommends a trusted default-branch push to refresh caches and restore-only behavior for low-trust
workflows. The [`github` context reference](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts)
supports expressions over the event name and full ref.

**Decision.** Every cache step explicitly names the local `target/`, enables dependency-target
caching, rejects failure saves, excludes all/workspace-crate retention, and saves only when
`github.event_name == 'push'` and `github.ref == 'refs/heads/main'`. Pull requests restore but cannot
save through this action. macOS and Windows run the same locked ordinary workspace test as Linux
rather than a build followed by an all-target test that recompiles overlapping targets.

## Measurement boundary and deferred tools

Generated Cargo output is reproducible convenience state, not approval evidence. Storage reduction,
warm feedback, peak memory, executable counts, and cache-runner value therefore require measured
fixed-head comparisons rather than assumptions.

**Decision.** The later integration task measures clean and warm behavior on one frozen source head.
It may evaluate local-only sccache 0.16.0 and cargo-nextest 0.9.140 under the thresholds in the plan,
but neither tool is admitted by this policy seed. Ordinary Cargo remains authoritative, no compiler
wrapper is configured, and any version or adoption decision must be refreshed against primary
release documentation at the measurement barrier.

## Refresh and evidence limits

The audit commit is an anchor, not an approved candidate. Before integrated measurement or release
approval, refresh Cargo and action behavior against the approved head, inspect all profile and CI
diffs, and run the unchanged-head gate in a clean worktree-local target. Focused syntax, parse,
metadata, rejection, and formatting checks for this policy seed establish only bounded lane evidence.
