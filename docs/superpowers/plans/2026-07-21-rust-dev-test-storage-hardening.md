# Rust Development and Test Storage Hardening

> Execution branch: `feature/dev-test-hardening`
>
> Audit base: `34b8cc17f6fcf66f3b4d7735b3d98a60e43ad076`

## Global constraints

- Preserve the dirty `.worktrees/research-analytics` worktree and all unique source state.
- Delete only ignored, untracked Cargo output after a dry run and process audit.
- Give every worktree exactly one default worktree-local `target/`; never share a mutable target or
  external build directory across worktree paths.
- Keep normal developer builds incremental. Set `CARGO_INCREMENTAL=0` for agents, CI, and approval
  gates.
- Keep platform-default split debuginfo and leave codegen-unit selection to Cargo.
- Preserve every behavioral test. Do not add wording, documentation-shape, or file-existence tests.
- Use one cohesive branch/worktree per grouped lane, push accepted commits, and delete merged lane
  branches and worktrees after integration.
- Run broad workspace gates only at the integrated checkpoint.

## Task 1: Establish the build and verification policy seed

Implement the following on `feature/dev-test-hardening`:

1. Configure `dev` and `test` for `debug = "line-tables-only"` and `incremental = true`.
2. Disable debug information for non-workspace dependencies, including the existing named `argon2`
   overrides.
3. Add a `debugging` profile inheriting `dev`, with full workspace debug information and
   incremental compilation disabled.
4. Keep the release profile unchanged. Do not set `split-debuginfo`, `codegen-units`, target-dir,
   build-dir, or a compiler wrapper.
5. In the existing verification entry point, reject nonempty `CARGO_TARGET_DIR` and
   `CARGO_BUILD_BUILD_DIR`, export `CARGO_INCREMENTAL=0`, enforce a 20 GiB pre/post target ceiling,
   remove `--all-targets` from the workspace test, and avoid a redundant doctest pass when ordinary
   Cargo already ran doctests.
6. Keep strict all-target Clippy, no-default-feature checks, Loom, audits, CLI, offline mock, MCP,
   and release build coverage.
7. In CI, set `CARGO_INCREMENTAL=0`, make Rust-cache policy inputs explicit, allow cache saves only
   on trusted pushes to `main`, and replace redundant macOS/Windows build-plus-all-target-test pairs
   with the deterministic workspace test.
8. Update normative project memory, verification guidance, and contributing guidance. Persist the
   dated source-backed research report. Historical reports remain historical.
9. Add no new standalone cleanup, wording, or documentation-test script.

Verification:

- Bash syntax-check the verification wrapper.
- Parse the workflow and Cargo manifests using existing local tooling.
- Run `cargo metadata --no-deps --locked` with `CARGO_INCREMENTAL=0`.
- Demonstrate the verifier refuses both forbidden directory overrides before any Cargo work.
- Run formatting checks for changed Rust/TOML-adjacent content as applicable.

## Task 2: Consolidate core integration-test harnesses

Own only these packages: domain, live, application, and platform.

- Domain: 31 targets to `domain_contracts`, `domain_values_identity`, and an explicit `ui` target.
- Live: 14 targets to `market_state_features`, `action_runtime`, and an explicit `ui` target.
- Application: 14 targets to `live_pipeline`, `risk_execution`, and `control_plane`.
- Platform: 8 targets to `configuration_security` and `capture_journal`.
- Use `autotests = false` plus explicit `[[test]]` targets and small module roots.
- Keep substantive test source files focused. Change test bodies only where module-relative shared
  support imports must become crate-relative.
- Mark Trybuild UI targets `test = false`; the integrated verifier will invoke them explicitly.
- Capture the pre-change test inventory and target count before edits. Preserve every test and its
  ignored status.

Verification:

- Prove the intended target-count check fails before implementation and passes afterward.
- Run all owned deterministic harnesses, owned UI targets, strict package Clippy, and tests once
  with default scheduling and once with `--test-threads=1` where shared-process risk exists.

## Task 3: Consolidate service integration-test harnesses

Own only these packages: sources, analytics, execution, and SEC adapter.

- Sources: 7 targets to `decode_contracts` and `authority_capture`; update the child-process
  `--exact` selector to its module-qualified name.
- Analytics: 7 targets to `market_features` and `feature_contracts`.
- Execution: 6 targets to `orders_risk` and an explicit `ui` target.
- SEC adapter: 6 targets to `filings` and `xbrl_point_in_time`.
- Use the same explicit-target, module-root, UI isolation, inventory-preservation, and TDD rules as
  Task 2.
- Do not touch the active research worktree's modified data or file-adapter tests.

Verification:

- Prove the intended target-count check fails before implementation and passes afterward.
- Run all owned deterministic harnesses, owned UI targets, strict package Clippy, and scheduling
  checks needed by child-process or process-global tests.

## Task 4: Integrate and measure the hardened system

1. Merge the two reviewed harness lanes into the policy branch and reduce total workspace
   integration targets from 115 to at most 41.
2. Clean the integrated target, run the required nonincremental exact-head gates once, and record
   hardware, toolchain, elapsed time, target size, file count, executable count, and peak memory.
3. Require a clean full-gate target no larger than 15 GiB and a per-worktree hard ceiling of 20 GiB.
4. Compare normal developer clean/warm/incremental feedback against the fixed-head baseline; do not
   accept more than a 10% warm-feedback regression.
5. Evaluate local-only sccache 0.16.0 with a 4 GiB ceiling. Adopt it only for optional routine agent
   package gates if three runs improve second-worktree median time by at least 20% without output
   divergence.
6. Evaluate cargo-nextest 0.9.140. Adopt it only as an optional routine lane runner if three warm
   runs improve total median time by at least 15%, keep peak RSS within 10%, and preserve the test
   inventory. Ordinary Cargo remains authoritative.
7. Persist measurements and rejected outcomes without claiming unmeasured performance.

## Task 5: Final review, release integration, and closeout

1. Run the exact required format, all-target/all-feature Clippy, workspace test, and locked release
   build from an unchanged candidate head, plus the existing specialized security and behavior
   gates.
2. Obtain one broad whole-branch review and, in one grouped fix wave, fix every substantiated
   Critical, Important, and Minor finding or obtain its retraction with specific contrary evidence.
3. Push and merge the accepted branch into `release/market-squawk-v0.1.0`, then update draft PR #26
   and the owning hardening issue with exact evidence.
4. Merge the updated release into `feature/research-analytics` only after that lane reaches a clean
   atomic checkpoint; never overwrite its unique dirty state.
5. Clean completed lane targets, remove clean lane worktrees, prune metadata, delete proven-merged
   local/origin branches, close the GitHub project item, and update the durable progress ledger.
