# Market Squawk Stage 1 SDD Progress

Branch: `feat/stage-1-foundation`
Plan: `docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md`
Review cadence: quarter checkpoints after Tasks 4, 8, 12, and 16.

Stage 0: complete (commit 66d30fe, planning/research controls committed; baseline build and 24 tests passed)

## Quarter 1: Tasks 1-4

- Task 1: complete (commit 0cc2634; TDD report recorded; grouped review pending at Quarter 1 checkpoint)
- Task 2: complete (commit 7e91057; Rust 1.97 workspace, 30 tests, strict Clippy; grouped review pending)
- Task 3: complete (commits 0ea2fa2 and 6a9a3c4; identifier research and TDD report recorded; grouped review pending)
- Task 4: complete (commit f128edc; 63 domain tests and full workspace gate passed; grouped review pending)
- Quarter 1 review: approved at exact integrated commit
  `08d2ab24df91bc9fab8bee27763c164847bb1082`. Two independent final reviewers reported no
  Critical, Important, or Minor findings after the complete correction series. The exact root passed
  `PROPTEST_CASES=4096` focused domain suites and `PROPTEST_CASES=4096 ./scripts/verify.sh`, including
  29 policy tests, strict locked Clippy, all workspace tests/doctests, release build, warning-denied
  rustdoc, deterministic offline source smoke, and local MCP smoke.

## Quarter 2: Tasks 5-8

- Task 5: implementation complete through source/capture/current-batch integration commits
  `8edfe41`, `921f56f`, `d75176a`, and `3bdca01`; evidence in
  `.superpowers/sdd/q2-task5-sources-report.md` plus the compact-policy and routing-evidence
  follow-ups.
- Task 6: implementation complete through platform/capture commits `e3d1bd8`, `ad64388`, and
  `349aa08`; evidence in `.superpowers/sdd/q2-task6-platform-report.md` and
  `.superpowers/sdd/q2-capture-bridge-report.md`.
- Task 7: implementation complete at `247044e`; evidence in
  `.superpowers/sdd/q2-task7-live-report.md`.
- Task 8: implementation and deterministic test lanes complete at `c1faab0`; production/runtime
  behavior and the exact combined gate are recorded in
  `docs/reports/q2-task8-implementation.md`. Final documentation commit and root integration may
  advance the checkpoint head without changing the tested production behavior.
- Quarter 2 review: pending at the exact integrated Tasks 5-8 head. No approval is implied by lane
  completion or focused green gates.

## Quarter 3: Tasks 9-12

- Task 9: pending
- Task 10: pending
- Task 11: pending
- Task 12: pending
- Quarter 3 review: pending

## Quarter 4: Tasks 13-16

- Task 13: pending
- Task 14: pending
- Task 15: pending
- Task 16: pending
- Quarter 4 review: pending

## Review findings ledger

- Quarter 1 review must inspect the approved Task 2 deviation: `adapters/*` is omitted until Task 11 because Cargo 1.97 rejects unmatched globs and empty crates are forbidden.
- Quarter 1 review must inspect the documented `clippy::multiple_crate_versions` exception against the resolved graph.
- Quarter 1 review must inspect the Task 4 plan/target-state mismatch: the executable plan requires unit `ExecutionEligibility::Ineligible`, while typed derived failure reasons are retained separately on qualification evidence.
- Quarter 1 blockers: exact Decimal division/multiplication can silently round; live timing/freshness evidence is independently forgeable and can qualify old exchange data.
- Quarter 1 important correction groups: live provenance/generation/coverage/unknown availability/schema compatibility; auditable sequence/checksum evidence; identifier completeness/bounded parsing; journal write-current-only; Rust 1.97 deterministic CI and workspace-wide durable verification; current operational docs.
- Quarter 1 correction commits integrated: infrastructure/journal `90925c6`; exact financial arithmetic
  `d674c47` (source branch commit `b626c33`); identity contracts `86bd8ab`; reviewed identity
  dependency graph `eb9111a`; live qualification/provenance `af0d1ac`; venue-scoped symbol changes
  `2bbd18f`; cross-plane symbol-scope integration test `5375f8a`.
- Fresh full gate after integration: `./scripts/verify.sh` passed on 2026-07-16, including strict
  workspace Clippy/tests/release/rustdoc and the offline mock/MCP smokes.
- Final Quarter 1 contract/capacity corrections integrated through `08d2ab2`: exact provider
  evidence, bounded registry wire/reconstruction, transactional retry coalescing, logarithmic
  canonical-prefix retry lookup, pre-allocation growth rejection, source-authority wording, and
  directional supersession of stale audit reports. Approval applies only to exact commit `08d2ab2`.
- Quarter 2 Task 8 focused gate at `c1faab0`: formatting; strict locked all-target/all-feature
  Clippy for `market-squawk-live` and `market-squawk`; all-feature tests for both packages; release
  builds for both packages; and diff-check all exited zero against the regenerated integrated lock.
  The live library has 74 passing unit tests plus integration/property/compile-fail suites, and the
  application has 5 focused diagnostic/runtime-composition tests. No performance claim is made.
- Quarter 2 grouped review must inspect exact-generation invalidation ordering, actor terminal
  invalidation order, reader permits charged per retained shard generation, clean replacement,
  application diagnostic quarantine/deletion trigger, and the absence of prohibited evasion
  surfaces. Review remains pending.
