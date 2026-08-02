# Historical worktree cleanup disposition

- Date: 2026-07-16
- Root branch: `feat/stage-1-foundation`
- Audited root commit: `52b20c39ebf91b2f1e12c5a47aa23f3c76380572`
- Audit mode: read-only comparison of commits, blobs, reports, tests, processes, and open files
- Cleanup policy: preserve evidence, reconcile only proven-redundant dirt, remove without force,
  retain branches

This record formally supersedes the historical lane-local artifacts below. It exists so temporary
worktrees do not become an accidental archive and so their unique untracked files are not discarded
without an evidence-backed disposition. Historical lane reports are not current exact-head approval
evidence. The tracked implementation, research, tests, and successor reports named here remain the
authoritative record.

## Dispositions

### Q1 domain hardening

- Worktree: `.worktrees/q1-fix-domain-hardening`
- Branch: `fix/q1-domain-hardening`
- Lane head: `b3cf5f2ee69e466422c6504d4f9897f8779a520b`
- Untracked artifact: `.superpowers/sdd/q1-fix-domain-hardening-report.md`
- Artifact blob: `11c3c2743966ad6bf8e5ba90a29d04d39bd8617f`
- Root counterpart: `ac50acdaf737f280b31a716adae10890fcc17a5d`
- Equivalence: `git range-diff b3cf5f2^! ac50acd^!` reports an exact patch match.

The implementation and its tests are integrated. The FIX maturity and leg-evidence sources remain
in `docs/research/2026-07-16-q1-contract-decisions.md`. The resulting identity contracts remain in
the domain tests and the tracked Q1 evidence, final-contract, correction, and approval reports.
Only the old RED/GREEN narration and worktree-local handoff prose are unique. The untracked report
is superseded and must not be presented as current exact-head evidence.

### Q1 live contracts

- Worktree: `.worktrees/q1-fix-live-contracts`
- Branch: `fix/q1-live-contracts`
- Lane head: `800765e645d15ec115fc236767337bd176a7212b`
- Untracked artifact: `.superpowers/sdd/q1-fix-live-contracts-report.md`
- Artifact blob: `24a5a926d3e1dbbc183b32e541efb09c79870372`
- Root counterpart: `af0d1ac964dda4927fa27060dc22007f28ee3f61`

The range comparison differs only because the root integration retained concurrent hardening: the
`Denomination` export and stricter `PayloadReference` hashing/unknown-field behavior. No branch-only
production behavior is missing. The report's `QualificationEvidence` and `LiveVerificationState`
terminology is obsolete; current root uses the subsequently hardened `QualificationAssessment`
contracts. The tracked live-trust, provenance, schema, durability, coverage, and timing tests plus
the later final Q1 reports supersede this artifact.

### Q1 plan contract

- Worktree: `.worktrees/q1-fix-plan-contract`
- Branch: `fix/q1-plan-contract`
- Lane head: `67fd1c8e5b5515600092fd28798e44ebbe6afbbd`
- Untracked artifact: `.superpowers/sdd/q1-fix-plan-contract-report.md`
- Artifact blob: `be51b284584dba4cf9dc602cc68d416c06e6f751`
- Root counterpart: `84af63be583d2258d494cd67a6dadc8259d3a60f`
- Equivalence: `git range-diff 67fd1c8^! 84af63b^!` reports an exact patch match.

The live execution authority chain remains tracked in the Stage 1 plan, target architecture, and
implementation plan. The supporting FIX research remains tracked in the Q1 contract-decisions
report. The untracked lane summary adds no current contract and is superseded.

### Q1 platform operations

- Worktree: `.worktrees/q1-fix-platform-ops`
- Branch: `fix/q1-platform-ops`
- Lane head: `3edb84ead259820af10c1e37cd0b42b7cb3e96c2`
- Untracked artifact: `.superpowers/sdd/q1-fix-platform-ops-report.md`
- Artifact blob: `a71b53ca873ccb5e751b08c4ed6572d937d1d62f`
- Root counterpart: `16300f91465c8acd068d6feabee8e94896e464c0`
- Equivalence: `git range-diff 3edb84e^! 16300f9^!` reports an exact patch match.

Journal selection behavior and tests are integrated. Official GitHub Action release and immutable
commit research remains in `docs/research/2026-07-16-ci-action-pins.md`. Later tracked gate and
approval reports supersede the artifact's historical gate claim.

### Q2 Task 5 sources

- Worktree: `.worktrees/q2-task5-sources`
- Branch: `feat/q2-task5-sources`
- Lane head: `17e6a71953db0c867b18b51e37299643d9802574`
- Untracked artifact: `.superpowers/sdd/task-5-brief.md`
- Artifact blob: `c584ed992230e9e9bcd1a0f6df28270271b4ea28`
- Root counterpart: `8edfe4125f1458d91fc8b916e56c0669f19aba2e`
- Equivalence: `git cherry -v HEAD feat/q2-task5-sources` marks the source commit integrated.

The private brief is intentionally not integrated verbatim. Its useful controller addendum is
retained by current source/capture contracts, the tracked Task 5, Task 6, and capture-bridge reports,
and `docs/plans/q2-live-readiness-audit.md`. Its statement that a decoded provider batch already
contains canonical `MarketEvent` values is obsolete and rejected: current root correctly keeps
bounded provider-normalized observations in `DecodedProviderBatch` and constructs canonical
executable events only after state and integrity validation. The generated lane-local `Cargo.lock`
is not an archive and may be reconciled to the lane head.

### Q2 Task 6 platform

- Worktree: `.worktrees/q2-task6-platform`
- Branch: `feat/q2-task6-platform`
- Lane head: `9c68345769c6377f588dbd2e0c67637e8e6a149f`
- Root counterpart: `e3d1bd85031cb17eb491cce412c09f0421b99a81`
- Tracked report blob in both lane and root: `0bc58b45485d76be11ef8a07524adee2d0e2dfd6`

The only implementation range difference is workspace-manifest integration context. Production
code, tests, and the exact Task 6 report are tracked in root. The only uncommitted lane state is a
stale generated lockfile. The lane is formally superseded.

### Q2 Task 8 routing tests

- Worktree: `.worktrees/q2-task8-routing-tests`
- Branch: `test/q2-task8-routing`
- Lane head: `f35aceec7e53e3c554372b0c855eb7d6dbd8b32e`
- Apparently non-equivalent commit: `c6bc06345e3cc5e525f3c4b9297f5b245801b02f`
- Root counterpart: `069ad583f0b15b498710e6ea064c29986e9d6b1f`
- Equivalence: `git range-diff c6bc063^! 069ad58^!` reports an exact patch match.

The current root routing test is a strict superset of the lane's nine routing-vector tests. The
dirty `runtime/memory.rs` blob, `106727a6f77aff1080f66cffd0a3b1a037c6956e`, is reachable from root
history at `c7404acd49ea68f36059cddbd00dd952966fcab0` and
`c60045123a379db3ac8f28e788cdb6217fadb0a7`. The remaining dirty lockfile is generated state. The
lane is formally superseded.

### Q2 capture bridge

- Worktree: `.worktrees/q2-capture-bridge`
- Branch: `feat/q2-capture-bridge`
- Lane head: `d8c7487cb1c09b5b26f6b6164a9933a41f0144f1`
- Tracked report blob in both lane and root: `e3999a865fbc5aa80111cd561dec15bf2c95425e`

Every lane commit has a retained root counterpart:

| Lane commit | Root counterpart | Disposition evidence |
| --- | --- | --- |
| `9f94d4ba3a05` | `e3d1bd85031c` | Same Task 6 code; manifest integration differs |
| `4bade9514ea9` | `8edfe4125f14` | Same Task 5 code; manifest integration differs |
| `c54dbdc0c68b` | `ad64388da622` | Same bridge code; root retains stronger integrated tests |
| `87a2d95aa725` | `921f56fa723e` | Same compact-policy correction |
| `0d8de196f56e` | `d75176a596dd` | Exact range-diff equivalence |
| `d8c7487cb1c` | `349aa084f365` | Exact range-diff equivalence |

At disposition time, the collaboration tree had no capture-bridge agent; process, cwd, and open-file
scans found no owner under the capture-bridge worktree. Root explicitly accepts cleanup ownership
based on that negative ownership evidence, the exact tracked report blob, and the complete commit
mapping above. This authorization extends only to reconciling its generated `Cargo.lock` and
performing an ordinary worktree removal after a fresh ownership and clean-status check. It does not
authorize branch deletion or forced removal.

## Removal invariants

Before removal, each lane head must still match this record. Only the exact audited untracked
artifacts and generated/redundant tracked paths named above may be reconciled. Every worktree must
have an empty `git status --short --untracked-files=all`, no registered agent, and no process cwd or
open file beneath it. Worktrees are removed with ordinary `git worktree remove`, never `--force`.
Branches and commits remain available after directory cleanup.
