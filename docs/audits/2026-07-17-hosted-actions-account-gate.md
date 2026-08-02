# Hosted Actions account evidence gap

Date: 2026-07-17

## Exact candidate

- Repository: [Sawmonabo/market-squawk](https://github.com/Sawmonabo/market-squawk)
- Branch: `feat/stage-1-foundation`
- Commit: `ab3f7c19000884357c38702edf6b4acc6a80c483`
- Workflow run:
  [GitHub Actions 29564138664](https://github.com/Sawmonabo/market-squawk/actions/runs/29564138664)

## Result

The workflow created its Ubuntu, macOS, and Windows check runs, but GitHub did not assign a runner
or execute any workflow step. This run is not evidence that repository checkout, formatting,
Clippy, tests, release build, authority persistence, or platform behavior failed.

The Actions API reported the following for all three jobs:

```text
runner_id: 0
runner_name: ""
steps: []
status: completed
conclusion: failure
```

Each check-run annotation stated that the job was not started because recent account payments had
failed or the Actions spending limit needed to be increased, and directed the account owner to
Billing & plans.

## Classification

This is an external account-level hosted-evidence blocker. It is not a source-code, workflow
syntax, runner-image, Rust toolchain, test, or cross-platform behavior failure. Repository Actions
are enabled and allow all actions; changing project code cannot resolve the account annotation.

The exact candidate already has separate local evidence:

- complete `scripts/verify.sh` pass on an unchanged clean commit;
- 25 consecutive complete 111-test source-library passes with 16 test threads;
- standalone `rustfmt` passes for every changed included/path test module;
- strict workspace lint, tests, release build, rustdoc, Loom, CLI, offline mock, and MCP smoke
  passes; and
- independent exact-hash review with zero Critical, Important, or Minor findings.

That evidence remains valid and is the mandatory local exact-head evidence. It does not establish
hosted Ubuntu, hosted macOS, or Windows behavior. Conversely, GitHub Actions is an optional
collaboration and portability-evidence surface: Market Squawk's no-mandatory-cloud constraint means
the account condition cannot become a paid-service prerequisite for A4 implementation or local
exact-head approval.

## Hosted-evidence completion condition

The account owner must correct the payment or Actions spending-limit condition in GitHub Billing &
plans. Then rerun workflow run `29564138664` and require all of the following. Later documentation-
only branch commits do not change the run's immutable `headSha`; do not substitute a different
candidate for this A3 hosted-evidence run.

1. the run head remains exact commit `ab3f7c19000884357c38702edf6b4acc6a80c483`;
2. Ubuntu executes the complete verification job successfully;
3. macOS executes its platform authority and integration gates successfully;
4. Windows executes authority replacement, lock, path, and integration gates successfully; and
5. the PR receives a follow-up comment linking the successful exact-hash run.

Do not use repeated reruns before the account condition changes, weaken the workflow, skip a
platform, or reinterpret the no-run annotation as a successful portability result.

A4 may proceed from the locally approved A3 production tree at
`ab3f7c19000884357c38702edf6b4acc6a80c483` after the reviewed Wave 0 documentation
commit is integrated. The unavailable-hosted conclusion in this audit applies only to exact A3 run
`29564138664` at that commit. Until the account condition is corrected and that run is rerun, the A3
run cannot substantiate hosted cross-platform success.

Every later exact candidate classifies only its own exact-SHA workflow runs. A later candidate may
obtain valid hosted evidence for its own commit without rerunning A3; neither the A3 account-gated
result nor a later success may be transferred between commit SHAs.
