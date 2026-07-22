# Market Squawk Delivery Control Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement
> this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the approved lean GitHub delivery surface, consume the frozen capture benchmark
window, integrate the current Task 3 and Task 5 candidates safely, and resume parallel product work
on canonical Tasks 2 and 4.

**Architecture:** GitHub issues carry mutable task transitions, one private Project is only a visual
projection, and Git remains the code/evidence authority. Root performs every GitHub mutation and
shared-file integration; subagents work only in disjoint cohesive lanes and return structured
handoffs. This bootstrap plan ends by returning execution to the canonical Tasks 0–20 plan rather
than creating a competing application plan.

**Tech Stack:** Git, GitHub CLI, GitHub Issues and Projects, Rust 1.97.1, Cargo, the existing
capture benchmark harness, and Codex subagents/worktrees.

## Global Constraints

- The approved design is
  `docs/superpowers/specs/2026-07-19-market-squawk-delivery-control-design.md` at exact commit
  `9df7486eb879cfe040d17992336957c3f26dcd07`.
- Product scope, dependencies, and path ownership remain controlled by the canonical complete-
  release plan, `docs/project-memory.md`, and
  `docs/verification/usable-release-path-ownership.json`.
- Keep `Sawmonabo/market-squawk` as the private product repository and PR #1 as the active product
  integration pull request.
- Root is the sole GitHub-state writer and sole integrator of manifests, `Cargo.lock`, migrations
  registry, application composition, checkpoint evidence, and cross-lane conflict resolution.
- Create exactly 21 task issues, one for each canonical Task 0–20. Create no subtask, finding,
  commit, test, quarter-epic, or agent-session issues.
- The Project uses built-in `Status`, one custom `Delivery Cut` field, and only `All Tasks` and
  `Active` saved views. If CLI-supported Project setup is not complete within ten minutes, issues
  become the temporary live surface and product work resumes; no GraphQL helper or automation is
  built.
- Initial setup is limited to 60 minutes, ordinary transitions to two minutes, integration-cut
  updates to ten minutes, and quarter reconciliation to fifteen minutes. Exceeding a limit means
  simplify the projection, not pause product work.
- Do not add tracking scripts, GraphQL helpers, field registries, Actions, tracking/prose tests,
  Project JSON snapshots, generated reports, percentages, ETAs, burn charts, or parallel GitHub
  writers.
- Exact SHAs, gates, blockers, review decisions, and next actions go in concise issue transition
  comments. Project fields are only a visual projection.
- The product root stays unchanged at
  `23bfecc1bfebc32364ffc68584aa18fb5b3c465c` until its already tool-reviewed benchmark measurement
  is attempted and dispositioned.
- Benchmark evidence moves only through `Prepared`, `Tool Reviewed`, `Measured`, and `Accepted`.
  Only `Accepted` releases the live/capture prerequisite.
- Review follows the finite `C0 -> U0 -> C1 -> R1 -> C2 -> R2` protocol. The current Task 3 work is
  already past broad review; perform closure-scoped review only. There is no automatic loop after
  `R2`.
- A blocking finding requires a violated invariant, supported scenario, exact anchor, reproducible
  failure/proof, impact, and smallest closure. Style, prose, test preference, or unsupported
  hypotheticals are Notes.
- Use at most three disjoint current-quarter workers/reviewers plus root. One worktree owns a
  cohesive lane, not a checklist item.
- Preserve dirty worktrees. Remove a worktree only after its work is integrated or handed off, the
  owner is inactive, and `git status --short` is empty.
- Tests remain thin, concise, and behavior-critical. Do not add tests for the tracker or prose.

---

## Fixed issue map

Every issue body links to the exact canonical plan at product commit `23bfecc`. The issue title is
the corresponding canonical task heading; the heading itself is the required closing outcome.

| Task | Delivery Cut | Dependencies | Initial Status |
| ---: | --- | --- | --- |
| 0 | Quarter 1 of 4 | none | In Review |
| 1 | Quarter 1 of 4 | 0 | In Review |
| 2 | Quarter 1 of 4 | 1 plus accepted live/capture prerequisite | Blocked |
| 3 | Quarter 1 of 4 | 1 | In Progress |
| 4 | Quarter 1 of 4 | 3 | Backlog |
| 5 | Quarter 1 of 4 | 1 | In Progress |
| 6 | Quarter 1 of 4 | 2 | Backlog |
| 7 | Quarter 2 of 4 | 4 | Backlog |
| 8 | Quarter 2 of 4 | 4 | Backlog |
| 9 | Quarter 2 of 4 | 4 | Backlog |
| 10 | Quarter 2 of 4 | 4 | Backlog |
| 11 | Quarter 2 of 4 | 7, 8, 9, 10 | Backlog |
| 12 | Quarter 2 of 4 | 11 | Backlog |
| 13 | Quarter 3 of 4 | 11, 12 | Backlog |
| 14 | Quarter 3 of 4 | 13 | Backlog |
| 15 | Quarter 3 of 4 | 13 | Backlog |
| 16 | Quarter 3 of 4 | 10, 11, 12 | Backlog |
| 17 | Quarter 3 of 4 | 13, 16 | Backlog |
| 18 | Quarter 3 of 4 | 11, 12, 16 | Backlog |
| 19 | Quarter 4 of 4 | 2–18 | Backlog |
| 20 | Quarter 4 of 4 | 19 | Backlog |

The 21 exact titles are:

```text
[Task 0] Refresh the audited baseline and current release truth
[Task 1] Freeze the dependency DAG, current research, and path ownership
[Task 2] Complete production live features, Coinbase, risk, and realistic paper execution
[Task 3] Implement the SQLite catalog, durable registries, secrets, and rights admission
[Task 4] Implement Arrow schemas, immutable Parquet publication, and bounded DataFusion
[Task 5] Implement the bounded MCP protocol crate over abstract services
[Task 6] Implement the production Kraken live-to-paper vertical
[Task 7] Implement local file, database-export, OFX/QFX, and Parquet adapters
[Task 8] Implement SEC EDGAR, submissions, filings, XBRL, and Company Facts
[Task 9] Implement FRED/ALFRED, BLS, and US Treasury adapters
[Task 10] Implement portfolio import and raw-record reconciliation
[Task 11] Compose research ingestion and point-in-time datasets
[Task 12] Implement complete Rust batch analytics and feature registry
[Task 13] Implement model registry, complete bundles, and native Rust inference
[Task 14] Implement the Python financial analytics and training product
[Task 15] Implement required local ONNX inference and an optional external-runtime backend
[Task 16] Implement complete portfolio accounting and analytics
[Task 17] Implement PIT research backtesting and experiment governance
[Task 18] Implement ASC 820/IFRS 13 fair-value analysis
[Task 19] Complete shared application services, CLI, and MCP domains
[Task 20] Prove, review, publish, and stop at the usable complete local release
```

### Task 1: Establish the GitHub delivery surface

**Files:**

- Modify externally: GitHub authentication scopes
- Modify externally: repository Projects setting
- Create externally: one private user-owned GitHub Project
- Create externally: one `Delivery Cut` single-select field

**Interfaces:**

- Consumes: approved design commit `9df7486`, GitHub account `Sawmonabo`, private repository
  `Sawmonabo/market-squawk`.
- Produces: one linked private Project or the explicit issues-only fallback; no repository file.

- [ ] **Step 1: Verify the existing repository and authentication without mutation**

Run:

```bash
gh auth status
gh repo view Sawmonabo/market-squawk \
  --json nameWithOwner,url,visibility,hasIssuesEnabled,hasProjectsEnabled
gh issue list --repo Sawmonabo/market-squawk --state all --limit 100 \
  --json number,title,state,url
```

Expected: private repository, issues enabled, and no existing Task 0–20 issues. If task issues now
exist, reconcile them by exact title rather than creating duplicates.

- [ ] **Step 2: Add Project authorization once**

Run:

```bash
gh auth refresh -s project
gh project list --owner Sawmonabo --format json
```

Expected: Project listing succeeds. If authorization cannot complete in ten minutes, record the
issues-only fallback and continue to Task 2 without creating any helper.

- [ ] **Step 3: Enable repository Projects and create the private Project**

Run only if Project authorization succeeded:

```bash
gh repo edit Sawmonabo/market-squawk --enable-projects
gh project create --owner Sawmonabo \
  --title "Market Squawk — Usable Local Release" --format json
```

Capture the returned project number and ID in the current session, not in a repository file. Then:

```bash
PROJECT_NUMBER="$(gh project list --owner Sawmonabo --format json --jq \
  '.projects[] | select(.title == "Market Squawk — Usable Local Release") | .number')"
test -n "$PROJECT_NUMBER"
PROJECT_DESCRIPTION="Visual delivery index for canonical Market Squawk Tasks 0–20; "\
"Git and task issue transitions remain authoritative."
gh project edit "$PROJECT_NUMBER" --owner Sawmonabo --visibility PRIVATE \
  --description "$PROJECT_DESCRIPTION"
gh project link "$PROJECT_NUMBER" --owner Sawmonabo --repo market-squawk
gh project field-create "$PROJECT_NUMBER" --owner Sawmonabo \
  --name "Delivery Cut" --data-type SINGLE_SELECT \
  --single-select-options \
  "Quarter 1 of 4,Quarter 2 of 4,Quarter 3 of 4,Quarter 4 of 4"
```

Expected: one private linked Project and exactly one custom field. Do not create another Project if
a retry is needed.

- [ ] **Step 4: Apply the UI-only projection or activate the fallback**

Use the Project UI only if it is immediately available. Configure built-in `Status` as `Backlog`,
`Ready`, `In Progress`, `Blocked`, `In Review`, `Quarter Approved`, and `Done`. Keep only an
unfiltered table named `All Tasks` and a Status-grouped board named `Active` filtered to exclude
`Quarter Approved` and `Done`.

If the current tool surface cannot perform this within the remaining ten-minute Project budget,
leave the Project private and linked but do not treat it as operational. Continue with issues as the
live surface and product work as required by the approved fallback.

### Task 2: Create the 21 canonical task issues and initialize truth

**Files:**

- Create externally: exactly 21 GitHub issues
- Modify externally: Project items and initial fields when the Project is operational

**Interfaces:**

- Consumes: fixed issue map above and canonical plan commit `23bfecc`.
- Produces: one immutable GitHub issue number for every canonical task and the first root-authored
  transition on Tasks 0–5.

- [ ] **Step 1: Create each issue individually through `gh issue create`**

Append the matching exact fragment below to the canonical plan URL when creating each issue:

```text
task-0-refresh-the-audited-baseline-and-current-release-truth
task-1-freeze-the-dependency-dag-current-research-and-path-ownership
task-2-complete-production-live-features-coinbase-risk-and-realistic-paper-execution
task-3-implement-the-sqlite-catalog-durable-registries-secrets-and-rights-admission
task-4-implement-arrow-schemas-immutable-parquet-publication-and-bounded-datafusion
task-5-implement-the-bounded-mcp-protocol-crate-over-abstract-services
task-6-implement-the-production-kraken-live-to-paper-vertical
task-7-implement-local-file-database-export-ofxqfx-and-parquet-adapters
task-8-implement-sec-edgar-submissions-filings-xbrl-and-company-facts
task-9-implement-fredalfred-bls-and-us-treasury-adapters
task-10-implement-portfolio-import-and-raw-record-reconciliation
task-11-compose-research-ingestion-and-point-in-time-datasets
task-12-implement-complete-rust-batch-analytics-and-feature-registry
task-13-implement-model-registry-complete-bundles-and-native-rust-inference
task-14-implement-the-python-financial-analytics-and-training-product
task-15-implement-required-local-onnx-inference-and-an-optional-external-runtime-backend
task-16-implement-complete-portfolio-accounting-and-analytics
task-17-implement-pit-research-backtesting-and-experiment-governance
task-18-implement-asc-820ifrs-13-fair-value-analysis
task-19-complete-shared-application-services-cli-and-mcp-domains
task-20-prove-review-publish-and-stop-at-the-usable-complete-local-release
```

For each fixed title above, create one issue whose body has exactly this structure, substituting the
literal matching URL, delivery cut, dependency sentence, and closing outcome from the fixed maps:

```markdown
Canonical task: literal exact canonical plan URL with matching fragment
Delivery cut: literal fixed Delivery Cut value
Dependencies: literal Task references from the fixed issue map
Required closing outcome: literal canonical task heading without the `[Task N]` prefix

The linked canonical plan and `docs/verification/usable-release-path-ownership.json` control on
conflict.
```

Use `gh issue create --repo Sawmonabo/market-squawk --title ... --body ...`. Do not generate a shell
script or body files. Record returned issue URLs only in the current execution context.

- [ ] **Step 2: Reconcile the issue set**

Run:

```bash
gh issue list --repo Sawmonabo/market-squawk --state all --limit 100 \
  --json number,title,state,url
```

Expected: exactly 21 task issues, exactly one for each title, all open. Stop and reconcile before
continuing if any task number is missing or duplicated.

- [ ] **Step 3: Add issues to the operational Project**

If the Project passed Task 1, run `gh project item-add` once per issue URL. Use `gh project
item-list`, `gh project field-list`, and `gh project item-edit` to set the fixed Delivery Cut and
initial Status matrix. Do not persist Project/item/field IDs.

Expected: 21 Project items with no draft items and no non-task items. If Project configuration is
not operational, skip this visual projection without blocking issue transitions.

- [ ] **Step 4: Post the initial authoritative transitions**

Post one concise root-authored comment to Tasks 0–5 using the approved transition template. Bind:

- Tasks 0 and 1 to integrated audit anchor `23bfecc`, with quarter approval still pending;
- Task 2 to the live/capture benchmark barrier at tool-reviewed SHA `23bfecc`;
- Task 3 to full local candidate `63c2578fa81af899bc2e2d9e3d9acbaaecdb6a3f`, including its dirty
  root-owned manifest/lock/migration state;
- Task 4 to the Task 3 catalog-interface barrier; and
- Task 5 to clean local candidate `1395e239b2f2a0bc2d8884f1d2f1a3c4e468502e`.

Before each comment, re-read the branch SHA and worktree status. If either changed, record the fresh
fact instead of copying this bootstrap observation.

### Task 3: Consume the exclusive live/capture benchmark window

**Files:**

- Create after successful standard measurement:
  `docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md`
- Create after successful standard measurement:
  `docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json`
- Create after successful paired measurement: `docs/reports/2026-07-17-q2-a4-evidence-lock.json`
- Create after successful paired measurement: `docs/reports/2026-07-17-q2-a4-verification.md`
- Modify after successful paired measurement: `docs/architecture/current-state.md`
- Modify after successful paired measurement: `docs/architecture/target-state.md`
- Modify after successful paired measurement: `docs/plans/gap-analysis.md`
- Modify after successful paired measurement: `docs/plans/implementation-plan.md`
- Modify after successful paired measurement: `docs/project-memory.md`
- Modify after successful paired measurement: `docs/verification/usable-release-baseline.md`
- Modify externally: Task 2 issue transition and one meaningful PR #1 integration/evidence comment

**Interfaces:**

- Consumes: unchanged clean pushed product root
  `23bfecc1bfebc32364ffc68584aa18fb5b3c465c`, its independent `Tool Reviewed` disposition, the
  existing canonical benchmark procedure, and an idle admitted host.
- Produces: truthful host-gate refusal or measured standard/candidate evidence. Only a reviewed
  accepted paired result releases Task 2.

- [ ] **Step 1: Reserve the exclusive host**

Confirm no subagent is active and no Cargo, rustc, Criterion, or capture-evidence process is
running.
Verify root HEAD, upstream, and clean state equal `23bfecc`. Do not run the benchmark while an
implementation worker or competing build is active.

- [ ] **Step 2: Run the exact canonical standard measurement block**

Set:

```bash
export REVIEWED_STANDARD_HEAD=23bfecc1bfebc32364ffc68584aa18fb5b3c465c
```

Then execute, without modification, the complete `Standard measurement and baseline lock` shell
block in the canonical plan. Expected: either a typed host-gate refusal with no performance claim,
or five standard-backend repetitions and a verified `SHA256SUMS` inventory under the head-qualified
ignored evidence directory.

- [ ] **Step 3: Handle the disposition once**

If the host gate refuses admission, preserve only valid ignored evidence, release any owned lock by
the canonical command, and post one Task 2 blocker transition naming the exact refusal and
`critical_since`. Do not revise the harness. A second refusal or 90 minutes triggers the approved
single scheduling/host decision, not more governance work.

If measurement succeeds, use the measured artifacts to write only the two canonical standard
baseline files, validate their closed fields and hashes, commit them as the sole product-root delta,
and push. Do not merge this delivery-control docs branch into the benchmark candidate.

- [ ] **Step 4: Run and adjudicate the paired candidate**

On the report-only clean child, execute the complete canonical `Paired candidate and prerequisite
approval` block without modification. Produce the required evidence/truth commit only from valid
paired artifacts, run its exact locked gate, obtain the required read-only approval of the unchanged
head, push it, and transition Task 2 to `Ready` only if the paired result is `Accepted`.

### Task 4: Reconcile and closure-review the Task 3 candidate

**Files:**

- Review: Task 3 product paths enumerated by the canonical plan and ownership JSON
- Integrator-only modify: `Cargo.toml`, `Cargo.lock`,
  `crates/market-squawk-platform/Cargo.toml`,
  `crates/market-squawk-data/Cargo.toml`, and
  `crates/market-squawk-data/src/migrations.rs`
- Modify externally: Task 3 issue transition

**Interfaces:**

- Consumes: Task 3 candidate `63c2578`, its seven-commit remediation chain, the approved Task 3
  interface contract on root, and the finite-review rubric.
- Produces: one clean pushed Task 3 product candidate with root-owned integration deltas separated
  and one `R1` closure-scoped verdict.

- [ ] **Step 1: Freeze exact candidate inputs**

Re-read Task 3 HEAD, status, merge base, changed-file list, and focused test evidence. Preserve the
existing dirty manifest/lock/migration changes; classify each as required integration input,
generated lock change, or unrelated drift. Do not discard or force-clean anything.

- [ ] **Step 2: Separate the reviewable product slice**

Create one clean integration candidate from `23bfecc` containing only Task 3's owned production and
behavioral-test paths plus root-reviewed manifest, migration-registry, and lock changes. Exclude the
lane's stale plan/research/ownership diffs. Audit every included path against the ownership JSON and
inspect the complete manifest/lock diff before committing.

- [ ] **Step 3: Run the focused locked Task 3 gate**

Run:

```bash
cargo test --manifest-path crates/market-squawk-data/Cargo.toml --all-features --locked
cargo test -p market-squawk-platform --all-features --locked
cargo clippy --manifest-path crates/market-squawk-data/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
cargo clippy -p market-squawk-platform \
  --all-targets --all-features --locked -- -D warnings
git diff --check
```

Expected: all commands pass at one clean exact candidate SHA.

- [ ] **Step 4: Dispatch one Task 3 `R1` closure reviewer**

Provide the exact candidate package, Task 3 contract, admitted prior findings represented by the
remediation chain, remediation diff, and declared catalog/secrets/authority-state blast radius.
Review only original findings, remediation, blast radius, and remediation-introduced material
regressions. Apply the admission rubric. If clean, approve the exact candidate. If not, perform at
most the finite `C2`/`R2` closure defined by the design; a remaining material blocker escalates.

### Task 5: Validate and integrate Task 5 with Task 3

**Files:**

- Integrate: Task 5 owned service/MCP crate product and behavioral-test paths
- Integrator-only modify: `Cargo.toml`, `Cargo.lock`,
  `crates/market-squawk-services/Cargo.toml`, and
  `crates/market-squawk-mcp/Cargo.toml`
- Modify externally: Task 3 and Task 5 issues and one PR #1 integration-cut comment

**Interfaces:**

- Consumes: accepted Task 3 candidate, clean Task 5 candidate
  `1395e239b2f2a0bc2d8884f1d2f1a3c4e468502e`, and root-owned dependency requests.
- Produces: one clean pushed Wave 1A integration head exposing catalog/control and bounded MCP
  foundations to Tasks 2 and 4.

- [ ] **Step 1: Revalidate Task 5 without broad re-review**

Confirm Task 5 is clean, based on `23bfecc`, and contains only the intended services/MCP product and
behavioral-test implementation after stale plan/research/ownership diffs are excluded. Run its
focused locked tests and Clippy. This is lane validation, not another independent task-review round.

- [ ] **Step 2: Integrate Task 3, then Task 5**

Starting from the accepted benchmark/evidence head, apply the exact accepted Task 3 product tree,
review manifest/migration/lock changes once, and commit. Then apply Task 5's exact product tree,
review its manifests and the single lock resolution once, and commit. Do not merge the delivery-
control docs branch into an active benchmark candidate before benchmark disposition.

- [ ] **Step 3: Run the integrated Wave gate**

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
cargo metadata --locked --all-features --format-version 1 > /dev/null
python3 scripts/check_workspace_boundaries.py
git diff --check
```

Expected: all locked integrated gates pass at one clean exact pushed SHA. Dependency, vulnerability,
license, and credential gates remain required at the Quarter 1 freeze; they are not duplicated for
every lane commit.

- [ ] **Step 4: Publish one dependency-release transition**

Update Task 3 and Task 5 with the integrated SHA, locked evidence, exact review disposition, and
worktree cleanup decision. Post one concise PR #1 comment for the Wave integration cut. Do not post
per-commit or per-test comments.

### Task 6: Clean completed worktrees and merge delivery-control documentation

**Files:**

- Integrate after benchmark disposition:
  `docs/superpowers/specs/2026-07-19-market-squawk-delivery-control-design.md`
- Integrate after benchmark disposition:
  `docs/superpowers/plans/2026-07-19-market-squawk-delivery-control-bootstrap.md`

**Interfaces:**

- Consumes: pushed accepted Task 3/5 integration, clean inactive lane worktrees, and docs branch
  `docs/delivery-control-design`.
- Produces: clean worktree inventory and durable reviewed operating documentation without
  invalidating an unmeasured freeze.

- [ ] **Step 1: Verify cleanup eligibility**

For each completed lane, verify its commits or exact product tree are integrated/pushed, its handoff
evidence is recorded, no agent owns it, and `git status --short` is empty. Dirty state blocks
removal.

- [ ] **Step 2: Remove eligible worktrees normally**

Run `git worktree remove .worktrees/wave1a-data-control` and
`git worktree remove .worktrees/wave1a-services-mcp` only when each named worktree is eligible. Then
run `git worktree prune` and inspect `git worktree list --porcelain`. Preserve branches until normal
branch completion.

- [ ] **Step 3: Integrate the approved docs branch**

After benchmark disposition and product integration, integrate the two reviewed documentation files
without bringing unrelated changes. Run `git diff --check`, commit, push, and keep Project/issues as
the live state rather than editing the docs for each transition.

### Task 7: Resume the Market Squawk product build

**Files:**

- Task 2: exact paths governed by
  `docs/verification/stage-live-execution-path-ownership.json` after its required refresh
- Task 4: exact `market-squawk-data` Arrow/Parquet/DataFusion paths in the canonical plan
- Task 6: no edits until Task 2 freezes the live-source interface

**Interfaces:**

- Consumes: accepted benchmark prerequisite, integrated Task 3/5 foundations, canonical Task 2 and
  Task 4 contracts, and available disjoint agent slots.
- Produces: concurrent Task 2 live/risk/paper and Task 4 research-storage implementation lanes;
  Task 6 Kraken becomes Ready only after Task 2 freezes the interface it consumes.

- [ ] **Step 1: Dispatch Task 2 and Task 4 concurrently**

Use one cohesive worktree per lane and fresh subagents with the exact canonical task briefs. Task 2
owns live/features/Coinbase/risk/paper paths; Task 4 owns Arrow/Parquet/DataFusion paths. Root
retains all shared manifests, `Cargo.lock`, composition, and integration.

- [ ] **Step 2: Enforce lane handoffs**

Each worker returns task, exact candidate SHA, owned paths, focused gate results, material blockers,
review state, and recommended transition. Root verifies every handoff before one issue transition.

- [ ] **Step 3: Admit Kraken only at its real barrier**

Do not start Task 6 merely to fill a slot. When Task 2 freezes the production live-source interface
and records the exact consumer contract, transition Task 6 to `Ready` and dispatch its cohesive
Kraken live-to-paper lane.

- [ ] **Step 4: Continue the canonical plan without another planning stop**

After Tasks 2 and 4 are active, continue the dependency-safe Wave and quarter sequence in the
canonical Tasks 0–20 plan. Stop only for a genuine user-authority blocker, finite-review
`BLOCKED ESCALATION`, or the usable complete local release at Task 20.
