# Market Squawk Project Memory

Status: Binding project operating decisions  
Established: 2026-07-16  
Applies to: planning, research, implementation, integration, verification, and review

This file preserves project-level decisions that must survive context compaction, agent changes,
and later implementation sessions. It is not a transient progress report. Root
[`AGENTS.md`](../AGENTS.md) requires future agents to read and follow it.

## Product and quality posture

Market Squawk is implemented as hardened local production infrastructure. "Production-ready" is an
evidence standard: invariant-preserving contracts, fail-closed authority, bounded ownership,
checked financial arithmetic, deterministic lifecycle control, durable audit/recovery semantics,
adversarial tests, and verification at the exact reviewed commit.

Do not choose compatibility scaffolding, a cheap shortcut, or an artificial deferral when the
current architectural boundary is the correct place to implement the complete contract. Do not
silently broaden scope either: significant new authority or product decisions still require an
explicit design decision.

The production Rust baseline is 1.97.1, not 1.97.0. On 2026-07-16 the Rust project published
1.97.1 to correct an LLVM miscompilation in 1.97.0; the upstream compiler issue is classified
critical and demonstrates correct optimized Rust lowering to a release-mode crash on x86-64.
No release, benchmark, checkpoint, or approval evidence may be produced with 1.97.0. A later
toolchain change requires current primary-source research, a recorded decision, exact CI/toolchain
pins, inherited workspace MSRV enforcement, and a fresh locked all-feature verification run.

A4 capture-performance approval uses a bounded measurement trust model, not a hostile same-UID or
byte-reproducible build-supply-chain claim. Pre-change standard-channel results and the current
preparer/host bundle are diagnostic until regenerated at one clean exact integrated candidate. Final
standard-versus-ring measurements must use direct Rust 1.97.1, locked dependencies, exact
source/fixture/executable hashes, bounded whole-process supervision, a separate RSS observer,
documented hardware/OS/host state, identical paired conditions, and no false hosted-CI claim. The
unchanged clean-head full gate and grouped independent quarter review confer approval authority;
self-signed baseline literals do not. Same-UID compiler/build-script hostility and independent
reproducible-build attestation are outside this measurement trust model and must not be implied.

## Planning and implementation are separate deliverables

An implementation plan, design, or research report that is independently useful must be delivered
when it is complete. Do not hold it behind an unrelated code-integration or checkpoint-approval
barrier.

When an upstream implementation head is not yet approved, the planning artifact must:

1. name the exact audited base commit;
2. state that the base is an audit anchor rather than approval;
3. begin execution with a mandatory refresh against the approved head;
4. refresh paths, line anchors, interfaces, dependency edges, and baseline evidence before workers
   start; and
5. remain truthful about which capabilities are planned versus implemented.

Preparing a valid future plan and repairing the current implementation may proceed in parallel.
They converge only at the plan's explicit approved-base refresh barrier.

## Maximum-safe parallelism

Parallelism follows dependencies and ownership, not raw agent count.

Before dispatch, publish a wave table containing:

- task dependencies and start barriers;
- the files or crates owned by each lane;
- shared conflict hotspots reserved to the integration owner;
- the ordered merge sequence;
- focused lane gates; and
- the exact-head integration gate.

Run independent, disjoint lanes concurrently up to the available capacity. Do not assign multiple
writers to the same authority-critical module, application composition file, workspace manifest,
or lockfile. Typical serialized hotspots include:

```text
Cargo.toml
Cargo.lock
application service composition
live actor/authority handoff
execution approval/dispatch capability boundaries
cross-lane conflict resolution
checkpoint evidence and approval state
```

A serialized hotspot does not justify idling unrelated work. Documentation, provider research,
fixtures, isolated adapters, pure analytical kernels, tests in disjoint packages, and later-wave
design can proceed when their inputs are stable or explicitly provisional.

Workspace-wide Cargo verification is also a serialized integration resource. The integration
owner runs the one authoritative full-workspace gate; active lanes run their focused package and
dependency-ripple gates until asked to freeze. Do not run duplicate full-workspace tests or builds
from multiple worktrees: they contend for CPU, hide timing defects, and multiply large Rust build
artifacts without adding independent evidence. Before a release build or new worktree, inspect free
space and active Cargo processes. Reclaim only reproducible `target/` output from completed or idle
lanes when capacity is tight; never delete a dirty worktree, research evidence, source, or unique Git
state to make room. Generated build caches are convenience state, not project memory or approval
evidence.

Every checkout and worktree owns its default worktree-local `target/`. Scheduled commands,
verification, and CI must not redirect Cargo output through `CARGO_TARGET_DIR`,
`CARGO_BUILD_BUILD_DIR`, `target-dir`, or `build-dir`, and must not install a compiler wrapper by
default. Sharing mutable Cargo output across distinct source paths is prohibited even when Cargo
writers are serialized.

Normal developer `dev` and `test` builds remain incremental with line-table debug information;
non-workspace dependencies carry no debug information. The opt-in `debugging` profile provides full
workspace debug information with incremental compilation disabled. Agent, CI, benchmark, and
approval commands export `CARGO_INCREMENTAL=0` so their evidence does not depend on incremental
state. The verification entry point enforces a 20 GiB hard ceiling on its local `target/` before and
after the gate. Reclaim only ignored reproducible Cargo output after checking active processes and
preserving every dirty or unique worktree state.

### Worktree lifecycle

An isolated lane worktree is temporary execution infrastructure, not a permanent archive. Remove it
promptly after all of its commits and follow-up artifacts are integrated or otherwise handed off,
its reported evidence is recorded, its agent is no longer using it, and `git status --short` is
clean. Prune the corresponding worktree metadata after removal. The branch and commits may remain
until the normal branch-completion decision; deleting a worktree does not imply deleting its branch.

Never use forced worktree removal merely to make the directory list tidy. A modified or untracked
worktree is unresolved state: inspect it, determine ownership, and either integrate, preserve, or
explicitly escalate it. Do not discard user or prior-agent files, and do not remove a worktree while
an agent or verification command still depends on it.

### Completion hygiene

A lane is not operationally complete until its accepted commits are integrated and pushed, its
closing evidence is recorded on the owning GitHub issue, the issue is closed when its stated outcome
is actually delivered, and its clean worktree is removed. Delete completed local and remote lane
branches after proving their commits are merged, patch-equivalent, or superseded by the accepted
integration. Preserve active branches, open dependency-update branches, and any unique unintegrated
commit until its disposition is explicit.

Update the README only when runnable or release-blocking product truth changes. Update the local
delivery ledger at every integration barrier with the exact pushed heads, active worktree, open
blocker, next release event, issue state, and cleanup disposition. Repository prose is tracking and
handoff evidence; it never substitutes for behavioral verification.

If one lane remains the critical path across two consecutive status updates or one complete
verification/review cycle, the integration owner must perform a scheduling audit:

1. distinguish implementation, remediation, verification, review, and integration work;
2. split any newly independent work into disjoint lanes;
3. stop work that is not required for the user's current deliverable;
4. explain the remaining serialization dependency concretely; and
5. report the next event that releases the barrier.

Do not describe hours of checkpoint remediation, audit, and plan work merely as being "on Task 7"
or another historical task label.

## Four-quarter review policy

Fresh independent specialist reviews are grouped at four delivery-quarter checkpoints, not repeated
after every ordinary task. Lane workers still perform TDD, self-review, focused verification, and
blast-radius inspection before handoff. The canonical plan maps every Stage and Wave into exactly one
of those four checkpoints; it must never invent Q5 or a higher-numbered quarter.

At each quarter checkpoint:

1. integrate the candidate and make the worktree clean;
2. freeze one exact commit;
3. give every reviewer that same commit and a non-mutating review scope;
4. dispatch the maximum number of non-overlapping reviewers in parallel batches;
5. do not remediate or change the candidate between review batches;
6. union and deduplicate all findings before implementation begins;
7. remediate substantiated findings in disjoint red/green lanes where safe;
8. serialize shared-file fixes through the integration owner; and
9. obtain re-review of the new exact head before approval.

A re-review required to close findings from a rejected checkpoint is part of that checkpoint's
remediation. It is not an avoidable per-task review round.

Every substantiated Critical, Important, and Minor finding blocks quarter-checkpoint approval until it is
fixed or retracted with specific contrary evidence. Severity determines order, not whether a known
defect may remain.

Historical Q-prefixed checkpoint and finding identifiers remain immutable audit locators. They do
not authorize additional active quarters. Current delivery uses the explicit labels `Quarter 1 of
4` through `Quarter 4 of 4` so historical and active identifiers cannot be confused.

## Verification must exercise behavior

Do not add tests or scripts that make README sentences, plan headings, task labels, report wording,
template prose, or verification-wrapper command strings into executable product contracts. Do not
maintain a parallel capability ledger whose only purpose is to mirror the README. User-facing safety
language remains important, but stable authority, quality, coverage, bounds, and execution limits
must be represented in typed or structured contracts and tested through behavior.

Run security tools directly. Cargo Deny, RustSec advisory scanning, and Gitleaks results are stronger
evidence than unit tests that merely snapshot their configuration files. Custom automation is
appropriate when it exercises a real protocol, invariant, resource bound, artifact, or end-to-end
vertical; it is not appropriate merely to enforce prose, command ordering, or report choreography.

The test suite must stay thin, concise, and critical. Add the smallest proof that can fail for a
real product defect, prefer extending an existing behavioral suite over creating another test file,
and remove redundant cases once one stronger invariant or end-to-end boundary covers them. Test
counts are never a delivery metric. File-existence checks, duplicate fixtures, broad snapshot churn,
wrapper tests, prose assertions, implementation-detail assertions, and near-identical example
matrices are prohibited unless the file or serialization itself is the security or compatibility
boundary under test. Security-, authority-, accounting-, recovery-, bounded-resource-, parser-, and
producer-to-consumer failures remain critical and must not be weakened merely to make the suite
smaller.

Keep grouped quarter-checkpoint evidence concise. Do not create a tracked report per subtask or
reviewer. One consolidated checkpoint review record per frozen candidate is sufficient; exact
command outputs and bulky transient artifacts remain ignored and are summarized by commit and
digest.

## Candidate, commit, and evidence discipline

There are three distinct evidence levels:

| Evidence | Meaning | May claim approval? |
| --- | --- | --- |
| Focused lane tests | The bounded lane behavior works in its isolated branch. | No |
| Dirty candidate gate | The reviewed intended diff passes before commit. | No |
| Clean exact-head gate | The committed, integrated, unchanged head passes every required local gate. | Only with completed review and no unresolved findings |

Never call a pre-commit run, cached run from another head, or isolated-lane result "exact-head"
evidence. After an approval review, any further commit—even documentation or style—invalidates the
reviewed head and requires the applicable gate and re-review again.

Hosted CI is separate optional evidence. Do not infer hosted success from local checks. Market
Squawk must remain operable and approvable without a mandatory cloud service.

## GitHub publication discipline

The canonical collaboration remote is the private GitHub repository
[`Sawmonabo/market-squawk`](https://github.com/Sawmonabo/market-squawk). The local repository keeps
the imported bundle as `bundle-backup`; `origin` is the GitHub SSH remote.

After a scoped integration change is locally verified and intentionally committed:

1. push the exact integration branch commit to `origin`;
2. add a concise status comment to the active integration pull request with the commit, outcome,
   local verification evidence, and any hosted checks still pending;
3. inspect failed GitHub Actions checks to root cause before changing code;
4. update the comment or add a follow-up when hosted evidence finishes; and
5. never push a dirty/rejected lane merely to obtain CI feedback.

Branch names describe the product delivery, never the execution machinery. Use release-oriented
names such as `release/market-squawk-v0.1.0` for integration and feature-oriented names such as
`feature/live-action-pipeline` or `fix/catalog-recovery` for bounded work. Do not name branches for
stages, quarters, task numbers, agents, worktrees, or orchestration lanes. Historical branch names
may remain only where renaming would destroy audit continuity; all new branches follow this rule.

GitHub is a collaboration and optional verification surface, not a runtime dependency. A clean
local exact-head gate remains mandatory. Active dirty worktrees stay local until their scoped
review and integration barrier is satisfied.

## Progress reporting contract

Status updates must lead with the outcome and include:

```text
delivered outcome
exact branch/worktree and frozen commit
active lanes and what each owns
completed focused or full gates
remaining blocker and why it is serialized
the precise event that releases the next barrier
whether root is clean and unchanged
completed worktrees removed and any dirty worktrees deliberately preserved
```

Report reviews as `pending`, `rejected`, or `approved` with the reviewed commit. Report
implementation, review, verification, and integration as different activities. Do not present a
long-running historical task label as if it explains current work.

When the user asks for a plan, hand off the completed plan promptly with its base/refresh status,
even if a separate implementation checkpoint continues. When the user asks for implementation,
continue through safe in-scope completion rather than stopping at scaffolding.

## 2026-07-16 coordination correction

During Q2 closure, the live and platform remediation lanes completed in parallel while the source
authority lane remained on the critical path. A future Q3 production plan was also produced in
parallel, but its handoff was unnecessarily withheld while the source lane underwent implementation,
full-workspace verification, rejection, remediation, and exact-head re-review. Referring to all of
that elapsed work as "Task 7" hid the actual delivered outcomes and serialization barrier.

The correction is permanent:

- deliver completed planning artifacts independently with an explicit refresh barrier;
- distinguish checkpoint remediation from ordinary task implementation;
- freeze and review only exact commits;
- keep unsafe shared-authority edits serialized while filling every safe disjoint lane;
- perform the scheduling audit when one lane remains critical across two updates or a full cycle;
  and
- communicate the blocker and release event in concrete terms.

This correction does not weaken production gates. It removes avoidable coordination latency while
preserving the authority, memory, persistence, lifecycle, and review invariants those gates protect.

## Superseded halfway instruction

On 2026-07-17 an earlier instruction defined a production-weighted halfway stop. The user then issued
explicit `resume`, `continue`, and `continue to completion` instructions and rejected a delivery
contract that could stop before Python/modeling, complete MCP, and other mandatory planes existed.
The old halfway criteria are retained in repository history only; they have no authority to pause or
terminate active delivery. Progress weighting is status information, not a stopping gate.

## Usable complete-release terminal condition

Active delivery continues through a usable complete local release. It does not stop at 50 percent,
at the end of a numbered Stage, or when only contracts, schemas, mocks, synthetic sources,
diagnostic paths, plans, or focused lane tests exist.

The terminal condition requires all of the following at one clean, unchanged exact head:

1. Every mandatory live, research, adapter, storage, point-in-time, analytics, Python/modeling,
   backtesting, portfolio, execution/risk, valuation, CLI, and MCP capability is a working bounded
   producer-to-consumer vertical slice.
2. The integrated local demonstration exercises the required CLI and complete typed MCP surfaces
   without a mandatory paid API, cloud service, external database, container runtime, or telemetry
   service.
3. Deterministic tests, separately gated authorized network smokes, parser/model/MCP fuzz targets,
   measured performance, and security, dependency, vulnerability, license, credential, and
   generated-artifact checks provide fresh exact-head evidence.
4. The Quarter 4 of 4 grouped independent review approves the same frozen commit with no unresolved
   substantiated Critical, Important, or Minor finding.
5. The exact commit is clean, pushed to `origin`, reported on the active pull request with truthful
   local and hosted evidence, and every completed lane worktree is safely removed after handoff.

Only then may implementation stop for complete-release handoff. A user-approved scope change may
alter the product contract, but progress percentage or elapsed time cannot waive a mandatory
capability or release gate.

## 2026-07-21 Task 11 research vertical closeout

Task 11 is integrated on `release/market-squawk-v0.1.0` at merge head `8f03d87`. The delivered
vertical includes provider/local revision authority, source-authored universe membership,
point-in-time selection, corporate-action evidence, leakage-bounded feature/label datasets,
authority-before-publication Arrow/Parquet storage, bounded DataFusion access, and the application
research service that owns source registration, rights admission, ingest reservation, ingestion,
dataset construction, and analytical access.

The grouped checkpoint review rejected three material defects—canonical identity compatibility,
post-allocation memory accounting, and unusable application ingest authority—and approved the
exact implementation only after all three were corrected. The completed feature branch and origin
branch were deleted, its worktree was removed, and its 18 GiB generated target was reclaimed.

## 2026-07-22 Task 12 analytics vertical closeout

Task 12 is integrated on `release/market-squawk-v0.1.0` at code head `9702556`. It delivers the
complete Rust batch-analytics families, exact financial and statistical boundary types, robust
factor regression, and the code-owned live/batch feature registry. Every one of the 43 batch
entries binds its full input schema and execution-relevant policies into the semantic digest.

The implementation retained the two existing consolidated analytics test executables and 24
focused behavioral tests. Three independent Quarter 2 reviews reported no Critical or Important
finding, and the full unchanged-head verification gate passed at `9702556`. After the release
closeout was pushed and issue `#17` was closed, its 9.8 GiB generated target, clean worktree, and
merged local/origin feature branches were removed and metadata was pruned.

## 2026-07-22 Task 13 and Task 16 core integration

Task 13 is accepted at exact head `59ba05c`. Complete immutable model bundles bind Task 11 dataset,
universe, label, and code-revision identities to Task 12 ordered feature semantics and exact artifact
hashes. The bounded registry preserves stable bundle/model series, and reusable borrowed inputs plus
shared immutable output identity keep native linear/logistic inference allocation-free. Execution
owns the model strategy boundary: every model failure produces zero intents and a typed audit fact.
The paper-bot consumer persists the mixed execution/no-action stream using an explicit v2 envelope
and never appends the new wire shape to historical v1 files.

Task 16 Steps 1–5 are accepted at exact feature head `e124722` and integrated at `9a26be4`. The
portfolio core consumes adapter-produced, source-neutral economic evidence rather than caller-made
financial scalars; publishes immutable revisions; performs checked long/short lot accounting,
income, cash flow, performance, exposure, attribution, risk, scenarios, and proposal-only
rebalancing; preserves authoritative cumulative corporate-action snapshots; and represents
unresolved basis as incomplete rather than exact zero.

Task 16 Step 6 is accepted and fast-forwarded into the release branch at exact head `7621552`.
Execution owns the portfolio capability and loads a complete immutable risk projection under the
account partition lock immediately before assessment/reservation. Risk derives its financial state
from that projection, binds the exact revision, content digest, and monotonic publication generation
into each approval, and rechecks the binding before the one permitted adapter call. Publication is
serialized and rejects rollback, stale/revoked identities, resurrection, and competing successors.
The real queued-dispatch regression proves that a revision change after approval rejects the order,
does not call the adapter, and releases its reservation. Targeted tests, strict Clippy, formatting,
boundaries, and diff hygiene passed; independent exact-head re-review reported no remaining Critical
or Important finding.

The Task 16 release checkpoint was pushed and recorded on PR `#26`; issue `#21` was closed and its
Project 5 item set to `Done`. The completed generated target was cleaned, the clean worktree and
merged local feature branch were removed, no matching origin branch remained, and Git worktree and
remote metadata were pruned. The active Python feature lane is intentionally retained until its
review blockers are corrected, re-reviewed, integrated, and closed out through the same invariant.

After the accepted release state and GitHub evidence were pushed, both completed feature targets
were cleaned (6.9 GiB total), both clean worktrees were removed, both merged local/origin feature
branches were deleted, and worktree/remote metadata was pruned. Only the release worktree remains.

Task 14's Python financial analytics/training product is implemented on its feature lane but is not
accepted: exact-head review found release-blocking gaps in Task 11 provenance, PIT/split binding,
bounded native admission, safe artifact cleanup, authenticated training-run identity, and
independent Rust expectations. Those findings must be corrected and re-reviewed before integration.
Task 15 ONNX integration begins only against the frozen Task 13 contract and must not weaken
controlled-root, exact-identity, bounded-runtime, or no-action semantics. Issue `#31` remains the
mandatory zero-fee provider onboarding portal: it may automate
official provider enrollment and key setup, while preserving and resuming any provider-required
human consent or verification step.
