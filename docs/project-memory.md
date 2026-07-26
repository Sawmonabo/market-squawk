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

## Documentation is a release product

The approved documentation system in
[`2026-07-22-market-squawk-documentation-system-design.md`](superpowers/specs/2026-07-22-market-squawk-documentation-system-design.md)
is required for the first complete local release. `docs/README.md` routes readers into focused
architecture, operations, reference, ADR, audit, plan, report, research, testing, and verification
areas. Architecture explains system boundaries and decisions; operations documents only runnable
procedures; reference describes exact current interfaces; the delivery ledger alone owns mutable
release state.

The documentation migration preserved the architecture source documents as dated audit evidence
and preserved the ONNX runbook's history in the broader model-inference runbook. Current pages use
stable GitHub-rendered Mermaid forms with accompanying prose and record direct relevant sources
with substantive review dates. Mandatory unfinished capabilities remain release blockers and do
not receive fictional operating instructions. Do not add redirect-only pages, empty section shells,
documentation checker scripts, prose tests, or new Rust test targets for documentation work.

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
state. VS Code rust-analyzer on-save flycheck is disabled for this workspace because its default
workspace/all-target invocation duplicates explicit gates; analyzer-owned Cargo invocations also
disable incremental state. Run focused diagnostics or the repository verification entry point on
demand. The verification entry point enforces a 20 GiB hard ceiling on its local `target/` before
and after the gate. Reclaim only ignored reproducible Cargo output after checking active processes
and preserving every dirty or unique worktree state.

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
remote metadata were pruned.

After the accepted release state and GitHub evidence were pushed, both completed feature targets
were cleaned (6.9 GiB total), both clean worktrees were removed, both merged local/origin feature
branches were deleted, and worktree/remote metadata was pruned. Only the release worktree remains.

Task 14's Python financial analytics/training product is accepted and fast-forwarded into the
release branch at exact code head `02ab5cd`. Fair value retains migration `0010`; Python dataset
admission is `0011`; both research-v3 and feature-label-v2 catalog triggers coexist; and the combined
workspace lock and 357-path source closure are reconciled. The product admits only catalog-backed
Task 11 point-in-time exports, preserves Decimal128 as `decimal.Decimal`, exposes bounded Rust
financial kernels, performs deterministic training, and publishes only finalized model candidates.

The first exact-head review rejected two model-publication authority defects. Remediation freezes
artifact, training-run, and final metadata bytes before authority issuance; authority schema v4
binds all three digests plus the existing dataset/model identities; Rust expectations enforce those
independent hashes; and the public API cannot select a validator executable. The only admitted
validator is adjacent to the active interpreter and must match the bounded pre/post SHA-256 compiled
into `_native`. Exact-head rereview accepted `02ab5cd` with no remaining Critical or Important
finding.

The single sealed offline matrix at `02ab5cd` passed 9/9 contracts on CPython 3.12.12 and 9/9 on
3.13.7 without retry. The persisted release-evidence identities are: manifest
`5403a73fbfe03d715b192e9da19cf9e7cfc8b7aa31f773bdd39586534b44618d`, wheel
`f19be320abd91ed73637f6d7edfa8df133ff5149cfaa8804663dadcd4134a25c`, validator
`2b8576c3e6f219f34d958c863e08cf2b68599306faa2668ba8cf348f705e1b1c`, source/wheelhouse lock
`92657a32099c7b309e9b73b674ae1ecee26f8c70d71e69e1a72f225a5e510f9a`, and build environment
`d0a9479dae9eb8024e5a4c6bfb1e5fa606a03e0858530ca3a1622b2580379931`. After fast-forward
integration, the clean Python worktree, its 5.5 GiB target and 1.2 GiB ignored release evidence, and
the merged local feature branch were removed; no origin feature branch existed; metadata was
pruned; and only the release worktree remains.

Task 15 ONNX integration was built against the frozen Task 13 contract and preserves controlled-
root, exact-identity, bounded-runtime, and no-action semantics. Issue `#31` remains the mandatory
zero-fee provider onboarding portal: it automates local setup, official provider handoff and key
activation while preserving and resuming any provider-required human consent or verification step.

## 2026-07-22 Task 18 fair-value closeout

Task 18 is accepted at exact feature head `31de1a5`, merged into the release branch at `051ee3c`,
and lock-reconciled at `5c34b7d`. It delivers durable ASC 820/IFRS 13 measurement, evidence,
classification, override, approval, revocation, market-access, audit, recovery, and bounded service
authority. Live evidence crosses a count-and-byte-bounded post-action export rather than the hot
path; research, feature, and portfolio evidence retain their independent producer authority.

Level 1 requires the complete code-owned conjunction for identical unadjusted quoted evidence in
an active, accessible market at the measurement cutoff. Historical activity binds source, receive,
availability, qualification-evaluation, and qualification-validity times. Level 2 and Level 3 do
not become execution quality, and neither Level 1 nor an `Unclassified` decision can be created by
override. Catalog writes use one coherent transactional snapshot, stale-writer comparison,
canonical identities, semantic recovery, audit-chain triggers, and global record/result bounds.

The accepted implementation retained one consolidated fair-value integration executable. Focused
locked valuation, live-export, catalog/query, strict Clippy, formatting, and diff gates passed on
the merged release tree. Independent review found two material PIT/override defects; the exact-head
remediation rereview found no remaining Critical or Important blocker. The closeout was pushed,
issue `#23` and its Project 5 item were marked Done, and the 2.9 GiB feature target, clean worktree,
merged local branch, and remote metadata were removed.

## 2026-07-22 Quarter 3 remediation state

Tasks 13–18 reached pre-status capability-code head `daf183a` on
`release/market-squawk-v0.1.0`. That durable milestone is not the moving release-branch head; obtain
the current exact head from Git and pull request `#26`. The first frozen grouped review rejected the
candidate with thirteen Important findings. Fair-value evidence-authority remediation is integrated
through release head `6c114c7`. Backtest recovery authority is integrated through `a57d5df`, and
ONNX lifecycle authority is integrated through `3305db6`; both four-commit series range-diffed 1:1
and passed fresh focused tests on the release tree. Their generated targets, clean worktrees, and
patch-equivalent local branches were removed after push.

The subsequent grouped review of exact head `a94f33b` accepted the Tasks 13–15 slice and the Task 18
plus cross-plane slice with no Critical or Important finding. It rejected the Tasks 16–17 slice on
five substantiated Important blockers: corporate-action knowledge cutoff, portfolio-analytics
point-in-time/source authority, portfolio analytics resource and retained-byte bounds, historical
instrument-definition authority in backtests, and canonical reservation-bound attempt recovery.
The suggested coupling of a pure cohort plan to one inventory's configured trial limit was rejected
because service-owned admission already enforces that local limit and adding it to the pure value
would invert the established boundary. Task 16 is reopened; Task 17 remains open. The full gate must
not run until those five blockers are fixed and the exact-candidate rereview accepts the result.

The two bounded remediation lanes closed the original five blockers at their focused task-review
boundaries. Historical backtests consume a catalog-minted point-in-time instrument-definition
receipt, resolve terms at every decision cutoff, bind both receipt identities into dataset identity
v2, and validate the complete fixed-space attempt namespace against the actual canonical
reservation. Portfolio analytics derives sealed evidence from the revision, binds
dataset/source/policy/time authority, and enforces both corporate-action cutoffs. Both lanes
received independent focused approval with zero remaining finding at those boundaries.

The portfolio series range-diffed 1:1 onto release heads `71da30d` and `91f9f79`; its fresh
integrated harness passed 13/13 before push and cleanup. The backtest series range-diffed 1:1 onto
release heads `1814b25` and `c70601a`; fresh integrated gates passed catalog 3/3, backtesting 12/12,
and the real application authority vertical 1/1. That vertical enlarged the existing consolidated
`control_plane` test executable beyond arm64 compact-unwind's measured limit, so the previously
approved Rust #159105 classification was scoped to that test root after measuring a 354,986,784-byte
binary and 23,027,100-byte `__eh_frame`. Workspace-wide linker diagnostics and production linking
remain unchanged. The grouped review of frozen exact head `797a359` accepted the backtest slice and
cross-plane boundaries with zero finding but rejected the portfolio slice on three Important
defects: reporting currency is absent from the immutable revision digest; Attribution and Risk do
not admit complete aggregate work or consistently enforce the instrument ceiling; and Exposure's
temporary `BTreeMap` nodes allocate infallibly. One grouped portfolio lane closed those defects at
integrated head `e468d01`: revision identity v3 binds reporting currency plus schema name, version,
and fingerprint; Attribution and Risk admit complete aggregate work and instrument counts; and
Exposure uses fallibly reserved deterministic vectors with UTF-8-safe in-place ASCII
normalization. The first task review found omitted schema name/version and byte-wise UTF-8
corruption; the follow-up fixed both, and exact-head rereview accepted with zero finding. The fresh
integrated portfolio harness passed 15/15. Corrected portfolio and cross-plane Quarter 3 reviews of
frozen candidate `053f5e2` both accepted with zero Critical, Important, or Minor finding. Together
with the previously accepted Tasks 13–15, Task 18, and backtest slices, this accepted the Quarter 3
candidate. At that checkpoint, the root target still had to be cleaned before the one
nonincremental full release gate, and issues `#20`, `#21`, and `#22` remained open pending that
gate. The terminal outcome is recorded in the 2026-07-23 section below.

Task 15 provides required zero-service ONNX inference through the self-contained Rust
`TractOnnxBackend`. It admits exact bounded graphs and tensors, runs through a bounded model-owned
helper process, binds warm-up evidence, and maps every policy/runtime/deadline failure to no action.
The modeling library's operator-supplied ONNX Runtime 1.24.4 path is optional and Linux-only; it
admits an exact descriptor-verified ELF library through immutable sealed memory and requires
warm-up parity. The reviewed product composition does not select that backend and always constructs
tract. Cleanup ownership exists before helper spawn, blocking reap/join work remains asynchronous
and bounded, and uncertain helper termination denies tract fallback and produces no output.

The sealed Python package now installs its native extension as `market_squawk/__init__.abi3.so`.
Native signed-environment verification of the complete Market Squawk, PyArrow, interpreter and
native-library file sets is therefore the first shipped code executed by `import market_squawk`.
Only after verification succeeds are native APIs, the compatibility alias and mutable Python
modules exposed. The focused sealed matrix passed 10/10 on CPython 3.12 and 10/10 on CPython 3.13;
the rebased integrated source closure was separately re-admitted without a rebuild.

Task 17 provides application-owned point-in-time backtesting over exact dataset and partition
authority, source-authored historical universes, bounded admitted strategies, research execution
assumptions, deterministic portfolio accounting/reconciliation, reserve-before-run experiment
governance, immutable artifacts and exactly one success/failure terminal. Cohort, deflated-
performance and overfitting diagnostics remain research evidence and cannot mint execution
authority. Task 18 retains the accepted fair-value closeout above.

The earlier model-containment lane rebased with an exact 1:1 range-diff, fast-forwarded, and closed
cleanly. Its 7.1 GiB target, worktree and merged local branch were removed; no matching origin branch
existed. All three previous product-named remediation worktrees are accepted, integrated, pushed,
and removed. Their matching local branches are deleted, no matching origin branches remain, and
metadata is pruned. The three protected stashes and `bundle-backup` remain. New remediation uses
only two cohesive product lanes, reuses consolidated test harnesses, enforces the 10 GiB lane target
ceiling, and cleans each lane after accepted integration. The root target remains generated cache
only and must be cleaned immediately before the planned Quarter 3 full gate. The
canonical plan defines the final delivery
quarter as Tasks 19, 19A and 20, preserves the descriptor-driven shared CLI/MCP architecture, adds
the evidence-bound local onboarding portal, and rejects redundant standalone test executables and
checker scripts.

The portfolio revision/resource closeout fast-forwarded the exact two-commit lane through
`e468d01`, reclaimed its 1.7 GiB generated target, removed its clean worktree, deleted the merged
local product branch, confirmed no matching origin branch existed, and pruned metadata. Only the
release worktree remains; the protected stashes, `bundle-backup`, and Dependabot refs remain intact.

## 2026-07-23 Quarter 3 terminal gate and Quarter 4 start

Quarter 3 is terminally accepted at exact pushed head
`c6f0124c2b27c4777947de8c42b6a5f97868aaf5`. This supersedes the earlier operational statements
that the Quarter 3 full gate was still pending. The final delta review reported no Critical,
Important, or Minor finding.

The first clean gate attempt exposed a real Cargo/rustdoc target collision between the application
library and the Python extension. The production fix gives the Rust Python target the unique crate
name `market_squawk_python` while retaining Maturin module name
`market_squawk.market_squawk`, the `market_squawk` PyO3 initializer, and the shipped Python import
identity. The final reviewer then rejected the intermediate head because the sealed wheel source
authority still bound the old manifest and omitted accepted Quarter 3 source changes. The lock now
binds 370 sorted, unique paths; the reviewer independently compared all 370 sizes and SHA-256 values
to the exact Git blobs with zero mismatch. One concise assertion in the existing release-builder
harness calls the production source-admission path, so the ordinary verification gate now rejects
future closure drift without adding a test file, target, script, or authority surface.

`CARGO_INCREMENTAL=0 ./scripts/verify.sh` exited zero on the same unchanged exact head. It passed
103 Python checks, dependency/license/vulnerability and credential-history checks, formatting, both
workspace Clippy modes, complete locked all-feature tests, UI/Trybuild, Loom, locked all-feature
release build, rustdoc contract inventory, offline product smoke, and stdio MCP smoke. Generated
output peaked at 15,131,260 KiB, below the 20 GiB ceiling. `cargo clean` then removed 36,502 files
and 14.3 GiB; `target/` is absent and approximately 125 GiB is free.

GitHub issues `#20`, `#21`, and `#22` are closed and their Project 5 items are Done. Only the root
release worktree remains; local and origin release heads match. The three protected stashes,
`bundle-backup`, and Dependabot refs remain intact.

Quarter 4 is the final delivery quarter and consists only of Tasks 19, 19A, and 20. Task 19 owns the
shared transport-neutral application services, complete CLI hierarchy, complete bounded typed MCP
domains, control-plane configuration, and doctor surface. Task 19A owns the zero-fee provider
capability/onboarding portal and secure credential lifecycle. Task 20 owns the integrated all-
vertical demonstration, benchmark/fuzz/security/release evidence, final review, exact-head gate,
publication, and closeout. Open prerequisite issues `#7`, `#9`, `#10`, and `#11` must be reconciled
through these product slices or closed with exact evidence; none may be silently ignored. The
approved production documentation-system design and committed implementation plan control the
required migration before Task 20 closeout.

## 2026-07-23 Task 19/19A provider-activation checkpoint

The approved documentation-system design and implementation plan are committed at `a4ba164` and
`4bcd654`; the design-review gate is complete. The production migration remains required before
Task 20 release closeout.

Provider activation is pushed through `ae94d6c`. Code-owned onboarding profiles now bind the full
rights decision, the selected exact persistence evidence, duties, release state, capability
revision and current evidence digests. Research adapters can no longer receive independently
caller-constructed persistence authority. SEC, BLS and Treasury Fiscal Data portal requests build
provider-specific adapter configuration, publish digest-bound evidence and the desired restart
recipe before registration, and serialize publication per provider surface. Exact retries are
idempotent; a rejected candidate can disable only its compare-and-swap-matched recipe. Invalid or
superseded restart state disables only the affected provider while unrelated product domains keep
starting.

Platform-managed credential work is admitted through one shared blocking-operation permit. The
permit is retained until the worker exits, cancellation is rechecked after backend serialization,
and an early-returning request hands the worker to a runtime reaper. Restart restoration performs
no secret read or platform interaction: registered BLS and FRED recipes remain desired but disabled
until an explicit foreground resume. The independent provider-evidence audit passed with zero
Critical, Important or Minor finding; its tracked validation record and exact hashes are in
`docs/research/2026-07-23-provider-activation-evidence-validation.md`.

Measured macOS debug-link classification is pushed at `0521d0e`. It remains limited to the exact
oversized debug/test roots; release diagnostics and generated code are unchanged. The locked
optimized application build succeeded, and the release MCP process started and stopped cleanly
twice against the same fresh local state root.

The shipping MCP contract is corrected at `8f82483`: `market-squawk mcp serve` and the compatibility
`market-squawk mcp` form use the sole production application composition. The removed diagnostic
five-tool journal server is no longer advertised or tested. One existing consolidated composition
test compares the complete served tool list to the application capability registry and verifies
durable audit, controlled artifacts and a governed mutation.

Fresh focused evidence on the committed tree passed: application `control_plane` 23/23; BLS 13/13;
FRED 15 passed with one controlled local-evidence test ignored by contract; shared
sources 205/205 including doctests; app all-target/all-feature Clippy with warnings denied;
formatting; diff integrity; the locked all-feature release application build; and two same-root
release MCP startup cycles. Root generated output is 15 GiB, below the enforced 20 GiB ceiling.
Only the release worktree exists, `.worktrees` is empty, local branches are `main` and the release
branch, and local/origin release heads match.

Issues `#24` and `#31` remain In Progress. Task 19A still requires its clean-machine final
demonstration and closure of every required provider workflow; Task 19 remains open until its
application/CLI/MCP acceptance evidence and prerequisite issue reconciliation are recorded. Task
20 remains Todo and owns the final exact-head product proof, release review, publication, and
repository cleanup after the documentation candidate is accepted.

## 2026-07-23 product documentation candidate

The GitHub-native documentation portal is assembled on `docs/product-documentation` against frozen
product head `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` and tree
`774a7bc9f4f26eb437fa1ab061dc4b557d20d0bc`. The migration preserved the two architecture
baselines as dated audit evidence, preserved the model runbook's `git log --follow` ancestry, and
added focused architecture, ADR, operations, and reference pages plus reader-oriented indexes. The
published content waves through `531a7df` contain no product/build-input delta from the frozen head.

Documentation work used one shared root worktree and no per-page branch, worktree, Cargo command,
test, checker, generator, or build-cache duplicate. At the candidate checkpoint, `.worktrees` is
empty and the existing root `target/` is 15 GiB, below the enforced 20 GiB ceiling. Completed
content and closeout commits are recorded in the delivery ledger.

The first frozen candidate `b0ed3e9` completed its bounded content, navigation, GitHub Mermaid, and
three-scope grouped review gates but was rejected on substantiated findings. Correction head
`e063419` closed the architecture and reference findings; operations re-review rejected three
remaining copy/paste and public-query ripples, which accepted head `a2596a6` corrected. The complete
correction set repairs authority diagrams, fail-fast no-overwrite installation and recovery
procedures, explicit restore coordinates, historical/current navigation, and public product truth.
At that historical head it also recorded then-open first-use handoff gaps. The 2026-07-24
integrations below supersede its provider-discovery, dataset-cursor/export, feature-registry,
artifact-read, paper-fee, and sealed application/worker entries. Current unresolved blockers remain
a supported production training driver and ONNX candidate demonstration, provider qualification and
rights outcomes, clean-machine acceptance, and Task 20's final release gate.

Accepted exact head `a2596a6ae4dafa9915d2b42cac71635c77c632f8` closed every review finding;
all three final scopes reported zero Critical, Important, or Minor findings. Documentation Tasks
1–7 are complete. The accepted head was fast-forwarded to `release/market-squawk-v0.1.0`, published
to PR `#26`, and the completed `docs/product-documentation` branch was deleted locally and on
origin before metadata was pruned. Product issues and Project 5 items remain open until their
separate acceptance evidence exists. Work resumes with Tasks 19/19A, then Task 20.

## 2026-07-24 research/model first-use and shared-release checkpoint

Research/model first-use authority is integrated through merge head `92f2b72`. The accepted lane
supplies durable
point-in-time feature datasets, stable bounded pagination, retained Python export digests, signed
application/validator/ONNX-worker release construction, and bounded artifact-read worker ownership.

Release commit `3ef05dc` composes those capabilities into the product:

- one application-owned, path-free `ArtifactRepository` is shared by CLI and MCP;
- `Analysis.ReadArtifact` reconstructs the complete opaque identity, verifies the complete artifact,
  and returns only caller-bounded 32 KiB Base64 chunks;
- the production model domain verifies the running application and sibling ONNX worker against the
  signed release-manifest digests;
- configured paper initial cash becomes an immutable evidence-bound sandbox portfolio revision
  consumed by central risk instead of an unavailable placeholder; and
- the production analysis service receives the durable feature-dataset reader.

Fresh focused evidence passed at this checkpoint: the exact production MCP composition, paper
composition, and merged point-in-time backtest tests; affected services/application/MCP/modeling
Clippy with warnings denied; formatting; and diff integrity. A services-only Clippy gate exposed
and corrected an undeclared Serde `rc` feature dependency before the commit was pushed.

The accepted research worktree target removed 8.3 GiB, after which the worktree, merged local
branch, origin branch, and stale refs were deleted.

Coinbase Direct integrity candidate `6182da007312023ef5fa78a0537ccb273d63a24f` completed final
independent review with zero Critical, Important, or Minor findings and was integrated at release
merge `4cb6e02124a3f730430e4e152b1b1f29e7e0f9fe`. The later authenticated Direct transport candidate
`cef4d59` also completed independent review with zero findings. Its four commits rebased one-for-one
onto the current release history and were accepted unchanged through `ff406e9`; source authority
was reconciled at exact integrated head `2e6d6c6faca8bd23e5496a717787a043068dc517`, tree
`eadc19e1d02324a0af2eef1a0596f4945a9eb239`, with 790 of 790 expected source files locked.
Focused authenticated-profile, bounded bootstrap/queue, same-owner handoff, and
sink-rejection-before-mutation tests passed with strict Coinbase/sources Clippy, formatting, and
diff integrity. The accepted target, worktree, merged local branch, and stale refs were removed;
no matching origin feature branch existed. The remaining product boundary is application-owned
credential activation, shared provider-rate authority, shipping composition, central
qualification, strategy/risk/paper authority, and an authorized unchanged-head trace. Issue `#7`
therefore remains open and In Progress.

GitHub Actions run `30131065227` at `3ef05dc` created `verify`, `macos`, and `windows` jobs with
empty step lists; no workflow step executed. This was an external Actions account
billing/spending-state blocker rather than a code-owned failure.

## 2026-07-24 release-truth reconciliation and active product lanes

Release documentation commits `241599a` and
`3c2cfe5516db47e7a3489921586360332ff3725c` reconcile maintained documentation with code already
present at the reviewed product head:

- the sole production stdio MCP registry contains 62 tools;
- CLI object listing and receipt-bound provider discovery-to-ingestion are public, bounded product
  paths; and
- `LocalProduct` composes OS-keyring-first secret storage with an initially locked, code-owned
  encrypted fallback whose only unlock/lock surface is the foreground loopback portal.

These corrections removed false release blockers without product-code changes, Cargo commands,
new tests, checkers, generators, or scripts.

The independent Coinbase review rejected initial candidate `09f289f` before integration with zero
Critical and seven Important findings: auction authority, trusted observation time,
generation/frame/receipt binding, current modify-order price semantics, bounded live publication,
secret zeroization, and focused proof of those exact invariants. Subsequent exact-head reviews
rejected two remaining lifecycle-authority defects and accepted `6182da0` only after both were
closed. No rejected intermediate was integrated.

The grouped `feature/research-model-release` lane completed public analytical-query overflow and the
supported production training/ONNX candidate workflow at exact accepted head
`37b664111ff8f3342a9851b411f5fe6b6f16dc97`, tree
`bda7c091a19fdbda58c365df74aaeb8125fa37d5`. Independent final review reported zero Critical,
Important, or Minor findings. Strict app Clippy, formatting, source-lock admission, and the exact
787-file closure passed.

The direct sealed release matrix passed 11 tests and 2 training subtests on each of CPython 3.12.12
and 3.13.7. It exercised signed linear and logistic candidate production/admission and bounded
logistic prediction. The release-manifest, evidence, and wheel SHA-256 identities are respectively
`f0409fe78a8bafbb188b625abb03a468a0772ffb9b9c7ca571b5f11aa21e8d72`,
`69b8ae141694f360e8917c3d7649b05034b0a8c5c5e1387ecf595c871aa9d714`, and
`f972e8bdcf3fd0bb35aa6835db6df1935fcecb2cefb28af93d350e1a59632da6`.
The release branch fast-forwarded unchanged to that candidate. Its 9.0 GiB generated target,
worktree, and merged local branch were removed; no matching origin branch existed.

Generated Cargo state is approximately 6.1 GiB in root and 5.0 GiB in the sole active onboarding
lane. Both remain below their enforced ceilings. Task 4 issue `#9` is closed and its Project 5 item
is `Done`; `#7`, `#10`, `#11`, `#24`, and `#31` remain In Progress, and `#25` remains Todo.
Actions run `30144094932` at integrated Coinbase head `2e6d6c6` created three jobs with empty step
lists. Each check reported that recent account payments failed or the spending limit must be
increased; no repository checkout or code-owned CI step ran.

## 2026-07-25 release-source and performance-evidence checkpoint

The moving release head is `620d212ea4f28b8b50130fb5f430a42947e41cd4`, tree
`596d9b33ef6415741b19e230a91a416fd907b77c`. Release-source authority is integrated through
`c8ceb82`: authenticated Coinbase Direct transport remains distinct from the public Coinbase and
Kraken compatibility sources, whose declared quality ceilings remain `DirectUnverified`.

Release-performance candidate `afd9a58c7f8e36be7448543d61da2b0e6f36be10` was accepted by an
independent exact-head review and merged at `620d212`. Its evidence publisher keeps the final path
absent until post-measurement executable/repository identity validation, then performs an atomic
no-clobber commit with exact transaction-owned cleanup. RSS results now report only finite
observations with optional maxima, sample counts, and observation windows. Focused release-evidence
application check, formatting, and diff integrity passed at the integrated head.

The completed performance lane, its 1.0 GiB target, both merged local branches, and the remaining
origin feature branch were removed and pruned. The root target is 7.1 GiB and the sole active
provider-onboarding lane target is 6.9 GiB; both incremental caches are 0 bytes and approximately
113 GiB is free. GitHub Actions run `30179378256` created three empty-step jobs and reported the
account-level payment/spending-limit annotation, so it contains no code-owned CI failure.

Task 6 issue `#11` is closed and its Project 5 item is `Done`. The open issues are `#7`, `#10`,
`#24`, `#25`, and `#31`; Tasks 19A and 20 are In Progress. The provider-onboarding remediation is
the sole active feature worktree. Exact-head production measurements and the remaining product
acceptance work keep the release blocked.

## 2026-07-25 provider-onboarding control-plane checkpoint

The moving release head is
`3219662a44026b45201b96e087d570c2f48a3724`, tree
`f114fe931b90fbb91adac2361df115dd94e57390`. It merges the independently accepted
provider-onboarding candidate `489113fae63ae2e7288be2bf784abea6651a8bec` without changing the
accepted provider blobs. The integrated control plane now owns shared provider rate budgets,
generation-bound activation, transactional credential replacement, bounded failed-cutover
recovery, candidate-preferred renewal, and retained portal transaction ownership through durable
mutation and shutdown.

Focused integrated verification passed: the release-evidence application check; the one-shot
source activation vertical; the exact prepared-candidate cutover transition; failed replacement
recovery; strict application Clippy both without defaults and with `release-evidence`; formatting;
and staged/unstaged diff integrity. Independent exact-candidate and merge-resolution reviews found
no material blocker. Broad workspace verification remains reserved for Task 20.

The completed provider lane reclaimed 6.8 GiB of generated output. Its clean worktree and merged
local branch were removed, no matching origin branch existed, and worktree/remote metadata was
pruned. Only the release worktree remains; `.worktrees` is empty. Root `target/` is approximately
11 GiB, `target/debug/incremental` is 0 bytes, approximately 113 GiB is free, and the root cache
remains below its 20 GiB ceiling.

GitHub Actions run `30182462309` at the exact integrated head created `verify`, `windows`, and
`macos` jobs with no steps. GitHub reported the account payment/spending-limit annotation before
checkout, so this run contains no code-owned CI failure. Issue `#31` remains In Progress: Treasury
is release-available, while SEC/BLS evidence refresh, FRED rights, and the clean-machine
activation/recovery acceptance remain open.

## 2026-07-25 Coinbase Direct application checkpoint

The moving release head is `041175590bd2e4a357ea28d75c675c252d3b3746`, tree
`b0233f5b60cb6451d3feab410dba32abcc1144ba`. The shipping application now composes an exact active
`coinbase.exchange-direct-market-data` onboarding session through its current signer, shared
provider-rate and account authority, authenticated snapshot/live transport, canonical
`BookSnapshot`/`BookDelta` publication, central qualification, strategy, risk, dispatcher, audit,
checkpoint, and realistic paper execution. CLI and MCP both select this authority through
`Bot.Start` provider `coinbase-direct` plus the exact session UUID. Public Coinbase and Kraken
remain distinct `DirectUnverified` paths.

Generation-bound start cancellation prevents stop or kill-switch requests from racing into a late
running publication. Terminal Direct supervisor exit clears source health and cancels the shared
run token; bot status reports failure and source, market, and execution operations fail closed until
stop owns cleanup. Direct market observations are not sent to research or fair-value persistence
because the current rights admit retrieval/display only.

Focused verification passed formatting, application all-feature compile, strict application
Clippy, binary compile, and all 47 existing application tests. A narrow read-only re-review
confirmed both lifecycle remediations and found no regression in their immediate state/health
surface. No new test target was added. The remaining Direct blocker is an authorized unchanged-head
external credential-to-qualified-book-to-paper trace, coordinated with the provider clean-machine
acceptance work and final Task 20 evidence.

## 2026-07-25 provider release-admission checkpoint

Provider release admission is integrated at product head
`bf02a0b3d35108f1ef771f3e7e292a552395f126`. The shipping
`release evidence providers` command is the sole provider acceptance producer. It must use the
production `LocalProduct` authorities, an exact clean repository head/tree, explicit operator
network and terms gates, exact built-in surface identifiers, an absent controlled output
directory, and the running executable's stable identity. It records credential-free capability,
rights, runtime-response, live-quality, paper-action, shutdown, and restart-recovery evidence and
publishes only after the repository and executable remain unchanged.

The release closer requires the complete mandatory provider set, exact evidence-to-release-binary
identity, `DirectVerified` Coinbase Direct quality with a nonempty risk-approved paper action,
`DirectUnverified` public Coinbase/Kraken evidence with no orders, callable durable research
runtimes, admitted FRED/ALFRED persistence and model-training rights, and exact restart recovery.
Diagnostic subsets cannot close the release.

Capability revisions 1 and 2 remain immutable. Revised SEC, FRED/ALFRED, BLS, Treasury daily XML,
and Treasury Fiscal contracts use contiguous revision 3. FRED limits are enforced conjunctively in
both capability and activated-runtime layers. Treasury daily XML uses one code-owned completed
year, an exact family/year request, and the production bounded schema parser.

The acceptance producer does not convert missing external evidence into authority. SEC and BLS
remain refresh-required until accepted successful official bodies exist; FRED/ALFRED durable use
remains rights-blocked; Treasury daily XML durable publication remains closed; and Coinbase Direct
still requires its authorized unchanged-head external trace. Issues `#7` and `#31` remain open until
those exact predicates are satisfied. The provider release-admission worktree, its 6.9 GiB generated
target, and its merged local/origin feature branch were removed after integration and push.

## 2026-07-26 Task 19 local control-plane acceptance checkpoint

Task 19's local application, CLI, and MCP implementation is accepted and pushed at exact release
head `879e505223729fee4a5be607b21a6deb396f849f`, tree
`76c096f88f5b6cde7c465275c52bd39bbb2d9fdb`. The shipping CLI now owns initialized and recovered
full-product startup, redacted configuration provenance, and bounded query-only diagnostics over
existing storage. `doctor` does not create, migrate, recover, or exclusively lock product
authorities. The sole 62-tool stdio MCP registry advertises and validates operation-specific output
schemas, distinguishes actionable tool rejections from protocol failures, and drains through
application-owned bounded shutdown. Platform termination listeners are installed before product
composition so Unix or Windows termination arriving during startup cannot bypass cleanup.

Focused application, services, and MCP verification; strict affected-package Clippy; formatting;
diff integrity; shipping MCP smoke; repeated byte-identical nonmutating diagnostics; and a real
startup-time SIGTERM probe passed. No new test executable was added. Independent exact-range
re-review reported zero Critical, Important, or Minor findings.

Correction `a3609b3aa4890fe6970d3994abf2bd172f9d3239`, tree
`ef62a12c7b9be1172b57d2f0d7dc609cd28c7509`, moved provider-generic order synchronization and exact
decimal normalization into `market-squawk-sources`, retained the live crate's public API through
exact-type re-exports, and removed the Coinbase adapter's normal dependency on the live crate. The
required workspace-boundary gate, 264 existing sources/live/Coinbase unit tests, strict affected-
package Clippy, application compile, formatting, and diff integrity passed. No new test or test
target was added. Issues `#10` and `#24` are closed and their Project 5 items are `Done`.

After the accepted head was pushed, the Task 19 lane reclaimed 8.4 GiB, its clean owned worktree and
merged local branch were removed, no matching origin branch existed, and worktree/remote metadata
was pruned. Only the release worktree remains. Root generated state is approximately 13 GiB,
`.worktrees` is empty, root incremental state is approximately 9 MiB, and approximately 114 GiB is
free. Draft release PR `#26` is the sole open pull request; no dependency-bot PR remains open.
Hosted Actions run `30183191490` again created three empty-step jobs and reported the account
payment/spending-limit annotation before checkout, so it provides no code-owned CI failure to fix.

The boundary-correction lane subsequently reclaimed 1.9 GiB, its clean worktree and merged local
branch were removed, no matching origin branch existed, and worktree/remote metadata was pruned.
Only issues `#7`, `#25`, and `#31` remain open and In Progress. Root generated state remains
approximately 13 GiB, `.worktrees` is empty, root incremental state is approximately 9 MiB, and
approximately 114 GiB is free.

## 2026-07-26 release-PR ancestry and CI recovery checkpoint

GitHub stopped scheduling `pull_request` workflows after `b15178b` because release PR `#26` had
become merge-conflicted with `main`. The only five mainline-only commits were the merged Dependabot
updates for anyhow 1.0.104, clap 4.6.4, serde_json 1.0.151, tokio 1.53.1, and
tokio-tungstenite 0.30.0. Those exact versions were already present in the release lockfile and had
been integrated and verified at `b15178b`; the histories differed only because main retained the
five individual bot commits while release retained the consolidated integration commit.

An ancestry-only `ours` merge at `ed86d4ff5b4ff26a8b22a8bf7b592a50cc1e714e` records
`ea2408eddb5a521aae2d766a059f9db0b4bbb904` as the second parent while preserving exact release tree
`4087626a8c3722fd07d38f2bd970ba316e30d2e2`. PR `#26` immediately changed from conflicting to
mergeable and scheduled run `30197493366`. Its `verify`, `macos`, and `windows` jobs contain no
steps; all three were rejected before checkout by the GitHub account payment/spending-limit
annotation. Repository workflow enablement, trigger configuration, PR ancestry, and current-head
scheduling are therefore healthy; hosted execution remains externally blocked until the account
state is repaired.
