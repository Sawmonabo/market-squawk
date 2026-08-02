# Market Squawk Lean Delivery Control Design

## Document control

- Written: 2026-07-19
- Product audit anchor: `23bfecc1bfebc32364ffc68584aa18fb5b3c465c`
- Canonical delivery plan: [usable-complete release plan][canonical-plan]
- Canonical dependency and path ownership:
  [`usable-release-path-ownership.json`](../../verification/usable-release-path-ownership.json)
- Binding project decisions: [`project-memory.md`](../../project-memory.md)
- Disposition: the design decisions were approved in conversation; this written specification must
  be reviewed before an implementation plan is written or GitHub delivery state is mutated.

This document controls delivery operations. It does not replace product requirements, task scope,
the dependency graph, path ownership, or exact-head release evidence. It is intentionally small so
maintaining the control system cannot compete with building Market Squawk.

## Problem and outcome

The project has repeatedly spent too long in review/remediation loops while progress state was
distributed across chat, branches, worktrees, reports, and pull-request comments. The correction
must make the next product action obvious, keep all required capabilities in scope, use safe
parallelism, and force repeated review churn to terminate in an explicit decision.

The outcome is one lean live operations surface:

- the existing GitHub repository and its existing pull request remain in place;
- exactly 21 task issues represent Tasks 0 through 20 from the canonical plan;
- one private GitHub Project linked to the repository presents those issues;
- Git commits, annotated checkpoint tags, gates, and issue transition comments carry exact evidence;
- root is the only writer of GitHub delivery state; and
- review, remediation, barriers, and benchmark preparation have finite state transitions.

This is not a second product repository, a new source of scope, or an invitation to add project-
management software to the application.

## Authority and truth precedence

When two artifacts appear inconsistent, use this order instead of reconciling by intuition:

1. The canonical release plan, project memory, and path-ownership JSON define product scope,
   dependencies, execution barriers, and file ownership.
2. Pushed Git commits and trees define integrated code. Annotated checkpoint tags identify an exact
   reviewed candidate and its disposition.
3. Exact command output bound to an unchanged Git SHA defines gate or benchmark evidence.
4. The latest root-authored transition comment on a task issue defines that task's current
   operational state, exact candidate SHA, material blocker, and next action.
5. GitHub Project `Status` and `Delivery Cut` are a visual projection of the preceding authorities.

Worktrees are temporary working state only. Agent messages and chat updates are handoffs, not
durable project truth. Existing historical reports remain audit history but cannot override a newer
exact-head decision.

## Minimal GitHub operating surface

### Repository and issues

Keep `Sawmonabo/market-squawk` as the product repository. Create exactly one issue for each
canonical Task 0 through Task 20. Do not create one issue per review finding, test, commit, subtask,
adapter endpoint, or document.

Each task issue contains only durable routing information:

- canonical task number and title;
- link to the exact task section in the canonical plan;
- quarter and dependency list from the ownership JSON;
- concise release outcome and acceptance boundary;
- current integration or review candidate when one exists; and
- a statement that the canonical plan and ownership JSON control on conflict.

Issue bodies do not copy the complete task plan. Subtasks remain in the canonical plan and normal
Git history. Review findings remain in a consolidated transition comment on the owning task or
quarter checkpoint, not in a proliferation of new issues.

### Private project

Create one private user-owned GitHub Project and link it to the repository. Add the 21 task issues.
Use GitHub's built-in `Status` field and exactly one custom single-select field, `Delivery Cut`.

`Delivery Cut` has four values:

- `Quarter 1 of 4`
- `Quarter 2 of 4`
- `Quarter 3 of 4`
- `Quarter 4 of 4`

The built-in `Status` field has these values:

- `Backlog`: dependencies or quarter admission are not yet satisfied.
- `Ready`: dependencies are satisfied and the task may take a writer slot.
- `In Progress`: production implementation or integration is active.
- `Blocked`: one named material barrier prevents meaningful progress.
- `In Review`: a frozen exact candidate is in a required gate or finite review cycle.
- `Quarter Approved`: the task is included in an approved unchanged quarter head.
- `Done`: the terminal Task 20 release decision has made the task complete.

Create only two saved views:

1. `All Tasks`: an unfiltered table showing issue, Status, and Delivery Cut.
2. `Active`: a board grouped by Status and filtered to exclude `Quarter Approved` and `Done`.

Do not add priority, percentage, estimate, iteration, wave, owner, risk, gate, SHA, review count, or
blocker fields. The canonical plan already owns dependencies, waves, and ownership. Exact mutable
evidence belongs in task transition comments.

### GitHub-state writer

Root is the sole writer of issue state, Project fields, checkpoint tags, and integration-cut
pull-request comments. Subagents do not edit issues, Project items, labels, milestones, releases, or
pull-request state. They return structured handoffs to root containing:

```text
task
candidate SHA
owned paths
focused gates and results
material blockers
review state or findings
recommended next transition
```

Root validates the handoff against Git and the canonical authority before writing one transition.
This prevents racing status writers and keeps a transition coupled to an observable repository
state.

### Transition comments

Write a task comment only when operational state changes: task admission, candidate handoff,
material block/unblock, review disposition, integration, quarter approval, or terminal completion.
Do not post heartbeat, elapsed-time, percentage, speculative schedule, or unchanged-state comments.

A transition comment uses this compact form:

```text
State: <previous> -> <current>
Candidate: <40-hex SHA or none>
Evidence: <commands, exact evidence reference, review disposition, or none>
Blocker: <one material blocker and critical_since, or none>
Next: <single dependency-releasing action>
Authority: <canonical task/quarter reference>
```

GitHub supplies author and timestamp. When blocked, `critical_since` is the timestamp at which the
exact named barrier first became critical. It resets only after that barrier closes or materially
changes. Root may correct a stale Project field directly from the latest authoritative transition
without creating another comment.

The active pull request receives comments only for meaningful integration cuts: pushed Wave heads,
quarter freezes and dispositions, and the final release result. Task chatter stays on task issues.

## Delivery scheduling

### Work in progress and ownership

With four agent slots, root keeps its integration slot and dispatches at most three disjoint
current-quarter writers or reviewers. One worktree owns a cohesive lane, not one small task. The
canonical ownership JSON and Wave table determine whether work is disjoint. Shared manifests, the
lockfile, application composition, authority handoffs, checkpoint evidence, and cross-lane conflict
resolution remain serialized under root.

Do not fill capacity with future-quarter documentation, speculative scaffolding, or optional work
while a current-quarter product barrier is rejected. A blocked lane should release its slot unless
it has another dependency-safe owned action that advances the same current-quarter outcome.

Root selects the next action in this order:

1. consume a ready exclusive benchmark or gate window;
2. integrate a handoff-ready candidate that releases dependencies;
3. close the current material blocker;
4. fill remaining slots with disjoint current-quarter production work; and
5. perform documentation or research only when required by steps 1 through 4.

This order is operational, not a change to the canonical dependency graph.

### Barrier aging

An exact barrier that remains critical for two consecutive root updates or 90 minutes receives one
bounded ten-minute scheduling audit. The audit must return exactly one decision:

- split off a dependency-safe portion and run it in parallel;
- stop a non-critical lane and reassign the slot; or
- keep the barrier serialized until a named observable event.

Do not repeat the scheduling audit until the exact barrier closes or changes. The audit cannot
invent product scope, waive an invariant, lower a gate, or create a report.

### Oversize authority work

Before dispatch, decompose a candidate expected to exceed roughly 1,500 handwritten changed lines
or to modify more than one independent authority or security domain. An exception requires a
written reason in the task transition explaining why separation would break one atomic invariant.
Generated files and mechanically formatted output do not determine the threshold.

## Finite review protocol

Review is a state machine, not an open-ended request to find anything else:

```text
PREFLIGHT
  -> C0 broad grouped review
  -> U0 union and adjudication
  -> C1 consolidated remediation
  -> R1 closure-scoped review
  -> APPROVED
     or C2 bounded repair / architecture decision
       -> R2 targeted closure review
       -> APPROVED or BLOCKED ESCALATION
```

There is no automatic third remediation loop. After `R2`, a still-open material finding becomes a
bounded architecture decision or a user-authority blocker. Root does not dispatch another broad
review under a new name.

### Finding admission

A review item is a blocking finding only when it contains all of the following:

- the violated canonical requirement or invariant identifier;
- a supported production scenario;
- an exact code, schema, command-output, or evidence anchor;
- a reproducible failure or concrete proof of the violation;
- the material impact; and
- the smallest closure that preserves adjacent contracts.

Otherwise classify it as a `Note`. Style, naming, prose preference, test-count preference, repeated
documentation of existing evidence, and unsupported hypothetical scenarios are Notes unless they
demonstrably violate an admitted invariant. Notes do not block integration or approval.

Use these material severities:

- `Critical`: could authorize an unsafe action, corrupt authority or money, evade a mandatory
  boundary, or make release evidence materially false.
- `Important`: breaks a required product contract, durable recovery, resource bound, correctness
  invariant, or mandatory capability.
- `Minor`: a real bounded requirement violation with low blast radius; it is not cosmetic
  preference.

All admitted findings block the candidate until closure or explicit adjudication as unsupported.

### Review scope

`C0` reviewers inspect the frozen quarter or architecture boundary broadly and return findings only;
they do not edit. `U0` deduplicates all reviewer output into one closure set before any remediation
dispatch. `C1` assigns that consolidated set across disjoint paths where safe.

`R1` and `R2` are limited to:

- the original admitted findings;
- the remediation diff;
- the declared blast radius; and
- material regressions introduced by that remediation.

They are not fresh broad reviews. A remediation-discovered unrelated issue is routed to its owning
canonical task unless it invalidates the current candidate.

### Architecture audit trigger

Run one read-only architecture audit only when:

- the same invariant fails twice in the current finite review cycle;
- multiple admitted findings share one underlying architectural cause; or
- closure requires changing frozen authority, schema, or ownership.

The audit is limited to 60 minutes, creates no committed report, and receives no audit-of-audit. It
must return one of: a bounded repair, slice reset/decomposition, or a specific user-authority
blocker. Root records the decision in the task transition comment.

## Exact-head gates and benchmark protection

Code review, gates, measurements, and approvals bind to one clean exact 40-hex Git SHA. A product
change after a freeze invalidates the affected evidence. A docs-only delivery-control branch cannot
be merged into a frozen benchmark candidate before that candidate's measurement and disposition.

Benchmark candidates move through four explicit states:

- `Prepared`: exact clean pushed candidate and immutable inputs are available.
- `Tool Reviewed`: the candidate, harness, toolchain, fixture, and host controls were independently
  accepted for measurement.
- `Measured`: the unchanged reviewed candidate produced bounded evidence on admitted hardware.
- `Accepted`: evidence met the canonical threshold and the applicable unchanged-head gate/review
  accepted it.

Only `Accepted` releases a benchmark-dependent product barrier. After `Tool Reviewed`, freeze the
candidate and measure at the first eligible exclusive host window. Do not insert root governance or
documentation commits into that candidate.

## Setup and maintenance kill switches

The control surface must remain cheaper than the product work:

- initial GitHub setup is limited to 60 minutes;
- if Project authorization or repository linking is not working within 10 minutes, use the 21
  issues as the temporary live ledger and defer only the visual Project projection;
- a normal state update is limited to two minutes;
- an integration-cut update is limited to ten minutes;
- quarter reconciliation is limited to fifteen minutes; and
- if operation appears to require a tracking script, GraphQL helper, field registry, Action,
  tracking test, prose test, JSON ledger snapshot, or generated report, simplify the operation.

The prohibited helpers above must not be added to the repository. There are no delivery percentages,
ETAs, burn charts, per-finding issues, automated status inference, or parallel GitHub writers.

## Failure and recovery

- If GitHub is unavailable, Git and exact command evidence continue to advance product work. Root
  posts the deferred transition after service returns; no local shadow ledger is created.
- If a Project field contradicts an issue transition, root repairs the field from the issue/Git
  authority.
- If an issue transition contradicts an exact pushed SHA, Git wins and root posts one correcting
  transition.
- If an agent worktree is dirty after handoff, treat it as unresolved owned state. Inspect and
  integrate, preserve, or escalate it; never force-remove it.
- If a clean handed-off worktree is no longer in use, remove it promptly and prune worktree
  metadata.
- If current authority cannot decide a material scope or safety question, mark the owning task
  `Blocked`, name the precise user decision, and stop that lane without blocking disjoint work.

## First-cycle proof

The operating design is accepted only after one real current-quarter cycle demonstrates all of the
following without adding repository tracking machinery:

1. Create and link the private Project without modifying the benchmark-frozen product root.
2. Create all 21 task issues and make the exact current Task 3 and Task 5 candidates visible.
3. Complete one closure-scoped Task 3 re-review using the finite review protocol.
4. Stop competing agents and consume the unchanged root's eligible benchmark window before another
   root governance or documentation integration commit.
5. Integrate accepted Task 3 and Task 5 candidates in dependency-safe order and publish the exact
   integration cut.
6. Remove their clean completed worktrees and prune metadata.
7. Start disjoint Task 2 and Task 4 production lanes concurrently when their canonical barriers are
   satisfied.
8. Start Kraken only after Task 2 freezes the live-source interface it consumes.
9. Show that issue/Project transitions took less effort than the implementation and created no
   script, generated ledger, tracking test, or extra review loop.

At the audit anchor, Task 3 had local candidate `63c2578` plus integration-owner manifest, lockfile,
and migration work still to reconcile; Task 5 had clean local candidate `1395e23`. Neither was an
origin-backed integrated product candidate. These are bootstrap observations, not permanent status
authority; the first GitHub transition must verify and record their full SHAs and current trees.

## Implementation boundary

After this written specification is reviewed, the implementation plan must sequence GitHub setup,
issue population, current-state reconciliation, finite Task 3 closure review, benchmark protection,
Task 3/Task 5 integration, worktree cleanup, and the next disjoint production dispatch. GitHub
Project access requires explicit user authorization if the current token lacks the `project` scope.

The implementation must not change product code merely to support this operating model. Success is
faster dependency-releasing integration with intact requirements and finite review—not a more
elaborate tracker.

[canonical-plan]: ../plans/2026-07-17-market-squawk-usable-complete-release.md
