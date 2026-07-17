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

Identity/account rotation to evade limits, browser or TLS fingerprint spoofing, CAPTCHA or
anti-bot bypass, proxy rotation intended to defeat blocking, distributed quota evasion, stealth
scraping, and access-control circumvention are permanently prohibited. Provider constraints are
handled through authorized identities, shared authoritative budgets, persistence, caching,
backoff, source health, failover, and explicit coverage metadata.

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

If one lane remains the critical path across two consecutive status updates or one complete
verification/review cycle, the integration owner must perform a scheduling audit:

1. distinguish implementation, remediation, verification, review, and integration work;
2. split any newly independent work into disjoint lanes;
3. stop work that is not required for the user's current deliverable;
4. explain the remaining serialization dependency concretely; and
5. report the next event that releases the barrier.

Do not describe hours of checkpoint remediation, audit, and plan work merely as being "on Task 7"
or another historical task label.

## Review policy

Fresh independent specialist reviews are grouped at quarter checkpoints, not repeated after every
ordinary task. Lane workers still perform TDD, self-review, focused verification, and blast-radius
inspection before handoff.

At a quarter checkpoint:

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

Every substantiated Critical, Important, and Minor finding blocks checkpoint approval until it is
fixed or retracted with specific contrary evidence. Severity determines order, not whether a known
defect may remain.

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

