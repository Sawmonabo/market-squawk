# Market Squawk V1 — complete working workflows for owner testing

Owner-approved goal alignment: 2026-09-29.

This is the approved scope and acceptance contract. Current execution status, completed checkpoints
and next assignments live in the [delivery ledger](delivery-ledger.md). Updating this document does
not resume execution; explicit owner pauses and later resume instructions control.

For execution after explicit resume, follow the reviewed
[workflow completion plan](v1-workflow-completion-plan.md) for remaining outcomes and dependencies,
then the [first-stock wave](v1-first-stock-wave.md) for initial tasks and file ownership.
The [independent review record](../reports/2026-09-29-v1-completion-plan-review.md) records approval
and finding closure. These plans implement this goal; they do not replace or narrow its acceptance
criteria. The delivery ledger remains the authority for current assignments and evidence.

## Objective and scope

Complete the existing V1 feature branch as a fully working, installed application for owner testing
in the current repository. A person without financial expertise must be able to find and analyze
investments, understand potential returns and risks, inspect evidence-backed entry/exit ranges, save
results and use explicitly virtual paper trading. Estimates must communicate uncertainty, never
guaranteed profits.

Deliver every agreed provider and capability through the actual default and Advanced Desktop
workflows, CLI and MCP. The complete flow is:

**Durable normalized enriched data → provider-neutral selectors →
reference/fundamental/macro/options/history consumers → features, forecasting, financial modeling,
causal harmonics, valuation and realistic out-of-sample backtests → actionable investment ranges,
persisted recommendations, portfolio/risk and virtual paper → installed shutdown/restart.**

The primary finish line is complete working behavior, not an open-ended claim of “production-ready”
infrastructure. Adapters, scaffolding, disconnected calculators, mock screens and isolated tests do
not establish a completed product journey. Preserve the complete agreed feature scope; do not
silently substitute a smaller demonstration for it.

Use the [market-data provider architecture](../architecture/market-data-provider-architecture.md),
[project memory](../project-memory.md) and [delivery ledger](delivery-ledger.md) to locate the
agreed requirements and evidence. The earlier local handoff is historical recovery evidence, not
required to understand this contract. This alignment and later owner instructions supersede
conflicting historical execution, model-selection, resource-target or stopping rules. Historical
plans and audits do not independently expand the goal. Existing mandatory provider, asset,
model-family and platform requirements remain in scope.

## Finite functional acceptance checklist

1. **Providers and durable consumers:** Every selected provider's agreed data families have
   working acquisition, durable normalized publication, typed selection and their intended
   provider-neutral product consumers. Cover the selected reference, fundamentals/filings,
   macro/rates, market/history, options, funds/NAV, crypto and enrichment workflows. Disabled
   optional connections and unsupported external coverage remain truthful; their required
   implementation still ships. A provider health light or adapter test alone does not pass.

2. **Complete investment analysis:** A supported stock traverses real selected data through price
   forecasts, all three separately evidenced probabilities (price rising, outperforming the
   selected benchmark, and profit after modeled costs), financial modeling/valuation, causal
   harmonic evidence, realistic chronological out-of-sample backtesting, portfolio/risk context
   and understandable actionable seven-range entry/exit evidence into a persisted Investment
   Brief. Retain assumptions, uncertainty, cutoffs and original evidence. Honest abstention or no
   qualifying pattern is valid; a permanently disconnected capability is not.

3. **Discovery and recommendations:** Find opportunities and ad-hoc analysis work through the
   default workflow without manual pipeline operation. Preserve ranking, complete/excluded
   coverage, reasons, currentness, expiry and saved recommendation/outcome evidence. One
   successful stock journey does not waive remaining provider, asset or required model-family
   coverage.

4. **Portfolio, risk and virtual paper:** Supported account/asset workflows, positions, cash,
   corporate actions, candidate impact and risk operate over the shared evidence. Explicitly
   requested or configured virtual paper uses fresh checks, realistic fills/costs and durable
   accounting/reconciliation. No real brokerage trading or money-movement authority is introduced.

5. **Complete Desktop:** Every agreed Everyday and Advanced screen operates its real capabilities
   in the approved Obsidian Signal design. Include configurable benchmark comparison, the three
   distinct probabilities, hover/accessibility equivalents, selectable forecast/range/action
   layers and causal harmonic geometry, confirmation dates, invalidation and original evidence.
   Exact saved analysis remains available after restart. React renders backend-authoritative
   values; ordinary screens are provider-neutral and free of engineering language, with provider
   naming confined to settings/logs.

6. **Shared clients and workflow control:** Desktop, CLI and MCP expose the agreed
   operations/results through one shared installed service and storage root, including concurrent
   clients. Selection, demand loading, cursor navigation, cancellation and recovery work without
   duplicated financial logic or runtimes. Closing a detail read does not silently cancel an
   independent durable job.

7. **Installed lifecycle and recovery:** Installation and managed runtime setup, service start,
   native onboarding, optional application locking, saved credential reuse/OAuth where required, real workflows, clean
   shutdown/restart, saved-result reopening, backup/restore and stale-credential rejection/fresh
   reconnect work. Missing or expired provider credentials produce recoverable connection states
   and do not block ordinary application launch. The owner does not need a developer
   Python/runtime setup.
   Application locking is opt-in: configured connections reopen without an app password across
   launches/rebuilds. Optional locking supports remembered OS-secured access, explicit Lock/Forget
   and a user-selected reauthentication interval. Provider expiry/key replacement remains separate.

Track each item as missing implementation, implemented but unproven, critically verified, live
verified or installed-workflow complete, with evidence and explicit remaining gaps. Do not infer
live or installed completion from compilation or fixtures.

## Scope control and anti-churn rule

Before starting additional engineering work, identify **the acceptance item it closes, the concrete
failure or missing behavior, and the smallest complete fix**. If that connection cannot be stated,
do not make the work a feature-branch completion blocker.

Infrastructure, refactoring, optimization and hardening belong in this goal only when necessary for
an agreed workflow, financial correctness, credential/data protection, reliable
persistence/recovery, or remediation of an existing substantiated finding. Necessary architectural
changes remain allowed. Prefer the better cohesive design when it resolves a demonstrated problem,
updating affected consumers together.

Do not expand work through speculative threats, hypothetical future scale, general cleanup,
preferred abstractions, new frameworks or stricter self-imposed standards. Do not add arbitrary
startup, data, model or history restrictions to claim success. Preserve legitimate integrity and
resource-exhaustion safeguards with a concrete rationale. A broken financial result, exposed
credential, lost saved result or other substantiated acceptance defect must be fixed; it cannot be
relabeled as optional hardening.

Keep existing required review findings and platform proofs visible. This alignment does not waive
them. New publication-specific assurance beyond the agreed owner-test contract is separate scope
requiring owner direction; it must not become an implicit prerequisite for every ordinary feature
change.

## Delivery sequence and integration ownership

Model routing correction: all future Sol assignments use **GPT-6.1 Sol High**
(`gpt-6.1-sol`, High effort), including references inherited from older goals, plans and handoffs.
**GPT-6 Astra High** (`gpt-6-astra`) retains its existing financial/advanced task assignments.
This substitution does not resume implementation or change any other delivery rule.

On explicit resumption, perform a short refresh of the acceptance map against the actual branch.
Locate existing behavior before assigning work; do not restart an infrastructure audit or rebuild
completed capabilities.

Organize delivery around: (1) the first complete stock analysis and saved-result restart; (2)
discovery, recommendations, portfolio/risk and virtual paper; (3) remaining provider and asset-family
coverage; (4) complete Desktop/CLI/MCP journeys; (5) installed owner-test delivery and final
verification. These are dependency milestones, not giant commits or new review quarters. Keep
provider work and product completion moving concurrently; do not wait for every supplementary source
before proving the first stock journey, and do not drop those sources from final acceptance.

- Use `feature/v1-installed-product-experience` and its current primary worktree. Create no additional branches or worktrees without explicit owner approval. Keep main/release unchanged.
- Maintain one current execution table in `docs/plans/delivery-ledger.md`: concrete outcome, dependencies, exact file ownership, required critical evidence, blocker/next dependency and pushed commit. Historical handoffs are evidence, not competing task queues.
- Start with provider integration, financial workflow and Desktop implementation lanes. Add parallel work only when dependencies and disjoint ownership support it. Use bounded briefs with `fork_turns="none"`. Use GPT-6 Astra High for advanced forecasting/modeling, harmonics, financial algorithms, backtesting, difficult debugging and advanced optimization; GPT-6.1 Sol High for other tasks.
- The lead owns shared authority/contracts, schemas, manifests, lockfiles, application composition, shared transport, integration acceptance, Git and build/test scheduling. Helpers may trace wiring or prepare assigned changes; they do not create branches/worktrees, run competing builds or independently change shared files.
- Assign each agent a finishable outcome, explicit files, dependencies and the smallest relevant critical check. Inspect actual changes and affected consumers; a completion message is not proof. If a lane stalls, identify the concrete dependency and split genuinely independent work rather than repeatedly replacing or restarting it.
- Integrate producer and consumer changes together. Run the relevant critical checks, commit and push the coherent checkpoint, update delivery/PR evidence, and release ownership before accumulating replacement work. Do not hold an independently coherent result behind unrelated work or grow another unnecessarily broad uncommitted batch.
- Preserve unrelated WIP, accepted evidence, the original session and recovery backups. Never discard or hide changes to manufacture cleanliness. Remove any subsequently approved temporary branch/worktree only after its work is proved integrated or otherwise explicitly preserved.

## Engineering and verification

This is greenfield V1. Change active implementations, schemas, bindings, configuration and
documentation in place; remove superseded paths. Add no backward-compatibility layer, migration
program or duplicate old/new stack.

Use ripgrep and read relevant implementations/contracts. Trace callers, ownership, persistence,
cancellation/shutdown, error handling and Desktop/CLI/MCP consumers. Preserve backend financial
authority and local Rust/Tauri workflow ownership. Organize code into cohesive feature/domain
modules; reuse shared logic without catch-all utilities, oversized multipurpose files, needless
wrappers or speculative abstractions.

Reuse existing components and maintained dependencies. Research unfamiliar technical failures before
inventing infrastructure, but do not turn research into framework, dependency-upgrade or
documentation churn. Material scope, cost or authority changes require owner direction.

Reuse existing critical verification. Add or extend a test only for an otherwise uncovered critical
failure and briefly identify the gap. No routine component-test expansion, broad matrices, duplicate
harnesses or prose tests. Deterministic tests must not depend on live credentials; authorized
network checks report their separate setup/entitlement state honestly.

Serialize local compilation with one compiler job and nonincremental agent builds. Do not run CI/CD
or release builds after ordinary tasks. Preserve the existing four-quarter review policy, grouped
reviews and required remediation; do not restart the sequence, invent additional quarters or add
per-task review ceremonies. A timeout or round count is not approval. Reserve the broad gate for the
unchanged final candidate, correcting real failures and rerunning affected verification as
necessary.

## Resource efficiency and final owner-test handoff

Use streaming, batching, indexed/disk-backed processing, demand loading, cursor pagination, bounded
resident caches and shared services. Avoid unnecessary polling, whole-history materialization,
duplicate state/model runtimes and repeated computation. Do not sacrifice capability, evidence
completeness or financial correctness to reduce reported consumption.

The complete single-user application's memory objective is **500 MB–1.5 GB, with a 2 GB maximum**.
Lower consumption is welcome; 500 MB is not a minimum. This is a measured objective, not a runtime
admission rule. **Defer whole-app RAM measurement until the complete application workflows are
ready.** Then measure Desktop/WebView, shared service and active analytical/model/Python helpers
together under representative complete workflows. Optimize demonstrated avoidable consumption first;
obtain owner approval before changing the target. Do not invent hardware or numeric responsiveness
requirements.

Report functional completion separately from release readiness. Functional acceptance comes first;
final whole-app measurements, consolidated verification, applicable grouped review/remediation and
the agreed four native-platform owner-test package proofs (Linux x64, Windows x64, macOS Intel and
macOS Apple Silicon) follow. These final obligations remain required for goal completion but must
not repeatedly interrupt ordinary feature implementation.

Finish with verified owner-test packages, runnable instructions, synchronized acceptance evidence
and updated PR #43. Report concrete outcomes, pushed commits, active owners, actual verification,
blockers and the next dependency. After explicit resumption, continue through safe authorized work
without reopening settled permissions. Stop before public publication or merging into main/release.
Mark the goal complete only when the finite functional checklist and the agreed final owner-test
obligations are supported by evidence; do not claim public-release approval from feature completion.
