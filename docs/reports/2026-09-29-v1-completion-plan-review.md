# V1 completion plans — independent review

Date: 2026-09-29. Source audit base: `523da3b94826eaa47cba59b0bff7e85f5b2e09c7`.
**Approved:** all four reviewers confirmed the corrected plan hashes, with zero unresolved valid
findings. Product implementation remains paused. This report concerns planning quality, not product approval,
live verification or permission to resume engineering.

## Reviewed scope and independence

Three author assessments informed the lead's plans: provider/data and Desktop authors used GPT-6
Sol High; financial analysis used GPT-6 Astra High. Four different, fresh GPT-6 Astra High agents
then independently reviewed disjoint specialties with `fork_turns="none"`. None authored the
plans or changed them. The lead applied the corrections; the reviewers checked closure.

Each reviewer read the owner-test contract, binding project instructions and both complete plans,
then inspected the relevant current source. No build, test, provider call, installed journey or
resource measurement was performed. No product file or registered goal changed, and no branch or worktree was created.
This owner-requested planning review does not add or restart delivery-quarter reviews.

| Reviewer | Independent scope | Final result |
| --- | --- | --- |
| `/root/review_plan_data` — GPT-6 Astra High | Selected provider families, data readiness, acquisition/publication/selectors and consumer authority | Approved; no findings |
| `/root/review_plan_finance` — GPT-6 Astra High | Publication correctness, financial/model families, harmonics, valuation and backtest evidence | Approved; findings closed |
| `/root/review_plan_clients` — GPT-6 Astra High | 17 screens, lookup contract, Desktop/CLI/MCP workflows, demand reads and saved recovery | Approved; findings closed |
| `/root/review_plan_execution` — GPT-6 Astra High | DAG, file ownership, coherent commits, thin checks, quarter mapping and final acceptance | Approved; findings closed |

## Findings and corrections

Initial review found no Critical findings, two Important findings and two Minor findings. The
provider/data reviewer found no substantiated issue. Severity did not waive any finding.

| Finding | Evidence and consequence | Correction verified and closed |
| --- | --- | --- |
| C1 — Important | `apps/market-squawk-desktop/src/features/portfolio/portfolio-page.tsx` always renders detail unavailable and passes null scenario/planning choices; `portfolio-history.tsx` is static. This is missing composition, not merely an unproven journey. | Explicit Wave 2 task for selected-account holdings/cash/performance/exposure/risk, history/attribution and admitted scenario/position/rebalance choices. Reuse financial authorities; root owns shared projections, Desktop owns rendering. Require actual available evidence and reopened results. |
| C2 — Important | `apps/market-squawk/src/cli.rs` has no saved-chart command; `local_product/cli_transport.rs` maps Show only to `Decision.GetInvestmentAnalysis`, which returns no chart payload. | Explicit Wave 4 thin CLI adapter for existing `Decision.GetInvestmentChart`, with saved action, layer, exact window and point limit. Shared structured evidence and restart proof; no renderer or new chart service. Wave 1 clearly claims brief readback only. |
| FIN-1 — Minor | `workflow_driver.rs::publication_arguments` and `investment_request.rs::validate_canonical_request` have two independent reference-admission predicates. The original test brief omitted forecast present with final market absent. | Add that case within the same critical regression: preserve original chart action evidence, clear current-share evidence, verify binding/request agreement and actual admission. No new suite or validator relaxation. |
| EX-1 — Minor | Binding project memory requires every Stage/Wave to map to an existing delivery quarter; the draft did not explicitly map Waves 0–5. | Map every wave to existing Quarter 4 of 4, preserving earlier history and final grouped review/remediation. No extra quarter or per-wave review. |

All four reviewers re-read the corrected plans and verified the final hashes below. Each confirmed
no remaining substantiated finding in their scope; the three reviewers with findings explicitly
closed them. Original findings and closure responses are preserved in the session and local review
artifacts; this tracked report contains the complete actionable dispositions and identities.

The lead independently inspected the cited Portfolio, CLI, publication/admission and quarter-policy
seams before accepting these findings. No finding was dismissed, deferred or waived.

## Exact plan identities

The initial reviewed drafts had these SHA-256 values:

| Document | Initial SHA-256 |
| --- | --- |
| `docs/plans/v1-workflow-completion-plan.md` | `02019013669405c492236aa01fd99eacdeb1a6d9736869b065f67bfe0ea53199` |
| `docs/plans/v1-first-stock-wave.md` | `8927c4b3f15ee0689d4cfd1217f8e1a2535db2ecc5c462f51efa192fc54026f0` |

Final approved plan contents:

| Document | Corrected SHA-256 |
| --- | --- |
| [Complete V1 coverage and sequence](../plans/v1-workflow-completion-plan.md) | `f480c59ce30619196ee1f8cf9460fc7f8dc6206d1b439648b677a0be07402625` |
| [First-stock execution wave](../plans/v1-first-stock-wave.md) | `4eb6eab75a2121b784dfe148e32d573b6e7094f185a58e200484dc2822895b47` |

Approval applies only to the identified plan contents and assigned review scopes. Any substantive
plan/source change requires a scoped refresh of affected dependencies and evidence; it does not
justify restarting an unchanged repository-wide audit. Source inspection confirms proposed
boundaries, not actual credentials, model outputs, live data or installed restart success.

## Handoff boundary

The complete plan preserves all seven owner-test acceptance items, every selected provider and
model family, all 17 screens and the four required native-platform owner packages. The first-stock
wave is a bounded first implementation milestone, not a smaller substitute for V1.

On later explicit resume, use the same feature branch and primary worktree. S0 refreshes source and
inputs; provider P1 and financial F1 can proceed independently; root R1 establishes the admitted
lookup contract before Desktop U1 navigation. R1/U1 producer/schema/consumer changes land together.
Root integrates and schedules focused verification; the full gate and whole-app measurement follow
complete workflows. Current assignments and pushed checkpoints live in the delivery ledger.

After this documentation checkpoint is committed and pushed, remain paused for owner review.
