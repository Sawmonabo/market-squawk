# Market Squawk Documentation Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved GitHub-native Market Squawk documentation portal with preserved
history and architecture, operations, and reference content derived from one exact accepted product
head.

**Architecture:** A single integration owner first refreshes the plan against the accepted
integrated head. Three grouped architecture lanes reconcile current pages while the target-state
source remains in place; only then does the integration owner perform the architecture-audit moves.
Three grouped reference lanes and three grouped operations lanes work on disjoint files; the
integration owner alone owns history-bearing moves, indexes, root navigation, mutable delivery
state, final reconciliation, publication, and cleanup.

**Tech Stack:** GitHub-flavored Markdown, stable Mermaid `flowchart`, `sequenceDiagram`,
`stateDiagram-v2`, and `erDiagram`, Git, `rg`, and existing source/release evidence. This is
documentation-only work and adds no runtime dependency, generator, test, or checker.

## Document control

| Field | Value |
| --- | --- |
| Plan status | Ready for accepted-head refresh |
| Planning audit base | `46f86d9496287e1995f584537153ecb3fcb271ac` |
| Audit-base meaning | Evidence anchor only; not implementation or release approval |
| Approved design | `docs/superpowers/specs/2026-07-22-market-squawk-documentation-system-design.md` |
| Approved design blob | `7fdb58ece5b41211493cd4026773974ff30ce240` |
| Release boundary | Required for the first complete local `v0.1.0` release |
| Delivery-state authority | `docs/plans/delivery-ledger.md` |

The product remains under active integration. Task 1 is therefore a hard barrier: before any
documentation writer starts, the release owner must select one clean accepted integrated head and
refresh exact paths and line anchors, CLI/MCP/configuration schemas and interfaces, dependency
relationships, source metadata, runnable evidence, and release blockers.

## Global constraints

- Read `AGENTS.md`, `docs/project-memory.md`, and the approved design before execution.
- Preserve the approved tree and page responsibilities. A material design change requires user
  approval.
- The root README owns concise `Runnable now`, `Required but missing`, and `Release blocked until
  implemented` truth. The delivery ledger alone owns mutable integration state.
- Operations document only runnable procedures. If the accepted head does not expose a real
  command/service/recovery path, stop that lane and return the product blocker; do not publish
  aspiration.
- Reference pages describe exact current types, schemas, defaults, bounds, errors, and authority.
  Architecture may discuss an incomplete binding invariant only when it labels and links the
  blocker.
- Every current page records document type, audience, status, substantive review date, and exact
  reviewed commit. Long pages add a linked contents list.
- Every substantive page states purpose, scope/non-goals, relevant flow, responsibilities and
  invariants, failure/recovery, authority/security considerations, related pages/code/evidence, and
  direct relevant sources with review dates.
- Every operations page additionally contains prerequisites/platforms, pre-mutation authority
  checks, exact procedure, expected success evidence, rollback/recovery, bounded diagnosis, relevant
  local locations, and factual reference links.
- Use direct official sources for external claims. An external source is not evidence that local
  runtime behavior exists.
- Use only stable Mermaid forms named above. Each diagram states its question, keeps one abstraction
  level, labels material relationships, avoids color-only meaning, and has equivalent explanatory
  prose.
- Perform the model-runbook Git move before its substantive rewrite. Reconcile the two architecture
  sources into current pages before archiving them, then verify useful `git log --follow` ancestry
  for all three history-bearing moves.
- Do not add redirect-only files, empty pages/directories, content-free indexes, fictional
  procedures, a documentation site generator, checker/policy script, prose/snapshot/file-existence
  test, or Rust test target.
- Do not modify Rust, Python, manifests, lockfiles, fixtures, schemas, or build inputs.
- Do not run Cargo for this documentation-only lane. Consume the exact accepted product gate already
  produced by the implementation owner.
- Use product-oriented branches such as `docs/product-documentation`,
  `docs/architecture-guide`, `docs/reference-manual`, and `docs/operator-runbooks`.
- Fresh independent review remains grouped with Quarter 4 of 4. Lanes self-review before handoff;
  do not create one fresh review round per page.
- After integration, promptly remove clean lane worktrees, prune metadata, and delete
  merged/patch-equivalent local and origin lane branches. Preserve dirty or unique state.

## Approved output tree

```text
docs/
├── README.md
├── architecture/
│   ├── README.md
│   ├── overview.md
│   ├── building-blocks.md
│   ├── live-execution-plane.md
│   ├── research-data-plane.md
│   ├── control-plane.md
│   ├── data-time-and-provenance.md
│   ├── security-and-trust-boundaries.md
│   ├── deployment.md
│   ├── quality-attributes.md
│   └── decisions/
│       ├── README.md
│       ├── 0001-separate-live-and-research-planes.md
│       ├── 0002-evidence-derived-execution-quality.md
│       ├── 0003-single-writer-live-state.md
│       ├── 0004-local-analytical-storage-stack.md
│       └── 0005-central-risk-and-execution-authority.md
├── operations/
│   ├── README.md
│   ├── installation-and-bootstrap.md
│   ├── configuration-and-secrets.md
│   ├── source-operations.md
│   ├── research-ingestion.md
│   ├── datasets-and-query.md
│   ├── model-inference.md
│   ├── portfolio-and-paper-execution.md
│   ├── backup-and-recovery.md
│   └── troubleshooting.md
├── reference/
│   ├── README.md
│   ├── cli.md
│   ├── configuration.md
│   ├── mcp.md
│   ├── source-coverage.md
│   ├── data-quality.md
│   └── time-and-provenance.md
└── audits/
    └── architecture/
        ├── 2026-07-15-current-state-anchor.md
        └── 2026-07-16-target-state-baseline.md
```

Existing populated `plans/`, `reports/`, `research/`, `testing/`, and `verification/` areas remain
in place. `docs/README.md` routes to them without reorganizing historical evidence.

## Audit-base source map

Task 1 must refresh this mapping to accepted-head paths and exact line ranges. The absence of
`apps/market-squawk/src/application.rs` and `apps/market-squawk/src/cli.rs` at the audit base is
evidence of active Task 19 work, not permission to document the older control plane.

| Subject | Planning-audit source |
| --- | --- |
| Capability truth/blockers | `README.md:9-191`; `docs/plans/delivery-ledger.md:9-276` |
| Historical/target architecture | `docs/architecture/current-state.md:1-353`; `docs/architecture/target-state.md:14-778` |
| Workspace/dependencies | `Cargo.toml:1-189`, all package manifests, `rust-toolchain.toml`, `.cargo/config.toml` |
| CLI/application/MCP | `apps/market-squawk/src/main.rs:32-908`; `apps/market-squawk/src/mcp.rs`; `apps/market-squawk/src/mcp/services.rs:64-207`; `crates/market-squawk-mcp/src/` |
| Configuration/paths/secrets | `crates/market-squawk-platform/src/config.rs:230-559`, `paths.rs:71-300`, `secrets.rs:23-101` |
| Classification/time/provenance | `crates/market-squawk-domain/src/classification.rs:33-75`, `time.rs`, `provenance.rs`, `provenance/`, `research/` |
| Sources/live | `crates/market-squawk-sources/src/metadata.rs:25-430`, `health.rs:19-620`, registry/policy/extraction modules; `crates/market-squawk-live/src/` |
| Research/PIT/storage | `crates/market-squawk-data/src/catalog/`, `manifest.rs`, `parquet_store/`, `query.rs:88-430`, `pit/`, `dataset_builder/`, `python_dataset/` |
| Analytics/modeling | `crates/market-squawk-analytics/src/`; `crates/market-squawk-modeling/src/bundle.rs:44-520`, `registry.rs`, `native.rs`, `onnx.rs:39-300`, `onnx/` |
| Portfolio/execution/fair value | `crates/market-squawk-portfolio/src/`; `crates/market-squawk-execution/src/`; `crates/market-squawk-valuation/src/` |
| Provider coverage | Each `adapters/market-squawk-adapter-*/src/lib.rs` and source/config/rights modules; `docs/research/providers/`; `docs/research/2026-07-22-zero-fee-provider-onboarding/` |
| Model runbook/release evidence | `docs/operations/onnx-runtime.md:1-146`; accepted release-gate and `docs/verification/` evidence |

## Dependency and ownership schedule

No writer starts before Task 1. No parallel lane starts before Task 2 is integrated. Architecture
and reference lanes may overlap when capacity permits because their ownership is disjoint, but the
architecture-audit moves wait for all three architecture lanes.

| Wave | Lane | Exclusive files | Dependencies | Merge order |
| --- | --- | --- | --- | --- |
| Barrier | Task 1 refresh | This plan, delivery ledger | Approved design | First |
| Preparation | Task 2 move inventory | Model runbook move and architecture-link inventory | Task 1 | Second |
| A1 | Context/deployment architecture | `overview.md`, `building-blocks.md`, `deployment.md`, `quality-attributes.md` | Task 2 | A1 |
| A2 | Runtime-plane architecture | `live-execution-plane.md`, `research-data-plane.md`, `control-plane.md` | Task 2 | A2 |
| A3 | Semantics/trust/ADRs | Two architecture pages and five ADRs | Task 2 | A3 |
| A4 | Architecture audit archive | Two history-bearing architecture moves and maintained-link repairs | A1-A3 | After A3 |
| B1 | CLI/config reference | `reference/cli.md`, `reference/configuration.md` | Task 2 | B1 |
| B2 | MCP/source reference | `reference/mcp.md`, `reference/source-coverage.md` | Task 2 | B2 |
| B3 | Quality/time reference | `reference/data-quality.md`, `reference/time-and-provenance.md` | Task 2 | B3 |
| C1 | Bootstrap/config/source operations | Three operations pages | A4 and Wave B | C1 |
| C2 | Research/dataset/model operations | Two new pages and moved model runbook | A4 and Wave B | C2 |
| C3 | Portfolio/recovery/troubleshooting | Three operations pages | A4 and Wave B | C3 |
| Integration | Task 6 portal/truth | All indexes, root README, ledger, cross-links | Waves A-C | Last content commit |

The integration owner reserves `README.md`, all five new indexes, the delivery ledger, this plan,
the approved design, moved files until assigned, cross-lane conflict resolution, and all GitHub/
cleanup state.

---

### Task 1: Freeze and refresh documentation authority

**Files:**

- Modify:
  `docs/superpowers/plans/2026-07-23-market-squawk-documentation-migration.md`
- Modify: `docs/plans/delivery-ledger.md`
- Inspect only: all audit-base source-map paths and accepted release evidence

**Produces:** one committed documentation base with exact accepted source head, refreshed anchors,
runnable capabilities, blockers, and lane ownership.

- [ ] **Freeze one clean accepted integrated product head**

Create `docs/product-documentation` from the exact accepted release commit, then run:

```bash
git branch --show-current
git status --short
git rev-parse HEAD
git show -s --format='%H %T %cI %s' HEAD
git hash-object \
  docs/superpowers/specs/2026-07-22-market-squawk-documentation-system-design.md
```

Expected: clean worktree, exact accepted product head, and design blob
`7fdb58ece5b41211493cd4026773974ff30ce240`. Stop if any condition fails.

- [ ] **Refresh all factual anchors**

Use focused source inspection:

```bash
rg -n 'derive\\(.*Parser|enum Command|ToolDescriptor|ToolContract|ToolServices|async fn call' \
  apps/market-squawk/src crates/market-squawk-services/src crates/market-squawk-mcp/src
rg -n 'AppConfig|ConfigOrigin|ConfigSetting|MARKET_SQUAWK_|ArtifactRoot|SecretStore' \
  crates/market-squawk-platform/src
rg -n 'FairValueHierarchy|MarketDepth|DataQuality|available_at|published_at|superseded_at' \
  crates/market-squawk-domain/src crates/market-squawk-data/src
rg -n 'DirectVerified|Quarantined|checksum|sequence|ShardRouter|RiskService|ExecutionDispatcher' \
  crates/market-squawk-live/src crates/market-squawk-execution/src
rg -n 'DatasetManifest|PointInTime|QueryLimits|ModelBundle|InferenceBackend|Backtest' \
  crates apps/market-squawk/src
rg -n 'SourceMetadata|Coverage|Authorization|DirectUnverified' \
  crates/market-squawk-sources/src adapters/*/src
```

Replace audit-base entries with exact accepted paths/line ranges. Identify producer and terminal
consumer for every runnable claim. Retain the planning audit base; add exact `Execution source head`
and `Refresh commit` rows.

- [ ] **Reconcile runnable truth and blockers**

Inspect README status sections, ledger, accepted verification, and GitHub:

```bash
sed -n '/^## Runnable now/,/^## Required but missing/p' README.md
sed -n '/^## Required but missing/,/^## Release blocked until implemented/p' README.md
sed -n '1,300p' docs/plans/delivery-ledger.md
gh issue list --repo Sawmonabo/market-squawk --state open --limit 100
gh project item-list 5 --owner Sawmonabo --format json
```

Any code/evidence/README/ledger disagreement blocks writing; return it to the product owner rather
than resolving it through prose.

- [ ] **Record, verify, and commit the barrier**

Update the ledger with accepted source head, design blob, refresh outcome, allowed runnable scope,
blockers, ownership, and Task 2 as the next barrier.

```bash
git diff --check -- \
  docs/superpowers/plans/2026-07-23-market-squawk-documentation-migration.md \
  docs/plans/delivery-ledger.md
git status --short
git add \
  docs/superpowers/plans/2026-07-23-market-squawk-documentation-migration.md \
  docs/plans/delivery-ledger.md
git commit -m "docs(plan): refresh documentation migration authority"
git push -u origin docs/product-documentation
```

No lane branches before this commit.

---

### Task 2: Inventory architecture links and preserve the model runbook

**Files:**

- Retain as architecture source authority until Task 3 completes:
  `docs/architecture/current-state.md`, `docs/architecture/target-state.md`
- Move: `docs/operations/onnx-runtime.md` → `docs/operations/model-inference.md`
- Modify for the model-runbook move only: maintained incoming links identified by the inventory
- Record for Task 3: maintained links to the two architecture source documents

- [ ] **Inventory and classify old-path occurrences**

```bash
git grep -n -E \
  '(architecture/(current-state|target-state)|operations/onnx-runtime)\\.md|current-state\\.md|target-state\\.md|onnx-runtime\\.md' \
  HEAD -- '*.md'
```

Separate maintained Markdown links from frozen literal paths, historical commands, and migration
descriptions. Record the maintained architecture links for Task 3, but do not move or rewrite the
architecture source documents in this task. Do not rewrite historical literals.

- [ ] **Move the model runbook and repair only its maintained links**

```bash
git mv docs/operations/onnx-runtime.md docs/operations/model-inference.md
```

Update only maintained incoming links to the model runbook. Leave architecture links unchanged for
the writers and integration owner in Task 3. Leave
`docs/verification/usable-release-baseline.md:29` and historical literal source paths unchanged. Do
not leave the old model-runbook path in place.

- [ ] **Verify, commit, and prove ancestry**

```bash
git diff --check
git grep -n -E \
  '\\]\\([^)]*operations/onnx-runtime\\.md[^)]*\\)' \
  -- '*.md'
git add -A -- README.md docs
git commit -m "docs: preserve model runbook history"
git log --follow --oneline -- docs/operations/model-inference.md
git push origin docs/product-documentation
```

Expected: no maintained old model-runbook link, useful pre-move history for that file, and both
architecture source documents still present. This commit is the common base for Waves A and B.

---

### Task 3: Execute the grouped architecture wave

**Files and required content:**

| Lane | Files | Required content |
| --- | --- | --- |
| A1 | `docs/architecture/overview.md`, `building-blocks.md`, `deployment.md`, `quality-attributes.md` | System context and runtime-container flowcharts; constraints; cohesive crate/adapter boundaries and dependency direction; hot-path exclusions; local process/on-disk deployment; measurable quality scenarios without unmeasured performance claims |
| A2 | `docs/architecture/live-execution-plane.md`, `research-data-plane.md`, `control-plane.md` | Live authority sequence and integrity state machine; research extraction/Arrow/publication/Parquet/DataFusion/Python flow; shared CLI/MCP application-service control flow, cancellation, bounds, artifacts, audit, CLI-only SQL, and central risk |
| A3 | `docs/architecture/data-time-and-provenance.md`, `security-and-trust-boundaries.md`, five ADR files | Point-in-time sequence and provenance ER diagram; credentials/provider/parser/model/portfolio/fair-value/risk/MCP trust boundaries; five accepted decisions exactly specified by the approved design |

**Produces:** complete architecture content except indexes.

- [ ] **Dispatch three disjoint lanes from the Task 2 commit**

Each lane follows the global page contract and derives claims from Task 1 anchors. It may read other
lanes but must edit only its assigned files. The integration owner does not edit lane files while
workers are active.

- [ ] **Require the architecture invariants**

Across the wave, ensure:

- independent live/research planes over shared domain contracts;
- evidence-derived execution quality separate from fair value and market depth;
- deterministic single-writer live state;
- bounded queues/resources and explicit failure consequences;
- SQLite/Arrow/Parquet/DataFusion ownership;
- point-in-time availability/revision/supersession semantics;
- strategies/models → intent → central risk → execution authority;
- no database, Parquet, Python, MCP, LLM, arbitrary filesystem, or unrelated network work in the
  live event-to-action path.

- [ ] **Self-review and commit one cohesive change per lane**

```bash
git diff --check -- docs/architecture
git diff -- docs/architecture
```

Commit messages:

```text
docs(architecture): explain context deployment and quality
docs(architecture): document the three runtime planes
docs(architecture): record data trust and binding decisions
```

Each handoff includes exact commit and manual source/diagram review evidence; no tests or checker are
added.

- [ ] **Integrate A1, A2, A3 in order and clean each lane**

Verify exclusive ownership, integrate unchanged, record commits in the ledger, push
`docs/product-documentation`, then remove clean worktrees and merged/patch-equivalent branches.

- [ ] **Archive the reconciled architecture baselines after all three lanes are integrated**

The integration owner, not a lane writer, performs the history-bearing moves only after the new
architecture pages have reconciled the durable content from both sources:

```bash
mkdir -p docs/audits/architecture
git mv docs/architecture/current-state.md \
  docs/audits/architecture/2026-07-15-current-state-anchor.md
git mv docs/architecture/target-state.md \
  docs/audits/architecture/2026-07-16-target-state-baseline.md
```

Add prominent metadata to both archived documents stating that they are dated historical evidence,
have no current architecture authority, and link to `docs/architecture/README.md` and
`docs/plans/delivery-ledger.md`. Adjust their relative links for the extra directory depth and repair
maintained incoming architecture links identified in Task 2. Preserve frozen literal paths,
historical commands, and migration descriptions.

```bash
git diff --check -- docs/architecture docs/audits README.md docs/plans
git grep -n -E \
  '\\]\\([^)]*architecture/(current-state|target-state)\\.md[^)]*\\)' \
  -- '*.md'
git add -A -- README.md docs
git commit -m "docs(architecture): archive reconciled architecture baselines"
git log --follow --oneline -- \
  docs/audits/architecture/2026-07-15-current-state-anchor.md
git log --follow --oneline -- \
  docs/audits/architecture/2026-07-16-target-state-baseline.md
git push origin docs/product-documentation
```

Expected: both archives retain useful pre-move ancestry, identify themselves as historical, and no
maintained link grants either archive current authority.

---

### Task 4: Execute the grouped factual-reference wave

**Files and required content:**

| Lane | Files | Required content |
| --- | --- | --- |
| B1 | `docs/reference/cli.md`, `configuration.md` | Exact command hierarchy, parameters/defaults/limits/output/errors/service mapping; exact config keys/types/defaults/precedence/environment mapping/provenance/secret/reload behavior |
| B2 | `docs/reference/mcp.md`, `source-coverage.md` | Exact stdio framing, registered tools/resources, closed schemas, bounds, authorization, cancellation, artifacts/audit/errors; per-adapter source class, authority, coverage, quality ceiling, auth/account requirement, rights and health constraints |
| B3 | `docs/reference/data-quality.md`, `time-and-provenance.md` | Exact quality variants/evidence/transitions/permitted consumers; separate hierarchy/depth types; canonical timestamps, precision, ordering, PIT selection, revisions, supersession, and lineage |

**Produces:** complete factual reference except its index.

- [ ] **Dispatch three disjoint lanes**

Writers derive tables from accepted public types, Clap/application definitions, configuration
parsing, MCP descriptors, source metadata, and accepted bounds—not target prose.

- [ ] **Enforce factual authority**

- B1 lists no command without a real handler and marks DataFusion SQL CLI-only.
- B2 lists no MCP tool without a registered descriptor/handler. Provider account/key requirements
  remain separate from cost. A denial response is retrieval-health evidence, not provider-contract
  evidence.
- B3 uses exact accepted type/field names and never collapses fair-value hierarchy, market depth, and
  data quality.

- [ ] **Self-review and commit one cohesive change per lane**

```bash
git diff --check -- docs/reference
git diff -- docs/reference
```

Commit messages:

```text
docs(reference): document CLI and configuration contracts
docs(reference): document MCP and source coverage
docs(reference): define quality time and provenance
```

- [ ] **Integrate B1, B2, B3 in order and clean each lane**

Verify each table directly against Task 1 sources, integrate unchanged, update the ledger, push,
then clean worktrees/branches.

---

### Task 5: Execute the grouped operations wave

**Files and required outcomes:**

| Lane | Files | Runnable outcomes |
| --- | --- | --- |
| C1 | `docs/operations/installation-and-bootstrap.md`, `configuration-and-secrets.md`, `source-operations.md` | Install/bootstrap/doctor; precedence and opaque-secret lifecycle/redaction; source register/setup/start/status/coverage/health/stop/resynchronize using accepted commands |
| C2 | `docs/operations/research-ingestion.md`, `datasets-and-query.md`, moved `model-inference.md` | Idempotent extraction/ingestion/provenance; manifests/PIT/compaction/bounded CLI query/Python export; preserved and broadened native/tract/optional ONNX bundle operations and recovery |
| C3 | `docs/operations/portfolio-and-paper-execution.md`, `backup-and-recovery.md`, `troubleshooting.md` | Portfolio import/reconciliation/analytics; risk-enforced paper lifecycle; consistent catalog/dataset/journal/model/artifact backup/restore; bounded diagnosis across supported planes |

**Consumes:** completed reference pages and the exact accepted release artifact/evidence.

- [ ] **Dispatch three disjoint operations lanes**

Each writer exercises current procedures against a disposable controlled local instance or exact
accepted fixture/evidence without rebuilding. A missing handler or unverifiable recovery path stops
that lane and returns a product blocker.

- [ ] **Enforce operational safety and truth**

- Mutations use accepted confirmation/authority and describe effect, success evidence, rollback, and
  recovery.
- Source connectivity never implies `DirectVerified`.
- Research procedures preserve rights, availability, revision, lineage, and bounded publication.
- SQL remains read-only and CLI-only.
- Model failures produce no action; the moved ONNX runbook retains its license/admission and
  termination-gated fallback contracts.
- Portfolio/paper procedures preserve accounting/reconciliation/risk authority and never imply live
  broker enablement.
- Backup/recovery never exposes secret values or deletes unique state.

- [ ] **Self-review and commit one cohesive change per lane**

```bash
git diff --check -- docs/operations
git diff -- docs/operations
git log --follow --oneline -- docs/operations/model-inference.md
```

Commit messages:

```text
docs(operations): document bootstrap configuration and sources
docs(operations): document research datasets and models
docs(operations): document portfolio recovery and diagnosis
```

- [ ] **Integrate C1, C2, C3 in order and clean each lane**

Confirm every command exists in reference and every success/failure claim has accepted evidence.
Integrate unchanged, update the ledger, push, then clean worktrees/branches.

---

### Task 6: Integrate portal navigation, cross-links, and capability truth

**Files:**

- Create: `docs/README.md`
- Create: `docs/architecture/README.md`
- Create: `docs/architecture/decisions/README.md`
- Create: `docs/operations/README.md`
- Create: `docs/reference/README.md`
- Modify: `README.md`
- Modify: `docs/plans/delivery-ledger.md`
- Modify maintained pages only for final cross-links/review metadata

- [ ] **Write substantive reader-oriented indexes**

- `docs/README.md` routes by intent to architecture, operations, reference, audits, plans, reports,
  research, testing, and verification; it distinguishes current product docs, historical evidence,
  and delivery state.
- The architecture index contains the approved context-first reading flow, notation/status meaning,
  diagram legend, and links to all pages/ADRs.
- The ADR index records status/date/decision/related architecture.
- Operations routes operator outcomes and safety conventions without duplicating procedures.
- Reference states version/source-of-truth rules and links all factual pages.

- [ ] **Complete cross-links and metadata**

Every maintained page links relevant architecture/operations/reference/ADR/code/evidence and
records the exact reviewed source head/date. No page is reachable only through a circular peer link.
Do not add evidence-area indexes merely for symmetry.

- [ ] **Reconcile README and ledger**

Add the docs portal to root navigation and use archived audit paths. Change capability status only
when Task 1 evidence supports it. Update the ledger with all integrated commits, accepted source
head, remaining blockers, Quarter 4 review state, cleanup disposition, and the exact-head gate as
the next barrier.

- [ ] **Verify and commit the integrated portal**

```bash
git diff --check
git status --short
git diff --stat
git diff -- README.md docs/README.md docs/architecture/README.md \
  docs/architecture/decisions/README.md docs/operations/README.md \
  docs/reference/README.md docs/plans/delivery-ledger.md
git add README.md docs
git commit -m "docs: publish the Market Squawk product portal"
git push origin docs/product-documentation
```

Stage only reviewed documentation changes; if unrelated WIP appears, stop and reconcile ownership
before committing.

---

### Task 7: Freeze, review, publish, and clean the documentation candidate

**Files:** Inspect the complete approved tree, root README, ledger, maintained incoming links, and
accepted implementation sources. No planned edit occurs in this task; any material fix creates a new
candidate and restarts the gate.

- [ ] **Freeze a clean exact candidate**

```bash
git status --short
git rev-parse HEAD
git show -s --format='%H %T %cI %s' HEAD
```

Expected: clean `docs/product-documentation` and one exact committed candidate.

- [ ] **Verify moves and maintained navigation**

```bash
git log --follow --oneline -- \
  docs/audits/architecture/2026-07-15-current-state-anchor.md
git log --follow --oneline -- \
  docs/audits/architecture/2026-07-16-target-state-baseline.md
git log --follow --oneline -- docs/operations/model-inference.md
git grep -n -E \
  '\\]\\([^)]*(architecture/(current-state|target-state)|operations/onnx-runtime)\\.md[^)]*\\)' \
  -- '*.md'
```

Expected: useful pre-move ancestry and no maintained old-path link. Historical literals and the
approved migration description remain historical.

- [ ] **Perform the bounded content gate**

Manually follow every link from the root and four section indexes. At the frozen head compare:

- architecture against accepted boundaries and evidence;
- commands/configuration/MCP/source/quality/time reference against implementation;
- operations against actual handlers, success/failure evidence, and recovery;
- README/ledger/blockers against accepted code and GitHub state;
- sources for direct relevance and review date;
- Mermaid syntax/abstraction/labels and equivalent prose.

Inspect Mermaid blocks in GitHub-rendered Markdown after push. Do not add a checker or prose test.

- [ ] **Run repository hygiene only**

```bash
documentation_base_sha=$(
  git log --first-parent --format='%H' \
    --grep='^docs(plan): refresh documentation migration authority$' -n 1
)
git diff --check "${documentation_base_sha}^..HEAD"
git diff --stat "${documentation_base_sha}^..HEAD"
git status --short
```

Do not run Cargo. The product owner runs the normal exact-head release gate only when product/build
inputs change.

- [ ] **Submit one integrated documentation domain to Quarter 4 review**

Give all reviewers the same commit and non-mutating scopes: architecture/authority, operational
runnability/recovery, factual reference correspondence, and navigation/history/source truth. Freeze
the candidate across batches; union/deduplicate findings; remediate every substantiated Critical,
Important, or Minor finding and re-review the new exact head.

- [ ] **Fast-forward release, publish evidence, and clean**

After approval, confirm the release branch has not advanced from Task 1's source head. Fast-forward
it to the accepted documentation commit from the release owner's clean worktree, not from an active
lane worktree:

```bash
git switch release/market-squawk-v0.1.0
git merge --ff-only docs/product-documentation
git push origin release/market-squawk-v0.1.0
head_sha=$(git rev-parse HEAD)
tree_sha=$(git rev-parse 'HEAD^{tree}')
integration_pr=$(
  gh pr list --repo Sawmonabo/market-squawk \
    --head release/market-squawk-v0.1.0 --state open \
    --json number --jq '.[0].number'
)
gh pr comment "${integration_pr}" --repo Sawmonabo/market-squawk --body \
  "Documentation portal ${head_sha} (tree ${tree_sha}) is integrated and reviewed. The delivery
ledger records local evidence, Mermaid rendering, remaining product blockers, and cleanup."
```

If the release branch advanced, do not merge: refresh the factual head and repeat the affected
content/gate. Close the owning issue/Project 5 item only after integration and review. Remove clean
documentation worktrees, prune metadata, and delete merged/patch-equivalent local/origin branches.

## Completion condition

The lane is complete only when one clean committed head contains the entire approved tree with
substantive current content, truthful runnable/missing/release-blocked boundaries, preserved
architecture and model-runbook history, resolved maintained links, stable GitHub-rendered Mermaid
with prose equivalents, direct reviewed sources, and no fictional procedure, redirect stub,
placeholder page, documentation-specific script, or test. This removes the documentation-system
release blocker; it does not approve unrelated product blockers or the complete release.
