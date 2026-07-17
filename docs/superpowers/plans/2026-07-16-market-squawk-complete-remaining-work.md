# Market Squawk Complete Remaining Work Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. A worktree represents a complete conflict-isolated lane, never one
> checklist step. Formal independent reviews occur only at the quarter checkpoints defined here.

**Goal:** Close every remaining mandatory requirement in the Market Squawk complete-local-release
specification and demonstrate the complete release on one clean, unchanged, reviewed exact commit.

**Architecture:** Preserve the independent live execution and research data planes over shared
domain contracts. Complete the current authority foundation first, then build the production live
slice, local analytical data plane, analytics/modeling/portfolio systems, fair-value and typed local
control surfaces, and final measured release evidence. Each quarter remains runnable and receives
one grouped exact-head review after all of its parallel lanes are integrated.

**Tech Stack:** Rust 1.97.1 stable, Edition 2024, Cargo resolver 3, Tokio, Serde, Reqwest,
Tokio-Tungstenite, Arrow, Parquet, DataFusion, SQLite, Clap, Tracing, Thiserror, Rust Decimal,
Proptest, Criterion, Cargo-fuzz, optional Python outside the live path, and conditional local
ONNX-compatible inference.

Rust 1.97.0 is ineligible for implementation, benchmark, checkpoint, or release evidence because
the Rust project subsequently classified and corrected its LLVM miscompilation in 1.97.1.

## Global Constraints

- No paid software, paid API, cloud service, external database service, mandatory container
  runtime, mandatory telemetry infrastructure, or OpenTelemetry version-1 dependency.
- No identity/account rotation to evade limits, fingerprint spoofing, CAPTCHA bypass,
  blocking-evasion proxy rotation, distributed quota evasion, or other access-control evasion.
- Only current, registry-owned `DirectVerified` authority can permit immediate automated action by
  default. A quality enum, archived assessment, modeled value, CLI value, or MCP request is never
  execution authority.
- Live event-to-decision code performs no SQLite, Arrow/Parquet/DataFusion, Python, MCP, LLM,
  arbitrary filesystem, unrelated network, or unbounded-queue operation.
- Financial orders, balances, fees, cost basis, and accounting never use binary floating point.
- All queues, result sets, parser inputs, retained object graphs, and artifact writes are bounded
  with checked arithmetic and typed fail-closed behavior.
- Every implementation task begins with a failing test, ends with affected-crate verification, and
  is committed independently inside its lane. A contract or mock does not count as a production
  capability.
- Synthetic exchanges are test fixtures only and can never be registered, configured, or described
  as production sources.
- Root alone owns workspace manifests, `Cargo.lock`, cross-lane conflict resolution, checkpoint
  documents, exact-head gates, tags, and release claims.
- At most three disjoint implementation lanes run beside root. Review agents are read-only against
  the already integrated exact candidate and do not receive separate implementation worktrees.
- Clean lane worktrees are removed immediately after integration and handoff. Dirty or active
  worktrees are never force-removed; branch deletion is a separate decision.
- Provider research, retrieval date, official links, protocol choice, coverage ceiling, licensing
  constraints, fixtures, and known gaps are persisted under `docs/research/`.

---

## Current truthful baseline

- Clean root: `feat/stage-1-foundation` at
  `d619a8f76c95b5d3487010f0a8cca487f4a55610`.
- One active paused worktree: `.worktrees/q2-authority-persistence` at rejected commit
  `9d9ce8ec0273027ca3e909b438d97ca8ad268607`, with uncommitted review remediation preserved.
- Workspace packages currently present: app, domain, platform, sources, and live.
- Production analytics, data, modeling, portfolio, execution, valuation, MCP, and adapter packages
  are not yet present.
- The current gap ledger reports 74 Implemented, 42 Partial, 76 Missing, 4 Incorrect, 9 Unsafe,
  and 3 Intentionally deferred entries. Q2 code already integrated in root has not yet received the
  final documentation reclassification or exact-head approval.
- Q2 live-memory and application-boundary lanes are integrated. Source authority A1-A3 are not
  integrated, A3 is rejected, A4 is not started, checkpoint truth is not refreshed, and Q2 is not
  approved.
- The detailed Q3 production design and 19-task implementation plan exist, but execution is blocked
  until formal Q2 approval and a Task 0 refresh against that exact approved commit.

## Completion dependency graph

```text
Q2 authority/capture remediation and exact-head approval
    |
    v
Q3 production live features + strategy/risk + Coinbase + paper execution + app services
    |
    +------------------------------+
    |                              |
    v                              v
Q4 Kraken/live-source closure   Q4 local research/storage/provider ingestion
    |                              |
    +---------------+--------------+
                    v
Q5 analytics + PIT datasets + modeling/backtesting + portfolio
                    |
                    v
Q6 fair value + complete shared services + typed MCP/CLI
                    |
                    v
Q7 fuzz/performance/security/release demonstration
```

The Q4 live-source and research-provider branches may run in parallel after Q3 because they consume
frozen source/domain contracts and do not share hot-path implementation files. Q5 requires both.

## Common exact-head checkpoint gate

Every Q2-Q7 quarter checkpoint runs from a clean worktree, records the candidate before the gate,
and asserts that neither the commit nor worktree changed afterward:

```bash
candidate="$(git rev-parse HEAD)"
test -z "$(git status --short)"

./scripts/verify.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
cargo deny check
cargo audit --deny warnings
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
python3 scripts/check_brand.py
python3 scripts/check_generated_artifacts.py
gitleaks dir --redact --no-banner --config .gitleaks.toml .
gitleaks git --redact --no-banner --config .gitleaks.toml
git diff --check

test "$(git rev-parse HEAD)" = "$candidate"
test -z "$(git status --short)"
```

A missing required audit tool is a blocker, not a skipped success. Hosted CI is recorded only when
actually inspected and never inferred from local results.

---

## Quarter 2 checkpoint: finish the production authority foundation

Detailed controlling plan:
[`2026-07-16-q2-integrated-checkpoint-remediation.md`](2026-07-16-q2-integrated-checkpoint-remediation.md).

Detailed A4 contract and parallel-lane preflight:
[`2026-07-17-q2-a4-capture-authority-preflight.md`](2026-07-17-q2-a4-capture-authority-preflight.md).

### Task Q2.1: Remediate the rejected A3 exact candidate

**Lane:** Existing `q2-authority-persistence` worktree; no additional worktree.

**Files:** Source budget/registry/persistence modules, four live fixtures, and focused unit,
integration, subprocess, property, compile-fail, and memory-boundary tests.

- [ ] Migrate all four live fixtures from removed generic `try_new()` to the explicit ephemeral
  diagnostic constructor so the complete workspace remains runnable.
- [ ] Enumerate every Retry-After, refusal, clock, and deadline persistence-failure branch; add
  failing tests proving each failure latches global terminal/unavailable authority across retained
  handles, later generations, aliases, and newly registered scopes.
- [ ] Persist every restrictive state transition before exposing it and prohibit any
  availability-increasing recovery after its durable write fails.
- [ ] Make durable budget retained-size accounting exhaustive over the reachable durability
  session, checkpoint envelope, declarations, registry state, store ownership, synchronization
  objects, strings/vectors, and control blocks, using checked structural formulas and exact-boundary
  tests.
- [ ] Replace the public arbitrary `Arc<dyn AuthorityStateStore>` production constructor with an
  owned concrete `LocalAuthorityStateStore` boundary. The registry must take sole capability
  ownership; callers cannot retain a clone and write around registry transactions.
- [ ] Keep generic failure-injection storage and constructors crate-private. Move external
  in-memory failure tests into module/unit tests rather than reopening a public diagnostic bypass.
- [ ] Canonicalize every nested `used_revisions` sequence, reject duplicates/noncanonical state,
  and prove byte-identical state over at least 100 permutations of sources, per-source revisions,
  policies, budget groups, and declarations.
- [ ] Re-audit all modified production files against the normal 500-700-line ceiling and preserve
  focused module boundaries.
- [ ] Run formatting, full workspace check, platform/sources/live/app all-target/all-feature tests,
  sources doctests, strict Clippy, release build, boundary checks, dependency checks, and
  `git diff --check` on the replacement lane commit.
- [ ] Obtain a fresh independent exact-head A3 review; all five Important findings and their
  adjacent blast radius must be explicitly closed before integration.

### Task Q2.2: Seed the exhaustive capture retained-size contract

**Owner:** Root integration lane after Q2.1 approval.

- [ ] Add the required, non-default
  `CaptureAuthorityBundle::checked_retained_bytes() -> Option<usize>` contract.
- [ ] Implement it with exhaustive destructuring over every allocation-bearing domain/source
  capture field and capacity-sensitive session, binding, lease, admission, and degradation value.
- [ ] Define the value as the conservative complete generation/bundle charge applied once per
  queued message; platform separately charges exact frame and queue-record storage.
- [ ] Make overflow typed and fail closed. Update every implementation and call site in the same
  seed commit so the workspace stays runnable.
- [ ] Freeze the contract with exact-boundary and new-field compile-pressure tests before parallel
  Q2.3 lanes start.

### Task Q2.3: Run the two disjoint A4 lanes in parallel

**Lane A4-TIME — source-owned trusted time:**

- [ ] Remove caller-authored timestamps from `RawFrameFactory::try_frame` without a compatibility
  overload.
- [ ] Sample and seal paired wall/monotonic receipt observations inside registry-owned authority.
- [ ] Maintain a non-resetting wall high-water and permanently latch any rollback/discontinuity for
  that registry generation.
- [ ] Reject forged, old buffered, future, rolled-back, current, live, queued, capture, and health
  authority after discontinuity; require a new durably valid registry composition for recovery.
- [ ] Migrate every raw-frame call site and add signature/compile-fail, rollback, buffering, health,
  and replacement-generation tests.

**Lane A4-MEM — platform-owned capture admission:**

- [ ] Persist the precomputed complete bundle charge in each capture generation.
- [ ] Add exact frame, queue-record, session-capacity, and complete generation/bundle charges to
  every queued-record reservation with checked arithmetic.
- [ ] Release reservations on every success, error, cancellation, shutdown, writer failure, and
  drop path.
- [ ] Add exact-limit/one-byte-under tests and a blocked-writer rotation test retaining one queued
  frame from every distinct generation.
- [ ] Prove no uniquely retained allocation is omitted and document intentional per-message
  overcharge of shared generation state.

### Task Q2.4: Integrate, reconcile checkpoint truth, and approve Q2

- [ ] Integrate A1, A2, approved A3 remediation, Q2.2, A4-TIME, and A4-MEM in dependency order;
  reject unexplained manifest or lockfile changes.
- [ ] Rerun affected package gates after each integration boundary and the combined source/live/
  platform/app gate after the full merge.
- [ ] Finish the semantic documentation-coherence policy test.
- [ ] Refresh current-state, gap analysis, implementation plan, Q2 checkpoint append, SDD progress,
  README, and controlling-plan links to one `pending-exact-head-rereview` candidate while preserving
  prior rejected reviews verbatim.
- [ ] Freeze one clean candidate and run `./scripts/verify.sh`, Cargo deny, Cargo audit, working-tree
  and history Gitleaks, generated-artifact, brand, dependency-boundary, duplicate-dependency,
  rustdoc, smoke, release-build, and unchanged-HEAD/status assertions.
- [ ] Dispatch three read-only reviewers against that exact commit: authority/time/capture;
  live-memory/concurrency/shutdown/MCP; architecture/security/docs/audits.
- [ ] Union and remediate every substantiated Critical, Important, and Minor finding, then rerun the
  entire exact-head gate and all three reviews until clean.
- [ ] Create the annotated Q2 approval tag without a post-review mutation.
- [ ] Remove the authority and A4 worktrees normally after integration, evidence handoff, clean
  status, and agent/process ownership checks.

---

## Quarter 3 checkpoint: production live decisions and realistic paper execution

Detailed controlling design and plan:

- [`../specs/2026-07-16-market-squawk-q3-production-design.md`](../specs/2026-07-16-market-squawk-q3-production-design.md)
- [`2026-07-16-market-squawk-q3-production-plan.md`](2026-07-16-market-squawk-q3-production-plan.md)

All 19 Q3 tasks remain pending until Q2 approval:

- [ ] Q3.0 refresh every plan/design path, interface, and baseline against the approved Q2 tag.
- [ ] Q3.1 enforce the closed Q3 dependency DAG and prohibit production `test-support` features.
- [ ] Q3.2 add invariant-preserving account, strategy, model, order, client-order, and approval
  identities plus complete typed order primitives.
- [ ] Q3.3 split the oversized live actor into focused scheduling, processing, action, and snapshot
  modules without changing behavior.
- [ ] Q3.4 implement typed, capture-bound decoder outcomes for data, control, ignored,
  resynchronization, and quarantine frames.
- [ ] Q3.5 integrate Wave 0 and add nonempty analytics, execution, Coinbase, and paper packages with
  real consumers and frozen contracts.
- [ ] Q3.6 implement every required pure live feature kernel and the complete versioned feature
  registry metadata contract.
- [ ] Q3.7 implement authoritative account coordination, capital/position/exposure/leverage/rate/
  duplicate/loss/drawdown controls, expiring reservations, and deterministic risk reasons.
- [ ] Q3.8 implement the pinned Coinbase Exchange adapter, official provider research, bounded
  exact-lexeme decoding, and honest single-venue `DirectUnverified` ceiling.
- [ ] Q3.9 integrate Wave 1, resolve the root lockfile once, and run the exact wave gate.
- [ ] Q3.10 attach bounded route-owned feature windows and bounded cross-venue state to live actors,
  including reset, staleness, saturation, memory, and immutable snapshot policy.
- [ ] Q3.11 implement canonical strategies and complete intents; consume live authority through
  comprehensive risk exactly once; privately construct approval and adapter-only dispatch values;
  audit every decision outside the hot path.
- [ ] Q3.12 compose Coinbase through registry, budget, health, capture, decoder, session/current
  validation, subscription acknowledgement, live ingress, resynchronization, and bounded app
  supervision rather than the diagnostic engine.
- [ ] Q3.13 implement realistic deterministic paper execution: accepted/partial/filled/cancel/
  rejected/expired states, fees, seeded latency, bid/ask, depth, slippage, impact, cancellation
  races, balances, reservations, positions, fills, reconciliation, recovery, and audit.
- [ ] Q3.14 integrate Wave 2 and prove the complete feature/risk/dispatch/paper composition and peak
  memory ceiling.
- [ ] Q3.15 rewire app, CLI, and MCP compatibility commands through shared production services and
  delete the diagnostic engine where its migration is complete.
- [ ] Q3.16 add Coinbase/paper/parser fuzz targets and measured decoder, feature, risk, paper,
  event-to-decision, and RSS evidence without overstating final-release performance.
- [ ] Q3.17 reconcile architecture, gap, coverage, provider research, README, and operations docs
  with the exact integrated code.
- [ ] Q3.18 run the grouped Q3 exact-head gate and independent quarter review; remediate all
  severities, tag the unchanged approved commit, and remove all completed Q3 lane worktrees.

Q3 must additionally prove no Coinbase path can mint `DirectVerified` authority and no strategy,
model, app service, CLI, MCP tool, replay/archive value, or adapter can bypass risk/dispatch.

---

## Quarter 4 checkpoint: complete live-source coverage and the local research data plane

### Task Q4.1: Complete the required Kraken live adapter

- [ ] Verify the live-source contracts can represent exchange WebSocket/REST snapshots, authorized
  broker market/account/order streams, FIX/native binary profiles, trades, quotes, auctions, halts,
  and top/price/order-level depth across equities, options, futures, FX, crypto, and on-chain
  instruments without conflating a supported contract with a working production adapter.
- [ ] Implement Kraken WebSocket v2 subscription, snapshots, message-atomic price-level updates,
  depth truncation, delete-on-zero, exact decimal lexemes, canonical top-ten CRC32, and typed
  control/recovery outcomes.
- [ ] Bind Kraken through the same registry, provider budget, capture, current-source, instrument,
  qualification, route, and shutdown contracts as Coinbase.
- [ ] Quarantine immediately on checksum, sequence, depth, status, precision, freshness, generation,
  or subscription mismatch; reconnect through a new generation, fresh snapshot, and full
  requalification.
- [ ] Persist official protocol/checksum links, retrieval date, fixtures, coverage, and quality
  evidence; keep public endpoint tests separate from deterministic local-WebSocket tests.
- [ ] Add Coinbase/Kraken cross-venue comparison, source-health, coverage, reconnect, saturation,
  decoder fuzz, and kernel benchmark evidence.
- [ ] Require every present or future equity adapter to disclose single-venue, partial, delayed, or
  consolidated coverage and prevent coverage terminology from upgrading data quality.

### Task Q4.2: Build the research-plane package and local catalog foundation

- [ ] Add nonempty data and research-adapter packages only with their first working consumers.
- [ ] Implement SQLite migrations with strict tables, foreign keys, transactions, busy policy,
  integrity checks, backups, versioned schema, and local run/source/cursor/manifest/audit state.
- [ ] Implement the durable instrument/identifier/venue registry with symbol history, CUSIP, ISIN,
  SEDOL, FIGI, OCC options, futures contracts/rolls, crypto pairs/chain addresses, provider IDs,
  mergers, delistings, and corporate actions.
- [ ] Implement a versioned Arrow schema registry with exact Decimal128, currency, scale,
  provenance, source/effective/published/available/ingested time, revision, and supersession fields.
- [ ] Implement atomic Parquet publication through content-addressed manifests, idempotency keys,
  deduplication, query-driven partitions, small-file limits, compaction, lineage, and crash recovery.
- [ ] Implement bounded embedded DataFusion services and read-only CLI SQL; never expose unrestricted
  SQL through MCP or place analytical dependencies on the live path.

### Task Q4.3: Implement bounded local-file and financial-export adapters

Run disjoint adapter lanes after Q4.2 freezes schemas:

- [ ] CSV/TSV with explicit schema, delimiter, encoding, size, row-error, decimal, and timestamp
  policy.
- [ ] JSON/NDJSON with strict schemas, bounded nesting/record sizes, duplicate-field policy, and
  raw-source hashes.
- [ ] XML with external-entity/network prohibition and bounded depth/entity/text behavior.
- [ ] Excel with bounded sheets/cells, formula/macro safety, explicit schemas, and cached-value
  policy.
- [ ] Parquet import with schema/version/provenance validation.
- [ ] Read-only SQLite/database export ingestion with snapshot consistency and allowlisted schemas.
- [ ] OFX/QFX and broker exports with raw-record preservation, account/currency validation,
  duplicate detection, cost-basis fields, and supplied-total reconciliation.
- [ ] Generic user-owned/licensed and alternative datasets through explicit authorization,
  provenance, coverage, schema, and lineage—not access-control circumvention.

### Task Q4.4: Implement official filings and macroeconomic adapters

Run provider lanes in parallel behind one shared lawful-access/budget/cache contract:

- [ ] SEC EDGAR submissions, filings, XBRL, Company Facts, bulk initialization/reconciliation,
  declared user agent, shared published-rate ceiling, payload hashes, amendments, and honest
  availability semantics.
- [ ] FRED and ALFRED series, observations, real-time parameters, vintages, revisions, pagination,
  caching, API-key references, and HTTP 429/source-health transitions.
- [ ] BLS v1/v2 deterministic chunking, public/registered quota policy, preliminary indicators,
  series metadata, and explicit vintage limitations.
- [ ] US Treasury Fiscal Data pagination and official rate-file ingestion with schema/change
  detection and source hashes.
- [ ] Corporate-action ingestion and a documented raw/adjusted/total-return policy that preserves
  original observations.

### Task Q4.5: Implement portfolio import, secrets, and core datasets

- [ ] Preserve raw holdings, transactions, cash flows, cost basis, and broker-supplied totals before
  normalization; emit typed reconciliation discrepancies.
- [ ] Implement OS-keyring storage where available and authenticated-encrypted local fallback using
  a user-supplied unlock secret never stored beside ciphertext; cover redaction and rotation.
- [ ] Implement source caches, cursors, health, retry/backoff, coverage metadata, lawful endpoint
  allowlists, redirect policy, failover among authorized adapters, and controlled artifact storage.
- [ ] Publish all required dataset families: instruments, identifiers, venues, trades, quotes,
  order books, corporate actions, filings, XBRL facts, financial statements, fundamentals, macro
  series/observations, accounts, positions, transactions, cash flows, features, labels,
  predictions, models, strategies, orders, fills, risk decisions, valuations, fair-value evidence,
  quality results, and lineage.
- [ ] Prove point-in-time filtering, revision preservation, supersession, idempotent re-ingestion,
  compaction invariance, and no dependency on the live feed's historical coverage.

### Task Q4.6: Gate and review Q4

- [ ] Add parser/schema/capture fuzz targets and opt-in external provider smoke tests.
- [ ] Prove live crates and hot-path binaries have no SQLite, Arrow, Parquet, DataFusion, Python, or
  provider-extraction dependency.
- [ ] Run the full exact-head gate, three grouped Q4 reviews—storage/PIT, adapters/lawful access,
  architecture/security—and remediate every severity.
- [ ] Tag the unchanged Q4 commit and remove all completed Q4 lane worktrees.

---

## Quarter 5 checkpoint: analytics, point-in-time modeling, backtesting, and portfolio

### Task Q5.1: Complete batch analytics and the feature registry

- [ ] Record complete feature metadata: name/version, input schema, parameters, time semantics,
  warm-up, null policy, output type/units, live/PIT compatibility, and implementation revision/hash.
- [ ] Implement price/total returns, volatility, drawdown, correlation, beta, alpha, Sharpe,
  Sortino, tracking error, information ratio, historical/parametric VaR, coherent discrete Expected
  Shortfall, factor exposures, and numerical/property fixtures.
- [ ] Implement fundamental growth/margins, valuation multiples, FCF, earnings/macro surprises,
  yield-curve/rate features, portfolio exposure/attribution, scenarios, and stress tests.
- [ ] Share only pure kernels where semantics match and prove live/batch agreement at explicit
  conversion boundaries.

### Task Q5.2: Build reproducible point-in-time feature/label datasets

- [ ] Select instrument universes as of time using historical constituents, symbol history,
  mergers, delistings, options/futures lifecycle and rolls, and corporate-action policy.
- [ ] Join only observations whose evidenced `available_at` permits use, preserve revisions and
  vintages, enforce label cutoffs and chronological train/validation/test splits, and apply explicit
  missing-value policies.
- [ ] Add future-perturbation, delayed-publication, revision, delisting, survivor-bias, and leakage
  tests.
- [ ] Publish reproducible content-addressed Parquet feature/label datasets with complete lineage.

### Task Q5.3: Implement model bundles and local inference

- [ ] Add model registry, artifact/hash, format/version, feature schema/versions, normalization,
  training period/universe, dataset versions, label definition, training revision, metrics,
  thresholds, intended use, and fallback validation.
- [ ] Implement native Rust inference with bounded input/output and no-action on any error.
- [ ] Implement conditional ONNX-compatible inference with pinned runtime provenance, size/operator/
  schema allowlists, warm-up and threading policy, and a stable fallback.
- [ ] Add a Python research/training package outside live dependencies with reproducible environment,
  export, evaluation, and artifact-hash handoff.

### Task Q5.4: Implement research backtesting and experiment governance

- [ ] Backtest over research datasets with point-in-time timing, fees, slippage, corporate actions,
  portfolio constraints, delistings, and no requirement to mirror/replay the live feed.
- [ ] Record strategy/model/data/code versions, trial inventory, parameter search, selection
  criteria, and backtest-selection-overfitting diagnostics.
- [ ] Reconcile backtest orders/fills/cash/positions against the same invariant-preserving portfolio
  accounting kernels used by paper execution where semantics match.

### Task Q5.5: Implement the complete portfolio system

- [ ] Implement accounts, holdings, transactions, cash flows, tax/cost-basis lots, realized and
  unrealized gains, income, multi-currency cash, and typed reconciliation.
- [ ] Implement allocation plus sector, factor, currency, issuer, venue, and instrument exposures.
- [ ] Implement time-weighted/money-weighted performance as selected and documented, attribution,
  rebalancing, tracking error, VaR/Expected Shortfall, scenario, and stress services.
- [ ] Integrate authoritative portfolio/account revisions into the Q3 risk coordinator without
  allowing callers to supply current balances or positions.

### Task Q5.6: Gate and review Q5

- [ ] Prove future data cannot change earlier features, historical/delisted universes persist,
  model mismatches fail closed, inference errors produce no action, and portfolio totals reconcile
  or emit typed discrepancies.
- [ ] Run full exact-head verification and three grouped reviews—PIT/analytics, modeling/backtest,
  portfolio/risk integration—then remediate every severity.
- [ ] Tag the unchanged Q5 commit and remove all completed Q5 worktrees.

---

## Quarter 6 checkpoint: fair-value analysis and complete local control plane

### Task Q6.1: Implement fair-value storage, evidence, and rules

- [ ] Add valuation, valuation input, method, classification, classification reason, evidence,
  ruleset version, override, approval, reviewer, and immutable audit records.
- [ ] Implement deterministic Level 1 candidate rules for identical instrument, quoted price,
  active/accessed market, measurement-date relevance, no disqualifying adjustment, source/venue
  evidence, and freshness.
- [ ] Return `Unclassified` for missing evidence. Never silently qualify delayed, stale, proxy,
  adjusted, modeled, estimated, or similar-instrument values as Level 1.
- [ ] Make fair-value hierarchy, market depth, data quality, and execution eligibility impossible to
  substitute through public constructors, Serde, conversions, or MCP inputs.
- [ ] Prove Level 2/3 analytical observations can never mint `DirectVerified` or execution
  authority.

### Task Q6.2: Build shared bounded application services

- [ ] Implement service boundaries reused by CLI and MCP for sources, market state, research,
  fundamentals, macro, portfolios, analytics, models, valuation, bots, and execution.
- [ ] Enforce typed authorization, cancellation/deadlines, time/instrument/result limits, source
  coverage, audit admission, and controlled content-hashed artifact references in the services,
  rather than duplicating policy in transports.
- [ ] Keep mutable live ownership inside the live runtime and expose only bounded immutable
  snapshots/control commands outside the event path.

### Task Q6.3: Complete typed local stdio MCP

- [ ] Implement protocol initialization/version/capability negotiation, request IDs, progress,
  cancellation, deadlines, bounded framing/results, typed errors, and audit.
- [ ] Implement complete Source, Market, Research, Fundamental, Macro, Portfolio, Analysis, Model,
  FairValue, Bot, and Execution tool/resource domains through shared services.
- [ ] Write large outputs only to the controlled artifact directory and return content-hashed
  references.
- [ ] Prove MCP exposes no arbitrary shell/path/SQL, credentials, remote code loading, unchecked
  order submission, risk bypass, or audit deletion.

### Task Q6.4: Complete the CLI hierarchy

- [ ] Implement `init`, `config`, `source`, `capture`, `ingest`, `dataset`, `query`, `feature`,
  `model`, `portfolio`, `backtest`, `bot`, `execution`, `fair-value`, `mcp serve`, and `doctor`
  command families over the same services as MCP.
- [ ] Restrict read-only DataFusion SQL to the local CLI with bounded resources and controlled
  artifact output.
- [ ] Preserve documented compatibility aliases without preserving diagnostic authority claims.

### Task Q6.5: Gate and review Q6

- [ ] Run fair-value golden/property/no-promotion tests plus MCP schemas, lifecycle, cancellation,
  result bounds, artifact confinement, audit, and prohibited-surface tests.
- [ ] Run full exact-head verification and three grouped reviews—fair value/accounting, MCP/CLI
  protocol/services, security/authority boundaries—then remediate every severity.
- [ ] Tag the unchanged Q6 commit and remove all completed Q6 worktrees.

---

## Quarter 7 checkpoint: measured release hardening and complete demonstration

### Task Q7.1: Complete code-quality, documentation, and test coverage

- [ ] Document every public API's invariants, units, ownership, errors, panics, and safety contract;
  eliminate unjustified lint exceptions and split modified hotspots above the focused-file ceiling.
- [ ] Complete deterministic tests for financial conversion, instruments, sequence/checksum/books,
  bounded queues, reconnect/resynchronization, quality transitions, PIT/revisions/corporate actions,
  features, models, portfolios, risk, paper execution, fair value, MCP, and CLI.
- [ ] Complete property tests for financial, book, analytical, portfolio, and execution invariants.
- [ ] Complete fuzz targets and corpora for Coinbase/Kraken/protocol decoders, JSON, CSV, XML,
  Excel/schema inference, capture records, binary formats, and MCP request decoding.

### Task Q7.2: Measure every required performance dimension

- [ ] Add Criterion and end-to-end harnesses for decoding, sequence/checksum, bounded queues, books,
  online features, inference, strategy, risk, paper dispatch, and event-to-decision.
- [ ] Add Arrow/Parquet ingestion/write and DataFusion query benchmarks.
- [ ] Add sustained-burst and allocator/RSS peak-memory harnesses with no database/analytical I/O in
  the live path.
- [ ] Record hardware, OS, toolchain, fixture, event count, throughput, p50/p95/p99/max latency, and
  peak memory.
- [ ] Demonstrate at least 100,000 synthetic events/second, warmed sub-millisecond p99 internal
  event-to-decision latency, and bounded sustained memory. If any threshold fails, remediate and
  repeat before release rather than weakening the acceptance criterion.

### Task Q7.3: Complete supply-chain and security hardening

- [ ] Run and close dependency advisories, license/source bans, duplicate/native dependency review,
  Rust MSRV/toolchain lock, working-tree/history credential scans, generated-artifact checks, brand
  checks, and secret-redaction tests.
- [ ] Publish threat model, abuse cases, lawful provider-access policy, endpoint allowlists, secure
  defaults, local storage permissions, secret lifecycle, artifact confinement, and vulnerability
  response.
- [ ] Generate SBOM, source/build provenance, artifact hashes, reproducible local build
  instructions, and a signed release process where locally available without making hosted
  infrastructure mandatory.

### Task Q7.4: Demonstrate and package the complete local release

- [ ] Add one deterministic local demonstration that exercises live Coinbase/Kraken ingestion,
  qualification/books/features, source health, research/file/SEC/macro ingestion, Arrow/Parquet,
  DataFusion, PIT datasets, model inference, backtesting, portfolio analytics, risk-controlled paper
  execution, fair-value classification, CLI, and typed stdio MCP.
- [ ] Prove the release remains useful with local files and public/user-authorized free interfaces
  when external providers are unavailable; surface honest coverage/health rather than false success.
- [ ] Update README, SECURITY, CONTRIBUTING, CHANGELOG, operations, schemas, provider coverage,
  benchmark reports, release notes, and financial-use warnings.
- [ ] Build and hash release artifacts without requiring cloud, containers, telemetry, a remote
  database, or a paid dependency.

### Task Q7.5: Run the final exact-head gate and release review

- [ ] Run formatting; strict workspace all-target/all-feature Clippy; workspace tests/doctests;
  locked release build; Cargo deny/audit; boundary, brand, generated-artifact and duplicate checks;
  Gitleaks working-tree/history scans; every fuzz smoke; benchmarks; external-network gates; and the
  complete demonstration on one unchanged clean commit.
- [ ] Dispatch three independent final reviewers—live/execution/performance, research/model/
  portfolio/valuation, and architecture/security/operations/CLI/MCP—against that exact commit.
- [ ] Require every mandatory gap entry to be `Implemented`; only the explicitly non-release items
  below may remain `Intentionally deferred`. Remediate all severities and repeat gates/reviews.
- [ ] Tag the unchanged final release commit and remove every completed lane/review worktree.

---

## Explicit non-release items and permanent exclusions

The following are not missing first-release work:

- Optional paid/licensed feed adapters. The free/local product and adapter contracts are mandatory;
  a paid provider implementation is not.
- Optional authorized live-money broker execution. The non-bypassable adapter contract and complete
  paper execution are mandatory; live money remains disabled unless separately authorized later.
- Optional replay enhancements. Replay remains diagnostic and is not the research/backtesting
  architecture.
- Optional future OpenTelemetry adapter. Local tracing is mandatory; OpenTelemetry infrastructure
  is deliberately absent from version 1.
- Every evasion mechanism listed in Global Constraints is permanently prohibited, not deferred.

## Complete-release exit condition

Market Squawk is complete only when Q2 through Q7 are approved on exact commits; every required
functional adapter works; every required dataset and analytical/modeling/portfolio/fair-value/MCP/
CLI capability is demonstrated; all deterministic, property, fuzz, security, release-build, and
performance gates pass; no mandatory requirement remains Partial, Missing, Incorrect, or Unsafe;
and the product retains no paid/cloud/container/telemetry/evasion requirement.
