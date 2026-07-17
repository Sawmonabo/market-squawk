# Market Squawk Q3 Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete bounded live-feature, authoritative-risk, production Coinbase, and
realistic paper-execution slice that supersedes Stage-1 Tasks 9–12 without weakening Q2 authority,
memory, lifecycle, lawful-access, or hot-path invariants.

**Architecture:** Pure feature kernels live in `market-squawk-analytics`; instrument-owned live
actors own bounded feature/strategy state and expose a synchronous live-owned action hook implemented
by `market-squawk-execution`, preserving the dependency DAG. Sources return capture-bound typed
decoder outcomes, the app composes Coinbase through the authoritative registry/capture/live path,
and a bounded single-writer paper adapter consumes dispatcher-only values with deterministic
realistic market simulation.

**Tech Stack:** Rust 1.97.0 stable, Edition 2024, Cargo resolver 3, Tokio, Tokio-util, Serde,
Rust Decimal, UUID, Chrono, Thiserror, Tracing, Tokio-Tungstenite, Proptest, Trybuild, Criterion,
Cargo-fuzz targets, Python 3 boundary checks, and local Git worktrees.

**Planning evidence base:** `834674aa40198656b4486c4c64dec1fa788eae29`. This is an audit anchor,
not Q2 approval or the automatic Q3 execution base. Task 0 replaces it with the formally approved
integrated Q2 commit and refreshes every path/line anchor before implementation.

## Global Constraints

- This plan is **proposed for execution only after formal Q2 approval**.
- Before execution, rebase or recreate every implementation worktree at the approved integrated Q2
  commit, update the recorded base commit, and refresh every file/line anchor with `rg -n`/`nl -ba`.
- Local build/test results are local evidence only. Never state that hosted CI ran or passed without
  inspecting the hosted check result.
- Rust toolchain is exactly `1.97.0`, stable, Edition 2024, resolver 3; every package inherits
  workspace package metadata and lints.
- `unsafe_code = "forbid"`; production paths do not use `unwrap`, `expect`, `panic!`, `todo!`, or
  `unimplemented!`.
- Libraries use typed errors; `anyhow` remains app-boundary only.
- `DirectVerified` is the only default immediate automated-action quality.
- Coinbase Exchange remains capped at `DirectUnverified`; no test, configuration, or documentation
  may promote it.
- Financial orders, balances, fees, cost basis, and accounting use scaled integers or checked
  decimal values with explicit currency, scale, and rounding; they never use `f32`/`f64`.
- Every dispatch freezes the validated instrument-definition revision, price tick, lot size, quote
  and settlement currency, and exact positive contract multiplier. Paper accounting rejects term,
  scale, revision, or currency transplants and never treats ticks as currency minor units.
- Statistical `f64` is allowed only through an explicit analytics conversion boundary and never
  becomes an order price.
- The live event-to-decision path performs no SQLite/DataFusion/Parquet/Python/MCP/LLM call, no disk
  persistence, no unrelated network request, and no unbounded queue write.
- Every queue has count and byte bounds. Saturation has a typed fail-closed policy and is included in
  the checked startup memory model.
- Root `Cargo.toml`, `Cargo.lock`, app composition files, and cross-lane conflict resolution are owned
  only by the integration owner.
- External network tests are ignored/opt-in. Deterministic local WebSocket tests are mandatory in the
  default suite.
- Research conclusions, provider protocol choices, retrieval date, official links, fixture schema,
  coverage, quality ceiling, and known gaps are persisted under `docs/research/providers/`.
- Preserve raw frames and exact provider lexemes; the adapter never duplicates live tick/lot
  qualification.
- Preserve local-first/no-mandatory-paid/cloud/database/container/telemetry operation.
- Identity/account rotation to evade limits, fingerprint spoofing, CAPTCHA bypass, blocking-evasion
  proxy rotation, and distributed quota evasion remain permanently prohibited.
- Do not claim 100,000 events/s or sub-millisecond p99 before measured evidence on documented
  hardware.

## Supersession

This plan replaces Tasks 9–12 in
`docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md`. The historical text remains
for audit only and must not be executed. Specifically:

| Historical task | Superseding Q3 scope |
| --- | --- |
| Task 9 | Every required live feature, registry, bounded route/cross-venue state, actor integration, snapshots, reset policy, and memory proof. |
| Task 10 | Complete typed intent, authoritative account coordinator, all risk controls, live-owned capability consumption, private approval, one-time dispatcher, and audit. |
| Task 11 | Typed decoder outcomes, pinned Coinbase Exchange v1 adapter, exact capture/current validation, shared-budget supervisor, explicit instrument mapping, and actual app production composition. |
| Task 12 | Complete realistic paper execution: fees, seeded latency, slippage, depth-aware partial fills, order states, cancellation races, balances/positions, reconciliation, and audit. |

The controlling design is
`docs/superpowers/specs/2026-07-16-market-squawk-q3-production-design.md`.

## Frozen dependency DAG

```text
domain
├── platform
├── sources ───────────────> platform
├── analytics
├── live ──────────────────> sources, analytics
└── execution ─────────────> live, analytics

adapter-coinbase ──────────> domain, sources
adapter-paper ─────────────> domain, execution
app ───────────────────────> all composition dependencies
```

Forbidden edges include `live -> execution`, any adapter-to-app edge, Coinbase-to-live/platform,
paper-to-live/sources/platform, and production use of a `test-support` feature.

## Maximum-safe parallel ownership

At most three worker lanes run beside the root integration owner. A lane never edits another lane's
owned files. Lane commits do not include the root `Cargo.lock`; the integration owner resolves that
single lockfile after merging a wave and does not cherry-pick a lane root-lock diff. The isolated
`fuzz/Cargo.lock` is the only exception and belongs exclusively to Task 16 lane B's nested fuzz
workspace.

| Wave | Parallel lane A | Parallel lane B | Parallel lane C | Integration owner |
| --- | --- | --- | --- | --- |
| 0 | Boundary checker + domain execution identities | Live actor split | Sources fixture/module split + decoder outcomes | Merge in order, freeze cross-crate contracts, register nonempty Q3 crates, minimally resolve root lockfile |
| 1 | Complete analytics kernels/registry | Complete authoritative account/risk core | Complete Coinbase adapter | Merge manifests without lane lockfiles; run Wave 1 gate |
| 2 | Live feature/cross-venue integration | Coinbase app production composition | Complete realistic paper adapter | Subwave 2A: no competing live edits; review/merge Task 10. Subwave 2B: serialize execution approval/dispatcher/live hook from Task 11 on that merge before Task 14. |
| 3 | 3B after Task 15: analytics + execution benches/manifests | 3B after Task 15: Coinbase + paper benches and isolated fuzz workspace | 3B after Task 15: app latency/RSS harness, measurement script, tooling research | 3A serialize Task 15; 3B merge benchmark manifests/root lock and measure; 3C Task 17 docs; 3D Task 18 grouped review |

Shared conflict hotspots are always serialized:

```text
Cargo.toml
Cargo.lock
apps/market-squawk/src/lib.rs
apps/market-squawk/src/main.rs
apps/market-squawk/src/diagnostic_engine.rs
apps/market-squawk/src/bot.rs
apps/market-squawk/src/mcp.rs
crates/market-squawk-live/src/runtime/actor.rs
crates/market-squawk-execution/src/adapter.rs
```

Wave 2 has an explicit ordering barrier. Tasks 10, 12, and 13 may start in parallel because their
owned files do not overlap. The integration owner does not edit `live` concurrently. After Task 10's
live commit passes lane self-review/focused gates, merge it first; only then execute Task 11's
execution/live authority edits on top. Task 11 never runs concurrently with Task 10. Tasks 12 and 13
may finish during that serialized work because they remain within app/platform and paper ownership,
respectively. Task 14 starts only when Tasks 10–13 are complete.

Wave 3 is also barriered. Subwave 3A runs Task 15 alone and reaches its clean exact-head gate before
performance work begins. In subwave 3B, Task 16 may use three disjoint lanes: lane A owns analytics
and execution benches/package manifests; lane B owns Coinbase and paper benches/package manifests
plus the isolated `fuzz/` workspace; lane C owns the app latency/RSS harness, app manifest request,
measurement script, and tooling research. No lane edits root `Cargo.toml` or `Cargo.lock`; after all
three handoffs, the integration owner merges package manifests, resolves the root lock once, runs
measurements, writes final performance evidence, commits, and runs the exact-head gate. Subwave 3C
runs Task 17 only after that measured evidence exists. Subwave 3D runs Task 18 only after Task 17's
docs-inclusive candidate is clean. No review-evidence preparation starts before that exact candidate
exists.

## Wave candidate and exact-head integration gates

Before each wave commit, the integration owner runs this candidate gate over the reviewed intended
diff:

```bash
./scripts/verify.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check
cargo audit --deny warnings
gitleaks git --redact --no-banner --timeout 300 .
gitleaks dir --redact --no-banner --timeout 300 .
git diff --check
git status --short
```

Expected: every substantive command exits zero locally. `git status --short` is intentionally not
empty; compare it to the reviewed ownership/path list, reject unrelated/unreviewed changes, and stage
only the intended diff. This is candidate evidence, not exact-head evidence.

After the intentional wave commit, capture `WAVE_HEAD="$(git rev-parse HEAD)"`, rerun the same full
command set, replace the final status observation with:

```bash
test -z "$(git status --short)"
test "$(git rev-parse HEAD)" = "$WAVE_HEAD"
```

This second run is the complete Wave exact-head integration gate. Record the exact commit and exit
codes without editing or amending afterward. If it fails, return to red-green remediation, create a
new reviewed commit, and rerun at the new head. `cargo deny`, `cargo audit`, both Gitleaks scans, and
generated-artifact checks are mandatory after every manifest/dependency-changing wave and
unconditionally at Q3 quarter close. A missing audit tool is a blocker, not a skipped success.
Hosted CI status is recorded separately when it exists; it is never a prerequisite for local Q3
approval and is never inferred from local command success.

---

## Wave 0 — approved-base refresh and boundary prerequisites

### Task 0: Refresh the plan against the formally approved Q2 head

**Owner:** Integration owner; no parallel implementation starts before this task.

**Files:**

- Modify: `docs/superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md`
- Modify: `docs/superpowers/specs/2026-07-16-market-squawk-q3-production-design.md`
- Create: `docs/verification/q3-baseline.md`

**Interfaces:**

- Consumes: the formally approved integrated Q2 commit hash and its review evidence.
- Produces: exact Q3 base commit, refreshed path/line anchors, local baseline results, and a clean
  branch point for every Q3 worktree.

- [ ] **Step 1: Create the Q3 integration branch at the approved Q2 commit**

```bash
git status --short
git rev-parse HEAD
git show --stat --oneline HEAD
```

Expected: the worktree is clean and `HEAD` is the commit explicitly approved by the Q2 checkpoint.
If it is not, stop; do not infer approval from a branch name or local test result.

- [ ] **Step 2: Refresh every volatile source anchor**

```bash
rg -n "Task 9|Task 10|Task 11|Task 12" \
  docs/superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md
rg -n "Task 9's bounded|Task 10 returns|pub trait MarketDecoder|validate_decoded_batch_owned" \
  crates/market-squawk-live crates/market-squawk-sources
rg -n "pub struct OnlineFeatures|pub enum RiskDecision|pub struct PaperAccount|async fn run_source" \
  apps/market-squawk
```

Expected: every path exists. Update changed numeric anchors in the design and plan; do not preserve a
stale line number merely because the symbol still exists.

- [ ] **Step 3: Run the complete local baseline**

```bash
cargo build --workspace --all-features --locked
cargo test --workspace --all-features --locked
python3 scripts/check_workspace_boundaries.py
git status --short
```

Expected: all commands exit zero and the worktree is clean. Failure blocks Q3 until diagnosed; it is
not attributed to Q3 code.

- [ ] **Step 4: Record baseline evidence**

Write `docs/verification/q3-baseline.md` with this exact structure:

```markdown
# Q3 Local Baseline

- Approved Q2 commit: copy the exact output of `git rev-parse HEAD`
- UTC execution time: copy the exact output of `date -u +%Y-%m-%dT%H:%M:%SZ`
- OS and architecture: copy the exact output of `uname -a`
- Rust: copy the exact output of `rustc --version --verbose`
- Cargo: copy the exact output of `cargo --version --verbose`
- Local commands: exact commands, exit codes, and test totals from Step 3
- Hosted CI: `Not evaluated by this local baseline task`
- Q2 approval reference: the exact review artifact or commit note used by the integration owner
```

Do not summarize command output from memory; paste the evidence collected in this task.

- [ ] **Step 5: Commit the refreshed base record**

```bash
git diff --check
git add docs/superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md \
  docs/superpowers/specs/2026-07-16-market-squawk-q3-production-design.md \
  docs/verification/q3-baseline.md
git commit -m "docs(q3): bind plan to approved Q2 head"
```

### Task 1: Enforce the Q3 dependency DAG before adding crates

**Owner:** Wave 0 lane A.

**Files:**

- Modify: `scripts/check_workspace_boundaries.py`
- Create: `scripts/tests/test_workspace_boundaries.py`
- Modify: `scripts/verify.sh`

**Interfaces:**

- Consumes: Cargo metadata package names and dependency kinds.
- Produces:
  `dependency_boundary_violations(packages: list[dict[str, object]]) -> list[str]` and a closed
  `ALLOWED_WORKSPACE_DEPENDENCIES` matrix used by the production checker.

- [ ] **Step 1: Write failing DAG and test-support tests**

Create `scripts/tests/test_workspace_boundaries.py` with focused cases equivalent to:

```python
from scripts.check_workspace_boundaries import dependency_boundary_violations


def package(name: str, dependencies: list[dict[str, object]]) -> dict[str, object]:
    return {"name": name, "dependencies": dependencies}


def workspace_dependency(name: str, features: list[str] | None = None) -> dict[str, object]:
    return {
        "name": name,
        "source": None,
        "kind": None,
        "features": [] if features is None else features,
    }


def test_live_cannot_depend_on_execution() -> None:
    packages = [
        package(
            "market-squawk-live",
            [workspace_dependency("market-squawk-execution")],
        )
    ]


def test_platform_cannot_depend_on_execution_or_paper() -> None:
    packages = [
        package(
            "market-squawk-platform",
            [
                workspace_dependency("market-squawk-execution"),
                workspace_dependency("market-squawk-adapter-paper"),
            ],
        )
    ]
    assert dependency_boundary_violations(packages) == [
        "market-squawk-platform: forbidden workspace dependency market-squawk-adapter-paper",
        "market-squawk-platform: forbidden workspace dependency market-squawk-execution",
    ]
    assert dependency_boundary_violations(packages) == [
        "market-squawk-live: forbidden workspace dependency market-squawk-execution"
    ]


def test_production_dependency_cannot_enable_test_support() -> None:
    packages = [
        package(
            "market-squawk",
            [workspace_dependency("market-squawk-live", ["test-support"])],
        )
    ]
    assert dependency_boundary_violations(packages) == [
        "market-squawk: production dependency market-squawk-live enables test-support"
    ]
```

- [ ] **Step 2: Run the tests and confirm the missing function failure**

```bash
python3 -m unittest scripts.tests.test_workspace_boundaries -v
```

Expected: FAIL because `dependency_boundary_violations` is not defined.

- [ ] **Step 3: Implement the closed dependency matrix**

Add these exact allowed internal dependencies:

```python
ALLOWED_WORKSPACE_DEPENDENCIES: dict[str, frozenset[str]] = {
    "market-squawk-domain": frozenset(),
    "market-squawk-platform": frozenset({"market-squawk-domain"}),
    "market-squawk-sources": frozenset(
        {"market-squawk-domain", "market-squawk-platform"}
    ),
    "market-squawk-analytics": frozenset({"market-squawk-domain"}),
    "market-squawk-live": frozenset(
        {
            "market-squawk-domain",
            "market-squawk-sources",
            "market-squawk-analytics",
        }
    ),
    "market-squawk-execution": frozenset(
        {
            "market-squawk-domain",
            "market-squawk-live",
            "market-squawk-analytics",
        }
    ),
    "market-squawk-adapter-coinbase": frozenset(
        {"market-squawk-domain", "market-squawk-sources"}
    ),
    "market-squawk-adapter-paper": frozenset(
        {"market-squawk-domain", "market-squawk-execution"}
    ),
    "market-squawk": frozenset(
        {
            "market-squawk-domain",
            "market-squawk-platform",
            "market-squawk-sources",
            "market-squawk-analytics",
            "market-squawk-live",
            "market-squawk-execution",
            "market-squawk-adapter-coinbase",
            "market-squawk-adapter-paper",
        }
    ),
}
```

`dependency_boundary_violations` must sort packages and dependencies, reject any internal package not
in the matrix, reject every forbidden edge, inspect normal/build dependency features, and ignore
`test-support` only for `dev` dependencies.

- [ ] **Step 4: Run focused and production boundary checks**

```bash
python3 -m unittest scripts.tests.test_workspace_boundaries -v
python3 scripts/check_workspace_boundaries.py
```

Expected: PASS; the current pre-Q3 workspace remains allowed and synthetic fixtures prove each
forbidden edge fails.

- [ ] **Step 5: Add the focused test to the verification script and commit**

```bash
git diff --check
git add scripts/check_workspace_boundaries.py scripts/tests/test_workspace_boundaries.py \
  scripts/verify.sh
git commit -m "build(q3): enforce workspace dependency DAG"
```

### Task 2: Add execution identities and order primitives to the domain

**Owner:** Wave 0 lane A after Task 1's failing test is isolated; no root manifest edit.

**Files:**

- Create: `crates/market-squawk-domain/src/identifiers/execution.rs`
- Modify: `crates/market-squawk-domain/src/identifiers.rs`
- Create: `crates/market-squawk-domain/src/order.rs`
- Modify: `crates/market-squawk-domain/src/instrument/definition.rs`
- Modify: `crates/market-squawk-domain/src/lib.rs`
- Create: `crates/market-squawk-domain/tests/execution_contracts.rs`

**Interfaces:**

- Consumes: `InstrumentId`, `SourceIdentifier`, `Timestamp`, `PriceTicks`, `QuantityLots`,
  `TickSize`, `LotSize`, `Currency`, `Decimal`, and `BasisPoints`.
- Produces: `AccountId`, `StrategyId`, `ModelId`, `OrderId`, `ClientOrderId`, `ApprovalId`,
  `InstrumentDefinitionRevision`, invariant-preserving instrument execution terms, `OrderSide`,
  `OrderType`, `TimeInForce`, and `OrderReasonCode`.

- [ ] **Step 1: Write identity invariant tests**

```rust
#[test]
fn execution_uuid_identities_reject_nil_and_round_trip() -> Result<(), Box<dyn Error>> {
    assert!(AccountId::try_from(Uuid::nil()).is_err());
    assert!(OrderId::try_from(Uuid::nil()).is_err());
    assert!(ApprovalId::try_from(Uuid::nil()).is_err());

    let value = AccountId::try_from(Uuid::new_v4())?;
    let wire = serde_json::to_vec(&value)?;
    assert_eq!(serde_json::from_slice::<AccountId>(&wire)?, value);
    Ok(())
}

#[test]
fn client_and_reason_identifiers_are_bounded_and_strict() {
    assert!(ClientOrderId::try_from("").is_err());
    assert!(OrderReasonCode::try_from("paper.momentum.v1").is_ok());
    assert!(OrderReasonCode::try_from("reason with spaces").is_err());
}

#[test]
fn instrument_execution_terms_are_revisioned_exact_and_currency_bound() -> TestResult {
    let definition = configured_currency_instrument()?;
    assert!(definition.definition_revision().get() > 0);
    assert_eq!(definition.price_tick(), TickSize::try_from("0.01")?);
    assert_eq!(definition.lot_size(), LotSize::try_from("0.0001")?);
    assert_eq!(definition.quote_currency(), Currency::usd());
    assert_eq!(definition.settlement_currency(), Currency::usd());
    assert_eq!(definition.contract_multiplier(), Decimal::ONE);
    Ok(())
}
```

- [ ] **Step 2: Run the domain test and verify missing-type failures**

```bash
cargo test -p market-squawk-domain --test execution_contracts --locked
```

Expected: FAIL because the execution identity and order types do not exist.

- [ ] **Step 3: Implement invariant-preserving execution identities**

Define non-nil UUID newtypes for `AccountId`, `StrategyId`, `ModelId`, `OrderId`, and `ApprovalId`.
Define `ClientOrderId` and `OrderReasonCode` as private wrappers over validated bounded identifiers.
All constructors and deserializers must revalidate. Public fields are forbidden.

Add a nonzero `InstrumentDefinitionRevision` and invariant-preserving exact execution terms to
`InstrumentDefinition`: price tick, lot size, quote currency, settlement currency, and a positive
exact `Decimal` contract multiplier. The constructor and strict deserializer validate all terms.
Existing asset-class definitions must migrate explicitly—never synthesize a currency, multiplier,
or revision from a provider frame. If the approved Q2 domain uses a typed non-currency settlement
denomination, retain that type and add an explicit currency-settlement accessor; the Q3 paper adapter
must reject unsupported non-currency settlement before any reservation or ledger mutation.

Use these closed order enums:

```rust
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
    GoodTilCancelled,
    ImmediateOrCancel,
    FillOrKill,
}
```

- [ ] **Step 4: Add strict-wire and type-separation tests**

Test unknown fields, nil UUID wire values, oversized identifiers, invalid characters, and that
`AccountId`, `OrderId`, and `ApprovalId` cannot be interchanged by `From`/`Into`.

```bash
cargo test -p market-squawk-domain --test execution_contracts --locked
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
```

Expected: PASS.

- [ ] **Step 5: Commit the domain contract**

```bash
git diff --check
git add crates/market-squawk-domain/src/identifiers/execution.rs \
  crates/market-squawk-domain/src/identifiers.rs \
  crates/market-squawk-domain/src/instrument/definition.rs \
  crates/market-squawk-domain/src/order.rs \
  crates/market-squawk-domain/src/lib.rs \
  crates/market-squawk-domain/tests/execution_contracts.rs
git commit -m "feat(domain): add execution identity contracts"
```

### Task 3: Split the oversized live actor without changing behavior

**Owner:** Wave 0 lane B.

**Files:**

- Modify: `crates/market-squawk-live/src/runtime/actor.rs`
- Create: `crates/market-squawk-live/src/runtime/actor/processing.rs`
- Create: `crates/market-squawk-live/src/runtime/actor/scheduling.rs`
- Create: `crates/market-squawk-live/src/runtime/actor/snapshot_publication.rs`
- Modify: `crates/market-squawk-live/src/runtime.rs`
- Test: `crates/market-squawk-live/tests/runtime_rejection.rs`
- Test: existing actor fairness unit tests

**Interfaces:**

- Consumes: the existing `ShardActor`, `ShardCommand`, `FairTurn`, snapshot publisher, and health
  types without changing visibility.
- Produces: the same actor behavior in focused modules, leaving the committed-observation hook at a
  named `process_applied_observation` function for Task 5.

- [ ] **Step 1: Run characterization tests before moving code**

```bash
cargo test -p market-squawk-live runtime::actor::fairness_tests --locked
cargo test -p market-squawk-live --test runtime_rejection --locked
```

Expected: PASS on the approved Q2 base. Save the exact test totals in the lane handoff.

- [ ] **Step 2: Move scheduling code without semantic edits**

Move `FairTurn`, `FairEvent`, `SnapshotSchedule`, readiness selection, and fairness tests into
`runtime/actor/scheduling.rs`. Preserve enum variants, selection order, cancellation priority, and
snapshot cadence byte-for-byte apart from module paths.

```bash
cargo test -p market-squawk-live runtime::actor::fairness_tests --locked
```

Expected: PASS with the same test names.

- [ ] **Step 3: Move snapshot publication without semantic edits**

Move `publish_snapshot` and its retained-byte accounting into
`runtime/actor/snapshot_publication.rs`. Keep checked arithmetic, deterministic route ordering,
truncation, health emission, and `SnapshotBuildError` mapping unchanged.

```bash
cargo test -p market-squawk-live snapshot --locked
```

Expected: PASS.

- [ ] **Step 4: Extract the processing integration point**

Move `process`, `process_inner`, and applied-observation handling into
`runtime/actor/processing.rs`. The integration function must initially preserve no-strategy behavior:

```rust
fn process_applied_observation(
    owner: &mut RouteOwner,
    applied: AppliedLiveObservation,
) -> Result<(), ActorError> {
    if let Some(authority) = applied.authority.as_ref() {
        owner.processor.validate_applied_current(authority)?;
        owner.processor.validate_applied_current(authority)?;
    }
    Ok(())
}
```

Task 5 replaces the duplicated validation with the live-owned action boundary after feature types
exist.

- [ ] **Step 5: Verify file size and all live tests**

```bash
wc -l crates/market-squawk-live/src/runtime/actor.rs \
  crates/market-squawk-live/src/runtime/actor/*.rs
cargo test -p market-squawk-live --all-features --locked
cargo clippy -p market-squawk-live --all-targets --all-features --locked -- -D warnings
```

Expected: no focused production file exceeds 700 lines; all tests pass.

- [ ] **Step 6: Commit the mechanical split**

```bash
git diff --check
git add crates/market-squawk-live/src/runtime.rs \
  crates/market-squawk-live/src/runtime/actor.rs \
  crates/market-squawk-live/src/runtime/actor
git commit -m "refactor(live): split actor responsibilities"
```

### Task 4: Add typed, capture-bound decoder outcomes

**Owner:** Wave 0 lane C.

**Files:**

- Create: `crates/market-squawk-sources/src/decoder/outcome.rs`
- Modify: `crates/market-squawk-sources/src/decoder.rs`
- Modify: `crates/market-squawk-sources/src/decoder/batch.rs`
- Create: `crates/market-squawk-sources/src/registry/decode_outcome.rs`
- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/authority.rs`
- Modify: `crates/market-squawk-sources/src/lib.rs`
- Create: `crates/market-squawk-sources/tests/decode_outcomes.rs`
- Create: `crates/market-squawk-sources/tests/support/current_source.rs`
- Modify: `crates/market-squawk-sources/tests/registry_authority.rs`

**Interfaces:**

- Consumes: `ValidatedRawMarketFrame`, `DecoderEvidence`, `CaptureAdmissionReceipt`, current source
  authority, and existing `DecodedProviderBatch`.
- Produces: `DecodeOutcome`, `DecodeInternalError`, typed non-data dispositions,
  `ValidatedSessionDecodeOutcome`,
  `ValidatedSourceSession::validate_decode_outcome_owned`, and the data-only current upgrade
  `ValidatedCurrentSourceAuthority::validate_data_outcome_owned`.

- [ ] **Step 1: Extract reusable current-source fixtures**

Move fixture construction from the 1,004-line `registry_authority.rs` test into
`tests/support/current_source.rs` without making production constructors public. Keep the support
module integration-test-only.

```bash
cargo test -p market-squawk-sources --test registry_authority --locked
wc -l crates/market-squawk-sources/tests/registry_authority.rs
```

Expected: tests pass and the test file is below 700 lines.

- [ ] **Step 2: Write failing outcome-shape tests**

Add tests proving every variant has evidence and checked retained bytes:

```rust
#[test]
fn subscription_ack_is_control_not_an_empty_market_batch() -> TestResult {
    let fixture = CurrentSourceFixture::healthy()?;
    let (validated, receipt) = fixture.captured_text_frame(SUBSCRIPTION_ACK)?;
    let outcome = TestDecoder::default().decode(&validated)?;
    assert!(matches!(outcome, DecodeOutcome::Control(_)));
    assert!(outcome.retained_bytes()? > 0);
    let current = fixture
        .validated_session()?
        .validate_decode_outcome_owned(outcome, receipt)?;
    assert!(matches!(current, ValidatedSessionDecodeOutcome::Control(_)));
    Ok(())
}

#[test]
fn recovery_and_quarantine_do_not_produce_current_batches() -> TestResult {
    for outcome in [recovery_outcome()?, quarantine_outcome()?] {
        assert!(!matches!(outcome, DecodeOutcome::Data(_)));
    }
    Ok(())
}
```

- [ ] **Step 3: Run the tests and verify missing outcome failures**

```bash
cargo test -p market-squawk-sources --test decode_outcomes --locked
```

Expected: FAIL because `DecodeOutcome` and `ValidatedSessionDecodeOutcome` do not exist.

- [ ] **Step 4: Implement closed decoder outcomes**

Implement:

```rust
pub enum DecodeOutcome {
    Data(DecodedProviderBatch),
    Control(DecodedControlFrame),
    Ignored(DecodedIgnoredFrame),
    Resynchronize(DecodedRecoveryAction),
    Quarantine(DecodedQuarantineAction),
}

pub enum ControlFrameKind {
    SubscriptionAcknowledgement,
    Heartbeat,
    Ping,
    Pong,
    ProviderFlowControl,
}

pub enum IgnoredFrameReason {
    DocumentedForwardCompatibleExtension,
    DocumentedNoOp,
}

pub enum ResynchronizationReason {
    SnapshotRequired,
    ProviderRequestedReset,
    DecoderStateDiscontinuity,
}

pub enum QuarantineReason {
    MalformedPayload,
    SchemaViolation,
    WrongProduct,
    WrongChannel,
    InvalidTimestamp,
    InexactNumericValue,
    NegativeQuantity,
    UnsupportedSemanticChange,
    ProtocolInvariantViolation,
}
```

Every non-data variant owns `DecoderEvidence` plus closed/bounded metadata. None copies the raw
payload. `DecodeOutcome::retained_bytes` performs checked shallow and deep accounting.

- [ ] **Step 5: Change the object-safe decoder trait**

```rust
pub trait MarketDecoder: SourceMetadataProvider {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError>;
}
```

Keep provider-input failures as typed recovery/quarantine outcomes. Reserve `DecodeInternalError`
for allocation, retained-size overflow, or implementation invariant failure.

- [ ] **Step 6: Validate every outcome against exact session/capture authority**

Implement:

```rust
pub enum ValidatedSessionDecodeOutcome {
    Data(CapturedDecodedProviderBatch),
    Control(SessionControlDisposition),
    Ignored(SessionIgnoredDisposition),
    Resynchronize(SessionRecoveryDisposition),
    Quarantine(SessionQuarantineDisposition),
}
```

`validate_decode_outcome_owned` must compare shared session allocation, metadata revision, session,
generation, frame ID, receive time, payload digest, decoder rule, current lease, and exact capture
lease for every variant. It returns a non-Clone, non-Serde session-bound outcome; its `Data` variant
is only a captured batch and does not yet claim current coverage.

Add the separate data-only upgrade:

```rust
impl ValidatedCurrentSourceAuthority {
    pub fn validate_data_outcome_owned(
        &self,
        captured: CapturedDecodedProviderBatch,
    ) -> Result<CurrentDecodedProviderBatches, CurrentDataValidationError>;
}
```

This second upgrade requires the still-current session, generation, authorization, exact acknowledged
coverage, health, and capture lease. There is no control/ignored/recovery/quarantine current-data
upgrade. Remove `validate_decoded_batch_owned` after all callers migrate in the same task.

- [ ] **Step 7: Add transplant, memory, and Serde-negative tests**

Prove:

- A disposition with another receipt fails `CaptureReceiptMismatch`.
- A reconstructed/deserialized frame cannot create a current disposition.
- Non-data dispositions are not `Clone`, `Serialize`, or `DeserializeOwned` where they carry current
  authority.
- Maximum bounded provider labels report exact retained bytes.
- Retained-size overflow returns a typed error.
- Control and ignored outcomes never produce a `CurrentDecodedProviderBatch`.
- A session-valid `Data` value cannot become current before exact subscription coverage is healthy.
- A data value captured before acknowledgement remains rejected after acknowledgement; callers must
  not buffer pre-ack data for later promotion.

```bash
cargo test -p market-squawk-sources --test decode_outcomes --locked
cargo test -p market-squawk-sources --test registry_authority --locked
cargo clippy -p market-squawk-sources --all-targets --all-features --locked -- -D warnings
```

Expected: PASS.

- [ ] **Step 8: Commit the outcome contract**

```bash
git diff --check
git add crates/market-squawk-sources/src/decoder.rs \
  crates/market-squawk-sources/src/decoder/batch.rs \
  crates/market-squawk-sources/src/decoder/outcome.rs \
  crates/market-squawk-sources/src/registry.rs \
  crates/market-squawk-sources/src/registry/authority.rs \
  crates/market-squawk-sources/src/registry/decode_outcome.rs \
  crates/market-squawk-sources/src/lib.rs \
  crates/market-squawk-sources/tests/decode_outcomes.rs \
  crates/market-squawk-sources/tests/registry_authority.rs \
  crates/market-squawk-sources/tests/support/current_source.rs
git commit -m "feat(sources): add capture-bound decode outcomes"
```

### Task 5: Integrate Wave 0 and freeze nonempty Q3 crate contracts

**Owner:** Integration owner after Tasks 1–4 are reviewed. This task serializes all root manifest,
lockfile, live-action, and adapter-boundary changes.

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `crates/market-squawk-analytics/Cargo.toml`
- Create: `crates/market-squawk-analytics/src/lib.rs`
- Create: `crates/market-squawk-analytics/src/value.rs`
- Create: `crates/market-squawk-analytics/src/registry.rs`
- Create: `crates/market-squawk-analytics/tests/contracts.rs`
- Create: `crates/market-squawk-execution/Cargo.toml`
- Create: `crates/market-squawk-execution/src/lib.rs`
- Create: `crates/market-squawk-execution/src/intent.rs`
- Create: `crates/market-squawk-execution/src/adapter.rs`
- Create: `crates/market-squawk-execution/tests/contracts.rs`
- Create: `adapters/market-squawk-adapter-coinbase/Cargo.toml`
- Create: `adapters/market-squawk-adapter-coinbase/src/lib.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/config.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/contracts.rs`
- Create: `adapters/market-squawk-adapter-paper/Cargo.toml`
- Create: `adapters/market-squawk-adapter-paper/src/lib.rs`
- Create: `adapters/market-squawk-adapter-paper/src/config.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/contracts.rs`
- Create: `crates/market-squawk-live/src/action.rs`
- Modify: `crates/market-squawk-live/src/lib.rs`
- Modify: `crates/market-squawk-live/src/authority.rs`
- Modify: `crates/market-squawk-live/src/processor.rs`
- Modify: `crates/market-squawk-live/src/runtime/actor/processing.rs`
- Create: `crates/market-squawk-live/tests/action_boundary.rs`
- Modify: `crates/market-squawk-live/tests/authority_privacy.rs`

**Interfaces:**

- Consumes: Tasks 1–4, approved domain execution IDs, committed live events, applied authority, and
  checked book views.
- Produces: nonempty analytics/execution/Coinbase/paper packages; `FeatureValue`,
  `FeatureValidity`, `FeatureMetadata`, `LiveActionHook`, `CommittedActionContext`,
  `CurrentAuthorityGate`, `OrderIntent`, `ExecutionAdapter`, private-field `DispatchOrder`,
  `ExecutionMarketUpdate`, pinned Coinbase configuration, and realistic-paper configuration.

- [ ] **Step 1: Write cross-crate compile and privacy tests first**

Add tests that import the intended types and Trybuild fixtures proving downstream code cannot create
the authority gate or dispatch value:

```rust
use market_squawk_execution::DispatchOrder;

fn bypass() {
    let _ = DispatchOrder {};
}
```

```rust
use market_squawk_live::CurrentAuthorityGate;

fn bypass() {
    let _ = CurrentAuthorityGate {};
}
```

Run before creating the packages:

```bash
cargo test -p market-squawk-analytics --test contracts --locked
cargo test -p market-squawk-execution --test contracts --locked
```

Expected: FAIL because the packages do not exist.

- [ ] **Step 2: Register all four nonempty packages atomically**

Replace the stale root comment that says historical Task 11 will add `adapters/*`, add
`adapters/*` to workspace members in this task, and add these workspace dependencies:

```toml
market-squawk-analytics = { path = "crates/market-squawk-analytics", version = "=0.1.0" }
market-squawk-execution = { path = "crates/market-squawk-execution", version = "=0.1.0" }
market-squawk-adapter-coinbase = { path = "adapters/market-squawk-adapter-coinbase", version = "=0.1.0" }
market-squawk-adapter-paper = { path = "adapters/market-squawk-adapter-paper", version = "=0.1.0" }
```

Every new manifest contains:

```toml
[package]
name = "market-squawk-analytics"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lints]
workspace = true
```

Use the same inherited fields for the other manifests with these exact names:
`market-squawk-execution`, `market-squawk-adapter-coinbase`, and
`market-squawk-adapter-paper`.

- [ ] **Step 3: Implement foundational analytics values and metadata**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureValidity {
    Ready,
    WarmingUp,
    Unavailable,
    Overflow,
    TimestampRegression,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureValue<T> {
    value: Option<T>,
    observed_at: Timestamp,
    validity: FeatureValidity,
}
```

Constructors enforce `Some(value)` only for `Ready`; invalid/warming states carry no stale value.
`FeatureMetadata` records the complete registry dimensions from the design and validates bounded
names, nonzero versions, units, warm-up, null policy, live/PIT flags, and implementation revision.

- [ ] **Step 4: Implement the live-owned action seam**

`CommittedActionContext<'a>` borrows the canonical event, assessment identity, route, committed
market reference, and analytics feature view. `CurrentAuthorityGate<'actor>` contains only private
borrows of the exact processor and applied authority plus a `PhantomData<Rc<()>>` so it is neither
`Send` nor `Sync`.

```rust
pub trait LiveActionHook: Send + std::fmt::Debug {
    fn on_committed(
        &mut self,
        context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition;

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError>;
}
```

The gate delegates `issue` and `consume` to the exact processor. Add
`ConsumedLiveAuthority::valid_until() -> Timestamp`. Do not expose `AuthorityGate`,
`AppliedObservationAuthority`, a constructor, or a caller-selected clock.

- [ ] **Step 5: Freeze intent and adapter boundary types**

`OrderIntent::try_new` accepts typed identities, instrument, side/type, quantity, optional limit and
stop, TIF, signal/expiration, bounded reason codes, maximum slippage, required quality, and client
order ID. It rejects every inconsistent field combination.

```rust
pub trait ExecutionAdapter: Send + Sync + std::fmt::Debug {
    fn submit(
        &self,
        order: DispatchOrder,
    ) -> BoxFuture<'_, Result<ExecutionReceipt, ExecutionError>>;

    fn cancel(
        &self,
        order_id: &OrderId,
    ) -> BoxFuture<'_, Result<CancelReceipt, ExecutionError>>;

    fn reconcile(&self) -> BoxFuture<'_, Result<ExecutionState, ExecutionError>>;
}
```

`DispatchOrder` is public only for the trait signature; all fields and constructors are private and
it implements neither `Clone` nor Serde. It owns a bounded `ExecutionMarketReference` with top/depth,
observed time, quality, authority evidence, and immutable `InstrumentExecutionTerms`. The terms bind
the instrument ID and definition revision to exact price tick, lot size, quote currency, settlement
currency, and positive exact contract multiplier. Expose validated read-only accessors for order ID,
client order ID, account, instrument, definition revision, execution terms, side, order type,
quantity, prices, time-in-force, expiry, market reference, and audit/ruleset identity so an adapter
can implement the trait without reconstructing authority. `ExecutionMarketUpdate` is private-field
and constructible only inside execution; expose read-only accessors for instrument, definition
revision, execution terms, exact market reference, observed time, quality, and current binding
identity. Neither type exposes its constructor or mutable fields. Construction rejects a transplanted
instrument/revision, inexact tick or lot scale, nonpositive multiplier, unsupported settlement, or
currency mismatch before authority can reach an adapter.

- [ ] **Step 6: Freeze pinned Coinbase and realistic-paper configuration**

Coinbase configuration accepts only `wss://ws-feed.exchange.coinbase.com`, bounded `level2`,
`matches`, and `heartbeat` subscriptions, exact product mappings, frame ceiling, freshness policy,
and shared-budget scope. Its metadata constant is `DataQuality::DirectUnverified`.

Paper configuration requires nonzero command/update count and byte capacities, seed, latency bounds,
fee schedule, slippage bound, depth/participation bound, account currency, and audit capacity. It has
an explicit venue-session calendar/time zone for Day expiry, closed stop-trigger reference policy,
maker/taker classification policy, supported settlement currencies, and maximum recovery/journal
state bounds. It has no compatibility-only mode that can be selected as realistic behavior.

- [ ] **Step 7: Run compile, privacy, boundary, and lockfile tests**

After the manifest merge, perform the one intentional unlocked minimal resolution:

```bash
cargo check --workspace --all-features
git diff -- Cargo.toml Cargo.lock
```

Expected: only the four reviewed Q3 packages and explicitly reviewed dependencies are added. Do not
run `cargo generate-lockfile`; it may update the whole graph. Use `cargo update -p NAME --precise
VERSION` only when a separately documented review requires one exact targeted change.

Then return to locked commands:

```bash
cargo test -p market-squawk-analytics --test contracts --locked
cargo test -p market-squawk-execution --test contracts --locked
cargo test -p market-squawk-adapter-coinbase --test contracts --locked
cargo test -p market-squawk-adapter-paper --test contracts --locked
cargo test -p market-squawk-live --test action_boundary --test authority_privacy --locked
python3 scripts/check_workspace_boundaries.py
cargo metadata --format-version 1 --no-deps --locked >/dev/null
```

Expected: PASS; all packages are nonempty and every forbidden dependency/constructor test remains
negative.

- [ ] **Step 8: Verify the Wave 0 candidate, commit, and verify exact HEAD**

Run the pre-commit Wave candidate gate from the plan header and confirm `git status --short` contains
only the reviewed Wave 0 paths. Then:

```bash
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-analytics \
  crates/market-squawk-execution crates/market-squawk-live \
  adapters/market-squawk-adapter-coinbase adapters/market-squawk-adapter-paper
git commit -m "feat(q3): freeze production crate boundaries"
```

Capture `WAVE_HEAD="$(git rev-parse HEAD)"` and run the complete exact-head Wave gate from the plan
header. Expected: the worktree is clean, HEAD remains `WAVE_HEAD`, and every command exits zero.

---

## Wave 1 — parallel production cores

### Task 6: Implement every required pure live-feature kernel and registry

**Owner:** Wave 1 lane A; owns only `crates/market-squawk-analytics`.

**Files:**

- Create: `crates/market-squawk-analytics/src/book.rs`
- Create: `crates/market-squawk-analytics/src/trade.rs`
- Create: `crates/market-squawk-analytics/src/rolling.rs`
- Create: `crates/market-squawk-analytics/src/liquidity.rs`
- Create: `crates/market-squawk-analytics/src/cross_venue.rs`
- Modify: `crates/market-squawk-analytics/src/registry.rs`
- Modify: `crates/market-squawk-analytics/src/lib.rs`
- Create: `crates/market-squawk-analytics/tests/book_features.rs`
- Create: `crates/market-squawk-analytics/tests/trade_features.rs`
- Create: `crates/market-squawk-analytics/tests/rolling_features.rs`
- Create: `crates/market-squawk-analytics/tests/liquidity_features.rs`
- Create: `crates/market-squawk-analytics/tests/feature_properties.rs`
- Create: `crates/market-squawk-analytics/tests/registry.rs`

**Interfaces:**

- Consumes: immutable bounded scaled book/trade views, `PriceTicks`, `QuantityLots`, `Timestamp`,
  and frozen `FeatureValue`/metadata types.
- Produces: pure kernels for spread, midpoint, microprice, book imbalance, order-flow imbalance,
  depth-weighted price, aggressor imbalance, VWAP, volume velocity, momentum, rolling returns,
  rolling volatility, cross-venue divergence, liquidity, and slippage.

- [ ] **Step 1: Port black-box book-feature expectations as exact scaled tests**

Port the values from `apps/market-squawk/tests/order_book.rs` without importing app types. Add
empty, one-sided, crossed, zero-depth, half-tick midpoint, maximum-depth, and arithmetic overflow
cases.

```rust
#[test]
fn top_features_use_exact_scaled_inputs() -> TestResult {
    let top = TopOfBookView::try_new(
        PriceTicks::new(10_000),
        QuantityLots::new(20)?,
        PriceTicks::new(10_002),
        QuantityLots::new(10)?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let features = top_of_book_features(top)?;
    assert_eq!(features.spread().ready_value(), Some(PriceTicks::new(2)));
    assert_eq!(features.midpoint().ready_value(), Some(HalfTickPrice::new(20_002)?));
    Ok(())
}
```

- [ ] **Step 2: Run the book test and confirm missing-kernel failure**

```bash
cargo test -p market-squawk-analytics --test book_features --locked
```

Expected: FAIL because book feature views and kernels do not exist.

- [ ] **Step 3: Implement exact book kernels**

Use checked `i128` intermediates and explicit rational/fixed output types where midpoint or weighted
price is not an integer tick. Never round to an executable price inside a feature kernel. Implement
spread, midpoint, microprice, book imbalance, order-flow imbalance, and depth-weighted price.

```bash
cargo test -p market-squawk-analytics --test book_features --locked
```

Expected: PASS.

- [ ] **Step 4: Write failing trade and rolling-window tests**

Cover buy/sell/unknown aggressor treatment, warm-up, exact VWAP numerator/denominator, volume velocity,
momentum, returns, volatility, timestamp regression, duplicate time, zero price, capacity edge, and
overflow.

```bash
cargo test -p market-squawk-analytics --test trade_features \
  --test rolling_features --locked
```

Expected: FAIL because trade and rolling kernels do not exist.

- [ ] **Step 5: Implement bounded rolling kernels**

Rolling constructors require nonzero observation and duration bounds. Updates reject time regression,
perform checked eviction work proportional to the configured bound, and report `WarmingUp`,
`TimestampRegression`, or `Overflow` without returning a stale value. Returns/volatility use an
explicit `StatisticalF64` conversion boundary; price/order APIs do not accept that type.

```bash
cargo test -p market-squawk-analytics --test trade_features \
  --test rolling_features --locked
```

Expected: PASS.

- [ ] **Step 6: Write and implement liquidity, slippage, and cross-venue kernels**

Test side-aware walking of bounded price levels, insufficient depth, requested quantity zero,
weighted fill price, slippage basis points, stale/missing venues, and venue-count bounds. Implement
pure calculations only; queue ownership belongs to live Task 10.

```bash
cargo test -p market-squawk-analytics --test liquidity_features --locked
```

Expected: PASS after the implementation and FAIL before it.

- [ ] **Step 7: Register every feature and reject metadata conflicts**

Add one exact metadata record for each required feature. Test duplicate identical insertion is
idempotent, conflicting same-version metadata is rejected, every live feature declares units and
warm-up, and no statistical output claims it is an order price.

```bash
cargo test -p market-squawk-analytics --test registry --locked
```

Expected: PASS.

- [ ] **Step 8: Add property tests**

Property-test book translation, positive scaling, imbalance range, VWAP convex-hull bounds,
side-symmetry, rolling capacity, timestamp monotonicity, and slippage monotonicity. Compare exact
arithmetic against independent `BigInt` rational oracles at overflow boundaries.

```bash
cargo test -p market-squawk-analytics --test feature_properties --locked
cargo clippy -p market-squawk-analytics --all-targets --all-features --locked -- -D warnings
```

Expected: PASS.

- [ ] **Step 9: Commit analytics without root files**

```bash
git diff --check
git add crates/market-squawk-analytics
git commit -m "feat(analytics): implement complete live features"
```

### Task 7: Implement authoritative account coordination and deterministic risk

**Owner:** Wave 1 lane B; owns only `crates/market-squawk-execution`.

**Files:**

- Create: `crates/market-squawk-execution/src/account.rs`
- Create: `crates/market-squawk-execution/src/limits.rs`
- Create: `crates/market-squawk-execution/src/risk.rs`
- Create: `crates/market-squawk-execution/src/clock.rs`
- Modify: `crates/market-squawk-execution/src/intent.rs`
- Modify: `crates/market-squawk-execution/src/lib.rs`
- Create: `crates/market-squawk-execution/tests/intent.rs`
- Create: `crates/market-squawk-execution/tests/account_reservations.rs`
- Create: `crates/market-squawk-execution/tests/risk_matrix.rs`
- Create: `crates/market-squawk-execution/tests/risk_properties.rs`

**Interfaces:**

- Consumes: frozen intent/action/authority types and exact financial values.
- Produces: `RiskLimits`, `AccountRiskCoordinator`, private `AccountRiskReservation`,
  `RiskRejectionCode`, `RiskRejection`, `RiskService`, and a crate-private sealed trusted clock.

- [ ] **Step 1: Write the complete intent matrix**

Test every `OrderType`/limit/stop combination, TIF constraint, nonpositive quantity, signal/expiry
ordering, maximum slippage, reason count/size, required quality, and client-order idempotency.

```bash
cargo test -p market-squawk-execution --test intent --locked
```

Expected: FAIL until `OrderIntent::try_new` enforces every matrix row.

- [ ] **Step 2: Implement validated typed intents**

Keep all fields private and expose borrowed/copy accessors. Compute a stable input digest over every
field using an explicitly versioned canonical order. Deserialization, if supported for CLI input,
must call the constructor and never deserialize `ApprovedOrder` or `DispatchOrder`.

```bash
cargo test -p market-squawk-execution --test intent --locked
```

Expected: PASS.

- [ ] **Step 3: Write concurrent account-reservation tests**

Create two intents on different simulated shards whose combined notional/position violates one
account limit. Assert only one reservation succeeds. Add stale revision, insufficient cash,
insufficient position, leverage, capital, loss, drawdown, rate, duplicate client order, expiration,
capacity, lock contention, release, commit, and arithmetic overflow cases. Contention must return a
typed fail-closed reason immediately and leave state unchanged; the live actor may not wait on an
unbounded account lock or response queue.

```bash
cargo test -p market-squawk-execution --test account_reservations --locked
```

Expected: FAIL because the coordinator does not exist.

- [ ] **Step 4: Implement fixed-capacity authoritative account state**

`AccountRiskCoordinator` owns balances, positions, reservations, exposure, capital, losses,
drawdown, rate windows, and duplicate IDs. A successful `try_reserve` returns a private non-Serde,
non-Clone `AccountRiskReservation` bound to account ID, account revision, order digest, reserved cash
or quantity, and inclusive expiry. Drop/rejection releases; known fills commit; uncertain outcomes
enter reconciliation-required state.

Partition accounts by stable account hash into a fixed number of startup-sized single-writer
critical sections. The synchronous hot path uses nonblocking acquisition; contention yields
`AccountCoordinatorBusy`, zero mutation, and bounded health/audit rather than blocking an instrument
actor. All structures and worst-case per-attempt work are bounded by validated capacities, and a
reservation plus account revision publishes atomically inside one critical section.

The coordinator must never accept a caller-created current position/balance struct as authority.

```bash
cargo test -p market-squawk-execution --test account_reservations --locked
```

Expected: PASS.

- [ ] **Step 5: Write the all-reasons risk matrix**

Port the old stale/invalid/kill-switch negative fixtures, then cover source quality/freshness,
instrument/account eligibility, position, notional, exposure, leverage, capital, price, slippage,
rate, duplicate, loss, drawdown, intent expiry, checked arithmetic, and combined rejections. Assert
all applicable typed reasons are returned in stable order.

```bash
cargo test -p market-squawk-execution --test risk_matrix --locked
```

Expected: FAIL because `RiskService` is incomplete.

- [ ] **Step 6: Implement deterministic risk without caller time/account authority**

Production `RiskService` owns a reference/handle to `AccountRiskCoordinator` and a sealed
wall-plus-monotonic clock. Its public evaluation method does not accept `now`, current position,
balance, or a caller-authored account snapshot. It stages every fallible source, market, intent,
limit, account, arithmetic, audit-capacity, and deadline check without mutating account state, and
returns all reasons in stable order. When no reason exists, it atomically commits the still-current
account revision, reservation, and internal approval candidate using the staged plan. A revision
race returns `AccountStateChanged`; combined-reason evaluation never leaves a partial reservation.

```bash
cargo test -p market-squawk-execution --test risk_matrix --locked
```

Expected: PASS.

- [ ] **Step 7: Property-test reservations and arithmetic**

Test that accepted reservations never exceed configured aggregate limits under arbitrary order,
release is idempotent only through its state machine, revision changes revoke stale reservations,
and checked notional/fee arithmetic matches an independent integer oracle.

```bash
cargo test -p market-squawk-execution --test risk_properties --locked
cargo clippy -p market-squawk-execution --all-targets --all-features --locked -- -D warnings
```

Expected: PASS.

- [ ] **Step 8: Commit the risk core without app/root files**

```bash
git diff --check
git add crates/market-squawk-execution
git commit -m "feat(execution): add authoritative account risk"
```

### Task 8: Implement the pinned Coinbase Exchange adapter and persist provider research

**Owner:** Wave 1 lane C; owns the Coinbase adapter and its provider research file.

**Files:**

- Create: `adapters/market-squawk-adapter-coinbase/src/decoder.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/source.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/source/tests.rs`
- Modify: `adapters/market-squawk-adapter-coinbase/src/config.rs`
- Modify: `adapters/market-squawk-adapter-coinbase/src/lib.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/decode.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/metadata.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/public_endpoint.rs`
- Create: `docs/research/providers/coinbase-exchange-websocket-2026-07-16.md`

**Interfaces:**

- Consumes: `LiveMarketSource`, `MarketDecoder`, `DecodeOutcome`, `RawFrameFactory`,
  `RawMarketSink`, `SharedProviderBudget`, endpoint policy, and explicit instrument mapping.
- Produces: `CoinbaseExchangeDecoder`, `CoinbaseExchangeSource`, exact Exchange v1 metadata,
  deterministic wire fixtures, and persisted official-source rationale.

- [ ] **Step 1: Persist the source decision before implementation**

Write `docs/research/providers/coinbase-exchange-websocket-2026-07-16.md` with:

```text
retrieval date: 2026-07-16
selected protocol: Coinbase Exchange WebSocket v1
endpoint: wss://ws-feed.exchange.coinbase.com
channels: level2, matches, heartbeat
coverage: one venue, configured products/channels only
quality ceiling: DirectUnverified
level2 sequence qualification: unsupported by the selected contract
checksum qualification: unsupported
trade completeness: matches may be dropped
heartbeat semantics: connection/feed health, not market freshness
```

Cite and summarize without copying large passages:

- <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels>
- <https://docs.cdp.coinbase.com/exchange/websocket-feed/best-practices>
- <https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/guides/websocket>
- <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels>
- <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview>

Explain that Advanced Trade is a separate future protocol profile and its schemas are not mixed into
this adapter.

- [ ] **Step 2: Port every current fixture and add adversarial cases**

Port snapshot, `l2update`, maker-side `match`, heartbeat, and invalid timestamp fixtures from the app.
Add subscription acknowledgement, documented unknown extension, malformed/duplicate fields,
inexact decimal, negative quantity, wrong product/channel, binary frame, oversized frame, and
provider error cases.

```bash
cargo test -p market-squawk-adapter-coinbase --test decode --locked
```

Expected: FAIL because the decoder is not implemented.

- [ ] **Step 3: Implement exact typed decoding**

Use strict wire structs with unknown/duplicate-field rejection. Preserve bounded exact
`ProviderDecimalLexeme`; do not convert through tick/lot. Emit:

```text
snapshot/l2update/match -> DecodeOutcome::Data
subscription acknowledgement/heartbeat -> DecodeOutcome::Control
documented safe extension -> DecodeOutcome::Ignored
provider reset/snapshot request -> DecodeOutcome::Resynchronize
schema/product/channel/numeric violation -> DecodeOutcome::Quarantine
```

Heartbeat carries connection evidence but never updates price freshness.

```bash
cargo test -p market-squawk-adapter-coinbase --test decode --locked
```

Expected: PASS.

- [ ] **Step 4: Write deterministic one-generation source unit tests with private transport injection**

Inside private module unit tests at `src/source/tests.rs`, inject a sealed in-memory/local transport
into the private source implementation. The local server asserts exact subscription JSON, sends
control/data/ping/pong/close frames, and records received raw frames. Tests assert capture occurs
before decode, cancellation terminates, a close returns a typed terminal result, and the adapter
never creates a second connection with the same `RawFrameFactory`.

Public `CoinbaseExchangeSource` constructors always build the validated production connector and
accept only the allowlisted `wss://ws-feed.exchange.coinbase.com`. Do not expose a connector, URL,
loopback, TLS, or test-support override in a public API or Cargo feature. The private test seam is
compiled only for this module's unit tests and is absent from dependency/all-features builds.

```bash
cargo test -p market-squawk-adapter-coinbase --lib --locked source::tests
```

Expected: FAIL until source behavior exists, then PASS.

- [ ] **Step 5: Implement lawful bounded source behavior**

Validate the endpoint through `EndpointPolicy`, enforce redirect/host/TLS allowlists, maximum frame
size before buffering, cancellation on connect/read/write, and bounded subscription count/bytes.
Acquire permits from the exact registry-shared budget handle. Return typed refusal/backoff/network/
authorization/protocol outcomes to the supervisor; do not rotate identities, accounts, proxies,
fingerprints, or quotas.

- [ ] **Step 6: Prove metadata and quality ceiling**

```bash
cargo test -p market-squawk-adapter-coinbase --test metadata --locked
cargo test -p market-squawk-sources --test network_policy --locked
```

Expected: metadata reports single-venue partial subscribed coverage, real-time delivery without a
completeness guarantee, unsupported book sequence/checksum qualification, and
`DirectUnverified`. Network policy has no evasion surface.

- [ ] **Step 7: Add an opt-in availability test**

`public_endpoint.rs` is `#[ignore]` and runs only with `MARKET_SQUAWK_NETWORK_TESTS=1`. It validates
the allowlisted handshake and treats documented provider unavailability/refusal as an inspected
outcome, not a deterministic-suite failure.

Add public contract tests proving loopback/custom endpoints and connector injection are unavailable
or rejected, including with `--all-features`; only the ignored real-endpoint test performs external
I/O.

```bash
cargo test -p market-squawk-adapter-coinbase --test public_endpoint --locked -- --ignored
```

Expected without the environment opt-in: the test exits without making a network request or remains
ignored according to the harness contract.

- [ ] **Step 8: Verify and commit adapter plus research**

```bash
cargo test -p market-squawk-adapter-coinbase --all-features --locked
cargo clippy -p market-squawk-adapter-coinbase --all-targets --all-features --locked -- -D warnings
git diff --check
git add adapters/market-squawk-adapter-coinbase \
  docs/research/providers/coinbase-exchange-websocket-2026-07-16.md
git commit -m "feat(coinbase): add pinned Exchange v1 adapter"
```

### Task 9: Integrate Wave 1 and minimally resolve the single root lockfile

**Owner:** Integration owner.

**Files:**

- Modify: `Cargo.toml` only if a reviewed dependency feature is missing.
- Modify: `Cargo.lock`
- Modify: merged files only to resolve reviewed API conflicts; no behavior expansion.

**Interfaces:**

- Consumes: Tasks 6–8 lane commits, excluding their lockfiles.
- Produces: one runnable integrated Wave 1 with stable analytics, risk, and Coinbase contracts.

- [ ] **Step 1: Merge in dependency order**

```text
analytics
→ execution risk
→ Coinbase adapter
```

Resolve conflicts from the design and frozen interfaces; do not choose a side solely because it
applied cleanly.

- [ ] **Step 2: Resolve minimally and inspect the root lockfile**

```bash
cargo check --workspace --all-features
git diff -- Cargo.toml Cargo.lock
cargo metadata --format-version 1 --no-deps --locked >/dev/null
```

Expected: only reviewed Q3 packages/dependencies changed; no duplicate unreviewed protocol stack was
introduced. The first command is the wave's one intentional unlocked resolution. Do not run
`cargo generate-lockfile`; use a targeted `cargo update -p NAME --precise VERSION` only with an
explicit dependency-review note.

- [ ] **Step 3: Run the pre-commit Wave candidate gate**

Run every substantive command in the Wave candidate gate. Expected: all local commands exit zero and
`git status --short` contains exactly the reviewed Wave 1 paths.

- [ ] **Step 4: Commit the integrated wave**

```bash
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-analytics \
  crates/market-squawk-execution adapters/market-squawk-adapter-coinbase \
  docs/research/providers/coinbase-exchange-websocket-2026-07-16.md
git commit -m "feat(q3): integrate analytics risk and Coinbase cores"
```

Capture `WAVE_HEAD="$(git rev-parse HEAD)"` and run the complete exact-head Wave gate from the plan
header. Expected: the worktree is clean, HEAD remains `WAVE_HEAD`, and every command exits zero.

---

## Wave 2 — live integration, production composition, and realistic paper execution

### Task 10: Own every live feature in bounded route and cross-venue state

**Owner:** Wave 2 lane A. It owns live feature/cross-venue files and coordinates actor edits only
through the integration owner.

**Files:**

- Create: `crates/market-squawk-live/src/features.rs`
- Create: `crates/market-squawk-live/src/cross_venue.rs`
- Modify: `crates/market-squawk-live/src/processor.rs`
- Modify: `crates/market-squawk-live/src/runtime/actor/processing.rs`
- Modify: `crates/market-squawk-live/src/runtime/config.rs`
- Modify: `crates/market-squawk-live/src/runtime/memory.rs`
- Modify: `crates/market-squawk-live/src/snapshot.rs`
- Modify: `crates/market-squawk-live/src/processor/snapshot.rs`
- Modify: `crates/market-squawk-live/src/lib.rs`
- Create: `crates/market-squawk-live/tests/feature_state.rs`
- Create: `crates/market-squawk-live/tests/cross_venue_features.rs`
- Create: `crates/market-squawk-live/tests/feature_memory.rs`
- Modify: `crates/market-squawk-live/tests/runtime_rejection.rs`

**Interfaces:**

- Consumes: all analytics kernels and registry metadata, canonical committed live events, source
  generation/status state, actor route ownership, immutable snapshots, and the frozen action context.
- Produces: `RouteFeatureState`, `CrossVenueFeatureHub`, `LiveFeatureSnapshot`, configured capacities,
  exact retained-memory charges, reset/invalidation policy, and ready feature views for action hooks.

- [ ] **Step 1: Write failing exact-memory tests before allocating state**

Add tests for the exact incremental charge of one route window, one stream, one cross-venue slot, one
coalescing command, one published feature snapshot, and one retained action hook. Test the configured
ceiling at equality and one byte below.

```bash
cargo test -p market-squawk-live --test feature_memory --locked
```

Expected: FAIL because feature capacities are absent from `LiveRuntimeConfig` and the peak model.

- [ ] **Step 2: Add validated feature capacity configuration**

Extend `LiveRuntimeConfigInput` and checked `LiveRuntimeConfig` with:

```text
maximum_feature_window_observations_per_route
maximum_feature_window_bytes_per_route
maximum_feature_sets_per_route
cross_venue_command_count
cross_venue_command_bytes
maximum_cross_venue_instruments
maximum_venues_per_cross_venue_instrument
maximum_feature_snapshot_bytes
maximum_action_hook_bytes_per_route
```

Every field is nonzero, has a documented hard maximum, participates in checked multiplication/sum,
and is included in `estimated_peak_bytes` before any actor spawn.

```bash
cargo test -p market-squawk-live --test feature_memory --locked
```

Expected: exact memory tests PASS.

- [ ] **Step 3: Write failing route-state update/reset tests**

Test snapshot initialization, book delta, quote, and trade updates; warm-up progression; sequence of
feature timestamps; generation rollover; resynchronization; quarantine; halt; source replacement;
timestamp regression; overflow; capacity/accounting failure after a committed observation; and
recovery only after a fresh valid snapshot where required. Prove a post-commit feature failure does
not roll back committed book/trade state, terminate the actor, or expose a stale ready value.

```bash
cargo test -p market-squawk-live --test feature_state --locked
```

Expected: FAIL because `RouteFeatureState` does not exist.

- [ ] **Step 4: Implement route-owned bounded feature state**

Construct all windows at startup from checked capacities. Update only after candidate-and-commit live
state succeeds. A rejected observation leaves feature state byte-for-byte unchanged. Once the live
observation is committed, feature mutation is separately transactional: arithmetic, capacity, or
retained-size failure publishes `Unavailable`/`Overflow` for the affected feature set, suppresses
action for that observation, emits bounded health, and preserves the committed market state. It is
not a rollback request or actor-fatal error, and subsequent valid observations continue bounded
processing. DirectUnverified events may update features but never create an execution-ready action
context. Invalid/warming/stale/overflow features carry no ready value.

```bash
cargo test -p market-squawk-live --test feature_state --locked
```

Expected: PASS.

- [ ] **Step 5: Write failing cross-venue single-writer tests**

Use two venues for one instrument routed to different shards. Test deterministic venue ordering,
coalescing, stale/missing venue invalidation, configured venue/instrument limits, queue count/byte
saturation, generation replacement, snapshot immutability, and no silent omission.

```bash
cargo test -p market-squawk-live --test cross_venue_features --locked
```

Expected: FAIL because the cross-venue hub does not exist.

- [ ] **Step 6: Implement the bounded cross-venue hub**

The hub is one single-writer task/owner. Producers perform nonblocking coalescing publication of a
compact exact top/feature record. Saturation marks the affected cross-venue feature unavailable and
emits bounded health; it does not invalidate an otherwise valid venue book or silently use a partial
venue set. Readers use immutable bounded snapshots and never query another shard's mutable state.

```bash
cargo test -p market-squawk-live --test cross_venue_features --locked
```

Expected: PASS.

- [ ] **Step 7: Publish feature snapshots outside authority**

Extend route snapshots with bounded feature output and `SnapshotDimension` completeness. Feature
snapshots are authority-free, non-Clone where retained-generation bounds require it, and cannot mint
capabilities. Add byte truncation tests and exact retained accounting.

```bash
cargo test -p market-squawk-live snapshot feature --locked
```

Expected: PASS.

- [ ] **Step 8: Integrate feature evaluation at the committed actor point**

Actor order is exact:

```text
commit live observation
→ validate applied authority
→ transactionally update route feature state; on failure publish unavailable/overflow + bounded health
→ read current cross-venue snapshot
→ build feature validity view
→ validate applied authority again
→ invoke action hook only when configured required features are Ready
```

If no strategy is configured, features are not ready, or the feature transaction failed, no
capability is issued. Preserve actor fairness, snapshot trigger, bounded work, and live
current-batch atomicity. Add a regression in `runtime_rejection.rs` that injects a feature overflow,
observes committed market state plus unavailable feature health and zero action calls, then proves
the same actor processes the next valid event.

- [ ] **Step 9: Verify live behavior and commit without app/root files**

```bash
cargo test -p market-squawk-live --all-features --locked
cargo clippy -p market-squawk-live --all-targets --all-features --locked -- -D warnings
wc -l crates/market-squawk-live/src/runtime/actor.rs \
  crates/market-squawk-live/src/runtime/actor/*.rs \
  crates/market-squawk-live/src/features.rs \
  crates/market-squawk-live/src/cross_venue.rs
git diff --check
git add crates/market-squawk-live
git commit -m "feat(live): own bounded complete feature state"
```

Expected: all local checks pass and no focused production file exceeds 700 lines.

### Task 11: Consume live authority through risk and dispatch exactly once

**Owner:** Integration owner during Wave 2. It owns execution/live shared authority and adapter
contract files; no other lane edits them concurrently.

**Files:**

- Create: `crates/market-squawk-execution/src/approval.rs`
- Create: `crates/market-squawk-execution/src/audit.rs`
- Create: `crates/market-squawk-execution/src/dispatcher.rs`
- Create: `crates/market-squawk-execution/src/live_hook.rs`
- Create: `crates/market-squawk-execution/src/strategy.rs`
- Modify: `crates/market-squawk-execution/src/risk.rs`
- Modify: `crates/market-squawk-execution/src/adapter.rs`
- Modify: `crates/market-squawk-execution/src/lib.rs`
- Modify: `crates/market-squawk-live/src/action.rs`
- Modify: `crates/market-squawk-live/src/authority.rs`
- Modify: `crates/market-squawk-live/src/processor.rs`
- Create: `crates/market-squawk-execution/tests/approval_privacy.rs`
- Create: `crates/market-squawk-execution/tests/ui/approved_order_is_private.rs`
- Create: `crates/market-squawk-execution/tests/ui/dispatch_order_is_private.rs`
- Create: `crates/market-squawk-execution/tests/ui/current_gate_is_private.rs`
- Create: `crates/market-squawk-execution/tests/ui/capability_is_single_use.rs`
- Create: `crates/market-squawk-execution/tests/authority_adversarial.rs`
- Create: `crates/market-squawk-execution/tests/dispatch_once.rs`
- Create: `crates/market-squawk-execution/tests/audit_backpressure.rs`

**Interfaces:**

- Consumes: `LiveActionHook`, actor-scoped `CurrentAuthorityGate`, `LiveExecutionCapability`,
  authoritative account reservations, risk limits, complete feature views, and `ExecutionAdapter`.
- Produces: `Strategy`, `ExecutionLiveActionHook`, private `ApprovedOrder`, `RiskOutcome`,
  `ExecutionDispatcher`, bounded audit/handoff queues, and adapter-only private `DispatchOrder`.

- [ ] **Step 1: Write compile-fail bypass tests**

Prove downstream code cannot construct, deserialize, clone, or obtain field access to
`CurrentAuthorityGate`, `LiveExecutionCapability`, `AccountRiskReservation`, `ApprovedOrder`, or
`DispatchOrder`. Prove a domain `QualificationAssessment`, snapshot DTO, replay event, or
caller-authored `DirectVerified` value cannot satisfy the capability parameter.

```bash
cargo test -p market-squawk-execution --test approval_privacy --locked
```

Expected: FAIL until every UI stderr fixture is accepted for the intended privacy error.

- [ ] **Step 2: Write adversarial live-authority tests**

Test exact expiry and `valid_until + 1ns`, wall-clock rollback, monotonic expiry, source metadata
replacement, authorization/coverage expiry, health revocation, generation rollover, shard/runtime
shutdown, stream/status revision change, binding transplant, duplicate capability nonce, and queue
delay. Use the real test-only registry/runtime path; do not add a production capability constructor.

```bash
cargo test -p market-squawk-execution --test authority_adversarial --locked
```

Expected: FAIL because risk does not yet consume actor-owned capability.

- [ ] **Step 3: Implement live capability consumption inside deterministic risk**

Use this public shape without caller time/account state:

```rust
pub fn evaluate(
    &mut self,
    authority_gate: &mut CurrentAuthorityGate<'_>,
    capability: LiveExecutionCapability,
    intent: OrderIntent,
    market: &ExecutionMarketReference,
) -> RiskOutcome;
```

The method calls `authority_gate.consume(capability)` exactly once. It validates the resulting
`ConsumedLiveAuthority`, stages all deterministic and account checks without mutation, reserves
audit capacity, and collects all typed rejection reasons. Only an empty reason set may atomically
publish the still-current account reservation and private approval candidate. Revision or audit
permit races fail closed without partial account mutation.

The approval owns the consumed authority and reservation. Its expiry is the minimum of intent,
authority, authorization/coverage, account reservation, and policy deadlines. The live gate remains
implemented in live; execution never gains access to `AuthorityGate` or applied authority.

```bash
cargo test -p market-squawk-execution --test authority_adversarial --locked
```

Expected: PASS.

- [ ] **Step 4: Write one-time dispatch and uncertainty tests**

Cover duplicate approval ID, queue count saturation, queue byte saturation, approval expiry in queue,
authority revocation after risk, account revision after risk, audit saturation, adapter rejection,
known network failure, uncertain backend outcome, reconciliation requirement, and attempted retry.
Assert the backend invocation counter remains zero on every pre-dispatch failure and one on the
single accepted path.

```bash
cargo test -p market-squawk-execution --test dispatch_once \
  --test audit_backpressure --locked
```

Expected: FAIL because the dispatcher does not exist.

- [ ] **Step 5: Implement the bounded one-time dispatcher**

`ExecutionDispatcher` owns a fixed-capacity approval-ID registry, bounded count/byte command queue,
and bounded audit admission. `try_submit` moves `ApprovedOrder`; no clone/retry API exists. The worker
revalidates consumed authority, account reservation, and trusted-clock expiry immediately before
privately constructing `DispatchOrder` and invoking the adapter.

If audit admission or either queue bound fails, release the reservation and do not call the adapter.
An uncertain result marks account/order reconciliation-required and consumes the approval
permanently.

```bash
cargo test -p market-squawk-execution --test dispatch_once \
  --test audit_backpressure --locked
```

Expected: PASS.

- [ ] **Step 6: Implement bounded strategy-to-intent action hook**

```rust
pub trait Strategy: Send + std::fmt::Debug {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError>;

    fn retained_bytes(&self) -> Result<usize, StrategyError>;
}
```

`ExecutionLiveActionHook` owns bounded strategy state, risk service, and dispatcher handle. For each
bounded intent it requests one capability from the actor gate, moves it into risk, and performs a
nonblocking dispatcher handoff. No strategy calls an adapter. `NoStrategy`, `NoIntent`, rejection,
backpressure, and accepted handoff are typed dispositions.

- [ ] **Step 7: Verify complete authority path and commit**

```bash
cargo test -p market-squawk-execution --all-features --locked
cargo test -p market-squawk-live --test authority_privacy --test action_boundary --locked
cargo clippy -p market-squawk-execution -p market-squawk-live \
  --all-targets --all-features --locked -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff --check
git add crates/market-squawk-execution crates/market-squawk-live
git commit -m "feat(execution): enforce one-time live risk dispatch"
```

### Task 12: Compose Coinbase through the authoritative production pipeline

**Owner:** Wave 2 lane B. It creates focused app/platform composition modules but does not edit
`main.rs`, `lib.rs`, diagnostic engine, bot, risk, or MCP; Wave 3 owns those shared files.

**Files:**

- Create: `apps/market-squawk/src/live_source/mod.rs`
- Create: `apps/market-squawk/src/live_source/instruments.rs`
- Create: `apps/market-squawk/src/live_source/sink.rs`
- Create: `apps/market-squawk/src/live_source/subscription_state.rs`
- Create: `apps/market-squawk/src/live_source/supervisor.rs`
- Create: `apps/market-squawk/src/live_source/composition.rs`
- Create: `apps/market-squawk/src/live_source/tests/mod.rs`
- Create: `apps/market-squawk/src/live_source/tests/pipeline.rs`
- Create: `apps/market-squawk/src/live_source/tests/supervisor.rs`
- Create: `apps/market-squawk/src/live_source/tests/subscription.rs`
- Modify: `crates/market-squawk-platform/src/config.rs`
- Create: `crates/market-squawk-platform/src/config/instruments.rs`
- Create: `apps/market-squawk/tests/production_coinbase_pipeline.rs`
- Modify: `crates/market-squawk-platform/tests/config_precedence.rs`

**Interfaces:**

- Consumes: explicit instrument definitions, authoritative source registry, current source session
  and health, shared budget, raw frame factory, capture publisher/receipt, Coinbase source/decoder,
  `ValidatedSessionDecodeOutcome`, current-data upgrade, `LiveRuntimeComposition`, and route-bound
  ingress.
- Produces: `ProductionLiveSourceComposition`, `ProductionRawMarketSink`, one-generation
  `ProductionSourceSupervisor`, bounded per-generation `SubscriptionStateMachine`, and validated
  Coinbase instrument/subscription configuration.

- [ ] **Step 1: Write failing strict instrument mapping/config tests**

Configuration must map each provider product to an explicit internal `InstrumentId`, Coinbase
`VenueId`, tick size, lot size, asset/currency denomination, subscribed event classes, depth,
freshness, frame ceiling, and endpoint policy. Test missing mapping, duplicate product/instrument,
wrong venue, invalid precision, unknown fields/environment keys, excessive subscription count/bytes,
and precedence.

```bash
cargo test -p market-squawk-platform --test config_precedence --locked
```

Expected: FAIL until typed instrument configuration exists.

- [ ] **Step 2: Implement strict production instrument configuration**

Build `InstrumentDefinition` values only from validated local configuration. Do not generate or
hardcode production `InstrumentId` in the Coinbase adapter. Keep secret debug/error output redacted.

- [ ] **Step 3: Write a sealed deterministic end-to-end composition test**

The private `live_source` module test uses a sealed test-only composition harness to start the real
live runtime and local capture writer, register Coinbase metadata/session/health, obtain the exact
shared budget and raw frame factory, bind route ingress, and drive the adapter's private test
transport with subscription ack plus snapshot/delta/trade/heartbeat. It then reads immutable live
snapshots. Assert:

```text
raw frames were admitted before decode
subscription ack became Control
current coverage remained unhealthy before the exact ack
data before ack failed closed and was never buffered for later promotion
heartbeat did not update price freshness
data became current receipt-validated batches
live snapshot contains the expected book/trade state
quality never exceeds DirectUnverified
no execution capability/action was produced
```

```bash
cargo test -p market-squawk --lib --locked live_source::tests::production_coinbase_pipeline
```

Expected: FAIL because production composition is absent.

`apps/market-squawk/tests/production_coinbase_pipeline.rs` exercises only public safe configuration
and service contracts: the real allowlisted endpoint, typed metadata/coverage, `DirectUnverified`
ceiling, and rejection/unavailability of loopback/custom connector injection. It never starts a
local connector. No `test-support` feature is added to an app production dependency, and
`--all-features` exposes no connector override.

- [ ] **Step 4: Implement capture-first nonblocking sink composition**

For each exact frame:

```text
capture preflight
→ bounded capture enqueue
→ capture receipt issuance
→ current session frame validation
→ synchronous decoder outcome
→ session outcome + receipt validation
→ Control: bounded per-generation subscription/control transition
→ Data before exact ack: fail closed and invalidate generation
→ Data after exact ack: current coverage/data authority upgrade
→ current Data: bounded route ingress publication
→ other non-Data: bounded audit/health/supervisor disposition
```

Capture or ingress saturation invalidates the exact generation. Control/ignored frames do not enter
shards. Recovery/quarantine cannot be ignored.

- [ ] **Step 5: Implement the bounded subscription/control state machine**

The state machine is scoped to exactly one source session and connection generation. It stores the
canonical expected product/channel set, acknowledged set, deadline, transport state, transition
sequence, and bounded counters. Exact acknowledgement is required before the registry may publish
current coverage health. Missing, duplicate, excessive, unknown, or mismatched products/channels;
data-before-ack; deadline expiry; transition overflow; or control/audit count/byte saturation
invalidates the generation. Ping/pong and provider heartbeat update connection/feed liveness only,
never price freshness or current data coverage.

Every queue, set, retained identifier, and counter is included in startup peak-memory accounting.
The private `live_source::tests::subscription` module covers exact ack,
subset/superset/duplicate ack, data-before-ack,
heartbeat-before-ack, ack deadline, reconnect reset, stale-generation control, queue count/byte
saturation, counter overflow, and the invariant that rejected pre-ack data cannot be replayed or
promoted after a later ack.

```bash
cargo test -p market-squawk --lib --locked live_source::tests::subscription
```

Expected: FAIL before the state machine exists and PASS after implementation.

- [ ] **Step 6: Implement supervisor-owned generation lifecycle**

The adapter runs one connection generation. The supervisor alone performs:

```text
stop source
→ invalidate/end old registry session and capture/live bindings
→ apply exact shared budget/backoff/Retry-After
→ create new session and connection generation
→ obtain new RawFrameFactory and capture bundle
→ bind new route ingress before opening the feed
```

Add tests for normal close, cancellation, protocol quarantine, resynchronization, budget refusal,
backoff, capture failure, ingress saturation, subscription/control failure, non-data audit
saturation, abort, stale handle, generation overflow, and attempted same-generation reuse.

```bash
cargo test -p market-squawk --lib --locked live_source::tests::supervisor
```

Expected: PASS after implementation.

- [ ] **Step 7: Verify composition without touching shared app entry points**

```bash
cargo test -p market-squawk --test production_coinbase_pipeline \
  --locked
cargo test -p market-squawk --lib --locked live_source::tests
cargo test -p market-squawk-platform --test config_precedence --locked
cargo clippy -p market-squawk -p market-squawk-platform \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git add apps/market-squawk/src/live_source \
  apps/market-squawk/tests/production_coinbase_pipeline.rs \
  crates/market-squawk-platform/src/config.rs \
  crates/market-squawk-platform/src/config/instruments.rs \
  crates/market-squawk-platform/tests/config_precedence.rs
git commit -m "feat(app): compose authoritative Coinbase pipeline"
```

### Task 13: Implement complete deterministic realistic paper execution

**Owner:** Wave 2 lane C; owns only `adapters/market-squawk-adapter-paper`. It consumes the frozen
execution adapter/market-update contracts and requests root dependency changes from the integrator.

**Files:**

- Create: `adapters/market-squawk-adapter-paper/src/order.rs`
- Create: `adapters/market-squawk-adapter-paper/src/ledger.rs`
- Create: `adapters/market-squawk-adapter-paper/src/fees.rs`
- Create: `adapters/market-squawk-adapter-paper/src/latency.rs`
- Create: `adapters/market-squawk-adapter-paper/src/slippage.rs`
- Create: `adapters/market-squawk-adapter-paper/src/market.rs`
- Create: `adapters/market-squawk-adapter-paper/src/matching.rs`
- Create: `adapters/market-squawk-adapter-paper/src/audit.rs`
- Create: `adapters/market-squawk-adapter-paper/src/state.rs`
- Create: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/config.rs`
- Modify: `adapters/market-squawk-adapter-paper/src/lib.rs`
- Modify: `adapters/market-squawk-adapter-paper/Cargo.toml`
- Create: `adapters/market-squawk-adapter-paper/tests/state_machine.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/ledger.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/realistic_fills.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/order_semantics.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/matching_priority.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/cancellation_races.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/reconciliation.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/risk_gate.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/paper_properties.rs`

**Interfaces:**

- Consumes: dispatcher-only `DispatchOrder`, `ExecutionMarketUpdate`, immutable instrument-definition
  revision/execution terms, cancel/reconcile requests, typed order/financial identities, and frozen
  realistic paper configuration.
- Produces: `PaperExecutionAdapter`, `PaperMarketIngress`, `PaperAuditReader`, `PaperOrderState`,
  owned/reapable paper worker lifecycle, `PaperExecutionSnapshot`, receipts, fills, balances,
  positions, and reconciliation evidence.

- [ ] **Step 1: Write the legal state-transition table first**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaperOrderState {
    New,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelPending,
    Canceled,
    Rejected,
    Expired,
}
```

Test every legal edge and reject every other pair transactionally. Explicitly cover partial-fill to
cancel-pending/canceled, cancel/fill races, expiry, rejection, and terminal-state immutability.

```bash
cargo test -p market-squawk-adapter-paper --test state_machine --locked
```

Expected: FAIL because the state machine does not exist.

- [ ] **Step 2: Implement the explicit order state machine**

Transitions are methods with typed errors, monotonic order revision, deterministic event sequence,
and checked cumulative quantity. No public setter or Serde path can force a state.

```bash
cargo test -p market-squawk-adapter-paper --test state_machine --locked
```

Expected: PASS.

- [ ] **Step 3: Write exact ledger, reservation, and fee tests**

Cover multi-currency cash, reserved cash/quantity, long and short position policy, maker/taker fees,
minimum/maximum fee, partial fills, realized cash flow, insufficient cash/position, explicit
rounding, and every overflow boundary. Include non-unit tick/lot scales and contract multipliers;
quote/settlement currency mismatch; instrument-definition revision transplant; execution terms from
another instrument; unsupported non-currency settlement; and exact checked conversion of
`price_ticks × price_tick × quantity_lots × lot_size × contract_multiplier` into quote `Money`.
Assert every rejection leaves the ledger byte-for-byte unchanged.

```bash
cargo test -p market-squawk-adapter-paper --test ledger --locked
```

Expected: FAIL because the ledger and fee schedule do not exist.

- [ ] **Step 4: Implement checked single-writer ledger accounting**

All balances/fees/fills use `Money`, scaled quantity, immutable definition-revision-bound execution
terms, explicit quote/settlement currency, and configured rounding. Submit reserves resources; fills
consume reservations; cancel/reject/expiry releases them; uncertain outcomes remain reserved until
reconciliation. Reject a term/revision/scale/currency transplant before reservation. Never use
floating point or assume a tick is a currency minor unit.

```bash
cargo test -p market-squawk-adapter-paper --test ledger --locked
```

Expected: PASS.

- [ ] **Step 5: Write the complete order, TIF, latency, and liquidity-allocation matrix**

Use fixed seeds and paused time. Cover market, crossing limit, resting limit, stop trigger, and
stop-limit activation for both sides; Day, GTC, IOC, and FOK; latency-before-eligibility; session-day
expiry; maker/taker classification; stable price/time priority; maximum participation/slippage;
partial/resting continuation; and stale/invalid market updates. Feed multiple competing orders one
bounded depth/update and prove the available quantity is allocated once in price/time order—no two
orders may fill the same displayed liquidity. Assert identical seed/input yields identical
latency/fills and a different seed remains within configured bounds.

```bash
cargo test -p market-squawk-adapter-paper --test realistic_fills \
  --test order_semantics \
  --test matching_priority --locked
```

Expected: FAIL because the simulation models are absent.

- [ ] **Step 6: Implement stable seeded simulation models**

Use the pinned `ChaCha12Rng` algorithm with a persisted seed and configuration version. Convert
bounded random integers into configured latency without floating-point money arithmetic. Do not
evaluate an order against market liquidity until its acceptance latency expires. Use a single-writer
matching book with stable `(price_priority, eligible_at, deterministic_sequence)` ordering, one
per-update consumable liquidity ledger, exact stop activation rules, and complete Day/GTC/IOC/FOK
semantics. Walk exact bounded levels, compute weighted fill price and slippage with checked
rational/decimal arithmetic, classify maker/taker, apply fees, and retain eligible resting quantity
for subsequent `ExecutionMarketUpdate` values.

```bash
cargo test -p market-squawk-adapter-paper --test realistic_fills \
  --test order_semantics \
  --test matching_priority --locked
```

Expected: PASS.

- [ ] **Step 7: Implement bounded command, market, and audit ownership**

`PaperExecutionAdapter` owns one state-writer task. Submit/cancel/reconcile and
`PaperMarketIngress::try_publish` use independent count/byte bounds. Every accepted mutation first
admits a bounded audit record containing previous/new state, reason, order/fill ID, timestamp,
configuration/ruleset version, and input digest. Audit saturation rejects before mutation. A later
audit persistence failure enters reconciliation-required and blocks new submissions. The adapter
returns owned shutdown/reap handles; it never detaches its writer, and its bounded audit reader must
be continuously consumed by the Wave 3 application audit composition.

- [ ] **Step 8: Prove cancellation races and deterministic ordering**

With paused time, test cancel before acceptance latency, cancel simultaneous with first fill, cancel
after partial fill, fill after cancel request but before cancel acknowledgement, expiry during cancel,
duplicate cancel, and terminal cancel. Order by `(event_at, deterministic_sequence)` and assert exact
balances, positions, fills, fees, and terminal state.

```bash
cargo test -p market-squawk-adapter-paper --test cancellation_races --locked
```

Expected: PASS after implementation and FAIL before it.

- [ ] **Step 9: Implement reconciliation and idempotency**

Test duplicate client order ID, duplicate dispatch, snapshot/reconcile consistency, uncertain submit,
uncertain cancel, audit failure, strict checkpoint export/import, and correction. The adapter exposes
a bounded versioned recovery/checkpoint contract that includes orders, fills, balances, positions,
reservations, idempotency keys, sequence, and completeness; it does not read paths itself.
`reconcile` returns bounded orders/fills/balances/positions plus completeness and revision; it never
silently clears uncertainty. Task 15 composes actual startup journal/checkpoint recovery.

```bash
cargo test -p market-squawk-adapter-paper --test reconciliation --locked
```

Expected: PASS.

- [ ] **Step 10: Prove the risk gate at the adapter boundary**

External code attempts every public constructor and Serde route. Only
current live authority -> risk -> one-time dispatcher -> `DispatchOrder` reaches `submit`. Direct app,
strategy, CLI, MCP, snapshot, replay, and test-support values fail to compile or are rejected.

```bash
cargo test -p market-squawk-adapter-paper --test risk_gate --locked
```

Expected: PASS.

- [ ] **Step 11: Property-test state and ledger invariants**

For arbitrary legal command/update sequences, assert no negative reserved amount, conservation of
cash/position under fills/fees, cumulative fill does not exceed order quantity, terminal state never
reactivates, duplicate IDs do not mutate state, and retained count/bytes never exceed configuration.

```bash
cargo test -p market-squawk-adapter-paper --test paper_properties --locked
cargo clippy -p market-squawk-adapter-paper --all-targets --all-features --locked -- -D warnings
```

Expected: PASS.

- [ ] **Step 12: Commit the complete paper adapter without root/app files**

```bash
git diff --check
git add adapters/market-squawk-adapter-paper
git commit -m "feat(paper): add realistic deterministic execution"
```

### Task 14: Integrate Wave 2 and prove memory/authority composition

**Owner:** Integration owner.

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: reviewed live/execution/app/platform files only for contract resolution.
- Create: `apps/market-squawk/tests/action_memory.rs`
- Create: `apps/market-squawk/tests/end_to_end_authority.rs`

**Interfaces:**

- Consumes: Tasks 10–13.
- Produces: one integrated runtime with complete features, exact risk/dispatch authority, production
  Coinbase composition modules, and realistic paper backend ready for Wave 3 app wiring.

- [ ] **Step 1: Merge in safe dependency order**

```text
live features
→ execution authority/dispatcher
→ paper adapter
→ Coinbase app composition
```

The integration owner resolves `actor/processing.rs`, `execution/adapter.rs`, platform config, and
manifest/lock changes. Do not cherry-pick lane lockfiles.

- [ ] **Step 2: Add pinned deterministic RNG dependencies**

Add reviewed compatible versions of `rand_core` and `rand_chacha` to workspace dependencies and
enable only required features. Run the wave's one intentional unlocked
`cargo check --workspace --all-features`, inspect `Cargo.toml` and `Cargo.lock` for only those reviewed
additions, then use locked commands. Do not run `cargo generate-lockfile`. Paper must identify the
exact RNG algorithm/configuration version in audit metadata.

- [ ] **Step 3: Test the complete authority pipeline with a test-only verified source**

The test registers a synthetic source only in test support, passes through real registry/capture/
decoder/live qualification, warms all required features, creates a bounded strategy intent, consumes
one capability in risk, reserves account exposure, dispatches once, and observes a paper receipt.
Repeat with DirectUnverified, stale, quarantined, rolled generation, queue saturation, and expired
authority; every negative case must produce zero adapter calls.

```bash
cargo test -p market-squawk --test end_to_end_authority --locked
```

Expected: PASS. The synthetic source is absent from production registration and documentation.

- [ ] **Step 4: Extend the peak-memory proof across the action pipeline**

Each owning crate exposes only a checked, opaque retained-byte estimate for its configured production
component: live reports route/feature/cross-venue/capability state; execution reports strategy,
account/risk/reservation/approval/dispatcher/audit state; paper reports commands, market updates,
audits, orders, fills, ledger, recovery, and retained snapshots. No crate imports another crate merely
to test a forbidden dependency.

The app-level `action_memory.rs` composes those opaque estimates and the app subscription/control,
audit-journal, and service-worker state into the single startup ceiling. Test each exact delta,
checked cross-crate sum overflow, ceiling equality, and one byte below. Keep the existing per-crate
feature/execution/paper memory tests as local ownership proofs.

```bash
cargo test -p market-squawk --test action_memory --locked
```

Expected: PASS.

- [ ] **Step 5: Verify the Wave 2 candidate, commit, and verify exact HEAD**

Run the pre-commit Wave candidate gate and confirm only the reviewed Wave 2 paths are dirty. Then:

```bash
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-live \
  crates/market-squawk-execution adapters/market-squawk-adapter-paper \
  apps/market-squawk/src/live_source \
  apps/market-squawk/tests/action_memory.rs \
  apps/market-squawk/tests/end_to_end_authority.rs \
  apps/market-squawk/tests/production_coinbase_pipeline.rs \
  crates/market-squawk-platform
git commit -m "feat(q3): integrate live decision and paper pipeline"
```

Capture `WAVE_HEAD="$(git rev-parse HEAD)"` and run the complete exact-head Wave gate from the plan
header. Expected: the worktree is clean, HEAD remains `WAVE_HEAD`, and every command exits zero.

---

## Wave 3 — application wiring, hardening, and grouped Q3 checkpoint

### Task 15: Rewire the app, CLI, and MCP through shared production services

**Owner:** Integration owner. No parallel lane edits the shared app files during this task.

**Files:**

- Create: `apps/market-squawk/src/services.rs`
- Create: `apps/market-squawk/src/execution_service.rs`
- Create: `apps/market-squawk/src/paper_service.rs`
- Create: `apps/market-squawk/src/execution_audit.rs`
- Create: `crates/market-squawk-platform/src/audit_journal.rs`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `apps/market-squawk/src/lib.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/src/mcp.rs`
- Modify: `apps/market-squawk/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Remove after migration: `apps/market-squawk/src/features.rs`
- Remove after migration: `apps/market-squawk/src/risk.rs`
- Remove after migration: `apps/market-squawk/src/bot.rs`
- Remove after migration: `apps/market-squawk/src/source/coinbase.rs`
- Remove after migration: `apps/market-squawk/src/source/mod.rs`
- Move to test-only support: `apps/market-squawk/src/source/mock.rs`
- Modify or remove after bounded consumer migration: `apps/market-squawk/src/diagnostic_engine.rs`
- Create: `apps/market-squawk/tests/submission_entry_points.rs`
- Create: `apps/market-squawk/tests/paper_cli.rs`
- Create: `apps/market-squawk/tests/paper_mcp.rs`
- Create: `apps/market-squawk/tests/execution_audit.rs`
- Create: `apps/market-squawk/tests/execution_recovery.rs`
- Modify: `apps/market-squawk/tests/replay.rs`
- Modify: `apps/market-squawk/tests/journal_path_integration.rs`

**Interfaces:**

- Consumes: production Coinbase/live composition, immutable feature snapshots, execution action hook,
  dispatcher, paper adapter handles, bounded audit reader, local platform paths, CLI, and MCP.
- Produces: one `ApplicationServices` composition used by CLI and MCP, controlled paper bot/execution
  commands, durable local execution audit and startup recovery outside the live path, owned/reapable
  paper and audit workers, and no diagnostic production bypass.

- [ ] **Step 1: Enumerate every current and planned submission entry point in a failing test**

`submission_entry_points.rs` scans/uses public app services for strategy, bot, CLI, MCP, paper, and
adapter operations. It proves there is no public unchecked submit method and that direct imports of
capability/approval/dispatch test support fail. The only successful order path in the integration
test is:

```text
test-only current verified source
→ committed live event and ready features
→ Strategy
→ LiveActionHook
→ CurrentAuthorityGate issue/consume
→ RiskService and account reservation
→ ApprovedOrder
→ ExecutionDispatcher
→ DispatchOrder
→ PaperExecutionAdapter
```

```bash
cargo test -p market-squawk --test submission_entry_points --locked
```

Expected: FAIL while diagnostic bot/risk/direct paper fill paths remain.

- [ ] **Step 2: Compose one shared application service graph**

`ApplicationServices` owns source registry/composition, live runtime, execution hook/dispatcher,
paper worker/reap handles, paper audit reader, snapshot readers, and bounded audit writer lifecycle.
It continuously drains the paper reader into the controlled local audit journal; there is no
unconsumed receiver. CLI and MCP borrow service methods; they do not duplicate risk, account, source,
paper, or cancellation logic.

Startup validates all capacities and endpoint/artifact paths before spawning. Shutdown follows:
Startup opens and validates the audit/checkpoint, completes or fails closed on recovery, starts and
connects the audit consumers/writer, and only then enables the action hook, dispatcher, source, CLI,
or MCP mutation methods. A partially started service graph remains non-actionable and is reaped.
Shutdown follows:

```text
stop source producers
→ invalidate source/live authority
→ close action production
→ drain or reject execution commands by deadline
→ reconcile paper state
→ close/drain the paper audit reader into the controlled local journal
→ flush/reap local audit writer
→ join paper, audit, source, and service workers
→ close snapshots/services
```

- [ ] **Step 3: Persist execution/risk/paper audit through a generic platform journal**

`market-squawk-platform::audit_journal` is domain-agnostic: it owns only controlled path fencing,
bounded count/byte admission, strict version/sequence/length/checksum framing, flush, and owned worker
shutdown/reap. Platform must not depend on execution or paper crates and must not name their DTOs.

App-owned `execution_audit.rs` validates and canonically serializes bounded execution/risk/paper DTOs
from the execution and paper readers before passing opaque records to the generic journal. It records
decision, rejection, reservation, dispatch, submit, fill, fee, cancel, reconciliation, checkpoint,
and shutdown facts. Secret material, capabilities, approval objects, and credentials are never
serialized. The live path only performs nonblocking bounded admission and never waits on disk.
Writer failure atomically revokes future action production, marks paper/execution reconciliation
required, and remains visible in service health; no subsequent submission or claimed reconciliation
may proceed until controlled recovery succeeds.

Tests cover queue count/bytes, checksum corruption, truncated tail, writer failure, shutdown
deadline/reap, unconsumed-reader prevention, destination fencing, redaction, exact source/order/
ruleset linkage, zero platform-to-execution dependency, action revocation on writer failure, zero
adapter calls after failure, and the invariant that event-to-decision never waits on journal I/O.

```bash
cargo test -p market-squawk --test execution_audit --locked
```

Expected: PASS after implementation and FAIL before it.

- [ ] **Step 4: Recover paper and execution authority before accepting submissions**

At startup, app `execution_audit.rs` opens the controlled checkpoint/journal and validates strict
format version, monotonic record sequence, length, checksum/hash-chain continuity, and canonical DTO
schema before importing any state. Define and test an explicit torn/truncated-tail policy: only a
provably incomplete final frame after the last valid durable checkpoint may be truncated and
quarantined with recorded evidence; checksum failure in a complete frame, duplicate/reordered
records, a gap, unknown mandatory version, or inconsistency fails closed.

Recovery reconstructs paper orders, fills, balances, positions, reservations, idempotency state,
account/risk reconciliation state, and latest checkpoint revision, then compares the reconstructed
snapshot with adapter import/reconcile output. Any incomplete, inconsistent, unsupported, or
partially applied recovery publishes `ReconciliationRequired` before action-hook/dispatcher startup
and rejects every new submission. It never silently starts from an empty ledger.

`execution_recovery.rs` covers clean restart, version mismatch, sequence gap, duplicate/reordered
record, hash/checksum break, permitted torn final tail, forbidden corrupted complete tail,
checkpoint-plus-delta replay, reconstruction of every state category, idempotent second startup, and
zero adapter submit calls until reconciliation completes.

```bash
cargo test -p market-squawk --test execution_recovery --locked
```

Expected: FAIL before composed startup recovery and PASS afterward.

- [ ] **Step 5: Wire controlled CLI paper and execution commands**

Provide the Q3 portion of the required hierarchy:

```text
market-squawk capture ...
market-squawk bot status
market-squawk bot paper start
market-squawk bot paper stop
market-squawk execution orders
market-squawk execution fills
market-squawk execution cancel --order-id ...
market-squawk execution reconcile
market-squawk doctor
market-squawk mcp serve
```

Bot start configures a strategy; it does not submit an intent. Coinbase remains DirectUnverified, so
it cannot produce immediate automated actions. Order/fill output is bounded, cancel goes through the
execution service, and reconciliation is explicit. There is no CLI risk bypass or unchecked order
submission command.

```bash
cargo test -p market-squawk --test paper_cli --locked
```

Expected: PASS after implementation.

- [ ] **Step 6: Wire typed bounded MCP paper/execution tools**

Expose controlled status, start/stop paper bot, bounded order/fill reads, cancel, and reconcile
through the same services. Require strict schemas, result count/time limits, cancellation, rate
limits, and audit. MCP never accepts `LiveExecutionCapability`, `ApprovedOrder`, `DispatchOrder`,
credentials, arbitrary filesystem paths, unrestricted SQL, or unchecked order submission.

```bash
cargo test -p market-squawk --test paper_mcp --locked
python3 scripts/smoke_mcp.py
```

Expected: PASS.

- [ ] **Step 7: Remove diagnostic production paths only after black-box parity**

Delete app feature/risk/bot/source types after analytics, risk, production source composition, paper,
CLI, MCP, and replay tests pass through new services. Move synthetic source helpers under a
non-default integration-test-only support module. The boundary checker must reject production
enumeration or dependency on it.

Replay remains authority-free diagnostic tooling. It may reconstruct diagnostic state from raw
capture but cannot create current source leases, capabilities, approvals, dispatch values, or paper
fills.

- [ ] **Step 8: Verify every entry point and compatibility path**

```bash
cargo test -p market-squawk --all-features --locked
cargo test -p market-squawk --test submission_entry_points \
  --test production_coinbase_pipeline \
  --test paper_cli \
  --test paper_mcp \
  --test execution_audit \
  --test execution_recovery \
  --test replay \
  --test journal_path_integration --locked
cargo test -p market-squawk --lib --locked live_source::tests
python3 scripts/check_workspace_boundaries.py
```

Expected: PASS; no diagnostic production module remains capable of action.

- [ ] **Step 9: Resolve manifests minimally and commit**

If app manifest changes alter the lockfile, run exactly one unlocked
`cargo check --workspace --all-features`, inspect `Cargo.toml`/`Cargo.lock`, then return to locked
verification. Do not run `cargo generate-lockfile`.

Run the pre-commit Wave candidate gate and confirm only the reviewed Wave 3 application paths are
dirty. Then:

```bash
git diff --check
git add Cargo.toml Cargo.lock apps/market-squawk \
  crates/market-squawk-platform/src/audit_journal.rs \
  crates/market-squawk-platform/src/lib.rs
git commit -m "feat(app): wire production Q3 services"
```

Capture `WAVE_HEAD="$(git rev-parse HEAD)"` and run the complete exact-head Wave gate from the plan
header. Expected: the worktree is clean, HEAD remains `WAVE_HEAD`, and every command exits zero.

### Task 16: Add parser fuzz targets and measured Q3 performance evidence

**Owner:** Subwave 3B after Task 15 exact-head verification. Lane A owns analytics/execution
benches and their package manifests; lane B owns Coinbase/paper benches and package manifests plus
the isolated fuzz workspace; lane C owns app latency/RSS harnesses, measurement script, and tooling
research but only requests its app-manifest edit. The integration owner alone edits root/app
manifests and root lockfile, runs final measurements, writes result evidence, and commits the merged
task. No lane changes production behavior; any discovered bug returns to its owning production lane
under systematic debugging and a red regression test.

**Files:**

- Create: `fuzz/Cargo.toml`
- Create: `fuzz/Cargo.lock`
- Create: `fuzz/rust-toolchain.toml`
- Create: `fuzz/fuzz_targets/coinbase_decode.rs`
- Create: `fuzz/fuzz_targets/decode_outcome.rs`
- Create: `fuzz/fuzz_targets/execution_intent.rs`
- Create: `fuzz/fuzz_targets/paper_command.rs`
- Create: `crates/market-squawk-analytics/benches/live_features.rs`
- Modify: `crates/market-squawk-analytics/Cargo.toml`
- Create: `crates/market-squawk-execution/benches/risk_dispatch.rs`
- Modify: `crates/market-squawk-execution/Cargo.toml`
- Create: `adapters/market-squawk-adapter-coinbase/benches/decode.rs`
- Modify: `adapters/market-squawk-adapter-coinbase/Cargo.toml`
- Create: `adapters/market-squawk-adapter-paper/benches/paper_execution.rs`
- Modify: `adapters/market-squawk-adapter-paper/Cargo.toml`
- Create: `apps/market-squawk/benches/event_to_decision_latency.rs`
- Create: `apps/market-squawk/examples/q3_sustained_burst.rs`
- Modify: `apps/market-squawk/Cargo.toml`
- Create: `scripts/measure_q3_performance.sh`
- Create: `docs/research/tooling/cargo-fuzz-criterion-2026-07-16.md`
- Create: `docs/verification/q3-performance.md`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**

- Consumes: public parser/constructor APIs and deterministic production kernels.
- Produces: an isolated fuzz-only workspace/toolchain, bounded fuzz harnesses, Criterion throughput
  benchmarks, a deterministic percentile-latency harness, OS-measured peak RSS, and hardware/
  toolchain/result evidence without unmeasured acceptance claims.

- [ ] **Step 1: Persist primary tooling research and isolate fuzz from production stable**

Record retrieval date `2026-07-16`, selected commands, platform limits, and these primary sources:

- cargo-fuzz repository/README: <https://github.com/rust-fuzz/cargo-fuzz>
- cargo-fuzz 0.13.2 registry documentation: <https://docs.rs/crate/cargo-fuzz/0.13.2>
- Rust Fuzz Book cargo-fuzz guide: <https://rust-fuzz.github.io/book/cargo-fuzz/guide.html>
- Rust Fuzz Book setup requirements: <https://rust-fuzz.github.io/book/cargo-fuzz/setup.html>
- Criterion 0.8.2 registry documentation: <https://docs.rs/crate/criterion/0.8.2>
- Criterion user guide: <https://bheisler.github.io/criterion.rs/book/index.html>
- Criterion analysis process: <https://bheisler.github.io/criterion.rs/book/analysis.html>

The cargo-fuzz README states that libFuzzer requires sanitizer support and a nightly compiler, and
documents an independent fuzzing workspace. Therefore `fuzz/Cargo.toml` is its own nested
`[workspace]`, not a root workspace member; root `Cargo.toml` explicitly excludes `fuzz` if Cargo
workspace discovery requires it. `fuzz/rust-toolchain.toml` pins one reviewed dated nightly solely
for fuzzing. Root `rust-toolchain.toml` remains stable `1.97.0`; normal build, clippy, tests, release,
and audits never select fuzz nightly or consume `fuzz/Cargo.lock`.

```bash
git diff -- Cargo.toml rust-toolchain.toml fuzz/Cargo.toml fuzz/rust-toolchain.toml
cargo metadata --format-version 1 --no-deps --locked >/dev/null
(cd fuzz && cargo metadata --format-version 1 --no-deps --locked >/dev/null)
```

Expected: root metadata has no fuzz package; fuzz metadata is a separate workspace; the root stable
toolchain file is unchanged.

Install or verify the reviewed free fuzz driver without changing the production toolchain:

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cargo fuzz --version
```

Expected: the driver reports `cargo-fuzz 0.13.2`; record that independently from the pinned
fuzz-only compiler.

- [ ] **Step 2: Write fuzz harnesses with bounded input and no network/filesystem effects**

Targets cover Coinbase JSON/control/data frames, generic decoder outcome/request decoding, strict
intent wire/constructor paths, and paper command/state transitions. Cap input length before parse,
use deterministic in-memory fixtures, and assert no panic, unbounded allocation, or invariant bypass.

```bash
(cd fuzz && cargo +nightly-2026-07-15 fuzz build)
```

Pin `nightly-2026-07-15` in `fuzz/rust-toolchain.toml`; never use a floating `nightly` channel in
evidence. Expected: every target builds with the documented fuzz-only toolchain. If the pinned
nightly was not published for the target, `cargo-fuzz`, its required compiler, C++ toolchain, or
sanitizer support is unavailable, record a blocker and select a documented earlier published dated
nightly in a reviewed plan amendment; do not mark fuzzing complete or weaken production stable.

- [ ] **Step 3: Run time-bounded local fuzz smoke campaigns**

```bash
(cd fuzz && cargo +nightly-2026-07-15 fuzz run coinbase_decode -- -max_total_time=60)
(cd fuzz && cargo +nightly-2026-07-15 fuzz run decode_outcome -- -max_total_time=60)
(cd fuzz && cargo +nightly-2026-07-15 fuzz run execution_intent -- -max_total_time=60)
(cd fuzz && cargo +nightly-2026-07-15 fuzz run paper_command -- -max_total_time=60)
```

Expected: no crash/artifact. Any artifact becomes a committed deterministic regression fixture before
continuing.

- [ ] **Step 4: Add Criterion throughput benchmarks with explicit manifest ownership**

Add this reviewed exact dependency once under root `[workspace.dependencies]`:

```toml
criterion = { version = "=0.8.2", default-features = false, features = ["cargo_bench_support"] }
```

Add
`criterion.workspace = true` under `[dev-dependencies]` and an explicit `[[bench]]` with
`harness = false` in each of these exact package manifests:

```text
crates/market-squawk-analytics/Cargo.toml                 live_features
crates/market-squawk-execution/Cargo.toml                 risk_dispatch
adapters/market-squawk-adapter-coinbase/Cargo.toml        decode
adapters/market-squawk-adapter-paper/Cargo.toml           paper_execution
```

Benchmark:

```text
Coinbase snapshot/delta/trade/control decode throughput
decoder outcome/capture validation
all online feature updates and cross-venue reads
strategy plus risk evaluation
approval handoff and dispatcher preflight
paper submit, partial fill, cancel, and reconcile
end-to-end synthetic event-to-decision latency
```

Use warmed runs, fixed fixtures, explicit event count, and black-box outputs. Benchmark code must not
alter production algorithms or disable validation. Criterion supplies microbenchmark distribution/
throughput evidence; it is not accepted as proof of event-level p50/p95/p99/max or process peak RSS.

- [ ] **Step 5: Add deterministic event-latency and OS peak-memory harnesses**

`event_to_decision_latency.rs` drives the complete synthetic verified event-to-decision pipeline on
one pinned fixture after a documented warm-up. It records one monotonic duration per event into a
fixed-capacity preallocated histogram, uses checked counters, reports event count, throughput,
p50/p95/p99/max, and rejects dropped/overflowed samples. It has its own `[[bench]]` entry with
`harness = false` in the app manifest and deterministic machine-readable output.

`q3_sustained_burst.rs` drives the configured worst-case bounded burst for a fixed event count and
duration, verifies queue ceilings and steady-state retained-memory estimates, and exits nonzero on
drop, overflow, unbounded growth, or invariant failure. `scripts/measure_q3_performance.sh` selects
the documented OS-specific external measurement without hiding portability:

```text
macOS:  /usr/bin/time -l <release sustained-burst command>   # maximum resident set size
Linux:  /usr/bin/time -v <release sustained-burst command>   # Maximum resident set size
```

The script records raw command/output plus unit conversion. Unsupported OS/tool output is a blocker,
not an estimated peak. Warm-up allocation, baseline RSS, configured startup estimate, post-warm-up
RSS, peak RSS, and end RSS are all reported so bounded retained state is not confused with allocator
high-water behavior.

- [ ] **Step 6: Merge disjoint lane handoffs and resolve only reviewed benchmark dependencies**

The integration owner reviews the three lane commits, applies the requested app/root manifest edits,
and rejects any cross-lane file. After that manifest merge, run one intentional unlocked minimal
resolution:

```bash
cargo check --workspace --all-features
git diff -- Cargo.toml Cargo.lock \
  crates/market-squawk-analytics/Cargo.toml \
  crates/market-squawk-execution/Cargo.toml \
  adapters/market-squawk-adapter-coinbase/Cargo.toml \
  adapters/market-squawk-adapter-paper/Cargo.toml \
  apps/market-squawk/Cargo.toml
```

Expected: the root lock changes only for the reviewed Criterion benchmark graph. Do not run
`cargo generate-lockfile`. If an exact transitive correction is independently justified, use only
`cargo update -p NAME --precise VERSION`, document it, and inspect the targeted lock diff. Resolve
and commit `fuzz/Cargo.lock` from inside the isolated fuzz workspace; never merge it into root
`Cargo.lock`. Return immediately to locked root commands.

- [ ] **Step 7: Record measured facts**

Run:

```bash
cargo bench -p market-squawk-analytics --bench live_features --locked
cargo bench -p market-squawk-execution --bench risk_dispatch --locked
cargo bench -p market-squawk-adapter-coinbase --bench decode --locked
cargo bench -p market-squawk-adapter-paper --bench paper_execution --locked
cargo bench -p market-squawk --bench event_to_decision_latency --locked
./scripts/measure_q3_performance.sh
```

Write `q3-performance.md` with commit, UTC date, hardware, OS, toolchain, fixture, event count,
throughput, p50/p95/p99/max from the custom histogram, raw/converted peak RSS from the OS tool,
configured bound, queue/drop counters, and all command exit codes. State whether the product target
was met. Never infer percentiles from Criterion confidence intervals or convert an unmeasured target
into a claim.

- [ ] **Step 8: Verify the tooling candidate, commit, and verify exact HEAD**

Run the pre-commit Wave candidate gate using the root stable toolchain and confirm only the reviewed
tooling/benchmark/evidence paths are dirty. Then:

```bash
git diff --check
git add Cargo.toml Cargo.lock fuzz \
  crates/market-squawk-analytics/Cargo.toml \
  crates/market-squawk-analytics/benches \
  crates/market-squawk-execution/Cargo.toml \
  crates/market-squawk-execution/benches \
  adapters/market-squawk-adapter-coinbase/Cargo.toml \
  adapters/market-squawk-adapter-coinbase/benches \
  adapters/market-squawk-adapter-paper/Cargo.toml \
  adapters/market-squawk-adapter-paper/benches \
  apps/market-squawk/Cargo.toml \
  apps/market-squawk/benches/event_to_decision_latency.rs \
  apps/market-squawk/examples/q3_sustained_burst.rs \
  scripts/measure_q3_performance.sh \
  docs/research/tooling/cargo-fuzz-criterion-2026-07-16.md \
  docs/verification/q3-performance.md
git commit -m "test(q3): add fuzz and performance evidence"
```

Capture `WAVE_HEAD="$(git rev-parse HEAD)"` and run the complete exact-head Wave gate from the plan
header with root stable. Expected: the worktree is clean, HEAD remains `WAVE_HEAD`, and every command
exits zero.

### Task 17: Reconcile architecture, gap, coverage, and source documentation with code

**Owner:** Serialized subwave 3C documentation owner after Task 16's measured-evidence commit passes
its exact-head gate.

**Files:**

- Modify: `docs/architecture/current-state.md`
- Modify: `docs/architecture/target-state.md`
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Modify: `README.md`
- Modify: `SECURITY.md`
- Modify: `docs/research/providers/coinbase-exchange-websocket-2026-07-16.md`
- Create: `docs/verification/q3-production.md`

**Interfaces:**

- Consumes: integrated tests, source research, performance evidence, audit output, and exact code
  paths.
- Produces: truthful requirement classifications, coverage/quality language, local verification
  record, known limitations, and no stale compatibility or deferred-paper claim.

- [ ] **Step 1: Re-audit every superseded Task 9–12 requirement**

For every feature, risk rule, decoder disposition, Coinbase production step, and paper capability,
record exactly one of `Implemented`, `Partial`, `Missing`, `Incorrect`, `Unsafe`, or
`Intentionally deferred`, with a code/test/evidence link. Because this plan makes the enumerated Q3
scope mandatory, any `Partial`, `Missing`, `Incorrect`, or `Intentionally deferred` item blocks Q3
close. `Unsafe` is reserved only for the explicitly prohibited evasion mechanisms; it must not be
used to hide a missing/incorrect product requirement or an ordinary implementation risk.

- [ ] **Step 2: State provider and execution capability precisely**

Required wording:

```text
Coinbase Exchange v1 is a direct single-venue adapter for configured products/channels.
Its Q3 quality ceiling is DirectUnverified.
It is not consolidated and does not guarantee complete trade history.
It cannot qualify an immediate automated action.
```

Paper may be called realistic/complete only when fees, seeded latency, slippage, depth-aware partial
fills, all order states, cancellation races, balances/positions, reconciliation, and audit are
linked to passing evidence. The removed immediate/full-fill object is historical compatibility, not
the delivered capability.

- [ ] **Step 3: Persist source and verification evidence**

Confirm the provider research file contains retrieval date, official links, selected schema,
coverage, quality ceiling, completeness limitation, and Advanced Trade non-mixing decision. Confirm
raw frame persistence, decoder/audit lineage, and execution audit artifacts are documented with
controlled paths and retention/size behavior.

`q3-production.md` records exact local commands, tool versions, exit codes, test totals, fuzz run
durations/artifacts, audit outputs, benchmark evidence, and current commit. It contains a separate
`Hosted CI` section populated only from inspected hosted check results; otherwise it says
`Not evaluated in this local evidence record`.

- [ ] **Step 4: Scan documentation consistency and commit**

```bash
rg -n 'TBD|TODO|FIXME|implement later|fill in|appropriate error handling|similar to' \
  README.md SECURITY.md docs crates adapters apps scripts
python3 scripts/check_brand.py
python3 scripts/check_generated_artifacts.py
python3 -m unittest scripts.tests.test_documentation_contracts -v
git diff --check
git add README.md SECURITY.md docs/architecture docs/plans \
  docs/research/providers/coinbase-exchange-websocket-2026-07-16.md \
  docs/verification/q3-production.md
git commit -m "docs(q3): reconcile production capability evidence"
```

Expected: no placeholder or contradictory capability language remains.

### Task 18: Run the grouped Q3 quarter checkpoint review

**Owner:** Integration owner. Use fresh independent reviewers after all code/docs are integrated.

**Files:**

- Create: `docs/reviews/q3/authority-and-bypass.md`
- Create: `docs/reviews/q3/concurrency-memory-shutdown.md`
- Create: `docs/reviews/q3/provider-coverage-lawful-access.md`
- Create: `docs/reviews/q3/financial-paper-state.md`
- Create: `docs/reviews/q3/dependency-and-entry-points.md`
- Create: `docs/reviews/q3/checkpoint-summary.md`

**Interfaces:**

- Consumes: one clean integrated Q3 commit, complete local evidence, provider research, and separately
  inspected hosted CI state.
- Produces: independent findings with severity/evidence, remediation commits, rerun evidence, and an
  explicit approved/not-approved Q3 decision.

- [ ] **Step 1: Freeze one review commit and prohibit reviewer edits**

```bash
git status --short
git rev-parse HEAD
git show --stat --oneline HEAD
```

Expected: clean worktree. Give every reviewer the same exact commit. Reviewers write findings only;
remediation is performed in fresh implementation lanes and reviewed again.

Use the available slots in two quarter-checkpoint batches. Batch A dispatches Steps 2–4 in parallel
(three independent reviewers while the integration owner coordinates). After all Batch A reports
are frozen, Batch B dispatches Steps 5–6 in parallel against the same frozen commit. Do not remediate,
commit, or change the candidate HEAD between batches. Reviewers never edit production files or each
other's reports. After all five reports arrive, the integration owner unions and deduplicates
findings by root cause/evidence before Step 7. Remediation may then parallelize up to three
non-overlapping ownership lanes, but the integration owner serializes shared files and all re-review
conclusions.

- [ ] **Step 2: Dispatch the authority and bypass review**

The reviewer traces every strategy/app/CLI/MCP/paper/adapter entry point and attempts capability,
gate, reservation, approval, dispatch, clock, account, snapshot, replay, and test-support bypasses.
They verify one-time nonce/approval semantics, expiry/revocation races, audit admission, uncertain
outcomes, and zero adapter calls on negatives. Findings include exact path/line/test evidence.

- [ ] **Step 3: Dispatch the concurrency, memory, and shutdown review**

The reviewer audits single-writer ownership, all count/byte bounds, retained allocation accounting,
cross-venue coalescing, feature windows, risk/account reservations, dispatcher/paper queues, snapshot
retention, subscription/control state, generic audit journal, continuously consumed paper audit
reader, saturation policy, task cancellation, deadlines, abort-and-await, pending-worker reap, and
sustained-burst memory evidence.

- [ ] **Step 4: Dispatch the provider, coverage, and lawful-access review**

The reviewer compares the pinned fixtures/metadata/research to the official Coinbase links, verifies
one-generation source behavior, exact capture before decode, control/freshness separation,
DirectUnverified ceiling, shared provider budget, backoff/Retry-After, endpoint policy, source
persistence, and absence of every evasion mechanism.

- [ ] **Step 5: Dispatch the financial and paper-state review**

The reviewer audits every state transition, checked ledger operation, currency/rounding rule,
fee/latency/slippage/depth model, seeded determinism, partial/resting fill, cancellation race,
all order/TIF semantics, stable price/time priority, single-use market liquidity, instrument
revision/tick/lot/currency/multiplier terms, idempotency, balance/position reservation, audit
ordering, checkpoint/journal startup recovery, reconciliation, and property oracle.

- [ ] **Step 6: Dispatch the dependency and application entry-point review**

The reviewer runs/adversarially tests the DAG checker, Cargo feature graph, test-support bans,
platform journal's lack of execution/paper dependencies, app-owned audit DTO composition, app
service reuse, CLI/MCP schemas/limits, diagnostic deletion, replay authority separation, source
registration, controlled artifact paths, secret redaction, and generated-artifact/credential policy.

- [ ] **Step 7: Remediate every substantiated finding with red-green evidence**

For each finding, use systematic debugging, add a failing regression test, verify it fails for the
reported reason, implement the narrow root-cause fix, rerun the focused and full gates, and obtain
reviewer re-approval. This applies to every substantiated Critical, Important, and Minor finding;
severity affects prioritization, not whether Q3 may leave it unresolved. Do not close a finding
solely from a code diff. If a reviewer retracts a finding, record the exact contrary evidence.

- [ ] **Step 8: Run the pre-evidence candidate gate**

Run the complete command set below after remediation and record it in the draft review evidence. It
proves the code candidate but is not represented as the final docs-inclusive HEAD gate.

```bash
./scripts/verify.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check
cargo audit --deny warnings
cargo machete --with-metadata
gitleaks git --redact --no-banner --timeout 300 .
gitleaks dir --redact --no-banner --timeout 300 .
python3 scripts/smoke_mcp.py
git diff --check
git status --short
```

Expected: every local command exits zero and the worktree is clean before review-doc edits. Missing
tooling, skipped checks, any unresolved substantiated finding, absent fuzz artifacts/evidence, or
unmet performance evidence is a blocker.

- [ ] **Step 9: Inspect hosted CI separately**

Use the repository's hosted-check interface to record each required job name, commit SHA, conclusion,
and URL in `checkpoint-summary.md`. A green local gate does not imply hosted CI success. If hosted CI
is unavailable, not run, or intentionally not inspected, record `Not evaluated`; do not claim it
passed. Hosted CI is optional evidence and never a prerequisite for local Q3 approval because Market
Squawk has no mandatory cloud-service dependency.

- [ ] **Step 10: Apply exact acceptance language**

The checkpoint summary may say `Q3 approved` only if all of these are true:

```text
all required live features implemented, actor-integrated, bounded, reset-safe, and memory-accounted
all execution paths consume current live authority and authoritative account risk exactly once
decoder outcomes distinguish data/control/ignored/resync/quarantine with exact capture binding
Coinbase production composition works with an honest DirectUnverified single-venue ceiling
paper execution includes every required realistic capability and reconciliation/audit
all reviewers have no unresolved substantiated Critical, Important, or Minor finding
all mandatory local checks satisfy policy; hosted checks, when inspected, are reported separately
```

Before the final exact-head gate, draft the summary as `Q3 approval pending exact-head local gate`.
Otherwise record `Q3 not approved` and list exact blockers. Never describe Coinbase as
DirectVerified or diagnostic compatibility paper as the completed simulator.

- [ ] **Step 11: Commit review evidence**

```bash
git diff --check
git add docs/reviews/q3 docs/verification/q3-production.md
git commit -m "docs(q3): record quarter checkpoint review"
```

- [ ] **Step 12: Run the unconditional local gate at the final docs-inclusive HEAD**

After Step 11, capture the exact commit and rerun the complete gate without editing or amending any
file afterward:

```bash
Q3_FINAL_HEAD="$(git rev-parse HEAD)"
./scripts/verify.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check
cargo audit --deny warnings
cargo machete --with-metadata
gitleaks git --redact --no-banner --timeout 300 .
gitleaks dir --redact --no-banner --timeout 300 .
python3 scripts/smoke_mcp.py
git diff --check
test -z "$(git status --short)"
test "$(git rev-parse HEAD)" = "$Q3_FINAL_HEAD"
```

Report the exact `Q3_FINAL_HEAD`, command exit codes, and local `Q3 approved`/`Q3 not approved`
decision in the execution handoff. Do not describe the pre-evidence gate as exact-head verification.
If any command fails, approval remains false: return to remediation, update and recommit evidence,
then repeat this step at the new final HEAD. Hosted CI remains a separate optional status field.

## Execution handoff

After formal Q2 approval and Task 0 refresh, execute this plan with
`superpowers:subagent-driven-development`. Use fresh subagents for bounded lane tasks, with lane
self-review and focused gates during implementation. Fresh independent specialist reviews are
grouped only at the Q3 quarter checkpoint in Task 18; do not run per-task independent review rounds.
Do not dispatch two agents to a shared conflict hotspot. The integration owner performs every root
manifest/lock/app merge and every Wave gate.
