# Q2 Source Authority Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task-by-task and
> `superpowers:verification-before-completion` before reporting the quarter checkpoint complete.

**Goal:** Make source memory admission, temporal authority, provider budgets, authorization scope,
and registry mutation fail closed and process authoritative.

**Architecture:** Recursive checked retained-size accounting feeds bounded live admission. A sealed
registry clock and temporal health epochs bind source evidence to trustworthy time. A bounded
process-lifetime coordinator supplies one non-resettable, generation-revocable allocation per
canonical provider/account scope, while registries retain strong local catalogs. Registration and
health changes stage all fallible work before state publication.

**Tech stack:** Rust 1.97, Edition 2024, Tokio synchronization where already required, standard
`OnceLock`/`Mutex`/`Arc`/atomics for process coordination, Serde for persisted control-plane state,
Proptest for invariants, and deterministic thread/barrier tests for concurrency.

**Controlling design:**
[`2026-07-16-q2-source-authority-remediation-design.md`](../specs/2026-07-16-q2-source-authority-remediation-design.md)

## Non-negotiable rules

- Work only from clean base `20ad084b47cfc0624a17f42233ff1e2748a62b05` in the isolated Q2
  worktree.
- Write and run each focused failing test before its production change.
- Do not expose a caller-settable trusted time or let caller-supplied `BudgetHealth` qualify live
  data.
- Every unavailable budget transition revokes already-issued availability leases before returning.
- Conflicting process-wide registrations and all epoch-overflow paths are failure atomic.
- Run `cargo fmt --all --check` and `git diff --check` at every task boundary.

---

## Task 1: Account for complete retained memory and reject oversized live commands

**Files:**

- Modify: `crates/market-squawk-sources/src/bounded.rs`
- Modify: `crates/market-squawk-sources/src/decoder/payload.rs`
- Modify: `crates/market-squawk-sources/src/decoder/batch.rs`
- Modify: relevant source retained-size tests under `crates/market-squawk-sources/tests/`
- Modify: live admission regression under `crates/market-squawk-live/tests/`

- [ ] Add failing tests for `BoundedVec` allocation bytes, every provider payload variant, nested
  snapshot/delta storage at 1/10,000/20,000 elements, checked overflow behavior, and shared
  authority/frame allocation charged once per routed command.
- [ ] Run each focused test and confirm the pre-fix shallow estimate or admission result fails.
- [ ] Implement checked `BoundedVec` backing-allocation accounting and normalize deserialized
  capacity without weakening the element bound.
- [ ] Recursively include nested element arrays and string capacities in provider retained bytes;
  preserve exact-once shared `Arc` charging at the routed-command boundary.
- [ ] Add a live test whose maximum bytes lie between the old and corrected estimates and prove
  admission rejects without queueing or partial state.
- [ ] Run:

  ```bash
  cargo test -p market-squawk-domain bounded --all-features --locked
  cargo test -p market-squawk-sources retained --all-features --locked
  cargo test -p market-squawk-live overflow --all-features --locked
  cargo fmt --all --check
  git diff --check
  ```

---

## Task 2: Canonicalize authorization-bound budget scopes

**Files:**

- Modify: `crates/market-squawk-sources/src/policy/budget.rs`
- Modify: source metadata validation and builders
- Modify: `crates/market-squawk-sources/tests/network_policy.rs`
- Modify: affected adapter/source fixtures found by `rg 'BudgetScope|ProviderBudgetPolicy'`

- [ ] Add table-driven failing tests for all authorization modes: valid public provider-only,
  rejected public account scope, required exact user-account basis, required exact licensed basis,
  and no remote budget for user-owned local data.
- [ ] Add `BudgetScope::for_authorization` (or an equivalent invariant-preserving constructor) and
  make metadata validation exhaustively compare the scope with both authorization mode and basis.
- [ ] Migrate production builders and fixtures away from arbitrary aliases; retain deserialization
  compatibility only behind mandatory revalidation.
- [ ] Run source and adapter policy tests, `cargo fmt --all --check`, and `git diff --check`.

---

## Task 3: Share budgets process-wide and revoke leases synchronously

**Files:**

- Modify: `crates/market-squawk-sources/src/policy/budget.rs`
- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Modify: `crates/market-squawk-sources/src/registry/authority.rs`
- Modify/Create: budget concurrency and restore tests under `crates/market-squawk-sources/tests/`

- [ ] Add deterministic failing tests showing two registries oversubscribe one scope, concurrent
  conflicting policy registration is not linearized, and restore creates an independent budget.
- [ ] Add failing lease tests for retry-after, refusal, disable, poison/clock/overflow failures,
  cooldown expiry, and concurrent transition versus lease validation.
- [ ] Implement the bounded process-lifetime coordinator and registry-retained budget catalogs.
  Provide an atomic batch registration path for restore; dropping handles must not reset authority.
- [ ] Move availability generation into the shared allocation. Increment it before every
  unavailable return, derive `BudgetHealth` under the allocation state, and issue generation-bound
  availability leases only for currently available state.
- [ ] Bind health/current-authority/live leases to the budget availability lease. Recovery requires
  a new health report; an old lease stays invalid after cooldown.
- [ ] Use barriers and bounded joins rather than sleeps in concurrency tests. Run all budget,
  registry, restore, and live authority tests plus formatting/diff checks.

---

## Task 4: Seal health time and enforce the full temporal chain

**Files:**

- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/registry/authority.rs`
- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Modify: `crates/market-squawk-sources/src/health.rs` as needed
- Modify/Create: deterministic registry temporal tests

- [ ] Add failing tests for observation before session start, observation after trusted report time,
  validation before observation/report time, validation after deadline, backward trusted time,
  trusted clock failure, and deadline overflow.
- [ ] Add a sealed registry clock abstraction with a system production clock and non-public
  deterministic test clock. A session reporter samples it and puts an opaque trusted observation in
  `CurrentHealthUpdate`; it must not serialize or be caller-constructible.
- [ ] Retain session start and health lower bounds. Make epoch validation enforce lower bound,
  deadline, current source epoch, current health epoch, and budget generation.
- [ ] Derive health qualification from source evidence and authoritative budget state in one staged
  operation. Reject temporal errors without changing the health cursor or active leases.
- [ ] Run all registry, health, authority, serialization, and live qualification tests, then format
  and diff checks.

---

## Task 5: Make registration and health epoch overflow failure atomic

**Files:**

- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify/Create: registry overflow and state-snapshot tests

- [ ] Add failing tests that force source-epoch and health-epoch exhaustion and snapshot entries,
  history, budget coordinator/catalog, cursor, qualification, and prior lease validity before and
  after rejection.
- [ ] Refactor registration/replacement to compute epoch/history/coordinator results before
  publication. Refactor health recording to compute the entire temporal/budget/epoch result before
  mutating cursor or atomics.
- [ ] Add concurrent conflicting-registration coverage proving exactly one policy wins and the
  loser registry remains unchanged.
- [ ] Run the complete domain/sources/live suites, format, and `git diff --check`.

---

## Task 6: Inspect downstream source-authority blast radius

**Files:**

- Modify: source/network policy documentation and rustdoc found by focused `rg`
- Inspect: Task 6/7/8 source, live, execution/risk, persistence/restore, adapters, CLI, and MCP paths
- Persist: a concise Q2 checkpoint note in the existing planning/review location if the repository
  has one

- [x] Use `rg` to enumerate constructors, health reports, budget transitions, restore paths, and
  current-authority consumers; fix compile-time and semantic ripple effects rather than adding
  compatibility shims that weaken invariants.
- [x] Confirm no SQLite/DataFusion/Parquet/Python/MCP/filesystem work entered the live hot path.
- [x] Run deterministic workspace verification:

  ```bash
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  cargo test --workspace --all-features --locked
  cargo build --workspace --all-features --release --locked
  git diff --check
  ```

- [x] Run repository-provided dependency, vulnerability, license, credential, generated-artifact,
  boundary, property, and concurrency checks relevant to Q2. Keep external-network tests separate.
- [x] Inspect the final diff against base `20ad084b47cfc0624a17f42233ff1e2748a62b05` for unrelated
  changes and record exact verification output before claiming completion.

---

## Quarter checkpoint review

The Q2 checkpoint is eligible for review only after Tasks 1–6 pass. Reviewers must evaluate the
whole quarter as one authority system: memory admission, authorization identity, global budget
coordination, temporal qualification, revocation, persistence/restore, and live admission. A pass in
one subsystem does not offset a bypass in another.

After implementation, use `superpowers:requesting-code-review`. Address findings with
`superpowers:receiving-code-review`, rerun the full checkpoint verification, and only then use
`superpowers:finishing-a-development-branch` for handoff.
