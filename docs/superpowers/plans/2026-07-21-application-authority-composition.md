# Application Authority Composition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Seal live-source generation authority, add a real Kraken-to-risk-to-paper production
vertical, and route the shipping MCP command through the hardened audited stdio server.

**Architecture:** The registry alone mints a one-use `LiveSourceGeneration` that owns the frame
factory and exact session/capture/budget graph. One typed application supervisor composes sealed
Coinbase or Kraken profiles through shared capture/live/risk/paper services. MCP uses
transport-neutral services, durable pre-reserved mutation audit, controlled content-addressed
artifacts, and an owned SDK thread with bounded retained reaping.

**Tech Stack:** Rust 1.97.1, Tokio, Tokio-Tungstenite, Serde, rmcp, capability-confined platform
paths, existing source/live/execution/paper contracts.

## Global Constraints

- Work only in `.worktrees/application-authority-composition` on
  `fix/application-authority-composition`; do not edit README, root checkout, remotes, or GitHub.
- Preserve `DirectUnverified` for Coinbase and Kraken and capture-before-decode ordering.
- The test WebSocket connector exists only under
  `cfg(all(feature = "test-support", debug_assertions))`; release all-features exposes no override.
- Production CLI accepts a typed provider but no endpoint, SQL, filesystem path, shell, order, or
  risk-bypass authority.
- Cargo is serialized: request a slot before every RED/GREEN command group and release it after.
- Keep tests to the causal authority negatives, one true Kraken vertical, one MCP lifecycle proof,
  and the fixture provenance contract.

---

### Task 1: Registry-Owned Live Generation

**Files:**
- Modify: `crates/market-squawk-sources/src/registry/authority.rs`
- Modify: `crates/market-squawk-sources/src/registry/catalog.rs`
- Modify: `crates/market-squawk-sources/src/registry.rs`
- Modify: `crates/market-squawk-sources/src/live.rs`
- Modify: `crates/market-squawk-sources/src/lib.rs`
- Test: `crates/market-squawk-sources/tests/registry_authority/pre_feed_cases.rs`

**Interfaces:**
- Produces:
  `AuthoritativeSourceRegistry::take_live_source_generation(&mut self,
  &CurrentSourceSession) -> Result<LiveSourceGeneration, RegistryError>`.
- Produces: `LiveSourceGeneration::try_start(self, &SourceMetadata) ->
  Result<ActiveLiveSourceGeneration, SourceError>` with `frames_mut()` and `budget()` accessors.
- Changes: `LiveMarketSource::run(&mut self, &mut dyn RawMarketSink, CancellationToken)`; the source
  owns its frame factory and no caller can substitute one.

- [ ] **Step 1: Write the causal registry RED**

Extend the existing pre-feed authority test with this wished-for API and three assertions:

```rust
let generation = registry.take_live_source_generation(&session)?;
let successor = registry.begin_next_session(&registered, successor_id, at)?;
assert_eq!(
    generation.try_start(metadata),
    Err(SourceError::SessionNotCurrent),
);
assert!(matches!(
    other_registry.take_live_source_generation(&session),
    Err(RegistryError::HandleTransplanted)
));
assert!(matches!(
    registry.take_live_source_generation(&successor),
    Ok(_)
));
```

The test must activate the capture generation before `try_start` and also prove a second generation
mint for the same session is rejected.

- [ ] **Step 2: Request Cargo and verify RED**

Run:

```bash
cargo test -p market-squawk-sources --test registry_authority \
  pre_feed_current_leases_are_deadline_capture_health_and_registry_bound --locked
```

Expected: compile failure because `take_live_source_generation` and `LiveSourceGeneration` do not
exist. Release the Cargo slot immediately after recording that causal failure.

- [ ] **Step 3: Implement the registry-only mint**

`take_live_source_generation` must validate the exact session, require the capture issuer to have
been taken, internally consume the sole raw-frame-factory issuance flag, and create the following
non-clone graph from registry-private fields:

```rust
pub struct LiveSourceGeneration {
    binding: FrameSessionBinding,
    session_lease: Arc<SessionLeaseState>,
    capture_lease: CaptureGenerationLease,
    budget: Option<SharedProviderBudget>,
    frames: RawFrameFactory,
    not_sync: PhantomData<Cell<()>>,
}
```

`try_start` must consume the capability, validate metadata source/revision, current session lease,
healthy capture lease, exact frame-factory lease/binding, and shared budget allocation before it
returns `ActiveLiveSourceGeneration`. No public constructor or cloning implementation is permitted.

- [ ] **Step 4: Update the object-safe live-source trait**

Use the non-separable signature:

```rust
fn run<'a>(
    &'a mut self,
    sink: &'a mut dyn RawMarketSink,
    cancellation: CancellationToken,
) -> BoxFuture<'a, Result<(), SourceError>>;
```

Update deterministic test sources but do not add compatibility overloads accepting a factory.

- [ ] **Step 5: Request Cargo and verify GREEN**

Run the focused source test above, then:

```bash
cargo test -p market-squawk-sources --all-features --locked
```

Expected: PASS with no warnings. Release the slot.

### Task 2: Coinbase/Kraken Adapters and One Production Vertical

**Files:**
- Modify: `adapters/market-squawk-adapter-coinbase/src/source.rs`
- Modify: `adapters/market-squawk-adapter-coinbase/src/source/tests.rs`
- Modify: `adapters/market-squawk-adapter-kraken/Cargo.toml`
- Modify: `adapters/market-squawk-adapter-kraken/src/session.rs`
- Modify: `adapters/market-squawk-adapter-kraken/src/session_tests.rs`
- Create: `crates/market-squawk-platform/src/config/kraken.rs`
- Modify: `crates/market-squawk-platform/src/config.rs`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `apps/market-squawk/Cargo.toml`
- Modify: `apps/market-squawk/src/live_source/composition.rs`
- Create: `apps/market-squawk/src/live_source/provider.rs`
- Modify: `apps/market-squawk/src/live_source/sink.rs`
- Modify: `apps/market-squawk/src/live_source/subscription_state.rs`
- Modify: `apps/market-squawk/src/live_source/supervisor.rs`
- Modify: `apps/market-squawk/src/live_source/mod.rs`
- Modify: `apps/market-squawk/src/lib.rs`
- Modify: `apps/market-squawk/src/paper_bot.rs`
- Modify: `apps/market-squawk/src/paper_bot/defaults.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Create: `apps/market-squawk/tests/production_kraken_pipeline.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Both adapter constructors consume `LiveSourceGeneration`; their `run` methods first call
  `try_start` and only then authorize endpoints, acquire budget, or connect.
- Produces typed `ProductionMarketSource::{Coinbase, Kraken}` selection and
  `local_paper_bot(config, provider, cash, fees)` composition.
- Produces a Kraken test connector only under the global debug/test-support guard.

- [ ] **Step 1: Write adapter authority REDs**

Adapt one existing source test in each adapter so generation rollover occurs after source
construction and before `run`. Assert `SourceError::SessionNotCurrent`, an unchanged provider-budget
availability generation/request count through its existing observation API, and zero local-server
accepts.

- [ ] **Step 2: Write the true Kraken vertical RED**

The app integration test starts a local WebSocket through the guarded connector, sends the pinned
subscription acknowledgement and checksum-valid snapshot, and observes the production pipeline’s
existing execution audit and paper snapshot:

```rust
assert_eq!(observed_quality, DataQuality::DirectUnverified);
assert_eq!(risk_rejections, &[RiskRejectionCode::SourceQuality]);
assert_eq!(approved_count, 0);
assert_eq!(dispatch_count, 0);
assert_eq!(paper_after, paper_before);
```

It must call the public source/application startup path, not `run_established`, and shut down every
source, capture, live, dispatcher, paper, server, and reaper owner.

- [ ] **Step 3: Request Cargo and verify RED**

Run:

```bash
cargo test -p market-squawk-adapter-coinbase source::tests --locked
cargo test -p market-squawk-adapter-kraken --features test-support session_tests --locked
cargo test -p market-squawk --features test-support --test production_kraken_pipeline --locked
```

Expected: compile/test failures at the missing generation-consuming APIs, provider selection, and
Kraken composition. Release the slot.

- [ ] **Step 4: Implement generation-consuming adapters**

Store `Option<LiveSourceGeneration>` in each source, take it once at `run`, call `try_start`, and
use only the returned factory/budget. Keep endpoint authorization before connector invocation and
capture before all decoder work. The guarded connector must not exist in release all-features.

- [ ] **Step 5: Implement typed Kraken configuration and shared composition**

Add strict Kraken public-interface rights evidence, exact endpoint, enumerated symbol/instrument,
book depth, frame/freshness bounds, and provider budget configuration. Generalize only the
provider-specific profile/decoder/subscription pieces; retain one registry/capture/sink/live/risk/
paper supervisor and its existing shutdown ownership. The CLI accepts `--provider coinbase|kraken`
and no endpoint argument.

- [ ] **Step 6: Request Cargo and verify GREEN**

Run the three focused commands from Step 3 plus:

```bash
cargo test -p market-squawk-platform --all-features --locked
cargo test -p market-squawk --all-features --locked
```

Expected: PASS with no warnings. Release the slot.

### Task 3: Shipping Hardened MCP, Durable Audit/Artifacts, and SDK Reaping

**Files:**
- Modify: `crates/market-squawk-mcp/src/audit.rs`
- Modify: `crates/market-squawk-mcp/src/framing.rs`
- Modify: `crates/market-squawk-mcp/src/isolation.rs`
- Modify: `crates/market-squawk-mcp/src/server.rs`
- Modify: `crates/market-squawk-mcp/src/lib.rs`
- Test: `crates/market-squawk-mcp/tests/hostile_boundaries.rs`
- Test: `crates/market-squawk-mcp/tests/lifecycle_protocol.rs`
- Modify: `crates/market-squawk-platform/src/paths.rs`
- Create: `apps/market-squawk/src/mcp/audit.rs`
- Create: `apps/market-squawk/src/mcp/artifact.rs`
- Create: `apps/market-squawk/src/mcp/services.rs`
- Replace: `apps/market-squawk/src/mcp.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces a pre-service `MutationAuditReservation` for every non-read-only descriptor.
- Produces `ArtifactRoot::install_content_addressed(...)` with bounded no-follow staging, file and
  parent fsync, digest verification, atomic no-clobber installation, and opaque relative identity.
- Produces an owned SDK worker/reaper whose timed-out thread is retained and eventually joined.

- [ ] **Step 1: Write the MCP causal RED**

Extend one hostile-boundary test with a mutating service and a writer that fails after service
completion. The audit sink pre-reserves admission and terminal capacity; assert the mutation occurs
only after admission persistence, terminal service outcome exists exactly once, and a separate
`OutputUnavailable` record exists. Add a bounded reaper test whose cooperative worker crosses the
join deadline, transfers to the pre-reserved reaper, then drains cleanly.

- [ ] **Step 2: Request Cargo and verify RED**

Run:

```bash
cargo test -p market-squawk-mcp --test hostile_boundaries --locked
cargo test -p market-squawk-mcp --test lifecycle_protocol --locked
```

Expected: FAIL because mutation completion capacity is not pre-reserved and blocking SDK ownership
uses abort-only `spawn_blocking`. Release the slot.

- [ ] **Step 3: Implement independent mutation audit**

Before a non-read-only service call, reserve admitted and terminal capacity, persist admitted
evidence, then invoke the service. Commit terminal service class as soon as the service reaches its
authoritative outcome. Keep response-publication completion separate so artifact/stdout failure
records `OutputUnavailable` without changing the service outcome.

- [ ] **Step 4: Implement bounded retained SDK ownership**

Replace `spawn_blocking` ownership with an explicitly named SDK thread, cooperative cancellation,
outcome channel, bounded join deadline, and pre-reserved reaper admission. On timeout transfer the
`std::thread::JoinHandle` to the reaper; on reaper admission failure join synchronously rather than
detach. Application MCP shutdown drains the reaper before returning.

- [ ] **Step 5: Implement controlled local services**

Use `LocalPaths::artifacts()` and the atomic content-addressed install API. Implement a bounded
durable audit sink and transport-neutral diagnostic services with closed schemas and result limits.
Replace all production CLI calls to the legacy server with `market_squawk_mcp::McpServer`; remove
the legacy framing/handler implementation and its redundant tests.

- [ ] **Step 6: Request Cargo and verify GREEN**

Run the two focused MCP tests plus:

```bash
cargo test -p market-squawk-mcp --all-features --locked
cargo test -p market-squawk --all-features --locked
```

Expected: PASS with no warnings. Release the slot.

### Task 4: Coinbase Fixture Provenance and Exact-Head Gates

**Files:**
- Modify: `adapters/market-squawk-adapter-coinbase/fixtures/manifest.json`
- Modify: `adapters/market-squawk-adapter-coinbase/tests/decode.rs`
- Inspect: every changed public schema digest/revision domain and all exhaustive `SourceError`
  matches.

- [ ] **Step 1: Write provenance RED**

Require exact nonempty `authoritative_url`, ISO retrieval date, `derivation`, `protocol_revision`,
and `terms_url` fields in the existing manifest test while retaining all five SHA-256 checks.

- [ ] **Step 2: Request Cargo and verify RED**

Run:

```bash
cargo test -p market-squawk-adapter-coinbase --test decode \
  official_protocol_fixtures_match_the_pinned_manifest --locked
```

Expected: FAIL because the manifest omits provenance. Release the slot.

- [ ] **Step 3: Add reviewed provenance and verify GREEN**

Populate the manifest from the pinned official Coinbase Exchange protocol documentation and terms
already used by the adapter review. Run the focused test; expected PASS. Release the slot.

- [ ] **Step 4: Run blast-radius and dirty-candidate gates with a serialized slot**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
```

Also run direct dependency, vulnerability, license, credential, and generated-artifact checks used
by the repository. Confirm release symbols contain no guarded local connector and no legacy MCP
entry point is reachable. Record exact command outcomes, then release the slot.

- [ ] **Step 5: Commit, re-run clean exact-head gates, and hand off**

Commit only intended files, confirm `git status --short` is empty, repeat the required exact-head
gates under the serialized slot, and report branch, worktree, base, head SHA, focused RED/GREEN
evidence, full gates, and any remaining blocker. Do not push or clean up this worktree; the
integration owner performs merge and lifecycle cleanup after acceptance.
