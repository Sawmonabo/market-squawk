# GitHub Synthesis

## Table of Contents

- [Category Scope](#category-scope)
- [Sources Covered](#sources-covered)
- [High-Confidence Findings](#high-confidence-findings)
- [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
- [Conflicts and Disagreements](#conflicts-and-disagreements)
- [Trends and Patterns](#trends-and-patterns)
- [Implications for the Research Topic](#implications-for-the-research-topic)
- [Gaps](#gaps)
- [Source Matrix](#source-matrix)

## Category Scope

This synthesis consolidates three GitHub batches covering ten repositories as of
**2026-07-15**. It evaluates their evidence for Market Squawk's Rust architecture, live adapters,
research storage, modeling, valuation/risk, local MCP, paper execution, and release hardening. It
does not add repositories or sources beyond the batch reports.

“Confirmed” denotes a claim directly supported by repository code, documentation, tests, release
metadata, license, or policy at the reviewed commit. “Inference” denotes an adoption or design
recommendation for Market Squawk. Repository activity and version information are point-in-time
observations. Upstream policies, tests, or self-described performance are process evidence—not an
independent security, correctness, or performance certification.

## Sources Covered

The ten repositories fall into three adoption tiers:

1. **Pinned direct dependency candidates:**
   [`apache/arrow-rs`](https://github.com/apache/arrow-rs),
   [`apache/datafusion`](https://github.com/apache/datafusion), and
   [`modelcontextprotocol/rust-sdk`](https://github.com/modelcontextprotocol/rust-sdk).
2. **Conditional direct dependency candidate:**
   [`pykeio/ort`](https://github.com/pykeio/ort), only with default binary-download/network
   features disabled, a locally provisioned verified runtime, trusted model bundles, and a pinned
   release-candidate integration.
3. **Architectural, test, or offline-oracle references:**
   [`nautechsystems/nautilus_trader`](https://github.com/nautechsystems/nautilus_trader),
   [`barter-rs/barter-rs`](https://github.com/barter-rs/barter-rs),
   [`microsoft/qlib`](https://github.com/microsoft/qlib),
   [`OpenBB-finance/OpenBB`](https://github.com/OpenBB-finance/OpenBB),
   [`OpenGamma/Strata`](https://github.com/OpenGamma/Strata), and
   [`krakenfx/kraken-cli`](https://github.com/krakenfx/kraken-cli).

The direct/reference distinction reflects product fit, not a ranking of project quality. Arrow
Rust and DataFusion supply the specified analytical mechanics; the MCP SDK supplies protocol
plumbing. The other projects contain useful contracts, fixtures, and hardening examples but also
carry architecture, language/runtime, license, source-coverage, or assurance mismatches that make
wholesale embedding inappropriate.

## High-Confidence Findings

### Direct research-plane dependencies are available

**Confirmed.** Arrow Rust is the official Apache Rust implementation of Arrow and includes the
Parquet Rust implementation. DataFusion is an Apache-licensed Rust query engine using Arrow's
in-memory format, with SQL/DataFrame APIs, built-in local file formats, extension points, examples,
benchmarks, and an API-deprecation policy
([Arrow README](https://github.com/apache/arrow-rs/blob/ee30b61b00df8a590c4c45c490fbecc0962cfba5/README.md),
[DataFusion README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md)).
Both were active at the reviewed commits; Arrow 59.1.0 was released July 7, 2026
([release](https://github.com/apache/arrow-rs/releases/tag/59.1.0)).

**Inference.** Adopt compatible pinned `arrow`, `parquet`, and DataFusion versions behind
Market Squawk-owned dataset services. Canonical schemas, decimal/time policy, point-in-time logic,
provenance, revisions, deduplication, manifests, partitioning, cancellation, and resource/result
bounds remain application responsibilities. Keep these dependencies and their query/file I/O out
of the live event-to-action dependency graph.

### MCP protocol plumbing exists, but authorization remains application-owned

**Confirmed.** The official Rust MCP SDK supports Tokio servers, stdio transport, typed tool
routing and schemas, resources, prompts, and cancellation. Optional features permit a small stdio
server without enabling every remote transport. Its CI includes formatting, Clippy, tests,
semver/public-API checks, audit, protocol conformance, and cross-language checks
([README](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/README.md),
[`rmcp` features](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/crates/rmcp/Cargo.toml),
[workflows](https://github.com/modelcontextprotocol/rust-sdk/tree/839922d8fd44216024b23ae72d16d1eae8cbf013/.github/workflows)).

**Inference.** Use it as a pinned, minimal-feature protocol dependency. Market Squawk must still
own tool allowlists, bounded schemas/results, time/instrument limits, cancellation propagation,
audit records, controlled artifacts, redaction, and the mandatory risk boundary. Handlers should
call the same application services as the CLI, stay outside the live path, and log to stderr so
stdio protocol output remains valid.

### ONNX inference is feasible only under a constrained trust and build model

**Confirmed.** `ort` is a safe Rust wrapper around the unsafe ONNX Runtime C API. The reviewed
2.0.0-rc.12 targets ONNX Runtime 1.27, Edition 2024, and Rust 1.88, and tests several platforms,
no-default-feature builds, dynamic loading, and API levels
([manifest](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/Cargo.toml),
[README](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/README.md)).
Default features include binary downloading and native TLS. Its security policy flags malicious
ONNX models as an underlying runtime risk and says 1.x is unmaintained while 2.x transitions toward
stable
([security policy](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/SECURITY.md)).

**Inference.** Isolate `ort` behind `InferenceBackend`, disable default features, locally provision
and hash-verify the runtime, and reject remote or untrusted models. Bundle loading must verify model
hash/runtime version, feature schema, tensor names/shapes/types, normalization, universe, training
and dataset versions, thresholds, and fallback. Warm sessions before use; per-event inference must
perform no disk or network I/O and every error must yield no automated action.

### Trading engines are valuable references, not proof of the required live contract

**Confirmed.** NautilusTrader has broad Rust crates for live data, execution, risk, portfolio,
persistence, models, backtests, and adapters. Its official-adapter inventory includes Coinbase and
Kraken. The inspected Kraken Spot L3 code implements CRC32 checksum fixtures and bounded resync,
but its checksum cache also uses floating-point prices/sizes; the separately inspected L2 state file
did not itself perform a checksum comparison
([crates](https://github.com/nautechsystems/nautilus_trader/tree/c7d60c1d6e64d72076f8cd2a652d199263679223/crates),
[adapter inventory](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/ADAPTERS.md),
[L3 checksum](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs),
[resync](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/resync.rs)).

**Confirmed.** Barter provides clean transport/parser/transformer, reconnecting-stream,
strategy/risk-hook, execution-client, centralized state, mock exchange, and out-of-band audit
patterns. Its documented support table, however, lists Coinbase trades but not books and Kraken
trades/L1 but not required checksum depth. It expressly disclaims production/commercial live-
trading fitness
([data support](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-data/README.md),
[core](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter/README.md),
[disclaimer](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md#legal-disclaimer-and-limitation-of-liability)).

**Inference.** Borrow adapter decomposition, audit replication, checksum/resync fixtures, domain
boundaries, and benchmark ideas—not the platform core or upstream execution-quality conclusion.
Market Squawk must independently implement scaled-integer parsing, exact venue representations,
snapshot/delta state, sequence/checksum/freshness/status/precision/coverage validation, typed
quality transitions, bounded queues, and quarantine/resynchronization. An “official adapter” is a
maintenance designation, not `DirectVerified` evidence.

### Official Kraken client code supports lawful transport patterns, not a book engine

**Confirmed.** Kraken CLI uses rustls, HMAC signing, fresh nonces, idempotency-aware bounded retry,
server-authoritative rate-limit errors, bounded WebSocket reconnect/backoff, and stable-session reset
behavior. Its production WebSocket paths render frames but do not maintain or validate an
instrument-owned book
([REST client](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/client.rs),
[spot WebSocket](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/commands/websocket.rs)).
Its paper modes omit required fill dimensions, and its MCP acknowledgment can be disabled
([paper implementation](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/paper.rs),
[MCP server](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/mcp/server.rs)).

**Inference.** Use this official repository for signing, error-taxonomy, retry, reconnect, fixture,
and release-reference shapes. Add a lawful local limiter, cooldown/source-health policy, typed frame
decoder, book integrity state, and quarantine. Never copy its persistent client tracking or expose
TLS verification, endpoint allowlist, MCP acknowledgment, or other “danger” switches as production
bypasses.

### Research/provider/valuation references expose useful boundaries

**Confirmed.** Qlib documents train/validation/test workflows, experiment parameters/metrics/
artifacts, and a specialized point-in-time store containing publication date and period. Its
documented recorder uses MLflow and saved Python objects use pickle; its public data is described as
imperfect and official dataset access as temporarily restricted
([PIT](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/advanced/PIT.rst),
[recorder](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/component/recorder.rst),
[README](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/README.md)).

**Confirmed.** OpenBB separates query transformation, extraction, and result transformation; has
SEC/FRED/BLS/U.S.-government provider packages, recorded HTTP fixtures, secret-aware dispatch, and
bounded retry/cache/request-coalescing patterns. Its inspected FRED transformer drops
`realtime_start` and `realtime_end`, and its repository is AGPL-3.0
([Fetcher](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/core/openbb_core/provider/abstract/fetcher.py),
[FRED series](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/providers/fred/openbb_fred/models/series.py),
[license](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/LICENSE)).

**Confirmed.** Strata separates products, market data, pricers, calculation orchestration, and
measures; declares required observables and supplies deterministic scenario, PV, PV01, vega,
exposure, curve, and sensitivity patterns. It is Apache-2.0 and Java-based
([requirements](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/MarketDataRequirements.java),
[measures](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/measure/src/main/java/com/opengamma/strata/measure/Measures.java),
[scenarios](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/ScenarioDefinition.java)).

**Inference.** Reimplement these concepts in Rust-owned services: bitemporal/revision-aware
research schemas, safe local manifests instead of pickle/mandatory MLflow, typed extraction
adapters preserving raw objects/vintages/provenance, and dependency-explicit valuation/scenario
kernels. Qlib/OpenBB are references rather than embedded Python platforms; Strata may serve as an
offline numerical oracle, never a Java or floating-point dependency in the live order path.

### Release and security policy must be adopted as a process, not inherited as assurance

**Confirmed.** NautilusTrader documents the most comprehensive release-security posture in the set:
locked dependencies, cargo-vet/audit/deny and OSV scanning, Gitleaks, Zizmor, CodeQL, fuzzing,
provenance, checksum manifests, SBOMs, signed artifacts/images, and vulnerability support policy
([security policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md)).
Kraken CLI produces checksums, signatures, and artifact attestations, but delegates part of its
security workflow through a mutable external reference
([release workflow](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/.github/workflows/release.yml),
[SecureSDLC](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/.github/workflows/securesdlc.yml)).

**Inference.** Market Squawk should independently require a committed lockfile, exact toolchain and
CI-action pinning, dependency/vulnerability/license/credential audits, fuzz targets, reproducible
checksummed artifacts, SBOM/provenance where locally practical, and a vulnerability response
policy. Containers, cloud publishing, telemetry, and signed images remain optional rather than
core requirements.

## Medium- and Low-Confidence Findings

- **Medium:** `ort` is plausibly compatible with Rust 1.97 because its manifest declares Rust 1.88,
  but 2.0.0-rc.12 is not a stable release and the underlying native runtime expands the security and
  packaging surface. Compatibility and deterministic inference require a pinned local build/test
  matrix, not inference from metadata alone.
- **Medium:** The MCP SDK is an appropriate direct protocol dependency, but the reviewed workspace
  reports version 2.2.0 while the same commit's README example still shows `0.16.0`. The exact
  packaged release, features, semver, and mixed Apache/MIT-history/CC-BY documentation licensing
  require package-level verification
  ([manifest](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/Cargo.toml),
  [license](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/LICENSE)).
- **Medium-high:** Barter's mock execution documentation does not establish latency, fees,
  slippage, queue position, partial fills, balances, or calibrated rejection behavior. This is a
  documentation-scoped non-finding, not proof about every downstream implementation
  ([execution README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-execution/README.md)).
- **Medium-high:** OpenBB's inspected FRED path drops vintage bounds and no ALFRED implementation was
  found in the scoped provider search. The finding is pinned to the reviewed files and does not
  prove that no later or external provider supports vintages.
- **Medium:** DataFusion freshness was established through repository activity and roadmap rather
  than a current GitHub Releases record. Exact crate compatibility must be resolved in Cargo, not
  inferred from repository HEAD.
- **Low as transferable evidence:** stars, forks, benchmark adjectives, README production claims,
  and upstream latency numbers are not acceptance evidence. They are volatile popularity or
  self-description signals.

## Conflicts and Disagreements

Most tensions are scope or packaging conflicts rather than direct repository-to-repository
disagreement:

1. **Official support versus execution qualification.** NautilusTrader labels Coinbase/Kraken
   adapters official and Kraken CLI is maintained by Kraken, but neither fact proves Market Squawk's
   complete `DirectVerified` predicate. Maintenance provenance and data integrity are different
   claims.
2. **Broad provider coverage versus point-in-time preservation.** OpenBB exposes many providers and
   strong fetch lifecycle patterns, yet the inspected FRED transformation discards real-time bounds.
   Breadth does not imply revision/vintage correctness.
3. **Convenient defaults versus local-first security.** `ort` defaults to binary downloading and TLS,
   which conflicts with Market Squawk's no-hidden-outbound/reproducible-build constraint. The
   wrapper remains usable only after disabling that behavior.
4. **SDK documentation versus workspace version.** The MCP Rust SDK's README dependency example
   (`0.16.0`) disagrees with the reviewed workspace version (`2.2.0`). Exact package metadata takes
   precedence; examples must not drive version selection.
5. **Trading capability language versus assurance.** Barter presents live/paper/backtest components
   while explicitly disclaiming production fitness. Treat API availability as design evidence, not
   verification.
6. **Kraken paper documentation versus behavior.** The Kraken CLI batch found a conflict over
   configurable spot slippage between documentation and code. Its fixtures require behavioral
   verification before reuse; neither side establishes Market Squawk's realistic paper baseline.
7. **User acknowledgment versus risk.** Kraken CLI can gate dangerous MCP calls with an
   acknowledgment that a switch can disable. Market Squawk requires non-bypassable quantitative
   and data-quality risk, so this pattern is explicitly unsuitable as an approval boundary.

## Trends and Patterns

1. **Compose narrow dependencies; own domain policy.** Strong repositories supply mechanics—columnar
   arrays, query execution, protocol routing, tensors, signing, retries, or pricing kernels. None
   supplies Market Squawk's whole quality, provenance, point-in-time, fair-value, or risk contract.
2. **Transport is not state qualification.** Reconnect, normalized frames, and successful parsing
   must precede sequence, checksum, snapshot, freshness, precision, status, coverage, and quarantine
   validation—not replace them.
3. **Research correctness lives above generic tooling.** Arrow/DataFusion, Qlib, and OpenBB do not
   infer bitemporal semantics, corporate-action policy, revision identity, or leakage prevention.
4. **Capability minimization is a security control.** Disable unneeded transports, binary downloads,
   arbitrary Python loading, pickle, remote models, unrestricted SQL/filesystem access, endpoint
   bypasses, and persistent tracking.
5. **License and version are architecture inputs.** Apache/MIT dependencies are easiest to adopt;
   NautilusTrader's LGPL and OpenBB's AGPL require deliberate legal/architectural decisions. Release
   candidates, latest-only support, stale examples, and mutable CI references increase maintenance
   burden.
6. **Fixtures and policies transfer better than claims.** Recorded HTTP responses, checksum vectors,
   Wiremock tests, benchmark methodology, idempotency-aware retry, security checklists, and offline
   numerical oracles are reusable evidence. Upstream “production,” “official,” “fast,” or “safe”
   labels are not.

## Implications for the Research Topic

The following are consolidated **Inferences**:

- Pin Arrow/Parquet and DataFusion as research-only dependencies behind versioned schemas,
  manifests, path controls, memory/cancellation limits, point-in-time predicates, and bounded query
  services. Permit read-only CLI SQL if desired; never expose unrestricted MCP SQL.
- Pin the MCP Rust SDK with only server/stdio/schema capabilities. Add schema-limit, cancellation,
  artifact-path, stdout-purity, audit, redaction, and “no execution without application risk” tests.
- Gate `ort` behind an optional ONNX feature with default features disabled. Verify a locally
  provisioned runtime and trusted model bundle, warm before the live loop, prohibit remote loading,
  and test that every load/inference/schema error yields no action.
- Implement Coinbase and Kraken adapters application-first. Use pinned NautilusTrader and Kraken
  CLI fixtures as comparison evidence, then test raw decimal conversion, snapshot/delta order,
  checksum strings, sequence gaps/duplicates, connection generations, reconnect budgets,
  rate-limit cooldowns, book crossing, freshness/status, and quarantine/resync.
- Implement extraction sources around discover/extract/normalize stages inspired by OpenBB, while
  retaining raw objects, hashes, source timestamps, availability/vintage fields, coverage, lawful
  caching, bounded retry, and deterministic recorded-network fixtures.
- Build a local SQLite/manifest model and experiment registry from Qlib's lineage concepts, using
  explicit artifact formats. Reject pickle, arbitrary module paths, mandatory MLflow, or cloud
  services as production defaults.
- Build valuation and scenario services around Strata-like declared dependencies, units,
  currencies, measures, and named ordered perturbations. Keep analytical floating-point conversion
  explicit; use Rust kernels and optional offline oracle comparisons. Implement ASC 820/IFRS 13
  hierarchy and evidence separately from the pricing library.
- Make risk approval one application-owned constructor path into execution. Strategies, models,
  adapters, CLI, and MCP may emit/request typed intents but cannot construct approved orders or call
  execution around the risk service.
- Qualify paper execution independently of Barter/Kraken mocks: fees, latency, available liquidity,
  slippage, partial fills, rejects, cancellation races, balances, positions, and order-state
  transitions need calibrated models and property/integration tests.
- Copy security posture, not optional infrastructure: locked and audited dependencies, exact CI
  pins, fuzzing, credential scanning, license checks, checksum/provenance artifacts, secret
  redaction, and documented response. No telemetry, cloud, container, or outbound fetch is required.
- Respect provider limits through local throttles, `Retry-After`, bounded retry, caching,
  coalescing, cooldown and health degradation.

## Gaps

- No reviewed repository implements the complete `DirectVerified` contract, including known
  source/venue/instrument, authorized direct delivery, sequence/snapshot/checksum integrity,
  timestamps, freshness, status, precision, coverage, quarantine, and resynchronization.
- None supplies Market Squawk's non-bypassable pre-trade risk service. Strategy hooks, risk traits,
  paper acknowledgments, or trading-enabled flags are insufficient.
- No repository establishes a complete realistic paper adapter with calibrated fees, latency,
  slippage, queue/liquidity, partial fills, rejection/cancellation races, balances, positions, and
  full order-state reconciliation.
- Arrow/DataFusion provide mechanics, not canonical schemas, source lineage, idempotency,
  bitemporal joins, revisions, historical constituents, corporate-action policy, leakage checks, or
  controlled compaction.
- Qlib/OpenBB do not provide the required safe local Rust registry, general PIT dataset builder, or
  complete SEC/FRED/ALFRED/BLS/Treasury Rust adapters. OpenBB's AGPL obligations were flagged but not
  resolved.
- Strata supplies pricing/risk patterns, not an ASC 820/IFRS 13 hierarchy, evidence, override,
  approval, or Level 1 eligibility engine.
- The MCP SDK is not an authorization, artifact-control, audit, result-bounding, or risk system.
  `ort` is not a model registry, trust policy, feature registry, or decision-control system.
- No upstream tests or benchmarks were executed in these reviews; transitive dependencies were not
  audited, and security/performance claims were not independently replicated.
- No repository proves Market Squawk's Rust 1.97 all-feature build, 100,000 events/s,
  sub-millisecond warmed p99, bounded burst memory, fuzz/audit coverage, or release readiness.
- License observations are engineering research summaries and do not determine legal rights or
  obligations. Exact packaged releases, notices, feature graphs,
  transitive licenses, and LGPL/AGPL linkage/derivative-work questions require release-specific
  review.

## Source Matrix

| ID | Repository and pinned lineage | License | Maintenance/version signal at cutoff | Adoption posture | Principal caveat |
| --- | --- | --- | --- | --- | --- |
| github-001 | [`nautechsystems/nautilus_trader` @ `c7d60c1`](https://github.com/nautechsystems/nautilus_trader/commit/c7d60c1d6e64d72076f8cd2a652d199263679223) | LGPL-3.0 | v1.230.0 on 2026-06-29; active develop on 2026-07-15; extensive tests/benches/fuzz/security policy | Architectural, fixture, benchmark, and hardening reference | Legal review before linking/copying; official adapters do not prove `DirectVerified`; inspected Kraken representation differs from required scaled integers |
| github-002 | [`barter-rs/barter-rs` @ `33e5618`](https://github.com/barter-rs/barter-rs/commit/33e56188e2095781331f85aa3d7f88e251eec65a) | MIT | v0.12.5 and reviewed HEAD on 2026-05-09; CI/tests present | Trait, reconnect, audit-replica, state, and mock reference | Missing required documented book coverage; production disclaimer; weaker CI/supply-chain and paper-fill evidence |
| github-003 | [`apache/arrow-rs` @ `ee30b61`](https://github.com/apache/arrow-rs/commit/ee30b61b00df8a590c4c45c490fbecc0962cfba5) | Apache-2.0 | 59.1.0 on 2026-07-07; active main on 2026-07-15 | Pinned direct research-plane dependency | Supplies mechanics, not canonical finance schemas or PIT policy; keep out of hot path |
| github-004 | [`apache/datafusion` @ `18121a6`](https://github.com/apache/datafusion/commit/18121a68433ac19763787e9763ef3f50508befd5) | Apache-2.0 | Active main on 2026-07-16 UTC; current release not assessed through GitHub Releases | Pinned direct research-plane dependency | Requires compatible Arrow family, resource bounds, path controls, and application-owned semantics |
| github-005 | [`microsoft/qlib` @ `d5379c5`](https://github.com/microsoft/qlib/commit/d5379c520f66a39953bad76234a7019a72796fd0) | License not re-evaluated in batch evidence | Pinned current repository/code/docs review; multi-OS CI | Research-workflow, PIT, split, and experiment-lineage reference | Python/MLflow/pickle and dynamic loading mismatch; public data restrictions/quality caveats |
| github-006 | [`OpenBB-finance/OpenBB` @ `c78488d`](https://github.com/OpenBB-finance/OpenBB/commit/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690) | AGPL-3.0 | Pinned current provider code/tests review | Provider/fetcher, fixture, retry/cache design reference only | Deliberate license decision required; inspected FRED path drops vintage bounds; no ALFRED found |
| github-007 | [`modelcontextprotocol/rust-sdk` @ `839922d`](https://github.com/modelcontextprotocol/rust-sdk/commit/839922d8fd44216024b23ae72d16d1eae8cbf013) | Apache-2.0 transition with MIT history; docs CC-BY-4.0 treatment | Workspace 2.2.0; active CI/conformance; README example says 0.16.0 | Pinned minimal-feature direct MCP dependency | Verify exact package/version/license; SDK does not provide authorization, bounds, audit, or risk |
| github-008 | [`pykeio/ort` @ `9c840a3`](https://github.com/pykeio/ort/commit/9c840a386acc808aaaf5ac28ae0fc13ee164678c) | License not re-evaluated in batch evidence | 2.0.0-rc.12; targets ONNX Runtime 1.27 and Rust 1.88; 1.x unmaintained | Conditional optional ONNX dependency | RC/native runtime; default binary downloads/TLS; malicious-model risk; must provision locally |
| github-009 | [`OpenGamma/Strata` @ `39c46e3`](https://github.com/OpenGamma/Strata/commit/39c46e342a4a95ac083d66287f038f6ae276692a) | Apache-2.0 | v2.12.73 and HEAD on 2026-07-02; broad tests; latest-only security support | Valuation/risk/scenario design reference and optional offline oracle | Java/floating-point boundary; not fair-value hierarchy policy; upgrade pressure |
| github-010 | [`krakenfx/kraken-cli` @ `aa32814`](https://github.com/krakenfx/kraken-cli/commit/aa32814cea70913a70c9909693a7abd762963e83) | MIT | v0.3.2 and HEAD on 2026-04-20; fixtures and signed/attested releases | Official signing/retry/reconnect/security/fixture reference | No stateful validated book; incomplete paper fills; bypass switches/tracking unsuitable; mutable security workflow reference |
