# GitHub Batch 002 Deep Dive

**Topic:** Market Squawk complete local platform architecture, source adapters, analytics,
risk, valuation, and MCP implementation evidence
**As-of/access date:** 2026-07-15
**Scope:** `microsoft/qlib`, `OpenBB-finance/OpenBB`,
`modelcontextprotocol/rust-sdk`, and `pykeio/ort` only

## Table of Contents

- [Executive assessment](#executive-assessment)
- [Evidence table](#evidence-table)
- [Repository findings](#repository-findings)
  - [Qlib](#qlib)
  - [OpenBB](#openbb)
  - [MCP Rust SDK](#mcp-rust-sdk)
  - [ort](#ort)
- [Cross-source implications](#cross-source-implications)
- [Limitations and non-findings](#limitations-and-non-findings)
- [Source list](#source-list)

## Executive assessment

- **Inference — adopt as a dependency:** Use the official MCP Rust SDK for local stdio
  protocol/schema plumbing, but pin an exact release, enable only the necessary server and
  stdio features, and put Market Squawk authorization, audit, result bounds, artifacts, and
  execution-risk enforcement above it.
- **Inference — conditionally adopt as a dependency:** `ort` is a strong candidate for the
  ONNX inference backend only when default build-time binary downloading is disabled. Load a
  locally provisioned, hash-verified ONNX Runtime and accept only trusted model bundles.
- **Inference — reference, do not embed:** Qlib provides useful patterns for point-in-time
  data, train/validation/test segmentation, experiment records, signals, and backtests. Its
  Python/MLflow/pickle-oriented workflow is not the local Rust production registry required by
  Market Squawk.
- **Inference — reference only absent a deliberate license decision:** OpenBB demonstrates a
  broad provider/fetcher architecture and useful testing/rate-limit patterns, but its repository
  license is AGPL-3.0. Its FRED path also discards vintage fields, so it cannot be adopted as
  Market Squawk's ALFRED/PIT macro implementation as-is.

## Evidence table

| Source | Evidence | Status | Market Squawk implication |
|---|---|---|---|
| Qlib | The project describes data processing, training, backtesting, portfolio and order-execution workflows; it also warns that its public Yahoo-derived dataset is imperfect and that official dataset access is temporarily disabled because of restrictions. [README](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/README.md) | **Confirmed** | **Inference:** Reuse workflow concepts, not data or coverage assumptions. Every Market Squawk source needs explicit coverage and quality metadata. |
| Qlib | PIT storage records publication date, period, value, and a next-record offset, while documenting quarterly/annual-factor and performance limitations. [PIT documentation](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/advanced/PIT.rst) | **Confirmed** | **Inference:** Preserve `published_at`, `available_at`, revision, and supersession in canonical Parquet datasets; do not copy Qlib's specialized file format. |
| Qlib | The recorder abstraction stores parameters, metrics, and artifacts through experiments; its documented implementation is MLflow, and saved Python objects use pickle with cross-environment limitations. [Recorder documentation](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/component/recorder.rst) | **Confirmed** | **Inference:** Build a local manifest/SQLite model registry and safe, explicit artifact formats; never load untrusted pickle. |
| OpenBB | `Fetcher` separates query transformation, extraction, and result transformation and includes a test path for that lifecycle. [Fetcher abstraction](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/core/openbb_core/provider/abstract/fetcher.py) | **Confirmed** | **Inference:** Mirror the separation in Market Squawk extraction adapters while retaining typed provenance, hashes, health, and idempotency. |
| OpenBB | The repository license states that its files are AGPL-3.0. [LICENSE](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/LICENSE) | **Confirmed** | **Inference:** Treat implementation code as reference material unless project licensing is intentionally changed after legal review. |
| OpenBB | Its FRED series transformer removes `realtime_start` and `realtime_end` from returned observations. [FRED series implementation](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/providers/fred/openbb_fred/models/series.py) | **Confirmed** | **Inference:** Implement FRED and ALFRED separately enough to preserve vintages and availability-time semantics. |
| MCP Rust SDK | The SDK supports Tokio-based servers, stdio transport, tool routing, structured schemas, resources, prompts, and cancellation. [README](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/README.md) | **Confirmed** | **Inference:** It is the preferred protocol layer for Market Squawk's local typed MCP service. |
| MCP Rust SDK | The workspace is version 2.2.0 and Edition 2024, while the same commit's README dependency example still shows `0.16.0`. [Cargo.toml](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/Cargo.toml), [README](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/README.md) | **Confirmed** | **Inference:** Pin and validate the selected crate release rather than copying the stale example. |
| `ort` | `ort` 2.0.0-rc.12 targets ONNX Runtime 1.27, Edition 2024, and Rust 1.88; `ort-sys` is pinned to the same release candidate. [Cargo.toml](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/Cargo.toml) | **Confirmed** | **Inference:** Compatibility with Rust 1.97 is plausible, but the release-candidate status requires a pinned integration and regression suite. |
| `ort` | Default features include binary downloading and native TLS; the distribution manifest supplies CDN URLs and SHA-256 values. [Cargo.toml](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/Cargo.toml), [distribution manifest](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/ort-sys/dist.txt) | **Confirmed** | **Inference:** Use `default-features = false`; provision and verify the runtime locally so builds make no hidden outbound requests. |
| `ort` | The security policy identifies malicious ONNX models as an underlying runtime risk and says the 1.x line is unmaintained while 2.x support is transitioning toward stable. [SECURITY.md](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/SECURITY.md) | **Confirmed** | **Inference:** Treat model bundles as trusted executable inputs, verify hashes and schemas, and prohibit remote model loading. |

## Repository findings

### Qlib

**Confirmed:** Qlib has a coherent research workflow: configuration drives data loading and
processing, explicit train/validation/test segments, model fitting and inference, then signal and
portfolio analysis. Its workflow can instantiate Python classes from configured module paths.
[Workflow documentation](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/component/workflow.rst)

**Inference:** Market Squawk should adopt the explicit temporal split and recorded experiment
lineage, not dynamic module loading. Only trusted, compiled inference backends and registered
feature implementations should enter production; arbitrary module paths conflict with the ban on
remote code loading. Qlib's PIT design is evidence that publication-time-aware lookup is essential,
but the Market Squawk schema should use general `effective_at`, `published_at`, `available_at`,
`revision`, and `superseded_at` fields for filings, fundamentals, and macro series.

**Confirmed:** Qlib CI spans multiple operating systems and Python versions and checks formatting,
typing, documentation, notebooks, downloads, configured workflows, and tests.
[CI workflow](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/.github/workflows/test_qlib_from_source.yml)
**Inference:** This supports Qlib's maturity as a design reference, but mutable action tags and
network-installed packages do not meet Market Squawk's desired release reproducibility without
additional pinning and audits.

### OpenBB

**Confirmed:** OpenBB's provider core resolves provider-specific fetchers, removes secret values
from ordinary argument handling, and executes the fetch lifecycle through a registry.
[Query executor](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/core/openbb_core/provider/query_executor.py)
Its tree contains distinct SEC, FRED, BLS, and U.S. government provider packages. BLS and Treasury
tests use recorded HTTP fixtures, which is a useful pattern for deterministic default tests.
[BLS tests](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/providers/bls/tests/test_bls_fetchers.py),
[U.S. government tests](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/providers/government_us/tests/test_government_us_fetchers.py)

**Confirmed:** The inspected FRED utility implements bounded retry behavior, respects `Retry-After`,
rate-spaces requests, caches results, coalesces duplicate in-flight requests, and excludes the API
key from the cache key. [FRED rate limiter](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/openbb_platform/providers/fred/openbb_fred/utils/rate_limiter.py)
**Inference:** Those are resilience patterns worth independently implementing; source health should
degrade explicitly when documented limits are reached.

### MCP Rust SDK

**Confirmed:** The SDK's `rmcp` crate exposes optional client, server, HTTP, authentication, child
process, and Unix-socket capabilities; server/schema support does not require adopting all remote
transports. [rmcp Cargo features](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/crates/rmcp/Cargo.toml)
Its CI exercises formatting, Clippy, tests, semver/public-API checks, coverage, examples, security
audit, protocol conformance, and cross-language behavior.
[CI workflows](https://github.com/modelcontextprotocol/rust-sdk/tree/839922d8fd44216024b23ae72d16d1eae8cbf013/.github/workflows)

**Inference:** Select the smallest feature set for an stdio server. MCP handlers should call the
same application services as the CLI and remain outside the live path. The SDK is not an
authorization or risk engine: Market Squawk still owns tool allowlists, typed limits, cancellation,
audit records, controlled artifact references, secret redaction, and mandatory risk evaluation.
Stdio server logs must go to stderr so stdout remains valid protocol traffic, as the example does.
[stdio example](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/examples/servers/src/calculator_stdio.rs)

**Confirmed:** The license describes a transition from MIT to Apache-2.0, with retained MIT history
and CC-BY-4.0 documentation treatment. [LICENSE](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/LICENSE)
**Inference:** Audit the exact packaged release rather than assuming a single license from repository
metadata.

### ort

**Confirmed:** `ort` is a safe Rust wrapper around the unsafe ONNX Runtime C API, and its workflows
test major desktop platforms, no-default-feature configurations, dynamic loading, and supported API
levels. [README](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/README.md),
[other checks](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/.github/workflows/other-checks.yml)

**Inference:** Isolate `ort` behind `InferenceBackend` in the modeling crate. At bundle load, verify
the artifact hash, ONNX/runtime versions, feature schema and versions, tensor names/shapes/types,
normalization, thresholds, training window, universe, and dataset versions. Warm the session before
use; perform no disk or network I/O per event; return no automated action on any inference error.
The wrapper supplies tensor/session mechanics, not a model registry or bundle trust policy.

## Cross-source implications

1. **Inference:** Point-in-time correctness belongs in Market Squawk's canonical research schemas
   and dataset builder. Neither a Qlib-specific PIT binary layout nor OpenBB's current FRED series
   transformation satisfies filing-and-macro revision preservation broadly.
2. **Inference:** Provider contracts should separate discover/extract/normalize phases, but preserve
   source objects, hashes, timestamps, revisions, coverage, quality, and lawful backoff at every
   boundary. OpenBB is useful architectural evidence, not reusable dual-licensed implementation.
3. **Inference:** A production model path combines a Market Squawk-owned bundle/registry with an
   isolated `ort` backend. Qlib's experiment records are conceptual input; pickle and mandatory
   MLflow infrastructure are unsuitable defaults.
4. **Inference:** MCP should be narrow orchestration over bounded application services. It must never
   expose unrestricted SQL, filesystem access, shell execution, credential access, or risk bypass.

## Limitations and non-findings

- **Confirmed non-finding:** Scoped search of OpenBB's inspected FRED provider did not find an
  ALFRED vintage implementation; the inspected series path explicitly drops real-time bounds.
- **Confirmed non-finding:** No reviewed repository provides Market Squawk's complete model-bundle
  metadata, execution-quality qualification, fair-value classification, or enforced pre-trade risk
  contract. These remain application-owned capabilities.
- **Confirmed limitation:** This review covered repository code, documentation, tests, CI, licenses,
  and security policies at the pinned commits. It did not execute their external-network suites,
  conduct a legal opinion, or independently audit all transitive dependencies.
- **Confirmed limitation:** Repository activity and release state can change after the access date;
  lockfile and license/security audits must target the exact versions selected for Market Squawk.

## Source list

1. `microsoft/qlib`, pinned at [`d5379c5`](https://github.com/microsoft/qlib/commit/d5379c520f66a39953bad76234a7019a72796fd0), accessed 2026-07-15.
2. `OpenBB-finance/OpenBB`, pinned at [`c78488d`](https://github.com/OpenBB-finance/OpenBB/commit/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690), accessed 2026-07-15.
3. `modelcontextprotocol/rust-sdk`, pinned at [`839922d`](https://github.com/modelcontextprotocol/rust-sdk/commit/839922d8fd44216024b23ae72d16d1eae8cbf013), accessed 2026-07-15.
4. `pykeio/ort`, pinned at [`9c840a3`](https://github.com/pykeio/ort/commit/9c840a386acc808aaaf5ac28ae0fc13ee164678c), accessed 2026-07-15.
