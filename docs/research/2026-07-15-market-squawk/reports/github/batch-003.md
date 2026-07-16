# GitHub Batch 003 Deep Dive

**Topic:** Market Squawk complete local platform architecture, source adapters, analytics,
risk, valuation, and MCP implementation evidence
**As-of/access date:** 2026-07-15
**Scope:** `OpenGamma/Strata` and `krakenfx/kraken-cli` only

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch evaluates Strata as valuation/risk design evidence and Kraken CLI as official Kraken
protocol/client evidence. It does not treat either repository as proof of Market Squawk's
`DirectVerified` qualification, ASC 820/IFRS 13 classification, or mandatory pre-trade risk gates.

## Sources Reviewed

| Repository | Metadata and maintenance | License | Relevance and caveat |
|---|---|---|---|
| [`OpenGamma/Strata`](https://github.com/OpenGamma/Strata) | **Confirmed:** Java; 950 stars and 313 forks; release [`v2.12.73`](https://github.com/OpenGamma/Strata/releases/tag/v2.12.73) published 2026-07-02; HEAD [`39c46e3`](https://github.com/OpenGamma/Strata/commit/39c46e342a4a95ac083d66287f038f6ae276692a) on 2026-07-02. | **Confirmed:** Apache-2.0. [License](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/LICENSE.txt) | **Inference:** High-quality pricing/scenario reference and potential offline oracle; not a Rust live-path dependency or fair-value-policy engine. |
| [`krakenfx/kraken-cli`](https://github.com/krakenfx/kraken-cli) | **Confirmed:** Rust; 669 stars and 89 forks; release [`v0.3.2`](https://github.com/krakenfx/kraken-cli/releases/tag/v0.3.2) published 2026-04-20; HEAD [`aa32814`](https://github.com/krakenfx/kraken-cli/commit/aa32814cea70913a70c9909693a7abd762963e83) on 2026-04-20. | **Confirmed:** MIT. [License](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/LICENSE) | **Inference:** Useful official protocol, security, retry, and fixture reference; unsuitable as Market Squawk's stateful Kraken book adapter without substantial integrity logic. |

## Findings

**Confirmed:** Strata separates products, market data, pricers, calculation orchestration, and
measures. Its calculation layer derives required observables, non-observable market data, time
series, and output currencies from targets, rules, and requested columns.
[Module overview](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/README.md),
[market-data requirements](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/MarketDataRequirements.java)
**Inference:** Market Squawk should borrow this dependency-explicit calculation shape: valuation
methods declare inputs, units, currencies, and calibration requirements before evaluation. Each
input must additionally carry source, timestamp, quality, and fair-value evidence.

**Confirmed:** Strata supports present value, explain-PV, calibrated and market-quote PV01, vega,
currency exposure, and other measures. Scenario definitions apply named, ordered perturbations to
filtered market data, with consistent scenario counts and first-match behavior.
[Measures](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/measure/src/main/java/com/opengamma/strata/measure/Measures.java),
[ScenarioDefinition](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/ScenarioDefinition.java)
**Inference:** These are strong patterns for valuation explanations, curve risk, and deterministic
stress scenarios. Implement Rust-native analytical kernels and use selected Strata results as
offline regression oracles. Do not import Java into the live event path.

**Confirmed:** Kraken CLI's REST client uses rustls, HMAC signing, fresh nonces, bounded exponential
backoff for transient/5xx failures, and avoids retrying ambiguous non-idempotent 5xx requests. Rate
limits are server-authoritative: it does not pre-throttle and returns enriched rate-limit errors.
[REST client](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/client.rs),
[README](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/README.md)
**Inference:** Reuse the idempotency-aware retry policy and error taxonomy in a native adapter, but
add a lawful local limiter, bounded queues, source-health transitions, and explicit cooldowns. Never
use identity/account rotation, fingerprint spoofing, CAPTCHA bypass, concealment proxies, or
distributed requests to evade provider limits.

**Confirmed:** Spot and futures WebSocket commands implement bounded reconnect, exponential backoff
with jitter, a 12-attempt stream-lifecycle limit, a 120-reconnect/600-second safety budget, and
stable-session reset behavior. They subscribe and render received JSON but do not maintain an
instrument-owned order book.
[Spot WebSocket](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/commands/websocket.rs),
[futures WebSocket](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/commands/futures_ws.rs)
**Inference:** This is connection-management evidence only. Market Squawk must decode typed frames,
initialize snapshots, apply deltas, validate Kraken checksum rules, detect duplicates/gaps and
crossed books, check timestamps/precision/status, and quarantine until resynchronized.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| Strata is actively released and permissively licensed. | [Release](https://github.com/OpenGamma/Strata/releases/tag/v2.12.73), [license](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/LICENSE.txt) | Release 2.12.73 on 2026-07-02; Apache-2.0 text. | High — **Confirmed** | Latest-only security support increases upgrade pressure. |
| Strata offers reusable valuation/risk patterns. | [Measures](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/measure/src/main/java/com/opengamma/strata/measure/Measures.java), [scenarios](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/ScenarioDefinition.java) | Present value, PV01, vega, exposure, scenarios, curves, sensitivities. | High — **Confirmed** | **Inference:** Reimplement selectively in Rust with provenance and explicit rounding. |
| Kraken CLI handles retries conservatively. | [REST client](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/client.rs) | Fresh nonces; idempotency-aware 5xx handling; bounded backoff; immediate rate-limit errors. | High — **Confirmed** | Good adapter-boundary reference. |
| Kraken CLI is not an execution-quality book engine. | [WebSocket production path](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/commands/websocket.rs) | Frames are rendered; checksum values occur in formatting tests, not a maintained/validated live book; no production sequence validator was found. | High for inspected commit — **Confirmed non-finding** | **Inference:** It cannot establish `DirectVerified` by itself. |
| Kraken paper execution is below Market Squawk's baseline. | [README](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/README.md), [`paper.rs`](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/paper.rs) | Spot docs say partial fills are not modeled; futures docs say fills lack depth-based slippage and partial fills. | High — **Confirmed** | Code/docs also conflict on configurable spot slippage; validate behavior before borrowing fixtures. |
| Kraken's MCP acknowledgment is not a risk engine. | [MCP server](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/mcp/server.rs) | Dangerous calls require `acknowledged=true` by default, but `--allow-dangerous` disables the gate. | High — **Confirmed** | **Inference:** Market Squawk must never permit MCP to bypass application risk. |

## Source-Specific Notes

### OpenGamma/Strata

**Confirmed:** CircleCI builds and tests with JDK 8, 11, 17, and 21; Maven runs module tests and
stores JUnit results. The repository contains extensive pricer, measure, curve, sensitivity,
scenario, loader, and regression tests, plus weekly Dependabot configuration.
[CircleCI](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/.circleci/config.yml),
[pricer tests](https://github.com/OpenGamma/Strata/tree/39c46e342a4a95ac083d66287f038f6ae276692a/modules/pricer/src/test),
[Dependabot](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/.github/dependabot.yml)
**Confirmed:** Its security policy supports only the latest release.
[Security policy](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/SECURITY.md)

**Inference:** Strata's Apache license permits deliberate reuse with attribution, but semantic
porting is preferable to adding a Java runtime. Its pricing uses floating point, including unit
prices, so Market Squawk must keep it behind explicit analytical conversion boundaries and never
use such values for orders, balances, fees, cost basis, or accounting amounts.

### krakenfx/kraken-cli

**Confirmed:** The repository has unit, CLI, and Wiremock integration tests for signing, response
parsing, retry, rate-limit classification, output, configuration, paper state, and MCP gates.
[Wiremock tests](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/tests/integration/wiremock_tests.rs)
Release automation produces checksums, minisign signatures, and GitHub artifact attestations.
[Release workflow](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/.github/workflows/release.yml)
Its SecureSDLC workflow delegates to a mutable external `release-stable` workflow reference.
[SecureSDLC workflow](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/.github/workflows/securesdlc.yml)

**Confirmed:** The client sends product, agent-client, and persistent instance-ID headers to Kraken;
it stores the generated instance ID locally. It also exposes explicit danger switches for disabling
TLS verification or allowing non-Kraken endpoint hosts.
[Telemetry module](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/telemetry.rs),
[client safeguards](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/client.rs)
**Inference:** Market Squawk should not copy persistent client tracking or production TLS/allowlist
bypasses. Any provider-required identification must be disclosed, minimal, and separately
configurable.

## Cross-Source Patterns

1. **Inference:** Separate market-data provenance and qualification from valuation consumption.
   Strata-like pricers may consume Level 2/3 inputs, but their output remains modeled evidence—not
   Level 1 or execution-quality data.
2. **Inference:** Separate protocol transport from state validation. Kraken CLI helps with lawful
   connectivity, authentication, retry, and error envelopes; Market Squawk owns sequence/checksum,
   freshness, coverage, quarantine, and resynchronization.
3. **Inference:** Keep all scenario valuation and MCP activity outside the live event-to-action path.

## Limitations and Non-Findings

- **Confirmed non-finding:** Scoped repository searches found no ASC 820 or IFRS 13 hierarchy,
  evidence, override, approval, or Level 1 eligibility engine in Strata.
- **Confirmed non-finding:** No `DirectVerified` policy, full live checksum validator, sequence-state
  machine, or quarantine/resynchronization gate was found in Kraken CLI's inspected production
  WebSocket paths.
- **Confirmed non-finding:** Neither repository enforces Market Squawk's pre-trade risk contract.
  Kraken MCP's acknowledgment flag is user confirmation, and can be disabled; it is not position,
  notional, leverage, freshness, slippage, loss, drawdown, or duplicate-order evaluation.
- **Confirmed limitation:** This was a pinned source/docs/tests/CI review, not a legal opinion,
  live-network test, transitive-dependency audit, or independent numerical validation.

## Source List

1. `OpenGamma/Strata`, pinned at [`39c46e3`](https://github.com/OpenGamma/Strata/commit/39c46e342a4a95ac083d66287f038f6ae276692a), accessed 2026-07-15.
2. `krakenfx/kraken-cli`, pinned at [`aa32814`](https://github.com/krakenfx/kraken-cli/commit/aa32814cea70913a70c9909693a7abd762963e83), accessed 2026-07-15.
