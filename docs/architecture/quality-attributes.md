# Quality Attributes and Acceptance Scenarios

Market Squawk treats production quality as measurable behavior at an exact reviewed commit. This
page translates the architecture's latency, bounded-resource, integrity, durability, security,
privacy, recovery, portability, and audit goals into acceptance scenarios without promoting an
unmeasured target into a performance claim.

| Metadata | Value |
| --- | --- |
| Document type | Quality-attribute architecture |
| Audience | Maintainers, release reviewers, operators, security reviewers, and performance engineers |
| Status | Current |
| Last substantive review | 2026-07-23 |
| Reviewed commit | `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` |

## Contents

- [Scope and evidence levels](#scope-and-evidence-levels)
- [Quality priorities](#quality-priorities)
- [Acceptance scenarios](#acceptance-scenarios)
- [Performance measurement contract](#performance-measurement-contract)
- [Failure and recovery requirements](#failure-and-recovery-requirements)
- [Security, privacy, and audit requirements](#security-privacy-and-audit-requirements)
- [Portability and operability](#portability-and-operability)
- [Evidence map](#evidence-map)
- [Related documentation and code](#related-documentation-and-code)
- [External sources](#external-sources)

## Scope and evidence levels

This page defines architectural response measures. It does not claim final release approval. The
[delivery ledger](../plans/delivery-ledger.md) owns current verification and blocker state.

Evidence has three distinct levels:

| Level | Meaning |
| --- | --- |
| Implemented contract | The reviewed source encodes the invariant and has focused behavioral evidence. |
| Accepted checkpoint evidence | A prior clean exact head passed its applicable grouped review and verification. |
| Final release evidence | One clean, unchanged release head passes the complete demonstration, performance, fuzz, security, build, and review gate. |

The reviewed source head includes the accepted Quarter 3 product code plus subsequent
provider-activation and MCP corrections. The Quarter 3 full gate passed at
`c6f0124c2b27c4777947de8c42b6a5f97868aaf5`; focused gates cover the later changes through the
reviewed commit. A final unchanged-head release gate has not yet been recorded. Consequently, this
page labels performance targets and final acceptance evidence as pending.

## Quality priorities

In priority order:

1. **Financial integrity and safety:** invalid, stale, incomplete, or conflicting evidence must not
   create an executable action or silently alter accounting state.
2. **Bounded resource use:** every ingress, queue, parser, collection, query, model, result, and
   background owner must have finite count, byte, work, and time bounds.
3. **Deterministic authority:** mutable live state has one writer; current authority is opaque,
   generation-bound, revocable, and non-serializable; persistence uses exact identities.
4. **Durability and recovery:** committed local state survives restart, interrupted work is
   reconciled, and uncertainty remains unavailable rather than guessed.
5. **Low-latency live decisions:** the in-memory event-to-decision path excludes analytical I/O and
   must meet measured release targets without weakening the first four priorities.
6. **Local privacy and operability:** no mandatory cloud, telemetry, external database, container,
   or paid service is needed; explicit provider connections and local evidence remain observable.

## Acceptance scenarios

Each scenario names the stimulus, architectural response, measurable acceptance condition, and
current evidence disposition.

| ID | Stimulus and environment | Required response | Response measure | Evidence at reviewed commit |
| --- | --- | --- | --- | --- |
| QA-LIVE-01 | A current live update has a gap, invalid checksum, stale time, bad precision, crossed book, or invalid trading status. | Reject/roll back the transition as applicable, quarantine or degrade the affected stream, revoke executable authority, and require resynchronization. | Zero strategy-to-adapter action from the invalid observation; last-good state remains internally consistent. | Implemented in live/source authority; covered by accepted live checks and focused pipeline tests. |
| QA-LIVE-02 | A live, capture, cross-venue, export, audit, or dispatch queue reaches its count or byte limit during a burst. | Refuse admission without an unbounded allocation or silent critical-message loss; invalidate or suppress the affected action according to the owning boundary. | Resident state remains within the startup/configured model; caller receives a typed saturation disposition; no unauthorized adapter call. | Implemented; count/byte admission and lifecycle behavior are exercised in live, capture, execution, and MCP suites. |
| QA-LIVE-03 | An instrument route receives concurrent source/control work. | Stable routing sends mutable book, feature, strategy, and local action state to exactly one shard writer. | Same `(venue, instrument, routing version, shard count)` always selects the same shard; no second mutable owner exists. | Implemented with golden routing and actor ownership evidence. |
| QA-RISK-01 | A strategy/model emits an intent, or a caller attempts to submit without current evidence. | Central risk consumes current live authority and current portfolio state; only a private `ApprovedOrder` may enter one-use dispatch. | Every adapter submission is traceable to one nonexpired approval and reservation; rejected/failed inference yields zero submissions. | Implemented and accepted at Quarter 3; privacy compile-fail and real dispatch pipeline evidence exist. |
| QA-DATA-01 | Process interruption occurs between analytical staging, catalog mutation, and object publication. | Authority-before-publication keeps the previous generation current or recovers the exact new generation; directory contents do not establish completeness. | After restart, every visible generation has a valid manifest, object identities, lineage, and catalog authority; residue is bounded and non-current. | Implemented and accepted in the research vertical; publication-recovery tests remain part of the suite. |
| QA-PIT-01 | A later revision, universe change, instrument definition, or corporate action is added after an earlier research cutoff. | Point-in-time selection uses evidenced availability/revision/supersession and historical instrument terms at each cutoff. | Rebuilding the earlier cutoff yields the same admitted inputs and cannot observe the later evidence. | Implemented and accepted in data/backtest remediation; final release demonstration still pending. |
| QA-MODEL-01 | A bundle, runtime, graph, tensor, worker, warm-up, deadline, or output violates its admitted contract. | Reject or quarantine the model generation and produce typed no-action evidence. | Zero order intents from failed inference; `infer` uses only its admitted in-memory or prewarmed worker capability. | Implemented and accepted for native and tract ONNX paths; optional external runtime remains policy-gated. |
| QA-PORT-01 | Portfolio import, corporate-action evidence, valuation set, or aggregate work is incomplete or exceeds a limit. | Reject publication/result or mark the financial measure explicitly incomplete; never synthesize an exact zero or overflow allocation. | Published revision identity binds reporting currency, schema, source, policy, and evidence; work/results stay under admitted limits. | Implemented and accepted in Quarter 3 portfolio remediation. |
| QA-FV-01 | Delayed, adjusted, proxy, modeled, similar-instrument, stale, or inaccessible-market evidence is evaluated for Level 1. | Fail the Level 1 conjunction and retain Level 2, Level 3, or `Unclassified` as supported by evidence; never alter execution quality. | No Level 1 result without every code-owned predicate; no override can promote Level 1 or cure `Unclassified`. | Implemented and accepted in the fair-value closeout. |
| QA-CTRL-01 | CLI or MCP submits unknown fields, excessive structure/results, an expired deadline, cancellation, or an unauthorized mutation. | Apply the same code-owned descriptor and application service; reject before domain mutation or publish a controlled artifact reference. | Exact schema/bounds are identical across transports; cancellation/deadline stops owned work; mutation has durable audit. | Implemented; complete 11-domain application composition and focused MCP/control-plane evidence exist. |
| QA-REC-01 | Shutdown deadline, child/helper failure, or ambiguous persistence outcome occurs. | Stop admission, invalidate authority, preserve a terminal owner, and return incomplete/unavailable until reconciliation proves state. | No detached authority-bearing work; restart either resumes exact durable state or reports a typed recovery requirement. | Implemented across live, capture, execution, provider activation, model, portfolio, backtest, paper, and valuation lifecycles. |
| QA-PRIV-01 | The product runs locally with normal logging and configured provider access. | Keep structured logs local, redact secret values/references, use stdio MCP, and publish artifacts under controlled roots. | Network inventory matches declared provider/onboarding traffic; logs/results contain no secret material; paths and SQL stay within their typed capabilities. | Implemented contracts and security gates exist; final exact-head credential/network audit remains pending. |
| QA-BUILD-01 | Repeated development and verification builds produce generated Cargo state. | Use one worktree-local target, nonincremental approval gates, bounded debug profiles, and a hard disk ceiling. | Verification refuses a target over 20 GiB; source and release artifact size are reported separately from generated cache. | Enforced; accepted hardening gate peaked below 20 GiB and cleaned generated output afterward. |
| QA-PERF-01 | A documented modern consumer host processes a warmed synthetic live pipeline under the fixed release fixture. | Sustain the required workload while preserving all validation, strategy/model, risk, and memory invariants. | At least 100,000 events/s and sub-millisecond warmed p99 internal event-to-decision latency, with p50/p95/p99/max and peak memory recorded. | **Target only; not yet accepted or claimed.** Task 20 owns final measurement. |
| QA-PERF-02 | The same host runs analytical ingest/query fixtures. | Publish/read Arrow and Parquet and execute bounded DataFusion queries without bypassing manifest or memory authority. | Record rows/s or bytes/s, query latency, fixture identities, result cardinality, and peak memory on the exact release binary. | **Measurement pending.** Functional storage/query behavior is implemented. |

## Performance measurement contract

Performance is accepted only when the measurement describes:

- exact Git commit, clean worktree, Rust toolchain, executable hash, feature set, and locked
  dependency state;
- hardware model, CPU, memory, operating system, power/thermal state, and relevant host load;
- fixture version/hash, instrument/routes, event count, queue capacities, warm-up, repetition count,
  and whether capture and paper execution are enabled;
- decoder, sequence/checksum, queue, order-book, feature, strategy/model, risk, and complete
  event-to-decision measurements;
- throughput, p50, p95, p99, maximum latency, peak resident memory, drops/refusals, and
  resynchronizations; and
- Arrow/Parquet publication and DataFusion query fixtures, cardinalities, bytes, latency, and peak
  memory.

The measurement must exercise the production path, not a synthetic source represented as a
production provider. It must retain every evidence check, the production queue policy, central
risk, the disclosed timed interval, and the production memory bounds.

The repository contains bounded benchmark support for capture admission, but no historical or
focused benchmark alone approves the integrated release target. The final result must be generated
from the unchanged release candidate and linked from the delivery evidence.

## Failure and recovery requirements

Recovery quality is defined by what the system refuses to guess:

- source reconnect creates a new generation; a heartbeat cannot restore market-price freshness;
- stream resynchronization requires the provider-specific snapshot/checksum/sequence evidence;
- catalog and Parquet recovery require exact catalog, manifest, object, and root authority;
- portfolio, model, backtest, paper, and fair-value recovery require matching immutable identities
  and terminal records;
- provider activation recovery restores desired recipes without reading credentials or prompting
  during restart; credentialed sources require explicit foreground resume;
- task and helper timeout preserves cancellation/terminal ownership, and uncertain termination
  blocks reuse; and
- backup/restore succeeds only after verifying the catalog snapshot and controlled artifact
  inventory as one consistent set.

Recovery tests should inject failures at authority transitions, not merely assert that a file
exists or a status string changed.

## Security, privacy, and audit requirements

### Security

- Untrusted JSON, CSV, XML, Excel, OFX, Parquet, WebSocket, HTTP, MCP, model, and local-state inputs
  cross size, depth, count, type, schema, cancellation, and time bounds before authority changes.
- Endpoint, redirect, TLS, response-size, budget, and retry policy is source-owned.
- Secret values are zeroized/redacted and stored through an opaque backend reference.
- Local paths are capability-confined; publications use stable identity and no-clobber/no-follow
  rules.
- MCP exposes typed product operations through application descriptors. Path access, provider
  networking, credential resolution, analytical SQL, risk approval, and order dispatch remain
  with their dedicated capabilities; read-only DataFusion SQL is CLI-only.
- Risk and dispatch capabilities are private and non-serializable.

### Privacy

No cloud, telemetry, or analytics service is required. Local logs can be human-readable or JSON
and must redact secrets. Controlled artifacts return opaque relative references. Provider requests
carry the configured provider-facing identity and requested data.

### Auditability

Every authority-bearing workflow retains the identities needed to explain what happened:

- source/metadata/session/generation/coverage/quality and payload evidence;
- dataset schema, manifest, parents, sources, revisions, and object hashes;
- model bundle, artifact, feature, dataset, code, environment, and runtime identities;
- strategy/model, intent, risk decision, approval, dispatch, order, fill, and reconciliation;
- portfolio revision, reporting currency, source evidence, policy, and analytical cutoff;
- fair-value inputs, method, ruleset, classification reason, overrides, approvals, and revocations;
  and
- MCP request, effect class, terminal status, result/artifact metadata, and lifecycle.

Audit records explain authority but do not recreate a current capability.

## Portability and operability

The product uses stable Rust 1.97.1, Edition 2024, resolver 3, a committed lockfile, and local
dependencies. Platform code contains explicit Unix and Windows path/process/security branches,
while the sealed Python v0.1 target is CPython 3.12/3.13 on macOS arm64.

The accepted full-gate host recorded in project memory is Apple M1 Pro, macOS 26.5.1, 16 GiB RAM.
This is evidence for that host, not proof of every conditional platform implementation. A release
claim for another operating system requires the same locked functional/security gate and any
platform-specific helper, keyring, path, and process evidence.

Operational diagnostics expose source health, redacted effective configuration, application
readiness, bounded failures, durable audit, and controlled artifact references. `AppConfig`
retains per-setting provenance internally; the reviewed `config show` renderer does not yet expose
those origins. Diagnostics remain local and do not require a remote observability stack.

## Evidence map

| Quality area | Current source/evidence anchors |
| --- | --- |
| Live integrity and bounded runtime | [Live crate](../../crates/market-squawk-live/src/lib.rs), [live action runtime harness](../../crates/market-squawk-live/tests/harnesses/action_runtime.rs), [application live pipeline harness](../../apps/market-squawk/tests/harnesses/live_pipeline.rs) |
| Source/capture lifecycle | [Sources crate](../../crates/market-squawk-sources/src/lib.rs), [platform capture lifecycle harness](../../crates/market-squawk-platform/tests/harnesses/capture_journal.rs), [production source supervisor](../../apps/market-squawk/src/live_source/supervisor.rs) |
| Risk and one-use execution | [Execution action hook](../../crates/market-squawk-execution/src/live_hook.rs), [risk/dispatch harness](../../apps/market-squawk/tests/harnesses/risk_execution.rs) |
| Research publication and PIT | [Data crate](../../crates/market-squawk-data/src/lib.rs), [publication recovery](../../crates/market-squawk-data/tests/publication_recovery.rs), [point-in-time tests](../../crates/market-squawk-data/tests/pit.rs) |
| Model containment | [Modeling crate](../../crates/market-squawk-modeling/src/lib.rs), [modeling contracts](../../crates/market-squawk-modeling/tests/modeling_contracts.rs) |
| Portfolio | [Portfolio crate](../../crates/market-squawk-portfolio/src/lib.rs), [portfolio service tests](../../crates/market-squawk-portfolio/tests/service.rs) |
| Fair value | [Valuation crate](../../crates/market-squawk-valuation/src/lib.rs), [fair-value integration](../../crates/market-squawk-valuation/tests/fair_value.rs) |
| CLI/MCP services | [Application contracts](../../apps/market-squawk/src/application/contracts.rs), [production MCP composition](../../apps/market-squawk/tests/production_mcp_composition.rs), [control-plane harness](../../apps/market-squawk/tests/harnesses/control_plane.rs) |
| Provider activation | [Evidence validation](../research/2026-07-23-provider-activation-evidence-validation.md), [local provider activation](../../apps/market-squawk/src/local_product/cli_provider.rs) |
| Exact-head and disk evidence | [Project memory](../project-memory.md), [delivery ledger](../plans/delivery-ledger.md) |

## Related documentation and code

- [Architecture overview](overview.md)
- [Building blocks and hot-path exclusions](building-blocks.md)
- [Deployment](deployment.md)
- [Live execution plane](live-execution-plane.md)
- [Research data plane](research-data-plane.md)
- [Security and trust boundaries](security-and-trust-boundaries.md)
- [Data, time, and provenance](data-time-and-provenance.md)
- [Authority lifecycle model-checking strategy](../testing/authority-lifecycle-model-checking.md)

## External sources

| Source | Relevance | Reviewed |
| --- | --- | --- |
| [arc42 quality requirements](https://docs.arc42.org/section-10/) | Defines concrete quality scenarios with source, stimulus, response, and measurable acceptance criteria. | 2026-07-23 |
| [arc42 introduction and goals](https://docs.arc42.org/section-1/) | Recommends a small prioritized set of concrete architectural quality goals. | 2026-07-23 |
| [Tokio bounded `mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Documents finite-capacity message passing and backpressure used by bounded runtime handoffs. | 2026-07-23 |
| [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html) | Defines the in-memory analytical representation whose throughput and memory must be measured. | 2026-07-23 |
| [Apache DataFusion SQL API](https://datafusion.apache.org/library-user-guide/using-the-sql-api.html) | Documents SQL planning/execution and options that support read-only bounded analytical use. | 2026-07-23 |
