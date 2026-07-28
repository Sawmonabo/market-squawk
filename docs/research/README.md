# Research evidence index

Market Squawk records date-anchored research and primary-source links in this directory so design
decisions can be audited without relying on conversation history.

## Baseline research

- [Complete local-platform research report](2026-07-15-market-squawk/final-report.md) — product,
  architecture, providers, data semantics, modeling, execution, valuation, MCP, security, testing,
  and performance synthesis frozen on 2026-07-15.
- [Research manifest](2026-07-15-market-squawk/manifest.md) — collection method, source inventory,
  verification artifacts, and limitations.
- [Machine-readable source inventory](2026-07-15-market-squawk/source-inventory.json) — deduplicated
  source identities and evidence metadata.

## Stage 1 decisions

- [Quarter 1 contract decisions](2026-07-16-q1-contract-decisions.md) — domain and protocol
  authority decisions.
- [CI action pins](2026-07-16-ci-action-pins.md) — immutable CI action references.
- [Capability filesystem](2026-07-16-capability-filesystem.md) — capability-relative artifact and
  local-path confinement.
- [HTTP source policy](2026-07-16-http-source-policy.md) — endpoint authorization, redirects,
  proxies, response limits, shared provider budgets, and monotonic cooldowns.
- [Journal durability](2026-07-16-journal-durability.md) — exact-handle validation, file/directory
  synchronization, capability-bound journal I/O, and blocking-writer isolation.

## Stage 2 decisions

- [Authority lifecycle model checking](2026-07-17-authority-lifecycle-model-checking.md) — packed
  lifecycle transition properties, property testing, bounded Loom modeling, commands, limitations,
  and primary sources.
- [Cross-platform filesystem durability](2026-07-17-cross-platform-filesystem-durability.md) —
  Linux `O_PATH` directory synchronization, Windows stable metadata limits, capability-relative
  directory handles, and platform verification.
- [Source-authority memory reservation](2026-07-17-source-authority-memory-reservation.md) —
  composition-global graph and mutation ceilings, passive-lease decoupling, stable store bounds,
  live exact-once accounting, failure semantics, and blocking RED/GREEN evidence.

## Usable-release decisions

- [Dependency and provider decisions](2026-07-18-usable-release-dependencies.md) — exact analytical,
  MCP, Python, inference, parser, secret, and HTTP dependency admissions plus current provider
  rights, limits, coverage, fallback, and live-qualification policy.
- [Capability traceability](2026-07-18-usable-release-traceability.md) — honest current state and the
  mandatory producer-to-terminal-consumer, dataset, evidence, closing-task, and exact-head refresh
  map for the complete local release.
- [Rust development/test storage hardening](2026-07-21-rust-dev-test-storage-hardening.md) — Cargo
  profile, worktree-local output, test-target, CI cache, and measurement decisions for the storage
  hardening plan.
- [Zero-fee provider onboarding and local secret activation](2026-07-22-zero-fee-provider-onboarding/final-report.md)
  — capability-gated provider setup, human authorization boundaries, rights admission, credential
  lifecycle, 24 implementation acceptance criteria, and the complete source/evidence package.
- [Provider activation evidence validation](2026-07-23-provider-activation-evidence-validation.md)
  — independently audited provider decisions, exact artifact bindings, reconciled probe lineage,
  source links, and the activation/restart authority consequences implemented on 2026-07-23.

## Build and verification decisions

- [CI verification runtime and build-cache diagnosis](2026-07-27-ci-verification-runtime.md) —
  measured phase timing, the reproduced Cargo invalidation defect, inactive pull-request cache
  population, repeated Loom construction, the accepted cross-platform correctness follow-up,
  fail-closed parallel-job design, acceptance evidence, and primary sources for reducing feedback
  time without weakening release coverage.
- [Cross-platform installation and guided setup](2026-07-28-cross-platform-installation-and-guided-setup.md)
  — current cargo-dist, Qt Installer Framework, Tauri, uv, native packaging, secure activation,
  rollback, offline, headless, complete-default, and change-aware CI evidence; records the
  recommended hybrid direction while the signing and platform-matrix decisions remain pending.

## Provider decisions

- [Treasury daily rates release authority](providers/2026-07-26-treasury-daily-rates-release-authority.md)
  — official five-family feed coverage, CC0 durable-use authority, current implementation gap,
  and mandatory V1 release evidence.
- [FRED and ALFRED self-hosted API authority](providers/2026-07-26-fred-alfred-self-hosted-api-authority.md)
  — current two-gate durable-use decision, official source basis, and mandatory working real-data
  release proof.
- [Historical FRED and ALFRED blocked decision](providers/fred-alfred-rights-2026-07-22.md) —
  superseded audit evidence for the earlier blanket-blocked implementation state.
- [Coinbase Direct Market Data execution-quality candidate](providers/coinbase-direct-market-data-2026-07-22.md)
  — authenticated direct endpoint, full-channel snapshot/sequence contract, free-tier bound, and
  the exact evidence still required before `DirectVerified` qualification.
- [Coinbase Exchange public WebSocket v1 decision](providers/coinbase-exchange-websocket-2026-07-16.md)
  — public level2/matches/heartbeat coverage and its immutable `DirectUnverified` ceiling.
- [SEC EDGAR public API authority](providers/sec-edgar-public-api-2026-07-26.md) — zero-fee
  no-key submissions/company-facts scope, declared-contact and fair-access controls, and mandatory
  real-response release acceptance.
- [BLS Public Data API decision](providers/bls-public-data-api-2026-07-21.md) — available zero-fee
  public v1 scope, distinct registered-v2 lifecycle, exact limits, and source authority.
- [Local file adapter boundaries](providers/local-file-adapter-boundaries-2026-07-20.md) —
  controlled-root file admission and format-specific source boundaries.

## Evidence policy

- Prefer official specifications, official project documentation, primary research papers, and
  pinned repository evidence.
- Record the research date and version when behavior can change.
- Separate a source-backed fact from a Market Squawk design inference.
- Persist direct links beside the decision they support.
