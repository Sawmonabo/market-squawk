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

## Evidence policy

- Prefer official specifications, official project documentation, primary research papers, and
  pinned repository evidence.
- Record the research date and version when behavior can change.
- Separate a source-backed fact from a Market Squawk design inference.
- Persist direct links beside the decision they support.
- Never use research to justify access-control, CAPTCHA, identity, proxy, fingerprint, or provider
  quota evasion. Provider restrictions are handled through authorized adapters, shared budgets,
  local caching, failover, coverage metadata, and explicit unavailability.
