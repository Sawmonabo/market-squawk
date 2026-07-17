# Quarter 2 source-authority remediation checkpoint

Date: 2026-07-16

This checkpoint implements the source-authority remediation designed in
[`docs/superpowers/specs/2026-07-16-q2-source-authority-remediation-design.md`](../superpowers/specs/2026-07-16-q2-source-authority-remediation-design.md)
and planned in
[`docs/superpowers/plans/2026-07-16-q2-source-authority-remediation.md`](../superpowers/plans/2026-07-16-q2-source-authority-remediation.md).

## Delivered contracts

- Deep retained-size accounting closes nested decoded book storage, frame evidence, source
  authority, process budget, and sealed-clock allocations. Shared allocations are charged once per
  routed batch; admission rejects an overweight batch before it enters a bounded live queue.
- Remote provider budgets are process-authoritative by authorization-derived provider/account
  scope. Independent registries and restored state share request, concurrency, cooldown, disabled,
  and availability-generation state. Every unavailable transition synchronously revokes retained
  availability leases.
- Health authority uses a registry-sealed wall/monotonic pair. The accepted chain is
  `session start <= observation <= report <= acceptance <= current validation`, with inclusive wall
  and monotonic deadlines. Reporter, recorder, mint, scoped authority, and queued authority all
  fail closed on unavailable clocks, rollback, expiry, or arithmetic overflow.
- Registration, replacement, health, attestation, and revocation transitions stage fallible work
  before publishing authority. Normal revocation records its successor epoch in exportable
  authority history before invalidating leases; exported and restored state preserves the
  tombstone. Terminal revocation remains available at `u64::MAX`, where no successor epoch exists,
  and restored tombstones cannot resurrect the source.
- Live processor, qualification, snapshot, and runtime-admission fixtures now derive absolute
  timelines from the registry-sealed session start. No test-only public clock or production bypass
  was introduced.

## Provider-access boundary

Market Squawk honors provider authorization, rate limits, cooldowns, refusals, and blocking. It
does not implement identity/account rotation to evade limits, browser/TLS fingerprint spoofing,
CAPTCHA or anti-bot bypass, blocking-evasion proxy rotation, distributed quota evasion, stealth
scraping, or access-control circumvention. A restricted source becomes unavailable or degraded and
recovers only through an authorized provider path under the same evidenced identity.

## Verification scope

Deterministic coverage includes nested retained-size bounds, concurrent process-budget conflicts,
cross-registry shared concurrency/cooldown, every availability-revocation branch, temporal-order
rejection without mutation, trusted-clock failure and discontinuity, deadline/epoch overflow,
normal and terminal revocation persistence, current-authority expiry, downstream live admission,
and public capability privacy. External network tests remain separate from the deterministic suite.

## Verification results

The final implementation state passed:

- `./scripts/verify.sh`, including the Python policy tests, workspace-boundary and generated-file
  checks, formatting, strict all-target/all-feature Clippy, all-target/all-feature workspace tests,
  rustdoc tests, release build, warning-denied documentation, application build, and offline CLI/MCP
  smoke tests.
- `cargo deny check`: advisories, dependency bans, licenses, and sources all passed.
- `cargo audit --deny warnings`: the current RustSec advisory database reported no findings for the
  locked 281-crate dependency graph.
- `gitleaks dir --no-banner --redact --config .gitleaks.toml .`: no credential leaks found.
- Focused anti-evasion and live-hot-path searches, final diff whitespace validation, and the exact
  base-to-head blast-radius review.

No external provider/network integration test was included in the deterministic gate.
