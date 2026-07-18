# Market Squawk Usable-Release Baseline

**Inventory date:** 2026-07-18 (America/New_York)

**Product-code audit base:** `a829278aca4d4fc27d5a0c0aaa8e5a49f2cb5659`

**Product tree:** `6f5d9b7be896e9a5409f367c73aa4a5d95208a9c`

**Branch and upstream:** `feat/stage-1-foundation` and
`origin/feat/stage-1-foundation`, both at the product-code audit base when this inventory was
refreshed.

**Release disposition:** Blocked. This is an inventory, not release approval or performance
evidence.

## Scope and evidence boundary

The product-code audit base is the clean, pushed `feat/stage-1-foundation` commit shared with
`origin/feat/stage-1-foundation` when this inventory was refreshed. The historical Q2/A4
bounded-capture seed
and hardened release-evidence runner are integrated through that commit. Their source and evidence
boundary received focused independent review, and the clean release runner successfully prepared a
schema-5 exact-head build-evidence bundle. The idle-host gate has not admitted the five-repetition
measurement because normalized host load remained above its production threshold. Therefore no A4
performance result, historical Q2/A4 performance approval, active quarter approval, or
complete-release claim exists.
Stage 0 documentation and research commits after this audit base do not change product capability.

The previously named `docs/architecture/current-state.md` and `docs/plans/gap-analysis.md` remain
historical audits frozen at rejected commit `651a01e120dfe27a598b9475296733d238d870b7`.
They are not current approval evidence. This inventory, the README capability table, and the
canonical complete-release plan replace them as current delivery truth until the detailed audit is
refreshed after the approved live/capture prerequisite closure.

## Toolchain and workspace

- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`
- Host: `aarch64-apple-darwin`
- LLVM: `22.1.6`
- Edition: 2024
- Cargo resolver: 3
- Lockfile: tracked and consumed with `--locked`

Tracked workspace packages and local dependency edges from locked Cargo metadata:

| Package | Local dependencies |
| --- | --- |
| `market-squawk-domain` | none |
| `market-squawk-platform` | `market-squawk-domain` |
| `market-squawk-sources` | `market-squawk-domain`, `market-squawk-platform` |
| `market-squawk-live` | `market-squawk-domain`, `market-squawk-sources` |
| `market-squawk` | domain, live, platform, and sources crates |

No tracked production adapter path exists under `adapters/`. No tracked `python/` product package
exists. Python under `scripts/` is repository policy or protocol-smoke support, not the required
financial-data, analytics, or training product.

## Runnable product-code inventory

At the product-code audit base, the repository contains:

- invariant-preserving domain, identity, financial, time, quality, provenance, and source contracts;
- registry-owned source policy, budgets, durability, health, and opaque current-authority contracts;
- exact count-and-byte-bounded capture admission, asynchronous capture writing, and capture health;
- deterministic sharding, transactional price-level books, bounded snapshots, and live lifecycle;
- an authority-free Coinbase diagnostic compatibility reader and paper-only diagnostic calculation;
- bounded MSJ1 journal reading/writing and diagnostic reconstruction;
- a bounded five-tool stdio MCP compatibility server; and
- deterministic local mock, CLI, and protocol smoke paths.

These paths do not constitute production Coinbase or Kraken qualification, comprehensive risk,
realistic paper execution, a research plane, Python/modeling, portfolios, fair value, complete CLI,
or complete MCP.

## Interfaces consumed by the canonical plan

The plan must refresh these exact APIs against the approved live/capture prerequisite head before
dependent Stage 1 integration or Wave credit. Provisional disjoint Task 3/5 development may proceed
from this explicit audit anchor, but receives no integration or capability credit before that
refresh:

- domain financial, instrument, quality, provenance, and canonical event modules under
  `crates/market-squawk-domain/src/`;
- source contracts and registry authority under `crates/market-squawk-sources/src/`;
- capture admission, queue, writer, lifecycle, and benchmark support under
  `crates/market-squawk-platform/src/capture/`;
- sharding, actor ownership, books, snapshots, and runtime lifecycle under
  `crates/market-squawk-live/src/`; and
- diagnostic CLI/MCP/source composition under `apps/market-squawk/src/`.

No dirty-lane signature, provisional generated dependency, prepared-but-unmeasured evidence bundle,
or unreviewed benchmark result is a frozen prerequisite.

## Verification snapshot

The complete local gate passed on the intended A4 candidate before its final focused release-runner
and cross-language source-inventory corrections. Those corrections then passed their focused Rust,
Python, Clippy, clean release-build/preparer, and independent read-only review gates. This is strong
lane evidence, but it is not a clean full-gate result for `a829278` and does not approve a quarter.
Fresh exact-head verification, direct Cargo Deny/Cargo Audit/Gitleaks execution, measured performance
when the production host gate admits the machine, and grouped quarter review remain mandatory.

## 2026-07-17 verification-overhead audit result

The audit removed Python and Rust checks that enforced documentation wording, migration allowances,
configuration snapshots, exact dependency duplication, governance prose, wrapper command strings,
or one exact CLI-help sentence. It also removed the bespoke brand and duplicate-dependency checkers,
per-task reports, the stale transient progress report, and the rejected release-truth design. Git
history retains the removed artifacts. Test counts are deliberately not tracked as a delivery
metric.

The retained Python tests exercise repository-input hygiene, immutable CI/security configuration,
the bounded MCP process harness, and capture evidence/host-boundary behavior. The Rust suite
exercises financial, authority, qualification, lifecycle, concurrency, protocol, book, persistence,
and security behavior. Diagnostic MCP authority/coverage/quality limits are machine-readable `_meta`
contracts instead of English phrase assertions. Project memory requires every new test to be a thin,
critical behavioral or invariant proof.

The post-audit dirty candidate passed `./scripts/verify.sh`, Cargo Deny, Cargo Audit with warnings
denied, Gitleaks over the working tree, Gitleaks over 300 commits, formatting, and `git diff --check`.
The repository-input checker also gained a regression test after the audit exposed that it
incorrectly tried to inspect tracked paths already marked deleted. These results are dirty-candidate
evidence only; the same gates must run after the change is committed before approval.

### Retained automation audit

Every remaining script and Python test was inspected after the deletion pass:

| Automation | Disposition | Behavioral or security contract retained |
| --- | --- | --- |
| `check_workspace_boundaries.py` | Keep | Parses Cargo metadata/manifests and rejects invalid workspace membership, metadata inheritance, resolver, lint, and dependency-layer violations. |
| `check_generated_artifacts.py` and its eight tests | Keep | Enumerates Git-authoritative active inputs and rejects secret-shaped files, generated directories, unsafe links, unreviewed binaries, and oversized artifacts. |
| `test_ci_workflow_policy.py` | Keep, narrowed | Requires immutable 40-hex external-action references and credential-disabled checkout wherever checkout is used; it does not pin job count, runner labels, command text, or checkout-step count. |
| `smoke_mcp.py` | Keep | Exercises a real bounded stdio MCP process, framing deadlines, discovery, and the versioned typed authority/coverage/quality metadata contract. Its helper self-tests were removed; the product process smoke is the evidence. |
| `check_authority_lifecycle_loom.sh` | Keep | Executes the Loom concurrency model for authority admission, terminalization, and clean close. |
| `verify.sh` | Keep, simplified | Orchestrates the direct behavioral/build/security gates; no test snapshots its command order or help/report wording. |

The Rust test inventory contains no README, plan, report, task-label, or documentation-wording
contract. The remaining error-text assertions cover protocol diagnostics, safe redaction, or typed
failure routing in product code rather than repository prose. No additional prose/policy test or
documentation checker remains active.

## Review and release state

The original Task 0 documentation candidate was independently rejected at Critical 0 / Important 5 /
Minor 1; its remediation was committed at `84ffe97` after the product/build/security gate passed.
The subsequent A4 release-runner changes through `a829278` received focused read-only review at
Critical 0 / Important 0 / Minor 0 and clean preparer evidence, but the full exact-head gate,
five-repetition host measurement, and Quarter 1 grouped review remain pending. Focused approval does
not approve Tasks 0-6 or the usable release.

All mandatory release capabilities are listed in the README. Every current row is `Missing`, so the
usable complete release remains blocked.

## Verification-audit primary sources

The 2026-07-17 cleanup used the current primary references below:

- [MCP 2025-11-25 Tool schema](https://modelcontextprotocol.io/specification/2025-11-25/schema)
  defines extensible tool `_meta`; Market Squawk uses it for machine-readable diagnostic authority,
  coverage, and quality ceilings instead of testing English descriptions.
- [MCP `_meta` key rules](https://modelcontextprotocol.io/specification/2025-11-25/basic) require
  reverse-DNS-style prefixes; the compatibility server uses `org.market-squawk/` keys.
- [Cargo Deny bans configuration](https://embarkstudios.github.io/cargo-deny/checks/bans/cfg.html)
  distinguishes duplicate-version review signals from crate bans, while
  [`cargo deny check`](https://embarkstudios.github.io/cargo-deny/cli/check.html) executes the actual
  advisory, ban, license, and source policy.
- [taiki-e/install-action v2.83.4](https://github.com/taiki-e/install-action/releases/tag/v2.83.4)
  is pinned to commit `07b4745e0c39a41822af610387492e3e53aa222b` and installs exact Cargo Deny/Audit
  versions with checksum verification.
- [Gitleaks v8.30.1](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1) is installed in CI
  from the exact Linux archive after checking SHA-256
  `551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb`; the same
  open-source CLI is the local exact-head tree/history gate.
