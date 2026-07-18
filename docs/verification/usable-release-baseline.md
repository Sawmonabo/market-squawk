# Market Squawk Usable-Release Baseline

**Inventory date:** 2026-07-17 (America/New_York)

**Product-code audit base:** `e99f4ba13a6e622b899f169065348c484098c09d`

**Documentation-plan head before Task 0 remediation:**
`6f495d88a060fefbf3f9dbff99a386880cccebad`

**Release disposition:** Blocked. This is an inventory, not release approval or performance
evidence.

## Scope and evidence boundary

The product-code audit base is the clean `feat/stage-1-foundation` commit shared with
`origin/feat/stage-1-foundation` when this inventory was prepared. The separate Q2 A4 benchmark
lane contains uncommitted work and is deliberately excluded until it is clean, independently
reviewed, and promoted. Documentation changes after the product-code audit base do not turn that
work into implemented product capability.

The previously named `docs/architecture/current-state.md` and `docs/plans/gap-analysis.md` remain
historical audits frozen at rejected commit `651a01e120dfe27a598b9475296733d238d870b7`.
They are not current approval evidence. This inventory, the README capability table, and the
canonical complete-release plan replace them as current delivery truth until the detailed audit is
refreshed after Q2 closure.

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

The plan must refresh these exact APIs after Q2 approval before dependent implementation begins:

- domain financial, instrument, quality, provenance, and canonical event modules under
  `crates/market-squawk-domain/src/`;
- source contracts and registry authority under `crates/market-squawk-sources/src/`;
- capture admission, queue, writer, lifecycle, and benchmark support under
  `crates/market-squawk-platform/src/capture/`;
- sharding, actor ownership, books, snapshots, and runtime lifecycle under
  `crates/market-squawk-live/src/`; and
- diagnostic CLI/MCP/source composition under `apps/market-squawk/src/`.

No dirty-lane signature, provisional generated dependency, or unreviewed benchmark result is a
frozen prerequisite.

## Verification snapshot

Before the Task 0 verification audit, the unchanged documentation candidate ran:

- 69 Python tests, including 56 documentation, wording, configuration-snapshot, duplicate policy,
  and verification-wrapper tests that did not exercise product behavior and have now been removed;
- 651 Rust tests discovered by the locked all-feature workspace test inventory;
- formatting, strict Clippy, workspace tests, release builds, rustdoc, and the reserved Loom model;
- workspace-boundary, generated-artifact, offline mock, and MCP smoke gates.

That full run exited successfully but preceded this Task 0 remediation and therefore does not approve
the resulting candidate. The retained Python suite has 11 tests over repository-input hygiene,
immutable CI configuration, and the MCP smoke harness. Fresh exact-head
verification, direct Cargo Deny/Cargo Audit/Gitleaks execution, and independent review remain
mandatory.

## 2026-07-17 verification-overhead audit result

The audit removed 59 of the 69 Python tests because they enforced documentation wording, migration
allowances, configuration snapshots, exact dependency duplication, governance prose, or wrapper
command strings. It also removed one Rust CLI-help phrase test, the exact help-sentence check in
`verify.sh`, the bespoke brand and duplicate-dependency checkers, 25 per-task SDD reports, the stale
transient progress report, and the rejected release-truth design. Git history retains every deleted
artifact.

The active suite now contains 11 Python tests and 650 Rust tests. The retained Python tests exercise
repository-input hygiene, immutable CI/security configuration, and the bounded MCP process harness.
The Rust suite continues to exercise financial, authority, qualification, lifecycle, concurrency,
protocol, book, persistence, and security behavior. Diagnostic MCP authority/coverage/quality limits
are now machine-readable `_meta` contracts instead of English phrase assertions.

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
| `smoke_mcp.py` and its two harness tests | Keep | Exercises a real bounded stdio MCP process, framing deadlines, discovery, and the versioned typed authority/coverage/quality metadata contract. |
| `check_authority_lifecycle_loom.sh` | Keep | Executes the Loom concurrency model for authority admission, terminalization, and clean close. |
| `verify.sh` | Keep, simplified | Orchestrates the direct behavioral/build/security gates; no test snapshots its command order or help/report wording. |

The Rust test inventory contains no README, plan, report, task-label, or documentation-wording
contract. The remaining error-text assertions cover protocol diagnostics, safe redaction, or typed
failure routing in product code rather than repository prose. No additional prose/policy test or
documentation checker remains active.

## Review and release state

The first documentation candidate at dirty-diff SHA-256
`5d907af825966230fdf4599a025ea6d248be5af4f6a4ee97449158d9382d95af` was independently rejected at
Critical 0 / Important 5 / Minor 1. Task 0 remediation addresses the authority overclaim, stale
truth links, competing plan authority, incomplete policy invariants, ambient-directory coupling, and
journal-compaction ambiguity. No finding is considered closed until the complete new candidate is
unchanged and independently re-reviewed at Critical 0 / Important 0 / Minor 0.

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
