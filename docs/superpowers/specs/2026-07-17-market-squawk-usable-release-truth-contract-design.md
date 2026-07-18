# Market Squawk Usable-Release Truth Contract Design

**Status:** Approved for planning and implementation

**Approved direction:** 2026-07-17

**Audit base:** `e99f4ba13a6e622b899f169065348c484098c09d`

**Scope:** README truth, delivery terminology, release gating, and complete remaining-work planning

## Problem

The repository has a substantial Rust domain and live-authority foundation plus a runnable
diagnostic compatibility application. It is not yet the complete Market Squawk product. The
following mandatory product planes are absent or incomplete:

- production Kraken ingestion;
- SEC, FRED/ALFRED, BLS, Treasury, local-file, portfolio, and other required adapters;
- Arrow, Parquet, and DataFusion research datasets;
- point-in-time research and revision preservation;
- Python financial analytics and model training;
- model bundles, native inference, and conditional ONNX-compatible inference;
- research backtesting;
- portfolio accounting and analytics;
- realistic paper execution;
- fair-value analysis; and
- complete typed local MCP access.

Calling these capabilities work for a "later stage" is misleading. They are not optional future
enhancements. They are release blockers. A README can document what is runnable today, but it must
not imply that the current diagnostic foundation fulfills the complete product specification.

The active delivery vocabulary also conflates calendar quarters, four-task review batches, and
seven sequential implementation sections. `Q1` through `Q7` is not a coherent quarter model. More
importantly, the current halfway terminal condition would stop execution before Python/modeling and
complete MCP exist. That stop can therefore yield an unusable product and is superseded by this
design.

## Decisions

### 1. Use release-truth states, not vague deferral

The user-facing README uses exactly three capability states:

1. **Runnable now**
2. **Required but missing**
3. **Release blocked until implemented**

Every mandatory capability is named in one of those states. "Planned," "roadmap," "later stage,"
"subsequent stage," and similar wording cannot be used to weaken or obscure a mandatory release
requirement. A capability may be described as under active implementation only when its current
state remains explicit and no production claim is made.

The README quick start remains available, but it is titled and described as a diagnostic foundation
quick start. Commands must state what they demonstrate and what they cannot provide.

### 2. Separate historical identifiers from active delivery terminology

Historical checkpoint names and finding identifiers such as `Q1`, `Q2`, `Q2-I10`, and
`Q2-R01` remain unchanged. They are immutable audit locators and renaming them would damage
traceability.

New and active delivery documents use:

- **Stage** for a dependency-ordered body of implementation work;
- **Wave** for safely parallel implementation lanes inside a stage;
- **Release gate** for an exact-head verification and review boundary; and
- **Calendar Q1-Q4** only when referring to actual dates.

There are no active delivery "quarters" numbered beyond four.

### 3. Replace the halfway stop with a usable-release terminal condition

The halfway terminal condition in `docs/project-memory.md` is superseded. Work does not stop merely
because a weighted ledger crosses 50 percent or because a numbered stage ends.

The new terminal condition is a **usable complete local release**. It requires all mandatory product
capabilities, their producer-to-consumer integrations, exact-head verification, independent grouped
review, publication, and a runnable local demonstration. Progress weighting remains useful for
status reporting, but it cannot authorize a stop or substitute for an end-to-end capability.

The release cannot pass while any mandatory capability is represented only by:

- a trait or interface;
- an empty crate or directory;
- a schema without a working producer and consumer;
- a synthetic source represented as production;
- a mock, compatibility path, or diagnostic-only implementation;
- a research report or implementation plan;
- an unchecked roadmap item; or
- a focused lane test without integrated exact-head evidence.

### 4. Preserve the optional boundary narrowly

Only genuinely optional capabilities may remain post-release:

- paid or licensed provider adapters;
- authorized live-money execution adapters;
- replay beyond required diagnostic and decoder-validation needs; and
- a future observability adapter beyond required local structured tracing.

The no-evasion boundary remains permanent. Provider limitations are handled through authorized
identity, shared budgets, backoff, persistence, cache, health, failover, and explicit coverage—not
through identity rotation, fingerprint spoofing, CAPTCHA bypass, blocking-evasion proxies, or
distributed quota evasion.

## README information architecture

The README is reorganized around what a user can actually do.

### Status and release-blocking banner

The opening status states that the current checkout is a diagnostic foundation and is not the
complete usable release. It links to the current-state audit and complete-release plan.

### Runnable now

This section lists only verified current behaviors:

- Rust domain, source-authority, live-processing, capture, and snapshot foundations;
- deterministic local mock execution for diagnostics;
- public Coinbase diagnostic capture with explicit single-venue partial coverage;
- MSJ1 journal validation and diagnostic replay;
- diagnostic top-of-book calculations and paper simulation; and
- the five-tool diagnostic stdio MCP compatibility server.

Each entry preserves its authority and coverage limitations.

### Required but missing

This section names every mandatory missing capability from this design. It does not hide the list
behind a roadmap or stage number. Each capability links to the requirement/implementation plan once
that plan is frozen.

### Release blocked until implemented

This section defines the terminal gate in user terms. It states that Market Squawk is not a usable
complete release until all mandatory capabilities are runnable together, locally, through the CLI
and complete MCP where applicable.

### Diagnostic quick start

The existing commands remain, but the heading and surrounding text prevent them from being read as
a complete product demonstration. No quick-start command may imply production execution quality,
complete research coverage, or working modeling/portfolio/fair-value planes.

### Remove the roadmap as a substitute for delivery

The current roadmap is replaced with a **release-blocking implementation map**. The map describes
dependency order and current state. It is not a promise that required work will happen at an
unspecified time.

## Complete remaining-work plan contract

The superseding implementation plan must cover every mandatory capability as a real vertical slice.
Each slice names:

- exact producer and consumer;
- crate and adapter ownership;
- schema, provenance, availability, and quality contracts;
- persistence and restart behavior;
- resource, input, output, and time bounds;
- lawful provider access and rights disposition;
- deterministic fixtures and external-network-test separation;
- TDD sequence and adversarial/property/fuzz tests;
- focused lane gate;
- integration order;
- exact-head release evidence; and
- grouped review scope.

The plan uses dependency-safe waves so disjoint work proceeds concurrently without assigning two
writers to an authority-critical file, workspace manifest, lockfile, application composition root,
or checkpoint evidence.

At minimum, the plan contains these dependency groups:

1. current live/capture checkpoint closure and clean baseline;
2. shared data, adapter, analytics, modeling, portfolio, execution, valuation, and application-
   service boundaries with nonempty consumers;
3. SQLite control catalog, Arrow schemas, immutable Parquet publication, DataFusion confinement,
   and point-in-time selection;
4. Kraken plus file, SEC, macro, and portfolio adapters;
5. analytics, portfolio accounting, Python research/training, model bundles/inference, and
   research backtesting;
6. comprehensive risk and realistic paper execution;
7. fair-value analysis and complete MCP/CLI services;
8. integrated demonstrations, performance, fuzzing, security, supply-chain evidence, review,
   publication, and cleanup.

The implementation plan may split these groups into more waves when ownership or dependencies
require it. It may not remove a capability from the release gate merely to increase parallelism.

## Machine-enforced documentation invariants

Repository policy tests protect the user-facing truth contract.

The tests must fail when the active README:

- omits any of the three required state headings;
- describes a mandatory capability with vague "later stage" or "subsequent stage" language;
- contains a numbered delivery quarter beyond calendar Q4;
- describes the diagnostic MCP as the complete MCP;
- describes verification Python helpers as the Python product package;
- omits the empty/missing adapter status;
- presents the roadmap as a substitute for mandatory release work; or
- claims a usable release while a release-blocking capability is missing.

The tests do not rewrite or reject immutable historical finding IDs and research quotations. Their
scope is current user-facing and active delivery truth.

## Verification and review

The documentation correction passes:

- focused documentation-policy tests with an observed RED before implementation;
- the complete Python policy suite;
- brand, workspace-boundary, duplicate-dependency, and generated-artifact checks;
- Markdown/link/path inspection;
- `git diff --check`; and
- an independent read-only documentation and plan review.

The complete release later passes the full locked Rust verification suite, deterministic network-
free tests, separately gated external source smokes, parser/model/MCP fuzz targets, measured
performance on documented hardware, dependency/vulnerability/license/credential audits, runnable
CLI/MCP demonstrations, and grouped exact-head review with no unresolved Critical, Important, or
Minor findings.

## Ongoing-work coordination

The current live/capture closure continues in its existing grouped Q2 worktree while this release-
truth contract and remaining-work plan are prepared in a separate documentation worktree. A
read-only traceability lane audits the full mandatory requirement set. These lanes may proceed in
parallel because they do not share writable files.

Integration remains ordered:

1. approve and promote the exact live/capture closure;
2. refresh the complete-release plan against that approved head;
3. integrate the release-truth documentation without losing accepted Q2 truth;
4. launch dependency-safe implementation waves; and
5. stop only at the usable complete local release gate.

## Acceptance criteria

This design is implemented when:

1. the README contains the three required states and no vague mandatory-work deferral;
2. the project memory records the usable-release terminal condition and supersedes the halfway stop;
3. active delivery language uses stages, waves, and release gates;
4. a complete, requirement-traceable remaining-work plan covers every mandatory capability;
5. documentation-policy tests enforce the truth contract;
6. current Q2 implementation continues independently without document-lane interference; and
7. the exact documentation/plan commit passes focused verification and independent review before
   integration and publication.
