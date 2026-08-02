# Research Manifest

## Table of Contents

- [Topic](#topic)
- [As-of Date](#as-of-date)
- [Decision Context](#decision-context)
- [Required Source Categories](#required-source-categories)
- [Workflow](#workflow)
- [Expected Outputs](#expected-outputs)

## Topic

Market Squawk complete local platform architecture, source adapters, analytics, risk, valuation, and MCP implementation evidence

## As-of Date

2026-07-15

## Decision Context

Determine how to evolve the existing single-crate Market Squawk v0.1 into the requested
seven-stage, zero-mandatory-cost local platform. Validate current upstream capabilities,
compatibility, maintenance, licensing and usage constraints, point-in-time and low-latency
design evidence, fair-value guidance, and safe source-access patterns. Findings must directly
inform the repository audit, target architecture, gap analysis, and phased implementation plan.

## Required Source Categories

- `github`: GitHub repositories
- `papers`: Academic and research papers
- `docs`: Official documentation
- `reputable-sources`: Reputable sources

## Workflow

1. Discovery agents identify candidate sources.
2. Parent merges candidates into `source-inventory.json`.
3. Batch planner splits selected sources into context-safe batches.
4. Batch agents write cited deep-dive reports.
5. Category synthesis agents write category summaries.
6. Technical writer writes `final-report.md`.
7. Evidence verifier writes `reports/verification/evidence-audit.md`.

## Expected Outputs

- Research directory: `docs/research/2026-07-15-market-squawk`
- Source inventory: `docs/research/2026-07-15-market-squawk/source-inventory.json`
- Final report: `docs/research/2026-07-15-market-squawk/final-report.md`
- Evidence audit: `docs/research/2026-07-15-market-squawk/reports/verification/evidence-audit.md`

Workspace slug: `market-squawk-complete-local-platform-architecture-source-adapters-analytics-ris`
