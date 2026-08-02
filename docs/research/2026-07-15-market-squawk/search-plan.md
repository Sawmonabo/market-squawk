# Search Plan

## Table of Contents

- [Topic](#topic)
- [As-of Date](#as-of-date)
- [Discovery Categories](#discovery-categories)
- [Batching Rule](#batching-rule)

## Topic

Market Squawk complete local platform architecture, source adapters, analytics, risk, valuation, and MCP implementation evidence

## As-of Date

2026-07-15

## Decision Context

Validate the architecture and staged migration from the current single-crate v0.1 to the
complete local release, including upstream compatibility, source obligations, temporal-data
correctness, live-path integrity, risk, valuation, and bounded local MCP exposure.

## Discovery Categories

- `github` -> `prompts/discovery/github-discovery.md`
- `papers` -> `prompts/discovery/papers-discovery.md`
- `docs` -> `prompts/discovery/docs-discovery.md`
- `reputable-sources` -> `prompts/discovery/reputable-sources-discovery.md`

## Batching Rule

Discovery agents should find candidates, not deep-read every source. After candidate selection,
run `scripts/plan_batches.py` to split sources into context-safe batches.
