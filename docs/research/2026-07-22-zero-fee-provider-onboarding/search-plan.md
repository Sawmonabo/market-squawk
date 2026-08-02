# Search Plan


## Table of Contents

- [Topic](#topic)
- [As-of Date](#as-of-date)
- [Discovery Categories](#discovery-categories)
- [Batching Rule](#batching-rule)

## Topic

Zero-fee provider onboarding portal for Market Squawk: official user authorization, account and API credential issuance, local secret activation, and automation boundaries

## As-of Date

2026-07-22

## Discovery Categories

- `github` -> `prompts/discovery/github-discovery.md`
- `papers` -> `prompts/discovery/papers-discovery.md`
- `docs` -> `prompts/discovery/docs-discovery.md`
- `reputable-sources` -> `prompts/discovery/reputable-sources-discovery.md`

## Batching Rule

Discovery agents should find candidates, not deep-read every source. After candidate selection,
run `scripts/plan_batches.py` to split sources into context-safe batches.

Completed planning uses four official-documentation batches and one batch each for GitHub,
academic papers, and reputable sources. The exact assignment ledger is `source-inventory.json` and
`batch-manifest.md`; discovery candidates that were duplicative or less decision-relevant are
explicitly excluded rather than silently dropped.
