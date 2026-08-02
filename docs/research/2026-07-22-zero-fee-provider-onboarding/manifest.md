# Research Manifest

## Table of Contents

- [Topic](#topic)
- [As-of Date](#as-of-date)
- [Decision Context](#decision-context)
- [Required Source Categories](#required-source-categories)
- [Workflow](#workflow)
- [Completed Evidence Chain](#completed-evidence-chain)
- [Expected Outputs](#expected-outputs)

## Topic

Zero-fee provider onboarding portal for Market Squawk: official user authorization, account and API credential issuance, local secret activation, and automation boundaries

## As-of Date

2026-07-22

## Decision Context

Design mandatory Task 19A: a local onboarding portal that minimizes manual setup for Market Squawk's
zero-fee providers while using only provider-supported authorization and credential-issuance flows.
Determine which providers need no account, which require a free account or API key, which expose an
official OAuth/device/CLI/API enrollment path, which require human browser interaction, and how to
verify, store, rotate, revoke, and audit credentials without ever recording secret values.

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

## Completed Evidence Chain

All four required categories are included. The canonical inventory contains 60 assigned sources and
17 explicit candidate exclusions. Seven batch reports feed four category syntheses, which feed the
root final report. Every assigned source carries category, priority, status, batch, access date, and
content digest or explicit refresh-required stable reference. The current verifier file preserves
the prior failed audit and must be replaced by a fresh independent verification of this candidate.

## Expected Outputs

- Research directory: `docs/research/2026-07-22-zero-fee-provider-onboarding`
- Source inventory: `docs/research/2026-07-22-zero-fee-provider-onboarding/source-inventory.json`
- Final report: `docs/research/2026-07-22-zero-fee-provider-onboarding/final-report.md`
- Structured decision companion: `docs/research/2026-07-22-zero-fee-provider-onboarding/final-report.json`
- Evidence audit: `docs/research/2026-07-22-zero-fee-provider-onboarding/reports/verification/evidence-audit.md`

Workspace slug: `zero-fee-provider-onboarding-portal-for-market-squawk-official-user-authorizatio`
