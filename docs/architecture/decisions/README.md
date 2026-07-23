# Architecture decision records

This directory records accepted, architecturally significant Market Squawk decisions. Each ADR
captures the context, chosen structure, consequences, rejected alternatives, and exact evidence
supporting the choice.

| Field | Value |
| --- | --- |
| Document type | ADR index |
| Audience | Maintainers, reviewers, architects, and contributors |
| Status | Current |
| Last substantive review | 2026-07-23 |
| Reviewed commit | `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` |

## Accepted decisions

| ADR | Decision | Architectural consequence |
| --- | --- | --- |
| [0001](0001-separate-live-and-research-planes.md) | Separate live and research planes | Current market action and point-in-time research use independent pipelines over shared semantics |
| [0002](0002-evidence-derived-execution-quality.md) | Derive execution quality from complete evidence | `DirectVerified` and current execution authority cannot be assigned by adapters, archives, valuation, or depth |
| [0003](0003-single-writer-live-state.md) | Use deterministic single-writer live state | One shard actor orders mutation for each venue/instrument route under bounded admission |
| [0004](0004-local-analytical-storage-stack.md) | Use a local analytical storage stack | SQLite, Arrow, Parquet, and DataFusion each own a distinct control, exchange, storage, or query role |
| [0005](0005-central-risk-and-execution-authority.md) | Centralize risk and execution authority | Strategies produce intents, risk alone approves, and the bounded dispatcher alone reaches an adapter |

## ADR lifecycle

An ADR is added or superseded only for a decision with durable effects on dependencies, authority,
data semantics, runtime ownership, deployment, security, or quality attributes. Small implementation
choices remain in code and rustdoc.

Statuses have these meanings:

- `Proposed`: under review and not authoritative.
- `Accepted`: current architecture authority.
- `Superseded`: retained for history and linked to its replacement ADR.
- `Rejected`: considered but never adopted.

Accepted ADR text is not silently rewritten when the decision changes. A new ADR records the new
context and explicitly supersedes the old one. Corrections that only repair links, spelling, or a
fact about the same decision may update the existing record with normal review.

## Required ADR structure

Every new record contains:

1. a numbered title and status;
2. decision date;
3. context and architectural forces;
4. one explicit decision;
5. positive and negative consequences;
6. rejected alternatives;
7. related architecture; and
8. reviewed code evidence and relevant primary sources.

## Related documentation

- [Architecture index](../README.md)
- [Architecture overview](../overview.md)
- [Quality attributes](../quality-attributes.md)
- [Security and trust boundaries](../security-and-trust-boundaries.md)
- [ADR organization](https://adr.github.io/)
- [arc42 architecture decisions](https://docs.arc42.org/section-9/)
