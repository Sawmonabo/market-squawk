# Reference

These pages define the exact desktop and CLI launchers, configuration, local MCP surface, source
coverage, quality semantics, and time/provenance contract at the reviewed implementation base.

| Field | Value |
| --- | --- |
| Document type | Reference index |
| Audience | Operators, integration authors, maintainers, and reviewers |
| Status | Current |
| Last substantive review | 2026-08-11 |

## References

| Reference | Contract |
| --- | --- |
| [CLI and desktop launcher](cli.md) | Desktop options, CLI hierarchy, confirmations, limits, output, and authority mapping |
| [Configuration and secrets](configuration.md) | Precedence, keys, defaults, environment, provider profiles, secret locators, and reporting |
| [Provider credential input template](market-squawk-provider-credentials.env.example) | Exact `market-squawk-provider-credentials/v1` design contract for a thin no-portal parser in existing onboarding; not yet consumed by the product |
| [Selected provider contracts](providers/README.md) | Per-source authentication, endpoints, feed semantics, capacity evidence, canonical destinations, scheduling, implementation status, and acceptance gates |
| [Canonical market-data schemas](market-data-canonical-schemas.md) | Shared evidence envelope, typed data families, clocks, exact values, revision/supersession, Arrow/Parquet publication, PIT selection, model bindings, and product reads |
| [Model Context Protocol](mcp.md) | Stdio lifecycle, exact 63-tool registry, schemas, annotations, limits, artifacts, audits, and cancellation |
| [Source coverage](source-coverage.md) | Supported adapters, current coverage/quality ceilings, rights, credentials, and health semantics |
| [Data quality](data-quality.md) | Independent quality classes, `DirectVerified` evidence, transitions, and execution eligibility |
| [Time and provenance](time-and-provenance.md) | Canonical identifiers, event/research time fields, revision/supersession, and point-in-time rules |

## How the contracts relate

```mermaid
flowchart LR
    CLI["CLI command"]
    Desktop["Desktop presentation command"]
    MCP["MCP tool"]
    Config["Validated configuration"]
    Source["Source coverage and rights"]
    Schema["Canonical market-data schemas"]
    Time["Time and provenance"]
    Quality["Data quality"]
    Service["Typed application service"]
    Result["Bounded result or controlled artifact"]

    Config --> Service
    CLI --> Service
    Desktop --> Service
    MCP --> Service
    Source --> Service
    Schema --> Service
    Time --> Service
    Quality --> Service
    Service --> Result
```

Desktop, CLI, and MCP use the same application-domain services except for explicitly documented
presentation or CLI-owned operations such as provider setup, local initialization, confined input
admission, and bounded read-only DataFusion SQL. A well-formed presentation request does not create
financial authority.

## Reference conventions

- JSON field names use the exact external spelling.
- Byte and count limits are exact integers unless a page states otherwise.
- Time values identify their format and clock semantics; similarly named timestamps are not
  interchangeable.
- Source coverage, data quality, market depth, and fair-value hierarchy remain distinct types.
- Mutable release completion belongs in the [delivery ledger](../plans/delivery-ledger.md), not in
  these contracts.
- Examples illustrate schema shape unless explicitly identified as measured command output.

## Task-oriented guidance

Use the [operations portal](../operations/README.md) for procedures. Use the
[architecture portal](../architecture/README.md) for rationale, ownership, runtime flow, deployment,
and decisions.
