# Security Policy

## Supported release

The `main` branch and the latest tagged `0.x` release receive security fixes. This is research infrastructure and not a production brokerage or execution system.

## Reporting

Report suspected vulnerabilities privately through GitHub Security Advisories for the repository. Do not include live credentials, private portfolio data, exchange secrets, or proprietary datasets in an issue.

## Security boundaries

- Live order submission is not implemented.
- The built-in strategy is paper-only.
- MCP uses local stdio and exposes no arbitrary shell, SQL, network, or filesystem tool.
- Tool arguments are allowlisted and bounded.
- The paper risk kill switch cannot be reversed inside the running process.
- Raw source frames are accepted by the journal writer before decoded events are published.
- Each journal file enforces a single active writer through an operating-system file lock.
- A disconnect, crossed book, invalid sequence, delta-before-snapshot, or stale book fails closed.
- Heartbeats do not refresh order-book freshness.

## Secrets

Version 0.1 does not require credentials. Future credentialed adapters must use operating-system secret storage or encrypted local configuration, redact secrets from logs and MCP responses, and receive a separate threat-model review before merge.

## Non-goals

This project does not support quota evasion, identity rotation, anti-bot bypassing, stealth scraping, or circumvention of provider access controls.
