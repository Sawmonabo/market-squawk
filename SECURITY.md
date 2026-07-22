# Security Policy

## Supported release

The `main` branch and the latest tagged `0.x` release receive security fixes. This is research infrastructure and not a production brokerage or execution system.

## Reporting

Use the private vulnerability-reporting option on the repository's Security page when GitHub shows
it. If no private reporting option is visible, contact `@Sawmonabo` through an already established
private channel and request a reporting route without disclosing vulnerability details. Never open
a public issue or pull request containing a suspected vulnerability, live credential, private
portfolio data, exchange secret, proprietary dataset, or unredacted sensitive log.

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
