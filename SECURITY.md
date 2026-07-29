# Security Policy

## Supported release

The `1.0.0` release candidate is developed on the historically named
`release/market-squawk-v0.1.0` integration branch; that branch name is retained for audit
continuity and does not define the package version. No complete local release is currently approved
or tagged. Security corrections are integrated into the active release branch and then into `main`
through the reviewed release pull request. This is research infrastructure and not a production
brokerage or live-order execution system.

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

Core local operation and public-source adapters require no paid service and no mandatory
credential. Authenticated Coinbase Direct and registered provider modes use explicit user-owned
accounts where required. The shipping onboarding authority stores secret material in the operating
system keyring when available and otherwise in the encrypted local fallback; configuration,
tracing, CLI, MCP, provider evidence, and release evidence expose only redacted locators or
credential-free digests. Provider activation is generation-bound, and credential replacement is
transactional and recoverable. Never place a credential in a configuration file, command line,
issue, pull request, log excerpt, or evidence artifact.
