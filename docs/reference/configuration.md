# Configuration and secrets reference

This is the exact startup-configuration contract for the installed Market Squawk binaries. It is
not the editable product-settings contract: service-owned settings are read, previewed, and changed
only through `market-squawk operations settings` (or the equivalent typed application operation).

| Field | Value |
| --- | --- |
| Document type | Reference |
| Status | Current implementation contract |
| Last substantive review | 2026-08-03 |
| Authority | `crates/market-squawk-platform/src/config.rs` |

## Scope and precedence

`AppConfig` is composed once before a CLI, relay, service, or diagnostic command acquires product
authority. Its low-to-high precedence order is built-in default, the one explicit `--config` TOML
file, supported `MARKET_SQUAWK_*` environment settings, and the command's supported CLI
override. There is no implicit configuration-file discovery or in-process reload. Invalid
recognized values reject the merged configuration without echoing sensitive input.

Installed Desktop, CLI, service, and MCP relay capture only supported application setting names
from the ambient environment. Unrelated prefixed variables are ignored; recognized values remain
strictly validated. `MARKET_SQUAWK_LOG` is a separate tracing option. The installed service owns the active
workspace after startup. Client commands discover it through the authenticated owner-only rendezvous;
the rendezvous endpoint, credentials, service generation, and workspace identity are not user
configuration fields.

## Accepted startup settings

All integer ceilings are bytes, counts, or milliseconds as named. The accepted TOML keys are
closed. Installed binaries ignore unrelated inherited `MARKET_SQUAWK_*` names; a recognized
setting with a non-UTF-8 value or invalid scalar fails closed.

| TOML key | Environment | CLI override | Default | Exact admission |
| --- | --- | --- | --- | --- |
| `data_dir` | `MARKET_SQUAWK_DATA_DIR` | `--data-dir` | `.market-squawk` for CLI/service/relay | Nonempty local path; it may be created after configuration validation. |
| `products` | `MARKET_SQUAWK_PRODUCTS` | command-specific diagnostic override | `["BTC-USD"]` | Unique `1..=128` entries, each `1..=128` ASCII bytes from alphanumerics plus `-`, `.`, `_`, `/`. The environment form is comma-separated and does not trim whitespace. |
| `stale_after_ms` | `MARKET_SQUAWK_STALE_AFTER_MS` | internal only | `5000` | `250..=600000`; market-data freshness, not a process heartbeat. |
| `capture_queue_capacity` | `MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY` | `--capture-queue-capacity` | `16384` | `1..=1048576`. |
| `capture_memory_ceiling_bytes` | `MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES` | `--capture-memory-ceiling-bytes` | `67108864` | `1..=4294967295`. |
| `capture_destination_registry_memory_ceiling_bytes` | `MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES` | `--capture-destination-registry-memory-ceiling-bytes` | `1048576` | `1..=67108864`. |
| `paper_bot_enabled` | `MARKET_SQUAWK_PAPER_BOT_ENABLED` | diagnostic only | `false` | Boolean diagnostic capture/replay setting; it grants no production Paper or execution authority. |
| `capture_flush_interval_ms` | `MARKET_SQUAWK_CAPTURE_FLUSH_INTERVAL_MS` | internal only | `1000` | Positive and no larger than `capture_shutdown_ms`. |
| `capture_shutdown_ms` | `MARKET_SQUAWK_CAPTURE_SHUTDOWN_MS` | internal only | `5000` | Positive, no more than `60000`, and no less than the flush interval. |
| `source_shutdown_ms` | `MARKET_SQUAWK_SOURCE_SHUTDOWN_MS` | `--source-shutdown-ms` | `15000` | At least `2 × capture_shutdown_ms + 1000` and at most `121000`. |
| `training_release_root` | `MARKET_SQUAWK_TRAINING_RELEASE_ROOT` | `--training-release-root` | unset | Nonempty absolute path when set; admitted models also require the release-bound application and sibling ONNX worker identities to verify. |
| `coinbase` | `MARKET_SQUAWK_COINBASE_JSON` | internal typed override | unset | Complete closed Coinbase profile; environment JSON is at most 128 KiB. |
| `kraken` | `MARKET_SQUAWK_KRAKEN_JSON` | internal typed override | unset | Complete closed Kraken profile; environment JSON is at most 128 KiB. |

`--log` (or `MARKET_SQUAWK_LOG`) defaults to `info` and controls local stderr tracing. `--json-logs`
selects JSON tracing. Neither is an `AppConfig` setting. The public CLI's other global options are
listed in the [CLI reference](cli.md).

## Explicit TOML file

Pass a file only with `--config <PATH>`. It must be UTF-8 TOML no larger than 1 MiB; its root and
provider tables are closed objects. Omitted values retain lower-precedence values. This is a valid
shape, not a required complete file:

```toml
data_dir = "/absolute/or/relative/local/path"
products = ["BTC-USD"]
stale_after_ms = 5000
capture_queue_capacity = 16384
capture_memory_ceiling_bytes = 67108864
capture_destination_registry_memory_ceiling_bytes = 1048576
paper_bot_enabled = false
capture_flush_interval_ms = 1000
capture_shutdown_ms = 5000
source_shutdown_ms = 15000
training_release_root = "/absolute/path/to/installed-training-release"
```

Do not place a credential, bearer token, MCP token, raw provider secret, or secret locator in
startup TOML or environment. Provider credentials are managed through native onboarding.

## Provider profile contract

The optional `coinbase` and `kraken` profiles are complete, code-owned live-source profiles. A
profile permits construction only; it does not register a provider, prove rights, qualify a market
observation, or create execution authority. Those actions use the typed Source, Bot, Execution,
and Risk operations.

Both profiles require separate closed `authorization` and `reference_authorization` objects.
The former covers the live market feed; the latter covers Coinbase's public product REST
reference or Kraken's v2 instrument reference channel. Each object uses these fields:

| Field | Contract |
| --- | --- |
| `mode` | `public_interface` |
| `provider` | `coinbase-exchange` for Coinbase; `kraken` for Kraken |
| `basis`, `evidence_reference`, `evidence_version` | Valid bounded identifiers/locators |
| `evidence_sha256` | Exactly 64 hexadecimal characters |
| `effective_from_unix_nanos`, `effective_until_unix_nanos` | Signed nanoseconds; the half-open interval must be increasing and cover the use instant |

Coinbase is fixed to `wss://advanced-trade-ws.coinbase.com`, exactly one each of
`book_snapshot`, `book_delta`, and `trade`, and `price_level` depth. Its freshness is
`250..=600000` ms; the frame ceiling is `1..=16777216`; the control-byte ceiling is
`1..=4194304`; the acknowledgement timeout is `1..=60000` ms; control message capacity is
`1..=4096`; and the public runtime admits exactly one crypto instrument mapping per connection.
Its serialized subscription is capped at 16 KiB.

Kraken is fixed to `wss://ws.kraken.com/v2`, `book`, and depth `10`. It has the same freshness,
frame, acknowledgement, and control-capacity ranges, but admits one canonical crypto instrument
mapping. Canonical mapping fields are validated against the code-owned instrument-definition
contract; they are not a generic venue or symbol escape hatch.

## Secret boundary

`AppConfig` contains neither provider credentials nor secret locators. Provider onboarding prefers
the current user's OS credential facility (Apple Keychain, Windows Credential Manager, or Secret Service).
The locked encrypted-file fallback under the product control root is eligible only when the primary
backend is unavailable, the fallback was explicitly configured and unlocked, and the exact
operation permits it. Existing references are never silently migrated between backends.

Secret values are bounded to `1..=65536` bytes, redacted in debug output, and zeroized on drop.
The installed service separately holds its service/rendezvous and named-client MCP credentials in
protected per-user state. CLI arguments, configuration files, environment variables, MCP stdio,
and the WebView do not expose those credentials.

## Effective configuration versus product settings

`market-squawk config show` and `market-squawk config validate` emit the redacted
`market-squawk-effective-config-v1` view. Each startup setting reports `value` and one origin:
`safe_default`, `local_file`, `environment`, or `cli`. Live profiles are represented only as
configured/not-configured facts; provider credentials are outside this configuration view.

Runtime product settings are a distinct typed, revision-fenced authority. `operations settings get`
returns settings, origin, and restart impact; changes and rollbacks require a preview and then an
exact `--preview-id`, `--preview-digest`, and `--confirm` application. It intentionally has no raw
TOML editor, arbitrary key, arbitrary path, secret, or environment mutation surface.

## Failure and recovery

Configuration validation is necessary but not evidence that a provider, dataset, model, service,
or paper operation is ready. Correct the explicit source and rerun `market-squawk config validate`.
For service discovery, a missing, stale, retired, or unauthenticated rendezvous is a service
lifecycle/repair condition, not a reason to edit the rendezvous or inject a port/token manually.

## Related references

- [CLI reference](cli.md)
- [MCP reference](mcp.md)
- [Configuration and secrets operations](../operations/configuration-and-secrets.md)
- [Configuration implementation](../../crates/market-squawk-platform/src/config.rs)
- [Installed service](../../apps/market-squawk/src/service/mod.rs)
