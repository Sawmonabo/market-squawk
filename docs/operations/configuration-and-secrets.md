# Configuration and secrets operations

This runbook creates, validates, changes, and rolls back Market Squawk's startup configuration
without exposing credential material. It also defines the secret-storage boundary actually composed
by the reviewed `LocalProduct`.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local operators, security reviewers, and maintainers |
| Status | Current |
| Last substantive review | 2026-07-25 |
| Reviewed commit | `041175590bd2e4a357ea28d75c675c252d3b3746` |

## Contents

- [Scope and non-goals](#scope-and-non-goals)
- [Preconditions](#preconditions)
- [Safety and authority](#safety-and-authority)
- [Precedence and loading](#precedence-and-loading)
- [Exact settings and bounds](#exact-settings-and-bounds)
- [Create and validate a configuration](#create-and-validate-a-configuration)
- [Inspect effective values and precedence](#inspect-effective-values-and-precedence)
- [Secret operations boundary](#secret-operations-boundary)
- [Apply a correction or planned change](#apply-a-correction-or-planned-change)
- [Expected success evidence](#expected-success-evidence)
- [Rollback and recovery](#rollback-and-recovery)
- [Known failure modes](#known-failure-modes)
- [Local logs, data, and artifacts](#local-logs-data-and-artifacts)
- [Related documentation and code](#related-documentation-and-code)
- [Official sources](#official-sources)

## Scope and non-goals

Market Squawk builds one immutable `AppConfig` at process startup. This page covers its four
configuration layers, every accepted setting, closed-file validation, redacted inspection,
OS-keyring-first secret composition, explicit encrypted-fallback unlock, and the
stop-correct-validate-restart procedure.

Configuration can select local paths, resource ceilings, timing, optional provider profiles, and
opaque secret locators. It cannot:

- contain or return resolved credential bytes;
- grant provider rights or activate an adapter;
- qualify data as `DirectVerified`;
- admit a Python training release, model, order, or execution decision merely by naming a path;
- update a running process;
- provide an encrypted-file unlock through configuration, environment, or CLI arguments.

Use [Source operations](source-operations.md) for evidence-bound provider onboarding. Mutable
release blockers and provider-qualification status remain in the
[delivery ledger](../plans/delivery-ledger.md).

## Preconditions

- Complete [Installation and local bootstrap](installation-and-bootstrap.md), or otherwise have the
  reviewed `market-squawk` executable bundle available.
- Select one intended local data root and know whether it is new or already contains durable state.
- Select an operator-owned absolute path for the TOML file. The application accepts relative paths,
  but an absolute path avoids working-directory ambiguity in services and scheduled jobs.
- Inventory all inherited `MARKET_SQUAWK_*` environment variables in the launch environment.
- For any future code-admitted credential workflow, use an available interactive OS credential
  service or plan an explicit foreground unlock of the encrypted fallback through the loopback
  portal. At the reviewed commit, every credentialed built-in provider is release-gated, so no
  credentialed provider setup procedure is currently operable.

Examples use:

```bash
MSQ=/absolute/operator-owned/market-squawk/0.1.0-836aae6/bin/market-squawk
CONFIG=/absolute/operator-owned/market-squawk/config.toml
```

## Safety and authority

1. Never place a provider key, password, token, unlock phrase, cookie, or other credential value in
   TOML, an environment variable, a CLI argument, shell history, or an activation request file.
2. The `source_secret` setting is an opaque locator only. Its accepted syntax does not connect that
   locator to either onboarding backend in the current application composition.
3. Always pass `--config <PATH>` explicitly. Market Squawk does not search the current directory,
   home directory, XDG directories, or platform configuration folders.
4. A higher-precedence layer silently replaces the same setting from a lower layer before
   validation. Inspect the launch environment and CLI arguments when a corrected file appears to
   have no effect.
5. `config validate` is a parse and whole-object validation check. It does not open provider
   connections, resolve a secret, admit a training release, or prove runtime readiness.
6. `config show` and `config validate` are redacted, but their current output still reveals
   non-secret effective values such as products and resource ceilings. Treat captured output as
   operational metadata.
7. Stop a long-lived process before changing its configuration. There is no hot reload or
   in-process rollback.
8. A changed `data_dir` selects another state root; it does not move, merge, or restore the old
   root.

## Precedence and loading

Effective configuration is composed once, in this fixed low-to-high order:

```text
safe built-in defaults
  < explicit --config TOML file
  < MARKET_SQUAWK_* process environment
  < public CLI overrides
  -> whole-object validation
  -> immutable AppConfig
```

Only values present in a higher layer replace lower-layer values. Validation occurs after the
merge, so cross-setting rules apply to the final object.

The TOML file is optional at the code boundary and is loaded only when `--config` is present. The
file must be no larger than 1 MiB, must be valid UTF-8 TOML, and has a closed root: an unknown key
rejects the complete configuration.

The environment layer accepts only the fourteen keys in the next table. An unknown
`MARKET_SQUAWK_*` key, a non-UTF-8 in-scope key or value, an invalid scalar, or oversized provider
JSON rejects startup. `MARKET_SQUAWK_LOG` is the one separate tracing variable: the CLI consumes it
before `AppConfig` validation.

## Exact settings and bounds

All durations are integer milliseconds. All memory limits are exact integer bytes.

| TOML key | Environment key | Public CLI layer | Default | Validated contract |
| --- | --- | --- | --- | --- |
| `data_dir` | `MARKET_SQUAWK_DATA_DIR` | `--data-dir` | `.market-squawk` | Nonempty path; it may be absent before `init` |
| `products` | `MARKET_SQUAWK_PRODUCTS` | No general override; `capture --products` is command-specific | `["BTC-USD"]` | `1..=128` unique values; each is `1..=128` bytes and contains only ASCII alphanumeric characters or `-`, `.`, `_`, `/` |
| `stale_after_ms` | `MARKET_SQUAWK_STALE_AFTER_MS` | None | `5000` | `250..=600000` |
| `capture_queue_capacity` | `MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY` | `--capture-queue-capacity` | `16384` | `1..=1048576` |
| `capture_memory_ceiling_bytes` | `MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES` | `--capture-memory-ceiling-bytes` | `67108864` | `1..=4294967295` |
| `capture_destination_registry_memory_ceiling_bytes` | `MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES` | `--capture-destination-registry-memory-ceiling-bytes` | `1048576` | `1..=67108864` |
| `paper_bot_enabled` | `MARKET_SQUAWK_PAPER_BOT_ENABLED` | No general override; diagnostic commands may supply their own flag | `false` | Rust boolean syntax: `true` or `false`; grants no live execution authority |
| `capture_flush_interval_ms` | `MARKET_SQUAWK_CAPTURE_FLUSH_INTERVAL_MS` | None | `1000` | Positive and no greater than `capture_shutdown_ms` |
| `capture_shutdown_ms` | `MARKET_SQUAWK_CAPTURE_SHUTDOWN_MS` | None | `5000` | Positive, `<=60000`, and no less than the flush interval |
| `source_shutdown_ms` | `MARKET_SQUAWK_SOURCE_SHUTDOWN_MS` | `--source-shutdown-ms` | `5000` | `1..=60000` |
| `training_release_root` | `MARKET_SQUAWK_TRAINING_RELEASE_ROOT` | `--training-release-root` | Unset | When present, nonempty and absolute; startup verifies that the running application and sibling ONNX worker are the exact signed files installed there |
| `source_secret` | `MARKET_SQUAWK_SOURCE_SECRET` | None | Unset | Locator only; `1..=512` bytes, no control characters, prefix `keyring:` or `encrypted-file:` |
| `coinbase` | `MARKET_SQUAWK_COINBASE_JSON` | None | Unset | Complete closed Coinbase profile; environment JSON at most 128 KiB |
| `kraken` | `MARKET_SQUAWK_KRAKEN_JSON` | None | Unset | Complete closed Kraken profile; environment JSON at most 128 KiB |

`MARKET_SQUAWK_PRODUCTS` is split literally on commas. Whitespace is not trimmed. For example,
`BTC-USD, ETH-USD` rejects the second element because it starts with a space.

Coinbase and Kraken are complete, closed profiles rather than partial patches. Their endpoint,
authorization, event, instrument, frame, queue, and timing contracts are specified in the
[configuration reference](../reference/configuration.md). The configured public Coinbase and
Kraken paths remain `DirectUnverified`. Coinbase Direct reuses the configured Coinbase
product/instrument routes and limits but additionally requires an exact active onboarding-session
UUID; the current signing secret is resolved through the composed secret backend and never appears
in configuration, environment variables, or CLI arguments.

## Create and validate a configuration

### 1. Write a non-secret baseline

Create the operator-owned file at `CONFIG`. A conservative local baseline is:

```toml
data_dir = "/absolute/operator-owned/market-squawk-data"
products = ["BTC-USD"]
stale_after_ms = 5000

capture_queue_capacity = 16384
capture_memory_ceiling_bytes = 67108864
capture_destination_registry_memory_ceiling_bytes = 1048576
capture_flush_interval_ms = 1000
capture_shutdown_ms = 5000
source_shutdown_ms = 5000

paper_bot_enabled = false
```

Omit `training_release_root` until a sealed training release has been installed and independently
verified. Omit `source_secret`, `coinbase`, and `kraken` unless a separate, evidence-reviewed
procedure supplies their exact non-secret locator or closed profile.

Protect the file from unintended modification and reading according to the host's local account
model. Even though the file must not contain credentials, it records paths, products, and resource
policy.

### 2. Validate the exact launch sources

```bash
"$MSQ" --config "$CONFIG" --output json config validate
```

This command must exit `0` and return `"valid": true`.

### 3. Initialize or inspect the selected root

For a new root:

```bash
"$MSQ" --config "$CONFIG" init
"$MSQ" --config "$CONFIG" --output json doctor
```

For an existing root, take any required coherent backup before the first stateful command with a
new application version. `config validate` does not open the root, but `doctor` composes the
product and can initialize, recover, or migrate state.

### 4. Keep invocation consistent

Pass `--config "$CONFIG"` to every command and long-lived process. A successful one-off validation
does not cause later commands to remember the path.

## Inspect effective values and precedence

Run:

```bash
"$MSQ" --config "$CONFIG" --output json config show
```

The JSON is the effective redacted value view. It includes the products, timing, resource ceilings,
paper flag, and only configured/not-configured booleans for the data root, training release,
secret locator, Coinbase profile, and Kraken profile.

To test the precedence of one non-secret value without changing the file:

```bash
MARKET_SQUAWK_SOURCE_SHUTDOWN_MS=7000 \
  "$MSQ" --config "$CONFIG" --source-shutdown-ms 9000 --output json config show
```

The effective `source_shutdown_ms` must be `9000`: the CLI layer wins over the environment, which
wins over the file.

Internally, `AppConfig` records one origin for every setting:

- `safe_default`
- `local_file`
- `environment`
- `cli`

The internal redacted provenance schema is `market-squawk-effective-config-v1`. At the reviewed
commit, however, the public `config show` and `config validate` implementations do not call that
serializer. They do **not** expose per-setting origins. Do not infer that a value came from the
file merely because it equals the file or default value; inspect the environment and launch
arguments directly.

## Secret operations boundary

### What is composed now

Production `LocalProduct` constructs:

```text
PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
    "market-squawk",
    "<data-root>/control/secrets/provider-credentials",
)
```

The preferred store probes the operating-system credential service first:

- Apple Keychain on macOS;
- Windows Credential Manager on Windows;
- a Secret Service implementation on supported Linux/Unix desktops.

Credential bytes are accepted only through an admitted write-only onboarding flow, wrapped in a
redacting and zeroizing value, written under an opaque backend reference and generation, and
verified by readback. CLI and portal status surfaces return only secret-free state such as whether
a credential is stored and which generation is active. The platform secret value must be
`1..=65536` bytes; the portal's provider-key field, when a profile is release-enabled, is further
limited to 8192 characters.

At this reviewed source head, the current code-owned credentialed profiles are not release
available:

- `bls.v2-registered` is `refresh_required`;
- `fred-alfred.api-v1-v2` is `rights_blocked`.

The portal therefore provides no current credential-import procedure for either profile. Do not
weaken the release gate or use another provider's slot.

### Encrypted fallback lifecycle

The fallback root is code-owned under the data root and starts locked in every new process. No
unlock is read from a file, configuration, environment variable, command argument, or background
restart. To make the fallback eligible for the current portal process:

1. Open the bounded loopback portal with `source setup` as described in
   [Source operations](source-operations.md).
2. In **Encrypted credential fallback**, enter the fallback unlock and select **Unlock fallback**.
   The value is sent only to that loopback process as a bounded binary secret, redacted, and
   zeroized after admission.
3. Confirm that the portal reports the fallback as ready. Secret creation still uses the OS
   credential service when its exact lifecycle is available; only a proved unavailable,
   session-unavailable, or unsupported primary permits the ready fallback.
4. Select **Lock fallback** when the foreground workflow is finished. Process shutdown also drops
   the in-memory unlock authority.

An existing `SecretRef` remains permanently bound to its recorded backend and generation. The
router never probes another backend for that reference and never migrates secret bytes between the
OS store and encrypted vault. A syntactically valid `source_secret = "encrypted-file:..."` locator
does not unlock the vault or bypass the onboarding lifecycle.

There is also no public generic `secret set`, `secret get`, `secret list`, or `secret delete`
command. Do not manipulate catalog secret references or OS-keyring entries independently of their
onboarding lifecycle.

## Apply a correction or planned change

1. Identify every process using this configuration and data root.
2. Stop long-lived MCP, capture, bot, or other Market Squawk processes and wait for bounded
   shutdown.
3. Record the prior TOML file and the intended non-secret environment/CLI layers. For a data-root
   or application-version change, take a coherent offline backup of durable state.
4. Change the lowest appropriate layer. Remove an obsolete higher-precedence environment or CLI
   value rather than duplicating the correction in several layers.
5. Validate the complete intended launch:

   ```bash
   "$MSQ" --config "$CONFIG" --output json config validate
   ```

6. Inspect the redacted effective values:

   ```bash
   "$MSQ" --config "$CONFIG" --output json config show
   ```

7. Start the intended process with the same `--config`, environment, and CLI overrides.
8. Run `doctor` or the domain-specific read-only status command and retain stdout plus stderr.

No signal or file rewrite changes the configuration of a process that is already running.

## Expected success evidence

With the baseline above, `config validate` returns this shape:

```json
{
  "valid": true,
  "effective": {
    "capture_destination_registry_memory_ceiling_bytes": 1048576,
    "capture_flush_interval_ms": 1000,
    "capture_memory_ceiling_bytes": 67108864,
    "capture_queue_capacity": 16384,
    "capture_shutdown_ms": 5000,
    "coinbase_configured": false,
    "data_root_configured": true,
    "kraken_configured": false,
    "paper_bot_enabled": false,
    "products": ["BTC-USD"],
    "source_secret_configured": false,
    "source_shutdown_ms": 5000,
    "stale_after_ms": 5000,
    "training_release_configured": false
  }
}
```

Success proves:

- the explicit file was readable, bounded, valid UTF-8 TOML, and closed-schema compliant;
- all admitted environment and CLI values parsed;
- the merged values satisfied individual and cross-setting bounds;
- secret and provider-profile contents were not emitted.

It does not prove that the data root is writable, a keyring is available, a provider is reachable,
rights are admitted, or the full product can compose. Use `doctor` and domain-specific status
commands for those separate checks.

## Rollback and recovery

- **Invalid proposed file:** keep the running configuration unchanged, restore the prior file, and
  validate it before restart.
- **Wrong environment or CLI override:** remove or correct the higher-precedence value, start a new
  process, and inspect `config show`; editing the TOML file alone will not override it.
- **Wrong data root:** stop before performing more mutations. Restore the previous `data_dir`
  source and restart. Market Squawk does not merge the accidentally selected root into the intended
  one.
- **Runtime fails after an otherwise valid change:** quiesce it, restore the prior configuration
  layers, validate, and restart. If the new version opened durable state, use the coherent
  pre-change backup unless backward compatibility is explicitly verified.
- **Keyring operation is unavailable or indeterminate:** preserve the onboarding session status
  and error. For a code-admitted foreground workflow, explicitly unlock the configured fallback in
  the same portal process and retry only the lifecycle-owned operation; otherwise follow the
  session's fail-closed reconciliation state.
- **Encrypted fallback is locked after restart:** reopen the loopback portal and submit the same
  unlock through its write-only fallback control. Do not place the unlock in startup configuration
  or automation.
- **Fallback unlock is lost or rejected:** the vault cannot be recovered without its authentic
  unlock. Preserve the vault and backup evidence; do not replace it, guess completion, or rewrite
  retained secret references.
- **Secret reference appears orphaned:** do not delete catalog rows or keyring entries by hand.
  The recorded backend and generation are part of lifecycle authority; retain evidence for a
  product-owned reconciliation path.

## Known failure modes

| Symptom | Cause | Safe response |
| --- | --- | --- |
| `--config` file not found | Wrong explicit path or service working-directory assumption | Supply the intended absolute path; there is no discovery fallback |
| File rejected as too large, invalid UTF-8, malformed TOML, or unknown field | The 1 MiB closed-file contract failed | Correct the file; do not split hidden values across unreviewed sources |
| Unknown `MARKET_SQUAWK_*` error | Typo, stale variable, or unsupported setting in the inherited environment | Remove or correct the exact variable; unrelated variables are ignored |
| Products reject despite looking valid | Literal comma split retained whitespace, duplicate, empty, oversized, or disallowed character | Supply a unique list with no padding around comma-separated environment values |
| Capture timing rejects | Flush interval is zero/greater than shutdown, or shutdown exceeds `60000` | Correct both values as one merged configuration |
| Changed file has no effect | Environment or CLI layer wins, or the existing process has not restarted | Inspect launch inputs, stop the old process, validate, and start anew |
| `config show` has no origins | Current CLI does not expose stored `ConfigProvenance` | Audit file, environment, and CLI separately; do not infer |
| `source_secret_configured` is `true` but secret use fails | The value is only a locator and does not grant onboarding or fallback-unlock authority | Use the admitted portal lifecycle; do not put the credential or unlock in configuration |
| OS credential prompt or service is unavailable | Primary keyring backend/session unavailable | Unlock the code-owned fallback through the foreground portal before an admitted credential mutation, or fail closed |
| Portal reports `invalid_unlock` | Submitted unlock does not authenticate the retained vault authority | Preserve the vault, correct the operator-owned unlock, and retry through the same bounded portal |
| Portal reports `fallback_unavailable` | Fallback is locked, unavailable, or cannot complete the requested transition | Preserve portal stderr and vault state; do not delete, recreate, or bypass the authority |
| `config validate` succeeds but `doctor` fails | Configuration validity is narrower than application/path/catalog/helper composition | Preserve doctor stderr and repair the named composition failure |
| `doctor` remains top-level `blocked` | Release-level barriers remain | Consult the delivery ledger; configuration is not authority to clear them |

## Local logs, data, and artifacts

| Location or stream | Contents and handling |
| --- | --- |
| Explicit TOML path | Operator-owned non-secret startup policy; not copied into the data root by `AppConfig` |
| Process environment and launch arguments | Higher-precedence ephemeral configuration; inventory outside Market Squawk |
| OS credential service | Credential bytes and opaque generations for admitted onboarding; not stored under the data root |
| `<data-root>/control/secrets/provider-credentials/` | Authenticated encrypted fallback vault; unlock material is never persisted there |
| `<data-root>/catalog.sqlite3` | Durable onboarding references and product catalog; may have SQLite sidecars while active |
| `<data-root>/control/` | Durable non-secret authority and recovery state |
| stdout | Redacted command results |
| stderr | Configuration/startup tracing; `--json-logs` selects structured tracing; no log file exists by default |

Market Squawk does not persist an effective configuration cache or a plaintext resolved-secret
file.

## Related documentation and code

- [Installation and local bootstrap](installation-and-bootstrap.md)
- [Source operations](source-operations.md)
- [CLI reference](../reference/cli.md)
- [Configuration reference](../reference/configuration.md)
- [Data quality and live qualification](../reference/data-quality.md)
- [Control-plane architecture](../architecture/control-plane.md)
- [Deployment architecture](../architecture/deployment.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Delivery ledger](../plans/delivery-ledger.md)
- [Configuration loading and validation](../../crates/market-squawk-platform/src/config.rs)
- [Redacted provenance representation](../../crates/market-squawk-platform/src/config/report.rs)
- [Application configuration command output](../../apps/market-squawk/src/main.rs)
- [Production `LocalProduct` composition](../../apps/market-squawk/src/local_product/mod.rs)
- [Secret-store interfaces](../../crates/market-squawk-platform/src/secrets.rs)
- [Encrypted-file implementation](../../crates/market-squawk-platform/src/secrets/encrypted.rs)
- [OS-first fallback router and explicit unlock lifecycle](../../crates/market-squawk-platform/src/secrets/preferred.rs)

## Official sources

These upstream sources were reviewed directly on 2026-07-23. They define file syntax and operating
system credential facilities; the reviewed Market Squawk commit remains authoritative for
precedence, keys, bounds, redaction, and routing.

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [TOML v1.1.0 specification](https://toml.io/en/v1.1.0) | Syntax and value model of the explicit configuration file | 2026-07-23 |
| [Apple Keychain Services](https://developer.apple.com/documentation/security/keychain-services) | Platform credential-store boundary used by the macOS primary backend | 2026-07-23 |
| [Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management) | Platform credential-store boundary used by the Windows primary backend | 2026-07-23 |
| [Secret Service API specification](https://specifications.freedesktop.org/secret-service-spec/latest/) | Desktop secret-service boundary used by the supported Linux/Unix primary backend | 2026-07-23 |
