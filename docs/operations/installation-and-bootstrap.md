# Installation and local bootstrap

This runbook builds the reviewed Market Squawk source, installs the complete local executable
bundle into an operator-owned versioned directory, and initializes a new local data root. The
shipping core is a local Rust application. It does not require a container runtime, cloud account,
paid provider, Python interpreter, or external ONNX Runtime library.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local operators, release installers, and maintainers |
| Status | Current |
| Last substantive review | 2026-07-26 |
| Reviewed commit | `93f79a830765781242ce824e0db84f38d04c0b63` |

## Contents

- [Scope and non-goals](#scope-and-non-goals)
- [Preconditions](#preconditions)
- [Safety and authority](#safety-and-authority)
- [Build the locked executable bundle](#build-the-locked-executable-bundle)
- [Install the bundle](#install-the-bundle)
- [Bootstrap a new data root](#bootstrap-a-new-data-root)
- [Expected success evidence](#expected-success-evidence)
- [Safe restart and upgrade](#safe-restart-and-upgrade)
- [Rollback and recovery](#rollback-and-recovery)
- [Known failure modes](#known-failure-modes)
- [Local logs, data, and artifacts](#local-logs-data-and-artifacts)
- [Related documentation and code](#related-documentation-and-code)
- [Official sources](#official-sources)

## Scope and non-goals

Use this procedure for a source-based local installation at the reviewed product head. It covers:

- the exact Rust toolchain and locked Cargo dependency graph;
- all three executables needed by the installed application;
- an operator-owned, versioned installation directory;
- configuration validation, stateful `init`, and the bounded query-only `doctor` check;
- safe replacement of one installed version with another.

This runbook does not:

- claim that a local build is an accepted release candidate or reproduce the exact-head full
  release gate;
- install a container image, system service, hosted component, or external provider account;
- onboard a data provider, start a bot, or grant source, model, risk, or execution authority;
- install the optional Python product or optional external ONNX Runtime acceleration library.

The [delivery ledger](../plans/delivery-ledger.md) alone owns mutable release state, blockers, exact
performance evidence, and full-gate acceptance.

## Preconditions

### Required core toolchain

The repository pins Rust `1.97.1` in `rust-toolchain.toml`, with the minimal rustup profile plus
`rustfmt` and `clippy`. A source installation needs:

- Git and a checkout containing reviewed commit
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`;
- rustup with the pinned `1.97.1` toolchain available;
- the platform linker and native build tools required by Rust:
  - Xcode Command Line Tools on macOS;
  - a C toolchain and linker on Linux;
  - the MSVC build tools when using the supported MSVC Rust target on Windows;
- enough operator-owned disk space for Cargo build output, the installed binaries, and the chosen
  data root.

The tracked CI workflow declares Linux, macOS, and Windows jobs. A configured matrix is not
successful execution evidence; cross-platform acceptance requires completed exact-head jobs and is
recorded only in the delivery ledger. This runbook's shell examples use a POSIX shell; binary names
end in `.exe` on Windows.

### Optional Python product

Python is not imported or launched by `init`, `config validate`, `doctor`, source onboarding, or
the core Rust application. Install it only when an admitted Python training/analytics release is
actually needed.

The reviewed sealed Python product supports normal GIL-enabled CPython `3.12` and `3.13` on macOS
12 or newer on arm64. Its lock, offline wheelhouse, two-interpreter build, source-closure checks,
and native-module admission are a separate release workflow; an arbitrary virtual environment is
not an installed Market Squawk training release. See the
[Python dependency-admission decision](../research/2026-07-22-python-product-dependency-admission.md)
and the current release evidence in the [delivery ledger](../plans/delivery-ledger.md).

### Required and optional inference runtime

The required ONNX backend is the Rust `tract` implementation and is compiled into the product.
The installed bundle also includes `market-squawk-onnx-worker`, which isolates admitted model
work. No external ONNX library is needed for this required path.

The modeling library contains an optional ONNX Runtime `1.24.4` implementation for admitted Linux
arm64 and x86-64 targets, but the reviewed product composition does not select it. The shipping
application always constructs the required tract backend. The
[model-inference runbook](model-inference.md#optional-external-onnx-runtime-evidence) documents the
external runtime only for library-integration or retained release evidence, not as a currently
selectable installation path.

## Safety and authority

1. Build only from the reviewed Git commit and its locked `Cargo.lock`. `--locked` must remain on
   every release build command.
2. Use a clean source checkout. Do not treat locally modified manifests, source, lockfiles, build
   scripts, or adapter code as the reviewed product.
3. Install all three executables into the same versioned `bin` directory:
   `market-squawk`, `market-squawk-capture-helper`, and `market-squawk-onnx-worker`.
4. Do not make either helper a symbolic link. The capture helper must be the exact regular-file
   sibling of the running application, be executable, have the same owner on Unix, and not be
   group- or other-writable. The ONNX worker is also admitted as a stable regular file.
5. Run the application as the operator who owns its installation and data. Do not use elevated
   privileges to compensate for an incorrectly owned install or data root.
6. Choose a new, dedicated data root for first bootstrap. `init` owns product initialization,
   migration, recovery, and bounded shutdown. `doctor` never substitutes for that stateful step.
7. Keep stdout and stderr separate in automation. Command results use stdout; tracing uses stderr.
   `mcp serve` reserves stdout for protocol frames.
8. A successful build or `doctor` call is local evidence only. It does not grant provider rights,
   data quality, automated-action eligibility, or release approval.

## Build the locked executable bundle

### 1. Select and verify the reviewed source

Create or use a dedicated source checkout, then detach it at the reviewed commit:

```bash
git clone https://github.com/Sawmonabo/market-squawk.git market-squawk-0.1.0
cd market-squawk-0.1.0
git checkout --detach 836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
git status --short
```

Expected output:

- `git rev-parse HEAD` prints
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`;
- the tree is `774a7bc9f4f26eb437fa1ab061dc4b557d20d0bc`;
- `git status --short` prints nothing.

If the commit is already present in a trusted local clone, a second network clone is unnecessary.
The commit and clean-tree checks remain mandatory.

### 2. Confirm the pinned toolchain

From the repository root:

```bash
rustc --version
cargo --version
rustup show active-toolchain
```

`rustc --version` must identify Rust `1.97.1`. Stop if rustup cannot select the repository-pinned
toolchain. Do not silently substitute `1.97.0` or an unpinned newer compiler for this reviewed
build.

### 3. Build the application and capture helper

```bash
CARGO_INCREMENTAL=0 cargo build --locked --release \
  --package market-squawk \
  --bin market-squawk \
  --bin market-squawk-capture-helper
```

### 4. Build the ONNX worker with the required tract backend

```bash
CARGO_INCREMENTAL=0 cargo build --locked --release \
  --package market-squawk-modeling \
  --features onnx-tract \
  --bin market-squawk-onnx-worker
```

The resulting POSIX executables are:

```text
target/release/market-squawk
target/release/market-squawk-capture-helper
target/release/market-squawk-onnx-worker
```

On Windows, use the corresponding paths ending in `.exe`.

These focused commands produce the shipping bundle without claiming the broader all-workspace,
all-feature release gate. The accepted gate uses the repository's controlled verification
workflow and is recorded only in the delivery ledger.

## Install the bundle

### POSIX installation

Choose an absolute directory owned by the runtime operator. Keep the version in the path so an
upgrade never overwrites executables underneath a running process:

```bash
INSTALL_PARENT=/absolute/operator-owned/market-squawk
INSTALL_ROOT="$INSTALL_PARENT/0.1.0-836aae6"

(
  set -eu
  umask 022

  test -d "$INSTALL_PARENT"
  test ! -e "$INSTALL_ROOT"
  mkdir -m 0755 "$INSTALL_ROOT"
  mkdir -m 0755 "$INSTALL_ROOT/bin"
  install -m 0755 target/release/market-squawk \
    "$INSTALL_ROOT/bin/market-squawk"
  install -m 0755 target/release/market-squawk-capture-helper \
    "$INSTALL_ROOT/bin/market-squawk-capture-helper"
  install -m 0755 target/release/market-squawk-onnx-worker \
    "$INSTALL_ROOT/bin/market-squawk-onnx-worker"
)
```

The subshell exits on the first failed precondition or install. Plain `mkdir` makes an occupied
version root a hard failure; never rerun against a partially created root. If installation fails,
inspect and remove only that new, inactive version root, then restart the whole bundle installation
with a new empty path. Do not point a service or operator command at the version until all three
siblings pass the checks below.

Inspect the installed files before use:

```bash
ls -l "$INSTALL_ROOT/bin/market-squawk" \
  "$INSTALL_ROOT/bin/market-squawk-capture-helper" \
  "$INSTALL_ROOT/bin/market-squawk-onnx-worker"
```

All three must be regular executable files with the same owner. Neither helper may be a symlink,
and the capture helper must not have group or other write bits.

Record SHA-256 values as installation inventory. On Linux:

```bash
sha256sum "$INSTALL_ROOT"/bin/market-squawk*
```

On macOS:

```bash
shasum -a 256 "$INSTALL_ROOT"/bin/market-squawk*
```

These locally generated digests identify the installed bytes; they are not published release
approval or a reproducible-build claim.

### Windows installation

Copy all three `.exe` files into one new, versioned, operator-owned `bin` directory. Do not use a
symlink or junction for either helper, and do not copy a new helper over a file that may be in use.
Record each file with `Get-FileHash -Algorithm SHA256`, then invoke the application by the exact
versioned path during bootstrap.

## Bootstrap a new data root

Set the exact installed application path and a new absolute data-root path:

```bash
MSQ="$INSTALL_ROOT/bin/market-squawk"
DATA_ROOT=/absolute/operator-owned/market-squawk-data
```

Do not create `DATA_ROOT` over an unrelated directory. Then run:

```bash
"$MSQ" --version
"$MSQ" --data-dir "$DATA_ROOT" --output json config validate
"$MSQ" --data-dir "$DATA_ROOT" init
"$MSQ" --data-dir "$DATA_ROOT" --output json doctor
```

`config validate` parses and validates the effective configuration without proving source or
runtime readiness. `init` prepares the controlled root, initializes or opens the production
authorities, creates the initial diagnostic journal, and completes application-owned bounded
shutdown. `doctor` then performs a query-only inspection of the existing layout, catalog,
descriptor contracts, provider sessions, configuration, and local policy. It does not create,
migrate, recover, or lock an application/MCP authority and it never probes a remote endpoint.

For a persistent configuration file, complete
[Configuration and secrets operations](configuration-and-secrets.md), then pass the same explicit
file to every command:

```bash
"$MSQ" --config /absolute/path/market-squawk.toml --output json config validate
"$MSQ" --config /absolute/path/market-squawk.toml init
"$MSQ" --config /absolute/path/market-squawk.toml --output json doctor
```

There is no implicit configuration-file discovery.

## Expected success evidence

### Build and installation

- Both Cargo commands exit `0`.
- The three expected release executables exist.
- The installed executables are regular sibling files in the same versioned `bin` directory.
- `market-squawk --version` prints `market-squawk 0.1.0`.

### Configuration and initialization

The default JSON from `config validate` includes:

```json
{
  "valid": true,
  "effective": {
    "schemaVersion": "market-squawk-effective-config-v1",
    "dataDirectory": {
      "value": "/absolute/operator-owned/market-squawk-data",
      "origin": "cli"
    },
    "products": {
      "value": ["BTC-USD"],
      "origin": "safe_default"
    },
    "sourceSecretConfigured": {
      "value": false,
      "origin": "safe_default"
    },
    "coinbaseConfigured": {
      "value": false,
      "origin": "safe_default"
    },
    "krakenConfigured": {
      "value": false,
      "origin": "safe_default"
    }
  }
}
```

Additional bounded defaults appear in the actual result. `init` prints `initialized` followed by
the canonical data-root path.

Immediately after `init`, the root contains at least:

```text
<data-root>/
├── artifacts/
├── control/
└── journal/
    └── coinbase-exchange.msj
```

`init` also creates `catalog.sqlite3` and the service-specific control directories required by the
production composition. `doctor` reports a missing or incomplete layout as a blocker and leaves it
unchanged.

### Doctor interpretation

A valid query-only preflight has:

```json
{
  "localStorage": {
    "modifiedByInspection": false,
    "layout": {
      "state": "available",
      "error": null
    },
    "catalog": {
      "state": "available",
      "journalMode": "wal",
      "error": null
    }
  },
  "application": {
    "descriptorContractValid": true,
    "runtimeState": "not_observed",
    "requiredDomainsComplete": true,
    "error": null
  },
  "mcp": {
    "descriptorContractValid": true,
    "runtimeState": "not_observed",
    "transport": "stdio",
    "durableAuditConfigured": true,
    "controlledArtifactsConfigured": true,
    "error": null
  }
}
```

`runtimeState: "not_observed"` is deliberate: the query-only command does not start an application,
adapter, bot, or MCP session. The top-level `status` therefore remains `blocked` when current
provider onboarding or runtime health has not been established, when a code-owned profile requires
evidence refresh or rights admission, or when local storage is incomplete. Use the domain-specific
`source status`, `source coverage`, and `source health` commands inside the intended runtime
workflow for current source evidence. Exact-head provider, hosted-OS, fuzz, performance, security,
and publication evidence remains authoritative only in the
[delivery ledger](../plans/delivery-ledger.md); doctor does not infer it.

## Safe restart and upgrade

Configuration is immutable for the life of a process; there is no hot reload. A routine restart is:

1. Stop long-lived `mcp serve`, capture, bot, or other Market Squawk processes through their normal
   EOF, Ctrl-C, or controlled stop boundary.
2. Wait for bounded shutdown to finish. Do not replace either helper while the application could
   still own it.
3. Run `config validate` with the exact configuration sources intended for the next process.
4. Start the exact versioned application path and retain stderr until startup is confirmed.
5. Run the domain-specific status command appropriate to the restarted workload.

For an upgrade:

1. Quiesce every process using the data root.
2. Follow [Backup and recovery](backup-and-recovery.md) to take a coherent offline backup of the
   complete data root, including the catalog and any SQLite sidecar files present after shutdown.
3. Build from the approved new source and install all three executables into a new versioned
   directory.
4. Record the new file hashes and run `--version` plus `config validate`.
5. Point the service or operator command at the new versioned application path.
6. Run `init` with the new version to perform the explicit stateful open/migration/recovery and
   application-owned bounded shutdown.
7. Run `doctor` to inspect the resulting state without reopening writer or MCP audit authority.
8. Start the intended workload only after initialization succeeds and doctor reports no unexpected
   local storage or contract blocker.
9. Retain the prior versioned binaries and pre-upgrade data backup until recovery evidence has been
   reviewed.

Never overwrite the active version in place. Do not use a symlinked helper as a “current” switch.

## Rollback and recovery

- **Build failed:** leave the installed version untouched. Correct the source checkout or
  toolchain, then rebuild into `target/release`; do not copy a partial bundle.
- **Install validation failed:** discard only the newly created version directory after verifying
  its exact path. Reinstall all three files together.
- **Bootstrap failed on a new root:** preserve stderr and inspect the exact root before cleanup.
  Remove it only when it was created solely for the failed attempt and contains no wanted state.
- **New version failed before opening state:** point the launch command back to the previous
  versioned binary bundle.
- **New version ran `init` or another stateful command:** do not run the old binary against
  possibly migrated state on assumption alone. Quiesce the new version and restore the coherent
  pre-upgrade data-root backup unless compatibility has been explicitly verified.
- **Wrong data root selected:** stop immediately. Configuration does not move or merge state.
  Correct the highest-precedence setting, validate again, and restart; do not manually combine two
  roots.

## Known failure modes

| Symptom | Likely cause | Safe response |
| --- | --- | --- |
| rustup selects a compiler other than `1.97.1` | Pinned toolchain is unavailable or locally overridden | Stop and install/select the repository-pinned toolchain |
| Cargo reports that the lockfile must change | Source and `Cargo.lock` do not match, or the command omitted `--locked` | Restore a clean reviewed checkout; do not regenerate the lockfile for this installation |
| Application starts but capture helper admission fails | Helper missing, not an exact sibling, symlinked, non-executable, differently owned, or group/other writable | Quiesce the process and reinstall the whole bundle as regular sibling files |
| ONNX worker is unavailable | Worker omitted, moved, symlinked, changed during admission, or built without the required target | Reinstall the exact worker beside the application |
| `config validate` fails before state exists | Invalid file, environment, CLI override, or closed-schema value | Follow the configuration runbook; initialization is not a remedy for invalid configuration |
| `init` rejects the path | Root cannot be safely created or canonicalized, or conflicts with existing state | Correct ownership/path selection; do not elevate or overwrite unrelated data |
| `doctor` reports an unavailable layout or catalog | `init` was not run, the layout is incomplete, permissions changed, SQLite is unavailable, or catalog identities do not match | Preserve the JSON result; run the explicit bootstrap/upgrade procedure or repair the named local cause without deleting evidence |
| `doctor` reports an invalid application/MCP descriptor contract | The binary's compiled service contract is internally inconsistent | Stop and use a verified binary; initialization cannot repair a compiled contract |
| `doctor` reports top-level `blocked` with provider observations | Onboarding, rights, release evidence, or current runtime health is incomplete or deliberately not observed by the query-only command | Use provider/source operations and the delivery ledger; do not weaken a provider or execution gate |
| Old version cannot open state after an attempted upgrade | New version initialized or migrated durable state | Restore the coherent pre-upgrade backup unless backward compatibility is proven |

## Local logs, data, and artifacts

| Location or stream | Contents |
| --- | --- |
| `<source>/target/release/` | Local Cargo build outputs; generated, replaceable, and not runtime authority |
| `<install-root>/bin/` | The three installed executable files and operator-recorded inventory |
| `<data-root>/catalog.sqlite3` | Durable catalog after `init`; SQLite `-wal` and `-shm` sidecars may exist while active |
| `<data-root>/journal/` | Immutable diagnostic/capture journal state |
| `<data-root>/artifacts/` | Controlled application artifacts |
| `<data-root>/control/` | Durable control-plane and authority state |
| stdout | Command results, or MCP protocol frames for `mcp serve` |
| stderr | Human tracing, or structured tracing with `--json-logs`; no log file is created by default |

## Related documentation and code

- [Configuration and secrets operations](configuration-and-secrets.md)
- [Source operations](source-operations.md)
- [Backup and recovery](backup-and-recovery.md)
- [CLI reference](../reference/cli.md)
- [Configuration reference](../reference/configuration.md)
- [Deployment architecture](../architecture/deployment.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Delivery ledger](../plans/delivery-ledger.md)
- [Pinned Rust toolchain](../../rust-toolchain.toml)
- [Workspace and release profile](../../Cargo.toml)
- [Application executable targets](../../apps/market-squawk/Cargo.toml)
- [Modeling worker target](../../crates/market-squawk-modeling/Cargo.toml)
- [Executable sibling admission](../../apps/market-squawk/src/local_product/executable.rs)
- [Capture-helper admission](../../crates/market-squawk-platform/src/capture/process_journal/config.rs)
- [Controlled local paths](../../crates/market-squawk-platform/src/paths.rs)
- [Full verification workflow](../../scripts/verify.sh)

## Official sources

These upstream sources were reviewed directly on 2026-07-23. They describe prerequisite tools and
formats; the reviewed Market Squawk commit remains authoritative for the exact build, bundle, and
bootstrap procedure.

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [Install Rust](https://rust-lang.org/tools/install/) | rustup is the supported Rust toolchain installer; Windows Rust installation requires the corresponding Visual Studio prerequisites | 2026-07-23 |
| [Cargo build command](https://doc.rust-lang.org/cargo/commands/cargo-build.html) | `--release` selects the release profile and `--locked` rejects dependency resolution that would change the lockfile | 2026-07-23 |
| [Python virtual environments](https://docs.python.org/3/library/venv.html) | Upstream isolation mechanism used only as an input to the separate sealed Python release workflow | 2026-07-23 |
