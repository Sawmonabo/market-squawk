# Installation, first launch, and maintenance

Use this runbook to install the complete Market Squawk v1.0.0 product, open guided setup, verify
the installed state, update or repair it, roll back one version, and uninstall without deleting
user data.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Desktop users, headless operators, support engineers, and release verifiers |
| Status | Current v1.0.0 contract |
| Last substantive review | 2026-07-30 |
| Implementation review base | `da35ef2ca1f9e1d936d5c88014f11eb9304bcca3` |

## Contents

- [Scope](#scope)
- [What the complete installation contains](#what-the-complete-installation-contains)
- [Supported platforms](#supported-platforms)
- [Install](#install)
- [First launch](#first-launch)
- [Verify the installation](#verify-the-installation)
- [Program and data locations](#program-and-data-locations)
- [Update, repair, and rollback](#update-repair-and-rollback)
- [Uninstall](#uninstall)
- [Offline installation](#offline-installation)
- [Release integrity and platform trust](#release-integrity-and-platform-trust)
- [Failure and recovery](#failure-and-recovery)
- [Development installation](#development-installation)
- [Related documentation and code](#related-documentation-and-code)
- [Official sources](#official-sources)

## Scope

This page covers supported per-user installation from the public v1.0.0 release. The normal
desktop path requires no knowledge of Rust, Python, Node.js, databases, containers, or finance.
The headless path installs the same component set and exposes durable terminal entrypoints.

Installation proves that the complete software release is present and internally consistent. It
does not:

- create a provider account or accept provider terms for the user;
- qualify any observation as `DirectVerified`;
- import private portfolios or datasets without an explicit user action;
- admit a model, approve an order, or start paper execution; or
- claim Apple Developer ID or Windows Authenticode identity when a package declares
  `provenance-only`.

## What the complete installation contains

Every supported target is one closed release. It contains:

- the Obsidian Signal Tauri desktop;
- the `market-squawk` CLI and local stdio MCP server;
- the bounded raw-capture helper;
- the isolated ONNX worker;
- the model-bundle validator and supported training driver;
- the versioned installer and maintenance command;
- uv 0.12.0;
- managed CPython 3.14.6;
- the locked offline Python analytics and modeling environment; and
- schemas, notices, licenses, checksums, and release metadata.

The installer rejects a missing, additional, oversized, unsafe, or digest-mismatched component
before activation. A version is immutable after publication. Activation changes a small local
selector only after the complete candidate has been admitted.

## Supported platforms

| Platform | Minimum | Native packages | Terminal installer |
| --- | --- | --- | --- |
| macOS Apple Silicon | macOS 12 | DMG | Yes |
| macOS Intel | macOS 12 | DMG | Yes |
| Windows x64 | Windows 10 version 1809 | Guided NSIS installer and MSI | Use the native package |
| Linux x64 | Ubuntu 24.04-compatible | AppImage and DEB | Yes |

The Linux compatibility statement covers the release's glibc and native-library baseline. Other
distributions may work but are not represented as supported until they pass the same installed
product checks.

## Install

### Recommended desktop installation

Open the [latest GitHub Release](https://github.com/Sawmonabo/market-squawk/releases/latest) and
choose the package for the computer:

- **macOS:** use the DMG matching Apple Silicon or Intel, copy **Market Squawk** into
  Applications, and open it.
- **Windows:** use the guided `-setup.exe` package for the normal per-user flow. The MSI is
  available for operators who specifically manage MSI deployment.
- **Linux:** use the AppImage for a portable desktop or the DEB on a compatible Debian/Ubuntu
  system.

The first desktop start admits the complete release embedded in the native package, installs it in
the per-user program store, supplies the active Python/modeling release to the application, and
then opens guided setup.

### One-command macOS or Linux installation

Run:

```bash
curl -fsSL \
  https://github.com/Sawmonabo/market-squawk/releases/latest/download/install.sh | sh
```

The script performs only bounded bootstrap work:

1. detects a supported operating system and architecture;
2. downloads the exact release bootstrap over HTTPS;
3. verifies its release-published SHA-256 digest;
4. hands the target-specific manifest to the Rust installer; and
5. removes its temporary files.

The Rust installer downloads, verifies, and activates the complete release. It prints durable
Desktop, CLI, and maintenance paths when it succeeds. It does not edit the shell profile or
modify system Python.

The command requires `curl`, `sh`, and either `sha256sum` or `shasum`. It does not require an
existing Market Squawk build toolchain.

## First launch

Open **Market Squawk** from the operating system after a native installation. After a terminal
installation, run the exact Desktop path printed at completion.

The dark guided setup:

1. opens the local workspace and catalog;
2. explains each product area in plain language;
3. helps select and validate zero-fee sources;
4. handles provider credentials only when a chosen provider requires them;
5. checks research, portfolio, Python/modeling, MCP, paper-execution, and storage readiness; and
6. ends with either **Ready** or named recovery actions.

The portal is local. Protected provider setup may open a temporary loopback page. Closing that
page or the desktop does not publish data or send telemetry.

## Verify the installation

### Desktop

The Overview and Operations views show the installation version, integrity state, setup status,
and recovery actions. An installed component or active-selector mismatch reports repair required;
it is never converted into readiness.

### Terminal

Set `INSTALLER` and `MSQ` to the exact maintenance and CLI paths printed by the terminal installer:

```bash
INSTALLER="/exact/printed/path/market-squawk-installer"
MSQ="/exact/printed/path/market-squawk"
DATA_ROOT="/absolute/operator-owned/path/market-squawk-data"
```

Then verify the program and initialize a new data root:

```bash
"$INSTALLER" status --json
"$MSQ" --version
"$MSQ" --data-dir "$DATA_ROOT" config validate
"$MSQ" --data-dir "$DATA_ROOT" init
"$MSQ" --data-dir "$DATA_ROOT" doctor
```

Success requires:

- installer status reports `installed: true`, version `1.0.0`, and `healthy: true`;
- `market-squawk --version` reports `1.0.0`;
- configuration validation succeeds without unknown or invalid settings;
- `init` opens the controlled layout and shuts down cleanly; and
- `doctor` reports the existing local authorities without creating or repairing them.

`doctor` does not contact providers and does not turn an unconfigured source into a healthy one.

## Program and data locations

The default program store is separate from portfolios, datasets, models, configuration, and logs.
Ordinary uninstall can therefore remove the software without deleting user work.

| Platform | Default program root |
| --- | --- |
| macOS | `~/Library/Application Support/com.MarketSquawk.Market-Squawk/program` |
| Linux | `${XDG_DATA_HOME:-~/.local/share}/marketsquawk/program` |
| Windows | `%LOCALAPPDATA%\MarketSquawk\Market Squawk\data\program` |

On Linux, a relative `XDG_DATA_HOME` is ignored and the home-directory fallback is used.

The managed store has this shape:

```text
program/
├── installation.json
├── bin/                              stable Unix desktop, CLI, and installer entrypoints
├── versions/
│   ├── 1.0.0-<manifest-sha256>/      active immutable complete release
│   └── <previous-version>/           one retained rollback version, when available
├── releases/
│   └── <manifest-sha256>/            exact retained manifest and complete bundle
└── staging/                          cleared bounded recovery workspace
```

Windows native packages own the normal Start menu and application entrypoints. macOS and Linux
terminal installations use the stable `bin/` entrypoints shown above. Update and rollback refresh
those derived files from the selected immutable release, and status verifies them against the
component receipts.

The installed desktop uses Tauri's platform application-local data directory as its safe default.
The CLI uses `.market-squawk` only when no explicit data directory is supplied. For durable
headless operation, always choose an absolute `--data-dir`. Exact configuration and local storage
semantics are in [Configuration and secrets](configuration-and-secrets.md) and the
[configuration reference](../reference/configuration.md).

## Update, repair, and rollback

The desktop exposes Status, Update, Repair, and Rollback in its Operations area. Update and
rollback require a restart because they change the active complete release.

The terminal maintenance command uses the retained HTTPS release channel:

```bash
"$INSTALLER" status
"$INSTALLER" update
"$INSTALLER" repair
"$INSTALLER" rollback
```

- **Update** accepts only a strictly newer semantic version, stages it completely, and changes the
  selector only after validation.
- **Repair** revalidates the active tree and durable entrypoints. If needed, it reconstructs the
  same version from its exact retained manifest and bundle.
- **Rollback** revalidates the one retained previous version before selecting it. It never rewinds
  catalog or dataset schemas.

If an update or rollback reports `restartRequired`, close every Market Squawk desktop, CLI, MCP,
and helper process, then start the selected release again.

## Uninstall

### Preserve user data

On Windows, removing the NSIS or MSI package through **Installed apps** runs the same
data-preserving lifecycle before Windows removes the native package.

On macOS and Linux, first open **Backup & Recovery → Uninstall programs** in the desktop, then
remove the application, AppImage, or DEB package through the platform's normal flow. A dragged
macOS application or portable AppImage has no operating-system uninstall hook, and a system-wide
Linux package manager cannot safely infer which user's program store it should delete.

For a terminal installation—or before removing a macOS/Linux native package—run:

```bash
"$INSTALLER" uninstall
```

The default removes the program store and managed entrypoints. It preserves configuration,
credentials, catalogs, portfolios, datasets, models, logs, and artifacts.

### Delete selected data classes

Data deletion is separate and must identify each exact absolute directory:

```bash
"$INSTALLER" uninstall \
  --confirm-delete-configuration "/absolute/path/to/configuration" \
  --confirm-delete-logs "/absolute/path/to/logs"
```

Available confirmations are:

```text
--confirm-delete-configuration
--confirm-delete-credentials
--confirm-delete-catalogs
--confirm-delete-portfolios
--confirm-delete-datasets
--confirm-delete-models
--confirm-delete-logs
--confirm-delete-artifacts
```

Overlapping, relative, shallow, program-contained, symlinked, or non-directory deletion targets
are rejected. Back up portfolios, datasets, models, and credentials before requesting deletion.

## Offline installation

From an online machine, download the target-specific complete ZIP, target manifest, bootstrap,
`SHA256SUMS`, and attestations from the same immutable release. Preserve their filenames and verify
them before transfer.

On the offline target:

```bash
"/path/to/market-squawk-bootstrap-TARGET" \
  install \
  --manifest "/path/to/market-squawk-release-TARGET.json" \
  --bundle "/path/to/market-squawk-1.0.0-TARGET.zip"
```

Replace `TARGET` with one of:

```text
aarch64-apple-darwin
x86_64-apple-darwin
x86_64-pc-windows-msvc
x86_64-unknown-linux-gnu
```

Offline installation performs the same manifest, archive, inventory, digest, mode, and activation
checks. It does not resolve Python packages or download another component.

## Release integrity and platform trust

The public release is all-or-nothing: Linux, Windows, both macOS architectures, complete Python
3.14.6 products, native package installation, desktop startup, CLI doctor, MCP, lifecycle
operations, checksums, and GitHub attestations must pass before publication.

With GitHub CLI installed, verify release assets with:

```bash
mkdir market-squawk-release-verify
gh release download v1.0.0 \
  --repo Sawmonabo/market-squawk \
  --pattern SHA256SUMS \
  --pattern install.sh \
  --dir market-squawk-release-verify
gh release verify v1.0.0 --repo Sawmonabo/market-squawk
gh release verify-asset v1.0.0 \
  market-squawk-release-verify/install.sh \
  --repo Sawmonabo/market-squawk
gh attestation verify \
  market-squawk-release-verify/install.sh \
  --repo Sawmonabo/market-squawk
```

The cross-platform release index records one native trust mode for each target:

- `developer-id-signed-and-notarized` only after Apple verification, timestamping, notarization,
  and stapling succeed;
- `authenticode-signed` only after Windows publisher and timestamp verification succeeds; or
- `provenance-only` when the zero-cost release relies on GitHub provenance, attestation, exact
  checksums, and transparent package identity.

An operating system may show an unfamiliar-publisher warning for a provenance-only package. Verify
the release first, then use the operating system's documented manual-open path if the user chooses
to trust those exact bytes. Do not disable platform security globally.

## Failure and recovery

| Symptom | Meaning | Recovery |
| --- | --- | --- |
| Terminal installer says its template is unpublished | A source-tree template was run instead of the release asset | Download `install.sh` from `releases/latest/download` |
| Bootstrap digest mismatch | Downloaded bootstrap differs from the release-bound identity | Stop; remove the temporary download and retry from the official release |
| `already installed` | An active selector exists | Use `status`, `update`, or `repair` |
| `healthy: false` | Active release or derived Unix entrypoints differ from receipts | Close running processes and run `repair` |
| Update is not newer | Candidate version is equal to or older than active | Keep the active version or use the explicit one-version `rollback` |
| No previous version is available | No second admitted version is retained | Repair the current version or install a newer release |
| Desktop reports packaged release unavailable | A production package lacks its complete embedded release | Re-download the native package; do not copy only the desktop binary |
| Provider is not ready after install | Software installation and provider activation are separate | Continue guided setup and resolve the named provider requirement |
| Python/model readiness fails | The active complete release or admitted model does not match | Repair the release, then revalidate the exact model bundle |

Never edit `installation.json`, an immutable version, a retained manifest, or a bundle by hand.
Repair from the retained exact release or reinstall from the immutable public release.

## Development installation

Building from source is a contributor workflow, not the normal installation path. It requires the
pinned Rust toolchain, Node.js, pnpm, Tauri prerequisites, and platform build tools. Follow the
[README Development section](../../README.md#development) and
[CONTRIBUTING.md](../../CONTRIBUTING.md). A source build does not carry public-release
attestations merely because its tests pass.

## Related documentation and code

- [README quick start](../../README.md#quick-start)
- [Configuration and secrets operations](configuration-and-secrets.md)
- [Source operations](source-operations.md)
- [Model training and inference](model-inference.md)
- [Backup and recovery](backup-and-recovery.md)
- [Deployment architecture](../architecture/deployment.md)
- [Configuration reference](../reference/configuration.md)
- [CLI reference](../reference/cli.md)
- [Release trust decision](../architecture/decisions/0006-complete-versioned-release-bundles.md)
- [Installer lifecycle implementation](../../apps/market-squawk-installer/src/lifecycle.rs)
- [Release workflow](../../.github/workflows/release.yml)

## Official sources

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [Tauri distribution](https://v2.tauri.app/distribute/) | Native package families and platform distribution boundary | 2026-07-30 |
| [Tauri Windows installers](https://v2.tauri.app/distribute/windows-installer/) | NSIS/MSI and Windows runtime behavior | 2026-07-30 |
| [Tauri AppImage](https://v2.tauri.app/distribute/appimage/) | Linux AppImage runtime and compatibility considerations | 2026-07-30 |
| [GitHub artifact attestations](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations) | Public build-provenance verification | 2026-07-30 |
| [GitHub CLI attestation verification](https://cli.github.com/manual/gh_attestation_verify) | Local artifact-attestation command | 2026-07-30 |
| [GitHub immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases) | Tag and asset immutability boundary | 2026-07-30 |
| [Apple: Open a Mac app from an unidentified developer](https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac) | Narrow user-controlled manual-open recovery | 2026-07-30 |
| [Microsoft SmartScreen overview](https://learn.microsoft.com/en-us/windows/security/operating-system-security/virus-and-threat-protection/microsoft-defender-smartscreen/) | Windows reputation and warning boundary | 2026-07-30 |
| [`directories` 6.0.0](https://docs.rs/directories/6.0.0/directories/struct.ProjectDirs.html) | Per-user program-root derivation | 2026-07-30 |
