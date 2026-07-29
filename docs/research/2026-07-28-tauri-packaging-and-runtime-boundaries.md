# Tauri packaging and installed-runtime boundaries

This research records the current upstream basis for Market Squawk's desktop data-directory,
external-program, native-package, signing, and exact-head CI decisions.

| Metadata | Value |
| --- | --- |
| Document type | Date-anchored technical research |
| Audience | Desktop, release, CI, and security maintainers |
| Status | Applied to Quarter 4 remediation |
| Research date | 2026-07-28 |
| Audit base | `03783250a1020d79cdd7f8bda424da62568dd3d5` |
| Refresh gate | Recheck Tauri configuration, bundling, and installer behavior before changing package layout or signing policy |

## Contents

- [Questions](#questions)
- [Findings](#findings)
- [Market Squawk decisions](#market-squawk-decisions)
- [Known upstream caveats](#known-upstream-caveats)
- [Sources](#sources)

## Questions

1. Where should a double-click desktop launch store its default data when no CLI, environment, or
   local-file override is supplied?
2. How should one Tauri application package multiple Rust executables without weakening existing
   sibling-file and digest admission?
3. How can package-only external binaries coexist with ordinary `cargo check` and workspace tests?
4. Which CI commit should a pull-request package build and label?
5. Which license evidence must accompany the bundled Geist font files?

## Findings

### Installed application data

Tauri's application-local data directory maps to the operating system's per-application local data
location: XDG data on Linux, Application Support on macOS, and Local AppData on Windows. This path
does not depend on the process working directory. A desktop launcher therefore should use
`app_local_data_dir` as its lowest-precedence default while preserving Market Squawk's normal local
file, environment, and CLI overrides.

This is a Market Squawk integration inference from Tauri's path contract and the existing
configuration-precedence invariant. Changing the process working directory or treating the native
path as a CLI override would obscure provenance or change precedence.

### External programs

Tauri 2's supported sidecar mechanism is `bundle.externalBin`. Each source program uses a
target-triple-suffixed filename such as `name-aarch64-apple-darwin` or
`name-x86_64-pc-windows-msvc.exe`; Tauri removes that suffix in the installed package. Tauri's
official guide includes a pre-build script pattern that asks Rust for the host tuple and stages the
matching file.

Market Squawk already requires exact regular sibling programs:

- `market-squawk-capture-helper` for process-isolated durable capture;
- `market-squawk-onnx-worker` for admitted model execution; and
- `market-squawk` as the application digest identity bound by the existing signed training release.

Bundling those exact release binaries as external programs preserves those authorities. The
WebView receives no shell permission and does not execute them; existing Rust services locate and
validate only their exact sibling paths.

### Package-only configuration

`tauri-build` resolves and copies `externalBin` files from the effective configuration during the
Cargo build script. An ordinary direct `cargo check` does not run Tauri's
`beforeBuildCommand`, so putting generated external programs in the base configuration makes
normal workspace checks fail before the programs can be staged.

The Tauri CLI officially supports one or more `--config` files as ordered configuration overlays,
including build-flavor-specific configuration. The official Tauri GitHub Action passes those
arguments through and resolves a relative config path from `projectPath`.

Market Squawk therefore keeps package-only `externalBin` and `bundle.active = true` in
`tauri.bundle.conf.json`. The base configuration remains valid for ordinary Cargo and Tauri
development. The supported package command applies the overlay; its pre-build command compiles the
three release programs and stages the exact host-triple filenames before the desktop build begins.
No placeholder binary is committed.

### Pull-request candidate identity

GitHub's official checkout action documents that pull requests otherwise operate on a merge
context and shows `github.event.pull_request.head.sha` when the head commit itself is required.
Market Squawk's release evidence requires the unchanged candidate head, so CI defines one
`CANDIDATE_SHA`, checks out that exact commit in every job, and uses the same value in package
artifact names.

### Font notice

Geist is distributed under SIL Open Font License 1.1. The upstream package includes the copyright
notice and complete license. The OFL requires redistributed copies to carry that information in a
viewable form. Native packages therefore include the exact upstream Geist notice beside the
project and other required third-party notices.

## Market Squawk decisions

1. The installed desktop's default data root is Tauri's application-local data directory.
   Explicit Market Squawk configuration retains its documented precedence and provenance.
2. Every desktop package carries `market-squawk`, `market-squawk-capture-helper`, and
   `market-squawk-onnx-worker` as exact sibling executables.
3. A small Node script using only standard-library APIs derives the Rust host tuple, obtains
   Cargo's actual target directory, verifies each release program is a nonempty regular file, and
   stages the names required by Tauri.
4. Package-only settings live in `src-tauri/tauri.bundle.conf.json`. The supported `pnpm bundle`
   command and hosted package matrix always apply that overlay.
5. Native-package inspection must confirm all three sibling programs and every required notice.
6. Pull-request verification checks out and labels the exact head commit, never the synthetic merge
   commit.

## Known upstream caveats

- Tauri issue `#15134` reports stale external-binary reuse and Windows NSIS same-version reinstall
  behavior. Market Squawk overwrites the staged input on every package build and treats a release
  version as immutable. Windows installed-upgrade acceptance must still verify that all sibling
  bytes were replaced before a signed release is approved.
- Tauri issue `#11992` documents macOS signing/notarization sensitivity around external binaries.
  The current unsigned package matrix is compilation evidence only. Signed release work must
  inspect every nested executable's signature before notarization and installation acceptance.
- Tauri development or a direct Cargo build is not a native-package command. It intentionally does
  not activate the package overlay or manufacture sidecar placeholders.

## Sources

| Source | Applied evidence | Reviewed |
| --- | --- | --- |
| [Tauri sidecars](https://v2.tauri.app/develop/sidecar/) | Supported `externalBin` contract, target-triple filenames, and staging-script pattern | 2026-07-28 |
| [Tauri configuration reference](https://v2.tauri.app/reference/config/#bundleconfig) | Bundle resources and external-program configuration | 2026-07-28 |
| [Tauri CLI reference](https://v2.tauri.app/reference/cli/) | Ordered `--config` overlays for build flavors | 2026-07-28 |
| [Tauri path API](https://v2.tauri.app/reference/javascript/api/namespacepath/) | Operating-system mapping for the application-local data directory | 2026-07-28 |
| [Official Tauri GitHub Action](https://github.com/tauri-apps/tauri-action) | `args`, `projectPath`, and relative `--config` path behavior | 2026-07-28 |
| [Official checkout action](https://github.com/actions/checkout) | Pull-request head-SHA checkout pattern | 2026-07-28 |
| [Geist license](https://github.com/vercel/geist-font/blob/main/LICENSE.txt) | Exact Geist copyright and OFL 1.1 text | 2026-07-28 |
| [SIL OFL FAQ](https://openfontlicense.org/ofl-faq/) | Redistribution notice and license obligations | 2026-07-28 |
| [Tauri issue 15134](https://github.com/tauri-apps/tauri/issues/15134) | External-binary cache and NSIS reinstall caveat | 2026-07-28 |
| [Tauri issue 11992](https://github.com/tauri-apps/tauri/issues/11992) | macOS external-binary signing/notarization caveat | 2026-07-28 |
