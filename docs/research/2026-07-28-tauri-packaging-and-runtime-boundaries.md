# Tauri packaging and installed-runtime boundaries

This research records the current upstream basis for Market Squawk's desktop data-directory,
external-program, local MCP, native-package, signing, license, and exact-head CI decisions.

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
5. Which runtime evidence can support the desktop's local MCP availability claim?
6. Which license evidence must accompany the bundled Geist and Geist Mono font files?
7. How can a portable Linux AppImage expose a durable MCP client command after its desktop payload
   exits?
8. How can Linux native packages avoid executing mutable, unverified bundler downloads?

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

### Installed MCP availability

The application capability registry proves that the Rust operation surface exists, but it does not
prove that a native desktop package contains the CLI process that owns stdio MCP. Conversely, a
named sibling file does not prove the bounded MCP advertisement is valid. Desktop readiness must
therefore require both facts: the installed `market-squawk` sibling is a stable regular executable
with safe ownership/write permissions where the platform exposes them, and the complete
application capability set validates against the production MCP limits. This inspection starts no
protocol session and claims no peer identity.

### AppImage MCP dispatch

An AppImage is a portable executable, not an installed directory. Its runtime mounts the embedded
SquashFS under temporary `APPDIR`, starts the payload, then unmounts and removes that mountpoint
after the payload exits. A generated client command that points directly at an `externalBin`
sidecar under `APPDIR` therefore becomes invalid when the desktop closes.

The AppImage runtime separately supplies `APPIMAGE` as the resolved absolute path to the durable
outer image. Market Squawk uses that file as the generated client program and a hidden,
Linux-only `--stdio-mcp` package-transport flag. Before constructing Tauri, configuration, paths,
or `LocalProduct`, the desktop entrypoint validates the canonical AppImage/AppDir/current-program
relationship and the fixed CLI sibling, reconstructs only the typed configuration/data/model
arguments plus literal `mcp serve`, and uses Unix `exec` with inherited stdio.

The current Type-2 runtime replaces itself with the mounted `AppRun`; Tauri's AppRun then replaces
itself with the desktop payload; Market Squawk finally replaces the desktop payload with the CLI.
The normal production path therefore preserves one process identity and its standard streams
end-to-end while the mounted payload remains in use. `APPIMAGE_EXTRACT_AND_RUN=1`, which hosted CI
uses when FUSE is unavailable, instead introduces a wrapper process. CI consequently uses that mode
only to verify protocol startup, tool calls, and EOF shutdown; it does not treat it as evidence for
production signal-forwarding behavior. These conclusions are based on the current upstream runtime
and bundler implementations plus Rust's documented `CommandExt::exec` semantics.

### Font notices

Geist and Geist Mono are distributed under SIL Open Font License 1.1. The two locked Fontsource
5.3.0 packages contain distinct copyright statements: the Mono package names
`GeistMono-Italic[wght].ttf` and uses the repository's `.git` URL. The OFL requires each
redistributed copy to carry its applicable copyright notice and complete license in a viewable
form. Native packages therefore include the exact resolved Geist and Geist Mono license files
beside the project and other required third-party notices; one family notice cannot substitute for
the other.

### Linux bundler-tool integrity

Tauri 2.11.4's AppImage path prepares five external tools. When absent, it downloads each tool and
then executes the linuxdeploy/plugin chain. The current implementation uses the general download
helper rather than its available digest-verifying helper. Two inputs are raw `master` scripts and
one is a `continuous` release asset, so an exact Market Squawk source head alone cannot identify
the executed package toolchain.

Market Squawk enables Tauri's package-only `useLocalToolsDir` setting and prepares the exact cache
names under Cargo's `target/.tauri/` before Tauri checks them. The existing package-preparation
program admits only Linux x86-64 and these five reviewed identities:

| Cache name | Immutable source identity | Bytes | Reviewed source SHA-256 | Executed/cache SHA-256 |
| --- | --- | ---: | --- | --- |
| `AppRun-x86_64` | GitHub release asset `274691722` | 31,552 | `f30140a43a0a59e46db21bdefdf749b9e9f2c6946e92afabbacf98b8ae73fb4f` | Same as source |
| `linuxdeploy-x86_64.AppImage` | GitHub release asset `182515537` | 13,264,064 | `e762bea85c8eb0d4b3508d46e5c1f037f717d0f9303ae3b4aafc8b04991fa1ef` | `20eebde3c18ae2e44279bd624fc72482503aece216d5d77f10932235342f71c1` |
| `linuxdeploy-plugin-gtk.sh` | Commit `b5eb8d05b4c0ed40107fe2158c5d8527f94568ef` | 11,648 | `cb379f9b0733e9ad9f8bd78f8c2fa038aef2478523bb7d4c8e64ff6a1ea3501a` | Same as source |
| `linuxdeploy-plugin-gstreamer.sh` | Commit `2a2e67491c32995a3f279ad0ecbe77abd512b42a` | 4,857 | `c107b49d84edbffc6ab226ed1007e0626a4f7aa2c3a36b7782bef62351d49e94` | Same as source |
| `linuxdeploy-plugin-appimage.AppImage` | GitHub release asset `462804774` | 16,484,856 | `1da16a46fa5e058ae740e7c35ed0d36d86cb869ac9cc8a5fd9a1847d7978d99a` | Same as source |

Every package run verifies existing cache bytes, including cache restores, before use. A missing or
mismatched input is downloaded through its immutable asset API or commit URL into a bounded
temporary file, checked for exact length and SHA-256, atomically installed, and verified again.
Unsupported Linux architectures and any acquisition or identity mismatch stop packaging. Tauri
deterministically zeroes AppImage type-magic bytes 8 through 10 in the linuxdeploy launcher before
execution. Package preparation applies that reviewed transform itself, verifies the derived
executable digest shown above, and stores only the derived identity; Tauri's identical transform is
then idempotent and cache reuse remains exact.

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
5. Desktop MCP readiness requires both the safely installed CLI sibling and successful bounded
   capability-contract validation; availability does not mean a server is currently running.
6. Installed package formats expose the durable CLI directly. AppImage client JSON instead invokes
   the stable outer image through the hidden typed stdio-MCP dispatch before Tauri startup.
7. The Linux package lane runs the existing initialize/list/call/EOF MCP smoke through the produced
   AppImage with extract-and-run, verifies the transport is hidden from normal help, and verifies
   it fails without a valid AppImage context.
8. Linux package preparation admits and verifies the five exact bundler-tool identities above
   before Tauri executes them; Tauri's network fallback cannot run because every expected cache
   path already exists.
9. Native-package inspection must confirm all three sibling programs and both exact font notices.
10. Pull-request verification checks out and labels the exact head commit, never the synthetic merge
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
- Moving or replacing an AppImage after generating client JSON invalidates that command. Reopen the
  desktop from its durable location and regenerate the instruction; no temporary mount path is
  persisted.

## Sources

| Source | Applied evidence | Reviewed |
| --- | --- | --- |
| [Tauri sidecars](https://v2.tauri.app/develop/sidecar/) | Supported `externalBin` contract, target-triple filenames, and staging-script pattern | 2026-07-28 |
| [Tauri AppImage distribution](https://v2.tauri.app/distribute/appimage/) | AppImage is a portable, non-installed Linux artifact containing its bundled files | 2026-07-28 |
| [Tauri 2.11.4 Linux bundler source](https://github.com/tauri-apps/tauri/blob/8909f221d1515955fc843808032bdc5d62209c96/crates/tauri-bundler/src/bundle/linux/appimage/linuxdeploy.rs) | Five cache names, mutable default URLs, execution order, and linuxdeploy header mutation | 2026-07-28 |
| [Tauri 2.11.4 download helper](https://github.com/tauri-apps/tauri/blob/8909f221d1515955fc843808032bdc5d62209c96/crates/tauri-bundler/src/utils/http_utils.rs) | Default Linux tool acquisition performs a TLS download without invoking the available hash verifier | 2026-07-28 |
| [Tauri local-tools directory](https://github.com/tauri-apps/tauri/blob/8909f221d1515955fc843808032bdc5d62209c96/crates/tauri-cli/src/interface/mod.rs#L77-L83) | `useLocalToolsDir` resolves package tools below Cargo's target directory | 2026-07-28 |
| [Pinned GTK plugin commit](https://github.com/tauri-apps/linuxdeploy-plugin-gtk/tree/b5eb8d05b4c0ed40107fe2158c5d8527f94568ef) | Immutable GTK plugin script identity | 2026-07-28 |
| [Pinned GStreamer plugin commit](https://github.com/tauri-apps/linuxdeploy-plugin-gstreamer/tree/2a2e67491c32995a3f279ad0ecbe77abd512b42a) | Immutable GStreamer plugin script identity | 2026-07-28 |
| [AppImage output plugin release](https://github.com/linuxdeploy/linuxdeploy-plugin-appimage/releases/tag/continuous) | Reviewed release asset identity, size, and GitHub-published digest | 2026-07-28 |
| [AppImage runtime overview](https://docs.appimage.org/introduction/software-overview.html) | Temporary payload mount and cleanup after payload exit | 2026-07-28 |
| [AppImage runtime environment](https://docs.appimage.org/packaging-guide/environment-variables.html) | Stable resolved `APPIMAGE` path and temporary `APPDIR` mount path | 2026-07-28 |
| [AppImage Type-2 runtime source](https://github.com/AppImage/type2-runtime/blob/75849dce7cc37e4319b633df1f116ca895c71a12/src/runtime/runtime.c) | Normal runtime replaces itself with the mounted `AppRun`; extract-and-run uses a wrapper process | 2026-07-28 |
| [Tauri AppImage bundler source](https://github.com/tauri-apps/tauri/blob/872428fe910efe25eeaa959b56adcd9d9a9a2157/crates/tauri-bundler/src/bundle/linux/appimage/linuxdeploy.rs) | Current AppRun selection and AppImage construction path | 2026-07-28 |
| [Rust Unix `CommandExt::exec`](https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html#tymethod.exec) | Process replacement without fork and inherited stdio | 2026-07-28 |
| [Tauri configuration reference](https://v2.tauri.app/reference/config/#bundleconfig) | Bundle resources and external-program configuration | 2026-07-28 |
| [Tauri CLI reference](https://v2.tauri.app/reference/cli/) | Ordered `--config` overlays for build flavors | 2026-07-28 |
| [Tauri path API](https://v2.tauri.app/reference/javascript/api/namespacepath/) | Operating-system mapping for the application-local data directory | 2026-07-28 |
| [Official Tauri GitHub Action](https://github.com/tauri-apps/tauri-action) | `args`, `projectPath`, and relative `--config` path behavior | 2026-07-28 |
| [Official checkout action](https://github.com/actions/checkout) | Pull-request head-SHA checkout pattern | 2026-07-28 |
| [Geist license](https://github.com/vercel/geist-font/blob/main/LICENSE.txt) | Upstream Geist-family OFL 1.1 basis | 2026-07-28 |
| [Fontsource Geist 5.3.0](https://www.npmjs.com/package/@fontsource/geist/v/5.3.0?activeTab=code) | Resolved Geist package identity and copyright notice | 2026-07-28 |
| [Fontsource Geist Mono 5.3.0](https://www.npmjs.com/package/@fontsource/geist-mono/v/5.3.0?activeTab=code) | Distinct resolved Geist Mono copyright notice | 2026-07-28 |
| [SIL OFL FAQ](https://openfontlicense.org/ofl-faq/) | Redistribution notice and license obligations | 2026-07-28 |
| [Tauri issue 15134](https://github.com/tauri-apps/tauri/issues/15134) | External-binary cache and NSIS reinstall caveat | 2026-07-28 |
| [Tauri issue 11992](https://github.com/tauri-apps/tauri/issues/11992) | macOS external-binary signing/notarization caveat | 2026-07-28 |
