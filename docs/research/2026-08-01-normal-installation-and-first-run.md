# Normal installation and first-run experience

Purpose: define the evidence-backed, ordinary-user installation and first-run experience for the
complete self-hosted Market Squawk product.

| Metadata | Value |
| --- | --- |
| Document type | Product and release decision input |
| Audience | Product owners, designers, release engineers, security reviewers, maintainers |
| Status | Recommended design; implementation evidence still required |
| Research date | 2026-08-01 |
| Repository audit anchor | `f35d67247c93c3ab253aedbf663f6cb4c1f80b3e` |
| Refresh gate | Recheck platform rules, maintained-project behavior, and exact release assets at the frozen release candidate |

## Table of Contents

- [Decision](#decision)
- [Research coverage](#research-coverage)
- [What normal means](#what-normal-means)
- [Recommended user journey](#recommended-user-journey)
- [Installation and onboarding boundary](#installation-and-onboarding-boundary)
- [Distribution-channel roles](#distribution-channel-roles)
- [Platform decisions](#platform-decisions)
- [Update, repair, rollback, and removal](#update-repair-rollback-and-removal)
- [Market Squawk fit](#market-squawk-fit)
- [Release acceptance](#release-acceptance)
- [Evidence limits and open decisions](#evidence-limits-and-open-decisions)
- [Primary sources](#primary-sources)
- [Related documentation](#related-documentation)

## Decision

Market Squawk's ordinary installation should behave like a modern desktop application, not like a
developer project:

1. The website or platform catalog presents one obvious native download for the detected operating
   system and architecture.
2. The native installer places one complete, launchable product on the machine with safe per-user
   defaults and very few questions.
3. The installed product includes every required Market Squawk component. The user does not install
   Rust, Node.js, Python, uv, a database, or a container runtime separately.
4. The installer verifies the closed release before activation and registers the normal operating-
   system launcher, application identity, and removal entry.
5. First launch creates or validates the private workspace and opens the permanent Market Squawk
   application shell.
6. The first screen offers one understandable, useful outcome. Advanced configuration remains a
   visible, resumable checklist.
7. Updates, repair, rollback, uninstall, and explicit data purge are parts of the same product
   lifecycle.

The central distinction is:

> **Installation is complete by default; configuration is staged by purpose.**

The application, CLI, MCP server, managed Python 3.14 environment, uv, analytics dependencies,
modeling runtime, any model assets required by the advertised baseline, helper executables, notices,
and lifecycle tooling belong to the complete installed product. Provider credentials, portfolio
imports, additional datasets or models, external MCP-client configuration, and execution authority
are user-specific configuration and should be introduced when their purpose is clear.

## Research coverage

The research evaluated 120 candidates and selected 108 current evidence items for deep review:

| Evidence class | Reviewed | Purpose |
| --- | ---: | --- |
| Maintained GitHub repositories | 35 | Observe what comparable products actually ship |
| Official platform and tool documentation | 28 | Establish packaging, signing, update, runtime, and accessibility constraints |
| Academic and industry research papers | 22 | Evaluate onboarding, warnings, trust, updates, defaults, and recovery behavior |
| Reputable operational and security guidance | 23 | Validate secure deployment, supply-chain, usability, and lifecycle practices |

The repository sample intentionally spans five related groups:

| Group | Maintained projects reviewed |
| --- | --- |
| Stateful desktop applications | [GitHub Desktop](https://github.com/desktop/desktop), [AppFlowy](https://github.com/AppFlowy-IO/AppFlowy), [Logseq](https://github.com/logseq/logseq), [Standard Notes](https://github.com/standardnotes/app), [Cryptomator](https://github.com/cryptomator/cryptomator), [Actual Budget](https://github.com/actualbudget/actual), [AFFiNE](https://github.com/toeverything/AFFiNE), [GitButler](https://github.com/gitbutlerapp/gitbutler), [Bruno](https://github.com/usebruno/bruno), [KeePassXC](https://github.com/keepassxreboot/keepassxc) |
| Self-hosted data applications | [Ente](https://github.com/ente-io/ente), [Immich](https://github.com/immich-app/immich), [Paperless-ngx](https://github.com/paperless-ngx/paperless-ngx), [Syncthing](https://github.com/syncthing/syncthing) |
| Local AI and development runtimes | [Ollama](https://github.com/ollama/ollama), [Jan](https://github.com/janhq/jan), [AnythingLLM](https://github.com/Mintplex-Labs/anything-llm), [Open WebUI](https://github.com/open-webui/open-webui), [Open WebUI Desktop](https://github.com/open-webui/desktop), [Podman Desktop](https://github.com/podman-desktop/podman-desktop), [Rancher Desktop](https://github.com/rancher-sandbox/rancher-desktop) |
| Cross-platform desktop infrastructure | [Tailscale](https://github.com/tailscale/tailscale), [RustDesk](https://github.com/rustdesk/rustdesk), [LocalSend](https://github.com/localsend/localsend), [Joplin](https://github.com/laurent22/joplin), [Mullvad VPN](https://github.com/mullvad/mullvadvpn-app) |
| Installation and toolchain systems | [rustup](https://github.com/rust-lang/rustup), [uv](https://github.com/astral-sh/uv), [mise](https://github.com/jdx/mise), [Pixi](https://github.com/prefix-dev/pixi), [cargo-binstall](https://github.com/cargo-bins/cargo-binstall), [Homebrew](https://github.com/Homebrew/brew), [Atuin](https://github.com/atuinsh/atuin), [Tauri](https://github.com/tauri-apps/tauri), [cargo-dist](https://github.com/axodotdev/cargo-dist) |

No single project supplies Market Squawk's full lifecycle. The recommendation composes the repeated
good patterns while retaining Market Squawk's stricter verification, activation, risk, and data-
ownership requirements.

## What normal means

Across the maintained-project sample and platform guidance, the repeated ordinary-user pattern is:

- **Native acquisition is primary.** Users download or obtain an application for their operating
  system. Terminal and package-manager routes are additive choices.
- **Installation asks little.** Identity, destination only when necessary, disk requirement,
  progress, success, and launch are enough for the default path.
- **The installed product is immediately launchable.** Technical prerequisites do not become a
  scavenger hunt.
- **First run starts with a domain task.** GitButler imports a repository; KeePassXC creates or opens
  a database; Actual starts a budget; Bruno opens a collection. Infrastructure is introduced only
  where it supports the chosen task. ([GitButler guide](https://docs.gitbutler.com/guide),
  [KeePassXC](https://github.com/keepassxreboot/keepassxc),
  [Actual downloads](https://actualbudget.org/download/),
  [Bruno](https://github.com/usebruno/bruno))
- **Advanced setup is resumable.** A checklist is appropriate when work is genuinely optional or
  may span sessions; it should not replace simplifying the default path.
  ([GOV.UK task-list guidance](https://design-system.service.gov.uk/components/task-list/))
- **Permissions and credentials are contextual.** The user sees what an action unlocks, what it
  contacts or changes, and how to reverse it before granting authority.
- **Program and data lifecycles are separate.** Normal application removal does not imply permission
  to destroy user work.

## Recommended user journey

```mermaid
flowchart LR
    Discover["Visit download page or platform catalog"]
    Acquire["Download the detected native package"]
    Install["Install the complete product"]
    Verify["Verify and atomically activate one release"]
    Launch["Launch Market Squawk"]
    Workspace["Create or verify the private workspace"]
    Choose["Begin the complete recommended setup"]
    Value["See real market, research, or imported data"]
    Setup["Resume optional setup from Overview"]
    Lifecycle["Update, repair, rollback, or remove safely"]

    Discover --> Acquire --> Install --> Verify --> Launch
    Launch --> Workspace --> Choose --> Value --> Setup --> Lifecycle
```

### 1. Discovery and acquisition

The main download page should:

- detect the operating system and architecture;
- lead with one **Download Market Squawk** action;
- show version, platform, architecture, download size, publisher or source identity, and expected
  operating-system prompt;
- offer other native formats, checksums, attestations, and the terminal command under secondary
  choices; and
- never advertise an asset or command until the exact published bytes pass clean-machine release
  verification.

### 2. Native installation

The default native installer should:

- install per user without elevation where the operating system permits;
- check supported operating system, architecture, disk space, and required system facilities;
- install the complete release rather than downloading unpinned dependencies at first launch;
- verify the outer artifact, closed component manifest, size and digest identities, provenance,
  helper executables, licenses, managed Python environment, and any model assets required by the
  advertised baseline before
  activation;
- activate one immutable version atomically and preserve the current working version if activation
  fails;
- register the Start menu, Applications folder, or Linux desktop entry and removal integration;
- expose the CLI and MCP executable through product-owned stable entrypoints; and
- finish with **Launch Market Squawk** selected.

The installer should not ask for provider API keys, portfolio files, a model choice, MCP-client
details, execution settings, or financial expertise.

### 3. First launch

First launch should run a fast product-owned readiness check, create or validate the workspace, and
open the permanent Overview page. It should not open a developer console, a browser-only temporary
wizard, or a wall of configuration fields.

The binding V1 interface contract remains one clear welcome outcome: **Set up everything for me**,
with **Review advanced settings** as the subordinate alternative. This research does not silently
replace that approved route. Instead, the complete recommended setup should deliver an early useful
checkpoint—preferably real zero-fee market data—with a prepared offline workspace available when a
network or provider is unavailable. Portfolio or research import remains a plain-language task in
the guided flow.

Changing the welcome page to separate outcome cards such as **Explore free market data**,
**Explore a prepared workspace**, and **Import my data** requires a new product-interface review.
No automated execution, paper bot, external MCP connection, or sensitive permission starts merely
because setup opened. Closing the application preserves completed checkpoints, and reopening resumes
the approved flow.

### 4. Guided setup in the permanent application

Overview should retain a short, resumable checklist covering sources, research storage, portfolio,
models, paper execution, MCP, updates, backup, and recovery. Each task should state:

- what the user gains;
- whether the action contacts an external service;
- any storage or download impact;
- whether an account or credential is required;
- what authority will be granted;
- how to test success; and
- how to disconnect or undo it.

Long flows should be divided into logical steps, indicate progress, preserve entered state, make
optional stages obvious, and allow users to revisit completed steps.
([W3C multi-page forms](https://www.w3.org/WAI/tutorials/forms/multi-page/))

Market Squawk should report evidence-derived states such as **Installed**, **Connected**,
**Data ready**, **Available but stopped**, **Needs attention**, and **Recovery required**. Completing
a page is not evidence that data, a model, MCP, or execution is ready.

### 5. First useful result

The recommended complete-setup path should display real useful data at an early checkpoint instead
of withholding all value until every optional integration is configured. A product target of less
than five minutes from first launch to the first useful result is reasonable, but it is a Market
Squawk acceptance target rather than a threshold established by the reviewed research.

## Installation and onboarding boundary

| Installed as part of the complete product | Configured after launch, when relevant |
| --- | --- |
| Desktop application and UI assets | Provider account or credential activation |
| Market Squawk CLI and MCP server | User-selected datasets beyond the complete baseline |
| Capture and inference helpers | Portfolio and transaction imports |
| Managed CPython 3.14 runtime | User-specific storage choices that depart from safe defaults |
| Pinned uv and locked Python analytics/training environment | External MCP-client registration |
| Native and ONNX inference support | Model or strategy admission for a particular use |
| Model assets required by the advertised baseline and all notices | Paper-operation preferences and limits |
| Lifecycle, doctor, update, repair, rollback, and uninstall authority | Any separately authorized execution configuration |

This boundary keeps the product complete without forcing user-specific decisions into the operating-
system installer.

## Distribution-channel roles

| Channel | Role |
| --- | --- |
| Native package or platform catalog | Primary ordinary-user route; expected launcher, identity, trust prompt, and uninstall behavior |
| Project download page | Detects the platform and points to the exact native asset; also exposes evidence and alternatives |
| `curl … \| sh` | Secondary terminal and headless route; installs the same complete release and lifecycle |
| WinGet, Homebrew, and Linux catalogs | Additional discovery and automation; never a different or reduced product |
| GitHub Releases | Canonical versioned publication, checksums, manifests, SBOMs, and attestations |
| Source build | Developer and recovery route; not the ordinary-user installation |

The terminal bootstrap should be thin. It detects the supported platform, retrieves a versioned
release manifest and complete bundle, verifies the release before executing product code, activates
through the same installer authority, and exits nonzero on any download, verification, or activation
failure. It must not silently create a second target directory, a second updater, or an unmanaged
Python installation. Its stable CLI and desktop entrypoints must resolve to the same active release.

The currently approved simple command can remain the public terminal entrypoint, but release
acceptance must explicitly test missing URLs, HTTP errors, interrupted downloads, unsupported
platforms, corrupt bytes, and a closed terminal. A terminal pipeline must not report success merely
because the shell received no script.

## Platform decisions

### Windows

Microsoft currently recommends Microsoft Store distribution for most desktop applications. Store-
submitted MSIX packages receive Microsoft signing and Store-managed updates; Microsoft also supports
direct MSIX/App Installer and WinGet channels with different signing and hosting responsibilities.
([Microsoft distribution guide](https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/choose-distribution-path),
[MSIX deployment overview](https://learn.microsoft.com/en-us/windows/msix/desktop/managing-your-msix-deployment-overview),
[WinGet](https://learn.microsoft.com/en-us/windows/package-manager/winget/))

Market Squawk should lead with the best admitted native Windows package, register Start menu and
uninstall integration, support noninteractive installation for administrators, and add Store and
WinGet discovery when the exact release assets and publisher path are accepted. The package and the
application lifecycle must not become competing updaters.

### macOS

Apple defines Developer ID signing and notarization as the normal direct-distribution trust path.
Notarization checks Developer ID-signed software and lets Gatekeeper present the recognized result at
first launch. ([Apple notarization](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution),
[Developer ID](https://developer.apple.com/support/developer-id/))

Market Squawk's zero-mandatory-cost constraint means a release without Apple-granted authority must
describe the resulting Gatekeeper experience accurately and provide exact checksums and provenance;
it must not present those as equivalent to Apple publisher recognition. The normal DMG/application
flow, Applications-folder presence, launch behavior, and data-preserving removal still require real
clean-machine evidence.

### Linux

The accepted V1 Linux matrix remains Ubuntu 24.04-compatible x64 with DEB and AppImage packages.
The release should provide native package metadata and desktop integration in addition to a portable
artifact. RPM or another distribution-specific package requires a separate support and clean-machine
evidence decision. Desktop entries must identify the application, icon, executable, categories, and supported links correctly.
Program files and XDG configuration, data, state, cache, and runtime locations remain separate.
([XDG Base Directory specification](https://specifications.freedesktop.org/basedir-spec/latest/),
[Desktop Entry specification](https://specifications.freedesktop.org/desktop-entry-spec/latest/),
[AppImage packaging guide](https://docs.appimage.org/packaging-guide/index.html))

## Update, repair, rollback, and removal

Installation quality includes the entire lifecycle:

1. Check for updates according to a visible user policy after the application has delivered first
   value.
2. Show version, channel, size, material changes, expected interruption, and current progress.
3. Download into a non-active location with bounded resource use.
4. Verify authenticated release identity and freshness before activation.
5. Preflight disk, schema, runtime, and currently active work.
6. Activate atomically at a safe boundary and retain a known-good prior version.
7. Run a post-activation health check; automatically return to the prior version if admission fails.
8. Expose version history, repair, rollback, and actionable errors in the application and CLI.

This follows current secure-deployment guidance: updates require defined phases, testing,
measurement, controlled rollout, recovery, and software-integrity validation.
([CISA safe software deployment](https://www.cisa.gov/sites/default/files/2024-10/safe-software-deployment-how-software-manufacturers-can-ensure-reliability-for-customers-508c.pdf),
[NIST SSDF](https://csrc.nist.gov/pubs/sp/800/218/final),
[TUF specification](https://theupdateframework.github.io/specification/latest/))

Normal uninstall removes program files and native registrations while preserving the Market Squawk
workspace. **Remove Market Squawk and all local data** is a separate action that lists configuration,
credentials, datasets, portfolios, models, logs, and artifacts before explicit confirmation.

## Market Squawk fit

The current repository already contains substantial parts of the hard lifecycle foundation:

- a complete release composition containing desktop, CLI, capture/inference helpers, uv, managed
  CPython 3.14, and the locked Python product;
- closed-manifest verification and immutable activation;
- status, update, repair, rollback, and data-preserving uninstall services;
- a permanent Tauri desktop shell and guided setup surfaces; and
- cross-platform release workflows with native-package and installation evidence steps.

That foundation is stronger than the lifecycle exposed by many sampled repositories. It is not yet
the same as a proven normal-user release. The ordinary installation remains blocked until the exact
public assets and command exist and clean-machine evidence proves:

- the native download and terminal command resolve successfully;
- the correct architecture is selected;
- the application appears in the expected operating-system launcher;
- stable CLI and MCP entrypoints work after opening a new terminal;
- first launch enters the welcoming product path automatically;
- a user without financial or developer knowledge reaches a real first result;
- update, restart, repair, rollback, uninstall, and data preservation work from installed packages;
  and
- Windows, macOS, and Linux trust prompts match the published explanation.

## Release acceptance

The ordinary installation is complete only when the exact frozen release candidate passes the
following on clean supported systems:

- native install without a preinstalled development toolchain or container runtime;
- correct operating-system, architecture, package, identity, version, and manifest selection;
- truthful failure for offline, unavailable, unsupported, interrupted, truncated, corrupt, stale,
  or mismatched inputs;
- operating-system launcher, stable CLI/MCP entrypoints, file locations, and removal registration;
- first launch and workspace creation with no unexplained outbound traffic;
- first useful live, research, sample, or imported result through the recommended path;
- setup skip, resume, replay, keyboard-only operation, and actionable error recovery;
- safe handling of credentials and explicit authority;
- update while idle, deferred update during active work, restart, health validation, and known-good
  rollback;
- repair without changing user data;
- ordinary uninstall with preserved data and separate confirmed purge; and
- parity between the native package and terminal installation lifecycle.

Package construction success, screenshots, focused unit tests, or a manually launched development
build are not substitutes for this installed-product evidence.

## Evidence limits and open decisions

- The research supports the flow and lifecycle properties but does not establish one ideal screen
  count, copy set, visual layout, time-to-value threshold, Linux primary package, or update cadence.
- Most onboarding studies are small or use adjacent product categories. Representative beginner
  usability testing is still required.
- Native package rules and repository maintenance status can change. Refresh at the release
  candidate.
- The Windows Store/SignPath admissions and macOS publisher-authority decision remain governed by
  the separate zero-cost distribution research.
- The exact scope and size of required baseline models and offline sample data require a release-
  bundle decision without weakening the complete-default contract.
- Automatic update checking versus explicit opt-in requires a product decision that preserves no
  hidden outbound requests and remains independent of telemetry.

## Primary sources

- [Apple: Notarizing macOS software before distribution](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution)
- [Microsoft: Choose a distribution path for your Windows app](https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/choose-distribution-path)
- [Microsoft: Code-signing options for Windows applications](https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/code-signing-options)
- [Tauri 2 distribution overview](https://v2.tauri.app/distribute/)
- [Tauri 2 Windows installer](https://v2.tauri.app/distribute/windows-installer/)
- [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
- [CISA safe software deployment](https://www.cisa.gov/sites/default/files/2024-10/safe-software-deployment-how-software-manufacturers-can-ensure-reliability-for-customers-508c.pdf)
- [NIST Secure Software Development Framework](https://csrc.nist.gov/pubs/sp/800/218/final)
- [W3C multi-page forms](https://www.w3.org/WAI/tutorials/forms/multi-page/)
- [GOV.UK complete-multiple-tasks pattern](https://design-system.service.gov.uk/patterns/complete-multiple-tasks/)
- [Astral uv installer](https://docs.astral.sh/uv/reference/installer/)

The tracked [source matrix](2026-08-01-normal-installation-source-matrix.md) preserves every selected
source, evidence class, direct URL, research date, and selection rationale. Category and batch
reports remain working research artifacts rather than the sole durable evidence record.

## Related documentation

- [Cross-platform installation and guided setup](2026-07-28-cross-platform-installation-and-guided-setup.md)
- [Zero-cost macOS and Windows desktop distribution](2026-07-29-zero-cost-desktop-distribution.md)
- [Tauri packaging and installed-runtime boundaries](2026-07-28-tauri-packaging-and-runtime-boundaries.md)
- [Installation and bootstrap operations](../operations/installation-and-bootstrap.md)
- [Configuration and secrets operations](../operations/configuration-and-secrets.md)
- [CLI reference](../reference/cli.md)
