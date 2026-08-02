# Obsidian Signal Desktop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement
> this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the approved Obsidian Signal interface as Market Squawk's Tauri 2 desktop
application, with the existing protected browser and CLI paths preserved over the same Rust
authorities.

**Architecture:** Add one React/Vite application at `apps/market-squawk-desktop` and one Tauri Rust
crate at `apps/market-squawk-desktop/src-tauri`. The desktop crate owns window lifecycle and a
least-privilege presentation bridge; it delegates all business work to `LocalProduct`,
`Application`, and the existing provider-onboarding authority. The frontend owns rendering,
navigation, accessible interaction, and transient form state only. A shared frontend transport
interface keeps native IPC separate from the protected browser transport.

**Tech Stack:** Tauri 2, React 19, TypeScript, Vite, Tailwind CSS 4, shadcn/ui New York v4, Radix
primitives, Lucide, Geist, cmdk, Zod, pnpm, Vitest, and the existing Rust application services.

## Global Constraints

- Audit base: `dbc909eeb1ca334ae114947158a875fdda3d27d8`.
- Refresh before integration if the release branch moves; recheck application composition,
  provider onboarding, root manifests, CI, and installer ownership.
- Use one branch and worktree for this cohesive product feature:
  `feature/obsidian-signal-desktop` in `.worktrees/obsidian-signal-desktop`.
- Do not create component-level branches, worktrees, review loops, screenshot tests, golden files,
  prose tests, generated-source tests, or a new Rust integration-test executable.
- Keep no more than one frontend behavioral test file. It may cover only navigation accessibility,
  authority-derived readiness, and fail-closed mutation behavior.
- Use maintained upstream libraries for windowing, accessible primitives, icons, fonts, command
  discovery, validation, and charts. Do not add a dependency until the implemented screen uses it.
- Bundle every UI asset. No remote font, script, image, telemetry, or hidden UI request is allowed.
- Do not expose shell, filesystem, SQL, credential-read, arbitrary network, model-loading, or
  execution-bypass authority to the WebView.
- Keep the root `target` and the worktree's independent `target` below the recorded 20 GiB ceiling.
- Run focused checks while implementing. Run broad exact-head gates once at the delivery checkpoint.

---

## Task 1: Create the supported Tauri 2 application boundary

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `apps/market-squawk-desktop/package.json`
- Create: `apps/market-squawk-desktop/pnpm-lock.yaml`
- Create: `apps/market-squawk-desktop/index.html`
- Create: `apps/market-squawk-desktop/tsconfig.json`
- Create: `apps/market-squawk-desktop/tsconfig.app.json`
- Create: `apps/market-squawk-desktop/tsconfig.node.json`
- Create: `apps/market-squawk-desktop/vite.config.ts`
- Create: `apps/market-squawk-desktop/components.json`
- Create: `apps/market-squawk-desktop/src-tauri/Cargo.toml`
- Create: `apps/market-squawk-desktop/src-tauri/build.rs`
- Create: `apps/market-squawk-desktop/src-tauri/tauri.conf.json`
- Create: `apps/market-squawk-desktop/src-tauri/capabilities/main.json`
- Create: `apps/market-squawk-desktop/src-tauri/src/main.rs`
- Create: `apps/market-squawk-desktop/src-tauri/src/lib.rs`

- [ ] Replace the broad `apps/*` Cargo member with explicit application members and add
      `apps/market-squawk-desktop/src-tauri`, so the nested Tauri crate uses the single committed
      root lockfile.
- [ ] Initialize the frontend with exact locked stable dependencies. Generate only shadcn
      primitives used by this implementation.
- [ ] Configure Vite for a fixed local development port, Tauri's `TAURI_DEV_HOST`, relative bundled
      assets, and `src-tauri` watch exclusion.
- [ ] Configure one main window, bundled `dist`, strict CSP, no updater/telemetry/remote content,
      and a product identifier owned by Market Squawk.
- [ ] Register custom commands through a Tauri app manifest and grant only those commands to the
      main window capability.
- [ ] Keep `main.rs` as a one-line entry into the library-owned builder:

```rust
fn main() {
    market_squawk_desktop::run();
}
```

- [ ] Verify the empty boundary before feature code:

```bash
pnpm --dir apps/market-squawk-desktop install --frozen-lockfile
pnpm --dir apps/market-squawk-desktop build
cargo check -p market-squawk-desktop --locked
```

## Task 2: Add the narrow Rust presentation bridge

**Files:**

- Modify: `apps/market-squawk/src/local_product/mod.rs`
- Modify: `apps/market-squawk/src/local_product/cli_provider.rs`
- Create: `apps/market-squawk-desktop/src-tauri/src/bridge.rs`
- Create: `apps/market-squawk-desktop/src-tauri/src/contracts.rs`
- Modify: `apps/market-squawk-desktop/src-tauri/src/lib.rs`
- Modify: `apps/market-squawk-desktop/src-tauri/capabilities/main.json`

- [ ] Retain the already-composed `ProviderPortalActivationAuthority` in `LocalProduct` and expose
      it as an opaque cloned trait object. Do not make its implementation or durable state public.
- [ ] Construct one `DesktopState` from normal `AppConfig` precedence and `LocalProduct::try_new`.
      Initialization failure must prevent the desktop from presenting itself as ready.
- [ ] Add a read-only `desktop_bootstrap` command returning only typed, secret-free facts:

```rust
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DesktopBootstrap {
    pub application_version: &'static str,
    pub data_root_ready: bool,
    pub paper_mode_enabled: bool,
    pub model_runtime: Readiness,
    pub mcp: Readiness,
    pub provider_profiles: serde_json::Value,
    pub provider_sessions: serde_json::Value,
}
```

- [ ] Add `application_invoke` for the existing closed `Application` registry. Bound operation
      length, JSON depth/bytes, request deadline, result bytes/items, and request identity before
      calling `Application::invoke`. Serialize `TypedToolResult` into data plus metadata; map
      `ServiceError` to a small redacted error code.
- [ ] Add a closed `provider_onboarding` command whose payload is a tagged enum covering the same
      operations as the protected portal: start/resume, secret fallback lock/unlock, secret import,
      activate, renew, cleanup, and cancel. Each variant delegates to the existing onboarding or
      activation authority and carries a request-owned cancellation token.
- [ ] Add `open_official_url` through the official Tauri opener plugin with exact code-owned
      provider URL scope; never accept an arbitrary URL from frontend state.
- [ ] On window exit, synchronously stop admission and complete bounded `Application` shutdown.
- [ ] Add one focused unit test inside `bridge.rs` only if an authorization/limit function contains
      new branch behavior. Do not create a test target for DTO serialization or Tauri boilerplate.
- [ ] Verify:

```bash
cargo fmt --all --check
cargo clippy -p market-squawk-desktop -p market-squawk --all-targets --locked -- -D warnings
cargo test -p market-squawk --lib --locked
```

## Task 3: Build the permanent Obsidian Signal shell

**Files:**

- Create: `apps/market-squawk-desktop/src/main.tsx`
- Create: `apps/market-squawk-desktop/src/app/app.tsx`
- Create: `apps/market-squawk-desktop/src/app/routes.tsx`
- Create: `apps/market-squawk-desktop/src/components/app-sidebar.tsx`
- Create: `apps/market-squawk-desktop/src/components/app-header.tsx`
- Create: `apps/market-squawk-desktop/src/components/status-rail.tsx`
- Create: `apps/market-squawk-desktop/src/components/squawk-signal.tsx`
- Create: `apps/market-squawk-desktop/src/components/domain-page.tsx`
- Create: `apps/market-squawk-desktop/src/components/ui/*`
- Create: `apps/market-squawk-desktop/src/lib/navigation.ts`
- Create: `apps/market-squawk-desktop/src/lib/schemas.ts`
- Create: `apps/market-squawk-desktop/src/lib/transport.ts`
- Create: `apps/market-squawk-desktop/src/lib/tauri-transport.ts`
- Create: `apps/market-squawk-desktop/src/styles/globals.css`

- [ ] Generate and adapt the shadcn New York v4 sidebar, button, separator, tooltip, collapsible,
      dialog, input, label, progress, alert, and command components actually used.
- [ ] Implement the exact permanent navigation and ordering from the approved specification.
      Disabled routes remain keyboard-reachable and state why their prerequisite is missing.
- [ ] Implement the approved desktop frame, header, status rail, signal motif, typography, palette,
      compact sidebar, responsive drawer, focus states, reduced motion, zoom/reflow, and semantic
      landmarks.
- [ ] Define `ProductTransport` and validate every native response with Zod at the presentation
      boundary:

```ts
export interface ProductTransport {
  bootstrap(signal?: AbortSignal): Promise<DesktopBootstrap>
  invoke(request: ApplicationRequest, signal?: AbortSignal): Promise<ApplicationResult>
  onboard(request: ProviderOnboardingRequest, signal?: AbortSignal): Promise<ProviderResult>
  openOfficialProviderPage(providerId: string): Promise<void>
}
```

- [ ] Implement Overview plus honest domain pages for every permanent route. Domain pages call only
      mapped read operations and show loading, empty, blocked, stale, and error states instead of
      sample data.
- [ ] Use cmdk only for route and admitted-command discovery. It must not submit orders, import
      secrets, or bypass confirmations.

## Task 4: Move guided setup into the permanent shell

**Files:**

- Create: `apps/market-squawk-desktop/src/components/setup/setup-overview.tsx`
- Create: `apps/market-squawk-desktop/src/components/setup/setup-flow.tsx`
- Create: `apps/market-squawk-desktop/src/components/setup/provider-step.tsx`
- Create: `apps/market-squawk-desktop/src/components/setup/verification-panel.tsx`
- Create: `apps/market-squawk-desktop/src/components/setup/credential-field.tsx`
- Create: `apps/market-squawk-desktop/src/lib/setup-state.ts`
- Create: `apps/market-squawk-desktop/src/test/app.test.tsx`
- Create: `apps/market-squawk-desktop/src/test/setup.ts`
- Modify: `apps/market-squawk/assets/provider-onboarding/index.html`
- Modify: `apps/market-squawk/assets/provider-onboarding/portal.css`
- Modify: `apps/market-squawk/assets/provider-onboarding/portal.js`

- [ ] Reproduce the approved welcome screen from the tracked baseline using real bootstrap facts.
      Never display `Verified`, `Ready`, a Python version, signing state, or provider activation
      unless the corresponding Rust authority returned that fact.
- [ ] Implement the recommended eight-step flow: system, storage, free sources, research/modeling,
      portfolio, paper execution, MCP, and review. Explain each step in plain language and expose
      advanced settings without moving them into the primary path.
- [ ] Drive provider screens from real profiles and durable sessions. Keep credentials in a
      write-only field, clear the value immediately after submission, and never place it in logs,
      URLs, browser storage, diagnostics, or React query caches.
- [ ] Derive resume position and completion from durable backend authorities rather than a second
      frontend completion ledger.
- [ ] Restyle the protected browser fallback to the same tokens, hierarchy, copy, and accessible
      controls while preserving its current security transport and framework-free build
      independence.
- [ ] Write one frontend test file containing only these three failure-sensitive behaviors:
      accessible permanent navigation; backend-blocked readiness never renders as ready; a failed
      confirmed mutation never advances setup or retains credential text.
- [ ] Run each test red before implementation, then green:

```bash
pnpm --dir apps/market-squawk-desktop test --run
pnpm --dir apps/market-squawk-desktop typecheck
pnpm --dir apps/market-squawk-desktop build
```

## Task 5: Package, verify, and integrate the desktop product

**Files:**

- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/verify.sh`
- Modify: `README.md`
- Modify: `docs/operations/installation-and-bootstrap.md`
- Modify: `docs/reference/cli.md`
- Modify: `docs/architecture/deployment.md`
- Modify: `docs/plans/delivery-ledger.md`
- Modify: `docs/project-memory.md`

- [ ] Add affected-path CI classification so documentation-only changes skip compiler/frontend
      builds while policy checks still run. Desktop changes must run pnpm lock/install, typecheck,
      the one test file, Vite build, Tauri compile, and platform packaging prerequisites.
- [ ] Add desktop verification to the release gate without making Node or a WebView mandatory for
      CLI/headless-only use.
- [ ] Build and manually inspect the native window at the 1,375 × 806 reference viewport, compact
      sidebar width, narrow drawer width, 200% text zoom, keyboard-only flow, and reduced motion.
- [ ] Confirm the protected browser portal and CLI setup still work against the same services.
- [ ] Build platform bundles on the supported matrix and record facts without claiming signing
      until signed release evidence exists.
- [ ] Check generated storage before and after the focused gates; clean only completed build state
      if the worktree approaches the 20 GiB ceiling.
- [ ] Update the maintained docs and ledger with implemented behavior and remaining release
      evidence only—no fictional readiness and no implementation diary.
- [ ] Run the exact-head checkpoint once:

```bash
pnpm --dir apps/market-squawk-desktop install --frozen-lockfile
pnpm --dir apps/market-squawk-desktop typecheck
pnpm --dir apps/market-squawk-desktop test --run
pnpm --dir apps/market-squawk-desktop build
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
./scripts/verify.sh
```

- [ ] Commit and push the unchanged verified head, update issue `#36` and Project 5 with exact
      evidence, request grouped Quarter 4 review, and integrate only after all substantiated review
      findings close.
- [ ] After integration, remove the clean feature worktree, prune worktree metadata, delete the
      merged local/origin feature branch, and confirm root/worktree target sizes.

## Plan self-review

- The plan preserves the approved Tauri-default, browser-fallback, and CLI/headless presentation
  modes without introducing a second business authority.
- Every desktop command is closed, bounded, typed, and mapped to an existing Rust service.
- Every sample success state in the visual baseline is replaced by authority-derived state.
- The only new behavioral test surface is one frontend file with three critical failure cases;
  no screenshot, snapshot, prose, generator, or redundant Rust integration tests are planned.
- Shared manifests, lockfiles, CI, application composition, provider activation, and release docs
  are owned by this single serialized lane.
- No placeholder implementation, unowned shared file, or type/interface contradiction remains in
  the task ordering.
