# Market Squawk developer workflow implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this
> plan task-by-task in the existing feature worktree. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Provide one cross-platform `just setup` / `just dev` workflow that prepares and runs the
complete source-built Market Squawk desktop against isolated development data.

**Architecture:** A root `justfile` is a thin command router over the existing Cargo, pnpm, uv, and
Tauri authorities. The desktop gains only the debug-only sibling MCP-relay resolution needed by a
source build; release builds retain installer-manifest authority.

**Tech Stack:** just 1.57.0, Rust 1.97.1, Node.js 24.18.0, pnpm 10.31.0, uv 0.12.1, CPython 3.14.6,
Tauri 2.11, Vite 8.

## Global constraints

- Approved design: `docs/superpowers/specs/2026-08-03-market-squawk-developer-workflow-design.md`.
- Audit base: `68f58d99a0f078d74222cf4ba2c4e50554414e6c`. This is an audit anchor, not release approval.
- Execute in `.worktrees/v1-installed-product-experience` on
  `feature/v1-installed-product-experience`; create no branch, worktree, or subagent lane.
- Preserve the concurrent uncommitted Python release-integrity change. Stage and commit only files
  owned by this plan until the integration owner reconciles the whole feature worktree.
- Keep the command layer cross-platform and thin. Add no command-wrapper test, prose test, snapshot,
  generated fixture, task-runner crate, or second process supervisor.
- Normal developer builds remain incremental. Release and repository approval gates keep their
  existing `CARGO_INCREMENTAL=0` authority.
- The local Cargo target was `18,889,132 KiB` at the audit base. Do not run a broad Cargo gate until
  generated output is safely reclaimed below the 20 GiB ceiling.

---

### Task 1: Root developer command contract

**Files:**
- Create: `justfile`
- Modify: `apps/market-squawk-desktop/package.json`
- Create: `apps/market-squawk-desktop/.npmrc`
- Modify: `python/pyproject.toml`

**Interfaces:**
- Consumes: existing Cargo workspace, desktop package scripts, Python hashed requirements, and
  Tauri development command.
- Produces: the exact command surface in the approved design.

- [ ] **Step 1: Prove the command surface is absent**

  Run the exact downloaded just 1.57.0 binary with `--list`. Expected: failure because no
  `justfile` exists at the repository root.

- [ ] **Step 2: Add the thin cross-platform `justfile`**

  Use `justfile_directory()` and `join()` for absolute paths. Implement only the approved recipes.
  Build every required sibling before `tauri dev`; pass the absolute ignored development data root
  after Tauri's application-argument separator. Use Node.js only for the confirmed development-data
  reset because it is already a required cross-platform input.

- [ ] **Step 3: Enforce existing frontend and Python tool pins**

  Add exact Node.js/pnpm engines and project-local strict engine enforcement to the desktop package
  without changing dependency resolution. Add uv's exact `required-version` and managed-Python
  preference to the existing Python project without adding `uv.lock` or changing the sealed
  requirements.

- [ ] **Step 4: Verify command parsing and expansion**

  Run:

  ```text
  just --fmt --check
  just --list
  just --dry-run setup
  just --dry-run dev
  just --dry-run dev-service
  just --dry-run check
  just --dry-run test
  just --dry-run test-package market-squawk-domain
  just --dry-run test-all
  just --dry-run build
  ```

  Confirm every path is absolute where product data or executables require it, no recipe uses an
  OS-specific shell command, and `dev-web` is described as frontend-only.

### Task 2: Debug-only MCP relay discovery

**Files:**
- Modify: `apps/market-squawk-desktop/src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: installer-owned `ProgramName::McpRelay` snapshots and the existing
  `McpClientRegistrationManager` executable verification.
- Produces: `mcp_relay_program`, which selects the installed manifest-owned relay first and permits
  an exact sibling path only in debug-assertion builds.

- [ ] **Step 1: Add one focused failing test in the existing module**

  Add a single test proving that an installed relay wins and that a debug build selects the relay
  beside the running desktop when no installed relay exists. Run only that existing-module test and
  observe failure because the selector does not exist.

- [ ] **Step 2: Implement the selector and wire desktop composition**

  Resolve the installer snapshot first. Under `cfg(debug_assertions)` only, derive
  `market-squawk-mcp-relay{EXE_SUFFIX}` from the current desktop executable's parent. Under
  `cfg(not(debug_assertions))`, missing installed authority remains an error. Pass the selected path
  into `DesktopMcpClientState::try_new`, retaining its stable regular executable validation.

- [ ] **Step 3: Run focused verification**

  Run the exact selector test, desktop library check, rustfmt check, frontend typecheck, and
  `git diff --check`. If the existing shared feature tree has unrelated compilation failures,
  report the exact earlier failure without weakening the selector or adding an allowance.

### Task 3: Developer documentation and integration evidence

**Files:**
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`
- Modify: `docs/operations/installation-and-bootstrap.md`

**Interfaces:**
- Consumes: the verified recipe names and exact tool versions.
- Produces: one source-development quick path and links to the detailed operations runbook.

- [ ] **Step 1: Replace the manual development startup sequence**

  Document exact just 1.57.0 installation through Cargo, then `just setup` and `just dev`. Retain a
  short manual troubleshooting path by reference rather than making it the primary workflow.

- [ ] **Step 2: Document lifecycle and isolation truthfully**

  State that Vite-only mode is not the full product, development data is isolated under the ignored
  repository root, and the one shared service may remain available to CLI/MCP clients after the
  desktop exits.

- [ ] **Step 3: Verify and commit the bounded change**

  Re-run `just --fmt --check`, `just --list`, dry-run the public recipes, run the focused affected
  checks that fit below the disk ceiling, and inspect `git diff --check`. Stage only this plan's
  files and commit them on the existing feature branch. Do not push while unrelated uncommitted
  Python work keeps the feature worktree dirty; the integration owner pushes once that work is
  reconciled and the branch is clean.
