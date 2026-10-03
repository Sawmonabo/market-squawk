# Market Squawk developer workflow design

Status: Approved for implementation

Document type: Developer-experience design

Audience: Market Squawk contributors

Audit base: `68f58d99a0f078d74222cf4ba2c4e50554414e6c`

Last substantive review: 2026-08-03

## Purpose

Provide one memorable, cross-platform developer entry point for preparing and running the complete
Market Squawk desktop product from source. The developer workflow must not make the React frontend
appear to own the Rust service, MCP, modeling worker, or product lifecycle.

## Decision

Market Squawk uses exact `just` 1.57.0 as its root command runner. The `justfile` remains a thin
router over the repository's existing Cargo, pnpm, uv, Tauri, and verification authorities. It does
not reproduce product logic, package logic, or release gates.

The normal source workflow is:

```text
cargo install just --version 1.57.0 --locked
just setup
just dev
```

`just setup` prepares the frozen frontend and Python development dependencies and fetches the
locked Rust dependency graph. It consumes the existing Rust toolchain, package manager, Python
requirements, and wheelhouse authorities. It does not install operating-system packages or alter
global shell configuration.

`just dev` incrementally builds the CLI, service, MCP relay, capture helper, and ONNX worker as
sibling programs before launching `tauri dev`. The desktop receives an absolute, ignored
repository-local development data root. Tauri continues to own Vite hot reload and connects to the
one shared service; the frontend is never launched as if it were the whole product.

The service is intentionally shared and may outlive the desktop, matching the V1 runtime design for
Desktop, CLI, Claude Code, and Codex concurrency. The developer data root is separate from installed
product data. Resetting it requires an interactive confirmation and is never part of normal startup.

## Command surface

| Command | Contract |
| --- | --- |
| `just setup` | Prepare frozen Rust, frontend, and Python development inputs. |
| `just dev` | Build required sibling programs and run the complete Tauri desktop against isolated development data. |
| `just dev-service` | Run the shared service in the foreground for service-focused work. |
| `just dev-web` | Run Vite only; explicitly diagnostic and not the complete product. |
| `just doctor` | Report the exact active toolchain and Tauri host-prerequisite state without changing it. |
| `just check` | Run formatting, Rust compilation checks, and frontend type checking. |
| `just test` | Run the existing critical application, frontend, and Python tests. |
| `just test-package <crate>` | Run one focused Rust package test suite. |
| `just test-all` | Run the existing complete deterministic Rust, frontend, and Python suites. |
| `just build` | Produce a local production-profile Rust build and compiled frontend without publishing or packaging. |
| `just reset-dev` | With confirmation, remove only the ignored development data root. |

## Cross-platform rules

The command surface supports Windows, macOS, and Linux. Recipes invoke portable executables
directly and use `just` path functions rather than shell-derived repository paths. Unix-only
commands, ambient `sh` assumptions, and duplicated Bash/PowerShell orchestration are prohibited.
The only destructive recipe delegates the recursive removal to the already-required Node.js
runtime and requires `just`'s interactive confirmation.

Rust `1.97.1`, Node.js `24.18.0`, pnpm `10.31.0`, uv `0.12.1`, and managed CPython `3.14.6` remain
exact development inputs. The Rust toolchain file, pnpm package metadata with project-local strict
engine enforcement, uv project configuration, and Python requirement hashes enforce their
respective boundaries.

## Debug and release boundary

Development Tauri binaries are not installed release generations. A debug-only desktop seam may
resolve the prebuilt MCP relay from the running executable's sibling directory. The existing MCP
registration manager must still validate that file as an absolute, stable, owner-protected regular
executable before use.

Non-debug builds continue to require the installer-owned MCP relay snapshot. No debug fallback,
repository path, environment override, or caller-supplied executable may enter a release build.

## Failure behavior

- Missing or incompatible tools stop the command that needs them; `just doctor` shows the active
  versions and Tauri host diagnosis.
- Frozen dependency mismatch stops `just setup`; it never rewrites a lock automatically.
- Failure to build any required sibling prevents Tauri startup.
- Service, MCP, or desktop startup errors remain visible product errors; the command runner does not
  hide them, increase timeouts, or substitute mocks.
- `just dev-web` is labelled frontend-only so it cannot be mistaken for product readiness.

## Verification

No prose or wrapper-command snapshot test is added. Verification exercises the real command file
through `just --fmt --check`, recipe enumeration, dry-run expansion, frozen dependency setup where
the exact tools are available, affected Rust/frontend checks, and a full desktop start after the
independent installed-service credential repair is integrated.

## Sources

- [`just` repository, installation, Windows shells, and platform support](https://github.com/casey/just), reviewed 2026-08-03.
- [`just` 1.57.0 immutable release](https://github.com/casey/just/releases/tag/1.57.0), reviewed 2026-08-03.
- [Tauri development command](https://v2.tauri.app/develop/), reviewed 2026-08-03.
- [Tauri Vite integration](https://v2.tauri.app/start/frontend/vite/), reviewed 2026-08-03.
- [uv configuration and `required-version`](https://docs.astral.sh/uv/reference/settings/), reviewed 2026-08-03.
