# Installed service and Python package repair plan

> Execution uses the existing `feature/v1-installed-product-experience` worktree and the approved
> subagent-driven workflow. This plan repairs the two exact hosted failures, produces one pushed
> candidate, runs the four-platform installed-product proof, and closes with one grouped review.

**Audit base:** `2b93cdcdc03ab4107aabd93c15ad6c0e59746cc5` is an audit anchor, not
release approval. Execution begins by refreshing paths, interfaces, dependencies, CI failure logs,
and Git state against the actual unchanged feature head.

**Design:**
[`2026-08-03-installed-service-credential-bootstrap-repair-design.md`](../specs/2026-08-03-installed-service-credential-bootstrap-repair-design.md)
and the approved
[`2026-08-01-market-squawk-v1-installed-product-experience-design.md`](../specs/2026-08-01-market-squawk-v1-installed-product-experience-design.md).

## Global constraints

- Preserve the existing `Initializing`/`Active` runtime documents, opaque backend-bound
  `SecretRef` values, central service authority, authenticated normal rendezvous, risk boundary,
  and single active workspace.
- Never put an unlock or runtime/client/provider secret in arguments, environment, configuration,
  general RPC, rendezvous state, logs, artifacts, or a plaintext file.
- The native bootstrap endpoint is short lived and exposes only status, foreground-keyring retry,
  and encrypted-fallback unlock. It is not another application API.
- Use `interprocess = "=2.4.3"` with `tokio`; enable the existing `tokio-util = "=0.7.19"`
  dependency's `codec` feature; use target-specific `security-framework = "=3.7.0"`,
  `win-security-identifier = "=0.2.0"`, and `widestring = "=1.2.1"` only where required. Recheck
  exact latest versions, licenses, and locked transitive impact immediately before editing
  manifests.
- Keep `unsafe_code = "forbid"`. OS FFI remains inside admitted dependencies.
- Extend existing behavioral test harnesses only. Add no prose test, wrapper test, file-existence
  test, duplicate fixture, or new integration-test target.
- Shared manifests, lockfile, service composition, workflow, Python source lock, ledger, commit,
  and push have one writer. Run one Cargo gate at a time with `CARGO_INCREMENTAL=0` and stop at the
  20 GiB worktree-local target ceiling.
- Do not publish a release or merge to `main`/the release branch. The terminal outcome is the
  owner-testable V1 candidate for the user's live E2E period.
- Any source change after exact-head package proof invalidates that proof and the final review.

## Dependency and ownership DAG

| Wave | Lane | Depends on | Exclusive ownership | Focused gate | Merge order |
| --- | --- | --- | --- | --- | --- |
| 0 | Research/design | Current failure evidence | Scratch research; focused design/plan | Source validation and self-review | 1 |
| 1A | Python package repair | Existing PyPA/uv investigation | `scripts/build_python_release.py`, its existing test module | Existing 16-case builder suite and source-admission check | Already integrated locally |
| 1B | Service repair | Wave 0 | Platform secret adapter, bootstrap modules, service/CLI/Desktop/installer integration; one worker owns all shared Rust composition | Existing platform/application/installer/Desktop gates | 2 |
| 2 | Integration/docs | Waves 1A/1B | `Cargo.toml`, `Cargo.lock`, CI/package scripts, maintained docs, ledger, Python source lock | Focused affected gates, formatting, diff check | 3 |
| 3 | Hosted proof | Clean pushed Wave 2 head | No source writes | Four target installed-package jobs | 4 |
| 4 | Quarter 4 grouped review | Successful unchanged Wave 3 head | Read-only reviewers; one remediation owner only if needed | Exact-head evidence and review closure | 5 |

The research writer may run alongside Wave 1B because it writes ignored scratch only. No two
implementation workers edit the same feature worktree concurrently.

## Task 1: Close and persist the decision-grade research

**Files:**

- Read: canonical ignored research workspace under `.agents/tmp/research/`
- Create: `docs/research/2026-08-03-installed-service-credential-bootstrap-and-python-integrity.md`
- Modify: `docs/research/README.md`

1. Complete category synthesis, final technical synthesis, and evidence verification through the
   existing deep-research workflow.
2. Validate the scratch workspace with its supplied validator.
3. Persist one concise maintained report containing only durable findings, direct links, review
   date, chosen dependencies, limitations, and implementation consequences. Do not track the raw
   research workspace or dead ends.
4. Self-review every claim against the cited source and current locked versions.

## Task 2: Keep the completed Python `RECORD` repair closed

**Files already changed and locally committed at the audit base:**

- `scripts/build_python_release.py`
- `scripts/tests/test_build_python_release.py`
- `python/wheelhouse-lock.json`

1. Reconfirm the existing focused suite passes after the service changes settle.
2. Preserve strict direct-child script ownership, normalized containment, type/hash/size checks,
   cross-distribution collision rejection, and final closed-tree reconciliation.
3. Do not refresh the final source closure yet; Task 6 owns the one final refresh after every
   tracked source/document change.

## Task 3: Make OS keyring interaction policy truthful

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/market-squawk-platform/Cargo.toml`
- Modify: `crates/market-squawk-platform/src/secrets/managed.rs`
- Modify: `crates/market-squawk-platform/src/secrets/keyring.rs`
- Modify: `crates/market-squawk-platform/src/secrets/preferred.rs`
- Modify: existing platform secret tests in the current test modules/harnesses

1. Add one failing existing-harness scenario proving a `Forbid` operation either uses a concrete
   no-UI backend path or returns `InteractionRequired` before mutation.
2. Expose only the minimal internal policy inspection required by the adapter.
3. Mark Windows Credential Manager CRUD as non-interactive. Serialize macOS `Forbid` operations
   under the `security-framework` no-interaction guard and restore the process-global state on every
   exit. Keep Linux Secret Service prompt-capable and fail before a prompt path under `Forbid`.
4. Permit an already-ready encrypted fallback to own a **new** planned generation only after the
   primary proves before mutation that it cannot operate non-interactively. Never treat
   indeterminate completion as unavailable; never move an existing backend-bound reference.
5. Run:

   ```bash
   CARGO_INCREMENTAL=0 cargo test -p market-squawk-platform secrets --locked
   CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-platform --all-targets --locked -- -D warnings
   ```

## Task 4: Implement the owner-authenticated bootstrap transport

**Files:**

- Create: `apps/market-squawk/src/service/bootstrap.rs`
- Create: `apps/market-squawk/src/service/bootstrap/unix.rs`
- Create: `apps/market-squawk/src/service/bootstrap/windows.rs`
- Modify: `apps/market-squawk/src/service/mod.rs`
- Modify: `apps/market-squawk/src/service/runtime.rs`
- Modify: `apps/market-squawk/src/bin/market-squawk-service.rs`
- Modify: `apps/market-squawk/src/cli.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: existing control-plane service harness

1. Add one consolidated failing scenario: a locked fallback produces `bootstrap_required`, normal
   rendezvous remains absent, an owner-authenticated unlock resumes the exact initialization, and
   only then does normal readiness publish.
2. Implement the versioned binary protocol and strict limits from the design. Zeroize secret
   buffers on every path and keep response/debug/log data non-secret.
3. On Linux create the socket as `0600` inside the owner control root. On macOS use a prevalidated
   `0700` private parent, then set and verify the node as `0600`. Require bilateral matching
   effective UID. On Windows create a local-only, first-instance named pipe with an exact
   current-logon-SID DACL; authenticate a fixed non-secret preface on the synchronous stream under
   RAII client impersonation and effective-token logon-SID membership, then safely hand the
   authenticated handle to Tokio. Never substitute PID lookup for connection-bound identity.
4. Make service startup hold one installation-global bootstrap guard, attempt exact runtime
   preparation, wait only on typed recoverable credential conditions, retry the unchanged durable
   plan, close the endpoint, compose, self-probe, and publish the normal rendezvous last.
5. Add `service bootstrap` and typed `service start/status` reporting. Unlock input is no-echo TTY
   or explicit bounded stdin, never argv/env.
6. Run:

   ```bash
   CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane service_runtime --locked
   CARGO_INCREMENTAL=0 cargo clippy -p market-squawk -p market-squawk-runtime \
     -p market-squawk-platform --all-targets --locked -- -D warnings
   ```

## Task 5: Wire first-run, installer repair, and installed smoke

**Files:**

- Modify: `apps/market-squawk-desktop/src-tauri/src/service.rs`
- Modify: `apps/market-squawk-desktop/src-tauri/src/lib.rs`
- Modify: `apps/market-squawk-desktop/src-tauri/src/bridge.rs`
- Modify: only the existing setup service-unavailable UI components required for the typed flow
- Modify: `apps/market-squawk-installer/src/service_registration.rs`
- Modify: the existing platform registration adapters only if lifecycle handling changes
- Modify: `scripts/smoke_mcp.py`
- Modify: existing Desktop and installer tests only where the real boundary is already covered

1. Desktop connects normally first. On typed bootstrap state, its Rust layer owns the native client
   and exposes a narrow setup action; the WebView receives no endpoint/path/runtime credential.
2. Installer activation/repair treats `bootstrap_required` as recoverable foreground work, not an
   opaque health failure or package rollback. Invalid package/generation/protocol/health still
   fails closed.
3. Improve `smoke_mcp.py` diagnostics to include bounded redacted service stderr on early exit. For
   the hosted Ubuntu environment, generate a process-local test unlock and send it through the
   installed CLI stdin bootstrap flow; never inject it into service argv/env or artifacts.
4. Run the affected existing frontend, Tauri, installer, and installed-service focused gates once.

## Task 6: Integrate, document, refresh the source closure, and push once

**Files:**

- Modify: maintained architecture/operations/reference pages affected by actual behavior
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Modify: `docs/project-memory.md` only for durable operating decisions
- Modify: `.github/workflows/ci.yml` or package scripts only if the real installed flow requires it
- Modify: `python/wheelhouse-lock.json` last

1. Reconcile all code and research changes; remove scratch/dead-end tracked material; run
   `cargo fmt --all` and `git diff --check`.
2. Update only documentation whose runnable truth changed. Keep mutable progress in the delivery
   ledger, not README prose.
3. Run the Python builder focused suite, then refresh the source closure **last**:

   ```bash
   PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest \
     scripts.tests.test_build_python_release.PythonReleaseBuilderContracts -v
   python3 -I scripts/build_python_release.py \
     --refresh-source-closure --lock python/wheelhouse-lock.json
   PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest \
     scripts.tests.test_build_python_release.PythonReleaseBuilderContracts.test_source_refresh_changes_only_the_complete_source_closure \
     scripts.tests.test_build_python_release.PythonReleaseBuilderContracts.test_repository_source_closure_contains_required_inputs -v
   ```

4. Run the affected Rust/Desktop/installer gates once, inspect target size before and after, and
   require a clean worktree.
5. Commit coherent repair changes, push the exact feature head, update draft PR #43 with the commit,
   outcome, local evidence, target size, and hosted proof still pending.

## Task 7: Run the four-platform installed-product proof

1. Trigger the explicit candidate workflow for the clean pushed head.
2. Require successful sealed Python/native package construction and installed behavior on Ubuntu
   x64, Windows x64, macOS Intel, and macOS Apple Silicon.
3. Each job must bind the same head/tree/source closure/component manifest and prove install,
   bootstrap/readiness, Desktop/CLI/two MCP clients, restart/repair, and data-preserving uninstall.
4. Inspect every failed job log before any change. A source change returns to Task 6 and invalidates
   all prior target evidence.

## Task 8: Conduct the one final grouped Quarter 4 review

1. Freeze the unchanged successful head and prepare one whole-branch review package.
2. Dispatch disjoint read-only reviewers concurrently for:
   - credential/bootstrap/transport authority;
   - service/installer/recovery lifecycle;
   - Python/package/reproducibility and four-target parity;
   - Desktop/CLI/MCP first-run usability;
   - documentation/operations/ledger and exact-head evidence.
3. Union and deduplicate findings. No Critical, Important, or Minor finding may remain unresolved.
4. If remediation changes source, rerun affected gates, the complete four-platform proof, and the
   grouped re-review at the new exact head.
5. Hand off the owner-testable package references for the user's live E2E period. Do not publish or
   merge. Close completed GitHub issues/project items and clean only completed lane worktrees and
   merged lane branches; preserve the active feature branch and draft PR.
