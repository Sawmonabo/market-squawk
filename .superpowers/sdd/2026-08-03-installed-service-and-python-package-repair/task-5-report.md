# Task 5 — First-run, installer repair, and installed smoke consumers

Status: consumer implementation complete and ready for parent integration review; committed locally
and not pushed. This result is not release-gate approval because the real-process macOS debug smoke
described below did not recover ordinary service readiness after a successful bootstrap action.

## Audit base and delivery commit

- Task 4 consumer-contract base: `91127d21376c9260a0fbc0c7344245adca2c0c0c`.
- Task 5 delivery commit: the commit containing this report; the exact immutable hash is recorded in
  the orchestrator handoff because a Git commit cannot embed its own hash.
- Refresh gate: integration must repeat exact-head verification if this candidate, Task 4's consumer
  contract, the workspace lockfile, or packaged CLI contents change.

## Delivered outcome

### Desktop first-run recovery

- Preserved normal authenticated service connection as the first path. Only Task 4's typed
  `InstalledServiceBootstrapState::Required` becomes setup-visible; `Retrying` is never treated as
  readiness.
- Kept the Task 4 `InstalledServiceConnector`, bootstrap client, service child, and readiness
  composition state in native Rust ownership while foreground recovery is pending.
- Added one narrowly scoped `desktop_service_bootstrap` Tauri command. It accepts exactly the typed
  encrypted-fallback unlock or foreground-keyring retry action, consumes unlock material through a
  zeroizing native secret, and returns no secret or connection material.
- After the action, required Task 4's typed response to be `Retrying` with no remaining requirement,
  then reconnected through the ordinary authenticated application connector before installing the
  ready Desktop and MCP state.
- Added a generated command permission and admitted it only for the existing `main` window. No broad
  application-invoke permission was added.
- Kept the existing loading/ready/error state correlation and existing Overview unavailable-service
  design. The minimal recovery form clears its unlock input before dispatch and refetches ordinary
  bootstrap state after success.
- Added existing-boundary coverage proving the exact unlock action, immediate field clearing, and no
  setup transition until the normal ready bootstrap response arrives. Added native coverage for
  rejecting an action that does not match the typed requirement.
- The bootstrap-required WebView document contains only `status` and the typed non-secret
  `requirement`; it does not contain an endpoint, socket/path, data root, installation identity,
  service generation, normal rendezvous credential, runtime bearer credential, or echoed unlock.

### Installer activation and repair

- Classified only the exact Task 4 top-level `bootstrap_required` status document as recoverable
  foreground work during candidate activation. This preserves the installed candidate and its final
  receipt instead of rolling back a healthy-but-locked first run.
- Kept ordinary verify/status behavior fail-closed: bootstrap-required is not reported as ready.
- Required the exact bootstrap document shape, required state, one of the two Task 4 requirement
  values, a non-nil installation UUID, and a nonzero service generation. Extra, malformed, retrying,
  stale/mismatched expected-identity, protocol, package, and ordinary health failures remain errors.
- Extended the existing activation lifecycle test to prove a normal health failure rolls back while
  exact bootstrap-required activation preserves the new candidate and previous-version receipt
  history. No platform registration adapter was changed.

### Installed smoke diagnostics and bootstrap

- Reused the packaged CLI contracts `market-squawk service status --output json` and
  `market-squawk service bootstrap --stdin`; no endpoint, framing, or credential lifecycle was
  reconstructed in Python.
- Generates the encrypted-fallback test unlock process-locally only after receiving the exact typed
  required state. The unlock is written only to the bootstrap subprocess's stdin and is absent from
  service argv, service environment, bootstrap argv, artifacts, fixtures, and repository content.
- Handles the foreground-keyring requirement through the packaged CLI's explicit retry flag.
- Reads at most 8 KiB plus one truncation-detection byte from diagnostics, strips control characters,
  redacts the process-local unlock and credential-like fields, and marks truncated output. The same
  helper protects bootstrap-command, MCP, and service stderr reporting.
- Adjusted only bounded status/bootstrap/start timeouts to accommodate Task 4's authenticated normal
  connection deadline.

## Verification evidence

### Test-driven boundary evidence

- Desktop frontend RED: the new existing-boundary test initially could not find `Fallback unlock`;
  the application attempted to parse the typed bootstrap-required setup object as the ordinary ready
  document.
- Desktop frontend GREEN under the originally available host Node: the focused app test passed.
- Desktop Rust RED: the focused native test failed to compile because
  `DesktopBootstrapAction`/bootstrap-action admission did not exist.
- Desktop Rust GREEN: the focused native bootstrap-action test passed.
- Installer RED: the focused service-registration test failed to compile because the typed
  activation health classifier did not exist.
- Installer GREEN:
  `CARGO_INCREMENTAL=0 cargo test -p market-squawk-installer --lib service_ --locked`
  passed 3 tests with 13 filtered out.

### Affected gates

- `CARGO_INCREMENTAL=0 cargo test -p market-squawk-desktop --lib --locked`
  passed all 5 Desktop library tests.
- With explicit `source /Users/sawmonabo/.nvm/nvm.sh && nvm use 24.18.0`, the exact repo-pinned
  toolchain reported Node `v24.18.0` and pnpm `10.31.0`.
- From `apps/market-squawk-desktop`,
  `pnpm test --run src/test/app.test.tsx` passed 1 file and all 4 tests under that exact toolchain.
- From `apps/market-squawk-desktop`, `pnpm typecheck` passed under that exact toolchain.
- `python3 -m py_compile scripts/smoke_mcp.py` passed.
- A focused Python pressure check proved the bootstrap unlock was present only on subprocess stdin,
  absent from argv/environment, redacted from diagnostics, and subject to the 8 KiB bound.
- `cargo fmt --all` completed successfully at final source content.
- `git diff --check` passed after final formatting.
- A targeted source audit found no bootstrap endpoint/path/runtime credential in the new WebView
  setup document and confirmed stdin-only unlock handling plus bounded/redacted stderr in the smoke
  consumer.
- `target` was approximately 16 GiB during verification, below the 20 GiB ceiling.

## Bounded obstruction and concerns

- **Correction to an earlier progress update:** the statement that no smoke issue remained was too
  broad. The isolated helper/pressure check passed, but an actual local macOS debug service run did
  not complete the post-bootstrap transition to ordinary readiness.
- In that real-process run, the first packaged status command returned the exact
  `bootstrap_required`/`encrypted_fallback_locked` document. The packaged stdin bootstrap command
  succeeded and returned typed `retrying` with `requirement: null`. Subsequent ordinary status calls
  returned `installed Market Squawk service is unavailable` until the bounded 60-second start
  deadline, even though the service process remained alive and emitted no stderr. This may be a
  local debug-service readiness/lifecycle issue beyond the Task 5 consumer, but its cause is not yet
  proven. The hosted installed-service gate or equivalent exact packaged macOS reproduction must be
  green before approval.
- No broad Cargo, workspace, clippy, package, or hosted workflow gate was rerun after the bounded
  focused evidence above. That was an explicit orchestration constraint, and this report makes no
  approval claim.
- No workflow, lockfile, platform registration adapter, remote branch, or external system was
  modified. No push or remote mutation was performed.
