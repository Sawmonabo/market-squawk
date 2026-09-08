# Task 5A — Ready-state native credential admission

Status: implementation complete, focused macOS evidence green, committed locally after the final
checks described below, and not pushed. This report is task-lane evidence, not release-gate
approval.

## Audit base and refresh gate

- Frozen implementation base: `8e1fc6bee2eff059b65f8410b11514e8f0e5e114`.
- Delivery commit: the commit containing this implementation; its immutable hash is supplied in the
  orchestrator handoff because a commit cannot contain its own hash.
- Refresh gate: repeat the exact focused real-process test and exact-head review if the service
  lifecycle, runtime/MCP credential authority, native local-socket dependencies, workspace
  lockfile, or consolidated control-plane harness changes before integration.

## Proven failure and TDD boundary

- The pre-change real-process scenario successfully unlocked the encrypted fallback in the service
  process, reached normal service composition, and then failed a newly launched connector with
  `Error: SecretStore`. This proved that each fresh connector was incorrectly reopening a new,
  locked runtime secret store after bootstrap.
- After changing the harness to the required explicit installation-root entry points, the focused
  no-run build failed with `E0599` because
  `InstalledService::start_at_installation_root` and
  `InstalledServiceConnector::try_new_at_installation_root` did not exist. This was the explicit
  root API RED rather than a fixture or compiler failure.
- The final focused real-process test is one consolidated scenario. It seeds an encrypted runtime
  in a bounded subprocess, starts a separate service process, proves the exact bootstrap-required
  state, supplies the unlock only through the owner-authenticated bootstrap client, proves the
  bootstrap endpoint is removed, and launches a fresh process that admits CLI, Desktop, Claude Code
  and Codex. It then restarts the service, proves bootstrap is required again with a different
  bootstrap generation, unlocks again, and admits a fresh CLI process.

## Delivered outcome

### Service-lifetime ready admission

- Added a distinct service-lifetime admission listener and fixed binary protocol. It accepts only
  the four closed `NamedClient` values and binds metadata, request, and response to protocol V1,
  exact installation/workspace/service generation, exact process/start identity, endpoint nonce,
  named client, request nonce, and bounded monotonic deadline.
- The response contains only a validated `RendezvousRecord`, reference-free client ID and
  credential-generation projection, and the exact credential. It never serializes a `SecretRef`,
  rendezvous signing key, unlock capability, provider credential, or arbitrary secret lookup.
- Request/response frames, credential bytes, connection count, per-connection time, and trailing
  data are bounded. Secret-bearing response frames are zeroized on both service and connector
  paths, and debug output remains redacted.
- Unix admission uses an owner-private `0700` root, `0600` local socket, bilateral effective-UID
  peer checks, a distinct domain-separated socket name, and device/inode-safe cleanup.
- Windows admission uses a logon-SID-only pipe DACL, client impersonation and exact logon-SID
  membership check, local/first-instance listener defaults, nonblocking bounded accept/preface
  handling, and bounded nonblocking client reads/writes. The pre-existing Windows bootstrap bind
  now creates its metadata root before publication, matching the Unix startup invariant.

### Exact ready authority and lifecycle

- `InstalledServiceConnector` no longer owns or opens any runtime `SecretStore`. Normal native and
  MCP connections request exact material from the ready broker and then construct the existing
  authenticated loopback client/relay.
- Desktop and CLI credentials are read from the already-unlocked service authority. Claude Code
  and Codex admission is performed under the live `InstalledMcpControl` mutation gate, rejects
  pending/revoked state, and reads the exact effective generation, so rotation/revocation cannot
  race a stale disk/root projection.
- Startup binds the private listener, proves it with an admitted self-probe, publishes the ordinary
  signed rendezvous, and publishes admission metadata last. Failure unwinds both authorities.
- Shutdown retires exact-generation admission metadata and joins the broker before draining the
  ordinary transport and before releasing the installation instance guard. Unexpected broker exit
  is terminal, and admission retirement is part of the typed shutdown-completeness report.
- A process restart retains durable runtime identity but creates a new process/endpoint nonce and
  requires encrypted-fallback unlock again; stale admission metadata and sockets therefore fail
  closed.

### Explicit installation authority and keyring isolation

- Added validated absolute-root constructors for the connector and service, including a
  process-owned logging-store service entry point and
  `InstalledServiceLogging::install_at_installation_root` for binary startup plumbing.
- The selected root covers installation instance/identity/rendezvous/bootstrap/admission state,
  encrypted secret fallback, and structured logs. Existing default APIs remain unchanged.
- Non-default installations now use a bounded deterministic keyring service namespace derived from
  the canonical installation authority root. The canonical default installation retains the
  legacy `market-squawk-runtime` namespace so existing V1 installations keep their current keyring
  entries; every explicit/non-default root uses
  `market-squawk-runtime-v1-<128-bit-root-digest>`.

## Verification evidence

- `CARGO_INCREMENTAL=0 cargo check -p market-squawk --lib --locked` passed after the broker and
  lifecycle wiring.
- `CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane
  production_mcp_composition::service_runtime_is_the_single_authority_for_native_and_mcp_clients
  --locked --no-run` passed at final test source.
- `CARGO_INCREMENTAL=0 cargo clippy -p market-squawk --lib --test control_plane --locked -- -D
  warnings` passed with no warnings at final source in 39.22 seconds (after the earlier cold focused
  clippy pass).
- `CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane
  production_mcp_composition::service_runtime_is_the_single_authority_for_native_and_mcp_clients
  --locked -- --exact --nocapture --test-threads=1` passed: 1 passed, 0 failed, 32 filtered out,
  in 121.30 seconds at final source.
- `cargo fmt --all -- --check` passed at final source.
- `git diff --check` passed at final source.
- Final `du -sk target` was `7702172 target` (approximately 7.35 GiB), below the binding 20-GiB
  ceiling.

## Bounded obstruction and concerns

- A Windows target check was attempted in an isolated `/tmp` target directory. It was obstructed
  before reaching `market-squawk` by the host's missing Windows C SDK/sysroot: `stacker` failed on
  `windows.h`, and `zstd-sys` failed on standard C headers such as `stdlib.h`/`string.h`. This is an
  environmental cross-toolchain obstruction, not a Rust diagnostic from the new Windows module;
  Windows CI remains the required execution/compile proof.
- The packaged CLI/service/Desktop hidden-root plumbing and installed smoke script are owned by the
  parent Task 5B lane and are intentionally not changed here. This lane provides the narrow public
  at-root APIs they require.
- No manifest, lockfile, smoke script, Python release code, documentation, workflow, README,
  progress ledger, remote branch, or external system was modified. No push was performed.
