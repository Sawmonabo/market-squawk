# Quarter 2 Task 6 Platform and Capture Report

Date: 2026-07-16

Branch: `feat/q2-task6-platform`

Base commit: `e33f5815480d4447304f0139957ead28b64ecd94`

## Result

Task 6 now has a nonempty `market-squawk-platform` production crate and the application consumes it
for configuration, capability-confined paths, journal compatibility, raw capture, writer
supervision, and capture-generation lifecycle control. The former application-local configuration
and journal implementations were removed after their callers and tests were migrated.

Raw capture is admitted synchronously through a bounded `try_send`; publication never waits for a
filesystem acknowledgement. A dedicated standard thread owns sink I/O and absolute flush
deadlines. Queue-byte accounting is represented by exactly-once RAII reservations. Saturation,
closure, invalid records, writer failure, accounting failure, supervisor exit, and shutdown
deadline all fail capture integrity closed.

## Configuration and path confinement

- Configuration precedence is deterministic: safe defaults, bounded TOML, supplied environment,
  then CLI overrides.
- Unknown `MARKET_SQUAWK_*` names, non-UTF environment values, malformed or oversized TOML,
  duplicate/empty product lists, and invalid combined flush/shutdown values fail closed.
- Secret values and references have redacted `Debug`; owned secret material is zeroized on drop.
- `LocalPaths` retains capability directory handles. Artifact and journal operations validate
  bounded portable components and remain relative to those capabilities.
- Final journal symlinks are rejected. Unix journal opens additionally use `O_NOFOLLOW`, and tests
  cover ancestor and retained-directory substitution without ambient-path escape.

## Journal compatibility and durability

- Writers create current `MSJ1/.msj` only. Readers retain exact current and committed legacy-v1
  compatibility.
- CRC, length, per-record size, record count, aggregate collection, offset arithmetic, lock,
  truncated-frame, and unsupported-magic behavior remain typed and fail closed.
- New-file initialization synchronizes the header and parent directory handle; flush synchronizes
  file data.
- Writer startup validates existing frames by streaming to EOF. It no longer reuses the 512 MiB
  in-memory collection limit, so valid large capture journals can be reopened. Checked per-record,
  record-count, and offset arithmetic remain in force; synthetic-over-ceiling and torn-tail
  regressions cover both sides of the contract.
- Both committed journal versions retain diagnostic raw bytes only.
  `JournalReplayAuthority::UnavailableByFormat`
  makes explicit that replay cannot reconstruct current capture or execution authority.

## Capture admission and lifecycle authority

- `RawCapturePublisher` is cloneable but degradation-only. It cannot activate or rotate an
  allocation.
- `RawCaptureControl` is non-clone and solely owns positive initial activation and generation
  rotation. Dropping it irreversibly invalidates the active allocation.
- Allocation lifecycle is one-way: initializing to healthy to incomplete. A faulted generation
  cannot be reactivated; recovery requires a newer generation and a distinct connection UUID.
- Rotation stops the old allocation with release ordering before RCU replacement. Post-enqueue
  validation requires writer-running state, old-allocation acceptance, and exact `Arc` identity.
  A barrier race proves an old publisher cannot retain a healthy receipt after rotation returns.
- `CaptureAdmissionReceipt` is owned, non-clone, non-Serde evidence bound to the exact allocation,
  event identity, source sequence, receive timestamp, and SHA-256 raw-frame digest. It is not
  execution authority and reflects later allocation invalidation.
- Live record construction normalizes visible payload bytes, preventing a tiny slice from retaining
  an attacker-sized backing allocation. Compatibility construction remains separately bounded and
  permissive only for committed historical wire semantics.
- Queue capacity and aggregate queued bytes are independently bounded. Reservations release on
  enqueue failure, dequeue, sink failure, rollover, deadline detach, and receiver drop. Underflow
  is never hidden by saturation; it increments a saturating diagnostic and invalidates capture.
- Handle drop and deadline-detach cleanup use nonblocking receiver-lock acquisition. A deterministic
  held-lock regression proves handle drop cannot wait behind the receiver mutex.

## Application supervision boundary

`CaptureContext` contains only a publisher and one immutable generation key. Source adapters cannot
activate or rotate capture. A source session returns a typed `SourceRunOutcome`; the app-owned
`SourceSupervisor` alone owns `RawCaptureControl`, applies bounded exponential reconnect backoff,
creates a fresh connection identity, and rotates to the next generation.

Lifecycle tests cover normal completion, explicit cancellation, adapter error, typed reconnect,
and task abort. Every supervisor exit path drops positive control and invalidates the active
allocation. The Coinbase source captures exact websocket bytes before JSON decode.

## Dependency and repository policy

- TOML parsing uses current `toml` 1.1, eliminating avoidable older `winnow` and `toml_datetime`
  families.
- `cap-std` 4's unavoidable `io-lifetimes` and Windows support generations are recorded as an exact
  reviewed duplicate inventory; unexpected family or version changes still fail the gate.
- Legacy-brand allowances were migrated from deleted app-local journal code to exact platform
  compatibility lines.
- A pre-existing research sentence triggered Gitleaks' generic-key heuristic. The exact prose line
  now carries an inline, rationale-qualified `gitleaks:allow`; a fresh 552.7 MiB directory scan
  reports no leaks.

## Verification evidence

The final tree passed:

```text
./scripts/verify.sh                                             PASS
  29 Python policy tests                                       PASS
  brand, boundary, and duplicate-dependency gates              PASS
  cargo fmt --all -- --check                                   PASS
  strict locked workspace Clippy, all targets/features         PASS
  locked workspace tests, all targets/features                 PASS
  locked workspace doctests                                    PASS
  locked all-feature release build                             PASS
  RUSTDOCFLAGS=-D warnings workspace docs                      PASS
  debug application build and CLI identity                     PASS
  deterministic offline mock (101 processed events)            PASS
  local stdio MCP smoke                                        PASS

gitleaks dir --redact --no-banner .                            PASS, no leaks
git diff --check                                                PASS
```

Focused platform coverage includes 7 unit tests, 18 capture/backpressure tests, 9 configuration
tests, 6 compatibility-journal tests, and 11 path-confinement tests. App source supervision adds 4
lifecycle tests in addition to the migrated application suite.

`Cargo.lock` was regenerated locally for all locked verification commands but is deliberately not
part of this lane's commit because the quarter coordinator owns the single authoritative lockfile
regeneration after parallel workspace lanes are integrated.

No performance claim is made. No identity/account rotation, TLS/browser fingerprint concealment,
CAPTCHA or anti-bot bypass, blocking-evasion proxy rotation, distributed quota evasion, telemetry,
paid service, cloud dependency, risk bypass, or replay-derived execution authority was added.
