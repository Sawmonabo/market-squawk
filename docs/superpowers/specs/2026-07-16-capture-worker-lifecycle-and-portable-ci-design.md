# Capture Worker Lifecycle and Portable CI Design

**Status:** Approved with lifecycle-ownership correction on 2026-07-16

## Objective

Make bounded capture shutdown truthful when a dedicated operating-system writer thread is blocked
inside sink I/O. The deadline must revoke capture authority immediately without pretending the
thread was cancelled. Shutdown must return explicit ownership of any still-running worker, preserve
the final outcome and late-write accounting after reap, and prevent a successor from opening the
same capture destination until the old worker has terminated and been joined.

Add pinned Linux, macOS, and Windows CI coverage without making cloud CI a runtime dependency. The
complete local verification gate remains on Linux; macOS and Windows exercise locked builds and the
platform path, journal, and capture suites.

## Non-goals

- Do not claim that `std::thread` or a blocking filesystem call can be forcibly cancelled.
- Do not implement provider identity/account rotation, proxy or fingerprint spoofing, CAPTCHA
  bypass, or any quota-evasion behavior.
- Do not replace the journal format or introduce a subprocess/IPC writer protocol in this lane.
- Do not change source-qualification or live-plane production code owned by other remediation lanes.
- Do not make GitHub Actions availability a prerequisite for local builds, tests, or runtime use.

## Considered approaches

### Approved: deadline revocation plus an explicitly owned pending worker

The writer remains a dedicated OS thread. At shutdown, capture authority is revoked synchronously
and queued reservations are released. If the worker finishes before the deadline, shutdown returns
a terminated report. If it is still inside append or flush, shutdown returns a `PendingCaptureWriter`
that owns the thread join handle, completion channel, and deadline snapshot. The pending owner can be
queried, nonblockingly polled, or asynchronously reaped. Its `Drop` joins synchronously as fail-closed
misuse behavior; it never spawns a hidden background reaper and never silently detaches the worker.

The worker holds an exact destination lease until it has terminated. The pending owner holds the
join responsibility until reap completes. A successor targeting the same destination cannot start
while either condition remains outstanding.

### Rejected: subprocess writer isolation

A subprocess can be terminated at an OS boundary, but it requires a new bounded IPC protocol,
journal recovery semantics, child-process supervision, and cross-platform process controls. That is
a separate architectural change and is not necessary to make current shutdown reporting truthful.

### Rejected: cooperative cancellation alone

A cooperative flag is useful between I/O operations but cannot prove that a blocked OS call ended.
Returning a final outcome merely because a deadline elapsed would repeat the existing defect.

## Capture lifecycle model

The public state model separates three facts that were previously conflated:

1. **Authority state:** publication and generation authority are revoked at or before the shutdown
   deadline.
2. **Worker state:** the writer thread is either terminated-and-joined or still pending.
3. **Storage result:** the worker's eventual final outcome includes all records whose append returned
   successfully, including records completed after authority revocation.

`CaptureWriterHandle::shutdown(self, deadline)` consumes the only ordinary handle and returns one of:

- `CaptureShutdown::Terminated(CaptureWorkerTermination)`, which proves the thread was joined and
  contains the final `CaptureWriterOutcome`; or
- `CaptureShutdown::Pending(PendingCaptureWriter<B>)`, which proves authority was revoked but does
  not claim worker termination.

`CaptureWorkerTermination` records:

- the final `CaptureWriterOutcome`;
- the record count observed when authority was revoked;
- the final record count after join; and
- the checked difference as late completed writes.

Normal completion and `wait` also return joined termination evidence. Thread panic, a closed
completion channel, or a failed join produces a fail-closed incomplete outcome and never a clean
termination claim.

## Pending ownership and reap

`PendingCaptureWriter<B>` is `#[must_use]` and retains the thread join handle. It exposes:

- the deadline-time authority-revocation snapshot;
- `is_worker_terminated`, using `JoinHandle::is_finished` only as a query;
- `try_reap`, which joins only after the worker is known to be finished; and
- `reap(self)`, which waits for completion outside the Tokio executor's blocking worker path and
  returns the persisted final termination report.

Dropping a pending owner synchronously joins. This can block indefinitely if a custom or OS sink
never returns, and that is intentional fail-closed misuse behavior: ownership may not disappear.
Application code must therefore propagate or retain `PendingCaptureWriter` as typed lifecycle state,
not erase it into an ordinary error.

The app's source-run result becomes a typed shutdown result that can carry a pending writer back to
its composition owner. A clean application result is impossible while a pending capture worker
exists. Tests and CLI composition explicitly reap or return that owner.

## Destination fencing

Every `CaptureSink` publishes a stable `CaptureDestination` identity before its worker starts.
`JournalWriter` derives it from its confined journal path using a domain-separated SHA-256 digest;
test and alternative sinks use an explicit bounded destination label. The digest prevents path or
label content from appearing in public debug output.

A process-local registry atomically acquires one lease per destination during
`spawn_capture_writer`. The lease moves into the writer thread and is released only when that thread
exits. A spawn attempt for an already leased destination returns a typed `DestinationBusy` error.
The existing per-allocation lifecycle atomic independently prevents a second writer for the same
`RawCaptureWriter` allocation. Journal file locking remains an additional OS-level fence and is
tested through late completion and reap.

The destination identity is part of the sink contract, so independent constructors cannot omit the
fence. Test-only convenience sinks receive unique destinations by default and can be constructed
with an explicit shared destination for collision tests.

## Cooperative sink context

Append and flush receive a read-only `CaptureIoContext` containing shutdown-request and deadline
state. `checkpoint()` fails with a typed shutdown-deadline error after revocation. Production
`JournalWriter` checks the context before and after append/flush boundaries. The writer also checks
after every sink return before dequeuing another record.

This context limits additional work after a blocked call returns. It is never treated as evidence
that a call currently blocked in the OS has stopped. A successful append that returns after the
deadline increments the final record count and therefore appears in `late_records_written`; no
execution authority is restored by that accounting.

## Failure and drop behavior

- Shutdown first stops publication and degrades the exact current generation.
- Queue draining releases every queued byte reservation even if the worker owns one in-flight frame.
- A deadline returns `Pending`, not an ordinary incomplete outcome, while the thread remains alive.
- A blocked append or flush that later fails remains incomplete and is persisted in the reap report.
- A blocked append that later succeeds is counted as a late completed write before the final
  deadline outcome is reported.
- `CaptureWriterHandle::Drop` requests shutdown, revokes authority, drains queued reservations, and
  synchronously joins. It does not detach.
- `PendingCaptureWriter::Drop` synchronously joins. Production composition avoids this blocking path
  by retaining and explicitly reaping the owner.
- The destination lease and journal lock remain held until worker exit, including after a deadline.

## Regression strategy

The TDD sequence begins with failing integration tests for:

1. a blocked append returning `Pending` by the deadline while authority is revoked and Tokio remains
   responsive;
2. a blocked flush returning `Pending` under the same conditions;
3. a same-destination successor being rejected before release/reap;
4. gated append completion being included in late-write accounting after reap;
5. gated flush completion producing a joined final outcome without inventing a late append;
6. queue-byte reservations being released at deadline;
7. a journal destination/file lock remaining unavailable until the old worker is reaped, then being
   reusable; and
8. drop behavior joining rather than silently detaching.

Existing capture authority, writer failure, rotation, source-supervisor, and journal compatibility
tests remain green. No test uses `unwrap`, `expect`, or intentional panic paths.

## Portable CI

The Linux job keeps `./scripts/verify.sh` and is pinned to the repository's chosen Ubuntu image.
Separate macOS and Windows jobs perform locked all-feature workspace builds and explicit locked
platform tests for path confinement, journal compatibility, capture lifecycle, and the capture
authority bridge. Runner labels are selected only after checking current official GitHub-hosted
runner documentation.

Every third-party action reference is a full 40-hex commit SHA. Every checkout step declares
`persist-credentials: false`. A standard-library Python policy test parses the workflow text and
fails on mutable action references, missing checkout hardening, unapproved runner labels, missing
locked commands, or removal of the full Linux verification gate.

## Acceptance evidence

- Each lifecycle behavior follows a witnessed red-green TDD cycle.
- `cargo fmt --all -- --check` passes.
- Strict locked Clippy passes for the platform and impacted app targets.
- Locked platform and app tests, including the new lifecycle regressions, pass.
- The locked all-feature release build passes.
- Python policy tests and the CI workflow policy test pass.
- `git diff --check` passes.
- The final report includes the exact base, focused commit range, commands, exit status, test counts,
  cross-lane file overlap, and any evidence that could not be run locally.
