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
- Do not replace the journal format or introduce a subprocess/IPC writer protocol in this lane.
- Do not change source-qualification or live-plane production code owned by other remediation lanes.
- Do not make GitHub Actions availability a prerequisite for local builds, tests, or runtime use.

## Considered approaches

### Approved: deadline revocation plus an explicitly owned pending worker

The writer remains a dedicated OS thread. At shutdown, capture authority is revoked synchronously
and queued reservations are released at the deadline. Shutdown always returns a
`PendingCaptureWriter` that owns the thread join handle, completion channel, and deadline snapshot;
borrowing async waits distinguish an observed worker exit from deadline expiry. Its `Drop` joins
synchronously as fail-closed misuse behavior; it never spawns a hidden background reaper and never
silently detaches the worker.

The worker and lifecycle owner each hold one strong side of an exact, process-wide, weak-registry
destination lease. The worker side remains held until thread exit. The owner side remains held until
`JoinHandle::join` has completed and the final termination report has been persisted. A successor
targeting the same destination cannot start while either side remains outstanding, including the
observable interval where `JoinHandle::is_finished()` is true but no owner has reaped it.

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

`CaptureWriterHandle::shutdown(self, deadline)` is synchronous: it consumes the only ordinary
handle, revokes authority, requests bounded cooperative shutdown, and returns a
`PendingCaptureWriter<B>` lifecycle owner. That owner exposes an async, borrowing
`wait_until_deadline(&mut self)` operation which reports one of:

- `CaptureShutdownStatus::WorkerTerminated`, which means `is_finished()` is true but deliberately
  does not yet claim join; or
- `CaptureShutdownStatus::DeadlineElapsed`, which proves authority was revoked but does not claim
  worker termination.

After `WorkerTerminated`, `try_reap(&mut self)` performs `JoinHandle::join`, persists and returns
`CaptureWorkerTermination`, and releases the owner half of the destination fence. `try_reap` refuses
to join while `is_finished()` is false. Rust 1.97 documents that a true
[`JoinHandle::is_finished`](https://doc.rust-lang.org/1.97.0/std/thread/struct.JoinHandle.html#method.is_finished)
means the associated thread has finished its main function, so the subsequent join is expected to
return quickly; this is not presented as a hard real-time nonblocking guarantee.

`CaptureWorkerTermination` records:

- the final `CaptureWriterOutcome`;
- whether the configured shutdown deadline elapsed;
- the record count observed when authority was revoked;
- the final record count after join; and
- the checked difference as late completed writes.

Natural worker completion is observed through the same lifecycle owner and `try_reap` path. Thread
panic, missing final outcome, or a failed join produces a fail-closed incomplete outcome and never a
clean termination claim.

Deadline and storage failures are independent facts. When a blocked operation returns a storage
error after the deadline, the causal outcome remains `WriterFailed` and
`shutdown_deadline_elapsed` is also true. When a blocked append succeeds after the deadline, its
commit is counted and the writer's post-I/O checkpoint produces `ShutdownDeadline`. This preserves
multiple causes without silently replacing either one.

## Pending ownership and reap

`PendingCaptureWriter<B>` is `#[must_use]` and retains the thread join handle. It exposes:

- the deadline-time authority-revocation snapshot;
- `is_worker_terminated`, using `JoinHandle::is_finished` only as a query;
- `wait_until_deadline(&mut self)`, which only awaits notifications/timers and never joins;
- `wait_until_terminated(&mut self)`, which only awaits a worker-finished notification and never
  joins; and
- `try_reap(&mut self)`, which joins only after the worker is known to be finished and persists the
  final termination report.

The async wait methods borrow rather than consume the lifecycle owner. Cancelling or dropping one of
their futures therefore leaves the queryable, joinable owner in the calling composition. No
`spawn_blocking`, detached join wrapper, background reaper task, or spawn-and-forget future is used.
The synchronous `try_reap` first observes `is_finished()` and returns `WorkerStillRunning` rather
than calling `join` when termination has not been proven.

Dropping a pending owner synchronously joins. This can block indefinitely if a custom or OS sink
never returns, and that is intentional fail-closed misuse behavior: ownership may not disappear.
Application code must therefore propagate or retain `PendingCaptureWriter` as typed lifecycle state,
not erase it into an ordinary error. In particular, relying on `Drop` can block an async executor
thread; async composition must use the borrowing wait and explicit reap path.

The app's source-run result becomes a typed shutdown result that can carry a pending writer back to
its composition owner. A clean application result is impossible while a pending capture worker
exists. Tests and CLI composition explicitly reap or return that owner.

## Destination fencing

Every `CaptureSink` publishes a stable, collision-resistant `CaptureDestination` identity for its
underlying physical endpoint before its worker starts. Every handle in the process that can reach
the same storage must return the same identity; per-instance or random aliases are forbidden for
shared alternative storage. `JournalWriter` derives the identity from its prepared canonical root
using a domain-separated SHA-256 digest, while test and alternative sinks use an explicit bounded
destination label. The digest prevents path or label content from appearing in public debug output.

A process-local registry atomically acquires one weak/reclaimable entry per destination during
`spawn_capture_writer`. Two strong guards are created: one moves into the writer thread and one into
the `CaptureWriterHandle`, then into `PendingCaptureWriter` at shutdown. The worker guard is released
only on thread exit. The owner guard is released only after the thread was joined and its final
outcome persisted. The last lease drop removes its exact weak entry with a pointer check, and the
registry has an explicit capacity bound. A spawn attempt for a still-strong destination entry
returns a typed `DestinationBusy` error without upgrading a weak pointer while holding the registry
mutex. The existing
per-allocation lifecycle atomic independently prevents a second writer for the same
`RawCaptureWriter` allocation. Journal file locking remains an additional OS-level fence and is
tested through late completion and reap. The process-local registry is not cross-process exclusion:
custom sinks shared across processes must provide their own operating-system or storage-level
ownership primitive. The prepared journal's exclusive file lock supplies that separate protection
for `JournalWriter`.

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
- A deadline wait returns `DeadlineElapsed`, not an ordinary incomplete outcome, while the thread
  remains alive.
- A blocked append or flush that later fails remains incomplete and is persisted in the reap report.
- A blocked append that later succeeds is counted as a late completed write before the final
  deadline outcome is reported.
- `CaptureWriterHandle::Drop` requests shutdown, revokes authority, drains queued reservations, and
  synchronously joins. It does not detach.
- `PendingCaptureWriter::Drop` synchronously joins. Production composition avoids this blocking path
  by retaining and explicitly reaping the owner.
- The worker-side destination lease and journal lock remain held until worker exit. The owner-side
  destination lease remains held through successful join and final-report persistence.

## Regression strategy

The TDD sequence begins with failing integration tests for:

1. a blocked append yielding `DeadlineElapsed` by the deadline while its pending owner remains
   retained, authority is revoked, and Tokio remains responsive;
2. a blocked flush yielding `DeadlineElapsed` under the same conditions;
3. a same-destination successor being rejected before release/reap;
4. gated append completion being included in late-write accounting after reap;
5. gated flush completion producing a joined final outcome without inventing a late append;
6. a successor remaining rejected after `is_finished()` becomes true but before `try_reap` joins the
   worker and releases the owner fence;
7. queue-byte reservations being released at deadline;
8. a journal's OS file lock releasing on worker exit while its in-process destination remains busy
   until the finished owner is explicitly reaped, then being reusable; and
9. drop behavior joining rather than silently detaching.

Existing capture authority, writer failure, rotation, source-supervisor, and journal compatibility
tests remain green. No test uses `unwrap`, `expect`, or intentional panic paths.

## Portable CI

The Linux job keeps `./scripts/verify.sh` on the GA `ubuntu-24.04` image. Separate
`macos-15-intel` and `windows-2025` jobs perform locked all-feature workspace builds and locked
all-workspace, all-target, all-feature tests. That gate compiles cfg-specific unit, integration,
example, and benchmark targets across every crate, including platform path confinement, journal
compatibility, capture lifecycle, and capture authority coverage. These explicit labels were
checked on 2026-07-16 against GitHub's
[hosted-runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
and the official [runner-images repository](https://github.com/actions/runner-images). The preview
`ubuntu-26.04` image and mutable `*-latest` aliases are deliberately excluded.

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
