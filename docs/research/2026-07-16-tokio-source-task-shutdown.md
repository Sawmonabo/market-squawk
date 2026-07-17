# Tokio source-task shutdown evidence

Date anchored: 2026-07-16  
Repository dependency lock: Tokio 1.52.4, Tokio Util 0.7.18  
Scope: Q2 bounded source shutdown and application lifecycle remediation

## Decision

Market Squawk must retain the sole `JoinHandle` for every source task. Cooperative cancellation is
the first shutdown phase. If the configured nonzero deadline expires, the owner aborts the task and
then awaits the same handle before allowing dependent event and capture owners to shut down. Merely
dropping a `JoinHandle`, calling `abort`, or observing `is_finished` is not reaping evidence.

The application orchestration must treat signal, MCP, source, event-task, and capture errors as
outcomes to aggregate after reverse-order cleanup. A `?` after the first worker is spawned must not
permit an early return that drops a join handle and detaches the task.

## Primary evidence

- Tokio documents `JoinHandle` as the owned permission to join a task. Dropping it detaches the
  task, while awaiting it proves the task destructor has completed. `&mut JoinHandle<T>` is cancel
  safe in `select!`, so an alternate branch may win without losing the eventual task result:
  <https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html>
- Tokio's task cancellation documentation states that `abort` only schedules cancellation. The
  caller must await the handle to wait for cancellation and destructor completion; a task may also
  complete normally in the race with abort:
  <https://docs.rs/tokio/latest/tokio/task/index.html#cancellation>
- Tokio `timeout` cancels its wrapped future by dropping that future when the deadline elapses. In
  this design the wrapped future only borrows and polls the retained join handle, so the owner still
  has the handle available to abort and await after timeout:
  <https://docs.rs/tokio/latest/tokio/time/fn.timeout.html>
- Tokio `select!` drops non-winning branch futures. Its documentation requires cancellation-safety
  analysis at every awaited operation and lists cancellation-safe primitives:
  <https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety>
- Tokio Util 0.7.18 documents that cancelling a parent `CancellationToken` cancels its child tokens,
  `cancelled()` is cancellation safe, and a child cannot cancel its parent:
  <https://docs.rs/tokio-util/0.7.18/tokio_util/sync/struct.CancellationToken.html>

The exact locked Tokio 1.52.4 source was also inspected locally at
`tokio-1.52.4/src/runtime/task/join.rs`, `tokio-1.52.4/src/task/mod.rs`,
`tokio-1.52.4/src/time/timeout.rs`, and `tokio-1.52.4/src/macros/select.rs`. Its relevant contracts
match the public documentation above.

## Required verification consequences

1. Exercise natural completion, typed source failure, cooperative cancellation, deadline abort,
   join panic/cancellation, reconnect backoff, signal failure, MCP failure, and event-worker failure.
2. In every branch prove the source handle is consumed, the event sender is dropped and event task
   joined, and the capture worker is shut down or returned as an explicit pending owner.
3. Ensure abort races accept either a natural classified completion or the explicit
   deadline-aborted outcome, but never leave an owned handle unreaped.
4. Keep generalized task-spawning callbacks private; the production entry point owns a supervised
   market source rather than exposing a new detach-capable application API.
5. Do not retain arbitrary `anyhow` display chains as diagnostics unless a proven non-secret
   boundary has sanitized them.
