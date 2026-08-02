# Capture Worker Lifecycle and Portable CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make capture shutdown revoke authority at its deadline without claiming a blocked OS writer terminated, retain a reapable owner and two-party destination fence until join, persist late-write accounting, and add immutable-action cross-platform CI coverage.

**Architecture:** `CaptureWriterHandle::shutdown` synchronously consumes the handle and returns a `PendingCaptureWriter`; borrowing async waits report deadline versus observed thread exit, while `try_reap` joins only after [`JoinHandle::is_finished`](https://doc.rust-lang.org/1.97.0/std/thread/struct.JoinHandle.html#method.is_finished) reports true, making join expected to return quickly without promising a hard real-time bound. A process-wide weak registry is held by separate worker and owner guards, so a destination is not reusable until both worker exit and owner join. The application handles pending ownership as typed state, and CI policy is enforced locally by Python tests.

**Tech Stack:** Rust 1.97, Tokio, standard OS threads and synchronization, SHA-256 destination identities, GitHub Actions, Python `unittest`.

## Global Constraints

- Exact base: `20ad084b47cfc0624a17f42233ff1e2748a62b05` in an isolated worktree.
- TDD is mandatory: witness each new regression fail for the intended reason before production edits.
- No hidden background reaper, detached join wrapper, or claim that a blocked thread was cancelled.
- Async waits borrow the lifecycle owner and never call blocking join; `try_reap` joins only after `is_finished`.
- Both worker and owner destination guards must be gone before the weak registry permits reuse.
- Preserve bounded queue accounting and persist late completed writes in the final reap report.
- Do not edit source/live production files unless compilation makes it unavoidable; report all overlap.
- No `unwrap`, `expect`, or intentional panic.
- Cloud CI is optional evidence; local scripts remain authoritative.

---

### Task 1: Exact destination identity and two-party process fence

**Files:**
- Modify: `crates/market-squawk-platform/src/capture/writer.rs`
- Modify: `crates/market-squawk-platform/src/capture.rs`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `crates/market-squawk-platform/src/journal.rs`
- Test: `crates/market-squawk-platform/tests/capture_lifecycle.rs`

**Interfaces:**
- Produces: `CaptureDestination`, `CaptureDestinationError`, required `CaptureSink::destination`, and `CaptureWriterSpawnError::DestinationBusy`.
- Produces internally: process-wide `Mutex<HashMap<CaptureDestination, Weak<_>>>` with one worker and one owner strong guard.

- [ ] **Step 1: Write the failing destination-fence regression**

Add a deterministic gated sink whose destination is supplied by the test. Start worker A and block
it in append, then attempt worker B with the same destination and assert `DestinationBusy`. Release
and explicitly wait/join A, then assert B starts. The key assertions are:

```rust
assert!(matches!(
    spawn_capture_writer(second_writer, second_sink, policy),
    Err(CaptureWriterSpawnError::DestinationBusy { .. })
));
release_sender.send(())?;
assert!(!first_handle.wait().await.is_incomplete());
let second_handle = spawn_capture_writer(third_writer, third_sink, policy)?;
```

- [ ] **Step 2: Run the regression and verify RED**

Run:

```bash
cargo test -p market-squawk-platform --all-features --locked --test capture_lifecycle destination_fence_rejects_concurrent_independent_writer -- --exact
```

Expected: compilation fails because `CaptureDestination`, the required sink destination method, and
destination-busy spawn error do not exist.

- [ ] **Step 3: Implement destination identity and fence acquisition**

Add a redacted digest identity and bounded named constructor:

```rust
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CaptureDestination([u8; 32]);

impl CaptureDestination {
    pub fn try_named(label: &str) -> Result<Self, CaptureDestinationError>;
    pub(crate) fn for_journal(path: &std::path::Path) -> Self;
    fn unique_memory() -> Self;
}
```

Make `CaptureSink::destination(&self) -> CaptureDestination` mandatory. `JournalWriter` returns a domain-separated hash of its confined path; `MemoryCaptureSink` stores a process-unique destination. Acquire the weak registry entry before changing writer lifecycle. Put one strong guard in the worker closure and one in `CaptureWriterHandle`; release the owner guard only after a completed join and persisted report.

- [ ] **Step 4: Run focused platform tests and verify GREEN**

Run:

```bash
cargo test -p market-squawk-platform --all-features --locked --test capture_lifecycle destination_fence_rejects_concurrent_independent_writer -- --exact
cargo test -p market-squawk-platform --all-features --locked --test capture_lifecycle
```

Expected: the new fence regression and existing lifecycle suite pass.

- [ ] **Step 5: Commit the fence**

```bash
git add crates/market-squawk-platform/src/capture crates/market-squawk-platform/src/capture.rs crates/market-squawk-platform/src/lib.rs crates/market-squawk-platform/src/journal.rs crates/market-squawk-platform/tests/capture_lifecycle.rs
git commit -m "fix(platform): fence capture destinations through join"
```

### Task 2: Pending shutdown, cooperative I/O checkpoints, and late-write reap report

**Files:**
- Modify: `crates/market-squawk-platform/src/capture/writer.rs`
- Modify: `crates/market-squawk-platform/src/capture.rs`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `crates/market-squawk-platform/tests/capture_lifecycle.rs`
- Modify: `crates/market-squawk-platform/tests/capture_authority_bridge.rs`
- Modify: `crates/market-squawk-platform/tests/capture_authority_bridge/cases.rs`

**Interfaces:**
- Produces: `CaptureIoContext`, `PendingCaptureWriter<B>`, `CaptureShutdownStatus`, `CaptureWorkerTermination`, `CaptureWorkerReapError`.
- Changes: `CaptureWriterHandle::shutdown(self, Duration) -> PendingCaptureWriter<B>` is synchronous.
- Produces: `PendingCaptureWriter::wait_until_deadline(&mut self)`, `wait_until_terminated(&mut self)`, `is_worker_terminated`, and nonblocking `try_reap(&mut self)`.

- [ ] **Step 1: Write failing gated append and flush regressions**

Create separate gated append and gated flush sinks. For each, enter the blocking operation, call synchronous shutdown, await the borrowing deadline wait, and assert:

```rust
let mut pending = handle.shutdown(Duration::from_millis(10));
assert_eq!(
    pending.wait_until_deadline().await,
    CaptureShutdownStatus::DeadlineElapsed
);
assert!(!pending.is_worker_terminated());
assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
assert_eq!(publisher.queued_bytes(), 0);
assert!(matches!(
    pending.try_reap(),
    Err(CaptureWorkerReapError::WorkerStillRunning)
));
```

Release the append gate, await termination by borrowing `pending`, reap, and assert final count `1`,
revocation count `0`, and late count `1`. Release the flush gate and assert late count `0`. Both tests
must demonstrate an unrelated Tokio timer runs before release.

Add the separate two-party-fence regression: release a gated worker, await until
`is_worker_terminated()` is true without calling `try_reap`, and assert an independent
same-destination spawn is still `DestinationBusy`. Reap the finished worker, then assert the
successor starts.

- [ ] **Step 2: Run both regressions and verify RED**

Run:

```bash
cargo test -p market-squawk-platform --all-features --locked --test capture_lifecycle blocked_append_returns_owned_pending_worker_and_persists_late_write -- --exact
cargo test -p market-squawk-platform --all-features --locked --test capture_lifecycle blocked_flush_returns_owned_pending_worker_without_false_termination -- --exact
```

Expected: compilation fails on the missing pending/status/reap APIs.

- [ ] **Step 3: Implement the synchronous shutdown handoff and borrowing waits**

Move the join handle, receiver, state, deadline, completion notification, shared final outcome, and owner destination guard into `PendingCaptureWriter`. Shutdown synchronously stops acceptance, records `records_written_at_revocation`, requests cooperative drain, and returns the owner. `wait_until_deadline` polls `is_finished` and a bounded Tokio timer without joining. At deadline it releases queued reservations and records `ShutdownDeadline`.

Implement the report shape:

```rust
pub struct CaptureWorkerTermination {
    outcome: CaptureWriterOutcome,
    records_written_at_revocation: u64,
    final_records_written: u64,
    late_records_written: u64,
}
```

`try_reap` first rejects a non-finished thread, then joins, consumes the stored final outcome, computes the checked late delta, persists the report, and finally releases the owner fence. Thread panic, missing outcome, or accounting reversal becomes `Incomplete { reason: WriterFailed }`.

- [ ] **Step 4: Add cooperative context without treating it as cancellation proof**

Change the sink contract to:

```rust
fn append(
    &mut self,
    record: &CapturedRawRecord,
    context: &CaptureIoContext,
) -> Result<(), CaptureSinkError>;
fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError>;
```

Check the context before and after Journal/Memory append and flush boundaries. After a sink call returns, count a successful append before checking deadline so late durable work is not erased. Never infer worker termination from the context flag.

- [ ] **Step 5: Update all platform sink implementations and ordinary shutdown paths**

Give every test sink an explicit destination and context arguments. Replace every awaited old shutdown with:

```rust
let mut pending = handle.shutdown(Duration::from_secs(1));
assert_eq!(
    pending.wait_until_deadline().await,
    CaptureShutdownStatus::WorkerTerminated
);
let termination = pending.try_reap()?.ok_or("worker did not retain termination")?;
```

No `_outcome` binding may drop a pending owner on an executor.

- [ ] **Step 6: Run all platform tests and verify GREEN**

Run:

```bash
cargo test -p market-squawk-platform --all-targets --all-features --locked
```

Expected: all platform unit, capture, path, journal, and configuration tests pass.

- [ ] **Step 7: Commit lifecycle semantics**

```bash
git add crates/market-squawk-platform
git commit -m "fix(platform): retain timed-out capture workers for reap"
```

### Task 3: Propagate typed ownership through sources tests and application composition

**Files:**
- Modify: `crates/market-squawk-sources/tests/capture_bridge.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/tests/source_supervisor.rs`
- Modify: `apps/market-squawk/tests/coinbase_source.rs`

**Interfaces:**
- Consumes: the Task 2 pending/status/reap API.
- Produces in app main: a typed internal run result that carries `PendingCaptureWriter<DiagnosticCaptureBundle>` rather than erasing it into `anyhow::Error`.

- [ ] **Step 1: Write a failing app ownership regression**

Add an application-level helper regression proving ordinary shutdown explicitly waits and reaps a joined termination and that a pending variant retains the lifecycle owner. Use static assertions or pattern matching to ensure the pending variant contains the concrete pending type.

- [ ] **Step 2: Run impacted tests and verify RED**

Run:

```bash
cargo test -p market-squawk --all-features --locked --test source_supervisor
cargo test -p market-squawk-sources --all-features --locked --test capture_bridge
```

Expected: compilation fails because old shutdown results are no longer futures/outcomes and the app lacks typed pending propagation.

- [ ] **Step 3: Update every downstream call site**

Introduce a private app run disposition:

```rust
enum RunSourceDisposition {
    Complete(DiagnosticEngineSnapshot),
    CapturePending(PendingCaptureWriter<DiagnosticCaptureBundle>),
}
```

`run_source` returns `CapturePending` on deadline instead of constructing an ordinary error. The CLI composition owner retains the pending value, awaits worker termination through a borrowing wait, calls `try_reap`, and only then converts the persisted final outcome to user-facing failure. Fast-path shutdown in the app, sources bridge test, supervisor tests, and Coinbase test always waits and reaps explicitly.

- [ ] **Step 4: Verify downstream GREEN**

Run:

```bash
cargo test -p market-squawk-sources --all-features --locked --test capture_bridge
cargo test -p market-squawk --all-targets --all-features --locked
```

Expected: the sources bridge and all app targets pass.

- [ ] **Step 5: Commit call-site propagation**

```bash
git add crates/market-squawk-sources/tests/capture_bridge.rs apps/market-squawk/src/main.rs apps/market-squawk/tests/source_supervisor.rs apps/market-squawk/tests/coinbase_source.rs
git commit -m "fix(app): retain capture shutdown ownership through reap"
```

### Task 4: Pin portable CI and enforce its policy locally

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `scripts/tests/test_ci_workflow_policy.py`

**Interfaces:**
- Produces: explicit `ubuntu-24.04`, `macos-15-intel`, and `windows-2025` jobs.
- Preserves: Linux `./scripts/verify.sh`; local `scripts/verify.sh` remains the authority.

- [ ] **Step 1: Write the failing CI workflow policy tests**

The standard-library test must assert all action references match `@[0-9a-f]{40}`, every checkout step's bounded block contains `persist-credentials: false`, `*-latest` is absent, the exact GA labels are present, Linux still runs `./scripts/verify.sh`, and macOS/Windows contain locked workspace build plus locked all-workspace/all-target/all-feature tests.

- [ ] **Step 2: Run the policy test and verify RED**

Run:

```bash
python3 -m unittest scripts.tests.test_ci_workflow_policy -v
```

Expected: failure because macOS/Windows jobs and exact runner labels are absent.

- [ ] **Step 3: Implement pinned cross-platform jobs**

Retain the existing immutable action SHAs. Use `ubuntu-24.04`, `macos-15-intel`, and
`windows-2025`; set `persist-credentials: false` on every checkout. Non-Linux jobs run:

```yaml
- run: cargo build --workspace --all-features --locked
- run: cargo test --workspace --all-targets --all-features --locked
```

- [ ] **Step 4: Verify CI policy GREEN**

Run:

```bash
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
```

Expected: all local policy tests pass.

- [ ] **Step 5: Commit CI coverage**

```bash
git add .github/workflows/ci.yml scripts/tests/test_ci_workflow_policy.py
git commit -m "ci: add pinned portable platform coverage"
```

### Task 5: Final API audit, verification, and focused commits

**Files:**
- Review: `crates/market-squawk-platform/src/capture.rs`
- Review: `crates/market-squawk-platform/src/capture/writer.rs`
- Review: `crates/market-squawk-platform/src/lib.rs`
- Review: every `shutdown`/`spawn_capture_writer` call site from `git grep`.

**Interfaces:**
- Confirms all public types have accurate rustdoc and every ordinary shutdown reaches joined reap.

- [ ] **Step 1: Audit public API and call-site completeness**

Run `git grep -n 'shutdown\|spawn_capture_writer\|impl CaptureSink' -- '*.rs'` and verify every sink has a destination/context contract, every fast shutdown borrows a wait and reaps, pending ownership is typed, and no ignored binding can detach lifecycle ownership.

- [ ] **Step 2: Run formatting and strict impacted Clippy**

```bash
cargo fmt --all -- --check
cargo clippy -p market-squawk-platform -p market-squawk-sources -p market-squawk --all-targets --all-features --locked -- -D warnings
```

- [ ] **Step 3: Run locked acceptance tests**

```bash
cargo test -p market-squawk-platform --all-targets --all-features --locked
cargo test -p market-squawk-sources --all-features --locked --test capture_bridge
cargo test -p market-squawk --all-targets --all-features --locked
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
```

- [ ] **Step 4: Run release, docs, and repository hygiene checks**

```bash
cargo build --workspace --all-features --release --locked
RUSTDOCFLAGS="-D warnings" cargo doc -p market-squawk-platform -p market-squawk --all-features --no-deps --locked
git diff --check 20ad084b47cfc0624a17f42233ff1e2748a62b05..HEAD
git status --short
```

- [ ] **Step 5: Commit any verification-only corrections**

Stage only focused files and use a descriptive `fix(platform): ...` or `test(platform): ...` commit. Do not squash away the witnessed TDD sequence.
