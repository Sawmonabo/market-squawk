# Journal durability and capability-bound I/O

Date: 2026-07-16  
Scope: Stage 1 platform raw-capture journal and controlled artifact storage

## Decision

Market Squawk will treat a successful buffered `write` or `flush` as insufficient evidence of
durability. The background capture writer must:

1. write through a directory capability rather than reopening an ambient path;
2. flush any userspace buffer;
3. synchronize the file and handle the returned error explicitly;
4. synchronize creation/rename metadata through the containing directory where the platform
   supports it; and
5. report a failed or incomplete durable capture when any required operation fails.

The live event-to-action path only performs a bounded `try_send`. File creation, validation,
serialization, flushing, synchronization, rotation, and recovery remain on the supervised
background/control-plane path.

## Primary-source findings

- Rust documents that errors detected while a `File` is closed by `Drop` are ignored and directs
  applications that need to handle them to an explicit synchronization operation. Rust also
  distinguishes `sync_data`, which may omit filesystem metadata, from `sync_all`, which attempts
  to synchronize both data and metadata. For journal creation and structural changes, the platform
  therefore uses the stronger operation unless an explicitly documented, measured append-only
  policy proves that `sync_data` is sufficient.
  [Rust `std::fs::File`](https://doc.rust-lang.org/std/fs/struct.File.html)

- Linux documents that synchronizing a file does not necessarily make the containing directory
  entry durable; the directory also needs explicit synchronization. This matters for first
  creation and atomic replacement/rotation, even after the file itself has been synchronized.
  [Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)

- POSIX specifies that `fsync` does not return until the requested synchronization has completed or
  an error is detected. The platform must propagate that error into capture health rather than
  equating a completed write call with durable capture.
  [POSIX `fsync`](https://www.man7.org/linux/man-pages/man3/fsync.3p.html)

- `cap-std` models filesystem authority as an already-open `Dir`, and its operations resolve paths
  relative to that capability. Its `File` exposes both `sync_all` and `sync_data`. This avoids the
  canonicalize-then-reopen race that would otherwise restore access to the ambient filesystem.
  [`cap_std::fs::Dir`](https://docs.rs/cap-std/4.0.2/cap_std/fs/struct.Dir.html),
  [`cap_std::fs::File`](https://docs.rs/cap-std/4.0.2/cap_std/fs/struct.File.html)

- POSIX explains that descriptor-relative rename exists to avoid path-component races and specifies
  atomic rename behavior within its applicability. Journal rotation or manifest replacement must
  therefore operate relative to held directory capabilities and must not cross filesystems.
  [POSIX `rename`/`renameat`](https://man7.org/linux/man-pages/man3/rename.3p.html)

- Tokio tasks are cooperatively scheduled, so synchronous append, flush, and synchronization calls
  must not run inside an ordinary `tokio::spawn` future. Tokio's current guidance distinguishes
  short-lived blocking operations (`spawn_blocking`) from long-lived persistent blocking workers,
  for which it recommends a dedicated thread. Raw capture is a persistent ordered writer, so it
  uses one supervised dedicated OS thread and a bounded bridge; shutdown is cooperative and
  deadline-aware rather than relying on aborting an uninterruptible blocking operation.
  [Tokio task blocking guidance](https://docs.rs/tokio/1.52.3/tokio/task/index.html),
  [Tokio `spawn_blocking`](https://docs.rs/tokio/1.52.3/tokio/task/fn.spawn_blocking.html)

## Required implementation consequences

- `JournalWriter` receives or owns a directory capability plus a validated relative journal name;
  it does not accept an unrestricted `PathBuf` at the authoritative boundary.
- Startup validation reads from the same already-open, exclusively locked file handle that will be
  used for append, eliminating validate-by-path/reopen time-of-check/time-of-use gaps.
- Existing journals are scanned under explicit record-count and aggregate-byte budgets before the
  writer accepts new frames.
- A newly created journal header is flushed and synchronized before capture can be reported healthy.
- The containing directory is synchronized after durable file creation and after any future atomic
  rename. A platform that cannot provide that guarantee must expose the limitation explicitly and
  must not claim crash-durable creation.
- Shutdown drains only to its configured deadline, then performs the required flush/synchronization.
  Timeout or synchronization failure leaves the exact source/revision/session/generation capture
  binding incomplete.
- The persistent writer performs synchronous filesystem operations on a supervised dedicated
  thread, not a Tokio core worker. The async control-plane handle may await a completion signal, but
  aborting that handle is not treated as proof that an in-progress blocking sync was cancelled.
- Literal legacy `MEJ1` fixtures remain read-only. New writes use only `MSJ1`; existing legacy data
  is never silently rewritten or shadowed.

## Non-goals

- No filesystem call enters the live decision hot path.
- No durability claim is made for hardware or filesystems that acknowledge synchronization without
  honoring it; the documented local filesystem/OS contract is the software boundary.
- This policy does not require a container, cloud store, database service, or telemetry service.
