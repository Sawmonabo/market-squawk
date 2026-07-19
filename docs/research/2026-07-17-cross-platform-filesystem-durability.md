# Cross-platform filesystem identity and journal durability

Date: 2026-07-17

Toolchain: Rust 1.97.0 (`rustc 2d8144b78 2026-07-07`)

Dependency anchor: `cap-std`, `cap-primitives`, and `cap-fs-ext` 4.0.2

Incident anchors: GitHub Actions runs
[`29557828946`](https://github.com/Sawmonabo/market-squawk/actions/runs/29557828946) and
[`29559148251`](https://github.com/Sawmonabo/market-squawk/actions/runs/29559148251)

## Decision

Market Squawk uses two distinct filesystem capabilities on Unix when creating a journal:

1. the retained `cap_std::fs::Dir` remains the path-resolution authority; and
2. a capability-relative, read-only `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC` handle to `.` is opened
   solely to synchronize the new directory entry.

The second handle is deliberately opened without `O_PATH`. On Windows, the journal file's
successful `sync_all` is the supported durability boundary and no unsupported directory flush is
attempted. Other non-Unix targets fail closed until an equivalent durability and root-identity
contract is implemented.

Authority-state root identity comparison remains path metadata versus opened `(device, inode)` on
Unix. On Windows, stable Rust does not expose the equivalent volume serial and file index APIs for
`std::fs::Metadata`. The opened root handle is authoritative after
`FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS` opening and handle-derived
reparse/directory validation. Because that handle omits `FILE_SHARE_DELETE`, the code then opens a
second equally validated handle and compares the two cap-fs-ext identities before retaining the
first. The code does not enable nightly merely to inspect the pre-open standard metadata.

These are target-specific contracts, not silently weakened cross-platform approximations.

Writer exclusion is also target-specific while preserving one public contract: exactly one writer
owns a journal, and diagnostic readers may consume already committed bytes while that writer is
active. Unix retains its advisory exclusive lock on the journal file. Windows opens the writer
handle with `FILE_SHARE_READ` only, which atomically denies a competing write-capable open without
locking the journal's readable byte range. Windows also requests `FILE_FLAG_OPEN_REPARSE_POINT`
and rejects a non-regular or reparse handle after opening it.

## Failure evidence

The first hosted run exposed two independent failures.

### Linux: `EBADF` during journal-directory synchronization

The `verify` job failed while constructing a new journal:

```text
failed to synchronize journal directory handle: Bad file descriptor (os error 9)
```

The failing implementation cloned the `cap_std::fs::Dir` returned by `open_dir` and called
`File::sync_all` on that clone. This works on macOS because the directory capability is an ordinary
readable descriptor there. It fails on Linux because cap-primitives 4.0.2 deliberately uses
`O_PATH` for directory capabilities when directory-entry reads are not requested.

This is not a transient runner failure. The pinned cap-primitives source defines ordinary
directory options with `dir_required(true)` and maps that combination to `O_PATH` on Linux:

- [cap-primitives 4.0.2 `dir_utils.rs`](https://github.com/bytecodealliance/cap-std/blob/v4.0.2/cap-primitives/src/rustix/fs/dir_utils.rs)
- [cap-primitives 4.0.2 `oflags.rs`](https://github.com/bytecodealliance/cap-std/blob/v4.0.2/cap-primitives/src/rustix/fs/oflags.rs)
- [cap-primitives 4.0.2 `open_dir.rs`](https://github.com/bytecodealliance/cap-std/blob/v4.0.2/cap-primitives/src/fs/open_dir.rs)

Linux documents that an `O_PATH` descriptor supports a deliberately restricted operation set;
operations outside that set fail with `EBADF`. It remains valid as an `openat` directory anchor,
which is why all earlier capability-relative file operations succeeded before the later `fsync`
failed. See [`open(2)`](https://man7.org/linux/man-pages/man2/open.2.html).

### Windows: unavailable stable metadata identity methods

The Windows job failed at compile time:

```text
no method named `dev` found for struct `std::fs::Metadata`
no method named `ino` found for struct `std::fs::Metadata`
```

`cap-fs-ext::MetadataExt` implements these operations for `std::fs::Metadata` on Unix. Its Windows
implementation is conditional on Rust's `windows_by_handle` configuration, because the standard
library's `volume_serial_number`, `file_index`, and related methods are still experimental in Rust
1.97. The official
[`std::os::windows::fs::MetadataExt`](https://doc.rust-lang.org/1.97.0/std/os/windows/fs/trait.MetadataExt.html)
documentation marks those methods nightly-only.

Calls on the pre-open `std::fs::Metadata` must therefore be compiled only on Unix. The
`cap_fs_ext::MetadataExt` trait itself remains imported on Windows because cap-derived metadata
uses it for stable device/file identity and hard-link counts. This does not leave a Windows symlink
or junction gap: the Windows opener requests `FILE_FLAG_OPEN_REPARSE_POINT`, validates
`file.metadata()` from each opened handle with the stable `file_attributes` API, and compares the
two opened identities while the retained handle prevents deletion or replacement. Microsoft
documents `FILE_FLAG_BACKUP_SEMANTICS` as the required flag for obtaining a directory handle:
[Directory Handles](https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory).

### Windows: an exclusive byte-range lock also denied diagnostic reads

The second hosted run compiled successfully on Windows but failed two journal tests. A competing
writer did not map to the stable `AlreadyLocked` result, and a reader could not inspect a flushed
journal while its writer remained active:

```text
The process cannot access the file because another process has locked a portion of the file.
(os error 33)
```

This was a production contract defect rather than a test-only difference. Microsoft's
[`LockFileEx`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)
documentation states that an exclusive region lock denies other processes both read and write
access to that region. The pinned `fs2` implementation requests an exclusive lock covering the
complete range, so it cannot provide single-writer/multiple-reader behavior on Windows.

Windows writer exclusion therefore moves to the handle-open share contract. The writer requests
read and append access but shares only reads. A second writer's requested write access is rejected
atomically with `ERROR_SHARING_VIOLATION`, while an ordinary read handle remains compatible. Rust's
stable
[`OpenOptionsExt::share_mode`](https://doc.rust-lang.org/1.97.0/std/os/windows/fs/trait.OpenOptionsExt.html#tymethod.share_mode)
exposes this `CreateFile` share-mode contract without unsafe code. The code also recognizes
`ERROR_LOCK_VIOLATION` as contention for compatibility with an older process that still holds the
former byte-range lock.

A persistent sidecar lock was rejected because removing and recreating its pathname while a handle
remained open could split writer ownership across two filesystem objects. The journal's retained
Windows handle instead makes ownership inseparable from the exact data file and omits delete
sharing for its lifetime.

## Why a separate Unix synchronization handle is required

The retained `Dir` and synchronization handle serve different purposes:

| Handle | Required behavior | Linux opening mode |
|---|---|---|
| Retained directory capability | Confined relative resolution, metadata, rename, and endpoint validation | May be `O_PATH` |
| Parent-directory sync handle | Synchronize creation of the journal directory entry | `O_RDONLY | O_DIRECTORY`, never `O_PATH` |

Replacing the retained capability with an ambient path reopen would reintroduce substitution risk.
Cloning the capability preserves the unsuitable `O_PATH` status flags. Reopening `.` through the
retained capability gives the new descriptor the same directory identity and confinement while
allowing explicitly selected access flags.

The reopen uses:

```text
relative path: .
access: read-only
flags: O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC
root: retained journal capability
```

It therefore neither follows an ambient pathname nor grants write access to arbitrary files.

## Non-Unix policy

The code represents directory-entry synchronization with a target-specific
`ParentDirectorySync` type:

- Unix instances must contain a real syncable directory file and `synchronize()` calls
  `File::sync_all`.
- Windows instances are an explicit zero-sized policy witness. Journal creation has already
  flushed the exact journal file handle with `File::sync_all`; no directory flush is attempted.
- Other non-Unix targets return typed unsupported errors rather than inheriting Windows behavior.

Microsoft's
[`FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
contract requires an open file handle with write access. A read-only Windows directory handle is
not an interchangeable substitute. The code must not ignore an expected directory-flush failure
or pretend it supplied Unix `fsync(directory)` semantics.

## Windows catalog-backup publication

Date revalidated: 2026-07-18

The catalog-backup protocol has a different operation boundary from creation of an append-only
journal. It first creates and flushes a complete SQLite backup in the destination directory and
then publishes that prepared file under its final, caller-selected name. On Windows, publication
must use `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` and without
`MOVEFILE_REPLACE_EXISTING`. Microsoft specifies that the write-through flag does not return until
the file is actually moved on disk; omitting the replacement flag preserves the backup API's
no-clobber contract. The prepared file and final name are children of the same retained destination
directory, so the protocol does not opt into cross-volume copy behavior.

This is a Market Squawk design inference from the operating-system contracts, not a claim that a
read-only Windows directory handle can be synchronized. Microsoft documents that
[`FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
requires `GENERIC_WRITE`, while its supported
[`Directory Handles`](https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory)
operations do not include that function. The publication primitive and its durability semantics
are documented by
[`MoveFileExW`](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw).

The selected safe Rust boundary is the exact-pinned MIT-licensed `atomicwrites` 0.4.4
[`move_atomic`](https://docs.rs/crate/atomicwrites/0.4.4/source/src/lib.rs) function, admitted only
for Windows builds of the data and platform crates. Its audited Windows implementation accepts
`Path` values and invokes `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` alone. This preserves Windows
path encoding, keeps Market Squawk's workspace-wide `unsafe_code = "forbid"` policy intact, and
avoids exposing a general Win32 API surface. The wrapper owns its FFI safety boundary; Market
Squawk still owns every domain-specific authority, endpoint, close-before-publish, identity, and
recovery check.

Neither Microsoft's `MoveFileExW` page nor the safe wrapper is treated as a formal guarantee of
atomic visibility for every Windows filesystem and filter-driver combination. Market Squawk claims
the narrower documented contract: same-volume no-clobber publication that returns only after the
requested write-through move completes. A published backup is usable only after exact-file
identity, the exact size and SHA-256 in its pathless `BackupReceipt`, and SQLite integrity
validation. Backup verification requires that receipt and repeats the root/file identity, size,
digest, application identity, migration identity, foreign-key, and SQLite integrity checks before
the artifact can be used for restore. `ReplaceFileW` is not a substitute because this API
deliberately publishes to a previously absent destination rather than overwriting an existing file.
The wrapper's unresolved
[`MoveFileEx` atomicity issue](https://github.com/untitaker/rust-atomicwrites/issues/27) is therefore
tracked as a reason not to claim a stronger guarantee.

The exact wrapper release has no declared MSRV, its last release and repository code activity were
in 2024, and its upstream CI does not exercise Windows. It is admitted despite that maintenance
gap because its small exact-pinned safe API is the only reviewed option that preserves arbitrary
Windows `Path` values and the required flags without weakening the unsafe-code policy. The more
active `winsafe` 0.0.28 wrapper accepts only UTF-8 `&str` paths. Exact Rust 1.97.1 Windows build
evidence is therefore a release gate for this dependency. The ambient-path call also cannot close
the hostile same-user parent-substitution race by itself; the retained root capability,
cross-process writer exclusion, immediate root/endpoint revalidation, unpredictable prepared name,
and fail-closed receipt verification bound that residual risk. This primitive remains prohibited
for in-place replacement of a sole authoritative catalog, secret vault, or control-plane state
file.

The Windows implementation must close every write-capable SQLite and prepared-file handle before
publication, retain the destination writer exclusion for the whole operation, reject reparse or
non-regular endpoints, and validate that the published file is the prepared file. It must not
substitute a best-effort `rename`, hard-link sequence, ignored directory-flush error, or overwrite
operation while advertising the same durability and no-clobber contract. Unix retains its
same-directory publication followed by a real parent-directory synchronization.

### Windows authority-state generations

The encrypted secret fallback and other authority-state consumers use a stricter retained-
generation protocol. A single `replace_atomic` call is not accepted as proof for authoritative
state: it adds `MOVEFILE_REPLACE_EXISTING` and `MOVEFILE_WRITE_THROUGH`, but neither Microsoft nor
the wrapper establishes atomic visibility on every Windows filesystem/filter combination. Losing
the sole old name before post-publication verification would make recovery depend on precisely the
stronger guarantee Market Squawk does not claim.

The authority store therefore retains two bounded immutable slots. Each authenticated envelope
binds its generation, payload digest, and predecessor identity. For every acknowledged logical
update, the store writes and synchronizes a same-directory temporary candidate, installs and
reopens it in the inactive slot while the other known-good slot survives, then installs and verifies
the same submitted logical payload as a new linked generation in the former active slot. Success is
returned only when both slots hold that logical payload. First creation uses no-clobber
`move_atomic`; replacement is permitted only for the inactive slot while the other verified slot is
retained, so replacement atomicity is never the recovery proof. Opening the store scans only those
fixed slots and selects the highest valid linked generation; a corrupt, torn, or missing slot does
not erase the other valid generation. An interrupted second installation returns a typed recovery-
required outcome and latches ordinary reads/writes until the verified newer slot repairs its peer.
Startup rejects unrelated valid heads, non-successor generations, generation overflow, unsafe
entries, and every ambiguous chain. Loads, stores, recovery, and cleanup share the store's lifetime
lock and in-process serialization so an intermediate publication is never treated as a completed
authority transition.

The vault adds a keyed whole-context authenticator covering its version, phase, set roles, and full
canonical entry membership, as well as the sealed authority generation and predecessor supplied
before vault serialization. Prepared and committed states carry independently verifiable context
tags under both permitted unlocks. Per-entry AEAD plus an unkeyed envelope digest cannot detect
entry deletion, cross-phase substitution, or replayed membership under the same unlock. A hostile
actor who can replace both slots with an older internally valid pair is outside the rollback
guarantee of a local-files-only store; preventing that requires an independently protected monotonic
anchor. Ordinary corruption or loss of either slot remains recoverable without such an external
service.

`RotationOutcome::Complete` is returned only after the final stable vault has been durably installed
and authenticated in both slots, so no valid retained generation still contains prior-unlock
recovery ciphertext. Failure after the first stable slot is admitted remains
`RotationFinalizationPending`; recovery uses the highest valid generation and completes retirement
before the prior unlock may be discarded. This protocol uses
`atomicwrites::move_atomic` only as a lossless-path, no-clobber, write-through publication primitive,
not as an unsupported atomic-visibility claim.

## Authority-state root safety

The root-open sequence remains fail-closed:

1. inspect or create the configured root;
2. reject a non-directory, final symlink, or Windows reparse point;
3. open without following the final component;
4. inspect the opened handle;
5. on Unix, compare the opened `(device, inode)` with the inspected path identity;
6. on Windows, reject an opened reparse point or non-directory, open a second equally validated
   handle while deletion/replacement sharing is denied, and require identical cap-fs-ext identity;
7. retain only the opened capability for later operations.

After step 7, path replacement cannot redirect state access because all reserved-name operations
are relative to the retained handle. Enabling nightly for the Windows pre-open identity fields was
rejected: the project baseline requires stable Rust, and the opened no-follow handle is already the
correct authority boundary.

## Verification contract

The regression is covered at three levels:

- platform tests create new journals, which exercises parent-directory synchronization on Unix;
- platform and application tests keep a writer active while reading its flushed committed records
  and separately require a typed rejection of a second writer;
- the application deadline test opens the exact journal path that failed in hosted Linux CI;
- GitHub Actions builds and tests Linux, macOS, and Windows with the corrected Rust 1.97.1
  production baseline. The 1.97.0 toolchain recorded at the top of this research remains the
  historical environment in which the filesystem investigation was conducted, not valid current
  release evidence.

Required focused commands:

```bash
cargo test -p market-squawk-platform --all-targets --all-features --locked
cargo clippy -p market-squawk-platform --all-targets --all-features --locked -- -D warnings
cargo test -p market-squawk --bin market-squawk \
  tests::application_deadline_reaps_source_then_event_and_capture_workers \
  --locked -- --exact --nocapture
```

Hosted success is evidence for the target-specific branches; it does not replace the local default
suite or make GitHub a mandatory runtime dependency.

## Revalidation triggers

Re-audit this decision when any of the following changes:

- `cap-std`, `cap-primitives`, or `cap-fs-ext` version;
- `atomicwrites` version or Windows `move_atomic` implementation;
- Rust stabilizes the Windows by-handle metadata APIs;
- journal creation or rename durability protocol;
- catalog-backup preparation, no-clobber publication, or Windows move flags;
- journal writer exclusion, Windows share modes, or concurrent diagnostic reads;
- supported operating-system targets;
- capability-root construction or final-component no-follow policy.
