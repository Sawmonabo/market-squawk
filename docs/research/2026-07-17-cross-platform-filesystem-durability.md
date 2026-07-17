# Cross-platform filesystem identity and journal durability

Date: 2026-07-17

Toolchain: Rust 1.97.0 (`rustc 2d8144b78 2026-07-07`)

Dependency anchor: `cap-std`, `cap-primitives`, and `cap-fs-ext` 4.0.2

Incident anchor: GitHub Actions run
[`29557828946`](https://github.com/Sawmonabo/market-squawk/actions/runs/29557828946)

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
- the application deadline test opens the exact journal path that failed in hosted Linux CI;
- GitHub Actions builds and tests Linux, macOS, and Windows with Rust 1.97.0.

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
- Rust stabilizes the Windows by-handle metadata APIs;
- journal creation or rename durability protocol;
- supported operating-system targets;
- capability-root construction or final-component no-follow policy.
