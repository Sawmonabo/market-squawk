# Local Authority-State Durability Research

## Document control

- Researched: 2026-07-16
- Applies to: Q2-I03 provider-budget restart persistence
- Implementation base: Q2 remediation plan commit `de101ee`
- Platform-store implementation: `61c4292`
- Source policy: primary standards, Rust 1.97 documentation, Microsoft platform documentation,
  and the exact pinned `cap-std`/`cap-fs-ext` 4.0.2 API documentation

## Decision

The provider-budget authority store uses a bounded, versioned, integrity-protected envelope in a
path-confined directory. One writer holds an interprocess lock. A commit writes a same-directory
temporary file, checks every write/close-relevant error through `sync_all`, atomically replaces the
canonical name without a delete-then-rename fallback, and synchronizes the containing directory
where the platform exposes that operation. Startup never treats a leftover temporary file as
committed authority. A missing, corrupt, ambiguous, future-dated, or rollback-inconsistent canonical
file fails closed instead of starting with a fresh quota.

This store is outside the market event-to-action path. Restrictive in-memory transitions remain
restrictive if persistence fails; an availability-increasing transition is not published until its
checkpoint is durable enough for the supported platform contract.

## Evidence

### Rust 1.97 file synchronization and rename

Rust documents that dropping a `File` ignores close errors and directs callers that need those
errors handled to `File::sync_all`. Its implementation maps `sync_all` to the platform file-sync
operation. Therefore a plain write followed by drop is insufficient for an authority checkpoint.

Rust's `rename` replaces an existing destination, requires source and destination to remain on the
same mount, maps to `rename` on Unix and Windows rename primitives on Windows, and documents
platform differences when the destination exists. The implementation must keep the temporary file
in the same confined directory and must test replacement on every supported target rather than
assuming Unix behavior on Windows.

Sources:

- [Rust 1.97 `File::sync_all` source and contract](https://doc.rust-lang.org/1.97.0/src/std/fs.rs.html#779-782)
- [Rust 1.97 `std::fs::rename`](https://doc.rust-lang.org/1.97.0/std/fs/fn.rename.html)

### POSIX atomicity versus durability

POSIX requires a replacing rename to keep either the old or new directory entry visible throughout
the operation. That is namespace atomicity, not by itself a durability guarantee. The POSIX
rationale explicitly describes the temporary-file, file-sync, rename sequence and says a directory
must also be synchronized when the application needs certainty that the new name is durable after a
crash.

The Q2 implementation therefore must not equate atomic rename with completed durable publication.
It syncs the file before replacement and the directory after replacement on supported Unix targets.
Any sync error is a typed persistence failure and cannot increase provider availability.

Sources:

- [POSIX.1-2024 `rename` specification](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html)
- [POSIX.1-2024 filesystem-cache rationale](https://pubs.opengroup.org/onlinepubs/9799919799/xrat/V4_xbd_chap01.html)
- [Linux `fsync(2)` documentation](https://man7.org/linux/man-pages/man2/fsync.2.html)

### Windows flush and replacement behavior

Microsoft documents `FlushFileBuffers` as the operation that writes buffered file data to the
device. `ReplaceFileW` performs a combined replacement, requires the files to be on the same volume,
and documents several failure outcomes in which both names can remain or replacement state can be
partially rearranged. Its nominal write-through flag is explicitly unsupported.

Consequences for the portable Rust implementation:

- sync the complete temporary file before replacement;
- never implement a fallback that first deletes the canonical state;
- retain and classify replacement errors;
- after a clean successful replacement, reopen/validate the canonical bounded envelope;
- on startup, accept only the canonical, fully validated envelope while holding the writer lock;
  and
- fail closed when the canonical state is absent or ambiguous rather than adopting an orphan temp
  as quota authority.

This Q2 contract proves clean process restart and fail-closed interrupted-write recovery. It does
not make an unqualified claim that every filesystem/storage controller survives arbitrary power
loss; the exact platform sync results remain part of the persistence outcome.

Sources:

- [Microsoft `FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
- [Microsoft `ReplaceFileW`](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-replacefilew)

### Capability-confined paths

The pinned `cap-std` API represents an already-open directory and performs open/rename operations
relative to that directory. This is the correct boundary for preventing the persisted state key or
temporary filename from becoming ambient arbitrary-filesystem authority. Its documentation also
calls out a Windows sharing-mode constraint when constructing a `Dir` from a standard file, so the
implementation should use the repository's established confined-directory constructors rather than
reconstructing one from an arbitrary handle.

Source:

- [`cap-std` 4.0.2 `Dir` API](https://docs.rs/cap-std/4.0.2/cap_std/fs/struct.Dir.html)

`cap-std` deliberately keeps final-component symbolic-link policy below its public convenience
surface. The companion `cap-fs-ext` crate exposes that existing capability through
`OpenOptionsFollowExt::follow`. The store pins the matching 4.0.2 versions and sets
`FollowSymlinks::No` for lock, temporary, and canonical opens. It additionally rejects
non-regular and multiply linked reserved entries, compares the opened root handle's filesystem
identity with the no-follow pre-open identity, and changes root permissions through the opened
handle rather than through a second ambient path lookup.

Sources:

- [`cap-fs-ext` 4.0.2 crate documentation](https://docs.rs/cap-fs-ext/4.0.2/cap_fs_ext/)
- [`OpenOptionsFollowExt` 4.0.2](https://docs.rs/cap-fs-ext/4.0.2/cap_fs_ext/trait.OpenOptionsFollowExt.html)

### Implemented platform boundary

The implemented store bounds payloads at 8 MiB and uses an 8-byte magic, 16-bit format version,
64-bit payload length, SHA-256 digest, and payload envelope. It validates the filesystem-reported
length before allocation, probes for concurrent trailing growth, zeroes transient payload copies,
serializes writers inside a process, and retains an exclusive filesystem lock across the store
lifetime. On Unix, installation is same-directory replacement followed by directory sync and a
canonical reopen/digest/payload verification.

The workspace forbids unsafe Rust. The pinned safe capability APIs do not expose Windows'
`ReplaceFileW`; ordinary Windows rename does not provide a portable replace-existing contract.
Consequently the store supports first installation on Windows but returns a typed, fail-closed
`AtomicReplaceUnsupported` error when a canonical file already exists. It never deletes the old
file to simulate replacement. Full Windows replacement support requires a separately reviewed safe
platform boundary; no Windows durability result is claimed by this macOS implementation run.

## Required regression matrix

- bounded exact-envelope read and maximum-plus-one rejection;
- exclusive writer-lock contention across separate handles/processes;
- canonical replacement when no prior state exists and when valid prior state exists;
- write, sync, rename, canonical reopen, and directory-sync failure injection;
- crash/interruption artifacts before sync, after file sync, and around rename;
- corrupt/truncated/digest-mismatched canonical state;
- canonical plus orphan-temp state, where only canonical may be restored;
- no canonical plus orphan-temp state, which must fail closed;
- canonical serialization equality across map insertion order;
- clean subprocess restart preserving window use, in-flight conservative use, cooldown, refusal,
  disabled/terminal state, and availability generation; and
- deterministic tests on Linux, macOS, and Windows runners without claiming hosted results that
  were not observed.
