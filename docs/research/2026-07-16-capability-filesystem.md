# Capability-based local filesystem decision

Date: 2026-07-16

## Question

How should the platform create controlled artifacts without a check-then-open symlink race or
ambient-path escape on Linux, macOS, and Windows?

## Decision

Use an open directory capability for the controlled artifact root and perform relative opens and
creates through that capability. Reject absolute paths, parent traversal, non-UTF-8 components in
the public artifact-reference format, and platform-specific unsafe path forms before the
capability-relative operation. Use create-new semantics for immutable artifact publication and
retain the directory capability for the lifetime of resolved artifact handles.

`canonicalize` plus a later `std::fs::OpenOptions::open` is not the security boundary: a concurrent
rename or symlink swap can occur between those operations. Canonicalization remains useful only for
diagnostics and initial control-plane setup.

The selected implementation candidate is `cap-std` 4.0.2. Its `Dir` represents an already-open
directory, its file operations are relative to that directory, and the project documents that
relative traversal or symlinks escaping the capability root fail. The implementation supports
Linux, macOS, FreeBSD, and Windows. The crate is permissively licensed and has no default feature
set that must be enabled.

The platform must still test hostile intermediate symlinks and rename/swap behavior. Holding a
capability does not remove the need for explicit filename policy, create-new semantics, output-size
limits, and durable publication rules.

## Primary sources

- [`cap_std::fs::Dir` API](https://docs.rs/cap-std/4.0.2/cap_std/fs/struct.Dir.html)
- [`cap_std::fs` capability model](https://docs.rs/cap-std/4.0.2/cap_std/fs/index.html)
- [`cap-std` 4.0.2 README and platform behavior](https://docs.rs/crate/cap-std/4.0.2/source/README.md)
- [`cap-std` 4.0.2 release metadata](https://docs.rs/crate/cap-std/4.0.2)
- [`cap-primitives` 4.0.2 license and platform dependencies](https://docs.rs/crate/cap-primitives/4.0.2/source/Cargo.toml.orig)
- [Rust `OpenOptions` documentation](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html)
- [`rustix` `openat2` resolve flags](https://docs.rs/rustix/latest/rustix/fs/struct.ResolveFlags.html)

## Rejected alternatives

- Canonicalize, prefix-check, then reopen by ambient absolute path: check/open race remains.
- Linux-only `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` as the sole implementation:
  hardened on Linux but does not satisfy the required macOS/Windows local baseline without a
  separately designed and tested backend.
- A string-only path wrapper: prevents obvious traversal but does not constrain filesystem name
  resolution.
