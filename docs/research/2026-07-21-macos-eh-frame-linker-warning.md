# macOS `__eh_frame` linker warning

**As of:** 2026-07-21

**Last measured update:** 2026-07-23

## Decision

Treat the warning as a narrowly scoped limitation of measured oversized macOS test executables. Do
not change release linking, do not disable compact unwind, and do not suppress linker diagnostics
workspace-wide. Allow `linker_messages` only at an affected test crate root after measuring its
executable and `__eh_frame`,
with a comment linking the open Rust issue. Remove those allowances when Rust or Apple's linker
provides a safe upstream resolution.

## Local evidence

The clean gate ran on macOS 26.5.1 arm64 with Apple `ld` 1267 and Rust 1.97.1 / LLVM 22.1.6.
`otool -l` reports:

| Target | Executable size | `__eh_frame` |
| --- | ---: | ---: |
| `ingest_vertical` | 328,274,176 bytes | 20.11 MiB |
| `market_squawk_data` lib test | 323,676,176 bytes | 19.94 MiB |
| `publication_recovery` | 322,018,376 bytes | 19.68 MiB |
| `control_plane` | 354,986,784 bytes | 21.96 MiB |
| Debug `market-squawk` binary | 538,619,984 bytes | 33.63 MiB |
| Release `market-squawk` | 8,804,400 bytes | 42,024 bytes |

The `control_plane` measurement was added after its existing consolidated harness gained the real
research-dataset-to-backtest authority vertical. Its measured `__eh_frame` is 23,027,100 bytes;
the allowance remains scoped to that test crate root.

The debug application-binary measurement was added after the complete local product composition
linked the analytical and inference dependency graph into the executable built alongside
integration tests. Its measured `__eh_frame` is 35,259,372 bytes. The allowance is restricted to
macOS builds with debug assertions, so release builds continue to surface every linker diagnostic.

The LLVM arm64 Mach-O definition assigns only the low 24 bits of a compact-unwind entry to the
DWARF FDE offset and defines the mask as `0x00FF_FFFF`, or 16 MiB minus one byte. Each affected
test executable exceeds that representational limit; the release executable does not.

Rust 1.97 started forwarding successful-linker output through the warn-by-default
`linker_messages` lint. Therefore the warning became visible after the toolchain update even though
the underlying large test-binary condition was already possible.

## Rejected alternatives

- `panic = "abort"` is already used by the release profile and keeps the shipping binary well
  below the limit. Cargo documents that stable test harnesses ignore this setting and require
  unwinding, so it cannot solve these test links.
- `split-debuginfo`, stripping, and fewer debug symbols do not remove runtime unwind metadata.
  Cargo also defaults debug-enabled macOS profiles to unpacked split debuginfo; the warning occurs
  under that configuration.
- `-Wl,-no_compact_unwind` is not an acceptable workspace fix. It changes macOS unwind behavior,
  can break mixed Rust/C++ exception propagation and crash reporting, and would affect more than
  the measured oversized targets unless introduced through additional build machinery.
- A workspace-wide `linker_messages = "allow"` would hide unrelated future linker warnings.
- Changing optimization or dependency topology solely to silence this diagnostic would alter the
  entire test build and require a separate performance and semantic evaluation.

## Bounded classification

Add `#![allow(linker_messages)]` only to the currently measured affected roots:

1. the `publication_recovery` integration-test root;
2. the `ingest_vertical` integration-test root; and
3. the `market-squawk-data` library only under `cfg(test)`; and
4. the consolidated application `control_plane` integration-test root; and
5. the application binary only for macOS debug-assertion builds.

This does not change generated code, unwind behavior, product behavior, or test coverage. It
classifies five measured diagnostics while every other linker message remains visible.

## Sources

- [Rust 1.97 release: linker output is no longer hidden by default](https://blog.rust-lang.org/2026/07/09/Rust-1.97.0/)
- [Open Rust issue #159105 for this exact macOS warning](https://github.com/rust-lang/rust/issues/159105)
- [LLVM arm64 compact-unwind encoding and 24-bit DWARF offset](https://github.com/llvm/llvm-project/blob/main/libunwind/include/mach-o/compact_unwind_encoding.h)
- [Cargo profile documentation](https://doc.rust-lang.org/stable/cargo/reference/profiles.html)
- [rustc code-generation options](https://doc.rust-lang.org/rustc/codegen-options/index.html)
- [Evidence from Oxen rejecting `-no_compact_unwind` after native exception failures](https://github.com/Oxen-AI/Oxen/blob/6509ed1fd25f191c2cb689fe83e85c0b7ef78f41/Cargo.toml#L27-L33)
