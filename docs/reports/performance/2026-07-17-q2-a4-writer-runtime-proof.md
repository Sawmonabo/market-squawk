# Q2 A4 writer-runtime retained-memory proof

Status: implementation proof input, formula revision 1  
Date: 2026-07-17  
Rust release: 1.97.1  
Rust commit: `8bab26f4f68e0e26f0bb7960be334d5b520ea452`

## Scope and trust boundary

This artifact bounds Rust-visible heap storage created by one Market Squawk capture-writer start.
It does not claim to bound the native stack, kernel thread object, platform handle bookkeeping,
allocator metadata, fragmentation, or resident set size. Those terms require the separately
supervised RSS evidence gate.

The implementation accepts these compiled targets:

```text
aarch64-apple-darwin
x86_64-apple-darwin
aarch64-unknown-linux-gnu
x86_64-unknown-linux-gnu
aarch64-pc-windows-msvc
x86_64-pc-windows-msvc
```

Any other compiled target fails writer start with `WriterRuntimeProofError::CompiledTargetMismatch`.

## Primary-source basis

The pinned standard-library implementation is recorded at the exact compiler commit:

- [thread builder and `Builder::spawn_unchecked`](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/thread/builder.rs)
- [spawn packet, closure box, `ThreadInit`, and `JoinInner`](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/thread/lifecycle.rs)
- [`Thread` and its system-allocator-backed inner allocation](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/thread/thread.rs)
- [`JoinHandle`](https://github.com/rust-lang/rust/blob/8bab26f4f68e0e26f0bb7960be334d5b520ea452/library/std/src/thread/join_handle.rs)

At that revision, spawning creates a shared packet allocation, a boxed Rust entry closure, a boxed
`ThreadInit`, a system-allocator-backed `Thread::Inner`, a bounded thread-name allocation, and the
returned `JoinInner`/`JoinHandle` control value. Market Squawk separately charges its concrete
closure capture, destination lease, scratch allocations, lifecycle core, and fixed-reservation
owner. The source-derived private-standard-library allowance is 16,384 bytes per writer. This is
deliberately larger than the enumerated fixed private headers and control blocks on every supported
64-bit target; any Rust/toolchain or target change invalidates formula revision 1 and requires a new
artifact, hash, type-size fixture, and review.

## Formula revision 1

```text
writer_start_fixed_bytes =
    source_scratch.capacity()
  + generation_scratch.capacity()
  + event_scratch.capacity()
  + complete Arc<CaptureDestinationLease> allocation
  + complete Arc<WriterFixedStorageOwner> allocation
  + bounded builder/C-string thread-name bytes
  + concrete Market Squawk spawn-closure capture bytes
  + 16,384-byte pinned standard-library private-runtime allowance
```

`source_scratch`, `generation_scratch`, and `event_scratch` are prepared with
`Vec::try_reserve_exact`, then charged at allocator-observed capacity. Their logical maxima derive
from `SourceId::MAX_LENGTH`, both `SourceIdentifier::MAX_LENGTH` values in the authority identity,
length prefixes, the connection generation, one UUID, and one frame ordinal. They never grow after
writer start.

The channel accounts its coalesced `WriterLifecycleCore` at channel construction. Sink-retained
storage is governed by the sink's distinct ledger; the closure-capture term nevertheless includes
the concrete inline `S` value because the standard library physically boxes that capture during
spawn. Conservative overlap between independently enforced ledgers is accepted; omission is not.

## Current-target fixture

The formula fixture was compiled directly with Rust 1.97.1 on
`aarch64-apple-darwin` and recorded:

```text
std::thread::Thread                 8
std::thread::JoinHandle<()>        24
std::thread::Builder               48
String                             24
Arc<()>                            8
Box<dyn FnOnce() + Send>           16
```

The implementation does not substitute these public type sizes for the private allocation
allowance. It stores the compiled-target identifier, formula revision, and SHA-256 of this complete
artifact in every `WriterFixedStorageReceipt`, and validates them before reserving channel memory or
publishing a running writer.
