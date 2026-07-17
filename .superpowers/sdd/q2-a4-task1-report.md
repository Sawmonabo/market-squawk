# Q2 A4 Task 1 implementation report

Audit/base head: `e63807db03614fdfea64e85d4afc252bcea347ac`

Status: implementation candidate is intentionally uncommitted pending the full Step-3 seed gates
and independent review. No performance baseline has been run and no performance claim is made.

All earlier Rust 1.97.0 command results and layout assumptions are historical and non-authoritative.
The binding production baseline is Rust 1.97.1; A4 cannot be frozen or approved until its complete
evidence set and private-layout validation have been regenerated at an unchanged exact 1.97.1 head.

The earlier standard-channel result and current preparer/host bundle are diagnostic only. They are
not frozen baseline or approval evidence. Final performance evidence will be a paired
standard-versus-ring run from the same clean exact Q2 candidate under the bounded measurement trust
model recorded in the active plan and project memory; no hostile same-UID or reproducible-build
attestation is claimed.

## Barriers and execution decision

- The linked worktree was clean at the base and `./scripts/verify.sh` exited 0 before mutation.
- Bootstrap classifier tests, domain/platform checks, locked metadata, and the pinned Criterion
  0.8.2 graph passed before production edits.
- The original Step 1 text attempted to run new trybuild fixtures before reviewed `.stderr` files
  existed, while the binding RED classifier correctly rejects trybuild `wip/` output. The lane kept
  the classifier fail-closed: it first classified a purpose-built compiler/runtime RED, generated
  each omission fixture separately, inspected the exact missing-method error, accepted the stderr,
  then required the complete compile-fail suite to pass.

## Implemented queue-independent authority contracts

- Added the dependency-neutral checked `Arc` retained-layout helper in
  `market-squawk-domain::retained` and removed the sources-private duplicate model.
- Added the private empty/shared `CapturePayload` representation over a normalized `Arc<[u8]>`,
  exact 4 MiB live and 33,554,431-byte committed-wire policies, checked retained allocation size,
  allocation identity proof, and preserved Serde wire compatibility.
- Added mandatory no-default capture payload, footprint, resident-shared, bundle-retained-size,
  and concrete receipt contracts. Compile-fail fixtures ensure implementers cannot omit them.
- Added the opaque, non-cloneable `CaptureResidentGenerationLease` and concrete receipt proof. The
  platform moves a lease from the exact active generation allocation into receipt issuance, then
  verifies allocation identity and zero unreserved receipt-owned dynamic bytes before returning.
- Migrated production source, diagnostic, live, and test bundle implementations exhaustively. A
  compatibility-only payload cannot enter a live frame; generic frame-to-record conversion preserves
  the exact payload allocation rather than making an implicit copy.
- Split platform capture admission into the focused `capture/admission.rs` module while preserving
  the existing standard bounded channel and public lifecycle behavior.
- Closed two lifecycle ripple defects found during the migration: shutdown receives a fresh bounded
  deadline, and permit-drop/coordinator failure accounting remains terminal and reconciled.
- Closed a benchmark-only flush-finalization race found by the complete all-target gate. A
  flush-inclusive append makes record accounting visible before its policy flush releases the
  capacity permit; finalization now waits for both exact record accounting and complete permit
  return before evaluating terminal invariants. A controlled flush gate makes that ordering
  deterministic in regression coverage, and engineering failure output names the exact operation
  and Criterion iteration count.

## Production benchmark seam and fixed evidence target

- Added the opt-in production support feature `capture-benchmark`. The name follows the official
  Clippy guidance at <https://rust-lang.github.io/rust-clippy/master/#redundant_feature_names>.
- Added five separately named production-path seams: queue push, the one-owner queue pop with
  producer fan-in, capture admission, capture-writer dispatch into a benchmark-only observer sink,
  and dispatch through that observer sink's policy-flush callback. The writer cases exercise the
  production capture-writer scheduling/dispatch path but perform no journal serialization, file
  write, or `fsync`; they have zero durable-I/O performance authority.
- Queue/admission depth cases are exactly `1`, `64`, and `16_384`; writer cases use depth `64` only.
  Payloads are exactly `0`, `1_024`, and `4_194_304` bytes. The 0/1,024-byte cells require checked
  `max(1_000_000, producers * 100_000)` operations; each 4 MiB stress cell requires 10,000. Every
  cell requires the same exact latency-observation count as its operation quota. Configured depth
  and the exact production effective-capacity quote are recorded separately.
- The named latency interval excludes permit acquisition and barrier wait; the overall case timer
  includes thread, barrier, and capacity wait. Writer observations are recorded in the real
  single-writer append/flush order.
- Sustained memory evidence uses the unthrottled real production offered-load queue and requires
  both accepted and refused offers. RSS now comes from the in-process `memory-stats` 1.2.0 adapter,
  whose documented physical-memory value is RSS in bytes on Linux/macOS; repeated `ps` subprocesses
  are gone. Each typed sample records epoch, target offset, observed monotonic offset, and bytes.
  Missed 100 ms deadlines outside a fixed 25 ms tolerance fail rather than being backfilled.
- `capture_admission_evidence` is the only authoritative fixed-quota target.
  `capture_admission_criterion` uses Criterion's adaptive API for engineering exploration but is
  permanently labeled `exploratory_zero_authority`; it can neither create nor compare baselines.

## Build and host evidence integrity

- Cargo's built-in bench profile inherits release, per
  <https://doc.rust-lang.org/cargo/reference/profiles.html>. Evidence uses the exact label
  `cargo-bench-inherits-release:opt-level=3:lto=thin:codegen-units=1:panic=abort:strip=symbols`.
- The build-evidence preparer owns the exact Cargo command, binds canonical Cargo and Git
  executable paths/digests, rejects ambient `CC`/`CXX`/`AR`, discovered Cargo config, and
  compiler/profile/target override surfaces, requires clean Git,
  captures bounded Cargo JSON, validates the direct artifact and embedded/current source bindings,
  and transactionally no-clobber publishes Cargo JSON, the executable, and build evidence.
- Captured subprocesses enforce stdout/stderr limits while the child is running. Timeout or output
  overflow terminates the entire new-session process group. No unbounded temporary output file is
  used. The host and build command collectors share this bounded primitive.
- `build.rs` uses bounded concurrent stdout/stderr draining, bounded descriptor hashing, and a
  depth/count-limited symlink-rejecting source walk. It binds the clean full Git object ID, exact source inventory and trees, lock/workspace/
  package manifests, entry/backend/criterion/immutable modules, build script, host-gate tools,
  preparer/build helper, Cargo/Git digests, exact command digest, sanitized environment digest, and
  environment policy. The complete real rustc/linker/SDK closure is not yet implemented, so no
  closed-toolchain or authoritative-build claim is made.
- Build bundle publication validates before mutation and rolls back only exact inode-bound files
  created by the invocation if any publication boundary fails. Fixture evidence carries an extra
  `test_fixture` member and therefore cannot deserialize as authoritative Rust `BuildEvidence`.
- The host gate requires descriptor-relative no-follow primitives, private ownership and modes,
  exact bounded read/write loops, stable fd/path identity, single-link files, duplicate-free exact
  JSON schemas, fsync publication, and no-clobber outputs.
- `measure` owns preflight, five repetitions, continuous bounded monitoring, and postflight under one
  lock. It executes a private mode-0500 evidence-local copy made from the verified runner, while the
  original runner, execution copy, and build evidence are bound by device/inode/size/digest before,
  during, and after each repetition. The persisted no-other-agent attestation is the explicit
  residual same-UID boundary; periodic pathname checks are not claimed to be mathematically
  race-free.
- Release requires the exact strict preflight ticket plus caller-supplied lock, owner, and nonce
  identity; it verifies the lock contains only its owner before unlink and preserves the lock on any
  mismatch.
- Finalization rejects unknown JSON fields, non-lowercase hashes, abbreviated Git IDs, wrong
  configured/effective capacity, inexact throughput, any matrix quota deviation, a comparable-full
  count other than one, sustained elapsed/sample violations, extra/missing root or host artifacts,
  or any `CAPTURE_BENCH_FINALIZE_ONLY` value other than `1`. The no-clobber manifest binds ordered
  repetition, artifact, tool, host, build, and source digests and is self-read into the typed schema
  before success.

## Deterministic hostile test inventory

- Host gate: one exact-cardinality inventory executes 110 independently named subtests covering
  schema/type/boundary errors, redaction, load/clock/host/toolchain drift, competing processes,
  symlink/hardlink/mode/root/output/attestation/owner replacement, partial I/O, fsync/interruption,
  caller-bound release, unexpected lock contents, monitored-runner failures, and runner/build
  replace-and-restore attacks.
- Closed Cargo process: eight named policy cases cover exact argv/environment and EOF stdin,
  nonzero status, empty output, finite and sustained stdout overflow, stderr overflow, timeout with
  child process-group cleanup, and missing Cargo.
- Transactional publication: five named CLI boundary failures cover post-runner validation,
  post-current-tree validation, and all three publication points; each proves a seeded run directory
  remains byte-for-byte unchanged. Separate CLI fixtures prove success/no-clobber and early failed
  Cargo publishes nothing.

## Current non-authoritative verification

- `cargo check -p market-squawk-platform --all-targets --all-features --locked`: passed after the
  timing/RSS/build-binding changes.
- `cargo test -p market-squawk-platform --lib --all-features --locked
  capture::benchmark_support -- --nocapture`: 7 passed, including immutable late-start and delayed
  permit-exclusion regressions.
- `cargo test -p market-squawk-platform --lib --locked raw_record::tests -- --nocapture`: 9 passed
  after exact compatibility-body and constant-time live-bound validation.
- `./scripts/tests/test_assert_expected_red.sh`: passed with per-header symbol attribution and
  compiler-termination rejection.
- `cargo test -p market-squawk-platform --test build_support_bounds --all-features --locked`: 3
  passed before the follow-up descendant-PID assertion was added; that strengthened test still
  requires a fresh run. No earlier broad Python/Rust counts are carried forward as current proof.

## Remaining seed barrier

1. Close the real pinned Cargo/rustc/linker/SDK toolchain, Git configuration/loader, and immutable
   execution-copy contracts; complete the independently anchored baseline lock/report validator.
2. Split and separately bind the oversized preparer, benchmark schema, and capture-authority bridge
   test modules; add bounded property proof for compatibility JSON sizing.
3. Run the complete workspace format, test, Clippy, build, benchmark no-run, script, brand,
   documentation, and generated-artifact gates against the unchanged dirty candidate.
4. Obtain independent review of the exact diff. Resolve every substantiated Critical, Important, or
   Minor finding and rerun all affected gates.
5. Only after review may the lane commit. A clean exact-head authoritative build/measurement is a
   later explicit barrier; this report does not authorize or claim it.
