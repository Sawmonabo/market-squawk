# Quarter 2 Task 5/6 Capture Bridge Report

Date: 2026-07-16

Branch: `feat/q2-capture-bridge`

Task 5 source commit integrated locally: `4bade95`

Primary bridge commit: `c54dbdc`

Source compact-policy follow-up: `87a2d95`

Source routing/evidence follow-up: `0d8de19`

## Result

The source-owned capture authority from Task 5 is now connected to the Task 6 platform capture
queue and writer without moving authority into the platform or introducing a production dependency
from `market-squawk-platform` to `market-squawk-sources`.

The bridge is generic over one complete [`CaptureAuthorityBundle`]. Its associated frame and
receipt types remain linked through the type system. With the production source bundle, the queue
stores the exact `market_squawk_sources::RawMarketFrame` and successful publication returns the
exact non-clone, non-Serde `market_squawk_sources::CaptureAdmissionReceipt`. There is no trait-object
frame erasure, platform receipt surrogate, loose capability constructor, or receipt-before-enqueue
path.

## Authority and publication contract

- `CaptureAuthorityIdentity` is a data-only, read-only audit identity. It exposes the exact source,
  metadata revision, session, and connection generation but grants no positive authority.
- `CaptureAuthorityBundle` owns its associated initializer, admission capability, degradation
  capability, exact frame, and exact receipt. Splitting the bundle consumes it.
- Task 5's `RawMarketFrame` implements `RawCaptureFrameView` with exact identity, nonzero
  generation-local ordinal, receive time, payload bytes, and checked deep retained-memory charge.
- Task 5's `CaptureGenerationCapabilities` implements the complete domain bundle. Its existing
  initializer, admission issuer, degradation capability, and concrete receipt implement the domain
  bridge traits directly.
- `RawCapturePublisher<B>`, `RawCaptureControl<B>`, `RawCaptureWriter<B>`, and
  `CaptureWriterHandle<B>` preserve the single bundle type `B` throughout their lifecycle.
- `RawCapturePublisher::try_publish` accepts `&B::Frame`. For the production Task 5 frame, the
  queued clone shares immutable bounded payload storage. No filesystem work or await occurs in the
  publication path.
- Publication performs authority preflight, reserves aggregate queue bytes, attempts bounded
  `try_send`, issues the source receipt only after enqueue, and then revalidates the exact active
  allocation. Any failed final validation returns no healthy receipt.
- Queue count and aggregate byte capacity are independently bounded. Byte permits are RAII-owned
  and release on every enqueue failure, dequeue, receiver drop, writer error, and shutdown cleanup
  path.
- Queue saturation, retained-size arithmetic failure, queue closure, writer termination, writer
  failure, positive-control loss, and incomplete shutdown degrade the exact affected generation.
  Execution eligibility cannot survive a capture-integrity fault.

## Lifecycle and race hardening

- Positive initialization and generation rotation remain exclusive to the non-clone control
  capability. Publishers are degradation-only.
- A successor bundle initializes before the prior bundle is revoked. Failed successor
  initialization degrades the rejected successor while leaving the current healthy allocation in
  place.
- Successful rotation invalidates the prior generation before installing the successor and
  rechecks writer lifecycle after installation. Wrong-session and non-increasing-generation
  bundles fail closed.
- Writer/control transitions are serialized. The publisher's disconnected-queue path never waits
  for the lifecycle mutex: it atomically marks the writer stopped and degrades both the observed
  and currently installed generation.
- Authority preflight does not retain the allocation lock across frame cloning or bounded enqueue.
  The post-enqueue issue/revalidation sequence closes the resulting rotation race.
- A deterministic issue barrier proves an old in-flight publication cannot return a healthy receipt
  after rotation linearizes.
- A deterministic degradation barrier proves a writer stop racing rotation cannot leave an
  installed successor healthy.
- Blocking sink work runs on the dedicated writer thread and does not stall Tokio. Shutdown and
  handle-drop tests prove queued byte permits are released even when sink work is in flight.
- Natural writer completion, writer failure, shutdown deadline, and both control/writer drop paths
  revoke previously issued receipts.

The authority suite retains 15 capture bridge tests and 4 lifecycle tests. The formerly oversized
824-line bridge test is now a focused 639-line integration root plus a 189-line race-case module;
no coverage was removed by the split.

## Diagnostic journal boundary and application compatibility

The writer derives the bounded `RawCaptureRecord` diagnostic representation from the exact queued
frame on the writer thread. The committed `MSJ1` format remains diagnostic only and cannot
reconstruct current source, capture, sequence, checksum, freshness, venue, or execution authority.
The stale rustdoc reference to the deleted platform-local generation key was removed.

The existing Stage 1 application supervisor now uses the generic platform API through an explicitly
named `DiagnosticCaptureBundle`. That bundle is a compatibility boundary, not the Task 5 production
receipt authority. The type is documented as nonauthoritative and remains disjoint from the source
bundle that the live adapter integration will consume.

## Exact source bridge and follow-ups

The cross-crate integration test constructs a real Task 5 registry session, takes its exact source
capture bundle and frame factory, publishes an exact raw frame through the generic platform queue,
and passes the returned concrete Task 5 receipt into current source validation. Dropping positive
capture control later makes the already-routed current batch fail validation and marks publisher
integrity incomplete.

The test was updated for the routing/evidence follow-up's plural
`CurrentDecodedProviderBatches` result. It asserts exactly one routed batch for the single-scope
fixture before validating the batch, so a missing or unexpectedly expanded route cannot be hidden.

The two source follow-up commits are included in this lane:

- Current policies retain compact exact coverage projections instead of cloning a potentially
  4,096-instrument coverage universe into each observation.
- Mixed-scope provider frames are validated and split into homogeneous routed batches while
  retaining shared exact frame/decoder evidence and original within-key wire order.

`market-squawk-sources` uses `market-squawk-platform` only as a development dependency for the
cross-crate integration test. Production platform code depends on the domain traits, not the source
crate.

## Repository guard maintenance

The full gate exposed exact-inventory drift caused by the integrated Task 5/6 work. Both guards were
updated narrowly:

- `scripts/check_brand.py` moved four compatibility allowances to their current immutable line
  numbers and added one exact allowance for the journal durability research sentence. Every entry
  still pins the complete path, line, token, sentence, and occurrence count. No wildcard, directory,
  or broad pattern exemption was added.
- `scripts/check_duplicate_dependencies.py` contracts the reviewed Windows duplicate inventory to
  the regenerated graph: `windows-sys` 0.52.0 and 0.61.2. Stale `windows-targets` and architecture
  helper allowances were removed rather than retained after those duplicate families disappeared.

All 29 policy-script tests pass with the updated inventories.

## Verification evidence

The final uncommitted candidate passed the repository's complete verification script:

```text
./scripts/verify.sh                                             PASS
  exact legacy-brand inventory                                 PASS
  29 Python policy tests                                       PASS
  workspace-boundary and duplicate-dependency inventories      PASS
  cargo fmt --all -- --check                                   PASS
  strict locked workspace Clippy, all targets/features         PASS
  locked workspace tests, all targets/features                 PASS
  locked workspace doctests                                    PASS
  locked all-feature release build                             PASS
  RUSTDOCFLAGS=-D warnings workspace docs                      PASS
  debug application build and CLI identity                     PASS
  deterministic offline mock (101 processed events)            PASS
  local stdio MCP smoke                                        PASS

cargo test -p market-squawk-platform \
  --test capture_authority_bridge --all-features --locked       PASS, 15 tests

cargo test -p market-squawk-sources \
  --test capture_bridge --locked                               PASS, 2 tests

cargo clippy \
  -p market-squawk-domain \
  -p market-squawk-sources \
  -p market-squawk-platform \
  -p market-squawk \
  --all-targets --all-features --locked -- -D warnings         PASS

git diff --check                                                PASS
gitleaks dir --redact --no-banner .                            PASS, no leaks
```

No performance claim is made; the required performance acceptance evidence belongs to the later
measured benchmark stage.

## Root-owned lockfile delta

`Cargo.lock` was regenerated locally so every `--locked` verification command exercised the actual
integrated dependency graph. Relative to this lane's committed base it adds 483 lines, including the
Task 5/6 manifest graph for `arc-swap`, `cap-std`, `tokio-util`, `url`, TOML 1.1, and their
transitives. The resulting reviewed duplicate graph contains only `windows-sys` 0.52.0 and 0.61.2
for the Windows family.

Per quarter coordination, `Cargo.lock` remains the sole intentionally uncommitted file. Root owns
the single authoritative lockfile regeneration after all parallel lanes and Task 8 are integrated.

## Explicit exclusions

No identity/account rotation, browser or TLS fingerprint concealment, CAPTCHA or anti-bot bypass,
blocking-evasion proxy rotation, distributed quota evasion, telemetry, paid service, cloud
dependency, database/analytical I/O in the live publication path, risk bypass, or replay-derived
execution authority was added.
