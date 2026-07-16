# Q1 Live Relational Corrections Report

Date: 2026-07-16

## Scope

This correction closes the final quarter-one review findings in live timing authority, source
coverage identity, book/non-book integrity semantics, and sequence/snapshot relational checks.
Research provenance and instrument identity contracts were intentionally left unchanged.

## RED evidence

The first focused run was:

```text
cargo test -p market-squawk-domain --test live_trust_contracts --locked
```

It failed 6 of 16 tests for the expected reasons:

- market- and source-age deadlines could be stretched by a caller-supplied `valid_until`;
- a qualification input accepted that stretched timing window;
- coverage evidence could be transplanted across source or provider channel;
- a non-book binding accepted retained book state;
- non-book coverage accepted market depth.

A subsequent RED compile established the missing event-shaped checksum and book-applicability
contracts: `PayloadChecksumScope`, `validate_book`, `validate_payload`, and
`BookIntegrity::NotApplicable` did not yet exist.

## GREEN implementation

- `LiveTimingAssessment::maximum_valid_instant` derives the inclusive minimum of
  `source_timestamp + maximum_source_age` and `received_at + maximum_market_age` using `i128`
  arithmetic. Deadlines beyond the timestamp domain clamp to `i64::MAX`.
- `BoundAssessment` applies the domain result's intrinsic validity ceiling during construction and
  deserialization. Exact timing deadlines pass; deadline plus one nanosecond is rejected.
- `CoverageScope` now includes `SourceId` and `ProviderChannel`, validates both against the complete
  live binding, rejects non-book depth, and retains symmetric Serde validation.
- Non-book live bindings reject `BookStateBinding`; book bindings continue to require it and its
  canonical digest.
- Checksums use distinct `ChecksumScope` and `PayloadChecksumScope` values under an event-shaped
  `ChecksumTarget`. Qualification accepts book targets only for book events and payload targets only
  for non-book events. Metadata-declared unsupported checksum protocols remain explicit.
- Book events require metadata-backed `SnapshotApplicability::Required`, initialized book state,
  matching depth, and applicable book-integrity evidence. Non-book events require metadata-backed
  `NotApplicable`, no book state/depth, payload checksum evidence when provided, and
  `BookIntegrity::NotApplicable`.
- Provided sequence evidence on book events now requires exact `Option` equality for both snapshot
  and observed sequence pairs. `Some`/`None`, `None`/`Some`, and unequal `Some`/`Some` pairs all fail.
  Metadata-declared unsupported sequence protocols remain retained but cannot become
  `DirectVerified`.
- Tautological taxonomy runtime assertions were removed. Compile-time guards still prohibit implicit
  fair-value/depth conversions into data quality or execution eligibility; the live qualification
  behavior test now exercises qualification without a fair-value input.
- Oversized files were split at cohesive boundaries: qualification inputs versus derivation, checksum
  evidence versus sequence/snapshot evidence, and timing tests versus relational trust tests.

## Adversarial coverage

- source, venue, product, channel, event class, depth, and metadata-revision coverage transplants;
- exact market/source deadlines, plus-one queue delay, future-skew and transport boundaries, and
  `i64::MIN`/`i64::MAX` arithmetic;
- every live assessment component and every complete binding identity dimension;
- wrong checksum target and wrong book-integrity applicability in both event directions;
- wrong snapshot applicability in both event directions;
- all partial and contradictory sequence/snapshot option combinations;
- explicit unsupported checksum and sequence metadata behavior;
- archival no-current-authority behavior and existing provenance/schema tests through the full suite.

## Fresh verification

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./scripts/verify.sh
```

All commands exited 0. The repository verification script ran helper-policy tests, workspace tests,
release build, rustdoc, the 101-event offline mock, and MCP smoke test. Focused final suites passed:

```text
live_timing_contracts: 7 passed, 0 failed
live_trust_contracts: 15 passed, 0 failed
```

## Self-review

- Invalid or stale timing evidence has no intrinsic future-validity ceiling, but it carries a retained
  eligibility failure and therefore remains rejected at every instant. Valid/fresh timing evidence is
  the only case that can contribute current policy satisfaction and is always objectively capped.
- Future-skew and transport constraints are immutable source-to-receive relations; they are validated
  atomically. Source-age and receive-age constraints are the two relations that establish a future
  deadline.
- The public API changes are deliberate invariant strengthening during the pre-release domain stage;
  no backward-compatibility claim is made for the incomplete 0.1.0 workspace.
- No current-execution authority, risk bypass, research provenance, identity model, or external I/O was
  added.
