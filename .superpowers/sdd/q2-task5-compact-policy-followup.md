# Q2 Task 5 compact-policy follow-up

Date: 2026-07-16

## Finding

The root integration audit found that the initial Task 5 commit retained a cloned
`SourceCoverage` inside every `ValidatedLiveScope` and `CurrentLivePolicy`. An enumerated coverage
declaration may own 4,096 instruments, so a multi-observation decoded frame could retain that
universe once per observation. `CurrentDecodedProviderBatch::retained_bytes` charged only the
shallow observation structure and did not include those policy allocations.

The focused TDD red gate was:

```text
error[E0599]: no method named `coverage` found for reference `&CurrentLivePolicy`
help: there is a method `static_coverage` with a similar name
```

## Correction

- Added non-Serde `CurrentCoveragePolicy`, a compact exact-scope projection containing only the
  validated source, venue, provider product/channel, event/depth, delay, consolidation, delivery,
  exact coverage evidence, effective interval, and metadata revision.
- Removed `SourceCoverage` from both the validated scope and current observation handoff.
- Removed duplicate provider product/channel ownership from `CurrentLivePolicy`; those accessors
  now delegate to the compact coverage projection.
- Added checked conservative retained-memory charges for all bounded dynamic identifiers/evidence
  that a current policy may own and one shared authority-allocation charge per batch.
- Added an adversarial integration case with the maximum 4,096-instrument metadata universe and two
  current observations. The resulting batch remains below 128 KiB, exposes only the compact scope,
  and retains no serializable current coverage authority.

## Verification

The following commands passed on the integrated root before this follow-up commit:

```text
cargo fmt --all --check
cargo test -p market-squawk-sources --all-features --locked
cargo clippy -p market-squawk-sources --all-targets --all-features --locked -- -D warnings
git diff --check
```

The focused source suite reports 26 passing tests and zero failures. The root-owned integrated
`Cargo.lock` remains deliberately uncommitted until the Task 5/6 capture bridge is complete.
