# Q1 Final Live Audit and Durability Correction Report

Date: 2026-07-16
Branch: `fix/q1-final-live-audit`

## Scope

This correction closes the final Quarter 1 trust-boundary findings in live provenance,
qualification durability, digest identity, and coverage validity. It deliberately creates no
execution authority: `LiveProvenance` and `QualificationAssessment` remain durable audit evidence,
and archived `DirectVerified` labels still require live-plane requalification.

## TDD evidence

### Explicit live availability

RED:

```text
cargo test -p market-squawk-domain --test live_provenance_availability --locked
```

Failed because live provenance inputs had no `available_at`, the record had no accessor, and the
ordering error did not exist.

GREEN: 4 tests passed. Both decoded and recorded constructors and the checked wire now require an
explicit `available_at`. They enforce the documented inclusive ordering
`received_at <= available_at <= ingested_at`. The field has no default or alias, round-trips through
a composite `MarketEvent`, and invalid constructor and wire permutations are rejected.

### Coverage-owned validity

RED:

```text
cargo test -p market-squawk-domain --test coverage_validity --locked
```

The first run demonstrated that a `BoundAssessment<SourceCoverageRecord>` could extend one
nanosecond beyond the metadata scope's `effective_until`.

GREEN: 2 tests passed. `SourceCoverageRecord::maximum_valid_until` now returns the scope's inclusive
`effective_until`, so `BoundAssessment` rejects an extension at `+1ns`. Qualification status is
satisfied at the exact strict boundary and rejected one nanosecond later.

### Algorithm- and rule-qualified evidence digests

RED:

```text
cargo test -p market-squawk-domain --test digest_binding --locked
```

Failed because `EvidenceDigest` carried only bytes and canonical-state rule types did not exist.

GREEN: 4 tests passed. The final structure is:

```text
DigestAlgorithm
  -> EvidenceDigest { algorithm, bytes }

CanonicalizationRule { rule identity, one-based RuleVersion }
  + EvidenceDigest
  -> CanonicalStateDigest
```

The algorithm enum lives in a neutral domain module rather than creating a provenance/classification
module cycle. `PayloadHash` and live payload bindings compare the complete algorithm plus bytes.
Book state, canonical state, and initialized snapshots use `CanonicalStateDigest`; equality therefore
binds algorithm, bytes, canonicalization identity, and rule revision. Tests reject SHA-256/BLAKE3
same-byte transplants, rule and version transplants, unknown wire fields, and zero rule versions.

### Durable qualification assessments

RED:

```text
cargo test -p market-squawk-domain --test qualification_durability --locked
```

Failed at compile time because `QualificationAssessment` intentionally had no `Deserialize`
implementation.

GREEN: 4 tests passed. The custom, deny-unknown-fields wire deserializer:

1. checks nested binding and validity-window types through their constructors;
2. reconstructs sequence, snapshot, checksum, and timing evidence from retained operands;
3. rejects contradictions in nested derived integrity/freshness fields;
4. creates a cohesive `QualificationAssessmentInput`;
5. routes the complete value through `QualificationAssessment::try_from` to revalidate relational
   bindings, generation, source capabilities, snapshot/checksum applicability, coverage, and the
   strict validity intersection;
6. recomputes failures, recorded quality, `evaluated_at`, and `valid_until`; and
7. rejects the wire if any retained derived value differs from the recomputed result.

The tests include a compile-time `for<'de> Deserialize<'de>` assertion, checked round-trip,
top-level unknown-field rejection, individual tampering of all four derived fields, binding
transplant, sequence-result forgery, and timing-freshness forgery. Deserialization returns audit
evidence only and exposes no execution capability.

## Ripple audit

All live provenance constructors and canonical-event fixtures were migrated to explicit local
availability. All payload and state digest consumers were migrated to the new strongly separated
types. Existing financial exactness, identity, source coverage, timing, canonical event,
qualification, and archive-authority tests remained green.

## Verification evidence

The following completed successfully after the final changes:

```text
cargo fmt --all --check
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
cargo test -p market-squawk-domain --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
./scripts/verify.sh
```

The repository verification script additionally passed its 16 Python gate tests, release build,
documentation build, CLI/help checks, the 101-event offline mock smoke test, and the MCP smoke test.
