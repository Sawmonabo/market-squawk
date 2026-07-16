# Quarter 1 Checkpoint Correction Report

Date: 2026-07-16

Integrated correction head before this report: `9965d0d`

Scope: grouped acceptance findings for Stage 1 Tasks 1 through 4.

## Review findings closed

### Provider identity evidence and ingestion

- Provider assertion identity is now the algorithm-qualified content digest plus the substantive
  normalized assertion. Optional source locators are bounded retrieval metadata and do not create
  false conflicts.
- Locator metadata is retained in deterministic sorted and deduplicated order, including locator
  evidence on equivalent supersession edges.
- Provider record observations, locators, conflict competitors, accepted records, conflict groups,
  and reconstruction input all have explicit capacity bounds.
- `ProviderIdentityRegistry` owns accepted records and quarantined conflicts. Its transactional
  `ingest` operation returns exhaustive typed successful outcomes: `Inserted`,
  `ObservationCoalesced`, `SupersedingRevisionAppended`, or `ConflictQuarantined`.
- Failed graph, capacity, or quarantined-key transitions leave the registry unchanged. A duplicate
  is matched against every quarantined variant before classification, and a new revision cannot be
  reported as a checked successor behind an unresolved conflict.
- `InstrumentDefinition` owns the checked registry and validates its instrument identity across
  accepted assertions and quarantined competitors.

Implementation commit: `9965d0d` (source-lane commit `0881d1c`).

TDD evidence retained by the implementation lane:

```text
Initial RED: missing ProviderIdentityRegistry, ProviderIdentityIngestOutcome,
locator-collection APIs, and InstrumentDefinition registry accessor.

Adversarial RED: missing ProviderIdentityKeyQuarantined typed error.

GREEN: provider_identity_evidence 9/9; provider_identity_registry 10/10.
```

### Exact identity payload evidence

- Added neutral `ExactPayloadEvidence`, which always contains an algorithm-qualified digest and may
  retain an optional, separately version-pinned bounded locator.
- Added `RevisionBoundPayloadEvidence`, atomically retaining a typed metadata revision with the
  exact payload evidence that established it.
- Futures security definitions and authoritative external identifier assignments no longer accept
  a generic moving source reference as exact immutable evidence.
- Legacy bare-locator and loose-revision wire shapes are rejected; strict unknown-field handling is
  preserved.

Implementation commit: `7c6c99f` (source-lane commit `3cc3420`).

### Archive provenance and financial wire documentation

- Public provenance documentation now states that recorded direct-verified quality is a
  caller-supplied archival assertion paired with an opaque reference. The record does not retain or
  dereference assessment evidence, prove the reference exists, prove the referenced assessment
  produced the classification, or grant current execution authority.
- Generic source references are documented as opaque locator or record identities without an
  inherent immutability, existence, or retrievability guarantee.
- The `Money` deserialization wire rejects unknown nested fields while preserving exact canonical
  round trips.
- Persistent documentation-contract tests cover the corrected public Rust API statements.

Implementation commit: `83f8fc5` (source-lane commit `d6f86ff`).

## Integrated verification

The following combined checks passed after all three commits were integrated:

```text
PROPTEST_CASES=2048 cargo test -p market-squawk-domain \
  --test provider_identity_evidence \
  --test provider_identity_registry \
  --test exact_payload_evidence \
  --test futures_identities \
  --test maturity_month_year \
  --test instrument_identity_records \
  --test financial_values \
  --test live_authority_boundary \
  --locked

python3 -m unittest discover -s scripts/tests -p 'test_*.py'

cargo clippy -p market-squawk-domain \
  --all-targets --all-features --locked -- -D warnings

RUSTDOCFLAGS='-D warnings' \
  cargo doc -p market-squawk-domain --all-features --no-deps --locked

git diff --check 66d30fe..HEAD
```

Results:

- 69 focused Rust tests passed.
- 21 Python policy and regression tests passed.
- Strict domain Clippy passed.
- Warning-denied domain rustdoc passed.
- The integrated diff check passed.

The full root `./scripts/verify.sh` gate passed after the three implementation commits and this
report were integrated. It included all 21 policy tests, formatting, warning-denied locked
workspace Clippy, locked workspace tests and doctests, locked release build, warning-denied
rustdoc, the deterministic 101-event offline smoke, and the local stdio MCP smoke. Final grouped
signoff is recorded in the Stage 1 progress ledger after independent reviewers approve the exact
post-report head.
