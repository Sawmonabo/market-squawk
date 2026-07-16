# Quarter 1 Final Registry-Bounds Correction Report

Date: 2026-07-16

Branch: `fix/q1-final-registry-bounds`

Base commit: `75452af4178d56f2f54ef0faf7df24f4341103b0`

## Scope

This correction closes the final grouped-review finding in provider-identity registry
deserialization. The lane changes only the provider registry/module, its integration tests, and
provider-local metadata-revision wording.

## Aggregate wire bound

The prior nested wire admitted up to 65,536 accepted records and 16,384 conflict groups containing
up to 256 assertions each, then checked the 262,144-record reconstruction budget only after the
complete nested value had been retained.

The corrected constant-product contract is:

```text
MAX_ACCEPTED_RECORDS = 65,536
MAX_CONFLICTS = 768
MAX_COMPETING_ASSERTIONS = 256

65,536 + (768 * 256) = 262,144 = MAX_RECONSTRUCTION_RECORDS
```

`MAX_WIRE_RECORDS` is calculated with checked multiplication and addition. A module-level constant
assertion makes overflow or any future result above `MAX_RECONSTRUCTION_RECORDS` a compilation
failure. This gives the derived nested Serde wire the same hard aggregate record budget as batch
reconstruction without allocating a 262,144-record test fixture.

## Ingest and documentation contract

`ObservationCoalesced` now explicitly covers both cases:

- content-equivalent evidence that contributes new bounded locator or observation metadata; and
- an already-retained exact duplicate, which returns `ObservationCoalesced` without changing
  canonical registry state.

Provider-local `MetadataRevision` field/accessor documentation now describes a bounded
caller/source revision identity whose authority is established by the surrounding assertion
evidence. The classification-side type documentation remains outside this lane.

## Test-first evidence

RED:

```text
cargo test -p market-squawk-domain --test provider_identity_registry --locked

error[E0599]: no associated function or constant named `MAX_WIRE_RECORDS` found for struct
`ProviderIdentityRegistry`
```

The failure was the intended missing aggregate wire-capacity contract.

GREEN after the minimal production correction:

```text
cargo test -p market-squawk-domain --test provider_identity_registry --locked

14 passed; 0 failed
```

The added regressions prove:

- the worst-case nested wire exactly matches the aggregate reconstruction budget;
- one additional maximum-sized conflict group would exceed that budget;
- an exact duplicate reports coalescing without changing state;
- a representative quarantined registry round-trips canonically; and
- conflict-first field order reconstructs the same checked state.

## Verification ledger

The focused final commands and observed results were:

```text
PROPTEST_CASES=2048 cargo test -p market-squawk-domain \
  --test provider_identity_evidence \
  --test provider_identity_registry \
  --locked

23 passed; 0 failed

cargo clippy -p market-squawk-domain \
  --all-targets --all-features --locked -- -D warnings

exit 0

RUSTDOCFLAGS='-D warnings' \
  cargo doc -p market-squawk-domain --all-features --no-deps --locked

exit 0
```

The report-inclusive tree then passed `./scripts/verify.sh` with exit code zero. That gate covered
all 21 repository policy tests, formatting, warning-denied locked all-target/all-feature workspace
Clippy, locked workspace tests and doctests, the locked release build, warning-denied rustdoc, the
deterministic 101-event offline smoke test, and the local stdio MCP smoke test. The complete gate was
run again after recording this result so its evidence applies to the final pre-commit tree.
