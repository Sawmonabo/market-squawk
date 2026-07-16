# Quarter 1 Final Registry Asymptotics Correction Report

Date: 2026-07-16

Base: `2a6c5b9fe7f9e44e655c0e5e52be1b09f733498b`

Scope: close the two Important findings and two documentation Minors from the independent final
Quarter 1 acceptance review. This report supersedes the retry-path performance conclusions in the
earlier capacity correction report where the current implementation differs.

## Findings addressed

1. Content-equivalent retries still scanned as many as 262,144 assertions, and an accepted metadata
   merge sorted as many as 65,536 accepted records.
2. A guaranteed growth rejection at the reconstruction ceiling cloned the complete nested registry
   before checking the already-known capacity failure.
3. Public revision and provider-record documentation retained weaker or overstrong trust language.

## TDD evidence

The first RED added production-path probes before implementation:

```text
cargo test -p market-squawk-domain --lib \
  instrument::provider_identities::registry::tests::accepted_retry_lookup_is_logarithmic_and_does_not_reconstruct \
  --locked

error[E0425]: cannot find function `reset_registry_test_probes`
error[E0425]: cannot find function `reconstruction_build_count`
error[E0425]: cannot find function `revision_lookup_comparison_count`
5 compile errors total across the new retry and capacity probes
```

After the ordered lookup/count implementation, the registry tests passed but the strengthened
documentation policy remained RED because the Rust documentation did not yet contain the exact
independent-verification boundary:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 12 tests
FAILED (failures=2)
```

The expanded ordering regression also produced a compile-time RED (`E0599`) when it deliberately
used an unsupported borrowed conversion on `ProviderInstrumentId`; the test was corrected to use
the public display contract before the final GREEN run.

## Implementation

- Accepted assertions are located by binary search over the canonical ordering prefix:
  `(source_id, provider_instrument_id, metadata_revision)`.
- Conflict groups are located by the same prefix; only the matched, bounded competitor collection
  (maximum 256) is scanned and locally sorted.
- Whole-key quarantine uses a separate binary search over the two-field natural key, so a conflict
  at one revision still rejects an absent/new revision.
- Accepted metadata replacement is transactional and in place. Since a valid registry owns at most
  one accepted record per natural-key/revision prefix, metadata-only changes cannot move it across
  another accepted record and no aggregate sort is needed.
- Growth classification uses the same ordered prefix invariants rather than aggregate scans.
- Canonical record count uses checked arithmetic before allocation or cloning. Capacity failure is
  returned immediately; permitted growth preallocates exactly the current record count plus one.
- Reconstruction and complete normalization/revision-graph validation remain the only growth commit
  point.
- Test-only probes demonstrate logarithmic lookup (at most 16 comparisons across 4,096 accepted
  records) and zero reconstruction builds for exact retries and at-ceiling growth rejection.
- Registry unit tests moved to `provider_identities/registry/tests.rs`; production `registry.rs` is
  477 lines and the focused test module is 382 lines.
- `MetadataRevision` and provider-record rustdoc now state that caller/source-supplied values alone
  establish neither authority nor immutability. Independent applicable registered-source and
  source-specific adapter verification is required.

## Final verification

Focused high-case verification passed:

```text
PROPTEST_CASES=4096 cargo test -p market-squawk-domain \
  --lib --test provider_identity_evidence --test provider_identity_registry --locked

registry unit tests: 11 passed
provider evidence tests: 9 passed
provider registry tests: 14 passed
```

Documentation contracts passed:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 12 tests
OK
```

The complete report-preparation tree passed:

```text
PROPTEST_CASES=4096 ./scripts/verify.sh
```

That gate included 29 Python policy tests, brand/workspace/dependency checks, formatting, locked
warning-denied workspace Clippy, all workspace tests and doctests, locked all-feature release build,
warning-denied rustdoc, CLI validation, the deterministic 101-event offline source smoke, and local
stdio MCP smoke. It exited with status 0.

No access-control or quota-evasion capability was added.
