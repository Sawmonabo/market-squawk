# Quarter 1 Provider-Identity Coalescing Capacity Correction Report

Date: 2026-07-16

Branch: `fix/q1-final-coalescing-capacity`

Base commit: `7c5051237b97b92ec5142301c903c34d41b81a95`

## Outcome

`ProviderIdentityRegistry::ingest` now coalesces content-equivalent accepted and quarantined
provider assertions even when the registry has reached its reconstruction-record ceiling. The
ceiling applies only to transitions that increase the canonical record count.

The coalescing path is bounded and transactional:

- An exact full-metadata retry returns `ObservationCoalesced` immediately. It does not clone the
  registry, rebuild the aggregate record set, or replace the containing vector allocation.
- A metadata-only retry clones only the matching canonical record, applies checked locator and
  observation-timestamp merging to that clone, and replaces the original only after every check
  succeeds.
- A successful metadata merge re-sorts only the affected accepted or competing-assertion vector
  with the canonical record comparator. This preserves deterministic serialization order without
  rebuilding unrelated records.
- Locator or observation-timestamp exhaustion returns the typed capacity error and leaves the
  entire registry unchanged.
- A transition that would add a canonical record still reconstructs and validates the full
  candidate registry transactionally. It is rejected at the configured reconstruction ceiling
  before mutation.
- A quarantined key still rejects a new revision. Same-revision variants remain eligible for the
  existing conflict transition, while content-equivalent variants take the coalescing path.

The production ceiling remains unchanged. A private, test-only-accessible ingestion helper accepts
a simulated reconstruction ceiling so the boundary policy can be exercised with small deterministic
fixtures instead of allocating the production maximum.

## Contract and documentation corrections

Provider-identity rustdoc now states the actual trust boundary:

- A supersession is a provider-supplied predecessor claim bound to exact content evidence; the
  evidence does not itself authorize the provider's revision.
- A source/caller revision identifier establishes neither authority nor immutability by itself.
- Evidence contains a mandatory content digest plus bounded optional version-pinned locators.
  Locators are non-substantive retrieval metadata, not identity or authority.

Repository documentation-contract tests enforce these phrases and reject the previous overclaims.
No architecture, research, planning, or pre-existing report document was changed by this correction.

## Test-driven development evidence

The correction used two explicit RED-to-GREEN cycles.

### Cycle 1: capacity semantics

Tests were added first for accepted and quarantined assertions at a simulated ceiling, transactional
growth rejection, locator exhaustion, observation exhaustion, and deterministic conflict ordering.

The first focused RED command was:

```text
cargo test -p market-squawk-domain --lib --locked
```

Compilation failed with seven `E0599` errors because
`ProviderIdentityRegistry::ingest_with_reconstruction_limit` did not exist. The initial
documentation-contract run executed nine tests and failed the provider-identity contract test
because the corrected trust-boundary language did not exist. After the first implementation, all
seven focused registry tests and all nine documentation-contract tests passed.

### Cycle 2: direct in-place coalescing

The first green implementation still reconstructed the aggregate vector before committing a
coalesced assertion. Allocation-stability assertions were therefore added to the four accepted and
quarantined exact/metadata ceiling tests before production code was revised.

The second RED focused run compiled and then failed those four new pointer-stability assertions;
the other three registry tests passed. The production path was then changed to scan canonical
records directly, return exact duplicates without cloning, clone only a metadata-merge candidate,
and sort the affected vector in place.

The final high-case focused command was:

```text
PROPTEST_CASES=2048 cargo test -p market-squawk-domain \
  --lib \
  --test provider_identity_evidence \
  --test provider_identity_registry \
  --locked
```

All 30 tests passed: seven library unit tests, nine provider-identity-evidence integration tests,
and fourteen provider-identity-registry integration tests. The property-test environment was fixed
at 2,048 cases for this acceptance run.

## Coverage added

The focused correction tests prove:

- exact accepted retry at the simulated reconstruction ceiling;
- metadata-only accepted retry at the ceiling;
- exact quarantined retry at the ceiling;
- metadata-only quarantined retry at the ceiling;
- no accepted or competing-vector allocation replacement on those paths;
- deterministic reordering after merge changes comparator-visible locator metadata;
- transactional rejection of growth at the ceiling;
- transactional locator-capacity failure; and
- transactional observation-capacity failure in quarantine.

Existing provider-identity tests continue to cover normalization, conflict formation, revision
graphs, strict Serde validation, canonical ordering, and proptest-generated registry behavior.

## Verification ledger

The following focused and static gates passed:

```text
cargo fmt --all --check
PROPTEST_CASES=2048 cargo test -p market-squawk-domain \
  --lib \
  --test provider_identity_evidence \
  --test provider_identity_registry \
  --locked
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc \
  -p market-squawk-domain --all-features --no-deps --locked
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_brand.py
git diff --check
```

The policy/documentation suite passed all 26 tests. The full `./scripts/verify.sh` repository gate
also passed on the final report-inclusive tree. It covered repository policy checks, formatting,
workspace Clippy with warnings denied, the complete workspace test suite, release compilation,
strict rustdoc, the deterministic offline source fixture, and the local MCP smoke test.

## Explicit exclusions

This correction does not add identity or account rotation, browser or TLS fingerprint spoofing,
CAPTCHA or anti-bot bypass, proxy rotation intended to defeat blocking, or distributed quota
evasion. It does not weaken source coverage, entitlement, provenance, quality, quarantine,
execution, or audit controls.
