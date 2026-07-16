# Q1 Provenance and Money Wire Correction Report

Date: 2026-07-16
Base: `fed21ab3786630e979991037a18ab2fd566c9dd0`
Branch: `fix/q1-provenance-money`

## Result

The public provenance API now describes the implemented archive-only authority boundary without
claiming that an opaque assessment reference retains, resolves, or proves assessment evidence.
`PayloadReference::SourceReference` is documented as an opaque locator/record identity rather than
immutable evidence. `Money` deserialization now rejects unknown object fields while preserving its
exact canonical round trip and constructor normalization.

No execution-authority behavior, provider-identity behavior, financial arithmetic, architecture
plan, or runtime hot-path behavior changed.

## Test-first evidence

The documentation regression was added before the rustdoc correction. It failed on both stale
contracts:

```text
FAIL: test_public_provenance_rustdoc_does_not_overclaim_archival_evidence
AssertionError: 'it requires a successful qualification' unexpectedly found

FAIL: test_payload_reference_rustdoc_preserves_opaque_locator_semantics
AssertionError: 'opaque source-side record locator' not found
```

The `Money` wire regression was also added before schema hardening and failed because Serde accepted
the unrecognized nested field:

```text
test money_wire_is_exact_and_rejects_nested_unknown_fields ... FAILED
assertion failed: serde_json::from_value::<Money>(unexpected).is_err()
```

After the implementation changes, both focused tests passed.

## Corrections

- `ProvenanceError::UnqualifiedDirectVerified` now states that a recorded label is only a
  caller-supplied archival assertion paired with an opaque reference and does not prove successful
  qualification.
- `LiveProvenance` states that it retains no assessment evidence, does not dereference the opaque
  reference, proves neither its existence nor relationship to the recorded classification, and
  never grants current execution authority.
- Generic and research payload-reference rustdoc distinguishes a content digest from an opaque
  source locator/record identity and disclaims inherent existence, immutability, and retrievability.
- The private `MoneyFields` deserialization wire denies unknown fields.
- Persistent documentation-contract tests protect the corrected public Rust API statements.
- A `Money` regression protects exact canonical round trips and nested unknown-field rejection.

## Verification

The complete repository gate passed:

```text
./scripts/verify.sh
```

That gate ran and passed brand/policy tests, all Python unit tests, workspace-boundary and
duplicate-dependency checks, `cargo fmt --all -- --check`, workspace Clippy with warnings denied,
all locked workspace tests and doctests, the locked release build, rustdoc with warnings denied, the
offline CLI mock smoke test, and the MCP smoke test.

The complete domain test suite also passed independently:

```text
cargo test -p market-squawk-domain --all-features --locked
```

The base checkout has no `scripts/check_generated_artifacts.py`; the repository's authoritative
`scripts/verify.sh` therefore defines and passed the available generated-artifact-independent gate.
