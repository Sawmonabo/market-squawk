# Quarter 1 Final Contract-Truth Correction Report

Date: 2026-07-16

Branch: `fix/q1-final-contract-truth`

Base commit: `75452af4178d56f2f54ef0faf7df24f4341103b0`

## Outcome

The authoritative Quarter 1 architecture, research decision, and Stage 1 plan now describe the
implemented evidence and provider-registry contracts without treating digest-algorithm changes as
wire errors or treating all content-equivalent reingestion as a metadata-free no-op.

No provider-registry or financial implementation was changed. The correction is limited to
persistent contract tests, one exact-evidence wire regression, public `MetadataRevision` rustdoc,
authoritative documentation, migration notes, and historical report reconciliation.

## Contract corrections

- `ExactPayloadEvidence` rejects omitted content evidence, unknown fields, and legacy insufficient
  shapes. It preserves the explicitly supplied digest algorithm as part of evidence identity. The
  same bytes under a different supported algorithm deserialize as distinct valid evidence.
- Provider ingestion is idempotent at the logical-assertion layer: content-equivalent reingestion
  creates no second logical assertion. The checked registry deterministically coalesces bounded
  locator/observation metadata and returns `ObservationCoalesced`; a byte-for-byte semantic repeat
  with no new metadata leaves canonical registry state unchanged.
- `MetadataRevision` is documented as a bounded caller/source-supplied identifier. Authority and
  immutability come from the surrounding registered/evidenced contract, not the identifier alone.
- The stale final-domain report is prominently marked superseded. The final-docs report now states
  the integrated coalescing contract, and the exact-identity report no longer claims a
  generated-artifact gate that was unavailable on its historical branch.
- `CHANGELOG.md` records the breaking wire migration to `ExactPayloadEvidence`,
  `RevisionBoundPayloadEvidence`, rejected legacy `source_reference` shapes, and the checked
  `provider_identity_registry` field owned by `InstrumentDefinition`.

## Test-driven evidence

Tests were added before the authority text was changed.

RED:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 8 tests
FAILED (failures=8)
```

The failures independently identified the stale algorithm-rejection language, duplicate-no-op
language, overclaimed `MetadataRevision` rustdoc, missing supersession marker, false historical gate
claim, and absent changelog migration note.

The exact-evidence algorithm test was intentionally a characterization regression: it passed before
documentation changes, proving that production Serde already accepted the changed supported
algorithm and distinguished the resulting evidence. No production evidence code needed alteration.

GREEN:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 8 tests
OK

cargo test -p market-squawk-domain \
  --test exact_payload_evidence \
  --test provider_identity_evidence \
  --test provider_identity_registry \
  --test classification \
  --test strict_identity_wires \
  --all-features \
  --locked
34 passed; 0 failed
```

## Final verification

The following commands completed successfully on the final implementation and authority-document
content:

```text
cargo fmt --all
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_brand.py
git diff --check
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
./scripts/verify.sh
```

The full gate included formatting, repository policy tests, locked all-target/all-feature Clippy,
locked workspace tests and doctests, locked release build, warning-denied rustdoc, the deterministic
101-event offline mock, and the local stdio MCP smoke test. No external network test or performance
claim is part of this correction.

## Scope guard

The correction does not add identity/account rotation, fingerprint concealment, CAPTCHA or anti-bot
bypass, blocking-evasion proxy rotation, distributed quota evasion, execution bypasses, paid/cloud
requirements, telemetry, or hidden outbound behavior.
