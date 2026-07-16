# Quarter 1 Exact Identity Evidence Correction Report

Date: 2026-07-16

Branch: `fix/q1-exact-identity-evidence`

Base commit: `fed21ab`

## Scope

This correction closes the Quarter 1 identity-evidence gap without changing provider-registry
semantics, the generic provenance contracts, the live path, financial value implementation, brand
enforcement, or source-access policy.

The correction introduces three neutral domain contracts:

- `ExactPayloadEvidence` always contains an algorithm-qualified `EvidenceDigest`. Its optional
  `VersionPinnedSourceLocator` is retrieval and explanation metadata only.
- `VersionPinnedSourceLocator` retains a bounded source/caller-supplied locator and a separate
  source/caller-supplied version pin. The type does not independently prove that the pin is
  immutable; the mandatory digest remains authoritative.
- `RevisionBoundPayloadEvidence` atomically binds a typed `MetadataRevision` to the exact payload
  evidence that established the revision claim.

All fields are private, construction is invariant-preserving, and all three Serde object contracts
deny unknown fields. A moving URL such as a `FIX.Latest` locator, a bare `SourceReference`, or an
unversioned locator is structurally insufficient to qualify as exact payload evidence.

## Migrated identity contracts

- `FuturesContractIdentity` now carries `RevisionBoundPayloadEvidence`. Its metadata revision is
  typed and cannot be retained separately from the exact digest-backed security-definition
  evidence. The former generic `source_reference` plus loose `metadata_revision` wire shape is
  intentionally rejected.
- `ExternalIdentifierRecord` now carries `ExactPayloadEvidence` for assignment evidence. The former
  generic `source_reference` wire shape is intentionally rejected.
- Existing futures and external-identifier fixtures were migrated to the stronger contracts.
- Rustdoc, the Quarter 1 decision record, target-state architecture, implementation plan, and final
  domain-contract report were updated to state the invariant and its limits accurately.

## Test-driven evidence

The implementation followed focused RED-to-GREEN cycles:

1. The initial exact-evidence tests failed to compile because `ExactPayloadEvidence`,
   `VersionPinnedSourceLocator`, and `RevisionBoundPayloadEvidence` did not exist.
2. After the neutral contracts were implemented, those three tests passed.
3. Migration tests then failed to compile because `ExternalIdentifierRecordInput` and
   `FuturesContractIdentityInput` still exposed generic `source_reference` fields, futures still
   exposed a loose source identifier as its metadata revision, and neither record exposed the new
   evidence accessor.
4. After migrating the production contracts and fixtures, the five focused exact-evidence tests
   passed.

The tests cover mandatory digest evidence, preservation of digest algorithm identity, optional
version-pinned locator round trips, atomic typed revision binding, strict unknown-field rejection,
legacy generic-reference rejection, and rejection of a moving `FIX.Latest` URL as standalone
evidence.

## Verification

The following gates passed before the final repository gate:

```text
cargo test -p market-squawk-domain --test exact_payload_evidence --locked
cargo test -p market-squawk-domain --all-targets --all-features --locked
cargo test -p market-squawk-domain \
  --test exact_payload_evidence \
  --test instrument_identity_records \
  --test futures_identities \
  --test maturity_month_year \
  --test financial_values \
  --locked
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc \
  -p market-squawk-domain --all-features --no-deps --locked
python3 scripts/check_brand.py
git diff --check
```

The full `./scripts/verify.sh` repository gate passed on the final formatted implementation tree.
It covered the policy checks available on that branch, formatting, workspace Clippy with warnings
denied, the full workspace test suite, release compilation, strict rustdoc, and local smoke checks.

## Explicit exclusions

This correction does not add identity/account rotation, fingerprint spoofing, CAPTCHA or anti-bot
bypass, proxy rotation for blocking evasion, or distributed quota evasion. It does not weaken
coverage, entitlement, provenance, quality, or execution controls.
