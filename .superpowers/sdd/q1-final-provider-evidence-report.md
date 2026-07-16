# Q1 Final Provider Evidence Correction Report

Date: 2026-07-16

Branch: `fix/q1-final-provider-evidence`

Base: `77a369441b756bdb8374114515cf895dd8035184`

## Outcome

Provider identity assertions and supersession edges can no longer use a bare `PayloadReference` as
immutable evidence. The provider-specific contract now requires an algorithm-qualified
`EvidenceDigest` and may separately retain a version-pinned source locator.

## Contract changes

- `ProviderIdentityEvidence` always contains `content_digest: EvidenceDigest`.
- `ProviderIdentityLocator` contains separate bounded `reference` and `version` identities.
- A locator is optional retrieval/explanation metadata and cannot replace the content digest.
- `ProviderIdentityRecordInput`, the normalized record wire, and
  `ProviderIdentitySupersession` use `ProviderIdentityEvidence` directly.
- Canonical comparison includes digest algorithm, digest bytes, locator reference, and locator
  version, preserving deterministic normalization and permutation invariance.
- Strict Serde rejects missing digests, locator values without versions, legacy bare
  `source_reference` shapes, unknown fields, and malformed nested identities.
- SHA-256 and BLAKE3 evidence with identical digest bytes remain distinct.
- Public `# Errors` documentation now enumerates all lifecycle ordering and instrument/provider
  graph construction failures.

Existing provider observation coalescing, same-revision conflict quarantine, revision-graph
validation, active-resolution suppression, and input-order invariance remain unchanged.

## TDD evidence

The new tests were written before production changes. The first syntactically valid RED run failed
because `ProviderIdentityEvidence` and `ProviderIdentityLocator` did not exist. After the minimal
contract implementation and fixture migrations, the focused GREEN run passed:

```text
cargo test -p market-squawk-domain \
  --test provider_identity_evidence \
  --test instrument_identity_records \
  --test financial_values \
  --locked

provider_identity_evidence: 8 passed
instrument_identity_records: 5 passed
financial_values: 29 passed
```

## Fresh verification

```text
PROPTEST_CASES=2048 cargo test -p market-squawk-domain --all-features --locked  PASS
cargo clippy -p market-squawk-domain --all-targets --all-features --locked
  -- -D warnings                                                              PASS
RUSTDOCFLAGS='-D warnings' cargo doc -p market-squawk-domain
  --all-features --no-deps --locked                                            PASS
./scripts/verify.sh                                                            PASS
git diff --check                                                               PASS
```

The repository gate included the exact Rust 1.97 workspace policy checks, strict locked
workspace-wide Clippy and tests, release build, rustdoc, CLI/offline mock smoke, and stdio MCP smoke.

No floating-point financial representation, unsafe code, credential behavior, outbound request,
or access-limit evasion capability was added.
