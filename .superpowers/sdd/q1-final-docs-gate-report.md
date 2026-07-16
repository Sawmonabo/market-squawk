# Q1 Final Documentation and Verification Gate Report

Date: 2026-07-16

Branch: `fix/q1-final-docs-gate`

## Result

Quarter 1's authoritative target architecture and Stage 1 plan now name the hardened domain APIs
instead of superseded proposal names. The verification entry point explicitly runs locked,
all-feature workspace doctests. The Quarter 1 research decision record now retains reproducible FIX
and Chain Agnostic source metadata, per-URL access instants/timezones, rendered editions/profiles,
response-body SHA-256 digests, byte counts, relevant sections, and canonical links.

This lane changed documentation, repository policy scripts/tests, and research evidence only. It did
not edit Rust domain behavior.

## Test-first verification-gate change

RED:

```text
python3 -m unittest scripts.tests.test_verify_script
```

The new regression test failed because `scripts/verify.sh` did not contain the exact required
`cargo test --doc --workspace --all-features --locked` command.

GREEN:

```text
python3 -m unittest scripts.tests.test_verify_script
.
Ran 1 test in 0.000s
OK
```

The production change was one explicit doctest command in `scripts/verify.sh`. The complete Python
policy suite subsequently passed 17 tests.

## Authoritative API reconciliation

- Replaced the obsolete assessment-status proposal names with `AssessmentStatus` and
  `assessment_status_at(at)`.
- Documented `EligibilityFailures`, `failures()`, and `has_failure(EligibilityFailure)` as audit
  diagnostics; `QualificationAssessment` has no execution-eligibility API.
- Corrected `ExecutionEligibility::Ineligible` and `CaptureIntegrityState::Incomplete` to their unit
  variants/current operational form.
- Required invariant-preserving `QualificationAssessment` deserialization that recomputes and
  verifies derived quality, failures, evaluation time, and deadline.
- Required explicit `LiveProvenance::available_at()`, enforced
  `received_at <= available_at <= ingested_at`, a durable assessment reference rather than an
  embedded assessment, and archive-only unit `Ineligible` status.
- Documented algorithm-qualified payload digests and rule/version-qualified canonical-state
  digests using the finalized neutral digest APIs.
- Updated Task 4's file list and commands to the actual domain test targets.

## Identity and provider semantics

- Added independent first/last-trade, expiration, notice, delivery, and settlement lifecycle
  requirements; FIX tags 200, 541, 610, and 611 remain independent claims.
- Distinguished generic CAIP-2 envelope validation from `eip155` and `solana` namespace-profile
  qualification.
- Defined deterministic provider-registry ingestion: content-equivalent reingestion creates no
  second logical assertion, deterministically coalesces bounded locator and observation metadata,
  and returns `ObservationCoalesced`; an exact repeat with no new metadata leaves canonical state
  unchanged. Same-revision disagreement is retained/quarantined, and valid newer revisions append.
- Corrected language that treated mutable URLs or aliases as immutable. Provider identity evidence
  always includes an algorithm-qualified content digest; an optional locator carries explicit
  reference and version identities but remains non-authoritative retrieval metadata.

## Persisted external evidence

Fresh fetch instant for FIX and namespace profiles:
`2026-07-16T05:27:26-04:00` (`America/New_York`, EDT). The base CAIP-2 page was fetched at
`2026-07-16T05:38:32-04:00`.

```text
FIX tag 200          7721110c47caf818497ac2b23d7ca7f12cd43278fe416ef2e9ddaa9652ba20b5  EP307
FIX tag 541          187ecfeb096a0be4a2a23175f717857960514fb20452328e2df0ba982678adee  EP307
FIX tag 610          b16873961ae1c27be3c3bd914cbbc841cfd503fb371aef5ec291a91d740e3191  EP307
FIX tag 611          27b63acc02ddc4437dc454e6bb79dbc54a6d68428a7ecc1acd1653764067e3c9  EP307
FIXML datatypes      7ba910d037e37c57056db1cbdb65cd84b1790326d5666dd5a94f856ffc43a586  EP307
CAIP-2               f2995ed64502408d69e315b8736e5acf96dd7f85a5f3702b1a35b053674347d9  Final
EIP155 profile       423487876763c2922736a9d274f87f3660e7ea3350724272913c7fc39b91e05e  Draft
Solana profile       5598020d520135b0b1d84ad89833785eb7f425b40620941e02d29b69165a12ad  Draft
```

The earlier domain-evidence report's un-hashed EP302 observation for tag 611 is preserved as an
evidence-history discrepancy. The current fetched body renders EP307; the moving alias and absence
of retained earlier bytes prevent attributing the old value to either a transient source edition or
a reporting error.

## Verification evidence

The following policy/documentation checks completed successfully after the final edits:

```text
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_brand.py
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
bash -n scripts/verify.sh
authoritative stale-API ripgrep gate
changed-Markdown balanced-fence gate
documented domain test-target existence gate
git diff --check
```

`./scripts/verify.sh` completed with exit code zero after the script change. It ran the repository
policy gates, formatting, strict locked all-target/all-feature Clippy, locked all-target/all-feature
tests, the explicit locked all-feature doctest pass, release build, warning-denied rustdoc, debug
build, CLI help, the deterministic 101-event offline mock, and the timeout-bounded MCP smoke test.
The explicit doctest pass ran one compile-fail authority-boundary example successfully.

No performance, external-network adapter, or release-completeness claim is made by this report.
