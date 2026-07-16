# Q1 Final Provenance Documentation Correction Report

Date: 2026-07-16
Branch: `fix/q1-final-provenance-docs`
Reviewed base: `77a369441b756bdb8374114515cf895dd8035184`

## Result

Quarter 1's authoritative documentation now matches the implemented archive-only live provenance
contract. No Rust behavior, runtime authority, adapter, or later-stage implementation changed.

## Corrected contract

- `LiveProvenance` owns one complete `LiveEvidenceBinding`; it does not expose independently writable
  flattened source, venue, instrument, generation, channel, event, payload, or canonical-state
  identity fields.
- Its public source/instrument/venue/generation views delegate to the binding.
- Explicit source, receive, availability, and ingestion times remain distinct. Construction and
  deserialization continue to require `received_at <= available_at <= ingested_at`.
- The archive `record_state` is not serialized. Checked construction sets it, while deserialization
  reconstructs it from the optional retained assessment reference.
- `RecordedLiveProvenanceInput` accepts a caller-supplied archival classification plus an opaque
  assessment reference. The recorded construction path structurally requires the reference, and the
  wire rejects a `DirectVerified` record without one.
- `LiveProvenance` does not dereference or prove the external assessment relationship. The retained
  classification is an audit assertion, remains execution-ineligible, and must be independently
  revalidated by the future stateful live-plane authority issuer.

The Stage 1 implementation plan, global implementation plan, target architecture, and Quarter 1
contract-decision record all state the same boundary without implying a nonexistent derivation or
authorization guarantee.

## Regression gate

A persistent Python documentation-contract test now rejects:

- reintroduction of the flattened `LiveProvenance` example;
- omission of the binding or derived archive record state; and
- renewed claims that provenance validates the object named by an opaque assessment reference.

The repository's exact legacy-brand allowlist was remapped only for the documented line shifts.

## Test-first evidence

The initial narrow documentation assertion failed with:

```text
AssertionError: Task 4 still omits the actual binding field
```

After the correction, the same assertion completed with:

```text
Task 4 provenance documentation contract is current
```

## Verification evidence

The final branch was checked with:

```text
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_brand.py
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
bash -n scripts/verify.sh
authoritative Task 4 stale-contract assertion
changed-Markdown balanced-fence check
git diff --check
```

All completed successfully. `scripts/verify.sh` itself was not changed, so the docs-only correction
did not rerun the full Rust release/smoke pipeline already passed at the reviewed base.
