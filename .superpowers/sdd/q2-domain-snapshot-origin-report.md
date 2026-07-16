# Quarter 2 domain prerequisite: snapshot origin vs current book state

Date: 2026-07-16

## Defect

`BookStateBinding` was documented as the exact current canonical order-book state, but live
qualification required that current identity/digest to equal the initializing snapshot's
identity/digest. That made a truthful post-delta state impossible: applying a real delta normally
changes current state while retaining its snapshot origin.

## TDD evidence

The first focused run failed at compilation because tests required the absent
`BookStateBinding::new_with_snapshot_origin`, `snapshot_state_id`, and `snapshot_state_digest`
contracts:

```text
cargo test -p market-squawk-domain --test live_trust_contracts --locked
error[E0599]: no associated function or constant named `new_with_snapshot_origin`
error[E0599]: no method named `snapshot_state_id`
error[E0599]: no method named `snapshot_state_digest`
```

The implementation then separated current state identity/digest from immutable snapshot-origin
identity/digest. `BookStateBinding::new` remains the snapshot convenience constructor where both
are equal; `new_with_snapshot_origin` constructs an evolved current state. Qualification compares
`InitializedSnapshot` to the explicit origin and additionally requires a `BookSnapshot` event's
current state to equal that origin.

Regression tests prove:

- a BookDelta assessment remains valid when its current state differs from its snapshot origin;
- a BookSnapshot assessment rejects differing current/origin state;
- a BookSnapshot with identical current/origin state remains valid; and
- snapshot origin identity and digest cannot be transplanted between assessment bindings.

## Verification

```text
cargo test -p market-squawk-domain --test live_trust_contracts --locked
# 17 passed

PROPTEST_CASES=4096 cargo test -p market-squawk-domain --all-features --locked
# all unit, integration, property, and doc tests passed

cargo fmt --all --check
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
git diff --check
# passed
```
