# Hosted persistence concurrency-test failure

Date: 2026-07-17
Candidate: `2b708a5fc4c5d4d26b9381d8fffe93dd86a3d3c1`
Hosted run: [GitHub Actions 29562087233](https://github.com/Sawmonabo/market-squawk/actions/runs/29562087233)

## Result

The candidate passed hosted macOS and Windows. The Linux `verify` job failed while running the
108-test `market-squawk-sources` library suite in parallel. Two deterministic lifecycle assertions
received synthetic `Store` failures instead of their expected linearization results:

```text
clean_close_serializes_against_and_rejects_a_waiting_budget_update
terminal_writer_owns_failed_overwrite_after_blocked_normal_store
```

All production authority-state cases, including all 16 platform authority-state tests, passed in
the same hosted run. The failures were isolated to the crate-private blocking persistence test
double.

## Root cause

`BlockingStore::store` used a one-second wall-clock timeout while waiting for the test controller to
release an intentionally blocked store call. Under the fully parallel hosted suite, the controller
was not rescheduled before that timeout. The test double then invented
`AuthorityStateStoreError::Unavailable`, producing `AuthorityPersistenceError::Store` in both
otherwise valid schedules.

The failure timing and results are consistent with that injected timeout:

- both failures returned the exact error synthesized by the blocking store timeout;
- neither failure reported a lifecycle-word, canonical-state, or production-store invariant
  violation; and
- the same exact tests passed locally when scheduled without hosted-suite contention.

The adjacent blocking clock test double had the same timeout pattern even though it did not fail in
this run. The review therefore treated the incident as a shared concurrency-fixture defect rather
than patching only the two observed tests.

## Remediation contract

The blocking store and clock now wait for an explicit controller release. The controller constructs
an owned release guard before it begins observing entry and receives that guard only after the
blocked operation has entered. Explicit `release` consumes the guard, while `Drop` releases on an
observation timeout, assertion failure, or early return. This provides two properties:

1. scheduler delay cannot be misrepresented as a production persistence or clock failure; and
2. a failed assertion cannot strand a worker indefinitely behind the test double.

A 30-second watchdog remains only at observation points that must detect a worker that never
entered or a peer that incorrectly waited. It is not used to determine the result of a store or
clock operation. `Condvar::wait_timeout_while` also handles spurious notifications without
misclassifying them as entry. Dedicated regressions prove that a worker entering after the observer
has timed out still observes the guard-owned release and terminates. Terminal-latch publication is
observed with the same elapsed-time watchdog instead of a scheduler-dependent fixed count of
`yield_now` calls.

Every blocking persistence and blocking clock call site was migrated to retain the guard through
the asserted linearization window. The short peer-response watchdogs were changed to the shared
test watchdog so a saturated runner does not create a false failure while still detecting a genuine
deadlock.

No production code, lifecycle transition, store contract, error mapping, or synchronization order
was changed.

## Verification

Before committing the remediation candidate:

- both formerly failing tests passed individually;
- all 110 sources library tests passed with 16 test threads;
- all sources tests and integration tests passed with all targets and features;
- strict all-target/all-feature sources Clippy passed; and
- formatting and diff checks passed.

The exact committed remediation must also pass the complete local verifier, independent review,
and a fresh hosted Ubuntu/macOS/Windows run before integration approval.
