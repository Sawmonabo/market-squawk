# Provider-budget coordinator cooldown test flake

Date: 2026-07-17

## Result

Independent review of the hosted persistence-race remediation found an inherited flaky assertion in
`dropping_every_external_handle_preserves_refusal_disabled_and_terminal_state`. The issue existed at
the integrated base `2b708a5fc4c5d4d26b9381d8fffe93dd86a3d3c1`; it was not introduced by the
remediation branch.

A balanced 16-thread stress control produced the same failure on both revisions:

- remediation candidate `f902543b22aae85686f6c249aa8aeb2c6d47b91f`: 4 failures in 50 runs;
- exact base `2b708a5fc4c5d4d26b9381d8fffe93dd86a3d3c1`: 3 failures in 50 runs.

The finding remained release-blocking because an inherited scheduler-dependent test can still fail
the exact hosted candidate.

## Root cause

The test applied a refusal with the intentionally small one-millisecond backoff used by its policy
fixture, dropped the public budget handle, reacquired the process-coordinated handle, and called
`try_acquire`. It required that call to return the original cooldown deadline.

That assertion combined two independent contracts:

1. dropping every external authority handle must not replace or reset the process-owned canonical
   allocation; and
2. a cooldown must block acquisition only while its monotonic deadline remains current.

The first contract is the purpose of this coordinator test. The second contract legitimately stops
holding after one millisecond and is tested separately with controlled clocks. A loaded test runner
can deschedule the test across that deadline, after which `try_acquire` correctly follows the
expired-cooldown path and the test incorrectly reports a failure.

## Remediation contract

The coordinator test now observes allocation identity with a test-only weak reference. A weak
reference does not retain the allocation data or constitute a public authority handle, while its
control-block identity proves that re-registration returned the same process-owned allocation
rather than a newly initialized replacement.

After re-registration, the test inspects the coordinator-private state under its mutex and verifies
the exact refusal deadline and refusal count. The assertion therefore proves persistence of the
canonical allocation and request state without allowing scheduler delay or wall-clock passage to
change the expected result.

A separate runtime unit test uses the existing manual monotonic clock to prove the complementary
boundary contract: acquisition returns the same refusal deadline one nanosecond before expiry and
becomes ready exactly at expiry. Coordinator ownership and deadline enforcement are therefore
covered independently without a process-global clock override.

Production clock selection, cooldown expiry, acquisition behavior, coordinator ownership, and
public APIs are unchanged.

## Verification requirements

The frozen remediation candidate must pass:

- the exact coordinator test repeatedly under the same 16-thread stress conditions;
- the complete 110-test sources library suite repeatedly at 16 threads;
- all source targets and strict source Clippy;
- standalone `rustfmt --check` for changed nested `#[path]` modules;
- the complete local verifier and independent exact-hash review; and
- fresh hosted Ubuntu, macOS, and Windows jobs.
