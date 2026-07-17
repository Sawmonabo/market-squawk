# Q2 A3 I02 Durable Terminalization RED Evidence

## Audit anchor

The RED suite is based on commit `a2a6e5c6609f2ea981cf2d8f8d60072a4a133d5f`. This is an
implementation anchor, not checkpoint approval. The quarter gate and independent re-review remain
required at the final clean, unchanged exact head.

## Required contract

An integrity, arithmetic, synchronization, or persistence failure in a restart-durable provider
budget must irreversibly terminalize the affected allocation, durably mark its last trusted
checkpoint disabled/terminal/poisoned when the store remains available, and invalidate the entire
durability session. Aliases, other scopes, future registration, clean shutdown, and later
availability generations must not restore authority. If the terminal checkpoint cannot be stored,
the previously durable `InUse` envelope must remain as the conservative unclean-restart barrier.

The tests cover fatal branches for availability leases, acquisition, relative and absolute
Retry-After handling, refusals, success recording, administrative disable, and permit release.
They inject state poison, monotonic regression, checked deadline failures, corrupt counters,
generation exhaustion, store rejection, permit underflow, and an already unavailable session.
Every applicable case checks alias and peer blast radius, future registration, clean shutdown,
durable state, and restart behavior.

## Evidence-based branch normalization

Two review labels were reconciled against the actual type and concurrency invariants:

- After checked `i64` wall-clock subtraction and a strict `delay > 0` test, conversion to `u64`
  cannot fail. The redundant conversion error arm will be removed rather than supplied with an
  impossible test. Wall subtraction overflow and monotonic deadline overflow remain separate,
  reachable fatal cases.
- Request and in-flight increment overflow cannot occur after valid configured-maximum checks.
  Injected `u32::MAX` and `u16::MAX` runtime states exceed their configured maxima and therefore
  must be detected first as terminal `StateCorrupt`, rather than misreported as ordinary quota or
  concurrency exhaustion.

A generation change after a lease is minted is not itself corruption: another worker may
legitimately consume the final slot before post-mint revalidation. Existing and new durable tests
therefore keep that outcome nonterminal. Likewise, excessive Retry-After is a durably restrictive
per-budget disable, not a session-integrity failure. Cooldown, exact quota exhaustion, and exact
concurrency exhaustion remain transient and recover through time or permit release.

## RED result

The focused command was:

```text
cargo test -p market-squawk-sources --lib \
  policy::persistence::tests::terminalization -- --nocapture
```

The suite compiled and ran 12 tests. The two classification controls passed:

- `over_policy_retry_after_is_durably_restrictive_without_invalidating_peers`
- `cooldown_quota_concurrency_and_post_mint_generation_changes_remain_transient`

Ten terminalization tests failed because the pre-fix implementation only revoked a local
availability generation or locally latched persistence failure. The first minimal RED remains:

```text
fatal entry point recovered: AvailabilityLease
```

Additional failures demonstrate that aliases can recover, peer scopes remain usable, and failed
terminal stores do not currently invalidate the session. These are intentional RED failures, not
passing-gate evidence.
