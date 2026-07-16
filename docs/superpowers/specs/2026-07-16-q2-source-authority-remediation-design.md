# Q2 Source Authority Remediation Design

## Goal

Close the Quarter 2 source-authority defects that can admit oversized live payloads, allow stale or
future-dated authority evidence, let budget leases outlive an unavailable transition, divide a
provider budget across registry instances, mis-bind account-qualified policy, or partially mutate a
registry when an epoch cannot advance.

The design preserves the separation between source decoding, source qualification, live admission,
and the live hot path. Coordination and accounting occur at source registration, health reporting,
and bounded command admission rather than per book update.

## Security boundary

The system supports provider-authorized use only. It must never rotate identities, accounts,
fingerprints, proxies, or request origins to avoid a provider limit; bypass CAPTCHA or anti-bot
controls; or distribute requests to evade quotas. Provider responses that reduce or revoke
availability are authoritative fail-closed transitions. The implementation may coordinate multiple
legitimate callers so they share one stricter budget, but it may not manufacture extra capacity.

## Retained-memory accounting

Live admission uses closed-shape retained-size accounting. A routed command is charged for:

- its inline representation;
- every uniquely owned backing allocation, including outer vectors, nested book levels/changes, and
  decimal lexemes;
- shared authority and frame allocations exactly once per routed command, even when multiple
  observations reference them.

`BoundedVec` exposes checked allocation byte accounting based on its actual capacity. Construction
and deserialization normalize capacity where possible, but correctness does not depend on that
normalization. Recursive payload accounting includes the backing array for snapshot and delta
elements plus all nested strings. Arithmetic overflow rejects the batch; it never saturates into an
undercharge.

Tests cover each payload variant and boundary-size snapshots/deltas at 1, 10,000, and 20,000
elements. A live admission regression chooses a byte limit between the former shallow estimate and
the correct retained size and proves rejection.

## Trusted health time and temporal chain

Health authority uses one ordered chain:

```text
session_started_at <= snapshot.observed_at <= trusted_reported_at <= validation_at <= deadline
```

The registry owns a sealed clock. A session-issued reporter samples that clock and embeds an opaque,
non-serializable trusted observation in the update. Callers may describe when source evidence was
observed, but cannot forge the registry's trusted report time. Production uses system time; unit
tests inject a deterministic monotonic test clock through a non-public constructor.

Each health epoch records both a lower and upper validity bound. Every validated authority and live
lease checks the lower bound, deadline, current source epoch, current health epoch, and budget
availability generation. Backward time, future source observations, deadline overflow, or a clock
failure rejects without mutating current health.

## Budget authority and synchronous revocation

A process-wide coordinator interns budget allocations by canonical `BudgetScope`. The coordinator
stores weak references, while each registry catalog retains strong references for its registered
policies. This yields these invariants:

- identical active scopes and policies share one allocation across all registries and restored
  registries;
- a conflicting policy for an active scope is rejected under the same coordinator lock;
- a dead allocation can be reclaimed and replaced, with bounded cleanup on control-plane paths;
- batch restore preflights every scope and commits all coordinator entries atomically.

Each allocation owns an atomic availability generation in addition to its rate state. Acquiring
availability produces an unforgeable lease bound to that generation. Before an unavailable
transition returns—including retry-after, refusal, disable, poisoned state, clock regression, or
deadline overflow—the generation advances. Existing health and live leases therefore fail
immediately. Cooldown recovery may make new acquisition possible, but cannot reactivate an old
lease; newly qualified health is required.

`BudgetHealth` is derived synchronously from the allocation state. It is not accepted as a
caller-controlled claim for execution qualification.

## Authorization-bound budget scopes

Budget scopes are constructed or validated against the complete `AuthorizationGrant` mode and
basis:

- `PublicInterface` is provider-only;
- `UserAuthorized` is account-qualified by the exact authorization basis;
- remote `Licensed` access is account-qualified by the exact license basis;
- `UserOwnedLocal` has no remote provider budget.

Provider-only scope for account-qualified access, an extra account on a public interface, or an
alias that differs from the authorization basis is rejected. The mapping is exhaustive so adding a
new authorization mode creates a compile-time decision point.

## Failure atomicity

Source registration and health recording stage all fallible work before publishing state.
Registration validates metadata, computes the next epoch and complete history record, and resolves
coordinator conflicts before mutating entries or history. Epoch overflow therefore leaves entries,
history, and coordinator state unchanged.

Health recording validates the sealed temporal chain, budget state, qualification, cursor ordering,
and next health epoch before updating atomics or the persisted health cursor. Rejection preserves the
previous snapshot, cursor, qualification, and leases.

## Compatibility and blast radius

The public live/extraction source contracts remain distinct. The change deliberately affects source
metadata construction, registry/session health reporting, current-authority validation, and live
command admission. Adapter, live shard, execution/risk, persistence/restore, and task 6/7/8 tests
must be checked for assumptions that caller-supplied budget health or registry-local budgets were
authoritative.

No database, analytical query, Python, MCP, filesystem operation, or unrelated network request is
introduced into the live event-to-action path.

