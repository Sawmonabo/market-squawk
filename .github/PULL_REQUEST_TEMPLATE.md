## Outcome and scope

Describe the user-visible or architectural outcome, the exact scope of this change, and any
deliberate non-goals. Link the issue, plan, decision record, or review finding when one exists.

## Security and authority impact

- [ ] This change introduces no new credential, network, filesystem, execution, order, risk-bypass,
      provider-access, or data-classification authority.
- [ ] Any new authority is explicitly modeled, fail-closed, bounded, documented, and covered by
      negative tests.
- [ ] Provider terms, source coverage, provenance, data quality, and execution eligibility remain
      explicit where applicable.
- [ ] Security-sensitive details are reported privately through [SECURITY.md](../SECURITY.md), not
      disclosed in this pull request.

Do not paste credentials, private portfolio data, exchange secrets, proprietary datasets, or
unredacted logs into a pull request.

## Verification evidence

Record the exact commit and fresh commands run against it. Distinguish focused lane evidence from
the clean exact-head integration gate and from hosted CI.

```text
Commit:
Focused commands:
Full local gate:
Hosted checks:
```

## Blast radius and rollback

List affected crates, adapters, schemas, persisted formats, public APIs, command behavior, security
boundaries, and operational state. Explain compatibility or migration handling and the safe rollback
or forward-recovery path.

## Reviewer checklist

- [ ] The change matches the linked requirement or finding without silent scope expansion.
- [ ] Invariants, error paths, cancellation, bounds, and adversarial cases are tested.
- [ ] Financial units, precision, time semantics, provenance, and data quality remain explicit.
- [ ] Dependencies and external actions are justified, reviewed, and immutably pinned where required.
- [ ] Documentation, migration notes, and audit evidence are current and do not overclaim capability.
- [ ] No generated artifact, credential, private data, or access-control-evasion mechanism is present.
- [ ] All substantiated review findings are resolved before integration approval.
