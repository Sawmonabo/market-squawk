# Quarter 1 Final Audit-Truth Correction Report

Date: 2026-07-16

Branch: `fix/q1-final-audit-truth`

Exact base: `7c5051237b97b92ec5142301c903c34d41b81a95`

## Result

The Quarter 1 target architecture, contract-decision record, and Stage 1 implementation plan now
distinguish source-supplied identity claims from authority. Provider revision and predecessor values
are retained as caller/source-supplied claims bound to exact content evidence. The binding preserves
the association but does not prove revision authority.

Authority must be established separately by the applicable registered source and source-specific
adapter verification; the caller/source-supplied values do not establish it. This is a fail-closed
future requirement: source registration and adapter verification are not represented as completed
Quarter 1 capabilities.

`ProviderIdentityEvidence` is now documented consistently as retaining zero or more bounded,
canonical, version-pinned locators. The explicit bound is
`ProviderIdentityEvidence::MAX_LOCATORS = 64`. Locator metadata is non-substantive retrieval
metadata: it does not participate in assertion identity and never replaces exact content evidence.

The historical provider-evidence lane report now opens with a prominent `SUPERSEDED` banner. The
banner explains the current locator semantics and directs readers to the Quarter 1 checkpoint
correction report, current target architecture, and Quarter 1 contract decisions.

This lane changed documentation and persistent documentation-policy tests only. It did not edit
Rust source, provider adapter documentation, or provider behavior.

## Test-driven correction evidence

### Cycle 1: stale authority and locator semantics

The first test-first change added persistent assertions for the three governing documents and the
historical report banner.

RED:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 11 tests
FAILED (failures=7)
```

The seven failures covered all three provider-identity sections, all three exact-identity sections,
and the missing directional supersession banner.

GREEN after the first documentation correction:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 11 tests
OK
```

### Cycle 2: fail-closed authority boundary

The persistent assertions were then tightened before the documents to require modal language that
does not imply Quarter 1 already establishes source authority.

RED:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
Ran 11 tests
FAILED (failures=6)
```

The six failures covered the provider-identity and exact-identity sections in each governing
document because they still used present-tense authority language.

GREEN after applying the fail-closed language:

```text
python3 -m unittest scripts.tests.test_documentation_contracts
...........
Ran 11 tests in 0.006s
OK
```

## Policy and consistency checks

The post-correction policy gate passed:

```text
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
Ran 28 tests
OK

python3 scripts/check_brand.py
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
git diff --check
```

The exact brand-compatibility allowances were moved only to the new line positions produced by
readable Markdown reflow. Their approved containers and occurrence counts are unchanged.

The complete `./scripts/verify.sh` gate passed with exit code zero after the correction. It covered
the 28-test Python policy suite, brand and workspace policies, formatting, warning-denied locked
workspace Clippy, locked workspace tests, locked doctests, locked release build, warning-denied
rustdoc, debug build, CLI identity/help, the deterministic 101-event offline mock, and the local
stdio MCP smoke test. The same gate was rerun after this report was finalized and force-added, so
the final result covers the complete staged tree.
