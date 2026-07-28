# Cross-platform installation research evidence audit

| Metadata | Value |
| --- | --- |
| Document type | Research evidence audit |
| Audience | Product owners, maintainers, release engineers, security reviewers |
| Verdict | `PASS_WITH_NOTES` |
| Audit date | 2026-07-28 |
| Repository audit anchor | `f9b8b4e5cfb84b30a0a682fd7952e766a14b4ba1` |
| Audited report | [Cross-platform installation and guided setup](../research/2026-07-28-cross-platform-installation-and-guided-setup.md) |

## Table of Contents

- [Verdict](#verdict)
- [Evidence coverage](#evidence-coverage)
- [Citation and source quality](#citation-and-source-quality)
- [Supported conclusions](#supported-conclusions)
- [Notes and unresolved decisions](#notes-and-unresolved-decisions)
- [Refresh gate](#refresh-gate)

## Verdict

`PASS_WITH_NOTES`

The evidence supports the hybrid release, native-bootstrap, and Market Squawk-owned setup
architecture as the recommended design direction. It also supports the conclusion that Qt Installer
Framework is the strongest all-in-one fallback and that Tauri should not be introduced solely for
installation.

The notes concern product-policy and support-matrix decisions. They are not research defects and do
not authorize implementation.

## Evidence coverage

The source investigation inventoried 97 sources and assigned 90 to formal bounded review:

| Category | Reviewed | Result |
| --- | ---: | --- |
| Official documentation | 38 | Pass |
| Maintained GitHub repositories | 11 | Pass |
| Primary academic papers | 26 | Pass |
| Standards and reputable security sources | 15 | Pass |

A supplemental primary-source comparison covered cargo-dist 0.32, Qt Installer Framework 4.11,
Tauri 2, cargo-packager 0.11.8, Zero Install, uv, native packaging, and the existing local portal.

The research artifact structure, required report categories, final report, and evidence-audit shape
passed the deep-research structural validator before this maintained handoff.

## Citation and source quality

- Installer and platform behavior uses first-party cargo-dist, Qt, Tauri, uv, Apple, Microsoft,
  freedesktop.org, and GitHub documentation.
- Release-trust conclusions use primary SLSA, TUF, Sigstore, in-toto, OpenSSF, and NIST material.
- Repository evidence records exact versions or heads and does not treat popularity as proof.
- Academic claims retain study scale, method, safety-definition differences, age, and
  prepublication limits.
- Market Squawk recommendations are presented as inferences and not as prescriptions from an
  external source.

No decision-critical unsupported factual claim was found.

## Supported conclusions

The evidence supports:

1. separating release, native installation, and product setup authorities;
2. using cargo-dist as the release foundation rather than the complete setup authority;
3. reusing the current portal and one Rust desired-state engine for desktop, headless, and
   unattended setup;
4. using a pinned uv executable and release-bound managed Python/wheel artifacts without modifying
   system Python;
5. making every guided recommended route install the complete supported product;
6. verifying the complete bundle before atomic activation;
7. retaining a last-known-good version and preserving user data by default;
8. building and smoke-testing installation on each supported operating system;
9. keeping affected-change CI as scheduling evidence rather than exact-head release approval.

## Notes and unresolved decisions

Before implementation planning:

1. Decide whether the zero-mandatory-cost rule permits maintainer-funded platform signing while the
   product remains free to users.
2. Freeze the supported operating-system and architecture matrix.
3. Reconcile exact bundle contents against the then-current Rust workspace and Python release
   matrix.
4. Decide primary distribution channels, rollback retention, offline size budgets, and the Linux
   compatibility baseline.

Current sealed Python evidence is insufficient for a complete Windows/macOS/Linux installation
claim. That coverage gap must remain explicit until native artifacts pass the installed-product
workflow.

## Refresh gate

Refresh this evidence before implementation planning when any of the following is true:

- the repository audit anchor materially changes the release composition or portal;
- cargo-dist, Qt IFW, Tauri, cargo-packager, or uv has a material release;
- Apple, Microsoft, GitHub, or Linux distribution/signing requirements change;
- the supported operating-system or architecture matrix changes;
- the Python lock, CPython version, or wheelhouse policy changes.

The refresh must update the research report and this audit together.
