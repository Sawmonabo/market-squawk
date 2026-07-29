# Cross-platform installation research evidence audit

| Metadata | Value |
| --- | --- |
| Document type | Research evidence audit |
| Audience | Product owners, maintainers, release engineers, security reviewers |
| Verdict | `PASS_WITH_NOTES`; implementation decisions resolved 2026-07-29 |
| Audit date | 2026-07-28; decision refresh 2026-07-29 |
| Repository audit anchor | `e6f77d564b00a6e6911c30be60d441f0576e9e08` |
| Audited report | [Cross-platform installation and guided setup](../research/2026-07-28-cross-platform-installation-and-guided-setup.md) |

## Table of Contents

- [Verdict](#verdict)
- [Evidence coverage](#evidence-coverage)
- [Citation and source quality](#citation-and-source-quality)
- [Supported conclusions](#supported-conclusions)
- [Notes and resolved decisions](#notes-and-resolved-decisions)
- [Refresh gate](#refresh-gate)

## Verdict

`PASS_WITH_NOTES`

The evidence supports the hybrid release, native-bootstrap, and Market Squawk-owned setup
architecture as the recommended design direction. It also supports the conclusion that Qt Installer
Framework is the strongest all-in-one fallback and that Tauri should not be introduced solely for
installation.

The original notes concerned product-policy and support-matrix decisions. The product owner
subsequently approved the complete default, the curl and native-package channels, uv, Python 3.14,
and the permanent Tauri desktop. The 2026-07-29 refresh freezes the remaining decisions in the
audited report and ADR 0006. Native installed-product evidence remains a release gate rather than a
research defect.

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

## Notes and resolved decisions

The maintained report now records:

1. stable native packages require maintainer-supplied platform signing while installation and
   runtime remain free to users;
2. Linux x64, Windows x64, macOS Intel, and macOS Apple Silicon are the V1 targets;
3. every route carries the complete Rust, desktop, uv, CPython 3.14.6, and locked Python product;
4. native packages and the one-line curl route are first-class;
5. one prior known-good version is retained; and
6. bundle, entry, and manifest size ceilings are fixed.

Current sealed Python evidence is still insufficient for a complete Windows/macOS/Linux
installation claim. That coverage gap remains explicit until native artifacts pass the
installed-product workflow.

## Refresh gate

Refresh this evidence before implementation planning when any of the following is true:

- the repository audit anchor materially changes the release composition or portal;
- cargo-dist, Qt IFW, Tauri, cargo-packager, or uv has a material release;
- Apple, Microsoft, GitHub, or Linux distribution/signing requirements change;
- the supported operating-system or architecture matrix changes;
- the Python lock, CPython version, or wheelhouse policy changes.

The refresh must update the research report and this audit together.
