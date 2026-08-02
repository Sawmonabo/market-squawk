# Zero-cost desktop distribution research evidence audit

| Field | Value |
| --- | --- |
| Document type | Research evidence audit |
| Audience | Product owners, release engineers, security reviewers, and maintainers |
| Verdict | `PASS` |
| Audit date | 2026-07-29 |
| Audited report | [Zero-cost macOS and Windows desktop distribution](../research/2026-07-29-zero-cost-desktop-distribution.md) |
| Frozen deep-research report | `2347c177b8709ec785f98201b9453071e4c39255df39dc9a88961785c3620e6e` |
| Frozen deep-research audit | `a291de5c5b06d2c9f19a827f2a9add366f533fdfd02a5c16b46f1ad5941dc32f` |
| Source inventory | `57cf8b5319e7e48720e84342f5696dc28df3f32a000da943a60b41ee9d0c512d` |

## Table of Contents

- [Verdict](#verdict)
- [Evidence coverage](#evidence-coverage)
- [Supported conclusions](#supported-conclusions)
- [Limitations](#limitations)
- [Refresh gate](#refresh-gate)

## Verdict

`PASS`

The frozen evidence set supports the report’s strict conclusion: native platform trust,
publisher identity, package-channel acceptance, artifact provenance, and software safety are
separate properties, and no reviewed source establishes one generally available zero-fee route
that supplies the ordinary trusted-by-default experience on both macOS and Windows.

The maintained report applies that finding to the repository’s zero-mandatory-cost requirement
without claiming that Store MSIX, SignPath, Apple waiver eligibility, or native warning behavior
has already been obtained.

## Evidence coverage

The deep-research run inventoried 84 sources and assigned 70 to 17 bounded reports:

| Category | Assigned sources | Batches |
| --- | ---: | ---: |
| Maintained GitHub repositories | 16 | 4 |
| Academic and research papers | 18 | 5 |
| Official documentation | 15 | 4 |
| Reputable primary or expert sources | 21 | 4 |

All four category syntheses and all 17 batch reports were present. The structural validator passed
after the final report and independent audit were written. No decision-changing unsupported claim,
missing category, material freshness defect, lost non-finding, or overclaimed implementation
conclusion was found.

## Supported conclusions

The evidence supports:

1. Apple Developer ID/notarization is not a generally available zero-fee authority for an ordinary
   individual project.
2. Apple’s fee waiver is narrow, conditional, and not established for Market Squawk.
3. Microsoft Store MSIX signing is the strongest documented no-fee Windows native-trust candidate.
4. Store-listed MSI/EXE artifacts must already be signed and therefore do not obtain a free
   signature merely by being listed.
5. SignPath Foundation is a real but discretionary no-fee Authenticode program for qualifying
   accepted open-source projects.
6. GitHub/Sigstore attestations and checksums strengthen provenance without replacing native
   platform trust.
7. Package managers add discovery, integrity checks, or moderation without donating publisher
   identity.
8. The core release can remain zero-mandatory-cost only if its release contract represents actual
   trust evidence per artifact and does not require unavailable paid credentials.

## Limitations

The research does not prove:

- Apple fee-waiver or SignPath eligibility for Market Squawk;
- Microsoft Store account approval;
- compatibility of the complete Market Squawk topology with MSIX;
- Store certification;
- a warning-free direct download;
- native signing, notarization, or Store acceptance of any exact Market Squawk artifact; or
- a total operating-cost model.

Those are external or implementation predicates, not defects hidden by the research verdict.

## Refresh gate

Repeat the focused policy and implementation review before a release-authority change if Apple,
Microsoft, Tauri, SignPath, GitHub attestation, or supported-package requirements change; if the
project obtains or loses a signing/Store authority; or if the complete installed-product topology
changes.
