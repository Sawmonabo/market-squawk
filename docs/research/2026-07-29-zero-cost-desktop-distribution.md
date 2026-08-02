# Zero-cost macOS and Windows desktop distribution

Market Squawk must remain installable and usable without a paid developer membership, paid
certificate, or paid signing service. This report separates that product requirement from the
native trust signals controlled by Apple and Microsoft.

| Field | Value |
| --- | --- |
| Document type | Technical research and release-policy input |
| Audience | Product owners, release engineers, security reviewers, and maintainers |
| Status | Current; independently evidence-audited |
| Research date | 2026-07-29 |
| Repository integration base | Commit `775e21da52a8eb08d812bee01e172f55ad93e7ef`, tree `46c4b357f3db2a640031c5791824ca7694715eef` |
| Final research SHA-256 | `2347c177b8709ec785f98201b9453071e4c39255df39dc9a88961785c3620e6e` |
| Evidence-audit SHA-256 | `a291de5c5b06d2c9f19a827f2a9add366f533fdfd02a5c16b46f1ad5941dc32f` |
| Audit verdict | `PASS` |

## Table of Contents

- [Decision summary](#decision-summary)
- [Trust properties](#trust-properties)
- [macOS result](#macos-result)
- [Windows result](#windows-result)
- [Repository integration result](#repository-integration-result)
- [Market Squawk release policy](#market-squawk-release-policy)
- [Required evidence](#required-evidence)
- [Refresh gates](#refresh-gates)
- [Primary sources](#primary-sources)

## Decision summary

The evidence does not establish a generally available, zero-fee route that gives an ordinary
individual project the normal trusted-by-default experience on both macOS and Windows.

The zero-mandatory-cost Market Squawk release therefore uses an asymmetric policy:

1. GitHub release assets, exact checksums, manifests, SBOMs, and GitHub artifact attestations form
   the mandatory cross-platform provenance layer.
2. macOS packages remain usable without Apple credentials, but must not be described as Developer
   ID signed, notarized, or trusted by Gatekeeper unless Apple has actually supplied that authority
   and the exact final artifact passes verification.
3. Microsoft Store signing of an MSIX is the primary no-fee Windows native-trust candidate.
4. SignPath Foundation is the candidate for no-fee direct MSI/EXE Authenticode signing if Market
   Squawk is accepted into its open-source program.
5. Neither Store nor SignPath enrollment is a runtime requirement. Direct GitHub installation and
   local source installation remain available so the application itself never requires a paid
   service or a cloud service.

This is a release-channel decision. It does not weaken the complete-product manifest, immutable
installation, rollback, repair, data-preserving uninstall, or exact-head verification contracts.

## Trust properties

The word “signed” is not sufficient to describe a release. Market Squawk records these properties
independently:

| Property | Evidence |
| --- | --- |
| Artifact integrity | Exact size and SHA-256 identity |
| Build provenance | GitHub artifact attestation bound to repository, workflow, commit, and final bytes |
| Publisher identity | Apple Developer ID, Microsoft Store signature, or accepted Authenticode chain |
| Platform admission | Gatekeeper/notarization or the applicable Windows policy result |
| Channel acceptance | Store or package-manager review of the exact submitted artifact |
| Software safety | Separate testing, dependency review, vulnerability review, and release approval |

Checksums and attestations materially improve integrity and provenance. They do not cause
Gatekeeper, SmartScreen, or Smart App Control to recognize a publisher.

## macOS result

Apple documents Developer ID as the direct-distribution identity for software outside the Mac App
Store. Obtaining and using that identity requires Apple Developer Program membership, and
notarization requires a valid Developer ID signature. Apple lists the ordinary membership at
USD 99 per year.

Apple has a fee-waiver process, but it is limited to qualifying nonprofit legal entities,
accredited educational institutions, and government entities. It explicitly excludes individuals,
sole proprietors, and single-person businesses. Market Squawk has no evidence that its publisher
qualifies or that Apple has approved a waiver.

Ad-hoc signing and self-signing are free development mechanisms. They do not establish an Apple
publisher identity and are not accepted substitutes for Developer ID notarization. Apple documents
**Open Anyway** as a deliberate user exception for software from an unidentified developer, not as
the trusted-by-default distribution path.

Consequently, a zero-fee Market Squawk macOS release can provide:

- a complete DMG/application and the one-command installer;
- immutable checksums, manifests, SBOMs, and GitHub attestations;
- a source-build installation path;
- accurate guidance for the normal macOS decision when the system does not recognize the
  publisher.

It cannot truthfully promise Developer ID, notarization, or a warning-free first launch without
Apple-granted authority.

## Windows result

Microsoft documents two materially different Store paths:

- An **MSIX** submitted to the Microsoft Store is re-signed by Microsoft after certification.
  Microsoft describes that signing as free, and its current onboarding flow has no registration
  fee for new individual developers in supported markets.
- An externally hosted **MSI or EXE** listed in the Store is not re-signed. The installer and its
  included executable files must already have a qualifying signature.

SignPath Foundation offers a no-fee Authenticode service to accepted qualifying open-source
projects. Acceptance is discretionary. The Foundation is the certificate publisher, production
requests require controlled provenance and approval, and a valid signature does not guarantee
immediate SmartScreen reputation.

The release policy therefore treats:

- Store MSIX as the preferred no-fee Windows native-trust channel;
- SignPath as the preferred no-fee direct MSI/EXE signing application;
- direct unsigned MSI/EXE as provenance-verifiable downloads that must not be labelled as having a
  verified Windows publisher.

## Repository integration result

The integration audit at the recorded repository base found:

- the current CI package matrix successfully builds Windows MSI and NSIS outputs without signing;
- the stable release workflow currently requires Apple and Windows certificate secrets before any
  platform release can proceed;
- the same workflow labels all platform outputs as signed and requires notarization or
  Authenticode evidence;
- the current Tauri 2 Store guidance states that Tauri emits EXE and MSI installers, not MSIX, and
  that those installers must already be signed for Store submission;
- GitHub artifact attestation, exact checksums, draft assembly, remote digest verification, and
  four-platform installation smoke infrastructure already exist.

The mandatory signing-secret gate conflicts with the zero-mandatory-cost product constraint. It
must become a truthful release policy in which the provenance-verified core release does not depend
on paid credentials, and native-trust channels are admitted only when their actual authority is
present.

Adding Store MSIX is real product work rather than a configuration rename. It requires an MSIX
manifest, Store-assigned identity, complete-bundle layout, data/update/uninstall compatibility,
Windows App Certification Kit evidence, Store submission, and installed-product verification.

## Market Squawk release policy

The implementation must:

1. remove paid Apple and Windows credentials as prerequisites for the core stable release;
2. record each artifact’s actual trust mode rather than applying one global “signed” label;
3. verify and attest the exact final bytes after every packaging or signing mutation;
4. publish complete Linux, macOS, and Windows artifacts with accurate platform-trust descriptions;
5. preserve the one-command installer and local source-build route;
6. implement and prove a separate Store-MSIX lane before claiming free Windows Store signing;
7. prepare the existing MSI/EXE pipeline for SignPath only after program acceptance and exact
   workflow configuration are available;
8. retain any later Apple signing/notarization path as optional authority, never as a requirement
   for Market Squawk to remain functional; and
9. keep package-manager entries bound to immutable canonical release assets and exact digests.

The release page and installer must not turn an unsigned, ad-hoc, self-signed, or merely attested
artifact into a native-publisher claim.

## Required evidence

Before the core release is published:

- all four packages install, launch the desktop, execute CLI diagnostics, import the sealed Python
  product, expose the bounded MCP registry, repair, and uninstall on the supported target;
- the public installer verifies the selected artifact and installs the same complete product;
- release metadata states the exact native-trust mode for every artifact;
- checksums and attestations match the final public bytes;
- no release job requires an unavailable paid credential.

Before the Microsoft Store route is presented as available:

- the exact complete product is packaged as MSIX with the Store-assigned identity;
- Windows App Certification Kit and Store certification pass;
- install, launch, update, rollback, local-data, MCP, and uninstall behavior are proven on a clean
  supported Windows system; and
- the Store-delivered final artifact has the expected Microsoft signature.

Before SignPath-signed direct packages are presented as available:

- SignPath has accepted the project and supplied the exact project/signing-policy identifiers;
- the workflow submits only an immutable reviewed artifact from the frozen release commit;
- the returned MSI/EXE and nested executable files have the expected chain and timestamp; and
- SmartScreen and Smart App Control outcomes are reported from actual tests rather than inferred
  from the presence of a signature.

## Refresh gates

Refresh this report before changing release authority when:

- Apple changes membership, fee-waiver, Developer ID, notarization, or Gatekeeper policy;
- Microsoft changes Store enrollment, Store signing, MSIX, SmartScreen, or Smart App Control policy;
- Tauri gains supported MSIX generation or materially changes its Store guidance;
- SignPath changes eligibility, terms, publisher identity, or workflow requirements;
- Market Squawk receives or loses Store, SignPath, or Apple authority; or
- the supported target, installer, complete-bundle, update, or uninstall topology changes.

## Primary sources

- [Apple Developer ID](https://developer.apple.com/support/developer-id/)
- [Apple notarization requirements](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution)
- [Apple Developer Program fee waiver](https://developer.apple.com/help/account/membership/fee-waivers/)
- [Apple guidance for unidentified developers](https://support.apple.com/102445)
- [Microsoft code-signing options](https://learn.microsoft.com/windows/apps/package-and-deploy/code-signing-options)
- [Microsoft free individual Store enrollment](https://learn.microsoft.com/windows/apps/publish/whats-new-individual-developer)
- [Microsoft Windows distribution-path comparison](https://learn.microsoft.com/windows/apps/package-and-deploy/choose-distribution-path)
- [Microsoft MSI/EXE Store requirements](https://learn.microsoft.com/windows/apps/publish/publish-your-app/msi/app-package-requirements)
- [Microsoft MSIX packaging documentation](https://learn.microsoft.com/windows/msix/)
- [Tauri 2 Microsoft Store guidance](https://v2.tauri.app/distribute/microsoft-store/)
- [SignPath Foundation](https://signpath.org/)
- [SignPath origin verification](https://docs.signpath.io/origin-verification/)
- [GitHub artifact attestations](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations)
- [Sigstore threat model](https://docs.sigstore.dev/about/threat-model/)
- [NIST security considerations for code signing](https://nvlpubs.nist.gov/nistpubs/CSWP/NIST.CSWP.01262018.pdf)
