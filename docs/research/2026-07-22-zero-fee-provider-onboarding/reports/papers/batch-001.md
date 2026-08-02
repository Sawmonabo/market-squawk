# Academic Papers Batch 001: OAuth Composition and Financial-Grade Profiles


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews three original research papers relevant to a multi-provider local authorization
portal. It distinguishes formal proofs from empirical measurements and does not use paper findings
as evidence that a specific provider supports OAuth or any particular extension.

## Sources Reviewed

### PAPER-009 — A Comprehensive Formal Security Analysis of OAuth 2.0

- **Authors:** Daniel Fett, Ralf Küsters, and Guido Schmitz.
- **Source/date:** arXiv:1601.01229v4, revised 2016-08-08; abridged version at CCS 2016.
- **Method:** formal OAuth model in an expressive web model, covering four grant types and malicious
  relying parties, identity providers, and browsers.
- **Result:** the authors found four attacks, proposed mitigations, and proved authorization,
  authentication, and session-integrity properties for their corrected modeled profile.
- **Limitation:** a formal model and assumptions are not a conformance test or proof for Market
  Squawk, a provider implementation, a browser, or a selected library.
- **Relevance:** supports complete participant/session/issuer binding rather than isolated checks.
  [Primary paper](https://arxiv.org/abs/1601.01229)

### PAPER-010 — Universal Cross-app Attacks

- **Authors:** Kaixuan Luo, Xianbo Wang, Pui Ho Adonis Fung, Wing Cheong Lau, and Julien Lecomte.
- **Source/date:** 34th USENIX Security Symposium, August 2025, pages 3221–3238.
- **Method:** COVScan semi-automated black-box profiling plus a measurement study of 18 consumer or
  enterprise integration platforms.
- **Result:** the paper reports 11 platforms vulnerable to cross-app account takeover and another 5
  vulnerable to cross-app request forgery, both rooted in insufficient app differentiation.
- **Limitation:** studied multi-app cloud integration platforms and disclosed deployments; results do
  not measure Market Squawk or prove that every local portal has the same flaws.
- **Relevance:** directly supports namespacing and binding provider, adapter, account, client,
  redirect, and one-time authorization transaction.
  [USENIX paper page](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan)

### PAPER-012 — An Extensive Formal Security Analysis of the OpenID Financial-grade API

- **Authors:** Daniel Fett, Pedram Hosseyni, and Ralf Küsters.
- **Source/date:** arXiv:1901.11520v1, 2019-01-31; abridged version at IEEE Security & Privacy 2019.
- **Method:** formal FAPI profiles and security features in the Web Infrastructure Model with defined
  authentication, authorization, and session-integrity properties.
- **Result:** the authors found severe attacks in the analyzed profile, developed mitigations, and
  proved the corrected modeled version under stated assumptions.
- **Limitation:** the analyzed 2019 profile and formal assumptions do not establish current provider
  support, conformance, or a release-ready implementation.
- **Relevance:** if execution-capable broker OAuth is later admitted, adopt one complete supported
  high-assurance profile and conformance boundary rather than selecting disconnected mechanisms.
  [Primary paper](https://arxiv.org/abs/1901.11520)

## Findings

1. **Confirmed research result:** formal analyses found that familiar OAuth/FAPI mechanisms can
   remain insecure when their composition, participant binding, or assumptions are incomplete.
   [OAuth analysis](https://arxiv.org/abs/1601.01229),
   [FAPI analysis](https://arxiv.org/abs/1901.11520)
2. **Confirmed empirical result:** multi-app integration platforms can create cross-app compromise
   when authorization transactions are not differentiated by application.
   [USENIX 2025](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan)
3. **Engineering inference:** Market Squawk should own a typed, one-time authorization transaction
   binding provider, adapter, account, client, issuer, redirect, state, PKCE verifier, scopes,
   initiation session, expiry, and terminal consumption. Generic callback or token slots shared
   across providers are not acceptable.
4. **Engineering inference:** research strengthens the need for a coherent capability-gated profile;
   it does not enable OAuth, device authorization, DCR, or FAPI for a provider whose official
   documentation has not admitted them.

## Evidence Table

| Claim | Source | Evidence type | Confidence | Notes |
| --- | --- | --- | --- | --- |
| Complete profile composition matters | PAPER-009 | Formal result under model assumptions | High for modeled system | Not implementation proof |
| Multi-app differentiation prevents cross-app confusion classes | PAPER-010 | Empirical black-box study | High for studied platforms | Local-portal application is inference |
| Financial-grade controls must be analyzed as a coherent profile | PAPER-012 | Formal result under model assumptions | High for modeled profile | Current provider/profile conformance still required |
| Typed per-provider transaction binding is required | PAPER-009, PAPER-010, PAPER-012 | Engineering synthesis | Medium-high | Must be verified behaviorally in Market Squawk |

## Limitations and Non-Findings

- No paper establishes a provider's current endpoint, scope, client-registration, pricing, rights,
  or operational support.
- Formal proof depends on model scope and assumptions; empirical platform measurements do not prove
  universal prevalence.
- The papers do not evaluate Market Squawk, oauth2-rs, the existing encrypted vault, or any selected
  OS secret store.

## Source List

- PAPER-009: [arXiv:1601.01229v4](https://arxiv.org/abs/1601.01229)
- PAPER-010: [USENIX Security 2025](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan)
- PAPER-012: [arXiv:1901.11520v1](https://arxiv.org/abs/1901.11520)

All three are first-class inventory records assigned to `papers-batch-001` with access date and
response digest.
