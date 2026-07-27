# Reputable Sources Discovery Report

## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Candidate Sources](#candidate-sources)
- [Decision Notes by Candidate](#decision-notes-by-candidate)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps](#coverage-gaps)
- [Source List](#source-list)

## Research Scope

This discovery is anchored to **2026-07-15**. It selects eight primary or highly
authoritative source families that can constrain implementation and governance decisions for
Market Squawk's zero-mandatory-cost, self-hosted market-data, research, modeling, portfolio,
execution, and fair-value platform. The selected organizations are public data providers,
standards bodies, public-sector security authorities, a vendor-neutral security foundation, and
financial regulators or supervisory standard setters.

The scope covers four decision areas:

- Provider fair-access and rate-limit policies, including identification, throttling, caching,
  backoff, and documented request ceilings.
- Secure-development, application-control, and software-supply-chain baselines.
- The authority and limitations of accessible ASC 820 and IFRS 13 fair-value material.
- Risk-based model governance, validation, monitoring, portfolio stress testing, and scenario
  governance.

Every source was selected for an evidence or constraint that can be converted into a testable
Market Squawk policy. Compliance applicability is not assumed: Federal Reserve and Basel
materials are banking guidance, and their use for a personal/local platform is an implementation
inference, not a claim that Market Squawk is subject to those regimes. Similarly, OWASP ASVS is
used as a verification checklist, not as proof that a Rust CLI or MCP server is a web application.

Access date for every selected source is **2026-07-15**.

## Search Queries Used

The discovery reused authoritative pages already opened during official-documentation discovery
and performed only the following narrowly targeted authority checks:

1. `site:sec.gov webmaster frequently asked questions automated access 10 requests per second`
2. `site:bls.gov/developers api FAQs daily query limits registration CAPTCHA`
3. `site:csrc.nist.gov SP 800-218 Secure Software Development Framework SSDF official`
4. `site:owasp.org ASVS 5.0 official application security verification standard`
5. `site:openssf.org SLSA software supply chain official` and the linked approved SLSA v1.2 spec
6. `site:fasb.org ASC 820 fair value hierarchy official` and
   `site:ifrs.org IFRS 13 fair value measurement official`
7. `site:federalreserve.gov revised guidance model risk management SR 26-2 official`
8. `site:bis.org/bcbs stress testing principles portfolio risk current`

Search-result snippets were not used as final evidence. Selected results were opened on the
issuing organization's site, including the Federal Reserve's current 2026 attachment and the
approved SLSA v1.2 specification.

## Candidate Sources

| ID | Source | URL | Type | Credibility Signal | Freshness Signal | Priority | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R01 | SEC webmaster fair-access policy | [SEC Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | Public-provider access policy | SEC `.gov` statement of automated-access requirements | Current policy reviewed on 2026-07-15 | P0 | Provides a concrete aggregate request ceiling, declared-user-agent requirement, bulk-data preference, and enforcement context. |
| R02 | BLS Public Data API fair-use and quota policy | [BLS API FAQs](https://www.bls.gov/developers/api_faqs.htm) | Public-provider rate and registration policy | BLS `.gov` primary API policy | FAQ identifies live v1/v2 limits; accessed 2026-07-15 | P0 | Supplies exact registered and unregistered limits and makes registration a user-controlled, non-automated boundary. |
| R03 | NIST SP 800-218 SSDF v1.1 | [NIST SP 800-218](https://csrc.nist.gov/pubs/sp/800/218/final) | Public-sector secure-development framework | NIST final publication with DOI and change history | Final dated 2022-02-03; current project page accessed 2026-07-15 | P0 | Gives an outcome-oriented secure-development baseline spanning organizational preparation, software protection, secure production, and vulnerability response. |
| R04 | OWASP ASVS 5.0.0 | [OWASP ASVS](https://owasp.org/www-project-application-security-verification-standard/) | Vendor-neutral application-security verification standard | OWASP flagship project with versioned, machine-readable requirements | Stable 5.0.0 released 2025-05-30 | P0 | Converts security expectations into versioned verification requirements suitable for CLI/MCP boundary review where applicable. |
| R05 | OpenSSF SLSA v1.2 | [SLSA v1.2 specification](https://slsa.dev/spec/v1.2/) | Vendor-neutral software-supply-chain specification | OpenSSF/Linux Foundation project and approved industry-consensus specification | Approved v1.2 was current on 2026-07-15 | P0 | Defines source/build tracks, provenance, artifact verification, and increasing integrity guarantees for release hardening. |
| R06 | FASB ASC 820 and IFRS 13 authority family | [FASB ASU 2011-04](https://storage.fasb.org/ASU2011-04.pdf) | Accounting standard-setter material | FASB and IFRS Foundation primary material | IFRS page marked `Standard 2026 Issued`; accessed 2026-07-15 | P0 | Supports hierarchy rules while preserving the distinction between authoritative standards, amendment text, and non-authoritative implementation material. |
| R07 | Federal Reserve/OCC/FDIC SR 26-2 model-risk guidance | [Federal Reserve SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm) | Joint U.S. banking supervisory guidance | Federal Reserve primary publication with OCC and FDIC attachment | Issued and last updated 2026-04-17 | P0 | Current risk-based guidance covers materiality, effective challenge, validation, monitoring, inventory, documentation, governance, and vendor models. |
| R08 | Basel Committee stress-testing principles | [BCBS stress-testing principles](https://www.bis.org/bcbs/publ/d450.htm) | International supervisory guidelines | Basel Committee publication on the BIS primary site | Page status is `Current`; accessed 2026-07-15 | P1 | Provides a durable governance framework for multi-risk portfolio scenarios, methodology, resources, documentation, use, and oversight. |

## Decision Notes by Candidate

### R01 — SEC fair access

- **Claim/evidence:** The SEC's [Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
  requires automated clients to declare a user agent and asks users to keep aggregate traffic at
  no more than ten requests per second. The limit applies to request traffic as a whole rather
  than offering a per-machine loophole.
- **Implementation inference:** configure one SEC-wide token bucket shared across all tasks and
  processes where practical; identify the application and contact; prefer SEC bulk archives;
  cache immutable filings; persist cursors; and honor blocking or throttling signals.
- **Limits:** the SEC can change its access policy, and a nominal request rate does not guarantee
  availability or timely acceptance. The adapter needs bounded concurrency, exponential backoff,
  circuit breaking, and source-health reporting.
### R02 — BLS quotas and registration

- **Claim/evidence:** The [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm)
  distinguishes unregistered v1 from registered v2. It documents 25 versus 500 daily queries,
  25 versus 50 series per query, 10 versus 20 years per query, and 50 queries per ten seconds for
  both versions.
- **Implementation inference:** operate without registration by default within v1 limits; accept
  an explicitly user-supplied v2 registration key as an optional capability; split date ranges
  within documented bounds; persist results and only refresh mutable observations.
- **Limits:** registration includes a human CAPTCHA and annual renewal. Limits and registration
  terms can change, so configuration must not hard code entitlement assumptions without coverage
  metadata.
### R03 — NIST SSDF

- **Claim/evidence:** [NIST SP 800-218](https://csrc.nist.gov/pubs/sp/800/218/final)
  defines SSDF v1.1 as high-level practices that can be integrated into an existing software
  development lifecycle to reduce vulnerabilities, mitigate undetected weaknesses, and address
  root causes. NIST supplies a DOI, final-publication history, and supplemental mappings.
- **Implementation inference:** map Market Squawk release gates to SSDF outcomes: protected source
  and build environments, reviewed changes, third-party component verification, security tests,
  vulnerability intake, prioritization, remediation, and lessons learned.
- **Limits:** SSDF is outcome-oriented rather than a Rust command list. It does not choose exact
  tools or prove compliance merely because `cargo audit`, license checks, fuzzing, or signing ran.
  The repository needs a documented control-to-evidence mapping.

### R04 — OWASP ASVS

- **Claim/evidence:** [OWASP ASVS](https://owasp.org/www-project-application-security-verification-standard/)
  provides testable technical-control requirements and recommends version-qualified identifiers.
  Version 5.0.0 is the latest stable release identified by the project and is also available in
  structured formats.
- **Implementation inference:** use only applicable ASVS 5.0.0 requirements to review local MCP
  request validation, authentication/authorization if configured, secret handling, injection
  resistance, error handling, audit records, file/artifact boundaries, and outbound endpoints.
- **Limits:** ASVS targets web applications. It does not cover exchange-book integrity, financial
  precision, pre-trade risk, or the complete threat model of a local Rust process. Requirement
  applicability and compensating controls must be recorded rather than claiming blanket ASVS
  conformance.

### R05 — SLSA supply-chain integrity

- **Claim/evidence:** The approved [SLSA v1.2 specification](https://slsa.dev/spec/v1.2/)
  defines source and build tracks, increasing assurance levels, artifact/source verification,
  threats and mitigations, and recommended provenance and verification-summary formats.
- **Implementation inference:** begin with reproducible, isolated release workflows; preserve
  source revision and dependency lock evidence; generate provenance; sign releases where a free
  local workflow is available; verify artifacts before distribution; and document the achieved
  track and level instead of using a vague “SLSA compliant” claim.
- **Limits:** SLSA focuses on source/build integrity. It does not replace dependency vulnerability
  review, credential scanning, license analysis, runtime hardening, or financial-domain tests.
  Some higher-level assurance may depend on CI infrastructure choices and is not mandatory for a
  fully local build.

### R06 — Fair-value authority

- **Claim/evidence:** FASB's [ASU 2011-04](https://storage.fasb.org/ASU2011-04.pdf)
  contains the Topic 820 amendment text and states that the Accounting Standards Codification,
  not the ASU itself, is authoritative. IFRS Foundation's
  [IFRS 13 page](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/)
  identifies the measurement-date exit-price objective and current issued standard. The
  Foundation separately labels its
  [supporting material](https://www.ifrs.org/supporting-implementation/supporting-materials-by-ifrs-standards/ifrs-13/)
  as non-authoritative implementation help.
- **Implementation inference:** version the classification ruleset and store the source evidence,
  measurement date, active/accessibility assessment, adjustments, reason codes, overrides, and
  approvals. Require identical-instrument, quoted-price, active-market, accessible-market, and
  measurement-date evidence before Level 1 classification.
- **Limits:** software cannot replace accounting judgment or professional review. Accessible ASU
  text is not a substitute for verifying current ASC 820 Codification content, and supporting
  IFRS examples are not additional requirements. Jurisdiction, amendments, and entity-specific
  elections need external confirmation.

### R07 — Current model-risk governance

- **Claim/evidence:** [SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm)
  supersedes SR 11-7 and SR 21-8. Its
  [joint attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf)
  adopts a risk-based approach covering model purpose, inherent risk, exposure/materiality,
  individual and aggregate risk, effective challenge, development, validation, outcome analysis,
  ongoing monitoring, governance, inventory, documentation, and third-party models.
- **Implementation inference:** model bundles and registry entries should preserve purpose,
  intended use, limitations, data and code lineage, materiality, validation state, performance
  thresholds, monitoring, approvals, exceptions, and fallback behavior. Vendor ONNX artifacts
  require the same fit-for-purpose and ongoing-monitoring treatment as native models.
- **Limits:** the guidance is most relevant to Federal Reserve-regulated banking organizations
  above $30 billion in assets, is non-prescriptive, and expressly excludes generative/agentic AI
  from its model definition. Applying its principles to Market Squawk is a conservative design
  inference, not a legal applicability or compliance assertion.

### R08 — Portfolio stress testing

- **Claim/evidence:** The Basel Committee's current
  [Stress testing principles](https://www.bis.org/bcbs/publ/d450.htm) identify objectives,
  governance, policies, processes, methodology, resources, documentation, implementation, and
  oversight as core elements of a stress-testing framework. They supersede the 2009 principles.
- **Implementation inference:** portfolio scenarios should be versioned and reproducible, cover
  multiple risk types and concentrations, include forward-looking severe-but-plausible cases,
  expose assumptions and data gaps, and feed limits or decisions rather than exist only as a
  report. Results should be independently reviewed proportionate to use.
- **Limits:** these are high-level banking/supervisory guidelines, not a prescribed VaR or
  expected-shortfall algorithm and not a personal-portfolio compliance standard. Market Squawk
  still needs explicit scenario libraries, factor mappings, liquidity assumptions, backtesting,
  and validation thresholds.

## Excluded Sources

| Source | URL | Reason Excluded |
| --- | --- | --- |
| Federal Reserve SR 11-7 | https://www.federalreserve.gov/supervisionreg/srletters/sr1107.htm | Superseded by SR 26-2 on 2026-04-17; retaining it as current guidance would be materially stale. |
| 2009 Basel stress-testing principles | https://www.bis.org/publ/bcbs155.htm | Marked superseded by the selected current 2018 principles. |
| OpenSSF S2C2F announcement blog | https://openssf.org/blog/2022/11/16/openssf-expands-supply-chain-integrity-efforts-with-s2c2f/ | Useful context, but the approved SLSA v1.2 specification provides a clearer versioned release-integrity baseline for this bounded inventory. |
| OWASP Top Ten | https://owasp.org/www-project-top-ten/ | Awareness list rather than the testable, version-addressable control catalog needed here; ASVS was selected. |
| Accounting-firm ASC 820/IFRS 13 summaries | — | Secondary interpretations were unnecessary because standard-setter sources were available; such summaries also cannot establish authority. |
| Generic cybersecurity listicles and product marketing | — | Lack primary evidence, versioned control requirements, or vendor-neutral credibility. |

## Coverage Gaps

- **Current ASC text and legal interpretation:** the accessible FASB ASU explicitly says the
  Codification is authoritative. A release claiming U.S. GAAP compliance must verify the current
  ASC 820 text and amendments through authorized access and obtain qualified accounting review.
- **Control mapping:** SSDF, ASVS, and SLSA overlap but do not automatically map to Market
  Squawk's Rust crates, local MCP surface, provider credentials, hot path, or artifact directory.
  A threat model and control-to-test matrix remain necessary.
- **Local secret fallback:** the selected sources do not specify a complete cross-platform OS
  keyring plus encrypted-file fallback design. Cryptographic format, key derivation, rotation,
  recovery, permissions, and redaction need separate primary-source review.
- **Provider-policy drift:** SEC and BLS can change limits or enforcement behavior. Source
  metadata must record policy URL/access date, and adapters must treat `429`, `403`, timeouts, and
  HTML challenge pages as health events rather than invitations to disguise traffic.
- **Providers without numerical quotas:** this set does not establish a numeric ceiling for every
  required provider. Where a provider documents only `429` or general fair use, use conservative
  configurable limits and bounded backoff rather than inferring an entitlement.
- **Model-guidance applicability:** SR 26-2 is very new and banking-focused. Additional agency
  interpretations may emerge after 2026-07-15; its generative/agentic-AI exclusion and asset-size
  scope must be preserved in any downstream synthesis.
- **Portfolio-risk implementation:** Basel principles are governance-level. Separate empirical
  research is needed to choose and validate VaR, expected shortfall, stress scenarios, liquidity
  shocks, factor models, and backtesting methods for Market Squawk's intended users.
- **No certification claim:** selecting these sources does not certify SSDF, ASVS, SLSA,
  accounting, banking, or Basel compliance. Any such claim requires scoped evidence and, where
  applicable, professional or independent review.

## Source List

All links were accessed on **2026-07-15**.

1. SEC, [Webmaster Frequently Asked Questions](https://www.sec.gov/about/webmaster-frequently-asked-questions).
2. SEC, [EDGAR application programming interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).
3. BLS, [Public Data API Frequently Asked Questions](https://www.bls.gov/developers/api_faqs.htm).
4. BLS, [Public Data API landing page](https://www.bls.gov/developers/home.htm).
5. NIST, [SP 800-218: Secure Software Development Framework v1.1](https://csrc.nist.gov/pubs/sp/800/218/final).
6. NIST, [SSDF project](https://csrc.nist.gov/projects/ssdf).
7. OWASP, [Application Security Verification Standard](https://owasp.org/www-project-application-security-verification-standard/).
8. OpenSSF/SLSA, [SLSA v1.2 specification](https://slsa.dev/spec/v1.2/).
9. OpenSSF, [SLSA project page](https://openssf.org/projects/slsa/).
10. FASB, [Accounting Standards Update 2011-04](https://storage.fasb.org/ASU2011-04.pdf).
11. FASB, [Accounting Standards Updates index](https://fasb.org/standards/accounting-standard-updates).
12. IFRS Foundation, [IFRS 13 Fair Value Measurement](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/).
13. IFRS Foundation, [IFRS 13 supporting implementation material](https://www.ifrs.org/supporting-implementation/supporting-materials-by-ifrs-standards/ifrs-13/).
14. Federal Reserve, [SR 26-2 revised model-risk guidance](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm).
15. Federal Reserve/OCC/FDIC, [SR 26-2 joint attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf).
16. Basel Committee, [Stress testing principles](https://www.bis.org/bcbs/publ/d450.htm).
