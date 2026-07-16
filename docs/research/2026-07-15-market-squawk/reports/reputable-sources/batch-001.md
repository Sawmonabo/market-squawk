# Reputable Sources Batch 001 Deep Dive

**Topic:** Market Squawk complete local platform architecture, source adapters, analytics,
risk, valuation, and MCP implementation evidence
**As-of/access date:** 2026-07-15

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews only the assigned FASB/IFRS fair-value authority, SEC and BLS access
policies, NIST SSDF 1.1, OWASP ASVS 5.0.0, and SLSA 1.2 families. It translates their
requirements into Market Squawk repository gates. **Inference:** Accounting classification,
provider-access compliance, application verification, secure development, and artifact provenance
must remain separate controls; passing one cannot imply another.

## Sources Reviewed

| Source | Authority and freshness | Scope limitation |
|---|---|---|
| [FASB ASU 2011-04](https://storage.fasb.org/ASU2011-04.pdf) and [IFRS 13](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/) | **Confirmed:** Primary accounting standard-setter material; the 2011 work converged the definition and broad measurement/disclosure requirements. | **Confirmed limitation:** The ASU is an amendment document, not the current complete Codification. IFRS 13 governs how to measure when another IFRS requires or permits fair value; it does not decide when fair value is required. |
| [SEC Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | **Confirmed:** SEC's first-party automated-access guidance; last reviewed 2024-08-23. | **Confirmed limitation:** Policy may change and SEC does not provide scripted-download technical support. |
| [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm) and [terms](https://www.bls.gov/developers/termsOfService.htm) | **Confirmed:** BLS's first-party limits, registration requirements, and use terms. | **Inference:** Published observations are not automatically point-in-time vintages; retrieved analyses are the user's responsibility. |
| [NIST SP 800-218 v1.1](https://csrc.nist.gov/pubs/sp/800/218/final) | **Confirmed:** Final February 2022 SSDF recommendations from NIST. | **Confirmed limitation:** High-level, risk-based practices; not a product certification or prescribed toolchain. |
| [OWASP ASVS 5.0.0](https://owasp.org/www-project-application-security-verification-standard/) | **Confirmed:** Stable standard released 2025-05-30 for verifying web applications and services. | **Confirmed limitation:** It is a requirements catalog, not a secure-SDLC or supply-chain standard. **Inference:** Many browser/session controls are inapplicable to local stdio MCP. |
| [SLSA 1.2](https://slsa.dev/spec/v1.2/) | **Confirmed:** Approved industry-consensus specification, released 2025-11-24, with Build and Source tracks. | **Confirmed limitation:** Provenance and source/build integrity do not establish application correctness, vulnerability absence, license compliance, or runtime safety. |

## Findings

### Fair-value classification boundaries

**Confirmed:** ASC 820 defines fair value as a measurement-date exit price in an orderly
transaction in the principal market, or absent one, the most advantageous market. Transaction
costs do not adjust that price. The hierarchy gives highest priority to unadjusted quoted prices in
active markets for identical items and classifies a measurement at the lowest-level input significant
to the measurement in its entirety. [ASU 2011-04, ASC 820-10-35-9A, 35-37A, 35-40](https://storage.fasb.org/ASU2011-04.pdf)

**Confirmed:** Level 1 requires an unadjusted quoted price in an active market for an identical
asset or liability that the entity can access at the measurement date. Adjusting that quote for new
information lowers the classification. Level 2 comprises observable direct or indirect inputs other
than Level 1; Level 3 uses unobservable inputs and retains the market-participant exit-price
objective. [ASC 820-10-35-40 through 35-54A](https://storage.fasb.org/ASU2011-04.pdf)
The IFRS Interpretations Committee likewise states that a third-party price can be Level 1 only when
it relies solely on an unadjusted quoted price in an active market for an identical instrument the
entity can access at the measurement date. [IFRS 13 third-party-price decision](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html)

**Inference:** `FairValueHierarchy`, `MarketDepth`, and `DataQuality` must be different Rust types.
A model, proxy, indicative quote, similar instrument, inactive-market price, or adjusted quote cannot
be promoted to Level 1. Conversely, Level 1 evidence is not automatically `DirectVerified`: execution
still requires authorized direct delivery, known source/venue/instrument, sequence and checksum
integrity, valid timestamps, freshness, precision, trading status, and coverage. Persist valuation
method, every input and input level, principal-market/access/active-market evidence, measurement
date, adjustment, lowest-significant-input rationale, ruleset version, override, and approval.

### Compliant public-source access

**Confirmed:** SEC permits scripted access, requires a declared user agent containing organization
and contact information, and currently caps aggregate access at 10 requests/second. The cap applies
regardless of the number of machines; excessive access can be temporarily limited, with access
resuming after the rate remains below threshold for ten minutes. The SEC recommends bulk ZIP files
for large API data. [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[SEC security policy and aggregate limit](https://www.sec.gov/about/privacy-information),
[EDGAR APIs and bulk data](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)

**Confirmed:** SEC acceptance time is not an exact public-availability timestamp; filings are often
available within one to three minutes, but the delay is not guaranteed and no timestamp identifies
first availability on sec.gov. [SEC FAQ, EDGAR timestamps](https://www.sec.gov/about/webmaster-frequently-asked-questions)
**Inference:** Retain acceptance/published time separately from the locally observed `available_at`,
`received_at`, and `ingested_at`; never backfill an invented availability instant.

**Confirmed:** BLS API v1 is unregistered and permits 25 daily queries, 25 series/query, and 10
years/query. Registered v2 permits 500 daily queries, 50 series/query, and 20 years/query; both are
limited to 50 requests per 10 seconds. V2 registration requires an email, organization, CAPTCHA,
key, and annual renewal. BLS may block attempts to exceed or circumvent limits and requires users to
cite retrieval date and disclaim BLS responsibility for post-retrieval analyses.
[BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm),
[BLS terms](https://www.bls.gov/developers/termsOfService.htm)

**Inference:** Each adapter needs one process-wide budget shared across workers, bounded concurrency,
batching, cache/ETag or local-manifest reuse, exponential backoff, and explicit degraded/unavailable
health. On 429 or blocking, stop and wait. Do not rotate accounts or identities, spoof browser/TLS
fingerprints, bypass CAPTCHA, rotate proxies to defeat blocking, or distribute requests to evade an
aggregate quota. V2 registration is a user-completed, authorized action; v1 remains the no-key
fallback within its smaller limits.

### Secure development and release gates

**Confirmed:** SSDF 1.1 groups practices into Prepare the Organization, Protect the Software,
Produce Well-Secured Software, and Respond to Vulnerabilities. It calls for maintained security
requirements and roles, secure toolchains/environments, defined verification criteria, protected
code and release integrity, secure design/reuse/build/test/configuration, vulnerability response,
remediation, and root-cause prevention. [NIST SP 800-218](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-218.pdf)

**Inference:** Repository gates should include committed threat/security requirements; CODEOWNERS
and reviewed exceptions; protected release branches/tags; locked dependencies; `fmt`, Clippy, tests,
release build, parser fuzz smoke tests, dependency/advisory/license/source policy, secret scanning,
and generated-artifact checks; a vulnerability-reporting policy; and incident/root-cause records.
Release inputs, compiler/tool versions, lockfile, checks, hashes, and retained artifacts must be
reconstructable. Fail closed on any required gate.

**Confirmed:** ASVS 5.0.0 is the stable production baseline; its project recommends version-qualified
requirement identifiers because identifiers can change. It provides testable application-security
requirements and machine-readable release artifacts. [ASVS project and versioning guidance](https://github.com/OWASP/ASVS/tree/v5.0.0_release)
**Inference:** Pin `v5.0.0-<requirement>` IDs in a repository checklist. Target Level 2 for the local
control plane and selected Level 3 controls for credential storage, execution/risk authorization,
artifact paths, and MCP boundaries. Record `applicable`, `not_applicable`, evidence, test, reviewer,
and rationale. Verify schema/bounds validation, least privilege, authorization at every service
boundary, secret redaction, safe cryptography, secure defaults, log integrity, path containment, and
denial of arbitrary shell/filesystem/SQL/credential operations. ASVS does not replace domain tests
for trading risk or market-data integrity.

**Confirmed:** SLSA 1.2 Build L1 requires automatically generated build provenance; Build L2 adds
hosted-platform-generated authenticated provenance; Build L3 adds hardened isolation and prevents
user build steps from accessing provenance-signing secrets. Its new Source track progresses from
version control (L1), preserved history/source provenance (L2), enforced organizational controls
(L3), to two-party review (L4). [Build levels](https://slsa.dev/spec/v1.2/build-track-basics),
[Source requirements](https://slsa.dev/spec/v1.2/source-requirements)

**Inference:** The zero-cloud-required local build should first produce Build L1-style provenance,
checksums, an SBOM, and optional user-controlled signatures from a clean locked source revision.
An optional free hosted release path may target Build L3 and Source L4, but it cannot become a core
runtime/build prerequisite. Release verification must bind artifact digest to source revision,
builder identity, build definition, declared inputs, and verified provenance. SBOM, vulnerability,
license, credential, and reproducibility checks remain separate because SLSA does not supply them.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| Hierarchy classifies valuation inputs, not feed quality. | [FASB](https://storage.fasb.org/ASU2011-04.pdf), [IFRS](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/) | Level 1/2/3 depend on input observability and the lowest significant input. | High — **Confirmed** | Execution eligibility is a separate Market Squawk policy (**Inference**). |
| Adjusted or third-party prices do not silently become Level 1. | [IFRS decision](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html) | Level 1 requires sole reliance on an accessible, active-market, identical-item, unadjusted quote. | High — **Confirmed** | **Inference:** Store evidence and adjustment lineage. |
| SEC limit is aggregate, not per worker/machine. | [SEC policy](https://www.sec.gov/about/privacy-information) | No more than 10 requests/second regardless of machines. | High — **Confirmed** | **Inference:** Central limiter; no distributed evasion. |
| BLS offers lawful free tiers with explicit limits. | [BLS FAQ](https://www.bls.gov/developers/api_faqs.htm) | v1/v2 daily, series, years, and rate limits; v2 registration. | High — **Confirmed** | **Inference:** User completes CAPTCHA; cache and batch. |
| SSDF provides lifecycle practices, not certification. | [NIST](https://csrc.nist.gov/pubs/sp/800/218/final) | Four practice groups integrated into an SDLC. | High — **Confirmed** | **Inference:** Map to evidence-producing repo gates. |
| ASVS and SLSA cover different assurance planes. | [ASVS](https://owasp.org/www-project-application-security-verification-standard/), [SLSA](https://slsa.dev/spec/v1.2/) | Application verification versus source/build provenance and tamper resistance. | High — **Confirmed** | **Inference:** Use both; neither proves financial correctness. |

## Source-Specific Notes

- **Inference:** FASB/IFRS rules should be encoded as versioned, explainable decision rules with
  human override/approval—not a model that outputs an unexplained hierarchy label.
- **Inference:** SEC/BLS adapters must persist source-policy version, budget state, response status,
  retry decision, cache hit, and coverage gap without storing keys in logs.
- **Inference:** NIST supplies the lifecycle; ASVS supplies selected testable application controls;
  SLSA supplies source/build attestations. Their evidence should converge in a local release manifest.

## Cross-Source Patterns

1. **Inference:** Trust is contextual: observable valuation evidence, executable market data,
   compliant acquisition, secure application behavior, and trustworthy artifacts require separate
   typed claims with separate evidence.
2. **Inference:** Every exception—fair-value override, access-policy override, security-control N/A,
   failed release gate—needs an owner, reason, timestamp, approval, and expiration. No exception may
   bypass pre-trade risk.
3. **Inference:** Local-first does not mean unverifiable: manifests, hashes, attestations, audit logs,
   test results, and source snapshots can all remain local and use free tooling.

## Limitations and Non-Findings

- **Confirmed limitation:** This is engineering research, not accounting, audit, or legal advice.
  Current reporting conclusions require the applicable complete standards and professional judgment.
- **Confirmed non-finding:** FASB/IFRS hierarchy rules do not define exchange-feed integrity or
  automated execution eligibility; NIST/ASVS/SLSA do not define fair-value hierarchy.
- **Confirmed non-finding:** SEC and BLS policies provide no permission for quota circumvention,
  CAPTCHA bypass, identity rotation, concealment, or distributed evasion; BLS terms expressly allow
  blocking suspected circumvention.
- **Confirmed limitation:** ASVS applicability depends on architecture; stdio MCP has no browser
  session, while a future remote transport would require a new threat model and control selection.
- **Confirmed limitation:** SLSA levels are claims about a specific source revision/artifact and its
  producing systems, not a blanket property of a repository. NIST SP 800-218 v1.1 is the assigned
  final publication; later drafts or profiles require separate adoption decisions.

## Source List

1. FASB, *Accounting Standards Update 2011-04*, accessed 2026-07-15: https://storage.fasb.org/ASU2011-04.pdf
2. IFRS Foundation, *IFRS 13 Fair Value Measurement*, accessed 2026-07-15: https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/
3. SEC, *Webmaster Frequently Asked Questions*, accessed 2026-07-15: https://www.sec.gov/about/webmaster-frequently-asked-questions
4. BLS, *Public Data API FAQs*, accessed 2026-07-15: https://www.bls.gov/developers/api_faqs.htm
5. NIST, *SP 800-218 SSDF v1.1*, accessed 2026-07-15: https://csrc.nist.gov/pubs/sp/800/218/final
6. OWASP, *ASVS 5.0.0*, accessed 2026-07-15: https://owasp.org/www-project-application-security-verification-standard/
7. OpenSSF SLSA, *Specification v1.2*, accessed 2026-07-15: https://slsa.dev/spec/v1.2/
