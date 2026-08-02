# Reputable Sources Synthesis

**Topic:** Market Squawk complete local platform architecture, source adapters, analytics,
risk, valuation, and MCP implementation evidence
**As-of date:** 2026-07-15
**Input lineage:** [Reputable-source batch 001](../reputable-sources/batch-001.md) and
[batch 002](../reputable-sources/batch-002.md)

## Table of Contents

- [Category Scope](#category-scope)
- [Sources Covered](#sources-covered)
- [High-Confidence Findings](#high-confidence-findings)
- [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
- [Conflicts and Disagreements](#conflicts-and-disagreements)
- [Trends and Patterns](#trends-and-patterns)
- [Implications for the Research Topic](#implications-for-the-research-topic)
- [Gaps](#gaps)
- [Source Matrix](#source-matrix)

## Category Scope

This synthesis combines authoritative accounting, public-source access, secure-development,
application-verification, software-supply-chain, model-risk, and stress-testing sources. It does not
add sources or treat supervisory guidance as directly binding on a local software project.

The central synthesis is **high-confidence inference**: Market Squawk needs distinct, typed claims
for (1) fair-value input hierarchy, (2) market-data and execution quality, (3) lawful source access,
(4) model validation, (5) scenario governance, (6) application security, and (7) source/build
integrity. Evidence in one plane cannot promote a record or artifact in another.

## Sources Covered

- **Accounting authority:** FASB ASU 2011-04/ASC 820 material and IFRS 13, including IFRS's
  third-party-price interpretation. [FASB](https://storage.fasb.org/ASU2011-04.pdf),
  [IFRS 13](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/),
  [IFRS interpretation](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html)
- **Access-policy authority:** SEC automated-access/security guidance and BLS API limits and terms.
  [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
  [SEC aggregate limit](https://www.sec.gov/about/privacy-information),
  [BLS FAQ](https://www.bls.gov/developers/api_faqs.htm),
  [BLS terms](https://www.bls.gov/developers/termsOfService.htm)
- **Security and supply chain:** NIST SSDF 1.1, OWASP ASVS 5.0.0, and SLSA 1.2.
  [NIST](https://csrc.nist.gov/pubs/sp/800/218/final),
  [ASVS](https://owasp.org/www-project-application-security-verification-standard/),
  [SLSA](https://slsa.dev/spec/v1.2/)
- **Model and stress governance:** April 2026 Federal Reserve/OCC/FDIC SR 26-2 and the current
  Basel Committee 2018 stress-testing principles.
  [SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm),
  [joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf),
  [Basel publication](https://www.bis.org/bcbs/publ/d450.htm),
  [Basel guideline](https://www.bis.org/bcbs/publ/d450.pdf)

## High-Confidence Findings

### 1. Fair-value hierarchy and execution quality are orthogonal

**Confirmed:** ASC 820/IFRS 13 define a measurement-date, market-participant exit-price framework.
Level 1 depends on an unadjusted quoted price in an active market for an identical item that the
entity can access at the measurement date. Level 2 uses other observable direct or indirect inputs;
Level 3 uses unobservable inputs. A measurement is classified using the lowest-level input
significant to the measurement in its entirety. An adjusted quote or a third-party price not relying
solely on qualifying Level 1 inputs is lower in the hierarchy.
[FASB ASC 820-10-35-37A and 35-40 through 35-54A](https://storage.fasb.org/ASU2011-04.pdf),
[IFRS third-party-price decision](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html)

**Inference:** `FairValueHierarchy`, `MarketDepth`, and `DataQuality` must be separate domain types
and columns. A `DirectVerified` feed does not prove Level 1 without principal-market, accessibility,
active-market, identical-instrument, measurement-date, and no-adjustment evidence. A Level 1
classification does not prove executable integrity without direct authorized delivery, identity and
coverage resolution, sequence/checksum integrity, valid timestamps, freshness, precision, and
trading status. Level 2/3 inputs and model outputs never become executable merely by producing a
price.

### 2. Compliant ingestion requires aggregate budgets and explicit unavailability

**Confirmed:** SEC currently limits access to 10 requests/second in aggregate regardless of the
number of machines, requires a declared organization/contact user agent, may limit excessive
requests, and recommends bulk data for large retrievals. BLS v1 and registered v2 have different
daily, series, and year limits, while both allow 50 requests per 10 seconds. V2 requires user
registration, CAPTCHA, a key, and annual renewal.
[SEC policy](https://www.sec.gov/about/privacy-information),
[SEC bulk guidance](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
[BLS FAQ](https://www.bls.gov/developers/api_faqs.htm),
[BLS terms](https://www.bls.gov/developers/termsOfService.htm)

**Inference:** Each extraction provider needs a single supervised local budget across its workers
and processes, bounded concurrency, request coalescing/batching, cache and manifests, backoff, and
health states such as healthy, degraded, rate-limited, and unavailable. On rejection or blocking,
stop and wait. Separate hosts running under the same identity remain a documented coordination
responsibility because a purely local installation cannot enforce a provider's cross-machine total.

**Confirmed:** SEC acceptance time is not the exact public-availability time; typical lag is not
guaranteed and no SEC timestamp identifies first availability.
[SEC EDGAR timestamp FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
**Inference:** Preserve source acceptance/publication, locally observed availability, receipt, and
ingestion as different fields. Do not fabricate retrospective `available_at` values.

### 3. Secure development, application verification, and release provenance are complementary

**Confirmed:** NIST SSDF 1.1 covers organizational preparation, protection of software, production of
well-secured software, and vulnerability response. ASVS 5.0.0 provides versioned, testable
application-security requirements. SLSA 1.2 separately describes source and build assurance:
Build L1 provenance, Build L2 authenticated provenance from a hosted platform, Build L3 hardened
isolation; and Source L1 version control through Source L4 two-party review.
[NIST SSDF](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-218.pdf),
[ASVS stable version](https://github.com/OWASP/ASVS/tree/v5.0.0_release),
[SLSA build levels](https://slsa.dev/spec/v1.2/build-track-basics),
[SLSA source levels](https://slsa.dev/spec/v1.2/source-requirements)

**Inference:** Market Squawk should maintain one evidence-producing release gate that runs the
required locked Rust formatting, lint, test, and release build; dependency/advisory/license/source
policy; secret and generated-artifact checks; parser-fuzz smoke tests; and model/schema validations.
It should emit a release manifest binding source revision, Rust/tool versions, lockfile, inputs,
checks, artifact hashes, SBOM, and provenance. A local build can be fully functional and produce
Build L1-style provenance without cloud. A free hosted Build L3/Source L4 release channel is an
optional assurance path, never a core build or runtime dependency.

**Inference:** Pin ASVS requirement IDs to `v5.0.0`, select applicability explicitly, target Level 2
for the local control plane, and apply selected Level 3 controls at credential, execution/risk,
artifact, and MCP boundaries. Stdio MCP makes browser/session controls inapplicable, but typed input
bounds, least privilege, authorization, secret redaction, secure defaults, logging, path containment,
and denial of arbitrary shell/filesystem/SQL/credential access remain relevant. A future remote MCP
transport requires a new threat model and ASVS selection.

### 4. Model and stress use must be purpose-bound and fail closed

**Confirmed:** SR 26-2 superseded SR 11-7 and SR 21-8 on 2026-04-17. It is non-prescriptive
supervisory guidance most relevant to Federal Reserve-regulated banking organizations above
$30 billion, not a rule for Market Squawk. It scales rigor with inherent risk and materiality and
emphasizes intended use, input/data constraints, effective challenge, validation normally before
use, outcome analysis, monitoring, thresholds, dependencies, inventories, changes, and constrained
urgent use. Vendor models remain within its governance approach.
[SR 26-2 status](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm),
[attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf)

**Inference:** A model bundle needs artifact/data hashes, model and feature versions, purpose,
permitted universe and decisions, schemas and quality requirements, training/calibration period,
assumptions/dependencies, metrics/thresholds, validation state/evidence/reviewer, limitations,
fallback, and change history. `Unvalidated`, out-of-scope, changed, stale, threshold-breaching, or
failed inference states produce no automated action. Deterministic rules and generative/agentic AI,
which SR 26-2 excludes from its model definition, still need separate Market Squawk inventory and
control categories.

**Confirmed:** Basel's current 2018 Guidelines replaced its 2009 principles. They call for documented
objectives/governance, material-risk coverage, severe and plausible scenarios, accurate granular
data and aggregation, fit-for-purpose methods, challenge/review, communication, and use of results.
They support historical, hypothetical, emerging-risk, and reverse stresses; overlays and expert
judgment should be documented and challenged.
[Basel status](https://www.bis.org/bcbs/publ/d450.htm),
[principles](https://www.bis.org/bcbs/publ/d450.pdf)

**Inference:** Scenario artifacts need an ID/version/hash, objective, as-of time, horizon, narrative,
shocks, severity rationale, scope/exclusions, dependencies/correlations, aggregation policy,
data/model versions, overlays, limitations, approval, and outputs. Tests should reconcile
instrument-to-portfolio totals, units and currencies; verify shock propagation and correlated-risk
aggregation; exercise reverse-stress thresholds; and invalidate approval after material input,
overlay, or model changes. Risk limits remain authoritative over every model/stress result.

## Medium- and Low-Confidence Findings

- **Medium-confidence inference:** ASVS Level 2 plus selected Level 3 is a proportionate target, but
  the final control set depends on the implemented CLI/MCP/secret architecture; ASVS does not assign
  Market Squawk's target level.
- **Medium-confidence inference:** SLSA Build L1-style local provenance is achievable without a
  hosted service; any formal SLSA level claim must be assessed against the exact producing platform
  and artifact. Hosted L3 and two-party Source L4 may be impractical for a solo/local release.
- **Medium-confidence inference:** SR 26-2 and Basel provide strong model/scenario design patterns,
  but their banking scope cannot support a claim that Market Squawk is regulator-approved or
  compliant.
- **Low-confidence/open design:** Neither model-risk nor stress guidance supplies universal metric
  thresholds, shock magnitudes, probabilities, capital/loss limits, or validation cadence. Those
  values require strategy, instrument, horizon, exposure, and user-risk-policy decisions.

## Conflicts and Disagreements

There is no direct conflict among the sources; apparent conflicts come from scope or supersession:

- **Current-source precedence:** SR 26-2 replaces SR 11-7 and SR 21-8; Basel 2018 replaces its 2009
  principles; ASVS 5.0.0 and SLSA 1.2 are the pinned stable/current baselines in these reports.
  Historic versions should not be presented as current. [SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm),
  [Basel](https://www.bis.org/bcbs/publ/d450.htm), [ASVS](https://owasp.org/www-project-application-security-verification-standard/),
  [SLSA](https://slsa.dev/spec/v1.2/)
- **Classification versus qualification:** Accounting hierarchy categorizes valuation inputs;
  Market Squawk data quality qualifies feeds for use. These must not be collapsed.
- **Provider-policy variance:** SEC's cross-machine aggregate limit and BLS's tiered quotas require
  provider-specific policy objects, not one hard-coded rate.
- **Self-hosted versus hosted assurance:** SLSA Build L2/L3 relies on hosted build infrastructure,
  while Market Squawk cannot require cloud. Resolve this with a complete local build and an optional
  hosted assurance channel, without claiming L2/L3 for local artifacts.
- **Guidance applicability:** Supervisory model/stress principles inform engineering but do not
  create direct Market Squawk obligations. The FASB ASU is amendment material rather than the
  complete current Codification; accounting conclusions still require the applicable full standards
  and professional judgment.

## Trends and Patterns

Across the category, trust comes from explicit scope, versioned evidence, reproducibility of the
decision process, independent challenge, and controlled exceptions. Repeated patterns are:

1. State the purpose and permitted use before measurement, model inference, stress, or release.
2. Preserve source-to-output lineage and distinguish observed facts from modeled assumptions.
3. Scale rigor by materiality, uncertainty, exposure, and consequence.
4. Validate before consequential use, monitor after use, and revalidate after material change.
5. Treat aggregate dependencies and correlated failure—not only individual components—as risks.
6. Fail closed when evidence, freshness, validation, authorization, provenance, or a required gate is
   missing.
7. Record every exception with owner, reason, evidence, approval, timestamp, and expiration; no
   exception can bypass pre-trade risk.

## Implications for the Research Topic

| Market Squawk area | Required local artifact/control | Fail-closed gate |
|---|---|---|
| Domain and valuation | Separate hierarchy/depth/quality types; versioned valuation inputs, method, evidence, rules, overrides, approvals | No Level 1 without every authoritative criterion and lowest-significant-input analysis |
| SEC/BLS adapters | Provider policy/version, declared identity, shared budget, batching/cache, cursor, health, response/audit metadata | Stop on exhausted quota, 429, blocking, or policy uncertainty |
| Live execution | Direct source/venue/instrument coverage plus sequence, checksum, timestamp, freshness, precision, and status validation | Only `DirectVerified`; quarantine and resynchronize on integrity failure |
| Model registry | Purpose-bound immutable bundle, dependency graph, validation status, thresholds, limitations, fallback, change lineage | No automated action for invalid, changed, stale, failed, or out-of-scope bundle |
| Stress analytics | Versioned scenario manifest, coherent shocks/correlations, aggregation and reconciliation evidence, approvals | Reject missing material exposures, inconsistent units/currencies, or stale approval |
| CLI/MCP security | Version-pinned ASVS applicability/evidence; typed schemas, bounds, cancellation, least privilege, path control, audit | No arbitrary shell/files/SQL/credentials and no risk bypass |
| Repository/release | SSDF evidence, locked checks, audits, fuzz smoke, SBOM, hashes, provenance, retained release manifest | Publish nothing when a required check or artifact verification fails |

## Gaps

- No source defines Market Squawk's live sequence/checksum algorithms, staleness budgets, queue
  overflow policy, or execution-risk thresholds; these require provider specifications, domain tests,
  measurement, and explicit user policy.
- No authoritative accounting source in these batches provides a self-executing classifier or
  removes the need for current complete standards, evidence review, judgment, override governance,
  and auditability.
- SEC/BLS policies can change. Source-policy metadata and deterministic policy tests need a review
  cadence; multiple independent hosts cannot be coordinated without explicit user administration.
- BLS current observations and SEC acceptance timestamps do not by themselves establish historical
  point-in-time availability or revision lineage.
- No reviewed security standard proves vulnerability absence, financial correctness, safe order
  intent, model validity, or stress adequacy. Cross-domain tests and human review remain necessary.
- Formal ASVS and SLSA targets have not been assessed against implemented architecture/builders.
  A solo local workflow may lack independent challenge, two-party review, or hardened hosted builds.
- Model-validation thresholds, scenario magnitudes/probabilities, aggregation formulas, and capital/
  loss limits remain product-policy decisions. Performance and correctness claims require measured
  fixtures and passing tests, not standards citations.

## Source Matrix

| Source family | Current status as reviewed | Highest-confidence contribution | Applicability boundary | Input batch |
|---|---|---|---|---|
| [FASB ASC 820 material](https://storage.fasb.org/ASU2011-04.pdf) / [IFRS 13](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/) | Primary authority; ASU is amendment material | Exit-price objective and Level 1/2/3 input hierarchy | Accounting measurement, not feed/execution qualification; full current standards and judgment still required | 001 |
| [SEC access policy](https://www.sec.gov/about/webmaster-frequently-asked-questions) | First-party current operational guidance in batch | Declared identity, aggregate cap, bulk access, timestamp limitation | Policy can change; no technical support or exact public-availability timestamp | 001 |
| [BLS API policy](https://www.bls.gov/developers/api_faqs.htm) | First-party API limits/terms | Registered/unregistered limits | Not a vintage database | 001 |
| [NIST SSDF 1.1](https://csrc.nist.gov/pubs/sp/800/218/final) | Assigned final publication | Secure-development lifecycle and vulnerability-response practices | High-level guidance, not certification or tool mandate | 001 |
| [OWASP ASVS 5.0.0](https://owasp.org/www-project-application-security-verification-standard/) | Stable version in batch | Versioned testable application-security requirements | Architecture-specific applicability; stdio is not a browser app | 001 |
| [SLSA 1.2](https://slsa.dev/spec/v1.2/) | Approved/current specification in batch | Source/build levels, provenance and tamper-resistance requirements | Artifact/platform-specific; does not replace SBOM, audits, or runtime security | 001 |
| [SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm) | Current 2026 guidance; supersedes SR 11-7/SR 21-8 | Purpose/materiality, validation, monitoring, inventory, change and dependency governance | Non-prescriptive banking guidance; not direct Market Squawk regulation; excludes deterministic and generative/agentic processes from its model definition | 002 |
| [Basel stress principles](https://www.bis.org/bcbs/publ/d450.htm) | Current Guidelines; replaces 2009 principles | Scenario governance, material-risk coverage, data aggregation, challenge and use | Primarily large banks/authorities; no universal shocks, thresholds, or formulas | 002 |
