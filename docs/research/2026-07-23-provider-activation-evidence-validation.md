# Provider activation evidence validation

Purpose: preserve the independently verified, date-anchored evidence decision used by the local
provider-onboarding and research-adapter activation boundary.

| Metadata | Value |
| --- | --- |
| Document type | Research validation record |
| Audience | Maintainers, security reviewers, release reviewers |
| Status | Verified; fail-closed release gates remain in force |
| Research date | 2026-07-23 |
| Last substantive review | 2026-07-23 |
| Repository audit anchor | `4bcd6543eb1dc2000af2381161f22ea105b123bc` |

## Table of Contents

- [Scope](#scope)
- [Verified decision](#verified-decision)
- [Evidence identity and lineage](#evidence-identity-and-lineage)
- [Probe-lineage reconciliation](#probe-lineage-reconciliation)
- [Assigned source register](#assigned-source-register)
- [Implementation consequences](#implementation-consequences)
- [Limitations and refresh triggers](#limitations-and-refresh-triggers)
- [Related evidence](#related-evidence)

## Scope

The validation covered the production activation decisions for SEC EDGAR, BLS public v1, BLS
registered v2, U.S. Treasury Fiscal Data, Treasury daily-interest-rate XML, and FRED/ALFRED. It
kept technical access, credential state, provider rights, and Market Squawk activation authority as
four independent axes. A responding endpoint or an issued credential does not by itself establish
durable-use authority.

The independent verifier reported `PASS` with zero Critical, Important, or Minor findings after
checking all 31 assigned sources, all 16 required report sections, all 14 linked input-artifact
digests, and the terminal research-artifact validator.

## Verified decision

| Provider surface | Evidence disposition | Product release state | Required evidence before activation or publication |
| --- | --- | --- | --- |
| SEC EDGAR public APIs | `RightsEligible; CompliantProbeRequired` | `RefreshRequired` | Capture exact current official content and a bounded production-runtime response using the documented application and monitored-contact identity. |
| BLS public API v1 | `RefreshRequired` | `RefreshRequired` | Retain and hash the current provider terms body, preserve BLS duties, then run the bounded public request contract. |
| BLS registered API v2 | `CredentialPending + RefreshRequired` | `RefreshRequired` | Satisfy the v1 rights refresh; the user then completes provider-controlled registration, imports the emailed key locally, and runs a bounded keyed verification. |
| Treasury Fiscal Data API | `EligibleFromAssignedEvidence; EndpointProbeRequired` | Profile available; adapter activation remains probe-gated | Validate the selected dataset, response schema, pagination, and notice through a bounded production-runtime request while retaining exact dataset/version provenance. |
| Treasury daily-interest-rate XML | `EvidenceIncomplete` | `RightsLimited` | Record surface-applicable durable-use authority and an exact response/schema or XSD validation. Fiscal Data rights are not inherited by this separate surface. |
| FRED/ALFRED API | `DeniedForIntendedDurableUse` | `RightsBlocked` | Keep durable ingestion inactive unless later provider-controlled permission or unambiguous changed terms authorize the exact use and each selected series passes its own rights review. |

These are engineering admission decisions based on the assigned evidence. They are not provider-
defined status names or legal opinions.

## Evidence identity and lineage

Product code binds `MSQ-ONBOARDING-REPORT-2026-07-23` to the canonical structured decision record,
not to a mutable web page or the transient research workspace.

| Artifact | SHA-256 | Role |
| --- | --- | --- |
| [Canonical structured decision](2026-07-22-zero-fee-provider-onboarding/final-report.json) | `55b7f0385015fbd318c877f99e329c3198024a31fd8f05cb9e7e12e7663180cb` | Code-owned capability, rights, and refresh decision bound by `REPORT_DIGEST` |
| [Canonical source inventory](2026-07-22-zero-fee-provider-onboarding/source-inventory.json) | `7da1197edca1b182f6ebf4eecd97e2b3cc6200af75b32d5db46b7da09def369a` | Stable project source identifiers, including Treasury Fiscal Data as `DOC-031` |
| Independent validation report | `2e69cd3f8a3beeafbd1c8d6b7d7c4e74a9e4f978b4a9d262fad6ab6e7a81056c` | Semantically deduplicated validation across the assigned 2026-07-23 evidence |
| Independent validation inventory | `300ac3bfb9eac9428047634bffc86abcfbae5bd5364ffe4bc09cb26e09744b9d` | Frozen 31-source assignment plus explicit exclusions |
| Independent evidence audit | `1f29bfd6b58e143ee8fdce6520c637106a32f5fbcb66fd5da68820c867fa6003` | Terminal independent `PASS` verdict |

The validation run used batch-local `DOC-*` identifiers. Those identifiers do not replace the
canonical project inventory. In particular, Treasury Fiscal Data remains canonical `DOC-031` in
product evidence even though it was `DOC-009` inside the validation batch.

## Probe-lineage reconciliation

The final audit required each observation to retain its phase and evidentiary strength:

- SEC discovery observed HTTP 200 JSON with a declared user agent; a later batch received HTTP 403
  using a project URL. Neither observation is an exact, digest-bound production-runtime receipt
  using the complete monitored-contact identity.
- Treasury Fiscal Data discovery observed HTTP 200 JSON at `/services`; this is not a substitute
  for an exact selected-dataset response with schema, pagination, notice, and digest evidence.
- Treasury daily XML discovery observed HTTP 200 XML; no exact response-body receipt, schema/XSD
  result, or surface-specific durable-use grant was retained.
- A deliberately keyless FRED request observed the documented HTTP 400 missing-key behavior. It
  establishes neither credential authority nor permission for durable use.

These reconciliations preserve the fail-closed decisions in the matrix above.

## Assigned source register

Provider-controlled documentation supplies technical and rights evidence. Repositories and papers
inform implementation and provenance design but do not grant provider authority.

### Official provider and government sources

- SEC: [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
  and [Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions).
- BLS: [getting started](https://www.bls.gov/developers/home.htm),
  [FAQ and limits](https://www.bls.gov/developers/api_FAQs.htm),
  [registration](https://data.bls.gov/registrationEngine/),
  [v1 request contract](https://www.bls.gov/developers/api_signature.htm),
  [v2 request contract](https://www.bls.gov/developers/api_signature_v2.htm), and
  [terms](https://www.bls.gov/developers/termsOfService.htm).
- U.S. Treasury: [Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/),
  [daily interest-rate XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed), and
  [XML migration notice](https://home.treasury.gov/developer-notice-xml-changes).
- FRED/ALFRED: [v1 key contract](https://fred.stlouisfed.org/docs/api/api_key.html),
  [v2 key contract](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html),
  [account registration](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/),
  [API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html),
  [error contract](https://fred.stlouisfed.org/docs/api/fred/errors.html), and
  [current services terms](https://fred.stlouisfed.org/legal/terms/).

### Reviewed implementation evidence

- [dgunning/edgartools](https://github.com/dgunning/edgartools) for SEC client and filing workflow
  patterns.
- [keberwein/blscrapeR](https://github.com/keberwein/blscrapeR) for BLS request-shaping evidence.
- [fedspendingtransparency/fiscal-data](https://github.com/fedspendingtransparency/fiscal-data) for
  the provider-owned Fiscal Data implementation lineage.

### Research and standards evidence

- Federal Reserve Bank of St. Louis,
  [FRED, ALFRED, and VDC](https://doi.org/10.20955/r.88.81-94).
- BLS,
  [long-term revisions to U.S. labor-productivity estimates](https://www.bls.gov/osmr/research-papers/2024/pdf/ec240100.pdf).
- NIST, [Research Data Framework 2.0](https://doi.org/10.6028/NIST.SP.1500-18r2).
- OWASP, [Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html),
  [Authorization](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html),
  and [API resource-consumption controls](https://owasp.org/API-Security/editions/2023/en/0xa4-unrestricted-resource-consumption/).
- IETF, [OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/rfc/rfc8628.html) and
  [HTTP 429](https://www.rfc-editor.org/rfc/rfc6585.html).
- AWS, [control and limit retries](https://docs.aws.amazon.com/wellarchitected/latest/framework/rel_mitigate_interaction_failure_limit_retries.html).
- W3C, [PROV-DM](https://www.w3.org/TR/prov-dm/).
- DataCite, [rights and version metadata](https://datacite-metadata-schema.readthedocs.io/en/4.7/properties/rights/).

## Implementation consequences

- Provider profiles carry code-owned evidence and release state; caller-supplied rights cannot
  create persistence authority.
- A source becomes callable only after the onboarding lease, provider-specific request, evidence
  objects, durable desired recipe, and research-source registration agree exactly.
- Activation is serialized per provider surface. Exact retries are idempotent; a failed candidate
  can quarantine only its own compare-and-swap-matched durable state.
- Invalid legacy or superseded state quarantines only its provider. It does not prevent unrelated
  CLI, MCP, research, portfolio, valuation, or execution domains from starting.
- Credential-store work is outside the live path, single-flight, cancellation-aware after store
  serialization, and reaped if the request returns before the blocking provider call.
- Restart never prompts for a platform-managed credential. A valid recipe that needs user
  interaction remains desired but disabled until an explicit foreground resume.

## Limitations and refresh triggers

Refresh the affected profile before release whenever provider terms, quota dimensions, endpoint
schema, credential lifecycle, canonical URL, or selected-dataset notice changes. Denial-body hashes
and browser-rendered text are health or review evidence; they do not satisfy an exact HTTP 200
content-digest gate. Runtime probes remain separate, explicitly authorized release evidence and are
not part of the deterministic default test suite.

## Related evidence

- [Complete onboarding decision package](2026-07-22-zero-fee-provider-onboarding/final-report.md)
- [FRED/ALFRED rights decision](providers/fred-alfred-rights-2026-07-22.md)
- [BLS provider decision](providers/bls-public-data-api-2026-07-21.md)
- [HTTP source policy](2026-07-16-http-source-policy.md)
- [Capability traceability](2026-07-18-usable-release-traceability.md)
