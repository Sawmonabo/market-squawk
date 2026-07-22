# Evidence Audit


## Table of Contents

- [Artifact Coverage](#artifact-coverage)
- [Required Category Coverage](#required-category-coverage)
- [Citation and Lineage Coverage](#citation-and-lineage-coverage)
- [Source Quality Findings](#source-quality-findings)
- [BLS Rights and Provider Decisions](#bls-rights-and-provider-decisions)
- [Acceptance Criteria and Project Provenance](#acceptance-criteria-and-project-provenance)
- [Existing Vault Context](#existing-vault-context)
- [Redundancy Findings](#redundancy-findings)
- [Unsupported Claims](#unsupported-claims)
- [Staleness or Freshness Risks](#staleness-or-freshness-risks)
- [Prior Defect Closure](#prior-defect-closure)
- [Required Fixes and Notes](#required-fixes-and-notes)
- [Verdict](#verdict)

## Artifact Coverage

This is a fresh independent audit of the remediated Task 19A research candidate as of
**2026-07-22**. The repository audit base in the final artifacts,
`fe600f7c50af34482bb95feacacf6d0fdc2dbb03`, matched repository `HEAD` during verification.
No provider call, account, credential, authorization flow, OS secure-store operation, tracked
repository file, Cargo build, branch, or worktree was changed by this audit.

The canonical artifact paths are now aligned for the decision outputs:

- `final-report.md`
- `final-report.json`
- `source-inventory.json`
- `reports/verification/evidence-audit.md`

The manifest, final-writer prompt, verifier prompt, final Markdown appendix, and final JSON artifact
list all identify those root/canonical locations consistently.

Structural verification passed:

- every JSON artifact parsed successfully;
- every canonical Markdown report contains `## Table of Contents` and source URLs;
- all seven planned batches have matching prompt and report files;
- all four included categories have a category synthesis;
- the final report contains every section required by the report contract;
- no unfinished marker or exact duplicated long paragraph was found; and
- the deep-research validator returned `Deep research artifacts are structurally valid.`

## Required Category Coverage

All four required categories have genuine selected evidence, a canonical batch, and a category
synthesis. RFCs are correctly classified as official standards documentation rather than being
used to simulate the academic-paper lane.

| Category | Assigned sources | Canonical batch coverage | Synthesis | Result |
|---|---:|---|---|---|
| Official documentation | 46 | `docs-batch-001` through `docs-batch-004` | `docs-synthesis.md` | Pass |
| GitHub repositories | 5 | `github-batch-001` | `github-synthesis.md` | Pass |
| Academic/research papers | 3 | `papers-batch-001` | `papers-synthesis.md` | Pass |
| Reputable sources | 6 | `reputable-sources-batch-001` | `reputable-sources-synthesis.md` | Pass |

The GitHub batch records exact commits, stars, forks, license, freshness/maintenance, relevance, and
caveats. The paper batch records title, authors, source/venue/date, method, result, limitations, and
relevance for two formal analyses and one empirical study. The reputable-source batch records
organization, credibility rationale, scope, limitations, and relevance. Official-documentation
claims use official provider, government, RFC Editor, or OS-platform sources.

## Citation and Lineage Coverage

The canonical inventory contains **77 records: 60 assigned and 17 explicitly excluded**. Assigned
sources are distributed as 46 documentation, 5 GitHub, 3 paper, and 6 reputable-source records.

Every assigned record has:

- a unique ID and URL;
- category, priority, `assigned` status, and a real batch assignment;
- access/retrieval date;
- credibility, freshness, selection, source-type, and content-reference metadata; and
- either a captured SHA-256 response digest, an exact Git commit, or an explicit
  `reference_only_refresh_required` state.

The assigned-source-to-batch set and the batch-manifest source set are exactly equal. Every one of
the 60 assigned IDs appears in its declared batch prompt and batch report. All seven batch prompt
and report paths exist. Category syntheses identify their complete batch inputs.

The final source graph is complete:

- `final-report.json.sources` contains exactly the same 60 selected source objects as the assigned
  subset of `source-inventory.json`;
- all 60 source IDs are unique;
- every source ID referenced anywhere in the final JSON resolves;
- no final source is unreferenced;
- the final Markdown contains all 60 selected source IDs and no unknown ID; and
- the final source matrix provides a primary URL and decision role for every selected source.

The inventory's digest evidence is internally coherent: 47 sources carry SHA-256 response digests,
5 GitHub sources carry exact 40-character Git commits, and 8 mutable official pages carry explicit
refresh-required stable references. The five Git commits still resolve through the GitHub API.
Independent digest spot checks for Coinbase documentation, an RFC, an arXiv paper, and OWASP
guidance reproduced the inventory hashes exactly.

One selected source, `DOC-FRED-RT-001`, was manually promoted from the documentation discovery
report rather than its structured candidate JSON. It is nevertheless fully registered, digested,
assigned to `docs-batch-003`, deep-dived, synthesized, and cited. `DOC-014-LEGACY` is an excluded
redirect-history record; canonical `DOC-014` is the selected Kraken key-info source.

## Source Quality Findings

Source quality is appropriate for the claims made:

- provider access, authentication, quota, lifecycle, and rights claims use official provider or
  government pages;
- OAuth behavior uses RFC Editor publications, while provider support remains a separate gate;
- OS secure-store behavior uses Apple, Microsoft, and freedesktop primary documentation;
- implementation-fit observations use exact-commit upstream or official GitHub repositories;
- research claims use the original arXiv or USENIX publications and retain model/study limitations;
  and
- OWASP, UK NCSC, and NIST sources are explicitly treated as guidance rather than provider terms.

Current official-source spot checks on 2026-07-22 supported the material provider conclusions. In
particular, the [BLS terms](https://www.bls.gov/developers/termsOfService.htm),
[BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
[FRED legal terms](https://fred.stlouisfed.org/legal/),
[Coinbase public endpoint documentation](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api),
[Kraken Exchange overview](https://docs.kraken.com/exchange/guides/overview), and
[SEC webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) remain consistent
with the final report's scoped claims.

Confidence survives synthesis. Each of the ten provider rows has both a confidence value and an
`evidence_classification` that distinguishes confirmed official facts from scoped engineering
inference. GitHub, paper, reputable-source, and acceptance-criteria conclusions similarly preserve
fact/result/guidance versus inference or project-policy boundaries.

## BLS Rights and Provider Decisions

The prior material BLS error is corrected throughout the batch, documentation synthesis, final
Markdown, and final JSON.

The current official BLS terms establish two sides that the remediated artifacts now preserve:

1. BLS.gov data generally should not carry end-use controls after access.
2. Users must preserve access-date citation and the specified quality/timeliness disclaimer, avoid
   false representation and unauthorized logo use, comply with access limits, and respect any
   third-party intellectual-property boundary.

The final report no longer treats BLS as a pure rights non-finding or as a blanket permission. It
labels scoped BLS release admission as an engineering inference, binds it to exact BLS provenance
and intended operations, preserves the duties and third-party boundary, and still requires bounded
runtime evidence. The v2 key is correctly limited to quota/features rather than being represented
as a rights grant.

Dependent material is consistent:

- both BLS provider rows use scoped, runtime-smoke-pending release states;
- the dedicated BLS conflict preserves the permission signal and duties;
- AC-08 requires tier limits, human v2 registration/renewal, and the scoped BLS rights record;
- AC-22 explicitly prevents representing BLS as a pure non-finding and requires duty enforcement;
- the decision, release-state definitions, risks, gaps, and implementation recommendations all use
  the corrected scope; and
- BLS is not represented as release-approved because no runtime or terms-duty implementation was
  exercised.

The other provider decisions remain evidence-faithful. FRED stays hard-blocked because current terms
conflict with mandatory storage/database and modeling use. Coinbase/Kraken public routes remain
rights-limited, private routes remain evidence-incomplete, Treasury XML remains durability-pending,
and SEC/Fiscal Data remain documentation-ready but runtime-smoke-pending. No provider is claimed
release-ready solely from this research.

## Acceptance Criteria and Project Provenance

The final JSON contains exactly 24 unique criteria, `T19A-AC-01` through `T19A-AC-24`. Every record
has a requirement, evidence requirement, blocking scope, and `basis_type`. Every external source ID
resolves to a selected inventory source and an assigned batch.

The provenance correction is explicit:

- AC-21 is typed as mixed external rate evidence and Market Squawk architecture. Its heartbeat-
  versus-market-freshness clause is explicitly identified as an internal project requirement.
- AC-24 has no external source IDs and is typed entirely as a Market Squawk architecture/authority
  requirement. The final report expressly says external provider/RFC/OS/GitHub/paper/guidance
  sources do not prove it.
- AC-14 has no external source IDs and is typed as project context requiring exact-commit admission
  of an existing implementation.

The remaining 21 criteria are labeled as external-evidence-derived engineering requirements. That
label correctly avoids claiming that external sources prescribe Market Squawk's exact internal type
names, state machine, or test topology.

## Existing Vault Context

The existing-vault wording is accurate and bounded. The report describes
`market-squawk-platform::secrets` as an existing candidate supplied by project context, not an
absent feature, selected external construction, or admitted release capability.

Read-only exact-head inspection confirms the candidate basis:

- `crates/market-squawk-platform/src/secrets/crypto.rs` contains Argon2id derivation,
  XChaCha20-Poly1305 encryption, random salt/nonce handling, associated-data binding, and
  zeroization paths;
- `crates/market-squawk-platform/src/secrets/encrypted.rs` implements a capability-confined,
  crash-recoverable encrypted-store and rotation state machine; and
- `crates/market-squawk-platform/src/authority_state.rs` and its submodules implement durable
  generation/envelope/recovery machinery while explicitly documenting residual rollback scope.

The report does not convert that existence check into AC-14 admission. It still requires independent
review of parameters, custody, permissions, corruption, migration/backup, rollback, crash/fault
behavior, and plaintext-fallback invariants before a release claim. No correction is required.

## Redundancy Findings

The final report is a coherent decision document rather than a concatenation of batch reports.
Provider findings are summarized once, category sections preserve their distinct evidence role, and
the source matrix supplies lookup provenance without repeating the full analysis. No exact
duplicated long paragraph was found. Repeated release gates in the executive summary, decision, and
acceptance criteria are purposeful decision-to-implementation traceability, not content churn.

## Unsupported Claims

No unsupported material conclusion was found.

The report consistently labels release states, capability models, state machines, and acceptance
criteria as engineering decisions or project requirements when the sources provide inputs rather
than the exact design. It preserves explicit non-findings for provider account/key automation,
universal zero-fee guarantees, provider OAuth/device/DCR availability, remote revoke, cross-platform
store equivalence, runtime behavior, legal conclusions, and external validation of the existing
vault.

The report's recommendation to proceed is conditional. It does not claim that Task 19A, any
provider surface, any secure-store backend, or the encrypted fallback has passed implementation or
release gates.

## Staleness or Freshness Risks

Eight selected official sources do not have a retained response-body digest and are explicitly
marked `reference_only_refresh_required`: `DOC-009`, `DOC-010`, `DOC-014`, `DOC-019`, `DOC-020`,
`DOC-026`, `DOC-028`, and `DOC-029`. These include mutable provider terms, SEC behavior, BLS quota/
terms pages, and Kraken key-info documentation.

This does not invalidate the research verdict because the exact official URL, access date, visible
revision/reference, and conservative release effect are retained, and current primary-source spot
checks confirmed the material claims. The inventory policy and AC-01/AC-22/AC-23 correctly require
a new digest or bounded runtime evidence before implementation/release admission. These eight
records must not be mistaken for immutable captured evidence.

Five GitHub observations are exact-commit stable. Stars, forks, latest release, and default-branch
head are inherently time-varying; the batch correctly labels them as 2026-07-22 observations rather
than permanent quality or adoption facts.

## Prior Defect Closure

| Prior defect | Closure evidence | Status |
|---|---|---|
| BLS rights omission | Correct permission signal, duties, conflict, provider rows, AC-08, and AC-22 are present and current-source verified | Closed |
| Missing GitHub/paper/reputable lanes | One genuine batch and synthesis now exist for each, with category-required metadata | Closed |
| Incomplete source assignment and batch lineage | 60 assigned sources exactly equal the seven batch source sets; every ID appears in its prompt/report | Closed |
| Incomplete root inventory | Complete selected/excluded ledger with category, priority, status, assignment, access, and digest/reference policy | Closed |
| Lost confidence and inference labels | Provider confidence/evidence classification and category/AC basis labels are preserved in Markdown and JSON | Closed |
| Conflicting final artifact paths | Canonical root final report/JSON and `reports/verification/evidence-audit.md` agree across manifest and final prompts | Closed |
| AC-21/AC-24 provenance conflation | Mixed and project-only provenance are explicitly typed and described | Closed |
| Existing vault mischaracterization risk | Existing candidate is acknowledged, directly grounded, and still independently admission-gated | Closed |

## Required Fixes and Notes

No material evidence correction is required before using this report as the Task 19A implementation
design input. Three non-blocking artifact notes remain:

1. `prompts/discovery/github-discovery.md` and `prompts/discovery/papers-discovery.md` declare
   separate `github-discovery.md`/`papers-discovery.md` and candidate JSON outputs, while the actual
   discovery evidence is the combined `github-papers-discovery.md` and
   `github-papers-candidates.json`. The combined artifact substantively covers both categories, but
   the two prompt output declarations should be updated or the report split before archival.
2. `final-report.json.category_evidence` contains explicit GitHub, paper, and reputable-source
   entries but no parallel documentation entry. Documentation evidence is fully represented by the
   provider matrix, 46 final source records, and the Markdown category section, so the source graph
   is complete; adding a docs category entry would make the structured companion symmetric.
3. Capture fresh response-body digests for the eight `reference_only_refresh_required` official
   sources before implementation admission or release evidence is frozen, as already required by
   the inventory policy and acceptance criteria.

## Verdict

**PASS_WITH_NOTES**

The remediated candidate is evidence-complete enough to guide Task 19A implementation. All four
required source categories have genuine batch and synthesis coverage; the 60-source lineage is
complete; material claims and all 24 acceptance criteria are traceable; BLS rights are accurately
scoped; confidence and inference boundaries survive synthesis; project-only requirements are
separated; and the existing vault is correctly treated as an unadmitted existing candidate. The
three notes above are artifact/freshness hygiene and do not reopen the prior material FAIL.
