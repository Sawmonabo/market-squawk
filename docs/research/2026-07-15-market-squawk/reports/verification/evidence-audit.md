# Evidence Audit

## Table of Contents

- [Artifact Coverage](#artifact-coverage)
- [Required Category Coverage](#required-category-coverage)
- [Citation Coverage](#citation-coverage)
- [Source Quality Findings](#source-quality-findings)
- [Redundancy Findings](#redundancy-findings)
- [Unsupported Claims](#unsupported-claims)
- [Staleness or Freshness Risks](#staleness-or-freshness-risks)
- [Required Fixes](#required-fixes)
- [Verdict](#verdict)

## Artifact Coverage

The research workspace is structurally complete for its **2026-07-15** cutoff.

| Artifact class | Verified result |
| --- | --- |
| Research controls | `manifest.md`, `search-plan.md`, `batch-manifest.md`, and valid machine-readable [`source-inventory.json`](../../source-inventory.json) are present. |
| Inventory | 42 assigned source families: 10 GitHub, 10 papers, 14 official-documentation, and 8 reputable-authority families. No source is unassigned. |
| Batch lineage | 14 planned batches: 3 GitHub, 4 papers, 5 documentation, and 2 reputable-source batches. Every inventory assignment matches the batch manifest, and every batch prompt/report path exists. |
| Discovery | Discovery reports and candidate JSON exist for all four categories. |
| Synthesis | All four category reports exist: [GitHub](../category-synthesis/github-synthesis.md), [papers](../category-synthesis/papers-synthesis.md), [official documentation](../category-synthesis/docs-synthesis.md), and [reputable sources](../category-synthesis/reputable-sources-synthesis.md). |
| Final report | [`final-report.md`](../../final-report.md) contains every Final Report contract heading, a table of contents, category sections, source matrix, source-inventory appendix, and batch-report inventory. |
| Local links | Automated resolution of local Markdown links in the final report and four category syntheses found zero broken targets. |

Every reviewed report contains `## Table of Contents` and source URLs. The only non-blocking shape
variance is [GitHub batch 002](../github/batch-002.md), which uses semantically equivalent headings
such as “Executive assessment,” “Repository findings,” and “Cross-source implications” instead of
all exact Batch Deep-Dive contract labels. It still contains scope metadata, evidence, repository
notes, limitations/non-findings, inline citations, and a source list, so no evidence is missing.

## Required Category Coverage

**GitHub — covered.** The inventory records stars, forks, license, maintenance/freshness, selection
rationale, and caveats for all ten repositories. Batch reports use pinned commits where code-level
claims matter. The synthesis separates direct dependency candidates (Arrow/Parquet, DataFusion,
MCP SDK), conditional `ort`, and architecture/reference-only systems. LGPL, AGPL, release-candidate,
license-transition, mutable-workflow, paper-fill, and production-disclaimer risks are retained.

**Papers — covered.** All ten papers retain authors, publication/venue or preprint status, method,
key result, limitations, and Market Squawk relevance. The two 2026 v1 preprints remain explicitly
provisional; Fonseca is identified as a nine-day-old, single-author, unreviewed submission with
unavailable proprietary validation data. The PBO and Almgren–Chriss reviews correctly limit detailed
claims because only publisher abstracts were available. Historical Disruptor performance is not
transferred to Rust or Market Squawk.

**Official documentation — covered.** Fourteen source families use first-party language/runtime,
Apache, SQLite, MCP, Coinbase, Kraken, SEC, FRED/ALFRED, BLS, Treasury, and project Rustdoc sources.
Provider-specific differences—Coinbase sequence recovery, Kraken CRC precision, Treasury pagination,
SEC identity/rate limits, FRED vintages, BLS tiers, SQLite writer/WAL behavior, and unbounded
DataFusion defaults—are preserved rather than flattened into generic adapter claims.

**Reputable sources — covered.** Accounting uses FASB/IFRS material; public access uses SEC/BLS;
security and provenance use NIST SSDF, OWASP ASVS, and SLSA; model/stress governance uses current
SR 26-2 and Basel guidance. The reports preserve authority boundaries: FASB ASU 2011-04 is amendment
material rather than the complete current Codification; IFRS supporting material is not itself the
standard; SR 26-2 and Basel are banking guidance, not direct obligations or approval for local
software.

The final report gives every required category its own section and does not imply that one category
proves another. In particular, provider reputation does not prove `DirectVerified`, fair-value
hierarchy does not prove execution quality, protocol conformance does not prove authorization, and
security-framework alignment does not prove financial correctness.

## Citation Coverage

Citation coverage is sufficient for a decision synthesis:

- Confirmed factual claims in the final report have nearby primary-source links. Protocol, quota,
  version, accounting, and current-status claims use authoritative sources rather than repository
  summaries.
- Design recommendations are labeled **Inference**, and their lineage is visible through the
  final category sections, source matrix, four category syntheses, and linked batch reports.
- The final report does not attempt one row per source in its decision-oriented source matrix;
  Appendix A correctly delegates exhaustive 42-family lineage to the machine-readable inventory.
- Appendix B links all 14 batch reports and all four syntheses. Automated local-link checking found
  no broken final/synthesis artifact links.
- No bare performance, implementation-completion, regulatory-compliance, or production-readiness
  claim was found. The report repeatedly states that actual repository capabilities, all-feature
  compatibility, latency, throughput, memory, and release acceptance remain unproven.

Some executive/source-matrix links point to moving repository or project roots, but the GitHub
synthesis preserves pinned commit lineage for code-specific claims. This is acceptable for a
high-level report, though pinned links are preferable when citing implementation details.

## Source Quality Findings

The source hierarchy is sound and appropriately scoped:

1. **Highest authority:** official exchange/provider specifications and policies control channel,
   checksum, quota, vintage, and pagination claims; official standard-setter/government/industry
   sources control accounting, security, provenance, and supervisory status.
2. **Implementation evidence:** GitHub claims use repository code, tests, policies, releases, and
   pinned commits. Popularity is not treated as assurance. Upstream security policies are described
   as process evidence, not certification.
3. **Research evidence:** peer-reviewed/full primary papers support mathematics and mechanisms.
   Vendor-authored, dated, narrow-sample, abstract-only, and unreviewed sources have explicit
   confidence limits. Sample coefficients and simulation fidelity are not generalized.
4. **Adoption evidence:** direct dependencies are recommended only for mechanics they actually
   supply. Application-owned provenance, point-in-time semantics, source qualification, model
   trust, risk, fair-value evidence, MCP authorization, and paper execution remain separate.

The final report correctly gives primary official documentation precedence over repository
behavior for Coinbase/Kraken protocol rules and over academic associations for ASC 820/IFRS 13
classification. It also preserves current supersession: SR 26-2 replaces SR 11-7/SR 21-8, and the
2018 Basel principles replace the 2009 set.

## Redundancy Findings

The final report is a genuine synthesis, not a concatenation of the batch reports. Its 349 lines
compress substantially larger batch/category evidence into decision areas, adoption tiers, staged
gates, risks, and a source matrix. Repeated themes—fail-closed behavior, live/research separation,
versioned provenance, source-specific qualification, and non-bypassable risk—appear in the
executive summary, category findings, and recommendations because they are cross-cutting acceptance
constraints, not accidental duplicate evidence.

The explicit no-evasion boundary is repeated in documentation, reputable-source, GitHub, and final
reports. That repetition is appropriate because it resolves a direct requested capability and
prevents ordinary proxy/retry configuration from being misread as permission to circumvent access
controls.

## Unsupported Claims

No material unsupported claim requiring rejection was found.

- The architecture, dependency, risk, point-in-time, model-bundle, scenario, and test conclusions
  are identified as inferences or adoption decisions.
- `tract-onnx` and `ort` are presented as bundle-qualified candidates, not universal ONNX support.
- Arrow/DataFusion/SQLite/MCP are credited with mechanics only; finance semantics, resource policy,
  authorization, and risk remain application-owned.
- Coinbase `full` and checksum-validated Kraken data are only candidates after additional local
  gates. No provider-wide or implementation-wide `DirectVerified` claim is made.
- Supervisory and security sources are not presented as certification, applicability, or regulatory
  approval. Fair-value conclusions remain subject to complete current standards and professional
  judgment.
- The final report does not claim that Market Squawk is implemented, tested, secure, compliant, or
  performance-qualified. Its opening decision—proceed through staged test gates and do not claim the
  complete release—is consistent with every category's non-findings.

The report expressly excludes identity/account rotation, misleading identity, browser/TLS
fingerprint spoofing, CAPTCHA/anti-bot bypass, proxy rotation intended to defeat blocking, and
distributed quota evasion. Lawful user keys, disclosed configured proxies, caching, coalescing,
bounded retry, and optional licensed adapters remain correctly distinguished.

## Staleness or Freshness Risks

- The research is frozen at **2026-07-15**. This audit was completed on **2026-07-16**; the one-day
  difference does not invalidate the date-anchored conclusions, but the report must continue to
  display its cutoff.
- GitHub stars, forks, branch heads, releases, support windows, CI workflows, and security policies
  are volatile. Pinned commits protect reviewed code claims; implementation selection must refresh
  releases, exact package metadata, transitive licenses, and advisories.
- Documentation links using `latest` or unversioned project roots can move. Rust, Tokio, Reqwest,
  Arrow/DataFusion, MCP SDK, `tract`, `ort`, and SQLite compatibility must ultimately be frozen by
  the toolchain, features, lockfile, native runtime, and local verification output.
- SEC, BLS, FRED, Treasury, Coinbase, and Kraken contracts/policies can change. Provider-policy and
  protocol fixtures need review cadence, source health, and opt-in live-contract tests.
- FASB amendment/supporting material does not replace access to the applicable complete current
  accounting literature. Model/stress guidance and ASVS/SLSA versions require re-evaluation before
  any formal compliance or assurance claim.
- Fonseca and Noble et al. were v1 preprints at cutoff. Their review/artifact status should be
  refreshed before relying on their formal or calibration claims beyond the current test-design
  inspiration.

## Required Fixes

There are **no blocking evidence fixes** required before using the final report as a dated
architecture and implementation-planning document.

Recommended archival and implementation follow-ups are:

1. Normalize [GitHub batch 002](../github/batch-002.md) to the exact Batch Deep-Dive heading names
   if strict report-contract uniformity is required; its substantive evidence is already complete.
2. Prefer pinned commit/release URLs for any future code-specific claim copied from the final
   executive summary or source matrix.
3. At dependency selection time, record exact crates/features/licenses and refresh advisories;
   resolve the MCP SDK version/example mismatch, compatible Arrow/DataFusion family, SQLite runtime,
   and `ort` native-runtime provenance through locked tests.
4. Refresh exchange/provider policies, accounting authority, GitHub releases, and the two provisional
   papers before reissuing the report with a later “as of” date.
5. Do not convert any architectural recommendation into an implementation, performance, security,
   accounting, legal, or regulatory claim until the repository produces the specified local
   verification evidence.

## Verdict

**PASS_WITH_NOTES**

The research set satisfies required category coverage, source/batch lineage, primary-source and
confidence discipline, final-report structure, citation traceability, uncertainty preservation,
local-link integrity, and the no-evasion boundary. The notes concern one non-material batch-heading
variance and expected freshness/version closure before implementation or republication; they do
not undermine the final report's staged, test-gated decision.
