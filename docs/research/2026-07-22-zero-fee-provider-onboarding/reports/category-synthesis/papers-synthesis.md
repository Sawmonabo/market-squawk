# Academic and Research Paper Synthesis


## Table of Contents

- [Category Scope](#category-scope)
- [Sources Covered](#sources-covered)
- [High-Confidence Findings](#high-confidence-findings)
- [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
- [Conflicts and Disagreements](#conflicts-and-disagreements)
- [Trends and Patterns](#trends-and-patterns)
- [Implications for Market Squawk](#implications-for-market-squawk)
- [Gaps](#gaps)
- [Source Matrix](#source-matrix)

## Category Scope

This synthesis covers three original formal or empirical security papers. The seven RFC sources are
handled by official-documentation synthesis; they are not counted as academic research.

## Sources Covered

- PAPER-009: Fett, Küsters, and Schmitz, formal OAuth 2.0 analysis, arXiv v4/CCS 2016.
- PAPER-010: Luo et al., empirical integration-platform OAuth study, USENIX Security 2025.
- PAPER-012: Fett, Hosseyni, and Küsters, formal Financial-grade API analysis, arXiv/IEEE S&P 2019.

Bibliographic, method, result, limitation, and relevance details are in
`reports/papers/batch-001.md`.

## High-Confidence Findings

- Formal analyses found attacks in OAuth/FAPI compositions and proved corrected modeled profiles
  only after precise security properties, participant assumptions, and mitigations were specified.
  [OAuth analysis](https://arxiv.org/abs/1601.01229),
  [FAPI analysis](https://arxiv.org/abs/1901.11520)
- The 2025 empirical study found cross-app takeover/forgery classes across studied integration
  platforms when app differentiation was insufficient.
  [USENIX study](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan)

## Medium- and Low-Confidence Findings

- **Medium-high engineering inference:** a multi-provider local portal needs exact per-provider,
  per-client, per-account, per-redirect, per-session, one-time transaction binding.
- **Low confidence / not admitted:** that any studied vulnerability exists in Market Squawk; no
  product implementation was evaluated.

## Conflicts and Disagreements

There is no source conflict. The evidence types differ: two papers provide formal results under
model assumptions, while one measures real integration platforms. The synthesis does not convert
either into universal prevalence or implementation proof.

## Trends and Patterns

- Security depends on coherent protocol composition, not isolated checklists.
- Integration layers increase cross-provider/app confusion risk.
- High-assurance financial profiles still require exact conformance and threat assumptions.

## Implications for Market Squawk

Use a typed authorization transaction that binds every participant and consumes callback/state once.
Provider documentation must enable the complete profile; libraries and standards cannot infer it.
If execution-capable broker OAuth is introduced, adopt one supported high-assurance profile and its
conformance boundary rather than selecting individual FAPI mechanisms.

## Gaps

- No direct evaluation of Market Squawk or selected Rust libraries.
- No provider-specific OAuth capability, rights, or operational evidence.
- Formal assumptions and empirical platform scope must be revisited during implementation review.

## Source Matrix

| Batch | Papers | Evidence type |
| --- | --- | --- |
| papers-batch-001 | PAPER-009, PAPER-010, PAPER-012 | Two formal analyses and one empirical measurement study |
