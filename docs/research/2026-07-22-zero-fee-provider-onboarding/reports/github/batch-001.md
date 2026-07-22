# GitHub Repository Batch 001: Authorization and Native Credential References


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews five assigned repositories at exact default-branch commits observed on
2026-07-22. Repository evidence informs implementation fit and dependency evaluation; it does not
establish provider support, provider rights, or release admission.

## Sources Reviewed

| ID | Repository at observed commit | Stars / forks | License | Freshness and maintenance | Relevance and caveat |
| --- | --- | ---: | --- | --- | --- |
| GH-001 | [`cli/cli@efe3f16`](https://github.com/cli/cli/commit/efe3f165dd297c85fff11473dbf586f2d39fbf86) | 45,367 / 8,741 | MIT | Pushed 2026-07-22; v2.96.0 released 2026-07-02; active workflows/docs | Mature cross-platform CLI/auth reference; GitHub-specific and Go, not a provider capability source |
| GH-003 | [`git-ecosystem/git-credential-manager@2fe99b8`](https://github.com/git-ecosystem/git-credential-manager/commit/2fe99b867b710265e3273b48da7513d91e6ef8eb) | 9,114 / 2,860 | MIT per repository README/license | Commit and v2.9.1 release 2026-07-14; active cross-platform project | Demonstrates browser-mediated auth and platform stores; .NET/Git-specific |
| GH-004 | [`open-source-cooperative/keyring-rs@17054f0`](https://github.com/open-source-cooperative/keyring-rs/commit/17054f05971a4e8eabbcd5970ad37bcfa7e61048) | 756 / 66 | Apache-2.0 OR MIT | v4.1.5 released 2026-07-14; pushed 2026-07-20; CI/examples visible | Direct Rust candidate for native stores; backend semantics and supported targets still require exact admission |
| GH-005 | [`ramosbugs/oauth2-rs@72ce744`](https://github.com/ramosbugs/oauth2-rs/commit/72ce74401c26eb4dc85dcbfde587bbcfc149e3ae) | 1,198 / 207 | Apache-2.0 OR MIT | Pushed 2026-02-22; repository activity observed 2026-07-21; 47 open issues | Typed Rust OAuth building blocks; cannot create provider support or registration eligibility |
| GH-008 | [`krakenfx/kraken-cli@aa32814`](https://github.com/krakenfx/kraken-cli/commit/aa32814cea70913a70c9909693a7abd762963e83) | 672 / 95 | MIT | v0.3.2 released 2026-04-20; repository activity observed 2026-07-21 | Official public/paper/private CLI comparison; marked experimental and stores may differ from Market Squawk requirements |

Counts and timestamps are GitHub API observations as of 2026-07-22, not permanent popularity or
quality claims.

## Findings

1. **Confirmed repository fact:** keyring-rs exposes a common set/get/delete API over native macOS,
   Windows, and Unix stores, while its documentation recommends selecting only the specific backend
   crates an application needs. **Inference:** it is a strong candidate for Market Squawk's OS-store
   adapter, subject to locked dependency, platform, prompt, deletion, headless, and failure testing.
   [keyring-rs](https://github.com/open-source-cooperative/keyring-rs/tree/17054f05971a4e8eabbcd5970ad37bcfa7e61048)
2. **Confirmed repository fact:** Git Credential Manager provides cross-platform secure credential
   storage and opens a human sign-in flow when needed. **Inference:** provider-controlled human
   steps should be resumable product states, while credentials remain reusable opaque store items.
   [Git Credential Manager](https://github.com/git-ecosystem/git-credential-manager/tree/2fe99b867b710265e3273b48da7513d91e6ef8eb)
3. **Confirmed repository fact:** oauth2-rs is a strongly typed OAuth 2.0 library with documented
   Rust support and extension surfaces. **Inference:** a protocol library can implement an admitted
   flow but must stay below Market Squawk's provider capability policy; it cannot infer an issuer,
   redirect, scope, client registration, device flow, or rights grant.
   [oauth2-rs](https://github.com/ramosbugs/oauth2-rs/tree/72ce74401c26eb4dc85dcbfde587bbcfc149e3ae)
4. **Confirmed repository fact:** Kraken CLI separates public market data and paper operations from
   authenticated private commands and instructs users to create keys in Kraken settings. Its own
   README warns that the software is experimental and can execute financial transactions.
   **Inference:** it corroborates the manual-key boundary but is not safe authority for Market
   Squawk execution or secret-storage design.
   [Kraken CLI](https://github.com/krakenfx/kraken-cli/tree/aa32814cea70913a70c9909693a7abd762963e83)
5. **Confirmed repository fact:** GitHub CLI is a maintained cross-platform command product with
   release provenance and a large test/contribution surface. **Inference:** it is an operational
   reference for human-resumed CLI onboarding, but GitHub-specific behavior is not automatically
   portable to financial providers.
   [GitHub CLI](https://github.com/cli/cli/tree/efe3f165dd297c85fff11473dbf586f2d39fbf86)

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Decision implication |
| --- | --- | --- | --- | --- |
| Mature products separate protocol/provider policy from credential storage | GH-001, GH-003, GH-004, GH-005 | Cross-repository inference | Medium | Preserve explicit service/adaptor boundaries |
| Rust has maintained candidate libraries for OAuth and native stores | GH-004, GH-005 | Confirmed ecosystem fact | High | Evaluate rather than inventing another generic implementation |
| A provider CLI consuming a key does not prove automated issuance | GH-008 | Confirmed limitation | High | Retain manual provider action |
| Repository activity is not provider release evidence | All | Methodological constraint | High | Official docs and runtime gates remain authoritative |

## Limitations and Non-Findings

- Stars and release frequency do not prove security, correctness, adoption, or fitness.
- No dependency was added, built, executed, or approved by this research.
- Repository code does not establish provider rights, price, eligibility, quotas, or protocol support.
- The existing Market Squawk encrypted vault is project context and remains an exact-commit
  admission candidate; none of these repositories externally validates it.

## Source List

GH-001, GH-003, GH-004, GH-005, and GH-008 are first-class inventory records assigned to
`github-batch-001`. Exact observed commits are stored as their content digests.
