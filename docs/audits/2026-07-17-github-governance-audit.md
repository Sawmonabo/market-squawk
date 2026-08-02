# GitHub repository-governance audit — 2026-07-17

- Retrieved: 2026-07-17
- Audit base: `6d8801a1e9f5027bc3e1db3500a9a1a9f9fb85d6`
- Remote `main`: `c568ef07c676e0f0e440a5e218c653bcc8757e94`
- Remote integration branch: `6d8801a1e9f5027bc3e1db3500a9a1a9f9fb85d6`

This is a read-only audit of `Sawmonabo/market-squawk`. The audit used authenticated GitHub CLI
`GET` requests and repository files; it did not change repository settings, branches, pull
requests, labels, or remote content. The local audit base is an evidence anchor, not quarter
approval. Remote settings are point-in-time observations and must be refreshed before enforcement
or release claims.

## Observed repository state

- The repository was private with `main` as the default branch. Issues were enabled; projects,
  wiki, and discussions were disabled. Squash was the only enabled merge method and merged branches
  were configured for deletion.
- The personal private repository reported `allow_forking: true`. A separate setup pass attempted
  to disable it and received HTTP 422 stating that this setting can be changed only on
  organization-owned private repositories. It is therefore recorded as non-configurable and
  non-applicable to this personal-repository governance lane, not as an unresolved toggle.
- Remote `main` was still the initial repository commit. Draft pull request 1 targeted `main` from
  `feat/stage-1-foundation`; the integration branch matched this audit base.
- Dependabot vulnerability-alert access returned HTTP 204, demonstrating that the authenticated
  caller could access that endpoint at retrieval time.
- GitHub Actions was enabled with all actions permitted. GitHub's repository-level
  `sha_pinning_required` field was `false`; the version-controlled workflow policy therefore remains
  the active control that rejects mutable action references.
- The repository exposed the expected self-hosted, Rust, market-data, portfolio, risk, and MCP topic
  metadata. These topics are descriptive and do not establish implemented capability.
- A separate repository-setup pass created the Q2, platform, sources, authority-risk, remediation,
  and checkpoint labels; applied the three applicable Q2/remediation/authority-risk labels to pull
  request 1; and created open milestone 1, `Q2 — Authority foundation`. This read-only lane did not
  duplicate or alter those settings.

## Version-controlled findings and remediation

The audited tree declared `Apache-2.0 OR MIT` in Cargo metadata but had neither full license text.
It also lacked structured issue forms, a pull-request template, CODEOWNERS, Actions dependency
updates, and superseded-PR concurrency. This governance lane adds:

- canonical `LICENSE-APACHE` and `LICENSE-MIT` files and a matching README declaration;
- weekly Cargo and GitHub Actions Dependabot update discovery;
- PR-only CI cancellation keyed by workflow, event, and pull-request number while giving `main`
  pushes a unique run ID and never cancelling them;
- default and `.github/` ownership by `@Sawmonabo`;
- structured bug and feature forms, a hardened pull-request template, and a chooser that routes
  confidential security reports to `SECURITY.md`; and
- a standard-library policy test for these durable invariants without full-file snapshots.

These files improve review inputs and detect local drift. CODEOWNERS requests review only after it
is present on the pull request's base branch, and by itself does not enforce approval or protect a
branch.

## Residual remote enforcement

### Branch protection and rulesets

Authenticated reads of the `main` branch-protection and repository-ruleset endpoints returned HTTP
403 with GitHub's upgrade-or-public response. That response is recorded as the exact observation;
it is not generalized here as a permanent product-plan or entitlement claim. No active protection
rule was independently observable during this audit. The integration owner must refresh the API
state before claiming that `main` requires pull requests, status checks, conversation resolution,
linear history, signed commits, or CODEOWNER approval.

### Secret scanning and push protection

The repository response did not expose a `security_and_analysis` state, so secret scanning and push
protection were not independently verified. The private-vulnerability-reporting endpoint returned
HTTP 404. The repository still has a local full-history credential gate and a `SECURITY.md` policy,
but those controls do not establish hosted secret scanning, hosted push protection, or an externally
available private reporting form. Refresh these states through the authenticated UI or API before
claiming them. Do not route vulnerability details through a public issue while that route is being
resolved.

### Auto-merge enforcement

The repository response reported `allow_auto_merge: false`. More importantly, enabling auto-merge
would not itself enforce the review and status requirements that decide when a pull request is
eligible to merge. That eligibility requires a verified protection rule or active ruleset. This
version-controlled lane does not enforce either remote control and intentionally does not mutate
the setting.

### Actions allowlist

Actions were enabled with `allowed_actions: all`, while the current workflow uses only reviewed
full-commit pins and disables checkout credential persistence. The policy tests protect the checked
in workflow but do not restrict future remote workflows. A future settings pass should evaluate an
organization/owner allowlist only after confirming every required action and update workflow; it
must not weaken local operation or make a cloud service mandatory.

## Code of Conduct: intentionally omitted

The current official template is Contributor Covenant 3.0, stewarded by the Organization for
Ethical Source under CC BY-SA 4.0. Its official publication instructions explicitly require
adopters to replace the reporting placeholder and review the enforcement section. This audit could
verify the exact current text and attribution, but it could not verify an authorized, usable private
conduct-reporting channel. A vulnerability advisory is not silently repurposed for conduct reports,
and this lane will not publish the owner's private email. Shipping the template with a placeholder
or a nonfunctional channel would be misleading. Add the exact 3.0 text only after the owner selects
a private conduct-reporting channel and responsible moderators, then remove every `[NOTE: ...]`
placeholder and retain the official attribution and CC BY-SA notice.

## Primary sources

- [GitHub Actions workflow syntax: concurrency](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
- [GitHub issue forms and chooser configuration](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/configuring-issue-templates-for-your-repository)
- [GitHub issue-form syntax](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/syntax-for-issue-forms)
- [GitHub CODEOWNERS behavior and limitations](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
- [GitHub Dependabot options](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference)
- [GitHub repository rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets)
- [GitHub secret scanning](https://docs.github.com/en/code-security/concepts/secret-security/secret-scanning)
- [Contributor Covenant 3.0](https://www.contributor-covenant.org/version/3/0/)
- [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
- [Open Source Initiative MIT License](https://opensource.org/license/mit)
- [SPDX canonical MIT text](https://github.com/spdx/license-list-data/blob/main/text/MIT.txt)
