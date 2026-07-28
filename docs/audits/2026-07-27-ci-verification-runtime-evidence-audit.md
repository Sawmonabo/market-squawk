# CI verification runtime research evidence audit

Purpose: record the terminal evidence review of the CI runtime diagnosis before it entered the
maintained research documentation.

| Metadata | Value |
| --- | --- |
| Document type | Evidence audit |
| Audience | Maintainers, CI owners, release reviewers |
| Verdict | `PASS_WITH_NOTES` |
| Evidence cutoff | 2026-07-27 |
| Last substantive review | 2026-07-27 |
| Repository audit anchor | `75de7d43a74b0a1b7a5e9cd2f19e311a7ae2ed45` |
| Audited report | [CI verification runtime diagnosis](../research/2026-07-27-ci-verification-runtime.md) |

## Table of Contents

- [Audit scope](#audit-scope)
- [Findings](#findings)
- [Verified claims](#verified-claims)
- [Source coverage](#source-coverage)
- [Notes](#notes)
- [Conclusion](#conclusion)

## Audit scope

The audit compared the research report with:

- the 21-source research inventory;
- four source-category syntheses and four bounded batch reports;
- the repository workflow, verification scripts, Loom wrapper, and platform build script;
- GitHub Actions run and job metadata;
- retained Linux, macOS, and Windows job logs; and
- a two-pass Cargo fingerprint reproduction.

## Findings

| Severity | Finding |
| --- | --- |
| Critical | None |
| Important | None |
| Minor | None requiring correction before use as decision input |

The report does not recommend removing a required verification surface, weakening the release
profile, adopting a paid runner, treating a cache as approval evidence, or claiming an unmeasured
speedup.

## Verified claims

- The successful Linux verification job in
  [run 30329093586](https://github.com/Sawmonabo/market-squawk/actions/runs/30329093586/job/90180354327)
  took approximately 60 minutes and its phase totals support the report's breakdown.
- The current workflow's `save-if` expression prevents pull-request cache writes, the retained job
  records `save-if: false` and `No cache found`, and the repository cache inventory was empty at the
  audit time.
- The platform build script passes Git-relative metadata paths to `cargo:rerun-if-changed`; the
  second identical fingerprint diagnostic recorded the resulting missing package-relative
  `.git/HEAD` path and rebuilt the package.
- The exact Loom wrapper retains an inventory comparison but invokes Cargo once per declared model.
  The successful job log shows repeated platform builds during those invocations.
- The most recent 20 release-branch runs at the audit time consisted of 13 failures and 7
  cancellations with 8.236 hours in summed run duration.
- The proposed 28–32 minute cold wall time is explicitly labeled a planning projection. It is not
  presented as measured evidence.

The retained diagnostic log identities were:

| Evidence | SHA-256 |
| --- | --- |
| Successful Linux job `90180354327` | `1e26fb7eb46fd4d9d11cb607d9b2f61603dd7329c2a5618320f667df6976ce27` |
| Current Linux job `90189278958` | `4c3ee09f3143e425dd93371b7a83e1a4f494e13be177c142426345e3591463c4` |
| Current Windows job `90189278913` | `eab9f3c4fc4868bf0d1e5eba22ae4d9263a49980a199032a208652078e737bc7` |
| Current macOS job `90189278954` | `a20deae2bee0c04774b7e450bcaa67f71284f400f9887681d73c73d96237c1c9` |
| Fingerprint pass 1 | `ed761532181b39a3ba187cca4e9d6702bfbb4593c2f82bfbf6ea58255dc5628f` |
| Fingerprint pass 2 | `8968e6a24f28e05a44544d972178b727d70ed7037048113422adeefc3b0ec062` |

The hashes identify the working evidence used during the audit. The large transient logs are not
tracked project artifacts; the report retains the durable GitHub run links and relevant excerpts.

## Source coverage

| Category | Sources | Audit result |
| --- | ---: | --- |
| GitHub repositories | 6 | Covered; maintained primary project repositories |
| Official documentation | 7 | Covered; Cargo and GitHub first-party documentation |
| Academic papers | 4 | Covered; repository-transfer limits are explicit |
| Reputable sources | 4 | Covered; first-party documentation or direct Rust expertise |

The report cites sources beside its material claims and distinguishes source-backed facts from
Market Squawk design inferences. It does not rely on academic evidence for a repository-specific
numerical forecast.

## Notes

- Cache size, restore time, save time, and warm-run benefit remain open measurements. Cache
  population must be a bounded experiment and retained only after demonstrating a net benefit.
- The complete discovery and batch reports remain temporary working papers because they repeat the
  reviewed report and are not required for day-to-day maintenance.
- The current Linux and Windows test failures are correctness findings at the candidate commit.
  They are separate from the verified runtime diagnosis and must not be masked by the performance
  correction.
- The audit verdict approves this report as decision input. It is not release approval and not
  post-change performance evidence.

## Conclusion

The report is fit to preserve as a date-anchored diagnostic and implementation-decision input. Its
two repository-specific root causes are directly reproduced, its workflow-shape diagnosis is
supported by run evidence and official documentation, and its expected runtime is appropriately
qualified pending post-change measurement.
