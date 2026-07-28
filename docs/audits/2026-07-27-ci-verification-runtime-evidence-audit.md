# CI verification runtime research evidence audit

Purpose: record the evidence review of the CI runtime diagnosis and its subsequent correctness
follow-up in the maintained research documentation.

| Metadata | Value |
| --- | --- |
| Document type | Evidence audit |
| Audience | Maintainers, CI owners, release reviewers |
| Verdict | `PASS_WITH_NOTES` |
| Evidence cutoff | 2026-07-28 |
| Last substantive review | 2026-07-28 |
| Repository audit anchor | `75de7d43a74b0a1b7a5e9cd2f19e311a7ae2ed45` |
| Correctness follow-up candidate | `c7b045fcf09553b934d388a62ca9fe7e0ea36b82` |
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
- retained Linux, macOS, and Windows job logs;
- a two-pass Cargo fingerprint reproduction;
- the Linux authority-lock, Windows analytical-backup, and manifest-allocation implementations;
- the unchanged Windows job rerun and file-adapter clock/deadline fixture; and
- the relevant Linux, Windows, Rust, and SQLite platform contracts.

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
- The Linux authority failure follows Linux's documented open-file-description `flock` lifetime;
  the explicit-unlock guard corrects that lifecycle without accepting real contention. Exact
  candidate `c7b045f` subsequently passed the complete hosted Linux verify job in 58m54s.
- The former Windows URI builder incorrectly rejected canonical local `VerbatimDisk` paths. Prefix
  classification and SQLite's documented `/D:/...` URI form support that correction, but exact
  candidate `c7b045f` reproduced all four coarsened backup failures and proved the path-only causal
  conclusion incomplete.
- The remaining backup failure is a deterministic Windows `LockFileEx` self-conflict: retained
  verification exclusively byte-locked the database and then read the same range through a second
  handle. Microsoft's contract directly describes that denied access. An existing, noncreating
  private writer-sidecar lease preserves exclusivity without blocking receipt and SQLite reads.
- The unchanged Windows rerun failed earlier in the file-adapter harness because a one-second
  synthetic fixture deadline competed with its intended injected clock failure. Source, runner
  image, toolchain, and test executable were unchanged, and the production deadline result was
  correct.
- The fifth Windows failure repeated at `c7b045f`. Rust 1.97.1's `Result<Vec<_>, _>` collection
  produced spare capacity; `into_boxed_slice` then shrank it through Windows `HeapReAlloc`, which
  may move the block. The manifest's pointer-identity guard consequently rejected valid evidence.
  Exact-capacity normalization at the central manifest boundary removes that allocator dependency
  without weakening evidence semantics.

The retained diagnostic log identities were:

| Evidence | SHA-256 |
| --- | --- |
| Successful Linux job `90180354327` | `1e26fb7eb46fd4d9d11cb607d9b2f61603dd7329c2a5618320f667df6976ce27` |
| Current Linux job `90189278958` | `4c3ee09f3143e425dd93371b7a83e1a4f494e13be177c142426345e3591463c4` |
| Current Windows job `90189278913` | `eab9f3c4fc4868bf0d1e5eba22ae4d9263a49980a199032a208652078e737bc7` |
| Current macOS job `90189278954` | `a20deae2bee0c04774b7e450bcaa67f71284f400f9887681d73c73d96237c1c9` |
| Windows rerun job `90209089614` | `387e6aea41dbe4606d92efb72b104134e62fcd77e962a97410027905c695a8b9` |
| Candidate `c7b045f` Windows job `90214783655` | `db7140c27c4a0fcfb1eeeeaece9e6b3de5b6d00dea2db7ce943b49a3559fde61` |
| Fingerprint pass 1 | `ed761532181b39a3ba187cca4e9d6702bfbb4593c2f82bfbf6ea58255dc5628f` |
| Fingerprint pass 2 | `8968e6a24f28e05a44544d972178b727d70ed7037048113422adeefc3b0ec062` |

The hashes identify the working evidence used during the audit. The large transient logs are not
tracked project artifacts; the report retains the durable GitHub run links and relevant excerpts.

## Source coverage

| Category | Sources | Audit result |
| --- | ---: | --- |
| GitHub repositories | 6 | Covered; maintained primary project repositories |
| Official documentation and exact toolchain source | 18 | Seven runtime sources plus current platform-contract sources |
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
- The Linux lock defect, Windows URI boundary, Windows retained-backup lock conflict, Windows
  manifest-allocation dependency, and file-adapter fixture race now have bounded causal
  explanations and focused local evidence.
- The corrected backup and manifest designs use the existing failing tests as focused proof; no
  retry, sleep, serialization, fixture rewrite, new test target, or weakened evidence rule was
  introduced.
- Exact candidate `c7b045f` completed without cancellation: Linux and macOS passed, while Windows
  repeated the five failures addressed by the next candidate.
- The correctness fixes are not release evidence until one unchanged candidate passes Linux,
  macOS, and Windows.
- The audit verdict approves this report as decision input. It is not release approval and not
  post-change performance evidence.

## Conclusion

The report is fit to preserve as a date-anchored diagnostic and implementation-decision input. Its
two repository-specific runtime root causes are directly reproduced, its workflow-shape diagnosis
is supported by run evidence and official documentation, and its expected runtime is appropriately
qualified pending post-change measurement.
