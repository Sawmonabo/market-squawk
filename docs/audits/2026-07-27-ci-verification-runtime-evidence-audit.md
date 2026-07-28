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
| Correctness follow-up candidate | `605362c495e6b139ccdbbdda85d86a69de96eb18` |
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

- the initial 21-source research inventory and subsequent primary-source correctness follow-ups;
- four source-category syntheses and four bounded batch reports;
- the repository workflow, verification scripts, Loom wrapper, and platform build script;
- GitHub Actions run and job metadata;
- retained Linux, macOS, and Windows job logs;
- a two-pass Cargo fingerprint reproduction;
- the Linux authority-lock, Windows analytical-backup, and manifest-allocation implementations;
- the unchanged Windows job rerun and file-adapter clock/deadline fixture;
- the Windows ONNX worker's resource profile and pinned Job Object dependency; and
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
- Exact candidate `05b406f` passed macOS. Its Windows job passed all four corrected backup cases,
  the allocator-sensitive derived-evidence case, and the file-adapter clock case before exposing a
  later catalog contention-classification defect.
- The Windows catalog remained exclusively owned. `fs2 0.4.3` returned raw
  `ERROR_LOCK_VIOLATION` 33, Rust 1.97.1 classified that general I/O error as `Uncategorized`, and
  the old `WouldBlock` comparison converted expected contention into public `UnsafePath`. Rust's
  stable `File::try_lock` API provides the exact typed contention boundary without changing the
  operating-system locking primitive.
- The Linux MCP failure is a production session-isolation defect: an individual session drained
  one process-global reaper and could therefore inherit an unrelated session's pending SDK thread
  or failure. Exact per-transfer join receipts preserve bounded reaper ownership while making each
  session wait only for its own worker.
- Exact candidate `f7c7712` passed the complete Linux and macOS jobs. Its Windows job passed the
  previously failing boundaries reached before modeling, then became the first retained Windows
  run to execute the modeling harness. It stopped at modeling before the later platform
  `configuration_security` harness and therefore did not prove those platform cases.
- The Windows ONNX worker configured `limit_working_memory(0, 3 GiB)`, which violates Microsoft's
  requirement that a nonzero maximum working set have a nonzero minimum. `win32job 2.0.3` applied
  the invalid profile before process assignment, so each helper exited before protocol
  initialization and the parent consistently mapped EOF to public `WarmUp`.
- A valid nonzero working-set minimum would not cap committed memory. The correction instead
  binds both per-process and job-wide 3 GiB committed-memory limits plus kill-on-close through an
  audited local patch of the exact licensed dependency. The patch source is now included in the
  Python release source closure, and runtime resource evidence advances to version 2.
- Exact candidate `605362c` passed its complete macOS job. Its Windows modeling harness passed all
  13 contracts, providing hosted evidence for the committed-memory correction, and then the later
  platform `configuration_security` harness exposed four previously unreached failures.
- Windows authority publication uses rename rather than Unix's hard-link installation proof.
  Open-time reserved temporary state cannot therefore be proven as legitimate on Windows and must
  be classified fail-closed as `UnsafeFileType`, not recoverable state.
- The pinned `atomicwrites 0.4.4` Windows replacement path uses `MoveFileExW` only. Rust 1.97.1's
  rename implementation adds a `FileRenameInfoEx` POSIX-semantics fallback on access denied, which
  is the supported replacement path when the destination remains open with delete sharing.
- The old non-Unix build-input identity fallback did not enforce a single hard link and could miss
  a same-length rewrite. Capability-handle metadata from `cap-fs-ext 4.0.2`, a second no-follow
  open, a second bounded hash, and retained root-directory identity make the Windows boundary
  content- and identity-based rather than timestamp-dependent.

The retained diagnostic log identities were:

| Evidence | SHA-256 |
| --- | --- |
| Successful Linux job `90180354327` | `1e26fb7eb46fd4d9d11cb607d9b2f61603dd7329c2a5618320f667df6976ce27` |
| Current Linux job `90189278958` | `4c3ee09f3143e425dd93371b7a83e1a4f494e13be177c142426345e3591463c4` |
| Current Windows job `90189278913` | `eab9f3c4fc4868bf0d1e5eba22ae4d9263a49980a199032a208652078e737bc7` |
| Current macOS job `90189278954` | `a20deae2bee0c04774b7e450bcaa67f71284f400f9887681d73c73d96237c1c9` |
| Windows rerun job `90209089614` | `387e6aea41dbe4606d92efb72b104134e62fcd77e962a97410027905c695a8b9` |
| Candidate `c7b045f` Windows job `90214783655` | `db7140c27c4a0fcfb1eeeeaece9e6b3de5b6d00dea2db7ce943b49a3559fde61` |
| Candidate `05b406f` Linux job `90227935404` | `e00fb737ab040a38697db0172cd9456cfd9e461b7f20ac845745419f18769d1a` |
| Candidate `05b406f` Windows job `90227935382` | `5528931b286af7a140be0f697e6961aacdcb011597534edf9fca2296bab5c2a2` |
| Candidate `05b406f` macOS job `90227935405` | `b7910509c67325af7e547d219eb480e906a72211bc9994ed196ffa23097ad8da` |
| Candidate `f7c7712` Linux job `90241570286` | `78bbb617c584f51567afc42ce62392f9b9822f38fd765cbec111f4a0839f3a57` |
| Candidate `f7c7712` Windows job `90241570389` | `9234b45044e1d2959cba1e54f4be507989631dcb952745127096d982a8317efb` |
| Candidate `f7c7712` macOS job `90241570407` | `e412fcdee3c80d849e25410f18bd41f4c1d65eec255080dbfc3250cccd52f403` |
| Candidate `605362c` Windows job `90260430186` | `33bf821efd984f0646afe8bde79fb23f34e3d1182bd2c046764ab48ec31886af` |
| Candidate `605362c` macOS job `90260430125` | `5c44cf3d3c4f4968a0ce3d7221c45261f902f2e0e2570c468f2074cc74c628e3` |
| Candidate `605362c` Linux job `90260430159` | `93e2345ba46d5d54a368e57b045179891ed6be5d6ee47d1449d38b63c63045cf` |
| Fingerprint pass 1 | `ed761532181b39a3ba187cca4e9d6702bfbb4593c2f82bfbf6ea58255dc5628f` |
| Fingerprint pass 2 | `8968e6a24f28e05a44544d972178b727d70ed7037048113422adeefc3b0ec062` |

The hashes identify the working evidence used during the audit. The large transient logs are not
tracked project artifacts; the report retains the durable GitHub run links and relevant excerpts.

## Source coverage

| Category | Listed sources | Audit result |
| --- | ---: | --- |
| Cargo, Rust, and toolchain documentation, including exact source | 23 | Covered |
| Operating-system, storage, and locked-dependency contracts | 20 | Covered |
| Official GitHub documentation and maintained CI tools | 9 | Covered |
| Academic papers | 4 | Covered; repository-transfer limits are explicit |

The report cites sources beside its material claims and distinguishes source-backed facts from
Market Squawk design inferences. It does not rely on academic evidence for a repository-specific
numerical forecast.

## Notes

- Cache size, restore time, save time, and warm-run benefit remain open measurements. Cache
  population must be a bounded experiment and retained only after demonstrating a net benefit.
- The complete discovery and batch reports remain temporary working papers because they repeat the
  reviewed report and are not required for day-to-day maintenance.
- The Linux lock defect, Windows URI boundary, Windows retained-backup lock conflict, Windows
  manifest-allocation dependency, file-adapter fixture race, Windows lock-contention
  classification, MCP cross-session reaper dependency, and Windows ONNX Job Object profile now
  have bounded causal explanations.
- The corrected backup and manifest designs use the existing failing tests as focused proof; no
  retry, sleep, serialization, fixture rewrite, new test target, or weakened evidence rule was
  introduced.
- Exact candidate `c7b045f` completed without cancellation: Linux and macOS passed, while Windows
  repeated five failures.
- Exact candidate `05b406f` completed without cancellation: macOS passed; Windows passed those five
  corrections and exposed catalog lock classification; Linux exposed MCP session/global-reaper
  coupling.
- Exact candidate `f7c7712` completed without cancellation: Linux and macOS passed; Windows passed
  the prior corrections and exposed the invalid ONNX Job Object profile in the first retained
  Windows execution of that harness. It did not reach the later platform configuration/security
  cases.
- Exact candidate `605362c` passed the complete Linux and macOS jobs. Windows passed all 13
  modeling contracts, then exposed four platform authority and build-input cases.
- The new Windows authority and build-input diagnosis is grounded in the exact locked dependency
  and Rust 1.97.1 sources plus current Microsoft filesystem contracts. The local correction has
  focused macOS and strict Clippy evidence but remains pending hosted Windows proof.
- The correctness fixes are not release evidence until one unchanged candidate passes Linux,
  macOS, and Windows.
- The audit verdict approves this report as decision input. It is not release approval and not
  post-change performance evidence.

## Conclusion

The report is fit to preserve as a date-anchored diagnostic and implementation-decision input. Its
two repository-specific runtime root causes are directly reproduced, its workflow-shape diagnosis
is supported by run evidence and official documentation, and its expected runtime is appropriately
qualified pending post-change measurement.
