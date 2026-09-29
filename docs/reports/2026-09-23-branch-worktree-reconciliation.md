# Branch and worktree reconciliation audit — 2026-09-23

## Final branch retirement after the integrated V1 checkpoint

The combined source API and consumers are committed and pushed together at
`ff16c370c90d930979e3b69b4f641f19ede43302`. The remaining historical branches
were compared by required behavior, not treated as patch-equivalent. Useful
behavior is incorporated in the current implementation; replaced public
attestation, synchronous publication, setup-website and older proposal paths
were explicitly retired rather than restored by wholesale cherry-picking.

The lead independently verified all 26 remaining stale refs against the
complete September 7 recovery bundle, its SHA-256
`0b8cba3838cb3c0c7bc4aa0fe63212f1a57264f406ef0b780e15ee30ce020ee6`,
current committed replacement hashes, live origin heads, sole worktree and
open PRs. Both divergent Alpaca tips were checked separately. The 11 origin
heads were deleted atomically with exact expected-head leases, followed by
the 15 exact-checked local labels and remote-tracking pruning.

The removed local branches were Census's pre-facade checkpoint; Codex's
Alpaca shutdown, common-seal integration and Schwab handoff; and the feature
branches for Alpaca identity, BEA, BLS, Census, Coinbase identity, current-market
attestation, IEX HIST, Kraken handoff, Kraken identity, opportunities and Schwab.
All matching live origin heads were removed. The five incorporated Dependabot
updates and their closed PRs are recorded below; no bot branch remains.

Verified result: **3 local branches, 3 live origin branches, 1 primary
worktree and zero linked worktrees**. Only `main`,
`release/market-squawk-v0.1.0` and `feature/v1-installed-product-experience`
remain. Main and release were unchanged; no merge or public release occurred.
The original session and all independent recovery backups remain intact.
The dated counts below are historical. Repository reconciliation is complete;
complete installed V1 product acceptance remains open.

## September 29 subsequent worktree retirement

After the dependency checkpoint below, two obsolete archival branch labels,
`codex/fred-shared-integration` (`a882d169`) and `codex/sec-product-handoff`
(`7efda6a2`), were retired after comparison with current V1 behavior. Their exact
histories are retained in an independently verified bundle with SHA-256
`000df9908ad7aa9508928675bc59c3af7e75de0e56f78b36d916a0ff391012f6`.
Required source behavior remains subject to current-product verification;
obsolete setup-website and superseded shared implementations were not replayed.

The Alpaca native-identity and Kraken native-identity worktrees were then
removed without force. Before restoring their archived tracked paths and
removing archived untracked files, the lead compared live heads, status,
index, source bytes and backup bytes. Kraken's seven ignored proof files were
preserved independently as well. Neither retirement deletes the corresponding
local or remote branch; replacement integration is still pending.

Census and Schwab were subsequently retired after root independently verified
all three Census and twenty-three Schwab changed files against their independent
archives, exact status/index/patches, advertised bundle heads and no active
handles. Schwab's two ignored proof files were independently preserved and
verified. Normal worktree removal followed restoration of only those enumerated
archived tracked paths. Both branch references remain.

One required Census donor behavior was retained in the active implementation:
valid unrelated global catalog entries without a year are skipped without
inventing a vintage. The complete catalog evidence remains retained; malformed
distributions and mismatched time-series coordinates are rejected. All seven
Census library tests passed, including this focused regression.

The last linked worktree, common-seal, was subsequently removed normally.
Root independently compared all 255 working/deletion states with the original
September 7 archive, reconstructed all fourteen staged versions using the
archived index patch and donor HEAD, and verified exact Git state, complete
history bundle and absence of active handles. The old synchronous Coinbase
publication actor is superseded by the current asynchronous successor and
committed-predecessor validation. Only explicitly archived paths were restored
or unlinked; no forced removal or blanket cleaning occurred.

Inventory after these actions: **18 local branches, 14 origin heads, one local
worktree: V1**. Three local and two remote Codex branches remain. All linked
worktrees are gone; branch references remain a separate disposition barrier.
No main/release merge, public publication, old-session deletion or recovery-backup
deletion occurred.

## September 29 dependency and branch closure checkpoint

Dependency updates are consolidated on the V1 feature branch in `a7987440`:
futures-util 0.3.34, async-trait 0.1.92, rust_decimal 1.43.0, clap 4.6.7
and uuid 1.26.1. Registry checksums match the five proposed bot updates.
The isolated dependency candidate passed locked offline Cargo metadata;
30 existing financial-value tests passed against the integration workspace.
No full release gate or CI/CD ran. PRs #46, #48, #52, #53 and #54 were closed
with incorporation comments; live origin inventory confirms all five bot
branches are absent. Neither main nor release was merged.

The worktree-free `codex/crypto-canonical-data` branch at `3facc2b2` was
also deleted with ordinary `git branch -d` after fresh ancestor verification
against the pushed target. Its history remains in the target. It had no
remote branch or open PR. No donor worktree was removed in this checkpoint.

Three redundant local labels were retired after exact-head and ancestry checks:
`feature/board-h15-native-publication` and `feature/treasury-sealed-publication`
both at `0e4ca488`, and `feature/provider-native-lineage-sidecar` at `b0ce2d47`.
All commits remain reachable through retained `codex/common-seal-root-integration`
at `988c8547`; none had a separate worktree, live remote branch or open PR.
This is label consolidation, not acceptance of the common-seal candidate.

The `codex/sealed-binding-catalog` label at `9a0170e2` was retired after
independent comparison: its first two outside-target commits are patch-equivalent,
and all 14 blobs in its remaining commit exactly match the retained common-seal
index. The verified complete-history September 7 bundle also contains that exact
branch tip. No worktree, remote branch or open PR belonged to this label. The
common-seal index and backup remain intact; their product acceptance is still open.

Fresh inventory: **20 local branches, 14 origin heads, six local worktrees
including the target**. Five local and two remote Codex branches remain.
The five donor worktrees are Alpaca, Census, Kraken, Schwab and common-seal
integration. Unique commits and dirty layers still require disposition;
this checkpoint does not claim that all source work is integrated.

The source API and updated callers already coexist as pending changes in
the target checkout. They must be committed as a coherent dependency set;
there is no separate caller commit to cherry-pick. Existing useful lane
commits can be cherry-picked when their prerequisites are present. Replaced
or rejected changes are preserved and explicitly retired rather than replayed.
Original session and recovery backups remain protected. PR #43 records the
pushed dependency checkpoint and focused verification limits.

## September 28 closure checkpoint

The obsolete `source-current-integration` checkout has been retired. Before
cleanup, the lead compared all 57 live source files with the independent
September 23 backup, and the full-index staged/unstaged patches and status with
the preserved packet. The September 7 complete-history bundle verified; its
SHA-256 remains `0b8cba3838cb3c0c7bc4aa0fe63212f1a57264f406ef0b780e15ee30ce020ee6`.
Neither independent backup nor the original session was removed.

The current V1 registry selects native identity through the catalog, checks the
independent native coordinates, and assigns original row ordinals before route
grouping. The donor's public-record attestation and precommit publication-context
variants are superseded. Required external budget callers now use the public
reservation/dispatch API through one existing test-support module; assertions
are preserved. Current observation fixtures still need catalog-selected setup;
syntax inspection is not a passing Rust or installed journey result.

After preserving and dispositioning the dirty layers, the lead restored only
the 57 archived donor paths, confirmed a clean checkout, removed it without
force and pruned metadata. Its integrated branch `codex/source-current-integration`
at `8c7ee0b0` was deleted normally. The separate rejected
`codex/source-current-publication` branch at `27c57146` duplicates the donor's
ten staged blobs and was explicitly retired; its full history remains in the
verified backup bundle. Neither branch exists on origin or has an open PR.

Fresh counts after cleanup: **25 local branches, 19 origin heads, six worktrees
including the target**. Remaining worktrees are Alpaca, Census, Kraken, Schwab,
and common-seal integration. Product acceptance and the clean integrated V1
checkpoint remain open; worktree retirement is not provider completion.


Audit target: pushed `feature/v1-installed-product-experience` at
`18f11e072fd451b242788f121d86becdeaccbc91`. Live origin heads were read with
`git ls-remote --heads origin`; no branch or worktree was removed in this audit.
This is a custody and scheduling record, not product or deletion approval. Refresh
target ancestry, worktree status, PR state and live origin heads before any removal.

## Counts and immediate decision

- Local: 29 branches, including protected `main`, `release` and the target; 26 side branches.
- Origin: 20 heads, including the three protected/target heads; 17 side heads.
- Linked worktrees: 12, including the target; all 12 dirty. Worktree branches are
  included in the local-branch count. None qualifies for immediate removal.
- Only three side heads are exact ancestors of the target, and all three have dirty
  linked worktrees. No side branch without a worktree is proven safe to retire as
  integrated. No live origin side head is an exact ancestor of the target.
- Five open Dependabot PRs (#46, #48, #52, #53, #54) are lockfile-only updates
  against `main`; review separately, rather than treating them as feature merges.
  PRs #43 and #26 remain delivery/release handoffs. The ledger's September 16
  four-open-PR count is stale.

## Branch disposition at this audit base

| Group | Local heads | Current disposition |
| --- | --- | --- |
| Exact commit already in target | `codex/crypto-canonical-data` `3facc2b2`; `codex/source-current-integration` `8c7ee0b0`; `feature/coinbase-provider-identity-selection` `b04c2674` | Commit ancestry is proven. Inspect/preserve their dirty worktrees before retirement. |
| Duplicate or subset of unaccepted successor | `feature/board-h15-native-publication` `0e4ca488`; `feature/treasury-sealed-publication` `0e4ca488`; `feature/provider-native-lineage-sidecar` `b0ce2d47` | First two are the same head; all are ancestors of dirty `codex/common-seal-root-integration`, not of the target. Resolve that successor before retiring labels. |
| Mixed/uncertain historical code | `checkpoint/census-durable-pre-facade` `0ac1ebff`; `codex/fred-shared-integration` `a882d169`; `codex/sec-product-handoff` `7efda6a2` | Review individual behavior against current V1. FRED branch includes removed setup-website code and cannot be wholesale merged. |
| Provider and authority work with unique commits | `codex/alpaca-history-shutdown` `90da2307`; `codex/common-seal-root-integration` `988c8547`; `codex/schwab-product-handoff` `fea9d7ae`; `codex/sealed-binding-catalog` `9a0170e2`; `codex/source-current-publication` `27c57146`; `feature/alpaca-native-identity` `049faf72`; `feature/bea-durable-macro` `d6ea34cf`; `feature/bls-product-vertical` `4a6e8364`; `feature/census-durable-macro` `2ad15f6c`; `feature/coinbase-native-identity` `12a6b5d7`; `feature/current-market-native-attestation` `75285e03`; `feature/eia-durable-macro` `9f4219f6`; `feature/iex-hist-durable-v1` `cfd28f7f`; `feature/kraken-market-handoff` `7d88d0db`; `feature/kraken-native-identity` `a0cdc33c`; `feature/opportunity-product-v1` `4299d786`; `feature/schwab-product-vertical` `c3b257af` | Unique commits warrant semantic review, not automatic adoption. Selectively integrate current-V1 behavior through the target; explicitly reject replaced or obsolete behavior with evidence. |

The local and origin heads diverge for `codex/alpaca-history-shutdown`
(`90da2307` local, `8e80d64b` origin) and `feature/eia-durable-macro`
(`9f4219f6` local, `2727b7a3` origin). Preserve and review each remote-only
commit separately before any remote branch action. Five other origin-only side
heads belong to the open Dependabot PRs. Ten origin side heads match local heads.

## Dirty worktree custody

Counts are tracked changes / untracked files / staged changes after the target
advanced to `18f11e07`; byte differences are not proof that old behavior is wanted.

| Worktree | Counts | Current custody |
| --- | ---: | --- |
| Target checkout | 588 / 326 / 0 | Lead owns shared integration. |
| `alpaca-native-identity` | 9 / 3 / 0 | Owner unconfirmed; reconcile adapter and branch commits. |
| `census-durable-macro` | 3 / 0 / 0 | Active off-tree reconciliation; retained until decoder, consumer, restart and remaining branch-only differences are dispositioned. |
| `coinbase-native-identity` | 11 / 2 / 0 | Owner unconfirmed; reconcile adapter and branch commits. |
| `common-seal-root-integration` | 217 / 38 / 14 | Owner unconfirmed; shared authority, manifests and index state. Serialize last. |
| `crypto-canonical-data` | 22 / 2 / 0 | Commit integrated; dirty canonical-data WIP still requires review. |
| `current-market-native-attestation` | 44 / 0 / 0 | Owner unconfirmed; review after provider-native identity inputs. |
| `eia-durable-macro` | 6 / 1 / 0 | Reviewed against current root: production changes are already present or regress later fixes. Unique ignored test and divergent remote commit still need disposition; no removal yet. |
| `kraken-native-identity` | 6 / 0 / 0 | Owner unconfirmed; reconcile adapter and application caller. |
| `opportunity-product-v1` | 19 / 1 / 0 | Owner unconfirmed; reconcile after financial/input authority. |
| `schwab-product-vertical` | 23 / 0 / 0 | Owner unconfirmed; reconcile provider runtime and missing daily-history caller. |
| `source-current-integration` | 57 / 0 / 10 | Commit integrated; dirty shared market/application/index state needs serial review. |

## Integration and retirement order

1. Stop branch creation for reconciliation; work against the existing target
   and use bounded off-tree packets with exact preimages. Keep one integration
   owner for shared application composition, authority, manifests and lockfiles.
2. Record branch commits plus tracked, staged and untracked WIP before deciding
   behavior. Reconcile smaller provider lanes (Census, EIA, Alpaca, Coinbase,
   Kraken, BLS, IEX HIST, BEA) against the selected-source and neutral-consumer
   contract; reject stale code explicitly. Preserve divergent origin commits.
3. Reconcile canonical data and current-market attestation before Opportunity,
   Schwab and shared source/product composition. Handle the two staged shared
   worktrees serially, after their dependencies settle.
4. Review five Dependabot PRs against actual need and locked dependency impact;
   accept or close each with a reason. Keep `main` and `release` untouched.
5. For each lane: inspect changed consumers; verify the accepted behavior with
   focused critical checks; commit and push the target checkpoint; record the
   candidate and disposition; prove the lane worktree has no unique WIP or active
   user, make it clean without discarding data, remove it without force and prune
   metadata; only then retire local/remote side refs with freshly verified heads
   and close the corresponding PR/issue where its stated outcome is delivered.

This audit removed **zero** worktrees, branches or PRs. Installed live restart,
whole-app resource measurement and complete V1 workflows remain separate
acceptance barriers.

The EIA per-file and per-commit review, including hashed WIP and remote-only
patches, is retained outside both worktrees at
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-23-eia-wip-reconciliation/`.
Its manifest SHA-256 at review was
`e11286d84eb58512b74cec197cfda7bd6980602ed809460205bfc7d5ee80830b`.
That preservation is not permission to discard the still-dirty source worktree.

## Post-audit EIA disposition and retirement

The lead independently compared the remote-only `2727b7a3` to local `9cfe443d`:
their EIA tree differs only by a borrow-scope correction in the old ignored live
test. Local `9f4219f6` diagnostics and useful production behavior are already
present in the target; the dirty source edits would regress current bounded
reopen, coordinates and diagnostics. The ignored test uses obsolete APIs; its
fresh live journey remains a V1 verification requirement, not code to replay.
There was no open PR or runtime handle for the EIA worktree.

Before cleanup, the live tracked diff and untracked activation file matched the
independent backup byte-for-byte. Its seven artifacts and manifest checksum
verified again after cleanup. The lead restored only the backed-up obsolete EIA
working files, removed that backed-up untracked source copy, confirmed a clean
worktree, removed it without force, and pruned worktree metadata. The remote
branch was deleted with an exact-head lease on `2727b7a3`, then local branch
`9f4219f6` was deleted. Live origin and filesystem checks confirm both branch
and worktree are absent. No target product file or backup was deleted.

The resulting live inventory is **28 local branches, 19 origin heads and 11
linked worktrees**. EIA's full live-to-installed product journey remains open.

## Post-audit Coinbase disposition and retirement

The directory named `coinbase-native-identity` belonged to branch
`feature/coinbase-provider-identity-selection` at `b04c2674`, an ancestor of
the pushed target `fb1fd319`. This is distinct from the still-live local and
origin branch `feature/coinbase-native-identity` at `12a6b5d7`, whose unique
commits remain pending separate semantic reconciliation. The retired worktree's
11 modified tracked files and two untracked files implemented adapter-owned
copies of provider identity and synthetic reference
fixtures. Current V1 selects an opaque identity in the instrument catalog,
retains it with source and capture authority in the registry, and revalidates
that selection through application publication. Coinbase public profile and
decoder binding to that selection remain open. Replaying the old constructor
would break current application callers and duplicate that authority. The
per-file disposition and exact WIP copy are preserved in
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-23-coinbase-wip-reconciliation/`.
The backup manifest SHA-256 is
`2096478cdfad086316120797cda67f614011b8812896b877bd7b34aed06369db`;
all five listed artifact checksums passed before cleanup and again afterward,
and the 13 recorded source hashes matched the live worktree before cleanup.

No open PR, origin head or runtime handle used this branch or worktree. After
the source diff and untracked hashes matched the backup, the lead reconciled
only those 13 obsolete working files, confirmed a clean source worktree,
removed it without force, pruned metadata and deleted its already-integrated
local branch with `git branch -d`. The resulting live inventory is **27 local
branches, 19 origin heads and 10 linked worktrees**. The target, independent
backup and other worktrees remain intact. Coinbase's complete live-to-installed
product journey remains a separate acceptance barrier.

## Retained work after the cleanup wave

The `feature/coinbase-native-identity` branch at `12a6b5d7` is a different,
worktree-free branch from the retired Coinbase worktree above. Its ten unique
commits use an older native-attestation design; current provider-neutral
startup and publication wiring is still uncommitted. It has no open PR, but
retirement awaits accepted replacement or independent commit custody.

The independent backups in this section are under
`/Users/sawmonabo/dev/market-squawk-handoff-backups/`. The
`alpaca-native-identity` worktree at `049faf72` has nine tracked and three
untracked files plus six unique commits. Its current-design successors exist
only in the dirty target, so it remains linked. All source files, commits and a
Git bundle are preserved in the independent
`2026-09-23-alpaca-wip-reconciliation` backup (manifest SHA-256
`79e199e1b187e8c8358f0dc61213f9e7371ca3ca3797ac95e442f5ab00f501c1`).

The `source-current-integration` head `8c7ee0b0` is an ancestor of the target,
but its worktree has ten staged and 47 unstaged files. Its static attestation
code cannot be adopted as written: its public crypto composition uses an empty
identity registry and old native namespaces. Fourteen files still express
required Coinbase/Kraken intent: the target selects catalog identity, but
constructs live profiles and decoder coordinates from static configuration
before consuming that selection. The corrected 57-file custody backup is
`2026-09-23-source-current-wip-reconciliation` (manifest SHA-256
`c1a14e0b5adf8911b3895e15c43de641b1181995755e39df5e2ec0df1a5dd089`);
its 70 checksums and live staged/unstaged patch hashes verified. This
worktree remains until the current provider bindings are integrated.

The `kraken-native-identity` worktree at `a0cdc33c` has five unique commits and
six tracked edits with still-needed catalog-bound profile and worker-draining
behavior. The `crypto-canonical-data` worktree at `3facc2b2` retains an
unresolved transitive source-run availability proof for derived data. Neither
was removed; old code will be selected only where it fits the current V1
contracts. These retained states are product/integration work, not accepted
provider completion.

The September 24 review of `crypto-canonical-data` confirmed that its 22 tracked
and two untracked files still match the independent September 7 backup. Its
branch head is already an ancestor of the target, and its old market-event and
raw-recovery stack is superseded. The lane still contains a missing transitive
source-run availability proof for derived generations and bounds for legacy
order-book vectors. Its Parquet retention accounting exposed a current memory
gap, now being reconciled in the active implementation. The worktree remains
linked until those current-design behaviors and their consumers are verified;
the old schema and unfinished event API will not be merged wholesale.

## September 24 current-market attestation disposition

The `feature/current-market-native-attestation` worktree at `75285e03` contains
four branch-only commits and 44 modified files that adapt provider, application,
platform, test and fuzz callers to the older `ProviderNativeInstrumentAttestation`
type. Its embedded Coinbase/Kraken identity records are obsolete under the
current catalog-selected native identity design. The current target carries
the intended native identity through provider publication using catalog-owned
selection and v2 lineage; the old branch adds no distinct restart or recovery
behavior. Installed live restart of the current path remains an open product
acceptance gate, not evidence supplied by this branch.

The independent September 7 backup records the exact branch head and all 44
modified files. On September 24 the lead rechecked the head, the 44 live file
hashes and status entries (zero staged or untracked), all five relevant backup
checksums, and the verifying Git bundle containing this branch. No runtime
process or agent still used the worktree. Its old behavior is explicitly
dispositioned as superseded; its source and commits remain recoverable from
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-07/`. The local
branch remains until the current replacement is accepted and its branch
retirement is recorded.

After the exact backup comparison, the lead reconciled only those 44 obsolete
working files, verified a clean source status, removed the idle worktree
without force and pruned its metadata. The live inventory is now **27 local
branches, 19 origin heads and 9 linked worktrees**. The target checkout, other
worktrees and independent backup were not removed.

## September 24 opportunity candidate disposition

The `feature/opportunity-product-v1` head `4299d786` has two unique commits and
19 tracked edits plus one untracked macro-assumptions file. Its older proposal,
valuation and presentation implementation is superseded by the current V1
product design; wholesale merge would regress the current forecast, historical
study and portfolio contracts. The two still-useful Desktop behaviors were
carried into the target: saved-analysis dates now use the product timestamp
formatter, and the current analysis decoder checks portfolio/evidence,
action, price-projection and position-scale consistency. The Desktop typecheck
passed after those changes. This is focused integration evidence, not installed
or exact-head product approval.

An independent copy of both commits, a complete Git bundle, exact tracked and
untracked source bytes, staged/unstaged patches and status is in
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-23-opportunity-wip-reconciliation/`.
Its manifest SHA-256 is
`eb4e4ca42d951c37e2a9d24a4be1ebceaed015f08d30cc745c74c4fdb4d89f55`.
The lead verified all 30 listed artifact checksums, the complete-history bundle,
all 20 live source file hashes and byte copies, and the full-index working diff
and status against the archive before retirement. The branch remains until the
accepted target contains these consumer fixes and its separate ref disposition
is recorded.

After exact comparison, the lead reconciled only the 19 archived tracked files
and the archived untracked file, confirmed the source status was clean, removed
the idle worktree without force, and pruned metadata. All 30 backup checksums
passed again afterward. The linked-worktree count is now **8**, including the
target; the branch remains at `4299d786` with its complete-history backup.

## September 24 remaining worktrees and dependency queue

A fresh read-only audit found all seven remaining side worktrees dirty:
`alpaca-native-identity`, `census-durable-macro`, `common-seal-root-integration`,
`crypto-canonical-data`, `kraken-native-identity`, `schwab-product-vertical`, and
`source-current-integration`. Alpaca, Census, Kraken, Schwab and common-seal
have branch-only commits; crypto and source-current have target-ancestor heads
but still retain unique uncommitted state. None qualifies for removal.
Alpaca has nine modified and three untracked paths; Census has three modified;
Kraken has six modified; Schwab has 23 modified; common-seal has 255 changed
paths including 14 staged and 38 untracked; crypto has 22 modified and two
untracked; source-current has 57 modified including ten staged. Refresh these
counts before acting. No `codex/` side ref was deleted merely for its age.

Schwab's September 7 backup covered six dirty paths, while its live worktree
now has 23. A new independent backup at
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-24-schwab-wip-reconciliation/`
preserves all 23 current modified files, full index and patch state, and a
complete-history bundle at `c3b257af`. Its manifest SHA-256 is
`ff8c9894df6dba5e0b2c2de6cd60a8a0765583d8808322480b51092d4393870c`.
The lead independently verified every archived file hash against the live
source, all archive artifact hashes and the bundle. Schwab remains linked
because its provider behavior is not yet accepted in the target.

The five open Dependabot PRs (#46, #48, #52, #53, #54) still target `main`.
Their requested `futures-util`, `async-trait`, `rust_decimal`, `clap`, and
`uuid` versions are now in the local V1 lockfile; the decimal pin was also
updated in the workspace manifest. A focused `market-squawk --lib` compile
and nine existing financial exactness checks passed after the combined update.
Dependency policy then exposed an affected `rustls` 0.23.42 and yanked
`chacha20` 0.10.1; the local lock now selects 0.23.45 and 0.10.2 respectively.
`cargo deny check` passes on the revised graph. The later security-update
compile passed; the encrypted-store restart proof remains a separate gate. This is
uncommitted, non-final integration evidence. Keep the PRs and remote bot branches until the V1 checkpoint is
accepted and pushed and their `main` disposition is explicit. No merge into
`main` or `release` was made.

The September 24 combined V1 application compile also passed after integrating
the Coinbase Direct two-product coordinator, Census reobservation comparator,
H.15 selected full-history replay and neutral macro consumer, and the
source-bound current-share decision core. The existing critical Census
reobservation test, H.15 selected-partition replay test, and Coinbase Direct
two-product coordinator/restart test each passed. These
are focused source checks, not installed live journeys or accepted release
evidence. Current-share generation, typed saved replay, Desktop chart overlays,
and provider restart remain open; an unfinished candidate cannot serve as a
branch-retirement proof. The seven remaining side worktrees and all five
Dependabot PRs remain in place.

## September 24 crypto canonical worktree disposition

The old `codex/crypto-canonical-data` worktree head `3facc2b2` is an ancestor
of the V1 target. Its 22 tracked edits and two untracked files implement an
older raw/event schema and catalog path. Current V1 source has the necessary
transitive source-run closure, availability-gated point-in-time selectors,
bounded Parquet retention accounting and 4,096-entry order-book bounds. The
older fixture and schema paths are obsolete under the current native lineage
design; no old patch is being merged wholesale. Current-root presence and
focused source tests do not establish installed restart acceptance.

The independent September 7 archive records this exact head, all 24 real
source files, status and full-index patches, plus a complete-history Git
bundle. The lead independently compared each archived real file byte-for-byte
with the live worktree; all 24 matched, as did the status, index and working
patches. The bundle verified and contains `codex/crypto-canonical-data` at
`3facc2b2`. Twelve AppleDouble `._` tar metadata entries are not source files
and were excluded from the byte comparison. No persistent process or active
agent owns this worktree. This is an explicit preserved handoff and obsolete
source disposition under the worktree lifecycle rule; the branch and backup
remain for the later accepted-integration branch decision.

After a final exact-status comparison, the lead restored only the 22 archived
tracked paths and removed only the two archived untracked paths in that old
worktree. It then verified a clean status, removed the idle worktree without
force and pruned worktree metadata. The independent archive checksum remains
`72b1d33be08c2f01616d93ae25ce958ec02184b1160b25a322a2ff8be710d2a6`;
the local `codex/crypto-canonical-data` branch still points to `3facc2b2`.
The live inventory is now **27 local branches, 19 origin heads and 7 linked
worktrees**. No other source or ref was removed.

## September 24 source-current and Schwab follow-up

The earlier 14-file source-current gap assessment above is superseded for code
presence. The current V1 target selects catalog identity before public-source
profile installation and carries that selection through Coinbase and Kraken
decoders and publication checks. The old worktree's static registry and empty
identity tables are obsolete. Both the September 7 and September 23 independent
archives match all 57 live files, status, index listing and full-index patches;
the September 7 complete-history bundle verifies. No process held an open
handle at the audit. The worktree remains linked pending a focused installed
selected-identity, publication, typed-read and restart check for the current
path. The branch remains separately pending an accepted target checkpoint.

The Schwab daily-history mapper had asserted raw adjustment and period-start
timestamps without an accessible official price-history contract. The target
now retains sealed raw response evidence but denies canonical daily bars until
those semantics can be verified. Its focused existing adapter check passed.
This does not complete Schwab quote, reference, options or Streamer journeys,
nor does it permit retirement of the backed-up 23-file Schwab worktree.

The Census worktree still has three unstaged files and 22 branch-only commits.
Its September 7 archive predates those edits, so a new independent exact-file,
index, patch and complete-history bundle backup was created at
`/Users/sawmonabo/dev/market-squawk-handoff-backups/2026-09-23-census-wip-reconciliation/`.
All 11 archive checksums passed; the source remains linked pending branch-only
behavior disposition and installed restart proof. The Kraken worktree's six
unstaged files and five branch-only commits match its verified September 23
independent backup. Current V1 has newer catalog-selected behavior, but its
publication/shutdown/restart proof is not yet accepted; Kraken remains linked.
