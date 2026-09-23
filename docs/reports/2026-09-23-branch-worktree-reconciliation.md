# Branch and worktree reconciliation audit — 2026-09-23

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
