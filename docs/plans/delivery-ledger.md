# Market Squawk Delivery Ledger

Last updated: 2026-08-31

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Current execution handoff — 2026-08-31

This section supersedes the 2026-08-30 active-state summary below. Historical release and audit
records remain unchanged as locators.

- The pushed feature branch reached code head
  `4ca2f68ebdd52e82a75eccbfffbfe328addbb470`, tree
  `d07ba073ecbadd68607696703df470b73af7a1cf`, on
  `feature/v1-installed-product-experience`. The main checkout was clean and matched `origin`
  immediately after that push. No merge to `main` or a release branch, package publication, public
  release, or CI/CD dispatch occurred.
- FRED/ALFRED is now integrated as a **durable data-source-complete** lane: one bounded official
  FRED current UNRATE journey published 2,197 rows from three pages and one bounded ALFRED vintage
  journey published 961 rows from one page; each retained sealed raw evidence, canonical immutable
  publication, an exact typed point-in-time read, complete `LocalProduct` shutdown, construction of
  a new product instance, exact manifest/raw/native reopen, and an identical typed read. The source
  candidate `49023f7124480b08b431e05d97a362b3ac3f4b47` was independently approved with zero Critical,
  Important, or Minor findings. Its fifteen commits were replayed onto current root with exact
  range-diff equality, and the current-root application library compiled successfully with Rust
  1.97.1 and locked dependencies.
- FRED/ALFRED is not yet a **full product vertical**. Provider-neutral macro selection, exact
  feature/model/forecast/financial-model/valuation/backtest consumption, calibrated recommendation
  evidence, Desktop/CLI/MCP composition, and the installed shutdown/restart journey remain open.
  Federal Reserve Board H.15 remains the other proven durable macro baseline; its provider-neutral
  investment-evidence leaf is integrated, but the same complete product edge remains open.
- Integrated product building blocks also include the sealed provider-neutral EIA analytical
  handoff and the provider-neutral harmonic-pattern kernel. Harmonics cover the eight closed V1
  patterns with causal pivots, exact ratios, ranges, targets, invalidation, expiry, implementation
  identity, parent manifests, and an evidence digest. They deliberately confer neither confidence
  nor execution authority until bound to chronological out-of-sample and complete decision
  evidence.
- Active provider lanes are disjoint: reference identity; Alpaca durable current/history/options;
  Coinbase and Kraken native identity; Schwab read-only REST/Streamer authority; Treasury,
  Census, BLS, BEA, Yahoo, and IEX HIST remediation. The decision/product lane separately owns
  explicit chronological out-of-sample evidence, method-specific financial-model evidence, exact
  harmonic-evidence binding, and the provider-neutral Investment Brief contract. Root alone owns
  shared manifests, catalog migrations, application/Tauri registration, workspace manifests and
  lockfiles, and ordered integration.
- The current serialized barrier is canonical reference resolution. Its source-qualified reverse
  selector must be durable and reachable through the application before Coinbase, Kraken, Alpaca,
  Schwab, and IEX mappings can be composed without fabricated ticker or startup-time identity.
  After that seam lands, the completed provider candidates integrate sequentially through the
  shared catalog/application hotspots while unrelated provider remediation continues.
- Thin verification remains the rule: one response-family mapper/authority case, one publication,
  degradation, or restart case, and one typed product journey where required. The complete local
  gate, native packages, installed E2E, and hosted CI remain reserved for the final unchanged
  feature candidate.
- Completed FRED integration and candidate worktrees were removed after handoff, their worktree
  metadata was pruned, the patch-equivalent local branches were deleted, and the remote candidate
  branch was deleted. Dirty older shared worktrees remain deliberately preserved and must not be
  force-removed. Root was clean after the integration and cleanup.

## Current execution handoff — 2026-08-30

This section supersedes older active-state statements below. Historical release and audit records
remain unchanged as locators.

- Frozen and pushed feature checkpoint:
  `fe8c130cd48301874705f6b665b826c904b5d6a2` on
  `feature/v1-installed-product-experience`. No release-branch or mainline merge, public release,
  package publication, or CI/CD dispatch occurred.
- Accepted focused evidence at that unchanged checkpoint:
  application library compilation, diff integrity, and the existing critical publication journey
  covering sealed provider capture, canonical and derived publication, product admission, exact
  historical forecast evidence, process reopen, backup, and fresh restore. The Desktop TypeScript
  tree was unchanged from the earlier accepted `a66970fc` typecheck. These are focused integration
  proofs, not the final release gate.
- Integrated provider/data outcomes include the existing durable Federal Reserve Board H.15
  vertical plus durable provider leaves for FRED/ALFRED, Treasury fiscal and daily rates, BEA, BLS,
  Census, EIA, Alpaca history, Nasdaq reference, OCC/Cboe reference, SEC filings/fundamentals/funds,
  Yahoo, Tiingo, IEX HIST, Coinbase/Kraken public data, and the Coinbase Direct production join.
  H.15 remains the only source currently counted as a complete installed live-to-restart product
  vertical; the other entries are not represented as fully composed merely because their durable
  leaves exist.
- The shared ordinary-result envelope and the rewritten Markets, Macro, Forecast, and Backtest
  slices are provider-neutral. Provider names, source/runtime state, retry details, manifests,
  digests, and configuration evidence are being confined to Connections, Settings, Logs, and
  Diagnostics. Older Advanced Research and Decisions browser contracts still expose some
  data-management coordinates and remain active release-blocking remediation; the complete
  ordinary Desktop boundary is not yet accepted. Forecasts and backtests now use opaque product
  tokens and expose financial meaning, point-in-time/out-of-sample evidence, costs, uncertainty,
  limitations, expiry, invalidators, and honest unavailable/no-action states.
- The single V1 product dataset recipe now combines immutable price-return evidence with the closed
  twelve-component macro context and the fixed-horizon forward-return label. The production
  publication path retains capture, provider-publication, and complete-history lineage transitively
  across derived generations, and the same admitted generations reopen for forecast and backtest
  consumers. Schwab quote activation now requires exact sealed provider-authored timing evidence;
  unknown timing is retained internally and fails closed.
- Active Wave C uses disjoint ownership for: the single V1 macro-enriched feature recipe; neutral
  reference/fundamental/fund reads; neutral options and history reads; Schwab current quote runtime;
  Schwab history/options adapter mapping; credential/live-evidence verification; and one serialized
  ordinary CLI/MCP visibility policy. Shared contracts, application composition, Tauri registration,
  and Desktop transport remain serialized integration hotspots.
- Remaining terminal path:
  neutral consumer composition -> features/forecasts/valuation/backtests -> recommendations and
  portfolio/risk/paper -> fully wired Desktop/CLI/MCP -> installed live restart journey -> one final
  unchanged release gate.
- The main worktree was clean immediately after pushing `fe8c130c`. Seven older auxiliary worktrees
  remain preserved because each contains unique uncommitted state: `alpaca-history-shutdown` (17
  paths), `common-seal-root-integration` (255), `crypto-canonical-data` (24),
  `fred-shared-integration` (41), `postqualified-live-export` (5), `sec-product-handoff` (28), and
  `source-current-integration` (57). They must not be force-removed; each will be reconciled or
  preserved before cleanup.

## Active installed-product V1 execution

- Active branch: `feature/v1-installed-product-experience`, based on
  `release/market-squawk-v0.1.0`. No public release, package publication, merge to `main`, or final
  release-branch integration is authorized in this execution scope.
- The latest pushed product-code checkpoint is
  `f1dafac589cbcf4feb66d478bfdf2fece6ee642c`, tree
  `ead687bc2e00d1f5a484842f9713b544a36e340f`. It preserves the bounded Federal Reserve Board H.15
  dashboard vertical, Desktop service-generation reconnect barrier, and reviewed feature-product
  authority checkpoint, then adds the protected main-window provider-credential import described
  below.
- The main checkout owns the feature branch. `.worktrees` is empty and no temporary lane branch
  exists. The research/data authority and Desktop credential-import slices are integrated and
  pushed; product code was clean and upstream-aligned at `f1dafac5`. This delivery-ledger update is
  the sole recording overlay. The Python source-closure lock is intentionally not refreshed because
  that release authority is updated only after the remaining product source changes are final.
- The approved Markets expansion is now a V1 release blocker in issue
  [#45](https://github.com/Sawmonabo/market-squawk/issues/45), the maintained installed-product
  design/plan, and the
  [provider-ecosystem decision](../research/2026-08-08-unified-markets-provider-ecosystem.md). V1
  requires one unified non-technical feed/search/instrument experience over bounded concurrent
  providers, a searchable multi-asset universe, best-available-depth disclosure, deterministic
  source selection/downgrade evidence, and end-to-end use by forecasts, targets, backtests,
  portfolio analytics, risk, and paper workflows.
- The audited market-data closure is now a V1 release blocker alongside that Markets work. The
  maintained [provider architecture](../architecture/market-data-provider-architecture.md) assigns
  Alpaca Paper Only/Basic to the governed free IEX live/WARM and stock-history core; Nasdaq Trader,
  OCC, and Cboe to content-addressed reference discovery; SEC to company/fund evidence; FRED/ALFRED plus
  direct government providers to macro; optional Tiingo to bounded daily mutual-fund NAV/EOD; and
  a default-enabled pinned Yahoo contract to adaptive explicit-demand enrichment only. Low-capacity
  free tiers are not admitted unless their complete assigned workload fits. Schwab's Individual
  Trader API is now an optional owner-enabled complementary market-data source, not a base
  dependency. Current
  first-party documentation proves the 30-minute access/seven-day refresh lifecycle and one
  Streamer connection/user; a bounded authenticated read-only probe proved the configured app's
  multi-asset REST shapes, 500/500 single-request quote return, option/history/reference surfaces,
  and five accepted Streamer services. Schwab still publishes no numeric market-data REST rate,
  REST batch maximum, or Streamer symbol maximum, and normal-session sustainable throughput is not
  release-proven. Its implementation therefore requires a strict market-data/User Preference
  allowlist, protected token rotation, one multiplexed socket, adaptive capacity, exact
  delay/feed/depth provenance, unlink/revocation handling, and no account/order routes. The exact
  [credential input](../reference/market-squawk-provider-credentials.env.example) and
  [account setup](../operations/provider-account-setup.md) are documented. The pushed candidate
  implements the strict 32-field `market-squawk-provider-credentials/v1` parser and the one-time
  installed command
  `market-squawk source import-credentials <absolute-file> --confirm`. It stages bounded bytes to
  the existing onboarding/secret-store service and returns exactly 17 secret-free provider
  dispositions: `disabled`, `credential_stored_unverified`, `probe_required`, or
  `profile_unavailable`, and the Desktop now invokes that same protected operation through a
  main-window-only native picker and one-shot staged ticket. Import never probes, activates,
  schedules, publishes, or trades. The former fixed Yahoo 25-symbol
  value had no provider evidence and is removed: one shared runtime lane must measure actual
  attempts and returns, coalesce/cache demand, and stop on its provider-wide 429 circuit. IEX HIST
  enablement authorizes only explicitly selected, byte-admitted feed/date cold jobs and never an
  automatic full-catalog download. Current in-flight core/transport adapters now exist for Yahoo,
  IEX HIST, OCC/Cboe reference, owner-enabled Schwab, optional Tiingo, BEA, Federal Reserve Board,
  Census, and EIA; installed activation bindings, doctors, transport completion where applicable,
  publication, PIT reads, and product composition remain incomplete. FRED v2 release bulk; SEC
  N-PORT/N-CEN; complete Alpaca historical and current-batch composition; adaptive scheduling;
  quota/quality telemetry; and the corresponding canonical product consumers also remain
  incomplete.
  Yahoo cannot become WARM or sole decision authority without a retained normal-session benchmark,
  and the 8,000-symbol Alpaca target is conditional on an effective batch of at least 50 plus
  authenticated rate/entitlement proof. The credential file is one-time operator input, not a
  startup/runtime configuration layer and not an availability claim. The per-source contracts are
  indexed under
  [selected providers](../reference/providers/README.md), and the shared closed data families,
  clocks, exact values, immutable generations, PIT selection, analytical bindings, and typed reads
  are governed by the [canonical schema contract](../reference/market-data-canonical-schemas.md).
  Tiingo NAV specifically requires the closed
  `ResearchObservation::FundNav(FundNavObservation)` variant, exact fund/share-class and NAV-date
  identity, value-or-missing state, availability/revision/PIT evidence, immutable publication, and
  a bounded typed fund read; provider EOD bars cannot substitute for NAV.
  FRED remains version-specific: v1 observations use up to 100,000 rows/page with offsets and no
  reviewed numeric v1 request-rate ceiling; v2 release observations use up to 500,000 rows/page
  with cursors and a documented 2-request/second throttle. Market Squawk retains one conservative
  shared 1-request/second v1/v2 queue. Capacity acceptance must report actual valid returned
  observations, contracts/Greeks, stream events, generated bars, manifest rows, and bytes separately
  from requests and requested slots; full-session actuals remain unmeasured until retained probes
  establish them.
- Data-first resumption contract, 2026-08-11: the maintained provider architecture now defines the
  full closure path `configured -> entitled -> producing -> published -> queryable -> composed ->
  release-proven`. An enabled provider field is only import/probe intent. New sources must publish
  exact raw evidence and canonical observations through the existing capture, SQLite authority,
  Arrow/Parquet generation, manifest, PIT selector, and typed application-read boundaries before
  any Desktop/CLI/MCP workflow becomes available. The required first verticals are: provider
  import/doctors; reference identity plus Alpaca IEX into Markets search/current; owner-enabled
  Schwab read-only market data; Alpaca/Schwab history into charts and reusable model/backtest
  generations; SEC and macro into fundamentals/research; entitlement-gated options and optional
  Tiingo funds; specialized Yahoo/IEX HIST lanes; then
  recommendations, portfolio/risk, and virtual paper over those same typed reads. The exact
  32-field credential/probe-intent example
  schema and the owner-local credential file have matching field names; the local file remains
  mode `0600`, and its values are not recorded here. Import produces only Configured,
  Probe-required, Disabled, or Profile-unavailable evidence. Available still requires the complete
  chain above. Implementation has resumed; the next serialized vertical is an Alpaca Paper/IEX
  read-only doctor and durable activation boundary, not an inference from imported credentials.
- The first dirty-tree integration review found concrete paper/live lifecycle, provider-switch,
  research-file client-isolation/crash-recovery, desktop bootstrap, startup-window, stored-source
  attribution, preview-retention, and development-runtime defects. The code checkpoint closes
  those bounded defects with serialized live ownership, owner-scoped durable import recovery, one
  explicit bootstrap action, delayed window reveal, source-bound stored evidence, bounded preview
  retention, and a reusable two-program model runtime. The market-runtime checkpoint then removes
  the one-live-provider restriction and prevents paper execution from opening a duplicate market
  connection. This is remediation and implementation evidence, not a new review checkpoint or
  release approval.
- Issue [#25](https://github.com/Sawmonabo/market-squawk/issues/25), issue
  [#45](https://github.com/Sawmonabo/market-squawk/issues/45), draft PR
  [#43](https://github.com/Sawmonabo/market-squawk/pull/43), and the Project items remain open and
  `In Progress`. The pushed Markets slice passed a locked application-library check with zero
  warnings, a locked Tauri Desktop check, Desktop TypeScript compilation, the one critical unified
  Markets journey, the account-group resynchronization authority case, repository formatting, and
  diff integrity. The release-evidence slice passed 21 host-boundary cases, the exact
  source-closure-drift case, and Python syntax validation. These are focused checkpoint results,
  not the future unchanged-head release gate. Generated Cargo output is 17,482,952 KiB, below the
  20 GiB ceiling; no extra worktree exists.
  Automatic broad run
  `31322655877` was cancelled before completion. Workflow checkpoint
  `9baac4f4f24af67befb5ffca406ce2348084f45e` now reserves compiler/test matrices for explicit
  frozen-candidate dispatches and pushes to integration branches; intermediate pull-request pushes
  retain only lightweight classification, generated-input, credential, and documentation policy
  checks.
- Remaining barriers before the requested owner-test handoff are outcome-based:

  1. Carry the implemented credential import through exact provider doctors and activation without
     adding a credential crate or configuration system, then close the Alpaca Paper batch and
     entitlement doctor, owner-enabled Schwab read-only OAuth/REST/Streamer binding, Yahoo
     experimental binding, Nasdaq/OCC/Cboe reference ingestion, optional IEX HIST and Tiingo
     lanes, BEA, Board, Census, EIA, FRED v2, SEC fund, Alpaca historical,
     quota/checkpoint, raw-evidence publication, canonical schema/generation, PIT selector,
     scheduler, telemetry, and fixed typed application surfaces without adding trading authority
     or a parallel data application. Only the selected provider set participates in credentials,
     scheduling, fallback, and product composition.
  2. Run the isolated no-account and credential-authorized live Market paths against exact current
     provider responses. Prove startup, search, subscriptions, source selection, order-level
     resynchronization, rate budgeting, fallback disclosure, restart, stale-credential rejection,
     and shared Desktop/CLI/MCP reads before advertising that coverage as accepted.
  3. Complete the unified non-technical investment workspace above those published typed reads:
     Markets search/current/history, bars, options when entitled, funds/NAV, fundamentals/filings,
     macro evidence, features, forecasts, buy/add/trim/sell targets, backtests, portfolio impact,
     risk, virtual paper, and personalized opportunities. A provider is not complete
     merely because its adapter runs; each intended workflow must consume its exact data or remain
     explicitly unavailable. Derived-index children remain absent unless their configured identity
     and source evidence exist.
  4. Complete the resumable guided setup execution and every remaining shared-service/MCP,
     onboarding, research, Python/model, portfolio, decision, source, and restart workflow.
  5. Run one focused installed integration/e2e pass, including every desktop route and every flow
     not blocked by an unavailable user account or key, plus fresh shared Claude Code and Codex MCP
     clients and restart/stale-credential recovery.
  6. Refresh the Python source closure only after the product source is final, freeze one unchanged
     feature head, run the complete local release gate once, obtain all four
     platform installed-product proofs, close every grouped Quarter 4 finding, update PR #43 and
     the ledger with exact-head evidence, and prepare the owner-test package.
- Completion stops at the feature-branch packaged V1 handoff. Publishing assets, creating a public
  release, or merging to the release branch or `main` remains explicitly outside this execution.

## Current v1.0.0 release state

- The accepted desktop and installer product candidate is
  `1611c268bb04cf5ed872749bfe44d7e3bfca8c04`, tree
  `6489b4bd5fbc6d3d68157dd8efc37d3d957ee2bc`. Installer PR
  [#39](https://github.com/Sawmonabo/market-squawk/pull/39) and desktop PR
  [#37](https://github.com/Sawmonabo/market-squawk/pull/37) are merged at that unchanged commit.
  Mainline reconciliation commit `d2a5fe16538b335018c0f05edac9c9b16c846c07` records
  `origin/main` head `da0dbf845136fad475fca3b9fb45faf6cb6be150` as integrated without
  changing the accepted product tree: the release and fuzz locks already contained the exact
  `futures-util` 0.3.33, `async-trait` 0.1.91, and Serde 1.0.229 dependency records plus the
  release-only graph. Draft release PR
  [#26](https://github.com/Sawmonabo/market-squawk/pull/26) is now mergeable; no public release was
  created.
- The product release version is `1.0.0`. The complete release carries the Obsidian Signal
  desktop, CLI, capture helper, ONNX worker, model validator, training driver, Rust installer,
  uv 0.12.0, managed CPython 3.14.6, and the exact locked Python analytics and training product on
  Linux x64, Windows x64, macOS Intel, and macOS Apple Silicon.
- The complete-bundle builder, immutable installation lifecycle, native package assembly,
  four-platform release workflow, artifact attestations, conditional native publisher signing,
  truthful zero-cost `provenance-only` mode, and targeted pull-request CI are implemented. The
  installer now publishes durable verified desktop, CLI, and maintenance entrypoints on POSIX
  systems and refreshes them across install, update, repair, and rollback.
- Exact candidate `1611c268` passed normal hosted CI unchanged in
  [run 30685016357](https://github.com/Sawmonabo/market-squawk/actions/runs/30685016357).
  Explicit release-platform
  [run 30685104590](https://github.com/Sawmonabo/market-squawk/actions/runs/30685104590)
  passed every shared gate plus installed-product verification on Linux x64, Windows x64, macOS
  Apple Silicon, and macOS Intel. The exact-head review reported no findings, and PRs #37 and #39
  have no unresolved review thread.
- Desktop issue [#36](https://github.com/Sawmonabo/market-squawk/issues/36) is closed and its
  Project 5 item is `Done`. Installer/publication issue
  [#38](https://github.com/Sawmonabo/market-squawk/issues/38) remains open and `In Progress` only
  for its separate stable-endpoint and public-release-asset acceptance. This checkpoint does not
  publish the application.
- V1 release work remains open under provider/external-evidence issue
  [#7](https://github.com/Sawmonabo/market-squawk/issues/7), provider-onboarding issue
  [#31](https://github.com/Sawmonabo/market-squawk/issues/31), public distribution issue #38, and
  terminal release issue [#25](https://github.com/Sawmonabo/market-squawk/issues/25). Mainline
  reconciliation is complete. The dependency locks, full all-target/all-feature workspace check,
  workspace-boundary check, formatting diff check, and clean-tree check passed at `d2a5fe1`; the
  accepted desktop and installer implementation is not represented as terminal V1 publication.
- The completed installer worktree and its 8.7 GiB of generated Cargo output were removed. The
  merged local/origin installer and desktop branches and temporary platform-verification branch
  were deleted, and worktree/remote metadata was pruned. Only the clean release worktree remains;
  its 12 GiB target is below the 20 GiB ceiling, `.worktrees` is empty, approximately 149 GiB is
  free, and no Cargo or Rust compiler process is active.

## Historical integration record through 2026-07-28

- Release branch: `release/market-squawk-v0.1.0`
- Latest integrated product-capability head:
  `f8c2569ee4addcfbd8d93553d6b4c541dbdb00ae`
  (`Coordinate paper recovery sequence handoff`), tree
  `0a8d5ab177b53d0496d6fecb8672f3262ae8e533`.
- Exact candidate `f8c2569` passed unchanged in hosted Actions
  [run 30366976240](https://github.com/Sawmonabo/market-squawk/actions/runs/30366976240):
  Linux `scripts/verify.sh` completed in 49m20s, Windows completed in 15m19s, and macOS completed
  in 25m50s. This accepts the cross-platform paper-recovery and preceding correctness remediation at
  that code head. It is not terminal V1 approval; the provider and final release predicates below
  remain open.
- The active package candidate is `0.2.0`; the published `v0.1.0` foundation tag remains immutable.
  Public BLS v1 now exposes its exact adapter-owned dataset identity through activation, status,
  portal bootstrap, and restart recovery. Treasury Fiscal Data now performs the complete bounded
  discover/ingest/publish/query/restart workflow and binds every page, request, payload, manifest,
  row, and lineage identity into schema-version-5 provider evidence. The sealed Python builder
  copies the application, ONNX worker, and validator into both retained environments before
  signing and selects the immutable CPython 3.12 application copy for all downstream evidence.
  Terminal provider closure accepts exactly the eight mandatory surfaces.
- Research/model first use is integrated at that exact head. Verified non-inline DataFusion results
  are republished as durable content-addressed Parquet for bounded CLI/MCP retrieval. Compact Arrow
  results whose JSON envelope exceeds the inline ceiling retain the exact hard-result budget and
  reach MCP's controlled opaque-overflow publisher. The signed release installs the deterministic
  `market-squawk-train` driver for linear/regression and logistic/binary-probability ONNX proposal,
  Rust validation, admission, and tract inference.
- Independent review accepted the exact source candidate with zero Critical, Important, or Minor
  findings. Strict production-app Clippy, formatting, diff integrity, JSON/source-lock admission,
  and the 787-of-787 release source closure passed.
- The sealed offline matrix passed on both CPython 3.12.12 and 3.13.7 with 11 tests and 2 training
  subtests per interpreter. The signed foundation SHA-256 is
  `50a48460a41c0c0f581a3eeeed1543937a994874f1fc26880814bff50a24340a`; release-manifest
  SHA-256 is `f0409fe78a8bafbb188b625abb03a468a0772ffb9b9c7ca571b5f11aa21e8d72`;
  evidence SHA-256 is `69b8ae141694f360e8917c3d7649b05034b0a8c5c5e1387ecf595c871aa9d714`;
  and the sealed wheel SHA-256 is
  `f972e8bdcf3fd0bb35aa6835db6df1935fcecb2cefb28af93d350e1a59632da6`.
- Shared release composition is integrated at `3ef05dc`: one controlled path-free artifact
  repository serves application, CLI, and MCP; `Analysis.ReadArtifact` exposes digest-bound
  32 KiB chunks; the model domain admits the signed application and ONNX worker; and configured
  initial paper cash becomes an immutable evidence-bound portfolio revision consumed by central
  risk.
- Fresh focused evidence at `3ef05dc`: exact production MCP composition, production paper
  composition, and merged point-in-time backtest tests passed; affected services/application/MCP/
  modeling Clippy passed with warnings denied; formatting and diff checks passed.
- Coinbase Direct integrity candidate `6182da007312023ef5fa78a0537ccb273d63a24f` and authenticated
  transport candidate `cef4d59` passed independent review with zero Critical, Important, or Minor
  findings. The transport's four commits were rebased one-for-one onto the integrated release tree
  and accepted unchanged at `ff406e9`; release-source authority was reconciled at `2e6d6c6` with
  all 790 expected source files locked. Focused evidence passed for authenticated-profile truth,
  bounded HTTP bootstrap and sequenced-frame queuing, same-owner handoff to live supervision,
  sink-rejection-before-state-mutation, strict Coinbase/sources Clippy, formatting, and diff
  integrity. The application now binds an exact active Direct onboarding generation, current
  signer, shared provider-rate/account authority, canonical snapshot/delta publication, central
  qualification, and explicit risk-paper selection. The code boundary passed focused compile,
  strict Clippy, formatting, and all 47 existing application tests; a focused lifecycle review
  confirmed cancellable generation-bound startup and terminal-supervisor health propagation. An
  authorized unchanged-head external trace remains open under issue `#7`.
- Release-source admission is integrated through `c8ceb82`: authenticated Coinbase Direct release
  sources carry the required transport authority while the public Coinbase and Kraken compatibility
  sources retain `DirectUnverified` ceilings. No compatibility source can be promoted to
  execution-eligible quality by composition.
- Release-performance evidence candidate
  `afd9a58c7f8e36be7448543d61da2b0e6f36be10` passed focused check and strict Clippy and was
  independently accepted with no material blocker. Merge head `620d212` preserves finite RSS
  observation semantics and publishes evidence only after executable/repository identity
  validation through an atomic no-clobber commit. The focused integrated application check,
  formatting, and diff integrity passed. Exact-head production measurements and final Task 20
  evidence remain open.
- Provider-onboarding control-plane candidate
  `489113fae63ae2e7288be2bf784abea6651a8bec` was accepted by independent exact-head review and
  merged unchanged at `3219662`. The integrated authority owns shared provider rate budgets,
  generation-bound activation, transactional credential replacement, bounded failed-cutover
  recovery, candidate-preferred renewal, and retained portal transaction ownership through durable
  mutation and shutdown. Focused integrated application checks, the one-shot activation vertical,
  exact cutover and recovery transitions, strict no-default and release-evidence Clippy, formatting,
  and diff integrity passed. Issue `#31` remains In Progress because provider release availability
  and the clean-machine activation/recovery acceptance evidence remain incomplete.
- Provider release-admission candidate `978db45a0c60427531fc6e3d44fd4d52ba75772a`
  is integrated at `bf02a0b`. The production CLI now collects exact-head provider evidence through
  the shipping onboarding, activation, live-quality, central-risk, paper-execution,
  research-runtime, shutdown, and restart-recovery authorities. The release closer requires the
  closed mandatory surface set, exact executable identity, a real `DirectVerified` paper action,
  admitted FRED/ALFRED persistence and model-training rights, and complete restart evidence.
  SEC/BLS successful official-body evidence, FRED/ALFRED durable-use rights, Treasury daily-rate
  exact-head external proof, and the authorized Coinbase Direct trace remain fail-closed acceptance
  inputs; no release predicate was manufactured. Official Treasury/Data.gov research dated
  2026-07-26 establishes CC0 durable-use authority for all five daily-rate families, and the
  mandatory internal implementation subsequently completed at `50912c1`.
- Task 19 local control-plane candidate `879e505223729fee4a5be607b21a6deb396f849f`
  is integrated unchanged after independent exact-range review reported zero Critical, Important,
  or Minor findings. The shipping CLI now owns full-product `init`, provenance-bearing redacted
  configuration reads, and query-only `doctor` diagnostics that never create, migrate, recover, or
  exclusively lock product state. The sole 62-tool stdio MCP registry advertises and validates
  operation-specific output schemas, maps actionable service rejections to protocol tool errors,
  and performs application-owned bounded shutdown. Unix and Windows termination listeners are
  installed before composition so startup-time signals cannot bypass the MCP, application, or
  audit drain.
- Focused Task 19 evidence passed the existing application, services, and MCP suites; strict
  affected-package Clippy; formatting; diff integrity; shipping MCP smoke; real `init` followed by
  two byte-identical nonmutating `doctor` runs; and a real startup-time SIGTERM process probe.
  Correction `a3609b3` moved provider-generic order synchronization and exact decimal normalization
  into `market-squawk-sources`, retained the live crate's public API through exact-type re-exports,
  and removed the Coinbase adapter's normal dependency on the live crate. The required boundary
  gate, 264 existing sources/live/Coinbase unit tests, strict affected-package Clippy, application
  compile, formatting, and diff integrity passed. Issues `#10` and `#24` and their Project 5 items
  are closed/Done.
- Task 20's exact-head product demonstration is integrated at `4ac54d2`. The shipping
  `release demonstrate` path composes the production local application, storage and point-in-time
  selection, DataFusion query, signed Python environments, native and ONNX inference, research
  backtest, live integrity and features, central strategy/risk/dispatch, realistic paper
  execution, portfolio analytics, fair-value evidence, CLI operations, and the sole typed stdio
  MCP registry. Its closer verifies immutable repository, executable, provider, Python, inventory,
  artifact, and result identities and fails closed on a dirty or changed head, incomplete
  evidence, stopped-operation success, credential-bearing output, or missing paper fill.
- Focused demonstration evidence passed the consolidated offline-admission test, the existing
  next-snapshot partial-fill backtest test, affected-package all-target/all-feature Clippy with
  warnings denied, application all-target/all-feature compile, formatting, diff integrity, the
  fuzz workspace's locked offline metadata admission, and ownership-map JSON validation. The
  demonstration proves the complete offline product surface without fabricating the separately
  required authorized Coinbase Direct trace or provider persistence/training rights.
- Provider-onboarding coverage is integrated at `2a8e9ab`. The loopback portal now commits scoped
  source sessions for public Coinbase, Coinbase Direct, Kraken, and Treasury daily XML; builds the
  closed Coinbase credential envelope from separate write-only fields; continues cleanup and
  restart flows; and exposes product-owned local-authority removal. Research surfaces cannot enter
  that source-only path. The provider evidence producer now commits its automatically probed
  no-credential sessions before requiring active authority.
- Exact integrated evidence passed the new source-session allowlist/commit test in the existing
  library harness and the existing CSRF/write-only-secret portal vertical. Strict application
  all-target/all-feature Clippy, formatting, diff integrity, and direct Node syntax validation of
  the embedded portal JavaScript passed. No new test executable or worktree was created.
- The accepted research/model worktree and its 9.0 GiB generated target are removed; the merged
  local feature branch is deleted, no matching origin branch existed, and worktree/remote metadata
  is pruned. The accepted Coinbase target and worktree are likewise removed, its merged local
  branch is deleted, no matching origin branch existed, and metadata is pruned. The completed
  performance-evidence worktree, its 1.0 GiB generated target, both merged local branches, and the
  remaining origin feature branch are also removed and pruned. The accepted provider-onboarding
  lane reclaimed 6.8 GiB before its clean worktree and merged local branch were removed; no matching
  origin branch existed. The provider release-admission lane reclaimed 6.9 GiB before its clean
  worktree and merged local/origin branch were removed and metadata was pruned. Task 19 reclaimed
  8.4 GiB before its clean owned worktree and merged local branch were removed; no matching origin
  branch existed, and worktree/remote metadata was pruned. The dependency-boundary correction then
  reclaimed 1.9 GiB before its clean owned worktree and merged local branch were removed; no
  matching origin branch existed, and metadata was pruned. Only the release worktree remains. Its
  generated target is approximately 13 GiB, below the enforced 20 GiB ceiling; `.worktrees` is
  empty, the root incremental directory is approximately 9 MiB, and approximately 114 GiB is free.
  The release-demonstration lane then reclaimed 7.1 GiB before its clean worktree and merged local
  branch were removed; no matching origin branch existed, and worktree/remote metadata was pruned.
  The onboarding-coverage lane used the root target, introduced no worktree or origin branch, and
  deleted its merged local feature branch immediately after fast-forward integration. The root
  target is 17,055,000 KiB, below its 20 GiB ceiling, with 9,500 KiB of incremental state.
- Dependabot pull requests `#2`, `#32`, `#33`, `#34`, and `#35` are merged; superseded updates
  `#3` and `#4` are closed. No dependency pull request or Dependabot branch remains open locally or
  on origin.
- Mainline ancestry merge `ed86d4f` records the five already-integrated Dependabot commits from
  `main` without changing the accepted release tree
  `4087626a8c3722fd07d38f2bd970ba316e30d2e2`. The release PR changed from conflicting to mergeable
  and immediately scheduled current-head Actions run `30197493366`.
- Hosted Actions run `30197493366` at `ed86d4f` created `verify`, `macos`, and `windows` jobs with
  empty step lists. No checkout, build, lint, or test step ran; each check reported the external
  account payment/spending-limit blocker. That run contains no code-owned CI failure to remediate.
- Hosted Actions run `30201225241` at demonstration head `4ac54d2` repeated that exact condition:
  all three jobs have empty step lists and the account payment/spending-limit annotation. The
  workflow scheduled at the current code head, but GitHub rejected every job before checkout.
- Documentation execution source head:
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` with tree
  `774a7bc9f4f26eb437fa1ab061dc4b557d20d0bc`. The source worktree was clean, the release
  branch matched `origin`, and the approved design blob was
  `7fdb58ece5b41211493cd4026773974ff30ce240` when the migration branch was created.
- Completed documentation branch: `docs/product-documentation` was fast-forwarded into the release
  branch at accepted content head `a2596a6`, then deleted locally and on origin. The lane used only
  the root worktree; no per-page branch, worktree, Cargo invocation, or duplicate build cache was
  created.
- Documentation authority/history commits: accepted-head refresh `93cd746`, pinned execution
  barrier `6a06b34`, and history-preserving model-runbook move `c3b3512`.
- Documentation architecture/reference commits: tool-inventory correction `97383df`, CLI/config
  reference `c863df1`, time/trust/ADR content `f2a6331`, quality/time reference `b29d13a`, runtime
  planes `c31a7a3`, context/deployment/quality `6d44f8e`, archived baselines `bc46da3`, portal/ADR
  indexes `27b30af`, and MCP/source reference `ee0f32e`.
- Documentation operations commits: bootstrap/configuration/sources `03d44ad`,
  research/datasets/models `593c5ee`, and portfolio/recovery/troubleshooting `531a7df`. The portal,
  root navigation, plan state, memory, and this ledger are finalized by the commit containing this
  record. Grouped-review corrections are `e063419` and accepted closeout head `a2596a6`.
- Pre-status Quarter 3 capability-code head: `daf183a`
  (`fix(python): make native module the sealed package root`). This is a durable milestone, not the
  moving release-branch head.
- Accepted documentation content head: `a2596a6ae4dafa9915d2b42cac71635c77c632f8`, tree
  `56969cc0d585cbd207237f6b724de9adb8270ce5`. Pull request `#26` identifies the moving integrated
  release head; tracked prose does not self-pin the status commit that contains it.
- Quarter 3 status: terminally accepted at exact pushed head
  `c6f0124c2b27c4777947de8c42b6a5f97868aaf5`. The earlier grouped reviews accepted Tasks 13–15,
  Task 18, backtest authority, and the cross-plane boundaries while rejecting substantiated
  portfolio and backtest gaps. Each accepted remediation was integrated unchanged and freshly
  verified. The corrected portfolio and cross-plane reviews reported no Critical, Important, or
  Minor finding. The final exact-head review then found a stale sealed Python source authority,
  rejected the intermediate head, and accepted `c6f0124` only after the 370-file authority was
  reconciled and the existing release-builder harness was made to exercise production source
  admission. The exact-head nonincremental full release gate passed, and issues `#20`, `#21`, and
  `#22` plus their Project 5 items are closed/Done. The proposed coupling of a pure cohort plan to
  one inventory's configured trial limit remains rejected as a layering regression.
- Task 14 accepted feature and fast-forwarded release head: `02ab5cd`
- Task 18 release merge head: `051ee3c`; reconciled lock head: `5c34b7d`
- Task 13 accepted feature and release head: `59ba05c`
- Task 16 accepted core head: `e124722`; accepted execution-binding and release head: `7621552`
- Task 12 code integration head: `9702556` (`fix(analytics): bind complete batch semantics`)
- Integrated and pushed hardening code head: `2d39b0a34eb818f817973210148355c88f8f4b52`
- Hardening owner: GitHub issue `#30`, Project 5
- Hardening status: implemented, verified, integrated, pushed, and cleaned up
- Documentation-system lane: complete, reviewed, integrated, published, and cleaned. The canonical
  written design is
  [`2026-07-22-market-squawk-documentation-system-design.md`](../superpowers/specs/2026-07-22-market-squawk-documentation-system-design.md).
  Architecture, operations, reference, ADR, and dated-audit pages now satisfy the approved content
  tree against product head `836aae6`. Documentation-migration Tasks 1–6 are complete. Task 7's
  first frozen candidate `b0ed3e9` completed content, navigation, GitHub Mermaid, and grouped review
  gates but was rejected on substantiated documentation findings. First correction head `e063419`
  closed the architecture and reference findings, but its operations re-review exposed three
  remaining runbook ripples. Accepted exact head `a2596a6`
  closed those findings; all three final scopes reported zero Critical, Important, or Minor
  findings. The release fast-forward, PR evidence comment, local/origin branch deletion, and
  metadata prune are complete.
- Product release status: runnable product capabilities exist across every required domain, but the
  release remains blocked on provider qualification and rights outcomes, complete onboarding and
  clean-machine evidence, prerequisite-issue reconciliation, performance/fuzz/security evidence,
  final grouped review, exact-head gate, publication, and cleanup.

## Documentation candidate and accepted-head truth

The 2026-07-24 refresh inspected code, focused exact-head evidence, the
README, this ledger, open GitHub issues, and Project 5. It established the following current scope
for documentation writers:

- The shipping CLI exposes the complete public hierarchy from `init` through `mcp serve` and
  `doctor`, with portfolio import/analytics and fair-value workflows routed through the production
  `LocalProduct` and shared application services.
- The shipping stdio MCP surface is the sole production composition over all 11 required domains
  and 62 code-owned tool descriptors. The removed five-tool diagnostic server is neither a current
  capability nor a reference source.
- Arrow/Parquet/DataFusion research storage, point-in-time dataset construction, Python financial
  and training components, immutable model bundles, native and tract ONNX inference, governed
  backtesting, portfolio accounting/analytics, realistic paper execution, and fair-value analysis
  are implemented product capabilities at the source head. Query-overflow retrieval and the sealed
  model driver now provide their public first-use handoffs.
- SEC, BLS, and Treasury Fiscal Data have evidence-bound onboarding and adapter-activation
  implementations. Only Treasury Fiscal Data is release-available at this head. SEC and BLS require
  refreshed code-owned evidence, FRED is rights-blocked, and the clean-machine Task 19A
  demonstration is not accepted.
- Public Coinbase and Kraken remain capped at `DirectUnverified`. The distinct authenticated
  Coinbase Direct path can derive `DirectVerified` authority and reach central risk/paper execution,
  but its required authorized unchanged-head acceptance trace is not complete. FRED/ALFRED durable
  use remains fail-closed without affirmative per-series rights.

The migration corrected stale README statements for the removed diagnostic MCP, complete CLI/MCP,
portfolio import, FairValue composition, Python source-closure cardinality, and the Quarter 3 gate.
Subsequent source-derived review established the current first-use handoff state:

- `source discover` now returns bounded exact provider objects without minting authority; confirmed
  ingestion independently discovers the selected object and consumes its process-local receipt.
- `feature build` and `dataset build` now publish only immutable phase-one analytical generations
  and a deterministic phase-one descriptor. They do not populate the receipt-backed product
  registry and do not authorize Python/model training. Product reads and `market-squawk-train`
  require a separate code-owned Training-contract production receipt; no generic CLI, job, or
  caller-materialized value path can mint one.
- The Python release builder builds and signs the application, validator, and ONNX worker and
  installs the supported production training driver. Its code-owned producer emits deterministic
  static-shape linear and logistic graphs, with terminal `Sigmoid` required for
  `binary_probability`.
- Optional external ONNX Runtime support exists at library/evidence level but is not selectable by
  the current product composition; required tract inference remains the shipping ONNX path.
- Operator SQL and fixed-template application/MCP query services compose transient publication
  authority, verify oversized query output, and republish it into the shared terminal repository as
  `application/vnd.apache.parquet`. The opaque reference is retrievable through `query artifact` or
  typed bounded `Analysis.ReadArtifact`; transient reservation owner/expiry are not public terminal
  fields.
- The reviewed `LocalProduct` composes OS-keyring-first routing with a code-owned, initially locked
  encrypted-file fallback and explicit foreground portal unlock/lock. The remaining onboarding
  blocker is provider release availability and the clean-machine acceptance demonstration.

As verified through GitHub on 2026-07-26, Task 5 issue `#10` and Task 19 issue `#24` are closed and
their Project 5 items are `Done`. Issues `#7`, `#25`, and `#31` remain open and In Progress. The
active barriers are provider and Coinbase Direct clean-machine/external acceptance evidence and
Task 20's exact-head acceptance.

## Product delivery closeout and next barrier

- Task 13 owner: GitHub issue `#18`, closed with its Project 5 item `Done`.
- Delivered at exact accepted head `59ba05c`: capability-scoped immutable bundles; complete
  dataset, label, universe, feature, artifact, and code-revision validation; atomic bounded model
  generations; allocation-free deterministic native linear/logistic inference; execution-owned
  fail-closed model strategy; and durable typed no-action audit through the versioned paper-bot
  consumer.
- Task 13 verification: modeling 9/9, execution 32/32, paper-bot audit 2/2, strict affected-package
  Clippy, workspace boundaries, formatting, and diff checks passed. Independent review rejected
  three material implementation defects and two audit-consumer/wire ripples; each was fixed, and the
  final exact-head re-review reported no remaining Critical or Important finding.
- Task 16 owner: GitHub issue `#21`, Project 5, status `Done` after the accepted remediation and
  terminal Quarter 3 gate.
- Delivered through Steps 1–5 at accepted head `e124722`: source-evidenced normalized portfolio
  transactions, immutable revisions, long/short lots, FIFO/specific identification, cash flows,
  exact gains, explicit incomplete-basis measurements, authoritative corporate-action snapshots,
  reconciliation, performance, exposure, attribution, risk, scenarios, and proposal-only
  rebalancing.
- Task 16 Steps 1–5 verification: full domain tests, four portfolio-adapter integrations, the single
  consolidated 8-test portfolio executable, strict affected-package Clippy, boundaries, formatting,
  and diff checks passed. Independent review rejected four financial/evidence defects; exact-head
  re-review at `e124722` confirmed all four closed with no remaining Critical or Important finding.
- Task 16 Step 6 is accepted at exact head `7621552`: execution owns an immutable portfolio read
  capability; risk derives settlement cash, position, gross exposure, marked and peak equity,
  realized/unrealized loss, leverage, and drawdown from the current complete portfolio projection;
  approvals bind the exact revision, snapshot digest, and monotonic publication generation; and the
  dispatcher rechecks that authority before its sole adapter call. Publication rejects rollback,
  sibling races, identity resurrection, and stale or revoked revisions.
- Task 16 Step 6 verification: portfolio 8/8, execution 34/34, application risk-dispatch 6/6,
  strict affected-package Clippy, workspace boundaries, formatting, and diff hygiene passed.
  Independent exact-head re-review confirmed all three earlier authority/concurrency/dispatch
  findings closed with no remaining Critical or Important finding.
- Task 16 Quarter 3 remediation is integrated through release head `91f9f79`. Portfolio analytics
  now derives private, non-deserializable point-in-time evidence from the exact immutable revision;
  binds dataset, source, policy, and time authority; enforces both corporate-action cutoffs; and
  admits factor, scenario, history, work, output, and retained bytes before allocation. Independent
  task review approved the final lane with no remaining finding; the fresh integrated consolidated
  portfolio harness passed 13/13. The generated lane target, clean worktree, and patch-equivalent
  local branch were removed, and no matching origin branch existed. The later grouped exact-head
  review found three remaining cross-contract defects outside that focused acceptance: reporting
  currency is absent from revision identity, Attribution and Risk do not bound their total temporary
  and result work under one checked admission or consistently enforce `max_instruments`, and
  Exposure's temporary `BTreeMap` nodes allocate infallibly. The grouped correction is integrated
  through `e468d01`. Its first task review found omitted retained-schema identity and unsafe UTF-8
  byte lowercasing; both were fixed without adding a test target, and exact-head rereview accepted
  the two-commit lane with no remaining finding. The fresh integrated consolidated portfolio gate
  passed 15/15. The 1.7 GiB generated target, clean worktree, and merged local branch were removed;
  no matching origin branch existed.
- Task 18 owner: GitHub issue `#23`, Project 5, status `Done`.
- Delivered at accepted feature head `31de1a5`: nonforgeable producer receipts; point-in-time market
  activity and evidence admission; strict Level 1 classification; usable Level 2/Level 3 input
  judgments; non-promotable `Unclassified` evidence; durable dual-approved market access,
  overrides, approvals, revocations, audit chains, catalog CAS, bounded recovery, and global limits.
- Task 18 verification: the complete valuation package, four-case consolidated fair-value harness,
  bounded live-export route, catalog recovery/query regressions, strict affected-package Clippy,
  formatting, and diff hygiene passed on the integrated locked tree. Exact-head review rejected two
  point-in-time/override defects; remediation-only rereview accepted `31de1a5` with no remaining
  Critical or Important finding.
- Quarter 3 follow-up at reviewed candidate `e59dfca` closed stale-quality classification and
  legacy-v1 analytical-evidence recovery defects without changing historical identities. The exact
  two-commit series range-diffed 1:1 onto release commits `6a9a685` and `6c114c7`; the integrated
  valuation gate passed 8 unit and 4 consolidated integration tests before push and cleanup.
- Task 14 owner: GitHub issue `#19`, Project 5, status `Done` after this closeout push.
- Delivered at accepted and fast-forwarded release head `02ab5cd`: catalog-authorized Task 11
  point-in-time dataset access; fixed-width Arrow/Parquet schema-v2 validation; exact
  `decimal.Decimal` accounting inputs; bounded Rust financial kernels; visualization; deterministic
  native linear/logistic training; and finalized model-bundle publication. External authority v4
  independently binds final metadata, artifact, training-run, catalog, export, selection, feature,
  label, universe, split, code, and environment identities. The production API cannot select a
  validator executable; the adjacent Rust validator is size/type bounded and its pre/post hash must
  match the identity compiled into the native wheel.
- Task 14 verification: exact-head rereview accepted `02ab5cd` after the original catalog, memory,
  Decimal, runtime, cancellation, migration, schema, model-authority, and validator findings were
  closed. The single sealed offline release matrix admitted 357 source paths and passed 9/9 product
  contracts on CPython 3.12.12 and 9/9 on 3.13.7 with no retry. Exact evidence: release manifest
  `5403a73fbfe03d715b192e9da19cf9e7cfc8b7aa31f773bdd39586534b44618d`, project wheel
  `f19be320abd91ed73637f6d7edfa8df133ff5149cfaa8804663dadcd4134a25c`, validator
  `2b8576c3e6f219f34d958c863e08cf2b68599306faa2668ba8cf348f705e1b1c`, wheelhouse/source lock
  `92657a32099c7b309e9b73b674ae1ecee26f8c70d71e69e1a72f225a5e510f9a`, and sealed build
  environment `d0a9479dae9eb8024e5a4c6bfb1e5fa606a03e0858530ca3a1622b2580379931`.
- Task 15 owner: GitHub issue `#20`, Project 5, status `Done`. The integrated implementation
  provides the required self-contained tract ONNX backend,
  bounded helper-process/resource/deadline contracts, exact graph and warm-up admission, and
  no-action failure. The optional operator-supplied ONNX Runtime path is Linux-only, descriptor-
  verified, sealed in immutable memory, and parity-checked. Cleanup ownership is reserved before
  spawn, post-spawn waits and joins are asynchronous and bounded, and uncertain helper termination
  denies optional tract fallback.
- Task 17 owner: GitHub issue `#22`, Project 5, status `Done`. The integrated application-owned PIT
  backtesting service binds exact dataset partitions,
  executable/model/configuration identities, research execution assumptions, reconciled portfolio
  accounting, immutable success/failure terminals, artifacts, cohorts and overfitting diagnostics.
  Recovery rejects conflicting attempt-terminal namespaces, parses untrusted cohort collections
  through bounded visitors, and binds exact V3 candidate cardinality while preserving V1/V2 identity.
  The Quarter 3 remediation is integrated through release head `c70601a`: catalog-minted historical
  instrument definitions resolve at each decision cutoff and bind dataset identity, while attempt
  recovery validates every bounded canonical entry against the actual reservation digest. A real
  application vertical proves research ingest, feature-label publication, pinned query, receipt
  minting, public backtest admission, strategy-visible revisions, and exact receipt coverage.
  Independent task review approved the final lane with no remaining finding. Fresh integrated
  gates passed catalog 3/3, backtesting 12/12, and the filtered application vertical 1/1.
- Quarter 3 closed only after `CARGO_INCREMENTAL=0 ./scripts/verify.sh` passed at exact pushed head
  `c6f0124`. Quarter 4 is now the active delivery quarter: Task 19's local control-plane
  implementation is accepted, Task 19A's external/provider acceptance remains open, and Task 20
  follows both. Open prerequisite issue `#7` requires exact external evidence and cannot be ignored
  at release closeout.
- Task 19 owner: GitHub issue `#24`, closed with its Project 5 item `Done`.
- Delivered at exact accepted and pushed head `879e505`: full-product initialization and bounded
  shutdown, redacted configuration provenance, nonmutating query-only diagnostics, operation-
  specific MCP output schemas and validation, protocol-correct service rejection, and startup-safe
  Unix/Windows termination ownership.
- Task 19 verification: focused application/services/MCP suites, strict affected-package Clippy,
  formatting, diff integrity, shipping MCP smoke, repeated nonmutating `doctor`, and a real
  startup-time SIGTERM process probe passed. Independent exact-range review reported no Critical,
  Important, or Minor finding. The subsequent dependency correction at `a3609b3` passed the
  required workspace-boundary gate, 264 existing affected unit tests, strict affected-package
  Clippy, application compile, formatting, and diff integrity without adding a test target.

- Task 12 owner: GitHub issue `#17`, Project 5, status `Done`.
- Exact feature and fast-forwarded release code head: `9702556`.
- Delivered: complete pure-Rust batch returns, risk, factor, fundamental, macro, exposure,
  attribution, and scenario kernels; exact-rate and monetary-basis contracts; cadence-bound return
  series; typed statistical location/dispersion; scale-safe correlation; scaled streaming Givens QR
  factor regression; and a code-owned 43-entry batch registry whose semantic digests bind every
  execution-relevant input and policy.
- Verification: both consolidated analytics test executables passed all 24 focused tests; strict
  all-target/all-feature Clippy passed; three independent Quarter 2 reviewers reported no Critical
  or Important finding; and `CARGO_INCREMENTAL=0 ./scripts/verify.sh` passed the complete workspace,
  release, audit, documentation, offline-product, and MCP-smoke gate at exact head `9702556`.
- The former Task 13 serialization barrier is satisfied at accepted head `59ba05c`.

- Task 11 owner: GitHub issue `#16`, Project 5, status `Done`.
- Exact feature head: `95fbf0e`; exact release integration head: `8f03d87`.
- Delivered: durable provider/local revision assignment before publication; production revision
  plans for FRED/ALFRED, SEC, BLS, and Treasury; source-authored historical-universe evidence;
  conservative corporate-action and point-in-time selection; leakage-bounded feature/label dataset
  construction; authority-bound Arrow/Parquet publication and DataFusion query; and the application
  research service that owns source registration, rights admission, ingest reservation, ingestion,
  dataset construction, and analytical access.
- The checkpoint review approved the exact implementation after canonical-identity compatibility,
  aggregate retained-memory admission, and application-owned ingest-authority blockers were fixed.
- Final gates passed: the complete `market-squawk-data` suite, application control-plane suite,
  strict affected-package all-target/all-feature Clippy, full workspace all-target/all-feature
  compile, formatting, and diff hygiene. Focused verification also passed on the merged release tree.

## Rust development and test hardening delivered

- Removed 96 GiB of generated root Cargo output and 4 GiB from the preserved research worktree
  without deleting source, branches, commits, or uncommitted research changes.
- Retired `target/agent-shared`. Each worktree now owns one default local `target/`; verification
  rejects Cargo target/build-directory overrides and compiler wrappers.
- Routine dev/test profiles retain incremental feedback with line-table debug information and no
  dependency debug information. Agent, CI, benchmark, and approval gates are nonincremental. Full
  debugging is an explicit opt-in profile.
- The verifier enforces a 20 GiB target ceiling before and after its gate. CI cache writes are
  restricted to trusted mainline events.
- Consolidated existing integration tests from 115 separately linked executables to 41 without
  adding behavioral tests or removing the inventoried assertions, ignored network tests, Loom
  models, or Trybuild privacy checks.
- Scoped Rust 1.97's macOS compact-unwind linker diagnostic only at the five measured affected test
  crates. The production binary and workspace-wide diagnostics remain unsuppressed; unsafe linker
  workarounds were not introduced.
- Corrected the stdio MCP smoke client's required initialization handshake and published bounded,
  namespaced machine-readable authority contracts from the transport-neutral tool descriptors.

## Verification evidence

The Quarter 3 terminal full gate ran at exact pushed head
`c6f0124c2b27c4777947de8c42b6a5f97868aaf5`:

```text
CARGO_INCREMENTAL=0 ./scripts/verify.sh
exit: 0
Python checks: 103 passed
sealed Python source authority: 370 of 370 paths admitted
final target footprint: 15,131,260 KiB
hard ceiling: 20 GiB
```

The gate passed dependency policy, vulnerability and credential/history scans, formatting, both
workspace Clippy modes, complete locked all-feature tests, explicit UI/Trybuild checks, Loom models,
the locked all-feature release build, rustdoc and compiler-derived contract inventory, offline
product smoke, and stdio MCP smoke. The final narrow reviewer independently compared every sealed
source size and SHA-256 to the exact Git blobs and accepted the pushed head with no Critical,
Important, or Minor finding. Authorized external-network tests remained explicitly opt-in.

The prior development/test-hardening full gate ran at exact pushed head
`2d39b0a34eb818f817973210148355c88f8f4b52`:

```text
CARGO_INCREMENTAL=0 ./scripts/verify.sh
exit: 0
elapsed: 370.23 seconds
maximum resident set size: 1,675,378,688 bytes
```

The gate passed formatting, workspace-boundary policy, dependency/license/vulnerability checks,
current-tree and Git-history credential scans, strict all-target/all-feature Clippy, the complete
consolidated workspace tests, explicit UI/Trybuild tests, Loom models, locked all-feature release
build, rustdoc, compiler-derived capture-contract checks, offline product smoke, and stdio MCP
smoke. Authorized external-network tests remained explicitly opt-in.

Measurement host and artifacts:

```text
macOS 26.5.1 (25F80), Apple M1 Pro, 16 GiB RAM
rustc 1.97.1, LLVM 22.1.6, aarch64-apple-darwin
clean nonincremental pre-fix gate footprint: 8.7 GiB
exact-head checkpoint footprint after focused recompiles and the full gate: 12 GiB
exact-head target files: 29,662
exact-head executable files: 637
release application: 8,804,512 bytes
hard ceiling: 20 GiB
```

The 12 GiB checkpoint includes retained focused-build variants accumulated after the clean
baseline; it remained bounded below the enforced ceiling. It is generated compiler state, not
application size.

## Cleanup state

- Documentation lane closeout: the sole root worktree is on
  `release/market-squawk-v0.1.0`; `docs/product-documentation` is deleted locally and on origin;
  `.worktrees` is empty; and the 15 GiB root `target/` remains below the enforced 20 GiB ceiling.
  The documentation lane ran no Cargo command.

- After the terminal Quarter 3 gate, recorded the 15,131,260 KiB peak and removed 36,502 generated
  files/14.3 GiB with `cargo clean`. The root target is absent, about 125 GiB is free, only the
  release worktree remains, local and origin release heads match, and issues `#20`, `#21`, and `#22`
  plus their Project 5 items are closed/Done.
- Removed the completed hardening target: 29,662 files and 11.9 GiB.
- Removed `.worktrees/dev-test-hardening` and pruned worktree metadata.
- Deleted merged local and origin branch `feature/dev-test-hardening`.
- Preserved the root release worktree.
- Integrated derivative commit `16466db3410ccbccecbe47e8b5dedca1f07a2806`; removed its clean
  worktree; deleted local and origin branch `feature/derivatives-lifecycle-selection`; and pruned
  worktree/remote metadata.
- Removed the completed `.worktrees/research-analytics` worktree and its 18 GiB generated target;
  deleted merged local and origin branch `feature/research-analytics`; and pruned worktree and remote
  metadata.
- Removed the completed `.worktrees/analytics-feature-vertical` worktree after reclaiming its
  9.8 GiB generated target; deleted merged local and origin branch
  `feature/analytics-feature-vertical`; and pruned worktree and remote metadata. Issue `#17` is
  closed and its Project 5 item is `Done`.
- Removed the accepted model-bundle worktree after reclaiming 4.1 GiB and the accepted portfolio
  core worktree after reclaiming 2.8 GiB; deleted both merged local and origin product branches and
  pruned worktree/remote metadata. At that closeout, only the release worktree remained and issue
  `#18` was closed with its Project 5 item `Done`. Issue `#21` subsequently completed at `7621552`,
  was closed, and its Project 5 item was set to `Done`. Its generated target was cleaned, its clean
  worktree and merged local feature branch were removed, no matching origin branch remained, and
  worktree/remote metadata was pruned.
- Fast-forwarded Task 14 to `02ab5cd`, removed the clean Python feature worktree and its 5.5 GiB
  target plus 1.2 GiB ignored release evidence, deleted merged local branch
  `feature/python-financial-training`, confirmed no matching origin branch existed, and pruned
  worktree/remote metadata. Only the release worktree remains.
- Integrated the reviewed Task 15/Python containment series by an exact 1:1 range-diff at
  `daf183a`; reclaimed its 7.1 GiB target; removed the clean
  `.worktrees/model-runtime-containment` worktree; deleted merged local branch
  `feature/model-runtime-containment`; confirmed no matching origin branch existed; and pruned
  worktree/remote metadata. The three protected stashes, `bundle-backup`, main/release branches and
  Dependabot branches remain. At that closeout boundary, `.worktrees` was empty; the three active
  Quarter 3 remediation worktrees were created afterward.
- Integrated the independently accepted fair-value evidence-authority series through release head
  `6c114c7`, pushed it, removed 5,496 generated files and 3.4 GiB from its target, removed the clean
  `.worktrees/fair-value-evidence-authority` worktree, deleted the patch-equivalent local feature
  branch, confirmed no matching origin branch existed, and pruned worktree/remote metadata. The
  model-runtime and backtest-experiment-integrity worktrees remained active while their closure
  findings were remediated.
- Integrated the accepted backtest series through `a57d5df` and model-runtime series through
  `3305db6`, with exact 1:1 range-diffs and fresh integrated package gates. After push, cleaned 9.8
  GiB of generated lane targets, removed both clean owned worktrees, deleted both patch-equivalent
  local feature branches, confirmed no matching origin branches existed, and pruned worktree and
  remote metadata. Only the release worktree remains; the three protected stashes,
  `bundle-backup`, and Dependabot branches remain intact.
- Integrated the independently accepted portfolio revision/resource series unchanged through
  `e468d01`; the fresh consolidated portfolio gate passed 15/15. Reclaimed 1.7 GiB of generated
  target state, removed the clean `.worktrees/portfolio-revision-resource-authority` worktree,
  deleted the merged local product branch, confirmed no matching origin branch existed, and pruned
  worktree and remote metadata. Only the release worktree remains.

The next delivery event is provider/Task 19A acceptance, followed by Task 20's exact-head
production measurements, fuzz/security evidence, full release gate, grouped review, publication,
and repository closeout. The integrated product demonstration is a required internal predicate;
it does not claim that the Market Squawk product release is complete while the external provider
predicates remain open.

## Active Quarter 4 closeout sequence

The provider, research/model, and execution/paper implementation lanes are integrated. Remaining
work is no longer represented as those three active implementation lanes:

1. Run the mandatory unchanged-head provider acceptance. The five-family Treasury daily-rate
   implementation is complete at `50912c18271a0389fb5ac8817555230930dd0506`; the provider run must
   still retrieve, publish, query, and recover fresh official Treasury bodies alongside the
   remaining SEC/BLS evidence, FRED/ALFRED durable-use rights, and Coinbase Direct credential
   inputs. Issues `#7` and `#31` remain open until those predicates succeed.
2. Freeze the Quarter 4 candidate only after the Task 19A predicates close, then run Task 20's
   mandatory single full nonincremental gate, clean-machine demonstration, fuzz/security/
   performance evidence, grouped exact-head review, release publication, and repository closeout.
   These are release requirements, not optional follow-up work.

Task 20 hardening preparation may continue while externally coordinated provider inputs are
obtained, but final provider evidence and every Task 20 exact-head artifact are serialized behind
one unchanged release candidate. Focused work continues with `CARGO_INCREMENTAL=0`; only Task 20
may run the broad workspace gate. The root target remains capped at 20 GiB, and every completed
feature lane must reclaim its generated target and delete its clean local/origin branch and
worktree.

## 2026-07-26 Task 20 release-evidence authority checkpoint

Capability commit `ca3d6b6162f4488da8af8224f983ef2f4a7993e2`, tree
`e4ac5a66f7ec1d5bb6f801db421280faa5405e98`, closes the internal evidence-authority findings found
before the exact candidate freeze. The selected release executable now parent-supervises the sole
checked-in `scripts/verify.sh` gate with an eight-hour deadline, a 16 GiB sampled process-tree RSS
ceiling, log-only 64 MiB file-size enforcement, process-group cleanup, no-clobber output, immutable
input revalidation, and an in-process 20 GiB target-tree measurement. A prewritten log can no
longer manufacture a successful full-gate receipt.

The terminal closer now admits strict fuzz, performance, and full-gate schemas instead of trusting
opaque JSON objects or prose markers. It recomputes workload sizes, operation rates, latency
relationships, queue accounting, memory growth, process bounds, threshold outcomes, storage and
Python row counts, exact fixture/file identities, and ordered timestamps. It also binds the signed
Python release to the selected application binary and revalidates the complete evidence topology,
artifact inventory, executable, verification script, log, and clean repository on both sides of
pending-manifest preparation. Task `19A` is explicitly represented in the ownership map and blocks
Task 20 closure.

Focused verification passed:

```text
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk --lib --bin market-squawk \
  --features release-evidence --locked -- -D warnings
CARGO_INCREMENTAL=0 cargo check -p market-squawk --lib --bin market-squawk \
  --no-default-features --locked
CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib \
  --features release-evidence --locked closing_contract
result: 2 passed
sealed Python source closure: 842 of 842 paths admitted
cargo fmt --all --check; diff and ownership JSON integrity: passed
```

No new integration-test executable, worktree, or duplicate Cargo target was created. Root generated
state is `10,332,448 KiB`, below both the 10 GiB focused-lane budget and the 20 GiB release ceiling;
`.worktrees` is empty and incremental compilation remained disabled.

The capability and checkpoint commits were fast-forwarded through release head
`8a20d4b4c9b9746a7619f17290f00cd336ab2d3c` and pushed. The completed
`fix/release-evidence-authority` branch was then deleted locally and on origin, remote and worktree
metadata were pruned, and the sole checkout is the clean release worktree. PR `#26` and issues
`#7`, `#25`, and `#31` contain the checkpoint evidence; all three issue items remain `In Progress`
on Project 5. Task 20's GitHub dependency record now names Task 19A. No Dependabot pull request or
remote Dependabot branch remains open.

This checkpoint does not close issues `#7`, `#25`, or `#31`. The authorized Coinbase Direct
credential/network trace, provider terms confirmation, unresolved FRED/ALFRED durable-use release
contract, exact-head provider evidence, final fuzz/performance/full-gate evidence, grouped Quarter
4 review, and release publication remain open. At this 2026-07-26 checkpoint, Hosted Actions was
externally blocked before checkout by the GitHub account billing/spending state and had not exposed
a code-owned CI failure.

## 2026-07-26 five-family Treasury and Python release-matrix checkpoint

Implementation commit `50912c18271a0389fb5ac8817555230930dd0506` completes the mandatory
Treasury daily-rate product path. The public, no-credential profile now admits all six durable-use
operations under the pinned Treasury/Data.gov CC0 evidence. The portal activates one bounded year
range containing all five official families. The shipping adapter implements exact year, month,
and all-history queries; strict family-specific XML schemas; checked financial values; canonical
`OfficialDelayed` observations; raw-payload and revision lineage; complete-or-error cross-page
integrity; Arrow/Parquet publication; queryability; and durable restart recovery.

The exact-head provider producer derives its proof year from the active configuration, retrieves
and publishes one common year for every family, verifies each result through
`Macro.GetObservations`, and repeats the authority check after restart. The closer reconstructs
each canonical family/query and binds the report to its exact daily-rate object, request digest,
payload digest, manifest, and lineage. Invented family labels, permuted datasets, Fiscal Data
objects, repeated pages, malformed empty entries, and partial bounded histories fail closed.

The same commit hardens the signed Python release matrix. Closure now proves distinct signed
CPython 3.12 and 3.13 tags and versions, reconciles each environment receipt with the declared
support matrix, binds both roots to the same top-level signed release manifest and selected
application/ONNX worker, and repeats the verification at publication barriers.

Focused verification passed the Treasury adapter's existing consolidated suite, the existing
Treasury profile authority test, the existing modeling content-identity regression, strict
Treasury/modeling/application Clippy, application release-feature compile, Rust formatting,
Python builder syntax validation, and diff integrity. A narrow re-review confirmed all five
material integration findings resolved. No new integration-test executable or worktree was
created. `.worktrees` remains empty and the root target is approximately 11 GiB, below the
20 GiB ceiling.

This checkpoint completes implementation, not the mandatory external proof. Task 19A and release
closure remain blocked until the unchanged candidate exercises fresh official Treasury responses
through this shipping path together with every other required provider predicate.

## 2026-07-26 FRED/ALFRED two-gate authority correction

Current official FRED Services and API terms prohibit storage, caching, archival, database
incorporation, and software or model training. The revision-4 profile is therefore
`rights_limited`, not generally available for durable use. The maintained authority decision is
`docs/research/providers/2026-07-26-fred-alfred-self-hosted-api-authority.md`, SHA-256
`658324385cd927d258890028838b59dde5335a29f82a0346c1f23736abe5668b`.

Durable activation now requires both exact written St. Louis Fed service permission and independent
exact-series authority. The raw Bank response must be matched byte-for-byte against a fresh
application-owned reacquisition from its exact official HTTPS URL and remain bound to an explicit
local review containing reviewer, issuer, grantee, service, exact series, operations, conditions,
effective date, optional document expiry, and finite revalidation. Email headers, a contact
submission, API key, public-domain series, or caller-declared operation list cannot independently
unlock persistence or training. Legacy schema-version-2 recipes decode for recovery but cannot
bypass either current gate.

The adapter, distinct provider/analytical identities, revision-preserving publication path,
restart checks, and PIT/Python acceptance producer remain implemented. They are not releasable for
durable FRED/ALFRED data until both gates and the real official-provider proof pass unchanged-head
verification. Direct BLS `LNS14000000` is the zero-fee durable unemployment-data route and must
retain BLS provenance; true point-in-time vintages require archived BLS release evidence.

Fresh focused verification of the corrected implementation passed:

```text
CARGO_INCREMENTAL=0 cargo test -p market-squawk-adapter-fred --lib --tests --locked
result: 16 passed; one controlled local-evidence test ignored by contract
CARGO_INCREMENTAL=0 cargo test -p market-squawk-sources \
  available_persistence_is_bound_to_exact_current_evidence --locked
result: 1 passed
CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib \
  fred_request_v3_is_closed_and_v2_recovers_as_legacy_owner_permission --locked
result: 1 passed
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-adapter-fred \
  --all-targets --all-features --locked -- -D warnings
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk \
  --all-targets --all-features --locked -- -D warnings
CARGO_INCREMENTAL=0 cargo check -p market-squawk --all-features --locked
result: passed
```

Rust formatting and diff integrity also passed. This verifies the fail-closed local implementation;
it does not supply the external permission or the required working real-data release proof.
FRED/ALFRED remains an open mandatory V1 release blocker; it is not optional, deferred, or complete
merely because its adapter or acceptance producer exists.

## 2026-07-26 Treasury five-family fresh-root provider proof

The shipping CLI binary with SHA-256
`8fb722ace4dc1d5c5fb0f233d3f06c7db53b257c870856d081d9d00d7f769a6d` completed a fresh
official-provider run in `/private/tmp/market-squawk-treasury-final.v8eRP4`. The no-credential
Treasury profile reopened as `active_scoped`, with all five 2025 families and an
`OfficialDelayed` quality ceiling.

| Analytical dataset | Generation | Objects | Rows | Manifest content SHA-256 |
| --- | ---: | ---: | ---: | --- |
| `treasury.daily-par-yield-curve.2025` | 1 | 1 | 3,455 | `16287e151a6fcad2aa792e4d39ef2028a7842b08b34ac9b5ceee79f130e0aae8` |
| `treasury.daily-bill-rates.2025` | 1 | 1 | 6,848 | `7d017269b7d6ff6cdaffd0126910853f7b00d9233cb91ef9c6f4f2e65ce07222` |
| `treasury.daily-long-term-rates.2025` | 1 | 1 | 747 | `d7b204bc9ef702923de562577b2aaea629ff662e8e68c909821e298c51d2d104` |
| `treasury.daily-real-par-yield-curve.2025` | 1 | 1 | 1,245 | `7125de5d5e3b1451428bd56789ca82a8acb348503d1240fbc05a7ed085b715ef` |
| `treasury.daily-real-long-term-rates.2025` | 1 | 1 | 249 | `711ed1a05c3629ef659e838e29a2d27dc0585dbbe54f36cba5264d14657b9017` |

Each official object was rediscovered with the same exact identity and payload digest, then
reingested. Every retry returned the original manifest version, content and lineage hashes, row
count, byte count, and one-object count. A cold reopen recovered all five manifests, and a bounded
DataFusion `COUNT(*)` exactly matched every reported row count. The catalog retained five succeeded
ingest runs, five payloads, five manifests, one generation and one object per family, and no
reserved, failed, duplicate, or second-generation record.

This is successful dirty-candidate behavioral evidence, not final release approval: the worktree
contained concurrent uncommitted release-lane changes. The unchanged clean exact-head provider run
must still repeat and bind this proof through the release evidence producer before Task 19A closes.

## 2026-07-26 bounded provider-research and onboarding checkpoint

Capability commit `c64eb49035115b0805fe8de7493acc8227d802cb` completes the current provider
implementation wave without closing the externally controlled release predicates. The shipping
application now exposes 63 typed CLI/MCP operations, including bounded `Source.Inspect` retrieval
for the active FRED/ALFRED onboarding session. Inspection performs credentialed official-API
retrieval without durable research publication, returns canonical macro observations plus exact
page evidence, enforces page/record/result/cancellation limits, and validates the complete nested
result against a closed descriptor before publication.

`FRED_ALFRED_API_SURFACE_ID` is the single Rust authority for the canonical
`fred-alfred.api-v1-v2` surface across the built-in profile, production activation, ephemeral
inspection, and structured-result schema. Durable FRED/ALFRED authority remains two-gated and
HTTPS-only: the exact imported Bank permission bytes must match a fresh application-owned
reacquisition from the exact official URL, and the selected series must carry independent exact
series authority. Unauthenticated email files or headers are not an admitted permission channel.
The dark onboarding portal presents this boundary directly, retains write-only secret handling,
and keeps durable actions disabled until both gates exist.

The same candidate integrates the bounded BLS, SEC, Treasury daily-rate, provider-rate,
research-publication, restart, point-in-time, Python-admission, and release-evidence corrections
developed in this provider wave. The sealed Python release lock now admits 854 exact source
identities. The maintained FRED/ALFRED authority report has SHA-256
`658324385cd927d258890028838b59dde5335a29f82a0346c1f23736abe5668b`.

Verification on the unchanged capability tree includes:

```text
CARGO_INCREMENTAL=0 cargo test -p market-squawk-adapter-fred --lib --tests --locked
result: 17 passed; one controlled exact-evidence test ignored by contract
CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib --locked
result: 54 passed
CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane --locked \
  research_vertical::registered_provider_discovery_returns_exact_ingestible_object_and_rights_evidence \
  -- --exact
result: passed
CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane --locked \
  production_mcp_composition::shipping_mcp_constructor_uses_the_bounded_sdk_durable_audit_and_controlled_artifacts \
  -- --exact
result: passed
CARGO_INCREMENTAL=0 cargo test -p market-squawk --test control_plane --all-features --locked \
  release_demonstration::usable_release_vertical_requires_explicit_offline_admission -- --exact
result: passed
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-services -p market-squawk-sources \
  -p market-squawk-adapter-fred -p market-squawk \
  --all-targets --all-features --locked -- -D warnings
result: passed
```

Rust formatting, diff integrity, portal JavaScript syntax, Python builder syntax, source-lock
admission, and both requested tracked-phrase scans passed. The final focused staff re-review closed
at zero Critical and zero Important findings. No new integration-test executable or worktree was
created. `.worktrees` is empty and root generated state is `14,839,000 KiB`, below the 20 GiB
ceiling.

This checkpoint makes the workflows runnable; it does not manufacture provider permission,
credentials, or unchanged-candidate external evidence. Issues `#7`, `#25`, and `#31` remain open
until the authorized Coinbase Direct trace, required SEC/BLS/FRED official-provider proof, exact
FRED durable authorities, final unchanged-candidate provider report, Task 20 gate, Quarter 4
review, and release publication actually close. Hosted Actions remains externally stopped before
checkout by the GitHub account payment/spending-limit state.

## 2026-07-26 0.2.0 provider-evidence and immutable-binary checkpoint

Capability commit `ce304b59a79e3bd422fb4ca58a93fc8780bb320b`, tree
`e59391ae85768e95dea8303c3e9a56e6c833b588`, integrates three release-critical corrections:

1. Public BLS v1 constructs one exact adapter-owned configuration, returns its exact provider
   dataset through portal and CLI activation, exposes it through `Source.GetStatus`, and recovers
   it from the callable runtime after process restart. Registered BLS v2 remains a separate
   provisional `refresh_required` surface and cannot replace public v1 in terminal evidence.
2. Treasury Fiscal Data derives the release query from the durable desired recipe and current
   runtime, discovers every bounded official page, ingests every page through the production
   application service, queries the published analytical generation, and repeats the same
   manifest-bound read after restart. Provider evidence schema version 5 binds the canonical query,
   page/request/payload/object chain, row provenance, manifest, lineage, and restart equality.
3. The sealed Python builder compiles the application with the exact release-evidence feature,
   copies all three native executables as independent read/execute-only files into CPython 3.12 and
   3.13 release roots before signing, and signs the canonical CPython 3.12 copies. Every downstream
   provider, fuzz, performance, demonstration, gate, and closer command uses that immutable
   application rather than mutable `target/release` output.

The workspace, internal dependency requirements, Python distribution, native extension, training
environment, lockfiles, documentation, and issue template now consistently identify candidate
version `0.2.0`. The existing `v0.1.0` tag was not moved or reused. The Python source lock admits
854 exact source identities.

Fresh focused candidate evidence passed:

```text
python3 -I scripts/tests/test_build_python_release.py
result: 10 passed

CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib --locked \
  local_product::cli_provider::tests::public_bls_activation_returns_its_exact_discovery_dataset \
  -- --exact
result: 1 passed

CARGO_INCREMENTAL=0 cargo test -p market-squawk-adapter-treasury --lib --locked \
  source::tests::authority_bound_sources_emit_canonical_fiscal_and_daily_rate_records -- --exact
result: 1 passed

CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib --features release-evidence --locked \
  release::close_provider::tests::treasury_fiscal_runtime_requires_durable_publication_evidence \
  -- --exact
result: 1 passed

CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-adapter-treasury --lib --locked -- -D warnings
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk --lib --features release-evidence \
  --locked -- -D warnings
result: passed
```

The complete 854-file source closure, root and fuzz lock/version metadata, workspace boundaries,
Rust formatting, portal JavaScript syntax, Python compilation, tracked prohibited-phrase scan, and
diff integrity also passed. The root debug/test cache reached 19.14 GiB during the version rebuild,
was safely cleaned after confirming no Cargo process was active, and is now approximately 800 MiB;
`.worktrees` remains empty.

This is a verified implementation checkpoint, not final unchanged-head release approval. The
terminal provider report still requires the authorized Coinbase Direct credential trace, truthful
SEC identity and CIK, exact FRED written service permission plus exact-series authority, and fresh
official-provider responses against one unchanged candidate. The complete release evidence block,
Quarter 4 grouped review, publication, and issue closeout remain open. At this 2026-07-26
checkpoint, Hosted Actions was externally blocked before checkout by the GitHub account
payment/spending-limit state.

## 2026-07-28 cross-platform paper-recovery correctness checkpoint

Exact product-capability commit `f8c2569ee4addcfbd8d93553d6b4c541dbdb00ae`, tree
`0a8d5ab177b53d0496d6fecb8672f3262ae8e533`, closes the production paper-recovery sequence
handoff found after the Kraken verticals adopted the shipping multi-thread Tokio scheduler.
Startup recovery now waits for the short shared sequence critical section only inside its existing
cancellation and deadline. Live and dispatcher producers remain nonblocking. No deadline, retry,
serialization, queue, or assertion was weakened.

Local verification on the clean unchanged code head passed the existing paper-adapter library
suite (15 of 15), the complete application library suite (56 of 56), strict affected-package
Clippy, formatting, Python source-closure admission for 862 exact source identities, generated
artifact inspection, and diff hygiene. The existing typed Kraken-selection fixture now owns an
isolated temporary data root and no longer leaks SQLite control state into the source tree; no test
or test target was added.

Hosted Actions
[run 30366976240](https://github.com/Sawmonabo/market-squawk/actions/runs/30366976240)
then completed successfully at that exact head:

| Job | Duration | Result | Retained log SHA-256 |
| --- | ---: | --- | --- |
| Linux `verify` (`90300620390`) | 49m20s | Passed complete `scripts/verify.sh` | `cd099473c99177d1b56126def9c57bb1ff6395d93bd7e80a23d4f60edd1dfc45` |
| Windows (`90300620276`) | 15m19s | Passed complete locked workspace test job | `2ebd8dee2601a747ebfc823887d2a77485c5945761f57d6980e8d15f3bb5b0ce` |
| macOS (`90300620453`) | 25m50s | Passed complete locked all-feature workspace test job | `9ff06329b7ecbc7193e82dda32b919b6d067f5e60ac630561093fc0682058004` |

Run metadata SHA-256 is
`25b4d876a1d3afab388979e3f5e72c182a8bd5039ed252caa03f668144b860dd`.
The detailed causal record and primary sources are maintained in the
[CI verification runtime diagnosis](../research/2026-07-27-ci-verification-runtime.md) and its
[evidence audit](../audits/2026-07-27-ci-verification-runtime-evidence-audit.md). Raw hosted logs
remain transient working evidence and are not tracked.

The root generated target is 6.5 GiB, `.worktrees` is empty, incremental compilation remains
disabled, and 153 GiB is free. PR `#26` remains the sole open pull request. Issues `#7`, `#25`, and
`#31` remain open until their actual provider and terminal-release predicates close. This
cross-platform correctness checkpoint does not close those predicates, does not implement the
proposed CI sharding/cache redesign, and does not transfer exact-head evidence to a later commit.

## 2026-07-28 approved desktop-interface baseline

The Obsidian Signal desktop-shell and guided-setup design is approved at audit base
`cfb902b007f66b49b366b3e7f5d03a640e11f9aa`. The canonical specification is
[`2026-07-28-market-squawk-obsidian-signal-interface-design.md`](../superpowers/specs/2026-07-28-market-squawk-obsidian-signal-interface-design.md);
its digest-bound tracked PNG preserves the exact accepted visual baseline.

The required implementation outcome is one permanent Tauri 2 product shell, protected loopback
browser fallback, and first-class CLI/headless route over shared Rust application services. The
approved shell uses shadcn/ui `new-york-v4/sidebar-07` structure, the recorded permanent
navigation, the Obsidian Signal visual tokens, accessible responsive behavior, bundled local
assets, and least-privilege typed commands. Setup must guide a non-specialist through the complete
supported product without hard-coded readiness or duplicated business authority.

This is independently persisted approved design evidence, not implementation or release approval.
The desktop shell and complete guided setup remain release-blocking product work. Before code
changes, the implementation owner must pass the specification's accepted-head refresh gate and
record the resulting dependency/ownership lane without moving or weakening the existing provider
and terminal-release predicates.

## 2026-07-28 Obsidian Signal implementation lane

The approved desktop-interface refresh gate passed against accepted integration head
`dbc909eeb1ca334ae114947158a875fdda3d27d8`. Tauri 2 and the selected maintained frontend
foundations remain compatible with the supported platform/toolchain baseline. Current composition
confirms that the desktop can reuse `LocalProduct`, the closed `Application` operation registry,
and the existing durable provider-onboarding and activation authorities without introducing a
second backend.

Implementation is owned by one serialized product lane,
`feature/obsidian-signal-desktop`, with one worktree at
`.worktrees/obsidian-signal-desktop`. Its complete dependency-ordered plan is
[`2026-07-28-obsidian-signal-desktop.md`](../superpowers/plans/2026-07-28-obsidian-signal-desktop.md).
The lane owns the nested Tauri app, React presentation, root manifests and lockfiles, the narrow
presentation bridge, browser-fallback visual reconciliation, affected-path CI, maintained
documentation, and release-gate integration. It must not split these shared hotspots across
parallel branches.

Verification is intentionally thin during implementation: one frontend test file may protect only
accessible navigation, authority-derived readiness, and fail-closed mutation behavior. Focused
package/type/build checks run within the lane; the broad unchanged-head workspace and platform
gates run once at the grouped Quarter 4 checkpoint. Issue `#36` remains the implementation and
release-blocker tracker until native packaging, browser/CLI continuity, and exact-head evidence are
complete.

### Implemented lane checkpoint

The pushed feature history currently consists of:

- `940c9a4` — the nested Tauri 2 application, locked React presentation, permanent Obsidian Signal
  shell, guided provider setup, protected browser fallback styling, and three critical frontend
  behaviors;
- `9f6a4e2` — the closed, bounded presentation authority over `LocalProduct`, the read-only
  application registry, and the existing confirmed provider-onboarding services; and
- `85cdf07` — affected-path CI classification, desktop package jobs, release-gate frontend
  verification, exact third-party notice handling, and the locally patched upstream GLib
  soundness fix with provenance.

The implemented desktop uses five window-scoped Tauri commands, bundled fonts and assets, a strict
content-security policy, normal local configuration precedence, application-owned readiness, and
bounded shutdown. It preserves the complete CLI and local stdio MCP as first-class headless
interfaces. Coinbase public, Coinbase Exchange direct, and Kraken setup use the native guided
flow; research-provider setup reuses the protected loopback browser workflow.

Focused evidence at `85cdf07` includes a frozen pnpm install, all three frontend behaviors,
TypeScript compilation, Vite production output, Rust formatting, strict desktop/application
Clippy, locked offline desktop compilation, workspace-boundary and generated-artifact policy,
dependency/license/advisory review, credential scans, and the refreshed 958-source Python release
closure. The desktop worktree target measured 9.1 GiB, below the 20 GiB ceiling.

This checkpoint does not accept a desktop release. Its remaining barriers included one inspected
local Apple Silicon application/DMG build, successful hosted package jobs for Linux, both macOS
architectures, and Windows, signed installation evidence, the clean unchanged exact-head release
gate, and the grouped Quarter 4 review. Issue `#36` and its Project 5 item remain open until the
current predicates close and the integrated lane is cleaned up.

The current lane candidate subsequently completed the focused Apple Silicon package check:

| Evidence | Result |
| --- | --- |
| Release build | Completed in 9m05s with `CARGO_INCREMENTAL=0` and one worktree-local target |
| Application bundle | 88 MiB, arm64, identifier `com.marketsquawk.desktop`, version `0.2.0` |
| Application executable SHA-256 | `5aead5b6b1773e89441c0fd406bfcd38d99aeea6af99d8388d1febcd57fcbc04` |
| DMG | 34 MiB; `hdiutil verify` passed |
| DMG SHA-256 | `4763907e7dd0a38428c118c13ed2515d2c5fc40994a52d52f10de14a16aeeb5c` |
| Bundled resources | Project Apache-2.0/MIT licenses plus Tauri/GTK and tract notices |
| Launch evidence | Opened from the application bundle against a fresh temporary root, created the local catalog/control layout, and completed bounded shutdown |
| Signing state | No developer-identity signature or notarization; the linker-created ad-hoc Mach-O signature is not distribution signing |
| Generated storage | 13 GiB after packaging, below the 20 GiB ceiling |

This is focused working-tree package evidence, not unchanged-commit or cross-platform release
acceptance. The local Apple Silicon build barrier is closed; hosted package jobs, signed
installation evidence, the exact-head gate, and grouped Quarter 4 review remain.

### Quarter 4 desktop review and remediation

The first grouped Quarter 4 review audited pushed candidate
`03783250a1020d79cdd7f8bda424da62568dd3d5` against release base
`95d6792e5cae38b5ec829061451a95886e4b2ad2` and rejected release approval. Its substantiated
findings require:

- an operating-system native installed data default rather than a relative launch directory;
- authority-derived setup completion, recovery, navigation admission, and durable resume;
- complete native packages containing the exact CLI, capture helper, and ONNX worker siblings;
- action-specific validation for provider responses and awaited credential continuations;
- exact pull-request-head checkout and artifact identity in CI;
- affected-path classification for direct package license/vendor inputs;
- the exact Geist OFL notice; and
- platform-correct command-palette shortcut text.

Hosted run
[`30418056063`](https://github.com/Sawmonabo/market-squawk/actions/runs/30418056063)
completed every scheduled lane successfully but checked a synthetic pull-request merge commit. It
is cross-platform defect-detection evidence only, even where its source tree matches the candidate,
and cannot approve the exact feature head.

The active remediation keeps the five-command authority boundary and the one-file/three-test
frontend limit. It uses Tauri's application-local data resolver, a package-only configuration
overlay, target-triple external-program staging, the existing Rust onboarding/model/capture
authorities, and one shared navigation-admission function. Current upstream decisions and caveats
are preserved in
[`2026-07-28-tauri-packaging-and-runtime-boundaries.md`](../research/2026-07-28-tauri-packaging-and-runtime-boundaries.md).

Focused remediation evidence from the dirty feature worktree is:

| Evidence | Result |
| --- | --- |
| Frontend | Production build passed; the single test file passed all three critical cases |
| Rust | `cargo fmt --all --check` and focused strict Clippy passed for platform, application, and desktop packages |
| Application bundle | 195 MiB allocated; desktop, CLI, capture helper, and ONNX worker are ARM64 regular executables |
| Sibling identity | Every bundled sidecar matched its staged release binary byte-for-byte by SHA-256 |
| Notices | Project licenses, exact Geist OFL notice, Tauri/GTK notice, and tract notice are present |
| DMG | Mounted read-only; the application and all four executable hashes matched the inspected bundle |
| Native default | A launch from `/private/tmp` with an isolated home created state only under `Library/Application Support/com.marketsquawk.desktop` |
| Signing state | The package was built with `--no-sign`; no signing, notarization, or installed-release approval is claimed |
| Generated storage | 16 GiB after release packaging and focused Clippy, below the 20 GiB ceiling |

This evidence establishes the corrected local package shape and runtime path only. The next barrier
is one remediation commit and push. The same Quarter 4 reviewer then closes the existing findings;
a clean unchanged exact-head hosted gate follows once. Issue `#36`, draft PR `#37`, and the
Project 5 item remain open and in progress until those outcomes and the separate
signed-installation predicate are actually complete.

Pushed remediation `be8619bfe693eb12ccdcc6477c0b92ae46248250` started exact-head hosted run
[`30423427243`](https://github.com/Sawmonabo/market-squawk/actions/runs/30423427243). Classification
and policy passed, but Linux release verification correctly rejected two stale content identities
in the complete Python release source closure: the changed application executable-admission module
and platform configuration module. The expected path set remained complete and all 956 other
source records matched. Remediation updates only those two `sources` size/SHA-256 records, as
required by the existing source-closure invariant; dependency artifacts, interpreter coverage, and
platform policy remain unchanged. The previously failing focused admission contract now passes.

The resulting desktop candidate also closes the remaining semantic and package findings without
expanding the five-command WebView boundary or the one-file/three-test frontend limit:

- Markets readiness requires an active exact Coinbase public, Coinbase Exchange direct, or Kraken
  live-market surface.
- Research and Portfolio readiness require their complete application operation contracts; import
  history is optional information rather than authority.
- Paper readiness requires the exact eight-operation production `Bot`/`Execution`/`Risk` contract,
  starts stopped, remains paper-only, and is independent of the diagnostic-capture
  `paper_bot_enabled` setting.
- MCP readiness requires the installed CLI sibling and complete bounded tool contract. Installed
  packages emit durable client instructions, and Linux AppImage uses a hidden typed pre-Tauri
  `exec` dispatch through the durable outer image.
- Linux package preparation verifies five immutable AppImage-tool identities by owner, type,
  length, and SHA-256 before Tauri can execute them. Both locked font families carry their exact
  license notices.

The final focused Apple Silicon package evidence is:

| Evidence | Result |
| --- | --- |
| Application bundle | 195 MiB allocated; identifier `com.marketsquawk.desktop`; version `0.2.0` |
| Desktop executable | 91,988,880 bytes; SHA-256 `ce228e88c5c39fc30f1cd1256295416923fee8b5dcd60e6377ac6d0bf39cd254` |
| CLI sibling | 96,717,264 bytes; SHA-256 `8761d9b9a2cd89c98a228d77ddeee476dd6d9c39bba7dcb42f37371d58e66318` |
| Capture helper | 561,536 bytes; SHA-256 `831022cbd45bf593e2d73b09d239c52eeb227c6d2b4bdf10cd0fdb95e0bb2072` |
| ONNX worker | 15,298,816 bytes; SHA-256 `3276f191c4d6e375a086a1202f5d6f884ef95b3e65b5eecddbb3752876e88c05` |
| DMG | 78,943,301 bytes; SHA-256 `54fc42be4a4389ac6ab163e71718899c4c2ca526876cfbd7caf881d4ddd0a86f`; `hdiutil verify` passed |
| Mounted image | All four executables and both exact Geist notices matched the inspected application byte-for-byte |
| Fresh launch | Created the controlled catalog, artifacts, journal, provider-rate, source, and portfolio layout before bounded termination; stdout/stderr remained empty |
| Signing | Built with `--no-sign`; the linker ad-hoc signature is not signing or notarization evidence |
| Generated storage | 17 GiB, below the lane's 20 GiB ceiling; 132 GiB free |

The independent Quarter 4 reviewer found no remaining substantiated Critical, Important, or Minor
semantic finding on the current tree. That is not exact-head approval until the tree is committed,
pushed, clean, unchanged, and re-identified by the reviewer. The next barrier is that exact-head
closure plus the hosted native-package and release results. Complete guided native bootstrap,
uv/managed-Python installation, and signed installation evidence remain mandatory release blockers.
Issue `#36`, draft PR `#37`, and the Project 5 item therefore remain open and In Progress.

## 2026-07-29 complete installation hosted checkpoint

Draft PR `#39` and issue `#38` now own the complete-installation and public-release lane, stacked on
the accepted desktop candidate in PR `#37`. Candidate
`775e21da52a8eb08d812bee01e172f55ad93e7ef` includes the immutable Rust installer lifecycle, sealed
CPython 3.14/PyArrow product, complete platform bundles, Tauri embedding, four-platform package
matrix, stable-release transaction, and real dashboard data/MCP exploration.

[Hosted run 30487393236](https://github.com/Sawmonabo/market-squawk/actions/runs/30487393236)
completed without cancellation. Windows and macOS workspace jobs passed in 17m11s and 25m35s, and
the complete Linux release verification passed in 69m10s. The four package jobs did not provide
release approval:

- Windows exposed a Unix-only release-cleanup call after 61m34s.
- Linux rejected the measured 1.62 MiB complete manifest against an obsolete 1 MiB ceiling after
  73m26s.
- Apple Silicon installed and repaired the complete product, then correctly rejected build-only
  environment variables leaked into the runtime smoke after 97m09s.
- Intel macOS lost hosted-runner communication after 81m07s without reporting a product failure.

The next frozen candidate corrects the three deterministic boundaries without weakening product
admission: Windows uses its supported no-follow/reparse-point and read-only cleanup contracts, the
per-platform manifest ceiling is consistently 8 MiB across every producer and consumer, and
installed-product smoke removes only the four explicit build-only environment keys. Focused
installer tests, desktop Rust compilation, frontend type checking, the existing Python release
contracts, workflow policy, YAML parsing, formatting, source-lock identity, workspace boundaries,
and generated-artifact checks pass locally.

The maintained CI runtime report now records both the exact repository timings and current
industry context. Ordinary pull-request feedback is targeted at 10–20 minutes, platform proof at
30 minutes, and the complete frozen-release build is a separate measured workflow rather than an
ordinary change loop. The independently audited zero-cost distribution policy also removes paid
signing credentials as a prerequisite for the core release and requires truthful per-artifact
trust evidence.

Issue `#38`, PR `#39`, and the Project item remain In Progress. The next barrier is one unchanged
hosted run of the corrected exact head, followed by implementation of the accepted no-cost
release-trust policy, publication of real assets, installed public-endpoint verification, grouped
Quarter 4 acceptance, merge, and branch/worktree/cache closeout.

## 2026-08-12 Federal Reserve Board H.15 dashboard checkpoint

Product checkpoint `66c989b23daa63b7c06542d207b67c64d02845a3`, with lifecycle correction parent
`faa784c94fd11708a1d2f3cb02af389dacf66a5f`, advances the installed-product candidate from an
evidence-bound dashboard contract to a bounded installed producer-to-consumer vertical. The active
product dataset is the exact `Output.aspx` rolling response with lowercase `lastobs=100`: 100 dates
by the eleven admitted Treasury constant-maturity series, or exactly 1,100 observations. The doctor
remains a separate ten-date readiness contract. The exact full-history `Download.aspx` identity is
preserved but fails closed with `PartitionedExtractionRequired` because its 179,311-observation
2024 response cannot fit the indivisible 100,000-record/64 MiB publication boundary; future full
history requires partitioned, checkpointed, resumable ingestion rather than raised bounds.

The rolling contract digest is
`339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f`. Its provider dataset is
`federal-reserve-board:h15:h15-treasury-constant-maturities:339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f`;
its analytical dataset is
`federal-reserve-board.h15.h15-treasury-constant-maturities.339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f`.

The Desktop does not select a provider dataset, series set, cutoff, maturity order, revision, or
financial arithmetic. The application derives those from the frozen Board contract and returns
exact decimal strings or explicit provider missing states. The wire keeps the bounded pinned-query
result digest separate from the final typed-selection digest and keeps durable publication
readiness separate from current provider-runtime readiness.

The existing installed control-plane journey now proves, without a new test target:

- revision-4 no-key onboarding and the exact eleven-series/ten-date doctor;
- one durable shared-rate refusal followed by admission after the governed 60-second advance;
- one rolling production discovery, rich capture, `MSJ1` seal, catalog publication, immutable
  Parquet manifest, and a 1,100-row bounded history artifact;
- the closed `Macro.GetDashboard` output in canonical maturity order, including an exact latest
  20-year `ND` state while preserving an older observed value;
- clean installed shutdown and same-root reopen with stable manifest, object, artifact, and
  dashboard evidence; and
- zero provider HTTP calls after restart.

The exact focused command passed 1/1 with 31 filtered cases. The existing server-resolved portfolio
candidate proof also passed after the lifecycle cut. Desktop TypeScript compilation and the existing
grouped Research journey passed with the exact rolling provider/analytical identities. Rust
formatting and diff/whitespace checks are clean. No CI or broad workspace suite ran at this
checkpoint; generated Cargo output is approximately 16.5 GiB after the final focused journey,
below the 20 GiB ceiling.

One separately authorized direct probe of the exact rolling URL returned HTTP 200 `text/csv`, 8,627
bytes, SHA-256 `5c7bd008c221e1b33b6a865cf7d1bbb4620661f57908a4e7dd00822bf8104579`, and exactly 100 dates/1,100
cells. That validates the current official response shape but is not an installed-service smoke.
The real-network installed barrier subsequently closed at exact source head
`ba95a954883d4feb3dd328b40019682998c0b8e7`. The rebuilt CLI and service binaries were exercised
against fresh, separate installation and workspace roots. The run completed the real no-key doctor,
proved an immediate rolling discovery refusal under the durable one-request-per-minute authority,
waited for the natural window, retrieved the exact 8,627-byte official response, and published its
1,100 observations. The manifest content hash was
`9904df0db93ef299d853b13d517d7ec2cb7109908770973df3097f1f4704b915`; the single raw object was
26,994 bytes with content-addressed SHA-256
`c51523f076698c74bbdef30841474541a81a6e0a80c4b25bdc721ef54e596abc`; and the single Parquet
object was 295,309 bytes with SHA-256
`516406c1ca4c85bdcad8e0d7075a04ebcd29efccdb0d4bb0df65c0e3adc9413e`.

An authenticated MCP `Macro.GetDashboard` read returned all eleven ordered maturities for
2026-08-10 with exact decimals, record provenance, immutable publication evidence, pinned result
digest `8a7716bf6f4c8d8e1138e57f7b020613b5d3a7f2216dd6138de541562331602a`, and a separate final typed
selection digest. The service then stopped cleanly and reopened the same workspace at generation 3.
A local-only dashboard read returned the same manifest, object graph, pinned result digest, source
payload identities, dates, revisions, and values; only the query and selection identities that bind
the fresh evaluated-at cutoff changed. No post-restart source operation ran, and the raw/Parquet
object counts and hashes remained unchanged. All temporary product processes were stopped after the
proof. No CI or broad suite ran; the exact binary build left `target/` at approximately 19.9 GiB,
below but close to the 20 GiB ceiling.

Native Tauri/WebView package acceptance remains later release evidence. The broader product still
lacks its guided Find/Analyze producer, current Investment Brief and track-record Desktop wiring,
governed recommendation-to-user-target handoff, several selected-provider publication/PIT/Desktop
verticals, and the final unchanged-head Quarter 4/release gates.

## 2026-08-12 selected-candidate analysis and Investment Brief checkpoint

Pushed checkpoint `ca3901b520bb91e74a60f1d8f73d5feab722dbfc` closes the next generic
analysis-evidence barrier without introducing a backend-owned guided/default workflow. The backend
continues to expose independently composable capabilities and immutable results; the Desktop Tauri
controller remains the owner of the opinionated Market Squawk Default V1 profile and eventual
multi-step Find/Analyze orchestration.

The decision authority now retains a complete selected-candidate binding rather than only an
instrument or proposal coordinate. Its identity includes the exact immutable SavedScreen policy,
screen revision and universe, as-of semantics, ordered predicates and null policies, ranking,
result bound, admitted quality constraints and complete feature-semantic closure, as well as the
exact ScreenRun, candidate rank/score/contributions, coverage, liquidity, portfolio revision,
flags, and evidence identity. A selected-candidate analysis can be published only as one prepared
bundle containing the proposal decision, publication, selected-candidate evidence, and immutable
typed explanation. The application journal persists that bundle as one strict version-4 record;
standalone proposal persistence rejects selected-candidate analyses so the binding cannot be
silently omitted.

The prepared append path stages every fallible validation before mutation, writes the durable
record before committing the staged in-memory repository state, and poisons the authority on an
impossible post-journal divergence. Recovery requires the exact SavedScreen and ScreenExecution to
appear before the bundle, reconstructs and revalidates the selected-candidate evidence, and rejects
out-of-order, partial, or mismatched public results. There is no legacy v3 compatibility reader or
migration: this unreleased greenfield wire was updated in place to singular v4.

The same checkpoint also adds generic research-only prerequisites for a future producer:

- an exact-horizon conditional-mean price forecast projection with complete 50/80/95 calibration,
  model, artifact, vintage, availability, expiry, and newest-valid selection evidence;
- a strict recommendation-outcome signal-plan materializer over a complete paired subject and
  benchmark PIT population, three non-overlapping two-year folds, conservative execution costs,
  one-lot simulation quantities, complete caller-authorized Entry/NoAction/Unavailable evidence,
  and fixed work bounds;
- complete imported-portfolio/current-market analytical prerequisites with exact selected-source
  marks and depth, exact-decimal historical 95% VaR/expected-shortfall authority, checked risk and
  side-aware liquidity capacity, and repeated portfolio/market rechecks;
- a generic evidence-derived market-reference identity approval that joins exact Nasdaq listing,
  OpenFIGI mapping, canonical definition, coverage, rights, currentness, and expiry without
  hard-coding a benchmark, ticker-derived UUID, currency, or consumer asset-class policy; and
- pure automatic DCF, comparable, residual-income, and forecast-distribution calculation receipts
  over genuine PIT valuation inputs and rights evidence. These calculations deliberately do not
  claim to be governed `ValuationMeasurement`s yet; a separate evidence-origin/measurement adapter
  remains required before classification, approval, or latest-valid selection.

The Desktop Investment Brief now strictly admits the current complete
`Decision.GetInvestmentAnalysis` response, including execution ineligibility, publication,
projection, sizing, and realized-outcome sidecars, and cross-binds them to the generated proposal.
It also invokes `Decision.GetRecommendationTrackRecord` with the exact publication profile,
policy horizon, and one server-coordinate cutoff, then renders the complete fixed six-cohort
envelope. Integer time coordinates cross the WebView boundary as canonical decimal text and are
parsed in Tauri. React performs no financial calculation, account inference, dataset selection, or
evidence authorship.

Focused checkpoint evidence was:

| Evidence | Result |
| --- | --- |
| Rust formatting and whitespace | `cargo +1.97.1 fmt --all -- --check` and `git diff --check` passed |
| Serialized application compile | `CARGO_INCREMENTAL=0 cargo +1.97.1 check --locked -p market-squawk --lib` passed; only the existing warning backlog remained |
| Atomic decision/restart proof | Existing `control_plane` decision-persistence case passed 1/1, with 31 filtered cases |
| Desktop compile | `pnpm --dir apps/market-squawk-desktop typecheck` passed in the frozen Desktop lane |
| Desktop critical journey | Existing grouped product-navigation case passed 1/1, with 6 skipped cases |
| Storage hygiene | Reproducible `market-squawk` Cargo artifacts were reclaimed after the proof; `target/` returned to approximately 9.2 GiB, below the 20 GiB ceiling |

The focused decision case forces a SQLite rejection of the prepared bundle and proves that no
proposal, publication, or bundle row becomes visible. It then publishes the exact bundle, proves
idempotency and conflict handling, reopens the same decision store, and verifies the complete
SavedScreen/candidate/proposal/publication/explanation/projection identities. This is lane evidence,
not clean exact-head release approval or a substitute for the unchanged-head gate.

The branch was clean and upstream-aligned at `ca3901b5`, and the checkpoint was recorded on draft
PR `#43`. No CI, broad workspace suite, release matrix, merge, or publication ran.

The active next barrier is a generic application-owned PIT feature-dataset producer. It must derive
features and labels from exact admitted research parents rather than accept caller-computed values,
preflight and reauthorize rights, select a complete source-authored universe, pin all market
definitions as of the knowledge cutoff, prove one evidence-backed completed market session, and
publish a required immutable production receipt alongside the existing dataset generation. Only
after those authorities freeze can the Desktop controller truthfully start/resume Find and Analyze.
Provider-specific investment workflows, recommendation-to-governed-target adoption, explicit paper
draft confirmation, native package acceptance, grouped Quarter 4 review, and exact-head release
gates remain open.

## 2026-08-12 feature-product authority remediation checkpoint

Pushed checkpoint `215f27582d596707ae6e04a536b5c6e3aac00fc0`, tree
`d134d38071f1ee314f9a7b233b5f07723598e74f`, integrates the reviewed research/data candidate. It
separates caller-materialized phase-one analytical generations from product admission, removes the
raw-generation bypass from `Analysis.GetFeatureDatasets`, and adds a non-cloneable, session-bound
publisher for the closed price-return/fixed-horizon-forward-return Analysis and Training contracts.
The final catalog transaction revalidates exact source roots, use, output rights, generation
objects, contract, producer proof, descriptor, and receipt, then publishes the descriptor and
canonical receipt atomically. Exact historical manifest reads remain selectable after a successor
version, while relocated catalogs cannot re-fence a receipt bound to another catalog endpoint.

Generic `dataset build` and `feature build` operations now report an immutable
`phase_one_derived_generation`. Their operation result truthfully states that the phase-one
operation did not itself admit a product. Live generic Research dataset reads make the narrower
claim that product admission is not established on that surface; receipt-backed product status is
owned by `Analysis.GetFeatureDatasets`. FRED/BLS provider release evidence retains raw publication,
query, restart, and Train-rights facts but no longer counterfeits Python training from a generic
phase-one descriptor. The provider closer therefore remains explicitly blocked until a real
code-owned Training-contract producer receipt exists.

Focused checkpoint evidence is intentionally thin:

- the existing data recovery test proves phase-one invisibility, atomic publication/replay,
  Analysis/Training isolation, exact v1 selection after v2, same-root restart, bounded backup
  verification, and fail-closed rejection of a distinct-root restore whose retained catalog
  endpoint does not match the live endpoint;
- the existing control-plane backtest case proves a phase-one generation remains queryable but is
  absent from the product registry, while the schema-v3 pinned backtest admission remains valid;
- focused application/modeling/Python compilation, the exact backtest case, and the exact provider
  release blocker passed; and
- no CI, broad workspace suite, release build, package matrix, or source-lock refresh ran.

The grouped staged-candidate review initially rejected seven Important findings. Remediation closed
the live product-state wording, catalog endpoint binding, operator documentation, contract/use
pairing, raw-versus-prepared provenance, exact-qualification bypass, and realized-target currentness
findings. Forecast product selection is deliberately unavailable for current vintages: the only
closed producer emits forward returns, while the prior exact-price path lacked a separately admitted
Analysis dataset, a governed return-to-price/current-mark calculation, and a sealed prepared-vintage
provenance chain. Raw and currently prepared forecasts remain research artifacts and cannot become
recommendation-facing price evidence. The final grouped staged-index review accepted the exact
64-path candidate at Critical 0, Important 0, Minor 0. Its binary patch SHA-256 was
`053a26646511fe664880602bf118fe5d085a2e76ac0e182badd1ed7039804d8a`, and the committed diff has
the same digest. The focused final app check, exact forecast selection test, exact output-contract
test, exact publication/recovery test, and unchanged earlier lane gates all passed; no broad suite,
CI, release build, package matrix, or source-lock refresh ran.

After that checkpoint, the active product barrier is still the installed private producer. It must
reconstruct retained completed-session evidence from the exact immutable manifest/capture graph,
move the sole production publisher into the code-owned recipe, publish genuine separate Analysis
and Training products, and retain their pairing/derivation receipts. Governed terminal-price
forecasting, valuation, recommendation/targets, portfolio/risk, Alpaca Paper/IEX doctor and runtime
activation, and the novice Find → Analyze → forecast/backtest → signal → paper journey remain open
until those producer authorities exist. `.worktrees` remains empty. The subsequent protected
Desktop credential-import checkpoint is recorded below.

## 2026-08-12 protected Desktop credential-import checkpoint

Pushed checkpoint `f1dafac589cbcf4feb66d478bfdf2fece6ee642c`, tree
`ead687bc2e00d1f5a484842f9713b544a36e340f`, closes the installed WebView-to-service credential
origin boundary without creating provider availability. The main window can select one confirmed
`.env` file through the native picker; native code opens only a non-empty regular file with
no-follow semantics, enforces the 64 KiB ceiling, hashes the opened descriptor, stages it under one
generation/workspace/client-bound opaque ticket, and immediately consumes that ticket through the
existing private `Source.ImportCredentialBundle` operation. Path, bytes, digest, ticket, service
envelope, and unexpected fields never cross into the WebView.

Native and TypeScript layers independently require the exact closed schema, all 17 providers in
code-owned order, the four admitted dispositions, and consistent enabled state. The UI renders only
redacted setup dispositions and explicitly says that import does not verify, activate, start,
schedule, publish, or trade. A cancelled picker truthfully leaves setup unchanged. Any non-cancelled
failure warns that earlier provider entries may already have been stored, invalidates the source
authority domain, and refreshes status, coverage, health, and retained-manifest evidence before the
user retries.

Focused checkpoint evidence was intentionally limited to the authority boundary:

- Rust formatting, capability JSON parsing, cached diff integrity, and the locked offline Desktop
  native check passed;
- Desktop TypeScript compilation passed;
- the existing grouped product-navigation journey passed 1/1 with six skipped cases and proves
  cancel, redacted success, strict rejection of an unexpected secret-like field, truthful warning
  that earlier entries may have been stored, and a source-status refetch after the failed result;
  and
- no broad suite, CI, release build, package matrix, provider network request, or source-lock
  refresh ran.

The final 13-file candidate was independently accepted at Critical 0, Important 0, Minor 0. Its
reviewed and committed binary patch SHA-256 is
`2c646a1a79b430d87aa6cf3acf6dcf74bd32df23a28f7656e91bbbaa15a3d90a`. Product code was clean and
upstream-aligned at that checkpoint, and the checkout still has one worktree; this ledger update is
the only subsequent overlay. Import is only Configured or Probe-required evidence. The next barrier
is a read-only Alpaca doctor that durably binds the exact paper-realm credential generation and
non-trading account identity to IEX market-data endpoint/feed, batch/cardinality,
historical-bars/calendar entitlement, and rate-capacity evidence. It must never call or authorize
account, position, order, or trading routes; only after that evidence is current may the existing
source-start authority create an IEX market-data runtime.

## 2026-08-13 Alpaca Paper/IEX doctor and source-runtime checkpoint

Pushed checkpoint `9c1be5fded3b87b055cdaa50297bb80617046b4c`, tree
`506581da9603877ef88515475fcb5ef62541f6f2`, closes the first selected-market-data authority
barrier without claiming a live external-provider result. The installed product now owns a closed
five-probe Alpaca Paper/IEX doctor covering the fixed quote, exact 50-symbol snapshot batch, IEX
WebSocket authentication/subscription acknowledgement, terminal raw-history pagination, and exact
Paper IEX/UTC calendar reconciliation. Its provider-observed result is nonconvertible from the
installed scripted fixture. The durable receipt binds the exact credential generation, non-trading
market-data principal, profile/configuration/rights/rate identities, complete observation digest,
fifteen-minute exclusive validity, and same-generation renewal predecessor.

`Source.Verify`, `Source.Start`, restart restoration, expiry, renewal, resynchronization, and
shutdown now retain exact receipt/configuration/generation authority. Alpaca, Tradier, and Kraken
account runtimes begin with display reads closed; final publication and read admission occur while
the registry, onboarding mutation guard, and entry authority remain coherently held. Weak-only
currentness monitors revoke reads before cancellation, generation-CAS health drains remove and join
only the stale generation, every shutdown has a finite code-owned deadline, and historical Alpaca
capabilities require the exact runtime receipt plus credential generation. A same-generation
renewal accepts an already-drained prior runtime as idempotently stopped, while every present entry
still receives complete request validation. Failed or expired post-start transitions clean the
exact runtime under a fresh product-owned deadline before durable reconciliation.

Catalog migration 0016 remains immutable. Forward migration 0022 adds exact per-session onboarding
stream heads and performs a bounded Rust backfill inside the migration transaction, including
zero-event retained sessions. Replay validates canonical reservation, audit, event, deadline,
lifecycle, and cumulative-chain evidence and applies trusted current-time deadline semantics before
returning an exact replay. Desktop Sources strictly renders the server-owned doctor evidence and
closed Verify/Start/Resynchronize/renewal controls; setup copy states that doctor success neither
starts a source nor grants trading authority.

Focused checkpoint evidence remained deliberately thin:

- the locked offline application compile passed on the frozen source candidate;
- the existing exact source receipt/renewal test passed;
- the three existing application tests for cancellable monitor join, exact historical
  receipt/generation mismatch, and stale-generation drain CAS each passed;
- the existing catalog replay/migration test passed, including the retained zero-event stream;
- Desktop TypeScript compilation and the existing grouped product-navigation journey passed; and
- no broad workspace suite, CI/CD, release build, native package matrix, provider network request,
  or source-lock refresh ran.

The closing grouped remediation review accepted the frozen working source at Critical 0,
Important 0, Minor 0; its 28-file reviewed aggregate SHA-256 was
`d37cb8a4858ba8139e2c5d827a73035038f2ea91c0751ed86d25c678a988cb89`.
That is focused checkpoint evidence, not clean exact-head release approval. The branch and upstream
matched at the pushed commit, the sole worktree was clean, no completed worktree or lane branch
remained to remove, and generated `target/` state was approximately 13 GiB under the 20 GiB ceiling.

The active barrier is the first honest installed Markets producer-to-Desktop vertical: a
credential-free, network-denied AAPL/IEX fixture must traverse the real Alpaca decoder, bounded
live-source supervisor, display directory, `Market.GetUnifiedFeed`, and existing Markets UI while
remaining visibly `InstalledFixture` and `DirectUnverified`. The fixture cannot infer a canonical
instrument from `AAPL`, fabricate Nasdaq/OpenFIGI/FIGI evidence, reuse a production doctor receipt,
or obtain account, historical-provider, order, or trading authority. The next integration event is
an exact retained AAPL reference definition plus the sealed fixture runtime and one existing Rust
installed-service journey and grouped Desktop journey proving the real read path. Live Alpaca
entitlement remains a later separately authorized provider smoke; historical/PIT product
publication, models, forecasting, valuation, recommendation, backtesting, portfolio/risk, and the
complete novice decision journey remain release blockers after this current-market slice.

## 2026-08-14 real Alpaca and strict Markets working-candidate checkpoint

This entry supersedes only the stale *next-barrier* statement at the end of the 2026-08-13 entry;
it does not rewrite that historical checkpoint. Work began from frozen pushed base
`ca0601a5969b0e23bdc99c870b2cb4b8dc879ab9`. The evidence below was produced from the current
working-tree candidate layered on that base and is therefore focused dirty-candidate evidence, not
clean exact-head approval.

**Integrated implementation commit: `f2d6f3b7` (`feat: wire real Alpaca market dashboard`).** The
checkpoint history that follows records that code commit and the later protected-currentness
assertion remediation; any approval claim must use the final clean, unchanged head rather than
infer authority from the earlier working tree.

The current candidate replaces the proposed scripted-fixture barrier with the protected production
Alpaca Paper/IEX path. The application performs the bounded real REST boot snapshot before its real
WebSocket session, requires the exact IEX subscription acknowledgement, retains raw capture and
freshness/currentness authority, projects the active account group through the Source lifecycle,
and preserves a canonical unavailable Market row when after-hours data is not current. The
protected journey imported the configured credential bundle, ran all five real doctor probes,
started the source, queried the same Market authority through native and MCP clients, shut down,
reopened the same root, and queried native and MCP again. The after-hours execution observed the
truthful unavailable branch; it did not substitute a fixture, stub, fake provider, or scripted
market response.

The Desktop and Rust wires now use closed schemas for the unified feed, secondary trade/quote/book/
comparison detail, and Source status/coverage/health. Source lifecycle status is the sole
operational authority, while coverage and health can enrich only exact matching status rows. Live
quotes and books remain visible, but current hot rows are explicitly
`runtime_display_only`, `executionEligible: false`, and unavailable for investment analysis until
durable point-in-time evidence exists. Exact instrument-definition evidence, reference identity,
effective interval, revision, and definition digest stay bound through selection. A live hot source
cannot mint durable investment, feature, portfolio-mark, recommendation, backtest, forecast, or
execution authority.

Tradier is unselected and removed from shipped application discovery, credential import,
onboarding, activation, configuration, runtime, display, lifecycle, restore, and Desktop controls.
The public Coinbase Advanced Trade and public Kraken Spot sources remain selected no-key crypto-only
specialists; optional authenticated Coinbase Direct remains a separate crypto complement. None of
those crypto sources is represented as stock, ETF, index, bond, mutual-fund, or REIT breadth, and
this focused Alpaca journey is not fresh release proof for them.

Focused working-candidate gates completed:

- `CARGO_INCREMENTAL=0 cargo +1.97.1 check --locked -p market-squawk --lib` passed; only the
  existing warning backlog remained.
- `cargo +1.97.1 fmt --all -- --check` and `git diff --check` passed.
- The existing exact Alpaca doctor-receipt/current-generation renewal case passed 1/1.
- The existing protected production journey passed 1/1 with 31 filtered cases: protected import,
  five real probes, `Source.Start`, native Market read, MCP Market read, shutdown/restart, and both
  reads again. A later MCP read may truthfully transition to the strict zero-row result only when
  its metadata is complete, exact-scoped to the requested Alpaca surface, and reports zero current
  observations; the journey does not freeze a stale row across non-atomic reads.
- Desktop TypeScript compilation passed, and the narrow existing grouped selector passed 2 cases
  with 5 skipped.

No broad workspace suite, CI/CD workflow, release build, package matrix, source-lock refresh,
Quarter 4 review, or clean exact-head release gate ran. These focused results prove only the current
Alpaca/display slice. Final checkpoint authority remains pending the root-filled commit, unchanged-
head verification, and the applicable grouped review.

The next barrier is not a fixture. It is application-owned durable Alpaca daily-history and market-
calendar publication through the existing capture/catalog/storage authorities, followed by a
manifest-pinned `Market.GetHistory` read and the Desktop chart composition over that exact immutable
generation. The following dependency is a separately sealed forward live-event archive. Only real
publication-time evidence accumulated by those durable paths may unlock genuine point-in-time
features, backtests, forecasts, recommendations and track records; first-observed-now history can
support current charts and research but cannot be presented as retrospective PIT evidence.

### 2026-08-14 exact-definition and canonical-market remediation

Exact code checkpoint `5133f338279dffa17fe3e72b447cac503239fa60`, tree
`c423a5c65c7ff2599e70c9aeb79f523e892f47c9`, closes the grouped I1–I4 remediation on the real
Alpaca/Markets vertical. `Market.GetUnifiedFeed` now selects an immutable market-data definition
that was both knowable and effective at the operation's exact reference time, rejects expired or
future-published definitions, and binds the nonzero whole-definition SHA-256 through every
candidate, the selection request and digest, the row, the receipt, MCP, and the strict Desktop
parser. Desktop independently requires the same end-exclusive effective interval and exact
row-to-receipt digest equality.

The operation is deliberately a hot current-display operation: it may show a fresh selected-source
trade or bid/ask midpoint, but its closed Rust and Desktop contracts always report
`runtime_display_only`, `executionEligible: false`, and no durable analytical observation. The UI
states only that this live-feed response is not PIT evidence; it does not claim that a separate
archive is absent. A canonical instrument row may transition between live and unavailable across
non-atomic reads, but it may no longer disappear from MCP after the native read has established the
configured topology. Stable identity, including the exact definition digest, survives that
transition and restart.

Focused clean, unchanged, exact-head evidence on `5133f338` passed:

- Rust formatting and diff integrity;
- the bounded output-schema validator's single nonzero-SHA-256 proof;
- the existing market-selection determinism/downgrade/execution case, including definition-revision
  mismatch and digest-change proof;
- Desktop TypeScript compilation and the existing unified-market journey (1 passed, 6 skipped);
  and
- the protected real Alpaca installed-service journey (1 passed, 31 filtered): credential import,
  five real probes, start, native and MCP reads, clean shutdown, same-root restart, and both reads
  again.

The frozen 15-file remediation was independently reviewed at Critical 0, Important 0, Minor 0;
its pre-commit aggregate patch SHA-256 was
`a3a1ea38a2e31bb34e5ba49782730ead2c7a9839e923a85c4a53c3d738597b84`. No broad suite, CI/CD,
release build, package matrix, or release-branch merge ran. This is an exact product checkpoint,
not complete V1 or release approval. The active barrier remains complete Alpaca daily-history and
calendar publication, manifest-pinned `Market.GetHistory`, Desktop charts, and then the separately
sealed forward live-event archive needed before genuine PIT analytics can become available.
