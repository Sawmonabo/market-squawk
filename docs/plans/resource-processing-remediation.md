# Resource processing remediation

Status: **Implemented and pushed at `9543ed357349a83715079ba0720b0f4789f9da58`; scoped critical
verification completed. This batch is closed; full product acceptance remains open.**

Current scope is the [owner-test goal](v1-owner-test-goal.md); execution status and assignments are
in the [delivery ledger](delivery-ledger.md). The design and ownership sequence below document the
completed batch, not authorization to restart it. Whole-app measurement remains deferred.

Prepared 2026-09-28 against `913bb0127fed3866b6411de6225c2467421d169c` plus the existing
working changes in `feature/v1-installed-product-experience`. This is an investigation anchor,
not a release-approved commit. Refresh changed contracts before implementation. All work remains
in the primary worktree; no new branches or worktrees. Original session and recovery backup stay
intact. The four investigation agents made no application edits or build/test/Git mutations.

## Approved execution

The owner approved changing the processing architecture below rather than increasing hardcoded
limits. Execute: implement the complete coordinated changes in place, run focused critical
verification, inspect the integrated consumers, commit and push the verified change to the current
V1 branch, then pause. Do not publish a release or merge main/release. Do not commit unrelated or
unverified preexisting WIP merely to produce a clean checkpoint.

The whole-app memory objective is **500 MB–1.5 GB, maximum 2 GB**, with all required capability.
Lower memory is welcome; 500 MB is not a minimum allocation. Limits on the size of an in-memory
batch, number of concurrent jobs, temporary disk usage and malformed input are distinct from
limits on how much valid history or how many saved models the application can retain. Reducing
data coverage, skipping evidence or rejecting required models cannot constitute success.

## Confirmed causes at the investigation base

| Area | Current source behavior | Consequence |
| --- | --- | --- |
| Query execution | `crates/market-squawk-data/src/query.rs:676` disables DataFusion disk spill. At `:748` execution returns a stream, but `:760–803` retains all batches; `:826` concatenates them before artifact publication. `QueryLimits` also requires the RAM budget to cover the output-byte budget. | A streaming upstream API still becomes whole-result memory, followed by another large allocation. Large disk output is incorrectly coupled to working RAM. |
| Pinned storage reads | `parquet_store.rs:1367` returns a whole-generation `Vec<RecordBatch>`. The existing object reader at `:1621` reduces scope but still materializes a whole object. | Reading one object at a time is useful overlap reduction, not a complete solution for large objects or histories. |
| SEC parsing | `adapters/market-squawk-adapter-sec/src/xbrl.rs:42` cumulatively charges allocations without refunding released ownership; `normalize.rs:619` doubles that cumulative amount for one in-flight fact. Shared nonnumeric contexts and occurrence graphs are also charged or copied repeatedly. | Estimated memory can greatly exceed actual retained memory; avoidable copies still increase actual usage. |
| Filing storage/read | One plain JSON sidecar holds the complete nonnumeric filing graph. `native_lineage.rs:23` caps it at 4 MiB; `catalog/provider_capture.rs` stores it as a SQLite BLOB. `sec_research.rs:1011` retains all Arrow batches before decoding. | Persistence exists, but the producer and reader still require whole documents and duplicated representations in memory. |
| Point-in-time datasets | `dataset_builder/build.rs:374` accumulates parent Arrow batches and decoded candidates; `pit/select.rs:67` repeatedly prepares canonical identities and full sort-index vectors per selection. | Repeated scanning and sorting of complete histories, plus overlapping input/output allocations. |
| Backtests | `backtesting/dataset/admission.rs:575` builds grouped observations while borrowing the complete Arrow input; `:309` accepts all realized histories and builds another sorted bar vector. `engine.rs:361` creates a return vector alongside equity marks. | Multiple historical representations remain live. Study-epoch admission already drops its query before conversion; that path should not be falsely described as missing early release. |
| Model lifecycle | `apps/market-squawk/src/application/model/runtime.rs:848` rebuilds the complete registry/backend list at startup and model admission. Each ONNX backend owns a helper; `onnx/worker.rs:25` permits 16 cleanup owners. | Saved model history is coupled to live processes; old and new runtime images can overlap. Raising the worker count would worsen the cause. |
| Model consumers | Backup copies each retained member with `Arc::from(bytes)` in `application/model/backup.rs:204`; restore and forecast preparation also work through corpus-sized retained structures. | Lazy model loading alone would not remove hidden copies in backup, restore and forecast selection. |
| Live runtime | `live/src/runtime/memory.rs:127` charges feature snapshots again after complete snapshot generations. `runtime/actor/snapshot_publication.rs:67` already includes those features within the shard snapshot budget. | Retained state is double-counted. Construction scratch still exists and must be counted accurately rather than deleting the charge outright. |

These are source-traced findings, not measured peak-RAM results. The live public-paper path uses
actually subscribed products rather than the entire searchable universe; there is no evidence
justifying a blanket lazy-universe rewrite there. Current books and rolling windows legitimately
remain in memory.

## Recommended architecture

Use the existing Rust service, SQLite catalog/indexes, immutable Parquet/Arrow storage, DataFusion
and model helpers. No additional database service, distributed processing system, generic resource
framework or dependency upgrade is proposed.

```mermaid
flowchart LR
    A[Provider or saved input] --> B[Incremental decode]
    B --> C[Indexed staging and immutable batches]
    C --> D[Complete validation and atomic publication]
    D --> E[Requested columns and ordered cursors]
    E --> F[Active calculation state]
    F --> G[Persisted result and paged Desktop view]
```

### 1. Shared streaming storage and query publication

Extend the existing pinned-object reader so callers can consume verified batches without a
whole-object or whole-generation result vector. Retain manifest identity, digest, row-count,
schema and cancellation checks. Check complete-input integrity before authoritative output is
published; corrupted late chunks must not leave a successful partial result.

Keep only a small inline result when appropriate. Once the inline threshold is crossed, write
batches incrementally under the existing artifact reservation/publication lease. Flush and release
batch buffers; finish counts and digests, then atomically bind the complete artifact. Remove the
whole-result `concat_batches` requirement. Readers consume a sealed artifact cursor or page rather
than rematerializing it to cross the next API boundary.

Enable DataFusion-supported spill in a private operation-owned scratch directory. Use its memory
pool and disk accounting rather than a new spill engine. Account working RAM, temporary disk and
complete output independently. Cancellation, failure and restart must reclaim scratch while
preserving committed artifacts. Disk exhaustion must produce a clear recoverable job failure,
never partial success, application startup failure or automatic deletion of user history.

Large sorts/joins can use external-memory algorithms. Not every operator can spill, and some
allocations happen outside DataFusion's pool; implementation must trace the actual physical plan
and include those allocations in measurement. Merely enabling the disk manager is insufficient.

### 2. Filing-scoped indexed processing

First eliminate redundant graph copies and charge shared contexts once. Separate temporary peak
allocation from durable output and cumulative work; preserve the already-corrected structural
child bound while assessing other relationship limits against the new representation.

Parse XML incrementally into filing-scoped disk-backed staging. Index original context/unit/fact
IDs, continuations, relationship endpoints and duplicate keys. Retain lexical text, namespace
identity, nil state, source ordering, exact capture identity and all occurrences. Resolve forward
references and continuation chains in a subsequent validation pass; do not discard unresolved
records or publish prematurely.

Stream canonical numeric facts and native nonnumeric/context/relationship chunks into immutable
storage. Replace the giant sidecar BLOB with a small descriptor that references complete chunk
identities, counts, ordering and digests. Update the active V1 format and all consumers together;
no old/new stack or backward-compatibility layer. Preserve unique existing source/WIP evidence.

Readers request the needed filing, fact group, context or complete traversal. Completeness checks
remain independent of time filtering: common-share valuation deliberately examines excluded facts
for conflicting share classes. The new reader must still see those conflicts. Materializing a
complete filing may be an explicit convenience for an appropriate consumer, but cannot be the
required ingestion/intermediate representation.

Apply the separately identified SEC `numwordsen` zero-value correction only with exact namespace
and lexical semantics. It is a real-data correctness dependency, not a memory-limit workaround.

### 3. Point-in-time datasets and backtests

Compute canonical candidate identities once and use an ordered, operation-local disk-backed
representation. Select revision groups incrementally while preserving the existing canonical
ordering, cutoffs, temporal incomparability, conflicts and duplicate dispositions. Existing audit
digests cover every candidate decision; count/replay passes may be required for count-prefixed
hashes. Filtering discarded rows before audit would change the evidence and is prohibited.

Construct output Arrow batches incrementally under the existing publication lease. Backtests
consume authenticated, chronologically ordered signal and realized-outcome cursors. Merge
per-instrument histories without collecting and globally sorting the full corpus. Retain only
active positions, orders, required rolling windows and calculation state; persist complete fills
and marks for replay, charts and reconciliation. Use repeatable iterators over marks where they
remove a second return vector; adopt online statistics only with verified numerical equivalence
or explicitly justified numerical treatment.

Historical terms, corporate actions, costs, execution timing and out-of-sample boundaries remain
unchanged. No truncation of long histories or silent reduction of the investment universe.

### 4. Model inventory versus active execution

Keep durable model versions and metadata on disk. Startup loads the catalog/index and validates
its integrity without compiling every historical model or launching one helper per version.
Acquire the selected model through a shared active-use lease. Reuse unchanged bundles/backends
across readers and admission; queue work when execution capacity is occupied rather than rejecting
another saved generation. Release idle compiled models and helpers without deleting saved history.
Existing active requests retain their exact model version until completion or cancellation.

Do not retain encoded initialization bytes in a worker after model compilation if only dimensions
are still needed. Stream backup/restore members and request metadata for forecast selection rather
than retaining/copying the entire model corpus. Verify model files before activation and preserve
exact model/input/output identity through restart and backup.

Graph integrity, compatible operators, static shape checks and typed output meaning are legitimate
contracts, not interchangeable RAM knobs. Trace trained forecast exports through their Rust
consumer. A scalar trading-signal contract must not accidentally reject a required multi-horizon
forecast; extend the correct typed forecast path and supported exporter/runtime combination
without accepting arbitrary code or unsupported operators. Size/node/deadline limits require
workload evidence after lifecycle fixes; do not replace them with a universal 2 GB kill switch.
The scalar production exporter and broader research-forecast exporter require different typed
contracts. The research path uses `skl2onnx`; its default linear converter can emit
`ai.onnx.ml::LinearRegressor`, while the Rust policy rejects that domain. Multi-horizon outputs
also conflict with the scalar policy. This is a source-established mismatch, not an executed
reproduction. Use the pinned converter's supported affine export and explicit horizon/output
mapping, preserving lag order, recursive/chained semantics and separate probability calibration.
The [pinned converter implementation](https://raw.githubusercontent.com/onnx/sklearn-onnx/1.20.0/skl2onnx/operator_converters/linear_regressor.py)
provides the export decision evidence. Do not globally remove scalar or operator validation.

Activate models during application preparation, before entering the existing inference call;
do not hide database access or compilation inside the inference hot path. Tract's existing
persistent execution state is a reuse candidate, but verify reset semantics for admitted models
before using it across requests. Its [pinned implementation](https://docs.rs/tract-core/0.23.4/src/tract_core/plan.rs.html)
shows plan `run` spawning a new state. The current model index independently defaults to 64
versions with a 256 maximum; changing only the registry's separate 4,096 ceiling cannot fix it.

Some active weights and execution state genuinely need RAM. This proposal does not claim arbitrary
models can execute within 2 GB; a remaining required-workload incompatibility must be reported.

### 5. Correct live working-set accounting

Charge each retained snapshot generation once. Replace the duplicated feature-generation term
with actual construction scratch per concurrently building shard, including candidate feature
sets, ordering references and container-conversion overlap. Keep mailbox backpressure, active
book state, rolling windows, reader lifetimes and checked arithmetic.

Do not treat the existing 512 MiB local budget as either proven necessary or proven wrong solely
because of its number. First correct ownership/accounting, then exercise required subscriptions,
book depth and features. If a required workload still cannot start, change partitioning or
publication/consumption rather than accepting an unexplained rejection. Snapshot pagination must
not hide canonical financial evidence or turn incomplete data into an eligible trade.

## Desktop demand loading and pagination — explicit scope

This is required remediation scope, not a backend-only change. Frontend work is included in the approved implementation. The first proposal mentioned paged Desktop consumers but did not spell out
all the following behaviors; this section makes them explicit.

- **Summary first, details on demand.** Load inexpensive summary fields for the current screen.
  Fetch substantial filing text, evidence tables, historical periods, model details and secondary
  chart layers when their panel opens or the user selects them. A collapsed HTML `details` element
  alone does not prevent its data from being fetched or retained. Reuse shared queries instead of
  copying the same payload into local component state. Tiny already-loaded details need no extra
  request merely for uniformity.
- **Backend cursor pagination.** Filter and sort in Rust/storage, with a stable unique ordering and
  backend-issued cursor tied to the query and retained generation/snapshot. Preserve exact integer
  cursors; never round them through JavaScript numbers. Avoid fetching everything then slicing in
  React. Define refresh and cursor-expiry behavior so live changes cannot silently mix generations,
  skip entries or duplicate them. Retained pins/cursors must also release storage ownership when
  abandoned; an expired cursor offers restart, not invented continuation.
- **Resident page window, complete browseability.** Keep only the current/nearby pages and necessary
  navigation state in memory; refetch evicted pages from durable storage. A cache/page-retention
  limit must not become a maximum number of pages the user can ever inspect. Prefer explicit
  next/previous cursor navigation where it is clearer than endlessly accumulating results.
  Table virtualization can reduce rendered rows when useful, but does not by itself reduce fetched
  data or query-cache memory. Use existing components/dependencies first.
- **Viewport-based charts.** Request the visible time range, required columns and suitable display
  resolution; request finer detail when zooming and original evidence when a marker is selected.
  Keep dates, extrema, gaps, forecast boundaries and harmonic pivot/confirmation identities honest.
  Display aggregation never alters the full-resolution input to analysis or backtesting. Return
  backend-authored financial values; React does not recompute signals, probabilities or patterns.
- **Cancellation and freshness.** Cancel obsolete screen/detail reads across React, Tauri and the
  Rust service when selection changes or a request has no consumers. Collapse/navigation must not
  cancel an independently running durable analysis job. Deduplicate shared reads, ignore stale
  responses by request identity, release unused payloads and stop unnecessary hidden-view polling.
  Retain explicit freshness/invalidation; do not turn every panel expansion into a costly rebuild.
- **Whole-path consistency.** Update typed requests/responses, native bindings and Desktop/CLI/MCP
  consumers together. Keep provider names out of ordinary views. Preserve keyboard access, focus,
  selection and clear loading/empty/error states when pages or details load asynchronously.

A targeted source read found useful existing patterns: `RecommendationStudyPanel` in
`features/backtests/backtests-page.tsx:69` already enables its request only after opening;
`features/advanced/profile-controls.tsx` gates history visibility; jobs and backups already use
cursor-shaped infinite queries. Reuse those foundations, not a second frontend data framework.

It also found explicit total-page stopping conditions in `features/operations/operations-page.tsx`,
`features/backup/backup-recovery-page.tsx` and `features/logs/logs-page.tsx`. Replace inappropriate
browse-depth restrictions with retained-page windowing plus backend navigation; do not simply
remove caps while accumulating all pages. `app/query-client.ts` has a five-minute inactive-cache
lifetime, which alone is not a byte budget. Forecast detail/outcome queries currently activate on
selection; trace substantial nested sections to decide which should have independent reads.
These are specific inspected paths, not a completed audit of all screens or native cancellation.

Implementation ownership: Sol High handles feature-specific React query/render behavior with
exclusive feature files after root settles service cursor/detail contracts. Root serializes shared
query-client policy, transport/schema bindings, service composition and catalog cursor authority.
This lane depends on the sealed streaming/page readers and can run alongside other disjoint work.

Use focused browser/transport evidence to prove unopened heavy sections do not fetch; pagination
can pass former page stops without retaining all earlier payloads; filter/selection changes do not
apply stale responses; close/cancel releases read work; chart zoom keeps exact evidence available;
and reopening retains correct saved results. Measure WebView/JS and native memory during long
browsing sessions as part of the whole-app workload. Add no routine component-test matrix.

## Implemented dependency and ownership sequence

| Stage | Work and exclusive owner | Dependency / completion condition |
| --- | --- | --- |
| A | Root: query/receipt, pinned reader, controlled artifact writer, catalog publication, shared schemas/contracts, manifests if necessary | Streamed sealed artifact, spill lifecycle, complete identity and cancellation semantics |
| B1 | Astra High: SEC parser/chunked filing producers and typed readers | Root storage contract; real filing and complete cross-chunk validation |
| B2 | Astra High: PIT selector, dataset builder and backtest cursors | Root storage contract; identical financial and audit semantics |
| Parallel | Astra High: model inventory/activation, backup/forecast consumers and necessary model producer contracts | Root serializes shared application composition and Python/native output contracts |
| Parallel | Sol High: live accounting plus scratch-sizing helper | Disjoint from storage/model authority; accurate retained/scratch accounting |
| B3 | Sol High: Desktop demand loading, cursor navigation, retained-page windows and chart viewport reads | Root settles service/cursor contracts; shared transport and query policy remain serialized |
| C | Root: Desktop/CLI/MCP and application consumer integration, docs and one compiler slot | No hidden rematerialization at consumers; required workflows and restart |
| D | Root: focused verification, checkpoint commit/push to existing V1 branch, pause | Evidence covers complete coordinated change; no unrelated WIP bundled blindly |

Shared writers, catalog schemas, manifests/lockfiles and application authority are serialized.
There are no new worktrees. Review is grouped at existing delivery checkpoints, not after each
small edit. Implementation tasks must have bounded outputs and no autonomous duplicate builds.

## Critical verification and acceptance

Extend existing critical suites only where these changes create an uncovered failure boundary:

- Identical complete data, canonical counts/digests and financial outcomes across batch boundaries;
  references/revisions/conflicts crossing chunks; malformed or missing final objects rejected.
- Cancellation/crash before commit leaves no authoritative partial result; restart restores the
  same selected evidence and outcomes and reclaims operation-owned scratch.
- Model admission and backup do not rebuild or duplicate unrelated active models; required actual
  forecast exports run through native inference with correct output meaning.
- Live accounting distinguishes retained generations from construction scratch; required route,
  book and feature coverage survives without false startup rejection.
- A representative workload larger than its permitted working set completes through disk-backed
  processing, with no missing facts/history/models. Record bytes/rows processed and disk usage,
  not just a lower RAM number.

Owner sequencing correction: defer whole-application RAM measurement until the complete application
and workflows are ready. It is not a prerequisite for committing this remediation batch. At that
final stage, measure idle, active and peak memory across Desktop/WebView, shared service and all
active model/Python/helper processes, with complete workflow, cancellation and restart evidence.
Report OS counters and MB/GiB conversion; logical component estimates are not resident-memory
measurements. No performance acceptance exists yet.

Build checks are serialized, single-job and nonincremental with current pinned toolchains. No CI
or release build per task. Monitor the development machine's memory/disk separately from the
installed application's memory; do not recreate the prior overlapping build workload.

## Reuse evidence and limitations

The repository already pins DataFusion 54.0.0 and Parquet 58.3.0. The installed DataFusion source
confirms `DiskManagerBuilder`, `DiskManagerMode::Directories` and temporary-size accounting.
Current upstream documents spill support in its [disk manager](https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/)
and [memory-constrained query guidance](https://datafusion.apache.org/user-guide/configs.html).
Use the pinned local API rather than assuming newer documentation is source-compatible.

Parquet's [Arrow writer](https://arrow.apache.org/rust/parquet/arrow/arrow_writer/struct.ArrowWriter.html)
and [reader](https://arrow.apache.org/rust/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html)
support the existing batched processing direction. The repository's `parquet_store.rs:882` already
uses a controlled row-group reader/writer path; reuse its lifecycle rather than creating a parallel
persistence stack. The SEC zero transform has an upstream [SEC-authored implementation](https://raw.githubusercontent.com/Arelle/EDGAR/master/transform/__init__.py).

The earlier audit was bounded, not an everywhere-clearance. Implementation must trace affected
consumers and remaining provider, Python training, snapshot, dataset and startup limits before
claiming all unjustified restrictions are removed. Retain a disposition for each inspected limit:
removed through streaming, corrected ownership estimate, replaced by working-set scheduling,
legitimate integrity/interface safeguard, or unresolved with concrete evidence. Unresolved required
capability is not silently converted to a backlog item or accepted as performance savings.

## History acquisition closure discovered during implementation

Tiingo acquisition retained decoded responses, mapped pages, complete session vectors and native
action evidence concurrently despite already persisting every original page. Its planner admitted
up to 256 windows, while publication imposed separate cumulative 64-page, 64 MiB and 10,000-row
limits. Current shorter product requests fitting those limits does not justify the incompatible
representation or prove resource compliance.

Close this through the same approved pipeline: retain original page/checkpoint authority, decode
and index one page at a time, validate the full calendar and terminal, then stream raw/adjusted bars
and the complete action suffix into immutable logical publication. Preserve original per-response
limits and atomic completion. Shared logical writer code must support typed source-specific
terminal validation without a duplicate publication stack. History and calendar replay consume
indexed original evidence rather than rebuilding the complete capture in memory.

## Disposition of inspected limits

| Boundary | Current treatment |
| --- | --- |
| Complete query output and pinned history | Stream verified batches into immutable storage; DataFusion working RAM and disk/output budgets are independent. Preserve complete counts and hashes before publication. |
| Filing relationships and native evidence | Filing-scoped SQLite indexes and logical chunks replace duplicated graphs and giant sidecars. A chunk row window does not restrict total filing occurrences. Per-response XML depth, text and malformed-input checks remain. |
| Dataset and backtest history | Ordered disk staging, cursors and persisted fills/marks replace simultaneous complete input/output vectors. Financial cutoffs, corporate actions and audit identities are preserved. |
| Retained provider originals and native reference captures | Removed lifetime row/metadata-byte quotas from immutable custody tables. Indexed exact reads and keyset pages retain bounded resident payloads; per-record integrity, rights and complete custody remain enforced. |
| Saved model generations | Durable catalog pages replace the complete in-memory inventory and former 64-version application ceiling. The modeling library's explicit in-memory registry limits do not govern the installed saved inventory. |
| Active model graphs | Per-selected-model shape, operator, 64 MiB artifact, 1,024-node and inference-deadline checks remain. Current affine research exports use the typed multi-horizon path; signed outputs are not forced through scalar probability admission. These are graph/execution safeguards, not total saved-history quotas. |
| Forecast evidence | Compact record decoder bounds, the existing per-vintage artifact limit and typed horizon bounds remain. Chart range reads never reduce the full-resolution analytical source. Complete chart rows are verified during restore. |
| Live market state | Retained feature snapshots are charged once; construction scratch is separate. The existing local live budget remains distinct from whole-app memory. Required complete-product subscription/depth workloads still need final acceptance evidence. |
| Desktop pages | Backend cursor pages and chart viewports bound resident payloads; they do not set a maximum browse depth. Exact selected evidence remains available independently of the current page. |

This is a disposition of the approved affected paths, not an assertion that every numeric bound
throughout the repository has been removed or that the full V1 product has passed acceptance.
Required workloads must remain complete. Final installed workflow and whole-app measurements
remain separate from this remediation checkpoint.

## Implementation evidence (resource checkpoint, not release acceptance)

The coordinated changes stay in the existing V1 worktree and update the active schemas and
consumers in place. No compatibility reader, data conversion program or new worktree is added.
The original SEC/common-share financial work is included where required by the new indexed
filing and financial consumers; unrelated editor troubleshooting prose remains outside the batch.

Focused checks completed during integration:

- The complete million-row ordered query passes with an 8 MiB working-memory budget, native
  disk spill, complete output counting and scratch cleanup. Cancellation before artifact commit
  also passes. Parquet encoding coalesces small batches rather than creating a footer entry for
  every input batch, and page sizing uses the actual writer allocation.
- Point-in-time dataset publication passes with its original budget, including rejection after
  publication authority is revoked. Projected storage batches preserve canonical schema metadata.
- The SEC indexed test parses the real Microsoft filing, preserving occurrence/context counts;
  its separate physical filing fixture covers normalization, cross-chunk publication, financial
  identities, typed reads and restart. This does not establish full Microsoft financial generation.
- Tiingo logical history passes original retention, atomic publication, physical service reopen,
  exact bars/actions/calendar evidence and corrupted-index rejection. The source contract revision
  and registered metadata digest are both verified through custody and restart. Native-reference
  custody recovery also passes after removing cumulative saved-history quotas.
- Streamed portfolio replay and immutable backtest fills/equity artifacts pass their existing
  critical regressions. Streamed benchmark calculation also passes.
- Durable model inventory passes physical restart, 65 retained generations, insertion during
  fenced pagination, exact lookup, retries and tamper rejection. Selection metadata is read
  without loading weights and still verifies provenance.
- Native ONNX execution passes scalar and two signed forecast horizons, repeated inference,
  exact identities and incompatible-input rejection. Worker retirement confirms process exit
  before capacity is released. Python affine export passes against the ONNX reference evaluator.
- The existing analytical backup bundle create/reopen/restore regression passes. Archive
  framing/cancellation and chart publication/range integrity also pass. Restore now verifies
  complete chart row ordering, endpoints, counts and digest; missing first or last rows fail.
  These component checks do not establish an installed end-to-end backup journey.
- Live feature accounting, sealed journal storage/recovery, logical-object failed-publication
  cleanup and runtime read cancellation pass. Retained feature snapshots are charged once,
  independently of construction scratch.
- Desktop TypeScript compilation and the existing market-screen journey pass demand loading,
  exact values, viewport generation binding, cancellation and reopening. Application and Desktop
  Rust compilation pass; existing compiler warnings are not represented as a warning-free gate.

The source closure manifest is refreshed without acquiring or rebuilding the Python runtime;
only its source identities change. Exact rights, object and custody hashes remain enforced.
No full CI/release gate, installed complete-V1 acceptance or whole-app RAM measurement is claimed.
The resource checkpoint was committed and pushed; product execution is paused. Whole-application
RAM measurement waits for complete product workflows.
