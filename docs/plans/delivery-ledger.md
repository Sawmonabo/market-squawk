# Market Squawk Delivery Ledger

## Active startup, optional-lock and previous-close integration — 2026-09-30

Independent-read checkpoint: `1e82c1e5` (PR #43 comment 5923702152). History checkpoint: `0206d13a` (PR #43 comment 5923592639). Request-admission checkpoint: `e092a17b` (PR #43 comment 5923423225); Desktop reconnect: `251c8e2e` (PR #43 comment 5923314684); currentness: `bb600ce5`; data-read checkpoint: `a9277150` (PR #43 comment 5923037557); startup checkpoint: `98104fff` (comment 5922645807).
History integration audit base: `a0e8b77b`; both preserved on `feature/v1-installed-product-experience`.
One primary worktree, three local branches and three origin branches; no linked worktrees.
Original session and recovery backups remain protected. No CI or release gate has run for this wave.

Owner correction (committed in `a0e8b77b`): locking is opt-in. Configured connections reuse saved
credentials across normal launches/rebuilds without an application password. Provider expiry/key
replacement affects its connection. Optional locking supports OS-remembered access, explicit
Lock/Forget, and a user-selected reauthentication interval. Saved pages remain usable. This
supersedes forced password storage in development and any conflicting import-only guidance.

### Current outcomes and verification

**Pushed checkpoint — `be6e661b` (2026-10-01).** SQLite active-event storage and its
exact current/PIT/valuation consumers are integrated with independent startup preparation.
PR #43 evidence: comment5928955440. Worktree was clean after push; one worktree and three
local/origin branches remain. Main/release, original session and recovery backups are unchanged.

Critical evidence: event restart/integrity, bulk history and Fund NAV checks pass; current shared-
service startup/restart/optional-access passes1/1 in100.15s. Locked offline single-job control-plane
build passes4m33s. Live crypto fails79.21s: Coinbase reports local `publication_admission` timeout,
then the unified row has no selected source; official reference assertions pass. This is not live,
continuous-ingestion or installed acceptance. The accidental prior-executable run was terminated
and supplies no evidence. Current logs: `sqlite-bootstrap-raw-only-build.log`,
`sqlite-bootstrap-raw-only-live-current.log`, `sqlite-current-shared-service-critical.log` under
`.agents/tmp/v1-first-stock/`. The unrelated existing decision fixture still lacks genuine current-
share evidence and remains failed/open.

Archive integration checkpoint **`d9f91902` pushed** (based on `be6e661b`): bounded cold placement,
exact hot/cold reads, mixed backup/restore and joined maintenance shutdown are implemented.
Existing shared Desktop/CLI/MCP service authority/restart check passes **1/1,100.08s**
(`sqlite-archive-shared-service-critical.log`). Event archive/backup, general backup and worker
release checks pass as detailed below. These are focused working-tree checks, not quarter/release
approval. The independently pending terminal-batch changes remain uncommitted; real Coinbase
publication still fails and is not represented as completed live behavior.

**Streamed recovery/backup checkpoint `ee77ff55` pushed** (PR #43 comment5930175512).
Its focused evidence is recorded below. Independent terminal-batch WIP remains preserved.

### Current dependency and ownership wave

Latest integrated outcome: **real Coinbase and Kraken installed-service publication/restart
journeys PASS** (`kraken-live-trades-status-live.log`, 155.49s). Both publish genuine typed events,
stop cleanly, resume publication after restart, reopen identical original publication/evidence, and
remain stopped across a third start after explicit Source.Stop. Kraken also reuses the prepared
anonymous configuration session and exposes both required book/trade streams. This is installed
service plus typed Desktop transport evidence, not native WebView or complete-V1 acceptance.

The checkpoint containing this entry implements live Kraken trade updates without requesting
historical snapshot rows under newly observed identity authority. Matching ACK checks and all
original timestamp/identity validation remain; book snapshots and authorized historical decoding
are retained. Shared status correction was pushed as `60578fa6` (PR #43 comment5935128626).
Storage, reference custody and graceful publication drain were integrated through `1a23ad84`.

| Owner | State / exact scope | Next barrier |
| --- | --- | --- |
| Astra `research_options_failure` | Released: Kraken config/decoder and existing critical case. Lead inspected; focused case PASS1/1. | No active edits. |
| Astra `calendar_currentness_failure` | Read-only stock trace complete: real current-share projection wiring exists; fixture failure alone proves no runtime defect. | Inspect real FinalPrepare receipts when resuming stock journey. |
| Lead | Current integration, docs, Git/PR. Application build PASS5m03s; test-helper rebuild PASS17.54s; combined live journey PASS155.49s. | Commit/push this coherent checkpoint, then real stock/Everyday-Advanced workflow evidence. |

One primary worktree and three local/three origin branches remain; no new branches/worktrees.
No build or live check remains running. Native Desktop startup/screens, remaining provider and
stock/financial workflows, final owner packages and whole-app measurement remain incomplete.
The stock decision fixture still lacks genuine current-share proof; do not confuse it with the
verified runtime wiring or waive it as final acceptance. Dated details below preserve prior
checks/failures and are historical evidence, not additional active assignments.

### Raw capture serialization correction — base `ee77ff55`

The 74–77s sample caught `write_current_frame_inner -> serde_json -> CountingCrcWriter` and the
parallel capture pipe's per-fragment SHA writer. Payload bytes are JSON integers; downstream
file/pipe buffering does not coalesce checksum updates. This is CPU serialization evidence, not
an fsync or mutex stall. The elapsed live check records about 5.2s before source admission fails.

| Owner | Exact ownership / dependency | Completion evidence |
| --- | --- | --- |
| Astra research_options_failure | Platform `journal.rs`, `raw_record.rs` if a cohesive shared serializer belongs there, and `capture/process_journal/{sink.rs,protocol.rs}`; relevant existing platform critical cases only | Coalesce serialization fragments before CRC/SHA using maintained buffering, preserve exact bytes/length/two-pass checks, cancellation and honest fixed memory accounting. No all-record materialization, new format/version, cap/deadline increases, independent builds or Git. Return any accounting/API caller changes to lead before expanding ownership. |
| Lead | Shared exports/accounting callers if needed; application integration, build/test scheduling, Git/ledger/PR | Inspect actual changes, run existing journal/capture integrity and cancellation checks once, then unchanged live ingestion/restart proof. |

This bounded wave follows the concrete occupied stack; it does not create another review quarter
or performance acceptance claim. Earlier diagnostic-only owners are frozen.

Implementation is frozen: one shared 8 KiB serializer buffer precedes CRC/SHA/count/control writers.
Successful completion drains without forcing an underlying file/pipe flush; failure disarms buffer
Drop so cancelled/failed writes are not retried. Journal accounting includes temporary workspace.
Existing critical checks pass 5/5: exact bytes/no failed-write retry, sealed restart/recovery, frame
length/CRC, exact storage accounting, and helper kill/reap/release. Platform compilation passes in 12.64s.
Evidence: `buffered-capture-{build,critical}.log`. The application build passes in 6m55s
(`buffered-capture-app-build.log`); the unchanged public-feed restart check fails in 132.26s
(`buffered-capture-live.log`). No timeout,
data limit or wire-format change was made.


### Live publication follow-up — current buffering candidate

The updated live run fails in 132.26s. It reaches canonical publication after raw sealing and complete terminal-row
pairing at about 2.98s publication age, then reports `publication_authority_invalid`.
The final check reports no array-shaped unified feed after source teardown. Kraken is not reached. This is progress beyond the prior occupied
serialization stack, not completed live acceptance. Astra research_options_failure owns a read-only
trace of that exact error through Coinbase publication authority, canonical ingestion and revocation;
no edits, builds, timeout changes or Git. Lead retains all source ownership and live execution. The
outcome is an exact failing condition and smallest correction proposal before further changes.

### Committed-row clock correction — base `8404085c`

The prior batch/buffer checkpoint is pushed as `8404085c` (PR #43 comment5930638154).
Astra found a concrete mismatch: buffered pre-acknowledgement frames keep their raw receipt time,
but live qualification uses the later acknowledgement/processing time. Publication rechecks the
committed row at the earlier raw time, before its health lease begins. Current live diagnostics
identify the authority stage but do not yet prove which predicate rejected that particular row.

| Owner | Exact files / outcome | Critical evidence |
| --- | --- | --- |
| Astra research_options_failure | `crates/market-squawk-live/src/qualification.rs`, `apps/market-squawk/src/application/research/ingest/crypto_market.rs`; expose existing committed provenance ingestion time and retain it per row for source/native-identity checks. Preserve all raw/publication clocks and sealed currentness checks. | Locate/extend only existing critical authority coverage for different receipt/commit times; return selectors before build. No new clock, limit, deadline, authority bypass, build or Git. |
| Lead | Shared integration, docs, existing live check, serialized builds and Git | Inspect provenance/currentness path; compile once frozen, run critical existing check and unchanged real publication/restart journey. |

Both clock-fix files are frozen and lead-inspected. Canonical event availability/ingestion already
use actual qualification time, and SQLite commit availability uses its trusted commit clock;
original raw times remain unchanged. Existing source lease time-bound/expiry/revocation coverage
passes1/1 (0.01s; build19.59s), log `committed-clock-authority-critical.log`. This unchanged source
check does not independently prove the app's new per-row clock wiring; the existing real public
publication/restart journey exercises that integration. No new carrier fixture/harness was added.
The first application compile found one third helper caller in the composite-book path
(`crypto_market/kraken_rendezvous.rs`); lead aligned it and stopped that known-failed build
(exit130), so it is not acceptance evidence. Repository-wide caller search confirms the three
canonical consumers now use the same helper. The single-job retry passes in4m28s
(`committed-clock-app-build-retry.log`). The unchanged live journey fails in132.50s
(`committed-clock-live.log`). It passes committed-row authority and now returns a Coinbase
publication error during canonical evidence binding; the source stops and no selected feed becomes
available. Kraken is not reached. The clock correction is implemented/compiled, with unchanged
source authority critical coverage passing, but complete live ingestion remains unverified.
Next: identify the closed adapter validation failure rather than relaxing evidence checks.

### Adapter canonical-binding trace — base `5a763d8d`

Astra research_options_failure owns read-only tracing of Coinbase `publication.rs` and its existing
critical fixtures for the newly reached canonical-binding failure. No contradictory predicate is proved. Owner now has bounded implementation ownership of
`adapters/market-squawk-adapter-coinbase/src/publication.rs` only: preserve all comparisons and emit
failure-only closed stage/field names, without provider values, for committed-row and common-binding
validation. Reuse a cohesive local helper where it eliminates duplicate checks/logging. Lead owns
supervisor typed-error logging and integrated compilation/live check. No independent builds/Git or
weakened validation. This follows the pushed clock correction;
no other implementation lane is active.

The diagnostic files are frozen and lead-inspected; all comparison predicates are preserved.
The single-job integrated build passes in4m34s (`coinbase-binding-diagnostic-build.log`);
the unchanged live check fails in132.41s (`coinbase-binding-diagnostic-live.log`). The expanded
field-level diagnostics are investigative; reduce them to useful maintained failure context after
the cause is identified, rather than retaining unnecessary debugging bulk.

Live diagnostics identify exactly one mismatched common-row field: `provider_product`; all other
identity/provenance/clock/quality comparisons pass. Astra research_options_failure now owns the
bounded semantic trace and in-place correction in Coinbase `publication.rs`, `market_handoff.rs`
and `config.rs`; report any additional producer/shared application caller needs to lead before edits. Preserve exact
instrument/native identity and provider feed-product binding; do not remove the check. Lead owns
application/shared metadata integration and removing unnecessary diagnostic bulk after the cause
is resolved. No new harness or independent build. Existing live test remains the completion check.

### Bounded storage implementation wave — base `d9f91902`

The diagnostic build passed5m01s; the existing live check failed79.22s
(`sqlite-terminal-stage-diagnostic-live.log`). All four interrupted Coinbase publications were at
raw sealing, before terminal-row pairing or SQLite event publication. Source edits below are not
covered by that executable. No new compilation until all owners freeze.

| Owner | Exact ownership / outcome | Start barrier / evidence |
| --- | --- | --- |
| Astra first_brief_readiness | Platform `journal/{sealed.rs,sealed_object.rs,sealed_backup.rs}` and cohesive recovery child; data `catalog/provider_capture.rs`, `ingest/source_backup.rs`, new `ingest/provider_capture_recovery.rs` | Implement owner-scoped stage/receipt pins and bounded retained recovery; remove lifetime capture admission in Rust. First return shared API/SQL needs; lead owns exports/schema/parent declarations and caller alignment. Preserve existing selected verification and extend only existing critical platform recovery cases. |
| Astra streamed_backup | Data `catalog/evidence.rs`, `catalog/evidence/market_events.rs`, `authority_transition/evidence/{catalog.rs,canonical.rs,mod.rs}` | Canonical streamed evidence and compact exact summaries. Lead-approved summary/cursor contract; no total-history count ceiling, excluded evidence, new format version or parallel old API. Propose existing test amendments; lead owns tests. |
| Astra streamed_backup_files | Data `authority_transition/evidence/{fs.rs,fs/materialize.rs}`, `analytical_backup.rs`, `authority_transition/restore.rs` | Consume frozen compact snapshot and physical-evidence visitor API; one-object verification/copy against retained immutable catalog, full restore preflight. No all-history DTO/FD inventory or receipt-count ceiling. Existing critical backup/restore checks owned by lead. |
| Lead | SQL0021/digest; platform/data exports; `catalog/read_snapshot.rs`; parent `ingest.rs`; research_service and local_product startup/composition; existing data tests; Git/build/ledger | Settle interfaces before dependent edits, serialize shared files. Critical existing raw recovery, mixed event archive/backup, general backup and exact-subset retry; then shared application integration. |

Shared integration now replaces the startup-wide raw scan with a retained bounded background
cursor; selected reads preserve full verification. Snapshot membership is fresh per object after
checking live pins. SQL0021 adds physical-identity and backup lookup indexes and replaces lifetime
raw-object quotas with signed-integer overflow protection. Analytical backup's obsolete artifact/
reference-count constructor settings are removed in place, including service and test callers.
These edits pass the platform and data critical checks below; application integration is pending.
Existing recovery and backup/restart tests cover the changed paths; no new harness or full gate is added.

Focused storage verification passes after checked SQLite integer conversion and snapshot import
corrections. Data critical executable build passes1m12s (`streamed-storage-data-build-retry.log`).
Existing checks: mixed event archive/restart/backup1/1,4.83s; general backup1/1,5.59s; native-reference
recovery1/1,0.95s; complete history/recovery1/1,1.63s; prepared exact-subset restore retry1/1,5.85s;
streamed lineage replay1/1; receipt count/byte validation1/1. Logs use
`streamed-storage-{event,general-backup,native-recovery,history-recovery,exact-retry,lineage,count}-critical.log`.
Application control-plane compilation passes6m57s (`streamed-storage-app-build.log`).
Existing shared service/startup/restart/optional-access check passes1/1,100.25s
(`streamed-storage-shared-service-critical.log`). Actual public-feed verification fails79.22s (`streamed-storage-live.log`): one capture is at
`current_join`/`physical_seal`, three wait at `gate_admission`/`queued`. Coinbase readiness times
out before selected publication. Those phases describe teardown only. The repeated run fails78.87s, but its58.8s stack
sample has idle runtime/blocking workers and no active seal. Neither a long seal stall nor runtime
starvation is proved. Astra finished this read-only trace; lead will sample the final admission
interval before choosing a behavioral fix. The68–71s sample also has no active seal.
Astra research_options_failure now owns only failure-age diagnostics in
`live_source/coinbase_publication_supervisor.rs`; lead owns matching seal-phase elapsed evidence in
`research_service.rs`. No scheduling/timeout/cap changes; compile only after both freeze, then the
same live check. These diagnostics close the gap between teardown phase and actual wait duration. Both owners are
frozen; the single-job application rebuild passes4m30s
(`storage-publication-elapsed-build.log`). Lead is running the unchanged live check with elapsed
failure fields (`storage-publication-elapsed-live.log`). The68–71s sampled run ultimately fails132.33s
(`streamed-storage-live-admission-sample.log`). No behavior change is claimed from diagnostics. The elapsed run fails79.09s: every raw future
was polled immediately; one physical seal runs5243ms and three requests queue behind it. Thus
the earlier68–71s sample preceded physical sealing. Lead is sampling the actual74–77s interval
(`storage-physical-seal-sample.txt`, `storage-physical-seal-sample-live.log`); Astra retains read-only stack diagnosis. This does not establish
live ingestion or Desktop completion. The separately preserved terminal-batch WIP remains open.
No whole-app RAM or release claim.

No additional branch/worktree, competing build, new broad harness or full CI. Raw and backup work
are independent implementations; they converge at the lead's caller/schema and critical-check
barrier. Original sessions, saved evidence and recovery backups stay protected.

Terminal-batch implementation and its corrected linear allocation accounting are frozen and
lead-inspected. Existing `action_runtime` rejection checks pass **3/3** (0.02s; final single-job
build4.84s), including complete seven-row export through one slot and exact rejected coverage
(`sqlite-terminal-batch-accounting-critical.log`). This is focused evidence, not live admission or
installed acceptance.

Archive/backup critical now passes **1/1,4.41s** (`sqlite-archive-backup-retry-critical.log`).
The fixture reacquires its exact durable reservation after reopen; capability checks are unchanged.
Proof covers coherent WAL handoff, archived idempotent retry, late hot/cold arrival, mixed backup
restore, exact original evidence and corruption rejection. All implementation files are frozen.

The existing general backup case exposed a stale SEC fixture retaining a disk-backed selection
across same-root reopen (`Parquet(RootAuthorityAlreadyOwned)`, 5.13s). Sol corrected only that
fixture: preserve every evidence field and decoded row, release the original root, then compare the
reopened selection. Production root exclusivity and evidence checks remain unchanged. The corrected existing case
passes **1/1,5.14s** (`sqlite-archive-general-backup-retry.log`). Archive/restart/backup also
passes **1/1,4.41s** after the worker-ownership fix (`sqlite-archive-shutdown-critical.log`,
single-job build44.79s).

The first application compile was deliberately stopped (exit143;
`sqlite-archive-terminal-app-build.log`) and supplies no build evidence. Astra corrected the
source-proven archive shutdown gap in `blocking_supervisor.rs`,
`ingest/market_event_store/archive.rs` and `parquet_store/streaming.rs`: the operation now cancels
and waits for its own SQL/encoder workers before returning. Encoder cancellation remains local so
successful encoding does not cancel catalog publication. All implementation owners have frozen
and released their files. Lead owns current critical verification and integration. Astra owns only the existing blocking
supervisor test extension is frozen and passes **1/1,0.01s**
(`sqlite-archive-worker-release-critical.log`, build27.09s), proving idle wait remains pending while
a worker retains its guard and completes after release. No new harness or production changes.
Application build passes **5m35s** (`sqlite-archive-terminal-app-rebuild.log`), producing
`control_plane-63b9e23bcdd0961d`. The unchanged real Coinbase/Kraken publication/restart/Stop check
has again reported Coinbase `publication_admission` timeout before source readiness
(`sqlite-archive-terminal-live.log`), then fails **132.51s** without a usable unified feed. The terminal-export unit fix
has not established the root cause of the live stall. Astra research_options_failure owns a
read-only trace of the exact frame permit lifetime and pending publication tasks, limited to
live-source admission/sink/publication supervision and crypto rendezvous. No edits/builds until it
identifies the unresolved dependency; lead runs the independent shared-service critical check.

The next raw-custody investigation is complete, with no implementation dispatched yet. Replace
lifetime claim/byte admission and full-store startup scanning with indexed claim membership and
retained incremental reconciliation. Background quarantine first requires receipt-held live-object
pins to protect sealed objects awaiting catalog publication. Backup keeps full verification through
an independent read snapshot. This is the next required dependency after the current archive/live
batch checkpoint; it is not completed continuous-ingestion evidence. The existing analytical backup snapshot also
materializes relation evidence under a 400,000-reference ceiling (`catalog/evidence.rs`,
`authority_transition/evidence/catalog.rs`). Event rows now participate in that exact evidence;
replace this whole-snapshot admission with streamed evidence in the same remaining backup work,
rather than raising the ceiling or excluding event evidence. Sol desktop_disconnect_recovery
has read-only ownership of this finite follow-up: trace snapshot/digest/inventory consumers and
return the smallest streaming contract, shared-file boundaries and existing critical check. No
implementation or additional build before the current checkpoint is integrated.

Archive encoding remains uncompressed: existing streaming budgets do not yet account native
compression workspaces; no compression or throughput claim is made. Raw-custody lifetime admission
and whole-store startup scanning remain required unfinished work. The explicitly authorized live check owns temporary source processes during verification.
No new branch/worktree, CI/release gate or whole-app RAM measurement. Historical investigation
records below preserve evidence; their assignments and in-progress status are superseded by this table.

The capacity candidate compiles (7m49s; `sqlite-public-capacity-build.log`) and the existing
shared-service startup/restart critical passes1/1 in101.32s (`sqlite-shared-service-critical.log`).
The real public-feed restart case still fails after132.82s: preparation returns `Unavailable` and
no exact publication is selected (`sqlite-public-capacity-live.log`). That run contains no
full-queue error or stack overflow; it does not establish completed startup.

A concrete readiness race was then identified: the sink signals when its first batch is enqueued,
while activation immediately requires a complete published snapshot. The unpublished public
runtime now awaits the existing per-shard notification channels and rechecks the complete route
and source topology under its original deadline/cancellation. No new queue, task, poller, delayed
sleep or larger deadline. All five affected files are frozen: Astra research_options_failure
finished the live-runtime async notification and two wrappers; lead integrated composition and
market-runtime readiness/cleanup. A closed onboarding-activation diagnostic preserves the error
phase. The sole compiler is rebuilding in `sqlite-snapshot-readiness-build.log`; the same live
publication/restart/Stop check follows. The enqueue race is established in source; the prior log
alone does not prove it was the cause of that particular run.

Snapshot readiness build passes5m51s. The live rerun now exposes a more specific failure:
Coinbase's committed publication worker reports `publication_rendezvous_unavailable`, followed by
snapshot-readiness unavailability and shutdown failures (`sqlite-snapshot-readiness-live.log`).
Lead retains all implementation ownership; Astra research_options_failure has read-only ownership
of the publication supervisor/rendezvous error trace. No evidence checks are loosened and no queue
or deadline is raised. Determine the concrete pairing failure before the next correction.

The pairing trace identifies a concrete admission mismatch: a four-slot raw channel can release
slots into four concurrent raw publications while more committed frame keys arrive at a four-key
rendezvous. Queue capacity alone does not bound the complete pipeline. The correction retains one
existing-capacity admission permit from pre-read through raw/committed pairing and publication;
Kraken book/trade share that same budget. Astra research_options_failure owns only
`live_source/{publication_admission,coinbase_publication,kraken_publication,sink,coinbase_publication_supervisor,kraken_publication_supervisor}.rs`.
Lead owns module declaration, composition and Direct constructor alignment plus closed-reason
rendezvous diagnostics. No cap increase; Direct's existing synchronous submission behavior remains
explicitly outside public-feed async admission. Freeze all files before the next serialized build.

End-to-end admission build passes4m39s (`sqlite-public-frame-admission-build.log`). Live case
reaches a real Coinbase unified investment row but fails79.03s at its missing `referenceEvidence`
assertion (`sqlite-public-frame-admission-live.log`, production_mcp_composition.rs4322). That run
shows no preceding queue/rendezvous/snapshot failure; full restart remains unverified. Sol
desktop_disconnect_recovery owns read-only tracing of `application/paper/market.rs` and
`market/unified.rs` reference-record selection into crypto rows. Lead retains all edits, Git and
checks until exact missing producer-to-consumer wiring is identified. Do not relax the assertion.

Reference wiring correction: the unified feed now loads canonical official records for the union
of display identities and live crypto execution identities, then validates display batches against
exact selected records separately. Crypto definitions are durably published during public source
preparation; they were omitted by the display-only consumer query. Lead owns this single-file
correction; all implementation files are frozen for `sqlite-crypto-reference-build.log` and the
existing exact public publication/restart live case.

Reference-query build passes4m38s. Live check fails132.54s before obtaining a usable feed;
Coinbase reports a subscription-state resynchronization during readiness
(`sqlite-crypto-reference-live.log`). Astra research_options_failure owns read-only diagnosis of
subscription acknowledgement and end-to-end permit ordering, including pre-ack buffered data.
Lead owns diagnostic clarity, exact phase context and all edits/checks. No timeout/cap increases.

Confirmed source dependency cycle: Coinbase submits pre-ack data to raw publication while
normalized rows wait for ACK; four retained frame permits can prevent reading ACK itself. Astra
research_options_failure owns `live_source/sink.rs` and Coinbase adapter `publication.rs` only:
retain exact raw work in the existing bounded pre-ack buffer, then asynchronously drain under
normal publication permits after ACK. Preserve Direct/Alpaca synchronous paths, original clocks,
byte accounting, deadlines and Kraken zero-pre-ack semantics. Lead owns checks/integration.

An affected financial restart check also fails before persistence: existing
`decision_append_is_durable_idempotent_and_recovers_under_one_writer_lease` returns
"complete restart fixture must generate a buy proposal" (0.30s,
`sqlite-decision-restart-critical.log`). Astra calendar_currentness_failure owns read-only
tracing of that exact proposal fixture and current generator; distinguish stale fixture inputs
from real financial behavior. No changed thresholds, test weakening, builds or Git.

Financial-check diagnosis: unchanged HEAD fixture constructs `InvestmentAnalysisEvidence::new`
without a current-share projection, while unchanged HEAD authority requires it. This is a
pre-existing fixture gap, not evidence of a SQLite regression; the check remains failed/open.
A valid repair needs actual automatic-valuation and share-conversion receipts through the existing
production path, including the fixture’s no-action variant. Do not synthesize a digest, relax the
gate or count this check as passed. Pair its repair with the genuine saved Investment Brief
workflow; storage/startup evidence is separately reported.

Bootstrap correction is implemented and frozen in the two assigned files. Lead inspected
FIFO ownership, byte charges, original clocks, cancellation before dequeue and shared active
admission. Existing source error display now includes its closed subscription failure; live test
errors identify first-start versus resumed-start phase. Single compiler runs
`sqlite-bootstrap-admission-build.log`; same real source/restart case follows. No source edits
during compilation.

Bootstrap build passes5m39s. Live case fails132.40s on first start with generic source
network failure (`sqlite-bootstrap-admission-live.log`). No completion claim. Lead owns bounded
closed-stage diagnostics in Coinbase source transport. Astra research_options_failure owns
read-only follow-up on stale pre-ACK rows: only admitted output may wait for paired canonical
publication; preserve raw capture and do not fabricate fresh clocks. No timeout/cap increase.

Direct public endpoint diagnostic opens in385ms and receives real subscription/data frames
within903ms, including data between partial subscription messages. One unauthenticated connection,
no payloads/credentials logged. This does not prove the app transport path. Source tracing now logs
only closed operation stages; existing workspace tracing dependency added to Coinbase manifest/lock.
Astra research_options_failure owns sink.rs, coinbase_publication.rs and its supervisor: qualify
buffered output before raw handoff; freshness-unqualified frames still seal exact custody via the
existing raw-only disposition, but do not await nonexistent committed rows. Current rendezvous
wait is finite; the defect is occupying admission until that timeout, not an infinite wait.

Stale bootstrap correction frozen/released and inspected: input carries a closed
AwaitCommittedRows/FreshnessUnqualified disposition; both seal exact raw evidence, only admitted
rows wait for canonical publication. Lead transport diagnostics distinguish connect/send/flush,
receive and publication-admission timeout without frame bodies or credentials. Single-job build:
`sqlite-bootstrap-raw-only-build.log`; existing live restart case follows on unchanged source.

The data restart, bulk-history and Fund NAV critical checks below pass. Next dependency after the
coherent pushed checkpoint: bounded archive handoff/read/reclamation and incremental raw custody.
No live or installed completion, broad CI, whole-app measurement or new branches/worktrees.

The following dated investigation records retain the evidence and ownership handoffs that led to
this candidate; their earlier compile/assignment states are superseded by the current paragraph.

The second live run exits normally with a failed assertion (132.64s), without a stack overflow:
`sqlite-public-live-restart-rerun.log`. The concrete remaining source failure is four-slot raw
publication admission using `try_send` before async workers can drain a burst. Lead owns the
shared `sources/src/live.rs` capacity hook; Astra research_options_failure owns the corresponding
Coinbase adapter receive loop and application `live_source/{sink.rs,coinbase_publication.rs}`.
Reserve existing channel capacity before receiving the next frame, retain the owned permit until
that frame's atomic submission, and use the same strictest deadline/cancellation for waiting and
receiving. No larger queue, discarded input, replacement runtime or new branch. All other source
is frozen; lead rebuilds/reruns after this bounded producer-to-consumer correction freezes.
The same defect exists in Kraken's adjacent public feed. Extend the shared sink reservation enum
to Kraken: Astra owns `live_source/kraken_publication.rs` as well; lead owns the existing Kraken
adapter `session.rs` receive loop. Both providers use the same capacity hook and their original
receive deadline; Kraken retains quarantine and subscription-release semantics on failures.

Integrated build passes (5m29s) after lead updated two paper-market timestamp callers. The rebuilt
live public-source restart case still aborts on worker stack overflow after Coinbase's durable
publication queue fills; evidence `sqlite-public-live-restart.log`, executable UUID
`A12542C9-72E1-32BE-8F7E-62D6564FD76D`. The first boxing change reduced the two startup poll frames
from 1,481,920 to 852,992 bytes but does not close the crash. Source checkpoint is not accepted.
Next bounded remediation: Astra live_stack_failure owns startup future layout inside
`application/market_runtime.rs::start_owned` and its cohesive private helpers only, coordinating any
other path with lead; Astra research_options_failure investigates the publication queue stall
read-only in live-source sinks/data ingest. All other source stays frozen; lead owns integration,
Git and the next single-job check. No thread-stack inflation or dropped market data as a fix.
Queue investigation found no missing consumer or proven SQLite lock cycle. It did identify a
state-check-before-notification-registration race in the shared crypto rendezvous. Astra
research_options_failure now owns only `research/ingest/crypto_market/kraken_rendezvous.rs` to
register the existing waiter before reading state (including expiry driver), preserving deadlines
and cancellation. This small substantiated correction joins the startup fix before compilation;
no queue capacity increase or unproven broad backpressure change is authorized for this check.

Active-event/valuation integration status (2026-10-01, audit base `4d0893d5`): all agent-owned
implementation files are frozen and released. Lead owns the integrated candidate and single-job
compiler. The extended existing event restart case passes 1/1 in 1.57s, including retained exact
input reconstruction and rejection of a changed canonical-event digest; evidence
`.agents/tmp/v1-first-stock/sqlite-active-events-retained-rights-critical.log`. Existing bulk-stock
history and Fund NAV restart cases also pass against the revised schema. Fresh and retained
valuation callers now distinguish physical research roots from logical event inputs, without
changing financial formulas. The combined application control-plane harness is compiling in
`sqlite-app-integration-build.log`; next is the previously crashing live public-source restart case
with the boxed child future. These are implemented changes under critical verification, not a
live/installed completion claim. Archive handoff/reclamation and incremental raw-custody recovery
remain subsequent dependencies; no new branches, worktrees, broad CI or RAM measurement.

While compilation runs, Astra first_brief_readiness has a read-only dependency brief: identify the
smallest bounded archive transaction and reader handoff using the current event store and existing
Parquet/pin owners, including exact files and reuse points. No source edits, builds, Git or new
framework; report the concrete integration seam to lead before the next implementation dispatch.

Archive dependency handoff is complete (read-only): export whole publications in bounded commit
order using the existing staged Parquet writer/finalizer, then atomically admit object metadata,
publication row offsets and keyset progress. Resolve active versus archived placement inside the
existing WAL snapshot; preserve all logical coordinates. Before payload deletion, integrate the
complete-commit view, guarded delete/retry paths, referenced-object recovery/backup enumeration
and existing verified-file reader. Analytical query admission must use genuine logical event
authority, never fabricated manifest roots. This is the next coupled checkpoint after current
producer/consumer checks, not implemented behavior or a separate task queue.

Astra calendar_currentness_failure has a read-only integration follow-up while source is frozen:
trace whether transient source-use permit expiry improperly prevents historical saved-valuation
display, distinguishing that display from current decision admission. Return a concrete affected
workflow and minimal correction only if substantiated; no implementation or separate review round.
Follow-up resolved without code changes: historical valuation/Brief reads retain original receipts
without current permit expiry admission. Active decision reuse is already bounded by the selected
market's freshness (current V1 policy 60 seconds), stricter than the 300-second source-use permit.
Fresh authorization must not revive that stale market input; combined event recovery clocks agree.

Active SQLite focused check passes: existing publication recovery case 1/1 in1.46s (compile43.47s), including exact succeeded retry, independent reads while writer is held, selected-row digest corruption rejection, all PIT ties, original raw/native evidence after newest commit/restart, and3 logical commits/35 canonical rows/0 Parquet artifacts. Source trigger placement and physical-only output coverage guard were corrected in the active schema; no forward migration. Evidence `sqlite-active-events-critical.log`. Valuation consumer/retained-rights integration remains in progress, so this is data-slice verification only, not a complete application or archival claim.

Financial caller integration: Sol desktop_disconnect_recovery owns application/fair_value/automatic.rs and automatic/{forecast,financial}.rs plus service/market_evidence.rs. Dependency is the Astra canonical valuation rights/evidence contract; no financial formula changes. Lead reserves shared data/valuation exports and market_selection/investment.rs visibility. Complete both fresh calculations and saved-result reauthorization with exact event inputs.

Valuation integration ownership extension: first_brief_readiness owns evidence.rs and valuation persistence{.rs,/write.rs,/recovery.rs}; calendar_currentness_failure owns valuation automatic.rs exact event admissions alongside existing physical-source rights, and its data research_use/market_event.rs only after compiler release. Lead owns application valuation callers, shared exports and all checks. Logical events remain exact retained evidence and are never represented as fake physical manifests. This closes affected required financial consumers, not a new architecture project.

Newly traced financial consumer: Astra first_brief_readiness owns `crates/market-squawk-valuation/src/evidence.rs` event-publication identity/validation conversion only. Existing valuation paths still expected a physical event manifest; update to logical commits with unchanged financial rules. Lead owns any shared serialization contract and downstream composition, and compiles after freeze.

Integration ownership refresh: all active-event implementation slices are frozen; lead integrates shared modules, rights wrappers, canonical SQL hashes and application fixtures. Astra live_stack_failure owns only `apps/market-squawk/src/paper_bot/defaults.rs` to box the identified large child future before the deadline wrapper; no larger thread stack. Astra research_options_failure owns only the existing `provider_market_event_publication_is_restart_queryable` case in `data/tests/publication_recovery.rs`, converting its physical event assumptions to logical commits while preserving exact/PIT/retry/restart and selected-evidence integrity checks. No other test expansion, builds, Git or runtime by agents. Lead runs the single compiler after these files freeze.

Live source check failure: unchanged compiled control-plane executable aborts on Tokio worker stack overflow after Coinbase publication queue reports full (independent-startup-public-live.log). Source checkpoint remains staged, uncommitted and unaccepted pending cause/fix. New Astra live_stack_failure owns read-only crash/backtrace investigation and smallest exact correction proposal; no builds/Git/source edits. Database lanes continue in their assigned files.

Source-readiness integration: the existing installed shared-service critical passes (1/1,106.80s; compile6m08s), including native/MCP authority, concurrent reads, optional access and restart. Service admission now schedules saved research/account restoration in the existing retained startup owner instead of awaiting provider network work before transport readiness. Public source preparation remains independently retained and cancellable. This does not establish native live-price/startup completion; the existing public-source live publication/restart case is next using the unchanged compiled executable.

Active-event implementation wave (2026-10-01): direct-lineage checkpoint `4d0893d5` is pushed. Lead owns SQL, exported logical commit types, shared `ingest.rs`/module registration, application composition and all verification. Source-readiness critical check occupies the only compiler slot; no agent edits compiled files until released. Dependency contract is `.agents/tmp/v1-first-stock/active-row-contract.md`.

| Owner | Finishable active-event outcome and exclusive files | Dependency / critical check |
| --- | --- | --- |
| Astra first_brief_readiness | Active SQLite writer/retry and canonical row reconstruction in new cohesive `data/src/catalog/market_event_store.rs` and `data/src/ingest/market_event_store.rs`; propose exact changes to existing `catalog/provider_event.rs`, publication transaction and SQL for lead integration before editing those shared files | Lead logical commit/SQL contract; acknowledge only atomically committed native evidence + rows + successful run. Existing publication restart/retry case. |
| Astra research_options_failure | Indexed event PIT/current selection and exact row reopening; initial contract alignment only, then `provider_event_selection.rs`, `catalog/market_recovery.rs`, event portions of `manifest/catalog.rs`, and cohesive `ingest/market_event_read.rs` after lead releases them | Writer logical commit/row contract; preserve all equal-time candidates, clocks and raw evidence. Existing PIT/restart case. |
| Sol desktop_disconnect_recovery | Application event receipts/publishers in research/ingest/{crypto_market,schwab_market,yahoo_enrichment,alpaca_historical/market}.rs; market_selection/investment.rs; market_runtime/{schwab_sink,schwab_streamer/publication}.rs; source provider_runtime discovery wrapper and service crypto fixture after exact path confirmation | Logical commit APIs and paged route discovery; update all affected application consumers together, lead owns shared exports/tests. |
| Astra calendar_currentness_failure | Exact event research-use authority boundary: inspect existing grant selection/permit revalidation and propose the smallest shared implementation contract; no source edits until lead assigns ownership | Active event receipts cannot impersonate physical manifest roots or bypass source-use rights. Lead owns shared authority integration. |
| Lead | Source readiness completion, shared contracts/schema and application integration; archive/query ownership follows these exact APIs | Producer and consumer must integrate together. Active SQLite slice does not claim completed archival, bounded retention or continuous-ingestion acceptance. |

Agents first return exact API/signature amendments, then receive one coordinated implementation release; no replacement plans, new database/runtime, Git/builds or branches. Reuse current canonical payload encoding and precision. Archive publication/reclamation remains required and follows the active read/write slice. Raw-capture custody also has a confirmed cumulative 25,000-object/512-GiB admission/recovery boundary (`catalog/provider_capture.rs`, `ingest.rs`, `ingest/source_backup.rs`). Its current whole-store recovery ownership must become incremental/indexed before continuous-ingestion acceptance; merely removing its constants would leave the platform recovery ceiling and startup scan. Preserve all raw evidence while resolving this after the active-event checkpoint.

Direct-lineage integration evidence, 2026-10-01: existing event restart/compaction case passes (1.44s), complete stock-history/PIT/native-evidence restart passes (1.52s), and Fund NAV exact vertical passes (0.69s). The first event run exposed stale embedded schema hashes; lead updated the two current schema fingerprints in place and the rerun passed. No new migration or compatibility path. The application control-plane harness compiles with all changed callers (6m37s); no live or lifetime-scale acceptance is claimed. All implementation agents are frozen; the lead is closing the source-readiness wait before resuming active-row edits. The active-row implementation contract is recorded at `.agents/tmp/v1-first-stock/active-row-contract.md` and awaits the coherent integration checkpoint, not further owner approval.

Owner authorized the recommended database-primary implementation on 2026-10-01; the lead owns the design choice. No further design permission is pending.

Resumed integration wave, 2026-10-01, audit base `0e1a2291`: previous turn made progress by pushing the independently checked storage decision. Acceptance items 1/2/5/7 remain blocked by incomplete durable ingestion and source startup. No completed Brief or live-price claim.

| Owner | Outcome and exact ownership | Dependency and smallest critical evidence |
| --- | --- | --- |
| Astra research_options_failure | Finish already-started direct-lineage consumers in data `manifest/catalog.rs`, `manifest/catalog/lineage.rs`, `manifest/availability.rs`, `provider_event_selection.rs`, `catalog/{provider_capture,provider_logical,read_snapshot}.rs`, and option-selection owner as discovered; data ingest/SEC changes already frozen | No new storage partition design. Shared SQL remains lead-owned. Existing publication/history/NAV restart and PIT cases must retain inherited evidence; report exact changed files before editing any additional owner. |
| Astra first_brief_readiness | Concrete active-row ingestion implementation contract only, in `.agents/tmp/v1-first-stock/active-row-contract.md`; inspect current event writer/selector/schema and existing DataFusion providers | New database-primary direction; specify smallest producer-to-consumer slice and exact schema/API/file ownership, durable ack, late revisions and hot/archive union; no application edits, tests or independent framework. |
| Lead | Canonical SQL, all source-startup/composition WIP, fixture integration, compilation, Git and runtime | Apply reviewed existing-schema edits, inspect released source startup, serialize one-job critical checks. Push a coherent independently useful checkpoint before starting further implementation. |

All other implementation files remain frozen. Order: finish inherited lineage consumers + lead SQL/source integration → focused checks/push → database-primary active event slice using the concrete contract → native workflow. The lineage correction remains necessary for bulk history and exact saved evidence; it does not substitute for the active-row storage change. No new branches/worktrees or broad gate.

Storage research synthesis, 2026-10-01 (independent evidence audit PASS; report SHA-256 `e52d52338685734c90c8b54b7c682f3d4a18dc79d0e06f88da1e0f57c6591b7d`): [decision report](../research/2026-10-01-market-data-storage/final-report.md)
recommends database-primary active microbatches and indexed reads using SQLite, with bounded
cold Parquet queried through existing DataFusion. This deliberately revises the prior control-plane-only
boundary; it is not implemented or a measured performance winner. Native DuckDB remains a
compared alternative if it removes enough custom lifecycle machinery or the required workload
outgrows the proposed writer. No lifetime-cap increase or whole-history single-file rewrite is
accepted as completion. Required handoff: shared logical evidence/acknowledgment contract →
active ingestion/selectors → pinned archive publication → bounded maintenance and evidence-safe
reclamation → critical restart/consumer verification. Frozen direct-lineage/startup WIP remains
preserved and uncompiled; reconcile it with this direction before resuming storage edits.
No build, runtime launch, CI, new branch/worktree or whole-app RAM measurement ran during research.

Owner storage-architecture research request, 2026-10-01: pause further physical partition/compaction design implementation pending a primary-source comparison of database-primary versus hybrid immutable-columnar storage for the actual local workload. Direct-lineage agents freeze their coherent WIP; independent source-startup lane may finish and freeze. Research is recorded under .agents/tmp/research/market-squawk-embedded-market-data-storage-architecture. Four bounded discovery/deep-dive categories (GitHub, papers, official docs, primary engineering sources) precede synthesis and evidence verification. No build or new worktree during research. This is the requested architecture decision, not a new delivery-quarter review.

Current checkpoint `140cfa1c` is pushed to origin. The following supersedes older ownership rows below. Continuous ingestion remains incomplete until both lifetime lineage and physical object traversal are corrected.

Frozen implementation wave, 2026-10-01 (all implementation owners have released files; not compiled or accepted):
- Astra research_options_failure: direct publication/source-run lineage and exact indexed ancestry in data manifest/catalog, availability, typed history/NAV and capture/logical consumers. Return canonical SQL edits for lead application; no Git/build/runtime. Existing per-operation bounds remain; cumulative copied closure and lifetime admission limits are removed together. Lead owns critical fixtures and source-qualified application dataset callers.
- Sol credential_runtime_lifecycle: source_lifecycle.rs and cohesive public-preparation child, source_lifecycle/credential_access.rs, local_product/startup.rs, local_product/credential_access.rs and application/market_runtime.rs. Retain two independent public preparations in the existing task owner; prepare outside gates; Stop/Lock/shutdown cancel/join with readmission excluded; commit revalidates original saved intent. Lead owns composition, runtime error, local_product/mod.rs, service/mod.rs and shared exports.
- Astra first_brief_readiness: typed history/NAV direct-edge conversion in manifest/market_history.rs, market_history/nominal.rs and fund_nav.rs, using the core lane exact ancestry helper; no builds/SQL/Git.
- Sol desktop_disconnect_recovery: exact-membership API callers only in application/research/ingest.rs, ingest/{alpaca_historical/market,schwab_market,crypto_market,yahoo_enrichment}.rs; no partition edits or tests. Coordinate new indexed membership API with core data owner.
- Astra calendar_currentness_failure: read-only concrete partition admission/selection design, including rollover idempotency, cross-partition PIT ties and existing 256-route bound; no source edits.
- Lead: bounded source-qualified physical partition contract (existing indexed discovery preferred over rewriting all manifest readers); bounded-range/paged-object storage contract and canonical SQL, shared integration, tests, native runtime, Git and serialized single-job compilation. Storage handoff must cover the one-file compaction ceiling and eventual object-list ceiling; raising caps is not acceptance.
Resume dependency: finish the storage research decision, reconcile frozen storage WIP with that decision, then inspect producer/consumer integration → existing critical restart/cancellation checks → coherent push → native workflow. No release gate or whole-app RAM measurement at this stage. Original live catalog and recovery evidence remain preserved.

Compaction integration checks complete: data restart case1/1 passes1.45s; native Desktop/service/CLI build passes5m51s; control_plane consumer harness compiles5m49s but the initially supplied selector matches0 tests (compile evidence only). Correct existing live_pipeline consumer selector then passes1/1 in0.02s. Original MSFT analysis now saves completedSteps6 but remains paused/interrupted with0 results. The frozen checkpoint includes the typed bounded restart-reader callers and only their deadline hunks in the public-crypto fixture; independent startup/rate changes remain explicit WIP. Next barrier is direct lineage without lifetime ceilings and independent startup, not another whole-app gate.

Next dependency/ownership wave (execution after current checkpoint is pushed): Astra research_options_failure owns the cohesive data-lineage correction in manifest internals and existing canonical SQL; lead approves exact contract and owns caller/test integration. Sol credential_runtime_lifecycle owns source_lifecycle.rs plus cohesive public-preparation child, credential_access cancellation hook and market_runtime.rs prepare/commit boundaries. Lead reserves local_product/mod.rs, startup.rs, service/mod.rs, composition/shared contracts, all tests, Git/build/runtime scheduling. Outcome: retained source startup runs independently, network/rate waits hold no global lifecycle/registry mutation lock, and Stop/Lock/shutdown cancel/join preparation before commit; saved intent/session/revision must be revalidated. No rate/deadline inflation or additional runtime/scheduler. The first handoff is the exact public preparation API and file list; no source edits during the current frozen compilation. Source-qualified dataset caller integration follows the data API agreement and remains lead-owned.

Native candidate compaction build passes (5m51s). Real preserved dataset advances1024objects/16782rows → compacted generation1025 with1object/16782rows → generation1026 with2objects/16800rows. All17 native routes settle without active-query errors, but Home/Markets truthfully show enrichment unavailable; this is not price/workflow completion. Source restoration reports0 restored and runtime publication revoked; shared startup timing/global network-preparation locks remain the source-lifecycle dependency. Original MSFT resume still has no result. Owned native17681/service17824 were stopped to end repeated failed recovery retries. Evidence: market-compaction-native-build.log, market-compaction-native-routes.json, market-compaction-msft-{resume,status}.json. Source/caller consumer compilation occupies the only compiler slot before committing the coherent storage checkpoint.

Owner explicitly confirmed continuous-ingestion lifetime correctness on2026-10-01: the4096 publication/source-run boundary is required remediation, not an accepted limit. Next storage wave follows the frozen compaction checkpoint: Astra research_options_failure will own manifest catalog/availability and canonical lineage SQL after lead releases the shared files; lead owns affected callers, tests, schema integration, runtime, Git and compilation. Before editing, trace complete consumers and return exact contract/SQL file list; update active V1 definitions in place, with no new migration or compatibility stack. Preserve existing local catalog and recovery evidence; live verification must use a compatible canonical V1 workspace without silently replacing saved data. Source-qualified physical datasets and provider-neutral discovery are part of the same required multi-provider correction. No additional branch/worktree is authorized.

Streaming-compaction critical passes (market-compaction-critical.log, 1.45s; compile47.49s). Existing event restart case now crosses a configured two-object threshold, preserves row count/lineage, accepts the third publication, retains current/PIT tie semantics and reopens original raw/native evidence through the compacted generation after restart. Lead's exact-origin resolver validates parent ancestry and original artifact identities. App trigger and bounded caller changes await native compilation/live proof; no stock completion claimed.

Required next storage correction is concrete, not optional hardening: source-publication lineage is copied cumulatively and rejected at4096 publications/source runs (catalog.rs, availability.rs and canonical SQL ordinal constraint). Compaction retains that lineage and therefore does not close continuous-ingestion acceptance. Replace cumulative copied closure with direct publication edges plus exact parent ancestry and paged enumeration. The shared physical market-event dataset also conflicts with the existing single-source manifest invariant; central source-qualified physical datasets must update producers and selector guard together, retaining provider-neutral logical discovery. Do not drop source-rights checks or merely raise limits. These findings must be integrated after the current coherent compaction checkpoint, with ownership refreshed before edits.

Current integration ownership: lead owns schema/catalog compaction proof, Alpaca publication trigger, publication_recovery critical and all builds/Git. Sol desktop_disconnect_recovery owns bounded explicit market-event restart-reader caller updates in research/ingest/{crypto_market,schwab_market,yahoo_enrichment}.rs, service/crypto_installed_fixture.rs and directly affected caller/test contracts only; coordinate any additional path before editing. Existing API now accepts Arc capture store and caller-owned deadline. No new timeouts, builds or branches. Public rate lane is frozen awaiting composition/scheduling integration. The owned native Desktop/service were stopped after repeated failed publication retries; Vite remains available.

Implementation wave: stock publication requires streaming market compaction plus exact original-publication ancestry reads. Astra research_options_failure owns ingest.rs and a cohesive ingest/market_compaction.rs child only (market_event.rs only after naming a needed shared helper). Reuse existing PinnedBatchCursor/StreamingParquetWriter; no whole-history collection, cap increase, object deletion, build or Git. Lead owns manifest/catalog.rs, shared schema helpers, app research/ingest/alpaca_historical/market.rs trigger/rights, existing publication_recovery critical, integration and checks. In parallel Astra calendar_currentness_failure owns only live_source/crypto_reference.rs and crypto_reference_transport.rs for canonical existing REST policy and deadline/cancellation-aware budget waiting; no rate change or startup scheduling edits. Provider scheduling remains a lead-owned integration dependency, so that slice cannot be accepted complete alone. Both lanes must return exact API contracts before edits, freeze promptly and name remaining dependencies.

Next bounded wave: Astra research_options_failure owns read-only trace of the live Alpaca publication manifest1024-object rejection and existing compaction integration; return exact producer/consumer correction without increasing caps/deleting data. Sol credential_runtime_lifecycle completed public scheduling proposal; lead retains shared startup/composition/lifecycle hotspots, all Git/runtime/builds. No new implementation agent edits until exact scope is assigned. Native collection checkpoint c24b1a8f is committed; independent public-source and starter-history WIP remains preserved.

Native candidate b72d5cd2 builds (6m57s) and starts password-free. After readiness, actual Home/Markets show all nine named starter investments with no query errors (collection visible521/531ms; calendar-collection-settled-native.json); the other15 settled routes have no active-query errors. First two sweep samples captured startup and are superseded only by this settled two-route check. CLI collection independently returns all nine saved choices/revision3. Prices remain unavailable. Original MSFT resume advances completedSteps4→5 then pauses/interrupted with no result; that service5312 was subsequently stopped by the lead to end repeated failed-publication retries. Native log sequence366 proves next stock barrier: manifest requires compaction before exceeding1024objects. No cap increased or saved data deleted.

While native code remains frozen, Sol credential_runtime_lifecycle has a bounded read-only follow-up: inspect existing startup-task/source restoration ownership for policy-derived rate-admission waiting that does not delay unrelated sources or ordinary pages. No edits, new scheduler, rate change, build or Git action are authorized to that lane; return exact existing boundary and smallest correction proposal. This addresses the demonstrated fresh public activation dependency, not a general orchestration redesign.

Public-source cause is now bounded: reference REST registration incorrectly carries its WebSocket policy into the REST host already registered by onboarding. Fresh anonymous setup also spends the shared REST allowance. Official anonymous REST documentation does not establish a numeric quota; the existing 1/min is a local conservative fallback, not a documented provider maximum. A constant-only change would conflict with retained policy state. Remaining correction must unify REST ownership and handle rate admission waiting/cancellation; no arbitrary rate/deadline increase or authority bypass has been made. Native stock/Desktop proof proceeds independently.

Serialized native candidate build is running (calendar-collection-native-build.log); all code is frozen during compilation. Provider investigator remains read-only, checking documented REST policy and correct shared-budget waiting: setup and reference both spend the Coinbase REST allowance, so a policy-only change would leave fresh activation incomplete. Existing Coinbase consumer contract test passes (0.02s; coinbase-contract-consumer-critical.log). Lead will run actual collection/screens and original MSFT resume with the independent calendar/raw-read/deadline fixes.

Installed shared-service critical passes (101.90s; independent-collection-service-critical.log), including concurrent startup reads, nine default choices, revision-checked removal, optional access and saved choices after restart. Final renderer critical passes (0.76s) and typecheck passes; price refresh no longer extends a completed choice mutation. Native transport compilation/live screens remain the next collection barrier. Public reference investigation identifies a REST/WebSocket policy mismatch at one shared Coinbase host; shared enforcement is correct and remains unchanged pending the bounded caller correction.

Public registry diagnostic compiles (6m57s) and fails at coinbase_register with RegistryError::BudgetCoordinator before network dispatch (12.86s; public-default-registry-diagnostic.log). Astra credential/runtime investigation is assigned read-only to calendar_currentness_failure: exact shared budget registration/composition path and smallest correction, no edits/builds/Git. All implementation files remain lead-owned/frozen. Lead runs the existing installed-service startup/collection/restart critical, then integrates the native candidate. Calendar checkpoint `6ef35767` is pushed (PR43 comment5925522990).

Calendar reobservation critical passes (1.60s; calendar-reobservation-critical.log): identical later sealed downloads retain fresh physical custody and the original canonical manifest/binding, changed native facts are rejected, and exact replay/reobservation survive reopen. Lead inspected all four changed files. This closes the demonstrated calendar content-reuse defect at the durable boundary; original MSFT live resume still awaits the integrated native build. Public registry diagnostic now occupies the sole compiler slot.

Frozen integration wave: both implementation lanes released their files. The existing Desktop market journey passes (0.79s; independent-collection-renderer-critical.log): real saved choices remain visible and revision-checked Remove/Keep work when enrichment fails. Desktop typecheck passes. The calendar reobservation/restart critical is compiling under the single compiler slot. Immutable raw-read checkpoint is pushed as `1a578cad` (PR43 comment5925458836). Shared collection contracts/CLI/native transport and starter-history operation lifetime remain pending native verification; no claim of live prices or a saved Investment Brief.

Immutable raw-read critical passes (0.08s; immutable-raw-read-critical.log): controlled open/verify succeed under the real seal mutex and concurrent reader, recovery still excludes both, prior corruption/cancellation checks remain. Lead inspected the two platform files; independent checkpoint follows. This fixes a demonstrated normal-concurrency failure without asserting that old generic logs uniquely identified it. Desktop lane additionally owns one extension of the existing app.test.tsx market journey; lead carries includeMarket through shared contract/CLI/native transport. Lead corrected starter history to begin its existing recovery lifetime when the owned worker starts, rather than inherit already-consumed activation time; original admission/current-generation/cancellation checks remain.

Implementation wave after completed diagnostic build: (1) Astra research_options_failure owns platform journal/sealed.rs + sealed_object.rs and the existing inline sealed-segment recovery case: immutable reads bypass seal mutation mutex, shared recovery exclusion preserves destructive recovery. (2) Astra calendar_currentness_failure owns data ingest.rs, catalog/provider_capture.rs, existing publication_recovery.rs and app market_calendar/alpaca/durable.rs: identical fresh calendar capture uses exact validated reobservation, retaining original publication evidence. (3) Sol desktop_disconnect_recovery owns paper/market/durable_product.rs and renderer market-collection.tsx: durable starter choices load independently from explicitly requested price enrichment. Lead owns contracts.rs, native/renderer transport, CLI, compilation/Git and public-source error trace. These independent slices join at one serialized critical/native build; no new branches, worktrees, generic infrastructure or relaxed validation.

Desktop read-only lane: desktop_disconnect_recovery traces why Market.GetCollection market-price work gates all starter identities on Home/Markets. Scope durable_product.rs, market collection contract and renderer; propose the smallest existing-boundary separation without fake prices or duplicated authority. No edits/builds. Lead retains shared contracts and integration.

Pushed startup deadline checkpoint ff8a9474 (PR43 comment5925317238). Actual native sweep15/17 routes without errors; Home/Markets still time out. Original MSFT resume survives in the same service87828, but pauses at current_session/source_evidence_invalid (completedSteps4, no result). Matched binary5e86b40c reduces the demonstrated nested stack path including outer ancestry/worker from over2MiB to1,940,656 bytes; this is bounded path evidence, not every later workflow. Astra research_options_failure traces retained raw-object verification and current market reads read-only; Astra calendar_currentness_failure traces current_session and starter extraction failure read-only. Lead owns crypto-reference diagnostics, serialized compilation and integration.

Operation-owned startup deadlines are live verified on preserved workspace: native UUID5e86b40c (build5m22s) reaches Ready without password or startup timeout. Current CLI setup status independently confirms approved Ready revision1 and all allocation values after restart. This checkpoint does not close screen/data failures: Home collection still exceeds its request deadline, public crypto catalog admission fails, and starter history reports extraction deadline. Native17-route sweep and full rebuilt stack-path inspection remain active before original MSFT resume. Evidence: operation-deadline-anchor-native-build.log, approved-practice-setup-current.json, current installed startup state.

Public-source diagnostic compiled in5m13s and failed13.15s at reference catalog admission, after successful anonymous activation (public-default-live-diagnostic.log). Astra credential_runtime_lifecycle traces that exact catalog error read-only. Lead has started the one native build with operation-owned recovery deadlines and heap-owned action anchor (operation-deadline-anchor-native-build.log); no other compilation or code edits run concurrently.

Startup sampling confirms catalog integrity and executable hashing consumed the shared recovery lifetime (startup-service-sample-{2,3,late}.txt). Lead corrected operation-owned deadlines in local_product/mod.rs: capture recovery after catalog open, credential opening, reconnect binding, then one combined retained valuation/decision recovery. Existing duration, cancellation/reap and integrity checks remain unchanged. The source-action optional anchor now owns its two cursors on the heap. Both patches are frozen for the serialized public-source critical/live diagnostic, native rebuild and matching-binary stack inspection. No startup or stock-analysis completion claim yet.

Current wave: native UUID7f3822da compiles but startup fails twice at runtime composition with the shared recovery deadline. Lead owns startup sampling, local_product/mod.rs and integration; Astra calendar_currentness_failure traces the deadline read-only. Astra first_brief_readiness owns only corporate_actions/applicable.rs and applicable/anchor.rs to heap-own the confirmed 9,696-byte plan anchor at construction; existing preparation/history edits remain frozen. This corrects value ownership without larger worker stacks or data restrictions. Dependencies: both causes corrected → one serialized build → matched stack path check → native startup and original MSFT resume. Public runtime remains separately unverified after its live fixture failure.

Public-default critical passes (0.11s; public-default-lifecycle-critical.log). Existing authorized live restart case compiles but fails after 12.85s: Coinbase anonymous session is ActiveScoped, lifecycle revision2 is blocked/reconciliation and runtime not active. This closes neither public runtime nor restart acceptance. Lead retains integrated native build/diagnostics; credential_runtime_lifecycle traces the exact post-onboarding Start failure read-only before further edits.

Pushed instrument checkpoint `91100370` (PR43 comment5924839999); Coinbase adapter tests compile (2m28s). Public-default startup is now frozen for lead verification: missing anonymous sessions caused the Unauthorized before any provider call. Existing onboarding now creates/reuses the exact verified session and ordinary Start preserves revision/Stop checks; interrupted bound Start/Retry identities are covered in the existing critical case. The serialized lifecycle critical is running, followed by the existing live public-crypto restart case and native build/resume. History stack allocation and closed InvalidResult diagnostics remain frozen, unproven live.

Instrument snapshot checkpoint: existing native-reference custody/restart critical passes in 0.98s with reads under the real writer lock, cancellation and expired requests. All five external constructor callers are updated. The application production library compiled in the consumer check, which then stopped on three pre-existing stale live-pipeline fixture API calls; lead corrected those callers separately and will recheck after the public-source lane freezes. No claim that native prices or the analysis are complete.

Current integration: both instrument snapshots and history future allocation are frozen and owned by the lead. The held-writer instrument custody/restart critical passes (0.98s; instrument-snapshot-critical.log); all five external constructor callers are updated together and their adapter compilation passes; application unit compilation is included in the current lifecycle critical. Next barrier is the serialized native rebuild, matched stack-frame comparison and original MSFT resume. Astra first_brief_readiness only updates the scratch frame-comparison helper; no implementation agent owns active edits.

Saved-source Retry correction is critically and live verified: constructor/control test passes; new native startup restores the Alpaca source without the previous InvalidRequest and without a password. Public Coinbase/Kraken restoration reports Unauthorized and remains unresolved. MSFT resume pauses on retained `invalid_result` at Market.PrepareInvestmentEvidence (same service remains alive, completedSteps3), so stack/source preparation is not accepted complete. All17 routes load with no query errors; both Home/Markets still have unavailable prices.

Native rebuild passes (6m12s; `preparation-stack-native-build.log`). Matched binary UUID `0ca71f5a-08a2-35d7-86d5-99f729f61156` reduces the four crashing ancestor polling frames from 1,628,784 to 266,992 bytes; source acquire remains 787,696 bytes and current-source acquisition is 90,176 bytes. Evidence: `market-preparation-frames-{before,after}.jsonl`. This measures compiled stack frames, not whole-app RAM. That follow-up measured the remaining nested history path; the frozen correction and current ownership are recorded above.

Automatic saved-source Retry constructor now passes its critical contract/cancellation/deadline case (`automatic-retry-contract-critical.log`). Preparation allocation changes compile in that same application test build. That earlier native build completed; the subsequent history correction still requires rebuild, matching-binary frame comparison and original MSFT workflow resume. No stack setting or data restriction changed.

Pushed retained-calendar snapshot correction `6c166637` passes both existing critical cases: complete history/native replay/restart with a held real writer (1.47s) and event publication/routes/metadata/restart (1.38s). Evidence: `calendar-snapshot-{history,events}-critical.log`. The first history run failed because its single-thread test executor blocked in the writer seam; matching the existing two-worker concurrency fixture fixes the test without changing application timeouts or stack size. All five bounded immutable recovery reads now use independent snapshots; publication/reobservation writes retain authority. Live verification follows the application rebuild.

Pushed integration `3ba9da1c` and identity snapshot `095b8c4a` are followed by a successful native build (6m13s) and Desktop typecheck. Actual hidden native sweep loads all17 routes with zero active-query errors; Home5.61s, Markets5.11s. Startup is password-free and saved recommendation setup remains ready. Prices still unavailable: startup Retry construction violates the closed command contract, and an ordinary CLI Retry succeeds but preparation reaches a separate retained-calendar read/writer conflict. Current ownership below fixes these actual remaining barriers; neither native price nor full stock journey is claimed complete.

Native identity selection now keeps only trusted-clock/watch capture under the writer and resolves/replays evidence in an independent read snapshot, then checks revocation. Shared identity replay is reused by publication and reads. The existing native-reference custody/restart case passes with held-writer waiting, cancellation and expiry added (0.58s; `native-identity-snapshot-critical.log`). This is focused correctness evidence from the integrated working tree, not live price completion or release approval.

The historical adapter now physically seals and rejoins native bar/page evidence before canonical ingestion. Its existing terminal-page/token-chain critical case passes (0.10s; `historical-native-handoff-critical-final.log`), including mismatched seal and revoked identity rejection. Application integration and native price/restart remain unverified.

Pushed lease-bound renewal: `f8f14c14` (PR #43 comment 5923796790), following independent reads `1e82c1e5` and retained history `0206d13a`. The exact-generation critical case passes (9.31s), the serialized native build passes (7m13s), and the actual native sweep loads all 17 routes without active query errors or a password (`verified-renewal-native-routes.json`). This is screen-load evidence, not complete market data: starter prices remain unavailable.

Live Retry now reaches historical extraction instead of rejecting renewed reference authority, but fails before canonical publication. Sequence 303 identifies registered ingest; source tracing found captured history remains a pending handoff that the publisher correctly refuses. Sequences 305/307 separately show native identity selection still contends with the writer. Both require corrections; no claim that all contention or live startup data is resolved.

Automatic Alpaca proof renewal and finite generation-bound history work are integrated through existing lifecycle/health/startup owners. The installed-service critical case now passes (101.53s; `automatic-renewal-startup-critical-rerun.log`), covering shared-client startup, optional Lock, saved reads and restart. The earlier private-import compile failure is superseded by this successful run, not acceptance evidence itself. Adapter capture and native identity critical cases above also pass. No new timer, runtime, expiry extension, migration or branch. Native price publication, active renewal and retained-price restart still require the rebuilt app.

The owner approved practice-only allocation: 2–5% positions, USD10,000 cash reserve, 1% marked-equity downside, 365 days. The exact preview was checked and committed through existing CLI setup operations; revision1 reports ready and survives the rebuilt service restart (`approved-practice-setup-{preview,commit,after,restart}.json`). No orders submitted. The existing USD100,000 portfolio and 0.25% simulated cost configuration remain the authorized test context.

### Prior integrated observations

Coherent retained-history checkpoint: adapter request identity/asof, shared capture contract, catalog publication/reopen validation and all application producers/consumers are integrated together. The complete-history critical case now also verifies actual native calendar rejoin at the inclusive daily boundary before/after restart (1.37s). Heap-owned extraction payloads preserve serialization and avoid the observed stack crash. Existing V1 schema/checksum updated in place, no migration path; original local catalog recovery backup retained. This checkpoint is critically verified, not live price completion: native source retry succeeds but separate calendar preparation currently rejects before history publication. Display warming/previous-close UI remains in pending working-tree integration.

Request admission is pushed in `e092a17b` (PR #43 comment 5923423225). Its native build passes (6m); all 17 routes load without query errors again. Real Alpaca retry now succeeds with applied/available receipt and does not terminate on concurrency exhaustion. Starter history still reports InvalidResult with zero history publications in the preserved catalog; lead is tracing the remaining preparation edge. This does not establish live-price completion. The real native-session boundary/restart regression passes (1.37s; `native-history-rejoin-critical.log`). Home remains Ready/connected with zero active-query errors after the successful live retry; prices remain unavailable (`request-admission-after-live-retry.json`).

Request-slot waiting passes the focused overlap/cancellation/uncharged-expiry case (0.03s). One serialized native build is running (`request-admission-native-build.log`); live price availability remains unproven. Lead owns build/native actions and integration. Astra `calendar_currentness_failure` has a read-only integration trace of the retained history identity/publication/previous-close changes; no file edits, builds or additional review round. This trace identifies concrete consumer gaps before the coherent history checkpoint.

Native backend replacement is live verified: after the owned service was stopped, Desktop launched a new service and moved to a new product-session token; both contexts became Ready, the event stream connected, and Home had no query errors or password prompt. Evidence: `native-backend-reconnect-{before,after,completed}.json` and `native-reconnect-read-probe.json`. The first 55-second observation expired while connecting; completion was observed later, so no fast-recovery timing claim. A thread sample showed no sustained synchronous native deadlock. Reconnect latency remains unexplained.

Updated native build passes (7m16s), and the preserved workspace loads all 17 routes with Ready Product/System states and no active-query errors. Home settles in 3.06s and Markets in 4.12s; all nine starter identities are visible, prices still unavailable. No password submitted. Evidence: `startup-integrated-{native-build.log,routes.json}`. Live history retry now returns without a stack crash, but fails with provider `BudgetUnavailable`/`ConcurrencyExhausted`, followed by runtime unavailability; this is not a completed live-data workflow.

Durable route discovery is corrected without schema/cache changes: compact dataset/source/venue keys and EXISTS preserve the exact original eligibility joins. Existing provider-event restart/cutoff/corruption case passes (0.99s). Read-only preserved-catalog comparisons match all 36 result sets across nine instruments and four clock/event cases; current Quote/Trade routes drop from 9.41s to 16ms total. Evidence: `route-query-{critical.log,results.json,measure.py,original.sql}`. The integrated native build is running; no Desktop timeout-closure claim yet.

Exact publication reads are critically verified in the existing restart case (1.01s), including inherited selection and unrelated/selected file corruption. Complete history/restart passes (1.19s); adapter contract case passes. Native three-binary build passes (5m52s). The native sweep still reports Home/Markets failures, so no screen-completion claim: the subsequent live retry exposed an extraction stack overflow and additional currentness contention. This data-read checkpoint is independently useful and does not claim to close those remaining defects.

Accepted startup checkpoint: automatic local access, optional connection Lock and concurrent Research loading. Native preserved runtime recovery succeeds; native/CLI builds pass, CLI provider recovery returns the structured Ready receipt, and the same installation restarts without any password submission. Both Desktop contexts are Ready and events reconnect; service status confirms provider `credentialAccess.access=ready`, `enabled=false`. Evidence: `automatic-access-native-recovery.json`, `automatic-access-native-restart.json`, `automatic-access-restart-service-status.json`, `automatic-access-client-build.log`. This is ordinary checkpoint evidence from the integrated working tree, not a clean unchanged release gate. A restart sweep passes 16/17 routes; only Markets overview still exceeds the native deadline. Previous-close history remains pending in preserved unstaged work.

Native automatic restart is verified. The remaining market read opens all 583 cumulative event objects for each quote/trade selection; an exact-publication read is integrated for critical verification. Calendar replay also misclassifies activation-lock contention as stale; its existing bounded account guard is now held across replay and released before final currentness validation.

| Outcome | Implemented in working tree | Evidence / remaining acceptance |
| --- | --- | --- |
| Automatic local startup and optional credential protection | Existing encrypted vault gains private automatic-key retention, explicit policy and rotation; service/provider composition no longer selects forced password locking by build/signing type | Existing platform lifecycle case passes (71.28s), including reopen, wrong password, explicit Lock, expiry, recovery and disabling Lock. Native Keychain remembering remains unverified. |
| Settings-only optional lock | Shared current V1 status/commands replace fallback-specific UI/transport; General Settings owns controls, connection import uses credential readiness | Desktop typecheck passes. Existing provider-boundary case passes (2.55s); no global provider-lock page gate. Real native automatic restart verified; optional Keychain remembering remains unverified. |
| Stop credential activity when explicitly locked | Reversible OAuth, research/live runtime and private-paper drain reuse existing owners; saved choices/data retained; paper never auto-trades on unlock | Lead integrated producer/consumer contracts and timer into installed service lifetime. Existing installed service case now covers default Ready, Lock with saved reads still usable, unlock, disable and restart. The integrated case passes (102.28s): saved reads and status while locked, unlock, disable, in-process restart, automatic subprocess startup and crash/restart. Native automatic restart and preserved-vault recovery now pass. |
| Reliable initial Research loading | Shared I/O owner for catalog/macro/preparation reads; complete cursor scan replaces expensive all-observation query in preparation options | Earlier five-operation case failed with exact `observation_read/query_memory`. The five concurrent startup assertions pass in the completed integrated critical run. Native Research previously passed 8/8 concurrent rounds but one Advanced profile-options load still failed. |
| Real previous-close starter data | Prior history/identity/calendar/corporate-action batch remains intact | Complete-history/restart critical case passes (1.19s). Live retry previously failed at `calendar-origin-read`; fresh metadata plus exact diagnostics identified activation-lock contention. The replay-guard correction is integrated; native price/restart proof awaits the rebuilt application. |

Evidence under `.agents/tmp/v1-first-stock/`:
`optional-access-platform-test.log`, `optional-access-desktop-typecheck.log`,
`optional-access-desktop-critical.log`, `optional-access-installed-critical.log`,
`optional-access-recovery-critical.log`,
`research-preparation-diagnostic-test.log`, `research-concurrent-preparation.json`,
`research-preparation-routes.json`, `advanced-concurrent-before.json`,
`calendar-currentness-diagnostic-retry.json`, `credential-access-diagnostic-service.log`.

### Dependency and ownership

New native startup failure at current candidate: structured log reports runtime-composition / artifact publication deadline exceeded before source-restoration report; Desktop/service exit, no new crash report. Astra calendar_currentness_failure assigned read-only exact startup artifact/deadline propagation trace, no edits/builds/runtime/Git, identify cause and minimal complete correction. Lead retains startup replay and diagnostics. Original catalog/session/backups preserved.

Native frame follow-up: lead observed rebuilt ancestors + acquire + complete + nominal + reopen reserve 1,837,184B. Astra first_brief_readiness owns read-only new-binary child/factory-frame analysis before original MSFT resume; prior native-calendar child sizes would exceed 2MiB if unchanged. No code/build/runtime/Git authority. Native service is restarted for retained startup logs; workflow remains paused pending this check.

Public-default correction: compiler has finished. GPT-6.1 Sol credential_runtime_lifecycle owns source_lifecycle.rs (or a cohesive child) and existing tests/production_mcp_composition.rs exact public-crypto startup/restart journey. Use existing anonymous onboarding, bind ordinary Start, reuse interrupted preparation and retain explicit Stop/Remove. No other writes/builds/runtime/Git. Lead owns discovered stale production_coinbase_pipeline.rs fixture callers and consumer recheck.

Concurrent read-only provider lane: GPT-6.1 Sol credential_runtime_lifecycle traces the observed Coinbase/Kraken Unauthorized during saved public-source restoration. Scope is exact startup/Retry-to-activation authorization and saved non-secret state; no credentials, runtime actions, edits or builds. Return the concrete failing boundary and smallest producer/consumer correction, or evidence that owner/provider action is required. Lead retains all shared lifecycle editing and integration.

Lead additionally owns `application/analytical_workflow/host.rs` for closed operation/failure-stage logging of the observed InvalidResult; no payload logging or validation relaxation. Preparation agent owns the matching fatal source-step diagnostic using its existing closed failure classifier.

Instrument-read correction authorized: Astra calendar_currentness_failure owns data `catalog/market_data_instruments.rs`, `catalog/market_data_instruments/native_reference.rs`, `catalog/read_snapshot.rs`, `ingest.rs`, `tests/catalog.rs`, and `tests/publication_recovery.rs`. Cache exact endpoint binding/result limits during service composition; immutable latest/population/search/enumeration/native-resolution methods reuse existing SQL on independent snapshots. Keep clock/watch mutation and borrowed publication validation unchanged. Lead owns all constructor caller changes in app provider_activation/market_config.rs, live_source/tests/supervisor.rs, paper_bot/benchmark_support/source/metadata.rs, application/paper/market/product.rs and adapter-coinbase/tests/common/catalog.rs. Existing critical custody/held-writer tests only; no new runtime/compatibility path. Lifecycle command correction is independently verified and ready for its own checkpoint.

2026-10-01 native follow-up: 17 routes have no active query errors (`preparation-stack-native-routes.json`), password-free startup; Home/Markets prices remain unavailable. Saved MSFT resume advanced completedSteps2→3 and paused without process replacement; lead traces its retained outcome. Startup history fails at display-instrument-definitions, whose `MarketDataInstrumentReadCapability::latest` still uses one-shot writer admission. Astra calendar_currentness_failure owns read-only diagnosis/proposal for that exact instrument reader and adjacent read consumers before edits. Astra first_brief_readiness receives additional exclusive `application/research/corporate_actions/preflight/history_plan.rs` ownership: rebuilt acquire ancestry plus complete/nominal-history nested frames total 2,378,208 bytes, so fix that measured producer boundary and inspect its complete child chain before another build. Existing two service preparation files remain theirs; no other changes/builds/runtime/Git.

Parallel first-analysis crash trace: after approved setup, ordinary MSFT selection and analysis start succeed (`first-stock-msft-start.json`, workflow `workflow_99e0def309c05b92b6e577a7f07a5cab`), but service30865 then aborts with a Tokio worker stack overflow during Alpaca doctor evidence deserialization. Crash report `/Users/sawmonabo/Library/Logs/DiagnosticReports/market-squawk-service-2026-09-30-235358.ips`. Desktop launches successor36428. Astra `first_brief_readiness` now owns `service/market_evidence.rs` and `service/market_evidence/preparation.rs`: matched-binary disassembly establishes 1.63 MB in four nested polling frames and another 1.20 MB acquisition frame. Separate large acquisition futures at producer boundaries without changing runtime stacks, inputs, cancellation or authority. Lead owns rebuilt-frame comparison and original-workflow resume proof. No credential reads, builds, runtime actions or broad audit. Independent of data snapshots and lifecycle-command fixes.

Parallel live-calendar correction: manual ordinary Retry applies/available, but finite history stops at `calendar-native-read-worker/catalog-busy`. Astra `calendar_currentness_failure` owns the bounded generation capture/retained-metadata read path in data `ingest.rs`, `catalog/{read_snapshot.rs,provider_capture.rs,provider_capture/metadata.rs,market_recovery.rs}` and `manifest/catalog.rs`, plus the existing provider publication/restart critical case if needed. Resolve actual read-only writer contention with the existing snapshot facility/shared SQL; inspect all callers of `market_recovery_authority` for the same live-reader failure rather than adding retries in the UI. No global wait extension/new runtime/relaxed evidence. Coordinate any additional file first. Independent of lifecycle command repair; lead integrates both before one compile/native rebuild.

Native follow-up at pushed `3ba9da1c`: all 17 native routes load without active-query errors; source restoration rejects `InvalidRequest` before history begins. `SourceLifecycleCommand::try_new` requires Retry to have no session/configuration/runtime digest and a reason, but the new startup and automatic-renewal callers violate it. Astra `credential_runtime_lifecycle` owns only `local_product/source_lifecycle.rs` and its `reconnect.rs` to correct both callers while preserving exact durable/runtime CAS and Stop/Lock. May extend the existing local command/lifecycle test only if needed for this uncovered critical caller boundary, after naming exact file. Lead retains public lifecycle contract, native commands, compilation/Git and ledger. No broad contract relaxation or new command path.

Current ownership: calendar snapshot and lifecycle-command corrections are pushed. Instrument snapshots and preparation stack corrections are frozen and released to lead verification. Lead owns integration, serialized checks, native actions and Git. No new branch/worktree, runtime, polling loop, expiry extension or compatibility path.

Existing journey: same-root `analysis setup status`, `market search --query <symbol>`, then `analysis start --selection-token <returned token> --confirm`. Recommendation setup must be ready with account/allocation; the default analytical policy trains from admitted data without a manually supplied model UUID. Saved `analysis results` / `analysis show --action-token <id>` / `analysis chart` provide restart comparison. Approved setup is saved as Ready revision 1 and survived restart. Resume the original paused MSFT workflow after the crash correction; training and saved-result completion remain unverified.

### Historical ownership and diagnostic sequence (released; not active assignments)

Next dependency after the current renewal/native checkpoint: reuse the existing account-health reconnect owner for expired Alpaca doctor receipts. The saved keys remain valid; current code drains non-Schwab accounts at the application's 15-minute proof expiry and never renews them automatically. No new poller or longer expiry. Exact durable revision/runtime checks and user Stop/Lock win. Separately, move finite starter history preparation from the source activation response into existing ProductStartupTasks ownership with exact-generation admission, cancellation/drain and restart re-admission together; no detached future or new job framework. These are bounded follow-up proposals, not implemented outcomes.

Current renewal critical compilation exposed a diagnostic type mismatch (identity selection returns RegistryError); lead corrected its closed classifier and reruns the focused check. No acceptance is inferred from the failed run.

Confirmed source renewal defect: current-generation replacement recognizes fresh unchanged-credential doctor evidence only for named Schwab profiles, rejecting the renewed Alpaca asset-reference slot after expiry. Astra `credential_runtime_lifecycle` owns `application/research/ingest/provider_runtime.rs` and its existing exact-generation critical case: replace named exceptions with lease-bound renewal evidence, preserving later interval, exact slot/credentials/capability and drained predecessor. Lead owns composition in `provider_activation/mod.rs` and callers `schwab_reference.rs`, `schwab_quote_metadata.rs`, `schwab_market_hours.rs`. No expiry increase or bypass.

Calendar lane next bounded diagnostic ownership: Astra `calendar_currentness_failure` owns app `research/corporate_actions/preflight/history.rs` and app `research/ingest.rs` only to expose existing closed ProviderOperationDiagnostic phase/failure and remaining canonical-publication stage. First three failures precede activation expiry and no history ingest run exists. No speculative behavior fix. Lead owns new critical writer-held/read assertion in existing data publication_recovery case; it uses the existing precommit seam, not a production testing API.

Independent bounded diagnosis: Astra `credential_runtime_lifecycle` traces automatic source restoration and settled Retry returning Unavailable after activation-expired, including whether initial display warming blocks ordinary readiness. Read-only source/log trace; no edits/runtime/builds. Lead retains lifecycle/composition ownership. Calendar lane owns canonical-publication failure diagnosis; snapshot lane owns data reads.

Current integration: `0206d13a` is pushed. The actual native-calendar inclusive-end/restart regression passes (1.37s), and the serialized native build passes. Live starter prices remain unverified; preparation still fails before history publication in the prior runtime.

Next ownership wave: independent PIT read snapshot → critical concurrent-read integrity check → native market retry. Astra `research_options_failure` owns only data `catalog/read_snapshot.rs`, `catalog/provider_event.rs`, `manifest/catalog.rs`, `provider_event_selection.rs`, and `ingest.rs`, plus the small shared identity replay helper in `catalog/market_data_instruments.rs`, for one read-only WAL snapshot inside the existing supervised worker. Lead reserves `catalog.rs`, shared exports/composition, existing critical tests, Git and build scheduling. Preserve exact original clocks, publication/identity checks, cancellation/deadline and bounded object reads; no global operation gate, extra worker/runtime, immutable live database, compatibility path or new dependency. Calendar diagnosis remains an independent lane in app files; lead owns runtime actions.

Confirmed presentation defect: an absent event kind yields a valid complete empty selection, but durable route assembly treats it as InvalidResult. Astra `research_options_failure` exclusively owns `application/paper/market.rs` and its existing inline critical case (if needed) to preserve receipt identity/completeness validation and omit only genuinely empty selections. Separately, investigate independent existing catalog read ownership for PIT reads; do not serialize all reads behind the global ingest gate without establishing necessity. No data-layer edits yet.

Parallel read-only contention trace: Astra `research_options_failure` inspects the proven live `analytical catalog authority is busy` current-market read (sequence 280) and resulting InvalidResult route errors. Scope is existing analytical catalog/read ownership and durable market route error mapping; no edits, builds, runtime actions or wider audit. Return the exact mutex/transaction lifetime and smallest correction if confirmed. Calendar preparation diagnostics stay with the other agent.

Next concrete history failure: live source retry returns applied/available, but preparation reports InvalidResult before any history publication or per-instrument failure. Astra `calendar_currentness_failure` owns a bounded trace and minimal categorical stage diagnostics in `application/market_calendar/read.rs` and `research/corporate_actions/preflight/display_history.rs` only if existing source/log evidence cannot locate it. Lead owns native actions; no speculative behavior changes or builds by agent. Native-session inclusive-end regression passes (1.37s).

Native-session read correction: Astra `calendar_currentness_failure` exclusively owns `crates/market-squawk-data/src/analytical_read/history_sessions.rs` to align final period-end inclusion with the existing inclusive request-end contract (period_end - 1ns). Publication and replay currently disagree for completed daily requests ending at midnight minus 1ns. Astra also owns a narrowly scoped extension inside the existing `publication_recovery.rs` complete-history case and its calendar capture builder: one real sealed calendar replay across publication/restart at the inclusive final daily boundary. Existing synthetic cases stay intact; no new harness or test matrix. Lead owns compilation and real read/restart verification. No other agent edits these files.

Native reconnect diagnosis is closed without speculative lock changes: replacement and same-runtime reconnect both pass in the real WebView. Same-runtime evidence preserves the token (`native-same-runtime-reconnect.json`); replacement latency remains unmeasured beyond the initial observation window. Provider admission is the only active implementation slice below; other returned slices remain lead integration work.

Authorized request-admission fix: Astra `credential_runtime_lifecycle` owns adapter Alpaca `live.rs`, `boot_snapshot.rs`, `historical.rs` and a cohesive shared `budget.rs` helper if needed; lead owns `lib.rs` declaration. Reuse existing admission semantics, wait only on ConcurrencyExhausted with original deadline/cancellation, retain exact budget identity and terminal errors. No budget increase/new background worker. Critical gap is live bootstrap/history overlap incorrectly stopping the source; extend only an existing adapter admission case if needed, after returning its exact ownership.

Measured market query correction: Astra `research_options_failure` exclusively owns `crates/market-squawk-data/src/catalog/market_recovery.rs`, only durable-route SQL. Existing route query costs 1.34–1.51s for one instrument in the preserved catalog; nine sequential reads consume most of the native deadline. Compact dataset/route keys plus an exact EXISTS eligibility join measured 46.95ms for all nine. Preserve all source-input/schema/clock predicates, ordering, limits and cancellation. No other query changes, schema/index/cache or new test harness. Lead runs the existing publication/restart critical case and native proof.

Authorized extraction-stack correction: Astra `credential_runtime_lifecycle` exclusively owns app `application/research/ingest.rs`, `ingest/alpaca_historical.rs`, and sources `extraction/capture.rs`. Heap-own managed extraction/handoff payloads and optional semantic capture payload so large results are not copied through every nested async frame. Bounded crash disassembly confirms cumulative frame pressure (ingest_inner approximately459KiB), not a large RawTask frame alone. Preserve capture serialization/digests and AbortOnDrop/cancellation; no increased stack limit, extra runtime or data cap.

Authorized Desktop reconnect correction: Sol `desktop_disconnect_recovery` owns renderer `lib/{transport.ts,tauri-transport.ts}`, `app/product-context.tsx`, existing `test/app.test.tsx`. Wire existing native reconnect through the existing reconnect schedule, preserving same-session cursor resume and replacing bootstrap for a new verified generation. Lead exclusively owns native `bridge.rs` same-generation handling. No new native capability or timer loop.

Authorized currentness correction: Astra `calendar_currentness_failure` exclusively owns `provider_onboarding/{service.rs,service/lifecycle_runtime.rs,mod.rs}`, `provider_activation/{account.rs,eia.rs,census.rs}`, plus guard comments in `market_runtime/alpaca_historical/calendar/publication.rs` and `market_calendar/alpaca/completed.rs`. Replace the single activation mutex with one reader/writer authority: immutable exact lease/currentness and publication guards share reads; mutations retain exclusive writes. Preserve pending-writer failure, durable lease/expiry checks and cancellation. Add only the critical shared-read/exclusive-revocation assertion within existing onboarding tests. Lead serializes verification and integration.

The extraction-stack, shared-currentness and Desktop reconnect slices are returned and released to the lead. Actual changes have been inspected; no agent ran builds or Git. The complete-history/restart case still passes after the heap-ownership change (1.24s). The exact shared-read/revocation regression passes (0.47s; `shared-lease-critical.log`); Desktop typecheck and the existing reconnect critical case pass (1.02s). Its two initial assertion failures were the wrong loading label and an unflushed React Query notification under fake timers; replacement receipt, cursor and failure assertions remain intact. The measured route-query correction and one integrated native build follow. Native proof must exercise real history ingestion, simultaneous ordinary screen reads and backend replacement. None of these three fixes is yet live verified.

The observed failure remains explicit: service PID3969 aborted during real history extraction, and Desktop then remained on Loading workspace. The earlier rebuilt sweep had Home/Markets timeouts with the other 15 routes loading. Currentness diagnostics identified activation-busy at history receipt authorization. These are the concrete acceptance barriers; passing narrow checks alone does not close them.

Market read correction authorized: latest manifest has 583 original objects; nine instruments × Quote/Trade repeatedly verify all 583 objects per point read. Existing catalog mappings prove each publication has one original object still present. Astra `credential_runtime_lifecycle` exclusively owns data `manifest/catalog.rs`, `parquet_store.rs`, `ingest.rs`: select publication-owned objects in the requested pinned generation and reuse exact bounded object verification; missing or inconsistent original metadata fails. Source tracing confirmed market-event compaction is currently unsupported: its publication identity resides in per-object metadata and the generic compactor only accepts research observations. No speculative fallback is retained or compaction-support claim made. No new schema/index/cache or app API. Lead owns existing critical selection fixture, compilation and native timing/restart proof.

Calendar correction authorized: Astra `calendar_currentness_failure` exclusively owns `market_calendar/alpaca/durable.rs` and `completed.rs`; acquire existing bounded account authority before queueing replay, validate under that guard, release before final async currentness check. Preserve all revocation/lease/cancellation checks and activation-before-worker lock order. Lead integrates and schedules the existing calendar critical check/native retry.

Both bounded Astra slices are returned, inspected and released to the lead. The existing provider-event restart case is extended only for the new critical integrity gap: unrelated-object corruption must not block an exact selection, selected-object corruption must fail, and inherited original files remain queryable. The attempted event-compaction continuation exposed the existing unsupported schema and was removed from this scoped check; it is not accepted compaction evidence. The retained complete-history/restart case passes (1.19s). The exact-object restart/corruption case passes (1.01s). The serialized native build passed; no compiler is currently active and no CI ran.

Current startup verification: the installed-service case passes (102.28s), including optional Lock with saved reads, unlock/disable, five concurrent startup reads, automatic child startup and crash/restart without a password. Desktop typecheck and the existing provider-boundary case pass. Evidence: `automatic-startup-critical.log`, `optional-access-desktop-{typecheck,critical}.log`. Native bootstrap deadline and CLI recovery response defects are corrected in `98104fff` and live recovery/restart is verified.

Policy → platform store → shared app contracts/composition → reversible credential-runtime drain
→ installed critical case → actual native fresh/restart journey → coherent commit/push.
Research and history fixes proceed in independent files, then join the same native-screen proof.
The first integrated check compiled but failed after 24.22s because the new test helper exceeded its client request lifetime; corrected without changing production limits. Both correction slices are returned and inspected: retained credential drain/resume steps, pending vault completion and source gate ordering; Advanced catalog pages on existing owned I/O plus closed stage diagnostics. Lead wired the existing configure deadline and corrected the test helper timeout. The serialized critical rerun passed. No competing compilers.

| Owner | Exact bounded slice | State |
| --- | --- | --- |
| Sol `optional_access_docs` | Only operations/provider-account-setup.md and architecture/security-and-trust-boundaries.md, deployment.md; align current optional-access behavior with integrated source | Returned and inspected; three current docs aligned. Lead corrected discovered CLI access-status reader; native CLI proof passed. |
| Lead | Shared schemas/contracts, service and LocalProduct composition, credential coordinator, native bridge/transport, critical installed case, Git/build/native actions | Startup commit/push; market-query integration and native verification owner. |
| Astra `optional_lock_backend` | `platform/src/secrets.rs`, `secrets/access.rs`, `secrets/preferred.rs`, existing platform secrets case | Returned; inspected, critical case passes. |
| Sol `optional_lock_ui` | Settings application-lock/settings-page; sources connection-setup/provider-credential-import/sources-page | Returned; inspected, typecheck and existing boundary case pass. |
| Astra `credential_runtime_lifecycle` | Source lifecycle and credential_access child; provider activation; research provider_runtime; paper controller and shutdown owner | Returned; inspected; integrated critical verification passes. |
| Astra `research_options_failure` | Research dataset_preparation cursor; subsequently cli_provider and schwab_oauth_runtime resumable access | Returned; inspected; read-only fixture-impact follow-up while compiler runs. |
| Astra `calendar_currentness_failure` | market_calendar/read and alpaca/completed/durable diagnostics; earlier account diagnostics | Replay guard fix returned and inspected; live retry awaits the serialized build. No freshness/revocation relaxation. |

Remaining barriers: resolve Home/Markets query deadlines and the proven calendar activation-lock
contention, then retry actual starter history and complete native screen/restart proof. Commit/push independently coherent slices
without claiming the entire V1 contract or final release verification complete. RAM measurement
remains deferred until all application workflows are ready.

## Earlier pushed startup checkpoints and retained evidence

Pushed checkpoint `d6162c6b` acquires the service instance before writing startup state/logs, retains its lock
through final drain, restores authenticated crash predecessors using retained subject evidence,
and labels expected locked startup as Unlock required. The existing installed-service critical
case passes (68.07s), platform lifetime case passes, authenticated recovery case passes, and
Desktop typecheck passes. Native unlock and duplicate-process exclusion pass. These checks ran
in the integrated working tree with the pending history slice; this ordinary checkpoint is not
an unchanged full-candidate release approval. Research initial-read and price evidence remain open.

Previous pushed checkpoint: `01ca6478` — exclusive structured-log writer ownership prevents
rejected duplicate service launches from corrupting retained log sequences. The focused log
retention/reopen case passes. PR #43:
https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5920788849.

Earlier pushed checkpoint: `38d1203e` — every locked startup route renders secure recovery,
retaining the original URL and resuming it after unlock. PR #43:
https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5920710814.

Previous pushed checkpoint: `73ea4778` — Desktop reconnects valid interrupted event streams at the
retained session/cursor, with backoff and native admission before reopening product pages.
PR #43: https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5920292726.
Existing secure-startup/reconnect critical case passed (1.96s); Desktop typecheck passed. Native
reload cleared a development HMR context exception. All 17 routes retained ready Product/System
states and a connected stream; Markets settled without query errors. Settings had one initial
`Operations.GetSettings` rejection, followed by a successful retry and 12 concurrent refreshes.
Its original typed cause and the discarded original event failure remain unproven. Evidence:
`.agents/tmp/v1-first-stock/event-recovery-{test,typecheck}.log` and `event-recovery-native-*.json`.

Secure-startup routing checkpoint: a genuinely locked launch correctly returned System
`recovery_required`, but Home displayed generic “Investment workspace unavailable” because only
Settings rendered the recovery form. `AppRoutes` now renders that existing form for every locked
entry while retaining the original URL; normal routes resume after unlock/event admission. Astra
`startup_bootstrap_trace` returned `routes.tsx` and the extended existing `src/test/app.test.tsx`.
The critical Home → secure recovery → Home/reconnect case passes (1.07s); typecheck passes.
Evidence: `history-native-locked-start.json`, `history-native-bootstrap-context.json`, and
`startup-recovery-gate-{test,typecheck}.log`. Fresh native form unlock after this correction is pending; no foreground automation is used.

The integrated backend build passed, and the local development catalog's exact unreleased v21
history trigger/checksum was refreshed offline after backup under `.market-squawk/recovery/`.
All original product data is preserved; no shipping migration was added. Real source Retry then
exposed a worker-stack overflow in managed history extraction. Astra `stock_capture_trace` owns
diagnosis/fix in its existing history-ingestion file; other edits require lead ownership. No
stack-size increase or capability restriction. The crash stack confirms nested polling exhausted the worker stack; the exact extraction future now
runs in an owned Tokio task with abort-on-drop, preserving runtime/cancellation/identity authority.
The log reopen failure was independently traced to two valid records with sequence 94: a rejected
duplicate launch installed logging before service-instance admission. Astra `startup_catalog_contention`
returned `application/logs.rs` and `logs/store.rs`: an exclusive lifetime writer lease precedes load,
retention and append; existing readers retain the same shared store. Original log bytes are backed up
under `.market-squawk/recovery/pre-log-sequence-20260930/`; all 99 event values remain unchanged,
with only the last five duplicate/overlapping sequence numbers and their hashes repaired offline.
The existing critical log pipeline case now checks second-owner rejection and reopened sequencing;
the focused case passes (0.06s), and the history-directory case still passes after the owned-task fix.
That service rebuild passed; actual extraction and fresh native startup remain unproven. Lead owns native reproduction and integration. These failures
block live market-data completion; do not substitute the successful fixtures for that proof.

Current live dependencies after the recovery rebuild: native locked Home renders the correct form,
but submitting it starts the service then reports a post-unlock handoff failure. Astra
`startup_bootstrap_trace` owns read-only diagnosis and, if needed, closed sanitized diagnostics in
Desktop `src-tauri/src/service.rs`/`bridge.rs`. Live Retry no longer reaches extraction because source
startup reports “source authority predecessor did not shut down cleanly” after the earlier abort.
Astra `startup_source_authority` owns a read-only trace of that exact crash-recovery boundary; no
state deletion/reset. Lead owns all live actions and compilation. Both failures remain open.

Startup correction ownership: lead owns `service/mod.rs`, the service binary, and the platform
instance guard. Dependency: acquire the existing installation capability before startup evidence
or logging, pass that same capability into composition, retain only its lifetime through final
status/log drain; workspace binding remains consuming and non-cloneable. Astra
`startup_bootstrap_trace` supplies read-only API/caller analysis. Lead also owns the two Alpaca live
key constants and startup reconciliation in `local_product/mod.rs`. The retained predecessor is
integrity-valid and terminalized by strict admission; recover it through the existing exclusive
replacement operation, never delete/reset it. Critical evidence: second-owner rejection throughout
shutdown, existing installed-service critical case, then real locked-native unlock and source Retry.

The duplicate-process live proof passes: refusal preserves startup-state bytes exactly
(`startup-owned-duplicate-proof.json`). First unlock exposed authenticated recovery using an
unconfigured subject resolver. Lead corrected the existing recovery method to take the durable
subject resolver; it reads retained integrity-checked account mappings without accessing secrets.
The temporary recovery registry issues no request authority. Both Alpaca keys and existing crypto/
Schwab callers now use that same resolver. Astra `startup_source_authority` extended the existing
critical recovery case with authenticated retained state and missing-resolver rejection; it passes
(0.01s, `startup-authenticated-recovery-test.log`) with quota/history/generation assertions intact.

The installed critical case failed in its child during journal replay: measured startup poll frames
plus replay left only 5,424 bytes of the existing stack for runtime frames. Astra
`stock_capture_trace` completed the read-only diagnosis; lead heap-owned the existing large startup
composition future, reducing duplication through callers without changing scheduling or limits.
The existing installed-service case is recompiling for recheck. Its failed predecessor run is
retained in `startup-instance-service-critical-test.log`; no acceptance is inferred from it.

GPT-6.1 Sol `starter_market_ui` returned only `components/app-sidebar.tsx` and `status-rail.tsx`:
locked recovery now says Unlock required instead of generic Unavailable. Loading, genuine failure
and readiness behavior remain intact. Desktop typecheck passes (`startup-unlock-label-typecheck.log`).
All helper ownership is released; lead owns current build/live/native verification and integration.

The rebuilt installed-service critical case passes (68.07s; `startup-instance-service-critical-recheck.log`).
Native unlock reaches Ready and Home now loads. The first route sweep overlapped startup, so its
first five routes are not acceptance evidence. Settled Research exposed one real `Macro.GetContext`
operation rejection. Astra `macro_restart_digest` owns read-only diagnosis of that exact query and
its persisted macro consumers; lead owns further native/CLI reproduction. Real Alpaca Retry is
running after both local stores unlocked; provider history remains unproven until publication/read.

Outcome being integrated: real completed-session closes in the shared nine-investment collection,
retained across restart, plus the evidenced concurrent-workspace-read defect. No fabricated prices,
backdated identity intervals, current-price trading authority or source freshness relaxation.
DAG: current native reference → ordinary historical acquisition → canonical capture/publication →
provider-neutral previous-close reader → shared Home/Markets cards → native restart evidence.

| Owner / status | Exact ownership and dependency | Evidence / next barrier |
| --- | --- | --- |
| Astra `startup_source_authority`, implementation returned | Alpaca config/historical/transport and existing tests: current native UUID capability, explicit NY symbol `asof`, completed daily request bounds | All six adapter tests pass (`history-native-adapter-final-test.log`), including mismatch/revocation, page completion, rate admission and DST boundary. |
| Astra `historical_identity_store`, implementation returned | Data catalog identity replay helper/export; manifest history/nominal initializer; existing v21 history trigger; existing publication/restart fixture | Focused `complete_alpaca_history_is_exact_clock_safe_and_restart_selectable` passes on ordinary stack, 1.29s (`history-native-publication-schema-test.log`). |
| Astra `stock_capture_trace`, implementation returned | Existing app history directory, runtime admission and calendar capture | Current identity capability retained and revalidated through publication. Existing directory case passes after updating its frozen parent digest for the required `asof` endpoint contract; integrated build passes. Real managed extraction exposed the separate stack failure above. |
| Astra `startup_catalog_contention`, implementation returned | `application/lifecycle.rs` only | Shared read locks replace exclusive read/read contention; switch/journal writes remain exclusive. Extended existing lifecycle case passes (`startup-lifecycle-critical-test.log`). Native events use a different authority. |
| Lead, integration active | Shared source capture contract/hash, preflight identity selection, source lifecycle hook/composition, previous-close consumer, card timestamp, schema digest, sanitized Operations diagnostics, builds/Git | Single compilation queue. Helpers released ownership. No new branches/worktrees. |

The exact catalog selection is retained through `CurrentCatalogProviderIdentity` and
`ProviderIdentitySelectionEvidence`. Timestamped history capture requires identity/asof; the
nominal-date path retains its existing separate semantics. Publication and reopening reproduce the
original catalog selection at its original knowledge/effective cutoffs. Current-research and
retrospective classifications remain unchanged; today's reference is never claimed as past knowledge.
The existing v21 schema definition and embedded checksum are updated in place, with no new migration.
The fixture's large inline async futures were boxed, and rejection windows separated to avoid
revision interference; no application stack/resource limits were raised.

The previous-close reader reuses the canonical history cursor and native-calendar rejoin, retaining
only the terminal completed bar and its authentic session close. Source activation awaits finite
missing-history preparation using the existing publisher and original cancellation/deadline;
optional display-history failure does not undo a healthy connection. Daily requests end at the
last complete NY provider day after the existing delay; the same plan endpoint governs the skip
check. Reads never start provider acquisition.

Remaining barrier: integrated application critical checks/build, safe local development-catalog
schema refresh with original data preserved, real source retry, native startup/price/restart checks,
then a coherent pushed checkpoint. Whole-app RAM and full V1/installed acceptance remain pending.

## Active market-startup delivery wave — 2026-09-30

Refresh base `95871cc081dce724b707c6a3fb49f002b8976b4f`, clean and pushed. The previous wave
made native reads work and proved background WebView interaction. This wave delivers the owner's
first-launch investment collection and resolves the evidenced live-capture interruption.

| Owner | Exclusive files | Finishable outcome / dependency / evidence |
| --- | --- | --- |
| Astra `stock_capture_trace` | `apps/market-squawk/src/live_source/sink.rs` only | Retain the typed capture rejection and existing capture-health reason in the current source-qualified failure diagnostics. Inspect redaction before logging. No changed limits, fallback, provider state, builds or Git. Lead then rebuilds/retries the same source and fixes the evidenced root cause. |
| GPT-6.1 Sol `starter_market_trace` | New `apps/market-squawk/src/application/market_collection.rs` only | Durable nine-symbol keep/remove preference using `LocalAuthorityStateStore`, initializing once and preserving an entirely removed collection through restart. Closed revision-checked mutation and retained backup bytes/revalidation/fresh-target restore. No financial identity/price authority, migration, application wiring, builds or Git. |
| GPT-6.1 Sol `starter_market_ui` | `features/markets/markets-page.tsx`, new `features/markets/market-collection.tsx`, `features/overview/use-overview.ts` and `overview-dashboard.tsx` under Desktop `src/` | Shared provider-neutral collection read/controls in Home and Markets, all kept starters visible, removed starters restorable, pending genuine data displayed honestly. Preserve search/detail/history and existing design. Uses the frozen wire contract below; no transport/schema/Git/build ownership. |
| Lead | Shared exports/contracts/schemas, provider default selection, application/service composition, backup adapter, native/CLI/MCP transport, UI contract and integration checks | Wire the one collection authority into both ordinary screens and all shared consumers. Freeze the UI contract before assigning disjoint presentation work. Extend existing critical persistence/restart coverage; prove native remove/keep and genuine quote behavior after implementation. |

Lead additionally owns `application/analytical_workflow/host.rs` and `service/mod.rs` for the
substantiated startup defect: the private workflow was bound to filtered external MCP descriptors,
so required portfolio/job operations were always considered absent. Bind full native descriptors
with the existing weak dispatcher; preserve external MCP filtering and per-call authority checks.
The same installed-service critical check must cover internal workflow availability and restricted
external discovery. No additional review round or separate dispatcher is introduced.

Owner follow-up: resolve the remaining startup page errors, not just read-command admission.
GPT-6.1 Sol `starter_market_ui` has released its four implementation files and now owns a read-only
startup-state trace: inspect the retained 17-route native evidence and affected page/bootstrap/query
paths, classify concrete failures and identify minimal fixes. No edits, builds, Git, new worktrees
or foreground interaction. Lead retains integration and the single compilation queue. This trace
is independent of the frozen collection build and does not create a new review ceremony.

Startup trace produced two additional concrete UI state defects. GPT-6.1 Sol `starter_market_ui`
now exclusively owns Desktop `features/lifecycle/lifecycle-page.tsx`, `features/paper/manual-paper-draft.tsx`
and `features/paper/paper-execution-page.tsx`: a development run with no installed release must not
claim repair is required, and an inactive paper session must route to the existing start controls
instead of a generic Connections/Updates failure. Preserve genuine read errors and execution
checks; no fictitious readiness, release installation or paper start. No other edits/tests/builds/Git.
Lead retains the pending Home comparison failure trace; its error must be captured before a fix.

All three implementation helpers have released their files. Astra `startup_source_authority`
owns a read-only diagnosis of the current source Verify/restore failure after both secure stores
unlock: trace CLI admission, installed authority, source state and sanitized service diagnostics.
No edits, provider mutations, builds or Git. Return the precise failing boundary and smallest
correction; lead owns live retries and implementation. This runs independently of the lead's
native collection and screen checks and the single existing critical test.

Astra `startup_catalog_contention` owns a read-only trace of Home's reproducible first-load
`Analysis.ReadWorkflow/profileOptions` unavailable error. Explicit retry returns genuine SPY/VTI/QQQ
choices. Inspect shared catalog locking and concurrent Home reads; identify a minimal correction
with cancellation/deadline and publication authority intact. No edits/builds/Git until ownership
is narrowed. Lead retains native reproduction and shared integration.

Source diagnosis is complete: the retained Alpaca Verify intent is in reconciliation, and Retry
requires an unexpired lease before entering its renewal path. Astra `startup_source_authority`
now exclusively owns `apps/market-squawk/src/local_product/source_lifecycle.rs`: correct explicit
Retry admission and preserve expired-active-lease errors using existing retained bindings and
doctor verification. Persist intent before renewal; require fresh admitted authority before
runtime start. Preserve CAS, cancellation, draining and all other providers. No edits to tests,
shared contracts, manifests or Git; lead owns the existing critical/live recovery verification.

Catalog diagnosis is complete; Astra released ownership without edits. The comparison handler
bypassed the existing single research I/O lane used by concurrent collection reads, hitting the
catalog's fail-fast mutex. Lead owns `service/analytical_profile.rs` and its
`service/tool_services.rs` call: execute comparisons on that same supervised worker with the
original deadline and cancellation. No catalog lock changes, request retries or new worker are
needed. Extend the existing installed-service case with concurrent comparison/collection reads.

Verified checkpoint evidence (2026-09-30):

- All 17 native Desktop routes loaded with zero query errors in
  `.agents/tmp/v1-first-stock/startup-recovery-route-check.json`. Fresh Home comparisons load
  without Retry. The workflow now sees its private native capabilities while external MCP
  discovery remains restricted.
- Native Remove on Home / Keep on Markets persisted, and the nine saved choices survived service
  restart. The existing installed-service critical test passed in 50.36 seconds, covering concurrent
  comparison/collection reads, workflow availability, MCP filtering, stale revisions and the
  entirely removed collection surviving restart. Its initial request-contract omission was fixed.
- The final single-job build passed in 6m29s (`startup-source-renewal-build.log`); Desktop typecheck
  passed (`market-startup-typecheck.log`). The final renewal-state change was live checked:
  source Retry renewed the same session/configuration/generation with fresh doctor evidence and
  transitioned Alpaca from blocked revision 4 to active revision 5. The retained receipt is
  `source-renewal-retry.json`. No scope or credential replacement was introduced.
- Fresh native Home after that rebuild/unlock still shows all nine choices and zero query errors
  (`source-renewal-home-settled.json`). Native and service shutdown of the preceding run both
  exited normally. Only the two new analytical-profile/test blocks received formatting afterward;
  no post-build behavioral change was made. This is ordinary checkpoint evidence, not the final
  unchanged-candidate release gate.

Remaining data dependency is concrete: the eight retained canonical IDs and IEX mappings match,
but their latest quotes are stale; TSLA has no canonical reference in this root yet. The source
lifecycle now reports active, while generic `Source.Health` reports no generic runtime records;
this is not proof of fresh capture. The doctor verifies historical access but does not publish
OHLC bars. There are no complete-history publications, and collection pricing never reads daily
history for its existing `previous_close` display state. Next checkpoint must reuse the canonical
Alpaca history plan/publication and provider-neutral history reader for genuine completed-session
closes, plus provision TSLA through the reference owner. No freshness relaxation, fabricated price
or second ingestion stack. All current helper ownership is released; lead owns Git/checkpointing.
No full investment-analysis, installed-package or whole-app RAM acceptance is claimed.

Lead also owns `src/app/query-client.ts` and the affected Models/Forecast and Backup query callers:
uppercase `Model`/`Operations` cache domains do not match lowercase native invalidation events,
leaving startup-empty screens stale. Reuse the existing `DesktopInvalidationDomain` type at the
shared query-key boundary and correct callers in place; no normalization shim or new event system.
Typechecking additionally exposed Backtests, Settings/workspace and a dead Home invalidation key;
lead owns those affected callers and the Overview helper type. Agent owns its lifecycle/paper
caller corrections within the existing three-file assignment.

Frozen collection authority contract: `STARTER_MARKET_SYMBOLS` contains SPY, QQQ, DIA, IWM, VTI,
AAPL, MSFT, NVDA, TSLA. `MarketCollectionAuthority::try_open(control_root)` opens the single
`market-collection-authority` directory. `snapshot()` returns `{revision, choices:[{symbol,kept}]}`;
`set_choice(expected_revision, symbol, kept)` atomically persists a validated choice and returns
that snapshot. Unknown symbols and stale revisions are rejected; repeated identical choices do
not reset or recreate defaults. Backup methods retain canonical bytes/digest, revalidate the same
state, verify an absent target and restore that state. This is a preference, not a canonical market
identity or trade authority. Provider preparation uses the shared starter definition; changing
visibility does not revoke independently needed data or benchmarks. No tests/builds outside the
lead queue. DAG: authority → lead shared operation/projection → disjoint UI → native restart proof;
capture diagnostics → same-source retry → evidenced producer correction runs concurrently.

Frozen UI wire contract: `transport.query({query:"marketCollection"})` returns an ordinary
ApplicationResult whose data is `{revision:string, entries:[{symbol:string, kept:boolean,
market:MarketProductRow|null}]}` for the nine defaults in their defined order. Revision is lossless
unsigned decimal text. `transport.query({query:"marketSetCollectionChoice", expectedRevision:string,
symbol:string, kept:boolean, confirmed:true})` returns `{revision:string, choices:[{symbol,kept}]}`.
Explicit Keep/Remove is the confirmation; invalidate/refetch both collection and market overview
queries after success. Missing canonical metadata is null, never a fabricated selection token or
price. Show the symbol while details load/become available. Search and detail/history remain
independent; hiding a starter does not delete its catalog/data or revoke a benchmark.

## Active live-stock and harmonic-evidence wave — 2026-09-30

Refresh base `1572f4fb19d18add8bcd73eee7d2f69ff2834fcf`; execution remains authorized.
One primary worktree and only the three approved local/origin branches were verified live.
Old roots and matching binaries are preserved; the old service/Desktop stopped normally.
Preserved diagnostic root: `.market-squawk/v1-owner-test-stock/{data,installation}`. Its protected
startup, unlock and credential import previously succeeded; the post-reboot identity failure is
recorded below. Corrected V1 native verification uses a fresh
`.market-squawk/v1-owner-test-stable-endpoints/{data,installation}` root. No migration or stored
binding rewrite is authorized or implemented.

| Owner | Exclusive scope | Outcome, verification and next dependency |
| --- | --- | --- |
| Lead — background native automation and read-command admission verified | Desktop Cargo feature/dependency, native startup, command manifest/capability, generated permissions and troubleshooting instructions | Base `52c415ad`. Embedded WebdriverIO operated the actual hidden WKWebView. Native transport reproduced `register_read not allowed. Command not found`; six registered commands were missing from the manifest and five lacked main-window grants. All explicit registrations/grants are corrected, retaining cancellation/session checks. Rebuild passed; native Market.GetOverview now returns eight canonical investments, all 17 routes rendered, and clicking Apple opened genuine detail/comparison choices. Corrected launch/navigation/screenshot/selection remained behind Terminal throughout 1,200 foreground samples. Live prices and full analytical workflows remain incomplete. No global keystrokes, mock backend or release endpoint. |
| Astra `stock_capture_trace` — read-only follow-up complete | Fresh-root source health and quote publication/selection path; no edits | At pushed `52c415ad`, Verify/Start succeeded and 383 canonical rows were published before raw capture failed at 19:14:33.762881Z; downstream `CaptureMaterial` followed 71.562 ms later. Runtime is now inactive while onboarding remains `active_scoped`; all eight overview prices are unavailable. `ProductionSinkFailure::Capture` discards its typed reason in Display and publisher health is not drained. Lead must retain those existing diagnostic reasons, then retry the same source to identify the actual correction. No limit increase, quote fallback or selection change is justified by current evidence. |
| GPT-6.1 Sol `starter_market_trace` — read-only | Current startup/source activation, catalog population, Home/Markets and existing preference persistence | Owner requires a default keep/remove collection with genuine quotes/details on first launch, including TSLA. Locate the smallest complete existing producer-to-screen path and exact gaps; no edits, Git or builds. Lead freezes the shared contract after this handoff; implementation follows integration of the pending Desktop/restart correction. |
| Lead | Runtime/setup, shared contracts/composition, ledger, Git and builds | Pushed healthy-renewal fix `be143bf1` and persistent endpoint fix `028248c8`, with focused critical checks passed. Desktop event metadata and complete paged-history verification are implemented; TypeScript passed. The service check passed setup events and paged history, then exposed timing-dependent macro evidence after restart. Its shared semantic-identity correction passed the query identity case; the full installed-service case passed, including exact restart equality and later publication. Fresh normal service startup and native Home rendering passed; the full native route sweep/restart remains pending background automation. Cleanup is complete. |
| GPT-6.1 Sol `desktop_bootstrap_trace` — released; lead implements | Lead: installed source/governance/MCP descriptors, MCP cache key and existing installed-service critical case | Native inspector proves bootstrap succeeds but staged CLI unlock emits an operation missing from the Desktop event index; native Apply also emits wrong-case source domain. Correct producer metadata and preserve private CLI/ticket authority, then prove actual Desktop admission. Same confirmed gap affects governance and MCP control: use canonical domains and the existing operations cache. |
| Astra `macro_restart_digest` — released; lead verifies | `crates/market-squawk-data/src/query.rs`, `query/receipt.rs`, `query/tests.rs`, `analytical_read.rs`, and application `research/macro_context.rs` plus `macro_context/{census,energy,provider_periods}.rs` receipt selection | Variable remaining deadlines entered durable financial evidence. Implemented shared semantic manifest/schema/SQL identity alongside unchanged execution/admission identity, covering Board and Treasury together. Existing identity case passed. Lead renamed the receipt field explicitly and corrected three additional Census/energy/provider-period producers plus the shared provider-period selection digest after compilation exposed missed consumers. Full installed-service equality, later publication and original-cutoff preservation passed. All limits and artifact reservation identity remain unchanged; no hash-version stack or migration. Lead owns builds/Git. |
| Astra `restart_catalog_identity` — released; lead verifies | Platform persistent endpoint helper/export; data catalog and Parquet authority callers only | Confirmed transient device-number change across reboot. Shared macOS volume-UUID/file-ID binding is pushed with replacement checks retained. Platform identity and three storage cases passed. Lead added a scoped Foundation autorelease pool for Rust worker threads; its identity critical case passed again. Final service/native verification is pending; old roots remain untouched. |
| GPT-6.1 Sol `board_history_contract` — released; lead inspected | Existing `production_mcp_composition.rs` history helpers/callers only; preserve lead event additions | Handoff inspected: existing service case consumes all 1,100 complete observations through cursors, checking bounds, duplicates and exact evidence after restart. The production paged contract is unchanged. Passed in the single-job service check; no new harness or separate build. |
| Astra `stock_capture_trace` — released; lead verified | Registry health snapshot and existing unit/integration health cases; shared explicit native-identity fixture | Healthy renewal atomically retains the preceding epoch's original interval; unhealthy/terminal transitions revoke it. Queued-time, second-renewal, expiry, unhealthy recovery and three exhaustion/recovery cases passed. No deadline extension or weakened native-identity admission. Live continuity awaits rebuilt service. |
| Astra `native_setup_current` — released | Native fund catalog admission plus existing catalog case | Pushed `4139b2a1`: remove the artificial new-ETF rejection; derive Equity/Fund from the official listing; preserve existing issuer and native identity evidence. Critical catalog creation/conflict/revocation/reopen/replay case passed (1 test). Native Retry now passed this admission edge on the rebuilt service; live continuity still fails downstream. |
| GPT-6.1 Sol `brief_pattern_projection` — released; lead integrated schema | Saved Brief status projection and one existing-module critical case | Pushed `60bdf071`: actual retained disposition now supplies required `pricePattern.state/outcome/summary`; reasons reuse its explanation. Whole projected Brief passes its published descriptor for expired and unevaluated evidence; missing/contradictory outcome fails. One critical case passed. Shared Rust schema matches existing strict Desktop fields. No new compatibility path. |
| GPT-6.1 Sol `live_input_recipe` — released | Read-only SEC producer/consumer handoff | Exact company/security and fiscal wiring gaps below; no implementation or live completion claimed. |
| Astra `harmonic_status_v1` — released | Read-only full harmonic status/geometry contract | Forming and terminal geometry preservation remain required, beyond this Brief decoding fix. Lead must freeze shared declarations before disjoint producer/renderer edits. |

Earlier resource interruption (historical): the lead stopped the sole Cargo build (exit 130)
after machine pressure; that check did not pass. Concurrent renderer Vitest workers belong to the
separate `ai-sidekicks` Claude session and remain untouched, as directed. Memory pressure later
returned to normal; current compilation remains single-job and low priority.

Background native evidence under `.agents/tmp/v1-first-stock/`:
`native-webdriver-permissions-build.log` (exit 0), `native-webdriver-market-query.json`
(original command rejection), `native-webdriver-market-query-corrected.json` (successful real
transport read), `native-webdriver-routes-corrected.json` (17 route/content observations),
`native-webdriver-corrected-{home,markets}.png`, `native-webdriver-market-selection.json`, and
`native-webdriver-corrected-focus.log` (Terminal remained foreground for the complete 1,200-sample
run). The first hidden Desktop also exited normally through WebDriver window closure. No release
build, broad CI or whole-app RAM measurement was run. Source capture failure still explains missing
prices; missing model/data inputs and trusted-update setup are not reclassified as completed workflows.

Authorized stale-artifact cleanup completed: removed 123 obsolete September live-test
executable copies, 23 disposable hashed build/test executables and 3,026 compiler intermediates.
`target/` decreased from 32 to 17 GiB; `.agents/tmp/` from 56 to 5.6 GiB; measured free disk
increased from 63 to 121 GiB. Original session/recovery backup, patches, logs, data roots,
current top-level executables and explicitly retained prior-schema binary pairs remain.
Exact removal inventory: `.agents/tmp/v1-first-stock/stale-artifact-cleanup-20260930.json`.
Current build output is needed for the native verification below; these cleanup measurements
are not whole-application performance acceptance.

The rebuilt service accepted secure unlock but failed composition with
`analytical artifact root belongs to a different catalog`. Read-only comparison reproduced both
stored catalog and artifact digests from their unchanged paths/inodes using device 16777229;
the current device is 16777233. Persisting that transient device number explains the post-reboot
rejection. Existing marker/binding checksums validate. The retained root is not reset or rebound;
a stable endpoint-identity correction is required before native page/restart acceptance.

Current recovery: memory pressure returned to normal; the separate session remains untouched.
Four focused source cases passed: `health-renewal-critical.log` (1),
`health-epoch-critical.log` (2) and `source-epoch-critical.log` (1). They exercise queued work
across healthy renewal while retaining expiry, unhealthy revocation and terminal exhaustion.
The shared fixture now supplies explicit native identity evidence to both existing test owners.
The subsequent Desktop authority check passed its new setup/event assertions but failed the stale Board history artifact expectation. The existing case now consumes all 1,100 complete rows through opaque cursors and compares the same evidence after restart; rerun includes the frozen startup correction and scoped Foundation autorelease pool. The new persistent endpoint critical case passed (1 test, real Foundation lookup plus mount-number/replacement checks). Three existing storage cases also passed: second-catalog exclusion, catalog replacement between opens, and replacement directory rejection. Evidence: `persistent-endpoint-critical.log`, `catalog-root-isolation-critical.log`, `catalog-replacement-critical.log`, `artifact-replacement-critical.log`. This is critical implementation evidence; fresh native startup and screen/restart verification remain pending.

Fresh normal service composition on `v1-owner-test-stable-endpoints` succeeded after protected
bootstrap. Native Home visibly rendered “Workspace ready”; protected CLI unlock also succeeded
while Desktop was open. Evidence: `stable-endpoints-live-{service,desktop}.log`,
`stable-endpoints-live-status.json`, `stable-endpoints-native-ready.png`. The full 17-route capture
is not yet established: Mac foreground focus changed during automation, so no input was sent when
the focus guard failed. Owner requested focus-free automation; foreground automation is stopped. Home is not counted as all-screen acceptance. Owner approved the recommended development-only embedded WebDriver integration; lead owns manifests, native registration and its background proof.

The next installed-service attempt passed source/MCP event assertions, complete cursor history and
catalog reopening, then failed exact H.15 consumed/investment digest equality. Only those two fields
differed: variable remaining execution time entered the query admission hash. The shared query
receipt now also carries semantic manifest/schema/SQL identity for successful macro evidence;
artifact admission encoding, limits and cancellation remain unchanged. The existing identity case
passed (1 test); the full service/restart case also passed (1 test, 33 filtered, 72.88s). Reactivation reuses retained provider verification; its stale second-doctor expectation was corrected without weakening later-publication or exact cutoff/evidence checks. Preserved failing evidence:
`desktop-event-authority-before-semantic-identity.log`; current checks:
`semantic-query-identity-critical.log`, `desktop-event-authority-critical.log`.
The pending Desktop/restart integration is critically verified; this is not full native-screen or complete stock-workflow acceptance.

Critical evidence under `.agents/tmp/v1-first-stock/`: `fund-admission-critical.log`
(1 passed, 7 filtered), `brief-pattern-contract-critical.log` (1 passed, 132 filtered),
and `stock-live-desktop-build.log` (successful single-job debug build). The Brief case closes an
uncovered essential workflow failure: its real backend payload omitted a required Desktop field.
The first compile caught a test-only array dereference; corrected before the passing run.
This is critically verified implementation, not complete live/installed Investment Brief acceptance.

Live stock verification succeeded, then startup failed at QQQ because new Fund identities were
rejected despite authentic current listing/native evidence. `4139b2a1` fixes that generic rule
without a fund whitelist, invented CUSIP, fabricated issuer relationship or weakened custody check.
Retained evidence and exact failure analysis are in `qqq-admission-failure.md`; prior SPY/VTI
issuer identities remain unchanged. No schema change or migration is needed for this correction.
The initial standalone Nasdaq `kind:source` request was invalid; existing demand-loaded directory
publication owns that route. H.15's expired anonymous session was renewed through native Start,
then activation succeeded on session `816af143-1016-4450-98f8-e248b89aae06`. This proves setup
recovery only; publication/typed read on this root still need verification. Owner selected $100,000 USD and 0.25% simulated costs. Fresh native choices, Prepare and
Create succeeded; the virtual account is ready with its session stopped and no orders. Evidence:
`stock-account-{choices-current,prepared,created,readback}.json`. No arbitrary account values
were supplied.

Live readback at `60bdf071`: native Retry admitted the configured stock/fund group; AAPL search
returns a stock and QQQ search returns a fund through the ordinary market API. AAPL selection
returns its genuine identity/history token with unavailable price after the source failure, not
invented market values. Rebuilt Desktop was reopened on the same service/root. This is catalog
and account readback evidence, not sustained live-market or complete Investment Brief acceptance.
`stock-admission-live-retry.json`, `stock-admission-{aapl,qqq}-search.json`,
`stock-admission-aapl-selection.json` and `stock-admission-live-service.log` retain the actual results.
The earlier Verify error was a reconciliation precondition, resolved by native Retry without edits.
Astra's current diagnosis owns the subsequent live capture/health error; do not reclassify it as
an admission failure or hide it behind the successful startup response.

Next dependencies (handoff details retained under the same scratch directory):

- SEC currently normalizes under a CIK-derived instrument rather than the catalog equity. Existing
  company/security review has no installed caller. Complete company capture, evidence-bound
  relationship and canonical fiscal selection together; a link-only or arbitrary ID replacement is
  insufficient. Inspect whether company-scoped provenance should stay unchanged through the
  authorized security join before choosing a normalization change. `current-sec-recipe.md` locates
  the exact current interfaces and critical coverage.
- Fiscal plan/build/forecast operations are declared and driven but lack installed service handlers;
  reuse existing preparation, job and model owners rather than create another pipeline.
- Retained managed-runtime receipts disagree and required release binaries are absent. Use the
  supported cache-reusing refresh and compile-time foundation path in `current-model-runtime-recipe.md`
  after source changes are frozen; do not forge receipts or launch competing builders.
- `harmonic-status-contract.md` defines required causal forming evidence and retained original
  invalidated/expired geometry, with active decision receipts confined to confirmed patterns.

DAG: protected setup → actual stock/native identities → SEC/history/rates → managed forecast
readiness → saved Investment Brief → same-root restart. In parallel: fixed harmonic contract →
disjoint producer/renderer changes → critical saved-evidence proof. Existing valuation and portfolio
work is reused. No new branches/worktrees, broad CI, review round, release build or whole-app RAM
measurement. Remaining allocation choices remain owner-selected; all full V1 acceptance remains open
until actual end-to-end evidence exists.

## Completed stock admission and saved-planning implementation wave — 2026-09-30

Refresh base `505685e667a4650506556ee5725cc8ec3e8bcabb`, clean and pushed; contention checkpoint
completed with four critical cases and live startup/read evidence (PR #43 comment 5909800002).
One primary worktree and exactly three local/origin branches remain, freshly verified with live
remote heads. No extra branch was created. The contracts below define this wave; this table alone
owns current assignments. Lead schedules one single-job critical check at a time.

| Owner | Exclusive files | Outcome / dependency / critical evidence |
| --- | --- | --- |
| GPT-6.1 Sol `live_input_recipe` | `adapters/market-squawk-adapter-alpaca/src/asset_reference.rs`; `crates/market-squawk-data/src/catalog/market_data_instruments/alpaca_asset_reference.rs`; existing `crates/market-squawk-data/tests/catalog.rs` | Generic selected-equity creation from current listing/native UUID and documented quote units, retaining exact replay/conflict/currentness. Hand off the admission DTO before dependent composition changes. No build/Git. |
| GPT-6.1 Sol `saved_planning_catalog` | New cohesive `crates/market-squawk-data/src/catalog/portfolio_planning.rs` only | Immutable completion records, idempotent saved markers, account-bound paged reads and backup inventory fences. Follow the prepared contract below; send exact schema/export/factory needs to lead. No service dependency, whole-inventory cache, migration, build/Git or extra harness. |
| Astra `native_setup_current` | `provider_activation/alpaca.rs`, `provider_activation/market_config.rs`, `local_product/market_provider_configuration.rs`, `application/market_runtime/group.rs`, `application/market_runtime/alpaca_asset_reference.rs`, resolver cancellation handoff in `application/market_runtime.rs` under `apps/market-squawk/src/` | Integrate native acquisition before canonical binding with one retained account owner; disjoint provider composition ownership delegated by lead after fixed admission DTO. Resolve reference metadata without fictional canonical IDs. No Git/build. |
| GPT-6.1 Sol `position_desktop_trace` | `apps/market-squawk/src/portfolio_application.rs`, new `portfolio_application/saved_planning.rs`, existing `portfolio_application/advanced/scenario.rs`, `advanced/planning.rs`, `candidate.rs` | Persist completed calculations and expose shared Save/List/Get against catalog and controlled artifacts; retain internal candidate evidence. Stable catalog DTO dependency. No financial math changes, Git or builds; lead owns operation schemas/composition/backup/UI. |
| Astra `position_authority_trace` | `apps/market-squawk/src/portfolio_application/backup.rs`; `local_product/operations/portfolio_backup.rs`, `workspace_restore.rs`; `local_product/operations/portfolio_backup/planning.rs`; joined I/O method only in `research_service.rs` | Include completion/save fences and every retained planning artifact in streamed backup/restore, with restored-catalog agreement; coordinate narrow portfolio runtime access with app owner. No Git/build or unrelated backup redesign. |
| GPT-6.1 Sol `rebalance_desktop_v1` | Portfolio feature files under `apps/market-squawk-desktop/src/features/portfolio/` only | Shared Save control for completed scenario/rebalance/position results; demand-loaded saved list/detail with original backend output. Stable Save/List/Get contract; lead owns native bridge/transport declarations. No financial calculations, Git or builds. |
| Lead | Shared catalog schema/export/factory, issuer helper visibility, saved-planning application contracts/composition; ledger/Git/builds | Wire saved completion storage through application/client/backup consumers before claiming that journey complete. Serialize shared schema and transport changes. |

DAG: native evidence + catalog creation → lead account preparation/binding/runtime integration →
existing critical stock/replay check → real stock discovery/restart. Independently, planning catalog
→ application Save/List/Get and completion hooks → backup/client integration → existing critical
saved-result/restart case. Assign dependent writers only when the shared interface is fixed. Each
complete producer-to-consumer checkpoint is verified and pushed separately; no scaffold is completion.

Implementation is frozen and all worker ownership is released. Generic-equity creation, native
UUID replay, conflicting-identity rejection and revoked publication passed the focused catalog
case. Initial failures were incomplete fixture broker-budget and process TLS setup; production
safeguards remain unchanged. The extended Desktop selected-portfolio journey passed explicit Save,
reopening without recalculation, mutation independence and closed-detail read cancellation. Desktop
typecheck passed again after that fixture extension. Lead inspection
corrected saved-list pagination admission and separated public result limits from private artifact
reads; backup and application reuse one artifact-reference converter.

The new inline backup roundtrip passed with real catalog, controlled artifact repository and owned
worker: saved/unsaved bytes survive reopen, missing payloads fail backup, damaged/truncated streams
fail restore. Its metadata fixture represents the analytical snapshot precondition; it does not
claim an installed full-backup journey. The existing LocalProduct restart/import case passed
(1 test, 33 filtered), including idempotent Save, account isolation, one-row cursor continuation and
exact original-result reopening after later import. Its first run exposed fixture initialization/stack setup and a real
saved-result metadata mismatch: Save/List now use non-source storage metadata, while Get requires
the retained original source evidence. The fixture initializes storage before writing imports and
uses the installed service entry point's existing 8 MiB thread stack. The native Desktop library
check passed with locked/offline dependencies and one compiler job. Native cancellation custody, exact account rebinding and
candidate-impact evidence each passed their focused existing lib case. The formatted equity
admission catalog case passed again. No new harness, CI, release build or whole-app RAM measurement
is scheduled.

Schema changes update V1 in place. Existing live roots are preserved; matching pre-change debug
programs are retained under `.agents/tmp/v1-first-stock/pre-planning-schema-505685e6-binaries/`.
A later new-schema live journey uses a fresh root, not a migration or rewrite of old evidence.
Stock admission is critically verified and pushed as `9e4bef63`. Saved planning is critically verified
through real LocalProduct restart/import, saved/unsaved artifact backup, original candidate evidence,
Desktop Save/reopen/read cancellation, final TypeScript typecheck and native Desktop bridge check;
its separate integration checkpoint contains this ledger update. All worker ownership is released.
Evidence logs are under `.agents/tmp/v1-first-stock/`: `equity-admission-critical.log`,
`native-account-{custody,rebind}-critical.log`, `saved-planning-{restart,backup,candidate,desktop}-critical.log`,
`saved-planning-typecheck.log` and `saved-planning-native-bridge-check.log`.

These outcomes are implemented and critically verified, not live or installed-workflow complete on
the updated schema. The next dependency is fresh-root live stock discovery through the complete
saved Investment Brief workflow. Preserve the old live root and matching binaries; do not migrate
or rewrite earlier evidence. No additional review quarter, broad gate or resource measurement was run.

## Completed native-identity contention wave — 2026-09-30

Audit base `5d9a137e`, clean primary worktree; prior turn made verified progress. Discovery and
stale-price selection are pushed and live verified (PR #43 comments 5909221084 / 5909403787).
Acceptance 1/7 defect: normal catalog contention can end a live source immediately, despite the
existing identity-selection deadline. Fix that cause without retrying invalid authority or blocking
a Tokio worker. Provider and saved-product preparation continue independently below.

| Owner | Exclusive scope | Dependency / finish evidence |
| --- | --- | --- |
| Lead | Registry error declaration `crates/market-squawk-sources/src/registry/current_batch/validation.rs`; shared consumers/ledger/Git/builds | Add explicit transient `ProviderIdentityAuthorityBusy`, retaining terminal unavailable/poison/auth meanings; serialize integration/checks. |
| Astra `native_setup_current` | `crates/market-squawk-data/src/catalog/market_data_instruments.rs`, existing `tests/catalog.rs`; `apps/market-squawk/src/live_source/supervisor.rs` and existing supervisor test module | Map only genuine mutex/SQLite busy to transient error; one cancellation/deadline-bound async selection path before session admission, including startup; existing critical contention/cancellation tests. No builds/Git. Request any additional caller ownership first. |
| GPT-6.1 Sol `live_input_recipe` | Read-only native asset/listing publication and installed selected-equity composition | Refresh prior generic-stock gap into exact proposed producer/consumer patch and evidence authority; no hardcoded subject, no repeated provider audit, no edits/builds/Git/credentials. |
| Astra `position_authority_trace` | Read-only saved portfolio calculation/artifact/backup owners | Resolve only remaining completed-result retention/backup contract using existing authorities; exact shared operation/schema changes and smallest restart test. Reuse prior saved-planning trace; no implementation or new harness. |

DAG: lead transient error → contention data/supervisor implementation → critical checks → actual
live selection/continuity → pushed checkpoint. Independent provider and saved-result contracts
feed the next implementation assignments; no new worktree, branch or review ceremony.

Implementation is frozen across the five code/test files and agent ownership is released. The
existing catalog custody/restart case passed (one test, six filtered), exercising real mutex and
SQLite contention, recovery, cancellation/deadline precedence and terminal poison handling.
Evidence: `.agents/tmp/v1-first-stock/native-contention-catalog-critical.log`. Of the existing three
supervisor cases, the extended contention/cancellation case passed; restart and capture-activation
cases failed `LiveScopeNotCovered` (`native-contention-supervisor-critical.log`). Their unchanged
fixtures omitted catalog identities now required for market sessions. Both now share real parsed
Coinbase product evidence published through the catalog. Restart checks reopen catalog/durable
registry/rate owners, resume exact metadata and select identity before generation admission;
capture cleanup retains its assertions. The old pre-cancelled supervisor fixture is removed,
along with its three unused test-only helpers; no production cancellation hook was added.
The corrected three-case rerun passed (three tests, 127 filtered;
`native-contention-supervisor-catalog-critical.log`).

The single-job CLI/service build passed. Same-root service shutdown exited zero, rebuilt startup
and protected credential unlock succeeded, then expired doctor renewal through Verify and Start
succeeded. Runtime reports `active_group` / lifecycle `active`; concurrent SPY search and selection
passed with retained selection/history identity and truthful stale-price absence. The new service
log contains no supervisor/registry failure at this cutoff. Desktop was reopened using its existing
binary; this is not a newly built installed package or full stock-analysis acceptance. Evidence:
`.agents/tmp/v1-first-stock/native-contention-live-{build,service}.log`,
`native-contention-live-{verify,start,search,selection,status}.json`. Test-only helper removal
followed the binary build and changes no non-test code. This is critically and live verified
contention recovery, not complete V1 or final installed acceptance. No schema, data limit or
compatibility path changed. All agents are finished and ownership released; the next dependency
is selected-stock admission alongside saved-planning implementation below.

### Saved-planning contract ready for the next implementation checkpoint

Acceptance 4/5/6: freeze one immutable completed calculation and a separate idempotent saved
marker. Persist every successful calculation's original request/output/account/snapshot/time and
internal evidence in the existing ControlledArtifactRepository plus indexed catalog completion
row before returning `calculationToken` / `calculatedAtUnixNanos`. No second in-memory result
store, TTL or automatic eviction; completed-unsaved results remain reachable and included in backup.
Save takes only account/token and never recalculates. List exposes saved results with account-bound
sequence-fenced cursors; Get reads one verified artifact and retains original evidence after expiry
or later imports. Operations: `Portfolio.SavePlanningResult`, `Portfolio.ListPlanningResults`,
`Portfolio.GetPlanningResult`. Cancellation of a later read cannot undo an accepted Save.

Persistence owner: cohesive `catalog/portfolio_planning.rs` using existing catalog authority;
lead owns current schema/export/factory changes in place, no new migration. Application owner:
`portfolio_application/saved_planning.rs` plus existing scenario/planning/candidate calculation hooks;
lead owns dispatch/contracts/transports. Backup owner: existing Portfolio component and
`local_product/operations/portfolio_backup.rs`, with a cohesive stream helper only if warranted.
Use completion/save head fences and streamed pages/one artifact at a time, including unsaved
completions. Retain heads before analytical catalog backup, revalidate after streaming and final
lease check. Restore through existing staged verify-and-rewind, match the restored catalog inventory
and exact artifact references before activation; index-only backup is insufficient.

Extend the existing portfolio control-plane restart case: save/reopen original results after
restart/import, idempotent Save, account isolation and backup/restored artifact agreement. Reuse
existing candidate evidence coverage; no new harness or routine component-test expansion. Writers
are not yet assigned: this contract is ready, but contention checkpoint integration remains first.

### Selected-stock admission contract ready for the next implementation checkpoint

Acceptance 1/2/3: remove the existing preparation cycle in which canonical IEX bindings are needed
before native assets can be acquired. Acquire the existing account/credential/rate owner before
binding construction, publish the selected native references, then transfer that same owner into
runtime startup. Do not acquire a second account owner or append routes after configuration seals.
Extend the existing Alpaca asset publisher with catalog-owned creation from a verified native UUID,
active US-equity classification and exact current official listing join. Allocate identity only
inside its transaction; replay resolves the source-qualified native UUID and rejects conflicting
listing/security joins. Preserve rights, custody, precommit and currentness checks. MSFT may be a
verification subject, never an admission allowlist. Do not invent a CUSIP or loosen the separate
Schwab source-reference contract. An Equity identity alone does not establish common-share fiscal
eligibility; the existing SEC company/security authority still owns that richer claim.

Use the adapter's existing `options_contract_reference.rs` pattern for hash-pinned primary-source
denomination evidence, tied to the actual request mode. The Alpaca
[latest-quote](https://docs.alpaca.markets/us/reference/stocklatestquotesingle-1) and
[startup snapshot](https://docs.alpaca.markets/us/reference/stocksnapshots-1) contracts (checked
2026-09-30) explicitly default prices to USD; that establishes quote units, not issuer
facts. The production publisher remains `market_data_instruments/alpaca_asset_reference.rs`;
adapter evidence belongs in `asset_reference.rs`. Composition changes span
`provider_activation/alpaca.rs`, `provider_activation/market_config.rs`,
`local_product/market_provider_configuration.rs`, `market_runtime/group.rs` and
`market_runtime/alpaca_asset_reference.rs`. Shared contracts/composition remain lead-owned.

Extend the existing production Alpaca composition/restart check with canonical stock creation,
native UUID retention, duplicate-free replay, token discovery and PIT/restart preservation;
use the existing catalog harness for conflict/revocation. There is no existing stock-creation case
that proves this missing branch. Preparation currently uses the resolver's bounded overview
selections; an already healthy group's Retry may return existing evidence without reconfiguration.
No new selection operation or reconfiguration behavior is claimed by this preparatory contract.
Read-only handoffs are complete and released; implementation ownership follows the contention
checkpoint. No new branches or worktrees were created. Fresh local and `git ls-remote --heads
origin` inventory confirms only feature/main/release; the separate `bundle-backup` is preserved.

## Prior discovery and freshness checkpoint — 2026-09-30

Pushed discovery checkpoint: `8f18c9ad` (three critical Rust checks, Desktop typecheck and
CLI/service build passed; ticker search and corrected source status live verified).

Current outcome: secure startup, protected credential import, fresh doctor verification and live
Alpaca Start/publication are verified at `88cf3ff1`, with evidence checkpoint `cbec2f05` and
PR #43 comments `5908352575` / `5908549656`. Desktop runs on the fresh corrected-schema roots
`.market-squawk/v1-owner-test-current`. The retained cutoff shows eight successful market-event
generations (32 cumulative rows) and a 23-row exchange calendar. This is not yet a complete stock
analysis or installed journey. Original roots and matching recovery binaries remain preserved.

Next concrete acceptance 1/3/5/6 defects: Market search matches only case-sensitive display names,
so admitted SPY is not found by ticker; source status exposes a hard-coded zero as a measured
qualified-record count. Quote timestamps from the prior session remain a separate freshness limit;
do not weaken that check or infer qualification from the placeholder count.

| Owner | Exclusive scope | Dependency and completion evidence |
| --- | --- | --- |
| GPT-6.1 Sol `live_input_recipe` | `crates/market-squawk-data/src/catalog/market_data_instruments.rs`, its existing `tests/catalog.rs` case; `application/market_selection/product.rs`, `application/paper/market/product.rs`, `durable_product.rs` | Reuse canonical term normalization/validity for ticker/name matching, project only an unambiguous admitted symbol, preserve full population binding/opaque pagination. Existing critical discovery coverage; no builds/Git. |
| GPT-6.1 Sol `position_desktop_trace` | `apps/market-squawk-desktop/src/features/sources/source-evidence.ts` only | Remove the fictional qualified-record-count field from active-group type/parser alongside lead contract change; no new UI, tests, builds or Git. |
| Astra `native_setup_current` | `crates/market-squawk-data/src/provider_event_selection.rs`, `src/manifest/catalog.rs`, existing `tests/publication_recovery.rs`; app `application/research/ingest/crypto_market.rs`, `application/market_selection/investment.rs`, `application/paper/market.rs` | Explicit current-mark tie policy: latest eligible source timestamp then latest received cohort, preserving equal-cohort ambiguity and original freshness. Generic historical all-ties remains intact; bind policy/exclusions into exact restart evidence. Extend existing publication/restart case with repeated observations. No cap/schema changes, builds or Git. |
| Lead | `application/source.rs`, source output contract in `application/contracts/output.rs`, existing `tests/production_mcp_composition.rs`, CLI/shared operation descriptions, data `lib.rs` export; shared authority, ledger, Git and checks | Remove fabricated count rather than manufacture a measurement; inspect consumers, one relevant existing critical check and integrated build. |

DAG: independent source-status correction and canonical search implementation →
integrated discovery check/build → actual ticker selection using current roots → pushed checkpoint.
Live name search `SPDR` returns the retained SPY token, proving identity publication. Selecting it
fails before freshness evaluation: 43 captures at the retained failure cutoff share the same source
timestamp, exceeding the historical all-ties request bound of 32. The current-read policy repair
selects the latest eligible received cohort without dropping historical rows or raising the bound;
equal-cohort ambiguity remains explicit. Source-status removal and Desktop typecheck are complete.
Search and current-selection implementations are frozen. The existing data publication/restart
case passed (1 test, 17 filtered), including repeated observations and equal-cohort conflict;
the existing canonical catalog identity case passed (1 test, 6 filtered), including ticker/name
matching and expired-alias exclusion. The product search regression passed (one test, 129 filtered), and the single-job CLI/service
build passed. Same-root restart/unlock and uppercase/lowercase SPY searches passed. The expired
doctor was renewed through Verify, then Start succeeded. The follow-up freshness correction is critically and live verified against these same roots:
SPY selection and the SPY/VTI overview succeed with original selection/history identity and
null price/asOf plus unavailable status. Existing source eligibility/freshness exclusions now
run before current native-reference validation; every surviving fresh candidate still receives
all reference and optional execution-term checks. No freshness limit, interval or identity was
weakened or backdated. The extended existing product case passed (one test, 129 filtered), and
the single-job CLI/service build passed. Same-root shutdown, secure startup, credential unlock
and retained-data CLI selection/overview passed; Desktop was reopened, not claimed as a fresh
installed package or visually verified complete workflow. Both code files are frozen and agent
ownership released. Evidence: `.agents/tmp/v1-first-stock/stale-market-product-critical.log`,
`stale-market-live-build.log`, `stale-market-live-selection.json`, `stale-market-live-overview.json`.
After renewing the expired doctor through Verify and successfully starting the source, SPY
selection also passed (`stale-market-live-active-selection.json`). The stale price remains null.
A separate runtime log reports ProviderIdentityAuthorityUnavailable; shared-catalog try_lock
contention and SQLite Busy/Locked currently share that category, so its cause is not yet proved.
Astra contention diagnosis is complete and ownership released. A single native identity
try_lock WouldBlock or SQLite Busy/Locked can terminate the async source supervisor before
using its existing selection deadline. The next bounded fix needs a distinct transient-busy
error and cancellation/deadline-bound async retry before session admission, covering constructor
and runtime selection through one owner. Preserve terminal poison/corruption/auth/stale-identity
handling and all-or-nothing mapping installation; do not block a Tokio worker or blanket-retry
AuthorityUnavailable. Reuse existing catalog custody and supervisor critical cases. No contention
implementation has begun; it remains separate from the frozen freshness correction.

Next dependency after this checkpoint: the installed source resolver currently admits only
predeclared SPY/VTI fund scopes and skips selected common stocks as CanonicalInstrumentUnresolved.
Stock admission must use genuine listing/native-asset evidence for supported selected equities,
with catalog-minted identity and exact currency/class/venue evidence. Do not add an MSFT-only
allowlist as a demonstration; MSFT may be the verification subject, not the admission rule.
Primary-source check on 2026-09-30: Alpaca's [latest-quote contract](https://docs.alpaca.markets/us/reference/stocklatestquotesingle-1)
explicitly declares a currency parameter with USD default; its [asset guide](https://docs.alpaca.markets/us/docs/working-with-assets)
describes genuine US-equity asset lookup. These are inputs to the next admission design, not proof
of an implemented stock identity or permission to infer issuer/share-class facts.

Saved-planning implementation has not begun and remains required. No new worktree/branch, full CI,
review round, release build or whole-app RAM measurement. This checkpoint has three passing
critical Rust cases, passing Desktop typecheck and a passing CLI/service build. Same-root secure
startup, credential unlock, Verify/Start, ticker search and active-group status are live verified;
stale-price selection is now live verified by the follow-up above; complete installed stock
analysis remains open. Exactly three local/origin branches and
one primary worktree were verified; no extra branches require removal.

Current checkpoint evidence: `.agents/tmp/v1-first-stock/market-discovery-live-*`,
`current-market-ties-critical.log`, `market-search-catalog-critical.log`,
`market-search-product-critical.log` and `search-status-desktop-typecheck.log`.

Prior live evidence: `.agents/tmp/v1-first-stock/current-live-publication-evidence.json`,
`current-live-alpaca-start.json`, `current-live-alpaca-status.json`, `current-live-spy-search.json`,
and `current-live-service.log`. Prior `.market-squawk/v1-owner-test` roots and matching binaries at
`.agents/tmp/v1-first-stock/pre-schema-a63d286b-binaries` are preserved, not migrated or rewritten.

Recovery checkpoint pushed: `44ba9cb6`; PR #43 evidence comment `5907711371`.
Next concrete defect: native Alpaca asset-reference publication registers a source only when absent,
then rejects changed metadata. The retained source binds the expired doctor; fresh verification
necessarily changes that metadata. Lead exclusively owns
`crates/market-squawk-data/src/catalog/market_data_instruments/alpaca_asset_reference.rs` and
`apps/market-squawk/src/application/market_runtime/alpaca_asset_reference.rs` for the existing
register-on-change revision path and typed publication-error logging. Preserve exact equality after
registration and all custody/identity/precommit checks. Existing source-revision coverage plus a
single-job binary build and actual retained Retry are the relevant checks; no new harness.

Native reference revision fix critically and live verified: the existing catalog recovery/revision
case passed (one test), and the CLI/service single-job build passed. Same-root secure bootstrap and
credential unlock succeeded. Retained Retry renewed the expired doctor without reimport and
published genuine SPY and VTI native references at 2026-09-30T09:09:48Z; the onboarding session
advanced to `active_scoped`. The market publication worker then failed with
`analytical manifest catalog operation failed`, leaving the runtime inactive and cleanup retained.
This is the next concrete blocker, not a password failure or a completed live-stock journey.
Evidence: `.agents/tmp/v1-first-stock/asset-reference-source-revision-critical.log`,
`asset-reference-recovery-build.log`, `asset-reference-recovery-service.log`,
`asset-reference-recovery-retry.err`, and `asset-reference-recovery-status.json`.
Astra `native_setup_current` next owns read-only diagnosis of the publication failure and retained
worker cleanup, tracing `market_runtime/alpaca_publication.rs` through the existing publication and
manifest owners. Return the smallest concrete fix and existing critical check; no edits/build/Git
or credential reads. Lead alone owns further integration. One worktree and exactly three local and
origin branches (feature/main/release) were freshly verified; none require cleanup.

Native reference checkpoint is pushed as `a63d286b`; PR #43 comment `5908009405`.
Next bounded remediation (acceptance 1/7): the first market-event Parquet object is present but its
catalog transaction never committed; the generic ingest error hid the manifest cause. Astra
`native_setup_current` owns only `apps/market-squawk/src/application/market_runtime/alpaca_publication.rs`
for bounded nested manifest diagnostics and separating failed workload from confirmed joined cleanup,
and `apps/market-squawk/src/application/research/ingest/alpaca_historical/market.rs` for an explicit
initial capture-custody error. Extend the existing library test module for the otherwise uncovered
cleanup distinction; no data-crate edit or tracing dependency is needed. Preserve genuine custody/join failures and all publication guards;
no speculative manifest behavior change, SQL/error-body logging, state edits or new harness.
Lead runs the existing `provider_market_event_publication_is_restart_queryable` data test and then
schedules integrated checks/build/live Retry. No other writer overlaps these files.

Existing data critical check is red: `provider_market_event_publication_is_restart_queryable`
fails `Catalog(ProviderEventMismatch)` (one test, 17 filtered) before the live path's generic
Manifest failure. Astra `position_authority_trace` has bounded read-only ownership of that existing
case and provider-event/catalog admission to identify whether it is a fixture defect or a producer
contract defect. The deterministic mismatch is a stale test source: the market-event fixture emits
AAPL/trades but reuses history-only source coverage with no live channel. Astra may fix that fixture
only in `crates/market-squawk-data/tests/publication_recovery.rs`, leaving the historical helper and
production admission unchanged. No new harness, builds/Git or production edits; root reruns and
retains data-crate integration. Its result must be reconciled
before accepting publication behavior; do not weaken the check or assume identical live cause.

Corrected fixture reaches the production defect: `Manifest(Sqlite(...1811...))`, because the
market-event schema fingerprint in the existing `0021_market_data_instruments.sql` differs from
canonical Arrow output. The actual live Parquet footer retains fingerprint
`e0bf8cc9a74c880cc772d3987907b13eb3d4d8fc2dc3ca1a239873d650a151f0`; SQL still pins
`631b28797ea2bacb7fd09f1669478f3a02d6a264c93d1929650ddef2ca3c96f4`.
Lead alone owns updating that existing schema definition and its digest in `src/migrations.rs`.
No new migration or compatibility bypass. Existing owner-test roots remain preserved; a fresh
owner-test root is required for the corrected greenfield schema, rather than rewriting retained
catalog history. Preserve the current matching binaries before rebuilding for recovery.

Publication repair verification passed: `provider_market_event_publication_is_restart_queryable`
(one test, 17 filtered) now publishes and reopens the exact market-event evidence. The application
library case `joined_publication_failure_preserves_custody_cleanup_authority` also passed (one test,
128 filtered), retaining the original publication failure while distinguishing confirmed cleanup
from custody/bounds/join failure. Evidence: `market-event-publication-critical.log` and
`publication-cleanup-critical.log` under `.agents/tmp/v1-first-stock`. No new migration/harness,
full CI, review round or resource measurement. Actual corrected-schema live acquisition is pending.

Audit base: `bcf22c8c`, primary feature branch, clean worktree. Acceptance 4/5/6 requires saved
portfolio scenarios, rebalance and position comparisons to reopen with their original assumptions
and evidence. These calculations now work across shared contracts, but the product has no save/read
journey. Acceptance 1/2/7 independently requires actual admitted stock/benchmark/fiscal inputs;
service unlock/import and Alpaca doctor probes now pass, but they are not publication evidence.

| Owner | Exclusive scope | Required handoff |
| --- | --- | --- |
| Astra High financial authority | Read-only existing portfolio calculation outputs, durable repositories/artifacts and relevant persistence tests | Smallest reusable saved-plan authority for all three results; exact input/output/persistence/identity/cancellation contracts and existing critical restart check. No edits/builds/Git. |
| GPT-6.1 Sol High Desktop | Read-only Portfolio panels/hooks plus existing saved-result selection/rendering | Precise reusable save/list/reopen UX and file ownership after backend contract; no duplicate calculator or whole-list preload. No edits/builds/Git. |
| GPT-6.1 Sol High provider | Read-only current provider/market-evidence preparation and retained non-secret readiness evidence | Exact existing operation sequence from imported/verified Alpaca plus references/SEC/rates to admitted subject/SPY/VTI history/fiscal inputs. Locate first real prerequisite; no credentials, network mutations, builds or Git. |
| Astra High lifecycle diagnosis | Read-only `local_product/source_lifecycle.rs`, account-group runtime and retained non-secret source status/logs | Reproduce from recorded start failure: doctor passed, Source.Start unavailable, revision 3 blocked/reconciliation. Identify exact failing transition and existing repair; no edits/builds/mutations/credential reads. |
| Lead | Shared contracts/composition/transport, live service actions, ledger, Git and builds | Freeze save contract after authority trace, then assign disjoint writers; perform current live-input operations independently. |

DAG: independent saved-authority and Desktop traces → one frozen persistence contract → disjoint
backend/Desktop implementation → lead integration and one critical save/restart check → pushed
checkpoint. Provider trace → lead live acquisition/typed read runs concurrently. Traces are bounded
handoffs, not new planning/review rounds; no broad audit or new branch/worktree.

Provider diagnosis complete: native asset-reference publication legitimately advances the effective
start, while `try_rebind_after_alpaca_reference` incorrectly requires an identical interval. Astra
`native_setup_current` now exclusively owns `apps/market-squawk/src/provider_activation/market_config.rs`
including one existing-library-target regression for this uncovered admission seam. Replace interval
equality with valid-at-cutoff, non-backdating/non-widening checks; retain every identity, currency,
reference, mapping and latest-revision check. No lifecycle reset or credential reimport. Lead reviews,
runs the single-job critical check, rebuilds the needed existing binaries once, and resumes the retained
transition through its supported recovery operation. Other authority/schema writers remain held until
this small provider checkpoint is frozen. Lead additionally owns `cli.rs`, `local_product/cli_transport.rs`
and the two source CLI reference/runbook entries for a thin `source retry` binding: ordinary MCP
intentionally omits source administration, and research-only RestoreSaved cannot resume Alpaca.
Reuse the existing lifecycle helper and its revision/configuration fencing; do not add a recovery owner. Saved-plan traces are complete; their implementation has
not started and remains the next independent product checkpoint.

Desktop recovery dependency: the existing blocked-source controls explicitly omit Alpaca Retry even
when a retained transition needs reconciliation. GPT-6.1 Sol `position_desktop_trace` exclusively owns
`apps/market-squawk-desktop/src/features/sources/source-evidence.ts` and its existing focused critical
control test, if present, to expose that existing native Retry action for the actual recoverable saved
configuration. Request remains observed revision plus reason, without replacement session/configuration.
No new transport, backend authority, build, Git operation or independent review is needed.

Native reference regression passed: the existing library binary ran exactly one
`provider_activation::market_config::tests::alpaca_native_rebind_accepts_later_catalog_publication_and_rejects_changed_authority`
case (later real catalog publication, reusable exact revision, stale/currency-changing rejection).
The first Cargo name filter selected zero cases; its exit status was not accepted as verification.
The built binary was then invoked with the fully qualified exact name and passed one case.
Evidence: `.agents/tmp/v1-first-stock/alpaca-rebind-critical-result.log`.

Live setup: runtime/provider unlock and credential import remain successful; H.15 expired anonymous
setup was refreshed through staged Start, then real activation succeeded, retaining its new recipe.
This establishes activation, not full historical publication. Source recovery exposed a separate
expired-pending-doctor dead end: Verify rejects the unfinished transition and Retry cannot renew its
expired candidate. The CLI/service build was interrupted early to avoid a duplicate build before
that repair. No active compiler remains; no state or credential reset was performed.

Next bounded authority remediation: Astra `native_setup_current` prepares changes only in
`apps/market-squawk/src/local_product/source_lifecycle.rs`,
`apps/market-squawk/src/provider_onboarding/service.rs`,
`crates/market-squawk-data/src/catalog_capabilities.rs`,
`crates/market-squawk-data/src/catalog/onboarding.rs` (the same retained-renewal deadline admission),
`crates/market-squawk-sources/src/onboarding/lifecycle.rs` and its existing `tests.rs`.
Preserve the existing RuntimeVerified event and same candidate generation. Only an expired exact
Alpaca receipt may gain fresh doctor evidence, with retained predecessor/configuration/credential
identity and current/newer verification checks; remain RuntimeVerificationPending until runtime
activation succeeds. Lead retains final shared-authority integration and all checks/Git. Extend the
existing same-generation renewal critical test; no new event/schema, authority bypass or reset.
Desktop's existing `src/test/app.test.tsx` is assigned solely for the one missing recovery control assertion.

Reference admission fix is pushed as `98fc0d43`. The pending-doctor repair and Desktop/CLI Retry
bindings are integrated in the current worktree. Lead also updated the dependent Schwab reconnect
caller in `source_lifecycle/reconnect.rs`; automatic reconnect does not gain Alpaca doctor renewal.
The existing renewal authority case passes one exact test; Desktop's blocked-Alpaca Retry case
passes one test and its actual `tsc --build` typecheck passes. The integration build found and
corrected a private cross-crate comparison and the missed reconnect caller; rebuilding remains
single-job. Live Retry and same-root reconstruction are not yet verified. No credential reset,
reimport, new worktree, whole-app measurement or broad gate occurred.

Live recovery exposed a further installed-startup defect: `resume()` expires the initial reservation
even for a stored, verified pending Alpaca candidate, appending CleanupRequired (event 7) and
unsupported remote revocation (event 8). Existing Retry cannot revive terminal credentials, while
the unfinished Start blocks Remove/Reconfigure. No local credential deletion occurred. Astra
`native_setup_current` now owns the bounded startup-retention fix in
`provider_onboarding/service/lifecycle_runtime.rs` and pending-operation cancellation in
`local_product/source_lifecycle.rs`, with the nearest existing critical test. Preserve terminal
cleanup semantics and audit events; permit an explicit stop/remove to drain and finish the old
intent under its exact revision before normal cleanup/new onboarding. Lead owns client bindings,
integration, single-job checks and live recovery. This is acceptance 7 recovery, not a state reset.
Integration additionally requires cancellation intent to be durable before any drain: a crash must
reopen Stop/Remove, never resume the superseded Start. Astra owns the necessary exact-record
transition change and existing durability regression in `local_product/provider_activation_state.rs`.
That existing regression now passes exactly one test (127 filtered): both Stop/Remove take over
under a new revision before cleanup, reject changed targets/stale callers, reopen as the replacement
intent, and preserve the completed operation identity. Evidence:
`.agents/tmp/v1-first-stock/account-cancellation-critical.log`. All writers are frozen; lead is
building the CLI/service for the retained-root cleanup and new verification. The prior terminal
onboarding session will not be revived or edited; supported cleanup and ordinary new setup are
required, with audit history and market data preserved.

Live evidence after the final binary build (`account-recovery-build.log`, passed): service shutdown
completed normally, same-root secure bootstrap and provider unlock were accepted. `Source.Remove`
completed at revision 4, preserving historical audit/data. Protected import reused existing saved
credential setups and created Alpaca session `269ab508-a402-49c2-bb47-46b69d2ddb06`.
Verification completed at revision 5: 31 historical bars on 31 dates, matching calendar and admitted
quote/WebSocket probes. It does not establish SIP/NBBO or full returned snapshot coverage.
The following Start failed at `application/market_runtime/group.rs::await_before` with unavailable;
revision 6 remains blocked/reconciliation, with its fresh pending candidate preserved.
Evidence: `.agents/tmp/v1-first-stock/native-alpaca-remove.json`,
`native-provider-recovery-import.json`, `native-alpaca-recovery-verify.json`,
`native-alpaca-recovery-start.err`, `native-alpaca-post-recovery-status.json` and
`account-recovery-service.log` in the same directory. The source-expiry authority regression,
existing durable cancellation/reopen regression, Desktop Retry regression and Desktop typecheck
passed. No full CI, new quarter review, release build or whole-app RAM measurement ran.

Saved-planning handoff (implementation pending): share the existing SQLite catalog authority and
`ControlledArtifactRepository`; do not retain a whole saved-results index in application memory.
All three calculations must issue an opaque server-owned completed-result token with their original
request, output, account/snapshot, calculation time and source evidence. Save takes account plus token,
never client financial JSON or recalculation. List uses account-bound, sequence-fenced cursors; Get
loads one immutable artifact and preserves the original evidence after later imports or price expiry.
Desktop reuses the three existing result renderers, adds explicit Save and one demand-loaded saved
panel with cursor navigation and selection-only detail fetch. Cancellation of detail reads must not
undo an accepted save. Before assigning writers, lead must settle completed-but-unsaved artifact
retention and include artifact reachability in existing backup/export; index-only backup is insufficient.
Extend the existing portfolio control-plane restart case to reopen saved outputs, rather than adding
another harness or accepting its current post-restart recalculation as save/reopen proof.

## Position-impact integration — 2026-09-30

Pushed source checkpoint: `afa8ae6ccb2cd2669ddd056127c9d61d574892c2`.
[PR #43 evidence](https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5906585986)
records critical check scope and remaining live/installed gaps. No lane remains assigned for this
calculation checkpoint; lead owns next saved-planning integration and live setup follow-through.

Audit base: pushed `d151c37f777fb0acd6369c3a5264c303be7e05a5`, then a clean primary worktree.
Acceptance 4/5/6 defect: Portfolio supplied `positionChoices={null}`, and the existing candidate
operation used recommendation setup rather than the displayed account. The integrated change
replaces that placeholder with explicit investment selection, total quantity and percentage inputs,
using the selected current account and the existing shared financial calculation.

| Owner | Scope | Finish evidence |
| --- | --- | --- |
| Astra High | Read-only `portfolio_application/candidate.rs`, `recommendation.rs`, `service/portfolio_analysis.rs` and direct contracts/tests | Exact smallest change to reuse candidate calculation with explicit product selection, financial assumptions and evidence; identify existing critical check. No edits/builds/Git. |
| GPT-6.1 Sol High | Read-only Portfolio planning, investment selection, product transport and candidate projection consumers | Reusable selection/rendering path, precise missing bindings and proposed disjoint Desktop files. No edits/builds/Git. |
| Lead | Shared contracts/authority, ledger and integration | Freeze coherent producer/consumer contract after traces, then assign bounded implementation; serialize checks and commit/push. |

DAG: independent authority/UI traces → frozen contract and file ownership → parallel implementation
→ integrated critical verification → pushed checkpoint. Saved planning persistence/reopening remains
required after calculation contracts; first live stock journey remains independently required.

Contract frozen after traces: `Portfolio.EvaluateCandidateImpact` takes explicit `accountToken`,
canonical `instrumentId`, exact-string `proposedQuantity` (total desired quantity, zero means exit),
and `scenarioShockPercent`. Backend converts percent; no defaults or client arithmetic. This is
a fresh-evidence calculation, not a historical stress snapshot. Resolve the chosen current account
without changing recommendation settings, retain current market freshness/rechecks and reuse the
existing financial calculation. Source-admitted fractional holdings must not be rejected merely
because the current execution lot size differs; proposed quantities retain their explicit policy.

Existing output remains, adding `accountToken`, UUID `snapshotToken`,
`portfolioEffectiveAtUnixNanos`, `portfolioAvailableAtUnixNanos`, SHA-256 `evidenceDigest`,
`assumptions:{proposedQuantity,scenarioShockPercent,quantityMeaning:"target_total",
fundingAssumption:"cash_transfer_before_costs",
portfolioValueBasis:"source_reported_holdings_with_selected_candidate_revalued",
scenarioScope:"candidate_position_only"}` and `price.freshUntilUnixNanos`. Projection must describe cash-funded capital transfer and omitted
costs/settlement authority honestly. Financial authority retains original evidence for subsequent saving.

Implementation ownership supersedes read-only entries above: Astra owns `portfolio_application/candidate.rs`
and `application/paper/market/candidate.rs`, with existing local critical cases; lead owns shared
exports/factory/composition and contracts. Sol owns Portfolio `portfolio-planning.tsx`, new
`portfolio-position-impact.tsx`, `portfolio-contracts.ts`, `use-portfolio.ts`, `portfolio-page.tsx`.
Lead owns all shared TS/native transport, CLI, existing app/service harnesses, verification and Git.
No overlapping edits or independent builds. Inline investment lookup reuses the admitted canonical
investment result, supports new and held investments, and shares calculation cancellation lifecycle.

Implementation handoffs are integrated and ownership released. Lead inspected the affected
financial authority, Desktop consumers, native/CLI contracts and shared metadata. Superseded
prepared-choice UI and unused recommendation-bound candidate constructors were removed. The
independent investment-analysis setup authority remains intact. MCP uses the same updated
application contract; no second calculator or financial frontend arithmetic was introduced.

Critical verification:
- Existing candidate case passes (1 test): explicit non-default account, original decimal inputs,
  fractional existing holding with a different execution lot, zero-quantity exit, financial values,
  cancellation/mismatched evidence and actual output-contract validation. Initial fixture metadata
  was corrected to match production source evidence; the operation schema was not weakened.
- Existing selected-portfolio/planning UI checks pass (2 tests): real canonical lookup selection,
  exact account/assumption request, returned financial values, edit invalidation, close cancellation
  and discarded late replies. No new component harness was added.
- Desktop TypeScript and the native Desktop library check passed. Existing unrelated warnings
  remain; `git diff --check` passed. All Rust checks used one job and disabled incremental state.
Logs: `.agents/tmp/v1-first-stock/portfolio-position-{math,ui,typecheck,native}.log`.

This is an implemented, critically checked calculation slice; saving/reopening planning results,
current-build live position calculation and full installed acceptance remain required. Next product
dependency is common saved planning evidence/reopening. Native setup and first-stock input admission
continue independently. No full CI/release gate or whole-app RAM measurement ran.

Native setup with owner present: the supplied local unlock was accepted through the existing
CLI bootstrap and provider-store unlock operations. A separately started existing service now
reports ready against the preserved `.market-squawk/v1-owner-test` root. The protected one-time
credential import completed for 17 provider entries: eight credentials stored but unverified,
nine awaiting probes. Receipt: `.agents/tmp/v1-first-stock/native-provider-import.json` (redacted
operation output only). No credential values were written to documentation or shell arguments.
Alpaca's live verification then returned 31 IEX daily bars across 31 dates, and admitted latest
quote, UTC calendar and WebSocket checks; snapshot-batch coverage was degraded. Lifecycle
availability remains indeterminate. Receipt: `.agents/tmp/v1-first-stock/native-alpaca-verify.json`.
This proves the named live probes, not durable analytical inputs or installed workflow completion.

The prior Desktop cached its initial bootstrap state after an external CLI unlock. Restarting that
Desktop also ended its child service in this execution environment. Lead therefore started the
existing service separately, unlocked both stores, imported the bundle and reopened Desktop to
connect to that ready service. No new build or data reset was used. Native activation returned
success, but an on-screen product window has not yet been independently confirmed. Astra's
read-only startup diagnosis is complete; ownership is released. Native reconnect behavior after
external unlock remains a concrete lifecycle gap to reconcile with the current source.

## Model-routing correction — 2026-09-29

Before resuming, the owner replaced every future GPT-6 Sol assignment with **GPT-6.1 Sol High**
(`gpt-6.1-sol`, High effort). Unversioned Sol labels in older plans/handoffs mean this current
selection. **GPT-6 Astra High** (`gpt-6-astra`) and all task boundaries, ownership, review,
verification and stopping rules remain unchanged. Historical agent provenance is not rewritten.
The tracked goal, execution plans, project memory and live goal attachment carry this correction.
The documentation-only checkpoint `edbce3b2` started no agents, builds or product changes and
preserved the unfinished rebalance work. Its execution hold ended with the subsequent active goal
resumption below. Earlier quota-stopped rebalance attempts produced no edits; the resumed financial
and Desktop lanes use the corrected model routing.

## Integrated rebalance calculation — 2026-09-30

Execution resumed through the active goal continuation after model-routing commit `edbce3b2`.
The earlier documentation hold is lifted. Refreshed source confirms the same duplicate cash-scaling
defect and unchanged local contract WIP. Reassign the reserved financial/Desktop files below;
previous quota-stopped attempts produced no edits. The lead retains all shared-file and check ownership.

Acceptance 4/5/6 at `5e0e2ee6`: planning renders null prepared choices. Two active implementations
compute buy capacity from unscaled sales, then scale both buys and sales; cash 100, holdings
200/100, targets 25%/75%, reserve 50 wrongly fails though scale 0.5 is feasible. Integrate one
shared financial calculation and snapshot-bound explicit planning across clients.

| Owner | Exact exclusive files | Dependency and finish evidence |
| --- | --- | --- |
| Astra High financial | `crates/market-squawk-portfolio/src/rebalance.rs`, existing `tests/analytics.rs`; `apps/market-squawk/src/portfolio_application/advanced/planning.rs` | Correct cash/turnover constrained common value calculation, reuse from existing revision-bound proposal and application. No fabricated ledger revision. Historical display, original assumptions, cancellation. Existing critical rebalance case; no builds/Git. |
| GPT-6.1 Sol High Desktop | Desktop `src/features/portfolio/portfolio-planning.tsx`, new cohesive `portfolio-rebalance.tsx`, `portfolio-contracts.ts`, `use-portfolio.ts`, `portfolio-page.tsx` | Replace obsolete prepared rebalance choices with explicit form; reuse snapshot-pinned paged holdings lifecycle from stress without copying it. Demand load, exact strings, clear assumptions/cost gaps and cancellation. Position-impact remains separately unfinished. No builds/Git. |
| Lead | Portfolio crate public export only; application advanced routing/read/service; shared contracts and TS/native transports; CLI/docs; existing critical service/UI journey; ledger/Git/PR | Freeze input/output below; integrate shared financial authority and consumers; serialize critical checks, push coherent checkpoint. |

ProposeRebalance: `accountToken`, `snapshotToken`, `proposal:{targets:[{instrumentId,targetPercent}],
maxTurnoverPercent,minimumCash:{amount,currency},allowShort}`. Percentages and amounts are exact
strings, no frontend financial conversion; every held instrument needs one explicit target, totaling
100% of portfolio value including cash. Backend evaluates reserve/turnover constraints and reports
partial progress honestly. No default targets, reserve, turnover or permission to retain shorts.
Output: standard account/snapshot/clocks/confidence plus original `proposal`, `totalValue` money,
`trades:[{instrumentId,investment:null|{name,symbol},currentValue,valueChange,projectedValue}]`,
`projectedCash` money, `turnoverPercent` string, `constrained` boolean. Values are hypothetical
adjustments, not executable quantities/orders; fees and current execution prices are not estimated.

DAG: contract → disjoint financial/Desktop work → lead integration and existing critical checks
→ commit/push and release ownership. Shared kernel API/export is settled with financial owner
before application integration. No independent builds, CI, resource measurement or review rounds.
Saved planning result persistence/reopening follows the calculation contracts; remains required.

Financial and Desktop handoffs are implemented and inspected; ownership is released to the lead
for verification/integration. One `RebalanceCalculation` serves both real revision-bound proposals
and source-observation planning. Exact integer intermediates constrain net cash and half-gross
turnover; conservative representational rounding preserves conservation and cannot create new
shorts. Original input strings, selected snapshot and historical display are retained. Report row
paging defaults no longer limit the number of required allocation targets. Existing request/result
byte and publication bounds remain; no inputs are truncated or fabricated.

Desktop uses the shared pinned position selector and transient calculation cancellation for stress
and rebalance. Targets survive cursor navigation; every policy input is explicit. Root consolidated
exact percent parsing and investment labels, updated the CLI/native/MCP producers and consumers,
and inspected the short-position precision boundary. Position-impact and saved planning/reopening
remain separate required work, not claimed complete by this calculation checkpoint.

Critical verification passed on the integrated source:

- Existing financial case `analytics_reports_are_policy_explicit_bounded_and_revision_bound`:
  1 passed; covers feasible reserve scaling, nonterminating ratios, conservation and turnover.
- Existing service case `portfolio_import_atomically_publishes_the_queried_revision`: 1 passed;
  original snapshot, submitted assumptions and rebalance result survive restart and newer imports.
- Existing Desktop selected-portfolio and explicit-planning cases: 2 passed, 7 skipped; explicit
  inputs, cursor-spanning targets, backend results, edit invalidation and late-response cancellation.
- Desktop TypeScript and native library compilation passed. Existing unrelated warnings remain.

Logs are under `.agents/tmp/v1-first-stock/portfolio-rebalance-{math,service,ui,typecheck,native}.log`.
Rust compilation was serialized with one job. Reproducible application package debug output cleanup
removed 16.8 GiB before compilation (target fell from 29 GB to 13 GB). No CI, release build,
whole-app memory measurement or extra review round ran. This is implemented and critically verified;
live verification and installed workflow completion are not established by these checks.
Next dependency: complete position-impact calculation and common saved planning evidence/reopening;
the first live stock journey remains independently required. All lane ownership is released.

## Integrated stress-scenario calculation — 2026-09-29

Acceptance 4/5/6, refreshed at `005037da`: stress UI is an unconditional prepared-choice
placeholder; existing exact calculation selects latest account state and can compose additive
price shocks below -100%. Replace the active path in place, not a preset registry or second engine.

| Owner | Exclusive files | Dependency and completion evidence |
| --- | --- | --- |
| Astra High financial | `apps/market-squawk/src/portfolio_application/advanced.rs`, `advanced/scenario.rs`; `crates/market-squawk-analytics/src/scenarios.rs`, existing `tests/golden.rs` | Frozen selected-snapshot input below. Correct composed price floor; calculate only explicitly affected holdings with cancellation, preserve submitted assumptions and historical display. Extend existing exact scenario critical case. No builds or Git. |
| Sol High Desktop | Desktop `src/features/portfolio/portfolio-scenarios.tsx`, `portfolio-contracts.ts`, `use-portfolio.ts`, `portfolio-page.tsx` | Frozen response below. Demand-loaded paged position selector, explicit single/batch assumptions and calculation, cancellation and selection invalidation. Replace obsolete stress choices in place. No financial arithmetic, builds or Git. |
| Lead | Shared Rust operation/output contracts, read/snapshot selection and service composition; TS/native transport; CLI adapter/docs; existing control-plane/Desktop critical journey; ledger/PR/Git | Pin opaque account plus snapshot; integrate all consumers, inspect handoffs, serialize one-job Rust checks, typecheck and existing critical UI/service/math cases, then commit and push. |

Contract: EvaluateScenario/Batch take `accountToken`, `snapshotToken` and `scenario`/`scenarios`.
Each scenario has `id`, explicit `composition` (additive/compounded), and `shocks` containing
`instrumentId` plus exact string `percentChange` (e.g. `-10`). Backend alone converts percent to
rate. Results retain report clocks/confidence/snapshot, submitted shocks, affected contributions
with `instrumentId`, nullable `investment:{name,symbol}`, money `amount`, and money `total`.
Unshocked positions and cash are unchanged; results are hypothetical position-value changes,
not forecasts, probabilities, trades or saved scenario records. No scenario or shock defaults.
Single and batch use the same admission/calculation. Work is request-sized rather than rejecting
large portfolios through the existing arbitrary allocation-times-shock budget.

DAG: frozen contracts → disjoint financial/UI implementation → lead producer/consumer integration
→ critical checks → pushed checkpoint and ownership release. Native live stock remains pending
owner unlock/setup; independently useful work continues. No full CI, release gate or RAM measurement.

Financial and Desktop handoffs are complete and inspected; their ownership is released to the
lead for final integration. Exact affected-holding calculation replaces whole-portfolio materialization
and the arbitrary allocation-times-shock cap. Repeated shocks are grouped by investment and
composed with the existing exact money operators; a negative terminal price or precision loss is
rejected as an invalid calculation, not clamped into a plausible result. Submitted names/percentages
are retained without the old generic report string rewriting. The same selected-snapshot request
runs through Desktop, CLI and shared MCP descriptors. Historical display uses original clocks.

The Desktop stress panel loads only on expansion, pages holdings within one fixed observation,
accepts explicit single/batch assumptions, and clears/cancels obsolete calculations on edit,
refresh or close. Ordinary account-directory refresh retains its existing semantics. No compatibility
path, preset registry, new branch/worktree or financial calculation in React was added.

Critical evidence: existing exact scenario golden case passed (1 selected); existing portfolio
import/restart case passed (1 selected, 28.65 seconds), including single/batch schema validation,
submitted-string preservation, invalid combined-price rejection and identical original-snapshot
calculation after restart plus two later imports. Existing Desktop selected-portfolio journey passed
(1 selected, 8 skipped), covering actual form/request/result and late-response cancellation;
TypeScript and native Desktop library compilation passed; `git diff --check` passed.
All checkpoint source ownership is released; the lead owns only commit/push and evidence recording.

These are critical fixture proofs, not live or installed acceptance. Stress results remain transient
calculations over durable portfolio observations; saved planning assumptions/results and reopening
still require implementation. Next dependency after this checkpoint is corrected rebalance planning,
then common saved planning evidence/reopening. Native setup and the real saved stock journey remain
open independently; no full CI, release gate or whole-app memory measurement ran.

Pushed source checkpoint: `c7a5170c17fb8b60930b80f6d76df8b369a3c641`.
[PR #43 delivery evidence](https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5894065626)
records the exact check scope and remaining acceptance gaps. One primary worktree remains;
no auxiliary branch/worktree was created. Local check logs are under
`.agents/tmp/v1-first-stock/portfolio-stress-{math,service,ui,typecheck,native}.log`.

## Current execution — first-stock wave resumed — 2026-09-29

The owner resumed the registered goal after reviewed planning. The [owner-test goal](v1-owner-test-goal.md)
remains the acceptance contract; the [reviewed first-stock wave](v1-first-stock-wave.md) supplies
bounded tasks. Source refresh at `21b7f2254743f92d4e5ebe756916fdd8701b67b5` found only documentation
changes since reviewed base `523da3b9`; product baseline remains `9543ed35`. Planning approval is
recorded in the [independent review](../reports/2026-09-29-v1-completion-plan-review.md).

One primary worktree on `feature/v1-installed-product-experience`; no new branches/worktrees.
No Cargo/rustc build was active at resume; free disk was 126 GiB. Rust target was deliberately
cleaned. Root alone schedules one-job, nonincremental builds and relevant critical checks. No
ordinary-task CI, release gate or whole-app RAM measurement. Sessions and recovery backups stay intact.

| Task / concrete outcome | Current owner and exclusive files | State / next dependency / critical evidence |
| --- | --- | --- |
| S0/P1: actual stock/benchmark/fiscal readiness | Sol High readiness handoff complete; lead owns runtime/setup scheduling. `apps/market-squawk/src/application/research/corporate_actions/preflight/history.rs` remains unchanged and unassigned until a demonstrated failure | Both retained workspaces have no admitted stock/benchmark/fiscal inputs or confirmed account/allocation setup. Retained model runtime receipts are stale. Local read-only findings and source-defined commands: `.agents/tmp/v1-first-stock/readiness.md`. Root must start a current service before acquisition proof. |
| F1: valid absent forecast/current-market publication | Astra High handoff integrated; ownership released | Implemented and critically verified in `c33065b2`. One existing-library regression passes all three forecast/market combinations through actual canonical request admission, preserves independent evidence, checks binding agreement and begins with the real market-token producer. |
| R1: admitted lookup destination | Lead integration complete; ownership released | Implemented in `c33065b2`: existing point-in-time market-reference search and exact-ID pinning return canonical selection tokens through the shared contract. No whole-universe load or execution-definition prerequisite. Compiled and covered at the Desktop destination seam; live catalog lookup remains part of P1/I1 proof. |
| R1 follow-through: admit actual Markets tokens into analysis | Lead integration complete; ownership released | Corrected in `c33065b2`: workflow admission, command schema and shared schema validator use the actual 32-character market-token suffix. The critical regression uses the real producer and registers application capabilities; existing schema assertions pass. Obsolete hyphenated format removed in place. |
| U1: requested investment and truthful profile availability | Sol High handoff integrated; ownership released | Implemented in `c33065b2`: backend-token routing, exact requested detail, stale-selection recovery and actual profile availability. Final existing lookup/market critical checks passed (2 selected tests); TypeScript passed. These are fixture checks, not live Desktop proof. |
| I1: real saved brief and shared-service restart | Lead; existing lifecycle/persistence tests only if an uncovered critical seam requires extension | Depends on usable P1 inputs and F1/R1/U1 integration. Actual positive stock result, immutable saved readback and restart are not yet proved. |

Lead reserves all other shared contracts/composition/transport/manifests/lockfiles, Git and test
scheduling. Additional file ownership requires an explicit ledger assignment before edits. Inspect
actual handoffs, integrate producer/consumer slices, verify, commit/push and then release ownership.
The token follow-through also assigns `crates/market-squawk-services/src/output_schema.rs` to the
lead: its existing closed-schema validator carries the same obsolete hyphenated token pattern.
Update that pattern and its existing critical admission assertions together; no new test harness.
Current blockers: no admitted first-stock inputs or confirmed account/allocation; retained model
runtime is stale. Next barrier: start the current service and acquire actual inputs through native
setup. Existing model-less startup can support
connection setup because neither workspace has durable model admissions; positive forecasting still
requires a refreshed model runtime. No runtime-admission bypass is authorized.
Source checkpoint `c33065b2` integrates F1/R1/U1 and the token follow-through. Both shared Rust
checks passed with one compiler job: `publication_action_references_follow_forecast_and_market_admission`
(1 selected test, including application capability registration) and
`operation_schema_must_be_specific_and_runtime_validation_is_closed` (1 selected test). Existing
Desktop lookup/market checks passed (2 selected tests) and TypeScript passed. `git diff --check`
passed. Logs are retained locally under `.agents/tmp/v1-first-stock/`; the test names and scope above
are the durable evidence summary. No full CI, release gate, RAM measurement, live provider admission,
positive forecast, saved-Brief restart or installed-journey completion is claimed.

Current native service/CLI/helpers built successfully with one compiler job. The normal development
root failed startup because its retained identity payload is an obsolete untagged format-2 record
(`formatVersion`, `installationId`, `legacySecretCleanup`); both payload hashes match, but this is not
the current tagged runtime identity. No migration, deletion or identity rewrite was performed.
Root explicitly selected the retained `.market-squawk/oauth-live-v1-20260923/installation` and
paired `data` root to reuse its current identity and saved native setup. The current service reaches
the typed `encrypted_fallback_locked` bootstrap state there. This is setup progress, not data admission.
Both workspaces and their original evidence remain preserved.

Root owns native Desktop build/launch and unlock setup; builds remain serial, one-job and
nonincremental. Stale model-runtime binaries are not used as current proof. The bounded read-only
Astra High investigation confirmed obsolete MCP migration/cleanup code. Root now owns
`service/mcp_control.rs`, its `try_prepare` call in `service/runtime.rs`, and the obsolete MCP
cleanup-reference append in `service/mod.rs`. Remove that path in place while preserving durable
revocations, live credential rotation and real runtime signing-secret cleanup. Reuse the existing
shared-service restart test and MCP smoke; add no test suite. Native compilation was stopped before
application compilation so this correction can enter the same launch candidate; completed compiler
cache remains intact. Both retained MCP documents contain the obsolete cleanup field. Preserve them
and use a fresh `.market-squawk/v1-owner-test` installation for current first-launch verification,
without translating old state, resetting its revocations or claiming old-workspace recovery.
The existing shared-service restart test passes after removal (one selected case, 48.06 seconds).
The MCP smoke exposed stale expectations: it still requires management-only domains, the hidden
`Market.GetSnapshot` operation and native metadata intentionally absent from product discovery.
Root also owns `scripts/smoke_mcp.py` to align that existing check with `product_capabilities`,
the provider-neutral `Market.GetOverview` operation and public MCP authority annotations. The
actual status/mutation/EOF/shutdown checks remain; no product authority is widened to satisfy smoke.
The smoke's old paper `state`/`shutdownComplete` expectations were also updated to the current
`sessionAvailability`/`safeguards` product contract. Final smoke exits zero, including real service
bootstrap, MCP calls, relay EOF and signing-secret retirement. The shared-service restart check and
`git diff --check` also pass. This checkpoint removes the obsolete MCP path across three service
files and updates one existing smoke; no new test, migration, branch or worktree was added. The
read-only helper is complete. Root's single-job native Desktop build subsequently completed for the fresh
V1 root; local secure setup and actual provider/data admission are still pending. No live financial
or installed workflow completion is claimed.

All earlier active/current tables and local wave handoffs are historical, not live assignments.

### Integrated client slice — saved valuation — 2026-09-29

Acceptance items 2/5/6: render actual saved valuation methods and ranges rather than an unconditional
unavailable page. Source refresh after `1ca49bf6` confirms the backend producer and Rust output schema
already publish `priceSummary.valuationMethods`; Desktop's strict parser omits it, rejecting that
saved result even when the value is null. The Valuation page unconditionally renders unavailable.

| Owner | Exact writable files | Dependency / completion |
| --- | --- | --- |
| Lead | Desktop `src/features/opportunities/contracts.ts`, existing `src/test/app.test.tsx`, ledger/Git | Match the existing Rust method-set schema in place, including signed amounts and method-specific assumptions. Reuse exact-decimal primitives. One existing-harness critical journey for saved-result parsing/selection and rendered methods; typecheck. No Rust change or compiler duplication. |
| Sol High UI (`saved_valuation_finish`, continuing preserved partial edits) | Desktop `src/features/fair-value/fair-value-page.tsx`, optional cohesive `valuation-evidence.tsx`; `src/features/opportunities/opportunities-read-experience.tsx`, `investment-brief.tsx`, `format.ts`, optional `use-saved-investment-analysis.ts` | Shared saved-list/detail reads and range presentation may be extracted within these files for reuse by both views. Use the lead's method-set contract; explicit selected saved result, cursor list, demand detail, cancellation/session scoping; show four methods and their actual amount basis, assumptions, missing reasons and saved ranges. No financial arithmetic, new transports or backend edits. Report exact changes without builds/Git. |

The preserved UI work was completed by `saved_valuation_finish` and inspected by the lead; its
ownership is released. The coherent checkpoint containing this entry fixes the strict saved-result
parser and wires Valuation & Targets to exact saved analyses, independently of the current cursor
page. Both it and Opportunities reuse one cancellable, session-scoped list/detail reader and one
history presentation. The four method outcomes retain signed amounts, actual value basis, method
assumptions and original cutoffs. The Everyday brief exposes these details on expansion. Both views
reuse all seven range slots, with unavailable values and saved explanations instead of omitting
missing ranges. Exact money formatting is shared; no financial formula or backend authority moved
to React. No compatibility path, migration, branch or worktree was added.

Critical verification: the existing Desktop app harness now has one saved-valuation journey for the
previously uncovered producer/parser/display seam. It opens a URL-selected result absent from the
current history page, checks total-equity versus per-unit presentation and negative amounts,
preserves seven unavailable range slots, then explicitly switches to the listed result. It also
admits the backend's required null method-set case. Final selected run passed (1 passed, 7 skipped,
1.46 seconds); TypeScript and `git diff --check` passed. No broad CI or new test harness ran. These
are critical fixture checks; real stock calculation and installed saved-result restart remain open.
The pushed implementation is `3b25b647`; its verification is recorded in PR #43.

The lead's one-job native Desktop build completed successfully in 11m55s. The current Desktop and
shared service initially started against `.market-squawk/v1-owner-test`; the current CLI confirmed
`bootstrap_required` / `encrypted_fallback_locked`. At final observation the Desktop process remains
open, but the service is terminal and `service status` is unavailable; its startup receipt records
`failed/runtime-composition`. This is consistent with the existing five-minute bootstrap deadline
elapsing before local password entry. No unlocked workspace or visible usable window was verified.
Reopen native secure setup when the owner is present, using the built binaries rather than another
build. The owner enters the password only in the app; no password is requested in chat or invented
by an agent.
Both retained installations remain preserved. The model runtime and live input barriers remain;
UI fixture success does not establish real-data or installed workflow completion.

### Active client slice — saved chart CLI — 2026-09-29

Acceptance item 6 / reviewed Wave 4 finding C2, refreshed at `93c3b0be`: the saved chart
operation already owns exact viewport/layer reads, but `analysis` has no command for it.
Lead owns `apps/market-squawk/src/cli.rs` and `src/local_product/cli_transport.rs`, ledger,
Git and verification. Add a thin installed-service adapter for the saved action token,
optional nanosecond window, layer and display point limit. Preserve service validation and
structured evidence; no financial logic or new history materialization. Sol High owns only
`docs/reference/cli.md` for the corresponding operator instructions, after the command contract
is supplied. These files are disjoint; no other assignments are active. Reuse the existing
closed-operation schema check plus compilation; live three-client/restart equivalence remains
part of I1 and is not proved by this adapter check. Native secure setup remains independently
pending owner input; no native rebuild is scheduled for this slice.

Implemented: `analysis chart` forwards the original saved token and optional viewport/layer to
`Decision.GetInvestmentChart`, with nanoseconds encoded as exact strings and no client financial
calculation. Omitted fields retain service defaults; existing shared admission owns valid layers
and viewport bounds. Lead inspected the docs helper's changes, the command mapping and the existing
saved reader; docs ownership is released. The whole application library compiled with one job,
and existing `saved_benchmark_chart_accepts_retained_series_and_unavailability` passed (1 passed,
125 filtered, 0.08 seconds; compilation 3m50s). `git diff --check` passed. No tests or harnesses were
added; this verifies compilation and the existing chart publication contract, not live CLI dispatch
or three-client restart equality. Those require the original saved-stock evidence in I1.
The coherent implementation/docs checkpoint is `25ec5422`, pushed to origin; CLI implementation
ownership is released.

Independent native-startup investigation: Astra High has read-only ownership of the existing
Desktop visibility/bootstrap path (`src-tauri/src/lib.rs`, `service.rs`, related bootstrap state
and retained native diagnostics). Concrete failure: Desktop process is alive but no usable window
was verified, while secure setup timed out. Determine whether source/runtime evidence identifies
an application defect before treating this solely as owner delay. No edits, builds, restarts,
credential access or user interaction; lead retains application composition. Return a bounded
diagnosis and exact next action, not a new infrastructure plan.

Investigation complete; read-only ownership released. The native process sample is inside
`run_return` / `NSApplication run`, not blocked on service connection. macOS unified logs report
the created window visible but occluded, and the lead independently confirmed
`CGSSessionScreenIsLocked=Yes` through read-only `ioreg`. This supports a locked desktop obscuring
the window, not a demonstrated startup defect. No visibility code or dependency change is justified.
Next native step: after the owner unlocks macOS, restart the expired setup against the same V1 root
using existing binaries, verify the visible setup screen, and let the owner enter the app password
locally. Usable rendering, unlocked service and live data admission remain unverified.

### Active Portfolio integration — 2026-09-29

Acceptance 4/5/6, reviewed Wave 2, refreshed at `35caedd4`: Portfolio renders unconditional
unavailability for selected accounts, while Risk & Guidance already consumes the actual product
risk report by opaque account token. First coherent dependency checkpoint: reuse that exact
cancellable report and its presentation in the selected Portfolio view, independently of missing
holdings/performance/history/planning wiring. Do not call this complete Portfolio acceptance.

| Owner | Exact scope | Completion evidence / dependency |
| --- | --- | --- |
| Sol High Portfolio risk UI | Desktop `src/features/risk/risk-page.tsx`, new cohesive `src/features/risk/account-risk.tsx`, `src/features/portfolio/portfolio-page.tsx` | Extract existing account risk read/presentation without copying it; both screens use it. Explicit account choice, session/account isolation, abort on deselection, demand loading for expanded analysis, visible retry. Remove the unconditional risk-unavailable claim while retaining truthful gaps for unwired details. No new schema/transport/financial calculations. |
| Lead | Existing Desktop `src/test/app.test.tsx`, ledger/Git/check scheduling | Extend the existing essential account-selection journey if needed to prove the newly reachable report cannot leak the previous account's values. Typecheck and one critical selected check; no Rust build. |
| Sol High contract trace (read-only) | Existing `portfolio_application/{read,product,analytics}.rs`, shared operation contracts, native dashboard transport, canonical instrument lookup and existing tests | Identify the smallest coherent producer-to-consumer change for real holdings/performance/exposure/history from the selected account token. Account picker currently supplies no account ID; existing UI schemas do not match real payloads. Return exact files/dependencies and reusable identity/format owners; no edits or new parallel architecture. |

Lead retains all shared bindings/contracts and application composition. No other file ownership,
Git operations, builds or worktrees are delegated. Native setup awaits the unlocked local session.

Risk UI handoff integrated and ownership released. Portfolio now opens the actual account risk
report on expansion; Risk & Guidance reuses the same query and presentation module. Session changes
clear selection, account changes close the panel, and closing/deselecting releases its observer.
Request signals and zero inactive-cache retention remain intact. Read errors have a direct retry.
Backend quantities, money, measures, cutoffs and recommendation evidence are rendered unchanged.

Critical gap/check: the existing app harness gained one account-switch cancellation journey, since
displaying the prior account's late financial response under the new selection was otherwise
uncovered. It proves no read before expansion, cancellation on account switch, the actual second
account report, rejection of a late first-account response and release on close. Passed (1 selected,
8 skipped, 1.83 seconds), with TypeScript and `git diff --check` passing. No Rust build, broad suite,
CI, resource measurement or live Portfolio acceptance is claimed. Holdings, cash/performance,
exposure, history/attribution and planning remain explicit next integrations, not data absence.
Implementation/checkpoint `957d04d9` is pushed to origin and recorded in PR #43.

Contract trace complete; read-only ownership released. `portfolio_application/analytics.rs` already
calculates current value, exact returns, cash/accounting and exposure. `read.rs` already resolves
opaque account tokens for risk, but other reads and `ListRevisions` require raw IDs. Update existing
operations in place, preserving their supported instrument/time filters: the current product scope
helper rejects `instrumentIds` specifically for risk and must not accidentally narrow those reads.
Do not change the shared `ACCOUNT_ARGUMENT` globally without updating its distinct consumers.

Next coherent outcome is selected-account cash and performance through the real `GetPerformance`
payload, not the unused speculative Desktop shape. Lead freezes its token/input/output bindings
and composition; backend and Desktop can then own disjoint producer/rendering files. Follow with
holdings/exposure/history on the same canonical contracts, preserving snapshot identity and
continuation instead of presenting truncated holdings as complete. The existing
`portfolio_import_atomically_publishes_the_queried_revision` check currently compares missing
`revisionId` fields; extend it to use the actual ListAccounts token, emitted `snapshotToken`, exact
cash/values and registered schema before claiming this boundary verified.

Named historical holdings/transactions need canonical display at their original clocks. Reuse
`ResearchService::market_data_instruments()` / `MarketDataInstrumentReadCapability` indexed exact-ID
population pinning in existing supported batches; no universe scan or inferred ticker. The current
private saved-analysis display helper is current-only/name-only, not a historical resolver.
Attribution remains reported-value change before cash-flow/corporate-action adjustments, not a
return estimate. Full selected-account completion also requires consistent revisions across reads,
history/attribution and explicit scenarios/rebalancing through the existing financial authorities.
These are identified dependencies, not completed behavior or permission for an unbounded rewrite.

### Integrated selected-account cash and performance — 2026-09-29

Source checkpoint `4b5e4f7e`; acceptance 4/5/6, refreshed clean base `3b74186b`. Existing GetPerformance computes cash,
reported value, exact returns and accounting/reconciliation, but its raw account-ID request and
unused Desktop shape prevent the selected account from opening it. Update this V1 operation in
place to the existing opaque account token; retain instrument/time filters and exact output.

| Owner | Exclusive files | Dependency and finish condition |
| --- | --- | --- |
| Lead | `application/contracts.rs`, Desktop `src/lib/transport.ts`, native `contracts.rs`/`service_client.rs`, existing `tests/harnesses/control_plane.rs` and Desktop `src/test/app.test.tsx`, `docs/reference/cli.md`, ledger/Git/check scheduling | Freeze GetPerformance accountToken input with existing portfolio scope filters and unchanged canonical output; align native and CLI documentation. Extend existing import/publication case to real snapshot identity, token selection, exact accounting, output validation and same-root reopen. |
| Sol High performance reader | `apps/market-squawk/src/portfolio_application/read.rs` only | Resolve GetPerformance through existing token resolver; preserve its instrument/time filters while keeping GetRisk's current restriction. No output/calculation changes, builds or Git. |
| Sol High performance Desktop | Desktop `src/features/portfolio/{portfolio-contracts.ts,portfolio-panels.tsx,portfolio-page.tsx,use-portfolio.ts,portfolio-format.ts}`, optional cohesive `account-performance.tsx` | Consume canonical output in `application/contracts/output.rs::portfolio_performance`; replace unused speculative performance/accounting shape and update all its renderers in owned files. Demand-loaded selected account, cancellation/session isolation, retry, exact money/rate formatting and explicit partial/unavailable evidence. Input freezes as `{query: "portfolioPerformance", accountToken}`. No shared transport/backend edits, tests/builds/Git. |

Reader inspection found an adjacent financial correctness defect: a requested period wholly after
retained observations leaves all old history admitted, potentially showing old returns for an empty
period. Astra High owns only `portfolio_application/analytics.rs` to correct that existing history
selection in place; lead owns the corresponding extension of the existing import/publication check.
No additional review round or new harness. Reader handoff is integrated; its ownership is released.

DAG: shared input contract → independent reader and Desktop implementation → lead integration and
existing critical checks → coherent commit/push. No new worktree or branch. Native setup remains
separately pending; fixture/restart checks do not establish live financial completion. Holdings,
exposure and history stay subsequent required checkpoints, not silently waived capabilities.

Reader, Desktop and financial-period handoffs are inspected and integrated; agent ownership is
released. GetPerformance uses the same canonical account-token resolver as risk, preserving its
existing instrument/time filters. Native and TypeScript requests agree; CLI/MCP retain the shared
operation. Desktop now renders actual cash, exact returns, accounting, reconciliation and original
cutoffs on expansion, with cancellation/session isolation and retry. Missing realized gain and
partial income remain explicit. Existing money formatting is reused; percentage display shifts
exact decimal text without floating-point arithmetic. No new financial computation lives in React.
The history fix preserves the preceding opening observation only when an in-range observation exists.

The valid two-revision check passed updated accounting and period assertions, then exposed a
same-root reopening failure: restoration reimported old publications against the latest raw
adapter head, which correctly rejects reactivating superseded records. Astra High implemented
`PortfolioExtractionSource::restore_published_batch` in adapter `src/archive.rs`; lead integrated
both ordinary and governed restoration in `portfolio_application/import.rs`. Ownership is released.
Exact previously admitted records are normalized and reconciled with their historical account
bindings without changing the active head. Fresh or partially rebuilt archives use unchanged live
import transitions. Raw-only failed records are not treated as admitted. Both paths reuse the same
revision builders and financial calculations; no cursor, new store, migration or compatibility path.

Verification: Desktop TypeScript and the existing selected-account journey passed, including
large exact cash, fractional return display, partial/unavailable accounting and stale-response
cancellation. Native Desktop compilation passed before the recovery change. The existing Rust
publication case now covers actual snapshot identities, two valid superseding imports, token
lookup, canonical output validation, accounting, empty-period rejection, a retained opening
observation and same-root reopening. Its recovery rerun passed (one selected case, 5.75 seconds; one-job compilation).
The existing adapter correction case also checks historical reconstruction without head rollback,
raw-only rejection and interrupted fresh-archive rebuilding; it passed (one selected case, 0.30 seconds). These extensions cover the discovered
persistence failure and its live-import authority boundary; no new harness or broad suite.
`git diff --check` passed. This checkpoint is implemented and critically verified; all agent
ownership is released. No CI, resource measurement, live or installed completion is claimed.
Next dependency: selected holdings/exposure/history identity and continuation, alongside pending
native secure setup and actual stock/model/input admission.

### Selected-account positions checkpoint — 2026-09-29

Acceptance 4/5/6; clean refreshed base `50b79f6b`. GetHoldings already owns exact financial
values but takes raw account IDs, materializes every result and truncates without continuation;
Desktop's unused speculative schema leaves the positions table disconnected. Complete this
existing operation in place, with account-token selection, immutable-snapshot cursor pages,
canonical historical investment display and cancellable demand loading. No new endpoint or
financial calculation. Exposure/history remain required subsequent producer-to-consumer slices.

| Owner | Exclusive writable files | Dependency / finish condition |
| --- | --- | --- |
| Lead | `portfolio_application.rs`, `portfolio_application/read.rs`, `local_product/mod.rs`, shared input/output contracts, CLI/native/TypeScript transport, existing control-plane and Desktop app checks, ledger/docs/Git | Freeze holdings page contract, register existing catalog read authority, schedule blocking catalog work through existing lifecycle, update all consumers and prove cursor/restart/cancellation before commit/push. |
| Sol High holdings page | New cohesive `portfolio_application/holdings.rs` only | Implement `call(image, request, context, limits, instruments: Option<&MarketDataInstrumentReadCapability>)`; reuse ReadScope token resolver and financial row helpers. Return only a bounded page plus continuation pinned to the original revision and query scope; never materialize all JSON rows. |
| Sol High investment display | New `portfolio_application/instrument_display.rs` only | Resolve page IDs through `pin_population_as_of` at retained portfolio knowledge/effective clocks, batching within existing catalog limits and respecting cancellation/deadline. Return actual nullable name/symbol; no ticker inference, current-name substitution or fabricated identity. |
| Sol High positions Desktop | `features/portfolio/{portfolio-contracts.ts,holding-table.tsx,use-portfolio.ts,portfolio-page.tsx,portfolio-panels.tsx}`, new cohesive `account-holdings.tsx` if needed | Replace only unused holding shape with actual frozen row/page contract. Demand-load selected account, reuse cursor navigation, cancel/release pages on close/account change, exact numbers and truthful reported-price/basis/missing-name display. Preserve risk/performance. No builds/tests/Git. |

Lead shared scope is `application/contracts.rs`, `application/contracts/output.rs`,
`cli.rs`, `local_product/cli_transport.rs`, `release/demonstrate/local.rs`, Desktop
`src/lib/transport.ts`, `features/shared/cursor-navigation.tsx` and native
`contracts.rs`/`service_client.rs`; existing checks are
`tests/harnesses/control_plane.rs` and Desktop `src/test/app.test.tsx`. The release demonstration
consumer is updated in place to actual saved-import output and listed account tokens; it is not run
as a per-task release gate.

DAG: frozen input/output → independent page/display/UI → lead shared integration and critical
existing checks → commit/push and ownership release. All work stays in the primary worktree;
lead alone owns compilation. The page response carries its own cursor so Previous also restores
the first page of the original snapshot; only explicit refresh/restart selects the latest one.
Provider/live stock admission remains waiting on native secure setup;
no installed or live evidence is claimed for this slice.

Implemented and critically verified in `69a64fbc`; all three Sol High handoffs were inspected,
integrated and released. The lead corrected first-page navigation to preserve its original snapshot,
updated all current callers in place, and retained the shared catalog's original knowledge/effective
clocks for nullable investment names. No compatibility reader, migration, new endpoint or financial
calculation was added. The response builds only the requested page and supplies continuation rather
than silently truncating a whole-account JSON result.

Critical verification (local, one compiler job; no broad CI or release gate):

- Existing `portfolio_import_atomically_publishes_the_queried_revision` passes (1 selected test).
  It now checks admitted holdings output, exact quantities, scope-bound cursors, first/next pages
  across a newer import, and identical first/next pages after service reopen. Existing performance
  and accounting/recovery assertions remain. The additional fixture's fractional quantity initially
  had an invalid whole-share lot size; the fixture was corrected, not the importer validation.
- Existing Desktop `loads selected portfolio reports on demand and discards a cancelled account
  response` passes (1 selected test). It now covers positions expansion, cancelled account reads,
  exact money/fractional quantity, unknown names/basis, Next/Previous pinning, explicit refresh and
  close. No new component suite or harness.
- Desktop TypeScript validation and `cargo check --locked --offline -p market-squawk-desktop --lib`
  pass. The Rust check/test use `CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0`. Existing compiler warnings
  remain; `git diff --check` passes.

Local logs: `.agents/tmp/v1-first-stock/portfolio-holdings-{check,typecheck,ui,native-check}.log`.
These prove the changed deterministic contracts and wiring, not live catalog-name resolution or an
installed portfolio journey. Native secure setup and real stock/model input admission remain the
live dependency. Exposure, transactions/history/attribution, scenario/planning and paper composition
remain required; whole-app RAM and final owner-test gates remain deferred until complete workflows.
Only the lead owns the next integration assignment; no implementation agents remain active for this
checkpoint, and no additional branch/worktree was created. PR #43 records the pushed identity.

### Selected-account exposure checkpoint — 2026-09-29

Source checkpoint `5bddacf1bbb2cd035924309586c23b833d8d1cf9` is pushed to origin;
PR #43 carries its delivery evidence. All implementation ownership for this slice is released.

Acceptance 4/5/6; refreshed clean base `09dac494`. Exposure exists but the selected-account
Desktop is disconnected. Its read materializes allocations and every instrument row; preserve
complete coverage by reusing the admitted positions page and adding whole-snapshot totals.
History/attribution remain required subsequent work, not waived by this slice.

| Owner | Exclusive files | Dependency / finish condition |
| --- | --- | --- |
| Astra High exposure aggregation | `portfolio_application/analytics.rs`, `crates/market-squawk-analytics/src/scenarios.rs` and its existing scenario tests if a critical arithmetic gap demands extension | Replace exposure with `exposure_summary(revision, scope, context) -> Result<Value, PortfolioApplicationServiceError>`. Reuse exact financial authority with streaming aggregation; no per-position JSON/allocations vector, artificial count cap, lost signs, invented classifications or frontend math. Preserve performance/risk. |
| Sol High exposure Desktop | `features/portfolio/{portfolio-contracts.ts,use-portfolio.ts,portfolio-panels.tsx,portfolio-page.tsx,account-holdings.tsx}`, consolidated `account-positions.tsx` replacing duplicated position/exposure renderers | Render frozen exposure page below on explicit expansion; share existing position table/paging, exact formatting, cancellation/session/account lifecycle. No builds/tests/Git. |
| Lead | `portfolio_application/{holdings.rs,read.rs}`, `portfolio_application.rs`, shared application contracts/output, CLI/native/TS transports, release demonstration, existing control-plane/Desktop app test, docs/ledger/Git | Reuse same immutable page selection for exposure and holdings, fit summary and rows together within response budget, update all callers, verify whole-snapshot totals with one-row pages and restart. |

Frozen exposure output is the existing holdings page (`holdings`, `pageCursor`, `nextCursor`,
`snapshotToken`, original effective/available clocks), plus `exposure`: nullable money `net`/`gross`,
`positionCount`, `currency` rows `{currency, amount}`, `sector`/`factor` rows
`{classification, amount}`, `calculationStatus` (`available`/`no_positions`) and
`classificationStatus` (`not_supplied_by_portfolio_source`). Net/gross describe positions;
currency totals preserve existing cash/receivable inclusion. Missing classification stays explicit.
Input is the same accountToken/cursor/limit/instrument/time/result scope as holdings. No new endpoint.

DAG: frozen contract → independent financial/Desktop lanes → lead shared page/transport/checks
→ coherent commit/push and ownership release. Only lead schedules single-job builds. Existing
publication/restart and selected-portfolio Desktop checks cover totals independent of page length,
original snapshot persistence, exact signs/amounts and demand/cancellation; no new harness.
Native live admission is still waiting on owner secure setup (macOS lock rechecked as Yes).

Financial and Desktop handoffs are inspected and integrated; both owners are released. The
shared exact exposure accumulator avoids retaining a second allocations vector or whole-account
instrument JSON. Holdings and exposure share immutable page selection, historical display lookup,
response budgeting, cancellation and one Desktop component/query lifecycle. Exposure totals cover
the full selected snapshot regardless of page size; signed position totals stay distinct from
cash-inclusive currency totals. No migration, compatibility reader, duplicate financial formula,
new endpoint or imposed position-count admission limit was added.

Critical checks extend existing cases only, covering otherwise unverified exact incremental
arithmetic and newly reachable exposure paging/account isolation:

- Analytics `feature_contracts::golden::portfolio_attribution_and_composed_scenarios_remain_exact`
  passed (1 selected): signed net/gross, currency/basis mismatch, overflow and precision loss.
- Application `control_plane::portfolio_application::portfolio_import_atomically_publishes_the_queried_revision`
  passed (1 selected, 11.43 seconds after single-job compilation): registered exposure output,
  full totals on one-row pages, cash-only scope, continuation after a newer import and identical
  saved output after service reopen. Existing holdings/performance/recovery assertions remain.
- Desktop TypeScript and the existing selected-portfolio app journey passed (1 selected UI test):
  demand loading, cancelled account reads, signed exact amounts, stable totals across Next/Previous
  and close. No new component suite or harness.
- Native `cargo check --locked --offline -p market-squawk-desktop --lib` and `git diff --check`
  passed. Both Rust commands used `CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0`; existing unrelated
  compiler warnings remain. This slice is implemented and critically verified.

Local logs are `.agents/tmp/v1-first-stock/portfolio-exposure-{math,check,typecheck,ui,native-check}.log`.
These are deterministic integration checks, not live/installed Portfolio acceptance or final
approval. Native secure setup and actual stock/model input admission remain the live dependency;
transactions/history/attribution, scenarios/planning and paper composition remain required work.
No additional branch/worktree, ordinary-task CI, release gate or whole-app RAM measurement.

### Integrated saved portfolio comparison — 2026-09-29

Acceptance 4/5/6, reviewed Wave 2; clean refreshed base `30964544`. At that base, history was a static
Desktop placeholder; ListRevisions accepted raw IDs and materialized all rows, and attribution
omitted newly added holdings and derived change through a rounded division. This checkpoint delivers
explicit saved-version selection and exact paged value changes through the existing operations. Transactions
remain a subsequent required integration. Native setup remains externally pending (macOS locked).

| Owner | Exclusive files | Dependency / finish condition |
| --- | --- | --- |
| Astra High saved comparison | New `portfolio_application/history.rs`, existing `portfolio_application/advanced.rs` | Implement saved revision pages and snapshot-pinned exact union-of-holdings comparisons; remove superseded attribution implementation. Stream exact closing minus opening, include opened/closed/short/zero positions, share canonical money/display and request checks. No per-account output vector, invented return, builds or Git. |
| Sol High history Desktop | `features/portfolio/{portfolio-history.tsx,portfolio-contracts.ts,use-portfolio.ts,portfolio-page.tsx}` | Explicit earlier-version choice, demand-loaded snapshot list and paged comparison; exact amounts, original dates, cancellation/session/account isolation and retry. Replace speculative schemas, reuse navigation/formatting. No tests/builds/Git. |
| Lead | Shared operation/output contracts, `portfolio_application.rs`, `portfolio_application/read.rs`, CLI/native/TS transports, existing control-plane/app checks and docs | Freeze contracts below; remove old list dispatch, integrate both clients and CLI commands, validate same-snapshot restart and financial comparison. Sole Git/build owner. |

DAG: frozen contracts → independent backend/Desktop → integrated critical checks → commit/push.
ListRevisions uses accountToken/cursor/limit plus existing scope; output is `{revisions:[existing
snapshot summary],pageCursor,nextCursor,selectedSnapshotToken}`. The selected snapshot anchors the
listing across newer imports. GetAttribution uses accountToken, selectedSnapshotToken,
baselineSnapshotToken, cursor/limit and existing scope. Output has contributions (instrumentId,
nullable name/symbol investment, opening/closing/amount money), whole-comparison total, pageCursor,
nextCursor, snapshotToken, baselineSnapshotToken, both original effective/available clocks and
explanation. Change is reported market value before cash-flow/corporate-action adjustments, not
performance. Page defaults 25/max 100; continuation covers all rows instead of rejecting larger
portfolios. No new endpoint or compatibility path. Existing publication/restart and selected-account
journey are the critical checks; no broad gate, new review round or RAM measurement.

Both agents completed their assigned files; the lead inspected the changes and affected consumers.
Their ownership is released. The old history reader and attribution calculation are removed in
place. Desktop opens history only on demand, requires an explicit earlier-version choice, retains
the selected observations through pagination/retry, and cancels reads on close/account change.
CLI now exposes `portfolio revisions` and `portfolio attribution`; MCP uses the same canonical
operation contracts. A constant-auxiliary-memory merge includes opened, closed, short and zero-value
holdings with exact monetary subtraction and full totals independent of the displayed page.

Critical verification passed: the new single financial regression covers the previously uncovered
union-of-holdings arithmetic and overflow; the existing publication/restart case covers canonical
output admission, paged comparisons, invalid selection/scope rejection, and identical saved reads
after restart plus another import. The existing selected-account Desktop case covers demand loading,
cancelled-account isolation, explicit comparison choice and pagination. TypeScript and the native
Desktop library check pass; `git diff --check` passes. Logs are under
`.agents/tmp/v1-first-stock/portfolio-history-*.log`; no live or installed completion is claimed.
Next dependencies remain transaction history, real stress/planning choices and the native setup
needed for live stock inputs. This checkpoint does not waive them.
Source checkpoint `56415f547ab631f651bbc16804c53193ea60cbcd` is pushed to origin and recorded in
[PR #43 delivery evidence](https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5889238461).
The primary worktree remains the only worktree; no implementation ownership remains active for this slice.

### Integrated transaction history — 2026-09-29

Acceptance 4/5/6, reviewed Wave 2; refreshed clean base `4bfc16ea`. The transaction reader
materialized all rows and truncated without continuation; Desktop exposed no transaction read.
This checkpoint delivers recorded transaction history through the existing operation and all clients, using
opaque account selection, exact values, original dates and snapshot-pinned continuation.

| Owner | Exclusive files | Finish condition |
| --- | --- | --- |
| Sol High backend transactions | `portfolio_application/holdings.rs`, new `transactions.rs` and `snapshot_page.rs`; `read.rs` only transaction removal/helper visibility | Reuse snapshot pagination admission/pinning/byte fitting across positions and transactions without copying it; bounded transaction pages and historical investment display. Preserve existing holdings/exposure behavior. No Git/build/test execution. |
| Sol High Desktop transactions | New `features/portfolio/account-transactions.tsx`; `portfolio-history.tsx`, `portfolio-contracts.ts`, `use-portfolio.ts` | Replace the transaction placeholder with an independently demand-loaded, cancellable selected-account transaction page, exact formatting, pinned refresh/retry and cursor navigation. No automatic comparison or financial calculations. No Git/build/test execution. |
| Lead | Shared operation/output contracts, `portfolio_application.rs`, CLI/native/TS transport, existing control-plane/app checks, docs | Freeze shared shapes, integrate both handoffs, schedule thin checks then commit/push. |

DAG: frozen shared contract → disjoint backend/Desktop → integrated critical checks → pushed
checkpoint. `Portfolio.GetTransactions` takes accountToken/cursor/limit and existing scope. It returns
`{transactions,pageCursor,nextCursor,snapshotToken,effectiveAtUnixNanos,availableAtUnixNanos}`.
Rows preserve transactionToken, accountId, snapshotToken, nullable instrumentId, category,
amount, nullable quantity, occurredAtUnixNanos and nullable lotMethod; add nullable investment with
nullable name/symbol (null for activity without an instrument). Exact values remain strings.
Page defaults 25/max 100; cursor at most 512 characters; all rows remain reachable. Only returned
rows need display lookup. Metadata counts transaction rows. Reuse current transaction categories
and deterministic stored ordering; preserve original observation and availability semantics.
Extend the existing publication/restart and selected-account UI checks only for this uncovered
continuation/recovery seam. No extra review, full CI or RAM measurement. Native setup remains
pending an unlocked macOS session, independently of this slice.
Lead also owns the affected `release/demonstrate/local.rs` caller: update its transaction command
and payload access in place without running the final release demonstration during this checkpoint.
During serialized verification, an Astra High read-only trace inspected existing stress/planning
authorities and their Desktop callers. It owned no writable files, tests or Git; the next
implementation remains behind this checkpoint's integration barrier.

Both implementation agents finished and released their files; the lead inspected their actual changes
and affected consumers. Shared `snapshot_page.rs` now owns account/scope admission, immutable pinning,
result metadata and byte fitting for holdings/exposure and transactions. Transaction reads seek within
the saved ordered observation, retain only the requested page plus lookahead, and resolve display only
for that page. The old materialization/truncation path and unused helper are removed. Desktop uses
an independent Transaction history expansion and shared pinned-read behavior with saved-version history.
The CLI and release-demonstration caller consume the same revised contract; MCP uses shared descriptors.

Existing selected-account Desktop check and TypeScript passed. The existing portfolio publication/restart
check passed with exact fee/income values at equal timestamps, continuation/scope validation, and identical
transaction pages after restart plus a later import; its holdings/exposure/comparison assertions also
passed. Native Desktop library compilation and `git diff --check` passed. Local evidence is retained under
`.agents/tmp/v1-first-stock/portfolio-transactions-*.log`. No extra test target, review round, CI gate,
live/installed completion or resource measurement is claimed.
Source checkpoint `bcce0161692e50d7802d98bfeed03cc3cafa6406` is pushed to origin and recorded in
[PR #43 delivery evidence](https://github.com/Sawmonabo/market-squawk/pull/43#issuecomment-5889453208).

The read-only financial trace is complete and owns no files. Next sequence is explicit snapshot-bound
stress assumptions, corrected rebalance proposals, then position impact, preserving all three required
workflows. Existing `advanced/scenario.rs` and `advanced/planning.rs` already calculate results; the
frontend prepared-choice shapes have no producer and must be replaced in place. Candidate impact uses
the durably configured recommendation account and must explicitly match the selected account before
being exposed there. These calls do not currently persist saved planning results; recomputation must
not be called saved-result retrieval. Refresh these findings against the pushed transaction checkpoint
before assigning implementation:

- `advanced/planning.rs` and `crates/market-squawk-portfolio/src/rebalance.rs` calculate buy capacity
  using unscaled sales, then scale both buys and sales. With cash 100, holdings 200/100, targets .25/.75,
  minimum cash 50 and unrestricted turnover, scale .75 fails at cash 25 although .5 is feasible. Correct
  the shared financial behavior and existing critical arithmetic case when wiring rebalance.
- `crates/market-squawk-analytics/src/scenarios.rs` validates each shock at least -1 but does not enforce
  that floor after additive composition. The new stress workflow must resolve the combined price-shock
  semantics rather than presenting a loss beyond the supported price basis. Reuse its existing exact
  scenario test; no broad matrix.

## Resource processing checkpoint — 2026-09-29

The owner resumed work after the mockup pause and approved
[`resource-processing-remediation.md`](resource-processing-remediation.md): implement in the
current `feature/v1-installed-product-experience` worktree, run critical verification, commit and
push, then pause. The audit base is `913bb0127fed3866b6411de6225c2467421d169c` plus preserved
financial WIP. This entry supersedes the older active-lane and parser-blocker descriptions below;
those entries remain historical evidence. The resulting checkpoint is
`9543ed357349a83715079ba0720b0f4789f9da58`, pushed to origin and recorded in PR #43. This resource batch is implemented; full product acceptance remains open.

The integrated batch uses streamed query/storage output, indexed filing and point-in-time
processing, disk-backed backtest history/results, paged model/forecast inventories, selected-model
activation and Desktop demand/viewport reads. Required SEC/common-share financial consumers are
included with their producer changes. Active formats change in place without compatibility paths.

Critical evidence is listed in the resource plan: million-row disk spill, query cancellation,
PIT publication, real Microsoft indexed parsing plus separate physical filing restart, financial
replay/backtest artifacts, model inventory/native inference/retirement, archive/chart integrity,
live accounting, read cancellation, Desktop types/journey and native compilation. Tiingo history custody, publication,
restart and corrupted-index rejection also pass. These checks are not release approval.
The full installed financial workflow, whole-app measurement and unchanged final release gate
remain open. The owner deferred RAM measurement until complete application workflows are ready.
No new branches or worktrees are created; old sessions and recovery backups remain preserved.

## Historical screen-mock and resource correction — 2026-09-28

The owner requests a complete Desktop screen mockup set, followed immediately by pausing the
V1 goal and all agents. Mockups live only in the main worktree at `.agents/tmp/screen-mocks/`.
They cover current Everyday, Advanced, Connections & System routes and important detail flows;
illustrative data and designed states are explicitly distinct from implemented/live evidence.

The memory objective is corrected to **500 MB–1.5 GB, with 2 GB maximum for the whole app**.
Earlier 500 MB maximum references below are historical and superseded. The objective must not
be enforced by rejecting required data or hiding functionality. A bounded read-only restriction
audit is retained with the mockups; the repository-wide audit remains incomplete. Whole-app
measurements and full-workflow verification remain open.

Screen mock delivery: **34 rendered views**, including all 17 navigation screens and key detail
flows, are saved in `.agents/tmp/screen-mocks/index.html`; the screenshot gallery is
`.agents/tmp/screen-mocks/screens/index.html`. Local README contains the full inventory and
preview instructions. Browser receipts record navigation/rendering and chart interaction checks.
These artifacts use illustrative data and do not close implementation or live acceptance gaps.
All mock agents are finished. Work pauses here as requested; financial integration WIP and the
original session/recovery backup remain preserved. Resume requires owner direction. The next
implementation dependency remains genuine SEC filing normalization and complete financial
workflow/restart proof, alongside completing the resource-restriction audit.

## Historical integration and cleanup barrier — 2026-09-29

The active target is `feature/v1-installed-product-experience`. This combined
source checkpoint is committed and pushed at `ff16c370c90d930979e3b69b4f641f19ede43302`;
PR #43 records its focused verification and remaining acceptance gaps. Dependency checkpoint `a7987440`
incorporates futures-util 0.3.34, async-trait 0.1.92, rust_decimal 1.43.0,
clap 4.6.7 and uuid 1.26.1. The five corresponding Dependabot PRs are closed
and their origin branches are absent. Main and release remain untouched.

Fresh inventory is **3 local branches, 3 origin heads and one primary
worktree: the V1 target; zero linked worktrees**. The only remaining branches
are V1, main and release. All stale Codex and provider branches are retired.
Alpaca and Kraken worktrees were retired after exact
source and independent-backup comparison. Census and Schwab checkouts were also removed
after independently verifying all 26 archived changed files, exact Git state,
bundled histories and Schwab ignored proof files. Finally, the common-seal
checkout was removed normally after independently verifying all 255 working
states and reconstructing all 14 staged versions from the untouched recovery
backup. After checkpoint `ff16c370`, the final 15 local labels and 11 origin
heads were removed following semantic disposition and independent verification
of every exact tip in the recovery bundle. Remote deletions were atomic and
used exact expected-head leases. Main and release heads remained unchanged.
The original session and independent recovery backups remain intact.

The source API, native identity callers, current-share generation, chart ranges
and retained recovery changes are committed together in this coherent source
checkpoint. This is not accepted complete V1 delivery.
The application library check passed before the final backup artifact change;
the final backup decoding fix also passed the application library check.
A subsequent startup correction isolates unavailable retained sources to the
decision domain. Its focused regression passed with a real durable journal:
partial/empty results, mutations and backups remain unavailable; writer custody
is retained, and explicit reopen restores the original saved record. Desktop
type-checking passed with the pinned Node and pnpm versions.
Three existing capture/routing cases and seven Census library cases passed. These focused results do not
prove installed generation, backup/restore, restart or release acceptance.

Historical dependency and ownership wave (closed by the later integrated checkpoint):

| Lane | Owner and scope | Next dependency |
| --- | --- | --- |
| SEC filing context | Sol High; canonical filing context preservation and verified typed reads | Expose complete source facts to valuation without duplicating context graphs |
| Financial valuation and recovery | Astra High; per-share DCF, residual-income and comparables evidence, persisted replay | Consume verified filing context; prove genuine generated-decision restart |
| Financial action acquisition | Astra High; existing action preflight, historical/current source join and original history retention | Candidate integrated locally; verify one completed history plus two final-session action requests per instrument |
| Workflow request integration | Sol High; investment request, shared operation contracts and workflow driver | Required original financial cutoff and canonical filing/action references; completed locally, awaiting combined verification |
| Integration | Lead; market preparation service/output contracts, source registry composition, manifests, Git, documentation and serialized checks | Acquire original source intervals before one final quote selection; verify financial consumers and durable filing restart |

Editor resource correction `913bb012` is pushed: manifest-triggered and build-script-on-save
rust-analyzer Cargo work is disabled; explicit project refresh remains available.
The running editor used the parent `~/dev` workspace instead of the nested repository settings.
After stopping its unplanned Cargo check, the parent settings were preserved and corrected;
the extension log confirms check-on-save, automatic reload and rebuild-on-save are disabled,
with one compiler job and no incremental output. No editor Cargo process remained afterward.

The next financial checkpoint is still uncommitted. Its integrated application library check
passed with one compiler job. The focused SEC parser/context roundtrip also passed. The source
reader, share projection, saved-analysis reconstruction and backup artifact traversal are wired
locally; these checks do not prove a generated result survives restart. The remaining focused
barrier is genuine financial classification and generated-result recovery. The existing adapter
test now passes normalization, durable filing publication, typed reads and restart. Full filing
memory admission must be measured against a
representative filing before claiming that live analysis works. Preparation now drops each
complete peer filing after retaining its compact financial evidence instead of retaining up to
seventeen filings during network acquisition. The action join uses the original calendar's
civil-date bounds for closing, after-hours and weekend quote cutoffs. The per-share arithmetic
regression passed. Cold analysis now prepares source intervals before selecting its final prices;
the shared workflow retains the original financial cutoff separately from that fresh market
cutoff. A combined check exposed three historical-valuation callers of the extracted source
selector; their signatures are corrected and the follow-up application check passed. The physical
filing proof exposed missing taxonomy-publisher catalog registration; shared metadata now feeds
the passing fixture and SEC activation. The activation change still needs the integrated check.
The retained Microsoft annual filing exposed a separate real parser defect: nested continuation
sections were rejected. Its repair and representative memory measurement remain open. The
passing synthetic source fixture does not establish live filing availability.
The same fixture now also passes genuine company-identity selection, reported-common-share
classification and identical financial evidence after restart. The authentic filing now reaches
footnote relationships, exposing a second parser gap that is being repaired without discarding
the footnote text. A measured 17-instrument carrier exceeded the former 64-KiB request limit:
66,576 carrier bytes and 91,122 full-request bytes. The integrated candidate uses a 128-KiB
carrier and 256-KiB generation request, with matching workflow, transport and recovery bounds;
these changes still require the combined check. Source-shaped sizing is not live generation proof.

No new branch or worktree is needed. Only one Cargo command runs at a time,
with one compiler job. Automatic CI is limited to release-branch pushes; the manual frozen-candidate
release gate remains available. No full CI/CD or release gate ran. Retained issuer snapshots are stored byte-for-byte in Git, with their staged
hashes checked against the source catalog. The final unchanged
candidate still requires complete provider and Desktop/CLI/MCP workflows,
installed live shutdown/restart, owner-test packages, whole-application RAM
measurement against the corrected 500 MB–1.5 GB range / 2 GB maximum, and updated PR #43 delivery evidence. Financial
model per-share support and genuine generated-decision recovery remain open;
compilation does not substitute for either.

## 2026-09-16 dependency PR retirement

PRs #47 (clap), #50 (uuid), and #51 (crc32fast) were closed without merging after
current-use and upstream-change review. Their exact remote heads were respectively
`a1332cdde7953e2ff0ac28ab03b337682cca7345`,
`ab843460c23e4744a52249d43780aca896ec6f37`, and
`9703acbbf10b8a0b04bc8ee5ae5130708ece1d26`. Each remote branch was deleted with an
explicit expected-head lease, then verified absent. No local branch, worktree, application data,
old session, or recovery backup was removed by this action.

These optional upgrades were declined, not labeled integrated or superseded: the CLI does not
use the affected help/new API, UUID's changed diagnostic does not change invalid-input rejection,
and no current product measurement warrants the CRC optimization. Relevant upstream evidence:
[clap 4.6.5](https://github.com/clap-rs/clap/releases/tag/v4.6.5),
[clap 4.6.6](https://github.com/clap-rs/clap/releases/tag/v4.6.6),
[UUID issue 898](https://github.com/uuid-rs/uuid/issues/898), and
[CRC comparison](https://github.com/srijs/rust-crc32fast/compare/v1.5.0...v1.5.1).
PRs #46 and #48 retain their relevant fix-review work; #43 and #26 remain delivery/release PRs.
Four PRs remain open. Broader branch/worktree reconciliation remains unfinished.

Exact pre-action PR metadata and diffs, retirement reasons, and post-action verification are
preserved in `.agents/tmp/resume-2026-09-15/support/root-live/branch-cleanup-current/optional-retirement/`.
This checkpoint records repository housekeeping only, not source integration or product acceptance.

Last updated: 2026-09-29

This is the compact operational handoff required by
[`project-memory.md`](../project-memory.md). It records integrated work and exact verification
evidence; it does not replace the README capability truth or the canonical release plan.

## Historical integration barrier — 2026-09-16

The active checkout is `feature/v1-installed-product-experience` at pushed commit
`5024a5dcd390401a51f162aa2a71cf360b4bf2d0`, with substantial uncommitted integration work.
This checkpoint isolates the query-future compiler fix; PR #43 comment 5706768630 records its scope.
CI was skipped for this owner-authorized checkpoint. The earlier combined-check audit base was
`176dc400ffd8d3c2fef1c4a32726e7a536d93f52`.
The current task, candidate, dependency and retirement queue is
`.agents/tmp/resume-2026-09-15/support/active-integration-queue-medium/queue.json`.
Root owns acceptance and shared-file/Git integration; the medium-effort coordinator tracks all
the current workers and their retained candidates. A frozen handoff is not accepted product delivery.

The current integration now includes saved-forecast measurement/preparation through the installed
model, CLI and shared tool contracts; exact source error propagation; dataset/Find custody and
recovery; fiscal hindcast readers; and H.15-backed premium/valuation library consumers. Desktop
history now retains genuine nominal dates or timestamped periods and rejects mixed precision;
its recorded-outcome schema includes the actual recording time. These are uncommitted source
integrations, not complete default/Advanced journeys or accepted financial behavior.

The earlier combined application check, session 73101, passed with exit 0 on 1,887 unchanged
Rust/manifest/lock inputs. Its predecessor finished with one harmonic bar-count type error;
explicitly typing the slice count as `usize` resolved it. This also establishes that the previous
async trait recursion overflow no longer blocks the combined application. Exact working-source
evidence is in `.agents/tmp/resume-2026-09-15/support/root-live/financial-parent-integration-r4/verification.json`.
This is compilation evidence, not financial correctness, clean checkpoint, or release approval.
The existing bounded-query, payable-entitlement/immutable-ledger replay, exact split arithmetic,
and 365-day recommendation outcome cases each passed (four focused cases total) on 1,185 unchanged
crate inputs. Evidence: `root-live/ledger-accounting-integration-r1/critical-gates/verification.json`
under the support directory above. No new test target or full CI gate was introduced.

The subsequent Schwab foundation55, original-calendar portfolio leaves and generation-authority
joins exposed real integration errors: the first combined check stopped at an unchecked SQLite
integer parameter; the second reached the application and reported eight private-facade, missing
API and exhaustive-error mapping errors on 1,778 unchanged inputs. Narrow fixes are applied;
the next application check reached the newer Find/generation joins and failed with three errors on
1,782 unchanged inputs. Its digest accessor is corrected; the two missing current-valuation APIs
are assigned to the existing High financial owner. Evidence: `root-live/schwab-foundation-integration-r1/`
(`combined-check`, `combined-check-r2`, `sql-bound-fix`, `compile-followon`). Neither failed check
supersedes the scope of the earlier successful snapshot with a success claim.

Three further existing accounting cases passed on 1,211 unchanged crate/adapter inputs:
opposing merger lots, incomplete-basis disposition, and return-of-capital settlement separation.
Evidence: `root-live/ledger-accounting-integration-r1/critical-followon/adjacent-consumers/gates/verification.json`.
Investment generation now has actual installed construction and dispatch, with the shared exact
completed-forecast reader. Strict descriptor/schema registration and current Find service dispatch
are integrated in working source. Native requests now include the existing bounded result limits
before retaining their recovery hash; the complete workflow driver is still pending. Native
orchestration and complete historical fiscal sequencing remain open. This is source integration,
not a callable end-to-end or live installed product claim.

Actual unpaid-entitlement ledger/shadow accounting, harmonic-history production and historical
recommendation custody compiled together at the earlier check. Current disjoint ownership is:
High retained registry/startup/OAuth cleanup (five Wave B leaves plus narrow four-child follow-ons),
High current valuation integration (six paths), and High complete fiscal paging (four paths).
Medium native request-limit integration has released its two live files and retained the driver-only
successor separately. A Medium lane reconciles the clean BEA worktree against current requirements;
that is read-only evidence, not permission to import obsolete code. Root owns remaining application
parents, manifests, Git and Cargo; the Medium coordinator retains candidate/cleanup custody. Original-calendar
portfolio reads and Tiingo acquisition coverage are integrated; their complete installed journey
remains unverified. Controlled materialization persistence,
installed default/Advanced workflows, live source restart and whole-app memory remain open.

Current Find screening now retains complete population counts, real calendar/profile inputs and
exact estimated-return ordering. The historical backtest owner now has the genuine async financial
issuer in place of its former placeholder; original history/action/calendar references are stored
for physical replay. Recommendation core and codecs retain authenticated requests, both valuation
and harmonic audits, and outcome/sizing projections in one publication. These additions still need
installed service production, relevant behavioral verification and clean checkpoint acceptance.
Desktop saved-screen contracts now pass original calendar/profile references through the native
bridge; TypeScript checking passed after the changes. Actual Advanced/default UI journeys and
native Rust verification remain open. No numeric journal-version bump, migration, alternate decoder
or application-data reset was performed; the sole existing format was updated in place.

A real original-history blocker was corrected without changing HTTP request bounds: the inclusive
request end now agrees with the exclusive completion boundary in calendar, publication, replay
and existing V1 schema validation. Genuine Split receipts are separately retained and cannot
substitute for Raw outcome history. The existing publication/restart case was extended only for
these uncovered critical paths. Its initial run stopped before publication with
`MigrationRegistryMismatch`; root synchronized the changed existing schema checksum, verified all
22 registry entries, and reran the same focused case: the retained log confirms 1 passed, 0 failed.
The original process exit status was consumed before recovery; it is not invented. Evidence is in
`.agents/tmp/resume-2026-09-15/support/root-live/history-anchor-integration-r1/`.
Desktop TypeScript checking passed with exit 0. Both results are focused working-source evidence;
no full CI or new test target was introduced.

Coinbase continuation, retained shutdown worker ownership, and removal of two unused startup
paths are now applied across 11 unchanged paths. The existing scripted adapter continuation test
passed 1/1, including depth updates, deletion, quotes and stale-generation rejection. The earlier composed
application check failed with the same 71 error headers/primary files and 62 warnings and added
no Coinbase diagnostics; the later combined application check above now passes. The active startup and dynamic paper hooks remain; publication
registration and runtime now use the same order-level metadata builder. Evidence is in
`.agents/tmp/resume-2026-09-15/support/root-live/coinbase-continuation-integration-r1/verification.json`.
Physical publication, shutdown failure/timeout behavior and installed restart remain unverified.
These source changes are uncommitted and unaccepted. No whole-app, RAM or release acceptance is
claimed. No full CI or new branch/worktree was started for this integration wave.

The existing Schwab scripted capture case passed on 1,211 unchanged crate/adapter inputs. It
checks sealed frames and credential exclusion; it is not a new live provider pull or installed
restart. Evidence: `root-live/schwab-foundation-integration-r1/critical-adapter/verification.json`.
The actual StudyInputs endpoint now retains the genuine final close while evaluation remains
half-open; original folds and target censoring are unchanged. Fiscal continuation will retain every
eligible source origin using bounded pages, not reduce the population to fit an envelope. These
latest joins await composed verification; no completion or financial approval is inferred.

Historical cleanup remains incomplete. The H.15 verification and explicitly superseded
postqualified worktrees were removed earlier. PRs #47/#50/#51 and their exact remote branches are
now retired as recorded above; four PRs remain open. The clean BEA worktree retains needed unique
metadata/lifecycle work, while its website and inherited Treasury changes are obsolete or replaced.
No wholesale branch adoption is approved. Other dirty worktrees and unaccepted candidates retain
explicit custody. Current-design disposition and verified integration or preservation precede retirement.

## Historical execution handoff — 2026-09-08

The owner resumed the full V1 owner-test goal after the accepted recovery audit. The independent
backup verified 57/57 checksums at resumption and remains preserved. The dependency and exclusive
file ownership wave is retained in `.agents/tmp/resume-2026-09-07/wave-1.md`. No main/release merge,
public release, brokerage execution, or routine CI/CD is authorized by this implementation wave.

Latest pushed checkpoint: `3261e164019baba24b6c4e18b6aa82fe96930bf6`. The owner clarified that
the separate local setup website must be removed entirely and connection setup must live in
Desktop Settings → Onboarding. The CLI website-opening command, browser flag/helper and unused
browser dependency are removed. This supersedes the earlier optional-browser checkpoint
`39314057`; that intermediate behavior is not the accepted product requirement. The existing
CLI binary check passed on the clean unchanged final checkpoint (log SHA-256
`21d18a650734794bd5276c2700984509cd2b10121741fc1c7698e2200f925f38`). The temporary clean
verification worktree was removed after push. Native setup, removal of the HTTP site and saved
credential reuse are integrated into the working source. The combined CLI/service Rust check and
Desktop TypeScript check passed on 1,838 unchanged inputs; these are working-source checks, not
clean-head release approval. Evidence is in
`.agents/tmp/resume-2026-09-07/support/root-console-onboarding/{composed-r3,typescript-r2}-verification.json`.
Root navigation inspection found the current form is mounted under Connections; the owner-required
Settings → Onboarding placement is assigned as a correction using the same components. Actual setup,
live publication and installed restart verification remain open.

Treasury's real Fiscal and Daily activation reached the public services but failed before publication.
Retained bounded response diagnostics identify 28 literal missing Fiscal rates among 5,009 rows,
and an auxiliary Daily display field that differs from the actual thirty-year yield. The narrow
working-source fixes preserve missing observations and the genuine yield respectively. Both existing
all-history raw-seal/recovery cases passed on 1,781 unchanged Rust/dependency inputs, with no new
test case or CI. Evidence is in `.agents/tmp/resume-2026-09-07/support/root-treasury-source-registration/rate-schema-verification.json`.
Actual complete publication, typed reads and clean installed restart remain open; the last owned
service run exited 1 and is stopped. External diagnostic responses are not admitted capture authority.

The owner approved **SPY as the main scorecard, with VTI alongside it** on 2026-09-08. Both comparisons
must be fixed before historical evaluation; SPY determines the headline comparison and VTI supplies
the broader U.S. market comparison. A strategy cannot choose the more flattering benchmark after its
results are known. This choice grants no purchase or execution authority. Exact canonical identity,
source history, dividends/splits, costs and chronological evidence remain mandatory implementation
requirements. The selected funds are the [SPDR S&P 500 ETF Trust](https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy)
(ISIN US78462F1030) and [Vanguard's VTI total U.S. market fund](https://advisors.vanguard.com/investments/products/vti/vanguard-morningstar-total-stock-market-etf)
(CUSIP 922908769), checked against official issuer descriptions on 2026-09-08. This is an approved
comparison policy, not evidence that dual-benchmark production is already complete.

- Canonical Fund NAV publication/recovery checkpoint
  `a41aa6ba85195d776252b132b7991c0993b3a730` is pushed on
  `feature/v1-installed-product-experience` and reported on PR #43. It reconciles the interrupted
  atomic publication, schema hash, extraction-record binding and immutable selection. Existing
  data library compilation and the single existing NAV recovery case passed before commit.
- Pushed checkpoint `60b4f9c4e22ffdac82137430a6fc614ca6531530` adds current-date NAV selection without reading older Parquet rows,
  preserves cancellation/deadline/limit errors, and reads an exact selected provider definition at
  its original cutoffs. The existing catalog identity/restart case and NAV publication/recovery
  case each passed 1/1 before commit. These are scoped implementation checks, not clean whole-app
  or release approval. Fund product consumer wiring is prepared; neutral dispatch remains open.
- Census's row-scope correction passed its existing critical parser case and application test
  compilation. The bounded live attempt made four requests (three successful) and stopped during
  variables metadata acquisition with a transport-class failure whose cause was not retained.
  No QWI publication or restart proof occurred; no retry ran. Its durable live acceptance is open.
- Pushed shared-data checkpoint `f8cb4f0613f294bdfa8e992cb3b56ad19da9616a` adds atomic terminal-page staging and resumable
  32-page publication groups. The existing critical publication/recovery case passed 1/1 on
  2026-09-08, including interruption, restart, orphan preservation and exact retained rows under
  its unchanged 8 MiB processing cap. The data library check passed. The existing identity receipt
  case also passed 1/1. Final data source fingerprint:
  `6d755996f62205ab800151729f1b08801628e50878e4fe6aa9cf563de48a331a`.
  Real failures corrected during this work were the restore table inventory ceiling, source versus
  publication schema metadata, unnecessary output row groups, and omitted Arrow allocation overhead.
  Treasury application integration and whole-app verification remain open; the processing cap is
  not a measurement of the complete installed application's RAM. No CI ran for this checkpoint.
- Pushed checkpoint `4c6e2a9e887166d7d4978c54284af391019266b8` adds bounded durable market route discovery and exact retained
  source metadata revision reads using original event/knowledge cutoffs. It reuses the existing
  catalog and selection authority without new tables. The existing market publication/restart
  case passed 1/1, including cutoff exclusion, exact metadata, invalid input and cancellation.
  Data source fingerprint: `15ea5a6410d0009c92c5c7c8607c3944ab93fc000feeba9f32290488d5951f19`.
  Root inspected the six-file source/test delta. The subsequent corrected application library
  check and native Desktop library check passed on then-frozen WIP; TypeScript also passed.
  Later integration changed that WIP and requires a fresh affected check. These compilation
  results do not establish current rights or complete live workflow acceptance.
- Pushed checkpoint `3eedf0d4a80a4592f052749f6613ccfb38cb540c` adds cancellable, deadline-bound durable market replay using the
  existing supervised worker and retained file capabilities; ordinary catalog contention is
  distinct from corruption. Exact raw daily history can supply realized execution/outcome prices,
  preserving its original acquisition clocks and excluding historical signal/training authority.
  The existing publication/PIT recovery and complete-history cases each passed 1/1 on frozen data
  source on 2026-09-08. Root inspected all seven data files and the two application call-site
  updates. No new test case, schema/table, CI or provider call was introduced. Financial workflow,
  corporate-action integration and installed whole-app acceptance remain open.
- Pushed checkpoint `a6a715dfa56ed6e30d327db1164a974093503970` adds separately locked, bounded authority namespaces in the
  same retained directory. It reuses the existing two-copy writer and recovery path, allowing
  Desktop to preserve an unsupported profile payload before replacing its active settings.
  The existing `configuration_security` harness authority-state roundtrip case passed 1/1 on
  unchanged source (2026-09-08, session 37244). The case now covers namespace isolation,
  concurrent-lock rejection, retained-directory use after rename and interrupted publication.
  Root inspected the four platform file changes and updated the existing source-budget error
  mapping for the new typed invalid-namespace failure. This is scoped macOS persistence evidence;
  latest native Desktop integration and complete installed shutdown/restart remain unverified.
- Pushed checkpoint `e80860b2002baf068bafc3f175a0709eb49d049d` removes the superseded budget-format decoder, conversion
  types and its obsolete migration test. The single active bounded canonical representation and
  existing recovery policy remain authoritative. The existing malformed/truncated/noncanonical
  state rejection case passed 1/1 on unchanged source (2026-09-08, session 47370; no warnings).
  Root inspected both file changes and verified their final hashes. No persisted application
  state, provider credentials, backup or source snapshot was rewritten. No new test or CI ran.
- Current integration compilation corrected the remaining local-file adapter mapping for the
  typed invalid-namespace error, valuation recovery import/visibility, internal calendar imports,
  forecast evidence projection and new analytical-error consumers. Application plus PyO3 library
  compilation passed on the combined WIP (session 73083); native Desktop library compilation
  passed (session 69999). Existing warning backlog remains. The updated feature publication/reopen
  case passed 1/1 (30063), proving original evidenced availability separately from later local
  acquisition, completed-close coordinates and a source-bound feature-only read. Both existing
  recommendation backtest cases passed (67600), including entry exclusion at the financial target.
  These are affected integration checks; they do not establish clean exact-head release approval,
  complete default workflows, installed live restart or the 500 MB resource target.
- Desktop profile/transport/brief work passed the pinned TypeScript check. Native orchestration,
  actual component policy validation, calibrated forecast production, realistic historical
  backtest evidence, valuation, persisted decision production and complete default/Advanced
  journeys are still being integrated. Provider leaves continue concurrently; adapter reports
  and focused checks do not establish complete product workflows.
- Latest Wave 3 affected checks passed on retained WIP: application/PyO3 library check (61135,
  log `/tmp/market-squawk-wave3-combined-check-r3.log`, SHA-256
  `0b5633bd4273c81f233222e84081964537f62f9a1182650167a55c4b47a5a9eb`) and native Desktop
  check (29437). Existing publication recovery passed 1/1, decision generation passed 1/1, and
  the existing backtesting library passed 14/14 after shared fill/accounting changes. Desktop
  TypeScript passed. The forecast numerical check preserved exported model bytes and fitted
  offsets under held-out-only perturbation, and checked the actual MAPIE center against its
  exported affine representation. This used the production Python module directly; installed
  Python import and complete live workflows remain unproved. No CI or new routine test target ran.
- Actual guided backtest integration is still defective: advertised datasets lack its runner's
  required execution fields, history qualification is lost, action accounting is omitted,
  baseline/stress behavior differs from the displayed choices, and preview/read budgets disagree.
  The bounded source audit is `/tmp/market-squawk-guided-backtest-audit.md`; the existing guided
  route is being repaired through genuine study, raw-history, action and calendar authorities.
  Canonical investment-reference publication, per-request calendar-backed portfolio risk,
  energy-context consumption, complete Treasury application verification and full default orchestration are
  concurrent unfinished integrations. Adapter and compiler success cannot close these items.
- Pushed Treasury adapter checkpoint `9156b354abde8f298384268003a1dff481574c79` retains the
  actual replay worker through cancelled or dropped waiters, joins it before releasing ownership,
  preserves raw verification errors, and retains Fiscal Data's data-bearing terminal page. Root
  inspected the source changes and reconciled the preserved candidate with the current root.
  The two existing exact adapter cases passed 1/1 each on 2026-09-08; all 336 recorded adapter and
  dependency source hashes remained unchanged. Logs: `/tmp/market-squawk-treasury-replay-critical.log`
  (SHA-256 `1b17e3a6f42f1f90769eacd16c042da52030bca4b7bba6dbaa9ed0cec732ea56`) and
  `/tmp/market-squawk-treasury-fiscal-critical.log`
  (SHA-256 `5e6d3b0dfeedaa7f702fb27963fc2508948b0fd02d0bd4ba55fc57b2dcd47aa3`). No new
  test target, routine CI or provider call ran. The working tree also contains reviewed Treasury
  application activation/publication/drain wiring and current-schema decoder removal; its
  combined compilation and actual live installed journey remain pending. Historical review
  approval is not inferred from these focused checks.
- Root integrated bounded capture reads and exact original-publication discovery with their
  actual publication clock, the nine-file portfolio/calendar reference composition, and the
  shared forecast expectation arithmetic plus authentic historical valuation issuer. These are
  uncommitted application/data integrations pending the native fiscal interface barrier. A new
  live decision-publication lane connects the existing atomic saved-analysis authority; a causal
  harmonic lane connects the existing detector and preserves evaluated-no-pattern separately from
  unavailable evidence. Neither lane is complete merely because its read projection already exists.
- Current application integration now compiles after reconciling genuine price dates versus native
  fiscal periods in forecast, screening, valuation, backtest and dataset consumers. Model admission
  and restart retain the actual training product contract. The forecast descriptor uses the same
  model-owned request decoder as execution, replacing its duplicated validators. Normalized source
  error handling is shared by ingestion and market reads; cancellation and limits remain typed.
  Check r5 passed with 1,685 unchanged recorded inputs: `/tmp/market-squawk-app-integration-r5.log`,
  SHA-256 `07606cd4f4498023d22307a934724a8cfa524a131d7ebea5c265c26b8e11db5e`.
  The 1,699 application warnings and remaining disconnected workflow/provider candidates still
  require integration review. The existing forecast receipt case was updated to the current request
  contract, with no new case; its focused check passed 1/1 with unchanged inputs. Native Desktop
  compilation passed with 1,930 unchanged inputs, and pinned-toolchain TypeScript checking passed
  with 233 unchanged inputs. The existing job cancellation/publication case also passed after its
  critical extension for unknown write acknowledgment. Exact logs, hashes and candidate ownership
  are recorded in the wave document. Root then applied the 25-file product training and original-job
  recovery integration, including preservation of artifacts after uncertain durable publication.
  Application library check r2 passed with 1,688 unchanged inputs; the existing exact saved-model
  index replay case also passed 1/1 on that same input set. Native fiscal workflow consumers and
  actual installed training remain open. No CI ran. These changes are WIP, not accepted release evidence.
- Pushed checkpoint `0ceb23c6431eb96020db1f6b5adf3beea37bf90c` preserves uncertain job
  publication for reconciliation. Its existing cancellation/publication case passed 1/1 on a clean
  detached checkout with 1,733 unchanged inputs; log SHA-256
  `b7defdd61ece7ee50d9b6238038528d2c6dc6b7dcb283ddff41620b8fae6fb82`.
  The clean verification worktree was removed after push; all larger live WIP remains preserved.
- Pushed checkpoint `0dafb8e27a30be3661e57368cf386fb6b0a7a743` supports genuine monthly
  coordinates and scale-preserving decimal results in the existing finite output validator. The
  existing closed-schema case passed on a clean unchanged checkout; verification evidence is in
  `support/root-startup-schema-repair/services-checkpoint-verification.json`. The full production
  descriptor check also passed on current WIP. Root applied early contract validation before
  persistent application startup. Actual isolated CLI/service startup, genuine authentication,
  neutral economic-context read, clean shutdown and same-workspace restart passed; fixed-cutoff
  economic content matched and correctly reported zero imported indicators. These are debug
  executable/service results, not the final installed owner package.
- The earlier parallel usage-limit error was temporary. Existing Treasury, Schwab, data, market
  history/actions, jobs, backtest, fiscal and Desktop lanes resumed after the owner reported reset.
  Their current source responses verify execution; historical error status is not a current limit.
  The dependency/file-ownership record is `restart-closure-wave-2026-09-08.md` in the support root.
  Pending work includes Schwab,
  remaining provider publication/replay, real default dataset/training/fiscal jobs, complete
  forecasting/valuation/harmonics/backtests/recommendations/paper and Desktop/CLI/MCP journeys.
  No agent report or interrupted run establishes completion. The dependency/ownership record and
  exact before/after source inventories remain in `.agents/tmp/resume-2026-09-07/`.
- Running the actual current CLI exposed invalid response schemas before startup. The first
  correction compiled with 1,804 unchanged inputs; a fresh startup then identified the portfolio
  prerequisite descriptor, and source inspection found two unsupported macro formats. Root is
  finished those concrete contract repairs and passed the existing production-descriptor check.
  The first failed initialization's workspace remains preserved. Supported installed-workspace
  recovery passed without resetting state. Treasury retained import and exact-session cancellation
  are now integrated in WIP. The existing recovery case exposed a normal-stack overflow; the
  production fix boxes the one large compensation future while retaining its original ownership.
  The same existing case then passed 1/1 on the normal stack with 1,804 unchanged inputs, log
  SHA-256 `d1a93ab4270979d94e774001c72de3b71ebee25de902bfe06b45dbd23361cbd8`.
- Latest pushed checkpoint `884bf21b1db5b905eaf2d16eee7dc05190f557e0` registers the actual
  source metadata before retaining a first history-import checkpoint. A genuine Treasury Fiscal
  activation exposed the prior source-registration ordering failure. The existing data recovery
  case now exercises first registration itself; it passed 1/1 on the clean exact checkpoint with
  1,755 unchanged inputs, log SHA-256
  `15b4cdc9eac04bdfabbe473e55bb57b8aaa6b19ef88e14da376d59595f10f6be`.
  The clean verification worktree was removed after push; unique WIP remains preserved.
  Support: `.agents/tmp/resume-2026-09-07/support/root-treasury-source-registration/`.
- The current CLI/service build passed with 1,804 unchanged inputs, log SHA-256
  `31a011fd70bd99e385c9cd10caab7fcb1465974280080687738f9d52ef992c9b`.
  Actual Fiscal and Daily Treasury setup acknowledged retained import, then both failed before
  publication with `Extraction(Source(InvalidProtocolState))`. This is not data-pull completion.
  No raw response pages were retained; the source owner is preserving the real failure stage and
  cause instead of guessing a parser change. The failed generation's shutdown reported incomplete
  source cleanup and is not recorded as clean shutdown. Complete installed source restart remains open.
- The owner requested a dedicated in-console connection setup lane after root's verification CLI
  opened a local browser portal. Current Desktop Manage setup still delegates all providers to that
  portal and rejects native onboarding mutations; credential bundle import only stores credentials
  or setup intent. This is an incomplete implementation of the approved permanent-shell setup.
  The explicit dependency/file-ownership brief is
  `.agents/tmp/resume-2026-09-07/console-onboarding-brief-2026-09-08.md`.
  The lane reuses the installed service, existing saved sessions and protected credential import,
  keeps ordinary onboarding in Settings/Connections, and makes browser fallback explicit.
  External official-provider login remains distinct from Market Squawk's local setup UI.
- Quarter 4 remediation and the unchanged final owner-test/package/installed-restart/resource
  gate remain open. Measure the complete installed process tree against 500,000,000 bytes under
  heavy use with all capabilities retained; feasibility remains unproven. No hardware increase
  or feature reduction has been approved. Preserve active/unique WIP; clean only after verified
  integration and push establish preservation.

## Historical execution handoff — 2026-08-31

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

## Historical execution handoff — 2026-08-30

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

## Historical installed-product V1 execution

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

## Historical v1.0.0 release state

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

## Historical Quarter 4 closeout sequence

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

### Feed-product correction — base `5a763d8d`

The live field diagnostic proves only `provider_product` differs: public metadata declares
`coinbase-advanced-trade-configured-products-v1`, while handoff `product()` denotes the native
trading pair. The three-file adapter correction separates those meanings using the existing feed
constant; Direct keeps its exact-pair feed product. Sidecar evidence retains both canonical feed
product and native pair. All instrument/native selection, clocks and payload checks remain intact.

Astra's files are frozen and lead-inspected. Lead removed the temporary field/stage diagnostic
implementation, preserving its patch in ignored local evidence. A small closed typed-error log
remains in the application supervisor. Existing decoder and Direct-session checks, then the
unchanged live publication/restart journey, are the verification sequence; one compiler job.

Decoder evidence passes1/1 (0.99s). Existing Direct-session check fails before publication with
`LiveScopeNotCovered` (0.01s). Astra research_options_failure owns read-only tracing of that existing
fixture and coverage setup to distinguish a real regression from stale fixture metadata; no edits,
builds or Git. Lead retains implementation ownership and proceeds with the public live integration
build. Logs: `coinbase-product-{decode,direct}-critical.log`.

Direct failure is the existing fixture's identity-less session: `begin_session` rejects live market
coverage before network or publication code. Astra may update exactly
`tests/common/{catalog.rs,selected.rs}` and `src/direct_transport/tests.rs` in the Coinbase adapter,
reusing the genuine catalog fixture with an explicit namespace, attaching its authority and selected
identity before session creation, and retaining its owner for the session. No production guard
changes, new harness, builds or Git. Public semantic files remain frozen during the current build.

Lead aligned the additional public `src/source/tests.rs` fixture caller to the same explicit namespace.

Final production compilation passes4m35s (`coinbase-product-app-build.log`). Corrected existing
Direct fixture passes replay/handoff1/1 (0.39s; build5.30s) and rejection-before-mutation1/1.
Logs: `coinbase-product-direct-catalog-critical.log`,
`coinbase-product-direct-rejection-critical.log`. Genuine registry selection is checked at trusted
current time; the separate original HTTP product receipt keeps its own exact historical receipt
time. This fixture proves transport/replay behavior, not historical end-to-end selection.

The corrected public run now crosses adapter canonical binding and reaches ingestion, then reports
`publication_ingest`; the Markets read concurrently reports composition authority busy. No exact
IngestError variant is yet identified. Astra research_options_failure owns a read-only trace of
this failure through crypto publication and event ingest; no edits/builds/Git. Identify the exact
likely rejecting boundary and smallest safe diagnostic/correction, without weakening authority or
increasing timeouts. Lead retains changes and runtime scheduling.

Final decoder rerun with the shared fixture passes1/1 (0.99s). Live check fails132.32s
(`coinbase-product-live.log`), now at `publication_ingest` after successful adapter binding and
100/100 committed terminal rows. Kraken/restart are not reached. This checkpoint fixes the proven
feed/native-product comparison and restores two existing Direct critical checks; it does not claim
completed live ingestion or resolved Desktop startup. No timeouts/limits were increased.

Feed-product checkpoint `042fd569` is pushed; PR #43 comment5931459183. Astra traced the suspected
self-lock and ruled it out: retained row identity checks use atomic generations, while catalog
precommit receives the existing borrowed authority. Lead owns a bounded supervisor diagnostic
using closed IngestError/Arrow/Catalog Display messages (no recursive raw error material), followed
by the same single-job build/live check. The concurrent Markets busy message alone does not prove
the cause of ingestion failure.

While that diagnostic build runs, Astra research_options_failure owns a read-only trace of the
Markets instrument-definition `AuthorityBusy` path and its available WAL snapshot read capability.
Return the exact remaining read/write coupling and smallest in-place fix; no edits/builds/Git. This
addresses the observed screen failure independently and does not assume it caused ingestion failure.

Confirmed read coupling: Markets `load_instrument_definitions` ->
`InstrumentDefinitionReadCapability::latest` takes the sole writer's `try_lock`, mapping normal
contention to unavailable. Approved dependency: capability retains the existing manifest read
owner/result limits; latest uses one independent WAL snapshot for all selected IDs and shared
verified instrument-history SQL. Astra owns `catalog_capabilities.rs`, `catalog/records.rs`, and
`catalog/read_snapshot.rs`; lead owns constructor wiring in `ingest.rs`, existing critical-test
integration, builds and Git. Pin/pin_optional authority semantics stay intact. No new gate/worker,
compatibility path, separate harness or competing build. Current diagnostic binary excludes this
new data edit; rebuild only after the lane freezes.

Snapshot-read lane is frozen and lead-inspected. Lead aligned the only capability constructor and
extended the existing writer-held event restart test: a nonempty ID lookup must execute and return
the fixture's genuine absence while publication owns the writer. This covers the otherwise
uncovered normal-contention regression without a new fixture/harness. Pinning/search remain
unchanged; this is not a claim that every catalog read is independent. Diagnostic app build
passes4m31s and its live run excludes the new snapshot source changes.

Latest diagnostic narrows ingestion rejection to CatalogError::Sqlite; exact SQLite code/message is
not yet captured. Astra research_options_failure owns read-only tracing of event publication SQL
constraints against real 100-row public microbatches and existing multi-row fixtures. No edits or
builds. Snapshot-read files remain frozen with lead.

Diagnostic live run fails132.24s (`crypto-ingest-error-live.log`) with a SQLite engine rejection,
not a proved nested-lock failure. Lead adds engine/constraint context for parameterized ingestion
writes (no SQL parameter values) to the existing error log for the next combined build. The
snapshot read change is now under the existing held-writer critical check.

SQL contradiction is confirmed: validated 100-row Coinbase batches meet the Rust per-operation
contract but event SQL accepts at most64 rows; broad INSERT OR IGNORE suppresses the parent CHECK
failure before child FK failure. Lead owns in-place SQL0021 structural counts/parent ordinal
validation, schema digest, existing 100-row restart check and integration. Astra owns only
`catalog/provider_event.rs`: replace stale128 publication and64 journal-frame checks with the
existing source-contract bounds, and make affected idempotent SQL inserts ignore uniqueness
conflicts only so CHECK/NOT NULL failures stay visible. No new budgets, lifetime limits, migration,
backward compatibility, builds or Git. HTTP page64 remains its distinct source contract.

The expanded 100-row critical fixture compiles but returns Catalog(CorruptCatalog) in3.73s
(`market-event-batch-contract-critical.log`), whereas its prior33-row snapshot version passed.
Astra research_options_failure owns read-only diagnosis of remaining canonical replay/backup
count assumptions against this exact fixture; lead owns narrow temporary phase diagnostics in
the existing test and all builds. No authority weakening, new budgets or other-file edits.

Phase evidence:100-row ingestion, current/historical reads and101-row archival pass; backup
creation rejects the journal's100 physical frames through evidence.rs's duplicated64 bound.
Lead owns that one reader alignment to existing MAX_PROVIDER_EVENT_MICROBATCH_FRAMES; temporary
test phase prints removed. Existing complete catalog-control/history check runs separately.

Final100-row event critical passes1/1 in7.40s (build41.02s), including concurrent definition read,
archival, duplicate retry, corruption detection, mixed backup/restore and exact original reopen:
`market-event-batch-backup-critical.log`. The populated catalog-control/history critical passes1/1
in0.57s (`instrument-history-catalog-critical.log`). Astra's completed read-only audit found no
additional event count mismatch in reached backup paths; remaining64/63 values are distinct
logical-object or HTTP-page contracts. Combined application build now runs single-job; all runtime
source is frozen. These focused checks are not live or installed acceptance.

Combined app compilation passes5m26s (`market-event-batch-app-build.log`). Live run fails132.40s
(`market-event-batch-live.log`): no SQLite batch rejection or Markets definition-lock failure is
reported, but the unified-feed helper receives a non-array and later Coinbase health authority
is unqualified. Kraken/restart are not reached. Storage critical checks pass; live/installed
acceptance remains open. Astra research_options_failure owns read-only analysis of the current
unified-feed result envelope and health-invalidating path; lead owns any next integration, builds
and Git. No source edits during this handoff.

Checkpoint0852aad6 pushed; PR #43 comment5931971854. Source audit confirms the live helper's
envelope is current: a successful zero-row (or zero-fitting-row) response contains data:null.
Lead owns existing production_mcp_composition.rs helper diagnostic: include typed result metadata
for a null unified feed, preserving all success assertions and retry deadlines. Astra continues
read-only tracing of zero-row selection/source health; no production edits yet.

Next read-only diagnostic assignment to Astra: trace public runtime preparation's
wait_for_public_snapshots/aggregate against actual source-product and stream-product semantics,
and how those selected snapshots reach the runtime registry. Lead's metadata-only live run is
active; no source changes, builds or additional worktrees by the agent.

Diagnostic live run fails132.49s (`crypto-empty-feed-live.log`), with zero available/observed sources
and no result truncation. Health rejection precedes final test cancellation by54s. Astra owns
read-only assessment of registry.rs SessionHealthQualification's current/previous-epoch overlap
and actual public heartbeat update frequency: determine whether healthy renewals invalidate still-
in-deadline queued rows and propose a revocation-aware bounded design plus an existing critical
selector. No behavior changes until the concrete contract is established; lead retains integration.

Approved bounded authority correction: queued leases keep their original time interval across
benign health renewals, without retaining a count-limited epoch history. A constant-size healthy-run
floor is reset on unqualified state, session invalidation, changed authorization/coverage evidence
or narrowed authority. Original wall/monotonic/event deadlines and capture/budget/identity guards
remain. Astra owns sources registry.rs, registry/{health_authority.rs,authority.rs,current_batch.rs,
authority/live_scope.rs}, and existing registry_authority/pre_feed_cases.rs. Add closed predicate
diagnostics on failed queued authority to distinguish the actual live failure; lead owns the
existing workspace tracing dependency in sources Cargo.toml/lockfile, compilation and integration.
The110ms live failure is not yet proved to be two-epoch eviction; do not claim that cause.

Lead also owns the sole catalog.rs attestation caller: its existing explicit unqualification
passes benign_renewal=false. The registry metadata/attestation invalidation remains immediate.

Lead closed the authority-evidence critical gap in the same pre-feed case: a changed subscription
receipt revokes an otherwise time-extended lease. Correct compiled harness is decode_contracts
(module registry_authority), not a standalone registry_authority target; no new harness added.

Renewal/degradation/narrowing/evidence-change case passes1/1; terminal epoch exhaustion passes1/1.
Existing budget invalidation case fails before its assertions with LiveScopeNotCovered: unlike
the updated pre-feed case it omits required native identity fixture setup. Lead owns that one
function in tests/registry_authority.rs and reuses existing register_fixture_source with its exact
covered instrument. No production guard relaxation or new fixture/harness.

Final source critical checks: pre-feed authority continuity/revocation1/1 (22.47s build), budget
revocation1/1 in0.01s after existing-fixture repair (2.99s build), terminal epoch exhaustion1/1
(11.80s build). Logs healthy-renewal-{critical,budget-fixture-critical,exhaustion-critical}.log.
Lead inspected all source changes and aligned architecture prose; no expiry/freshness increase.
Single-job combined application build is running on frozen source.

Combined authority candidate compiles6m32s (`healthy-renewal-app-build.log`); exact new executable
is control_plane-7656717189705b23. Live Coinbase/Kraken restart scenario is running from that
executable with unchanged reviewed public configs and the existing120s readiness bound.

Live predicate diagnostic at13:21:25.845 proves the remaining rejection is trusted wall AND
monotonic expiry; event timestamp remains in bounds, epoch/session continuity remains valid,
capture is healthy and budget available. This is not count eviction or teardown. Astra
research_options_failure owns read-only tracing of which captured lease is attached to committed
rows (including actor reuse) and whether persistence incorrectly reuses live freshness admission.
Identify the exact age/owner and minimal correct authority boundary; no deadline increases, edits
or builds. Current test is still joining/finalizing and has not reached restart.

Healthy-renewal live run finishes failed132.52s; no prior count-eviction, SQLite or definition-writer
rejection is reported. Exact trusted lease expiry remains open; no live/restart acceptance claimed.
Critical renewal, revocation, evidence-change, budget and terminal-epoch checks pass as above.

### Committed live evidence publication boundary — active

Base: `56c7ea45` (pushed; focused checks, live failure remains). Required outcome: admitted
Coinbase/Kraken observations persist and reopen without confusing elapsed price freshness with
permission to store evidence. Existing live admission and real permission/revocation checks remain.

| Owner | Dependencies / exclusive files | Outcome / check |
| --- | --- | --- |
| Astra `research_options_failure` | Diagnosis complete; sources registry authority files and existing registry authority tests; live `qualification.rs` only | Opaque committed-evidence authority with original qualification and continuing permission/revocation checks; existing critical admission/expiry/revocation cases extended only for this gap. No builds or Git. |
| Lead | Agent contract first; application crypto publication callers, shared exports/manifests, documentation and integration | Wire persistence boundary; inspect changes, serialized single-job focused checks and real public feed restart test; commit/push coherent result. |

No lifetime/freshness timeout increase, fabricated receipts, compatibility path, or new worktree.
Live-first publication is blocked by original live freshness expiry after actor admission; permission
expiry must remain independently enforced. Native Desktop and full stock journey remain unverified.

Implementation integrated: source registry computes separate static/runtime permission deadlines;
actor commit seals an opaque identity/admission receipt; Coinbase/Kraken precommit retain that receipt
instead of replaying live freshness checks. Original live validation is unchanged. Lead inspected
all constructor propagation, adapter consumers and the existing memory-charge path (size-based
carrier accounting). No new compatibility path or duplicate identity/lease is retained.

Critical evidence: `committed-publication-authority-critical.log` passed 1/1 (0.01 s; build12.57 s),
extending the existing sealed-expiry test for delayed publication versus unchanged live rejection,
wall/monotonic permission expiry and session revocation. `committed-publication-live-guards.log`
passed existing pre-feed deadline/capture/health/registry check 1/1 (0.01 s; build14.32 s).
Application build and genuine public-network restart check remain pending; no live success claimed.

Application build passed (6m31s), but `committed-publication-live.log` failed at132.52s before
first feed acceptance. Earliest failure is now sink authority rejection, followed by pending
publication revocation, rather than publication freshness expiry. Read-only trace found data
renewal scheduled from configured market-age/2, ignoring the earlier actual admitted minimum
(source/transport/native/permission) deadline already returned by the actor. This is a concrete
adjacent startup defect; no freshness or timeout increase is authorized.

Next bounded ownership: Astra `research_options_failure` owns `live_source/sink.rs` and the existing
sink fixture case only, to schedule renewal from actual remaining authority and validate that the
original timestamps/expiry remain unchanged. Lead retains integration/docs/build/Git. No new branch,
worktree, harness, broad review or concurrent compilation.

Actual-deadline correction integrated: both HTTP and active streaming paths schedule refresh within
the actual returned minimum authority interval; active data compares genuine health observation
time, including buffered acknowledgement. Later batches preserve an earlier/shorter schedule until
renewal. Original receipt/source clocks, stale-data qualification and deadlines remain unchanged.
One new deterministic case in existing sink tests covers the previously uncovered shortened-deadline
startup failure; no new harness. Build/check/live verification pending in `actual-deadline-*` logs.

Integrated verification: `actual-deadline-app-build.log` passed (7m54s, one compiler job);
`actual-deadline-scheduling-critical.log` passed 1/1 (0.00s). The real public test in
`actual-deadline-live.log` still failed132.63s: first Coinbase exact publication missed its original
deadline because Market.GetUnifiedFeed returned unavailable. Unlike earlier runs, no sink or
publication-authority failure is logged. This is not proof of live completion or durable output;
next dependency is the read/runtime availability path. Kraken and restart remain unreached.

Checkpoint includes source/publication authority and actual-deadline scheduling together, with
all affected callers inspected. No full gate/CI, native Desktop proof or whole-app RAM measurement.
One primary worktree and the three intended local/remote branches remain; no extra refs were made.

Pushed checkpoint: `c9ab294a`; PR43 evidence comment5933027823. Ownership released for completed
source-authority/sink changes. Next bounded read-only diagnosis: Astra `research_options_failure`
traces unified-feed reference loading against real public crypto synchronization. The unified read
requires execution definitions for every stream; crypto synchronization publishes market-data
reference records. Establish the canonical producer/consumer mismatch before any further edit.
Lead retains all write/build/Git ownership; no new verification currently running.
Execution-definition absence and database endpoint mismatch were ruled out: startup seeds and pins
exact terms, and ingest composition enforces matching catalog bindings. Lead added failure-only
closed stage/error/count attribution to existing market reads; no query/admission behavior changes.
Next run locates the first real unavailable stage before any corrective design change.

Market-stage diagnostic build passed4m31s; real test `market-read-stage-live.log` failed132.48s
with the same endpoint unavailable, but no market-read stage warning. Read-only Astra diagnosis
now traces installed request routing before the Market domain (runtime dispatch, installed tool
routing, application composition); no edits/builds/Git. Lead retains all source ownership and
checks remaining uninstrumented market-result exits. No acceptance weakened.

Confirmed root cause: `live_selected_market_source` required a UUID for `shardId`, while the real
scalar producer emits canonical ShardId Display `index/count` (e.g. `0/1`). Application output
validation rejected valid rows as InvalidResult; installed dispatch collapsed that to unavailable.
Corrected selected-source schema to the same bounded shard text contract already used by market
details. No identity change or compatibility path. Removed the temporary repeated stage probes;
retained two central failure-only logs of closed error/operation categories at application result
validation and installed dispatch, without request/result payloads. Existing real selected-row
restart test supplies the critical coverage; no duplicate fixture or new harness. Lead owns all
changes; Astra diagnosis is complete. Next: serialized application build and unchanged live test.

Shard contract build passed4m32s. `market-shard-contract-live.log` now passes first Coinbase
selected market row, durable publication probe and exact typed readback (the test reaches the
first service shutdown after these assertions). The full test fails77.10s at shutdown: pending
publication receives HealthNotQualified/authority revocation, and Market plus dependent research
domains report incomplete drain. Restart/Kraken remain unreached. No live lifecycle completion
claimed. This proves the prior selected-row contract failure is fixed; checkpoint it separately.
Next bounded Astra read-only diagnosis owns shutdown/publication ordering trace; lead retains
all mutation/integration/build/Git authority. Preserve cancellation and revocation guarantees;
do not turn arbitrary publication failures into successful shutdown.

Pushed `6340c552`; first market/publication/readback now passes, shutdown remains blocked.
Confirmed shutdown dependency: public source cleanup drops route senders, cancels actors,
ends the registry session and invalidates capture before the separately owned publication worker
is cancelled/joined. Queued committed evidence consequently fails its correct revocation check.

Next bounded ownership: Astra `research_options_failure` implements graceful public publication
drain in `live_source/{publication_admission.rs,coinbase_publication.rs,kraken_publication.rs,
sink.rs,supervisor.rs}` only, reusing the existing end-to-end frame permits. Keep sink/route/session/
capture alive until admitted work completes; retain immediate failure/revocation behavior and all
actual worker outcomes. No new queues, builds, Git or broad tests. Lead owns composition, shared
contracts, integration, any critical existing-test extension and real lifecycle verification.

Graceful-drain integration is frozen for verification. The adapter retains its transport request
permit/decoder session through the shared sink cancellation hook; explicit orderly shutdown stops
network admission, drains the existing frame semaphore, then joins source/publication owners.
Forced cancellation and publication failure still revoke/fail. Kraken channel owners disarm their
Drop cancellation only after their original task is joined, so one clean channel cannot cancel its
sibling. Astra ownership additionally covered `live_source/kraken.rs` for that concrete integration
finding and is now released; lead owns remaining verification/Git.

The existing Coinbase cancellation test now exercises cancellation after subscriptions and proves
the request permit remains held across the async drain, then releases afterward: PASS 1/1
(`graceful-adapter-test.log`, build1m04s, test0.33s). This closes the uncovered authority-lifetime
regression without adding a test harness. Serialized application build and unchanged real installed
public-source restart test remain pending; no successful restart claim yet.

Graceful-drain application build PASS6m37s (`graceful-public-drain-build.log`). Real installed
public test FAIL249.80s (`graceful-public-drain-live.log`), now after completing the entire Coinbase
journey: genuine typed live publication, clean shutdown, resumed publication after restart, exact
original evidence equality, explicit Source.Stop and stopped-state persistence across a third
service start. Sequential test control flow reaches Kraken only after all those assertions pass.
This is installed-service/typed Desktop transport evidence, not native WebView or whole-V1 proof.

Kraken stops before its first publication because official instrument reference preparation returns
unavailable. Astra read-only trace identified a protocol mismatch: reference transport checks
`/result/success` although the [official v2 instrument response](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument)
and existing adapter message contract use top-level `/success`. Lead independently confirmed the
source/schema contradiction. Checkpoint the verified Coinbase drain separately; next lead-owned
change is the exact reference lookup correction and unchanged live recheck. No endpoint/deadline,
identity, credential or financial evidence checks will be weakened.

Coinbase drain checkpoint `618b62c8`. Lead changed the Kraken reference subscription success
lookup in place to the official top-level field; all channel, snapshot, capture and selection
validation remains. Reuse the same real lifecycle selector; no added harness. Build pending.

Kraken acknowledgement build PASS4m45s, but live recheck FAIL208.27s at Coinbase restart
(`kraken-reference-ack-live.log`): retained original reference capture unavailable, before Kraken.
The prior full Coinbase journey is valid evidence for that run, not repeatable acceptance. Astra
trace found two concrete capture hazards: immutable reopen treats recovery RwLock contention as
unavailable, and AcceptedNativeReferenceCapture discards its live receipt pin before catalog
publication. Committed native references are included in recovery membership; that index is not
missing. Exact failed branch needs closed diagnostic evidence.

Next finite ownership: Astra `research_options_failure` owns platform journal `sealed.rs` and
`sealed_recovery.rs`, and data catalog `market_data_instruments/native_reference.rs`, to fix those
two capture lifetime/contention defects with existing control/receipt ownership. No new queues,
builds, Git or compatibility paths. Lead owns safe closed diagnostics in crypto reference callers,
integration, existing critical verification, commits/pushes. Dependency: retain original through
catalog commit and allow controlled immutable reopen before rerunning the same lifecycle.

Capture corrections are frozen for focused verification. Lead reused the already pinned workspace
parking_lot dependency for timed recovery read admission (manifest/lock edge only, no version
change), and retained safe failure-only capture stage/control or filesystem ErrorKind diagnostics.
Astra additionally owned the existing `data/tests/catalog.rs` native-reference custody case;
its early producer-token drop and precommit recovery now exercise the previously uncovered pin
gap. The existing platform sealed-reopen case now exercises actual recovery lock contention and
original cancellation/deadline controls. No new harness or general review round. Platform/data
checks are serialized before the application build/live journey; exact prior capture failure branch
remains unproven pending diagnostic/live evidence.

Capture critical checks PASS: existing platform sealed-reopen/recovery test 1/1 (0.14s,
`reference-custody-platform-test.log`), existing native-reference custody/identity/restart case 1/1
(0.97s, `reference-custody-data-test.log`). The latter explicitly runs recovery before catalog
commit with only the accepted capture retaining the physical pin. Application rebuild and real
lifecycle recheck remain pending. No full gate or whole-app performance claim.

Capture application build PASS7m07s (`reference-custody-app-build.log`). Live combined check
FAIL245.53s (`reference-custody-live.log`), after the full Coinbase publication/shutdown/restart/
original-evidence/explicit-stop journey completes again. No reference capture error in this run.
Kraken now passes reference preparation and opens its source pair, but trade generation1 requests
DecoderStateDiscontinuity before subscription/first-data readiness. Therefore the Kraken ACK fix
is live exercised; Kraken publication/restart remains unverified. Focused capture tests prove the
two repaired hazards; this run does not retrospectively identify which caused the earlier failure.

Checkpoint the reference-custody/protocol fixes. Next bounded Astra read-only lane traces Kraken
trade continuity against the official schema and a small public WebSocket sample; no mutation or
build assigned. Lead retains application/Git/verification ownership. No final lifecycle/V1 approval.

Reference custody checkpoint pushed `1a23ad84`. Bounded live Kraken probe (3 public messages,
0.79s, then close) identified the trade startup failure: status includes `upcoming_maintenance`
and `emergency`; the strict StatusData model rejects them. Both are documented paired optional
object arrays in the official status schema. ACK and trade snapshot shapes matched the adapter.
Next Astra ownership: Kraken `src/messages.rs`, `src/decoder.rs`, `src/level3/messages.rs`,
`src/level3/decoder.rs` and their existing critical status tests only. Accept documented advisory
shape through a shared status contract; retain authoritative system/connection/API validation and
all financial integrity checks. Lead owns build/test/Git; no new harness or broad provider rewrite.

Status implementation decision after source/official-schema review: consume the authoritative
required status-row fields and allow additive unused advisory fields on StatusData only. Advisory
text does not qualify observations or authorize orders and remains in original raw capture. Keep
strict envelope and financial-message schemas, required field types and existing status-state
checks. Reuse shared status parsing/validation across public and Level3. This is upstream protocol
handling, not an application backward-compatibility stack. Astra may extend only the existing
`session_tests.rs::captured_public_and_level3_handoffs_preserve_identity_continuity_and_atomic_recovery`
case/fixture for the actual additive status fields and existing reset behavior. No new harness.

The focused Kraken critical test failed in preexisting fixture admission (`LiveScopeNotCovered`),
before status decoding. Extend the same Astra ownership to its existing session fixture: install
a real catalog identity authority and record selected public/L3 identities before session start.
Retain catalog lifetime through the case; do not bypass production authority or add a harness.
Lead owns the necessary existing-workspace dev-dependency edges and locked build scheduling.

Kraken status/fixture changes frozen and lead-inspected. Existing captured public/L3 handoff,
identity/continuity/recovery test PASS1/1 (0.55s, build3.63s), and both affected existing source
authority/budget cases PASS2/2 (0.72s). Logs `kraken-status-selected-fixture-test.log` and
`kraken-status-source-authority-test.log`. One shared local fixture synchronizes real catalog
identities and uses the actual selected reference receipt; no bypass or new harness. Application
rebuild and unchanged live lifecycle check are next; no live Kraken claim yet.

Status application build PASS4m47s (`kraken-status-app-build.log`). Unchanged live combined
check FAIL245.24s (`kraken-status-live.log`), after the full Coinbase restart/original-evidence/
explicit-stop journey again completes. Kraken now passes status decoding but generation1 trade
flow quarantines `ProtocolInvariantViolation`; subsequent `HealthNotQualified` and cancelled
publication/seal operations follow teardown. No Kraken publication/restart acceptance. Checkpoint
the independently critical-tested shared status/real fixture correction; Astra is read-only tracing
the first invariant failure, lead retains integration/build/Git. No timeout or evidence weakening.

Status checkpoint pushed `60578fa6` (PR #43 comment5935128626). Read-only trace found a
concrete fresh-start contradiction: trade subscription requests the last50 historical trades,
while selected reference validity begins at the newly observed reference. An earlier trade fails
the unchanged native-identity interval check and maps to ProtocolInvariantViolation. The exact
logged branch remains unproven, but the request/authority mismatch is established from source.
Official trade API defaults snapshot:false; live updates do not require an initializing snapshot.
Next bounded Astra ownership: Kraken `src/config.rs`, `src/decoder.rs`, existing
`tests/live_to_paper.rs` case `metadata_binds_the_reviewed_ceiling_and_contains_no_fabricated_sequence`.
Request updates only for live trades and validate the exact matching ACK; retain book snapshots,
temporal identity checks and separately authorized snapshot decoding. No backdated authority,
row filtering, new history restriction, test harness or independent build. Lead owns integration.

Live-trade correction focused existing metadata/authority case PASS1/1 (0.00s, build4.97s;
`kraken-live-trades-critical.log`); application build PASS5m03s (`kraken-live-trades-build.log`).
The unchanged real installed-service lifecycle test is running from its freshly printed executable.

Independent Astra stock trace completed without edits: runtime current-share wiring is present
(`workflow_driver.rs` final preparation/publication arguments; `investment_generation.rs` original
history + automatic valuation + current action projection). The old fixture failure is not proof
of a runtime wiring defect. Next real stock evidence must inspect FinalPrepare.sources entries
current_share_actions/fundamental_share_actions and sourceActionReference, then exact history
reopening/selected valuation. Do not weaken share authority or rewrite working producers merely
to make the stale fixture pass. No stock live-completion claim.

Live updates-only run reaches genuine Kraken durable publication, then FAIL130.60s on the
existing test helper's singleton Source.GetStatus assertion (`kraken-live-trades-live.log`).
Lead and Astra confirmed production contract is one row per runtime stream: Kraken has separate
book/trade streams under one surface with a common lifecycle. Lead owns correction in existing
production_mcp_composition.rs helper: require the complete exact active stream pair, identical
lifecycle and requested surface for every row, and singleton inactive status after stop. Return
validation failures through Result so the service shuts down before temporary-root deletion; do
not filter away extra rows. Post-panic teardown warnings are not independent startup evidence.

Live trade/status recheck PASS1/1 in155.49s (`kraken-live-trades-status-live.log`) after the
17.54s helper-only rebuild (`kraken-live-trades-status-build.log`). Both configured public providers
complete genuine typed publication, clean stop/restart, new publication, exact prior evidence
readback and explicit-stop persistence. No warnings/errors in the passing log. This is focused
working-tree live evidence, not clean-head full-gate, native WebView, throughput or RAM acceptance.
No additional test matrix or gate is needed for this coherent commit; remaining V1 work stays open.

### Native stock workflow continuation — base `0ac80011`

Prior goal turn made progress: both public crypto installed-service lifecycle journeys passed,
checkpoint pushed (PR #43 comment5935482496), and 6.8GB obsolete control-plane build artifacts
removed with current executable/evidence retained. This wave returns to required native screens
and real stock analysis. The prior stable-endpoints workspace predates current in-place schemas;
keep it and all original recovery/session state untouched. Use a fresh explicit
`.market-squawk/v1-owner-test-indexed-storage/{data,installation}` verification workspace, reusing
authorized config import and practice-account choices through current product operations.

| Owner | Disjoint scope / dependency | Completion evidence |
| --- | --- | --- |
| Lead | Runtime roots, credential import/native automation, shared contracts, ledger/Git and sole build scheduling | Current service + hidden Desktop; all ordinary routes settle; real starter data and stock workflow preparation receipts; fix only reproduced failures. |
| GPT-6.1 Sol native_stock_preparation | Read-only current setup/stock operation recipe; no credentials or runtime actions | Exact current operations for import, stock/benchmark/fiscal preparation, approved practice allocation and start/resume; reusable existing script locations. |
| Astra calendar_currentness_failure | Read-only interpretation of actual stock preparation/result receipts once lead captures them | First real missing evidence producer or proof of complete saved stock analysis; no fixture-only rewrite. |

DAG: current native build/service → config/setup and native route reads → stock/benchmark/fiscal
inputs + approved simulated account → real analysis → saved result/restart. No release build,
whole-app RAM check, new branch/worktree, foreground automation or competing compilation.

Fresh current service starts Ready without password. During authorized bundle import, some
credentials persist but CLI import returns generic Unavailable; same-bundle idempotent retry is
running. Root retains value-free receipts. Fresh combined public defaults also expose Kraken
HealthNotQualified followed by Coinbase publication backpressure, unlike separate-provider
lifecycle proof. Astra research_options_failure owns read-only first-failure/clock/teardown trace
in fresh service logs and live source/qualification/import activation paths; no edits/builds/runtime
control. Lead continues native build/setup; Sol recipe lane checks current import timeout routing.

Native continuation evidence: current Desktop build passed; hidden WebDriver inspected all17 routes
against fresh indexed-storage service. Home/Markets fail `Market.GetCollection`/`Market.GetOverview`;
other15 routes have no query errors in this read. This is not full workflow acceptance. Source
inspection plus read-only catalog query identifies two valid crypto definition revisions without
optional display_name; product_market_identities requires it and rejects the whole population.
Lead owns `application/market_selection/product.rs` plus existing product-market critical check
for this presentation defect. Astra owns only `live_source/sink.rs` closed-cause diagnostics for
separate combined-public health failure; no behavior relaxation until actual cause is captured.
Native route evidence: `.agents/tmp/v1-first-stock/indexed-storage-native-routes.json`.
Practice account $100,000USD/0.25% costs and approved allocation committed using genuine receipts;
no paper session/order started. Credential retry completed17 provider dispositions without secret
output. Initial Nasdaq kind=source request was invalid (SurfaceMismatch); recipe correction pending,
not evidence that Nasdaq itself failed. No new branches/worktrees, gates or memory measurement.

Financial dependency follow-up: Astra calendar_currentness_failure owns read-only confirmation
of the reported missing fiscal plan/build/forecast dispatch handlers and SEC canonical identity
consumer. Produce a bounded producer-to-consumer implementation recommendation against current
HEAD; no edits, builds, runtime actions or Git. Lead current implementation remains optional-name
market projection and its existing critical test, plus the closed live-health diagnostic only.

Alpaca verify/start succeeded and admitted9starter equities/funds. It subsequently failed with
TrustedReceiptHighWaterUnavailable. Astra traced the actual16-spin paired-clock seqlock; ordinary
writer preemption can exhaust it and terminate the stream. BudgetUnavailable follows transport
permit release, not an independent budget contention defect. Assign Astra only
`crates/market-squawk-sources/src/authority_time.rs` (including existing paired-sample critical case)
to replace the homemade seqlock with the existing arc-swap immutable paired snapshot. Preserve
continuity/revocation/range checks and account for snapshot allocation. Lead reserves dependencies,
Git/builds. Source-source critical check precedes rebuilt app/native verification. No timeout or
freshness increase. First optional-name test compile found a test-only closure error conversion;
lead corrected setup value construction before retry, no production behavioral change.

Read-only financial lane confirmed missing first-stock edges (not new calculators): fiscal
GetFiscalPreparationPlan/StartFiscalDatasetBuild/StartFiscalForecast have contracts/driver calls
but no installed handlers; existing dataset/forecast preparation authorities are disconnected.
SEC issuer normalization uses CIK-derived identity, while stock financial admission needs an
explicit authorized company/security relationship and exact canonical identity. The relationship
authority has no production caller; wiring it alone is insufficient because row admission still
requires ID equality. Next financial checkpoint must wire those existing owners coherently and
retain original issuer evidence, using current company-identity/restart tests. These remain
functional acceptance blockers; current-share projection itself was already traced as wired.

Paired-clock critical checks PASS3/3 in0.02s (`paired-clock-critical.log`): concurrent initialized
reads always return a coherent nonregressing pair; uninitialized and terminal states still reject;
poisoned-clock permanent invalidation and forged receipt range/continuity checks pass. Existing
arc-swap1.9.2 dependency reused, no manifest change. Closed health cause diagnostic is behavior-neutral.
This independently coherent source-authority correction is ready for commit/push; live proof waits
for rebuilt service. Optional-name product fix remains separate WIP with corrected critical compile
running. No completed V1/combined-live or exact-head full-gate claim.

Optional-name discovery check PASS1/1 in0.36s after6m15s serialized rebuild
(`optional-market-name-critical-retry.log`). Existing real-catalog test now includes an admitted
unnamed crypto instrument with ambiguous venue symbols alongside SPY/VTI: named stock lookup,
pagination, token resolution and truthful unavailable-price projection still pass; nameless crypto
remains selectable with honest label and no invented symbol. Shared product projection falls back
to its admitted unambiguous symbol, otherwise explicitly unavailable name. No rows/evidence are
dropped, no financial identity or rights checks changed. Native live recheck requires service
rebuild; baseline Home/Markets failures are not yet declared closed.

Native integration at `0d09e0f3`: service build PASS9m43s. Previous process65221 handled SIGTERM
and exited0. Hidden Desktop eventually launched/reconnected to current service69249 with no app
password; initial restart trace includes transient Starting and one exited child67247. Settled
recheck (`market-startup-settled-native-routes.json`) has17/17 Ready routes, zero query errors.
Home/Markets now show9named starters and CLI MSFT search returns its real canonical selection.
This closes the reproduced population-wide optional-name error, not complete live pricing.
Prices remain unavailable: Alpaca recovery is Blocked/reconciliation with null doctor and verify
returns Unavailable; automatic recovery reports expired/invalid publication authority. Separate
Kraken failure is now confirmed freshness-only via closed cause4112. No further clock retry
exhaustion was observed in this short run; that is not sustained ingestion/performance proof.
PR #43 implementation evidence: comment5936024993. Next barrier is recoverable live connection
renewal and freshness-only resynchronization, then genuine stock/fiscal workflow wiring. Keep
current root, saved account/config and original protected backups intact.

### Live connection recovery and fiscal dispatch — base `4a0465ad`

Owner authorizes the researched design and implementation in the current worktree. Storage is
already integrated; this wave closes demonstrated consumer failures rather than restarting storage
research. Current tree is clean at the audit base; no service/build is running.

| Owner | Exact ownership and dependency | Smallest completion evidence |
| --- | --- | --- |
| Lead | Shared `service/tool_services.rs`, lifecycle integration, ledger/Git/runtime and all builds; reserves manifests/contracts | Existing critical dispatch/recovery checks, then current workspace connection renewal and real market reads. |
| Astra calendar_currentness_failure | `service/forecast_preparation.rs` and `service/forecast_preparation/fiscal.rs` only; existing fiscal request/result contracts | Implement the three missing current fiscal operations through existing dataset/forecast owners; report exact shared dispatch needed. No builds/Git. |
| Astra research_options_failure | Read-only trace of retained Alpaca reconciliation blocking Verify in `local_product/source_lifecycle*`, `application/market_runtime*`, onboarding/account lifecycle | Identify first failed authority and smallest recovery fix; no edits or runtime actions. |

DAG: disjoint fiscal implementation + retained connection recovery trace → lead shared dispatch/
recovery integration → serialized critical checks → commit/push → real saved workspace retry.
No lifetime/freshness limit increases, new storage stack, branch/worktree, full gate or RAM check.
The separately confirmed Kraken freshness-only resynchronization remains open for its next coherent
checkpoint; this wave does not claim sustained combined ingestion or complete investment analysis.

Alpaca normal Retry reproduced Unavailable in the retained workspace; diagnostic service stopped
cleanly (PID80861, exit0). Read-only trace narrows failure to publication-generation registration;
null projected doctor does not prove missing verification. Lead additionally owns failure-only
stage logging in `provider_activation/alpaca.rs`, preserving underlying closed registration errors.
Fiscal dispatch now uses existing dataset/forecast job runners. Extend existing installed service
startup/restart case to assert a complete fiscal unavailable plan for missing input population;
this closes the missing-handler recovery gap, not successful fiscal forecast acceptance. Real
fiscal success remains dependent on SEC canonical issuer relation and admitted model runtime.

### Immediate Desktop window — base `4a0465ad`, alongside fiscal dispatch

Acceptance 5/7: observed roughly 50 seconds without a window because installation/configuration
and service connection execute before the native event loop. Replace that ordering in place: show
the existing main window immediately, initialize through the existing asynchronous bootstrap
command, and keep failure/retry inside the window. Preserve hidden automation and shutdown.

| Owner | Files / dependency | Critical evidence |
| --- | --- | --- |
| Lead | Desktop `src-tauri/src/lib.rs`, `bridge.rs`, `service.rs`, startup composition, ledger/Git/builds | Native window appears while bootstrap is pending; startup failure can retry; no duplicate initialization. |
| Sol frontend lane | Desktop `src/app/routes.tsx` only; existing product/system status contract | Shared startup loading/retry screen; no individual page unavailable errors during startup. |
| Astra fiscal lane | Existing fiscal files only; diagnose rejected new critical call | Identify contract mismatch; no builds or shared dispatch edits. |

DAG: native asynchronous startup and disjoint startup presentation → integrated native check.
Fiscal critical run compiled but rejected `Analysis.GetFiscalPreparationPlan`; diagnose before
committing that separate checkpoint. One compiler remains the lead-owned resource. No new worktrees.

Fiscal failure traced to shared `application/research/instrument_context/find.rs`: incomplete
population was handed to a complete-cohort constructor and mislabeled invalid request. Lead
corrects unavailable evidence before construction; no invalid-request catch-all or authority bypass.

Owner reports all Overview prices unavailable. Current live status confirms saved credentials and
available quote/snapshot/history/calendar/stream probes, but lifecycle is blocked with no current
generation. Later logs show activation did eventually reach generation5, then failed durable
publication ingress and stale-generation cleanup deadlines; the earlier registration error alone
is not the complete current cause. Lead restarts the existing service with the rebuilt diagnostic
binary while preserving the visible Desktop and retained root. Astra research_options_failure
owns a read-only trace of ingress failure/cleanup and published-close-read InvalidResult, with
no source edits or runtime actions until the exact failure is established.

Fiscal installed startup/restart check PASS1/1 (83.23s; single-job compile5m22s). Extended existing
Desktop startup/reconnect check PASS1/1 (3.78s total), including delayed initial opening, initial
failure and retry, optional credential recovery and stale-session rejection. Native startup
compile/live visibility still pending.

Before fiscal checkpoint acceptance, Astra calendar_currentness_failure additionally owns
`service/tool_services/training_preparation.rs` with `forecast_preparation/fiscal.rs` to reuse the
existing selected-export verifier and owned I/O worker. This removes a duplicated verification
policy in the new fiscal caller; no new runtime, limit increase or broad redesign. Lead next
compiles the coherent dependency set once those files are frozen.

Native/fiscal sources frozen for one serialized Desktop build (with development automation).
The fiscal lane released its shared verifier change; both callers retain exact export identities
and use the existing owned Research I/O worker. Current native build includes that dependency.
During compilation Astra calendar_currentness_failure is read-only on stale Alpaca reference
generation registration in `provider_runtime.rs` / `market_provider_configuration.rs`; Astra
research_options_failure remains read-only on the separate live ingress byte/slot backpressure.
The updated failure-only diagnostic confirms coordinator StaleRuntimeGeneration, distinct from
the subsequent observed live queue-full failure. No authority weakening or capacity increase.

Native build passes but the normal launch visibility check found zero accessible windows after
15 seconds; no immediate-window completion claim. Lead retains Desktop startup ownership.

### Restore Overview prices — current uncommitted integration on `4a0465ad`

| Owner | Exact ownership | Dependency and critical proof |
| --- | --- | --- |
| Astra calendar_currentness_failure | `provider_activation/market_config.rs` only | Remove transient lease issuance from durable metadata identity; same-authority rereads stable, changed authority remains distinct. No builds/Git. |
| Astra research_options_failure | `adapters/market-squawk-adapter-alpaca/src/live.rs`, `apps/market-squawk/src/live_source/alpaca_publication.rs`, `live_source/sink.rs`, `application/market_runtime/alpaca_publication.rs` only | Await both frame and byte admission through existing sink lifecycle; preserve custody/order/cancellation/drain. Existing critical case extension only for uncovered queue-full failure. No builds/Git. |
| Lead | Shared integration, Desktop startup, ledger/Git/builds/runtime | Inspect both changes, serialize critical checks, then verify actual saved Overview prices. |

DAG: stable generation identity + independent Alpaca backpressure + Desktop visibility correction
→ lead integration/critical checks → live retained-workspace prices and native launch → commit/push.
No authority relaxation, capacity increase, discarded evidence or new worktree.

Alpaca lane ownership also includes `adapters/market-squawk-adapter-alpaca/src/boot_snapshot.rs`: the initial snapshot must reserve publication capacity under its existing cancellation/deadline before emitting its first frame.

Native timing recheck uses CoreGraphics window metadata because AX alone returned zero while a
native window existed. Fresh normal launch reports a visible on-screen Market Squawk window at
0.422 seconds (receipt `immediate-window-visible-cg.json`). Concurrent second Desktop hit its
exclusive MCP receipt store; retry is being checked after closing the first instance. No claim
of simultaneous multi-Desktop workspace support or complete market prices.

Immediate-window slice verified: native build PASS; normal on-screen window at0.422s; native
initial composition failure followed by Try again reaches Ready after releasing the other
Desktop's exclusive local receipt owner. Existing startup/reconnect renderer test and typecheck
PASS. The Home saved collection renders all nine starters; its includeMarket request remains
rejected and is part of the separate active price-remediation wave. No complete market-data or
installed-release acceptance is claimed.

Window checkpoint pushed as `62a9be27`. Native Ready/Home recovery verified; no pricing claim.
Astra calendar_currentness_failure released stable metadata identity and now traces read-only
`application/paper/market/durable_product.rs` and market history previous-close reading to identify
the exact InvalidResult behind includeMarket rejection. No edits in this diagnostic follow-up;
lead retains shared history/read contracts. Alpaca ingress lane remains disjoint and active.

Lead adds closed, failure-only stages to `research/market_history/previous_close.rs` before the
next build. VTI stored21-bar publication is present and timestamp checks inspected so far pass;
exact failed reader/rejoin/projection predicate remains unproven. Diagnostics expose only stage,
instrument and closed service error, preserving all existing admission/integrity behavior.

Fiscal dispatch checkpoint: shared installed service startup/restart critical check PASS1/1
(86.29s; serialized compile8m55s) after shared dataset-export verification integration. The
three current fiscal operations now dispatch through existing dataset/forecast owners; absent
source population returns the complete nine-target unavailable plan. Positive fiscal forecasts
still require SEC issuer population and an admitted model runtime; no complete analysis claim.
Source metadata/backpressure and saved-price diagnostics remain separate uncommitted work.

### Saved-price rights correction — base `7062fb5a`

Confirmed: source ingestion admits operation rights but no downstream use grant; the previous-close
reader requests LocalAnalysis instead of Display. Separately, connection doctor expiry is copied
into retained personal-use rights. Existing reobservation preserves custody but cannot select a
new current authorization for original immutable payloads. These prevent saved price display.

| Owner | Exact ownership | Dependency and critical evidence |
| --- | --- | --- |
| Astra calendar_currentness_failure | `crates/market-squawk-data/src/research_use/` and `crates/market-squawk-data/src/ingest.rs` only | Existing grant admission and exact-payload current authorization; preserve original lineage, selected grant pinning, revocation and actual policy expiry. Extend existing critical rights tests only. No builds/Git. |
| Lead | Application policy in `provider_activation/mod.rs`, `market_config.rs`, shared `research_service.rs` integration, display projection use/error mapping; runtime/build/Git | Policy expiry separate from doctor freshness; explicit grants restricted to authorized operations. Saved prices and live prices verified separately in retained workspace. |

DAG: data authority API fix + application policy/display wiring → critical authorization checks →
retained workspace previous-close and live feed → coherent commit/push. No database reset, schema
migration, provenance rewrite, permission bypass or expiry extension of an old rights decision.
The already frozen metadata/backpressure changes are being exercised with the rebuilt service.

Rights wave ownership extends to existing data `tests/publication_recovery.rs` critical cases;
lead also owns `research/source_errors.rs` and chart-history `projection.rs` shared display
authorization. Preserve corrupt-authority errors while mapping genuine denied rights to recoverable
unauthorized state. Fiscal checkpoint pushed as `7062fb5a`.

Astra research_options_failure follows the rebuilt live service read-only: automatic Alpaca
recovery reports activation-busy / calendar publication unavailable after restart. Identify first
closed failure from new `overview-rebuilt-service.log`; no edits, builds, runtime actions or secret
reads. Lead keeps policy integration and service lifecycle ownership.

Lead owns the new cohesive `research_service/retained_use.rs` policy composition and its
existing `research_service.rs` owner, plus data public reexports. Logical market/valuation
consumers will use that shared owner; no independently copied permission checks.

Live failure diagnostic ownership: Astra research_options_failure may edit only
`application/research/market_history/native_sessions.rs` and
`application/research/calendar/alpaca/durable.rs` to retain closed stage/error identity at the
confirmed failing boundaries. No payload/credential values, changed admission or new tests.
These diagnostics join the next serialized build; no separate diagnostic rebuild.

Lead logical-consumer ownership: `application/market_selection/investment.rs`,
`application/fair_value/automatic.rs` and its `forecast.rs`/`financial.rs` callers. Shared retained
policy authorization runs through existing owned Research I/O; no new runtime or detached worker.
Correct diagnostic calendar path is `application/market_calendar/alpaca/durable.rs`.

Direct saved-history evidence: VTI original receipt/anchor/run completion agree, but catalog
availability is 13.127ms later; all source/object/capture identities match. Astra
research_options_failure owns correction in native_sessions.rs and identical corporate-actions
`applicable/anchor.rs` consumer. Data lane released ingest.rs; only a necessary generation-clock
accessor hunk there transfers to the clock lane. Retained-use methods remain frozen. Evidence:
`.agents/tmp/v1-first-stock/native-history-clock-diagnosis.json`.

Lead owns canonical `migrations/0008_research_use_authority.sql` and its `src/migrations.rs`
checksum: existing triggers must preserve original lineage while validating selected current
exact-payload rights. No new migration/version or compatibility path. Retained development catalog
requires an explicit backed-up in-place trigger/index correction after critical checks; it has
not been modified yet. Clock lane also owns only necessary origin-clock query fields in data
`manifest/catalog.rs`. Existing complete-history restart case supplies regression assertions.

Saved-price critical checks: `provider_market_event_publication_is_restart_queryable` PASS1/1
(7.68s), `complete_alpaca_history_is_exact_clock_safe_and_restart_selectable` PASS1/1 (2.11s).
The physical derived-data restart case initially reached `Parquet(RootAuthorityAlreadyOwned)`:
the fixture retained `historical_v1` and its disk-backed row-index owner across the new reopen.
After releasing that finished read, the existing physical restart case PASS1/1 (3.76s).
All three selected data checks now pass. No retained catalog or running binary changed,
and live dashboard price delivery remains unverified. Next: application compilation/checks,
backed-up canonical development schema correction, then live Overview and restart verification.

Live-price followup: Astra research_options_failure traces `freshness_only=true, causes=4112`
from the retained service log to exact health flags and fatal/recoverable handling. Read-only,
no builds/runtime actions; lead keeps all edits/integration ownership until a concrete cause.
Application metadata critical compilation is active in the single-job queue.

Confirmed live-feed failure: received stale provider timestamps become terminal health errors
(`4112` = source freshness + current-data deadline). Astra research_options_failure owns only
`live_source/sink.rs` for Alpaca nonterminal stale-data disposition and next-data requalification,
preserving captured publication and withholding current-price authority. Crypto rendezvous unchanged.
Application compile was stopped after explicit private-import errors; corrected import uses public
source exports. No compiler remains active while this sink correction is integrated.

Alpaca stale-data correction frozen: process_data now returns without live publication/readiness
for freshness-only rejection while retaining the existing pending durable handoff. It clears cached
qualification so the next batch rechecks current health; other failures and crypto unchanged.
Lead inspected the exact disposition and custody callers. Current serialized command is
`cargo build --locked --offline -p market-squawk --bin market-squawk-service` (one job),
log `.agents/tmp/v1-first-stock/saved-price-service-build.log`. This prioritizes the actual live
Desktop verification; application unit check was not passed or claimed. Offline schema dry-run
proves only two authority triggers and one selected-payload index differ; apply has not run.

Live verification: service build PASS (6m42s); offline schema correction applied after clean
shutdown, full catalog backup at `.market-squawk/recovery/pre-retained-rights-7062fb5a/catalog.sqlite3`.
992 source-rights rows, 876 runs, 11 generations and 3 history publications preserved.
Live startup FAILED with stack overflow during automatic Alpaca doctor/source restoration.
Crash report `market-squawk-service-2026-10-01-144420.ips` confirms the nested
restore_live_sources → execute_owned → continue_account_transition → doctor → HTTP stack.
Astra live_stack_failure owns read-only diagnosis of that exact path and smallest fix; no builds,
Git, runtime actions or edits until ownership proposal. Lead owns runtime/native verification.

Stack diagnosis measured exact current binary: 17 active frames consume 2,026,160 bytes on
a 2 MiB worker, before remaining HTTP/runtime frames. Largest: execute_prepared_commit422,768;
restore_live_sources354,736; execute_owned245,760; startup closure209,056;
continue_account_transition193,376. Astra live_stack_failure now owns only
`local_product/source_lifecycle.rs` for the narrow heap-owned transition-future boundary;
preserve control/cancellation and existing owner, no thread-stack increases or new runtime.
Lead will rebuild once, compare generated frame sizes and exercise normal startup.

Stack fix frozen: a non-inlined constructor owns the existing execute_prepared_commit future
on the heap; its existing body/control flow remains unchanged in the private inner method.
Lead inspected the15-line boundary and started one-job service rebuild; log
`.agents/tmp/v1-first-stock/saved-price-stack-build.log`. Native test app was stopped to prevent
repeated crashes. Post-crash catalog integrity is ok, zero FK violations, history rows preserved.

Screen resilience followup (2026-10-01): refreshed official Tauri process model, Tokio blocking
work, SQLite WAL and TanStack cached-query guidance against current code. Previous goal turn
produced new evidence: Markets discards cached market data on refresh error, and individual
instrument unexpected errors can abort collection aggregation. No blanket error swallowing.

| Owner | Exclusive files | Outcome and check |
| --- | --- | --- |
| GPT-6.1 Sol High screen_refresh | Desktop `src/features/markets/market-collection.tsx` only | Keep matching saved market data visible during refresh failure, clearly mark freshness/recovery; no stale identity reuse. Lead typecheck/native verification. |
| Lead | Runtime, shared backend contracts/aggregation, ledger, Git and builds | Verify completed stack build at normal stack size, then actual retained price reads and startup stability. Inspect per-instrument failure boundary before changing shared contracts. |

Dependencies: stack build completed (5m14s) -> generated frame inspection -> normal service startup
and native reads. UI presentation is independent, no extra build by agent. Integrate only after
actual diff inspection and critical checks; no new branches/worktrees or quarter review.

Live rebuilt service remains running through restoration (normal Tokio worker stack). Generated
execute_prepared_owned frame dropped to 61,232 bytes; exact binary UUID E477343A-E4F6-3D66-98ED-0E1DF4F019E9.
New live evidence now isolates calendar publication rejection: account-currentness-activation-lock
reports activation-busy during calendar_ingest (IngestError discriminant34). Astra
calendar_currentness_failure owns read-only trace of account.rs currentness, calendar durable
publication and source_lifecycle transition guard, to propose exact smallest shared-guard fix.
No edits/builds/Git; lead owns runtime and shared integration.

Further live result supersedes initial liveness observation: service PID19996 aborted exit134
with another Tokio stack overflow after calendar retry attempts. Native client restarted service;
lead stopped background Desktop to avoid retry churn. Astra live_stack_failure now owns exact
new crash diagnosis and may edit only source_lifecycle.rs plus propose other necessary boundaries
before editing them. Same single-job rebuild after both stack and calendar corrections freeze.

Calendar diagnosis correction: discriminant34 is catalog AuthorityBusy. Existing calendar
publication already reuses its held activation read guard; adjacent activation-busy warnings do
not prove self-contention. Two redundant one-shot catalog acquisitions in provider reobservation
are the direct failure sites. Astra calendar_currentness_failure now exclusively owns data
ingest.rs, app research_service.rs and calendar durable.rs for initial-reconciliation reuse and
existing owned-worker publication with deadline/cancel checks; no competing builds.

Stack ownership adds source_lifecycle/public_preparation.rs to live_stack_failure: measured
73,576-byte prepare_public_command and 73,216-byte scalar live.start future variants keep
restoration parents large. Isolate those canonical child constructions without increasing stacks.
Lead owns existing Desktop app.test.tsx extension: first unavailable -> real cached fixture price
-> failed refresh preserves price/time and disables changes -> recovery. Initial focused run
failed before Markets loaded during cold lazy transformation; real route preloaded for the
read-lifecycle regression, no product timing assertion loosened. Typecheck passed.

Independent screen-refresh checkpoint: shared Home/Markets collection preserves matching cached
choices/prices/timestamps when refresh fails, marks freshness unverified and disables collection
changes until choices recover. Existing market journey PASS1/1 (2.75s), including initial absence,
remove/restore, successful price read, both refresh failures, preserved timestamp and recovery.
TypeScript check passed. This is critical fixture evidence, not native/live startup completion.
Only its two Desktop files plus ledger are checkpointed; backend WIP remains explicitly pending.
Git inventory: one primary worktree, local and origin main/release/feature only. No cleanup needed.

Screen checkpoint pushed cbcd09e5; PR43 comment5938561586. Remaining backend changes inspected
and frozen for one service build (saved-price-restoration-build.log). Calendar uses owned optional-job
ingest worker and removes redundant try-lock reobservation; caller deadline/cancellation remain
bounded, while non-preemptible catalog wait retains original worker custody until it completes.
No 1ms polling, extra worker runtime, stack-size increase or authority bypass added. Stack fix
isolates both measured large public/scalar future variants. Next: real service startup + price reads,
then existing complete-history/reobservation critical check.

Lead-only Desktop reconnect slice during serialized backend build: product-context.tsx, routes.tsx,
status-rail.tsx, app-sidebar.tsx and existing app.test.tsx reconnect case. Confirmed global failure:
stream_disconnected clears already-admitted same-session state and unmounts every product route.
Keep that known session/rendered cached views during transport recovery; first admission, replacement
scope and rejected event/receipt still require verified admission. Show explicit reconnect/stale
notice and correct shell status. Existing reconnect test must prove retained view plus old-session
rejection and replacement admission; no new harness.

Desktop reconnect slice critically verified: existing startup/session replacement and market journey
cases PASS 2/2 (4.73s); TypeScript check passes. Already-admitted same-session routes remain mounted
with a disconnect notice and saved-price qualification, cancelled reads retain successful data,
and successful reconnect refreshes active queries. New sessions and invalid receipts/events still
require admission. Native combined-workflow verification remains pending. Backend compilation caught
a shared helper removed despite child-module callers; lead restored that still-used helper unchanged.
Reobservation retains its new owned-worker path; one service build is running.

Rebuild PASS (9m35s), binary UUID 58523BD6-E9B8-3874-A9A0-3D9761536541. Generated restoration
frames shrink; real service survives restoration and native Home admits. CLI collection returns
DIA/IWM/VTI/TSLA previous closes; five symbols remain unavailable. No full live completion claim.
Live log restoration-fixed-live.log shows research I/O gate waits interrupting crypto publication
and the whole starter-history batch reaching its shared 30-second deadline. Existing history
critical test is compiling. Astra calendar_currentness_failure owns read-only dependency/lock-order
diagnosis of ResearchIoWorker, calendar ingest and provider raw publication; propose smallest
correct scheduling fix before editing. Lead retains all edits/builds/integration and native checks.

Native Markets confirms all nine names and the four actual saved prices with original timestamps;
Home/Markets remain admitted and service PID13956 survives beyond three minutes of restoration.
Exact binary frame examples: execute_owned 76,240B; restore_live_sources 115,552B;
restore_active_live_sources_independently 64,576B; startup restoration 121,600B. No stack-size
increase. Existing complete history/calendar reobservation/restart case PASS1/1 (1.96s).
Stack boundaries are an independent checkpoint; retained-policy/backpressure/calendar-worker
changes remain pending their final checks and I/O scheduling diagnosis. PR43 includes c3c8edba.
