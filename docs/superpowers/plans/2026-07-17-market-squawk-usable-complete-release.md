# Market Squawk Usable Complete Local Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver one hardened, zero-mandatory-cost, usable local Market Squawk release in which all
required live, research, analytics, Python, modeling, portfolio, execution, valuation, CLI, and MCP
verticals run together and pass an exact-head release gate.

**Architecture:** Production live decisions remain a bounded single-writer Rust pipeline whose only
action path consumes current `DirectVerified` authority through comprehensive risk and one-time
dispatch. Independently, lawful live/research adapters publish canonical observations through a
single-writer SQLite catalog, versioned Arrow schemas, immutable content-addressed Parquet manifests,
bounded DataFusion/PIT services, Rust and Python analytics/modeling consumers, and shared bounded
application services used by CLI and local stdio MCP. Integration follows an explicit dependency DAG;
root manifests, application composition, authority handoff, evidence, and publication remain
serialized while disjoint grouped worktrees run concurrently.

**Tech Stack:** Rust 1.97.1 stable, Edition 2024, Cargo resolver 3, Tokio, Serde, Reqwest,
Tokio-Tungstenite/rustls, rust_decimal, SQLite through bundled rusqlite, Apache Arrow/Parquet 58.3.0,
DataFusion 54.0.0, PyO3/maturin with PyArrow and Python `decimal.Decimal`, Clap, tracing, rmcp,
tract-onnx as the required self-contained ONNX backend, optional operator-supplied ONNX Runtime
through `ort`, Proptest, Criterion, cargo-fuzz/libFuzzer, cargo-deny, cargo-audit, Gitleaks, and
deterministic Python policy/product tests. Task 1 verifies every non-frozen version against current
primary sources, Rust 1.97.1, licenses, and the locked dependency graph before a consumer is merged.

## Global Constraints

- This plan was refreshed against clean pushed product-code audit anchor
  `a829278aca4d4fc27d5a0c0aaa8e5a49f2cb5659`, tree
  `6f5d9b7be896e9a5409f367c73aa4a5d95208a9c`, on 2026-07-18. That SHA is an audit anchor, not
  approval: its A4 implementation and release-evidence runner are integrated and focused-review
  clean, but the idle-host gate has not yet admitted the required measurement and the full exact-head
  quarter gate has not run. Task 0 refreshes paths, APIs, dependencies, line anchors, evidence, and
  the DAG; Stage 1 integration credit still requires the approved live/capture closure.
- The read-only traceability input is `.agents/tmp/usable-release-traceability-audit.md`, SHA-256
  `57caaad73b638eeb785157a24ab54dba1e49251859c5f104d8f6ab6d259fb731`. Task 1 verifies that digest
  and persists its deduplicated capability/source-link matrix under `docs/research`; the ignored file
  is not the durable source of truth.
- This plan and `docs/project-memory.md` supersede the earlier halfway stop through the user's later
  explicit resume/continue-to-completion instructions. Stop only at the usable-complete-local-release
  terminal gate in Task 20.
- Stages and Waves describe dependency and ownership. Fresh grouped reviews occur at exactly four
  delivery checkpoints labeled **Quarter 1 of 4** through **Quarter 4 of 4**. Historical `Q1`, `Q2`,
  `Q2-I*`, `Q2-R*`, and existing filenames remain immutable audit locators, not extra active
  quarters.
- No paid software, paid API, cloud service, external database service, mandatory container runtime,
  mandatory telemetry infrastructure, or OpenTelemetry dependency.
- Rust is exactly 1.97.1 stable for release, benchmark, and approval evidence; 1.97.0 is forbidden.
  Every workspace package inherits Edition 2024, resolver 3, package metadata, and workspace lints;
  `Cargo.lock` is committed.
- No unsafe Rust. Production paths contain no `unwrap`, `expect`, `panic!`, `todo!`, or
  `unimplemented!`. Libraries return typed `thiserror` results; `anyhow` remains at application
  boundaries.
- Identity/account rotation to evade limits, browser/TLS fingerprint concealment, CAPTCHA or
  anti-bot bypass, blocking-evasion proxy rotation, stealth scraping, distributed quota evasion,
  access-control circumvention, and synthetic sources represented as production are prohibited.
- Provider access uses one authorized identity, a shared authoritative budget, durable cursors and
  caches where rights allow, conditional requests, `Retry-After`, bounded backoff, source health,
  authorized failover, and explicit coverage. Rights are enforced per retrieve/display/persist/cache/
  redistribute/train operation at extraction and storage boundaries.
- Financial/order/accounting values use checked scaled integers, `Decimal`, Arrow `Decimal128`, or
  Python `decimal.Decimal` with explicit currency, scale, tick/lot, unit, rounding, and overflow
  policy. Floating point is admitted only inside explicitly bounded statistical/model kernels.
- SQLite, Arrow/Parquet writes, DataFusion, Python, MCP, LLMs, arbitrary filesystem work, and
  unrelated network requests remain outside the live event-to-action path.
- Every queue, parser, frame, archive, page, retry, row group, query, result, artifact, audit stream,
  model, Python batch, and MCP request is bounded by count, retained bytes, deadline, and cancellation.
  Saturation fails closed before authoritative mutation.
- Research and live pipelines remain independent. Historical data need not mirror the live source;
  replay remains optional diagnostic tooling and is never the research/backtest architecture.
- Add a crate, adapter, schema, dataset, migration, or Python package atomically with its first
  working producer and consumer. Empty crates, schema-only future datasets, mocks, traits, plans, and
  compatibility paths receive no production credit.
- Strategies, models, adapters, CLI, MCP, replay, archives, and caller-authored DTOs cannot construct
  current live authority, `ApprovedOrder`, or adapter dispatch. Every paper action passes the sole
  risk and one-time dispatch boundary.
- A focused lane gate proves only that lane. Approval requires one clean unchanged exact commit, all
  locked local gates, deterministic default tests, the applicable grouped quarter review with zero
  unresolved Critical/Important/Minor findings, truthful external-smoke status, GitHub publication,
  and cleanup.
- External network tests are opt-in and separate. Default tests use content-hashed, rights-recorded
  fixtures through production parsers and local protocol servers.
- Tests are thin, concise, and critical: extend existing behavioral suites where practical, prove
  the narrowest authority/accounting/recovery/resource/parser or producer-to-consumer invariant,
  and consolidate overlapping cases. Test counts, prose/file-existence checks, wrapper snapshots,
  duplicate fixtures, and implementation-detail matrices provide no release credit. This does not
  relax adversarial tests for boundaries whose failure could corrupt money, authority, provenance,
  resource limits, or durable state.
- The integration owner alone edits `Cargo.toml`, `Cargo.lock`, shared public exports, application
  composition, live-authority/risk dispatch handoff, migrations registry, README capability state,
  checkpoint evidence, and review/publication state.
- New crates under `crates/*` and `adapters/*` enter the workspace automatically. A lane may use a
  generated lane-local lock only for provisional RED/GREEN diagnosis; it never stages, commits, or
  hands off that lock. Before handoff it proves `Cargo.lock` matches its lane base. At each Wave
  integration barrier, the integration owner reviews/merges exact manifests and workspace dependency
  requests, performs one minimal offline-first lock resolution, audits the lock diff, commits it,
  and reruns every affected focused gate plus the whole Wave gate with `--locked`. Until that locked
  rerun passes, lane evidence is provisional and the capability remains `Missing`.
- Grouped lane worktrees are removed normally after integration, evidence handoff, no active agent,
  and clean status. Never force-remove dirty or active worktrees.

---

## One-time live/capture prerequisite gate

This is an admission gate before Stage 1 integration. It is not a fifth delivery quarter, does not
replace the Quarter 1 of 4 checkpoint, and cannot award product credit. The integration owner and
one independent read-only reviewer are the prerequisite-review authority. The immutable audit
identity `q2-a4-seed-checkpoint-review` remains in the evidence schema only because the closed
harness requires it; it does not name a current quarter.

This section is the sole executable authority for the pending live/capture measurement. The dated
`2026-07-17-q2-a4-capture-authority-preflight.md` file remains supporting audit history. Its
unqualified `target/q2-a4-capture-benchmark/standard/` examples are invalid and must never be used.
Every standard artifact reference is head-qualified as
`target/q2-a4-capture-benchmark/standard-$STANDARD_HEAD/`.

### Reference freeze

The standard-reference head is the first clean pushed pre-Wave-1 commit containing the integrated
capture implementation and unchanged closed benchmark harness. Before measurement:

```bash
set -euo pipefail
STANDARD_HEAD="$(git rev-parse HEAD)"
test "${#STANDARD_HEAD}" -eq 40
test -z "$(git status --short)"
test "$(git rev-parse '@{upstream}')" = "$STANDARD_HEAD"
test "$(rustc --version)" = "rustc 1.97.1 (8bab26f4f 2026-07-14)"
./scripts/verify.sh
./scripts/check_capture_queue_loom.sh
cargo deny check
cargo audit --deny warnings
gitleaks dir --no-banner --redact --config .gitleaks.toml .
gitleaks git --no-banner --redact --config .gitleaks.toml .
test "$(git rev-parse HEAD)" = "$STANDARD_HEAD"
test -z "$(git status --short)"
```

The independent reviewer then verifies that exact SHA, the standard `sync_channel` reference, the
candidate fixed-ring backend, immutable harness/tool/source inventory, Rust 1.97.1 binding, release
profile, fixed workloads, five-repetition contract, memory accounting, and host-gate controls.
Every substantiated finding is remediated and the complete freeze gate and review repeat on a new
head. Approval freezes the unchanged `STANDARD_HEAD`; it is prerequisite approval, not a delivery-
quarter review. The reviewer returns that full literal SHA as `REVIEWED_STANDARD_HEAD`; the
measurement operator must supply it explicitly rather than deriving authority from the later
checkout.

### Standard measurement and baseline lock

Measurement waits until every other agent and all Cargo, rustc, Criterion, and capture-evidence
processes are stopped. At preflight admission, the one-minute load divided by logical CPU count must
be at most `0.10` (`0.80` on eight logical CPUs). The continuous monitor enforces competitor-process
absence and immutable input/tool bindings; it does not claim an interval load-average threshold.
The integration owner must truthfully attest `no-other-active-agents`; active background writers
invalidate the run.

```bash
set -euo pipefail
: "${REVIEWED_STANDARD_HEAD:?set the exact independently reviewed 40-hex standard head}"
STANDARD_HEAD="$REVIEWED_STANDARD_HEAD"
test "${#STANDARD_HEAD}" -eq 40
test "$(git rev-parse HEAD)" = "$STANDARD_HEAD"
test "$(git rev-parse '@{upstream}')" = "$STANDARD_HEAD"
test -z "$(git status --short)"
COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
RUN_DIR="$EVIDENCE_ROOT/standard-$STANDARD_HEAD"
HOST_EVIDENCE_DIR="$RUN_DIR/host-gate"
LOCK_DIR="$EVIDENCE_ROOT/.exclusive-lock"
PREFLIGHT="$HOST_EVIDENCE_DIR/preflight.json"
release_host_lock() {
  if ! test -d "$LOCK_DIR"; then return 0; fi
  if test -f "$LOCK_DIR/owner.json"; then
    if ! test -s "$PREFLIGHT"; then
      printf '%s\n' 'owned evidence lock has no valid release ticket; preserve and escalate' >&2
      return 1
    fi
    scripts/capture_benchmark_host_gate.sh release \
      --lock-dir "$LOCK_DIR" --release-ticket "$PREFLIGHT" \
      --expected-lock-device "$(jq -r '.lock_identity.device' "$PREFLIGHT")" \
      --expected-lock-inode "$(jq -r '.lock_identity.inode' "$PREFLIGHT")" \
      --expected-owner-device "$(jq -r '.owner_identity.device' "$PREFLIGHT")" \
      --expected-owner-inode "$(jq -r '.owner_identity.inode' "$PREFLIGHT")" \
      --expected-nonce-sha256 "$(jq -r '.lock_nonce_sha256' "$PREFLIGHT")"
  else
    test -z "$(find "$LOCK_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
    rmdir "$LOCK_DIR"
  fi
}
on_exit() {
  status=$?
  trap - EXIT
  release_host_lock || status=1
  exit "$status"
}
trap on_exit EXIT
test ! -e "$RUN_DIR"
umask 077
mkdir -p "$EVIDENCE_ROOT"
chmod 700 "$EVIDENCE_ROOT"
mkdir "$RUN_DIR"
chmod 700 "$RUN_DIR"
scripts/capture_benchmark_prepare_build_evidence.py \
  --run-dir "$RUN_DIR" --benchmark-backend standard
printf '%s\n' no-other-active-agents > "$RUN_DIR/active-agent-attestation.txt"
chmod 600 "$RUN_DIR/active-agent-attestation.txt"
mkdir "$LOCK_DIR"
chmod 700 "$LOCK_DIR"
scripts/capture_benchmark_host_gate.sh measure \
  --lock-dir "$LOCK_DIR" \
  --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
  --output-dir "$HOST_EVIDENCE_DIR" \
  --runner "$RUN_DIR/capture_admission_evidence-exe" \
  --build-evidence "$RUN_DIR/build-evidence.json"
env CAPTURE_BENCH_BACKEND=standard \
  CAPTURE_BENCH_FINALIZE_ONLY=1 \
  CAPTURE_BENCH_BUILD_EVIDENCE="$RUN_DIR/build-evidence.json" \
  CAPTURE_BENCH_HOST_EVIDENCE="$HOST_EVIDENCE_DIR/comparison.json" \
  CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
  "$RUN_DIR/capture_admission_evidence-exe" --bench
jq -e --arg head "$STANDARD_HEAD" \
  '.schema_version == 5 and .backend == "standard" and
   .measured_code_head == $head and .host_gate.valid == true and
   .repetitions == [1, 2, 3, 4, 5]' "$RUN_DIR/manifest.json"
release_host_lock
(cd "$RUN_DIR" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | \
  while IFS= read -r file; do shasum -a 256 "$file"; done > SHA256SUMS)
(cd "$RUN_DIR" && shasum -a 256 -c SHA256SUMS)
test "$(git rev-parse HEAD)" = "$STANDARD_HEAD"
test "$(git rev-parse '@{upstream}')" = "$STANDARD_HEAD"
test -z "$(git status --short)"
```

The integration owner writes exactly:

```text
docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md
docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json
```

The report records the exact measured head, hardware/OS/toolchain/profile, commands, fixed fixtures,
sample/repetition rationale, every latency/throughput/RSS result, host admission, raw artifact
references, and digests without making a candidate claim. The canonical compact-JSON lock copies
the complete closed field set enforced by `validate_baseline` in
`capture_benchmark_prepare_build_evidence.py`; it binds the head-qualified manifest reference,
manifest/report hashes, `independent_seed_review_approved` state,
`q2-a4-seed-checkpoint-review` identity, and every backend/build/harness/tool/artifact/repetition/
host/toolchain/profile digest. Commit only those two files. Their commit must be the only delta from
`STANDARD_HEAD`; otherwise discard the run and re-freeze a new standard head.

### Paired candidate and prerequisite approval

On that report-only clean child, run the candidate against the exact head-qualified standard
manifest and tracked baseline lock:

```bash
set -euo pipefail
CANDIDATE_HEAD="$(git rev-parse HEAD)"
test -z "$(git status --short)"
BASELINE_LOCK=docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json
STANDARD_HEAD="$(jq -er '.baseline_head' "$BASELINE_LOCK")"
git merge-base --is-ancestor "$STANDARD_HEAD" "$CANDIDATE_HEAD"
test "$(git diff --name-only "$STANDARD_HEAD..$CANDIDATE_HEAD")" = \
  $'docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json\ndocs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md'
COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
STANDARD_DIR="$EVIDENCE_ROOT/standard-$STANDARD_HEAD"
BASELINE_MANIFEST="$STANDARD_DIR/manifest.json"
RUN_DIR="$EVIDENCE_ROOT/candidate-$CANDIDATE_HEAD"
HOST_EVIDENCE_DIR="$RUN_DIR/host-gate"
LOCK_DIR="$EVIDENCE_ROOT/.exclusive-lock"
PREFLIGHT="$HOST_EVIDENCE_DIR/preflight.json"
release_host_lock() {
  if ! test -d "$LOCK_DIR"; then return 0; fi
  if test -f "$LOCK_DIR/owner.json"; then
    if ! test -s "$PREFLIGHT"; then
      printf '%s\n' 'owned evidence lock has no valid release ticket; preserve and escalate' >&2
      return 1
    fi
    scripts/capture_benchmark_host_gate.sh release \
      --lock-dir "$LOCK_DIR" --release-ticket "$PREFLIGHT" \
      --expected-lock-device "$(jq -r '.lock_identity.device' "$PREFLIGHT")" \
      --expected-lock-inode "$(jq -r '.lock_identity.inode' "$PREFLIGHT")" \
      --expected-owner-device "$(jq -r '.owner_identity.device' "$PREFLIGHT")" \
      --expected-owner-inode "$(jq -r '.owner_identity.inode' "$PREFLIGHT")" \
      --expected-nonce-sha256 "$(jq -r '.lock_nonce_sha256' "$PREFLIGHT")"
  else
    test -z "$(find "$LOCK_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
    rmdir "$LOCK_DIR"
  fi
}
on_exit() {
  status=$?
  trap - EXIT
  release_host_lock || status=1
  exit "$status"
}
trap on_exit EXIT
(cd "$STANDARD_DIR" && shasum -a 256 -c SHA256SUMS)
test ! -e "$RUN_DIR"
mkdir "$RUN_DIR"
chmod 700 "$RUN_DIR"
scripts/capture_benchmark_prepare_build_evidence.py \
  --run-dir "$RUN_DIR" --benchmark-backend candidate \
  --baseline-manifest "$BASELINE_MANIFEST"
printf '%s\n' no-other-active-agents > "$RUN_DIR/active-agent-attestation.txt"
chmod 600 "$RUN_DIR/active-agent-attestation.txt"
mkdir "$LOCK_DIR"
chmod 700 "$LOCK_DIR"
scripts/capture_benchmark_host_gate.sh measure \
  --lock-dir "$LOCK_DIR" \
  --active-agent-attestation "$RUN_DIR/active-agent-attestation.txt" \
  --output-dir "$HOST_EVIDENCE_DIR" \
  --runner "$RUN_DIR/capture_admission_evidence-exe" \
  --build-evidence "$RUN_DIR/build-evidence.json"
env CAPTURE_BENCH_BACKEND=candidate \
  CAPTURE_BENCH_BASELINE_MANIFEST="$RUN_DIR/baseline-manifest.json" \
  CAPTURE_BENCH_BASELINE_LOCK="$RUN_DIR/baseline-lock.json" \
  CAPTURE_BENCH_BUILD_EVIDENCE="$RUN_DIR/build-evidence.json" \
  CAPTURE_BENCH_FINALIZE_ONLY=1 \
  CAPTURE_BENCH_HOST_EVIDENCE="$HOST_EVIDENCE_DIR/comparison.json" \
  CAPTURE_BENCH_OUTPUT="$RUN_DIR" \
  "$RUN_DIR/capture_admission_evidence-exe" --bench
jq -e -s '.[0].schema_version == 5 and .[0].backend == "candidate" and
  .[0].host_gate.valid == true and .[0].repetitions == [1, 2, 3, 4, 5] and
  .[0].immutable_module_sha256 == .[1].immutable_module_sha256 and
  .[0].entrypoint_sha256 == .[1].entrypoint_sha256 and
  .[0].criterion_sha256 == .[1].criterion_sha256 and
  .[0].observer_sha256 == .[1].observer_sha256 and
  .[0].tool_sha256 == .[1].tool_sha256 and
  .[0].production_library_sha256 == .[1].production_library_sha256 and
  .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
  .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
  .[0].release_profile_sha256 == .[1].release_profile_sha256 and
  .[0].backend_sha256 != .[1].backend_sha256' \
  "$RUN_DIR/manifest.json" "$BASELINE_MANIFEST"
release_host_lock
(cd "$RUN_DIR" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | \
  while IFS= read -r file; do shasum -a 256 "$file"; done > SHA256SUMS)
(cd "$RUN_DIR" && shasum -a 256 -c SHA256SUMS)
test "$(git rev-parse HEAD)" = "$CANDIDATE_HEAD"
test -z "$(git status --short)"
```

The integration owner then creates
`docs/reports/2026-07-17-q2-a4-evidence-lock.json` and
`docs/reports/2026-07-17-q2-a4-verification.md`, and updates current state, target state, gap analysis,
the canonical entry point, project memory, and `docs/verification/usable-release-baseline.md` in the
same reviewed evidence/truth commit. The closed final lock binds both manifest hashes, both checksum-
inventory hashes, executable, immutable modules, entrypoint, distinct backends, and both host-gate
objects. Raw binaries and run data remain ignored under `target/`. This commit completes the required
Task 0 baseline SHA/tree/API/evidence refresh. The Task 1 path/DAG policy stays byte-identical; a
required ownership or DAG change must occur before a new standard freeze and forces a fresh paired
run.

After that exact eight-path truth commit is pushed, run the evidence-reuse and exact-head gate:

```bash
set -euo pipefail
FINAL_HEAD="$(git rev-parse HEAD)"
test -z "$(git status --short)"
BASELINE_LOCK=docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json
BASELINE_REPORT=docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md
FINAL_LOCK=docs/reports/2026-07-17-q2-a4-evidence-lock.json
STANDARD_HEAD="$(jq -er '.baseline_head' "$BASELINE_LOCK")"
COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
REPO_ROOT="$(dirname "$COMMON_GIT_DIR")"
EVIDENCE_ROOT="$REPO_ROOT/target/q2-a4-capture-benchmark"
STANDARD_DIR="$EVIDENCE_ROOT/standard-$STANDARD_HEAD"
STANDARD_MANIFEST="$STANDARD_DIR/manifest.json"
MEASURED_CANDIDATE_HEAD="$(jq -er '.measured_code_head' "$FINAL_LOCK")"
CANDIDATE_DIR="$EVIDENCE_ROOT/candidate-$MEASURED_CANDIDATE_HEAD"
CANDIDATE_MANIFEST="$CANDIDATE_DIR/manifest.json"
git merge-base --is-ancestor "$STANDARD_HEAD" "$MEASURED_CANDIDATE_HEAD"
git merge-base --is-ancestor "$MEASURED_CANDIDATE_HEAD" "$FINAL_HEAD"
test "$(git diff --name-only "$MEASURED_CANDIDATE_HEAD..$FINAL_HEAD")" = \
  $'docs/architecture/current-state.md\ndocs/architecture/target-state.md\ndocs/plans/gap-analysis.md\ndocs/plans/implementation-plan.md\ndocs/project-memory.md\ndocs/reports/2026-07-17-q2-a4-evidence-lock.json\ndocs/reports/2026-07-17-q2-a4-verification.md\ndocs/verification/usable-release-baseline.md'
(cd "$STANDARD_DIR" && shasum -a 256 -c SHA256SUMS)
(cd "$CANDIDATE_DIR" && shasum -a 256 -c SHA256SUMS)
STANDARD_MANIFEST_SHA="$(shasum -a 256 "$STANDARD_MANIFEST" | awk '{print $1}')"
CANDIDATE_MANIFEST_SHA="$(shasum -a 256 "$CANDIDATE_MANIFEST" | awk '{print $1}')"
STANDARD_SUMS_SHA="$(shasum -a 256 "$STANDARD_DIR/SHA256SUMS" | awk '{print $1}')"
CANDIDATE_SUMS_SHA="$(shasum -a 256 "$CANDIDATE_DIR/SHA256SUMS" | awk '{print $1}')"
BASELINE_LOCK_SHA="$(shasum -a 256 "$BASELINE_LOCK" | awk '{print $1}')"
BASELINE_REPORT_SHA="$(shasum -a 256 "$BASELINE_REPORT" | awk '{print $1}')"
jq -e -s \
  --arg standard_head "$STANDARD_HEAD" \
  --arg standard_manifest "$STANDARD_MANIFEST_SHA" \
  --arg candidate_manifest "$CANDIDATE_MANIFEST_SHA" \
  --arg standard_sums "$STANDARD_SUMS_SHA" \
  --arg candidate_sums "$CANDIDATE_SUMS_SHA" \
  --arg baseline_lock "$BASELINE_LOCK_SHA" \
  --arg baseline_report "$BASELINE_REPORT_SHA" \
  '.[2].schema_version == 1 and
   (.[2] | keys | sort) == ["candidate_backend_sha256", "candidate_checksums_sha256",
     "candidate_host_gate", "candidate_manifest_sha256", "entrypoint_sha256",
     "executable_sha256", "immutable_module_sha256", "measured_code_head", "schema_version",
     "standard_backend_sha256", "standard_checksums_sha256", "standard_host_gate",
     "standard_manifest_sha256"] and
   .[2].measured_code_head == .[0].measured_code_head and
   .[2].standard_manifest_sha256 == $standard_manifest and
   .[2].candidate_manifest_sha256 == $candidate_manifest and
   .[2].standard_checksums_sha256 == $standard_sums and
   .[2].candidate_checksums_sha256 == $candidate_sums and
   .[2].immutable_module_sha256 == .[0].immutable_module_sha256 and
   .[2].immutable_module_sha256 == .[1].immutable_module_sha256 and
   .[2].entrypoint_sha256 == .[0].entrypoint_sha256 and
   .[2].entrypoint_sha256 == .[1].entrypoint_sha256 and
   .[2].executable_sha256 == .[0].executable_sha256 and
   .[2].standard_backend_sha256 == .[1].backend_sha256 and
   .[2].candidate_backend_sha256 == .[0].backend_sha256 and
   .[2].standard_backend_sha256 != .[2].candidate_backend_sha256 and
   .[2].standard_host_gate == .[1].host_gate and
   .[2].candidate_host_gate == .[0].host_gate and
   .[1].measured_code_head == $standard_head and
   .[1].backend == "standard" and
   .[1].baseline_manifest_sha256 == null and .[1].baseline_lock_sha256 == null and
   .[0].baseline_manifest_sha256 == $standard_manifest and
   .[0].baseline_lock_sha256 == $baseline_lock and
   .[0].criterion_sha256 == .[1].criterion_sha256 and
   .[0].observer_sha256 == .[1].observer_sha256 and
   .[0].tool_sha256 == .[1].tool_sha256 and
   .[0].production_library_sha256 == .[1].production_library_sha256 and
   .[0].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
   .[0].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
   .[0].release_profile_sha256 == .[1].release_profile_sha256 and
   (.[3] | keys | sort) == ["approval_identity", "approval_state", "artifact_sha256",
     "backend", "backend_sha256", "baseline_head", "build_evidence_sha256",
     "criterion_sha256", "entrypoint_sha256", "host_fingerprint_sha256",
     "immutable_module_sha256", "manifest_reference", "manifest_sha256", "observer_sha256",
     "queue_private_storage_accounting", "queue_transport", "release_profile_sha256",
     "repetition_sha256", "report_reference", "report_sha256", "schema_version", "state",
     "tool_sha256", "toolchain_fingerprint_sha256"] and
   .[3].schema_version == 5 and .[3].state == "frozen_standard_baseline" and
   .[3].approval_state == "independent_seed_review_approved" and
   .[3].approval_identity == "q2-a4-seed-checkpoint-review" and
   .[3].baseline_head == $standard_head and
   .[3].backend == .[1].backend and
   .[3].queue_transport == .[1].queue_transport and
   .[3].queue_private_storage_accounting == .[1].queue_private_storage_accounting and
   .[3].manifest_sha256 == $standard_manifest and
   .[3].manifest_reference ==
     ("target/q2-a4-capture-benchmark/standard-" + $standard_head + "/manifest.json") and
   .[3].report_reference ==
     "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md" and
   .[3].report_sha256 == $baseline_report and
   .[3].immutable_module_sha256 == .[1].immutable_module_sha256 and
   .[3].entrypoint_sha256 == .[1].entrypoint_sha256 and
   .[3].criterion_sha256 == .[1].criterion_sha256 and
   .[3].observer_sha256 == .[1].observer_sha256 and
   .[3].tool_sha256 == .[1].tool_sha256 and
   .[3].artifact_sha256 == .[1].artifact_sha256 and
   .[3].repetition_sha256 == .[1].repetition_sha256 and
   .[3].backend_sha256 == .[1].backend_sha256 and
   .[3].build_evidence_sha256 == .[1].build_evidence_sha256 and
   .[3].host_fingerprint_sha256 == .[1].host_fingerprint_sha256 and
   .[3].toolchain_fingerprint_sha256 == .[1].toolchain_fingerprint_sha256 and
   .[3].release_profile_sha256 == .[1].release_profile_sha256' \
  "$CANDIDATE_MANIFEST" "$STANDARD_MANIFEST" "$FINAL_LOCK" "$BASELINE_LOCK"
RELATIVE_EXE="$(jq -er '.executable_path' "$CANDIDATE_MANIFEST")"
case "$RELATIVE_EXE" in ./*) ;; *) exit 1 ;; esac
CANDIDATE_EXE="$CANDIDATE_DIR/${RELATIVE_EXE#./}"
test -x "$CANDIDATE_EXE"
test "$(shasum -a 256 "$CANDIDATE_EXE" | awk '{print $1}')" = \
  "$(jq -er '.executable_sha256' "$FINAL_LOCK")"
./scripts/verify.sh
./scripts/check_capture_queue_loom.sh
cargo deny check
cargo audit --deny warnings
gitleaks dir --no-banner --redact --config .gitleaks.toml .
gitleaks git --no-banner --redact --config .gitleaks.toml .
test "$(git rev-parse HEAD)" = "$FINAL_HEAD"
test "$(git rev-parse '@{upstream}')" = "$FINAL_HEAD"
test -z "$(git status --short)"
```

The complete exact-head gate runs again. The independent prerequisite reviewer verifies the
standard/candidate threshold result, all immutable equality/difference constraints, both checksum
inventories, the final lock, truthful coverage, and the unchanged clean pushed head. Findings force
remediation and a fresh paired run when code, harness, fixture, tool, profile, or governed environment
changed. Only that independently approved exact head unlocks Task 2 dispatch and Wave 1 integration;
the approval is not another quarter checkpoint.

---

## Release capability truth

| Vertical | Producer | Required terminal consumer | Closing task |
| --- | --- | --- | --- |
| Production live | Coinbase and Kraken adapters | current shard -> feature -> strategy -> risk -> paper | 2, 6 |
| Local files | CSV/TSV/JSON/NDJSON/XML/Excel/SQLite/export/OFX/Parquet adapters | manifest-pinned query/PIT | 7, 11 |
| Filings | SEC submissions/filings/XBRL/Company Facts | fundamentals/PIT/analysis | 8, 11, 12 |
| Macro | FRED/ALFRED, BLS, Treasury | revisions/PIT/yield/surprise analysis | 9, 11, 12 |
| Research storage | canonical observations | SQLite/Arrow/Parquet/DataFusion services | 3, 4 |
| PIT datasets | manifest-bound observations/universes | feature/label datasets/model/backtest | 11 |
| Analytics | market/fundamental/macro/portfolio observations | Rust/Python/portfolio/model services | 12, 14, 16 |
| Python product | manifest-bound PIT data and Rust kernels | training/evaluation/model-bundle handoff | 14 |
| Modeling | feature registry and training artifact | native/ONNX inference and no-action failure | 13, 15 |
| Backtesting | PIT datasets and strategy/model | reconciled portfolio experiment output | 17 |
| Portfolio | raw portfolio imports | accounting/performance/risk/MCP | 10, 16 |
| Execution | complete intent/current authority | comprehensive risk/realistic paper/audit | 2 |
| Fair value | market/research/portfolio evidence | classification/evidence/approval services | 18 |
| Control plane | every bounded service | complete CLI and typed local stdio MCP | 5, 19 |
| Release | all verticals | deterministic local demo and exact-head evidence | 20 |

Dataset registration is atomic with a working writer and reader. The data crate supplies the
versioned extension/publication mechanism in Task 4, but it must not pre-create empty schemas for
later tasks. The integration owner enforces this first-producer/terminal-consumer map:

| Dataset families | First working writer | Required reader/evidence |
| --- | --- | --- |
| instruments, instrument_identifiers, venues | 3 registry plus 7-10 resolution | 4 query and 11 PIT identity/history |
| trades, quotes, order_books | 2/6 asynchronous live audit publication | 12 market analytics and 19 Market services |
| corporate_actions | 7/8/11 normalized action ingestion | 11 PIT policy, 16 accounting, 17 backtest |
| filings, xbrl_facts, financial_statements, fundamentals | 8 SEC ingestion/normalization | 11 PIT, 12 analytics, 19 Fundamental services |
| macro_series, macro_observations | 9 macro providers | 11 PIT, 12 yield/surprise analytics, 19 Macro services |
| accounts, positions, transactions, cash_flows | 10 import, then 16 authoritative revisions | 16 reconciliation/risk and 19 Portfolio services |
| features, labels | 11 reproducible dataset builder | 13 model bundles, 14 Python, 17 backtest |
| predictions, models | 13/14 validated model publication | 15 inference, 17 backtest, 19 Model services |
| strategies | 2 strategy registry with version/hash | 2 risk/audit and 17 experiment lineage |
| orders, fills, risk_decisions | 2 realistic paper/audit writer | 16 reconciliation and 19 Execution services |
| valuations, fair_value_evidence | 18 measurement/rules workflow | 18 approval and 19 FairValue/Analysis services |
| quality_results, lineage | each producer through 3/4 common commit | 11 PIT, 19 source/query domains, 20 evidence gate |

Every row stores schema/source/quality/provenance, relevant time semantics and content identity. Task
20 rejects a dataset family whose named producer or reader is absent, fixture-only, schema-only, or
unbound to a committed manifest.

## Dependency-safe wave and ownership table

One worktree owns a cohesive lane, not one small task. The integration owner publishes the exact
path set before dispatch and merges in the order shown.

| Stage / wave | Ready tasks and grouped lanes | Start barrier | Exclusive ownership | Merge order | Required Wave close |
| --- | --- | --- | --- | --- | --- |
| Stage 0 / Wave 0 | 0 truth refresh; 1 DAG/dependency governance | clean audited product-code anchor; independent research/policy artifacts require an approved-head refresh before Stage 1 credit | docs/policy and integrator hotspots | 0 then 1 | reviewed dependency policy and locked full-workspace gate |
| Stage 1 / Wave 1A | 2 live/risk/paper; 3 catalog/secrets; 5 MCP transport | Task 1 frozen interfaces; Tasks 3/5 may develop provisionally on the refreshed audit anchor, while Task 2 integration and all Wave credit wait for the approved live/capture head | execution/adapters; platform/data; MCP | 3, 5, then 2 exact integration | one reviewed lock resolution; Tasks 2/3/5 focused `--locked` reruns; locked Wave gate |
| Stage 1 / Wave 1B | 4 data storage/query; 6 Kraken | Task 3 catalog and Task 2 live interfaces frozen | data only; Kraken only | 4 then 6 | one reviewed lock resolution; Tasks 4/6 focused `--locked` reruns; locked Wave gate |
| Stage 2 / Wave 2 | 7 files; 8 SEC; 9 macro; 10 portfolio import | Task 4 ingest contract frozen | disjoint adapter crates | 7, 8, 9, 10 | one reviewed lock resolution; Tasks 7-10 focused `--locked` reruns; locked Wave gate |
| Stage 2 / Wave 3 | 11 PIT/research composition; 12 analytics | provider lanes and data manifests merged | data/app research; analytics | 11 then 12 | one reviewed lock resolution; Tasks 11/12 focused `--locked` reruns; locked Wave gate |
| Stage 3 / Wave 4A | 13 bundle/native; 14 Python; 16 portfolio | PIT and analytics APIs frozen | modeling; python; portfolio | 13, 16, then 14 | one reviewed lock resolution; Tasks 13/14/16 focused `--locked` reruns; locked Wave gate |
| Stage 3 / Wave 4B | 15 ONNX; 17 backtest; 18 fair value | bundle/portfolio/data interfaces frozen | modeling ONNX; backtesting crate; valuation | 15, 17, 18 | one reviewed lock resolution; Tasks 15/17/18 focused `--locked` reruns; locked Wave gate |
| Stage 4 / Wave 5 | 19 services/CLI/MCP domains | all domain services merged | app/MCP/CLI serialized composition | last implementation merge | one reviewed lock resolution; Task 19 focused `--locked` rerun; locked full workspace gate |
| Stage 5 / Wave 6 | 20 demo/hardening/review/publication | clean integrated candidate | benches/fuzz/docs/evidence, then frozen review | terminal | unchanged reviewed lock; all locked release gates at each freeze |

## Four delivery-quarter checkpoints

The word *quarter* refers only to these four production-weighted delivery groups. It does not imply
equal elapsed time, a calendar quarter, or permission to create Q5 and later groups. Stage and Wave
remain the fine-grained dependency/parallelization coordinates inside each quarter.

| Quarter checkpoint | Included work | Integrated outcome frozen for grouped review |
| --- | --- | --- |
| Quarter 1 of 4 | Tasks 0-6; Stages 0-1; Waves 0-1B | truthful baseline, governed dependencies, production live/risk/paper path, catalog/data/MCP foundations, and Kraken vertical |
| Quarter 2 of 4 | Tasks 7-12; Stage 2; Waves 2-3 | required file/SEC/macro/portfolio ingestion, research composition, PIT datasets, and Rust analytics |
| Quarter 3 of 4 | Tasks 13-18; Stage 3; Waves 4A-4B | model bundles, native/ONNX inference, Python product, portfolio accounting, backtesting, and fair value |
| Quarter 4 of 4 | Tasks 19-20; Stages 4-5; Waves 5-6 | complete shared services/CLI/MCP plus demo, fuzz, measurement, security, publication, and usable-release evidence |

At every quarter checkpoint the integration owner freezes one clean exact commit, runs the complete
applicable locked gate, and dispatches fresh non-mutating specialist reviewers in maximum parallel
batches. Reviewers receive the same commit and return findings without editing it. The integrator
unions and deduplicates the findings into one concise checkpoint record, remediates every
substantiated Critical, Important, and Minor finding, reruns the complete gate, and obtains fresh
read-only approval of the unchanged replacement head. Only an approved quarter head may start the
next quarter. The approved commit is pushed and reported on the active pull request, and completed
clean lane worktrees are removed. Focused lane and Wave gates remain mandatory but do not substitute
for a quarter checkpoint or trigger per-task independent review.

If four agent slots are available, the integration owner occupies one slot and dispatches at most
three disjoint writers. A lane blocked on a shared file sends an exact patch request to the integration
owner and continues only on its owned files. No two live writers touch the same worktree.

## Planned file ownership

```text
domain       crates/market-squawk-domain
platform     crates/market-squawk-platform
sources      crates/market-squawk-sources
live         crates/market-squawk-live
data         crates/market-squawk-data
analytics    crates/market-squawk-analytics
services     crates/market-squawk-services
modeling     crates/market-squawk-modeling
portfolio    crates/market-squawk-portfolio
backtesting  crates/market-squawk-backtesting
execution    crates/market-squawk-execution
valuation    crates/market-squawk-valuation
mcp          crates/market-squawk-mcp
coinbase     adapters/market-squawk-adapter-coinbase
kraken       adapters/market-squawk-adapter-kraken
sec          adapters/market-squawk-adapter-sec
fred         adapters/market-squawk-adapter-fred
bls          adapters/market-squawk-adapter-bls
treasury     adapters/market-squawk-adapter-treasury
files        adapters/market-squawk-adapter-files
port_import  adapters/market-squawk-adapter-portfolio
paper        adapters/market-squawk-adapter-paper
python       python/market_squawk, python/tests, python/pyproject.toml, python/requirements.lock
python_bind  crates/market-squawk-python
composition  apps/market-squawk
```

No package is created until the owning task includes its first production consumer and tests.
Every `Cargo.toml` path named in a task below is a serialized integration-owner action, even when it
appears beside lane-owned source files. A writer may use the root-provided provisional manifest in
its isolated worktree, but it never stages or commits that manifest. The integration owner likewise
owns the data migration registry and its ordered digest set; a data writer owns only the SQL bodies.

At the end of every Wave, each lane writes its exact manifest requests and generated-lock disposition
to the ignored handoff record named in `usable-release-path-ownership.json`. The integration owner
rejects a handoff with a staged/dirty `Cargo.lock`, undeclared dependency, unreviewed feature/default,
or network-fetched native artifact. After the single lock commit, workers may rerun in read-only
worktrees, but only integration-owner `--locked` results and the exact Wave head count toward release
evidence.

Every lane that creates a crate or changes dependency requests uses this two-phase command contract.
Its first dependency-resolving RED/GREEN commands deliberately omit `--locked` and are provisional.
Before the explicit literal-path lane commit, save the generated-lock diff only as ignored handoff
evidence, then restore the owner-excluded lock and prove it equals the lane base.
This required pre-staging sequence applies to every such task below even when the common commands are
not repeated in that task's focused command block:

```bash
git diff -- Cargo.lock > .agents/tmp/generated-lock-disposition.patch
git restore --source=HEAD --staged --worktree -- Cargo.lock
git diff --exit-code -- Cargo.lock
```

This restore is authorized only for a lane-generated `Cargo.lock` after the diff is preserved; it
must stop if the lock had pre-existing/user edits. The integrator then cherry-picks the clean lane
commit, applies the reviewed manifest set, runs one minimal lock resolution, commits the lock diff,
and reruns every lane's exact focused commands with `--locked`. A task's `Expected:` paragraph means
the post-merge locked result, never the provisional lane run. README capability state and release
evidence change only in that integration commit.

---

### Task 0: Refresh the audited baseline and current release truth

**Files:**

- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `docs/project-memory.md`
- Modify: `docs/architecture/current-state.md`
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Delete: `docs/superpowers/specs/2026-07-17-market-squawk-usable-release-truth-contract-design.md`
- Modify: `docs/superpowers/plans/2026-07-16-market-squawk-complete-remaining-work.md`
- Modify: `docs/superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md`
- Modify: `docs/superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md`
- Create: `docs/verification/usable-release-baseline.md`
- Delete: `scripts/check_brand.py`
- Delete: `scripts/check_duplicate_dependencies.py`
- Delete: `scripts/tests/test_check_brand.py`
- Delete: `scripts/tests/test_ci_pins.py`
- Delete: `scripts/tests/test_credential_policy.py`
- Delete: `scripts/tests/test_dependency_policy.py`
- Delete: `scripts/tests/test_documentation_contracts.py`
- Delete: `scripts/tests/test_duplicate_dependencies.py`
- Delete: `scripts/tests/test_repository_governance_policy.py`
- Delete: `scripts/tests/test_smoke_mcp.py`
- Delete: `scripts/tests/test_verify_script.py`
- Modify: `scripts/verify.sh`

**Interfaces:**

- Consumes: the clean audited live/capture anchor, its pending approval evidence, current tracked
  product code, and the approved usable-release scope. Before Stage 1 integration or Wave credit,
  paths, APIs and evidence must be refreshed against the approved live/capture prerequisite head.
- Produces: an honest README capability inventory, a dated exact baseline, one canonical plan
  authority, a usable complete-release terminal condition, and a lean verification suite that
  protects product/build/security behavior rather than documentation wording.

- [x] **Step 1: Refresh current capability truth**

Update the README to distinguish `Runnable now`, `Required but missing`, and
`Release blocked until implemented`. Name every mandatory producer-to-consumer vertical and its
closing task. Correct the diagnostic capture receipt, current-authority, five-tool MCP, adapter,
Python-product, and Parquet-compaction descriptions. Preserve zero-cost, no-evasion, journal,
coverage, security, and financial-use warnings.

- [x] **Step 2: Remove competing delivery authority**

Replace `docs/plans/implementation-plan.md` with the stable canonical entry point. Mark the dated
Q5-Q7 remaining-work plan and old Q3 plan superseded with no current execution authority. Preserve
historical IDs as audit locators. Replace the halfway stop in project memory with the usable
complete-release terminal and use Stage, Wave, and Release Gate for current work.

- [x] **Step 3: Record the exact baseline**

Record product SHA/tree/branch, toolchain, workspace members and dependency edges, tracked
adapter/Python-package state, runnable diagnostic capabilities, missing product planes, public
interfaces consumed by subsequent tasks, verification evidence, and review disposition in
`usable-release-baseline.md`. Mark stale current-state and gap documents as historical until the
approved live/capture prerequisite head is integrated and those detailed audits are refreshed.

- [x] **Step 4: Remove prose-policy automation**

Delete branding, documentation-contract, plan-label, governance-template, verification-wrapper,
and configuration-snapshot tests. Remove the exact CLI-help sentence assertion. Keep checks that
exercise actual workspace boundaries, dependency graphs, generated or secret artifacts, immutable
CI actions, MCP protocol behavior, concurrency models, and locked Rust builds/tests. Run Cargo Deny,
Cargo Audit, and Gitleaks themselves instead of unit-testing fragments of their configuration. Do
not replace the deleted machinery with another README parser, capability-ledger checker, or command-
string snapshot.

- [x] **Step 5: Verify and review**

```bash
./scripts/verify.sh
git diff --check
```

Inspect relative links and the exact changed-file inventory directly. Integrate Task 0 into the
Quarter 1 candidate; fresh independent review is grouped at the Quarter 1 checkpoint rather than
repeated for this ordinary task.

### Task 1: Freeze the dependency DAG, current research, and path ownership

**Files:**

- Create: `docs/verification/usable-release-path-ownership.json`
- Create: `docs/research/2026-07-18-usable-release-dependencies.md`
- Create: `docs/research/2026-07-18-usable-release-traceability.md`
- Modify: `scripts/check_workspace_boundaries.py`

**Interfaces:**

- Consumes: Task 0 exact inventory, official dependency/provider/MCP/runtime research, existing Q3
  detailed plan, and the provisional Q4 plan.
- Produces: a closed task-to-path ownership map for parallel work, an acyclic crate dependency
  allowlist, and a deduplicated requirement/producer/consumer/evidence/source-link matrix.

- [x] **Step 1: Inspect the actual dependency graph**

Use `cargo metadata --locked --all-features` and the current public APIs. Confirm this allowed local
crate graph and reject any proposed hot-path edge to data, MCP, Python, or provider adapters:

```text
domain -> none
analytics -> domain
platform -> domain
sources -> domain, platform
live -> domain, sources, analytics
data -> domain, platform, sources
services -> domain, platform
modeling -> domain, analytics, data
portfolio -> domain, analytics, data
backtesting -> domain, analytics, data, modeling, portfolio, execution
execution -> domain, live, analytics, modeling, portfolio
valuation -> domain, analytics, data, portfolio
mcp -> domain, platform, services
python bindings -> domain, analytics
provider adapter normal/build edges -> declared domain/source/platform contracts, never data
explicit package-local dev verticals -> Kraken to execution/paper; files and portfolio to data
paper adapter -> domain, execution, platform confinement contracts
app -> composition dependencies
```

- [x] **Step 2: Refresh dependency and provider research**

Use current official sources. Record exact compatible versions, enabled features, licenses, Rust and
Python floors, native artifacts, transitive risks, provider rights, coverage, quotas, retrieval date,
and stable fallback. Reject dependencies incompatible with Rust 1.97.1, local zero-mandatory-cost
operation, or the no-evasion boundary.

- [x] **Step 3: Freeze parallel ownership**

Record every Task 0-20 path in `usable-release-path-ownership.json`. A complete conflict-isolated
lane owns a cohesive group of files, not one worktree per checklist item. Shared manifests,
`Cargo.lock`, application composition, authority-critical files, and release evidence remain
serialized under the integration owner. Review the map directly for overlaps before starting a Wave.

- [x] **Step 4: Verify the real policies**

```bash
cargo metadata --locked --all-features --format-version 1 > /dev/null
python3 scripts/check_workspace_boundaries.py
cargo deny check
cargo audit --deny warnings
gitleaks dir --redact --no-banner .
gitleaks git --redact --no-banner
git diff --check
```

Do not add a parser that tests plan headings, checkboxes, task labels, or wording. Do not add a
custom staging wrapper. At execution time, enumerate every approved repository-relative path
literally in the `git add --` command from the reviewed ownership map.


### Task 2: Complete production live features, Coinbase, risk, and realistic paper execution

**Files:**

- Refresh/execute: `docs/superpowers/plans/2026-07-16-market-squawk-q3-production-plan.md`
- Create: `docs/verification/stage-live-execution-path-ownership.json`
- Create/modify: every exact product, test, and benchmark path enumerated by
  `stage-live-execution-path-ownership.json`, including stable modules
  `crates/market-squawk-domain/src/order.rs`,
  `crates/market-squawk-analytics/src/registry.rs`,
  `crates/market-squawk-live/src/action.rs`,
  `crates/market-squawk-execution/src/risk.rs`,
  `crates/market-squawk-execution/src/dispatcher.rs`,
  `adapters/market-squawk-adapter-coinbase/src/decoder.rs`,
  `adapters/market-squawk-adapter-coinbase/src/source.rs`,
  `adapters/market-squawk-adapter-paper/src/adapter.rs`,
  `adapters/market-squawk-adapter-paper/src/state.rs`,
  `apps/market-squawk/src/services.rs`, and
  `apps/market-squawk/tests/end_to_end_authority.rs`
- Create: `docs/verification/stage-live-execution.md`

**Interfaces:**

- Consumes: approved live/capture closure, Task 1 ownership/DAG, registry/capture/book/shard/snapshot
  contracts, and opaque `LiveExecutionCapability`.
- Produces: refreshed Q3 plan's exact `FeatureDefinition`, `OrderIntent`,
  `RiskCoordinator::evaluate`, privately constructible `ApprovedOrder`, single-use `DispatchOrder`,
  `ExecutionAdapter`, `ApplicationServices`, production Coinbase `LiveMarketSource`, and realistic
  paper order/account/reconciliation interfaces.

- [ ] **Step 1: Refresh the detailed Q3 plan**

Replace its audit base, active quarter wording, stale files/signatures, dependency pins, exact test
counts and ownership table. Preserve all 19 tasks and every authority, memory, feature, risk, paper,
fuzz, performance and review contract, but transfer creation/execution of fuzz targets to Task 20;
Task 2 owns only parser property tests and committed seed fixtures. Materialize every exact Task-2
path from the detailed plan into
`stage-live-execution-path-ownership.json`; the refreshed Q3 plan plus that closed JSON are normative
inputs to this master plan. Review the two artifacts directly for missing paths, globs/directories,
lane overlap, and shared hotspots before dispatching workers.

- [ ] **Step 2: Execute all detailed Q3 RED/GREEN commits**

Use subagent-driven development on the refreshed DAG. Required RED evidence includes absent complete
order identities/types, feature registry, risk reservations, production Coinbase current-batch
adapter, route-owned feature state, private approval/dispatch boundary, complete paper state machine,
shared app services and compile-fail bypass tests. Each lane runs its detailed focused test/clippy/
diff gate before handoff.

- [ ] **Step 3: Prove live-to-paper behavior**

Run the production Coinbase parser against a deterministic local server through source metadata,
shared budget, capture, exact decoding, current ingress, features, strategy, risk, one-use dispatch
and paper execution. Evaluate Coinbase channel/profile evidence against every `DirectVerified`
predicate; either prove and bind the exact approved qualification outcome or cap it below execution
and keep verified-action release evidence open. Never promote it merely because it is a direct
connection. Diagnostic values cannot enter production. Prove accepted/partial/filled/cancel-pending/canceled/rejected/expired,
fees, seeded latency, depth/slippage/impact, reservations, balances/positions, recovery and audit.

- [ ] **Step 4: Freeze focused interfaces and commit**

```bash
cargo fmt --all --check
cargo clippy -p market-squawk-domain -p market-squawk-live \
  -p market-squawk-analytics -p market-squawk-execution \
  -p market-squawk-adapter-coinbase -p market-squawk-adapter-paper \
  -p market-squawk --all-targets --all-features --locked -- -D warnings
cargo test -p market-squawk-domain -p market-squawk-live \
  -p market-squawk-analytics -p market-squawk-execution \
  -p market-squawk-adapter-coinbase -p market-squawk-adapter-paper \
  -p market-squawk --all-features --locked
git diff --check
git commit -m "feat(execution): complete production risk and paper vertical"
```

Expected: all focused gates pass and the clean commit exposes the frozen interfaces needed by Tasks
6, 16, 17 and 19. Stage approval still waits for integrated exact-head review.

### Task 3: Implement the SQLite catalog, durable registries, secrets, and rights admission

**Files:**

- Integration owner create: `crates/market-squawk-data/Cargo.toml`
- Create: `crates/market-squawk-data/src/lib.rs`
- Create: `crates/market-squawk-data/src/catalog.rs`
- Create: `crates/market-squawk-data/src/catalog/publication.rs`
- Create: `crates/market-squawk-data/src/catalog/records.rs`
- Create: `crates/market-squawk-data/src/catalog/runs.rs`
- Create: `crates/market-squawk-data/src/catalog/storage.rs`
- Create: `crates/market-squawk-data/src/catalog/types.rs`
- Integration owner create: `crates/market-squawk-data/src/migrations.rs`
- Create: `crates/market-squawk-data/src/rights.rs`
- Create: `crates/market-squawk-data/tests/catalog.rs`
- Create: `crates/market-squawk-data/migrations/0001_control.sql`
- Create: `crates/market-squawk-data/migrations/0002_instruments.sql`
- Integration owner modify: `crates/market-squawk-platform/Cargo.toml`
- Modify: `crates/market-squawk-platform/src/lib.rs`
- Modify: `crates/market-squawk-platform/src/authority_state.rs`
- Create: `crates/market-squawk-platform/src/authority_state/envelope.rs`
- Create: `crates/market-squawk-platform/src/authority_state/filesystem.rs`
- Create: `crates/market-squawk-platform/src/authority_state/recovery.rs`
- Modify: `crates/market-squawk-platform/src/paths.rs`
- Create: `crates/market-squawk-platform/src/paths/catalog.rs`
- Create: `crates/market-squawk-platform/src/secrets.rs`
- Create: `crates/market-squawk-platform/src/secrets/crypto.rs`
- Create: `crates/market-squawk-platform/src/secrets/encrypted.rs`
- Create: `crates/market-squawk-platform/src/secrets/keyring.rs`
- Modify: `crates/market-squawk-platform/tests/authority_state.rs`
- Create: `crates/market-squawk-platform/tests/secrets.rs`

**Interfaces:**

- Consumes: domain instrument/provenance/time values, source extraction/rights contracts, controlled
  platform paths, Task 1 dependency pins.
- Produces:

```rust
pub struct Catalog;
pub struct CatalogAuthority;
pub struct IngestReservation;
pub struct RegisteredRightsGrant;
pub trait SecretStore {
    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), SecretError>;
    fn load(&self, key: &SecretKey) -> Result<SecretValue, SecretError>;
}
impl CatalogAuthority {
    pub fn open(config: CatalogConfig) -> Result<Self, CatalogError>;
    pub fn catalog(&self) -> &Catalog;
    pub fn admit_source_rights(
        &self,
        command: RightsRegistrationCommand,
    ) -> Result<RegisteredRightsGrant, CatalogError>;
}
impl Catalog {
    pub fn reserve_ingest(
        &self,
        request: &IngestIdentity,
        grant: &RegisteredRightsGrant,
    ) -> Result<IngestReservation, CatalogError>;
}
```

Rights bind source, payload digest, retrieval time, exact terms URL/digest, authorization evidence/
expiry and permitted retrieve/display/persist/cache/redistribute/train operations.

- [ ] **Step 1: Extend only the thin catalog/rights, secret-store, and existing authority-state
      integrations with critical behavioral proofs**

Test `foreign_keys=ON`, `trusted_schema=OFF`, `synchronous=FULL`, bounded busy timeout, local-only WAL,
digest-bound ordered migrations, integrity/foreign-key checks, one writer, crash restart, backup/
restore, idempotency conflict, durable instruments/identifiers/symbol history/mergers/delistings/rolls/
corporate actions, operation mismatch/expiry, keyring success, and Argon2id + XChaCha20-Poly1305
fallback whose unlock secret is never stored beside ciphertext. Do not add another test file or
near-duplicate test case. Extend the existing catalog sequence once for durable rights resolution,
reserve -> publish -> restart -> resume -> semantic replay -> complete, and one oversized-result
budget failure. Extend the existing secret sequence once for stable two-slot retirement and
single-slot recovery, whole-state tamper rejection, and interrupted-peer repair before ordinary
access. Update the existing authority-state assertions only where the generational on-disk contract
changes.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path crates/market-squawk-data/Cargo.toml --test catalog
cargo test -p market-squawk-platform --all-features --locked
```

Expected: FAIL because the data package, migrations, catalog and secret provider do not exist.

- [ ] **Step 3: Implement strict catalog and rights admission**

Use prepared statements, checked transactions, a process-local non-clone writer permit, row and byte
bounded queries/results, a lowered SQLite value-length limit, exact UTC nanoseconds, rollback-
rejecting catalog authority time, immutable audits and typed conflicts. Application composition
alone receives the non-clone `CatalogAuthority`; its rights registrar remains private and sealed
inside that writer-open owner. Adapters provide untrusted evidence but cannot construct an admitted
grant. Resolve the durable private grant inside the reservation transaction and evaluate expiry
against catalog-observed admission time. Extraction output cannot grant itself persistence. Crash
recovery must return a freshly sealed reservation
only after validating the retained run and rights row, recover existing publication metadata, and
make semantic artifact/manifest replay idempotent across restart. Backup errors never promote
visible content to durable success and carry a versioned path-free receipt for reconciliation.
Cursor monotonicity is deletion-safe.
Persist instruments, identifiers, venues, symbol history, corporate actions, source configuration,
cursors, runs, manifests, audit and artifact metadata needed by Tasks 4-19.

- [ ] **Step 4: Implement secret providers and rotation**

Prefer OS keyring. The fallback records Argon2id parameters/random salt, uses a unique nonce and
XChaCha20-Poly1305 authenticated version/key-name metadata, and publishes below the confined secret
root. Authority state uses two bounded authenticated generations and Windows no-clobber write-
through publication; it never relies on in-place replacement or an unproven atomic-visibility claim.
Opening selects the highest valid linked generation while retaining one known-good predecessor.
Every acknowledged logical write is verified in both slots, and rotation is complete only after the
final stable vault is verified in both slots and neither retains prior-unlock recovery ciphertext.
A keyed whole-vault authenticator binds version, phase, set roles, and complete canonical entry
membership plus the sealed next authority generation/predecessor supplied before serialization;
prepared and committed phases are independently verifiable under both permitted unlocks. Per-entry
AEAD is not treated as proof of the surrounding vault state. An interrupted peer-slot installation
latches ordinary access behind typed recovery until the verified newer slot repairs its peer. The
local-only threat contract detects whole-state tampering but does not claim resistance to hostile
replay of an entire older valid two-slot pair without an independent monotonic anchor. The KDF uses
a caller-owned `Zeroizing<Vec<argon2::Block>>` arena, zeroizes plaintext/derived keys, and never uses
Argon2's convenience allocator because that does not wipe the complete memory arena.
`Debug`, `Display`, tracing and MCP never reveal the value, path token or key identity.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-data/Cargo.toml --all-features --locked
cargo test -p market-squawk-platform --all-features --locked
cargo clippy --manifest-path crates/market-squawk-data/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
cargo clippy -p market-squawk-platform --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(data): add durable local catalog and rights admission"
```

Expected: a real catalog consumer opens, migrates, writes, restarts and reads; all gates pass; this
lane does not edit any Cargo manifest, the migration registry, or the lockfile.

### Task 4: Implement Arrow schemas, immutable Parquet publication, and bounded DataFusion

**Files:**

- Modify: `crates/market-squawk-data/Cargo.toml`
- Modify: `crates/market-squawk-data/src/lib.rs`
- Create: `crates/market-squawk-data/src/schema.rs`
- Create: `crates/market-squawk-data/src/arrow_convert.rs`
- Create: `crates/market-squawk-data/src/manifest.rs`
- Create: `crates/market-squawk-data/src/parquet_store.rs`
- Create: `crates/market-squawk-data/src/ingest.rs`
- Create: `crates/market-squawk-data/src/query.rs`
- Create: `crates/market-squawk-data/tests/arrow_roundtrip.rs`
- Create: `crates/market-squawk-data/tests/publication_recovery.rs`
- Create: `crates/market-squawk-data/tests/query_limits.rs`
- Create: `crates/market-squawk-data/tests/compaction.rs`

**Interfaces:**

- Consumes: Task 3 catalog/rights reservation, canonical `ResearchObservation`, controlled artifact
  paths, exact schema/provenance values.
- Produces:

```rust
pub trait ResearchIngestService {
    async fn ingest(
        &self,
        reservation: IngestReservation,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;
}
pub struct DatasetManifestRef {
    dataset_id: DatasetId,
    manifest_version: u64,
    content_hash: Sha256Digest,
}
pub struct QueryLimits {
    max_rows: u64,
    max_bytes: u64,
    max_memory_bytes: u64,
    deadline: std::time::Duration,
}
pub trait ResearchQueryService {
    async fn query(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<QueryResult, QueryError>;
}
```

- [ ] **Step 1: Write RED schema/publication/query tests**

Cover Arrow Decimal128/currency/scale/time/provenance/revision/supersession metadata; invalid schema
versions; no lossy decimal conversion; object-before-manifest crash points; idempotent same-key same-
digest return; same-key different input conflict; orphan quarantine/grace collection; reader manifest
pinning; compaction row/hash/lineage/revision invariance; DataFusion row/byte/memory/deadline/
cancellation; forbidden writes/DDL/extension/network/filesystem UDFs; and small-file ceilings.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path crates/market-squawk-data/Cargo.toml \
  --test arrow_roundtrip --test publication_recovery --test query_limits --test compaction
```

Expected: FAIL because schema conversion, immutable publication and query services are absent.

- [ ] **Step 3: Implement versioned Arrow and crash-safe Parquet publication**

Convert only request-bound canonical observations. Write bounded Parquet objects to a confined
staging directory, close/fsync/hash exact bytes, publish once at
`objects/sha256/<prefix>/<digest>.parquet`, fsync created directories, then `BEGIN IMMEDIATE` and
atomically commit a complete immutable manifest generation. Readers pin a committed generation and
never infer completeness from directory listings. Compaction creates a new generation and never
mutates an object.

- [ ] **Step 4: Implement bounded DataFusion confinement**

Register only manifest-pinned tables, allow `SELECT`/CTE/subquery/explain against allowlisted schemas,
reject DDL/DML/copy/external table/UDF/extension statements, cap plans/partitions/rows/bytes/memory/
wall time, propagate cancellation, and return inline data only below the service threshold; larger
results use Task 3 artifact metadata and controlled content hashes.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-data/Cargo.toml --all-features --locked
cargo clippy --manifest-path crates/market-squawk-data/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff --check
git commit -m "feat(data): publish immutable analytical datasets"
```

Expected: every crash point recovers to either the prior complete manifest or the new complete
manifest; queries remain bounded; live packages have no analytical dependency.

### Task 5: Implement the bounded MCP protocol crate over abstract services

**Files:**

- Integration owner create: `crates/market-squawk-services/Cargo.toml`
- Create: `crates/market-squawk-services/src/lib.rs`
- Create: `crates/market-squawk-services/src/request.rs`
- Create: `crates/market-squawk-services/src/response.rs`
- Create: `crates/market-squawk-services/src/traits.rs`
- Integration owner create: `crates/market-squawk-mcp/Cargo.toml`
- Create: `crates/market-squawk-mcp/src/lib.rs`
- Create: `crates/market-squawk-mcp/src/framing.rs`
- Create: `crates/market-squawk-mcp/src/protocol.rs`
- Create: `crates/market-squawk-mcp/src/server.rs`
- Create: `crates/market-squawk-mcp/src/limits.rs`
- Create: `crates/market-squawk-mcp/src/audit.rs`
- Create: `crates/market-squawk-mcp/src/artifact.rs`
- Create: `crates/market-squawk-mcp/tests/lifecycle_protocol.rs`
- Create: `crates/market-squawk-mcp/tests/hostile_boundaries.rs`

**Interfaces:**

- Consumes: Task 1 rmcp decision and frozen abstract audit/artifact metadata contracts; existing
  bounded compatibility framing tests. It does not wait for Task 3's implementation.
- Produces:

```rust
pub struct McpServer<S: ToolServices>;
pub trait ToolServices: Send + Sync + 'static {
    fn capabilities(&self) -> ServiceCapabilities;
    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError>;
}
pub struct RequestContext {
    pub request_id: RequestId,
    pub cancellation: CancellationToken,
    pub deadline: Instant,
    pub limits: ServiceLimits,
}
```

`ToolServices`, `TypedToolRequest`, `TypedToolResult`, `RequestContext`, `ServiceLimits` and
`ServiceError` live in transport-neutral `market-squawk-services`; MCP implements only the transport.
The MCP crate knows protocol, bounds, audit envelopes and opaque artifacts; it contains no market
book, provider, portfolio, valuation, model, risk or execution implementation.

- [ ] **Step 1: Port compatibility tests and write hostile lifecycle RED tests**

Test initialize version/capability negotiation, initialized-state enforcement, string/integer IDs,
duplicate active IDs, notifications, ping, progress, cancellation, deadline, EOF, broken pipe,
maximum frame/body/depth/string/array/map size, output queue/backpressure, bounded errors, secret
redaction, and one audit admission/result class per request. Assert every domain tool is absent until
Task 19 registers a service.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path crates/market-squawk-mcp/Cargo.toml --all-features
cargo test --manifest-path crates/market-squawk-services/Cargo.toml --all-features
```

Expected: FAIL because the dedicated MCP package and lifecycle implementation do not exist.

- [ ] **Step 3: Implement rmcp lifecycle behind the existing bounded transport**

Adopt the Task 1-pinned official SDK features only. Retain maximum-plus-one incremental newline
framing; validate body/depth/string/container bounds before dispatch; enforce initialize state and
negotiated protocol; track active typed request IDs; connect cancel/progress/deadline to a child token;
bound the writer queue and write deadline; treat EOF/broken pipe as controlled shutdown; keep stdout
protocol-clean. `rmcp::JsonRpcMessageCodec::default()` is prohibited because its maximum length is
`usize::MAX`; the owned bounded transport feeds the official SDK lifecycle and protocol types.

- [ ] **Step 4: Implement audit and artifact response contracts**

Audit request ID, honest local process identity class and authentication status, tool/version,
admitted limits, start/finish/result class and content hashes without secrets/full financial
payloads. Task 5 reports inherited stdio as unverified and exposes no public constructor that can
mint an authenticated identity; Task 19 may consume a sealed platform-issued capability. Inline
results stay within bytes/items limits. Larger results cross a pathless `ArtifactRepository`
contract and return an opaque `ArtifactReference`; the Task 3/19 production implementation must
stage, fsync, atomically publish, hash and register the object. MCP receives neither ambient
filesystem authority nor a caller-authored path.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-mcp/Cargo.toml --all-features
cargo test --manifest-path crates/market-squawk-services/Cargo.toml --all-features
cargo clippy --manifest-path crates/market-squawk-mcp/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
cargo clippy --manifest-path crates/market-squawk-services/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(mcp): add bounded local protocol server"
```

Expected: all protocol/hostile/cancellation/output tests pass with a fake bounded service; no domain
tool or app composition is claimed.

### Task 6: Implement the production Kraken live-to-paper vertical

**Files:**

- Create: `adapters/market-squawk-adapter-kraken/Cargo.toml`
- Create: `adapters/market-squawk-adapter-kraken/src/lib.rs`
- Create: `adapters/market-squawk-adapter-kraken/src/config.rs`
- Create: `adapters/market-squawk-adapter-kraken/src/messages.rs`
- Create: `adapters/market-squawk-adapter-kraken/src/decoder.rs`
- Create: `adapters/market-squawk-adapter-kraken/src/session.rs`
- Create: `adapters/market-squawk-adapter-kraken/src/qualification.rs`
- Create: `adapters/market-squawk-adapter-kraken/tests/fixtures.rs`
- Create: `adapters/market-squawk-adapter-kraken/tests/local_websocket.rs`
- Create: `adapters/market-squawk-adapter-kraken/tests/live_to_paper.rs`
- Create: `adapters/market-squawk-adapter-kraken/fixtures/manifest.json`

**Interfaces:**

- Consumes: Task 2 live/risk/paper interfaces, source registry/budget/capture, instrument numeric
  policy, existing Kraken checksum profile.
- Produces: `KrakenSource: LiveMarketSource`, exact-lexeme bounded decoder outcomes, typed transport-
  order evidence distinct from venue sequence, coverage/health, quarantine/recovery and optional
  current qualification only under the approved `KrakenQualificationPolicy`.

- [ ] **Step 1: Write official-fixture and local-WebSocket RED tests**

Use content-hashed official snapshot/update/checksum fixtures. Test subscribe acknowledgement,
unsupported depth, exact decimal lexemes including leading/trailing zeros, message-atomic update,
top-ten bid-desc/ask-asc CRC32, delete-zero, depth truncation, malformed/oversized/duplicate fields,
heartbeat versus market freshness, status/precision, ping/close/cancellation, checksum mismatch,
generation reconnect, fresh snapshot and complete requalification. Assert no fabricated venue
sequence exists in any type or wire output.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-kraken/Cargo.toml --all-features
```

Expected: FAIL because the production Kraken package does not exist.

- [ ] **Step 3: Implement transport, parser, integrity, and recovery**

Bind the endpoint/redirect/TLS policy and shared authoritative budget before connect. Capture each raw
frame before decoding. Preserve lexemes, apply each frame's updates atomically in wire order, invoke
the existing closed checksum profile, and quarantine the generation on any parse, depth, checksum,
status, precision, freshness, subscription, capture or send failure. Reconnect under bounded backoff
with a new generation and require a fresh snapshot.

- [ ] **Step 4: Enforce the independent qualification decision**

`KrakenQualificationPolicy` is a versioned, reviewed source-policy input, not adapter discretion. If
continuous bounded transport order plus checksum-after-every-update does not satisfy the product's
sequence-progression predicate, cap Kraken below execution quality and assert risk rejection. If the
independent evidence decision approves the composite rule, bind its exact version/digest into source
metadata and prove current capability expiry/revocation through the full live-to-paper test.

- [ ] **Step 5: Run GREEN, parser property tests, and commit**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-kraken/Cargo.toml --all-features
cargo clippy --manifest-path adapters/market-squawk-adapter-kraken/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(kraken): add integrity-qualified live adapter"
```

Expected: deterministic fixtures and parser property tests pass. A non-executable Kraken cap is
truthful adapter behavior but does not close the production-live release row by itself: Task 20 must
still demonstrate verified qualification and automated paper action through an authorized direct
source, or the usable release remains blocked with the unmet qualification predicate reported.
External Kraken smoke remains opt-in and truthful. Task 20 owns the exact fuzz target and campaign.

### Task 7: Implement local file, database-export, OFX/QFX, and Parquet adapters

**Files:**

- Create: `adapters/market-squawk-adapter-files/Cargo.toml`
- Create: `adapters/market-squawk-adapter-files/src/lib.rs`
- Create: `adapters/market-squawk-adapter-files/src/csv.rs`
- Create: `adapters/market-squawk-adapter-files/src/json.rs`
- Create: `adapters/market-squawk-adapter-files/src/xml.rs`
- Create: `adapters/market-squawk-adapter-files/src/excel.rs`
- Create: `adapters/market-squawk-adapter-files/src/parquet.rs`
- Create: `adapters/market-squawk-adapter-files/src/database.rs`
- Create: `adapters/market-squawk-adapter-files/src/ofx.rs`
- Create: `adapters/market-squawk-adapter-files/tests/hostile_files.rs`
- Create: `adapters/market-squawk-adapter-files/tests/ingest_vertical.rs`
- Create: `adapters/market-squawk-adapter-files/fixtures/manifest.json`

**Interfaces:**

- Consumes: `ExtractionSource`, controlled no-follow local file capabilities, Task 3 rights decision,
  Task 4 `ResearchIngestService` and schema registry.
- Produces: `FileExtractionSource` with bounded discovery/extract for CSV/TSV, JSON/NDJSON, XML,
  Excel, Parquet, read-only SQLite/database exports, OFX/QFX/broker exports and user-authorized files;
  every batch retains raw-source hash, schema, row/field policy, provenance and idempotency identity.

- [ ] **Step 1: Write RED hostile parser and archive tests**

Test CSV quoting/encoding/delimiter/row/column/field/decimal/timestamp errors; JSON duplicate keys,
depth/container/string/record bounds and NDJSON line recovery policy; XML DTD/external/general/
parameter entity and network prohibition plus depth/text bounds; Excel ZIP entry/count/uncompressed-
bytes/compression-ratio/path traversal, macro/formula/external-link/cached-value/sheet/cell policies;
Parquet footer/schema/metadata/row-group/column bounds; read-only SQLite allowlisted schema and
consistent snapshot; OFX SGML/XML nesting, duplicate transaction IDs, account/currency and supplied
totals; symlink races and no-follow path confinement.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-files/Cargo.toml --all-features
```

Expected: FAIL because the file adapter package is absent.

- [ ] **Step 3: Implement bounded production parsers**

Parse streams incrementally under one `ExtractionLimits` object covering source bytes, decompressed
bytes, records, fields, depth, text, sheets/cells, row groups, elapsed time and cancellation. Reject
macros, formulas without an approved cached-value policy, external entities/links/network, archive
traversal/bombs, mutable database reads and inferred accounting scale. Preserve the raw record/hash
and explicit row-error disposition.

- [ ] **Step 4: Prove provider-to-query consumption**

For one fixture per format, reserve rights/idempotency, extract canonical observations, publish via
Task 4, restart, resolve the manifest and query the exact rows. Re-run and assert no duplicate logical
observation. A same key with changed source bytes/schema/config must fail as a typed conflict.

- [ ] **Step 5: Run GREEN, parser property tests, and commit**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-files/Cargo.toml --all-features
cargo clippy --manifest-path adapters/market-squawk-adapter-files/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(files): add bounded local extraction adapters"
```

Expected: hostile fixtures fail safely, valid fixtures reach a manifest-pinned query, and no arbitrary
path/network/SQL surface exists. Task 20 owns the exact parser fuzz targets and campaigns.

### Task 8: Implement SEC EDGAR, submissions, filings, XBRL, and Company Facts

**Files:**

- Create: `adapters/market-squawk-adapter-sec/Cargo.toml`
- Create: `adapters/market-squawk-adapter-sec/src/lib.rs`
- Create: `adapters/market-squawk-adapter-sec/src/client.rs`
- Create: `adapters/market-squawk-adapter-sec/src/submissions.rs`
- Create: `adapters/market-squawk-adapter-sec/src/filings.rs`
- Create: `adapters/market-squawk-adapter-sec/src/xbrl.rs`
- Create: `adapters/market-squawk-adapter-sec/src/company_facts.rs`
- Create: `adapters/market-squawk-adapter-sec/tests/official_fixtures.rs`
- Create: `adapters/market-squawk-adapter-sec/tests/restart_reconcile.rs`
- Create: `adapters/market-squawk-adapter-sec/fixtures/manifest.json`

**Interfaces:**

- Consumes: shared source budget/health/network policy, declared user-agent configuration, rights
  admission, instrument registry and Task 4 ingest.
- Produces: `SecExtractionSource` yielding filing/fundamental observations with accession, form,
  amendment, taxonomy/context/unit/period, raw payload hash, source/published/available/ingested times,
  revision and lineage.

- [ ] **Step 1: Write RED official-fixture tests**

Cover submissions pagination, recent/archive reconciliation, Company Facts units/contexts/segments,
XBRL numeric scale/sign/decimals/period/entity, amendments and supersession, malformed/oversized
documents, unavailable exact release time, CIK/instrument resolution, conditional requests, retry/
429/403 health transitions and shared request ceiling. Require the configured declared user agent.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-sec/Cargo.toml --all-features
```

Expected: FAIL because the SEC adapter does not exist.

- [ ] **Step 3: Implement bulk and incremental ingestion**

Use one authorized HTTP client and shared budget. Bound response/decompression/DOM/text, preserve
exact payload bytes/hashes, parse production fixtures with the same code as network responses, and
derive availability conservatively without inventing intraday timestamps. Bulk initialization and
incremental runs converge by accession/fact identity; amendments remain separate revisions.

- [ ] **Step 4: Run GREEN, prove restart/reconciliation, and commit**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-sec/Cargo.toml --all-features
cargo clippy --manifest-path adapters/market-squawk-adapter-sec/Cargo.toml \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(sec): ingest filings and company facts"
```

Expected: bulk plus incremental rerun is idempotent, revisions persist, provider-to-query tests pass,
and external SEC smoke remains opt-in.

### Task 9: Implement FRED/ALFRED, BLS, and US Treasury adapters

**Files:**

- Create: `adapters/market-squawk-adapter-fred/Cargo.toml`
- Create: `adapters/market-squawk-adapter-fred/src/lib.rs`
- Create: `adapters/market-squawk-adapter-fred/src/client.rs`
- Create: `adapters/market-squawk-adapter-fred/src/series.rs`
- Create: `adapters/market-squawk-adapter-fred/src/vintages.rs`
- Create: `adapters/market-squawk-adapter-fred/src/rights.rs`
- Create: `adapters/market-squawk-adapter-fred/tests/vintages.rs`
- Create: `adapters/market-squawk-adapter-fred/tests/rights.rs`
- Create: `adapters/market-squawk-adapter-bls/Cargo.toml`
- Create: `adapters/market-squawk-adapter-bls/src/lib.rs`
- Create: `adapters/market-squawk-adapter-bls/src/client.rs`
- Create: `adapters/market-squawk-adapter-bls/src/chunks.rs`
- Create: `adapters/market-squawk-adapter-bls/src/observations.rs`
- Create: `adapters/market-squawk-adapter-bls/tests/chunking.rs`
- Create: `adapters/market-squawk-adapter-treasury/Cargo.toml`
- Create: `adapters/market-squawk-adapter-treasury/src/lib.rs`
- Create: `adapters/market-squawk-adapter-treasury/src/client.rs`
- Create: `adapters/market-squawk-adapter-treasury/src/fiscal_data.rs`
- Create: `adapters/market-squawk-adapter-treasury/src/rates.rs`
- Create: `adapters/market-squawk-adapter-treasury/tests/pagination.rs`
- Create: `adapters/market-squawk-adapter-treasury/tests/rates.rs`
- Create: `docs/verification/fred-rights-decision.json`

**Interfaces:**

- Consumes: Task 3 rights/secrets/catalog, shared budget/health/network policy and Task 4 ingest.
- Produces: `FredExtractionSource`, `BlsExtractionSource`, `TreasuryExtractionSource`; exact macro
  series/observation/vintage/revision/availability/coverage records; machine-checked
  `FredRightsDecision { terms_digest, operations, expires_at, disposition }`.

- [ ] **Step 1: Write three provider RED suites**

FRED tests cover explicit realtime start/end, observation pagination, missing markers, vintage dates,
revisions, secret redaction and 429. BLS tests cover deterministic v1/v2 series/year chunking,
public/registered limits, partial/error messages, preliminary flags and explicit vintage limitation.
Treasury tests cover Fiscal Data pagination, repeated/missing pages, schema drift, official rate files,
yield methodology/version and source hashes. All test shared budgets, deadlines, cancellation and
conservative unknown release times.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-fred/Cargo.toml
cargo test --manifest-path adapters/market-squawk-adapter-bls/Cargo.toml
cargo test --manifest-path adapters/market-squawk-adapter-treasury/Cargo.toml
```

Expected: FAIL because none of the macro adapters exists.

- [ ] **Step 3: Implement lawful provider clients and exact revisions**

Use official endpoints, one declared/authorized identity where applicable, the shared durable budget,
bounded pagination and `Retry-After`. Preserve raw payload hashes, series metadata, reference period,
published/available evidence, vintage/revision and coverage limitations. Production parser code is
identical for content-hashed fixtures and opt-in network responses.

- [ ] **Step 4: Enforce FRED rights as a release predicate**

Before persistence, independently verify the exact current terms bytes and operation authorization.
If retrieve is permitted but persist/cache/archive/train is not affirmatively evidenced, run only the
permitted ephemeral operation, reject `Catalog::reserve_ingest`, record the fail-closed disposition,
and keep the usable release gate blocked. Do not treat user acknowledgement as rights, infer a bulk-
download exception, evade controls, or count an ephemeral stub as the durable FRED/ALFRED capability.
Release unblocks only when exact lawful source/use evidence supports the required local consumer or
the user explicitly revises the product requirement.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-fred/Cargo.toml --all-features
cargo test --manifest-path adapters/market-squawk-adapter-bls/Cargo.toml --all-features
cargo test --manifest-path adapters/market-squawk-adapter-treasury/Cargo.toml --all-features
cargo clippy --manifest-path adapters/market-squawk-adapter-fred/Cargo.toml \
  --all-targets --all-features -- -D warnings
cargo clippy --manifest-path adapters/market-squawk-adapter-bls/Cargo.toml \
  --all-targets --all-features -- -D warnings
cargo clippy --manifest-path adapters/market-squawk-adapter-treasury/Cargo.toml \
  --all-targets --all-features -- -D warnings
git diff --check
git commit -m "feat(macro): ingest official revisioned series"
```

Expected: provider suites pass and `fred-rights-decision.json` truthfully records either lawful durable
admission or a release-blocking rejection; it never fabricates completion.

### Task 10: Implement portfolio import and raw-record reconciliation

**Files:**

- Create: `adapters/market-squawk-adapter-portfolio/Cargo.toml`
- Create: `adapters/market-squawk-adapter-portfolio/src/lib.rs`
- Create: `adapters/market-squawk-adapter-portfolio/src/holdings.rs`
- Create: `adapters/market-squawk-adapter-portfolio/src/transactions.rs`
- Create: `adapters/market-squawk-adapter-portfolio/src/reconcile.rs`
- Create: `adapters/market-squawk-adapter-portfolio/tests/import.rs`
- Create: `adapters/market-squawk-adapter-portfolio/tests/reconcile.rs`
- Create: `adapters/market-squawk-adapter-portfolio/fixtures/manifest.json`

**Interfaces:**

- Consumes: Task 7 file/OFX records, Task 3 identities/secrets/rights, Task 4 ingest.
- Produces: `PortfolioExtractionSource`, immutable `RawPortfolioRecord`, normalized account/holding/
  transaction/cash-flow/cost-basis observations, `SuppliedTotals`, and bounded typed
  `ReconciliationDiscrepancy` values.

- [ ] **Step 1: Write RED preservation and reconciliation tests**

Cover exact raw bytes/hash, duplicate broker transaction IDs, account/instrument/currency resolution,
cash/income/fee/corporate-action transactions, signed quantities, explicit lot method, missing/
ambiguous basis, broker-rounded supplied totals, multi-account imports, corrected statements,
supersession and credential redaction. Never coerce an unreconciled source total into the calculated
ledger.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-portfolio/Cargo.toml --all-features
```

Expected: FAIL because the portfolio adapter is absent.

- [ ] **Step 3: Implement normalized import with preserved evidence**

Store the immutable source record before normalization; convert only through validated account,
instrument, currency, Decimal and timestamp constructors; retain source-provided values beside
calculated fields; emit discrepancies with field, supplied/calculated amount, currency, tolerance
policy and source reference. Corrected imports supersede rather than delete prior records.

- [ ] **Step 4: Run GREEN, prove data-plane consumption, and commit**

```bash
cargo test --manifest-path adapters/market-squawk-adapter-portfolio/Cargo.toml --all-features
cargo clippy --manifest-path adapters/market-squawk-adapter-portfolio/Cargo.toml \
  --all-targets --all-features -- -D warnings
git diff --check
git commit -m "feat(portfolio): import and reconcile source records"
```

Expected: valid fixtures persist raw and normalized records, reruns are idempotent, corrections retain
history, and discrepancies are explicit inputs to Task 16.

### Task 11: Compose research ingestion and point-in-time datasets

**Files:**

- Modify: `crates/market-squawk-data/src/lib.rs`
- Create: `crates/market-squawk-data/src/pit.rs`
- Create: `crates/market-squawk-data/src/universe.rs`
- Create: `crates/market-squawk-data/src/corporate_actions.rs`
- Create: `crates/market-squawk-data/src/dataset_builder.rs`
- Create: `crates/market-squawk-data/tests/pit.rs`
- Create: `crates/market-squawk-data/tests/corporate_actions.rs`
- Create: `crates/market-squawk-data/tests/dataset_builder.rs`
- Create: `apps/market-squawk/src/research_service.rs`
- Create: `apps/market-squawk/tests/research_vertical.rs`

**Interfaces:**

- Consumes: Tasks 4 and 7-10 committed manifests, instrument/symbol/corporate-action history,
  evidenced availability and revisions.
- Produces:

```rust
pub struct PointInTimePolicy {
    policy_version: PolicyVersion,
    require_available_at: bool,
    include_superseded_revisions: bool,
    corporate_action_policy: CorporateActionPolicy,
    missing_observation_policy: MissingObservationPolicy,
}
pub struct DatasetBuildRequest;
pub struct FeatureLabelDataset;
pub trait PointInTimeService {
    async fn select(
        &self,
        request: PointInTimeRequest,
        cancellation: CancellationToken,
    ) -> Result<PointInTimeResult, PointInTimeError>;
}
pub trait DatasetBuilder {
    async fn build(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<FeatureLabelDataset, DatasetBuildError>;
}
```

- [ ] **Step 1: Write RED temporal, universe, and action tests**

Test `available_at <= as_of`, effective intervals, `published_at`, `superseded_at`, deterministic
highest admitted revision, unknown/inferred availability excluded by default, provider civil date
with conservative not-before time, historical constituents, symbol changes, mergers, delistings,
option/futures expiry/rolls, splits/dividends/spinoffs/mergers, raw versus adjusted versus total-return
policies, and no mutation of original observations. Add future-perturbation, delayed-publication,
same-date unknown-time, survivorship and compaction-invariance tests.

- [ ] **Step 2: Run RED**

```bash
cargo test -p market-squawk-data --test pit --test corporate_actions \
  --test dataset_builder --all-features --locked
cargo test -p market-squawk --test research_vertical --locked
```

Expected: FAIL because PIT selection, corporate-action policy, dataset builder and research service
composition are absent.

- [ ] **Step 3: Implement conservative PIT and corporate-action policy**

Pin one manifest generation per input. Admit only records satisfying every versioned temporal,
revision and universe predicate. Keep raw events immutable; publish adjusted/total-return derived
datasets with policy version, parent hashes and adjustment lineage. Never invent publication time or
silently drop delisted instruments.

- [ ] **Step 4: Implement reproducible feature/label dataset construction**

Bind universe, as-of range, feature versions, inputs, label cutoff, chronological train/validation/
test splits, missing-value policy, corporate-action policy and implementation revision. Labels use
only data after each feature cutoff. Publish content-addressed Parquet through Task 4 and return a
complete manifest reference.

- [ ] **Step 5: Run GREEN, prove every research producer has a consumer, and commit**

```bash
cargo test -p market-squawk-data --all-features --locked
cargo test -p market-squawk --test research_vertical --locked
cargo clippy -p market-squawk-data -p market-squawk \
  --all-targets --all-features --locked -- -D warnings
git diff --check
git commit -m "feat(research): build point-in-time datasets"
```

Expected: file, SEC and authorized macro fixtures ingest, restart, select as-of, apply explicit
corporate actions and produce a queryable dataset; no live journal is required.

### Task 12: Implement complete Rust batch analytics and feature registry

**Files:**

- Modify: `crates/market-squawk-analytics/Cargo.toml`
- Modify: `crates/market-squawk-analytics/src/lib.rs`
- Create: `crates/market-squawk-analytics/src/batch.rs`
- Create: `crates/market-squawk-analytics/src/returns.rs`
- Create: `crates/market-squawk-analytics/src/risk.rs`
- Create: `crates/market-squawk-analytics/src/factors.rs`
- Create: `crates/market-squawk-analytics/src/fundamentals.rs`
- Create: `crates/market-squawk-analytics/src/macro_features.rs`
- Create: `crates/market-squawk-analytics/src/scenarios.rs`
- Modify: `crates/market-squawk-analytics/src/registry.rs`
- Create: `crates/market-squawk-analytics/tests/golden.rs`
- Create: `crates/market-squawk-analytics/tests/properties.rs`
- Create: `crates/market-squawk-analytics/tests/live_batch_parity.rs`

**Interfaces:**

- Consumes: domain Decimal/currency/unit/time values, Task 2 pure live kernels and Task 11 PIT batches.
- Produces: `FeatureDefinition`, `FeatureRegistry`, `AnalyticsPolicy`, typed results for returns,
  volatility/drawdown/correlation/beta/alpha, Sharpe/Sortino/tracking error/information ratio,
  historical/parametric VaR, coherent discrete Expected Shortfall, factors, fundamentals/valuation/
  FCF/surprises/yield curves, exposure/attribution and scenario/stress kernels.

- [ ] **Step 1: Write RED golden/property tests**

Define units, annualization, null/missing, weights, insufficient history and floating conversion for
each function. Include zero variance, negative prices, irregular dates, weighted observations, NaN/
infinity rejection, discrete ES ties/point masses/fractional quantile atom, currency/unit mismatch,
empty/one-element windows, drawdown recovery, beta singularity, factor rank deficiency, yield-curve
ordering, and scenario shock composition. Proptest algebraic invariants and explicit live/batch parity
where semantics match.

- [ ] **Step 2: Run RED**

```bash
cargo test -p market-squawk-analytics --test golden --test properties \
  --test live_batch_parity --all-features --locked
```

Expected: FAIL because complete batch modules and registry APIs are absent.

- [ ] **Step 3: Implement exact boundaries and deterministic kernels**

Keep money/accounting/fees/cost basis in checked Decimal/scaled types. Convert to finite `f64` only
through a typed `StatisticalInput` carrying source unit/scale, then return a typed result with units/
policy/observations. Use stable summation and documented sample/population/quantile conventions.
Avoid hidden annualization or implicit missing-value removal.

- [ ] **Step 4: Implement the feature registry**

Require name/version, input schema digest, parameters, time semantics, warm-up, null policy, output
type/unit, live compatibility, PIT compatibility and implementation revision/hash. Reject duplicate
identity with changed metadata, unknown implementation hash and incompatible requested version.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p market-squawk-analytics --all-features --locked
cargo clippy -p market-squawk-analytics --all-targets --all-features \
  --locked -- -D warnings
git diff --check
git commit -m "feat(analytics): add complete financial feature kernels"
```

Expected: all golden/property/parity tests pass; the crate has no data/platform/network/filesystem/
Python dependency.

### Task 13: Implement model registry, complete bundles, and native Rust inference

**Files:**

- Create: `crates/market-squawk-modeling/Cargo.toml`
- Create: `crates/market-squawk-modeling/src/lib.rs`
- Create: `crates/market-squawk-modeling/src/metadata.rs`
- Create: `crates/market-squawk-modeling/src/bundle.rs`
- Create: `crates/market-squawk-modeling/src/registry.rs`
- Create: `crates/market-squawk-modeling/src/input.rs`
- Create: `crates/market-squawk-modeling/src/native.rs`
- Create: `crates/market-squawk-modeling/tests/bundle.rs`
- Create: `crates/market-squawk-modeling/tests/native.rs`
- Create: `crates/market-squawk-modeling/tests/no_action.rs`

**Interfaces:**

- Consumes: Task 11 dataset manifests, Task 12 feature registry, controlled artifact paths.
- Produces:

```rust
pub trait InferenceBackend: Send + Sync {
    fn metadata(&self) -> &ModelMetadata;
    fn infer(&self, input: &ModelInput) -> Result<ModelOutput, InferenceError>;
}
pub struct ModelBundle;
pub struct ModelRegistry;
pub struct NativeLinearBackend;
```

`ModelBundle` binds artifact/hash, format/version, feature schema/versions, normalization, training
period/universe, dataset versions, label, training code revision, validation metrics, thresholds,
intended use/limitations and fallback.

- [ ] **Step 1: Write RED bundle/native/no-action tests**

Reject missing/unknown fields, wrong artifact hash, unsupported format/version, feature reorder/
version/schema mismatch, invalid normalizer, dataset/universe/period/label mismatch, nonfinite weights,
oversized input/output/artifact, threshold inconsistency, absent intended use/fallback and changed code
revision. Test deterministic native linear/logistic inference and that every load/validation/inference
error returns no `OrderIntent` through the Task 2 strategy integration.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path crates/market-squawk-modeling/Cargo.toml --all-features
```

Expected: FAIL because the modeling package and bundle/backend types do not exist.

- [ ] **Step 3: Implement closed bundle validation and registry**

Read only from the controlled model root; no remote URL/code/plugin. Bound bytes, JSON depth/members/
strings, feature count and tensors before allocation. Hash exact artifact and metadata bytes, validate
all relationships against the Task 11/12 registries, atomically register immutable generations and
retain prior versions for reproducibility.

- [ ] **Step 4: Implement native inference and strategy failure boundary**

Implement checked finite normalization, shape/version matching and deterministic native operations.
Return typed output with model/bundle/feature/dataset identities and confidence/decision fields. The
strategy adapter maps `Err` only to an audited no-action reason and cannot substitute a default score.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-modeling/Cargo.toml --all-features
cargo clippy --manifest-path crates/market-squawk-modeling/Cargo.toml \
  --all-targets --all-features -- -D warnings
git diff --check
git commit -m "feat(modeling): validate bundles and native inference"
```

Expected: complete bundle and native inference work with real PIT/feature metadata; every mismatch is
fail-closed and no action is produced.

### Task 14: Implement the Python financial analytics and training product

**Files:**

- Create: `crates/market-squawk-python/Cargo.toml`
- Create: `crates/market-squawk-python/src/lib.rs`
- Create: `python/pyproject.toml`
- Create: `python/requirements.lock`
- Create: `python/wheelhouse-lock.json`
- Create: `python/market_squawk/__init__.py`
- Create: `python/market_squawk/data.py`
- Create: `python/market_squawk/finance.py`
- Create: `python/market_squawk/training.py`
- Create: `python/market_squawk/bundle.py`
- Create: `python/market_squawk/visualization.py`
- Create: `python/tests/test_data.py`
- Create: `python/tests/test_finance_parity.py`
- Create: `python/tests/test_training_bundle.py`
- Create: `python/tests/test_visualization_examples.py`
- Create: `python/examples/pit_research.py`
- Create: `python/examples/pit_research.ipynb`
- Create: `scripts/build_python_release.py`
- Create: `scripts/tests/test_build_python_release.py`

**Interfaces:**

- Consumes: Task 11 manifest/PIT dataset, Task 12 analytics, Task 13 bundle schema, Task 1 exact Python
  dependency/runtime policy.
- Produces Python APIs `open_dataset(root, manifest_sha256, as_of)`, `market_squawk.finance` PyO3
bindings to Rust kernels, `TrainingRun.fit_evaluate_export`, and `BundleCandidate.write`, returning
only verified local paths/content hashes and exact Decimal/time/provenance metadata. The research API
also produces bounded self-contained chart specifications/static SVG from already loaded local
results; the executable notebook uses the same APIs with no download or release authority.

- [ ] **Step 1: Write RED packaging/data/parity/training tests**

Test deterministic source/wheel lock parsing, clean offline hash-locked install, supported interpreter
matrix, package import without network,
manifest hash/schema mismatch, symlink/path escape, PIT/as-of enforcement, Arrow Decimal128 <->
`decimal.Decimal` exact scale, timezone-aware nanoseconds, null policy, Rust/Python golden parity for
all exported finance kernels, deterministic seeded training split/evaluation, nonfinite rejection,
exported artifact hash and Task 13 bundle-candidate validation. Scan live/execution dependency graphs
to prove no Python/PyO3 import. Execute the example script and notebook in the clean offline venv;
assert fixed hashes for chart specifications, no external resource URLs, bounded point counts and no
secret/path leakage.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest scripts.tests.test_build_python_release -v
python3 scripts/build_python_release.py \
  --lock python/wheelhouse-lock.json \
  --wheelhouse .agents/tmp/python-wheelhouse \
  --venv .agents/tmp/python-release-venv
```

Expected: FAIL because the Python product, lock, native extension and release builder do not exist.

- [ ] **Step 3: Implement manifest-bound data access and native finance bindings**

Use PyArrow only after validating the catalog-exported manifest digest, schema version, object hashes
and as-of policy; never scan directories for datasets. Convert accounting values to `Decimal`, not
float. Build a `market_squawk._native` PyO3 extension from `market-squawk-python`, which depends only
on domain/analytics and exposes typed arrays/policies/results. Release the GIL only around bounded
pure Rust computation; translate typed errors without leaking paths/secrets.

`scripts/build_python_release.py` verifies every source/wheel filename, SHA-256, Python/platform tag
and license from `wheelhouse-lock.json`; populates the ignored wheelhouse from an explicitly supplied
local cache or an authorized preparation network mode. Release mode runs the Task 1-pinned maturin
with `cargo --locked` against the integration-owner-produced workspace lock. The builder hashes/adds
the `abi3` project wheel to the run manifest, creates a clean venv, installs dependencies and the
project wheel with `pip --no-index --find-links`, and records interpreter, compiler, Cargo, wheel and
lock digests. It never downloads during the offline verification mode used by Task 20.

- [ ] **Step 4: Implement deterministic training, evaluation, and export**

Require dataset/feature/label/universe/split hashes, seed, code/environment lock digest and explicit
missing policy. Record metrics and trial identity; export a size-bounded model plus normalization and
complete Task 13 metadata. Re-open and validate the candidate with the Rust bundle validator before
success. Examples use local fixtures and perform no hidden download.

- [ ] **Step 5: Run GREEN and commit**

```bash
python3 -m unittest scripts.tests.test_build_python_release -v
MARKET_SQUAWK_PYTHON_WHEEL_PREPARE_NETWORK=1 \
  python3 scripts/build_python_release.py --prepare-cache-only \
  --lock python/wheelhouse-lock.json \
  --wheelhouse .agents/cache/python-wheelhouse
python3 scripts/build_python_release.py \
  --offline \
  --lock python/wheelhouse-lock.json \
  --wheelhouse .agents/cache/python-wheelhouse \
  --venv .agents/tmp/python-release-venv
.agents/tmp/python-release-venv/bin/python -m pytest python/tests -q
cargo test -p market-squawk-python --all-features
cargo build --workspace --all-features
python3 scripts/check_workspace_boundaries.py
git diff --check
git commit -m "feat(python): add financial research and training product"
```

Expected: the pinned maturin build creates and installs `_native`, clean offline install and all data/
parity/training/bundle tests pass, and the full Rust workspace all-feature build remains green. The
Python extension is an analytical package dependency only; no live/execution crate links PyO3 or
requires a Python runtime.

### Task 15: Implement required local ONNX inference and an optional external-runtime backend

**Files:**

- Modify: `crates/market-squawk-modeling/Cargo.toml`
- Modify: `crates/market-squawk-modeling/src/lib.rs`
- Create: `crates/market-squawk-modeling/src/onnx.rs`
- Create: `crates/market-squawk-modeling/src/onnx/policy.rs`
- Create: `crates/market-squawk-modeling/src/onnx/worker.rs`
- Create: `crates/market-squawk-modeling/tests/onnx.rs`
- Create: `crates/market-squawk-modeling/tests/onnx_hostile.rs`
- Create: `crates/market-squawk-modeling/fixtures/onnx/manifest.json`
- Create: `docs/operations/onnx-runtime.md`
- Create: `docs/licenses/onnx-runtime-notice.md`
- Create: `scripts/verify_onnx_runtime.py`
- Create: `docs/verification/onnx-runtime-policy.json`

**Interfaces:**

- Consumes: Task 1 pinned `tract-onnx` decision and optional isolated `ort`/ONNX Runtime decision,
  Task 13 `ModelBundle` and `InferenceBackend`, controlled runtime/model roots.
- Produces: required `TractOnnxBackend: InferenceBackend`, optional
  `ExternalOnnxRuntimeBackend: InferenceBackend`, and one `OnnxModelPolicy` binding model digest,
  ONNX opset, allowed operators, static shapes, 64 MiB artifact ceiling, 1,024-node ceiling,
  256-tensor ceiling, 1,000,000-element per-request ceiling, bounded execution, warm-up result and
  stable native/no-action fallback. The required backend is fully local and self-contained; the
  optional backend cannot be the only way to satisfy the release's ONNX capability.

- [ ] **Step 1: Write thin RED model-policy, parity, and failure-boundary tests**

Use one table-driven hostile-model suite for external data, custom/control-flow/random operators,
unsupported opsets, unknown/dynamic/unbounded shapes, resource ceilings, corrupt protobuf and
nonfinite inputs/outputs. Use one golden model to prove native-versus-tract parity and the same model
to prove optional external-runtime parity when that feature and verified runtime are present. Assert
every preflight, load, warm-up, or inference failure yields the configured native fallback or audited
no-action, never a default score. Do not duplicate parser cases across backend-specific suites.

- [ ] **Step 2: Run RED**

```bash
cargo test -p market-squawk-modeling --test onnx --test onnx_hostile \
  --features onnx-tract
```

Expected: FAIL because the ONNX backend/policy and fixtures do not exist.

- [ ] **Step 3: Implement common preflight and the required tract backend**

Hash and parse the model before runtime load. Allow only `Add`, `Sub`, `Mul`, `Div`, `MatMul`, `Gemm`,
`Relu`, `Sigmoid`, `Tanh`, `Softmax`, `Reshape`, `Transpose`, `Gather`, `Concat`, `ReduceMean`, `Sqrt`,
`Clip`, `Cast`, and `Identity` under approved opsets and static bounded shapes. Reject external data,
custom domains, control flow and randomness. Compile the accepted graph with the pinned
`tract-onnx` crate, use bounded inputs/outputs and a serialized model-owned worker, then warm and
validate the result before atomic publication. No Python process, native runtime download, shared
library, network access, or build-time binary fetch is permitted for the required backend.

- [ ] **Step 4: Isolate and verify the optional ONNX Runtime backend**

Keep `ort` behind a separate `onnx-runtime` feature with default features disabled and only
`load-dynamic` enabled. Never download, fetch, copy, or remotely load native code at build or runtime.
An operator-supplied library must be confined to a configured local root, match the configured SHA-
256/version/platform tuple and carry the ONNX Runtime MIT notice/SBOM entry before it can construct a
backend. `verify_onnx_runtime.py` performs that direct admission check; it does not gain a unit-test
suite that merely re-tests its helpers. Failure to configure or admit the optional library leaves the
required tract backend available and is not a release blocker.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo build --workspace --all-features --locked
cargo test -p market-squawk-modeling --test onnx --test onnx_hostile \
  --features onnx-tract --locked
cargo clippy -p market-squawk-modeling --all-targets --features onnx-tract \
  --locked -- -D warnings
if test -n "${MARKET_SQUAWK_ONNX_RUNTIME:-}"; then
  python3 scripts/verify_onnx_runtime.py \
    --policy docs/verification/onnx-runtime-policy.json \
    --library "$MARKET_SQUAWK_ONNX_RUNTIME"
  cargo test -p market-squawk-modeling --test onnx \
    --features onnx-runtime --locked
fi
git diff --check
git commit -m "feat(modeling): add constrained ONNX inference"
```

Expected: full workspace all-features compiles without download or link-time runtime discovery; the
required tract backend runs approved fixtures fully offline; approved models pass parity;
hostile/resource cases fail closed. When an explicit hash-pinned external runtime is supplied, the
optional backend also passes verification and the shared golden parity case.

### Task 16: Implement complete portfolio accounting and analytics

**Files:**

- Create: `crates/market-squawk-portfolio/Cargo.toml`
- Create: `crates/market-squawk-portfolio/src/lib.rs`
- Create: `crates/market-squawk-portfolio/src/ledger.rs`
- Create: `crates/market-squawk-portfolio/src/lots.rs`
- Create: `crates/market-squawk-portfolio/src/reconcile.rs`
- Create: `crates/market-squawk-portfolio/src/performance.rs`
- Create: `crates/market-squawk-portfolio/src/exposure.rs`
- Create: `crates/market-squawk-portfolio/src/attribution.rs`
- Create: `crates/market-squawk-portfolio/src/rebalance.rs`
- Create: `crates/market-squawk-portfolio/src/risk.rs`
- Create: `crates/market-squawk-portfolio/tests/accounting.rs`
- Create: `crates/market-squawk-portfolio/tests/analytics.rs`
- Create: `crates/market-squawk-portfolio/tests/service.rs`
- Modify: `crates/market-squawk-execution/Cargo.toml`
- Modify: `crates/market-squawk-execution/src/risk.rs`
- Create: `crates/market-squawk-execution/tests/portfolio_state_integration.rs`

**Interfaces:**

- Consumes: Task 10 raw/normalized imports, Task 11 prices/actions/PIT, and Task 12 pure analytics
  kernels. Portfolio core has no dependency on execution or live authority.
- Produces: `PortfolioLedger`, immutable `PortfolioRevision`, account/holding/transaction/cash-flow/
  lot/gain/income results, `PerformanceReport`, `ExposureReport`, `AttributionReport`,
  `RebalanceProposal`, `PortfolioRiskReport`, and `PortfolioService` bounded queries. After the
  portfolio commit is merged, a serialized execution-owned integration consumes `PortfolioService`;
  the dependency remains `execution -> portfolio` and never reverses.

- [ ] **Step 1: Write RED accounting/property tests**

Cover buy/sell/short/cover, fees, dividends/interest/withholding, FIFO/specific-ID selected lot policy,
splits/mergers/spinoffs/return-of-capital, multi-currency cash with explicit FX provenance, realized/
unrealized gains, income, negative cash, corrected/superseded transactions, duplicate IDs, checked
overflow and raw/supplied-total reconciliation. Proptest inventory/cash/cost conservation under
partial lot disposal and corporate actions.

- [ ] **Step 2: Write RED analytics/risk tests and run RED**

Test time- and money-weighted performance under explicit policy, allocation totals, sector/factor/
currency/issuer/venue/instrument exposure, contribution-based attribution, constrained rebalancing,
tracking error, VaR/ES discrete cases, scenarios/stress and revision binding. Within portfolio tests,
assert that `PortfolioService` returns an opaque immutable current revision and rejects stale query
preconditions; do not import execution.

```bash
cargo test --manifest-path crates/market-squawk-portfolio/Cargo.toml --all-features
```

Expected: FAIL because the portfolio package and services do not exist.

- [ ] **Step 3: Implement immutable accounting and reconciliation**

Apply normalized transactions in deterministic account/time/source order through checked Decimal/
currency/lot operations, publish an immutable revision only after all invariants pass, retain source
and previous revision lineage, and emit typed discrepancies without overwriting supplied values.

- [ ] **Step 4: Implement analytics and the portfolio service boundary**

Use Task 12 kernels with explicit valuation/FX/as-of policies. Bound instruments/factors/scenarios/
history/result bytes. Rebalancing emits proposals, never approved orders. Publish current account
revision through an opaque read-only service capability. Portfolio DTOs cannot mint risk authority,
orders, or approvals, and the crate has no execution dependency.

- [ ] **Step 5: Run the portfolio GREEN gate and commit the independent core**

```bash
cargo test --manifest-path crates/market-squawk-portfolio/Cargo.toml --all-features
cargo clippy --manifest-path crates/market-squawk-portfolio/Cargo.toml \
  --all-targets --all-features -- -D warnings
git diff --check
git commit -m "feat(portfolio): add accounting analytics and risk state"
```

Expected: accounting, reconciliation, analytics and bounded service tests pass with no portfolio-to-
execution dependency.

- [ ] **Step 6: Serialize the execution-owned portfolio integration after merge**

Only the integration owner now changes `crates/market-squawk-execution/src/risk.rs` and adds
`portfolio_state_integration.rs`. Make risk load the authoritative `PortfolioRevision` immediately
before reserve/approve, bind the revision ID into `RiskDecision`/`ApprovedOrder`, and reject caller-
supplied balances/positions, missing accounts and stale/revoked revisions. Recheck the revision at
one-time dispatch. Do not add an execution import to portfolio.

```bash
cargo test -p market-squawk-execution --test portfolio_state_integration \
  --all-features --locked
cargo test -p market-squawk-execution --all-features --locked
cargo clippy -p market-squawk-execution --all-targets --all-features \
  --locked -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff --check
git commit -m "feat(execution): bind risk to portfolio revisions"
```

Expected: execution depends one-way on portfolio, risk and dispatch bind the same current revision,
and no alternate approval path exists.

### Task 17: Implement PIT research backtesting and experiment governance

**Files:**

- Modify: `apps/market-squawk/Cargo.toml`
- Create: `crates/market-squawk-backtesting/Cargo.toml`
- Create: `crates/market-squawk-backtesting/src/lib.rs`
- Create: `crates/market-squawk-backtesting/src/clock.rs`
- Create: `crates/market-squawk-backtesting/src/engine.rs`
- Create: `crates/market-squawk-backtesting/src/fills.rs`
- Create: `crates/market-squawk-backtesting/src/experiments.rs`
- Create: `crates/market-squawk-backtesting/src/service.rs`
- Create: `crates/market-squawk-backtesting/tests/backtest.rs`
- Create: `crates/market-squawk-backtesting/tests/backtest_leakage.rs`
- Create: `crates/market-squawk-backtesting/tests/experiment_governance.rs`
- Create: `apps/market-squawk/src/backtest_service.rs`
- Create: `apps/market-squawk/tests/backtest_vertical.rs`

**Interfaces:**

- Consumes: Task 11 PIT datasets, Task 12 pure analytics kernels, Task 13/15 models, Task 16
  accounting, and Task 2 strategy/order-intent types; never live replay. The orchestration crate is
  a leaf above analytics/modeling/portfolio/execution, so no dependency cycle is introduced.
- Produces: `BacktestRequest`, `BacktestResult`, `TrialRecord`, `ExperimentInventory`, reconciled
  orders/fills/cash/positions/performance and content-hashed artifacts/lineage.

- [ ] **Step 1: Write RED timing, leakage, accounting, and experiment tests**

Test signal timestamp versus next eligible execution, evidenced availability, fees/spread/slippage/
depth/partial fills, corporate actions, delistings, historical universe, portfolio constraints,
missing prices, stale inputs, model error/no action, cash/lot/gain reconciliation, deterministic seed,
rerun hash equality and no live-journal dependency. Future data perturbations cannot change earlier
features/orders. Trial inventory records data/model/strategy/code/config/seed, search space, selection
criterion and probability-of-backtest-overfitting/deflated-performance diagnostics.

- [ ] **Step 2: Run RED**

```bash
cargo test --manifest-path crates/market-squawk-backtesting/Cargo.toml \
  --test backtest --test backtest_leakage \
  --test experiment_governance --all-features
cargo test -p market-squawk --test backtest_vertical
```

Expected: FAIL because backtest/experiment modules and service are absent.

- [ ] **Step 3: Implement event-time backtest orchestration**

Implement orchestration in `market-squawk-backtesting`; keep only source-independent mathematical
kernels in `market-squawk-analytics`. Stream manifest-pinned PIT batches in deterministic order,
call the same typed strategy/model intent
contracts, simulate only with versioned research execution assumptions, and apply results through
Task 16 accounting. Separate research fill assumptions from Task 2 paper authority while sharing pure
fee/slippage/accounting kernels where semantics and units are identical.

- [ ] **Step 4: Implement experiment inventory and artifact publication**

Reserve a trial before execution; bind every immutable input and parameter; record completion/failure,
metrics and selection membership; publish bounded detailed outputs through controlled artifacts and a
small inline summary. Never delete losing trials or overwrite a trial identity.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-backtesting/Cargo.toml \
  --all-features
cargo test -p market-squawk --test backtest_vertical
cargo clippy -p market-squawk-backtesting -p market-squawk \
  --all-targets --all-features -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff -- Cargo.lock > .agents/tmp/generated-lock-disposition.patch
git restore --source=HEAD --staged --worktree -- Cargo.lock
git diff --exit-code -- Cargo.lock
git diff --check
git commit -m "feat(backtest): add point-in-time experiment engine"
```

Expected: PIT/leakage/reconciliation/experiment tests pass and a file/SEC/macro fixture produces a
reproducible backtest without capture replay.

### Task 18: Implement ASC 820/IFRS 13 fair-value analysis

**Files:**

- Create: `crates/market-squawk-valuation/Cargo.toml`
- Create: `crates/market-squawk-valuation/src/lib.rs`
- Create: `crates/market-squawk-valuation/src/measurement.rs`
- Create: `crates/market-squawk-valuation/src/evidence.rs`
- Create: `crates/market-squawk-valuation/src/rules.rs`
- Create: `crates/market-squawk-valuation/src/approval.rs`
- Create: `crates/market-squawk-valuation/src/service.rs`
- Create: `crates/market-squawk-valuation/tests/level1.rs`
- Create: `crates/market-squawk-valuation/tests/no_promotion.rs`
- Create: `crates/market-squawk-valuation/tests/override_approval.rs`

**Interfaces:**

- Consumes: instrument identity, Task 11 market/research evidence, Task 12 analytics, Task 16 portfolio
  positions, Task 3 immutable audit/catalog.
- Produces: `ValuationMeasurement`, `ValuationInput`, `ValuationMethod`, `FairValueEvidence`,
  versioned `ClassificationRuleset`, `ClassificationDecision`, `ValuationOverride`,
  `ValuationApproval`, and bounded `FairValueService`.

- [ ] **Step 1: Write RED Level 1 decision-table/property tests**

Require identical instrument, quoted unadjusted price, active market, accessible market,
measurement-date relevance, valid source/venue evidence and sufficient freshness. Missing evidence ->
`Unclassified`. Delayed/stale/proxy/adjusted/modeled/estimated/similar instrument cannot silently
qualify. Test market closure, thin/inactive market, inaccessible venue, post-measurement quote,
currency/scale mismatch and disqualifying adjustment.

- [ ] **Step 2: Write RED no-promotion and workflow tests; run RED**

Compile-fail and Serde tests prevent substitution among `FairValueHierarchy`, `MarketDepth`,
`DataQuality`, assessment and execution capability. Level 2/3 inputs remain analytical only. Test
ruleset version/hash, reason codes, evidence immutability, override justification/identity/time,
separation of preparer/approver, approval expiry/revocation and complete audit.

```bash
cargo test --manifest-path crates/market-squawk-valuation/Cargo.toml --all-features
```

Expected: FAIL because valuation package, rules and service are absent.

- [ ] **Step 3: Implement deterministic classification and workflow**

Construct measurements only from validated typed inputs; evaluate every ruleset predicate and retain
the complete truth table/reasons/evidence hashes. An override creates a new immutable decision and
never edits source evidence. Approval binds measurement/ruleset/override/reviewer/version and cannot
alter market data quality or execution eligibility.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test --manifest-path crates/market-squawk-valuation/Cargo.toml --all-features
cargo clippy --manifest-path crates/market-squawk-valuation/Cargo.toml \
  --all-targets --all-features -- -D warnings
git diff --check
git commit -m "feat(valuation): classify fair value with evidence"
```

Expected: decision tables, workflow and no-promotion tests pass; valuation remains outside execution
authority.

### Task 19: Complete shared application services, CLI, and MCP domains

**Files:**

- Modify: `apps/market-squawk/Cargo.toml`
- Modify: `crates/market-squawk-platform/src/config.rs`
- Modify: `crates/market-squawk-platform/src/paths.rs`
- Create: `crates/market-squawk-platform/src/observability.rs`
- Create: `crates/market-squawk-platform/tests/operations_policy.rs`
- Create: `apps/market-squawk/src/application.rs`
- Create: `apps/market-squawk/src/cli.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Modify: `apps/market-squawk/src/lib.rs`
- Modify: `crates/market-squawk-services/src/lib.rs`
- Create: `crates/market-squawk-services/src/source.rs`
- Create: `crates/market-squawk-services/src/market.rs`
- Create: `crates/market-squawk-services/src/research.rs`
- Create: `crates/market-squawk-services/src/fundamental.rs`
- Create: `crates/market-squawk-services/src/macro_data.rs`
- Create: `crates/market-squawk-services/src/portfolio.rs`
- Create: `crates/market-squawk-services/src/analysis.rs`
- Create: `crates/market-squawk-services/src/model.rs`
- Create: `crates/market-squawk-services/src/fair_value.rs`
- Create: `crates/market-squawk-services/src/bot.rs`
- Create: `crates/market-squawk-services/src/execution.rs`
- Create: `crates/market-squawk-services/tests/domain_contracts.rs`
- Modify: `crates/market-squawk-mcp/src/server.rs`
- Create: `crates/market-squawk-mcp/src/domains/source.rs`
- Create: `crates/market-squawk-mcp/src/domains/market.rs`
- Create: `crates/market-squawk-mcp/src/domains/research.rs`
- Create: `crates/market-squawk-mcp/src/domains/fundamental.rs`
- Create: `crates/market-squawk-mcp/src/domains/macro_data.rs`
- Create: `crates/market-squawk-mcp/src/domains/portfolio.rs`
- Create: `crates/market-squawk-mcp/src/domains/analysis.rs`
- Create: `crates/market-squawk-mcp/src/domains/model.rs`
- Create: `crates/market-squawk-mcp/src/domains/fair_value.rs`
- Create: `crates/market-squawk-mcp/src/domains/bot.rs`
- Create: `crates/market-squawk-mcp/src/domains/execution.rs`
- Create: `crates/market-squawk-mcp/tests/domains.rs`
- Create: `crates/market-squawk-mcp/tests/prohibited_surfaces.rs`
- Create: `apps/market-squawk/tests/cli_complete.rs`
- Create: `apps/market-squawk/tests/service_parity.rs`
- Create: `apps/market-squawk/tests/doctor_policy.rs`
- Create: `apps/market-squawk/tests/no_hidden_outbound.rs`
- Modify: `scripts/smoke_mcp.py`

**Interfaces:**

- Consumes: Task 2 `ApplicationServices`, Task 5 protocol, Tasks 4/11 query/PIT, Task 13/15 model,
  Task 16 portfolio, Task 17 backtest and Task 18 fair value services.
- Produces: one lifecycle-owned `Application` implementing versioned transport-neutral services from
  `market-squawk-services`, shared by the complete CLI and MCP. Every request has typed authorization,
  cancellation/deadline, time/instrument/result limits, source coverage, audit admission and
  controlled artifact policy. Neither transport owns business validation.

The same composition also produces `EffectiveConfig`, `EndpointPolicy`, `ArtifactRootPolicy`,
`RedactionPolicy`, `LocalTracingPolicy`, and a bounded `DoctorReport`. Configuration precedence is
exactly safe defaults -> local file -> `MARKET_SQUAWK_*` environment -> CLI override. Source and
execution endpoints are deny-by-default/allowlisted; the artifact root is capability-confined; local
tracing supports human-readable and JSON output with the same recursive redaction. Telemetry,
analytics beacons, OpenTelemetry, remote exporters and undocumented outbound clients are absent.

- [ ] **Step 1: Write RED CLI hierarchy and service-parity tests**

Require `init`, `config`, `source`, `capture`, `ingest`, `dataset`, `query`, `feature`, `model`,
`portfolio`, `backtest`, `bot`, `execution`, `fair-value`, `mcp serve`, and `doctor`, with human/JSON
output, precedence, bounded args, typed exit classes and diagnostic compatibility aliases. The same
request DTO and service call must produce semantically equal CLI/MCP results. DataFusion SQL exists
only under CLI `query` and cannot be reached by MCP.

Add precedence matrices for absent/file/env/CLI combinations, unknown `MARKET_SQUAWK_*` keys,
invalid/unset values and secret references. Run human/JSON tracing through nested typed errors,
provider payloads, URLs, headers, account/order IDs and `Debug`/`Display`; assert configured secret
values and capability paths never appear. `doctor --json` reports the precedence source for each
non-secret setting, allowlist decisions, artifact confinement, local-only tracing, telemetry disabled,
provider rights/health and release blockers, but no secret value or secret identifier. Socket/DNS
instrumentation asserts startup/doctor/default tests make no outbound call; only an explicitly
enabled allowlisted source/execution adapter can open its declared endpoint. Dependency/registration
scans reject `opentelemetry`, exporter/beacon code and hidden HTTP/WebSocket clients.

- [ ] **Step 2: Write RED complete MCP domain/schema/result tests**

Require these domains and minimum operations:

```text
Source.*      register/status/coverage/health
Market.*      snapshots/trades/quotes/books/quality/comparisons
Research.*    datasets/manifest/history/alternative data
Fundamental.* filings/facts/statements/ratios
Macro.*       series/observations/vintages/revisions
Portfolio.*   holdings/transactions/performance/exposure/risk
Analysis.*    returns/factors/valuation/scenarios/feature datasets/backtests
Model.*       metadata/bundles/evaluation/predictions
FairValue.*   measurements/classification/explanation/evidence/approval status
Bot.*         status/start/stop for controlled paper operation only
Execution.*   paper orders/fills/cancel/reconcile
```

Every schema rejects unknown fields and requires explicit instrument/time/result limits. Every call
propagates cancellation/deadline, records admitted/result audit, includes source coverage/quality,
returns truncation/completeness metadata, and spills over the inline byte/item ceiling to a content-
hashed opaque artifact. Mutations require local authorization/confirmation and the sole Task 2 risk/
dispatch service. Results cannot expose capability, credential, raw secret or arbitrary path.

- [ ] **Step 3: Run RED**

```bash
cargo test -p market-squawk-mcp --test domains --test prohibited_surfaces --locked
cargo test -p market-squawk-services --test domain_contracts --locked
cargo test -p market-squawk --test cli_complete --test service_parity \
  --test doctor_policy --test no_hidden_outbound --locked
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
cargo deny check
python3 scripts/check_workspace_boundaries.py
```

Expected: FAIL because complete domain handlers, CLI hierarchy and shared composition are absent.

- [ ] **Step 4: Implement one bounded application composition**

Start catalog/artifact/audit, research/query/PIT, portfolio/model/valuation, source/live/risk/paper,
then transports; publish the app only after every mandatory service is ready. Shutdown reverses the
order with bounded deadlines and retained owned handles. CLI/MCP borrow service handles and never
duplicate validation, read mutable live state, or call adapters/risk internals directly.

Load and validate one immutable effective configuration before constructing any outbound-capable
service. Install recursive redaction before the first trace event, bind both trace renderers to it,
and derive confined paths/endpoint capabilities from that configuration. `doctor` is read-only and
uses bounded health/status services; it never tests a remote endpoint unless the user invokes an
explicit source smoke command.

- [ ] **Step 5: Implement domain registration, mutation authority, and prohibited surfaces**

Map typed service errors to stable MCP/CLI errors without provider payloads/secrets. No shell,
arbitrary filesystem, unrestricted SQL, credential read, remote code/model loading, unchecked order,
risk bypass, audit deletion or provider-evasion tool exists in registration or hidden dispatch. Bot/
Execution accept intents/controlled commands only; risk constructs approvals and dispatcher consumes
them once.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p market-squawk-mcp --all-features --locked
cargo test -p market-squawk-services --all-features --locked
cargo test -p market-squawk --all-features --locked
cargo clippy -p market-squawk-services -p market-squawk-mcp -p market-squawk \
  --all-targets --all-features --locked -- -D warnings
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
cargo deny check
python3 scripts/check_workspace_boundaries.py
git diff --check
git commit -m "feat(app): complete local CLI and MCP services"
```

Expected: all domain, prohibited-surface, lifecycle, result-bound, cancellation/audit/artifact and
CLI-parity tests pass; compatibility aliases remain explicitly diagnostic.

### Task 20: Prove, review, publish, and stop at the usable complete local release

**Files:**

- Modify: `apps/market-squawk/Cargo.toml`
- Create: `apps/market-squawk/tests/usable_release.rs`
- Create: `apps/market-squawk/tests/external_coinbase_vertical.rs`
- Create: `apps/market-squawk/tests/external_kraken_vertical.rs`
- Create: `apps/market-squawk/tests/external_fred_alfred_vertical.rs`
- Create: `apps/market-squawk/src/bin/market-squawk-benchmark.rs`
- Create: `scripts/demo_usable_release.py`
- Create: `scripts/run_release_fuzz.py`
- Create: `scripts/run_sustained_benchmark.py`
- Create: `scripts/check_release_performance.py`
- Create: `scripts/check_external_release_evidence.py`
- Create: `scripts/check_release_evidence.py`
- Create: `scripts/tests/test_run_release_fuzz.py`
- Create: `scripts/tests/test_run_sustained_benchmark.py`
- Create: `scripts/tests/test_check_release_performance.py`
- Create: `scripts/tests/test_check_external_release_evidence.py`
- Create: `scripts/tests/test_check_release_evidence.py`
- Create: `fuzz/Cargo.toml`
- Create: `fuzz/fuzz_targets/coinbase_decoder.rs`
- Create: `fuzz/fuzz_targets/kraken_v2_decoder.rs`
- Create: `fuzz/fuzz_targets/csv_decoder.rs`
- Create: `fuzz/fuzz_targets/json_ndjson_decoder.rs`
- Create: `fuzz/fuzz_targets/xml_excel_decoder.rs`
- Create: `fuzz/fuzz_targets/xbrl_decoder.rs`
- Create: `fuzz/fuzz_targets/capture_record.rs`
- Create: `fuzz/fuzz_targets/mcp_request.rs`
- Create: `fuzz/fuzz_targets/model_bundle.rs`
- Modify: `apps/market-squawk/benches/event_to_decision_latency.rs`
- Modify: `crates/market-squawk-data/Cargo.toml`
- Create: `crates/market-squawk-data/benches/storage.rs`
- Create: `docs/verification/usable-release-demonstration.md`
- Create: `docs/verification/usable-release-performance.md`
- Create: `docs/verification/usable-release-gate.md`
- Create: `docs/reports/usable-release-review.md`
- Modify: `README.md`
- Modify: `SECURITY.md`
- Modify: `CHANGELOG.md`

**Interfaces:**

- Consumes: every Task 0-19 production vertical and exact immutable inputs/evidence.
- Produces: one clean exact candidate SHA, deterministic demo artifact set, measured performance/
  memory report, supply-chain/security evidence, zero-finding grouped review, GitHub publication and
  the only `usable_complete_local_release = true` determination.

- [ ] **Step 1: Write the failing all-vertical release demonstration**

The network-free demo initializes local state; runs deterministic local protocol servers through the
production Coinbase and Kraken parsers, sequence/checksum/books/features paths; verifies those local
simulations remain `DirectUnverified` and risk rejects automated action; exercises approved paper
orders from separately recorded direct-source evidence; ingests local files, SEC, authorized macro
and portfolio fixtures; restarts; queries Arrow/Parquet with DataFusion; builds PIT features/labels;
runs Rust/Python analytics and training; validates native/ONNX inference; backtests; produces
portfolio reports; classifies fair value; calls every CLI/MCP domain; and verifies all hashes/audits/
artifacts/reconciliation. It also executes `doctor` in human and JSON modes, verifies effective
configuration precedence, endpoint/artifact confinement, recursive secret redaction, telemetry/
OpenTelemetry disabled, and zero outbound socket/DNS calls in offline mode. A fixture or local server
can validate parsing and state transitions but can never mint `DirectVerified`. The demo fails if a
mock or diagnostic path substitutes for production, or if exact-head authorized direct-source
evidence is absent.

- [ ] **Step 2: Run RED**

```bash
cargo test -p market-squawk --test usable_release --all-features --locked
python3 scripts/demo_usable_release.py --offline-fixtures
```

Expected: FAIL until every required vertical, FRED rights predicate, and separate authorized
direct-source evidence predicate is satisfied and composed.

- [ ] **Step 3: Implement fuzz, performance, and sustained-memory evidence producers**

Task 20 is the sole owner of fuzz targets and campaigns; Tasks 6/7 contribute production parsers,
property tests and seed fixtures only. `run_release_fuzz.py` verifies the exact fuzz-only nightly from
Task 1, builds no release artifact, runs every named target serially with a 120-second/2 GiB ceiling,
rejects crashes/timeouts/OOMs, and emits a content-hashed JSON report. Stable Rust remains the only
release/approval toolchain.

The release benchmark binary measures decoder, sequence/checksum, bounded queue, book, online
features, strategy, native/ONNX inference, risk, one-time dispatch and end-to-decision separately and
together after an explicit warm-up. The storage bench measures Arrow/Parquet ingest/write/read,
DataFusion query and Python handoff. `run_sustained_benchmark.py` samples process RSS during at least
60 million events and fails if post-warm-up RSS grows beyond 32 MiB or 1% of the warm plateau,
whichever is larger, or if any queue exceeds configured capacity. Every run records hardware, OS,
toolchain, fixture/digest, event/row count, throughput, p50/p95/p99/max and peak RSS.

Before the candidate freeze, exercise every producer against a provisional ignored directory. These
runs validate the tooling but are never approval evidence:

```bash
EVIDENCE_DIR=target/release-evidence/provisional
mkdir -p "$EVIDENCE_DIR"
python3 scripts/run_release_fuzz.py \
  --toolchain nightly-2026-07-15 --seconds-per-target 120 \
  --rss-limit-mib 2048 --report "$EVIDENCE_DIR/fuzz.json"
cargo bench -p market-squawk --bench event_to_decision_latency \
  --all-features --locked -- --warm-up-events 1000000 --events 10000000 \
  --output "$EVIDENCE_DIR/live-bench.json"
cargo bench -p market-squawk-data --bench storage \
  --all-features --locked -- --rows 10000000 \
  --output "$EVIDENCE_DIR/data-bench.json"
cargo build -p market-squawk --bin market-squawk-benchmark \
  --release --all-features --locked
python3 scripts/run_sustained_benchmark.py \
  --binary target/release/market-squawk-benchmark --warm-up-events 1000000 \
  --events 60000000 --max-tail-growth-mib 32 --max-tail-growth-percent 1 \
  --report "$EVIDENCE_DIR/sustained-memory.json"
python3 scripts/check_release_performance.py \
  --live "$EVIDENCE_DIR/live-bench.json" \
  --data "$EVIDENCE_DIR/data-bench.json" \
  --memory "$EVIDENCE_DIR/sustained-memory.json" \
  --min-events-per-second 100000 --max-warmed-p99-ns 999999 \
  --report "$EVIDENCE_DIR/performance.json"
```

Expected: every command exits 0; measured end-to-decision throughput is at least 100,000 events/s,
warmed p99 is strictly below 1,000,000 ns, queues remain bounded, post-warm-up RSS meets the stated
growth bound, and analytical I/O probes record zero calls from the live path. The checker refuses to
write a passing report when measurements, fixture hashes, hardware fields or component distributions
are missing. No performance claim is permitted from estimates, local-server network timing, a failed
run or a different commit. The tracked demonstration, performance, and gate documents are prepared
before the candidate freeze as human-readable methodology and provisional-result summaries. They
must state that they are not exact-head approval evidence and are never rewritten after a freeze.

- [ ] **Step 4: Implement the separate authorized provider-evidence producer**

Run production adapters against direct venue/public interfaces and FRED/ALFRED only after the user has
affirmed the applicable terms, configured one authorized identity where required, and explicitly
enabled external networking. The command records source/venue/instrument/coverage, transport peer,
sequence/snapshot/checksum/timestamp/freshness/trading-status/precision predicates, denial/recovery
tests, binary/tree/head digests and rights decision. It never records credentials. Coinbase and Kraken
must each prove the exact quality ceiling their real channel supports. At least one authorized direct
source must satisfy every `DirectVerified` predicate and drive strategy -> comprehensive risk ->
paper action; otherwise the release remains blocked. FRED/ALFRED retrieval/persistence/training rights
must be admitted or the required macro predicate remains blocked.

The provisional provider run uses the same production tests and checker but writes only below
`target/release-evidence/provisional/providers`. It establishes parser/rights-checker behavior, not
release approval:

```bash
test "$MARKET_SQUAWK_EXTERNAL_NETWORK" = "1"
test "$MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED" = "1"
export MARKET_SQUAWK_RELEASE_EVIDENCE_DIR=target/release-evidence/provisional/providers
cargo test -p market-squawk --test external_coinbase_vertical \
  --all-features --locked -- --ignored --nocapture
cargo test -p market-squawk --test external_kraken_vertical \
  --all-features --locked -- --ignored --nocapture
cargo test -p market-squawk --test external_fred_alfred_vertical \
  --all-features --locked -- --ignored --nocapture
python3 scripts/check_external_release_evidence.py \
  --head "$(git rev-parse HEAD)" --tree "$(git rev-parse HEAD^{tree})" \
  --evidence-dir target/release-evidence/provisional/providers \
  --require-direct-verified-action --require-fred-alfred-rights
```

Expected: tests and checker pass only on real authorized network delivery at the unchanged candidate;
the checker rejects local servers, fixtures, synthetic transports, missing predicates, mismatched
head/tree/binary hashes, credentials in output, unsupported quality promotion and incomplete rights.

- [ ] **Step 5: Finish every tracked file and commit the candidate before collecting evidence**

First verify the hash-pinned local ONNX runtime, build the maturin extension and Python wheel using
only the committed wheelhouse lock, and run the deterministic local gate. The extension may call
analytical Rust kernels only; the dependency checker proves neither Python nor the extension enters
live/execution runtime edges. Complete the README, changelog, security guidance, review procedure,
and all tracked human-readable summaries now. A tracked file may summarize provisional evidence and
methodology, but it must never claim to contain evidence for the commit that contains itself.

```bash
python3 scripts/verify_onnx_runtime.py \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME"
python3 scripts/build_python_release.py \
  --wheelhouse .agents/cache/python-wheelhouse \
  --lock python/wheelhouse-lock.json \
  --venv target/usable-release-venv --offline \
  --report target/release-evidence/provisional/python-build.json
target/usable-release-venv/bin/python -m pytest python/tests -q
MARKET_SQUAWK_PYTHON=target/usable-release-venv/bin/python \
  python3 scripts/demo_usable_release.py --offline-fixtures \
  --provider-evidence target/release-evidence/provisional/providers \
  --report target/release-evidence/provisional/demo.json
./scripts/verify.sh
git diff --check
git commit -m "release: prove usable complete local platform"
```

Before the commit, stage only the literal paths in the reviewed ownership map and inspect the staged
diff. Do not use globs, generated path lists, custom staging wrappers, or angle-bracket pseudo-paths.
The exact literal staging command is recorded in the checkpoint handoff. The commit is only a clean
candidate; provisional results do not approve it.

- [ ] **Step 6: Collect the complete evidence set at the clean unchanged candidate**

Set one HEAD-keyed ignored artifact directory after the commit. Every producer and checker accepts
the exact HEAD/tree and records the relevant binary, model, fixture, toolchain, input, and output
hashes. `check_release_evidence.py` validates the closed artifact schema, rejects missing/extra or
cross-HEAD artifacts and credentials, verifies all internal hashes, and writes the final manifest in
that same ignored directory. It validates artifacts, not command text or plan prose.

Run this complete block on the documented target hardware. An abbreviated `verify.sh` invocation is
not a substitute for any command in the block:

```bash
set -euo pipefail
test -z "$(git status --porcelain)"
HEAD_SHA="$(git rev-parse HEAD)"
TREE_SHA="$(git rev-parse HEAD^{tree})"
EVIDENCE_DIR="target/release-evidence/$HEAD_SHA"
mkdir -p "$EVIDENCE_DIR/providers"

python3 scripts/run_release_fuzz.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --toolchain nightly-2026-07-15 --seconds-per-target 120 \
  --rss-limit-mib 2048 --report "$EVIDENCE_DIR/fuzz.json"
cargo bench -p market-squawk --bench event_to_decision_latency \
  --all-features --locked -- --warm-up-events 1000000 --events 10000000 \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --output "$EVIDENCE_DIR/live-bench.json"
cargo bench -p market-squawk-data --bench storage \
  --all-features --locked -- --rows 10000000 \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --output "$EVIDENCE_DIR/data-bench.json"
cargo build -p market-squawk --bin market-squawk-benchmark \
  --release --all-features --locked
python3 scripts/run_sustained_benchmark.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --binary target/release/market-squawk-benchmark --warm-up-events 1000000 \
  --events 60000000 --max-tail-growth-mib 32 --max-tail-growth-percent 1 \
  --report "$EVIDENCE_DIR/sustained-memory.json"
python3 scripts/check_release_performance.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --live "$EVIDENCE_DIR/live-bench.json" \
  --data "$EVIDENCE_DIR/data-bench.json" \
  --memory "$EVIDENCE_DIR/sustained-memory.json" \
  --min-events-per-second 100000 --max-warmed-p99-ns 999999 \
  --report "$EVIDENCE_DIR/performance.json"

test "$MARKET_SQUAWK_EXTERNAL_NETWORK" = "1"
test "$MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED" = "1"
export MARKET_SQUAWK_RELEASE_EVIDENCE_DIR="$EVIDENCE_DIR/providers"
cargo test -p market-squawk --test external_coinbase_vertical \
  --all-features --locked -- --ignored --nocapture
cargo test -p market-squawk --test external_kraken_vertical \
  --all-features --locked -- --ignored --nocapture
cargo test -p market-squawk --test external_fred_alfred_vertical \
  --all-features --locked -- --ignored --nocapture
python3 scripts/check_external_release_evidence.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --evidence-dir "$EVIDENCE_DIR/providers" \
  --require-direct-verified-action --require-fred-alfred-rights \
  --report "$EVIDENCE_DIR/provider-validation.json"

python3 scripts/verify_onnx_runtime.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --policy docs/verification/onnx-runtime-policy.json \
  --library "$MARKET_SQUAWK_ONNX_RUNTIME" \
  --report "$EVIDENCE_DIR/onnx-runtime.json"
python3 scripts/build_python_release.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --wheelhouse .agents/cache/python-wheelhouse \
  --lock python/wheelhouse-lock.json \
  --venv target/usable-release-venv --offline \
  --report "$EVIDENCE_DIR/python-build.json"
target/usable-release-venv/bin/python -m pytest python/tests -q \
  --junitxml "$EVIDENCE_DIR/python-tests.xml"
MARKET_SQUAWK_PYTHON=target/usable-release-venv/bin/python \
  python3 scripts/demo_usable_release.py --offline-fixtures \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --provider-evidence "$EVIDENCE_DIR/providers" \
  --report "$EVIDENCE_DIR/demo.json"
./scripts/verify.sh 2>&1 | tee "$EVIDENCE_DIR/full-gate.log"
python3 scripts/check_release_evidence.py \
  --head "$HEAD_SHA" --tree "$TREE_SHA" \
  --evidence-dir "$EVIDENCE_DIR" \
  --binary target/release/market-squawk \
  --benchmark-binary target/release/market-squawk-benchmark \
  --report "$EVIDENCE_DIR/manifest.json"
git diff --exit-code
test -z "$(git status --porcelain)"
git status --short --branch
```

Expected: every command exits 0; Git remains clean because every generated artifact is ignored; and
the manifest binds the complete artifact set to the unchanged candidate. The retained validators
check real artifacts and measurements. No wrapper tests command ordering or plan text.

- [ ] **Step 7: Run grouped review and repeat the full evidence block after any remediation**

Dispatch non-mutating reviewers in maximum parallel batches for: live/source/qualification/risk/
paper; catalog/storage/PIT/provider rights; analytics/Python/modeling/backtest/portfolio/fair value;
MCP/CLI/security/operations/supply chain; and performance/evidence/release truth. Freeze the candidate
between batches. Reviewers return findings without editing the candidate. The integrator unions and
deduplicates every finding into one concise `usable-release-review.md`, remediates every substantiated
Critical/Important/Minor in disjoint lanes, and commits the report with the remediation and any
truthful README changes. Do not create per-task report files or a report-generation script.

Before each remediation commit, the integration owner copies the exact approved paths from
`usable-release-path-ownership.json` and stages those literal paths only, then records that literal
command in the checkpoint handoff. A placeholder, glob, generated path list, or custom staging
wrapper is forbidden. Any remediation commit creates a new candidate and invalidates all prior
artifacts, even when the changed file is documentation.

For every replacement candidate, rerun the entire Step 6 command block into its new HEAD-keyed
directory, then dispatch the same grouped review domains against that unchanged head and manifest.
Continue this remediation/evidence/re-review loop until the reviewers report zero substantiated
findings of every severity. Final reviewer approvals and artifact digests are published in the PR and
release attestation without changing the approved commit. A final approval is never appended to a
tracked report after the freeze.

- [ ] **Step 8: Evaluate the terminal predicate**

Set release complete only when all statements are true:

```text
all mandatory producers and terminal consumers execute locally
Coinbase and Kraken quality/coverage are honest; verified automated paper action has authorized direct evidence
FRED/ALFRED required use is lawfully admitted or the release remains blocked
all required datasets have real producers and consumers
Python data/finance/training and model-bundle handoff work offline
native and constrained ONNX inference work; every error produces no action
backtest and portfolio accounting reconcile
fair-value Level 1/no-promotion rules pass
complete bounded/audited/cancellable CLI and MCP domains work
deterministic demo, full gates, audits, fuzz and measured reports pass at exact SHA
grouped review has zero unresolved finding of every severity
repository and all integrated worktrees are clean
```

Any false predicate means continue the owning task; weighting or stage completion cannot authorize a
stop.

- [ ] **Step 9: Publish exact evidence, clean lanes, and stop**

Push only the unchanged reviewed SHA to `origin`, update the active pull request with local evidence,
truthful hosted status and external-smoke status, create the approved release tag, and verify remote
identity. Remove clean inactive worktrees and prune metadata; preserve/escalate any dirty state. Hand
off the exact SHA, README capability state, immutable implementation evidence, demo, performance,
audits, consolidated review, provider rights, remaining optional work, and cleanup inventory. Do not
begin paid adapters, live-money execution, extended replay, or future observability without a new
user instruction.
