# Backup and recovery

This runbook defines the current safe operator procedure for backing up and restoring a complete
local Market Squawk data root without separating catalogs from their authority and artifact state.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local operators, release engineers, and incident responders |
| Status | Current |
| Last substantive review | 2026-07-23 |
| Reviewed commit | `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` |

## Contents

- [Scope](#scope)
- [Consistency model](#consistency-model)
- [Backup inventory](#backup-inventory)
- [Create a cold whole-root backup](#create-a-cold-whole-root-backup)
- [Restore to a fresh data root](#restore-to-a-fresh-data-root)
- [Validate the restored system](#validate-the-restored-system)
- [Failure-specific recovery](#failure-specific-recovery)
- [Security and retention](#security-and-retention)
- [Related documentation and code](#related-documentation-and-code)
- [External sources](#external-sources)

## Scope

The supported operator procedure at the reviewed commit is a **cold, whole-data-root backup**:
gracefully stop every process that owns the root, copy the complete root as one archive, verify the
archive digest, and restore into an empty destination. This preserves the SQLite catalog and
sidecars, immutable artifacts, current-authority records, paper checkpoints, audits, and journals
as one recovery unit.

This page does not describe a hot backup, a partial dataset export, a merge into a nonempty root, or
manual catalog/artifact repair. The data crate implements a receipt-bound analytical backup and
no-replace restore service, but that service is not exposed through the current CLI or MCP. It must
not be represented as a runnable operator command.

Configuration files, OS-keyring entries, installed Python training releases, optional ONNX Runtime
libraries, and original user-authorized input files can live outside the data root. Back them up or
re-provision them under their own authority; copying the data root alone does not include them.

## Consistency model

The catalog is not an independent index that can be recreated safely from a directory scan. It
contains current-generation, lineage, rights, run, and authority transitions bound to exact
artifact identities. Conversely, copying only `catalog.sqlite3` omits immutable objects and
control-plane authority needed to interpret it.

```mermaid
flowchart LR
    Stop["Graceful stop and terminal reconciliation"]
    Root["One closed data-root state"]
    Archive["Private no-overwrite archive"]
    Digest["Recorded SHA-256 digest"]
    Fresh["Fresh restore destination"]
    Doctor["Application-owned recovery and doctor"]

    Stop --> Root --> Archive --> Digest --> Fresh --> Doctor
```

A valid recovery unit contains every existing entry under the configured root, including:

```text
<data-root>/
├── catalog.sqlite3
├── catalog.sqlite3-wal            when present
├── catalog.sqlite3-shm            when present
├── journal/
├── artifacts/
├── control/
└── authority/                     when a production live source has created it
```

Optional directories may be absent on a new installation. Lock files and recovery manifests are
service-owned state and remain part of the archive; file existence alone does not prove a lock is
currently held.

## Backup inventory

Before creating the archive, record these recovery dependencies without recording secret values:

| Item | Include or record |
| --- | --- |
| Data root | Canonical absolute path and complete directory contents |
| Application | Market Squawk version, Git commit when locally built, and `Cargo.lock` identity |
| Configuration | Exact operator-selected TOML file and non-secret environment/CLI overrides |
| Secret locators | Backend and opaque reference only; the resolved secret remains in its secret store |
| OS keyring | Ensure the operating-system account/keyring backup policy covers required entries |
| Python training release | Exact separately installed release root and manifest/signature evidence |
| Optional ONNX Runtime | Exact admitted library, policy, notice, and admission evidence if used |
| User source records | Original filings, exports, manifests, and licensed/user-owned data outside the root |
| Backup archive | Creation time, source root, byte size, and SHA-256 digest |

Use a destination on storage independent from the active data root. The destination must have
enough free space for the complete root plus archive metadata.

## Create a cold whole-root backup

### 1. Quiesce all owners

1. Stop foreground capture and `bot start` sessions through their normal Ctrl-C or duration path.
2. Stop local MCP clients and allow `market-squawk mcp serve` to complete bounded shutdown.
3. Confirm any paper run reports a complete terminal shutdown and reconciliation.
4. Stop any other Market Squawk process using the same data root.
5. Do not begin the archive while a source, ingestion, query-publication, model-admission, backtest,
   portfolio-import, fair-value, or paper mutation is active.

If a process did not shut down cleanly, preserve the root unchanged and resolve its typed recovery
state before designating the copy as a known-clean backup.

### 2. Create a private no-overwrite archive

Set canonical absolute paths. `BACKUP_PARENT` must already exist on independent private storage;
`BACKUP_DIR` must be new and must not be inside `DATA_ROOT`. The fresh directory makes the archive,
digest, and retained metadata a single no-overwrite recovery point:

```bash
(
  set -eu
  umask 077

  DATA_ROOT="/absolute/path/to/.market-squawk"
  BACKUP_PARENT="/independent/private/storage"
  BACKUP_DIR="$BACKUP_PARENT/market-squawk-2026-07-23"
  BACKUP_ARCHIVE="$BACKUP_DIR/data-root.tar"

  test -d "$DATA_ROOT"
  test -d "$BACKUP_PARENT"
  test ! -e "$BACKUP_DIR"
  case "$BACKUP_DIR" in "$DATA_ROOT"|"$DATA_ROOT"/*) exit 1 ;; esac

  mkdir -m 0700 "$BACKUP_DIR"
  tar -C "$(dirname "$DATA_ROOT")" \
    -cpf "$BACKUP_ARCHIVE" \
    "$(basename "$DATA_ROOT")"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$BACKUP_ARCHIVE" > "$BACKUP_DIR/data-root.tar.sha256"
    shasum -a 256 -c "$BACKUP_DIR/data-root.tar.sha256"
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$BACKUP_ARCHIVE" > "$BACKUP_DIR/data-root.tar.sha256"
    sha256sum -c "$BACKUP_DIR/data-root.tar.sha256"
  else
    exit 1
  fi
)
```

The subshell must exit successfully. A failed `test`, `mkdir`, `tar`, digest write, or digest check
stops the procedure. Never reuse the failed `BACKUP_DIR`; inspect it, remove it only after confirming
that it contains no accepted recovery point, and repeat with another new directory after a clean
shutdown. An archive created while the source root changed is not a supported recovery point.

### 3. Record and verify the archive digest

The macOS commands above create and verify the digest inside the fresh recovery-point directory.
To verify it again later:

```bash
BACKUP_DIR="/independent/private/storage/market-squawk-2026-07-23"
shasum -a 256 -c "$BACKUP_DIR/data-root.tar.sha256"
```

On systems that provide GNU Coreutils, use `sha256sum` for both creation and verification inside the
same fresh directory. Protect the entire recovery-point directory from replacement. A digest
detects changed bytes; it does not prove that the source was quiescent or that an untrusted archive
is safe to extract.

### 4. Retain success evidence

Record:

- successful terminal shutdown evidence;
- canonical source-root identity;
- archive and digest paths;
- archive byte size and SHA-256;
- creation timestamp and host/platform; and
- identities of the separately backed-up configuration, keyring policy, training release, and
  optional native runtime.

## Restore to a fresh data root

Never overlay or merge a backup into an active or previously initialized data root. Restore into a
new empty parent, validate it, then explicitly point configuration at that restored root.

### 1. Verify before extraction

```bash
BACKUP_DIR="/independent/private/storage/market-squawk-2026-07-23"
BACKUP_ARCHIVE="$BACKUP_DIR/data-root.tar"
shasum -a 256 -c "$BACKUP_DIR/data-root.tar.sha256"
tar -tf "$BACKUP_ARCHIVE" >/dev/null
```

Confirm the archive has one expected top-level data-root directory and no absolute or parent (`..`)
paths before extracting data received from another trust boundary.

### 2. Extract into an empty private parent

```bash
(
  set -eu
  umask 077

  BACKUP_ARCHIVE="/independent/private/storage/market-squawk-2026-07-23/data-root.tar"
  RESTORE_PARENT="/absolute/path/to/fresh-restore-parent"

  test -f "$BACKUP_ARCHIVE"
  test ! -e "$RESTORE_PARENT"
  mkdir -m 0700 "$RESTORE_PARENT"
  tar -C "$RESTORE_PARENT" -xpf "$BACKUP_ARCHIVE"
)
```

The parent of `RESTORE_PARENT` must already exist. Plain `mkdir` and fail-fast execution make an
occupied restore path a hard failure; extraction never overlays an existing directory. Set
`RESTORED_DATA_ROOT` to the extracted top-level directory. Preserve ownership and permissions; do
not resolve a startup failure by making the root broadly writable.

### 3. Re-establish external dependencies

- restore the exact non-secret configuration or reconstruct it explicitly;
- ensure opaque secret locators resolve in the current operating-system account's keyring;
- restore the exact verified training release before durable model admissions are opened;
- restore and re-admit an optional ONNX Runtime only when its library and evidence tuple match; and
- make any operator-owned source inputs required for future ingestion available under new explicit
  input capabilities.

When moving to another machine, a keyring locator may survive in configuration while its secret
does not. Re-provision through the supported source setup/activation path; do not copy resolved
credential bytes into configuration.

## Validate the restored system

Point only the validation process at the restored root:

```bash
RESTORED_DATA_ROOT="/absolute/path/to/fresh-restore-parent/.market-squawk"
RESTORED_CONFIG="/absolute/path/to/restored-market-squawk.toml"
RESTORED_TRAINING_RELEASE_ROOT="/absolute/path/to/restored-python-release"

market-squawk \
  --config "$RESTORED_CONFIG" \
  --data-dir "$RESTORED_DATA_ROOT" \
  --training-release-root "$RESTORED_TRAINING_RELEASE_ROOT" \
  config validate
market-squawk \
  --config "$RESTORED_CONFIG" \
  --data-dir "$RESTORED_DATA_ROOT" \
  --training-release-root "$RESTORED_TRAINING_RELEASE_ROOT" \
  doctor
market-squawk \
  --config "$RESTORED_CONFIG" \
  --data-dir "$RESTORED_DATA_ROOT" \
  --training-release-root "$RESTORED_TRAINING_RELEASE_ROOT" \
  dataset list
```

If the restored root has no durable model admission, omit `--training-release-root`; otherwise it
must name the exact separately restored and verified release. Use the same explicit `--config`,
`--data-dir`, and applicable `--training-release-root` coordinates for every domain command below.

Then validate the domains actually present in the backup:

- `source status`, `source coverage`, and `source health` for restored providers;
- `dataset manifest <DATASET>` and bounded dataset queries for current generations;
- `model list` and `model metadata <MODEL>` for admitted bundles;
- `portfolio holdings --account <ACCOUNT>` for imported accounts;
- `backtest show <RUN>` for governed experiments;
- `fair-value list` and evidence reads for measurements; and
- a paper start only after checkpoint recovery, provider state, disk capacity, and operator intent
  are independently acceptable.

Validation is successful only when the application opens the exact authorities and returns bounded
typed results. A directory that merely contains plausible files is not a validated restore.

## Failure-specific recovery

| Failure | Immediate action | Supported recovery boundary |
| --- | --- | --- |
| Source disconnect, gap, stale data, or checksum failure | Keep the affected generation non-executable | Reconnect with a newer generation, obtain required snapshot/checksum evidence, and requalify |
| Interrupted immutable publication | Keep the prior manifest generation current | Let the owning service reconcile exact staged/object/catalog evidence; do not promote files manually |
| Authority-state peer mismatch | Stop mutation of that authority | Reopen through the owning service, which selects the highest verified peer or reports recovery required |
| Paper unclean-run or checkpoint mismatch | Keep paper execution stopped | Reconcile exact audit, configuration, and checkpoint evidence; publish a clean terminal checkpoint |
| Portfolio publication mismatch | Keep the affected account revision unavailable | Restore the exact artifact/publication pair or re-import the original immutable manifest |
| Model admission mismatch | Produce no inference output | Restore the exact bundle, training release, runtime admission, and signatures; re-admit explicitly if identity changed |
| Catalog or artifact corruption | Preserve the root and stop all mutation | Restore the complete known-good root into a fresh location; never splice a catalog and artifact tree from different recovery points |
| Missing keyring entry | Keep provider activation unavailable | Re-provision the secret through supported setup and bind a new validated locator/evidence revision |
| Disk full | Stop new publication and capture | Free capacity outside the root, preserve existing identities, then retry the owning service's exact recovery |

The lower-level analytical backup service creates receipt-bound catalog and artifact bundles and
supports fresh or exact-subset no-replace restore. Until a public product operation composes that
service, whole-root cold backup is the operator-facing recovery procedure.

## Security and retention

- Store archives with owner-only permissions and storage encryption appropriate for the underlying
  filings, portfolios, audit records, and licensed/user-owned data.
- Keep backups outside the active artifact root so ingest, compaction, and artifact inventories
  never discover them as product objects.
- Retain at least two verified recovery points on independent storage before deleting the oldest
  known-good archive.
- Test restores into fresh roots; do not test by overlaying production state.
- Apply the source data's retention and licensing terms to backups as well as active storage.
- Deleting an active artifact or audit is not backup rotation. Rotate only complete external
  archives after a successful newer restore test.

## Related documentation and code

- [Local deployment](../architecture/deployment.md)
- [Research data plane](../architecture/research-data-plane.md)
- [Configuration and secrets](configuration-and-secrets.md)
- [Datasets and query](datasets-and-query.md)
- [Portfolio and paper execution](portfolio-and-paper-execution.md)
- [Troubleshooting](troubleshooting.md)
- [Local path capabilities](../../crates/market-squawk-platform/src/paths.rs)
- [Analytical backup service](../../crates/market-squawk-data/src/analytical_backup.rs)
- [Catalog backup implementation](../../crates/market-squawk-data/src/catalog/backup.rs)
- [Paper checkpoint repository](../../adapters/market-squawk-adapter-paper/src/checkpoint_repository.rs)

## External sources

| Source | Operational relevance | Reviewed |
| --- | --- | --- |
| [SQLite Online Backup API](https://sqlite.org/backup.html) | Defines SQLite's consistent live-snapshot mechanism used by Market Squawk's lower-level analytical backup service | 2026-07-23 |
| [SQLite write-ahead logging](https://sqlite.org/wal.html) | Explains why the live catalog and its WAL/SHM state cannot be treated as unrelated files | 2026-07-23 |
| [SQLite How To Corrupt](https://sqlite.org/howtocorrupt.html) | Documents copy/overwrite and locking patterns that can damage a database | 2026-07-23 |
