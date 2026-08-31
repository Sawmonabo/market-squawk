# Backup and recovery

This runbook operates the installed service's complete product-backup, verification, retention,
restore, workspace-switch, and program-rollback workflows. Use the Desktop's **Backup & Recovery**
and **Settings** pages for the same service-owned procedures, or use the CLI commands below. Do
not copy SQLite files, artifacts, journals, or control files by hand while the service is active.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local operators and incident responders |
| Status | Current working-product procedure; not release approval evidence |
| Last substantive review | 2026-08-03 |

## Scope and safety boundary

A product backup is a service-created, immutable recovery unit for the active workspace. It binds
the catalog, datasets, controlled artifacts, journals, audit/recovery state, model/portfolio
state, provider metadata, and workspace configuration that the service can safely restore together.
The service records the backup's identity as a lowercase SHA-256 value and runs creation,
verification, retention, restore, workspace switching, update activation, and rollback as durable
jobs. A disconnected Desktop or CLI does not cancel an admitted job.

The following boundaries are deliberate:

- A **restore** creates and validates a fresh fenced workspace from one verified backup. It never
  overlays the active workspace or merges two backup generations.
- A **program rollback** changes only the verified installed program generation. It is not a data
  restore and cannot substitute for one.
- Retention deletes only backups selected by an exact service preview. It never deletes raw
  workspace files directly.
- A preview is scoped to the requesting workspace/client, has a returned digest, and is one-use.
  Re-query rather than reusing a stale preview after any change in jobs, disk, workspace, or
  program state.

The examples assume the installed service is available. Installed-only Operations and Setup
commands fail truthfully from an uninstalled/source-only execution rather than constructing a
second recovery authority.

```bash
MSQ="/path/to/installed/market-squawk"
"$MSQ" --output json doctor
"$MSQ" --output json operations backup list --limit 64
```

Success for the preflight is an exit status of `0`, a healthy or explicitly actionable `doctor`
result, and a bounded backup inventory. Preserve stderr separately from JSON output. A `doctor`
result is diagnostic evidence, not a proof that a new backup has completed.

## Create and verify a product backup

### Prerequisites and authority checks

Before creating a backup, confirm that the active workspace is the intended one, free storage is
available, no restore/workspace-switch/update boundary is in progress, and any paper operation is
stopped or has an explicitly healthy recoverable status. If an active job is incompatible, leave
it to complete, cancel it through `Job.Cancel` where allowed, or follow its owning recovery path.
Do not kill the service or remove a lock to make a backup start.

The Desktop displays these readiness facts and the returned job. For CLI operation, inspect jobs
after submission:

```bash
"$MSQ" --output json operations backup create --confirm
# Record the returned jobId and the resulting backupId when the job completes.
"$MSQ" --output json job get <JOB_ID>
"$MSQ" --output json job watch <JOB_ID> --after-sequence 0
```

`operations backup create --confirm` admits one durable job; it is not completion evidence.
Success requires its terminal `Completed` state and a returned backup manifest/`backupId`. If the
job is `AwaitingConfirmation`, use the explicit job-confirmation receipt supplied by the service;
if it is `Interrupted`, use `job get` and the documented retry/recovery state rather than starting
an uncoordinated duplicate.

Verify the exact retained backup before treating it as a recovery point:

```bash
"$MSQ" --output json operations backup verify <BACKUP_ID> --confirm
"$MSQ" --output json job watch <VERIFY_JOB_ID> --after-sequence 0
"$MSQ" --output json operations backup get <BACKUP_ID>
```

The verification job must complete and the final manifest must still identify the same
`backupId`, workspace/generation, content evidence, and verification state. Failure, cancellation,
or a changed/missing manifest means the backup is not an accepted recovery point. Preserve the
job/manifest evidence, free capacity or resolve the named authority failure, and create a new
backup; never edit a manifest or attempt to repair its files manually.

## Inspect and retain recovery points

Use inventory and exact reads rather than directory scans:

```bash
"$MSQ" --output json operations backup list --limit 64
"$MSQ" --output json operations backup get <BACKUP_ID>
```

Record the backup ID, creating/completing job IDs, source workspace identity/generation, manifest
digest, verification outcome, host/program version, and any separately required secret or
provider re-provisioning evidence. Backup contents do not expose secret bytes; moving to another
OS account may still require provider setup through its write-only credential lifecycle.

To change retention, preview the exact consequence first:

```bash
"$MSQ" --output json operations backup retention preview --keep-latest 3
```

Review the returned `previewId`, `previewDigest`, retained IDs, removal IDs, and blockers. Use a
count from `1` through `128`; do not apply retention if the preview removes the only tested
recovery point or a point required by an incident investigation. If the preview is correct, start
the preview-bound job exactly once:

```bash
"$MSQ" --output json operations backup retention apply \
  --preview-id <PREVIEW_UUID> \
  --preview-digest <LOWERCASE_SHA256> \
  --confirm
"$MSQ" --output json job watch <RETENTION_JOB_ID> --after-sequence 0
```

Successful retention has a completed job and a new inventory matching the approved kept/removed
sets. A stale preview, missing backup, capacity failure, or an interrupted job is a fail-closed
result: refresh inventory, preserve the original evidence, and obtain a new preview after the
underlying condition is resolved.

## Restore one backup safely

### Prerequisites

Restore is destructive only to the **new target workspace it creates**; it does not overwrite the
current workspace. It is nevertheless authority-sensitive because it changes the active workspace
after a service-owned fence and forces clients to resynchronize. Before proceeding, stop or
reconcile paper work, allow incompatible durable jobs to finish/cancel, retain the incident
evidence that selected the backup, and ensure the service reports sufficient destination disk.

### Procedure

1. List and inspect the exact verified backup.

   ```bash
   "$MSQ" --output json operations backup get <BACKUP_ID>
   ```

2. Ask the service for a restore preview. It checks the backup, schema compatibility, available
   disk, current workspace fence, and blockers; it does not begin extraction.

   ```bash
   "$MSQ" --output json operations backup restore preview <BACKUP_ID>
   ```

3. Review the returned `previewId`, `previewDigest`, selected backup, active generation, disk
   evidence, schema result, and every blocker. Resolve a blocker through its owner; do not bypass
   it by stopping files or changing a destination path outside the service.

4. Start only the returned preview after explicit confirmation.

   ```bash
   "$MSQ" --output json operations backup restore start \
     --preview-id <PREVIEW_UUID> \
     --preview-digest <LOWERCASE_SHA256> \
     --confirm
   "$MSQ" --output json job watch <RESTORE_JOB_ID> --after-sequence 0
   ```

5. Reconnect every Desktop, CLI, Claude Code, and Codex client after the service reports the new
   workspace generation. Re-run `doctor`, `operations workspace list --limit 64`, and bounded
   domain reads appropriate to the restored evidence (for example, portfolio holdings or model
   metadata). Do not reuse pre-restore client handles, job IDs, or mutation previews.

Restore success requires a terminal completed job, the service-reported post-restore generation and
health evidence, and successful bounded reads from the newly restored workspace. A plausible
directory tree is not success evidence.

If preview or execution fails, the operation remains fail-closed: preserve the original active
workspace, retain the failed job/preflight evidence, and use the returned recovery state. The
service either keeps the active state or reports recovery required; never combine catalog and
artifact trees from separate backups to manufacture a result.

## Switch workspaces and reconnect clients

One signed-in user has one active workspace service authority. To select an already known
workspace, list then preview the switch:

```bash
"$MSQ" --output json operations workspace list --limit 64
"$MSQ" --output json operations workspace switch preview <WORKSPACE_UUID>
```

The preview reports the current/target identity, draining and reconciliation blockers, audit
evidence, and required client resynchronization. Start it only with the returned one-use preview:

```bash
"$MSQ" --output json operations workspace switch start \
  --preview-id <PREVIEW_UUID> \
  --preview-digest <LOWERCASE_SHA256> \
  --confirm
"$MSQ" --output json job watch <SWITCH_JOB_ID> --after-sequence 0
```

Success is a completed job plus the new workspace/generation reported by the service; reconnect
all clients and repeat their safe bootstrap/health handshake. A desktop window or MCP relay that
still carries the old generation must refresh rather than retry a mutation. Workspace conflicts,
active paper operations, active jobs, or a stale preview must be resolved and freshly previewed.

## Program updates and program rollback

This is a program lifecycle procedure, not data recovery. It is available only when the installed
release has trusted update material; source/development execution or a package built without
production signing material reports that truthfully and must not be presented as an update failure.

```bash
"$MSQ" --output json operations update status
"$MSQ" --output json operations update check --confirm
"$MSQ" --output json operations update preview
```

`check` explicitly authorizes metadata contact and stages only an admitted immutable candidate.
Review availability, current/known-good generation, recovery state, selected candidate, component
identity, disk/schema compatibility, and active-work blockers in the preview. Then start the exact
preview and watch its durable job:

```bash
"$MSQ" --output json operations update start \
  --preview-id <PREVIEW_UUID> \
  --preview-digest <LOWERCASE_SHA256> \
  --confirm
"$MSQ" --output json job watch <UPDATE_JOB_ID> --after-sequence 0
```

After a completed update, restart/reconnect clients, run `doctor`, and retain the reported program
generation and health receipt. If activation fails, use the service's recorded recovery outcome;
do not replace individual sidecars, Python components, or manifests.

To return to a known-good **program** generation, first preview, then start the returned
preview-bound rollback:

```bash
"$MSQ" --output json operations update program-rollback preview
"$MSQ" --output json operations update program-rollback start \
  --preview-id <PREVIEW_UUID> \
  --preview-digest <LOWERCASE_SHA256> \
  --confirm
```

The preview must show a verified known-good target and no active-work blocker. A completed rollback
and fresh `doctor` result are success evidence. If the data itself is wrong, stop here and use the
restore procedure; program rollback does not revert data generations.

## Failure modes and evidence preservation

| Failure | Safe action and recovery boundary |
| --- | --- |
| Backup/verification job is failed, cancelled, or interrupted | Preserve its job ID, phase, error, and returned artifact/manifest identity; resolve the named storage/authority condition and create or retry only through the job authority. |
| Restore preview reports disk, schema, or active-work blocker | Do not start it. Free capacity outside the workspace, drain/reconcile the named work, or select a compatible backup, then request a new preview. |
| Preview digest is rejected | It expired, was consumed, or no longer matches current facts. Start again at the relevant preview command. |
| Client reports stale generation after restore/switch/update | Reconnect and re-bootstrap the client; never replay a mutation or handle created under the former generation. |
| Recovery reports required | Stop new mutation, preserve logs/jobs/manifests, and follow the owning service's exact recovery state. Do not manipulate internal paths. |
| Update source/trust is unavailable | Treat it as unavailable package evidence, not permission to use an unverified download or component-level replacement. |

## Local locations and related references

All internal backup, restore, workspace, update, and job paths are service-owned under the selected
workspace/control and artifact roots. The product intentionally returns opaque IDs, manifests,
receipts, and controlled artifacts instead of writable filesystem coordinates. Use the interface
above to inspect or recover them.

Related pages: [configuration and secrets](configuration-and-secrets.md),
[troubleshooting](troubleshooting.md), [CLI reference](../reference/cli.md),
[MCP reference](../reference/mcp.md), [deployment architecture](../architecture/deployment.md),
and [the delivery ledger](../plans/delivery-ledger.md). The code authorities are
[`operations.rs`](../../apps/market-squawk/src/application/operations.rs),
[`backup.rs`](../../apps/market-squawk/src/application/backup.rs), and the installed
[`operations`](../../apps/market-squawk/src/local_product/operations/) composition.
