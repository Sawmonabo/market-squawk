import * as React from "react"
import { Link } from "react-router-dom"
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  Archive,
  CircleAlert,
  HardDrive,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  Trash2,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Progress } from "@/components/ui/progress"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { OperationsControlRequest, ProductTransport } from "@/lib/transport"

import {
  encryptionLabel,
  formatBytes,
  formatSnapshotTime,
  parseBackupInventory,
  parseBackupJobReceipt,
  parseBackupJobs,
  parseProgramRollbackPreview,
  parseRestorePreview,
  parseRetentionPreview,
  shortBackupId,
  type BackupJob,
  type BackupJobReceipt,
  type BackupManifest,
  type ProgramRollbackPreview,
  type RestorePreview,
  type RetentionPreview,
} from "./contracts"

const INVENTORY_LIMIT = 64
const MAXIMUM_INVENTORY_PAGES = 4
const JOB_LIMIT = 50

type PendingConfirmation =
  | { kind: "create" }
  | { kind: "verify"; backup: BackupManifest }
  | { kind: "retention"; preview: RetentionPreview }
  | { kind: "restore"; preview: RestorePreview }
  | { kind: "programRollback"; preview: ProgramRollbackPreview }

export function BackupRecoveryPage() {
  const product = useProduct()

  if (product.status === "loading") return <BackupLoading />
  if (product.status === "error") {
    return (
      <BackupFrame>
        <Unavailable detail={product.error} onRetry={product.refresh} />
      </BackupFrame>
    )
  }

  return (
    <ReadyBackupRecovery
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadyBackupRecovery({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const scope = bootstrap.runtime
  const [keepLatestInput, setKeepLatestInput] = React.useState("3")
  const [retentionPreview, setRetentionPreview] = React.useState<RetentionPreview | null>(null)
  const [restorePreview, setRestorePreview] = React.useState<RestorePreview | null>(null)
  const [rollbackPreview, setRollbackPreview] = React.useState<ProgramRollbackPreview | null>(null)
  const [pendingConfirmation, setPendingConfirmation] =
    React.useState<PendingConfirmation | null>(null)
  const [receipt, setReceipt] = React.useState<BackupJobReceipt | null>(null)
  const [announcement, setAnnouncement] = React.useState("")

  const inventory = useInfiniteQuery({
    queryKey: productKeys.operation(scope, "Operations", "Operations.ListBackups", {
      limit: INVENTORY_LIMIT,
    }),
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) =>
      transport
        .query({
          query: "operationBackups",
          afterBackupId: pageParam,
          limit: INVENTORY_LIMIT,
        })
        .then(parseBackupInventory),
    getNextPageParam: (page, pages) =>
      pages.length < MAXIMUM_INVENTORY_PAGES
        ? (page.nextAfterBackupId ?? undefined)
        : undefined,
  })

  const inventoryPages = inventory.data?.pages ?? []
  const backups = React.useMemo(() => {
    const seen = new Map<string, BackupManifest>()
    for (const page of inventoryPages) {
      for (const manifest of page.manifests) seen.set(manifest.backupId, manifest)
    }
    return [...seen.values()]
  }, [inventoryPages])
  const inventoryRevision = inventoryPages[0]?.revision ?? null
  const pendingDeletions = inventoryPages[0]?.pendingDeletions ?? 0
  const retentionStale =
    retentionPreview !== null &&
    (inventoryRevision === null || retentionPreview.evidence.revision !== inventoryRevision)

  const retainedJob = useQuery({
    queryKey: productKeys.operation(scope, "job", "Job.List", { limit: JOB_LIMIT }),
    queryFn: () => transport.query({ query: "jobs", limit: JOB_LIMIT }).then(parseBackupJobs),
    enabled: receipt !== null,
    refetchInterval: receipt ? 5_000 : false,
  })
  const trackedJob = receipt
    ? retainedJob.data?.find(
        (job) => job.jobId === receipt.jobId && job.generation === receipt.generation,
      )
    : undefined

  const previewRetention = useMutation({
    mutationFn: (keepLatest: number) =>
      transport
        .query({ query: "operationBackupRetentionPreview", keepLatest })
        .then(parseRetentionPreview),
    onSuccess: (preview) => {
      setRetentionPreview(preview)
      setAnnouncement("Retention preview is ready for review.")
    },
  })
  const previewRestore = useMutation({
    mutationFn: (backupId: string) =>
      transport
        .query({ query: "operationRestorePreview", backupId })
        .then(parseRestorePreview),
    onSuccess: (preview) => {
      setRestorePreview(preview)
      setAnnouncement("Restore preview is ready for review.")
    },
  })
  const previewRollback = useMutation({
    mutationFn: () =>
      transport
        .query({ query: "operationProgramRollbackPreview" })
        .then(parseProgramRollbackPreview),
    onSuccess: (preview) => {
      setRollbackPreview(preview)
      setAnnouncement("Program rollback preview is ready for review.")
    },
  })
  const control = useMutation({
    mutationFn: (request: OperationsControlRequest) =>
      transport.operationsControl(request, true).then(parseBackupJobReceipt),
    onSuccess: async (newReceipt) => {
      setReceipt(newReceipt)
      setPendingConfirmation(null)
      setRetentionPreview(null)
      setRestorePreview(null)
      setRollbackPreview(null)
      setAnnouncement(
        `The service queued job ${newReceipt.jobId.slice(0, 8)}. Completion is not yet confirmed.`,
      )
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "Operations") }),
        queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "job") }),
      ])
    },
  })

  const refresh = () => {
    void Promise.all([inventory.refetch(), retainedJob.refetch()])
  }
  const beginRetentionPreview = () => {
    const keepLatest = Number(keepLatestInput)
    if (!Number.isInteger(keepLatest) || keepLatest < 1 || keepLatest > 128) {
      setAnnouncement("Enter a whole-number retention count from 1 through 128.")
      return
    }
    previewRetention.reset()
    setRetentionPreview(null)
    previewRetention.mutate(keepLatest)
  }
  const confirm = () => {
    if (!pendingConfirmation) return
    control.reset()
    control.mutate(controlRequest(pendingConfirmation))
  }

  return (
    <BackupFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={inventory.isFetching}>
          <RefreshCw className={inventory.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      <p className="sr-only" aria-live="polite">{announcement}</p>

      <Alert>
        <ShieldCheck aria-hidden="true" />
        <AlertTitle>Recovery preserves authority boundaries</AlertTitle>
        <AlertDescription>
          A data restore stages a verified backup into a fresh service-owned workspace, then switches only after validation. It never accepts a browser path or merges files into the active workspace.
        </AlertDescription>
      </Alert>

      <JobReceiptStatus receipt={receipt} job={trackedJob} loading={retainedJob.isFetching} error={retainedJob.error} />

      {control.isError ? (
        <Alert variant="destructive" className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>The service did not queue this operation</AlertTitle>
          <AlertDescription>{messageFrom(control.error)}</AlertDescription>
        </Alert>
      ) : null}

      <section className="mt-6" aria-labelledby="backup-inventory-heading">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Managed inventory</p>
            <h2 id="backup-inventory-heading" className="mt-1 text-xl font-semibold">Verified backups</h2>
            <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
              Immutable backup identities, ownership, encryption, and component evidence. Raw manifests and filesystem locations remain private to the service.
            </p>
          </div>
          <Button onClick={() => setPendingConfirmation({ kind: "create" })} disabled={control.isPending}>
            <Archive aria-hidden="true" />
            Create backup
          </Button>
        </div>

        {inventory.isPending ? <InventoryLoading /> : null}
        {inventory.isError ? <Unavailable detail={messageFrom(inventory.error)} onRetry={() => void inventory.refetch()} /> : null}
        {inventory.isSuccess && backups.length === 0 ? <InventoryEmpty /> : null}
        {backups.length > 0 ? (
          <div className="mt-4 grid gap-3">
            {backups.map((backup) => (
              <BackupCard
                key={backup.backupId}
                backup={backup}
                busy={control.isPending || previewRestore.isPending}
                onVerify={() => setPendingConfirmation({ kind: "verify", backup })}
                onRestore={() => {
                  previewRestore.reset()
                  setRestorePreview(null)
                  previewRestore.mutate(backup.backupId)
                }}
              />
            ))}
          </div>
        ) : null}
        {inventory.hasNextPage ? (
          <Button
            className="mt-4"
            variant="outline"
            onClick={() => void inventory.fetchNextPage()}
            disabled={inventory.isFetchingNextPage}
          >
            {inventory.isFetchingNextPage ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}
            Load more verified backups
          </Button>
        ) : null}
      </section>

      <section className="mt-6 grid gap-4 xl:grid-cols-2" aria-label="Retention and restore controls">
        <RetentionPanel
          keepLatestInput={keepLatestInput}
          setKeepLatestInput={setKeepLatestInput}
          preview={retentionPreview}
          stale={retentionStale}
          pendingDeletions={pendingDeletions}
          previewBusy={previewRetention.isPending}
          previewError={previewRetention.error}
          controlBusy={control.isPending}
          onPreview={beginRetentionPreview}
          onConfirm={() => retentionPreview && setPendingConfirmation({ kind: "retention", preview: retentionPreview })}
        />
        <RestorePanel
          preview={restorePreview}
          previewBusy={previewRestore.isPending}
          previewError={previewRestore.error}
          controlBusy={control.isPending}
          onConfirm={() => restorePreview && setPendingConfirmation({ kind: "restore", preview: restorePreview })}
        />
      </section>

      <ProgramRollbackPanel
        preview={rollbackPreview}
        previewBusy={previewRollback.isPending}
        previewError={previewRollback.error}
        controlBusy={control.isPending}
        onPreview={() => {
          previewRollback.reset()
          setRollbackPreview(null)
          previewRollback.mutate()
        }}
        onConfirm={() => rollbackPreview && setPendingConfirmation({ kind: "programRollback", preview: rollbackPreview })}
      />

      <ConfirmationDialog
        pending={pendingConfirmation}
        submitting={control.isPending}
        error={control.isError ? messageFrom(control.error) : null}
        onClose={() => !control.isPending && setPendingConfirmation(null)}
        onConfirm={confirm}
      />
    </BackupFrame>
  )
}

function BackupCard({
  backup,
  busy,
  onVerify,
  onRestore,
}: {
  backup: BackupManifest
  busy: boolean
  onVerify: () => void
  onRestore: () => void
}) {
  const totalBytes = backup.components
    .reduce((total, component) => total + BigInt(component.byteLength), 0n)
    .toString()
  return (
    <article className="rounded-xl border border-border bg-card/45 p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">Immutable backup</p>
          <h3 className="mt-1 font-mono text-sm font-semibold" title={backup.backupId}>{shortBackupId(backup.backupId)}</h3>
          <p className="mt-1 text-xs text-muted-foreground">Snapshot cutoff {formatSnapshotTime(backup.snapshot.cutoff)}</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button size="sm" variant="outline" disabled={busy} onClick={onVerify}>
            <ShieldCheck aria-hidden="true" /> Verify
          </Button>
          <Button size="sm" disabled={busy} onClick={onRestore}>
            <HardDrive aria-hidden="true" /> Preview restore
          </Button>
        </div>
      </div>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-4 text-xs sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Source workspace" value={shortId(backup.ownership.workspaceId)} />
        <Fact label="Installation" value={shortId(backup.ownership.installationId)} />
        <Fact label="Encryption" value={encryptionLabel(backup.encryption)} />
        <Fact label="Components" value={`${backup.components.length} · ${formatBytes(totalBytes)}`} />
      </dl>
      <div className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
        {backup.components.map((component) => (
          <div key={component.kind} className="rounded-lg border border-border/70 bg-background/35 p-3 text-xs">
            <p className="font-medium">{humanize(component.kind)}</p>
            <p className="mt-1 text-muted-foreground">{component.producer} · schema {String(component.schema.version)}</p>
            <p className="mt-1 text-muted-foreground">{formatBytes(component.byteLength)} · {humanize(component.sensitivity)}</p>
          </div>
        ))}
      </div>
    </article>
  )
}

function RetentionPanel({
  keepLatestInput,
  setKeepLatestInput,
  preview,
  stale,
  pendingDeletions,
  previewBusy,
  previewError,
  controlBusy,
  onPreview,
  onConfirm,
}: {
  keepLatestInput: string
  setKeepLatestInput: (value: string) => void
  preview: RetentionPreview | null
  stale: boolean
  pendingDeletions: number
  previewBusy: boolean
  previewError: unknown
  controlBusy: boolean
  onPreview: () => void
  onConfirm: () => void
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="retention-heading">
      <div className="flex items-start gap-3">
        <Trash2 className="mt-0.5 text-amber-300" aria-hidden="true" />
        <div>
          <h2 id="retention-heading" className="text-lg font-semibold">Retention preview</h2>
          <p className="mt-1 text-sm text-muted-foreground">The service calculates the exact immutable identities it will remove. Nothing is deleted when you make a preview.</p>
        </div>
      </div>
      <div className="mt-4 flex flex-wrap items-end gap-3">
        <label className="grid gap-1 text-xs font-medium" htmlFor="backup-retention-count">
          Keep latest backups (1–128)
          <Input
            id="backup-retention-count"
            inputMode="numeric"
            className="w-44"
            value={keepLatestInput}
            onChange={(event) => setKeepLatestInput(event.target.value)}
          />
        </label>
        <Button variant="outline" onClick={onPreview} disabled={previewBusy || controlBusy}>
          {previewBusy ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RefreshCw aria-hidden="true" />}
          Calculate delete set
        </Button>
      </div>
      {pendingDeletions > 0 ? <p className="mt-3 text-xs text-amber-300">{pendingDeletions.toLocaleString()} deletion{pendingDeletions === 1 ? " is" : "s are"} already being processed by durable work.</p> : null}
      {previewError ? <InlineFailure detail={messageFrom(previewError)} /> : null}
      {preview ? (
        <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
          {stale ? (
            <Alert variant="destructive">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>Retention preview is stale</AlertTitle>
              <AlertDescription>The inventory revision changed. Calculate the exact delete set again before confirmation.</AlertDescription>
            </Alert>
          ) : preview.evidence.deleteBackupIds.length === 0 ? (
            <p className="text-sm text-muted-foreground">The service found no backups to delete at this retention count. No retention job can be started.</p>
          ) : (
            <>
              <p className="text-sm font-medium">This confirmation deletes {preview.evidence.deleteBackupIds.length.toLocaleString()} exact backup {preview.evidence.deleteBackupIds.length === 1 ? "identity" : "identities"}.</p>
              <ul className="mt-3 grid gap-1 font-mono text-[11px] text-muted-foreground">
                {preview.evidence.deleteBackupIds.map((id) => <li key={id}>{shortBackupId(id)}</li>)}
              </ul>
              <Button className="mt-4" variant="destructive" disabled={controlBusy} onClick={onConfirm}>
                <Trash2 aria-hidden="true" /> Review retention deletion
              </Button>
            </>
          )}
        </div>
      ) : null}
    </section>
  )
}

function RestorePanel({
  preview,
  previewBusy,
  previewError,
  controlBusy,
  onConfirm,
}: {
  preview: RestorePreview | null
  previewBusy: boolean
  previewError: unknown
  controlBusy: boolean
  onConfirm: () => void
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="restore-heading">
      <div className="flex items-start gap-3">
        <HardDrive className="mt-0.5 text-primary" aria-hidden="true" />
        <div>
          <h2 id="restore-heading" className="text-lg font-semibold">Data restore</h2>
          <p className="mt-1 text-sm text-muted-foreground">Select a verified backup above to inspect compatibility, active-work fences, and required disk before this page can offer approval.</p>
        </div>
      </div>
      {previewBusy ? <p className="mt-4 flex items-center gap-2 text-sm text-muted-foreground"><LoaderCircle className="animate-spin" aria-hidden="true" /> Preparing restore preview…</p> : null}
      {previewError ? <InlineFailure detail={messageFrom(previewError)} /> : null}
      {preview ? <RestoreEvidence preview={preview} controlBusy={controlBusy} onConfirm={onConfirm} /> : <p className="mt-4 text-xs text-muted-foreground">No restore preview is active. A durable restore cannot be started without one.</p>}
    </section>
  )
}

function RestoreEvidence({ preview, controlBusy, onConfirm }: { preview: RestorePreview; controlBusy: boolean; onConfirm: () => void }) {
  const evidence = preview.evidence
  const diskSufficient =
    BigInt(evidence.availableDiskBytes) >= BigInt(evidence.requiredDiskBytes)
  const permitted = evidence.blockers.length === 0 && evidence.schemaCompatible && diskSufficient
  return (
    <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
      <p className="text-xs font-medium">Selected backup {shortBackupId(evidence.backup.backupId)}</p>
      <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2">
        <Fact label="Active workspace" value={`${shortId(evidence.active.workspaceId)} · generation ${evidence.active.generation}`} />
        <Fact label="Schema compatibility" value={evidence.schemaCompatible ? "Compatible" : "Not compatible"} />
        <Fact label="Available disk" value={formatBytes(evidence.availableDiskBytes)} />
        <Fact label="Required disk" value={formatBytes(evidence.requiredDiskBytes)} />
      </dl>
      {!permitted ? (
        <Alert variant="destructive" className="mt-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Restore is blocked</AlertTitle>
          <AlertDescription>
            <ul className="list-disc pl-4">
              {!evidence.schemaCompatible ? <li>The staged workspace schema is not compatible.</li> : null}
              {!diskSufficient ? <li>Available disk is below the service-required amount.</li> : null}
              {evidence.blockers.map((blocker) => <li key={blocker}>{humanize(blocker)}</li>)}
            </ul>
          </AlertDescription>
        </Alert>
      ) : (
        <>
          <p className="mt-4 text-xs text-muted-foreground">The service will restore this data into a fresh managed workspace, validate it, and atomically switch generation only after health checks. A queued receipt is not completion.</p>
          <Button className="mt-4" disabled={controlBusy} onClick={onConfirm}>
            <HardDrive aria-hidden="true" /> Review durable restore
          </Button>
        </>
      )}
    </div>
  )
}

function ProgramRollbackPanel({
  preview,
  previewBusy,
  previewError,
  controlBusy,
  onPreview,
  onConfirm,
}: {
  preview: ProgramRollbackPreview | null
  previewBusy: boolean
  previewError: unknown
  controlBusy: boolean
  onPreview: () => void
  onConfirm: () => void
}) {
  const allowed = preview && !preview.evidence.activeWorkBlocked && preview.evidence.knownGoodVerified
  return (
    <section className="mt-6 rounded-xl border border-border bg-card/45 p-5" aria-labelledby="program-rollback-heading">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex items-start gap-3">
          <RotateCcw className="mt-0.5 text-amber-300" aria-hidden="true" />
          <div>
            <h2 id="program-rollback-heading" className="text-lg font-semibold">Program rollback is not data restore</h2>
            <p className="mt-1 max-w-3xl text-sm text-muted-foreground">This distinct recovery action changes only the installed program release. It never restores backup data, switches a workspace, or reverses data migrations.</p>
          </div>
        </div>
        <Button variant="outline" disabled={previewBusy || controlBusy} onClick={onPreview}>
          {previewBusy ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RotateCcw aria-hidden="true" />}
          Preview program rollback
        </Button>
      </div>
      {previewError ? <InlineFailure detail={messageFrom(previewError)} /> : null}
      {preview ? (
        <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
          <dl className="grid gap-3 text-xs sm:grid-cols-4">
            <Fact label="Current program generation" value={preview.evidence.currentGeneration} />
            <Fact label="Known-good target" value={preview.evidence.targetVersion} />
            <Fact label="Known-good verification" value={preview.evidence.knownGoodVerified ? "Verified" : "Not verified"} />
            <Fact label="Active-work fence" value={preview.evidence.activeWorkBlocked ? "Blocked" : "Clear"} />
          </dl>
          {allowed ? <Button className="mt-4" variant="outline" disabled={controlBusy} onClick={onConfirm}><RotateCcw aria-hidden="true" /> Review program rollback</Button> : <p className="mt-4 text-xs text-amber-300">The service will not admit rollback until active work is clear and the known-good release is verified.</p>}
        </div>
      ) : null}
    </section>
  )
}

function JobReceiptStatus({ receipt, job, loading, error }: { receipt: BackupJobReceipt | null; job: BackupJob | undefined; loading: boolean; error: unknown }) {
  if (!receipt) return null
  const progress = job?.totalUnits && job.completedUnits !== null
    ? Number((BigInt(job.completedUnits) * 10_000n) / BigInt(job.totalUnits)) / 100
    : null
  return (
    <section className="mt-4 rounded-xl border border-primary/30 bg-primary/5 p-4" aria-labelledby="backup-job-heading">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-wider text-primary">Durable job receipt</p>
          <h2 id="backup-job-heading" className="mt-1 text-sm font-semibold">{job ? humanize(job.kind) : "Queued operation"}</h2>
          <p className="mt-1 text-xs text-muted-foreground">Job {shortId(receipt.jobId)} · generation {receipt.generation} · initial state {humanize(receipt.state)}.</p>
        </div>
        <Button asChild size="sm" variant="outline"><Link to="/system/logs-diagnostics">Open Job surface</Link></Button>
      </div>
      {error ? <InlineFailure detail={`The job could not be refreshed: ${messageFrom(error)}`} /> : null}
      {!job && !error ? <p className="mt-3 text-xs text-muted-foreground">{loading ? "Reconnecting to durable job evidence…" : "The queued job is not in this bounded page yet. Reconnect through the Job surface; this receipt does not prove completion."}</p> : null}
      {job ? (
        <div className="mt-3">
          <div className="flex flex-wrap items-center justify-between gap-2 text-xs"><span className="font-medium">{humanize(job.state)}{job.phase ? ` · ${humanize(job.phase)}` : ""}</span><span className="font-mono text-muted-foreground">{job.completedUnits !== null && job.totalUnits !== null ? `${job.completedUnits.toLocaleString()} / ${job.totalUnits.toLocaleString()} units` : "No measurable progress yet"}</span></div>
          {progress !== null ? <Progress className="mt-2" value={progress} aria-label="Backup job progress" /> : null}
          {job.state === "completed" ? <p className="mt-3 text-xs text-emerald-300">The durable Job surface reports a completed result. After a restore, reconnect to the new workspace generation and confirm its health before continuing work.</p> : null}
          {job.state === "failed" || job.state === "interrupted" ? <p className="mt-3 text-xs text-destructive">{job.failure ? `${humanize(job.failure.class)}: ${humanize(job.failure.diagnostic)}.` : "The job did not complete."} The active workspace remains the recovery authority until the service publishes a successful result.</p> : null}
        </div>
      ) : null}
    </section>
  )
}

function ConfirmationDialog({ pending, submitting, error, onClose, onConfirm }: { pending: PendingConfirmation | null; submitting: boolean; error: string | null; onClose: () => void; onConfirm: () => void }) {
  if (!pending) return null
  const content = confirmationContent(pending)
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader><DialogTitle>{content.title}</DialogTitle><DialogDescription>{content.description}</DialogDescription></DialogHeader>
        {content.details ? <div className="rounded-lg border border-border bg-card/45 p-3 text-xs text-muted-foreground">{content.details}</div> : null}
        {error ? <Alert variant="destructive"><CircleAlert aria-hidden="true" /><AlertTitle>The service did not queue this operation</AlertTitle><AlertDescription>{error}</AlertDescription></Alert> : null}
        <DialogFooter>
          <Button variant="outline" disabled={submitting} onClick={onClose}>Keep current state</Button>
          <Button variant={pending.kind === "retention" ? "destructive" : "default"} disabled={submitting} onClick={onConfirm}>
            {submitting ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}{submitting ? "Submitting" : content.confirm}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function confirmationContent(pending: PendingConfirmation): { title: string; description: string; details: string | null; confirm: string } {
  switch (pending.kind) {
    case "create": return { title: "Create coherent backup?", description: "The service will prepare a coherent snapshot as durable work. This acknowledgement only queues the job.", details: "No browser-selected destination or ambient path is accepted.", confirm: "Queue backup" }
    case "verify": return { title: "Verify this exact backup?", description: "The service will verify the selected immutable backup identity as durable work.", details: `Backup ${shortBackupId(pending.backup.backupId)}.`, confirm: "Queue verification" }
    case "retention": return { title: "Delete the previewed backups?", description: "Only the exact immutable identities in the current retention preview can be removed. The service rejects stale or changed evidence.", details: `${pending.preview.evidence.deleteBackupIds.length.toLocaleString()} backup identities will be scheduled for deletion.`, confirm: "Queue retention deletion" }
    case "restore": return { title: "Restore verified data?", description: "The service will stage this data into a fresh managed workspace, validate it, then switch only after durable health checks. It does not merge into the active workspace.", details: `Active workspace ${shortId(pending.preview.evidence.active.workspaceId)} at generation ${pending.preview.evidence.active.generation}.`, confirm: "Queue durable restore" }
    case "programRollback": return { title: "Roll back the installed program?", description: "This changes program files only. It does not restore backup data, switch a workspace, or reverse a data migration.", details: `Known-good program target ${pending.preview.evidence.targetVersion}.`, confirm: "Queue program rollback" }
  }
}

function controlRequest(pending: PendingConfirmation): OperationsControlRequest {
  switch (pending.kind) {
    case "create": return { action: "startBackup" }
    case "verify": return { action: "startBackupVerification", backupId: pending.backup.backupId }
    case "retention": return { action: "startBackupRetention", previewId: pending.preview.previewId, previewDigest: pending.preview.previewDigest }
    case "restore": return { action: "startRestore", previewId: pending.preview.previewId, previewDigest: pending.preview.previewDigest }
    case "programRollback": return { action: "startProgramRollback", previewId: pending.preview.previewId, previewDigest: pending.preview.previewDigest }
  }
}

function BackupFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7"><div className="flex flex-wrap items-start justify-between gap-3"><div><p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Market Squawk · Operations</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">Backup &amp; Recovery</h1><p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">Create and verify coherent backups, preview retained identities before deletion, and recover through service-owned validation and durable jobs.</p></div>{action}</div><div className="mt-6">{children}</div></div>
}

function InventoryLoading() { return <div className="mt-4 grid gap-3">{Array.from({ length: 3 }, (_, index) => <div key={index} className="h-44 animate-pulse rounded-xl border border-border bg-muted/35" />)}</div> }
function BackupLoading() { return <BackupFrame><InventoryLoading /></BackupFrame> }
function InventoryEmpty() { return <div className="mt-4 rounded-xl border border-dashed border-border bg-card/30 p-6 text-sm text-muted-foreground"><p className="font-medium text-foreground">No verified backups are retained</p><p className="mt-1">Create a coherent backup before relying on recovery or retention controls.</p></div> }
function Unavailable({ detail, onRetry }: { detail: string; onRetry: () => void }) { return <Alert variant="destructive" className="mt-4"><CircleAlert aria-hidden="true" /><AlertTitle>Backup &amp; Recovery is unavailable</AlertTitle><AlertDescription>{detail}<Button className="mt-2" variant="outline" size="sm" onClick={onRetry}>Reconnect</Button></AlertDescription></Alert> }
function InlineFailure({ detail }: { detail: string }) { return <p className="mt-3 text-xs text-destructive" role="alert">{detail}</p> }
function Fact({ label, value }: { label: string; value: React.ReactNode }) { return <div><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 break-words font-medium text-foreground">{value}</dd></div> }
function shortId(value: string): string { return `${value.slice(0, 8)}…${value.slice(-4)}` }
function humanize(value: string): string { const words = value.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").trim(); return words ? words.charAt(0).toUpperCase() + words.slice(1) : "Value" }
