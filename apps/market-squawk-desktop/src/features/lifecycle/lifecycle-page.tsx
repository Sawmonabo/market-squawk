import * as React from "react"
import { Link } from "react-router-dom"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  CheckCircle2,
  CircleAlert,
  Download,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  ShieldX,
  TriangleAlert,
  Trash2,
  Wrench,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
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
import { formatTimestamp } from "@/lib/time"
import type { InstallationStatus } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"

import {
  parseLifecycleJobReceipt,
  parseProgramRollbackPreview,
  parseUpdatePreview,
  parseUpdateStatus,
  type LifecycleJobReceipt,
  type ProgramRollbackPreview,
  type UpdatePreview,
  type UpdateStatus,
} from "./contracts"

type PendingConfirmation =
  | { kind: "check" }
  | { kind: "start-update"; preview: UpdatePreview }
  | { kind: "start-rollback"; preview: ProgramRollbackPreview }
  | { kind: "repair"; installation: InstallationStatus }
  | { kind: "remove"; installation: InstallationStatus }

const UPDATE_OPERATIONS = [
  "Operations.GetUpdateStatus",
  "Operations.CheckForUpdates",
  "Operations.PreviewUpdate",
  "Operations.StartUpdate",
  "Operations.PreviewProgramRollback",
  "Operations.StartProgramRollback",
] as const

export function LifecyclePage() {
  const product = useProduct()

  if (product.status === "loading") return <LifecycleLoading />
  if (product.status === "error") {
    return (
      <LifecycleFrame>
        <UnavailableState detail={product.error} />
      </LifecycleFrame>
    )
  }

  return (
    <ReadyLifecycle
      transport={product.transport}
      scope={product.bootstrap.runtime}
      operations={new Set(product.bootstrap.operations.map((operation) => operation.name))}
    />
  )
}

function ReadyLifecycle({
  transport,
  scope,
  operations,
}: {
  transport: ProductTransport
  scope: ProductScope
  operations: ReadonlySet<string>
}) {
  const queryClient = useQueryClient()
  const [pending, setPending] = React.useState<PendingConfirmation | null>(null)
  const [receipt, setReceipt] = React.useState<LifecycleJobReceipt | null>(null)
  const [notice, setNotice] = React.useState<string | null>(null)
  const supportsUpdateStatus = operations.has("Operations.GetUpdateStatus")
  const supportsLifecycle = UPDATE_OPERATIONS.every((operation) => operations.has(operation))
  const updatePreviewKey = productKeys.operation(
    scope,
    "operations",
    "Operations.PreviewUpdate",
    {},
  )
  const rollbackPreviewKey = productKeys.operation(
    scope,
    "operations",
    "Operations.PreviewProgramRollback",
    {},
  )
  const status = useQuery({
    queryKey: productKeys.operation(scope, "operations", "Operations.GetUpdateStatus", {}),
    enabled: supportsUpdateStatus,
    queryFn: async () => parseUpdateStatus(await transport.query({ query: "operationUpdateStatus" })),
    refetchInterval: 15_000,
  })
  const updatePreview = useQuery({
    queryKey: updatePreviewKey,
    enabled: false,
    queryFn: async () => parseUpdatePreview(await transport.query({ query: "operationUpdatePreview" })),
  })
  const rollbackPreview = useQuery({
    queryKey: rollbackPreviewKey,
    enabled: false,
    queryFn: async () =>
      parseProgramRollbackPreview(
        await transport.query({ query: "operationProgramRollbackPreview" }),
      ),
  })
  const installation = useQuery({
    queryKey: productKeys.operation(scope, "installation", "Installation.Status", {}),
    queryFn: async () => (await transport.installation({ action: "status" }, false)).status,
    refetchInterval: 15_000,
  })
  const lifecycleControl = useMutation({
    mutationFn: async (action: PendingConfirmation) => {
      if (action.kind === "check") {
        return {
          kind: action.kind,
          preview: parseUpdatePreview(
            await transport.operationsControl({ action: "checkForUpdates" }, true),
          ),
        }
      }
      if (action.kind === "start-update") {
        return {
          kind: action.kind,
          receipt: parseLifecycleJobReceipt(
            await transport.operationsControl(
              {
                action: "startUpdate",
                previewId: action.preview.previewId,
                previewDigest: action.preview.previewDigest,
              },
              true,
            ),
          ),
        }
      }
      if (action.kind === "start-rollback") {
        return {
          kind: action.kind,
          receipt: parseLifecycleJobReceipt(
            await transport.operationsControl(
              {
                action: "startProgramRollback",
                previewId: action.preview.previewId,
                previewDigest: action.preview.previewDigest,
              },
              true,
            ),
          ),
        }
      }
      const installationAction = action.kind === "repair" ? "repair" : "uninstall"
      return {
        kind: action.kind,
        installation: await transport.installation({ action: installationAction }, true),
      }
    },
    onSuccess: async (result) => {
      setPending(null)
      if ("preview" in result) {
        setNotice("Trusted metadata was checked and the staged candidate now has a fresh activation preflight.")
        queryClient.setQueryData(updatePreviewKey, result.preview)
      }
      if ("receipt" in result && result.receipt) {
        setReceipt(result.receipt)
        setNotice(
          result.kind === "start-update"
            ? "The update was admitted as a durable job. Reconnect below for activation and health evidence."
            : "The program rollback was admitted as a durable job. It does not restore workspace data.",
        )
      }
      if ("installation" in result) {
        setNotice(
          result.kind === "repair"
            ? "Program repair was accepted by the native installation authority."
            : "Native program removal was accepted. User data remains preserved unless a separate, explicit data-purge workflow is used.",
        )
      }
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(scope, "operations"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(scope, "job"),
        }),
        queryClient.invalidateQueries({
          queryKey: productKeys.domain(scope, "installation"),
        }),
      ])
    },
  })

  const requestPreview = (kind: "update" | "rollback") => {
    setNotice(null)
    if (kind === "update") void updatePreview.refetch()
    else void rollbackPreview.refetch()
  }
  const currentUpdatePreview = updatePreview.data
  const currentRollbackPreview = rollbackPreview.data
  const updatePreviewStale = currentUpdatePreview ? previewExpired(currentUpdatePreview.expiresAt) : false
  const rollbackPreviewStale = currentRollbackPreview
    ? previewExpired(currentRollbackPreview.expiresAt)
    : false

  return (
    <LifecycleFrame
      action={
        <Button
          variant="outline"
          size="sm"
          disabled={status.isFetching || !supportsUpdateStatus}
          onClick={() => void status.refetch()}
        >
          <RefreshCw className={cn(status.isFetching && "animate-spin")} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      {!supportsUpdateStatus ? (
        <UnavailableState detail="The installed service did not advertise the closed update-status operation." />
      ) : status.isPending ? (
        <LoadingEvidence />
      ) : status.isError ? (
        <UnavailableState detail={messageFrom(status.error)} retry={() => void status.refetch()} />
      ) : !status.data ? (
        <UnavailableState detail="The service returned no trusted update-status evidence." />
      ) : (
        <>
          <UpdateStatusPanel status={status.data} />
          {notice ? <SuccessNotice text={notice} /> : null}
          {receipt ? <JobReceipt receipt={receipt} /> : null}

          <section className="mt-6 grid gap-5 xl:grid-cols-[minmax(0,1.12fr)_minmax(320px,0.88fr)]">
            <UpdateFlow
              status={status.data}
              lifecycleAvailable={supportsLifecycle}
              preview={currentUpdatePreview}
              previewLoading={updatePreview.isFetching}
              previewError={updatePreview.isError ? messageFrom(updatePreview.error) : null}
              previewStale={updatePreviewStale}
              busy={lifecycleControl.isPending}
              onCheck={() => setPending({ kind: "check" })}
              onPreview={() => requestPreview("update")}
              onStart={(preview) => setPending({ kind: "start-update", preview })}
            />
            <RollbackFlow
              preview={currentRollbackPreview}
              loading={rollbackPreview.isFetching}
              error={rollbackPreview.isError ? messageFrom(rollbackPreview.error) : null}
              stale={rollbackPreviewStale}
              lifecycleAvailable={supportsLifecycle}
              busy={lifecycleControl.isPending}
              onPreview={() => requestPreview("rollback")}
              onStart={(preview) => setPending({ kind: "start-rollback", preview })}
            />
          </section>
          <NativeProgramControls
            installation={installation.data}
            installationLoading={installation.isPending}
            installationError={installation.isError ? messageFrom(installation.error) : null}
            busy={lifecycleControl.isPending}
            onRepair={(current) => setPending({ kind: "repair", installation: current })}
            onRemove={(current) => setPending({ kind: "remove", installation: current })}
          />
        </>
      )}

      <ConfirmationDialog
        pending={pending}
        busy={lifecycleControl.isPending}
        error={lifecycleControl.isError ? messageFrom(lifecycleControl.error) : null}
        onClose={() => {
          lifecycleControl.reset()
          setPending(null)
        }}
        onConfirm={() => {
          if (pending) lifecycleControl.mutate(pending)
        }}
      />
    </LifecycleFrame>
  )
}

function UpdateStatusPanel({ status }: { status: UpdateStatus }) {
  const available = status.availability === "available"
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex gap-3">
          <span className="flex size-10 items-center justify-center rounded-lg border border-border bg-background">
            {available ? (
              <ShieldCheck className="size-5 text-emerald-400" aria-hidden="true" />
            ) : (
              <ShieldX className="size-5 text-amber-400" aria-hidden="true" />
            )}
          </span>
          <div>
            <h2 className="text-lg font-semibold">Trusted release status</h2>
            <p className="mt-1 max-w-2xl text-sm leading-relaxed text-muted-foreground">
              {availabilityDetail(status.availability)}
            </p>
          </div>
        </div>
        {status.recoveryRequired ? (
          <span className="inline-flex items-center gap-1.5 rounded-full border border-amber-500/40 bg-amber-500/10 px-3 py-1 text-xs font-medium text-amber-300">
            <TriangleAlert className="size-3.5" aria-hidden="true" />
            Recovery required
          </span>
        ) : (
          <span className="inline-flex items-center gap-1.5 rounded-full border border-emerald-500/40 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-300">
            <CheckCircle2 className="size-3.5" aria-hidden="true" />
            Known-good program retained
          </span>
        )}
      </div>
      <dl className="mt-5 grid gap-4 border-t border-border pt-5 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="Known-good version" value={status.knownGoodVersion} />
        <Fact label="Active generation" value={status.currentGeneration} />
        <Fact
          label="Last trusted check"
          value={status.lastCheckedAt ? formatTimestamp(status.lastCheckedAt) : "No completed check recorded"}
        />
        <Fact
          label="Staged candidate"
          value={status.stagedCandidate?.version ?? "No candidate currently staged"}
        />
      </dl>
      {status.recoveryRequired ? (
        <Alert className="mt-5" variant="destructive">
          <TriangleAlert aria-hidden="true" />
          <AlertTitle>Program recovery needs attention</AlertTitle>
          <AlertDescription>
            Do not treat a program rollback as a data restore. Review the program rollback preflight below; backup and restore remain separate data-recovery workflows.
          </AlertDescription>
        </Alert>
      ) : null}
    </section>
  )
}

function UpdateFlow({
  status,
  lifecycleAvailable,
  preview,
  previewLoading,
  previewError,
  previewStale,
  busy,
  onCheck,
  onPreview,
  onStart,
}: {
  status: UpdateStatus
  lifecycleAvailable: boolean
  preview: UpdatePreview | undefined
  previewLoading: boolean
  previewError: string | null
  previewStale: boolean
  busy: boolean
  onCheck: () => void
  onPreview: () => void
  onStart: (preview: UpdatePreview) => void
}) {
  const updateAvailable = status.availability === "available" && lifecycleAvailable
  return (
    <section className="rounded-xl border border-border bg-card/30 p-5" aria-labelledby="update-flow-heading">
      <div className="flex items-start gap-3">
        <Download className="mt-0.5 size-5 text-primary" aria-hidden="true" />
        <div>
          <h2 id="update-flow-heading" className="text-lg font-semibold">Check, stage, and activate</h2>
          <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
            A metadata check stages only an admitted candidate. Activation is a separate, explicitly confirmed durable job.
          </p>
        </div>
      </div>
      {!updateAvailable ? (
        <UnavailableCard
          title="Trusted updates are unavailable"
          detail={
            lifecycleAvailable
              ? availabilityDetail(status.availability)
              : "The installed service did not advertise every closed update operation required for this workflow."
          }
        />
      ) : (
        <>
          <div className="mt-5 flex flex-wrap gap-2">
            <Button disabled={busy} onClick={onCheck}>
              <ShieldCheck aria-hidden="true" />
              Check trusted metadata and stage
            </Button>
            <Button variant="outline" disabled={busy || previewLoading} onClick={onPreview}>
              <RefreshCw className={cn(previewLoading && "animate-spin")} aria-hidden="true" />
              Review current activation preflight
            </Button>
          </div>
          {previewError ? <FailureNotice title="Activation preflight unavailable" detail={previewError} /> : null}
          {preview ? (
            <UpdatePreflight preview={preview} stale={previewStale} busy={busy} onStart={onStart} />
          ) : status.stagedCandidate ? (
            <p className="mt-4 rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
              A trusted candidate is staged. Review a current activation preflight before starting the update.
            </p>
          ) : (
            <p className="mt-4 rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
              No staged candidate is available. A confirmed trusted metadata check is required before activation can be considered.
            </p>
          )}
        </>
      )}
    </section>
  )
}

function UpdatePreflight({
  preview,
  stale,
  busy,
  onStart,
}: {
  preview: UpdatePreview
  stale: boolean
  busy: boolean
  onStart: (preview: UpdatePreview) => void
}) {
  const { activity, candidate, canApprove } = preview.evidence
  return (
    <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="font-medium">Activation preflight for {candidate.version}</h3>
          <p className="mt-1 text-xs text-muted-foreground">
            This preview is bound to the current service, workspace, candidate, and evidence. It expires at {formatTimestamp(preview.expiresAt)}.
          </p>
        </div>
        <PreflightBadge eligible={canApprove && !stale} />
      </div>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2">
        <Fact label="Workspace schema" value={`${activity.schemaVersion} (candidate supports ${candidate.minimumSchemaVersion}–${candidate.maximumSchemaVersion})`} />
        <Fact label="Disk evidence" value={`${formatBytes(activity.availableDiskBytes)} available; ${formatBytes(activity.requiredDiskBytes)} required`} />
        <Fact label="Active mutation jobs" value={String(activity.runningMutationJobs)} />
        <Fact label="Paper execution" value={activity.paperExecutionActive ? "Active — blocks activation" : "Inactive"} />
        <Fact label="Execution reconciliation" value={activity.executionReconciliationPending ? "Pending — blocks activation" : "Current"} />
        <Fact label="Restart and health" value="The update job restarts the program, proves health, and automatically restores the retained program on health failure." />
      </dl>
      {stale ? (
        <FailureNotice title="This preview expired" detail="Get a fresh activation preflight. Preview evidence expires after fifteen minutes and is invalidated by a service restart." />
      ) : !canApprove ? (
        <FailureNotice title="Activation is blocked" detail="Resolve the displayed compatibility, disk, active-work, paper, or reconciliation blocker, then obtain a fresh preview." />
      ) : (
        <Button className="mt-4" disabled={busy} onClick={() => onStart(preview)}>
          <Download aria-hidden="true" />
          Start verified update
        </Button>
      )}
    </div>
  )
}

function RollbackFlow({
  preview,
  loading,
  error,
  stale,
  lifecycleAvailable,
  busy,
  onPreview,
  onStart,
}: {
  preview: ProgramRollbackPreview | undefined
  loading: boolean
  error: string | null
  stale: boolean
  lifecycleAvailable: boolean
  busy: boolean
  onPreview: () => void
  onStart: (preview: ProgramRollbackPreview) => void
}) {
  return (
    <section className="rounded-xl border border-border bg-card/30 p-5" aria-labelledby="rollback-heading">
      <div className="flex items-start gap-3">
        <RotateCcw className="mt-0.5 size-5 text-primary" aria-hidden="true" />
        <div>
          <h2 id="rollback-heading" className="text-lg font-semibold">Program rollback</h2>
          <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
            Selects a retained known-good program release. It never restores workspace data, portfolios, datasets, configuration, or credentials.
          </p>
        </div>
      </div>
      {!lifecycleAvailable ? (
        <UnavailableCard title="Program rollback unavailable" detail="The installed service did not advertise the closed rollback preview and start operations." />
      ) : (
        <>
          <Button className="mt-5" variant="outline" disabled={busy || loading} onClick={onPreview}>
            <RefreshCw className={cn(loading && "animate-spin")} aria-hidden="true" />
            Review program rollback preflight
          </Button>
          {error ? <FailureNotice title="Program rollback preflight unavailable" detail={error} /> : null}
          {preview ? (
            <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <h3 className="font-medium">Retained target {preview.evidence.targetVersion}</h3>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Program generation {preview.evidence.currentGeneration}; preview expires at {formatTimestamp(preview.expiresAt)}.
                  </p>
                </div>
                <PreflightBadge eligible={!stale && !preview.evidence.activeWorkBlocked && preview.evidence.knownGoodVerified} />
              </div>
              <dl className="mt-4 grid gap-3">
                <Fact label="Known-good evidence" value={preview.evidence.knownGoodVerified ? "The rollback target is verified" : "The target is not verified and cannot be selected"} />
                <Fact label="Active work" value={preview.evidence.activeWorkBlocked ? "Active work blocks program rollback" : "No active-work blocker reported"} />
                <Fact label="Data preservation" value="This action changes program files only. Data recovery requires the separate Backup & Recovery workflow." />
              </dl>
              {stale ? (
                <FailureNotice title="This preview expired" detail="Get a fresh program rollback preflight before approval." />
              ) : preview.evidence.activeWorkBlocked || !preview.evidence.knownGoodVerified ? (
                <FailureNotice title="Program rollback is blocked" detail="Resolve active work or recovery evidence, then request a fresh preview." />
              ) : (
                <Button className="mt-4" variant="outline" disabled={busy} onClick={() => onStart(preview)}>
                  <RotateCcw aria-hidden="true" />
                  Start program rollback
                </Button>
              )}
            </div>
          ) : (
            <p className="mt-4 rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
              No program rollback preflight has been requested. The service decides whether a retained known-good program is eligible.
            </p>
          )}
        </>
      )}
    </section>
  )
}

function NativeProgramControls({
  installation,
  installationLoading,
  installationError,
  busy,
  onRepair,
  onRemove,
}: {
  installation: InstallationStatus | undefined
  installationLoading: boolean
  installationError: string | null
  busy: boolean
  onRepair: (installation: InstallationStatus) => void
  onRemove: (installation: InstallationStatus) => void
}) {
  return (
    <section className="mt-6 rounded-xl border border-border bg-card/30 p-5" aria-labelledby="native-controls-heading">
      <h2 id="native-controls-heading" className="text-lg font-semibold">Native program maintenance</h2>
      <p className="mt-1 max-w-3xl text-sm leading-relaxed text-muted-foreground">
        These controls use the platform installation authority, separately from service-owned trusted updates and program rollback. They do not expose paths or delete data by default.
      </p>
      {installationError ? (
        <FailureNotice title="Installed-program evidence is unavailable" detail={installationError} />
      ) : null}
      {installation ? (
        <dl className="mt-4 grid gap-3 rounded-lg border border-border bg-background/35 p-4 sm:grid-cols-3">
          <Fact label="Installed version" value={installation.active_version ?? "Not installed"} />
          <Fact label="Platform target" value={installation.target ?? "Unavailable"} />
          <Fact label="Component verification" value={installation.healthy ? "Healthy" : "Repair required"} />
        </dl>
      ) : null}
      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <div className="rounded-lg border border-border bg-background/35 p-4">
          <Wrench className="size-5 text-primary" aria-hidden="true" />
          <h3 className="mt-3 font-medium">Repair installed programs</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            Verifies installed program components and uses the native repair workflow. User configuration, credentials, portfolios, datasets, models, logs, and artifacts are preserved.
          </p>
          <Button className="mt-4" variant="outline" disabled={busy || installationLoading || !installation?.installed} onClick={() => installation && onRepair(installation)}>
            <Wrench aria-hidden="true" />
            Review and repair
          </Button>
        </div>
        <div className="rounded-lg border border-border bg-background/35 p-4">
          <Trash2 className="size-5 text-destructive" aria-hidden="true" />
          <h3 className="mt-3 font-medium">Remove installed programs</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            Removes native program components only. Data remains preserved; data purge is a separate explicit, inventory-driven action and is not available here.
          </p>
          <Button className="mt-4" variant="destructive" disabled={busy || installationLoading || !installation?.installed} onClick={() => installation && onRemove(installation)}>
            <Trash2 aria-hidden="true" />
            Review program removal
          </Button>
        </div>
      </div>
    </section>
  )
}

function ConfirmationDialog({
  pending,
  busy,
  error,
  onClose,
  onConfirm,
}: {
  pending: PendingConfirmation | null
  busy: boolean
  error: string | null
  onClose: () => void
  onConfirm: () => void
}) {
  const content = pending ? confirmationContent(pending) : null
  return (
    <Dialog open={pending !== null} onOpenChange={(open) => !open && !busy && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{content?.title ?? "Confirm lifecycle action"}</DialogTitle>
          <DialogDescription>{content?.description}</DialogDescription>
        </DialogHeader>
        {content?.scope ? <p className="rounded-lg border border-border bg-muted/35 p-3 text-xs leading-relaxed text-muted-foreground">{content.scope}</p> : null}
        {error ? <FailureNotice title="Nothing changed" detail={error} /> : null}
        <DialogFooter>
          <Button type="button" variant="ghost" disabled={busy} onClick={onClose}>Cancel</Button>
          <Button type="button" variant={content?.destructive ? "destructive" : "default"} disabled={busy || !pending} onClick={onConfirm}>
            {busy ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}
            {content?.confirm ?? "Confirm"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function confirmationContent(pending: PendingConfirmation) {
  switch (pending.kind) {
    case "check":
      return {
        title: "Check trusted metadata and stage a candidate?",
        description: "The service will contact only its immutable trusted update channel and may stage a verified candidate. It will not activate any program release.",
        scope: "This is a confirmed metadata check and staging operation. Activation remains a separate confirmation after a fresh preflight.",
        confirm: "Check and stage",
      }
    case "start-update":
      return {
        title: `Start verified update to ${pending.preview.evidence.candidate.version}?`,
        description: "The service will consume this exact preflight, drain to declared safe boundaries, activate the immutable release, restart, and check program and data health.",
        scope: "If startup health fails, the lifecycle authority restores the retained program. Data migrations are not blindly reversed.",
        confirm: "Start verified update",
      }
    case "start-rollback":
      return {
        title: `Start program rollback to ${pending.preview.evidence.targetVersion}?`,
        description: "The service will select the exact retained known-good program release from this fresh preflight.",
        scope: "Program rollback changes program files only. It does not restore workspace data, portfolios, datasets, settings, logs, or credentials.",
        confirm: "Start program rollback",
      }
    case "repair":
      return {
        title: `Repair installed program ${pending.installation.active_version ?? "release"}?`,
        description: "The native installation authority will verify and repair program components through the platform-safe installation workflow.",
        scope: `Exact program target: ${pending.installation.target ?? "unavailable"}. Current component health: ${pending.installation.healthy ? "verified" : "repair required"}. User data is preserved: configuration, credentials, portfolios, datasets, models, logs, and artifacts are not removed.`,
        confirm: "Repair programs",
      }
    case "remove":
      return {
        title: `Remove installed program ${pending.installation.active_version ?? "release"}?`,
        description: "The native installation authority will remove installed program components for this user.",
        scope: `Exact program target: ${pending.installation.target ?? "unavailable"}. User data is preserved. This is not a data purge and does not remove configuration, credentials, portfolios, datasets, models, logs, or artifacts.`,
        confirm: "Remove programs",
        destructive: true,
      }
  }
}

function JobReceipt({ receipt }: { receipt: LifecycleJobReceipt }) {
  return (
    <Alert className="mt-5">
      <CheckCircle2 aria-hidden="true" />
      <AlertTitle>Durable lifecycle job accepted</AlertTitle>
      <AlertDescription>
        Receipt {receipt.jobId} at generation {receipt.generation} is queued. <Link className="underline underline-offset-4" to="/operations">Reconnect in Operations</Link> for current phase, health evidence, recovery state, and terminal receipt.
      </AlertDescription>
    </Alert>
  )
}

function PreflightBadge({ eligible }: { eligible: boolean }) {
  return (
    <span className={cn("inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium", eligible ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300" : "border-amber-500/40 bg-amber-500/10 text-amber-300")}>
      {eligible ? <CheckCircle2 className="size-3.5" aria-hidden="true" /> : <CircleAlert className="size-3.5" aria-hidden="true" />}
      {eligible ? "Eligible for confirmation" : "Not eligible"}
    </span>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs leading-relaxed text-foreground">{value}</dd>
    </div>
  )
}

function FailureNotice({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert className="mt-4" variant="destructive">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}

function SuccessNotice({ text }: { text: string }) {
  return (
    <Alert className="mt-5">
      <CheckCircle2 aria-hidden="true" />
      <AlertTitle>Lifecycle evidence refreshed</AlertTitle>
      <AlertDescription>{text}</AlertDescription>
    </Alert>
  )
}

function UnavailableState({ detail, retry }: { detail: string; retry?: () => void }) {
  return (
    <Alert variant="destructive">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>Updates and recovery controls are unavailable</AlertTitle>
      <AlertDescription>
        {detail}
        {retry ? <Button className="mt-3" size="sm" variant="outline" onClick={retry}>Retry</Button> : null}
      </AlertDescription>
    </Alert>
  )
}

function UnavailableCard({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="mt-5 rounded-lg border border-dashed border-border p-4">
      <h3 className="text-sm font-medium">{title}</h3>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function LoadingEvidence() {
  return (
    <div className="rounded-xl border border-border bg-card/30 p-6" role="status">
      <LoaderCircle className="size-5 animate-spin text-primary" aria-hidden="true" />
      <p className="mt-3 text-sm text-muted-foreground">Loading trusted update and recovery evidence…</p>
    </div>
  )
}

function LifecycleLoading() {
  return (
    <LifecycleFrame>
      <LoadingEvidence />
    </LifecycleFrame>
  )
}

function LifecycleFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <main className="mx-auto w-full max-w-6xl p-6 lg:p-8">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div className="max-w-3xl">
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">Operations</p>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight">Updates & program recovery</h1>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Review trusted program lifecycle evidence before a confirmed check, activation, rollback, repair, or native program removal.
          </p>
        </div>
        {action}
      </header>
      <div className="mt-6">{children}</div>
    </main>
  )
}

function availabilityDetail(availability: UpdateStatus["availability"]): string {
  switch (availability) {
    case "available":
      return "This installed release has an admitted immutable update channel and trusted metadata authority."
    case "source_or_development_execution":
      return "This source or development execution has no installed update trust channel. It will not invent an update location or trust material."
    case "production_signing_material_unavailable":
      return "This installed package has no admitted production update-signing material. Trusted update staging and activation remain unavailable."
  }
}

function previewExpired(expiresAt: string): boolean {
  try {
    return BigInt(expiresAt) <= BigInt(Date.now()) * 1_000_000n
  } catch {
    return true
  }
}

function formatBytes(value: string): string {
  try {
    const bytes = BigInt(value)
    const units = ["bytes", "KiB", "MiB", "GiB", "TiB"]
    let unit = 0
    let divisor = 1n
    while (unit < units.length - 1 && bytes >= divisor * 1024n) {
      divisor *= 1024n
      unit += 1
    }
    if (unit === 0) return `${bytes.toString()} bytes`
    const whole = bytes / divisor
    const tenths = (bytes % divisor) * 10n / divisor
    return `${whole.toString()}.${tenths.toString()} ${units[unit]}`
  } catch {
    return "Unavailable"
  }
}
