import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  CheckCircle2,
  CircleAlert,
  KeyRound,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  Save,
  ShieldCheck,
  SlidersHorizontal,
  SwitchCamera,
} from "lucide-react"
import type { LucideIcon } from "lucide-react"

import { messageFrom, useSystem } from "@/app/product-context"
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
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import { compareLosslessIntegers, type LosslessInteger } from "@/lib/lossless-integer"
import { formatTimestamp } from "@/lib/time"
import type { OperationSettingValue, SystemTransport } from "@/lib/transport"

import {
  asOperationSettingValue,
  formatBytes,
  isMatchingSetting,
  parseJobReceipt,
  parseSettingsChangePreview,
  parseSettingsReceipt,
  parseSettingsRollbackPreview,
  parseSettingsSnapshot,
  parseWorkspacePage,
  parseWorkspaceSwitchPreview,
  settingValueToText,
  type JobReceipt,
  type SettingEntry,
  type SettingsChangePreview,
  type SettingsReceipt,
  type SettingsRollbackPreview,
  type WorkspacePage,
  type WorkspaceSwitchPreview,
} from "./contracts"

const WORKSPACE_PAGE_LIMIT = 64

type DraftValues = Record<string, string>
type FieldErrors = Record<string, string>
type Confirmation =
  | { kind: "settings"; preview: SettingsChangePreview }
  | { kind: "rollback"; preview: SettingsRollbackPreview }
  | { kind: "workspace"; preview: WorkspaceSwitchPreview }

export function SettingsPage() {
  const system = useSystem()

  if (system.status === "loading") return <SettingsFrame><SettingsSkeleton /></SettingsFrame>
  if (system.status === "recovery_required") {
    return (
      <SettingsFrame>
        <SecureStorageRecovery
          requiresUnlock={system.serviceBootstrap.requirement === "encrypted_fallback_locked"}
          pending={system.recoveryPending}
          error={system.recoveryError}
          onRecover={system.recoverService}
        />
      </SettingsFrame>
    )
  }
  if (system.status === "unavailable") {
    return (
      <SettingsFrame>
        <Unavailable detail={system.error} />
      </SettingsFrame>
    )
  }

  return (
    <SettingsWorkspace
      transport={system.transport}
      scope={system.bootstrap.productSessionToken}
      refreshSystem={system.refresh}
    />
  )
}

function SettingsWorkspace({
  transport,
  scope,
  refreshSystem,
}: {
  transport: SystemTransport
  scope: ProductScope
  refreshSystem: () => void
}) {
  const queryClient = useQueryClient()
  const [draft, setDraft] = React.useState<DraftValues>({})
  const [fieldErrors, setFieldErrors] = React.useState<FieldErrors>({})
  const [rollbackRevision, setRollbackRevision] = React.useState("")
  const [confirmation, setConfirmation] = React.useState<Confirmation | null>(null)
  const [receipt, setReceipt] = React.useState<SettingsReceipt | null>(null)
  const [switchReceipt, setSwitchReceipt] = React.useState<JobReceipt | null>(null)
  const [announcement, setAnnouncement] = React.useState("")

  const settingsKey = productKeys.operation(scope, "settings", "Operations.GetSettings", {})
  const workspaceKey = productKeys.operation(scope, "workspace", "Operations.ListWorkspaces", {
    limit: WORKSPACE_PAGE_LIMIT,
  })
  const settings = useQuery({
    queryKey: settingsKey,
    queryFn: async () => parseSettingsSnapshot(await transport.systemQuery({ query: "operationSettings" })),
    refetchInterval: 15_000,
  })
  const workspaces = useQuery({
    queryKey: workspaceKey,
    queryFn: async () =>
      parseWorkspacePage(
        await transport.systemQuery({ query: "operationWorkspaces", limit: WORKSPACE_PAGE_LIMIT }),
      ),
    refetchInterval: 15_000,
  })

  React.useEffect(() => {
    if (!settings.data) return
    setDraft((current) =>
      Object.keys(current).length === 0 ? entriesToDraft(settings.data.entries) : current,
    )
  }, [settings.data])

  const settingsPreview = useMutation({
    mutationFn: async (changes: OperationSettingValue[]) => {
      const snapshot = settings.data
      if (!snapshot) throw new Error("Settings are not available yet.")
      return parseSettingsChangePreview(
        await transport.systemQuery({
          query: "operationSettingsChangePreview",
          expectedRevision: snapshot.revision,
          changes,
        }),
      )
    },
    onSuccess: (preview) => setConfirmation({ kind: "settings", preview }),
  })
  const applySettings = useMutation({
    mutationFn: async (preview: SettingsChangePreview) =>
      parseSettingsReceipt(
        await transport.operationsControl(
          {
            action: "applySettingsChange",
            previewId: preview.previewId,
            previewDigest: preview.previewDigest,
          },
          true,
        ),
      ),
    onSuccess: async (next) => {
      setReceipt(next)
      setConfirmation(null)
      setFieldErrors({})
      setDraft({})
      setAnnouncement(`Settings revision ${next.activeRevision} was durably saved.`)
      await queryClient.invalidateQueries({ queryKey: settingsKey })
      refreshSystem()
    },
  })
  const rollbackPreview = useMutation({
    mutationFn: async (targetRevision: string) => {
      const snapshot = settings.data
      if (!snapshot) throw new Error("Settings are not available yet.")
      return parseSettingsRollbackPreview(
        await transport.systemQuery({
          query: "operationSettingsRollbackPreview",
          expectedRevision: snapshot.revision,
          targetRevision,
        }),
      )
    },
    onSuccess: (preview) => setConfirmation({ kind: "rollback", preview }),
  })
  const rollbackSettings = useMutation({
    mutationFn: async (preview: SettingsRollbackPreview) =>
      parseSettingsReceipt(
        await transport.operationsControl(
          {
            action: "rollbackSettings",
            previewId: preview.previewId,
            previewDigest: preview.previewDigest,
          },
          true,
        ),
      ),
    onSuccess: async (next) => {
      setReceipt(next)
      setConfirmation(null)
      setRollbackRevision("")
      setDraft({})
      setAnnouncement(
        `A new durable settings revision ${next.activeRevision} was created from revision ${next.rolledBackFromRevision ?? "the selected retained revision"}.`,
      )
      await queryClient.invalidateQueries({ queryKey: settingsKey })
      refreshSystem()
    },
  })
  const switchPreview = useMutation({
    mutationFn: async (workspaceId: string) =>
      parseWorkspaceSwitchPreview(
        await transport.systemQuery({ query: "operationWorkspaceSwitchPreview", workspaceId }),
      ),
    onSuccess: (preview) => setConfirmation({ kind: "workspace", preview }),
  })
  const switchWorkspace = useMutation({
    mutationFn: async (preview: WorkspaceSwitchPreview) =>
      parseJobReceipt(
        await transport.operationsControl(
          {
            action: "startWorkspaceSwitch",
            previewId: preview.previewId,
            previewDigest: preview.previewDigest,
          },
          true,
        ),
      ),
    onSuccess: async (next) => {
      setSwitchReceipt(next)
      setConfirmation(null)
      setAnnouncement(`Workspace switch job ${next.jobId} is queued for service generation ${next.generation}.`)
      await queryClient.invalidateQueries({ queryKey: productKeys.domain(scope, "job") })
    },
  })

  const updateDraft = (key: string, value: string) => {
    setDraft((current) => ({ ...current, [key]: value }))
    setFieldErrors((current) => {
      const { [key]: _, ...remaining } = current
      return remaining
    })
    settingsPreview.reset()
  }

  const requestSettingsPreview = () => {
    const snapshot = settings.data
    if (!snapshot) return
    const next = collectChanges(snapshot.entries, draft)
    setFieldErrors(next.errors)
    settingsPreview.reset()
    if (Object.keys(next.errors).length > 0) return
    if (next.changes.length === 0) {
      setFieldErrors({ form: "Change at least one locally mutable setting before requesting a preview." })
      return
    }
    settingsPreview.mutate(next.changes)
  }

  const requestRollbackPreview = () => {
    const snapshot = settings.data
    if (!snapshot) return
    rollbackPreview.reset()
    if (!isRetainedRevisionCandidate(rollbackRevision, snapshot.revision)) {
      setFieldErrors({
        rollback: `Enter a positive retained revision lower than the active revision ${snapshot.revision}.`,
      })
      return
    }
    setFieldErrors((current) => {
      const { rollback: _, ...remaining } = current
      return remaining
    })
    rollbackPreview.mutate(rollbackRevision)
  }

  const mutationError = firstError(
    settingsPreview.error,
    applySettings.error,
    rollbackPreview.error,
    rollbackSettings.error,
    switchPreview.error,
    switchWorkspace.error,
  )

  return (
    <SettingsFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            void settings.refetch()
            void workspaces.refetch()
          }}
          disabled={settings.isFetching || workspaces.isFetching}
        >
          <RefreshCw className={settings.isFetching || workspaces.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Refresh facts
        </Button>
      }
    >
      <p className="sr-only" aria-live="polite">{announcement}</p>

      {mutationError ? <MutationFailure error={mutationError} /> : null}
      {settings.isError ? (
        <Unavailable detail={messageFrom(settings.error)} />
      ) : settings.isPending ? (
        <SettingsSkeleton />
      ) : settings.data ? (
        <>
          <SettingsSummary snapshot={settings.data} />
          <section className="mt-6" aria-labelledby="typed-settings-heading">
            <SectionHeading
              eyebrow="Validated configuration"
              title="Typed settings"
              detail="Every effective value identifies its precedence origin, local mutability, enforced bound, and service impact. Secret values and raw configuration files are never exposed here."
            />
            <div className="mt-4 grid gap-3 xl:grid-cols-2">
              {settings.data.entries.map((entry) => (
                <SettingCard
                  key={entry.key}
                  entry={entry}
                  value={draft[entry.key] ?? settingValueToText(entry)}
                  error={fieldErrors[entry.key]}
                  onChange={(value) => updateDraft(entry.key, value)}
                />
              ))}
            </div>
            {fieldErrors.form ? <InlineError>{fieldErrors.form}</InlineError> : null}
            <div className="mt-4 flex flex-wrap gap-3">
              <Button
                onClick={requestSettingsPreview}
                disabled={settingsPreview.isPending || applySettings.isPending || rollbackSettings.isPending}
              >
                {settingsPreview.isPending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Save aria-hidden="true" />}
                Preview settings change
              </Button>
              <Button
                variant="outline"
                onClick={() => setDraft(entriesToDraft(settings.data.entries))}
                disabled={settingsPreview.isPending || applySettings.isPending}
              >
                Discard local edits
              </Button>
            </div>
          </section>

          <RollbackPanel
            activeRevision={settings.data.revision}
            targetRevision={rollbackRevision}
            error={fieldErrors.rollback}
            pending={rollbackPreview.isPending || rollbackSettings.isPending}
            onTargetChange={(value) => {
              setRollbackRevision(value)
              rollbackPreview.reset()
            }}
            onPreview={requestRollbackPreview}
          />
          {receipt ? <SettingsReceiptCard receipt={receipt} /> : null}
        </>
      ) : null}

      <WorkspaceSection
        page={workspaces.data}
        pending={switchPreview.isPending || switchWorkspace.isPending}
        error={workspaces.isError ? messageFrom(workspaces.error) : null}
        loading={workspaces.isPending}
        onPreview={(workspaceId) => {
          switchPreview.reset()
          switchPreview.mutate(workspaceId)
        }}
      />
      {switchReceipt ? <WorkspaceReceiptCard receipt={switchReceipt} /> : null}

      <ConfirmationDialog
        confirmation={confirmation}
        settingsPending={applySettings.isPending}
        rollbackPending={rollbackSettings.isPending}
        switchPending={switchWorkspace.isPending}
        error={confirmationError(confirmation, applySettings.error, rollbackSettings.error, switchWorkspace.error)}
        onDismiss={() => {
          if (!applySettings.isPending && !rollbackSettings.isPending && !switchWorkspace.isPending) {
            setConfirmation(null)
          }
        }}
        onConfirm={() => {
          if (!confirmation) return
          if (confirmation.kind === "settings") applySettings.mutate(confirmation.preview)
          if (confirmation.kind === "rollback") rollbackSettings.mutate(confirmation.preview)
          if (confirmation.kind === "workspace") switchWorkspace.mutate(confirmation.preview)
        }}
      />
    </SettingsFrame>
  )
}

function SettingsSummary({ snapshot }: { snapshot: { revision: LosslessInteger; digest: string; entries: SettingEntry[] } }) {
  const mutable = snapshot.entries.filter((entry) => entry.locallyMutable).length
  const restart = snapshot.entries.filter((entry) => entry.restartImpact === "service_restart").length
  return (
    <div className="grid gap-3 sm:grid-cols-3">
      <Fact icon={SlidersHorizontal} label="Active revision" value={snapshot.revision} detail="Monotonic durable settings authority." />
      <Fact icon={ShieldCheck} label="Locally mutable" value={`${mutable} of 9`} detail="The rest are controlled by their declared origin." />
      <Fact icon={RefreshCw} label="Restart-sensitive" value={`${restart} settings`} detail="Only previewed changes state their actual combined impact." />
      <div className="sm:col-span-3 rounded-lg border border-border bg-card/35 p-4">
        <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Active settings digest</p>
        <p className="mt-2 break-all font-mono text-xs">{snapshot.digest}</p>
      </div>
    </div>
  )
}

function SettingCard({
  entry,
  value,
  error,
  onChange,
}: {
  entry: SettingEntry
  value: string
  error?: string
  onChange: (value: string) => void
}) {
  const title = settingTitle(entry.key)
  const mutable = entry.locallyMutable && isMatchingSetting(entry)
  const description = settingDescription(entry.key)
  const inputId = `setting-${entry.key}`
  return (
    <article className="rounded-xl border border-border bg-card/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <h3 className="font-semibold">{title}</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">{description}</p>
        </div>
        <span className={mutable ? "rounded-full bg-primary/15 px-2 py-1 text-[10px] font-medium text-primary" : "rounded-full bg-muted px-2 py-1 text-[10px] font-medium text-muted-foreground"}>
          {mutable ? "Locally editable" : "Managed / read-only"}
        </span>
      </div>
      <div className="mt-4">
        <Label htmlFor={inputId}>{title} effective value</Label>
        <SettingInput entry={entry} id={inputId} value={value} disabled={!mutable} onChange={onChange} />
        {error ? <p className="mt-1 text-xs text-destructive">{error}</p> : null}
      </div>
      <dl className="mt-4 grid gap-2 text-xs sm:grid-cols-3">
        <div><dt className="text-muted-foreground">Origin</dt><dd className="mt-0.5 font-medium">{originLabel(entry.origin)}</dd></div>
        <div><dt className="text-muted-foreground">Validation</dt><dd className="mt-0.5 font-medium">{validationLabel(entry.key)}</dd></div>
        <div><dt className="text-muted-foreground">Impact</dt><dd className="mt-0.5 font-medium">{restartLabel(entry.restartImpact)}</dd></div>
      </dl>
    </article>
  )
}

function SettingInput({ entry, id, value, disabled, onChange }: { entry: SettingEntry; id: string; value: string; disabled: boolean; onChange: (value: string) => void }) {
  if (entry.key === "automatic_update_checks") {
    return (
      <select id={id} value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} className="mt-2 h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm disabled:cursor-not-allowed disabled:opacity-50">
        <option value="true">Enabled</option><option value="false">Disabled</option>
      </select>
    )
  }
  if (entry.key === "log_minimum_severity") {
    return <select id={id} value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} className="mt-2 h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm disabled:cursor-not-allowed disabled:opacity-50"><option value="trace">Trace</option><option value="debug">Debug</option><option value="info">Info</option><option value="warn">Warn</option><option value="error">Error</option></select>
  }
  if (entry.key === "update_channel") {
    return <select id={id} value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} className="mt-2 h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm disabled:cursor-not-allowed disabled:opacity-50"><option value="stable">Stable</option><option value="preview">Preview</option></select>
  }
  return <Input id={id} className="mt-2 font-mono" inputMode="numeric" value={value} disabled={disabled} onChange={(event) => onChange(event.target.value)} />
}

function RollbackPanel({ activeRevision, targetRevision, error, pending, onTargetChange, onPreview }: { activeRevision: LosslessInteger; targetRevision: string; error?: string; pending: boolean; onTargetChange: (value: string) => void; onPreview: () => void }) {
  return (
    <section className="mt-6 rounded-xl border border-border bg-card/35 p-5" aria-labelledby="settings-rollback-heading">
      <SectionHeading eyebrow="Retained revisions" title="Preview settings rollback" detail="A retained older revision is validated by the service before any action. Rollback creates a new monotonic revision; it never resurrects or overwrites history." />
      <div className="mt-4 flex max-w-xl flex-col gap-3 sm:flex-row sm:items-end">
        <div className="flex-1"><Label htmlFor="rollback-revision">Retained revision</Label><Input id="rollback-revision" className="mt-2 font-mono" inputMode="numeric" value={targetRevision} onChange={(event) => onTargetChange(event.target.value)} placeholder={`Lower than ${activeRevision}`} aria-invalid={Boolean(error)} /></div>
        <Button variant="outline" onClick={onPreview} disabled={pending}><RotateCcw aria-hidden="true" />{pending ? "Preparing…" : "Preview rollback"}</Button>
      </div>
      {error ? <InlineError>{error}</InlineError> : null}
    </section>
  )
}

function SettingsReceiptCard({ receipt }: { receipt: SettingsReceipt }) {
  const rollback = receipt.rolledBackFromRevision !== null
  return (
    <section className="mt-6 rounded-xl border border-primary/35 bg-primary/5 p-5" aria-labelledby="settings-receipt-heading">
      <div className="flex gap-3"><CheckCircle2 className="mt-0.5 size-5 text-primary" aria-hidden="true" /><div><h2 id="settings-receipt-heading" className="font-semibold">{rollback ? "Rollback outcome recorded" : "Durable save receipt"}</h2><p className="mt-1 text-sm text-muted-foreground">The service persisted the change before publishing this receipt.</p></div></div>
      <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-3"><ReceiptFact label="Previous revision" value={receipt.previousRevision} /><ReceiptFact label="Active revision" value={receipt.activeRevision} /><ReceiptFact label="Service impact" value={restartLabel(receipt.restartImpact)} />{rollback ? <ReceiptFact label="Rolled back from" value={receipt.rolledBackFromRevision ?? "Unavailable"} /> : null}</dl>
      <p className="mt-4 break-all font-mono text-[11px] text-muted-foreground">Active digest: {receipt.activeDigest}</p>
    </section>
  )
}

function WorkspaceSection({ page, pending, loading, error, onPreview }: { page?: WorkspacePage; pending: boolean; loading: boolean; error: string | null; onPreview: (workspaceId: string) => void }) {
  return (
    <section className="mt-8" aria-labelledby="workspaces-heading">
      <SectionHeading eyebrow="One active local workspace" title="Workspace switching" detail="The service owns workspace paths and authority. This bounded inventory is the only data-location descriptor presented to the desktop; no filesystem path or direct workspace mutation is available." />
      {loading ? <div className="mt-4 grid gap-3 sm:grid-cols-2"><Skeleton className="h-44" /><Skeleton className="h-44" /></div> : error ? <Unavailable detail={error} /> : page ? <>
        <div className="mt-4 rounded-lg border border-border bg-card/35 p-4"><p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Active runtime</p><p className="mt-2 font-semibold">Workspace {page.active.workspaceId}</p><p className="mt-1 text-sm text-muted-foreground">Active generation {page.active.generation}. A completed switch forces connected clients to re-sync to the new workspace and generation.</p></div>
        <div className="mt-4 grid gap-3 xl:grid-cols-2">{page.workspaces.map((workspace) => <WorkspaceCard key={workspace.workspaceId} workspace={workspace} active={workspace.workspaceId === page.active.workspaceId} pending={pending} onPreview={onPreview} />)}</div>
        {page.nextAfterWorkspaceId ? <p className="mt-3 text-xs text-muted-foreground">This inventory reached the service page limit of {WORKSPACE_PAGE_LIMIT}; additional workspaces remain undisclosed until queried through the bounded service workflow.</p> : null}
      </> : null}
    </section>
  )
}

function WorkspaceCard({ workspace, active, pending, onPreview }: { workspace: WorkspacePage["workspaces"][number]; active: boolean; pending: boolean; onPreview: (workspaceId: string) => void }) {
  return (
    <article className="rounded-xl border border-border bg-card/35 p-4"><div className="flex items-start justify-between gap-3"><div><h3 className="font-semibold">{workspace.displayName}</h3><p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">{workspace.workspaceId}</p></div><span className={active ? "rounded-full bg-primary/15 px-2 py-1 text-[10px] font-medium text-primary" : "rounded-full bg-muted px-2 py-1 text-[10px] font-medium text-muted-foreground"}>{active ? "Active" : humanize(workspace.health)}</span></div><dl className="mt-4 grid gap-2 text-xs sm:grid-cols-3"><div><dt className="text-muted-foreground">Service descriptor</dt><dd className="mt-0.5 font-medium">Schema {workspace.schemaVersion}</dd></div><div><dt className="text-muted-foreground">Health</dt><dd className="mt-0.5 font-medium">{humanize(workspace.health)}</dd></div><div><dt className="text-muted-foreground">Estimated data</dt><dd className="mt-0.5 font-medium">{formatBytes(workspace.estimatedBytes)}</dd></div></dl>{active ? <p className="mt-4 text-xs text-muted-foreground">This runtime is already active. Its actual local path remains service-owned and is not shown.</p> : <Button className="mt-4" variant="outline" size="sm" disabled={pending} onClick={() => onPreview(workspace.workspaceId)}><SwitchCamera aria-hidden="true" />Preview switch</Button>}</article>
  )
}

function WorkspaceReceiptCard({ receipt }: { receipt: JobReceipt }) {
  return <section className="mt-6 rounded-xl border border-primary/35 bg-primary/5 p-5"><div className="flex gap-3"><CheckCircle2 className="mt-0.5 size-5 text-primary" aria-hidden="true" /><div><h2 className="font-semibold">Workspace switch receipt</h2><p className="mt-1 text-sm text-muted-foreground">The service accepted a durable transition job. The active workspace does not change until it drains, reconciles, restarts, and publishes the next generation.</p></div></div><dl className="mt-4 grid gap-3 text-sm sm:grid-cols-3"><ReceiptFact label="Job" value={receipt.jobId} /><ReceiptFact label="Generation" value={receipt.generation} /><ReceiptFact label="Sequence" value={receipt.sequence} /></dl></section>
}

function ConfirmationDialog({ confirmation, settingsPending, rollbackPending, switchPending, error, onDismiss, onConfirm }: { confirmation: Confirmation | null; settingsPending: boolean; rollbackPending: boolean; switchPending: boolean; error: Error | null; onDismiss: () => void; onConfirm: () => void }) {
  const pending = settingsPending || rollbackPending || switchPending
  if (!confirmation) return null
  const contents = confirmation.kind === "settings" ? settingsConfirmation(confirmation.preview) : confirmation.kind === "rollback" ? rollbackConfirmation(confirmation.preview) : workspaceConfirmation(confirmation.preview)
  return <Dialog open onOpenChange={(open) => { if (!open && !pending) onDismiss() }}><DialogContent><DialogHeader><DialogTitle>{contents.title}</DialogTitle><DialogDescription>{contents.description}</DialogDescription></DialogHeader>{contents.body}{error ? <MutationFailure error={error} /> : null}<DialogFooter><Button variant="outline" disabled={pending} onClick={onDismiss}>Cancel</Button><Button variant={confirmation.kind === "rollback" ? "destructive" : "default"} disabled={pending || contents.blocked} onClick={onConfirm}>{pending ? "Submitting…" : contents.action}</Button></DialogFooter></DialogContent></Dialog>
}

function settingsConfirmation(preview: SettingsChangePreview) {
  return { title: "Apply this settings preview?", description: `This exact preview expires ${formatTimestamp(preview.expiresAt)}. Applying it persists revision ${preview.evidence.currentRevision} changes only after your explicit confirmation.`, action: "Save settings", blocked: false, body: <PreviewFacts facts={[ ["Changes", preview.evidence.changes.map((change) => settingTitle(change.kind)).join(", ")], ["Combined impact", restartLabel(preview.evidence.restartImpact)], ["Preview digest", preview.previewDigest] ]} /> }
}

function rollbackConfirmation(preview: SettingsRollbackPreview) {
  return { title: "Create a rollback revision?", description: `This exact preview expires ${formatTimestamp(preview.expiresAt)}. It creates a new revision from retained revision ${preview.evidence.targetRevision}; it does not resurrect historical state.`, action: "Create rollback revision", blocked: false, body: <PreviewFacts facts={[["Current revision", preview.evidence.currentRevision], ["Retained target", preview.evidence.targetRevision], ["Restart required", preview.evidence.restartRequired ? "Yes" : "No"], ["Result digest", preview.evidence.digest]]} /> }
}

function workspaceConfirmation(preview: WorkspaceSwitchPreview) {
  const activity = preview.evidence.activity
  const blocked = preview.evidence.blockers.length > 0
  return { title: "Start this workspace switch?", description: blocked ? "The service cannot start this exact transition while its reported blockers remain. Review the blockers, resolve them in their owning workflow, then request a new preview." : `This exact preview expires ${formatTimestamp(preview.expiresAt)}. The service will drain work, preserve reconciliation requirements, restart into the target workspace, and force connected clients to re-sync.`, action: "Start workspace switch", blocked, body: <><PreviewFacts facts={[["Active", `${preview.evidence.active.workspaceId} · generation ${preview.evidence.active.generation}`], ["Exact target", preview.evidence.target], ["Running jobs", String(activity.runningJobs)], ["Active sources", String(activity.activeSources)], ["Connected clients to re-sync", String(activity.connectedClients)], ["Available / required disk", `${formatBytes(activity.availableDiskBytes)} / ${formatBytes(activity.requiredDiskBytes)}`], ["Schema compatible", activity.schemaCompatible ? "Yes" : "No"]]} />{activity.paperExecutionActive || activity.executionReconciliationPending ? <Alert className="mt-3"><CircleAlert aria-hidden="true" /><AlertTitle>Drain and reconciliation are service-owned</AlertTitle><AlertDescription>Paper execution and execution reconciliation are checked before transition. The desktop cannot bypass, kill, or directly alter those workflows.</AlertDescription></Alert> : null}{blocked ? <InlineError>Blockers: {preview.evidence.blockers.map(humanize).join(", ")}.</InlineError> : null}</> }
}

function PreviewFacts({ facts }: { facts: [string, string][] }) { return <dl className="grid gap-2 rounded-lg border border-border bg-muted/30 p-3 text-xs">{facts.map(([label, value]) => <div key={label}><dt className="text-muted-foreground">{label}</dt><dd className="mt-0.5 break-all font-medium">{value}</dd></div>)}</dl> }

function Fact({ icon: Icon, label, value, detail }: { icon: LucideIcon; label: string; value: string; detail: string }) { return <div className="rounded-lg border border-border bg-card/35 p-4"><Icon className="size-4 text-primary" aria-hidden="true" /><p className="mt-3 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">{label}</p><p className="mt-1 text-sm font-semibold">{value}</p><p className="mt-1 text-xs leading-5 text-muted-foreground">{detail}</p></div> }
function ReceiptFact({ label, value }: { label: string; value: string }) { return <div><dt className="text-xs text-muted-foreground">{label}</dt><dd className="mt-1 break-all font-medium">{value}</dd></div> }
function SectionHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) { return <div><p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">{eyebrow}</p><h2 className="mt-2 text-xl font-semibold">{title}</h2><p className="mt-2 max-w-4xl text-sm leading-6 text-muted-foreground">{detail}</p></div> }
function InlineError({ children }: { children: React.ReactNode }) { return <p className="mt-3 text-sm text-destructive" role="alert">{children}</p> }
function MutationFailure({ error }: { error: unknown }) { const detail = messageFrom(error); const stale = /preview|expired|stale|revision|conflict/i.test(detail); return <Alert variant="destructive" className="mt-5"><CircleAlert aria-hidden="true" /><AlertTitle>{stale ? "Preview is stale or conflicts with current service state" : "The requested change was not completed"}</AlertTitle><AlertDescription>{detail}{stale ? <p>Refresh the service facts, resolve the reported conflict, and create a new preview before confirming again.</p> : null}</AlertDescription></Alert> }
function SecureStorageRecovery({ requiresUnlock, pending, error, onRecover }: { requiresUnlock: boolean; pending: boolean; error: string | null; onRecover: (unlock?: string) => Promise<void> }) {
  const recover = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const form = event.currentTarget
    const fields = new FormData(form)
    const unlock = String(fields.get("unlock") ?? "")
    form.reset()
    void onRecover(unlock)
  }
  return <section className="rounded-xl border border-amber-400/30 bg-amber-400/5 p-5"><div className="flex gap-3"><KeyRound className="mt-0.5 size-5 text-amber-300" aria-hidden="true" /><div><h2 className="font-semibold">Finish secure storage setup</h2><p className="mt-1 text-sm leading-6 text-muted-foreground">{requiresUnlock ? "Enter your local security password to unlock saved connection credentials." : "Continue once to approve secure credential storage with your operating system."}</p></div></div><form className="mt-5 flex max-w-xl flex-wrap items-end gap-3" onSubmit={recover}>{requiresUnlock ? <div className="min-w-56 flex-1"><Label htmlFor="service-fallback-unlock">Local security password</Label><Input id="service-fallback-unlock" name="unlock" type="password" autoComplete="current-password" spellCheck={false} className="mt-2 font-mono" disabled={pending} /></div> : null}<Button type="submit" disabled={pending}>{pending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}{pending ? "Finishing setup…" : requiresUnlock ? "Unlock secure storage" : "Continue securely"}</Button></form>{error ? <InlineError>{error}</InlineError> : null}</section>
}
function Unavailable({ detail }: { detail: string }) { return <Alert className="mt-5"><CircleAlert aria-hidden="true" /><AlertTitle>Settings service is unavailable</AlertTitle><AlertDescription>{detail} Reconnect to the installed Market Squawk service and retry; no local fallback can edit these authorities.</AlertDescription></Alert> }
function SettingsFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) { return <div className="mx-auto w-full max-w-[1320px] p-5 lg:p-7"><header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between"><div><p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">Service-owned configuration and workspace authority</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">Settings</h1><p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">Review effective typed configuration, preview revision-fenced changes, and switch only through the service’s durable workspace transition workflow.</p></div>{action}</header><div className="mt-6">{children}</div></div> }
function SettingsSkeleton() { return <><div className="grid gap-3 sm:grid-cols-3"><Skeleton className="h-32" /><Skeleton className="h-32" /><Skeleton className="h-32" /></div><div className="mt-6 grid gap-3 xl:grid-cols-2"><Skeleton className="h-72" /><Skeleton className="h-72" /></div></> }

function entriesToDraft(entries: SettingEntry[]): DraftValues { return Object.fromEntries(entries.map((entry) => [entry.key, settingValueToText(entry)])) }
function collectChanges(entries: SettingEntry[], draft: DraftValues): { changes: OperationSettingValue[]; errors: FieldErrors } { const errors: FieldErrors = {}; const changes: OperationSettingValue[] = []; for (const entry of entries) { if (!entry.locallyMutable || !isMatchingSetting(entry)) continue; const next = draft[entry.key] ?? settingValueToText(entry); if (!validateInput(entry, next)) { errors[entry.key] = `Enter ${validationLabel(entry.key).toLowerCase()}.`; continue } const value = asOperationSettingValue(entry, next); if (!value) { errors[entry.key] = "This value is not a valid typed setting."; continue } if (next !== settingValueToText(entry)) changes.push(value) } return { changes, errors } }
function validateInput(entry: SettingEntry, value: string) { if (entry.key === "automatic_update_checks" || entry.key === "log_minimum_severity" || entry.key === "update_channel") return true; if (!/^\d+$/.test(value)) return false; if (entry.key === "storage_soft_limit_bytes") { try { const bytes = BigInt(value); return bytes >= 1024n ** 3n && bytes <= 16n * 1024n ** 4n } catch { return false } } const number = Number(value); if (!Number.isSafeInteger(number)) return false; switch (entry.key) { case "log_retention_days": return number >= 1 && number <= 365; case "default_query_row_limit": return number >= 100 && number <= 1_000_000; case "maximum_concurrent_jobs": return number >= 1 && number <= 64; case "market_freshness_millis": return number >= 250 && number <= 600_000; case "backup_retention_count": return number >= 1 && number <= 64; default: return false } }
function isRetainedRevisionCandidate(target: string, active: LosslessInteger) { if (!/^\d+$/.test(target) || target === "0") return false; try { return compareLosslessIntegers(target, active) < 0 } catch { return false } }
function firstError(...errors: (Error | null)[]) { return errors.find((error) => error !== null) ?? null }
function confirmationError(confirmation: Confirmation | null, settings: Error | null, rollback: Error | null, workspace: Error | null) { if (!confirmation) return null; if (confirmation.kind === "settings") return settings; if (confirmation.kind === "rollback") return rollback; return workspace }
function settingTitle(key: string) { const labels: Record<string, string> = { log_retention_days: "Log retention", log_minimum_severity: "Log minimum severity", update_channel: "Update channel", automatic_update_checks: "Automatic update checks", storage_soft_limit_bytes: "Storage soft limit", default_query_row_limit: "Default query row limit", maximum_concurrent_jobs: "Maximum concurrent jobs", market_freshness_millis: "Market freshness", backup_retention_count: "Backup retention" }; return labels[key] ?? humanize(key) }
function settingDescription(key: string) { const descriptions: Record<string, string> = { log_retention_days: "Bounded retention for redacted structured logs.", log_minimum_severity: "Lowest severity admitted by the service logging policy.", update_channel: "Trusted product release stream, never an arbitrary URL.", automatic_update_checks: "Allows disclosed daily metadata checks after first value.", storage_soft_limit_bytes: "Governance threshold that pauses heavy work before storage pressure causes damage.", default_query_row_limit: "Safe default for bounded product queries.", maximum_concurrent_jobs: "Shared durable-job concurrency across local clients.", market_freshness_millis: "Maximum age allowed before live market facts become stale.", backup_retention_count: "Bounded count of retained local backup generations." }; return descriptions[key] ?? "Typed service-owned configuration." }
function validationLabel(key: string) { const labels: Record<string, string> = { log_retention_days: "1–365 days", log_minimum_severity: "Trace, debug, info, warn, or error", update_channel: "Stable or preview", automatic_update_checks: "Enabled or disabled", storage_soft_limit_bytes: "1 GiB–16 TiB, exact bytes", default_query_row_limit: "100–1,000,000 rows", maximum_concurrent_jobs: "1–64 jobs", market_freshness_millis: "250–600,000 ms", backup_retention_count: "1–64 backups" }; return labels[key] ?? "Service validation" }
function originLabel(origin: string) { const labels: Record<string, string> = { safe_default: "Safe default", local_persisted: "Local persisted", local_configuration: "Local configuration", environment: "Environment", cli_override: "CLI override", managed_policy: "Managed policy" }; return labels[origin] ?? humanize(origin) }
function restartLabel(impact: string) { const labels: Record<string, string> = { none: "No restart", service_reload: "Service reload", service_restart: "Service restart" }; return labels[impact] ?? humanize(impact) }
