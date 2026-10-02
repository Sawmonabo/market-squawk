import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { z } from "zod"

import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { jobReceiptSchema, type BackupJobReceipt } from "@/features/backup/contracts"
import { canCancel, isActiveJob, jobViewSchema, type JobView } from "@/features/operations/contracts"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { marketHistoryTokenSchema } from "./market-product"

const historyJobKind = "market.prepare-history.v1"
const historyResultAuthority = "market.adjusted-history-publication.v1"
const startViewSchema = z.object({
  state: z.enum(["unknown", "pending", "admitted", "not_admitted"]),
  job: jobViewSchema.nullable(),
}).refine((view) => (view.state === "admitted") === (view.job !== null), "Invalid preparation admission state.")

type Preparation = {
  startRequestId: string
  lookbackDays: number
  receipt: BackupJobReceipt | null
  admission: "starting" | "uncertain" | "admitted" | "not_admitted"
  busy: boolean
  error: string | null
  applied: boolean
}

const storagePrefix = "market-squawk.history-preparation.v1:"
const recoverySchema = z.object({
  version: z.literal(1),
  scope: z.string().uuid(),
  historyToken: marketHistoryTokenSchema,
  startRequestId: z.string().uuid(),
  lookbackDays: z.number().int().min(30).max(3650),
  receipt: jobReceiptSchema.nullable(),
}).strict()

function storageKey(scope: string, historyToken: string) { return `${storagePrefix}${scope}:${historyToken}` }

function restorePreparation(scope: string, historyToken: string): { value: Preparation | null; error: string | null } {
  try {
    const raw = sessionStorage.getItem(storageKey(scope, historyToken))
    if (raw === null) return { value: null, error: null }
    if (raw.length > 8192) throw new Error("Invalid preparation recovery record.")
    const saved = recoverySchema.parse(JSON.parse(raw))
    if (saved.scope !== scope || saved.historyToken !== historyToken) throw new Error("Invalid preparation recovery scope.")
    return { value: { startRequestId: saved.startRequestId, lookbackDays: saved.lookbackDays,
      receipt: saved.receipt, admission: saved.receipt ? "admitted" : "uncertain", busy: false, error: null, applied: false }, error: null }
  } catch {
    return { value: null, error: "Saved preparation recovery could not be read. Reload to retry before loading more history." }
  }
}

function persistPreparation(scope: string, historyToken: string, preparation: Preparation) {
  const record = recoverySchema.parse({ version: 1, scope, historyToken,
    startRequestId: preparation.startRequestId, lookbackDays: preparation.lookbackDays,
    receipt: preparation.receipt })
  const serialized = JSON.stringify(record)
  sessionStorage.setItem(storageKey(scope, historyToken), serialized)
  if (sessionStorage.getItem(storageKey(scope, historyToken)) !== serialized)
    throw new Error("Preparation recovery was not retained.")
}

// The small receipt survives Hide/Show and same-session reconnect. It contains no
// saved price payload. A pending identity must not disappear while admission is unknown.
export function HistoryPreparation({ historyToken, bootstrap, transport, hasSavedHistory, onPrepared }: {
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  hasSavedHistory: boolean
  onPrepared: () => Promise<void>
}) {
  const available = hasProductCapability(bootstrap, "market_history_preparation_start")
    && hasProductCapability(bootstrap, "market_history_preparation_get")
    && hasProductCapability(bootstrap, "market_history_preparation_cancel")
  return available ? <PreparationControls historyToken={historyToken} bootstrap={bootstrap}
    transport={transport} hasSavedHistory={hasSavedHistory} onPrepared={onPrepared} /> : null
}

function PreparationControls({ historyToken, bootstrap, transport, hasSavedHistory, onPrepared }: {
  historyToken: string; bootstrap: DesktopBootstrap; transport: ProductTransport
  hasSavedHistory: boolean; onPrepared: () => Promise<void>
}) {
  const queryClient = useQueryClient()
  const scope = bootstrap.productSessionToken
  const [restored] = React.useState(() => restorePreparation(scope, historyToken))
  const [storageError, setStorageError] = React.useState(restored.error)
  const receiptKey = productKeys.operation(bootstrap.productSessionToken, "job", "Desktop.HistoryPreparationReceipt", { historyToken })
  const receipt = useQuery<Preparation | null>({
    queryKey: receiptKey,
    queryFn: async () => null,
    initialData: restored.value,
    enabled: false,
    gcTime: Infinity,
  })
  const preparation = receipt.data
  const [lookbackDays, setLookbackDays] = React.useState(preparation?.lookbackDays ?? 365)
  const jobKey = productKeys.operation(bootstrap.productSessionToken, "job", "Market.GetHistoryPreparation", {
    historyToken, jobId: preparation?.receipt?.jobId, generation: preparation?.receipt?.generation,
  })
  const status = useQuery({
    queryKey: jobKey,
    enabled: preparation?.receipt !== null && preparation?.receipt !== undefined,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const expected = preparation?.receipt
      if (!expected) throw new Error("No history preparation has been admitted.")
      const result = await transport.marketHistoryPreparation({ action: "get", historyToken,
        jobId: expected.jobId, generation: expected.generation }, false, { signal })
      const previous = queryClient.getQueryData<JobView>(jobKey)
      return checkedJob(result.data, expected, previous?.sequence)
    },
  })
  const job = status.data
  const mutate = useMutation({
    retry: false,
    mutationFn: async (action: "start" | "reconcileStart" | "cancelStart" | "cancel") => {
      let current = queryClient.getQueryData<Preparation | null>(receiptKey)
      if (current?.busy || restored.error) return
      if (action === "start") {
        if (current && current.admission !== "not_admitted"
          && (!current.receipt || !job || isActiveJob(job.state))) return
        current = { startRequestId: crypto.randomUUID(), lookbackDays, receipt: null,
          admission: "starting", busy: true, error: null, applied: false }
      } else {
        if (!current) return
        current = { ...current, busy: true, error: null }
      }
      const original = current
      // Retain the exact identity before any durable start can be admitted.
      if (action === "start") {
        try { persistPreparation(scope, historyToken, original); setStorageError(null) }
        catch { setStorageError("Preparation recovery could not be saved. History was not started; try again."); return }
      }
      queryClient.setQueryData(receiptKey, original)
      const save = (next: Preparation) => {
        if (queryClient.getQueryData<Preparation | null>(receiptKey)?.startRequestId !== original.startRequestId) return
        queryClient.setQueryData(receiptKey, next)
        try {
          if (next.admission === "not_admitted") sessionStorage.removeItem(storageKey(scope, historyToken))
          else persistPreparation(scope, historyToken, next)
          setStorageError(null)
        } catch { setStorageError("Preparation recovery could not be updated. Keep this page open until preparation is checked.") }
      }
      try {
        if (action === "start") {
          // This direct click authorizes the stated finite coverage. Never invoke
          // start from an effect, viewport request, retry, reconnect or job event.
          const result = await transport.marketHistoryPreparation({ action, historyToken,
            lookbackDays: original.lookbackDays, startRequestId: original.startRequestId }, true)
          const admitted = jobReceiptSchema.parse(result.data)
          save({ ...original, receipt: admitted, admission: "admitted", busy: false })
        } else if (action === "cancel") {
          if (!job || !original.receipt || !canCancel(job)) { save({ ...original, busy: false }); return }
          const result = await transport.marketHistoryPreparation({ action, historyToken, jobId: job.jobId,
            generation: job.generation, expectedSequence: job.sequence }, true)
          const updated = checkedJob(result.data, original.receipt, job.sequence)
          queryClient.setQueryData(jobKey, updated)
          save({ ...original, busy: false })
        } else {
          const result = await transport.marketHistoryPreparation({ action, historyToken,
            lookbackDays: original.lookbackDays, startRequestId: original.startRequestId }, action === "cancelStart")
          const view = startViewSchema.parse(result.data)
          const admitted = view.job === null ? null : checkedJob(view.job)
          save({ ...original, receipt: admitted, busy: false,
            admission: view.state === "admitted" ? "admitted" : view.state === "not_admitted" ? "not_admitted" : "uncertain" })
        }
      } catch {
        // A failed acknowledgment is not proof that no durable job started.
        save({ ...original, busy: false,
          admission: original.receipt ? original.admission : "uncertain",
          error: action === "cancel" ? "Cancellation could not be verified. Check preparation before trying again."
            : "Preparation could not be verified. Check the original request before loading again." })
      }
    },
  })
  const recoveryChecked = React.useRef(false)
  React.useEffect(() => {
    // Restore an interrupted WebView observer by reading its original request,
    // never by replaying Start. StrictMode/remounts cannot schedule two checks.
    if (recoveryChecked.current || !restored.value || !preparation || preparation.busy
      || preparation.admission !== "uncertain") return
    recoveryChecked.current = true
    mutate.mutate("reconcileStart")
  }, [preparation, restored.value, mutate])
  React.useEffect(() => {
    try {
      const obsolete: string[] = []
      for (let index = 0; index < sessionStorage.length; index++) {
        const key = sessionStorage.key(index)
        if (key?.startsWith(storagePrefix) && !key.startsWith(`${storagePrefix}${scope}:`)) obsolete.push(key)
      }
      for (const key of obsolete) sessionStorage.removeItem(key)
    } catch { /* Starting still requires a successful exact-record write. */ }
  }, [scope])
  React.useEffect(() => {
    if (!job || isActiveJob(job.state)) return
    try { sessionStorage.removeItem(storageKey(scope, historyToken)) }
    catch { setStorageError("The completed preparation recovery record could not be cleared.") }
  }, [job, scope, historyToken])
  React.useEffect(() => {
    if (!job || job.state !== "completed" || !preparation?.receipt || preparation.applied) return
    const current = queryClient.getQueryData<Preparation | null>(receiptKey)
    if (!current || current.applied || current.startRequestId !== preparation.startRequestId) return
    queryClient.setQueryData(receiptKey, { ...current, applied: true })
    // The reader retains its chart and viewport while it switches to the newly
    // published generation. Only this successful terminal receipt resets it.
    void onPrepared()
  }, [job, preparation, onPrepared, queryClient, receiptKey])

  const unresolved = preparation !== null && preparation !== undefined
    && (preparation.admission === "starting" || preparation.admission === "uncertain")
  const active = Boolean(preparation?.receipt && (!job || isActiveJob(job.state)))
  const busy = Boolean(preparation?.busy)
  return <div className="min-w-0 flex-1 text-xs">
    <div className="flex flex-wrap items-center gap-2">
      <label className="flex items-center gap-2">History to load
        <select className="rounded-md border border-input bg-background px-2 py-1.5"
          value={lookbackDays} disabled={busy || active || unresolved}
          onChange={(event) => setLookbackDays(Number(event.target.value))}>
          <option value={30}>30 days</option><option value={90}>90 days</option>
          <option value={365}>1 year</option><option value={3650}>10 years</option>
        </select>
      </label>
      <Button variant="outline" size="sm" disabled={busy || active || unresolved || restored.error !== null}
        onClick={() => mutate.mutate("start")}>{hasSavedHistory ? "Update history" : "Load history"}</Button>
      {unresolved ? <>
        <Button variant="outline" size="sm" disabled={busy} onClick={() => mutate.mutate("reconcileStart")}>Check preparation</Button>
        <Button variant="ghost" size="sm" disabled={busy} onClick={() => mutate.mutate("cancelStart")}>Cancel pending start</Button>
      </> : job && canCancel(job) ? <Button variant="ghost" size="sm" disabled={busy}
        onClick={() => mutate.mutate("cancel")}>Cancel preparation</Button> : null}
      {status.isError || preparation?.error && preparation.receipt ? <Button variant="outline" size="sm" disabled={busy || status.isFetching}
        onClick={() => void status.refetch().then((checked) => {
          const current = queryClient.getQueryData<Preparation | null>(receiptKey)
          if (checked.isSuccess && current && current.startRequestId === preparation?.startRequestId)
            queryClient.setQueryData(receiptKey, { ...current, error: null })
        })}>Check preparation</Button> : null}
    </div>
    <div className="mt-2 min-h-10 leading-5" aria-live="polite">
      <p className="text-muted-foreground">Loads up to {lookbackDays} calendar days ending at preparation time. Saved dates and gaps determine the chart coverage.</p>
      {storageError ? <p role="alert" className="text-destructive">{storageError}</p> : null}
      {preparation?.error ? <p role="alert" className="text-destructive">{preparation.error}</p>
        : status.isError ? <p role="alert" className="text-destructive">Preparation status could not be checked. Saved prices remain visible.</p>
          : unresolved ? <p role="status" className="text-muted-foreground">{busy ? "Checking history preparation…" : "The original preparation request has not been verified."}</p>
            : job ? <p role={job.state === "failed" || job.state === "interrupted" ? "alert" : "status"}
              className={job.state === "failed" || job.state === "interrupted" ? "text-destructive" : "text-muted-foreground"}>{jobMessage(job)}</p>
              : preparation?.admission === "not_admitted" ? <p role="status" className="text-muted-foreground">The original request did not start. History can be loaded again.</p> : null}
    </div>
  </div>
}

function checkedJob(data: unknown, receipt?: BackupJobReceipt, previousSequence?: string): JobView {
  const job = jobViewSchema.parse(data)
  if (job.kind !== historyJobKind || (receipt && (job.jobId !== receipt.jobId || job.generation !== receipt.generation
    || BigInt(job.sequence) < BigInt(receipt.sequence)))
    || (previousSequence !== undefined && BigInt(job.sequence) < BigInt(previousSequence))
    || (job.state === "completed" && job.result?.authority !== historyResultAuthority))
    throw new Error("History preparation evidence does not match this request.")
  return job
}

function jobMessage(job: JobView): string {
  switch (job.state) {
    case "completed": return "History preparation completed."
    case "failed": return "History could not be prepared. Try loading history again."
    case "cancelled": return "History preparation was cancelled."
    case "interrupted": return "History preparation was interrupted."
    case "cancelling": return "Cancelling history preparation…"
    case "recovering": return "Checking an interrupted history preparation…"
    case "awaiting_confirmation": return "History preparation requires additional authorization."
    default: return job.totalUnits !== null && job.completedUnits !== null
      ? `Preparing history… ${job.completedUnits} of ${job.totalUnits} steps complete.` : "Preparing history…"
  }
}
