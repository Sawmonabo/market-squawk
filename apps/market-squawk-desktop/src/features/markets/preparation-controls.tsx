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

import { marketHistoryTokenSchema, marketSelectionTokenSchema } from "./market-product"

type PreparationKind = "history" | "financial"
const startViewSchema = z.object({
  state: z.enum(["unknown", "pending", "admitted", "not_admitted"]),
  job: jobViewSchema.nullable(),
}).refine((view) => (view.state === "admitted") === (view.job !== null), "Invalid preparation admission state.")

type Preparation = {
  startRequestId: string
  lookbackDays?: number
  receipt: BackupJobReceipt | null
  admission: "starting" | "uncertain" | "admitted" | "not_admitted"
  busy: boolean
  error: string | null
  applied: boolean
  settled: boolean
}

const historyRecoverySchema = z.object({
  version: z.literal(1),
  scope: z.string().uuid(),
  historyToken: marketHistoryTokenSchema,
  startRequestId: z.string().uuid(),
  lookbackDays: z.number().int().min(30).max(3650),
  receipt: jobReceiptSchema.nullable(),
}).strict()

const financialRecoverySchema = historyRecoverySchema.omit({ historyToken: true, lookbackDays: true })
  .extend({ selectionToken: marketSelectionTokenSchema })

function storagePrefix(kind: PreparationKind) { return `market-squawk.${kind === "history" ? "history" : "financial"}-preparation.v1:` }
function storageKey(kind: PreparationKind, scope: string, token: string) { return `${storagePrefix(kind)}${scope}:${token}` }

function restorePreparation(kind: PreparationKind, scope: string, token: string): { value: Preparation | null; error: string | null } {
  try {
    const raw = sessionStorage.getItem(storageKey(kind, scope, token))
    if (raw === null) return { value: null, error: null }
    if (raw.length > 8192) throw new Error("Invalid preparation recovery record.")
    const saved = kind === "history" ? historyRecoverySchema.parse(JSON.parse(raw)) : financialRecoverySchema.parse(JSON.parse(raw))
    const savedToken = "historyToken" in saved ? saved.historyToken : saved.selectionToken
    if (saved.scope !== scope || savedToken !== token) throw new Error("Invalid preparation recovery scope.")
    return { value: { startRequestId: saved.startRequestId, lookbackDays: "lookbackDays" in saved ? saved.lookbackDays : undefined,
      receipt: saved.receipt, admission: saved.receipt ? "admitted" : "uncertain", busy: false, error: null, applied: false, settled: false }, error: null }
  } catch {
    return { value: null, error: kind === "history"
      ? "The previous loading request could not be recovered. Reload before trying again."
      : "The previous loading request could not be recovered. Reload before trying again." }
  }
}

function persistPreparation(kind: PreparationKind, scope: string, token: string, preparation: Preparation) {
  const common = { version: 1, scope, startRequestId: preparation.startRequestId, receipt: preparation.receipt }
  const record = kind === "history"
    ? historyRecoverySchema.parse({ ...common, historyToken: token, lookbackDays: preparation.lookbackDays })
    : financialRecoverySchema.parse({ ...common, selectionToken: token })
  const serialized = JSON.stringify(record)
  sessionStorage.setItem(storageKey(kind, scope, token), serialized)
  if (sessionStorage.getItem(storageKey(kind, scope, token)) !== serialized)
    throw new Error("Preparation recovery was not retained.")
}

export type PreparationControllerProps = {
  kind: PreparationKind
  token: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  onPrepared: () => Promise<void>
  onSettled?: () => Promise<void>
}

// Shared only by selected history and selected financial information. The native
// calls remain the two closed product contracts; this is not an operation runner.
function preparationAvailable(props: PreparationControllerProps) {
  return props.kind === "history"
    ? hasProductCapability(props.bootstrap, "market_history_preparation_start")
      && hasProductCapability(props.bootstrap, "market_history_preparation_get")
      && hasProductCapability(props.bootstrap, "market_history_preparation_cancel")
    : hasProductCapability(props.bootstrap, "investment_financial_preparation_start")
      && hasProductCapability(props.bootstrap, "investment_financial_preparation_get")
      && hasProductCapability(props.bootstrap, "investment_financial_preparation_cancel")
}

export function usePreparationController(props: PreparationControllerProps) {
  const { kind, token, bootstrap, transport, onPrepared, onSettled } = props
  const available = preparationAvailable(props)
  const title = kind === "history" ? "History" : "Financial information"
  const target = kind === "history" ? { historyToken: token } : { selectionToken: token }
  const queryClient = useQueryClient()
  const scope = bootstrap.productSessionToken
  const [restored] = React.useState(() => available ? restorePreparation(kind, scope, token) : { value: null, error: null })
  const [storageError, setStorageError] = React.useState(restored.error)
  const receiptKey = productKeys.operation(bootstrap.productSessionToken, "job", kind === "history" ? "Desktop.HistoryPreparationReceipt" : "Desktop.FinancialPreparationReceipt", target)
  const receipt = useQuery<Preparation | null>({
    queryKey: receiptKey,
    queryFn: async () => null,
    initialData: restored.value,
    enabled: false,
    gcTime: Infinity,
  })
  const preparation = receipt.data
  const jobKey = productKeys.operation(bootstrap.productSessionToken, "job", kind === "history" ? "Market.GetHistoryPreparation" : "Research.GetInvestmentFinancialPreparation", {
    ...target, jobId: preparation?.receipt?.jobId, generation: preparation?.receipt?.generation,
  })
  const status = useQuery({
    queryKey: jobKey,
    enabled: available && preparation?.receipt !== null && preparation?.receipt !== undefined,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async ({ signal }) => {
      const expected = preparation?.receipt
      if (!expected) throw new Error("No preparation has been admitted.")
      const request = { action: "get" as const, jobId: expected.jobId, generation: expected.generation }
      const result = kind === "history"
        ? await transport.marketHistoryPreparation({ ...request, historyToken: token }, false, { signal })
        : await transport.investmentFinancialPreparation({ ...request, selectionToken: token }, false, { signal })
      const previous = queryClient.getQueryData<JobView>(jobKey)
      return checkedJob(result.data, kind, expected, previous?.sequence)
    },
  })
  const job = status.data
  const mutate = useMutation({
    retry: false,
    mutationFn: async (command: { action: "start"; original: Preparation } | { action: "reconcileStart" | "cancelStart" | "cancel" }) => {
      if (!available || restored.error) return
      const { action } = command
      const current = queryClient.getQueryData<Preparation | null>(receiptKey)
      if (!current || (action !== "start" && current.busy)) return
      const original = command.action === "start" ? command.original : { ...current, busy: true, error: null }
      if (current.startRequestId !== original.startRequestId) return
      queryClient.setQueryData(receiptKey, original)
      const save = (next: Preparation) => {
        if (queryClient.getQueryData<Preparation | null>(receiptKey)?.startRequestId !== original.startRequestId) return
        queryClient.setQueryData(receiptKey, next)
        try {
          if (next.admission === "not_admitted") sessionStorage.removeItem(storageKey(kind, scope, token))
          else persistPreparation(kind, scope, token, next)
          setStorageError(null)
        } catch { setStorageError(kind === "history" ? "Loading recovery could not be saved. Keep this page open until loading is checked."
          : "Loading recovery could not be saved. Keep this page open until loading is checked.") }
      }
      try {
        if (action === "start") {
          // A selected history range or missing financial reports authorize this exact request.
          const result = kind === "history"
            ? await transport.marketHistoryPreparation({ action, historyToken: token,
              lookbackDays: original.lookbackDays!, startRequestId: original.startRequestId }, true)
            : await transport.investmentFinancialPreparation({ action, selectionToken: token,
              startRequestId: original.startRequestId }, true)
          const admitted = jobReceiptSchema.parse(result.data)
          save({ ...original, receipt: admitted, admission: "admitted", busy: false })
        } else if (action === "cancel") {
          if (!job || !original.receipt || !canCancel(job)) { save({ ...original, busy: false }); return }
          const request = { action, jobId: job.jobId, generation: job.generation, expectedSequence: job.sequence }
          const result = kind === "history"
            ? await transport.marketHistoryPreparation({ ...request, historyToken: token }, true)
            : await transport.investmentFinancialPreparation({ ...request, selectionToken: token }, true)
          const updated = checkedJob(result.data, kind, original.receipt, job.sequence)
          queryClient.setQueryData(jobKey, updated)
          save({ ...original, busy: false })
        } else {
          const result = kind === "history"
            ? await transport.marketHistoryPreparation({ action, historyToken: token,
              lookbackDays: original.lookbackDays!, startRequestId: original.startRequestId }, action === "cancelStart")
            : await transport.investmentFinancialPreparation({ action, selectionToken: token,
              startRequestId: original.startRequestId }, action === "cancelStart")
          const view = startViewSchema.parse(result.data)
          const admitted = view.job === null ? null : checkedJob(view.job, kind)
          save({ ...original, receipt: admitted, busy: false,
            admission: view.state === "admitted" ? "admitted" : view.state === "not_admitted" ? "not_admitted" : "uncertain" })
        }
      } catch {
        // A failed acknowledgment is not proof that no durable job started.
        save({ ...original, busy: false,
          admission: original.receipt ? original.admission : "uncertain",
          error: kind === "history"
            ? action === "cancel" ? "Cancellation could not be checked. Check loading before trying again."
              : "History loading could not be checked. Check the original request before trying again."
            : action === "cancel" ? "Cancellation could not be checked. Check loading before trying again."
              : "Loading could not be checked. Check the original request before trying again." })
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
    mutate.mutate({ action: "reconcileStart" })
  }, [preparation, restored.value, mutate])
  React.useEffect(() => {
    try {
      const obsolete: string[] = []
      for (let index = 0; index < sessionStorage.length; index++) {
        const key = sessionStorage.key(index)
        if (key?.startsWith(storagePrefix(kind)) && !key.startsWith(`${storagePrefix(kind)}${scope}:`)) obsolete.push(key)
      }
      for (const key of obsolete) sessionStorage.removeItem(key)
    } catch { /* Starting still requires a successful exact-record write. */ }
  }, [kind, scope])
  React.useEffect(() => {
    if (!job || isActiveJob(job.state)) return
    try {
      // Keep the terminal request and sequence across remounts. Automatic loading
      // must not repeat a cancelled/failed request or accept older status.
      const current = queryClient.getQueryData<Preparation | null>(receiptKey)
      if (!current?.receipt || current.receipt.sequence === job.sequence) return
      const next = { ...current, receipt: { ...current.receipt, sequence: job.sequence } }
      persistPreparation(kind, scope, token, next)
      queryClient.setQueryData(receiptKey, next)
    } catch { setStorageError("The completed loading request could not be saved for recovery.") }
  }, [job, kind, scope, token, queryClient, receiptKey])
  React.useEffect(() => {
    if (!job || isActiveJob(job.state) || (kind === "history" && job.state !== "completed")
      || !preparation?.receipt || preparation.settled || preparation.applied) return
    const current = queryClient.getQueryData<Preparation | null>(receiptKey)
    if (!current || current.settled || current.applied || current.startRequestId !== preparation.startRequestId) return
    queryClient.setQueryData(receiptKey, { ...current, settled: true, applied: job.state === "completed" })
    // A financial job can retain genuine intermediate families before failing.
    // Observe settlement once without turning that outcome into applied success.
    if (job.state === "completed") void onPrepared()
    else void onSettled?.()
  }, [job, kind, preparation, onPrepared, onSettled, queryClient, receiptKey])

  const unresolved = preparation !== null && preparation !== undefined
    && (preparation.admission === "starting" || preparation.admission === "uncertain")
  const active = Boolean(preparation?.receipt && (!job || isActiveJob(job.state)))
  const busy = Boolean(preparation?.busy)
  const canStart = available && !restored.error && !busy && !active && !unresolved
  const start = (lookbackDays?: number) => {
    if (!available || restored.error) return false
    const current = queryClient.getQueryData<Preparation | null>(receiptKey)
    const checked = queryClient.getQueryData<JobView>(jobKey)
    if (current?.busy || current && current.admission !== "not_admitted"
      && (!current.receipt || !checked || isActiveJob(checked.state))) return false
    const original: Preparation = { startRequestId: crypto.randomUUID(), lookbackDays: kind === "history" ? lookbackDays : undefined,
      receipt: null, admission: "starting", busy: true, error: null, applied: false, settled: false }
    // Reserve and retain the original intent synchronously, before scheduling the call.
    try { persistPreparation(kind, scope, token, original); setStorageError(null) }
    catch { setStorageError(kind === "history" ? "History loading could not be saved for recovery. Try again."
      : `${title} loading could not be saved for recovery. Try again.`); return false }
    queryClient.setQueryData(receiptKey, original)
    mutate.mutate({ action: "start", original })
    return true
  }
  const checkStatus = () => void status.refetch().then((checked) => {
    const current = queryClient.getQueryData<Preparation | null>(receiptKey)
    if (checked.isSuccess && current && current.startRequestId === preparation?.startRequestId)
      queryClient.setQueryData(receiptKey, { ...current, error: null })
  })
  return { available, preparation, job, busy, active, unresolved, canStart, storageError, status, start, checkStatus,
    reconcileStart: () => mutate.mutate({ action: "reconcileStart" }),
    cancelStart: () => mutate.mutate({ action: "cancelStart" }),
    cancel: () => mutate.mutate({ action: "cancel" }),
  }
}

export type PreparationController = ReturnType<typeof usePreparationController>

export function PreparationStatus({ kind, controller, showMessage = true }: {
  kind: PreparationKind; controller: PreparationController; showMessage?: boolean
}) {
  const { preparation, job, busy, unresolved, storageError, status } = controller
  const noun = kind === "history" ? "history" : "financial information"
  const title = kind === "history" ? "History" : "Financial information"
  if (!controller.available) return null
  return <div className="min-w-0 flex-1 text-xs" role="group" aria-label={`${title} loading`}>
    <div className="flex flex-wrap items-center gap-2">
      {kind === "financial" && controller.canStart && (storageError || preparation?.admission === "not_admitted"
        || job && ["failed", "cancelled", "interrupted"].includes(job.state)) ? <Button variant="outline" size="sm"
          onClick={() => controller.start()}>Retry</Button> : null}
      {unresolved ? <>
        <Button variant="outline" size="sm" disabled={busy} onClick={controller.reconcileStart}>Check loading</Button>
        <Button variant="ghost" size="sm" disabled={busy} onClick={controller.cancelStart}>Cancel loading</Button>
      </> : job && canCancel(job) ? <Button variant="ghost" size="sm" disabled={busy}
        onClick={controller.cancel}>Cancel loading</Button> : null}
      {status.isError || preparation?.error && preparation.receipt ? <Button variant="outline" size="sm" disabled={busy || status.isFetching}
        onClick={controller.checkStatus}>Check loading</Button> : null}
    </div>
    {showMessage ? <div className="mt-2 leading-5" aria-live="polite">

      {storageError ? <p role="alert" className="text-destructive">{storageError}</p> : null}
      {preparation?.error ? <p role="alert" className="text-destructive">{preparation.error}</p>
        : status.isError ? <p role="alert" className="text-destructive">{kind === "history" ? "Loading status could not be checked. Showing the last checked prices." : "Loading status could not be checked. Showing the last checked information."}</p>
          : unresolved ? <p role="status" className="text-muted-foreground">{kind === "history"
            ? busy ? "Checking history loading…" : "The original loading request has not been checked."
            : busy ? `Checking ${noun} loading…` : "The original loading request has not been checked."}</p>
            : job ? <p role={job.state === "failed" || job.state === "interrupted" ? "alert" : "status"}
              className={job.state === "failed" || job.state === "interrupted" ? "text-destructive" : "text-muted-foreground"}>{jobMessage(job, kind)}</p>
              : preparation?.admission === "not_admitted" ? <p role="status" className="text-muted-foreground">{kind === "history" ? "The original request did not start. Select a history range to try again." : "The original request did not start. Information can be loaded again."}</p> : null}
    </div> : null}
  </div>
}

function checkedJob(data: unknown, kind: PreparationKind, receipt?: BackupJobReceipt, previousSequence?: string): JobView {
  const job = jobViewSchema.parse(data)
  const expectedKind = kind === "history" ? "market.prepare-history.v1" : "research.prepare-investment-financials.v1"
  const authority = kind === "history" ? "market.adjusted-history-publication.v1" : "research.financial-preparation-result.v1"
  if (job.kind !== expectedKind || (receipt && (job.jobId !== receipt.jobId || job.generation !== receipt.generation
    || BigInt(job.sequence) < BigInt(receipt.sequence)))
    || (previousSequence !== undefined && BigInt(job.sequence) < BigInt(previousSequence))
    || (job.state === "completed" && job.result?.authority !== authority))
    throw new Error("Preparation evidence does not match this request.")
  return job
}

function jobMessage(job: JobView, kind: PreparationKind): string {
  const noun = kind === "history" ? "history" : "financial information"
  const title = kind === "history" ? "History" : "Financial information"
  if (kind === "history") {
    switch (job.state) {
      case "completed": return "History is ready."
      case "failed": return "History could not be loaded. Select a history range to try again."
      case "cancelled": return "History loading was cancelled."
      case "interrupted": return "History loading was interrupted."
      case "cancelling": return "Cancelling history loading…"
      case "recovering": return "Resuming history loading…"
      case "awaiting_confirmation": return "History loading needs your authorization."
      default: return "Loading history…"
    }
  }
  switch (job.state) {
    case "completed": return `${title} is ready.`
    case "failed": return `${title} could not be loaded.`
    case "cancelled": return `${title} loading was cancelled.`
    case "interrupted": return `${title} loading was interrupted.`
    case "cancelling": return `Cancelling ${noun} loading…`
    case "recovering": return `Resuming ${noun} loading…`
    case "awaiting_confirmation": return `${title} loading needs your authorization.`
    default: return `Loading ${noun}…`
  }
}
