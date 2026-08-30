import * as React from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import {
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  ClipboardCheck,
  Download,
  Eye,
  FileCheck2,
  Filter,
  LoaderCircle,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"

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
import { Skeleton } from "@/components/ui/skeleton"
import { groupDecimal, humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"
import type { OperationLogFilter, SystemTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"

import {
  asUnsignedCursor,
  defaultLogFilterDraft,
  filterFromDraft,
  logDomainOptions,
  logSeverityOptions,
  MAXIMUM_LOG_PAGES,
  parseDiagnosticArtifactReceipt,
  parseStructuredLogPage,
  type DiagnosticArtifactReceipt,
  type LogFilterDraft,
  type StructuredLogRecord,
} from "./contracts"

export function LogsPage() {
  const system = useSystem()

  if (system.status === "loading") return <LogsLoading />
  if (system.status !== "ready") {
    return <LogsUnavailable detail={system.status === "unavailable" ? system.error : "Finish secure storage setup in Settings, then return to Logs."} />
  }

  if (!system.bootstrap.capabilities.includes("operations_log_query")) {
    return (
      <LogsUnavailable detail="Log browsing is not available in this installation." />
    )
  }

  return (
    <LogsWorkspace
      transport={system.transport}
      scope={system.bootstrap.productSessionToken}
      exportAvailable={system.bootstrap.capabilities.includes("operations_log_export")}
    />
  )
}

function LogsWorkspace({
  transport,
  scope,
  exportAvailable,
}: {
  transport: SystemTransport
  scope: ProductScope
  exportAvailable: boolean
}) {
  const [draft, setDraft] = React.useState<LogFilterDraft>(defaultLogFilterDraft)
  const [filter, setFilter] = React.useState<OperationLogFilter>(() => ({ limit: 100 }))
  const [filterError, setFilterError] = React.useState<string | null>(null)
  const [afterSequence, setAfterSequence] = React.useState<string | undefined>()
  const [priorCursors, setPriorCursors] = React.useState<(string | undefined)[]>([])
  const [selected, setSelected] = React.useState<StructuredLogRecord | null>(null)
  const [confirmExport, setConfirmExport] = React.useState(false)
  const [receipt, setReceipt] = React.useState<DiagnosticArtifactReceipt | null>(null)
  const [announcement, setAnnouncement] = React.useState("")
  const request = React.useMemo(
    () => ({ query: "operationLogs" as const, ...filter, afterSequence }),
    [afterSequence, filter],
  )
  const queryKey = productKeys.operation(scope, "operations", "Operations.QueryLogs", request)
  const logs = useQuery({
    queryKey,
    queryFn: () => transport.systemQuery(request).then(parseStructuredLogPage),
  })
  const exportMutation = useMutation({
    mutationFn: () =>
      transport
        .operationsControl({ action: "exportLogs", ...filter, afterSequence }, true)
        .then(parseDiagnosticArtifactReceipt),
    onSuccess: (next) => {
      setReceipt(next)
      setConfirmExport(false)
      setAnnouncement("The controlled redacted diagnostic artifact is ready.")
    },
  })

  const nextCursor = asUnsignedCursor(logs.data?.nextAfterSequence ?? null)
  const invalidNextCursor =
    logs.data?.nextAfterSequence !== null && logs.data !== undefined && nextCursor === null
  const reachedPageBound = priorCursors.length + 1 >= MAXIMUM_LOG_PAGES

  const applyFilters = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const next = filterFromDraft(draft)
    setFilterError(next.error)
    if (next.error) return
    setFilter(next.filter)
    setAfterSequence(undefined)
    setPriorCursors([])
    setSelected(null)
    setReceipt(null)
    exportMutation.reset()
    setAnnouncement("Log filters applied. The bounded query was refreshed.")
  }

  const resetFilters = () => {
    setDraft(defaultLogFilterDraft)
    setFilter({ limit: 100 })
    setFilterError(null)
    setAfterSequence(undefined)
    setPriorCursors([])
    setSelected(null)
    setReceipt(null)
    exportMutation.reset()
    setAnnouncement("Log filters reset to the bounded default.")
  }

  const loadNext = () => {
    if (nextCursor === null || reachedPageBound) return
    setPriorCursors((current) => [...current, afterSequence])
    setAfterSequence(nextCursor)
    setSelected(null)
  }
  const loadPrevious = () => {
    setPriorCursors((current) => {
      const previous = current.at(-1)
      setAfterSequence(previous)
      return current.slice(0, -1)
    })
    setSelected(null)
  }

  return (
    <LogsFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => void logs.refetch()}
          disabled={logs.isFetching}
        >
          <RefreshCw className={cn(logs.isFetching && "animate-spin")} aria-hidden="true" />
          Refresh query
        </Button>
      }
    >
      <p className="sr-only" aria-live="polite">{announcement}</p>

      <section className="rounded-xl border border-border bg-card/35 p-5" aria-labelledby="log-scope-heading">
        <div className="flex gap-3">
          <ShieldCheck className="mt-0.5 size-5 text-primary" aria-hidden="true" />
          <div>
            <h2 id="log-scope-heading" className="text-sm font-semibold">
              Redacted, retained diagnostic evidence
            </h2>
            <p className="mt-1 max-w-4xl text-sm leading-6 text-muted-foreground">
              This is a bounded query over the installed service&apos;s structured, retention-limited logs—not a live raw tail. Record fields are already redacted before they reach this page; secrets, paths, and raw payloads are not exposed here.
            </p>
          </div>
        </div>
      </section>

      <form className="mt-5 rounded-xl border border-border bg-card/35 p-5" onSubmit={applyFilters}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">Bounded query</p>
            <h2 className="mt-1 text-lg font-semibold">Find diagnostic records</h2>
            <p className="mt-1 text-sm text-muted-foreground">All filters are combined. Local times are sent to the service as exact signed Unix-nanosecond decimals.</p>
          </div>
          <div className="flex gap-2">
            <Button type="button" size="sm" variant="outline" onClick={resetFilters} disabled={logs.isFetching}>
              Reset
            </Button>
            <Button type="submit" size="sm" disabled={logs.isFetching}>
              <Filter aria-hidden="true" /> Apply filters
            </Button>
          </div>
        </div>

        <div className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <FilterField label="From local time" htmlFor="logs-from">
            <Input id="logs-from" type="datetime-local" step="1" value={draft.fromLocal} onChange={(event) => setDraft((current) => ({ ...current, fromLocal: event.target.value }))} />
          </FilterField>
          <FilterField label="Through local time" htmlFor="logs-through">
            <Input id="logs-through" type="datetime-local" step="1" value={draft.throughLocal} onChange={(event) => setDraft((current) => ({ ...current, throughLocal: event.target.value }))} />
          </FilterField>
          <FilterField label="Minimum severity" htmlFor="logs-severity">
            <select id="logs-severity" className={selectClassName} value={draft.minimumSeverity} onChange={(event) => setDraft((current) => ({ ...current, minimumSeverity: event.target.value as LogFilterDraft["minimumSeverity"] }))}>
              <option value="">Any severity</option>
              {logSeverityOptions.map((severity) => <option key={severity} value={severity}>{humanize(severity)}</option>)}
            </select>
          </FilterField>
          <FilterField label="Domain" htmlFor="logs-domain">
            <select id="logs-domain" className={selectClassName} value={draft.domain} onChange={(event) => setDraft((current) => ({ ...current, domain: event.target.value as LogFilterDraft["domain"] }))}>
              <option value="">Any domain</option>
              {logDomainOptions.map((domain) => <option key={domain} value={domain}>{humanize(domain)}</option>)}
            </select>
          </FilterField>
          <FilterField label="Source ID" htmlFor="logs-source">
            <Input id="logs-source" value={draft.sourceId} maxLength={256} onChange={(event) => setDraft((current) => ({ ...current, sourceId: event.target.value }))} placeholder="Exact source identifier" />
          </FilterField>
          <FilterField label="Job ID" htmlFor="logs-job">
            <Input id="logs-job" value={draft.jobId} maxLength={256} onChange={(event) => setDraft((current) => ({ ...current, jobId: event.target.value }))} placeholder="Exact durable job identifier" />
          </FilterField>
          <FilterField label="Correlation ID" htmlFor="logs-correlation">
            <Input id="logs-correlation" value={draft.correlationId} maxLength={256} onChange={(event) => setDraft((current) => ({ ...current, correlationId: event.target.value }))} placeholder="Request or operation correlation" />
          </FilterField>
          <FilterField label="Text search" htmlFor="logs-search">
            <Input id="logs-search" value={draft.search} maxLength={256} onChange={(event) => setDraft((current) => ({ ...current, search: event.target.value }))} placeholder="Search redacted messages and fields" />
          </FilterField>
          <FilterField label="Records per page" htmlFor="logs-limit">
            <Input id="logs-limit" type="number" min="1" max="1000" step="1" inputMode="numeric" value={draft.limit} onChange={(event) => setDraft((current) => ({ ...current, limit: event.target.value }))} />
          </FilterField>
        </div>

        {filterError ? (
          <Alert variant="destructive" className="mt-4">
            <AlertTriangle aria-hidden="true" />
            <AlertTitle>Filters need attention</AlertTitle>
            <AlertDescription>{filterError}</AlertDescription>
          </Alert>
        ) : null}
      </form>

      <section className="mt-6" aria-labelledby="log-results-heading">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">Query results</p>
            <h2 id="log-results-heading" className="mt-1 text-xl font-semibold">Structured records</h2>
            <p className="mt-1 text-sm text-muted-foreground">At most {filter.limit.toLocaleString()} records are requested per page; open a record to inspect its safe, typed details.</p>
          </div>
          {exportAvailable ? (
            <Button variant="outline" size="sm" onClick={() => { exportMutation.reset(); setConfirmExport(true) }} disabled={logs.isPending || logs.isError || exportMutation.isPending}>
              <Download aria-hidden="true" /> Export this bounded query
            </Button>
          ) : (
            <span className="text-xs text-muted-foreground">Controlled export is unavailable from this service.</span>
          )}
        </div>

        {logs.isPending ? <LogResultsLoading /> : null}
        {logs.isError ? <QueryError detail={messageFrom(logs.error)} onRetry={() => void logs.refetch()} /> : null}
        {logs.data && logs.data.records.length === 0 ? <EmptyResults /> : null}
        {logs.data && logs.data.records.length > 0 ? <LogRecords records={logs.data.records} onSelect={setSelected} /> : null}

        {logs.data ? (
          <Pagination
            canGoPrevious={priorCursors.length > 0}
            canGoNext={nextCursor !== null && !reachedPageBound && !invalidNextCursor}
            isFetching={logs.isFetching}
            onPrevious={loadPrevious}
            onNext={loadNext}
            page={priorCursors.length + 1}
            reachedPageBound={reachedPageBound && logs.data.nextAfterSequence !== null}
            invalidNextCursor={invalidNextCursor}
          />
        ) : null}
      </section>

      {receipt ? <ArtifactReceipt receipt={receipt} /> : null}

      <Dialog open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <DialogContent className="max-h-[min(760px,calc(100vh-2rem))] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Redacted log record</DialogTitle>
            <DialogDescription>Structured diagnostic evidence for sequence {selected?.sequence ?? ""}.</DialogDescription>
          </DialogHeader>
          {selected ? <LogRecordDetails record={selected} /> : null}
          <DialogFooter><Button variant="outline" onClick={() => setSelected(null)}>Close</Button></DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirmExport} onOpenChange={(open) => { if (!open && !exportMutation.isPending) setConfirmExport(false) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Export this bounded redacted query?</DialogTitle>
            <DialogDescription>The service will publish a controlled diagnostic artifact for the active filters and current pagination cursor. It will not receive a filesystem path or raw log authority.</DialogDescription>
          </DialogHeader>
          <dl className="grid gap-2 rounded-lg border border-border bg-muted/20 p-3 text-sm">
            <ReceiptFact label="Requested records" value={filter.limit.toLocaleString()} />
            <ReceiptFact label="Current page" value={String(priorCursors.length + 1)} />
            <ReceiptFact label="Scope" value={filterSummary(filter)} />
          </dl>
          {exportMutation.isError ? <Alert variant="destructive"><AlertTriangle aria-hidden="true" /><AlertTitle>Export was not published</AlertTitle><AlertDescription>{messageFrom(exportMutation.error)}</AlertDescription></Alert> : null}
          <DialogFooter>
            <Button variant="outline" disabled={exportMutation.isPending} onClick={() => setConfirmExport(false)}>Cancel</Button>
            <Button disabled={exportMutation.isPending} onClick={() => exportMutation.mutate()}>{exportMutation.isPending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <ClipboardCheck aria-hidden="true" />}{exportMutation.isPending ? "Publishing…" : "Confirm controlled export"}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </LogsFrame>
  )
}

const selectClassName = "flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 dark:bg-input/30"

function LogsFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return <main className="mx-auto w-full max-w-[1320px] p-5 lg:p-7"><header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between"><div><p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">Local operations · bounded diagnostic evidence</p><h1 className="mt-2 text-3xl font-semibold tracking-tight">Logs</h1><p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">Search retained, structured service evidence without opening a raw tail or exposing secrets.</p></div>{action}</header><div className="mt-6">{children}</div></main>
}

function LogsLoading() { return <LogsFrame><div className="grid gap-4"><Skeleton className="h-28 rounded-xl" /><Skeleton className="h-72 rounded-xl" /><Skeleton className="h-80 rounded-xl" /></div></LogsFrame> }
function LogsUnavailable({ detail }: { detail: string }) { return <LogsFrame><Alert><AlertTriangle aria-hidden="true" /><AlertTitle>Logs are unavailable</AlertTitle><AlertDescription>{detail} Restore the local service connection, then retry this page.</AlertDescription></Alert></LogsFrame> }
function FilterField({ label, htmlFor, children }: { label: string; htmlFor: string; children: React.ReactNode }) { return <label className="grid gap-1.5 text-sm font-medium" htmlFor={htmlFor}><span>{label}</span>{children}</label> }
function LogResultsLoading() { return <div className="mt-4 grid gap-3" role="status"><Skeleton className="h-20 rounded-xl" /><Skeleton className="h-20 rounded-xl" /><Skeleton className="h-20 rounded-xl" /><span className="sr-only">Loading bounded log records</span></div> }
function QueryError({ detail, onRetry }: { detail: string; onRetry: () => void }) { return <Alert variant="destructive" className="mt-4"><AlertTriangle aria-hidden="true" /><AlertTitle>Log query could not be completed</AlertTitle><AlertDescription>{detail}<div className="mt-3"><Button variant="outline" size="sm" onClick={onRetry}>Retry query</Button></div></AlertDescription></Alert> }
function EmptyResults() { return <div className="mt-4 rounded-xl border border-dashed border-border p-6 text-sm text-muted-foreground"><Eye className="size-5" aria-hidden="true" /><h3 className="mt-3 font-medium text-foreground">No retained records match this query</h3><p className="mt-1 leading-6">Try a wider time range or fewer filters. This page only searches records still retained by the local service.</p></div> }

function LogRecords({ records, onSelect }: { records: StructuredLogRecord[]; onSelect: (record: StructuredLogRecord) => void }) {
  return <div className="mt-4 overflow-x-auto rounded-xl border border-border"><table className="w-full min-w-[900px] text-left text-sm" aria-label="Bounded structured log records"><thead className="border-b border-border bg-muted/35 text-[11px] uppercase tracking-wider text-muted-foreground"><tr><th className="px-4 py-3 font-medium">Time</th><th className="px-4 py-3 font-medium">Severity</th><th className="px-4 py-3 font-medium">Domain</th><th className="px-4 py-3 font-medium">Message</th><th className="px-4 py-3 font-medium">Evidence</th><th className="px-4 py-3 font-medium"><span className="sr-only">Details</span></th></tr></thead><tbody>{records.map((record) => <tr key={record.sequence} className="border-b border-border/65 last:border-b-0 hover:bg-accent/25"><td className="whitespace-nowrap px-4 py-3 text-xs text-muted-foreground">{formatTimestamp(record.event.observedAt)}</td><td className="px-4 py-3"><SeverityBadge severity={record.event.severity} /></td><td className="px-4 py-3 font-mono text-xs">{humanize(record.event.domain)}</td><td className="max-w-[420px] px-4 py-3"><p className="line-clamp-2">{record.event.message}</p>{record.event.operation ? <p className="mt-1 font-mono text-[11px] text-muted-foreground">{record.event.operation}</p> : null}</td><td className="px-4 py-3 text-xs text-muted-foreground">{evidenceCount(record)}</td><td className="px-4 py-3"><Button size="sm" variant="ghost" onClick={() => onSelect(record)}>Details</Button></td></tr>)}</tbody></table></div>
}

function Pagination({ canGoPrevious, canGoNext, isFetching, onPrevious, onNext, page, reachedPageBound, invalidNextCursor }: { canGoPrevious: boolean; canGoNext: boolean; isFetching: boolean; onPrevious: () => void; onNext: () => void; page: number; reachedPageBound: boolean; invalidNextCursor: boolean }) {
  return <div className="mt-4"><div className="flex flex-wrap items-center justify-between gap-3 text-sm text-muted-foreground"><span aria-live="polite">Bounded page {page} of at most {MAXIMUM_LOG_PAGES}</span><div className="flex gap-2"><Button variant="outline" size="sm" onClick={onPrevious} disabled={!canGoPrevious || isFetching}><ChevronLeft aria-hidden="true" /> Previous</Button><Button variant="outline" size="sm" onClick={onNext} disabled={!canGoNext || isFetching}>Next <ChevronRight aria-hidden="true" /></Button></div></div>{reachedPageBound ? <p className="mt-3 text-xs text-muted-foreground">This view reached its {MAXIMUM_LOG_PAGES}-page safety limit. Narrow the filters to continue examining retained evidence.</p> : null}{invalidNextCursor ? <Alert className="mt-3"><AlertTriangle aria-hidden="true" /><AlertTitle>Pagination cursor was rejected</AlertTitle><AlertDescription>The service returned an invalid cursor, so pagination stopped without substituting or rounding a value.</AlertDescription></Alert> : null}</div>
}

function ArtifactReceipt({ receipt }: { receipt: DiagnosticArtifactReceipt }) { return <section className="mt-6 rounded-xl border border-primary/35 bg-primary/5 p-5" aria-labelledby="export-receipt-heading"><div className="flex gap-3"><FileCheck2 className="mt-0.5 size-5 text-primary" aria-hidden="true" /><div className="min-w-0"><h2 id="export-receipt-heading" className="text-sm font-semibold">Controlled export receipt</h2><p className="mt-1 text-sm text-muted-foreground">The service published the bounded, redacted artifact under its controlled artifact authority.</p><dl className="mt-4 grid gap-3 text-sm sm:grid-cols-3"><ReceiptFact label="Artifact reference" value={receipt.artifactReference} mono /><ReceiptFact label="Size" value={`${groupDecimal(String(receipt.byteLength))} bytes`} /><ReceiptFact label="SHA-256" value={receipt.sha256} mono /></dl></div></div></section> }
function LogRecordDetails({ record }: { record: StructuredLogRecord }) {
  const event = record.event
  const redactedFieldCount = Object.values(event.fields).filter(
    (value) => value === "[REDACTED]",
  ).length
  const indexed = [
    ["Observed", formatTimestamp(event.observedAt)],
    ["Sequence", record.sequence],
    ["Severity", humanize(event.severity)],
    ["Domain", humanize(event.domain)],
    ["Operation", event.operation],
    ["Source ID", event.sourceId],
    ["Job ID", event.jobId],
    ["Correlation ID", event.correlationId],
  ] as const

  return (
    <div className="grid gap-5">
      <Alert>
        <ShieldCheck aria-hidden="true" />
        <AlertTitle>Redaction evidence</AlertTitle>
        <AlertDescription>
          {redactedFieldCount > 0
            ? `${redactedFieldCount} structured field${redactedFieldCount === 1 ? " is" : "s are"} marked [REDACTED] by the service.`
            : "This record contains only the service-approved structured fields shown below."}
        </AlertDescription>
      </Alert>
      <section>
        <p className="text-sm font-medium">Message</p>
        <p className="mt-1 whitespace-pre-wrap text-sm leading-6 text-muted-foreground">
          {event.message}
        </p>
      </section>
      <dl className="grid gap-3 sm:grid-cols-2">
        {indexed.map(([label, value]) => (
          <ReceiptFact
            key={label}
            label={label}
            value={value ?? "Not recorded"}
            mono={label.endsWith("ID") || label === "Sequence"}
          />
        ))}
      </dl>
      <section>
        <p className="text-sm font-medium">Redacted structured fields</p>
        {Object.entries(event.fields).length ? (
          <dl className="mt-3 grid gap-2 rounded-lg border border-border p-3">
            {Object.entries(event.fields).map(([name, value]) => (
              <ReceiptFact key={name} label={humanize(name)} value={value} mono />
            ))}
          </dl>
        ) : (
          <p className="mt-1 text-sm text-muted-foreground">
            No additional safe fields were recorded.
          </p>
        )}
      </section>
    </div>
  )
}
function ReceiptFact({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) { return <div className="min-w-0"><dt className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">{label}</dt><dd className={cn("mt-1 break-words text-sm", mono && "font-mono text-xs")}>{value}</dd></div> }
function SeverityBadge({ severity }: { severity: StructuredLogRecord["event"]["severity"] }) { const colors = { trace: "border-slate-500/40 bg-slate-500/10 text-slate-700 dark:text-slate-300", debug: "border-sky-500/40 bg-sky-500/10 text-sky-700 dark:text-sky-300", info: "border-primary/40 bg-primary/10 text-primary", warn: "border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300", error: "border-destructive/40 bg-destructive/10 text-destructive" } satisfies Record<typeof severity, string>; return <span className={cn("inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium", colors[severity])}>{humanize(severity)}</span> }
function evidenceCount(record: StructuredLogRecord) { const dimensions = [record.event.sourceId, record.event.jobId, record.event.correlationId].filter(Boolean).length; const fields = Object.keys(record.event.fields).length; return `${dimensions} indexed · ${fields} field${fields === 1 ? "" : "s"}` }
function filterSummary(filter: OperationLogFilter) { const parts = [filter.fromUnixNanos || filter.throughUnixNanos ? "time range" : null, filter.minimumSeverity ? `severity ≥ ${humanize(filter.minimumSeverity)}` : null, filter.domain ? humanize(filter.domain) : null, filter.sourceId ? "source" : null, filter.jobId ? "job" : null, filter.correlationId ? "correlation" : null, filter.search ? "text search" : null].filter(Boolean); return parts.length ? parts.join(" · ") : "All retained domains and severities" }
