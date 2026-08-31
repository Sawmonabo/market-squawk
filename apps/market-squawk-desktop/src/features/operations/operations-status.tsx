import {
  Activity,
  Clock3,
  Database,
  HardDrive,
  LoaderCircle,
  RefreshCw,
  Server,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

import type { RuntimeStatus } from "./contracts"

export function OperationalHealth({
  status,
  pending,
  refreshing,
  error,
  onRefresh,
}: {
  status: RuntimeStatus | undefined
  pending: boolean
  refreshing: boolean
  error: string | null
  onRefresh: () => void
}) {
  const facts = status
    ? [
    {
      icon: Server,
      title: "Service health",
      value: "Ready",
      reason: `${status.connectedClients.toLocaleString()} connected client${status.connectedClients === 1 ? "" : "s"} · workspace generation ${status.workspace.generation}.`,
    },
    {
      icon: Database,
      title: "Runtime activity",
      value: `${status.runningJobs.toLocaleString()} jobs`,
      reason: `${status.runningMutationJobs.toLocaleString()} mutating · ${status.activeSources.toLocaleString()} active source${status.activeSources === 1 ? "" : "s"}.`,
    },
    {
      icon: HardDrive,
      title: "Available storage",
      value: formatBytes(status.availableDiskBytes),
      reason: `Workspace schema ${status.workspaceSchemaVersion} · ${status.workspace.workspaceId}.`,
    },
    {
      icon: Activity,
      title: "Paper execution",
      value: status.paperExecutionActive ? "Active" : "Stopped",
      reason: status.executionReconciliationPending
        ? "Execution reconciliation is pending."
        : "No execution reconciliation is pending.",
    },
  ] as const
    : []

  return (
    <section className="mt-7" aria-labelledby="operational-health-heading">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 id="operational-health-heading" className="text-lg font-semibold">
            Operational health
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Fresh, bounded facts from the shared installed service and active workspace.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={onRefresh} disabled={refreshing}>
          <RefreshCw className={cn(refreshing && "animate-spin")} aria-hidden="true" />
          Refresh health
        </Button>
      </div>
      {pending ? (
        <div className="mt-4 flex items-center gap-3 rounded-xl border border-border bg-card/30 p-5 text-sm text-muted-foreground" role="status">
          <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
          Loading operational health…
        </div>
      ) : error ? (
        <div className="mt-4 rounded-xl border border-destructive/35 bg-destructive/10 p-4">
          <p className="text-sm font-medium text-destructive">Operational health is unavailable</p>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{error}</p>
        </div>
      ) : (
      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {facts.map(({ icon: Icon, title, value, reason }) => (
          <div
            key={title}
            className="rounded-xl border border-border bg-card/30 p-4"
          >
            <Icon className="size-4 text-muted-foreground" aria-hidden="true" />
            <h3 className="mt-3 text-sm font-medium">{title}</h3>
            <p className="mt-1 font-mono text-lg font-semibold">{value}</p>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              {reason}
            </p>
          </div>
        ))}
      </div>
      )}
    </section>
  )
}

function formatBytes(value: string): string {
  const bytes = BigInt(value)
  const gibibyte = 1024n ** 3n
  const mebibyte = 1024n ** 2n
  if (bytes >= gibibyte) return `${Number(bytes / (gibibyte / 10n)) / 10} GiB`
  if (bytes >= mebibyte) return `${Number(bytes / (mebibyte / 10n)) / 10} MiB`
  return `${bytes.toString()} bytes`
}

export function SummaryFact({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Activity
  label: string
  value: number
  detail: string
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
        {detail}
      </p>
    </section>
  )
}

export function LoadingState() {
  return (
    <div
      className="mt-4 flex items-center gap-3 rounded-xl border border-border bg-card/30 p-5 text-sm text-muted-foreground"
      role="status"
    >
      <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
      Loading durable jobs…
    </div>
  )
}

export function EmptyJobs() {
  return (
    <div className="mt-4 rounded-xl border border-dashed border-border p-7 text-center">
      <Clock3 className="mx-auto size-5 text-muted-foreground" aria-hidden="true" />
      <h3 className="mt-3 text-sm font-medium">No durable jobs yet</h3>
      <p className="mx-auto mt-1 max-w-md text-xs leading-relaxed text-muted-foreground">
        Long-running research, model, forecast, backtest, import, export, and
        recovery work will appear here after it is admitted by the service.
      </p>
    </div>
  )
}
