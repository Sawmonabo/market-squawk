import {
  Activity,
  Boxes,
  Cable,
  CircleAlert,
  Database,
  ShieldCheck,
} from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import type { DesktopBootstrap } from "@/lib/schemas"

import type { McpClientsStatus } from "./contracts"

export function ServiceEvidence({
  bootstrap,
  status,
}: {
  bootstrap: DesktopBootstrap
  status: McpClientsStatus
}) {
  const verifiedClients = status.clients.filter(
    (client) => client.verification !== null,
  )
  const latestVerification = [...verifiedClients]
    .sort(
      (left, right) =>
        (right.verification?.verifiedAtUnixSeconds ?? 0) -
        (left.verification?.verifiedAtUnixSeconds ?? 0),
    )
    .at(0)?.verification

  return (
    <>
      {!status.serviceReady || !status.sharedEndpointReady ? (
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Shared MCP service needs attention</AlertTitle>
          <AlertDescription>
            The application service or its shared endpoint is not ready. Client
            changes remain unavailable until service repair restores both facts.
          </AlertDescription>
        </Alert>
      ) : null}

      <section
        aria-label="Shared MCP service"
        className="grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-3"
      >
        <ServiceFact
          icon={Activity}
          label="Shared service"
          value={status.serviceReady ? "Ready" : "Unavailable"}
          detail={`Market Squawk ${bootstrap.applicationVersion}`}
          healthy={status.serviceReady}
        />
        <ServiceFact
          icon={Cable}
          label="Endpoint"
          value={status.sharedEndpointReady ? "Ready" : "Unavailable"}
          detail="One authenticated local endpoint"
          healthy={status.sharedEndpointReady}
        />
        <ServiceFact
          icon={Database}
          label="Active workspace"
          value={shortIdentity(status.workspaceId)}
          detail={`Service generation ${status.serviceGeneration}`}
          healthy
        />
        <ServiceFact
          icon={ShieldCheck}
          label="Protocol"
          value={status.protocolVersion}
          detail="Shared through a stateless stdio relay"
          healthy={Boolean(status.protocolVersion)}
        />
        <ServiceFact
          icon={Boxes}
          label="Verified capabilities"
          value={
            latestVerification
              ? `${latestVerification.toolCount} tools · ${latestVerification.resourceCount} resources`
              : "Not verified"
          }
          detail={
            latestVerification
              ? `${verifiedClients.length} client${verifiedClients.length === 1 ? "" : "s"} verified`
              : "Verify a connected client to inspect the real surface"
          }
          healthy={Boolean(latestVerification)}
        />
        <ServiceFact
          icon={CircleAlert}
          label="Request activity"
          value={`${status.runtime.activeRequests} active`}
          detail={`${status.runtime.activeClients} active clients · ${status.runtime.admittedRequests ?? "overflow"} admitted · ${status.runtime.rateLimitedRequests ?? "overflow"} limited`}
          healthy={status.runtime.rateLimitedRequests === 0}
        />
      </section>

      <section className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4" aria-label="MCP runtime limits">
        <RuntimeFact
          label="Service process"
          value={formatBytes(status.runtime.process.residentMemoryBytes)}
          detail={`${formatDuration(status.runtime.uptimeSeconds)} uptime · ${status.runtime.sessionModel === "stateless_request_scoped" ? "stateless request sessions" : status.runtime.sessionModel}`}
        />
        <RuntimeFact
          label="Global request ceiling"
          value={`${status.runtime.limits.maximumActiveRequests} requests`}
          detail={`${formatBytes(status.runtime.limits.maximumBodyBytes)} request body · ${status.runtime.limits.requestTimeoutMilliseconds / 1_000}s deadline`}
        />
        <RuntimeFact
          label="Inline result ceiling"
          value={formatBytes(status.runtime.limits.maximumInlineBytes)}
          detail={`${status.runtime.limits.maximumInlineItems.toLocaleString()} logical items before artifact handoff`}
        />
        <RuntimeFact
          label="Maximum result"
          value={formatBytes(status.runtime.limits.maximumResultBytes)}
          detail={`${status.runtime.limits.maximumResultItems.toLocaleString()} logical items · ${status.runtime.rejectedCredentials} rejected credentials`}
        />
      </section>
    </>
  )
}

function RuntimeFact({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="rounded-lg border border-border bg-card/35 p-4">
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 text-sm font-semibold">{value}</p>
      <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function formatBytes(value: number | null) {
  if (value === null) return "Unavailable"
  if (value < 1_024) return `${value} B`
  const units = ["KiB", "MiB", "GiB"]
  let amount = value / 1_024
  let unit = units[0]
  for (let index = 1; index < units.length && amount >= 1_024; index += 1) {
    amount /= 1_024
    unit = units[index]
  }
  return `${amount.toFixed(amount >= 10 ? 1 : 2)} ${unit}`
}

function formatDuration(seconds: number) {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h`
  return `${Math.floor(seconds / 86_400)}d`
}

function ServiceFact({
  icon: Icon,
  label,
  value,
  detail,
  healthy = false,
}: {
  icon: typeof Activity
  label: string
  value: string
  detail: string
  healthy?: boolean
}) {
  return (
    <div className="border-b border-border p-4 last:border-b-0 sm:[&:nth-last-child(-n+2)]:border-b-0 xl:border-b-0 xl:border-r xl:last:border-r-0">
      <div className="flex items-center gap-2 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        {label}
      </div>
      <p className="mt-3 flex items-center gap-2 text-sm font-semibold">
        {healthy ? (
          <span className="size-1.5 rounded-full bg-[var(--success)]" aria-hidden="true" />
        ) : null}
        {value}
      </p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function shortIdentity(value: string) {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}
