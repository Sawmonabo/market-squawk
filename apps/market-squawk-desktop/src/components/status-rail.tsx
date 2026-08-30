import * as React from "react"

import type { ProductScope } from "@/app/query-client"
import { useProduct } from "@/app/product-context"
import { GlobalLookup } from "@/features/lookup/global-lookup"
import {
  isActiveResearchActivity,
  useHomeStatusQueries,
} from "@/features/overview/use-overview"
import type { ProductTransport } from "@/lib/transport"

export function StatusRail() {
  const product = useProduct()

  return (
    <section
      aria-label="Workspace summary"
      className="flex min-h-7 shrink-0 items-center gap-4 border-b border-border/70 bg-card/20 px-5 font-mono text-[9px] uppercase tracking-wide text-muted-foreground"
    >
      <StatusFact
        label="Workspace"
        value={
          product.status === "loading"
            ? "Starting"
            : product.status === "ready"
              ? "Ready"
              : "Unavailable"
        }
        ready={product.status === "ready"}
      />
      {product.status === "ready" ? (
        <>
          <ReadyStatusRail
            transport={product.transport}
            scope={product.bootstrap.runtime}
          />
        </>
      ) : null}
      <div className="ml-auto flex items-center gap-2">
        {product.status === "ready" ? (
          <GlobalLookup
            transport={product.transport}
            scope={product.bootstrap.runtime}
          />
        ) : null}
        <CurrentClock />
      </div>
    </section>
  )
}

function ReadyStatusRail({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const status = useHomeStatusQueries(transport, scope)
  const activeAnalyses =
    status.activities.status === "ready"
      ? status.activities.data.filter(isActiveResearchActivity).length
      : null
  return (
    <>
      <StatusFact
        label="Markets"
        value={status.markets.status === "ready" ? String(status.markets.data?.length ?? 0) : statusLabel(status.markets.status)}
        ready={status.markets.status === "ready" && (status.markets.data?.length ?? 0) > 0}
      />
      <StatusFact
        label="Analysis"
        value={
          activeAnalyses === null
            ? statusLabel(status.activities.status)
            : `${activeAnalyses} active`
        }
        ready={status.activities.status === "ready"}
      />
    </>
  )
}

function CurrentClock() {
  const [now, setNow] = React.useState(() => new Date())
  React.useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1_000)
    return () => window.clearInterval(timer)
  }, [])
  return (
    <time className="tabular-nums" dateTime={now.toISOString()}>
      {new Intl.DateTimeFormat(undefined, {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        timeZoneName: "short",
      }).format(now)}
    </time>
  )
}

function statusLabel(status: "loading" | "ready" | "unavailable") {
  return status === "loading" ? "Checking" : status === "ready" ? "Ready" : "Unavailable"
}

function StatusFact({
  label,
  value,
  ready = false,
}: {
  label: string
  value: string
  ready?: boolean
}) {
  return (
    <span className="flex items-center gap-1.5">
      {ready ? (
        <span
          className="size-1.5 rounded-full bg-[var(--success)]"
          aria-hidden="true"
        />
      ) : null}
      <span>{label}</span>
      <strong className="font-medium text-foreground/80">{value}</strong>
    </span>
  )
}
