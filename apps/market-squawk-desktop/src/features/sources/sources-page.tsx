import * as React from "react"
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  DatabaseZap,
  KeyRound,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type LifecycleControl,
  lifecycleControls,
  sourceEvidence,
} from "./source-evidence"

export function SourcesPage() {
  const product = useProduct()
  if (product.status === "loading") return <SourcesLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState title="Source service is unavailable" detail={product.error} />
      </PageFrame>
    )
  }
  return (
    <ReadySourcesPage
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadySourcesPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const statusReads = useQueries({
    queries: bootstrap.providerProfiles.map((profile) => ({
      queryKey: productKeys.operation(
        bootstrap.runtime,
        "Source",
        "Source.GetStatus",
        { sourceIds: [profile.id] },
      ),
      queryFn: () =>
        transport.query({ query: "sourceStatus", sourceIds: [profile.id] }),
    })),
  })
  const coverage = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Source",
      "Source.GetCoverage",
      {},
    ),
    queryFn: () => transport.query({ query: "sourceCoverage" }),
  })
  const health = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Source",
      "Source.GetHealth",
      {},
    ),
    queryFn: () => transport.query({ query: "sourceHealth" }),
  })
  const sources = sourceEvidence(
    bootstrap.providerProfiles,
    bootstrap.providerSessions,
    statusReads.flatMap((query) => (query.data ? [query.data] : [])),
    coverage.data,
    health.data,
  )
  const refreshing =
    statusReads.some((query) => query.isFetching) ||
    coverage.isFetching ||
    health.isFetching
  const failedStatusReads = statusReads.filter((query) => query.isError).length
  const failedReads =
    failedStatusReads + Number(coverage.isError) + Number(health.isError)
  const totalReads = statusReads.length + 2
  const active = sources.filter((source) => source.runtimeState === "active").length
  const fresh = sources.filter((source) => source.marketFreshness === "fresh").length

  const refresh = () => {
    void Promise.all([
      ...statusReads.map((query) => query.refetch()),
      coverage.refetch(),
      health.refetch(),
    ])
  }
  const refreshAuthority = () =>
    queryClient.invalidateQueries({
      queryKey: productKeys.domain(bootstrap.runtime, "Source"),
    })

  return (
    <PageFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      {failedReads === totalReads ? (
        <EmptyState
          title="Source evidence could not be read"
          detail={messageFrom(
            statusReads.find((query) => query.error)?.error ??
              coverage.error ??
              health.error,
          )}
        />
      ) : sources.length === 0 && refreshing ? (
        <SourceGridLoading />
      ) : sources.length === 0 ? (
        <EmptyState
          title="No registered sources"
          detail="Complete source setup to register an available provider or local dataset."
        />
      ) : (
        <>
          {failedReads > 0 ? (
            <Notice text={`${failedReads} of ${totalReads} source evidence reads could not be completed. Missing fields remain explicitly unreported.`} />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-3">
            <Summary label="Known sources" value={sources.length} icon={DatabaseZap} />
            <Summary label="Runtime active" value={active} icon={Activity} />
            <Summary label="Market fresh" value={fresh} icon={ShieldCheck} />
          </div>
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            {sources.map((source) => (
              <SourceCard
                key={source.id}
                source={source}
                transport={transport}
                onChanged={refreshAuthority}
              />
            ))}
          </div>
        </>
      )}
    </PageFrame>
  )
}

function SourceCard({
  source,
  transport,
  onChanged,
}: {
  source: ReturnType<typeof sourceEvidence>[number]
  transport: ProductTransport
  onChanged: () => Promise<void>
}) {
  const [pending, setPending] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [confirming, setConfirming] = React.useState<LifecycleControl | null>(null)
  const controls = lifecycleControls(source)
  const setupReady = source.nextAction === "active"
  const runtimeActive = source.runtimeState === "active"

  const run = async (control: LifecycleControl) => {
    setConfirming(null)
    setPending(control.action)
    setError(null)
    try {
      await transport.sourceControl(
        control.action,
        control.request,
        true,
      )
      await onChanged()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  return (
    <article className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="truncate font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {source.id}
          </p>
          <h2 className="mt-2 text-lg font-semibold">{source.name}</h2>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            {source.declaredCoverage ?? "Coverage is not reported by the installed service."}
          </p>
        </div>
        <StateBadge
          label={runtimeActive ? "Runtime active" : runtimeName(source.runtimeState)}
          active={runtimeActive}
        />
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        <EvidencePanel
          title="Setup"
          icon={KeyRound}
          headline={setupReady ? "Setup ready" : runtimeName(source.setupState)}
          detail={
            source.nextAction
              ? `Next setup action: ${humanize(source.nextAction)}`
              : "No setup action reported"
          }
        />
        <EvidencePanel
          title="Runtime"
          icon={Activity}
          headline={runtimeName(source.runtimeState)}
          detail={source.connection ? `Connection: ${humanize(source.connection)}` : "Connection not reported"}
        />
      </div>

      <dl className="mt-5 grid gap-x-4 gap-y-3 border-t border-border/70 pt-4 sm:grid-cols-2">
        <Fact label="Runtime source" value={source.sourceId ?? "Not reported"} />
        <Fact label="Venue" value={source.venueId ?? "Not reported"} />
        <Fact label="Instrument" value={source.instrumentId ?? "Not reported"} />
        <Fact label="Market freshness" value={runtimeName(source.marketFreshness)} />
        <Fact label="Stream integrity" value={runtimeName(source.integrity)} />
        <Fact label="Current quality" value={runtimeName(source.quality)} />
        <Fact label="Coverage state" value={runtimeName(source.coverageState)} />
        <Fact label="Quality ceiling" value={runtimeName(source.qualityCeiling)} />
        <Fact label="Runtime observed" value={dateTime(source.observedAt)} />
        <Fact label="Cost condition" value={source.zeroFee ?? "Not reported"} />
        <Fact label="Release state" value={runtimeName(source.releaseState)} />
        <Fact label="Account" value={source.accountRequirement ?? "Not reported"} />
        <Fact label="Credential" value={source.credentialRequirement ?? "Not reported"} />
      </dl>

      {controls.length > 0 ? (
        <div className="mt-5 flex flex-wrap gap-2 border-t border-border/70 pt-4">
          {controls.map((control) => (
            <Button
              key={control.action}
              size="sm"
              variant={control.destructive ? "outline" : "default"}
              disabled={pending !== null}
              onClick={() => {
                if (control.destructive) setConfirming(control)
                else void run(control)
              }}
            >
              {pending === control.action ? "Working…" : control.label}
            </Button>
          ))}
          <p className="w-full text-[10px] text-muted-foreground">
            Revision {source.lifecycle?.stateRevision}; controls use this exact returned state.
          </p>
        </div>
      ) : null}

      {error ? (
        <p role="alert" className="mt-4 text-xs text-red-400">
          {error}
        </p>
      ) : null}
      <Dialog
        open={confirming !== null}
        onOpenChange={(open) => {
          if (!open && pending === null) setConfirming(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirming?.label ?? "Change source state"}?</DialogTitle>
            <DialogDescription>
              This changes the runtime state for {source.name} using lifecycle revision{" "}
              {source.lifecycle?.stateRevision}. Market Squawk will reject the request if that
              source state has changed since this evidence was loaded.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" disabled={pending !== null} onClick={() => setConfirming(null)}>
              Keep current state
            </Button>
            <Button
              disabled={pending !== null || confirming === null}
              onClick={() => {
                if (confirming) void run(confirming)
              }}
            >
              {pending !== null ? "Working…" : "Confirm change"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </article>
  )
}

function EvidencePanel({
  title,
  headline,
  detail,
  icon: Icon,
}: {
  title: string
  headline: string
  detail: string
  icon: typeof Activity
}) {
  return (
    <div className="rounded-lg border border-border bg-background/45 p-3">
      <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        {title}
      </div>
      <p className="mt-3 text-sm font-medium">{headline}</p>
      <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function Summary({ label, value, icon: Icon }: { label: string; value: number; icon: typeof Activity }) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
    </div>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Provider evidence
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Sources</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Setup, runtime connection, coverage, freshness, integrity, and data quality remain
            separate so a configured source is never mistaken for a healthy market feed.
          </p>
        </div>
        {action}
      </div>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <DatabaseZap className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function SourcesLoading() {
  return (
    <PageFrame>
      <SourceGridLoading />
    </PageFrame>
  )
}

function SourceGridLoading() {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <Skeleton className="h-96 rounded-xl" />
      <Skeleton className="h-96 rounded-xl" />
    </div>
  )
}

function Notice({ text }: { text: string }) {
  return (
    <div className="mb-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-100">
      <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
      {text}
    </div>
  )
}

function StateBadge({ label, active }: { label: string; active: boolean }) {
  return (
    <span
      className={
        active
          ? "shrink-0 rounded-full border border-emerald-400/25 bg-emerald-400/10 px-2.5 py-1 text-[10px] font-medium text-emerald-300"
          : "shrink-0 rounded-full border border-border bg-background/50 px-2.5 py-1 text-[10px] font-medium text-muted-foreground"
      }
    >
      {label}
    </span>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs text-foreground/85">{value}</dd>
    </div>
  )
}

function runtimeName(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function dateTime(value: string | null) {
  if (!value) return "Not reported"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Not reported" : parsed.toLocaleString()
}
