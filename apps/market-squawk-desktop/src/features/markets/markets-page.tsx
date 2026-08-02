import { useQuery } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type BookLevel,
  marketEvidence,
  resultState,
} from "./market-evidence"

export function MarketsPage() {
  const product = useProduct()

  if (product.status === "loading") return <MarketsLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState
          title="Market service is unavailable"
          detail={product.error}
        />
      </PageFrame>
    )
  }

  return (
    <ReadyMarketsPage
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadyMarketsPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const snapshot = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Market",
      "Market.GetSnapshot",
      {},
    ),
    queryFn: () => transport.query({ query: "marketSnapshot" }),
  })
  const quality = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Market",
      "Market.GetQuality",
      {},
    ),
    queryFn: () => transport.query({ query: "marketQuality" }),
  })
  const evidence = marketEvidence(snapshot.data, quality.data)
  const refreshing = snapshot.isFetching || quality.isFetching
  const failed = snapshot.isError && quality.isError
  const partialFailure = snapshot.isError !== quality.isError
  const fresh = evidence.filter((row) => row.fresh === true).length
  const verified = evidence.filter(
    (row) => row.currentQuality === "direct_verified",
  ).length

  const refresh = () => {
    void Promise.all([snapshot.refetch(), quality.refetch()])
  }

  return (
    <PageFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      {failed ? (
        <EmptyState
          title="No current market evidence"
          detail={messageFrom(snapshot.error ?? quality.error)}
        />
      ) : evidence.length === 0 && (snapshot.isLoading || quality.isLoading) ? (
        <MarketGridLoading />
      ) : evidence.length === 0 ? (
        <EmptyState
          title="No active market observations"
          detail="Connect and start a supported market source. Market Squawk will only show a market as current when the installed service returns timestamped runtime evidence."
        />
      ) : (
        <>
          {partialFailure ? (
            <Notice text="One market evidence view could not be read. The cards below show only the fields returned by the remaining view." />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-3">
            <Summary label="Observed streams" value={evidence.length} icon={Activity} />
            <Summary label="Fresh at check" value={fresh} icon={Clock3} />
            <Summary label="Direct verified" value={verified} icon={ShieldCheck} />
          </div>
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            {evidence.map((row) => (
              <MarketCard key={row.key} evidence={row} />
            ))}
          </div>
          <ResultBoundary
            snapshot={resultState(snapshot.data)}
            quality={resultState(quality.data)}
          />
        </>
      )}
    </PageFrame>
  )
}

function MarketCard({ evidence }: { evidence: ReturnType<typeof marketEvidence>[number] }) {
  const integrityIssues = [
    evidence.snapshotInitialized === false ? "Snapshot not initialized" : null,
    evidence.generationCurrent === false ? "Connection generation is not current" : null,
    evidence.crossedBook === true ? "Crossed book" : null,
  ].filter((value): value is string => value !== null)

  return (
    <article className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {evidence.venueId} · {evidence.sourceId}
          </p>
          <h2 className="mt-2 truncate text-lg font-semibold">{evidence.instrumentId}</h2>
        </div>
        <EvidenceBadge
          label={qualityName(evidence.currentQuality)}
          tone={
            evidence.currentQuality === "direct_verified" &&
            evidence.fresh === true &&
            integrityIssues.length === 0
              ? "good"
              : evidence.fresh === false || integrityIssues.length > 0
                ? "bad"
                : "neutral"
          }
        />
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3">
        <PriceLevel label="Best bid" level={evidence.bestBid} />
        <PriceLevel label="Best ask" level={evidence.bestAsk} />
      </div>

      <dl className="mt-5 grid gap-x-4 gap-y-3 border-t border-border/70 pt-4 sm:grid-cols-2">
        <Fact label="As of" value={dateTime(evidence.asOf)} />
        <Fact label="Freshness" value={truth(evidence.fresh, "Fresh", "Stale")} />
        <Fact label="Valid until" value={dateTime(evidence.sourceValidUntil)} />
        <Fact label="Feed phase" value={name(evidence.phase)} />
        <Fact label="Recorded quality" value={qualityName(evidence.recordedQuality)} />
        <Fact
          label="Book depth"
          value={depth(evidence.bidDepth, evidence.askDepth)}
        />
        <Fact
          label="Last sequence"
          value={evidence.lastSequence?.toLocaleString() ?? "Not reported"}
        />
        <Fact label="Provider product" value={name(evidence.providerProduct)} />
        <Fact label="Channel" value={name(evidence.providerChannel)} />
      </dl>

      {integrityIssues.length > 0 ? (
        <div className="mt-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-200">
          <CircleAlert className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          <span>{integrityIssues.join(" · ")}</span>
        </div>
      ) : null}
    </article>
  )
}

function PriceLevel({ label, level }: { label: string; level: BookLevel | null }) {
  return (
    <div className="rounded-lg border border-border bg-background/45 p-3">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-2 font-mono text-lg font-semibold">
        {level ? `${level.priceTicks.toLocaleString()} ticks` : "Not reported"}
      </p>
      <p className="mt-1 text-[10px] text-muted-foreground">
        {level ? `${level.quantityLots.toLocaleString()} lots` : "No book level returned"}
      </p>
    </div>
  )
}

function Summary({
  label,
  value,
  icon: Icon,
}: {
  label: string
  value: number
  icon: typeof Activity
}) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
    </div>
  )
}

function ResultBoundary({
  snapshot,
  quality,
}: {
  snapshot: ReturnType<typeof resultState>
  quality: ReturnType<typeof resultState>
}) {
  return (
    <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
      Snapshot result: {boundary(snapshot)}. Quality result: {boundary(quality)}. Prices and
      quantities remain in instrument ticks and lots because this view does not receive an
      authoritative display scale.
    </p>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Current runtime
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Markets</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Current market state with its source, venue, time, freshness, quality, and integrity
            evidence kept visible.
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

function MarketsLoading() {
  return (
    <PageFrame>
      <MarketGridLoading />
    </PageFrame>
  )
}

function MarketGridLoading() {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <Skeleton className="h-80 rounded-xl" />
      <Skeleton className="h-80 rounded-xl" />
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

function EvidenceBadge({ label, tone }: { label: string; tone: "good" | "bad" | "neutral" }) {
  const toneClass =
    tone === "good"
      ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-300"
      : tone === "bad"
        ? "border-amber-400/25 bg-amber-400/10 text-amber-200"
        : "border-border bg-background/50 text-muted-foreground"
  return (
    <span className={`shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-medium ${toneClass}`}>
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

function qualityName(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function name(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function truth(value: boolean | null, yes: string, no: string) {
  return value === null ? "Not reported" : value ? yes : no
}

function depth(bids: number | null, asks: number | null) {
  return bids === null || asks === null ? "Not reported" : `${bids} bid / ${asks} ask levels`
}

function dateTime(value: string | null) {
  if (!value) return "Not reported"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Not reported" : parsed.toLocaleString()
}

function boundary(value: ReturnType<typeof resultState>) {
  return value
    ? `${humanize(value.completeness)}, ${value.returned} of ${value.available} rows`
    : "unavailable"
}
