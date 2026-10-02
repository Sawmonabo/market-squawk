import * as React from "react"
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query"

import { productKeys, snapshotQueryMeta } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryChart } from "./market-history-chart"
import { parseMarketHistoryResult, sourceInstantUnixNanos, type MarketHistoryBar, type MarketHistoryResult, type MarketHistoryViewportInput } from "./market-history"

const queryPolicy = { retry: false, refetchOnWindowFocus: false } as const

export function MarketHistoryRead({ historyToken, bootstrap, transport }: {
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const generation = React.useRef<string | undefined>(undefined)
  const lastChecked = React.useRef<MarketHistoryResult | undefined>(undefined)
  const epoch = React.useRef(0)
  const alive = React.useRef(true)
  const [revision, setRevision] = React.useState(0)
  const [refreshing, setRefreshing] = React.useState(false)
  const [viewport, setViewport] = React.useState<MarketHistoryViewportInput>({ pointLimit: 512 })
  const [selectedBar, setSelectedBar] = React.useState<MarketHistoryBar | null>(null)
  React.useEffect(() => {
    alive.current = true
    return () => { alive.current = false; epoch.current += 1; lastChecked.current = undefined }
  }, [])
  const queryKey = productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, ...viewport, revision })
  const history = useQuery({
    queryKey,
    gcTime: 0,
    meta: snapshotQueryMeta,
    placeholderData: keepPreviousData,
    enabled: !refreshing,
    queryFn: async ({ signal }) => {
      const requestEpoch = epoch.current
      const pinnedGeneration = generation.current
      const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, ...viewport,
        ...(pinnedGeneration === undefined ? {} : { generationToken: pinnedGeneration }) }, { signal }), historyToken, viewport, pinnedGeneration)
      if (signal.aborted || !alive.current || requestEpoch !== epoch.current) throw new DOMException("The view was closed.", "AbortError")
      if (result.data !== null && generation.current === undefined) generation.current = result.data.generationToken
      lastChecked.current = result
      return result
    },
    ...queryPolicy,
  })
  const reset = async () => {
    setRefreshing(true)
    epoch.current += 1
    await queryClient.cancelQueries({ queryKey, exact: true })
    if (!alive.current) return
    generation.current = undefined
    setSelectedBar(null)
    setRevision((current) => current + 1)
    setRefreshing(false)
  }
  const result = history.data ?? lastChecked.current
  const busy = history.isFetching || refreshing
  return <div className="mt-3 min-h-[640px]">
    <Button variant="outline" size="sm" disabled={refreshing} onClick={() => void reset()}>Refresh saved history</Button>
    <div className="mt-2 min-h-16 text-xs leading-5">
      {history.isError ? <div className="flex items-start justify-between gap-3">
        <p role="alert" className="text-destructive">{result?.data
          ? "Price history could not be updated. Showing the last checked price window; its currentness has not been verified."
          : "Price history could not be loaded. Refresh saved history to try again."}</p>
        <Button variant="outline" size="sm" disabled={busy} onClick={() => void history.refetch()}>Retry</Button>
      </div> : busy ? <p role="status" className="text-muted-foreground">{result?.data ? "Updating the requested price window… Showing the last checked prices." : "Loading the requested price window…"}</p> : null}
    </div>
    {result ? <div className={`[&>section]:mt-0 [&>section]:rounded-none [&>section]:border-0 [&>section]:bg-transparent [&>section]:p-0 ${result.data ? "[&>section>h3]:hidden" : ""}`}>
      <MarketHistoryChart key={result.data?.generationToken ?? "unavailable"} result={result}
        onViewportChange={(next) => { if (!refreshing) { setViewport(next); setSelectedBar(null) } }} onObservationSelect={setSelectedBar} />
    </div> : <div className="flex h-[536px] items-center justify-center text-sm text-muted-foreground">{history.isError ? "No checked price history is available." : "Opening saved price history…"}</div>}
    {selectedBar !== null && result?.data ? <OriginalMarketBarRead key={`${selectedBar.originalOrdinal}:${result.data.generationToken}`}
      bar={selectedBar} historyToken={historyToken} generationToken={result.data.generationToken} bootstrap={bootstrap} transport={transport} /> : null}
  </div>
}

function OriginalMarketBarRead({ bar, historyToken, generationToken, bootstrap, transport }: {
  bar: MarketHistoryBar; historyToken: string; generationToken: string; bootstrap: DesktopBootstrap; transport: ProductTransport
}) {
  const viewport: MarketHistoryViewportInput = bar.time.precision === "nominal_date"
    ? { startDate: bar.time.date, endDate: bar.time.date, pointLimit: 8 }
    : { startUnixNanos: sourceInstantUnixNanos(bar.time.startsAt), endUnixNanos: sourceInstantUnixNanos(bar.time.startsAt), pointLimit: 8 }
  const original = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, generationToken, ...viewport }),
    gcTime: 0,
    meta: snapshotQueryMeta,
    queryFn: async ({ signal }) => {
      const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, generationToken, ...viewport }, { signal }), historyToken, viewport, generationToken)
      const point = result.data?.bars.find((candidate) => candidate.originalOrdinal === bar.originalOrdinal)
      if (!point) throw new Error("The original price observation could not be verified.")
      return point
    },
  })
  return <div className="mt-4 rounded-lg border border-border p-3 text-xs">
    <p className="font-medium">Exact original price observation</p>
    {original.isPending ? <p role="status" className="mt-2 text-muted-foreground">Checking saved evidence…</p>
      : original.isError ? <p role="alert" className="mt-2 text-destructive">The saved observation could not be verified. <Button size="xs" variant="outline" onClick={() => void original.refetch()}>Retry</Button></p>
        : <dl className="mt-2 grid grid-cols-2 gap-2 sm:grid-cols-5">{([['Open', original.data.open], ['High', original.data.high], ['Low', original.data.low], ['Close', original.data.close], ['Volume', original.data.volume]] as const).map(([label, value]) => <div key={label}><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 font-mono">{value}</dd></div>)}</dl>}
  </div>
}
