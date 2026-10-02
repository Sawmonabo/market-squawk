import * as React from "react"
import { keepPreviousData, useQuery } from "@tanstack/react-query"

import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { MarketHistoryChart } from "./market-history-chart"
import { parseMarketHistoryResult, sourceInstantUnixNanos, type MarketHistoryBar, type MarketHistoryViewportInput } from "./market-history"

const queryPolicy = { retry: false, refetchOnWindowFocus: false } as const

export function MarketHistoryRead({ historyToken, bootstrap, transport }: {
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const generation = React.useRef<string | undefined>(undefined)
  const [revision, setRevision] = React.useState(0)
  const [viewport, setViewport] = React.useState<MarketHistoryViewportInput>({ pointLimit: 512 })
  const [selectedBar, setSelectedBar] = React.useState<MarketHistoryBar | null>(null)
  const history = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, ...viewport, revision }),
    gcTime: 0,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const pinnedGeneration = generation.current
      const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, ...viewport,
        ...(pinnedGeneration === undefined ? {} : { generationToken: pinnedGeneration }) }, { signal }), historyToken, viewport, pinnedGeneration)
      if (signal.aborted) throw new DOMException("The view was closed.", "AbortError")
      if (result.data !== null && generation.current === undefined) generation.current = result.data.generationToken
      return result
    },
    ...queryPolicy,
  })
  const reset = () => { generation.current = undefined; setViewport({ pointLimit: 512 }); setSelectedBar(null); setRevision((current) => current + 1) }
  return <>
    <Button variant="outline" size="sm" onClick={reset}>Refresh saved history</Button>
    {history.isError ? <p role="alert" className="mt-3 text-sm text-destructive">Price history could not be loaded. Refresh saved history to open a new generation. <Button variant="outline" size="sm" onClick={() => void history.refetch()}>Retry</Button></p> : null}
    {history.isFetching ? <p role="status" className="mt-3 text-sm text-muted-foreground">Loading the requested price window…</p> : null}
    {history.data ? <MarketHistoryChart key={history.data.data?.generationToken ?? revision} result={history.data}
      onViewportChange={(next) => { setViewport(next); setSelectedBar(null) }} onObservationSelect={setSelectedBar} /> : null}
    {selectedBar !== null && history.data?.data ? <OriginalMarketBarRead key={`${selectedBar.originalOrdinal}:${history.data.data.generationToken}`}
      bar={selectedBar} historyToken={historyToken} generationToken={history.data.data.generationToken} bootstrap={bootstrap} transport={transport} /> : null}
  </>
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

