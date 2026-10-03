import * as React from "react"
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query"

import { productKeys, snapshotQueryMeta } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { HistoryPreparation } from "./history-preparation"
import { MarketHistoryChart } from "./market-history-chart"
import { parseMarketHistoryResult, sourceInstantUnixNanos, type MarketHistoryBar, type MarketHistoryResult, type MarketHistoryViewportInput } from "./market-history"
import { usePreparationController } from "./preparation-controls"

const queryPolicy = { retry: false, refetchOnWindowFocus: false } as const

export function MarketHistoryRead({ historyToken, bootstrap, transport, refreshRevision = 0 }: {
  refreshRevision?: number
  historyToken: string
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const generation = React.useRef<string | undefined>(undefined)
  const awaitingReadRecovery = React.useRef(false)
  const lastChecked = React.useRef<MarketHistoryResult | undefined>(undefined)
  const lastGood = React.useRef<MarketHistoryResult | undefined>(undefined)
  const epoch = React.useRef(0)
  const alive = React.useRef(true)
  const [revision, setRevision] = React.useState(0)
  const [refreshing, setRefreshing] = React.useState(false)
  const [viewport, setViewport] = React.useState<MarketHistoryViewportInput>({ pointLimit: 512 })
  const [windowDays, setWindowDays] = React.useState("all")
  const [selectedBar, setSelectedBar] = React.useState<MarketHistoryBar | null>(null)
  React.useEffect(() => {
    alive.current = true
    return () => { alive.current = false; epoch.current += 1; lastChecked.current = undefined; lastGood.current = undefined }
  }, [])
  const queryKey = productKeys.operation(bootstrap.productSessionToken, "market", "Market.GetHistory", { historyToken, ...viewport, revision })
  const history = useQuery({
    queryKey,
    gcTime: 0,
    meta: generation.current === undefined || awaitingReadRecovery.current ? { domainRefresh: "automatic" } : snapshotQueryMeta,
    placeholderData: keepPreviousData,
    enabled: !refreshing,
    queryFn: async ({ signal }) => {
      const requestEpoch = epoch.current
      const pinnedGeneration = generation.current
      try {
        const result = parseMarketHistoryResult(await transport.query({ query: "marketHistory", historyToken, ...viewport,
          ...(pinnedGeneration === undefined ? {} : { generationToken: pinnedGeneration }) }, { signal }), historyToken, viewport, pinnedGeneration)
        if (signal.aborted || !alive.current || requestEpoch !== epoch.current) throw new DOMException("The view was closed.", "AbortError")
        if (result.data !== null) {
          if (generation.current === undefined) generation.current = result.data.generationToken
          lastGood.current = result
        }
        awaitingReadRecovery.current = result.unavailableReason === "temporarily_unavailable"
        lastChecked.current = result
        return result
      } catch (error) {
        if (!signal.aborted && alive.current && requestEpoch === epoch.current) awaitingReadRecovery.current = true
        throw error
      }
    },
    ...queryPolicy,
  })
  const reset = React.useCallback(async () => {
    setRefreshing(true)
    epoch.current += 1
    await queryClient.cancelQueries({ queryKey, exact: true })
    if (!alive.current) return
    generation.current = undefined
    awaitingReadRecovery.current = false
    setSelectedBar(null)
    setRevision((current) => current + 1)
    setRefreshing(false)
  }, [queryClient, queryKey])
  const seenRefresh = React.useRef(refreshRevision)
  React.useEffect(() => {
    if (seenRefresh.current === refreshRevision) return
    seenRefresh.current = refreshRevision
    void reset()
  }, [refreshRevision, reset])
  const observed = history.data ?? lastChecked.current
  const result = (history.isError || observed?.unavailableReason === "temporarily_unavailable") && lastGood.current
    ? lastGood.current : observed
  const preparation = usePreparationController({ kind: "history", token: historyToken, bootstrap, transport, onPrepared: reset })
  const initialLoadingAttempted = React.useRef(false)
  React.useEffect(() => {
    if (initialLoadingAttempted.current || !history.isSuccess || history.isPlaceholderData
      || history.data?.unavailableReason !== "not_available" || generation.current !== undefined
      || preparation.preparation || preparation.storageError || !preparation.canStart) return
    initialLoadingAttempted.current = true
    setWindowDays("365")
    setViewport(recentHistoryWindow(365, history.data))
    preparation.start(365)
  }, [history.isSuccess, history.isPlaceholderData, history.data, preparation])
  const restoredWindow = React.useRef(false)
  React.useEffect(() => {
    if (restoredWindow.current) return
    const days = preparation.preparation?.lookbackDays
    if (days === undefined) { restoredWindow.current = true; return }
    setWindowDays(String(days))
    if (result === undefined) return
    restoredWindow.current = true
    setViewport(recentHistoryWindow(days, result))
  }, [preparation.preparation, result])
  const selectWindow = (days: string) => {
    setWindowDays(days)
    setSelectedBar(null)
    setViewport(days === "all" ? { pointLimit: 512 } : recentHistoryWindow(Number(days), result))
    if (days !== "all") preparation.start(Number(days))
  }
  const busy = history.isFetching || refreshing
  const temporarilyUnavailable = observed?.unavailableReason === "temporarily_unavailable"
  const showPreparationStatus = !history.isError && !temporarilyUnavailable && Boolean(preparation.active || preparation.unresolved || preparation.busy
    || preparation.storageError || preparation.preparation?.error || preparation.status.isError
    || preparation.job && preparation.job.state !== "completed")
  const showReadStatus = !showPreparationStatus && (history.isError || temporarilyUnavailable || busy)
  return <div className="mt-3 min-h-[640px]">
    <HistoryPreparation controller={preparation} windowDays={windowDays} onWindowChange={selectWindow} showStatus={showPreparationStatus} />
    {result?.data || result && !showPreparationStatus && !showReadStatus ? <div className={`[&>section]:mt-0 [&>section]:rounded-none [&>section]:border-0 [&>section]:bg-transparent [&>section]:p-0 ${result?.data ? "[&>section>h3]:hidden" : ""}`}>
      <MarketHistoryChart result={result ?? null}
        windowDays={windowDays}
        onViewportChange={(next) => { if (!refreshing) { setViewport(next); setSelectedBar(null) } }} onObservationSelect={setSelectedBar} />
    </div> : <div className="h-[536px]" aria-hidden="true" />}
    <div className="mt-2 min-h-10 text-xs leading-5">
      {showReadStatus && (history.isError || temporarilyUnavailable) ? <div className="flex items-start justify-between gap-3">
        <p role="alert" className="text-destructive">{result?.data
          ? "Price history could not be updated. Showing saved prices, which may be out of date."
          : "Price history could not be loaded. Try again."}</p>
        <Button variant="outline" size="sm" disabled={busy} onClick={() => void history.refetch()}>Retry</Button>
      </div> : showReadStatus ? <p role="status" className="text-muted-foreground">{result?.data ? "Updating prices… Showing saved prices." : "Loading prices…"}</p> : null}
    </div>
    {selectedBar !== null && result?.data ? <OriginalMarketBarRead key={`${selectedBar.originalOrdinal}:${result.data.generationToken}`}
      bar={selectedBar} historyToken={historyToken} generationToken={result.data.generationToken} bootstrap={bootstrap} transport={transport} /> : null}
  </div>
}

function recentHistoryWindow(days: number, result: MarketHistoryResult | undefined): MarketHistoryViewportInput {
  // The first read establishes the source time precision; guessing dates rejects
  // timestamped histories when their first publication arrives.
  if (!result?.data) return { pointLimit: 512 }
  const end = Date.now()
  const start = end - days * 86_400_000
  const timestamped = result?.data?.bars[0]?.time.precision === "timestamped_period"
    || result?.data?.viewport.fullEndUnixNanos !== null && result?.data?.viewport.fullEndUnixNanos !== undefined
  return timestamped
    ? { startUnixNanos: (BigInt(start) * 1_000_000n).toString(), endUnixNanos: (BigInt(end) * 1_000_000n).toString(), pointLimit: 512 }
    : { startDate: new Date(start).toISOString().slice(0, 10), endDate: new Date(end).toISOString().slice(0, 10), pointLimit: 512 }
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
