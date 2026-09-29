import { useEffect, useRef, useState } from "react"
import { CandlestickSeries, LineSeries, ColorType, createChart, type BusinessDay, type CandlestickData, type IChartApi, type ISeriesApi, type Time, type UTCTimestamp } from "lightweight-charts"

import { useDebouncedChartCallback } from "@/components/charts/market-price-chart"

import { sourceInstantUnixNanos, type MarketHistoryBar, type MarketHistoryResult, type MarketHistoryViewportInput } from "./market-history"

export function MarketHistoryChart({ result, onViewportChange, onObservationSelect }: {
  result: MarketHistoryResult | null
  onViewportChange: (viewport: MarketHistoryViewportInput) => void
  onObservationSelect: (bar: MarketHistoryBar) => void
}) {
  if (!result?.data) return <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
    <h3 className="text-sm font-semibold">Price history is unavailable</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">Historical prices cannot be shown right now.</p>
  </section>
  const history = result.data
  return <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
    <h3 className="text-base font-semibold">Price history</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">
      {history.display.returnedPointCount} original observations shown from {history.display.visibleOriginalPointCount} within the requested window.
      {history.display.reduced ? " The service retains first, last, minimum and maximum original observations for drawing." : ""}
    </p>
    {history.bars.length > 0 ? <PriceSeries history={history} nominal={history.bars[0]!.time.precision === "nominal_date"} onViewportChange={onViewportChange} onObservationSelect={onObservationSelect} />
      : <p className="mt-4 text-xs text-muted-foreground">No original price observations fall within this requested window.</p>}
    <details className="mt-4">
      <summary className="cursor-pointer text-xs font-medium">Displayed closing prices</summary>
      <ol className="mt-3 divide-y divide-border" aria-label="Displayed closing prices">
        {history.bars.slice(-30).map((bar) => {
          const coordinate = bar.time.precision === "nominal_date" ? bar.time.date : bar.time.startsAt
          return <li key={`${bar.time.precision}:${coordinate}`} className="flex justify-between gap-4 py-2 text-xs">
            <time dateTime={coordinate}>{coordinate}</time>
            <span className="font-mono">{bar.close} {history.currency}</span>
          </li>
        })}
      </ol>
    </details>
  </section>
}

function PriceSeries({ history, nominal, onViewportChange, onObservationSelect }: {
  history: NonNullable<MarketHistoryResult["data"]>; nominal: boolean
  onViewportChange: (viewport: MarketHistoryViewportInput) => void
  onObservationSelect: (bar: MarketHistoryBar) => void
}) {
  const bars = history.bars
  const currency = history.currency
  const container = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candlesRef = useRef<ISeriesApi<"Candlestick"> | null>(null)
  const closeRef = useRef<ISeriesApi<"Line">[]>([])
  const layerVisibility = useRef({ candles: false, close: true })
  const lastRange = useRef<{ from: Time; to: Time } | null>(null)
  const interacting = useRef(false)
  const [pendingViewport, setPendingViewport] = useState<MarketHistoryViewportInput | null>(null)
  useDebouncedChartCallback(pendingViewport === null ? null : JSON.stringify(pendingViewport), pendingViewport, onViewportChange)
  const [drawingIssue, setDrawingIssue] = useState<string | null>(null)
  const [days, setDays] = useState("all")
  const [showClose, setShowClose] = useState(true)
  const [showCandles, setShowCandles] = useState(false)
  const [selectedCoordinate, setSelectedCoordinate] = useState<string | null>(null)
  const visibleBars = bars
  const requestWindow = (next: string) => {
    setDays(next)
    setSelectedCoordinate(null)
    lastRange.current = null
    interacting.current = false
    const bounds = history.viewport
    if (nominal) {
      if (bounds.fullStartDate === null || bounds.fullEndDate === null) return
      const from = next === "all" ? bounds.fullStartDate : new Date(Date.parse(`${bounds.fullEndDate}T00:00:00Z`) - Number(next) * 86_400_000).toISOString().slice(0, 10)
      onViewportChange({ startDate: from < bounds.fullStartDate ? bounds.fullStartDate : from, endDate: bounds.fullEndDate, pointLimit: 512 })
    } else {
      if (bounds.fullStartUnixNanos === null || bounds.fullEndUnixNanos === null) return
      const first = BigInt(bounds.fullStartUnixNanos)
      const from = next === "all" ? first : BigInt(bounds.fullEndUnixNanos) - BigInt(next) * 86_400_000_000_000n
      onViewportChange({ startUnixNanos: (from < first ? first : from).toString(), endUnixNanos: bounds.fullEndUnixNanos, pointLimit: 512 })
    }
  }
  const selectedOriginal = selectedCoordinate === null ? null : visibleBars.find((bar) => coordinate(bar) === selectedCoordinate) ?? null
  useDebouncedChartCallback(selectedOriginal === null ? null : `${coordinate(selectedOriginal)}:${selectedOriginal.originalOrdinal}`, selectedOriginal, onObservationSelect)
  const selectedIndex = visibleBars.findIndex((bar) => coordinate(bar) === selectedCoordinate)
  const index = selectedIndex >= 0 ? selectedIndex : visibleBars.length - 1
  const selected = visibleBars[index]
  useEffect(() => {
    layerVisibility.current = { candles: showCandles, close: showClose }
    candlesRef.current?.applyOptions({ visible: showCandles })
    closeRef.current.forEach((series) => series.applyOptions({ visible: showClose }))
  }, [showCandles, showClose])
  useEffect(() => {
    if (!container.current || !visibleBars.length) return
    setDrawingIssue(null)
    const data: CandlestickData<Time>[] = []
    const originalByTime = new Map<string, string>()
    for (const bar of visibleBars) {
      const open = Number(bar.open), high = Number(bar.high), low = Number(bar.low), close = Number(bar.close)
      // Decimal conversion is only for drawing. Hover and keyboard readouts retain source amounts.
      if (![open, high, low, close].every(Number.isFinite)) { setDrawingIssue("These original amounts exceed chart drawing precision. Inspect their exact values below."); return }
      let time: Time
      if (bar.time.precision === "nominal_date") {
        const [year, month, day] = bar.time.date.split("-").map(Number)
        time = { year: year!, month: month!, day: day! } satisfies BusinessDay
      } else {
        const seconds = Number(sourceInstantUnixNanos(bar.time.startsAt)) / 1_000_000_000
        if (!Number.isFinite(seconds)) return
        time = seconds as UTCTimestamp
      }
      if (originalByTime.has(timeKey(time))) { setDrawingIssue("These original periods are closer than chart drawing precision. Inspect their exact dates and values below."); return }
      data.push({ time, open, high, low, close })
      originalByTime.set(timeKey(time), coordinate(bar))
    }
    const chart = createChart(container.current, {
      autoSize: true, height: 330,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#94a3b8" },
      grid: { vertLines: { color: "#33415540" }, horzLines: { color: "#33415540" } },
      timeScale: { timeVisible: !nominal, secondsVisible: false },
      localization: { locale: navigator.language },
    })
    const candles = chart.addSeries(CandlestickSeries, {
      upColor: "#34d399", downColor: "#fb7185", borderVisible: false,
      wickUpColor: "#34d399", wickDownColor: "#fb7185", visible: layerVisibility.current.candles,
    })
    const segments: { time: Time; value: number }[][] = []
    let segment: { time: Time; value: number }[] = []
    data.forEach((bar, index) => {
      if (visibleBars[index]!.breakBefore[2] && segment.length > 0) { segments.push(segment); segment = [] }
      segment.push({ time: bar.time, value: bar.close })
    })
    if (segment.length > 0) segments.push(segment)
    const close = segments.map((points) => {
      const series = chart.addSeries(LineSeries, { color: "#e2e8f0", lineWidth: 2, visible: layerVisibility.current.close })
      series.setData(points)
      return series
    })
    chartRef.current = chart
    candlesRef.current = candles
    closeRef.current = close
    candles.setData(data)
    chart.subscribeCrosshairMove((event) => {
      if (event.time === undefined) return
      const original = originalByTime.get(timeKey(event.time))
      if (original !== undefined) setSelectedCoordinate(original)
    })
    interacting.current = false
    if (lastRange.current) chart.timeScale().setVisibleRange(lastRange.current)
    else chart.timeScale().fitContent()
    chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!range || !interacting.current) return
      const bounds = history.viewport
      const request: MarketHistoryViewportInput = { pointLimit: 512 }
      if (nominal) {
        if (bounds.fullStartDate === null || bounds.fullEndDate === null) return
        const start = chartDate(range.from), end = chartDate(range.to)
        request.startDate = start < bounds.fullStartDate ? bounds.fullStartDate : start
        request.endDate = end > bounds.fullEndDate ? bounds.fullEndDate : end
        if (request.startDate > request.endDate) return
      } else {
        if (typeof range.from !== "number" || typeof range.to !== "number"
          || bounds.fullStartUnixNanos === null || bounds.fullEndUnixNanos === null) return
        const first = BigInt(bounds.fullStartUnixNanos), last = BigInt(bounds.fullEndUnixNanos)
        const start = BigInt(Math.floor(range.from)) * 1_000_000_000n
        const end = BigInt(Math.ceil(range.to)) * 1_000_000_000n
        request.startUnixNanos = (start < first ? first : start).toString()
        request.endUnixNanos = (end > last ? last : end).toString()
        if (BigInt(request.startUnixNanos) > BigInt(request.endUnixNanos)) return
      }
      lastRange.current = range
      setPendingViewport(request)
      setSelectedCoordinate(null)
    })
    return () => {
      if (chartRef.current === chart) chartRef.current = null
      if (candlesRef.current === candles) candlesRef.current = null
      if (closeRef.current === close) closeRef.current = []
      chart.remove()
    }
  }, [visibleBars, nominal])
  return <figure className="mt-4">
    <div className="flex flex-wrap items-center gap-4 text-xs">
      <label className="flex items-center gap-2">History window
        <select className="rounded-md border border-input bg-background px-2 py-1.5" value={days}
          onChange={(event) => requestWindow(event.target.value)}>
          <option value="all">All available</option><option value="30">Last 30 days</option>
          <option value="90">Last 90 days</option><option value="365">Last year</option>
        </select>
      </label>
      <label className="flex items-center gap-2"><input type="checkbox" className="accent-primary" checked={showClose} onChange={(event) => setShowClose(event.target.checked)} />Closing-price line</label>
      <label className="flex items-center gap-2"><input type="checkbox" className="accent-primary" checked={showCandles} onChange={(event) => setShowCandles(event.target.checked)} />Candlesticks</label>
    </div>
    <div ref={container} className="mt-3 h-[330px] w-full" role="img"
      onPointerDown={() => { interacting.current = true }} onWheel={() => { interacting.current = true }}
      aria-label={`${nominal ? "Daily" : "Dated period"} investment prices in ${currency}. Hover to inspect; exact values are also available using the date slider below.`} />
    {drawingIssue ? <p role="status" className="mt-3 text-xs text-muted-foreground">{drawingIssue}</p> : null}
    <figcaption className="mt-2 text-xs leading-5 text-muted-foreground">
      {nominal ? "Daily prices by trading date; no intraday time is implied" : "Prices by recorded period"} · {currency}.
      Drag to pan or scroll to zoom; the visible window requests original observations from the same saved generation. Select a date to load its exact evidence.
    </figcaption>
    {selected ? <div className="mt-4 rounded-lg border border-border bg-background/25 p-3">
      <label className="grid gap-2 text-xs">Inspect a recorded date
        <input type="range" min={0} max={visibleBars.length - 1} step={1} value={index}
          aria-valuetext={periodLabel(selected)} onChange={(event) => {
            const bar = visibleBars[Number(event.target.value)]
            if (bar) setSelectedCoordinate(coordinate(bar))
          }} />
      </label>
      <p className="mt-3 break-all font-mono text-xs">{periodLabel(selected)}</p>
      <dl className="mt-3 grid grid-cols-2 gap-3 sm:grid-cols-4" aria-live="polite" aria-atomic="true">
        {([['Open', selected.open], ['High', selected.high], ['Low', selected.low], ['Close', selected.close]] as const).map(([label, value]) => <div key={label}>
          <dt className="text-xs text-muted-foreground">{label}</dt><dd className="mt-1 break-all font-mono text-xs">{value} {currency}</dd>
        </div>)}
      </dl>
    </div> : null}
  </figure>
}
function coordinate(bar: MarketHistoryBar): string { return bar.time.precision === "nominal_date" ? bar.time.date : bar.time.startsAt }
function periodLabel(bar: MarketHistoryBar): string {
  return bar.time.precision === "nominal_date" ? `${bar.time.date} · trading date` : `${bar.time.startsAt} – ${bar.time.endsAt}`
}
function timeKey(time: Time): string {
  return typeof time === "object" ? `${time.year}-${String(time.month).padStart(2, "0")}-${String(time.day).padStart(2, "0")}` : String(time)
}

function chartDate(time: Time): string {
  if (typeof time === "object") return `${String(time.year).padStart(4, "0")}-${String(time.month).padStart(2, "0")}-${String(time.day).padStart(2, "0")}`
  return typeof time === "string" ? time : new Date(time * 1_000).toISOString().slice(0, 10)
}
