import { useEffect, useMemo, useRef, useState } from "react"
import { CandlestickSeries, LineSeries, ColorType, createChart, type BusinessDay, type CandlestickData, type IChartApi, type ISeriesApi, type Time, type UTCTimestamp } from "lightweight-charts"

import type { MarketHistoryBar, MarketHistoryResult } from "./market-history"

export function MarketHistoryChart({ result }: { result: MarketHistoryResult | null }) {
  if (!result?.data) return <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
    <h3 className="text-sm font-semibold">Price history is unavailable</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">Historical prices cannot be shown right now.</p>
  </section>
  const history = result.data
  return <section className="mt-5 rounded-xl border border-border bg-card/30 p-5">
    <h3 className="text-base font-semibold">Price history</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">
      {history.partial ? "Part of the available history is shown." : "The available history is shown."}
    </p>
    <PriceSeries key={history.historyToken} bars={history.bars} currency={history.currency} nominal={history.bars[0]!.time.precision === "nominal_date"} />
    <details className="mt-4">
      <summary className="cursor-pointer text-xs font-medium">Recent closing prices</summary>
      <ol className="mt-3 divide-y divide-border" aria-label="Recent closing prices">
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

function PriceSeries({ bars, currency, nominal }: { bars: MarketHistoryBar[]; currency: string; nominal: boolean }) {
  const container = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candlesRef = useRef<ISeriesApi<"Candlestick"> | null>(null)
  const closeRef = useRef<ISeriesApi<"Line"> | null>(null)
  const layerVisibility = useRef({ candles: false, close: true })
  const [days, setDays] = useState("all")
  const [showClose, setShowClose] = useState(true)
  const [showCandles, setShowCandles] = useState(false)
  const [selectedCoordinate, setSelectedCoordinate] = useState<string | null>(null)
  const visibleBars = useMemo(() => {
    if (days === "all") return bars
    // Time-window selection changes the view only; no bars or financial values are aggregated.
    const last = bars.at(-1)
    if (!last) return []
    const cutoff = Date.parse(coordinate(last)) - Number(days) * 86_400_000
    return bars.filter((bar) => Date.parse(coordinate(bar)) >= cutoff)
  }, [bars, days])
  const selectedIndex = visibleBars.findIndex((bar) => coordinate(bar) === selectedCoordinate)
  const index = selectedIndex >= 0 ? selectedIndex : visibleBars.length - 1
  const selected = visibleBars[index]
  useEffect(() => {
    layerVisibility.current = { candles: showCandles, close: showClose }
    candlesRef.current?.applyOptions({ visible: showCandles })
    closeRef.current?.applyOptions({ visible: showClose })
  }, [showCandles, showClose])
  useEffect(() => {
    if (!container.current || !visibleBars.length) return
    const data: CandlestickData<Time>[] = []
    const originalByTime = new Map<string, string>()
    for (const bar of visibleBars) {
      const open = Number(bar.open), high = Number(bar.high), low = Number(bar.low), close = Number(bar.close)
      // Decimal conversion is only for drawing. Hover and keyboard readouts retain source amounts.
      if (![open, high, low, close].every(Number.isFinite)) return
      let time: Time
      if (bar.time.precision === "nominal_date") {
        const [year, month, day] = bar.time.date.split("-").map(Number)
        time = { year: year!, month: month!, day: day! } satisfies BusinessDay
      } else {
        const seconds = Date.parse(bar.time.startsAt) / 1_000
        if (!Number.isFinite(seconds)) return
        time = seconds as UTCTimestamp
      }
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
    const close = chart.addSeries(LineSeries, { color: "#e2e8f0", lineWidth: 2, visible: layerVisibility.current.close })
    chartRef.current = chart
    candlesRef.current = candles
    closeRef.current = close
    candles.setData(data)
    close.setData(data.map((bar) => ({ time: bar.time, value: bar.close })))
    chart.subscribeCrosshairMove((event) => {
      if (event.time === undefined) return
      const original = originalByTime.get(timeKey(event.time))
      if (original !== undefined) setSelectedCoordinate(original)
    })
    chart.timeScale().fitContent()
    return () => {
      if (chartRef.current === chart) chartRef.current = null
      if (candlesRef.current === candles) candlesRef.current = null
      if (closeRef.current === close) closeRef.current = null
      chart.remove()
    }
  }, [visibleBars, nominal])
  return <figure className="mt-4">
    <div className="flex flex-wrap items-center gap-4 text-xs">
      <label className="flex items-center gap-2">History window
        <select className="rounded-md border border-input bg-background px-2 py-1.5" value={days}
          onChange={(event) => { setDays(event.target.value); setSelectedCoordinate(null) }}>
          <option value="all">All available</option><option value="30">Last 30 days</option>
          <option value="90">Last 90 days</option><option value="365">Last year</option>
        </select>
      </label>
      <label className="flex items-center gap-2"><input type="checkbox" className="accent-primary" checked={showClose} onChange={(event) => setShowClose(event.target.checked)} />Closing-price line</label>
      <label className="flex items-center gap-2"><input type="checkbox" className="accent-primary" checked={showCandles} onChange={(event) => setShowCandles(event.target.checked)} />Candlesticks</label>
    </div>
    <div ref={container} className="mt-3 h-[330px] w-full" role="img"
      aria-label={`${nominal ? "Daily" : "Dated period"} investment prices in ${currency}. Hover to inspect; exact values are also available using the date slider below.`} />
    <figcaption className="mt-2 text-xs leading-5 text-muted-foreground">
      {nominal ? "Daily prices by trading date; no intraday time is implied" : "Prices by recorded period"} · {currency}.
      Drag to pan or scroll to zoom. The history window filters saved observations and does not request or estimate additional prices.
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
