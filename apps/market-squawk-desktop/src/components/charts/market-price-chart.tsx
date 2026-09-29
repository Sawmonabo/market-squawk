import * as React from "react"

export type ChartTime = number | string | bigint
// Number conversion is confined to drawing; the supplied decimal remains the displayed value.
export type ChartValue = number | string
export interface ObservedPricePoint {
  timeUnixNanos: ChartTime
  value: ChartValue | null
  quality: string | null
  sessionDate?: string
}
export interface ForecastPricePoint {
  timeUnixNanos: ChartTime
  central: ChartValue
  interval50?: readonly [ChartValue, ChartValue]
  interval80?: readonly [ChartValue, ChartValue]
  interval95?: readonly [ChartValue, ChartValue]
  actual?: ChartValue
}
export interface PriceTargetLayer { id: string; label: string; value: ChartValue; status: string; fromUnixNanos?: ChartTime; throughUnixNanos?: ChartTime; throughExclusiveUnixNanos?: ChartTime }
export interface PriceRangeLayer {
  id: string; label: string; lower: ChartValue; upper: ChartValue
  fromUnixNanos?: ChartTime; throughUnixNanos?: ChartTime; throughExclusiveUnixNanos?: ChartTime
}
export interface PricePatternLayer {
  label: string
  points: readonly {
    name: string; timeUnixNanos: ChartTime; value: ChartValue
    availableAtUnixNanos: ChartTime; confirmedAtUnixNanos: ChartTime
  }[]
}
export interface ScenarioPricePath {
  id: string
  label: string
  points: readonly { timeUnixNanos: ChartTime; value: ChartValue }[]
}
export interface MarketPriceChartProps {
  observed: readonly ObservedPricePoint[]
  forecast: readonly ForecastPricePoint[]
  cutoffUnixNanos: ChartTime | null
  cutoffLabel?: string
  targets?: readonly PriceTargetLayer[]
  ranges?: readonly PriceRangeLayer[]
  pattern?: PricePatternLayer
  onPatternSelect?: (pivot: PricePatternLayer["points"][number]) => void
  scenarios?: readonly ScenarioPricePath[]
  unit: string
  title?: string
  unavailableReason?: string
  className?: string
}
type Point = { time: bigint; value: number; exact: string }
const WIDTH = 960
const HEIGHT = 390
const PAD = { top: 25, right: 28, bottom: 45, left: 80 }
const BANDS = [
  { key: "interval95", label: "95% calibrated range", color: "rgba(96,165,250,0.09)" },
  { key: "interval80", label: "80% calibrated range", color: "rgba(96,165,250,0.15)" },
  { key: "interval50", label: "50% calibrated range", color: "rgba(96,165,250,0.23)" },
] as const
const WINDOWS = [
  { value: "all", label: "All history", days: null },
  { value: "30", label: "Last 30 days", days: 30 },
  { value: "90", label: "Last 90 days", days: 90 },
  { value: "365", label: "Last year", days: 365 },
] as const

export function MarketPriceChart({ observed, forecast, cutoffUnixNanos, targets = [], ranges = [], pattern, onPatternSelect, scenarios = [],
  unit, title = "History and forecast", cutoffLabel = "Observed through", unavailableReason, className }: MarketPriceChartProps) {
  const id = React.useId()
  const [hidden, setHidden] = React.useState<ReadonlySet<string>>(new Set())
  const [window, setWindow] = React.useState("all")
  const [selectedTime, setSelectedTime] = React.useState<bigint | null>(null)
  const visible = (key: string) => !hidden.has(key)
  const toggle = (key: string) => setHidden((previous) => {
    const next = new Set(previous)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    return next
  })
  const cutoff = parseTime(cutoffUnixNanos)
  const historyRows = observed.map((source) => ({
    source, time: parseTime(source.timeUnixNanos),
    plot: source.value === null ? null : point(source.timeUnixNanos, source.value),
  }))
    .filter((entry): entry is { source: ObservedPricePoint; time: bigint; plot: Point | null } => entry.time !== null)
    .filter(({ time }) => cutoff === null || time <= cutoff)
    .sort((a, b) => compareTime(a.time, b.time))
  const history = historyRows.filter((entry): entry is { source: ObservedPricePoint; time: bigint; plot: Point } => entry.plot !== null)
  const projected = forecast.map((source) => ({ source, plot: point(source.timeUnixNanos, source.central) }))
    .filter((entry): entry is { source: ForecastPricePoint; plot: Point } => entry.plot !== null)
    .filter(({ plot }) => cutoff !== null && plot.time > cutoff)
    .sort((a, b) => compareTime(a.plot.time, b.plot.time))
  const anchor = cutoff ?? history.at(-1)?.plot.time ?? projected.at(-1)?.plot.time ?? null
  const days = WINDOWS.find((choice) => choice.value === window)?.days ?? null
  const from = anchor !== null && days !== null ? anchor - BigInt(days) * 86_400_000_000_000n : null
  const inWindow = (time: bigint) => from === null || time >= from
  const observedRows = historyRows.filter(({ time }) => inWindow(time))
  const observedPoints = history.filter(({ plot }) => inWindow(plot.time))
  const historySegments: Point[][] = []
  let historySegment: Point[] = []
  for (const entry of observedRows) {
    if (entry.source.value === null) {
      if (historySegment.length) historySegments.push(historySegment)
      historySegment = []
    } else if (entry.plot !== null) {
      historySegment.push(entry.plot)
    }
  }
  if (historySegment.length) historySegments.push(historySegment)
  const forecastPoints = projected.filter(({ plot }) => inWindow(plot.time))
  const scenarioPoints = scenarios.map((scenario) => ({ ...scenario,
    points: scenario.points.map((source) => point(source.timeUnixNanos, source.value))
      .filter((value): value is Point => value !== null && inWindow(value.time))
      .sort((a, b) => compareTime(a.time, b.time)),
  }))
  const patternPoints = (pattern?.points ?? []).map((source) => ({ source, plot: point(source.timeUnixNanos, source.value) }))
    .filter((entry): entry is { source: PricePatternLayer["points"][number]; plot: Point } => entry.plot !== null)
    .filter(({ plot }) => inWindow(plot.time))
  const layerTimes = [...ranges, ...targets].flatMap((layer) => [layer.fromUnixNanos, layer.throughUnixNanos, layer.throughExclusiveUnixNanos])
    .flatMap((value) => { const time = value === undefined ? null : parseTime(value); return time !== null && inWindow(time) ? [time] : [] })
  const times = [...new Set([
    ...observedRows.map(({ time }) => time), ...forecastPoints.map(({ plot }) => plot.time),
    ...scenarioPoints.flatMap((scenario) => scenario.points.map((plot) => plot.time)),
    ...patternPoints.map(({ plot }) => plot.time), ...layerTimes,
  ])].sort(compareTime)
  const values = [
    ...observedPoints.map(({ plot }) => plot.value),
    ...forecastPoints.flatMap(({ source, plot }) => [plot.value,
      ...BANDS.flatMap(({ key }) => validBounds(source, key) ?? []),
      ...(source.actual === undefined ? [] : [Number(source.actual)])]),
    ...targets.map((target) => Number(target.value)),
    ...ranges.flatMap((range) => [Number(range.lower), Number(range.upper)]),
    ...patternPoints.map(({ plot }) => plot.value),
    ...scenarioPoints.flatMap((scenario) => scenario.points.map((plot) => plot.value)),
  ].filter(Number.isFinite)
  const selected = selectedTime !== null && times.includes(selectedTime) ? selectedTime
    : forecastPoints.at(-1)?.plot.time ?? observedPoints.at(-1)?.plot.time ?? patternPoints.at(-1)?.plot.time ?? times.at(-1) ?? null
  const controls = [
    ...(history.length ? [{ key: "history", label: "Observed history" }] : []),
    ...(projected.length ? [{ key: "central", label: "Central forecast" }] : []),
    ...BANDS.filter(({ key }) => projected.some(({ source }) => validBounds(source, key))).map(({ key, label }) => ({ key, label })),
    ...(projected.some(({ source }) => source.actual !== undefined) ? [{ key: "actual", label: "Realized outcomes" }] : []),
    ...(patternPoints.length ? [{ key: "pattern", label: pattern!.label }] : []),
    ...ranges.map((range) => ({ key: `range:${range.id}`, label: range.label })),
    ...targets.map((target) => ({ key: `target:${target.id}`, label: target.label })),
    ...scenarios.map((scenario) => ({ key: `scenario:${scenario.id}`, label: `Scenario: ${scenario.label}` })),
  ]
  if (!times.length || !values.length) return <figure className={`rounded-xl border border-border bg-card/35 p-5 ${className ?? ""}`}>
    <figcaption className="text-sm font-semibold">{title}</figcaption>
    <p className="mt-2 text-sm text-muted-foreground">{unavailableReason ?? "No dated values are available in this window."}</p>
    {window !== "all" ? <button className="mt-3 text-sm underline" onClick={() => setWindow("all")}>Show all history</button> : null}
  </figure>
  const minTime = times[0]!
  const maxTime = times.at(-1)!
  const span = maxTime - minTime
  const minValue = Math.min(...values)
  const maxValue = Math.max(...values)
  const margin = Math.max((maxValue - minValue) * 0.08, Math.abs(maxValue) * 0.001, 0.0001)
  const low = minValue - margin
  const high = maxValue + margin
  const x = (time: bigint) => PAD.left + (span === 0n ? 0.5 : Number(time - minTime) / Number(span)) * (WIDTH - PAD.left - PAD.right)
  const y = (value: number) => PAD.top + (high - value) / (high - low) * (HEIGHT - PAD.top - PAD.bottom)
  const layerSpan = (layer: { fromUnixNanos?: ChartTime; throughUnixNanos?: ChartTime; throughExclusiveUnixNanos?: ChartTime }) => {
    if (layer.fromUnixNanos === undefined && layer.throughUnixNanos === undefined && layer.throughExclusiveUnixNanos === undefined) return [PAD.left, WIDTH - PAD.right] as const
    const start = parseTime(layer.fromUnixNanos ?? minTime) ?? minTime
    const exclusiveEnd = parseTime(layer.throughExclusiveUnixNanos ?? null)
    const end = exclusiveEnd ?? parseTime(layer.throughUnixNanos ?? maxTime) ?? maxTime
    const left = start < minTime ? minTime : start
    const right = end > maxTime ? maxTime : end
    return left < right || left === right && exclusiveEnd === null ? [x(left), x(right)] as const : null
  }
  const selectedHistory = observedPoints.find(({ plot }) => plot.time === selected)
  const selectedGap = observedRows.find(({ time, source }) => time === selected && source.value === null)
  const selectedForecast = forecastPoints.find(({ plot }) => plot.time === selected)
  const selectedSessionDate = selectedHistory?.source.sessionDate ?? selectedGap?.source.sessionDate
  const selectedDate = selectedSessionDate
    ? `Trading date ${selectedSessionDate} · recorded session close ${formatChartTimestamp(selected!)}`
    : selected === null ? "No date selected" : formatChartTimestamp(selected)
  const readout: { label: string; value: string; unit?: string }[] = []
  if (visible("history") && selectedHistory) readout.push({ label: "Observed", value: selectedHistory.plot.exact })
  if (visible("history") && selectedGap) readout.push({ label: "Observed", value: "Missing source session", unit: "" })
  if (selectedForecast) {
    const source = selectedForecast.source
    if (visible("central")) readout.push({ label: "Central forecast", value: String(source.central) })
    for (const band of BANDS) if (visible(band.key) && validBounds(source, band.key)) {
      readout.push({ label: band.label, value: `${source[band.key]![0]} – ${source[band.key]![1]}` })
    }
    if (visible("actual") && source.actual !== undefined) readout.push({ label: "Realized outcome", value: String(source.actual) })
  }
  for (const scenario of scenarioPoints) {
    const value = scenario.points.find((plot) => plot.time === selected)
    if (value && visible(`scenario:${scenario.id}`)) readout.push({ label: scenario.label, value: value.exact })
  }
  if (visible("pattern")) for (const entry of patternPoints) {
    if (entry.plot.time === selected) {
      readout.push({ label: `${entry.source.name} pivot`, value: entry.plot.exact })
      readout.push({ label: `${entry.source.name} available`, value: formatChartTimestamp(entry.source.availableAtUnixNanos), unit: "" })
      readout.push({ label: `${entry.source.name} confirmed`, value: formatChartTimestamp(entry.source.confirmedAtUnixNanos), unit: "" })
    }
  }
  const selectedWithin = (layer: { fromUnixNanos?: ChartTime; throughUnixNanos?: ChartTime; throughExclusiveUnixNanos?: ChartTime }) => selected !== null
    && (layer.fromUnixNanos === undefined || selected >= (parseTime(layer.fromUnixNanos) ?? selected))
    && (layer.throughUnixNanos === undefined || selected <= (parseTime(layer.throughUnixNanos) ?? selected))
    && (layer.throughExclusiveUnixNanos === undefined || selected < (parseTime(layer.throughExclusiveUnixNanos) ?? selected))
  for (const range of ranges) if (visible(`range:${range.id}`) && selectedWithin(range)) readout.push({ label: range.label, value: `${range.lower} – ${range.upper}` })
  for (const target of targets) if (visible(`target:${target.id}`) && selectedWithin(target)) readout.push({ label: target.label, value: String(target.value) })
  return <figure className={`overflow-hidden rounded-xl border border-border bg-card/35 ${className ?? ""}`}>
    <figcaption className="flex flex-wrap items-center justify-between gap-3 p-4">
      <span className="text-sm font-semibold">{title} · {unit}</span>
      <label className="flex items-center gap-2 text-xs">History window
        <select className="rounded-md border border-input bg-background px-2 py-1.5" value={window} onChange={(event) => { setWindow(event.target.value); setSelectedTime(null) }}>
          {WINDOWS.map((choice) => <option key={choice.value} value={choice.value}>{choice.label}</option>)}
        </select>
      </label>
    </figcaption>
    <fieldset className="flex flex-wrap gap-x-4 gap-y-2 border-y border-border px-4 py-3">
      <legend className="sr-only">Visible chart layers</legend>
      {controls.map((control) => <label key={control.key} className="inline-flex items-center gap-2 text-xs">
        <input type="checkbox" className="accent-primary" checked={visible(control.key)} onChange={() => toggle(control.key)} />{control.label}
      </label>)}
    </fieldset>
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none" className="h-[340px] w-full min-w-[640px]"
        role={onPatternSelect && patternPoints.length ? "group" : "img"} aria-labelledby={`${id}-title ${id}-description`}
        onPointerMove={(event) => {
          const bounds = event.currentTarget.getBoundingClientRect()
          const pointerX = (event.clientX - bounds.left) / bounds.width * WIDTH
          const nearest = times.reduce((best, time) => Math.abs(x(time) - pointerX) < Math.abs(x(best) - pointerX) ? time : best)
          setSelectedTime(nearest)
        }}>
        <title id={`${id}-title`}>{title}</title>
        <desc id={`${id}-description`}>Solid observed history, dashed central forecasts, and shaded calibrated ranges. Exact values for the selected date appear below. Use the date slider with arrow keys.</desc>
        {Array.from({ length: 5 }, (_, index) => high - (high - low) * index / 4).map((value, index) => <g key={index}>
          <line x1={PAD.left} x2={WIDTH - PAD.right} y1={y(value)} y2={y(value)} stroke="currentColor" className="text-border" />
          <text x={PAD.left - 8} y={y(value) + 4} textAnchor="end" className="fill-muted-foreground font-mono text-[11px]">{axisValue(value)}</text>
        </g>)}
        {BANDS.filter(({ key }) => visible(key)).map((band) => <React.Fragment key={band.key}>
          {intervalSegments(forecastPoints, band.key).map((segment, index) => segment.length === 1
            ? <line key={index} x1={x(segment[0]!.plot.time)} x2={x(segment[0]!.plot.time)} y1={y(segment[0]!.bounds[0])} y2={y(segment[0]!.bounds[1])} stroke="rgb(96 165 250)" strokeWidth="8" opacity="0.3" />
            : <polygon key={index} fill={band.color} points={[
              ...segment.map(({ plot, bounds }) => `${x(plot.time)},${y(bounds[1])}`),
              ...[...segment].reverse().map(({ plot, bounds }) => `${x(plot.time)},${y(bounds[0])}`),
            ].join(" ")} />)}
        </React.Fragment>)}
        {ranges.filter((range) => visible(`range:${range.id}`)).map((range) => {
          const span = layerSpan(range), lower = Number(range.lower), upper = Number(range.upper)
          return span === null || !Number.isFinite(lower) || !Number.isFinite(upper) || lower > upper ? null : <g key={range.id}>
            <rect x={span[0]} width={Math.max(span[1] - span[0], 1)} y={y(upper)} height={Math.max(y(lower) - y(upper), 1)} fill="#fbbf24" fillOpacity="0.09" stroke="#fbbf24" strokeOpacity="0.6" strokeDasharray="3 6" />
            <title>{range.label}: {range.lower} – {range.upper} {unit}</title>
          </g>
        })}
        {targets.filter((target) => visible(`target:${target.id}`) && Number.isFinite(Number(target.value))).map((target) => {
          const span = layerSpan(target)
          return span === null ? null : <g key={target.id}>
            <line x1={span[0]} x2={span[1]} y1={y(Number(target.value))} y2={y(Number(target.value))} stroke="#fbbf24" strokeDasharray="3 6" />
            <title>{target.label}: {target.value} {unit} · {target.status}</title>
          </g>
        })}
        {scenarioPoints.filter((scenario) => visible(`scenario:${scenario.id}`)).map((scenario) => <path key={scenario.id} d={linePath(scenario.points, x, y)} fill="none" stroke="#c084fc" strokeDasharray="2 6" strokeWidth="2" />)}
        {cutoff !== null && cutoff >= minTime && cutoff <= maxTime ? <g>
          <line x1={x(cutoff)} x2={x(cutoff)} y1={PAD.top} y2={HEIGHT - PAD.bottom} stroke="#94a3b8" strokeDasharray="4 5" />
          <text x={Math.min(x(cutoff) + 6, WIDTH - 160)} y={PAD.top + 12} className="fill-muted-foreground text-[10px]">{cutoffLabel}</text>
        </g> : null}
        {visible("history") ? <>
          {historySegments.map((segment, index) => segment.length === 1
            ? <circle key={index} cx={x(segment[0]!.time)} cy={y(segment[0]!.value)} r="4" fill="#e2e8f0" />
            : <path key={index} d={linePath(segment, x, y)} fill="none" stroke="#e2e8f0" strokeWidth="2.25" />)}
          {observedRows.filter(({ source }) => source.value === null).map(({ time }) =>
            <line key={`gap-${time}`} x1={x(time)} x2={x(time)} y1={PAD.top} y2={HEIGHT - PAD.bottom}
              stroke="#94a3b8" strokeDasharray="2 6" opacity="0.45">
              <title>Missing source session · {formatChartTimestamp(time)}</title>
            </line>)}
        </> : null}
        {visible("central") ? <>
          <path d={linePath(forecastPoints.map(({ plot }) => plot), x, y)} fill="none" stroke="#60a5fa" strokeDasharray="7 5" strokeWidth="2.25" />
          {forecastPoints.map(({ plot }) => <circle key={plot.time.toString()} cx={x(plot.time)} cy={y(plot.value)} r="3" fill="#60a5fa" />)}
        </> : null}
        {visible("actual") ? forecastPoints.map(({ source, plot }) => source.actual === undefined || !Number.isFinite(Number(source.actual)) ? null : <circle key={plot.time.toString()} cx={x(plot.time)} cy={y(Number(source.actual))} r="4" fill="#34d399" />) : null}
        {visible("pattern") && patternPoints.length ? <g>
          <path d={linePath(patternPoints.map(({ plot }) => plot), x, y)} fill="none" stroke="#c084fc" strokeWidth="2" />
          {patternPoints.map(({ source, plot }) => <g key={source.name}
            role={onPatternSelect ? "button" : undefined}
            tabIndex={onPatternSelect ? 0 : undefined}
            aria-label={onPatternSelect ? `Open ${pattern!.label} evidence for ${source.name} pivot, observed ${formatChartTimestamp(plot.time)}, ${plot.exact} ${unit}` : undefined}
            className={onPatternSelect ? "cursor-pointer outline-none [&:focus-visible>circle]:stroke-foreground [&:focus-visible>circle]:stroke-[3]" : undefined}
            onFocus={() => setSelectedTime(plot.time)}
            onClick={onPatternSelect ? () => { setSelectedTime(plot.time); onPatternSelect(source) } : undefined}
            onKeyDown={onPatternSelect ? (event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault()
                setSelectedTime(plot.time)
                onPatternSelect(source)
              }
            } : undefined}>
            {onPatternSelect ? <circle cx={x(plot.time)} cy={y(plot.value)} r="12" fill="transparent" aria-hidden="true" /> : null}
            <circle cx={x(plot.time)} cy={y(plot.value)} r="4" fill="#c084fc" />
            <text x={x(plot.time) + 6} y={y(plot.value) - 8} className="fill-purple-300 text-[11px]">{source.name}</text>
            <title>{source.name} pivot · Observed {formatChartTimestamp(plot.time)} · {plot.exact} {unit} · Available {formatChartTimestamp(source.availableAtUnixNanos)} · Confirmed {formatChartTimestamp(source.confirmedAtUnixNanos)}</title>
          </g>)}
        </g> : null}
        {selected !== null ? <line x1={x(selected)} x2={x(selected)} y1={PAD.top} y2={HEIGHT - PAD.bottom} stroke="#e2e8f0" opacity="0.5" pointerEvents="none" /> : null}
        <text x={PAD.left} y={HEIGHT - 15} className="fill-muted-foreground text-[11px]">{shortDate(minTime)}</text>
        <text x={WIDTH - PAD.right} y={HEIGHT - 15} textAnchor="end" className="fill-muted-foreground text-[11px]">{shortDate(maxTime)}</text>
      </svg>
    </div>
    <div className="border-t border-border p-4">
      <label className="grid gap-2 text-xs">Inspect a date · UTC
        <input type="range" min={0} max={times.length - 1} step={1} value={selected === null ? 0 : times.indexOf(selected)}
          aria-valuetext={selectedDate} onChange={(event) => setSelectedTime(times[Number(event.target.value)] ?? null)} />
      </label>
      <p className="mt-3 break-all font-mono text-xs">{selectedDate}</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3" aria-live="polite" aria-atomic="true">
        {readout.map((entry) => <div key={entry.label}><dt className="text-xs text-muted-foreground">{entry.label}</dt><dd className="mt-1 break-all font-mono text-xs">{entry.value} {entry.unit ?? unit}</dd></div>)}
      </dl>
      {!readout.length ? <p className="mt-3 text-xs text-muted-foreground">No visible series has an observation at this date.</p> : null}
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">Solid: observed. Dashed blue: central forecast. Blue shading: calibrated uncertainty. Amber: saved reference ranges or levels. Purple: recorded pattern geometry where supplied. Pivot positions mark when prices occurred; availability and confirmation show when the evidence became known. Observed lines stop at missing source sessions. Hovering shows the nearest recorded date without estimating a value between points. The history window changes only the view; the saved forecast horizon stays fixed.</p>
    </div>
  </figure>
}
function validBounds(source: ForecastPricePoint, key: typeof BANDS[number]["key"]): [number, number] | null {
  const raw = source[key]
  if (!raw) return null
  const lower = Number(raw[0]), upper = Number(raw[1]), central = Number(source.central)
  return [lower, upper, central].every(Number.isFinite) && lower <= central && central <= upper ? [lower, upper] : null
}
function intervalSegments(points: { source: ForecastPricePoint; plot: Point }[], key: typeof BANDS[number]["key"]) {
  const segments: { plot: Point; bounds: [number, number] }[][] = []
  let segment: { plot: Point; bounds: [number, number] }[] = []
  for (const entry of points) {
    const bounds = validBounds(entry.source, key)
    if (bounds) segment.push({ plot: entry.plot, bounds })
    else if (segment.length) { segments.push(segment); segment = [] }
  }
  if (segment.length) segments.push(segment)
  return segments
}
function point(time: ChartTime, value: ChartValue): Point | null {
  const parsed = parseTime(time), drawingValue = Number(value)
  return parsed === null || !Number.isFinite(drawingValue) ? null : { time: parsed, value: drawingValue, exact: String(value) }
}
function parseTime(value: ChartTime | null): bigint | null {
  if (value === null || typeof value === "number" && !Number.isSafeInteger(value)) return null
  try { return BigInt(value) } catch { return null }
}
function compareTime(a: bigint, b: bigint) { return a < b ? -1 : a > b ? 1 : 0 }
function linePath(points: readonly Point[], x: (time: bigint) => number, y: (value: number) => number) {
  return points.map((plot, index) => `${index ? "L" : "M"}${x(plot.time)},${y(plot.value)}`).join(" ")
}
function axisValue(value: number) { return new Intl.NumberFormat(undefined, { maximumSignificantDigits: 6 }).format(value) }
export function formatChartTimestamp(coordinate: ChartTime | null): string {
  const value = parseTime(coordinate)
  if (value === null) return "Unavailable"
  const seconds = value >= 0n ? value / 1_000_000_000n : (value - 999_999_999n) / 1_000_000_000n
  const milliseconds = Number(seconds * 1_000n)
  const date = new Date(milliseconds)
  if (!Number.isSafeInteger(milliseconds) || Number.isNaN(date.valueOf())) return `${value} ns since Unix epoch`
  return `${date.toISOString().replace(/\.\d{3}Z$/, "")}.${(value - seconds * 1_000_000_000n).toString().padStart(9, "0")}Z`
}
function shortDate(value: bigint) { return formatChartTimestamp(value).slice(0, 10) }
