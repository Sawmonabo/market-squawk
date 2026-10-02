import * as React from "react"
import { formatCalendarDate } from "@/lib/time"

import { formatChartTimestamp, useDebouncedChartCallback, type ChartViewport } from "@/components/charts/market-price-chart"

import type { InvestmentChart } from "./contracts"

type Benchmark = InvestmentChart["benchmark"]
type AvailableBenchmark = Extract<Benchmark, { state: "available" }>

const WIDTH = 960
const HEIGHT = 360
const PAD = { top: 28, right: 24, bottom: 42, left: 66 }
const COLORS = ["#e2e8f0", "#60a5fa", "#c084fc"] as const
const WINDOWS = [
  { key: "all", label: "All saved dates", days: null },
  { key: "30", label: "Last 30 days", days: 30 },
  { key: "90", label: "Last 90 days", days: 90 },
  { key: "365", label: "Last year", days: 365 },
] as const
const UNAVAILABLE = {
  selection_unavailable: "The requested comparison could not be used for this saved analysis.",
  missing_subject: "Price history for this investment was unavailable as of the analysis date.",
  missing_selected_comparison: "Price history for the selected comparison was unavailable as of the analysis date.",
  no_common_observation: "The two investments had no shared session with recorded prices.",
  storage_unavailable: "The original saved comparison data could not be reopened.",
  not_requested: "Open the comparison layer to load its saved observations.",
  integrity_unproven: "The original comparison could not be verified.",
} as const

export function SavedBenchmarkChart({ benchmark, currency, onViewportChange, onObservationSelect }: {
  benchmark: Benchmark; currency: string
  onViewportChange: (viewport: ChartViewport) => void
  onObservationSelect: (point: AvailableBenchmark["points"][number]) => void
}) {
  const subject = benchmark.members[0]
  if (subject === undefined) return <section className="rounded-xl border border-border bg-card/35 p-4"><h4 className="text-sm font-semibold">Saved comparisons</h4><p className="mt-2 text-xs text-muted-foreground">Open the comparison layer to load its saved observations.</p></section>
  const selected = benchmark.members[1]
  const sameSelection = selected !== undefined && subject.instrumentId === selected.instrumentId
  const accompanying = benchmark.members[2]
  const duplicateAccompanying = accompanying !== undefined && benchmark.members.slice(0, 2)
    .some((member) => member.instrumentId === accompanying.instrumentId)
  return <section className="rounded-xl border border-border bg-card/35" aria-label="Saved price comparison">
    <div className="p-4">
      <h4 className="text-sm font-semibold">{benchmark.state === "available"
        ? <>Price change since <time dateTime={benchmark.baseline.date} title={benchmark.baseline.date}>{formatCalendarDate(benchmark.baseline.date)}</time></> : "Comparison data unavailable"}</h4>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">
        {subject.label}{selected ? ` compared with ${selected.label}` : " has no saved comparison investment"}
        {selected && accompanying && !duplicateAccompanying ? `, with ${accompanying.label} alongside` : ""}.
      </p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        {selected ? `Saved selected comparison: ${selected.label}.` : "No selected comparison identity is available in this saved analysis."}
      </p>
      {sameSelection ? <p className="mt-1 text-xs leading-5 text-muted-foreground">
        The selected comparison is this investment itself, so its price lines may overlap.
      </p> : null}
      {duplicateAccompanying ? <p className="mt-1 text-xs leading-5 text-muted-foreground">
        The accompanying comparison is already shown as this investment or the selected comparison.
      </p> : null}
      {benchmark.state === "available" ? <p className="mt-2 text-xs leading-5 text-muted-foreground">
        The saved split-adjusted prices start at 100 on the same trading date. An index value of 105 means
        the price is 5% above its starting price. Cash distributions and trading costs are excluded.
      </p> : <>
        <p className="mt-3 text-xs leading-5 text-muted-foreground">{UNAVAILABLE[benchmark.reason]}</p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">{benchmark.summary}</p>
      </>}
    </div>
    {benchmark.state === "available" && benchmark.points.length > 0
      ? <AvailableChart benchmark={benchmark} currency={currency} onViewportChange={onViewportChange} onObservationSelect={onObservationSelect} />
      : benchmark.state === "available" ? <p className="p-4 text-xs text-muted-foreground">No original comparison observations fall within this requested window.</p> : null}
  </section>
}

function AvailableChart({ benchmark, currency, onViewportChange, onObservationSelect }: {
  benchmark: AvailableBenchmark; currency: string
  onViewportChange: (viewport: ChartViewport) => void
  onObservationSelect: (point: AvailableBenchmark["points"][number]) => void
}) {
  const id = React.useId()
  const [window, setWindow] = React.useState<(typeof WINDOWS)[number]["key"]>("all")
  const [hidden, setHidden] = React.useState<ReadonlySet<number>>(new Set())
  const [selectedTime, setSelectedTime] = React.useState<string | null>(null)
  const displayMembers = React.useMemo(() => benchmark.members.map((member, index) => ({ member, index }))
    .filter(({ member, index }) => index !== 2 || !benchmark.members.slice(0, 2)
      .some((earlier) => earlier.instrumentId === member.instrumentId)), [benchmark.members])
  const rows = benchmark.points
  const selectedOriginal = selectedTime === null ? null : rows.find((point) => point.coordinate.sessionCloseUnixNanos === selectedTime) ?? null
  useDebouncedChartCallback(selectedOriginal === null ? null : `${selectedOriginal.coordinate.sessionCloseUnixNanos}:${selectedOriginal.originalOrdinal}`,
    selectedOriginal, onObservationSelect)
  const requestWindow = (next: typeof window) => {
    setWindow(next)
    setSelectedTime(null)
    const first = benchmark.display.firstTimeUnixNanos
    const last = benchmark.display.lastTimeUnixNanos
    if (first === null || last === null) return
    const days = WINDOWS.find((choice) => choice.key === next)?.days ?? null
    const from = days === null ? BigInt(first) : BigInt(last) - BigInt(days) * 86_400_000_000_000n
    onViewportChange({ fromUnixNanos: (from < BigInt(first) ? BigInt(first) : from).toString(), throughUnixNanos: last, pointLimit: 512 })
  }
  const rowIndexes = React.useMemo(() => new Map<string, number>(rows.map((row, index): [string, number] =>
    [row.coordinate.sessionCloseUnixNanos, index])), [rows])
  const plot = React.useMemo(() => {
    const firstTime = BigInt(rows[0]!.coordinate.sessionCloseUnixNanos)
    const finalTime = BigInt(rows.at(-1)!.coordinate.sessionCloseUnixNanos)
    const span = finalTime - firstTime
    const x = (time: string) => PAD.left + (span === 0n ? 0.5
      : Number(BigInt(time) - firstTime) / Number(span)) * (WIDTH - PAD.left - PAD.right)
    let minimum = 100
    let maximum = 100
    for (const row of rows) for (const item of row.observations) if (item !== null) {
      const value = Number(item.priceIndex)
      if (Number.isFinite(value)) {
        minimum = Math.min(minimum, value)
        maximum = Math.max(maximum, value)
      }
    }
    const margin = Math.max((maximum - minimum) * 0.08, 0.5)
    const low = minimum - margin
    const high = maximum + margin
    const y = (value: number) => PAD.top + (high - value) / (high - low) * (HEIGHT - PAD.top - PAD.bottom)
    const segments = benchmark.members.map((_, memberIndex) => {
      const result: { time: string; value: number }[][] = []
      let segment: { time: string; value: number }[] = []
      for (const row of rows) {
        const value = row.observations[memberIndex]?.priceIndex
        const numeric = value === undefined ? NaN : Number(value)
        if (row.breakBefore[memberIndex] && segment.length) {
          result.push(segment)
          segment = []
        }
        if (value !== undefined && Number.isFinite(numeric)) {
          segment.push({ time: row.coordinate.sessionCloseUnixNanos, value: numeric })
        } else if (segment.length) {
          result.push(segment)
          segment = []
        }
      }
      if (segment.length) result.push(segment)
      return result
    })
    return { x, y, low, high, segments }
  }, [rows, benchmark.members])
  const seriesElements = React.useMemo(() => displayMembers.map(({ member, index }) => hidden.has(index) ? null : <g key={member.role}>
    {plot.segments[index]!.map((segment, segmentIndex) => segment.length === 1
      ? <circle key={segmentIndex} cx={plot.x(segment[0]!.time)} cy={plot.y(segment[0]!.value)} r="3.5" fill={COLORS[index]} />
      : <path key={segmentIndex} d={segment.map((point, pointIndex) =>
          `${pointIndex ? "L" : "M"}${plot.x(point.time)},${plot.y(point.value)}`).join(" ")}
          fill="none" stroke={COLORS[index]} strokeDasharray={index === 1 ? "7 4" : undefined} strokeWidth="2.25" />)}
  </g>), [displayMembers, hidden, plot])
  const chosenIndex = rowIndexes.get(selectedTime ?? "") ?? rows.length - 1
  const chosen = rows[chosenIndex]!
  const chosenTime = chosen.coordinate.sessionCloseUnixNanos
  const toggle = (memberIndex: number) => setHidden((previous) => {
    const next = new Set(previous)
    if (next.has(memberIndex)) next.delete(memberIndex)
    else next.add(memberIndex)
    return next
  })
  const coordinateLabel = `${formatCalendarDate(chosen.coordinate.date)} · regular session close ${formatChartTimestamp(chosenTime)}`
  const missingMembers = React.useMemo(() => displayMembers.filter(({ index }) =>
    benchmark.points.every((point) => point.observations[index] === null)), [benchmark.points, displayMembers])
  return <>
    {missingMembers.length ? <p className="border-t border-border px-4 py-3 text-xs leading-5 text-muted-foreground">
      No saved comparison prices are available for {missingMembers.map(({ member }) => member.label).join(", ")}.
    </p> : null}
    <fieldset className="flex flex-wrap gap-x-5 gap-y-2 border-y border-border px-4 py-3">
      <legend className="sr-only">Visible comparisons</legend>
      {displayMembers.map(({ member, index }) => <label key={member.role} className="inline-flex items-center gap-2 text-xs">
        <input type="checkbox" className="accent-primary" checked={!hidden.has(index)}
          onChange={() => toggle(index)} />
        <span className="inline-block size-2 rounded-full" style={{ backgroundColor: COLORS[index] }} aria-hidden="true" />
        {member.label}
      </label>)}
      <label className="ml-auto inline-flex items-center gap-2 text-xs">History window
        <select className="rounded-md border border-input bg-background px-2 py-1.5" value={window}
          onChange={(event) => requestWindow(event.target.value as typeof window)}>
          {WINDOWS.map((choice) => <option key={choice.key} value={choice.key}>{choice.label}</option>)}
        </select>
      </label>
    </fieldset>
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none"
        className="h-[320px] w-full min-w-[640px]" role="img" aria-labelledby={`${id}-title ${id}-description`}
        onPointerMove={(event) => {
          const bounds = event.currentTarget.getBoundingClientRect()
          if (bounds.width <= 0) return
          const pointerX = (event.clientX - bounds.left) / bounds.width * WIDTH
          let lower = 0
          let upper = rows.length - 1
          while (lower < upper) {
            const middle = Math.floor((lower + upper) / 2)
            if (plot.x(rows[middle]!.coordinate.sessionCloseUnixNanos) < pointerX) lower = middle + 1
            else upper = middle
          }
          const after = rows[lower]!
          const before = rows[Math.max(0, lower - 1)]!
          const nearest = Math.abs(plot.x(before.coordinate.sessionCloseUnixNanos) - pointerX)
            <= Math.abs(plot.x(after.coordinate.sessionCloseUnixNanos) - pointerX) ? before : after
          const nearestTime = nearest.coordinate.sessionCloseUnixNanos
          if (nearestTime !== chosenTime) setSelectedTime(nearestTime)
        }}>
        <title id={`${id}-title`}>Saved split-adjusted price comparison</title>
        <desc id={`${id}-description`}>Base-100 saved price indexes. Missing observations break each line. The date slider below reads exact saved values with arrow keys.</desc>
        {Array.from({ length: 5 }, (_, index) => plot.high - (plot.high - plot.low) * index / 4).map((value, index) => <g key={index}>
          <line x1={PAD.left} x2={WIDTH - PAD.right} y1={plot.y(value)} y2={plot.y(value)} stroke="currentColor" className="text-border" />
          <text x={PAD.left - 8} y={plot.y(value) + 4} textAnchor="end" className="fill-muted-foreground font-mono text-[11px]">{value.toFixed(2)}</text>
        </g>)}
        <line x1={PAD.left} x2={WIDTH - PAD.right} y1={plot.y(100)} y2={plot.y(100)} stroke="#fbbf24" strokeDasharray="3 5" opacity="0.65" />
        {seriesElements}
        <line x1={plot.x(chosenTime)} x2={plot.x(chosenTime)} y1={PAD.top} y2={HEIGHT - PAD.bottom} stroke="#e2e8f0" opacity="0.5" />
        <text x={PAD.left} y={HEIGHT - 15} className="fill-muted-foreground text-[11px]">{formatCalendarDate(rows[0]!.coordinate.date)}</text>
        <text x={WIDTH - PAD.right} y={HEIGHT - 15} textAnchor="end" className="fill-muted-foreground text-[11px]">{formatCalendarDate(rows.at(-1)!.coordinate.date)}</text>
      </svg>
    </div>
    <div className="border-t border-border p-4">
      <label className="grid gap-2 text-xs">Inspect a trading date
        <input type="range" min={0} max={rows.length - 1} step={1} value={chosenIndex}
          aria-valuetext={coordinateLabel}
          onChange={(event) => {
            const nextTime = rows[Number(event.target.value)]?.coordinate.sessionCloseUnixNanos
            if (nextTime !== undefined && nextTime !== chosenTime) setSelectedTime(nextTime)
          }} />
      </label>
      <p className="mt-3 text-xs" data-session-date={chosen.coordinate.date} data-session-close-unix-nanos={chosenTime}>{coordinateLabel}</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3" aria-live="polite" aria-atomic="true">
        {displayMembers.map(({ member, index }) => hidden.has(index) ? null : <div key={member.role}>
          <dt className="text-xs text-muted-foreground">{member.label}</dt>
          <dd className="mt-1 text-xs">{chosen.observations[index]
            ? <><span className="font-mono">{chosen.observations[index]!.priceIndex}</span> price index
              <span className="block font-mono text-muted-foreground">Split-adjusted close {chosen.observations[index]!.close} {currency}</span></>
            : "No saved observation on this date"}</dd>
        </div>)}
      </dl>
      {displayMembers.every(({ index }) => hidden.has(index))
        ? <p className="mt-3 text-xs text-muted-foreground">Show a comparison to inspect its value.</p> : null}
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        Lines connect recorded observations only. A break means that investment has no saved price on the intervening session.
        Moving the pointer or using the date slider shows the original date and exact recorded values.
      </p>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
        {benchmark.display.returnedPointCount} original observations shown from {benchmark.display.visibleOriginalPointCount} within the requested window
        {benchmark.display.reduced ? "; the service preserves the first, last, minimum and maximum observations for drawing." : "."}
        Select a date to load its exact saved evidence.
      </p>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{benchmark.summary}</p>
    </div>
  </>
}
