import * as React from "react"

export type ChartTime = number | string | bigint

export interface ObservedPricePoint {
  timeUnixNanos: ChartTime
  value: number
  quality: string
}

export interface ForecastPricePoint {
  timeUnixNanos: ChartTime
  central: number
  interval50?: readonly [number, number]
  interval80?: readonly [number, number]
  interval95?: readonly [number, number]
  actual?: number
}

export interface PriceTargetLayer {
  id: string
  label: string
  value: number
  status: string
}

export interface ScenarioPricePath {
  id: string
  label: string
  points: readonly { timeUnixNanos: ChartTime; value: number }[]
}

export interface MarketPriceChartProps {
  observed: readonly ObservedPricePoint[]
  forecast: readonly ForecastPricePoint[]
  cutoffUnixNanos: ChartTime | null
  targets?: readonly PriceTargetLayer[]
  scenarios?: readonly ScenarioPricePath[]
  unit: string
  unavailableReason?: string
  className?: string
}

interface PlotPoint {
  time: bigint
  value: number
}

const WIDTH = 960
const HEIGHT = 390
const PADDING = { top: 28, right: 26, bottom: 44, left: 70 }

export function MarketPriceChart({
  observed,
  forecast,
  cutoffUnixNanos,
  targets = [],
  scenarios = [],
  unit,
  unavailableReason,
  className,
}: MarketPriceChartProps) {
  const gradientId = React.useId().replaceAll(":", "")
  const cutoff = parseTime(cutoffUnixNanos)
  const observedPoints = observed
    .map((point) => plotPoint(point.timeUnixNanos, point.value))
    .filter((point): point is PlotPoint => point !== null)
    .filter((point) => cutoff === null || point.time <= cutoff)
    .sort(compareTime)
  const forecastPoints = forecast
    .map((point) => ({ source: point, plot: plotPoint(point.timeUnixNanos, point.central) }))
    .filter(
      (entry): entry is { source: ForecastPricePoint; plot: PlotPoint } =>
        entry.plot !== null,
    )
    .filter((entry) => cutoff === null || entry.plot.time > cutoff)
    .sort((left, right) => compareTime(left.plot, right.plot))
  const scenarioPoints = scenarios.map((scenario) => ({
    ...scenario,
    points: scenario.points
      .map((point) => plotPoint(point.timeUnixNanos, point.value))
      .filter((point): point is PlotPoint => point !== null)
      .sort(compareTime),
  }))
  const targetValues = targets
    .map((target) => target.value)
    .filter(Number.isFinite)
  const values = [
    ...observedPoints.map((point) => point.value),
    ...forecastPoints.flatMap(({ source, plot }) => [
      plot.value,
      ...(source.interval50 ?? []),
      ...(source.interval80 ?? []),
      ...(source.interval95 ?? []),
      ...(source.actual === undefined ? [] : [source.actual]),
    ]),
    ...scenarioPoints.flatMap((scenario) =>
      scenario.points.map((point) => point.value),
    ),
    ...targetValues,
  ].filter(Number.isFinite)
  const times = [
    ...observedPoints.map((point) => point.time),
    ...forecastPoints.map(({ plot }) => plot.time),
    ...scenarioPoints.flatMap((scenario) =>
      scenario.points.map((point) => point.time),
    ),
  ]

  if (times.length === 0 || values.length === 0) {
    return (
      <ChartFrame className={className}>
        <div className="flex min-h-72 items-center justify-center p-8 text-center">
          <div className="max-w-xl">
            <p className="text-sm font-semibold">Price layers unavailable</p>
            <p className="mt-2 text-sm leading-6 text-muted-foreground">
              {unavailableReason ??
                "No timestamped observed prices or complete forecast points were supplied."}
            </p>
            <p className="mt-3 text-xs leading-5 text-muted-foreground">
              Charts show only complete dated prices and forecast ranges.
            </p>
          </div>
        </div>
        <ChartLegend />
      </ChartFrame>
    )
  }

  const minTime = times.reduce((left, right) => (left < right ? left : right))
  const maxTime = times.reduce((left, right) => (left > right ? left : right))
  const minimumValue = Math.min(...values)
  const maximumValue = Math.max(...values)
  const valuePadding = Math.max((maximumValue - minimumValue) * 0.08, 0.01)
  const yMinimum = minimumValue - valuePadding
  const yMaximum = maximumValue + valuePadding
  const x = (time: bigint) => {
    const span = maxTime - minTime
    if (span === 0n) return (PADDING.left + WIDTH - PADDING.right) / 2
    return (
      PADDING.left +
      (Number((time - minTime) / 1_000_000n) /
        Number(span / 1_000_000n || 1n)) *
        (WIDTH - PADDING.left - PADDING.right)
    )
  }
  const y = (value: number) =>
    PADDING.top +
    ((yMaximum - value) / (yMaximum - yMinimum || 1)) *
      (HEIGHT - PADDING.top - PADDING.bottom)
  const observedPath = linePath(observedPoints, x, y)
  const centralPath = linePath(
    forecastPoints.map(({ plot }) => plot),
    x,
    y,
  )
  const cutoffX = cutoff === null ? null : x(cutoff)
  const axisValues = Array.from({ length: 5 }, (_, index) =>
    yMaximum - ((yMaximum - yMinimum) * index) / 4,
  )

  return (
    <ChartFrame className={className}>
      <div className="overflow-x-auto">
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          className="min-h-[320px] w-full min-w-[680px]"
          role="img"
          aria-labelledby={`${gradientId}-title ${gradientId}-description`}
        >
          <title id={`${gradientId}-title`}>Observed and modeled price evidence</title>
          <desc id={`${gradientId}-description`}>
            Observed values end at the exact cutoff. Modeled central values and uncertainty begin
            strictly after it. Targets and deterministic scenarios use separate visual encodings.
          </desc>
          <defs>
            <linearGradient id={`${gradientId}-forecast`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stopColor="rgb(96 165 250)" stopOpacity="0.26" />
              <stop offset="1" stopColor="rgb(96 165 250)" stopOpacity="0.04" />
            </linearGradient>
          </defs>

          {axisValues.map((value) => (
            <g key={value}>
              <line
                x1={PADDING.left}
                x2={WIDTH - PADDING.right}
                y1={y(value)}
                y2={y(value)}
                stroke="currentColor"
                className="text-border"
                strokeWidth="1"
              />
              <text
                x={PADDING.left - 12}
                y={y(value) + 4}
                textAnchor="end"
                className="fill-muted-foreground font-mono text-[11px]"
              >
                {formatValue(value)}
              </text>
            </g>
          ))}

          {forecastPoints.length > 1 ? (
            <>
              <IntervalArea
                points={forecastPoints}
                interval="interval95"
                x={x}
                y={y}
                className="fill-blue-400/8"
              />
              <IntervalArea
                points={forecastPoints}
                interval="interval80"
                x={x}
                y={y}
                className="fill-blue-400/12"
              />
              <IntervalArea
                points={forecastPoints}
                interval="interval50"
                x={x}
                y={y}
                className="fill-blue-400/18"
              />
            </>
          ) : null}

          {targets.map((target) =>
            Number.isFinite(target.value) ? (
              <g key={target.id}>
                <line
                  x1={PADDING.left}
                  x2={WIDTH - PADDING.right}
                  y1={y(target.value)}
                  y2={y(target.value)}
                  stroke="rgb(251 191 36)"
                  strokeDasharray="3 6"
                  strokeWidth="1.5"
                />
                <text
                  x={WIDTH - PADDING.right}
                  y={y(target.value) - 6}
                  textAnchor="end"
                  className="fill-amber-300 text-[10px]"
                >
                  Target · not a prediction · {target.label}
                </text>
              </g>
            ) : null,
          )}

          {scenarioPoints.map((scenario) => (
            <path
              key={scenario.id}
              d={linePath(scenario.points, x, y)}
              fill="none"
              stroke="rgb(192 132 252)"
              strokeDasharray="2 6"
              strokeWidth="1.5"
              aria-label={`Deterministic scenario: ${scenario.label}`}
            />
          ))}

          {cutoffX !== null ? (
            <g>
              <line
                x1={cutoffX}
                x2={cutoffX}
                y1={PADDING.top}
                y2={HEIGHT - PADDING.bottom}
                stroke="rgb(148 163 184)"
                strokeDasharray="5 5"
              />
              <text
                x={cutoffX + 8}
                y={PADDING.top + 12}
                className="fill-muted-foreground text-[10px]"
              >
                Observed-through cutoff
              </text>
            </g>
          ) : null}

          <path
            d={observedPath}
            fill="none"
            stroke="rgb(226 232 240)"
            strokeWidth="2.25"
          />
          <path
            d={centralPath}
            fill="none"
            stroke="rgb(96 165 250)"
            strokeDasharray="7 5"
            strokeWidth="2.25"
          />
          {forecastPoints.map(({ source, plot }) =>
            source.actual === undefined || !Number.isFinite(source.actual) ? null : (
              <circle
                key={plot.time.toString()}
                cx={x(plot.time)}
                cy={y(source.actual)}
                r="4"
                fill="rgb(52 211 153)"
                stroke="rgb(6 78 59)"
                strokeWidth="1.5"
              />
            ),
          )}

          <text
            x={PADDING.left}
            y={HEIGHT - 14}
            className="fill-muted-foreground text-[10px]"
          >
            {formatTime(minTime)}
          </text>
          <text
            x={WIDTH - PADDING.right}
            y={HEIGHT - 14}
            textAnchor="end"
            className="fill-muted-foreground text-[10px]"
          >
            {formatTime(maxTime)}
          </text>
          <text
            x="14"
            y="18"
            className="fill-muted-foreground font-mono text-[10px]"
          >
            {unit}
          </text>
        </svg>
      </div>
      <ChartLegend />
      <p className="border-t border-border px-4 py-3 text-[11px] leading-5 text-muted-foreground">
        Text summary: {observedPoints.length} observed point
        {observedPoints.length === 1 ? "" : "s"}; {forecastPoints.length} modeled point
        {forecastPoints.length === 1 ? "" : "s"}; {targets.length} target layer
        {targets.length === 1 ? "" : "s"}; {scenarioPoints.length} deterministic scenario
        {scenarioPoints.length === 1 ? "" : "s"}. Unit: {unit}.
      </p>
    </ChartFrame>
  )
}

function IntervalArea({
  points,
  interval,
  x,
  y,
  className,
}: {
  points: readonly { source: ForecastPricePoint; plot: PlotPoint }[]
  interval: "interval50" | "interval80" | "interval95"
  x: (time: bigint) => number
  y: (value: number) => number
  className: string
}) {
  const admitted = points.filter(({ source }) => {
    const bounds = source[interval]
    return (
      bounds !== undefined &&
      Number.isFinite(bounds[0]) &&
      Number.isFinite(bounds[1]) &&
      bounds[0] <= source.central &&
      source.central <= bounds[1]
    )
  })
  if (admitted.length < 2) return null
  const upper = admitted.map(({ source, plot }) => `${x(plot.time)},${y(source[interval]![1])}`)
  const lower = [...admitted]
    .reverse()
    .map(({ source, plot }) => `${x(plot.time)},${y(source[interval]![0])}`)
  return <polygon points={[...upper, ...lower].join(" ")} className={className} />
}

function ChartFrame({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}) {
  return (
    <figure
      className={`overflow-hidden rounded-xl border border-border bg-card/35 ${className ?? ""}`}
    >
      {children}
    </figure>
  )
}

function ChartLegend() {
  return (
    <figcaption className="flex flex-wrap gap-x-5 gap-y-2 border-t border-border px-4 py-3 text-[10px] uppercase tracking-wider text-muted-foreground">
      <Legend swatch="bg-slate-200" label="Observed evidence" />
      <Legend swatch="border-t-2 border-dashed border-blue-400" label="Modeled central" />
      <Legend swatch="bg-blue-400/20" label="50 / 80 / 95 uncertainty" />
      <Legend swatch="bg-emerald-400" label="Realized outcome" />
      <Legend swatch="border-t-2 border-dotted border-amber-300" label="Target · not prediction" />
      <Legend swatch="border-t-2 border-dotted border-purple-400" label="Deterministic scenario" />
    </figcaption>
  )
}

function Legend({ swatch, label }: { swatch: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-2">
      <span className={`block h-2 w-5 rounded-sm ${swatch}`} aria-hidden="true" />
      {label}
    </span>
  )
}

function plotPoint(time: ChartTime, value: number): PlotPoint | null {
  const parsed = parseTime(time)
  return parsed === null || !Number.isFinite(value) ? null : { time: parsed, value }
}

function parseTime(value: ChartTime | null): bigint | null {
  if (value === null) return null
  try {
    return typeof value === "bigint" ? value : BigInt(value)
  } catch {
    return null
  }
}

function compareTime(left: PlotPoint, right: PlotPoint): number {
  return left.time < right.time ? -1 : left.time > right.time ? 1 : 0
}

function linePath(
  points: readonly PlotPoint[],
  x: (time: bigint) => number,
  y: (value: number) => number,
): string {
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"}${x(point.time)},${y(point.value)}`)
    .join(" ")
}

function formatValue(value: number): string {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 4,
  }).format(value)
}

function formatTime(value: bigint): string {
  const milliseconds = Number(value / 1_000_000n)
  if (!Number.isSafeInteger(milliseconds)) return "Time unavailable"
  const date = new Date(milliseconds)
  return Number.isNaN(date.valueOf()) ? "Time unavailable" : date.toLocaleString()
}
