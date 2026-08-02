import { useQuery } from "@tanstack/react-query"
import { AlertTriangle, CalendarClock, ChartNoAxesCombined, ShieldCheck } from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import {
  MarketPriceChart,
  type ForecastPricePoint,
} from "@/components/charts/market-price-chart"
import { humanize } from "@/lib/formatters"
import type { LosslessInteger } from "@/lib/lossless-integer"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseForecastOutcomes,
  parseForecastVintage,
  type ForecastOutcome,
  type ForecastSummary,
  type ForecastVintage,
} from "./models-contracts"

export function ForecastReview({
  bootstrap,
  transport,
  forecasts,
  selected,
  available,
  loading,
  error,
  completeness,
  select,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  forecasts: ForecastSummary[]
  selected: ForecastSummary | null
  available: boolean
  loading: boolean
  error: string | null
  completeness: string | null
  select: (vintageId: string) => void
}) {
  const operations = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const detailAvailable = operations.has("Model.GetForecast")
  const outcomesAvailable = operations.has("Model.GetForecastOutcomes")
  const detail = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecast",
      { vintageId: selected?.vintageId ?? null },
    ),
    queryFn: async () => {
      if (!selected) throw new Error("No forecast vintage is selected.")
      return parseForecastVintage(
        await transport.query({ query: "forecast", vintageId: selected.vintageId }),
      )
    },
    enabled: detailAvailable && selected !== null,
  })
  const outcomes = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecastOutcomes",
      { vintageId: selected?.vintageId ?? null },
    ),
    queryFn: async () => {
      if (!selected) throw new Error("No forecast vintage is selected.")
      return parseForecastOutcomes(
        await transport.query({
          query: "forecastOutcomes",
          vintageId: selected.vintageId,
        }),
      )
    },
    enabled: outcomesAvailable && selected !== null,
  })

  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-wider text-primary">
            Immutable vintage review
          </p>
          <h2 className="mt-2 text-xl font-semibold">Forecast evidence</h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Statistical forecasts, realized outcomes, targets, and deterministic scenarios retain
            separate semantics.
          </p>
        </div>
        {completeness ? (
          <span className="rounded-full border border-border px-2.5 py-1 text-[10px] uppercase tracking-wider text-muted-foreground">
            {humanize(completeness)} list
          </span>
        ) : null}
      </div>

      {!available ? (
        <Unavailable text="Model.ListForecasts is not registered by this service generation." />
      ) : loading ? (
        <Unavailable text="Loading durable forecast vintages…" />
      ) : error ? (
        <Unavailable text={error} />
      ) : forecasts.length === 0 ? (
        <Unavailable text="No immutable forecast vintage exists for this admitted bundle." />
      ) : (
        <>
          <div className="mt-4 flex gap-2 overflow-x-auto pb-1" aria-label="Forecast vintage selection">
            {forecasts.map((forecast) => (
              <button
                key={forecast.vintageId}
                type="button"
                aria-pressed={selected?.vintageId === forecast.vintageId}
                onClick={() => select(forecast.vintageId)}
                className={`min-w-52 rounded-lg border px-3 py-2 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  selected?.vintageId === forecast.vintageId
                    ? "border-primary/45 bg-primary/10"
                    : "border-border bg-background/25"
                }`}
              >
                <span className="block font-mono text-[10px] text-muted-foreground">
                  {short(forecast.vintageId)}
                </span>
                <span className="mt-1 block text-xs font-medium">
                  {forecast.horizonPoints} × {formatDuration(forecast.horizonStepNanos)}
                </span>
              </button>
            ))}
          </div>
          {selected ? <SummaryEvidence summary={selected} /> : null}
          <ForecastDetail
            summary={selected}
            detail={detail.data ?? null}
            detailAvailable={detailAvailable}
            detailLoading={detail.isPending && selected !== null}
            detailError={detail.isError ? messageFrom(detail.error) : null}
            outcomes={outcomes.data?.outcomes ?? []}
            outcomesAvailable={outcomesAvailable}
            outcomesLoading={outcomes.isPending && selected !== null}
            outcomesError={outcomes.isError ? messageFrom(outcomes.error) : null}
            outcomesTruncated={outcomes.data?.truncated ?? false}
          />
        </>
      )}
    </section>
  )
}

function SummaryEvidence({ summary }: { summary: ForecastSummary }) {
  return (
    <dl className="mt-4 grid gap-x-6 gap-y-3 border-y border-border py-4 sm:grid-cols-2 xl:grid-cols-4">
      <Fact label="Instrument" value={summary.instrumentId} mono />
      <Fact label="Observed through" value={formatUnixNanos(summary.observedThroughUnixNanos)} />
      <Fact label="Created" value={formatUnixNanos(summary.createdAtUnixNanos)} />
      <Fact label="Expires" value={formatUnixNanos(summary.expiresAtUnixNanos)} />
      <Fact label="Quality" value="Modeled · never market evidence" />
      <Fact
        label="Calibrated intervals"
        value={summary.hasCalibratedIntervals ? "Present in vintage" : "Unavailable"}
      />
      <Fact label="Request" value={short(summary.requestHash)} mono />
      <Fact
        label="Controlled artifact"
        value={`${summary.controlledArtifact.byteCount.toLocaleString()} bytes · ${short(summary.controlledArtifact.sha256)}`}
        mono
      />
    </dl>
  )
}

function ForecastDetail({
  summary,
  detail,
  detailAvailable,
  detailLoading,
  detailError,
  outcomes,
  outcomesAvailable,
  outcomesLoading,
  outcomesError,
  outcomesTruncated,
}: {
  summary: ForecastSummary | null
  detail: ForecastVintage | null
  detailAvailable: boolean
  detailLoading: boolean
  detailError: string | null
  outcomes: ForecastOutcome[]
  outcomesAvailable: boolean
  outcomesLoading: boolean
  outcomesError: string | null
  outcomesTruncated: boolean
}) {
  if (!summary) return null
  if (!detailAvailable) {
    return <Unavailable text="Model.GetForecast is not registered, so points and interval bounds cannot be reviewed." />
  }
  if (detailLoading) return <Unavailable text="Loading exact forecast payload…" />
  if (detailError) return <Unavailable text={detailError} />
  if (!detail) return <Unavailable text="No complete forecast payload was returned." />

  const outcomeByTarget = new Map(
    outcomes.map((outcome) => [outcome.targetAtUnixNanos, outcome]),
  )
  const chartPoints = detail.points.map((point) =>
    chartPoint(point, outcomeByTarget.get(point.targetAtUnixNanos)),
  )
  const chartUnavailable =
    "Observed historical prices were not returned by the closed model queries. The modeled path can be reviewed, but Market Squawk will not manufacture pre-cutoff market evidence, targets, or scenarios."

  return (
    <div className="mt-5 space-y-4">
      <MarketPriceChart
        observed={[]}
        forecast={chartPoints}
        cutoffUnixNanos={detail.observedThroughUnixNanos}
        unit={`mantissa × 10^-${detail.points[0]?.decimalScale ?? "?"}`}
        unavailableReason={chartUnavailable}
      />

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MiniFact icon={ChartNoAxesCombined} label="Modeled points" value={detail.points.length.toLocaleString()} />
        <MiniFact icon={CalendarClock} label="Model age at publication" value={formatDuration(detail.modelAgeNanosAtPublication)} />
        <MiniFact icon={CalendarClock} label="Data age at publication" value={formatDuration(detail.dataAgeNanosAtPublication)} />
        <MiniFact
          icon={ShieldCheck}
          label="Outcome evidence"
          value={
            !outcomesAvailable
              ? "Operation unavailable"
              : outcomesLoading
                ? "Loading…"
                : outcomesError
                  ? "Unavailable"
                  : `${outcomes.length}${outcomesTruncated ? "+" : ""} realized`
          }
        />
      </div>

      <CalibrationEvidence vintage={detail} />

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full min-w-[720px] text-left text-xs">
          <caption className="border-b border-border px-3 py-2 text-left text-[10px] uppercase tracking-wider text-muted-foreground">
            Exact decimal payload · statistical forecast only
          </caption>
          <thead className="bg-background/35 text-[10px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Target time</th>
              <th className="px-3 py-2 font-medium">Central</th>
              <th className="px-3 py-2 font-medium">50% interval</th>
              <th className="px-3 py-2 font-medium">80% interval</th>
              <th className="px-3 py-2 font-medium">95% interval</th>
              <th className="px-3 py-2 font-medium">Actual outcome</th>
            </tr>
          </thead>
          <tbody>
            {detail.points.map((point) => {
              const outcome = outcomeByTarget.get(point.targetAtUnixNanos)
              return (
                <tr key={`${point.targetAtUnixNanos}:${point.centralMantissa}`} className="border-t border-border">
                  <td className="px-3 py-2 text-muted-foreground">{formatUnixNanos(point.targetAtUnixNanos)}</td>
                  <td className="px-3 py-2 font-mono">{formatDecimal(point.centralMantissa, point.decimalScale)}</td>
                  <td className="px-3 py-2 font-mono">{formatInterval(point.intervals?.interval50, point.decimalScale)}</td>
                  <td className="px-3 py-2 font-mono">{formatInterval(point.intervals?.interval80, point.decimalScale)}</td>
                  <td className="px-3 py-2 font-mono">{formatInterval(point.intervals?.interval95, point.decimalScale)}</td>
                  <td className="px-3 py-2 font-mono">
                    {outcome ? (
                      <span title={`Quality: ${humanize(outcome.quality)}`}>
                        {formatDecimal(outcome.actualMantissa, outcome.decimalScale)} · {humanize(outcome.quality)}
                      </span>
                    ) : (
                      <span className="font-sans text-muted-foreground">Not observed</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {outcomesError ? <Unavailable text={`Outcomes could not be loaded: ${outcomesError}`} /> : null}
      {!outcomesAvailable ? (
        <Unavailable text="Model.GetForecastOutcomes is not registered; actuals and errors are unavailable." />
      ) : null}
      {detail.limitations.length > 0 ? (
        <div className="rounded-lg border border-amber-400/25 bg-amber-400/5 p-3">
          <p className="flex items-center gap-2 text-xs font-medium text-amber-200">
            <AlertTriangle className="size-3.5" aria-hidden="true" />
            Forecast limitations
          </p>
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {detail.limitations.map((limitation) => <li key={limitation}>{limitation}</li>)}
          </ul>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Failure behavior: {detail.unavailableReason}. No automated action is authorized.
          </p>
        </div>
      ) : null}
      <p className="text-[11px] leading-5 text-muted-foreground">
        No target or deterministic scenario query was requested in this Models view. Neither layer
        is derived from the modeled central path. Observed history also remains unavailable from the
        current desktop market query surface.
      </p>
    </div>
  )
}

function CalibrationEvidence({ vintage }: { vintage: ForecastVintage }) {
  const calibration = vintage.calibration
  if (!calibration) {
    return (
      <Unavailable text="This vintage has no admitted calibration evidence. The chart and table omit uncertainty bands." />
    )
  }
  return (
    <div className="rounded-lg border border-blue-400/25 bg-blue-400/5 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs font-medium text-blue-200">
          {humanize(calibration.method)} calibration · {calibration.observations.toLocaleString()} observations
        </p>
        <p className="font-mono text-[10px] text-muted-foreground">
          policy {short(calibration.policyHash)}
        </p>
      </div>
      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        {([0, 1, 2] as const).map((index) => (
          <div key={index} className="rounded-md border border-border bg-background/25 p-2.5">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {calibration.targetCoverageBasisPoints[index] / 100}% target
            </p>
            <p className="mt-1 font-mono text-sm">
              {calibration.realizedCovered[index].toLocaleString()} / {calibration.realizedTotal[index].toLocaleString()} realized
            </p>
          </div>
        ))}
      </div>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {calibration.coverageInterpretation}. {calibration.dependenceAssumptions}
      </p>
    </div>
  )
}

function chartPoint(
  point: ForecastVintage["points"][number],
  outcome: ForecastOutcome | undefined,
): ForecastPricePoint {
  const scale = point.decimalScale
  return {
    timeUnixNanos: point.targetAtUnixNanos,
    central: decimalNumber(point.centralMantissa, scale),
    interval50: numberInterval(point.intervals?.interval50, scale),
    interval80: numberInterval(point.intervals?.interval80, scale),
    interval95: numberInterval(point.intervals?.interval95, scale),
    actual: outcome ? decimalNumber(outcome.actualMantissa, outcome.decimalScale) : undefined,
  }
}

function numberInterval(
  pair: readonly [string, string] | undefined,
  scale: number,
): [number, number] | undefined {
  return pair
    ? [decimalNumber(pair[0], scale), decimalNumber(pair[1], scale)]
    : undefined
}

function decimalNumber(mantissa: string, scale: number): number {
  return Number(mantissa) / 10 ** scale
}

function formatDecimal(mantissa: string, scale: number): string {
  const negative = mantissa.startsWith("-")
  const digits = negative ? mantissa.slice(1) : mantissa
  if (scale === 0) return mantissa
  const padded = digits.padStart(scale + 1, "0")
  const split = padded.length - scale
  return `${negative ? "-" : ""}${padded.slice(0, split)}.${padded.slice(split)}`
}

function formatInterval(pair: readonly [string, string] | undefined, scale: number): string {
  return pair ? `${formatDecimal(pair[0], scale)} – ${formatDecimal(pair[1], scale)}` : "Unavailable"
}

function Fact({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className={`mt-1 break-words text-xs ${mono ? "font-mono" : ""}`}>{value}</dd>
    </div>
  )
}

function MiniFact({ icon: Icon, label, value }: { icon: typeof CalendarClock; label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-3">
      <Icon className="size-3.5 text-muted-foreground" aria-hidden="true" />
      <p className="mt-2 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-xs font-medium">{value}</p>
    </div>
  )
}

function Unavailable({ text }: { text: string }) {
  return (
    <p className="mt-4 rounded-lg border border-border bg-background/25 p-4 text-sm leading-6 text-muted-foreground">
      {text}
    </p>
  )
}

function short(value: string): string {
  return value.length <= 18 ? value : `${value.slice(0, 10)}…${value.slice(-6)}`
}

function formatUnixNanos(value: LosslessInteger): string {
  return formatTimestamp(value)
}

function formatDuration(value: LosslessInteger): string {
  const nanos = BigInt(value)
  if (nanos < 0n) return "Unavailable"
  const seconds = Number(nanos) / 1_000_000_000
  if (!Number.isFinite(seconds)) return `${value} ns`
  if (seconds < 60) return `${seconds.toLocaleString(undefined, { maximumFractionDigits: 2 })} sec`
  const minutes = seconds / 60
  if (minutes < 60) return `${minutes.toLocaleString(undefined, { maximumFractionDigits: 2 })} min`
  const hours = minutes / 60
  if (hours < 48) return `${hours.toLocaleString(undefined, { maximumFractionDigits: 2 })} hr`
  return `${(hours / 24).toLocaleString(undefined, { maximumFractionDigits: 2 })} days`
}
