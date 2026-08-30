import { useQuery } from "@tanstack/react-query"
import { AlertTriangle, CalendarClock, ChartNoAxesCombined, ShieldCheck } from "lucide-react"

import { productKeys } from "@/app/query-client"
import { humanize } from "@/lib/formatters"
import type { LosslessInteger } from "@/lib/lossless-integer"
import { productCapabilitySet } from "@/lib/product-capabilities"
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
  select,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  forecasts: ForecastSummary[]
  selected: ForecastSummary | null
  available: boolean
  loading: boolean
  error: string | null
  select: (forecastToken: string) => void
}) {
  const capabilities = productCapabilitySet(bootstrap)
  const detailAvailable = capabilities.has("forecast_detail")
  const outcomesAvailable = capabilities.has("forecast_outcomes")
  const detail = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecast",
      { forecastToken: selected?.forecastToken ?? null },
    ),
    queryFn: async () => {
      if (!selected) throw new Error("No forecast is selected.")
      return parseForecastVintage(
        await transport.query({
          query: "forecast",
          forecastToken: selected.forecastToken,
        }),
      )
    },
    enabled: detailAvailable && selected !== null,
  })
  const outcomes = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetForecastOutcomes",
      { forecastToken: selected?.forecastToken ?? null },
    ),
    queryFn: async () => {
      if (!selected) throw new Error("No forecast is selected.")
      return parseForecastOutcomes(
        await transport.query({
          query: "forecastOutcomes",
          forecastToken: selected.forecastToken,
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
            Forecast review
          </p>
          <h2 className="mt-2 text-xl font-semibold">Forecast evidence</h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Compare each forecast with what actually happened, then review its target, range, and
            assumptions.
          </p>
        </div>
      </div>

      {!available ? (
        <Unavailable text="Forecasts are unavailable in this workspace." />
      ) : loading ? (
        <Unavailable text="Loading forecasts…" />
      ) : error ? (
        <Unavailable text="Forecasts cannot be shown right now. Try refreshing the page." />
      ) : forecasts.length === 0 ? (
        <Unavailable text="No forecast is ready for this model yet." />
      ) : (
        <>
          <div className="mt-4 flex gap-2 overflow-x-auto pb-1" aria-label="Forecast selection">
            {forecasts.map((forecast, index) => (
              <button
                key={forecast.forecastToken}
                type="button"
                aria-pressed={selected?.forecastToken === forecast.forecastToken}
                onClick={() => select(forecast.forecastToken)}
                className={`min-w-52 rounded-lg border px-3 py-2 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  selected?.forecastToken === forecast.forecastToken
                    ? "border-primary/45 bg-primary/10"
                    : "border-border bg-background/25"
                }`}
              >
                <span className="block text-[10px] text-muted-foreground">
                  Forecast {index + 1} · {formatUnixNanos(forecast.createdAtUnixNanos)}
                </span>
                <span className="mt-1 block text-xs font-medium">
                  {forecast.horizon.points} × {formatDuration(forecast.horizon.stepNanos)}
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
            detailError={detail.isError ? "Forecast details are unavailable right now." : null}
            outcomes={outcomes.data?.outcomes ?? []}
            outcomesAvailable={outcomesAvailable}
            outcomesLoading={outcomes.isPending && selected !== null}
            outcomesError={outcomes.isError ? "Forecast outcomes are unavailable right now." : null}
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
      <Fact label="Observed through" value={formatUnixNanos(summary.observedThroughUnixNanos)} />
      <Fact label="Created" value={formatUnixNanos(summary.createdAtUnixNanos)} />
      <Fact label="Expires" value={formatUnixNanos(summary.expiresAtUnixNanos)} />
      <Fact
        label="Uncertainty evidence"
        value={summary.evidenceState === "calibrated" ? "Calibrated ranges available" : "Limited"}
      />
      <Fact label="Historical observations" value={summary.historicalObservationCount.toLocaleString()} />
      <Fact label="Use" value="Investment research only" />
      <Fact label="If unavailable" value="No action suggested" />
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
    return <Unavailable text="Forecast details and uncertainty ranges are unavailable." />
  }
  if (detailLoading) return <Unavailable text="Loading forecast evidence…" />
  if (detailError) return <Unavailable text="Forecast details are unavailable right now." />
  if (!detail) return <Unavailable text="No complete forecast was returned." />

  const outcomeByTarget = new Map(
    outcomes.map((outcome) => [outcome.targetAtUnixNanos, outcome]),
  )
  return (
    <div className="mt-5 space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MiniFact icon={ChartNoAxesCombined} label="Forecast points" value={detail.estimates.length.toLocaleString()} />
        <MiniFact icon={CalendarClock} label="Information through" value={formatUnixNanos(detail.observedThroughUnixNanos)} />
        <MiniFact icon={CalendarClock} label="Valid until" value={formatUnixNanos(detail.expiresAtUnixNanos)} />
        <MiniFact
          icon={ShieldCheck}
          label="Outcome evidence"
          value={
            !outcomesAvailable
              ? "Outcome history unavailable"
              : outcomesLoading
                ? "Loading…"
                : outcomesError
                  ? "Unavailable"
                  : `${outcomes.length}${outcomesTruncated ? "+" : ""} realized`
          }
        />
      </div>

      <CalibrationEvidence vintage={detail} />
      <DriftMonitoring vintage={detail} />

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full min-w-[720px] text-left text-xs">
          <caption className="border-b border-border px-3 py-2 text-left text-[10px] uppercase tracking-wider text-muted-foreground">
            Statistical forecast · estimates only
          </caption>
          <thead className="bg-background/35 text-[10px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Target time</th>
              <th className="px-3 py-2 font-medium">Central</th>
              <th className="px-3 py-2 font-medium">Likely range</th>
              <th className="px-3 py-2 font-medium">Wider range</th>
              <th className="px-3 py-2 font-medium">Stress range</th>
              <th className="px-3 py-2 font-medium">Actual outcome</th>
            </tr>
          </thead>
          <tbody>
            {detail.estimates.map((point) => {
              const outcome = outcomeByTarget.get(point.targetAtUnixNanos)
              return (
                <tr key={`${point.targetAtUnixNanos}:${point.central}`} className="border-t border-border">
                  <td className="px-3 py-2 text-muted-foreground">{formatUnixNanos(point.targetAtUnixNanos)}</td>
                  <td className="px-3 py-2 font-mono">{point.central}</td>
                  <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.likely)}</td>
                  <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.wider)}</td>
                  <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.stress)}</td>
                  <td className="px-3 py-2 font-mono">
                    {outcome ? (
                      outcome.actual
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

      {outcomesError ? <Unavailable text="Forecast outcomes are unavailable right now." /> : null}
      {!outcomesAvailable ? (
        <Unavailable text="Actual outcomes and forecast errors are unavailable." />
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
            If required evidence becomes unavailable, Market Squawk suggests no action. No
            automated action is authorized.
          </p>
        </div>
      ) : null}
      <p className="text-[11px] leading-5 text-muted-foreground">
        These are statistical estimates with uncertainty, not guaranteed outcomes. Weigh them
        alongside valuation, risk, and other research before acting.
      </p>
    </div>
  )
}

function DriftMonitoring({ vintage }: { vintage: ForecastVintage }) {
  const monitoring = vintage.outcomeMonitoring
  return (
    <div className="rounded-lg border border-violet-400/25 bg-violet-400/5 p-3">
      <p className="text-xs font-medium text-violet-200">Outcome drift monitoring</p>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Fact label="State" value={humanize(monitoring.state)} />
        <Fact
          label="Observed outcomes"
          value={`${monitoring.includedCount.toLocaleString()}${monitoring.truncated ? "+" : ""} of ${monitoring.observedCount.toLocaleString()}`}
        />
        <Fact label="Mean absolute error" value={monitoring.meanAbsoluteError ?? "Not available"} mono />
      </dl>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {monitoring.interpretation}
      </p>
    </div>
  )
}

function CalibrationEvidence({ vintage }: { vintage: ForecastVintage }) {
  const calibration = vintage.calibration
  if (!calibration) {
    return (
      <Unavailable text="This forecast has no usable calibration history, so uncertainty ranges are unavailable." />
    )
  }
  return (
    <div className="rounded-lg border border-blue-400/25 bg-blue-400/5 p-3">
      <div>
        <p className="text-xs font-medium text-blue-200">
          Calibrated uncertainty · {calibration.observationCount.toLocaleString()} observations
        </p>
      </div>
      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        {calibration.coverage.map((band) => (
          <div key={band.targetCoveragePercent} className="rounded-md border border-border bg-background/25 p-2.5">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {band.targetCoveragePercent}% target
            </p>
            <p className="mt-1 font-mono text-sm">
              {band.realizedCovered.toLocaleString()} / {band.realizedTotal.toLocaleString()} realized
            </p>
          </div>
        ))}
      </div>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {calibration.interpretation}. {calibration.assumptions}
      </p>
    </div>
  )
}

function formatRange(range: { lower: string; upper: string } | undefined): string {
  return range ? `${range.lower} – ${range.upper}` : "Unavailable"
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
