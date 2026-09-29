import { useState } from "react"
import { Button } from "@/components/ui/button"
import { DemandPanel } from "../shared/demand-panel"
import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"
import { keepPreviousData, useQuery } from "@tanstack/react-query"
import { AlertTriangle, CalendarClock, ChartNoAxesCombined, ShieldCheck } from "lucide-react"

import { MarketPriceChart, type ChartViewport, type ObservedPricePoint } from "@/components/charts/market-price-chart"

import { productKeys } from "@/app/query-client"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  parseForecastChart,
  parseForecastOutcomes,
  parseForecastVintage,
  type ForecastChart,
  type ForecastChartViewportInput,
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
        <Unavailable text="No forecast is ready yet." />
      ) : (
        <>
          <div className="mt-4 flex gap-2 overflow-x-auto pb-1" aria-label="Forecast selection">
            {forecasts.map((forecast) => (
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
                  {investmentLabel(forecast.investment)} · {formatTimestamp(forecast.createdAtUnixNanos)}
                </span>
                <span className="mt-1 block text-xs font-medium">
                  {forecast.target.label} · {forecast.horizon.label}
                </span>
              </button>
            ))}
          </div>
          {selected ? <SummaryEvidence summary={selected} /> : null}
          {selected ? <DemandPanel key={selected.forecastToken} title="Open forecast details and history" className="mt-5 rounded-lg border p-4">
            <ForecastEvidenceRead summary={selected} bootstrap={bootstrap} transport={transport} />
          </DemandPanel> : null}
        </>
      )}
    </section>
  )
}

type OriginalForecastSelection = { kind: "history" | "estimate"; originalOrdinal: string; time?: string; fiscalOrdinal?: number }

function ForecastEvidenceRead({ summary, bootstrap, transport }: {
  summary: ForecastSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [outcomesOpen, setOutcomesOpen] = useState(false)
  const [viewport, setViewport] = useState<ForecastChartViewportInput>({ pointLimit: 512 })
  const [selectedOriginal, setSelectedOriginal] = useState<OriginalForecastSelection | null>(null)
  const [fiscalError, setFiscalError] = useState<string | null>(null)
  const [viewReset, setViewReset] = useState(0)
  const capabilities = productCapabilitySet(bootstrap)
  const detailAvailable = capabilities.has("forecast_detail")
  const outcomesAvailable = capabilities.has("forecast_outcomes")
  const detail = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "Model", "Model.GetForecast", { forecastToken: summary.forecastToken }),
    gcTime: 0,
    queryFn: async ({ signal }) => parseForecastVintage(await transport.query({ query: "forecast", forecastToken: summary.forecastToken }, { signal })),
    enabled: detailAvailable,
  })
  const chart = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "Model", "Model.GetForecastChart", { forecastToken: summary.forecastToken, ...viewport }),
    gcTime: 0,
    placeholderData: keepPreviousData,
    enabled: detailAvailable,
    queryFn: async ({ signal }) => parseForecastChart(await transport.query({ query: "forecastChart", forecastToken: summary.forecastToken, ...viewport }, { signal }), summary.forecastToken, viewport),
  })
  const onViewportChange = (next: ChartViewport) => {
    if (viewport.startUnixNanos === undefined && next.fromUnixNanos === chart.data?.viewport.fullStartUnixNanos
      && next.throughUnixNanos === chart.data?.viewport.fullEndUnixNanos) return
    setViewport({ startUnixNanos: next.fromUnixNanos, endUnixNanos: next.throughUnixNanos, pointLimit: next.pointLimit })
    setSelectedOriginal(null)
  }
  const onObservationSelect = (point: ObservedPricePoint) => {
    if (point.originalOrdinal !== undefined) setSelectedOriginal({ kind: "history", time: String(point.timeUnixNanos), originalOrdinal: point.originalOrdinal })
  }
  const onEstimateSelect = (point: ForecastChart["estimates"][number]) => setSelectedOriginal({ kind: "estimate", originalOrdinal: point.originalOrdinal,
    ...(point.targetAtUnixNanos === null ? { fiscalOrdinal: point.financialTarget!.ordinal } : { time: point.targetAtUnixNanos }) })
  const detailProps = {
    summary, detail: detail.data ?? null, detailAvailable,
    chart: chart.data ?? null, chartLoading: chart.isPending, chartError: chart.isError,
    onViewportChange, onObservationSelect, onEstimateSelect, viewReset,
    detailLoading: detail.isPending,
    detailError: detail.isError ? "Forecast details are unavailable right now." : null,
  }
  return <>
    <Button size="sm" variant="outline" onClick={() => { setViewport({ pointLimit: 512 }); setSelectedOriginal(null); setFiscalError(null); setViewReset((current) => current + 1) }}>Reset saved forecast view</Button>
    {chart.data?.coordinateKind === "fiscal_period" ? <form className="mt-3 flex flex-wrap items-end gap-3 text-xs" onSubmit={(event) => {
      event.preventDefault()
      const fields = new FormData(event.currentTarget)
      const start = Number(fields.get("start")), end = Number(fields.get("end"))
      if (!Number.isInteger(start) || !Number.isInteger(end) || start < 1 || end < start || end > 4_294_967_295) { setFiscalError("Choose ordered reporting periods."); return }
      setFiscalError(null); setSelectedOriginal(null); setViewport({ startFiscalOrdinal: start, endFiscalOrdinal: end, pointLimit: 512 })
    }}>
      <label className="grid gap-1">First reporting period<input className="rounded border bg-background p-2" name="start" type="number" min={1} defaultValue={chart.data.viewport.fullStartFiscalOrdinal ?? 1} required /></label>
      <label className="grid gap-1">Last reporting period<input className="rounded border bg-background p-2" name="end" type="number" min={1} defaultValue={chart.data.viewport.fullEndFiscalOrdinal ?? 1} required /></label>
      <Button size="sm" variant="outline" type="submit">Open period window</Button>
      {fiscalError ? <p role="alert" className="text-destructive">{fiscalError}</p> : null}
    </form> : null}
    {chart.isFetching ? <p role="status" className="mt-3 text-xs text-muted-foreground">Loading the requested forecast window…</p> : null}
    {chart.isError || detail.isError ? <Button size="sm" variant="outline" onClick={() => { if (chart.isError) void chart.refetch(); if (detail.isError) void detail.refetch() }}>Retry forecast evidence</Button> : null}
    {outcomesAvailable ? <Button size="sm" variant="outline" onClick={() => setOutcomesOpen((current) => !current)} aria-expanded={outcomesOpen}>
      {outcomesOpen ? "Close actual outcomes" : "Open actual outcomes"}
    </Button> : null}
    {outcomesOpen ? <ForecastOutcomeRead {...detailProps} bootstrap={bootstrap} transport={transport} />
      : <ForecastDetail {...detailProps} outcomes={[]} outcomesAvailable={outcomesAvailable} outcomesRequested={false} outcomesLoading={false} outcomesError={null} outcomesTruncated={false} />}
    {selectedOriginal !== null ? <OriginalForecastPointRead key={`${selectedOriginal.kind}:${selectedOriginal.originalOrdinal}`} point={selectedOriginal} forecastToken={summary.forecastToken} bootstrap={bootstrap} transport={transport} /> : null}
  </>
}

function OriginalForecastPointRead({ point, forecastToken, bootstrap, transport }: {
  point: OriginalForecastSelection; forecastToken: string; bootstrap: DesktopBootstrap; transport: ProductTransport
}) {
  const viewport: ForecastChartViewportInput = point.time !== undefined ? { startUnixNanos: point.time, endUnixNanos: point.time, pointLimit: 8 }
    : { startFiscalOrdinal: point.fiscalOrdinal, endFiscalOrdinal: point.fiscalOrdinal, pointLimit: 8 }
  const original = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "Model", "Model.GetForecastChart", { forecastToken, ...viewport }),
    gcTime: 0,
    queryFn: async ({ signal }) => {
      const chart = parseForecastChart(await transport.query({ query: "forecastChart", forecastToken, ...viewport }, { signal }), forecastToken, viewport)
      const row = point.kind === "history" ? chart.observedHistory.find((entry) => entry.originalOrdinal === point.originalOrdinal)
        : chart.estimates.find((entry) => entry.originalOrdinal === point.originalOrdinal)
      if (!row) throw new Error("The original forecast evidence could not be verified.")
      return row
    },
  })
  return <div className="mt-4 rounded-lg border p-3 text-xs"><p className="font-medium">Exact original forecast evidence</p>
    {original.isPending ? <p role="status" className="mt-2 text-muted-foreground">Checking saved evidence…</p>
      : original.isError ? <p role="alert" className="mt-2 text-destructive">The saved evidence could not be verified. <Button size="xs" variant="outline" onClick={() => void original.refetch()}>Retry</Button></p>
        : "value" in original.data ? <p className="mt-2 font-mono">{original.data.value.formatted}</p>
          : <dl className="mt-2 grid gap-2 sm:grid-cols-4"><Fact label="Central" value={original.data.central.formatted} mono /><Fact label="Likely range" value={formatRange(original.data.ranges?.likely)} mono /><Fact label="Wider range" value={formatRange(original.data.ranges?.wider)} mono /><Fact label="Stress range" value={formatRange(original.data.ranges?.stress)} mono /></dl>}
  </div>
}

function ForecastOutcomeRead({ bootstrap, transport, ...detailProps }: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
} & Pick<Parameters<typeof ForecastDetail>[0], "summary" | "detail" | "detailAvailable" | "detailLoading" | "detailError" | "chart" | "chartLoading" | "chartError" | "onViewportChange" | "onObservationSelect" | "onEstimateSelect" | "viewReset">) {
  const forecastToken = detailProps.summary!.forecastToken
  const navigation = useCursorNavigation()
  const outcomes = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "Model", "Model.GetForecastOutcomes", { forecastToken, cursor: navigation.after, limit: 25 }),
    gcTime: 0,
    queryFn: async ({ signal }) => parseForecastOutcomes(await transport.query({ query: "forecastOutcomes", forecastToken, cursor: navigation.after, limit: 25 }, { signal }), forecastToken),
  })
  return <>
    <CursorNavigation navigation={navigation} next={outcomes.data?.nextCursor} busy={outcomes.isFetching} error={outcomes.isError} onRestart={() => { if (navigation.after === undefined) void outcomes.refetch() }} />
    <ForecastDetail {...detailProps}
    outcomes={outcomes.data?.forecastToken === forecastToken ? outcomes.data.outcomes : []}
    outcomesAvailable outcomesRequested
    outcomesLoading={outcomes.isPending}
    outcomesError={outcomes.isError ? "Forecast outcomes are unavailable right now." : null}
    outcomesTruncated={outcomes.data?.nextCursor !== null && outcomes.data?.nextCursor !== undefined} />
  </>
}

function SummaryEvidence({ summary }: { summary: ForecastSummary }) {
  return (
    <dl className="mt-4 grid gap-x-6 gap-y-3 border-y border-border py-4 sm:grid-cols-2 xl:grid-cols-4">
      <Fact label="Investment" value={investmentLabel(summary.investment)} />
      <Fact label="Forecast target" value={summary.target.label} />
      <Fact label="Target type" value={targetKindLabel(summary.target.valueKind)} />
      <Fact label="Target unit" value={summary.target.unitLabel} />
      {summary.target.currencyCode ? (
        <Fact label="Currency" value={summary.target.currencyCode} />
      ) : null}
      <Fact label="Horizon" value={summary.horizon.label} />
      <Fact label="Observed through" value={formatObservedThrough(summary.observedThroughUnixNanos)} />
      <Fact label="Created" value={formatTimestamp(summary.createdAtUnixNanos)} />
      <Fact label="Expires" value={formatTimestamp(summary.expiresAtUnixNanos)} />
      <Fact
        label="Overall model evidence"
        value={evidenceLevelLabel(summary.modelEvidence.overall)}
      />
      <Fact label="Point-in-time inputs" value={evidenceLevelLabel(summary.modelEvidence.pitInputs)} />
      <Fact label="Held-out evaluation" value={evidenceLevelLabel(summary.modelEvidence.outOfSample)} />
      <Fact label="Horizon alignment" value={evidenceLevelLabel(summary.modelEvidence.horizonAlignment)} />
      <Fact label="Calibration" value={calibrationStateLabel(summary.modelEvidence.calibration, summary.target.valueKind === "probability")} />
      <Fact label="Evidence meaning" value={summary.modelEvidence.interpretation} />
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
  chart, chartLoading, chartError, onViewportChange, onObservationSelect, onEstimateSelect, viewReset,
  outcomes,
  outcomesAvailable,
  outcomesRequested,
  outcomesLoading,
  outcomesError,
  outcomesTruncated,
}: {
  summary: ForecastSummary | null
  detail: ForecastVintage | null
  detailAvailable: boolean
  detailLoading: boolean
  detailError: string | null
  chart: ForecastChart | null
  chartLoading: boolean
  chartError: boolean
  onViewportChange: (viewport: ChartViewport) => void
  onObservationSelect: (point: ObservedPricePoint) => void
  onEstimateSelect: (point: ForecastChart["estimates"][number]) => void
  viewReset: number
  outcomes: ForecastOutcome[]
  outcomesAvailable: boolean
  outcomesRequested: boolean
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
  if (detail.forecastToken !== summary.forecastToken) {
    return <Unavailable text="The selected forecast could not be verified." />
  }
  if (!sameModelEvidence(detail.modelEvidence, summary.modelEvidence)) {
    return <Unavailable text="The selected forecast evidence could not be verified." />
  }

  if (chart !== null && (chart.target.valueKind !== detail.target.valueKind || chart.target.label !== detail.target.label
    || chart.target.currencyCode !== detail.target.currencyCode || chart.target.unitLabel !== detail.target.unitLabel
    || chart.observedThroughUnixNanos !== detail.observedThroughUnixNanos)) {
    return <Unavailable text="The forecast chart target and information cutoff could not be verified." />
  }

  const estimates = chart?.estimates ?? []
  const observedHistory = chart?.observedHistory ?? []
  const outcomeByTarget = new Map(
    outcomes.map((outcome) => [outcome.targetAtUnixNanos, outcome]),
  )
  return (
    <div className="mt-5 space-y-4">
      <div className="rounded-lg border border-primary/20 bg-primary/[0.04] p-4">
        <p className="text-sm font-semibold">
          {investmentLabel(detail.investment)} · {detail.target.label}
        </p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          {detail.investment.description}
        </p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          {detail.target.meaning} This is a{" "}
          {targetKindLabel(detail.target.valueKind).toLowerCase()} forecast.
          Values are shown in {detail.target.unitLabel}
          {detail.target.currencyCode ? ` (${detail.target.currencyCode})` : ""}.
        </p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          {detail.horizon.description}
        </p>
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MiniFact icon={ChartNoAxesCombined} label="Forecast points" value={chartLoading ? "Loading…" : chartError ? "Unavailable" : estimates.length.toLocaleString()} />
        <MiniFact icon={CalendarClock} label="Information through" value={formatObservedThrough(detail.observedThroughUnixNanos)} />
        <MiniFact icon={CalendarClock} label="Valid until" value={formatTimestamp(detail.expiresAtUnixNanos)} />
        <MiniFact
          icon={ShieldCheck}
          label="Outcome evidence"
          value={
            !outcomesRequested
              ? "Open actual outcomes to compare"
              : !outcomesAvailable
              ? "Outcome history unavailable"
              : outcomesLoading
                ? "Loading…"
                : outcomesError
                  ? "Unavailable"
                  : `${outcomes.length} on this outcome page${outcomesTruncated ? "; more pages available" : ""}`
          }
        />
      </div>

      {chartLoading ? <Unavailable text="Loading the requested forecast chart…" /> : chartError ? <Unavailable text="The saved forecast chart is unavailable." /> : detail.target.valueKind === "probability" ? <ProbabilityEventEvidence vintage={detail} estimates={estimates} />
        : detail.target.valueKind !== "financial_amount" ? <MarketPriceChart
        key={`${detail.forecastToken}:${viewReset}`}
        title={`${detail.target.label}: history and forecast`}
        viewportBounds={chart?.viewport.fullStartUnixNanos !== null && chart?.viewport.fullStartUnixNanos !== undefined && chart.viewport.fullEndUnixNanos !== null
          ? { fromUnixNanos: chart.viewport.fullStartUnixNanos, throughUnixNanos: chart.viewport.fullEndUnixNanos } : undefined}
        onViewportChange={onViewportChange}
        onObservationSelect={onObservationSelect}
        displayResolution={chart?.display}
        viewportPointLimit={512}
        unit={detail.target.currencyCode ?? detail.target.unitLabel}
        cutoffUnixNanos={detail.observedThroughUnixNanos}
        observed={observedHistory.map((point) => ({
          timeUnixNanos: point.observedAtUnixNanos,
          value: point.value.exact,
          quality: "Recorded forecast input", originalOrdinal: point.originalOrdinal, breakBefore: point.breakBefore[0],
        }))}
        forecast={estimates.flatMap((point) => {
          if (point.targetAtUnixNanos === null) return []
          const outcome = outcomeByTarget.get(point.targetAtUnixNanos)
          return [{
            timeUnixNanos: point.targetAtUnixNanos,
            central: point.central.exact,
            ...(detail.calibration && point.ranges ? {
              interval50: [point.ranges.likely.lower.exact, point.ranges.likely.upper.exact] as const,
              interval80: [point.ranges.wider.lower.exact, point.ranges.wider.upper.exact] as const,
              interval95: [point.ranges.stress.lower.exact, point.ranges.stress.upper.exact] as const,
            } : {}),
            ...(outcome ? { actual: outcome.actual.exact } : {}),
          }]
        })}
      /> : <Unavailable text="This forecast uses financial reporting periods. Exact period estimates are shown below; no daily price path is implied." />}

      <CalibrationEvidence vintage={detail} />
      <DriftMonitoring vintage={detail} />

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full min-w-[720px] text-left text-xs">
          <caption className="border-b border-border px-3 py-2 text-left text-[10px] uppercase tracking-wider text-muted-foreground">
            {detail.target.label} · {detail.target.unitLabel} · estimates only
          </caption>
          <thead className="bg-background/35 text-[10px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Target time or reporting period</th>
              <th className="px-3 py-2 font-medium">{detail.target.valueKind === "probability" ? "Estimated probability" : "Central"}</th>
              {detail.target.valueKind !== "probability" ? <>
                <th className="px-3 py-2 font-medium">Likely range</th>
                <th className="px-3 py-2 font-medium">Wider range</th>
                <th className="px-3 py-2 font-medium">Stress range</th>
              </> : null}
              <th className="px-3 py-2 font-medium">Actual outcome</th>
            </tr>
          </thead>
          <tbody>
            {estimates.map((point, index) => {
              const outcome = point.targetAtUnixNanos === null ? undefined : outcomeByTarget.get(point.targetAtUnixNanos)
              return (
                <tr key={`${point.targetAtUnixNanos ?? point.financialTarget?.ordinal}:${index}`} className="border-t border-border">
                  <td className="px-3 py-2 text-muted-foreground"><button type="button" className="text-left underline" onClick={() => onEstimateSelect(point)}>{formatForecastCoordinate(point)}</button></td>
                  <td className="px-3 py-2 font-mono">{point.central.formatted}</td>
                  {detail.target.valueKind !== "probability" ? <>
                    <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.likely)}</td>
                    <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.wider)}</td>
                    <td className="px-3 py-2 font-mono">{formatRange(point.ranges?.stress)}</td>
                  </> : null}
                  <td className="px-3 py-2 font-mono">
                    {outcome ? (
                      outcome.actual.formatted
                    ) : (
                      <span className="font-sans text-muted-foreground">{!outcomesRequested ? "Open actual outcomes" : outcomesLoading ? "Loading…" : outcomesError ? "Unavailable" : "Not returned on this outcome page"}</span>
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
        <Fact label="State" value={monitoringStateLabel(monitoring.state)} />
        <Fact
          label="Observed outcomes"
          value={`${monitoring.includedCount.toLocaleString()}${monitoring.truncated ? "+" : ""} of ${monitoring.observedCount.toLocaleString()}`}
        />
        <Fact
          label="Mean absolute error"
          value={monitoring.meanAbsoluteError?.value.formatted ?? "Not available"}
          mono
        />
        <Fact
          label="Mean calculation"
          value={roundingLabel(monitoring.meanAbsoluteError?.rounding ?? null)}
        />
      </dl>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {monitoring.interpretation}
      </p>
    </div>
  )
}

function CalibrationEvidence({ vintage }: { vintage: ForecastVintage }) {
  if (vintage.target.valueKind === "probability") return <ProbabilityCalibrationEvidence vintage={vintage} />
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
          <div key={band.targetCoveragePercent.exact} className="rounded-md border border-border bg-background/25 p-2.5">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {band.targetCoveragePercent.formatted} target
            </p>
            <p className="mt-1 font-mono text-sm">
              Fitted interval target
            </p>
          </div>
        ))}
      </div>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        Calibration window: {formatCalibrationWindow(calibration.window)}.
      </p>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {calibration.interpretation}. {calibration.assumptions}
      </p>
    </div>
  )
}

function formatRange(
  range:
    | {
        lower: { exact: string; formatted: string }
        upper: { exact: string; formatted: string }
      }
    | undefined,
): string {
  return range
    ? `${range.lower.formatted} – ${range.upper.formatted}`
    : "Unavailable"
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

function investmentLabel(investment: ForecastSummary["investment"]): string {
  return investment.symbol
    ? `${investment.name} (${investment.symbol})`
    : investment.name
}

function monitoringStateLabel(
  state: ForecastVintage["outcomeMonitoring"]["state"],
): string {
  return state === "outcomes_available"
    ? "Observed outcomes available"
    : "Waiting for outcomes"
}

function evidenceLevelLabel(
  state: ForecastSummary["modelEvidence"]["overall"],
): string {
  switch (state) {
    case "sufficient":
      return "Sufficient for this research use"
    case "limited":
      return "Limited"
    case "unavailable":
      return "Unavailable"
  }
}

function calibrationStateLabel(
  state: ForecastSummary["modelEvidence"]["calibration"],
  probability = false,
): string {
  switch (state) {
    case "calibrated":
      return probability ? "Probability calibration available" : "Calibrated ranges available"
    case "limited":
      return probability ? "Limited probability evidence" : "Limited; ranges may be unavailable"
    case "unavailable":
      return "Unavailable"
  }
}

function roundingLabel(
  rounding:
    | NonNullable<
        ForecastVintage["outcomeMonitoring"]["meanAbsoluteError"]
      >["rounding"]
    | null,
): string {
  if (!rounding) return "Not available"
  return rounding.state === "exact"
    ? `Exact at ${rounding.decimalPlaces.toLocaleString()} decimal places · half-even policy`
    : `Rounded to ${rounding.decimalPlaces.toLocaleString()} decimal places · half-even`
}

function sameModelEvidence(
  left: ForecastVintage["modelEvidence"],
  right: ForecastSummary["modelEvidence"],
): boolean {
  return (
    left.modelToken === right.modelToken &&
    left.overall === right.overall &&
    left.pitInputs === right.pitInputs &&
    left.outOfSample === right.outOfSample &&
    left.horizonAlignment === right.horizonAlignment &&
    left.calibration === right.calibration &&
    left.interpretation === right.interpretation
  )
}

function targetKindLabel(
  valueKind: ForecastSummary["target"]["valueKind"],
): string {
  switch (valueKind) {
    case "market_price":
      return "Market price"
    case "financial_amount":
      return "Financial reporting amount"
    case "percentage_return":
      return "Percentage return"
    case "probability":
      return "Probability"
  }
}

function formatObservedThrough(value: string | null): string {
  return value === null ? "Reporting period; no exact observation time" : formatTimestamp(value)
}
function formatCalendarDate(value: { year: number; month: number; day: number }): string {
  return `${String(value.year).padStart(4, "0")}-${String(value.month).padStart(2, "0")}-${String(value.day).padStart(2, "0")}`
}
function formatForecastCoordinate(point: ForecastChart["estimates"][number]): string {
  if (point.targetAtUnixNanos !== null) return formatTimestamp(point.targetAtUnixNanos)
  const target = point.financialTarget
  if (!target) return "Reporting period unavailable"
  const period = target.period
  if (!period) return `Reporting period ${target.ordinal}; exact dates unavailable`
  return period.kind === "instant" ? formatCalendarDate(period.instant)
    : `${formatCalendarDate(period.start)} – ${formatCalendarDate(period.end)}`
}
function formatCalibrationWindow(window: NonNullable<ForecastVintage["calibration"]>["window"]): string {
  return window.kind === "exact_time" ? `${formatTimestamp(window.start)} – ${formatTimestamp(window.end)}`
    : `${formatCalendarDate(window.start)} – ${formatCalendarDate(window.end)} (reporting dates)`
}

function ProbabilityEventEvidence({ vintage, estimates }: { vintage: ForecastVintage; estimates: ForecastChart["estimates"] }) {
  const event = vintage.target.event
  if (!event) return <Unavailable text="The event definition is unavailable." />
  const definition = event.definition
  const title = definition.kind === "price_higher" ? "Chance of a higher price"
    : definition.kind === "benchmark_outperformance" ? "Chance of beating the selected benchmark"
      : "Chance of profit after modeled trading costs"
  return <section className="rounded-lg border border-primary/25 bg-primary/5 p-4">
    <h3 className="text-sm font-semibold">{title}</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">{vintage.target.meaning}</p>
    <dl className="mt-4 grid gap-3 sm:grid-cols-2">
      {estimates.map((point, index) => <Fact key={index} label={formatForecastCoordinate(point)} value={point.central.formatted} mono />)}
    </dl>
    <p className="mt-3 text-xs leading-5 text-muted-foreground">This is the chance of the named event over {vintage.horizon.label.toLowerCase()}. It is separate from expected percentage gain or a future price range.</p>
    <details className="mt-4 rounded-md border border-border bg-background/25 p-3 text-xs">
      <summary className="cursor-pointer font-medium">Saved event assumptions</summary>
      <dl className="mt-3 grid gap-3 sm:grid-cols-2">
        <Fact label="Starting observation" value={event.originBasis === "completed_bar_close" ? "Completed price bar"
          : event.originBasis === "named_session_close_for_nominal_daily_bar" ? "Recorded close of the named trading session" : "Exact recorded observation"} />
        <Fact label="Horizon" value={vintage.horizon.label} />
        {definition.kind === "benchmark_outperformance" ? <Fact label="Selected benchmark identifier" value={definition.benchmarkInstrumentId} /> : null}
        {definition.kind === "profit_after_costs" ? <>
          <Fact label="Trade size" value={`${definition.policy.quantity_lots} lots`} />
          <Fact label="Reporting currency" value={definition.policy.reporting_currency} />
          <Fact label="Fee per fill" value={`${definition.policy.fee_basis_points} basis points`} />
          <Fact label="Modeled slippage" value={`${definition.policy.slippage_basis_points} basis points`} />
          <Fact label="Maximum additional slippage" value={`${definition.policy.maximum_random_slippage_basis_points} basis points`} />
          <Fact label="Maximum trading participation" value={`${definition.policy.maximum_participation_basis_points} basis points`} />
          <Fact label="Partial fills" value={definition.policy.allow_partial_fills ? "Allowed" : "Not allowed"} />
          <Fact label="Execution evidence" value={definition.policy.execution_basis === "completed_daily_bar" ? "Completed daily bars" : "Observed quotes and depth"} />
          {definition.policy.daily_bar_assumed_spread_basis_points !== null ? <Fact label="Assumed daily-bar spread" value={`${definition.policy.daily_bar_assumed_spread_basis_points} basis points`} /> : null}
          <Fact label="Profit definition" value="Positive total wealth from the long round trip after modeled costs, including distributions and unpaid entitlements." />
        </> : null}
      </dl>
    </details>
  </section>
}
function ProbabilityCalibrationEvidence({ vintage }: { vintage: ForecastVintage }) {
  const calibration = vintage.probabilityCalibration
  if (!calibration) return <Unavailable text="Probability calibration evidence is unavailable for this forecast." />
  return <section className="rounded-lg border border-blue-400/25 bg-blue-400/5 p-4">
    <h3 className="text-sm font-semibold text-blue-200">Probability calibration and held-out outcomes</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">These results evaluate predictions against completed events on a separate chronological evaluation period. They are not price ranges or a guarantee that this event will occur.</p>
    <dl className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
      {([['Training', calibration.trainWindow], ['Calibration', calibration.calibrationWindow], ['Held-out evaluation', calibration.evaluationWindow]] as const).map(([label, window]) => <Fact key={label} label={label}
        value={`${formatTimestamp(window.start)} – ${formatTimestamp(window.end)} · ${window.observationCount.toLocaleString()} observations`} />)}
      <Fact label="Brier score" value={String(calibration.brierScore)} mono />
      <Fact label="Log loss" value={String(calibration.logLoss)} mono />
    </dl>
    <p className="mt-3 text-xs leading-5 text-muted-foreground">Brier score and log loss measure prediction error; lower values indicate less error on these held-out outcomes.</p>
    <details className="mt-4 rounded-md border border-border bg-background/25 p-3">
      <summary className="cursor-pointer text-xs font-medium">Reliability by prediction group</summary>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-left text-xs">
          <caption className="mb-2 text-left text-muted-foreground">Original evaluation groups; empty groups remain unavailable.</caption>
          <thead><tr><th className="p-2 font-medium">Group</th><th className="p-2 font-medium">Outcomes</th><th className="p-2 font-medium">Mean predicted probability</th><th className="p-2 font-medium">Observed event frequency</th></tr></thead>
          <tbody>{calibration.reliabilityBins.map((bin, index) => <tr key={index} className="border-t border-border">
            <td className="p-2">{index + 1}</td><td className="p-2">{bin.observationCount.toLocaleString()}</td>
            <td className="p-2 font-mono">{bin.meanProbability === null ? "Unavailable" : String(bin.meanProbability)}</td>
            <td className="p-2 font-mono">{bin.observedFrequency === null ? "Unavailable" : String(bin.observedFrequency)}</td>
          </tr>)}</tbody>
        </table>
      </div>
      <p className="mt-3 text-xs text-muted-foreground">Probabilities and frequencies in this table use the 0–1 scale.</p>
    </details>
  </section>
}
