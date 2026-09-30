import * as React from "react"
import { CircleAlert, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney } from "@/lib/formatters"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import { PlanningSaveControl } from "./planning-save-control"
import { investmentDisplayName } from "./portfolio-format"
import { portfolioScenarioInputSchema } from "./portfolio-contracts"
import type { PortfolioAccountSummary, PortfolioHolding, PortfolioScenarioReport, PortfolioScenarioResult } from "./portfolio-contracts"
import { usePortfolioScenarioCalculation, usePortfolioPlanningPositions } from "./use-portfolio"

type ScenarioProps = { account: PortfolioAccountSummary; bootstrap: DesktopBootstrap; transport: ProductTransport }
type DraftShock = { key: number; instrumentId: string; percentChange: string; investmentLabel: string }
type DraftScenario = { key: number; id: string; composition: "" | "additive" | "compounded"; shocks: DraftShock[] }
const selectClass = "h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
const blankScenario = (key: number): DraftScenario => ({ key, id: "", composition: "", shocks: [] })

// The enclosing demand panel mounts this read only while stress tests are open.
export function PortfolioScenarios(props: ScenarioProps) {
  const [generation, setGeneration] = React.useState(0)
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">Test an assumption</p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio stress lab</h2>
        <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
          Apply your explicit price changes to a saved portfolio observation. Results show a
          hypothetical position-value change, not a forecast, probability, or trade.
          Unshocked positions and cash are unchanged. Fees are not included.
        </p>
      </header>
      <StressRead key={generation} {...props} refresh={() => setGeneration((value) => value + 1)} />
    </section>
  )
}

function StressRead({ account, bootstrap, transport, refresh }: ScenarioProps & { refresh: () => void }) {
  const readSession = React.useId()
  const positions = usePortfolioPlanningPositions(transport, bootstrap, account.accountToken, readSession, "scenario")
  const calculation = usePortfolioScenarioCalculation(transport, account.accountToken, positions.selection)
  const singleAvailable = hasProductCapability(bootstrap, "portfolio_scenario")
  const batchAvailable = hasProductCapability(bootstrap, "portfolio_scenario_batch")
  const [mode, setMode] = React.useState<"single" | "batch">("single")
  const [scenarios, setScenarios] = React.useState<DraftScenario[]>([blankScenario(0)])
  const nextKey = React.useRef(1)
  const [validationError, setValidationError] = React.useState<string | null>(null)
  const page = positions.query.data
  const selection = positions.selection
  const invalidate = () => {
    calculation.invalidate()
    setValidationError(null)
  }
  const changeScenario = (changed: DraftScenario) => {
    invalidate()
    setScenarios((current) => current.map((scenario) => scenario.key === changed.key ? changed : scenario))
  }
  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!selection || calculation.pending || !(mode === "batch" ? batchAvailable : singleAvailable)) return
    const submitted = scenarios.map((scenario) => portfolioScenarioInputSchema.safeParse({
      id: scenario.id,
      composition: scenario.composition,
      shocks: scenario.shocks.map(({ instrumentId, percentChange }) => ({ instrumentId, percentChange })),
    }))
    if (submitted.some((scenario) => !scenario.success)) {
      calculation.invalidate()
      setValidationError("Use a scenario name without spaces, choose its composition, and add at least one investment with a percentage, such as -10. Decimal percentages are accepted.")
      return
    }
    const inputs = submitted.map((scenario) => {
      if (!scenario.success) throw new Error("Scenario assumptions are incomplete.")
      return scenario.data
    })
    if (new Set(inputs.map((scenario) => scenario.id)).size !== inputs.length) {
      calculation.invalidate()
      setValidationError("Give each scenario a different name.")
      return
    }
    setValidationError(null)
    void calculation.calculate(inputs, mode === "batch")
  }

  if (!positions.available || (!singleAvailable && !batchAvailable)) {
    return <StressError title="Stress tests unavailable" detail="Stress calculations cannot currently be opened for this portfolio." />
  }

  return (
    <div className="mt-5 space-y-4" aria-label={`Stress tests for ${account.displayName}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
          Browse positions one page at a time. The saved observation stays fixed while you choose
          investments. Refresh to use the latest observation and clear all assumptions and results.
        </p>
        <Button variant="outline" onClick={refresh}><RefreshCw aria-hidden="true" /> Refresh stress positions</Button>
      </div>
      {selection ? <dl className="grid gap-3 text-xs sm:grid-cols-2">
        <div><dt className="text-muted-foreground">Selected portfolio observation</dt>
          <dd className="mt-1">{formatUnixNanos(selection.effectiveAtUnixNanos)}</dd></div>
        <div><dt className="text-muted-foreground">Information available</dt>
          <dd className="mt-1">{selection.availableAtUnixNanos === null ? "Not recorded" : formatUnixNanos(selection.availableAtUnixNanos)}</dd></div>
      </dl> : null}
      {positions.query.isPending ? (
        <Skeleton className="h-32 rounded-xl" aria-label="Loading stress positions" />
      ) : positions.query.isError ? (
        <div>
          <StressError title="Stress positions could not be opened" detail="Try again for the same saved observation, or refresh stress positions to start again." />
          <Button className="mt-4" onClick={() => void positions.query.refetch()} disabled={positions.query.isFetching}>Try again</Button>
        </div>
      ) : page ? (
        <div className="rounded-lg border border-border bg-background/25 p-4">
          <h3 className="text-sm font-semibold">Investments available on this page</h3>
          {page.holdings.length ? <ul className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
            {page.holdings.map((holding) => <li key={holding.instrumentId}>{investmentDisplayName(holding.investment, holding.instrumentId)}</li>)}
          </ul> : <p className="mt-3 text-xs text-muted-foreground">No positions are available on this page.</p>}
          <p className="mt-3 text-xs text-muted-foreground">Choose an investment from this page in a price shock below. Earlier choices stay selected while you browse.</p>
        </div>
      ) : null}
      <CursorNavigation navigation={positions.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={positions.query.isFetching} error={positions.query.isError} onRestart={refresh} />
      {selection ? <form onSubmit={submit} className="space-y-4" aria-label="Stress scenario assumptions">
        <label className="grid gap-1.5 text-xs"><span className="font-semibold">Scenario mode</span>
          <select className={selectClass} value={mode} onChange={(event) => {
            invalidate()
            setMode(event.target.value as "single" | "batch")
            setScenarios([blankScenario(nextKey.current++)])
          }}>
            <option value="single" disabled={!singleAvailable}>Single scenario</option>
            <option value="batch" disabled={!batchAvailable}>Compare scenarios</option>
          </select>
        </label>
        {mode === "single" && !singleAvailable ? <p className="text-xs text-muted-foreground">Choose Compare scenarios to use the available batch calculation.</p> : null}
        {scenarios.map((scenario, index) => <ScenarioFields key={scenario.key} scenario={scenario} index={index}
          holdings={page?.holdings ?? []} positionsReady={!!page && !positions.query.isFetching && !positions.query.isError}
          change={changeScenario} nextKey={() => nextKey.current++}
          remove={mode === "batch" && scenarios.length > 1 ? () => {
            invalidate()
            setScenarios((current) => current.filter((item) => item.key !== scenario.key))
          } : undefined} />)}
        {mode === "batch" ? <Button type="button" variant="outline" onClick={() => {
          invalidate()
          setScenarios((current) => [...current, blankScenario(nextKey.current++)])
        }}>Add scenario</Button> : null}
        {validationError ? <p role="alert" className="text-xs text-destructive">{validationError}</p> : null}
        <div className="flex flex-wrap gap-3">
          <Button type="submit" disabled={calculation.pending || !(mode === "batch" ? batchAvailable : singleAvailable)}>
            {mode === "batch" ? "Calculate scenarios" : "Calculate scenario"}
          </Button>
          {calculation.pending ? <Button type="button" variant="outline" onClick={calculation.cancel}>Cancel calculation</Button> : null}
        </div>
      </form> : null}
      {calculation.pending ? <p role="status" className="text-xs text-muted-foreground">Calculating your selected assumptions…</p> : null}
      {calculation.cancelled ? <p role="status" className="text-xs text-muted-foreground">Stress calculation cancelled. No result is shown.</p> : null}
      {calculation.error ? <StressError title="Stress calculation could not be completed" detail={calculation.error} /> : null}
      {calculation.result ? <>
        <PortfolioScenarioReportView report={calculation.result} />
        <PlanningSaveControl key={calculation.result.calculationToken} accountToken={account.accountToken}
          calculation={calculation.result} kind={"scenario" in calculation.result ? "scenario" : "scenario_batch"}
          bootstrap={bootstrap} transport={transport} />
      </> : null}
    </div>
  )
}

export function PortfolioScenarioReportView({ report }: { report: PortfolioScenarioReport }) {
  return <section className="space-y-4" aria-label="Stress calculation results">
    <h3 className="text-sm font-semibold">Hypothetical position-value change</h3>
    <p className="text-xs leading-5 text-muted-foreground">
      Selected portfolio observation: {formatUnixNanos(report.effectiveAtUnixNanos)}.
      {" Information available: "}{report.availableAtUnixNanos === null ? "Not recorded" : formatUnixNanos(report.availableAtUnixNanos)}.
      {" Limited data confidence. These results apply only to the assumptions shown below. Unshocked positions and cash are unchanged; fees are not included."}
    </p>
    {report.scenarios.map((scenario) => <ScenarioResult key={scenario.id} scenario={scenario} />)}
  </section>
}

function ScenarioFields({ scenario, index, holdings, positionsReady, change, nextKey, remove }: {
  scenario: DraftScenario; index: number; holdings: PortfolioHolding[]; positionsReady: boolean
  change: (scenario: DraftScenario) => void; nextKey: () => number; remove?: () => void
}) {
  const fieldId = React.useId()
  return <fieldset className="space-y-4 rounded-lg border border-border p-4">
    <legend className="px-1 text-sm font-semibold">Scenario {index + 1}</legend>
    <div className="grid gap-4 sm:grid-cols-2">
      <label className="grid gap-1.5 text-xs"><span className="font-semibold">Scenario name</span>
        <Input value={scenario.id} maxLength={512} onChange={(event) => change({ ...scenario, id: event.target.value })} autoComplete="off" />
      </label>
      <label className="grid gap-1.5 text-xs"><span className="font-semibold">Shock composition</span>
        <select className={selectClass} value={scenario.composition} onChange={(event) => change({ ...scenario, composition: event.target.value as DraftScenario["composition"] })}>
          <option value="">Choose composition</option><option value="additive">Additive</option><option value="compounded">Compounded</option>
        </select>
      </label>
    </div>
    <p className="text-xs text-muted-foreground">Use a short name without spaces, such as market-drop. Hyphens are allowed.</p>
    <p className="text-xs leading-5 text-muted-foreground">Additive adds price changes for the same investment. Compounded applies them in sequence. The calculation rejects changes that would produce a negative price.</p>
    {scenario.shocks.map((shock, shockIndex) => <fieldset key={shock.key} className="rounded-md border border-border/70 p-3">
      <legend className="px-1 text-xs font-medium">Price shock {shockIndex + 1}</legend>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="grid gap-1.5 text-xs" htmlFor={`${fieldId}-investment-${shock.key}`}><span className="font-semibold">Shock investment</span>
          <select id={`${fieldId}-investment-${shock.key}`} className={selectClass} value={shock.instrumentId}
            onChange={(event) => {
              const holding = holdings.find((item) => item.instrumentId === event.target.value)
              const changed = { ...shock, instrumentId: event.target.value,
                investmentLabel: holding ? investmentDisplayName(holding.investment, holding.instrumentId) : "" }
              change({ ...scenario, shocks: scenario.shocks.map((item) => item.key === shock.key ? changed : item) })
            }}>
            <option value="">Choose an investment</option>
            {shock.instrumentId && (!positionsReady || !holdings.some((holding) => holding.instrumentId === shock.instrumentId))
              ? <option value={shock.instrumentId}>{shock.investmentLabel}</option> : null}
            {positionsReady ? holdings.map((holding) => <option key={holding.instrumentId} value={holding.instrumentId}>
              {investmentDisplayName(holding.investment, holding.instrumentId)}
            </option>) : null}
          </select>
        </label>
        <label className="grid gap-1.5 text-xs"><span className="font-semibold">Price change (%)</span>
          <Input type="text" inputMode="decimal" autoComplete="off" value={shock.percentChange}
            onChange={(event) => change({ ...scenario, shocks: scenario.shocks.map((item) => item.key === shock.key
              ? { ...item, percentChange: event.target.value } : item) })} />
        </label>
      </div>
      <Button type="button" size="sm" variant="outline" className="mt-3" onClick={() => change({
        ...scenario, shocks: scenario.shocks.filter((item) => item.key !== shock.key),
      })}>Remove price shock</Button>
    </fieldset>)}
    <p className="text-xs text-muted-foreground">Enter a signed percentage: negative for a price drop, positive for a rise. No price change is assumed automatically.</p>
    <div className="flex flex-wrap gap-3">
      <Button type="button" variant="outline" size="sm" onClick={() => change({ ...scenario, shocks: [...scenario.shocks,
        { key: nextKey(), instrumentId: "", percentChange: "", investmentLabel: "" }],
      })}>Add price shock</Button>
      {remove ? <Button type="button" variant="outline" size="sm" onClick={remove}>Remove scenario</Button> : null}
    </div>
  </fieldset>
}

function ScenarioResult({ scenario }: { scenario: PortfolioScenarioResult }) {
  return <section className="space-y-4 rounded-lg border border-border bg-background/25 p-4" aria-label={`Result for ${scenario.id}`}>
    <h4 className="text-sm font-semibold">{scenario.id}</h4>
    <div className="text-xs">
      <p className="font-semibold">Original assumptions</p>
      <p className="mt-1 text-muted-foreground">{scenario.composition === "additive" ? "Additive" : "Compounded"} price shocks</p>
      <ul className="mt-2 space-y-1 text-muted-foreground">{scenario.shocks.map((shock, index) => {
        const contribution = scenario.contributions.find((item) => item.instrumentId === shock.instrumentId)
        return <li key={index}>{investmentDisplayName(contribution?.investment ?? null, shock.instrumentId)}: {shock.percentChange}%</li>
      })}</ul>
    </div>
    <dl className="text-xs"><dt className="text-muted-foreground">Total hypothetical position-value change</dt>
      <dd className="mt-1 font-mono text-lg tabular-nums">{formatMoney(scenario.total)}</dd></dl>
    <div className="overflow-x-auto"><table className="w-full text-left text-xs">
      <caption className="sr-only">Affected investment value changes for {scenario.id}</caption>
      <thead className="border-b border-border text-muted-foreground"><tr>
        <th scope="col" className="px-3 py-3 font-medium">Investment</th>
        <th scope="col" className="px-3 py-3 font-medium">Hypothetical change</th>
      </tr></thead>
      <tbody>{scenario.contributions.map((contribution) => <tr key={contribution.instrumentId} className="border-b border-border/60">
        <th scope="row" className="px-3 py-3 font-medium">{investmentDisplayName(contribution.investment, contribution.instrumentId)}</th>
        <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(contribution.amount)}</td>
      </tr>)}</tbody>
    </table></div>
  </section>
}



function StressError({ title, detail }: { title: string; detail: string }) {
  return <Alert className="mt-4"><CircleAlert aria-hidden="true" /><AlertTitle>{title}</AlertTitle>
    <AlertDescription>{detail}</AlertDescription></Alert>
}
