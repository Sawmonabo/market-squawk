import * as React from "react"
import { CircleAlert, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import { investmentDisplayName } from "./portfolio-format"
import { portfolioRebalanceInputSchema } from "./portfolio-contracts"
import type { PortfolioAccountSummary, PortfolioHolding, PortfolioRebalanceReport } from "./portfolio-contracts"
import { usePortfolioPlanningPositions, usePortfolioRebalanceCalculation } from "./use-portfolio"

type RebalanceProps = { account: PortfolioAccountSummary; bootstrap: DesktopBootstrap; transport: ProductTransport }
type DraftTarget = { instrumentId: string; targetPercent: string; investmentLabel: string }
const selectClass = "h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

// The enclosing demand panel owns opening/closing. Refresh replaces the pinned
// read and transient form together, releasing old requests and entered targets.
export function PortfolioRebalance(props: RebalanceProps) {
  const [generation, setGeneration] = React.useState(0)
  return <section className="space-y-4">
    <header>
      <h3 className="text-sm font-semibold">Review a hypothetical rebalance</h3>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">
        Enter your targets and limits for one saved portfolio observation. Results show hypothetical
        investment value changes. This calculation cannot place orders. Transaction costs and live
        execution prices are not included.
      </p>
    </header>
    <RebalanceRead key={`${props.account.accountToken}:${generation}`} {...props} refresh={() => setGeneration((value) => value + 1)} />
  </section>
}

function RebalanceRead({ account, bootstrap, transport, refresh }: RebalanceProps & { refresh: () => void }) {
  const readSession = React.useId()
  const positions = usePortfolioPlanningPositions(transport, bootstrap, account.accountToken, readSession, "rebalance")
  const calculation = usePortfolioRebalanceCalculation(transport, account.accountToken, positions.selection)
  const [targets, setTargets] = React.useState<DraftTarget[]>([])
  const [maxTurnoverPercent, setMaxTurnoverPercent] = React.useState("")
  const [minimumCashAmount, setMinimumCashAmount] = React.useState("")
  const [minimumCashCurrency, setMinimumCashCurrency] = React.useState("")
  const [shortPositions, setShortPositions] = React.useState<"" | "allow" | "exclude">("")
  const [validationError, setValidationError] = React.useState<string | null>(null)
  const page = positions.query.data
  const selection = positions.selection
  const invalidate = () => {
    calculation.invalidate()
    setValidationError(null)
  }
  const changeTarget = (holding: PortfolioHolding, targetPercent: string) => {
    invalidate()
    const entered = { instrumentId: holding.instrumentId, targetPercent,
      investmentLabel: investmentDisplayName(holding.investment, holding.instrumentId) }
    setTargets((current) => targetPercent === ""
      ? current.filter((target) => target.instrumentId !== holding.instrumentId)
      : current.some((target) => target.instrumentId === holding.instrumentId)
        ? current.map((target) => target.instrumentId === holding.instrumentId ? entered : target)
        : [...current, entered])
  }
  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!selection || calculation.pending || !positions.available) return
    const submitted = portfolioRebalanceInputSchema.safeParse({
      targets: targets.map(({ instrumentId, targetPercent }) => ({ instrumentId, targetPercent })),
      maxTurnoverPercent,
      minimumCash: { amount: minimumCashAmount, currency: minimumCashCurrency },
      allowShort: shortPositions === "" ? undefined : shortPositions === "allow",
    })
    if (!submitted.success) {
      calculation.invalidate()
      setValidationError("Enter a percentage for every held investment, a maximum turnover percentage, a cash reserve and its currency, and choose how to handle short positions. Decimal values are accepted.")
      return
    }
    setValidationError(null)
    void calculation.calculate(submitted.data)
  }

  if (!positions.available) return <RebalanceError title="Rebalance planning unavailable"
    detail="A rebalance calculation cannot currently be opened for this portfolio." />

  return <div className="space-y-4" aria-label={`Rebalance planning for ${account.displayName}`}>
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
        Browse investments one page at a time. Entered targets stay with this saved observation.
        Refresh to use the latest observation and clear all inputs and results.
      </p>
      <Button variant="outline" onClick={refresh}><RefreshCw aria-hidden="true" /> Refresh rebalance positions</Button>
    </div>
    {selection ? <dl className="grid gap-3 text-xs sm:grid-cols-2">
      <Fact label="Selected portfolio observation" value={formatUnixNanos(selection.effectiveAtUnixNanos)} />
      <Fact label="Information available" value={selection.availableAtUnixNanos === null ? "Not recorded" : formatUnixNanos(selection.availableAtUnixNanos)} />
    </dl> : null}
      <p className="text-xs leading-5 text-muted-foreground">
        Enter every held investment once. Targets must total 100% of portfolio value, including
        cash in that value. The cash reserve and turnover limit may allow only a partial move
        toward those targets. Market Squawk checks the complete set and totals when you calculate.
      </p>
      {positions.query.isPending ? <Skeleton className="h-32 rounded-xl" aria-label="Loading rebalance positions" />
        : positions.query.isError ? <div>
          <RebalanceError title="Rebalance positions could not be opened"
            detail="Try again for the same saved observation, or refresh rebalance positions to start again." />
          <Button type="button" className="mt-4" onClick={() => void positions.query.refetch()} disabled={positions.query.isFetching}>Try again</Button>
        </div> : page ? <fieldset className="space-y-3 rounded-lg border border-border p-4">
          <legend className="px-1 text-xs font-semibold">Targets for investments on this page</legend>
          {page.holdings.length ? page.holdings.map((holding) => <label key={holding.instrumentId} className="grid gap-1.5 text-xs">
            <span className="font-semibold">Target for {investmentDisplayName(holding.investment, holding.instrumentId)} (%)</span>
            <Input type="text" inputMode="decimal" autoComplete="off"
              value={targets.find((target) => target.instrumentId === holding.instrumentId)?.targetPercent ?? ""}
              disabled={positions.query.isFetching}
              onChange={(event) => changeTarget(holding, event.target.value)} />
          </label>) : <p className="text-xs text-muted-foreground">No held investments are available on this page.</p>}
        </fieldset> : null}
      <CursorNavigation navigation={positions.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={positions.query.isFetching} error={positions.query.isError} onRestart={refresh} />
      {targets.length ? <section aria-label="Entered rebalance targets" className="rounded-lg border border-border/70 p-4">
        <h4 className="text-xs font-semibold">Entered targets across pages</h4>
        <ul className="mt-2 space-y-2 text-xs">{targets.map((target) => <li key={target.instrumentId} className="flex flex-wrap items-center justify-between gap-2">
          <span>{target.investmentLabel}: <span className="font-mono">{target.targetPercent}%</span></span>
          <Button type="button" size="sm" variant="outline" aria-label={`Remove target for ${target.investmentLabel}`} onClick={() => {
            invalidate()
            setTargets((current) => current.filter((item) => item.instrumentId !== target.instrumentId))
          }}>Remove target</Button>
        </li>)}</ul>
      </section> : null}
    <form onSubmit={submit} className="space-y-4" aria-label="Rebalance assumptions">
      {selection ? <>
        <div className="grid gap-4 sm:grid-cols-2">
          <label className="grid gap-1.5 text-xs"><span className="font-semibold">Maximum turnover (%)</span>
            <Input type="text" inputMode="decimal" autoComplete="off" value={maxTurnoverPercent}
              onChange={(event) => { invalidate(); setMaxTurnoverPercent(event.target.value) }} />
          </label>
          <label className="grid gap-1.5 text-xs"><span className="font-semibold">Minimum cash reserve</span>
            <Input type="text" inputMode="decimal" autoComplete="off" value={minimumCashAmount}
              onChange={(event) => { invalidate(); setMinimumCashAmount(event.target.value) }} />
          </label>
          <label className="grid gap-1.5 text-xs"><span className="font-semibold">Reserve currency</span>
            <select className={selectClass} value={minimumCashCurrency} onChange={(event) => { invalidate(); setMinimumCashCurrency(event.target.value) }}>
              <option value="">Choose reserve currency</option><option value={account.currency}>{account.currency}</option>
            </select>
          </label>
          <label className="grid gap-1.5 text-xs"><span className="font-semibold">Short positions</span>
            <select className={selectClass} value={shortPositions} onChange={(event) => { invalidate(); setShortPositions(event.target.value as typeof shortPositions) }}>
              <option value="">Choose how to handle shorts</option>
              <option value="exclude">Require no short positions</option>
              <option value="allow">Allow existing short positions to remain</option>
            </select>
          </label>
        </div>
        <p className="text-xs leading-5 text-muted-foreground">
          Turnover is half the total absolute investment value changes, as a percentage of portfolio
          value. These are value adjustments, not share quantities or executable orders. No costs are estimated.
        </p>
        {validationError ? <p role="alert" className="text-xs text-destructive">{validationError}</p> : null}
        <div className="flex flex-wrap gap-3">
          <Button type="submit" disabled={calculation.pending}>Calculate rebalance</Button>
          {calculation.pending ? <Button type="button" variant="outline" onClick={calculation.cancel}>Cancel calculation</Button> : null}
        </div>
      </> : null}
    </form>
    {calculation.pending ? <p role="status" className="text-xs text-muted-foreground">Calculating your rebalance assumptions…</p> : null}
    {calculation.cancelled ? <p role="status" className="text-xs text-muted-foreground">Rebalance calculation cancelled. No result is shown.</p> : null}
    {calculation.error ? <RebalanceError title="Rebalance calculation could not be completed" detail={calculation.error} /> : null}
    {calculation.result ? <RebalanceResult report={calculation.result} targets={targets} /> : null}
  </div>
}

function RebalanceResult({ report, targets }: { report: PortfolioRebalanceReport; targets: DraftTarget[] }) {
  return <section className="space-y-4 rounded-lg border border-border bg-background/25 p-4" aria-label="Rebalance calculation results">
    <h4 className="text-sm font-semibold">Hypothetical rebalance value changes</h4>
    <p className="text-xs leading-5 text-muted-foreground">
      Selected portfolio observation: {formatUnixNanos(report.effectiveAtUnixNanos)}.
      {" Information available: "}{report.availableAtUnixNanos === null ? "Not recorded" : formatUnixNanos(report.availableAtUnixNanos)}.
      {" Limited data confidence. Transaction costs and live execution prices are not included."}
    </p>
    <dl className="grid gap-3 text-xs sm:grid-cols-3">
      <Fact label="Portfolio value used" value={formatMoney(report.totalValue)} />
      <Fact label="Projected cash" value={formatMoney(report.projectedCash)} />
      <Fact label="Hypothetical turnover" value={`${report.turnoverPercent}%`} />
    </dl>
    <p role="status" className="text-xs leading-5 text-muted-foreground">{report.constrained
      ? "The cash reserve, turnover limit, or decimal precision allows only a partial move toward your targets."
      : "These hypothetical changes meet your entered limits."}</p>
    <section aria-label="Original rebalance assumptions" className="space-y-3 text-xs">
      <h5 className="font-semibold">Original assumptions</h5>
      <dl className="grid gap-3 sm:grid-cols-3">
        <Fact label="Maximum turnover" value={`${report.proposal.maxTurnoverPercent}%`} />
        <Fact label="Cash reserve" value={formatMoney(report.proposal.minimumCash)} />
        <Fact label="Short positions" value={report.proposal.allowShort ? "Existing short positions may remain" : "Require no short positions"} />
      </dl>
      <ul className="space-y-1">{report.proposal.targets.map((target) => {
        const trade = report.trades.find((item) => item.instrumentId === target.instrumentId)
        const label = trade?.investment
          ? investmentDisplayName(trade.investment, target.instrumentId)
          : targets.find((entered) => entered.instrumentId === target.instrumentId)?.investmentLabel
            ?? investmentDisplayName(null, target.instrumentId)
        return <li key={target.instrumentId}>{label}: {target.targetPercent}%</li>
      })}</ul>
    </section>
    {report.trades.length ? <div className="overflow-x-auto"><table className="w-full text-left text-xs">
      <caption className="sr-only">Hypothetical investment value changes</caption>
      <thead className="border-b border-border text-muted-foreground"><tr>
        <th scope="col" className="px-3 py-3 font-medium">Investment</th>
        <th scope="col" className="px-3 py-3 font-medium">Original value</th>
        <th scope="col" className="px-3 py-3 font-medium">Hypothetical change</th>
        <th scope="col" className="px-3 py-3 font-medium">Projected value</th>
      </tr></thead>
      <tbody>{report.trades.map((trade) => <tr key={trade.instrumentId} className="border-b border-border/60">
        <th scope="row" className="px-3 py-3 font-medium">{investmentDisplayName(trade.investment, trade.instrumentId)}</th>
        <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(trade.currentValue)}</td>
        <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(trade.valueChange)}</td>
        <td className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{formatMoney(trade.projectedValue)}</td>
      </tr>)}</tbody>
    </table></div> : <p className="text-xs text-muted-foreground">No investment value changes are proposed within these limits.</p>}
  </section>
}



function Fact({ label, value }: { label: string; value: string }) {
  return <div><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 font-mono tabular-nums">{value}</dd></div>
}

function RebalanceError({ title, detail }: { title: string; detail: string }) {
  return <Alert><CircleAlert aria-hidden="true" /><AlertTitle>{title}</AlertTitle><AlertDescription>{detail}</AlertDescription></Alert>
}
