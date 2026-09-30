import * as React from "react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { formatMoney, groupDecimal } from "@/lib/formatters"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import { productLookupCategory, type ProductTransport } from "@/lib/transport"
import { useLookup, type ProductLookupMatch } from "../lookup/use-lookup"
import { formatUnixNanos } from "../opportunities/format"

import { PlanningSaveControl } from "./planning-save-control"
import { investmentDisplayName } from "./portfolio-format"
import { portfolioCandidateImpactInputSchema } from "./portfolio-contracts"
import type { PortfolioAccountSummary, PortfolioCandidateImpact } from "./portfolio-contracts"
import { formatPortfolioRate } from "./portfolio-format"
import { usePortfolioCandidateImpactCalculation } from "./use-portfolio"

const investmentCategories = [productLookupCategory.investment]
type InvestmentChoice = Extract<ProductLookupMatch, { category: "investment" }>

// The enclosing demand panel releases search and calculation requests on close.
export function PortfolioPositionImpact({ account, bootstrap, transport }: {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const available = hasProductCapability(bootstrap, "portfolio_candidate_impact")
  const lookupAvailable = hasProductCapability(bootstrap, "investment_lookup")
  const [text, setText] = React.useState("")
  const [investment, setInvestment] = React.useState<InvestmentChoice | null>(null)
  const [proposedQuantity, setProposedQuantity] = React.useState("")
  const [scenarioShockPercent, setScenarioShockPercent] = React.useState("")
  const [validationError, setValidationError] = React.useState<string | null>(null)
  const lookup = useLookup(transport, bootstrap.productSessionToken,
    available && lookupAvailable ? text : "", investmentCategories)
  const calculation = usePortfolioCandidateImpactCalculation(transport, account.accountToken)
  const invalidate = () => { calculation.invalidate(); setValidationError(null) }
  const choose = (choice: InvestmentChoice) => {
    invalidate()
    setInvestment(choice)
    setProposedQuantity("")
    setScenarioShockPercent("")
  }
  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!investment || !available || calculation.pending) return
    const input = portfolioCandidateImpactInputSchema.safeParse({
      instrumentId: investment.destination.instrumentId, proposedQuantity, scenarioShockPercent,
    })
    if (!input.success) {
      calculation.invalidate()
      setValidationError("Enter the total quantity you want to hold and a signed price-change percentage. Decimal values are accepted; zero quantity means exit the position.")
      return
    }
    setValidationError(null)
    void calculation.calculate(input.data)
  }

  if (!available || !lookupAvailable) return <ComparisonError title="Position comparison unavailable"
    detail="Investment selection and position comparison must both be available to open this calculation." />

  return <section className="space-y-4" aria-label={`Position comparison for ${account.displayName}`}>
    <p className="text-xs leading-5 text-muted-foreground">
      Choose an investment and the total quantity you would like to hold in {account.displayName}.
      Each calculation checks the latest recorded portfolio and a current price for that investment.
      Enter a price change to compare its hypothetical impact. This cannot place an order.
    </p>
    <label className="grid gap-1.5 text-xs"><span className="font-semibold">Find an investment for comparison</span>
      <Input value={text} autoComplete="off" placeholder="Enter a ticker or investment name" onChange={(event) => {
        invalidate()
        setText(event.target.value)
        setInvestment(null)
        setProposedQuantity("")
        setScenarioShockPercent("")
      }} />
    </label>
    {lookup.status === "idle" ? <p className="text-xs text-muted-foreground">Enter at least two characters to find an investment.</p>
      : lookup.status === "loading" ? <p role="status" className="text-xs text-muted-foreground">Finding investments…</p>
        : lookup.status === "unavailable" ? <ComparisonError title="Investment search unavailable" detail={lookup.message} />
          : <div className="space-y-3" aria-label="Investment matches">
            {lookup.data.matches.filter((match): match is InvestmentChoice => match.category === "investment").map((match) =>
              <Button key={match.destination.instrumentId} type="button" variant="outline"
                className="h-auto w-full justify-start whitespace-normal py-3 text-left"
                aria-label={`Choose ${match.title}`} aria-pressed={investment?.destination.instrumentId === match.destination.instrumentId}
                onClick={() => choose(match)}>
                <span><span className="block">{match.title}</span><span className="mt-1 block text-xs text-muted-foreground">{match.subtitle}</span></span>
              </Button>)}
            {lookup.data.matches.length === 0 ? <p className="text-xs text-muted-foreground">No matching investments. Try a different name or ticker.</p> : null}
            {lookup.data.truncated ? <p className="text-xs text-muted-foreground">More investments match. Narrow your search to find the one you want.</p> : null}
            {lookup.data.categories.some((category) => category.category === "investment" && category.state === "unavailable")
              ? <p role="status" className="text-xs text-muted-foreground">Investment search has incomplete coverage. An investment without current evidence cannot be compared.</p> : null}
          </div>}
    <form onSubmit={submit} className="space-y-4" aria-label="Position comparison assumptions">
      <p className="text-xs font-semibold">{investment ? `Selected investment: ${investment.title}` : "Choose an investment before calculating."}</p>
      <div className="grid gap-4 sm:grid-cols-2">
        <label className="grid gap-1.5 text-xs"><span className="font-semibold">Target total quantity</span>
          <Input type="text" inputMode="decimal" autoComplete="off" value={proposedQuantity} disabled={!investment}
            onChange={(event) => { invalidate(); setProposedQuantity(event.target.value) }} />
        </label>
        <label className="grid gap-1.5 text-xs"><span className="font-semibold">Price change (%)</span>
          <Input type="text" inputMode="decimal" autoComplete="off" value={scenarioShockPercent} disabled={!investment}
            onChange={(event) => { invalidate(); setScenarioShockPercent(event.target.value) }} />
        </label>
      </div>
      <p className="text-xs leading-5 text-muted-foreground">
        Quantity is the total you would hold after the change. Enter zero to exit. Enter a negative
        percentage for a price drop or a positive percentage for a rise. Neither input is assumed.
        Quantity must match the investment’s supported lot size.
      </p>
      <p className="text-xs leading-5 text-muted-foreground">
        The comparison assumes a cash transfer for the position change before fees and slippage.
        Other investments keep their recorded values; only the chosen investment is revalued.
        The price shock affects only this position. Cash availability, settlement, and permission
        to trade are not established by this calculation.
      </p>
      {validationError ? <p role="alert" className="text-xs text-destructive">{validationError}</p> : null}
      <div className="flex flex-wrap gap-3">
        <Button type="submit" disabled={!investment || calculation.pending}>Compare position</Button>
        {calculation.pending ? <Button type="button" variant="outline" onClick={calculation.cancel}>Cancel calculation</Button> : null}
      </div>
    </form>
    {calculation.pending ? <p role="status" className="text-xs text-muted-foreground">Calculating your position comparison…</p> : null}
    {calculation.cancelled ? <p role="status" className="text-xs text-muted-foreground">Position comparison cancelled. No result is shown.</p> : null}
    {calculation.error ? <ComparisonError title="Position comparison could not be completed" detail={calculation.error} /> : null}
    {calculation.result && investment ? <>
      <PortfolioPositionReportView report={calculation.result} investmentLabel={investment.title} />
      <PlanningSaveControl key={calculation.result.calculationToken} accountToken={account.accountToken}
        calculation={calculation.result} kind="position_comparison" bootstrap={bootstrap} transport={transport} />
    </> : null}
  </section>
}

export function PortfolioPositionReportView({ report, investmentLabel = investmentDisplayName(null, report.instrumentId) }: { report: PortfolioCandidateImpact; investmentLabel?: string }) {
  return <section className="space-y-4 rounded-lg border border-border bg-background/25 p-4" aria-label="Position comparison results">
    <h3 className="text-sm font-semibold">{investmentLabel} · {report.positionState === "new" ? "New position" : "Existing position"}</h3>
    <p className="text-xs leading-5 text-muted-foreground">
      Calculated {formatUnixNanos(report.updatedAtUnixNanos)}. This result does not update automatically.
      Compare again to check new portfolio or market evidence.
    </p>
    <dl className="grid gap-3 text-xs sm:grid-cols-2">
      <Fact label="Portfolio observation used" value={formatUnixNanos(report.portfolioEffectiveAtUnixNanos)} />
      <Fact label="Portfolio information available" value={formatUnixNanos(report.portfolioAvailableAtUnixNanos)} />
      <Fact label="Original target total quantity" value={report.assumptions.proposedQuantity} />
      <Fact label="Original price change" value={`${report.assumptions.scenarioShockPercent}%`} />
      <Fact label="Quantity at calculation" value={groupDecimal(report.currentQuantity)} />
      <Fact label="Calculated target quantity" value={groupDecimal(report.proposedQuantity)} />
    </dl>
    <div className="overflow-x-auto"><table className="w-full text-left text-xs">
      <caption className="sr-only">Original and proposed position comparison</caption>
      <thead className="border-b border-border text-muted-foreground"><tr>
        <th scope="col" className="px-3 py-3 font-medium">Measure</th>
        <th scope="col" className="px-3 py-3 font-medium">Original</th>
        <th scope="col" className="px-3 py-3 font-medium">Proposed</th>
        <th scope="col" className="px-3 py-3 font-medium">Change</th>
      </tr></thead>
      <tbody>
        <ComparisonRow label="Position value" current={formatMoney(report.currentMarketValue)} proposed={formatMoney(report.proposedMarketValue)} change={formatMoney(report.capitalChange)} />
        <ComparisonRow label="Portfolio weight" current={formatPortfolioRate(report.concentration.current)} proposed={formatPortfolioRate(report.concentration.proposed)} change={`${formatPortfolioRate(report.concentration.change).slice(0, -1)} percentage points`} />
        <ComparisonRow label="Hypothetical price-shock impact" current={formatMoney(report.scenario.currentImpact)} proposed={formatMoney(report.scenario.proposedImpact)} change={formatMoney(report.scenario.marginalImpact)} />
      </tbody>
    </table></div>
    <dl className="grid gap-3 text-xs sm:grid-cols-2">
      <Fact label="Portfolio value used" value={formatMoney(report.portfolioValue)} />
      <Fact label="Capital change before costs" value={formatMoney(report.capitalChange)} />
      <Fact label="Fee estimate" value={report.costs.fees.state === "available" ? formatMoney(report.costs.fees.amount) : "Not available"} />
      <Fact label="Slippage estimate" value={report.costs.slippage.state === "available" ? formatMoney(report.costs.slippage.amount) : "Not available"} />
    </dl>
    <p className="text-xs leading-5 text-muted-foreground">
      The portfolio value combines recorded values for other investments with the chosen investment
      at the price below. The proposed change assumes a cash transfer before costs. This is a partial
      revaluation, and the scenario covers only the chosen position. Risk assessment remains incomplete;
      this does not establish cash availability, settlement-backed sizing, or permission to trade.
    </p>
    <details className="rounded-lg border border-border/70 p-3">
      <summary className="cursor-pointer text-xs font-semibold">Price and calculation evidence</summary>
      <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2">
        <Fact label="Price used" value={formatMoney(report.price.amount)} />
        <Fact label="Price method" value={report.price.method} />
        <Fact label="Price observation time" value={formatUnixNanos(report.price.asOfUnixNanos)} />
        <Fact label="Price freshness cutoff" value={formatUnixNanos(report.price.freshUntilUnixNanos)} />
        <Fact label="Price confidence" value={report.price.confidence} />
        <Fact label="Supported lot size" value={groupDecimal(report.instrumentTerms.lotSize)} />
        <Fact label="Contract multiplier" value={groupDecimal(report.instrumentTerms.contractMultiplier)} />
        <Fact label="Price tick" value={groupDecimal(report.instrumentTerms.priceTick)} />
        <Fact label="Risk checks completed" value={report.riskAssessment.checksCompleted.toLocaleString()} />
        <Fact label="Risk checks unavailable" value={report.riskAssessment.checksUnavailable.toLocaleString()} />
      </dl>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">The price was checked when calculated. Its freshness cutoff does not guarantee the price remains available now.</p>
      {report.missingInformation.length ? <div className="mt-3 text-xs">
        <h4 className="font-semibold">Missing information</h4>
        <ul className="mt-2 space-y-1 text-muted-foreground">{report.missingInformation.map((item) => <li key={item}>{item}</li>)}</ul>
      </div> : null}
    </details>
  </section>
}

function ComparisonRow({ label, current, proposed, change }: { label: string; current: string; proposed: string; change: string }) {
  return <tr className="border-b border-border/60">
    <th scope="row" className="px-3 py-3 font-medium">{label}</th>
    {[current, proposed, change].map((value, index) => <td key={index} className="whitespace-nowrap px-3 py-3 font-mono tabular-nums">{value}</td>)}
  </tr>
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 break-words font-mono tabular-nums">{value}</dd></div>
}

function ComparisonError({ title, detail }: { title: string; detail: string }) {
  return <Alert><AlertTitle>{title}</AlertTitle><AlertDescription>{detail}</AlertDescription></Alert>
}
