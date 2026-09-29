import { formatMoney as money, formatUnixNanos } from "../opportunities/format"
import type { InvestmentAnalysis } from "../opportunities/contracts"

type MethodSet = NonNullable<InvestmentAnalysis["priceSummary"]["valuationMethods"]>
type Method = MethodSet["methods"][number]
type CalculatedMethod = Extract<Method, { status: "calculated" }>

const methodLabels: Record<Method["method"], string> = {
  discounted_cash_flow: "Discounted cash flow",
  comparable_companies: "Comparable companies",
  residual_income: "Residual income",
  forecast_distribution: "Forecast distribution",
}
const basisLabels: Record<CalculatedMethod["basis"], string> = {
  per_instrument_unit: "Per instrument unit",
  total_common_equity: "Total common equity",
  reporting_entity_total: "Reporting entity total",
  position_total: "Position total",
}
const recommendationLabels: Record<CalculatedMethod["recommendationUse"], string> = {
  selected: "Selected for the saved recommendation.",
  not_per_instrument_unit: "This total value was not used as a price per instrument unit.",
  share_unit_basis_unproven: "The share-unit basis was not established for the saved recommendation.",
  another_method_selected: "Another method was selected; this method was not checked for recommendation use.",
  admission_unavailable: "This calculation could not support the saved recommendation.",
}

export function ValuationEvidence({ analysis }: { analysis: InvestmentAnalysis }) {
  const methods = analysis.priceSummary.valuationMethods
  return <section className="mt-5 rounded-lg border border-border bg-background/25 p-4" aria-labelledby="valuation-methods-title">
    <h3 id="valuation-methods-title" className="text-sm font-semibold">Saved valuation methods</h3>
    <p className="mt-2 text-xs leading-5 text-muted-foreground">{analysis.analyticalEvidence.valuation.summary}</p>
    {methods === null ? <p className="mt-3 text-xs leading-5 text-muted-foreground">
      No saved method calculations are available for this analysis.
    </p> : <>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        Each amount retains its original value basis. Total equity, entity and position values cannot be compared directly with a price per instrument unit.
      </p>
      <dl className="mt-4 grid gap-4 sm:grid-cols-3">
        <MethodFact label="Source information cutoff" value={formatUnixNanos(methods.sourceCutoffUnixNanos)} />
        <MethodFact label="Market information cutoff" value={formatUnixNanos(methods.marketCutoffUnixNanos)} />
        <MethodFact label="Calculation completed" value={formatUnixNanos(methods.completedAtUnixNanos)} />
      </dl>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        {methods.methods.map((method) => <MethodCard key={method.method} method={method} />)}
      </div>
    </>}
  </section>
}

function MethodCard({ method }: { method: Method }) {
  return <section className="rounded-lg border border-border bg-card/35 p-4" aria-label={methodLabels[method.method]}>
    <h4 className="text-sm font-semibold">{methodLabels[method.method]}</h4>
    {method.status === "unavailable" ? <>
      <p className="mt-3 text-xs font-medium">Unavailable</p>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">{method.summary}</p>
    </> : <>
      <p className="mt-2 text-xs font-medium">{basisLabels[method.basis]}</p>
      <dl className="mt-4 grid gap-3 sm:grid-cols-3">
        <MethodFact label="Lower estimate" value={money(method.lower)} />
        <MethodFact label="Central estimate" value={money(method.central)} />
        <MethodFact label="Upper estimate" value={money(method.upper)} />
      </dl>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">{recommendationLabels[method.recommendationUse]}</p>
      {method.terminalGrowth ? <div className="mt-4 border-t border-border pt-3">
        <p className="text-xs font-medium">Saved terminal growth assumption</p>
        <dl className="mt-3 space-y-3">
          <MethodFact label="Uncapped annual growth ratio" value={method.terminalGrowth.uncapped} />
          <MethodFact label="Risk-free annual cap ratio" value={method.terminalGrowth.riskFreeCap} />
          <MethodFact label="Applied annual growth ratio" value={method.terminalGrowth.applied} />
        </dl>
      </div> : null}
      {method.residualTerminal ? <div className="mt-4 border-t border-border pt-3">
        <p className="text-xs font-medium">Saved residual-income terminal assumption</p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">{method.residualTerminal.condition}</p>
        <dl className="mt-3 space-y-3">
          <MethodFact label="Explicit periods" value={method.residualTerminal.explicitPeriods.toLocaleString("en-US")} />
          <MethodFact label="Continuing-value sensitivity coefficient" value={method.residualTerminal.continuingValueSensitivity} />
        </dl>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">This coefficient describes sensitivity to continuing value; it is not an estimate of that value.</p>
      </div> : null}
    </>}
  </section>
}

function MethodFact({ label, value }: { label: string; value: string }) {
  return <div>
    <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
    <dd className="mt-1 text-xs leading-5">{value}</dd>
  </div>
}
