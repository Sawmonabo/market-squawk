import {
  Activity,
  BadgeDollarSign,
  CircleAlert,
  CircleCheck,
  Clock3,
  Layers3,
  ShieldAlert,
  WalletCards,
} from "lucide-react"
import type { ReactNode } from "react"

import { PortfolioChart } from "@/components/charts/portfolio-chart"
import { formatMoney } from "@/lib/formatters"
import { formatUnixNanos } from "../opportunities/format"

import type {
  PortfolioAccount,
  PortfolioExposure,
  PortfolioHolding,
  PortfolioPerformance,
  PortfolioRisk,
} from "./portfolio-contracts"
import {
  formatProductTime,
  formatPortfolioRate,
  investmentDisplayName,
  portfolioDisplayName,
} from "./portfolio-format"

export function PortfolioSummary({ account }: { account: PortfolioAccount }) {
  return (
    <section aria-label={`${portfolioDisplayName(account)} summary`}>
      <div className="rounded-xl border border-border bg-card/45 p-5">
        <p className="text-sm font-semibold">{portfolioDisplayName(account)}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          {account.accountTypeLabel} · Updated {formatProductTime(account.updatedAt)}
        </p>
      </div>
      <div className="mt-3 grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-5">
        <SummaryFact
          icon={WalletCards}
          label="Portfolio value"
          value={account.currentValue ? formatMoney(account.currentValue) : "Unavailable"}
          help="The latest complete value supplied for this portfolio."
        />
        <SummaryFact
          icon={BadgeDollarSign}
          label="Cash"
          value={formatMoney(account.cashBalance)}
          help="Cash reported for this portfolio; not a bank balance."
        />
        <SummaryFact
          icon={Activity}
          label="Return"
          value={account.returnSinceStart?.display ?? "Unavailable"}
          help="The prepared return for the available portfolio history."
        />
        <SummaryFact
          icon={Layers3}
          label="Positions"
          value={account.positionCount.toLocaleString()}
          help={`${account.transactionCount.toLocaleString()} recorded transactions.`}
        />
        <SummaryFact
          icon={account.reviewFindingCount === 0 ? CircleCheck : CircleAlert}
          label="Items to review"
          value={account.reviewFindingCount.toLocaleString()}
          help={account.reviewState.explanation}
          tone={account.reviewState.tone === "attention" ? "warning" : "default"}
        />
      </div>
    </section>
  )
}

export function AllocationPanel({ holdings }: { holdings: PortfolioHolding[] }) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="What you own"
        title="Positions on this page"
        detail="Reported market values for this page only, not the complete portfolio allocation. Negative values represent short exposure."
      />
      <div className="mt-4">
        <PortfolioChart
          data={holdings.map((holding) => ({
            label: investmentDisplayName({
              name: holding.investment.name ?? "Investment name unavailable",
              symbol: holding.investment.symbol,
            }),
            exactAmount: holding.marketValue.amount,
            currency: holding.marketValue.currency,
          }))}
        />
      </div>
    </section>
  )
}

export function PerformancePanel({ performance }: { performance: PortfolioPerformance }) {
  const coverage = performance.historyStatus === "insufficient_history"
    ? "At least two portfolio observations are needed to calculate returns."
    : performance.historyStatus === "insufficient_comparable_history"
      ? "The recorded observations do not support a comparable return period."
      : `Returns use ${performance.periods?.toLocaleString()} comparable portfolio update periods. They are not annualized.`
  const values: [string, string][] = [
    ["Portfolio value", formatMoney(performance.currentValue)],
    ["Time-weighted return", formatPortfolioRate(performance.timeWeightedReturn)],
    ["Money-weighted return", formatPortfolioRate(performance.moneyWeightedReturn)],
    ["Comparable periods", performance.periods?.toLocaleString() ?? "Not available"],
  ]
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading eyebrow="How it has changed" title="Performance" detail={coverage} />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        {values.map(([label, value]) => <Fact key={label} label={label} value={value} />)}
      </dl>
      <p className="mt-4 text-xs leading-5 text-muted-foreground">
        Time-weighted return adjusts for external cash transfers. Money-weighted return uses
        the recorded cash transfers and portfolio values to estimate the return on invested money.
      </p>
      <AccountingPanel accounting={performance.accountingEvidence} />
      <EvidenceNote icon={Clock3}>
        Values are reported portfolio observations, not current market prices. Portfolio value
        includes cash, reported holdings and any unpaid cash entitlements. Returns use the
        available recorded history; enough comparable periods does not establish complete history.
      </EvidenceNote>
      <dl className="mt-4 grid gap-4 sm:grid-cols-2">
        <Fact label="Portfolio observation" value={formatUnixNanos(performance.effectiveAtUnixNanos)} />
        <Fact label="Information available" value={performance.availableAtUnixNanos === null
          ? "Not recorded" : formatUnixNanos(performance.availableAtUnixNanos)} />
        <Fact label="Data confidence" value={performance.dataConfidence === "limited"
          ? "Limited" : performance.dataConfidence === "moderate" ? "Moderate" : "Strong"} />
      </dl>
    </section>
  )
}

export function ExposurePanel({ exposure }: { exposure: PortfolioExposure }) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Where risk is concentrated"
        title="Exposure"
        detail={exposure.coverageExplanation}
      />
      <div className="mt-5 grid gap-5 lg:grid-cols-2">
        <ExposureList title="By currency" rows={exposure.byCurrency} />
        <ExposureList title="By investment" rows={exposure.byInvestment.slice(0, 8)} />
      </div>
      {(exposure.net || exposure.gross) && (
        <dl className="mt-5 grid gap-4 sm:grid-cols-2">
          <Fact label="Net exposure" value={exposure.net ? formatMoney(exposure.net) : "Not available"} />
          <Fact label="Gross exposure" value={exposure.gross ? formatMoney(exposure.gross) : "Not available"} />
        </dl>
      )}
    </section>
  )
}

export function RiskPanel({ risk }: { risk: PortfolioRisk }) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="What could go wrong"
        title="Risk overview"
        detail={risk.coverageExplanation}
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        {risk.metrics.map((metric) => (
          <div key={metric.label}>
            <Fact label={metric.label} value={metric.value} />
            <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
              {metric.explanation}
            </p>
          </div>
        ))}
      </dl>
      {risk.stress ? (
        <div className="mt-5 rounded-lg border border-amber-400/20 bg-amber-400/5 p-4">
          <div className="flex items-center gap-2 text-sm font-medium text-amber-200">
            <ShieldAlert className="size-4" aria-hidden="true" />
            {risk.stress.title}
          </div>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Assumption: {risk.stress.assumption}
          </p>
          <p className="mt-2 text-sm text-muted-foreground">{risk.stress.result}</p>
          {risk.stress.impact ? (
            <p className="mt-2 font-mono text-sm tabular-nums">
              {formatMoney(risk.stress.impact)}
            </p>
          ) : null}
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Uncertainty: {risk.stress.uncertainty}
          </p>
        </div>
      ) : null}
    </section>
  )
}

export function DataQualityPanel({ account }: { account: PortfolioAccount }) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Before relying on these numbers"
        title="Portfolio coverage"
        detail={account.reviewState.explanation}
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        <Fact label="Reporting currency" value={account.reportingCurrency} />
        <Fact label="Portfolio updated" value={formatProductTime(account.updatedAt)} />
        <Fact label="Analysis prepared" value={formatProductTime(account.preparedAt)} />
        <Fact label="Review state" value={account.reviewState.label} />
      </dl>
      <EvidenceNote icon={Clock3}>
        Portfolio values can differ from current market prices. Review the update time and any
        items needing attention before acting.
      </EvidenceNote>
    </section>
  )
}

export function ReconciliationPanel({ performance }: { performance: PortfolioPerformance | null }) {
  const reconciliation = performance?.accountingEvidence.reconciliation
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Import confidence"
        title="Reconciliation"
        detail={reconciliation?.status === "needs_review"
          ? "Reported and calculated totals have recorded differences that need review."
          : reconciliation ? "No comparison differences are recorded for this portfolio observation."
            : "A reported-versus-calculated comparison is not available."}
      />
      {!reconciliation ? (
        <EvidenceNote icon={CircleAlert}>
          Do not treat a missing comparison as confirmation that the totals agree.
        </EvidenceNote>
      ) : (
        <div className="mt-5 space-y-3">
          {reconciliation.discrepancies.map((finding, index) => (
            <div key={`${finding.field}:${index}`} className="rounded-lg border border-border bg-background/35 p-3 text-xs">
              <p className="font-medium text-foreground">
                {finding.field === "cash" ? "Cash" : finding.field === "market_value" ? "Market value" : "Cost basis"}
                {" · "}{finding.currency}
              </p>
              <dl className="mt-2 grid gap-2 sm:grid-cols-3">
                <Fact label="Reported" value={formatMoney(finding.supplied)} />
                <Fact label="Calculated" value={formatMoney(finding.calculated)} />
                <Fact label="Allowed difference" value={formatMoney(finding.tolerance.amount)} />
              </dl>
            </div>
          ))}
          <p className="text-xs leading-5 text-muted-foreground">
            This shows recorded comparison findings. The absence of a recorded difference
            does not establish that every account figure has been independently verified.
          </p>
        </div>
      )}
    </section>
  )
}

function AccountingPanel({ accounting }: { accounting: PortfolioPerformance["accountingEvidence"] }) {
  const measures = [
    { label: "Unrealized gain", value: accounting.unrealizedGain,
      detail: "Uses reported holdings values and resolved cost basis. Missing or ambiguous basis makes this unavailable." },
    { label: "Realized gain", value: accounting.realizedGain,
      detail: accounting.realizedGain.status === "not_available"
        ? "The recorded trade history does not yet support a realized gain calculation."
        : "Calculated from the available recorded trade history." },
    { label: "Recorded income", value: accounting.income,
      detail: accounting.income.status === "partial"
        ? "The recorded income total is partial: dividends, interest and withholding have not been separately identified."
        : accounting.income.status === "not_available" ? "The recorded history does not support an income total."
          : "The total of income recorded in this portfolio history." },
    { label: "Recorded fees", value: accounting.fees,
      detail: "The signed total of transactions recorded as fees; this does not establish complete fee history." },
  ]
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <h3 className="text-sm font-medium">Cash and account summary</h3>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2">
        <Fact label="Reported cash" value={formatMoney(accounting.cash.amount)} />
        <Fact label="Reported holdings value" value={formatMoney(accounting.reportedMarketValue)} />
        {measures.map(({ label, value, detail }) => (
          <div key={label}>
            <Fact label={label} value={measuredAmount(value)} />
            <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{detail}</p>
          </div>
        ))}
      </dl>
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        Cash observed {formatUnixNanos(accounting.cash.observedAtUnixNanos)}. This reported
        portfolio cash is not a current bank balance or confirmation of funds available to trade.
      </p>
    </section>
  )
}

function measuredAmount(value: PortfolioPerformance["accountingEvidence"]["income"]): string {
  if (value.status === "not_available" || value.amount === undefined) return "Not available"
  return `${formatMoney(value.amount)}${value.status === "partial" ? " · Partial" : ""}`
}

function SummaryFact({
  icon: Icon,
  label,
  value,
  help,
  tone = "default",
}: {
  icon: typeof BadgeDollarSign
  label: string
  value: string
  help: string
  tone?: "default" | "warning"
}) {
  return (
    <div className="border-b border-border p-4 last:border-b-0 sm:border-b-0 sm:border-r sm:last:border-r-0">
      <Icon className={`size-4 ${tone === "warning" ? "text-amber-300" : "text-primary"}`} aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{help}</p>
    </div>
  )
}

function PanelHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) {
  return (
    <header>
      <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">{eyebrow}</p>
      <h2 className="mt-2 text-lg font-semibold">{title}</h2>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{detail}</p>
    </header>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-sm tabular-nums">{value}</dd>
    </div>
  )
}

function ExposureList({
  title,
  rows,
}: {
  title: string
  rows: { label: string; amount: { amount: string; currency: string } }[]
}) {
  return (
    <div>
      <h3 className="text-xs font-semibold">{title}</h3>
      {rows.length ? (
        <dl className="mt-3 space-y-2">
          {rows.map((row) => (
            <div key={`${row.label}-${row.amount.amount}`} className="flex justify-between gap-4 text-xs">
              <dt className="truncate text-muted-foreground">{row.label}</dt>
              <dd className="shrink-0 font-mono tabular-nums">{formatMoney(row.amount)}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="mt-3 text-xs text-muted-foreground">No complete exposure is available.</p>
      )}
    </div>
  )
}

function EvidenceNote({
  icon: Icon,
  children,
}: {
  icon: typeof Clock3
  children: ReactNode
}) {
  return (
    <div className="mt-5 flex gap-2 rounded-lg border border-border bg-background/35 p-3 text-xs leading-5 text-muted-foreground">
      <Icon className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <span>{children}</span>
    </div>
  )
}
