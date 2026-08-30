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

import type {
  PortfolioAccount,
  PortfolioExposure,
  PortfolioHolding,
  PortfolioPerformance,
  PortfolioRisk,
} from "./portfolio-contracts"
import {
  formatProductTime,
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
        title="Largest positions"
        detail="Market value by named investment. Negative values represent short exposure."
      />
      <div className="mt-4">
        <PortfolioChart
          data={holdings.map((holding) => ({
            label: investmentDisplayName(holding.investment),
            exactAmount: holding.marketValue.amount,
            currency: holding.marketValue.currency,
          }))}
        />
      </div>
    </section>
  )
}

export function PerformancePanel({ performance }: { performance: PortfolioPerformance }) {
  const values: [string, string][] = [
    ["Current value", formatMoney(performance.currentValue)],
    ["Time-weighted return", performance.timeWeightedReturn?.display ?? "Not available"],
    ["Money-weighted return", performance.moneyWeightedReturn?.display ?? "Not available"],
    [
      "Comparable periods",
      performance.comparablePeriods?.toLocaleString() ?? "Not available",
    ],
  ]
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="How it has changed"
        title="Performance"
        detail={performance.coverageExplanation}
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        {values.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
      {performance.accounting ? <AccountingPanel accounting={performance.accounting} /> : null}
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
  const accounting = performance?.accounting
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Import confidence"
        title="Reconciliation"
        detail={
          accounting?.reconciliationExplanation ??
          "A complete supplied-versus-calculated comparison is not available."
        }
      />
      {!accounting ? (
        <EvidenceNote icon={CircleAlert}>
          Do not treat a missing comparison as confirmation that the totals agree.
        </EvidenceNote>
      ) : (
        <div className="mt-5 space-y-3">
          <p className="text-sm font-semibold">{accounting.reconciliationLabel}</p>
          {accounting.reconciliationFindings.map((finding) => (
            <div
              key={finding.findingActionToken}
              className="rounded-lg border border-border bg-background/35 p-3 text-xs"
            >
              <p className="font-medium text-foreground">{finding.label}</p>
              <p className="mt-1 leading-5 text-muted-foreground">{finding.explanation}</p>
              <dl className="mt-2 grid gap-2 sm:grid-cols-3">
                <Fact label="Supplied" value={formatMoney(finding.supplied)} />
                <Fact label="Calculated" value={formatMoney(finding.calculated)} />
                <Fact label="Tolerance" value={formatMoney(finding.tolerance)} />
              </dl>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

function AccountingPanel({
  accounting,
}: {
  accounting: NonNullable<PortfolioPerformance["accounting"]>
}) {
  const entries: [string, string][] = [
    ["Reported cash", formatMoney(accounting.cash)],
    ["Reported market value", formatMoney(accounting.reportedMarketValue)],
    ["Unrealized gain", measuredAmount(accounting.unrealizedGain)],
    ["Realized gain", measuredAmount(accounting.realizedGain)],
    ["Reported income", measuredAmount(accounting.income)],
    ["Reported fees", measuredAmount(accounting.fees)],
  ]
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <p className="text-sm font-medium">Account summary</p>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {entries.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
      <p className="mt-3 text-[11px] text-muted-foreground">
        Cash updated {formatProductTime(accounting.cashUpdatedAt)}
      </p>
    </section>
  )
}

function measuredAmount(value: {
  state: "available" | "unavailable"
  amount?: { amount: string; currency: string }
  explanation?: string
}) {
  return value.state === "available" && value.amount
    ? formatMoney(value.amount)
    : value.explanation ?? "Not available"
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
