import {
  Activity,
  BadgeDollarSign,
  CircleAlert,
  CircleCheck,
  Clock3,
  Database,
  Layers3,
  ShieldAlert,
  WalletCards,
} from "lucide-react"

import { PortfolioChart } from "@/components/charts/portfolio-chart"
import { formatMoney, humanize } from "@/lib/formatters"

import type {
  PortfolioAccount,
  PortfolioExposure,
  PortfolioHolding,
  PortfolioPerformance,
  PortfolioResult,
  PortfolioRisk,
} from "./portfolio-contracts"
import {
  compactMoney,
  evidenceLabel,
  formatPercent,
  formatTimestamp,
  shortIdentity,
} from "./portfolio-format"

export function PortfolioSummary({
  account,
  holdings,
  performance,
}: {
  account: PortfolioAccount
  holdings: PortfolioHolding[] | null
  performance: PortfolioPerformance | null
}) {
  return (
    <section
      aria-label="Portfolio summary"
      className="grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-5"
    >
      <SummaryFact
        icon={WalletCards}
        label="Account value"
        value={performance ? compactMoney(performance.currentValue) : "Unavailable"}
        help="Reported cash plus current asset values for this account only."
      />
      <SummaryFact
        icon={BadgeDollarSign}
        label="Account cash"
        value={
          performance?.accountingEvidence
            ? compactMoney(performance.accountingEvidence.cash.amount)
            : "Unavailable"
        }
        help="The reported account cash balance; not an inferred checking or savings balance."
      />
      <SummaryFact
        icon={Activity}
        label="Return"
        value={formatPercent(performance?.timeWeightedReturn)}
        help={
          performance?.historyStatus
            ? evidenceLabel(performance.historyStatus)
            : "Time-weighted return for the available account history."
        }
      />
      <SummaryFact
        icon={Layers3}
        label="Assets"
        value={(holdings?.length ?? account.holdingCount).toLocaleString()}
        help={`${account.transactionCount.toLocaleString()} recorded transactions across supported asset types.`}
      />
      <SummaryFact
        icon={
          account.reconciliationDiscrepancies === 0 ? CircleCheck : CircleAlert
        }
        label="Reconciliation findings"
        value={
          account.reconciliationDiscrepancies === 0
            ? "None retained"
            : `${account.reconciliationDiscrepancies} to review`
        }
        help="A zero count does not prove that every possible total was supplied."
        tone={account.reconciliationDiscrepancies === 0 ? "default" : "warning"}
      />
    </section>
  )
}

export function FinancialPositionCoverage({
  accounts,
  holdingsAvailable,
  performanceAvailable,
  transactionsAvailable,
}: {
  accounts: PortfolioAccount[]
  holdingsAvailable: boolean
  performanceAvailable: boolean
  transactionsAvailable: boolean
}) {
  return (
    <section
      className="mt-5 rounded-xl border border-border bg-card/35 p-5"
      aria-label="Financial-position coverage"
    >
      <PanelHeading
        eyebrow="What this workspace can represent"
        title="Financial-position coverage"
        detail="Available means the account contains enough information for this view. Setup required and Unavailable never mean a zero balance."
      />
      <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        <CoverageItem
          label="Imported accounts"
          state={accounts.length > 0 ? "available" : "setup"}
          detail={
            accounts.length > 0
              ? `${accounts.length.toLocaleString()} account record${accounts.length === 1 ? "" : "s"} returned. Their account classifications were not supplied.`
              : "Import an account file before Portfolio can show account details."
          }
        />
        <CoverageItem
          label="Investments and other assets"
          state={holdingsAvailable ? "available" : "unavailable"}
          detail={
            holdingsAvailable
              ? "Asset details, quantities, prices, and cost-basis status are available after account selection."
              : "Holding details are unavailable right now."
          }
        />
        <CoverageItem
          label="Account cash"
          state={performanceAvailable ? "available" : "unavailable"}
          detail={
            performanceAvailable
              ? "Reported cash appears with the selected account. It is not inferred from holdings."
              : "No reported cash balance is available for this account."
          }
        />
        <CoverageItem
          label="Transactions, income, and fees"
          state={transactionsAvailable ? "available" : "unavailable"}
          detail={
            transactionsAvailable
              ? "Transactions are available, although some income categories may need clarification."
              : "Transaction details are unavailable right now."
          }
        />
        <CoverageItem
          label="Bank, checking, and savings synchronization"
          state="setup"
          detail="Bank connections and synchronized balances are not available yet."
        />
        <CoverageItem
          label="Liabilities and net worth"
          state="unavailable"
          detail="Liabilities and a combined net-worth view are not available. Accounts and currencies are never silently combined."
        />
        <CoverageItem
          label="Recommendation account and profile"
          state="setup"
          detail="Choose a recommendation account and allocation profile in a future update."
        />
      </div>
    </section>
  )
}

export function RecommendationSetupPanel({
  selectedAccount,
}: {
  selectedAccount: PortfolioAccount | null
}) {
  return (
    <section
      className="mt-5 rounded-xl border border-amber-400/25 bg-amber-400/5 p-5"
      aria-labelledby="portfolio-recommendation-setup"
    >
      <div className="flex gap-3">
        <ShieldAlert className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden="true" />
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-amber-200">
            Setup required
          </p>
          <h2 id="portfolio-recommendation-setup" className="mt-2 text-base font-semibold">
            Recommendation account and profile are not connected
          </h2>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            {selectedAccount
              ? `${shortIdentity(selectedAccount.accountId, "Account")} is selected only for inspection on this page. `
              : "No account is selected for inspection. "}
            Personalized Add, Hold, Trim, or Sell recommendations are not available for this
            account yet.
            Market Squawk will not reuse the first account or this page selection as that authority.
          </p>
          <p className="mt-3 text-xs leading-5 text-muted-foreground">
            Holdings, cash, performance, and risk details below remain available independently.
          </p>
        </div>
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
        detail="Signed market value by holding. Negative bars represent short exposure."
      />
      <div className="mt-4">
        <PortfolioChart
          data={holdings.map((holding) => ({
            label: shortIdentity(holding.instrument_id, "Asset"),
            exactAmount: holding.market_value.amount,
            currency: holding.market_value.currency,
          }))}
        />
      </div>
    </section>
  )
}

export function PerformancePanel({
  performance,
}: {
  performance: PortfolioPerformance
}) {
  const values: [string, string][] = [
    ["Current value", formatMoney(performance.currentValue)],
    ["Time-weighted return", formatPercent(performance.timeWeightedReturn)],
    ["Money-weighted return", formatPercent(performance.moneyWeightedReturn)],
    ["Comparable periods", performance.periods?.toLocaleString() ?? "Not available"],
  ]
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="How it has changed"
        title="Performance"
        detail="Returns use the available account history and do not fill in missing periods."
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        {values.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
      {performance.historyStatus ? (
        <EvidenceNote icon={Clock3}>
          {evidenceLabel(performance.historyStatus)}. Market Squawk does not extrapolate missing
          history.
        </EvidenceNote>
      ) : null}
      {performance.accountingEvidence ? (
        <AccountingPanel accounting={performance.accountingEvidence} />
      ) : null}
    </section>
  )
}

export function ExposurePanel({ exposure }: { exposure: PortfolioExposure }) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Where risk is concentrated"
        title="Exposure"
        detail="Holdings and currency totals from the selected account snapshot."
      />
      <div className="mt-5 grid gap-5 lg:grid-cols-2">
        <ExposureList
          title="By currency"
          rows={exposure.currency.map((row) => ({
            label: row.currency.toUpperCase(),
            amount: formatMoney(row.amount),
          }))}
        />
        <ExposureList
          title="By asset"
          rows={exposure.instrument.slice(0, 8).map((row) => ({
            label: shortIdentity(row.instrumentId, "Asset"),
            amount: formatMoney(row.amount),
          }))}
        />
      </div>
      {exposure.classificationStatus ? (
        <EvidenceNote icon={CircleAlert}>
          Sector and factor classifications are {humanize(exposure.classificationStatus).toLowerCase()}.
          No classification was inferred from the asset identifier.
        </EvidenceNote>
      ) : null}
    </section>
  )
}

export function RiskPanel({ risk }: { risk: PortfolioRisk }) {
  const confidence = formatPercent(risk.confidence)
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="What could go wrong"
        title="Risk snapshot"
        detail="Historical loss estimates and one clearly labelled deterministic stress—not a forecast."
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        <Fact
          label={`${confidence} historical loss threshold`}
          value={formatPercent(risk.valueAtRisk)}
        />
        <Fact
          label="Average loss beyond threshold"
          value={formatPercent(risk.expectedShortfall)}
        />
        <Fact
          label="Annualized volatility"
          value={formatPercent(risk.annualizedVolatility)}
        />
        <Fact
          label="Return observations"
          value={risk.observations?.toLocaleString() ?? "Not available"}
        />
      </dl>
      <div className="mt-5 rounded-lg border border-amber-400/20 bg-amber-400/5 p-4">
        <div className="flex items-center gap-2 text-sm font-medium text-amber-200">
          <ShieldAlert className="size-4" aria-hidden="true" />
          Stress scenario: {humanize(risk.scenario.id)}
        </div>
        <p className="mt-2 text-sm text-muted-foreground">
          {risk.scenario.impact
            ? `${formatMoney(risk.scenario.impact)} estimated portfolio impact.`
            : evidenceLabel(risk.scenario.status)}
        </p>
      </div>
      {risk.historyStatus || risk.volatilityStatus ? (
        <EvidenceNote icon={Clock3}>
          {[risk.historyStatus, risk.volatilityStatus]
            .filter((value): value is string => value !== undefined)
            .map(evidenceLabel)
            .join(" · ")}
        </EvidenceNote>
      ) : null}
    </section>
  )
}

export function DataQualityPanel({
  account,
  holdingsResult,
}: {
  account: PortfolioAccount
  holdingsResult: PortfolioResult<PortfolioHolding[]>
}) {
  const quality = holdingsResult.evidence.dataQuality
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Why these numbers can be trusted"
        title="Data confidence"
        detail="Review when the account was updated, its coverage, and whether its values are suitable for analysis."
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        <Fact label="Reporting currency" value={account.currency.toUpperCase()} />
        <Fact
          label="Account updated"
          value={formatTimestamp(account.currentRevision.effectiveAtUnixNanos)}
        />
        <Fact
          label="Analysis updated"
          value={formatTimestamp(account.currentRevision.availableAtUnixNanos)}
        />
        <Fact
          label="Data quality"
          value={evidenceLabel(quality?.class)}
        />
        <Fact
          label="Suitable for analysis"
          value={quality?.executionEligible === true ? "Yes" : "No"}
        />
        <Fact
          label="Coverage"
          value={`${holdingsResult.evidence.returnedItems} of ${holdingsResult.evidence.availableItems} holdings`}
        />
      </dl>
      <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
        <p className="text-sm font-medium text-foreground">Import progress</p>
        <ol className="mt-3 space-y-2 text-xs leading-5 text-muted-foreground">
          <li>
            1. The account file was imported successfully.
          </li>
          <li>
            2. {account.holdingCount.toLocaleString()} holdings and {" "}
            {account.transactionCount.toLocaleString()} transactions are available to
            review.
          </li>
          <li>
            3. {account.reconciliationDiscrepancies === 0
              ? "Review the reconciliation explanation before relying on the totals."
                : "Review each reconciliation difference below, then import a corrected file if needed."}
          </li>
        </ol>
      </div>
      <EvidenceNote icon={CircleAlert}>
        Imported account values may differ from current market prices. Check each holding's update
        time and data quality before acting on it.
      </EvidenceNote>
    </section>
  )
}

export function ReconciliationPanel({
  account,
  performance,
}: {
  account: PortfolioAccount
  performance: PortfolioPerformance | null
}) {
  const details = performance?.accountingEvidence?.reconciliation
  const hasNoFindings = details?.discrepancies.length === 0
  const missingDetails = details === undefined
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <PanelHeading
        eyebrow="Import confidence"
        title="Reconciliation"
        detail="Market Squawk compares supplied cash, market value, and cost basis with independently calculated totals."
      />
      <div
        className={`mt-5 rounded-lg border p-4 ${
          hasNoFindings
            ? "border-emerald-400/20 bg-emerald-400/5"
            : "border-amber-400/20 bg-amber-400/5"
        }`}
      >
        <div className="flex items-center gap-2 text-sm font-medium">
          {hasNoFindings ? (
            <CircleCheck className="size-4 text-emerald-300" aria-hidden="true" />
          ) : (
            <CircleAlert className="size-4 text-amber-300" aria-hidden="true" />
          )}
          {missingDetails
            ? "Detailed reconciliation information is unavailable"
            : hasNoFindings
            ? "No supplied-versus-calculated discrepancy was retained"
            : `${account.reconciliationDiscrepancies} supplied total${
                account.reconciliationDiscrepancies === 1 ? "" : "s"
              } need review`}
        </div>
        {missingDetails ? (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Detailed comparison information is unavailable for this account snapshot. Do not treat
            the discrepancy count as a completed reconciliation.
          </p>
        ) : !hasNoFindings ? (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Each mismatch compares the supplied value with the independently calculated value and
            tolerance. Correct the export and import it again if needed.
          </p>
        ) : (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            This means no recorded comparison exceeded its tolerance. It does not confirm that every
            possible total was included.
          </p>
        )}
      </div>
      {details && details.discrepancies.length > 0 ? (
        <div className="mt-4 space-y-3">
          {details.discrepancies.map((detail) => (
            <div key={`${detail.sourceReference}-${detail.field}`} className="rounded-lg border border-border bg-background/35 p-3 text-xs">
              <p className="font-medium text-foreground">{humanize(detail.field)}</p>
              <dl className="mt-2 grid gap-2 sm:grid-cols-3">
                <Fact label="Supplied" value={formatMoney(detail.supplied)} />
                <Fact label="Calculated" value={formatMoney(detail.calculated)} />
                <Fact label="Tolerance" value={formatMoney(detail.tolerance.amount)} />
              </dl>
            </div>
          ))}
        </div>
      ) : null}
    </section>
  )
}

function AccountingPanel({
  accounting,
}: {
  accounting: NonNullable<PortfolioPerformance["accountingEvidence"]>
}) {
  const entries: [string, string][] = [
    ["Reported cash", formatMoney(accounting.cash.amount)],
    ["Reported market value", formatMoney(accounting.reportedMarketValue)],
    ["Unrealized gain", accountingValue(accounting.unrealizedGain)],
    ["Realized gain", accountingValue(accounting.realizedGain)],
    ["Reported income", accountingValue(accounting.income)],
    ["Reported fees", accountingValue(accounting.fees)],
  ]
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <p className="font-medium text-sm">Account summary</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Reported values and calculated values remain separate. A gain appears only when the account
        includes enough cost-basis and transaction history.
      </p>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {entries.map(([label, value]) => <Fact key={label} label={label} value={value} />)}
      </dl>
      <p className="mt-3 text-[11px] text-muted-foreground">
        Cash updated {formatTimestamp(accounting.cash.observedAtUnixNanos)}
      </p>
    </section>
  )
}

function accountingValue(value: {
  status: string
  amount?: { amount: string; currency: string }
  reason?: string
}) {
  return value.amount ? formatMoney(value.amount) : evidenceLabel(value.status)
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
  tone?: "default" | "good" | "warning"
}) {
  const iconClass =
    tone === "good"
      ? "text-emerald-300"
      : tone === "warning"
        ? "text-amber-300"
        : "text-primary"
  return (
    <div className="border-b border-border p-4 last:border-b-0 sm:border-b-0 sm:border-r sm:last:border-r-0">
      <Icon className={`size-4 ${iconClass}`} aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">{help}</p>
    </div>
  )
}

function PanelHeading({
  eyebrow,
  title,
  detail,
}: {
  eyebrow: string
  title: string
  detail: string
}) {
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
  rows: { label: string; amount: string }[]
}) {
  return (
    <div>
      <h3 className="text-xs font-semibold">{title}</h3>
      {rows.length ? (
        <dl className="mt-3 space-y-2">
          {rows.map((row) => (
            <div key={`${row.label}-${row.amount}`} className="flex justify-between gap-4 text-xs">
              <dt className="truncate text-muted-foreground">{row.label}</dt>
              <dd className="shrink-0 font-mono tabular-nums">{row.amount}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="mt-3 text-xs text-muted-foreground">No classified exposure returned.</p>
      )}
    </div>
  )
}

function CoverageItem({
  label,
  state,
  detail,
}: {
  label: string
  state: "available" | "setup" | "unavailable"
  detail: string
}) {
  const stateLabel =
    state === "available" ? "Available" : state === "setup" ? "Setup required" : "Unavailable"
  const classes =
    state === "available"
      ? "border-emerald-400/20 bg-emerald-400/5 text-emerald-200"
      : state === "setup"
        ? "border-amber-400/20 bg-amber-400/5 text-amber-200"
        : "border-border bg-background/30 text-muted-foreground"
  return (
    <div className={`rounded-lg border p-4 ${classes}`}>
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold text-foreground">{label}</h3>
        <span className="shrink-0 rounded-md border border-current/20 px-2 py-1 text-[9px] font-medium uppercase tracking-wider">
          {stateLabel}
        </span>
      </div>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">{detail}</p>
    </div>
  )
}

function EvidenceNote({
  icon: Icon,
  children,
}: {
  icon: typeof Database
  children: React.ReactNode
}) {
  return (
    <div className="mt-5 flex gap-2 rounded-lg border border-border bg-background/35 p-3 text-xs leading-5 text-muted-foreground">
      <Icon className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <span>{children}</span>
    </div>
  )
}
