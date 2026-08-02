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
      className="grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-4"
    >
      <SummaryFact
        icon={WalletCards}
        label="Portfolio value"
        value={performance ? compactMoney(performance.currentValue) : "Unavailable"}
        help="Cash plus the source-backed market value of the selected holdings."
      />
      <SummaryFact
        icon={Activity}
        label="Return"
        value={formatPercent(performance?.timeWeightedReturn)}
        help={
          performance?.historyStatus
            ? evidenceLabel(performance.historyStatus)
            : "Time-weighted return for the available comparable revision history."
        }
      />
      <SummaryFact
        icon={Layers3}
        label="Holdings"
        value={(holdings?.length ?? account.holdingCount).toLocaleString()}
        help={`${account.transactionCount.toLocaleString()} source transactions in this revision.`}
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
        detail="Returns are calculated only from revisions that were available at the selected point in time."
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
        detail="Exact holdings and currency totals from the selected immutable revision."
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

export function ProvenancePanel({
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
        title="Value provenance"
        detail="Every imported value stays tied to its portfolio source, immutable revision, and availability time."
      />
      <dl className="mt-5 grid gap-4 sm:grid-cols-2">
        <Fact label="Reporting currency" value={account.currency.toUpperCase()} />
        <Fact label="Source" value={account.currentRevision.sourceId} />
        <Fact
          label="Portfolio effective time"
          value={formatTimestamp(account.currentRevision.effectiveAtUnixNanos)}
        />
        <Fact
          label="Available to analysis"
          value={formatTimestamp(account.currentRevision.availableAtUnixNanos)}
        />
        <Fact
          label="Data quality"
          value={evidenceLabel(quality?.class)}
        />
        <Fact
          label="Execution eligible"
          value={quality?.executionEligible === true ? "Yes" : "No"}
        />
      </dl>
      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4 text-xs text-muted-foreground">
        <p className="font-medium text-foreground">Immutable evidence</p>
        <p className="mt-2 break-all font-mono">Revision {account.currentRevision.revisionId}</p>
        <p className="mt-1 break-all font-mono">Artifact {account.currentRevision.artifactSha256}</p>
        <p className="mt-2">
          Coverage: {account.currentRevision.sourceCoverage.join(", ") || "Not reported"}. Result:
          {" "}{humanize(holdingsResult.evidence.completeness)} ({holdingsResult.evidence.returnedItems}
          {" "}of {holdingsResult.evidence.availableItems}).
        </p>
      </div>
      <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
        <p className="text-sm font-medium text-foreground">Import progress</p>
        <ol className="mt-3 space-y-2 text-xs leading-5 text-muted-foreground">
          <li>
            1. Source file archived and normalized into immutable revision {" "}
            <span className="font-mono text-[10px] text-foreground">
              {shortIdentity(account.currentRevision.revisionId, "Revision")}
            </span>
            .
          </li>
          <li>
            2. {account.holdingCount.toLocaleString()} holdings and {" "}
            {account.transactionCount.toLocaleString()} source transactions are available to
            review.
          </li>
          <li>
            3. {account.reconciliationDiscrepancies === 0
              ? "Review the reconciliation explanation before relying on the totals."
              : "Review each reconciliation difference below, then import a corrected later source revision if needed."}
          </li>
        </ol>
      </div>
      <EvidenceNote icon={CircleAlert}>
        Each holding row shows its exact imported mark source and observation time. This portfolio
        source has not supplied a venue, market-freshness policy, or alternate mark authority, so
        Market Squawk does not promote the values to live, delayed, stale, or modeled marks.
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
            ? "Detailed reconciliation evidence is unavailable"
            : hasNoFindings
            ? "No supplied-versus-calculated discrepancy was retained"
            : `${account.reconciliationDiscrepancies} supplied total${
                account.reconciliationDiscrepancies === 1 ? "" : "s"
              } need review`}
        </div>
        {missingDetails ? (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            The service has not returned the source-total comparison rows for this revision. Do
            not treat the discrepancy count as a reconciliation result.
          </p>
        ) : !hasNoFindings ? (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            Each mismatch below keeps the source value, independently calculated value, tolerance,
            and raw source reference together. Correct the source export or import a later
            revision; the dashboard never overwrites either side.
          </p>
        ) : (
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            This says only that no retained comparison exceeded its declared tolerance. It does not
            establish that the source supplied every possible total.
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
              <p className="mt-2 break-all font-mono text-[10px] text-muted-foreground">
                Source {detail.sourceReference}
              </p>
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
    ["Source-reported cash", formatMoney(accounting.cash.amount)],
    ["Source-reported market value", formatMoney(accounting.reportedMarketValue)],
    ["Unrealized gain", accountingValue(accounting.unrealizedGain)],
    ["Realized gain", accountingValue(accounting.realizedGain)],
    ["Source-classified income", accountingValue(accounting.income)],
    ["Source-classified fees", accountingValue(accounting.fees)],
  ]
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <p className="font-medium text-sm">Accounting evidence</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Exact source values and calculated values remain separate. A gain is shown only after the
        import has enough basis and trade-lifecycle evidence to support it.
      </p>
      <dl className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {entries.map(([label, value]) => <Fact key={label} label={label} value={value} />)}
      </dl>
      <p className="mt-3 text-[11px] text-muted-foreground">
        Cash observed {formatTimestamp(accounting.cash.observedAtUnixNanos)} · source {accounting.cash.sourceReference}
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
