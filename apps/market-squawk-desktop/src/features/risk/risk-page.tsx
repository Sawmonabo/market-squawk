import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  CircleAlert,
  DatabaseZap,
  Gauge,
  RefreshCw,
  Scale,
  ShieldAlert,
  ShieldCheck,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { RiskChart, type RiskChartValue } from "@/components/charts/risk-chart"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  type PortfolioAccountRiskSummary,
  type PortfolioRiskReport,
  parseRiskAccounts,
  parseRiskReport,
} from "./contracts"

export function RiskPage() {
  const product = useProduct()

  if (product.status === "loading") return <RiskLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState title="Risk evidence is unavailable" detail={product.error} />
      </PageFrame>
    )
  }

  return <ReadyRiskPage bootstrap={product.bootstrap} transport={product.transport} />
}

function ReadyRiskPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const accounts = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Portfolio",
      "Portfolio.ListAccounts",
      {},
    ),
    queryFn: async () =>
      parseRiskAccounts(await transport.query({ query: "portfolioAccounts" })),
  })
  const availableAccounts = accounts.data?.value ?? []
  const [selectedAccount, setSelectedAccount] = React.useState<string | null>(null)
  const accountId = availableAccounts.some((account) => account.accountId === selectedAccount)
    ? selectedAccount
    : availableAccounts[0]?.accountId ?? null

  return (
    <PageFrame
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => void accounts.refetch()}
          disabled={accounts.isFetching}
        >
          <RefreshCw className={accounts.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Refresh accounts
        </Button>
      }
    >
      {accounts.isLoading ? (
        <RiskGridLoading />
      ) : accounts.isError ? (
        <EmptyState title="Portfolio risk could not be read" detail={messageFrom(accounts.error)} />
      ) : availableAccounts.length === 0 ? (
        <EmptyState
          title="No portfolio risk is available"
          detail="Import and publish a portfolio account before requesting account-scoped risk evidence."
        />
      ) : (
        <>
          <div className="flex flex-wrap items-end justify-between gap-4 rounded-xl border border-border bg-card/45 p-4">
            <div>
              <label htmlFor="risk-account" className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Portfolio account
              </label>
              <select
                id="risk-account"
                value={accountId ?? ""}
                onChange={(event) => setSelectedAccount(event.target.value)}
                className="mt-2 block min-w-64 rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                {availableAccounts.map((account) => (
                  <option key={account.accountId} value={account.accountId}>
                    {account.accountId} · {account.currency}
                  </option>
                ))}
              </select>
            </div>
            <p className="max-w-xl text-xs leading-5 text-muted-foreground">
              This view is bound to the account&apos;s current immutable portfolio revision. It does not treat historical portfolio risk as an execution approval.
            </p>
          </div>
          {accountId ? (
            <AccountRisk
              key={accountId}
              account={availableAccounts.find((account) => account.accountId === accountId)!}
              bootstrap={bootstrap}
              transport={transport}
            />
          ) : null}
          <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
            Account listing: {countBoundary(accounts.data)}. Central pre-trade risk decisions and limits are not inferred from this portfolio analytics report.
          </p>
        </>
      )}
    </PageFrame>
  )
}

function AccountRisk({
  account,
  bootstrap,
  transport,
}: {
  account: PortfolioAccountRiskSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const risk = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Portfolio",
      "Portfolio.GetRisk",
      { accountId: account.accountId },
    ),
    queryFn: async () =>
      parseRiskReport(
        await transport.query({
          query: "portfolioRisk",
          accountId: account.accountId,
        }),
      ),
  })

  if (risk.isLoading) {
    return (
      <div className="mt-4">
        <RiskGridLoading />
      </div>
    )
  }
  if (risk.isError) {
    return (
      <div className="mt-4">
        <EmptyState title="This account&apos;s risk could not be read" detail={messageFrom(risk.error)} />
      </div>
    )
  }
  if (!risk.data) {
    return (
      <div className="mt-4">
        <RiskGridLoading />
      </div>
    )
  }

  const report = risk.data.value
  return (
    <div className="mt-4 space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Summary label="Confidence" value={formatPercent(report.confidence)} icon={Gauge} />
        <Summary
          label="Observations"
          value={report.observations?.toLocaleString() ?? "Not available"}
          icon={Scale}
        />
        <Summary
          label="Holdings"
          value={account.holdingCount.toLocaleString()}
          icon={ShieldCheck}
        />
        <Summary
          label="Reconciliation breaks"
          value={account.reconciliationDiscrepancies.toLocaleString()}
          icon={ShieldAlert}
          bad={account.reconciliationDiscrepancies > 0}
        />
      </div>
      <div className="grid gap-4 xl:grid-cols-[1.35fr_1fr]">
        <RiskMeasures report={report} />
        <ScenarioPanel report={report} account={account} />
      </div>
      <EvidencePanel report={report} result={risk.data} />
    </div>
  )
}

function RiskMeasures({ report }: { report: PortfolioRiskReport }) {
  const values: RiskChartValue[] = [
    report.valueAtRisk === undefined
      ? null
      : { label: "Value at risk", value: report.valueAtRisk, color: "var(--primary)" },
    report.expectedShortfall === undefined
      ? null
      : { label: "Expected shortfall", value: report.expectedShortfall, color: "var(--warning)" },
    report.annualizedVolatility === undefined
      ? null
      : { label: "Volatility", value: report.annualizedVolatility, color: "var(--success)" },
  ].filter((value): value is RiskChartValue => value !== null)

  return (
    <Panel title="Measured risk" subtitle="Historical return risk from the selected immutable revision.">
      {values.length > 0 ? (
        <RiskChart values={values} />
      ) : (
        <InlineEmpty
          detail={
            report.historyStatus
              ? `Risk measures are unavailable: ${humanize(report.historyStatus)}.`
              : "The service did not return historical risk measures for this revision."
          }
        />
      )}
    </Panel>
  )
}

function ScenarioPanel({
  report,
  account,
}: {
  report: PortfolioRiskReport
  account: PortfolioAccountRiskSummary
}) {
  return (
    <Panel title="Standard stress" subtitle="A deterministic scenario, kept separate from forecasts.">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {humanize(report.scenario.id)}
      </p>
      <p className="mt-3 font-mono text-2xl font-semibold">
        {report.scenario.impact
          ? formatMoney(report.scenario.impact)
          : report.scenario.status
            ? humanize(report.scenario.status)
            : "Not reported"}
      </p>
      <dl className="mt-5 grid gap-3 border-t border-border/70 pt-4 sm:grid-cols-2 xl:grid-cols-1">
        <Fact label="Account currency" value={account.currency} />
        <Fact label="Policy" value={humanize(report.policy)} />
        <Fact label="Revision" value={shortDigest(report.revisionId)} />
        <Fact
          label="Tracking error"
          value={report.trackingErrorStatus ? humanize(report.trackingErrorStatus) : "Not reported"}
        />
      </dl>
    </Panel>
  )
}

function EvidencePanel({
  report,
  result,
}: {
  report: PortfolioRiskReport
  result: { completeness: string; returnedItems: number; availableItems: number }
}) {
  const limitations = [report.historyStatus, report.volatilityStatus]
    .filter((value): value is string => Boolean(value))
    .map(humanize)
  return (
    <section className="rounded-xl border border-border bg-card/35 p-4">
      <div className="flex gap-3">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Evidence boundary</h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {result.completeness}; {result.returnedItems} of {result.availableItems} result items. Effective {timeFromNanos(report.effectiveAtUnixNanos)}; available {timeFromNanos(report.availableAtUnixNanos)}.
          </p>
          {limitations.length > 0 ? (
            <p className="mt-2 text-xs text-amber-200">Limitations: {limitations.join(" · ")}</p>
          ) : null}
        </div>
      </div>
    </section>
  )
}

function Summary({
  label,
  value,
  icon: Icon,
  bad = false,
}: {
  label: string
  value: string
  icon: typeof Gauge
  bad?: boolean
}) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon className={bad ? "size-4 text-rose-400" : "size-4 text-primary"} aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold">{value}</p>
    </div>
  )
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <h2 className="text-base font-semibold">{title}</h2>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{subtitle}</p>
      <div className="mt-5">{children}</div>
    </section>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1280px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Portfolio analytics
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Risk</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Revision-bound historical risk and deterministic stress evidence, without converting analytics into execution authority.
          </p>
        </div>
        {action}
      </div>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <DatabaseZap className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function InlineEmpty({ detail }: { detail: string }) {
  return (
    <div className="rounded-lg border border-dashed border-border p-5 text-sm text-muted-foreground">
      {detail}
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-xs">{value}</dd>
    </div>
  )
}

function RiskLoading() {
  return (
    <PageFrame>
      <RiskGridLoading />
    </PageFrame>
  )
}

function RiskGridLoading() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {Array.from({ length: 4 }, (_, index) => (
          <Skeleton key={index} className="h-28 rounded-xl" />
        ))}
      </div>
      <div className="grid gap-4 xl:grid-cols-[1.35fr_1fr]">
        <Skeleton className="h-80 rounded-xl" />
        <Skeleton className="h-80 rounded-xl" />
      </div>
    </div>
  )
}

function formatPercent(value: number) {
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 2,
  }).format(value)
}

function countBoundary(
  value: { completeness: string; returnedItems: number; availableItems: number } | undefined,
) {
  return value
    ? `${value.completeness}, ${value.returnedItems} of ${value.availableItems} returned`
    : "unavailable"
}

function shortDigest(value: string) {
  return value.length > 18 ? `${value.slice(0, 10)}…${value.slice(-6)}` : value
}

function timeFromNanos(value: string | null | undefined) {
  if (!value) return "not reported"
  try {
    const milliseconds = BigInt(value) / 1_000_000n
    const date = new Date(Number(milliseconds))
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
  } catch {
    return value
  }
}
