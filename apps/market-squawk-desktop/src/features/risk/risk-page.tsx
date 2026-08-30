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

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney } from "@/lib/formatters"
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
        <EmptyState
          title="Risk guidance is unavailable"
          detail="Try again. If the problem continues, review the app setup before relying on these estimates."
        />
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
      bootstrap.productSessionToken,
      "portfolio",
      "Portfolio.ListAccounts",
      {},
    ),
    queryFn: async () =>
      parseRiskAccounts(await transport.query({ query: "portfolioAccounts" })),
  })
  const availableAccounts = accounts.data?.value ?? []
  const [selectedIndex, setSelectedIndex] = React.useState("")
  const index = parseSelectedIndex(selectedIndex, availableAccounts.length)
  const selected = index === null ? null : availableAccounts[index] ?? null

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
          Refresh
        </Button>
      }
    >
      <RiskBoundary />
      {accounts.isLoading ? (
        <RiskGridLoading />
      ) : accounts.isError ? (
        <EmptyState
          title="Portfolio risk could not be opened"
          detail="Try refreshing. If the problem continues, review the portfolio setup."
        />
      ) : availableAccounts.length === 0 ? (
        <EmptyState
          title="No portfolio risk is available"
          detail="Import a portfolio account to review its risk and decision context."
        />
      ) : (
        <>
          <div className="rounded-xl border border-border bg-card/45 p-4">
            <label
              htmlFor="risk-account"
              className="text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              Portfolio
            </label>
            <select
              id="risk-account"
              value={selectedIndex}
              onChange={(event) => setSelectedIndex(event.target.value)}
              className="mt-2 block min-w-64 rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">Select a portfolio</option>
              {availableAccounts.map((account, accountIndex) => (
                <option key={`${account.displayName}:${accountIndex}`} value={String(accountIndex)}>
                  {account.displayName} · {account.currency}
                </option>
              ))}
            </select>
            <p className="mt-3 max-w-3xl text-xs leading-5 text-muted-foreground">
              Selecting a portfolio only opens its latest risk guidance. It does not change the
              portfolio, approve a trade, or start paper trading.
            </p>
          </div>

          {selected ? (
            <AccountRisk
              key={selected.accountToken}
              account={selected}
              bootstrap={bootstrap}
              transport={transport}
            />
          ) : (
            <div className="mt-4">
              <EmptyState
                title="Choose a portfolio"
                detail="Market Squawk will show its action, horizon, ranges, reasons, risks, assumptions, invalidators, and uncertainty."
              />
            </div>
          )}
          <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
            Showing {accounts.data?.returnedItems ?? 0} of {accounts.data?.availableItems ?? 0}{" "}
            portfolios.
            Risk guidance informs a decision but never approves or places a trade.
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
      bootstrap.productSessionToken,
      "portfolio",
      "Portfolio.GetRisk",
      { accountToken: account.accountToken },
    ),
    queryFn: async () =>
      parseRiskReport(
        await transport.query({
          query: "portfolioRisk",
          accountToken: account.accountToken,
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
        <EmptyState
          title="This portfolio&apos;s risk guidance could not be opened"
          detail="Try again. If the problem continues, refresh the portfolio before relying on these estimates."
        />
      </div>
    )
  }
  if (!risk.data) return null

  return (
    <div className="mt-4 space-y-4">
      <Recommendation report={risk.data.value} />
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Summary
          label="Evidence coverage"
          value={coverageLabel(risk.data.value.coverage.state)}
          icon={Gauge}
          bad={risk.data.value.coverage.state === "unavailable"}
        />
        <Summary
          label="Observations"
          value={risk.data.value.coverage.observations.toLocaleString()}
          icon={Scale}
        />
        <Summary label="Holdings" value={account.holdings.toLocaleString()} icon={ShieldCheck} />
        <Summary
          label="Portfolio data issues"
          value={account.dataIssues.toLocaleString()}
          icon={ShieldAlert}
          bad={account.dataIssues > 0}
        />
      </div>
      <div className="grid gap-4 xl:grid-cols-[1.35fr_1fr]">
        <RiskMeasures report={risk.data.value} />
        <StressPanel report={risk.data.value} />
      </div>
      <EvidencePanel report={risk.data.value} result={risk.data} />
    </div>
  )
}

function Recommendation({ report }: { report: PortfolioRiskReport }) {
  const recommendation = report.recommendation
  return (
    <section className="rounded-xl border-2 border-primary/35 bg-primary/[0.06] p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
            Portfolio risk guidance
          </p>
          <h2 className="mt-2 text-2xl font-semibold">{actionLabel(recommendation.action)}</h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            {recommendation.summary}
          </p>
        </div>
        <div className="rounded-lg border border-border bg-background/35 px-4 py-3 text-right">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Horizon</p>
          <p className="mt-1 text-sm font-semibold">{recommendation.horizon}</p>
        </div>
      </div>

      {recommendation.ranges.length > 0 ? (
        <dl className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {recommendation.ranges.map((range) => (
            <Fact
              key={range.label}
              label={range.label}
              value={`${formatMoney(range.lower)} to ${formatMoney(range.upper)}`}
            />
          ))}
        </dl>
      ) : null}

      <div className="mt-5 grid gap-4 lg:grid-cols-2">
        <TextList title="Why" items={recommendation.reasons} />
        <TextList title="Main risks" items={recommendation.risks} />
        <TextList title="Assumptions" items={recommendation.assumptions} />
        <TextList title="What would invalidate this guidance" items={recommendation.invalidators} />
      </div>

      <div className="mt-5 rounded-lg border border-border bg-background/35 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="text-sm font-semibold">
            Uncertainty: {uncertaintyLabel(recommendation.uncertainty.level)}
          </p>
          <p className="text-xs text-muted-foreground">
            {recommendation.validity.state === "available"
              ? `Review by ${formatProductTimestamp(recommendation.validity.expiresAt)}`
              : recommendation.validity.explanation}
          </p>
        </div>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          {recommendation.uncertainty.explanation}
        </p>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Fact
            label="Out-of-sample evidence"
            value={outOfSampleStateLabel(recommendation.uncertainty.outOfSampleEvidence)}
          />
          <Fact
            label="Calibration"
            value={calibrationStateLabel(recommendation.uncertainty.calibration)}
          />
          <Fact
            label="Trading costs"
            value={costStateLabel(recommendation.uncertainty.tradingCosts)}
          />
          <Fact
            label="Point-in-time inputs"
            value={pointInTimeLabel(recommendation.uncertainty.pointInTimeInputs)}
          />
        </dl>
      </div>
    </section>
  )
}

function RiskMeasures({ report }: { report: PortfolioRiskReport }) {
  return (
    <Panel
      title="Measured risk"
      subtitle={`Exact percentages for ${report.horizon}; unavailable measures remain unavailable.`}
    >
      <div className="space-y-3">
        {report.measures.map((measure) => (
          <article key={measure.label} className="rounded-lg border border-border bg-background/35 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="text-sm font-semibold">{measure.label}</p>
              <p className="text-lg font-semibold tabular-nums">
                {measure.value ?? measureStatusLabel(measure.status)}
              </p>
            </div>
            <p className="mt-2 text-xs leading-5 text-muted-foreground">{measure.explanation}</p>
          </article>
        ))}
      </div>
    </Panel>
  )
}

function StressPanel({ report }: { report: PortfolioRiskReport }) {
  return (
    <Panel title="Stress check" subtitle="A downside scenario kept separate from forecasts.">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {report.stress.label}
      </p>
      <p className="mt-3 text-2xl font-semibold">
        {report.stress.impact
          ? formatMoney(report.stress.impact)
          : stressStatusLabel(report.stress.status)}
      </p>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {report.stress.explanation}
      </p>
      <TextList title="Scenario assumptions" items={report.stress.assumptions} />
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
  return (
    <section className="rounded-xl border border-border bg-card/35 p-4">
      <div className="flex gap-3">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Coverage and timing</h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {report.coverage.explanation} Period: {report.coverage.period}. Risk period ends{" "}
            {formatProductTimestamp(report.asOf)}; updated{" "}
            {formatProductTimestamp(report.availableAt)}.
          </p>
          <p className="mt-2 text-[10px] text-muted-foreground">
            Showing {result.returnedItems} of {result.availableItems} available risk results;{" "}
            {result.completeness === "complete" ? "complete" : "partial"} coverage.
          </p>
        </div>
      </div>
    </section>
  )
}

function RiskBoundary() {
  return (
    <section className="mb-4 rounded-xl border border-border bg-card/35 p-4">
      <div className="flex gap-3">
        <ShieldAlert className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Decision support, not trade approval</h2>
          <p className="mt-1 max-w-4xl text-xs leading-5 text-muted-foreground">
            This page explains portfolio-level action guidance and uncertainty. It cannot change a
            portfolio, alter safeguards, start paper trading, or approve an order.
          </p>
        </div>
      </div>
    </section>
  )
}

function TextList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="rounded-lg border border-border bg-background/35 p-4">
      <h3 className="text-xs font-semibold">{title}</h3>
      <ul className="mt-2 space-y-2 text-xs leading-5 text-muted-foreground">
        {items.map((item) => (
          <li key={item}>• {item}</li>
        ))}
      </ul>
    </div>
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
            Market Squawk · Decision support
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Risk &amp; Guidance</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Understand what to do, over what horizon, why, what could go wrong, and how much
            uncertainty remains before changing an investment plan.
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

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs">{value}</dd>
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

function parseSelectedIndex(value: string, length: number): number | null {
  if (!/^\d+$/.test(value)) return null
  const index = Number(value)
  return Number.isSafeInteger(index) && index >= 0 && index < length ? index : null
}

function actionLabel(value: PortfolioRiskReport["recommendation"]["action"]): string {
  switch (value) {
    case "buy":
      return "Buy"
    case "add":
      return "Add"
    case "hold":
      return "Hold"
    case "trim":
      return "Trim"
    case "sell":
      return "Sell"
    case "abstain":
      return "Wait — evidence is not strong enough"
  }
}

function uncertaintyLabel(
  value: PortfolioRiskReport["recommendation"]["uncertainty"]["level"],
): string {
  switch (value) {
    case "low":
      return "Lower"
    case "moderate":
      return "Moderate"
    case "high":
      return "High"
    case "unavailable":
      return "Not established"
  }
}

function coverageLabel(value: PortfolioRiskReport["coverage"]["state"]): string {
  switch (value) {
    case "complete":
      return "Complete"
    case "partial":
      return "Partial"
    case "unavailable":
      return "Unavailable"
  }
}

function measureStatusLabel(value: PortfolioRiskReport["measures"][number]["status"]): string {
  switch (value) {
    case "available":
      return "Available"
    case "insufficient_history":
      return "Not enough history"
    case "unavailable":
      return "Unavailable"
  }
}

function stressStatusLabel(value: PortfolioRiskReport["stress"]["status"]): string {
  switch (value) {
    case "available":
      return "Available"
    case "incomplete":
      return "Incomplete"
    case "unavailable":
      return "Unavailable"
  }
}

function outOfSampleStateLabel(value: "sufficient" | "limited" | "unavailable"): string {
  switch (value) {
    case "sufficient":
      return "Enough evidence"
    case "limited":
      return "Limited"
    case "unavailable":
      return "Unavailable"
  }
}

function calibrationStateLabel(value: "supported" | "limited" | "unavailable"): string {
  switch (value) {
    case "supported":
      return "Supported by evidence"
    case "limited":
      return "Limited"
    case "unavailable":
      return "Unavailable"
  }
}

function costStateLabel(value: "included" | "partial" | "unavailable"): string {
  switch (value) {
    case "included":
      return "Included"
    case "partial":
      return "Partially included"
    case "unavailable":
      return "Unavailable"
  }
}

function pointInTimeLabel(value: "supported" | "partial" | "unavailable"): string {
  switch (value) {
    case "supported":
      return "Supported"
    case "partial":
      return "Partial"
    case "unavailable":
      return "Unavailable"
  }
}

function formatProductTimestamp(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? "Unavailable" : date.toLocaleString()
}
