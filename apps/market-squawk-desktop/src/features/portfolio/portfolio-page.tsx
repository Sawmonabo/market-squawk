import * as React from "react"
import { AlertCircle, BriefcaseBusiness, ChevronDown, RefreshCw } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { PortfolioHistory } from "./portfolio-history"
import { PortfolioImportWorkflow } from "./portfolio-import-workflow"
import { PortfolioPlanning } from "./portfolio-planning"
import { PortfolioScenarios } from "./portfolio-scenarios"
import type { PortfolioAccount } from "./portfolio-contracts"
import { formatProductTime, portfolioDisplayName } from "./portfolio-format"
import { DataQualityPanel, PortfolioSummary } from "./portfolio-panels"
import { usePortfolioAccounts } from "./use-portfolio"

export function PortfolioPage() {
  const product = useProduct()

  if (product.status === "loading") {
    return (
      <PortfolioFrame>
        <PortfolioLoading />
      </PortfolioFrame>
    )
  }
  if (product.status === "error") {
    return (
      <PortfolioFrame>
        <PortfolioError retry={product.refresh} />
      </PortfolioFrame>
    )
  }

  return <PortfolioWorkspace bootstrap={product.bootstrap} transport={product.transport} />
}

function PortfolioWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const accounts = usePortfolioAccounts(transport, bootstrap)
  const rows = accounts.query.data ?? []
  const [selectedToken, setSelectedToken] = React.useState<string | null>(null)
  const selected =
    rows.find((account) => account.accountToken === selectedToken) ?? null

  React.useEffect(() => {
    if (
      selectedToken !== null &&
      !rows.some((account) => account.accountToken === selectedToken)
    ) {
      setSelectedToken(null)
    }
  }, [rows, selectedToken])

  return (
    <PortfolioFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Your financial position
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Portfolio</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review portfolio value, cash, investments, performance, and risk without combining
            separate accounts or currencies.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => void accounts.query.refetch()}
          disabled={accounts.query.isFetching}
        >
          <RefreshCw
            className={accounts.query.isFetching ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh
        </Button>
      </header>

      {!accounts.available ? (
        <UnavailablePortfolio />
      ) : accounts.query.isPending ? (
        <PortfolioLoading />
      ) : accounts.query.isError ? (
        <PortfolioError retry={() => void accounts.query.refetch()} />
      ) : rows.length === 0 ? (
        <EmptyPortfolio />
      ) : (
        <>
          <AccountDirectory
            accounts={rows}
            selectedToken={selectedToken}
            select={setSelectedToken}
          />
          {selected ? <SelectedPortfolio account={selected} /> : <SelectPortfolioPrompt />}
        </>
      )}

      <details className="group mt-5 rounded-xl border border-border bg-card/30 p-4">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          <span>Import or update portfolio details</span>
          <ChevronDown
            className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
            aria-hidden="true"
          />
        </summary>
        <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
          Review a portfolio file before saving any holdings, cash, transactions, or corrections.
          This page never chooses a portfolio for you.
        </p>
        <PortfolioImportWorkflow
          selectedPortfolioName={selected ? portfolioDisplayName(selected) : null}
        />
      </details>
    </PortfolioFrame>
  )
}

function SelectedPortfolio({ account }: { account: PortfolioAccount }) {
  return (
    <div className="mt-5 space-y-4">
      <PortfolioSummary account={account} />
      <Alert>
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Detailed analysis is unavailable</AlertTitle>
        <AlertDescription>
          Holdings, performance, exposure, and risk are hidden until complete named investments,
          exact values, dates, and safeguards are available together.
        </AlertDescription>
      </Alert>
      <details className="group rounded-xl border border-border bg-card/20 p-4">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          <span>History, stress tests, and planning</span>
          <ChevronDown
            className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
            aria-hidden="true"
          />
        </summary>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          No comparison, stress scenario, position change, or rebalance plan is selected by
          default. These tools cannot place an order.
        </p>
        <div className="mt-4 space-y-4">
          <PortfolioHistory />
          <div className="grid gap-4 2xl:grid-cols-2">
            <PortfolioScenarios choices={null} />
            <PortfolioPlanning positionChoices={null} rebalanceChoices={null} />
          </div>
          <DataQualityPanel account={account} />
        </div>
      </details>
    </div>
  )
}

function AccountDirectory({
  accounts,
  selectedToken,
  select,
}: {
  accounts: PortfolioAccount[]
  selectedToken: string | null
  select: (accountToken: string) => void
}) {
  return (
    <section
      className="mt-5 rounded-xl border border-border bg-card/35 p-5"
      aria-labelledby="portfolio-accounts"
    >
      <div>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Your portfolios
        </p>
        <h2 id="portfolio-accounts" className="mt-2 text-lg font-semibold">
          Choose a portfolio to inspect
        </h2>
        <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
          Selection controls only this page. Market Squawk does not choose the first portfolio on
          your behalf.
        </p>
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {accounts.map((account) => {
          const selected = selectedToken === account.accountToken
          return (
            <button
              key={account.accountToken}
              type="button"
              aria-pressed={selected}
              onClick={() => select(account.accountToken)}
              className={`rounded-lg border p-4 text-left transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary ${
                selected
                  ? "border-primary/60 bg-primary/10"
                  : "border-border bg-background/30 hover:border-primary/30 hover:bg-background/50"
              }`}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold">{account.portfolioName}</p>
                  {account.accountName !== account.portfolioName ? (
                    <p className="mt-1 text-xs text-muted-foreground">{account.accountName}</p>
                  ) : null}
                </div>
                <span className="rounded-md border border-border px-2 py-1 font-mono text-[10px]">
                  {account.reportingCurrency}
                </span>
              </div>
              <dl className="mt-4 grid grid-cols-2 gap-3 text-xs">
                <AccountFact label="Account type" value={account.accountTypeLabel} />
                <AccountFact label="Updated" value={formatProductTime(account.updatedAt)} />
                <AccountFact label="Positions" value={account.positionCount.toLocaleString()} />
                <AccountFact
                  label="Transactions"
                  value={account.transactionCount.toLocaleString()}
                />
              </dl>
              <p className="mt-4 text-[11px] font-medium text-primary">
                {selected ? "Selected" : "View this portfolio"}
              </p>
            </button>
          )
        })}
      </div>
    </section>
  )
}

function AccountFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate text-[11px]">{value}</dd>
    </div>
  )
}

function SelectPortfolioPrompt() {
  return (
    <section className="mt-5 rounded-xl border border-dashed border-border bg-card/20 p-7 text-center">
      <BriefcaseBusiness className="mx-auto size-6 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-lg font-semibold">Select a portfolio to view its details</h2>
      <p className="mx-auto mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        Separate portfolios and currencies are never silently combined.
      </p>
    </section>
  )
}

function PortfolioFrame({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto w-full max-w-[1280px] p-5 lg:p-7">{children}</div>
}

function PortfolioLoading() {
  return (
    <div className="mt-6 space-y-4" aria-label="Loading portfolio details">
      <Skeleton className="h-32 rounded-xl" />
      <div className="grid gap-4 xl:grid-cols-2">
        <Skeleton className="h-96 rounded-xl" />
        <Skeleton className="h-96 rounded-xl" />
      </div>
    </div>
  )
}

function PortfolioError({ retry }: { retry: () => void }) {
  return (
    <div className="mt-6">
      <Alert variant="destructive">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Portfolio unavailable</AlertTitle>
        <AlertDescription>
          Portfolio details cannot be loaded right now. Try again, or review Logs &amp;
          Diagnostics if the problem continues.
        </AlertDescription>
      </Alert>
      <Button className="mt-4" onClick={retry}>
        Try again
      </Button>
    </div>
  )
}

function EmptyPortfolio() {
  return (
    <section className="mt-6 rounded-xl border border-border bg-card/45 p-7">
      <BriefcaseBusiness className="size-6 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-lg font-semibold">No portfolio is available</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        Import is available only when Market Squawk can show the destination portfolio and every
        required review choice before saving.
      </p>
    </section>
  )
}

function UnavailablePortfolio() {
  return (
    <section className="mt-6 rounded-xl border border-border bg-card/45 p-7">
      <BriefcaseBusiness className="size-6 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-xl font-semibold">Portfolio unavailable</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        No account, cash balance, investment, or risk estimate is inferred when complete portfolio
        information is unavailable.
      </p>
    </section>
  )
}
