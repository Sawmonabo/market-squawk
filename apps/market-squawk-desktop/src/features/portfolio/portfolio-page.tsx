import * as React from "react"
import {
  AlertCircle,
  BriefcaseBusiness,
  ChevronDown,
  RefreshCw,
} from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { HoldingTable } from "./holding-table"
import { PortfolioHistory } from "./portfolio-history"
import { PortfolioPlanning } from "./portfolio-planning"
import { PortfolioScenarios } from "./portfolio-scenarios"
import type { PortfolioAccount } from "./portfolio-contracts"
import { formatTimestamp, shortIdentity } from "./portfolio-format"
import { PortfolioImportWorkflow } from "./portfolio-import-workflow"
import {
  AllocationPanel,
  ExposurePanel,
  FinancialPositionCoverage,
  PerformancePanel,
  PortfolioSummary,
  DataQualityPanel,
  RecommendationSetupPanel,
  ReconciliationPanel,
  RiskPanel,
} from "./portfolio-panels"
import { usePortfolioAccounts, usePortfolioDetails } from "./use-portfolio"

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
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Portfolio workspace unavailable</AlertTitle>
          <AlertDescription>
            Portfolio details cannot be loaded right now. Try again, or review Logs &amp;
            Diagnostics if the problem continues.
          </AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </PortfolioFrame>
    )
  }

  return (
    <PortfolioWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function PortfolioWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const accounts = usePortfolioAccounts(transport, bootstrap)
  const rows = accounts.query.data?.pages.flatMap((page) => page.value) ?? []
  const [selectedId, setSelectedId] = React.useState<string | null>(null)
  const selected = rows.find((account) => account.accountId === selectedId) ?? null
  const details = usePortfolioDetails(
    transport,
    bootstrap.runtime,
    bootstrap,
    selected?.accountId ?? null,
  )

  React.useEffect(() => {
    if (
      selectedId !== null &&
      !rows.some((account) => account.accountId === selectedId)
    ) {
      setSelectedId(null)
    }
  }, [rows, selectedId])

  if (!accounts.available) {
    return (
      <PortfolioFrame>
        <UnavailablePortfolio />
        <PortfolioImportWorkflow
          bootstrap={bootstrap}
          selectedAccountId={null}
          onCommitted={() => accounts.query.refetch()}
        />
      </PortfolioFrame>
    )
  }

  const refresh = () => {
    void accounts.query.refetch()
    void details.refresh()
  }

  return (
    <PortfolioFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Your supported financial position
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Portfolio</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review every returned account and asset without assuming it is a stock portfolio.
            Market Squawk keeps imported investments, account cash, missing bank coverage, and
            unavailable liabilities clearly separated.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={refresh}
          disabled={accounts.query.isFetching || details.isFetching}
        >
          <RefreshCw
            className={accounts.query.isFetching || details.isFetching ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh
        </Button>
      </header>

      {accounts.query.isPending ? (
        <PortfolioLoading />
      ) : accounts.query.isError ? (
        <PortfolioError
          title="Accounts could not be read"
          message="Your accounts could not be loaded right now. Try again."
          retry={() => void accounts.query.refetch()}
        />
      ) : rows.length === 0 ? (
        <>
          <FinancialPositionCoverage
            accounts={rows}
            holdingsAvailable={hasProductCapability(bootstrap, "portfolio_holdings")}
            performanceAvailable={hasProductCapability(bootstrap, "portfolio_performance")}
            transactionsAvailable={hasProductCapability(bootstrap, "portfolio_transactions")}
          />
          <EmptyPortfolio />
        </>
      ) : (
        <>
          <FinancialPositionCoverage
            accounts={rows}
            holdingsAvailable={hasProductCapability(bootstrap, "portfolio_holdings")}
            performanceAvailable={hasProductCapability(bootstrap, "portfolio_performance")}
            transactionsAvailable={hasProductCapability(bootstrap, "portfolio_transactions")}
          />
          <AccountDirectory
            accounts={rows}
            selectedId={selectedId}
            select={setSelectedId}
            loadMore={
              accounts.query.hasNextPage
                ? () => void accounts.query.fetchNextPage()
                : null
            }
            loadingMore={accounts.query.isFetchingNextPage}
          />
          <RecommendationSetupPanel selectedAccount={selected} />
          {selected ? (
            <DetailWorkspace
              account={selected}
              details={details}
              bootstrap={bootstrap}
              transport={transport}
            />
          ) : (
            <SelectAccountPrompt />
          )}
        </>
      )}

      <details className="group mt-5 rounded-xl border border-border bg-card/30 p-4">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          <span>Import or update account details</span>
          <ChevronDown
            className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
            aria-hidden="true"
          />
        </summary>
        <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
          Import an account file to review its holdings, cash, transactions, and reconciliation
          results. It does not connect a bank or choose an account for recommendations.
        </p>
        <PortfolioImportWorkflow
          bootstrap={bootstrap}
          selectedAccountId={selected?.accountId ?? null}
          onCommitted={async () => {
            await accounts.query.refetch()
            await details.refresh()
          }}
        />
      </details>
    </PortfolioFrame>
  )
}

function DetailWorkspace({
  account,
  details,
  bootstrap,
  transport,
}: {
  account: PortfolioAccount
  details: ReturnType<typeof usePortfolioDetails>
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const holdings = details.holdings.data?.value ?? null
  const performance = details.performance.data?.value ?? null
  const missing = Object.entries(details.capabilityAvailable)
    .filter(([, available]) => !available)
    .map(([capability]) => detailName(capability as ProductCapability))
  const hasDetailError = [
    details.holdings.error,
    details.performance.error,
    details.exposure.error,
    details.risk.error,
  ].some(Boolean)

  return (
    <div className="mt-5 space-y-4">
      {missing.length ? (
        <Alert>
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Some portfolio details are unavailable</AlertTitle>
          <AlertDescription>
            {missing.join(", ")} are not available right now. The available sections below remain
            usable.
          </AlertDescription>
        </Alert>
      ) : null}
      {hasDetailError ? (
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Some portfolio sections could not be refreshed</AlertTitle>
          <AlertDescription>
            Some information could not be loaded right now. Try refreshing; detailed diagnostics
            are available in Logs &amp; Diagnostics.
          </AlertDescription>
        </Alert>
      ) : null}

      <PortfolioSummary
        account={account}
        holdings={holdings}
        performance={performance}
      />

      {(details.capabilityAvailable.portfolio_holdings && details.holdings.isPending) ||
      (details.capabilityAvailable.portfolio_performance && details.performance.isPending) ? (
        <div className="grid gap-4 xl:grid-cols-2">
          <Skeleton className="h-96 rounded-xl" />
          <Skeleton className="h-96 rounded-xl" />
        </div>
      ) : (
        <div className="grid gap-4 xl:grid-cols-2">
          {holdings ? <AllocationPanel holdings={holdings} /> : null}
          {performance ? <PerformancePanel performance={performance} /> : null}
        </div>
      )}

      <section className="rounded-xl border border-border bg-card/35 p-5">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Your account details
          </p>
          <h2 className="mt-2 text-lg font-semibold">Holdings</h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Market value, quantity, cost-basis status, and the latest available details for every
            asset in this account.
          </p>
        </div>
        <div className="mt-5">
          {details.capabilityAvailable.portfolio_holdings && details.holdings.isPending ? (
            <Skeleton className="h-72 rounded-lg" />
          ) : holdings ? (
            <HoldingTable holdings={holdings} />
          ) : (
            <InlineUnavailable text="Holding records are unavailable for this account." />
          )}
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-2">
        {details.capabilityAvailable.portfolio_exposure && details.exposure.isPending ? (
          <Skeleton className="h-80 rounded-xl" />
        ) : details.exposure.data ? (
          <ExposurePanel exposure={details.exposure.data.value} />
        ) : (
          <InlineUnavailable text="Exposure analysis is unavailable for this account." />
        )}
        {details.capabilityAvailable.portfolio_risk && details.risk.isPending ? (
          <Skeleton className="h-80 rounded-xl" />
        ) : details.risk.data ? (
          <RiskPanel risk={details.risk.data.value} />
        ) : (
          <InlineUnavailable text="Risk analysis is unavailable for this account." />
        )}
      </div>

      <details className="group rounded-xl border border-border bg-card/20 p-4">
        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          <span>History, stress tests, and planning</span>
          <ChevronDown
            className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
            aria-hidden="true"
          />
        </summary>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          Compare earlier account snapshots, run stress tests, and explore changes before making a
          decision. None of these controls can place an order.
        </p>
        <div className="mt-4 space-y-4">
          <PortfolioHistory account={account} bootstrap={bootstrap} transport={transport} />

          <div className="grid gap-4 2xl:grid-cols-2">
            <PortfolioScenarios
              account={account}
              holdings={holdings ?? []}
              bootstrap={bootstrap}
              transport={transport}
            />
            <PortfolioPlanning
              account={account}
              holdings={holdings ?? []}
              bootstrap={bootstrap}
              transport={transport}
            />
          </div>

          <div className="grid gap-4 xl:grid-cols-2">
            {details.holdings.data ? (
              <DataQualityPanel account={account} holdingsResult={details.holdings.data} />
            ) : (
              <InlineUnavailable text="Market-price details are unavailable without holdings." />
            )}
            <ReconciliationPanel
              account={account}
              performance={details.performance.data?.value ?? null}
            />
          </div>
        </div>
      </details>
    </div>
  )
}

function detailName(capability: ProductCapability) {
  switch (capability) {
    case "portfolio_holdings":
      return "holding details"
    case "portfolio_performance":
      return "performance history"
    case "portfolio_exposure":
      return "exposure analysis"
    case "portfolio_risk":
      return "risk analysis"
    default:
      return "a required portfolio view"
  }
}

function AccountDirectory({
  accounts,
  selectedId,
  select,
  loadMore,
  loadingMore,
}: {
  accounts: PortfolioAccount[]
  selectedId: string | null
  select: (accountId: string) => void
  loadMore: (() => void) | null
  loadingMore: boolean
}) {
  return (
    <section
      className="mt-5 rounded-xl border border-border bg-card/35 p-5"
      aria-labelledby="portfolio-accounts"
    >
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Returned accounts
          </p>
          <h2 id="portfolio-accounts" className="mt-2 text-lg font-semibold">
            Choose an account to inspect
          </h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground">
            Selection controls only this page. It does not select a recommendation account, and
            Market Squawk never chooses the first returned account on your behalf.
          </p>
        </div>
        {loadMore ? (
          <Button variant="outline" size="sm" onClick={loadMore} disabled={loadingMore}>
            {loadingMore ? "Loading…" : "Load more accounts"}
          </Button>
        ) : null}
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {accounts.map((account) => {
          const selected = selectedId === account.accountId
          return (
            <button
              key={account.accountId}
              type="button"
              aria-pressed={selected}
              onClick={() => select(account.accountId)}
              className={`rounded-lg border p-4 text-left transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary ${
                selected
                  ? "border-primary/60 bg-primary/10"
                  : "border-border bg-background/30 hover:border-primary/30 hover:bg-background/50"
              }`}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold">
                    {shortIdentity(account.accountId, "Account")}
                  </p>
                </div>
                <span className="rounded-md border border-border px-2 py-1 font-mono text-[10px]">
                  {account.currency.toUpperCase()}
                </span>
              </div>
              <dl className="mt-4 grid grid-cols-2 gap-3 text-xs">
                <AccountFact label="Account type" value="Not supplied" />
                <AccountFact
                  label="Updated"
                  value={formatTimestamp(account.currentSnapshot.effectiveAtUnixNanos)}
                />
                <AccountFact label="Assets" value={account.holdingCount.toLocaleString()} />
                <AccountFact
                  label="Transactions"
                  value={account.transactionCount.toLocaleString()}
                />
              </dl>
              <p className="mt-4 text-[11px] font-medium text-primary">
                {selected ? "Selected for inspection" : "Inspect this account"}
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
      <dd className="mt-1 truncate font-mono text-[11px]">{value}</dd>
    </div>
  )
}

function SelectAccountPrompt() {
  return (
    <section className="mt-5 rounded-xl border border-dashed border-border bg-card/20 p-7 text-center">
      <BriefcaseBusiness className="mx-auto size-6 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-lg font-semibold">Select an account to view its details</h2>
      <p className="mx-auto mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        Market Squawk will load holdings, account cash, performance, exposure, and risk only after
        you make an explicit selection. Accounts in different currencies are never silently
        combined.
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

function PortfolioError({
  title,
  message,
  retry,
}: {
  title: string
  message: string
  retry: () => void
}) {
  return (
    <div className="mt-6">
      <Alert variant="destructive">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>{title}</AlertTitle>
        <AlertDescription>{message}</AlertDescription>
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
      <h2 className="mt-4 text-lg font-semibold">No account has been imported</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        Open{" "}
        <span className="font-medium text-foreground">Import or update account details</span>
        {" "}below to select an account file, review the imported transactions, and confirm any
        reconciliation differences.
      </p>
    </section>
  )
}

function UnavailablePortfolio() {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-7">
      <BriefcaseBusiness className="size-6 text-muted-foreground" aria-hidden="true" />
      <h1 className="mt-4 text-xl font-semibold">Portfolio unavailable</h1>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        Portfolio details cannot be loaded right now. No account or balance is inferred from files,
        paper activity, or another workspace.
      </p>
    </section>
  )
}

function InlineUnavailable({ text }: { text: string }) {
  return (
    <div className="flex min-h-40 items-center justify-center rounded-xl border border-dashed border-border bg-card/20 p-6 text-center text-sm text-muted-foreground">
      {text}
    </div>
  )
}
