import { RefreshButton } from "@/components/ui/refresh-button"
import * as React from "react"
import { ShieldAlert } from "lucide-react"

import { CursorNavigation } from "../shared/cursor-navigation"
import { usePortfolioAccounts } from "../portfolio/use-portfolio"

import { useProduct } from "@/app/product-context"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { AccountRisk, RiskEmptyState, RiskGridLoading } from "./account-risk"

export function RiskPage() {
  const product = useProduct()

  if (product.status === "loading") return <RiskLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <RiskEmptyState
          title="Risk guidance is unavailable"
          detail="Try again. If the problem continues, review the app setup before relying on these estimates."
        />
      </PageFrame>
    )
  }

  return (
    <ReadyRiskPage
      key={product.bootstrap.productSessionToken}
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadyRiskPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const accountDirectory = usePortfolioAccounts(transport, bootstrap)
  const accounts = accountDirectory.query
  const availableAccounts = accounts.data?.accounts ?? []
  const [selectedToken, setSelectedToken] = React.useState("")
  const selected = availableAccounts.find((account) => account.accountToken === selectedToken) ?? null

  return (
    <PageFrame
      action={
        <RefreshButton label="Refresh" refreshing={accounts.isFetching} onClick={() => void accounts.refetch()}
          disabled={accounts.isFetching} />
      }
    >
      <RiskBoundary />
      <CursorNavigation navigation={accountDirectory.navigation} next={accounts.data?.nextCursor} busy={accounts.isFetching} error={accounts.isError}
        onNavigate={() => setSelectedToken("")} onRestart={() => { if (accountDirectory.navigation.after === undefined) void accounts.refetch() }} />
      {accounts.isLoading ? (
        <RiskGridLoading />
      ) : accounts.isError ? (
        <RiskEmptyState
          title="Portfolio risk could not be opened"
          detail="Try refreshing. If the problem continues, review the portfolio setup."
        />
      ) : availableAccounts.length === 0 ? (
        <RiskEmptyState
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
              value={selected?.accountToken ?? ""}
              onChange={(event) => setSelectedToken(event.target.value)}
              className="mt-2 block min-w-64 rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">Select a portfolio</option>
              {availableAccounts.map((account) => (
                <option key={account.accountToken} value={account.accountToken}>
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
              key={`${bootstrap.productSessionToken}:${selected.accountToken}`}
              account={selected}
              bootstrap={bootstrap}
              transport={transport}
            />
          ) : (
            <div className="mt-4">
              <RiskEmptyState
                title="Choose a portfolio"
                detail="Market Squawk will show its action, horizon, ranges, reasons, risks, assumptions, invalidators, and uncertainty."
              />
            </div>
          )}
          <p className="mt-4 text-[10px] leading-relaxed text-muted-foreground">
            Showing {availableAccounts.length} portfolios on this page.
            Risk guidance informs a decision but never approves or places a trade.
          </p>
        </>
      )}
    </PageFrame>
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

function RiskLoading() {
  return (
    <PageFrame>
      <RiskGridLoading />
    </PageFrame>
  )
}
