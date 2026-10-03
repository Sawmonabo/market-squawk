import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { DemandPanel } from "../shared/demand-panel"

import type { PortfolioAccountSummary } from "./portfolio-contracts"
import { PortfolioPositionImpact } from "./portfolio-position-impact"
import { PortfolioRebalance } from "./portfolio-rebalance"

export function PortfolioPlanning({ account, bootstrap, transport }: {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Plan before acting
        </p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio planning</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Enter your position or rebalance assumptions before making a decision. Planning cannot
          place an order, and no choice is selected automatically.
        </p>
      </header>
      <div className="mt-5 space-y-4">
        <DemandPanel title="Compare a position change" className="rounded-lg border border-border bg-background/25 p-4">
          <PortfolioPositionImpact key={`${bootstrap.productSessionToken}:${account.accountToken}`}
            account={account} bootstrap={bootstrap} transport={transport} />
        </DemandPanel>
        <DemandPanel title="Rebalance plan" className="rounded-lg border border-border bg-background/25 p-4">
          <PortfolioRebalance account={account} bootstrap={bootstrap} transport={transport} />
        </DemandPanel>
      </div>
    </section>
  )
}
