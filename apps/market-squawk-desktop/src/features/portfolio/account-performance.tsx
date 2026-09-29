import { CircleAlert, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import type { PortfolioAccountSummary } from "./portfolio-contracts"
import { PerformancePanel, ReconciliationPanel } from "./portfolio-panels"
import { usePortfolioPerformance } from "./use-portfolio"

export function AccountPerformance({ account, bootstrap, transport }: {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const performance = usePortfolioPerformance(transport, bootstrap, account.accountToken)

  if (!performance.available) {
    return <PerformanceUnavailable title="Cash and performance unavailable"
      detail="These details cannot currently be opened for this portfolio." />
  }
  if (performance.query.isPending) {
    return (
      <div className="space-y-4" aria-label={`Loading cash and performance for ${account.displayName}`}>
        <Skeleton className="h-80 rounded-xl" />
        <Skeleton className="h-32 rounded-xl" />
      </div>
    )
  }
  if (performance.query.isError) {
    return (
      <div>
        <PerformanceUnavailable title="This portfolio’s cash and performance could not be opened"
          detail="Try again, or refresh the portfolio before relying on these figures." />
        <Button className="mt-4" onClick={() => void performance.query.refetch()}
          disabled={performance.query.isFetching}>
          <RefreshCw className={performance.query.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Try again
        </Button>
      </div>
    )
  }
  if (!performance.query.data) return null

  return (
    <div className="space-y-4" aria-label={`Cash and performance for ${account.displayName}`}>
      <PerformancePanel performance={performance.query.data} />
      <ReconciliationPanel performance={performance.query.data} />
      <Button variant="outline" onClick={() => void performance.query.refetch()}
        disabled={performance.query.isFetching}>
        <RefreshCw className={performance.query.isFetching ? "animate-spin" : ""} aria-hidden="true" />
        Refresh cash and performance
      </Button>
    </div>
  )
}

function PerformanceUnavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert>
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}
