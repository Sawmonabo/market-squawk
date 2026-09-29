import { CircleAlert, RefreshCw } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import { HoldingTable } from "./holding-table"
import type { PortfolioAccountSummary } from "./portfolio-contracts"
import { usePortfolioHoldings } from "./use-portfolio"

export function AccountHoldings({ account, bootstrap, transport }: {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const positions = usePortfolioHoldings(transport, bootstrap, account.accountToken)
  const page = positions.query.data

  if (!positions.available) {
    return <PositionsUnavailable title="Positions unavailable"
      detail="These details cannot currently be opened for this portfolio." />
  }

  return (
    <div className="space-y-4" aria-label={`Positions for ${account.displayName}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
          Positions are shown one page at a time from the same recorded portfolio observation.
          Refresh positions to start again with the latest available observation.
        </p>
        <Button variant="outline" onClick={positions.refresh} disabled={positions.query.isFetching}>
          <RefreshCw className={positions.query.isFetching ? "animate-spin" : ""} aria-hidden="true" />
          Refresh positions
        </Button>
      </div>
      {positions.query.isPending ? (
        <div aria-label={`Loading positions for ${account.displayName}`}>
          <Skeleton className="h-80 rounded-xl" />
        </div>
      ) : positions.query.isError ? (
        <div>
          <PositionsUnavailable title="This portfolio’s positions could not be opened"
            detail="Try again, or refresh positions to start with the latest available portfolio observation." />
          <Button className="mt-4" onClick={() => void positions.query.refetch()} disabled={positions.query.isFetching}>
            Try again
          </Button>
        </div>
      ) : page ? (
        <>
          <dl className="grid gap-3 text-xs sm:grid-cols-2">
            <div>
              <dt className="text-muted-foreground">Portfolio observation</dt>
              <dd className="mt-1">{formatUnixNanos(page.effectiveAtUnixNanos)}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Information available</dt>
              <dd className="mt-1">{page.availableAtUnixNanos === null
                ? "Not recorded" : formatUnixNanos(page.availableAtUnixNanos)}</dd>
            </div>
          </dl>
          <HoldingTable key={page.snapshotToken + ":" + positions.navigation.page} holdings={page.holdings} />
        </>
      ) : null}
      <CursorNavigation navigation={positions.navigation} current={page?.pageCursor} next={page?.nextCursor}
        busy={positions.query.isFetching} error={positions.query.isError}
        onRestart={positions.refresh} />
    </div>
  )
}

function PositionsUnavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert>
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}
