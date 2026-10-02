import { RefreshButton } from "@/components/ui/refresh-button"
import { CircleAlert } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { formatUnixNanos } from "../opportunities/format"
import { CursorNavigation } from "../shared/cursor-navigation"

import { HoldingTable } from "./holding-table"
import type { PortfolioAccountSummary } from "./portfolio-contracts"
import { ExposurePanel } from "./portfolio-panels"
import { usePortfolioPositions } from "./use-portfolio"

export function AccountPositions({ account, bootstrap, transport, mode }: {
  account: PortfolioAccountSummary
  bootstrap: DesktopBootstrap
  transport: ProductTransport
  mode: "holdings" | "exposure"
}) {
  const positions = usePortfolioPositions(transport, bootstrap, account.accountToken, mode)
  const page = positions.query.data
  const label = mode === "holdings" ? "Positions" : "Exposure"
  const detail = label.toLowerCase()

  if (!positions.available) {
    return <PositionsUnavailable title={`${label} unavailable`}
      detail="These details cannot currently be opened for this portfolio." />
  }

  return (
    <div className="space-y-4" aria-label={`${label} for ${account.displayName}`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
          {mode === "exposure"
            ? "Exposure totals and paged positions use the same recorded portfolio observation."
            : "Positions are shown one page at a time from the same recorded portfolio observation."}
          {` Refresh ${detail} to start again with the latest available observation.`}
        </p>
        <RefreshButton label={`Refresh ${detail}`} refreshing={positions.query.isFetching} onClick={positions.refresh} disabled={positions.query.isFetching} />
      </div>
      {positions.query.isPending ? (
        <div aria-label={`Loading ${detail} for ${account.displayName}`}>
          <Skeleton className="h-80 rounded-xl" />
        </div>
      ) : positions.query.isError ? (
        <div>
          <PositionsUnavailable title={`This portfolio’s ${detail} could not be opened`}
            detail={`Try again, or refresh ${detail} to start with the latest available portfolio observation.`} />
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
          {mode === "exposure" && "exposure" in page ? (
            <ExposurePanel exposure={page.exposure} />
          ) : null}
          {mode === "exposure" ? (
            <div>
              <h3 className="text-sm font-semibold">Positions on this page</h3>
              <p className="mt-1 text-xs leading-5 text-muted-foreground">
                This page shows part of the position detail. The exposure totals above cover the
                complete observation and do not change with the page.
              </p>
            </div>
          ) : null}
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
