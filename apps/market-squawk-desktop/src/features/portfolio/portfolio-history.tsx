import * as React from "react"
import type { ColumnDef } from "@tanstack/react-table"
import { AlertCircle, Clock3, GitCompareArrows, History } from "lucide-react"

import { DataTable } from "@/components/tables/data-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { formatMoney, humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import type {
  PortfolioAccount,
  PortfolioAttribution,
  PortfolioTransaction,
} from "./portfolio-contracts"
import { formatTimestamp, shortIdentity } from "./portfolio-format"
import { usePortfolioHistory } from "./use-portfolio"

export function PortfolioHistory({
  account,
  bootstrap,
  transport,
}: {
  account: PortfolioAccount
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [baselineId, setBaselineId] = React.useState<string | null>(null)
  const history = usePortfolioHistory(
    transport,
    bootstrap.runtime,
    bootstrap,
    account.accountId,
    baselineId,
  )
  const revisions = history.revisions.data?.pages.flatMap((page) => page.value) ?? []
  const baselines = revisions.filter(
    (revision) => revision.revisionId !== account.currentRevision.revisionId,
  )

  React.useEffect(() => {
    setBaselineId(null)
  }, [account.accountId])

  React.useEffect(() => {
    if (baselineId === null && baselines.length > 0) {
      setBaselineId(baselines.at(-1)?.revisionId ?? null)
    }
  }, [baselineId, baselines])

  const hasHistoryError = [
    history.transactions.error,
    history.revisions.error,
    history.attribution.error,
  ].some(Boolean)

  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          What changed
        </p>
        <h2 className="mt-2 text-lg font-semibold">History and attribution</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Review recorded transactions and compare the current account snapshot with an earlier one.
          Changes in cash flows and corporate actions are shown separately from investment returns.
        </p>
      </header>

      {hasHistoryError ? (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Some history could not be read</AlertTitle>
          <AlertDescription>
            Some account history is unavailable right now. Try refreshing; detailed diagnostics are
            available in Logs &amp; Diagnostics.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="mt-5 grid gap-4 xl:grid-cols-[0.8fr_1.2fr]">
        <RevisionComparison
          baselines={baselines}
          selectedId={baselineId}
          select={setBaselineId}
          loading={history.revisions.isPending}
          available={history.operationAvailable["Portfolio.ListRevisions"]}
          loadMore={
            history.revisions.hasNextPage
              ? () => void history.revisions.fetchNextPage()
              : null
          }
          loadingMore={history.revisions.isFetchingNextPage}
          attribution={history.attribution.data?.value ?? null}
          attributionLoading={history.attribution.isPending && baselineId !== null}
          attributionAvailable={history.operationAvailable["Portfolio.GetAttribution"]}
        />
        <TransactionHistory
          rows={history.transactions.data?.value ?? []}
          loading={history.transactions.isPending}
          available={history.operationAvailable["Portfolio.GetTransactions"]}
        />
      </div>
    </section>
  )
}

function RevisionComparison({
  baselines,
  selectedId,
  select,
  loading,
  available,
  loadMore,
  loadingMore,
  attribution,
  attributionLoading,
  attributionAvailable,
}: {
  baselines: PortfolioAccount["currentRevision"][]
  selectedId: string | null
  select: (revisionId: string) => void
  loading: boolean
  available: boolean
  loadMore: (() => void) | null
  loadingMore: boolean
  attribution: PortfolioAttribution | null
  attributionLoading: boolean
  attributionAvailable: boolean
}) {
  return (
    <div className="rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <GitCompareArrows className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Compare account snapshots</h3>
      </div>
      {!available ? (
        <Unavailable text="Portfolio history is unavailable right now." />
      ) : loading ? (
        <Skeleton className="mt-4 h-40 rounded-lg" />
      ) : baselines.length === 0 ? (
        <Unavailable text="At least two account snapshots are needed to show changes over time." />
      ) : (
        <>
          <label className="mt-4 grid gap-1.5 text-xs">
            <span className="font-medium">Earlier snapshot</span>
            <select
              value={selectedId ?? ""}
              onChange={(event) => select(event.target.value)}
              className="h-9 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              {baselines.map((revision) => (
                <option key={revision.revisionId} value={revision.revisionId}>
                  {formatTimestamp(revision.effectiveAtUnixNanos)} · {revision.holdingCount} holdings
                </option>
              ))}
            </select>
          </label>
          {loadMore ? (
            <Button
              variant="outline"
              size="sm"
              className="mt-3"
              onClick={loadMore}
              disabled={loadingMore}
            >
              {loadingMore ? "Loading…" : "Load earlier snapshots"}
            </Button>
          ) : null}
          {!attributionAvailable ? (
            <Unavailable text="Changes over time are unavailable right now." />
          ) : attributionLoading ? (
            <Skeleton className="mt-4 h-28 rounded-lg" />
          ) : attribution ? (
            <AttributionResult result={attribution} />
          ) : null}
        </>
      )}
    </div>
  )
}

function AttributionResult({ result }: { result: PortfolioAttribution }) {
  return (
    <div className="mt-4 rounded-lg border border-border bg-card/30 p-4">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Change in market value
      </p>
      <p className="mt-1 font-mono text-lg font-semibold tabular-nums">
        {formatMoney(result.total)}
      </p>
      <div className="mt-3 space-y-2">
        {result.contributions.slice(0, 6).map((row) => (
          <div key={row.instrumentId} className="flex justify-between gap-3 text-xs">
            <span className="truncate text-muted-foreground">
              {shortIdentity(row.instrumentId, "Asset")}
            </span>
            <span className="shrink-0 font-mono">{formatMoney(row.amount)}</span>
          </div>
        ))}
      </div>
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        Cash flows and corporate actions are not adjusted in this calculation.
      </p>
    </div>
  )
}

function TransactionHistory({
  rows,
  loading,
  available,
}: {
  rows: PortfolioTransaction[]
  loading: boolean
  available: boolean
}) {
  const columns = React.useMemo<ColumnDef<PortfolioTransaction, unknown>[]>(
    () => [
      {
        accessorKey: "occurred_at",
        header: "Occurred",
        cell: ({ row }) => formatTimestamp(row.original.occurred_at),
      },
      {
        accessorKey: "kind",
        header: "Type",
        cell: ({ row }) => humanize(row.original.kind),
      },
      {
        id: "asset",
        header: "Asset",
        cell: ({ row }) =>
          row.original.instrument_id
            ? shortIdentity(row.original.instrument_id, "Asset")
            : "Account cash",
      },
      {
        id: "amount",
        header: "Amount",
        cell: ({ row }) => (
          <span className="font-mono tabular-nums">{formatMoney(row.original.amount)}</span>
        ),
      },
    ],
    [],
  )
  return (
    <div className="min-w-0 rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <History className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Transactions</h3>
      </div>
      {!available ? (
        <Unavailable text="Transaction history is unavailable right now." />
      ) : loading ? (
        <Skeleton className="mt-4 h-72 rounded-lg" />
      ) : (
        <div className="mt-4">
          <DataTable
            columns={columns}
            data={rows}
            emptyMessage="No transactions are available in this account snapshot."
            getRowId={(row) => row.broker_transaction_id}
            pageSize={8}
            ariaLabel="Portfolio transactions"
          />
        </div>
      )}
    </div>
  )
}

function Unavailable({ text }: { text: string }) {
  return (
    <div className="mt-4 flex gap-2 rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
      <Clock3 className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <span>{text}</span>
    </div>
  )
}
