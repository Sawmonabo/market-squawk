import * as React from "react"
import type { ColumnDef } from "@tanstack/react-table"
import { AlertCircle, Clock3, GitCompareArrows, History } from "lucide-react"

import { messageFrom } from "@/app/product-context"
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

  const errors = [
    history.transactions.error,
    history.revisions.error,
    history.attribution.error,
  ].filter((error): error is Error => error instanceof Error)

  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          What changed
        </p>
        <h2 className="mt-2 text-lg font-semibold">History and attribution</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Review retained source transactions and compare the current immutable revision with an
          earlier one. Attribution uses source mark changes and does not adjust for cash flows or
          corporate actions.
        </p>
      </header>

      {errors.length ? (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Some history could not be read</AlertTitle>
          <AlertDescription>{errors.map(messageFrom).join(" · ")}</AlertDescription>
        </Alert>
      ) : null}

      <div className="mt-5 grid gap-4 xl:grid-cols-[0.8fr_1.2fr]">
        <RevisionComparison
          currentId={account.currentRevision.revisionId}
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
  currentId,
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
  currentId: string
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
        <h3 className="text-sm font-semibold">Compare revisions</h3>
      </div>
      {!available ? (
        <Unavailable text="Portfolio revision history is not registered." />
      ) : loading ? (
        <Skeleton className="mt-4 h-40 rounded-lg" />
      ) : baselines.length === 0 ? (
        <Unavailable text="At least two available revisions are required for attribution." />
      ) : (
        <>
          <label className="mt-4 grid gap-1.5 text-xs">
            <span className="font-medium">Earlier revision</span>
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
              {loadingMore ? "Loading…" : "Load earlier revisions"}
            </Button>
          ) : null}
          <p className="mt-3 break-all text-[10px] text-muted-foreground">
            Current revision: <span className="font-mono">{currentId}</span>
          </p>
          {!attributionAvailable ? (
            <Unavailable text="Portfolio attribution is not registered." />
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
        Total source-mark change
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
      {
        accessorKey: "source_reference",
        header: "Source reference",
        cell: ({ row }) => (
          <span className="block max-w-44 truncate font-mono text-xs">
            {row.original.source_reference}
          </span>
        ),
      },
    ],
    [],
  )
  return (
    <div className="min-w-0 rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <History className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Source transactions</h3>
      </div>
      {!available ? (
        <Unavailable text="Portfolio transaction history is not registered." />
      ) : loading ? (
        <Skeleton className="mt-4 h-72 rounded-lg" />
      ) : (
        <div className="mt-4">
          <DataTable
            columns={columns}
            data={rows}
            emptyMessage="No transactions were retained in this revision."
            getRowId={(row) => row.broker_transaction_id}
            pageSize={8}
            ariaLabel="Portfolio source transactions"
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
