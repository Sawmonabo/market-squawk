import * as React from "react"
import type { ColumnDef } from "@tanstack/react-table"
import { Search } from "lucide-react"

import { DataTable } from "@/components/tables/data-table"
import { Input } from "@/components/ui/input"
import { formatMoney, groupDecimal, humanize } from "@/lib/formatters"

import type { PortfolioHolding } from "./portfolio-contracts"
import { formatTimestamp, shortIdentity } from "./portfolio-format"

export function HoldingTable({ holdings }: { holdings: PortfolioHolding[] }) {
  const [filter, setFilter] = React.useState("")
  const normalized = filter.trim().toLocaleLowerCase()
  const visible = normalized
    ? holdings.filter((holding) =>
        [
          holding.instrumentId,
          holding.marketValue.currency,
          holding.costBasis.state,
        ].some((value) => value.toLocaleLowerCase().includes(normalized)),
      )
    : holdings

  return (
    <div>
      <div className="relative mb-3 max-w-sm">
        <Search
          className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
          aria-hidden="true"
        />
        <Input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Find an asset or currency"
          aria-label="Filter portfolio holdings"
          className="pl-9"
        />
      </div>
      <DataTable
        ariaLabel="Portfolio holdings"
        columns={holdingColumns}
        data={visible}
        getRowId={(holding) => holding.instrumentId}
        emptyMessage={
          holdings.length === 0
            ? "This account has no holdings in the selected snapshot."
            : "No holding matches this filter."
        }
      />
    </div>
  )
}

const holdingColumns: ColumnDef<PortfolioHolding, unknown>[] = [
  {
    accessorKey: "instrumentId",
    header: "Asset",
    cell: ({ row }) => (
      <div className="min-w-44">
        <p className="font-medium">{shortIdentity(row.original.instrumentId, "Asset")}</p>
        <p className="mt-1 max-w-52 truncate font-mono text-[10px] text-muted-foreground">
          {row.original.instrumentId}
        </p>
      </div>
    ),
  },
  {
    id: "price",
    accessorFn: (holding) => holding.price.asOfUnixNanos,
    header: "Price status",
    cell: ({ row }) => <MarkEvidence holding={row.original} />,
  },
  {
    id: "marketValue",
    accessorFn: (holding) => holding.marketValue.amount,
    header: "Market value",
    enableSorting: false,
    meta: { className: "font-mono tabular-nums" },
    cell: ({ row }) => formatMoney(row.original.marketValue),
  },
  {
    id: "quantity",
    accessorFn: (holding) => holding.quantity,
    header: "Quantity",
    enableSorting: false,
    meta: { className: "font-mono tabular-nums" },
    cell: ({ row }) => (
      <div>
        <p>{groupDecimal(row.original.quantity)}</p>
        <p className="mt-1 text-[10px] text-muted-foreground">
          Lot size {groupDecimal(row.original.lotSize)}
        </p>
      </div>
    ),
  },
  {
    id: "basis",
    accessorFn: (holding) => holding.costBasis.state,
    header: "Cost basis",
    cell: ({ row }) => <BasisValue holding={row.original} />,
  },
]

function MarkEvidence({ holding }: { holding: PortfolioHolding }) {
  const mark = holding.price
  return (
    <div className="max-w-56 text-xs">
      <p>{humanize(mark.state)}</p>
      <p className="mt-1 text-[10px] text-muted-foreground">
        Updated {formatTimestamp(mark.asOfUnixNanos)}
      </p>
      <p className="mt-1 text-[10px] text-muted-foreground">{mark.explanation}</p>
    </div>
  )
}

function BasisValue({ holding }: { holding: PortfolioHolding }) {
  switch (holding.costBasis.state) {
    case "available":
      return (
        <div>
          <p className="font-mono tabular-nums">
            {formatMoney(holding.costBasis.amount)}
          </p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            {holding.costBasis.method}
          </p>
        </div>
      )
    case "needs_review":
      return (
        <div>
          <p className="text-amber-300">Needs review</p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            {holding.costBasis.choices.length} possible matches
          </p>
        </div>
      )
    case "not_available":
      return <span className="text-muted-foreground">Not supplied</span>
  }
}
