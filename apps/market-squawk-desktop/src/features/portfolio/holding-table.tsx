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
          holding.instrument_id,
          holding.source_reference,
          holding.market_value.currency,
          holding.basis.status,
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
          placeholder="Find an asset, source, or currency"
          aria-label="Filter portfolio holdings"
          className="pl-9"
        />
      </div>
      <DataTable
        ariaLabel="Portfolio holdings"
        columns={holdingColumns}
        data={visible}
        getRowId={(holding) => holding.instrument_id}
        emptyMessage={
          holdings.length === 0
            ? "This account has no holdings in the selected revision."
            : "No holding matches this filter."
        }
      />
    </div>
  )
}

const holdingColumns: ColumnDef<PortfolioHolding, unknown>[] = [
  {
    accessorKey: "instrument_id",
    header: "Asset",
    cell: ({ row }) => (
      <div className="min-w-44">
        <p className="font-medium">{shortIdentity(row.original.instrument_id, "Asset")}</p>
        <p className="mt-1 max-w-52 truncate font-mono text-[10px] text-muted-foreground">
          {row.original.instrument_id}
        </p>
      </div>
    ),
  },
  {
    id: "marketValue",
    accessorFn: (holding) => holding.market_value.amount,
    header: "Market value",
    enableSorting: false,
    meta: { className: "font-mono tabular-nums" },
    cell: ({ row }) => formatMoney(row.original.market_value),
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
          Lot size {groupDecimal(row.original.lot_size)}
        </p>
      </div>
    ),
  },
  {
    id: "basis",
    accessorFn: (holding) => holding.basis.status,
    header: "Cost basis",
    cell: ({ row }) => <BasisValue holding={row.original} />,
  },
  {
    accessorKey: "source_reference",
    header: "Source reference",
    cell: ({ row }) => (
      <div className="max-w-48">
        <p className="truncate text-xs">{row.original.source_reference}</p>
        <p className="mt-1 text-[10px] text-muted-foreground">
          Source as of {formatTimestamp(row.original.as_of)}
        </p>
      </div>
    ),
  },
]

function BasisValue({ holding }: { holding: PortfolioHolding }) {
  switch (holding.basis.status) {
    case "resolved":
      return (
        <div>
          <p className="font-mono tabular-nums">
            {formatMoney(holding.basis.observation.amount)}
          </p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            {humanize(holding.basis.observation.lot_method)}
          </p>
        </div>
      )
    case "ambiguous":
      return (
        <div>
          <p className="text-amber-300">Needs review</p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            {holding.basis.candidates.length} source candidates
          </p>
        </div>
      )
    case "missing":
      return <span className="text-muted-foreground">Not supplied</span>
  }
}
