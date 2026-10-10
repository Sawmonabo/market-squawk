import type { ColumnDef } from "@tanstack/react-table"

import { DataTable } from "@/components/tables/data-table"
import { formatMoney, groupDecimal } from "@/lib/formatters"
import { formatUnixNanos } from "../opportunities/format"

import type { PortfolioHolding } from "./portfolio-contracts"

export function HoldingTable({ holdings }: { holdings: PortfolioHolding[] }) {
  return (
    <DataTable
      ariaLabel="Portfolio positions"
      columns={holdingColumns}
      data={holdings}
      getRowId={(holding) => holding.instrumentId}
      pageSize={Math.max(holdings.length, 1)}
      emptyMessage="This portfolio observation has no positions."
    />
  )
}

const holdingColumns: ColumnDef<PortfolioHolding, unknown>[] = [
  {
    id: "investment",
    header: "Investment",
    cell: ({ row }) => {
      const { investment, asOfUnixNanos } = row.original
      return (
        <div className="min-w-44">
          <p className="font-medium">
            {investment.name ?? "Investment name unavailable"}
            {investment.symbol ? ` (${investment.symbol})` : ""}
          </p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            Recorded {formatUnixNanos(asOfUnixNanos)}
          </p>
        </div>
      )
    },
  },
  {
    id: "price",
    header: "Price information",
    cell: ({ row }) => <PriceSummary holding={row.original} />,
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
    cell: ({ row }) => (
      <div>
        <p className="font-mono tabular-nums">{groupDecimal(row.original.quantity)}</p>
        <p className="mt-1 text-[10px] text-muted-foreground">
          Lot size {groupDecimal(row.original.lotSize)}
        </p>
      </div>
    ),
  },
  {
    id: "basis",
    header: "Cost basis",
    cell: ({ row }) => <BasisValue holding={row.original} />,
  },
]

function PriceSummary({ holding }: { holding: PortfolioHolding }) {
  const price = holding.price
  const label = price.state === "reported" ? "Reported portfolio value"
    : price.state === "current" ? "Current market price"
      : price.state === "stale" ? "Older price" : "Price unavailable"
  const confidence = price.confidence === "limited" ? "Limited"
    : price.confidence === "moderate" ? "Moderate" : "Strong"
  return (
    <div className="max-w-64 text-xs">
      <p>{label}</p>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
        As of {formatUnixNanos(price.asOfUnixNanos)} · {confidence} confidence
      </p>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{price.explanation}</p>
    </div>
  )
}

function BasisValue({ holding }: { holding: PortfolioHolding }) {
  const basis = holding.costBasis
  switch (basis.state) {
    case "available":
      return (
        <div>
          <p className="font-mono tabular-nums">{formatMoney(basis.amount)}</p>
          <p className="mt-1 text-[10px] text-muted-foreground">{basis.method}</p>
        </div>
      )
    case "needs_review":
      return (
        <div className="max-w-64">
          <p className="text-amber-300">Review needed</p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
            {basis.method} · Conflicting reported amounts; no cost basis has been selected.
          </p>
          <ul className="mt-2 space-y-1 text-[10px] text-muted-foreground">
            {basis.choices.map((choice, index) => (
              <li key={`${choice.currency}:${choice.amount}:${index}`} className="font-mono tabular-nums">
                {formatMoney(choice)}
              </li>
            ))}
          </ul>
        </div>
      )
    case "not_available":
      return <span className="text-xs text-muted-foreground">Cost basis not available</span>
  }
}
