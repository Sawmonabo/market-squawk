import * as React from "react"
import type { ColumnDef } from "@tanstack/react-table"
import { Search } from "lucide-react"

import { DataTable } from "@/components/tables/data-table"
import { Input } from "@/components/ui/input"
import { formatMoney } from "@/lib/formatters"

import type { PortfolioHolding } from "./portfolio-contracts"
import { formatProductTime, investmentDisplayName } from "./portfolio-format"

export function HoldingTable({ holdings }: { holdings: PortfolioHolding[] }) {
  const [filter, setFilter] = React.useState("")
  const normalized = filter.trim().toLocaleLowerCase()
  const visible = normalized
    ? holdings.filter((holding) =>
        [
          holding.investment.name,
          holding.investment.symbol ?? "",
          holding.investment.typeLabel,
          holding.marketValue.currency,
          holding.costBasis.state === "available"
            ? holding.costBasis.methodLabel
            : holding.costBasis.explanation,
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
          placeholder="Find an investment or currency"
          aria-label="Filter portfolio positions"
          className="pl-9"
        />
      </div>
      <DataTable
        ariaLabel="Portfolio positions"
        columns={holdingColumns}
        data={visible}
        getRowId={(holding) => holding.positionActionToken}
        emptyMessage={
          holdings.length === 0
            ? "This portfolio has no positions."
            : "No position matches this filter."
        }
      />
    </div>
  )
}

const holdingColumns: ColumnDef<PortfolioHolding, unknown>[] = [
  {
    id: "investment",
    header: "Investment",
    cell: ({ row }) => (
      <div className="min-w-44">
        <p className="font-medium">{investmentDisplayName(row.original.investment)}</p>
        <p className="mt-1 text-[10px] text-muted-foreground">
          {row.original.investment.typeLabel}
        </p>
      </div>
    ),
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
    meta: { className: "font-mono tabular-nums" },
    cell: ({ row }) => row.original.quantityLabel,
  },
  {
    id: "basis",
    accessorFn: (holding) => holding.costBasis.state,
    header: "Cost basis",
    cell: ({ row }) => <BasisValue holding={row.original} />,
  },
]

function PriceSummary({ holding }: { holding: PortfolioHolding }) {
  const price = holding.price
  return (
    <div className="max-w-64 text-xs">
      <p>{price.label}</p>
      <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
        {price.updatedAt ? `Updated ${formatProductTime(price.updatedAt)}. ` : ""}
        {price.explanation}
      </p>
    </div>
  )
}

function BasisValue({ holding }: { holding: PortfolioHolding }) {
  switch (holding.costBasis.state) {
    case "available":
      return (
        <div>
          <p className="font-mono tabular-nums">{formatMoney(holding.costBasis.amount)}</p>
          <p className="mt-1 text-[10px] text-muted-foreground">
            {holding.costBasis.methodLabel}
          </p>
        </div>
      )
    case "needs_review":
      return (
        <div className="max-w-64">
          <p className="text-amber-300">Review needed</p>
          <p className="mt-1 text-[10px] leading-4 text-muted-foreground">
            {holding.costBasis.explanation}
          </p>
          {holding.costBasis.choices.length > 0 ? (
            <ul className="mt-2 space-y-1 text-[10px] text-muted-foreground">
              {holding.costBasis.choices.map((choice) => (
                <li key={choice.choiceToken}>
                  {choice.label}
                  {choice.amount ? ` · ${formatMoney(choice.amount)}` : ""}
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      )
    case "not_available":
      return (
        <span className="max-w-64 text-xs text-muted-foreground">
          {holding.costBasis.explanation}
        </span>
      )
  }
}
