import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { formatMoney } from "@/lib/formatters"

export interface PortfolioChartDatum {
  label: string
  exactAmount: string
  currency: string
}

interface ChartRow extends PortfolioChartDatum {
  value: number
}

export function PortfolioChart({ data }: { data: PortfolioChartDatum[] }) {
  const currencies = new Set(data.map((row) => row.currency))
  const rows = data
    .map((row) => ({ ...row, value: Number(row.exactAmount) }))
    .filter((row) => Number.isFinite(row.value))
    .sort((left, right) => Math.abs(right.value) - Math.abs(left.value))
    .slice(0, 8)

  if (rows.length === 0 || currencies.size !== 1) {
    return (
      <div className="flex h-72 items-center justify-center rounded-lg border border-dashed border-border px-8 text-center text-sm leading-6 text-muted-foreground">
        Allocation chart unavailable. Exact values remain available in the holdings table.
      </div>
    )
  }

  return (
    <div>
      <div
        className="h-72 w-full"
        role="img"
        aria-label="Largest portfolio holdings by signed market value"
      >
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={rows}
            layout="vertical"
            margin={{ top: 6, right: 18, bottom: 2, left: 12 }}
            accessibilityLayer
          >
            <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" horizontal={false} />
            <XAxis
              type="number"
              tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              tickFormatter={compactNumber}
            />
            <YAxis
              dataKey="label"
              type="category"
              width={88}
              tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
            />
            <ReferenceLine x={0} stroke="var(--muted-foreground)" strokeOpacity={0.45} />
            <Tooltip content={<HoldingTooltip />} cursor={{ fill: "var(--accent)", opacity: 0.35 }} />
            <Bar dataKey="value" radius={[3, 3, 3, 3]} maxBarSize={22}>
              {rows.map((row) => (
                <Cell
                  key={row.label}
                  fill={row.value < 0 ? "var(--destructive)" : "var(--primary)"}
                  fillOpacity={0.82}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
        The chart uses approximate display coordinates only. The table preserves the exact source
        amounts and currencies.
      </p>
      <ul className="sr-only">
        {rows.map((row) => (
          <li key={row.label}>
            {row.label}: {formatMoney({ amount: row.exactAmount, currency: row.currency })}
          </li>
        ))}
      </ul>
    </div>
  )
}

function HoldingTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: ReadonlyArray<{ payload?: ChartRow }>
}) {
  const row = payload?.[0]?.payload
  if (!active || !row) return null
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-xl">
      <p className="font-medium text-popover-foreground">{row.label}</p>
      <p className="mt-1 font-mono text-muted-foreground">
        {formatMoney({ amount: row.exactAmount, currency: row.currency })}
      </p>
    </div>
  )
}

function compactNumber(value: number) {
  return Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value)
}
