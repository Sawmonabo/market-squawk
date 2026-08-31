import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts"

export interface RiskChartValue {
  label: string
  value: number
  color: string
}

export function RiskChart({ values }: { values: RiskChartValue[] }) {
  if (values.length === 0) return null

  return (
    <div>
      <div
        className="h-56 w-full"
        role="img"
        aria-label="Portfolio risk measures shown as percentages"
      >
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={values}
            layout="vertical"
            margin={{ top: 8, right: 12, bottom: 8, left: 8 }}
          >
            <CartesianGrid horizontal={false} stroke="var(--border)" strokeDasharray="3 3" />
            <XAxis
              type="number"
              axisLine={false}
              tickLine={false}
              tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
              tickFormatter={(value: number) => formatPercent(value)}
            />
            <YAxis
              type="category"
              dataKey="label"
              width={108}
              axisLine={false}
              tickLine={false}
              tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
            />
            <Bar dataKey="value" radius={[0, 4, 4, 0]} maxBarSize={22}>
              {values.map((entry) => (
                <Cell key={entry.label} fill={entry.color} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <dl className="mt-3 grid gap-2 sm:grid-cols-3">
        {values.map((entry) => (
          <div key={entry.label} className="rounded-md border border-border bg-background/40 p-3">
            <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {entry.label}
            </dt>
            <dd className="mt-1 font-mono text-sm font-semibold">
              {formatPercent(entry.value)}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function formatPercent(value: number) {
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 2,
  }).format(value)
}
