import { Clock3, History } from "lucide-react"

export function PortfolioHistory() {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          What changed
        </p>
        <h2 className="mt-2 text-lg font-semibold">History and attribution</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Review transactions and compare your current portfolio with an earlier saved version.
          Cash flows and corporate actions remain separate from investment returns.
        </p>
      </header>
      <div className="mt-5 grid gap-4 xl:grid-cols-2">
        <UnavailableHistory
          icon={Clock3}
          title="Choose an earlier portfolio"
          detail="A comparison is shown only after Market Squawk supplies a dated, named choice and you select it. No earlier version is selected automatically."
        />
        <UnavailableHistory
          icon={History}
          title="Transaction history"
          detail="Transactions are shown only when their investment names, amounts, quantities, and dates are complete."
        />
      </div>
    </section>
  )
}

function UnavailableHistory({
  icon: Icon,
  title,
  detail,
}: {
  icon: typeof Clock3
  title: string
  detail: string
}) {
  return (
    <div className="rounded-lg border border-dashed border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">{title}</h3>
      </div>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">{detail}</p>
    </div>
  )
}
