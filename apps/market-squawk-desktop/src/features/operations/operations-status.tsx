import {
  Activity,
  Clock3,
  Database,
  HardDrive,
  LoaderCircle,
  Server,
} from "lucide-react"

export function UnavailableOperationalFacts() {
  const facts = [
    {
      icon: Server,
      title: "Service health",
      reason:
        "Unavailable: the desktop does not have a closed service-health query.",
    },
    {
      icon: Database,
      title: "Runtime resources",
      reason:
        "Unavailable: no bounded runtime CPU or memory query is exposed.",
    },
    {
      icon: HardDrive,
      title: "Storage pressure",
      reason:
        "Unavailable: setup readiness does not prove current storage usage or pressure.",
    },
  ] as const

  return (
    <section className="mt-7" aria-labelledby="operational-health-heading">
      <h2 id="operational-health-heading" className="text-lg font-semibold">
        Operational health
      </h2>
      <p className="mt-1 text-sm text-muted-foreground">
        Only facts backed by a dedicated closed query are reported as available.
      </p>
      <div className="mt-4 grid gap-3 md:grid-cols-3">
        {facts.map(({ icon: Icon, title, reason }) => (
          <div
            key={title}
            className="rounded-xl border border-border bg-card/30 p-4"
          >
            <Icon className="size-4 text-muted-foreground" aria-hidden="true" />
            <h3 className="mt-3 text-sm font-medium">{title}</h3>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              {reason}
            </p>
          </div>
        ))}
      </div>
    </section>
  )
}

export function SummaryFact({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Activity
  label: string
  value: number
  detail: string
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
        {detail}
      </p>
    </section>
  )
}

export function LoadingState() {
  return (
    <div
      className="mt-4 flex items-center gap-3 rounded-xl border border-border bg-card/30 p-5 text-sm text-muted-foreground"
      role="status"
    >
      <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
      Loading durable jobs…
    </div>
  )
}

export function EmptyJobs() {
  return (
    <div className="mt-4 rounded-xl border border-dashed border-border p-7 text-center">
      <Clock3 className="mx-auto size-5 text-muted-foreground" aria-hidden="true" />
      <h3 className="mt-3 text-sm font-medium">No durable jobs yet</h3>
      <p className="mx-auto mt-1 max-w-md text-xs leading-relaxed text-muted-foreground">
        Long-running research, model, forecast, backtest, import, export, and
        recovery work will appear here after it is admitted by the service.
      </p>
    </div>
  )
}
