import { useProduct } from "@/app/product-context"

export function StatusRail() {
  const product = useProduct()
  const bootstrap = product.status === "ready" ? product.bootstrap : null

  return (
    <section
      aria-label="Operational status"
      className="flex min-h-7 shrink-0 items-center gap-4 border-b border-border/70 bg-card/20 px-5 font-mono text-[9px] uppercase tracking-wide text-muted-foreground"
    >
      <StatusFact
        label="Local"
        value={
          product.status === "loading"
            ? "Starting"
            : product.status === "ready"
              ? product.bootstrap.storage.label
              : "Unavailable"
        }
        ready={bootstrap?.storage.state === "ready"}
      />
      <StatusFact
        label="Install"
        value={bootstrap?.installation.label ?? "Unknown"}
        ready={bootstrap?.installation.state === "ready"}
      />
      <StatusFact
        label="Mode"
        value={bootstrap?.paperModeEnabled ? "Paper" : "Safe idle"}
      />
      <StatusFact
        label="Telemetry"
        value={bootstrap?.telemetryEnabled ? "On" : "Off"}
      />
      <time className="ml-auto tabular-nums" dateTime={new Date().toISOString()}>
        {new Intl.DateTimeFormat(undefined, {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          timeZoneName: "short",
        }).format(new Date())}
      </time>
    </section>
  )
}

function StatusFact({
  label,
  value,
  ready = false,
}: {
  label: string
  value: string
  ready?: boolean
}) {
  return (
    <span className="flex items-center gap-1.5">
      {ready ? (
        <span
          className="size-1.5 rounded-full bg-[var(--success)]"
          aria-hidden="true"
        />
      ) : null}
      <span>{label}</span>
      <strong className="font-medium text-foreground/80">{value}</strong>
    </span>
  )
}
