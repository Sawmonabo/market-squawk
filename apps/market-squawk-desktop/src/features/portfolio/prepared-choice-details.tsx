import { formatProductTime } from "./portfolio-format"

export function PreparedChoiceDetails({
  choice,
}: {
  choice: {
    action: string
    horizon: string
    range: string
    reasons: string[]
    risks: string[]
    assumptions: string[]
    expiresAt: string
    invalidators: string[]
    uncertainty: string
  }
}) {
  return (
    <div className="mt-4 space-y-4 rounded-lg border border-border bg-background/30 p-4">
      <dl className="grid gap-3 sm:grid-cols-3">
        <Fact label="Action" value={choice.action} />
        <Fact label="Horizon" value={choice.horizon} />
        <Fact label="Range" value={choice.range} />
      </dl>
      <PreparedList title="Why this may help" items={choice.reasons} />
      <PreparedList title="Risks" items={choice.risks} />
      <PreparedList title="Assumptions" items={choice.assumptions} />
      <PreparedList title="What would invalidate it" items={choice.invalidators} />
      <dl className="grid gap-3 sm:grid-cols-2">
        <Fact label="Valid until" value={formatProductTime(choice.expiresAt)} />
        <Fact label="Uncertainty" value={choice.uncertainty} />
      </dl>
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-sm leading-5">{value}</dd>
    </div>
  )
}

function PreparedList({ title, items }: { title: string; items: string[] }) {
  return (
    <div>
      <h4 className="text-xs font-semibold">{title}</h4>
      <ul className="mt-2 space-y-1 text-xs leading-5 text-muted-foreground">
        {items.map((item) => (
          <li key={item}>• {item}</li>
        ))}
      </ul>
    </div>
  )
}
