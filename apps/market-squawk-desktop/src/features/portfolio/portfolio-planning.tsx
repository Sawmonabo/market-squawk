import * as React from "react"
import { ArrowRightLeft, Target } from "lucide-react"

import { formatMoney } from "@/lib/formatters"

import type {
  PortfolioPositionChoice,
  PortfolioRebalanceChoice,
} from "./portfolio-contracts"
import { investmentDisplayName } from "./portfolio-format"
import { PreparedChoiceDetails } from "./prepared-choice-details"

export function PortfolioPlanning({
  positionChoices,
  rebalanceChoices,
}: {
  positionChoices: PortfolioPositionChoice[] | null
  rebalanceChoices: PortfolioRebalanceChoice[] | null
}) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Plan before acting
        </p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio planning</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Review complete position and rebalance choices before making a decision. Planning cannot
          place an order, and no choice is selected automatically.
        </p>
      </header>
      <div className="mt-5 space-y-4">
        <PositionPlanner choices={positionChoices} />
        <RebalancePlanner choices={rebalanceChoices} />
      </div>
    </section>
  )
}

function PositionPlanner({ choices }: { choices: PortfolioPositionChoice[] | null }) {
  const [selectedToken, setSelectedToken] = React.useState("")
  const selected = choices?.find((choice) => choice.actionToken === selectedToken) ?? null
  React.useEffect(() => setSelectedToken(""), [choices])

  return (
    <PlanningChoice
      icon={Target}
      title="Compare a position change"
      unavailable={
        !choices || choices.length === 0
          ? "No complete position choices are available. Market Squawk will not assume an investment, quantity, price, or cost."
          : null
      }
    >
      {choices && choices.length > 0 ? (
        <>
          <ChoiceSelect
            label="Position choice"
            value={selectedToken}
            options={choices.map((choice) => ({
              token: choice.actionToken,
              label: `${investmentDisplayName(choice.investment)} · ${choice.title}`,
            }))}
            select={setSelectedToken}
          />
          {selected ? <PreparedChoiceDetails choice={selected} /> : null}
        </>
      ) : null}
    </PlanningChoice>
  )
}

function RebalancePlanner({ choices }: { choices: PortfolioRebalanceChoice[] | null }) {
  const [selectedToken, setSelectedToken] = React.useState("")
  const selected = choices?.find((choice) => choice.actionToken === selectedToken) ?? null
  React.useEffect(() => setSelectedToken(""), [choices])

  return (
    <PlanningChoice
      icon={ArrowRightLeft}
      title="Review a rebalance plan"
      unavailable={
        !choices || choices.length === 0
          ? "No complete rebalance choices are available. Market Squawk will not assume allocation targets, turnover, cash, costs, or concentration limits."
          : null
      }
    >
      {choices && choices.length > 0 ? (
        <>
          <ChoiceSelect
            label="Rebalance choice"
            value={selectedToken}
            options={choices.map((choice) => ({
              token: choice.actionToken,
              label: choice.title,
            }))}
            select={setSelectedToken}
          />
          {selected ? (
            <>
              <PreparedChoiceDetails choice={selected} />
              <dl className="mt-4 grid gap-3 sm:grid-cols-2">
                <PlanFact
                  label="Estimated turnover"
                  value={selected.estimatedTurnover?.display ?? "Not available"}
                />
                <PlanFact
                  label="Estimated costs"
                  value={
                    selected.estimatedCosts
                      ? formatMoney(selected.estimatedCosts)
                      : "Not available"
                  }
                />
              </dl>
            </>
          ) : null}
        </>
      ) : null}
    </PlanningChoice>
  )
}

function PlanningChoice({
  icon: Icon,
  title,
  unavailable,
  children,
}: {
  icon: typeof Target
  title: string
  unavailable: string | null
  children: React.ReactNode
}) {
  return (
    <div className="rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">{title}</h3>
      </div>
      {unavailable ? (
        <p className="mt-3 text-xs leading-5 text-muted-foreground">{unavailable}</p>
      ) : (
        children
      )}
    </div>
  )
}

function ChoiceSelect({
  label,
  value,
  options,
  select,
}: {
  label: string
  value: string
  options: { token: string; label: string }[]
  select: (token: string) => void
}) {
  return (
    <label className="mt-4 grid gap-1.5 text-xs">
      <span className="font-semibold">{label}</span>
      <select
        value={value}
        onChange={(event) => select(event.target.value)}
        className="h-9 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <option value="">Choose an option</option>
        {options.map((option) => (
          <option key={option.token} value={option.token}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  )
}

function PlanFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-sm tabular-nums">{value}</dd>
    </div>
  )
}
