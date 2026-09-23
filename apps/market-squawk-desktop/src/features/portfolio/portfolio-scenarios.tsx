import * as React from "react"
import { FlaskConical } from "lucide-react"

import { formatMoney } from "@/lib/formatters"

import type { PortfolioStressChoice } from "./portfolio-contracts"
import { PreparedChoiceDetails } from "./prepared-choice-details"

export function PortfolioScenarios({
  choices,
}: {
  choices: PortfolioStressChoice[] | null
}) {
  const [selectedToken, setSelectedToken] = React.useState("")
  const selected = choices?.find((choice) => choice.actionToken === selectedToken) ?? null

  React.useEffect(() => setSelectedToken(""), [choices])

  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Test an assumption
        </p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio stress lab</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Inspect a prepared market scenario without treating it as a predicted price. No scenario
          is selected automatically.
        </p>
      </header>
      {!choices || choices.length === 0 ? (
        <UnavailableScenario />
      ) : (
        <>
          <label className="mt-5 grid gap-1.5 text-xs">
            <span className="font-semibold">Scenario</span>
            <select
              value={selectedToken}
              onChange={(event) => setSelectedToken(event.target.value)}
              className="h-9 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">Choose a scenario</option>
              {choices.map((choice) => (
                <option key={choice.actionToken} value={choice.actionToken}>
                  {choice.title}
                </option>
              ))}
            </select>
          </label>
          {selected ? (
            <>
              <PreparedChoiceDetails choice={selected} />
              <div className="mt-4 rounded-lg border border-amber-400/20 bg-amber-400/5 p-4">
                <p className="text-sm font-semibold">Prepared result</p>
                <p className="mt-2 text-xs leading-5 text-muted-foreground">
                  {selected.result}
                </p>
                {selected.estimatedImpact ? (
                  <p className="mt-2 font-mono text-sm tabular-nums">
                    {formatMoney(selected.estimatedImpact)}
                  </p>
                ) : null}
              </div>
            </>
          ) : null}
        </>
      )}
    </section>
  )
}

function UnavailableScenario() {
  return (
    <div className="mt-5 flex gap-3 rounded-lg border border-dashed border-border p-5 text-sm text-muted-foreground">
      <FlaskConical className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      <p>
        No complete stress choices are available for this portfolio. No estimate is shown until
        the assumption, affected investments, result, risks, and uncertainty can be presented
        together.
      </p>
    </div>
  )
}
