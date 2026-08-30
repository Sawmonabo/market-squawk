import * as React from "react"
import { useMutation } from "@tanstack/react-query"
import { AlertCircle, FlaskConical, Layers3 } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { formatMoney } from "@/lib/formatters"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  parsePortfolioResult,
  portfolioScenarioBatchResultSchema,
  portfolioScenarioResultSchema,
  type PortfolioAccount,
  type PortfolioHolding,
  type PortfolioScenarioBatchResult,
  type PortfolioScenarioResult,
} from "./portfolio-contracts"
import {
  basisPointsToUnitRate,
  percentageToBasisPoints,
  shortIdentity,
} from "./portfolio-format"

type ScenarioOutput =
  | { kind: "single"; value: PortfolioScenarioResult }
  | { kind: "batch"; value: PortfolioScenarioBatchResult }

export function PortfolioScenarios({
  account,
  holdings,
  bootstrap,
  transport,
}: {
  account: PortfolioAccount
  holdings: PortfolioHolding[]
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const capabilities = productCapabilitySet(bootstrap)
  const canRunOne = capabilities.has("portfolio_scenario")
  const canRunBatch = capabilities.has("portfolio_scenario_batch")
  const [scope, setScope] = React.useState("all")
  const [shock, setShock] = React.useState("-10")
  const [composition, setComposition] = React.useState<"additive" | "compounded">("additive")
  const [validation, setValidation] = React.useState<string | null>(null)

  React.useEffect(() => {
    setScope("all")
  }, [account.accountId])

  const mutation = useMutation({
    mutationFn: async ({ batch, basisPoints }: { batch: boolean; basisPoints: number }) => {
      const instrumentIds =
        scope === "all" ? holdings.map((holding) => holding.instrumentId) : [scope]
      const scenarios = (batch ? [1, 2, 3] : [1]).map((multiple) => {
        const scaled = basisPoints * multiple
        return {
          id: scenarioId(scaled, multiple),
          composition,
          shocks: instrumentIds.map((instrumentId) => ({
            instrumentId,
            rate: basisPointsToUnitRate(scaled),
          })),
        }
      })
      if (batch) {
        return {
          kind: "batch" as const,
          value: parsePortfolioResult(
            await transport.query({
              query: "portfolioScenarioBatch",
              accountId: account.accountId,
              scenarios,
            }),
            portfolioScenarioBatchResultSchema,
          ).value,
        }
      }
      const scenario = scenarios[0]
      if (!scenario) throw new Error("A stress assumption is required.")
      return {
        kind: "single" as const,
        value: parsePortfolioResult(
          await transport.query({
            query: "portfolioScenario",
            accountId: account.accountId,
            scenario,
          }),
          portfolioScenarioResultSchema,
        ).value,
      }
    },
  })

  const run = (batch: boolean) => {
    setValidation(null)
    const basisPoints = percentageToBasisPoints(shock)
    if (basisPoints === null || basisPoints < -10_000 || basisPoints > 10_000) {
      setValidation("Enter a percentage from -100 to 100 with at most two decimal places.")
      return
    }
    if (basisPoints === 0) {
      setValidation("Enter a non-zero change so the stress result is useful.")
      return
    }
    if (batch && Math.abs(basisPoints) > 3_333) {
      setValidation("The three-level range requires a starting change no larger than 33.33%.")
      return
    }
    mutation.mutate({ batch, basisPoints })
  }

  const results = scenarioRows(mutation.data ?? null)

  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Test an assumption
        </p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio stress lab</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Apply an exact price change to one holding or the entire portfolio. Results are
          deterministic sensitivity analysis, not predicted prices.
        </p>
      </header>

      {!canRunOne && !canRunBatch ? (
        <Unavailable text="Portfolio stress analysis is unavailable right now." />
      ) : holdings.length === 0 ? (
        <Unavailable text="Stress analysis requires at least one holding with a current price." />
      ) : (
        <>
          <div className="mt-5 grid gap-3 md:grid-cols-3">
            <label className="grid gap-1.5 text-xs">
              <span className="font-medium">Apply to</span>
              <select
                value={scope}
                onChange={(event) => setScope(event.target.value)}
                className="h-9 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="all">Every holding</option>
                {holdings.map((holding) => (
                  <option key={holding.instrumentId} value={holding.instrumentId}>
                    {shortIdentity(holding.instrumentId, "Asset")}
                  </option>
                ))}
              </select>
            </label>
            <label className="grid gap-1.5 text-xs">
              <span className="font-medium">Price change (%)</span>
              <Input
                type="number"
                inputMode="decimal"
                min="-100"
                max="100"
                step="0.01"
                value={shock}
                onChange={(event) => setShock(event.target.value)}
                aria-invalid={validation !== null}
              />
            </label>
            <label className="grid gap-1.5 text-xs">
              <span className="font-medium">Multiple shocks</span>
              <select
                value={composition}
                onChange={(event) =>
                  setComposition(event.target.value as "additive" | "compounded")
                }
                className="h-9 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="additive">Add changes together</option>
                <option value="compounded">Compound changes</option>
              </select>
            </label>
          </div>
          <div className="mt-4 flex flex-wrap gap-2">
            <Button onClick={() => run(false)} disabled={!canRunOne || mutation.isPending}>
              <FlaskConical aria-hidden="true" />
              {mutation.isPending ? "Calculating…" : "Run stress"}
            </Button>
            <Button
              variant="outline"
              onClick={() => run(true)}
              disabled={!canRunBatch || mutation.isPending}
            >
              <Layers3 aria-hidden="true" />
              Run 3-level range
            </Button>
          </div>
          {validation ? <InlineError text={validation} /> : null}
          {mutation.isError ? (
            <InlineError text="The stress test could not be calculated right now. Try again." />
          ) : null}
          {results.length ? <ScenarioResults results={results} /> : null}
        </>
      )}
    </section>
  )
}

function ScenarioResults({
  results,
}: {
  results: PortfolioScenarioResult["scenario"][]
}) {
  return (
    <div className="mt-5 grid gap-3 md:grid-cols-3" aria-live="polite">
      {results.map((result) => (
        <div key={result.id} className="rounded-lg border border-border bg-background/30 p-4">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
            {result.id.replaceAll("-", " ")}
          </p>
          <p className="mt-2 font-mono text-lg font-semibold tabular-nums">
            {formatMoney(result.total)}
          </p>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Estimated change across {result.contributions.length} holding
            {result.contributions.length === 1 ? "" : "s"}.
          </p>
        </div>
      ))}
    </div>
  )
}

function scenarioRows(output: ScenarioOutput | null) {
  if (!output) return []
  return output.kind === "single" ? [output.value.scenario] : output.value.scenarios
}

function scenarioId(basisPoints: number, level: number) {
  const direction = basisPoints < 0 ? "down" : "up"
  return `dashboard-${direction}-${Math.abs(basisPoints)}bp-level-${level}`
}

function InlineError({ text }: { text: string }) {
  return (
    <Alert variant="destructive" className="mt-4">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>Stress could not be calculated</AlertTitle>
      <AlertDescription>{text}</AlertDescription>
    </Alert>
  )
}

function Unavailable({ text }: { text: string }) {
  return (
    <div className="mt-5 rounded-lg border border-dashed border-border p-5 text-sm text-muted-foreground">
      {text}
    </div>
  )
}
