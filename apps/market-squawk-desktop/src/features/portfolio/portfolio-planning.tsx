import * as React from "react"
import { useMutation } from "@tanstack/react-query"
import { AlertCircle, ArrowRightLeft, ShieldCheck, Target } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { formatMoney, humanize } from "@/lib/formatters"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  parsePortfolioResult,
  portfolioCandidateImpactSchema,
  portfolioRebalanceSchema,
  type PortfolioAccount,
  type PortfolioCandidateImpact,
  type PortfolioHolding,
  type PortfolioRebalance,
} from "./portfolio-contracts"
import {
  basisPointsToPercentage,
  basisPointsToUnitRate,
  formatPercent,
  percentageToBasisPoints,
  shortIdentity,
} from "./portfolio-format"

const NONNEGATIVE_DECIMAL = /^(?:0|[1-9]\d*)(?:\.\d+)?$/
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i

export function PortfolioPlanning({
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
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <header>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Plan before acting
        </p>
        <h2 className="mt-2 text-lg font-semibold">Portfolio planning</h2>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Compare a position quantity or create a rebalance plan. These tools cannot submit orders.
        </p>
      </header>
      <div className="mt-5 grid gap-4 xl:grid-cols-2">
        <CandidatePlanner
          account={account}
          holdings={holdings}
          transport={transport}
          available={capabilities.has("portfolio_candidate_impact")}
        />
        <RebalancePlanner
          account={account}
          holdings={holdings}
          transport={transport}
          available={capabilities.has("portfolio_rebalance")}
        />
      </div>
    </section>
  )
}

function CandidatePlanner({
  account,
  holdings,
  transport,
  available,
}: {
  account: PortfolioAccount
  holdings: PortfolioHolding[]
  transport: ProductTransport
  available: boolean
}) {
  const [instrumentId, setInstrumentId] = React.useState(holdings[0]?.instrumentId ?? "")
  const selected = holdings.find((holding) => holding.instrumentId === instrumentId)
  const [proposedQuantity, setProposedQuantity] = React.useState(selected?.quantity ?? "")
  const [shock, setShock] = React.useState("-10")
  const [validation, setValidation] = React.useState<string | null>(null)

  React.useEffect(() => {
    const holding = holdings[0]
    setInstrumentId(holding?.instrumentId ?? "")
    setProposedQuantity(holding?.quantity ?? "")
  }, [account.currentSnapshot.snapshotToken, holdings])

  const mutation = useMutation({
    mutationFn: async ({
      selectedInstrumentId,
      quantity,
      shockRate,
    }: {
      selectedInstrumentId: string
      quantity: string
      shockRate: string
    }) =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioCandidateImpact",
          instrumentId: selectedInstrumentId,
          proposedQuantity: quantity,
          scenarioShock: shockRate,
        }),
        portfolioCandidateImpactSchema,
      ).value,
  })

  const selectHolding = (nextId: string) => {
    setInstrumentId(nextId)
    const holding = holdings.find((row) => row.instrumentId === nextId)
    if (holding) setProposedQuantity(holding.quantity)
    mutation.reset()
  }

  const run = () => {
    setValidation(null)
    if (!UUID.test(instrumentId)) {
      setValidation("Choose a holding or enter the exact instrument ID for a new position.")
      return
    }
    if (!NONNEGATIVE_DECIMAL.test(proposedQuantity)) {
      setValidation("Enter a non-negative exact quantity without symbols, commas, or exponents.")
      return
    }
    const shockBasisPoints = percentageToBasisPoints(shock)
    if (shockBasisPoints === null || shockBasisPoints < -10_000 || shockBasisPoints > 10_000) {
      setValidation("Enter a stress percentage from -100 to 100 with at most two decimal places.")
      return
    }
    mutation.mutate({
      selectedInstrumentId: instrumentId,
      quantity: proposedQuantity,
      shockRate: basisPointsToUnitRate(shockBasisPoints),
    })
  }

  return (
    <div className="rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <Target className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Compare a position quantity</h3>
      </div>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
        Choose a holding or enter an asset ID for a new position. Current price, terms, and account
        details are used for the comparison.
      </p>
      {!available ? (
        <Unavailable text="Position impact analysis is unavailable right now." />
      ) : (
        <>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label
              htmlFor="portfolio-candidate-instrument"
              className="grid gap-1.5 text-xs sm:col-span-2"
            >
              <span className="font-medium">Instrument ID</span>
              <Input
                id="portfolio-candidate-instrument"
                list="portfolio-candidate-instruments"
                value={instrumentId}
                onChange={(event) => selectHolding(event.target.value)}
                placeholder="Choose a holding or enter an exact instrument ID"
              />
            </label>
            <datalist id="portfolio-candidate-instruments">
              {holdings.map((holding) => (
                <option key={holding.instrumentId} value={holding.instrumentId}>
                  {shortIdentity(holding.instrumentId, "Asset")}
                </option>
              ))}
            </datalist>
            <label htmlFor="portfolio-candidate-quantity" className="grid gap-1.5 text-xs">
              <span className="font-medium">Proposed quantity</span>
              <Input
                id="portfolio-candidate-quantity"
                inputMode="decimal"
                value={proposedQuantity}
                onChange={(event) => setProposedQuantity(event.target.value)}
              />
              <span className="text-[10px] text-muted-foreground">
                {selected
                  ? `Current ${selected.quantity} · lot ${selected.lotSize}`
                  : "Lot-size alignment is checked before the comparison."}
              </span>
            </label>
            <label htmlFor="portfolio-candidate-shock" className="grid gap-1.5 text-xs">
              <span className="font-medium">Stress change (%)</span>
              <Input
                id="portfolio-candidate-shock"
                type="number"
                inputMode="decimal"
                min="-100"
                max="100"
                step="0.01"
                value={shock}
                onChange={(event) => setShock(event.target.value)}
              />
            </label>
          </div>
          <Button className="mt-4" onClick={run} disabled={mutation.isPending}>
            {mutation.isPending ? "Calculating…" : "Compare impact"}
          </Button>
          {validation ? <PlanningError text={validation} /> : null}
          {mutation.isError ? (
            <PlanningError text="The position comparison could not be calculated right now. Try again." />
          ) : null}
          {mutation.data ? <CandidateResult result={mutation.data} /> : null}
        </>
      )}
    </div>
  )
}

function CandidateResult({ result }: { result: PortfolioCandidateImpact }) {
  return (
    <div className="mt-4 rounded-lg border border-border bg-card/30 p-4" aria-live="polite">
      <dl className="grid gap-3 sm:grid-cols-2">
        <Fact label="Selected account" value={shortIdentity(result.accountId, "Account")} />
        <Fact label="Current value" value={formatMoney(result.currentMarketValue)} />
        <Fact label="Proposed value" value={formatMoney(result.proposedMarketValue)} />
        <Fact label="Capital change" value={formatMoney(result.capitalChange)} />
        <Fact label="Portfolio value" value={formatMoney(result.portfolioValue)} />
        <Fact
          label="Position type"
          value={result.positionState === "new" ? "New position" : "Existing holding"}
        />
        <Fact label="Current quantity" value={result.currentQuantity} />
        <Fact label="Proposed quantity" value={result.proposedQuantity} />
        <Fact label="Concentration change" value={formatPercent(result.concentration.change)} />
        <Fact label="Marginal stress impact" value={formatMoney(result.scenario.marginalImpact)} />
        <Fact label="Selected price" value={formatMoney(result.price.amount)} />
        <Fact label="Price confidence" value={humanize(result.price.confidence)} />
        <Fact label="Lot size" value={result.instrumentTerms.lotSize} />
        <Fact label="Contract multiplier" value={result.instrumentTerms.contractMultiplier} />
      </dl>
      <div className="mt-4 grid gap-2 sm:grid-cols-2">
        <EvidenceStatus label="Estimated fees" evidence={result.costs.fees} />
        <EvidenceStatus label="Estimated slippage" evidence={result.costs.slippage} />
      </div>
      <Alert className="mt-4 border-amber-400/25 bg-amber-400/5">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Risk review remains incomplete</AlertTitle>
        <AlertDescription>
          <ul className="mt-1 list-disc space-y-1 pl-4">
            {result.missingInformation.map((label) => (
              <li key={label}>{label} is unavailable.</li>
            ))}
            {result.riskAssessment.checksUnavailable > 0 ? (
              <li>{result.riskAssessment.checksUnavailable} additional checks are unavailable.</li>
            ) : null}
          </ul>
        </AlertDescription>
      </Alert>
      <AuthorityNote />
    </div>
  )
}

function EvidenceStatus({
  label,
  evidence,
}: {
  label: string
  evidence: PortfolioCandidateImpact["costs"]["fees"]
}) {
  return (
    <div className="rounded-md border border-border/70 p-3 text-xs">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono">
        {evidence.state === "available" ? formatMoney(evidence.amount) : "Unavailable"}
      </p>
    </div>
  )
}

function RebalancePlanner({
  account,
  holdings,
  transport,
  available,
}: {
  account: PortfolioAccount
  holdings: PortfolioHolding[]
  transport: ProductTransport
  available: boolean
}) {
  const [targets, setTargets] = React.useState<Record<string, string>>(() => equalWeights(holdings))
  const [maxTurnover, setMaxTurnover] = React.useState("20")
  const [minimumCash, setMinimumCash] = React.useState("0")
  const [validation, setValidation] = React.useState<string | null>(null)

  React.useEffect(() => {
    setTargets(equalWeights(holdings))
  }, [account.currentSnapshot.snapshotToken, holdings])

  const mutation = useMutation({
    mutationFn: async (proposal: Record<string, unknown>) =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioRebalance",
          accountId: account.accountId,
          proposal,
        }),
        portfolioRebalanceSchema,
      ).value,
  })

  const run = () => {
    setValidation(null)
    const targetRows = holdings.map((holding) => ({
      instrumentId: holding.instrumentId,
      basisPoints: percentageToBasisPoints(targets[holding.instrumentId] ?? ""),
    }))
    if (targetRows.some((row) => row.basisPoints === null)) {
      setValidation("Every target must be a percentage with at most two decimal places.")
      return
    }
    const admitted = targetRows as { instrumentId: string; basisPoints: number }[]
    if (admitted.some((row) => row.basisPoints < 0 || row.basisPoints > 10_000)) {
      setValidation("Each target must be between 0% and 100%.")
      return
    }
    if (admitted.reduce((total, row) => total + row.basisPoints, 0) !== 10_000) {
      setValidation("Target weights must total exactly 100%.")
      return
    }
    const turnoverBasisPoints = percentageToBasisPoints(maxTurnover)
    if (
      turnoverBasisPoints === null ||
      turnoverBasisPoints < 0 ||
      turnoverBasisPoints > 10_000
    ) {
      setValidation("Maximum turnover must be between 0% and 100%.")
      return
    }
    if (!NONNEGATIVE_DECIMAL.test(minimumCash)) {
      setValidation("Minimum cash must be a non-negative exact amount without symbols or commas.")
      return
    }
    mutation.mutate({
      targets: admitted.map((row) => ({
        instrumentId: row.instrumentId,
        targetWeight: basisPointsToUnitRate(row.basisPoints),
      })),
      maxProposals: holdings.length,
      maxTurnover: basisPointsToUnitRate(turnoverBasisPoints),
      minimumCash: { amount: minimumCash, currency: account.currency },
      allowShort: false,
    })
  }

  const totalBasisPoints = holdings.reduce(
    (total, holding) => total + (percentageToBasisPoints(targets[holding.instrumentId] ?? "") ?? 0),
    0,
  )

  return (
    <div className="rounded-lg border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <ArrowRightLeft className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Draft a rebalance proposal</h3>
      </div>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
        The starting fields are equal-weight targets. Adjust every holding so the total remains
        exactly 100%; short positions are disabled in this guided workflow.
      </p>
      {!available ? (
        <Unavailable text="Rebalance planning is unavailable right now." />
      ) : holdings.length === 0 ? (
        <Unavailable text="A rebalance proposal requires at least one holding." />
      ) : (
        <>
          <div className="mt-4 max-h-56 space-y-2 overflow-y-auto pr-1">
            {holdings.map((holding) => (
              <label
                key={holding.instrumentId}
                className="grid grid-cols-[1fr_7rem] items-center gap-3 text-xs"
              >
                <span className="truncate">
                  {shortIdentity(holding.instrumentId, "Asset")}
                </span>
                <span className="relative">
                  <Input
                    type="number"
                    inputMode="decimal"
                    min="0"
                    max="100"
                    step="0.01"
                    value={targets[holding.instrumentId] ?? ""}
                    onChange={(event) =>
                      setTargets((current) => ({
                        ...current,
                        [holding.instrumentId]: event.target.value,
                      }))
                    }
                    className="pr-7 text-right font-mono"
                  />
                  <span className="pointer-events-none absolute right-3 top-2 text-muted-foreground">%</span>
                </span>
              </label>
            ))}
          </div>
          <p
            className={`mt-2 text-right font-mono text-xs ${
              totalBasisPoints === 10_000 ? "text-emerald-300" : "text-amber-300"
            }`}
          >
            Total {basisPointsToPercentage(totalBasisPoints)}%
          </p>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label className="grid gap-1.5 text-xs">
              <span className="font-medium">Maximum turnover (%)</span>
              <Input
                type="number"
                inputMode="decimal"
                min="0"
                max="100"
                step="0.01"
                value={maxTurnover}
                onChange={(event) => setMaxTurnover(event.target.value)}
              />
            </label>
            <label className="grid gap-1.5 text-xs">
              <span className="font-medium">Minimum cash ({account.currency})</span>
              <Input
                inputMode="decimal"
                value={minimumCash}
                onChange={(event) => setMinimumCash(event.target.value)}
              />
            </label>
          </div>
          <Button className="mt-4" onClick={run} disabled={mutation.isPending}>
            {mutation.isPending ? "Calculating…" : "Create proposal"}
          </Button>
          {validation ? <PlanningError text={validation} /> : null}
          {mutation.isError ? (
            <PlanningError text="The rebalance plan could not be calculated right now. Try again." />
          ) : null}
          {mutation.data ? <RebalanceResult result={mutation.data} /> : null}
        </>
      )}
    </div>
  )
}

function RebalanceResult({ result }: { result: PortfolioRebalance }) {
  return (
    <div className="mt-4 rounded-lg border border-border bg-card/30 p-4" aria-live="polite">
      <div className="flex justify-between gap-3 text-xs">
        <span className="text-muted-foreground">Projected cash</span>
        <span className="font-mono">{formatMoney(result.projectedCash)}</span>
      </div>
      <div className="mt-2 flex justify-between gap-3 text-xs">
        <span className="text-muted-foreground">Turnover</span>
        <span className="font-mono">{formatPercent(result.turnover)}</span>
      </div>
      <div className="mt-4 space-y-2">
        {result.trades.length ? (
          result.trades.map((trade) => (
            <div key={trade.instrumentId} className="flex justify-between gap-3 text-xs">
              <span className="truncate">{shortIdentity(trade.instrumentId, "Asset")}</span>
              <span className="shrink-0 font-mono">{formatMoney(trade.valueChange)}</span>
            </div>
          ))
        ) : (
          <p className="text-xs text-muted-foreground">No value changes were required.</p>
        )}
      </div>
      {result.constrained ? (
        <p className="mt-3 text-[11px] text-amber-200">
          The proposal was reduced to respect cash or turnover limits.
        </p>
      ) : null}
      <AuthorityNote />
    </div>
  )
}

function equalWeights(holdings: PortfolioHolding[]) {
  if (holdings.length === 0) return {}
  const base = Math.floor(10_000 / holdings.length)
  let remainder = 10_000 - base * holdings.length
  return Object.fromEntries(
    holdings.map((holding) => {
      const basisPoints = base + (remainder > 0 ? 1 : 0)
      remainder = Math.max(0, remainder - 1)
      return [holding.instrumentId, basisPointsToPercentage(basisPoints)]
    }),
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 font-mono text-xs tabular-nums">{value}</dd>
    </div>
  )
}

function AuthorityNote() {
  return (
    <div className="mt-4 flex gap-2 rounded-md border border-emerald-400/20 bg-emerald-400/5 p-3 text-[11px] leading-5 text-muted-foreground">
      <ShieldCheck className="mt-0.5 size-4 shrink-0 text-emerald-300" aria-hidden="true" />
      <span>
        Analysis only. No portfolio mutation, risk reservation, approval, or order was created.
      </span>
    </div>
  )
}

function PlanningError({ text }: { text: string }) {
  return (
    <Alert variant="destructive" className="mt-4">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>Plan could not be calculated</AlertTitle>
      <AlertDescription>{text}</AlertDescription>
    </Alert>
  )
}

function Unavailable({ text }: { text: string }) {
  return (
    <div className="mt-4 rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
      {text}
    </div>
  )
}
