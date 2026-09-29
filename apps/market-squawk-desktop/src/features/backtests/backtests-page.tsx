import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  AlertCircle,
  BarChart3,
  CalendarClock,
  CircleDollarSign,
  FlaskConical,
  GitCompareArrows,
  Play,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"

import { useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { ProductCapability } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"
import { formatLosslessInteger, formatUnixNanos } from "@/features/opportunities/format"

import {
  newestBacktests,
  parseBacktestActivities,
  parseBacktestPreparationOptions,
  parseBacktestPreparationPreview,
  parseBacktestResult,
  parseBacktestStart,
  parseRecommendationBacktest,
  type BacktestActivity,
  type BacktestPreparationOptions,
  type BacktestPreparationPreview,
  type BacktestPreparationSelection,
  type BacktestResult,
  type CompletedBacktest,
  type RecommendationBacktest,
} from "./contracts"

const PREPARATION_CAPABILITIES = [
  "backtest_preparation",
  "backtest_prepared_start",
] as const
const CONTROL_CLASS =
  "h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"

export function RecommendationStudyPanel({
  actionToken,
  transport,
  scope,
}: {
  actionToken: string
  transport: ProductTransport
  scope: ProductScope
}) {
  const [opened, setOpened] = React.useState(false)
  const study = useQuery({
    queryKey: productKeys.operation(scope, "backtest", "Analysis.GetRecommendationBacktest", { actionToken }),
    enabled: opened,
    retry: false,
    queryFn: async () => parseRecommendationBacktest(
      await transport.backtestProducts({ action: "recommendationStudy", actionToken }),
    ),
  })
  return (
    <section className="mt-6 rounded-lg border border-border p-4">
      <h3 className="text-sm font-semibold">Saved historical study</h3>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Open the study used by this analysis to review simulated results, comparisons, costs,
        and every included or incomplete observation.
      </p>
      <Button
        type="button"
        size="sm"
        variant="outline"
        className="mt-3"
        disabled={opened && study.isFetching}
        onClick={() => opened ? void study.refetch() : setOpened(true)}
      >
        {opened ? "Refresh saved study" : "Open saved study"}
      </Button>
      {opened && study.isPending ? <Loading label="Opening the saved study…" /> : null}
      {opened && study.isError ? (
        <Unavailable
          title="This saved study could not be opened"
          detail="Its complete historical evidence could not be retrieved. Try refreshing the saved study."
        />
      ) : null}
      {opened && study.isSuccess ? <RecommendationStudyReport report={study.data} /> : null}
    </section>
  )
}

function RecommendationStudyReport({ report }: { report: RecommendationBacktest }) {
  const aggregate = report.aggregate
  const methodology = report.methodology
  const assumptions = report.executionAssumptions
  return (
    <div className="mt-5 grid gap-5">
      <div className="rounded-lg border border-border bg-background/25 p-3 text-xs leading-5">
        <p className="font-semibold">
          {report.studyBasis === "historical_as_known"
            ? "Simulation using information known at the time"
            : "Historical simulation using later data"}
        </p>
        <p className="mt-1 text-muted-foreground">
          {report.studyBasis === "historical_as_known"
            ? "Inputs were qualified as known at the historical decision times. These are simulated results."
            : "This study uses a fixed copy of historical data available later. Its results do not establish what could have been achieved with information known at the time."}
        </p>
        {report.studyLimitations.length > 0 ? (
          <ul className="mt-2 list-disc space-y-1 pl-4">
            {report.studyLimitations.map((limitation) => (
              <li key={limitation}>{STUDY_LIMITATION_LABELS[limitation]}</li>
            ))}
          </ul>
        ) : null}
      </div>

      {aggregate.status === "available" ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            <Metric label="Mean return after costs" value={studyPercent(aggregate.meanCostAdjustedReturn)} />
            <Metric label="Worst maximum drawdown" value={studyPercent(aggregate.worstMaximumDrawdown)} />
            <Metric label="Positive test periods" value={studyPercent(aggregate.positiveFoldStability)} />
          </div>
          <dl className="grid gap-3 sm:grid-cols-3">
            <Fact label="Completed observations in result" value={formatLosslessInteger(aggregate.observationCount)} />
            <Fact label="Independent test periods" value={formatLosslessInteger(aggregate.independentFoldCount)} />
            <Fact label="Periods with positive returns" value={formatLosslessInteger(aggregate.positiveFoldCount)} />
          </dl>
          <div className="grid gap-3 sm:grid-cols-2">
            <StudyComparison title="Main comparison" result={aggregate.benchmark} />
            <StudyComparison title="Accompanying comparison" result={aggregate.accompanyingBenchmark} />
          </div>
        </>
      ) : (
        <Unavailable title="An aggregate result is unavailable" detail={STUDY_AGGREGATE_REASONS[aggregate.reason]} />
      )}

      <div>
        <h4 className="text-sm font-semibold">Observation coverage</h4>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          These counts retain no-action decisions and incomplete outcomes. Comparison counts
          overlap the investment counts; they are not additional observations.
        </p>
        <StudyPopulation population={report.population} />
      </div>

      <details className="rounded-lg border border-border p-3">
        <summary className="cursor-pointer text-sm font-medium">Individual historical test periods</summary>
        <div className="mt-3 grid gap-3">
          {report.folds.map((fold, index) => (
            <details key={fold.foldId} className="rounded border border-border p-3">
              <summary className="cursor-pointer text-xs font-medium">Test period {index + 1}</summary>
              <dl className="mt-3 grid gap-3 sm:grid-cols-2">
                <Fact label="Starts" value={formatUnixNanos(fold.startsAtUnixNanos)} />
                <Fact label="Ends" value={formatUnixNanos(fold.endsAtUnixNanos)} />
              </dl>
              <StudyPopulation population={fold.population} />
            </details>
          ))}
        </div>
      </details>

      <details className="rounded-lg border border-border p-3">
        <summary className="cursor-pointer text-sm font-medium">Simulation assumptions and dates</summary>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <Fact label="Reporting currency" value={methodology.reportingCurrency} />
          <Fact label="Target horizon" value="365 elapsed days from the forecast’s financial starting point" />
          <Fact label="Decision delay" value={methodology.decisionLagNanos === null ? "Not specified" : `${formatStudyDecimal(methodology.decisionLagNanos, -9)} seconds`} />
          <Fact label="Execution prices" value={methodology.executionBasis === "completed-daily-bar" ? "Completed daily closing prices" : "Observed quotes and available size"} />
          <Fact label="Simulated order timing" value={methodology.fillTiming === "next-eligible-completed-bar-close" ? "Next eligible completed daily close" : "Next eligible market observation"} />
          <Fact label="Participation measured against" value={methodology.participationBasis === "completed-bar-traded-volume" ? "Daily traded volume" : "Observed available size"} />
          <Fact label="Assumed full spread" value={methodology.assumedFullSpreadBasisPoints === null ? "Observed spread" : `${formatStudyDecimal(methodology.assumedFullSpreadBasisPoints, -2)}%`} />
          <Fact label="Fees on each trade leg" value={`${formatStudyDecimal(assumptions.feeBasisPointsPerLeg, -2)}%`} />
          <Fact label="Slippage on each trade leg" value={`${formatStudyDecimal(assumptions.slippageBasisPointsPerLeg, -2)}%`} />
          <Fact label="Maximum additional random slippage" value={`${formatStudyDecimal(assumptions.maximumRandomSlippageBasisPointsPerLeg, -2)}%`} />
          <Fact label="Maximum participation" value={`${formatStudyDecimal(assumptions.maximumParticipationBasisPoints, -2)}%`} />
          <Fact label="Execution delay" value={`${formatStudyDecimal(assumptions.latencyNanos, -9)} seconds`} />
          <Fact label="Partial fills" value={assumptions.allowPartialFills ? "Allowed" : "Not allowed"} />
          <Fact label="Historical data snapshot" value={formatUnixNanos(report.snapshotAsOfUnixNanos)} />
          <Fact label="Simulation stops at" value={formatUnixNanos(report.simulationCutoffUnixNanos)} />
          <Fact label="Calculated" value={formatUnixNanos(report.evaluatedAtUnixNanos)} />
          <Fact label="Published" value={formatUnixNanos(report.publishedAtUnixNanos)} />
          <Fact label="Available for analysis from" value={formatUnixNanos(report.availableAtUnixNanos)} />
          <Fact label="Analytical use expires" value={formatUnixNanos(report.expiresAtUnixNanos)} />
        </dl>
        <p className="mt-3 text-xs leading-5 text-muted-foreground">
          The study uses unadjusted trading prices with recorded corporate actions.
          Distributions are treated as cash entitlements without reinvestment.
          Simulated costs are applied before prices are rounded against the trade.
          The saved report remains readable after its analytical use expires.
        </p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">{methodology.distributionTiming}</p>
        {methodology.executionLimitations.length > 0 ? (
          <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
            {methodology.executionLimitations.map((limitation) => <li key={limitation}>{limitation}</li>)}
          </ul>
        ) : null}
      </details>
    </div>
  )
}

function StudyComparison({ title, result }: {
  title: string
  result: Extract<RecommendationBacktest["aggregate"], { status: "available" }>["benchmark"]
}) {
  return (
    <div className="rounded-lg border border-border p-3">
      <h4 className="text-sm font-semibold">{title}</h4>
      {result.status === "available" ? (
        <dl className="mt-3 grid gap-3">
          <Fact label="Comparison mean return after costs" value={studyPercent(result.meanCostAdjustedReturn)} />
          <Fact label="Investment’s mean excess return" value={studyPercent(result.meanExcessReturn)} />
        </dl>
      ) : <p className="mt-2 text-xs text-muted-foreground">A complete comparison result is unavailable.</p>}
    </div>
  )
}

const STUDY_POPULATION_LABELS = {
  totalSignals: "All historical decisions",
  completedSubjectAndBenchmark: "Investment and main comparison completed",
  noAction: "No action",
  unavailable: "Decision unavailable",
  censoredTargetAfterCutoff: "Target beyond simulation cutoff",
  censoredOutsideFold: "Target outside its test period",
  entryUnfilled: "Entry unfilled",
  exitUnfilled: "Exit unfilled",
  benchmarkUnavailable: "Main comparison unavailable",
  completedSubject: "Investment completed",
  accompanyingBenchmarkCompleted: "Accompanying comparison completed",
  accompanyingBenchmarkUnavailable: "Accompanying comparison unavailable",
} satisfies Record<keyof RecommendationBacktest["population"], string>

function StudyPopulation({ population }: { population: RecommendationBacktest["population"] }) {
  return (
    <dl className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
      {Object.entries(STUDY_POPULATION_LABELS).map(([key, label]) => (
        <Fact key={key} label={label} value={formatLosslessInteger(population[key as keyof typeof population])} />
      ))}
    </dl>
  )
}

const STUDY_LIMITATION_LABELS = {
  historical_revision_coverage_unproven: "The complete history of data revisions has not been established.",
  later_vintage_inputs: "Some inputs became available after the historical decisions being simulated.",
  present_day_fixed_cohort: "The investment group was fixed using present-day membership, which may omit past failures or former members.",
  simulated_availability: "Historical availability is simulated rather than established from recorded publication times.",
} satisfies Record<RecommendationBacktest["studyLimitations"][number], string>

const STUDY_AGGREGATE_REASONS = {
  "truncated-signal-population": "The complete population of historical decisions was not retained.",
  "incomplete-declared-entry": "One or more declared entries could not be completed.",
  "missing-completed-observation-in-fold": "At least one historical test period has no completed observation.",
} satisfies Record<Extract<RecommendationBacktest["aggregate"], { status: "unavailable" }>["reason"], string>

function studyPercent(value: string): string {
  return `${formatStudyDecimal(value, 2)}%`
}

/** Changes display units by moving the decimal point; no financial metric is calculated here. */
function formatStudyDecimal(value: string, places: number): string {
  const negative = value.startsWith("-")
  const magnitude = negative ? value.slice(1) : value
  const separator = magnitude.indexOf(".")
  const whole = separator < 0 ? magnitude : magnitude.slice(0, separator)
  const fraction = separator < 0 ? "" : magnitude.slice(separator + 1)
  const digits = whole + fraction
  const position = whole.length + places
  const integer = (position <= 0 ? "0" : digits.slice(0, position).padEnd(position, "0")).replace(/^0+(?=\d)/, "")
  const decimal = (position <= 0 ? "0".repeat(-position) + digits : digits.slice(position)).replace(/0+$/, "")
  const sign = negative && /[1-9]/.test(integer + decimal) ? "-" : ""
  return `${sign}${integer}${decimal ? `.${decimal}` : ""}`
}

export function BacktestsPage() {
  const product = useProduct()
  if (product.status !== "ready") {
    return (
      <PageFrame>
        <Unavailable
          title="Backtests are unavailable"
          detail={
            product.status === "error"
              ? "Market Squawk could not open Backtests. Check Settings, then try again."
              : "Getting Backtests ready."
          }
        />
      </PageFrame>
    )
  }

  return (
    <BacktestsWorkspace
      transport={product.transport}
      scope={product.bootstrap.productSessionToken}
      capabilities={productCapabilitySet(product.bootstrap)}
    />
  )
}

function BacktestsWorkspace({
  transport,
  scope,
  capabilities,
}: {
  transport: ProductTransport
  scope: ProductScope
  capabilities: ReadonlySet<ProductCapability>
}) {
  const queryClient = useQueryClient()
  const activityAvailable = capabilities.has("backtest_activity")
  const resultAvailable = capabilities.has("backtest_result")
  const [selectedToken, setSelectedToken] = React.useState<string | null>(null)
  const activitiesKey = productKeys.operation(
    scope,
    "backtest",
    "Backtest.ListProductResults",
    {},
  )
  const activitiesQuery = useQuery({
    queryKey: activitiesKey,
    enabled: activityAvailable,
    refetchInterval: 5_000,
    queryFn: async () => {
      return parseBacktestActivities(
        await transport.backtestProducts({ action: "list" }),
      ).sort(newestBacktests)
    },
  })
  const selected =
    activitiesQuery.data?.find(
      (activity) => activity.backtestToken === selectedToken,
    ) ?? null
  const resultQuery = useQuery({
    queryKey: productKeys.operation(
      scope,
      "backtest",
      "Backtest.GetProductResult",
      { backtestToken: selected?.backtestToken ?? null },
    ),
    enabled: resultAvailable && selected?.state === "completed",
    queryFn: async () => {
      if (!selected) {
        throw new Error("No completed backtest is selected.")
      }
      return parseBacktestResult(
        await transport.backtestProducts({
          action: "get",
          backtestToken: selected.backtestToken,
        }),
      )
    },
  })

  return (
    <PageFrame>
      <BacktestBuilder
        transport={transport}
        scope={scope}
        capabilities={capabilities}
        onStarted={async () => {
          await queryClient.invalidateQueries({ queryKey: activitiesKey })
        }}
      />

      <section className="mt-7 grid gap-5 xl:grid-cols-[minmax(0,0.82fr)_minmax(0,1.18fr)]">
        <div>
          <div className="flex items-end justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Recent backtests</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Review progress and compare completed investment research.
              </p>
            </div>
            <Button
              size="sm"
              variant="outline"
              disabled={activitiesQuery.isFetching || !activityAvailable}
              onClick={() => void activitiesQuery.refetch()}
            >
              <RefreshCw
                className={cn(activitiesQuery.isFetching && "animate-spin")}
                aria-hidden="true"
              />
              Refresh
            </Button>
          </div>

          {!activityAvailable ? (
            <Unavailable
              title="Backtest results are unavailable"
              detail="Backtest result history is not available in this installation."
            />
          ) : activitiesQuery.isPending ? (
            <Loading label="Loading backtests…" />
          ) : activitiesQuery.isError ? (
            <Unavailable
              title="Backtests could not be loaded"
              detail="Refresh the page and try again."
            />
          ) : activitiesQuery.data.length === 0 ? (
            <Unavailable
              title="No backtests yet"
              detail="Prepare a backtest above to measure an investment approach against historical conditions."
            />
          ) : (
            <div className="mt-4 grid gap-2">
              {activitiesQuery.data.map((activity) => (
                <ActivityButton
                  key={activity.backtestToken}
                  activity={activity}
                  selected={
                    activity.backtestToken === selected?.backtestToken
                  }
                  onSelect={() => setSelectedToken(activity.backtestToken)}
                />
              ))}
            </div>
          )}
        </div>

        <BacktestEvidence
          activity={selected}
          result={resultQuery.data ?? null}
          loading={resultQuery.isPending && resultQuery.fetchStatus !== "idle"}
          error={resultQuery.isError}
        />
      </section>
    </PageFrame>
  )
}

function BacktestBuilder({
  transport,
  scope,
  capabilities,
  onStarted,
}: {
  transport: ProductTransport
  scope: ProductScope
  capabilities: ReadonlySet<ProductCapability>
  onStarted: () => Promise<unknown>
}) {
  const available = PREPARATION_CAPABILITIES.every((capability) =>
    capabilities.has(capability),
  )
  const [selection, setSelection] =
    React.useState<BacktestPreparationSelection | null>(null)
  const [preview, setPreview] =
    React.useState<BacktestPreparationPreview | null>(null)
  const [started, setStarted] = React.useState(false)
  const optionsQuery = useQuery({
    queryKey: productKeys.operation(
      scope,
      "backtest",
      "Backtest.GetPreparation",
      {},
    ),
    enabled: available,
    staleTime: 30_000,
    queryFn: async () =>
      parseBacktestPreparationOptions(
        await transport.backtestPreparation({ action: "options" }),
      ),
  })
  const previewMutation = useMutation({
    mutationFn: async (candidate: BacktestPreparationSelection) =>
      parseBacktestPreparationPreview(
        await transport.backtestPreparation({
          action: "preview",
          selection: candidate,
        }),
      ),
    onSuccess: setPreview,
  })
  const startMutation = useMutation({
    mutationFn: async (confirmationToken: string) =>
      parseBacktestStart(
        await transport.backtestPreparation(
          { action: "start", confirmationToken },
          true,
        ),
      ),
    onSuccess: async () => {
      setPreview(null)
      setStarted(true)
      await onStarted()
    },
  })

  React.useEffect(() => {
    if (!optionsQuery.data) return
    setSelection(emptySelection(optionsQuery.data))
    setPreview(null)
    setStarted(false)
  }, [optionsQuery.data])

  const context =
    optionsQuery.data && selection
      ? resolveSelection(optionsQuery.data, selection)
      : null
  const ready = context !== null

  return (
    <section className="rounded-2xl border border-border bg-card/55 p-5 sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-[0.2em] text-primary">
            Historical reality check
          </p>
          <h2 className="mt-2 text-xl font-semibold">Test an investment approach</h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
            Measure performance using only information available at each simulated
            decision, including trading costs and out-of-sample checks.
          </p>
        </div>
        <FlaskConical className="size-5 text-primary" aria-hidden="true" />
      </div>

      {!available ? (
        <Unavailable
          title="Backtest preparation is unavailable"
          detail="This workspace cannot prepare backtests yet."
        />
      ) : optionsQuery.isPending ? (
        <Loading label="Loading backtest choices…" />
      ) : optionsQuery.isError ? (
        <Unavailable
          title="Backtest choices are unavailable"
          detail="Refresh the page and try again."
        />
      ) : optionsQuery.data.histories.length === 0 ? (
        <Unavailable
          title="No point-in-time history is ready"
          detail="Backtesting needs enough historical information with known availability times."
        />
      ) : selection ? (
        <div className="mt-5 space-y-4">
          <BuilderFields
            options={optionsQuery.data}
            selection={selection}
            disabled={previewMutation.isPending || startMutation.isPending}
            onChange={(next) => {
              setSelection(next)
              setPreview(null)
              setStarted(false)
              previewMutation.reset()
              startMutation.reset()
            }}
          />
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-border bg-background/25 p-3">
            <p className="max-w-3xl text-xs leading-5 text-muted-foreground">
              {optionsQuery.data.guidance}
            </p>
            <Button
              disabled={!ready || previewMutation.isPending}
              onClick={() => {
                if (ready) previewMutation.mutate(selection)
              }}
            >
              <ShieldCheck aria-hidden="true" />
              {previewMutation.isPending ? "Preparing…" : "Review backtest"}
            </Button>
          </div>
          {previewMutation.isError ? (
            <Status text="This backtest could not be prepared. Review the choices and try again." />
          ) : null}
          {started ? (
            <Status text="Backtest started. Progress appears below." success />
          ) : null}
        </div>
      ) : null}

      <Dialog
        open={preview !== null}
        onOpenChange={(open) => {
          if (!open && !startMutation.isPending) setPreview(null)
        }}
      >
        <DialogContent className="max-h-[88vh] overflow-y-auto sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>Start this backtest?</DialogTitle>
            <DialogDescription>
              Review the point-in-time evidence, out-of-sample plan, trading
              costs, assumptions, and limitations first.
            </DialogDescription>
          </DialogHeader>
          {preview ? <BacktestPreview preview={preview} /> : null}
          {startMutation.isError ? (
            <Status text="The backtest could not be started. Review it and try again." />
          ) : null}
          <DialogFooter>
            <Button
              variant="outline"
              disabled={startMutation.isPending}
              onClick={() => setPreview(null)}
            >
              Go back
            </Button>
            <Button
              disabled={!preview || startMutation.isPending}
              onClick={() => {
                if (preview) startMutation.mutate(preview.confirmationToken)
              }}
            >
              <Play aria-hidden="true" />
              {startMutation.isPending
                ? "Starting…"
                : "Confirm and start backtest"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function BuilderFields({
  options,
  selection,
  disabled,
  onChange,
}: {
  options: BacktestPreparationOptions
  selection: BacktestPreparationSelection
  disabled: boolean
  onChange: (selection: BacktestPreparationSelection) => void
}) {
  const history = options.histories.find(
    (candidate) => candidate.historyToken === selection.historyToken,
  )
  const period = history?.periods.find(
    (candidate) => candidate.periodToken === selection.periodToken,
  )
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      <Choice
        id="backtest-history"
        label="Investment history"
        value={selection.historyToken}
        disabled={disabled}
        options={options.histories.map((history) => ({
          token: history.historyToken,
          label: `${history.label} · ${history.investmentCount.toLocaleString()} investments`,
        }))}
        onChange={(historyToken) => {
          onChange({ ...selection, historyToken, periodToken: "" })
        }}
      />
      <Choice
        id="backtest-period"
        label="Evaluation period"
        value={selection.periodToken}
        disabled={disabled || !history}
        options={(history?.periods ?? []).map((candidate) => ({
          token: candidate.periodToken,
          label: candidate.label,
        }))}
        onChange={(periodToken) => onChange({ ...selection, periodToken })}
      />
      <Choice
        id="backtest-method"
        label="Investment approach"
        value={selection.methodToken}
        disabled={disabled}
        options={choiceOptions(options.methods)}
        onChange={(methodToken) => onChange({ ...selection, methodToken })}
      />
      <Choice
        id="backtest-costs"
        label="Trading costs"
        value={selection.costToken}
        disabled={disabled}
        options={choiceOptions(options.costPlans)}
        onChange={(costToken) => onChange({ ...selection, costToken })}
      />
      <Choice
        id="backtest-portfolio"
        label="Portfolio rules"
        value={selection.portfolioToken}
        disabled={disabled}
        options={choiceOptions(options.portfolios)}
        onChange={(portfolioToken) =>
          onChange({ ...selection, portfolioToken })
        }
      />
      <Choice
        id="backtest-comparison"
        label="Comparison"
        value={selection.comparisonToken}
        disabled={disabled}
        options={choiceOptions(options.comparisons)}
        onChange={(comparisonToken) =>
          onChange({ ...selection, comparisonToken })
        }
      />
      {period ? (
        <p className="rounded-lg border border-border bg-background/25 p-3 text-xs leading-5 text-muted-foreground md:col-span-2 xl:col-span-3">
          {formatDate(period.startsAt)} through {formatDate(period.endsAt)}.
          Results must disclose incomplete coverage rather than treating missing
          history as success.
        </p>
      ) : null}
    </div>
  )
}

function BacktestPreview({ preview }: { preview: BacktestPreparationPreview }) {
  const facts = [
    ["Investment history", preview.investmentUniverse],
    ["Evaluation period", preview.period],
    ["Investment approach", preview.method],
    ["Portfolio rules", preview.portfolio],
    ["Comparison", preview.comparison],
    ["Point-in-time evidence", evidenceStateLabel(preview.pointInTimeEvidence)],
    ["Out-of-sample plan", preview.outOfSamplePlan],
  ] as const
  return (
    <div className="space-y-4 py-2">
      <dl className="grid gap-2 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
      <EvidenceList title="Evidence checks" values={preview.evidence} />
      <EvidenceList title="Assumptions" values={preview.assumptions} />
      <EvidenceList title="Known limitations" values={preview.limitations} />
      <CostGrid costs={preview.costs} />
      <p className="flex gap-2 text-xs leading-5 text-muted-foreground">
        <CalendarClock className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
        This review expires {formatDateTime(preview.expiresAt)}. If it expires,
        prepare the backtest again before starting.
      </p>
    </div>
  )
}

function ActivityButton({
  activity,
  selected,
  onSelect,
}: {
  activity: BacktestActivity
  selected: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={onSelect}
      className={cn(
        "rounded-xl border p-3 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        selected
          ? "border-primary/45 bg-primary/10"
          : "border-border bg-card/35 hover:bg-accent/40",
      )}
    >
      <span className="flex items-center justify-between gap-3">
        <span className="truncate text-sm font-medium">{activity.label}</span>
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {activityStateLabel(activity.state)}
        </span>
      </span>
      <span className="mt-1 block text-xs leading-5 text-muted-foreground">
        {activityStateDescription(activity.state)}
      </span>
      <span className="mt-1 block text-[11px] text-muted-foreground">
        Updated {formatDateTime(activity.updatedAt)}
        {activity.progressPercent ? ` · ${activity.progressPercent}% complete` : ""}
      </span>
    </button>
  )
}

function BacktestEvidence({
  activity,
  result,
  loading,
  error,
}: {
  activity: BacktestActivity | null
  result: BacktestResult | null
  loading: boolean
  error: boolean
}) {
  return (
    <section className="rounded-2xl border border-border bg-card/55 p-5 sm:p-6">
      <p className="text-xs font-medium uppercase tracking-[0.2em] text-primary">
        Evidence review
      </p>
      <h2 className="mt-2 text-xl font-semibold">Backtest result</h2>
      {!activity ? (
        <Unavailable
          title="Select a completed backtest"
          detail="Choose a backtest to review its returns, risks, costs, and out-of-sample evidence."
        />
      ) : activity.state !== "completed" ? (
        <Unavailable
          title={activity.label}
          detail={activityStateDescription(activity.state)}
        />
      ) : loading ? (
        <Loading label="Loading the backtest result…" />
      ) : error ? (
        <Unavailable
          title="The result is unavailable"
          detail="Refresh the page and try again."
        />
      ) : !result ? (
        <Unavailable
          title="The result is not ready"
          detail="The completed backtest did not return a reviewable result."
        />
      ) : result.state === "unavailable" ? (
        <div>
          <Unavailable title={result.label} detail={result.reason} />
          <div className="mt-4">
            <EvidenceList title="Limitations" values={result.limitations} />
          </div>
        </div>
      ) : (
        <CompletedBacktestView result={result} />
      )}
    </section>
  )
}

function CompletedBacktestView({ result }: { result: CompletedBacktest }) {
  const performance = [
    ["Total return", percent(result.performance.totalReturnPercent)],
    ["Annualized return", maybePercent(result.performance.annualizedReturnPercent)],
    ["Maximum drawdown", percent(result.performance.maximumDrawdownPercent)],
    ["Annualized volatility", maybePercent(result.performance.annualizedVolatilityPercent)],
    ["Sharpe ratio", result.performance.sharpeRatio ?? "Unavailable"],
    ["Win rate", maybePercent(result.performance.winRatePercent)],
    ["Turnover", maybePercent(result.performance.turnoverPercent)],
    ["Trading costs", percent(result.costs.totalCostPercent)],
  ] as const
  return (
    <div className="mt-5 space-y-5">
      <div className="rounded-xl border border-primary/20 bg-primary/[0.04] p-4">
        <p className="text-sm font-semibold">{result.label}</p>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          {result.investmentUniverse} · {result.method} · {formatDate(result.period.startsAt)}
          {" through "}
          {formatDate(result.period.endsAt)}
        </p>
        <p className="mt-3 text-sm leading-6">{result.interpretation}</p>
      </div>

      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        {performance.map(([label, value]) => (
          <Metric key={label} label={label} value={value} />
        ))}
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <EvidenceCard
          icon={ShieldCheck}
          title="Point-in-time evidence"
          state={result.pointInTimeEvidence.state}
          interpretation={result.pointInTimeEvidence.interpretation}
          facts={[
            ["Information cutoff", formatDateTime(result.pointInTimeEvidence.informationCutoff)],
            [
              "History covered",
              `${formatDate(result.pointInTimeEvidence.observedFrom)} through ${formatDate(result.pointInTimeEvidence.observedThrough)}`,
            ],
            ["Observations", result.pointInTimeEvidence.observationCount.toLocaleString()],
            ["Coverage", maybePercent(result.pointInTimeEvidence.coveragePercent)],
          ]}
        />
        <EvidenceCard
          icon={GitCompareArrows}
          title="Out-of-sample evidence"
          state={result.outOfSampleEvidence.state}
          interpretation={result.outOfSampleEvidence.interpretation}
          facts={[
            ["Evaluation method", result.outOfSampleEvidence.method],
            [
              "Independent test windows",
              result.outOfSampleEvidence.foldCount.toLocaleString(),
            ],
            ["Observations", result.outOfSampleEvidence.observationCount.toLocaleString()],
            [
              "Overfitting probability",
              maybePercent(result.outOfSampleEvidence.probabilityOfOverfittingPercent),
            ],
            [
              "Deflated performance probability",
              maybePercent(
                result.outOfSampleEvidence.deflatedPerformanceProbabilityPercent,
              ),
            ],
          ]}
        />
      </div>

      <CostGrid costs={result.costs} />
      <div className="grid gap-2 sm:grid-cols-3">
        <Metric label="Fills" value={result.execution.fillCount.toLocaleString()} />
        <Metric
          label="Partial fills"
          value={result.execution.partialFillCount.toLocaleString()}
        />
        <Metric
          label="No-action decisions"
          value={result.execution.noActionCount.toLocaleString()}
        />
      </div>

      {result.comparison ? (
        <div className="rounded-xl border border-border bg-background/25 p-4">
          <p className="flex items-center gap-2 text-sm font-semibold">
            <BarChart3 className="size-4 text-primary" aria-hidden="true" />
            Comparison with {result.comparison.label}
          </p>
          <div className="mt-3 grid gap-2 sm:grid-cols-2">
            <Metric
              label="Comparison return"
              value={percent(result.comparison.totalReturnPercent)}
            />
            <Metric
              label="Difference"
              value={percent(result.comparison.excessReturnPercent)}
            />
          </div>
        </div>
      ) : null}

      <EvidenceList title="Limitations" values={result.limitations} />
      <EvidenceList title="What would invalidate this result" values={result.invalidators} />
      {result.expiresAt ? (
        <p className="flex gap-2 text-xs leading-5 text-muted-foreground">
          <CalendarClock className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
          This result expires {formatDateTime(result.expiresAt)}. Re-evaluate it
          after that time before using it in a current investment decision.
        </p>
      ) : null}
      <p className="text-xs leading-5 text-muted-foreground">
        Historical performance does not guarantee future profit. This result is
        investment research, not permission to trade. Its uncertainty is{" "}
        {uncertaintyLabel(result.uncertainty)}; a score alone is never treated as
        confidence.
      </p>
    </div>
  )
}

function EvidenceCard({
  icon: Icon,
  title,
  state,
  interpretation,
  facts,
}: {
  icon: typeof ShieldCheck
  title: string
  state: string
  interpretation: string
  facts: readonly (readonly [string, string])[]
}) {
  return (
    <div className="rounded-xl border border-border bg-background/25 p-4">
      <p className="flex items-center gap-2 text-sm font-semibold">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        {title}
      </p>
      <p className="mt-1 text-xs text-muted-foreground">
        {evidenceStateLabel(state)}
      </p>
      <dl className="mt-3 grid gap-2 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
      <p className="mt-3 text-xs leading-5 text-muted-foreground">
        {interpretation}
      </p>
    </div>
  )
}

function CostGrid({ costs }: { costs: BacktestPreparationPreview["costs"] }) {
  const rows = [
    ["Fees", costs.fees],
    ["Bid/ask spread", costs.spread],
    ["Slippage", costs.slippage],
    ["Latency", costs.latency],
    ["Participation limit", costs.participationLimit],
    ["Partial fills", costs.partialFills],
  ] as const
  return (
    <div className="rounded-xl border border-border bg-background/25 p-4">
      <p className="flex items-center gap-2 text-sm font-semibold">
        <CircleDollarSign className="size-4 text-primary" aria-hidden="true" />
        Trading-cost assumptions
      </p>
      <dl className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
        {rows.map(([label, value]) => (
          <Fact key={label} label={label} value={value} />
        ))}
      </dl>
    </div>
  )
}

function EvidenceList({ title, values }: { title: string; values: string[] }) {
  if (values.length === 0) return null
  return (
    <div className="rounded-xl border border-border bg-background/25 p-4">
      <p className="text-sm font-semibold">{title}</p>
      <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-muted-foreground">
        {values.map((value) => (
          <li key={value}>{value}</li>
        ))}
      </ul>
    </div>
  )
}

function emptySelection(
  options: BacktestPreparationOptions,
): BacktestPreparationSelection | null {
  if (options.histories.length === 0) return null
  return {
    historyToken: "",
    periodToken: "",
    methodToken: "",
    costToken: "",
    portfolioToken: "",
    comparisonToken: "",
  }
}

interface BuilderContext {
  history: BacktestPreparationOptions["histories"][number]
  period: BacktestPreparationOptions["histories"][number]["periods"][number]
}

function resolveSelection(
  options: BacktestPreparationOptions,
  selection: BacktestPreparationSelection,
): BuilderContext | null {
  const history = options.histories.find(
    (candidate) => candidate.historyToken === selection.historyToken,
  )
  const period = history?.periods.find(
    (candidate) => candidate.periodToken === selection.periodToken,
  )
  const method = options.methods.some(
    (candidate) => candidate.token === selection.methodToken,
  )
  const costs = options.costPlans.some(
    (candidate) => candidate.token === selection.costToken,
  )
  const portfolio = options.portfolios.some(
    (candidate) => candidate.token === selection.portfolioToken,
  )
  const comparison = options.comparisons.some(
    (candidate) => candidate.token === selection.comparisonToken,
  )
  return history && period && method && costs && portfolio && comparison
    ? { history, period }
    : null
}

function choiceOptions(
  values: BacktestPreparationOptions["methods"],
): { token: string; label: string; description: string }[] {
  return values.map((value) => ({
    token: value.token,
    label: value.label,
    description: value.description,
  }))
}

function Choice({
  id,
  label,
  value,
  options,
  disabled,
  onChange,
}: {
  id: string
  label: string
  value: string
  options: { token: string; label: string; description?: string }[]
  disabled: boolean
  onChange: (value: string) => void
}) {
  const selected = options.find((option) => option.token === value)
  return (
    <label className="grid gap-1.5 text-xs" htmlFor={id}>
      <span>{label}</span>
      <select
        id={id}
        className={CONTROL_CLASS}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
      >
        <option value="">Choose {label.toLowerCase()}</option>
        {options.map((option) => (
          <option key={option.token} value={option.token}>
            {option.label}
          </option>
        ))}
      </select>
      {selected?.description ? (
        <span className="text-[11px] leading-5 text-muted-foreground">
          {selected.description}
        </span>
      ) : null}
    </label>
  )
}

function PageFrame({ children }: { children: React.ReactNode }) {
  return (
    <main className="mx-auto w-full max-w-[1500px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-6">
        <p className="text-xs font-medium uppercase tracking-[0.2em] text-primary">
          Investment research
        </p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Backtests</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Test whether an investment approach held up after realistic costs,
          drawdowns, incomplete coverage, and out-of-sample checks.
        </p>
      </div>
      {children}
    </main>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-background/25 p-3">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-sm">{value}</p>
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 text-xs leading-5">{value}</dd>
    </div>
  )
}

function Loading({ label }: { label: string }) {
  return (
    <p className="mt-4 rounded-xl border border-border bg-background/25 p-4 text-sm text-muted-foreground">
      {label}
    </p>
  )
}

function Unavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <Alert className="mt-4">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>{detail}</AlertDescription>
    </Alert>
  )
}

function Status({ text, success = false }: { text: string; success?: boolean }) {
  return (
    <p
      className={cn(
        "rounded-lg border border-border bg-background/25 p-3 text-xs leading-5",
        success ? "text-emerald-300" : "text-red-300",
      )}
    >
      {text}
    </p>
  )
}

function percent(value: string): string {
  return `${value}%`
}

function maybePercent(value: string | null): string {
  return value === null ? "Unavailable" : percent(value)
}

function activityStateLabel(state: BacktestActivity["state"]): string {
  switch (state) {
    case "queued":
      return "Waiting to start"
    case "running":
      return "In progress"
    case "completed":
      return "Completed"
    case "failed":
      return "Could not complete"
  }
}

function activityStateDescription(state: BacktestActivity["state"]): string {
  switch (state) {
    case "queued":
      return "This backtest is waiting to start."
    case "running":
      return "Market Squawk is evaluating the historical period."
    case "completed":
      return "This backtest is ready to review."
    case "failed":
      return "This backtest could not be completed."
  }
}

function uncertaintyLabel(
  uncertainty: CompletedBacktest["uncertainty"],
): string {
  switch (uncertainty) {
    case "supported":
      return "supported by the disclosed evidence"
    case "limited":
      return "limited"
    case "unavailable":
      return "unavailable"
  }
}

function evidenceStateLabel(state: string): string {
  switch (state) {
    case "verified":
      return "Verified"
    case "evaluated":
      return "Evaluated"
    case "limited":
      return "Limited"
    case "unavailable":
      return "Unavailable"
    case "not_evaluated":
      return "Not evaluated"
    default:
      return "Unavailable"
  }
}

function formatDate(value: string): string {
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(
    new Date(value),
  )
}

function formatDateTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value))
}
