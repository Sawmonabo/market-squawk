import * as React from "react"
import { useInfiniteQuery, useMutation } from "@tanstack/react-query"
import {
  AlertCircle,
  ListFilter,
  Plus,
  RefreshCw,
} from "lucide-react"

import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { dataQualities, qualityLabel, type DataQuality } from "@/lib/quality"
import type { ProductTransport } from "@/lib/transport"

import {
  digestEvidence,
  digestHex,
  parseDecisionJobReceipt,
  parseFeatureDatasetPage,
  parseSavedScreenOutcome,
  type FeatureDatasetView,
  type SavedScreenView,
} from "./contracts"
import { StateLabel } from "./decision-boundaries"
import { DatasetEvidence, Field, PredicateRow, Receipt } from "./screen-builder-fields"
import {
  bindingFor,
  bindingKey,
  contractKey,
  datasetKeyFor,
  DEFAULT_QUALITIES,
  emptyPredicate,
  featureDatasetLabel,
  featureLabel,
  isDataQuality,
  screenKey,
  SELECT_CLASS,
  validateDraft,
  type PredicateDraft,
  type RankingDirection,
  type SavedScreenReceipt,
} from "./screen-builder-model"

export function ScreenBuilder({
  transport,
  scope,
  screens,
  onSaved,
}: {
  transport: ProductTransport
  scope: ProductScope
  screens: SavedScreenView[]
  onSaved: () => Promise<void>
}) {
  const evidence = useInfiniteQuery({
    queryKey: productKeys.operation(scope, "analysis", "feature-datasets", {}),
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      parseFeatureDatasetPage(
        await transport.query({
          query: "analysisFeatureDatasets",
          ...(pageParam ? { afterDataset: pageParam } : {}),
        }),
        pageParam,
      ),
    getNextPageParam: (page) =>
      page.hasMore ? (page.nextAfterDataset ?? undefined) : undefined,
  })
  const contracts = React.useMemo(
    () =>
      evidence.data?.pages
        .flatMap((page) => page.contracts)
        .filter(
          (contract) =>
            contract.pointInTimeCompatible && contract.outputType === "statistical_f64",
        ) ?? [],
    [evidence.data],
  )
  const datasets = React.useMemo(
    () => evidence.data?.pages.flatMap((page) => page.datasets) ?? [],
    [evidence.data],
  )
  const [selectedRevision, setSelectedRevision] = React.useState("new")
  const [screenId, setScreenId] = React.useState("")
  const [datasetKey, setDatasetKey] = React.useState("")
  const [predicates, setPredicates] = React.useState<PredicateDraft[]>([
    emptyPredicate(),
  ])
  const [rankingFeatureKey, setRankingFeatureKey] = React.useState("")
  const [rankingDirection, setRankingDirection] =
    React.useState<RankingDirection>("descending")
  const [maximumResults, setMaximumResults] = React.useState("50")
  const [minimumCoverage, setMinimumCoverage] = React.useState("80")
  const [minimumLiquidity, setMinimumLiquidity] = React.useState("0")
  const [qualities, setQualities] = React.useState<DataQuality[]>(DEFAULT_QUALITIES)
  const [receipt, setReceipt] = React.useState<SavedScreenReceipt | null>(null)
  const [asOf, setAsOf] = React.useState(defaultCutoff())

  const selectedScreen = screens.find(
    (screen) => screenKey(screen) === selectedRevision,
  )
  const selectedDataset = datasets.find(
    (dataset) => datasetKeyFor(dataset) === datasetKey,
  )
  const contractByKey = new Map(
    contracts.map((contract) => [contractKey(contract), contract] as const),
  )
  const revision = selectedScreen ? selectedScreen.revision + 1 : 1

  function resetDraft() {
    setScreenId("")
    setDatasetKey(datasets[0] ? datasetKeyFor(datasets[0]) : "")
    const featureKey = contracts[0] ? contractKey(contracts[0]) : ""
    setPredicates([{ ...emptyPredicate(), featureKey }])
    setRankingFeatureKey(featureKey)
    setRankingDirection("descending")
    setMaximumResults("50")
    setMinimumCoverage("80")
    setMinimumLiquidity("0")
    setQualities(DEFAULT_QUALITIES)
    setReceipt(null)
  }

  function selectRevision(nextRevision: string) {
    setSelectedRevision(nextRevision)
    if (nextRevision === "new") {
      resetDraft()
      return
    }
    const screen = screens.find((candidate) => screenKey(candidate) === nextRevision)
    if (!screen) return
    const matchingDataset = datasets.find(
      (dataset) => dataset.universeDigest === digestHex(screen.universeIdentity),
    )
    setScreenId(screen.id)
    setDatasetKey(matchingDataset ? datasetKeyFor(matchingDataset) : "")
    setPredicates(
      screen.predicates.map((predicate) => ({
        featureKey: bindingKey(predicate.binding),
        operator: predicate.operator,
        threshold: String(predicate.threshold),
        nullPolicy: predicate.nullPolicy,
      })),
    )
    setRankingFeatureKey(bindingKey(screen.ranking.binding))
    setRankingDirection(screen.ranking.direction)
    setMaximumResults(String(screen.maximumResults))
    setMinimumCoverage(String(screen.constraints.minimumCoverage * 100))
    setMinimumLiquidity(String(screen.constraints.minimumLiquidity))
    setQualities(screen.constraints.admittedDataQualities.filter(isDataQuality))
    setReceipt(null)
  }

  React.useEffect(() => {
    const defaultFeature = contracts[0]
    const defaultDataset = datasets[0]
    if (selectedRevision !== "new") return
    if (defaultFeature) {
      const key = contractKey(defaultFeature)
      setPredicates((current) =>
        current.length === 1 && current[0]?.featureKey === ""
          ? [{ ...current[0], featureKey: key }]
          : current,
      )
      setRankingFeatureKey((current) => current || key)
    }
    if (defaultDataset) {
      setDatasetKey((current) => current || datasetKeyFor(defaultDataset))
    }
  }, [contracts, datasets, selectedRevision])

  React.useEffect(() => {
    if (!selectedScreen || datasetKey !== "") return
    const matchingDataset = datasets.find(
      (dataset) => dataset.universeDigest === digestHex(selectedScreen.universeIdentity),
    )
    if (matchingDataset) setDatasetKey(datasetKeyFor(matchingDataset))
  }, [datasetKey, datasets, selectedScreen])

  const save = useMutation({
    mutationFn: async (input: {
      expectedRevision?: number
      screen: Record<string, unknown>
      dataset: FeatureDatasetView
    }) => ({
      outcome: parseSavedScreenOutcome(
        await transport.decisionControl(
          {
            action: "saveScreen",
            ...(input.expectedRevision === undefined
              ? {}
              : { expectedRevision: input.expectedRevision }),
            screen: input.screen,
          },
          true,
        ),
      ),
      dataset: input.dataset,
    }),
    onSuccess: async ({ outcome, dataset }, input) => {
      setReceipt({
        outcome,
        screenId: String(input.screen.id),
        revision: Number(input.screen.revision),
        dataset,
      })
      await onSaved()
      setSelectedRevision(`${String(input.screen.id)}:${Number(input.screen.revision)}`)
    },
  })

  const run = useMutation({
    mutationFn: async (input: {
      screen: SavedScreenView
      dataset: FeatureDatasetView
      asOf: string
    }) =>
      parseDecisionJobReceipt(
        await transport.decisionControl(
          {
            action: "runScreen",
            screenId: input.screen.id,
            screenRevision: input.screen.revision,
            datasetManifest: input.dataset.manifest,
            asOf: input.asOf,
          },
          true,
        ),
      ),
  })

  const validation = validateDraft({
    screenId,
    selectedDataset,
    predicates,
    contractByKey,
    rankingFeatureKey,
    maximumResults,
    minimumCoverage,
    minimumLiquidity,
    qualities,
  })

  function submit(event: React.FormEvent) {
    event.preventDefault()
    if (!validation.valid || !selectedDataset) return
    const ranking = contractByKey.get(rankingFeatureKey)
    if (!ranking) return
    const normalizedPredicates = predicates.map((predicate) => {
      const contract = contractByKey.get(predicate.featureKey)
      if (!contract) throw new Error("A selected feature is unavailable.")
      return {
        binding: bindingFor(contract),
        operator: predicate.operator,
        threshold: Number(predicate.threshold),
        nullPolicy: predicate.nullPolicy,
      }
    })
    save.mutate({
      ...(selectedScreen ? { expectedRevision: selectedScreen.revision } : {}),
      dataset: selectedDataset,
      screen: {
        id: screenId,
        revision,
        universeIdentity: digestEvidence(selectedDataset.universeDigest),
        predicates: normalizedPredicates,
        ranking: {
          binding: bindingFor(ranking),
          direction: rankingDirection,
        },
        maximumResults: Number(maximumResults),
        constraints: {
          minimumCoverage: Number(minimumCoverage) / 100,
          minimumLiquidity: Number(minimumLiquidity),
          admittedDataQualities: qualities,
        },
      },
    })
  }

  return (
    <section aria-labelledby="screen-builder-heading" className="mt-6">
      <div className="rounded-xl border border-primary/25 bg-card/55">
        <header className="flex flex-wrap items-start justify-between gap-4 border-b border-border p-5">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
              Guided point-in-time screen
            </p>
            <h2 id="screen-builder-heading" className="mt-1 text-lg font-semibold">
              Find investments using evidence you choose
            </h2>
            <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
              Choose prepared research data, add clear rules, and save the screen. This defines
              research criteria only; it cannot place an order.
            </p>
          </div>
          <StateLabel value={selectedScreen ? `editing revision ${selectedScreen.revision}` : "new screen"} />
        </header>

        {evidence.isPending ? (
          <div className="grid gap-3 p-5" aria-label="Loading research data for the screen">
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-40 w-full" />
          </div>
        ) : evidence.isError ? (
          <Alert variant="destructive" className="m-5">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>Screen data could not be loaded</AlertTitle>
            <AlertDescription>
              Prepared research data could not be retrieved. Retry, and check Logs if the problem
              continues.
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="mt-2"
                onClick={() => void evidence.refetch()}
              >
                Retry
              </Button>
            </AlertDescription>
          </Alert>
        ) : contracts.length === 0 || datasets.length === 0 ? (
          <Alert className="m-5">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>Prepare research data first</AlertTitle>
            <AlertDescription>
              A saved screen needs a historical statistical feature and a prepared dataset. Build
              the dataset in Research, then return here.
            </AlertDescription>
          </Alert>
        ) : (
          <form className="grid gap-6 p-5" onSubmit={submit}>
            <div className="grid gap-4 lg:grid-cols-2">
              <Field label="Start a new screen or revise one" htmlFor="screen-revision-source">
                <select
                  id="screen-revision-source"
                  className={SELECT_CLASS}
                  value={selectedRevision}
                  onChange={(event) => selectRevision(event.target.value)}
                >
                  <option value="new">Create a new screen</option>
                  {screens.map((screen) => (
                    <option key={screenKey(screen)} value={screenKey(screen)}>
                      {screen.id} · revision {screen.revision}
                    </option>
                  ))}
                </select>
              </Field>
              <Field
                label="Screen ID"
                htmlFor="screen-id"
                help="Use lowercase letters, numbers, dots, dashes, or underscores. This stable name keeps revision history together."
              >
                <Input
                  id="screen-id"
                  value={screenId}
                  onChange={(event) => {
                    setScreenId(event.target.value)
                    setReceipt(null)
                  }}
                  maxLength={128}
                  autoComplete="off"
                  placeholder="quality-value-screen"
                  disabled={selectedScreen !== undefined}
                  className="mt-2"
                />
              </Field>
            </div>

            <div className="grid gap-4 rounded-xl border border-border bg-background/45 p-4 lg:grid-cols-[minmax(0,1fr)_minmax(280px,0.72fr)]">
              <Field
                label="Prepared research dataset"
                htmlFor="screen-dataset"
                help="The dataset supplies the historical investment universe used by this screen."
              >
                <select
                  id="screen-dataset"
                  className={SELECT_CLASS}
                  value={datasetKey}
                  onChange={(event) => {
                    setDatasetKey(event.target.value)
                    setReceipt(null)
                  }}
                >
                  <option value="">Select a point-in-time dataset</option>
                  {datasets.map((dataset) => (
                    <option key={datasetKeyFor(dataset)} value={datasetKeyFor(dataset)}>
                      {featureDatasetLabel(dataset)}
                    </option>
                  ))}
                </select>
                {evidence.hasNextPage && (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="mt-3"
                    disabled={evidence.isFetchingNextPage}
                    onClick={() => void evidence.fetchNextPage()}
                  >
                    {evidence.isFetchingNextPage ? (
                      <RefreshCw className="animate-spin" aria-hidden="true" />
                    ) : null}
                    Load more datasets
                  </Button>
                )}
              </Field>
              {selectedDataset ? (
                <DatasetEvidence dataset={selectedDataset} />
              ) : (
                <p className="self-center text-xs leading-5 text-muted-foreground">
                  Select a dataset to review its coverage and prepared sample counts.
                </p>
              )}
            </div>

            <fieldset className="grid gap-3">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                  <legend className="text-sm font-semibold">Screen rules</legend>
                  <p className="mt-1 text-xs leading-5 text-muted-foreground">
                    Each rule compares a feature available at the research cutoff with a threshold.
                  </p>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setPredicates((current) => [...current, emptyPredicate()])}
                  disabled={predicates.length >= 32}
                >
                  <Plus aria-hidden="true" />
                  Add rule
                </Button>
              </div>
              {predicates.map((predicate, index) => (
                <PredicateRow
                  key={index}
                  index={index}
                  predicate={predicate}
                  contracts={contracts}
                  removable={predicates.length > 1}
                  onChange={(next) =>
                    setPredicates((current) =>
                      current.map((entry, entryIndex) =>
                        entryIndex === index ? next : entry,
                      ),
                    )
                  }
                  onRemove={() =>
                    setPredicates((current) =>
                      current.filter((_entry, entryIndex) => entryIndex !== index),
                    )
                  }
                />
              ))}
            </fieldset>

            <div className="grid gap-4 lg:grid-cols-3">
              <Field
                label="Rank results by"
                htmlFor="screen-ranking-feature"
                help="Choose whether the lowest or highest values appear first."
              >
                <select
                  id="screen-ranking-feature"
                  className={SELECT_CLASS}
                  value={rankingFeatureKey}
                  onChange={(event) => setRankingFeatureKey(event.target.value)}
                >
                  <option value="">Select a ranking feature</option>
                  {contracts.map((contract) => (
                    <option key={contractKey(contract)} value={contractKey(contract)}>
                      {featureLabel(contract)}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label="Ranking order" htmlFor="screen-ranking-direction">
                <select
                  id="screen-ranking-direction"
                  className={SELECT_CLASS}
                  value={rankingDirection}
                  onChange={(event) =>
                    setRankingDirection(event.target.value as RankingDirection)
                  }
                >
                  <option value="descending">Highest values first</option>
                  <option value="ascending">Lowest values first</option>
                </select>
              </Field>
              <Field
                label="Maximum results"
                htmlFor="screen-maximum-results"
                help="Choose between 1 and 1,024 candidates."
              >
                <Input
                  id="screen-maximum-results"
                  className="mt-2"
                  type="number"
                  min={1}
                  max={1024}
                  step={1}
                  value={maximumResults}
                  onChange={(event) => setMaximumResults(event.target.value)}
                />
              </Field>
            </div>

            <div className="grid gap-4 rounded-xl border border-border bg-background/45 p-4 lg:grid-cols-2">
              <Field
                label="Required data coverage"
                htmlFor="screen-minimum-coverage"
                help="The percentage of required inputs that must be present."
              >
                <div className="mt-2 flex items-center gap-2">
                  <Input
                    id="screen-minimum-coverage"
                    type="number"
                    min={0}
                    max={100}
                    step="0.1"
                    value={minimumCoverage}
                    onChange={(event) => setMinimumCoverage(event.target.value)}
                  />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </Field>
              <Field
                label="Minimum liquidity"
                htmlFor="screen-minimum-liquidity"
                help="Measured in the selected dataset's declared liquidity units. Use 0 when no positive floor is required."
              >
                <Input
                  id="screen-minimum-liquidity"
                  className="mt-2"
                  type="number"
                  min={0}
                  step="any"
                  value={minimumLiquidity}
                  onChange={(event) => setMinimumLiquidity(event.target.value)}
                />
              </Field>
              <fieldset className="lg:col-span-2">
                <legend className="text-sm font-medium">Allowed data quality</legend>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  These choices affect research inclusion only. Estimated or older information
                  remains clearly labeled and is never treated as current observed data.
                </p>
                <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                  {dataQualities.map((quality) => (
                    <label
                      key={quality}
                      className="flex items-start gap-2 rounded-lg border border-border bg-card/45 p-3 text-xs"
                    >
                      <input
                        type="checkbox"
                        className="mt-0.5 size-4 accent-primary"
                        checked={qualities.includes(quality)}
                        onChange={(event) =>
                          setQualities((current) =>
                            event.target.checked
                              ? [...current, quality]
                              : current.filter((entry) => entry !== quality),
                          )
                        }
                      />
                      <span>
                        <span className="font-medium">{qualityLabel(quality)}</span>
                        {(quality === "stale" || quality === "quarantined") && (
                          <span className="mt-1 block text-muted-foreground">
                            Include only when you explicitly want to review lower-quality data.
                          </span>
                        )}
                      </span>
                    </label>
                  ))}
                </div>
              </fieldset>
            </div>

            {!validation.valid && (
              <Alert>
                <AlertCircle aria-hidden="true" />
                <AlertTitle>Complete the screen before saving</AlertTitle>
                <AlertDescription>{validation.reason}</AlertDescription>
              </Alert>
            )}
            {save.isError && (
              <Alert variant="destructive">
                <AlertCircle aria-hidden="true" />
                <AlertTitle>The screen was not saved</AlertTitle>
                <AlertDescription>
                  Market Squawk could not save this screen. Review the inputs and try again. Check
                  Logs if the problem continues.
                </AlertDescription>
              </Alert>
            )}
            {run.isError && (
              <Alert variant="destructive">
                <AlertCircle aria-hidden="true" />
                <AlertTitle>The screen run was not started</AlertTitle>
                <AlertDescription>
                  Market Squawk could not start this screen run. Try again, and check Logs if the
                  problem continues.
                </AlertDescription>
              </Alert>
            )}
            {receipt && <Receipt receipt={receipt} />}
            {run.data && (
              <Alert>
                <ListFilter aria-hidden="true" />
                <AlertTitle>Screen queued</AlertTitle>
                <AlertDescription>
                  The screen is running. Ranked candidates will appear in the candidate funnel when
                  it completes.
                </AlertDescription>
              </Alert>
            )}

            <div className="grid gap-4 border-t border-border pt-4 lg:grid-cols-[minmax(0,1fr)_minmax(280px,0.55fr)] lg:items-end">
              <p className="max-w-2xl text-xs leading-5 text-muted-foreground">
                Saving records this screen revision. Running it separately uses only information
                available by the selected research cutoff.
              </p>
              <Field
                label="Research cutoff"
                htmlFor="screen-research-cutoff"
                help="Only evidence available by this local date and time is eligible."
              >
                <Input
                  id="screen-research-cutoff"
                  className="mt-2"
                  type="datetime-local"
                  step={1}
                  value={asOf}
                  onChange={(event) => {
                    setAsOf(event.target.value)
                    run.reset()
                  }}
                />
              </Field>
              <div className="flex flex-wrap justify-end gap-2 lg:col-span-2">
                <Button type="submit" disabled={!validation.valid || save.isPending}>
                  {save.isPending ? (
                    <RefreshCw className="animate-spin" aria-hidden="true" />
                  ) : (
                    <ListFilter aria-hidden="true" />
                  )}
                  Save revision {revision}
                </Button>
                <Button
                  type="button"
                  variant="secondary"
                  disabled={
                    !selectedScreen ||
                    !selectedDataset ||
                    cutoffNanos(asOf) === null ||
                    run.isPending
                  }
                  onClick={() => {
                    const cutoff = cutoffNanos(asOf)
                    if (!selectedScreen || !selectedDataset || cutoff === null) return
                    run.mutate({
                      screen: selectedScreen,
                      dataset: selectedDataset,
                      asOf: cutoff,
                    })
                  }}
                >
                  {run.isPending ? (
                    <RefreshCw className="animate-spin" aria-hidden="true" />
                  ) : (
                    <ListFilter aria-hidden="true" />
                  )}
                  Run saved revision
                </Button>
              </div>
            </div>
          </form>
        )}
      </div>
    </section>
  )
}

function defaultCutoff(): string {
  const now = new Date()
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60_000)
  return local.toISOString().slice(0, 19)
}

function cutoffNanos(value: string): string | null {
  const milliseconds = Date.parse(value)
  if (!Number.isFinite(milliseconds)) return null
  return (BigInt(Math.trunc(milliseconds)) * 1_000_000n).toString()
}
