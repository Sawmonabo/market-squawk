import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  Activity,
  Boxes,
  ChartNoAxesCombined,
  RefreshCw,
  ShieldAlert,
} from "lucide-react"

import { useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { DemandPanel } from "../shared/demand-panel"
import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"

import { BundleEvidence } from "./bundle-evidence"
import { ForecastPreparation } from "./forecast-preparation"
import { ForecastReview } from "./forecast-review"
import { ModelJobActivity } from "./model-jobs"
import {
  isActiveModelActivity,
  parseForecasts,
  parseModelActivities,
  parseModelEvidence,
  parseModelSummaryPage,
} from "./models-contracts"

export function ModelsPage() {
  const product = useProduct()

  if (product.status === "loading") return <ModelsLoading />
  if (product.status === "error") {
    return (
      <ModelsFrame>
        <UnavailableEvidence
          title="Model workspace unavailable"
          detail="Models and forecasts cannot be shown right now. Try again when the workspace is available."
        />
      </ModelsFrame>
    )
  }

  return (
    <ModelsWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ModelsWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const [selectedModelToken, setSelectedModelToken] = React.useState<
    string | null
  >(null)
  const [selectedForecastToken, setSelectedForecastToken] = React.useState<
    string | null
  >(null)
  const capabilities = productCapabilitySet(bootstrap)
  const forecastsAvailable = capabilities.has("forecast_list")
  const modelsAvailable = capabilities.has("model_evidence")
  const modelActivityAvailable = capabilities.has("model_activity")

  const navigation = useCursorNavigation()
  const models = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "Model",
      "Model.ListBundles",
      { cursor: navigation.after, limit: 25 },
    ),
    queryFn: async ({ signal }) => {
      return parseModelSummaryPage(await transport.modelProducts({ action: "list", cursor: navigation.after, limit: 25 }, { signal }))
    },
    enabled: modelsAvailable,
    gcTime: 0,
  })
  const forecastNavigation = useCursorNavigation()
  const forecasts = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "Model",
      "Model.ListForecasts",
      { cursor: forecastNavigation.after, limit: 25 },
    ),
    queryFn: async ({ signal }) =>
      parseForecasts(await transport.query({ query: "forecasts", cursor: forecastNavigation.after, limit: 25 }, { signal })),
    enabled: forecastsAvailable,
    gcTime: 0,
  })
  const activityNavigation = useCursorNavigation()
  const activities = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "Model",
      "Model.ListProductActivity",
      { cursor: activityNavigation.after, limit: 25 },
    ),
    queryFn: async ({ signal }) => {
      return parseModelActivities(
        await transport.modelProducts({ action: "activity", cursor: activityNavigation.after, limit: 25 }, { signal }),
      )
    },
    enabled: modelActivityAvailable,
    gcTime: 0,
    refetchInterval: (query) => activityNavigation.after === undefined && query.state.data?.activities.some(isActiveModelActivity) ? 5_000 : false,
    refetchIntervalInBackground: false,
  })

  const selectedModelEvidence = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "Model", "Model.GetBundle", { modelToken: selectedModelToken }),
    enabled: modelsAvailable && selectedModelToken !== null,
    gcTime: 0,
    queryFn: async ({ signal }) => parseModelEvidence(await transport.modelProducts({ action: "get", modelToken: selectedModelToken! }, { signal }), selectedModelToken!),
  })
  const modelRows = models.data?.models ?? []
  const selectedModel =
    modelRows.find((model) => model.modelToken === selectedModelToken) ?? null
  const forecastRows = forecasts.data?.forecasts ?? []
  const selectedForecast =
    forecastRows.find(
      (forecast) => forecast.forecastToken === selectedForecastToken,
    ) ?? null
  const activityRows = activities.data?.activities ?? []
  const activeCount = activityRows.filter(isActiveModelActivity).length
  const calibratedForecasts = forecastRows.filter(
    (forecast) => forecast.modelEvidence.calibration === "calibrated",
  ).length
  const refreshing =
    models.isFetching || selectedModelEvidence.isFetching || forecasts.isFetching || activities.isFetching

  const refresh = () => {
    if (modelsAvailable) {
      void models.refetch()
      void activities.refetch()
      if (selectedModelToken !== null) void selectedModelEvidence.refetch()
    }
    if (forecastsAvailable) void forecasts.refetch()
  }

  return (
    <ModelsFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Investment research · no automatic trading
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">
            Models & forecasts
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review model purpose, out-of-sample evidence, forecasts,
            uncertainty, and limitations. Modeled values are estimates, not
            guaranteed outcomes.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={refresh}
          disabled={
            refreshing || (!modelsAvailable && !forecastsAvailable)
          }
        >
          <RefreshCw
            className={refreshing ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh evidence
        </Button>
      </header>

      <section
        aria-label="Model evidence summary"
        className="mt-5 grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-4"
      >
        <SummaryFact
          icon={Boxes}
          label="Models on this page"
          value={queryCount(
            modelsAvailable,
            models.isPending,
            models.isError,
            modelRows.length,
          )}
        />
        <SummaryFact
          icon={ChartNoAxesCombined}
          label="Forecasts on this page"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            forecastRows.length,
          )}
        />
        <SummaryFact
          icon={ShieldAlert}
          label="Calibrated on this page"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            calibratedForecasts,
          )}
        />
        <SummaryFact
          icon={Activity}
          label="Active on this activity page"
          value={queryCount(
            modelsAvailable,
            activities.isPending,
            activities.isError,
            activeCount,
          )}
        />
      </section>

      <div className="mt-5 grid gap-4 xl:grid-cols-[minmax(260px,0.72fr)_minmax(0,1.5fr)]">
        <section className="overflow-hidden rounded-xl border border-border bg-card/35">
          <div className="border-b border-border p-4">
            <h2 className="text-sm font-semibold">Available models</h2>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Select a model to review its purpose, validation, limitations,
              and out-of-sample evidence.
            </p>
          </div>
          {!modelsAvailable ? (
            <InlineUnavailable text="Model evidence is unavailable in this installation." />
          ) : models.isPending ? (
            <ListLoading />
          ) : models.isError ? (
            <InlineUnavailable text="Model evidence is unavailable right now." />
          ) : modelRows.length === 0 ? (
            <InlineUnavailable text="No model is ready for investment research yet." />
          ) : (
            <ul className="max-h-[570px] space-y-1 overflow-y-auto p-2">
              {modelRows.map((model) => {
                const active = model.modelToken === selectedModel?.modelToken
                return (
                  <li key={model.modelToken}>
                    <button
                      type="button"
                      aria-pressed={active}
                      onClick={() => {
                        setSelectedModelToken(model.modelToken)
                        setSelectedForecastToken(null)
                      }}
                      className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                        active
                          ? "border-primary/45 bg-primary/10"
                          : "border-transparent hover:border-border hover:bg-accent/45"
                      }`}
                    >
                      <span className="block truncate text-sm font-medium">
                        {model.label}
                      </span>
                      <span className="mt-1 block text-[11px] text-muted-foreground">
                        {model.evidenceState === "sufficient"
                          ? "Evidence available"
                          : model.evidenceState === "limited"
                            ? "Limited evidence"
                            : "Unavailable"}
                      </span>
                    </button>
                  </li>
                )
              })}
            </ul>
          )}
          <div className="p-3"><CursorNavigation navigation={navigation} next={models.data?.nextCursor} busy={models.isFetching} error={models.isError}
            onRestart={() => { if (navigation.after === undefined) void models.refetch() }} /></div>
        </section>

        <div className="space-y-4">
          <BundleEvidence
            model={selectedModelEvidence.data ?? null}
            available={modelsAvailable}
            loading={selectedModelToken !== null && selectedModelEvidence.isPending}
            error={selectedModelEvidence.isError ? "Try refreshing the page." : null}
          />
          <DemandPanel title="Open forecast preparation" className="rounded-xl border p-4">
          <ForecastPreparation
            bootstrap={bootstrap}
            transport={transport}
            onStarted={async () => {
              activityNavigation.restart(); forecastNavigation.restart()
              await Promise.all([
                modelActivityAvailable && activityNavigation.after === undefined
                  ? activities.refetch()
                  : Promise.resolve(),
                forecastsAvailable && forecastNavigation.after === undefined ? forecasts.refetch() : Promise.resolve(),
              ])
            }}
          />
          </DemandPanel>
          <ForecastReview
            bootstrap={bootstrap}
            transport={transport}
            forecasts={forecastRows}
            selected={selectedForecast}
            available={forecastsAvailable}
            loading={forecasts.isPending}
            error={
              forecasts.isError ? "Forecasts are unavailable right now." : null
            }
            select={setSelectedForecastToken}
          />
      {forecastsAvailable ? <CursorNavigation navigation={forecastNavigation} next={forecasts.data?.nextCursor} busy={forecasts.isFetching} error={forecasts.isError}
        onRestart={() => { if (forecastNavigation.after === undefined) void forecasts.refetch() }} /> : null}
          {modelActivityAvailable ? <CursorNavigation navigation={activityNavigation} next={activities.data?.nextCursor} busy={activities.isFetching} error={activities.isError}
            onRestart={() => { if (activityNavigation.after === undefined) void activities.refetch() }} /> : null}
          <ModelJobActivity
            activities={activityRows}
            available={modelActivityAvailable}
            loading={activities.isPending}
            error={
              activities.isError
                ? "Research activity is unavailable right now."
                : null
            }
          />
        </div>
      </div>
    </ModelsFrame>
  )
}

function SummaryFact({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Activity
  label: string
  value: string
}) {
  return (
    <div className="border-b border-border p-4 sm:border-r xl:border-b-0 xl:last:border-r-0">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
    </div>
  )
}

function queryCount(
  available: boolean,
  pending: boolean,
  error: boolean,
  count: number,
): string {
  if (!available || error) return "Unavailable"
  if (pending) return "Loading…"
  return count.toLocaleString()
}

function InlineUnavailable({ text }: { text: string }) {
  return <p className="p-5 text-sm leading-6 text-muted-foreground">{text}</p>
}

function UnavailableEvidence({
  title,
  detail,
}: {
  title: string
  detail: string
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <ShieldAlert className="size-5 text-muted-foreground" aria-hidden="true" />
      <h1 className="mt-4 text-lg font-semibold">{title}</h1>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">
        {detail}
      </p>
    </section>
  )
}

function ModelsFrame({ children }: { children: React.ReactNode }) {
  return (
    <main className="mx-auto w-full max-w-[1320px] p-5 lg:p-7">
      {children}
    </main>
  )
}

function ModelsLoading() {
  return (
    <ModelsFrame>
      <Skeleton className="h-8 w-52" />
      <Skeleton className="mt-3 h-4 w-full max-w-2xl" />
      <div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {Array.from({ length: 4 }, (_, index) => (
          <Skeleton key={index} className="h-24 rounded-xl" />
        ))}
      </div>
      <Skeleton className="mt-5 h-[560px] rounded-xl" />
    </ModelsFrame>
  )
}

function ListLoading() {
  return (
    <div className="space-y-2 p-3">
      {Array.from({ length: 4 }, (_, index) => (
        <Skeleton key={index} className="h-16 rounded-lg" />
      ))}
    </div>
  )
}
