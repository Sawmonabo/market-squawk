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

import { BundleEvidence } from "./bundle-evidence"
import { ForecastPreparation } from "./forecast-preparation"
import { ForecastReview } from "./forecast-review"
import { ModelJobActivity } from "./model-jobs"
import {
  isActiveModelActivity,
  parseForecasts,
  parseModelActivities,
  parseModelEvidence,
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

  const models = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.ListProductEvidence",
      {},
    ),
    queryFn: async () => {
      return parseModelEvidence(await transport.modelProducts({ action: "list" }))
    },
    enabled: modelsAvailable,
  })
  const forecasts = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.ListForecasts",
      {},
    ),
    queryFn: async () =>
      parseForecasts(await transport.query({ query: "forecasts" })),
    enabled: forecastsAvailable,
  })
  const activities = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.ListProductActivity",
      {},
    ),
    queryFn: async () => {
      return parseModelActivities(
        await transport.modelProducts({ action: "activity" }),
      )
    },
    enabled: modelActivityAvailable,
    refetchInterval: 5_000,
  })

  const modelRows = models.data ?? []
  const selectedModel =
    modelRows.find((model) => model.modelToken === selectedModelToken) ??
    modelRows[0] ??
    null
  const forecastRows = forecasts.data?.forecasts ?? []
  const selectedForecast =
    forecastRows.find(
      (forecast) => forecast.forecastToken === selectedForecastToken,
    ) ??
    forecastRows[0] ??
    null
  const activityRows = activities.data ?? []
  const activeCount = activityRows.filter(isActiveModelActivity).length
  const calibratedForecasts = forecastRows.filter(
    (forecast) => forecast.evidenceState === "calibrated",
  ).length
  const refreshing =
    models.isFetching || forecasts.isFetching || activities.isFetching

  const refresh = () => {
    if (modelsAvailable) {
      void models.refetch()
      void activities.refetch()
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
          label="Research models"
          value={queryCount(
            modelsAvailable,
            models.isPending,
            models.isError,
            modelRows.length,
          )}
        />
        <SummaryFact
          icon={ChartNoAxesCombined}
          label="Forecasts"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            forecastRows.length,
          )}
        />
        <SummaryFact
          icon={ShieldAlert}
          label="Calibrated forecasts"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            calibratedForecasts,
          )}
        />
        <SummaryFact
          icon={Activity}
          label="Work in progress"
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
        </section>

        <div className="space-y-4">
          <BundleEvidence
            model={selectedModel}
            available={modelsAvailable}
            loading={models.isPending}
            error={models.isError ? "Try refreshing the page." : null}
          />
          <ForecastPreparation
            bootstrap={bootstrap}
            transport={transport}
            onStarted={async () => {
              await Promise.all([
                modelsAvailable
                  ? activities.refetch()
                  : Promise.resolve(),
                forecastsAvailable ? forecasts.refetch() : Promise.resolve(),
              ])
            }}
          />
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
          <ModelJobActivity
            activities={activityRows}
            available={modelsAvailable}
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
