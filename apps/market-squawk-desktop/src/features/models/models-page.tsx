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
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { BundleEvidence } from "./bundle-evidence"
import { ForecastPreparation } from "./forecast-preparation"
import { ForecastReview } from "./forecast-review"
import { ModelJobActivity } from "./model-jobs"
import { ModelWorkflows } from "./model-workflows"
import {
  isActiveModelJob,
  parseForecasts,
  parseModelMetadata,
  parseModelBundles,
  parseModelJobs,
} from "./models-contracts"

export function ModelsPage() {
  const product = useProduct()

  if (product.status === "loading") return <ModelsLoading />
  if (product.status === "error") {
    return (
      <ModelsFrame>
        <UnavailableEvidence
          title="Model workspace unavailable"
          detail={product.error}
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
  const [selectedBundleId, setSelectedBundleId] = React.useState<string | null>(
    null,
  )
  const [selectedVintageId, setSelectedVintageId] = React.useState<
    string | null
  >(null)
  const supports = (operation: string) =>
    bootstrap.operations.some((candidate) => candidate.name === operation)
  const bundleAvailable = supports("Model.ListBundles")
  const forecastsAvailable = supports("Model.ListForecasts")
  const jobsAvailable = supports("Job.List")
  const metadataAvailable = supports("Model.GetMetadata")

  const bundles = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.ListBundles",
      {},
    ),
    queryFn: async () => parseModelBundles(await transport.query({ query: "modelBundles" })),
    enabled: bundleAvailable,
  })
  const forecasts = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.ListForecasts",
      {},
    ),
    queryFn: async () => parseForecasts(await transport.query({ query: "forecasts" })),
    enabled: forecastsAvailable,
  })
  const jobs = useQuery({
    queryKey: [
      ...productKeys.domain(bootstrap.runtime, "job"),
      "model-activity",
    ],
    queryFn: async () =>
      parseModelJobs(await transport.query({ query: "jobs", limit: 25 })),
    enabled: jobsAvailable,
    refetchInterval: 5_000,
  })

  const bundleRows = bundles.data?.bundles ?? []
  const selectedBundle =
    bundleRows.find(
      (bundle) =>
        `${bundle.bundleId}@${bundle.bundleVersion}` === selectedBundleId,
    ) ??
    bundleRows[0] ??
    null
  const forecastRows = forecasts.data?.forecasts ?? []
  const relevantForecasts = selectedBundle
    ? forecastRows.filter(
        (forecast) =>
          forecast.modelId === selectedBundle.modelId &&
          forecast.bundleId === selectedBundle.bundleId &&
          forecast.bundleVersion === selectedBundle.bundleVersion,
      )
    : forecastRows
  const selectedForecast =
    relevantForecasts.find(
      (forecast) => forecast.vintageId === selectedVintageId,
    ) ??
    relevantForecasts[0] ??
    null
  const modelJobs = jobs.data ?? []
  const metadata = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "Model",
      "Model.GetMetadata",
      { modelId: selectedBundle?.modelId ?? null },
    ),
    queryFn: async () => {
      if (!selectedBundle) throw new Error("No admitted model is selected.")
      return parseModelMetadata(
        await transport.query({
          query: "modelMetadata",
          modelId: selectedBundle.modelId,
        }),
      )
    },
    enabled: metadataAvailable && selectedBundle !== null,
  })
  const activeJobs = modelJobs.filter(isActiveModelJob).length
  const calibratedForecasts = forecastRows.filter(
    (forecast) => forecast.hasCalibratedIntervals,
  ).length
  const refreshing =
    bundles.isFetching || forecasts.isFetching || jobs.isFetching

  const refresh = () => {
    if (bundleAvailable) void bundles.refetch()
    if (forecastsAvailable) void forecasts.refetch()
    if (jobsAvailable) void jobs.refetch()
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
            Review model quality, forecasts, uncertainty, and background work. Modeled values
            are estimates, not guaranteed outcomes or automatic investment actions.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={refresh}
          disabled={refreshing || (!bundleAvailable && !forecastsAvailable && !jobsAvailable)}
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
          label="Admitted bundles"
          value={queryCount(bundleAvailable, bundles.isPending, bundles.isError, bundleRows.length)}
        />
        <SummaryFact
          icon={ChartNoAxesCombined}
          label="Forecast vintages"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            forecastRows.length,
          )}
        />
        <SummaryFact
          icon={ShieldAlert}
          label="Calibrated vintages"
          value={queryCount(
            forecastsAvailable,
            forecasts.isPending,
            forecasts.isError,
            calibratedForecasts,
          )}
        />
        <SummaryFact
          icon={Activity}
          label="Active model jobs"
          value={queryCount(jobsAvailable, jobs.isPending, jobs.isError, activeJobs)}
        />
      </section>

      <div className="mt-5 grid gap-4 xl:grid-cols-[minmax(260px,0.72fr)_minmax(0,1.5fr)]">
        <section className="overflow-hidden rounded-xl border border-border bg-card/35">
          <div className="border-b border-border p-4">
            <h2 className="text-sm font-semibold">Available models</h2>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Select a model to review its purpose, validation, limitations, and forecasts.
            </p>
          </div>
          {!bundleAvailable ? (
            <InlineUnavailable text="Models are unavailable in this workspace." />
          ) : bundles.isPending ? (
            <ListLoading />
          ) : bundles.isError ? (
            <InlineUnavailable text="Models are unavailable right now." />
          ) : bundleRows.length === 0 ? (
            <InlineUnavailable text="No model is ready for investment research yet." />
          ) : (
            <ul className="max-h-[570px] space-y-1 overflow-y-auto p-2">
              {bundleRows.map((bundle) => {
                const identity = `${bundle.bundleId}@${bundle.bundleVersion}`
                const active = selectedBundle
                  ? bundle.bundleId === selectedBundle.bundleId &&
                    bundle.bundleVersion === selectedBundle.bundleVersion
                  : false
                return (
                  <li key={`${bundle.modelId}:${identity}`}>
                    <button
                      type="button"
                      aria-pressed={active}
                      onClick={() => {
                        setSelectedBundleId(identity)
                        setSelectedVintageId(null)
                      }}
                      className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                        active
                          ? "border-primary/45 bg-primary/10"
                          : "border-transparent hover:border-border hover:bg-accent/45"
                      }`}
                    >
                      <span className="block truncate text-sm font-medium">
                        {bundle.bundleId}
                      </span>
                      <span className="mt-1 flex items-center justify-between gap-3 text-[11px] text-muted-foreground">
                        <span className="truncate">{bundle.format.replaceAll("_", " ")}</span>
                        <span className="shrink-0 font-mono">
                          v{bundle.bundleVersion}
                        </span>
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
            bundle={selectedBundle}
            metadata={metadata.data ?? null}
            metadataAvailable={metadataAvailable}
            loading={metadata.isPending && selectedBundle !== null}
            error={metadata.isError ? "Model details are unavailable right now." : null}
          />
          <ModelWorkflows
            bootstrap={bootstrap}
            transport={transport}
            metadata={metadata.data ?? null}
          />
          <ForecastPreparation
            bootstrap={bootstrap}
            transport={transport}
            selectedModel={
              selectedBundle
                ? {
                    modelId: selectedBundle.modelId,
                    bundleId: selectedBundle.bundleId,
                    bundleVersion: selectedBundle.bundleVersion,
                  }
                : null
            }
            onStarted={async () => {
              await Promise.all([
                jobsAvailable ? jobs.refetch() : Promise.resolve(),
                forecastsAvailable ? forecasts.refetch() : Promise.resolve(),
              ])
            }}
          />
          <ForecastReview
            bootstrap={bootstrap}
            transport={transport}
            forecasts={relevantForecasts}
            selected={selectedForecast}
            available={forecastsAvailable}
            loading={forecasts.isPending}
            error={forecasts.isError ? "Forecasts are unavailable right now." : null}
            completeness={forecasts.data?.completeness ?? null}
            select={setSelectedVintageId}
          />
          <ModelJobActivity
            jobs={modelJobs}
            available={jobsAvailable}
            loading={jobs.isPending}
            error={jobs.isError ? "Model activity is unavailable right now." : null}
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
  return <main className="mx-auto w-full max-w-[1320px] p-5 lg:p-7">{children}</main>
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
