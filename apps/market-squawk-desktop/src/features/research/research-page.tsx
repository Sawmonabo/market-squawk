import * as React from "react"
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query"
import {
  Activity,
  AlertCircle,
  Database,
  HardDrive,
  RefreshCw,
  Rows3,
  Search,
} from "lucide-react"
import { Link } from "react-router-dom"

import { productKeys } from "@/app/query-client"
import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import {
  H15Dashboard,
  H15_SURFACE_ID,
  parseMacroDashboard,
  type MacroDashboardSourceReadiness,
} from "@/features/macro"
import {
  sourceEvidence,
  type SourceEvidence,
} from "@/features/sources/source-evidence"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { JobControlRequest, ProductTransport } from "@/lib/transport"

import { DatasetBuilder } from "./dataset-builder"
import { DatasetEvidence } from "./dataset-evidence"
import { ResearchIngestion } from "./research-ingestion"
import {
  parseResearchDatasetPage,
  parseResearchJobs,
  type ResearchJob,
} from "./research-contracts"

type ResearchJobMutationRequest = Extract<
  JobControlRequest,
  { action: "cancel" | "retry" }
>

export function ResearchPage() {
  const product = useProduct()

  if (product.status === "loading") return <ResearchLoading />
  if (product.status === "error") {
    return (
      <ResearchFrame>
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Research workspace unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </ResearchFrame>
    )
  }

  const available = product.bootstrap.operations.some(
    (operation) => operation.name === "Research.ListDatasets",
  )
  if (!available) {
    return (
      <ResearchFrame>
        <UnavailableResearch />
      </ResearchFrame>
    )
  }

  return (
    <ResearchWorkspace
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ResearchWorkspace({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const [filter, setFilter] = React.useState("")
  const [selectedId, setSelectedId] = React.useState<string | null>(null)
  const datasetKey = [
    ...productKeys.domain(bootstrap.runtime, "research"),
    "datasets",
  ] as const
  const jobKey = [
    ...productKeys.domain(bootstrap.runtime, "job"),
    "research-activity",
  ] as const
  const operations = new Set(
    bootstrap.operations.map((operation) => operation.name),
  )
  const macroDashboardAvailable = operations.has("Macro.GetDashboard")
  const sourceStatusAvailable = operations.has("Source.GetStatus")
  const h15Key = productKeys.operation(
    bootstrap.runtime,
    "research",
    "Macro.GetDashboard",
    {
      provider: H15_SURFACE_ID,
      release: "h15",
    },
  )
  const h15SourceKey = productKeys.operation(
    bootstrap.runtime,
    "source",
    "Source.GetStatus",
    { sourceIds: [H15_SURFACE_ID] },
  )
  const datasets = useInfiniteQuery({
    queryKey: datasetKey,
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      parseResearchDatasetPage(
        await transport.query({
          query: "researchDatasets",
          ...(pageParam ? { afterDataset: pageParam } : {}),
        }),
      ),
    getNextPageParam: (page) =>
      page.hasMore ? (page.nextAfterDataset ?? undefined) : undefined,
  })
  const jobs = useQuery({
    queryKey: jobKey,
    queryFn: async () =>
      parseResearchJobs(await transport.query({ query: "jobs", limit: 25 })),
    refetchInterval: 5_000,
  })
  const h15 = useQuery({
    queryKey: h15Key,
    enabled: macroDashboardAvailable,
    queryFn: async () =>
      parseMacroDashboard(
        await transport.query({
          query: "macroDashboard",
          provider: H15_SURFACE_ID,
          release: "h15",
        }),
      ),
  })
  const h15Source = useQuery({
    queryKey: h15SourceKey,
    enabled: macroDashboardAvailable && sourceStatusAvailable,
    queryFn: () =>
      transport.query({
        query: "sourceStatus",
        sourceIds: [H15_SURFACE_ID],
      }),
  })
  const jobMutation = useMutation({
    mutationFn: ({ request }: { request: ResearchJobMutationRequest }) =>
      transport.jobControl(request, true),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: jobKey }),
  })

  const allDatasets = datasets.data?.pages.flatMap((page) => page.items) ?? []
  const normalizedFilter = filter.trim().toLocaleLowerCase()
  const visibleDatasets = normalizedFilter
    ? allDatasets.filter((dataset) =>
        [
          dataset.manifest.datasetId,
          dataset.sourceId,
          dataset.manifest.schema.name,
          dataset.generationKind,
        ].some((value) => value.toLocaleLowerCase().includes(normalizedFilter)),
      )
    : allDatasets
  const selected =
    visibleDatasets.find(
      (dataset) => dataset.manifest.datasetId === selectedId,
    ) ??
    visibleDatasets[0] ??
    null
  const totalRows = allDatasets.reduce(
    (total, dataset) => total + dataset.rowCount,
    0,
  )
  const totalBytes = allDatasets.reduce(
    (total, dataset) => total + dataset.totalBytes,
    0,
  )
  const activeJobs = (jobs.data ?? []).filter((job) =>
    ["queued", "preparing", "running", "awaiting_confirmation", "recovering"].includes(
      job.state,
    ),
  ).length
  const h15SourceReadiness = macroSourceReadiness(
    sourceStatusAvailable,
    h15Source.isPending,
    h15Source.isError,
    h15Source.data
      ? sourceEvidence(
          bootstrap.providerProfiles,
          bootstrap.providerSessions,
          [h15Source.data],
          undefined,
          undefined,
        ).find((source) => source.id === H15_SURFACE_ID)
      : undefined,
  )
  const refreshing =
    datasets.isFetching ||
    jobs.isFetching ||
    (macroDashboardAvailable && h15.isFetching) ||
    (macroDashboardAvailable && sourceStatusAvailable && h15Source.isFetching)

  return (
    <ResearchFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Point-in-time research library
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">
            Research
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Inspect immutable analytical datasets, their exact lineage, and
            durable research work without changing the underlying evidence.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => {
            void datasets.refetch()
            void jobs.refetch()
            if (macroDashboardAvailable) void h15.refetch()
            if (macroDashboardAvailable && sourceStatusAvailable) {
              void h15Source.refetch()
            }
          }}
          disabled={refreshing}
        >
          <RefreshCw
            className={refreshing ? "animate-spin" : ""}
            aria-hidden="true"
          />
          Refresh
        </Button>
      </header>

      {macroDashboardAvailable ? (
        <div className="mt-6">
          <H15Dashboard
            state={
              h15.isPending
                ? { status: "loading" }
                : h15.isError
                  ? {
                      status: "error",
                      message: messageFrom(h15.error),
                      onRetry: () => void h15.refetch(),
                    }
                  : {
                      status: "ready",
                      dashboard: h15.data,
                      sourceReadiness: h15SourceReadiness,
                    }
            }
          />
        </div>
      ) : null}

      <ResearchIngestion
        bootstrap={bootstrap}
        transport={transport}
        onStarted={() => {
          void queryClient.invalidateQueries({ queryKey: jobKey })
        }}
      />

      <DatasetBuilder
        bootstrap={bootstrap}
        transport={transport}
        onStarted={async () => {
          await Promise.all([
            queryClient.invalidateQueries({ queryKey: jobKey }),
            queryClient.invalidateQueries({ queryKey: datasetKey }),
          ])
        }}
      />

      {datasets.isPending ? (
        <ResearchContentLoading />
      ) : datasets.isError ? (
        <DatasetError
          message={messageFrom(datasets.error)}
          retry={() => void datasets.refetch()}
        />
      ) : allDatasets.length === 0 ? (
        <>
          <EmptyResearch />
          <div className="mt-4">
            <ResearchActivity
              jobs={jobs.data ?? []}
              loading={jobs.isPending}
              error={jobs.isError ? messageFrom(jobs.error) : null}
              pendingJobId={
                jobMutation.isPending
                  ? jobMutation.variables.request.jobId
                  : null
              }
              mutationError={
                jobMutation.isError ? messageFrom(jobMutation.error) : null
              }
              act={(job, action) =>
                jobMutation.mutate({
                  request: {
                    action,
                    jobId: job.jobId,
                    generation: job.generation,
                    expectedSequence: job.sequence,
                  },
                })
              }
            />
          </div>
        </>
      ) : (
        <>
          <section
            aria-label="Loaded research facts"
            className="mt-5 grid overflow-hidden rounded-xl border border-border bg-card/50 sm:grid-cols-2 xl:grid-cols-4"
          >
            <ResearchFact
              icon={Database}
              label="Loaded generations"
              value={formatCount(allDatasets.length)}
            />
            <ResearchFact
              icon={Rows3}
              label="Rows represented"
              value={formatCount(totalRows)}
            />
            <ResearchFact
              icon={HardDrive}
              label="Immutable storage"
              value={formatBytes(totalBytes)}
            />
            <ResearchFact
              icon={Activity}
              label="Active research jobs"
              value={
                jobs.isError
                  ? "Unavailable"
                  : jobs.isPending
                    ? "Loading…"
                    : formatCount(activeJobs)
              }
            />
          </section>

          <div className="mt-5 grid min-h-[560px] gap-4 xl:grid-cols-[minmax(280px,0.78fr)_minmax(0,1.42fr)]">
            <section className="overflow-hidden rounded-xl border border-border bg-card/35">
              <div className="border-b border-border p-4">
                <label
                  htmlFor="research-dataset-filter"
                  className="text-xs font-semibold"
                >
                  Find a dataset
                </label>
                <div className="relative mt-2">
                  <Search
                    className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
                    aria-hidden="true"
                  />
                  <Input
                    id="research-dataset-filter"
                    value={filter}
                    onChange={(event) => setFilter(event.target.value)}
                    placeholder="Name, source, schema, or generation"
                    className="pl-9"
                  />
                </div>
              </div>
              <div className="max-h-[620px] overflow-y-auto p-2">
                {visibleDatasets.length ? (
                  <ul className="space-y-1" aria-label="Research datasets">
                    {visibleDatasets.map((dataset) => {
                      const active =
                        selected?.manifest.datasetId ===
                        dataset.manifest.datasetId
                      return (
                        <li key={dataset.manifest.datasetId}>
                          <button
                            type="button"
                            aria-pressed={active}
                            onClick={() =>
                              setSelectedId(dataset.manifest.datasetId)
                            }
                            className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                              active
                                ? "border-primary/45 bg-primary/10"
                                : "border-transparent hover:border-border hover:bg-accent/45"
                            }`}
                          >
                            <span className="block truncate text-sm font-medium">
                              {dataset.manifest.datasetId}
                            </span>
                            <span className="mt-1 flex items-center justify-between gap-3 text-[11px] text-muted-foreground">
                              <span className="truncate">{dataset.sourceId}</span>
                              <span className="shrink-0 font-mono">
                                v{dataset.manifest.manifestVersion}
                              </span>
                            </span>
                          </button>
                        </li>
                      )
                    })}
                  </ul>
                ) : (
                  <p className="p-5 text-sm leading-6 text-muted-foreground">
                    No loaded dataset matches “{filter}”. Try a dataset name,
                    source, schema, or generation type.
                  </p>
                )}
              </div>
              {datasets.hasNextPage ? (
                <div className="border-t border-border p-3">
                  <Button
                    className="w-full"
                    variant="outline"
                    onClick={() => void datasets.fetchNextPage()}
                    disabled={datasets.isFetchingNextPage}
                  >
                    {datasets.isFetchingNextPage ? "Loading…" : "Load more datasets"}
                  </Button>
                </div>
              ) : null}
            </section>

            <div className="space-y-4">
              {selected ? (
                <DatasetEvidence
                  dataset={selected}
                  bootstrap={bootstrap}
                  transport={transport}
                />
              ) : null}
              <ResearchActivity
                jobs={jobs.data ?? []}
                loading={jobs.isPending}
                error={jobs.isError ? messageFrom(jobs.error) : null}
                pendingJobId={
                  jobMutation.isPending
                    ? jobMutation.variables.request.jobId
                    : null
                }
                mutationError={
                  jobMutation.isError ? messageFrom(jobMutation.error) : null
                }
                act={(job, action) =>
                  jobMutation.mutate({
                    request: {
                      action,
                      jobId: job.jobId,
                      generation: job.generation,
                      expectedSequence: job.sequence,
                    },
                  })
                }
              />
            </div>
          </div>
        </>
      )}
    </ResearchFrame>
  )
}

function ResearchActivity({
  jobs,
  loading,
  error,
  pendingJobId,
  mutationError,
  act,
}: {
  jobs: ResearchJob[]
  loading: boolean
  error: string | null
  pendingJobId: string | null
  mutationError: string | null
  act: (job: ResearchJob, action: "cancel" | "retry") => void
}) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <div className="flex items-center gap-3">
        <Activity className="size-4 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Research activity</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Durable work continues if this window closes.
          </p>
        </div>
      </div>
      {loading ? (
        <Skeleton className="mt-4 h-20 rounded-lg" />
      ) : error ? (
        <p className="mt-4 text-xs leading-5 text-destructive">{error}</p>
      ) : jobs.length ? (
        <ul className="mt-4 space-y-2">
          {jobs.slice(0, 5).map((job) => (
            <li
              key={`${job.jobId}-${job.generation}`}
              className="rounded-lg border border-border bg-background/40 p-3"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="truncate text-xs font-medium">{jobLabel(job.kind)}</p>
                  <p className="mt-1 font-mono text-[10px] text-muted-foreground">
                    {job.jobId.slice(0, 12)} · generation {job.generation}
                  </p>
                </div>
                <EvidenceBadge>{humanize(job.state)}</EvidenceBadge>
              </div>
              {job.totalUnits !== null && job.completedUnits !== null ? (
                <div className="mt-3">
                  <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                    <div
                      className="h-full rounded-full bg-primary"
                      style={{
                        width: `${Math.min(100, (job.completedUnits / Math.max(1, job.totalUnits)) * 100)}%`,
                      }}
                    />
                  </div>
                  <p className="mt-1 text-[10px] text-muted-foreground">
                    {formatCount(job.completedUnits)} of {formatCount(job.totalUnits)}
                    {job.phase ? ` · ${humanize(job.phase)}` : ""}
                  </p>
                </div>
              ) : null}
              <JobAction
                job={job}
                pending={pendingJobId === job.jobId}
                act={act}
              />
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-4 text-xs leading-5 text-muted-foreground">
          No research ingestion, dataset build, or export job is currently retained.
        </p>
      )}
      {mutationError ? (
        <p className="mt-3 text-xs leading-5 text-destructive">{mutationError}</p>
      ) : null}
    </section>
  )
}

function JobAction({
  job,
  pending,
  act,
}: {
  job: ResearchJob
  pending: boolean
  act: (job: ResearchJob, action: "cancel" | "retry") => void
}) {
  if (["failed", "cancelled", "interrupted"].includes(job.state)) {
    return (
      <Button
        className="mt-3"
        size="xs"
        variant="outline"
        disabled={pending}
        onClick={() => act(job, "retry")}
      >
        Retry from retained input
      </Button>
    )
  }
  if (["queued", "preparing", "running", "awaiting_confirmation", "recovering"].includes(job.state)) {
    return (
      <Button
        className="mt-3"
        size="xs"
        variant="outline"
        disabled={pending}
        onClick={() => act(job, "cancel")}
      >
        Cancel job
      </Button>
    )
  }
  return null
}

function EmptyResearch() {
  return (
    <section className="mt-6 rounded-xl border border-dashed border-border bg-card/30 p-8 text-center">
      <Database className="mx-auto size-7 text-primary" aria-hidden="true" />
      <h2 className="mt-4 text-lg font-semibold">No analytical datasets yet</h2>
      <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
        Connect and verify a research source first. Market Squawk will show a
        dataset here only after its immutable generation is durably published.
      </p>
      <Button asChild className="mt-5">
        <Link to="/connections/sources">Review research sources</Link>
      </Button>
    </section>
  )
}

function UnavailableResearch() {
  return (
    <Alert>
      <AlertCircle aria-hidden="true" />
      <AlertTitle>Research service is not ready</AlertTitle>
      <AlertDescription>
        Restore the installed Research service from Home before opening
        local analytical datasets.
      </AlertDescription>
    </Alert>
  )
}

function DatasetError({ message, retry }: { message: string; retry: () => void }) {
  return (
    <div className="mt-6">
      <Alert variant="destructive">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Datasets could not be loaded</AlertTitle>
        <AlertDescription>{message}</AlertDescription>
      </Alert>
      <Button className="mt-4" variant="outline" onClick={retry}>
        Try again
      </Button>
    </div>
  )
}

function ResearchFact({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Database
  label: string
  value: string
}) {
  return (
    <div className="border-b border-border p-4 last:border-b-0 sm:odd:border-r xl:border-b-0 xl:border-r xl:last:border-r-0">
      <div className="flex items-center gap-2 text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        <p className="text-[9px] uppercase tracking-wider">{label}</p>
      </div>
      <p className="mt-2 font-mono text-lg font-semibold">{value}</p>
    </div>
  )
}

function EvidenceBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className="rounded-full border border-primary/30 bg-primary/10 px-2.5 py-1 text-[10px] font-medium text-primary">
      {children}
    </span>
  )
}

function ResearchFrame({ children }: { children: React.ReactNode }) {
  return <main className="mx-auto w-full max-w-[1240px] p-5 lg:p-7">{children}</main>
}

function ResearchLoading() {
  return (
    <ResearchFrame>
      <Skeleton className="h-4 w-40" />
      <Skeleton className="mt-3 h-10 w-52" />
      <Skeleton className="mt-3 h-5 w-3/5" />
      <ResearchContentLoading />
    </ResearchFrame>
  )
}

function ResearchContentLoading() {
  return (
    <div className="mt-6 space-y-4" aria-label="Loading research datasets">
      <Skeleton className="h-20 rounded-xl" />
      <div className="grid gap-4 xl:grid-cols-[minmax(280px,0.78fr)_minmax(0,1.42fr)]">
        <Skeleton className="h-[560px] rounded-xl" />
        <Skeleton className="h-[560px] rounded-xl" />
      </div>
    </div>
  )
}

function formatCount(value: number) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value)
}

function formatBytes(value: number) {
  if (value < 1_024) return `${value} B`
  const units = ["KiB", "MiB", "GiB", "TiB"]
  let amount = value
  let unit = -1
  do {
    amount /= 1_024
    unit += 1
  } while (amount >= 1_024 && unit < units.length - 1)
  return `${amount.toFixed(amount >= 10 ? 0 : 1)} ${units[unit]}`
}

function humanize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase())
}

function jobLabel(kind: string) {
  if (kind === "research.ingest-source.v1") return "Ingest research source"
  if (kind === "research.dataset-build.v1") return "Build point-in-time dataset"
  if (kind === "research.dataset-export.v1") return "Export research history"
  return humanize(kind.replace(/^research\./, "").replace(/\.v\d+$/, ""))
}

function messageFrom(error: unknown) {
  return error instanceof Error
    ? error.message
    : "Market Squawk could not complete this local research request."
}

function macroSourceReadiness(
  available: boolean,
  pending: boolean,
  failed: boolean,
  source: SourceEvidence | undefined,
): MacroDashboardSourceReadiness | null {
  if (!available) return null
  if (pending) {
    return {
      state: "unknown",
      label: "Checking",
      detail:
        "Current provider acquisition readiness is being checked separately from stored data.",
      lifecycleObservedAt: null,
      runtimeObservedAt: null,
    }
  }
  if (failed) {
    return {
      state: "unavailable",
      label: "Readiness unavailable",
      detail:
        "Current provider acquisition readiness could not be read; the stored publication remains queryable.",
      lifecycleObservedAt: null,
      runtimeObservedAt: null,
    }
  }
  if (!source?.operationalState) {
    return {
      state: "unknown",
      label: "Not reported",
      detail:
        "The source status response did not contain a safely recognized acquisition state.",
      lifecycleObservedAt: source?.lifecycle?.observedAt ?? null,
      runtimeObservedAt: source?.runtimeObservedAt ?? null,
    }
  }

  switch (source.operationalState) {
    case "active":
      return {
        state: "active",
        label: "Active",
        detail:
          "Provider acquisition is active; stored publication readiness remains independently evidenced.",
        lifecycleObservedAt: source.lifecycle?.observedAt ?? null,
        runtimeObservedAt: source.runtimeObservedAt,
      }
    case "stopped":
    case "removed":
      return {
        state: "inactive",
        label: "Inactive",
        detail:
          "Provider acquisition is inactive; the retained publication is still stored and queryable.",
        lifecycleObservedAt: source.lifecycle?.observedAt ?? null,
        runtimeObservedAt: source.runtimeObservedAt,
      }
    case "blocked":
      return {
        state: "blocked",
        label: "Blocked",
        detail:
          "Provider acquisition requires attention; the retained publication is still stored and queryable.",
        lifecycleObservedAt: source.lifecycle?.observedAt ?? null,
        runtimeObservedAt: source.runtimeObservedAt,
      }
    case "unavailable":
    case "failed":
      return {
        state: "unavailable",
        label: "Unavailable",
        detail:
          "Provider acquisition is unavailable; the retained publication is still stored and queryable.",
        lifecycleObservedAt: source.lifecycle?.observedAt ?? null,
        runtimeObservedAt: source.runtimeObservedAt,
      }
    default:
      return {
        state: "unknown",
        label: humanize(source.operationalState),
        detail:
          "Provider acquisition is changing or unrecognized; stored publication readiness remains independent.",
        lifecycleObservedAt: source.lifecycle?.observedAt ?? null,
        runtimeObservedAt: source.runtimeObservedAt,
      }
  }
}
