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
import { friendlyResearchCollectionName } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { JobControlRequest, ProductTransport } from "@/lib/transport"

import { DatasetBuilder } from "./dataset-builder"
import { DatasetEvidence } from "./dataset-evidence"
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
          <AlertTitle>Research is unavailable right now</AlertTitle>
          <AlertDescription>
            Your other workspace areas are still available. Try opening Research again.
          </AlertDescription>
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
  const jobMutation = useMutation({
    mutationFn: ({ request }: { request: ResearchJobMutationRequest }) =>
      transport.jobControl(request, true),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: jobKey }),
  })

  const allDatasets = datasets.data?.pages.flatMap((page) => page.items) ?? []
  const normalizedFilter = filter.trim().toLocaleLowerCase()
  const visibleDatasets = normalizedFilter
    ? allDatasets.filter((dataset) =>
        friendlyResearchCollectionName(dataset.manifest.schema.name)
          .toLocaleLowerCase()
          .includes(normalizedFilter),
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
  const activeJobs = (jobs.data ?? []).filter((job) =>
    ["queued", "preparing", "running", "awaiting_confirmation", "recovering"].includes(
      job.state,
    ),
  ).length
  const refreshing = datasets.isFetching || jobs.isFetching

  return (
    <ResearchFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[10px] uppercase tracking-[0.18em] text-primary">
            Research and data
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Research</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Prepare dated information, review its history and limitations, and use it in analysis
            or model work. Connections manages where information comes from; Operations &amp; Jobs
            shows technical activity.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => {
            void datasets.refetch()
            void jobs.refetch()
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
        <DatasetError retry={() => void datasets.refetch()} />
      ) : allDatasets.length === 0 ? (
        <>
          <EmptyResearch />
          <div className="mt-4">
            <ResearchActivity
              jobs={jobs.data ?? []}
              loading={jobs.isPending}
              failed={jobs.isError}
              pendingJobId={
                jobMutation.isPending ? jobMutation.variables.request.jobId : null
              }
              mutationFailed={jobMutation.isError}
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
            className="mt-5 grid overflow-hidden rounded-xl border border-border bg-card/50 sm:grid-cols-3"
          >
            <ResearchFact
              icon={Database}
              label="Available collections"
              value={formatCount(allDatasets.length)}
            />
            <ResearchFact
              icon={Rows3}
              label="Research observations"
              value={formatCount(totalRows)}
            />
            <ResearchFact
              icon={Activity}
              label="Work in progress"
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
                  Find a collection
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
                    placeholder="Name of the information"
                    className="pl-9"
                  />
                </div>
              </div>
              <div className="max-h-[620px] overflow-y-auto p-2">
                {visibleDatasets.length ? (
                  <ul className="space-y-1" aria-label="Research collections">
                    {visibleDatasets.map((dataset) => {
                      const active =
                        selected?.manifest.datasetId === dataset.manifest.datasetId
                      return (
                        <li key={dataset.manifest.datasetId}>
                          <button
                            type="button"
                            aria-pressed={active}
                            onClick={() => setSelectedId(dataset.manifest.datasetId)}
                            className={`w-full rounded-lg border px-3 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                              active
                                ? "border-primary/45 bg-primary/10"
                                : "border-transparent hover:border-border hover:bg-accent/45"
                            }`}
                          >
                            <span className="block truncate text-sm font-medium">
                              {friendlyResearchCollectionName(dataset.manifest.schema.name)}
                            </span>
                            <span className="mt-1 block truncate text-[11px] text-muted-foreground">
                              {formatCount(dataset.rowCount)} observations
                            </span>
                          </button>
                        </li>
                      )
                    })}
                  </ul>
                ) : (
                  <p className="p-5 text-sm leading-6 text-muted-foreground">
                    No collection matches “{filter}”. Try another description.
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
                    {datasets.isFetchingNextPage ? "Loading…" : "Load more collections"}
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
                failed={jobs.isError}
                pendingJobId={
                  jobMutation.isPending ? jobMutation.variables.request.jobId : null
                }
                mutationFailed={jobMutation.isError}
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
  failed,
  pendingJobId,
  mutationFailed,
  act,
}: {
  jobs: ResearchJob[]
  loading: boolean
  failed: boolean
  pendingJobId: string | null
  mutationFailed: boolean
  act: (job: ResearchJob, action: "cancel" | "retry") => void
}) {
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
        <Activity className="size-4 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Background activity</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Longer tasks continue if this window closes.
          </p>
        </div>
        </div>
        <Button asChild size="xs" variant="outline">
          <Link to="/system/operations-jobs">See all activity</Link>
        </Button>
      </div>
      {loading ? (
        <Skeleton className="mt-4 h-20 rounded-lg" />
      ) : failed ? (
        <p className="mt-4 text-xs leading-5 text-destructive">
          Current research activity could not be loaded.
        </p>
      ) : jobs.length ? (
        <ul className="mt-4 space-y-2">
          {jobs.slice(0, 5).map((job) => (
            <li
              key={`${job.jobId}-${job.generation}`}
              className="rounded-lg border border-border bg-background/40 p-3"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <p className="truncate text-xs font-medium">{jobLabel(job.kind)}</p>
                <EvidenceBadge>{activityStateLabel(job.state)}</EvidenceBadge>
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
          Nothing is running right now.
        </p>
      )}
      {mutationFailed ? (
        <p className="mt-3 text-xs leading-5 text-destructive">
          That activity could not be changed. Refresh and try again.
        </p>
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
        Retry
      </Button>
    )
  }
  if (
    ["queued", "preparing", "running", "awaiting_confirmation", "recovering"].includes(
      job.state,
    )
  ) {
    return (
      <Button
        className="mt-3"
        size="xs"
        variant="outline"
        disabled={pending}
        onClick={() => act(job, "cancel")}
      >
        Cancel
      </Button>
    )
  }
  return null
}

function EmptyResearch() {
  return (
    <section className="mt-6 rounded-xl border border-dashed border-border bg-card/30 p-8 text-center">
      <Database className="mx-auto size-7 text-primary" aria-hidden="true" />
      <h2 className="mt-4 text-lg font-semibold">No research collections yet</h2>
      <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
        Add the information you want from Connections. Completed research will appear here when it
        is ready to use.
      </p>
      <Button asChild className="mt-5">
        <Link to="/connections/sources">Manage connections</Link>
      </Button>
    </section>
  )
}

function UnavailableResearch() {
  return (
    <Alert>
      <AlertCircle aria-hidden="true" />
      <AlertTitle>Research is not ready</AlertTitle>
      <AlertDescription>
        Finish local setup from Home before preparing or inspecting research data.
      </AlertDescription>
    </Alert>
  )
}

function DatasetError({ retry }: { retry: () => void }) {
  return (
    <div className="mt-6">
      <Alert variant="destructive">
        <AlertCircle aria-hidden="true" />
        <AlertTitle>Research information could not be loaded</AlertTitle>
        <AlertDescription>
          Refresh the research library to try again.
        </AlertDescription>
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
    <div className="mt-6 space-y-4" aria-label="Loading research information">
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

function jobLabel(kind: string) {
  if (kind === "research.ingest-source.v1") return "Load research information"
  if (kind === "research.phase-one-derived-generation-job.v1") {
    return "Prepare derived research data"
  }
  if (kind === "analysis.phase-one-feature-derived-generation-job.v1") {
    return "Prepare model features"
  }
  if (kind === "research.dataset-export.v1") return "Export research history"
  return "Research activity"
}

function activityStateLabel(state: string) {
  if (["queued", "preparing", "recovering"].includes(state)) return "Getting ready"
  if (state === "running") return "In progress"
  if (state === "awaiting_confirmation") return "Needs review"
  if (state === "cancelling") return "Stopping"
  if (state === "completed") return "Complete"
  if (["failed", "interrupted"].includes(state)) return "Needs attention"
  if (state === "cancelled") return "Stopped"
  return "Status unavailable"
}
