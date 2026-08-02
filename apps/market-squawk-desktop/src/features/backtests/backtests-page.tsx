import * as React from "react"
import {
  Activity,
  AlertCircle,
  BarChart3,
  CircleDollarSign,
  Database,
  FlaskConical,
  GitCompareArrows,
  LoaderCircle,
  RefreshCw,
  ShieldCheck,
  Split,
} from "lucide-react"
import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query"

import { messageFrom, useProduct } from "@/app/product-context"
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
import {
  digestHex,
  parseJobPage,
  type JobView,
  type PendingJobAction,
} from "@/features/operations/contracts"
import { JobCard } from "@/features/operations/job-card"
import { humanize } from "@/lib/formatters"
import { compareLosslessIntegers } from "@/lib/lossless-integer"
import { formatTimestamp } from "@/lib/time"
import type { JobControlRequest, ProductTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"

import {
  BACKTEST_JOB_KIND,
  BACKTEST_RESULT_AUTHORITY,
  parseBacktestRecord,
  type BacktestMetric,
  type BacktestRecord,
} from "./contracts"

const JOB_LIMIT = 50

export function BacktestsPage() {
  const product = useProduct()

  if (product.status !== "ready") {
    return (
      <BacktestsFrame>
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Backtests are unavailable</AlertTitle>
          <AlertDescription>
            {product.status === "error"
              ? product.error
              : "Connecting to the installed Market Squawk service."}
          </AlertDescription>
        </Alert>
      </BacktestsFrame>
    )
  }

  return (
    <BacktestsWorkspace
      transport={product.transport}
      scope={product.bootstrap.runtime}
      operations={new Set(
        product.bootstrap.operations.map((operation) => operation.name),
      )}
    />
  )
}

function BacktestsWorkspace({
  transport,
  scope,
  operations,
}: {
  transport: ProductTransport
  scope: ProductScope
  operations: ReadonlySet<string>
}) {
  const queryClient = useQueryClient()
  const [selectedJobKey, setSelectedJobKey] = React.useState<string | null>(null)
  const [pendingAction, setPendingAction] =
    React.useState<PendingJobAction | null>(null)
  const jobsKey = productKeys.operation(scope, "job", "Job.List", {
    kind: BACKTEST_JOB_KIND,
    limit: JOB_LIMIT,
  })
  const jobsQuery = useQuery({
    queryKey: jobsKey,
    enabled: operations.has("Job.List"),
    queryFn: async () =>
      parseJobPage(await transport.query({ query: "jobs", limit: JOB_LIMIT })),
    refetchInterval: 5_000,
  })
  const mutation = useMutation({
    mutationFn: (action: PendingJobAction) =>
      transport.jobControl(controlRequest(action), true),
    onSuccess: async () => {
      setPendingAction(null)
      await queryClient.invalidateQueries({ queryKey: jobsKey })
    },
  })
  const jobs = React.useMemo(
    () =>
      (jobsQuery.data?.jobs ?? [])
        .filter((job) => job.kind === BACKTEST_JOB_KIND)
        .sort((left, right) =>
          compareLosslessIntegers(right.updatedAt, left.updatedAt),
        ),
    [jobsQuery.data],
  )
  const selectedJob =
    jobs.find((job) => jobKey(job) === selectedJobKey) ?? jobs[0] ?? null
  const resultIdentity =
    selectedJob?.state === "completed" &&
    selectedJob.result?.authority === BACKTEST_RESULT_AUTHORITY
      ? selectedJob.result.identity
      : null
  const resultQuery = useQuery({
    queryKey: productKeys.operation(scope, "analysis", "Analysis.GetBacktests", {
      runId: resultIdentity,
    }),
    enabled: resultIdentity !== null && operations.has("Analysis.GetBacktests"),
    queryFn: async () => {
      if (!resultIdentity) throw new Error("No governed result identity is available.")
      return parseBacktestRecord(
        await transport.query({ query: "backtest", runId: resultIdentity }),
      )
    },
  })
  const reportArtifact = React.useMemo(() => {
    if (resultQuery.data?.status.state !== "completed") return null
    const artifact = resultQuery.data.status.artifact
    return "artifactId" in artifact ? artifact : null
  }, [resultQuery.data])
  const reportQuery = useQuery({
    queryKey: productKeys.operation(scope, "analysis", "Analysis.ReadArtifact", {
      artifactId: reportArtifact?.artifactId ?? null,
    }),
    enabled: false,
    queryFn: async () => {
      if (!reportArtifact) throw new Error("No compatible governed report is available.")
      return transport.query({
        query: "analysisArtifact",
        artifactId: reportArtifact.artifactId,
        sha256: reportArtifact.sha256,
        byteCount: reportArtifact.byteCount,
        mediaType: reportArtifact.mediaType,
        offset: 0,
        maximumBytes: Math.min(reportArtifact.byteCount, 64 * 1024),
      })
    },
  })

  return (
    <BacktestsFrame>
      <BuilderAvailability
        operations={operations}
        transport={transport}
        onStarted={() => queryClient.invalidateQueries({ queryKey: jobsKey })}
      />

      <section className="mt-7 grid gap-5 xl:grid-cols-[minmax(0,0.92fr)_minmax(0,1.08fr)]">
        <div>
          <div className="flex items-end justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Durable backtest activity</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Reconnect to admitted runs and act only on their current job evidence.
              </p>
            </div>
            <Button
              size="sm"
              variant="outline"
              disabled={jobsQuery.isFetching || !operations.has("Job.List")}
              onClick={() => void jobsQuery.refetch()}
            >
              <RefreshCw
                className={cn(jobsQuery.isFetching && "animate-spin")}
                aria-hidden="true"
              />
              Refresh
            </Button>
          </div>

          {!operations.has("Job.List") ? (
            <UnavailableCard
              title="Backtest jobs unavailable"
              detail="The installed service did not advertise the closed Job.List operation."
            />
          ) : jobsQuery.isPending ? (
            <LoadingCard label="Loading durable backtest jobs…" />
          ) : jobsQuery.isError ? (
            <UnavailableCard
              title="Backtest jobs could not be loaded"
              detail={messageFrom(jobsQuery.error)}
            />
          ) : jobs.length === 0 ? (
            <UnavailableCard
              title="No governed backtests yet"
              detail="A run will appear here only after the service admits its immutable point-in-time inputs and durable job."
            />
          ) : (
            <div className="mt-4 grid gap-3">
              {jobs.map((job) => (
                <div
                  key={jobKey(job)}
                  className={cn(
                    "rounded-xl ring-offset-background transition",
                    jobKey(job) === jobKey(selectedJob) &&
                      "ring-1 ring-primary/60",
                  )}
                >
                  <JobCard
                    job={job}
                    transport={transport}
                    scope={scope}
                    mutationPending={mutation.isPending}
                    onAction={(action) => {
                      mutation.reset()
                      setPendingAction(action)
                    }}
                  />
                  <Button
                    className="mx-4 mb-4"
                    size="sm"
                    variant={
                      jobKey(job) === jobKey(selectedJob) ? "default" : "outline"
                    }
                    onClick={() => setSelectedJobKey(jobKey(job))}
                  >
                    {jobKey(job) === jobKey(selectedJob)
                      ? "Selected"
                      : "Inspect result"}
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>

        <BacktestEvidence
          job={selectedJob}
          record={resultQuery.data ?? null}
          loading={resultQuery.isPending && resultQuery.fetchStatus !== "idle"}
          error={resultQuery.isError ? messageFrom(resultQuery.error) : null}
          canRead={operations.has("Analysis.GetBacktests")}
          onReadReport={reportArtifact ? () => void reportQuery.refetch() : null}
          reportState={
            reportQuery.isFetching
              ? "loading"
              : reportQuery.isError
                ? messageFrom(reportQuery.error)
                : reportQuery.data
                  ? "retrieved"
                  : null
          }
        />
      </section>

      <SemanticsGuide />

      {mutation.isError && (
        <Alert variant="destructive" className="mt-5">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>The backtest job did not change</AlertTitle>
          <AlertDescription>{messageFrom(mutation.error)}</AlertDescription>
        </Alert>
      )}
      <ActionDialog
        action={pendingAction}
        pending={mutation.isPending}
        error={mutation.isError ? messageFrom(mutation.error) : null}
        onOpenChange={(open) => {
          if (!open && !mutation.isPending) setPendingAction(null)
        }}
        onConfirm={() => {
          if (pendingAction) mutation.mutate(pendingAction)
        }}
      />
    </BacktestsFrame>
  )
}

function BuilderAvailability({
  operations,
  transport,
  onStarted,
}: {
  operations: ReadonlySet<string>
  transport: ProductTransport
  onStarted: () => Promise<unknown>
}) {
  const [confirmationOpen, setConfirmationOpen] = React.useState(false)
  const mutation = useMutation({
    mutationFn: () => transport.startBacktestFromFile(true),
    onSuccess: async (result) => {
      setConfirmationOpen(false)
      if (result !== null) await onStarted()
    },
  })
  const available = operations.has("Analysis.StartBacktest")

  return (
    <section className="rounded-2xl border border-border bg-card/45 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Governed experiment builder
          </p>
          <h2 className="mt-2 text-xl font-semibold">Test an idea against history</h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            A run must bind one admitted universe, point-in-time dataset, strategy or model,
            evaluation period, cost policy, cohort plan, and deterministic seed before it can start.
          </p>
        </div>
        <Button
          disabled={!available || mutation.isPending}
          onClick={() => {
            mutation.reset()
            setConfirmationOpen(true)
          }}
        >
          Configure backtest
        </Button>
      </div>
      {!available && (
        <Alert className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Starting a run is unavailable</AlertTitle>
          <AlertDescription>
            The installed service did not advertise the closed Analysis.StartBacktest operation.
          </AlertDescription>
        </Alert>
      )}
      {mutation.isError && (
        <Alert variant="destructive" className="mt-4">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>The backtest was not started</AlertTitle>
          <AlertDescription>{messageFrom(mutation.error)}</AlertDescription>
        </Alert>
      )}
      <Dialog
        open={confirmationOpen}
        onOpenChange={(open) => {
          if (!mutation.isPending) setConfirmationOpen(open)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start a governed backtest?</DialogTitle>
            <DialogDescription>
              Market Squawk will open the protected native file picker. Select a canonical JSON
              registration that binds the admitted universe, point-in-time dataset, strategy or
              model, evaluation period, execution costs, cohort plan, and deterministic seed.
              Exact integer evidence is parsed and validated entirely in Rust.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={mutation.isPending}
              onClick={() => setConfirmationOpen(false)}
            >
              Cancel
            </Button>
            <Button
              disabled={mutation.isPending}
              onClick={() => mutation.mutate()}
            >
              {mutation.isPending ? "Opening secure picker…" : "Choose registration"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

function BacktestEvidence({
  job,
  record,
  loading,
  error,
  canRead,
  onReadReport,
  reportState,
}: {
  job: JobView | null
  record: BacktestRecord | null
  loading: boolean
  error: string | null
  canRead: boolean
  onReadReport: (() => void) | null
  reportState: "loading" | "retrieved" | string | null
}) {
  return (
    <section className="rounded-2xl border border-border bg-card/35 p-5" aria-labelledby="backtest-evidence-heading">
      <h2 id="backtest-evidence-heading" className="text-lg font-semibold">
        Result and evidence
      </h2>
      <p className="mt-1 text-sm text-muted-foreground">
        Historical performance is shown only with its exact data, assumptions, and experiment evidence.
      </p>
      {!job ? (
        <UnavailableCard
          title="No run selected"
          detail="Select a durable backtest job to inspect its current result evidence."
        />
      ) : job.state !== "completed" ? (
        <UnavailableCard
          title="No terminal result"
          detail="This generation has not published a completed governed backtest record. Its progress and failure evidence remain on the job card."
        />
      ) : job.result?.authority !== BACKTEST_RESULT_AUTHORITY ? (
        <UnavailableCard
          title="Unsupported result authority"
          detail="The completed job did not publish the governed backtest result authority expected by this release."
        />
      ) : !canRead ? (
        <UnavailableCard
          title="Result read unavailable"
          detail="The installed service did not advertise the closed Analysis.GetBacktests operation."
        />
      ) : loading ? (
        <LoadingCard label="Loading the governed terminal record…" />
      ) : error ? (
        <UnavailableCard title="Result details unavailable" detail={error} />
      ) : record ? (
        <BacktestRecordView
          record={record}
          onReadReport={onReadReport}
          reportState={reportState}
        />
      ) : (
        <UnavailableCard
          title="Result details unavailable"
          detail="The job published an identity, but no governed record was returned."
        />
      )}
    </section>
  )
}

function BacktestRecordView({
  record,
  onReadReport,
  reportState,
}: {
  record: BacktestRecord
  onReadReport: (() => void) | null
  reportState: "loading" | "retrieved" | string | null
}) {
  const metrics = record.status.state === "completed" ? record.status.metrics : []
  const pbo =
    record.status.state === "completed" && record.status.cohortDiagnostics?.state === "completed"
      ? record.status.cohortDiagnostics.probabilityOfBacktestOverfitting
      : null
  const deflated =
    record.status.state === "completed" && record.status.cohortDiagnostics?.state === "completed"
      ? record.status.cohortDiagnostics.deflatedPerformanceProbability
      : null

  return (
    <div className="mt-5 grid gap-4">
      <div className="grid gap-3 sm:grid-cols-2">
        <EvidenceFact
          icon={Database}
          label="Point-in-time dataset"
          value={shortDigest(record.datasetIdentity)}
          detail={`Object graph ${shortDigest(record.objectGraphDigest)}`}
        />
        <EvidenceFact
          icon={CircleDollarSign}
          label="Execution assumptions"
          value={shortDigest(record.executionAssumptionDigest)}
          detail="Fees, spread, slippage, latency, fills, and liquidity are bound by this evidence—not inferred by the UI."
        />
        <EvidenceFact
          icon={GitCompareArrows}
          label="Cohort authority"
          value={
            record.cohortAuthorityDigest
              ? shortDigest(record.cohortAuthorityDigest)
              : "Not supplied"
          }
          detail={
            record.cohortUniverseDigest
              ? `Universe ${shortDigest(record.cohortUniverseDigest)}`
              : "This record does not contain comparable-cohort evidence."
          }
        />
        <EvidenceFact
          icon={Split}
          label="Selection and seed"
          value={humanize(record.selectionCriterion)}
          detail={`Deterministic seed ${record.seed.toLocaleString()}`}
        />
      </div>

      {record.status.state === "failed" ? (
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Governed run failed</AlertTitle>
          <AlertDescription>
            No performance, fills, or overfitting claim is available from this terminal record.
          </AlertDescription>
        </Alert>
      ) : (
        <CompletedResult
          status={record.status}
          metrics={metrics}
          pbo={pbo}
          deflated={deflated}
          onReadReport={onReadReport}
          reportState={reportState}
        />
      )}
    </div>
  )
}

function CompletedResult({
  status,
  metrics,
  pbo,
  deflated,
  onReadReport,
  reportState,
}: {
  status: Extract<BacktestRecord["status"], { state: "completed" }>
  metrics: readonly BacktestMetric[]
  pbo: number | null
  deflated: number | null
  onReadReport: (() => void) | null
  reportState: "loading" | "retrieved" | string | null
}) {
  return (
    <>
      <div className="grid gap-3 sm:grid-cols-3">
        <ResultFact label="Recorded metrics" value={metrics.length.toLocaleString()} />
        <ResultFact label="Simulated fills" value={status.fillCount.toLocaleString()} />
        <ResultFact
          label="Partial fills"
          value={
            status.partialFillCount === undefined
              ? "Legacy record"
              : status.partialFillCount.toLocaleString()
          }
        />
        <ResultFact label="No-action decisions" value={status.noActionCount.toLocaleString()} />
      </div>
      {metrics.length > 0 ? (
        <div className="rounded-xl border border-border bg-background/25 p-4">
          <h3 className="text-sm font-medium">Published metrics</h3>
          <dl className="mt-3 grid gap-x-5 gap-y-3 sm:grid-cols-2">
            {metrics.map((metric) => (
              <div key={metric.name} className="flex items-baseline justify-between gap-4 border-b border-border/60 pb-2">
                <dt className="text-xs text-muted-foreground">{humanize(metric.name)}</dt>
                <dd className="font-mono text-sm">{formatMetric(metric.value)}</dd>
              </div>
            ))}
          </dl>
        </div>
      ) : (
        <UnavailableCard
          title="Performance metrics unavailable"
          detail="The completed record did not publish bounded summary metrics."
        />
      )}
      <div className="grid gap-3 sm:grid-cols-2">
        <DiagnosticFact
          label="Probability of backtest overfitting"
          value={pbo}
          unavailable="No completed controlled cohort evaluation qualifies this terminal, so PBO is not available."
        />
        <DiagnosticFact
          label="Deflated performance probability"
          value={deflated}
          unavailable="No completed controlled cohort evaluation qualifies this terminal, so no multiple-testing-adjusted probability is available."
        />
      </div>
      <ExecutionAssumptions assumptions={status.executionAssumptions ?? null} />
      <div className="rounded-xl border border-border bg-background/25 p-4 text-xs leading-relaxed text-muted-foreground">
        <p>
          Accounting reconciliation: <span className="font-medium text-foreground">independent</span>.
          The governed report is {status.artifact.byteCount.toLocaleString()} bytes.
        </p>
        {"artifactId" in status.artifact ? (
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <Button size="sm" variant="outline" disabled={reportState === "loading"} onClick={onReadReport ?? undefined}>
              {reportState === "loading" ? "Retrieving report…" : "Retrieve controlled report"}
            </Button>
            <span>
              {reportState === "retrieved"
                ? "The first bounded report segment was verified and retrieved."
                : typeof reportState === "string"
                  ? `Report retrieval failed: ${reportState}`
                  : `ID ${shortDigest(status.artifact.artifactId)} · SHA-256 ${shortDigest(status.artifact.sha256)}`}
            </span>
          </div>
        ) : (
          <p className="mt-2">This legacy terminal has no compatible controlled report identity.</p>
        )}
      </div>
    </>
  )
}

function ExecutionAssumptions({
  assumptions,
}: {
  assumptions: Extract<BacktestRecord["status"], { state: "completed" }>["executionAssumptions"] | null
}) {
  if (!assumptions) {
    return (
      <UnavailableCard
        title="Execution assumptions are legacy-only"
        detail="This terminal binds an assumption digest but predates the readable V2 fee, spread, slippage, latency, participation, liquidity, and partial-fill evidence."
      />
    )
  }
  const facts = [
    ["Fee", `${assumptions.feeBasisPoints} bp`],
    ["Spread", "Observed point-in-time half spread"],
    ["Adverse slippage", `${assumptions.slippageBasisPoints} bp + up to ${assumptions.maximumRandomSlippageBasisPoints} bp seeded`],
    ["Latency", `${assumptions.latencyNanos} ns minimum event-time delay`],
    ["Participation", `${(assumptions.maximumParticipationBasisPoints / 100).toLocaleString(undefined, { maximumFractionDigits: 2 })}% of evidenced depth`],
    ["Liquidity priority", humanize(assumptions.liquidityPriority)],
    ["Partial fills", assumptions.partialFillsAllowed ? "Permitted when depth is insufficient" : "Rejected when depth is insufficient"],
    ["Fee rounding", `Scale ${assumptions.feeDecimalScale}; nearest-even`],
  ] as const
  return (
    <div className="rounded-xl border border-border bg-background/25 p-4">
      <h3 className="text-sm font-medium">Bound execution assumptions</h3>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
        Policy v{assumptions.policyVersion}. These values are immutable run evidence; the UI does not infer costs or liquidity.
      </p>
      <dl className="mt-3 grid gap-x-5 gap-y-3 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <div key={label} className="border-b border-border/60 pb-2">
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd className="mt-1 text-sm">{value}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function SemanticsGuide() {
  const facts = [
    {
      icon: Database,
      title: "Point-in-time first",
      detail: "Historical membership, revisions, delistings, and instrument definitions must be knowable at each decision cutoff.",
    },
    {
      icon: CircleDollarSign,
      title: "Costs stay visible",
      detail: "Fees, spread, slippage, latency, partial fills, and liquidity belong to the immutable assumption evidence.",
    },
    {
      icon: GitCompareArrows,
      title: "Compare one admitted cohort",
      detail: "Variant comparison is meaningful only when candidate membership and selection authority are fixed before evaluation.",
    },
    {
      icon: ShieldCheck,
      title: "Overfitting is evidence",
      detail: "PBO and deflated-performance diagnostics qualify a result; they never promise future returns.",
    },
  ] as const
  return (
    <section className="mt-7" aria-labelledby="backtest-semantics-heading">
      <h2 id="backtest-semantics-heading" className="text-lg font-semibold">How to read a backtest</h2>
      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {facts.map(({ icon: Icon, title, detail }) => (
          <article key={title} className="rounded-xl border border-border bg-card/30 p-4">
            <Icon className="size-4 text-primary" aria-hidden="true" />
            <h3 className="mt-3 text-sm font-medium">{title}</h3>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
          </article>
        ))}
      </div>
    </section>
  )
}

function EvidenceFact({ icon: Icon, label, value, detail }: { icon: typeof Activity; label: string; value: string; detail: string }) {
  return (
    <article className="rounded-xl border border-border bg-background/25 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-sm font-medium">{value}</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
    </article>
  )
}

function ResultFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-border bg-background/25 p-3">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-lg font-semibold">{value}</p>
    </div>
  )
}

function DiagnosticFact({ label, value, unavailable }: { label: string; value: number | null; unavailable: string }) {
  return (
    <div className="rounded-xl border border-border bg-background/25 p-4">
      <div className="flex items-center gap-2">
        <BarChart3 className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-medium">{label}</h3>
      </div>
      {value === null ? (
        <p className="mt-2 text-xs text-muted-foreground">Unavailable: {unavailable}</p>
      ) : (
        <p className="mt-2 font-mono text-xl font-semibold">{formatProbability(value)}</p>
      )}
    </div>
  )
}

function UnavailableCard({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="mt-4 rounded-xl border border-dashed border-border p-5">
      <AlertCircle className="size-4 text-muted-foreground" aria-hidden="true" />
      <h3 className="mt-3 text-sm font-medium">{title}</h3>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function LoadingCard({ label }: { label: string }) {
  return (
    <div className="mt-4 flex items-center gap-3 rounded-xl border border-border p-5 text-sm text-muted-foreground" role="status">
      <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
      {label}
    </div>
  )
}

function ActionDialog({ action, pending, error, onOpenChange, onConfirm }: { action: PendingJobAction | null; pending: boolean; error: string | null; onOpenChange: (open: boolean) => void; onConfirm: () => void }) {
  if (!action) return null
  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{actionTitle(action.kind)}</DialogTitle>
          <DialogDescription>
            This request is fenced to generation {action.job.generation}, sequence {action.job.sequence}. The service rejects it if the job changed.
          </DialogDescription>
        </DialogHeader>
        {action.kind === "confirm" && (
          <dl className="rounded-lg border border-border bg-card/40 p-4 text-xs">
            <dt className="text-muted-foreground">Exact confirmation evidence</dt>
            <dd className="mt-1 break-all font-mono text-[10px]">sha256:{digestHex(action.confirmation.digest)}</dd>
            <dt className="mt-3 text-muted-foreground">Expires</dt>
            <dd className="mt-1">{formatTimestamp(BigInt(action.confirmation.expiresAt))}</dd>
          </dl>
        )}
        {error && (
          <Alert variant="destructive">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>The job did not change</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button variant="outline" disabled={pending} onClick={() => onOpenChange(false)}>Keep current state</Button>
          <Button variant={action.kind === "cancel" ? "destructive" : "default"} disabled={pending} onClick={onConfirm}>
            {pending && <LoaderCircle className="animate-spin" aria-hidden="true" />}
            {pending ? "Submitting" : actionButtonLabel(action.kind)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function BacktestsFrame({ children }: { children: React.ReactNode }) {
  return (
    <main className="mx-auto w-full max-w-[1280px] p-5 lg:p-7">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">Research validation</p>
      <div className="mt-2 flex items-start gap-3">
        <FlaskConical className="mt-1 size-6 text-primary" aria-hidden="true" />
        <div>
          <h1 className="text-3xl font-semibold tracking-tight">Backtests</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Test admitted investment rules against point-in-time history, declared trading costs, and explicit experiment governance. Historical results are evidence—not a forecast or guarantee.
          </p>
        </div>
      </div>
      <div className="mt-6">{children}</div>
    </main>
  )
}

function controlRequest(action: PendingJobAction): JobControlRequest {
  const common = { jobId: action.job.jobId, generation: action.job.generation, expectedSequence: action.job.sequence }
  return action.kind === "confirm"
    ? { action: "confirm", ...common, identity: action.confirmation.identity, digest: digestHex(action.confirmation.digest) }
    : { action: action.kind, ...common }
}

function jobKey(job: JobView | null): string {
  return job ? `${job.jobId}:${job.generation}` : ""
}

function shortDigest(value: string): string {
  return `${value.slice(0, 10)}…${value.slice(-8)}`
}

function formatMetric(value: number): string {
  return Math.abs(value) >= 1_000 ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 6 })
}

function formatProbability(value: number): string {
  return value >= 0 && value <= 1
    ? value.toLocaleString(undefined, { style: "percent", maximumFractionDigits: 1 })
    : formatMetric(value)
}

function actionTitle(kind: PendingJobAction["kind"]): string {
  return kind === "cancel" ? "Cancel this backtest?" : kind === "retry" ? "Retry this backtest?" : "Confirm this backtest request?"
}

function actionButtonLabel(kind: PendingJobAction["kind"]): string {
  return kind === "cancel" ? "Request cancellation" : kind === "retry" ? "Start retry" : "Confirm exact request"
}
