import { RefreshButton } from "@/components/ui/refresh-button"
import * as React from "react"
import {
  Activity,
  CheckCircle2,
  CircleAlert,
  LoaderCircle,
  ShieldAlert,
} from "lucide-react"
import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query"

import { messageFrom, useSystem } from "@/app/product-context"
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
import { humanize } from "@/lib/formatters"
import { type LosslessInteger } from "@/lib/lossless-integer"
import { formatTimestamp } from "@/lib/time"
import type { JobControlRequest, SystemTransport } from "@/lib/transport"

import {
  digestHex,
  isActiveJob,
  parseJobPage,
  parseRuntimeStatus,
  type PendingJobAction,
} from "./contracts"
import { JobCard } from "./job-card"
import { jobKindLabel } from "./job-presentation"
import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"
import {
  EmptyJobs,
  LoadingState,
  SummaryFact,
  OperationalHealth,
} from "./operations-status"

const JOB_PAGE_LIMIT = 50

export function OperationsPage() {
  const system = useSystem()

  if (system.status !== "ready") {
    return (
      <OperationsFrame>
        <Alert>
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Operations are unavailable</AlertTitle>
          <AlertDescription>
            Restore the connection to the installed Market Squawk service, then
            retry this page.
          </AlertDescription>
        </Alert>
      </OperationsFrame>
    )
  }

  return (
    <ReadyOperations
      transport={system.transport}
      scope={system.bootstrap.productSessionToken}
    />
  )
}

function ReadyOperations({
  transport,
  scope,
}: {
  transport: SystemTransport
  scope: ProductScope
}) {
  const queryClient = useQueryClient()
  const [pendingAction, setPendingAction] =
    React.useState<PendingJobAction | null>(null)
  const [announcement, setAnnouncement] = React.useState("")
  const navigation = useCursorNavigation()
  const jobsQueryKey = productKeys.operation(scope, "job", "Job.List", {
    limit: JOB_PAGE_LIMIT,
    afterJobId: navigation.after,
  })
  const jobsQuery = useQuery({
    queryKey: jobsQueryKey,
    gcTime: 0,
    queryFn: ({ signal }) => transport.systemQuery({
      query: "jobs", afterJobId: navigation.after, limit: JOB_PAGE_LIMIT,
    }, { signal }),
    select: parseJobPage,
    refetchInterval: (query) => {
      if (navigation.after !== undefined || !query.state.data) return false
      try { return parseJobPage(query.state.data).jobs.some((job) => isActiveJob(job.state)) ? 5_000 : false }
      catch { return false }
    },
    refetchIntervalInBackground: false,
  })
  const runtimeQuery = useQuery({
    queryKey: productKeys.operation(
      scope,
      "operations",
      "Operations.GetRuntimeStatus",
      {},
    ),
    queryFn: async ({ signal }) =>
      parseRuntimeStatus(
        await transport.systemQuery({ query: "operationRuntimeStatus" }, { signal }),
      ),
    refetchInterval: (query) => query.state.data && (query.state.data.runningJobs > 0 || query.state.data.activeSources > 0 || query.state.data.paperExecutionActive || query.state.data.executionReconciliationPending) ? 5_000 : false,
    refetchIntervalInBackground: false,
  })
  const mutation = useMutation({
    mutationFn: (action: PendingJobAction) =>
      transport.jobControl(controlRequest(action), true),
    onSuccess: async (_result, action) => {
      setAnnouncement(`${actionLabel(action.kind)} accepted by the service.`)
      setPendingAction(null)
      await queryClient.invalidateQueries({
        queryKey: productKeys.domain(scope, "job"),
      })
    },
  })

  const jobs = jobsQuery.data?.jobs ?? []
  const active = jobs.filter((job) => isActiveJob(job.state)).length
  const attention = jobs.filter((job) =>
    ["awaiting_confirmation", "failed", "interrupted"].includes(job.state),
  ).length
  const completed = jobs.filter((job) => job.state === "completed").length

  return (
    <OperationsFrame>
      <p className="sr-only" aria-live="polite">
        {announcement}
      </p>

      <div className="grid gap-3 sm:grid-cols-3">
        <SummaryFact
          icon={Activity}
          label="Active in this view"
          value={active}
          detail="Queued, running, recovering, or awaiting your decision."
        />
        <SummaryFact
          icon={ShieldAlert}
          label="Needs attention"
          value={attention}
          detail="Jobs needing confirmation, retry, or recovery."
        />
        <SummaryFact
          icon={CheckCircle2}
          label="Completed in this view"
          value={completed}
          detail="Results have been saved."
        />
      </div>

      <section className="mt-6" aria-labelledby="jobs-heading">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h2 id="jobs-heading" className="text-lg font-semibold">
              Jobs
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Follow jobs that continue after you close the app.
              Up to {JOB_PAGE_LIMIT} jobs are shown per page.
            </p>
          </div>
          <RefreshButton label="Refresh" refreshing={jobsQuery.isFetching} onClick={() => void jobsQuery.refetch()}
            disabled={jobsQuery.isFetching} />
        </div>

        {jobsQuery.isPending ? (
          <LoadingState />
        ) : jobsQuery.isError ? (
          <Alert variant="destructive" className="mt-4">
            <CircleAlert aria-hidden="true" />
            <AlertTitle>Jobs could not be loaded</AlertTitle>
            <AlertDescription>
              {messageFrom(jobsQuery.error)}
              <Button
                variant="outline"
                size="sm"
                className="mt-2"
                onClick={() => void jobsQuery.refetch()}
              >
                Retry
              </Button>
            </AlertDescription>
          </Alert>
        ) : jobs.length === 0 ? (
          <EmptyJobs />
        ) : (
          <div className="mt-4 grid gap-3">
            {jobs.map((job) => (
              <JobCard
                key={`${job.jobId}:${job.generation}`}
                job={job}
                transport={transport}
                scope={scope}
                mutationPending={mutation.isPending}
                onAction={(action) => {
                  mutation.reset()
                  setPendingAction(action)
                }}
              />
            ))}
          </div>
        )}

        <CursorNavigation
          navigation={navigation}
          next={jobsQuery.data?.next}
          busy={jobsQuery.isFetching} error={jobsQuery.isError}
          onRestart={() => { if (navigation.after === undefined) void jobsQuery.refetch() }}
        />
      </section>

      <OperationalHealth
        status={runtimeQuery.data}
        pending={runtimeQuery.isPending}
        refreshing={runtimeQuery.isFetching}
        error={runtimeQuery.isError ? messageFrom(runtimeQuery.error) : null}
        onRefresh={() => void runtimeQuery.refetch()}
      />

      {mutation.isError && (
        <Alert variant="destructive" className="mt-5">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>The job did not change</AlertTitle>
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
    </OperationsFrame>
  )
}

function ActionDialog({
  action,
  pending,
  error,
  onOpenChange,
  onConfirm,
}: {
  action: PendingJobAction | null
  pending: boolean
  error: string | null
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  if (!action) return null
  const isCancel = action.kind === "cancel"

  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{actionTitle(action.kind)}</DialogTitle>
          <p className="text-sm font-medium">{jobKindLabel(action.job.kind)}</p>
          <DialogDescription>
            {action.kind === "cancel"
              ? "Request cancellation. The job stops when it reaches a safe stopping point."
              : action.kind === "retry"
                ? "Run this job again using its original inputs."
                : "Approve the pending action for this job. Refresh and review again if the job has changed."}
          </DialogDescription>
        </DialogHeader>
        {action.kind === "confirm" && (
          <div className="rounded-lg border border-border bg-card/40 p-4 text-xs">
            <dl className="grid gap-3">
              <div>
                <dt className="text-muted-foreground">Pending action</dt>
                <dd className="mt-1 break-all font-medium">{humanize(action.confirmation.identity)}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Expires</dt>
                <dd className="mt-1">{formatJobTime(action.confirmation.expiresAt)}</dd>
              </div>
            </dl>
            <details className="mt-3">
              <summary className="cursor-pointer text-muted-foreground">Confirmation diagnostics</summary>
              <dl className="mt-3 grid gap-3">
              <div>
                <dt className="text-muted-foreground">Evidence digest</dt>
                <dd className="mt-1 break-all font-mono text-[10px]">
                  sha256:{digestHex(action.confirmation.digest)}
                </dd>
              </div>
                <div><dt className="text-muted-foreground">Confirmation reference</dt><dd className="mt-1 break-all font-mono">{action.confirmation.identity}</dd></div>
              </dl>
            </details>
          </div>
        )}
        {error && (
          <Alert variant="destructive">
            <CircleAlert aria-hidden="true" />
            <AlertTitle>The job did not change</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={pending}
          >
            Go back
          </Button>
          <Button
            variant={isCancel ? "destructive" : "default"}
            onClick={onConfirm}
            disabled={pending}
          >
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : null}
            {pending ? "Submitting" : actionButtonLabel(action.kind)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function OperationsFrame({ children }: { children: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1120px] p-5 lg:p-7">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
        Market Squawk
      </p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">Operations</h1>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
        Follow background jobs, review results, and manage jobs that need attention.
      </p>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function controlRequest(action: PendingJobAction): JobControlRequest {
  const common = {
    jobId: action.job.jobId,
    generation: action.job.generation,
    expectedSequence: action.job.sequence,
  }
  if (action.kind === "confirm") {
    return {
      action: "confirm",
      ...common,
      identity: action.confirmation.identity,
      digest: digestHex(action.confirmation.digest),
    }
  }
  return { action: action.kind, ...common }
}

function formatJobTime(value: LosslessInteger): string {
  return formatTimestamp(value)
}

function actionTitle(kind: PendingJobAction["kind"]): string {
  switch (kind) {
    case "cancel":
      return "Cancel this job?"
    case "confirm":
      return "Confirm this job action?"
    case "retry":
      return "Retry this job?"
  }
}

function actionButtonLabel(kind: PendingJobAction["kind"]): string {
  switch (kind) {
    case "cancel":
      return "Request cancellation"
    case "confirm":
      return "Confirm action"
    case "retry":
      return "Start retry"
  }
}

function actionLabel(kind: PendingJobAction["kind"]): string {
  switch (kind) {
    case "cancel":
      return "Cancellation"
    case "confirm":
      return "Confirmation"
    case "retry":
      return "Retry"
  }
}
