import { Activity, AlertTriangle, CheckCircle2, Clock3 } from "lucide-react"

import { humanize } from "@/lib/formatters"

import type { ModelJob } from "./models-contracts"

export function ModelJobActivity({
  jobs,
  available,
  loading,
  error,
}: {
  jobs: ModelJob[]
  available: boolean
  loading: boolean
  error: string | null
}) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-center gap-3">
        <Activity className="size-4 text-primary" aria-hidden="true" />
        <div>
          <h2 className="text-sm font-semibold">Durable model activity</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Recent training, evaluation, and forecast activity.
          </p>
        </div>
      </div>
      {!available ? (
        <Message text="Model activity is unavailable in this workspace." />
      ) : loading ? (
        <Message text="Loading model jobs…" />
      ) : error ? (
        <Message text={error} />
      ) : jobs.length === 0 ? (
        <Message text="No durable model or training job has been retained." />
      ) : (
        <ul className="mt-4 grid gap-2">
          {jobs.map((job) => {
            const progress =
              job.completedUnits !== null && job.totalUnits !== null && job.totalUnits > 0
                ? Math.min(100, (job.completedUnits / job.totalUnits) * 100)
                : null
            const Icon = job.failure
              ? AlertTriangle
              : job.state === "completed"
                ? CheckCircle2
                : Clock3
            return (
              <li key={`${job.jobId}:${job.generation}`} className="rounded-lg border border-border bg-background/25 p-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="flex min-w-0 items-start gap-2.5">
                    <Icon className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                    <div className="min-w-0">
                      <p className="truncate text-xs font-medium">{humanize(job.kind)}</p>
                      <p className="mt-1 font-mono text-[10px] text-muted-foreground">
                        Job {job.jobId}
                      </p>
                    </div>
                  </div>
                  <span className="rounded-full border border-border px-2 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                    {humanize(job.state)}
                  </span>
                </div>
                {job.phase ? <p className="mt-2 text-xs text-muted-foreground">Phase: {humanize(job.phase)}</p> : null}
                {progress !== null ? (
                  <div className="mt-2" aria-label={`${progress.toFixed(0)} percent complete`}>
                    <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                      <div className="h-full bg-primary" style={{ width: `${progress}%` }} />
                    </div>
                    <p className="mt-1 text-[10px] text-muted-foreground">
                      {job.completedUnits?.toLocaleString()} / {job.totalUnits?.toLocaleString()} units
                    </p>
                  </div>
                ) : null}
                {job.failure ? (
                  <p className="mt-2 text-xs leading-5 text-red-300">
                    {humanize(job.failure.class)}: {job.failure.diagnostic}
                    {job.failure.retryable ? " · retryable" : " · not retryable"}
                  </p>
                ) : null}
                {job.recovery ? <p className="mt-2 text-xs text-amber-200">Recovery: {job.recovery}</p> : null}
              </li>
            )
          })}
        </ul>
      )}
      <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
        Cancellation, retry, and exact confirmation remain centralized in Operations, where the
        the latest job state is checked before the change is applied.
      </p>
    </section>
  )
}

function Message({ text }: { text: string }) {
  return <p className="mt-4 text-sm leading-6 text-muted-foreground">{text}</p>
}
