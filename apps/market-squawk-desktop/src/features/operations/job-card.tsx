import { jobFailureLabel, jobKindLabel, jobPhaseLabel, jobStateLabel } from "./job-presentation"
import { DemandPanel } from "../shared/demand-panel"
import {
  CheckCircle2,
  FileText,
  RotateCcw,
  Square,
} from "lucide-react"
import { useQuery } from "@tanstack/react-query"

import { messageFrom } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { formatTimestamp } from "@/lib/time"
import type { LosslessInteger } from "@/lib/lossless-integer"
import type { SystemTransport } from "@/lib/transport"
import { cn } from "@/lib/utils"

import {
  canCancel,
  canRetry,
  parseArtifactChunk,
  parseCurrentConfirmation,
  previewableMediaType,
  type JobArtifact,
  type JobState,
  type JobView,
  type PendingJobAction,
} from "./contracts"

const ARTIFACT_PREVIEW_BYTES = 64 * 1024
const ARTIFACT_CHUNK_BYTES = 32 * 1024

export function JobCard({
  job,
  transport,
  scope,
  mutationPending,
  onAction,
  presentation = "operations",
}: {
  job: JobView
  transport: SystemTransport
  scope: ProductScope
  mutationPending: boolean
  onAction: (action: PendingJobAction) => void
  presentation?: "operations" | "product"
}) {
  const productPresentation = presentation === "product"
  const currentSequence = BigInt(job.sequence)
  const afterSequence = (
    currentSequence > 0n ? currentSequence - 1n : 0n
  ).toString()
  const confirmationQuery = useQuery({
    queryKey: productKeys.operation(scope, "job", "Job.Watch", {
      jobId: job.jobId,
      generation: job.generation,
      afterSequence,
      limit: 1,
    }),
    enabled: job.state === "awaiting_confirmation",
    queryFn: async () =>
      parseCurrentConfirmation(
        await transport.jobControl({
          action: "watch",
          jobId: job.jobId,
          generation: job.generation,
          afterSequence,
          limit: 1,
        }),
        job.sequence,
      ),
    staleTime: Infinity,
  })
  const confirmation = confirmationQuery.data ?? null
  const confirmationExpired =
    confirmation !== null &&
    BigInt(confirmation.expiresAt) <= BigInt(Date.now()) * 1_000_000n

  return (
    <article className="rounded-xl border border-border bg-card/45 p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <StateBadge state={job.state} />
          </div>
          <h3 className="mt-2 truncate text-sm font-semibold" title={jobKindLabel(job.kind)}>
            {jobKindLabel(job.kind)}
          </h3>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Updated {formatJobTime(job.updatedAt)}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {canRetry(job) && (
            <Button
              size="sm"
              variant="outline"
              disabled={mutationPending}
              onClick={() => onAction({ kind: "retry", job })}
            >
              <RotateCcw aria-hidden="true" />
              Retry
            </Button>
          )}
          {job.state === "awaiting_confirmation" && (
            <Button
              size="sm"
              disabled={!confirmation || confirmationExpired || mutationPending}
              onClick={() => {
                if (confirmation) onAction({ kind: "confirm", job, confirmation })
              }}
            >
              <CheckCircle2 aria-hidden="true" />
              {confirmationQuery.isPending
                ? "Checking confirmation"
                : confirmationExpired
                  ? "Confirmation expired"
                  : "Review"}
            </Button>
          )}
          {canCancel(job) && (
            <Button
              size="sm"
              variant="destructive"
              disabled={mutationPending}
              onClick={() => onAction({ kind: "cancel", job })}
            >
              <Square aria-hidden="true" />
              Cancel
            </Button>
          )}
        </div>
      </div>

      <JobProgress job={job} />
      {!productPresentation ? (
        <details className="mt-3 rounded-lg border border-border/70 p-3 text-xs">
          <summary className="cursor-pointer text-muted-foreground">Job diagnostics</summary>
          <dl className="mt-3 grid gap-2">
            <div><dt>Job ID</dt><dd className="break-all font-mono">{job.jobId}</dd></div>
            <div><dt>Job kind</dt><dd className="break-all font-mono">{job.kind}</dd></div>
            <div><dt>Generation / sequence</dt><dd>{job.generation} / {job.sequence}</dd></div>
            {job.phase ? <div><dt>Phase</dt><dd className="break-all font-mono">{job.phase}</dd></div> : null}
            {job.failure ? <div><dt>Failure</dt><dd className="break-all font-mono">{job.failure.class} · {job.failure.diagnostic}</dd></div> : null}
            {job.recovery ? <div><dt>Recovery</dt><dd className="break-all font-mono">{job.recovery}</dd></div> : null}
            {job.result ? <div><dt>Result</dt><dd className="break-all font-mono">{job.result.authority} · {job.result.identity}</dd></div> : null}
            {confirmationQuery.isError ? <div><dt>Confirmation</dt><dd>{messageFrom(confirmationQuery.error)}</dd></div> : null}
          </dl>
        </details>
      ) : null}

      {job.cancellationRequested && (
        <p className="mt-3 text-xs text-amber-300">
          Cancellation is in progress.
        </p>
      )}
      {job.failure && (
        <div className="mt-3 rounded-lg border border-destructive/35 bg-destructive/10 p-3">
          <p className="text-xs font-medium text-destructive">
            {jobFailureLabel(job.failure.class)}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            {job.failure.retryable ? "You can retry this job." : "Review Logs & Diagnostics for details."}
          </p>
        </div>
      )}
      {job.recovery && (
        <div className="mt-3 rounded-lg border border-amber-400/30 bg-amber-400/5 p-3">
          <p className="text-xs font-medium text-amber-300">Recovery status</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {job.recovery === "interrupted-requires-explicit-retry"
              ? "This job was interrupted. Review the available actions before continuing."
              : "Recovery is in progress. Available actions appear when the job is ready."}
          </p>
        </div>
      )}
      {job.result && (
        <div className="mt-3 rounded-lg border border-emerald-400/25 bg-emerald-400/5 p-3">
          <p className="text-xs font-medium text-emerald-300">Results ready</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {job.result.artifacts.length > 0
              ? `${job.result.artifacts.length} result file${job.result.artifacts.length === 1 ? " is" : "s are"} available below.`
              : "The job completed and its result was saved."}
          </p>
          {!productPresentation && job.result.artifacts.length > 0 && (
            <div className="mt-3 grid gap-3">
              {job.result.artifacts.map((artifact) => (
                <JobArtifactPreview
                  key={`${artifact.id}:${artifact.sha256}`}
                  artifact={artifact}
                  transport={transport}
                  scope={scope}
                />
              ))}
            </div>
          )}
        </div>
      )}
      {confirmationQuery.isError && job.state === "awaiting_confirmation" && (
        <p className="mt-3 text-xs text-destructive">
          Confirmation could not be checked. Try again or review Logs & Diagnostics.
        </p>
      )}
      {confirmationQuery.isSuccess &&
        !confirmation &&
        job.state === "awaiting_confirmation" && (
          <p className="mt-3 text-xs text-destructive">
            Confirmation is unavailable for the current job status.
          </p>
        )}
    </article>
  )
}

function JobArtifactPreview({
  artifact,
  transport,
  scope,
}: {
  artifact: JobArtifact
  transport: SystemTransport
  scope: ProductScope
}) {
  const mediaType = previewableMediaType(artifact)
  return <section className="rounded-md border border-border/70 bg-background/25 p-3">
    <p className="flex items-center gap-2 text-xs font-medium"><FileText className="size-3.5" aria-hidden="true" />Result file</p>
    <p className="mt-1 break-all font-mono text-[10px] text-muted-foreground">{artifact.id} · sha256:{artifact.sha256} · {artifact.byteCount.toLocaleString()} bytes</p>
    {mediaType ? <DemandPanel title="View preview" className="mt-3">
      <JobArtifactPreviewRead artifact={artifact} transport={transport} scope={scope} />
    </DemandPanel> : <p className="mt-2 text-xs text-muted-foreground">Viewing is unavailable: this dashboard can safely render only JSON or NDJSON artifact previews.</p>}
  </section>
}

function JobArtifactPreviewRead({
  artifact,
  transport,
  scope,
}: {
  artifact: JobArtifact
  transport: SystemTransport
  scope: ProductScope
}) {
  const previewBytes = Math.min(artifact.byteCount, ARTIFACT_PREVIEW_BYTES)
  const mediaType = previewableMediaType(artifact)
  const previewQuery = useQuery({
    queryKey: productKeys.operation(scope, "analysis", "Analysis.ReadArtifact", {
      artifactId: artifact.id,
      sha256: artifact.sha256,
      byteCount: artifact.byteCount,
      mediaType: artifact.mediaType,
      maximumBytes: previewBytes,
    }),
    gcTime: 0,
    queryFn: async ({ signal }) => {
      if (!mediaType) {
        throw new Error("This artifact does not have a previewable media type.")
      }
      const firstMaximum = Math.min(previewBytes, ARTIFACT_CHUNK_BYTES)
      const first = parseArtifactChunk(
        await transport.systemQuery({
          query: "analysisArtifact",
          artifactId: artifact.id,
          sha256: artifact.sha256,
          byteCount: artifact.byteCount,
          mediaType,
          offset: 0,
          maximumBytes: firstMaximum,
        }, { signal }),
        artifact,
        0,
        firstMaximum,
      )
      if (first.complete || first.returnedBytes >= previewBytes) {
        return {
          chunksBase64: [first.contentBase64],
          returnedBytes: first.returnedBytes,
          complete: first.complete,
        }
      }
      if (first.returnedBytes === 0) {
        throw new Error("The service returned an empty non-terminal artifact chunk.")
      }
      const secondMaximum = Math.min(
        previewBytes - first.returnedBytes,
        ARTIFACT_CHUNK_BYTES,
      )
      const second = parseArtifactChunk(
        await transport.systemQuery({
          query: "analysisArtifact",
          artifactId: artifact.id,
          sha256: artifact.sha256,
          byteCount: artifact.byteCount,
          mediaType,
          offset: first.nextOffset,
          maximumBytes: secondMaximum,
        }, { signal }),
        artifact,
        first.nextOffset,
        secondMaximum,
      )
      return {
        chunksBase64: [first.contentBase64, second.contentBase64],
        returnedBytes: first.returnedBytes + second.returnedBytes,
        complete: second.complete,
      }
    },
  })
  const preview = previewQuery.data
  const content = preview ? decodeUtf8(preview.chunksBase64) : null

  return (
    <div>
      <Button size="sm" variant="outline" disabled={previewQuery.isFetching} onClick={() => void previewQuery.refetch()}>
        {previewQuery.isFetching ? "Retrieving preview…" : "Refresh preview"}
      </Button>
      {previewQuery.isError && (
        <p className="mt-2 text-xs text-destructive">
          The preview could not be retrieved: {messageFrom(previewQuery.error)}
        </p>
      )}
      {preview && content !== null && (
        <div className="mt-3">
          <p className="text-[11px] text-muted-foreground">
            {preview.complete
              ? `Verified complete artifact (${preview.returnedBytes.toLocaleString()} bytes).`
              : `Verified first ${preview.returnedBytes.toLocaleString()} bytes; the remaining artifact is not loaded into the dashboard.`}
          </p>
          <pre className="mt-2 max-h-64 overflow-auto rounded border border-border bg-background p-3 text-xs leading-relaxed text-foreground">
            {content}
          </pre>
        </div>
      )}
    </div>
  )
}

function decodeUtf8(chunksBase64: string[]): string {
  try {
    const chunks = chunksBase64.map((contentBase64) => {
      const binary = atob(contentBase64)
      return Uint8Array.from(binary, (character) => character.charCodeAt(0))
    })
    const byteCount = chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
    const bytes = new Uint8Array(byteCount)
    let offset = 0
    for (const chunk of chunks) {
      bytes.set(chunk, offset)
      offset += chunk.byteLength
    }
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes)
  } catch {
    return "This result file cannot be displayed as text."
  }
}

function JobProgress({ job }: { job: JobView }) {
  if (!job.phase) return null

  const total = job.totalUnits
  const completed = job.completedUnits
  const percent =
    total !== null && total > 0 && completed !== null
      ? Math.min(100, (completed / total) * 100)
      : null

  return (
    <div className="mt-4">
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium">{jobPhaseLabel(job.phase)}</span>
        {total !== null && completed !== null ? <span className="font-mono text-muted-foreground">
          {completed.toLocaleString()} / {total.toLocaleString()}
        </span> : null}
      </div>
      {percent !== null ? (
        <Progress
          className="mt-2"
          value={percent}
          aria-label={`${jobPhaseLabel(job.phase)} progress`}
        />
      ) : null}
    </div>
  )
}

function StateBadge({ state }: { state: JobState }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider",
        stateTone(state),
      )}
    >
      {jobStateLabel(state)}
    </span>
  )
}

function stateTone(state: JobState): string {
  switch (state) {
    case "completed":
      return "border-emerald-400/30 bg-emerald-400/10 text-emerald-300"
    case "failed":
      return "border-destructive/40 bg-destructive/10 text-destructive"
    case "awaiting_confirmation":
    case "interrupted":
      return "border-amber-400/35 bg-amber-400/10 text-amber-300"
    case "cancelled":
      return "border-border bg-muted text-muted-foreground"
    default:
      return "border-primary/30 bg-primary/10 text-blue-300"
  }
}

function formatJobTime(value: LosslessInteger): string {
  return formatTimestamp(value)
}
