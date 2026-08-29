import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  AlertCircle,
  Clock3,
  Download,
  LoaderCircle,
} from "lucide-react"

import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { friendlyResearchCollectionName } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  parseResearchJobReceipt,
  parseResearchManifest,
  parseResearchObservations,
  type ResearchDataset,
  type ResearchObservationResult,
} from "./research-contracts"

export function DatasetEvidence({
  dataset,
  bootstrap,
  transport,
}: {
  dataset: ResearchDataset
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const datasetId = dataset.manifest.datasetId
  const detailKey = [
    ...productKeys.domain(bootstrap.runtime, "research"),
    "dataset",
    datasetId,
  ] as const
  const manifest = useQuery({
    queryKey: [...detailKey, "manifest"],
    queryFn: async () =>
      parseResearchManifest(
        await transport.query({ query: "researchManifest", dataset: datasetId }),
        datasetId,
      ),
  })
  const history = useQuery({
    queryKey: [...detailKey, "history"],
    queryFn: async () =>
      parseResearchObservations(
        await transport.query({ query: "researchHistory", dataset: datasetId }),
        datasetId,
      ),
  })
  const alternative = useQuery({
    queryKey: [...detailKey, "alternative-data"],
    queryFn: async () =>
      parseResearchObservations(
        await transport.query({
          query: "researchAlternativeData",
          dataset: datasetId,
        }),
        datasetId,
      ),
  })
  const canExport = bootstrap.operations.some(
    (operation) => operation.name === "Research.StartExport",
  )
  const exportJob = useMutation({
    mutationFn: async () =>
      parseResearchJobReceipt(
        await transport.researchControl(
          { action: "startExport", dataset: datasetId },
          true,
        ),
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: productKeys.domain(bootstrap.runtime, "job"),
      }),
  })
  const exact = manifest.data ?? dataset

  return (
    <article className="rounded-xl border border-border bg-card/45">
      <div className="border-b border-border p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
              Selected collection
            </p>
            <h2 className="mt-2 break-words text-xl font-semibold">
              {friendlyResearchCollectionName(exact.manifest.schema.name)}
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {canExport ? (
              <Button
                size="sm"
                variant="outline"
                onClick={() => exportJob.mutate()}
                disabled={exportJob.isPending}
              >
                {exportJob.isPending ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <Download aria-hidden="true" />
                )}
                Export history
              </Button>
            ) : null}
          </div>
        </div>
        <p className="mt-3 text-sm text-muted-foreground">
          Dated research information available for repeatable analysis.
        </p>
        {exportJob.data ? (
          <p className="mt-3 text-xs text-[var(--success)]" role="status">
            Export started. Follow its progress in Operations &amp; Jobs.
          </p>
        ) : null}
        {exportJob.isError ? (
          <p className="mt-3 text-xs text-destructive" role="alert">
            The export could not be started. Try again.
          </p>
        ) : null}
      </div>

      {manifest.isError ? (
        <InlineError
          title="This collection could not be checked"
          retry={() => void manifest.refetch()}
        />
      ) : null}
      <div className="grid gap-px bg-border sm:grid-cols-2">
        <EvidenceMetric label="Observations" value={formatCount(exact.rowCount)} />
        <EvidenceMetric label="Availability" value="Ready to review" />
      </div>

      <div className="p-5">
        <EvidenceBlock
          icon={Clock3}
          title="History and revisions"
          description="When the information applied, when it became public, and whether it later changed."
        >
          <HistoryEvidence
            query={{
              data: history.data,
              isPending: history.isPending,
              isError: history.isError,
              retry: () => void history.refetch(),
            }}
          />
          <AlternativeEvidence
            result={alternative.data}
            loading={alternative.isPending}
            error={alternative.error}
            retry={() => void alternative.refetch()}
          />
        </EvidenceBlock>
      </div>
    </article>
  )
}

function HistoryEvidence({
  query,
}: {
  query: {
    data: ResearchObservationResult | undefined
    isPending: boolean
    isError: boolean
    retry: () => void
  }
}) {
  if (query.isPending) return <Skeleton className="h-48 rounded-lg" />
  if (query.isError) {
    return (
      <InlineError title="History could not be loaded" retry={query.retry} />
    )
  }
  if (!query.data || query.data.kind === "empty") {
    return (
      <p className="rounded-md border border-border bg-background/40 p-3 text-xs leading-5 text-muted-foreground">
        This collection contains no matching history.
      </p>
    )
  }
  if (query.data.kind === "artifact") {
    return (
      <div className="rounded-md border border-border bg-background/40 p-3">
        <p className="text-xs font-medium">
          {formatCount(query.data.artifact.rowCount)} observations are available
        </p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          This is more information than the page can show at once. Export the history to review
          the full collection.
        </p>
      </div>
    )
  }
  return (
    <div className="space-y-2">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {formatCount(query.data.returnedItems)} observations returned
      </p>
      <ul className="max-h-80 space-y-2 overflow-y-auto pr-1">
        {query.data.rows.slice(0, 8).map((row, index) => (
          <ObservationEvidence key={index} row={row} />
        ))}
      </ul>
      {query.data.rows.length > 8 ? (
        <p className="text-[10px] text-muted-foreground">
          Showing 8 of {formatCount(query.data.rows.length)} inline rows.
        </p>
      ) : null}
    </div>
  )
}

function ObservationEvidence({ row }: { row: Record<string, unknown> }) {
  const revision = numberValue(row.revision)
  const quality = textValue(row.quality)
  return (
    <li className="rounded-md border border-border bg-background/45 p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="text-xs font-medium">Observation</p>
        </div>
        <EvidenceBadge>Revision {revision ?? "unavailable"}</EvidenceBadge>
      </div>
      <dl className="mt-3 grid gap-2 text-[10px] sm:grid-cols-2">
        <TimeFact label="Effective" value={row.effective_at ?? row.effective_date} />
        <TimeFact label="First public" value={row.published_at ?? row.published_date} />
        <TimeFact label="Available" value={row.available_at} />
        <TimeFact label="Superseded" value={row.superseded_at ?? row.superseded_date} />
        <div>
          <dt className="text-muted-foreground">Quality</dt>
          <dd className="mt-0.5 text-foreground">{qualityLabel(quality)}</dd>
        </div>
      </dl>
    </li>
  )
}

function AlternativeEvidence({
  result,
  loading,
  error,
  retry,
}: {
  result: ResearchObservationResult | undefined
  loading: boolean
  error: unknown
  retry: () => void
}) {
  if (loading) return <Skeleton className="h-12 rounded-lg" />
  if (error) {
    return (
      <InlineError
        title="Additional information could not be loaded"
        retry={retry}
      />
    )
  }
  const value = !result || result.kind === "empty"
    ? "No additional information in this collection"
    : result.kind === "artifact"
      ? `${formatCount(result.artifact.rowCount)} additional observations available`
      : `${formatCount(result.returnedItems)} additional observations available`
  return (
    <div className="rounded-md border border-border bg-background/40 p-3">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">
        Additional information
      </p>
      <p className="mt-1 text-xs text-foreground">{value}</p>
    </div>
  )
}

function TimeFact({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 break-words text-foreground">{temporalValue(value)}</dd>
    </div>
  )
}

function InlineError({
  title,
  retry,
}: {
  title: string
  retry: () => void
}) {
  return (
    <Alert variant="destructive" className="my-3">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <p>Refresh this collection to try again.</p>
        <Button size="xs" variant="outline" onClick={retry}>
          Try again
        </Button>
      </AlertDescription>
    </Alert>
  )
}

function EvidenceBlock({
  icon: Icon,
  title,
  description,
  children,
}: {
  icon: typeof Clock3
  title: string
  description: string
  children: React.ReactNode
}) {
  return (
    <section>
      <div className="flex items-start gap-3">
        <Icon className="mt-0.5 size-4 text-primary" aria-hidden="true" />
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">{description}</p>
        </div>
      </div>
      <div className="mt-4 space-y-3">{children}</div>
    </section>
  )
}

function EvidenceMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 bg-card/55 p-4">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-2 truncate text-xs font-medium" title={value}>{value}</p>
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

function textValue(value: unknown) {
  return typeof value === "string" && value.length ? value : null
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null
}

function temporalValue(value: unknown) {
  if (typeof value === "string" && value.length) return value
  if (typeof value === "number" && Number.isFinite(value)) return String(value)
  return "Not reported"
}

function formatCount(value: number) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value)
}

function qualityLabel(value: string | null) {
  if (!value) return "Not rated"
  const quality = value.toLocaleLowerCase()
  if (["estimated", "preliminary", "provisional"].some((part) => quality.includes(part))) {
    return "Preliminary"
  }
  if (["revised", "superseded"].some((part) => quality.includes(part))) {
    return "Revised"
  }
  if (["missing", "incomplete", "degraded", "suspect", "invalid"].some((part) => quality.includes(part))) {
    return "Needs review"
  }
  if (["verified", "final", "complete", "valid", "good"].some((part) => quality.includes(part))) {
    return "Checked"
  }
  return "Not rated"
}
