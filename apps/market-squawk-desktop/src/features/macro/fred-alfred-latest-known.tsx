import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import {
  AlertCircle,
  CalendarClock,
  Database,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"
import { Link } from "react-router-dom"

import { messageFrom } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import type { DesktopBootstrap } from "@/lib/schemas"
import type {
  FredAlfredImmutableGeneration,
  ProductTransport,
} from "@/lib/transport"

import {
  FRED_ALFRED_OPERATION,
  fredAlfredCutoffsSchema,
  fredAlfredGenerationKey,
  isFredAlfredReadyAvailability,
  parseFredAlfredAvailability,
  parseFredAlfredLatestKnownRead,
  sameFredAlfredReadyAvailability,
  type FredAlfredCutoffs,
  type FredAlfredReadyAvailability,
} from "./fred-alfred-latest-known-contracts"

type ReadRequest = {
  availability: FredAlfredReadyAvailability
  generation: FredAlfredImmutableGeneration
  cutoffs: FredAlfredCutoffs
}

export interface FredAlfredLatestKnownProps {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}

/**
 * Reads one application-owned, manifest-pinned FRED/ALFRED observation.
 *
 * The component accepts only explicit point-in-time cutoffs. Provider, dataset, series, and
 * generation selection remain with the installed Rust service.
 */
export function FredAlfredLatestKnown({
  bootstrap,
  transport,
}: FredAlfredLatestKnownProps) {
  const operationAvailable = bootstrap.operations.some(
    (operation) =>
      operation.name === FRED_ALFRED_OPERATION && operation.readOnly,
  )
  const status = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "research",
      FRED_ALFRED_OPERATION,
      { mode: "availability" },
    ),
    enabled: operationAvailable,
    queryFn: async () =>
      parseFredAlfredAvailability(
        await transport.query({ query: "fredAlfredLatestKnownStatus" }),
      ),
  })
  const readyStatus = isFredAlfredReadyAvailability(status.data)
    ? status.data
    : null
  const readyGenerationKey = readyStatus
    ? fredAlfredGenerationKey(readyStatus.data.generation)
    : null
  const [knowledgeCutoff, setKnowledgeCutoff] = React.useState("")
  const [effectiveDateCutoff, setEffectiveDateCutoff] = React.useState("")
  const [readRequest, setReadRequest] = React.useState<ReadRequest | null>(null)
  const [readFailure, setReadFailure] = React.useState<string | null>(null)
  const admittedCutoffs = fredAlfredCutoffsSchema.safeParse({
    knowledgeCutoff,
    effectiveDateCutoff,
  })
  const requestIsCurrent =
    readRequest !== null &&
    readyStatus !== null &&
    sameFredAlfredReadyAvailability(readRequest.availability, readyStatus)

  React.useEffect(() => {
    if (!readRequest) return
    if (
      !readyStatus ||
      !sameFredAlfredReadyAvailability(readRequest.availability, readyStatus)
    ) {
      setReadRequest(null)
      setReadFailure(
        "The immutable FRED/ALFRED generation changed. Review the refreshed generation before reading again.",
      )
    }
  }, [readRequest, readyGenerationKey])

  const read = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "research",
      FRED_ALFRED_OPERATION,
      readRequest
        ? {
            mode: "latest-known",
            generation: fredAlfredGenerationKey(readRequest.generation),
            ...readRequest.cutoffs,
          }
        : { mode: "latest-known", state: "not-requested" },
    ),
    enabled: requestIsCurrent,
    retry: false,
    queryFn: async () => {
      const requested = requiredReadRequest(readRequest)
      try {
        return parseFredAlfredLatestKnownRead(
          await transport.query({
            query: "fredAlfredLatestKnownRead",
            generation: requested.generation,
            ...requested.cutoffs,
          }),
          requested.availability,
          requested.cutoffs,
        )
      } catch (error) {
        setReadFailure(messageFrom(error))
        await status.refetch()
        throw error
      }
    },
  })

  const refresh = () => {
    setReadRequest(null)
    setReadFailure(null)
    void status.refetch()
  }

  if (!operationAvailable) {
    return (
      <FredFrame>
        <Alert>
          <AlertCircle aria-hidden="true" />
          <AlertTitle>FRED/ALFRED point-in-time research is unavailable</AlertTitle>
          <AlertDescription>
            This installed service generation does not expose the exact read-only macro
            operation. No provider request or untyped fallback was attempted.
          </AlertDescription>
        </Alert>
      </FredFrame>
    )
  }

  if (status.isPending) {
    return (
      <FredFrame busy>
        <Skeleton className="h-28 rounded-lg" />
        <Skeleton className="mt-3 h-44 rounded-lg" />
      </FredFrame>
    )
  }

  if (status.isError) {
    return (
      <FredFrame>
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>FRED/ALFRED availability could not be read</AlertTitle>
          <AlertDescription>{messageFrom(status.error)}</AlertDescription>
        </Alert>
        <Button className="mt-4" variant="outline" onClick={refresh}>
          <RefreshCw aria-hidden="true" />
          Retry availability
        </Button>
      </FredFrame>
    )
  }

  if (!status.data) return null

  if (status.data.data.state === "setup_required") {
    return (
      <FredFrame>
        <AvailabilityNotice
          title="Connect FRED/ALFRED research"
          detail="No desired FRED/ALFRED activation exists in this workspace. Complete source setup before Market Squawk can publish a point-in-time macro generation."
          setup
        />
      </FredFrame>
    )
  }

  if (status.data.data.state === "unavailable") {
    const datasetAbsent =
      status.data.data.reason === "exact_provider_dataset_absent"
    return (
      <FredFrame>
        <AvailabilityNotice
          title={
            datasetAbsent
              ? "FRED/ALFRED dataset is not bound"
              : "FRED/ALFRED publication is not ready"
          }
          detail={
            datasetAbsent
              ? "Source setup exists, but the installed service has not retained one exact provider dataset for this workspace."
              : "The exact provider dataset is retained, but no immutable analytical manifest is available yet."
          }
          setup
        />
        <Button className="mt-4" variant="outline" onClick={refresh}>
          <RefreshCw aria-hidden="true" />
          Check publication again
        </Button>
      </FredFrame>
    )
  }

  const availability = readyStatus
  if (!availability) return null
  const generation = availability.data.generation

  return (
    <FredFrame>
      <header className="flex flex-wrap items-start justify-between gap-4 border-b border-border p-5">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-primary">
            FRED/ALFRED point-in-time research
          </p>
          <h2 id="fred-alfred-latest-known-title" className="mt-2 text-xl font-semibold">
            Latest known observation at your cutoffs
          </h2>
          <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
            Choose when the application was allowed to know the observation and the latest
            effective date it may use. The installed service retains the exact provider series
            and immutable publication; this view cannot substitute another source or generation.
          </p>
        </div>
        <EvidenceBadge>Official delayed · research only</EvidenceBadge>
      </header>

      <div className="p-5">
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(18rem,0.7fr)]">
          <form
            className="rounded-lg border border-border bg-background/30 p-4"
            onSubmit={(event) => {
              event.preventDefault()
              if (!admittedCutoffs.success) return
              setReadFailure(null)
              setReadRequest({
                availability,
                generation,
                cutoffs: admittedCutoffs.data,
              })
            }}
          >
            <div className="grid gap-4 sm:grid-cols-2">
              <div>
                <Label htmlFor="fred-knowledge-cutoff">Knowledge cutoff</Label>
                <Input
                  id="fred-knowledge-cutoff"
                  className="mt-2 font-mono text-xs"
                  value={knowledgeCutoff}
                  placeholder="2026-08-28T14:30:00Z"
                  autoComplete="off"
                  spellCheck={false}
                  onChange={(event) => {
                    setKnowledgeCutoff(event.target.value)
                    setReadRequest(null)
                    setReadFailure(null)
                  }}
                />
                <p className="mt-2 text-[10px] leading-4 text-muted-foreground">
                  Enter an explicit RFC 3339 timestamp with a UTC or numeric offset.
                </p>
              </div>
              <div>
                <Label htmlFor="fred-effective-cutoff">Effective-date cutoff</Label>
                <Input
                  id="fred-effective-cutoff"
                  type="date"
                  className="mt-2 font-mono text-xs"
                  value={effectiveDateCutoff}
                  onChange={(event) => {
                    setEffectiveDateCutoff(event.target.value)
                    setReadRequest(null)
                    setReadFailure(null)
                  }}
                />
                <p className="mt-2 text-[10px] leading-4 text-muted-foreground">
                  This date must not follow the UTC date of the knowledge cutoff.
                </p>
              </div>
            </div>
            {!admittedCutoffs.success &&
            (knowledgeCutoff.length > 0 || effectiveDateCutoff.length > 0) ? (
              <p className="mt-3 text-xs text-destructive" role="alert">
                Enter both valid cutoffs. The effective date cannot follow the knowledge
                cutoff date.
              </p>
            ) : null}
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <Button type="submit" disabled={!admittedCutoffs.success || read.isFetching}>
                <CalendarClock aria-hidden="true" />
                {read.isFetching ? "Reading exact generation…" : "Read point-in-time value"}
              </Button>
              <Button
                type="button"
                variant="outline"
                disabled={status.isFetching}
                onClick={refresh}
              >
                <RefreshCw
                  className={status.isFetching ? "animate-spin" : ""}
                  aria-hidden="true"
                />
                Refresh generation
              </Button>
            </div>
          </form>

          <GenerationEvidence generation={generation} />
        </div>

        {readFailure ? (
          <Alert className="mt-4" variant="destructive">
            <AlertCircle aria-hidden="true" />
            <AlertTitle>The exact point-in-time read was rejected</AlertTitle>
            <AlertDescription>
              {readFailure} The application refreshed generation availability and did not fall
              forward to a newer publication.
            </AlertDescription>
          </Alert>
        ) : null}

        {read.isPending && readRequest ? (
          <Skeleton className="mt-4 h-48 rounded-lg" />
        ) : read.data && requestIsCurrent ? (
          <ObservationResult read={read.data} />
        ) : null}
      </div>
    </FredFrame>
  )
}

function ObservationResult({
  read,
}: {
  read: ReturnType<typeof parseFredAlfredLatestKnownRead>
}) {
  const result = read.data.result
  const observation = result.observation

  return (
    <article className="mt-4 rounded-lg border border-border bg-background/30 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
            {observation.seriesId}
          </p>
          <h3 className="mt-2 text-base font-semibold">
            {observation.value.state === "observed"
              ? observation.value.decimal
              : observation.value.marker}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            {observation.value.state === "observed"
              ? observation.unitId
              : (observation.value.reason ?? "Provider reported an explicit missing value.")}
          </p>
        </div>
        <EvidenceBadge>
          {observation.value.state === "observed" ? "Observed" : "Missing"}
        </EvidenceBadge>
      </div>

      <dl className="mt-5 grid gap-4 border-t border-border pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <EvidenceFact label="Effective date" value={observation.effectiveDate} />
        <EvidenceFact label="Published vintage" value={observation.publishedVintage} />
        <EvidenceFact label="Knowledge cutoff" value={result.selection.knowledgeCutoff} mono />
        <EvidenceFact
          label="Effective cutoff"
          value={result.selection.effectiveDateCutoff}
        />
      </dl>

      <details className="mt-4 rounded-md border border-border p-3">
        <summary className="cursor-pointer text-xs font-semibold focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary">
          Publication, revision, and provenance evidence
        </summary>
        <dl className="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <EvidenceFact label="Revision" value={String(observation.revision)} mono />
          <EvidenceFact label="First observed locally" value={observation.availableAt} mono />
          <EvidenceFact label="Received" value={observation.receivedAt} mono />
          <EvidenceFact label="Ingested" value={observation.ingestedAt} mono />
          <EvidenceFact
            label="Superseded after"
            value={observation.supersededAfter ?? "Not superseded in this generation"}
          />
          <EvidenceFact label="Source identifier" value={observation.sourceIdentifier} mono />
          <EvidenceFact label="Raw page SHA-256" value={observation.rawPageDigest} mono />
          <EvidenceFact
            label="Selection SHA-256"
            value={result.selection.selectionDigest}
            mono
          />
          <EvidenceFact
            label="Object graph SHA-256"
            value={result.binding.objectGraphDigest}
            mono
          />
          <EvidenceFact label="Query SHA-256" value={result.binding.queryIdentity} mono />
          <EvidenceFact label="Result SHA-256" value={result.binding.resultDigest} mono />
          <EvidenceFact label="Quality" value="Official delayed · research only" />
        </dl>
      </details>
    </article>
  )
}

function AvailabilityNotice({
  title,
  detail,
  setup,
}: {
  title: string
  detail: string
  setup: boolean
}) {
  return (
    <Alert>
      <Database aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <p>{detail}</p>
        {setup ? (
          <Button asChild className="mt-3" size="sm">
            <Link to="/connections/sources">Open Connections & Sources</Link>
          </Button>
        ) : null}
      </AlertDescription>
    </Alert>
  )
}

function GenerationEvidence({
  generation,
}: {
  generation: FredAlfredImmutableGeneration
}) {
  return (
    <aside className="rounded-lg border border-border bg-background/30 p-4">
      <div className="flex items-center gap-2">
        <ShieldCheck className="size-4 text-[var(--success)]" aria-hidden="true" />
        <h3 className="text-sm font-semibold">Immutable generation</h3>
      </div>
      <p className="mt-2 text-[11px] leading-5 text-muted-foreground">
        Every read must echo this complete generation. A changed or stale identity is rejected
        and refreshed; it never silently selects “latest.”
      </p>
      <dl className="mt-4 grid gap-3">
        <EvidenceFact label="Manifest" value={`v${generation.manifestVersion}`} />
        <EvidenceFact
          label="Schema"
          value={`${generation.schema.name} v${generation.schema.version}`}
        />
        <EvidenceFact
          label="Schema fingerprint"
          value={generation.schema.fingerprint}
          mono
        />
        <EvidenceFact label="Content SHA-256" value={generation.contentHash} mono />
      </dl>
    </aside>
  )
}

function FredFrame({
  children,
  busy = false,
}: {
  children: React.ReactNode
  busy?: boolean
}) {
  return (
    <section
      className="rounded-xl border border-border bg-card/45"
      aria-label="FRED/ALFRED point-in-time research"
      aria-busy={busy}
    >
      {children}
    </section>
  )
}

function EvidenceFact({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="min-w-0">
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </dt>
      <dd
        className={`mt-1 break-all text-[11px] leading-4 text-foreground/85 ${
          mono ? "font-mono" : ""
        }`}
      >
        {value}
      </dd>
    </div>
  )
}

function EvidenceBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className="rounded border border-border px-2 py-1 text-[9px] font-medium uppercase tracking-wider text-muted-foreground">
      {children}
    </span>
  )
}

function requiredReadRequest(request: ReadRequest | null): ReadRequest {
  if (!request) throw new Error("Choose explicit point-in-time cutoffs before reading.")
  return request
}
