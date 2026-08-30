import * as React from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { DatabaseZap, LoaderCircle, RefreshCw } from "lucide-react"
import { Link } from "react-router-dom"

import { productKeys } from "@/app/query-client"
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
import { projectDesktopBootstrap, type DesktopSystemBootstrap } from "@/lib/schemas"
import type { SystemTransport } from "@/lib/transport"

import {
  parseResearchJobReceipt,
  type ResearchJobReceipt,
} from "./research-system-contracts"
import { ResearchFileImport } from "@/features/research/research-file-import"

import {
  parseResearchSourceInputs,
  parseResearchSourceObjects,
  receiptForDiscoveredObject,
  type ResearchSourceInput,
  type ResearchSourceObject,
} from "./research-ingestion-contracts"

export function ResearchIngestion({
  bootstrap,
  connectedSourceIngestionAvailable,
  transport,
  onStarted,
}: {
  bootstrap: DesktopSystemBootstrap
  connectedSourceIngestionAvailable: boolean
  transport: SystemTransport
  onStarted: () => void
}) {
  const queryClient = useQueryClient()
  const sourceKey = [
    ...productKeys.domain(bootstrap.productSessionToken, "source"),
    "research-inputs",
  ] as const
  const sources = useQuery({
    queryKey: sourceKey,
    enabled: connectedSourceIngestionAvailable,
    queryFn: async () =>
      parseResearchSourceInputs(
        await transport.systemQuery({ query: "sourceStatus" }),
      ),
  })
  const [sourceIdentity, setSourceIdentity] = React.useState("")
  const source = selectedSource(sources.data ?? [], sourceIdentity)
  const objectKey = [
    ...productKeys.domain(bootstrap.productSessionToken, "source"),
    "research-objects",
    source?.provider ?? null,
    source?.dataset ?? null,
  ] as const
  const objects = useQuery({
    queryKey: objectKey,
    enabled: connectedSourceIngestionAvailable && source !== null,
    queryFn: async () => {
      const selected = requiredSource(source)
      return parseResearchSourceObjects(
        await transport.systemQuery({
          query: "researchSourceObjects",
          provider: selected.provider,
          dataset: selected.dataset,
        }),
        selected,
      )
    },
  })
  const [objectId, setObjectId] = React.useState("")
  const object = selectedObject(objects.data ?? [], objectId)
  const [confirmationOpen, setConfirmationOpen] = React.useState(false)
  const [receipt, setReceipt] = React.useState<ResearchJobReceipt | null>(null)
  const ingest = useMutation({
    mutationFn: async ({
      source,
      object,
    }: {
      source: ResearchSourceInput
      object: ResearchSourceObject
    }) => {
      const discovery = await transport.researchControl(
        {
          action: "discoverSourceObjects",
          provider: source.provider,
          dataset: source.dataset,
        },
        true,
      )
      const discoveryReceipt = receiptForDiscoveredObject(
        discovery,
        source,
        object.object_id,
      )
      return parseResearchJobReceipt(
        await transport.researchControl(
          {
            action: "startIngestSource",
            provider: source.provider,
            object: object.object_id,
            dataset: source.dataset,
            discoveryReceipt,
          },
          true,
        ),
      )
    },
    onSuccess: (started) => {
      setReceipt(started)
      setConfirmationOpen(false)
      onStarted()
      void queryClient.invalidateQueries({ queryKey: objectKey })
    },
  })

  React.useEffect(() => {
    if (source && sourceIdentity !== identity(source)) {
      setSourceIdentity(identity(source))
    }
  }, [source, sourceIdentity])

  React.useEffect(() => {
    if (object && object.object_id !== objectId) {
      setObjectId(object.object_id)
    }
  }, [object, objectId])

  if (!connectedSourceIngestionAvailable) {
    return (
      <>
        <ResearchFileImport bootstrap={projectDesktopBootstrap(bootstrap)} onStarted={onStarted} />
        <ActionUnavailable
          title="Connected-source ingestion is not available"
          detail="Connect and verify an eligible source before starting a durable import."
        />
      </>
    )
  }

  return (
    <>
      <ResearchFileImport bootstrap={projectDesktopBootstrap(bootstrap)} onStarted={onStarted} />
      <section className="mt-5 rounded-xl border border-border bg-card/35 p-5">
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <DatabaseZap className="size-4 text-primary" aria-hidden="true" />
              <h2 className="text-sm font-semibold">
                Ingest a discovered source object
              </h2>
            </div>
            <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
              Choose an object from a configured source. Confirmation creates a
              short-lived, single-use discovery receipt and immediately starts a durable
              ingestion job for that exact object. No filesystem path or unrestricted
              query is accepted.
            </p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              void sources.refetch()
              if (source) void objects.refetch()
            }}
            disabled={sources.isFetching || objects.isFetching}
          >
            <RefreshCw
              className={
                sources.isFetching || objects.isFetching ? "animate-spin" : ""
              }
              aria-hidden="true"
            />
            Refresh inputs
          </Button>
        </div>

        {sources.isPending ? (
          <p className="mt-4 text-xs text-muted-foreground">
            Reading configured research sources…
          </p>
        ) : sources.isError ? (
          <ActionUnavailable
            title="Research sources could not be read"
            detail={messageFrom(sources.error)}
          />
        ) : sources.data.length === 0 ? (
          <div className="mt-4 rounded-lg border border-dashed border-border p-4">
            <p className="text-xs font-medium">
              No ingest-capable source input is configured.
            </p>
            <p className="mt-1 text-[11px] leading-5 text-muted-foreground">
              Complete a research source setup before discovering provider objects.
            </p>
            <Button asChild className="mt-3" size="sm">
              <Link to="/connections/sources">Open Connections &amp; Sources</Link>
            </Button>
          </div>
        ) : (
          <div className="mt-4 grid gap-4 lg:grid-cols-2">
            <label className="text-xs font-medium">
              Configured source dataset
              <select
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
                value={source ? identity(source) : ""}
                onChange={(event) => {
                  setSourceIdentity(event.target.value)
                  setObjectId("")
                  setReceipt(null)
                }}
              >
                {sources.data.map((input) => (
                  <option key={identity(input)} value={identity(input)}>
                    {input.label} · {input.dataset}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs font-medium">
              Exact provider object
              <select
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
                value={object?.object_id ?? ""}
                disabled={
                  objects.isPending || objects.isError || !objects.data?.length
                }
                onChange={(event) => {
                  setObjectId(event.target.value)
                  setReceipt(null)
                }}
              >
                {objects.data?.map((item) => (
                  <option key={item.object_id} value={item.object_id}>
                    {item.object_id} · {item.media_type}
                  </option>
                ))}
              </select>
            </label>
          </div>
        )}

        {source && objects.isPending ? (
          <p className="mt-3 text-xs text-muted-foreground">
            Listing bounded source objects…
          </p>
        ) : null}
        {objects.isError ? (
          <p className="mt-3 text-xs leading-5 text-destructive">
            {messageFrom(objects.error)}
          </p>
        ) : null}
        {source &&
        !objects.isPending &&
        !objects.isError &&
        objects.data?.length === 0 ? (
          <p className="mt-3 text-xs leading-5 text-muted-foreground">
            The selected source returned no ingestible object. Refresh the source or
            review its setup and coverage.
          </p>
        ) : null}

        {object ? (
          <div className="mt-4 flex flex-wrap items-center gap-3 border-t border-border pt-4">
            <Button
              size="sm"
              onClick={() => setConfirmationOpen(true)}
              disabled={ingest.isPending}
            >
              {ingest.isPending ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <DatabaseZap aria-hidden="true" />
              )}
              Start ingestion
            </Button>
            <p className="text-[10px] text-muted-foreground">
              {object.expected_bytes === null
                ? "Provider did not declare an object byte count."
                : `${formatBytes(object.expected_bytes)} expected from the provider.`}
            </p>
          </div>
        ) : null}

        {receipt ? (
          <Alert className="mt-4">
            <DatabaseZap aria-hidden="true" />
            <AlertTitle>Durable ingestion queued</AlertTitle>
            <AlertDescription>
              Job {receipt.jobId} · generation {receipt.generation} · sequence{" "}
              {receipt.sequence}. Progress remains available in Research activity and
              Operations if this window closes.
            </AlertDescription>
          </Alert>
        ) : null}
        {ingest.isError ? (
          <p className="mt-3 text-xs leading-5 text-destructive">
            {messageFrom(ingest.error)}
          </p>
        ) : null}

        <Dialog open={confirmationOpen} onOpenChange={setConfirmationOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Start this ingestion job?</DialogTitle>
              <DialogDescription>
                Market Squawk will confirm discovery of{" "}
                {object?.object_id ?? "the selected object"}
                {source ? ` from ${source.label}` : ""}, consume its single-use receipt,
                and queue durable local ingestion. The source request may use the provider
                network budget.
              </DialogDescription>
            </DialogHeader>
            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => setConfirmationOpen(false)}
                disabled={ingest.isPending}
              >
                Keep reviewing
              </Button>
              <Button
                onClick={() => {
                  if (source && object) ingest.mutate({ source, object })
                }}
                disabled={!source || !object || ingest.isPending}
              >
                {ingest.isPending ? "Starting…" : "Confirm and start"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </section>
    </>
  )
}

function ActionUnavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="mt-4 rounded-lg border border-border bg-background/35 p-4">
      <p className="text-xs font-medium">{title}</p>
      <p className="mt-1 text-[11px] leading-5 text-muted-foreground">{detail}</p>
    </div>
  )
}

function selectedSource(inputs: ResearchSourceInput[], selected: string) {
  return inputs.find((input) => identity(input) === selected) ?? inputs[0] ?? null
}

function selectedObject(objects: ResearchSourceObject[], selected: string) {
  return objects.find((object) => object.object_id === selected) ?? objects[0] ?? null
}

function requiredSource(source: ResearchSourceInput | null) {
  if (!source) throw new Error("Choose a configured research source first.")
  return source
}

function identity(input: ResearchSourceInput) {
  return `${input.provider}\u0000${input.dataset}`
}

function formatBytes(value: number | string) {
  const numeric = Number(value)
  if (!Number.isSafeInteger(numeric)) return `${value} B`
  if (numeric < 1_024) return `${numeric} B`
  const units = ["KiB", "MiB", "GiB", "TiB"]
  let amount = numeric
  let unit = -1
  do {
    amount /= 1_024
    unit += 1
  } while (amount >= 1_024 && unit < units.length - 1)
  return `${amount.toFixed(amount >= 10 ? 0 : 1)} ${units[unit]}`
}

function messageFrom(error: unknown) {
  return error instanceof Error
    ? error.message
    : "Market Squawk could not complete this local research request."
}
