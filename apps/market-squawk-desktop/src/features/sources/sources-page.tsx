import * as React from "react"
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  Activity,
  CircleAlert,
  DatabaseZap,
  KeyRound,
  RefreshCw,
  ShieldCheck,
} from "lucide-react"
import { Link } from "react-router-dom"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys } from "@/app/query-client"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  parseResearchManifest,
  type ResearchDataset,
} from "@/features/research/research-contracts"

import {
  type DoctorRateEvidence,
  type LifecycleControl,
  type SourceEvidence,
  type StoredDataEvidence,
  attachStoredData,
  lifecycleControls,
  sourceEvidence,
  sourceNeedsSetup,
} from "./source-evidence"
import { ProviderCredentialImport } from "./provider-credential-import"

export function SourcesPage() {
  const product = useProduct()
  if (product.status === "loading") return <SourcesLoading />
  if (product.status === "error") {
    return (
      <PageFrame>
        <EmptyState title="Source service is unavailable" detail={product.error} />
      </PageFrame>
    )
  }
  return (
    <ReadySourcesPage
      bootstrap={product.bootstrap}
      transport={product.transport}
    />
  )
}

function ReadySourcesPage({
  bootstrap,
  transport,
}: {
  bootstrap: DesktopBootstrap
  transport: ProductTransport
}) {
  const queryClient = useQueryClient()
  const statusReads = useQueries({
    queries: bootstrap.providerProfiles.map((profile) => ({
      queryKey: productKeys.operation(
        bootstrap.runtime,
        "source",
        "Source.GetStatus",
        { sourceIds: [profile.id] },
      ),
      queryFn: () =>
        transport.query({ query: "sourceStatus", sourceIds: [profile.id] }),
    })),
  })
  const coverage = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "source",
      "Source.GetCoverage",
      {},
    ),
    queryFn: () => transport.query({ query: "sourceCoverage" }),
  })
  const health = useQuery({
    queryKey: productKeys.operation(
      bootstrap.runtime,
      "source",
      "Source.GetHealth",
      {},
    ),
    queryFn: () => transport.query({ query: "sourceHealth" }),
  })
  const sourceRows = sourceEvidence(
    bootstrap.providerProfiles,
    bootstrap.providerSessions,
    statusReads.flatMap((query) => (query.data ? [query.data] : [])),
    coverage.data,
    health.data,
  )
  const manifestReadsAvailable = bootstrap.operations.some(
    (operation) => operation.name === "Research.GetManifest",
  )
  const providerDatasets = manifestReadsAvailable
    ? [
        ...new Set(
          sourceRows.flatMap((source) =>
            source.providerDatasetIdentifier
              ? [source.providerDatasetIdentifier]
              : [],
          ),
        ),
      ].sort()
    : []
  const manifestReads = useQueries({
    queries: providerDatasets.map((dataset) => ({
      queryKey: productKeys.operation(
        bootstrap.runtime,
        "research",
        "Research.GetManifest",
        { dataset },
      ),
      queryFn: async () =>
        parseResearchManifest(
          await transport.query({ query: "researchManifest", dataset }),
          dataset,
        ),
    })),
  })
  const sources = attachStoredData(
    sourceRows,
    manifestReads.flatMap((query) =>
      query.data ? [storedDataEvidence(query.data)] : [],
    ),
  )
  const refreshing =
    statusReads.some((query) => query.isFetching) ||
    coverage.isFetching ||
    health.isFetching ||
    manifestReads.some((query) => query.isFetching)
  const failedStatusReads = statusReads.filter((query) => query.isError).length
  const failedReads =
    failedStatusReads +
    Number(coverage.isError) +
    Number(health.isError) +
    manifestReads.filter((query) => query.isError).length
  const totalReads = statusReads.length + manifestReads.length + 2
  const active = sources.filter(
    (source) => source.operationalState === "active",
  ).length
  const fresh = sources.filter((source) => source.marketFreshness === "fresh").length
  const stored = sources.filter((source) => source.storedData !== null).length
  const quarantined = sources.filter(
    (source) => source.storedDataQuarantine !== null,
  ).length

  const refresh = () => {
    void Promise.all([
      ...statusReads.map((query) => query.refetch()),
      ...manifestReads.map((query) => query.refetch()),
      coverage.refetch(),
      health.refetch(),
    ])
  }
  const refreshAuthority = () =>
    queryClient.invalidateQueries({
      queryKey: productKeys.domain(bootstrap.runtime, "source"),
    })
  const credentialImportAvailable = bootstrap.operations.some(
    (operation) => operation.name === "Source.ImportCredentialBundle",
  )

  return (
    <PageFrame
      action={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          <RefreshCw className={refreshing ? "animate-spin" : ""} aria-hidden="true" />
          Refresh evidence
        </Button>
      }
    >
      <ProviderCredentialImport
        available={credentialImportAvailable}
        transport={transport}
        onAttempted={() => {
          void refreshAuthority()
          refresh()
        }}
      />
      {failedReads === totalReads ? (
        <EmptyState
          title="Source evidence could not be read"
          detail={messageFrom(
            statusReads.find((query) => query.error)?.error ??
              coverage.error ??
              health.error ??
              manifestReads.find((query) => query.error)?.error,
          )}
        />
      ) : sources.length === 0 && refreshing ? (
        <SourceGridLoading />
      ) : sources.length === 0 ? (
        <EmptyState
          title="No registered sources"
          detail="Complete source setup to register an available provider or local dataset."
        />
      ) : (
        <>
          {failedReads > 0 ? (
            <Notice text={`${failedReads} of ${totalReads} source evidence reads could not be completed. Missing fields remain explicitly unreported.`} />
          ) : null}
          {quarantined > 0 ? (
            <Notice
              text={`${quarantined} stored dataset association${quarantined === 1 ? " is" : "s are"} quarantined because the source and dataset identities did not establish one exact match.`}
            />
          ) : null}
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Summary label="Known sources" value={sources.length} icon={DatabaseZap} />
            <Summary label="Operational" value={active} icon={Activity} />
            <Summary label="Stored datasets" value={stored} icon={DatabaseZap} />
            <Summary label="Market fresh" value={fresh} icon={ShieldCheck} />
          </div>
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            {sources.map((source) => (
              <SourceCard
                key={source.id}
                source={source}
                transport={transport}
                onChanged={refreshAuthority}
              />
            ))}
          </div>
        </>
      )}
    </PageFrame>
  )
}

function SourceCard({
  source,
  transport,
  onChanged,
}: {
  source: SourceEvidence
  transport: ProductTransport
  onChanged: () => Promise<void>
}) {
  const [pending, setPending] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [confirming, setConfirming] = React.useState<LifecycleControl | null>(null)
  const controls = lifecycleControls(source)
  const setupReady = source.nextAction === "active"
  const operationalActive = source.operationalState === "active"
  const setupAgain = sourceNeedsSetup(source)

  const run = async (control: LifecycleControl) => {
    setConfirming(null)
    setPending(control.action)
    setError(null)
    try {
      await transport.sourceControl(
        control.action,
        control.request,
        true,
      )
      await onChanged()
    } catch (requestError) {
      setError(messageFrom(requestError))
    } finally {
      setPending(null)
    }
  }

  return (
    <article className="rounded-xl border border-border bg-card/45 p-5">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="truncate font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            {source.id}
          </p>
          <h2 className="mt-2 text-lg font-semibold">{source.name}</h2>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            {source.declaredCoverage ?? "Coverage is not reported by the installed service."}
          </p>
        </div>
        <StateBadge
          label={operationalActive ? "Operational" : runtimeName(source.operationalState)}
          active={operationalActive}
        />
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        <EvidencePanel
          title="Operational state"
          icon={ShieldCheck}
          headline={runtimeName(source.operationalState)}
          detail={operationalDetail(source)}
        />
        <EvidencePanel
          title="Latest setup attempt"
          icon={KeyRound}
          headline={
            source.setupState
              ? setupReady
                ? "Completed"
                : runtimeName(source.setupState)
              : "No setup attempt"
          }
          detail={
            source.nextAction
              ? `Latest recorded next action: ${humanize(source.nextAction)}`
              : "No historical setup action is recorded."
          }
        />
        {source.lifecycle?.startEligibility !== "not_applicable" ? (
          <EvidencePanel
            title="Doctor / start gate"
            icon={ShieldCheck}
            headline={startEligibilityLabel(source.lifecycle?.startEligibility ?? null)}
            detail={doctorGateDetail(source)}
          />
        ) : null}
        <EvidencePanel
          title="Stored data"
          icon={DatabaseZap}
          headline={
            source.storedData
              ? `${source.storedData.rowCount.toLocaleString()} rows available`
              : source.storedDataQuarantine
                ? "Stored data quarantined"
                : source.providerDatasetIdentifier
                  ? "Stored data not established"
                  : "No stored dataset"
          }
          detail={storedDataDetail(source)}
        />
        <EvidencePanel
          title="Live runtime"
          icon={Activity}
          headline={runtimeName(source.runtimeState)}
          detail={source.connection ? `Connection: ${humanize(source.connection)}` : "Connection not reported"}
        />
      </div>

      {source.lifecycle?.doctor ? (
        <DoctorEvidence evidence={source.lifecycle.doctor} />
      ) : null}

      <dl className="mt-5 grid gap-x-4 gap-y-3 border-t border-border/70 pt-4 sm:grid-cols-2">
        <Fact label="Runtime source" value={source.sourceId ?? "Not reported"} />
        <Fact label="Venue" value={source.venueId ?? "Not reported"} />
        <Fact label="Instrument" value={source.instrumentId ?? "Not reported"} />
        <Fact label="Market freshness" value={runtimeName(source.marketFreshness)} />
        <Fact label="Stream integrity" value={runtimeName(source.integrity)} />
        <Fact label="Current quality" value={runtimeName(source.quality)} />
        <Fact label="Coverage state" value={runtimeName(source.coverageState)} />
        <Fact label="Quality ceiling" value={runtimeName(source.qualityCeiling)} />
        <Fact label="Lifecycle support" value={runtimeName(source.lifecycleSupport)} />
        <Fact
          label="Provider dataset"
          value={source.providerDatasetIdentifier ?? "None published"}
        />
        <Fact
          label="Runtime generation SHA-256"
          value={source.lifecycle?.runtimeGenerationSha256 ?? "Not reported"}
        />
        <Fact
          label="Lifecycle observed"
          value={dateTime(source.lifecycle?.observedAt ?? null)}
        />
        <Fact
          label="Runtime observed"
          value={dateTime(source.runtimeObservedAt)}
        />
        <Fact label="Cost condition" value={source.zeroFee ?? "Not reported"} />
        <Fact label="Release state" value={runtimeName(source.releaseState)} />
        <Fact label="Account" value={source.accountRequirement ?? "Not reported"} />
        <Fact label="Credential" value={source.credentialRequirement ?? "Not reported"} />
      </dl>

      {controls.length > 0 || setupAgain ? (
        <div className="mt-5 flex flex-wrap gap-2 border-t border-border/70 pt-4">
          {controls.map((control) => (
            <Button
              key={control.action}
              size="sm"
              variant={control.destructive ? "outline" : "default"}
              disabled={pending !== null}
              onClick={() => {
                if (control.destructive) setConfirming(control)
                else void run(control)
              }}
            >
              {pending === control.action ? "Working…" : control.label}
            </Button>
          ))}
          {setupAgain ? (
            <Button asChild size="sm">
              <Link to="/">Set up again</Link>
            </Button>
          ) : null}
          {source.lifecycle ? (
            <p className="w-full text-[10px] text-muted-foreground">
              Revision {source.lifecycle.stateRevision}; controls use this exact returned state and
              its retained configuration only.
            </p>
          ) : null}
        </div>
      ) : null}

      {error ? (
        <p role="alert" className="mt-4 text-xs text-red-400">
          {error}
        </p>
      ) : null}
      <Dialog
        open={confirming !== null}
        onOpenChange={(open) => {
          if (!open && pending === null) setConfirming(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirming?.label ?? "Change source state"}?</DialogTitle>
            <DialogDescription>
              {confirming?.action === "verify" &&
              source.id === "alpaca.basic-market-data" &&
              (source.lifecycle?.state === "active" || source.lifecycle?.state === "blocked")
                ? "Running or renewing the Paper/IEX doctor stops any retained source runtime, including one currently reported as blocked. Starting it again remains a separate explicit action. "
                : "This changes the runtime state for this source. "}
              Market Squawk will use lifecycle revision {source.lifecycle?.stateRevision} and
              reject the request if that source state has changed since this evidence was loaded.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" disabled={pending !== null} onClick={() => setConfirming(null)}>
              Keep current state
            </Button>
            <Button
              disabled={pending !== null || confirming === null}
              onClick={() => {
                if (confirming) void run(confirming)
              }}
            >
              {pending !== null ? "Working…" : "Confirm change"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </article>
  )
}

function doctorGateDetail(source: SourceEvidence) {
  const lifecycle = source.lifecycle
  if (!lifecycle) return "No server-owned start conclusion is available."
  switch (lifecycle.startEligibility) {
    case "eligible":
      return "The exact Paper/IEX receipt is current. Starting remains a separate explicit operation."
    case "already_active":
      return "The exact Paper/IEX receipt already owns the active runtime; another Start is not applicable."
    case "doctor_required":
      return "Run the code-owned doctor before this stopped configuration can start."
    case "doctor_expired":
      return "The retained receipt is historical evidence only; renew the doctor before starting."
    case "credential_stale":
      return "Credential generation or immutable configuration no longer matches the retained receipt."
    case "reconciliation_required":
      return "Durable lifecycle state must be reconciled before another doctor or start."
    case "provider_unavailable":
      return "The doctor retained a bounded unavailable or degraded capability result; it did not admit Start."
    case "not_applicable":
      return "This source does not use the Paper/IEX doctor contract."
  }
}

function startEligibilityLabel(
  eligibility: NonNullable<SourceEvidence["lifecycle"]>["startEligibility"] | null,
) {
  switch (eligibility) {
    case "eligible":
      return "Ready to start"
    case "already_active":
      return "Already active"
    case "doctor_required":
      return "Doctor required"
    case "doctor_expired":
      return "Doctor expired"
    case "credential_stale":
      return "Credential stale"
    case "reconciliation_required":
      return "Reconciliation required"
    case "provider_unavailable":
      return "Provider unavailable"
    case "not_applicable":
      return "Not applicable"
    default:
      return "Not established"
  }
}

function DoctorEvidence({
  evidence,
}: {
  evidence: NonNullable<SourceEvidence["lifecycle"]>["doctor"]
}) {
  if (!evidence) return null
  const capabilities = evidence.capabilities
  return (
    <section className="mt-5 rounded-lg border border-border bg-background/35 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Alpaca Paper / IEX doctor evidence
          </p>
          <p className="mt-1 text-sm font-medium">
            {evidence.current ? "Current receipt" : "Expired retained receipt"}
          </p>
        </div>
        <StateBadge label="Direct unverified" active={false} />
      </div>
      <dl className="mt-4 grid gap-x-4 gap-y-3 sm:grid-cols-2">
        <Fact label="Realm" value="Paper" />
        <Fact label="Credential generation" value={String(evidence.credentialGeneration)} />
        <Fact label="Verified" value={dateTime(evidence.verifiedAt)} />
        <Fact label="Exclusive expiry" value={dateTime(evidence.exclusiveExpiresAt)} />
        <Fact label="Receipt SHA-256" value={evidence.receiptSha256} />
        <Fact label="Market-data principal SHA-256" value={evidence.marketDataPrincipalSha256} />
        <Fact
          label="Latest AAPL/IEX quote"
          value={runtimeName(capabilities.iexLatestQuote.disposition)}
        />
        <Fact
          label="IEX snapshot batch"
          value={`${runtimeName(capabilities.iexSnapshotBatch.disposition)} · ${capabilities.iexSnapshotBatch.valid ?? 0}/${capabilities.iexSnapshotBatch.requested ?? 0} valid · ${capabilities.iexSnapshotBatch.missing ?? 0} missing`}
        />
        <Fact
          label="Snapshot rate evidence"
          value={doctorRateSummary(capabilities.iexSnapshotBatch.rate)}
        />
        <Fact
          label="IEX WebSocket auth + subscription"
          value={runtimeName(capabilities.iexWebSocket.disposition)}
        />
        <Fact
          label="WebSocket handshake rate evidence"
          value={doctorRateSummary(capabilities.iexWebSocket.rate)}
        />
        <Fact
          label="Historical bars"
          value={`${runtimeName(capabilities.iexHistoricalBars.disposition)} · ${capabilities.iexHistoricalBars.bars ?? 0} bars · terminal ${capabilities.iexHistoricalBars.terminalPagination === true ? "yes" : "no"}`}
        />
        <Fact
          label="IEX / UTC calendar"
          value={`${runtimeName(capabilities.iexUtcCalendar.disposition)} · ${capabilities.iexUtcCalendar.matchedDates ?? 0}/${capabilities.iexUtcCalendar.sessions ?? 0} dates matched`}
        />
        <Fact
          label="Authority boundary"
          value="Market-data credential principal only; no brokerage account, positions, orders, execution, or trading authority."
        />
      </dl>
      <details className="mt-4 rounded-md border border-border bg-card/35 p-3">
        <summary className="cursor-pointer text-[10px] uppercase tracking-wider text-muted-foreground">
          Exact authority evidence
        </summary>
        <dl className="mt-3 grid gap-x-4 gap-y-3 sm:grid-cols-2">
          <Fact label="Doctor revision" value={evidence.doctorRevision} />
          <Fact label="Doctor contract SHA-256" value={evidence.doctorContractSha256} />
          <Fact label="Capability revision" value={evidence.capabilityRevision} />
          <Fact label="Capability SHA-256" value={evidence.capabilitySha256} />
          <Fact
            label="Public configuration SHA-256"
            value={evidence.publicConfigurationSha256}
          />
          <Fact label="Rights decision SHA-256" value={evidence.rightsDecisionSha256} />
          <Fact label="Rate policy SHA-256" value={evidence.ratePolicySha256} />
        </dl>
      </details>
    </section>
  )
}

function doctorRateSummary(rate: DoctorRateEvidence | null): string {
  if (!rate) return "No response rate evidence retained"
  const limit = rate.limit.state === "observed" ? String(rate.limit.value) : "missing"
  const remaining = rate.remaining.state === "observed"
    ? String(rate.remaining.value)
    : "missing"
  const reset = rate.resetUnixSeconds.state === "observed"
    ? rate.resetUnixSeconds.value
    : "missing"
  const retry = rate.retryAfter.state === "observed"
    ? `${humanize(rate.retryAfter.value.kind)} ${rate.retryAfter.value.value}`
    : "missing"
  return `Provider headers: limit ${limit} · remaining ${remaining} · reset ${reset} · retry-after ${retry}`
}

function storedDataEvidence(dataset: ResearchDataset): StoredDataEvidence {
  return {
    datasetId: dataset.manifest.datasetId,
    sourceId: dataset.sourceId,
    generationKind: dataset.generationKind,
    manifestVersion: dataset.manifest.manifestVersion,
    rowCount: dataset.rowCount,
    totalBytes: dataset.totalBytes,
    objectCount: dataset.objectCount,
  }
}

function operationalDetail(source: SourceEvidence): string {
  if (source.lifecycle?.blocker) {
    return `Callable source blocked: ${humanize(source.lifecycle.blocker)}.`
  }
  if (source.lifecycleSupport === "not_applicable") {
    return "This surface is managed by its product domain rather than source lifecycle controls."
  }
  if (!source.lifecycle) {
    return "Lifecycle evidence could not be verified."
  }
  if (source.lifecycle.currentGeneration) {
    return `Verified live generation ${source.lifecycle.currentGeneration}.`
  }
  if (source.lifecycle.runtimeGenerationSha256) {
    return "A callable source runtime generation is verified."
  }
  return `Lifecycle revision ${source.lifecycle.stateRevision} is authoritative.`
}

function storedDataDetail(source: SourceEvidence): string {
  if (source.storedData) {
    return `${source.storedData.datasetId} · manifest ${source.storedData.manifestVersion} · ${source.storedData.objectCount.toLocaleString()} objects · ${source.storedData.totalBytes.toLocaleString()} bytes.`
  }
  if (source.storedDataQuarantine) {
    const quarantine = source.storedDataQuarantine
    const observed = quarantine.observedSourceIds.join(", ")
    if (quarantine.reason === "source_identity_missing") {
      return (
        `${quarantine.datasetId} reports stored source ${observed}, but this source has no ` +
        "current source identity. The manifest is not attached."
      )
    }
    if (quarantine.reason === "ambiguous_dataset_identity") {
      return (
        `${quarantine.datasetId} resolved to multiple stored manifest records ` +
        `(sources: ${observed}). None is attached.`
      )
    }
    return (
      `${quarantine.datasetId} belongs to stored source ${observed}, not current source ` +
      `${quarantine.expectedSourceId}. The mismatched manifest is not attached.`
    )
  }
  if (source.providerDatasetIdentifier) {
    return `The source reports ${source.providerDatasetIdentifier}, but its current manifest was not verified.`
  }
  return "No immutable dataset manifest is currently associated with this source."
}

function EvidencePanel({
  title,
  headline,
  detail,
  icon: Icon,
}: {
  title: string
  headline: string
  detail: string
  icon: typeof Activity
}) {
  return (
    <div className="rounded-lg border border-border bg-background/45 p-3">
      <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
        <Icon className="size-3.5" aria-hidden="true" />
        {title}
      </div>
      <p className="mt-3 text-sm font-medium">{headline}</p>
      <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">{detail}</p>
    </div>
  )
}

function Summary({ label, value, icon: Icon }: { label: string; value: number; icon: typeof Activity }) {
  return (
    <div className="rounded-xl border border-border bg-card/35 p-4">
      <Icon className="size-4 text-primary" aria-hidden="true" />
      <p className="mt-3 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 font-mono text-2xl font-semibold">{value}</p>
    </div>
  )
}

function PageFrame({ children, action }: { children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <div className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Market Squawk · Provider evidence
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Sources</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Setup, runtime connection, coverage, freshness, integrity, and data quality remain
            separate so a configured source is never mistaken for a healthy market feed.
          </p>
        </div>
        {action}
      </div>
      <div className="mt-6">{children}</div>
    </div>
  )
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-xl border border-border bg-card/45 p-6">
      <DatabaseZap className="size-5 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-4 text-base font-semibold">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-muted-foreground">{detail}</p>
    </section>
  )
}

function SourcesLoading() {
  return (
    <PageFrame>
      <SourceGridLoading />
    </PageFrame>
  )
}

function SourceGridLoading() {
  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <Skeleton className="h-96 rounded-xl" />
      <Skeleton className="h-96 rounded-xl" />
    </div>
  )
}

function Notice({ text }: { text: string }) {
  return (
    <div className="mb-4 flex gap-2 rounded-lg border border-amber-400/20 bg-amber-400/5 p-3 text-xs text-amber-100">
      <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
      {text}
    </div>
  )
}

function StateBadge({ label, active }: { label: string; active: boolean }) {
  return (
    <span
      className={
        active
          ? "shrink-0 rounded-full border border-emerald-400/25 bg-emerald-400/10 px-2.5 py-1 text-[10px] font-medium text-emerald-300"
          : "shrink-0 rounded-full border border-border bg-background/50 px-2.5 py-1 text-[10px] font-medium text-muted-foreground"
      }
    >
      {label}
    </span>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words text-xs text-foreground/85">{value}</dd>
    </div>
  )
}

function runtimeName(value: string | null) {
  return value ? humanize(value) : "Not reported"
}

function dateTime(value: string | null) {
  if (!value) return "Not reported"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? "Not reported" : parsed.toLocaleString()
}
