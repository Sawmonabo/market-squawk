import * as React from "react"
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query"
import {
  AlertCircle,
  BookOpenCheck,
  BriefcaseBusiness,
  RefreshCw,
  Search,
} from "lucide-react"

import { messageFrom } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { humanize } from "@/lib/formatters"
import { formatTimestamp } from "@/lib/time"
import type { ProductTransport } from "@/lib/transport"

import {
  digestHex,
  parseDossierCreateOutcome,
  parseDossierPreparationInventory,
  parseDossierPreparationPreview,
  parseDecisionCandidates,
  parseDecisionCandidateDossierPage,
  parseDecisionScreenRunPage,
  type CandidateView,
  type DecisionDossierView,
  type DossierPreparationPreview,
  type ScreenRunIndexView,
} from "./contracts"
import { EvidenceIdentity, StateLabel } from "./decision-boundaries"

const DISCOVERY_LIMIT = 100

export function CandidateDossierWorkspace({
  transport,
  scope,
  selectedTargetDossierId,
  onSelectTargetDossier,
}: {
  transport: ProductTransport
  scope: ProductScope
  selectedTargetDossierId: string | null
  onSelectTargetDossier: (dossier: DecisionDossierView) => void
}) {
  const queryClient = useQueryClient()
  const [runId, setRunId] = React.useState("")
  const [candidateId, setCandidateId] = React.useState("")
  const runs = useInfiniteQuery({
    queryKey: productKeys.operation(scope, "decision", "screen-runs", {
      limit: DISCOVERY_LIMIT,
    }),
    refetchInterval: 5_000,
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      parseDecisionScreenRunPage(
        await transport.query({
          query: "decisionScreenRuns",
          ...(pageParam ? { afterRunId: pageParam } : {}),
          limit: DISCOVERY_LIMIT,
        }),
      ),
    getNextPageParam: (page) => page.nextAfter ?? undefined,
  })

  const candidates = useQuery({
    queryKey: productKeys.operation(scope, "decision", "candidates", { runId }),
    queryFn: async () =>
      parseDecisionCandidates(
        await transport.query({ query: "decisionCandidates", runId }),
      ),
    enabled: runId.length > 0,
  })
  const dossierKey = productKeys.operation(scope, "decision", "candidate-dossiers", {
    candidateId,
    limit: DISCOVERY_LIMIT,
  })
  const dossiers = useInfiniteQuery({
    queryKey: dossierKey,
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      parseDecisionCandidateDossierPage(
        await transport.query({
          query: "decisionCandidateDossiers",
          candidateId,
          ...(pageParam ? { afterDossierId: pageParam } : {}),
          limit: DISCOVERY_LIMIT,
        }),
      ),
    enabled: candidateId.length > 0,
    getNextPageParam: (page) => page.nextAfter ?? undefined,
  })
  const dossierPreparation = useQuery({
    queryKey: productKeys.operation(scope, "decision", "dossier-preparation", {
      candidateId,
    }),
    enabled: candidateId.length > 0,
    queryFn: async () =>
      parseDossierPreparationInventory(
        await transport.query({
          query: "decisionDossierPreparation",
          candidateId,
        }),
      ),
  })
  const prepareDossier = useMutation({
    mutationFn: async () => {
      const inventory = dossierPreparation.data
      if (!inventory) throw new Error("Candidate evidence is not available yet.")
      return parseDossierPreparationPreview(
        await transport.decisionControl(
          {
            action: "prepareDossier",
            draft: {
              candidateId: inventory.candidateId,
              evidence: [
                ...inventory.requiredEvidence,
                ...(inventory.portfolioImpactAvailable
                  ? (["portfolio_impact"] as const)
                  : []),
              ],
            },
          },
          false,
        ),
      )
    },
  })
  const createDossier = useMutation({
    mutationFn: async (preview: DossierPreparationPreview) =>
      parseDossierCreateOutcome(
        await transport.decisionControl(
          { action: "createDossier", receiptId: preview.receiptId },
          true,
        ),
      ),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: dossierKey })
    },
  })
  const runEntries = runs.data?.pages.flatMap((page) => page.items) ?? []
  const dossierEntries = dossiers.data?.pages.flatMap((page) => page.items) ?? []

  return (
    <section aria-labelledby="candidate-funnel-heading" className="mt-8">
      <div>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
          Screen-to-decision evidence
        </p>
        <h2 id="candidate-funnel-heading" className="mt-1 text-lg font-semibold">
          Candidate funnel and dossier
        </h2>
        <p className="mt-1 max-w-3xl text-sm leading-6 text-muted-foreground">
          Start with an immutable point-in-time screen run, review its bounded ranked candidates,
          then open only dossiers the decision authority retained for the selected candidate.
        </p>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-2">
        <RecordPanel title="Saved-screen runs" icon={Search}>
          {runs.isPending ? (
            <PanelLoading />
          ) : runs.isError ? (
            <RecordError
              title="Saved-screen runs could not be loaded"
              error={runs.error}
              retry={() => void runs.refetch()}
            />
          ) : runEntries.length === 0 ? (
            <PromptState text="No immutable saved-screen runs are retained in this workspace." />
          ) : (
            <div className="mt-4 grid gap-2">
              {runEntries.map((run) => (
                <ScreenRunCard
                  key={run.id}
                  run={run}
                  selected={run.id === runId}
                  onSelect={() => {
                    setRunId(run.id)
                    setCandidateId("")
                    prepareDossier.reset()
                    createDossier.reset()
                  }}
                />
              ))}
              {runs.hasNextPage && (
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={runs.isFetchingNextPage}
                  onClick={() => void runs.fetchNextPage()}
                >
                  {runs.isFetchingNextPage ? <RefreshCw className="animate-spin" /> : null}
                  Load more runs
                </Button>
              )}
            </div>
          )}
        </RecordPanel>

        <RecordPanel title="Candidates from the selected run" icon={Search}>
          {!runId ? (
            <PromptState text="Select a retained saved-screen run to load its ranked candidate funnel." />
          ) : candidates.isPending ? (
            <PanelLoading />
          ) : candidates.isError ? (
            <RecordError
              title="Candidates could not be loaded"
              error={candidates.error}
              retry={() => void candidates.refetch()}
            />
          ) : candidates.data.length === 0 ? (
            <PromptState text="This durable screen run contains no selected candidates." />
          ) : (
            <div className="mt-4 grid gap-3">
              {candidates.data.map((candidate) => (
                <CandidateCard
                  key={candidate.id}
                  candidate={candidate}
                  selected={candidate.id === candidateId}
                  onSelect={() => {
                    setCandidateId(candidate.id)
                    prepareDossier.reset()
                    createDossier.reset()
                  }}
                />
              ))}
            </div>
          )}
        </RecordPanel>

        <RecordPanel title="Dossiers for the selected candidate" icon={BookOpenCheck}>
          {!candidateId ? (
            <PromptState text="Select a candidate to discover its authoritative decision dossiers." />
          ) : (
            <div className="mt-4 grid gap-4">
              <div className="rounded-xl border border-primary/25 bg-primary/5 p-4">
                <h3 className="text-sm font-semibold">Build an evidence-bound dossier</h3>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  Market Squawk assembles the selected candidate, exact dataset, historical
                  universe, and available portfolio evidence. Review the preview before retaining
                  it for targets and later paper decisions.
                </p>
                {dossierPreparation.isPending ? (
                  <Skeleton className="mt-3 h-9 w-40" />
                ) : dossierPreparation.isError ? (
                  <RecordError
                    title="Dossier evidence could not be loaded"
                    error={dossierPreparation.error}
                    retry={() => void dossierPreparation.refetch()}
                  />
                ) : (
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={prepareDossier.isPending}
                      onClick={() => prepareDossier.mutate()}
                    >
                      {prepareDossier.isPending ? (
                        <RefreshCw className="animate-spin" aria-hidden="true" />
                      ) : (
                        <BookOpenCheck aria-hidden="true" />
                      )}
                      Prepare dossier
                    </Button>
                    {dossierPreparation.data?.portfolioImpactAvailable ? (
                      <StateLabel value="portfolio evidence included" />
                    ) : (
                      <StateLabel value="no portfolio evidence" />
                    )}
                  </div>
                )}
                {prepareDossier.isError && (
                  <Alert variant="destructive" className="mt-3">
                    <AlertCircle aria-hidden="true" />
                    <AlertTitle>Dossier preparation failed</AlertTitle>
                    <AlertDescription>{messageFrom(prepareDossier.error)}</AlertDescription>
                  </Alert>
                )}
                {prepareDossier.data && (
                  <div className="mt-3 rounded-lg border border-border bg-background/60 p-3">
                    <p className="text-xs font-medium">
                      Preview {prepareDossier.data.dossierId}
                    </p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      Evidence: {prepareDossier.data.evidence.join(", ")}
                    </p>
                    <Button
                      type="button"
                      size="sm"
                      className="mt-3"
                      disabled={createDossier.isPending || createDossier.isSuccess}
                      onClick={() => createDossier.mutate(prepareDossier.data)}
                    >
                      {createDossier.isPending ? (
                        <RefreshCw className="animate-spin" aria-hidden="true" />
                      ) : (
                        <BookOpenCheck aria-hidden="true" />
                      )}
                      Confirm and retain dossier
                    </Button>
                  </div>
                )}
                {createDossier.isError && (
                  <Alert variant="destructive" className="mt-3">
                    <AlertCircle aria-hidden="true" />
                    <AlertTitle>Dossier was not retained</AlertTitle>
                    <AlertDescription>{messageFrom(createDossier.error)}</AlertDescription>
                  </Alert>
                )}
                {createDossier.isSuccess && (
                  <Alert className="mt-3">
                    <BookOpenCheck aria-hidden="true" />
                    <AlertTitle>Dossier retained</AlertTitle>
                    <AlertDescription>
                      The immutable dossier is now available for target preparation.
                    </AlertDescription>
                  </Alert>
                )}
              </div>

              {dossiers.isPending ? (
                <PanelLoading />
              ) : dossiers.isError ? (
                <RecordError
                  title="Candidate dossiers could not be loaded"
                  error={dossiers.error}
                  retry={() => void dossiers.refetch()}
                />
              ) : dossierEntries.length === 0 ? (
                <PromptState text="The selected candidate has no retained dossier yet." />
              ) : (
                <div className="grid gap-3">
                  {dossierEntries.map((dossier) => (
                    <DossierCard
                      key={dossier.id}
                      dossier={dossier}
                      selected={dossier.id === selectedTargetDossierId}
                      onSelect={() => onSelectTargetDossier(dossier)}
                    />
                  ))}
                  {dossiers.hasNextPage && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={dossiers.isFetchingNextPage}
                      onClick={() => void dossiers.fetchNextPage()}
                    >
                      {dossiers.isFetchingNextPage ? (
                        <RefreshCw className="animate-spin" />
                      ) : null}
                      Load more dossiers
                    </Button>
                  )}
                </div>
              )}
            </div>
          )}
        </RecordPanel>
      </div>
    </section>
  )
}

function ScreenRunCard({
  run,
  selected,
  onSelect,
}: {
  run: ScreenRunIndexView
  selected: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      className="rounded-lg border border-border bg-background/45 p-3 text-left transition-colors hover:border-primary/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      aria-pressed={selected}
      onClick={onSelect}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-xs font-semibold">
            {run.screenId} · revision {run.screenRevision}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Cutoff {formatTimestamp(run.asOf)} · {run.candidateCount} selected
          </p>
        </div>
        <StateLabel value={selected ? "selected" : "open"} />
      </div>
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        <EvidenceBlock label="Dataset identity" digest={run.datasetIdentity} />
        <EvidenceBlock label="Universe identity" digest={run.universeIdentity} />
      </div>
    </button>
  )
}

function CandidateCard({
  candidate,
  selected,
  onSelect,
}: {
  candidate: CandidateView
  selected: boolean
  onSelect: () => void
}) {
  return (
    <article
      className={`rounded-xl border bg-background/45 p-4 ${
        selected ? "border-primary/60" : "border-border"
      }`}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-primary">
            Rank {candidate.rank}
          </p>
          <h3 className="mt-1 truncate text-sm font-semibold" title={candidate.instrumentId}>
            {candidate.instrumentId}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Score {candidate.score} · selected {formatTimestamp(candidate.selectedAt)}
          </p>
          <EvidenceIdentity value={candidate.id} />
        </div>
        <StateLabel value={candidate.dataQuality} />
      </div>

      <dl className="mt-4 grid grid-cols-2 gap-2 text-xs">
        <CandidateFact
          label="Saved screen"
          value={`${candidate.screenId} · revision ${candidate.screenRevision}`}
        />
        <CandidateFact label="Screen run" value={candidate.screenRunId} />
        <CandidateFact label="Coverage" value={`${candidate.coverage}`} />
        <CandidateFact label="Liquidity" value={`${candidate.liquidity}`} />
      </dl>

      <div className="mt-4">
        <h4 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
          Score contributions
        </h4>
        <ul className="mt-2 grid gap-2">
          {candidate.scoreContributions.map((contribution) => (
            <li
              key={`${contribution.binding.name}:${contribution.binding.version}`}
              className="rounded-lg border border-border/60 px-3 py-2 text-xs"
            >
              <div className="flex items-center justify-between gap-3">
                <span>
                  {humanize(contribution.binding.name)} v{contribution.binding.version}
                </span>
                <span className="text-right text-muted-foreground">
                  observed {contribution.observed ?? "missing"} · contribution {contribution.contribution}
                </span>
              </div>
              <EvidenceIdentity value={digestHex(contribution.binding.semanticDigest)} />
            </li>
          ))}
        </ul>
      </div>

      {candidate.flags.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {candidate.flags.map((flag) => (
            <StateLabel key={flag} value={flag} />
          ))}
        </div>
      )}

      <div className="mt-4 grid gap-3 border-t border-border pt-3">
        <EvidenceBlock label="Candidate evidence" digest={candidate.evidenceIdentity} />
        <EvidenceBlock label="Portfolio impact revision" digest={candidate.portfolioRevision} />
      </div>
      <Button type="button" variant="outline" size="sm" className="mt-4" onClick={onSelect}>
        <BookOpenCheck aria-hidden="true" />
        {selected ? "Showing dossiers" : "Discover dossiers"}
      </Button>
    </article>
  )
}

function DossierCard({
  dossier,
  selected,
  onSelect,
}: {
  dossier: DecisionDossierView
  selected: boolean
  onSelect: () => void
}) {
  return (
    <article
      className={`mt-4 rounded-xl border bg-background/45 p-4 ${
        selected ? "border-primary/60" : "border-border"
      }`}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-semibold" title={dossier.instrumentId}>
            {dossier.instrumentId}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Assembled {formatTimestamp(dossier.assembledAt)}
          </p>
        </div>
        <StateLabel value={`${dossier.references.length} references`} />
      </div>

      <dl className="mt-4 grid gap-3 text-xs sm:grid-cols-2">
        <DossierFact label="Dossier" value={dossier.id} />
        <DossierFact label="Candidate" value={dossier.candidateId} />
        <DossierFact label="Model bundle" value={dossier.evidence.modelBundle ?? "Not bound"} />
        <DossierFact
          label="Fair-value decision"
          value={dossier.evidence.fairValueDecision ?? "Not bound"}
        />
      </dl>

      <div className="mt-4 rounded-lg border border-border/70 p-3">
        <div className="flex items-center gap-2 text-xs font-medium">
          <BriefcaseBusiness className="size-3.5 text-primary" aria-hidden="true" />
          Portfolio impact evidence
        </div>
        <EvidenceIdentity value={digestHex(dossier.evidence.portfolioRevision)} />
      </div>

      <div className="mt-4">
        <h4 className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
          Section references
        </h4>
        {dossier.references.length === 0 ? (
          <p className="mt-2 text-xs text-muted-foreground">No section references are recorded.</p>
        ) : (
          <ul className="mt-2 grid gap-2 sm:grid-cols-2">
            {dossier.references.map((reference, index) => (
              <li
                key={`${reference.section}:${index}`}
                className="rounded-lg border border-border/60 px-3 py-2"
              >
                <span className="text-xs font-medium">{humanize(reference.section)}</span>
                <EvidenceIdentity value={digestHex(reference.contentIdentity)} />
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="mt-4 border-t border-border pt-3">
        <span className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
          Dossier content identity
        </span>
        <EvidenceIdentity value={digestHex(dossier.evidence.contentIdentity)} />
      </div>
      <Button type="button" className="mt-4" variant="outline" size="sm" onClick={onSelect}>
        <BookOpenCheck aria-hidden="true" />
        {selected ? "Selected for investment target" : "Use for investment target"}
      </Button>
    </article>
  )
}

function RecordPanel({
  title,
  icon: Icon,
  children,
}: {
  title: string
  icon: typeof Search
  children: React.ReactNode
}) {
  return (
    <div className="rounded-xl border border-border bg-card/40 p-4">
      <div className="flex items-center gap-2">
        <Icon className="size-4 text-primary" aria-hidden="true" />
        <h3 className="text-sm font-semibold">{title}</h3>
      </div>
      {children}
    </div>
  )
}

function CandidateFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border/60 p-3">
      <dt className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate font-medium" title={value}>
        {value}
      </dd>
    </div>
  )
}

function DossierFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate font-mono text-[10px]" title={value}>{value}</dd>
    </div>
  )
}

function EvidenceBlock({
  label,
  digest,
}: {
  label: string
  digest: readonly number[] | null
}) {
  return (
    <div>
      <span className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</span>
      <EvidenceIdentity value={digestHex(digest)} />
    </div>
  )
}

function PromptState({ text }: { text: string }) {
  return (
    <div className="mt-4 rounded-xl border border-dashed border-border p-5 text-xs leading-5 text-muted-foreground">
      {text}
    </div>
  )
}

function PanelLoading() {
  return (
    <div className="mt-4 space-y-2" aria-label="Loading decision records">
      <Skeleton className="h-24 w-full" />
      <Skeleton className="h-16 w-full" />
    </div>
  )
}

function RecordError({
  title,
  error,
  retry,
}: {
  title: string
  error: unknown
  retry: () => void
}) {
  return (
    <Alert variant="destructive" className="mt-4">
      <AlertCircle aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        {messageFrom(error)}
        <Button type="button" variant="outline" size="sm" className="mt-2" onClick={retry}>
          Retry
        </Button>
      </AlertDescription>
    </Alert>
  )
}
