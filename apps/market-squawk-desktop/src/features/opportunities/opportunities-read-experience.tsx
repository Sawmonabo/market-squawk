import { useMemo, useState } from "react"
import { useInfiniteQuery, useQuery } from "@tanstack/react-query"
import {
  ChevronRight,
  CircleAlert,
  History,
  RefreshCw,
  Search,
} from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { useAnalyticalControllerStatus } from "@/features/advanced/use-analytical-profile"
import type { ProductTransport } from "@/lib/transport"

import {
  parseInvestmentAnalysis,
  parseInvestmentAnalysisPage,
  parseRecommendationTrackRecord,
  recommendationTrackRecordRequestForAnalysis,
  type InvestmentAnalysisLocator,
} from "./contracts"
import {
  analysisOutcomeTone,
  BriefLoading,
  InvestmentBrief,
  locatorOutcomeLabel,
  type TrackRecordPresentation,
} from "./investment-brief"
import { formatUnixNanos } from "./format"

const ANALYSIS_PAGE_LIMIT = 24

export function OpportunitiesReadExperience({
  transport,
  scope,
  readAvailable,
}: {
  transport: ProductTransport
  scope: ProductScope
  readAvailable: boolean
}) {
  const [selectedAnalysisId, setSelectedAnalysisId] = useState<string | null>(null)
  const product = useProduct()
  const controller = useAnalyticalControllerStatus(transport, scope)
  const trackRecordOperationAvailable =
    product.status === "ready" &&
    product.bootstrap.operations.some(
      (operation) =>
        operation.readOnly &&
        operation.name === "Decision.GetRecommendationTrackRecord",
    )
  const analyses = useInfiniteQuery({
    queryKey: productKeys.operation(
      scope,
      "decision",
      "Decision.ListInvestmentAnalyses",
      { limit: ANALYSIS_PAGE_LIMIT },
    ),
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) => {
      const request = {
        ...(pageParam ? { afterAnalysisId: pageParam } : {}),
        limit: ANALYSIS_PAGE_LIMIT,
      }
      return parseInvestmentAnalysisPage(
        await transport.query({
          query: "decisionInvestmentAnalyses",
          ...request,
        }),
        request,
      )
    },
    getNextPageParam: (page) =>
      page.completeness === "truncated"
        ? (page.nextAfterAnalysisId ?? undefined)
        : undefined,
    enabled: readAvailable,
  })
  const history = useMemo(
    () => analyses.data?.pages.flatMap((page) => page.analyses) ?? [],
    [analyses.data],
  )
  const repeatedIdentity =
    new Set(history.map((analysis) => analysis.analysisId)).size !== history.length
  const selectedIsRetained =
    selectedAnalysisId !== null &&
    history.some((analysis) => analysis.analysisId === selectedAnalysisId)
  const selected = useQuery({
    queryKey: productKeys.operation(
      scope,
      "decision",
      "Decision.GetInvestmentAnalysis",
      { analysisId: selectedAnalysisId },
    ),
    queryFn: async () => {
      const analysisId = selectedAnalysisId
      if (analysisId === null) {
        throw new Error("Select one retained analysis before opening its brief.")
      }
      return parseInvestmentAnalysis(
        await transport.query({
          query: "decisionInvestmentAnalysis",
          analysisId,
        }),
        analysisId,
      )
    },
    enabled: readAvailable && selectedIsRetained && !repeatedIdentity,
  })
  const trackRecordRequestAvailability = useMemo(() => {
    if (!selected.data || selected.dataUpdatedAt <= 0) return null
    const evaluatedAtUnixNanos = (
      BigInt(selected.dataUpdatedAt) * 1_000_000n
    ).toString()
    return recommendationTrackRecordRequestForAnalysis(
      selected.data,
      evaluatedAtUnixNanos,
    )
  }, [selected.data, selected.dataUpdatedAt])
  const trackRecordRequest =
    trackRecordRequestAvailability?.kind === "available"
      ? trackRecordRequestAvailability.request
      : null
  const trackRecord = useQuery({
    queryKey: productKeys.operation(
      scope,
      "decision",
      "Decision.GetRecommendationTrackRecord",
      trackRecordRequest ?? {
        unavailable:
          trackRecordRequestAvailability?.kind === "unavailable"
            ? trackRecordRequestAvailability.reason
            : "analysis_not_loaded",
      },
    ),
    queryFn: async () => {
      const request = trackRecordRequest
      if (!request) {
        throw new Error(
          "The selected analysis does not expose a callable track-record binding.",
        )
      }
      return parseRecommendationTrackRecord(
        await transport.query({
          query: "decisionRecommendationTrackRecord",
          ...request,
        }),
        request,
      )
    },
    enabled:
      readAvailable &&
      selectedIsRetained &&
      !repeatedIdentity &&
      trackRecordOperationAvailable &&
      trackRecordRequest !== null,
  })
  const trackRecordPresentation: TrackRecordPresentation =
    !trackRecordOperationAvailable
      ? {
          state: "unavailable",
          detail:
            "This installed service generation does not advertise the exact recommendation track-record read.",
        }
      : trackRecordRequestAvailability?.kind === "unavailable"
        ? {
            state: "unavailable",
            detail: trackRecordUnavailableDetail(
              trackRecordRequestAvailability.reason,
            ),
          }
        : trackRecord.isPending
          ? { state: "loading" }
          : trackRecord.isError
            ? {
                state: "error",
                detail: messageFrom(trackRecord.error),
                onRetry: () => void trackRecord.refetch(),
              }
            : trackRecord.data
              ? { state: "ready", value: trackRecord.data }
              : { state: "loading" }

  return (
    <>
      <header
        className={
          "flex flex-col gap-5 border-b border-border pb-6 " +
          "lg:flex-row lg:items-end lg:justify-between"
        }
      >
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Retained, evidence-bound investment analysis
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Opportunities</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review investment analyses already retained by Market Squawk. History stays in its
            durable append order; this page does not rank instruments or claim that a new search
            has run.
          </p>
        </div>
        <div className="max-w-sm rounded-lg border border-border bg-card/45 p-3">
          <Button
            type="button"
            disabled
            aria-describedby="find-opportunities-readiness"
            className="w-full sm:w-auto"
          >
            <Search aria-hidden="true" />
            Find opportunities
          </Button>
          <p
            id="find-opportunities-readiness"
            className="mt-2 text-xs leading-5 text-muted-foreground"
          >
            {controller.data
              ? `${controller.data.activeProfile.displayName} is active, but ${controller.data.workflowReadiness.blockers[0]?.detail ?? "the required analysis capabilities are unavailable"} This control starts no work.`
              : controller.isError
                ? `The durable Desktop analytical profile could not be read: ${messageFrom(controller.error)} This control starts no work.`
                : "Checking the durable analytical profile and workflow blockers. This control starts no work."}
          </p>
        </div>
      </header>

      {!readAvailable ? (
        <Alert className="mt-6">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Investment-analysis history is unavailable</AlertTitle>
          <AlertDescription>
            This installed service generation does not advertise both exact investment-analysis
            read operations. Update or repair the installation; the desktop will not substitute a
            different decision record or infer a latest result.
          </AlertDescription>
        </Alert>
      ) : (
        <section className="mt-6" aria-labelledby="opportunity-history-title">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-2">
                <History className="size-4 text-primary" aria-hidden="true" />
                <h2 id="opportunity-history-title" className="text-lg font-semibold">
                  Retained analysis history
                </h2>
              </div>
              <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
                Generated, no-action, and unavailable outcomes are all kept. Their storage order
                is not a quality score or recommendation ranking.
              </p>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => void analyses.refetch()}
              disabled={analyses.isFetching}
            >
              <RefreshCw
                className={analyses.isFetching ? "animate-spin" : undefined}
                aria-hidden="true"
              />
              Refresh history
            </Button>
          </div>

          {analyses.isPending ? (
            <HistoryLoading />
          ) : analyses.isError && history.length === 0 ? (
            <HistoryError
              detail={messageFrom(analyses.error)}
              onRetry={() => void analyses.refetch()}
            />
          ) : repeatedIdentity ? (
            <Alert variant="destructive" className="mt-5">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>Analysis history could not be reconciled</AlertTitle>
              <AlertDescription>
                The exact append-order pages repeated a stable analysis identity. The desktop
                will not hide or reorder the conflict.
              </AlertDescription>
            </Alert>
          ) : history.length === 0 ? (
            <EmptyHistory />
          ) : (
            <>
              <p className="mt-5 text-xs text-muted-foreground">
                {history.length.toLocaleString("en-US")} retained analysis
                {history.length === 1 ? "" : "es"} loaded in append order.
              </p>
              <div className="mt-3 grid gap-3 xl:grid-cols-2">
                {history.map((analysis) => (
                  <AnalysisHistoryCard
                    key={analysis.analysisId}
                    analysis={analysis}
                    selected={analysis.analysisId === selectedAnalysisId}
                    onSelect={() => setSelectedAnalysisId(analysis.analysisId)}
                  />
                ))}
              </div>

              {analyses.isError ? (
                <HistoryError
                  detail={
                    `An exact retained page could not be loaded: ` +
                    messageFrom(analyses.error)
                  }
                  onRetry={() => void analyses.refetch()}
                />
              ) : null}

              {analyses.hasNextPage ? (
                <div className="mt-5 flex justify-center">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void analyses.fetchNextPage()}
                    disabled={analyses.isFetchingNextPage}
                  >
                    {analyses.isFetchingNextPage ? (
                      <RefreshCw className="animate-spin" aria-hidden="true" />
                    ) : (
                      <ChevronRight aria-hidden="true" />
                    )}
                    Load the next retained page
                  </Button>
                </div>
              ) : null}
            </>
          )}
        </section>
      )}

      {readAvailable && !repeatedIdentity ? (
        <div className="mt-8">
          {!selectedIsRetained ? (
            <SelectBriefPrompt />
          ) : selected.isPending ? (
            <BriefLoading />
          ) : selected.isError ? (
            <Alert variant="destructive">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>The Investment Brief could not be loaded</AlertTitle>
              <AlertDescription>
                {messageFrom(selected.error)}
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="mt-2"
                  onClick={() => void selected.refetch()}
                >
                  Retry exact analysis
                </Button>
              </AlertDescription>
            </Alert>
          ) : (
            <InvestmentBrief
              analysis={selected.data}
              trackRecord={trackRecordPresentation}
              refreshing={selected.isFetching || trackRecord.isFetching}
              onRefresh={() => void selected.refetch()}
            />
          )}
        </div>
      ) : null}
    </>
  )
}

function trackRecordUnavailableDetail(
  reason:
    | "analysis_not_published"
    | "profile_digest_algorithm_unsupported"
    | "profile_identifier_unsupported",
): string {
  switch (reason) {
    case "analysis_not_published":
      return "This retained analysis has not been published under an exact analytical profile, so no profile-bound track record can be requested."
    case "profile_digest_algorithm_unsupported":
      return "The publication uses a profile digest algorithm that the current track-record operation cannot accept."
    case "profile_identifier_unsupported":
      return "The publication's analytical-profile binding is not accepted by the current track-record request contract."
  }
}

function AnalysisHistoryCard({
  analysis,
  selected,
  onSelect,
}: {
  analysis: InvestmentAnalysisLocator
  selected: boolean
  onSelect: () => void
}) {
  const tone = analysisOutcomeTone(analysis.outcome.kind)
  const toneClass =
    tone === "good"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
      : tone === "attention"
        ? "border-amber-400/30 bg-amber-400/10 text-amber-100"
        : "border-border bg-muted/40 text-muted-foreground"

  return (
    <button
      type="button"
      onClick={onSelect}
      aria-pressed={selected}
      className={`rounded-xl border p-4 text-left transition-colors ${
        selected
          ? "border-primary/50 bg-primary/5"
          : "border-border bg-card/40 hover:border-primary/25 hover:bg-card/65"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[9px] uppercase tracking-[0.14em] text-muted-foreground">
            Analysis {analysis.analysisId.slice(0, 12)}…
          </p>
          <p className="mt-2 break-all text-sm font-semibold">{analysis.instrumentId}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {analysis.currency} · account {analysis.accountId}
          </p>
        </div>
        <span className={`shrink-0 rounded-full border px-2 py-1 text-[10px] ${toneClass}`}>
          {analysis.outcome.kind === "generated"
            ? "Generated"
            : analysis.outcome.kind === "no_action"
              ? "No action"
              : "Unavailable"}
        </span>
      </div>
      <p className="mt-4 text-xs font-medium">{locatorOutcomeLabel(analysis.outcome)}</p>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-3">
        <CardFact label="Evidence as of" value={formatUnixNanos(analysis.asOf)} />
        <CardFact label="Horizon" value={formatUnixNanos(analysis.horizonAt)} />
        <CardFact label="Expires" value={formatUnixNanos(analysis.expiresAt)} />
      </dl>
      <div className="mt-4 flex items-center justify-end gap-1 text-xs font-medium text-primary">
        Open exact brief
        <ChevronRight className="size-3" aria-hidden="true" />
      </div>
    </button>
  )
}

function CardFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-[10px] leading-4">{value}</dd>
    </div>
  )
}

function EmptyHistory() {
  return (
    <div className="mt-5 rounded-xl border border-dashed border-border bg-card/30 p-6">
      <p className="text-sm font-semibold">No retained investment analyses</p>
      <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
        No Generated, No action, or Unavailable result has been stored. This does not mean a
        search ran and found nothing; the guided finding workflow is not available from this page.
      </p>
    </div>
  )
}

function SelectBriefPrompt() {
  return (
    <div className="rounded-xl border border-dashed border-border bg-card/30 p-6">
      <p className="text-sm font-semibold">Select one retained analysis</p>
      <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
        Choose a history item to request that exact analysis identity and open its Investment
        Brief. The desktop will not substitute the newest record.
      </p>
    </div>
  )
}

function HistoryLoading() {
  return (
    <div className="mt-5 grid gap-3 xl:grid-cols-2" aria-label="Loading retained analyses">
      <Skeleton className="h-52 w-full" />
      <Skeleton className="h-52 w-full" />
    </div>
  )
}

function HistoryError({ detail, onRetry }: { detail: string; onRetry: () => void }) {
  return (
    <Alert variant="destructive" className="mt-5">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>Retained analysis history could not be loaded</AlertTitle>
      <AlertDescription>
        {detail}
        <Button type="button" variant="outline" size="sm" className="mt-2" onClick={onRetry}>
          Retry history read
        </Button>
      </AlertDescription>
    </Alert>
  )
}
