import { useQuery } from "@tanstack/react-query"
import {
  ChevronRight,
  CircleAlert,
  History,
  RefreshCw,
} from "lucide-react"
import { useSearchParams } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { ProductTransport } from "@/lib/transport"

import {
  admittedAnalysisActionToken,
  admittedSavedScreenId,
  parseSavedScreenProduct,
  type InvestmentAnalysisLocator,
} from "./contracts"
import {
  analysisOutcomeTone,
  BriefError,
  BriefLoading,
  InvestmentBrief,
  formatProductTimestamp,
  locatorOutcomeLabel,
} from "./investment-brief"
import { AnalysisActivity } from "./analysis-activity"
import { AnalysisLaunch } from "./analysis-launch"

import { CursorNavigation, useCursorNavigation } from "../shared/cursor-navigation"

import { useSavedInvestmentAnalyses, useSavedInvestmentAnalysis } from "./use-saved-investment-analysis"

export function OpportunitiesReadExperience({
  transport,
  scope,
  readAvailable,
}: {
  transport: ProductTransport
  scope: ProductScope
  readAvailable: boolean
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedAnalysis = searchParams.get("analysis")
  const selectedActionToken = admittedAnalysisActionToken(requestedAnalysis)
  const product = useProduct()
  const requestedScreenValue = searchParams.get("screenId")
  const requestedScreenId = admittedSavedScreenId(requestedScreenValue)
  const screenReadAvailable =
    product.status === "ready" &&
    hasProductCapability(product.bootstrap, "decision_screen_list")
  const selectedScreen = useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.GetScreen", {
      screenId: requestedScreenId,
    }),
    queryFn: async ({ signal }) => {
      const screenId = requestedScreenId
      if (screenId === null) {
        throw new Error("Select a saved screen before opening it.")
      }
      return parseSavedScreenProduct(
        await transport.query({ query: "decisionScreen", screenId }, { signal }),
        screenId,
      )
    },
    enabled:
      requestedScreenValue !== null &&
      requestedScreenId !== null &&
      screenReadAvailable,
  })
  const navigation = useCursorNavigation()
  const analyses = useSavedInvestmentAnalyses({ transport, scope, available: readAvailable, after: navigation.after })
  const selected = useSavedInvestmentAnalysis({ transport, scope, available: readAvailable, actionToken: selectedActionToken })
  const trackRecordAvailable =
    product.status === "ready" &&
    hasProductCapability(product.bootstrap, "decision_recommendation_history")

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
            Saved investment analysis
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Opportunities</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Find opportunities with your current investment settings, or review saved analyses.
            Completed searches retain their saved ordering; history is shown in creation order.
          </p>
        </div>
        <div className="max-w-sm rounded-lg border border-border bg-card/45 p-3">
          <AnalysisLaunch transport={transport} scope={scope} />
        </div>
      </header>

      <AnalysisActivity transport={transport} scope={scope} />

      {requestedScreenValue !== null ? (
        <SelectedSavedScreen
          invalidIdentity={requestedScreenId === null}
          available={screenReadAvailable}
          pending={selectedScreen.isPending}
          error={selectedScreen.isError}
          screen={selectedScreen.data ?? null}
          onRetry={() => void selectedScreen.refetch()}
        />
      ) : null}

      {readAvailable ? (
        <div className="mt-8">
          {requestedAnalysis !== null && selectedActionToken === null ? (
            <Alert variant="destructive">
              <CircleAlert aria-hidden="true" />
              <AlertTitle>This investment brief could not be opened</AlertTitle>
              <AlertDescription>Choose a saved analysis from your history.</AlertDescription>
            </Alert>
          ) : selectedActionToken === null ? (
            <SelectBriefPrompt />
          ) : selected.isPending ? (
            <BriefLoading />
          ) : selected.isError ? (
            <BriefError onRetry={() => void selected.refetch()} />
          ) : (
            <InvestmentBrief
              key={selectedActionToken}
              analysis={selected.data}
              transport={transport}
              scope={scope}
              trackRecordAvailable={trackRecordAvailable}
              refreshing={selected.isFetching}
              onRefresh={() => void selected.refetch()}
            />
          )}
        </div>
      ) : null}

      {!readAvailable ? (
        <Alert className="mt-6">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Investment-analysis history is unavailable</AlertTitle>
          <AlertDescription>
            Saved investment analyses cannot be opened right now. Try again later.
          </AlertDescription>
        </Alert>
      ) : (
        <SavedAnalysisHistory analyses={analyses} navigation={navigation}
          selectedActionToken={selectedActionToken} onSelect={(actionToken) => setSearchParams((current) => {
            const next = new URLSearchParams(current)
            next.set("analysis", actionToken)
            return next
          })} />
      )}


    </>
  )
}

function SelectedSavedScreen({
  invalidIdentity,
  available,
  pending,
  error,
  screen,
  onRetry,
}: {
  invalidIdentity: boolean
  available: boolean
  pending: boolean
  error: boolean
  screen: ReturnType<typeof parseSavedScreenProduct> | null
  onRetry: () => void
}) {
  if (invalidIdentity || !available) {
    return (
      <Alert className="mt-6">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Saved screen is unavailable</AlertTitle>
        <AlertDescription>
          This saved screen cannot be opened right now. Choose another saved screen or try again.
        </AlertDescription>
      </Alert>
    )
  }
  if (pending) {
    return <Skeleton className="mt-6 h-28 w-full rounded-xl" aria-label="Opening saved screen" />
  }
  if (error || screen === null) {
    return (
      <Alert variant="destructive" className="mt-6">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Saved screen could not be opened</AlertTitle>
        <AlertDescription>
          Try again later.
          <Button type="button" variant="outline" size="sm" className="mt-2" onClick={onRetry}>
            Try again
          </Button>
        </AlertDescription>
      </Alert>
    )
  }
  return (
    <section
      className="mt-6 rounded-xl border border-primary/35 bg-primary/5 p-5"
      aria-labelledby="selected-saved-screen-title"
    >
      <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
        Open saved screen
      </p>
      <h2 id="selected-saved-screen-title" className="mt-2 text-lg font-semibold">
        {screen.title}
      </h2>
      <p className="mt-1 text-sm text-muted-foreground">{screen.subtitle}</p>
    </section>
  )
}

function AnalysisHistoryCard({
  analysis,
  selected,
  onSelect,
  openLabel,
}: {
  analysis: InvestmentAnalysisLocator
  selected: boolean
  onSelect: () => void
  openLabel: string
}) {
  const tone = analysisOutcomeTone(analysis.recommendation)
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
            Investment analysis
          </p>
          <p className="mt-2 text-sm font-semibold">
            {analysis.investment.symbol ?? analysis.investment.name ?? "Investment brief"}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            {[analysis.investment.name, analysis.portfolioLabel, analysis.currency]
              .filter(Boolean)
              .join(" · ")}
          </p>
        </div>
        <span className={`shrink-0 rounded-full border px-2 py-1 text-[10px] ${toneClass}`}>
          {analysis.recommendation.kind === "action"
            ? "Action"
            : analysis.recommendation.kind === "abstain"
              ? "Abstain"
              : "Unavailable"}
        </span>
      </div>
      <p className="mt-4 text-xs font-medium">
        {locatorOutcomeLabel(analysis.recommendation)}
      </p>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-3 sm:grid-cols-3">
        <CardFact
          label="Information current through"
          value={formatProductTimestamp(analysis.horizon.informationCurrentThrough)}
        />
        <CardFact label="Horizon" value={formatProductTimestamp(analysis.horizon.endsAt)} />
        <CardFact label="Expires" value={formatProductTimestamp(analysis.horizon.expiresAt)} />
      </dl>
      <div className="mt-4 flex items-center justify-end gap-1 text-xs font-medium text-primary">
        {openLabel}
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
      <p className="text-sm font-semibold">No saved investment analyses</p>
      <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
        No saved action, abstain, or unavailable analysis is ready to review yet.
      </p>
    </div>
  )
}

function SelectBriefPrompt() {
  return (
    <div className="rounded-xl border border-dashed border-border bg-card/30 p-6">
      <p className="text-sm font-semibold">Select a saved analysis</p>
      <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
        Choose a history item to open its Investment Brief. Market Squawk will keep your selection
        instead of silently switching to a newer analysis.
      </p>
    </div>
  )
}

function HistoryLoading() {
  return (
    <div className="mt-5 grid gap-3 xl:grid-cols-2" aria-label="Loading saved analyses">
      <Skeleton className="h-52 w-full" />
      <Skeleton className="h-52 w-full" />
    </div>
  )
}

function HistoryError({ detail, onRetry }: { detail: string; onRetry: () => void }) {
  return (
    <Alert variant="destructive" className="mt-5">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>Saved analysis history could not be loaded</AlertTitle>
      <AlertDescription>
        {detail}
        <Button type="button" variant="outline" size="sm" className="mt-2" onClick={onRetry}>
          Retry history read
        </Button>
      </AlertDescription>
    </Alert>
  )
}

export function SavedAnalysisHistory({ analyses, navigation, selectedActionToken, onSelect, openLabel = "Open brief" }: {
  analyses: ReturnType<typeof useSavedInvestmentAnalyses>
  navigation: ReturnType<typeof useCursorNavigation>
  selectedActionToken: string | null
  onSelect: (actionToken: string) => void
  openLabel?: string
}) {
  const history = analyses.data?.analyses ?? []
  return (
    <section className="mt-6" aria-labelledby="opportunity-history-title">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <History className="size-4 text-primary" aria-hidden="true" />
            <h2 id="opportunity-history-title" className="text-lg font-semibold">
              Saved analysis history
            </h2>
          </div>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            Generated, no-action, and unavailable outcomes are all kept. Their order is not a
            quality score or recommendation ranking.
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
          detail="Saved analyses could not be retrieved. Try again."
          onRetry={() => void analyses.refetch()}
        />
      ) : history.length === 0 ? (
        <EmptyHistory />
      ) : (
        <>
          <p className="mt-5 text-xs text-muted-foreground">
            {history.length.toLocaleString("en-US")} saved analysis
            {history.length === 1 ? "" : "es"} loaded in creation order.
          </p>
          <div className="mt-3 grid gap-3 xl:grid-cols-2">
            {history.map((analysis) => (
              <AnalysisHistoryCard
                key={analysis.actionToken}
                analysis={analysis}
                selected={analysis.actionToken === selectedActionToken}
                onSelect={() => onSelect(analysis.actionToken)}
                openLabel={openLabel}
              />
            ))}
          </div>

          {analyses.isError ? (
            <HistoryError
              detail="More saved analyses could not be retrieved. Try again."
              onRetry={() => void analyses.refetch()}
            />
          ) : null}

        </>
      )}
      {analyses.data || navigation.canGoPrevious || analyses.isError ? (
        <CursorNavigation navigation={navigation} next={analyses.data?.completeness === "truncated" ? analyses.data.nextAfterActionToken : null} busy={analyses.isFetching}
          onRestart={() => { if (navigation.after === undefined) void analyses.refetch() }} />
      ) : null}
    </section>
  )
}
