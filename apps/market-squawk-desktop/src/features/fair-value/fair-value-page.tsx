import { RefreshButton } from "@/components/ui/refresh-button"
import { CircleAlert } from "lucide-react"
import type { ReactNode } from "react"
import { Link, useSearchParams } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import type { ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { admittedAnalysisActionToken, type InvestmentAnalysis } from "@/features/opportunities/contracts"
import {
  EvidenceSummary,
  PriceRanges,
  ProductLists,
} from "@/features/opportunities/investment-brief"
import { SavedAnalysisHistory } from "@/features/opportunities/opportunities-read-experience"
import { formatMoney as money } from "@/features/opportunities/format"
import { useSavedInvestmentAnalyses, useSavedInvestmentAnalysis } from "@/features/opportunities/use-saved-investment-analysis"
import { useCursorNavigation } from "@/features/shared/cursor-navigation"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { ProductTransport } from "@/lib/transport"
import { formatProductTimestamp } from "@/lib/time"

import { ValuationEvidence } from "./valuation-evidence"

export function FairValuePage() {
  const product = useProduct()

  if (product.status === "loading") return <ValuationLoading />
  if (product.status === "error") {
    return <ValuationFrame>
      <Alert variant="destructive">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Valuation research is unavailable</AlertTitle>
        <AlertDescription>Market Squawk could not open this research. Try again, or review the app setup if the problem continues.</AlertDescription>
      </Alert>
      <Button type="button" className="mt-4" onClick={product.refresh}>Try again</Button>
    </ValuationFrame>
  }

  const capabilities = productCapabilitySet(product.bootstrap)
  return <ValuationFrame>
    <SavedValuationRead key={JSON.stringify(product.bootstrap.productSessionToken)}
      transport={product.transport} scope={product.bootstrap.productSessionToken}
      available={capabilities.has("decision_analysis_list") && capabilities.has("decision_analysis")} />
  </ValuationFrame>
}

function SavedValuationRead({ transport, scope, available }: {
  transport: ProductTransport
  scope: ProductScope
  available: boolean
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedAnalysis = searchParams.get("analysis")
  const actionToken = admittedAnalysisActionToken(requestedAnalysis)
  const navigation = useCursorNavigation()
  const analyses = useSavedInvestmentAnalyses({ transport, scope, available, after: navigation.after })
  const selected = useSavedInvestmentAnalysis({ transport, scope, available, actionToken })
  const select = (token: string | null) => setSearchParams((current) => {
    const next = new URLSearchParams(current)
    if (token === null) next.delete("analysis")
    else next.set("analysis", token)
    return next
  })

  if (!available) return <Alert>
    <CircleAlert aria-hidden="true" />
    <AlertTitle>Saved valuation research is unavailable</AlertTitle>
    <AlertDescription>Saved investment analyses cannot be opened right now. Try again later.</AlertDescription>
  </Alert>

  return <>
    {requestedAnalysis !== null && actionToken === null ? <Alert variant="destructive">
      <CircleAlert aria-hidden="true" />
      <AlertTitle>This saved valuation could not be opened</AlertTitle>
      <AlertDescription>Choose a saved analysis from your history.</AlertDescription>
    </Alert> : actionToken === null ? <div className="rounded-xl border border-dashed border-border bg-card/30 p-6">
      <h2 className="text-sm font-semibold">Select a saved analysis</h2>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">Choose a history item to review its original valuation methods, price ranges and evidence.</p>
    </div> : <>
      <div className="mb-4 flex justify-end">
        <Button type="button" variant="outline" size="sm" onClick={() => select(null)}>Close valuation</Button>
      </div>
      {selected.isPending ? <ValuationSkeleton /> : selected.isError ? <Alert variant="destructive">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Saved valuation could not be loaded</AlertTitle>
        <AlertDescription>Try again later.
          <Button type="button" variant="outline" size="sm" className="mt-2" onClick={() => void selected.refetch()}>Retry valuation</Button>
        </AlertDescription>
      </Alert> : <SavedValuation key={actionToken} analysis={selected.data} refreshing={selected.isFetching} onRefresh={() => void selected.refetch()} />}
    </>}
    <SavedAnalysisHistory analyses={analyses} navigation={navigation} selectedActionToken={actionToken} onSelect={select} openLabel="Open valuation" />
  </>
}

function SavedValuation({ analysis, refreshing, onRefresh }: {
  analysis: InvestmentAnalysis
  refreshing: boolean
  onRefresh: () => void
}) {
  return <section className="rounded-xl border border-border bg-card/45 p-5" aria-labelledby="saved-valuation-title">
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">Saved investment analysis</p>
        <h2 id="saved-valuation-title" className="mt-2 text-xl font-semibold">{[analysis.investment.symbol, analysis.investment.name].filter(Boolean).join(" · ") || "Saved valuation"}</h2>
        <p className="mt-1 text-xs text-muted-foreground">{analysis.portfolioLabel}</p>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button asChild variant="outline" size="sm"><Link to={`/opportunities?analysis=${encodeURIComponent(analysis.actionToken)}`}>Open Investment Brief</Link></Button>
        <RefreshButton label="Refresh valuation" refreshing={refreshing} onClick={onRefresh} disabled={refreshing} />
      </div>
    </div>
    <p className="mt-4 text-sm leading-6 text-muted-foreground">{analysis.recommendation.summary}</p>
    <dl className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
      <ValuationFact label="Information current through" value={<time dateTime={analysis.horizon.informationCurrentThrough} title={analysis.horizon.informationCurrentThrough}>{formatProductTimestamp(analysis.horizon.informationCurrentThrough)}</time>} />
      <ValuationFact label="Analysis horizon" value={<time dateTime={analysis.horizon.endsAt} title={analysis.horizon.endsAt}>{formatProductTimestamp(analysis.horizon.endsAt)}</time>} />
      <ValuationFact label="Analysis expires" value={<time dateTime={analysis.horizon.expiresAt} title={analysis.horizon.expiresAt}>{formatProductTimestamp(analysis.horizon.expiresAt)}</time>} />
      <ValuationFact label="Saved current price per instrument unit" value={analysis.priceSummary.current ? money(analysis.priceSummary.current) : "Unavailable"} />
      <ValuationFact label="Saved fair value per instrument unit" value={analysis.priceSummary.fairValue ? money(analysis.priceSummary.fairValue) : "Unavailable"} />
      <ValuationFact label="Reporting currency" value={analysis.currency} />
    </dl>
    <ValuationEvidence analysis={analysis} />
    <PriceRanges analysis={analysis} />
    <ProductLists analysis={analysis} />
    <EvidenceSummary analysis={analysis} />
  </section>
}

function ValuationFact({ label, value }: { label: string; value: ReactNode }) {
  return <div><dt className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</dt><dd className="mt-1 text-xs leading-5">{value}</dd></div>
}

function ValuationFrame({ children }: { children: ReactNode }) {
  return <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
    <header className="border-b border-border pb-6">
      <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">Advanced · Investment research</p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight">Valuation &amp; targets</h1>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">Review what an investment may be worth, its saved entry and exit ranges, and what could change the outlook. Estimates retain their original information cutoff and are not guaranteed returns.</p>
    </header>
    <div className="mt-5">{children}</div>
  </main>
}

function ValuationSkeleton() {
  return <div className="space-y-4" aria-label="Loading saved valuation">
    <Skeleton className="h-24 rounded-xl" />
    <div className="grid gap-4 lg:grid-cols-2"><Skeleton className="h-36 rounded-xl" /><Skeleton className="h-36 rounded-xl" /></div>
  </div>
}

function ValuationLoading() {
  return <ValuationFrame><ValuationSkeleton /></ValuationFrame>
}
