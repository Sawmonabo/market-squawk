import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { AlertCircle, RefreshCw } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { OpportunitiesReadExperience } from "@/features/opportunities"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { ProductTransport } from "@/lib/transport"

import { CandidateDossierWorkspace } from "./candidate-dossier"
import { parseDecisionScreens, type DecisionDossierView } from "./contracts"
import { DecisionBoundaries } from "./decision-boundaries"
import { SavedScreens } from "./saved-screens"
import { ScreenBuilder } from "./screen-builder"
import { TargetGovernanceWorkspace } from "./target-governance"

const SCREEN_LIMIT = 100

export function DecisionsPage() {
  const product = useProduct()

  if (product.status === "loading") return <DecisionsLoading />
  if (product.status === "error") {
    return (
      <DecisionsFrame>
        <Alert variant="destructive">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Opportunities are unavailable</AlertTitle>
          <AlertDescription>
            Market Squawk could not open this workspace. Try again, and check Logs if the problem
            continues.
          </AlertDescription>
        </Alert>
        <Button type="button" className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </DecisionsFrame>
    )
  }

  const capabilities = productCapabilitySet(product.bootstrap)

  return (
    <ReadyDecisions
      transport={product.transport}
      scope={product.bootstrap.runtime}
      investmentReadsAvailable={
        capabilities.has("decision_analysis_list") &&
        capabilities.has("decision_analysis")
      }
      manualAnalysisAvailable={capabilities.has("decision_screen_list")}
    />
  )
}

function ReadyDecisions({
  transport,
  scope,
  investmentReadsAvailable,
  manualAnalysisAvailable,
}: {
  transport: ProductTransport
  scope: ProductScope
  investmentReadsAvailable: boolean
  manualAnalysisAvailable: boolean
}) {
  const [targetDossier, setTargetDossier] = useState<DecisionDossierView | null>(null)
  const screens = useQuery({
    queryKey: productKeys.operation(scope, "decision", "screens", {
      limit: SCREEN_LIMIT,
    }),
    queryFn: async () =>
      parseDecisionScreens(
        await transport.query({ query: "decisionScreens", limit: SCREEN_LIMIT }),
      ),
    enabled: manualAnalysisAvailable,
  })

  return (
    <DecisionsFrame>
      <OpportunitiesReadExperience
        transport={transport}
        scope={scope}
        readAvailable={investmentReadsAvailable}
      />

      <details className="mt-10 rounded-xl border border-border bg-card/35 p-5">
        <summary
          className={
            "cursor-pointer list-none focus-visible:outline-none " +
            "focus-visible:ring-2 focus-visible:ring-ring"
          }
        >
          <span className="text-base font-semibold">Advanced manual analysis</span>
          <span className="mt-1 block max-w-3xl text-xs leading-5 text-muted-foreground">
            Build and inspect manual screens, candidate dossiers, and governed target revisions.
            These expert tools remain separate from retained Investment Briefs.
          </span>
        </summary>

        <div className="mt-5 border-t border-border pt-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
                Guided research judgment
              </p>
              <h2 className="mt-1 text-lg font-semibold">Manual screens and targets</h2>
            </div>
            <Button
              type="button"
              variant="outline"
              onClick={() => void screens.refetch()}
              disabled={!manualAnalysisAvailable || screens.isFetching}
            >
              <RefreshCw
                className={screens.isFetching ? "animate-spin" : undefined}
                aria-hidden="true"
              />
              Refresh screens
            </Button>
          </div>

          <div className="mt-5">
            <DecisionBoundaries />
          </div>

          {!manualAnalysisAvailable ? (
            <Alert className="mt-6">
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Manual analysis is unavailable</AlertTitle>
              <AlertDescription>
                This installation cannot open saved screens yet. Update or repair Market Squawk
                before using the advanced manual workspace.
              </AlertDescription>
            </Alert>
          ) : screens.isPending ? (
            <ScreensLoading />
          ) : screens.isError ? (
            <Alert variant="destructive" className="mt-6">
              <AlertCircle aria-hidden="true" />
              <AlertTitle>Saved screens could not be loaded</AlertTitle>
              <AlertDescription>
                Market Squawk could not retrieve your saved screens. Retry, and check Logs if the
                problem continues.
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="mt-2"
                  onClick={() => void screens.refetch()}
                >
                  Retry
                </Button>
              </AlertDescription>
            </Alert>
          ) : (
            <>
              <ScreenBuilder
                transport={transport}
                scope={scope}
                screens={screens.data}
                onSaved={async () => {
                  await screens.refetch()
                }}
              />
              <SavedScreens screens={screens.data} />
            </>
          )}

          {manualAnalysisAvailable ? (
            <>
              <CandidateDossierWorkspace
                transport={transport}
                scope={scope}
                selectedTargetDossierId={targetDossier?.id ?? null}
                onSelectTargetDossier={setTargetDossier}
              />
              <TargetGovernanceWorkspace
                transport={transport}
                scope={scope}
                dossier={targetDossier}
              />
            </>
          ) : null}
        </div>
      </details>
    </DecisionsFrame>
  )
}

function DecisionsFrame({ children }: { children: React.ReactNode }) {
  return <main className="mx-auto w-full max-w-[1180px] px-4 py-6 sm:px-6 lg:px-8">{children}</main>
}

function DecisionsLoading() {
  return (
    <DecisionsFrame>
      <Skeleton className="h-4 w-36" />
      <Skeleton className="mt-3 h-10 w-56" />
      <Skeleton className="mt-3 h-5 w-full max-w-2xl" />
      <OpportunityLoading />
    </DecisionsFrame>
  )
}

function OpportunityLoading() {
  return (
    <div className="mt-6 grid gap-3 xl:grid-cols-2" aria-label="Loading Opportunities">
      <Skeleton className="h-52 w-full" />
      <Skeleton className="h-52 w-full" />
    </div>
  )
}

function ScreensLoading() {
  return (
    <div className="mt-6 grid gap-3 xl:grid-cols-2" aria-label="Loading saved screens">
      <Skeleton className="h-56 w-full" />
      <Skeleton className="h-56 w-full" />
    </div>
  )
}
