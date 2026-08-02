import { useQuery } from "@tanstack/react-query"
import { AlertCircle, RefreshCw } from "lucide-react"

import { messageFrom, useProduct } from "@/app/product-context"
import { productKeys, type ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import type { ProductTransport } from "@/lib/transport"

import { CandidateDossierWorkspace } from "./candidate-dossier"
import { parseDecisionScreens } from "./contracts"
import { DecisionBoundaries } from "./decision-boundaries"
import { SavedScreens } from "./saved-screens"
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
          <AlertTitle>Decision workspace unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button type="button" className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </DecisionsFrame>
    )
  }

  const hasDecisionReads = product.bootstrap.operations.some(
    (operation) => operation.name === "Decision.ListScreens",
  )
  if (!hasDecisionReads) {
    return (
      <DecisionsFrame>
        <Alert>
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Decision records are not exposed</AlertTitle>
          <AlertDescription>
            This installed service does not advertise the closed Decision read contract.
            Update or repair the installation before using this workspace.
          </AlertDescription>
        </Alert>
      </DecisionsFrame>
    )
  }

  return (
    <ReadyDecisions
      transport={product.transport}
      scope={product.bootstrap.runtime}
    />
  )
}

function ReadyDecisions({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const screens = useQuery({
    queryKey: productKeys.operation(scope, "decision", "screens", {
      limit: SCREEN_LIMIT,
    }),
    queryFn: async () =>
      parseDecisionScreens(
        await transport.query({ query: "decisionScreens", limit: SCREEN_LIMIT }),
      ),
  })

  return (
    <DecisionsFrame>
      <header className="flex flex-col gap-4 border-b border-border pb-6 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
            Evidence-bound research judgment
          </p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Decisions</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Follow durable screens into candidate evidence, global dossiers, and governed target
            revisions without collapsing observed data, modeled analysis, judgment, or execution.
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          onClick={() => void screens.refetch()}
          disabled={screens.isFetching}
        >
          <RefreshCw
            className={screens.isFetching ? "animate-spin" : undefined}
            aria-hidden="true"
          />
          Refresh screens
        </Button>
      </header>

      <div className="mt-5">
        <DecisionBoundaries />
      </div>

      {screens.isPending ? (
        <ScreensLoading />
      ) : screens.isError ? (
        <Alert variant="destructive" className="mt-6">
          <AlertCircle aria-hidden="true" />
          <AlertTitle>Saved screens could not be loaded</AlertTitle>
          <AlertDescription>
            {messageFrom(screens.error)}
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
        <SavedScreens screens={screens.data} />
      )}

      <CandidateDossierWorkspace transport={transport} scope={scope} />
      <TargetGovernanceWorkspace transport={transport} scope={scope} />
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
      <ScreensLoading />
    </DecisionsFrame>
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
