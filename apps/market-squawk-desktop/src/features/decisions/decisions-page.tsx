import { AlertCircle } from "lucide-react"

import { useProduct } from "@/app/product-context"
import type { ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { OpportunitiesReadExperience } from "@/features/opportunities"
import { productCapabilitySet } from "@/lib/product-capabilities"
import type { ProductTransport } from "@/lib/transport"

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
      scope={product.bootstrap.productSessionToken}
      investmentReadsAvailable={
        capabilities.has("decision_analysis_list") &&
        capabilities.has("decision_analysis")
      }
    />
  )
}

function ReadyDecisions({
  transport,
  scope,
  investmentReadsAvailable,
}: {
  transport: ProductTransport
  scope: ProductScope
  investmentReadsAvailable: boolean
}) {
  return (
    <DecisionsFrame>
      <OpportunitiesReadExperience
        transport={transport}
        scope={scope}
        readAvailable={investmentReadsAvailable}
      />
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
