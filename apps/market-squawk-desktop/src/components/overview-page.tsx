import { Sparkles } from "lucide-react"
import { Link } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { SquawkSignal } from "@/components/squawk-signal"
import { OverviewDashboard } from "@/features/overview/overview-dashboard"

export function OverviewPage() {
  const product = useProduct()
  if (product.status === "loading") {
    return <OverviewLoading />
  }
  if (product.availability === "degraded") {
    return (
      <div className="mx-auto w-full max-w-[1120px] p-5 lg:p-7">
        <Alert>
          <AlertTitle>Secure setup is waiting for you</AlertTitle>
          <AlertDescription>
            Complete the recovery action in the shared banner to unlock secure
            storage. More detail is available in Logs &amp; Diagnostics.
          </AlertDescription>
        </Alert>
      </div>
    )
  }
  if (product.status === "error") {
    return (
      <div className="mx-auto max-w-3xl p-8">
        <Alert variant="destructive">
          <AlertTitle>Local application unavailable</AlertTitle>
          <AlertDescription>
            Try again or review Logs &amp; Diagnostics for details.
          </AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </div>
    )
  }
  const bootstrap = product.bootstrap
  return (
    <div className="mx-auto w-full max-w-[1120px] space-y-4 p-5 lg:p-7">
      <section className="grid items-start gap-6 lg:grid-cols-[1fr_340px]">
        <div className="pt-1">
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            Home
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-[-0.04em] sm:text-4xl">
            What needs your attention now?
          </h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-muted-foreground">
            Review your exact account context, retained investment analyses, work
            in progress, and setup gaps. Full market, recommendation, portfolio,
            and system workspaces remain on their owning pages.
          </p>
          <div className="mt-5 flex flex-wrap items-center gap-3">
            <Button
              type="button"
              disabled
              aria-describedby="find-opportunities-readiness"
            >
              <Sparkles aria-hidden="true" />
              Find opportunities
            </Button>
            <Button asChild variant="outline">
              <Link to="/opportunities">Review retained analyses</Link>
            </Button>
          </div>
          <p
            id="find-opportunities-readiness"
            role="status"
            className="mt-3 max-w-3xl text-[11px] leading-5 text-muted-foreground"
          >
            Opportunity search is not available in this build yet. You can
            still review your existing analyses.
          </p>
        </div>
        <SquawkSignal status={bootstrap.storage.label} />
      </section>

      <OverviewDashboard
        transport={product.transport}
        scope={bootstrap.runtime}
        bootstrap={bootstrap}
      />

      <section className="rounded-xl border border-border bg-card/35 p-5">
        <h2 className="text-sm font-semibold">Setup and connections</h2>
        <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
          Manage data connections, credentials, and connection health in their dedicated settings
          workspace. Home shows only the resulting investment and readiness summaries.
        </p>
        <Button asChild className="mt-4" variant="outline">
          <Link to="/connections/sources">Manage connections</Link>
        </Button>
      </section>

    </div>
  )
}

function OverviewLoading() {
  return (
    <div className="mx-auto w-full max-w-[1120px] space-y-5 p-7" aria-label="Loading workspace">
      <Skeleton className="h-4 w-32" />
      <Skeleton className="h-11 w-3/5" />
      <Skeleton className="h-5 w-4/5" />
      <Skeleton className="h-16 w-full rounded-xl" />
      <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
        <Skeleton className="h-80 rounded-xl" />
        <Skeleton className="h-80 rounded-xl" />
      </div>
    </div>
  )
}
