import { Sparkles } from "lucide-react"
import { Link } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
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
          <AlertTitle>Setup is waiting for you</AlertTitle>
          <AlertDescription>
            Complete the setup step shown above, then return here to continue.
          </AlertDescription>
        </Alert>
      </div>
    )
  }
  if (product.status === "error") {
    return (
      <div className="mx-auto max-w-3xl p-8">
        <Alert variant="destructive">
          <AlertTitle>Investment workspace unavailable</AlertTitle>
          <AlertDescription>
            Your investment workspace cannot be shown right now. Try again.
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
            Review your account, saved investment analyses, work in progress,
            and anything that needs setup. Markets, recommendations, and portfolio
            tools remain in their dedicated workspaces.
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
            Opportunity search is unavailable right now. You can still review
            your existing analyses.
          </p>
        </div>
        <section className="flex min-h-32 flex-col justify-between rounded-xl border border-border bg-card/45 p-4">
          <p className="font-mono text-[9px] uppercase tracking-widest text-muted-foreground">
            Research workspace
          </p>
          <div>
            <p className="text-lg font-semibold">Ready for your next review</p>
            <p className="mt-2 text-xs leading-5 text-muted-foreground">
              Start with saved analyses, then review opportunities as new
              research becomes available.
            </p>
          </div>
        </section>
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
