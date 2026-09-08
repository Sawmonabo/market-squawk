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
            Start with clear investment guidance, then check current prices,
            uncertainty, and portfolio impact before deciding what to do.
          </p>
          <div className="mt-5 flex flex-wrap items-center gap-3">
            <Button asChild>
              <Link to="/opportunities">
                <Sparkles aria-hidden="true" />
                Review investment guidance
              </Link>
            </Button>
            <Button asChild variant="outline">
              <Link to="/markets">Explore investments</Link>
            </Button>
          </div>
        </div>
        <section className="flex min-h-32 flex-col justify-between rounded-xl border border-border bg-card/45 p-4">
          <p className="font-mono text-[9px] uppercase tracking-widest text-muted-foreground">
            Decision checklist
          </p>
          <div>
            <p className="text-lg font-semibold">Evidence before action</p>
            <p className="mt-2 text-xs leading-5 text-muted-foreground">
              Check the horizon, ranges, reasons, risks, assumptions, expiry,
              invalidators, evidence coverage, and uncertainty together.
            </p>
          </div>
        </section>
      </section>

      <OverviewDashboard
        transport={product.transport}
        scope={bootstrap.productSessionToken}
      />

      <section className="rounded-xl border border-border bg-card/35 p-5">
        <h2 className="text-sm font-semibold">Setup and connections</h2>
        <p className="mt-2 max-w-3xl text-xs leading-5 text-muted-foreground">
          Manage setup and connectivity in Settings. Home shows only the resulting
          investment guidance and availability.
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
