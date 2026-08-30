import { CircleAlert, Landmark, ShieldCheck } from "lucide-react"
import type { ReactNode } from "react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"

export function FairValuePage() {
  const product = useProduct()

  if (product.status === "loading") return <ValuationLoading />
  if (product.status === "error") {
    return (
      <ValuationFrame>
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Valuation research is unavailable</AlertTitle>
          <AlertDescription>
            Market Squawk could not open this research. Try again, or review the app setup if the
            problem continues.
          </AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </ValuationFrame>
    )
  }

  return <UnavailableValuation />
}

function UnavailableValuation() {
  return (
    <ValuationFrame>
      <Alert className="border-amber-400/25 bg-amber-400/5">
        <CircleAlert className="text-amber-200" aria-hidden="true" />
        <AlertTitle>No complete valuation estimate is available</AlertTitle>
        <AlertDescription>
          Market Squawk does not have a defensible investment value to show here. No target,
          upside, downside, buy range, or sell range is inferred from incomplete information.
        </AlertDescription>
      </Alert>

      <section
        aria-label="Unavailable valuation summary"
        className="mt-5 grid overflow-hidden rounded-xl border border-border bg-card/45 sm:grid-cols-2 xl:grid-cols-4"
      >
        <UnavailableFact label="Estimated value" />
        <UnavailableFact label="Expected range" />
        <UnavailableFact label="Upside or downside" />
        <UnavailableFact label="Valid through" />
      </section>

      <div className="mt-5 grid gap-4 lg:grid-cols-2">
        <UnavailableSection
          title="Estimate and method"
          detail="No supported valuation method or value range is available for review."
        />
        <UnavailableSection
          title="Reasons and assumptions"
          detail="No valuation claim is being made, so there are no supporting reasons or assumptions to rely on."
        />
        <UnavailableSection
          title="Risks and invalidators"
          detail="No valuation claim exists to monitor or invalidate."
        />
        <UnavailableSection
          title="Evidence and uncertainty"
          detail="The available information is not sufficient to present an investment valuation. Market Squawk is abstaining instead of overstating confidence."
          icon="shield"
        />
      </div>
    </ValuationFrame>
  )
}

function UnavailableFact({ label }: { label: string }) {
  return (
    <div className="border-border p-4 sm:border-r sm:last:border-r-0">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-2 text-sm font-semibold">Not available</p>
    </div>
  )
}

function UnavailableSection({
  title,
  detail,
  icon = "valuation",
}: {
  title: string
  detail: string
  icon?: "valuation" | "shield"
}) {
  const Icon = icon === "shield" ? ShieldCheck : Landmark
  return (
    <section className="rounded-xl border border-border bg-card/35 p-5">
      <Icon className="size-4 text-muted-foreground" aria-hidden="true" />
      <h2 className="mt-3 text-sm font-semibold">{title}</h2>
      <p className="mt-2 text-xs leading-5 text-muted-foreground">{detail}</p>
    </section>
  )
}

function ValuationFrame({ children }: { children: ReactNode }) {
  return (
    <main className="mx-auto w-full max-w-[1180px] p-5 lg:p-7">
      <header className="border-b border-border pb-6">
        <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
          Advanced · Investment research
        </p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Valuation &amp; targets</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Understand what an investment may be worth, how that estimate compares with its current
          price, and what could change the outlook.
        </p>
      </header>
      <div className="mt-5">{children}</div>
    </main>
  )
}

function ValuationLoading() {
  return (
    <ValuationFrame>
      <div className="space-y-4" aria-label="Loading valuation research">
        <Skeleton className="h-24 rounded-xl" />
        <div className="grid gap-4 lg:grid-cols-2">
          <Skeleton className="h-36 rounded-xl" />
          <Skeleton className="h-36 rounded-xl" />
        </div>
      </div>
    </ValuationFrame>
  )
}
