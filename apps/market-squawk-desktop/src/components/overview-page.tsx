import { CheckCircle2, ShieldCheck } from "lucide-react"

import { useProduct } from "@/app/product-context"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { SquawkSignal } from "@/components/squawk-signal"
import { SetupOverview } from "@/components/setup/setup-overview"
import { VerificationPanel } from "@/components/setup/verification-panel"
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
          <AlertTitle>Local application unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={product.refresh}>
          Try again
        </Button>
      </div>
    )
  }
  const bootstrap = product.bootstrap
  const paper = bootstrap.setupSteps.find((step) => step.id === "paper")
  const firstIncomplete = bootstrap.setupSteps.findIndex(
    (step) => !step.complete,
  )
  const currentStep = firstIncomplete < 0 ? bootstrap.setupSteps.length : firstIncomplete + 1
  const setupComplete = firstIncomplete < 0

  return (
    <div className="mx-auto w-full max-w-[1120px] space-y-4 p-5 lg:p-7">
      <section className="grid items-start gap-6 lg:grid-cols-[1fr_340px]">
        <div className="pt-1">
          <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
            {setupComplete
              ? "Your decision workspace"
              : `Setup · Step ${currentStep} of ${bootstrap.setupSteps.length}`}
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-[-0.04em] sm:text-4xl">
            <span className="text-white">Welcome to Market</span>{" "}
            <span className="text-primary">Squawk</span>
          </h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-muted-foreground">
            {setupComplete
              ? "See what needs attention, check the evidence behind current data, and find anything in your installed workspace."
              : "Finish the guided setup, then use this page to monitor live data, research, decisions, and safe paper execution in plain language."}
          </p>
        </div>
        <SquawkSignal status={bootstrap.storage.label} />
      </section>

      <section
        aria-label="Application facts"
        className="grid overflow-hidden rounded-xl border border-border bg-card/55 sm:grid-cols-2 lg:grid-cols-4"
      >
        <Fact
          label="Application"
          value={bootstrap.storage.label}
          ready={bootstrap.storage.state === "ready"}
        />
        <Fact
          label="Release"
          value={`v${bootstrap.applicationVersion} · ${bootstrap.installation.label}`}
        />
        <Fact label="Model runtime" value={bootstrap.modelRuntime.label} />
        <Fact
          label="Execution safety"
          value={paper?.complete ? "Central risk required" : "Unavailable"}
          ready={paper?.complete}
        />
      </section>

      <OverviewDashboard
        transport={product.transport}
        scope={bootstrap.runtime}
      />

      {setupComplete ? (
        <section className="grid gap-4 lg:grid-cols-[1fr_340px]">
          <div className="rounded-xl border border-border bg-card/35 p-5">
            <div className="flex items-start gap-3">
              <CheckCircle2 className="mt-0.5 size-5 text-[var(--success)]" aria-hidden="true" />
              <div>
                <h2 className="text-sm font-semibold">Recommended setup is complete</h2>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  You can revisit Sources, Portfolios, Models, MCP, or Settings whenever your
                  needs change. Readiness still depends on the live authority checks shown above.
                </p>
              </div>
            </div>
          </div>
          <VerificationPanel bootstrap={bootstrap} />
        </section>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
          <SetupOverview
            bootstrap={bootstrap}
            transport={product.transport}
            onRefresh={product.refresh}
          />
          <VerificationPanel bootstrap={bootstrap} />
        </div>
      )}

      <aside className="flex items-start gap-3 rounded-lg border border-border bg-card/20 px-4 py-3 text-[11px] leading-relaxed text-muted-foreground">
        <ShieldCheck
          className="mt-0.5 size-4 shrink-0 text-foreground/70"
          aria-hidden="true"
        />
        <span>
          Safe to close. Accepted provider work is checkpointed by the Rust
          authorities and resumes without exposing credentials or fabricating
          readiness.
        </span>
      </aside>
    </div>
  )
}

function Fact({
  label,
  value,
  ready = false,
}: {
  label: string
  value: string
  ready?: boolean
}) {
  return (
    <div className="min-h-16 border-b border-border px-4 py-3 last:border-b-0 sm:odd:border-r lg:border-b-0 lg:border-r lg:last:border-r-0">
      <p className="font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 flex items-center gap-2 text-xs font-medium">
        {ready ? (
          <span
            className="size-1.5 rounded-full bg-[var(--success)]"
            aria-hidden="true"
          />
        ) : null}
        {value}
      </p>
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
