import { ShieldCheck, Sparkles } from "lucide-react"
import { Link } from "react-router-dom"

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
  if (product.availability === "degraded") {
    return (
      <div className="mx-auto w-full max-w-[1120px] p-5 lg:p-7">
        <Alert>
          <AlertTitle>Secure setup is waiting for you</AlertTitle>
          <AlertDescription>
            {product.error} Complete the single recovery action in the shared
            banner, or keep browsing the workspace navigation while secure
            storage remains locked.
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
          <AlertDescription>{product.error}</AlertDescription>
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
            Market Squawk Default V1 is retained locally, but the required
            canonical data and pure backend analysis capabilities are not yet
            composed into a restart-proven workflow. This disabled control
            starts no scan and creates no recommendation.
          </p>
        </div>
        <SquawkSignal status={bootstrap.storage.label} />
      </section>

      <OverviewDashboard
        transport={product.transport}
        scope={bootstrap.runtime}
        bootstrap={bootstrap}
      />

      <details className="rounded-xl border border-border bg-card/35">
        <summary className="cursor-pointer px-5 py-4 text-sm font-semibold">
          Guided setup and installation evidence
        </summary>
        <div className="space-y-4 border-t border-border p-4">
          <section
            aria-label="Application facts"
            className="grid overflow-hidden rounded-xl border border-border bg-card/55 sm:grid-cols-2 lg:grid-cols-4"
          >
            <Fact
              label="Workspace"
              value={bootstrap.storage.label}
              ready={bootstrap.storage.state === "ready"}
            />
            <Fact
              label="Release"
              value={`v${bootstrap.applicationVersion} · ${bootstrap.installation.label}`}
              ready={bootstrap.installation.state === "ready"}
            />
            <Fact
              label="Model runtime"
              value={bootstrap.modelRuntime.label}
              ready={bootstrap.modelRuntime.state === "ready"}
            />
            <Fact
              label="Local AI service"
              value={bootstrap.mcp.label}
              ready={bootstrap.mcp.state === "ready"}
            />
          </section>
          <div className="grid gap-4 lg:grid-cols-[1fr_340px]">
            <SetupOverview
              bootstrap={bootstrap}
              transport={product.transport}
              onRefresh={product.refresh}
            />
            <VerificationPanel bootstrap={bootstrap} />
          </div>
        </div>
      </details>

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
