import {
  Bot,
  ChartNoAxesCombined,
  CircleAlert,
  FileClock,
  FlaskConical,
  Landmark,
  ShieldCheck,
} from "lucide-react"
import { Link } from "react-router-dom"

import { messageFrom, useProduct } from "@/app/product-context"
import type { ProductScope } from "@/app/query-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Skeleton } from "@/components/ui/skeleton"
import { formatUnixNanos } from "@/features/opportunities/format"
import type { ProductTransport } from "@/lib/transport"

import { useAnalyticalControllerStatus } from "./use-analytical-profile"

const workspaces = [
  {
    label: "Research & Data",
    path: "/advanced/research-data",
    detail: "Inspect sources, point-in-time datasets, lineage, rights, and feature preparation.",
    icon: FlaskConical,
  },
  {
    label: "Models & Forecasts",
    path: "/advanced/models-forecasts",
    detail: "Review model evidence, forecast preparation, outcomes, drift, and calibration.",
    icon: Bot,
  },
  {
    label: "Backtests",
    path: "/advanced/backtests",
    detail: "Configure and inspect point-in-time backtests, costs, artifacts, and diagnostics.",
    icon: FileClock,
  },
  {
    label: "Valuation & Targets",
    path: "/advanced/valuation-targets",
    detail: "Review valuation evidence, governance, and explicitly custom research targets.",
    icon: Landmark,
  },
  {
    label: "Risk & Recommendation Policy",
    path: "/advanced/risk-recommendation-policy",
    detail: "Inspect detailed risk evidence and, when available, recommendation policy controls.",
    icon: ShieldCheck,
  },
]

export function AdvancedOverviewPage() {
  const product = useProduct()
  if (product.status === "loading") {
    return <AdvancedLoading />
  }
  if (product.status === "error") {
    return (
      <main className="mx-auto w-full max-w-[1180px] px-4 py-6 sm:px-6 lg:px-8">
        <Alert variant="destructive">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Advanced analysis is unavailable</AlertTitle>
          <AlertDescription>{product.error}</AlertDescription>
        </Alert>
      </main>
    )
  }
  return (
    <ReadyAdvancedOverview
      transport={product.transport}
      scope={product.bootstrap.runtime}
    />
  )
}

function ReadyAdvancedOverview({
  transport,
  scope,
}: {
  transport: ProductTransport
  scope: ProductScope
}) {
  const controller = useAnalyticalControllerStatus(transport, scope)

  return (
    <main className="mx-auto w-full max-w-[1180px] px-4 py-6 sm:px-6 lg:px-8">
      <header className="border-b border-border pb-6">
        <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-primary">
          Advanced
        </p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Research & analysis</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
          Inspect and customize the analytical evidence behind Market Squawk. Ordinary investment
          workflows will use governed defaults after the canonical backend path is composed.
        </p>
      </header>

      <AnalyticalProfileStatus query={controller} />

      <section className="mt-6" aria-labelledby="advanced-workspaces">
        <div className="flex items-center gap-2">
          <ChartNoAxesCombined className="size-4 text-primary" aria-hidden="true" />
          <h2 id="advanced-workspaces" className="text-lg font-semibold">
            Analytical workspaces
          </h2>
        </div>
        <div className="mt-4 grid gap-4 md:grid-cols-2">
          {workspaces.map((workspace) => (
            <Link
              key={workspace.path}
              to={workspace.path}
              className="rounded-xl border border-border bg-card/45 p-5 transition-colors hover:border-primary/35 hover:bg-card/70 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
            >
              <workspace.icon className="size-5 text-primary" aria-hidden="true" />
              <h3 className="mt-4 text-base font-semibold">{workspace.label}</h3>
              <p className="mt-2 text-xs leading-5 text-muted-foreground">{workspace.detail}</p>
            </Link>
          ))}
        </div>
      </section>
    </main>
  )
}

function AnalyticalProfileStatus({
  query,
}: {
  query: ReturnType<typeof useAnalyticalControllerStatus>
}) {
  if (query.isPending) {
    return <Skeleton className="mt-6 h-40 w-full rounded-xl" />
  }
  if (query.isError) {
    return (
      <Alert variant="destructive" className="mt-6">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>The Desktop analytical profile could not be opened</AlertTitle>
        <AlertDescription>{messageFrom(query.error)}</AlertDescription>
      </Alert>
    )
  }

  const status = query.data
  const active = status.activeProfile
  return (
    <section
      className="mt-6 rounded-xl border border-border bg-card/45 p-5"
      aria-labelledby="advanced-profile-status"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.16em] text-primary">
            Active analytical profile
          </p>
          <h2 id="advanced-profile-status" className="mt-2 text-lg font-semibold">
            {active.displayName}
          </h2>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {active.kind === "default"
              ? "The immutable built-in profile is active. Custom copies can be compared, validated, activated, and restored without changing historical results."
              : "A validated custom profile is active. Historical results keep the exact profile revision and digest used when they were created."}
          </p>
        </div>
        <span className="rounded-full border border-border bg-background/40 px-2.5 py-1 text-[10px] uppercase tracking-wider text-muted-foreground">
          {active.kind === "default" ? "Default" : "Custom"}
        </span>
      </div>
      <dl className="mt-4 grid gap-3 border-t border-border/70 pt-4 sm:grid-cols-2 lg:grid-cols-4">
        <ProfileFact label="Profile version" value={`V${active.version}`} />
        <ProfileFact label="Profile revision" value={active.profileRevision} />
        <ProfileFact label="Activation revision" value={active.activationRevision} />
        <ProfileFact label="Activated" value={formatUnixNanos(active.activatedAt)} />
      </dl>
      <p className="mt-4 break-all font-mono text-[9px] text-muted-foreground">
        Configuration SHA-256 {active.configDigest}
      </p>

      <div className="mt-5 flex gap-3 rounded-lg border border-amber-400/25 bg-amber-400/5 p-4">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-amber-300" aria-hidden="true" />
        <div>
          <h3 className="text-sm font-semibold">Find and Analyze remain blocked</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            The profile and restart journal are durable, but a profile is not financial evidence.
            Market Squawk will not start an analysis until the canonical data and pure backend
            capabilities listed below are composed and restart-proven.
          </p>
          <ul className="mt-3 space-y-2 text-xs leading-5 text-muted-foreground">
            {status.workflowReadiness.blockers.map((blocker) => (
              <li key={blocker.code}>• {blocker.detail}</li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  )
}

function ProfileFact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-xs font-medium">{value}</dd>
    </div>
  )
}

function AdvancedLoading() {
  return (
    <main
      className="mx-auto w-full max-w-[1180px] space-y-5 px-4 py-6 sm:px-6 lg:px-8"
      aria-label="Loading advanced analysis"
    >
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-10 w-80" />
      <Skeleton className="h-40 w-full rounded-xl" />
    </main>
  )
}
