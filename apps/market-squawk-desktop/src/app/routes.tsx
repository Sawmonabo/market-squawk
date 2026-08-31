import { Component, lazy, Suspense, type ErrorInfo, type ReactNode } from "react"
import { Link, Navigate, Route, Routes, useLocation } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import { McpPage } from "@/components/mcp-page"

const OverviewPage = lazy(() =>
  import("@/components/overview-page").then((module) => ({
    default: module.OverviewPage,
  })),
)
const AdvancedOverviewPage = lazy(() =>
  import("@/features/advanced/advanced-overview-page").then((module) => ({
    default: module.AdvancedOverviewPage,
  })),
)

const MarketsPage = lazy(() =>
  import("@/features/markets").then((module) => ({ default: module.MarketsPage })),
)
const SourcesPage = lazy(() =>
  import("@/features/sources").then((module) => ({ default: module.SourcesPage })),
)
const ResearchPage = lazy(() =>
  import("@/features/research").then((module) => ({ default: module.ResearchPage })),
)
const PortfolioPage = lazy(() =>
  import("@/features/portfolio").then((module) => ({ default: module.PortfolioPage })),
)
const ModelsPage = lazy(() =>
  import("@/features/models").then((module) => ({ default: module.ModelsPage })),
)
const DecisionsPage = lazy(() =>
  import("@/features/decisions").then((module) => ({ default: module.DecisionsPage })),
)
const BacktestsPage = lazy(() =>
  import("@/features/backtests").then((module) => ({ default: module.BacktestsPage })),
)
const PaperExecutionPage = lazy(() =>
  import("@/features/paper").then((module) => ({ default: module.PaperExecutionPage })),
)
const RiskPage = lazy(() =>
  import("@/features/risk").then((module) => ({ default: module.RiskPage })),
)
const FairValuePage = lazy(() =>
  import("@/features/fair-value").then((module) => ({ default: module.FairValuePage })),
)
const OperationsPage = lazy(() =>
  import("@/features/operations").then((module) => ({ default: module.OperationsPage })),
)
const LifecyclePage = lazy(() =>
  import("@/features/lifecycle").then((module) => ({ default: module.LifecyclePage })),
)
const BackupRecoveryPage = lazy(() =>
  import("@/features/backup").then((module) => ({ default: module.BackupRecoveryPage })),
)
const LogsPage = lazy(() =>
  import("@/features/logs").then((module) => ({ default: module.LogsPage })),
)
const SettingsPage = lazy(() =>
  import("@/features/settings").then((module) => ({ default: module.SettingsPage })),
)

export function AppRoutes() {
  const location = useLocation()
  const product = useProduct()

  if (product.status === "loading") return <RouteLoading />

  return (
    <RouteErrorBoundary key={location.pathname}>
      <Suspense fallback={<RouteLoading />}>
        <Routes>
          <Route path="/home" element={<OverviewPage />} />
          <Route path="/markets" element={<MarketsPage />} />
          <Route path="/opportunities" element={<DecisionsPage />} />
          <Route path="/portfolio" element={<PortfolioPage />} />
          <Route path="/paper-execution" element={<PaperExecutionPage />} />
          <Route path="/advanced" element={<AdvancedOverviewPage />} />
          <Route path="/advanced/research-data" element={<ResearchPage />} />
          <Route path="/advanced/models-forecasts" element={<ModelsPage />} />
          <Route path="/advanced/backtests" element={<BacktestsPage />} />
          <Route path="/advanced/valuation-targets" element={<FairValuePage />} />
          <Route path="/advanced/risk-recommendation-policy" element={<RiskPage />} />
          <Route path="/connections/sources" element={<SourcesPage />} />
          <Route path="/system/ai-connections" element={<McpPage />} />
          <Route path="/system/operations-jobs" element={<OperationsPage />} />
          <Route path="/system/updates-repair" element={<LifecyclePage />} />
          <Route path="/system/backup-recovery" element={<BackupRecoveryPage />} />
          <Route path="/system/logs-diagnostics" element={<LogsPage />} />
          <Route path="/system/settings" element={<SettingsPage />} />
          <Route path="*" element={<Navigate to="/home" replace />} />
        </Routes>
      </Suspense>
    </RouteErrorBoundary>
  )
}

class RouteErrorBoundary extends Component<
  { children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false }

  static getDerivedStateFromError() {
    return { failed: true }
  }

  componentDidCatch(error: Error, information: ErrorInfo) {
    console.error("Market Squawk page failed to render", error, information)
  }

  render() {
    if (!this.state.failed) return this.props.children
    return (
      <main className="grid min-h-[55vh] place-items-center px-6">
        <section className="w-full max-w-lg rounded-xl border border-border bg-card/55 p-6 shadow-2xl">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-primary">
            Page recovery
          </p>
          <h1 className="mt-2 text-xl font-semibold">This page could not load</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Your local service and the rest of the workspace remain available. Return to Home,
            then reopen this page after checking its status.
          </p>
          <Link
            className="mt-5 inline-flex h-9 items-center rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground"
            to="/home"
          >
            Return to Home
          </Link>
        </section>
      </main>
    )
  }
}

function RouteLoading() {
  return (
    <main className="grid min-h-[55vh] place-items-center" aria-live="polite">
      <p className="text-sm text-muted-foreground">Loading workspace…</p>
    </main>
  )
}
