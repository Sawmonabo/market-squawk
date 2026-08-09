import { Component, lazy, Suspense, type ErrorInfo, type ReactNode } from "react"
import { Link, Navigate, Route, Routes, useLocation } from "react-router-dom"

import { McpPage } from "@/components/mcp-page"

const OverviewPage = lazy(() =>
  import("@/components/overview-page").then((module) => ({
    default: module.OverviewPage,
  })),
)
const LookupPage = lazy(() =>
  import("@/features/lookup").then((module) => ({ default: module.LookupPage })),
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

  return (
    <RouteErrorBoundary key={location.pathname}>
      <Suspense fallback={<RouteLoading />}>
        <Routes>
          <Route path="/overview" element={<OverviewPage />} />
          <Route path="/lookup" element={<LookupPage />} />
          <Route path="/markets" element={<MarketsPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/research" element={<ResearchPage />} />
          <Route path="/portfolios" element={<PortfolioPage />} />
          <Route path="/models" element={<ModelsPage />} />
          <Route path="/decisions" element={<DecisionsPage />} />
          <Route path="/backtests" element={<BacktestsPage />} />
          <Route path="/paper-execution" element={<PaperExecutionPage />} />
          <Route path="/risk" element={<RiskPage />} />
          <Route path="/fair-value" element={<FairValuePage />} />
          <Route path="/updates" element={<LifecyclePage />} />
          <Route path="/mcp" element={<McpPage />} />
          <Route path="/operations" element={<OperationsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/backup-recovery" element={<BackupRecoveryPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
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
            Your local service and the rest of the workspace remain available. Return to Overview,
            then reopen this page after checking its status.
          </p>
          <Link
            className="mt-5 inline-flex h-9 items-center rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground"
            to="/overview"
          >
            Return to Overview
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
