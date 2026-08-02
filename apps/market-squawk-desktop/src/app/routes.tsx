import { lazy, Suspense } from "react"
import { Navigate, Route, Routes } from "react-router-dom"

import { DomainPage } from "@/components/domain-page"
import { InstallationPage } from "@/components/installation-page"
import { McpPage } from "@/components/mcp-page"
import { OverviewPage } from "@/components/overview-page"

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

const operatingRoutes = [
  {
    path: "/settings",
    title: "Settings",
    description:
      "Review validated configuration origins, safety defaults, local paths, and advanced resource limits.",
  },
] as const

export function AppRoutes() {
  return (
    <Suspense fallback={<RouteLoading />}>
      <Routes>
        <Route path="/overview" element={<OverviewPage />} />
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
        <Route path="/updates" element={<InstallationPage />} />
        <Route path="/mcp" element={<McpPage />} />
        <Route path="/logs" element={<OperationsPage />} />
        <Route path="/backup-recovery" element={<InstallationPage recovery />} />
        {operatingRoutes.map((route) => (
          <Route
            key={route.path}
            path={route.path}
            element={<DomainPage title={route.title} description={route.description} />}
          />
        ))}
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </Suspense>
  )
}

function RouteLoading() {
  return (
    <main className="grid min-h-[55vh] place-items-center" aria-live="polite">
      <p className="text-sm text-muted-foreground">Loading workspace…</p>
    </main>
  )
}
