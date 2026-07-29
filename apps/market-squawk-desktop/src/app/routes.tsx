import { Navigate, Route, Routes } from "react-router-dom"

import { DomainPage } from "@/components/domain-page"
import { OverviewPage } from "@/components/overview-page"

const domainRoutes = [
  {
    path: "/markets",
    title: "Markets",
    domain: "market",
    description:
      "Inspect bounded market snapshots, trades, quotes, books, quality, and comparisons with explicit source evidence.",
  },
  {
    path: "/sources",
    title: "Sources",
    domain: "source",
    description:
      "Configure supported providers and inspect their current coverage, health, and durable onboarding state.",
  },
  {
    path: "/research",
    title: "Research",
    domain: "research",
    description:
      "Work with versioned local datasets, manifests, point-in-time history, and alternative data.",
  },
  {
    path: "/portfolios",
    title: "Portfolios",
    domain: "portfolio",
    description:
      "Reconcile holdings and transactions, then inspect performance, exposure, attribution, and risk.",
  },
  {
    path: "/models",
    title: "Models",
    domain: "model",
    description:
      "Inspect admitted model bundles and run bounded native predictions from verified feature contracts.",
  },
  {
    path: "/backtests",
    title: "Backtests",
    domain: "analysis",
    description:
      "Build point-in-time research experiments without crossing into the live execution plane.",
  },
  {
    path: "/paper-execution",
    title: "Paper Execution",
    domain: "execution",
    description:
      "Inspect risk-approved paper orders, fills, balances, positions, cancellations, and reconciliation.",
  },
  {
    path: "/risk",
    title: "Risk",
    domain: "bot",
    description:
      "Review safety state and control paper operations through the centralized risk authority.",
  },
  {
    path: "/fair-value",
    title: "Fair Value",
    domain: "fair_value",
    description:
      "Analyze ASC 820 and IFRS 13 measurements, classifications, approvals, and retained evidence.",
  },
] as const

const operatingRoutes = [
  {
    path: "/mcp",
    title: "MCP",
    description:
      "Inspect the bounded local stdio tool surface and client setup without placing MCP in the live path.",
  },
  {
    path: "/updates",
    title: "Updates",
    description:
      "Review signed release availability and installation status. No update is installed without explicit authority.",
  },
  {
    path: "/backup-recovery",
    title: "Backup & Recovery",
    description:
      "Protect local catalogs, manifests, datasets, artifacts, and configuration with controlled recovery procedures.",
  },
  {
    path: "/logs",
    title: "Logs",
    description:
      "Inspect redacted local operating evidence without telemetry or hidden outbound reporting.",
  },
  {
    path: "/settings",
    title: "Settings",
    description:
      "Review validated configuration origins, safety defaults, local paths, and advanced resource limits.",
  },
] as const

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/overview" element={<OverviewPage />} />
      {domainRoutes.map((route) => (
        <Route
          key={route.path}
          path={route.path}
          element={
            <DomainPage
              title={route.title}
              domain={route.domain}
              description={route.description}
            />
          }
        />
      ))}
      {operatingRoutes.map((route) => (
        <Route
          key={route.path}
          path={route.path}
          element={
            <DomainPage title={route.title} description={route.description} />
          }
        />
      ))}
      <Route path="*" element={<Navigate to="/overview" replace />} />
    </Routes>
  )
}

