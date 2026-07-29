import {
  Activity,
  ArchiveRestore,
  BarChart3,
  Bot,
  Boxes,
  BriefcaseBusiness,
  Database,
  FileClock,
  FileTerminal,
  FlaskConical,
  Landmark,
  Logs,
  Network,
  Settings,
  ShieldCheck,
  type LucideIcon,
} from "lucide-react"

import type { DesktopBootstrap } from "@/lib/schemas"

export interface NavigationItem {
  label: string
  path: string
  icon: LucideIcon
  domain?: string
}

const overviewNavigation: NavigationItem = {
  label: "Overview",
  path: "/overview",
  icon: Boxes,
}

export const workspaceNavigation: NavigationItem[] = [
  overviewNavigation,
  { label: "Markets", path: "/markets", icon: BarChart3, domain: "market" },
  { label: "Sources", path: "/sources", icon: Database, domain: "source" },
  {
    label: "Research",
    path: "/research",
    icon: FlaskConical,
    domain: "research",
  },
  {
    label: "Portfolios",
    path: "/portfolios",
    icon: BriefcaseBusiness,
    domain: "portfolio",
  },
  { label: "Models", path: "/models", icon: Bot, domain: "model" },
  {
    label: "Backtests",
    path: "/backtests",
    icon: FileClock,
    domain: "analysis",
  },
  {
    label: "Paper Execution",
    path: "/paper-execution",
    icon: FileTerminal,
    domain: "execution",
  },
  { label: "Risk", path: "/risk", icon: ShieldCheck, domain: "bot" },
  {
    label: "Fair Value",
    path: "/fair-value",
    icon: Landmark,
    domain: "fair_value",
  },
  { label: "MCP", path: "/mcp", icon: Network },
]

export const operationsNavigation: NavigationItem[] = [
  { label: "Updates", path: "/updates", icon: Activity },
  {
    label: "Backup & Recovery",
    path: "/backup-recovery",
    icon: ArchiveRestore,
  },
  { label: "Logs", path: "/logs", icon: Logs },
  { label: "Settings", path: "/settings", icon: Settings },
]

export const allNavigation = [
  ...workspaceNavigation,
  ...operationsNavigation,
]

export function navigationForPath(pathname: string) {
  return (
    allNavigation.find((item) => item.path === pathname) ??
    overviewNavigation
  )
}

export interface NavigationAdmission {
  admitted: boolean
  reason: string | null
}

export function navigationAdmission(
  item: NavigationItem,
  bootstrap: DesktopBootstrap,
): NavigationAdmission {
  const stepReady = (id: DesktopBootstrap["setupSteps"][number]["id"]) =>
    bootstrap.setupSteps.some((step) => step.id === id && step.complete)
  const operationReady =
    !item.domain ||
    bootstrap.operations.some((operation) => operation.domain === item.domain)

  const prerequisite = (() => {
    switch (item.path) {
      case "/markets":
        return {
          ready: stepReady("sources"),
          reason: "Connect and verify a market-data source first.",
        }
      case "/research":
      case "/backtests":
        return {
          ready: stepReady("research"),
          reason: "Restore the complete Research services first.",
        }
      case "/portfolios":
        return {
          ready: stepReady("portfolio"),
          reason: "Restore the complete Portfolio services first.",
        }
      case "/models":
        return {
          ready: bootstrap.modelRuntime.state === "ready",
          reason: "Configure and admit a verified local training release first.",
        }
      case "/paper-execution":
      case "/risk":
        return {
          ready: stepReady("paper"),
          reason: "Restore the complete risk-controlled paper services first.",
        }
      default:
        return { ready: true, reason: null }
    }
  })()

  if (!prerequisite.ready) {
    return { admitted: false, reason: prerequisite.reason }
  }
  if (!operationReady) {
    return {
      admitted: false,
      reason: `The installed application does not expose the ${item.label} service.`,
    }
  }
  return { admitted: true, reason: null }
}
