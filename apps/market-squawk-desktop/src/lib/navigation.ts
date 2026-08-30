import {
  Activity,
  ArchiveRestore,
  BarChart3,
  Bot,
  Boxes,
  BriefcaseBusiness,
  Crosshair,
  Database,
  FileClock,
  FileTerminal,
  FlaskConical,
  Landmark,
  Logs,
  Network,
  Settings,
  ShieldCheck,
  ServerCog,
  type LucideIcon,
} from "lucide-react"

import { productCapabilitySet } from "@/lib/product-capabilities"
import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"

export interface NavigationItem {
  label: string
  path: string
  icon: LucideIcon
  capabilities?: readonly ProductCapability[]
}

export interface NavigationSection {
  label: string
  items: NavigationItem[]
}

const homeNavigation: NavigationItem = {
  label: "Home",
  path: "/home",
  icon: Boxes,
}

export const everydayNavigation: NavigationItem[] = [
  homeNavigation,
  {
    label: "Markets",
    path: "/markets",
    icon: BarChart3,
    capabilities: ["market_overview", "market_universe"],
  },
  {
    label: "Opportunities",
    path: "/opportunities",
    icon: Crosshair,
    capabilities: ["decision_screen_list", "decision_analysis_list"],
  },
  {
    label: "Portfolio",
    path: "/portfolio",
    icon: BriefcaseBusiness,
    capabilities: ["portfolio_account_list", "portfolio_holdings"],
  },
]

export const paperExecutionNavigation: NavigationItem[] = [
  {
    label: "Paper Execution",
    path: "/paper-execution",
    icon: FileTerminal,
    capabilities: ["execution_orders", "bot_status"],
  },
]

export const advancedNavigation: NavigationItem[] = [
  { label: "Advanced Overview", path: "/advanced", icon: Boxes },
  {
    label: "Research & Data",
    path: "/advanced/research-data",
    icon: FlaskConical,
    capabilities: ["research_dataset_list", "macro_context"],
  },
  {
    label: "Models & Forecasts",
    path: "/advanced/models-forecasts",
    icon: Bot,
    capabilities: ["model_evidence", "forecast_list"],
  },
  {
    label: "Backtests",
    path: "/advanced/backtests",
    icon: FileClock,
    capabilities: ["backtest_preparation", "backtest_activity"],
  },
  {
    label: "Valuation & Targets",
    path: "/advanced/valuation-targets",
    icon: Landmark,
    capabilities: ["fair_value_measurement", "fair_value_workspace"],
  },
  {
    label: "Risk & Recommendation Policy",
    path: "/advanced/risk-recommendation-policy",
    icon: ShieldCheck,
    capabilities: ["portfolio_risk", "risk_kill_switch", "bot_status"],
  },
]

export const connectionsSystemNavigation: NavigationItem[] = [
  {
    label: "Connections & Sources",
    path: "/connections/sources",
    icon: Database,
  },
  { label: "AI Connections", path: "/system/ai-connections", icon: Network },
  {
    label: "Operations & Jobs",
    path: "/system/operations-jobs",
    icon: ServerCog,
  },
  { label: "Updates & Repair", path: "/system/updates-repair", icon: Activity },
  {
    label: "Backup & Recovery",
    path: "/system/backup-recovery",
    icon: ArchiveRestore,
  },
  { label: "Logs & Diagnostics", path: "/system/logs-diagnostics", icon: Logs },
  { label: "Settings", path: "/system/settings", icon: Settings },
]

const everydaySection: NavigationSection = {
  label: "Everyday",
  items: everydayNavigation,
}

export const navigationSections: NavigationSection[] = [
  everydaySection,
  { label: "Simulated execution", items: paperExecutionNavigation },
  { label: "Advanced", items: advancedNavigation },
  { label: "Connections & System", items: connectionsSystemNavigation },
]

export const allNavigation = navigationSections.flatMap((section) => section.items)

export function navigationForPath(pathname: string) {
  return allNavigation.find((item) => item.path === pathname) ?? homeNavigation
}

export function navigationSectionForPath(pathname: string) {
  return (
    navigationSections.find((section) =>
      section.items.some((item) => item.path === pathname),
    ) ?? everydaySection
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
  const capabilities = productCapabilitySet(bootstrap)
  const capabilityReady =
    !item.capabilities ||
    item.capabilities.some((capability) => capabilities.has(capability))
  if (!capabilityReady) {
    return {
      admitted: false,
      reason: `${item.label} is not available in this workspace.`,
    }
  }
  return { admitted: true, reason: null }
}
