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

import type { DesktopBootstrap } from "@/lib/schemas"

export interface NavigationItem {
  label: string
  path: string
  icon: LucideIcon
  domain?: string
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
  { label: "Markets", path: "/markets", icon: BarChart3, domain: "market" },
  {
    label: "Opportunities",
    path: "/opportunities",
    icon: Crosshair,
    domain: "decision",
  },
  {
    label: "Portfolio",
    path: "/portfolio",
    icon: BriefcaseBusiness,
    domain: "portfolio",
  },
]

export const paperExecutionNavigation: NavigationItem[] = [
  {
    label: "Paper Execution",
    path: "/paper-execution",
    icon: FileTerminal,
    domain: "execution",
  },
]

export const advancedNavigation: NavigationItem[] = [
  { label: "Advanced Overview", path: "/advanced", icon: Boxes },
  {
    label: "Research & Data",
    path: "/advanced/research-data",
    icon: FlaskConical,
    domain: "research",
  },
  {
    label: "Models & Forecasts",
    path: "/advanced/models-forecasts",
    icon: Bot,
    domain: "model",
  },
  {
    label: "Backtests",
    path: "/advanced/backtests",
    icon: FileClock,
    domain: "analysis",
  },
  {
    label: "Valuation & Targets",
    path: "/advanced/valuation-targets",
    icon: Landmark,
    domain: "fair_value",
  },
  {
    label: "Risk & Recommendation Policy",
    path: "/advanced/risk-recommendation-policy",
    icon: ShieldCheck,
    domain: "bot",
  },
]

export const connectionsSystemNavigation: NavigationItem[] = [
  {
    label: "Connections & Sources",
    path: "/connections/sources",
    icon: Database,
    domain: "source",
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
  const operationReady =
    !item.domain ||
    bootstrap.operations.some((operation) => operation.domain === item.domain)
  if (!operationReady) {
    return {
      admitted: false,
      reason: `The installed application does not expose the ${item.label} service.`,
    }
  }
  return { admitted: true, reason: null }
}
