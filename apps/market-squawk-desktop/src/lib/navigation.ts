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
