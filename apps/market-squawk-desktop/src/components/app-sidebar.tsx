import { AudioWaveform, ChevronsUpDown, LockKeyhole } from "lucide-react"
import { Link, useLocation } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
} from "@/components/ui/sidebar"
import {
  operationsNavigation,
  workspaceNavigation,
} from "@/lib/navigation"

export function AppSidebar() {
  const location = useLocation()
  const product = useProduct()
  const localStatus =
    product.status === "ready" ? product.bootstrap.storage.label : product.status
  const localStatusColor =
    product.status === "ready" && product.bootstrap.storage.state === "ready"
      ? "bg-[var(--success)]"
      : product.status === "error"
        ? "bg-destructive"
        : "bg-[var(--warning)]"

  return (
    <Sidebar collapsible="icon" className="border-sidebar-border">
      <SidebarHeader className="px-3 pt-4 pb-3">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              asChild
              size="lg"
              tooltip="Market Squawk workspace"
              className="h-auto gap-3 px-1.5 py-2"
            >
              <Link to="/overview" aria-label="Market Squawk workspace">
                <span className="flex size-9 shrink-0 items-center justify-center rounded-[9px] bg-primary text-primary-foreground shadow-[0_0_0_1px_oklch(0.72_0.18_257/0.18)]">
                  <AudioWaveform className="size-[19px]" aria-hidden="true" />
                </span>
                <span className="grid min-w-0 flex-1 text-left leading-tight">
                  <span className="truncate text-sm font-semibold text-white">
                    Market Squawk
                  </span>
                  <span className="truncate text-[11px] text-sidebar-foreground/55">
                    Active local workspace
                  </span>
                </span>
                <ChevronsUpDown
                  className="ml-auto size-3.5 text-sidebar-foreground/50"
                  aria-hidden="true"
                />
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        <nav aria-label="Market Squawk">
          <SidebarGroup>
            <SidebarGroupLabel>Workspace</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {workspaceNavigation.map((item) => (
                  <SidebarMenuItem key={item.path}>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === item.path}
                      tooltip={item.label}
                      className="relative h-9 gap-3 px-2.5 text-[13px] data-[active=true]:before:absolute data-[active=true]:before:inset-y-1 data-[active=true]:before:-left-2 data-[active=true]:before:w-0.5 data-[active=true]:before:rounded-full data-[active=true]:before:bg-primary"
                    >
                      <Link to={item.path}>
                        <item.icon aria-hidden="true" />
                        <span>{item.label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          <SidebarSeparator />

          <SidebarGroup>
            <SidebarGroupLabel>Operations</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {operationsNavigation.map((item) => (
                  <SidebarMenuItem key={item.path}>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === item.path}
                      tooltip={item.label}
                      className="relative h-9 gap-3 px-2.5 text-[13px] data-[active=true]:before:absolute data-[active=true]:before:inset-y-1 data-[active=true]:before:-left-2 data-[active=true]:before:w-0.5 data-[active=true]:before:rounded-full data-[active=true]:before:bg-primary"
                    >
                      <Link to={item.path}>
                        <item.icon aria-hidden="true" />
                        <span>{item.label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </nav>
      </SidebarContent>

      <SidebarFooter className="px-3 pb-4">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              size="lg"
              tooltip="Local system"
              className="h-auto border border-sidebar-border bg-sidebar-accent/40 px-2 py-2.5"
            >
              <span className="relative flex size-8 shrink-0 items-center justify-center rounded-md border border-sidebar-border bg-background font-mono text-[10px] font-semibold">
                MS
                <span
                  className={`absolute -right-1 -bottom-1 size-2.5 rounded-full border-2 border-sidebar ${localStatusColor}`}
                />
              </span>
              <span className="grid min-w-0 flex-1 text-left leading-tight">
                <span className="truncate text-xs font-semibold">Local system</span>
                <span className="flex items-center gap-1 truncate text-[10px] text-sidebar-foreground/55">
                  <LockKeyhole className="size-2.5" aria-hidden="true" />
                  {localStatus}
                </span>
              </span>
              <ChevronsUpDown className="ml-auto size-3.5" aria-hidden="true" />
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}
