import { ChevronsUpDown, LockKeyhole } from "lucide-react"
import { Link, useLocation } from "react-router-dom"

import { useProduct } from "@/app/product-context"
import marketSquawkMarkUrl from "@/assets/market-squawk-mark.svg"
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
  type NavigationItem,
  navigationAdmission,
  operationsNavigation,
  workspaceNavigation,
} from "@/lib/navigation"
import type { DesktopBootstrap } from "@/lib/schemas"

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
              className="h-11 justify-center px-1 py-2 group-data-[collapsible=icon]:justify-center"
            >
              <Link to="/overview" aria-label="Market Squawk workspace">
                <span className="flex min-w-0 items-center leading-none">
                  <span className="text-[18px] font-bold tracking-[-0.045em] text-white group-data-[collapsible=icon]:hidden">
                    Market
                  </span>
                  <img
                    src={marketSquawkMarkUrl}
                    alt=""
                    aria-hidden="true"
                    className="ml-0.5 h-[21px] w-auto shrink-0 group-data-[collapsible=icon]:ml-0 group-data-[collapsible=icon]:h-7"
                  />
                  <span
                    className="text-[18px] font-bold tracking-[-0.045em] text-primary group-data-[collapsible=icon]:hidden"
                    style={{ marginLeft: "-1px" }}
                  >
                    quawk
                  </span>
                </span>
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
                  <ProductNavigationItem
                    key={item.path}
                    item={item}
                    bootstrap={
                      product.status === "ready" ? product.bootstrap : null
                    }
                    active={location.pathname === item.path}
                  />
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
                  <ProductNavigationItem
                    key={item.path}
                    item={item}
                    bootstrap={
                      product.status === "ready" ? product.bootstrap : null
                    }
                    active={location.pathname === item.path}
                  />
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

function ProductNavigationItem({
  item,
  bootstrap,
  active,
}: {
  item: NavigationItem
  bootstrap: DesktopBootstrap | null
  active: boolean
}) {
  const admission = bootstrap
    ? navigationAdmission(item, bootstrap)
    : {
        admitted: item.path === "/overview",
        reason:
          item.path === "/overview"
            ? null
            : "Wait for the local application to finish starting.",
      }
  const className =
    "relative h-9 gap-3 px-2.5 text-[13px] data-[active=true]:before:absolute data-[active=true]:before:inset-y-1 data-[active=true]:before:-left-2 data-[active=true]:before:w-0.5 data-[active=true]:before:rounded-full data-[active=true]:before:bg-primary"

  return (
    <SidebarMenuItem>
      {admission.admitted ? (
        <SidebarMenuButton
          asChild
          isActive={active}
          tooltip={item.label}
          className={className}
        >
          <Link to={item.path}>
            <item.icon aria-hidden="true" />
            <span>{item.label}</span>
          </Link>
        </SidebarMenuButton>
      ) : (
        <SidebarMenuButton
          type="button"
          aria-disabled="true"
          title={admission.reason ?? undefined}
          tooltip={`${item.label} — ${admission.reason}`}
          className={`${className} cursor-not-allowed opacity-55`}
        >
          <item.icon aria-hidden="true" />
          <span>{item.label}</span>
          <LockKeyhole className="ml-auto size-3" aria-hidden="true" />
          <span className="sr-only">Unavailable: {admission.reason}</span>
        </SidebarMenuButton>
      )}
    </SidebarMenuItem>
  )
}
