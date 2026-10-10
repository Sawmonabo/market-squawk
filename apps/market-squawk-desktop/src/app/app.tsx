import { QueryClientProvider } from "@tanstack/react-query"
import * as React from "react"
import { useLocation } from "react-router-dom"

import { AppHeader } from "@/components/app-header"
import { AppSidebar } from "@/components/app-sidebar"
import { StatusRail } from "@/components/status-rail"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import type { DesktopTransport } from "@/lib/transport"

import { ProductProvider } from "./product-context"
import { createProductQueryClient } from "./query-client"
import { AppRoutes } from "./routes"

export function App({ transport }: { transport: DesktopTransport }) {
  const [queryClient] = React.useState(createProductQueryClient)
  const { pathname } = useLocation()
  const page = React.useRef<HTMLDivElement>(null)
  React.useLayoutEffect(() => {
    window.scrollTo({ top: 0, left: 0, behavior: "instant" })
    page.current?.scrollTo({ top: 0, left: 0, behavior: "instant" })
  }, [pathname])
  return (
    <QueryClientProvider client={queryClient}>
      <ProductProvider transport={transport}>
        <SidebarProvider
          style={
            {
              "--sidebar-width": "16.125rem",
              "--sidebar-width-icon": "4.875rem",
            } as React.CSSProperties
          }
        >
          <AppSidebar />
          <SidebarInset className="min-w-0">
            <AppHeader />
            <StatusRail />
            <div ref={page} className="min-h-0 flex-1 overflow-auto">
              <AppRoutes />
            </div>
          </SidebarInset>
        </SidebarProvider>
      </ProductProvider>
    </QueryClientProvider>
  )
}
