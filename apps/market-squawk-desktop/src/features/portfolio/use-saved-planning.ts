import { useQuery } from "@tanstack/react-query"
import { useRef } from "react"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { useCursorNavigation } from "../shared/cursor-navigation"

import { parseSavedPlanningDetail, parseSavedPlanningPage } from "./saved-planning-contracts"
import type { SavedPlanningSummary } from "./saved-planning-contracts"

// Keep only one summary page plus cursor identities. Refetch uses the original
// first-page fence; only refresh/remount chooses the newly saved result set.
export function useSavedPlanningResults(transport: ProductTransport, bootstrap: DesktopBootstrap,
  accountToken: string, readSession: string) {
  const available = hasProductCapability(bootstrap, "portfolio_planning_list")
  const navigation = useCursorNavigation()
  const firstPageCursor = useRef<string | undefined>(undefined)
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.ListPlanningResults",
      { accountToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parseSavedPlanningPage(await transport.query({ query: "portfolioPlanningResults",
        accountToken, cursor: navigation.after ?? firstPageCursor.current, limit: 25,
      }, { signal }), accountToken)
      if (signal.aborted) throw new Error("The saved planning read was cancelled.")
      if (navigation.after === undefined) firstPageCursor.current = page.pageCursor
      return page
    },
  })
  return { available, query, navigation }
}

export function useSavedPlanningResult(transport: ProductTransport, bootstrap: DesktopBootstrap,
  selected: SavedPlanningSummary, readSession: string) {
  const available = hasProductCapability(bootstrap, "portfolio_planning_get")
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.GetPlanningResult",
      { accountToken: selected.accountToken, savedResultToken: selected.savedResultToken, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const result = await transport.query({ query: "portfolioPlanningResult",
        accountToken: selected.accountToken, savedResultToken: selected.savedResultToken,
      }, { signal })
      if (signal.aborted) throw new Error("The saved planning read was cancelled.")
      return parseSavedPlanningDetail(result, selected)
    },
  })
  return { available, query }
}
