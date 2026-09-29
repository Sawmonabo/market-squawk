import { useQuery } from "@tanstack/react-query"
import { useRef } from "react"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { useCursorNavigation } from "../shared/cursor-navigation"
import { parsePortfolioAccountPage, parsePortfolioAttribution, parsePortfolioExposure, parsePortfolioHoldings, parsePortfolioPerformance, parsePortfolioRevisions } from "./portfolio-contracts"
import type { PortfolioExposurePage, PortfolioHoldingsPage } from "./portfolio-contracts"

// Both account consumers share raw native responses and the same summary
// projection. Navigation retains only cursor identities and the current page.
export function usePortfolioAccounts(transport: ProductTransport, bootstrap: DesktopBootstrap) {
  const available = hasProductCapability(bootstrap, "portfolio_account_list")
  const navigation = useCursorNavigation()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.ListAccounts", { cursor: navigation.after, limit: 25 }),
    enabled: available, gcTime: 0,
    queryFn: ({ signal }) => transport.query({ query: "portfolioAccounts", cursor: navigation.after, limit: 25 }, { signal }),
    select: parsePortfolioAccountPage,
  })
  return { available, query, navigation }
}

// Holdings and exposure use the same immutable position-page lifecycle. Older
// pages retain cursor identities only; closing releases the request and data.
export function usePortfolioPositions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  mode: "holdings" | "exposure",
) {
  const kind = mode === "holdings" ? "portfolioHoldings" : "portfolioExposure"
  const available = hasProductCapability(bootstrap,
    kind === "portfolioHoldings" ? "portfolio_holdings" : "portfolio_exposure")
  const navigation = useCursorNavigation()
  const query = useQuery<PortfolioHoldingsPage | PortfolioExposurePage>({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "portfolio",
      kind === "portfolioHoldings" ? "Portfolio.GetHoldings" : "Portfolio.GetExposure",
      { accountToken, cursor: navigation.after, limit: 25 },
    ),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const result = await transport.query({ query: kind, accountToken, cursor: navigation.after, limit: 25 }, { signal })
      return mode === "holdings" ? parsePortfolioHoldings(result) : parsePortfolioExposure(result)
    },
  })
  const refresh = () => {
    navigation.restart()
    if (navigation.after === undefined) void query.refetch()
  }
  return { available, query, navigation, refresh }
}

// Mounted only while the selected account's cash and performance panel is open.
export function usePortfolioPerformance(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_performance")
  const query = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "portfolio",
      "Portfolio.GetPerformance",
      { accountToken },
    ),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => parsePortfolioPerformance(
      await transport.query({ query: "portfolioPerformance", accountToken }, { signal }),
    ),
  })
  return { available, query }
}

// History stays anchored to its first selected snapshot until the view refreshes.
// A fresh mounted read identity prevents old first-page cache reuse on refresh.
export function usePortfolioRevisions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  readSession: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_revision_list")
  const navigation = useCursorNavigation()
  const selectedSnapshot = useRef<string | null>(null)
  const firstPageCursor = useRef<string | null>(null)
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.ListRevisions",
      { accountToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioRevisions(await transport.query({
        query: "portfolioRevisions", accountToken,
        cursor: navigation.after ?? firstPageCursor.current ?? undefined, limit: 25,
      }, { signal }))
      if (signal.aborted) throw new Error("The saved portfolio read was cancelled.")
      if (selectedSnapshot.current !== null && selectedSnapshot.current !== page.selectedSnapshotToken) {
        throw new Error("The selected saved portfolio changed. Refresh history to start again.")
      }
      selectedSnapshot.current = page.selectedSnapshotToken
      if (navigation.after === undefined) firstPageCursor.current = page.pageCursor
      return page
    },
  })
  return { available, query, navigation }
}

export function usePortfolioAttribution(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  selectedSnapshotToken: string,
  baselineSnapshotToken: string,
  readSession: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_attribution")
  const navigation = useCursorNavigation()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.GetAttribution",
      { accountToken, selectedSnapshotToken, baselineSnapshotToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioAttribution(await transport.query({
        query: "portfolioAttribution", accountToken, selectedSnapshotToken,
        baselineSnapshotToken, cursor: navigation.after, limit: 25,
      }, { signal }))
      if (page.snapshotToken !== selectedSnapshotToken || page.baselineSnapshotToken !== baselineSnapshotToken) {
        throw new Error("The saved portfolio comparison does not match your selection.")
      }
      return page
    },
  })
  const restart = () => {
    navigation.restart()
    if (navigation.after === undefined) void query.refetch()
  }
  return { available, query, navigation, restart }
}
