import { useQuery } from "@tanstack/react-query"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { useCursorNavigation } from "../shared/cursor-navigation"
import { parsePortfolioAccountPage, parsePortfolioHoldings, parsePortfolioPerformance } from "./portfolio-contracts"

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

// Mounted only while the selected account's positions panel is open. Older
// pages retain cursor identities only; closing releases the request and data.
export function usePortfolioHoldings(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_holdings")
  const navigation = useCursorNavigation()
  const query = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "portfolio",
      "Portfolio.GetHoldings",
      { accountToken, cursor: navigation.after, limit: 25 },
    ),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => parsePortfolioHoldings(
      await transport.query({ query: "portfolioHoldings", accountToken, cursor: navigation.after, limit: 25 }, { signal }),
    ),
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
