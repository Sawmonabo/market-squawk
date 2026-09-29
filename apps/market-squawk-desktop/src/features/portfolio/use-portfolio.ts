import { useQuery } from "@tanstack/react-query"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"
import { useCursorNavigation } from "../shared/cursor-navigation"
import { parsePortfolioAccountPage } from "./portfolio-contracts"

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
