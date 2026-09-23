import { useQuery } from "@tanstack/react-query"
import { z } from "zod"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import { parsePortfolioResult, portfolioAccountSchema } from "./portfolio-contracts"

export function usePortfolioAccounts(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
) {
  const available = hasProductCapability(bootstrap, "portfolio_account_list")
  const query = useQuery({
    queryKey: [
      ...productKeys.domain(bootstrap.productSessionToken, "portfolio"),
      "accounts",
      "product-v1",
    ],
    enabled: available,
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({ query: "portfolioAccounts" }),
        z.array(portfolioAccountSchema).max(500),
        [],
      ),
  })
  return { available, query }
}
