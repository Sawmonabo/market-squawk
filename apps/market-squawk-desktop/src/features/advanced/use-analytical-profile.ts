import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import type { ProductTransport } from "@/lib/transport"

import { analyticalProductProjectionSchema } from "./analytical-profile-contracts"

export function useAnalyticalProductProjection(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return useQuery({
    queryKey: productKeys.operation(
      scope,
      "analysis",
      "Analysis.GetSettingsSummary",
      {},
    ),
    queryFn: async () => {
      const response = await transport.query({ query: "analysisSettings" })
      return analyticalProductProjectionSchema.parse(response.data)
    },
  })
}
