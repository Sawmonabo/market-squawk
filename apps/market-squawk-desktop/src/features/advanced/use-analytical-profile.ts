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
    queryFn: async ({ signal }) => {
      const response = await transport.query({ query: "analysisSettings" }, { signal })
      return analyticalProductProjectionSchema.parse(response.data)
    },
  })
}

export function useAnalyticalControllerStatus(
  transport: Pick<ProductTransport, "analyticalController">,
  scope: ProductScope,
) {
  return useQuery({
    queryKey: productKeys.operation(scope, "analysis", "Desktop.AnalyticalProfiles", {}),
    queryFn: async ({ signal }) => {
      const response = await transport.analyticalController({ action: "status" }, false, { signal })
      if (response.kind !== "status") throw new Error("Analysis settings could not be opened.")
      return response
    },
    refetchInterval: (query) => query.state.data?.workflows.some((workflow) =>
      workflow.state === "waiting" || workflow.state === "in_progress" || workflow.state === "cancelling",
    ) ? 1_500 : false,
    refetchIntervalInBackground: false,
  })
}
