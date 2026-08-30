import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import type { SystemTransport } from "@/lib/transport"

export function useAnalyticalControllerStatus(
  transport: SystemTransport,
  scope: ProductScope,
) {
  return useQuery({
    queryKey: productKeys.operation(
      scope,
      "desktop_analytical_controller",
      "status",
      {},
    ),
    queryFn: async () => {
      const response = await transport.analyticalController({ action: "status" })
      if (response.kind !== "status") {
        throw new Error("The Desktop analytical controller returned an unsupported status.")
      }
      return response
    },
  })
}
