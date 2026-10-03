import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import type { ProductTransport } from "@/lib/transport"

import { parseInvestmentAnalysis, parseInvestmentAnalysisPage } from "./contracts"

const ANALYSIS_PAGE_LIMIT = 24

export function useSavedInvestmentAnalyses({ transport, scope, available, after }: {
  transport: ProductTransport
  scope: ProductScope
  available: boolean
  after: string | undefined
}) {
  const request = {
    limit: ANALYSIS_PAGE_LIMIT,
    ...(after ? { afterActionToken: after } : {}),
  }
  return useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.ListInvestmentAnalyses", request),
    gcTime: 0,
    enabled: available,
    queryFn: async ({ signal }) => parseInvestmentAnalysisPage(
      await transport.query({ query: "decisionInvestmentAnalyses", ...request }, { signal }),
      request,
    ),
  })
}

export function useSavedInvestmentAnalysis({ transport, scope, available, actionToken }: {
  transport: ProductTransport
  scope: ProductScope
  available: boolean
  actionToken: string | null
}) {
  return useQuery({
    queryKey: productKeys.operation(scope, "decision", "Decision.GetInvestmentAnalysis", { actionToken }),
    enabled: available && actionToken !== null,
    gcTime: 0,
    queryFn: async ({ signal }) => {
      if (actionToken === null) throw new Error("Select a saved analysis before opening it.")
      return parseInvestmentAnalysis(
        await transport.query({ query: "decisionInvestmentAnalysis", actionToken }, { signal }),
        actionToken,
      )
    },
  })
}
