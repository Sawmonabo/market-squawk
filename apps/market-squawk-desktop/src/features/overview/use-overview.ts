import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import { parseMarketProductResult } from "@/features/markets/market-product"
import { parseInvestmentAnalysisPage } from "@/features/opportunities/contracts"
import type { ApplicationResult } from "@/lib/schemas"
import type { ProductQuery, ProductTransport } from "@/lib/transport"

export type ReadState<T> =
  | { status: "loading"; data: null }
  | { status: "ready"; data: T }
  | { status: "unavailable"; data: null }

const MARKET_INPUT = { query: "marketOverview" } as const
const ANALYSIS_INPUT = {
  query: "decisionInvestmentAnalyses",
  limit: 4,
} as const

export function useOverviewQueries(
  transport: ProductTransport,
  scope: ProductScope,
) {
  const analyses = useParsedProductQuery(
    transport,
    scope,
    "decision",
    "investment-analyses",
    ANALYSIS_INPUT,
    (result) =>
      parseInvestmentAnalysisPage(result, { limit: ANALYSIS_INPUT.limit }),
  )
  const markets = useMarketOverviewQuery(transport, scope)
  return { analyses, markets }
}

function useMarketOverviewQuery(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return useParsedProductQuery(
    transport,
    scope,
    "market",
    "overview",
    MARKET_INPUT,
    (result) => parseMarketProductResult(result).data,
  )
}

function useParsedProductQuery<Result>(
  transport: ProductTransport,
  scope: ProductScope,
  domain: string,
  operation: string,
  input: ProductQuery,
  parse: (result: ApplicationResult) => Result,
): ReadState<Result> {
  const query = useQuery({
    queryKey: productKeys.operation(scope, domain, operation, input),
    queryFn: () => transport.query(input),
  })

  if (query.isPending) {
    return { status: "loading", data: null }
  }
  if (query.isError) {
    return { status: "unavailable", data: null }
  }
  try {
    return { status: "ready", data: parse(query.data) }
  } catch {
    return { status: "unavailable", data: null }
  }
}
