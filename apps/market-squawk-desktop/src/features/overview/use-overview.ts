import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import { marketOverviewRows } from "@/features/markets/market-product"
import { parseInvestmentAnalysisPage } from "@/features/opportunities/contracts"
import {
  parseResearchActivities,
  type ResearchActivity,
} from "@/features/research/research-contracts"
import type { ApplicationResult } from "@/lib/schemas"
import type { DashboardQuery, ProductTransport } from "@/lib/transport"

export type ReadState<T> =
  | { status: "loading"; data: null; message: null }
  | { status: "ready"; data: T; message: null }
  | { status: "unavailable"; data: null; message: string }

const MARKET_INPUT = { query: "marketOverview" } as const
const ACTIVITY_INPUT = { query: "researchActivities" } as const
const ANALYSIS_INPUT = {
  query: "decisionInvestmentAnalyses",
  limit: 12,
} as const

export function isActiveResearchActivity(activity: ResearchActivity) {
  return [
    "queued",
    "preparing",
    "running",
    "awaiting_confirmation",
    "cancelling",
    "recovering",
  ].includes(activity.state)
}

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
  const activities = useResearchActivityQuery(transport, scope)
  return { activities, analyses, markets }
}

export function useHomeStatusQueries(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return {
    activities: useResearchActivityQuery(transport, scope),
    markets: useMarketOverviewQuery(transport, scope),
  }
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
    marketOverviewRows,
  )
}

function useResearchActivityQuery(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return useParsedProductQuery(
    transport,
    scope,
    "research",
    "activity",
    ACTIVITY_INPUT,
    parseResearchActivities,
  )
}

function useParsedProductQuery<Result>(
  transport: ProductTransport,
  scope: ProductScope,
  domain: string,
  operation: string,
  input: DashboardQuery,
  parse: (result: ApplicationResult) => Result,
): ReadState<Result> {
  const query = useQuery({
    queryKey: productKeys.operation(scope, domain, operation, input),
    queryFn: () => transport.query(input),
  })

  if (query.isPending) {
    return { status: "loading", data: null, message: null }
  }
  if (query.isError) {
    return {
      status: "unavailable",
      data: null,
      message: "This information is unavailable right now.",
    }
  }
  try {
    return { status: "ready", data: parse(query.data), message: null }
  } catch {
    return {
      status: "unavailable",
      data: null,
      message: "This information is unavailable right now.",
    }
  }
}
