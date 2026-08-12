import { useQuery } from "@tanstack/react-query"
import type { z } from "zod"

import { productKeys, type ProductScope } from "@/app/query-client"
import { messageFrom } from "@/app/product-context"
import { parseInvestmentAnalysisPage } from "@/features/opportunities/contracts"
import type { ApplicationResult } from "@/lib/schemas"
import type { DashboardQuery, ProductTransport } from "@/lib/transport"

import {
  decisionOverviewSchema,
  jobListSchema,
  marketSnapshotSchema,
  sourceHealthSchema,
} from "./schemas"

export type ReadState<T> =
  | { status: "loading"; data: null; message: null }
  | { status: "ready"; data: T; message: null }
  | { status: "unavailable"; data: null; message: string }

const OVERVIEW_INPUT = { query: "overview" } as const
const SOURCE_INPUT = { query: "sourceHealth" } as const
const MARKET_INPUT = { query: "marketSnapshot" } as const
const JOB_INPUT = { query: "jobs", limit: 24 } as const
const ANALYSIS_INPUT = {
  query: "decisionInvestmentAnalyses",
  limit: 12,
} as const

export function useOverviewQueries(
  transport: ProductTransport,
  scope: ProductScope,
) {
  const overview = useProductQuery(
    transport,
    scope,
    "analysis",
    "decision-overview",
    OVERVIEW_INPUT,
    decisionOverviewSchema,
  )
  const analyses = useParsedProductQuery(
    transport,
    scope,
    "decision",
    "investment-analyses",
    ANALYSIS_INPUT,
    (result) =>
      parseInvestmentAnalysisPage(result, { limit: ANALYSIS_INPUT.limit }),
  )
  const sources = useSourceHealthQuery(transport, scope)
  const markets = useMarketSnapshotQuery(transport, scope)
  const jobs = useJobListQuery(transport, scope)
  return { overview, analyses, sources, markets, jobs }
}

export function useOperationalQueries(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return {
    sources: useSourceHealthQuery(transport, scope),
    markets: useMarketSnapshotQuery(transport, scope),
    jobs: useJobListQuery(transport, scope),
  }
}

function useSourceHealthQuery(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return useProductQuery(
    transport,
    scope,
    "source",
    "health",
    SOURCE_INPUT,
    sourceHealthSchema,
  )
}

function useMarketSnapshotQuery(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return useProductQuery(
    transport,
    scope,
    "market",
    "snapshot",
    MARKET_INPUT,
    marketSnapshotSchema,
  )
}

function useJobListQuery(transport: ProductTransport, scope: ProductScope) {
  return useProductQuery(
    transport,
    scope,
    "job",
    "list",
    JOB_INPUT,
    jobListSchema,
  )
}

function useProductQuery<Schema extends z.ZodType>(
  transport: ProductTransport,
  scope: ProductScope,
  domain: string,
  operation: string,
  input: DashboardQuery,
  schema: Schema,
): ReadState<z.infer<Schema>> {
  return useParsedProductQuery(
    transport,
    scope,
    domain,
    operation,
    input,
    (result) => {
      const parsed = parseApplicationData(result, schema)
      if (!parsed.success) {
        throw new Error(
          "The installed service returned data this screen cannot safely interpret.",
        )
      }
      return parsed.data
    },
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
    return { status: "unavailable", data: null, message: messageFrom(query.error) }
  }
  try {
    return { status: "ready", data: parse(query.data), message: null }
  } catch (error) {
    return { status: "unavailable", data: null, message: messageFrom(error) }
  }
}

function parseApplicationData<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
) {
  return schema.safeParse(result.data)
}
