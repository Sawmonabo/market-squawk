import { useQuery } from "@tanstack/react-query"
import type { z } from "zod"

import { productKeys, type ProductScope } from "@/app/query-client"
import { messageFrom } from "@/app/product-context"
import type { ApplicationResult } from "@/lib/schemas"
import type { DashboardQuery, ProductTransport } from "@/lib/transport"

import {
  decisionOverviewSchema,
  jobListSchema,
  marketSnapshotSchema,
  paperStatusSchema,
  sourceHealthSchema,
} from "./schemas"

export type ReadState<T> =
  | { status: "loading"; data: null; message: null }
  | { status: "ready"; data: T; message: null }
  | { status: "unavailable"; data: null; message: string }

const OVERVIEW_INPUT = { query: "overview" } as const
const SOURCE_INPUT = { query: "sourceHealth" } as const
const MARKET_INPUT = { query: "marketSnapshot" } as const
const PAPER_INPUT = { query: "paperStatus" } as const
const JOB_INPUT = { query: "jobs", limit: 24 } as const

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
  const operational = useOperationalQueries(transport, scope)
  return { overview, ...operational }
}

export function useOperationalQueries(
  transport: ProductTransport,
  scope: ProductScope,
) {
  return {
    sources: useProductQuery(
      transport,
      scope,
      "source",
      "health",
      SOURCE_INPUT,
      sourceHealthSchema,
    ),
    markets: useProductQuery(
      transport,
      scope,
      "market",
      "snapshot",
      MARKET_INPUT,
      marketSnapshotSchema,
    ),
    paper: useProductQuery(
      transport,
      scope,
      "bot",
      "status",
      PAPER_INPUT,
      paperStatusSchema,
    ),
    jobs: useProductQuery(
      transport,
      scope,
      "job",
      "list",
      JOB_INPUT,
      jobListSchema,
    ),
  }
}

function useProductQuery<Schema extends z.ZodType>(
  transport: ProductTransport,
  scope: ProductScope,
  domain: string,
  operation: string,
  input: DashboardQuery,
  schema: Schema,
): ReadState<z.infer<Schema>> {
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
  const parsed = parseApplicationData(query.data, schema)
  return parsed.success
    ? { status: "ready", data: parsed.data, message: null }
    : {
        status: "unavailable",
        data: null,
        message: "The installed service returned data this screen cannot safely interpret.",
      }
}

function parseApplicationData<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
) {
  return schema.safeParse(result.data)
}
