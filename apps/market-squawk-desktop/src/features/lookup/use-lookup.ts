import * as React from "react"
import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import type { ProductTransport } from "@/lib/transport"

import {
  lookupCategories,
  lookupResultSchema,
  normalizeLookupQuery,
  type LookupCategory,
  type LookupMatch,
  type LookupResult,
} from "./schemas"

export type ProductLookupMatch = LookupMatch

export type LookupState =
  | { status: "idle"; data: null; message: null }
  | { status: "loading"; data: null; message: null }
  | { status: "ready"; data: LookupResult; message: null }
  | { status: "unavailable"; data: null; message: string }

export function useLookup(
  transport: ProductTransport,
  scope: ProductScope,
  text: string,
  categories: LookupCategory[],
): LookupState {
  const normalized = normalizeLookupQuery(text)
  const deferred = React.useDeferredValue(normalized)
  const ready = deferred !== null && [...deferred].length >= 2
  const input = React.useMemo(
    () => ({
      query: "lookup" as const,
      text: deferred ?? "",
      categories: categories.length === 0 ? [...lookupCategories] : categories,
    }),
    [categories, deferred],
  )
  const query = useQuery({
    queryKey: productKeys.operation(scope, "analysis", "lookup", input),
    queryFn: () => transport.query(input),
    enabled: ready,
    staleTime: 30_000,
  })

  if (!ready) {
    return { status: "idle", data: null, message: null }
  }
  if (query.isPending || deferred !== normalized) {
    return { status: "loading", data: null, message: null }
  }
  if (query.isError) {
    return {
      status: "unavailable",
      data: null,
      message: "Search is unavailable right now.",
    }
  }
  const parsed = lookupResultSchema.safeParse(query.data.data)
  if (!parsed.success) {
    return {
      status: "unavailable",
      data: null,
      message: "Search results are unavailable right now.",
    }
  }

  return {
    status: "ready",
    data: parsed.data,
    message: null,
  }
}
