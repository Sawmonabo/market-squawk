import * as React from "react"
import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import { messageFrom } from "@/app/product-context"
import type { ProductTransport } from "@/lib/transport"

import {
  lookupResultSchema,
  type LookupCategory,
  type LookupResult,
} from "./schemas"

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
  const normalized = text.trim().slice(0, 256)
  const deferred = React.useDeferredValue(normalized)
  const input = React.useMemo(
    () => ({
      query: "lookup" as const,
      text: deferred,
      categories: categories.length === 0 ? undefined : categories,
    }),
    [categories, deferred],
  )
  const query = useQuery({
    queryKey: productKeys.operation(scope, "analysis", "lookup", input),
    queryFn: () => transport.query(input),
    enabled: deferred.length >= 2,
    staleTime: 30_000,
  })

  if (deferred.length < 2) {
    return { status: "idle", data: null, message: null }
  }
  if (query.isPending || deferred !== normalized) {
    return { status: "loading", data: null, message: null }
  }
  if (query.isError) {
    return { status: "unavailable", data: null, message: messageFrom(query.error) }
  }
  const parsed = lookupResultSchema.safeParse(query.data.data)
  return parsed.success
    ? { status: "ready", data: parsed.data, message: null }
    : {
        status: "unavailable",
        data: null,
        message: "The installed service returned results this screen cannot safely interpret.",
      }
}
