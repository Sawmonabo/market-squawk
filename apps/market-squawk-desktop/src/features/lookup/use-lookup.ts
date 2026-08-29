import * as React from "react"
import { useQuery } from "@tanstack/react-query"

import { productKeys, type ProductScope } from "@/app/query-client"
import { messageFrom } from "@/app/product-context"
import type { ProductTransport } from "@/lib/transport"

import {
  lookupResultSchema,
  productLookupCategories,
  type LookupCategory,
  type LookupMatch,
  type LookupResult,
  type ProductLookupCategory,
} from "./schemas"

const productCategorySet = new Set<LookupCategory>(productLookupCategories)

export type ProductLookupMatch = Omit<LookupMatch, "category"> & {
  category: ProductLookupCategory
}

type ProductLookupResult = Omit<LookupResult, "matches" | "categories"> & {
  matches: ProductLookupMatch[]
  categories: Array<{
    category: ProductLookupCategory
    state: "available" | "unavailable"
    reason?: string
  }>
}

export type LookupState =
  | { status: "idle"; data: null; message: null }
  | { status: "loading"; data: null; message: null }
  | { status: "ready"; data: ProductLookupResult; message: null }
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
      categories: categories.length === 0 ? [...productLookupCategories] : categories,
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
  if (!parsed.success) {
    return {
        status: "unavailable",
        data: null,
        message: "Search results are unavailable right now.",
      }
  }

  return {
    status: "ready",
    data: {
      ...parsed.data,
      matches: parsed.data.matches.filter(isProductLookupMatch),
      categories: parsed.data.categories.filter(isProductLookupCategoryState),
    },
    message: null,
  }
}

function isProductLookupMatch(match: LookupMatch): match is ProductLookupMatch {
  return productCategorySet.has(match.category)
}

function isProductLookupCategoryState(
  category: LookupResult["categories"][number],
): category is ProductLookupResult["categories"][number] {
  return productCategorySet.has(category.category)
}
