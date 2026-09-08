import { QueryClient } from "@tanstack/react-query"

import type { DesktopBootstrap } from "@/lib/schemas"

export type ProductScope = DesktopBootstrap["productSessionToken"]

export const productKeys = {
  bootstrap: ["market-squawk", "bootstrap"] as const,
  root: (scope: ProductScope) => ["market-squawk", scope] as const,
  domain: (scope: ProductScope, domain: string) =>
    [...productKeys.root(scope), "domain", domain] as const,
  operation: (
    scope: ProductScope,
    domain: string,
    operation: string,
    input: Readonly<object>,
  ) => [...productKeys.domain(scope, domain), operation, input] as const,
}

export function createProductQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: Number.POSITIVE_INFINITY,
        gcTime: 5 * 60_000,
        retry: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
      },
      mutations: { retry: false },
    },
  })
}
