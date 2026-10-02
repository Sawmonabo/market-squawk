import { QueryClient } from "@tanstack/react-query"

import type { DesktopBootstrap, DesktopInvalidationDomain } from "@/lib/schemas"

export interface ProductQueryMeta extends Record<string, unknown> {
  domainRefresh?: "automatic" | "explicit"
}

declare module "@tanstack/react-query" {
  interface Register {
    queryMeta: ProductQueryMeta
  }
}

// These reads acquire or address a frozen snapshot. Routine publication events
// do not replace that snapshot; explicit reads, source authority changes and full reconnect refreshes do.
export const snapshotQueryMeta = { domainRefresh: "explicit" } as const satisfies ProductQueryMeta

// Current display projections reuse the bounded session cache and revalidate on return.
// Do not apply this policy to owned snapshot/read handles or consequential operations.
export const currentDisplayQueryOptions = {
  staleTime: 0,
  refetchOnMount: "always",
} as const

export type ProductScope = DesktopBootstrap["productSessionToken"]

export const productKeys = {
  bootstrap: ["market-squawk", "bootstrap"] as const,
  root: (scope: ProductScope) => ["market-squawk", scope] as const,
  domain: (scope: ProductScope, domain: DesktopInvalidationDomain) =>
    [...productKeys.root(scope), "domain", domain] as const,
  operation: (
    scope: ProductScope,
    domain: DesktopInvalidationDomain,
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
