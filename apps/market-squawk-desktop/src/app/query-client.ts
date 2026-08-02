import { QueryClient } from "@tanstack/react-query"

import type { DesktopBootstrap } from "@/lib/schemas"

export type ProductScope = DesktopBootstrap["runtime"]

export const productKeys = {
  bootstrap: ["market-squawk", "bootstrap"] as const,
  root: (scope: ProductScope) =>
    [
      "market-squawk",
      scope.installationId,
      scope.workspaceId,
      scope.serviceGeneration,
    ] as const,
  domain: (scope: ProductScope, domain: string) =>
    [...productKeys.root(scope), "domain", domain] as const,
  operation: (
    scope: ProductScope,
    domain: string,
    operation: string,
    input: Readonly<Record<string, unknown>>,
  ) => [...productKeys.domain(scope, domain), operation, input] as const,
}

export function createProductQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 15_000,
        gcTime: 5 * 60_000,
        retry: 1,
        refetchOnWindowFocus: true,
        refetchOnReconnect: true,
      },
      mutations: { retry: false },
    },
  })
}
