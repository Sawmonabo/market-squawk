import { useInfiniteQuery, useQuery } from "@tanstack/react-query"
import { z } from "zod"

import { productKeys, type ProductScope } from "@/app/query-client"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

import {
  portfolioAttributionSchema,
  portfolioRevisionSchema,
  portfolioTransactionSchema,
  exposureSchema,
  holdingSchema,
  parsePortfolioResult,
  performanceSchema,
  portfolioAccountSchema,
  riskSchema,
} from "./portfolio-contracts"

const REQUIRED_DETAILS = [
  "Portfolio.GetHoldings",
  "Portfolio.GetPerformance",
  "Portfolio.GetExposure",
  "Portfolio.GetRisk",
] as const

const HISTORY_OPERATIONS = [
  "Portfolio.GetTransactions",
  "Portfolio.ListRevisions",
  "Portfolio.GetAttribution",
] as const

export function usePortfolioAccounts(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
) {
  const available = hasOperation(bootstrap, "Portfolio.ListAccounts")
  const query = useInfiniteQuery({
    queryKey: [
      ...productKeys.domain(bootstrap.runtime, "portfolio"),
      "accounts",
    ],
    initialPageParam: undefined as string | undefined,
    enabled: available,
    queryFn: async ({ pageParam }) =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioAccounts",
          ...(pageParam ? { afterAccountId: pageParam } : {}),
        }),
        z.array(portfolioAccountSchema),
        [],
      ),
    getNextPageParam: (page) => {
      if (page.evidence.completeness === "complete") return undefined
      return page.value.at(-1)?.accountId
    },
  })
  return { available, query }
}

export function usePortfolioDetails(
  transport: ProductTransport,
  scope: ProductScope,
  bootstrap: DesktopBootstrap,
  accountId: string | null,
) {
  const operationAvailable = Object.fromEntries(
    REQUIRED_DETAILS.map((operation) => [
      operation,
      hasOperation(bootstrap, operation),
    ]),
  ) as Record<(typeof REQUIRED_DETAILS)[number], boolean>
  const enabled = accountId !== null

  const holdings = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "holdings", {
      accountId,
    }),
    enabled: enabled && operationAvailable["Portfolio.GetHoldings"],
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioHoldings",
          accountId: requiredAccount(accountId),
        }),
        z.array(holdingSchema),
        [],
      ),
  })
  const performance = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "performance", {
      accountId,
    }),
    enabled: enabled && operationAvailable["Portfolio.GetPerformance"],
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioPerformance",
          accountId: requiredAccount(accountId),
        }),
        performanceSchema,
      ),
  })
  const exposure = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "exposure", {
      accountId,
    }),
    enabled: enabled && operationAvailable["Portfolio.GetExposure"],
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioExposure",
          accountId: requiredAccount(accountId),
        }),
        exposureSchema,
      ),
  })
  const risk = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "risk", {
      accountId,
    }),
    enabled: enabled && operationAvailable["Portfolio.GetRisk"],
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioRisk",
          accountId: requiredAccount(accountId),
        }),
        riskSchema,
      ),
  })

  return {
    operationAvailable,
    holdings,
    performance,
    exposure,
    risk,
    refresh: () =>
      Promise.all([
        ...(enabled && operationAvailable["Portfolio.GetHoldings"] ? [holdings.refetch()] : []),
        ...(enabled && operationAvailable["Portfolio.GetPerformance"]
          ? [performance.refetch()]
          : []),
        ...(enabled && operationAvailable["Portfolio.GetExposure"] ? [exposure.refetch()] : []),
        ...(enabled && operationAvailable["Portfolio.GetRisk"] ? [risk.refetch()] : []),
      ]),
    isFetching:
      holdings.isFetching ||
      performance.isFetching ||
      exposure.isFetching ||
      risk.isFetching,
  }
}

export function usePortfolioHistory(
  transport: ProductTransport,
  scope: ProductScope,
  bootstrap: DesktopBootstrap,
  accountId: string | null,
  baselineRevisionId: string | null,
) {
  const operationAvailable = Object.fromEntries(
    HISTORY_OPERATIONS.map((operation) => [
      operation,
      hasOperation(bootstrap, operation),
    ]),
  ) as Record<(typeof HISTORY_OPERATIONS)[number], boolean>
  const enabled = accountId !== null

  const transactions = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "transactions", {
      accountId,
    }),
    enabled: enabled && operationAvailable["Portfolio.GetTransactions"],
    queryFn: async () =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioTransactions",
          accountId: requiredAccount(accountId),
        }),
        z.array(portfolioTransactionSchema),
        [],
      ),
  })

  const revisions = useInfiniteQuery({
    queryKey: productKeys.operation(scope, "portfolio", "revisions", {
      accountId,
    }),
    initialPageParam: undefined as string | undefined,
    enabled: enabled && operationAvailable["Portfolio.ListRevisions"],
    queryFn: async ({ pageParam }) =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioRevisions",
          accountId: requiredAccount(accountId),
          ...(pageParam ? { afterRevisionId: pageParam } : {}),
        }),
        z.array(portfolioRevisionSchema),
        [],
      ),
    getNextPageParam: (page) => {
      if (page.evidence.completeness === "complete") return undefined
      return page.value.at(-1)?.revisionId
    },
  })

  const attribution = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "attribution", {
      accountId,
      baselineRevisionId,
    }),
    enabled:
      enabled &&
      baselineRevisionId !== null &&
      operationAvailable["Portfolio.GetAttribution"],
    queryFn: async () => {
      if (baselineRevisionId === null) {
        throw new Error("Choose an earlier portfolio revision first.")
      }
      return parsePortfolioResult(
        await transport.query({
          query: "portfolioAttribution",
          accountId: requiredAccount(accountId),
          baselineRevisionId,
        }),
        portfolioAttributionSchema,
      )
    },
  })

  return {
    operationAvailable,
    transactions,
    revisions,
    attribution,
  }
}

function hasOperation(bootstrap: DesktopBootstrap, name: string) {
  return bootstrap.operations.some((operation) => operation.name === name)
}

function requiredAccount(accountId: string | null): string {
  if (accountId === null) throw new Error("Select a portfolio account first.")
  return accountId
}
