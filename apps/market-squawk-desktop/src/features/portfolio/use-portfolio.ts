import { useInfiniteQuery, useQuery } from "@tanstack/react-query"
import { z } from "zod"

import { productKeys, type ProductScope } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { DesktopBootstrap, ProductCapability } from "@/lib/schemas"
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
  "portfolio_holdings",
  "portfolio_performance",
  "portfolio_exposure",
  "portfolio_risk",
] as const satisfies readonly ProductCapability[]

const HISTORY_CAPABILITIES = [
  "portfolio_transactions",
  "portfolio_revision_list",
  "portfolio_attribution",
] as const satisfies readonly ProductCapability[]

export function usePortfolioAccounts(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
) {
  const available = hasProductCapability(bootstrap, "portfolio_account_list")
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
      if (page.evidence.state === "complete") return undefined
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
  const capabilityAvailable = Object.fromEntries(
    REQUIRED_DETAILS.map((capability) => [
      capability,
      hasProductCapability(bootstrap, capability),
    ]),
  ) as Record<(typeof REQUIRED_DETAILS)[number], boolean>
  const enabled = accountId !== null

  const holdings = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "holdings", {
      accountId,
    }),
    enabled: enabled && capabilityAvailable.portfolio_holdings,
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
    enabled: enabled && capabilityAvailable.portfolio_performance,
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
    enabled: enabled && capabilityAvailable.portfolio_exposure,
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
    enabled: enabled && capabilityAvailable.portfolio_risk,
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
    capabilityAvailable,
    holdings,
    performance,
    exposure,
    risk,
    refresh: () =>
      Promise.all([
        ...(enabled && capabilityAvailable.portfolio_holdings ? [holdings.refetch()] : []),
        ...(enabled && capabilityAvailable.portfolio_performance
          ? [performance.refetch()]
          : []),
        ...(enabled && capabilityAvailable.portfolio_exposure ? [exposure.refetch()] : []),
        ...(enabled && capabilityAvailable.portfolio_risk ? [risk.refetch()] : []),
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
  baselineSnapshotToken: string | null,
) {
  const capabilityAvailable = Object.fromEntries(
    HISTORY_CAPABILITIES.map((capability) => [
      capability,
      hasProductCapability(bootstrap, capability),
    ]),
  ) as Record<(typeof HISTORY_CAPABILITIES)[number], boolean>
  const enabled = accountId !== null

  const transactions = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "transactions", {
      accountId,
    }),
    enabled: enabled && capabilityAvailable.portfolio_transactions,
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
    enabled: enabled && capabilityAvailable.portfolio_revision_list,
    queryFn: async ({ pageParam }) =>
      parsePortfolioResult(
        await transport.query({
          query: "portfolioRevisions",
          accountId: requiredAccount(accountId),
          ...(pageParam ? { afterSnapshotToken: pageParam } : {}),
        }),
        z.array(portfolioRevisionSchema),
        [],
      ),
    getNextPageParam: (page) => {
      if (page.evidence.state === "complete") return undefined
      return page.value.at(-1)?.snapshotToken
    },
  })

  const attribution = useQuery({
    queryKey: productKeys.operation(scope, "portfolio", "attribution", {
      accountId,
      baselineSnapshotToken,
    }),
    enabled:
      enabled &&
      baselineSnapshotToken !== null &&
      capabilityAvailable.portfolio_attribution,
    queryFn: async () => {
      if (baselineSnapshotToken === null) {
        throw new Error("Choose an earlier portfolio snapshot first.")
      }
      return parsePortfolioResult(
        await transport.query({
          query: "portfolioAttribution",
          accountId: requiredAccount(accountId),
          baselineSnapshotToken,
        }),
        portfolioAttributionSchema,
      )
    },
  })

  return {
    capabilityAvailable,
    transactions,
    revisions,
    attribution,
  }
}

function requiredAccount(accountId: string | null): string {
  if (accountId === null) throw new Error("Select a portfolio account first.")
  return accountId
}
