import { useQuery } from "@tanstack/react-query"
import { useEffect, useRef, useState } from "react"

import { productKeys } from "@/app/query-client"
import { hasProductCapability } from "@/lib/product-capabilities"
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import type { ProductQuery, ProductTransport } from "@/lib/transport"
import { useCursorNavigation } from "../shared/cursor-navigation"
import { parsePortfolioAccountPage, parsePortfolioAttribution, parsePortfolioExposure, parsePortfolioHoldings, parsePortfolioPerformance, parsePortfolioRevisions, parsePortfolioRebalanceReport, parsePortfolioScenarioReport, parsePortfolioTransactions, parsePortfolioCandidateImpact } from "./portfolio-contracts"
import type { PortfolioExposurePage, PortfolioHoldingsPage, PortfolioRebalanceInput, PortfolioRebalanceReport, PortfolioScenarioInput, PortfolioScenarioReport, PortfolioCandidateImpact, PortfolioCandidateImpactInput } from "./portfolio-contracts"

// Both account consumers share raw native responses and the same summary
// projection. Navigation retains only cursor identities and the current page.
export function usePortfolioAccounts(transport: ProductTransport, bootstrap: DesktopBootstrap) {
  const available = hasProductCapability(bootstrap, "portfolio_account_list")
  const navigation = useCursorNavigation()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.ListAccounts", { cursor: navigation.after, limit: 25 }),
    enabled: available, gcTime: 0,
    queryFn: ({ signal }) => transport.query({ query: "portfolioAccounts", cursor: navigation.after, limit: 25 }, { signal }),
    select: parsePortfolioAccountPage,
  })
  return { available, query, navigation }
}

// Holdings and exposure use the same immutable position-page lifecycle. Older
// pages retain cursor identities only; closing releases the request and data.
export function usePortfolioPositions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  mode: "holdings" | "exposure",
) {
  const kind = mode === "holdings" ? "portfolioHoldings" : "portfolioExposure"
  const available = hasProductCapability(bootstrap,
    kind === "portfolioHoldings" ? "portfolio_holdings" : "portfolio_exposure")
  const navigation = useCursorNavigation()
  const query = useQuery<PortfolioHoldingsPage | PortfolioExposurePage>({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "portfolio",
      kind === "portfolioHoldings" ? "Portfolio.GetHoldings" : "Portfolio.GetExposure",
      { accountToken, cursor: navigation.after, limit: 25 },
    ),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const result = await transport.query({ query: kind, accountToken, cursor: navigation.after, limit: 25 }, { signal })
      return mode === "holdings" ? parsePortfolioHoldings(result) : parsePortfolioExposure(result)
    },
  })
  const refresh = () => {
    navigation.restart()
    if (navigation.after === undefined) void query.refetch()
  }
  return { available, query, navigation, refresh }
}

// Mounted only while the selected account's cash and performance panel is open.
export function usePortfolioPerformance(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_performance")
  const query = useQuery({
    queryKey: productKeys.operation(
      bootstrap.productSessionToken,
      "portfolio",
      "Portfolio.GetPerformance",
      { accountToken },
    ),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => parsePortfolioPerformance(
      await transport.query({ query: "portfolioPerformance", accountToken }, { signal }),
    ),
  })
  return { available, query }
}

// History stays anchored to its first selected snapshot until the view refreshes.
// A fresh mounted read identity prevents old first-page cache reuse on refresh.
export function usePortfolioRevisions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  readSession: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_revision_list")
  const navigation = useCursorNavigation()
  const pinned = usePinnedPortfolioPage()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.ListRevisions",
      { accountToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioRevisions(await transport.query({
        query: "portfolioRevisions", accountToken,
        cursor: pinned.cursor(navigation.after), limit: 25,
      }, { signal }))
      pinned.retain(page.selectedSnapshotToken, page.pageCursor, navigation.after, signal)
      return page
    },
  })
  return { available, query, navigation }
}

// Transactions and saved-version listings retain the initial page cursor for
// retries/refetches. Only a newly mounted read may select the latest snapshot.
function usePinnedPortfolioPage() {
  const snapshot = useRef<string | null>(null)
  const firstPageCursor = useRef<string | null>(null)
  return {
    cursor: (after: string | undefined) => after ?? firstPageCursor.current ?? undefined,
    retain: (snapshotToken: string, pageCursor: string, after: string | undefined, signal: AbortSignal) => {
      if (signal.aborted) throw new Error("The saved portfolio read was cancelled.")
      if (snapshot.current !== null && snapshot.current !== snapshotToken) {
        throw new Error("The selected saved portfolio changed. Refresh to start again.")
      }
      snapshot.current = snapshotToken
      if (after === undefined) firstPageCursor.current = pageCursor
    },
  }
}

export type PortfolioPlanningSelection = Pick<PortfolioHoldingsPage,
  "snapshotToken" | "effectiveAtUnixNanos" | "availableAtUnixNanos"> & { accountId?: string }

// This independent selector keeps one page of holdings and a minimal immutable
// selection. Refresh remounts it, releasing its pin and every prior assumption.
export function usePortfolioPlanningPositions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  readSession: string,
  mode: "scenario" | "rebalance",
) {
  const available = hasProductCapability(bootstrap, "portfolio_holdings")
    && (mode === "rebalance" ? hasProductCapability(bootstrap, "portfolio_rebalance")
      : (hasProductCapability(bootstrap, "portfolio_scenario")
        || hasProductCapability(bootstrap, "portfolio_scenario_batch")))
  const navigation = useCursorNavigation()
  const pinned = usePinnedPortfolioPage()
  const [selection, setSelection] = useState<PortfolioPlanningSelection | null>(null)
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.GetHoldings",
      { accountToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioHoldings(await transport.query({
        query: "portfolioHoldings", accountToken, cursor: pinned.cursor(navigation.after), limit: 25,
      }, { signal }))
      pinned.retain(page.snapshotToken, page.pageCursor, navigation.after, signal)
      setSelection((current) => current ?? {
        snapshotToken: page.snapshotToken,
        effectiveAtUnixNanos: page.effectiveAtUnixNanos,
        availableAtUnixNanos: page.availableAtUnixNanos,
        accountId: page.holdings[0]?.accountId,
      })
      return page
    },
  })
  return { available, query, navigation, selection }
}

// Calculations are transient reads. Editing, cancellation, and unmount all
// retire the request identity before aborting; even a late reply is discarded.
function usePortfolioCalculation<Result>(
  transport: ProductTransport,
  accountToken: string,
  snapshotToken: string | null,
  failureMessage: string,
) {
  const active = useRef<AbortController | null>(null)
  const [state, setState] = useState<{
    pending: boolean; result: Result | null; error: string | null; cancelled: boolean
  }>({ pending: false, result: null, error: null, cancelled: false })
  const retire = () => {
    const controller = active.current
    active.current = null
    controller?.abort()
  }
  useEffect(() => {
    setState({ pending: false, result: null, error: null, cancelled: false })
    return () => {
      const controller = active.current
      active.current = null
      controller?.abort()
    }
  }, [accountToken, snapshotToken, transport])

  const invalidate = () => {
    retire()
    setState({ pending: false, result: null, error: null, cancelled: false })
  }
  const cancel = () => {
    retire()
    setState({ pending: false, result: null, error: null, cancelled: true })
  }
  const calculate = async (request: ProductQuery, parse: (response: ApplicationResult) => Result) => {
    retire()
    const controller = new AbortController()
    active.current = controller
    setState({ pending: true, result: null, error: null, cancelled: false })
    try {
      const response = await transport.query(request, { signal: controller.signal })
      if (active.current !== controller || controller.signal.aborted) return
      const result = parse(response)
      active.current = null
      setState({ pending: false, result, error: null, cancelled: false })
    } catch (error) {
      if (active.current !== controller || controller.signal.aborted) return
      active.current = null
      setState({ pending: false, result: null,
        error: failureMessage, cancelled: false })
    }
  }
  return { ...state, calculate, invalidate, cancel }
}

export function usePortfolioScenarioCalculation(
  transport: ProductTransport,
  accountToken: string,
  selection: PortfolioPlanningSelection | null,
) {
  const calculation = usePortfolioCalculation<PortfolioScenarioReport>(transport, accountToken, selection?.snapshotToken ?? null,
    "The stress calculation could not be completed. Try again.")
  return { ...calculation, calculate: async (scenarios: PortfolioScenarioInput[], batch: boolean) => {
    if (!selection) return
    const request = batch
      ? { query: "portfolioScenarioBatch" as const, accountToken, snapshotToken: selection.snapshotToken, scenarios }
      : { query: "portfolioScenario" as const, accountToken, snapshotToken: selection.snapshotToken, scenario: scenarios[0]! }
    await calculation.calculate(request, (response) => parsePortfolioScenarioReport(response, selection, scenarios, batch))
  } }
}

export function usePortfolioRebalanceCalculation(
  transport: ProductTransport,
  accountToken: string,
  selection: PortfolioPlanningSelection | null,
) {
  const calculation = usePortfolioCalculation<PortfolioRebalanceReport>(transport, accountToken, selection?.snapshotToken ?? null,
    "The rebalance calculation could not be completed. Try again.")
  return { ...calculation, calculate: async (proposal: PortfolioRebalanceInput) => {
    if (!selection) return
    await calculation.calculate({ query: "portfolioRebalance", accountToken, snapshotToken: selection.snapshotToken, proposal },
      (response) => parsePortfolioRebalanceReport(response, selection, proposal))
  } }
}

// Position impact selects current account and market evidence when requested.
// It shares calculation cancellation without claiming a historical snapshot pin.
export function usePortfolioCandidateImpactCalculation(
  transport: ProductTransport,
  accountToken: string,
) {
  const calculation = usePortfolioCalculation<PortfolioCandidateImpact>(transport, accountToken, null,
    "The position comparison could not be completed. Try again.")
  return { ...calculation, calculate: async (input: PortfolioCandidateImpactInput) => {
    await calculation.calculate({ query: "portfolioCandidateImpact", accountToken, ...input },
      (response) => parsePortfolioCandidateImpact(response, accountToken, input))
  } }
}

export function usePortfolioTransactions(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  readSession: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_transactions")
  const navigation = useCursorNavigation()
  const pinned = usePinnedPortfolioPage()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.GetTransactions",
      { accountToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioTransactions(await transport.query({
        query: "portfolioTransactions", accountToken,
        cursor: pinned.cursor(navigation.after), limit: 25,
      }, { signal }))
      pinned.retain(page.snapshotToken, page.pageCursor, navigation.after, signal)
      return page
    },
  })
  return { available, query, navigation }
}

export function usePortfolioAttribution(
  transport: ProductTransport,
  bootstrap: DesktopBootstrap,
  accountToken: string,
  selectedSnapshotToken: string,
  baselineSnapshotToken: string,
  readSession: string,
) {
  const available = hasProductCapability(bootstrap, "portfolio_attribution")
  const navigation = useCursorNavigation()
  const query = useQuery({
    queryKey: productKeys.operation(bootstrap.productSessionToken, "portfolio", "Portfolio.GetAttribution",
      { accountToken, selectedSnapshotToken, baselineSnapshotToken, cursor: navigation.after, limit: 25, readSession }),
    enabled: available,
    gcTime: 0,
    retry: false,
    queryFn: async ({ signal }) => {
      const page = parsePortfolioAttribution(await transport.query({
        query: "portfolioAttribution", accountToken, selectedSnapshotToken,
        baselineSnapshotToken, cursor: navigation.after, limit: 25,
      }, { signal }))
      if (page.snapshotToken !== selectedSnapshotToken || page.baselineSnapshotToken !== baselineSnapshotToken) {
        throw new Error("The saved portfolio comparison does not match your selection.")
      }
      return page
    },
  })
  const restart = () => {
    navigation.restart()
    if (navigation.after === undefined) void query.refetch()
  }
  return { available, query, navigation, restart }
}
