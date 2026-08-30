import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import type { AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import { lookupResultSchema } from "@/features/lookup/schemas"
import type {
  PortfolioAccount,
  PortfolioHolding,
} from "@/features/portfolio/portfolio-contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import {
  productLookupActions,
  productLookupCategory,
  type ProductTransport,
} from "@/lib/transport"

const blockedBootstrap: DesktopBootstrap = {
  contractVersion: "market-squawk-desktop-v1",
  applicationVersion: "1.0.0",
  buildProfile: "development",
  platform: "macos",
  dataRoot: ".market-squawk",
  runtime: {
    installationId: "7e8299e7-9757-4441-926f-d0b22c767a65",
    workspaceId: "55e7626c-81c8-4e78-8aa6-45a1d9c2949a",
    serviceGeneration: 1,
  },
  storage: {
    state: "ready",
    label: "Ready",
    detail: "The controlled workspace opened.",
  },
  installation: {
    state: "unverified",
    label: "Not verified",
    detail: "No signed installation receipt was admitted.",
  },
  modelRuntime: {
    state: "not_configured",
    label: "Not configured",
    detail: "No verified training release is configured.",
  },
  mcp: {
    state: "available",
    label: "Available",
    detail: "The local MCP service can be started.",
  },
  telemetryEnabled: false,
  capabilities: [],
}

function transport(
  bootstrap = blockedBootstrap,
  onboard: ProductTransport["onboard"] = async () => {
    throw new Error("Provider onboarding is not configured for this test.")
  },
  query: ProductTransport["query"] = async () => ({
    data: null,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
      sourceCoverage: { status: "not_applicable" },
      dataQuality: { status: "not_applicable" },
    },
  }),
): ProductTransport {
  const productResult = (data: unknown): ApplicationResult => ({
    data,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
      sourceCoverage: { status: "not_applicable" },
      dataQuality: { status: "not_applicable" },
    },
  })
  return {
    bootstrap: async () => bootstrap,
    bootstrapService: async () => {
      throw new Error("Service bootstrap is not configured for this test.")
    },
    reconnectService: async () => {
      throw new Error("Service reconnect is not configured for this test.")
    },
    installation: async () => ({
      action: "status",
      status: {
        installed: false,
        active_version: null,
        previous_version: null,
        target: null,
        manifest_sha256: null,
        channel_manifest_url: null,
        healthy: false,
      },
      receipt: null,
      restartRequired: false,
    }),
    query,
    modelProducts: async (request) =>
      productResult(request.action === "list" ? { models: [] } : { activities: [] }),
    backtestProducts: async (request) => {
      if (request.action === "get") {
        throw new Error("No completed backtest is configured for this test.")
      }
      return productResult({ activities: [] })
    },
    analyticalController: async () =>
      analyticalControllerStatus(bootstrap.runtime.workspaceId),
    researchControl: async () =>
      query({ query: "researchCollections" }),
    datasetPreparation: async () =>
      query({ query: "analysisFeatureDatasets" }),
    backtestPreparation: async () =>
      query({ query: "jobs", limit: 25 }),
    startBacktestFromFile: async () =>
      query({ query: "jobs", limit: 25 }),
    modelControl: async () =>
      query({ query: "jobs", limit: 25 }),
    forecastPreparation: async () =>
      query({ query: "jobs", limit: 25 }),
    decisionControl: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    governanceQuery: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    governanceControl: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    fairValueControl: async () =>
      query({ query: "fairValueWorkspace", at: new Date().toISOString() }),
    paperControl: async () =>
      query({ query: "paperStatus" }),
    manualPaper: async () =>
      query({ query: "paperStatus" }),
    jobControl: async (request) =>
      query({ query: "jobs", limit: "limit" in request ? request.limit : 25 }),
    sourceControl: async (_action, _request) =>
      query({ query: "sourceStatus" }),
    importProviderCredentialBundle: async () => null,
    operationsControl: async () =>
      query({ query: "operationUpdateStatus" }),
    stageTrainingInput: async () => null,
    mcpClients: async () => {
      const claudeService = {
        client: "claude_code" as const,
        clientId: "d6b1a16d-bdf9-44d9-b10b-6e7558d701cb",
        credentialGeneration: 1,
        credentialIdentity: "d6b1a16d-bdf9-44d9-b10b-6e7558d701cb:1",
        maximumActiveRequests: 4,
        activeRequests: 0,
        admittedRequests: 0,
        rateLimitedRequests: 0,
        observedRelayInitializations: 0,
        lastActivityUnixSeconds: null,
        credentialRotationRecoveryPending: false,
        priorCredentialCleanupPending: false,
        accessRevoked: false,
      }
      const codexService = {
        client: "codex" as const,
        clientId: "6c4a5edb-caa2-4945-91fb-95baaca448f8",
        credentialGeneration: 1,
        credentialIdentity: "6c4a5edb-caa2-4945-91fb-95baaca448f8:1",
        maximumActiveRequests: 4,
        activeRequests: 0,
        admittedRequests: 0,
        rateLimitedRequests: 0,
        observedRelayInitializations: 0,
        lastActivityUnixSeconds: null,
        credentialRotationRecoveryPending: false,
        priorCredentialCleanupPending: false,
        accessRevoked: false,
      }
      const serviceClients = [claudeService, codexService]
      return {
        serviceReady: true,
        sharedEndpointReady: true,
        workspaceId: bootstrap.runtime.workspaceId,
        serviceGeneration: bootstrap.runtime.serviceGeneration,
        protocolVersion: "2025-11-25",
        transport: "stdio_relay",
        runtime: {
          sessionModel: "stateless_request_scoped",
          activeClients: 0,
          activeRequests: 0,
          admittedRequests: 0,
          rateLimitedRequests: 0,
          rejectedCredentials: 0,
          uptimeSeconds: 60,
          process: {
            residentMemoryBytes: 16_777_216,
            virtualMemoryBytes: 67_108_864,
          },
          limits: {
            maximumFrameBytes: 1_048_576,
            maximumBodyBytes: 1_048_576,
            maximumActiveRequests: 8,
            maximumInlineBytes: 262_144,
            maximumInlineItems: 1_000,
            maximumResultBytes: 16_777_216,
            maximumResultItems: 10_000,
            requestTimeoutMilliseconds: 30_000,
          },
          clients: serviceClients,
        },
        clients: [
          {
            client: "claude_code",
            label: "Claude Code",
            state: "absent",
            clientVersion: null,
            receipt: null,
            verification: null,
            blocker: null,
            service: claudeService,
          },
          {
            client: "codex",
            label: "Codex",
            state: "absent",
            clientVersion: null,
            receipt: null,
            verification: null,
            blocker: null,
            service: codexService,
          },
        ],
      }
    },
    mcpClientControl: async () =>
      Promise.reject(new Error("MCP mutation is not configured for this test.")),
    subscribe: async (request) => ({
      receipt: {
        subscriptionId: "f49e02f6-8c47-43a5-bb33-030e8e0d12bb",
        runtime: request.runtime,
        sequence: request.afterSequence,
        resumed: request.afterSequence !== "0",
      },
      unsubscribe: async () => undefined,
    }),
    onboard,
    openOfficialProviderPage: async () => undefined,
    openProtectedProviderSetup: async () => undefined,
  }
}

function analyticalControllerStatus(workspaceId: string): AnalyticalControllerStatus {
  const digest = "73ed06501d2693749c6966f0a8fdcf10c48b8f03632959c0c847ee8a4be9db54"
  const profileId = "0467d4c9-befd-5b7d-b4b5-99b673662c86"
  const config = {
    supportedInvestmentPolicy: { kind: "default_required" as const },
    pointInTimeDatasetPolicy: { kind: "default_required" as const },
    requiredFeatureSet: { kind: "default_required" as const },
    modelBundlePolicy: { kind: "default_required" as const },
    trainingCalibrationPolicy: { kind: "default_required" as const },
    forecastHorizonPolicy: { kind: "default_required" as const },
    valuationPolicy: { kind: "default_required" as const },
    backtestCostPolicy: { kind: "default_required" as const },
    recommendationPolicy: { kind: "default_required" as const },
    riskFreshnessAbstentionPolicy: { kind: "default_required" as const },
  }
  const profile = {
    profileId,
    ownerWorkspaceId: workspaceId,
    displayName: "Market Squawk Default V1",
    kind: "default" as const,
    version: 1,
    revision: "1",
    configDigest: digest,
    config,
    validationState: "default_immutable" as const,
    lastValidation: null,
    createdAt: "1800000000000000000",
    updatedAt: "1800000000000000000",
  }
  return {
    kind: "status",
    controllerSchemaVersion: 1,
    ownerWorkspaceId: workspaceId,
    controllerRevision: "1",
    activeProfile: {
      profileId,
      ownerWorkspaceId: workspaceId,
      displayName: profile.displayName,
      kind: "default",
      version: 1,
      profileRevision: "1",
      configDigest: digest,
      activationRevision: "1",
      activatedAt: "1800000000000000000",
    },
    profiles: [profile],
    workflowRuns: [],
    workflowReadiness: {
      state: "blocked",
      blockers: [
        {
          code: "canonical_data_and_backend_composition_required",
          detail: "Required canonical data and pure backend operations are not composed.",
          owner: "installed_application",
        },
        {
          code: "desktop_start_resume_not_registered",
          detail: "Find and Analyze start commands are intentionally absent.",
          owner: "desktop",
        },
      ],
    },
  }
}

const emptyRowsResult: ApplicationResult = {
  data: [],
  metadata: {
    completeness: "complete",
    returnedItems: 0,
    availableItems: 0,
    sourceCoverage: { status: "not_applicable" },
    dataQuality: { status: "not_applicable" },
  },
}

const marketInstrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
const marketObservedAt = "2026-08-09T14:30:00.000000000Z"
const marketUpdatedAt = "2026-08-09T14:30:00.011000000Z"

const marketOverviewRow = {
  instrumentId: marketInstrumentId,
  displaySymbol: "BTC-USD",
  name: "Bitcoin",
  assetClass: "crypto",
  currency: "USD",
  availability: "live",
  confidence: "moderate",
  currentPrice: {
    value: "68000.15",
    currency: "USD",
    basis: "bid_ask_midpoint",
    observedAt: marketObservedAt,
    currentThrough: marketObservedAt,
  },
  quote: {
    bidPrice: "68000.1",
    bidSize: "0.25",
    askPrice: "68000.2",
    askSize: "0.3",
    midPrice: "68000.15",
    lastPrice: null,
    lastSize: null,
    quoteObservedAt: marketObservedAt,
    lastObservedAt: null,
  },
  marketState: {
    timing: "real_time",
    quality: "direct",
    health: "healthy",
    integrity: "verified",
    coverage: "single_market",
    depth: "order_level",
    freshness: "fresh",
    observedAt: marketObservedAt,
    updatedAt: marketUpdatedAt,
    currentThrough: marketObservedAt,
  },
  observations: {
    admittedCount: 2,
    independentCount: null,
    agreement: "not_established",
  },
  depthSummary: {
    kind: "order_level",
    bidLevels: 1,
    askLevels: 1,
    individualOrderCount: 2,
    truncated: false,
  },
  depthDetails: null,
  analysisUse: "current_only",
}

function marketResult(
  row: typeof marketOverviewRow | (Omit<typeof marketOverviewRow, "depthDetails"> & {
    depthDetails: {
      kind: "order_level"
      bids: Array<{ price: string; quantity: string }>
      asks: Array<{ price: string; quantity: string }>
      individualOrders: {
        bidOrders: Array<{ price: string; quantity: string }>
        askOrders: Array<{ price: string; quantity: string }>
        totalCount: number
        returnedCount: number
        truncated: boolean
      }
    }
  }),
): ApplicationResult {
  return {
    data: [row],
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: {
        availability: "available",
        complete: true,
        returnedInstrumentCount: 1,
        observationCount: 2,
      },
      dataQuality: {
        referenceAt: marketUpdatedAt,
        observationCount: 2,
      },
    },
  }
}

const marketOverviewResult = marketResult(marketOverviewRow)
const marketInstrumentResult = marketResult({
  ...marketOverviewRow,
  depthDetails: {
    kind: "order_level",
    bids: [{ price: "68000.1", quantity: "0.25" }],
    asks: [{ price: "68000.2", quantity: "0.3" }],
    individualOrders: {
      bidOrders: [{ price: "68000.1", quantity: "0.25" }],
      askOrders: [{ price: "68000.2", quantity: "0.3" }],
      totalCount: 2,
      returnedCount: 2,
      truncated: false,
    },
  },
})

const macroKnowledgeCutoff = "2026-08-28T14:30:00Z"
const macroEffectiveDateCutoff = "2026-08-27"
const macroIndicatorDefinitions = [
  ["us-government-yield-1m", "1-month government yield", "4.32"],
  ["us-government-yield-3m", "3-month government yield", "4.28"],
  ["us-government-yield-6m", "6-month government yield", "4.18"],
  ["us-government-yield-1y", "1-year government yield", "4.02"],
  ["us-government-yield-2y", "2-year government yield", "3.88"],
  ["us-government-yield-3y", "3-year government yield", "3.82"],
  ["us-government-yield-5y", "5-year government yield", "3.86"],
  ["us-government-yield-7y", "7-year government yield", "3.98"],
  ["us-government-yield-10y", "10-year government yield", "4.12"],
  ["us-government-yield-20y", "20-year government yield", "4.48"],
  ["us-government-yield-30y", "30-year government yield", "4.39"],
  ["us-unemployment-rate", "Unemployment rate", "4.2"],
] as const

function macroContextResult(cutoffs = {
  knowledgeCutoff: macroKnowledgeCutoff,
  effectiveDateCutoff: macroEffectiveDateCutoff,
}): ApplicationResult {
  return {
    data: {
      schemaIdentity: "market-squawk-macro-context/v1",
      availability: "available",
      selection: {
        ...cutoffs,
        evaluatedAt: "2026-08-28T14:30:01Z",
        complete: true,
      },
      confidence: {
        level: "moderate",
        summary: "All requested economic indicators are available for the selected dates.",
      },
      coverage: {
        requested: 12,
        observed: 12,
        missing: 0,
        unavailable: 0,
      },
      observations: macroIndicatorDefinitions.map(
        ([indicatorId, label, decimal], index) => ({
          indicatorId,
          label,
          category: index < 11 ? "interest_rates" : "labor_market",
          frequency: index < 11 ? "business_daily" : "monthly",
          seasonalAdjustment:
            index < 11 ? "not_applicable" : "seasonally_adjusted",
          unit: {
            code:
              index < 11 ? "percent_per_year" : "percent_of_labor_force",
            label: index < 11 ? "Percent per year" : "Percent of labor force",
            symbol: "%",
          },
          effectiveDate: index < 11 ? "2026-08-27" : "2026-07-01",
          recorded: { state: "known", date: "2026-08-27" },
          availableAt: "2026-08-28T12:00:00Z",
          revision: 1,
          supersededAfter: null,
          value: { state: "observed", decimal },
          availability: "available",
          confidence: {
            level: "moderate",
            summary: "Available for the selected dates.",
          },
        }),
      ),
    },
    metadata: {
      completeness: "complete",
      returnedItems: 12,
      availableItems: 12,
      sourceCoverage: { status: "complete" },
      dataQuality: { status: "current" },
    },
  }
}

const portfolioAccountId = "55e7626c-81c8-4e78-8aa6-45a1d9c2949a"
const heldInstrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
const candidateInstrumentId = "0467d4c9-befd-5b7d-b4b5-99b673662c86"
const portfolioSnapshotToken = "f311efc6-a85b-4f3d-a8c4-dc65f67b52b7"
const portfolioAccount: PortfolioAccount = {
  accountId: portfolioAccountId,
  currency: "USD",
  cashBalance: { amount: "1000", currency: "USD" },
  currentSnapshot: {
    snapshotToken: portfolioSnapshotToken,
    effectiveAtUnixNanos: "1800000000000000000",
    availableAtUnixNanos: "1800000001000000000",
    holdingCount: 1,
    transactionCount: 1,
    dataIssueCount: 0,
    dataState: "ready",
  },
  holdingCount: 1,
  transactionCount: 1,
  reconciliationDiscrepancies: 0,
}
const portfolioHolding: PortfolioHolding = {
  accountId: portfolioAccountId,
  snapshotToken: portfolioSnapshotToken,
  instrumentId: heldInstrumentId,
  currency: "USD",
  quantity: "10",
  lotSize: "1",
  marketValue: { amount: "1000", currency: "USD" },
  asOfUnixNanos: "1800000000000000000",
  costBasis: { state: "not_available" },
  price: {
    asOfUnixNanos: "1800000000000000000",
    state: "reported",
    confidence: "moderate",
    explanation: "Reported with the portfolio snapshot.",
  },
}

function candidateImpactResult(instrumentId: string): ApplicationResult {
  return {
    data: {
      accountId: portfolioAccountId,
      instrumentId,
      positionState: "new",
      currentQuantity: "0",
      proposedQuantity: "3",
      currentMarketValue: { amount: "0", currency: "USD" },
      proposedMarketValue: { amount: "300", currency: "USD" },
      capitalChange: { amount: "300", currency: "USD" },
      portfolioValue: { amount: "2500", currency: "USD" },
      instrumentTerms: {
        priceTick: "0.01",
        lotSize: "1",
        quoteCurrency: "USD",
        contractMultiplier: "1",
      },
      costs: {
        fees: { state: "not_available" },
        slippage: { state: "not_available" },
      },
      concentration: { current: "0", proposed: "0.12", change: "0.12" },
      scenario: {
        shock: "-0.1",
        currentImpact: { amount: "0", currency: "USD" },
        proposedImpact: { amount: "-30", currency: "USD" },
        marginalImpact: { amount: "-30", currency: "USD" },
      },
      price: {
        amount: { amount: "100", currency: "USD" },
        asOfUnixNanos: "1800000002000000000",
        state: "current",
        method: "Last trade",
        confidence: "moderate",
      },
      missingInformation: [
        "Settlement-backed sizing",
        "Liquidity",
        "Estimated fees",
        "Estimated slippage",
      ],
      riskAssessment: {
        state: "incomplete",
        evaluatedAtUnixNanos: "1800000002000000002",
        checksCompleted: 5,
        checksUnavailable: 0,
      },
      updatedAtUnixNanos: "1800000002000000002",
      analysisOnly: true,
    },
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: {
        status: "complete",
        providers: ["owned-portfolio-import", "selected-market-source"],
      },
      dataQuality: { status: "direct_verified" },
    },
  }
}

describe("Market Squawk desktop boundary", () => {
  it("keeps lookup output closed and bound to exact product destinations", async () => {
    const instrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
    const screenId = "screen.long-term-value"
    const output = {
      query: "value",
      matches: [
        {
          category: productLookupCategory.investment,
          title: "MSQ",
          subtitle: "Stock · USD · Active",
          destination: {
            action: productLookupActions.openInvestment,
            instrumentId,
          },
        },
        {
          category: productLookupCategory.savedScreen,
          title: "Long Term Value",
          subtitle: "Saved investment screen",
          destination: {
            action: productLookupActions.openSavedScreen,
            screenId,
          },
        },
      ],
      categories: [
        { category: productLookupCategory.investment, state: "available" },
        { category: productLookupCategory.savedScreen, state: "available" },
      ],
      truncated: false,
    }
    const parsed = lookupResultSchema.parse(output)

    expect(lookupRoute(parsed.matches[0]!)).toBe(`/markets?instrumentId=${instrumentId}`)
    expect(lookupRoute(parsed.matches[1]!)).toBe(
      `/opportunities?screenId=${encodeURIComponent(screenId)}`,
    )
    expect(
      lookupResultSchema.safeParse({
        ...output,
        matches: [
          {
            ...output.matches[0],
            provider: "provider-sentinel",
            sourceId: "source-sentinel",
            manifest: "manifest-sentinel",
          },
        ],
      }).success,
    ).toBe(false)

    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    render(
      <MemoryRouter
        initialEntries={[
          `/opportunities?screenId=${encodeURIComponent(screenId)}`,
        ]}
      >
        <App
          transport={transport(
            {
              ...blockedBootstrap,
              capabilities: ["decision_screen_list"],
            },
            undefined,
            async (request) => {
              issuedQueries.push(request)
              if (request.query === "decisionScreen") {
                return {
                  data: parsed.matches[1],
                  metadata: {
                    completeness: "complete",
                    returnedItems: 1,
                    availableItems: 1,
                    sourceCoverage: { status: "not_applicable" },
                    dataQuality: { status: "not_applicable" },
                  },
                }
              }
              if (request.query === "decisionScreens") {
                return {
                  data: { screens: [] },
                  metadata: {
                    completeness: "complete",
                    returnedItems: 0,
                    availableItems: 0,
                    sourceCoverage: { status: "not_applicable" },
                    dataQuality: { status: "not_applicable" },
                  },
                }
              }
              throw new Error(`Unexpected lookup journey query: ${request.query}`)
            },
          )}
        />
      </MemoryRouter>,
    )

    expect(
      await screen.findByRole("heading", { name: "Long Term Value" }),
    ).toBeTruthy()
    expect(issuedQueries).toContainEqual({ query: "decisionScreen", screenId })
    expect(document.body.textContent ?? "").not.toMatch(
      /provider-sentinel|source-sentinel|manifest-sentinel/i,
    )
  })

  it("renders one provider-neutral market journey with current price and depth", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      capabilities: ["market_overview", "market_instrument"],
    }
    render(
      <MemoryRouter initialEntries={["/markets"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            issuedQueries.push(request)
            if (request.query === "marketOverview") return marketOverviewResult
            if (request.query === "marketInstrument") return marketInstrumentResult
            throw new Error(`Unexpected market query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    const marketHeading = await screen.findByRole("heading", { name: "BTC-USD" })
    const marketCard = marketHeading.closest("button")
    expect(marketCard).toBeInstanceOf(HTMLButtonElement)
    if (!(marketCard instanceof HTMLButtonElement)) {
      throw new Error("The market card is absent")
    }

    expect(within(marketCard).getByText("Bitcoin")).toBeTruthy()
    expect(within(marketCard).getByText("Live")).toBeTruthy()
    expect(within(marketCard).getByText("68000.15 USD")).toBeTruthy()
    expect(within(marketCard).getByText("Individual orders")).toBeTruthy()

    await user.click(marketCard)
    expect(await screen.findByRole("heading", { name: "Market depth" })).toBeTruthy()
    expect(screen.getByText("Buy interest")).toBeTruthy()
    expect(screen.getByText("Sell interest")).toBeTruthy()
    expect(screen.getByText("2 individual orders are available.")).toBeTruthy()
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "marketInstrument"),
      ).toEqual([
        { query: "marketInstrument", instrumentId: marketInstrumentId },
      ])
    })
    expect(
      issuedQueries.some((request) => request.query === "marketOverview"),
    ).toBe(true)
    expect(
      issuedQueries.some((request) =>
        [
          "marketSnapshot",
          "marketQuality",
          "marketUnifiedFeed",
          "marketTrades",
          "marketQuotes",
          "marketBooks",
          "marketComparisons",
        ].includes(request.query),
      ),
    ).toBe(false)

    const renderedText = document.body.textContent ?? ""
    expect(renderedText).not.toMatch(/kraken|coinbase|websocket-v2/i)
    expect(renderedText).not.toContain(marketInstrumentId)
    expect(renderedText).not.toMatch(/\bticks?\b|\blots?\b/i)
  })

  it("renders one provider-neutral economic context with paired date cutoffs", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      capabilities: ["research_dataset_list", "macro_context"],
    }
    render(
      <MemoryRouter initialEntries={["/advanced/research-data"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            issuedQueries.push(request)
            if (request.query === "macroContext") {
              return macroContextResult({
                knowledgeCutoff:
                  request.knowledgeCutoff ?? macroKnowledgeCutoff,
                effectiveDateCutoff:
                  request.effectiveDateCutoff ?? macroEffectiveDateCutoff,
              })
            }
            if (request.query === "researchCollections" || request.query === "jobs") {
              return emptyRowsResult
            }
            throw new Error(`Unexpected research query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    const macroHeading = await screen.findByRole("heading", {
      name: "Rates and labor conditions",
    })
    const macroSection = macroHeading.closest("section")
    expect(macroSection).toBeInstanceOf(HTMLElement)
    if (!(macroSection instanceof HTMLElement)) {
      throw new Error("The economic context is absent")
    }
    expect(within(macroSection).getByText("12 of 12 available")).toBeTruthy()
    expect(
      within(macroSection)
        .getAllByRole("heading", { level: 4 })
        .map((heading) => heading.textContent),
    ).toEqual(macroIndicatorDefinitions.map(([, label]) => label))

    fireEvent.change(within(macroSection).getByLabelText("What was known by"), {
      target: { value: "2026-08-26T15:00:00Z" },
    })
    fireEvent.change(within(macroSection).getByLabelText("Use data through"), {
      target: { value: "2026-08-25" },
    })
    await user.click(
      within(macroSection).getByRole("button", { name: "Apply dates" }),
    )
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "macroContext"),
      ).toEqual([
        { query: "macroContext" },
        {
          query: "macroContext",
          knowledgeCutoff: "2026-08-26T15:00:00Z",
          effectiveDateCutoff: "2026-08-25",
        },
      ])
    })

    const renderedMacro = macroSection.textContent ?? ""
    expect(renderedMacro).not.toMatch(
      /Federal Reserve|FRED|ALFRED|H\.?15|Macro\.GetContext|\bprovider\b|\bsource\b|\bmanifest\b|\bdigest\b/i,
    )
  })

  it("keeps candidate impact server-resolved and visibly analysis-only", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      capabilities: ["portfolio_candidate_impact"],
    }
    const candidateTransport = transport(
      readyBootstrap,
      undefined,
      async (request) => {
        issuedQueries.push(request)
        if (request.query === "portfolioCandidateImpact") {
          return candidateImpactResult(request.instrumentId)
        }
        throw new Error(`Unexpected candidate query: ${request.query}`)
      },
    )
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    })
    render(
      <QueryClientProvider client={queryClient}>
        <PortfolioPlanning
          account={portfolioAccount}
          holdings={[portfolioHolding]}
          bootstrap={readyBootstrap}
          transport={candidateTransport}
        />
      </QueryClientProvider>,
    )

    const instrument = screen.getByLabelText("Instrument ID")
    await user.clear(instrument)
    await user.type(instrument, candidateInstrumentId)
    const quantity = screen.getByLabelText(/^Proposed quantity/)
    await user.clear(quantity)
    await user.type(quantity, "3")
    await user.click(screen.getByRole("button", { name: "Compare impact" }))

    expect(await screen.findByText("Risk review remains incomplete")).toBeTruthy()
    expect(issuedQueries).toEqual([
      {
        query: "portfolioCandidateImpact",
        instrumentId: candidateInstrumentId,
        proposedQuantity: "3",
        scenarioShock: "-0.1",
      },
    ])
    expect(screen.getByText("New position")).toBeTruthy()
    expect(screen.getAllByText("USD 300")).toHaveLength(2)
    expect(screen.getByText("Settlement-backed sizing is unavailable.")).toBeTruthy()
    expect(screen.getAllByText("Unavailable")).toHaveLength(2)
    expect(
      screen.getByText(
        "Analysis only. No portfolio mutation, risk reservation, approval, or order was created.",
      ),
    ).toBeTruthy()
    expect(screen.queryByText("Projected cash")).toBeNull()
  })

  it("keeps fallback bootstrap native and enters the ready workspace only after reconnect", async () => {
    const user = userEvent.setup()
    let ready = false
    let submittedUnlock: string | null = null
    const bootstrapTransport = {
      ...transport(),
      bootstrap: async () =>
        ready
          ? blockedBootstrap
          : {
              status: "bootstrap_required" as const,
              requirement: "encrypted_fallback_locked" as const,
            },
      bootstrapService: async (request: {
        action: "unlock_encrypted_fallback"
        unlock: string
      }) => {
        submittedUnlock = request.unlock
        ready = true
      },
    } satisfies ProductTransport

    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={bootstrapTransport} />
      </MemoryRouter>,
    )

    const field = await screen.findByLabelText("Local security password")
    await user.type(field, "process-local-test-unlock")
    await user.click(screen.getByRole("button", { name: "Unlock secure storage" }))

    expect((field as HTMLInputElement).value).toBe("")
    expect(submittedUnlock).toBe("process-local-test-unlock")
    expect(
      await screen.findByRole("heading", { name: "What needs your attention now?" }),
    ).toBeTruthy()
  })

  it("keeps provider plumbing behind Connections", async () => {
    const providerSentinel = "Privileged provider sentinel"
    const onboardingRequests: Parameters<ProductTransport["onboard"]>[0][] = []
    const boundaryTransport = transport(
      blockedBootstrap,
      (async (request) => {
        onboardingRequests.push(request)
        if (request.action !== "bootstrap") {
          throw new Error("Unexpected provider onboarding request")
        }
        return {
          profiles: [
            {
              id: "privileged.test-source",
              display_name: providerSentinel,
              official_handoff_url: "https://example.com",
              handoff_instruction: "Open the protected connection flow.",
              zero_fee: "No fee",
              account_requirement: "No account",
              credential_requirement: "No credential",
              release_state: "available",
              coverage: "Protected connection evidence",
              quality_ceiling: "official_delayed",
            },
          ],
          sessions: [],
          encryptedFileFallback: "locked",
          capabilities: {
            credentialImport: false,
            health: false,
            manifestEvidence: false,
            researchIngestion: false,
            status: false,
            coverage: false,
          },
        }
      }) as ProductTransport["onboard"],
    )

    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={boundaryTransport} />
      </MemoryRouter>,
    )

    expect(
      await screen.findByRole("heading", { name: "What needs your attention now?" }),
    ).toBeTruthy()
    expect(document.body.textContent).not.toContain(providerSentinel)
    expect(document.body.textContent).not.toContain("Source.GetStatus")
    expect(onboardingRequests).toHaveLength(0)

    fireEvent.click(screen.getByRole("link", { name: "Markets" }))
    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    expect(document.body.textContent).not.toContain(providerSentinel)
    expect(document.body.textContent).not.toContain("Source.GetStatus")

    fireEvent.click(screen.getByRole("link", { name: "Research & Data" }))
    expect(await screen.findByText("Research is not ready")).toBeTruthy()
    expect(document.body.textContent).not.toContain(providerSentinel)
    expect(document.body.textContent).not.toContain("Source.GetStatus")

    fireEvent.click(screen.getByRole("link", { name: "Opportunities" }))
    expect(
      await screen.findByRole("heading", { name: "Opportunities" }),
    ).toBeTruthy()
    expect(document.body.textContent).not.toContain(providerSentinel)
    expect(document.body.textContent).not.toContain("Source.GetStatus")

    fireEvent.click(screen.getByRole("link", { name: "Portfolio" }))
    expect(
      await screen.findByRole("heading", { name: "Portfolio unavailable" }),
    ).toBeTruthy()
    expect(document.body.textContent).not.toContain(providerSentinel)
    expect(document.body.textContent).not.toContain("Source.GetStatus")
    expect(onboardingRequests).toHaveLength(0)

    fireEvent.click(screen.getByRole("link", { name: "Connections & Sources" }))
    expect(await screen.findByText(providerSentinel)).toBeTruthy()
    expect(onboardingRequests).toEqual([{ action: "bootstrap" }])
  })

  it("never promotes an unverified backend state to installation readiness", async () => {
    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    expect((await screen.findAllByText("Not verified")).length).toBeGreaterThan(0)
    expect(
      screen.getByRole("heading", { name: "What needs your attention now?" }),
    ).toBeTruthy()
    expect(screen.queryByText("Installation verified")).toBeNull()
    expect(screen.getByText("No signed installation receipt was admitted.")).toBeTruthy()
  })
})
