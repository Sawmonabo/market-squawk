import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { QueryClientProvider, QueryObserver } from "@tanstack/react-query"
import { ProductProvider } from "@/app/product-context"
import { createProductQueryClient, productKeys, snapshotQueryMeta } from "@/app/query-client"
import { describe, expect, it, vi } from "vitest"

import { App } from "@/app/app"
import type { AnalyticalControllerResponse, AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import { lookupResultSchema } from "@/features/lookup/schemas"
import type { MarketProductRow } from "@/features/markets/market-product"
import { parseInvestmentAnalysis, type InvestmentAnalysis } from "@/features/opportunities/contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
import type { PortfolioRiskReport } from "@/features/risk/contracts"
import { lifecycleControls, type SourceEvidence } from "@/features/sources/source-evidence"
import {
  type ApplicationResult,
  type DesktopSystemBootstrap,
  type DesktopInvalidationDomain,
  type NativeEvidenceApplicationResult,
} from "@/lib/schemas"
import {
  productLookupActions,
  productLookupCategory,
  type DesktopTransport,
  type DesktopEventSubscription,
  type ProductTransport,
  type ProductQuery,
  type SystemTransport,
} from "@/lib/transport"

// Drawing is outside this read-lifecycle regression; preserve the real React
// consumers, query cache, demand controls and native transport contract.
vi.mock("lightweight-charts", () => ({
  ColorType: { Solid: "solid" }, CandlestickSeries: "candles", LineSeries: "line",
  createChart: () => ({
    addSeries: () => ({ setData: () => undefined, applyOptions: () => undefined }),
    subscribeCrosshairMove: () => undefined,
    timeScale: () => ({ fitContent: () => undefined, setVisibleRange: () => undefined, subscribeVisibleTimeRangeChange: () => undefined }),
    remove: () => undefined,
  }),
}))

// jsdom does not implement element scrolling used by route navigation.
Object.defineProperty(HTMLElement.prototype, "scrollTo", {
  configurable: true,
  value: () => undefined,
})

const TEST_WORKSPACE_ID = "55e7626c-81c8-4e78-8aa6-45a1d9c2949a"
const TEST_SERVICE_GENERATION = 1

const blockedBootstrap: DesktopSystemBootstrap = {
  contractVersion: "market-squawk-desktop-v1",
  applicationVersion: "1.0.0",
  buildProfile: "development",
  platform: "macos",
  dataRoot: ".market-squawk",
  productSessionToken: "7e8299e7-9757-4441-926f-d0b22c767a65",
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
  onboard: SystemTransport["onboard"] = async () => {
    throw new Error("Provider onboarding is not configured for this test.")
  },
  query: ProductTransport["query"] = async () => ({
    data: null,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
    },
  }),
  marketHistoryPreparation: ProductTransport["marketHistoryPreparation"] = async () => {
    throw new Error("History preparation is not configured for this test.")
  },
  subscriptions?: Parameters<SystemTransport["subscribe"]>[1][],
  investmentFinancialPreparation: ProductTransport["investmentFinancialPreparation"] = async () => {
    throw new Error("Financial preparation is not configured for this test.")
  },
): DesktopTransport {
  const productResult = (data: unknown): ApplicationResult => ({
    data,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
    },
  })
  const systemResult = (data: unknown): NativeEvidenceApplicationResult => ({
    data,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
      sourceCoverage: null,
      dataQuality: null,
    },
  })
  const bridge: ProductTransport & SystemTransport = {
    bootstrap: async () => bootstrap,
    reconnect: async () => bootstrap,
    bootstrapService: async () => {
      throw new Error("Service bootstrap is not configured for this test.")
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
    marketHistoryPreparation,
    investmentFinancialPreparation,
    systemQuery: async () => systemResult(null),
    modelProducts: async (request) =>
      productResult(request.action === "list" ? { models: [], nextCursor: null } : { activities: [], nextCursor: null }),
    backtestProducts: async (request) => {
      if (request.action === "get") {
        throw new Error("No completed backtest is configured for this test.")
      }
      return productResult({ activities: [], nextCursor: null })
    },
    analyticalController: async (request) => request.action === "profileOptions"
      ? { kind: "profile_options", options: {
          benchmarkChoices: [],
          modelChoices: [{ token: "recommended", label: "Recommended calibrated forecast" }],
          fixedSettings: [], nextCursor: null,
        } }
      : analyticalControllerStatus(),
    researchControl: async () =>
      systemResult(null),
    researchExport: async () =>
      productResult(null),
    datasetPreparation: async () =>
      productResult(null),
    backtestPreparation: async () =>
      productResult(null),
    startBacktestFromFile: async () =>
      productResult(null),
    modelControl: async () =>
      productResult(null),
    forecastPreparation: async () =>
      productResult(null),
    decisionControl: async () =>
      productResult(null),
    governanceQuery: async () =>
      systemResult(null),
    governanceControl: async () =>
      systemResult(null),
    fairValueControl: async () =>
      productResult(null),
    paperControl: async () =>
      query({ query: "paperStatus" }),
    manualPaper: async () =>
      query({ query: "paperStatus" }),
    recommendationSetup: async () => {
      throw new Error("Recommendation setup is not configured for this test.")
    },
    jobControl: async (request) =>
      systemResult({ request }),
    sourceControl: async (_action, _request) =>
      systemResult(null),
    importProviderCredentialBundle: async () => null,
    operationsControl: async () =>
      systemResult(null),
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
        workspaceId: TEST_WORKSPACE_ID,
        serviceGeneration: TEST_SERVICE_GENERATION,
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
    subscribe: async (request, onEvent) => {
      subscriptions?.push(onEvent)
      return {
        receipt: {
          subscriptionId: "f49e02f6-8c47-43a5-bb33-030e8e0d12bb",
          productSessionToken: request.productSessionToken,
          sequence: request.afterSequence,
          resumed: request.afterSequence !== "0",
        },
        unsubscribe: async () => undefined,
      }
    },
    onboard,
    openOfficialProviderPage: async () => undefined,
  }
  const product: ProductTransport = {
    query: bridge.query,
    marketHistoryPreparation: bridge.marketHistoryPreparation,
    investmentFinancialPreparation: bridge.investmentFinancialPreparation,
    analyticalController: bridge.analyticalController,
    modelProducts: bridge.modelProducts,
    backtestProducts: bridge.backtestProducts,
    datasetPreparation: bridge.datasetPreparation,
    backtestPreparation: bridge.backtestPreparation,
    forecastPreparation: bridge.forecastPreparation,
    researchExport: bridge.researchExport,
    paperControl: bridge.paperControl,
    manualPaper: bridge.manualPaper,
    recommendationSetup: bridge.recommendationSetup,
  }
  const system: SystemTransport = {
    bootstrap: bridge.bootstrap,
    reconnect: bridge.reconnect,
    bootstrapService: bridge.bootstrapService,
    installation: bridge.installation,
    systemQuery: bridge.systemQuery,
    researchControl: bridge.researchControl,
    startBacktestFromFile: bridge.startBacktestFromFile,
    modelControl: bridge.modelControl,
    decisionControl: bridge.decisionControl,
    governanceQuery: bridge.governanceQuery,
    governanceControl: bridge.governanceControl,
    fairValueControl: bridge.fairValueControl,
    jobControl: bridge.jobControl,
    sourceControl: bridge.sourceControl,
    importProviderCredentialBundle: bridge.importProviderCredentialBundle,
    operationsControl: bridge.operationsControl,
    stageTrainingInput: bridge.stageTrainingInput,
    mcpClients: bridge.mcpClients,
    mcpClientControl: bridge.mcpClientControl,
    subscribe: bridge.subscribe,
    onboard: bridge.onboard,
    openOfficialProviderPage: bridge.openOfficialProviderPage,
  }
  return { product, system }
}

function analyticalControllerStatus(): AnalyticalControllerStatus {
  const profile = {
    profileToken: "profile_11111111111111111111111111111111",
    profileStateToken: "state_22222222222222222222222222222222",
    displayName: "Market Squawk Default V1",
    version: 1,
    mode: "recommended" as const,
    active: true,
    validation: {
      state: "built_in" as const,
      label: "Built-in recommended settings",
      explanation: "Market Squawk's built-in recommended settings are fixed and ready to use.",
      validatedAt: null,
    },
    validationToken: null,
    activationToken: "activation_33333333333333333333333333333333",
    differencesFromRecommended: [],
    createdAt: "1800000000000000000",
    updatedAt: "1800000000000000000",
    activatedAt: "1800000000000000000",
    canValidate: false,
    canActivate: false,
    canRestoreRecommended: false,
    canEdit: false,
    analysisScope: "balanced" as const,
    financialPreferences: {
      coverage: "stocks_and_etfs" as const,
      modelChoice: "recommended",
      allowRetrospectiveStudies: true,
      fields: [
        { key: "portfolio_risk_minimum_daily_returns", label: "Minimum portfolio history",
          group: "Portfolio risk history", value: "252", unit: "daily returns" as const, choices: [] },
        { key: "portfolio_risk_maximum_daily_returns", label: "Maximum portfolio history",
          group: "Portfolio risk history", value: "1260", unit: "daily returns" as const, choices: [] },
      ],
    },
  }
  return {
    kind: "status",
    activeProfile: profile,
    profiles: [profile],
    workflows: [],
    workflowAvailability: {
      state: "unavailable",
      explanation: "New investment analysis is not available yet.",
      nextAction: "Review saved investment analyses, or try again later.",
    },
    canCreateCustomProfile: true,
    profileRecoveryNotice: null,
  }
}

const emptyRowsResult: ApplicationResult = {
  data: [],
  metadata: {
    completeness: "complete",
    returnedItems: 0,
    availableItems: 0,
  },
}

const marketSelectionToken = "market_0123456789abcdef0123456789abcdef"
const marketObservedAt = "2026-08-09T14:30:00.000000000Z"

const marketOverviewRow = {
  selectionToken: marketSelectionToken,
  historyToken: null,
  identity: {
    symbol: "BTC-USD",
    name: "Bitcoin",
    assetClass: "crypto",
  },
  price: {
    value: "68000.15",
    currency: "USD",
  },
  priceBasis: "last_trade",
  priceCurrentThrough: "2026-08-09T14:30:05.000000000Z",
  quote: {
    quoteSizeBasis: "quantity", currency: "USD", bidPrice: "68000.1", bidSize: "2", askPrice: "68000.2", askSize: "3",
    midPrice: "68000.15", lastPrice: "68000.15", lastSize: "0.5", tradeStatus: "available",
    quoteObservedAt: marketObservedAt, lastObservedAt: marketObservedAt,
    quoteCurrentThrough: "2026-08-09T14:30:05.000000000Z", lastCurrentThrough: "2026-08-09T14:30:05.000000000Z",
    quoteFresh: true, lastFresh: true,
  },
  changePercent: "1.25",
  changeBasis: {
    priceBasis: "last_trade", priceAsOf: marketObservedAt, adjustment: "raw",
    previousClose: { value: "67160.64197530864197530864197531", currency: "USD", sessionDate: "2026-08-08", asOf: "2026-08-08T20:00:00.000000000Z" },
  },
  changeUnavailableReason: null,
  asOf: marketObservedAt,
  availability: "current",
} satisfies MarketProductRow

function marketResult(row: MarketProductRow): ApplicationResult {
  return {
    data: {
      data: [row],
      page: {
        hasMore: false,
        nextPageToken: null,
      },
    },
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
    },
  }
}

const marketOverviewResult = marketResult(marketOverviewRow)

const macroKnowledgeCutoff = "2026-08-28T14:30:00Z"
const macroEffectiveDateCutoff = "2026-08-27"
const macroIndicatorDefinitions = [
  ["us-government-yield-1m", "1-month government bond yield", "4.32"],
  ["us-government-yield-3m", "3-month government bond yield", "4.28"],
  ["us-government-yield-6m", "6-month government bond yield", "4.18"],
  ["us-government-yield-1y", "1-year government bond yield", "4.02"],
  ["us-government-yield-2y", "2-year government bond yield", "3.88"],
  ["us-government-yield-3y", "3-year government bond yield", "3.82"],
  ["us-government-yield-5y", "5-year government bond yield", "3.86"],
  ["us-government-yield-7y", "7-year government bond yield", "3.98"],
  ["us-government-yield-10y", "10-year government bond yield", "4.12"],
  ["us-government-yield-20y", "20-year government bond yield", "4.48"],
  ["us-government-yield-30y", "30-year government bond yield", "4.39"],
  ["us-unemployment-rate", "U.S. unemployment rate", "4.2"],
  ["us-residential-electricity-price", "U.S. residential electricity price", "17.47"],
  ["california-beginning-quarter-employment", "California beginning-of-quarter employment", "17500000"],
  ["california-annual-personal-income", "California annual personal income", "3400000000000"],
] as const

function macroContextResult(cutoffs = {
  knowledgeCutoff: macroKnowledgeCutoff,
  effectiveDateCutoff: macroEffectiveDateCutoff,
}): ApplicationResult {
  const knowledgeCutoff = new Date(cutoffs.knowledgeCutoff).toISOString().replace(/\.[0-9]{3}Z$/, ".000000000Z")
  const [year, month, day] = cutoffs.effectiveDateCutoff.split("-").map(Number)
  return {
    data: {
      availability: "available",
      investmentContext: {
        availability: "available",
        curve: "mixed",
        effective: {
          schema_version: 2,
          coordinate: { precision: "calendar_date", value: { year, month, day } },
        },
        threeMonthToTenYearSpreadPercentagePoints: "-0.16",
        twoYearToTenYearSpreadPercentagePoints: "0.24",
        governmentYieldReferences: [
          { maturityYears: 10, annualPercent: "4.12", availableAt: knowledgeCutoff },
          { maturityYears: 30, annualPercent: "4.39", availableAt: knowledgeCutoff },
        ],
      },
      selection: {
        ...cutoffs,
        knowledgeCutoff,
        effectiveMonthCutoff: "2026-07",
        evaluatedAt: "2026-08-28T14:30:01.000000000Z",
        complete: true,
      },
      confidence: {
        level: "moderate",
        summary: "All requested economic indicators are available for the selected dates.",
      },
      coverage: {
        requested: macroIndicatorDefinitions.length,
        observed: macroIndicatorDefinitions.length,
        missing: 0,
        unavailable: 0,
      },
      observations: macroIndicatorDefinitions.map(
        ([indicatorId, label, decimal], index) => ({
          indicatorId,
          label,
          category: index < 11 ? "interest_rates" : index === 12 ? "energy_prices" : index === 14 ? "income" : "labor_market",
          frequency: index < 11 ? "business_daily" : index === 13 ? "quarterly" : index === 14 ? "annual" : "monthly",
          seasonalAdjustment:
            index < 11 ? "not_applicable" : index >= 12 ? "not_supplied" : "seasonally_adjusted",
          unit: {
            code:
              index < 11 ? "percent_per_year" : index === 12 ? "native_energy_price" : index === 13 ? "persons" : index === 14 ? "native_income" : "percent_of_labor_force",
            label: index < 11 ? "Percent per year" : index === 12 ? "cents per kilowatthour" : index === 13 ? "Persons" : index === 14 ? "Dollars" : "Percent of labor force",
            symbol: index >= 12 ? null : "%",
          },
          effectiveDate: index < 11 ? cutoffs.effectiveDateCutoff : index >= 12 ? null : "2026-07-01",
          ...(index === 12 ? { effectivePeriod: "2026-07" } : index === 13 ? { effectivePeriod: "2026-Q2" } : index === 14 ? { effectivePeriod: "2025" } : {}),
          recorded: { state: "known", date: cutoffs.effectiveDateCutoff },
          availableAt: knowledgeCutoff,
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
      returnedItems: macroIndicatorDefinitions.length,
      availableItems: macroIndicatorDefinitions.length,
    },
  }
}

describe("Market Squawk desktop boundary", () => {
  it("keeps lookup output closed and bound to exact product destinations", async () => {
    await import("@/features/markets")
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
            selectionToken: marketSelectionToken,
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

    expect(lookupRoute(parsed.matches[0]!)).toBe(`/investments/${marketSelectionToken}`)
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
    const savedScreen = render(
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
    savedScreen.unmount()

    const historyToken = "history_0123456789abcdef0123456789abcdef"
    const historyJobId = "781276a0-33f1-4fb3-8cbb-bb2095acd0cf"
    const jobGeneration = "9007199254740993"
    const initialHistoryGeneration = "a".repeat(64)
    const publishedHistoryGeneration = "c".repeat(64)
    let preparationState: "running" | "completed" | "cancelled" = "running"
    let historyAvailability: "available" | "missing" | "error" = "available"
    let historyStarts = 0
    let reconciliations = 0
    const preparationRequests: { request: Parameters<ProductTransport["marketHistoryPreparation"]>[0]; confirmed: boolean | undefined }[] = []
    const subscriptions: Parameters<SystemTransport["subscribe"]>[1][] = []
    const recoveryKey = `market-squawk.history-preparation.v1:${blockedBootstrap.productSessionToken}:${historyToken}`
    sessionStorage.removeItem(recoveryKey)
    const historyJob = () => ({
      jobId: historyStarts > 1 ? "781276a0-33f1-4fb3-8cbb-bb2095acd0c8" : historyJobId,
      generation: jobGeneration, sequence: preparationState === "completed" ? "11" : preparationState === "cancelled" ? "12" : "10",
      kind: "market.prepare-history.v1", state: preparationState, phase: null,
      completedUnits: preparationState === "completed" ? 1 : 0, totalUnits: 1,
      cancellationRequested: preparationState === "cancelled", failure: null, updatedAt: "1786363200000000000", recovery: null,
      result: preparationState === "completed" ? {
        authority: "market.adjusted-history-publication.v1", identity: "prepared-selected-history",
        evidenceDigest: { algorithm: "sha256", bytes: Array<number>(32).fill(1) }, artifacts: [],
      } : null,
    })
    const historyPreparation: ProductTransport["marketHistoryPreparation"] = async (request, confirmed) => {
      preparationRequests.push({ request, confirmed })
      if (request.action === "start") { historyStarts += 1; throw new Error("The durable start acknowledgment was lost.") }
      if (request.action === "cancel") preparationState = "cancelled"
      const data = request.action === "get" ? historyJob()
        : request.action === "reconcileStart" ? ++reconciliations === 1
          ? { state: "unknown", job: null } : { state: "admitted", job: historyJob() }
          : request.action === "cancel" ? historyJob() : null
      if (data === null) throw new Error(`Unexpected history action: ${request.action}`)
      return { data, metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 } }
    }
    const savedHistory: ProductTransport["query"] = async (request) => {
      if (request.query !== "marketHistory") throw new Error("Expected a saved history read.")
      if (historyAvailability === "error") throw new Error("The saved history read was temporarily unavailable.")
      if (historyAvailability === "missing") return {
        data: { data: null, unavailableReason: "not_available" },
        metadata: { completeness: "complete", returnedItems: 0, availableItems: 0 },
      }
      const generationToken = request.generationToken ?? (preparationState === "completed" ? publishedHistoryGeneration : initialHistoryGeneration)
      const bars = ["2026-06-01", "2026-08-08"].map((date, index) => ({
        originalOrdinal: String(index), breakBefore: [false, false, false],
        time: { precision: "nominal_date", date }, open: "120", high: "130", low: "110",
        close: generationToken === publishedHistoryGeneration ? "124.56789" : "123.456789", volume: "12",
      })).filter((bar) => (!request.startDate || bar.time.date >= request.startDate)
        && (!request.endDate || bar.time.date <= request.endDate))
      return { data: { data: {
        historyToken, currency: "USD", partial: false, generationToken, bars,
        display: { method: "first_last_min_max", originalPointCount: "2", visibleOriginalPointCount: String(bars.length),
          returnedPointCount: bars.length, firstTimeUnixNanos: null, lastTimeUnixNanos: null, projectionDigest: "b".repeat(64), reduced: false },
        viewport: { startUnixNanos: null, endUnixNanos: null, startDate: request.startDate ?? null, endDate: request.endDate ?? null,
          pointLimit: request.pointLimit ?? 512, fullStartUnixNanos: null, fullEndUnixNanos: null,
          fullStartDate: "2026-06-01", fullEndDate: "2026-08-08" },
      }, unavailableReason: null }, metadata: { completeness: "complete", returnedItems: bars.length, availableItems: bars.length } }
    }
    const requestedRow: MarketProductRow = {
      ...marketOverviewRow, historyToken,
      identity: { symbol: "MSQ", name: "Requested investment", assetClass: "equity" },
    }
    let wrongProfileSelection = false
    let financialMode: "available" | "mismatch" | "pending" = "available"
    let financialSignal: AbortSignal | undefined
    let finishFinancial: ((value: ApplicationResult) => void) | undefined
    const financialRead = "781276a0-33f1-4fb3-8cbb-bb2095acd0ce"
    let financialVersion = 0
    let financialMissingReason: "identity_missing" | "no_records" = "identity_missing"
    let financialPreparationState: "running" | "cancelled" | "completed" = "running"
    let financialStarts = 0
    let wrongFinancialJob = true
    const financialJobId = "781276a0-33f1-4fb3-8cbb-bb2095acd0cd"
    const financialSequence = "9007199254740994"
    const financialRecoveryKey = `market-squawk.financial-preparation.v1:${blockedBootstrap.productSessionToken}:${marketSelectionToken}`
    sessionStorage.removeItem(financialRecoveryKey)
    const financialPreparationRequests: { request: Parameters<ProductTransport["investmentFinancialPreparation"]>[0]; confirmed: boolean | undefined }[] = []
    const financialJob = () => ({
      ...historyJob(), jobId: financialStarts <= 2 ? financialJobId
        : financialStarts === 3 ? "781276a0-33f1-4fb3-8cbb-bb2095acd0cb" : "781276a0-33f1-4fb3-8cbb-bb2095acd0ca",
      kind: "research.prepare-investment-financials.v1", state: financialPreparationState,
      sequence: financialPreparationState === "running" ? financialSequence : "9007199254740995",
      cancellationRequested: financialPreparationState === "cancelled",
      result: financialPreparationState === "completed" ? {
        authority: "research.financial-preparation-result.v1", identity: "prepared-selected-financials",
        evidenceDigest: { algorithm: "sha256", bytes: Array<number>(32).fill(2) }, artifacts: [],
      } : null,
    })
    const financialPreparation: ProductTransport["investmentFinancialPreparation"] = async (request, confirmed) => {
      financialPreparationRequests.push({ request, confirmed })
      if (request.action === "start") {
        financialPreparationState = "running"
        if (++financialStarts <= 2) throw new Error("The financial start acknowledgment was lost.")
      }
      if (request.action === "cancel") financialPreparationState = "cancelled"
      const data = request.action === "cancelStart" || request.action === "reconcileStart"
        ? financialStarts === 1 ? { state: "not_admitted", job: null } : { state: "admitted", job: financialJob() }
        : request.action === "get" && wrongFinancialJob ? { ...financialJob(), jobId: historyJobId } : financialJob()
      return { data, metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 } }
    }
    const financialFact = {
      scope: "company_wide", revision: "current", metric: "current_assets", displayName: "Current assets",
      value: "123456.78", unit: { kind: "currency", currency: "USD" },
      period: { kind: "instant", instant: { year: 2026, month: 6, day: 30 } },
      fiscalContext: { fiscalYear: 2026, fiscalPeriod: "second_quarter", cadence: "quarterly" },
      reportingContext: { dimensionality: "no_dimensions", consolidation: "reported_consolidated", amendment: "original", restatement: "unavailable", occurrence: 1 },
      filedOn: { year: 2026, month: 8, day: 1 },
      effective: { precision: "calendar_date", value: { year: 2026, month: 6, day: 30 } },
      knownAt: "1788220800000000000",
    }
    const financialResult = (cursor = financialVersion === 0 ? "financial-first" : `financial-updated-${financialVersion}`): ApplicationResult => ({
      data: {
        selectionToken: financialMode === "mismatch" ? "market_ffffffffffffffffffffffffffffffff" : marketSelectionToken,
        section: "facts", knowledgeAt: marketObservedAt, effectiveOn: "2026-08-10", revisionPolicy: "latestKnown",
        state: "reported", families: [{ family: "company_facts", state: "reported", reason: null }],
        items: [{ ...financialFact, value: cursor === "financial-next" ? "234567.89"
          : cursor === "financial-updated-1" ? "345678.90" : cursor === "financial-updated-2" ? "456789.01" : financialFact.value }],
        currentCursor: cursor, nextCursor: cursor === "financial-first" ? "financial-next" : null,
        readToken: cursor.startsWith("financial-updated") ? "781276a0-33f1-4fb3-8cbb-bb2095acd0cc" : financialRead,
        omittedItems: 0, limitations: [],
      },
      metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 },
    })
    const profileResult = (): ApplicationResult => ({
      data: {
        selectionToken: wrongProfileSelection ? "market_ffffffffffffffffffffffffffffffff" : marketSelectionToken,
        knowledgeAt: marketObservedAt, state: "available", reason: null,
        profile: {
          displayName: "Requested investment", symbol: "MSQ", assetClass: "equity", currency: "USD",
          listingVenue: "XNAS", exchangeTradedFund: false, roundLotSize: 100,
          effectiveFrom: "2026-08-01T00:00:00.000000000Z", effectiveUntil: null,
          knownAt: marketObservedAt, referenceUpdatedAt: "2026-08-09T12:00:00.000000000Z",
          lifecycle: "successor_and_delisting_not_established",
        },
      },
      metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 },
    })
    const openInvestment = (route: string, supportsFinancials = true) => render(
      <MemoryRouter initialEntries={[route]}>
        <App transport={transport(
          { ...blockedBootstrap, capabilities: ["market_overview", "market_instrument", "market_history", "market_history_preparation_start", "market_history_preparation_get", "market_history_preparation_cancel", ...(supportsFinancials ? ["investment_financials", "investment_financials_close", "investment_financial_preparation_start", "investment_financial_preparation_get", "investment_financial_preparation_cancel"] as const : [])] },
          undefined,
          async (request, options) => {
            issuedQueries.push(request)
            if (request.query === "closeInvestmentFinancials") return { data: { released: true }, metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 } }
            if (request.query === "investmentFinancials") {
              if (financialMode === "pending" && request.section === "facts") {
                financialSignal = options?.signal
                return new Promise<ApplicationResult>((resolve) => { finishFinancial = resolve })
              }
              if (request.section !== "facts") return {
                ...financialResult(), data: {
                  ...financialResult().data as object, section: request.section, items: [],
                  state: financialVersion === 0 ? "missing" : "reported",
                  families: [{ family: "filings", state: financialVersion === 0 ? "missing" : "reported",
                    reason: financialVersion === 0 ? financialMissingReason : null }],
                  nextCursor: null,
                },
                metadata: { completeness: "complete", returnedItems: 0, availableItems: 0 },
              }
              return financialResult(request.cursor)
            }
            if (request.query === "marketHistory") return savedHistory(request, options)
            if (request.query === "marketOverview") return marketOverviewResult
            if (request.query === "investmentProfile" && request.selectionToken === marketSelectionToken) return profileResult()
            if (request.query === "marketInstrument" && request.selectionToken === marketSelectionToken) {
              return marketResult(requestedRow)
            }
            throw new Error("This investment selection is no longer available.")
          },
          historyPreparation,
          subscriptions,
          financialPreparation,
        )} />
      </MemoryRouter>,
    )
    let investment = openInvestment(lookupRoute(parsed.matches[0]!))
    expect(await screen.findByRole("heading", { name: "MSQ · Requested investment" })).toBeTruthy()
    expect(issuedQueries).toContainEqual({ query: "marketInstrument", selectionToken: marketSelectionToken })
    const profile = within(screen.getByRole("region", { name: "Investment profile" }))
    expect(await profile.findByText("XNAS")).toBeTruthy()
    wrongProfileSelection = true
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))
    expect((await profile.findByRole("alert")).textContent).toContain("The profile could not be refreshed")
    expect(profile.getByText("XNAS")).toBeTruthy()
    expect(screen.getByRole("heading", { name: "MSQ · Requested investment" })).toBeTruthy()
    // Financial demand reads are independent of quotes/profile and retain exact page identity.
    expect(screen.getByRole("tab", { name: "Facts" }).getAttribute("aria-selected")).toBe("true")
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials").every((request) => request.section === "facts")).toBe(true)
    const facts = within(await screen.findByRole("region", { name: "Reported financial facts" }))
    expect(await facts.findByText("USD 123,456.78")).toBeTruthy()
    expect(financialPreparationRequests).toHaveLength(0)
    expect(screen.queryByRole("button", { name: /^(Load|Update) financial information$/ })).toBeNull()
    await userEvent.setup().click(facts.getByRole("button", { name: "Next" }))
    expect(await facts.findByText("USD 234,567.89")).toBeTruthy()
    expect(issuedQueries).toContainEqual({ query: "investmentFinancials", selectionToken: marketSelectionToken, section: "facts", limit: 32, cursor: "financial-next" })
    await userEvent.setup().click(facts.getByRole("button", { name: "Previous" }))
    expect(await facts.findByText("USD 123,456.78")).toBeTruthy()
    expect(issuedQueries).toContainEqual({ query: "investmentFinancials", selectionToken: marketSelectionToken, section: "facts", limit: 32, cursor: "financial-first" })
    financialMode = "mismatch"
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))
    expect((await facts.findByRole("alert")).textContent).toContain("Could not update financial information")
    expect(facts.getByText("USD 123,456.78")).toBeTruthy()
    expect(screen.getByRole("heading", { name: "MSQ · Requested investment" })).toBeTruthy()
    requestedRow.identity.assetClass = "fund"
    const instrumentReadsBeforeFund = issuedQueries.filter((request) => request.query === "marketInstrument").length
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketInstrument")).toHaveLength(instrumentReadsBeforeFund + 1))
    await waitFor(() => expect((facts.getByRole("button", { name: "Retry" }) as HTMLButtonElement).disabled).toBe(false))
    financialMode = "pending"
    await userEvent.setup().click(facts.getByRole("button", { name: "Retry" }))
    await waitFor(() => expect(financialSignal).toBeDefined())
    await userEvent.setup().click(screen.getByRole("tab", { name: "Filings" }))
    await waitFor(() => expect(financialSignal?.aborted).toBe(true))
    financialMode = "available"
    finishFinancial?.(financialResult())
    await waitFor(() => expect(issuedQueries).toContainEqual({ query: "closeInvestmentFinancials", selectionToken: marketSelectionToken, readToken: financialRead }))
    expect(screen.queryByRole("region", { name: "Reported financial facts" })).toBeNull()

    // A warm first-page display survives the tab's closed lease. It must not
    // reuse that lease for pagination while its replacement read is pending.
    financialMode = "pending"
    finishFinancial = undefined
    await userEvent.setup().click(screen.getByRole("tab", { name: "Facts" }))
    const warmFacts = within(await screen.findByRole("region", { name: "Reported financial facts" }))
    expect(warmFacts.getByText("USD 123,456.78")).toBeTruthy()
    expect(warmFacts.queryByRole("button", { name: "Next" })).toBeNull()
    await waitFor(() => expect(finishFinancial).toBeTypeOf("function"))
    financialMode = "available"
    await act(async () => { finishFinancial?.(financialResult()) })
    await waitFor(() => expect((warmFacts.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(false))
    await userEvent.setup().click(screen.getByRole("tab", { name: "Filings" }))

    // Fund identities do not enter the company-filing acquisition path.
    await within(screen.getByRole("region", { name: "Filings" })).findByText("No reported information is available for this section at the information date.")
    expect(financialPreparationRequests).toHaveLength(0)
    requestedRow.identity.assetClass = "equity"
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))

    // A successful missing-family read loads financial information automatically.
    // Recover its lost acknowledgment, reject another job, and cancel using
    // the original lossless generation and the last checked sequence.
    let financialControlNode = screen.getByRole("group", { name: "Financial information loading" })
    let financialControls = within(financialControlNode)
    await financialControls.findByText("Loading could not be checked. Check the original request before trying again.")
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(1)
    expect(financialControls.queryByRole("button", { name: /^(Load|Update) financial information$/ })).toBeNull()

    const notAdmittedStart = financialPreparationRequests[0]!.request
    if (notAdmittedStart.action !== "start") throw new Error("Expected the original financial Start.")
    await userEvent.setup().click(financialControls.getByRole("button", { name: "Check loading" }))
    await financialControls.findByText("The original request did not start. Information can be loaded again.")
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!)).toMatchObject({ startRequestId: notAdmittedStart.startRequestId, receipt: null })
    investment.unmount()
    investment = openInvestment(lookupRoute(parsed.matches[0]!))
    await screen.findByRole("heading", { name: "MSQ · Requested investment" })
    await userEvent.setup().click(screen.getByRole("tab", { name: "Filings" }))
    financialControlNode = screen.getByRole("group", { name: "Financial information loading" })
    financialControls = within(financialControlNode)
    await financialControls.findByText("The original request did not start. Information can be loaded again.")
    await within(screen.getByRole("region", { name: "Filings" })).findByText("No reported information is available for this section at the information date.")
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(1)
    expect(financialPreparationRequests.filter(({ request }) => request.action === "reconcileStart")).toEqual([
      { request: { action: "reconcileStart", selectionToken: marketSelectionToken, startRequestId: notAdmittedStart.startRequestId }, confirmed: false },
      { request: { action: "reconcileStart", selectionToken: marketSelectionToken, startRequestId: notAdmittedStart.startRequestId }, confirmed: false },
    ])
    await userEvent.setup().click(financialControls.getByRole("button", { name: "Retry" }))
    await financialControls.findByText("Loading could not be checked. Check the original request before trying again.")
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
    const retriedStart = financialPreparationRequests.filter(({ request }) => request.action === "start").at(-1)!
    const financialStart = retriedStart.request
    if (financialStart.action !== "start") throw new Error("Expected financial Start.")
    expect(retriedStart.confirmed).toBe(true)
    expect(financialStart.startRequestId).not.toBe(notAdmittedStart.startRequestId)
    expect(financialStart).toEqual({ action: "start", selectionToken: marketSelectionToken, startRequestId: financialStart.startRequestId })
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!).startRequestId).toBe(financialStart.startRequestId)
    await userEvent.setup().click(financialControls.getByRole("button", { name: "Cancel loading" }))
    await financialControls.findByText("Loading status could not be checked. Showing the last checked information.")
    expect(financialPreparationRequests.find(({ request }) => request.action === "cancelStart")).toEqual({
      request: { action: "cancelStart", selectionToken: marketSelectionToken, startRequestId: financialStart.startRequestId }, confirmed: true,
    })
    expect(financialControls.queryByRole("button", { name: "Cancel loading" })).toBeNull()
    expect(financialControls.queryByRole("button", { name: "Retry" })).toBeNull()
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
    wrongFinancialJob = false
    await userEvent.setup().click(financialControls.getByRole("button", { name: "Check loading" }))
    await financialControls.findByText(/^Loading financial information…/)
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!).receipt).toMatchObject({
      jobId: financialJobId, generation: jobGeneration, sequence: financialSequence,
    })
    const financialReadsBeforeCancel = issuedQueries.filter((request) => request.query === "investmentFinancials").length
    financialMissingReason = "no_records"
    await userEvent.setup().click(financialControls.getByRole("button", { name: "Cancel loading" }))
    await financialControls.findByText("Financial information loading was cancelled.")
    expect(financialPreparationRequests.at(-1)).toEqual({ request: {
      action: "cancel", selectionToken: marketSelectionToken, jobId: financialJobId,
      generation: jobGeneration, expectedSequence: financialSequence,
    }, confirmed: true })
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "investmentFinancials")).toHaveLength(financialReadsBeforeCancel + 1))
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials").at(-1)).toEqual({
      query: "investmentFinancials", selectionToken: marketSelectionToken, section: "filings", limit: 32,
    })
    expect(financialControls.getByText("Financial information loading was cancelled.")).toBeTruthy()
    expect(financialControls.queryByText("Financial information is ready.")).toBeNull()
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!)).toMatchObject({
      startRequestId: financialStart.startRequestId,
      receipt: { jobId: financialJobId, generation: jobGeneration, sequence: "9007199254740995" },
    })

    investment.unmount()
    investment = openInvestment(lookupRoute(parsed.matches[0]!))
    await screen.findByRole("heading", { name: "MSQ · Requested investment" })
    await userEvent.setup().click(screen.getByRole("tab", { name: "Filings" }))
    financialControlNode = screen.getByRole("group", { name: "Financial information loading" })
    financialControls = within(financialControlNode)
    await financialControls.findByText("Financial information loading was cancelled.")
    await within(screen.getByRole("region", { name: "Filings" })).findByText("No reported information is available for this section at the information date.")
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!).startRequestId).toBe(financialStart.startRequestId)

    await userEvent.setup().click(financialControls.getByRole("button", { name: "Retry" }))
    await financialControls.findByText(/^Loading financial information…/)
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(3)
    await userEvent.setup().click(screen.getByRole("tab", { name: "Facts" }))
    expect(screen.getByRole("group", { name: "Financial information loading" })).toBe(financialControlNode)
    const preparedFacts = within(await screen.findByRole("region", { name: "Reported financial facts" }))
    await preparedFacts.findByText("USD 123,456.78")
    await userEvent.setup().click(preparedFacts.getByRole("button", { name: "Next" }))
    await preparedFacts.findByText("USD 234,567.89")
    const pagedReads = issuedQueries.filter((request) => request.query === "investmentFinancials").length
    let financialEventSequence = 0
    const publishFinancialJob = () => subscriptions.at(-1)!({ productSessionToken: blockedBootstrap.productSessionToken,
      sequence: String(++financialEventSequence), body: { type: "invalidate", domains: ["job"] } })
    financialVersion = 1
    financialPreparationState = "completed"
    await act(async () => { publishFinancialJob() })
    await preparedFacts.findByText("Updated financial information is ready. Use the refresh icon to open it.")
    expect(preparedFacts.getByText("USD 234,567.89")).toBeTruthy()
    expect(preparedFacts.getByText("Page 2")).toBeTruthy()
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials")).toHaveLength(pagedReads)
    financialMode = "pending"
    financialSignal = undefined
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))
    await waitFor(() => expect(financialSignal).toBeDefined())
    expect(preparedFacts.getByText("USD 234,567.89")).toBeTruthy()
    financialMode = "available"
    await act(async () => { finishFinancial?.(financialResult()) })
    await preparedFacts.findByText("USD 345,678.90")
    expect(preparedFacts.getByText("Page 1")).toBeTruthy()
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials").at(-1)).toEqual({
      query: "investmentFinancials", selectionToken: marketSelectionToken, section: "facts", limit: 32,
    })

    // Refresh failures preserve the verified page and recover through the read's
    // Retry; a completed loading request cannot admit another automatic job.
    await financialControls.findByText("Financial information is ready.")
    expect(financialControls.queryByRole("button", { name: /^(Load|Update) financial information$/ })).toBeNull()
    financialVersion = 2
    financialMode = "mismatch"
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh investment" }))
    await preparedFacts.findByRole("alert")
    expect(preparedFacts.getByText("USD 345,678.90")).toBeTruthy()
    financialMode = "available"
    await userEvent.setup().click(preparedFacts.getByRole("button", { name: "Retry" }))
    await preparedFacts.findByText("USD 456,789.01")
    const freshReads = issuedQueries.filter((request) => request.query === "investmentFinancials").length
    await act(async () => { publishFinancialJob() })
    await financialControls.findByText("Financial information is ready.")
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials")).toHaveLength(freshReads)
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(3)
    expect(JSON.parse(sessionStorage.getItem(financialRecoveryKey)!).receipt).toMatchObject({
      jobId: "781276a0-33f1-4fb3-8cbb-bb2095acd0cb", generation: jobGeneration, sequence: "9007199254740995",
    })
    await userEvent.setup().click(screen.getByRole("tab", { name: "Filings" }))
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "investmentFinancials").at(-1)).toEqual({
      query: "investmentFinancials", selectionToken: marketSelectionToken, section: "filings", limit: 32,
    }))
    expect(financialPreparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(3)

    // Selecting a recent range loads it immediately; a lost acknowledgment
    // survives a fresh App/QueryClient without replaying the durable start.
    const user = userEvent.setup()
    const selectedAt = Date.parse("2026-08-10T12:00:00Z")
    const rangeClock = vi.spyOn(Date, "now").mockReturnValue(selectedAt)
    const requestedWindow = {
      startDate: new Date(selectedAt - 90 * 86_400_000).toISOString().slice(0, 10),
      endDate: new Date(selectedAt).toISOString().slice(0, 10), pointLimit: 512,
    }
    try {
      const retainedChart = await screen.findByRole("img", { name: /Daily investment prices in USD/ })
      expect(preparationRequests).toHaveLength(0)
      await user.selectOptions(screen.getByLabelText("History window"), "90")
      await screen.findByText("History loading could not be checked. Check the original request before trying again.")
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({
        query: "marketHistory", historyToken, ...requestedWindow, generationToken: initialHistoryGeneration,
      }))
      expect(screen.queryByRole("button", { name: /^(Load|Update) history$/ })).toBeNull()
      expect(preparationRequests).toHaveLength(1)
      const started = preparationRequests[0]!
      expect(started.confirmed).toBe(true)
      expect(started.request).toMatchObject({ action: "start", historyToken, lookbackDays: 90 })
      if (started.request.action !== "start") throw new Error("The first preparation request was not Start.")
      const startRequestId = started.request.startRequestId
      expect(startRequestId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
      expect(JSON.parse(sessionStorage.getItem(recoveryKey)!)).toMatchObject({ startRequestId, lookbackDays: 90 })
      expect((screen.getByLabelText("History window") as HTMLSelectElement).disabled).toBe(true)
      expect(screen.getByRole("img", { name: /Daily investment prices in USD/ })).toBe(retainedChart)
      await user.click(screen.getByRole("button", { name: "Check loading" }))
      await waitFor(() => expect(preparationRequests.filter(({ request }) => request.action === "reconcileStart")).toHaveLength(1))
      await screen.findByText("The original loading request has not been checked.")
      expect((screen.getByLabelText("History window") as HTMLSelectElement).disabled).toBe(true)

      const subscriptionsBeforeReload = subscriptions.length
      const getsBeforeReload = preparationRequests.filter(({ request }) => request.action === "get").length
      investment.unmount()
      wrongProfileSelection = false
      const reloaded = openInvestment(lookupRoute(parsed.matches[0]!))
      await screen.findByText("Loading history…")
      await waitFor(() => expect(preparationRequests.filter(({ request }) => request.action === "get")).toHaveLength(getsBeforeReload + 1))
      expect(preparationRequests.at(-1)).toEqual({ request: { action: "get", historyToken, jobId: historyJobId, generation: jobGeneration }, confirmed: false })
      expect(preparationRequests.filter(({ request }) => request.action === "reconcileStart")).toEqual([
        { request: { action: "reconcileStart", historyToken, lookbackDays: 90, startRequestId }, confirmed: false },
        { request: { action: "reconcileStart", historyToken, lookbackDays: 90, startRequestId }, confirmed: false },
      ])
      expect(preparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(1)
      await waitFor(() => expect((screen.getByLabelText("History window") as HTMLSelectElement).value).toBe("90"))
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({
        query: "marketHistory", historyToken, ...requestedWindow, generationToken: initialHistoryGeneration,
      }))
      const historyReadsBeforeCompletion = issuedQueries.filter((request) => request.query === "marketHistory").length
      await waitFor(() => expect(subscriptions).toHaveLength(subscriptionsBeforeReload + 1))
      let eventSequence = 0
      const publishJob = () => subscriptions.at(-1)!({ productSessionToken: blockedBootstrap.productSessionToken,
        sequence: String(++eventSequence), body: { type: "invalidate", domains: ["job"] } })
      rangeClock.mockReturnValue(selectedAt + 86_400_000)
      preparationState = "completed"
      await act(async () => { publishJob() })
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(historyReadsBeforeCompletion + 1))
      expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({
        query: "marketHistory", historyToken, ...requestedWindow,
      })
      await waitFor(() => expect(screen.getAllByText("124.56789 USD").length).toBeGreaterThan(0))
      expect((screen.getByLabelText("History window") as HTMLSelectElement).value).toBe("90")
      expect(JSON.parse(sessionStorage.getItem(recoveryKey)!).receipt.sequence).toBe("11")
      const completedGets = preparationRequests.filter(({ request }) => request.action === "get").length
      await act(async () => { publishJob() })
      await waitFor(() => expect(preparationRequests.filter(({ request }) => request.action === "get")).toHaveLength(completedGets + 1))
      expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(historyReadsBeforeCompletion + 1)
      await user.selectOptions(screen.getByLabelText("History window"), "all")
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({
        query: "marketHistory", historyToken, pointLimit: 512, generationToken: publishedHistoryGeneration,
      }))
      expect(preparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(1)
      reloaded.unmount()

      // A first open with no saved history automatically requests one year.
      // Publications can fill the chart before the job settles; later price
      // events must not reopen an acquired snapshot or retry the mutation.
      sessionStorage.removeItem(recoveryKey)
      historyAvailability = "missing"
      preparationState = "running"
      const firstMissingReadOffset = issuedQueries.length
      const firstMissing = openInvestment(lookupRoute(parsed.matches[0]!))
      await screen.findByText("History loading could not be checked. Check the original request before trying again.")
      expect(preparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
      expect(preparationRequests.filter(({ request }) => request.action === "start").at(-1)?.request).toMatchObject({
        action: "start", historyToken, lookbackDays: 365,
      })
      expect((screen.getByLabelText("History window") as HTMLSelectElement).value).toBe("365")
      expect(screen.queryByRole("heading", { name: "Price history is unavailable" })).toBeNull()
      expect(issuedQueries.slice(firstMissingReadOffset).filter((request) => request.query === "marketHistory")).toEqual([
        { query: "marketHistory", historyToken, pointLimit: 512 },
      ])
      const initialRecovery = JSON.parse(sessionStorage.getItem(recoveryKey)!)
      expect(initialRecovery.startRequestId).not.toBe(startRequestId)
      let arrivalSequence = 0
      const publishHistory = (domain: "market" | "source") => subscriptions.at(-1)!({
        productSessionToken: blockedBootstrap.productSessionToken, sequence: String(++arrivalSequence),
        body: { type: "invalidate", domains: [domain] },
      })
      const missingReads = issuedQueries.filter((request) => request.query === "marketHistory").length
      await act(async () => { publishHistory("market") })
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(missingReads + 1))
      expect(preparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
      historyAvailability = "available"
      await act(async () => { publishHistory("market") })
      const arrivedChart = await screen.findByRole("img", { name: /Daily investment prices in USD/ })
      const pinnedReads = issuedQueries.filter((request) => request.query === "marketHistory").length
      await act(async () => { publishHistory("market") })
      expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(pinnedReads)
      historyAvailability = "error"
      await act(async () => { publishHistory("source") })
      await screen.findByText("Price history could not be updated. Showing saved prices, which may be out of date.")
      expect(screen.getByRole("img", { name: /Daily investment prices in USD/ })).toBe(arrivedChart)
      expect(screen.queryByRole("heading", { name: "Price history is unavailable" })).toBeNull()
      expect(screen.queryByText("History loading could not be checked. Check the original request before trying again.")).toBeNull()
      historyAvailability = "available"
      await act(async () => { publishHistory("market") })
      await waitFor(() => expect(screen.queryByText("Price history could not be updated. Showing saved prices, which may be out of date.")).toBeNull())
      expect(screen.getByRole("img", { name: /Daily investment prices in USD/ })).toBe(arrivedChart)
      expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toMatchObject({
        generationToken: initialHistoryGeneration,
      })
      const recoveredReads = issuedQueries.filter((request) => request.query === "marketHistory").length
      await act(async () => { publishHistory("market") })
      expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(recoveredReads)
      await user.click(screen.getByRole("button", { name: "Check loading" }))
      await screen.findByText("Loading history…")
      await user.click(screen.getByRole("button", { name: "Cancel loading" }))
      await screen.findByText("History loading was cancelled.")
      expect(JSON.parse(sessionStorage.getItem(recoveryKey)!).receipt.sequence).toBe("12")
      firstMissing.unmount()
      historyAvailability = "missing"
      const cancelledHistory = openInvestment(lookupRoute(parsed.matches[0]!))
      await screen.findByText("History loading was cancelled.")
      const cancelledReads = issuedQueries.filter((request) => request.query === "marketHistory").length
      arrivalSequence = 0
      await act(async () => { publishHistory("market") })
      await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(cancelledReads + 1))
      expect(preparationRequests.filter(({ request }) => request.action === "start")).toHaveLength(2)
      expect(JSON.parse(sessionStorage.getItem(recoveryKey)!).startRequestId).toBe(initialRecovery.startRequestId)
      cancelledHistory.unmount()
      historyAvailability = "available"
    } finally {
      rangeClock.mockRestore()
    }

    // A development UI refresh must not send a new command to an older running native bridge.
    const financialQueriesBefore = issuedQueries.filter((request) => request.query === "investmentFinancials").length
    const unsupported = openInvestment(lookupRoute(parsed.matches[0]!), false)
    expect(await screen.findByRole("heading", { name: "MSQ · Requested investment" })).toBeTruthy()
    expect(screen.getByText("Financial details are not available in this app session.")).toBeTruthy()
    expect(screen.queryByRole("tab", { name: "Facts" })).toBeNull()
    expect(issuedQueries.filter((request) => request.query === "investmentFinancials")).toHaveLength(financialQueriesBefore)
    unsupported.unmount()

    const staleToken = "market_ffffffffffffffffffffffffffffffff"
    openInvestment(`/investments/${staleToken}`)
    await waitFor(() => expect(screen.getAllByRole("alert").some((alert) => alert.textContent?.includes("This investment could not be opened"))).toBe(true))
    expect(issuedQueries).toContainEqual({ query: "marketInstrument", selectionToken: staleToken })
    expect(screen.queryByRole("heading", { name: "MSQ · Requested investment" })).toBeNull()
    // A rejected detail route must not select another investment as fallback.
    expect(screen.queryByRole("heading", { name: "Bitcoin" })).toBeNull()
  })

  it("renders one provider-neutral market journey with current price and explicit selection", async () => {
    // Load the real lazy route before timing UI assertions; Vite's cold transform is not app latency.
    await import("@/features/markets")
    await import("@/components/overview-page")
    await import("@/features/opportunities")
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    let collectionRevision = 3
    let marketRefreshFails = true
    let collectionRefreshFails = false
    let collectionChoices = ["SPY", "QQQ", "DIA", "IWM", "VTI", "AAPL", "MSFT", "NVDA", "TSLA"]
      .map((symbol) => ({ symbol, kept: symbol !== "QQQ" }))
    const historyToken = "history_0123456789abcdef0123456789abcdef"
    const generationToken = "a".repeat(64)
    const historyResult: ApplicationResult = {
      data: { data: {
        historyToken, currency: "USD", partial: false, generationToken,
        bars: [
          { originalOrdinal: "0", breakBefore: [false, false, false], time: { precision: "nominal_date", date: "2026-06-01" }, open: "65000.123456789", high: "65002", low: "64999", close: "65001.123456789", volume: "12" },
          { originalOrdinal: "2", breakBefore: [false, false, false], time: { precision: "nominal_date", date: "2026-08-08" }, open: "68000", high: "68002", low: "67999", close: "68001.123456789", volume: "15" },
        ],
        display: { method: "first_last_min_max", originalPointCount: "3", visibleOriginalPointCount: "3", returnedPointCount: 2,
          firstTimeUnixNanos: null, lastTimeUnixNanos: null, projectionDigest: "b".repeat(64), reduced: true },
        viewport: { startUnixNanos: null, endUnixNanos: null, startDate: null, endDate: null, pointLimit: 512,
          fullStartUnixNanos: null, fullEndUnixNanos: null, fullStartDate: "2026-06-01", fullEndDate: "2026-08-08" },
      }, unavailableReason: null },
      metadata: { completeness: "complete", returnedItems: 2, availableItems: 2 },
    }
    let viewportSignal: AbortSignal | undefined
    let resolveViewport: ((result: ApplicationResult) => void) | undefined
    let holdInstrumentRead = false
    let holdHistoryRead = false
    let resolveHistory: (() => void) | undefined
    let resolveInstrument: (() => void) | undefined
    const readyBootstrap: DesktopSystemBootstrap = {
      ...blockedBootstrap,
      capabilities: ["market_overview", "market_instrument"],
    }
    const nativeTransport = transport(readyBootstrap, undefined, async (request, options) => {
      issuedQueries.push(request)
      if (request.query === "analysisSettings") return {
        data: { label: "Recommended", kind: "recommended", activatedAt: "1800000000000000000",
          workflowAvailability: "available", nextAction: "Analyze this investment." },
        metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 },
      }
      if (request.query === "marketCollection") {
        if (collectionRefreshFails || (request.includeMarket === true && marketRefreshFails)) {
          throw new Error("Current market evidence could not be read.")
        }
        return {
          data: { revision: collectionRevision.toString(), entries: collectionChoices.map((choice) => ({
            ...choice,
            market: request.includeMarket && choice.symbol === "SPY" ? {
              ...marketOverviewRow,
              identity: { symbol: "SPY", name: "S&P 500 fund", assetClass: "fund" },
            } : null,
          })) },
          metadata: { completeness: "complete", returnedItems: collectionChoices.length, availableItems: collectionChoices.length },
        }
      }
      if (request.query === "marketSetCollectionChoice") {
        if (request.expectedRevision !== collectionRevision.toString()) throw new Error("Collection revision is stale.")
        collectionChoices = collectionChoices.map((choice) => choice.symbol === request.symbol
          ? { ...choice, kept: request.kept } : choice)
        collectionRevision += 1
        return {
          data: { revision: collectionRevision.toString(), choices: collectionChoices },
          metadata: { completeness: "complete", returnedItems: collectionChoices.length, availableItems: collectionChoices.length },
        }
      }
      if (request.query === "investmentProfile") return {
        data: { selectionToken: request.selectionToken, knowledgeAt: marketObservedAt,
          state: "missing", reason: "official_membership", profile: null },
        metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 },
      }
      if (request.query === "marketOverview") return marketOverviewResult
      if (request.query === "marketInstrument") {
        if (holdInstrumentRead) await new Promise<void>((resolve) => { resolveInstrument = resolve })
        return marketResult({
          ...marketOverviewRow, historyToken, priceBasis: "bid_ask_midpoint",
          changeBasis: { ...marketOverviewRow.changeBasis, priceBasis: "bid_ask_midpoint" },
          quote: { ...marketOverviewRow.quote, tradeStatus: "ambiguous", lastPrice: null, lastSize: null,
            lastObservedAt: null, lastCurrentThrough: null, lastFresh: false },
        })
      }
      if (request.query === "marketHistory") {
        if (holdHistoryRead) await new Promise<void>((resolve) => { resolveHistory = resolve })
        if (request.startDate !== undefined) {
          viewportSignal = options?.signal
          return new Promise<ApplicationResult>((resolve) => { resolveViewport = resolve })
        }
        return historyResult
      }
      throw new Error(`Unexpected market query: ${request.query}`)
    })
    const setupMessage = "Choose a portfolio and confirm your allocation preferences on the Portfolio page before starting analysis."
    const workflow: Extract<AnalyticalControllerResponse, { kind: "workflow" }>["workflow"] = {
      workflowToken: "workflow_44444444444444444444444444444444", kind: "investment_analysis", state: "waiting",
      progress: { stage: "preparing", completedSteps: 0, waitingForBackgroundWork: false },
      coverage: null, resultCount: 0, resultActionTokens: [], resultOrdering: null, unavailableMembers: [],
      startedAt: "1800000000000000000", updatedAt: "1800000000000000000", explanation: null,
      canCancel: true, canResume: false,
    }
    const analysisStarts: { request: Parameters<ProductTransport["analyticalController"]>[0]; confirmed?: boolean }[] = []
    const readController = nativeTransport.product.analyticalController
    nativeTransport.product.analyticalController = async (request, confirmed, options) => {
      if (request.action === "analyzeInvestment") {
        analysisStarts.push({ request, confirmed })
        if (analysisStarts.length === 1) throw { code: "analysis_setup_required", message: setupMessage }
        if (analysisStarts.length === 2) throw { code: "internal", message: "Private diagnostic details" }
        return { kind: "workflow", workflow }
      }
      if (request.action === "status" && analysisStarts.length === 3) {
        return { ...analyticalControllerStatus(), workflows: [workflow] }
      }
      return readController(request, confirmed, options)
    }
    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App
          transport={nativeTransport}
        />
      </MemoryRouter>,
    )

    const collection = within(await screen.findByRole("region", { name: "Watchlist" }))
    expect(await collection.findByText("Market information is unavailable")).toBeTruthy()
    await user.click(collection.getByText("Removed investments (1)"))
    for (const choice of collectionChoices) expect(collection.getByText(choice.symbol)).toBeTruthy()
    expect((collection.getByRole("button", { name: "Remove SPY from your watchlist" }) as HTMLButtonElement).disabled).toBe(false)
    expect((collection.getByRole("button", { name: "Follow QQQ in your watchlist" }) as HTMLButtonElement).disabled).toBe(false)
    expect(collection.queryByText("$68,000.15")).toBeNull()
    expect(issuedQueries).toContainEqual({ query: "marketCollection", includeMarket: true })
    await user.click(collection.getByRole("button", { name: "Remove SPY from your watchlist" }))
    const restore = await collection.findByRole("button", { name: "Follow SPY in your watchlist" })
    await waitFor(() => expect((restore as HTMLButtonElement).disabled).toBe(false))
    expect(await collection.findByText("Market information is unavailable")).toBeTruthy()
    await user.click(restore)
    await waitFor(() => expect((collection.getByRole("button", { name: "Remove SPY from your watchlist" }) as HTMLButtonElement).disabled).toBe(false))
    expect(issuedQueries.filter((request) => request.query === "marketSetCollectionChoice")).toEqual([
      { query: "marketSetCollectionChoice", expectedRevision: "3", symbol: "SPY", kept: false, confirmed: true },
      { query: "marketSetCollectionChoice", expectedRevision: "4", symbol: "SPY", kept: true, confirmed: true },
    ])
    expect(await collection.findByText("Market information is unavailable")).toBeTruthy()

    // A failed background read must preserve the last matching price, without claiming it is live.
    marketRefreshFails = false
    await user.click(collection.getByRole("button", { name: "Refresh watchlist" }))
    expect(await collection.findByText("$68,000.15")).toBeTruthy()
    marketRefreshFails = true
    collectionRefreshFails = true
    await user.click(collection.getByRole("button", { name: "Refresh watchlist" }))
    await waitFor(() => expect((collection.getByRole("button", { name: "Remove SPY from your watchlist" }) as HTMLButtonElement).disabled).toBe(true))
    expect(await collection.findByText("Saved watchlist could not be refreshed")).toBeTruthy()
    expect(collection.getByText("$68,000.15")).toBeTruthy()
    expect(collection.getByText(/Showing saved prices and investment details/)).toBeTruthy()
    expect(collection.queryByText(/^Current/)).toBeNull()
    collectionRefreshFails = false
    marketRefreshFails = false
    await user.click(collection.getByRole("button", { name: "Refresh watchlist" }))
    await waitFor(() => expect((collection.getByRole("button", { name: "Remove SPY from your watchlist" }) as HTMLButtonElement).disabled).toBe(false))

    await user.click(screen.getByRole("link", { name: "Explore investments" }))
    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    const marketHeading = await screen.findByRole("heading", { name: "Bitcoin" })
    const marketCard = marketHeading.closest("button")
    expect(marketCard).toBeInstanceOf(HTMLButtonElement)
    if (!(marketCard instanceof HTMLButtonElement)) {
      throw new Error("The market card is absent")
    }

    expect(within(marketCard).getByText("68000.15 USD")).toBeTruthy()
    expect(
      issuedQueries.filter((request) => request.query === "marketInstrument"),
    ).toHaveLength(0)

    expect(screen.queryByRole("region", { name: "Watchlist" })).toBeNull()
    await user.click(marketCard)
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "marketInstrument"),
      ).toEqual([
        { query: "marketInstrument", selectionToken: marketSelectionToken },
      ])
    })
    expect(screen.getAllByRole("heading", { name: "BTC-USD · Bitcoin" })).toHaveLength(1)
    const quote = within(screen.getByRole("region", { name: "Quote and last trade" }))
    expect(quote.getByText("USD 68,000.1")).toBeTruthy()
    expect(quote.getByText("USD 68,000.2")).toBeTruthy()
    expect(quote.getByText(/Several trades share the latest timestamp/)).toBeTruthy()
    expect(quote.queryByText("0.5")).toBeNull()
    const investmentPrice = within(screen.getByRole("region", { name: "Investment price" }))
    expect(investmentPrice.getByText(/^Bid\/ask midpoint · Current/)).toBeTruthy()
    expect(investmentPrice.getByText(/^Gain: \+1\.25% Compared with/)).toBeTruthy()
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

    await screen.findByRole("img", { name: /Daily investment prices in USD/ })
    expect(issuedQueries.filter((request) => request.query === "marketHistory")).toEqual([
      { query: "marketHistory", historyToken, pointLimit: 512 },
    ])
    expect(screen.getAllByText("68001.123456789 USD").length).toBeGreaterThan(0)
    const historyWindow = screen.getByLabelText("History window")
    expect(screen.getByRole("button", { name: "Refresh investment" }).textContent).toBe("")
    expect(screen.queryByRole("button", { name: /Refresh (price|profile|saved history|this section)/ })).toBeNull()
    const selectedAt = Date.now()
    await user.selectOptions(historyWindow, "30")
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toEqual([
      { query: "marketHistory", historyToken, pointLimit: 512 },
      { query: "marketHistory", historyToken,
        startDate: new Date(selectedAt - 30 * 86_400_000).toISOString().slice(0, 10),
        endDate: new Date(selectedAt).toISOString().slice(0, 10), pointLimit: 512, generationToken },
    ]))
    expect(screen.getByLabelText("History window")).toBe(historyWindow)
    expect(screen.getAllByText("68001.123456789 USD").length).toBeGreaterThan(0)
    expect(viewportSignal?.aborted).toBe(false)
    await user.click(screen.getByRole("link", { name: "Back to Markets" }))
    await waitFor(() => expect(viewportSignal?.aborted).toBe(true))
    expect(screen.queryByLabelText("History window")).toBeNull()
    // A late cancelled response cannot repopulate the departed detail route.
    // Returning opens saved history without the previous window or generation.
    resolveViewport?.(historyResult)
    const reopenedCard = (await screen.findByRole("heading", { name: "Bitcoin" })).closest("button")
    if (!reopenedCard) throw new Error("The returning market card is absent")
    await user.click(reopenedCard)
    await screen.findByRole("img", { name: /Daily investment prices in USD/ })
    expect((screen.getByLabelText("History window") as HTMLSelectElement).value).toBe("all")
    expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({ query: "marketHistory", historyToken, pointLimit: 512 })
    await user.click(screen.getByRole("button", { name: "Refresh investment" }))
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(4))
    expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({ query: "marketHistory", historyToken, pointLimit: 512 })

    // Warm navigation must retain the selected price without claiming that the
    // returning screen has checked its freshness before revalidation completes.
    holdInstrumentRead = true
    holdHistoryRead = true
    await user.click(screen.getByRole("link", { name: "Back to Markets" }))
    const returningCard = (await screen.findByRole("heading", { name: "Bitcoin" })).closest("button")
    if (!returningCard) throw new Error("The returning market card is absent")
    await user.click(returningCard)
    const returningPrice = within(await screen.findByRole("region", { name: "Investment price" }))
    expect(returningPrice.getByText("USD 68,000.15")).toBeTruthy()
    expect(returningPrice.getByText(/Saved price/)).toBeTruthy()
    expect(returningPrice.queryByText(/^Bid\/ask midpoint · Current/)).toBeNull()
    await waitFor(() => expect(resolveHistory).toBeTypeOf("function"))
    expect(screen.getByRole("img", { name: /Daily investment prices in USD/ })).toBeTruthy()
    expect(screen.getAllByText("68001.123456789 USD").length).toBeGreaterThan(0)
    expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({ query: "marketHistory", historyToken, pointLimit: 512 })
    holdHistoryRead = false
    await act(async () => { resolveHistory?.() })
    await waitFor(() => expect(resolveInstrument).toBeTypeOf("function"))
    resolveInstrument?.()
    expect(await returningPrice.findByText(/^Bid\/ask midpoint · Current/)).toBeTruthy()

    const renderedText = document.body.textContent ?? ""
    expect(renderedText).not.toMatch(/kraken|coinbase|websocket-v2/i)
    expect(renderedText).not.toContain(marketSelectionToken)
    expect(renderedText).not.toMatch(/\bticks?\b|\blots?\b/i)

    await user.click(screen.getByRole("button", { name: "Analyze this investment" }))
    await waitFor(() => expect(screen.getByRole("alert")).toHaveProperty("textContent", setupMessage))
    expect(screen.getByRole("link", { name: "Open Portfolio" }).getAttribute("href")).toBe("/portfolio")
    expect(screen.getByRole("link", { name: "Back to Markets" })).toBeTruthy()
    expect(analysisStarts).toHaveLength(1)
    await user.click(screen.getByRole("button", { name: "Analyze this investment" }))
    await waitFor(() => expect(screen.getByRole("alert")).toHaveProperty("textContent", "Analysis could not start. Check current analysis activity and try again."))
    expect(screen.queryByText("Private diagnostic details")).toBeNull()
    expect(screen.queryByRole("link", { name: "Open Portfolio" })).toBeNull()
    expect(analysisStarts).toHaveLength(2)
    await user.click(screen.getByRole("button", { name: "Analyze this investment" }))
    expect(await screen.findByText("Selected analysis")).toBeTruthy()
    expect(analysisStarts).toEqual(Array.from({ length: 3 }, () => ({
      request: { action: "analyzeInvestment", selectionToken: marketSelectionToken }, confirmed: true,
    })))
  })

  it("opens the exact saved valuation with method amounts and preserves explicit selection", async () => {
    const user = userEvent.setup()
    const selectedToken = "11111111-1111-4111-8111-111111111111"
    const listedToken = "22222222-2222-4222-8222-222222222222"
    const unavailable = { state: "unavailable" as const, summary: "Insufficient completed outcomes." }
    const money = (amount: string) => ({ amount, currency: "USD" })
    const families = ["current_market", "broader_research", "price_pattern", "forecast", "financial_model", "valuation", "historical_test", "out_of_sample", "liquidity", "portfolio_risk"] as const
    const analysis: InvestmentAnalysis = {
      actionToken: selectedToken, investment: { symbol: "MSFT", name: "Microsoft" },
      portfolioLabel: "Research portfolio", currency: "USD",
      recommendation: { kind: "unavailable", summary: "Research values are available; an investment decision is not." },
      horizon: { informationCurrentThrough: "2026-09-01T00:00:00.000000000Z", endsAt: "2026-10-01T00:00:00.000000000Z", expiresAt: "2026-09-02T00:00:00.000000000Z" },
      priceSummary: {
        current: null, fairValue: null, scenarios: null, actionRanges: null,
        valuationMethods: {
          sourceCutoffUnixNanos: "1788220800000000000", marketCutoffUnixNanos: "1788220800000000000", completedAtUnixNanos: "1788220800000000001",
          methods: [
            { method: "discounted_cash_flow", status: "calculated", basis: "total_common_equity", lower: money("900000"), central: money("1000000"), upper: money("1100000"), recommendationUse: "not_per_instrument_unit", terminalGrowth: { uncapped: "0.04", riskFreeCap: "0.03", applied: "0.03" }, residualTerminal: null },
            { method: "comparable_companies", status: "unavailable", summary: "Comparable-company evidence is missing." },
            { method: "residual_income", status: "calculated", basis: "reporting_entity_total", lower: money("-100"), central: money("0"), upper: money("100"), recommendationUse: "not_per_instrument_unit", terminalGrowth: null, residualTerminal: { condition: "Residual earnings fade to zero.", explicitPeriods: 5, continuingValueSensitivity: "0.2" } },
            { method: "forecast_distribution", status: "calculated", basis: "per_instrument_unit", lower: money("90"), central: money("100"), upper: money("110"), recommendationUse: "admission_unavailable", terminalGrowth: null, residualTerminal: null },
          ],
        },
      },
      chart: null, chartAvailable: false,
      probabilities: { priceHigher: { ...unavailable, benchmark: null, assumptions: [] }, benchmarkOutperformance: { ...unavailable, benchmark: null, assumptions: [] }, profitAfterCosts: { ...unavailable, benchmark: null, assumptions: [] } },
      reasons: ["Saved method evidence is retained independently of recommendation availability."], risks: [], assumptions: ["Original financial inputs remain fixed."], invalidators: [],
      evidenceSummary: { coverage: { availableCount: 0, possibleCount: 10, items: families.map((kind) => ({ kind, state: "unavailable" })), summary: "No combined investment estimate is available." }, calibration: unavailable, outOfSample: unavailable, historicalTest: null, costs: unavailable, uncertainty: unavailable },
      analyticalEvidence: { currentMarket: unavailable, broaderResearch: unavailable, pricePattern: { ...unavailable, outcome: "not_evaluated" }, forecast: unavailable, financialModel: unavailable, valuation: unavailable, historicalTest: unavailable, outOfSample: unavailable, liquidity: unavailable, portfolioRisk: unavailable, combination: { state: "insufficient", summary: "Research is not an investment recommendation." } },
      liquidity: unavailable, portfolioContext: unavailable,
      virtualPaperEligibility: { state: "not_eligible", executionAuthority: "none", requiresExplicitPaperApproval: true, requiresFreshRiskCheck: true, summary: "No paper action is authorized." },
      outcomeProjection: null, sizing: { ...unavailable, reason: "no_generated_proposal" }, expectedReturn: unavailable, realizedOutcome: null, trackRecordActionToken: null,
    }
    const result = (data: unknown): ApplicationResult => ({ data, metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 } })
    // The backend always publishes this field, including null when no methods exist.
    const noMethods = { ...analysis, actionToken: listedToken, investment: { symbol: "AAPL", name: "Apple" }, priceSummary: { ...analysis.priceSummary, valuationMethods: null } }
    expect(parseInvestmentAnalysis(result(noMethods), listedToken).priceSummary.valuationMethods).toBeNull()
    const requested: string[] = []
    render(
      <MemoryRouter initialEntries={[`/advanced/valuation-targets?analysis=${selectedToken}`]}>
        <App transport={transport({ ...blockedBootstrap, capabilities: ["decision_analysis", "decision_analysis_list"] }, undefined, async (request) => {
          if (request.query === "decisionInvestmentAnalyses") return result({ completeness: "complete", returnedCount: 1, availableCount: 1, nextAfterActionToken: null, analyses: [{ actionToken: listedToken, investment: noMethods.investment, portfolioLabel: analysis.portfolioLabel, currency: analysis.currency, horizon: analysis.horizon, recommendation: analysis.recommendation }] })
          if (request.query === "decisionInvestmentAnalysis") {
            requested.push(request.actionToken)
            return result(request.actionToken === selectedToken ? analysis : noMethods)
          }
          throw new Error(`Unexpected valuation query: ${request.query}`)
        })} />
      </MemoryRouter>,
    )
    expect(await screen.findByText("Comparable-company evidence is missing.")).toBeTruthy()
    expect(screen.getByText("Residual earnings fade to zero.")).toBeTruthy()
    const cashFlow = within(screen.getByRole("region", { name: "Discounted cash flow" }))
    expect(cashFlow.getByText("Total common equity")).toBeTruthy()
    expect(cashFlow.getByText(/1000000/)).toBeTruthy()
    expect(cashFlow.queryByText("Per instrument unit")).toBeNull()
    const residual = within(screen.getByRole("region", { name: "Residual income" }))
    expect(residual.getByText("Reporting entity total")).toBeTruthy()
    expect(residual.getByText(/-100/)).toBeTruthy()
    const ranges = screen.getByRole("heading", { name: "Price ranges" }).closest("section")
    expect(ranges?.querySelectorAll("dd")).toHaveLength(7)
    expect([...ranges!.querySelectorAll("dd")].every((value) => value.textContent === "Unavailable")).toBe(true)
    expect(requested).toEqual([selectedToken])
    await user.click(screen.getByRole("button", { name: /AAPL/ }))
    await waitFor(() => expect(requested).toEqual([selectedToken, listedToken]))
    await waitFor(() => expect(screen.queryByText("Residual earnings fade to zero.")).toBeNull())
  })

  it("renders one provider-neutral economic context with paired date cutoffs", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopSystemBootstrap = {
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
            if (request.query === "researchCollections") {
              return emptyRowsResult
            }
            throw new Error(`Unexpected research query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    const macroHeading = await screen.findByRole("heading", {
      name: "Rates, labor, income and energy prices",
    })
    const macroSection = macroHeading.closest("section")
    expect(macroSection).toBeInstanceOf(HTMLElement)
    if (!(macroSection instanceof HTMLElement)) {
      throw new Error("The economic context is absent")
    }
    expect(within(macroSection).getByText(`${macroIndicatorDefinitions.length} of ${macroIndicatorDefinitions.length} available`)).toBeTruthy()
    expect(
      within(macroSection)
        .getAllByRole("heading", { level: 4 })
        .map((heading) => heading.textContent).sort(),
    ).toEqual(macroIndicatorDefinitions.map(([, label]) => label).sort())

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

    const refreshedMacro = await screen.findByRole("heading", {
      name: "Rates, labor, income and energy prices",
    })
    const renderedMacro = refreshedMacro.closest("section")?.textContent ?? ""
    expect(renderedMacro).not.toMatch(
      /Federal Reserve|FRED|ALFRED|H\.?15|Macro\.GetContext|\bprovider\b|\bsource\b|\bmanifest\b|\bdigest\b/i,
    )
  })

  it("loads selected portfolio reports on demand and discards a cancelled account response", async () => {
    const user = userEvent.setup()
    const first = "portfolio_11111111111111111111111111111111"
    const second = "portfolio_22222222222222222222222222222222"
    const accounts = [first, second].map((accountToken, index) => ({
      accountToken, displayName: `Portfolio ${index + 1}`, currency: "USD", holdings: 2, dataIssues: 0,
    }))
    const result = (data: unknown, count = 1): ApplicationResult => ({
      data, metadata: { completeness: "complete", returnedItems: count, availableItems: count },
    })
    const report = (name: string): PortfolioRiskReport => ({
      accountName: name, asOf: "2026-09-01T00:00:00Z", availableAt: "2026-09-02T00:00:00Z", horizon: "One day",
      coverage: { state: "complete", observations: 65, period: "Saved account history", explanation: "Comparable recorded periods." },
      measures: ["Value at risk", "Expected shortfall", "Annualized volatility"].map((label) => ({
        label: label as PortfolioRiskReport["measures"][number]["label"], value: "2.5%", status: "available", explanation: "Measured from retained returns.",
      })),
      stress: { label: "Market decline", impact: { amount: "-125", currency: "USD" }, status: "available", explanation: "An assumed decline, not a forecast.", assumptions: ["Holdings remain fixed."] },
      recommendation: {
        action: "abstain", horizon: "One day", summary: `${name} original risk guidance`, ranges: [],
        reasons: ["Risk measures alone do not establish a trade."], risks: ["Losses can exceed historical estimates."],
        assumptions: ["Recorded holdings remain unchanged."], invalidators: ["A new account revision."],
        validity: { state: "unavailable", explanation: "No trade approval." },
        uncertainty: { level: "high", explanation: "Historical estimates can change.", outOfSampleEvidence: "unavailable", calibration: "unavailable", tradingCosts: "unavailable", pointInTimeInputs: "supported" },
      },
    })
    const cashReport = {
      accountId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", snapshotToken: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      effectiveAtUnixNanos: "1790000000000000000", availableAtUnixNanos: "1790000000000000001", dataConfidence: "limited",
      currentValue: { amount: "9007199254741043.01", currency: "USD" }, historyStatus: "insufficient_history",
      accountingEvidence: {
        cash: { amount: { amount: "9007199254740993.01", currency: "USD" }, observedAtUnixNanos: "1790000000000000000", status: "available" },
        reportedMarketValue: { amount: "50", currency: "USD" },
        unrealizedGain: { status: "available", amount: { amount: "10", currency: "USD" } },
        realizedGain: { status: "not_available" },
        income: { status: "partial", amount: { amount: "7.50", currency: "USD" } },
        fees: { status: "available", amount: { amount: "-2", currency: "USD" } },
        reconciliation: { status: "clear", discrepancies: [] },
      },
    }
    const holdingsPage = (next: boolean, name: string | null = next ? null : "Original investment") => ({
      pageCursor: next ? "next-position-page" : "first-position-page",
      nextCursor: next ? null : "next-position-page",
      snapshotToken: cashReport.snapshotToken, effectiveAtUnixNanos: cashReport.effectiveAtUnixNanos,
      availableAtUnixNanos: cashReport.availableAtUnixNanos,
      holdings: [{
        accountId: cashReport.accountId, snapshotToken: cashReport.snapshotToken,
        instrumentId: next ? "22222222-2222-4222-8222-222222222222" : "11111111-1111-4111-8111-111111111111",
        investment: { name, symbol: null }, currency: "USD", quantity: "1.000000000000000001", lotSize: "0.000000000000000001",
        marketValue: { amount: "9007199254740993.01", currency: "USD" },
        asOfUnixNanos: cashReport.effectiveAtUnixNanos, costBasis: { state: "not_available" },
        price: { asOfUnixNanos: cashReport.effectiveAtUnixNanos, state: "reported", confidence: "limited", explanation: "Value recorded in this portfolio observation." },
      }],
    })
    const exposurePage = (next: boolean, name = "Exposure investment") => {
      const page = holdingsPage(next, name)
      return { ...page, holdings: page.holdings.map((holding) => ({ ...holding,
        quantity: next ? "-1.000000000000000001" : holding.quantity,
        marketValue: { amount: next ? "-75.01" : "50", currency: "USD" },
      })), exposure: {
        net: { amount: "-25.01", currency: "USD" }, gross: { amount: "125.01", currency: "USD" }, positionCount: 2,
        currency: [{ currency: "USD", amount: { amount: "974.99", currency: "USD" } }],
        sector: [{ classification: "unclassified", amount: { amount: "-25.01", currency: "USD" } }],
        factor: [{ classification: "unclassified", amount: { amount: "-25.01", currency: "USD" } }],
        calculationStatus: "available", classificationStatus: "not_supplied_by_portfolio_source",
      } }
    }
    const scenarioReads: ProductQuery[] = []
    let scenarioSignal: AbortSignal | undefined
    let resolveScenario: ((value: ApplicationResult) => void) | undefined
    const scenarioResult = (request: Extract<ProductQuery, { query: "portfolioScenario" }>) => result({
      calculationToken: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", calculatedAtUnixNanos: "1780000000000000004",
      accountId: cashReport.accountId, snapshotToken: request.snapshotToken,
      effectiveAtUnixNanos: cashReport.effectiveAtUnixNanos, availableAtUnixNanos: cashReport.availableAtUnixNanos,
      dataConfidence: "limited", scenario: { ...request.scenario,
        contributions: [{ instrumentId: "11111111-1111-4111-8111-111111111111",
          investment: { name: "Stress investment", symbol: null }, amount: { amount: "-900719925474099.301", currency: "USD" } }],
        total: { amount: "-900719925474099.301", currency: "USD" },
      },
    })
    const rebalanceReads: Extract<ProductQuery, { query: "portfolioRebalance" }>[] = []
    let rebalanceSignal: AbortSignal | undefined
    let resolveRebalance: ((value: ApplicationResult) => void) | undefined
    const rebalanceResult = (request: Extract<ProductQuery, { query: "portfolioRebalance" }>) => result({
      calculationToken: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", calculatedAtUnixNanos: "1780000000000000004",
      accountId: cashReport.accountId, snapshotToken: request.snapshotToken,
      effectiveAtUnixNanos: cashReport.effectiveAtUnixNanos, availableAtUnixNanos: cashReport.availableAtUnixNanos,
      dataConfidence: "limited", proposal: request.proposal,
      totalValue: { amount: "1075", currency: "USD" },
      trades: [{ instrumentId: "11111111-1111-4111-8111-111111111111",
        investment: { name: "Rebalance investment", symbol: null },
        currentValue: { amount: "50", currency: "USD" }, valueChange: { amount: "109.375", currency: "USD" },
        projectedValue: { amount: "159.375", currency: "USD" },
      }, { instrumentId: "22222222-2222-4222-8222-222222222222", investment: null,
        currentValue: { amount: "25", currency: "USD" }, valueChange: { amount: "390.625", currency: "USD" },
        projectedValue: { amount: "415.625", currency: "USD" },
      }], projectedCash: { amount: "500", currency: "USD" }, turnoverPercent: "23.25581395348837209302325581", constrained: true,
    })
    let resolveSave: ((value: ApplicationResult) => void) | undefined
    let saveOptions: { signal?: AbortSignal } | undefined
    let savedDetailSignal: AbortSignal | undefined
    let resolveSavedDetail: ((value: ApplicationResult) => void) | undefined
    let savedDetailReads = 0
    const planningReads: ProductQuery[] = []
    const savedSummary = {
      savedResultToken: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      calculationToken: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", accountToken: second,
      kind: "rebalance", snapshotToken: cashReport.snapshotToken,
      portfolioEffectiveAtUnixNanos: cashReport.effectiveAtUnixNanos,
      portfolioAvailableAtUnixNanos: cashReport.availableAtUnixNanos,
      calculatedAtUnixNanos: "1780000000000000004", savedAtUnixNanos: "1790000000000000004",
    }
    const savedDetail = () => result({ summary: savedSummary,
      request: { operation: "Portfolio.ProposeRebalance", arguments: rebalanceReads[0] },
      result: rebalanceResult(rebalanceReads[0]!).data,
    })
    const exposureReads: { account: string; cursor?: string }[] = []
    const baselineSnapshot = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    const historyPage = {
      selectedSnapshotToken: cashReport.snapshotToken,
      pageCursor: "first-history-page", nextCursor: null,
      revisions: [cashReport.snapshotToken, baselineSnapshot].map((snapshotToken, index) => ({
        snapshotToken, effectiveAtUnixNanos: index === 0 ? cashReport.effectiveAtUnixNanos : "1780000000000000000",
        availableAtUnixNanos: index === 0 ? cashReport.availableAtUnixNanos : "1780000000000000001",
        holdingCount: 2, transactionCount: 0, dataIssueCount: 0, dataState: "ready",
      })),
    }
    const comparisonPage = (next: boolean) => ({
      snapshotToken: cashReport.snapshotToken, baselineSnapshotToken: baselineSnapshot,
      effectiveAtUnixNanos: cashReport.effectiveAtUnixNanos, availableAtUnixNanos: cashReport.availableAtUnixNanos,
      baselineEffectiveAtUnixNanos: "1780000000000000000", baselineAvailableAtUnixNanos: "1780000000000000001",
      pageCursor: next ? "next-comparison-page" : "first-comparison-page", nextCursor: next ? null : "next-comparison-page",
      total: { amount: "-24", currency: "USD" },
      explanation: "Reported market value change before cash-flow and corporate-action adjustments. This is not investment performance.",
      contributions: [{
        instrumentId: next ? "22222222-2222-4222-8222-222222222222" : "11111111-1111-4111-8111-111111111111",
        investment: { name: next ? "New short position" : "Compared investment", symbol: null },
        opening: { amount: next ? "0" : "3", currency: "USD" },
        closing: { amount: next ? "-25" : "4", currency: "USD" },
        amount: { amount: next ? "-25" : "1", currency: "USD" },
      }],
    })
    const historyReads: string[] = []
    const transactionReads: { account: string; cursor?: string }[] = []
    let transactionSignal: AbortSignal | undefined
    let resolveTransactions: ((value: ApplicationResult) => void) | undefined
    const transactionPage = (next: boolean) => ({
      snapshotToken: cashReport.snapshotToken,
      effectiveAtUnixNanos: cashReport.effectiveAtUnixNanos,
      availableAtUnixNanos: cashReport.availableAtUnixNanos,
      pageCursor: next ? "next-transaction-page" : "first-transaction-page",
      nextCursor: next ? null : "next-transaction-page",
      transactions: [{
        transactionToken: next ? "dddddddd-dddd-4ddd-8ddd-dddddddddddd" : "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        accountId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", snapshotToken: cashReport.snapshotToken,
        instrumentId: next ? null : "11111111-1111-4111-8111-111111111111",
        investment: next ? null : { name: "Recorded investment", symbol: "REC" },
        category: next ? "fee" : "trade", amount: { amount: next ? "-1.25" : "9007199254740993.02", currency: "USD" },
        quantity: next ? null : "-0.000000000000000001", occurredAtUnixNanos: "1780000000000000000",
        lotMethod: next ? null : "First in, first out",
      }],
    })
    const comparisonReads: { account: string; selected: string; baseline: string; cursor?: string }[] = []
    let historySignal: AbortSignal | undefined
    let resolveHistory: ((value: ApplicationResult) => void) | undefined
    let exposureSignal: AbortSignal | undefined
    let resolveExposure: ((value: ApplicationResult) => void) | undefined
    const holdingsReads: { account: string; cursor?: string }[] = []
    let holdingsSignal: AbortSignal | undefined
    let resolveHoldings: ((value: ApplicationResult) => void) | undefined
    const performanceReads: string[] = []
    let performanceSignal: AbortSignal | undefined
    let resolvePerformance: ((value: ApplicationResult) => void) | undefined
    const reads: string[] = []
    let firstSignal: AbortSignal | undefined
    let resolveFirst: ((value: ApplicationResult) => void) | undefined
    render(
      <MemoryRouter initialEntries={["/portfolio"]}>
        <App transport={transport({ ...blockedBootstrap, capabilities: ["portfolio_account_list", "portfolio_risk", "portfolio_performance", "portfolio_holdings", "portfolio_exposure", "portfolio_revision_list", "portfolio_attribution", "portfolio_transactions", "portfolio_scenario", "portfolio_scenario_batch", "portfolio_rebalance", "portfolio_planning_save", "portfolio_planning_list", "portfolio_planning_get"] }, undefined, async (request, options) => {
          if (request.query === "portfolioSavePlanningResult") {
            planningReads.push(request)
            saveOptions = options
            return new Promise<ApplicationResult>((resolve) => { resolveSave = resolve })
          }
          if (request.query === "portfolioPlanningResults") {
            planningReads.push(request)
            return result({ results: [savedSummary], pageCursor: "saved-first-page", nextCursor: null })
          }
          if (request.query === "portfolioPlanningResult") {
            planningReads.push(request)
            savedDetailReads += 1
            if (savedDetailReads > 1) {
              savedDetailSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveSavedDetail = resolve })
            }
            return savedDetail()
          }
          if (request.query === "portfolioRebalance") {
            rebalanceReads.push(request)
            if (rebalanceReads.length > 1) {
              rebalanceSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveRebalance = resolve })
            }
            return rebalanceResult(request)
          }
          if (request.query === "portfolioScenario") {
            scenarioReads.push(request)
            if (scenarioReads.length > 1) {
              scenarioSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveScenario = resolve })
            }
            return scenarioResult(request)
          }
          if (request.query === "portfolioAccounts") return result({ accounts, nextCursor: null }, 2)
          if (request.query === "portfolioTransactions") {
            transactionReads.push({ account: request.accountToken, cursor: request.cursor })
            if (request.accountToken === first) {
              transactionSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveTransactions = resolve })
            }
            return result(transactionPage(request.cursor === "next-transaction-page"))
          }
          if (request.query === "portfolioRevisions") {
            historyReads.push(request.accountToken)
            if (request.accountToken === first) {
              historySignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveHistory = resolve })
            }
            return result(historyPage, 2)
          }
          if (request.query === "portfolioAttribution") {
            comparisonReads.push({ account: request.accountToken, selected: request.selectedSnapshotToken,
              baseline: request.baselineSnapshotToken, cursor: request.cursor })
            return result(comparisonPage(request.cursor === "next-comparison-page"))
          }
          if (request.query === "portfolioExposure") {
            exposureReads.push({ account: request.accountToken, cursor: request.cursor })
            if (request.accountToken === first) {
              exposureSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveExposure = resolve })
            }
            return result(exposurePage(request.cursor === "next-position-page"))
          }
          if (request.query === "portfolioHoldings") {
            holdingsReads.push({ account: request.accountToken, cursor: request.cursor })
            if (request.accountToken === first) {
              holdingsSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveHoldings = resolve })
            }
            return result(holdingsPage(request.cursor === "next-position-page"))
          }
          if (request.query === "portfolioPerformance") {
            performanceReads.push(request.accountToken)
            if (request.accountToken === first) {
              performanceSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolvePerformance = resolve })
            }
            return result(performanceReads.length > 2 ? {
              ...cashReport, historyStatus: undefined, periods: 1,
              timeWeightedReturn: "-0.01234567890123456789", moneyWeightedReturn: "0.0125",
            } : cashReport)
          }
          if (request.query === "portfolioRisk") {
            reads.push(request.accountToken)
            if (request.accountToken === first) {
              firstSignal = options?.signal
              return new Promise<ApplicationResult>((resolve) => { resolveFirst = resolve })
            }
            return result(report("Portfolio 2"))
          }
          throw new Error(`Unexpected portfolio query: ${request.query}`)
        })} />
      </MemoryRouter>,
    )
    await user.click(await screen.findByRole("button", { name: /Portfolio 1/ }, { timeout: 5_000 }))
    expect(reads).toEqual([])
    expect(performanceReads).toEqual([])
    expect(holdingsReads).toEqual([])
    expect(exposureReads).toEqual([])
    expect(historyReads).toEqual([])
    expect(comparisonReads).toEqual([])
    await user.click(screen.getByText("History and planning"))
    await waitFor(() => expect(historyReads).toEqual([first]))
    expect(transactionReads).toEqual([])
    await user.click(screen.getByText("Transaction history", { selector: "summary" }))
    await waitFor(() => expect(transactionReads).toEqual([{ account: first, cursor: undefined }]))
    await user.click(screen.getByText("Exposure"))
    await waitFor(() => expect(exposureReads).toEqual([{ account: first, cursor: undefined }]))
    await user.click(screen.getByText("Positions"))
    await waitFor(() => expect(holdingsReads).toEqual([{ account: first, cursor: undefined }]))
    await user.click(screen.getByText("Cash and performance"))
    await waitFor(() => expect(performanceReads).toEqual([first]))
    await user.click(screen.getByText("Risk and guidance"))
    await waitFor(() => expect(reads).toEqual([first]))
    await user.click(screen.getByRole("button", { name: /Portfolio 2/ }))
    await waitFor(() => expect(firstSignal?.aborted).toBe(true))
    expect(performanceSignal?.aborted).toBe(true)
    expect(holdingsSignal?.aborted).toBe(true)
    expect(exposureSignal?.aborted).toBe(true)
    expect(historySignal?.aborted).toBe(true)
    expect(transactionSignal?.aborted).toBe(true)
    await user.click(screen.getByText("Cash and performance"))
    expect(await screen.findByText("USD 9,007,199,254,740,993.01")).toBeTruthy()
    expect(screen.getByText("USD 7.50 · Partial")).toBeTruthy()
    expect(screen.getByText("At least two portfolio observations are needed to calculate returns.")).toBeTruthy()
    expect(screen.getByText("Realized gain").parentElement?.textContent).toContain("Not available")
    resolvePerformance?.(result({ ...cashReport, currentValue: { amount: "999", currency: "USD" } }))
    await waitFor(() => expect(performanceReads).toEqual([first, second]))
    expect(screen.queryByText("USD 999")).toBeNull()
    await user.click(screen.getByRole("button", { name: "Refresh cash and performance" }))
    expect(await screen.findByText("-1.234567890123456789%")).toBeTruthy()
    expect(screen.getByText("1.25%")).toBeTruthy()
    await user.click(screen.getByText("Cash and performance"))
    await waitFor(() => expect(screen.queryByText("USD 9,007,199,254,740,993.01")).toBeNull())
    expect(reads).toEqual([first])
    await user.click(screen.getByText("Risk and guidance"))
    expect(await screen.findByText("Portfolio 2 original risk guidance")).toBeTruthy()
    resolveFirst?.(result(report("Portfolio 1")))
    await waitFor(() => expect(reads).toEqual([first, second]))
    expect(screen.queryByText("Portfolio 1 original risk guidance")).toBeNull()
    await user.click(screen.getByText("Risk and guidance"))
    await waitFor(() => expect(screen.queryByText("Portfolio 2 original risk guidance")).toBeNull())
    await user.click(screen.getByText("Positions"))
    expect(await screen.findByText("Original investment")).toBeTruthy()
    expect(screen.getByText("1.000000000000000001")).toBeTruthy()
    expect(screen.getByText("USD 9,007,199,254,740,993.01")).toBeTruthy()
    expect(screen.getByText("Cost basis not available")).toBeTruthy()
    resolveHoldings?.(result(holdingsPage(false, "Wrong account investment")))
    const positions = within(screen.getByLabelText("Positions for Portfolio 2"))
    await user.click(positions.getByRole("button", { name: "Next" }))
    expect(await screen.findByText("Investment name unavailable")).toBeTruthy()
    expect(screen.queryByText("Original investment")).toBeNull()
    await user.click(positions.getByRole("button", { name: "Previous" }))
    expect(await screen.findByText("Original investment")).toBeTruthy()
    expect(holdingsReads.at(-1)).toEqual({ account: second, cursor: "first-position-page" })
    expect(screen.queryByText("Wrong account investment")).toBeNull()
    await user.click(positions.getByRole("button", { name: "Refresh positions" }))
    await waitFor(() => expect(holdingsReads.at(-1)).toEqual({ account: second, cursor: undefined }))
    await user.click(screen.getByText("Positions"))
    await waitFor(() => expect(screen.queryByText("Original investment")).toBeNull())
    await user.click(screen.getByText("Exposure"))
    expect(await screen.findByText("Exposure investment")).toBeTruthy()
    const exposure = within(screen.getByLabelText("Exposure for Portfolio 2"))
    expect(exposure.getByText("USD -25.01")).toBeTruthy()
    expect(exposure.getByText("USD 125.01")).toBeTruthy()
    expect(exposure.getByText("USD 974.99")).toBeTruthy()
    expect(exposure.getByText(/Missing classifications do not mean zero exposure/)).toBeTruthy()
    resolveExposure?.(result(exposurePage(false, "Wrong account exposure")))
    await user.click(exposure.getByRole("button", { name: "Next" }))
    expect(await screen.findByText("USD -75.01")).toBeTruthy()
    expect(exposure.getByText("USD -25.01")).toBeTruthy()
    expect(exposure.getByText("USD 125.01")).toBeTruthy()
    await user.click(exposure.getByRole("button", { name: "Previous" }))
    await waitFor(() => expect(exposureReads.at(-1)).toEqual({ account: second, cursor: "first-position-page" }))
    expect(screen.queryByText("Wrong account exposure")).toBeNull()
    await user.click(screen.getByText("Exposure", { selector: "summary" }))
    await waitFor(() => expect(screen.queryByText("Exposure investment")).toBeNull())
    await user.click(screen.getByText("History and planning"))
    const chooseEarlier = await screen.findByRole("button", { name: "Compare with this version" })
    expect(comparisonReads).toEqual([])
    resolveHistory?.(result({ ...historyPage, selectedSnapshotToken: baselineSnapshot }, 2))
    await user.click(chooseEarlier)
    expect(await screen.findByText("Compared investment")).toBeTruthy()
    expect(comparisonReads.at(-1)).toEqual({ account: second, selected: cashReport.snapshotToken,
      baseline: baselineSnapshot, cursor: undefined })
    const comparison = within(screen.getByLabelText("Saved position value comparison"))
    expect(comparison.getByText("USD -24")).toBeTruthy()
    expect(comparison.getByText("USD 1")).toBeTruthy()
    await user.click(comparison.getByRole("button", { name: "Next" }))
    expect(await screen.findByText("New short position")).toBeTruthy()
    expect(comparison.getByText("USD -24")).toBeTruthy()
    expect(comparison.getAllByText("USD -25")).toHaveLength(2)
    await user.click(comparison.getByRole("button", { name: "Previous" }))
    expect(await screen.findByText("Compared investment")).toBeTruthy()
    expect(comparisonReads.at(-1)?.cursor).toBe("first-comparison-page")
    await user.click(screen.getByRole("button", { name: "Clear comparison" }))
    expect(screen.queryByLabelText("Saved position value comparison")).toBeNull()
    await user.click(screen.getByText("Transaction history", { selector: "summary" }))
    expect(await screen.findByText("USD 9,007,199,254,740,993.02")).toBeTruthy()
    const transactionRegion = within(screen.getByLabelText("Transaction history for Portfolio 2"))
    expect(transactionRegion.getByText("-0.000000000000000001")).toBeTruthy()
    resolveTransactions?.(result({ ...transactionPage(false), transactions: [{ ...transactionPage(false).transactions[0],
      investment: { name: "Wrong account activity", symbol: null } }] }))
    await user.click(transactionRegion.getByRole("button", { name: "Next" }))
    expect(await screen.findByText("USD -1.25")).toBeTruthy()
    expect(screen.queryByText("Wrong account activity")).toBeNull()
    await user.click(transactionRegion.getByRole("button", { name: "Previous" }))
    expect(await screen.findByText("USD 9,007,199,254,740,993.02")).toBeTruthy()
    expect(transactionReads.at(-1)?.cursor).toBe("first-transaction-page")
    await user.click(screen.getByText("Transaction history", { selector: "summary" }))
    await waitFor(() => expect(screen.queryByLabelText("Transaction history for Portfolio 2")).toBeNull())
    await user.click(screen.getByText("History and planning"))
    await waitFor(() => expect(screen.queryByLabelText("Saved portfolio history for Portfolio 2")).toBeNull())
    expect(scenarioReads).toEqual([])
    await user.click(screen.getByText("Stress tests", { selector: "summary" }))
    await user.type(await screen.findByLabelText("Scenario name"), "decline")
    await user.selectOptions(screen.getByLabelText("Shock composition"), "additive")
    await user.click(screen.getByRole("button", { name: "Add price shock" }))
    await user.selectOptions(screen.getByLabelText("Shock investment"), "11111111-1111-4111-8111-111111111111")
    await user.type(screen.getByLabelText("Price change (%)"), "-10.0")
    await user.click(screen.getByRole("button", { name: "Calculate scenario" }))
    expect(await screen.findByRole("rowheader", { name: "Stress investment" })).toBeTruthy()
    expect(scenarioReads).toEqual([{ query: "portfolioScenario", accountToken: second,
      snapshotToken: cashReport.snapshotToken,
      scenario: { id: "decline", composition: "additive", shocks: [{
        instrumentId: "11111111-1111-4111-8111-111111111111", percentChange: "-10.0",
      }] },
    }])
    fireEvent.change(screen.getByLabelText("Price change (%)"), { target: { value: "-20" } })
    expect(screen.queryByText("Stress investment")).toBeNull()
    await user.click(screen.getByRole("button", { name: "Calculate scenario" }))
    await waitFor(() => expect(scenarioReads).toHaveLength(2))
    await user.click(screen.getByText("Stress tests", { selector: "summary" }))
    expect(scenarioSignal?.aborted).toBe(true)
    const lateScenario = scenarioReads[1]
    if (lateScenario?.query === "portfolioScenario") resolveScenario?.(scenarioResult(lateScenario))
    await waitFor(() => expect(screen.queryByText("Stress investment")).toBeNull())

    expect(rebalanceReads).toEqual([])
    await user.click(screen.getByText("History and planning", { selector: "summary" }))
    const rebalanceToggle = await screen.findByText("Rebalance plan", { selector: "summary" })
    await user.click(rebalanceToggle)
    const rebalancePanel = within(rebalanceToggle.parentElement!)
    await user.type(await rebalancePanel.findByLabelText(/^Target for/), "25.0")
    await user.click(rebalancePanel.getByRole("button", { name: "Next" }))
    await user.type(await rebalancePanel.findByLabelText(/^Target for/), "75")
    await user.type(rebalancePanel.getByLabelText("Maximum turnover (%)"), "100")
    await user.type(rebalancePanel.getByLabelText("Minimum cash reserve"), "500")
    await user.selectOptions(rebalancePanel.getByLabelText("Reserve currency"), "USD")
    await user.selectOptions(rebalancePanel.getByLabelText("Short positions"), "exclude")
    await user.click(rebalancePanel.getByRole("button", { name: "Calculate rebalance" }))
    expect(await screen.findByText("Rebalance investment")).toBeTruthy()
    expect(screen.getByText("USD 109.375")).toBeTruthy()
    expect(rebalanceReads).toEqual([{ query: "portfolioRebalance", accountToken: second,
      snapshotToken: cashReport.snapshotToken, proposal: {
        targets: [
          { instrumentId: "11111111-1111-4111-8111-111111111111", targetPercent: "25.0" },
          { instrumentId: "22222222-2222-4222-8222-222222222222", targetPercent: "75" },
        ], maxTurnoverPercent: "100", minimumCash: { amount: "500", currency: "USD" }, allowShort: false,
      },
    }])
    expect(planningReads).toEqual([])
    await user.click(rebalancePanel.getByRole("button", { name: "Save planning result" }))
    await waitFor(() => expect(planningReads).toHaveLength(1))
    expect(planningReads[0]).toEqual({ query: "portfolioSavePlanningResult", accountToken: second,
      calculationToken: savedSummary.calculationToken, confirmed: true })
    expect(saveOptions?.signal).toBeUndefined()
    fireEvent.change(rebalancePanel.getByLabelText("Minimum cash reserve"), { target: { value: "600" } })
    expect(screen.queryByLabelText("Rebalance calculation results")).toBeNull()
    await user.click(rebalancePanel.getByRole("button", { name: "Calculate rebalance" }))
    await waitFor(() => expect(rebalanceReads).toHaveLength(2))
    await user.click(rebalanceToggle)
    await waitFor(() => expect(rebalanceSignal?.aborted).toBe(true))
    resolveRebalance?.(rebalanceResult(rebalanceReads[1]!))
    await waitFor(() => expect(screen.queryByText("Rebalance investment")).toBeNull())
    resolveSave?.(result({ summary: savedSummary }))
    await user.click(screen.getByText("Saved planning results", { selector: "summary" }))
    await user.click(await screen.findByRole("button", { name: /Open saved rebalance plan calculated/ }))
    expect(await screen.findByText("Rebalance investment")).toBeTruthy()
    expect(screen.getByText("USD 109.375")).toBeTruthy()
    expect(rebalanceReads).toHaveLength(2)
    await user.click(screen.getByRole("button", { name: "Close saved result" }))
    await user.click(screen.getByRole("button", { name: /Open saved rebalance plan calculated/ }))
    await waitFor(() => expect(savedDetailSignal).toBeDefined())
    await user.click(screen.getByRole("button", { name: "Close saved result" }))
    await waitFor(() => expect(savedDetailSignal?.aborted).toBe(true))
    resolveSavedDetail?.(savedDetail())
    await waitFor(() => expect(screen.queryByText("Rebalance investment")).toBeNull())
  })

  it("keeps portfolio planning explicit and analysis-only", async () => {
    const user = userEvent.setup()
    const accountToken = "portfolio_11111111111111111111111111111111"
    const instrumentId = "22222222-2222-4222-8222-222222222222"
    const reads: Extract<ProductQuery, { query: "portfolioCandidateImpact" }>[] = []
    let pendingSignal: AbortSignal | undefined
    let resolvePending: ((result: ApplicationResult) => void) | undefined
    const result = (data: unknown): ApplicationResult => ({ data,
      metadata: { completeness: "complete", returnedItems: 1, availableItems: 1 } })
    const amount = (amount: string) => ({ amount, currency: "USD" })
    const impact = (request: Extract<ProductQuery, { query: "portfolioCandidateImpact" }>) => result({
      calculationToken: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", calculatedAtUnixNanos: "1780000000000000004",
      accountToken: request.accountToken, accountId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      instrumentId, snapshotToken: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      portfolioEffectiveAtUnixNanos: "1780000000000000000",
      portfolioAvailableAtUnixNanos: "1780000000000000001", evidenceDigest: "a".repeat(64),
      assumptions: { proposedQuantity: request.proposedQuantity, scenarioShockPercent: request.scenarioShockPercent,
        quantityMeaning: "target_total", fundingAssumption: "cash_transfer_before_costs",
        portfolioValueBasis: "source_reported_holdings_with_selected_candidate_revalued", scenarioScope: "candidate_position_only" },
      positionState: "existing", currentQuantity: "0.5", proposedQuantity: "0",
      currentMarketValue: amount("50"), proposedMarketValue: amount("0"), capitalChange: amount("-50"),
      portfolioValue: amount("1000"),
      instrumentTerms: { priceTick: "0.01", lotSize: "1", quoteCurrency: "USD", contractMultiplier: "1" },
      costs: { fees: { state: "not_available" }, slippage: { state: "not_available" } },
      concentration: { current: "0.05", proposed: "0", change: "-0.05" },
      scenario: { shock: "-0.1", currentImpact: amount("-5"), proposedImpact: amount("0"), marginalImpact: amount("5") },
      price: { amount: amount("100"), asOfUnixNanos: "1780000000000000002", freshUntilUnixNanos: "1780000001000000000",
        state: "current", method: "Last trade", confidence: "limited" },
      missingInformation: ["Portfolio-wide current prices"],
      riskAssessment: { state: "incomplete", evaluatedAtUnixNanos: "1780000000000000003", checksCompleted: 1, checksUnavailable: 4 },
      updatedAtUnixNanos: "1780000000000000003", analysisOnly: true,
    })
    const product = transport(blockedBootstrap, undefined, async (request, options) => {
      if (request.query === "lookup") return result({ query: request.text, matches: [{
        category: "investment", title: "Example Company", subtitle: "Stock · USD",
        destination: { action: "open_investment", instrumentId, selectionToken: marketSelectionToken },
      }], categories: [{ category: "investment", state: "available" }], truncated: false })
      if (request.query !== "portfolioCandidateImpact") throw new Error("Unexpected planning read")
      reads.push(request)
      if (reads.length > 1) {
        pendingSignal = options?.signal
        return new Promise<ApplicationResult>((resolve) => { resolvePending = resolve })
      }
      return impact(request)
    }).product
    render(<QueryClientProvider client={createProductQueryClient()}>
      <PortfolioPlanning
        account={{ accountToken, displayName: "Portfolio 1", currency: "USD", holdings: 2, dataIssues: 0 }}
        bootstrap={{ productSessionToken: blockedBootstrap.productSessionToken, capabilities: ["portfolio_candidate_impact", "investment_lookup"] }}
        transport={product}
      />
    </QueryClientProvider>)
    const toggle = screen.getByText("Compare a position change")
    await user.click(toggle)
    expect(reads).toHaveLength(0)
    expect((screen.getByLabelText("Target total quantity") as HTMLInputElement).value).toBe("")
    expect((screen.getByLabelText("Price change (%)") as HTMLInputElement).value).toBe("")
    await user.type(screen.getByLabelText("Find an investment for comparison"), "Example")
    await user.click(await screen.findByRole("button", { name: "Choose Example Company" }))
    await user.type(screen.getByLabelText("Target total quantity"), "0.00")
    await user.type(screen.getByLabelText("Price change (%)"), "-10.0")
    await user.click(screen.getByRole("button", { name: "Compare position" }))
    const report = await screen.findByLabelText("Position comparison results")
    expect(reads).toEqual([{ query: "portfolioCandidateImpact", accountToken, instrumentId,
      proposedQuantity: "0.00", scenarioShockPercent: "-10.0" }])
    expect(within(report).getAllByText(/50/).length).toBeGreaterThan(0)
    fireEvent.change(screen.getByLabelText("Price change (%)"), { target: { value: "-20" } })
    expect(screen.queryByLabelText("Position comparison results")).toBeNull()
    await user.click(screen.getByRole("button", { name: "Compare position" }))
    await waitFor(() => expect(reads).toHaveLength(2))
    await user.click(toggle)
    await waitFor(() => expect(pendingSignal?.aborted).toBe(true))
    resolvePending?.(impact(reads[1]!))
    await waitFor(() => expect(screen.queryByLabelText("Position comparison results")).toBeNull())
  })

  it("coalesces committed updates during a read without cancelling or losing the next refresh", async () => {
    const queryClient = createProductQueryClient()
    const scope = blockedBootstrap.productSessionToken
    const queryKey = productKeys.operation(scope, "market", "marketOverview", {})
    const inactiveKey = productKeys.operation(scope, "market", "marketInstrument", { selectionToken: marketSelectionToken })
    const savedKey = productKeys.operation(scope, "decision", "decisionInvestmentAnalysis", { actionToken: "saved" })
    queryClient.setQueryData(queryKey, marketOverviewResult)
    queryClient.setQueryData(inactiveKey, marketOverviewResult)
    queryClient.setQueryData(savedKey, { original: true })
    const reads: { signal: AbortSignal; resolve: (result: ApplicationResult) => void; reject: (error: Error) => void }[] = []
    const observer = new QueryObserver(queryClient, {
      queryKey,
      queryFn: ({ signal }) => new Promise<ApplicationResult>((resolve, reject) => {
        reads.push({ signal, resolve, reject })
      }),
    })
    const releaseObserver = observer.subscribe(() => undefined)
    const snapshotKeys = [
      productKeys.operation(scope, "market", "Market.GetHistory", { generationToken: "saved", pointLimit: 512 }),
      productKeys.operation(scope, "market", "Market.GetHistory", { generationToken: "saved", originalOrdinal: "1" }),
      productKeys.operation(scope, "research", "Research.GetInvestmentFinancials", { cursor: "saved-page" }),
    ]
    const snapshotReads: { signal: AbortSignal; resolve: (result: ApplicationResult) => void }[][] = snapshotKeys.map(() => [])
    const snapshotObservers = snapshotKeys.map((snapshotKey, index) => {
      queryClient.setQueryData(snapshotKey, marketOverviewResult)
      return new QueryObserver(queryClient, {
        queryKey: snapshotKey,
        meta: snapshotQueryMeta,
        queryFn: ({ signal }) => new Promise<ApplicationResult>((resolve) => {
          snapshotReads[index]!.push({ signal, resolve })
        }),
      })
    })
    const releaseSnapshots = snapshotObservers.map((snapshotObserver) => snapshotObserver.subscribe(() => undefined))
    const profileKey = productKeys.operation(scope, "research", "Research.GetInvestmentProfile", { selectionToken: marketSelectionToken })
    queryClient.setQueryData(profileKey, marketOverviewResult)
    let profileReads = 0
    const profileObserver = new QueryObserver(queryClient, {
      queryKey: profileKey,
      queryFn: async () => { profileReads += 1; return marketOverviewResult },
    })
    const releaseProfile = profileObserver.subscribe(() => undefined)
    const subscriptions: Parameters<SystemTransport["subscribe"]>[1][] = []
    const base = transport(undefined, undefined, undefined, undefined, subscriptions)
    const view = render(<QueryClientProvider client={queryClient}>
      <ProductProvider transport={base}>{null}</ProductProvider>
    </QueryClientProvider>)
    let sequence = 0
    const invalidate = (domains: DesktopInvalidationDomain[] = ["market"]) => subscriptions[0]!({ productSessionToken: scope,
      sequence: String(++sequence), body: { type: "invalidate", domains } })
    try {
      await waitFor(() => expect(subscriptions).toHaveLength(1))
      const initial = observer.refetch()
      expect(reads).toHaveLength(1)
      // Routine publications refresh current data without opening saved snapshots.
      await act(async () => { invalidate(["market", "research"]) })
      await waitFor(() => expect(profileReads).toBe(1))
      snapshotReads.forEach((requests) => expect(requests).toHaveLength(0))
      snapshotKeys.forEach((snapshotKey) => expect(queryClient.getQueryState(snapshotKey)?.isInvalidated).toBe(false))
      // An explicit read is still allowed, and publications arriving during it
      // neither cancel it nor schedule another snapshot read afterward.
      const explicitSnapshots = snapshotObservers.map((snapshotObserver) => snapshotObserver.refetch())
      snapshotReads.forEach((requests) => expect(requests).toHaveLength(1))
      await act(async () => { invalidate(["market", "research"]); invalidate(); invalidate() })
      snapshotReads.forEach((requests) => {
        expect(requests).toHaveLength(1)
        expect(requests[0]!.signal.aborted).toBe(false)
      })
      await act(async () => {
        snapshotReads.forEach((requests) => requests[0]!.resolve(marketOverviewResult))
        await Promise.all(explicitSnapshots)
      })
      snapshotReads.forEach((requests) => expect(requests).toHaveLength(1))
      expect(reads).toHaveLength(1)
      expect(reads[0]!.signal.aborted).toBe(false)
      expect(queryClient.getQueryState(inactiveKey)?.isInvalidated).toBe(true)
      expect(queryClient.getQueryState(savedKey)?.isInvalidated).toBe(false)

      const updated = marketResult({ ...marketOverviewRow, price: { value: "68001.15", currency: "USD" } })
      await act(async () => { reads[0]!.resolve(updated); await initial })
      await waitFor(() => expect(reads).toHaveLength(2))
      expect(queryClient.getQueryData(queryKey)).toEqual(updated)
      // Further updates belong to one follow-up after this refresh, not three
      // replacement requests that repeatedly abort and starve a slow read.
      await act(async () => { invalidate(); invalidate(); invalidate() })
      expect(reads).toHaveLength(2)
      expect(reads[1]!.signal.aborted).toBe(false)
      await act(async () => { reads[1]!.resolve(marketOverviewResult) })
      await waitFor(() => expect(reads).toHaveLength(3))
      await act(async () => {
        invalidate()
        reads[2]!.reject(new Error("Current evidence could not be read."))
      })
      await waitFor(() => expect(queryClient.getQueryState(queryKey)?.fetchStatus).toBe("idle"))
      expect(reads).toHaveLength(3)
      expect(queryClient.getQueryData(queryKey)).toEqual(marketOverviewResult)
      expect(queryClient.getQueryState(queryKey)?.isInvalidated).toBe(true)

      // Even a Source-only lifecycle event revalidates both saved domains.
      await act(async () => { invalidate(["source"]) })
      await waitFor(() => snapshotReads.forEach((requests) => expect(requests).toHaveLength(2)))
      expect(reads).toHaveLength(4)
      await act(async () => {
        // Authority changes during a read retain one revalidation after it.
        invalidate(["source", "market", "research"])
        snapshotReads.forEach((requests) => requests[1]!.resolve(marketOverviewResult))
      })
      await waitFor(() => snapshotReads.forEach((requests) => expect(requests).toHaveLength(3)))
      await act(async () => { snapshotReads.forEach((requests) => requests[2]!.resolve(marketOverviewResult)) })
      await waitFor(() => snapshotKeys.forEach((snapshotKey) => expect(queryClient.getQueryState(snapshotKey)?.fetchStatus).toBe("idle")))
      await act(async () => {
        invalidate()
        subscriptions[0]!({ productSessionToken: scope, sequence: String(sequence), body: { type: "stream_disconnected" } })
      })
      expect(reads[3]!.signal.aborted).toBe(true)
      await act(async () => { reads[3]!.resolve(updated) })
      expect(reads).toHaveLength(4)
      expect(queryClient.getQueryData(queryKey)).toEqual(marketOverviewResult)
      // The same admitted scope reconnect still revalidates every snapshot.
      await waitFor(() => expect(subscriptions).toHaveLength(2), { timeout: 3_000 })
      await waitFor(() => snapshotReads.forEach((requests) => expect(requests).toHaveLength(4)))
      snapshotReads.forEach((requests) => expect(requests[3]!.signal.aborted).toBe(false))
      await act(async () => { snapshotReads.forEach((requests) => requests[3]!.resolve(marketOverviewResult)) })
    } finally {
      view.unmount()
      releaseObserver()
      releaseSnapshots.forEach((releaseSnapshot) => releaseSnapshot())
      releaseProfile()
      queryClient.clear()
    }
  })

  it("admits secure startup and reconnects the workspace without accepting an old session", async () => {
    const user = userEvent.setup()
    let rejectInitialStartup: ((error: Error) => void) | undefined
    const initialStartup = new Promise<never>((_resolve, reject) => { rejectInitialStartup = reject })
    let startupAttempts = 0
    let ready = false
    let submittedUnlock: string | null = null
    const subscriptions: {
      request: Parameters<SystemTransport["subscribe"]>[0]
      onEvent: Parameters<SystemTransport["subscribe"]>[1]
      unsubscribe: DesktopEventSubscription["unsubscribe"]
    }[] = []
    let resolveResume: ((subscription: DesktopEventSubscription) => void) | undefined
    let resolveReplacement: ((subscription: DesktopEventSubscription) => void) | undefined
    const replacementBootstrap = {
      ...blockedBootstrap,
      productSessionToken: "8cfa9e1a-652b-4475-ab21-15a28186c449",
    }
    const reconnectService = vi.fn<SystemTransport["reconnect"]>()
      .mockResolvedValueOnce(blockedBootstrap)
      .mockResolvedValueOnce(replacementBootstrap)
      .mockRejectedValueOnce(new Error("The local service could not restart."))
    const baseTransport = transport()
    const bootstrapTransport = {
      product: baseTransport.product,
      system: {
        ...baseTransport.system,
        bootstrap: async () => {
          if (++startupAttempts === 1) return initialStartup
          return ready
            ? blockedBootstrap
            : {
                status: "bootstrap_required" as const,
                requirement: "encrypted_fallback_locked" as const,
              }
        },
        bootstrapService: async (request) => {
          if (request.action !== "unlock_encrypted_fallback") {
            throw new Error("Expected the encrypted fallback unlock request.")
          }
          submittedUnlock = request.unlock
          ready = true
        },
        reconnect: reconnectService,
        subscribe: async (request, onEvent) => {
          const unsubscribe = vi.fn(async () => undefined)
          subscriptions.push({ request, onEvent, unsubscribe })
          if (subscriptions.length === 2) {
            return new Promise<DesktopEventSubscription>((resolve) => {
              resolveResume = resolve
            })
          }
          if (subscriptions.length === 3) {
            return new Promise<DesktopEventSubscription>((resolve) => {
              resolveReplacement = resolve
            })
          }
          return {
            receipt: {
              subscriptionId: "f49e02f6-8c47-43a5-bb33-030e8e0d12bb",
              productSessionToken: request.productSessionToken,
              sequence: request.afterSequence,
              resumed: request.afterSequence !== "0",
            },
            unsubscribe,
          }
        },
      },
    } satisfies DesktopTransport

    const view = render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={bootstrapTransport} />
      </MemoryRouter>,
    )

    expect(await screen.findByText("Loading workspace…")).toBeTruthy()
    expect(subscriptions).toHaveLength(0)
    expect(startupAttempts).toBe(1)
    await act(async () => { rejectInitialStartup!(new Error("Service startup failed")) })
    await user.click(await screen.findByRole("button", { name: "Try again" }))
    const field = await screen.findByLabelText("Local security password")
    expect(screen.queryByText("Investment workspace unavailable")).toBeNull()
    expect(subscriptions).toHaveLength(0)
    await user.type(field, "process-local-test-unlock")
    await user.click(screen.getByRole("button", { name: "Unlock secure storage" }))

    expect((field as HTMLInputElement).value).toBe("")
    expect(submittedUnlock).toBe("process-local-test-unlock")
    await waitFor(() => {
      expect(screen.queryByLabelText("Local security password")).toBeNull()
    })
    expect(await screen.findByRole("heading", { name: "What needs your attention now?" })).toBeTruthy()

    const summary = within(screen.getByRole("region", { name: "Workspace summary" }))
    await waitFor(() => expect(summary.getByText("Ready")).toBeTruthy())
    vi.useFakeTimers()
    try {
      await act(async () => {
        subscriptions[0]!.onEvent({
          productSessionToken: blockedBootstrap.productSessionToken,
          sequence: "1",
          body: { type: "invalidate", domains: ["job"] },
        })
        subscriptions[0]!.onEvent({
          productSessionToken: blockedBootstrap.productSessionToken,
          sequence: "1",
          body: { type: "stream_disconnected" },
        })
      })
      expect(summary.getByText("Reconnecting")).toBeTruthy()
      expect(screen.getByRole("heading", { name: "What needs your attention now?" })).toBeTruthy()
      expect(screen.queryByText("Loading workspace…")).toBeNull()
      expect(subscriptions[0]!.unsubscribe).toHaveBeenCalledOnce()
      await act(async () => { await vi.advanceTimersByTimeAsync(1_000) })
      expect(reconnectService).toHaveBeenNthCalledWith(1, blockedBootstrap.productSessionToken)
      expect(subscriptions).toHaveLength(2)
      expect(subscriptions[1]!.request).toEqual({
        productSessionToken: blockedBootstrap.productSessionToken,
        afterSequence: "1",
      })
      expect(summary.getByText("Reconnecting")).toBeTruthy()
      expect(screen.getByRole("heading", { name: "What needs your attention now?" })).toBeTruthy()
      expect(screen.queryByText("Loading workspace…")).toBeNull()
      await act(async () => {
        resolveResume!({
          receipt: {
            subscriptionId: "1aa5a4c6-c80e-4a09-b123-2ca6a4d24f47",
            productSessionToken: blockedBootstrap.productSessionToken,
            sequence: "1",
            resumed: true,
          },
          unsubscribe: subscriptions[1]!.unsubscribe,
        })
      })
      expect(summary.getByText("Ready")).toBeTruthy()
      await act(async () => {
        subscriptions[1]!.onEvent({
          productSessionToken: blockedBootstrap.productSessionToken,
          sequence: "1",
          body: { type: "stream_disconnected" },
        })
      })
      expect(screen.queryByText("Loading workspace…")).toBeNull()
      expect(screen.getByText(/Live updates are disconnected/)).toBeTruthy()
      await act(async () => { await vi.advanceTimersByTimeAsync(2_000) })
      expect(reconnectService).toHaveBeenNthCalledWith(2, blockedBootstrap.productSessionToken)
      // Deliver the replacement bootstrap through React Query's notification scheduler.
      await act(async () => { await vi.advanceTimersByTimeAsync(1) })
      expect(subscriptions).toHaveLength(3)
      expect(subscriptions[2]!.request).toEqual({
        productSessionToken: replacementBootstrap.productSessionToken,
        afterSequence: "0",
      })
      expect(screen.getByText("Loading workspace…")).toBeTruthy()
      await act(async () => {
        resolveReplacement!({
          receipt: {
            subscriptionId: "36aed8b2-3b6f-4541-8a43-ec80a2fdf89d",
            productSessionToken: replacementBootstrap.productSessionToken,
            sequence: "0",
            resumed: false,
          },
          unsubscribe: subscriptions[2]!.unsubscribe,
        })
      })
      expect(screen.queryByText("Loading workspace…")).toBeNull()
      expect(screen.getByRole("heading", { name: "What needs your attention now?" })).toBeTruthy()
      expect(summary.getByText("Ready")).toBeTruthy()
      await act(async () => {
        subscriptions[2]!.onEvent({
          productSessionToken: blockedBootstrap.productSessionToken,
          sequence: "0",
          body: { type: "stream_disconnected" },
        })
        await vi.advanceTimersByTimeAsync(30_000)
      })
      expect(summary.getByText("Unavailable")).toBeTruthy()
      expect(reconnectService).toHaveBeenCalledTimes(2)
      expect(subscriptions).toHaveLength(3)
      expect(subscriptions[1]!.unsubscribe).toHaveBeenCalledOnce()
      expect(subscriptions[2]!.unsubscribe).toHaveBeenCalledOnce()
      await act(async () => {
        fireEvent.click(screen.getByRole("button", { name: "Try again" }))
      })
      expect(reconnectService).toHaveBeenNthCalledWith(3, replacementBootstrap.productSessionToken)
      expect(screen.getByText("Workspace could not open")).toBeTruthy()
      expect(summary.getByText("Unavailable")).toBeTruthy()
      await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })
      expect(reconnectService).toHaveBeenCalledTimes(3)
      expect(subscriptions).toHaveLength(3)
    } finally {
      view.unmount()
      vi.useRealTimers()
    }
  })

  it("offers Retry for blocked Alpaca with retained configuration", () => {
    const source = {
      id: "alpaca.basic-market-data", name: "Alpaca",
      declaredCoverage: null, qualityCeiling: null, releaseState: null, zeroFee: null,
      accountRequirement: null, credentialRequirement: null, setupState: null, nextAction: null,
      lifecycleSupport: "managed", operationalState: "blocked", runtimeState: "not_active",
      sourceId: null, venueId: null, instrumentId: null, connection: null,
      marketFreshness: null, integrity: null, quality: null, coverageState: null,
      runtimeObservedAt: null, latestSetupSessionId: null, providerDatasetIdentifier: null,
      storedData: null, storedDataQuarantine: null,
      lifecycle: {
        provider: "alpaca.basic-market-data", state: "blocked", stateRevision: "3",
        configurationSessionId: "11111111-1111-4111-8111-111111111111",
        publicConfigurationSha256: "a".repeat(64), doctor: null,
        startEligibility: "reconciliation_required", blocker: "reconciliation",
        observedAt: "2026-09-30T14:30:00.000000000Z",
      },
    } satisfies SourceEvidence

    expect(lifecycleControls(source).find((control) => control.action === "retry")).toEqual({
      action: "retry", label: "Retry", destructive: false,
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "3",
        reason: "desktop-user-request",
      },
    })
    expect(lifecycleControls({ ...source, lifecycle: {
      ...source.lifecycle, configurationSessionId: null,
    } }).some((control) => control.action === "retry")).toBe(false)
    expect(lifecycleControls({ ...source, lifecycle: {
      ...source.lifecycle, publicConfigurationSha256: undefined,
    } }).some((control) => control.action === "retry")).toBe(false)
  })

  it("keeps provider plumbing behind Settings onboarding", async () => {
    const providerSentinel = "Privileged provider sentinel"
    const onboardingRequests: Parameters<SystemTransport["onboard"]>[0][] = []
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
          setup: [],
          credentialAccess: { enabled: true, rememberInKeychain: false, reauthenticateAfterSeconds: null, access: "locked", rememberedAccessAvailable: false, reauthenticateAtUnixSeconds: null },
          capabilities: {
            credentialImport: false,
            health: false,
            manifestEvidence: false,
            researchIngestion: false,
            status: false,
            coverage: false,
          },
        }
      }) as SystemTransport["onboard"],
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

    fireEvent.click(screen.getByRole("link", { name: "Settings" }))
    fireEvent.click(await screen.findByRole("link", { name: "Onboarding" }))
    expect(await screen.findByText(providerSentinel)).toBeTruthy()
    expect(onboardingRequests).toEqual([{ action: "bootstrap" }])
  })

  it("keeps installation evidence out of the ordinary workspace", async () => {
    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    expect(
      await screen.findByRole("heading", { name: "What needs your attention now?" }),
    ).toBeTruthy()
    expect(screen.queryByText("Installation verified")).toBeNull()
    expect(screen.queryByText("Not verified")).toBeNull()
    expect(screen.queryByText("No signed installation receipt was admitted.")).toBeNull()
  })
})
