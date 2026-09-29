import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it, vi } from "vitest"

import { App } from "@/app/app"
import type { AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import { lookupResultSchema } from "@/features/lookup/schemas"
import type { MarketProductRow } from "@/features/markets/market-product"
import { parseInvestmentAnalysis, type InvestmentAnalysis } from "@/features/opportunities/contracts"
import type { PortfolioPositionChoice } from "@/features/portfolio/portfolio-contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
import type { PortfolioRiskReport } from "@/features/risk/contracts"
import {
  type ApplicationResult,
  type DesktopSystemBootstrap,
  type NativeEvidenceApplicationResult,
} from "@/lib/schemas"
import {
  productLookupActions,
  productLookupCategory,
  type DesktopTransport,
  type ProductTransport,
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
    subscribe: async (request) => ({
      receipt: {
        subscriptionId: "f49e02f6-8c47-43a5-bb33-030e8e0d12bb",
        productSessionToken: request.productSessionToken,
        sequence: request.afterSequence,
        resumed: request.afterSequence !== "0",
      },
      unsubscribe: async () => undefined,
    }),
    onboard,
    openOfficialProviderPage: async () => undefined,
  }
  const product: ProductTransport = {
    query: bridge.query,
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
  changePercent: "1.25",
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

const portfolioPositionChoice: PortfolioPositionChoice = {
  actionToken: "position_choice_add_three_shares",
  title: "Add three shares",
  action: "Review adding three shares",
  horizon: "Next 30 days",
  range: "Two to three shares",
  reasons: ["The position remains within the prepared concentration range."],
  risks: ["The investment may fall before the review expires."],
  assumptions: ["The available cash balance remains unchanged."],
  expiresAt: "2026-09-01T14:30:00Z",
  invalidators: ["The prepared risk review changes."],
  uncertainty: "Price and portfolio conditions may change before action.",
  investment: {
    name: "Example Company",
    symbol: "EXM",
    typeLabel: "Stock",
  },
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

    expect(lookupRoute(parsed.matches[0]!)).toBe(`/markets?selectionToken=${marketSelectionToken}`)
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

    const requestedRow: MarketProductRow = {
      ...marketOverviewRow,
      identity: { symbol: "MSQ", name: "Requested investment", assetClass: "equity" },
    }
    const openInvestment = (route: string) => render(
      <MemoryRouter initialEntries={[route]}>
        <App transport={transport(
          { ...blockedBootstrap, capabilities: ["market_overview", "market_instrument"] },
          undefined,
          async (request) => {
            issuedQueries.push(request)
            if (request.query === "marketOverview") return marketOverviewResult
            if (request.query === "marketInstrument" && request.selectionToken === marketSelectionToken) {
              return marketResult(requestedRow)
            }
            throw new Error("This investment selection is no longer available.")
          },
        )} />
      </MemoryRouter>,
    )
    const investment = openInvestment(lookupRoute(parsed.matches[0]!))
    expect(await screen.findByRole("heading", { name: "Requested investment" })).toBeTruthy()
    expect(issuedQueries).toContainEqual({ query: "marketInstrument", selectionToken: marketSelectionToken })
    investment.unmount()

    const staleToken = "market_ffffffffffffffffffffffffffffffff"
    openInvestment(`/markets?selectionToken=${staleToken}`)
    expect((await screen.findByRole("alert")).textContent).toContain("This investment could not be opened")
    expect(issuedQueries).toContainEqual({ query: "marketInstrument", selectionToken: staleToken })
    expect(screen.queryByRole("heading", { name: "Requested investment" })).toBeNull()
    // The overview still contains Bitcoin, but a rejected route must not select it as fallback.
    expect(screen.getAllByRole("heading", { name: "Bitcoin" })).toHaveLength(1)
  })

  it("renders one provider-neutral market journey with current price and explicit selection", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
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
    const readyBootstrap: DesktopSystemBootstrap = {
      ...blockedBootstrap,
      capabilities: ["market_overview", "market_instrument"],
    }
    render(
      <MemoryRouter initialEntries={["/markets"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request, options) => {
            issuedQueries.push(request)
            if (request.query === "marketOverview") return marketOverviewResult
            if (request.query === "marketInstrument") return marketResult({ ...marketOverviewRow, historyToken })
            if (request.query === "marketHistory") {
              if (request.startDate !== undefined) {
                viewportSignal = options?.signal
                return new Promise<ApplicationResult>((resolve) => { resolveViewport = resolve })
              }
              return historyResult
            }
            throw new Error(`Unexpected market query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

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

    await user.click(marketCard)
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "marketInstrument"),
      ).toEqual([
        { query: "marketInstrument", selectionToken: marketSelectionToken },
      ])
    })
    expect(screen.getAllByRole("heading", { name: "Bitcoin" })).toHaveLength(2)
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

    expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(0)
    const historyToggle = screen.getByText("Open price history")
    await user.click(historyToggle)
    await screen.findByLabelText("History window")
    expect(issuedQueries.filter((request) => request.query === "marketHistory")).toEqual([
      { query: "marketHistory", historyToken, pointLimit: 512 },
    ])
    expect(screen.getAllByText("68001.123456789 USD").length).toBeGreaterThan(0)
    await user.selectOptions(screen.getByLabelText("History window"), "30")
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toEqual([
      { query: "marketHistory", historyToken, pointLimit: 512 },
      { query: "marketHistory", historyToken, startDate: "2026-07-09", endDate: "2026-08-08", pointLimit: 512, generationToken },
    ]))
    expect(viewportSignal?.aborted).toBe(false)
    await user.click(historyToggle)
    await waitFor(() => expect(viewportSignal?.aborted).toBe(true))
    expect(screen.queryByLabelText("History window")).toBeNull()
    // A late cancelled response cannot repopulate a closed panel or pin a new
    // reader to the old window. Reopening starts from unpinned saved history.
    resolveViewport?.(historyResult)
    await user.click(historyToggle)
    await screen.findByLabelText("History window")
    expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({ query: "marketHistory", historyToken, pointLimit: 512 })
    await user.click(screen.getByRole("button", { name: "Refresh saved history" }))
    await waitFor(() => expect(issuedQueries.filter((request) => request.query === "marketHistory")).toHaveLength(4))
    expect(issuedQueries.filter((request) => request.query === "marketHistory").at(-1)).toEqual({ query: "marketHistory", historyToken, pointLimit: 512 })

    const renderedText = document.body.textContent ?? ""
    expect(renderedText).not.toMatch(/kraken|coinbase|websocket-v2/i)
    expect(renderedText).not.toContain(marketSelectionToken)
    expect(renderedText).not.toMatch(/\bticks?\b|\blots?\b/i)
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
    expect(within(macroSection).getByText("13 of 13 available")).toBeTruthy()
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
    const exposureReads: { account: string; cursor?: string }[] = []
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
        <App transport={transport({ ...blockedBootstrap, capabilities: ["portfolio_account_list", "portfolio_risk", "portfolio_performance", "portfolio_holdings", "portfolio_exposure"] }, undefined, async (request, options) => {
          if (request.query === "portfolioAccounts") return result({ accounts, nextCursor: null }, 2)
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
  })

  it("keeps portfolio planning explicit and analysis-only", async () => {
    const user = userEvent.setup()
    render(
      <PortfolioPlanning
        positionChoices={[portfolioPositionChoice]}
        rebalanceChoices={null}
      />,
    )

    const choice = screen.getByLabelText("Position choice")
    expect(choice).toBeInstanceOf(HTMLSelectElement)
    if (!(choice instanceof HTMLSelectElement)) {
      throw new Error("The position choice control is absent")
    }
    expect(choice.value).toBe("")
    expect(screen.queryByText("Review adding three shares")).toBeNull()

    await user.selectOptions(choice, portfolioPositionChoice.actionToken)

    expect(screen.getByText("Review adding three shares")).toBeTruthy()
    expect(screen.getByText("Next 30 days")).toBeTruthy()
    expect(screen.getByText("Two to three shares")).toBeTruthy()
    expect(
      screen.getByText(
        /Planning cannot place an order, and no choice is selected automatically\./,
      ),
    ).toBeTruthy()
    expect(
      screen.getByText(
        "No complete rebalance choices are available. Market Squawk will not assume allocation targets, turnover, cash, costs, or concentration limits.",
      ),
    ).toBeTruthy()
  })

  it("keeps fallback bootstrap native and enters the ready workspace only after reconnect", async () => {
    const user = userEvent.setup()
    let ready = false
    let submittedUnlock: string | null = null
    const baseTransport = transport()
    const bootstrapTransport = {
      product: baseTransport.product,
      system: {
        ...baseTransport.system,
        bootstrap: async () =>
          ready
            ? blockedBootstrap
            : {
                status: "bootstrap_required" as const,
                requirement: "encrypted_fallback_locked" as const,
              },
        bootstrapService: async (request) => {
          if (request.action !== "unlock_encrypted_fallback") {
            throw new Error("Expected the encrypted fallback unlock request.")
          }
          submittedUnlock = request.unlock
          ready = true
        },
      },
    } satisfies DesktopTransport

    render(
      <MemoryRouter initialEntries={["/system/settings/onboarding"]}>
        <App transport={bootstrapTransport} />
      </MemoryRouter>,
    )

    const field = await screen.findByLabelText("Local security password")
    await user.type(field, "process-local-test-unlock")
    await user.click(screen.getByRole("button", { name: "Unlock secure storage" }))

    expect((field as HTMLInputElement).value).toBe("")
    expect(submittedUnlock).toBe("process-local-test-unlock")
    await waitFor(() => {
      expect(screen.queryByLabelText("Local security password")).toBeNull()
    })
    expect(screen.getByRole("heading", { name: "Settings" })).toBeTruthy()
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
