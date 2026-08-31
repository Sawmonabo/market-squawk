import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import type { AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import { lookupResultSchema } from "@/features/lookup/schemas"
import type { MarketProductRow } from "@/features/markets/market-product"
import type { PortfolioPositionChoice } from "@/features/portfolio/portfolio-contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
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
      productResult(request.action === "list" ? { models: [] } : { activities: [] }),
    backtestProducts: async (request) => {
      if (request.action === "get") {
        throw new Error("No completed backtest is configured for this test.")
      }
      return productResult({ activities: [] })
    },
    analyticalController: async () => analyticalControllerStatus(),
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
    openProtectedProviderSetup: async () => undefined,
  }
  const product: ProductTransport = {
    query: bridge.query,
    modelProducts: bridge.modelProducts,
    backtestProducts: bridge.backtestProducts,
    datasetPreparation: bridge.datasetPreparation,
    backtestPreparation: bridge.backtestPreparation,
    forecastPreparation: bridge.forecastPreparation,
    researchExport: bridge.researchExport,
    paperControl: bridge.paperControl,
    manualPaper: bridge.manualPaper,
  }
  const system: SystemTransport = {
    bootstrap: bridge.bootstrap,
    bootstrapService: bridge.bootstrapService,
    installation: bridge.installation,
    systemQuery: bridge.systemQuery,
    analyticalController: bridge.analyticalController,
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
    openProtectedProviderSetup: bridge.openProtectedProviderSetup,
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
const marketInstrumentResult = marketResult(marketOverviewRow)

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
  })

  it("renders one provider-neutral market journey with current price and explicit selection", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopSystemBootstrap = {
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

    const renderedText = document.body.textContent ?? ""
    expect(renderedText).not.toMatch(/kraken|coinbase|websocket-v2/i)
    expect(renderedText).not.toContain(marketSelectionToken)
    expect(renderedText).not.toMatch(/\bticks?\b|\blots?\b/i)
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
      <MemoryRouter initialEntries={["/system/settings"]}>
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

  it("keeps provider plumbing behind Connections", async () => {
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

    fireEvent.click(screen.getByRole("link", { name: "Connections & Sources" }))
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
