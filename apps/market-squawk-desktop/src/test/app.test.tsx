import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import marketSquawkMarkSvg from "@/assets/market-squawk-mark.svg?raw"
import { CredentialField } from "@/components/setup/credential-field"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import type { ApplicationResult, DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

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
  encryptedFileFallback: "locked",
  providerProfiles: [],
  providerSessions: [],
  operations: [],
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
  return {
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
    researchControl: async () =>
      query({ query: "researchDatasets" }),
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
      query({ query: "fairValueMeasurements" }),
    paperControl: async () =>
      query({ query: "paperStatus" }),
    manualPaper: async () =>
      query({ query: "paperStatus" }),
    jobControl: async (request) =>
      query({ query: "jobs", limit: "limit" in request ? request.limit : 25 }),
    sourceControl: async (_action, _request) =>
      query({ query: "sourceStatus" }),
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
    subscribe: async () => () => undefined,
    onboard,
    openOfficialProviderPage: async () => undefined,
    openProtectedProviderSetup: async () => undefined,
  }
}

function datasetRead(
  name: string,
  domain: string,
  description: string,
): DesktopBootstrap["operations"][number] {
  return {
    name,
    description,
    domain,
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        dataset: { type: "string" },
        resultLimits: { type: "object" },
      },
      required: ["dataset", "resultLimits"],
    },
  }
}

const unifiedKrakenMarket: ApplicationResult = {
  data: [
    {
      instrumentId: "7e8299e7-9757-4441-926f-d0b22c767a65",
      symbol: "BTC/USD",
      symbolKind: "provider_subscription",
      symbolVenueId: "kraken",
      assetClass: "crypto",
      quoteCurrency: "USD",
      definitionKind: "execution_capable",
      definitionRevision: 3,
      referenceRevision: null,
      permanentFigi: null,
      displayName: "Bitcoin",
      tickSize: "0.1",
      lotSize: "0.00000001",
      executionTermsAvailable: true,
      referenceEvidence: null,
      availability: "Live",
      confidence: "Direct unverified",
      quote: {
        bidPrice: "68000.1",
        bidPriceProviderLexeme: "68000.1",
        bidSize: "0.25",
        bidSizeProviderLexeme: "0.25000000",
        askPrice: "68000.2",
        askPriceProviderLexeme: "68000.2",
        askSize: "0.3",
        askSizeProviderLexeme: "0.30000000",
        midPrice: "68000.15",
        midPriceBasis: "best_bid_ask",
        lastPrice: "68000.2",
        lastPriceProviderLexeme: "68000.2",
        lastSize: "0.02",
        lastSizeProviderLexeme: "0.02000000",
        lastSourceTimestamp: "2026-08-09T14:30:00Z",
        lastReceivedAt: "2026-08-09T14:30:00.010Z",
        lastAvailableAt: "2026-08-09T14:30:00.011Z",
        lastQuality: "direct_unverified",
        lastFreshAtSelection: true,
        quoteEvidence: { surfaceId: "kraken-l3" },
        tradeEvidence: null,
      },
      orderBook: {
        depth: "order_level",
        revision: "17",
        phase: "healthy",
        quarantineReason: null,
        quality: "direct_unverified",
        freshness: "fresh",
        lastMarketAt: "2026-08-09T14:30:00Z",
        usableForSelection: true,
        totalOrderCount: 2,
        returnedOrderCount: 2,
        sampleTruncated: false,
        samplePolicy: "stable_provider_order_id_prefix",
        orders: [
          {
            orderId: "kraken-bid-1",
            side: "bid",
            price: "68000.1",
            priceTicks: "680001",
            quantity: "0.25",
            quantityLots: "25000000",
            providerOrderTimestamp: "2026-08-09T14:30:00Z",
            providerPriority: null,
            firstSeenIn: "snapshot",
            lastUpdatedIn: "snapshot",
            lastSourceTimestamp: "2026-08-09T14:30:00Z",
            lastReceivedAt: "2026-08-09T14:30:00.010Z",
            arrivalOrdinal: "1",
          },
          {
            orderId: "kraken-ask-1",
            side: "ask",
            price: "68000.2",
            priceTicks: "680002",
            quantity: "0.3",
            quantityLots: "30000000",
            providerOrderTimestamp: "2026-08-09T14:30:00Z",
            providerPriority: null,
            firstSeenIn: "snapshot",
            lastUpdatedIn: "snapshot",
            lastSourceTimestamp: "2026-08-09T14:30:00Z",
            lastReceivedAt: "2026-08-09T14:30:00.010Z",
            arrivalOrdinal: "2",
          },
        ],
      },
      selectedSource: {
        surfaceId: "kraken-l3",
        providerId: "kraken",
        providerSymbol: "BTC/USD",
        sourceId: "kraken-l3-account",
        venueId: "kraken",
        providerProduct: "websocket-v2",
        providerChannel: "level3",
        timing: "live",
        depth: "order_level",
        depthLabel: "Order-level book",
        quality: "direct_unverified",
        coverage: "single_venue",
        health: "healthy",
        freshness: {
          receivedAt: "2026-08-09T14:30:00.010Z",
          availableAt: "2026-08-09T14:30:00.011Z",
          sourceValidUntil: "2026-08-09T14:30:05Z",
          freshAtSelection: true,
        },
        integrity: {
          state: "checksum_valid",
          phase: "healthy",
          generationCurrent: true,
          snapshotInitialized: true,
        },
      },
      alternatives: [],
      selectionReceipt: { policyRevision: 1 },
    },
  ],
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: { status: "complete", providers: ["kraken"] },
    dataQuality: { status: "direct_unverified" },
  },
}

describe("Market Squawk desktop boundary", () => {
  it("keeps an instrument lookup bound to its exact Markets context", () => {
    const instrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
    expect(
      lookupRoute({
        category: "instrument",
        id: instrumentId,
        label: "MSQ · nasdaq",
        detail: {},
        destination: { kind: "market_instrument", instrumentId },
      }),
    ).toBe(`/markets?instrumentId=${instrumentId}`)
  })

  it("renders one unified market with automatic source confidence and order-level detail", async () => {
    const user = userEvent.setup()
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Market.GetUnifiedFeed",
          "market",
          "Return the bounded unified market view.",
        ),
      ],
    }
    render(
      <MemoryRouter initialEntries={["/markets"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            if (request.query === "marketUnifiedFeed") return unifiedKrakenMarket
            throw new Error(`Unexpected market query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    const marketHeading = await screen.findByRole("heading", { name: "BTC/USD" })
    const marketCard = marketHeading.closest("button")
    expect(marketCard).toBeInstanceOf(HTMLButtonElement)
    if (!(marketCard instanceof HTMLButtonElement)) {
      throw new Error("Unified market card is absent")
    }
    expect(within(marketCard).getByText("Bitcoin")).toBeTruthy()
    expect(within(marketCard).getByText("Live")).toBeTruthy()
    expect(within(marketCard).getByText("Order-level book")).toBeTruthy()
    expect(within(marketCard).getByText("2 of 2")).toBeTruthy()
    expect(screen.queryByRole("combobox", { name: /provider/i })).toBeNull()

    await user.click(
      screen.getByText("Show detailed trades, quotes, order book, and source comparison"),
    )
    expect(await screen.findByText("Chosen automatically")).toBeTruthy()
    expect(screen.getByText("Orders behind the visible market")).toBeTruthy()
    await user.click(screen.getByText("Data confidence"))
    expect(screen.getAllByText("kraken").length).toBeGreaterThan(0)
    expect(screen.getByText("Checksum valid")).toBeTruthy()
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
      <MemoryRouter initialEntries={["/overview"]}>
        <App transport={bootstrapTransport} />
      </MemoryRouter>,
    )

    const field = await screen.findByLabelText("Local security password")
    await user.type(field, "process-local-test-unlock")
    await user.click(screen.getByRole("button", { name: "Unlock secure storage" }))

    expect((field as HTMLInputElement).value).toBe("")
    expect(submittedUnlock).toBe("process-local-test-unlock")
    expect(
      await screen.findByRole("heading", { name: "Welcome to Market Squawk" }),
    ).toBeTruthy()
  })

  it("uses accessible product navigation to explore real research and MCP state", async () => {
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Research.ListDatasets",
          "research",
          "Return the bounded local research dataset inventory.",
        ),
        datasetRead(
          "Research.GetManifest",
          "research",
          "Return one immutable analytical dataset manifest.",
        ),
        datasetRead(
          "Fundamental.GetFacts",
          "fundamental",
          "Return bounded reported fundamental facts.",
        ),
        datasetRead(
          "Macro.GetRevisions",
          "macro",
          "Return bounded macroeconomic revision history.",
        ),
      ],
    }
    const rendered = render(
      <MemoryRouter initialEntries={["/research"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )

    const heading = await screen.findByRole("heading", { name: "Research" })
    expect(heading.tagName).toBe("H1")
    const workspaceHome = screen.getByRole("link", {
      name: "Market Squawk workspace",
    })
    expect({
      centered: workspaceHome.classList.contains("justify-center"),
      leftAligned: workspaceHome.classList.contains("justify-start"),
    }).toEqual({ centered: true, leftAligned: false })
    const marketWord = within(workspaceHome).getByText("Market")
    const quawkWord = within(workspaceHome).getByText("quawk")
    const marketSquawkMark = workspaceHome.querySelector('img[alt=""]')
    expect(marketSquawkMark).toBeInstanceOf(HTMLImageElement)
    if (!(marketSquawkMark instanceof HTMLImageElement)) {
      throw new Error("Market Squawk mark is absent")
    }
    expect({
      marketText: marketWord.classList.contains("text-[18px]"),
      markHeight: marketSquawkMark.classList.contains("h-[21px]"),
      squawkText: quawkWord.classList.contains("text-[18px]"),
    }).toEqual({ marketText: true, markHeight: true, squawkText: true })
    expect(quawkWord.style.marginLeft).toBe("-1px")
    const markDocument = new DOMParser().parseFromString(
      marketSquawkMarkSvg,
      "image/svg+xml",
    )
    const whiteBird = markDocument.querySelector('path[fill="#fafafa"]')
    const whiteBirdPath = whiteBird?.getAttribute("d") ?? ""
    expect({
      fillRule: whiteBird?.getAttribute("fill-rule") ?? null,
      contours: whiteBirdPath.match(/[Mm]/g)?.length ?? 0,
    }).toEqual({ fillRule: null, contours: 1 })
    const navigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(navigation).toBeTruthy()
    if (!(navigation instanceof HTMLElement)) {
      throw new Error("Market Squawk navigation is absent")
    }
    expect(navigation.querySelectorAll("a,button")).toHaveLength(18)
    const paperExecution = await within(navigation).findByRole("link", {
      name: "Paper Execution",
    })
    expect(paperExecution.getAttribute("aria-disabled")).toBeNull()
    expect(
      await within(navigation).findByRole("link", {
        name: "Backup & Recovery",
      }),
    ).toBeTruthy()
    expect(await screen.findByText("No analytical datasets yet")).toBeTruthy()
    expect(screen.queryByText("Operation arguments")).toBeNull()

    rendered.unmount()
    const mcpRendered = render(
      <MemoryRouter initialEntries={["/mcp"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(await screen.findByText("One authenticated local endpoint")).toBeTruthy()
    expect(screen.getByText("Claude Code and Codex")).toBeTruthy()
    expect(screen.getByText("stateless request sessions", { exact: false })).toBeTruthy()

    mcpRendered.unmount()
    render(
      <MemoryRouter initialEntries={["/updates"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(
      await screen.findByRole("heading", { name: "Updates & program recovery" }),
    ).toBeTruthy()
  })

  it("never promotes an unverified backend state to installation readiness", async () => {
    render(
      <MemoryRouter initialEntries={["/overview"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    expect((await screen.findAllByText("Not verified")).length).toBeGreaterThan(0)
    const welcomeHeading = screen.getByRole("heading", {
      name: "Welcome to Market Squawk",
    })
    const whiteHeadingText = within(welcomeHeading).getByText(
      "Welcome to Market",
      { exact: true },
    )
    const cobaltHeadingText = within(welcomeHeading).getByText("Squawk", {
      exact: true,
    })
    expect(whiteHeadingText.classList.contains("text-white")).toBe(true)
    expect(cobaltHeadingText.classList.contains("text-primary")).toBe(true)
    expect(cobaltHeadingText.querySelector("img,svg")).toBeNull()
    expect(screen.queryByText("Installation verified")).toBeNull()
    expect(screen.getByText("No signed installation receipt was admitted.")).toBeTruthy()
  })

  it("clears rejected credential text and does not advance setup", async () => {
    let accepted = false
    const user = userEvent.setup()
    render(
      <CredentialField
        providerName="FRED and ALFRED"
        sessionId="5d67c2c6-4d02-43c4-b42a-d7762cb61bdb"
        transport={transport(blockedBootstrap, async () => {
          throw new Error("credential rejected")
        })}
        onAccepted={async () => {
          accepted = true
        }}
      />,
    )

    const field = screen.getByLabelText("Provider API key")
    await user.type(field, "not-a-real-key")
    await user.click(screen.getByRole("button", { name: "Save and verify" }))

    expect(await screen.findByRole("alert")).toBeTruthy()
    expect((field as HTMLInputElement).value).toBe("")
    expect(accepted).toBe(false)
  })
})
