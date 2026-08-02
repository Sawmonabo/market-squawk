import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import marketSquawkMarkSvg from "@/assets/market-squawk-mark.svg?raw"
import { CredentialField } from "@/components/setup/credential-field"
import type { DesktopBootstrap } from "@/lib/schemas"
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
  mcpClient: {
    program: "/Applications/Market Squawk.app/Contents/MacOS/market-squawk",
    arguments: [
      "--data-dir",
      "/Users/operator/Library/Application Support/com.market-squawk.desktop",
      "mcp",
      "serve",
    ],
    environment: {},
    requiresDesktopExit: true,
  },
  telemetryEnabled: false,
  encryptedFileFallback: "locked",
  providerProfiles: [],
  providerSessions: [],
  setupSteps: [
    {
      id: "system",
      label: "System",
      state: "complete",
      complete: true,
      detail: "The local application initialized.",
      blockingReason: null,
      recovery: null,
      action: null,
    },
    {
      id: "storage",
      label: "Storage",
      state: "complete",
      complete: true,
      detail: "The local workspace is ready.",
      blockingReason: null,
      recovery: null,
      action: null,
    },
    {
      id: "sources",
      label: "Sources",
      state: "action_required",
      complete: false,
      detail: "Connect a source.",
      blockingReason: "No source is active.",
      recovery: "Connect a source.",
      action: "configure_sources",
    },
    {
      id: "research",
      label: "Research",
      state: "action_required",
      complete: false,
      detail: "Configure research.",
      blockingReason: "Research is not ready.",
      recovery: "Configure research.",
      action: "configure_research",
    },
    {
      id: "portfolio",
      label: "Portfolio",
      state: "action_required",
      complete: false,
      detail: "Configure portfolio imports.",
      blockingReason: "Portfolio imports are not active.",
      recovery: "Configure portfolio imports.",
      action: "configure_portfolio",
    },
    {
      id: "paper",
      label: "Paper",
      state: "action_required",
      complete: false,
      detail: "Configure paper execution.",
      blockingReason: "The complete Paper services are unavailable.",
      recovery: "Restore the complete risk-controlled Paper services.",
      action: "configure_paper",
    },
    {
      id: "mcp",
      label: "MCP",
      state: "available",
      complete: true,
      detail: "The local MCP service is available.",
      blockingReason: null,
      recovery: null,
      action: "review_mcp",
    },
    {
      id: "review",
      label: "Review",
      state: "blocked",
      complete: false,
      detail: "Review setup.",
      blockingReason: "Required setup remains.",
      recovery: "Resolve blockers.",
      action: "review_status",
    },
  ],
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
    startBacktestFromFile: async () =>
      query({ query: "jobs", limit: 25 }),
    modelControl: async () =>
      query({ query: "jobs", limit: 25 }),
    fairValueControl: async () =>
      query({ query: "fairValueMeasurements" }),
    paperControl: async () =>
      query({ query: "paperStatus" }),
    jobControl: async (request) =>
      query({ query: "jobs", limit: "limit" in request ? request.limit : 25 }),
    sourceControl: async (_action, _request) =>
      query({ query: "sourceStatus" }),
    stageTrainingInput: async () => null,
    mcpStatus: async () => ({
      serviceReady: true,
      sharedEndpointReady: true,
      claudeCode: "registration_pending",
      codex: "registration_pending",
    }),
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

describe("Market Squawk desktop boundary", () => {
  it("uses accessible product navigation to explore real research and MCP state", async () => {
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      setupSteps: blockedBootstrap.setupSteps.map((step) =>
        step.id === "research"
          ? {
              ...step,
              state: "complete",
              complete: true,
              blockingReason: null,
              recovery: null,
            }
          : step,
      ),
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
    expect(navigation.querySelectorAll("a,button")).toHaveLength(16)
    const paperExecution = Array.from(
      navigation.querySelectorAll("button"),
    ).find((button) => button.textContent?.includes("Paper Execution"))
    expect(paperExecution?.getAttribute("aria-disabled")).toBe("true")
    expect(
      await within(navigation).findByRole("link", {
        name: "Backup & Recovery",
      }),
    ).toBeTruthy()
    expect(await screen.findByText("No analytical datasets yet")).toBeTruthy()
    expect(screen.queryByText("Operation arguments")).toBeNull()

    rendered.unmount()
    render(
      <MemoryRouter initialEntries={["/mcp"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(
      await screen.findByText(
        "/Applications/Market Squawk.app/Contents/MacOS/market-squawk",
      ),
    ).toBeTruthy()
    expect(screen.getByText("Desktop exit required")).toBeTruthy()
    expect(screen.getByText("Research.GetManifest")).toBeTruthy()
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
