import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import { CredentialField } from "@/components/setup/credential-field"
import type { DesktopBootstrap } from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

const blockedBootstrap: DesktopBootstrap = {
  contractVersion: "market-squawk-desktop-v1",
  applicationVersion: "1.0.0",
  buildProfile: "development",
  platform: "macos",
  dataRoot: ".market-squawk",
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
    invoke: async () => ({
      data: null,
      metadata: {
        completeness: "complete",
        returnedItems: 0,
        availableItems: 0,
        sourceCoverage: { status: "not_applicable" },
        dataQuality: { status: "not_applicable" },
      },
    }),
    onboard,
    openOfficialProviderPage: async () => undefined,
    openProtectedProviderSetup: async () => undefined,
  }
}

describe("Market Squawk desktop boundary", () => {
  it("exposes the permanent product navigation with accessible labels", async () => {
    render(
      <MemoryRouter initialEntries={["/overview"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    const welcome = await screen.findByText("Welcome to Market Squawk")
    expect(welcome.closest("h1,h2,h3,h4,h5,h6")).toBeTruthy()
    const navigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(navigation).toBeTruthy()
    if (!navigation) {
      throw new Error("Market Squawk navigation is absent")
    }
    expect(navigation.querySelectorAll("a,button")).toHaveLength(15)
    const paperExecution = Array.from(
      navigation.querySelectorAll("button"),
    ).find((button) => button.textContent?.includes("Paper Execution"))
    expect(paperExecution?.getAttribute("aria-disabled")).toBe("true")
    const backup = navigation.querySelector('a[href="/backup-recovery"]')
    expect(backup?.textContent).toContain("Backup & Recovery")
  })

  it("never promotes an unverified backend state to installation readiness", async () => {
    render(
      <MemoryRouter initialEntries={["/overview"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    expect((await screen.findAllByText("Not verified")).length).toBeGreaterThan(0)
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
