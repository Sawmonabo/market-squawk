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
  applicationVersion: "0.2.0",
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
  paperModeEnabled: false,
  telemetryEnabled: false,
  encryptedFileFallback: "locked",
  providerProfiles: [],
  providerSessions: [],
  setupSteps: [
    {
      id: "system",
      label: "System",
      state: "blocked",
      complete: false,
      detail: "Verify the installed release.",
      blockingReason: "No signed installation receipt was admitted.",
      recovery: "Install a verified package.",
      action: "review_installation",
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
      blockingReason: "Paper mode is not active.",
      recovery: "Enable paper mode.",
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

    expect(
      await screen.findByRole("heading", { name: "Welcome to Market Squawk" }),
    ).toBeTruthy()
    const navigation = screen.getByRole("navigation", {
      name: "Market Squawk",
    })
    expect(navigation.querySelectorAll("a,button")).toHaveLength(15)
    expect(
      screen.getByRole("button", { name: /Paper Execution/ }),
    ).toBeTruthy()
    expect(screen.getByRole("link", { name: "Backup & Recovery" })).toBeTruthy()
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
