import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import { CredentialField } from "@/components/setup/credential-field"
import type { DesktopBootstrap } from "@/lib/schemas"
import type {
  ProductTransport,
  ProviderOnboardingRequest,
} from "@/lib/transport"

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
  operations: [],
}

function transport(
  bootstrap = blockedBootstrap,
  onboard: (request: ProviderOnboardingRequest) => Promise<unknown> = async () =>
    ({}),
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
    expect(navigation.querySelectorAll("a")).toHaveLength(15)
    expect(screen.getByRole("link", { name: "Paper Execution" })).toBeTruthy()
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
        onAccepted={() => {
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
