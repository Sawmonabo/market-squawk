import { invoke, isTauri } from "@tauri-apps/api/core"

import {
  applicationResultSchema,
  desktopBootstrapSchema,
  encryptedFileFallbackSchema,
  providerActivationSchema,
  providerBootstrapSchema,
  providerSessionSchema,
} from "@/lib/schemas"
import type {
  ApplicationRequest,
  ProductTransport,
  ProviderOnboardingRequest,
  ProviderOnboardingResult,
} from "@/lib/transport"

export function createProductTransport(): ProductTransport {
  if (!isTauri()) {
    return new UnavailableBrowserTransport()
  }
  return new TauriTransport()
}

class TauriTransport implements ProductTransport {
  async bootstrap() {
    const value = await invoke("desktop_bootstrap")
    return desktopBootstrapSchema.parse(value)
  }

  async invoke(request: ApplicationRequest) {
    const value = await invoke("application_invoke", {
      request: {
        operation: request.operation,
        arguments: request.arguments ?? {},
      },
    })
    return applicationResultSchema.parse(value)
  }

  async onboard<Request extends ProviderOnboardingRequest>(
    request: Request,
  ): Promise<ProviderOnboardingResult<Request>> {
    const confirmed = !["bootstrap", "resume"].includes(request.action)
    const value = await invoke("provider_onboarding", { request, confirmed })
    return parseProviderResult(request, value)
  }

  async openOfficialProviderPage(providerId: string) {
    await invoke("open_official_provider_page", { providerId })
  }

  async openProtectedProviderSetup(providerId: string) {
    await invoke("open_protected_provider_setup", { providerId })
  }
}

function parseProviderResult<Request extends ProviderOnboardingRequest>(
  request: Request,
  value: unknown,
): ProviderOnboardingResult<Request> {
  const parsed = (() => {
    switch (request.action) {
      case "bootstrap":
        return providerBootstrapSchema.parse(value)
      case "unlockFallback":
      case "lockFallback":
        return encryptedFileFallbackSchema.parse(value)
      case "activate":
        return providerActivationSchema.parse(value)
      default:
        return providerSessionSchema.parse(value)
    }
  })()
  return parsed as ProviderOnboardingResult<Request>
}

class UnavailableBrowserTransport implements ProductTransport {
  bootstrap(): Promise<never> {
    return Promise.reject(
      new Error(
        "Open this interface through the Market Squawk desktop application. Use the protected provider portal for browser fallback.",
      ),
    )
  }

  invoke(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  onboard<Request extends ProviderOnboardingRequest>(
    _request: Request,
  ): Promise<ProviderOnboardingResult<Request>> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  openOfficialProviderPage(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  openProtectedProviderSetup(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }
}
