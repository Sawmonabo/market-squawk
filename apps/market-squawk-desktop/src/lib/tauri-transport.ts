import { invoke, isTauri } from "@tauri-apps/api/core"

import {
  applicationResultSchema,
  desktopBootstrapSchema,
} from "@/lib/schemas"
import type {
  ApplicationRequest,
  ProductTransport,
  ProviderOnboardingRequest,
} from "@/lib/transport"

export function createProductTransport(): ProductTransport {
  if (!isTauri()) {
    return new UnavailableBrowserTransport()
  }
  return new TauriTransport()
}

class TauriTransport implements ProductTransport {
  async bootstrap(signal?: AbortSignal) {
    const value = await abortable(invoke("desktop_bootstrap"), signal)
    return desktopBootstrapSchema.parse(value)
  }

  async invoke(request: ApplicationRequest, signal?: AbortSignal) {
    const value = await abortable(
      invoke("application_invoke", {
        request: {
          operation: request.operation,
          arguments: request.arguments ?? {},
        },
      }),
      signal,
    )
    return applicationResultSchema.parse(value)
  }

  onboard(request: ProviderOnboardingRequest, signal?: AbortSignal) {
    return abortable(invoke("provider_onboarding", { request }), signal)
  }

  async openOfficialProviderPage(providerId: string) {
    await invoke("open_official_provider_page", { providerId })
  }
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

  onboard(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  openOfficialProviderPage(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }
}

function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) {
    return promise
  }
  if (signal.aborted) {
    return Promise.reject(new DOMException("Request cancelled", "AbortError"))
  }
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      signal.addEventListener(
        "abort",
        () => reject(new DOMException("Request cancelled", "AbortError")),
        { once: true },
      )
    }),
  ])
}

