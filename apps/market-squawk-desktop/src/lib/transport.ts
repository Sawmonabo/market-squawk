import type {
  ApplicationResult,
  DesktopBootstrap,
} from "@/lib/schemas"

export interface ApplicationRequest {
  operation: string
  arguments?: Record<string, unknown>
}

export type ProviderOnboardingRequest =
  | { action: "bootstrap" }
  | {
      action: "start"
      surfaceId: string
      organization?: string
      administrativeEmail?: string
    }
  | { action: "resume"; sessionId: string }
  | { action: "unlockFallback"; secret: string }
  | { action: "lockFallback" }
  | { action: "submitSecret"; sessionId: string; secret: string }
  | {
      action: "activate"
      sessionId: string
      request: Record<string, unknown>
    }
  | { action: "renew"; sessionId: string }
  | { action: "cleanup"; sessionId: string }
  | { action: "cancel"; sessionId: string }

export interface ProductTransport {
  bootstrap(signal?: AbortSignal): Promise<DesktopBootstrap>
  invoke(
    request: ApplicationRequest,
    signal?: AbortSignal,
  ): Promise<ApplicationResult>
  onboard(
    request: ProviderOnboardingRequest,
    signal?: AbortSignal,
  ): Promise<unknown>
  openOfficialProviderPage(providerId: string): Promise<void>
  openProtectedProviderSetup(providerId: string): Promise<void>
}
