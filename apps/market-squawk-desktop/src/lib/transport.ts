import type {
  ApplicationResult,
  DesktopBootstrap,
  DesktopEvent,
  EncryptedFileFallback,
  InstallationControlResult,
  InputTicket,
  McpStatus,
  ProviderActivation,
  ProviderBootstrap,
  ProviderSession,
} from "@/lib/schemas"

export type DashboardQuery =
  | { query: "overview" }
  | { query: "lookup"; text: string; categories?: string[] }
  | { query: "marketSnapshot" | "marketQuality" }
  | { query: "sourceStatus" | "sourceCoverage" | "sourceHealth" }
  | { query: "researchDatasets"; afterDataset?: string }
  | { query: "portfolioAccounts"; afterAccountId?: string }
  | {
      query:
        | "portfolioHoldings"
        | "portfolioPerformance"
        | "portfolioExposure"
        | "portfolioRisk"
      accountId: string
    }
  | {
      query:
        | "modelBundles"
        | "forecasts"
        | "paperStatus"
        | "paperOrders"
        | "paperFills"
        | "fairValueMeasurements"
    }
  | { query: "backtests"; dataset?: string }
  | { query: "jobs"; afterJobId?: string; limit: number }

export type InstallationControlRequest =
  | { action: "status" }
  | { action: "update" }
  | { action: "repair" }
  | { action: "rollback" }
  | { action: "uninstall" }

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

export type ProviderOnboardingResult<
  Request extends ProviderOnboardingRequest,
> = Request extends { action: "bootstrap" }
  ? ProviderBootstrap
  : Request extends { action: "unlockFallback" | "lockFallback" }
    ? EncryptedFileFallback
    : Request extends { action: "activate" }
      ? ProviderActivation
      : ProviderSession

export interface ProductTransport {
  bootstrap(): Promise<DesktopBootstrap>
  installation(
    request: InstallationControlRequest,
  ): Promise<InstallationControlResult>
  query(request: DashboardQuery): Promise<ApplicationResult>
  jobControl(request: JobControlRequest, confirmed?: boolean): Promise<ApplicationResult>
  sourceControl(
    action: SourceLifecycleAction,
    request: SourceLifecycleRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  stageTrainingInput(kind: TrainingInputKind): Promise<InputTicket | null>
  mcpStatus(): Promise<McpStatus>
  subscribe(onEvent: (event: DesktopEvent) => void): Promise<() => void>
  onboard<Request extends ProviderOnboardingRequest>(
    request: Request,
  ): Promise<ProviderOnboardingResult<Request>>
  openOfficialProviderPage(providerId: string): Promise<void>
  openProtectedProviderSetup(providerId: string): Promise<void>
}

export type TrainingInputKind = "configuration" | "model_authority"

export type JobControlRequest =
  | { action: "list"; afterJobId?: string; limit: number }
  | { action: "get"; jobId: string }
  | {
      action: "watch"
      jobId: string
      generation: number
      afterSequence: number
      limit: number
    }
  | {
      action: "cancel" | "retry"
      jobId: string
      generation: number
      expectedSequence: number
    }
  | {
      action: "confirm"
      jobId: string
      generation: number
      expectedSequence: number
      identity: string
      digest: string
    }

export type SourceLifecycleAction =
  | "start"
  | "stop"
  | "retry"
  | "resynchronize"
  | "verify"
  | "reconfigure"
  | "remove"

export interface SourceLifecycleRequest {
  provider: string
  expectedStateRevision: number
  expectedGeneration?: number
  onboardingSessionId?: string
  publicConfigurationSha256?: string
  reason?: string
}
