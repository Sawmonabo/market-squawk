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
  | {
      query: "sourceStatus" | "sourceCoverage" | "sourceHealth"
      sourceIds?: string[]
    }
  | { query: "researchDatasets"; afterDataset?: string }
  | {
      query: "researchManifest" | "researchHistory" | "researchAlternativeData"
      dataset: string
    }
  | { query: "portfolioAccounts"; afterAccountId?: string }
  | {
      query:
        | "portfolioHoldings"
        | "portfolioTransactions"
        | "portfolioPerformance"
        | "portfolioExposure"
        | "portfolioRisk"
      accountId: string
    }
  | {
      query: "portfolioRevisions"
      accountId: string
      afterRevisionId?: string
    }
  | {
      query: "portfolioAttribution"
      accountId: string
      baselineRevisionId: string
    }
  | {
      query: "portfolioScenario"
      accountId: string
      scenario: Record<string, unknown>
    }
  | {
      query: "portfolioScenarioBatch"
      accountId: string
      scenarios: unknown[]
    }
  | {
      query: "portfolioRebalance"
      accountId: string
      proposal: Record<string, unknown>
    }
  | {
      query: "portfolioCandidateImpact"
      accountId: string
      candidate: Record<string, unknown>
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
  | { query: "modelMetadata"; modelId: string }
  | {
      query: "modelPrediction"
      modelId: string
      input: Record<string, unknown>
    }
  | { query: "forecast" | "forecastOutcomes"; vintageId: string }
  | { query: "decisionScreens"; limit: number }
  | { query: "decisionCandidates"; runId: string }
  | { query: "decisionDossier"; dossierId: string }
  | {
      query: "decisionTarget" | "decisionTargetStatus"
      targetId: string
      revision: number
    }
  | { query: "decisionTargets"; targetId: string }
  | {
      query:
        | "fairValueClassification"
        | "fairValueExplanation"
        | "fairValueEvidence"
      measurementId: string
    }
  | {
      query: "fairValueApprovalStatus"
      measurementId: string
      at: string
    }
  | {
      query: "fairValueAudit"
      after?: Record<string, unknown>
      limit: number
    }
  | { query: "fairValueMarketAccess"; assessmentId: string }
  | { query: "backtest"; runId: string }
  | {
      query: "analysisArtifact"
      artifactId: string
      sha256: string
      byteCount: number
      mediaType: "application/json" | "application/vnd.apache.parquet"
      offset: number
      maximumBytes: number
    }
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
  researchControl(
    request: ResearchControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  startBacktestFromFile(confirmed?: boolean): Promise<ApplicationResult | null>
  modelControl(
    request: ModelControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  fairValueControl(
    request: FairValueControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  paperControl(
    request: PaperControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
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

export type ResearchControlRequest = { action: "startExport"; dataset: string }

export type ModelControlRequest =
  | {
      action: "evaluate"
      modelId: string
      input: Record<string, unknown>
    }
  | {
      action: "startTraining"
      configTicketId: string
      authorityTicketId: string
    }

export type FairValueControlRequest = {
  action: "classify"
  measurementId: string
}

export type PaperControlRequest =
  | {
      action: "start"
      provider: "coinbase" | "coinbase-direct" | "kraken"
      providerSessionId?: string
      initialCash: string
      feeBasisPoints: number
    }
  | { action: "stop" | "triggerKillSwitch"; reason: string }
  | { action: "cancel"; orderId: string }
  | { action: "reconcile" }

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
