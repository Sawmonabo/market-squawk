import type {
  AnalyticalControllerRequest,
  AnalyticalControllerResponse,
} from "@/features/advanced/analytical-profile-contracts"
import type {
  ApplicationResult,
  DesktopBootstrap,
  DesktopEvent,
  DesktopEventSubscriptionReceipt,
  DesktopStartup,
  EncryptedFileFallback,
  InstallationControlResult,
  InputTicket,
  McpClientsStatus,
  ProviderActivation,
  ProviderBootstrap,
  ProviderSession,
} from "@/lib/schemas"

export type DesktopEventSubscriptionRequest = {
  runtime: DesktopBootstrap["runtime"]
  afterSequence: DesktopEvent["sequence"]
}

export type DesktopEventSubscription = {
  receipt: DesktopEventSubscriptionReceipt
  unsubscribe: () => Promise<void>
}

export type SetupGoal =
  | "everything_recommended"
  | "explore_public_markets"
  | "research_investments"
  | "manage_portfolio"
  | "build_and_evaluate_models"
  | "practice_paper_execution"
  | "use_claude_code"
  | "use_codex"

export type SetupStarterPlan =
  | "everything_recommended"
  | "public_markets"
  | "research"
  | "portfolio"
  | "models"
  | "paper_practice"
  | "ai_clients"

export type SetupPlanSelection = {
  goals: SetupGoal[]
  starterPlan: SetupStarterPlan
}

export type FredAlfredImmutableGeneration = {
  manifestVersion: string
  schema: {
    name: string
    version: number
    fingerprint: string
  }
  contentHash: string
}

export type DashboardQuery =
  | { query: "overview" }
  | {
      query: "macroDashboard"
      provider: "federal-reserve-board.data-download-program"
      release: "h15"
    }
  | { query: "fredAlfredLatestKnownStatus" }
  | {
      query: "fredAlfredLatestKnownRead"
      generation: FredAlfredImmutableGeneration
      knowledgeCutoff: string
      effectiveDateCutoff: string
    }
  | { query: "lookup"; text: string; categories?: string[] }
  | { query: "marketSnapshot" | "marketQuality" | "marketUnifiedFeed" }
  | { query: "marketUniverse"; text?: string }
  | {
      query:
        | "marketTrades"
        | "marketQuotes"
        | "marketBooks"
        | "marketComparisons"
      instrumentId: string
    }
  | {
      query: "sourceStatus" | "sourceCoverage" | "sourceHealth"
      sourceIds?: string[]
    }
  | { query: "researchDatasets"; afterDataset?: string }
  | {
      query: "researchManifest" | "researchHistory" | "researchAlternativeData"
      dataset: string
    }
  | {
      query: "researchSourceObjects"
      provider: string
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
      instrumentId: string
      proposedQuantity: string
      scenarioShock: string
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
      query: "latestValidForecast"
      instrumentId: string
      asOf: string
    }
  | {
      query: "modelPrediction"
      modelId: string
      input: Record<string, unknown>
    }
  | { query: "forecast" | "forecastOutcomes"; vintageId: string }
  | { query: "decisionScreens"; limit: number }
  | { query: "analysisFeatureDatasets"; dataset?: string; afterDataset?: string }
  | {
      query: "decisionScreenRuns"
      afterRunId?: string
      limit: number
    }
  | { query: "decisionCandidates"; runId: string }
  | {
      query: "decisionCandidateDossiers"
      candidateId: string
      afterDossierId?: string
      limit: number
    }
  | { query: "decisionDossier"; dossierId: string }
  | { query: "decisionDossierPreparation"; candidateId: string }
  | { query: "decisionInvestmentAnalysis"; analysisId: string }
  | {
      query: "decisionInvestmentAnalyses"
      afterAnalysisId?: string
      limit: number
    }
  | {
      query: "decisionRecommendationTrackRecord"
      profileId: string
      profileRevision: number
      profileDigest: string
      horizonNanos: string
      evaluatedAtUnixNanos: string
    }
  | { query: "decisionTargetPreparation"; dossierId: string }
  | {
      query: "decisionTarget" | "decisionTargetStatus"
      targetId: string
      revision: number
    }
  | { query: "decisionTargets"; targetId: string }
  | { query: "decisionTargetIndex"; afterTargetId?: string; limit: number }
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
      mediaType:
        | "application/json"
        | "application/vnd.apache.parquet"
        | "application/x-ndjson"
      offset: number
      maximumBytes: number
    }
  | { query: "jobs"; afterJobId?: string; limit: number }
  | { query: "operationRuntimeStatus" }
  | { query: "operationBackups"; afterBackupId?: string; limit: number }
  | { query: "operationBackup"; backupId: string }
  | { query: "operationBackupRetentionPreview"; keepLatest: number }
  | { query: "operationRestorePreview"; backupId: string }
  | {
      query: "operationWorkspaces"
      afterWorkspaceId?: string
      limit: number
    }
  | { query: "operationWorkspaceSwitchPreview"; workspaceId: string }
  | {
      query:
        | "operationUpdateStatus"
        | "operationUpdatePreview"
        | "operationProgramRollbackPreview"
        | "operationSettings"
    }
  | ({ query: "operationLogs" } & OperationLogFilter)
  | {
      query: "operationSettingsChangePreview"
      expectedRevision: string
      changes: OperationSettingValue[]
    }
  | {
      query: "operationSettingsRollbackPreview"
      expectedRevision: string
      targetRevision: string
    }
  | { query: "setupPlanStatus" }
  | {
      query: "setupPlanPreview"
      expectedRevision: string
      selection: SetupPlanSelection
    }

export type InstallationControlRequest =
  | { action: "status" }
  | { action: "update" }
  | { action: "repair" }
  | { action: "rollback" }
  | { action: "uninstall" }

export type DesktopServiceBootstrapRequest =
  | { action: "unlock_encrypted_fallback"; unlock: string }
  | { action: "retry_after_foreground_keyring" }

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
  bootstrap(): Promise<DesktopStartup>
  bootstrapService(request: DesktopServiceBootstrapRequest): Promise<void>
  reconnectService(
    expectedRuntime: DesktopBootstrap["runtime"],
  ): Promise<DesktopStartup>
  installation(
    request: InstallationControlRequest,
    confirmed?: boolean,
  ): Promise<InstallationControlResult>
  query(request: DashboardQuery): Promise<ApplicationResult>
  analyticalController(
    request: AnalyticalControllerRequest,
    confirmed?: boolean,
  ): Promise<AnalyticalControllerResponse>
  researchControl(
    request: ResearchControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  datasetPreparation(
    request: unknown,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  backtestPreparation(
    request: unknown,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  startBacktestFromFile(confirmed?: boolean): Promise<ApplicationResult | null>
  modelControl(
    request: ModelControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  forecastPreparation(
    request: unknown,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  decisionControl(
    request: DecisionControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  governanceQuery(request: GovernanceQueryRequest): Promise<ApplicationResult>
  governanceControl(
    request: GovernanceControlRequest,
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
  manualPaper(
    request: ManualPaperRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  jobControl(request: JobControlRequest, confirmed?: boolean): Promise<ApplicationResult>
  sourceControl(
    action: SourceLifecycleAction,
    request: SourceLifecycleRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  importProviderCredentialBundle(): Promise<unknown | null>
  operationsControl(
    request: OperationsControlRequest,
    confirmed?: boolean,
  ): Promise<ApplicationResult>
  stageTrainingInput(kind: TrainingInputKind): Promise<InputTicket | null>
  mcpClients(): Promise<McpClientsStatus>
  mcpClientControl(
    request: McpClientControlRequest,
    confirmed?: boolean,
  ): Promise<McpClientsStatus>
  subscribe(
    request: DesktopEventSubscriptionRequest,
    onEvent: (event: DesktopEvent) => void,
    onProtocolError: (error: Error) => void,
  ): Promise<DesktopEventSubscription>
  onboard<Request extends ProviderOnboardingRequest>(
    request: Request,
  ): Promise<ProviderOnboardingResult<Request>>
  openOfficialProviderPage(providerId: string): Promise<void>
  openProtectedProviderSetup(providerId: string): Promise<void>
}

export type OperationLogSeverity = "trace" | "debug" | "info" | "warn" | "error"

export type OperationLogDomain =
  | "application"
  | "source"
  | "market"
  | "research"
  | "portfolio"
  | "model"
  | "backtest"
  | "execution"
  | "risk"
  | "fair_value"
  | "mcp"
  | "lifecycle"

export interface OperationLogFilter {
  fromUnixNanos?: string
  throughUnixNanos?: string
  minimumSeverity?: OperationLogSeverity
  domain?: OperationLogDomain
  sourceId?: string
  jobId?: string
  correlationId?: string
  search?: string
  afterSequence?: string
  limit: number
}

export type OperationSettingValue =
  | { kind: "log_retention_days"; value: number }
  | { kind: "log_minimum_severity"; value: OperationLogSeverity }
  | { kind: "update_channel"; value: "stable" | "preview" }
  | { kind: "automatic_update_checks"; value: boolean }
  | { kind: "storage_soft_limit_bytes"; value: string }
  | { kind: "default_query_row_limit"; value: number }
  | { kind: "maximum_concurrent_jobs"; value: number }
  | { kind: "market_freshness_millis"; value: number }
  | { kind: "backup_retention_count"; value: number }

type PreviewReference = {
  previewId: string
  previewDigest: string
}

export type OperationsControlRequest =
  | { action: "checkForUpdates" }
  | ({ action: "exportLogs" } & OperationLogFilter)
  | { action: "startBackup" }
  | { action: "startBackupVerification"; backupId: string }
  | ({ action: "startBackupRetention" } & PreviewReference)
  | ({ action: "startRestore" } & PreviewReference)
  | ({ action: "startWorkspaceSwitch" } & PreviewReference)
  | ({ action: "startUpdate" } & PreviewReference)
  | ({ action: "startProgramRollback" } & PreviewReference)
  | ({ action: "applySettingsChange" } & PreviewReference)
  | ({ action: "rollbackSettings" } & PreviewReference)
  | {
      action: "applySetupPlan"
      previewId: string
      previewSha256: string
    }

export type TrainingInputKind = "configuration" | "model_authority"

export type McpClientControlRequest = {
  action:
    | "connect"
    | "reconnect"
    | "verify"
    | "repair"
    | "rotateCredential"
    | "revokeCredential"
    | "disconnect"
  client: "claude_code" | "codex"
}

export type ResearchControlRequest =
  | { action: "discoverSourceObjects"; provider: string; dataset: string }
  | {
      action: "startIngestSource"
      provider: string
      object: string
      dataset: string
      discoveryReceipt: string
    }
  | { action: "startExport"; dataset: string }

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

export type GovernanceQueryRequest =
  | { query: "provisioningStatus" }
  | {
      query: "principals"
      after?: string
      limit?: number
    }

export type GovernanceControlRequest =
  | {
      action: "provisionPrincipalSet"
      primaryDisplayName: string
      primaryCredential: string
      reviewerDisplayName: string
      reviewerCredential: string
    }
  | {
      action: "authenticateAction"
      previewId: string
      principalId: string
      credential: string
    }

export type DecisionControlRequest =
  | {
      action: "saveScreen"
      expectedRevision?: number
      screen: Record<string, unknown>
    }
  | {
      action: "runScreen"
      screenId: string
      screenRevision: number
      datasetManifest: Record<string, unknown>
      asOf: string
    }
  | { action: "prepareDossier"; draft: Record<string, unknown> }
  | { action: "createDossier"; receiptId: string }
  | { action: "prepareTargetSet"; draft: Record<string, unknown> }
  | { action: "createTargetSet" | "reevaluateTargetSet"; receiptId: string }
  | {
      action: "previewGovernanceAction"
      proposal:
        | {
            kind: "review"
            targetId: string
            targetRevision: number
            disposition: "activate" | "reject" | "needs_changes"
            note: string
          }
        | {
            kind: "invalidation"
            targetId: string
            targetRevision: number
            invalidationKind:
              | "corporate_action"
              | "model"
              | "data"
              | "reference_mark"
              | "assumption"
            note: string
          }
    }
  | {
      action: "commitGovernanceAction"
      previewId: string
      authorizationHandles: string[]
    }

export type FairValueGovernanceProposal =
  | {
      kind: "approve"
      measurementId: string
      decisionId: string
      expiresAt: string
    }
  | {
      kind: "override"
      measurementId: string
      decisionId: string
      requestedHierarchy: "level_2" | "level_3"
      justification: string
      expiresAt: string
    }
  | {
      kind: "revoke"
      approvalId: string
      reason: string
    }
  | {
      kind: "market_access"
      accountId: string
      venueId: string
      instrumentId: string
      conclusion: "accessible" | "inaccessible"
      effectiveFrom: string
      effectiveUntil: string
      rationale: string
    }

export type FairValueControlRequest =
  | { action: "measure"; measurement: Record<string, unknown> }
  | { action: "classify"; measurementId: string }
  | {
      action: "previewGovernanceAction"
      proposal: FairValueGovernanceProposal
    }
  | {
      action: "commitGovernanceAction"
      previewId: string
      authorizationHandles: string[]
    }

export type PaperControlRequest =
  | {
      action: "start"
      provider: "coinbase" | "coinbase-direct" | "kraken"
      providerSessionId?: string
      strategyMode: "manual" | "book_imbalance"
      initialCash: string
      feeBasisPoints: number
    }
  | { action: "stop" | "triggerKillSwitch"; reason: string }
  | { action: "cancel"; orderId: string }
  | { action: "reconcile" }

export type ManualPaperTargetLevel =
  | "downside"
  | "add"
  | "entry_lower"
  | "entry_upper"
  | "base"
  | "trim_lower"
  | "trim_upper"
  | "exit_lower"
  | "exit_upper"
  | "upside"

export type ManualPaperRequest =
  | { action: "targets" }
  | {
      action: "submit"
      targetId: string
      targetRevision: number
      side: "buy" | "sell"
      orderType: "market" | "limit" | "stop" | "stop_limit"
      quantityLots: string
      limitTargetLevel?: ManualPaperTargetLevel
      stopTargetLevel?: ManualPaperTargetLevel
      timeInForce:
        | "day"
        | "good_til_cancelled"
        | "immediate_or_cancel"
        | "fill_or_kill"
    }

export type JobControlRequest =
  | { action: "list"; afterJobId?: string; limit: number }
  | { action: "get"; jobId: string; generation: string }
  | {
      action: "watch"
      jobId: string
      generation: string
      afterSequence: string
      limit: number
    }
  | {
      action: "cancel" | "retry"
      jobId: string
      generation: string
      expectedSequence: string
    }
  | {
      action: "confirm"
      jobId: string
      generation: string
      expectedSequence: string
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
  expectedStateRevision: string
  expectedGeneration?: string
  expectedRuntimeGenerationSha256?: string
  onboardingSessionId?: string
  publicConfigurationSha256?: string
  reason?: string
}
