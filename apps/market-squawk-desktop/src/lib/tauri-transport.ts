import { Channel, invoke, isTauri } from "@tauri-apps/api/core"

import {
  applicationResultSchema,
  desktopEventSchema,
  desktopStartupSchema,
  encryptedFileFallbackSchema,
  installationControlResultSchema,
  inputTicketSchema,
  mcpClientsStatusSchema,
  providerActivationSchema,
  providerBootstrapSchema,
  providerSessionSchema,
} from "@/lib/schemas"
import type {
  DashboardQuery,
  DecisionControlRequest,
  FairValueControlRequest,
  GovernanceControlRequest,
  GovernanceQueryRequest,
  JobControlRequest,
  InstallationControlRequest,
  ModelControlRequest,
  McpClientControlRequest,
  ManualPaperRequest,
  OperationsControlRequest,
  PaperControlRequest,
  ProductTransport,
  ProviderOnboardingRequest,
  ProviderOnboardingResult,
  ResearchControlRequest,
  SourceLifecycleAction,
  SourceLifecycleRequest,
  TrainingInputKind,
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
    return desktopStartupSchema.parse(value)
  }

  async bootstrapService(request: Parameters<ProductTransport["bootstrapService"]>[0]) {
    await invoke("desktop_service_bootstrap", { request })
  }

  async installation(request: InstallationControlRequest, confirmed = false) {
    const value = await invoke("installation_control", {
      request,
      confirmed,
    })
    return installationControlResultSchema.parse(value)
  }

  async query(request: DashboardQuery) {
    const value = await invoke("dashboard_query", { request })
    return applicationResultSchema.parse(value)
  }

  async researchControl(request: ResearchControlRequest, confirmed = false) {
    const value = await invoke("research_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async datasetPreparation(request: unknown, confirmed = false) {
    const value = await invoke("analysis_control", {
      request: mapPreparationAction(request, {
        options: "featureDatasetOptions",
        preview: "previewFeatureDataset",
        start: "startPreparedFeatureDataset",
      }),
      confirmed,
    })
    return applicationResultSchema.parse(value)
  }

  async backtestPreparation(request: unknown, confirmed = false) {
    const value = await invoke("analysis_control", {
      request: mapPreparationAction(request, {
        options: "backtestOptions",
        preview: "previewBacktest",
        start: "startPreparedBacktest",
      }),
      confirmed,
    })
    return applicationResultSchema.parse(value)
  }

  async startBacktestFromFile(confirmed = false) {
    const value = await invoke("start_backtest_from_file", { confirmed })
    return value === null ? null : applicationResultSchema.parse(value)
  }

  async modelControl(request: ModelControlRequest, confirmed = false) {
    const value = await invoke("model_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async forecastPreparation(request: unknown, confirmed = false) {
    const value = await invoke("model_control", {
      request: mapPreparationAction(request, {
        options: "forecastPreparationOptions",
        preview: "prepareForecast",
        start: "startPreparedForecast",
      }),
      confirmed,
    })
    return applicationResultSchema.parse(value)
  }

  async decisionControl(request: DecisionControlRequest, confirmed = false) {
    const value = await invoke("decision_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async governanceQuery(request: GovernanceQueryRequest) {
    const value = await invoke("governance_query", { request })
    return applicationResultSchema.parse(value)
  }

  async governanceControl(request: GovernanceControlRequest, confirmed = false) {
    const value = await invoke("governance_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async fairValueControl(request: FairValueControlRequest, confirmed = false) {
    const value = await invoke("fair_value_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async paperControl(request: PaperControlRequest, confirmed = false) {
    const value = await invoke("paper_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async manualPaper(request: ManualPaperRequest, confirmed = false) {
    const value = await invoke("paper_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async jobControl(request: JobControlRequest, confirmed = false) {
    const value = await invoke("job_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async sourceControl(
    action: SourceLifecycleAction,
    request: SourceLifecycleRequest,
    confirmed = false,
  ) {
    const value = await invoke("source_control", { action, request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async operationsControl(request: OperationsControlRequest, confirmed = false) {
    const value = await invoke("operations_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async stageTrainingInput(kind: TrainingInputKind) {
    const value = await invoke("stage_training_input", { kind })
    return value === null ? null : inputTicketSchema.parse(value)
  }

  async mcpClients() {
    const value = await invoke("mcp_status")
    return mcpClientsStatusSchema.parse(value)
  }

  async mcpClientControl(request: McpClientControlRequest, confirmed = false) {
    const value = await invoke("mcp_client_control", { request, confirmed })
    return mcpClientsStatusSchema.parse(value)
  }

  async subscribe(onEvent: Parameters<ProductTransport["subscribe"]>[0]) {
    let active = true
    const channel = new Channel<unknown>((value) => {
      if (active) onEvent(desktopEventSchema.parse(value))
    })
    await invoke("subscribe_service_events", { onEvent: channel })
    return () => {
      active = false
    }
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

function mapPreparationAction(
  request: unknown,
  operations: Readonly<Record<"options" | "preview" | "start", string>>,
): Record<string, unknown> {
  if (
    typeof request !== "object" ||
    request === null ||
    Array.isArray(request) ||
    !("action" in request)
  ) {
    throw new Error("The guided preparation request is invalid.")
  }
  const action = request.action
  if (action !== "options" && action !== "preview" && action !== "start") {
    throw new Error("The guided preparation action is unsupported.")
  }
  return { ...request, action: operations[action] }
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

  bootstrapService(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  installation(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  query(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  researchControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  datasetPreparation(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  backtestPreparation(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  startBacktestFromFile(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  modelControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  forecastPreparation(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  decisionControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  governanceQuery(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  governanceControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  fairValueControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  paperControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  manualPaper(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  jobControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  sourceControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  operationsControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  stageTrainingInput(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  mcpClients(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  mcpClientControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  subscribe(): Promise<never> {
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
