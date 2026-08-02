import { Channel, invoke, isTauri } from "@tauri-apps/api/core"

import {
  applicationResultSchema,
  desktopEventSchema,
  desktopBootstrapSchema,
  encryptedFileFallbackSchema,
  installationControlResultSchema,
  inputTicketSchema,
  mcpStatusSchema,
  providerActivationSchema,
  providerBootstrapSchema,
  providerSessionSchema,
} from "@/lib/schemas"
import type {
  DashboardQuery,
  DecisionControlRequest,
  FairValueControlRequest,
  JobControlRequest,
  InstallationControlRequest,
  ModelControlRequest,
  PaperControlRequest,
  PortfolioControlRequest,
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
    return desktopBootstrapSchema.parse(value)
  }

  async installation(request: InstallationControlRequest) {
    const value = await invoke("installation_control", {
      request,
      confirmed: request.action !== "status",
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

  async startBacktestFromFile(confirmed = false) {
    const value = await invoke("start_backtest_from_file", { confirmed })
    return value === null ? null : applicationResultSchema.parse(value)
  }

  async decisionControl(request: DecisionControlRequest, confirmed = false) {
    const value = await invoke("decision_control", { request, confirmed })
    return applicationResultSchema.parse(value)
  }

  async modelControl(request: ModelControlRequest, confirmed = false) {
    const value = await invoke("model_control", { request, confirmed })
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

  async portfolioControl(request: PortfolioControlRequest, confirmed = false) {
    const value = await invoke("portfolio_control", { request, confirmed })
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

  async stageTrainingInput(kind: TrainingInputKind) {
    const value = await invoke("stage_training_input", { kind })
    return value === null ? null : inputTicketSchema.parse(value)
  }

  async mcpStatus() {
    const value = await invoke("mcp_status")
    return mcpStatusSchema.parse(value)
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

  installation(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  query(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  researchControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  startBacktestFromFile(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  decisionControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  modelControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  fairValueControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  paperControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  portfolioControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  jobControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  sourceControl(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  stageTrainingInput(): Promise<never> {
    return Promise.reject(new Error("The local application is not connected."))
  }

  mcpStatus(): Promise<never> {
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
