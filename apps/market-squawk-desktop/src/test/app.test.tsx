import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import marketSquawkMarkSvg from "@/assets/market-squawk-mark.svg?raw"
import { CredentialField } from "@/components/setup/credential-field"
import { ProviderStep } from "@/components/setup/provider-step"
import type { AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import {
  parseSourceCoverageResult,
  parseSourceHealthResult,
  parseSourceLifecycleReceipt,
  parseSourceStatusResult,
} from "@/features/sources/source-evidence"
import type {
  PortfolioAccount,
  PortfolioHolding,
} from "@/features/portfolio/portfolio-contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
import type {
  ApplicationResult,
  DesktopBootstrap,
  DesktopEvent,
  ProviderSession,
} from "@/lib/schemas"
import type {
  DesktopEventSubscriptionRequest,
  ProductTransport,
  SourceLifecycleAction,
  SourceLifecycleRequest,
} from "@/lib/transport"

const blockedBootstrap: DesktopBootstrap = {
  contractVersion: "market-squawk-desktop-v1",
  applicationVersion: "1.0.0",
  buildProfile: "development",
  platform: "macos",
  dataRoot: ".market-squawk",
  runtime: {
    installationId: "7e8299e7-9757-4441-926f-d0b22c767a65",
    workspaceId: "55e7626c-81c8-4e78-8aa6-45a1d9c2949a",
    serviceGeneration: 1,
  },
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
  telemetryEnabled: false,
  encryptedFileFallback: "locked",
  providerProfiles: [],
  providerSessions: [],
  operations: [],
}

function transport(
  bootstrap = blockedBootstrap,
  onboard: ProductTransport["onboard"] = async () => {
    throw new Error("Provider onboarding is not configured for this test.")
  },
  query: ProductTransport["query"] = async () => ({
    data: null,
    metadata: {
      completeness: "complete",
      returnedItems: 0,
      availableItems: 0,
      sourceCoverage: { status: "not_applicable" },
      dataQuality: { status: "not_applicable" },
    },
  }),
): ProductTransport {
  return {
    bootstrap: async () => bootstrap,
    bootstrapService: async () => {
      throw new Error("Service bootstrap is not configured for this test.")
    },
    reconnectService: async () => {
      throw new Error("Service reconnect is not configured for this test.")
    },
    installation: async () => ({
      action: "status",
      status: {
        installed: false,
        active_version: null,
        previous_version: null,
        target: null,
        manifest_sha256: null,
        channel_manifest_url: null,
        healthy: false,
      },
      receipt: null,
      restartRequired: false,
    }),
    query,
    analyticalController: async () =>
      analyticalControllerStatus(bootstrap.runtime.workspaceId),
    researchControl: async () =>
      query({ query: "researchDatasets" }),
    datasetPreparation: async () =>
      query({ query: "analysisFeatureDatasets" }),
    backtestPreparation: async () =>
      query({ query: "jobs", limit: 25 }),
    startBacktestFromFile: async () =>
      query({ query: "jobs", limit: 25 }),
    modelControl: async () =>
      query({ query: "jobs", limit: 25 }),
    forecastPreparation: async () =>
      query({ query: "jobs", limit: 25 }),
    decisionControl: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    governanceQuery: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    governanceControl: async () =>
      query({ query: "decisionScreens", limit: 25 }),
    fairValueControl: async () =>
      query({ query: "fairValueMeasurements" }),
    paperControl: async () =>
      query({ query: "paperStatus" }),
    manualPaper: async () =>
      query({ query: "paperStatus" }),
    jobControl: async (request) =>
      query({ query: "jobs", limit: "limit" in request ? request.limit : 25 }),
    sourceControl: async (_action, _request) =>
      query({ query: "sourceStatus" }),
    importProviderCredentialBundle: async () => null,
    operationsControl: async () =>
      query({ query: "operationUpdateStatus" }),
    stageTrainingInput: async () => null,
    mcpClients: async () => {
      const claudeService = {
        client: "claude_code" as const,
        clientId: "d6b1a16d-bdf9-44d9-b10b-6e7558d701cb",
        credentialGeneration: 1,
        credentialIdentity: "d6b1a16d-bdf9-44d9-b10b-6e7558d701cb:1",
        maximumActiveRequests: 4,
        activeRequests: 0,
        admittedRequests: 0,
        rateLimitedRequests: 0,
        observedRelayInitializations: 0,
        lastActivityUnixSeconds: null,
        credentialRotationRecoveryPending: false,
        priorCredentialCleanupPending: false,
        accessRevoked: false,
      }
      const codexService = {
        client: "codex" as const,
        clientId: "6c4a5edb-caa2-4945-91fb-95baaca448f8",
        credentialGeneration: 1,
        credentialIdentity: "6c4a5edb-caa2-4945-91fb-95baaca448f8:1",
        maximumActiveRequests: 4,
        activeRequests: 0,
        admittedRequests: 0,
        rateLimitedRequests: 0,
        observedRelayInitializations: 0,
        lastActivityUnixSeconds: null,
        credentialRotationRecoveryPending: false,
        priorCredentialCleanupPending: false,
        accessRevoked: false,
      }
      const serviceClients = [claudeService, codexService]
      return {
        serviceReady: true,
        sharedEndpointReady: true,
        workspaceId: bootstrap.runtime.workspaceId,
        serviceGeneration: bootstrap.runtime.serviceGeneration,
        protocolVersion: "2025-11-25",
        transport: "stdio_relay",
        runtime: {
          sessionModel: "stateless_request_scoped",
          activeClients: 0,
          activeRequests: 0,
          admittedRequests: 0,
          rateLimitedRequests: 0,
          rejectedCredentials: 0,
          uptimeSeconds: 60,
          process: {
            residentMemoryBytes: 16_777_216,
            virtualMemoryBytes: 67_108_864,
          },
          limits: {
            maximumFrameBytes: 1_048_576,
            maximumBodyBytes: 1_048_576,
            maximumActiveRequests: 8,
            maximumInlineBytes: 262_144,
            maximumInlineItems: 1_000,
            maximumResultBytes: 16_777_216,
            maximumResultItems: 10_000,
            requestTimeoutMilliseconds: 30_000,
          },
          clients: serviceClients,
        },
        clients: [
          {
            client: "claude_code",
            label: "Claude Code",
            state: "absent",
            clientVersion: null,
            receipt: null,
            verification: null,
            blocker: null,
            service: claudeService,
          },
          {
            client: "codex",
            label: "Codex",
            state: "absent",
            clientVersion: null,
            receipt: null,
            verification: null,
            blocker: null,
            service: codexService,
          },
        ],
      }
    },
    mcpClientControl: async () =>
      Promise.reject(new Error("MCP mutation is not configured for this test.")),
    subscribe: async (request) => ({
      receipt: {
        subscriptionId: "f49e02f6-8c47-43a5-bb33-030e8e0d12bb",
        runtime: request.runtime,
        sequence: request.afterSequence,
        resumed: request.afterSequence !== "0",
      },
      unsubscribe: async () => undefined,
    }),
    onboard,
    openOfficialProviderPage: async () => undefined,
    openProtectedProviderSetup: async () => undefined,
  }
}

function analyticalControllerStatus(workspaceId: string): AnalyticalControllerStatus {
  const digest = "73ed06501d2693749c6966f0a8fdcf10c48b8f03632959c0c847ee8a4be9db54"
  const profileId = "0467d4c9-befd-5b7d-b4b5-99b673662c86"
  const config = {
    supportedInvestmentPolicy: { kind: "default_required" as const },
    pointInTimeDatasetPolicy: { kind: "default_required" as const },
    requiredFeatureSet: { kind: "default_required" as const },
    modelBundlePolicy: { kind: "default_required" as const },
    trainingCalibrationPolicy: { kind: "default_required" as const },
    forecastHorizonPolicy: { kind: "default_required" as const },
    valuationPolicy: { kind: "default_required" as const },
    backtestCostPolicy: { kind: "default_required" as const },
    recommendationPolicy: { kind: "default_required" as const },
    riskFreshnessAbstentionPolicy: { kind: "default_required" as const },
  }
  const profile = {
    profileId,
    ownerWorkspaceId: workspaceId,
    displayName: "Market Squawk Default V1",
    kind: "default" as const,
    version: 1,
    revision: "1",
    configDigest: digest,
    config,
    validationState: "default_immutable" as const,
    lastValidation: null,
    createdAt: "1800000000000000000",
    updatedAt: "1800000000000000000",
  }
  return {
    kind: "status",
    controllerSchemaVersion: 1,
    ownerWorkspaceId: workspaceId,
    controllerRevision: "1",
    activeProfile: {
      profileId,
      ownerWorkspaceId: workspaceId,
      displayName: profile.displayName,
      kind: "default",
      version: 1,
      profileRevision: "1",
      configDigest: digest,
      activationRevision: "1",
      activatedAt: "1800000000000000000",
    },
    profiles: [profile],
    workflowRuns: [],
    workflowReadiness: {
      state: "blocked",
      blockers: [
        {
          code: "canonical_data_and_backend_composition_required",
          detail: "Required canonical data and pure backend operations are not composed.",
          owner: "installed_application",
        },
        {
          code: "desktop_start_resume_not_registered",
          detail: "Find and Analyze start commands are intentionally absent.",
          owner: "desktop",
        },
      ],
    },
  }
}

function datasetRead(
  name: string,
  domain: string,
  description: string,
): DesktopBootstrap["operations"][number] {
  return {
    name,
    description,
    domain,
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        dataset: { type: "string" },
        resultLimits: { type: "object" },
      },
      required: ["dataset", "resultLimits"],
    },
  }
}

const resultLimitsInputSchema = {
  type: "object",
  properties: {
    maximumItems: { type: "integer", minimum: 1, maximum: 100_000 },
    maximumBytes: { type: "integer", minimum: 1, maximum: 268_435_456 },
  },
  required: ["maximumItems", "maximumBytes"],
  additionalProperties: false,
}

function localRead(
  name: string,
  domain: string,
  description: string,
): DesktopBootstrap["operations"][number] {
  return {
    name,
    description,
    domain,
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: { resultLimits: resultLimitsInputSchema },
      required: ["resultLimits"],
      additionalProperties: false,
    },
  }
}

function sourceStatusRead(): DesktopBootstrap["operations"][number] {
  return {
    name: "Source.GetStatus",
    description: "Return bounded configured provider status.",
    domain: "source",
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: {
        resultLimits: resultLimitsInputSchema,
        sourceCoverage: {
          type: "array",
          minItems: 1,
          maxItems: 32,
          uniqueItems: true,
          items: { type: "string", minLength: 1, maxLength: 256 },
        },
      },
      required: ["resultLimits"],
      additionalProperties: false,
    },
  }
}

function investmentAnalysisListRead(): DesktopBootstrap["operations"][number] {
  return {
    name: "Decision.ListInvestmentAnalyses",
    description: "List retained investment analyses in durable append order.",
    domain: "decision",
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: {
        afterAnalysisId: { type: "string", pattern: "^[0-9a-f]{64}$" },
        limit: { type: "integer", minimum: 1, maximum: 1_000 },
        resultLimits: resultLimitsInputSchema,
      },
      required: ["limit", "resultLimits"],
      additionalProperties: false,
    },
  }
}

function investmentAnalysisRead(): DesktopBootstrap["operations"][number] {
  return {
    name: "Decision.GetInvestmentAnalysis",
    description: "Return one exact retained investment analysis.",
    domain: "decision",
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: {
        analysisId: { type: "string", pattern: "^[0-9a-f]{64}$" },
        resultLimits: resultLimitsInputSchema,
      },
      required: ["analysisId", "resultLimits"],
      additionalProperties: false,
    },
  }
}

function recommendationTrackRecordRead(): DesktopBootstrap["operations"][number] {
  return {
    name: "Decision.GetRecommendationTrackRecord",
    description: "Return one exact analytical-profile recommendation track record.",
    domain: "decision",
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: {
        profileId: {
          type: "string",
          minLength: 1,
          maxLength: 256,
          pattern: "^[A-Za-z0-9_.:/-]+$",
        },
        profileRevision: {
          type: "integer",
          minimum: 1,
          maximum: 4_294_967_295,
        },
        profileDigest: { type: "string", pattern: "^[0-9a-f]{64}$" },
        horizonNanos: {
          type: "integer",
          minimum: 1,
          maximum: 9_223_372_036_854_775_807,
        },
        evaluatedAtUnixNanos: {
          type: "integer",
          minimum: -9_223_372_036_854_775_808,
          maximum: 9_223_372_036_854_775_807,
        },
        resultLimits: resultLimitsInputSchema,
      },
      required: [
        "profileId",
        "profileRevision",
        "profileDigest",
        "horizonNanos",
        "evaluatedAtUnixNanos",
        "resultLimits",
      ],
      additionalProperties: false,
    },
  }
}

const h15ProviderProfile: DesktopBootstrap["providerProfiles"][number] = {
  id: "federal-reserve-board.data-download-program",
  display_name: "Federal Reserve Board Data Download Program",
  official_handoff_url: "https://www.federalreserve.gov/datadownload/",
  handoff_instruction: "No account or credential is required.",
  zero_fee: "No fee",
  account_requirement: "No account",
  credential_requirement: "No credential",
  release_state: "available",
  coverage: "Federal Reserve Board H.15 selected interest rates",
  quality_ceiling: "official_delayed",
  rights: [
    { operation: "retrieve", admission: "admitted" },
    { operation: "display", admission: "admitted" },
    { operation: "persist", admission: "admitted" },
    { operation: "model_training", admission: "admitted" },
  ],
}

const alpacaProviderProfile: DesktopBootstrap["providerProfiles"][number] = {
  id: "alpaca.basic-market-data",
  display_name: "Alpaca Basic Market Data",
  official_handoff_url: "https://alpaca.markets/",
  handoff_instruction: "Use a Paper-only market-data credential.",
  zero_fee: "Basic IEX market data",
  account_requirement: "Paper credential realm",
  credential_requirement: "Paper API key and secret",
  release_state: "available",
  coverage: "Alpaca Basic IEX market data",
  quality_ceiling: "direct_unverified",
  rights: [
    { operation: "retrieve", admission: "admitted" },
    { operation: "display", admission: "admitted" },
    { operation: "persist", admission: "blocked" },
    { operation: "model_training", admission: "blocked" },
  ],
}

const alpacaOnboardingSessionId = "3f99c998-b834-4d34-a759-66fbf3d8ab3a"
const alpacaPublicConfigurationSha256 = "c".repeat(64)
const initialAlpacaDoctorReceiptSha256 = "1a".repeat(32)
const renewedAlpacaDoctorReceiptSha256 = "2b".repeat(32)
const alpacaDoctorPredecessors = new Map([
  [renewedAlpacaDoctorReceiptSha256, initialAlpacaDoctorReceiptSha256],
])
const initialAlpacaRuntimeGenerationSha256 = "8".repeat(64)
const resynchronizedAlpacaRuntimeGenerationSha256 = "9".repeat(64)
const reactivatedAlpacaRuntimeGenerationSha256 = "a".repeat(64)

function alpacaDoctorRate(observed: boolean) {
  return observed
    ? {
        limit: { state: "observed", value: 200 },
        remaining: { state: "observed", value: 195 },
        reset_unix_seconds: { state: "observed", value: "1786567800" },
        retry_after: { state: "missing" },
      }
    : {
        limit: { state: "missing" },
        remaining: { state: "missing" },
        reset_unix_seconds: { state: "missing" },
        retry_after: { state: "missing" },
      }
}

function alpacaDoctorHttp(identity: string, observedRate = true) {
  return {
    endpointContractSha256: identity.repeat(64),
    requestSha256: "2".repeat(64),
    status: 200,
    bodySha256: "3".repeat(64),
    bytes: "512",
    receivedAt: "2026-08-12T20:00:05.000000000Z",
    latencyNanos: "125000000",
    rate: alpacaDoctorRate(observedRate),
  }
}

const alpacaDoctorAdditional = [
  ["options_rest", "not_probed"],
  ["options_stream", "not_probed"],
  ["fixed_income", "not_probed"],
  ["corporate_actions", "not_probed"],
  ["sip", "unavailable"],
  ["nbbo", "unavailable"],
  ["opra", "unavailable"],
  ["price_level_depth", "unavailable"],
  ["order_level_depth", "unavailable"],
  ["brokerage_account", "unavailable"],
  ["positions", "unavailable"],
  ["orders", "unavailable"],
  ["trading", "unavailable"],
].map(([capability, disposition], index) => ({
  capability,
  disposition,
  evidenceSha256: String((index % 9) + 1).repeat(64),
}))

function alpacaDoctorEvidence({
  receiptSha256,
  verifiedAt,
  exclusiveExpiresAt,
}: {
  receiptSha256: string
  verifiedAt: string
  exclusiveExpiresAt: string
}) {
  return {
    schema: "market-squawk.alpaca-paper-iex-doctor/v1",
    receiptSha256,
    surfaceId: "alpaca.basic-market-data",
    onboardingSessionId: alpacaOnboardingSessionId,
    credentialGeneration: "7",
    realm: "paper",
    marketDataPrincipalSha256: "b".repeat(64),
    principalSemantics:
      "non_trading_market_data_credential_principal_not_brokerage_account",
    capabilityRevision: "4",
    capabilitySha256: "d".repeat(64),
    publicConfigurationSha256: alpacaPublicConfigurationSha256,
    rightsDecisionSha256: "e".repeat(64),
    ratePolicySha256: "f".repeat(64),
    doctorRevision: "market-squawk.alpaca-paper-iex-doctor-implementation.v2",
    doctorContractSha256: "ed8ab1614fc4cee29b213b7eed8ce59033e0041378039f51368ad872bfe3a911",
    dataQuality: "direct_unverified",
    verifiedAt,
    exclusiveExpiresAt,
    current: true,
    capabilities: {
    iexLatestQuote: {
      disposition: "available",
      evidenceSha256: "2".repeat(64),
      observation: {
        http: alpacaDoctorHttp("4"),
        semanticResultSha256: "5".repeat(64),
        quoteTimestamp: "2026-08-12T20:00:04.000000000Z",
      },
    },
    iexSnapshotBatch: {
      disposition: "available",
      evidenceSha256: "3".repeat(64),
      observation: {
        http: alpacaDoctorHttp("5"),
        semanticResultSha256: "6".repeat(64),
        requested: 50,
        returned: 50,
        valid: 50,
        missing: 0,
        unexpected: 0,
        duplicate: 0,
        invalid: 0,
        requestedSetSha256: "7".repeat(64),
        returnedSetSha256: "8".repeat(64),
        missingSetSha256: "9".repeat(64),
        unexpectedSetSha256: "a".repeat(64),
      },
    },
    iexWebSocket: {
      disposition: "available",
      evidenceSha256: "4".repeat(64),
      observation: {
        endpointContractSha256: "5".repeat(64),
        requestSha256: "6".repeat(64),
        connectedFrameSha256: "7".repeat(64),
        authenticatedFrameSha256: "8".repeat(64),
        subscriptionFrameSha256: "9".repeat(64),
        semanticResultSha256: "a".repeat(64),
        handshakeStatus: 101,
        handshakeRate: alpacaDoctorRate(false),
        subscribedTrades: 1,
        subscribedQuotes: 1,
        framesObserved: 3,
        bytesObserved: "384",
        authenticatedAt: "2026-08-12T20:00:01.000000000Z",
        subscribedAt: "2026-08-12T20:00:02.000000000Z",
        closeSent: true,
        cleanCloseObserved: true,
        completedAt: "2026-08-12T20:00:03.000000000Z",
      },
    },
    iexHistoricalBars: {
      disposition: "available",
      evidenceSha256: "5".repeat(64),
      observation: {
        endpointContractSha256: "6".repeat(64),
        requestSha256: "7".repeat(64),
        semanticResultSha256: "8".repeat(64),
        startDate: { year: 2026, month: 8, day: 10 },
        endDate: { year: 2026, month: 8, day: 11 },
        pages: 2,
        bars: 2,
        distinctDates: 2,
        firstBarTimestamp: "2026-08-10T20:00:00.000000000Z",
        lastBarTimestamp: "2026-08-11T20:00:00.000000000Z",
        returnedDatesSha256: "9".repeat(64),
        paginationGraphSha256: "a".repeat(64),
        terminalPagination: true,
        pageEvidence: [
          {
            http: alpacaDoctorHttp("b"),
            requestPageTokenSha256: null,
            responsePageTokenSha256: "4c".repeat(32),
          },
          {
            http: alpacaDoctorHttp("c"),
            requestPageTokenSha256: "4c".repeat(32),
            responsePageTokenSha256: null,
          },
        ],
      },
    },
    iexUtcCalendar: {
      disposition: "available",
      evidenceSha256: "6".repeat(64),
      observation: {
        http: alpacaDoctorHttp("c"),
        semanticResultSha256: "d".repeat(64),
        startDate: { year: 2026, month: 8, day: 10 },
        endDate: { year: 2026, month: 8, day: 11 },
        sessions: 2,
        historyDates: 2,
        matchedDates: 2,
        missingHistoryDates: 0,
        unexpectedHistoryDates: 0,
        sessionDatesSha256: "9".repeat(64),
        historyDatesSha256: "9".repeat(64),
        exactDateReconciliation: true,
      },
    },
    additional: alpacaDoctorAdditional,
    },
  }
}

const initialAlpacaDoctorEvidence = alpacaDoctorEvidence({
  receiptSha256: initialAlpacaDoctorReceiptSha256,
  verifiedAt: "2026-08-12T20:00:06.000000000Z",
  exclusiveExpiresAt: "2026-08-12T20:15:06.000000000Z",
})

const renewedAlpacaDoctorEvidence = alpacaDoctorEvidence({
  receiptSha256: renewedAlpacaDoctorReceiptSha256,
  verifiedAt: "2026-08-12T20:05:06.000000000Z",
  exclusiveExpiresAt: "2026-08-12T20:20:06.000000000Z",
})

type AlpacaSourceStage =
  | "unconfigured"
  | "doctor_required"
  | "eligible"
  | "active"
  | "resynchronized"
  | "renewed"
  | "reactivated"

function alpacaSourceStatus(stage: AlpacaSourceStage): ApplicationResult {
  const configured = stage !== "unconfigured"
  const admitted = configured && stage !== "doctor_required"
  const active = stage === "active" || stage === "resynchronized" ||
    stage === "reactivated"
  const stateRevision = stage === "unconfigured"
    ? "7"
    : stage === "doctor_required"
      ? "8"
    : stage === "eligible"
      ? "9"
      : stage === "active"
        ? "10"
        : stage === "resynchronized"
          ? "11"
          : stage === "renewed"
            ? "12"
            : "13"
  const runtimeGenerationSha256 = stage === "active"
    ? initialAlpacaRuntimeGenerationSha256
    : stage === "resynchronized"
      ? resynchronizedAlpacaRuntimeGenerationSha256
      : stage === "reactivated"
        ? reactivatedAlpacaRuntimeGenerationSha256
        : null
  const doctor = admitted
    ? stage === "renewed" || stage === "reactivated"
      ? renewedAlpacaDoctorEvidence
      : initialAlpacaDoctorEvidence
    : null
  const observedAt = stage === "renewed" || stage === "reactivated"
    ? "2026-08-12T20:05:07.000000000Z"
    : "2026-08-12T20:00:07.000000000Z"
  const runtime = active
    ? {
        state: "active_group",
        runtimeGenerationSha256,
        qualifiedRuntimeRecordCount: 0,
      }
    : { state: "not_active" }
  const currentSession = configured
    ? {
        session_id: alpacaOnboardingSessionId,
        surface_id: "alpaca.basic-market-data",
        state: active ? "active_scoped" : stage === "doctor_required" ? "stored_unverified" : "runtime_verification_pending",
        next_action: active ? "active" : "verify_and_activate",
        credential_stored: true,
      }
    : null
  const result = {
    data: [{
      profile: {
        ...alpacaProviderProfile,
      },
      currentSession,
      providerDatasetIdentifier: null,
      lifecycleSupport: "managed",
      lifecycle: {
        provider: "alpaca.basic-market-data",
        state: active ? "active" : "stopped",
        stateRevision,
        configurationSessionId: configured ? alpacaOnboardingSessionId : null,
        currentGeneration: null,
        runtimeGenerationSha256,
        publicConfigurationSha256: configured ? alpacaPublicConfigurationSha256 : null,
        doctor,
        startEligibility: stage === "unconfigured" || stage === "doctor_required"
          ? "doctor_required"
          : active
              ? "already_active"
              : "eligible",
        blocker: null,
        observedAt,
      },
      runtime,
    }],
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: {
        authority: "code_owned_profiles_and_current_runtime_evidence",
        requestedSources: ["alpaca.basic-market-data"],
        profileCount: 1,
        runtimeRecordCount: 0,
        runtimeAbsence: "not_established",
      },
      dataQuality: {
        authority: "profile_ceiling_and_runtime_qualification",
        runtimeClasses: [],
        runtimeAbsence: "not_active",
        executionEligibilityUnchanged: true,
      },
    },
  }
  parseSourceStatusResult(result, ["alpaca.basic-market-data"])
  return result
}

function alpacaLifecycleResult(
  action: SourceLifecycleAction,
  request: SourceLifecycleRequest,
  stage: AlpacaSourceStage,
): ApplicationResult {
  const status = alpacaSourceStatus(stage)
  const statusRow = Array.isArray(status.data) ? status.data[0] : null
  if (!statusRow || typeof statusRow !== "object" || !("lifecycle" in statusRow)) {
    throw new Error("The Alpaca lifecycle status fixture is unavailable.")
  }
  const lifecycle = statusRow.lifecycle as Record<string, unknown>
  const active = lifecycle.state === "active"
  const doctor = lifecycle.doctor as ReturnType<typeof alpacaDoctorEvidence> | null
  const result: ApplicationResult = {
    data: {
      operationId: `desktop-alpaca-${action}-${String(lifecycle.stateRevision)}`,
      provider: "alpaca.basic-market-data",
      action,
      disposition: "applied",
      state: lifecycle.state,
      stateRevision: lifecycle.stateRevision,
      previousGeneration: null,
      currentGeneration: null,
      runtimeGenerationSha256: lifecycle.runtimeGenerationSha256,
      coverage: null,
      integrity: null,
      quality: null,
      rateBudget: { state: "indeterminate" },
      authorization: "admitted",
      availability: active ? "available" : "indeterminate",
      rightsEvidence: {
        id: "alpaca-paper-iex-rights",
        sha256: doctor?.rightsDecisionSha256,
        effectiveAt: "2026-08-12T19:59:00.000000000Z",
        expiresAt: null,
      },
      blocker: null,
      publicConfigurationSha256: lifecycle.publicConfigurationSha256,
      configurationSessionId: lifecycle.configurationSessionId,
      doctor,
      startEligibility: lifecycle.startEligibility,
      observedAt: lifecycle.observedAt,
    },
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: { status: "not_applicable" },
      dataQuality: { status: "not_applicable" },
    },
  }
  parseSourceLifecycleReceipt(result, action, request)
  return result
}

const inactiveH15SourceStatus: ApplicationResult = {
  data: [
    {
      profile: {
        ...h15ProviderProfile,
      },
      currentSession: null,
      providerDatasetIdentifier:
        "federal-reserve-board:h15:h15-treasury-constant-maturities:339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f",
      lifecycleSupport: "managed",
      lifecycle: {
        provider: "federal-reserve-board.data-download-program",
        state: "stopped",
        stateRevision: "7",
        configurationSessionId: null,
        currentGeneration: null,
        runtimeGenerationSha256: null,
        publicConfigurationSha256: null,
        doctor: null,
        startEligibility: "not_applicable",
        blocker: null,
        observedAt: "2026-08-11T20:31:00.000000000Z",
      },
      runtime: { state: "not_active" },
    },
  ],
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: {
      authority: "code_owned_profiles_and_current_runtime_evidence",
      requestedSources: ["federal-reserve-board.data-download-program"],
      profileCount: 1,
      runtimeRecordCount: 0,
      runtimeAbsence: "not_established",
    },
    dataQuality: {
      authority: "profile_ceiling_and_runtime_qualification",
      runtimeClasses: [],
      runtimeAbsence: "not_active",
      executionEligibilityUnchanged: true,
    },
  },
}

parseSourceStatusResult(
  inactiveH15SourceStatus,
  ["federal-reserve-board.data-download-program"],
)

function sourceSecondaryMetadata(): ApplicationResult["metadata"] {
  return {
    completeness: "complete",
    returnedItems: 2,
    availableItems: 2,
    sourceCoverage: {
      authority: "code_owned_profiles_and_current_runtime_evidence",
      requestedSources: [],
      profileCount: 2,
      runtimeRecordCount: 0,
      runtimeAbsence: "not_established",
    },
    dataQuality: {
      authority: "profile_ceiling_and_runtime_qualification",
      runtimeClasses: [],
      runtimeAbsence: "not_active",
      executionEligibilityUnchanged: true,
    },
  }
}

function sourceCoverageResult(): ApplicationResult {
  return {
    data: [
      {
        surfaceId: h15ProviderProfile.id,
        releaseState: h15ProviderProfile.release_state,
        declaredCoverage: h15ProviderProfile.coverage,
        qualityCeiling: h15ProviderProfile.quality_ceiling,
        rights: h15ProviderProfile.rights,
        runtimeCoverage: { state: "not_established" },
      },
      {
        surfaceId: alpacaProviderProfile.id,
        releaseState: alpacaProviderProfile.release_state,
        declaredCoverage: alpacaProviderProfile.coverage,
        qualityCeiling: alpacaProviderProfile.quality_ceiling,
        rights: alpacaProviderProfile.rights,
        runtimeCoverage: { state: "not_established" },
      },
    ],
    metadata: sourceSecondaryMetadata(),
  }
}

function sourceHealthResult(stage: AlpacaSourceStage): ApplicationResult {
  const alpacaStatus = alpacaSourceStatus(stage)
  const alpacaRow = Array.isArray(alpacaStatus.data)
    ? alpacaStatus.data[0] as Record<string, unknown> | undefined
    : undefined
  const session = alpacaRow?.currentSession as Record<string, unknown> | null | undefined
  return {
    data: [
      {
        surfaceId: h15ProviderProfile.id,
        onboardingState: null,
        runtimeHealth: { state: "not_active" },
      },
      {
        surfaceId: alpacaProviderProfile.id,
        onboardingState: session?.state ?? null,
        runtimeHealth: { state: "not_active" },
      },
    ],
    metadata: sourceSecondaryMetadata(),
  }
}

function groupedSourceStatuses(stage: AlpacaSourceStage) {
  return [
    ...parseSourceStatusResult(
      inactiveH15SourceStatus,
      ["federal-reserve-board.data-download-program"],
    ),
    ...parseSourceStatusResult(
      alpacaSourceStatus(stage),
      ["alpaca.basic-market-data"],
    ),
  ]
}

const stoppedPaperStatus: ApplicationResult = {
  data: { state: "stopped", lastShutdownComplete: true },
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: { status: "not_applicable" },
    dataQuality: { status: "not_applicable" },
  },
}

const emptyRowsResult: ApplicationResult = {
  data: null,
  metadata: {
    completeness: "complete",
    returnedItems: 0,
    availableItems: 0,
    sourceCoverage: { status: "not_applicable" },
    dataQuality: { status: "not_applicable" },
  },
}

const retainedAnalysisId = "c".repeat(64)
const retainedProposalId = "d".repeat(64)
const retainedDerivationDigest = "e".repeat(64)
const retainedEvidenceDigest = "f".repeat(64)
const retainedPolicyDigest = "1".repeat(64)
const retainedProfileDigest = "2".repeat(64)
const retainedProjectionDigest = "3".repeat(64)
const retainedSizingDigest = "4".repeat(64)
const retainedInstrumentId = "11111111-1111-4111-8111-111111111111"
const retainedAccountId = "22222222-2222-4222-8222-222222222222"
const retainedAsOf = "1800000000000000000"
const retainedHorizonAt = "1831536000000000000"
const retainedExpiresAt = "1800086400000000000"
const retainedHorizonNanos = "31536000000000000"

function retainedMoney(amount: string) {
  return { amount, currency: "USD" }
}

function retainedRange(lower: string, upper: string) {
  return { lower: retainedMoney(lower), upper: retainedMoney(upper) }
}

function retainedIdentity(character: string) {
  return { algorithm: "sha256", digest: character.repeat(64) }
}

function retainedWindow(character: string) {
  return {
    observedAt: "1799999800000000000",
    availableAt: "1799999900000000000",
    expiresAt: "1800001000000000000",
    contentIdentity: retainedIdentity(character),
  }
}

const retainedPriceCases = {
  downside: retainedMoney("80"),
  base: retainedMoney("110"),
  upside: retainedMoney("130"),
}

const retainedForecastRanges = {
  downside: retainedRange("75", "85"),
  base: retainedRange("100", "120"),
  upside: retainedRange("125", "140"),
}

const retainedEvidenceReliability = {
  meaning: "policy_weighted_evidence_reliability_v1",
  valuePpm: 850_000,
  components: [
    ["forecast_calibration", 200_000],
    ["valuation_agreement", 200_000],
    ["backtest_stability", 150_000],
    ["market_integrity", 150_000],
    ["liquidity_capacity", 150_000],
    ["portfolio_risk_capacity", 150_000],
  ].map(([kind, weightPpm]) => ({ kind, valuePpm: 850_000, weightPpm })),
}

const retainedGeneratedResult = {
  kind: "generated",
  proposalId: retainedProposalId,
  derivationDigest: retainedDerivationDigest,
  action: "buy",
  priceLadder: {
    cases: retainedPriceCases,
    ranges: {
      ...retainedForecastRanges,
      entry: retainedRange("95", "105"),
      add: retainedRange("85", "95"),
      trim: retainedRange("120", "130"),
      exit: retainedRange("135", "145"),
    },
    addCase: retainedMoney("90"),
  },
  actionZoneSemantics: {
    version: 1,
    referenceZone: retainedRange("95", "105"),
    triggerFloorExclusive: null,
    triggerFloorInclusive: null,
    triggerCeilingInclusive: retainedMoney("105"),
  },
  evidenceReliability: retainedEvidenceReliability,
  horizonAt: retainedHorizonAt,
  expiresAt: retainedExpiresAt,
}

const retainedInvestmentAnalysis: ApplicationResult = {
  data: {
    analysisId: retainedAnalysisId,
    executionEligibility: "research_only_execution_ineligible",
    policy: {
      version: 1,
      digest: retainedPolicyDigest,
      actionZoneSemanticsVersion: 1,
      horizonNanos: retainedHorizonNanos,
      proposalLifetimeNanos: "86400000000000",
      assumptions: [
        "All values are bound to retained point-in-time evidence.",
        "Gross price movement is not net portfolio profit.",
        "The analysis creates no execution authority.",
      ],
      invalidationConditions: [
        "The retained market mark expires.",
        "The forecast or valuation binding changes.",
        "The account-specific risk evidence changes.",
      ],
      limitations: [
        "No guaranteed return is represented.",
        "Forward costs and taxes are unavailable.",
        "Sizing feasibility is not an order instruction.",
      ],
    },
    evidence: {
      instrumentId: retainedInstrumentId,
      currency: "USD",
      accountId: retainedAccountId,
      asOf: retainedAsOf,
      market: {
        instrumentId: retainedInstrumentId,
        price: retainedMoney("100"),
        quality: "direct_verified",
        priceKind: "last_trade",
        adjustmentBasis: "unadjusted_spot",
        selectionReceiptIdentity: retainedIdentity("5"),
        selectedObservationIdentity: retainedIdentity("6"),
        window: retainedWindow("7"),
      },
      priceForecast: {
        instrumentId: retainedInstrumentId,
        cases: retainedPriceCases,
        ranges: retainedForecastRanges,
        horizonAt: retainedHorizonAt,
        expectedTerminal: {
          statistic: "model_estimated_conditional_mean",
          price: retainedMoney("110"),
          horizonAt: retainedHorizonAt,
          statisticIdentity: retainedIdentity("8"),
        },
        vintageId: "9".repeat(64),
        outputBindingIdentity: retainedIdentity("a"),
        calibrationIdentity: retainedIdentity("b"),
        outcomeSetIdentity: retainedIdentity("c"),
        calibration: {
          nominalCoveragePpm: 800_000,
          realizedCoveragePpm: 825_000,
          completedOutcomes: 40,
        },
        window: retainedWindow("d"),
      },
      valuation: {
        instrumentId: retainedInstrumentId,
        fairValue: retainedMoney("112"),
        basis: "per_instrument_unit",
        horizonAt: retainedHorizonAt,
        measurementId: "e".repeat(64),
        classificationDecisionId: "f".repeat(64),
        selectionReceiptHash: "1".repeat(64),
        window: retainedWindow("2"),
      },
      backtest: {
        instrumentId: retainedInstrumentId,
        currency: "USD",
        outcomeHorizonNanos: retainedHorizonNanos,
        netReturnBasisPoints: "650",
        maxDrawdownBasisPoints: "1200",
        feeBasisPoints: "10",
        slippageBasisPoints: "15",
        maximumRandomSlippageBasisPoints: "20",
        observations: 260,
        trials: 100,
        stabilityPpm: 850_000,
        simulationCutoffAt: "1799999700000000000",
        datasetIdentity: retainedIdentity("3"),
        commandIdentity: retainedIdentity("4"),
        terminalIdentity: retainedIdentity("5"),
        reportIdentity: retainedIdentity("6"),
        cohortIdentity: retainedIdentity("7"),
        costModelIdentity: retainedIdentity("8"),
        window: retainedWindow("9"),
      },
      liquidity: {
        instrumentId: retainedInstrumentId,
        currency: "USD",
        quotedSpreadBasisPoints: "12",
        capacityPpm: 900_000,
        quality: "direct_verified",
        assessmentIdentity: retainedIdentity("a"),
        window: retainedWindow("b"),
      },
      portfolioRisk: {
        instrumentId: retainedInstrumentId,
        accountId: retainedAccountId,
        currency: "USD",
        portfolioRevision: "c".repeat(64),
        positionState: { kind: "no_position" },
        riskCapacityPpm: 900_000,
        riskReportIdentity: retainedIdentity("d"),
        window: retainedWindow("e"),
      },
    },
    evidenceDigest: retainedEvidenceDigest,
    publication: {
      publicationId: "f".repeat(64),
      publishedAt: "1800000100000000000",
      executionEligibility: "research_only_execution_ineligible",
      analyticalProfile: {
        profileId: "market-squawk-default-v1",
        revision: 1,
        contentDigest: {
          algorithm: "sha256",
          digest: retainedProfileDigest,
        },
      },
      workflow: {
        workflowId: "market-squawk-investment-analysis-v1",
        revision: 1,
        contentDigest: retainedIdentity("1"),
      },
      accountSetup: {
        accountId: retainedAccountId,
        distinctFromAnalyticalProfile: true,
      },
      outcomeProjectionDigest: retainedProjectionDigest,
      sizingProjectionDigest: retainedSizingDigest,
    },
    projection: {
      resultDigest: retainedProjectionDigest,
      proposalId: retainedProposalId,
      derivationDigest: retainedDerivationDigest,
      authority: "analysis_only_no_mutation_no_execution",
      executionEligible: false,
      mark: retainedMoney("100"),
      horizonAt: retainedHorizonAt,
      downside: {
        priceRange: retainedForecastRanges.downside,
        grossReturnFromMark: {
          lowerNumerator: retainedMoney("-25"),
          upperNumerator: retainedMoney("-15"),
          denominator: retainedMoney("100"),
        },
      },
      base: {
        priceRange: retainedForecastRanges.base,
        grossReturnFromMark: {
          lowerNumerator: retainedMoney("0"),
          upperNumerator: retainedMoney("20"),
          denominator: retainedMoney("100"),
        },
      },
      upside: {
        priceRange: retainedForecastRanges.upside,
        grossReturnFromMark: {
          lowerNumerator: retainedMoney("25"),
          upperNumerator: retainedMoney("40"),
          denominator: retainedMoney("100"),
        },
      },
      netPnl: {
        kind: "unavailable",
        reason: "exact_forward_cost_evidence_not_supplied",
      },
      benchmarkReturn: {
        kind: "unavailable",
        reason: "exact_proposal_time_benchmark_evidence_not_supplied",
      },
      afterTaxPnl: {
        kind: "unavailable",
        reason: "exact_tax_evidence_not_supplied",
      },
    },
    sizing: {
      resultDigest: retainedSizingDigest,
      proposalId: retainedProposalId,
      derivationDigest: retainedDerivationDigest,
      authority: "analysis_only_no_mutation_no_execution",
      executionEligible: false,
      evaluatedAt: "1800000200000000000",
      currentLots: "2",
      hardFeasibleLots: { kind: "available", lower: "0", upper: "10" },
      preferredFeasibleLots: { kind: "available", lower: "2", upper: "6" },
      selectedTargetLots: null,
      orderQuantity: null,
    },
    realizedOutcome: {
      kind: "completed",
      metric: "gross_instrument_price_return",
      startMark: retainedMoney("100"),
      endpointPrice: retainedMoney("110"),
      grossPriceReturn: "0.1",
      observedAt: "1831536100000000000",
      availableAt: "1831536200000000000",
      selectionReceiptIdentity: retainedIdentity("2"),
      selectedObservationIdentity: retainedIdentity("3"),
      corporateActionEvidenceIdentity: retainedIdentity("4"),
      netReturn: {
        kind: "unavailable",
        reason: "exact_realized_cost_evidence_not_supplied",
      },
      benchmarkReturn: {
        kind: "unavailable",
        reason: "exact_benchmark_outcome_evidence_not_supplied",
      },
      afterTaxReturn: {
        kind: "unavailable",
        reason: "exact_tax_evidence_not_supplied",
      },
      settlement: {
        kind: "unavailable",
        reason: "no_execution_or_settlement_evidence",
      },
      seriesId: "5".repeat(64),
      revision: 1,
      statusDigest: "6".repeat(64),
      evaluatedAt: "1831536300000000000",
      executionEligible: false,
    },
    result: retainedGeneratedResult,
  },
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: { status: "not_applicable" },
    dataQuality: { status: "not_applicable" },
  },
}

const retainedInvestmentAnalysisPage: ApplicationResult = {
  data: {
    completeness: "complete",
    returnedCount: 1,
    availableCount: 1,
    nextAfterAnalysisId: null,
    analyses: [
      {
        analysisId: retainedAnalysisId,
        proposalId: retainedProposalId,
        derivationDigest: retainedDerivationDigest,
        instrumentId: retainedInstrumentId,
        accountId: retainedAccountId,
        currency: "USD",
        asOf: retainedAsOf,
        horizonAt: retainedHorizonAt,
        expiresAt: retainedExpiresAt,
        policyDigest: retainedPolicyDigest,
        evidenceDigest: retainedEvidenceDigest,
        outcome: { kind: "generated", action: "buy" },
      },
    ],
  },
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: { status: "not_applicable" },
    dataQuality: { status: "not_applicable" },
  },
}

type TrackRecordQuery = Extract<
  Parameters<ProductTransport["query"]>[0],
  { query: "decisionRecommendationTrackRecord" }
>

function retainedTrackRecord(request: TrackRecordQuery): ApplicationResult {
  const emptyGroup = (cohort: string) => ({
    cohort,
    publicationCount: 0,
    dueCount: 0,
    completedCount: 0,
    pendingCount: 0,
    unavailableCount: 0,
    coveragePpm: 0,
    performance: { kind: "unavailable", reason: "no_due_outcomes" },
  })
  return {
    data: {
      analyticalProfile: {
        profileId: request.profileId,
        revision: request.profileRevision,
        contentDigest: {
          algorithm: "sha256",
          digest: request.profileDigest,
        },
      },
      horizonNanos: request.horizonNanos,
      evaluatedAt: request.evaluatedAtUnixNanos,
      analysisUnavailableCount: 2,
      minimumCompletedSamples: 30,
      minimumCoveragePpm: 800_000,
      groups: [
        {
          cohort: "buy",
          publicationCount: 36,
          dueCount: 36,
          completedCount: 30,
          pendingCount: 6,
          unavailableCount: 0,
          coveragePpm: 833_333,
          performance: {
            kind: "available",
            metric: "mean_gross_instrument_price_return",
            meanGrossPriceReturn: "0.08",
            positiveOutcomes: 24,
            zeroOutcomes: 1,
            negativeOutcomes: 5,
          },
        },
        emptyGroup("add"),
        emptyGroup("hold"),
        emptyGroup("trim"),
        emptyGroup("sell"),
        emptyGroup("no_action_control"),
      ],
      forecastCalibrationIncluded: false,
      executionPerformanceIncluded: false,
    },
    metadata: {
      completeness: "complete",
      returnedItems: 6,
      availableItems: 6,
      sourceCoverage: { status: "not_applicable" },
      dataQuality: { status: "not_applicable" },
    },
  }
}

const marketInstrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
const marketObservedAt = "2026-08-09T14:30:00.000000000Z"
const marketUpdatedAt = "2026-08-09T14:30:00.011000000Z"

const marketOverviewRow = {
  instrumentId: marketInstrumentId,
  displaySymbol: "BTC-USD",
  name: "Bitcoin",
  assetClass: "crypto",
  currency: "USD",
  availability: "live",
  confidence: "moderate",
  currentPrice: {
    value: "68000.15",
    currency: "USD",
    basis: "bid_ask_midpoint",
    observedAt: marketObservedAt,
    currentThrough: marketObservedAt,
  },
  quote: {
    bidPrice: "68000.1",
    bidSize: "0.25",
    askPrice: "68000.2",
    askSize: "0.3",
    midPrice: "68000.15",
    lastPrice: null,
    lastSize: null,
    quoteObservedAt: marketObservedAt,
    lastObservedAt: null,
  },
  marketState: {
    timing: "real_time",
    quality: "direct",
    health: "healthy",
    integrity: "verified",
    coverage: "single_market",
    depth: "order_level",
    freshness: "fresh",
    observedAt: marketObservedAt,
    updatedAt: marketUpdatedAt,
    currentThrough: marketObservedAt,
  },
  observations: {
    admittedCount: 2,
    independentCount: null,
    agreement: "not_established",
  },
  depthSummary: {
    kind: "order_level",
    bidLevels: 1,
    askLevels: 1,
    individualOrderCount: 2,
    truncated: false,
  },
  depthDetails: null,
  analysisUse: "current_only",
}

function marketResult(
  row: typeof marketOverviewRow | (Omit<typeof marketOverviewRow, "depthDetails"> & {
    depthDetails: {
      kind: "order_level"
      bids: Array<{ price: string; quantity: string }>
      asks: Array<{ price: string; quantity: string }>
      individualOrders: {
        bidOrders: Array<{ price: string; quantity: string }>
        askOrders: Array<{ price: string; quantity: string }>
        totalCount: number
        returnedCount: number
        truncated: boolean
      }
    }
  }),
): ApplicationResult {
  return {
    data: [row],
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: {
        availability: "available",
        complete: true,
        returnedInstrumentCount: 1,
        observationCount: 2,
      },
      dataQuality: {
        referenceAt: marketUpdatedAt,
        observationCount: 2,
      },
    },
  }
}

const marketOverviewResult = marketResult(marketOverviewRow)
const marketInstrumentResult = marketResult({
  ...marketOverviewRow,
  depthDetails: {
    kind: "order_level",
    bids: [{ price: "68000.1", quantity: "0.25" }],
    asks: [{ price: "68000.2", quantity: "0.3" }],
    individualOrders: {
      bidOrders: [{ price: "68000.1", quantity: "0.25" }],
      askOrders: [{ price: "68000.2", quantity: "0.3" }],
      totalCount: 2,
      returnedCount: 2,
      truncated: false,
    },
  },
})

const macroKnowledgeCutoff = "2026-08-28T14:30:00Z"
const macroEffectiveDateCutoff = "2026-08-27"
const macroIndicatorDefinitions = [
  ["us-government-yield-1m", "1-month government yield", "4.32"],
  ["us-government-yield-3m", "3-month government yield", "4.28"],
  ["us-government-yield-6m", "6-month government yield", "4.18"],
  ["us-government-yield-1y", "1-year government yield", "4.02"],
  ["us-government-yield-2y", "2-year government yield", "3.88"],
  ["us-government-yield-3y", "3-year government yield", "3.82"],
  ["us-government-yield-5y", "5-year government yield", "3.86"],
  ["us-government-yield-7y", "7-year government yield", "3.98"],
  ["us-government-yield-10y", "10-year government yield", "4.12"],
  ["us-government-yield-20y", "20-year government yield", "4.48"],
  ["us-government-yield-30y", "30-year government yield", "4.39"],
  ["us-unemployment-rate", "Unemployment rate", "4.2"],
] as const

function macroContextResult(cutoffs = {
  knowledgeCutoff: macroKnowledgeCutoff,
  effectiveDateCutoff: macroEffectiveDateCutoff,
}): ApplicationResult {
  return {
    data: {
      schemaIdentity: "market-squawk-macro-context/v1",
      availability: "available",
      selection: {
        ...cutoffs,
        evaluatedAt: "2026-08-28T14:30:01Z",
        complete: true,
      },
      confidence: {
        level: "moderate",
        summary: "All requested economic indicators are available for the selected dates.",
      },
      coverage: {
        requested: 12,
        observed: 12,
        missing: 0,
        unavailable: 0,
      },
      observations: macroIndicatorDefinitions.map(
        ([indicatorId, label, decimal], index) => ({
          indicatorId,
          label,
          category: index < 11 ? "interest_rates" : "labor_market",
          frequency: index < 11 ? "business_daily" : "monthly",
          seasonalAdjustment:
            index < 11 ? "not_applicable" : "seasonally_adjusted",
          unit: {
            code:
              index < 11 ? "percent_per_year" : "percent_of_labor_force",
            label: index < 11 ? "Percent per year" : "Percent of labor force",
            symbol: "%",
          },
          effectiveDate: index < 11 ? "2026-08-27" : "2026-07-01",
          recorded: { state: "known", date: "2026-08-27" },
          availableAt: "2026-08-28T12:00:00Z",
          revision: 1,
          supersededAfter: null,
          value: { state: "observed", decimal },
          availability: "available",
          confidence: {
            level: "moderate",
            summary: "Available for the selected dates.",
          },
        }),
      ),
    },
    metadata: {
      completeness: "complete",
      returnedItems: 12,
      availableItems: 12,
      sourceCoverage: { status: "complete" },
      dataQuality: { status: "current" },
    },
  }
}

const portfolioAccountId = "55e7626c-81c8-4e78-8aa6-45a1d9c2949a"
const heldInstrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
const candidateInstrumentId = "0467d4c9-befd-5b7d-b4b5-99b673662c86"
const portfolioRevisionId = "c".repeat(64)
const portfolioAccount: PortfolioAccount = {
  accountId: portfolioAccountId,
  currency: "USD",
  currentRevision: {
    revisionId: portfolioRevisionId,
    effectiveAtUnixNanos: "1800000000000000000",
    availableAtUnixNanos: "1800000001000000000",
    sourceId: "owned-portfolio-import",
    sourceCoverage: ["owned-portfolio-import"],
    artifactSha256: "d".repeat(64),
    holdingCount: 1,
    transactionCount: 1,
    reconciliationDiscrepancies: 0,
  },
  holdingCount: 1,
  transactionCount: 1,
  reconciliationDiscrepancies: 0,
}
const portfolioHolding: PortfolioHolding = {
  account_id: portfolioAccountId,
  instrument_id: heldInstrumentId,
  currency: "USD",
  quantity: "10",
  lot_size: "1",
  market_value: { amount: "1000", currency: "USD" },
  as_of: "1800000000000000000",
  basis: { status: "missing" },
  source_reference: "owned:holding:1",
  revisionId: portfolioRevisionId,
  effectiveAtUnixNanos: "1800000000000000000",
  availableAtUnixNanos: "1800000001000000000",
  sourceId: "owned-portfolio-import",
  artifactSha256: "d".repeat(64),
  markEvidence: {
    sourceReference: "owned:holding:1",
    observedAtUnixNanos: "1800000000000000000",
    venue: null,
    venueStatus: "not_applicable",
    state: "source_reported",
    quality: "direct_unverified",
    executionEligible: false,
    freshness: { status: "current", reason: "source_reported" },
    fallback: { status: "not_used", reason: "source_reported" },
  },
}

function candidateImpactResult(instrumentId: string): ApplicationResult {
  const evidence = { algorithm: "sha256", bytes: "e".repeat(64) }
  return {
    data: {
      accountId: portfolioAccountId,
      revisionId: portfolioRevisionId,
      setupEvidence: {
        setupRevision: "1",
        setupDigest: "1".repeat(64),
        configurationDigest: "2".repeat(64),
        profileDigest: "3".repeat(64),
        catalogDigest: "4".repeat(64),
      },
      policy: "selected_market_candidate_impact_v3",
      evidenceSchemaVersion: 1,
      evidenceDigest: evidence,
      portfolioEvidence: {
        revisionId: portfolioRevisionId,
        effectiveAtUnixNanos: "1800000000000000000",
        availableAtUnixNanos: "1800000001000000000",
        sourceId: "owned-portfolio-import",
        sourceCoverage: ["owned-portfolio-import"],
        artifactSha256: "d".repeat(64),
      },
      instrumentId,
      positionState: "zero_position",
      currentQuantity: "0",
      proposedQuantity: "3",
      currentMarketValue: { amount: "0", currency: "USD" },
      proposedMarketValue: { amount: "300", currency: "USD" },
      capitalChange: { amount: "300", currency: "USD" },
      portfolioValue: { amount: "2500", currency: "USD" },
      portfolioValueBasis:
        "source_reported_holdings_with_selected_candidate_revalued",
      instrumentTerms: {
        definitionRevision: "3",
        priceTick: "0.01",
        lotSize: "1",
        quoteCurrency: "USD",
        settlementDenomination: { kind: "currency", currency: "USD" },
        contractMultiplier: "1",
      },
      costEvidence: {
        fees: { status: "unavailable", reason: "exact_fees" },
        slippage: { status: "unavailable", reason: "exact_slippage" },
      },
      concentration: { current: "0", proposed: "0.12", change: "0.12" },
      scenario: {
        scope: "candidate_position_only",
        shock: "-0.1",
        currentImpact: { amount: "0", currency: "USD" },
        proposedImpact: { amount: "-30", currency: "USD" },
        marginalImpact: { amount: "-30", currency: "USD" },
      },
      markEvidence: {
        status: "fresh_selected_market_observation",
        instrumentId,
        unitMark: { amount: "100", currency: "USD" },
        markKind: "last_trade",
        quality: "direct_verified",
        sourceId: "selected-market-source",
        observationDigest: evidence,
        observedAtUnixNanos: "1800000002000000000",
        availableAtUnixNanos: "1800000002000000001",
        freshUntilUnixNanosExclusive: "1800000007000000000",
        evaluatedAtUnixNanos: "1800000002000000002",
        portfolioRevisionId,
        selection: {
          instrumentId,
          sourceId: "selected-market-source",
          policyRevision: 1,
          policyDigest: evidence,
          receiptDigest: evidence,
          sourceStateRevision: "8",
          selectedAtUnixNanos: "1800000002000000001",
        },
      },
      availability: {
        portfolioWideSelectedMarks: {
          status: "unavailable",
          reason: "portfolio_wide_selected_market_marks",
        },
        liquidity: {
          status: "unavailable",
          reason: "exact_selected_source_liquidity",
        },
        settlementBackedSizing: {
          status: "unavailable",
          reason: "settlement_backed_sizing",
        },
        factorClassification: {
          status: "unavailable",
          reason: "exact_factor_classification",
        },
      },
      riskAdvisory: {
        outcome: "indeterminate_at_evaluation",
        evaluatedAtUnixNanos: "1800000002000000002",
        checksEvaluated: [
          "selected_account",
          "current_portfolio_revision",
          "fresh_selected_mark",
          "instrument_terms",
          "position_lot_alignment",
        ],
        checksUnavailable: [
          "portfolio_wide_selected_marks",
          "liquidity",
          "settlement_backed_sizing",
          "fees",
          "slippage",
        ],
        evidenceDigest: evidence,
        authority: "analysis_only",
        reservation: false,
        order: false,
      },
      authority: {
        analysisOnly: true,
        portfolioMutation: false,
        executionAuthority: false,
        riskAuthority: "analysis_only",
        reservation: false,
        order: false,
        riskApprovalRequiredBeforeAnyOrder: true,
      },
    },
    metadata: {
      completeness: "complete",
      returnedItems: 1,
      availableItems: 1,
      sourceCoverage: {
        status: "complete",
        providers: ["owned-portfolio-import", "selected-market-source"],
      },
      dataQuality: { status: "direct_verified" },
    },
  }
}

describe("Market Squawk desktop boundary", () => {
  it("keeps an instrument lookup bound to its exact Markets context", () => {
    const instrumentId = "7e8299e7-9757-4441-926f-d0b22c767a65"
    expect(
      lookupRoute({
        category: "instrument",
        id: instrumentId,
        label: "MSQ · nasdaq",
        detail: {},
        destination: { kind: "market_instrument", instrumentId },
      }),
    ).toBe(`/markets?instrumentId=${instrumentId}`)
  })

  it("renders one provider-neutral market journey with current price and depth", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Market.GetOverview",
          "market",
          "Return the current market overview.",
        ),
        datasetRead(
          "Market.GetInstrument",
          "market",
          "Return current information for one investment.",
        ),
      ],
    }
    render(
      <MemoryRouter initialEntries={["/markets"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            issuedQueries.push(request)
            if (request.query === "marketOverview") return marketOverviewResult
            if (request.query === "marketInstrument") return marketInstrumentResult
            throw new Error(`Unexpected market query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    const marketHeading = await screen.findByRole("heading", { name: "BTC-USD" })
    const marketCard = marketHeading.closest("button")
    expect(marketCard).toBeInstanceOf(HTMLButtonElement)
    if (!(marketCard instanceof HTMLButtonElement)) {
      throw new Error("The market card is absent")
    }

    expect(within(marketCard).getByText("Bitcoin")).toBeTruthy()
    expect(within(marketCard).getByText("Live")).toBeTruthy()
    expect(within(marketCard).getByText("68000.15 USD")).toBeTruthy()
    expect(within(marketCard).getByText("Individual orders")).toBeTruthy()

    await user.click(marketCard)
    expect(await screen.findByRole("heading", { name: "Market depth" })).toBeTruthy()
    expect(screen.getByText("Buy interest")).toBeTruthy()
    expect(screen.getByText("Sell interest")).toBeTruthy()
    expect(screen.getByText("2 individual orders are available.")).toBeTruthy()
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "marketInstrument"),
      ).toEqual([
        { query: "marketInstrument", instrumentId: marketInstrumentId },
      ])
    })
    expect(
      issuedQueries.some((request) => request.query === "marketOverview"),
    ).toBe(true)
    expect(
      issuedQueries.some((request) =>
        [
          "marketSnapshot",
          "marketQuality",
          "marketUnifiedFeed",
          "marketTrades",
          "marketQuotes",
          "marketBooks",
          "marketComparisons",
        ].includes(request.query),
      ),
    ).toBe(false)

    const renderedText = document.body.textContent ?? ""
    expect(renderedText).not.toMatch(/kraken|coinbase|websocket-v2/i)
    expect(renderedText).not.toContain(marketInstrumentId)
    expect(renderedText).not.toMatch(/\bticks?\b|\blots?\b/i)
  })

  it("renders one provider-neutral economic context with paired date cutoffs", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Research.ListDatasets",
          "research",
          "Return the local research collection.",
        ),
        localRead(
          "Macro.GetContext",
          "macro",
          "Return the economic context for the selected dates.",
        ),
      ],
    }
    render(
      <MemoryRouter initialEntries={["/advanced/research-data"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            issuedQueries.push(request)
            if (request.query === "macroContext") {
              return macroContextResult({
                knowledgeCutoff:
                  request.knowledgeCutoff ?? macroKnowledgeCutoff,
                effectiveDateCutoff:
                  request.effectiveDateCutoff ?? macroEffectiveDateCutoff,
              })
            }
            if (request.query === "researchDatasets" || request.query === "jobs") {
              return emptyRowsResult
            }
            throw new Error(`Unexpected research query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    const macroHeading = await screen.findByRole("heading", {
      name: "Rates and labor conditions",
    })
    const macroSection = macroHeading.closest("section")
    expect(macroSection).toBeInstanceOf(HTMLElement)
    if (!(macroSection instanceof HTMLElement)) {
      throw new Error("The economic context is absent")
    }
    expect(within(macroSection).getByText("12 of 12 available")).toBeTruthy()
    expect(
      within(macroSection)
        .getAllByRole("heading", { level: 4 })
        .map((heading) => heading.textContent),
    ).toEqual(macroIndicatorDefinitions.map(([, label]) => label))

    fireEvent.change(within(macroSection).getByLabelText("What was known by"), {
      target: { value: "2026-08-26T15:00:00Z" },
    })
    fireEvent.change(within(macroSection).getByLabelText("Use data through"), {
      target: { value: "2026-08-25" },
    })
    await user.click(
      within(macroSection).getByRole("button", { name: "Apply dates" }),
    )
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "macroContext"),
      ).toEqual([
        { query: "macroContext" },
        {
          query: "macroContext",
          knowledgeCutoff: "2026-08-26T15:00:00Z",
          effectiveDateCutoff: "2026-08-25",
        },
      ])
    })

    const renderedMacro = macroSection.textContent ?? ""
    expect(renderedMacro).not.toMatch(
      /Federal Reserve|FRED|ALFRED|H\.?15|Macro\.GetContext|\bprovider\b|\bsource\b|\bmanifest\b|\bdigest\b/i,
    )
  })

  it("keeps candidate impact server-resolved and visibly analysis-only", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Portfolio.EvaluateCandidateImpact",
          "portfolio",
          "Evaluate one server-resolved candidate impact.",
        ),
      ],
    }
    const candidateTransport = transport(
      readyBootstrap,
      undefined,
      async (request) => {
        issuedQueries.push(request)
        if (request.query === "portfolioCandidateImpact") {
          return candidateImpactResult(request.instrumentId)
        }
        throw new Error(`Unexpected candidate query: ${request.query}`)
      },
    )
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    })
    render(
      <QueryClientProvider client={queryClient}>
        <PortfolioPlanning
          account={portfolioAccount}
          holdings={[portfolioHolding]}
          bootstrap={readyBootstrap}
          transport={candidateTransport}
        />
      </QueryClientProvider>,
    )

    const instrument = screen.getByLabelText("Instrument ID")
    await user.clear(instrument)
    await user.type(instrument, candidateInstrumentId)
    const quantity = screen.getByLabelText(/^Proposed quantity/)
    await user.clear(quantity)
    await user.type(quantity, "3")
    await user.click(screen.getByRole("button", { name: "Compare impact" }))

    expect(await screen.findByText("Risk review remains incomplete")).toBeTruthy()
    expect(issuedQueries).toEqual([
      {
        query: "portfolioCandidateImpact",
        instrumentId: candidateInstrumentId,
        proposedQuantity: "3",
        scenarioShock: "-0.1",
      },
    ])
    expect(screen.getByText("New position")).toBeTruthy()
    expect(screen.getAllByText("USD 300")).toHaveLength(2)
    expect(screen.getByText("Settlement-backed sizing is unavailable.")).toBeTruthy()
    expect(screen.getAllByText("Unavailable")).toHaveLength(2)
    expect(
      screen.getByText(
        "Analysis only. No portfolio mutation, risk reservation, approval, or order was created.",
      ),
    ).toBeTruthy()
    expect(screen.queryByText("Projected cash")).toBeNull()
  })

  it("keeps fallback bootstrap native and enters the ready workspace only after reconnect", async () => {
    const user = userEvent.setup()
    let ready = false
    let submittedUnlock: string | null = null
    const bootstrapTransport = {
      ...transport(),
      bootstrap: async () =>
        ready
          ? blockedBootstrap
          : {
              status: "bootstrap_required" as const,
              requirement: "encrypted_fallback_locked" as const,
            },
      bootstrapService: async (request: {
        action: "unlock_encrypted_fallback"
        unlock: string
      }) => {
        submittedUnlock = request.unlock
        ready = true
      },
    } satisfies ProductTransport

    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={bootstrapTransport} />
      </MemoryRouter>,
    )

    const field = await screen.findByLabelText("Local security password")
    await user.type(field, "process-local-test-unlock")
    await user.click(screen.getByRole("button", { name: "Unlock secure storage" }))

    expect((field as HTMLInputElement).value).toBe("")
    expect(submittedUnlock).toBe("process-local-test-unlock")
    expect(
      await screen.findByRole("heading", { name: "Welcome to Market Squawk" }),
    ).toBeTruthy()
  })

  it("uses grouped product navigation to explore real research and AI connection state", async () => {
    const user = userEvent.setup()
    const issuedQueries: Parameters<ProductTransport["query"]>[0][] = []
    let alpacaStage: AlpacaSourceStage = "unconfigured"
    let corruptSecondaryEvidence = false
    const credentialProviders = [
      "schwab",
      "alpaca",
      "yahoo_finance_experimental",
      "nasdaq_trader_reference",
      "occ_options_reference",
      "cboe_options_reference",
      "iex_hist",
      "bls",
      "bea",
      "census",
      "eia",
      "fred_alfred",
      "tiingo",
      "sec",
      "treasury_fiscal_data",
      "treasury_daily_rates",
      "federal_reserve_board_direct",
    ]
    const credentialImportResult = {
      schema: "market-squawk-provider-credentials/v1",
      providers: credentialProviders.map((provider) => ({
        provider,
        enabled: provider === "alpaca",
        disposition:
          provider === "alpaca"
            ? "credential_stored_unverified"
            : "disabled",
        onboardingSessionId:
          provider === "alpaca"
            ? alpacaOnboardingSessionId
            : null,
      })),
    }
    const unsafeCredentialImportResult = {
      ...credentialImportResult,
      providers: credentialImportResult.providers.map((provider) =>
        provider.provider === "alpaca"
          ? { ...provider, secret: "should-never-reach-react" }
          : provider,
      ),
    }
    const credentialImportAttempts: unknown[] = [
      null,
      credentialImportResult,
      unsafeCredentialImportResult,
    ]
    let credentialImportAttempt = 0
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      providerProfiles: [h15ProviderProfile, alpacaProviderProfile],
      operations: [
        datasetRead(
          "Research.ListDatasets",
          "research",
          "Return the bounded local research dataset inventory.",
        ),
        datasetRead(
          "Research.GetManifest",
          "research",
          "Return one immutable analytical dataset manifest.",
        ),
        datasetRead(
          "Fundamental.GetFacts",
          "fundamental",
          "Return bounded reported fundamental facts.",
        ),
        datasetRead(
          "Macro.GetRevisions",
          "macro",
          "Return bounded macroeconomic revision history.",
        ),
        sourceStatusRead(),
        {
          name: "Source.ImportCredentialBundle",
          description: "Import one protected provider credential bundle.",
          domain: "source",
          authorization: "local_confirmation",
          readOnly: false,
          destructive: false,
          inputSchema: {
            type: "object",
            properties: {
              inputTicketId: { type: "string", format: "uuid" },
              confirm: { const: true },
            },
            required: ["inputTicketId", "confirm"],
            additionalProperties: false,
          },
        },
        investmentAnalysisListRead(),
        investmentAnalysisRead(),
        recommendationTrackRecordRead(),
        localRead(
          "Bot.GetStatus",
          "bot",
          "Return controlled paper-operation lifecycle and risk status.",
        ),
        localRead(
          "Execution.GetOrders",
          "execution",
          "Return bounded paper orders and state transitions.",
        ),
        localRead(
          "Execution.GetFills",
          "execution",
          "Return bounded paper fills.",
        ),
      ],
    }
    let currentBootstrap = readyBootstrap
    let admittedServiceGeneration = readyBootstrap.runtime.serviceGeneration
    const issuedBootstrapGenerations: number[] = []
    const reconnectRequests: DesktopBootstrap["runtime"][] = []
    let retainedGenerationUnavailable = false
    const desktopEvents: {
      listener: ((event: DesktopEvent) => void) | null
      protocolError: ((error: Error) => void) | null
    } = { listener: null, protocolError: null }
    const eventSubscriptions: DesktopEventSubscriptionRequest[] = []
    const releasedEventSequences: string[] = []
    const sourceControls: Array<{
      action: Parameters<ProductTransport["sourceControl"]>[0]
      request: Parameters<ProductTransport["sourceControl"]>[1]
    }> = []
    let admitInitialEventSubscription!: () => void
    const initialEventSubscriptionAdmission = new Promise<void>((resolve) => {
      admitInitialEventSubscription = resolve
    })
    let admitReplacementEventSubscription!: () => void
    const replacementEventSubscriptionAdmission = new Promise<void>((resolve) => {
      admitReplacementEventSubscription = resolve
    })
    const dashboardTransport: ProductTransport = {
      ...transport(readyBootstrap, undefined, async (request) => {
        issuedQueries.push(request)
        if (request.query === "sourceStatus") {
          return request.sourceIds?.includes("alpaca.basic-market-data") === true
            ? alpacaSourceStatus(alpacaStage)
            : inactiveH15SourceStatus
        }
        if (request.query === "paperStatus") return stoppedPaperStatus
        if (request.query === "sourceCoverage") {
          const result = sourceCoverageResult()
          if (corruptSecondaryEvidence && Array.isArray(result.data)) {
            const first = result.data[0] as Record<string, unknown> | undefined
            if (first) first.surfaceId = "alpaca.basic-market-data"
          }
          return result
        }
        if (request.query === "sourceHealth") return sourceHealthResult(alpacaStage)
        if (
          request.query === "paperOrders" ||
          request.query === "paperFills" ||
          request.query === "portfolioAccounts"
        ) {
          return emptyRowsResult
        }
        if (request.query === "decisionInvestmentAnalyses") {
          return retainedInvestmentAnalysisPage
        }
        if (request.query === "decisionInvestmentAnalysis") {
          return retainedInvestmentAnalysis
        }
        if (request.query === "decisionRecommendationTrackRecord") {
          return retainedTrackRecord(request)
        }
        return {
          data: null,
          metadata: {
            completeness: "complete",
            returnedItems: 0,
            availableItems: 0,
            sourceCoverage: { status: "not_applicable" },
            dataQuality: { status: "not_applicable" },
          },
        }
      }),
      bootstrap: async () => {
        if (retainedGenerationUnavailable) {
          throw new Error("The retained service generation is unavailable")
        }
        admittedServiceGeneration = currentBootstrap.runtime.serviceGeneration
        issuedBootstrapGenerations.push(admittedServiceGeneration)
        return currentBootstrap
      },
      reconnectService: async (expectedRuntime) => {
        reconnectRequests.push(expectedRuntime)
        retainedGenerationUnavailable = false
        admittedServiceGeneration = currentBootstrap.runtime.serviceGeneration
        return currentBootstrap
      },
      subscribe: async (request, listener, onProtocolError) => {
        eventSubscriptions.push(request)
        desktopEvents.listener = listener
        desktopEvents.protocolError = onProtocolError
        if (eventSubscriptions.length === 1) {
          await initialEventSubscriptionAdmission
        } else if (request.runtime.serviceGeneration === 3) {
          await replacementEventSubscriptionAdmission
        }
        return {
          receipt: {
            subscriptionId: `f49e02f6-8c47-43a5-bb33-${String(eventSubscriptions.length).padStart(12, "0")}`,
            runtime: request.runtime,
            sequence: request.afterSequence,
            resumed: request.afterSequence !== "0",
          },
          unsubscribe: async () => {
            releasedEventSequences.push(request.afterSequence)
            if (desktopEvents.listener === listener) desktopEvents.listener = null
            if (desktopEvents.protocolError === onProtocolError) {
              desktopEvents.protocolError = null
            }
          },
        }
      },
      importProviderCredentialBundle: async () => {
        const attempt = credentialImportAttempt
        const result = credentialImportAttempts[attempt] ?? null
        credentialImportAttempt += 1
        if (attempt === 1) alpacaStage = "doctor_required"
        return result
      },
      sourceControl: async (action, request) => {
        sourceControls.push({ action, request })
        if (request.provider === "alpaca.basic-market-data") {
          if (action === "verify") {
            const renewing = alpacaStage === "active" ||
              alpacaStage === "resynchronized" ||
              alpacaStage === "reactivated"
            if (renewing && alpacaDoctorPredecessors.get(
              renewedAlpacaDoctorEvidence.receiptSha256,
            ) !== initialAlpacaDoctorEvidence.receiptSha256) {
              throw new Error("The renewed doctor fixture lost its exact predecessor.")
            }
            alpacaStage = renewing ? "renewed" : "eligible"
          }
          if (action === "resynchronize") alpacaStage = "resynchronized"
          if (action === "start") {
            alpacaStage = alpacaStage === "renewed" ? "reactivated" : "active"
          }
        }
        return alpacaLifecycleResult(action, request, alpacaStage)
      },
    }
    const readCount = (
      query: Parameters<ProductTransport["query"]>[0]["query"],
    ) => issuedQueries.filter((request) => request.query === query).length
    const notifyAuthorityChanged = async (
      sequence: string,
      domain: string,
      operation: string,
    ) => {
      const listener = desktopEvents.listener
      if (!listener) throw new Error("The Desktop event subscription was not installed")
      await act(async () => {
        listener({
          runtime: currentBootstrap.runtime,
          sequence,
          body: {
            type: "authority_changed",
            domain,
            operation,
            requestId: `wave-12c-${domain}-refresh`,
          },
        })
      })
    }
    const rendered = render(
      <MemoryRouter initialEntries={["/advanced/research-data"]}>
        <App transport={dashboardTransport} />
      </MemoryRouter>,
    )

    await waitFor(() => expect(eventSubscriptions).toHaveLength(1))
    expect(screen.queryByRole("heading", { name: "Research" })).toBeNull()
    expect(readCount("sourceStatus")).toBe(0)
    await act(async () => {
      admitInitialEventSubscription()
      await initialEventSubscriptionAdmission
    })

    const heading = await screen.findByRole("heading", { name: "Research" })
    expect(heading.tagName).toBe("H1")
    const workspaceHome = screen.getByRole("link", {
      name: "Market Squawk workspace",
    })
    expect({
      centered: workspaceHome.classList.contains("justify-center"),
      leftAligned: workspaceHome.classList.contains("justify-start"),
    }).toEqual({ centered: true, leftAligned: false })
    const marketWord = within(workspaceHome).getByText("Market")
    const quawkWord = within(workspaceHome).getByText("quawk")
    const marketSquawkMark = workspaceHome.querySelector('img[alt=""]')
    expect(marketSquawkMark).toBeInstanceOf(HTMLImageElement)
    if (!(marketSquawkMark instanceof HTMLImageElement)) {
      throw new Error("Market Squawk mark is absent")
    }
    expect({
      marketText: marketWord.classList.contains("text-[18px]"),
      markHeight: marketSquawkMark.classList.contains("h-[21px]"),
      squawkText: quawkWord.classList.contains("text-[18px]"),
    }).toEqual({ marketText: true, markHeight: true, squawkText: true })
    expect(quawkWord.style.marginLeft).toBe("-1px")
    const markDocument = new DOMParser().parseFromString(
      marketSquawkMarkSvg,
      "image/svg+xml",
    )
    const whiteBird = markDocument.querySelector('path[fill="#fafafa"]')
    const whiteBirdPath = whiteBird?.getAttribute("d") ?? ""
    expect({
      fillRule: whiteBird?.getAttribute("fill-rule") ?? null,
      contours: whiteBirdPath.match(/[Mm]/g)?.length ?? 0,
    }).toEqual({ fillRule: null, contours: 1 })
    const navigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(navigation).toBeTruthy()
    if (!(navigation instanceof HTMLElement)) {
      throw new Error("Market Squawk navigation is absent")
    }
    expect(navigation.querySelectorAll("a,button")).toHaveLength(18)
    expect(within(navigation).getByText("Everyday")).toBeTruthy()
    expect(within(navigation).getByText("Advanced", { exact: true })).toBeTruthy()
    expect(within(navigation).getByText("Connections & System")).toBeTruthy()
    expect(within(navigation).queryByRole("link", { name: "Lookup" })).toBeNull()
    expect(
      await within(navigation).findByRole("link", { name: "Opportunities" }),
    ).toBeTruthy()
    const paperExecution = await within(navigation).findByRole("link", {
      name: "Paper Execution",
    })
    expect(paperExecution.getAttribute("aria-disabled")).toBeNull()
    expect(
      await within(navigation).findByRole("link", {
        name: "Backup & Recovery",
      }),
    ).toBeTruthy()
    expect(await screen.findByText("No analytical datasets yet")).toBeTruthy()
    const notifyDesktopEvent = desktopEvents.listener
    if (!notifyDesktopEvent) {
      throw new Error("The Desktop event subscription was not installed")
    }
    currentBootstrap = {
      ...readyBootstrap,
      runtime: {
        ...readyBootstrap.runtime,
        serviceGeneration: 2,
      },
    }
    await act(async () => {
      notifyDesktopEvent({
        runtime: readyBootstrap.runtime,
        sequence: "1",
        body: {
          type: "resync_required",
          reason: "service_event_stream_changed",
        },
      })
    })
    await waitFor(() => expect(issuedBootstrapGenerations).toContain(2))

    const refreshedNavigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(refreshedNavigation).toBeInstanceOf(HTMLElement)
    if (!(refreshedNavigation instanceof HTMLElement)) {
      throw new Error("The refreshed Market Squawk navigation is absent")
    }
    fireEvent.click(
      within(refreshedNavigation).getByRole("link", {
        name: "Connections & Sources",
      }),
    )
    expect(await screen.findByRole("heading", { name: "Sources" })).toBeTruthy()
    const alpacaSourceHeading = await screen.findByRole("heading", {
      name: "Alpaca Basic Market Data",
    })
    const alpacaSource = alpacaSourceHeading.closest("article")
    expect(alpacaSource).toBeInstanceOf(HTMLElement)
    if (!(alpacaSource instanceof HTMLElement)) {
      throw new Error("The Alpaca source evidence is absent")
    }
    expect(within(alpacaSource).getByRole("link", { name: "Set up again" })).toBeTruthy()
    expect(within(alpacaSource).queryByRole("button", { name: "Run doctor" })).toBeNull()
    const chooseCredentialBundle = screen.getByRole("button", {
      name: "Choose credential bundle",
    })
    await user.click(chooseCredentialBundle)
    expect(
      await screen.findByText(
        "No credential bundle was selected. Provider setup is unchanged.",
      ),
    ).toBeTruthy()
    const sourceReadsBeforeCredentialImport = readCount("sourceStatus")
    await user.click(chooseCredentialBundle)
    expect(await screen.findByText("Credential bundle processed")).toBeTruthy()
    expect(
      screen.getByText(
        "Credential stored; verification and activation are still required.",
      ),
    ).toBeTruthy()
    await waitFor(() => {
      expect(readCount("sourceStatus")).toBeGreaterThan(sourceReadsBeforeCredentialImport)
      expect(within(alpacaSource).getByRole("button", { name: "Run doctor" })).toBeTruthy()
    })
    expect(within(alpacaSource).queryByRole("link", { name: "Set up again" })).toBeNull()
    const sourceReadsBeforeFailedCredentialImport = readCount("sourceStatus")
    await user.click(
      screen.getByRole("button", { name: "Select another bundle" }),
    )
    expect(
      await screen.findByText("Credential import needs attention"),
    ).toBeTruthy()
    expect(
      screen.getByText(
        "Market Squawk could not complete this credential bundle. One or more earlier entries may already have been stored. Source evidence is refreshing; review it before correcting the file or service issue and trying again.",
      ),
    ).toBeTruthy()
    await waitFor(() => {
      expect(readCount("sourceStatus")).toBeGreaterThan(
        sourceReadsBeforeFailedCredentialImport,
      )
    })
    expect(screen.queryByText("should-never-reach-react")).toBeNull()
    const boardSourceHeading = await screen.findByRole("heading", {
      name: "Federal Reserve Board Data Download Program",
    })
    const boardSource = boardSourceHeading.closest("article")
    expect(boardSource).toBeInstanceOf(HTMLElement)
    if (!(boardSource instanceof HTMLElement)) {
      throw new Error("The Federal Reserve Board source evidence is absent")
    }
    const lifecycleObserved = within(boardSource)
      .getByText("Lifecycle observed")
      .closest("div")
    const runtimeObserved = within(boardSource)
      .getByText("Runtime observed")
      .closest("div")
    expect(lifecycleObserved?.querySelector("dd")?.textContent).toBe(
      new Date("2026-08-11T20:31:00Z").toLocaleString(),
    )
    expect(runtimeObserved?.querySelector("dd")?.textContent).toBe("Not reported")

    expect(within(alpacaSource).getByText("Doctor required")).toBeTruthy()
    expect(within(alpacaSource).queryByText("Alpaca Paper / IEX doctor evidence")).toBeNull()
    await user.click(within(alpacaSource).getByRole("button", { name: "Run doctor" }))
    expect(await within(alpacaSource).findByText("Ready to start")).toBeTruthy()
    expect(
      within(alpacaSource).getByText("Alpaca Paper / IEX doctor evidence"),
    ).toBeTruthy()
    expect(within(alpacaSource).getByText("Current receipt")).toBeTruthy()
    expect(within(alpacaSource).getByText(initialAlpacaDoctorReceiptSha256)).toBeTruthy()
    await user.click(within(alpacaSource).getByText("Exact authority evidence"))
    expect(
      within(alpacaSource).getByText(
        "market-squawk.alpaca-paper-iex-doctor-implementation.v2",
      ),
    ).toBeTruthy()
    expect(
      within(alpacaSource).getByText(
        "ed8ab1614fc4cee29b213b7eed8ce59033e0041378039f51368ad872bfe3a911",
      ),
    ).toBeTruthy()
    expect(
      within(alpacaSource).getByText("50/50 valid · 0 missing", { exact: false }),
    ).toBeTruthy()
    expect(
      within(alpacaSource).getByText(
        "Provider headers: limit 200 · remaining 195 · reset 1786567800 · retry-after missing",
      ),
    ).toBeTruthy()
    expect(
      within(alpacaSource).getByText(
        "Provider headers: limit missing · remaining missing · reset missing · retry-after missing",
      ),
    ).toBeTruthy()
    expect(
      within(alpacaSource).getByText(
        "Market-data credential principal only; no brokerage account, positions, orders, execution, or trading authority.",
      ),
    ).toBeTruthy()
    expect(sourceControls[0]).toEqual({
      action: "verify",
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "8",
        onboardingSessionId: alpacaOnboardingSessionId,
        publicConfigurationSha256: alpacaPublicConfigurationSha256,
      },
    })
    await user.click(within(alpacaSource).getByRole("button", { name: "Start" }))
    await waitFor(() => {
      expect(within(alpacaSource).getByText("Active account/display runtime")).toBeTruthy()
    })
    expect(within(alpacaSource).getByText("Already active")).toBeTruthy()
    expect(within(alpacaSource).queryByRole("button", { name: "Start" })).toBeNull()
    expect(sourceControls[1]).toEqual({
      action: "start",
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "9",
        onboardingSessionId: alpacaOnboardingSessionId,
        publicConfigurationSha256: alpacaPublicConfigurationSha256,
      },
    })
    const initialRuntimeGeneration = within(alpacaSource)
      .getByText("Runtime generation SHA-256")
      .closest("div")
    expect(initialRuntimeGeneration?.querySelector("dd")?.textContent).toBe(
      initialAlpacaRuntimeGenerationSha256,
    )
    await user.click(within(alpacaSource).getByRole("button", { name: "Resynchronize" }))
    expect(sourceControls[2]).toEqual({
      action: "resynchronize",
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "10",
        expectedRuntimeGenerationSha256: initialAlpacaRuntimeGenerationSha256,
        reason: "desktop-user-request",
      },
    })
    await waitFor(() => {
      const generation = within(alpacaSource)
        .getByText("Runtime generation SHA-256")
        .closest("div")
      expect(generation?.querySelector("dd")?.textContent).toBe(
        resynchronizedAlpacaRuntimeGenerationSha256,
      )
    })
    const alpacaSourceReadsBeforeDoctorRenewal = issuedQueries.filter(
      (request) => request.query === "sourceStatus" &&
        request.sourceIds?.includes("alpaca.basic-market-data") === true,
    ).length
    const sourceReadsBeforeDoctorRenewal = readCount("sourceStatus")
    await user.click(
      within(alpacaSource).getByRole("button", { name: "Renew doctor and stop source" }),
    )
    expect(
      await screen.findByText(
        "Running or renewing the Paper/IEX doctor stops any retained source runtime, including one currently reported as blocked. Starting it again remains a separate explicit action.",
        { exact: false },
      ),
    ).toBeTruthy()
    await user.click(screen.getByRole("button", { name: "Confirm change" }))
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) => request.query === "sourceStatus" &&
          request.sourceIds?.includes("alpaca.basic-market-data") === true).length,
      ).toBeGreaterThan(alpacaSourceReadsBeforeDoctorRenewal)
    })
    expect(readCount("sourceStatus")).toBeGreaterThan(sourceReadsBeforeDoctorRenewal)
    expect(within(alpacaSource).getByText("Ready to start")).toBeTruthy()
    expect(within(alpacaSource).getByRole("button", { name: "Start" })).toBeTruthy()
    expect(within(alpacaSource).getByText(renewedAlpacaDoctorReceiptSha256)).toBeTruthy()
    expect(within(alpacaSource).queryByText(initialAlpacaDoctorReceiptSha256)).toBeNull()
    const stoppedRuntimeGeneration = within(alpacaSource)
      .getByText("Runtime generation SHA-256")
      .closest("div")
    expect(stoppedRuntimeGeneration?.querySelector("dd")?.textContent).toBe("Not reported")
    expect(sourceControls[3]).toEqual({
      action: "verify",
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "11",
        onboardingSessionId: alpacaOnboardingSessionId,
        publicConfigurationSha256: alpacaPublicConfigurationSha256,
      },
    })
    await user.click(within(alpacaSource).getByRole("button", { name: "Start" }))
    await waitFor(() => {
      expect(within(alpacaSource).getByText("Active account/display runtime")).toBeTruthy()
    })
    expect(within(alpacaSource).getByText("Already active")).toBeTruthy()
    expect(sourceControls[4]).toEqual({
      action: "start",
      request: {
        provider: "alpaca.basic-market-data",
        expectedStateRevision: "12",
        onboardingSessionId: alpacaOnboardingSessionId,
        publicConfigurationSha256: alpacaPublicConfigurationSha256,
      },
    })
    const reactivatedRuntimeGeneration = within(alpacaSource)
      .getByText("Runtime generation SHA-256")
      .closest("div")
    expect(reactivatedRuntimeGeneration?.querySelector("dd")?.textContent).toBe(
      reactivatedAlpacaRuntimeGenerationSha256,
    )
    expect(reactivatedRuntimeGeneration?.querySelector("dd")?.textContent).not.toBe(
      resynchronizedAlpacaRuntimeGenerationSha256,
    )

    await waitFor(() => {
      expect(readCount("sourceCoverage")).toBeGreaterThan(0)
      expect(readCount("sourceHealth")).toBeGreaterThan(0)
    })
    const exactStatuses = groupedSourceStatuses(alpacaStage)
    expect(
      parseSourceCoverageResult(
        sourceCoverageResult(),
        readyBootstrap.providerProfiles,
        exactStatuses,
      ),
    ).toHaveLength(2)
    expect(
      parseSourceHealthResult(
        sourceHealthResult(alpacaStage),
        readyBootstrap.providerProfiles,
        exactStatuses,
      ),
    ).toHaveLength(2)

    const duplicateCoverage = structuredClone(sourceCoverageResult())
    const duplicateCoverageRows = duplicateCoverage.data as Array<Record<string, unknown>>
    if (!duplicateCoverageRows[0] || !duplicateCoverageRows[1]) {
      throw new Error("The source coverage fixture is absent")
    }
    duplicateCoverageRows[1] = { ...duplicateCoverageRows[0] }
    expect(() =>
      parseSourceCoverageResult(
        duplicateCoverage,
        readyBootstrap.providerProfiles,
        exactStatuses,
      )
    ).toThrow()

    const crossProfileHealth = structuredClone(sourceHealthResult(alpacaStage))
    const crossProfileHealthRows = crossProfileHealth.data as Array<Record<string, unknown>>
    if (!crossProfileHealthRows[0]) {
      throw new Error("The source health fixture is absent")
    }
    crossProfileHealthRows[0].surfaceId = "alpaca.basic-market-data"
    expect(() =>
      parseSourceHealthResult(
        crossProfileHealth,
        readyBootstrap.providerProfiles,
        exactStatuses,
      )
    ).toThrow()

    const activeGroupHealth = structuredClone(sourceHealthResult(alpacaStage))
    const activeGroupHealthRows = activeGroupHealth.data as Array<Record<string, unknown>>
    if (!activeGroupHealthRows[1]) {
      throw new Error("The Alpaca health fixture is absent")
    }
    activeGroupHealthRows[1].runtimeHealth = {
      state: "active",
      sourceId: "alpaca-iex-runtime",
      venueId: "iex",
      instrumentId: marketInstrumentId,
      connectionGeneration: "1",
      sessionId: "alpaca-runtime-session",
      healthEpoch: "1",
      stateRevision: "1",
      assessmentId: "alpaca-runtime-assessment",
      bindingDigest: "d".repeat(64),
      connection: "connecting",
      transportFreshness: "uninitialized",
      marketFreshness: "uninitialized",
      sourceTimestampFreshness: "uninitialized",
      streamIntegrity: "initializing",
      captureIntegrity: "disabled",
      coverageStatus: "unknown",
      quality: "direct_unverified",
      observedAtUnixNanos: "1786564800000000000",
      qualificationEvaluatedAtUnixNanos: "1786564800000000000",
      qualificationValidUntilUnixNanos: "1786564810000000000",
    }
    expect(() =>
      parseSourceHealthResult(
        activeGroupHealth,
        readyBootstrap.providerProfiles,
        exactStatuses,
      )
    ).toThrow()

    const mismatchedSourceCounts = structuredClone(sourceCoverageResult())
    const mismatchedCoverage = mismatchedSourceCounts.metadata.sourceCoverage as
      Record<string, unknown>
    mismatchedCoverage.profileCount = 1
    expect(() =>
      parseSourceCoverageResult(
        mismatchedSourceCounts,
        readyBootstrap.providerProfiles,
        exactStatuses,
      )
    ).toThrow()

    const sourceReadsBeforeSourceInvalidation = readCount("sourceStatus")
    const coverageReadsBeforeSourceInvalidation = readCount("sourceCoverage")
    const healthReadsBeforeSourceInvalidation = readCount("sourceHealth")
    corruptSecondaryEvidence = true
    await notifyAuthorityChanged("2", "source", "Source.Setup")
    await waitFor(() => {
      expect(readCount("sourceStatus")).toBeGreaterThan(
        sourceReadsBeforeSourceInvalidation,
      )
      expect(readCount("sourceCoverage")).toBeGreaterThan(
        coverageReadsBeforeSourceInvalidation,
      )
      expect(readCount("sourceHealth")).toBeGreaterThan(
        healthReadsBeforeSourceInvalidation,
      )
    })
    expect(
      await screen.findByText(/source evidence reads could not be completed/),
    ).toBeTruthy()
    const runtimeSource = within(alpacaSource)
      .getByText("Runtime source")
      .closest("div")
    const marketFreshness = within(alpacaSource)
      .getByText("Market freshness")
      .closest("div")
    expect(within(alpacaSource).getByText("Operational")).toBeTruthy()
    expect(runtimeSource?.querySelector("dd")?.textContent).toBe("Not reported")
    expect(marketFreshness?.querySelector("dd")?.textContent).toBe("Not reported")

    await user.click(
      within(refreshedNavigation).getByRole("link", { name: "Opportunities" }),
    )
    expect(
      await screen.findByRole("heading", { name: "Opportunities" }),
    ).toBeTruthy()
    const retainedAnalysisCard = await screen.findByRole("button", {
      name: /Generated · Buy/,
    })
    await user.click(retainedAnalysisCard)
    const briefHeading = await screen.findByRole("heading", {
      name: "Investment Brief",
    })
    const brief = briefHeading.closest("section")
    expect(brief).toBeInstanceOf(HTMLElement)
    if (!(brief instanceof HTMLElement)) {
      throw new Error("The exact retained Investment Brief is absent")
    }
    expect(within(brief).getByText("Research only — execution ineligible")).toBeTruthy()
    expect(within(brief).getByText("Buy analysis generated")).toBeTruthy()
    expect(within(brief).getByText("Gross outcome projection")).toBeTruthy()
    expect(within(brief).getByText("Sizing feasibility — no selected target")).toBeTruthy()
    expect(within(brief).getByText("Not selected")).toBeTruthy()
    expect(within(brief).getByText("Not created")).toBeTruthy()
    expect(within(brief).getByText("Current realized-outcome status")).toBeTruthy()
    expect(within(brief).getByText("Gross price-return decimal")).toBeTruthy()
    expect(within(brief).getByText("0.1")).toBeTruthy()
    expect(
      within(brief).getByText("Profile-bound recommendation track record"),
    ).toBeTruthy()
    expect(
      await within(brief).findByText(
        "Mean gross price-return decimal 0.08; 24 positive, 1 zero, 5 negative",
      ),
    ).toBeTruthy()
    expect(
      within(brief).getByText(
        "Current-status outcomes for the exact published analytical profile and recommendation horizon. Cohorts remain separate; the service owns sample gates, coverage, and mean-return arithmetic. Forecast calibration and execution performance are not included.",
      ),
    ).toBeTruthy()

    expect(
      issuedQueries.filter(
        (request) => request.query === "decisionInvestmentAnalyses",
      ),
    ).toEqual([{ query: "decisionInvestmentAnalyses", limit: 24 }])
    expect(
      issuedQueries.filter(
        (request) => request.query === "decisionInvestmentAnalysis",
      ),
    ).toEqual([
      { query: "decisionInvestmentAnalysis", analysisId: retainedAnalysisId },
    ])
    const trackRecordRequests = issuedQueries.filter(
      (request): request is TrackRecordQuery =>
        request.query === "decisionRecommendationTrackRecord",
    )
    expect(trackRecordRequests).toHaveLength(1)
    expect(trackRecordRequests[0]).toEqual({
      query: "decisionRecommendationTrackRecord",
      profileId: "market-squawk-default-v1",
      profileRevision: 1,
      profileDigest: retainedProfileDigest,
      horizonNanos: retainedHorizonNanos,
      evaluatedAtUnixNanos: expect.stringMatching(/^[1-9]\d*$/),
    })

    const decisionReadsBeforeInvalidation = issuedQueries.filter((request) =>
      request.query.startsWith("decisionInvestment"),
    ).length
    const trackReadsBeforeInvalidation = trackRecordRequests.length
    await notifyAuthorityChanged("3", "decision", "Decision.ReviewTargetSet")
    await waitFor(() => {
      expect(
        issuedQueries.filter((request) =>
          request.query.startsWith("decisionInvestment"),
        ).length,
      ).toBeGreaterThan(decisionReadsBeforeInvalidation)
      expect(
        issuedQueries.filter(
          (request) => request.query === "decisionRecommendationTrackRecord",
        ).length,
      ).toBeGreaterThan(trackReadsBeforeInvalidation)
    })

    await user.click(
      within(refreshedNavigation).getByRole("link", { name: "Paper Execution" }),
    )
    expect(
      await screen.findByRole("heading", { name: "Paper Execution" }),
    ).toBeTruthy()
    await waitFor(() => {
      expect(readCount("paperStatus")).toBeGreaterThan(0)
      expect(readCount("paperOrders")).toBeGreaterThan(0)
      expect(readCount("paperFills")).toBeGreaterThan(0)
    })
    const statusReadsBeforeBotInvalidation = readCount("paperStatus")
    const orderReadsBeforeBotInvalidation = readCount("paperOrders")
    const fillReadsBeforeBotInvalidation = readCount("paperFills")
    await notifyAuthorityChanged("4", "bot", "Bot.Stop")
    await waitFor(() => {
      expect(readCount("paperStatus")).toBeGreaterThan(
        statusReadsBeforeBotInvalidation,
      )
    })
    expect(readCount("paperOrders")).toBe(orderReadsBeforeBotInvalidation)
    expect(readCount("paperFills")).toBe(fillReadsBeforeBotInvalidation)

    const statusReadsBeforeExecutionInvalidation = readCount("paperStatus")
    const orderReadsBeforeExecutionInvalidation = readCount("paperOrders")
    const fillReadsBeforeExecutionInvalidation = readCount("paperFills")
    await notifyAuthorityChanged("5", "execution", "Execution.Reconcile")
    await waitFor(() => {
      expect(readCount("paperOrders")).toBeGreaterThan(
        orderReadsBeforeExecutionInvalidation,
      )
      expect(readCount("paperFills")).toBeGreaterThan(
        fillReadsBeforeExecutionInvalidation,
      )
    })
    expect(readCount("paperStatus")).toBe(statusReadsBeforeExecutionInvalidation)

    const paperStatusReadsBeforeRisk = readCount("paperStatus")
    const paperOrderReadsBeforeRisk = readCount("paperOrders")
    await user.click(
      within(refreshedNavigation).getByRole("link", {
        name: "Risk & Recommendation Policy",
      }),
    )
    expect(await screen.findByRole("heading", { name: "Risk" })).toBeTruthy()
    expect(await screen.findByText("No portfolio risk is available")).toBeTruthy()
    expect(readCount("paperStatus")).toBe(paperStatusReadsBeforeRisk)
    expect(readCount("paperOrders")).toBe(paperOrderReadsBeforeRisk)

    const accountReadsBeforePortfolioInvalidation = readCount("portfolioAccounts")
    await notifyAuthorityChanged(
      "6",
      "portfolio",
      "Portfolio.CommitRecommendationSetup",
    )
    await waitFor(() => {
      expect(readCount("portfolioAccounts")).toBeGreaterThan(
        accountReadsBeforePortfolioInvalidation,
      )
    })

    const subscriptionsBeforeDisconnect = eventSubscriptions.length
    const activeEventListener = desktopEvents.listener
    if (!activeEventListener) {
      throw new Error("The resumable Desktop event subscription is absent")
    }
    await act(async () => {
      activeEventListener({
        runtime: currentBootstrap.runtime,
        sequence: "6",
        body: {
          type: "stream_disconnected",
          reason: "service_event_stream_unavailable",
        },
      })
    })
    expect(screen.getByText("Retry 1/5")).toBeTruthy()
    await waitFor(() => {
      expect(eventSubscriptions.length).toBeGreaterThan(
        subscriptionsBeforeDisconnect,
      )
    })
    expect(eventSubscriptions.at(-1)).toEqual({
      runtime: currentBootstrap.runtime,
      afterSequence: "6",
    })
    expect(releasedEventSequences).toContain("0")
    expect(await screen.findByText("Connected")).toBeTruthy()

    const reportProtocolError = desktopEvents.protocolError
    if (!reportProtocolError) {
      throw new Error("The Desktop event protocol failure handler is absent")
    }
    currentBootstrap = {
      ...readyBootstrap,
      runtime: {
        ...readyBootstrap.runtime,
        serviceGeneration: 3,
      },
    }
    retainedGenerationUnavailable = true
    await act(async () => {
      reportProtocolError(new Error("Malformed Desktop event"))
    })
    await waitFor(
      () => {
        expect(reconnectRequests).toEqual([
          {
            ...readyBootstrap.runtime,
            serviceGeneration: 2,
          },
        ])
        expect(eventSubscriptions.at(-1)).toEqual({
          runtime: currentBootstrap.runtime,
          afterSequence: "0",
        })
      },
      { timeout: 3_000 },
    )
    const handoffNavigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(handoffNavigation?.getAttribute("aria-disabled")).toBe("true")
    expect(handoffNavigation?.querySelectorAll("a")).toHaveLength(0)
    expect(
      screen.queryByRole("link", { name: "Market Squawk workspace" }),
    ).toBeNull()
    expect(
      (
        screen.getByRole("button", {
          name: "Search or run a command",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true)
    expect(screen.queryByRole("heading", { name: "Risk" })).toBeNull()
    await act(async () => {
      admitReplacementEventSubscription()
      await replacementEventSubscriptionAdmission
    })
    expect(await screen.findByText("Connected")).toBeTruthy()

    rendered.unmount()
    const mcpRendered = render(
      <MemoryRouter initialEntries={["/system/ai-connections"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(await screen.findByText("One authenticated local endpoint")).toBeTruthy()
    expect(screen.getByText("Claude Code and Codex")).toBeTruthy()
    expect(screen.getByText("stateless request sessions", { exact: false })).toBeTruthy()

    mcpRendered.unmount()
    const updatesRendered = render(
      <MemoryRouter initialEntries={["/system/updates-repair"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(
      await screen.findByRole("heading", { name: "Updates & program recovery" }),
    ).toBeTruthy()

    updatesRendered.unmount()
    const setupSessionId = "8fdbfe5d-aee2-4a42-8573-dafdf582cab1"
    const needsCredential: ProviderSession = {
      session_id: setupSessionId,
      surface_id: alpacaProviderProfile.id,
      state: "user_action_required",
      next_action: "import_secret",
      credential_stored: false,
    }
    const readyToActivate: ProviderSession = {
      ...needsCredential,
      state: "stored_unverified",
      next_action: "verify_and_activate",
      credential_stored: true,
    }
    const authoritativeActive: ProviderSession = {
      ...readyToActivate,
      state: "active_scoped",
      next_action: "active",
    }
    const onboardingRequests: Parameters<ProductTransport["onboard"]>[0][] = []
    let setupRefreshes = 0
    const setupTransport = transport(
      blockedBootstrap,
      (async (request) => {
        onboardingRequests.push(request)
        if (request.action === "submitSecret") return readyToActivate
        if (request.action === "activate") {
          return {
            profile: alpacaProviderProfile.id,
            session_id: setupSessionId,
            capability_revision: 4,
          }
        }
        throw new Error("Unexpected setup action")
      }) as ProductTransport["onboard"],
    )
    const providerSetup = render(
      <ProviderStep
        profiles={[alpacaProviderProfile]}
        sessions={[needsCredential]}
        transport={setupTransport}
        onChanged={() => {
          setupRefreshes += 1
        }}
      />,
    )
    await user.type(screen.getByLabelText("Provider API key"), "paper-fixture-key")
    await user.click(screen.getByRole("button", { name: "Save and verify" }))
    expect(await screen.findByText("Credentials stored")).toBeTruthy()
    expect(screen.queryByRole("button", { name: "Verify and activate" })).toBeNull()
    expect(setupRefreshes).toBe(1)
    providerSetup.rerender(
      <ProviderStep
        profiles={[alpacaProviderProfile]}
        sessions={[authoritativeActive]}
        transport={setupTransport}
        onChanged={() => {
          setupRefreshes += 1
        }}
      />,
    )
    expect(screen.getByText("Credentials stored")).toBeTruthy()
    expect(screen.queryByRole("button", { name: "Verify and activate" })).toBeNull()
    expect(onboardingRequests.map((request) => request.action)).toEqual([
      "submitSecret",
      "activate",
    ])
  })

  it("never promotes an unverified backend state to installation readiness", async () => {
    render(
      <MemoryRouter initialEntries={["/home"]}>
        <App transport={transport()} />
      </MemoryRouter>,
    )

    expect((await screen.findAllByText("Not verified")).length).toBeGreaterThan(0)
    const welcomeHeading = screen.getByRole("heading", {
      name: "Welcome to Market Squawk",
    })
    const whiteHeadingText = within(welcomeHeading).getByText(
      "Welcome to Market",
      { exact: true },
    )
    const cobaltHeadingText = within(welcomeHeading).getByText("Squawk", {
      exact: true,
    })
    expect(whiteHeadingText.classList.contains("text-white")).toBe(true)
    expect(cobaltHeadingText.classList.contains("text-primary")).toBe(true)
    expect(cobaltHeadingText.querySelector("img,svg")).toBeNull()
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
