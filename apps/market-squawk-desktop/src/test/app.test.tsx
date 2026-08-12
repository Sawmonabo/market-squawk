import { act, render, screen, waitFor, within } from "@testing-library/react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router-dom"
import { describe, expect, it } from "vitest"

import { App } from "@/app/app"
import marketSquawkMarkSvg from "@/assets/market-squawk-mark.svg?raw"
import { CredentialField } from "@/components/setup/credential-field"
import type { AnalyticalControllerStatus } from "@/features/advanced/analytical-profile-contracts"
import { lookupRoute } from "@/features/lookup/lookup-surface"
import type {
  PortfolioAccount,
  PortfolioHolding,
} from "@/features/portfolio/portfolio-contracts"
import { PortfolioPlanning } from "@/features/portfolio/portfolio-planning"
import type {
  ApplicationResult,
  DesktopBootstrap,
  DesktopEvent,
} from "@/lib/schemas"
import type { ProductTransport } from "@/lib/transport"

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
    subscribe: async () => () => undefined,
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

function macroDashboardRead(): DesktopBootstrap["operations"][number] {
  return {
    name: "Macro.GetDashboard",
    description: "Return the exact stored H.15 dashboard publication.",
    domain: "macro",
    authorization: "read_only",
    readOnly: true,
    destructive: false,
    inputSchema: {
      type: "object",
      properties: {
        resultLimits: resultLimitsInputSchema,
        provider: {
          type: "string",
          enum: ["federal-reserve-board.data-download-program"],
        },
        release: { type: "string", enum: ["h15"] },
      },
      required: ["resultLimits", "provider", "release"],
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

const h15DashboardData = {
  schemaIdentity: "market-squawk-macro-dashboard/v1",
  binding: {
    surfaceId: "federal-reserve-board.data-download-program",
    sourceId: "federal-reserve-board-ddp",
    providerDatasetId:
      "federal-reserve-board:h15:h15-treasury-constant-maturities:339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f",
    analyticalDatasetId:
      "federal-reserve-board.h15.h15-treasury-constant-maturities.339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f",
    manifest: {
      datasetId:
        "federal-reserve-board.h15.h15-treasury-constant-maturities.339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f",
      manifestVersion: "3",
      schema: {
        name: "market-squawk-research-v3",
        version: 3,
        fingerprint: "1".repeat(64),
      },
      contentHash: "2".repeat(64),
    },
    objectGraphDigest: "3".repeat(64),
    queryIdentity: "4".repeat(64),
    resultDigest: "5".repeat(64),
  },
  release: {
    code: "H15",
    title: "H.15 Selected Interest Rates",
    family: "h15-treasury-constant-maturities",
    frequency: "business_daily",
    quality: "official_delayed",
  },
  selection: {
    policy: "latest_known_by_series_as_of_cutoff_v1",
    evaluatedAt: "2026-08-11T20:30:00Z",
    selectionDigest: "7".repeat(64),
    returnedSeries: 11,
    availableSeries: 10,
    missingSeries: 1,
    complete: true,
  },
  observations: [
    ["1m", "1 Month", "RIFLGFCM01_N.B"],
    ["3m", "3 Month", "RIFLGFCM03_N.B"],
    ["6m", "6 Month", "RIFLGFCM06_N.B"],
    ["1y", "1 Year", "RIFLGFCY01_N.B"],
    ["2y", "2 Year", "RIFLGFCY02_N.B"],
    ["3y", "3 Year", "RIFLGFCY03_N.B"],
    ["5y", "5 Year", "RIFLGFCY05_N.B"],
    ["7y", "7 Year", "RIFLGFCY07_N.B"],
    ["10y", "10 Year", "RIFLGFCY10_N.B"],
    ["20y", "20 Year", "RIFLGFCY20_N.B"],
    ["30y", "30 Year", "RIFLGFCY30_N.B"],
  ].map(([slot, label, providerSeriesName], index) => ({
    slot,
    label,
    maturityOrder: index + 1,
    seriesId: `federal-reserve-board:h15:H15%2FH15%2F${providerSeriesName}`,
    unitId: "federal-reserve-board-unit:Percent%3A_Per_Year:multiplier:1",
    unitPresentation: "percent_per_year",
    effectiveDate: "2026-08-11",
    availableAt: "2026-08-11T20:25:00Z",
    revision: 1,
    observation:
      slot === "20y"
        ? {
            state: "missing",
            marker: "ND",
            reason: "Provider reported no observation for this date.",
          }
        : { state: "observed", decimal: index === 0 ? "4.25" : "4.5" },
    sourceIdentifier: `frb-ddp:h15:${providerSeriesName}:2026-08-11`,
    sourcePayloadDigest: "6".repeat(64),
  })),
}

const h15ProviderProfile: DesktopBootstrap["providerProfiles"][number] = {
  id: "federal-reserve-board.data-download-program",
  display_name: "Federal Reserve Board Data Download Program",
  official_handoff_url: "https://www.federalreserve.gov/datadownload/",
  handoff_instruction: "No account or credential is required.",
  zero_fee: "No fee",
  account_requirement: "No account",
  credential_requirement: "No credential",
  release_state: "selected",
  coverage: "Federal Reserve Board H.15 selected interest rates",
  quality_ceiling: "official_delayed",
}

const h15DashboardResult: ApplicationResult = {
  data: h15DashboardData,
  metadata: {
    completeness: "complete",
    returnedItems: 11,
    availableItems: 11,
    sourceCoverage: {
      status: "complete",
      providers: ["federal-reserve-board.data-download-program"],
    },
    dataQuality: { status: "official_delayed" },
  },
}

const refreshedH15DashboardResult: ApplicationResult = {
  data: {
    ...h15DashboardData,
    binding: {
      ...h15DashboardData.binding,
      manifest: {
        ...h15DashboardData.binding.manifest,
        manifestVersion: "4",
        contentHash: "8".repeat(64),
      },
      resultDigest: "9".repeat(64),
    },
    selection: {
      ...h15DashboardData.selection,
      evaluatedAt: "2026-08-12T20:30:00Z",
      selectionDigest: "a".repeat(64),
    },
    observations: h15DashboardData.observations.map((observation, index) =>
      index === 0
        ? {
            ...observation,
            effectiveDate: "2026-08-12",
            availableAt: "2026-08-12T20:25:00Z",
            observation: { state: "observed", decimal: "4.35" },
            sourceIdentifier: "frb-ddp:h15:RIFLGFCM01_N.B:2026-08-12",
            sourcePayloadDigest: "b".repeat(64),
          }
        : observation,
    ),
  },
  metadata: h15DashboardResult.metadata,
}

const inactiveH15SourceStatus: ApplicationResult = {
  data: [
    {
      profile: {
        id: "federal-reserve-board.data-download-program",
        display_name: "Federal Reserve Board Data Download Program",
      },
      currentSession: null,
      providerDatasetIdentifier:
        "federal-reserve-board:h15:h15-treasury-constant-maturities:339413969849b22570e106bc02f2a86916f18345b8bb907b86147e69fe0a037f",
      lifecycleSupport: "managed",
      lifecycle: {
        provider: "federal-reserve-board.data-download-program",
        state: "stopped",
        stateRevision: 7,
        configurationSessionId: null,
        blocker: null,
        observedAt: "2026-08-11T20:31:00Z",
      },
      runtime: {},
    },
  ],
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: {
      status: "complete",
      providers: ["federal-reserve-board.data-download-program"],
    },
    dataQuality: { status: "not_applicable" },
  },
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

const unifiedKrakenMarket: ApplicationResult = {
  data: [
    {
      instrumentId: "7e8299e7-9757-4441-926f-d0b22c767a65",
      symbol: "BTC/USD",
      symbolKind: "provider_subscription",
      symbolVenueId: "kraken",
      assetClass: "crypto",
      quoteCurrency: "USD",
      definitionKind: "execution_capable",
      definitionRevision: 3,
      referenceRevision: null,
      permanentFigi: null,
      displayName: "Bitcoin",
      tickSize: "0.1",
      lotSize: "0.00000001",
      executionTermsAvailable: true,
      referenceEvidence: null,
      availability: "Live",
      confidence: "Direct unverified",
      quote: {
        bidPrice: "68000.1",
        bidPriceProviderLexeme: "68000.1",
        bidSize: "0.25",
        bidSizeProviderLexeme: "0.25000000",
        askPrice: "68000.2",
        askPriceProviderLexeme: "68000.2",
        askSize: "0.3",
        askSizeProviderLexeme: "0.30000000",
        midPrice: "68000.15",
        midPriceBasis: "best_bid_ask",
        lastPrice: "68000.2",
        lastPriceProviderLexeme: "68000.2",
        lastSize: "0.02",
        lastSizeProviderLexeme: "0.02000000",
        lastSourceTimestamp: "2026-08-09T14:30:00Z",
        lastReceivedAt: "2026-08-09T14:30:00.010Z",
        lastAvailableAt: "2026-08-09T14:30:00.011Z",
        lastQuality: "direct_unverified",
        lastFreshAtSelection: true,
        quoteEvidence: { surfaceId: "kraken-l3" },
        tradeEvidence: null,
      },
      orderBook: {
        depth: "order_level",
        revision: "17",
        phase: "healthy",
        quarantineReason: null,
        quality: "direct_unverified",
        freshness: "fresh",
        lastMarketAt: "2026-08-09T14:30:00Z",
        availableAt: "2026-08-09T14:30:00.011000000Z",
        usableForSelection: true,
        totalOrderCount: 2,
        returnedOrderCount: 2,
        sampleTruncated: false,
        samplePolicy: "stable_provider_order_id_prefix",
        orders: [
          {
            orderId: "kraken-bid-1",
            side: "bid",
            price: "68000.1",
            priceTicks: "680001",
            quantity: "0.25",
            quantityLots: "25000000",
            providerOrderTimestamp: "2026-08-09T14:30:00Z",
            providerPriority: null,
            firstSeenIn: "snapshot",
            lastUpdatedIn: "snapshot",
            lastSourceTimestamp: "2026-08-09T14:30:00Z",
            lastReceivedAt: "2026-08-09T14:30:00.010Z",
            arrivalOrdinal: "1",
          },
          {
            orderId: "kraken-ask-1",
            side: "ask",
            price: "68000.2",
            priceTicks: "680002",
            quantity: "0.3",
            quantityLots: "30000000",
            providerOrderTimestamp: "2026-08-09T14:30:00Z",
            providerPriority: null,
            firstSeenIn: "snapshot",
            lastUpdatedIn: "snapshot",
            lastSourceTimestamp: "2026-08-09T14:30:00Z",
            lastReceivedAt: "2026-08-09T14:30:00.010Z",
            arrivalOrdinal: "2",
          },
        ],
      },
      marketObservation: {
        availability: "available",
        instrumentId: "7e8299e7-9757-4441-926f-d0b22c767a65",
        mark: {
          value: "68000.2",
          currency: "USD",
          basis: "fresh_last_trade",
          evidenceIdentity: {
            algorithm: "sha256",
            bytes: "11".repeat(32),
          },
          freshUntil: "2026-08-09T14:30:05.000000000Z",
        },
        selectionDigest: {
          algorithm: "sha256",
          bytes: "22".repeat(32),
        },
        selectedAt: "2026-08-09T14:30:00.011000000Z",
        generation: "1",
        quality: "direct_unverified",
        depth: "order_level",
        coverage: "single_venue",
        integrity: "verified",
        features: {
          availability: "unavailable",
          reason: "source_does_not_publish_live_features",
        },
      },
      selectedSource: {
        surfaceId: "kraken-l3",
        providerId: "kraken",
        providerSymbol: "BTC/USD",
        sourceId: "kraken-l3-account",
        venueId: "kraken",
        providerProduct: "websocket-v2",
        providerChannel: "level3",
        timing: "live",
        depth: "order_level",
        depthLabel: "Order-level book",
        quality: "direct_unverified",
        coverage: "single_venue",
        health: "healthy",
        freshness: {
          receivedAt: "2026-08-09T14:30:00.010Z",
          availableAt: "2026-08-09T14:30:00.011Z",
          sourceValidUntil: "2026-08-09T14:30:05Z",
          freshAtSelection: true,
        },
        integrity: {
          state: "checksum_valid",
          phase: "healthy",
          generationCurrent: true,
          snapshotInitialized: true,
        },
      },
      alternatives: [],
      selectionReceipt: {
        policyRevision: 1,
        policyCandidateLimit: 256,
        policyDigest: {
          algorithm: "sha256",
          bytes: "33".repeat(32),
        },
        selectionDigest: {
          algorithm: "sha256",
          bytes: "22".repeat(32),
        },
        selectedAt: "2026-08-09T14:30:00.011000000Z",
        eligibleCount: 1,
        rejectedCount: 0,
        availableAlternativeCount: 0,
        returnedAlternativeCount: 0,
        alternativesComplete: true,
        selectionClass: "exact_requirements",
        downgradeDimensions: [],
      },
    },
  ],
  metadata: {
    completeness: "complete",
    returnedItems: 1,
    availableItems: 1,
    sourceCoverage: { status: "complete", providers: ["kraken"] },
    dataQuality: { status: "direct_unverified" },
  },
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

  it("renders one unified market with automatic source confidence and order-level detail", async () => {
    const user = userEvent.setup()
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      operations: [
        datasetRead(
          "Market.GetUnifiedFeed",
          "market",
          "Return the bounded unified market view.",
        ),
      ],
    }
    render(
      <MemoryRouter initialEntries={["/markets"]}>
        <App
          transport={transport(readyBootstrap, undefined, async (request) => {
            if (request.query === "marketUnifiedFeed") return unifiedKrakenMarket
            throw new Error(`Unexpected market query: ${request.query}`)
          })}
        />
      </MemoryRouter>,
    )

    expect(await screen.findByRole("heading", { name: "Markets" })).toBeTruthy()
    const marketHeading = await screen.findByRole("heading", { name: "BTC/USD" })
    const marketCard = marketHeading.closest("button")
    expect(marketCard).toBeInstanceOf(HTMLButtonElement)
    if (!(marketCard instanceof HTMLButtonElement)) {
      throw new Error("Unified market card is absent")
    }
    expect(within(marketCard).getByText("Bitcoin")).toBeTruthy()
    expect(within(marketCard).getByText("Live")).toBeTruthy()
    expect(within(marketCard).getByText("Order-level book")).toBeTruthy()
    expect(within(marketCard).getByText("2 of 2")).toBeTruthy()
    expect(screen.queryByRole("combobox", { name: /provider/i })).toBeNull()

    await user.click(
      screen.getByText("Show detailed trades, quotes, order book, and source comparison"),
    )
    expect(await screen.findByText("Chosen automatically")).toBeTruthy()
    expect(screen.getByText("Orders behind the visible market")).toBeTruthy()
    await user.click(screen.getByText("Data confidence"))
    expect(screen.getAllByText("kraken").length).toBeGreaterThan(0)
    expect(screen.getByText("Checksum valid")).toBeTruthy()
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
    const readyBootstrap: DesktopBootstrap = {
      ...blockedBootstrap,
      providerProfiles: [h15ProviderProfile],
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
        macroDashboardRead(),
        sourceStatusRead(),
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
    const desktopEvents: {
      listener: ((event: DesktopEvent) => void) | null
    } = { listener: null }
    const dashboardTransport: ProductTransport = {
      ...transport(readyBootstrap, undefined, async (request) => {
        issuedQueries.push(request)
        if (request.query === "macroDashboard") {
          return admittedServiceGeneration === 2
            ? refreshedH15DashboardResult
            : h15DashboardResult
        }
        if (request.query === "sourceStatus") return inactiveH15SourceStatus
        if (request.query === "paperStatus") return stoppedPaperStatus
        if (
          request.query === "paperOrders" ||
          request.query === "paperFills" ||
          request.query === "portfolioAccounts" ||
          request.query === "sourceCoverage" ||
          request.query === "sourceHealth"
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
        admittedServiceGeneration = currentBootstrap.runtime.serviceGeneration
        issuedBootstrapGenerations.push(admittedServiceGeneration)
        return currentBootstrap
      },
      subscribe: async (listener) => {
        desktopEvents.listener = listener
        return () => {
          if (desktopEvents.listener === listener) desktopEvents.listener = null
        }
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
    const h15Heading = await screen.findByRole("heading", {
      name: "H.15 Selected Interest Rates",
    })
    const h15Section = h15Heading.closest("section")
    expect(h15Section).toBeInstanceOf(HTMLElement)
    if (!(h15Section instanceof HTMLElement)) {
      throw new Error("The H.15 research section is absent")
    }
    expect(within(h15Section).getByText("Stored publication")).toBeTruthy()
    expect(within(h15Section).getByText("Queryable")).toBeTruthy()
    expect(within(h15Section).getByText("Source readiness")).toBeTruthy()
    expect(within(h15Section).getByText("Inactive")).toBeTruthy()
    expect(
      within(h15Section).getByText("Source lifecycle observed"),
    ).toBeTruthy()
    expect(
      within(h15Section).getByText("2026-08-11T20:31:00Z"),
    ).toBeTruthy()
    expect(
      within(h15Section).getByText("Source runtime observed"),
    ).toBeTruthy()
    expect(within(h15Section).getByText("Not reported")).toBeTruthy()
    expect(within(h15Section).getByText("4.25%")).toBeTruthy()
    expect(
      within(h15Section).getByText("Pinned query result digest"),
    ).toBeTruthy()
    expect(
      within(h15Section).getByText("Final typed selection digest"),
    ).toBeTruthy()
    expect(within(h15Section).getByText("5".repeat(64))).toBeTruthy()
    expect(within(h15Section).getByText("7".repeat(64))).toBeTruthy()
    const missingHeading = within(h15Section).getByRole("heading", {
      name: "20 Year",
    })
    const missingObservation = missingHeading.closest("article")
    expect(missingObservation).toBeInstanceOf(HTMLElement)
    if (!(missingObservation instanceof HTMLElement)) {
      throw new Error("The explicit H.15 missing observation is absent")
    }
    expect(within(missingObservation).getByText("Missing")).toBeTruthy()
    expect(within(missingObservation).getByText("ND")).toBeTruthy()
    expect(
      within(missingObservation).getByText(
        "Provider reported no observation for this date.",
      ),
    ).toBeTruthy()
    expect(
      within(missingObservation).getByText(
        "federal-reserve-board:h15:H15%2FH15%2FRIFLGFCY20_N.B",
      ),
    ).toBeTruthy()
    expect(
      issuedQueries.filter((request) => request.query === "macroDashboard"),
    ).toEqual([
      {
        query: "macroDashboard",
        provider: "federal-reserve-board.data-download-program",
        release: "h15",
      },
    ])
    expect(
      issuedQueries.filter((request) => request.query === "sourceStatus"),
    ).toEqual([
      {
        query: "sourceStatus",
        sourceIds: ["federal-reserve-board.data-download-program"],
      },
    ])
    expect(screen.queryByText("Operation arguments")).toBeNull()

    const initialMacroReads = readCount("macroDashboard")
    const initialSourceReads = readCount("sourceStatus")
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

    expect(await screen.findByText("4.35%")).toBeTruthy()
    const refreshedH15Heading = screen.getByRole("heading", {
      name: "H.15 Selected Interest Rates",
    })
    const refreshedH15Section = refreshedH15Heading.closest("section")
    expect(refreshedH15Section).toBeInstanceOf(HTMLElement)
    if (!(refreshedH15Section instanceof HTMLElement)) {
      throw new Error("The refreshed H.15 research section is absent")
    }
    expect(within(refreshedH15Section).queryByText("4.25%")).toBeNull()
    expect(
      within(refreshedH15Section).queryByText("5".repeat(64)),
    ).toBeNull()
    expect(
      within(refreshedH15Section).getByText("9".repeat(64)),
    ).toBeTruthy()
    expect(readCount("macroDashboard")).toBeGreaterThan(initialMacroReads)
    expect(readCount("sourceStatus")).toBeGreaterThan(initialSourceReads)
    expect(issuedBootstrapGenerations).toContain(2)

    const macroReadsBeforeResearchInvalidation = readCount("macroDashboard")
    const sourceReadsBeforeResearchInvalidation = readCount("sourceStatus")
    await notifyAuthorityChanged("1", "research", "Research.IngestSource")
    await waitFor(() => {
      expect(readCount("macroDashboard")).toBeGreaterThan(
        macroReadsBeforeResearchInvalidation,
      )
    })
    expect(readCount("sourceStatus")).toBe(sourceReadsBeforeResearchInvalidation)

    const refreshedNavigation = document.querySelector(
      'nav[aria-label="Market Squawk"]',
    )
    expect(refreshedNavigation).toBeInstanceOf(HTMLElement)
    if (!(refreshedNavigation instanceof HTMLElement)) {
      throw new Error("The refreshed Market Squawk navigation is absent")
    }
    await user.click(
      within(refreshedNavigation).getByRole("link", {
        name: "Connections & Sources",
      }),
    )
    expect(await screen.findByRole("heading", { name: "Sources" })).toBeTruthy()
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

    await waitFor(() => {
      expect(readCount("sourceCoverage")).toBeGreaterThan(0)
      expect(readCount("sourceHealth")).toBeGreaterThan(0)
    })
    const sourceReadsBeforeSourceInvalidation = readCount("sourceStatus")
    const coverageReadsBeforeSourceInvalidation = readCount("sourceCoverage")
    const healthReadsBeforeSourceInvalidation = readCount("sourceHealth")
    const macroReadsBeforeSourceInvalidation = readCount("macroDashboard")
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
    expect(readCount("macroDashboard")).toBe(macroReadsBeforeSourceInvalidation)

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
    render(
      <MemoryRouter initialEntries={["/system/updates-repair"]}>
        <App transport={transport(readyBootstrap)} />
      </MemoryRouter>,
    )
    expect(
      await screen.findByRole("heading", { name: "Updates & program recovery" }),
    ).toBeTruthy()
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
