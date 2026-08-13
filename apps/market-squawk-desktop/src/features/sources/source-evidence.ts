import type {
  ApplicationResult,
  ProviderProfile,
  ProviderSession,
} from "@/lib/schemas"
import type {
  SourceLifecycleAction,
  SourceLifecycleRequest,
} from "@/lib/transport"

type RecordValue = Record<string, unknown>

const LIVE_SOURCES = new Set([
  "coinbase.public-market-data",
  "coinbase.exchange-direct-market-data",
  "kraken.spot-public-market-data",
  "alpaca.basic-market-data",
  "tradier.brokerage-market-data",
  "kraken.spot-authenticated-level3-market-data",
])
const PUBLIC_LIVE_SOURCES = new Set([
  "coinbase.public-market-data",
  "kraken.spot-public-market-data",
])

export interface StoredDataEvidence {
  datasetId: string
  sourceId: string
  generationKind: string
  manifestVersion: number
  rowCount: number
  totalBytes: number
  objectCount: number
}

export interface StoredDataQuarantine {
  datasetId: string
  expectedSourceId: string | null
  observedSourceIds: string[]
  reason:
    | "source_identity_missing"
    | "source_identity_mismatch"
    | "ambiguous_dataset_identity"
}

export interface SourceEvidence {
  id: string
  name: string
  declaredCoverage: string | null
  qualityCeiling: string | null
  releaseState: string | null
  zeroFee: string | null
  accountRequirement: string | null
  credentialRequirement: string | null
  setupState: string | null
  nextAction: string | null
  lifecycleSupport: "managed" | "not_applicable" | null
  operationalState: string | null
  runtimeState: string | null
  sourceId: string | null
  venueId: string | null
  instrumentId: string | null
  connection: string | null
  marketFreshness: string | null
  integrity: string | null
  quality: string | null
  coverageState: string | null
  runtimeObservedAt: string | null
  latestSetupSessionId: string | null
  providerDatasetIdentifier: string | null
  storedData: StoredDataEvidence | null
  storedDataQuarantine: StoredDataQuarantine | null
  lifecycle: LifecycleEvidence | null
}

export interface LifecycleControl {
  action: SourceLifecycleAction
  label: string
  request: SourceLifecycleRequest
  destructive: boolean
}

export interface LifecycleEvidence {
  provider: string
  state: SourceLifecycleState
  stateRevision: number
  configurationSessionId: string | null
  currentGeneration?: number
  runtimeGenerationSha256?: string
  publicConfigurationSha256?: string
  doctor: SourceDoctorEvidence | null
  startEligibility: SourceStartEligibility
  blocker: string | null
  observedAt: string | null
}

export type SourceStartEligibility =
  | "eligible"
  | "already_active"
  | "doctor_required"
  | "doctor_expired"
  | "credential_stale"
  | "reconciliation_required"
  | "provider_unavailable"
  | "not_applicable"

export type SourceLifecycleState =
  | "stopped"
  | "starting"
  | "active"
  | "resynchronizing"
  | "blocked"
  | "removed"

export interface SourceDoctorEvidence {
  schema: "market-squawk.alpaca-paper-iex-doctor/v1"
  receiptSha256: string
  surfaceId: "alpaca.basic-market-data"
  onboardingSessionId: string
  credentialGeneration: string
  realm: "paper"
  marketDataPrincipalSha256: string
  principalSemantics:
    "non_trading_market_data_credential_principal_not_brokerage_account"
  capabilityRevision: string
  capabilitySha256: string
  publicConfigurationSha256: string
  rightsDecisionSha256: string
  ratePolicySha256: string
  doctorRevision: string
  doctorContractSha256: string
  verifiedAt: string
  exclusiveExpiresAt: string
  current: boolean
  capabilities: SourceDoctorCapabilities
}

export interface SourceDoctorCapabilities {
  iexLatestQuote: DoctorProbeSummary
  iexSnapshotBatch: DoctorProbeSummary & {
    requested: number | null
    returned: number | null
    valid: number | null
    missing: number | null
    rate: DoctorRateEvidence | null
  }
  iexWebSocket: DoctorProbeSummary & {
    rate: DoctorRateEvidence | null
  }
  iexHistoricalBars: DoctorProbeSummary & {
    pages: number | null
    bars: number | null
    terminalPagination: boolean | null
  }
  iexUtcCalendar: DoctorProbeSummary & {
    sessions: number | null
    matchedDates: number | null
    exactDateReconciliation: boolean | null
  }
}

export interface DoctorProbeSummary {
  disposition: "available" | "degraded" | "unavailable" | "not_probed"
  evidenceSha256: string
}

export interface DoctorRateEvidence {
  limit: DoctorObservedUnsigned
  remaining: DoctorObservedUnsigned
  resetUnixSeconds: DoctorObservedIntegerText
  retryAfter: DoctorObservedRetryAfter
}

export type DoctorObservedUnsigned =
  | { state: "missing" }
  | { state: "observed"; value: number }

export type DoctorObservedIntegerText =
  | { state: "missing" }
  | { state: "observed"; value: string }

export type DoctorObservedRetryAfter =
  | { state: "missing" }
  | {
      state: "observed"
      value:
        | { kind: "delay_seconds"; value: string }
        | { kind: "at_unix_seconds"; value: string }
    }

export function sourceEvidence(
  profiles: ProviderProfile[],
  sessions: ProviderSession[],
  statuses: ApplicationResult[],
  coverage: ApplicationResult | undefined,
  health: ApplicationResult | undefined,
): SourceEvidence[] {
  const statusById = new Map<string, RecordValue>()
  for (const status of statuses) {
    for (const [id, row] of indexRows(status.data, (item) =>
      text(record(item.profile)?.id),
    )) {
      statusById.set(id, row)
    }
  }
  const coverageById = indexRows(coverage?.data, (row) => text(row.surfaceId))
  const healthById = indexRows(health?.data, (row) => text(row.surfaceId))
  const profileById = new Map(profiles.map((profile) => [profile.id, profile]))
  const identifiers = new Set([
    ...profileById.keys(),
    ...statusById.keys(),
    ...coverageById.keys(),
    ...healthById.keys(),
  ])

  return [...identifiers]
    .map((id) =>
      toEvidence(
        id,
        profileById.get(id),
        sessions.find((session) => session.surface_id === id),
        statusById.get(id),
        coverageById.get(id),
        healthById.get(id),
      ),
    )
    .sort((left, right) => left.name.localeCompare(right.name))
}

export function lifecycleControls(source: SourceEvidence): LifecycleControl[] {
  const lifecycle = source.lifecycle
  if (!lifecycle) return []

  const base: SourceLifecycleRequest = {
    provider: lifecycle.provider,
    expectedStateRevision: lifecycle.stateRevision,
  }
  const current = lifecycle.currentGeneration
  const runtimeGeneration = lifecycle.runtimeGenerationSha256
  const hasConfiguration =
    lifecycle.configurationSessionId !== null &&
    lifecycle.publicConfigurationSha256 !== undefined
  const exactConfiguration = hasConfiguration
    ? {
        onboardingSessionId: lifecycle.configurationSessionId ?? undefined,
        publicConfigurationSha256: lifecycle.publicConfigurationSha256,
      }
    : {}
  const reconfigure =
    hasConfiguration && source.id !== "alpaca.basic-market-data"
      ? [
          control("reconfigure", "Apply prepared configuration", {
            ...base,
            onboardingSessionId: lifecycle.configurationSessionId ?? undefined,
            publicConfigurationSha256: lifecycle.publicConfigurationSha256,
          }),
        ]
      : []
  const remove = control(
    "remove",
    "Remove",
    { ...base, reason: "desktop-user-request" },
    true,
  )
  const removeControls =
    LIVE_SOURCES.has(source.id) || hasConfiguration || lifecycle.state === "active"
      ? [remove]
      : []

  switch (lifecycle.state) {
    case "stopped":
      return [
        ...(PUBLIC_LIVE_SOURCES.has(source.id)
          ? [control("start", "Start", base)]
          : source.id === "alpaca.basic-market-data" && hasConfiguration
            ? lifecycle.startEligibility === "eligible"
              ? [
                  control("start", "Start", {
                    ...base,
                    ...exactConfiguration,
                  }),
                ]
              : [
                  control("verify", "Run doctor", {
                    ...base,
                    ...exactConfiguration,
                  }),
                ]
            : hasConfiguration
            ? [
                control("retry", "Resume", {
                  ...base,
                  reason: "desktop-user-request",
                }),
              ]
            : []),
        ...(LIVE_SOURCES.has(source.id) && source.id !== "alpaca.basic-market-data"
          ? [control("verify", "Verify", { ...base, ...exactConfiguration })]
          : []),
        ...reconfigure,
        ...removeControls,
      ]
    case "active":
      return [
        ...(LIVE_SOURCES.has(source.id)
          ? source.id === "alpaca.basic-market-data"
            ? [
                control(
                  "verify",
                  "Renew doctor and stop source",
                  { ...base, ...exactConfiguration },
                  true,
                ),
              ]
            : [control("verify", "Verify", { ...base, ...exactConfiguration })]
          : []),
        ...(current || runtimeGeneration
          ? [
              control("resynchronize", "Resynchronize", {
                ...base,
                ...(current
                  ? { expectedGeneration: current }
                  : { expectedRuntimeGenerationSha256: runtimeGeneration }),
                reason: "desktop-user-request",
              }),
            ]
          : []),
        control(
          "stop",
          "Stop",
          { ...base, reason: "desktop-user-request" },
          true,
        ),
        ...reconfigure,
        ...removeControls,
      ]
    case "blocked":
      return [
        ...(LIVE_SOURCES.has(source.id) &&
        lifecycle.startEligibility !== "reconciliation_required"
          ? source.id === "alpaca.basic-market-data"
            ? [
                control(
                  "verify",
                  "Run doctor and stop source",
                  { ...base, ...exactConfiguration },
                  true,
                ),
              ]
            : [control("verify", "Run doctor", { ...base, ...exactConfiguration })]
          : []),
        ...(PUBLIC_LIVE_SOURCES.has(source.id) ||
        (hasConfiguration && source.id !== "alpaca.basic-market-data")
          ? [
              control("retry", "Retry", {
                ...base,
                reason: "desktop-user-request",
              }),
            ]
            : []),
        ...reconfigure,
        ...removeControls,
      ]
    case "removed":
      return PUBLIC_LIVE_SOURCES.has(source.id)
        ? [control("start", "Start again", base)]
        : []
    default:
      return []
  }
}

export function sourceNeedsSetup(source: SourceEvidence): boolean {
  const lifecycle = source.lifecycle
  if (!lifecycle || source.lifecycleSupport !== "managed") return false
  if (lifecycle.state === "active") return false
  if (PUBLIC_LIVE_SOURCES.has(source.id)) return false
  return !(
    lifecycle.configurationSessionId &&
    lifecycle.publicConfigurationSha256
  )
}

export function attachStoredData(
  sources: SourceEvidence[],
  stored: StoredDataEvidence[],
): SourceEvidence[] {
  const storedByDataset = new Map<string, StoredDataEvidence[]>()
  for (const item of stored) {
    const existing = storedByDataset.get(item.datasetId) ?? []
    existing.push(item)
    storedByDataset.set(item.datasetId, existing)
  }

  return sources.map((source) => {
    const datasetId = source.providerDatasetIdentifier
    const candidates = datasetId
      ? (storedByDataset.get(datasetId) ?? [])
      : []
    if (!datasetId || candidates.length === 0) {
      return { ...source, storedData: null, storedDataQuarantine: null }
    }
    const observedSourceIds = [
      ...new Set(candidates.map((candidate) => candidate.sourceId)),
    ].sort()
    const sourceId = source.sourceId
    if (!sourceId) {
      return {
        ...source,
        storedData: null,
        storedDataQuarantine: {
          datasetId,
          expectedSourceId: null,
          observedSourceIds,
          reason: "source_identity_missing",
        },
      }
    }
    const exact = candidates.filter(
      (candidate) =>
        candidate.sourceId === sourceId && candidate.datasetId === datasetId,
    )
    if (candidates.length === 1 && exact.length === 1) {
      return {
        ...source,
        storedData: exact[0] ?? null,
        storedDataQuarantine: null,
      }
    }
    return {
      ...source,
      storedData: null,
      storedDataQuarantine: {
        datasetId,
        expectedSourceId: sourceId,
        observedSourceIds,
        reason:
          candidates.length > 1
            ? "ambiguous_dataset_identity"
            : "source_identity_mismatch",
      },
    }
  })
}

function toEvidence(
  id: string,
  bootstrapProfile: ProviderProfile | undefined,
  bootstrapSession: ProviderSession | undefined,
  status: RecordValue | undefined,
  coverage: RecordValue | undefined,
  health: RecordValue | undefined,
): SourceEvidence {
  const profile = record(status?.profile)
  const session = record(status?.currentSession)
  const runtime = record(status?.runtime)
  const runtimeCoverage = record(coverage?.runtimeCoverage)
  const runtimeHealth = record(health?.runtimeHealth)
  const lifecycle = lifecycleEvidence(status?.lifecycle)
  const lifecycleSupport = lifecycleSupportEvidence(status?.lifecycleSupport)
  const runtimeState = text(runtime?.state) ?? text(runtimeHealth?.state)
  const providerDatasetIdentifier = text(status?.providerDatasetIdentifier)

  return {
    id,
    name:
      text(profile?.display_name) ?? bootstrapProfile?.display_name ?? id,
    declaredCoverage:
      text(coverage?.declaredCoverage) ??
      text(profile?.coverage) ??
      bootstrapProfile?.coverage ??
      null,
    qualityCeiling:
      text(coverage?.qualityCeiling) ??
      text(profile?.quality_ceiling) ??
      bootstrapProfile?.quality_ceiling ??
      null,
    releaseState:
      text(coverage?.releaseState) ??
      text(profile?.release_state) ??
      bootstrapProfile?.release_state ??
      null,
    zeroFee: text(profile?.zero_fee) ?? bootstrapProfile?.zero_fee ?? null,
    accountRequirement:
      text(profile?.account_requirement) ??
      bootstrapProfile?.account_requirement ??
      null,
    credentialRequirement:
      text(profile?.credential_requirement) ??
      bootstrapProfile?.credential_requirement ??
      null,
    setupState:
      text(session?.state) ?? bootstrapSession?.state ?? text(health?.onboardingState),
    nextAction:
      text(session?.next_action) ?? bootstrapSession?.next_action ?? null,
    lifecycleSupport,
    operationalState: lifecycle?.state ?? runtimeState,
    runtimeState,
    sourceId: text(runtime?.sourceId) ?? text(runtimeHealth?.sourceId),
    venueId: text(runtime?.venueId) ?? text(runtimeHealth?.venueId),
    instrumentId:
      text(runtime?.instrumentId) ?? text(runtimeHealth?.instrumentId),
    connection: evidenceName(runtime?.connection ?? runtimeHealth?.connection),
    marketFreshness: evidenceName(runtimeHealth?.marketFreshness),
    integrity: evidenceName(runtime?.integrity ?? runtimeHealth?.streamIntegrity),
    quality: evidenceName(runtime?.quality ?? runtimeHealth?.quality),
    coverageState: evidenceName(runtimeCoverage),
    runtimeObservedAt: unixNanos(
      runtime?.observedAtUnixNanos ?? runtimeHealth?.observedAtUnixNanos,
    ),
    latestSetupSessionId:
      text(session?.session_id) ?? bootstrapSession?.session_id ?? null,
    providerDatasetIdentifier,
    storedData: null,
    storedDataQuarantine: null,
    lifecycle,
  }
}

function lifecycleEvidence(value: unknown): LifecycleEvidence | null {
  const row = exactRecord(value, [
    "provider", "state", "stateRevision", "configurationSessionId",
    "currentGeneration", "runtimeGenerationSha256", "publicConfigurationSha256",
    "doctor", "startEligibility", "blocker", "observedAt",
  ])
  const provider = text(row?.provider)
  const state = sourceLifecycleState(row?.state)
  const stateRevision = positiveInteger(row?.stateRevision)
  if (!provider || !state || stateRevision === null) return null

  const configurationSessionId = uuid(row?.configurationSessionId)
  const currentGeneration = positiveInteger(row?.currentGeneration)
  const runtimeGenerationSha256 = text(row?.runtimeGenerationSha256)
  const publicConfigurationSha256 = text(row?.publicConfigurationSha256)
  const doctor = sourceDoctorEvidence(row?.doctor)
  const startEligibility = sourceStartEligibility(row?.startEligibility)
  const blocker = sourceLifecycleBlocker(row?.blocker)
  const observedAt = timestamp(row?.observedAt)
  const configurationPresent = row?.configurationSessionId !== null
  const currentGenerationPresent = row?.currentGeneration !== null
  const runtimeGenerationPresent = row?.runtimeGenerationSha256 !== null
  const publicConfigurationPresent = row?.publicConfigurationSha256 !== null
  const activeRuntimeIsExact = state === "active"
    ? currentGenerationPresent !== runtimeGenerationPresent
    : !currentGenerationPresent && !runtimeGenerationPresent
  const doctorAdmitsStart = doctor !== null && doctor.current &&
    Object.values(doctor.capabilities).every((capability) =>
      capability.disposition === "available"
    )
  const doctorCurrentnessIsExact = doctor === null || observedAt !== null &&
    doctor.current === (
      doctor.verifiedAt <= observedAt && observedAt < doctor.exclusiveExpiresAt
    )
  const isAlpaca = provider === "alpaca.basic-market-data"
  if (!startEligibility || !observedAt ||
    (configurationPresent && !configurationSessionId) ||
    (currentGenerationPresent && currentGeneration === null) ||
    (runtimeGenerationPresent && (!runtimeGenerationSha256 || !sha256(runtimeGenerationSha256))) ||
    (publicConfigurationPresent &&
      (!publicConfigurationSha256 || !sha256(publicConfigurationSha256))) ||
    configurationPresent !== publicConfigurationPresent ||
    !activeRuntimeIsExact ||
    (state === "blocked") !== (blocker !== null) ||
    (row?.blocker !== null && blocker === null) ||
    (row?.doctor !== null && doctor === null) ||
    !doctorCurrentnessIsExact ||
    (doctor !== null && (
      doctor.surfaceId !== provider ||
      doctor.onboardingSessionId !== configurationSessionId ||
      doctor.publicConfigurationSha256 !== publicConfigurationSha256
    )) ||
    (startEligibility === "eligible" &&
      (!isAlpaca || state !== "stopped" || !doctorAdmitsStart)) ||
    (startEligibility === "already_active" &&
      (!isAlpaca || state !== "active" || !doctorAdmitsStart)) ||
    (startEligibility === "not_applicable" && (isAlpaca || doctor !== null)) ||
    (startEligibility !== "not_applicable" && !isAlpaca)
  ) return null
  return {
    provider,
    state,
    stateRevision,
    configurationSessionId,
    ...(currentGeneration ? { currentGeneration } : {}),
    ...(runtimeGenerationSha256
      ? { runtimeGenerationSha256 }
      : {}),
    ...(publicConfigurationSha256
      ? { publicConfigurationSha256 }
      : {}),
    doctor,
    startEligibility,
    blocker,
    observedAt,
  }
}

function sourceLifecycleState(value: unknown): SourceLifecycleState | null {
  return value === "stopped" || value === "starting" || value === "active" ||
    value === "resynchronizing" || value === "blocked" || value === "removed"
    ? value
    : null
}

function sourceLifecycleBlocker(value: unknown): string | null {
  return value === "credential" || value === "rights" || value === "rate_budget" ||
    value === "integrity" || value === "provider_availability" ||
    value === "reconciliation" || value === "stale_precondition"
    ? value
    : null
}

function sourceStartEligibility(value: unknown): SourceStartEligibility | null {
  return value === "eligible" ||
    value === "already_active" ||
    value === "doctor_required" ||
    value === "doctor_expired" ||
    value === "credential_stale" ||
    value === "reconciliation_required" ||
    value === "provider_unavailable" ||
    value === "not_applicable"
    ? value
    : null
}

function sourceDoctorEvidence(value: unknown): SourceDoctorEvidence | null {
  if (value === null || value === undefined) return null
  const row = exactRecord(value, [
    "schema", "receiptSha256", "surfaceId", "onboardingSessionId",
    "credentialGeneration", "realm", "marketDataPrincipalSha256",
    "principalSemantics", "capabilityRevision", "capabilitySha256",
    "publicConfigurationSha256", "rightsDecisionSha256", "ratePolicySha256",
    "doctorRevision", "doctorContractSha256", "dataQuality", "verifiedAt",
    "exclusiveExpiresAt", "current", "capabilities",
  ])
  if (!row) return null
  const capabilities = doctorCapabilities(row.capabilities)
  const receiptSha256 = text(row.receiptSha256)
  const onboardingSessionId = uuid(row.onboardingSessionId)
  const generation = positiveIntegerText(row.credentialGeneration)
  const principal = text(row.marketDataPrincipalSha256)
  const capabilityRevision = positiveIntegerText(row.capabilityRevision)
  const capabilitySha256 = text(row.capabilitySha256)
  const configuration = text(row.publicConfigurationSha256)
  const rightsDecisionSha256 = text(row.rightsDecisionSha256)
  const ratePolicySha256 = text(row.ratePolicySha256)
  const doctorRevision = text(row.doctorRevision)
  const doctorContractSha256 = text(row.doctorContractSha256)
  const verifiedAt = timestamp(row.verifiedAt)
  const exclusiveExpiresAt = timestamp(row.exclusiveExpiresAt)
  if (
    row.schema !== "market-squawk.alpaca-paper-iex-doctor/v1" ||
    row.surfaceId !== "alpaca.basic-market-data" ||
    row.realm !== "paper" ||
    row.principalSemantics !==
      "non_trading_market_data_credential_principal_not_brokerage_account" ||
    row.dataQuality !== "direct_unverified" ||
    !receiptSha256 || !sha256(receiptSha256) || !onboardingSessionId ||
    generation === null || !principal || !sha256(principal) ||
    capabilityRevision === null || !capabilitySha256 || !sha256(capabilitySha256) ||
    !configuration || !sha256(configuration) ||
    !rightsDecisionSha256 || !sha256(rightsDecisionSha256) ||
    !ratePolicySha256 || !sha256(ratePolicySha256) ||
    !doctorRevision || doctorRevision.length > 128 ||
    !doctorContractSha256 || !sha256(doctorContractSha256) ||
    !verifiedAt || !exclusiveExpiresAt ||
    verifiedAt >= exclusiveExpiresAt ||
    typeof row.current !== "boolean" || !capabilities
  ) return null
  return {
    schema: row.schema,
    receiptSha256,
    surfaceId: row.surfaceId,
    onboardingSessionId,
    credentialGeneration: generation,
    realm: row.realm,
    marketDataPrincipalSha256: principal,
    principalSemantics: row.principalSemantics,
    capabilityRevision,
    capabilitySha256,
    publicConfigurationSha256: configuration,
    rightsDecisionSha256,
    ratePolicySha256,
    doctorRevision,
    doctorContractSha256,
    verifiedAt,
    exclusiveExpiresAt,
    current: row.current,
    capabilities,
  }
}

function doctorCapabilities(value: unknown): SourceDoctorCapabilities | null {
  const row = exactRecord(value, [
    "iexLatestQuote", "iexSnapshotBatch", "iexWebSocket",
    "iexHistoricalBars", "iexUtcCalendar", "additional",
  ])
  if (!row || !doctorAdditionalCapabilities(row.additional)) return null
  const quote = doctorProbe(row.iexLatestQuote, doctorQuoteObservation)
  const batch = doctorCountProbe(row.iexSnapshotBatch)
  const stream = doctorStreamProbe(row.iexWebSocket)
  const history = doctorProbe(row.iexHistoricalBars, doctorHistoryObservation)
  const calendar = doctorProbe(row.iexUtcCalendar, doctorCalendarObservation)
  if (!quote || !batch || !stream || !history || !calendar ||
    !doctorHistoryCalendarBinding(history.observation, calendar.observation)) return null
  return {
    iexLatestQuote: quote.summary,
    iexSnapshotBatch: batch,
    iexWebSocket: {
      ...stream.summary,
      rate: stream.observation?.handshakeRate ?? null,
    },
    iexHistoricalBars: doctorHistorySummary(history),
    iexUtcCalendar: doctorCalendarSummary(calendar),
  }
}

interface ParsedDoctorProbe<T> {
  summary: DoctorProbeSummary
  observation: T | null
}

function doctorProbe<T>(
  value: unknown,
  parseObservation: (value: unknown) => T | null,
): ParsedDoctorProbe<T> | null {
  const row = exactRecord(value, ["disposition", "evidenceSha256", "observation"])
  const disposition = doctorDisposition(row?.disposition)
  const evidenceSha256 = text(row?.evidenceSha256)
  if (!row || !disposition || !evidenceSha256 || !sha256(evidenceSha256)) return null
  const observation = row.observation === null ? null : parseObservation(row.observation)
  if (row.observation !== null && observation === null) return null
  if ((disposition === "available" || disposition === "degraded") && !observation) return null
  if (disposition === "not_probed" && observation) return null
  return { summary: { disposition, evidenceSha256 }, observation }
}

function doctorCountProbe(value: unknown) {
  const probe = doctorProbe(value, doctorBatchObservation)
  if (!probe) return null
  const observation = probe.observation
  return {
    ...probe.summary,
    requested: observation?.requested ?? null,
    returned: observation?.returned ?? null,
    valid: observation?.valid ?? null,
    missing: observation?.missing ?? null,
    rate: observation?.http.rate ?? null,
  }
}

function doctorStreamProbe(value: unknown) {
  return doctorProbe(value, doctorStreamObservation)
}

function doctorHistorySummary(probe: ParsedDoctorProbe<DoctorHistoryObservation>) {
  const observation = probe.observation
  return {
    ...probe.summary,
    pages: observation?.pages ?? null,
    bars: observation?.bars ?? null,
    terminalPagination: observation?.terminalPagination ?? null,
  }
}

function doctorCalendarSummary(probe: ParsedDoctorProbe<DoctorCalendarObservation>) {
  const observation = probe.observation
  return {
    ...probe.summary,
    sessions: observation?.sessions ?? null,
    matchedDates: observation?.matchedDates ?? null,
    exactDateReconciliation: observation?.exactDateReconciliation ?? null,
  }
}

interface DoctorHttpObservation {
  rate: DoctorRateEvidence
}

interface DoctorBatchObservation {
  http: DoctorHttpObservation
  requested: number
  returned: number
  valid: number
  missing: number
}

interface DoctorStreamObservation {
  handshakeRate: DoctorRateEvidence
}

interface DoctorHistoryObservation {
  startDate: string
  endDate: string
  pages: number
  bars: number
  distinctDates: number
  returnedDatesSha256: string
  terminalPagination: boolean
}

interface DoctorCalendarObservation {
  startDate: string
  endDate: string
  sessions: number
  historyDates: number
  matchedDates: number
  sessionDatesSha256: string
  historyDatesSha256: string
  exactDateReconciliation: boolean
}

function doctorQuoteObservation(value: unknown): object | null {
  const row = exactRecord(value, ["http", "semanticResultSha256", "quoteTimestamp"])
  return row && doctorHttp(row.http) && digest(row.semanticResultSha256) &&
    nullableTimestamp(row.quoteTimestamp) !== undefined ? {} : null
}

function doctorBatchObservation(value: unknown): DoctorBatchObservation | null {
  const row = exactRecord(value, [
    "http", "semanticResultSha256", "requested", "returned", "valid", "missing",
    "unexpected", "duplicate", "invalid", "requestedSetSha256", "returnedSetSha256",
    "missingSetSha256", "unexpectedSetSha256",
  ])
  const http = doctorHttp(row?.http)
  const requested = boundedInteger(row?.requested, 101)
  const returned = boundedInteger(row?.returned, 101)
  const valid = boundedInteger(row?.valid, 101)
  const missing = boundedInteger(row?.missing, 101)
  const unexpected = boundedInteger(row?.unexpected, 101)
  const duplicate = boundedInteger(row?.duplicate, 101)
  const invalid = boundedInteger(row?.invalid, 101)
  if (!row || !http || !digest(row.semanticResultSha256) || requested !== 50 ||
    returned === null || valid === null || missing === null || unexpected === null ||
    duplicate === null || invalid === null || returned + missing !== requested ||
    valid + invalid !== returned || !digest(row.requestedSetSha256) ||
    !digest(row.returnedSetSha256) || !digest(row.missingSetSha256) ||
    !digest(row.unexpectedSetSha256)) return null
  return { http, requested, returned, valid, missing }
}

function doctorStreamObservation(value: unknown): DoctorStreamObservation | null {
  const row = exactRecord(value, [
    "endpointContractSha256", "requestSha256", "connectedFrameSha256",
    "authenticatedFrameSha256", "subscriptionFrameSha256", "semanticResultSha256",
    "handshakeStatus", "handshakeRate", "subscribedTrades", "subscribedQuotes",
    "framesObserved", "bytesObserved", "authenticatedAt", "subscribedAt", "closeSent",
    "cleanCloseObserved", "completedAt",
  ])
  const handshakeRate = doctorRate(row?.handshakeRate)
  const subscribedTrades = boundedInteger(row?.subscribedTrades, 26)
  const subscribedQuotes = boundedInteger(row?.subscribedQuotes, 26)
  const frames = boundedInteger(row?.framesObserved, 26)
  const bytes = boundedInteger(row?.bytesObserved, 26 * 16 * 1024 * 1024)
  const authenticatedAt = timestamp(row?.authenticatedAt)
  const subscribedAt = timestamp(row?.subscribedAt)
  const completedAt = timestamp(row?.completedAt)
  if (!row || !digest(row.endpointContractSha256) || !digest(row.requestSha256) ||
    !digest(row.connectedFrameSha256) || !digest(row.authenticatedFrameSha256) ||
    !digest(row.subscriptionFrameSha256) || !digest(row.semanticResultSha256) ||
    boundedIntegerRange(row.handshakeStatus, 100, 599) === null || !handshakeRate ||
    subscribedTrades === null || subscribedQuotes === null || frames === null || frames < 3 ||
    bytes === null || bytes === 0 || !authenticatedAt || !subscribedAt || !completedAt ||
    authenticatedAt > subscribedAt || subscribedAt > completedAt ||
    typeof row.closeSent !== "boolean" || typeof row.cleanCloseObserved !== "boolean") return null
  return { handshakeRate }
}

function doctorHistoryObservation(value: unknown): DoctorHistoryObservation | null {
  const row = exactRecord(value, [
    "endpointContractSha256", "requestSha256", "semanticResultSha256", "startDate",
    "endDate", "pages", "bars", "distinctDates", "firstBarTimestamp", "lastBarTimestamp",
    "returnedDatesSha256", "paginationGraphSha256", "terminalPagination", "pageEvidence",
  ])
  const pages = boundedInteger(row?.pages, 8)
  const bars = nonnegativeInteger(row?.bars)
  const distinctDates = nonnegativeInteger(row?.distinctDates)
  const start = calendarDate(row?.startDate)
  const end = calendarDate(row?.endDate)
  const first = nullableTimestamp(row?.firstBarTimestamp)
  const last = nullableTimestamp(row?.lastBarTimestamp)
  if (!row || !digest(row.endpointContractSha256) || !digest(row.requestSha256) ||
    !digest(row.semanticResultSha256) || !start || !end || start > end || pages === null ||
    pages === 0 || bars === null || distinctDates !== bars || first === undefined ||
    last === undefined || (bars === 0 ? first !== null || last !== null : !first || !last) ||
    (first && last && first > last) ||
    !digest(row.returnedDatesSha256) || !digest(row.paginationGraphSha256) ||
    typeof row.terminalPagination !== "boolean" || !Array.isArray(row.pageEvidence) ||
    row.pageEvidence.length !== pages || !doctorHistoryPages(row.pageEvidence,
      row.terminalPagination)) return null
  return {
    startDate: start,
    endDate: end,
    pages,
    bars,
    distinctDates,
    returnedDatesSha256: row.returnedDatesSha256,
    terminalPagination: row.terminalPagination,
  }
}

function doctorHistoryPages(value: unknown[], terminal: boolean): boolean {
  let priorResponse: string | null = null
  const responseTokens = new Set<string>()
  for (const [index, item] of value.entries()) {
    const row = exactRecord(item, ["http", "requestPageTokenSha256", "responsePageTokenSha256"])
    const request = nullableDigest(row?.requestPageTokenSha256)
    const response = nullableDigest(row?.responsePageTokenSha256)
    const lastPage = index + 1 === value.length
    if (!row || !doctorHttp(row.http) || request === undefined || response === undefined ||
      (index === 0 ? request !== null : priorResponse === null || request !== priorResponse) ||
      (lastPage && terminal) !== (response === null) ||
      response !== null && responseTokens.has(response)) return false
    if (response !== null) responseTokens.add(response)
    priorResponse = response
  }
  return true
}

function doctorCalendarObservation(value: unknown): DoctorCalendarObservation | null {
  const row = exactRecord(value, [
    "http", "semanticResultSha256", "startDate", "endDate", "sessions", "historyDates",
    "matchedDates", "missingHistoryDates", "unexpectedHistoryDates", "sessionDatesSha256",
    "historyDatesSha256", "exactDateReconciliation",
  ])
  const sessions = nonnegativeInteger(row?.sessions)
  const historyDates = nonnegativeInteger(row?.historyDates)
  const matchedDates = nonnegativeInteger(row?.matchedDates)
  const missing = nonnegativeInteger(row?.missingHistoryDates)
  const unexpected = nonnegativeInteger(row?.unexpectedHistoryDates)
  const start = calendarDate(row?.startDate)
  const end = calendarDate(row?.endDate)
  const exactDateReconciliation = missing === 0 && unexpected === 0 &&
    historyDates !== null && matchedDates !== null && sessions !== null &&
    historyDates === matchedDates && sessions === matchedDates && matchedDates > 0
  if (!row || !doctorHttp(row.http) || !digest(row.semanticResultSha256) || !start || !end ||
    start > end || sessions === null || historyDates === null || matchedDates === null ||
    missing === null || unexpected === null || matchedDates + missing !== historyDates ||
    matchedDates + unexpected !== sessions || !digest(row.sessionDatesSha256) ||
    !digest(row.historyDatesSha256) || typeof row.exactDateReconciliation !== "boolean" ||
    row.exactDateReconciliation !== exactDateReconciliation ||
    row.exactDateReconciliation && row.sessionDatesSha256 !== row.historyDatesSha256) return null
  return {
    startDate: start,
    endDate: end,
    sessions,
    historyDates,
    matchedDates,
    sessionDatesSha256: row.sessionDatesSha256,
    historyDatesSha256: row.historyDatesSha256,
    exactDateReconciliation: row.exactDateReconciliation,
  }
}

function doctorHistoryCalendarBinding(
  history: DoctorHistoryObservation | null,
  calendar: DoctorCalendarObservation | null,
): boolean {
  if (calendar === null) return true
  return history !== null &&
    calendar.startDate === history.startDate &&
    calendar.endDate === history.endDate &&
    calendar.historyDates === history.distinctDates &&
    calendar.historyDates === history.bars &&
    calendar.historyDatesSha256 === history.returnedDatesSha256
}

function doctorHttp(value: unknown): DoctorHttpObservation | null {
  const row = exactRecord(value, [
    "endpointContractSha256", "requestSha256", "status", "bodySha256", "bytes",
    "receivedAt", "latencyNanos", "rate",
  ])
  const rate = doctorRate(row?.rate)
  return row && digest(row.endpointContractSha256) && digest(row.requestSha256) &&
    boundedIntegerRange(row.status, 100, 599) !== null && digest(row.bodySha256) &&
    boundedInteger(row.bytes, 8 * 1024 * 1024) !== null && timestamp(row.receivedAt) &&
    unsignedIntegerText(row.latencyNanos) && rate ? { rate } : null
}

function doctorRate(value: unknown): DoctorRateEvidence | null {
  const row = exactRecord(value, ["limit", "remaining", "reset_unix_seconds", "retry_after"])
  const limit = doctorObservedUnsigned(row?.limit, true)
  const remaining = doctorObservedUnsigned(row?.remaining, false)
  const reset = doctorObservedIntegerText(row?.reset_unix_seconds)
  const retry = doctorObservedRetryAfter(row?.retry_after)
  if (!row || !limit || !remaining || !reset || !retry ||
    limit.state === "observed" && remaining.state === "observed" &&
    remaining.value > limit.value) return null
  return { limit, remaining, resetUnixSeconds: reset, retryAfter: retry }
}

function doctorObservedUnsigned(value: unknown, positive: boolean): DoctorObservedUnsigned | null {
  const row = record(value)
  if (row?.state === "missing" && exactRecord(value, ["state"])) return { state: "missing" }
  const observed = exactRecord(value, ["state", "value"])
  const parsed = nonnegativeInteger(observed?.value)
  return observed?.state === "observed" && parsed !== null && (!positive || parsed > 0)
    ? { state: "observed", value: parsed }
    : null
}

function doctorObservedIntegerText(value: unknown): DoctorObservedIntegerText | null {
  const row = record(value)
  if (row?.state === "missing" && exactRecord(value, ["state"])) return { state: "missing" }
  const observed = exactRecord(value, ["state", "value"])
  const parsed = nonnegativeIntegerText(observed?.value)
  return observed?.state === "observed" && parsed !== null
    ? { state: "observed", value: parsed }
    : null
}

function doctorObservedRetryAfter(value: unknown): DoctorObservedRetryAfter | null {
  const row = record(value)
  if (row?.state === "missing" && exactRecord(value, ["state"])) return { state: "missing" }
  const observed = exactRecord(value, ["state", "value"])
  const item = exactRecord(observed?.value, ["kind", "value"])
  const parsed = nonnegativeIntegerText(item?.value)
  return observed?.state === "observed" && parsed !== null &&
    (item?.kind === "delay_seconds" || item?.kind === "at_unix_seconds")
    ? { state: "observed", value: { kind: item.kind, value: parsed } }
    : null
}

function doctorAdditionalCapabilities(value: unknown): boolean {
  const expected = [
    ["options_rest", "not_probed"], ["options_stream", "not_probed"],
    ["fixed_income", "not_probed"], ["corporate_actions", "not_probed"],
    ["sip", "unavailable"], ["nbbo", "unavailable"], ["opra", "unavailable"],
    ["price_level_depth", "unavailable"], ["order_level_depth", "unavailable"],
    ["brokerage_account", "unavailable"], ["positions", "unavailable"],
    ["orders", "unavailable"], ["trading", "unavailable"],
  ] as const
  return Array.isArray(value) && value.length === expected.length && value.every((item, index) => {
    const row = exactRecord(item, ["capability", "disposition", "evidenceSha256"])
    return row !== null && row.capability === expected[index]?.[0] &&
      row.disposition === expected[index]?.[1] &&
      digest(row.evidenceSha256)
  })
}

function doctorDisposition(value: unknown): DoctorProbeSummary["disposition"] | null {
  return value === "available" || value === "degraded" ||
    value === "unavailable" || value === "not_probed" ? value : null
}

function digest(value: unknown): value is string {
  return sha256(value)
}

function nullableDigest(value: unknown): string | null | undefined {
  return value === null ? null : digest(value) ? value : undefined
}

function timestamp(value: unknown): string | null {
  return typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$/.test(value) &&
    !Number.isNaN(Date.parse(value)) ? value : null
}

function nullableTimestamp(value: unknown): string | null | undefined {
  return value === null ? null : timestamp(value) ?? undefined
}

function calendarDate(value: unknown): string | null {
  const row = exactRecord(value, ["year", "month", "day"])
  const year = boundedIntegerRange(row?.year, 1, 9999)
  const month = boundedIntegerRange(row?.month, 1, 12)
  const day = boundedIntegerRange(row?.day, 1, 31)
  if (!row || year === null || month === null || day === null) return null
  const rendered = `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
  const parsed = new Date(`${rendered}T00:00:00.000Z`)
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().startsWith(rendered)
    ? rendered
    : null
}

function nonnegativeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : null
}

function boundedInteger(value: unknown, maximum: number): number | null {
  const parsed = nonnegativeInteger(value)
  return parsed !== null && parsed <= maximum ? parsed : null
}

function boundedIntegerRange(
  value: unknown,
  minimum: number,
  maximum: number,
): number | null {
  const parsed = nonnegativeInteger(value)
  return parsed !== null && parsed >= minimum && parsed <= maximum ? parsed : null
}

function positiveIntegerText(value: unknown): string | null {
  return typeof value === "string" && /^[1-9]\d*$/.test(value) ? value : null
}

function unsignedIntegerText(value: unknown): string | null {
  return typeof value === "string" && /^\d+$/.test(value) ? value : null
}

function nonnegativeIntegerText(value: unknown): string | null {
  return typeof value === "string" && /^(0|[1-9]\d*)$/.test(value) ? value : null
}

function exactRecord(value: unknown, keys: string[]): RecordValue | null {
  const row = record(value)
  if (!row || Object.keys(row).sort().join("\0") !== [...keys].sort().join("\0")) {
    return null
  }
  return row
}

function lifecycleSupportEvidence(
  value: unknown,
): "managed" | "not_applicable" | null {
  return value === "managed" || value === "not_applicable" ? value : null
}

function control(
  action: SourceLifecycleAction,
  label: string,
  request: SourceLifecycleRequest,
  destructive = false,
): LifecycleControl {
  return { action, label, request, destructive }
}

function indexRows(
  value: unknown,
  identity: (row: RecordValue) => string | null,
) {
  const result = new Map<string, RecordValue>()
  for (const item of Array.isArray(value) ? value : []) {
    const row = record(item)
    if (!row) continue
    const id = identity(row)
    if (id) result.set(id, row)
  }
  return result
}

function evidenceName(value: unknown): string | null {
  if (typeof value === "string") return value
  const row = record(value)
  if (!row) return null
  const named =
    text(row.state) ??
    text(row.status) ??
    text(row.quality) ??
    text(row.classification)
  if (named) return named
  const keys = Object.keys(row)
  return keys.length === 1 ? keys[0] ?? null : null
}

function unixNanos(value: unknown): string | null {
  const raw =
    typeof value === "string"
      ? value
      : typeof value === "number" && Number.isFinite(value) && value > 0
        ? String(value)
        : null
  if (!raw || !/^\d+$/.test(raw)) return null
  try {
    const milliseconds = raw.includes(".")
      ? Math.trunc(Number(raw) / 1_000_000)
      : Number(BigInt(raw) / 1_000_000n)
    const date = new Date(milliseconds)
    return Number.isNaN(date.getTime()) ? null : date.toISOString()
  } catch {
    return null
  }
}

function sha256(value: unknown) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value)
}

function uuid(value: unknown): string | null {
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value,
    )
    ? value
    : null
}

function record(value: unknown): RecordValue | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as RecordValue)
    : null
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null
}

function positiveInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0
    ? value
    : null
}
