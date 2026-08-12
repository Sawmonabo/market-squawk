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

interface LifecycleEvidence {
  provider: string
  state: string
  stateRevision: number
  configurationSessionId: string | null
  currentGeneration?: number
  runtimeGenerationSha256?: string
  publicConfigurationSha256?: string
  blocker: string | null
  observedAt: string | null
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
  const reconfigure =
    hasConfiguration
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
          : hasConfiguration
            ? [
                control("retry", "Resume", {
                  ...base,
                  reason: "desktop-user-request",
                }),
              ]
            : []),
        ...(LIVE_SOURCES.has(source.id)
          ? [control("verify", "Verify", base)]
          : []),
        ...reconfigure,
        ...removeControls,
      ]
    case "active":
      return [
        ...(LIVE_SOURCES.has(source.id)
          ? [control("verify", "Verify", base)]
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
        ...(LIVE_SOURCES.has(source.id)
          ? [control("verify", "Verify", base)]
          : []),
        ...(PUBLIC_LIVE_SOURCES.has(source.id) || hasConfiguration
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
  const row = record(value)
  const provider = text(row?.provider)
  const state = text(row?.state)
  const stateRevision = positiveInteger(row?.stateRevision)
  if (!provider || !state || stateRevision === null) return null

  const configurationSessionId = uuid(row?.configurationSessionId)
  const currentGeneration = positiveInteger(row?.currentGeneration)
  return {
    provider,
    state,
    stateRevision,
    configurationSessionId,
    ...(currentGeneration ? { currentGeneration } : {}),
    ...(sha256(row?.runtimeGenerationSha256)
      ? { runtimeGenerationSha256: text(row?.runtimeGenerationSha256) ?? undefined }
      : {}),
    ...(sha256(row?.publicConfigurationSha256)
      ? { publicConfigurationSha256: text(row?.publicConfigurationSha256) ?? undefined }
      : {}),
    blocker: text(row?.blocker),
    observedAt: text(row?.observedAt),
  }
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
