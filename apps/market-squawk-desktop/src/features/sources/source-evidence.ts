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
  runtimeState: string | null
  sourceId: string | null
  venueId: string | null
  instrumentId: string | null
  connection: string | null
  marketFreshness: string | null
  integrity: string | null
  quality: string | null
  coverageState: string | null
  observedAt: string | null
  onboardingSessionId: string | null
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
  currentGeneration?: number
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
  const reconfigure =
    source.onboardingSessionId && lifecycle.publicConfigurationSha256
      ? [
          control("reconfigure", "Apply prepared configuration", {
            ...base,
            onboardingSessionId: source.onboardingSessionId,
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

  switch (lifecycle.state) {
    case "stopped":
      return [
        control("start", "Start", base),
        control("verify", "Verify", base),
        ...reconfigure,
        remove,
      ]
    case "active":
      return [
        control("verify", "Verify", base),
        ...(current
          ? [
              control("resynchronize", "Resynchronize", {
                ...base,
                expectedGeneration: current,
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
        remove,
      ]
    case "blocked":
      return [
        control("verify", "Verify", base),
        control("retry", "Retry", {
          ...base,
          reason: "desktop-user-request",
        }),
        ...reconfigure,
        remove,
      ]
    default:
      return []
  }
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
    runtimeState: text(runtime?.state) ?? text(runtimeHealth?.state),
    sourceId: text(runtime?.sourceId) ?? text(runtimeHealth?.sourceId),
    venueId: text(runtime?.venueId) ?? text(runtimeHealth?.venueId),
    instrumentId:
      text(runtime?.instrumentId) ?? text(runtimeHealth?.instrumentId),
    connection: evidenceName(runtime?.connection ?? runtimeHealth?.connection),
    marketFreshness: evidenceName(runtimeHealth?.marketFreshness),
    integrity: evidenceName(runtime?.integrity ?? runtimeHealth?.streamIntegrity),
    quality: evidenceName(runtime?.quality ?? runtimeHealth?.quality),
    coverageState: evidenceName(runtimeCoverage),
    observedAt: unixNanos(
      runtime?.observedAtUnixNanos ?? runtimeHealth?.observedAtUnixNanos,
    ),
    onboardingSessionId:
      text(session?.session_id) ?? bootstrapSession?.session_id ?? null,
    lifecycle: lifecycleEvidence(status?.lifecycle),
  }
}

function lifecycleEvidence(value: unknown): LifecycleEvidence | null {
  const row = record(value)
  const provider = text(row?.provider)
  const state = text(row?.state)
  const stateRevision = positiveInteger(row?.stateRevision)
  if (!provider || !state || stateRevision === null) return null

  const currentGeneration = positiveInteger(row?.currentGeneration)
  return {
    provider,
    state,
    stateRevision,
    ...(currentGeneration ? { currentGeneration } : {}),
    ...(sha256(row?.publicConfigurationSha256)
      ? { publicConfigurationSha256: text(row?.publicConfigurationSha256) ?? undefined }
      : {}),
    blocker: text(row?.blocker),
    observedAt: text(row?.observedAt),
  }
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
