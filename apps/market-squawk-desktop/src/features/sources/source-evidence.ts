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
  "kraken.spot-authenticated-level3-market-data",
])
const PUBLIC_LIVE_SOURCES = new Set([
  "coinbase.public-market-data",
  "kraken.spot-public-market-data",
])
const ACCOUNT_GROUP_SOURCES = new Set([
  "alpaca.basic-market-data",
  "kraken.spot-authenticated-level3-market-data",
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
  stateRevision: string
  configurationSessionId: string | null
  currentGeneration?: string
  runtimeGenerationSha256?: string
  publicConfigurationSha256?: string
  doctor: SourceDoctorEvidence | null
  startEligibility: SourceStartEligibility
  blocker: string | null
  observedAt: string | null
}

export interface SourceStatusRow {
  profile: Record<string, unknown>
  currentSession: Record<string, unknown> | null
  providerDatasetIdentifier: string | null
  lifecycleSupport: "managed" | "not_applicable"
  lifecycle: LifecycleEvidence | null
  runtime: SourceStatusRuntime
}

type SourceConnection =
  | "connecting"
  | { live: { last_activity_at: string } }
  | { stale: { last_activity_at: string } }
  | { disconnected: { disconnected_at: string } }

type SourceStatusRuntime =
  | { state: "not_active" }
  | {
      state: "active_group"
      runtimeGenerationSha256: string
      qualifiedRuntimeRecordCount: 0
    }
  | SourceActiveRuntime

interface SourceActiveRuntime {
  state: "active"
  sourceId: string
  venueId: string
  instrumentId: string
  providerProduct: string
  providerChannel: string
  connectionGeneration: string
  sessionId: string
  healthEpoch: string
  stateRevision: string
  assessmentId: string
  bindingDigest: string
  connection: SourceConnection
  integrity: NonNullable<SourceLifecycleReceipt["integrity"]>
  quality: NonNullable<SourceLifecycleReceipt["quality"]>
  observedAtUnixNanos: string
  qualificationEvaluatedAtUnixNanos: string
  qualificationValidUntilUnixNanos: string
}

type SourceCoverageRuntime =
  | { state: "not_established" }
  | {
      state: "established"
      sourceId: string
      venueId: string
      instrumentId: string
      providerProduct: string
      providerChannel: string
      eventClass:
        | "trade"
        | "quote"
        | "book_snapshot"
        | "book_delta"
        | "auction"
        | "trading_halt"
        | "instrument_status"
        | "corporate_action"
      marketDepth: "top_of_book" | "price_level" | "order_level" | null
      delay: { kind: "real_time" } | { kind: "delayed"; value: string }
      consolidation: "single_venue" | "partial" | "consolidated"
      effectiveFromUnixNanos: string
      effectiveUntilUnixNanos: string | null
      metadataRevision: string
      status: "sufficient" | "insufficient" | "unknown"
    }

export interface SourceCoverageRow {
  surfaceId: string
  releaseState: "available" | "rights_limited" | "refresh_required" | "rights_blocked"
  declaredCoverage: string
  qualityCeiling: NonNullable<SourceLifecycleReceipt["quality"]>
  rights: Array<{
    operation: "retrieve" | "display" | "persist" | "model_training" | "export" | "redistribute"
    admission: "admitted" | "pending" | "blocked"
  }>
  runtimeCoverage: SourceCoverageRuntime
}

type SourceFreshness =
  | "uninitialized"
  | { fresh: Record<string, string> }
  | { stale: Record<string, string> }

interface SourceActiveHealth {
  state: "active"
  sourceId: string
  venueId: string
  instrumentId: string
  connectionGeneration: string
  sessionId: string
  healthEpoch: string
  stateRevision: string
  assessmentId: string
  bindingDigest: string
  connection: SourceConnection
  transportFreshness: SourceFreshness
  marketFreshness: SourceFreshness
  sourceTimestampFreshness: SourceFreshness
  streamIntegrity: NonNullable<SourceLifecycleReceipt["integrity"]>
  captureIntegrity: "disabled" | "healthy" | "incomplete"
  coverageStatus: "sufficient" | "insufficient" | "unknown"
  quality: NonNullable<SourceLifecycleReceipt["quality"]>
  observedAtUnixNanos: string
  qualificationEvaluatedAtUnixNanos: string
  qualificationValidUntilUnixNanos: string
}

export interface SourceHealthRow {
  surfaceId: string
  onboardingState:
    | "unavailable"
    | "anonymous_available"
    | "user_action_required"
    | "credential_imported_unverified"
    | "protocol_validated"
    | "stored_unverified"
    | "secret_reconciliation_required"
    | "verified_least_privilege"
    | "rights_admission_pending"
    | "runtime_verification_pending"
    | "active_scoped"
    | "renewal_required"
    | "refresh_required"
    | "rotation_pending"
    | "revocation_unconfirmed"
    | "indeterminate_remote_state"
    | "cleanup_required"
    | "blocked"
    | null
  runtimeHealth: { state: "not_active" } | SourceActiveHealth
}

export type SourceLifecycleDisposition =
  | "applied"
  | "replay"
  | "rejected"
  | "reconciliation_required"

export type SourceLifecycleRateBudget =
  | { state: "available" | "unavailable" | "indeterminate" }
  | { state: "cooling_down"; until: string }

export interface SourceLifecycleRightsEvidence {
  id: string
  sha256: string
  effectiveAt: string
  expiresAt: string | null
}

export interface SourceLifecycleReceipt {
  operationId: string
  provider: string
  action: SourceLifecycleAction
  disposition: SourceLifecycleDisposition
  state: SourceLifecycleState
  stateRevision: string
  previousGeneration: string | null
  currentGeneration: string | null
  runtimeGenerationSha256: string | null
  coverage: "sufficient" | "insufficient" | "unknown" | null
  integrity:
    | "initializing"
    | "synchronizing"
    | "validating"
    | "healthy"
    | "stale"
    | "gap_detected"
    | "checksum_failed"
    | "divergent"
    | "quarantined"
    | null
  quality:
    | "direct_verified"
    | "direct_unverified"
    | "official_delayed"
    | "aggregated"
    | "indicative"
    | "modeled"
    | "estimated"
    | "stale"
    | "quarantined"
    | null
  rateBudget: SourceLifecycleRateBudget
  authorization: "admitted" | "pending" | "blocked" | "not_required"
  availability:
    | "available"
    | "temporarily_unavailable"
    | "removed"
    | "indeterminate"
  rightsEvidence: SourceLifecycleRightsEvidence | null
  blocker: string | null
  publicConfigurationSha256: string | null
  configurationSessionId: string | null
  doctor: SourceDoctorEvidence | null
  startEligibility: SourceStartEligibility
  observedAt: string
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
  dataQuality: "direct_unverified"
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

export function parseSourceStatusResult(
  result: ApplicationResult,
  expectedProviderIds?: readonly string[],
): SourceStatusRow[] {
  const envelope = exactRecord(result, ["data", "metadata"])
  const metadata = exactRecord(envelope?.metadata, [
    "completeness",
    "returnedItems",
    "availableItems",
    "sourceCoverage",
    "dataQuality",
  ])
  const returnedItems = nonnegativeInteger(metadata?.returnedItems)
  const availableItems = nonnegativeInteger(metadata?.availableItems)
  const coverage = exactRecord(metadata?.sourceCoverage, [
    "authority",
    "requestedSources",
    "profileCount",
    "runtimeRecordCount",
    "runtimeAbsence",
  ])
  const quality = exactRecord(metadata?.dataQuality, [
    "authority",
    "runtimeClasses",
    "runtimeAbsence",
    "executionEligibilityUnchanged",
  ])
  const requestedSources = textArray(coverage?.requestedSources)
  const runtimeClasses = textArray(quality?.runtimeClasses)
  const profileCount = nonnegativeInteger(coverage?.profileCount)
  const runtimeRecordCount = nonnegativeInteger(coverage?.runtimeRecordCount)
  const rawRows = envelope?.data === null
    ? []
    : Array.isArray(envelope?.data)
      ? envelope.data
      : null
  if (
    !envelope || !metadata || returnedItems === null || availableItems === null ||
    !coverage || !quality || requestedSources === null || runtimeClasses === null ||
    profileCount === null || runtimeRecordCount === null || rawRows === null ||
    (metadata.completeness !== "complete" && metadata.completeness !== "truncated") ||
    rawRows.length !== returnedItems ||
    (envelope.data === null) !== (returnedItems === 0) ||
    availableItems < returnedItems ||
    metadata.completeness !== (returnedItems === availableItems ? "complete" : "truncated") ||
    coverage.authority !== "code_owned_profiles_and_current_runtime_evidence" ||
    coverage.runtimeAbsence !== "not_established" ||
    quality.authority !== "profile_ceiling_and_runtime_qualification" ||
    quality.runtimeAbsence !== "not_active" ||
    quality.executionEligibilityUnchanged !== true ||
    !runtimeClasses.every(sourceDataQuality) ||
    new Set(runtimeClasses).size !== runtimeClasses.length ||
    !isStrictlySorted(requestedSources) || !isStrictlySorted(runtimeClasses) ||
    (expectedProviderIds !== undefined &&
      !sameOrderedStrings(requestedSources, expectedProviderIds))
  ) {
    return invalidSourceResult("Source.GetStatus envelope")
  }

  const rows = rawRows.map((value) => parseSourceStatusRow(value))
  const profileBindings = new Map<string, string>()
  const runtimeIdentities = new Set<string>()
  const runtimeKinds = new Map<string, Set<SourceStatusRuntime["state"]>>()
  const profileRowCounts = new Map<string, number>()
  for (const [index, row] of rows.entries()) {
    const profileId = text(row.profile.id)
    if (!profileId || !requestedSources.includes(profileId)) {
      return invalidSourceResult("Source.GetStatus profile identity")
    }
    const raw = record(rawRows[index])
    if (!raw) return invalidSourceResult("Source.GetStatus row binding")
    const binding = JSON.stringify([
      raw.profile,
      raw.currentSession,
      raw.providerDatasetIdentifier,
      raw.lifecycleSupport,
      raw.lifecycle,
    ])
    const prior = profileBindings.get(profileId)
    if (prior !== undefined && prior !== binding) {
      return invalidSourceResult("Source.GetStatus repeated profile binding")
    }
    profileBindings.set(profileId, binding)
    const kinds = runtimeKinds.get(profileId) ?? new Set()
    kinds.add(row.runtime.state)
    runtimeKinds.set(profileId, kinds)
    profileRowCounts.set(profileId, (profileRowCounts.get(profileId) ?? 0) + 1)
    if (row.runtime.state === "active") {
      const identity = sourceRuntimeIdentity(row.runtime)
      if (runtimeIdentities.has(identity)) {
        return invalidSourceResult("Source.GetStatus duplicate runtime identity")
      }
      runtimeIdentities.add(identity)
    }
  }
  for (const [profileId, kinds] of runtimeKinds) {
    if (
      kinds.size !== 1 ||
      (!kinds.has("active") && (profileRowCounts.get(profileId) ?? 0) !== 1)
    ) {
      return invalidSourceResult("Source.GetStatus runtime grouping")
    }
  }
  const returnedProfiles = new Set(rows.map((row) => text(row.profile.id))).size
  const returnedRuntimeRecords = rows.filter(
    (row) => row.runtime.state === "active",
  ).length
  const returnedRuntimeClasses = [...new Set(rows.flatMap((row) =>
    row.runtime.state === "active" ? [row.runtime.quality] : [],
  ))].sort()
  if (
    profileCount > availableItems ||
    runtimeRecordCount > availableItems ||
    returnedProfiles > profileCount ||
    returnedRuntimeRecords > runtimeRecordCount ||
    (metadata.completeness === "complete" &&
      (returnedProfiles !== profileCount ||
        returnedRuntimeRecords !== runtimeRecordCount ||
        !sameOrderedStrings(returnedRuntimeClasses, runtimeClasses))) ||
    returnedRuntimeClasses.some((quality) => !runtimeClasses.includes(quality))
  ) {
    return invalidSourceResult("Source.GetStatus metadata counts")
  }
  return rows
}

interface ExpectedSecondarySourceRow {
  profile: ProviderProfile
  status: SourceStatusRow
}

export function parseSourceCoverageResult(
  result: ApplicationResult,
  profiles: readonly ProviderProfile[],
  statuses: readonly SourceStatusRow[],
  expectedRequestedSources: readonly string[] = [],
): SourceCoverageRow[] {
  const expected = expectedSecondarySourceRows(profiles, statuses)
  const rawRows = parseSecondarySourceEnvelope(
    result,
    expected,
    expectedRequestedSources,
    "Source.GetCoverage",
  )
  const rows = rawRows.map(sourceCoverageRow)
  const identities = new Set<string>()
  rows.forEach((row, index) => {
    const binding = expected[index]
    if (!binding || !coverageRowBinding(row, binding)) {
      invalidSourceResult("Source.GetCoverage row binding")
    }
    const identity = row.runtimeCoverage.state === "established"
      ? [
          row.surfaceId,
          row.runtimeCoverage.sourceId,
          row.runtimeCoverage.venueId,
          row.runtimeCoverage.instrumentId,
          row.runtimeCoverage.providerProduct,
          row.runtimeCoverage.providerChannel,
        ].join("\0")
      : row.surfaceId
    if (identities.has(identity)) {
      invalidSourceResult("Source.GetCoverage duplicate identity")
    }
    identities.add(identity)
  })
  return rows
}

export function parseSourceHealthResult(
  result: ApplicationResult,
  profiles: readonly ProviderProfile[],
  statuses: readonly SourceStatusRow[],
  expectedRequestedSources: readonly string[] = [],
): SourceHealthRow[] {
  const expected = expectedSecondarySourceRows(profiles, statuses)
  const rawRows = parseSecondarySourceEnvelope(
    result,
    expected,
    expectedRequestedSources,
    "Source.GetHealth",
  )
  const rows = rawRows.map(sourceHealthRow)
  const identities = new Set<string>()
  rows.forEach((row, index) => {
    const binding = expected[index]
    if (!binding || !healthRowBinding(row, binding)) {
      invalidSourceResult("Source.GetHealth row binding")
    }
    const identity = row.runtimeHealth.state === "active"
      ? [
          row.surfaceId,
          row.runtimeHealth.sourceId,
          row.runtimeHealth.venueId,
          row.runtimeHealth.instrumentId,
          row.runtimeHealth.connectionGeneration,
          row.runtimeHealth.sessionId,
          row.runtimeHealth.healthEpoch,
          row.runtimeHealth.stateRevision,
        ].join("\0")
      : row.surfaceId
    if (identities.has(identity)) {
      invalidSourceResult("Source.GetHealth duplicate identity")
    }
    identities.add(identity)
  })
  return rows
}

function expectedSecondarySourceRows(
  profiles: readonly ProviderProfile[],
  statuses: readonly SourceStatusRow[],
): ExpectedSecondarySourceRow[] {
  const profileById = new Map<string, ProviderProfile>()
  for (const profile of profiles) {
    if (profileById.has(profile.id)) {
      invalidSourceResult("provider bootstrap duplicate profile identity")
    }
    profileById.set(profile.id, profile)
  }
  const statusById = new Map<string, SourceStatusRow[]>()
  const runtimeIdentities = new Set<string>()
  for (const status of statuses) {
    const profileId = text(status.profile.id)
    const profile = profileId ? profileById.get(profileId) : undefined
    if (!profileId || !profile || !statusProfileBinding(profile, status.profile)) {
      invalidSourceResult("Source.GetStatus bootstrap profile binding")
    }
    const rows = statusById.get(profileId) ?? []
    rows.push(status)
    statusById.set(profileId, rows)
    if (status.runtime.state === "active") {
      const identity = sourceRuntimeIdentity(status.runtime)
      if (runtimeIdentities.has(identity)) {
        invalidSourceResult("Source.GetStatus duplicate runtime identity")
      }
      runtimeIdentities.add(identity)
    }
  }

  const expected: ExpectedSecondarySourceRow[] = []
  for (const profile of profiles) {
    const rows = statusById.get(profile.id)
    if (!rows?.length) {
      invalidSourceResult("Source.GetStatus missing profile authority")
    }
    const staticBinding = JSON.stringify([
      rows[0]?.profile,
      rows[0]?.currentSession,
      rows[0]?.providerDatasetIdentifier,
      rows[0]?.lifecycleSupport,
      rows[0]?.lifecycle,
    ])
    const states = new Set(rows.map((row) => row.runtime.state))
    if (
      states.size !== 1 ||
      (!states.has("active") && rows.length !== 1) ||
      rows.some((row) => JSON.stringify([
        row.profile,
        row.currentSession,
        row.providerDatasetIdentifier,
        row.lifecycleSupport,
        row.lifecycle,
      ]) !== staticBinding)
    ) {
      invalidSourceResult("Source.GetStatus repeated profile authority")
    }
    expected.push(...rows.map((status) => ({ profile, status })))
  }
  return expected
}

function parseSecondarySourceEnvelope(
  result: ApplicationResult,
  expected: readonly ExpectedSecondarySourceRow[],
  expectedRequestedSources: readonly string[],
  label: string,
): unknown[] {
  const envelope = exactRecord(result, ["data", "metadata"])
  const metadata = exactRecord(envelope?.metadata, [
    "completeness",
    "returnedItems",
    "availableItems",
    "sourceCoverage",
    "dataQuality",
  ])
  const coverage = exactRecord(metadata?.sourceCoverage, [
    "authority",
    "requestedSources",
    "profileCount",
    "runtimeRecordCount",
    "runtimeAbsence",
  ])
  const quality = exactRecord(metadata?.dataQuality, [
    "authority",
    "runtimeClasses",
    "runtimeAbsence",
    "executionEligibilityUnchanged",
  ])
  const returnedItems = nonnegativeInteger(metadata?.returnedItems)
  const availableItems = nonnegativeInteger(metadata?.availableItems)
  const requestedSources = textArray(coverage?.requestedSources)
  const profileCount = nonnegativeInteger(coverage?.profileCount)
  const runtimeRecordCount = nonnegativeInteger(coverage?.runtimeRecordCount)
  const runtimeClasses = textArray(quality?.runtimeClasses)
  const rawRows = envelope?.data === null
    ? []
    : Array.isArray(envelope?.data)
      ? envelope.data
      : null
  const expectedProfiles = new Set(
    expected.map((binding) => binding.profile.id),
  ).size
  const expectedRuntime = expected.filter(
    (binding) => binding.status.runtime.state === "active",
  ).length
  const expectedRuntimeClasses = [...new Set(expected.flatMap((binding) =>
    binding.status.runtime.state === "active"
      ? [binding.status.runtime.quality]
      : [],
  ))].sort()
  if (
    !envelope || !metadata || !coverage || !quality ||
    returnedItems === null || availableItems === null ||
    requestedSources === null || profileCount === null ||
    runtimeRecordCount === null || runtimeClasses === null || rawRows === null ||
    (metadata.completeness !== "complete" && metadata.completeness !== "truncated") ||
    rawRows.length !== returnedItems ||
    (envelope.data === null) !== (returnedItems === 0) ||
    availableItems !== expected.length || returnedItems > availableItems ||
    metadata.completeness !== (returnedItems === availableItems ? "complete" : "truncated") ||
    coverage.authority !== "code_owned_profiles_and_current_runtime_evidence" ||
    coverage.runtimeAbsence !== "not_established" ||
    profileCount !== expectedProfiles || runtimeRecordCount !== expectedRuntime ||
    quality.authority !== "profile_ceiling_and_runtime_qualification" ||
    quality.runtimeAbsence !== "not_active" ||
    quality.executionEligibilityUnchanged !== true ||
    !sameOrderedStrings(requestedSources, expectedRequestedSources) ||
    !isStrictlySorted(requestedSources) ||
    !sameOrderedStrings(runtimeClasses, expectedRuntimeClasses) ||
    !isStrictlySorted(runtimeClasses)
  ) {
    invalidSourceResult(`${label} envelope`)
  }
  return rawRows
}

function sourceCoverageRow(value: unknown): SourceCoverageRow {
  const row = exactRecord(value, [
    "surfaceId",
    "releaseState",
    "declaredCoverage",
    "qualityCeiling",
    "rights",
    "runtimeCoverage",
  ])
  const surfaceId = boundedText(row?.surfaceId, 512)
  const releaseState = sourceReleaseState(row?.releaseState)
  const declaredCoverage = text(row?.declaredCoverage)
  const qualityCeiling = sourceDataQuality(row?.qualityCeiling)
    ? row?.qualityCeiling
    : null
  const rights = sourceDataUseRights(row?.rights)
  const runtimeCoverage = sourceRuntimeCoverage(row?.runtimeCoverage)
  if (
    !row || !surfaceId || !releaseState || !declaredCoverage || !qualityCeiling ||
    !rights || !runtimeCoverage
  ) {
    return invalidSourceResult("Source.GetCoverage row")
  }
  return {
    surfaceId,
    releaseState,
    declaredCoverage,
    qualityCeiling,
    rights,
    runtimeCoverage,
  }
}

function sourceHealthRow(value: unknown): SourceHealthRow {
  const row = exactRecord(value, ["surfaceId", "onboardingState", "runtimeHealth"])
  const surfaceId = boundedText(row?.surfaceId, 512)
  const onboardingState = row?.onboardingState === null
    ? null
    : sourceOnboardingState(row?.onboardingState)
  const runtimeHealth = sourceRuntimeHealth(row?.runtimeHealth)
  if (
    !row || !surfaceId ||
    (row.onboardingState !== null && !onboardingState) || !runtimeHealth
  ) {
    return invalidSourceResult("Source.GetHealth row")
  }
  return { surfaceId, onboardingState, runtimeHealth }
}

function coverageRowBinding(
  row: SourceCoverageRow,
  binding: ExpectedSecondarySourceRow,
) {
  const { profile, status } = binding
  const expectedRights = sourceDataUseRights(status.profile.rights)
  if (
    row.surfaceId !== profile.id ||
    row.releaseState !== status.profile.release_state ||
    row.declaredCoverage !== status.profile.coverage ||
    row.qualityCeiling !== status.profile.quality_ceiling ||
    expectedRights === null || !sameDataUseRights(row.rights, expectedRights)
  ) return false
  if (status.runtime.state !== "active") {
    return row.runtimeCoverage.state === "not_established"
  }
  const coverage = row.runtimeCoverage
  return coverage.state === "established" &&
    coverage.sourceId === status.runtime.sourceId &&
    coverage.venueId === status.runtime.venueId &&
    coverage.instrumentId === status.runtime.instrumentId &&
    coverage.providerProduct === status.runtime.providerProduct &&
    coverage.providerChannel === status.runtime.providerChannel
}

function healthRowBinding(
  row: SourceHealthRow,
  binding: ExpectedSecondarySourceRow,
) {
  const { profile, status } = binding
  if (
    row.surfaceId !== profile.id ||
    row.onboardingState !== (text(status.currentSession?.state) ?? null)
  ) return false
  if (status.runtime.state !== "active") {
    return row.runtimeHealth.state === "not_active"
  }
  const health = row.runtimeHealth
  return health.state === "active" &&
    health.sourceId === status.runtime.sourceId &&
    health.venueId === status.runtime.venueId &&
    health.instrumentId === status.runtime.instrumentId &&
    health.connectionGeneration === status.runtime.connectionGeneration &&
    health.sessionId === status.runtime.sessionId &&
    health.healthEpoch === status.runtime.healthEpoch &&
    health.stateRevision === status.runtime.stateRevision &&
    health.assessmentId === status.runtime.assessmentId &&
    health.bindingDigest === status.runtime.bindingDigest &&
    JSON.stringify(health.connection) === JSON.stringify(status.runtime.connection) &&
    health.streamIntegrity === status.runtime.integrity &&
    health.quality === status.runtime.quality &&
    health.observedAtUnixNanos === status.runtime.observedAtUnixNanos &&
    health.qualificationEvaluatedAtUnixNanos ===
      status.runtime.qualificationEvaluatedAtUnixNanos &&
    health.qualificationValidUntilUnixNanos ===
      status.runtime.qualificationValidUntilUnixNanos
}

export function parseSourceLifecycleReceipt(
  result: ApplicationResult,
  action: SourceLifecycleAction,
  request: SourceLifecycleRequest,
): SourceLifecycleReceipt {
  validateLifecycleRequest(action, request)
  const envelope = exactRecord(result, ["data", "metadata"])
  const metadata = exactRecord(envelope?.metadata, [
    "completeness",
    "returnedItems",
    "availableItems",
    "sourceCoverage",
    "dataQuality",
  ])
  if (
    !envelope || !metadata || metadata.completeness !== "complete" ||
    metadata.returnedItems !== 1 || metadata.availableItems !== 1 ||
    !notApplicableEvidence(metadata.sourceCoverage) ||
    !notApplicableEvidence(metadata.dataQuality)
  ) {
    return invalidSourceResult("source lifecycle result envelope")
  }

  const row = exactRecord(envelope.data, [
    "operationId", "provider", "action", "disposition", "state", "stateRevision",
    "previousGeneration", "currentGeneration", "runtimeGenerationSha256", "coverage",
    "integrity", "quality", "rateBudget", "authorization", "availability",
    "rightsEvidence", "blocker", "publicConfigurationSha256", "configurationSessionId",
    "doctor", "startEligibility", "observedAt",
  ])
  const operationId = text(row?.operationId)
  const provider = text(row?.provider)
  const disposition = lifecycleDisposition(row?.disposition)
  const state = sourceLifecycleState(row?.state)
  const stateRevision = positiveIntegerText(row?.stateRevision)
  const previousGeneration = nullablePositiveIntegerText(row?.previousGeneration)
  const currentGeneration = nullablePositiveIntegerText(row?.currentGeneration)
  const runtimeGenerationSha256 = nullableSha256(row?.runtimeGenerationSha256)
  const coverage = sourceCoverageStatus(row?.coverage)
  const integrity = sourceStreamIntegrity(row?.integrity)
  const quality = nullableSourceDataQuality(row?.quality)
  const rateBudget = sourceRateBudget(row?.rateBudget)
  const authorization = sourceAuthorization(row?.authorization)
  const availability = sourceAvailability(row?.availability)
  const rightsEvidence = sourceRightsEvidence(row?.rightsEvidence)
  const blocker = sourceLifecycleBlocker(row?.blocker)
  const publicConfigurationSha256 = nullableSha256(row?.publicConfigurationSha256)
  const configurationSessionId = nullableUuid(row?.configurationSessionId)
  const doctor = sourceDoctorEvidence(row?.doctor)
  const startEligibility = sourceStartEligibility(row?.startEligibility)
  const observedAt = timestamp(row?.observedAt)
  const scalarLiveEvidence = [currentGeneration, coverage, integrity, quality]
  const hasScalarLiveEvidence = scalarLiveEvidence.some((value) => value !== null)
  const hasAllScalarLiveEvidence = scalarLiveEvidence.every((value) => value !== null)

  if (
    !row || !operationId || !provider || row.action !== action || provider !== request.provider ||
    !disposition || !state || stateRevision === null || previousGeneration === undefined ||
    currentGeneration === undefined || runtimeGenerationSha256 === undefined ||
    coverage === undefined || integrity === undefined || quality === undefined ||
    !rateBudget || !authorization || !availability || rightsEvidence === undefined ||
    (row.blocker !== null && blocker === null) ||
    publicConfigurationSha256 === undefined || configurationSessionId === undefined ||
    (row.doctor !== null && doctor === null) || !startEligibility || !observedAt ||
    (doctor !== null && doctor.current !==
      (doctor.verifiedAt <= observedAt && observedAt < doctor.exclusiveExpiresAt)) ||
    compareUnsignedIntegerText(stateRevision, request.expectedStateRevision) < 0 ||
    hasScalarLiveEvidence !== hasAllScalarLiveEvidence ||
    (state === "active") !==
      ((currentGeneration !== null) !== (runtimeGenerationSha256 !== null)) ||
    (state === "removed" &&
      (currentGeneration !== null || runtimeGenerationSha256 !== null)) ||
    (disposition === "rejected" || disposition === "reconciliation_required") !==
      (blocker !== null) ||
    (configurationSessionId !== null) !== (publicConfigurationSha256 !== null) ||
    !doctorReceiptBinding(
      provider,
      configurationSessionId,
      publicConfigurationSha256,
      doctor,
    ) ||
    !receiptStartEligibilityBinding(
      provider,
      state,
      startEligibility,
      doctor,
      rightsEvidence,
      authorization,
      availability,
      observedAt,
    ) ||
    !requestReceiptBinding(
      action,
      request,
      disposition,
      previousGeneration,
      currentGeneration,
      runtimeGenerationSha256,
      configurationSessionId,
      publicConfigurationSha256,
    )
  ) {
    return invalidSourceResult("source lifecycle receipt")
  }

  return {
    operationId,
    provider,
    action,
    disposition,
    state,
    stateRevision,
    previousGeneration,
    currentGeneration,
    runtimeGenerationSha256,
    coverage,
    integrity,
    quality,
    rateBudget,
    authorization,
    availability,
    rightsEvidence,
    blocker,
    publicConfigurationSha256,
    configurationSessionId,
    doctor,
    startEligibility,
    observedAt,
  }
}

export function sourceEvidence(
  profiles: ProviderProfile[],
  sessions: ProviderSession[],
  statuses: SourceStatusRow[],
  coverage: ApplicationResult | undefined,
  health: ApplicationResult | undefined,
): SourceEvidence[] {
  const profileById = new Map<string, ProviderProfile>()
  for (const profile of profiles) {
    if (profileById.has(profile.id)) {
      return invalidSourceResult("provider bootstrap duplicate profile identity")
    }
    profileById.set(profile.id, profile)
  }
  const statusById = new Map<string, SourceStatusRow[]>()
  for (const status of statuses) {
    const id = text(status.profile.id)
    const profile = id ? profileById.get(id) : undefined
    if (!id || !profile || !statusProfileBinding(profile, status.profile)) {
      return invalidSourceResult("Source.GetStatus profile identity")
    }
    const rows = statusById.get(id) ?? []
    rows.push(status)
    statusById.set(id, rows)
  }
  const runtimeIdentities = new Set<string>()
  for (const rows of statusById.values()) {
    const first = rows[0]
    const binding = JSON.stringify([
      first?.profile,
      first?.currentSession,
      first?.providerDatasetIdentifier,
      first?.lifecycleSupport,
      first?.lifecycle,
    ])
    const states = new Set(rows.map((row) => row.runtime.state))
    if (
      states.size !== 1 ||
      (!states.has("active") && rows.length !== 1) ||
      rows.some((row) => JSON.stringify([
        row.profile,
        row.currentSession,
        row.providerDatasetIdentifier,
        row.lifecycleSupport,
        row.lifecycle,
      ]) !== binding)
    ) {
      return invalidSourceResult("Source.GetStatus repeated profile authority")
    }
    for (const row of rows) {
      if (row.runtime.state !== "active") continue
      const identity = sourceRuntimeIdentity(row.runtime)
      if (runtimeIdentities.has(identity)) {
        return invalidSourceResult("Source.GetStatus duplicate runtime identity")
      }
      runtimeIdentities.add(identity)
    }
  }
  const completeStatusAuthority = profiles.every(
    (profile) => (statusById.get(profile.id)?.length ?? 0) > 0,
  )
  const coverageRows = coverage && completeStatusAuthority
    ? parseSourceCoverageResult(coverage, profiles, statuses)
    : []
  const healthRows = health && completeStatusAuthority
    ? parseSourceHealthResult(health, profiles, statuses)
    : []
  const coverageById = groupRows(coverageRows, (row) => row.surfaceId)
  const healthById = groupRows(healthRows, (row) => row.surfaceId)

  return [...profileById.keys()]
    .map((id) =>
      toEvidence(
        id,
        profileById.get(id),
        sessions.find((session) => session.surface_id === id),
        statusById.get(id) ?? [],
        coverageById.get(id) ?? [],
        healthById.get(id) ?? [],
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
  statuses: SourceStatusRow[],
  coverageRows: SourceCoverageRow[],
  healthRows: SourceHealthRow[],
): SourceEvidence {
  const status = statuses[0]
  const profile = status?.profile ?? null
  const session = status?.currentSession ?? null
  const runtimes = statuses.map((row) => row.runtime)
  const activeRuntimes = runtimes.filter(
    (runtime): runtime is SourceActiveRuntime => runtime.state === "active",
  )
  const lifecycle = status?.lifecycle ?? null
  const lifecycleSupport = status?.lifecycleSupport ?? null
  const runtimeState = commonValue(runtimes.map((runtime) => runtime.state))
  const providerDatasetIdentifier = status?.providerDatasetIdentifier ?? null
  const completeCoverage = coverageRows.length === statuses.length
  const completeHealth = healthRows.length === statuses.length
  const establishedCoverage = completeCoverage
    ? coverageRows.flatMap((row) =>
        row.runtimeCoverage.state === "established" ? [row.runtimeCoverage] : [],
      )
    : []
  const activeHealth = completeHealth
    ? healthRows.flatMap((row) =>
        row.runtimeHealth.state === "active" ? [row.runtimeHealth] : [],
      )
    : []
  const statusSourceId = commonValue(activeRuntimes.map((runtime) => runtime.sourceId))
  const statusVenueId = commonValue(activeRuntimes.map((runtime) => runtime.venueId))
  const statusInstrumentId = commonValue(
    activeRuntimes.map((runtime) => runtime.instrumentId),
  )
  const statusConnection = commonValue(
    activeRuntimes.map((runtime) => evidenceName(runtime.connection)),
  )
  const statusIntegrity = commonValue(
    activeRuntimes.map((runtime) => runtime.integrity),
  )
  const statusQuality = commonValue(activeRuntimes.map((runtime) => runtime.quality))
  const statusObserved = commonValue(
    activeRuntimes.map((runtime) => runtime.observedAtUnixNanos),
  )

  return {
    id,
    name:
      text(profile?.display_name) ?? bootstrapProfile?.display_name ?? id,
    declaredCoverage:
      text(profile?.coverage) ??
      bootstrapProfile?.coverage ??
      null,
    qualityCeiling:
      text(profile?.quality_ceiling) ??
      bootstrapProfile?.quality_ceiling ??
      null,
    releaseState:
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
      text(session?.state) ?? bootstrapSession?.state ?? null,
    nextAction:
      text(session?.next_action) ?? bootstrapSession?.next_action ?? null,
    lifecycleSupport,
    operationalState:
      lifecycle?.state ??
      (runtimeState === "active" || runtimeState === "active_group"
        ? "active"
        : runtimeState === "not_active"
          ? "stopped"
          : null),
    runtimeState,
    sourceId: statusSourceId,
    venueId: statusVenueId,
    instrumentId: statusInstrumentId,
    connection: statusConnection,
    marketFreshness: commonValue(
      activeHealth.map((runtime) => evidenceName(runtime.marketFreshness)),
    ),
    integrity: statusIntegrity,
    quality: statusQuality,
    coverageState: commonValue(
      establishedCoverage.map((runtime) => runtime.status),
    ),
    runtimeObservedAt: unixNanos(statusObserved),
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
  const stateRevision = positiveIntegerText(row?.stateRevision)
  if (!provider || !state || stateRevision === null) return null

  const configurationSessionId = uuid(row?.configurationSessionId)
  const currentGeneration = positiveIntegerText(row?.currentGeneration)
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
    doctorActivationReady(doctor)
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

function parseSourceStatusRow(value: unknown): SourceStatusRow {
  const row = exactRecord(value, [
    "profile", "currentSession", "providerDatasetIdentifier", "lifecycleSupport",
    "lifecycle", "runtime",
  ])
  const profile = record(row?.profile)
  const profileId = text(profile?.id)
  const currentSession = row?.currentSession === null ? null : record(row?.currentSession)
  const providerDatasetIdentifier = row?.providerDatasetIdentifier === null
    ? null
    : text(row?.providerDatasetIdentifier)
  const support = lifecycleSupportEvidence(row?.lifecycleSupport)
  const lifecycle = lifecycleEvidence(row?.lifecycle)
  const runtime = sourceStatusRuntime(row?.runtime)
  const accountGroup = profileId !== null && ACCOUNT_GROUP_SOURCES.has(profileId)
  if (
    !row || !profile || !profileId ||
    (row.currentSession !== null && !currentSession) ||
    (row.providerDatasetIdentifier !== null && !providerDatasetIdentifier) ||
    !support || !runtime ||
    (support === "managed") !== (lifecycle !== null) ||
    (row.lifecycle !== null && lifecycle === null) ||
    (lifecycle !== null && lifecycle.provider !== profileId) ||
    (accountGroup
      ? runtime?.state === "active" ||
        (lifecycle?.state === "active") !== (runtime?.state === "active_group")
      : runtime?.state === "active_group") ||
    (runtime?.state === "active_group" &&
      lifecycle?.runtimeGenerationSha256 !== runtime.runtimeGenerationSha256) ||
    (currentSession !== null && text(currentSession.surface_id) !== profileId)
  ) {
    return invalidSourceResult("Source.GetStatus row")
  }
  return {
    profile,
    currentSession,
    providerDatasetIdentifier,
    lifecycleSupport: support,
    lifecycle,
    runtime,
  }
}

function sourceStatusRuntime(value: unknown): SourceStatusRuntime | null {
  const inactive = exactRecord(value, ["state"])
  if (inactive?.state === "not_active") return { state: "not_active" }
  const activeGroup = exactRecord(value, [
    "state", "runtimeGenerationSha256", "qualifiedRuntimeRecordCount",
  ])
  if (
    activeGroup?.state === "active_group" &&
    nonzeroSha256(activeGroup.runtimeGenerationSha256) &&
    activeGroup.qualifiedRuntimeRecordCount === 0
  ) {
    return {
      state: "active_group",
      runtimeGenerationSha256: activeGroup.runtimeGenerationSha256,
      qualifiedRuntimeRecordCount: 0,
    }
  }
  const active = exactRecord(value, [
    "state", "sourceId", "venueId", "instrumentId", "providerProduct",
    "providerChannel", "connectionGeneration", "sessionId", "healthEpoch",
    "stateRevision", "assessmentId", "bindingDigest", "connection", "integrity",
    "quality", "observedAtUnixNanos", "qualificationEvaluatedAtUnixNanos",
    "qualificationValidUntilUnixNanos",
  ])
  const sourceId = boundedText(active?.sourceId, 128)
  const venueId = boundedText(active?.venueId, 64)
  const instrumentId = uuid(active?.instrumentId)
  const providerProduct = boundedText(active?.providerProduct, 512)
  const providerChannel = boundedText(active?.providerChannel, 512)
  const connectionGeneration = positiveIntegerText(active?.connectionGeneration)
  const sessionId = boundedText(active?.sessionId, 512)
  const healthEpoch = positiveIntegerText(active?.healthEpoch)
  const stateRevision = positiveIntegerText(active?.stateRevision)
  const assessmentId = boundedText(active?.assessmentId, 512)
  const bindingDigest = sha256(active?.bindingDigest) ? active.bindingDigest : null
  const connection = sourceConnection(active?.connection)
  const integrity = sourceStreamIntegrity(active?.integrity)
  const quality = sourceDataQuality(active?.quality) ? active.quality : null
  const observedAtUnixNanos = integerText(active?.observedAtUnixNanos)
  const qualificationEvaluatedAtUnixNanos = integerText(
    active?.qualificationEvaluatedAtUnixNanos,
  )
  const qualificationValidUntilUnixNanos = integerText(
    active?.qualificationValidUntilUnixNanos,
  )
  if (
    active?.state !== "active" || !sourceId || !venueId || !instrumentId ||
    !providerProduct || !providerChannel || connectionGeneration === null ||
    !sessionId || healthEpoch === null || stateRevision === null || !assessmentId ||
    !bindingDigest || !connection || !integrity || !quality ||
    observedAtUnixNanos === null || qualificationEvaluatedAtUnixNanos === null ||
    qualificationValidUntilUnixNanos === null
  ) return null
  return {
    state: "active",
    sourceId,
    venueId,
    instrumentId,
    providerProduct,
    providerChannel,
    connectionGeneration,
    sessionId,
    healthEpoch,
    stateRevision,
    assessmentId,
    bindingDigest,
    connection,
    integrity,
    quality,
    observedAtUnixNanos,
    qualificationEvaluatedAtUnixNanos,
    qualificationValidUntilUnixNanos,
  }
}

function sourceConnection(value: unknown): SourceConnection | null {
  if (value === "connecting") return value
  const row = record(value)
  if (!row || Object.keys(row).length !== 1) return null
  if ("live" in row) {
    const evidence = exactRecord(row.live, ["last_activity_at"])
    const at = integerText(evidence?.last_activity_at)
    return at === null ? null : { live: { last_activity_at: at } }
  }
  if ("stale" in row) {
    const evidence = exactRecord(row.stale, ["last_activity_at"])
    const at = integerText(evidence?.last_activity_at)
    return at === null ? null : { stale: { last_activity_at: at } }
  }
  if ("disconnected" in row) {
    const evidence = exactRecord(row.disconnected, ["disconnected_at"])
    const at = integerText(evidence?.disconnected_at)
    return at === null ? null : { disconnected: { disconnected_at: at } }
  }
  return null
}

function sourceRuntimeCoverage(value: unknown): SourceCoverageRuntime | null {
  const inactive = exactRecord(value, ["state"])
  if (inactive?.state === "not_established") return { state: "not_established" }
  const row = exactRecord(value, [
    "state",
    "sourceId",
    "venueId",
    "instrumentId",
    "providerProduct",
    "providerChannel",
    "eventClass",
    "marketDepth",
    "delay",
    "consolidation",
    "effectiveFromUnixNanos",
    "effectiveUntilUnixNanos",
    "metadataRevision",
    "status",
  ])
  const sourceId = boundedText(row?.sourceId, 128)
  const venueId = boundedText(row?.venueId, 64)
  const instrumentId = uuid(row?.instrumentId)
  const providerProduct = boundedText(row?.providerProduct, 512)
  const providerChannel = boundedText(row?.providerChannel, 512)
  const eventClass = sourceEventClass(row?.eventClass)
  const marketDepth = row?.marketDepth === null ? null : sourceMarketDepth(row?.marketDepth)
  const delay = sourceCoverageDelay(row?.delay)
  const consolidation = sourceConsolidation(row?.consolidation)
  const effectiveFromUnixNanos = integerText(row?.effectiveFromUnixNanos)
  const effectiveUntilUnixNanos = row?.effectiveUntilUnixNanos === null
    ? null
    : integerText(row?.effectiveUntilUnixNanos)
  const metadataRevision = boundedText(row?.metadataRevision, 512)
  const status = sourceCoverageValue(row?.status)
  if (
    row?.state !== "established" || !sourceId || !venueId || !instrumentId ||
    !providerProduct || !providerChannel || !eventClass ||
    (row.marketDepth !== null && !marketDepth) || !delay || !consolidation ||
    effectiveFromUnixNanos === null ||
    (row.effectiveUntilUnixNanos !== null && effectiveUntilUnixNanos === null) ||
    (effectiveUntilUnixNanos !== null &&
      compareIntegerText(effectiveUntilUnixNanos, effectiveFromUnixNanos) <= 0) ||
    !metadataRevision || !status
  ) return null
  return {
    state: "established",
    sourceId,
    venueId,
    instrumentId,
    providerProduct,
    providerChannel,
    eventClass,
    marketDepth,
    delay,
    consolidation,
    effectiveFromUnixNanos,
    effectiveUntilUnixNanos,
    metadataRevision,
    status,
  }
}

function sourceRuntimeHealth(
  value: unknown,
): SourceHealthRow["runtimeHealth"] | null {
  const inactive = exactRecord(value, ["state"])
  if (inactive?.state === "not_active") return { state: "not_active" }
  const row = exactRecord(value, [
    "state",
    "sourceId",
    "venueId",
    "instrumentId",
    "connectionGeneration",
    "sessionId",
    "healthEpoch",
    "stateRevision",
    "assessmentId",
    "bindingDigest",
    "connection",
    "transportFreshness",
    "marketFreshness",
    "sourceTimestampFreshness",
    "streamIntegrity",
    "captureIntegrity",
    "coverageStatus",
    "quality",
    "observedAtUnixNanos",
    "qualificationEvaluatedAtUnixNanos",
    "qualificationValidUntilUnixNanos",
  ])
  const sourceId = boundedText(row?.sourceId, 128)
  const venueId = boundedText(row?.venueId, 64)
  const instrumentId = uuid(row?.instrumentId)
  const connectionGeneration = positiveIntegerText(row?.connectionGeneration)
  const sessionId = boundedText(row?.sessionId, 512)
  const healthEpoch = positiveIntegerText(row?.healthEpoch)
  const stateRevision = positiveIntegerText(row?.stateRevision)
  const assessmentId = boundedText(row?.assessmentId, 512)
  const bindingDigest = sha256(row?.bindingDigest) ? row.bindingDigest : null
  const connection = sourceConnection(row?.connection)
  const transportFreshness = sourceFreshness(
    row?.transportFreshness,
    "last_transport_at",
  )
  const marketFreshness = sourceFreshness(row?.marketFreshness, "last_market_at")
  const sourceTimestampFreshness = sourceFreshness(
    row?.sourceTimestampFreshness,
    "last_source_at",
  )
  const streamIntegrity = sourceStreamIntegrity(row?.streamIntegrity)
  const captureIntegrity = sourceCaptureIntegrity(row?.captureIntegrity)
  const coverageStatus = sourceCoverageValue(row?.coverageStatus)
  const quality = sourceDataQuality(row?.quality) ? row.quality : null
  const observedAtUnixNanos = integerText(row?.observedAtUnixNanos)
  const qualificationEvaluatedAtUnixNanos = integerText(
    row?.qualificationEvaluatedAtUnixNanos,
  )
  const qualificationValidUntilUnixNanos = integerText(
    row?.qualificationValidUntilUnixNanos,
  )
  if (
    row?.state !== "active" || !sourceId || !venueId || !instrumentId ||
    connectionGeneration === null || !sessionId || healthEpoch === null ||
    stateRevision === null || !assessmentId || !bindingDigest || !connection ||
    !transportFreshness || !marketFreshness || !sourceTimestampFreshness ||
    !streamIntegrity || !captureIntegrity || !coverageStatus || !quality ||
    observedAtUnixNanos === null || qualificationEvaluatedAtUnixNanos === null ||
    qualificationValidUntilUnixNanos === null
  ) return null
  return {
    state: "active",
    sourceId,
    venueId,
    instrumentId,
    connectionGeneration,
    sessionId,
    healthEpoch,
    stateRevision,
    assessmentId,
    bindingDigest,
    connection,
    transportFreshness,
    marketFreshness,
    sourceTimestampFreshness,
    streamIntegrity,
    captureIntegrity,
    coverageStatus,
    quality,
    observedAtUnixNanos,
    qualificationEvaluatedAtUnixNanos,
    qualificationValidUntilUnixNanos,
  }
}

function sourceFreshness(value: unknown, field: string): SourceFreshness | null {
  if (value === "uninitialized") return value
  const row = record(value)
  if (!row || Object.keys(row).length !== 1) return null
  const variant = "fresh" in row ? "fresh" : "stale" in row ? "stale" : null
  if (!variant) return null
  const evidence = exactRecord(row[variant], [field])
  const at = integerText(evidence?.[field])
  return at === null
    ? null
    : variant === "fresh"
      ? { fresh: { [field]: at } }
      : { stale: { [field]: at } }
}

function sourceDataUseRights(value: unknown): SourceCoverageRow["rights"] | null {
  if (!Array.isArray(value) || value.length > 6) return null
  const rights: SourceCoverageRow["rights"] = []
  const operations = new Set<string>()
  for (const item of value) {
    const row = exactRecord(item, ["operation", "admission"])
    const operation = sourceDataUseOperation(row?.operation)
    const admission = sourceDataUseAdmission(row?.admission)
    if (!row || !operation || !admission || operations.has(operation)) return null
    operations.add(operation)
    rights.push({ operation, admission })
  }
  return rights
}

function sourceReleaseState(value: unknown): SourceCoverageRow["releaseState"] | null {
  return value === "available" || value === "rights_limited" ||
    value === "refresh_required" || value === "rights_blocked"
    ? value
    : null
}

function sourceOnboardingState(
  value: unknown,
): Exclude<SourceHealthRow["onboardingState"], null> | null {
  return value === "unavailable" || value === "anonymous_available" ||
    value === "user_action_required" || value === "credential_imported_unverified" ||
    value === "protocol_validated" || value === "stored_unverified" ||
    value === "secret_reconciliation_required" ||
    value === "verified_least_privilege" || value === "rights_admission_pending" ||
    value === "runtime_verification_pending" || value === "active_scoped" ||
    value === "renewal_required" || value === "refresh_required" ||
    value === "rotation_pending" || value === "revocation_unconfirmed" ||
    value === "indeterminate_remote_state" || value === "cleanup_required" ||
    value === "blocked"
    ? value
    : null
}

function sourceEventClass(
  value: unknown,
): Extract<SourceCoverageRuntime, { state: "established" }>["eventClass"] | null {
  return value === "trade" || value === "quote" || value === "book_snapshot" ||
    value === "book_delta" || value === "auction" || value === "trading_halt" ||
    value === "instrument_status" || value === "corporate_action"
    ? value
    : null
}

function sourceMarketDepth(
  value: unknown,
): Extract<SourceCoverageRuntime, { state: "established" }>["marketDepth"] {
  return value === "top_of_book" || value === "price_level" || value === "order_level"
    ? value
    : null
}

function sourceCoverageDelay(
  value: unknown,
): Extract<SourceCoverageRuntime, { state: "established" }>["delay"] | null {
  const realTime = exactRecord(value, ["kind"])
  if (realTime?.kind === "real_time") return { kind: "real_time" }
  const delayed = exactRecord(value, ["kind", "value"])
  const nanos = positiveIntegerText(delayed?.value)
  return delayed?.kind === "delayed" && nanos !== null
    ? { kind: "delayed", value: nanos }
    : null
}

function sourceConsolidation(
  value: unknown,
): Extract<SourceCoverageRuntime, { state: "established" }>["consolidation"] | null {
  return value === "single_venue" || value === "partial" || value === "consolidated"
    ? value
    : null
}

function sourceCoverageValue(
  value: unknown,
): "sufficient" | "insufficient" | "unknown" | null {
  return value === "sufficient" || value === "insufficient" || value === "unknown"
    ? value
    : null
}

function sourceCaptureIntegrity(
  value: unknown,
): SourceActiveHealth["captureIntegrity"] | null {
  return value === "disabled" || value === "healthy" || value === "incomplete"
    ? value
    : null
}

function sourceDataUseOperation(
  value: unknown,
): SourceCoverageRow["rights"][number]["operation"] | null {
  return value === "retrieve" || value === "display" || value === "persist" ||
    value === "model_training" || value === "export" || value === "redistribute"
    ? value
    : null
}

function sourceDataUseAdmission(
  value: unknown,
): SourceCoverageRow["rights"][number]["admission"] | null {
  return value === "admitted" || value === "pending" || value === "blocked"
    ? value
    : null
}

function validateLifecycleRequest(
  action: SourceLifecycleAction,
  request: SourceLifecycleRequest,
) {
  const row = record(request)
  const allowed = new Set([
    "provider", "expectedStateRevision", "expectedGeneration",
    "expectedRuntimeGenerationSha256", "onboardingSessionId",
    "publicConfigurationSha256", "reason",
  ])
  const provider = text(row?.provider)
  const revision = positiveIntegerText(row?.expectedStateRevision)
  const generation = row?.expectedGeneration === undefined
    ? undefined
    : positiveIntegerText(row.expectedGeneration)
  const runtime = row?.expectedRuntimeGenerationSha256 === undefined
    ? undefined
    : nonzeroSha256(row.expectedRuntimeGenerationSha256)
      ? row.expectedRuntimeGenerationSha256
      : null
  const session = row?.onboardingSessionId === undefined
    ? undefined
    : uuid(row.onboardingSessionId)
  const configuration = row?.publicConfigurationSha256 === undefined
    ? undefined
    : nonzeroSha256(row.publicConfigurationSha256)
      ? row.publicConfigurationSha256
      : null
  const reason = row?.reason === undefined ? undefined : text(row.reason)
  const pairPresent = session !== undefined && configuration !== undefined
  const pairAbsent = session === undefined && configuration === undefined
  const validShape = row && Object.keys(row).every((key) => allowed.has(key)) &&
    provider && revision !== null && generation !== null && runtime !== null &&
    session !== null && configuration !== null && reason !== null
  const validAction = action === "start" || action === "verify"
    ? generation === undefined && runtime === undefined && reason === undefined &&
      (pairPresent || pairAbsent)
    : action === "reconfigure"
      ? generation === undefined && runtime === undefined && pairPresent &&
        reason === undefined
      : action === "retry"
        ? generation === undefined && runtime === undefined && pairAbsent &&
          reason !== undefined
        : action === "resynchronize"
          ? (generation !== undefined) !== (runtime !== undefined) && pairAbsent &&
            reason !== undefined
          : !(generation !== undefined && runtime !== undefined) && pairAbsent &&
            reason !== undefined
  if (!validShape || !validAction) {
    invalidSourceResult("source lifecycle request")
  }
}

function lifecycleDisposition(value: unknown): SourceLifecycleDisposition | null {
  return value === "applied" || value === "replay" || value === "rejected" ||
    value === "reconciliation_required" ? value : null
}

function nullablePositiveIntegerText(value: unknown): string | null | undefined {
  return value === null ? null : positiveIntegerText(value) ?? undefined
}

function nullableSha256(value: unknown): string | null | undefined {
  return value === null ? null : sha256(value) ? value : undefined
}

function nullableUuid(value: unknown): string | null | undefined {
  return value === null ? null : uuid(value) ?? undefined
}

function sourceCoverageStatus(
  value: unknown,
): SourceLifecycleReceipt["coverage"] | undefined {
  return value === null || value === "sufficient" || value === "insufficient" ||
    value === "unknown" ? value : undefined
}

function sourceStreamIntegrity(
  value: unknown,
): SourceLifecycleReceipt["integrity"] | undefined {
  if (value === null) return null
  return value === "initializing" || value === "synchronizing" ||
    value === "validating" || value === "healthy" || value === "stale" ||
    value === "gap_detected" || value === "checksum_failed" ||
    value === "divergent" || value === "quarantined" ? value : undefined
}

function nullableSourceDataQuality(
  value: unknown,
): SourceLifecycleReceipt["quality"] | undefined {
  return value === null ? null : sourceDataQuality(value) ? value : undefined
}

function sourceDataQuality(
  value: unknown,
): value is NonNullable<SourceLifecycleReceipt["quality"]> {
  return value === "direct_verified" || value === "direct_unverified" ||
    value === "official_delayed" || value === "aggregated" ||
    value === "indicative" || value === "modeled" || value === "estimated" ||
    value === "stale" || value === "quarantined"
}

function sourceRateBudget(value: unknown): SourceLifecycleRateBudget | null {
  const simple = exactRecord(value, ["state"])
  if (
    simple &&
    (simple.state === "available" || simple.state === "unavailable" ||
      simple.state === "indeterminate")
  ) return { state: simple.state }
  const cooling = exactRecord(value, ["state", "until"])
  const until = timestamp(cooling?.until)
  return cooling?.state === "cooling_down" && until
    ? { state: "cooling_down", until }
    : null
}

function sourceAuthorization(
  value: unknown,
): SourceLifecycleReceipt["authorization"] | null {
  return value === "admitted" || value === "pending" || value === "blocked" ||
    value === "not_required" ? value : null
}

function sourceAvailability(
  value: unknown,
): SourceLifecycleReceipt["availability"] | null {
  return value === "available" || value === "temporarily_unavailable" ||
    value === "removed" || value === "indeterminate" ? value : null
}

function sourceRightsEvidence(
  value: unknown,
): SourceLifecycleRightsEvidence | null | undefined {
  if (value === null) return null
  const row = exactRecord(value, ["id", "sha256", "effectiveAt", "expiresAt"])
  const id = text(row?.id)
  const digest = text(row?.sha256)
  const effectiveAt = timestamp(row?.effectiveAt)
  const expiresAt = nullableTimestamp(row?.expiresAt)
  return row && id && digest && sha256(digest) && effectiveAt &&
    expiresAt !== undefined && (expiresAt === null || effectiveAt < expiresAt)
    ? { id, sha256: digest, effectiveAt, expiresAt }
    : undefined
}

function doctorReceiptBinding(
  provider: string,
  configurationSessionId: string | null,
  publicConfigurationSha256: string | null,
  doctor: SourceDoctorEvidence | null,
) {
  return doctor === null ||
    doctor.surfaceId === provider &&
      doctor.onboardingSessionId === configurationSessionId &&
      doctor.publicConfigurationSha256 === publicConfigurationSha256
}

function receiptStartEligibilityBinding(
  provider: string,
  state: SourceLifecycleState,
  eligibility: SourceStartEligibility,
  doctor: SourceDoctorEvidence | null,
  rights: SourceLifecycleRightsEvidence | null,
  authorization: SourceLifecycleReceipt["authorization"],
  availability: SourceLifecycleReceipt["availability"],
  observedAt: string,
) {
  const isAlpaca = provider === "alpaca.basic-market-data"
  const doctorAdmits = doctor !== null && doctor.current &&
    doctor.verifiedAt <= observedAt && observedAt < doctor.exclusiveExpiresAt &&
    doctorActivationReady(doctor)
  const eligibilityExact = eligibility === "eligible"
    ? isAlpaca && state === "stopped" && doctorAdmits
    : eligibility === "already_active"
      ? isAlpaca && state === "active" && doctorAdmits
      : eligibility === "not_applicable"
        ? !isAlpaca && doctor === null
        : isAlpaca
  const admittedEligibility = eligibility === "eligible" ||
    eligibility === "already_active"
  return eligibilityExact &&
    (!admittedEligibility ||
      authorization === "admitted" && rights !== null && doctor !== null &&
      rights.sha256 === doctor.rightsDecisionSha256) &&
    (eligibility !== "already_active" || availability === "available")
}

function doctorActivationReady(doctor: SourceDoctorEvidence) {
  return doctor.capabilities.iexLatestQuote.disposition === "available" &&
    doctor.capabilities.iexSnapshotBatch.disposition === "available" &&
    doctor.capabilities.iexWebSocket.disposition === "available" &&
    doctor.capabilities.iexHistoricalBars.disposition === "available" &&
    doctor.capabilities.iexUtcCalendar.disposition === "available"
}

function requestReceiptBinding(
  action: SourceLifecycleAction,
  request: SourceLifecycleRequest,
  disposition: SourceLifecycleDisposition,
  previousGeneration: string | null,
  currentGeneration: string | null,
  runtimeGenerationSha256: string | null,
  configurationSessionId: string | null,
  publicConfigurationSha256: string | null,
) {
  if (
    request.onboardingSessionId !== undefined &&
    (configurationSessionId !== request.onboardingSessionId ||
      publicConfigurationSha256 !== request.publicConfigurationSha256)
  ) return false
  if (
    request.expectedGeneration !== undefined && previousGeneration !== null &&
    previousGeneration !== request.expectedGeneration
  ) return false
  if (action === "resynchronize" && disposition === "applied") {
    if (request.expectedGeneration !== undefined) {
      return previousGeneration === request.expectedGeneration &&
        currentGeneration !== null &&
        compareUnsignedIntegerText(currentGeneration, request.expectedGeneration) > 0
    }
    return request.expectedRuntimeGenerationSha256 !== undefined &&
      previousGeneration === null && currentGeneration === null &&
      runtimeGenerationSha256 !== null &&
      runtimeGenerationSha256 !== request.expectedRuntimeGenerationSha256
  }
  return true
}

function notApplicableEvidence(value: unknown) {
  const row = exactRecord(value, ["status"])
  return row?.status === "not_applicable"
}

function textArray(value: unknown): string[] | null {
  if (!Array.isArray(value) || !value.every((item) => text(item) !== null)) return null
  return value as string[]
}

function sameOrderedStrings(left: readonly string[], right: readonly string[]) {
  return left.length === right.length && left.every((item, index) => item === right[index])
}

function isStrictlySorted(items: readonly string[]) {
  return items.every((item, index) => index === 0 || item > (items[index - 1] ?? ""))
}

function invalidSourceResult(message: string): never {
  throw new Error(`Invalid ${message}`)
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
    dataQuality: row.dataQuality,
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
  const bytes = boundedUnsignedIntegerText(
    row?.bytesObserved,
    String(26 * 16 * 1024 * 1024),
  )
  const authenticatedAt = timestamp(row?.authenticatedAt)
  const subscribedAt = timestamp(row?.subscribedAt)
  const completedAt = timestamp(row?.completedAt)
  if (!row || !digest(row.endpointContractSha256) || !digest(row.requestSha256) ||
    !digest(row.connectedFrameSha256) || !digest(row.authenticatedFrameSha256) ||
    !digest(row.subscriptionFrameSha256) || !digest(row.semanticResultSha256) ||
    boundedIntegerRange(row.handshakeStatus, 100, 599) === null || !handshakeRate ||
    subscribedTrades === null || subscribedQuotes === null || frames === null || frames < 3 ||
    bytes === null || bytes === "0" || !authenticatedAt || !subscribedAt || !completedAt ||
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
    boundedUnsignedIntegerText(row.bytes, String(8 * 1024 * 1024)) !== null &&
    timestamp(row.receivedAt) &&
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
  const parsed = integerText(observed?.value)
  return observed?.state === "observed" && parsed !== null
    ? { state: "observed", value: parsed }
    : null
}

function doctorObservedRetryAfter(value: unknown): DoctorObservedRetryAfter | null {
  const row = record(value)
  if (row?.state === "missing" && exactRecord(value, ["state"])) return { state: "missing" }
  const observed = exactRecord(value, ["state", "value"])
  const item = exactRecord(observed?.value, ["kind", "value"])
  if (observed?.state !== "observed" || !item) return null
  if (item.kind === "delay_seconds") {
    const parsed = unsignedIntegerText(item.value)
    return parsed === null
      ? null
      : { state: "observed", value: { kind: "delay_seconds", value: parsed } }
  }
  if (item.kind === "at_unix_seconds") {
    const parsed = integerText(item.value)
    return parsed === null
      ? null
      : { state: "observed", value: { kind: "at_unix_seconds", value: parsed } }
  }
  return null
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
  return typeof value === "string" && /^[1-9]\d*$/.test(value) &&
      compareUnsignedIntegerText(value, "18446744073709551615") <= 0
    ? value
    : null
}

function unsignedIntegerText(value: unknown): string | null {
  return typeof value === "string" && /^(?:0|[1-9]\d*)$/.test(value) &&
      compareUnsignedIntegerText(value, "18446744073709551615") <= 0
    ? value
    : null
}

function integerText(value: unknown): string | null {
  if (typeof value !== "string" || !/^(?:0|-?[1-9]\d*)$/.test(value)) return null
  const negative = value.startsWith("-")
  const magnitude = negative ? value.slice(1) : value
  const maximum = negative ? "9223372036854775808" : "9223372036854775807"
  return compareUnsignedIntegerText(magnitude, maximum) <= 0 ? value : null
}

function boundedUnsignedIntegerText(
  value: unknown,
  maximum: string,
): string | null {
  const parsed = unsignedIntegerText(value)
  return parsed !== null && compareUnsignedIntegerText(parsed, maximum) <= 0
    ? parsed
    : null
}

function compareUnsignedIntegerText(left: string, right: string): number {
  return left.length === right.length
    ? left === right
      ? 0
      : left < right
        ? -1
        : 1
    : left.length < right.length
      ? -1
      : 1
}

function compareIntegerText(left: string, right: string): number {
  const leftNegative = left.startsWith("-")
  const rightNegative = right.startsWith("-")
  if (leftNegative !== rightNegative) return leftNegative ? -1 : 1
  const leftMagnitude = leftNegative ? left.slice(1) : left
  const rightMagnitude = rightNegative ? right.slice(1) : right
  const comparison = compareUnsignedIntegerText(leftMagnitude, rightMagnitude)
  return leftNegative ? -comparison : comparison
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

function groupRows<T>(
  rows: readonly T[],
  identity: (row: T) => string,
) {
  const result = new Map<string, T[]>()
  for (const row of rows) {
    const id = identity(row)
    const existing = result.get(id) ?? []
    existing.push(row)
    result.set(id, existing)
  }
  return result
}

function commonValue<T>(values: readonly T[]): T | null {
  const first = values[0]
  return first === undefined || values.some((value) => value !== first)
    ? null
    : first
}

function statusProfileBinding(
  profile: ProviderProfile,
  statusProfile: RecordValue,
) {
  if (
    statusProfile.id !== profile.id ||
    statusProfile.display_name !== profile.display_name
  ) return false
  for (const key of [
    "release_state",
    "coverage",
    "quality_ceiling",
    "zero_fee",
    "account_requirement",
    "credential_requirement",
  ] as const) {
    if (statusProfile[key] !== undefined && statusProfile[key] !== profile[key]) return false
  }
  return true
}

function sourceRuntimeIdentity(runtime: SourceActiveRuntime) {
  return [
    runtime.sourceId,
    runtime.venueId,
    runtime.instrumentId,
    runtime.providerProduct,
    runtime.providerChannel,
    runtime.connectionGeneration,
    runtime.sessionId,
    runtime.healthEpoch,
    runtime.stateRevision,
  ].join("\0")
}

function sameDataUseRights(
  left: SourceCoverageRow["rights"],
  right: SourceCoverageRow["rights"],
) {
  return left.length === right.length && left.every((item, index) =>
    item.operation === right[index]?.operation &&
    item.admission === right[index]?.admission
  )
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
  const raw = integerText(value)
  if (raw === null) return null
  try {
    const milliseconds = Number(BigInt(raw) / 1_000_000n)
    const date = new Date(milliseconds)
    return Number.isNaN(date.getTime()) ? null : date.toISOString()
  } catch {
    return null
  }
}

function boundedText(value: unknown, maximum: number): string | null {
  const parsed = text(value)
  return parsed !== null && parsed.length <= maximum ? parsed : null
}

function sha256(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value)
}

function nonzeroSha256(value: unknown): value is string {
  return sha256(value) && value !== "0".repeat(64)
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
