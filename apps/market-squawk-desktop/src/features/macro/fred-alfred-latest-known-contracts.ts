import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import type { FredAlfredImmutableGeneration } from "@/lib/transport"

export const FRED_ALFRED_OPERATION =
  "Macro.GetFredAlfredLatestKnown" as const
export const FRED_ALFRED_OPERATION_SCHEMA =
  "market-squawk-fred-alfred-operation/v1" as const
export const FRED_ALFRED_READ_SCHEMA =
  "market-squawk-fred-alfred-point-in-time/v1" as const
export const FRED_ALFRED_SURFACE_ID = "fred-alfred.api-v1-v2" as const
export const FRED_ALFRED_SOURCE_ID =
  "fred-fred-alfred.api-v1-v2" as const

const ZERO_SHA256 = "0".repeat(64)
const sha256Schema = z
  .string()
  .regex(/^[0-9a-f]{64}$/)
  .refine((value) => value !== ZERO_SHA256, "Reserved SHA-256 identity.")
const boundedText = (maximum: number) => z.string().min(1).max(maximum)
const analyticalDatasetIdSchema = boundedText(256).regex(
  /^[A-Za-z0-9][A-Za-z0-9._-]*$/,
)
const positiveIntegerTextSchema = z
  .string()
  .regex(/^[1-9]\d{0,19}$/)
  .refine(
    (value) => BigInt(value) <= 18_446_744_073_709_551_615n,
    "Manifest version exceeds an unsigned 64-bit integer.",
  )
const calendarDateSchema = z
  .string()
  .regex(/^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$/)
  .refine(isCalendarDate, "Expected a valid calendar date.")
const timestampSchema = z.string().datetime({ offset: true })
const exactDecimalSchema = z
  .string()
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/)

export const fredAlfredGenerationSchema = z
  .object({
    manifestVersion: positiveIntegerTextSchema,
    schema: z
      .object({
        name: boundedText(256),
        version: z.number().int().min(1).max(65_535),
        fingerprint: sha256Schema,
      })
      .strict(),
    contentHash: sha256Schema,
  })
  .strict()

const providerBindingSchema = z
  .object({
    surfaceId: z.literal(FRED_ALFRED_SURFACE_ID),
    providerDatasetId: boundedText(512),
    analyticalDatasetId: analyticalDatasetIdSchema,
  })
  .strict()

const readProviderBindingSchema = providerBindingSchema.extend({
  sourceId: z.literal(FRED_ALFRED_SOURCE_ID),
  seriesId: boundedText(512),
}).strict()

const manifestSchema = fredAlfredGenerationSchema.extend({
  datasetId: analyticalDatasetIdSchema,
}).strict()

const readBindingSchema = z
  .object({
    provider: readProviderBindingSchema,
    manifest: manifestSchema,
    objectGraphDigest: sha256Schema,
    queryIdentity: sha256Schema,
    resultDigest: sha256Schema,
  })
  .strict()

const selectionSchema = z
  .object({
    policy: z.literal("latest_known_by_series_as_of_cutoff_v1"),
    knowledgeCutoff: timestampSchema,
    effectiveDateCutoff: calendarDateSchema,
    evaluatedAt: timestampSchema,
    selectionDigest: sha256Schema,
    complete: z.literal(true),
  })
  .strict()

const observedValueSchema = z
  .object({
    state: z.literal("observed"),
    decimal: exactDecimalSchema,
  })
  .strict()

const missingValueSchema = z
  .object({
    state: z.literal("missing"),
    marker: boundedText(512),
    reason: boundedText(512).nullable(),
  })
  .strict()

const observationSchema = z
  .object({
    seriesId: boundedText(512),
    unitId: boundedText(512),
    effectiveDate: calendarDateSchema,
    publishedVintage: calendarDateSchema,
    supersededAfter: calendarDateSchema.nullable(),
    availableAt: timestampSchema,
    receivedAt: timestampSchema,
    ingestedAt: timestampSchema,
    revision: z.number().int().min(1).max(4_294_967_295),
    value: z.discriminatedUnion("state", [
      observedValueSchema,
      missingValueSchema,
    ]),
    sourceIdentifier: boundedText(512),
    rawPageDigest: sha256Schema,
    quality: z.literal("official_delayed"),
  })
  .strict()

const pointInTimeReadSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_READ_SCHEMA),
    binding: readBindingSchema,
    selection: selectionSchema,
    observation: observationSchema,
  })
  .strict()

const setupRequiredDataSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_OPERATION_SCHEMA),
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("setup_required"),
    reason: z.literal("desired_activation_absent"),
  })
  .strict()

const providerDatasetUnavailableDataSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_OPERATION_SCHEMA),
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("unavailable"),
    reason: z.literal("exact_provider_dataset_absent"),
  })
  .strict()

const manifestUnavailableDataSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_OPERATION_SCHEMA),
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("unavailable"),
    reason: z.literal("exact_manifest_absent"),
    binding: providerBindingSchema,
  })
  .strict()

const readyStatusDataSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_OPERATION_SCHEMA),
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("ready"),
    binding: providerBindingSchema,
    generation: fredAlfredGenerationSchema,
  })
  .strict()

const readyReadDataSchema = z
  .object({
    schemaIdentity: z.literal(FRED_ALFRED_OPERATION_SCHEMA),
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("ready"),
    generation: fredAlfredGenerationSchema,
    result: pointInTimeReadSchema,
  })
  .strict()

const unavailableQuality = (reason: z.ZodLiteral<string>) =>
  z
    .object({
      classification: z.literal("unavailable"),
      reason,
      manifestPinned: z.literal(false),
      executionEligible: z.literal(false),
    })
    .strict()

const statusQualitySchema = z
  .object({
    classification: z.literal("manifest_bound_not_read"),
    manifestPinned: z.literal(true),
    executionEligible: z.literal(false),
    executionEligibility: z.literal("research_only_execution_ineligible"),
  })
  .strict()

const readQualitySchema = z
  .object({
    classification: z.literal("official_delayed_point_in_time"),
    recordLevelProvenance: z.literal(true),
    manifestPinned: z.literal(true),
    selectionComplete: z.literal(true),
    executionEligible: z.literal(false),
    executionEligibility: z.literal("research_only_execution_ineligible"),
  })
  .strict()

const setupCoverageSchema = z
  .object({
    operation: z.literal(FRED_ALFRED_OPERATION),
    surfaceId: z.literal(FRED_ALFRED_SURFACE_ID),
    state: z.literal("setup_required"),
    configured: z.literal(false),
  })
  .strict()

const providerDatasetUnavailableCoverageSchema = z
  .object({
    operation: z.literal(FRED_ALFRED_OPERATION),
    surfaceId: z.literal(FRED_ALFRED_SURFACE_ID),
    state: z.literal("unavailable"),
    configured: z.literal(true),
    datasetState: z.literal("unbound"),
  })
  .strict()

const manifestUnavailableCoverageSchema = z
  .object({
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("unavailable"),
    binding: providerBindingSchema,
    manifestState: z.literal("absent"),
  })
  .strict()

const readyStatusCoverageSchema = z
  .object({
    operation: z.literal(FRED_ALFRED_OPERATION),
    state: z.literal("ready"),
    binding: providerBindingSchema,
    generation: fredAlfredGenerationSchema,
  })
  .strict()

const readyReadCoverageSchema = z
  .object({
    operation: z.literal(FRED_ALFRED_OPERATION),
    binding: readBindingSchema,
    selection: selectionSchema,
  })
  .strict()

function resultSchema<Data extends z.ZodType, Coverage extends z.ZodType, Quality extends z.ZodType>(
  data: Data,
  returnedItems: 0 | 1,
  coverage: Coverage,
  quality: Quality,
) {
  return z
    .object({
      data,
      metadata: z
        .object({
          completeness: z.literal("complete"),
          returnedItems: z.literal(returnedItems),
          availableItems: z.literal(returnedItems),
          sourceCoverage: coverage,
          dataQuality: quality,
        })
        .strict(),
    })
    .strict()
}

const setupRequiredResultSchema = resultSchema(
  setupRequiredDataSchema,
  0,
  setupCoverageSchema,
  unavailableQuality(z.literal("desired_activation_absent")),
)
const providerDatasetUnavailableResultSchema = resultSchema(
  providerDatasetUnavailableDataSchema,
  0,
  providerDatasetUnavailableCoverageSchema,
  unavailableQuality(z.literal("exact_provider_dataset_absent")),
)
const manifestUnavailableResultSchema = resultSchema(
  manifestUnavailableDataSchema,
  0,
  manifestUnavailableCoverageSchema,
  unavailableQuality(z.literal("exact_manifest_absent")),
)
const readyStatusResultSchema = resultSchema(
  readyStatusDataSchema,
  0,
  readyStatusCoverageSchema,
  statusQualitySchema,
)

const fredAlfredStatusResultSchema = z.union([
  setupRequiredResultSchema,
  providerDatasetUnavailableResultSchema,
  manifestUnavailableResultSchema,
  readyStatusResultSchema,
])

const fredAlfredReadResultSchema = resultSchema(
  readyReadDataSchema,
  1,
  readyReadCoverageSchema,
  readQualitySchema,
)

export const fredAlfredCutoffsSchema = z
  .object({
    knowledgeCutoff: timestampSchema,
    effectiveDateCutoff: calendarDateSchema,
  })
  .strict()
  .superRefine((cutoffs, context) => {
    const knowledgeDate = timestampUtcDate(cutoffs.knowledgeCutoff)
    if (
      knowledgeDate === null ||
      cutoffs.effectiveDateCutoff > knowledgeDate
    ) {
      context.addIssue({
        code: "custom",
        path: ["effectiveDateCutoff"],
        message: "Effective-date cutoff cannot follow the knowledge cutoff date.",
      })
    }
  })

export type FredAlfredAvailability = z.infer<
  typeof fredAlfredStatusResultSchema
>
export type FredAlfredReadyAvailability = z.infer<
  typeof readyStatusResultSchema
>
export type FredAlfredLatestKnownRead = z.infer<
  typeof fredAlfredReadResultSchema
>
export type FredAlfredCutoffs = z.infer<typeof fredAlfredCutoffsSchema>

export function parseFredAlfredAvailability(
  result: ApplicationResult,
): FredAlfredAvailability {
  const parsed = fredAlfredStatusResultSchema.safeParse(result)
  if (!parsed.success) throw unsupportedResponse("availability")

  const value = parsed.data
  const ready = readyStatusResultSchema.safeParse(value)
  if (ready.success) {
    if (
      !sameProviderBinding(
        ready.data.data.binding,
        ready.data.metadata.sourceCoverage.binding,
      ) ||
      !sameGeneration(
        ready.data.data.generation,
        ready.data.metadata.sourceCoverage.generation,
      )
    ) {
      throw unsupportedResponse("availability binding")
    }
  } else {
    const manifestUnavailable = manifestUnavailableResultSchema.safeParse(value)
    if (
      manifestUnavailable.success &&
      !sameProviderBinding(
        manifestUnavailable.data.data.binding,
        manifestUnavailable.data.metadata.sourceCoverage.binding,
      )
    ) {
      throw unsupportedResponse("unavailable binding")
    }
  }

  return value
}

export function parseFredAlfredLatestKnownRead(
  result: ApplicationResult,
  availability: FredAlfredReadyAvailability,
  cutoffs: FredAlfredCutoffs,
): FredAlfredLatestKnownRead {
  const parsed = fredAlfredReadResultSchema.safeParse(result)
  if (!parsed.success) throw unsupportedResponse("point-in-time read")

  const value = parsed.data
  const inner = value.data.result
  const observation = inner.observation
  const manifestGeneration = generationFromManifest(inner.binding.manifest)
  const knowledgeDate = timestampUtcDate(inner.selection.knowledgeCutoff)
  const providerNamespace = inner.binding.provider.providerDatasetId.split(":", 1)[0]
  if (
    !sameGeneration(availability.data.generation, value.data.generation) ||
    !sameGeneration(value.data.generation, manifestGeneration) ||
    !sameReadBinding(inner.binding, value.metadata.sourceCoverage.binding) ||
    !sameSelection(inner.selection, value.metadata.sourceCoverage.selection) ||
    !sameProviderBinding(
      availability.data.binding,
      inner.binding.provider,
    ) ||
    inner.binding.provider.analyticalDatasetId !== inner.binding.manifest.datasetId ||
    inner.binding.provider.seriesId !== observation.seriesId ||
    !sameTimestamp(inner.selection.knowledgeCutoff, cutoffs.knowledgeCutoff) ||
    inner.selection.effectiveDateCutoff !== cutoffs.effectiveDateCutoff ||
    !timestampAtOrBefore(inner.selection.knowledgeCutoff, inner.selection.evaluatedAt) ||
    knowledgeDate === null ||
    observation.effectiveDate > inner.selection.effectiveDateCutoff ||
    observation.publishedVintage > knowledgeDate ||
    (observation.supersededAfter !== null &&
      observation.supersededAfter <= observation.publishedVintage) ||
    !sameTimestamp(observation.availableAt, observation.receivedAt) ||
    !timestampAtOrBefore(observation.receivedAt, observation.ingestedAt) ||
    !timestampAtOrBefore(observation.ingestedAt, inner.selection.knowledgeCutoff) ||
    !observation.unitId.startsWith("fred-unit:v1:") ||
    (providerNamespace !== "fred" && providerNamespace !== "alfred") ||
    observation.sourceIdentifier !==
      `${providerNamespace}:${observation.seriesId}:${observation.effectiveDate}:${observation.publishedVintage}`
  ) {
    throw unsupportedResponse("immutable generation or point-in-time evidence")
  }

  return value
}

export function sameFredAlfredGeneration(
  left: FredAlfredImmutableGeneration,
  right: FredAlfredImmutableGeneration,
): boolean {
  return sameGeneration(left, right)
}

export function sameFredAlfredReadyAvailability(
  left: FredAlfredReadyAvailability,
  right: FredAlfredReadyAvailability,
): boolean {
  return (
    sameProviderBinding(left.data.binding, right.data.binding) &&
    sameGeneration(left.data.generation, right.data.generation)
  )
}

export function isFredAlfredReadyAvailability(
  availability: FredAlfredAvailability | undefined,
): availability is FredAlfredReadyAvailability {
  return availability?.data.state === "ready"
}

export function fredAlfredGenerationKey(
  generation: FredAlfredImmutableGeneration,
): string {
  return [
    generation.manifestVersion,
    generation.schema.name,
    generation.schema.version,
    generation.schema.fingerprint,
    generation.contentHash,
  ].join(":")
}

function generationFromManifest(
  manifest: z.infer<typeof manifestSchema>,
): FredAlfredImmutableGeneration {
  return {
    manifestVersion: manifest.manifestVersion,
    schema: manifest.schema,
    contentHash: manifest.contentHash,
  }
}

function sameGeneration(
  left: FredAlfredImmutableGeneration,
  right: FredAlfredImmutableGeneration,
): boolean {
  return (
    left.manifestVersion === right.manifestVersion &&
    left.schema.name === right.schema.name &&
    left.schema.version === right.schema.version &&
    left.schema.fingerprint === right.schema.fingerprint &&
    left.contentHash === right.contentHash
  )
}

function sameProviderBinding(
  left: z.infer<typeof providerBindingSchema>,
  right: z.infer<typeof providerBindingSchema>,
): boolean {
  return (
    left.surfaceId === right.surfaceId &&
    left.providerDatasetId === right.providerDatasetId &&
    left.analyticalDatasetId === right.analyticalDatasetId
  )
}

function sameReadBinding(
  left: z.infer<typeof readBindingSchema>,
  right: z.infer<typeof readBindingSchema>,
): boolean {
  return (
    sameProviderBinding(left.provider, right.provider) &&
    left.provider.sourceId === right.provider.sourceId &&
    left.provider.seriesId === right.provider.seriesId &&
    left.manifest.datasetId === right.manifest.datasetId &&
    sameGeneration(
      generationFromManifest(left.manifest),
      generationFromManifest(right.manifest),
    ) &&
    left.objectGraphDigest === right.objectGraphDigest &&
    left.queryIdentity === right.queryIdentity &&
    left.resultDigest === right.resultDigest
  )
}

function sameSelection(
  left: z.infer<typeof selectionSchema>,
  right: z.infer<typeof selectionSchema>,
): boolean {
  return (
    left.policy === right.policy &&
    left.knowledgeCutoff === right.knowledgeCutoff &&
    left.effectiveDateCutoff === right.effectiveDateCutoff &&
    left.evaluatedAt === right.evaluatedAt &&
    left.selectionDigest === right.selectionDigest &&
    left.complete === right.complete
  )
}

function isCalendarDate(value: string): boolean {
  const parts = value.split("-").map(Number)
  if (parts.length !== 3) return false
  const year = parts[0]
  const month = parts[1]
  const day = parts[2]
  if (year === undefined || month === undefined || day === undefined) return false
  const date = new Date(Date.UTC(year, month - 1, day))
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  )
}

function sameTimestamp(left: string, right: string): boolean {
  const leftNanos = timestampNanoseconds(left)
  const rightNanos = timestampNanoseconds(right)
  return leftNanos !== null && leftNanos === rightNanos
}

function timestampAtOrBefore(left: string, right: string): boolean {
  const leftNanos = timestampNanoseconds(left)
  const rightNanos = timestampNanoseconds(right)
  return leftNanos !== null && rightNanos !== null && leftNanos <= rightNanos
}

function timestampUtcDate(value: string): string | null {
  const milliseconds = Date.parse(value)
  return Number.isFinite(milliseconds)
    ? new Date(milliseconds).toISOString().slice(0, 10)
    : null
}

function timestampNanoseconds(value: string): bigint | null {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/i.exec(
    value,
  )
  if (!match) return null
  const fraction = match[2] ?? ""
  const milliseconds = Date.parse(value)
  if (!Number.isFinite(milliseconds)) return null
  const fractionalMilliseconds = Number(fraction.padEnd(3, "0").slice(0, 3))
  const wholeSecondMilliseconds = milliseconds - fractionalMilliseconds
  if (!Number.isSafeInteger(wholeSecondMilliseconds / 1_000)) return null
  return (
    BigInt(wholeSecondMilliseconds / 1_000) * 1_000_000_000n +
    BigInt(fraction.padEnd(9, "0") || "0")
  )
}

function unsupportedResponse(part: string): Error {
  return new Error(
    `The installed service returned an unsupported FRED/ALFRED ${part} response.`,
  )
}
