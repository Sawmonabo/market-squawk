import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

const manifestSchema = z.object({
  dataset: z.string().min(1),
  manifestVersion: z.number().int().nonnegative(),
  schema: z.object({
    name: z.string().min(1),
    version: z.number().int().positive(),
    fingerprint: digestSchema,
  }),
  contentHash: digestSchema,
})

export const modelBundleSchema = z.object({
  modelId: z.string().uuid(),
  bundleId: z.string().min(1),
  bundleVersion: z.number().int().positive(),
  metadataHash: digestSchema,
  artifactHash: digestSchema,
  format: z.enum(["native_linear", "native_logistic", "onnx"]),
  formatVersion: z.number().int().positive(),
  trainingDataset: manifestSchema,
  fallbackBehavior: z.object({
    decision: z.literal("no_action"),
    reason: z.string().min(1),
  }),
})

const featureSchema = z.object({
  name: z.string().min(1),
  version: z.number().int().positive(),
  inputSchemaDigest: digestSchema,
  semanticDigest: digestSchema,
  normalizer: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("identity") }),
    z.object({
      kind: z.literal("standard"),
      mean: z.number().finite(),
      scale: z.number().finite(),
    }),
  ]),
})

export const modelMetadataSchema = modelBundleSchema.extend({
  trainingRunHash: digestSchema,
  features: z.array(featureSchema).min(1).max(4_096),
  trainingDataset: z.object({
    manifest: manifestSchema,
    buildSpecDigest: digestSchema,
    universeDigest: digestSchema,
    policyDigest: digestSchema,
    catalogIdentity: digestSchema,
    exportDigest: digestSchema,
    selectionDigest: digestSchema,
    selectionAsOfUnixNanos: losslessIntegerSchema,
    selectedComponentRows: z.number().int().positive(),
  }),
  universeId: z.string().min(1),
  trainingPeriod: z.object({
    startUnixNanos: losslessIntegerSchema,
    endUnixNanos: losslessIntegerSchema,
  }),
  label: z.object({
    name: z.string().min(1),
    version: z.number().int().positive(),
    kind: z.string().min(1),
  }),
  trainingCodeRevision: z.string().min(1),
  trainingEnvironmentHash: digestSchema,
  validationMetrics: z
    .array(
      z.object({
        name: z.enum([
          "mean_squared_error",
          "accuracy",
          "log_loss",
          "area_under_roc",
        ]),
        value: z.number().finite(),
      }),
    )
    .max(256),
  decisionThresholds: z.object({
    negativeMaximum: z.number().finite(),
    positiveMinimum: z.number().finite(),
    minimumConfidence: z.number().finite(),
  }),
  intendedUse: z.string().min(1),
  limitations: z.array(z.string().min(1)).max(256),
})

const modelBundlePageSchema = z.object({
  bundles: z.array(modelBundleSchema).max(4_096),
})

const controlledArtifactSchema = z.object({
  artifactId: z.string().min(1),
  sha256: digestSchema,
  byteCount: z.number().int().positive(),
  mediaType: z.literal("application/json"),
})

export const forecastSummarySchema = z.object({
  vintageId: digestSchema,
  requestHash: digestSchema,
  instrumentId: z.string().uuid(),
  modelId: z.string().uuid(),
  bundleId: z.string().min(1),
  bundleVersion: z.number().int().positive(),
  observedThroughUnixNanos: losslessIntegerSchema,
  createdAtUnixNanos: losslessIntegerSchema,
  expiresAtUnixNanos: losslessIntegerSchema,
  horizonPoints: z.number().int().positive().max(512),
  horizonStepNanos: losslessIntegerSchema,
  hasCalibratedIntervals: z.boolean(),
  quality: z.literal("modeled"),
  unavailableReason: z.string().min(1),
  controlledArtifact: controlledArtifactSchema,
})

const forecastPageSchema = z.object({
  forecasts: z.array(forecastSummarySchema).max(4_096),
  available: z.number().int().nonnegative(),
  truncated: z.boolean(),
})

const forecastIntervalsSchema = z.object({
  interval50: z.tuple([z.string().regex(/^-?\d+$/), z.string().regex(/^-?\d+$/)]),
  interval80: z.tuple([z.string().regex(/^-?\d+$/), z.string().regex(/^-?\d+$/)]),
  interval95: z.tuple([z.string().regex(/^-?\d+$/), z.string().regex(/^-?\d+$/)]),
})

const forecastPointSchema = z.object({
  targetAtUnixNanos: losslessIntegerSchema,
  centralMantissa: z.string().regex(/^-?\d+$/),
  decimalScale: z.number().int().min(0).max(18),
  intervals: forecastIntervalsSchema.nullable(),
})

const calibrationSchema = z.object({
  method: z.enum(["mapie_enbpi", "mapie_aci", "residual_quantile"]),
  windowStartUnixNanos: losslessIntegerSchema,
  windowEndUnixNanos: losslessIntegerSchema,
  observations: z.number().int().positive(),
  policyHash: digestSchema,
  residualsHash: digestSchema,
  targetCoverageBasisPoints: z.tuple([
    z.number().int().min(0).max(10_000),
    z.number().int().min(0).max(10_000),
    z.number().int().min(0).max(10_000),
  ]),
  lowerOffsets: z.tuple([z.number().finite(), z.number().finite(), z.number().finite()]),
  upperOffsets: z.tuple([z.number().finite(), z.number().finite(), z.number().finite()]),
  realizedCovered: z.tuple([
    z.number().int().nonnegative(),
    z.number().int().nonnegative(),
    z.number().int().nonnegative(),
  ]),
  realizedTotal: z.tuple([
    z.number().int().positive(),
    z.number().int().positive(),
    z.number().int().positive(),
  ]),
  coverageInterpretation: z.string().min(1),
  dependenceAssumptions: z.string().min(1),
})

export const forecastVintageSchema = z.object({
  vintageId: digestSchema,
  requestHash: digestSchema,
  controlledArtifact: controlledArtifactSchema,
  instrumentId: z.string().uuid(),
  modelId: z.string().uuid(),
  bundleId: z.string().min(1),
  bundleVersion: z.number().int().positive(),
  metadataHash: digestSchema,
  artifactHash: digestSchema,
  trainingRunHash: digestSchema,
  datasetExportHash: digestSchema,
  datasetSelectionHash: digestSchema,
  universeId: z.string().min(1),
  trainingStartUnixNanos: losslessIntegerSchema,
  trainingEndUnixNanos: losslessIntegerSchema,
  featureSemanticHashes: z.array(digestSchema).min(1).max(4_096),
  observedThroughUnixNanos: losslessIntegerSchema,
  availableAtUnixNanos: losslessIntegerSchema,
  createdAtUnixNanos: losslessIntegerSchema,
  expiresAtUnixNanos: losslessIntegerSchema,
  modelAgeNanosAtPublication: losslessIntegerSchema,
  dataAgeNanosAtPublication: losslessIntegerSchema,
  horizonPoints: z.number().int().positive().max(512),
  horizonStepNanos: losslessIntegerSchema,
  quality: z.literal("modeled"),
  points: z.array(forecastPointSchema).min(1).max(512),
  calibration: calibrationSchema.nullable(),
  limitations: z.array(z.string().min(1)).max(256),
  unavailableReason: z.string().min(1),
})

const forecastOutcomeSchema = z.object({
  outcomeId: digestSchema,
  vintageId: digestSchema,
  targetAtUnixNanos: losslessIntegerSchema,
  observedAtUnixNanos: losslessIntegerSchema,
  availableAtUnixNanos: losslessIntegerSchema,
  actualMantissa: z.string().regex(/^-?\d+$/),
  decimalScale: z.number().int().min(0).max(18),
  signedErrorMantissa: z.string().regex(/^-?\d+$/),
  absoluteErrorMantissa: z.string().regex(/^\d+$/),
  sourcePitHash: digestSchema,
  quality: z.enum([
    "direct_verified",
    "direct_unverified",
    "official_delayed",
    "aggregated",
    "indicative",
    "estimated",
    "stale",
    "quarantined",
  ]),
})

const forecastOutcomesSchema = z.object({
  vintageId: digestSchema,
  outcomes: z.array(forecastOutcomeSchema).max(4_096),
  available: z.number().int().nonnegative(),
  truncated: z.boolean(),
})

const jobSchema = z.object({
  jobId: z.string().uuid(),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  kind: z.string().min(1),
  state: z.enum([
    "queued",
    "preparing",
    "running",
    "awaiting_confirmation",
    "cancelling",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "recovering",
  ]),
  phase: z.string().min(1).nullable(),
  completedUnits: z.number().int().nonnegative().nullable(),
  totalUnits: z.number().int().nonnegative().nullable(),
  cancellationRequested: z.boolean(),
  updatedAt: losslessIntegerSchema,
  recovery: z.string().min(1).nullable(),
  failure: z
    .object({
      class: z.string().min(1),
      diagnostic: z.string().min(1),
      retryable: z.boolean(),
    })
    .nullable(),
})

const jobPageSchema = z.object({
  jobs: z.array(jobSchema).max(1_024),
  next: z.unknown().nullable(),
})

const evaluationResultSchema = z.object({
  modelId: z.string().uuid(),
  bundleId: z.string().min(1),
  bundleVersion: z.number().int().positive(),
  trainingDataset: manifestSchema,
  featureSemanticDigests: z.array(digestSchema).min(1).max(4_096),
  score: z.number().finite(),
  confidence: z.number().finite(),
  decision: z.enum(["negative", "no_action", "positive"]),
  executionAuthority: z.literal("none"),
  inferenceFailureBehavior: z.literal("no_action"),
  evaluationEvidence: z.object({
    sequence: z.number().int().nonnegative(),
    digest: digestSchema,
    retention: z.literal("bounded_process_local"),
  }),
  validationMetrics: z.array(z.object({ name: z.string().min(1), value: z.number().finite() })).max(256),
})

const jobReceiptSchema = z.object({
  jobId: z.string().uuid(),
  generation: z.number().int().positive(),
  sequence: z.number().int().nonnegative(),
  state: z.literal("queued"),
})

export type ModelBundle = z.infer<typeof modelBundleSchema>
export type ModelMetadata = z.infer<typeof modelMetadataSchema>
export type ForecastSummary = z.infer<typeof forecastSummarySchema>
export type ForecastVintage = z.infer<typeof forecastVintageSchema>
export type ForecastOutcome = z.infer<typeof forecastOutcomeSchema>
export type ModelJob = z.infer<typeof jobSchema>
export type EvaluationResult = z.infer<typeof evaluationResultSchema>
export type ModelJobReceipt = z.infer<typeof jobReceiptSchema>

export interface ModelBundlePage {
  bundles: ModelBundle[]
  completeness: string
  available: number
}

export interface ForecastPage {
  forecasts: ForecastSummary[]
  completeness: string
  available: number
  truncated: boolean
}

export function parseModelBundles(result: ApplicationResult): ModelBundlePage {
  const parsed = modelBundlePageSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported model-bundle response.",
    )
  }
  return {
    bundles: parsed.data.bundles,
    completeness: result.metadata.completeness,
    available: result.metadata.availableItems,
  }
}

export function parseForecasts(result: ApplicationResult): ForecastPage {
  const parsed = forecastPageSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported forecast-vintage response.",
    )
  }
  return { ...parsed.data, completeness: result.metadata.completeness }
}

export function parseModelMetadata(result: ApplicationResult): ModelMetadata {
  const parsed = modelMetadataSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned unsupported admitted-model metadata.",
    )
  }
  return parsed.data
}

export function parseForecastVintage(result: ApplicationResult): ForecastVintage {
  const parsed = forecastVintageSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported forecast vintage.",
    )
  }
  return parsed.data
}

export interface ForecastOutcomes {
  vintageId: string
  outcomes: ForecastOutcome[]
  available: number
  truncated: boolean
  completeness: string
}

export function parseForecastOutcomes(result: ApplicationResult): ForecastOutcomes {
  const parsed = forecastOutcomesSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned unsupported forecast outcomes.",
    )
  }
  return { ...parsed.data, completeness: result.metadata.completeness }
}

export function parseModelJobs(result: ApplicationResult): ModelJob[] {
  const parsed = jobPageSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported model-job response.",
    )
  }
  return parsed.data.jobs.filter(
    (job) => job.kind.startsWith("model.") || job.kind.startsWith("training."),
  )
}

export function parseEvaluationResult(result: ApplicationResult): EvaluationResult {
  const parsed = evaluationResultSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported model-evaluation result.",
    )
  }
  return parsed.data
}

export function parseModelJobReceipt(result: ApplicationResult): ModelJobReceipt {
  const parsed = jobReceiptSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported model-job receipt.",
    )
  }
  return parsed.data
}

export function isActiveModelJob(job: ModelJob): boolean {
  return !["completed", "failed", "cancelled", "interrupted"].includes(
    job.state,
  )
}
