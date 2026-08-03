import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

const manifestSchema = z
  .object({
    dataset: z.string().min(1).max(256),
    manifestVersion: z.number().int().nonnegative(),
    schema: z
      .object({
        name: z.string().min(1).max(256),
        version: z.number().int().positive(),
        fingerprint: digestSchema,
      })
      .strict(),
    contentHash: digestSchema,
  })
  .strict()

const instrumentSchema = z
  .object({
    instrumentId: z.string().uuid(),
    label: z.string().min(1).max(240),
    observedFromUnixNanos: losslessIntegerSchema,
    observedThroughUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    observedPoints: z.number().int().positive().max(4_096),
    decimalScale: z.number().int().min(0).max(12),
  })
  .strict()

const policySchema = z
  .object({
    maximumHorizonPoints: z.number().int().positive().max(512),
    horizonStepNanos: losslessIntegerSchema.refine(
      (value) => BigInt(value) > 0n,
      "Expected a positive forecast step.",
    ),
    maximumValidityNanos: losslessIntegerSchema.refine(
      (value) => BigInt(value) > 0n,
      "Expected a positive validity period.",
    ),
    minimumObservedPoints: z.number().int().positive().max(4_096),
  })
  .strict()

const datasetOptionSchema = z
  .object({
    manifest: manifestSchema,
    label: z.string().min(1).max(240),
    instruments: z.array(instrumentSchema).min(1).max(4_096),
    policies: z.array(policySchema).min(1).max(64),
  })
  .strict()

const modelOptionSchema = z
  .object({
    modelId: z.string().uuid(),
    bundleId: z.string().min(1).max(256),
    bundleVersion: z.number().int().positive(),
    metadataSha256: digestSchema,
    artifactSha256: digestSchema,
    datasetExportSha256: digestSchema,
    datasetPolicySha256: digestSchema,
    featureCount: z.number().int().positive().max(1_024),
    hasCalibratedIntervals: z.boolean(),
    format: z.enum(["native_linear", "native_logistic", "onnx"]),
    outputSemantics: z.enum(["regression", "binary_probability"]),
    intendedUse: z.string().min(1).max(4_096),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    fallbackReason: z.string().min(1).max(4_096),
    datasets: z.array(datasetOptionSchema).min(1).max(4_096),
  })
  .strict()

const forecastPreparationOptionsSchema = z
  .object({
    runtimeGenerationSha256: digestSchema,
    models: z.array(modelOptionSchema).max(4_096),
  })
  .strict()

const forecastPreparationReceiptSchema = z
  .object({
    receiptId: z.string().uuid(),
    receiptSha256: digestSchema,
    expiresAtUnixNanos: losslessIntegerSchema,
  })
  .strict()

const modelPreviewSchema = modelOptionSchema.omit({ datasets: true })

const forecastPreparationPreviewSchema = z
  .object({
    receipt: forecastPreparationReceiptSchema,
    preview: z
      .object({
        model: modelPreviewSchema,
        instrumentId: z.string().uuid(),
        instrumentLabel: z.string().min(1).max(240),
        observedFromUnixNanos: losslessIntegerSchema,
        observedThroughUnixNanos: losslessIntegerSchema,
        availableAtUnixNanos: losslessIntegerSchema,
        observedPoints: z.number().int().positive().max(4_096),
        horizonPoints: z.number().int().positive().max(512),
        horizonStepNanos: losslessIntegerSchema,
        validityNanos: losslessIntegerSchema,
        evidenceSha256: digestSchema,
        requestSha256: digestSchema,
        runtimeGenerationSha256: digestSchema,
      })
      .strict(),
  })
  .strict()

const forecastJobReceiptSchema = z
  .object({
    jobId: z.string().uuid(),
    generation: z.number().int().positive(),
    sequence: z.number().int().nonnegative(),
    state: z.literal("queued"),
  })
  .strict()

export type ForecastPreparationOptions = z.infer<
  typeof forecastPreparationOptionsSchema
>
export type ForecastPreparationModel = ForecastPreparationOptions["models"][number]
export type ForecastPreparationDataset = ForecastPreparationModel["datasets"][number]
export type ForecastPreparationPolicy = ForecastPreparationDataset["policies"][number]
export type ForecastPreparationManifest = z.infer<typeof manifestSchema>
export type ForecastPreparationReceipt = z.infer<
  typeof forecastPreparationReceiptSchema
>
export type ForecastPreparationPreview = z.infer<
  typeof forecastPreparationPreviewSchema
>
export type ForecastPreparedJobReceipt = z.infer<typeof forecastJobReceiptSchema>

export interface ForecastPreparationSelection {
  modelId: string
  bundleId: string
  bundleVersion: number
  datasetManifest: ForecastPreparationManifest
  instrumentId: string
  horizonPoints: number
  horizonStepNanos: string
  validityNanos: string
}

export function parseForecastPreparationOptions(
  result: ApplicationResult,
): ForecastPreparationOptions {
  const parsed = forecastPreparationOptionsSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned unsupported forecast-preparation choices.",
    )
  }
  return parsed.data
}

export function parseForecastPreparationPreview(
  result: ApplicationResult,
): ForecastPreparationPreview {
  const parsed = forecastPreparationPreviewSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported forecast evidence preview.",
    )
  }
  return parsed.data
}

export function parseForecastPreparedJobReceipt(
  result: ApplicationResult,
): ForecastPreparedJobReceipt {
  const parsed = forecastJobReceiptSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported durable forecast-job receipt.",
    )
  }
  return parsed.data
}
