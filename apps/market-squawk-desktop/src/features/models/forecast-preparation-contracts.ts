import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const investmentOptionSchema = z
  .object({
    investmentToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    observedFromUnixNanos: losslessIntegerSchema,
    observedThroughUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    observedPoints: z.number().int().positive().max(4_096),
  })
  .strict()

const forecastPolicySchema = z
  .object({
    policyToken: z.string().uuid(),
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

const forecastHistorySchema = z
  .object({
    historyToken: z.string().uuid(),
    instruments: z.array(investmentOptionSchema).min(1).max(4_096),
    policies: z.array(forecastPolicySchema).min(1).max(64),
  })
  .strict()

const forecastModelSchema = z
  .object({
    modelToken: z.string().uuid(),
    name: z.string().min(1).max(200),
    objective: z.enum(["numeric_outcome", "likelihood"]),
    intendedUse: z.string().min(1).max(4_096),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    evidenceState: z.enum(["calibrated", "limited"]),
    unavailableBehavior: z.literal("no_action"),
  })
  .strict()

const forecastPreparationModelSchema = forecastModelSchema.extend({
  histories: z.array(forecastHistorySchema).min(1).max(4_096),
})

const forecastPreparationOptionsSchema = z
  .object({
    models: z.array(forecastPreparationModelSchema).max(4_096),
  })
  .strict()

const forecastPreparationReceiptSchema = z
  .object({
    confirmationToken: z.string().uuid(),
    expiresAtUnixNanos: losslessIntegerSchema,
  })
  .strict()

const forecastPreparationPreviewSchema = z
  .object({
    receipt: forecastPreparationReceiptSchema,
    preview: z
      .object({
        model: forecastModelSchema,
        investmentToken: z.string().uuid(),
        instrumentLabel: z.string().min(1).max(240),
        observedFromUnixNanos: losslessIntegerSchema,
        observedThroughUnixNanos: losslessIntegerSchema,
        availableAtUnixNanos: losslessIntegerSchema,
        observedPoints: z.number().int().positive().max(4_096),
        horizonPoints: z.number().int().positive().max(512),
        horizonStepNanos: losslessIntegerSchema,
        validityNanos: losslessIntegerSchema,
        evidenceState: z.enum(["calibrated", "limited"]),
        analysisOnly: z.literal(true),
      })
      .strict(),
  })
  .strict()

const forecastStartResultSchema = z
  .object({ state: z.literal("queued") })
  .strict()

export type ForecastPreparationOptions = z.infer<
  typeof forecastPreparationOptionsSchema
>
export type ForecastPreparationModel = ForecastPreparationOptions["models"][number]
export type ForecastPreparationHistory = ForecastPreparationModel["histories"][number]
export type ForecastPreparationPolicy = ForecastPreparationHistory["policies"][number]
export type ForecastPreparationPreview = z.infer<
  typeof forecastPreparationPreviewSchema
>
export interface ForecastStartResult {
  state: "queued"
}

export interface ForecastPreparationSelection {
  modelToken: string
  historyToken: string
  investmentToken: string
  policyToken: string
  horizonPoints: number
  validityNanos: string
}

export function parseForecastPreparationOptions(
  result: ApplicationResult,
): ForecastPreparationOptions {
  const parsed = forecastPreparationOptionsSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error("Forecast choices are unavailable right now.")
  }
  return parsed.data
}

export function parseForecastPreparationPreview(
  result: ApplicationResult,
): ForecastPreparationPreview {
  const parsed = forecastPreparationPreviewSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error("This forecast cannot be reviewed right now.")
  }
  return parsed.data
}

export function parseForecastStart(
  result: ApplicationResult,
): ForecastStartResult {
  const parsed = forecastStartResultSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error("This forecast cannot be started right now.")
  }
  return { state: "queued" }
}
