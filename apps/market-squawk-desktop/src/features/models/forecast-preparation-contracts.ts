import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

import {
  forecastModelEvidenceSchema,
  forecastTargetSchema,
} from "./models-contracts"

const investmentOptionSchema = z
  .object({
    investmentToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    observedFromUnixNanos: losslessIntegerSchema,
    observedThroughUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    observationCount: z.number().int().positive().max(4_096),
  })
  .strict()

const forecastHorizonSchema = z
  .object({
    horizonToken: z.string().uuid(),
    label: z.string().min(1).max(200),
    description: z.string().min(1).max(1_000),
  })
  .strict()

const forecastHistorySchema = z
  .object({
    historyToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    investments: z.array(investmentOptionSchema).min(1).max(4_096),
    horizons: z.array(forecastHorizonSchema).min(1).max(64),
  })
  .strict()

const forecastModelSchema = z
  .object({
    modelToken: z.string().uuid(),
    name: z.string().min(1).max(200),
    objective: z.enum(["numeric_outcome", "likelihood"]),
    target: forecastTargetSchema,
    modelEvidence: forecastModelEvidenceSchema,
    intendedUse: z.string().min(1).max(4_096),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
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

const forecastPreparationPreviewSchema = z
  .object({
    confirmationToken: z.string().uuid(),
    expiresAtUnixNanos: losslessIntegerSchema,
    model: forecastModelSchema,
    investmentToken: z.string().uuid(),
    instrumentLabel: z.string().min(1).max(240),
    observedFromUnixNanos: losslessIntegerSchema,
    observedThroughUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    observationCount: z.number().int().positive().max(4_096),
    horizon: forecastHorizonSchema.omit({ horizonToken: true }),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    analysisOnly: z.literal(true),
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
export type ForecastPreparationHorizon = ForecastPreparationHistory["horizons"][number]
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
  horizonToken: string
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
