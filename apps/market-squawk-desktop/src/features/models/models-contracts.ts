import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)

const modelEvidenceSchema = z
  .object({
    modelToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    objective: z.enum(["numeric_outcome", "likelihood"]),
    intendedUse: z.string().min(1).max(4_096),
    evidenceState: z.enum(["sufficient", "limited", "unavailable"]),
    training: z
      .object({
        observedFromUnixNanos: losslessIntegerSchema,
        observedThroughUnixNanos: losslessIntegerSchema,
        availableAtUnixNanos: losslessIntegerSchema,
        trainingObservations: z.number().int().nonnegative(),
        validationObservations: z.number().int().nonnegative(),
        outOfSampleObservations: z.number().int().nonnegative(),
        rollingOutOfSampleFolds: z.number().int().nonnegative(),
        evaluatedHorizons: z.number().int().nonnegative(),
      })
      .strict(),
    validation: z
      .array(
        z
          .object({
            label: z.string().min(1).max(200),
            value: exactDecimalSchema,
            interpretation: z.string().min(1).max(1_000),
          })
          .strict(),
      )
      .max(64),
    coverage: z
      .array(
        z
          .object({
            label: z.string().min(1).max(200),
            state: z.enum(["evaluated", "limited", "unavailable"]),
            interpretation: z.string().min(1).max(1_000),
          })
          .strict(),
      )
      .max(64),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    unavailableBehavior: z.literal("no_action"),
    analysisOnly: z.literal(true),
  })
  .strict()

const modelEvidencePageSchema = z
  .object({ models: z.array(modelEvidenceSchema).max(4_096) })
  .strict()

const modelActivitySchema = z
  .object({
    activityToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    state: z.enum(["queued", "running", "completed", "failed"]),
    statusMessage: z.string().min(1).max(1_000),
    progressPercent: exactDecimalSchema.nullable(),
    updatedAtUnixNanos: losslessIntegerSchema,
  })
  .strict()

const modelActivityPageSchema = z
  .object({ activities: z.array(modelActivitySchema).max(1_024) })
  .strict()

const forecastEvidenceStateSchema = z.enum(["calibrated", "limited"])

export const forecastSummarySchema = z
  .object({
    forecastToken: z.string().uuid(),
    investmentToken: z.string().uuid(),
    observedThroughUnixNanos: losslessIntegerSchema,
    createdAtUnixNanos: losslessIntegerSchema,
    expiresAtUnixNanos: losslessIntegerSchema,
    horizon: z
      .object({
        points: z.number().int().positive().max(512),
        stepNanos: losslessIntegerSchema,
      })
      .strict(),
    evidenceState: forecastEvidenceStateSchema,
    historicalObservationCount: z.number().int().nonnegative().max(4_096),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
  })
  .strict()

const forecastPageSchema = z
  .object({
    forecasts: z.array(forecastSummarySchema).max(4_096),
    available: z.number().int().nonnegative(),
    truncated: z.boolean(),
  })
  .strict()

const forecastRangeSchema = z
  .object({ lower: exactDecimalSchema, upper: exactDecimalSchema })
  .strict()

const forecastPointSchema = z
  .object({
    targetAtUnixNanos: losslessIntegerSchema,
    central: exactDecimalSchema,
    ranges: z
      .object({
        likely: forecastRangeSchema,
        wider: forecastRangeSchema,
        stress: forecastRangeSchema,
      })
      .strict()
      .nullable(),
  })
  .strict()

const observedHistoryPointSchema = z
  .object({
    observedAtUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    value: exactDecimalSchema,
  })
  .strict()

const driftMonitoringSchema = z
  .object({
    state: z.enum(["awaiting_outcomes", "outcomes_available"]),
    observedCount: z.number().int().nonnegative(),
    includedCount: z.number().int().nonnegative(),
    truncated: z.boolean(),
    meanAbsoluteError: exactDecimalSchema.nullable(),
    interpretation: z.string().min(1).max(2_000),
  })
  .strict()

const calibrationSchema = z
  .object({
    windowStartUnixNanos: losslessIntegerSchema,
    windowEndUnixNanos: losslessIntegerSchema,
    observationCount: z.number().int().positive(),
    coverage: z
      .array(
        z
          .object({
            targetCoveragePercent: exactDecimalSchema,
            realizedCovered: z.number().int().nonnegative(),
            realizedTotal: z.number().int().positive(),
          })
          .strict(),
      )
      .length(3),
    interpretation: z.string().min(1).max(2_000),
    assumptions: z.string().min(1).max(2_000),
  })
  .strict()

export const forecastVintageSchema = z
  .object({
    forecastToken: z.string().uuid(),
    investmentToken: z.string().uuid(),
    observedThroughUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    createdAtUnixNanos: losslessIntegerSchema,
    expiresAtUnixNanos: losslessIntegerSchema,
    horizon: z
      .object({
        points: z.number().int().positive().max(512),
        stepNanos: losslessIntegerSchema,
      })
      .strict(),
    evidenceState: forecastEvidenceStateSchema,
    observedHistory: z.array(observedHistoryPointSchema).max(4_096),
    estimates: z.array(forecastPointSchema).min(1).max(512),
    calibration: calibrationSchema.nullable(),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    unavailableBehavior: z.literal("no_action"),
    outcomeMonitoring: driftMonitoringSchema,
    analysisOnly: z.literal(true),
  })
  .strict()

const forecastOutcomeSchema = z
  .object({
    targetAtUnixNanos: losslessIntegerSchema,
    observedAtUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    actual: exactDecimalSchema,
    signedError: exactDecimalSchema,
    absoluteError: exactDecimalSchema,
  })
  .strict()

const forecastOutcomesSchema = z
  .object({
    forecastToken: z.string().uuid(),
    outcomes: z.array(forecastOutcomeSchema).max(4_096),
    available: z.number().int().nonnegative(),
    truncated: z.boolean(),
  })
  .strict()

export type ModelEvidence = z.infer<typeof modelEvidenceSchema>
export type ModelActivity = z.infer<typeof modelActivitySchema>
export type ForecastSummary = z.infer<typeof forecastSummarySchema>
export type ForecastVintage = z.infer<typeof forecastVintageSchema>
export type ForecastOutcome = z.infer<typeof forecastOutcomeSchema>

export function parseModelEvidence(result: ApplicationResult): ModelEvidence[] {
  const parsed = modelEvidencePageSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Model evidence is unavailable right now.")
  return parsed.data.models
}

export function parseModelActivities(result: ApplicationResult): ModelActivity[] {
  const parsed = modelActivityPageSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Research activity is unavailable right now.")
  return parsed.data.activities
}

export interface ForecastPage {
  forecasts: ForecastSummary[]
  available: number
  truncated: boolean
}

export function parseForecasts(result: ApplicationResult): ForecastPage {
  const parsed = forecastPageSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Forecasts are unavailable right now.")
  return parsed.data
}

export function parseForecastVintage(result: ApplicationResult): ForecastVintage {
  const parsed = forecastVintageSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Forecast details are unavailable right now.")
  return parsed.data
}

export interface ForecastOutcomes {
  forecastToken: string
  outcomes: ForecastOutcome[]
  available: number
  truncated: boolean
}

export function parseForecastOutcomes(result: ApplicationResult): ForecastOutcomes {
  const parsed = forecastOutcomesSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Forecast outcomes are unavailable right now.")
  return parsed.data
}

export function isActiveModelActivity(activity: ModelActivity): boolean {
  return activity.state === "queued" || activity.state === "running"
}
