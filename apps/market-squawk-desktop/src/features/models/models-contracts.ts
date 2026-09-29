import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)

const nativeCalendarDateSchema = z
  .object({
    year: z.number().int().min(1).max(65_535),
    month: z.number().int().min(1).max(12),
    day: z.number().int().min(1).max(31),
  })
  .strict()

const modelTrainingPeriodSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("exact_time"),
      startUnixNanos: losslessIntegerSchema,
      endUnixNanos: losslessIntegerSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("fiscal_dates"),
      start: nativeCalendarDateSchema,
      end: nativeCalendarDateSchema,
    })
    .strict(),
])

const displayValueSchema = z
  .object({
    exact: exactDecimalSchema,
    formatted: z.string().min(1).max(120),
  })
  .strict()

const investmentDisplaySchema = z
  .object({
    name: z.string().min(1).max(240),
    symbol: z.string().min(1).max(64).nullable(),
    description: z.string().min(1).max(400),
  })
  .strict()

const unsignedIntegerTextSchema = z.string().regex(/^(?:0|[1-9]\d*)$/)
const positiveIntegerTextSchema = z.string().regex(/^[1-9]\d*$/)
const sha256Schema = z.string().regex(/^[0-9a-f]{64}$/)
const probabilitySchema = z.number().min(0).max(1)
const eventCostPolicySchema = z.object({
  version: z.literal(1),
  execution_policy_version: z.literal(3),
  fee_basis_points: z.number().int().min(0).max(10_000),
  slippage_basis_points: z.number().int().min(0).max(10_000),
  maximum_random_slippage_basis_points: z.number().int().min(0).max(10_000),
  maximum_participation_basis_points: z.number().int().min(1).max(10_000),
  latency_nanos: unsignedIntegerTextSchema,
  allow_partial_fills: z.boolean(),
  fee_decimal_scale: z.number().int().min(0).max(28),
  reporting_currency: z.string().regex(/^[A-Z]{3}$/),
  quantity_lots: positiveIntegerTextSchema,
  maximum_entry_lag_nanos: unsignedIntegerTextSchema,
  maximum_exit_lag_nanos: unsignedIntegerTextSchema,
  seed: unsignedIntegerTextSchema,
  execution_basis: z.enum(["observed_quote_depth", "completed_daily_bar"]),
  daily_bar_assumed_spread_basis_points: z.number().int().min(0).max(10_000).nullable(),
  liquidity_priority: z.literal("signal_time_then_order_id"),
  convention: z.literal("long_round_trip_total_wealth_including_entitlements"),
}).strict()
const probabilityEventSchema = z.object({
  horizonNanos: positiveIntegerTextSchema,
  originBasis: z.enum(["exact_effective_timestamp", "completed_bar_close", "named_session_close_for_nominal_daily_bar"]),
  definition: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("price_higher") }).strict(),
    z.object({ kind: z.literal("benchmark_outperformance"), benchmarkInstrumentId: z.string().uuid(),
      benchmarkDefinition: z.object({ algorithm: z.enum(["sha256", "blake3"]), digest: sha256Schema }).strict(),
    }).strict(),
    z.object({ kind: z.literal("profit_after_costs"), policy: eventCostPolicySchema }).strict(),
  ]),
}).strict()

export const forecastTargetSchema = z
  .object({
    label: z.string().min(1).max(200),
    meaning: z.string().min(1).max(1_000),
    valueKind: z.enum(["market_price", "percentage_return", "probability", "financial_amount"]),
    unitLabel: z.string().min(1).max(80),
    currencyCode: z.string().regex(/^[A-Z]{3}$/).nullable(),
    event: probabilityEventSchema.nullable(),
  })
  .strict()
  .refine((target) => (target.valueKind === "probability") === (target.event !== null),
    "An event probability must retain its original event definition.")

const productForecastHorizonSchema = z
  .object({
    label: z.string().min(1).max(200),
    description: z.string().min(1).max(1_000),
    points: z.number().int().positive().max(512),
  })
  .strict()

export const forecastModelEvidenceSchema = z
  .object({
    modelToken: z.string().uuid(),
    overall: z.enum(["sufficient", "limited", "unavailable"]),
    pitInputs: z.enum(["sufficient", "limited", "unavailable"]),
    outOfSample: z.enum(["sufficient", "limited", "unavailable"]),
    horizonAlignment: z.enum(["sufficient", "limited", "unavailable"]),
    calibration: z.enum(["calibrated", "limited", "unavailable"]),
    interpretation: z.string().min(1).max(1_000),
  })
  .strict()

const modelEvidenceSchema = z
  .object({
    modelToken: z.string().uuid(),
    label: z.string().min(1).max(240),
    objective: z.enum(["numeric_outcome", "likelihood"]),
    intendedUse: z.string().min(1).max(4_096),
    evidenceState: z.enum(["sufficient", "limited", "unavailable"]),
    training: z
      .object({
        period: modelTrainingPeriodSchema,
        studyBasis: z
          .enum(["historical_as_known", "retrospective_frozen_snapshot"])
          .nullable(),
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
    progressPercent: exactDecimalSchema.nullable(),
    updatedAtUnixNanos: losslessIntegerSchema,
  })
  .strict()

const modelActivityPageSchema = z
  .object({ activities: z.array(modelActivitySchema).max(1_024) })
  .strict()

export const forecastSummarySchema = z
  .object({
    forecastToken: z.string().uuid(),
    investment: investmentDisplaySchema,
    target: forecastTargetSchema,
    modelEvidence: forecastModelEvidenceSchema,
    observedThroughUnixNanos: losslessIntegerSchema.nullable(),
    createdAtUnixNanos: losslessIntegerSchema,
    expiresAtUnixNanos: losslessIntegerSchema,
    horizon: productForecastHorizonSchema,
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
  .object({ lower: displayValueSchema, upper: displayValueSchema })
  .strict()

const financialTargetSchema = z.object({
  ordinal: z.number().int().positive().max(4_294_967_295),
  period: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("instant"), instant: nativeCalendarDateSchema }).strict(),
    z.object({ kind: z.literal("duration"), start: nativeCalendarDateSchema, end: nativeCalendarDateSchema }).strict(),
  ]).nullable(),
}).strict()

const forecastPointSchema = z
  .object({
    targetAtUnixNanos: losslessIntegerSchema.nullable(),
    financialTarget: financialTargetSchema.nullable(),
    central: displayValueSchema,
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
    value: displayValueSchema,
  })
  .strict()

const driftMonitoringSchema = z
  .object({
    state: z.enum(["awaiting_outcomes", "outcomes_available"]),
    observedCount: z.number().int().nonnegative(),
    includedCount: z.number().int().nonnegative(),
    truncated: z.boolean(),
    meanAbsoluteError: z
      .object({
        value: displayValueSchema,
        rounding: z
          .object({
            state: z.enum(["exact", "rounded"]),
            decimalPlaces: z.number().int().nonnegative().max(18),
            mode: z.literal("half_even"),
          })
          .strict(),
      })
      .strict()
      .nullable(),
    interpretation: z.string().min(1).max(2_000),
  })
  .strict()

const calibrationSchema = z
  .object({
    window: z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("exact_time"), start: losslessIntegerSchema, end: losslessIntegerSchema }).strict(),
      z.object({ kind: z.literal("fiscal_dates"), start: nativeCalendarDateSchema, end: nativeCalendarDateSchema }).strict(),
    ]),
    observationCount: z.number().int().positive(),
    coverage: z
      .array(
        z
          .object({
            targetCoveragePercent: displayValueSchema,
          })
          .strict(),
      )
      .length(3),
    interpretation: z.string().min(1).max(2_000),
    assumptions: z.string().min(1).max(2_000),
  })
  .strict()

const probabilityWindowSchema = z.object({
  kind: z.literal("exact_time"), start: losslessIntegerSchema, end: losslessIntegerSchema,
  observationCount: z.number().int().positive().max(4_294_967_295),
}).strict().refine((window) => BigInt(window.start) < BigInt(window.end), "An evidence window must be ordered.")
const probabilityCalibrationSchema = z.object({
  method: z.literal("sigmoid_logit_affine_v1"),
  policySha256: sha256Schema,
  outcomesSha256: sha256Schema,
  trainWindow: probabilityWindowSchema,
  calibrationWindow: probabilityWindowSchema,
  evaluationWindow: probabilityWindowSchema,
  calibrationSlope: z.number(), calibrationIntercept: z.number(),
  brierScore: probabilitySchema, logLoss: z.number().nonnegative(),
  reliabilityBins: z.array(z.object({
    observationCount: z.number().int().nonnegative().max(4_294_967_295),
    meanProbability: probabilitySchema.nullable(),
    observedFrequency: probabilitySchema.nullable(),
  }).strict().refine((bin) => bin.observationCount === 0
    ? bin.meanProbability === null && bin.observedFrequency === null
    : bin.meanProbability !== null && bin.observedFrequency !== null,
  "A reliability group must distinguish missing outcomes from zero probability.")).length(10),
}).strict()

export const forecastVintageSchema = z
  .object({
    forecastToken: z.string().uuid(),
    investment: investmentDisplaySchema,
    target: forecastTargetSchema,
    modelEvidence: forecastModelEvidenceSchema,
    observedThroughUnixNanos: losslessIntegerSchema.nullable(),
    availableAtUnixNanos: losslessIntegerSchema,
    createdAtUnixNanos: losslessIntegerSchema,
    expiresAtUnixNanos: losslessIntegerSchema,
    horizon: productForecastHorizonSchema,
    observedHistory: z.array(observedHistoryPointSchema).max(4_096),
    estimates: z.array(forecastPointSchema).min(1).max(512),
    calibration: calibrationSchema.nullable(),
    probabilityCalibration: probabilityCalibrationSchema.nullable(),
    limitations: z.array(z.string().min(1).max(4_096)).max(256),
    unavailableBehavior: z.literal("no_action"),
    outcomeMonitoring: driftMonitoringSchema,
    analysisOnly: z.literal(true),
  })
  .strict()
  .superRefine((vintage, context) => {
    const financial = vintage.target.valueKind === "financial_amount"
    const probability = vintage.target.valueKind === "probability"
    if (probability ? vintage.calibration !== null || vintage.observedHistory.length !== 0
      || vintage.estimates.some((point) => point.ranges !== null || !/^(?:0(?:\.\d+)?|1(?:\.0+)?)$/.test(point.central.exact))
      : vintage.probabilityCalibration !== null) {
      context.addIssue({ code: "custom", message: "Event probabilities cannot reuse price history or interval evidence." })
    }
    if (financial !== (vintage.observedThroughUnixNanos === null)
      || financial && vintage.observedHistory.length !== 0
      || vintage.estimates.some((point) => financial
        ? point.targetAtUnixNanos !== null || point.financialTarget === null
        : point.targetAtUnixNanos === null || point.financialTarget !== null)) {
      context.addIssue({ code: "custom", message: "The forecast mixes fiscal periods and exact timestamps." })
    }
  })

const forecastOutcomeSchema = z
  .object({
    targetAtUnixNanos: losslessIntegerSchema,
    observedAtUnixNanos: losslessIntegerSchema,
    availableAtUnixNanos: losslessIntegerSchema,
    recordedAtUnixNanos: losslessIntegerSchema,
    actual: displayValueSchema,
    signedError: displayValueSchema,
    absoluteError: displayValueSchema,
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
