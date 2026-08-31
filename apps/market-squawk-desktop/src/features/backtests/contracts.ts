import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.string().datetime({ offset: true })
const exactDecimalSchema = z.string().regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/)
const opaqueTokenSchema = z.string().min(1).max(256)

const namedChoiceSchema = z
  .object({
    token: opaqueTokenSchema,
    label: z.string().min(1).max(200),
    description: z.string().min(1).max(1_000),
  })
  .strict()

const periodChoiceSchema = z
  .object({
    periodToken: opaqueTokenSchema,
    label: z.string().min(1).max(240),
    startsAt: timestampSchema,
    endsAt: timestampSchema,
  })
  .strict()
  .superRefine((period, context) => {
    if (Date.parse(period.startsAt) >= Date.parse(period.endsAt)) {
      context.addIssue({ code: "custom", message: "The backtest period is not ordered." })
    }
  })

const historyChoiceSchema = z
  .object({
    historyToken: opaqueTokenSchema,
    label: z.string().min(1).max(200),
    investmentCount: z.number().int().positive(),
    periods: z.array(periodChoiceSchema).min(1).max(8),
  })
  .strict()

const backtestPreparationOptionsSchema = z
  .object({
    histories: z.array(historyChoiceSchema).max(4_096),
    methods: z.array(namedChoiceSchema).min(1).max(16),
    costPlans: z.array(namedChoiceSchema).min(1).max(16),
    portfolios: z.array(namedChoiceSchema).min(1).max(16),
    comparisons: z.array(namedChoiceSchema).min(1).max(16),
    guidance: z.string().min(1).max(1_000),
  })
  .strict()

const costAssumptionsSchema = z
  .object({
    fees: z.string().min(1).max(200),
    spread: z.string().min(1).max(200),
    slippage: z.string().min(1).max(200),
    latency: z.string().min(1).max(200),
    participationLimit: z.string().min(1).max(200),
    partialFills: z.string().min(1).max(200),
  })
  .strict()

const evidenceStateSchema = z.enum(["verified", "limited", "unavailable"])

const backtestPreparationPreviewSchema = z
  .object({
    confirmationToken: opaqueTokenSchema,
    expiresAt: timestampSchema,
    investmentUniverse: z.string().min(1).max(400),
    period: z.string().min(1).max(300),
    method: z.string().min(1).max(200),
    costs: costAssumptionsSchema,
    portfolio: z.string().min(1).max(200),
    comparison: z.string().min(1).max(300),
    pointInTimeEvidence: evidenceStateSchema,
    outOfSamplePlan: z.string().min(1).max(1_000),
    evidence: z.array(z.string().min(1).max(1_000)).min(1).max(16),
    assumptions: z.array(z.string().min(1).max(1_000)).min(1).max(16),
    limitations: z.array(z.string().min(1).max(1_000)).max(32),
    analysisOnly: z.literal(true),
  })
  .strict()

const backtestStartResultSchema = z
  .object({ state: z.literal("queued") })
  .strict()

const backtestActivitySchema = z
  .object({
    backtestToken: opaqueTokenSchema,
    label: z.string().min(1).max(240),
    startedAt: timestampSchema,
    updatedAt: timestampSchema,
    state: z.enum(["queued", "running", "completed", "failed"]),
    progressPercent: exactDecimalSchema.nullable(),
  })
  .strict()

const backtestActivitiesSchema = z
  .object({
    activities: z.array(backtestActivitySchema).max(1_000),
  })
  .strict()

const pointInTimeEvidenceSchema = z
  .object({
    state: evidenceStateSchema,
    informationCutoff: timestampSchema,
    observedFrom: timestampSchema,
    observedThrough: timestampSchema,
    observationCount: z.number().int().nonnegative(),
    coveragePercent: exactDecimalSchema.nullable(),
    interpretation: z.string().min(1).max(2_000),
  })
  .strict()

const outOfSampleEvidenceSchema = z
  .object({
    state: z.enum(["evaluated", "limited", "not_evaluated"]),
    foldCount: z.number().int().nonnegative(),
    observationCount: z.number().int().nonnegative(),
    method: z.string().min(1).max(500),
    probabilityOfOverfittingPercent: exactDecimalSchema.nullable(),
    deflatedPerformanceProbabilityPercent: exactDecimalSchema.nullable(),
    expectedMaximumSharpe: exactDecimalSchema.nullable(),
    interpretation: z.string().min(1).max(2_000),
  })
  .strict()

const performanceSchema = z
  .object({
    totalReturnPercent: exactDecimalSchema,
    annualizedReturnPercent: exactDecimalSchema.nullable(),
    annualizedVolatilityPercent: exactDecimalSchema.nullable(),
    maximumDrawdownPercent: exactDecimalSchema,
    sharpeRatio: exactDecimalSchema.nullable(),
    winRatePercent: exactDecimalSchema.nullable(),
    turnoverPercent: exactDecimalSchema.nullable(),
  })
  .strict()

const costEvidenceSchema = costAssumptionsSchema.extend({
  totalCostPercent: exactDecimalSchema,
})

const executionEvidenceSchema = z
  .object({
    fillCount: z.number().int().nonnegative(),
    partialFillCount: z.number().int().nonnegative(),
    noActionCount: z.number().int().nonnegative(),
  })
  .strict()
  .superRefine((execution, context) => {
    if (execution.partialFillCount > execution.fillCount) {
      context.addIssue({
        code: "custom",
        message: "Partial fills cannot exceed total fills.",
      })
    }
  })

const comparisonEvidenceSchema = z
  .object({
    label: z.string().min(1).max(240),
    totalReturnPercent: exactDecimalSchema,
    excessReturnPercent: exactDecimalSchema,
  })
  .strict()

const completedBacktestSchema = z
  .object({
    state: z.literal("completed"),
    backtestToken: opaqueTokenSchema,
    label: z.string().min(1).max(240),
    completedAt: timestampSchema,
    expiresAt: timestampSchema.nullable(),
    investmentUniverse: z.string().min(1).max(400),
    method: z.string().min(1).max(240),
    period: z
      .object({ startsAt: timestampSchema, endsAt: timestampSchema })
      .strict(),
    pointInTimeEvidence: pointInTimeEvidenceSchema,
    outOfSampleEvidence: outOfSampleEvidenceSchema,
    performance: performanceSchema,
    costs: costEvidenceSchema,
    execution: executionEvidenceSchema,
    comparison: comparisonEvidenceSchema.nullable(),
    uncertainty: z.enum(["supported", "limited", "unavailable"]),
    interpretation: z.string().min(1).max(4_000),
    limitations: z.array(z.string().min(1).max(2_000)).max(64),
    invalidators: z.array(z.string().min(1).max(2_000)).max(64),
    analysisOnly: z.literal(true),
  })
  .strict()
  .superRefine((result, context) => {
    if (Date.parse(result.period.startsAt) >= Date.parse(result.period.endsAt)) {
      context.addIssue({ code: "custom", message: "The result period is not ordered." })
    }
  })

const unavailableBacktestSchema = z
  .object({
    state: z.literal("unavailable"),
    backtestToken: opaqueTokenSchema,
    label: z.string().min(1).max(240),
    reason: z.string().min(1).max(2_000),
    limitations: z.array(z.string().min(1).max(2_000)).max(64),
    unavailableBehavior: z.literal("no_action"),
  })
  .strict()

const backtestResultSchema = z.union([
  completedBacktestSchema,
  unavailableBacktestSchema,
])

export type BacktestPreparationOptions = z.infer<
  typeof backtestPreparationOptionsSchema
>
export type BacktestPreparationPreview = z.infer<
  typeof backtestPreparationPreviewSchema
>
export type BacktestActivity = z.infer<typeof backtestActivitySchema>
export type BacktestResult = z.infer<typeof backtestResultSchema>
export type CompletedBacktest = z.infer<typeof completedBacktestSchema>

export interface BacktestPreparationSelection {
  historyToken: string
  periodToken: string
  methodToken: string
  costToken: string
  portfolioToken: string
  comparisonToken: string
}

export function parseBacktestPreparationOptions(
  result: ApplicationResult,
): BacktestPreparationOptions {
  const parsed = backtestPreparationOptionsSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Backtest choices are unavailable right now.")
  return parsed.data
}

export function parseBacktestPreparationPreview(
  result: ApplicationResult,
): BacktestPreparationPreview {
  const parsed = backtestPreparationPreviewSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("This backtest cannot be reviewed right now.")
  return parsed.data
}

export function parseBacktestStart(result: ApplicationResult): { state: "queued" } {
  const parsed = backtestStartResultSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("This backtest cannot be started right now.")
  return { state: "queued" }
}

export function parseBacktestActivities(result: ApplicationResult): BacktestActivity[] {
  const parsed = backtestActivitiesSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("Backtest activity is unavailable right now.")
  return parsed.data.activities
}

export function parseBacktestResult(result: ApplicationResult): BacktestResult {
  const parsed = backtestResultSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("This backtest result is unavailable right now.")
  return parsed.data
}

export function newestBacktests(first: BacktestActivity, second: BacktestActivity): number {
  return Date.parse(second.updatedAt) - Date.parse(first.updatedAt)
}
