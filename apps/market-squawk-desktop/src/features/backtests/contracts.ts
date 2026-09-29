import { z } from "zod"

import { jobReceiptSchema } from "@/features/backup/contracts"
import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.string().datetime({ offset: true })
const exactDecimalSchema = z.string().regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/)
const opaqueTokenSchema = z.string().min(1).max(256)

const studyDigestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const studyIntegerTextSchema = z.string().regex(/^(?:0|-?[1-9][0-9]*)$/)
const studyUnsignedTextSchema = z.string().regex(/^(?:0|[1-9][0-9]*)$/)
const studyPositiveTextSchema = z.string().regex(/^[1-9][0-9]*$/)
const studyCountSchema = losslessIntegerSchema.refine((value) => /^(?:0|[1-9][0-9]*)$/.test(value))
const studyDecimalSchema = z.string().regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$/)
const studyUuidSchema = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
const studyTextSchema = z.string().min(1).max(1024)
const studyPopulationSchema = z.object({
  totalSignals: studyCountSchema,
  completedSubjectAndBenchmark: studyCountSchema,
  noAction: studyCountSchema,
  unavailable: studyCountSchema,
  censoredTargetAfterCutoff: studyCountSchema,
  censoredOutsideFold: studyCountSchema,
  entryUnfilled: studyCountSchema,
  exitUnfilled: studyCountSchema,
  benchmarkUnavailable: studyCountSchema,
  completedSubject: studyCountSchema,
  accompanyingBenchmarkCompleted: studyCountSchema,
  accompanyingBenchmarkUnavailable: studyCountSchema,
}).strict()
const studyBenchmarkSchema = z.object({
  instrumentId: studyUuidSchema,
  approvalDigest: studyDigestSchema,
}).strict()
const studyBenchmarkResultSchema = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("available"),
    meanCostAdjustedReturn: studyDecimalSchema,
    meanExcessReturn: studyDecimalSchema,
  }).strict(),
  z.object({ status: z.literal("unavailable") }).strict(),
])
const recommendationBacktestSchema = z.object({
  requestDigest: studyDigestSchema,
  evidenceDigest: studyDigestSchema,
  studyBasis: z.enum(["historical_as_known", "retrospective_frozen_snapshot"]),
  studyLimitations: z.array(z.enum([
    "historical_revision_coverage_unproven",
    "later_vintage_inputs",
    "present_day_fixed_cohort",
    "simulated_availability",
  ])).max(4),
  snapshotAsOfUnixNanos: studyIntegerTextSchema,
  sourceSnapshotDigest: studyDigestSchema,
  targetHorizonNanos: studyPositiveTextSchema,
  simulationCutoffUnixNanos: studyIntegerTextSchema,
  evaluatedAtUnixNanos: studyIntegerTextSchema,
  publishedAtUnixNanos: studyIntegerTextSchema,
  availableAtUnixNanos: studyIntegerTextSchema,
  expiresAtUnixNanos: studyIntegerTextSchema,
  population: studyPopulationSchema,
  folds: z.array(z.object({
    foldId: z.string().min(1).max(256),
    startsAtUnixNanos: studyIntegerTextSchema,
    endsAtUnixNanos: studyIntegerTextSchema,
    population: studyPopulationSchema,
  }).strict()).max(1024),
  aggregate: z.discriminatedUnion("status", [
    z.object({
      status: z.literal("available"),
      observationCount: studyCountSchema,
      independentFoldCount: studyCountSchema,
      meanCostAdjustedReturn: studyDecimalSchema,
      worstMaximumDrawdown: studyDecimalSchema,
      positiveFoldCount: studyCountSchema,
      positiveFoldStability: studyDecimalSchema,
      benchmark: studyBenchmarkResultSchema,
      accompanyingBenchmark: studyBenchmarkResultSchema,
    }).strict(),
    z.object({
      status: z.literal("unavailable"),
      reason: z.enum(["truncated-signal-population", "incomplete-declared-entry", "missing-completed-observation-in-fold"]),
    }).strict(),
  ]),
  methodology: z.object({
    policyDigest: studyDigestSchema,
    subjectInstrumentId: studyUuidSchema,
    reportingCurrency: z.string().regex(/^[A-Z]{3}$/),
    decisionLagNanos: studyUnsignedTextSchema.nullable(),
    executionPriceRounding: z.literal("adverse-tick-rounding-after-costs"),
    priceBasis: z.literal("raw-with-corporate-action-ledger"),
    targetTiming: z.literal("financial-origin-plus-365-elapsed-days"),
    distributionTreatment: z.literal("cash-entitlement-without-reinvestment"),
    distributionTiming: studyTextSchema,
    executionBasis: z.enum(["observed-quote-depth", "completed-daily-bar"]),
    assumedFullSpreadBasisPoints: studyCountSchema.nullable(),
    fillTiming: z.enum(["next-eligible-observation", "next-eligible-completed-bar-close"]),
    participationBasis: z.enum(["observed-executable-depth", "completed-bar-traded-volume"]),
    executionLimitations: z.array(studyTextSchema).max(4),
    rawPriceEvidenceDigest: studyDigestSchema,
    corporateActionContentDigest: studyDigestSchema,
    corporateActionAuditDigest: studyDigestSchema,
    corporateActionCoverageStartsAtUnixNanos: studyIntegerTextSchema,
    primaryBenchmark: studyBenchmarkSchema,
    accompanyingBenchmark: studyBenchmarkSchema,
  }).strict(),
  executionAssumptions: z.object({
    feeBasisPointsPerLeg: studyCountSchema,
    slippageBasisPointsPerLeg: studyCountSchema,
    maximumRandomSlippageBasisPointsPerLeg: studyCountSchema,
    maximumParticipationBasisPoints: studyCountSchema,
    latencyNanos: studyPositiveTextSchema,
    allowPartialFills: z.boolean(),
    digest: studyDigestSchema,
  }).strict(),
}).strict()

const recommendationBacktestEnvelopeSchema = z.object({
  data: recommendationBacktestSchema,
  metadata: z.object({
    completeness: z.literal("complete"),
    returnedItems: z.literal(1),
    availableItems: z.literal(1),
  }).strict(),
}).strict()

export type RecommendationBacktest = z.infer<typeof recommendationBacktestSchema>

export function parseRecommendationBacktest(result: ApplicationResult): RecommendationBacktest {
  const parsed = recommendationBacktestEnvelopeSchema.safeParse(result)
  if (!parsed.success) throw new Error("This saved historical study could not be opened.")
  return parsed.data.data
}

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

const backtestStartResultSchema = jobReceiptSchema.strict()

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
export type BacktestStartResult = z.infer<typeof backtestStartResultSchema>
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

export function parseBacktestStart(result: ApplicationResult): BacktestStartResult {
  const parsed = backtestStartResultSchema.safeParse(result.data)
  if (!parsed.success) throw new Error("This backtest cannot be started right now.")
  return parsed.data
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
