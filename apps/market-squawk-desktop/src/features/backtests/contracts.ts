import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import {
  compareLosslessIntegers,
  losslessIntegerSchema,
} from "@/lib/lossless-integer"

export const BACKTEST_JOB_KIND = "analysis.backtest.v1"
export const BACKTEST_RESULT_AUTHORITY =
  "analysis.governed-backtest-terminal.v1"

const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const identifierSchema = z
  .string()
  .min(1)
  .max(256)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/)
const timestampSchema = z.string().datetime({ offset: true })

const namedPreparationOptionSchema = z
  .object({
    id: identifierSchema,
    label: z.string().min(1).max(200),
    description: z.string().min(1).max(1_000),
  })
  .strict()

const periodOptionSchema = z
  .object({
    id: identifierSchema,
    label: z.string().min(1).max(240),
    startsAt: timestampSchema,
    endsAt: timestampSchema,
  })
  .strict()

const datasetPreparationOptionSchema = z
  .object({
    id: identifierSchema,
    label: z.string().min(1).max(200),
    immutableGeneration: losslessIntegerSchema.refine(
      (value) => BigInt(value) > 0n,
      "Expected a positive immutable generation.",
    ),
    instrumentCount: z.number().int().positive(),
    periods: z.array(periodOptionSchema).min(1).max(2),
  })
  .strict()

const backtestPreparationOptionsSchema = z
  .object({
    datasets: z.array(datasetPreparationOptionSchema).max(4_096),
    strategies: z.array(namedPreparationOptionSchema).min(1).max(16),
    costPolicies: z.array(namedPreparationOptionSchema).min(1).max(16),
    seeds: z.array(namedPreparationOptionSchema).min(1).max(16),
    portfolios: z.array(namedPreparationOptionSchema).min(1).max(16),
    comparisons: z.array(namedPreparationOptionSchema).min(1).max(16),
    defaultLimitPolicy: z.string().min(1).max(1_000),
  })
  .strict()

const backtestPreparationReceiptSchema = z
  .object({
    receiptId: z.string().uuid(),
    preparationDigest: digestSchema,
  })
  .strict()

const backtestPreparationPreviewSchema = z
  .object({
    receipt: backtestPreparationReceiptSchema,
    expiresAt: timestampSchema,
    dataset: z.string().min(1).max(400),
    period: z.string().min(1).max(300),
    strategy: z.string().min(1).max(200),
    costPolicy: z.string().min(1).max(200),
    deterministicSeed: z.string().min(1).max(200),
    portfolio: z.string().min(1).max(200),
    comparison: z.string().min(1).max(300),
    evidence: z.array(z.string().min(1).max(1_000)).min(1).max(16),
    assumptions: z.array(z.string().min(1).max(1_000)).min(1).max(16),
  })
  .strict()

const backtestJobReceiptSchema = z
  .object({
    jobId: z.string().uuid(),
    generation: z.number().int().positive(),
    sequence: z.number().int().nonnegative(),
    state: z.literal("queued"),
  })
  .strict()

export type BacktestPreparationOptions = z.infer<
  typeof backtestPreparationOptionsSchema
>
export type BacktestPreparationPreview = z.infer<
  typeof backtestPreparationPreviewSchema
>
export type BacktestPreparationReceipt = z.infer<
  typeof backtestPreparationReceiptSchema
>
export type BacktestJobReceipt = z.infer<typeof backtestJobReceiptSchema>
export type BacktestPreparationSelection = {
  dataset: string
  period: string
  strategy: string
  costPolicy: string
  seed: string
  portfolio: string
  comparison: string
}

export function parseBacktestPreparationOptions(
  result: ApplicationResult,
): BacktestPreparationOptions {
  const parsed = backtestPreparationOptionsSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned unsupported guided-backtest options.",
    )
  }
  return parsed.data
}

export function parseBacktestPreparationPreview(
  result: ApplicationResult,
): BacktestPreparationPreview {
  const parsed = backtestPreparationPreviewSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported guided-backtest preview.",
    )
  }
  return parsed.data
}

export function parseBacktestJobReceipt(
  result: ApplicationResult,
): BacktestJobReceipt {
  const parsed = backtestJobReceiptSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported backtest-job receipt.",
    )
  }
  return parsed.data
}
const metricSchema = z
  .object({
    name: z.string().min(1),
    value: z.number().refine(Number.isFinite),
  })
  .strict()

const executionAssumptionsSchema = z
  .object({
    policyVersion: z.literal(3),
    feeBasisPoints: z.number().int().min(0).max(10_000),
    spreadModel: z.literal("observed-point-in-time-half-spread"),
    slippageBasisPoints: z.number().int().min(0).max(10_000),
    maximumRandomSlippageBasisPoints: z.number().int().min(0).max(10_000),
    latencyNanos: losslessIntegerSchema,
    maximumParticipationBasisPoints: z.number().int().min(1).max(10_000),
    liquidityPriority: z.literal("signal-time-then-order-id"),
    partialFillsAllowed: z.boolean(),
    feeDecimalScale: z.number().int().min(0).max(28),
  })
  .strict()
  .superRefine((value, context) => {
    if (compareLosslessIntegers(value.latencyNanos, "0") <= 0) {
      context.addIssue({
        code: "custom",
        message: "Backtest latency must be positive.",
      })
    }
  })

const legacyArtifactSchema = z
  .object({
    reference: z.string().min(1),
    digest: digestSchema,
    byteCount: z.number().int().positive(),
  })
  .strict()

const controlledReportArtifactSchema = z
  .object({
    artifactId: z.string().regex(/^backtest-report-[0-9a-f]{64}$/),
    sha256: digestSchema,
    byteCount: z.number().int().positive(),
    mediaType: z.literal("application/json"),
  })
  .strict()
  .superRefine((artifact, context) => {
    if (artifact.artifactId !== `backtest-report-${artifact.sha256}`) {
      context.addIssue({
        code: "custom",
        message: "The report identity does not bind its digest.",
      })
    }
  })

const cohortDiagnosticsSchema = z.union([
  z.object({ state: z.literal("not-evaluated") }).strict(),
  z
    .object({
      state: z.literal("completed"),
      evaluationId: digestSchema,
      probabilityOfBacktestOverfitting: z.number().min(0).max(1).refine(Number.isFinite),
      foldCount: z.number().int().min(2).max(1024),
      deflatedPerformanceProbability: z.number().min(0).max(1).refine(Number.isFinite),
      expectedMaximumSharpe: z.number().refine(Number.isFinite),
    })
    .strict(),
])

const completedStatusSchema = z
  .object({
    state: z.literal("completed"),
    resultDigest: digestSchema,
    artifact: z.union([legacyArtifactSchema, controlledReportArtifactSchema]),
    metrics: z.array(metricSchema).max(256),
    datasetPartition: z
      .object({
        startsAtUnixNanos: losslessIntegerSchema,
        endsAtUnixNanos: losslessIntegerSchema,
      })
      .strict()
      .nullable(),
    fillCount: z.number().int().nonnegative(),
    partialFillCount: z.number().int().nonnegative().optional(),
    noActionCount: z.number().int().nonnegative(),
    accountingReconciliation: z.literal("independent"),
    executionAssumptions: executionAssumptionsSchema.optional(),
    cohortDiagnostics: cohortDiagnosticsSchema.optional(),
  })
  .strict()
  .superRefine((status, context) => {
    if (
      status.datasetPartition &&
      compareLosslessIntegers(
        status.datasetPartition.startsAtUnixNanos,
        status.datasetPartition.endsAtUnixNanos,
      ) >= 0
    ) {
      context.addIssue({
        code: "custom",
        message: "The dataset partition is not ordered.",
      })
    }
  })

const backtestRecordSchema = z
  .object({
    recordVersion: z.union([z.literal(1), z.literal(2)]),
    runId: digestSchema,
    datasetIdentity: digestSchema,
    objectGraphDigest: digestSchema,
    executionAssumptionDigest: digestSchema,
    cohortAuthorityDigest: digestSchema.nullable(),
    cohortUniverseDigest: digestSchema.nullable(),
    seed: z.number().int().nonnegative(),
    selectionCriterion: z.string().min(1),
    status: z.union([
      completedStatusSchema,
      z.object({ state: z.literal("failed") }).strict(),
    ]),
  })
  .strict()
  .superRefine((record, context) => {
    if (
      (record.cohortAuthorityDigest === null) !==
      (record.cohortUniverseDigest === null)
    ) {
      context.addIssue({
        code: "custom",
        message: "The cohort evidence pair is incomplete.",
      })
    }
    if (record.recordVersion === 2 && record.status.state === "completed") {
      if (
        !("artifactId" in record.status.artifact) ||
        record.status.partialFillCount === undefined ||
        record.status.executionAssumptions === undefined ||
        record.status.cohortDiagnostics === undefined
      ) {
        context.addIssue({
          code: "custom",
          message: "The V2 governed record is missing required evidence.",
        })
      }
      if (
        record.status.cohortDiagnostics?.state === "completed" &&
        (record.cohortAuthorityDigest === null || record.cohortUniverseDigest === null)
      ) {
        context.addIssue({
          code: "custom",
          message: "Completed cohort diagnostics require the exact cohort authority bindings.",
        })
      }
    }
  })

export type BacktestRecord = z.infer<typeof backtestRecordSchema>
export type BacktestMetric = z.infer<typeof metricSchema>

export function parseBacktestRecord(result: ApplicationResult): BacktestRecord {
  const parsed = backtestRecordSchema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned an unsupported governed backtest record.",
    )
  }
  return parsed.data
}

export function metricValue(
  metrics: readonly BacktestMetric[],
  ...acceptedNames: string[]
): number | null {
  const accepted = new Set(acceptedNames.map(normalizeMetricName))
  return (
    metrics.find((metric) => accepted.has(normalizeMetricName(metric.name)))
      ?.value ?? null
  )
}

function normalizeMetricName(value: string): string {
  return value.toLocaleLowerCase().replace(/[_\s]+/g, "-")
}
