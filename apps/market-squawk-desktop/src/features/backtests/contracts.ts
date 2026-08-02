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
const metricSchema = z
  .object({
    name: z.string().min(1),
    value: z.number().refine(Number.isFinite),
  })
  .strict()

const completedStatusSchema = z
  .object({
    state: z.literal("completed"),
    resultDigest: digestSchema,
    artifact: z
      .object({
        reference: z.string().min(1),
        digest: digestSchema,
        byteCount: z.number().int().positive(),
      })
      .strict(),
    metrics: z.array(metricSchema).max(256),
    datasetPartition: z
      .object({
        startsAtUnixNanos: losslessIntegerSchema,
        endsAtUnixNanos: losslessIntegerSchema,
      })
      .strict()
      .nullable(),
    fillCount: z.number().int().nonnegative(),
    noActionCount: z.number().int().nonnegative(),
    accountingReconciliation: z.literal("independent"),
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
    recordVersion: z.literal(1),
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
