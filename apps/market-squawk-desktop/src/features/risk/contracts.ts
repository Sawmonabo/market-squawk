import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const RAW_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
const accountTokenSchema = z
  .string()
  .min(16)
  .max(512)
  .refine((value) => !RAW_UUID.test(value), "Expected an opaque product account token.")
const timestampSchema = z.string().datetime({ offset: true })
const productTextSchema = z.string().min(1).max(4_096)
const percentageSchema = z
  .string()
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?%$/)
const moneySchema = z
  .object({
    amount: z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
  })
  .strict()

const accountSchema = z
  .object({
    accountToken: accountTokenSchema,
    displayName: z.string().min(1).max(256),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
    holdings: z.number().int().nonnegative(),
    dataIssues: z.number().int().nonnegative(),
  })
  .strict()

const exactRangeSchema = z
  .object({
    label: z.string().min(1).max(160),
    lower: moneySchema,
    upper: moneySchema,
  })
  .strict()
  .superRefine((range, context) => {
    if (range.lower.currency !== range.upper.currency) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Risk range currencies do not match.",
      })
    }
  })

const recommendationSchema = z
  .object({
    action: z.enum(["buy", "add", "hold", "trim", "sell", "abstain"]),
    horizon: z.string().min(1).max(160),
    summary: productTextSchema,
    ranges: z.array(exactRangeSchema).max(8),
    reasons: z.array(productTextSchema).min(1).max(12),
    risks: z.array(productTextSchema).min(1).max(12),
    assumptions: z.array(productTextSchema).min(1).max(12),
    invalidators: z.array(productTextSchema).min(1).max(12),
    validity: z.discriminatedUnion("state", [
      z
        .object({ state: z.literal("available"), expiresAt: timestampSchema })
        .strict(),
      z
        .object({ state: z.literal("unavailable"), explanation: productTextSchema })
        .strict(),
    ]),
    uncertainty: z
      .object({
        level: z.enum(["low", "moderate", "high", "unavailable"]),
        explanation: productTextSchema,
        outOfSampleEvidence: z.enum(["sufficient", "limited", "unavailable"]),
        calibration: z.enum(["supported", "limited", "unavailable"]),
        tradingCosts: z.enum(["included", "partial", "unavailable"]),
        pointInTimeInputs: z.enum(["supported", "partial", "unavailable"]),
      })
      .strict(),
  })
  .strict()

const measureSchema = z
  .object({
    label: z.enum(["Value at risk", "Expected shortfall", "Annualized volatility"]),
    value: percentageSchema.nullable(),
    status: z.enum(["available", "insufficient_history", "unavailable"]),
    explanation: productTextSchema,
  })
  .strict()

const riskReportSchema = z
  .object({
    accountName: z.string().min(1).max(256),
    asOf: timestampSchema,
    availableAt: timestampSchema,
    horizon: z.string().min(1).max(160),
    coverage: z
      .object({
        state: z.enum(["complete", "partial", "unavailable"]),
        observations: z.number().int().nonnegative(),
        period: z.string().min(1).max(160),
        explanation: productTextSchema,
      })
      .strict(),
    measures: z.array(measureSchema).length(3),
    stress: z
      .object({
        label: z.string().min(1).max(160),
        impact: moneySchema.nullable(),
        status: z.enum(["available", "incomplete", "unavailable"]),
        explanation: productTextSchema,
        assumptions: z.array(productTextSchema).min(1).max(12),
      })
      .strict(),
    recommendation: recommendationSchema,
  })
  .strict()

export type PortfolioAccountRiskSummary = z.infer<typeof accountSchema>
export type PortfolioRiskReport = z.infer<typeof riskReportSchema>

export interface RiskResult<T> {
  value: T
  completeness: "complete" | "partial"
  returnedItems: number
  availableItems: number
}

export function parseRiskAccounts(
  result: ApplicationResult,
): RiskResult<PortfolioAccountRiskSummary[]> {
  const accounts = z.array(accountSchema).parse(result.data)
  if (
    result.metadata.returnedItems !== accounts.length ||
    result.metadata.returnedItems > result.metadata.availableItems
  ) {
    throw new Error("Portfolio counts do not match the returned portfolios.")
  }
  return boundary(result, accounts)
}

export function parseRiskReport(result: ApplicationResult): RiskResult<PortfolioRiskReport> {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("Risk guidance is incomplete.")
  }
  return boundary(result, riskReportSchema.parse(result.data))
}

function boundary<T>(result: ApplicationResult, value: T): RiskResult<T> {
  const completeness = z.enum(["complete", "partial"]).parse(result.metadata.completeness)
  return {
    value,
    completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}
