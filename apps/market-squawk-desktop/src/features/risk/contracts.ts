import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const moneySchema = z.object({
  amount: z.string(),
  currency: z.string().min(1),
})

const revisionSchema = z
  .object({
    revisionId: z.string().min(1),
    effectiveAtUnixNanos: z.string(),
    availableAtUnixNanos: z.string().nullable(),
    sourceId: z.string().min(1),
    sourceCoverage: z.array(z.string()),
    artifactSha256: z.string().min(1),
    reconciliationDiscrepancies: z.number().int().nonnegative(),
  })
  .loose()

const accountSchema = z
  .object({
    accountId: z.string().min(1),
    currency: z.string().min(1),
    currentRevision: revisionSchema,
    holdingCount: z.number().int().nonnegative(),
    transactionCount: z.number().int().nonnegative(),
    reconciliationDiscrepancies: z.number().int().nonnegative(),
  })
  .loose()

const scenarioSchema = z
  .object({
    id: z.string().min(1),
    status: z.string().min(1).optional(),
    impact: moneySchema.optional(),
  })
  .loose()

const riskReportSchema = z
  .object({
    accountId: z.string().min(1),
    revisionId: z.string().min(1),
    policy: z.string().min(1),
    confidence: z.number().min(0).max(1),
    scenario: scenarioSchema,
    effectiveAtUnixNanos: z.string().optional(),
    availableAtUnixNanos: z.string().nullable().optional(),
    valueAtRisk: z.number().nonnegative().optional(),
    expectedShortfall: z.number().nonnegative().optional(),
    annualizedVolatility: z.number().nonnegative().optional(),
    observations: z.number().int().nonnegative().optional(),
    historyStatus: z.string().min(1).optional(),
    volatilityStatus: z.string().min(1).optional(),
    trackingErrorStatus: z.string().min(1).optional(),
  })
  .loose()

export type PortfolioAccountRiskSummary = z.infer<typeof accountSchema>
export type PortfolioRiskReport = z.infer<typeof riskReportSchema>

export interface RiskResult<T> {
  value: T
  completeness: string
  returnedItems: number
  availableItems: number
}

export function parseRiskAccounts(
  result: ApplicationResult,
): RiskResult<PortfolioAccountRiskSummary[]> {
  const value = result.data === null ? [] : z.array(accountSchema).parse(result.data)
  return boundary(result, value)
}

export function parseRiskReport(
  result: ApplicationResult,
): RiskResult<PortfolioRiskReport> {
  return boundary(result, riskReportSchema.parse(result.data))
}

function boundary<T>(result: ApplicationResult, value: T): RiskResult<T> {
  return {
    value,
    completeness: result.metadata.completeness,
    returnedItems: result.metadata.returnedItems,
    availableItems: result.metadata.availableItems,
  }
}
