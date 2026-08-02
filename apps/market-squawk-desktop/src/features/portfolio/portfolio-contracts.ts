import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"

import type { ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const digestSchema = z.string().regex(/^[0-9a-f]{64}$/)

export const moneySchema = z.object({
  amount: exactDecimalSchema,
  currency: z.string().min(1),
})

export const portfolioRevisionSchema = z.object({
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  sourceCoverage: z.array(z.string()),
  artifactSha256: digestSchema,
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  reconciliationDiscrepancies: z.number().int().nonnegative(),
})

export const portfolioAccountSchema = z.object({
  accountId: z.string().min(1),
  currency: z.string().min(1),
  currentRevision: portfolioRevisionSchema,
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  reconciliationDiscrepancies: z.number().int().nonnegative(),
})

const basisSchema = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("resolved"),
    observation: z.object({
      amount: moneySchema,
      lot_method: z.string(),
      source_reference: z.string(),
    }).loose(),
  }),
  z.object({ status: z.literal("missing") }),
  z.object({
    status: z.literal("ambiguous"),
    candidates: z.array(moneySchema),
    lot_method: z.string(),
  }),
])

const holdingMarkEvidenceSchema = z.object({
  sourceReference: z.string().min(1),
  observedAtUnixNanos: losslessIntegerSchema,
  venue: z.string().min(1).nullable(),
  venueStatus: z.string().min(1),
  state: z.string().min(1),
  quality: z.string().min(1),
  executionEligible: z.boolean(),
  freshness: z.object({
    status: z.string().min(1),
    reason: z.string().min(1),
  }),
  fallback: z.object({
    status: z.string().min(1),
    reason: z.string().min(1),
  }),
})

export const holdingSchema = z.object({
  account_id: z.string().min(1),
  instrument_id: z.string().min(1),
  currency: z.string().min(1),
  quantity: exactDecimalSchema,
  lot_size: exactDecimalSchema,
  market_value: moneySchema,
  as_of: losslessIntegerSchema,
  basis: basisSchema,
  source_reference: z.string().min(1),
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  artifactSha256: digestSchema,
  markEvidence: holdingMarkEvidenceSchema,
})

export const portfolioTransactionSchema = z.object({
  broker_transaction_id: z.string().min(1),
  account_id: z.string().min(1),
  instrument_id: z.string().min(1).nullable(),
  kind: z.enum(["trade", "cash_transfer", "income", "fee", "corporate_action"]),
  amount: moneySchema,
  quantity: exactDecimalSchema.nullable(),
  occurred_at: losslessIntegerSchema,
  lot_method: z.string().nullable(),
  source_reference: z.string().min(1),
  revisionId: digestSchema,
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
  sourceId: z.string().min(1),
  artifactSha256: digestSchema,
})

const reportBase = z.object({
  accountId: z.string().min(1),
  revisionId: digestSchema,
  policy: z.string().min(1),
  effectiveAtUnixNanos: z.string(),
  availableAtUnixNanos: z.string().nullable(),
})

const markEvidenceSchema = z.object({
  sourceId: z.string().min(1),
  sourceCoverage: z.array(z.string()),
  artifactSha256: digestSchema,
  quality: z.string().min(1),
  executionEligible: z.boolean(),
})

const advancedReportBase = reportBase.extend({
  markEvidence: markEvidenceSchema,
})

const contributionSchema = z.object({
  instrumentId: z.string().min(1),
  amount: moneySchema,
})

const evaluatedScenarioSchema = z.object({
  id: z.string().min(1),
  composition: z.enum(["additive", "compounded"]),
  contributions: z.array(contributionSchema),
  total: moneySchema,
})

export const portfolioAttributionSchema = advancedReportBase.extend({
  baselineRevisionId: digestSchema,
  baselineEffectiveAtUnixNanos: z.string(),
  baselineAvailableAtUnixNanos: z.string().nullable(),
  contributions: z.array(contributionSchema),
  total: moneySchema,
  methodDisclosure: z.literal(
    "source_mark_change_without_cash_flow_or_corporate_action_adjustment",
  ),
})

export const portfolioScenarioResultSchema = advancedReportBase.extend({
  scenario: evaluatedScenarioSchema,
})

export const portfolioScenarioBatchResultSchema = advancedReportBase.extend({
  scenarios: z.array(evaluatedScenarioSchema),
})

export const portfolioRebalanceSchema = advancedReportBase.extend({
  trades: z.array(
    z.object({
      instrumentId: z.string().min(1),
      valueChange: moneySchema,
    }),
  ),
  projectedCash: moneySchema,
  turnover: exactDecimalSchema,
  constrained: z.boolean(),
  authority: z.object({
    proposalOnly: z.literal(true),
    executionAuthority: z.literal(false),
    riskApprovalRequiredBeforeAnyOrder: z.literal(true),
  }),
})

export const portfolioCandidateImpactSchema = advancedReportBase.extend({
  instrumentId: z.string().min(1),
  currentMarketValue: moneySchema,
  proposedMarketValue: moneySchema,
  projectedCash: moneySchema,
  concentration: z.object({
    current: exactDecimalSchema,
    proposed: exactDecimalSchema,
    change: exactDecimalSchema,
  }),
  scenario: z.object({
    shock: exactDecimalSchema,
    currentImpact: moneySchema,
    proposedImpact: moneySchema,
    marginalImpact: moneySchema,
  }),
  unavailable: z.array(z.string()),
  authority: z.object({
    analysisOnly: z.literal(true),
    executionAuthority: z.literal(false),
    riskApprovalRequiredBeforeAnyOrder: z.literal(true),
  }),
})

const reconciliationDetailSchema = z.object({
  field: z.enum(["cash", "market_value", "cost_basis"]),
  supplied: moneySchema,
  calculated: moneySchema,
  currency: z.string().min(1),
  tolerance: z.object({
    kind: z.literal("absolute"),
    amount: moneySchema,
  }),
  sourceReference: z.string().min(1),
})

const measuredAccountingSchema = z.object({
  status: z.string().min(1),
  amount: moneySchema.optional(),
  reason: z.string().min(1).optional(),
})

const accountingEvidenceSchema = z.object({
  cash: z.object({
    amount: moneySchema,
    observedAtUnixNanos: losslessIntegerSchema,
    sourceReference: z.string().min(1),
    status: z.literal("source_reported_snapshot"),
  }),
  reportedMarketValue: moneySchema,
  unrealizedGain: measuredAccountingSchema,
  realizedGain: measuredAccountingSchema,
  income: measuredAccountingSchema,
  fees: measuredAccountingSchema,
  reconciliation: z.object({
    status: z.string().min(1),
    discrepancies: z.array(reconciliationDetailSchema),
  }),
})

export const performanceSchema = reportBase.extend({
  currentValue: moneySchema,
  historyStatus: z.string().optional(),
  timeWeightedReturn: exactDecimalSchema.optional(),
  moneyWeightedReturn: exactDecimalSchema.optional(),
  periods: z.number().int().nonnegative().optional(),
  analyticsEvidenceDigest: digestSchema.optional(),
  accountingEvidence: accountingEvidenceSchema.optional(),
})

const exposureRowSchema = z.object({
  amount: moneySchema,
})

export const exposureSchema = reportBase.extend({
  instrument: z.array(
    exposureRowSchema.extend({ instrumentId: z.string().min(1) }),
  ),
  currency: z.array(
    exposureRowSchema.extend({ currency: z.string().min(1) }),
  ),
  sector: z.array(
    exposureRowSchema.extend({ classification: z.string().min(1) }),
  ),
  factor: z.array(
    exposureRowSchema.extend({ classification: z.string().min(1) }),
  ),
  net: moneySchema.optional(),
  gross: moneySchema.optional(),
  calculationStatus: z.string().optional(),
  classificationStatus: z.string().optional(),
})

const scenarioSchema = z.object({
  id: z.string().min(1),
  status: z.string().optional(),
  impact: moneySchema.optional(),
})

export const riskSchema = reportBase.extend({
  confidence: z.number().min(0).max(1),
  scenario: scenarioSchema,
  historyStatus: z.string().optional(),
  valueAtRisk: z.number().nonnegative().optional(),
  expectedShortfall: z.number().nonnegative().optional(),
  observations: z.number().int().nonnegative().optional(),
  annualizedVolatility: z.number().nonnegative().optional(),
  volatilityStatus: z.string().optional(),
  trackingErrorStatus: z.string().optional(),
})

const qualitySchema = z
  .object({
    class: z.string().optional(),
    executionEligible: z.boolean().optional(),
    reconciliationDiscrepancies: z.number().int().nonnegative().optional(),
    rawEvidenceRetained: z.boolean().optional(),
  })
  .loose()

export interface ResultEvidence {
  completeness: string
  returnedItems: number
  availableItems: number
  sourceCoverage: unknown
  dataQuality: z.infer<typeof qualitySchema> | null
}

export interface PortfolioResult<T> {
  value: T
  evidence: ResultEvidence
}

export type PortfolioAccount = z.infer<typeof portfolioAccountSchema>
export type PortfolioRevision = z.infer<typeof portfolioRevisionSchema>
export type PortfolioHolding = z.infer<typeof holdingSchema>
export type PortfolioTransaction = z.infer<typeof portfolioTransactionSchema>
export type PortfolioPerformance = z.infer<typeof performanceSchema>
export type PortfolioExposure = z.infer<typeof exposureSchema>
export type PortfolioRisk = z.infer<typeof riskSchema>
export type PortfolioAttribution = z.infer<typeof portfolioAttributionSchema>
export type PortfolioScenarioResult = z.infer<typeof portfolioScenarioResultSchema>
export type PortfolioScenarioBatchResult = z.infer<
  typeof portfolioScenarioBatchResultSchema
>
export type PortfolioRebalance = z.infer<typeof portfolioRebalanceSchema>
export type PortfolioCandidateImpact = z.infer<typeof portfolioCandidateImpactSchema>
export type Money = z.infer<typeof moneySchema>

export function parsePortfolioResult<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
): PortfolioResult<z.infer<Schema>> {
  const parsed = schema.safeParse(result.data)
  if (!parsed.success) {
    throw new Error(
      "The installed service returned portfolio data this dashboard cannot safely interpret.",
    )
  }
  const quality = qualitySchema.safeParse(result.metadata.dataQuality)
  return {
    value: parsed.data,
    evidence: {
      completeness: result.metadata.completeness,
      returnedItems: result.metadata.returnedItems,
      availableItems: result.metadata.availableItems,
      sourceCoverage: result.metadata.sourceCoverage,
      dataQuality: quality.success ? quality.data : null,
    },
  }
}
