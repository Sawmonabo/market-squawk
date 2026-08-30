import { z } from "zod"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const identitySchema = z.string().min(1)
const tokenSchema = z.string().uuid()
const timeSchema = z.string().regex(/^-?\d+$/)
const confidenceSchema = z.enum(["limited", "moderate", "strong"])

export const moneySchema = z.object({
  amount: exactDecimalSchema,
  currency: z.string().regex(/^[A-Z]{3}$/),
}).strict()

export const portfolioSnapshotSchema = z.object({
  snapshotToken: tokenSchema,
  effectiveAtUnixNanos: timeSchema,
  availableAtUnixNanos: timeSchema.nullable(),
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  dataIssueCount: z.number().int().nonnegative(),
  dataState: z.enum(["ready", "needs_review"]),
}).strict()

export const portfolioRevisionSchema = portfolioSnapshotSchema

export const portfolioAccountSchema = z.object({
  accountId: identitySchema,
  currency: z.string().regex(/^[A-Z]{3}$/),
  cashBalance: moneySchema,
  currentSnapshot: portfolioSnapshotSchema,
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  reconciliationDiscrepancies: z.number().int().nonnegative(),
}).strict()

const lotMethodSchema = z.enum([
  "First in, first out",
  "Last in, first out",
  "Average cost",
  "Specific lots",
])

const costBasisSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"),
    amount: moneySchema,
    method: lotMethodSchema,
  }).strict(),
  z.object({ state: z.literal("not_available") }).strict(),
  z.object({
    state: z.literal("needs_review"),
    choices: z.array(moneySchema),
    method: lotMethodSchema,
  }).strict(),
])

const priceStateSchema = z.object({
  asOfUnixNanos: timeSchema,
  state: z.enum(["reported", "current", "stale", "not_available"]),
  confidence: confidenceSchema,
  explanation: z.string().min(1),
}).strict()

export const holdingSchema = z.object({
  accountId: identitySchema,
  snapshotToken: tokenSchema,
  instrumentId: identitySchema,
  currency: z.string().regex(/^[A-Z]{3}$/),
  quantity: exactDecimalSchema,
  lotSize: exactDecimalSchema,
  marketValue: moneySchema,
  asOfUnixNanos: timeSchema,
  costBasis: costBasisSchema,
  price: priceStateSchema,
}).strict()

export const portfolioTransactionSchema = z.object({
  transactionToken: tokenSchema,
  accountId: identitySchema,
  snapshotToken: tokenSchema,
  instrumentId: identitySchema.nullable(),
  category: z.enum(["trade", "cash_transfer", "income", "fee", "corporate_action"]),
  amount: moneySchema,
  quantity: exactDecimalSchema.nullable(),
  occurredAtUnixNanos: timeSchema,
  lotMethod: lotMethodSchema.nullable(),
}).strict()

const reportBase = z.object({
  accountId: identitySchema,
  snapshotToken: tokenSchema,
  effectiveAtUnixNanos: timeSchema,
  availableAtUnixNanos: timeSchema.nullable(),
  dataConfidence: confidenceSchema,
}).strict()

const contributionSchema = z.object({
  instrumentId: identitySchema,
  amount: moneySchema,
}).strict()

const evaluatedScenarioSchema = z.object({
  id: identitySchema,
  composition: z.enum(["additive", "compounded"]),
  contributions: z.array(contributionSchema),
  total: moneySchema,
}).strict()

export const portfolioAttributionSchema = reportBase.extend({
  baselineSnapshotToken: tokenSchema,
  baselineEffectiveAtUnixNanos: timeSchema,
  baselineAvailableAtUnixNanos: timeSchema.nullable(),
  contributions: z.array(contributionSchema),
  total: moneySchema,
  explanation: z.string().min(1),
}).strict()

export const portfolioScenarioResultSchema = reportBase.extend({
  scenario: evaluatedScenarioSchema,
}).strict()

export const portfolioScenarioBatchResultSchema = reportBase.extend({
  scenarios: z.array(evaluatedScenarioSchema),
}).strict()

export const portfolioRebalanceSchema = reportBase.extend({
  trades: z.array(z.object({
    instrumentId: identitySchema,
    valueChange: moneySchema,
  }).strict()),
  projectedCash: moneySchema,
  turnover: exactDecimalSchema,
  constrained: z.boolean(),
}).strict()

const candidateCostSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("available"), amount: moneySchema }).strict(),
  z.object({ state: z.literal("not_available") }).strict(),
])

export const portfolioCandidateImpactSchema = z.object({
  accountId: identitySchema,
  instrumentId: identitySchema,
  positionState: z.enum(["new", "existing"]),
  currentQuantity: exactDecimalSchema,
  proposedQuantity: exactDecimalSchema,
  currentMarketValue: moneySchema,
  proposedMarketValue: moneySchema,
  capitalChange: moneySchema,
  portfolioValue: moneySchema,
  instrumentTerms: z.object({
    priceTick: exactDecimalSchema,
    lotSize: exactDecimalSchema,
    quoteCurrency: z.string().regex(/^[A-Z]{3}$/),
    contractMultiplier: exactDecimalSchema,
  }).strict(),
  costs: z.object({
    fees: candidateCostSchema,
    slippage: candidateCostSchema,
  }).strict(),
  concentration: z.object({
    current: exactDecimalSchema,
    proposed: exactDecimalSchema,
    change: exactDecimalSchema,
  }).strict(),
  scenario: z.object({
    shock: exactDecimalSchema,
    currentImpact: moneySchema,
    proposedImpact: moneySchema,
    marginalImpact: moneySchema,
  }).strict(),
  price: z.object({
    amount: moneySchema,
    asOfUnixNanos: timeSchema,
    state: z.literal("current"),
    method: z.enum(["Last trade", "Bid-ask midpoint"]),
    confidence: confidenceSchema,
  }).strict(),
  missingInformation: z.array(z.string().min(1)),
  riskAssessment: z.object({
    state: z.literal("incomplete"),
    evaluatedAtUnixNanos: timeSchema,
    checksCompleted: z.number().int().nonnegative(),
    checksUnavailable: z.number().int().nonnegative(),
  }).strict(),
  updatedAtUnixNanos: timeSchema,
  analysisOnly: z.literal(true),
}).strict()

const reconciliationDetailSchema = z.object({
  field: z.enum(["cash", "market_value", "cost_basis"]),
  supplied: moneySchema,
  calculated: moneySchema,
  currency: z.string().regex(/^[A-Z]{3}$/),
  tolerance: z.object({
    kind: z.literal("absolute"),
    amount: moneySchema,
  }).strict(),
}).strict()

const measuredAccountingSchema = z.object({
  status: z.enum(["available", "partial", "not_available"]),
  amount: moneySchema.optional(),
}).strict()

const accountingEvidenceSchema = z.object({
  cash: z.object({
    amount: moneySchema,
    observedAtUnixNanos: timeSchema,
    status: z.literal("available"),
  }).strict(),
  reportedMarketValue: moneySchema,
  unrealizedGain: measuredAccountingSchema,
  realizedGain: measuredAccountingSchema,
  income: measuredAccountingSchema,
  fees: measuredAccountingSchema,
  reconciliation: z.object({
    status: z.enum(["clear", "needs_review"]),
    discrepancies: z.array(reconciliationDetailSchema),
  }).strict(),
}).strict()

export const performanceSchema = reportBase.extend({
  currentValue: moneySchema,
  historyStatus: z.string().min(1).optional(),
  timeWeightedReturn: exactDecimalSchema.optional(),
  moneyWeightedReturn: exactDecimalSchema.optional(),
  periods: z.number().int().nonnegative().optional(),
  accountingEvidence: accountingEvidenceSchema.optional(),
}).strict()

const exposureRowSchema = z.object({ amount: moneySchema }).strict()

export const exposureSchema = reportBase.extend({
  instrument: z.array(exposureRowSchema.extend({ instrumentId: identitySchema }).strict()),
  currency: z.array(exposureRowSchema.extend({
    currency: z.string().regex(/^[A-Z]{3}$/),
  }).strict()),
  sector: z.array(exposureRowSchema.extend({ classification: identitySchema }).strict()),
  factor: z.array(exposureRowSchema.extend({ classification: identitySchema }).strict()),
  net: moneySchema.optional(),
  gross: moneySchema.optional(),
  calculationStatus: z.string().min(1).optional(),
  classificationStatus: z.string().min(1).optional(),
}).strict()

const riskScenarioSchema = z.object({
  id: identitySchema,
  status: z.string().min(1).optional(),
  impact: moneySchema.optional(),
}).strict()

export const riskSchema = reportBase.extend({
  confidence: z.number().min(0).max(1),
  scenario: riskScenarioSchema,
  historyStatus: z.string().min(1).optional(),
  valueAtRisk: z.number().nonnegative().optional(),
  expectedShortfall: z.number().nonnegative().optional(),
  observations: z.number().int().nonnegative().optional(),
  annualizedVolatility: z.number().nonnegative().optional(),
  volatilityStatus: z.string().min(1).optional(),
  trackingErrorStatus: z.string().min(1).optional(),
}).strict()

export interface ResultEvidence {
  state: "complete" | "partial"
  returnedItems: number
  availableItems: number
  confidence: "limited" | "moderate" | "strong"
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
export type PortfolioScenarioBatchResult = z.infer<typeof portfolioScenarioBatchResultSchema>
export type PortfolioRebalance = z.infer<typeof portfolioRebalanceSchema>
export type PortfolioCandidateImpact = z.infer<typeof portfolioCandidateImpactSchema>
export type Money = z.infer<typeof moneySchema>

const portfolioImportTransactionSchema = z.object({
  recordToken: tokenSchema,
  category: z.enum(["trade", "cash_transfer", "income", "fee", "corporate_action"]),
  amount: moneySchema,
  quantity: exactDecimalSchema.nullable(),
  occurredAtUnixNanos: timeSchema,
  interpretationOptions: z.array(z.object({
    value: identitySchema,
    label: identitySchema,
    requiresLotSelection: z.boolean(),
  }).strict()),
  eligibleLotCount: z.number().int().nonnegative(),
}).strict()

const portfolioImportPreviewSchema = z.object({
  reviewToken: tokenSchema,
  accountId: identitySchema,
  state: z.enum(["ready", "already_saved"]),
  recordCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  dataIssueCount: z.number().int().nonnegative(),
  transactions: z.array(portfolioImportTransactionSchema),
  requiresCorporateActionReview: z.boolean(),
}).strict()

const portfolioImportCommitSchema = z.object({ accepted: z.literal(true) }).strict()

export type PortfolioImportPreview = z.infer<typeof portfolioImportPreviewSchema>
export type PortfolioImportTransaction = z.infer<typeof portfolioImportTransactionSchema>
export type PortfolioImportCommit = z.infer<typeof portfolioImportCommitSchema>

export function parsePortfolioImportPreview(value: unknown): PortfolioImportPreview {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportPreviewSchema })
    .safeParse(value)
  if (!parsed.success) {
    throw new Error("The import review could not be displayed safely.")
  }
  return parsed.data.data
}

export function parsePortfolioImportCommit(value: unknown): PortfolioImportCommit {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportCommitSchema })
    .safeParse(value)
  if (!parsed.success) {
    throw new Error("The account update could not be confirmed safely.")
  }
  return parsed.data.data
}

export function parsePortfolioResult<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
  emptyValue?: z.input<Schema>,
): PortfolioResult<z.infer<Schema>> {
  const parsed = schema.safeParse(
    result.data === null && emptyValue !== undefined ? emptyValue : result.data,
  )
  if (!parsed.success) {
    throw new Error("Portfolio information could not be displayed safely.")
  }
  const quality = z.object({ confidence: confidenceSchema }).passthrough()
    .safeParse(result.metadata.dataQuality)
  return {
    value: parsed.data,
    evidence: {
      state: result.metadata.completeness === "complete" ? "complete" : "partial",
      returnedItems: result.metadata.returnedItems,
      availableItems: result.metadata.availableItems,
      confidence: quality.success ? quality.data.confidence : "limited",
    },
  }
}
