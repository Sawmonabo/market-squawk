import { z } from "zod"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const productTextSchema = z.string().trim().min(1).max(512)
const productNameSchema = z.string().trim().min(1).max(160)
const productSymbolSchema = z.string().trim().min(1).max(32)
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const productTimeSchema = z.string().datetime({ offset: true })

export const portfolioActionTokenSchema = z
  .string()
  .min(16)
  .max(192)
  .regex(/^[A-Za-z0-9_-]+$/)

export const moneySchema = z
  .object({ amount: exactDecimalSchema, currency: currencySchema })
  .strict()

export const percentageSchema = z
  .object({
    exact: exactDecimalSchema,
    display: z.string().trim().min(1).max(32),
  })
  .strict()

export const investmentDisplaySchema = z
  .object({
    name: productNameSchema,
    symbol: productSymbolSchema.nullable(),
    typeLabel: productNameSchema,
  })
  .strict()

const reviewStateSchema = z
  .object({
    tone: z.enum(["ready", "attention", "unavailable"]),
    label: productNameSchema,
    explanation: productTextSchema,
  })
  .strict()

export const portfolioAccountSchema = z
  .object({
    accountToken: portfolioActionTokenSchema,
    portfolioName: productNameSchema,
    accountName: productNameSchema,
    accountTypeLabel: productNameSchema,
    reportingCurrency: currencySchema,
    updatedAt: productTimeSchema,
    preparedAt: productTimeSchema.nullable(),
    currentValue: moneySchema.nullable(),
    cashBalance: moneySchema,
    returnSinceStart: percentageSchema.nullable(),
    positionCount: z.number().int().nonnegative(),
    transactionCount: z.number().int().nonnegative(),
    reviewFindingCount: z.number().int().nonnegative(),
    reviewState: reviewStateSchema,
  })
  .strict()

const costBasisChoiceSchema = z
  .object({
    choiceToken: portfolioActionTokenSchema,
    label: productNameSchema,
    amount: moneySchema.nullable(),
    explanation: productTextSchema,
  })
  .strict()

const costBasisSchema = z.discriminatedUnion("state", [
  z
    .object({
      state: z.literal("available"),
      amount: moneySchema,
      methodLabel: productNameSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("not_available"),
      explanation: productTextSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("needs_review"),
      explanation: productTextSchema,
      choices: z.array(costBasisChoiceSchema).max(24),
    })
    .strict(),
])

const priceSummarySchema = z
  .object({
    updatedAt: productTimeSchema.nullable(),
    label: productNameSchema,
    explanation: productTextSchema,
  })
  .strict()

export const holdingSchema = z
  .object({
    positionActionToken: portfolioActionTokenSchema,
    investment: investmentDisplaySchema,
    quantity: exactDecimalSchema,
    quantityLabel: productNameSchema,
    marketValue: moneySchema,
    price: priceSummarySchema,
    costBasis: costBasisSchema,
  })
  .strict()

export const portfolioTransactionSchema = z
  .object({
    transactionActionToken: portfolioActionTokenSchema,
    categoryLabel: productNameSchema,
    investment: investmentDisplaySchema.nullable(),
    amount: moneySchema,
    quantity: exactDecimalSchema.nullable(),
    quantityLabel: productNameSchema.nullable(),
    occurredAt: productTimeSchema,
  })
  .strict()

export const portfolioRevisionChoiceSchema = z
  .object({
    comparisonActionToken: portfolioActionTokenSchema,
    label: productNameSchema,
    effectiveAt: productTimeSchema,
    positionCount: z.number().int().nonnegative(),
  })
  .strict()

const contributionSchema = z
  .object({
    contributionActionToken: portfolioActionTokenSchema,
    investment: investmentDisplaySchema,
    amount: moneySchema,
  })
  .strict()

export const portfolioAttributionSchema = z
  .object({
    comparisonLabel: productNameSchema,
    comparisonPeriod: productTextSchema,
    totalChange: moneySchema,
    contributions: z.array(contributionSchema).max(500),
    explanation: productTextSchema,
    uncertainty: productTextSchema,
  })
  .strict()

const measuredAmountSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("available"), amount: moneySchema }).strict(),
  z.object({ state: z.literal("unavailable"), explanation: productTextSchema }).strict(),
])

const reconciliationDetailSchema = z
  .object({
    findingActionToken: portfolioActionTokenSchema,
    label: productNameSchema,
    supplied: moneySchema,
    calculated: moneySchema,
    tolerance: moneySchema,
    explanation: productTextSchema,
  })
  .strict()

const accountingSummarySchema = z
  .object({
    cash: moneySchema,
    cashUpdatedAt: productTimeSchema,
    reportedMarketValue: moneySchema,
    unrealizedGain: measuredAmountSchema,
    realizedGain: measuredAmountSchema,
    income: measuredAmountSchema,
    fees: measuredAmountSchema,
    reconciliationLabel: productNameSchema,
    reconciliationExplanation: productTextSchema,
    reconciliationFindings: z.array(reconciliationDetailSchema).max(100),
  })
  .strict()

export const performanceSchema = z
  .object({
    currentValue: moneySchema,
    timeWeightedReturn: percentageSchema.nullable(),
    moneyWeightedReturn: percentageSchema.nullable(),
    comparablePeriods: z.number().int().nonnegative().nullable(),
    coverageExplanation: productTextSchema,
    accounting: accountingSummarySchema.nullable(),
  })
  .strict()

const exposureRowSchema = z
  .object({ label: productNameSchema, amount: moneySchema })
  .strict()

export const exposureSchema = z
  .object({
    byInvestment: z.array(exposureRowSchema).max(500),
    byCurrency: z.array(exposureRowSchema).max(64),
    bySector: z.array(exposureRowSchema).max(128),
    byFactor: z.array(exposureRowSchema).max(128),
    net: moneySchema.nullable(),
    gross: moneySchema.nullable(),
    coverageExplanation: productTextSchema,
  })
  .strict()

const riskMetricSchema = z
  .object({
    label: productNameSchema,
    value: z.string().trim().min(1).max(64),
    explanation: productTextSchema,
  })
  .strict()

const riskStressSummarySchema = z
  .object({
    title: productNameSchema,
    assumption: productTextSchema,
    impact: moneySchema.nullable(),
    result: productTextSchema,
    uncertainty: productTextSchema,
  })
  .strict()

export const riskSchema = z
  .object({
    metrics: z.array(riskMetricSchema).min(1).max(12),
    stress: riskStressSummarySchema.nullable(),
    coverageExplanation: productTextSchema,
  })
  .strict()

const preparedDecisionSchema = z
  .object({
    actionToken: portfolioActionTokenSchema,
    title: productNameSchema,
    action: productNameSchema,
    horizon: productNameSchema,
    range: productTextSchema,
    reasons: z.array(productTextSchema).min(1).max(8),
    risks: z.array(productTextSchema).min(1).max(8),
    assumptions: z.array(productTextSchema).min(1).max(8),
    expiresAt: productTimeSchema,
    invalidators: z.array(productTextSchema).min(1).max(8),
    uncertainty: productTextSchema,
  })
  .strict()

export const stressChoiceSchema = preparedDecisionSchema
  .extend({ result: productTextSchema, estimatedImpact: moneySchema.nullable() })
  .strict()

export const positionChoiceSchema = preparedDecisionSchema
  .extend({ investment: investmentDisplaySchema })
  .strict()

export const rebalanceChoiceSchema = preparedDecisionSchema
  .extend({
    estimatedTurnover: percentageSchema.nullable(),
    estimatedCosts: moneySchema.nullable(),
  })
  .strict()

const importPositionChoiceSchema = z
  .object({
    choiceToken: portfolioActionTokenSchema,
    label: productNameSchema,
    explanation: productTextSchema,
  })
  .strict()

const portfolioImportInterpretationChoiceSchema = z
  .object({
    choiceToken: portfolioActionTokenSchema,
    label: productNameSchema,
    explanation: productTextSchema,
    positionChoices: z.array(importPositionChoiceSchema).max(100),
    positionSelectionRequired: z.boolean(),
  })
  .strict()

const portfolioImportTransactionSchema = z
  .object({
    transactionActionToken: portfolioActionTokenSchema,
    categoryLabel: productNameSchema,
    investment: investmentDisplaySchema.nullable(),
    amount: moneySchema,
    quantityLabel: productNameSchema.nullable(),
    occurredAt: productTimeSchema,
    interpretationRequired: z.boolean(),
    interpretationChoices: z.array(portfolioImportInterpretationChoiceSchema).max(24),
  })
  .strict()

const portfolioImportPreviewSchema = z
  .object({
    reviewActionToken: portfolioActionTokenSchema,
    accountToken: portfolioActionTokenSchema,
    portfolioName: productNameSchema,
    stateLabel: productNameSchema,
    recordCount: z.number().int().nonnegative(),
    transactionCount: z.number().int().nonnegative(),
    reviewFindingCount: z.number().int().nonnegative(),
    transactions: z.array(portfolioImportTransactionSchema).max(5_000),
    saveAllowed: z.boolean(),
    saveExplanation: productTextSchema,
  })
  .strict()

const portfolioImportCommitSchema = z
  .object({
    accepted: z.literal(true),
    portfolioName: productNameSchema,
    message: productTextSchema,
  })
  .strict()

export type PortfolioAccount = z.infer<typeof portfolioAccountSchema>
export type PortfolioHolding = z.infer<typeof holdingSchema>
export type PortfolioTransaction = z.infer<typeof portfolioTransactionSchema>
export type PortfolioRevisionChoice = z.infer<typeof portfolioRevisionChoiceSchema>
export type PortfolioPerformance = z.infer<typeof performanceSchema>
export type PortfolioExposure = z.infer<typeof exposureSchema>
export type PortfolioRisk = z.infer<typeof riskSchema>
export type PortfolioAttribution = z.infer<typeof portfolioAttributionSchema>
export type PortfolioStressChoice = z.infer<typeof stressChoiceSchema>
export type PortfolioPositionChoice = z.infer<typeof positionChoiceSchema>
export type PortfolioRebalanceChoice = z.infer<typeof rebalanceChoiceSchema>
export type PortfolioImportPreview = z.infer<typeof portfolioImportPreviewSchema>
export type PortfolioImportTransaction = z.infer<typeof portfolioImportTransactionSchema>
export type PortfolioImportCommit = z.infer<typeof portfolioImportCommitSchema>
export type Money = z.infer<typeof moneySchema>

export function parsePortfolioImportPreview(value: unknown): PortfolioImportPreview {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportPreviewSchema })
    .safeParse(value)
  if (!parsed.success || parsed.data.metadata.completeness !== "complete") {
    throw new Error("The import review could not be displayed safely.")
  }
  return parsed.data.data
}

export function parsePortfolioImportCommit(value: unknown): PortfolioImportCommit {
  const parsed = applicationResultSchema
    .extend({ data: portfolioImportCommitSchema })
    .safeParse(value)
  if (!parsed.success || parsed.data.metadata.completeness !== "complete") {
    throw new Error("The account update could not be confirmed safely.")
  }
  return parsed.data.data
}

export function parsePortfolioResult<Schema extends z.ZodType>(
  result: ApplicationResult,
  schema: Schema,
  emptyValue?: z.input<Schema>,
): z.infer<Schema> {
  if (
    result.metadata.completeness !== "complete" ||
    result.metadata.returnedItems !== result.metadata.availableItems
  ) {
    throw new Error("Portfolio information is incomplete.")
  }
  const parsed = schema.safeParse(
    result.data === null && emptyValue !== undefined ? emptyValue : result.data,
  )
  if (!parsed.success) {
    throw new Error("Portfolio information could not be displayed safely.")
  }
  return parsed.data
}
