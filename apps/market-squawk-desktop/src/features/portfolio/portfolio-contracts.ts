import { z } from "zod"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const exactDecimalSchema = z.string().regex(/^-?\d+(?:\.\d+)?$/)
const productTextSchema = z.string().trim().min(1).max(512)
const productNameSchema = z.string().trim().min(1).max(160)
const productSymbolSchema = z.string().trim().min(1).max(32)
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const productTimeSchema = z.string().datetime({ offset: true })
const unixNanosSchema = z.string().regex(/^-?\d+$/)

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

export const portfolioAccountSummarySchema = z.strictObject({
  accountToken: z.string().min(16).max(512).refine((value) => !/^[0-9a-f]{8}-[0-9a-f-]{27}$/i.test(value), "Expected an opaque account token."),
  displayName: z.string().min(1).max(256), currency: z.string().regex(/^[A-Z]{3,8}$/),
  holdings: z.number().int().nonnegative(), dataIssues: z.number().int().nonnegative(),
})
export type PortfolioAccountSummary = z.infer<typeof portfolioAccountSummarySchema>
const portfolioAccountPageSchema = z.strictObject({
  accounts: z.array(portfolioAccountSummarySchema).max(100), nextCursor: z.string().min(1).max(512).nullable(),
})
export function parsePortfolioAccountPage(result: ApplicationResult) {
  const page = portfolioAccountPageSchema.parse(result.data)
  if (result.metadata.returnedItems !== page.accounts.length) throw new Error("Portfolio account counts are inconsistent.")
  return page
}

const lotMethodSchema = z.enum([
  "First in, first out", "Last in, first out", "Average cost", "Specific lots",
])
const costBasisSchema = z.discriminatedUnion("state", [
  z.strictObject({ state: z.literal("available"), amount: moneySchema, method: lotMethodSchema }),
  z.strictObject({ state: z.literal("not_available") }),
  z.strictObject({ state: z.literal("needs_review"), choices: z.array(moneySchema), method: lotMethodSchema }),
])

export const holdingSchema = z.strictObject({
  accountId: z.string().min(1),
  snapshotToken: z.string().uuid(),
  instrumentId: z.string().min(1),
  currency: currencySchema,
  quantity: exactDecimalSchema,
  lotSize: exactDecimalSchema,
  marketValue: moneySchema,
  asOfUnixNanos: unixNanosSchema,
  costBasis: costBasisSchema,
  price: z.strictObject({
    asOfUnixNanos: unixNanosSchema,
    state: z.enum(["reported", "current", "stale", "not_available"]),
    confidence: z.enum(["limited", "moderate", "strong"]),
    explanation: z.string(),
  }),
  investment: z.strictObject({ name: z.string().nullable(), symbol: z.string().nullable() }),
})

export const portfolioHoldingsPageSchema = z.strictObject({
  holdings: z.array(holdingSchema),
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
}).superRefine((page, context) => {
  const instruments = new Set<string>()
  for (const holding of page.holdings) {
    if (holding.snapshotToken !== page.snapshotToken
      || holding.marketValue.currency !== holding.currency
      || instruments.has(holding.instrumentId)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Position evidence is inconsistent." })
    }
    instruments.add(holding.instrumentId)
  }
})

export function parsePortfolioHoldings(result: ApplicationResult): PortfolioHoldingsPage {
  const page = parsePortfolioResult(result, portfolioHoldingsPageSchema)
  if (result.metadata.returnedItems !== page.holdings.length) {
    throw new Error("Position counts are inconsistent.")
  }
  return page
}

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

// The performance read uses the canonical Portfolio.GetPerformance projection.
// Keep its exact monetary values and optional history evidence intact.
const measuredAccountingSchema = z.strictObject({
  status: z.enum(["available", "partial", "not_available"]),
  amount: moneySchema.optional(),
}).superRefine((measure, context) => {
  if (measure.status !== "not_available" && measure.amount === undefined) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "An available amount is missing." })
  }
})

const reconciliationDetailSchema = z.strictObject({
  field: z.enum(["cash", "market_value", "cost_basis"]),
  supplied: moneySchema,
  calculated: moneySchema,
  currency: currencySchema,
  tolerance: z.strictObject({ kind: z.literal("absolute"), amount: moneySchema }),
})

const accountingEvidenceSchema = z.strictObject({
  cash: z.strictObject({
    amount: moneySchema,
    observedAtUnixNanos: unixNanosSchema,
    status: z.literal("available"),
  }),
  reportedMarketValue: moneySchema,
  unrealizedGain: measuredAccountingSchema,
  realizedGain: measuredAccountingSchema,
  income: measuredAccountingSchema,
  fees: measuredAccountingSchema,
  reconciliation: z.strictObject({
    status: z.enum(["clear", "needs_review"]),
    discrepancies: z.array(reconciliationDetailSchema),
  }),
})

export const performanceSchema = z.strictObject({
  accountId: z.string().min(1),
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
  dataConfidence: z.enum(["limited", "moderate", "strong"]),
  currentValue: moneySchema,
  historyStatus: z.enum(["insufficient_history", "insufficient_comparable_history"]).optional(),
  timeWeightedReturn: exactDecimalSchema.optional(),
  moneyWeightedReturn: exactDecimalSchema.optional(),
  periods: z.number().int().positive().optional(),
  accountingEvidence: accountingEvidenceSchema,
}).superRefine((performance, context) => {
  const hasReturns = performance.timeWeightedReturn !== undefined
    && performance.moneyWeightedReturn !== undefined
    && performance.periods !== undefined
  const noReturns = performance.timeWeightedReturn === undefined
    && performance.moneyWeightedReturn === undefined
    && performance.periods === undefined
  if (!(hasReturns && performance.historyStatus === undefined)
    && !(noReturns && performance.historyStatus !== undefined)) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Performance history evidence is inconsistent." })
  }
})

export function parsePortfolioPerformance(result: ApplicationResult): PortfolioPerformance {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("Performance information is incomplete.")
  }
  return parsePortfolioResult(result, performanceSchema)
}

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
export type PortfolioHoldingsPage = z.infer<typeof portfolioHoldingsPageSchema>
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
