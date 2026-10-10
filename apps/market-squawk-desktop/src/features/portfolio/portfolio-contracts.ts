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

const portfolioPositionPageSchema = z.strictObject({
  holdings: z.array(holdingSchema),
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
})

function validatePositionPage(page: z.infer<typeof portfolioPositionPageSchema>, context: z.RefinementCtx) {
  const instruments = new Set<string>()
  for (const holding of page.holdings) {
    if (holding.snapshotToken !== page.snapshotToken
      || holding.marketValue.currency !== holding.currency
      || instruments.has(holding.instrumentId)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Position evidence is inconsistent." })
    }
    instruments.add(holding.instrumentId)
  }
}

export const portfolioHoldingsPageSchema = portfolioPositionPageSchema.superRefine(validatePositionPage)

export function parsePortfolioHoldings(result: ApplicationResult): PortfolioHoldingsPage {
  return parsePositionPage(result, portfolioHoldingsPageSchema)
}

function parsePositionPage<Schema extends z.ZodType<z.infer<typeof portfolioPositionPageSchema>>>(
  result: ApplicationResult,
  schema: Schema,
): z.infer<Schema> {
  const page = parsePortfolioResult(result, schema)
  if (result.metadata.returnedItems !== page.holdings.length) {
    throw new Error("Position counts are inconsistent.")
  }
  return page
}

export const portfolioTransactionSchema = z.strictObject({
  transactionToken: z.string().uuid(),
  accountId: z.string().min(1),
  snapshotToken: z.string().uuid(),
  instrumentId: z.string().min(1).nullable(),
  category: z.enum(["trade", "cash_transfer", "income", "fee", "corporate_action"]),
  amount: moneySchema,
  quantity: exactDecimalSchema.nullable(),
  occurredAtUnixNanos: unixNanosSchema,
  lotMethod: lotMethodSchema.nullable(),
  investment: z.strictObject({ name: z.string().nullable(), symbol: z.string().nullable() }).nullable(),
})

export const portfolioTransactionPageSchema = z.strictObject({
  transactions: z.array(portfolioTransactionSchema),
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
}).superRefine((page, context) => {
  const tokens = new Set<string>()
  const accountId = page.transactions[0]?.accountId
  for (const transaction of page.transactions) {
    if (transaction.snapshotToken !== page.snapshotToken
      || transaction.accountId !== accountId
      || tokens.has(transaction.transactionToken)
      || (transaction.instrumentId === null && transaction.investment !== null)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Transaction evidence is inconsistent." })
    }
    tokens.add(transaction.transactionToken)
  }
})

export function parsePortfolioTransactions(result: ApplicationResult): PortfolioTransactionPage {
  const page = parsePortfolioResult(result, portfolioTransactionPageSchema)
  if (result.metadata.returnedItems !== page.transactions.length) {
    throw new Error("Transaction counts are inconsistent.")
  }
  return page
}

export const portfolioRevisionChoiceSchema = z.strictObject({
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
  holdingCount: z.number().int().nonnegative(),
  transactionCount: z.number().int().nonnegative(),
  dataIssueCount: z.number().int().nonnegative(),
  dataState: z.enum(["ready", "needs_review"]),
})

export const portfolioRevisionPageSchema = z.strictObject({
  revisions: z.array(portfolioRevisionChoiceSchema),
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
  selectedSnapshotToken: z.string().uuid(),
}).superRefine((page, context) => {
  const tokens = new Set<string>()
  for (const revision of page.revisions) {
    if (tokens.has(revision.snapshotToken)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Saved portfolio choices are inconsistent." })
    }
    tokens.add(revision.snapshotToken)
  }
})

export function parsePortfolioRevisions(result: ApplicationResult): PortfolioRevisionPage {
  const page = parsePortfolioResult(result, portfolioRevisionPageSchema)
  if (result.metadata.returnedItems !== page.revisions.length) {
    throw new Error("Saved portfolio counts are inconsistent.")
  }
  return page
}

const contributionSchema = z.strictObject({
  instrumentId: z.string().min(1),
  investment: z.strictObject({ name: z.string().nullable(), symbol: z.string().nullable() }),
  opening: moneySchema,
  closing: moneySchema,
  amount: moneySchema,
})

export const portfolioAttributionSchema = z.strictObject({
  contributions: z.array(contributionSchema),
  total: moneySchema,
  pageCursor: z.string().min(1).max(512),
  nextCursor: z.string().min(1).max(512).nullable(),
  snapshotToken: z.string().uuid(),
  baselineSnapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
  baselineEffectiveAtUnixNanos: unixNanosSchema,
  baselineAvailableAtUnixNanos: unixNanosSchema.nullable(),
  explanation: z.string(),
}).superRefine((page, context) => {
  const instruments = new Set<string>()
  if (page.snapshotToken === page.baselineSnapshotToken) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Choose an earlier saved portfolio." })
  }
  for (const contribution of page.contributions) {
    if (instruments.has(contribution.instrumentId)
      || contribution.opening.currency !== page.total.currency
      || contribution.closing.currency !== page.total.currency
      || contribution.amount.currency !== page.total.currency) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Saved portfolio comparison is inconsistent." })
    }
    instruments.add(contribution.instrumentId)
  }
})

export function parsePortfolioAttribution(result: ApplicationResult): PortfolioAttribution {
  const page = parsePortfolioResult(result, portfolioAttributionSchema)
  if (result.metadata.returnedItems !== page.contributions.length) {
    throw new Error("Comparison counts are inconsistent.")
  }
  return page
}

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

export const exposureSchema = z.strictObject({
  net: moneySchema.nullable(),
  gross: moneySchema.nullable(),
  positionCount: z.number().int().nonnegative(),
  currency: z.array(z.strictObject({ currency: currencySchema, amount: moneySchema })),
  sector: z.array(z.strictObject({ classification: productNameSchema, amount: moneySchema })),
  factor: z.array(z.strictObject({ classification: productNameSchema, amount: moneySchema })),
  calculationStatus: z.enum(["available", "no_positions"]),
  classificationStatus: z.literal("not_supplied_by_portfolio_source"),
})

export const portfolioExposurePageSchema = portfolioPositionPageSchema
  .extend({ exposure: exposureSchema })
  .superRefine(validatePositionPage)

export function parsePortfolioExposure(result: ApplicationResult): PortfolioExposurePage {
  return parsePositionPage(result, portfolioExposurePageSchema)
}

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

export const portfolioScenarioInputSchema = z.strictObject({
  id: z.string().min(1).max(512)
    .refine((value) => new TextEncoder().encode(value).length <= 512
      && !/[\p{White_Space}\p{Cc}]/u.test(value), "Use a scenario name without spaces or control characters."),
  composition: z.enum(["additive", "compounded"]),
  shocks: z.array(z.strictObject({
    instrumentId: z.string().uuid(),
    percentChange: exactDecimalSchema,
  })).min(1),
})

const portfolioScenarioResultSchema = portfolioScenarioInputSchema.extend({
  contributions: z.array(z.strictObject({
    instrumentId: z.string().uuid(),
    investment: z.strictObject({ name: z.string().nullable(), symbol: z.string().nullable() }).nullable(),
    amount: moneySchema,
  })),
  total: moneySchema,
}).superRefine((scenario, context) => {
  const affected = new Set(scenario.shocks.map((shock) => shock.instrumentId))
  const returned = new Set<string>()
  for (const contribution of scenario.contributions) {
    if (!affected.has(contribution.instrumentId) || returned.has(contribution.instrumentId)
      || contribution.amount.currency !== scenario.total.currency) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Scenario contributions are inconsistent." })
    }
    returned.add(contribution.instrumentId)
  }
  if (returned.size !== affected.size) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Scenario contributions are incomplete." })
  }
})

const planningReportFields = {
  calculationToken: z.string().uuid(),
  calculatedAtUnixNanos: unixNanosSchema,
  accountId: z.string().min(1),
  snapshotToken: z.string().uuid(),
  effectiveAtUnixNanos: unixNanosSchema,
  availableAtUnixNanos: unixNanosSchema.nullable(),
  dataConfidence: z.literal("limited"),
}
const portfolioScenarioReportSchema = z.strictObject({
  ...planningReportFields, scenario: portfolioScenarioResultSchema,
})
const portfolioScenarioBatchReportSchema = z.strictObject({
  ...planningReportFields, scenarios: z.array(portfolioScenarioResultSchema).min(1),
})

export function parsePortfolioScenarioReport(
  result: ApplicationResult,
  selection: { snapshotToken: string; effectiveAtUnixNanos: string; availableAtUnixNanos: string | null; accountId?: string },
  submitted: PortfolioScenarioInput[],
  batch: boolean,
): PortfolioScenarioReport {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("The stress calculation is incomplete.")
  }
  const report = batch
    ? parsePortfolioResult(result, portfolioScenarioBatchReportSchema)
    : parsePortfolioResult(result, portfolioScenarioReportSchema)
  const scenarios = "scenarios" in report ? report.scenarios : [report.scenario]
  if (report.snapshotToken !== selection.snapshotToken
    || report.effectiveAtUnixNanos !== selection.effectiveAtUnixNanos
    || report.availableAtUnixNanos !== selection.availableAtUnixNanos
    || (selection.accountId !== undefined && report.accountId !== selection.accountId)
    || scenarios.length !== submitted.length
    || new Set(scenarios.map((scenario) => scenario.id)).size !== scenarios.length
    || scenarios.some((scenario, index) => {
      const original = submitted[index]
      return !original || scenario.id !== original.id || scenario.composition !== original.composition
        || scenario.shocks.length !== original.shocks.length
        || scenario.shocks.some((shock, shockIndex) => {
          const input = original.shocks[shockIndex]
          return !input || shock.instrumentId !== input.instrumentId || shock.percentChange !== input.percentChange
        })
    })) {
    throw new Error("The stress calculation does not match your selected portfolio and assumptions.")
  }
  return { ...report, scenarios }
}

export const portfolioRebalanceInputSchema = z.strictObject({
  targets: z.array(z.strictObject({
    instrumentId: z.string().uuid(),
    targetPercent: exactDecimalSchema,
  })),
  maxTurnoverPercent: exactDecimalSchema,
  minimumCash: moneySchema,
  allowShort: z.boolean(),
}).superRefine((proposal, context) => {
  const instruments = proposal.targets.map((target) => target.instrumentId)
  if (new Set(instruments).size !== instruments.length) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Enter each investment target once." })
  }
})

const portfolioRebalanceReportSchema = z.strictObject({
  ...planningReportFields,
  proposal: portfolioRebalanceInputSchema,
  totalValue: moneySchema,
  trades: z.array(z.strictObject({
    instrumentId: z.string().uuid(),
    investment: z.strictObject({ name: z.string().nullable(), symbol: z.string().nullable() }).nullable(),
    currentValue: moneySchema,
    valueChange: moneySchema,
    projectedValue: moneySchema,
  })),
  projectedCash: moneySchema,
  turnoverPercent: exactDecimalSchema,
  constrained: z.boolean(),
}).superRefine((report, context) => {
  const instruments = new Set<string>()
  const targets = new Set(report.proposal.targets.map((target) => target.instrumentId))
  if (report.projectedCash.currency !== report.totalValue.currency
    || report.proposal.minimumCash.currency !== report.totalValue.currency) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Rebalance currencies are inconsistent." })
  }
  for (const trade of report.trades) {
    if (!targets.has(trade.instrumentId) || instruments.has(trade.instrumentId)
      || trade.currentValue.currency !== report.totalValue.currency
      || trade.valueChange.currency !== report.totalValue.currency
      || trade.projectedValue.currency !== report.totalValue.currency) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Rebalance investment evidence is inconsistent." })
    }
    instruments.add(trade.instrumentId)
  }
})

export function parsePortfolioRebalanceReport(
  result: ApplicationResult,
  selection: { snapshotToken: string; effectiveAtUnixNanos: string; availableAtUnixNanos: string | null; accountId?: string },
  submitted: PortfolioRebalanceInput,
): PortfolioRebalanceReport {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("The rebalance calculation is incomplete.")
  }
  const report = parsePortfolioResult(result, portfolioRebalanceReportSchema)
  const original = report.proposal
  if (report.snapshotToken !== selection.snapshotToken
    || report.effectiveAtUnixNanos !== selection.effectiveAtUnixNanos
    || report.availableAtUnixNanos !== selection.availableAtUnixNanos
    || (selection.accountId !== undefined && report.accountId !== selection.accountId)
    || original.allowShort !== submitted.allowShort
    || original.maxTurnoverPercent !== submitted.maxTurnoverPercent
    || original.minimumCash.amount !== submitted.minimumCash.amount
    || original.minimumCash.currency !== submitted.minimumCash.currency
    || original.targets.length !== submitted.targets.length
    || original.targets.some((target, index) => {
      const input = submitted.targets[index]
      return !input || target.instrumentId !== input.instrumentId || target.targetPercent !== input.targetPercent
    })) {
    throw new Error("The rebalance calculation does not match your selected portfolio and assumptions.")
  }
  return report
}

export const portfolioCandidateImpactInputSchema = z.strictObject({
  instrumentId: z.string().uuid(),
  proposedQuantity: exactDecimalSchema,
  scenarioShockPercent: exactDecimalSchema,
})

const candidateCostSchema = z.discriminatedUnion("state", [
  z.strictObject({ state: z.literal("available"), amount: moneySchema }),
  z.strictObject({ state: z.literal("not_available") }),
])

const portfolioCandidateImpactSchema = z.strictObject({
  calculationToken: z.string().uuid(),
  calculatedAtUnixNanos: unixNanosSchema,
  accountToken: portfolioAccountSummarySchema.shape.accountToken,
  accountId: z.string().min(1),
  snapshotToken: z.string().uuid(),
  portfolioEffectiveAtUnixNanos: unixNanosSchema,
  portfolioAvailableAtUnixNanos: unixNanosSchema,
  evidenceDigest: z.string().regex(/^[0-9a-f]{64}$/),
  instrumentId: z.string().uuid(),
  positionState: z.enum(["new", "existing"]),
  currentQuantity: exactDecimalSchema,
  proposedQuantity: exactDecimalSchema,
  currentMarketValue: moneySchema,
  proposedMarketValue: moneySchema,
  capitalChange: moneySchema,
  portfolioValue: moneySchema,
  instrumentTerms: z.strictObject({
    priceTick: exactDecimalSchema,
    lotSize: exactDecimalSchema,
    quoteCurrency: currencySchema,
    contractMultiplier: exactDecimalSchema,
  }),
  costs: z.strictObject({ fees: candidateCostSchema, slippage: candidateCostSchema }),
  concentration: z.strictObject({ current: exactDecimalSchema, proposed: exactDecimalSchema, change: exactDecimalSchema }),
  scenario: z.strictObject({
    shock: exactDecimalSchema,
    currentImpact: moneySchema,
    proposedImpact: moneySchema,
    marginalImpact: moneySchema,
  }),
  price: z.strictObject({
    amount: moneySchema,
    asOfUnixNanos: unixNanosSchema,
    freshUntilUnixNanos: unixNanosSchema,
    state: z.literal("current"),
    method: z.enum(["Last trade", "Bid-ask midpoint"]),
    confidence: z.enum(["limited", "moderate", "strong"]),
  }),
  assumptions: z.strictObject({
    proposedQuantity: exactDecimalSchema,
    scenarioShockPercent: exactDecimalSchema,
    quantityMeaning: z.literal("target_total"),
    fundingAssumption: z.literal("cash_transfer_before_costs"),
    portfolioValueBasis: z.literal("source_reported_holdings_with_selected_candidate_revalued"),
    scenarioScope: z.literal("candidate_position_only"),
  }),
  missingInformation: z.array(z.string().min(1)),
  riskAssessment: z.strictObject({
    state: z.literal("incomplete"),
    evaluatedAtUnixNanos: unixNanosSchema,
    checksCompleted: z.number().int().nonnegative(),
    checksUnavailable: z.number().int().nonnegative(),
  }),
  updatedAtUnixNanos: unixNanosSchema,
  analysisOnly: z.literal(true),
}).superRefine((report, context) => {
  const amounts = [report.currentMarketValue, report.proposedMarketValue, report.capitalChange,
    report.portfolioValue, report.price.amount, report.scenario.currentImpact,
    report.scenario.proposedImpact, report.scenario.marginalImpact]
  for (const cost of [report.costs.fees, report.costs.slippage]) {
    if (cost.state === "available") amounts.push(cost.amount)
  }
  if (amounts.some((amount) => amount.currency !== report.instrumentTerms.quoteCurrency)) {
    context.addIssue({ code: z.ZodIssueCode.custom, message: "Position comparison currencies are inconsistent." })
  }
})

export function parsePortfolioCandidateImpact(
  result: ApplicationResult,
  accountToken: string,
  submitted: PortfolioCandidateImpactInput,
): PortfolioCandidateImpact {
  if (result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1) {
    throw new Error("The position comparison is incomplete.")
  }
  const report = parsePortfolioResult(result, portfolioCandidateImpactSchema)
  if (report.accountToken !== accountToken || report.instrumentId !== submitted.instrumentId
    || report.assumptions.proposedQuantity !== submitted.proposedQuantity
    || report.assumptions.scenarioShockPercent !== submitted.scenarioShockPercent) {
    throw new Error("The position comparison does not match your selected portfolio and assumptions.")
  }
  return report
}

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
export type PortfolioTransactionPage = z.infer<typeof portfolioTransactionPageSchema>
export type PortfolioRevisionChoice = z.infer<typeof portfolioRevisionChoiceSchema>
export type PortfolioRevisionPage = z.infer<typeof portfolioRevisionPageSchema>
export type PortfolioPerformance = z.infer<typeof performanceSchema>
export type PortfolioExposure = z.infer<typeof exposureSchema>
export type PortfolioExposurePage = z.infer<typeof portfolioExposurePageSchema>
export type PortfolioRisk = z.infer<typeof riskSchema>
export type PortfolioAttribution = z.infer<typeof portfolioAttributionSchema>
export type PortfolioScenarioInput = z.infer<typeof portfolioScenarioInputSchema>
export type PortfolioScenarioResult = z.infer<typeof portfolioScenarioResultSchema>
export type PortfolioScenarioReport = z.infer<typeof portfolioScenarioBatchReportSchema>
export type PortfolioRebalanceInput = z.infer<typeof portfolioRebalanceInputSchema>
export type PortfolioRebalanceReport = z.infer<typeof portfolioRebalanceReportSchema>
export type PortfolioCandidateImpactInput = z.infer<typeof portfolioCandidateImpactInputSchema>
export type PortfolioCandidateImpact = z.infer<typeof portfolioCandidateImpactSchema>
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
