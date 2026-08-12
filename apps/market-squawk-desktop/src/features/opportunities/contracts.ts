import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"

const MAXIMUM_U32 = 4_294_967_295
const PARTS_PER_MILLION = 1_000_000
const MAXIMUM_PAGE_ANALYSES = 1_000
const MAXIMUM_AVAILABLE_ANALYSES = 4_096

const canonicalDigestSchema = z.string().regex(/^[0-9a-f]{64}$/)
const nonzeroDigestSchema = canonicalDigestSchema.refine(
  (value) => /[1-9a-f]/.test(value),
  "Expected a nonzero lowercase digest.",
)
const canonicalUuidSchema = z
  .string()
  .regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  )
  .refine(
    (value) => value !== "00000000-0000-0000-0000-000000000000",
    "Expected a non-nil canonical UUID.",
  )
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const canonicalDecimalSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/)
  .refine((value) => value !== "-0", "Expected a normalized exact decimal.")
const canonicalIntegerSchema = losslessIntegerSchema.refine(
  (value) => /^(?:0|-?[1-9]\d*)$/.test(value),
  "Expected a canonical lossless integer.",
)
const positiveIntegerSchema = canonicalIntegerSchema.refine(
  (value) => BigInt(value) > 0n,
  "Expected a positive lossless integer.",
)
const nonnegativeIntegerSchema = canonicalIntegerSchema.refine(
  (value) => BigInt(value) >= 0n,
  "Expected a nonnegative lossless integer.",
)
const positiveU32Schema = z.number().int().min(1).max(MAXIMUM_U32)
const ppmSchema = z.number().int().min(0).max(PARTS_PER_MILLION)

const notApplicableSchema = z
  .object({ status: z.literal("not_applicable") })
  .strict()

const moneySchema = z
  .object({
    amount: canonicalDecimalSchema,
    currency: currencySchema,
  })
  .strict()

const priceRangeSchema = z
  .object({
    lower: moneySchema,
    upper: moneySchema,
  })
  .strict()

const priceCasesSchema = z
  .object({
    downside: moneySchema,
    base: moneySchema,
    upside: moneySchema,
  })
  .strict()

const forecastRangesSchema = z
  .object({
    downside: priceRangeSchema,
    base: priceRangeSchema,
    upside: priceRangeSchema,
  })
  .strict()

const contentIdentitySchema = z
  .object({
    algorithm: z.enum(["sha256", "blake3"]),
    digest: nonzeroDigestSchema,
  })
  .strict()

const evidenceWindowSchema = z
  .object({
    observedAt: canonicalIntegerSchema,
    availableAt: canonicalIntegerSchema,
    expiresAt: canonicalIntegerSchema,
    contentIdentity: contentIdentitySchema,
  })
  .strict()

const dataQualitySchema = z.enum([
  "direct_verified",
  "direct_unverified",
  "official_delayed",
  "aggregated",
  "indicative",
  "modeled",
  "estimated",
  "stale",
  "quarantined",
])

const marketEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    price: moneySchema,
    quality: dataQualitySchema,
    priceKind: z.enum(["last_trade", "checked_bid_ask_midpoint"]),
    adjustmentBasis: z.literal("unadjusted_spot"),
    selectionReceiptIdentity: contentIdentitySchema,
    selectedObservationIdentity: contentIdentitySchema,
    window: evidenceWindowSchema,
  })
  .strict()

const expectedTerminalEvidenceSchema = z
  .object({
    statistic: z.literal("model_estimated_conditional_mean"),
    price: moneySchema,
    horizonAt: canonicalIntegerSchema,
    statisticIdentity: contentIdentitySchema,
  })
  .strict()

const forecastEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    cases: priceCasesSchema,
    ranges: forecastRangesSchema,
    horizonAt: canonicalIntegerSchema,
    expectedTerminal: expectedTerminalEvidenceSchema.nullable(),
    vintageId: nonzeroDigestSchema,
    outputBindingIdentity: contentIdentitySchema,
    calibrationIdentity: contentIdentitySchema,
    outcomeSetIdentity: contentIdentitySchema,
    calibration: z
      .object({
        nominalCoveragePpm: ppmSchema,
        realizedCoveragePpm: ppmSchema,
        completedOutcomes: positiveU32Schema,
      })
      .strict(),
    window: evidenceWindowSchema,
  })
  .strict()

const valuationEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    fairValue: moneySchema,
    basis: z.literal("per_instrument_unit"),
    horizonAt: canonicalIntegerSchema,
    measurementId: nonzeroDigestSchema,
    classificationDecisionId: nonzeroDigestSchema,
    selectionReceiptHash: nonzeroDigestSchema,
    window: evidenceWindowSchema,
  })
  .strict()

const backtestEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    currency: currencySchema,
    outcomeHorizonNanos: positiveIntegerSchema,
    netReturnBasisPoints: canonicalIntegerSchema,
    maxDrawdownBasisPoints: nonnegativeIntegerSchema,
    feeBasisPoints: nonnegativeIntegerSchema,
    slippageBasisPoints: nonnegativeIntegerSchema,
    maximumRandomSlippageBasisPoints: nonnegativeIntegerSchema,
    observations: positiveU32Schema,
    trials: positiveU32Schema,
    stabilityPpm: ppmSchema,
    simulationCutoffAt: canonicalIntegerSchema,
    datasetIdentity: contentIdentitySchema,
    commandIdentity: contentIdentitySchema,
    terminalIdentity: contentIdentitySchema,
    reportIdentity: contentIdentitySchema,
    cohortIdentity: contentIdentitySchema,
    costModelIdentity: contentIdentitySchema,
    window: evidenceWindowSchema,
  })
  .strict()

const liquidityEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    currency: currencySchema,
    quotedSpreadBasisPoints: nonnegativeIntegerSchema,
    capacityPpm: ppmSchema,
    quality: dataQualitySchema,
    assessmentIdentity: contentIdentitySchema,
    window: evidenceWindowSchema,
  })
  .strict()

const positionStateSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("no_position") }).strict(),
  z
    .object({
      kind: z.literal("position"),
      addAllowed: z.boolean(),
      trimAllowed: z.boolean(),
      exitAllowed: z.boolean(),
    })
    .strict(),
])

const portfolioRiskEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    accountId: canonicalUuidSchema,
    currency: currencySchema,
    // The all-zero portfolio revision is retained only so an unavailable result can explain it.
    portfolioRevision: canonicalDigestSchema,
    positionState: positionStateSchema,
    riskCapacityPpm: ppmSchema,
    riskReportIdentity: contentIdentitySchema,
    window: evidenceWindowSchema,
  })
  .strict()

const investmentAnalysisEvidenceSchema = z
  .object({
    instrumentId: canonicalUuidSchema,
    currency: currencySchema,
    accountId: canonicalUuidSchema,
    asOf: canonicalIntegerSchema,
    market: marketEvidenceSchema.nullable(),
    priceForecast: forecastEvidenceSchema.nullable(),
    valuation: valuationEvidenceSchema.nullable(),
    backtest: backtestEvidenceSchema.nullable(),
    liquidity: liquidityEvidenceSchema.nullable(),
    portfolioRisk: portfolioRiskEvidenceSchema.nullable(),
  })
  .strict()

const recommendationPolicySchema = z
  .object({
    version: positiveU32Schema,
    digest: nonzeroDigestSchema,
    actionZoneSemanticsVersion: positiveU32Schema,
    horizonNanos: positiveIntegerSchema,
    proposalLifetimeNanos: positiveIntegerSchema,
    assumptions: z.array(z.string().min(1).max(4_096)).length(3),
    invalidationConditions: z.array(z.string().min(1).max(4_096)).length(3),
    limitations: z.array(z.string().min(1).max(4_096)).length(3),
  })
  .strict()

const evidenceKindSchema = z.enum([
  "market",
  "price_forecast",
  "valuation",
  "backtest",
  "liquidity",
  "portfolio_risk",
])

function evidenceUnavailableReasonSchema<const Kind extends string>(kind: Kind) {
  return z
    .object({
      kind: z.literal(kind),
      evidence: evidenceKindSchema,
    })
    .strict()
}

function horizonUnavailableReasonSchema<const Kind extends string>(kind: Kind) {
  return z
    .object({
      kind: z.literal(kind),
      expected: canonicalIntegerSchema,
      actual: canonicalIntegerSchema,
    })
    .strict()
}

function countUnavailableReasonSchema<const Kind extends string>(kind: Kind) {
  return z
    .object({
      kind: z.literal(kind),
      required: positiveU32Schema,
      actual: positiveU32Schema,
    })
    .strict()
}

const unavailableReasonSchema = z.discriminatedUnion("kind", [
  evidenceUnavailableReasonSchema("missing_evidence"),
  z
    .object({
      kind: z.literal("instrument_mismatch"),
      evidence: evidenceKindSchema,
      expected: canonicalUuidSchema,
      actual: canonicalUuidSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("currency_mismatch"),
      evidence: evidenceKindSchema,
      expected: currencySchema,
      actual: currencySchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("account_mismatch"),
      expected: canonicalUuidSchema,
      actual: canonicalUuidSchema,
    })
    .strict(),
  evidenceUnavailableReasonSchema("not_available_at_cutoff"),
  evidenceUnavailableReasonSchema("expired_evidence"),
  evidenceUnavailableReasonSchema("stale_evidence"),
  z
    .object({
      kind: z.literal("rejected_quality"),
      evidence: evidenceKindSchema,
      quality: dataQualitySchema,
    })
    .strict(),
  horizonUnavailableReasonSchema("forecast_horizon_mismatch"),
  horizonUnavailableReasonSchema("valuation_horizon_mismatch"),
  z
    .object({
      kind: z.literal("backtest_horizon_mismatch"),
      expectedNanos: canonicalIntegerSchema,
      actualNanos: canonicalIntegerSchema,
    })
    .strict(),
  countUnavailableReasonSchema("insufficient_forecast_outcomes"),
  z
    .object({
      kind: z.literal("unsupported_forecast_coverage"),
      minimumPpm: ppmSchema,
      maximumPpm: ppmSchema,
      actualPpm: ppmSchema,
    })
    .strict(),
  countUnavailableReasonSchema("insufficient_backtest_observations"),
  countUnavailableReasonSchema("insufficient_backtest_trials"),
  z.object({ kind: z.literal("reserved_portfolio_revision") }).strict(),
])

const recommendationActionSchema = z.enum(["buy", "add", "hold", "trim", "sell"])

const noActionReasonSchema = z.enum([
  "conflicting_forecast_and_valuation",
  "backtest_below_policy",
  "liquidity_below_policy",
  "portfolio_risk_below_policy",
  "evidence_reliability_below_policy",
  "position_state_not_actionable",
  "generated_price_order_collapsed",
])

const proposalInvalidatorSchema = z.enum([
  "forecast_valuation_conflict",
  "backtest_policy_breach",
  "liquidity_policy_breach",
  "portfolio_risk_policy_breach",
  "evidence_reliability_policy_breach",
  "position_state_incompatible",
  "generated_price_order_collapsed",
])

const EVIDENCE_RELIABILITY_COMPONENTS = [
  "forecast_calibration",
  "valuation_agreement",
  "backtest_stability",
  "market_integrity",
  "liquidity_capacity",
  "portfolio_risk_capacity",
] as const

const evidenceReliabilitySchema = z
  .object({
    meaning: z.literal("policy_weighted_evidence_reliability_v1"),
    valuePpm: ppmSchema,
    components: z
      .array(
        z
          .object({
            kind: z.enum(EVIDENCE_RELIABILITY_COMPONENTS),
            valuePpm: ppmSchema,
            weightPpm: ppmSchema,
          })
          .strict(),
      )
      .length(EVIDENCE_RELIABILITY_COMPONENTS.length),
  })
  .strict()

const priceLadderSchema = z
  .object({
    cases: priceCasesSchema,
    ranges: z
      .object({
        downside: priceRangeSchema,
        base: priceRangeSchema,
        upside: priceRangeSchema,
        entry: priceRangeSchema,
        add: priceRangeSchema,
        trim: priceRangeSchema,
        exit: priceRangeSchema,
      })
      .strict(),
    addCase: moneySchema,
  })
  .strict()

const actionZoneSemanticsSchema = z
  .object({
    version: positiveU32Schema,
    referenceZone: priceRangeSchema.nullable(),
    triggerFloorExclusive: moneySchema.nullable(),
    triggerFloorInclusive: moneySchema.nullable(),
    triggerCeilingInclusive: moneySchema.nullable(),
  })
  .strict()

const generatedResultSchema = z
  .object({
    kind: z.literal("generated"),
    proposalId: nonzeroDigestSchema,
    derivationDigest: nonzeroDigestSchema,
    action: recommendationActionSchema,
    priceLadder: priceLadderSchema,
    actionZoneSemantics: actionZoneSemanticsSchema,
    evidenceReliability: evidenceReliabilitySchema,
    horizonAt: canonicalIntegerSchema,
    expiresAt: canonicalIntegerSchema,
  })
  .strict()

const noActionResultSchema = z
  .object({
    kind: z.literal("no_action"),
    proposalId: nonzeroDigestSchema,
    derivationDigest: nonzeroDigestSchema,
    reason: noActionReasonSchema,
    invalidators: z.array(proposalInvalidatorSchema).length(1),
    evidenceReliability: evidenceReliabilitySchema,
    horizonAt: canonicalIntegerSchema,
    expiresAt: canonicalIntegerSchema,
  })
  .strict()

const unavailableResultSchema = z
  .object({
    kind: z.literal("unavailable"),
    reason: unavailableReasonSchema,
    horizonAt: canonicalIntegerSchema,
    expiresAt: canonicalIntegerSchema,
  })
  .strict()

const investmentAnalysisResultSchema = z.union([
  generatedResultSchema,
  noActionResultSchema,
  unavailableResultSchema,
])

export const investmentAnalysisSchema = z
  .object({
    analysisId: nonzeroDigestSchema,
    policy: recommendationPolicySchema,
    evidence: investmentAnalysisEvidenceSchema,
    evidenceDigest: nonzeroDigestSchema,
    result: investmentAnalysisResultSchema,
  })
  .strict()

const investmentAnalysisEnvelopeSchema = z
  .object({
    data: investmentAnalysisSchema,
    metadata: z
      .object({
        completeness: z.literal("complete"),
        returnedItems: z.literal(1),
        availableItems: z.literal(1),
        sourceCoverage: notApplicableSchema,
        dataQuality: notApplicableSchema,
      })
      .strict(),
  })
  .strict()

const locatorOutcomeSchema = z.union([
  z
    .object({
      kind: z.literal("generated"),
      action: recommendationActionSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("no_action"),
      reason: noActionReasonSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      reason: unavailableReasonSchema,
    })
    .strict(),
])

const investmentAnalysisLocatorSchema = z
  .object({
    analysisId: nonzeroDigestSchema,
    proposalId: nonzeroDigestSchema.nullable(),
    derivationDigest: nonzeroDigestSchema.nullable(),
    instrumentId: canonicalUuidSchema,
    accountId: canonicalUuidSchema,
    currency: currencySchema,
    asOf: canonicalIntegerSchema,
    horizonAt: canonicalIntegerSchema,
    expiresAt: canonicalIntegerSchema,
    policyDigest: nonzeroDigestSchema,
    evidenceDigest: nonzeroDigestSchema,
    outcome: locatorOutcomeSchema,
  })
  .strict()
  .superRefine((locator, context) => {
    const hasProposal = locator.outcome.kind !== "unavailable"
    if (
      (locator.proposalId !== null) !== hasProposal ||
      (locator.derivationDigest !== null) !== hasProposal
    ) {
      context.addIssue({
        code: "custom",
        message: "The analysis locator does not match its retained outcome identity.",
      })
    }
  })

export const investmentAnalysisPageSchema = z
  .object({
    completeness: z.enum(["complete", "truncated"]),
    returnedCount: z.number().int().min(0).max(MAXIMUM_PAGE_ANALYSES),
    availableCount: z.number().int().min(0).max(MAXIMUM_AVAILABLE_ANALYSES),
    nextAfterAnalysisId: nonzeroDigestSchema.nullable(),
    analyses: z.array(investmentAnalysisLocatorSchema).max(MAXIMUM_PAGE_ANALYSES),
  })
  .strict()
  .superRefine((page, context) => {
    if (page.returnedCount !== page.analyses.length) {
      context.addIssue({
        code: "custom",
        path: ["returnedCount"],
        message: "The analysis page count does not match its retained locators.",
      })
    }
    if (page.availableCount < page.returnedCount) {
      context.addIssue({
        code: "custom",
        path: ["availableCount"],
        message: "The analysis page reports fewer available records than returned records.",
      })
    }
    if (
      new Set(page.analyses.map((analysis) => analysis.analysisId)).size !==
      page.analyses.length
    ) {
      context.addIssue({
        code: "custom",
        path: ["analyses"],
        message: "The analysis page contains a repeated stable identity.",
      })
    }

    if (page.completeness === "complete") {
      if (page.nextAfterAnalysisId !== null || page.availableCount !== page.returnedCount) {
        context.addIssue({
          code: "custom",
          message: "A complete analysis page retains an invalid continuation state.",
        })
      }
      return
    }

    const lastAnalysisId = page.analyses.at(-1)?.analysisId
    if (
      page.availableCount <= page.returnedCount ||
      page.nextAfterAnalysisId === null ||
      page.nextAfterAnalysisId !== lastAnalysisId
    ) {
      context.addIssue({
        code: "custom",
        message: "A truncated analysis page does not bind its exact next cursor.",
      })
    }
  })

const investmentAnalysisPageEnvelopeSchema = z
  .object({
    data: investmentAnalysisPageSchema,
    metadata: z
      .object({
        completeness: z.enum(["complete", "truncated"]),
        returnedItems: z.number().int().min(0).max(MAXIMUM_PAGE_ANALYSES),
        availableItems: z.number().int().min(0).max(MAXIMUM_AVAILABLE_ANALYSES),
        sourceCoverage: notApplicableSchema,
        dataQuality: notApplicableSchema,
      })
      .strict(),
  })
  .strict()
  .superRefine((envelope, context) => {
    if (
      envelope.metadata.completeness !== envelope.data.completeness ||
      envelope.metadata.returnedItems !== envelope.data.returnedCount ||
      envelope.metadata.availableItems !== envelope.data.availableCount
    ) {
      context.addIssue({
        code: "custom",
        path: ["metadata"],
        message: "The analysis-page envelope contradicts its business pagination state.",
      })
    }
  })

const investmentAnalysisPageRequestSchema = z
  .object({
    afterAnalysisId: nonzeroDigestSchema.optional(),
    limit: z.number().int().min(1).max(MAXIMUM_PAGE_ANALYSES),
  })
  .strict()

export type InvestmentAnalysis = z.infer<typeof investmentAnalysisSchema>
export type InvestmentAnalysisEvidence = z.infer<
  typeof investmentAnalysisEvidenceSchema
>
export type InvestmentAnalysisResult = z.infer<typeof investmentAnalysisResultSchema>
export type GeneratedInvestmentAnalysis = z.infer<typeof generatedResultSchema>
export type NoActionInvestmentAnalysis = z.infer<typeof noActionResultSchema>
export type UnavailableInvestmentAnalysis = z.infer<typeof unavailableResultSchema>
export type InvestmentAnalysisLocator = z.infer<typeof investmentAnalysisLocatorSchema>
export type InvestmentAnalysisPage = z.infer<typeof investmentAnalysisPageSchema>

export function parseInvestmentAnalysis(
  result: ApplicationResult,
  expectedAnalysisId: string,
): InvestmentAnalysis {
  const parsed = investmentAnalysisEnvelopeSchema.safeParse(result)
  const expected = nonzeroDigestSchema.safeParse(expectedAnalysisId)
  if (
    !parsed.success ||
    !expected.success ||
    parsed.data.data.analysisId !== expected.data
  ) {
    throw new Error("The installed service returned an unsupported investment analysis.")
  }
  return parsed.data.data
}

export function parseInvestmentAnalysisPage(
  result: ApplicationResult,
  request: { afterAnalysisId?: string; limit: number },
): InvestmentAnalysisPage {
  const parsed = investmentAnalysisPageEnvelopeSchema.safeParse(result)
  const expected = investmentAnalysisPageRequestSchema.safeParse(request)
  if (!parsed.success || !expected.success) {
    throw new Error("The installed service returned an unsupported investment-analysis page.")
  }
  const page = parsed.data.data
  if (
    page.returnedCount > expected.data.limit ||
    (page.completeness === "truncated" &&
      page.returnedCount !== expected.data.limit) ||
    (expected.data.afterAnalysisId !== undefined &&
      page.analyses.some(
        (analysis) => analysis.analysisId === expected.data.afterAnalysisId,
      ))
  ) {
    throw new Error("The installed service returned inconsistent investment-analysis paging.")
  }
  return page
}
