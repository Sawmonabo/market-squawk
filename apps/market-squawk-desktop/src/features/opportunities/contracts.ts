import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"
import { losslessIntegerSchema } from "@/lib/lossless-integer"
import {
  productLookupActions,
  productLookupCategory,
} from "@/lib/transport"

const MAXIMUM_U32 = 4_294_967_295
const PARTS_PER_MILLION = 1_000_000
const MAXIMUM_PAGE_ANALYSES = 1_000
const MAXIMUM_AVAILABLE_ANALYSES = 4_096
const MINIMUM_TRACK_RECORD_SAMPLES = 30
const MINIMUM_TRACK_RECORD_COVERAGE_PPM = 800_000
const MINIMUM_I64 = -(2n ** 63n)
const MAXIMUM_I64 = 2n ** 63n - 1n

const TRACK_RECORD_COHORTS = [
  "buy",
  "add",
  "hold",
  "trim",
  "sell",
  "no_action_control",
] as const

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
const signedI64Schema = canonicalIntegerSchema.refine(
  (value) => {
    const parsed = BigInt(value)
    return parsed >= MINIMUM_I64 && parsed <= MAXIMUM_I64
  },
  "Expected a signed 64-bit integer.",
)
const positiveI64Schema = signedI64Schema.refine(
  (value) => BigInt(value) > 0n,
  "Expected a positive signed 64-bit integer.",
)
const positiveU32Schema = z.number().int().min(1).max(MAXIMUM_U32)
const nonnegativeU32Schema = z.number().int().min(0).max(MAXIMUM_U32)
const ppmSchema = z.number().int().min(0).max(PARTS_PER_MILLION)
const operationIdentifierSchema = z
  .string()
  .min(1)
  .max(256)
  .regex(/^[A-Za-z0-9_.:/-]+$/)
const savedScreenIdSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-z][a-z0-9._-]*$/)

export const savedScreenProductSchema = z
  .object({
    category: z.literal(productLookupCategory.savedScreen),
    title: z.string().min(1).max(2_048),
    subtitle: z.string().min(1).max(2_048),
    destination: z
      .object({
        action: z.literal(productLookupActions.openSavedScreen),
        screenId: savedScreenIdSchema,
      })
      .strict(),
  })
  .strict()

const savedScreenProductEnvelopeSchema = z
  .object({
    data: savedScreenProductSchema,
    metadata: z
      .object({
        completeness: z.literal("complete"),
        returnedItems: z.literal(1),
        availableItems: z.literal(1),
        sourceCoverage: z.object({ status: z.literal("not_applicable") }).strict(),
        dataQuality: z.object({ status: z.literal("not_applicable") }).strict(),
      })
      .strict(),
  })
  .strict()

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

const analyticalProfileSchema = z
  .object({
    profileId: z.string().min(1).max(256),
    revision: positiveU32Schema,
    contentDigest: contentIdentitySchema,
  })
  .strict()

const investmentAnalysisPublicationSchema = z
  .object({
    publicationId: nonzeroDigestSchema,
    publishedAt: canonicalIntegerSchema,
    executionEligibility: z.literal("research_only_execution_ineligible"),
    analyticalProfile: analyticalProfileSchema,
    workflow: z
      .object({
        workflowId: z.string().min(1).max(256),
        revision: positiveU32Schema,
        contentDigest: contentIdentitySchema,
      })
      .strict(),
    accountSetup: z
      .object({
        accountId: canonicalUuidSchema,
        distinctFromAnalyticalProfile: z.literal(true),
      })
      .strict(),
    outcomeProjectionDigest: nonzeroDigestSchema.nullable(),
    sizingProjectionDigest: nonzeroDigestSchema.nullable(),
  })
  .strict()

function unavailableDisclosureSchema<const Reason extends string>(reason: Reason) {
  return z
    .object({
      kind: z.literal("unavailable"),
      reason: z.literal(reason),
    })
    .strict()
}

const grossMarkRelativeRangeSchema = z
  .object({
    priceRange: priceRangeSchema,
    grossReturnFromMark: z
      .object({
        lowerNumerator: moneySchema,
        upperNumerator: moneySchema,
        denominator: moneySchema,
      })
      .strict(),
  })
  .strict()

const investmentOutcomeProjectionSchema = z
  .object({
    resultDigest: nonzeroDigestSchema,
    proposalId: nonzeroDigestSchema,
    derivationDigest: nonzeroDigestSchema,
    authority: z.literal("analysis_only_no_mutation_no_execution"),
    executionEligible: z.literal(false),
    mark: moneySchema,
    horizonAt: canonicalIntegerSchema,
    downside: grossMarkRelativeRangeSchema,
    base: grossMarkRelativeRangeSchema,
    upside: grossMarkRelativeRangeSchema,
    netPnl: unavailableDisclosureSchema("exact_forward_cost_evidence_not_supplied"),
    benchmarkReturn: unavailableDisclosureSchema(
      "exact_proposal_time_benchmark_evidence_not_supplied",
    ),
    afterTaxPnl: unavailableDisclosureSchema("exact_tax_evidence_not_supplied"),
  })
  .strict()

const sizingUnavailableReasonSchema = z.enum([
  "capacity_not_supplied",
  "capacity_not_yet_available",
  "capacity_expired",
  "capacity_range_contains_no_lots",
  "cash_reserve_exceeds_gross_liquidatable_value",
  "no_hard_feasible_lot_intersection",
  "preferred_weight_range_contains_no_lots",
  "no_preferred_feasible_lot_intersection",
])

const feasibleLotsSchema = z.union([
  z
    .object({
      kind: z.literal("available"),
      lower: nonnegativeIntegerSchema,
      upper: nonnegativeIntegerSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      reasons: z.array(sizingUnavailableReasonSchema).min(1).max(8),
    })
    .strict()
    .superRefine((availability, context) => {
      if (new Set(availability.reasons).size !== availability.reasons.length) {
        context.addIssue({
          code: "custom",
          path: ["reasons"],
          message: "The sizing projection repeats one unavailability reason.",
        })
      }
    }),
])

const investmentSizingProjectionSchema = z
  .object({
    resultDigest: nonzeroDigestSchema,
    proposalId: nonzeroDigestSchema,
    derivationDigest: nonzeroDigestSchema,
    authority: z.literal("analysis_only_no_mutation_no_execution"),
    executionEligible: z.literal(false),
    evaluatedAt: canonicalIntegerSchema,
    currentLots: nonnegativeIntegerSchema,
    hardFeasibleLots: feasibleLotsSchema,
    preferredFeasibleLots: feasibleLotsSchema,
    selectedTargetLots: z.null(),
    orderQuantity: z.null(),
  })
  .strict()

const recommendationOutcomeCommonSchema = {
  seriesId: nonzeroDigestSchema,
  revision: positiveU32Schema,
  statusDigest: nonzeroDigestSchema,
  evaluatedAt: canonicalIntegerSchema,
  executionEligible: z.literal(false),
} as const

const recommendationOutcomeSchema = z.union([
  z
    .object({
      kind: z.literal("pending"),
      reason: z.enum(["awaiting_horizon", "awaiting_outcome_evidence"]),
      ...recommendationOutcomeCommonSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      reason: z.enum([
        "analysis_unavailable",
        "outcome_observation_unavailable",
        "ambiguous_outcome_observation",
        "incomplete_outcome_observation",
        "corporate_action_evidence_unavailable",
      ]),
      ...recommendationOutcomeCommonSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("completed"),
      metric: z.literal("gross_instrument_price_return"),
      startMark: moneySchema,
      endpointPrice: moneySchema,
      grossPriceReturn: canonicalDecimalSchema,
      observedAt: canonicalIntegerSchema,
      availableAt: canonicalIntegerSchema,
      selectionReceiptIdentity: contentIdentitySchema,
      selectedObservationIdentity: contentIdentitySchema,
      corporateActionEvidenceIdentity: contentIdentitySchema,
      netReturn: unavailableDisclosureSchema(
        "exact_realized_cost_evidence_not_supplied",
      ),
      benchmarkReturn: unavailableDisclosureSchema(
        "exact_benchmark_outcome_evidence_not_supplied",
      ),
      afterTaxReturn: unavailableDisclosureSchema("exact_tax_evidence_not_supplied"),
      settlement: unavailableDisclosureSchema("no_execution_or_settlement_evidence"),
      ...recommendationOutcomeCommonSchema,
    })
    .strict(),
])

function sameMoney(
  left: { amount: string; currency: string },
  right: { amount: string; currency: string },
): boolean {
  return left.amount === right.amount && left.currency === right.currency
}

function samePriceRange(
  left: { lower: { amount: string; currency: string }; upper: { amount: string; currency: string } },
  right: { lower: { amount: string; currency: string }; upper: { amount: string; currency: string } },
): boolean {
  return sameMoney(left.lower, right.lower) && sameMoney(left.upper, right.upper)
}

export const investmentAnalysisSchema = z
  .object({
    analysisId: nonzeroDigestSchema,
    executionEligibility: z.literal("research_only_execution_ineligible"),
    policy: recommendationPolicySchema,
    evidence: investmentAnalysisEvidenceSchema,
    evidenceDigest: nonzeroDigestSchema,
    publication: investmentAnalysisPublicationSchema.nullable(),
    projection: investmentOutcomeProjectionSchema.nullable(),
    sizing: investmentSizingProjectionSchema.nullable(),
    realizedOutcome: recommendationOutcomeSchema.nullable(),
    result: investmentAnalysisResultSchema,
  })
  .strict()
  .superRefine((analysis, context) => {
    const issue = (path: (string | number)[], message: string) => {
      context.addIssue({ code: "custom", path, message })
    }
    const publication = analysis.publication
    const projection = analysis.projection
    const sizing = analysis.sizing
    const realized = analysis.realizedOutcome

    if (
      publication &&
      publication.accountSetup.accountId !== analysis.evidence.accountId
    ) {
      issue(
        ["publication", "accountSetup", "accountId"],
        "The publication is bound to a different proposal account.",
      )
    }
    if (!publication && (projection || sizing || realized)) {
      issue(
        ["publication"],
        "A current analysis sidecar exists without its required publication.",
      )
    }
    if (
      (publication?.outcomeProjectionDigest ?? null) !==
      (projection?.resultDigest ?? null)
    ) {
      issue(
        ["publication", "outcomeProjectionDigest"],
        "The publication does not bind the current outcome projection.",
      )
    }
    if (
      (publication?.sizingProjectionDigest ?? null) !==
      (sizing?.resultDigest ?? null)
    ) {
      issue(
        ["publication", "sizingProjectionDigest"],
        "The publication does not bind the current sizing projection.",
      )
    }

    if (analysis.result.kind !== "generated" && (projection || sizing)) {
      issue(
        ["result"],
        "Only a generated proposal can own projection or sizing sidecars.",
      )
    }
    if (analysis.result.kind === "generated") {
      const generated = analysis.result
      if (
        projection &&
        (projection.proposalId !== generated.proposalId ||
          projection.derivationDigest !== generated.derivationDigest ||
          projection.horizonAt !== generated.horizonAt)
      ) {
        issue(
          ["projection"],
          "The outcome projection is not bound to the exact generated proposal.",
        )
      }
      if (
        sizing &&
        (sizing.proposalId !== generated.proposalId ||
          sizing.derivationDigest !== generated.derivationDigest)
      ) {
        issue(
          ["sizing"],
          "The sizing projection is not bound to the exact generated proposal.",
        )
      }
      if (projection && analysis.evidence.market) {
        if (!sameMoney(projection.mark, analysis.evidence.market.price)) {
          issue(
            ["projection", "mark"],
            "The outcome projection uses a different retained market mark.",
          )
        }
        const ranges = [
          [projection.downside.priceRange, generated.priceLadder.ranges.downside],
          [projection.base.priceRange, generated.priceLadder.ranges.base],
          [projection.upside.priceRange, generated.priceLadder.ranges.upside],
        ] as const
        if (ranges.some(([left, right]) => !samePriceRange(left, right))) {
          issue(
            ["projection"],
            "The outcome projection does not retain the generated price ranges.",
          )
        }
      }
    }

    if (realized?.kind === "completed") {
      if (analysis.result.kind === "unavailable") {
        issue(
          ["realizedOutcome"],
          "An unavailable analysis cannot have a completed realized outcome.",
        )
      }
      if (
        analysis.evidence.market &&
        !sameMoney(realized.startMark, analysis.evidence.market.price)
      ) {
        issue(
          ["realizedOutcome", "startMark"],
          "The realized outcome does not use the proposal's retained start mark.",
        )
      }
      if (realized.startMark.currency !== realized.endpointPrice.currency) {
        issue(
          ["realizedOutcome", "endpointPrice", "currency"],
          "The realized outcome endpoint uses a different currency.",
        )
      }
    }
  })

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

const recommendationTrackRecordRequestSchema = z
  .object({
    profileId: operationIdentifierSchema,
    profileRevision: positiveU32Schema,
    profileDigest: nonzeroDigestSchema,
    horizonNanos: positiveI64Schema,
    evaluatedAtUnixNanos: signedI64Schema,
  })
  .strict()

const recommendationTrackRecordPerformanceSchema = z.union([
  z
    .object({
      kind: z.literal("unavailable"),
      reason: z.literal("no_due_outcomes"),
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      reason: z.literal("insufficient_completed_samples"),
      required: z.literal(MINIMUM_TRACK_RECORD_SAMPLES),
      actual: nonnegativeU32Schema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      reason: z.literal("insufficient_coverage"),
      requiredPpm: z.literal(MINIMUM_TRACK_RECORD_COVERAGE_PPM),
      actualPpm: ppmSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("available"),
      metric: z.literal("mean_gross_instrument_price_return"),
      meanGrossPriceReturn: canonicalDecimalSchema,
      positiveOutcomes: nonnegativeU32Schema,
      zeroOutcomes: nonnegativeU32Schema,
      negativeOutcomes: nonnegativeU32Schema,
    })
    .strict(),
])

const recommendationTrackRecordGroupSchema = z
  .object({
    cohort: z.enum(TRACK_RECORD_COHORTS),
    publicationCount: nonnegativeU32Schema,
    dueCount: nonnegativeU32Schema,
    completedCount: nonnegativeU32Schema,
    pendingCount: nonnegativeU32Schema,
    unavailableCount: nonnegativeU32Schema,
    coveragePpm: ppmSchema,
    performance: recommendationTrackRecordPerformanceSchema,
  })
  .strict()
  .superRefine((group, context) => {
    const issue = (path: (string | number)[], message: string) => {
      context.addIssue({ code: "custom", path, message })
    }
    if (
      group.publicationCount !==
      group.completedCount + group.pendingCount + group.unavailableCount
    ) {
      issue(
        ["publicationCount"],
        "The track-record publication count contradicts its outcome counts.",
      )
    }
    if (
      group.completedCount + group.unavailableCount > group.dueCount ||
      group.dueCount > group.publicationCount
    ) {
      issue(
        ["dueCount"],
        "The track-record due count contradicts the retained outcome counts.",
      )
    }
    if (group.dueCount === 0 && group.coveragePpm !== 0) {
      issue(
        ["coveragePpm"],
        "A cohort without due outcomes cannot report completed-outcome coverage.",
      )
    }

    const performance = group.performance
    if (performance.kind === "available") {
      if (
        group.completedCount < MINIMUM_TRACK_RECORD_SAMPLES ||
        group.coveragePpm < MINIMUM_TRACK_RECORD_COVERAGE_PPM ||
        performance.positiveOutcomes +
          performance.zeroOutcomes +
          performance.negativeOutcomes !==
          group.completedCount
      ) {
        issue(
          ["performance"],
          "The available track-record summary contradicts its server-owned sample gates.",
        )
      }
      return
    }

    switch (performance.reason) {
      case "no_due_outcomes":
        if (group.dueCount !== 0) {
          issue(
            ["performance"],
            "The cohort reports no due outcomes despite a nonzero due count.",
          )
        }
        break
      case "insufficient_completed_samples":
        if (
          performance.actual !== group.completedCount ||
          group.dueCount === 0 ||
          group.completedCount >= MINIMUM_TRACK_RECORD_SAMPLES
        ) {
          issue(
            ["performance"],
            "The insufficient-sample disclosure contradicts the retained counts.",
          )
        }
        break
      case "insufficient_coverage":
        if (
          performance.actualPpm !== group.coveragePpm ||
          group.completedCount < MINIMUM_TRACK_RECORD_SAMPLES ||
          group.coveragePpm >= MINIMUM_TRACK_RECORD_COVERAGE_PPM
        ) {
          issue(
            ["performance"],
            "The insufficient-coverage disclosure contradicts the retained coverage.",
          )
        }
        break
    }
  })

export const recommendationTrackRecordSchema = z
  .object({
    analyticalProfile: analyticalProfileSchema,
    horizonNanos: positiveI64Schema,
    evaluatedAt: signedI64Schema,
    analysisUnavailableCount: nonnegativeU32Schema,
    minimumCompletedSamples: z.literal(MINIMUM_TRACK_RECORD_SAMPLES),
    minimumCoveragePpm: z.literal(MINIMUM_TRACK_RECORD_COVERAGE_PPM),
    groups: z.array(recommendationTrackRecordGroupSchema).length(TRACK_RECORD_COHORTS.length),
    forecastCalibrationIncluded: z.literal(false),
    executionPerformanceIncluded: z.literal(false),
  })
  .strict()
  .superRefine((trackRecord, context) => {
    trackRecord.groups.forEach((group, index) => {
      if (group.cohort !== TRACK_RECORD_COHORTS[index]) {
        context.addIssue({
          code: "custom",
          path: ["groups", index, "cohort"],
          message: "The track-record cohorts are not in the server-owned canonical order.",
        })
      }
    })
  })

const recommendationTrackRecordEnvelopeSchema = z
  .object({
    data: recommendationTrackRecordSchema,
    metadata: z
      .object({
        completeness: z.literal("complete"),
        returnedItems: z.literal(TRACK_RECORD_COHORTS.length),
        availableItems: z.literal(TRACK_RECORD_COHORTS.length),
        sourceCoverage: notApplicableSchema,
        dataQuality: notApplicableSchema,
      })
      .strict(),
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
export type RecommendationTrackRecord = z.infer<typeof recommendationTrackRecordSchema>
export type RecommendationTrackRecordRequest = z.infer<
  typeof recommendationTrackRecordRequestSchema
>
export type RecommendationTrackRecordRequestAvailability =
  | { kind: "available"; request: RecommendationTrackRecordRequest }
  | {
      kind: "unavailable"
      reason:
        | "analysis_not_published"
        | "profile_digest_algorithm_unsupported"
        | "profile_identifier_unsupported"
    }
export type SavedScreenProduct = z.infer<typeof savedScreenProductSchema>

export function admittedSavedScreenId(value: string | null): string | null {
  if (value === null) return null
  const parsed = savedScreenIdSchema.safeParse(value)
  return parsed.success ? parsed.data : null
}

export function parseSavedScreenProduct(
  result: ApplicationResult,
  expectedScreenId: string,
): SavedScreenProduct {
  const parsed = savedScreenProductEnvelopeSchema.safeParse(result)
  const expected = savedScreenIdSchema.safeParse(expectedScreenId)
  if (
    !parsed.success ||
    !expected.success ||
    parsed.data.data.destination.screenId !== expected.data
  ) {
    throw new Error("The installed service returned an unsupported saved screen.")
  }
  return parsed.data.data
}

export function recommendationTrackRecordRequestForAnalysis(
  analysis: InvestmentAnalysis,
  evaluatedAtUnixNanos: string,
): RecommendationTrackRecordRequestAvailability {
  const profile = analysis.publication?.analyticalProfile
  if (!profile) return { kind: "unavailable", reason: "analysis_not_published" }
  if (profile.contentDigest.algorithm !== "sha256") {
    return {
      kind: "unavailable",
      reason: "profile_digest_algorithm_unsupported",
    }
  }
  const request = recommendationTrackRecordRequestSchema.safeParse({
    profileId: profile.profileId,
    profileRevision: profile.revision,
    profileDigest: profile.contentDigest.digest,
    horizonNanos: analysis.policy.horizonNanos,
    evaluatedAtUnixNanos,
  })
  if (!request.success) {
    return { kind: "unavailable", reason: "profile_identifier_unsupported" }
  }
  return { kind: "available", request: request.data }
}

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

export function parseRecommendationTrackRecord(
  result: ApplicationResult,
  request: RecommendationTrackRecordRequest,
): RecommendationTrackRecord {
  const parsed = recommendationTrackRecordEnvelopeSchema.safeParse(result)
  const expected = recommendationTrackRecordRequestSchema.safeParse(request)
  if (!parsed.success || !expected.success) {
    throw new Error("The installed service returned an unsupported recommendation track record.")
  }
  const trackRecord = parsed.data.data
  if (
    trackRecord.analyticalProfile.profileId !== expected.data.profileId ||
    trackRecord.analyticalProfile.revision !== expected.data.profileRevision ||
    trackRecord.analyticalProfile.contentDigest.algorithm !== "sha256" ||
    trackRecord.analyticalProfile.contentDigest.digest !== expected.data.profileDigest ||
    trackRecord.horizonNanos !== expected.data.horizonNanos ||
    trackRecord.evaluatedAt !== expected.data.evaluatedAtUnixNanos
  ) {
    throw new Error(
      "The installed service returned a recommendation track record for a different binding.",
    )
  }
  return trackRecord
}
