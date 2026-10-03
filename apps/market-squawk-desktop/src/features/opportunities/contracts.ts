import { chartDisplaySchema, chartOriginalPointFields } from "../shared/chart-projection"
import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"
import {
  productLookupActions,
  productLookupCategory,
} from "@/lib/transport"

const MAXIMUM_PAGE_ANALYSES = 1_000
const MAXIMUM_AVAILABLE_ANALYSES = 4_096
const MAXIMUM_U32 = 4_294_967_295
const MINIMUM_TRACK_RECORD_SAMPLES = 30
const MINIMUM_TRACK_RECORD_COVERAGE_PERCENT = "80"

const actionTokenSchema = z
  .string()
  .regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  )
  .refine(
    (value) => value !== "00000000-0000-0000-0000-000000000000",
    "Expected an opaque product action token.",
  )
const canonicalDecimalSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/)
  .refine((value) => value !== "-0", "Expected a normalized exact decimal.")
const canonicalRfc3339Schema = z
  .string()
  .regex(
    /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$/,
  )
  .refine(isValidCanonicalRfc3339, "Expected a canonical UTC timestamp.")
const positiveDecimalSchema = canonicalDecimalSchema.refine(
  (value) => value !== "0" && !value.startsWith("-"),
  "Expected a positive exact amount.",
)
const percentageSchema = canonicalDecimalSchema.refine(
  (value) =>
    !value.startsWith("-") && comparePositiveDecimals(value, "100") <= 0,
  "Expected an exact percentage between 0 and 100.",
)
const canonicalIntegerSchema = losslessIntegerSchema.refine(
  (value) => /^(?:0|-?[1-9]\d*)$/.test(value),
  "Expected a canonical lossless integer.",
)
const nonnegativeIntegerSchema = canonicalIntegerSchema.refine(
  (value) => BigInt(value) >= 0n,
  "Expected a nonnegative integer.",
)
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const nonnegativeU32Schema = z.number().int().min(0).max(MAXIMUM_U32)
const positiveU32Schema = z.number().int().min(1).max(MAXIMUM_U32)
const productTextSchema = z.string().trim().min(1).max(2_048)
const unavailableSummarySchema = z.object({
  state: z.literal("unavailable"),
  summary: productTextSchema,
}).strict()
const studyQualificationSchema = z.object({
  basis: z.enum(["historical_as_known", "retrospective_frozen_snapshot"]),
  limitations: z.array(productTextSchema).max(4),
  summary: productTextSchema,
}).strict()
const savedScreenIdSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-z][a-z0-9._-]*$/)

const moneySchema = z
  .object({ amount: positiveDecimalSchema, currency: currencySchema })
  .strict()

const signedMoneySchema = z.object({
  amount: canonicalDecimalSchema,
  currency: currencySchema,
}).strict()

const priceRangeSchema = z
  .object({ lower: moneySchema, upper: moneySchema })
  .strict()
  .superRefine((value, context) => {
    if (
      value.lower.currency !== value.upper.currency ||
      comparePositiveDecimals(value.lower.amount, value.upper.amount) > 0
    ) {
      context.addIssue({ code: "custom", message: "The price range is inconsistent." })
    }
  })

const recommendationActionSchema = z.enum(["buy", "add", "hold", "trim", "sell"])

const recommendationSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("action"),
      action: recommendationActionSchema,
      summary: productTextSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("abstain"),
      summary: productTextSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      summary: productTextSchema,
    })
    .strict(),
])

const horizonSchema = z
  .object({
    informationCurrentThrough: canonicalRfc3339Schema,
    endsAt: canonicalRfc3339Schema,
    expiresAt: canonicalRfc3339Schema,
  })
  .strict()
  .superRefine((value, context) => {
    if (
      value.informationCurrentThrough > value.endsAt ||
      value.informationCurrentThrough > value.expiresAt
    ) {
      context.addIssue({
        code: "custom",
        message: "The investment horizon precedes its saved information cutoff.",
      })
    }
  })

const scenarioRangesSchema = z
  .object({
    endsAt: canonicalRfc3339Schema,
    downside: priceRangeSchema,
    base: priceRangeSchema,
    upside: priceRangeSchema,
  })
  .strict()
  .superRefine((value, context) => {
    if (
      !strictlyIncreasingMoney([
        value.downside.lower,
        value.downside.upper,
        value.base.lower,
        value.base.upper,
        value.upside.lower,
        value.upside.upper,
      ])
    ) {
      context.addIssue({
        code: "custom",
        message: "The forecast price ranges are not strictly ordered.",
      })
    }
  })

const actionRangesSchema = z
  .object({
    entry: priceRangeSchema,
    add: priceRangeSchema,
    trim: priceRangeSchema,
    exit: priceRangeSchema,
  })
  .strict()
  .superRefine((value, context) => {
    if (
      !strictlyIncreasingMoney([
        value.exit.lower,
        value.exit.upper,
        value.add.lower,
        value.add.upper,
        value.entry.lower,
        value.entry.upper,
      ]) ||
      !strictlyIncreasingMoney([value.trim.lower, value.trim.upper])
    ) {
      context.addIssue({
        code: "custom",
        message: "The action price ranges are not strictly ordered.",
      })
    }
  })

const valuationTimestampSchema = z.string().min(1).max(20).regex(/^(?:0|-?[1-9]\d*)$/)

function valuationMethodSchema<const Method extends string>(method: Method) {
  return z.discriminatedUnion("status", [
    z.object({ method: z.literal(method), status: z.literal("unavailable"), summary: productTextSchema }).strict(),
    z.object({
      method: z.literal(method),
      status: z.literal("calculated"),
      basis: z.enum(["per_instrument_unit", "total_common_equity", "reporting_entity_total", "position_total"]),
      lower: signedMoneySchema,
      central: signedMoneySchema,
      upper: signedMoneySchema,
      recommendationUse: z.enum(["selected", "not_per_instrument_unit", "share_unit_basis_unproven", "another_method_selected", "admission_unavailable"]),
      terminalGrowth: method === "discounted_cash_flow" ? z.object({
        uncapped: canonicalDecimalSchema, riskFreeCap: canonicalDecimalSchema, applied: canonicalDecimalSchema,
      }).strict() : z.null(),
      residualTerminal: method === "residual_income" ? z.object({
        condition: productTextSchema, explicitPeriods: positiveU32Schema,
        continuingValueSensitivity: canonicalDecimalSchema,
      }).strict() : z.null(),
    }).strict(),
  ])
}

const valuationMethodSetSchema = z.object({
  sourceCutoffUnixNanos: valuationTimestampSchema,
  marketCutoffUnixNanos: valuationTimestampSchema,
  completedAtUnixNanos: valuationTimestampSchema,
  methods: z.tuple([
    valuationMethodSchema("discounted_cash_flow"),
    valuationMethodSchema("comparable_companies"),
    valuationMethodSchema("residual_income"),
    valuationMethodSchema("forecast_distribution"),
  ]),
}).strict()

const priceSummarySchema = z
  .object({
    current: moneySchema.nullable(),
    fairValue: moneySchema.nullable(),
    valuationMethods: valuationMethodSetSchema.nullable(),
    scenarios: scenarioRangesSchema.nullable(),
    actionRanges: actionRangesSchema.nullable(),
  })
  .strict()

const coverageKinds = [
  "current_market",
  "broader_research",
  "price_pattern",
  "forecast",
  "financial_model",
  "valuation",
  "historical_test",
  "out_of_sample",
  "liquidity",
  "portfolio_risk",
] as const

const coverageSchema = z
  .object({
    availableCount: z.number().int().min(0).max(coverageKinds.length),
    possibleCount: z.literal(coverageKinds.length),
    items: z
      .array(
        z
          .object({
            kind: z.enum(coverageKinds),
            state: z.enum(["available", "unavailable"]),
          })
          .strict(),
      )
      .length(coverageKinds.length),
    summary: productTextSchema,
  })
  .strict()
  .superRefine((value, context) => {
    const availableCount = value.items.filter(
      (item) => item.state === "available",
    ).length
    if (
      value.availableCount !== availableCount ||
      value.items.some((item, index) => item.kind !== coverageKinds[index])
    ) {
      context.addIssue({
        code: "custom",
        message: "The evidence-coverage summary is internally inconsistent.",
      })
    }
  })

const calibrationSchema = z.discriminatedUnion("state", [
  z
    .object({
      state: z.literal("available"),
      nominalCoveragePercent: percentageSchema,
      realizedCoveragePercent: percentageSchema,
      completedOutcomes: positiveU32Schema,
      summary: productTextSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("unavailable"),
      summary: productTextSchema,
    })
    .strict(),
])

const historicalTestSchema = z
  .object({
    netReturnPercent: canonicalDecimalSchema,
    maximumDrawdownPercent: percentageSchema,
    observations: positiveU32Schema,
    trials: positiveU32Schema,
    stabilityPercent: percentageSchema,
    evaluatedThrough: canonicalRfc3339Schema,
    studyQualification: studyQualificationSchema,
    summary: productTextSchema,
  })
  .strict()

const costSummarySchema = z.discriminatedUnion("state", [
  z
    .object({
      state: z.literal("modeled"),
      feePercent: percentageSchema,
      slippagePercent: percentageSchema,
      maximumRandomSlippagePercent: percentageSchema,
      summary: productTextSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("unavailable"),
      summary: productTextSchema,
    })
    .strict(),
])

const uncertaintyKinds = [
  "forecast_calibration",
  "valuation_agreement",
  "backtest_stability",
  "market_integrity",
  "liquidity_capacity",
  "portfolio_risk_capacity",
] as const

const liquidityReliabilityReasons = [
  "buy_add_capacity_unavailable",
  "trim_sell_capacity_unavailable",
  "action_side_not_established",
] as const
const policyWeightSchema = z.number().int().min(0).max(1_000_000)
const uncertaintyComponentSchema = z.discriminatedUnion("state", [
  z.object({
    kind: z.enum(uncertaintyKinds),
    state: z.literal("available"),
    reliabilityPercent: percentageSchema,
    configuredWeightPpm: policyWeightSchema,
    reason: z.null(),
  }).strict(),
  z.object({
    kind: z.literal("liquidity_capacity"),
    state: z.literal("unavailable"),
    reliabilityPercent: z.null(),
    configuredWeightPpm: policyWeightSchema,
    reason: z.enum(liquidityReliabilityReasons),
  }).strict(),
  z.object({
    kind: z.literal("liquidity_capacity"),
    state: z.literal("not_applicable"),
    reliabilityPercent: z.null(),
    configuredWeightPpm: policyWeightSchema,
    reason: z.null(),
  }).strict(),
])
const uncertaintyComponentsSchema = z.array(uncertaintyComponentSchema)
  .length(uncertaintyKinds.length)
  .superRefine((components, context) => {
    if (components.some((component, index) => component.kind !== uncertaintyKinds[index])) {
      context.addIssue({
        code: "custom",
        message: "Evidence-reliability components are not in canonical order.",
      })
    }
  })
const uncertaintySchema = z.union([
  z.object({
    state: z.literal("available"),
    evidenceReliabilityPercent: percentageSchema,
    reason: z.null(),
    applicablePolicyWeightPpm: z.number().int().min(1).max(1_000_000),
    components: uncertaintyComponentsSchema,
    studyQualification: studyQualificationSchema,
    summary: productTextSchema,
  }).strict(),
  z.object({
    state: z.literal("unavailable"),
    evidenceReliabilityPercent: z.null(),
    reason: z.enum([...liquidityReliabilityReasons, "no_applicable_policy_weight"]),
    applicablePolicyWeightPpm: policyWeightSchema,
    components: uncertaintyComponentsSchema,
    studyQualification: studyQualificationSchema,
    summary: productTextSchema,
  }).strict(),
  unavailableSummarySchema,
])

const evidenceSummarySchema = z
  .object({
    coverage: coverageSchema,
    calibration: calibrationSchema,
    outOfSample: z.discriminatedUnion("state", [
      z.object({
        state: z.literal("available"),
        completedObservations: positiveU32Schema,
        totalSignals: positiveU32Schema,
        folds: positiveU32Schema,
        completionCoveragePercent: percentageSchema,
        evaluatedFrom: canonicalRfc3339Schema,
        evaluatedThrough: canonicalRfc3339Schema,
        studyQualification: studyQualificationSchema,
        summary: productTextSchema,
      }).strict(),
      unavailableSummarySchema,
    ]),
    historicalTest: historicalTestSchema.nullable(),
    costs: costSummarySchema,
    uncertainty: uncertaintySchema,
  })
  .strict()

const signedMoneyRangeSchema = z.object({
  lower: signedMoneySchema,
  upper: signedMoneySchema,
}).strict().superRefine((range, context) => {
  if (range.lower.currency !== range.upper.currency
    || compareCanonicalDecimals(range.lower.amount, range.upper.amount) > 0) {
    context.addIssue({ code: "custom", message: "The signed money range is inconsistent." })
  }
})
const grossPricePnlSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"),
    range: signedMoneyRangeSchema,
    summary: productTextSchema,
  }).strict(),
  unavailableSummarySchema,
])

const exactFinancialRatioSchema = z.object({ numerator: signedMoneySchema, denominator: moneySchema })
  .strict().superRefine((ratio, context) => {
    if (ratio.numerator.currency !== ratio.denominator.currency) {
      context.addIssue({ code: "custom", message: "The exact price-return ratio mixes currencies." })
    }
  })
const exactRatioRangeSchema = z.object({ lower: exactFinancialRatioSchema, upper: exactFinancialRatioSchema }).strict()
const expectedReturnSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), metric: z.literal("expected_gross_price_return"),
    basis: z.literal("admitted_conditional_mean_terminal_price"),
    grossPriceReturnPercent: canonicalDecimalSchema.nullable(), exactRatio: exactFinancialRatioSchema,
    summary: productTextSchema,
  }).strict(), unavailableSummarySchema,
])
const zoneDistanceSchema = z.object({
  priceRange: priceRangeSchema, absolutePriceChange: signedMoneyRangeSchema,
  exactPriceReturnRatio: exactRatioRangeSchema,
}).strict()

const priceChangeRangeSchema = z
  .object({
    priceRange: priceRangeSchema,
    absolutePriceChange: signedMoneyRangeSchema,
    grossPricePnl: grossPricePnlSchema,
    exactPriceReturnRatio: exactRatioRangeSchema,
    priceChangePercent: z
      .object({
        lower: canonicalDecimalSchema,
        upper: canonicalDecimalSchema,
      })
      .strict()
      .superRefine((value, context) => {
        if (compareCanonicalDecimals(value.lower, value.upper) > 0) {
          context.addIssue({ code: "custom", message: "The return range is reversed." })
        }
      })
      .optional(),
  })
  .strict()

const outcomeProjectionSchema = z
  .object({
    startingPrice: moneySchema,
    endsAt: canonicalRfc3339Schema,
    positionScale: z.object({
      quantityLots: nonnegativeIntegerSchema,
      summary: productTextSchema,
    }).strict().nullable(),
    downside: priceChangeRangeSchema,
    base: priceChangeRangeSchema,
    upside: priceChangeRangeSchema,
    entryDistance: zoneDistanceSchema, addDistance: zoneDistanceSchema,
    trimDistance: zoneDistanceSchema, exitDistance: zoneDistanceSchema,
    expectedReturn: expectedReturnSchema,
    expectedGrossPricePnl: z.discriminatedUnion("state", [
      z.object({
        state: z.literal("available"),
        amount: signedMoneySchema,
        summary: productTextSchema,
      }).strict(),
      unavailableSummarySchema,
    ]),
    netPnl: unavailableSummarySchema,
    benchmarkReturn: unavailableSummarySchema,
    afterTaxPnl: unavailableSummarySchema,
    limitations: z.array(productTextSchema).min(1).max(8),
  })
  .strict()
  .superRefine((value, context) => {
    if (
      !strictlyIncreasingMoney([
        value.downside.priceRange.lower,
        value.downside.priceRange.upper,
        value.base.priceRange.lower,
        value.base.priceRange.upper,
        value.upside.priceRange.lower,
        value.upside.priceRange.upper,
      ])
    ) {
      context.addIssue({
        code: "custom",
        message: "The projected price ranges are not strictly ordered.",
      })
    }
  })

const lotRangeSchema = z.union([
  z
    .object({
      kind: z.literal("available"),
      lower: nonnegativeIntegerSchema,
      upper: nonnegativeIntegerSchema,
    })
    .strict()
    .superRefine((value, context) => {
      if (BigInt(value.lower) > BigInt(value.upper)) {
        context.addIssue({ code: "custom", message: "The lot range is reversed." })
      }
    }),
  z
    .object({
      kind: z.literal("unavailable"),
      reasons: z.array(productTextSchema).min(1).max(8),
    })
    .strict(),
])

const nonnegativeMoneySchema = z.object({
  amount: canonicalDecimalSchema.refine((value) => !value.startsWith("-")), currency: currencySchema,
}).strict()
const sizingKinds = ["cash_reserve", "downside_loss", "liquidity", "portfolio_risk", "forward_cost", "preferred_weight"] as const
const notionalRangeSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("available"), lower: nonnegativeMoneySchema, upper: nonnegativeMoneySchema }).strict(),
  z.object({ kind: z.literal("unavailable"), reasons: z.array(productTextSchema).min(1).max(8) }).strict(),
]).superRefine((range, context) => {
  if (range.kind === "available" && (range.lower.currency !== range.upper.currency
    || comparePositiveDecimals(range.lower.amount, range.upper.amount) > 0)) {
    context.addIssue({ code: "custom", message: "The target notional range is inconsistent." })
  }
})
const sizingCapSchema = z.discriminatedUnion("state", [
  z.object({ kind: z.enum(sizingKinds), state: z.literal("available"), lower: nonnegativeIntegerSchema, upper: nonnegativeIntegerSchema }).strict(),
  z.object({ kind: z.enum(sizingKinds), state: z.literal("unavailable"), summary: productTextSchema }).strict(),
]).superRefine((cap, context) => {
  if (cap.state === "available" && BigInt(cap.lower) > BigInt(cap.upper)) {
    context.addIssue({ code: "custom", message: "The sizing constraint range is reversed." })
  }
})
const sizingSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("evaluated"), evaluatedAt: canonicalRfc3339Schema,
    currentLots: nonnegativeIntegerSchema, markedEquity: moneySchema,
    settlementAvailableCash: signedMoneySchema.nullable(), perLotNotional: moneySchema,
    perLotDownsideLoss: nonnegativeMoneySchema, constraintCaps: z.array(sizingCapSchema).length(6),
    hardFeasibleLots: lotRangeSchema, preferredFeasibleLots: lotRangeSchema,
    hardFeasibleTargetNotional: notionalRangeSchema, preferredFeasibleTargetNotional: notionalRangeSchema,
    hardBindingCaps: z.array(z.enum(sizingKinds)).max(5), preferredBindingCaps: z.array(z.enum(sizingKinds)).max(6),
    preferredWeightRounding: z.object({ lowerRoundUpExcess: nonnegativeMoneySchema, upperRoundDownRemainder: nonnegativeMoneySchema }).strict(),
    summary: productTextSchema,
  }).strict(),
  z.object({ state: z.literal("unavailable"),
    reason: z.enum(["no_generated_proposal", "price_not_on_execution_tick", "exact_portfolio_lots_unavailable"]),
    summary: productTextSchema,
  }).strict(),
])

const realizedOutcomeResultSchema = z.discriminatedUnion("kind", [
  z
    .object({ kind: z.literal("pending"), summary: productTextSchema })
    .strict(),
  z
    .object({ kind: z.literal("unavailable"), summary: productTextSchema })
    .strict(),
  z
    .object({
      kind: z.literal("completed"),
      metric: z.literal("gross_instrument_price_return"),
      startMark: moneySchema,
      endpointPrice: moneySchema,
      grossPriceReturnPercent: canonicalDecimalSchema,
      observedAt: canonicalRfc3339Schema,
      availableAt: canonicalRfc3339Schema,
      limitations: z.array(productTextSchema).min(1).max(8),
    })
    .strict(),
])

const realizedOutcomeSchema = z
  .object({
    evaluatedAt: canonicalRfc3339Schema,
    result: realizedOutcomeResultSchema,
  })
  .strict()

type ProductMoney = z.infer<typeof moneySchema>
type ProductPriceRange = z.infer<typeof priceRangeSchema>

function isValidCanonicalRfc3339(value: string): boolean {
  const year = Number(value.slice(0, 4))
  const month = Number(value.slice(5, 7))
  const day = Number(value.slice(8, 10))
  const hour = Number(value.slice(11, 13))
  const minute = Number(value.slice(14, 16))
  const second = Number(value.slice(17, 19))
  if (
    month < 1 ||
    month > 12 ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return false
  }
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
  const maximumDay = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][
    month - 1
  ]
  return maximumDay !== undefined && day >= 1 && day <= maximumDay
}

function comparePositiveDecimals(left: string, right: string): number {
  const [leftWhole = "0", leftFraction = ""] = left.split(".")
  const [rightWhole = "0", rightFraction = ""] = right.split(".")
  if (leftWhole.length !== rightWhole.length) {
    return leftWhole.length < rightWhole.length ? -1 : 1
  }
  const wholeComparison = leftWhole < rightWhole ? -1 : leftWhole > rightWhole ? 1 : 0
  if (wholeComparison !== 0) return wholeComparison
  const maximumFraction = Math.max(leftFraction.length, rightFraction.length)
  const normalizedLeft = leftFraction.padEnd(maximumFraction, "0")
  const normalizedRight = rightFraction.padEnd(maximumFraction, "0")
  return normalizedLeft < normalizedRight ? -1 : normalizedLeft > normalizedRight ? 1 : 0
}

function compareCanonicalDecimals(left: string, right: string): number {
  const leftNegative = left.startsWith("-")
  const rightNegative = right.startsWith("-")
  if (leftNegative !== rightNegative) return leftNegative ? -1 : 1
  const magnitude = comparePositiveDecimals(
    leftNegative ? left.slice(1) : left,
    rightNegative ? right.slice(1) : right,
  )
  return leftNegative ? -magnitude : magnitude
}

function exactCoveragePercent(completed: number, due: number): string {
  if (due === 0) return "0"
  const partsPerMillion =
    (BigInt(completed) * 1_000_000n) / BigInt(due)
  const whole = partsPerMillion / 10_000n
  const fractional = (partsPerMillion % 10_000n)
    .toString()
    .padStart(4, "0")
    .replace(/0+$/, "")
  return fractional ? `${whole}.${fractional}` : whole.toString()
}

function trackRecordGateIssue(
  context: {
    addIssue: (issue: {
      code: "custom"
      path: (string | number)[]
      message: string
    }) => void
  },
  index: number,
): void {
  context.addIssue({
    code: "custom",
    path: ["groups", index, "performance"],
    message: "Comparable-history performance does not match its evidence gate.",
  })
}

function rangeMoney(range: ProductPriceRange): ProductMoney[] {
  return [range.lower, range.upper]
}

function sameMoney(left: ProductMoney, right: ProductMoney): boolean {
  return left.amount === right.amount && left.currency === right.currency
}

function sameRange(left: ProductPriceRange, right: ProductPriceRange): boolean {
  return sameMoney(left.lower, right.lower) && sameMoney(left.upper, right.upper)
}

function strictlyIncreasingMoney(values: ProductMoney[]): boolean {
  const currency = values[0]?.currency
  return (
    currency !== undefined &&
    values.every((value) => value.currency === currency) &&
    values.slice(1).every(
      (value, index) =>
        comparePositiveDecimals(values[index]?.amount ?? "0", value.amount) < 0,
    )
  )
}

function analysisMoney(analysis: {
  priceSummary: z.infer<typeof priceSummarySchema>
  outcomeProjection: z.infer<typeof outcomeProjectionSchema> | null
  realizedOutcome: z.infer<typeof realizedOutcomeSchema> | null
}): ProductMoney[] {
  const values: ProductMoney[] = []
  if (analysis.priceSummary.current) values.push(analysis.priceSummary.current)
  if (analysis.priceSummary.fairValue) values.push(analysis.priceSummary.fairValue)
  if (analysis.priceSummary.scenarios) {
    values.push(
      ...rangeMoney(analysis.priceSummary.scenarios.downside),
      ...rangeMoney(analysis.priceSummary.scenarios.base),
      ...rangeMoney(analysis.priceSummary.scenarios.upside),
    )
  }
  if (analysis.priceSummary.actionRanges) {
    values.push(
      ...rangeMoney(analysis.priceSummary.actionRanges.entry),
      ...rangeMoney(analysis.priceSummary.actionRanges.add),
      ...rangeMoney(analysis.priceSummary.actionRanges.trim),
      ...rangeMoney(analysis.priceSummary.actionRanges.exit),
    )
  }
  if (analysis.outcomeProjection) {
    const projection = analysis.outcomeProjection
    values.push(
      projection.startingPrice,
      ...rangeMoney(projection.downside.priceRange),
      ...rangeMoney(projection.base.priceRange),
      ...rangeMoney(projection.upside.priceRange),
    )
    for (const scenario of [projection.downside, projection.base, projection.upside]) {
      values.push(...rangeMoney(scenario.absolutePriceChange),
        scenario.exactPriceReturnRatio.lower.numerator,
        scenario.exactPriceReturnRatio.lower.denominator,
        scenario.exactPriceReturnRatio.upper.numerator,
        scenario.exactPriceReturnRatio.upper.denominator)
      if (scenario.grossPricePnl.state === "available") {
        values.push(...rangeMoney(scenario.grossPricePnl.range))
      }
    }
    for (const distance of [projection.entryDistance, projection.addDistance,
      projection.trimDistance, projection.exitDistance]) {
      values.push(...rangeMoney(distance.priceRange), ...rangeMoney(distance.absolutePriceChange),
        distance.exactPriceReturnRatio.lower.numerator,
        distance.exactPriceReturnRatio.lower.denominator,
        distance.exactPriceReturnRatio.upper.numerator,
        distance.exactPriceReturnRatio.upper.denominator)
    }
    if (projection.expectedReturn.state === "available") {
      values.push(projection.expectedReturn.exactRatio.numerator, projection.expectedReturn.exactRatio.denominator)
    }
    if (projection.expectedGrossPricePnl.state === "available") {
      values.push(projection.expectedGrossPricePnl.amount)
    }
  }
  const realized = analysis.realizedOutcome?.result
  if (realized?.kind === "completed") {
    values.push(realized.startMark, realized.endpointPrice)
  }
  return values
}

const evidenceFamilySchema = z.object({
  state: z.enum(["available", "unavailable"]),
  summary: productTextSchema,
}).strict()
const pricePatternEvidenceSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"),
    outcome: z.enum(["pattern_detected", "no_matching_pattern", "pattern_expired", "pattern_invalidated"]),
    summary: productTextSchema,
  }).strict(),
  z.object({
    state: z.literal("unavailable"),
    outcome: z.enum([
      "insufficient_bars", "insufficient_turning_points", "history_unavailable",
      "adjustment_unavailable", "trading_activity_unavailable", "price_precision_unavailable",
      "assessment_unavailable", "not_evaluated",
    ]),
    summary: productTextSchema,
  }).strict(),
])
const analyticalEvidenceSchema = z.object({
  currentMarket: evidenceFamilySchema,
  broaderResearch: evidenceFamilySchema,
  pricePattern: pricePatternEvidenceSchema,
  forecast: evidenceFamilySchema,
  financialModel: evidenceFamilySchema,
  valuation: evidenceFamilySchema,
  historicalTest: evidenceFamilySchema,
  outOfSample: evidenceFamilySchema,
  liquidity: evidenceFamilySchema,
  portfolioRisk: evidenceFamilySchema,
  combination: z.object({
    state: z.enum(["multi_evidence", "insufficient"]),
    summary: productTextSchema,
  }).strict(),
}).strict()
const liquiditySchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"),
    quotedSpreadPercent: canonicalDecimalSchema.refine((value) => !value.startsWith("-")),
    buyAddCapacityPercent: percentageSchema.nullable(),
    trimSellCapacityPercent: percentageSchema.nullable(),
    summary: productTextSchema,
  }).strict(),
  unavailableSummarySchema,
])
const portfolioContextSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"),
    portfolioLabel: z.string().trim().min(1).max(128),
    positionState: z.enum(["no_position", "current_position"]),
    riskCapacityPercent: percentageSchema,
    summary: productTextSchema,
  }).strict(),
  unavailableSummarySchema,
])
const virtualPaperEligibilitySchema = z.object({
  state: z.literal("not_eligible"),
  executionAuthority: z.literal("none"),
  requiresExplicitPaperApproval: z.literal(true),
  requiresFreshRiskCheck: z.literal(true),
  summary: productTextSchema,
}).strict()

const chartValueSchema = positiveDecimalSchema.refine(
  (value) => Number.isFinite(Number(value)),
  "Expected a drawable saved price.",
)
const chartRangeSchema = z.object({ lower: chartValueSchema, upper: chartValueSchema }).strict()
  .superRefine((range, context) => {
    if (comparePositiveDecimals(range.lower, range.upper) > 0) {
      context.addIssue({ code: "custom", message: "The saved price range is reversed." })
    }
  })
const chartTimeSchema = canonicalIntegerSchema
const chartSessionCoordinateSchema = z.object({
  kind: z.literal("session_date"), date: z.iso.date(), sessionCloseUnixNanos: chartTimeSchema,
}).strict()
const chartCoordinateSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("timestamp"), timeUnixNanos: chartTimeSchema }).strict(),
  chartSessionCoordinateSchema,
])
const chartQualitySchema = z.enum(["direct_verified", "direct_unverified", "official_delayed", "aggregated", "indicative", "modeled", "estimated", "stale", "quarantined"])
const forecastOriginSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), basis: z.literal("split_adjusted_price"),
    coordinate: chartCoordinateSchema, value: chartValueSchema,
    quality: chartQualitySchema, summary: productTextSchema,
  }).strict(),
  unavailableSummarySchema,
])
const chartHistoryPointSchema = z.union([
  z.object({
    ...chartOriginalPointFields,
    coordinate: chartCoordinateSchema,
    availableAtUnixNanos: chartTimeSchema,
    value: chartValueSchema,
    quality: chartQualitySchema,
  }).strict(),
  z.object({
    ...chartOriginalPointFields,
    coordinate: chartCoordinateSchema,
    availableAtUnixNanos: z.null(), value: z.null(), quality: z.null(),
  }).strict(),
])
const chartHistorySchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), summary: productTextSchema,
    basis: z.literal("split_adjusted_price"),
    points: z.array(chartHistoryPointSchema).max(4_096),
    display: chartDisplaySchema,
  }).strict(),
  z.object({
    state: z.literal("unavailable"), summary: productTextSchema,
    basis: z.literal("split_adjusted_price"), points: z.tuple([]),
  }).strict(),
])
const forecastPointSchema = z.object({
  timeUnixNanos: chartTimeSchema, central: chartValueSchema,
  interval50: chartRangeSchema.nullable(), interval80: chartRangeSchema.nullable(),
  interval95: chartRangeSchema.nullable(),
}).strict().superRefine((point, context) => {
  for (const interval of [point.interval50, point.interval80, point.interval95]) {
    if (interval !== null && (comparePositiveDecimals(interval.lower, point.central) > 0
      || comparePositiveDecimals(point.central, interval.upper) > 0)) {
      context.addIssue({ code: "custom", message: "The forecast range does not contain its central price." })
    }
  }
  if (point.interval50 !== null && point.interval80 !== null
    && (comparePositiveDecimals(point.interval80.lower, point.interval50.lower) > 0
      || comparePositiveDecimals(point.interval50.upper, point.interval80.upper) > 0)) {
    context.addIssue({ code: "custom", message: "The calibrated forecast ranges are not nested." })
  }
  if (point.interval80 !== null && point.interval95 !== null
    && (comparePositiveDecimals(point.interval95.lower, point.interval80.lower) > 0
      || comparePositiveDecimals(point.interval80.upper, point.interval95.upper) > 0)) {
    context.addIssue({ code: "custom", message: "The calibrated forecast ranges are not nested." })
  }
})
const chartForecastSchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), summary: productTextSchema,
    basis: z.literal("saved_price_projection"),
    observedThroughUnixNanos: chartTimeSchema, origin: forecastOriginSchema,
    points: z.array(forecastPointSchema).max(1),
  }).strict(),
  z.object({
    state: z.literal("unavailable"), summary: productTextSchema,
    basis: z.literal("saved_price_projection"),
    observedThroughUnixNanos: z.null(), origin: unavailableSummarySchema,
    points: z.tuple([]),
  }).strict(),
])
const benchmarkMemberSchema = z.object({
  instrumentId: actionTokenSchema,
  label: z.string().trim().min(1).max(128),
}).strict()
const benchmarkMembersSchema = z.union([
  z.tuple([
    benchmarkMemberSchema.extend({ role: z.literal("subject") }),
    benchmarkMemberSchema.extend({ role: z.literal("selected") }),
  ]),
  z.tuple([
    benchmarkMemberSchema.extend({ role: z.literal("subject") }),
    benchmarkMemberSchema.extend({ role: z.literal("selected") }),
    benchmarkMemberSchema.extend({ role: z.literal("accompanying") }),
  ]),
])
const unavailableBenchmarkMembersSchema = z.union([
  z.tuple([]),
  z.tuple([benchmarkMemberSchema.extend({ role: z.literal("subject") })]),
  benchmarkMembersSchema,
])
const benchmarkCoordinateSchema = z.object({
  date: z.iso.date(), sessionCloseUnixNanos: chartTimeSchema,
}).strict()
const benchmarkObservationSchema = z.object({
  close: positiveDecimalSchema,
  priceIndex: positiveDecimalSchema.refine((value) => Number.isFinite(Number(value)),
    "Expected a drawable saved price index."),
  availableAtUnixNanos: chartTimeSchema,
  providerCompletedAtUnixNanos: chartTimeSchema.nullable(),
  quality: chartQualitySchema,
}).strict()
const benchmarkBasisSchema = z.literal("split_adjusted_price_index")
const benchmarkHistorySchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), summary: productTextSchema,
    basis: benchmarkBasisSchema, members: benchmarkMembersSchema,
    baseline: benchmarkCoordinateSchema,
    display: chartDisplaySchema,
    points: z.array(z.object({
      ...chartOriginalPointFields,
      coordinate: benchmarkCoordinateSchema,
      observations: z.array(benchmarkObservationSchema.nullable()).min(2).max(3),
    }).strict()).max(4_096),
  }).strict(),
  z.object({
    state: z.literal("unavailable"), summary: productTextSchema,
    basis: benchmarkBasisSchema, members: unavailableBenchmarkMembersSchema,
    reason: z.enum([
      "selection_unavailable", "missing_subject", "missing_selected_comparison",
      "no_common_observation", "storage_unavailable", "integrity_unproven", "not_requested",
    ]),
  }).strict(),
]).superRefine((benchmark, context) => {
  if (benchmark.state !== "available") {
    if (benchmark.members.length === 0 && benchmark.reason !== "not_requested") {
      context.addIssue({ code: "custom", message: "Unavailable comparison history lacks its saved identity." })
    }
    if (benchmark.members.length === 1 && benchmark.reason !== "selection_unavailable" && benchmark.reason !== "not_requested") {
      context.addIssue({ code: "custom", message: "Unavailable comparison history lacks its saved selection." })
    }
    return
  }
  // A viewport may start after the authoritative base-100 session. The saved
  // baseline remains separate from its displayed original observations.
  if (benchmark.display.returnedPointCount !== benchmark.points.length) {
    context.addIssue({ code: "custom", message: "The comparison display count does not match its original points." })
  }
  benchmark.points.forEach((point, index) => {
    if (point.observations.length !== benchmark.members.length
      || point.breakBefore.length !== benchmark.members.length
      || index > 0 && BigInt(point.originalOrdinal) <= BigInt(benchmark.points[index - 1]!.originalOrdinal)
      || index > 0 && (point.coordinate.date <= benchmark.points[index - 1]!.coordinate.date
        || BigInt(point.coordinate.sessionCloseUnixNanos) <= BigInt(benchmark.points[index - 1]!.coordinate.sessionCloseUnixNanos))) {
      context.addIssue({ code: "custom", path: ["points", index], message: "Comparison sessions or members are inconsistent." })
    }
  })
})
const chartActionKinds = ["entry", "add", "trim", "exit"] as const
const chartActionRangeSchema = z.object({
  kind: z.enum(chartActionKinds), label: productTextSchema,
  lower: chartValueSchema, upper: chartValueSchema,
  startAtUnixNanos: chartTimeSchema, endAtUnixNanos: chartTimeSchema,
  summary: productTextSchema,
}).strict().superRefine((range, context) => {
  if (comparePositiveDecimals(range.lower, range.upper) > 0
    || BigInt(range.startAtUnixNanos) >= BigInt(range.endAtUnixNanos)) {
    context.addIssue({ code: "custom", message: "The saved action reference range or interval is reversed." })
  }
})
const chartActionClocks = {
  basis: z.literal("split_adjusted_price"), summary: productTextSchema,
  informationCurrentThroughUnixNanos: chartTimeSchema,
  admittedAtUnixNanos: chartTimeSchema, expiresAtUnixNanos: chartTimeSchema,
}
const chartActionRangesSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("available"), ...chartActionClocks,
    ranges: z.array(chartActionRangeSchema).length(chartActionKinds.length),
  }).strict(),
  z.object({ state: z.literal("unavailable"), ...chartActionClocks,
    reason: z.enum(["no_supported_action_ranges", "share_conversion_unavailable",
      "original_history_unavailable", "expired_at_admission", "range_conversion_unavailable", "not_requested"]),
    ranges: z.tuple([]),
  }).strict(),
]).superRefine((levels, context) => {
  if (levels.state === "available" && (BigInt(levels.informationCurrentThroughUnixNanos) > BigInt(levels.admittedAtUnixNanos)
    || levels.ranges.some((range, index) => range.kind !== chartActionKinds[index]
      || range.startAtUnixNanos !== levels.admittedAtUnixNanos || range.endAtUnixNanos !== levels.expiresAtUnixNanos))) {
    context.addIssue({ code: "custom", message: "Saved action references contradict their original admission interval." })
  }
})
export const investmentChartSchema = z.object({
  viewport: z.strictObject({
    startUnixNanos: chartTimeSchema.nullable(),
    endUnixNanos: chartTimeSchema.nullable(),
    pointLimit: z.number().int().min(8).max(4_096),
    layer: z.enum(["all", "history", "forecast", "benchmark", "price_pattern", "action_ranges"]),
    fullStartUnixNanos: chartTimeSchema.nullable(),
    fullEndUnixNanos: chartTimeSchema.nullable(),
  }),
  informationCurrentThroughUnixNanos: chartTimeSchema,
  basisExplanation: productTextSchema,
  history: chartHistorySchema,
  forecast: chartForecastSchema,
  benchmark: benchmarkHistorySchema,
  actionRanges: chartActionRangesSchema,
  pricePattern: z.object({
    status: z.enum(["unavailable", "confirmed", "insufficient_bars", "insufficient_pivots", "no_matching_pattern", "expired", "invalidated"]),
    summary: productTextSchema, basis: z.literal("split_adjusted_price"),
    kind: z.enum(["ab_cd", "gartley", "bat", "butterfly", "crab", "deep_crab", "cypher", "shark"]).nullable(),
    direction: z.enum(["bullish", "bearish"]).nullable(),
    pivots: z.array(z.object({
      name: z.enum(["X", "A", "B", "C", "D"]), kind: z.enum(["high", "low"]),
      observedAtUnixNanos: chartTimeSchema, availableAtUnixNanos: chartTimeSchema,
      confirmedAtUnixNanos: chartTimeSchema, value: chartValueSchema,
    }).strict()).max(5),
    ratios: z.array(z.object({
      name: z.enum(["AB/XA", "BC/AB", "CD/BC", "CD/AB", "AD/XA", "XC/XA", "CD/XC"]),
      numerator: nonnegativeIntegerSchema, denominator: canonicalIntegerSchema.refine((value) => BigInt(value) > 0n),
    }).strict()).max(7),
    reversalZone: chartRangeSchema.nullable(), invalidation: chartValueSchema.nullable(),
    targets: z.array(chartValueSchema).max(3), expiresAtUnixNanos: chartTimeSchema.nullable(),
    observationCutoffUnixNanos: chartTimeSchema.nullable(), confirmationCutoffUnixNanos: chartTimeSchema.nullable(),
    interpretation: z.array(productTextSchema).max(8),
  }).strict(),
}).strict().superRefine((chart, context) => {
  const history = chart.history
  const cutoff = BigInt(chart.informationCurrentThroughUnixNanos)
  const viewport = chart.viewport
  if (viewport.startUnixNanos !== null && viewport.endUnixNanos !== null
    && BigInt(viewport.startUnixNanos) > BigInt(viewport.endUnixNanos)
    || (viewport.fullStartUnixNanos === null) !== (viewport.fullEndUnixNanos === null)
    || viewport.fullStartUnixNanos !== null && viewport.fullEndUnixNanos !== null
      && BigInt(viewport.fullStartUnixNanos) > BigInt(viewport.fullEndUnixNanos)) {
    context.addIssue({ code: "custom", path: ["viewport"], message: "The requested chart interval is reversed or incomplete." })
  }
  for (const series of [chart.history, chart.benchmark]) {
    if (series.state === "available" && series.points.length > viewport.pointLimit) {
      context.addIssue({ code: "custom", path: ["viewport"], message: "The saved chart exceeds its requested display resolution." })
    }
  }
  if (chart.actionRanges.informationCurrentThroughUnixNanos !== chart.informationCurrentThroughUnixNanos) {
    context.addIssue({ code: "custom", path: ["actionRanges"],
      message: "Saved action references contradict their original information cutoff." })
  }
  if (history.state === "available") {
    if (history.display.returnedPointCount !== history.points.length) {
      context.addIssue({ code: "custom", message: "The history display count does not match its original points." })
    }
    history.points.forEach((point, index) => {
      const coordinate = point.coordinate
      const time = coordinate.kind === "timestamp"
        ? coordinate.timeUnixNanos : coordinate.sessionCloseUnixNanos
      const previous = history.points[index - 1]?.coordinate
      const previousTime = previous?.kind === "timestamp"
        ? previous.timeUnixNanos : previous?.sessionCloseUnixNanos
      if (point.breakBefore.length !== 1
        || index > 0 && BigInt(point.originalOrdinal) <= BigInt(history.points[index - 1]!.originalOrdinal)
        || BigInt(time) > cutoff
        || point.availableAtUnixNanos !== null && BigInt(point.availableAtUnixNanos) > cutoff
        || previousTime !== undefined && BigInt(time) <= BigInt(previousTime)
        || coordinate.kind === "session_date" && previous?.kind === "session_date"
          && coordinate.date <= previous.date) {
        context.addIssue({ code: "custom", path: ["history", "points", index],
          message: "Saved price sessions or availability exceed the original information cutoff." })
      }
    })
  }
  const forecast = chart.forecast
  if (forecast.state === "available") {
    if (BigInt(forecast.observedThroughUnixNanos) > cutoff
      || forecast.points[0] !== undefined && BigInt(forecast.points[0].timeUnixNanos) <= BigInt(forecast.observedThroughUnixNanos)) {
      context.addIssue({ code: "custom", path: ["forecast"],
        message: "The saved forecast endpoint or origin contradicts its evidence cutoff." })
    }
    const origin = forecast.origin
    if (origin.state === "available") {
      const originTime = origin.coordinate.kind === "timestamp"
        ? origin.coordinate.timeUnixNanos : origin.coordinate.sessionCloseUnixNanos
      if (originTime !== forecast.observedThroughUnixNanos) {
        context.addIssue({ code: "custom", path: ["forecast", "origin"],
          message: "The original price does not match the saved forecast cutoff." })
      }

    }
  }
  const pattern = chart.pricePattern
  if (pattern.status === "confirmed" ? pattern.kind === null || pattern.direction === null || pattern.pivots.length !== 5
    || pattern.reversalZone === null || pattern.invalidation === null || pattern.expiresAtUnixNanos === null
    || pattern.observationCutoffUnixNanos === null || pattern.confirmationCutoffUnixNanos === null
    : pattern.pivots.length !== 0 || pattern.reversalZone !== null || pattern.invalidation !== null || pattern.targets.length !== 0) {
    context.addIssue({ code: "custom", message: "Pattern geometry must belong to confirmed saved evidence." })
  }
})

const savedBenchmarkIdentitySchema = z.object({
  instrumentId: actionTokenSchema, definitionAlgorithm: z.enum(["sha256", "blake3"]), definitionDigest: z.string().regex(/^[0-9a-f]{64}$/),
}).strict().nullable()
const savedProbabilitySchema = z.discriminatedUnion("state", [
  z.object({
    state: z.literal("available"), probabilityPercent: percentageSchema,
    benchmark: savedBenchmarkIdentitySchema,
    observedAt: canonicalRfc3339Schema, endsAt: canonicalRfc3339Schema,
    expiresAt: canonicalRfc3339Schema, assumptions: z.array(productTextSchema).max(5),
    calibration: z.object({
      evaluatedFrom: canonicalRfc3339Schema, evaluatedThrough: canonicalRfc3339Schema,
      completedOutcomes: positiveU32Schema, brierScore: z.number().min(0).max(1),
      logLoss: z.number().min(0),
    }).strict(),
  }).strict(),
  z.object({ state: z.literal("unavailable"), summary: productTextSchema,
    benchmark: savedBenchmarkIdentitySchema,
    assumptions: z.array(productTextSchema).max(5),
  }).strict(),
]).superRefine((event, context) => {
  if (event.state === "available" && (event.observedAt >= event.endsAt
    || event.observedAt >= event.expiresAt
    || event.calibration.evaluatedFrom >= event.calibration.evaluatedThrough
    || event.calibration.evaluatedThrough > event.observedAt)) {
    context.addIssue({ code: "custom", message: "Saved event probability has inconsistent evidence dates." })
  }
})
const savedProbabilitiesSchema = z.object({
  priceHigher: savedProbabilitySchema, benchmarkOutperformance: savedProbabilitySchema,
  profitAfterCosts: savedProbabilitySchema,
}).strict().superRefine((events, context) => {
  if (events.priceHigher.benchmark !== null || events.profitAfterCosts.benchmark !== null
    || (events.benchmarkOutperformance.state === "available" && events.benchmarkOutperformance.benchmark === null)) {
    context.addIssue({ code: "custom", message: "Saved benchmark identity does not match its event." })
  }
  const ready = Object.values(events).filter((event) => event.state === "available")
  if (ready.some((event) => event.observedAt !== ready[0]?.observedAt || event.endsAt !== ready[0]?.endsAt)) {
    context.addIssue({ code: "custom", message: "Saved event probabilities do not share their original horizon." })
  }
})

export const investmentAnalysisSchema = z
  .object({
    actionToken: actionTokenSchema,
    investment: z
      .object({
        symbol: z.string().trim().min(1).max(64).nullable(),
        name: productTextSchema.nullable(),
      })
      .strict(),
    portfolioLabel: z.string().trim().min(1).max(128),
    currency: currencySchema,
    recommendation: recommendationSchema,
    horizon: horizonSchema,
    priceSummary: priceSummarySchema,
    chart: z.null(),
    chartAvailable: z.boolean(),
    probabilities: savedProbabilitiesSchema,
    reasons: z.array(productTextSchema).min(1).max(32),
    risks: z.array(productTextSchema).max(32),
    assumptions: z.array(productTextSchema).max(32),
    invalidators: z.array(productTextSchema).max(32),
    evidenceSummary: evidenceSummarySchema,
    analyticalEvidence: analyticalEvidenceSchema,
    liquidity: liquiditySchema,
    portfolioContext: portfolioContextSchema,
    virtualPaperEligibility: virtualPaperEligibilitySchema,
    outcomeProjection: outcomeProjectionSchema.nullable(),
    sizing: sizingSchema,
    expectedReturn: expectedReturnSchema,
    realizedOutcome: realizedOutcomeSchema.nullable(),
    trackRecordActionToken: actionTokenSchema.nullable(),
  })
  .strict()
  .superRefine((analysis, context) => {
    if (
      analysis.recommendation.kind !== "action" &&
      (analysis.priceSummary.actionRanges !== null ||
        analysis.outcomeProjection !== null ||
        analysis.sizing.state === "evaluated")
    ) {
      context.addIssue({
        code: "custom",
        message: "Only an action recommendation may include action projections.",
      })
    }
    if (
      analysis.recommendation.kind === "action" &&
      (analysis.priceSummary.scenarios === null ||
        analysis.priceSummary.actionRanges === null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["priceSummary"],
        message: "An investment action is missing its saved price ladder.",
      })
    }
    if (
      analysis.trackRecordActionToken !== null &&
      analysis.trackRecordActionToken !== analysis.actionToken
    ) {
      context.addIssue({
        code: "custom",
        path: ["trackRecordActionToken"],
        message: "The track-record action token does not belong to this analysis.",
      })
    }
    if (
      analysis.priceSummary.scenarios !== null &&
      analysis.priceSummary.scenarios.endsAt !== analysis.horizon.endsAt
    ) {
      context.addIssue({
        code: "custom",
        path: ["priceSummary", "scenarios", "endsAt"],
        message: "The forecast scenarios use a different investment horizon.",
      })
    }
    if (
      analysis.outcomeProjection !== null &&
      analysis.outcomeProjection.endsAt !== analysis.horizon.endsAt
    ) {
      context.addIssue({
        code: "custom",
        path: ["outcomeProjection", "endsAt"],
        message: "The outcome projection uses a different investment horizon.",
      })
    }
    if (analysis.sizing.state === "evaluated") {
      const sizing = analysis.sizing
      const amounts = [sizing.markedEquity, sizing.perLotNotional, sizing.perLotDownsideLoss,
        sizing.preferredWeightRounding.lowerRoundUpExcess, sizing.preferredWeightRounding.upperRoundDownRemainder,
        ...(sizing.settlementAvailableCash === null ? [] : [sizing.settlementAvailableCash]),
        ...[sizing.hardFeasibleTargetNotional, sizing.preferredFeasibleTargetNotional]
          .flatMap((range) => range.kind === "available" ? [range.lower, range.upper] : [])]
      if (amounts.some((amount) => amount.currency !== analysis.currency)
        || sizing.constraintCaps.some((cap, index) => cap.kind !== sizingKinds[index])
        || new Set(sizing.hardBindingCaps).size !== sizing.hardBindingCaps.length
        || new Set(sizing.preferredBindingCaps).size !== sizing.preferredBindingCaps.length) {
        context.addIssue({ code: "custom", message: "Saved sizing uses inconsistent currencies or constraints." })
      }
    }
    if (analysis.expectedReturn.state === "available"
      && (analysis.expectedReturn.exactRatio.numerator.currency !== analysis.currency
        || analysis.expectedReturn.exactRatio.denominator.currency !== analysis.currency)) {
      context.addIssue({ code: "custom", message: "Expected return uses a different reporting currency." })
    }
    if (analysis.portfolioContext.state === "available"
      && analysis.portfolioContext.portfolioLabel !== analysis.portfolioLabel) {
      context.addIssue({ code: "custom", path: ["portfolioContext", "portfolioLabel"],
        message: "The portfolio context belongs to a different portfolio." })
    }
    const evidenceFamilies = [
      ["current_market", analysis.analyticalEvidence.currentMarket],
      ["broader_research", analysis.analyticalEvidence.broaderResearch],
      ["price_pattern", analysis.analyticalEvidence.pricePattern],
      ["forecast", analysis.analyticalEvidence.forecast],
      ["financial_model", analysis.analyticalEvidence.financialModel],
      ["valuation", analysis.analyticalEvidence.valuation],
      ["historical_test", analysis.analyticalEvidence.historicalTest],
      ["out_of_sample", analysis.analyticalEvidence.outOfSample],
      ["liquidity", analysis.analyticalEvidence.liquidity],
      ["portfolio_risk", analysis.analyticalEvidence.portfolioRisk],
    ] as const
    evidenceFamilies.forEach(([kind, family], index) => {
      const coverage = analysis.evidenceSummary.coverage.items[index]
      if (coverage?.kind !== kind || coverage.state !== family.state) {
        context.addIssue({ code: "custom", path: ["analyticalEvidence"],
          message: "The analytical evidence families contradict evidence coverage." })
      }
    })
    const expectedCombination = analysis.recommendation.kind === "unavailable"
      ? "insufficient" : "multi_evidence"
    if (analysis.analyticalEvidence.combination.state !== expectedCombination) {
      context.addIssue({ code: "custom", path: ["analyticalEvidence", "combination", "state"],
        message: "The evidence-combination state contradicts the recommendation." })
    }
    const structuredAvailability = [
      [analysis.priceSummary.current, analysis.analyticalEvidence.currentMarket.state],
      [analysis.priceSummary.fairValue, analysis.analyticalEvidence.valuation.state],
      [analysis.priceSummary.scenarios, analysis.analyticalEvidence.forecast.state],
      [analysis.evidenceSummary.historicalTest, analysis.analyticalEvidence.historicalTest.state],
    ] as const
    if (structuredAvailability.some(([structured, state]) =>
      (structured === null ? "unavailable" : "available") !== state)
      || analysis.liquidity.state !== analysis.analyticalEvidence.liquidity.state
      || analysis.portfolioContext.state !== analysis.analyticalEvidence.portfolioRisk.state
      || analysis.evidenceSummary.outOfSample.state !== analysis.analyticalEvidence.outOfSample.state) {
      context.addIssue({ code: "custom", path: ["analyticalEvidence"],
        message: "The evidence-family summary contradicts its structured evidence." })
    }
    if (analysis.recommendation.kind === "action"
      && [analysis.analyticalEvidence.currentMarket,
        analysis.analyticalEvidence.forecast,
        analysis.analyticalEvidence.financialModel,
        analysis.analyticalEvidence.valuation,
        analysis.analyticalEvidence.historicalTest,
        analysis.analyticalEvidence.outOfSample,
        analysis.analyticalEvidence.liquidity,
        analysis.analyticalEvidence.portfolioRisk,
      ].some((family) => family.state !== "available")) {
      context.addIssue({ code: "custom", path: ["analyticalEvidence"],
        message: "An investment action is missing an independent required evidence family." })
    }
    const projection = analysis.outcomeProjection
    const scenarios = analysis.priceSummary.scenarios
    const actionRanges = analysis.priceSummary.actionRanges
    if (projection === null) {
      if (analysis.expectedReturn.state !== "unavailable") {
        context.addIssue({ code: "custom", path: ["expectedReturn"],
          message: "Expected return is available without a saved outcome projection." })
      }
    } else {
      const grossPricePnlStates = [projection.downside.grossPricePnl.state,
        projection.base.grossPricePnl.state, projection.upside.grossPricePnl.state]
      if ((projection.positionScale === null
        && grossPricePnlStates.some((state) => state === "available"))
        || (projection.positionScale !== null
          && grossPricePnlStates.some((state) => state !== "available"))
        || (projection.expectedGrossPricePnl.state === "available"
          && projection.positionScale === null)) {
        context.addIssue({ code: "custom", path: ["outcomeProjection", "positionScale"],
          message: "Gross profit-or-loss availability contradicts the exact position scale." })
      }
      const expected = analysis.expectedReturn
      const projected = projection.expectedReturn
      if (expected.state !== projected.state || (expected.state === "available"
        && projected.state === "available"
        && (expected.grossPriceReturnPercent !== projected.grossPriceReturnPercent
          || expected.exactRatio.numerator.amount !== projected.exactRatio.numerator.amount
          || expected.exactRatio.denominator.amount !== projected.exactRatio.denominator.amount
          || expected.exactRatio.numerator.currency !== projected.exactRatio.numerator.currency
          || expected.exactRatio.denominator.currency !== projected.exactRatio.denominator.currency))) {
        context.addIssue({ code: "custom", path: ["expectedReturn"],
          message: "Expected return contradicts the saved outcome projection." })
      }
      if (scenarios === null || actionRanges === null || analysis.priceSummary.current === null
        || !sameMoney(projection.startingPrice, analysis.priceSummary.current)
        || !sameRange(projection.downside.priceRange, scenarios.downside)
        || !sameRange(projection.base.priceRange, scenarios.base)
        || !sameRange(projection.upside.priceRange, scenarios.upside)
        || !sameRange(projection.entryDistance.priceRange, actionRanges.entry)
        || !sameRange(projection.addDistance.priceRange, actionRanges.add)
        || !sameRange(projection.trimDistance.priceRange, actionRanges.trim)
        || !sameRange(projection.exitDistance.priceRange, actionRanges.exit)) {
        context.addIssue({ code: "custom", path: ["outcomeProjection"],
          message: "The outcome projection contradicts its saved prices and action ranges." })
      }
    }
    if (
      actionRanges !== null &&
      (scenarios === null ||
        !strictlyIncreasingMoney([
          scenarios.downside.upper,
          actionRanges.exit.lower,
        ]) ||
        !strictlyIncreasingMoney([
          actionRanges.entry.upper,
          scenarios.base.lower,
        ]) ||
        !strictlyIncreasingMoney([
          scenarios.base.upper,
          actionRanges.trim.lower,
        ]) ||
        !strictlyIncreasingMoney([
          actionRanges.trim.upper,
          scenarios.upside.lower,
        ]))
    ) {
      context.addIssue({
        code: "custom",
        path: ["priceSummary", "actionRanges"],
        message: "The action ranges do not fit the saved forecast ladder.",
      })
    }
    if (
      analysisMoney(analysis).some(
        (value) => value.currency !== analysis.currency,
      )
    ) {
      context.addIssue({
        code: "custom",
        path: ["currency"],
        message: "The investment analysis mixes currencies.",
      })
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
      })
      .strict(),
  })
  .strict()

const investmentAnalysisLocatorSchema = z
  .object({
    actionToken: actionTokenSchema,
    investment: z
      .object({
        symbol: z.string().trim().min(1).max(64).nullable(),
        name: productTextSchema.nullable(),
      })
      .strict(),
    portfolioLabel: z.string().trim().min(1).max(128),
    currency: currencySchema,
    horizon: horizonSchema,
    recommendation: recommendationSchema,
  })
  .strict()

export const investmentAnalysisPageSchema = z
  .object({
    completeness: z.enum(["complete", "truncated"]),
    returnedCount: z.number().int().min(0).max(MAXIMUM_PAGE_ANALYSES),
    availableCount: z.number().int().min(0).max(MAXIMUM_AVAILABLE_ANALYSES),
    nextAfterActionToken: actionTokenSchema.nullable(),
    analyses: z.array(investmentAnalysisLocatorSchema).max(MAXIMUM_PAGE_ANALYSES),
  })
  .strict()
  .superRefine((page, context) => {
    const tokens = page.analyses.map((analysis) => analysis.actionToken)
    if (
      page.returnedCount !== page.analyses.length ||
      page.availableCount < page.returnedCount ||
      new Set(tokens).size !== tokens.length
    ) {
      context.addIssue({
        code: "custom",
        message: "The saved-analysis page is internally inconsistent.",
      })
    }
    if (
      page.completeness === "complete" &&
      (page.nextAfterActionToken !== null ||
        page.availableCount !== page.returnedCount)
    ) {
      context.addIssue({ code: "custom", message: "A complete page has a continuation." })
    }
    if (
      page.completeness === "truncated" &&
      (page.availableCount <= page.returnedCount ||
        page.nextAfterActionToken !== tokens.at(-1))
    ) {
      context.addIssue({
        code: "custom",
        message: "A truncated page does not retain its exact continuation token.",
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
        message: "Saved-analysis pagination metadata contradicts its product data.",
      })
    }
  })

const trackRecordActions = [
  "buy",
  "add",
  "hold",
  "trim",
  "sell",
  "abstain",
] as const

const trackRecordPerformanceSchema = z.union([
  z
    .object({
      kind: z.literal("unavailable"),
      summary: productTextSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      summary: productTextSchema,
      required: z.literal(MINIMUM_TRACK_RECORD_SAMPLES),
      actual: nonnegativeU32Schema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("unavailable"),
      summary: productTextSchema,
      requiredPercent: z.literal(MINIMUM_TRACK_RECORD_COVERAGE_PERCENT),
      actualPercent: percentageSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("available"),
      meanGrossPriceReturnPercent: canonicalDecimalSchema,
      positiveOutcomes: nonnegativeU32Schema,
      unchangedOutcomes: nonnegativeU32Schema,
      negativeOutcomes: nonnegativeU32Schema,
      summary: productTextSchema,
    })
    .strict(),
])

const trackRecordGroupSchema = z
  .object({
    action: z.enum(trackRecordActions),
    recommendationCount: nonnegativeU32Schema,
    dueCount: nonnegativeU32Schema,
    completedCount: nonnegativeU32Schema,
    pendingCount: nonnegativeU32Schema,
    unavailableCount: nonnegativeU32Schema,
    coveragePercent: percentageSchema,
    performance: trackRecordPerformanceSchema,
  })
  .strict()

export const recommendationTrackRecordSchema = z
  .object({
    actionToken: actionTokenSchema,
    evaluatedAt: canonicalRfc3339Schema,
    unavailableAnalysisCount: nonnegativeU32Schema,
    minimumCompletedSamples: z.literal(MINIMUM_TRACK_RECORD_SAMPLES),
    minimumCoveragePercent: z.literal(MINIMUM_TRACK_RECORD_COVERAGE_PERCENT),
    groups: z.array(trackRecordGroupSchema).length(trackRecordActions.length),
    forecastCalibrationIncluded: z.literal(false),
    executionResultsIncluded: z.literal(false),
    summary: productTextSchema,
  })
  .strict()
  .superRefine((record, context) => {
    record.groups.forEach((group, index) => {
      if (group.action !== trackRecordActions[index]) {
        context.addIssue({
          code: "custom",
          path: ["groups", index, "action"],
          message: "Comparable-history groups are not in canonical order.",
        })
      }
      if (
        group.recommendationCount !==
          group.completedCount + group.pendingCount + group.unavailableCount ||
        group.completedCount + group.unavailableCount > group.dueCount ||
        group.dueCount > group.recommendationCount
      ) {
        context.addIssue({
          code: "custom",
          path: ["groups", index],
          message: "Comparable-history counts are internally inconsistent.",
        })
      }
      const expectedCoverage = exactCoveragePercent(
        group.completedCount,
        group.dueCount,
      )
      if (group.coveragePercent !== expectedCoverage) {
        context.addIssue({
          code: "custom",
          path: ["groups", index, "coveragePercent"],
          message: "Comparable-history coverage is inconsistent with its outcomes.",
        })
      }
      const performance = group.performance
      const sampleGatePassed = group.completedCount >= MINIMUM_TRACK_RECORD_SAMPLES
      const coverageGatePassed =
        comparePositiveDecimals(
          group.coveragePercent,
          MINIMUM_TRACK_RECORD_COVERAGE_PERCENT,
        ) >= 0
      if (group.dueCount === 0) {
        if (
          performance.kind !== "unavailable" ||
          "required" in performance ||
          "requiredPercent" in performance
        ) {
          trackRecordGateIssue(context, index)
        }
      } else if (!sampleGatePassed) {
        if (
          performance.kind !== "unavailable" ||
          !("required" in performance) ||
          performance.actual !== group.completedCount
        ) {
          trackRecordGateIssue(context, index)
        }
      } else if (!coverageGatePassed) {
        if (
          performance.kind !== "unavailable" ||
          !("requiredPercent" in performance) ||
          performance.actualPercent !== group.coveragePercent
        ) {
          trackRecordGateIssue(context, index)
        }
      } else if (
        performance.kind !== "available" ||
        performance.positiveOutcomes +
          performance.unchangedOutcomes +
          performance.negativeOutcomes !==
          group.completedCount
      ) {
        trackRecordGateIssue(context, index)
      }
    })
  })

const recommendationTrackRecordEnvelopeSchema = z
  .object({
    data: recommendationTrackRecordSchema,
    metadata: z
      .object({
        completeness: z.literal("complete"),
        returnedItems: z.literal(trackRecordActions.length),
        availableItems: z.literal(trackRecordActions.length),
      })
      .strict(),
  })
  .strict()

export const savedScreenProductSchema = z
  .object({
    category: z.literal(productLookupCategory.savedScreen),
    title: productTextSchema,
    subtitle: productTextSchema,
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
      })
      .strict(),
  })
  .strict()

export type InvestmentChart = z.infer<typeof investmentChartSchema>

export type InvestmentAnalysis = z.infer<typeof investmentAnalysisSchema>
export type StudyQualification = z.infer<typeof studyQualificationSchema>
export type InvestmentAnalysisLocator = z.infer<
  typeof investmentAnalysisLocatorSchema
>
export type InvestmentAnalysisPage = z.infer<typeof investmentAnalysisPageSchema>
export type RecommendationTrackRecord = z.infer<
  typeof recommendationTrackRecordSchema
>
export type SavedScreenProduct = z.infer<typeof savedScreenProductSchema>

export function admittedAnalysisActionToken(value: string | null): string | null {
  if (value === null) return null
  const parsed = actionTokenSchema.safeParse(value)
  return parsed.success ? parsed.data : null
}

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
    throw new Error("This saved screen could not be opened.")
  }
  return parsed.data.data
}

export function parseInvestmentChart(result: ApplicationResult, expected: {
  startUnixNanos?: string; endUnixNanos?: string; pointLimit: number; layer: InvestmentChart["viewport"]["layer"]
}): InvestmentChart {
  const parsed = investmentChartSchema.safeParse(result.data)
  if (!parsed.success || result.metadata.returnedItems !== 1 || result.metadata.availableItems !== 1
    || result.metadata.completeness !== "complete"
    || parsed.data.viewport.startUnixNanos !== (expected.startUnixNanos ?? null)
    || parsed.data.viewport.endUnixNanos !== (expected.endUnixNanos ?? null)
    || parsed.data.viewport.pointLimit !== expected.pointLimit || parsed.data.viewport.layer !== expected.layer) {
    throw new Error("This saved chart could not be opened.")
  }
  return parsed.data
}

export function parseInvestmentAnalysis(
  result: ApplicationResult,
  expectedActionToken: string,
): InvestmentAnalysis {
  const parsed = investmentAnalysisEnvelopeSchema.safeParse(result)
  const expected = actionTokenSchema.safeParse(expectedActionToken)
  if (
    !parsed.success ||
    !expected.success ||
    parsed.data.data.actionToken !== expected.data
  ) {
    throw new Error("This investment analysis could not be opened.")
  }
  return parsed.data.data
}

export function parseInvestmentAnalysisPage(
  result: ApplicationResult,
  request: { afterActionToken?: string; limit: number },
): InvestmentAnalysisPage {
  const parsed = investmentAnalysisPageEnvelopeSchema.safeParse(result)
  const after = request.afterActionToken
    ? actionTokenSchema.safeParse(request.afterActionToken)
    : null
  if (
    !parsed.success ||
    !Number.isInteger(request.limit) ||
    request.limit < 1 ||
    request.limit > MAXIMUM_PAGE_ANALYSES ||
    (after !== null && !after.success)
  ) {
    throw new Error("Saved investment analyses could not be loaded.")
  }
  const page = parsed.data.data
  if (
    page.returnedCount > request.limit ||
    (page.completeness === "truncated" && page.returnedCount !== request.limit) ||
    (after?.success &&
      page.analyses.some(
        (analysis) => analysis.actionToken === after.data,
      ))
  ) {
    throw new Error("Saved investment analysis history could not be reconciled.")
  }
  return page
}

export function parseRecommendationTrackRecord(
  result: ApplicationResult,
  expectedActionToken: string,
): RecommendationTrackRecord {
  const parsed = recommendationTrackRecordEnvelopeSchema.safeParse(result)
  const token = actionTokenSchema.safeParse(expectedActionToken)
  if (
    !parsed.success ||
    !token.success ||
    parsed.data.data.actionToken !== token.data
  ) {
    throw new Error("Comparable history could not be opened for this analysis.")
  }
  return parsed.data.data
}
