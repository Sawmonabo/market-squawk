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
const positiveIntegerSchema = canonicalIntegerSchema.refine(
  (value) => BigInt(value) > 0n,
  "Expected a positive integer.",
)
const currencySchema = z.string().regex(/^[A-Z]{3}$/)
const nonnegativeU32Schema = z.number().int().min(0).max(MAXIMUM_U32)
const positiveU32Schema = z.number().int().min(1).max(MAXIMUM_U32)
const productTextSchema = z.string().trim().min(1).max(2_048)
const savedScreenIdSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-z][a-z0-9._-]*$/)

const moneySchema = z
  .object({ amount: positiveDecimalSchema, currency: currencySchema })
  .strict()

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

const priceSummarySchema = z
  .object({
    current: moneySchema.nullable(),
    fairValue: moneySchema.nullable(),
    scenarios: scenarioRangesSchema.nullable(),
    actionRanges: actionRangesSchema.nullable(),
  })
  .strict()

const coverageKinds = [
  "current_market",
  "forecast",
  "valuation",
  "historical_test",
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

const uncertaintySchema = z.union([
  z
    .object({
      state: z.literal("available"),
      evidenceReliabilityPercent: percentageSchema,
      components: z
        .array(
          z
            .object({
              kind: z.enum(uncertaintyKinds),
              reliabilityPercent: percentageSchema,
            })
            .strict(),
        )
        .length(uncertaintyKinds.length),
      summary: productTextSchema,
    })
    .strict()
    .superRefine((value, context) => {
      if (
        value.components.some(
          (component, index) => component.kind !== uncertaintyKinds[index],
        )
      ) {
        context.addIssue({
          code: "custom",
          message: "Evidence-reliability components are not in canonical order.",
        })
      }
    }),
  z
    .object({
      state: z.literal("unavailable"),
      summary: productTextSchema,
    })
    .strict(),
])

const evidenceSummarySchema = z
  .object({
    coverage: coverageSchema,
    calibration: calibrationSchema,
    outOfSample: z
      .object({
        state: z.literal("not_established"),
        summary: productTextSchema,
      })
      .strict(),
    historicalTest: historicalTestSchema.nullable(),
    costs: costSummarySchema,
    uncertainty: uncertaintySchema,
  })
  .strict()

const priceChangeRangeSchema = z
  .object({
    priceRange: priceRangeSchema,
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
    downside: priceChangeRangeSchema,
    base: priceChangeRangeSchema,
    upside: priceChangeRangeSchema,
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
      lower: positiveIntegerSchema,
      upper: positiveIntegerSchema,
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
      reasons: z.array(productTextSchema).min(1).max(16),
    })
    .strict(),
])

const sizingSchema = z
  .object({
    evaluatedAt: canonicalRfc3339Schema,
    currentLots: nonnegativeIntegerSchema,
    hardFeasibleLots: lotRangeSchema,
    preferredFeasibleLots: lotRangeSchema,
    summary: productTextSchema,
  })
  .strict()

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
    values.push(
      analysis.outcomeProjection.startingPrice,
      ...rangeMoney(analysis.outcomeProjection.downside.priceRange),
      ...rangeMoney(analysis.outcomeProjection.base.priceRange),
      ...rangeMoney(analysis.outcomeProjection.upside.priceRange),
    )
  }
  const realized = analysis.realizedOutcome?.result
  if (realized?.kind === "completed") {
    values.push(realized.startMark, realized.endpointPrice)
  }
  return values
}

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
    reasons: z.array(productTextSchema).min(1).max(32),
    risks: z.array(productTextSchema).max(32),
    assumptions: z.array(productTextSchema).max(32),
    invalidators: z.array(productTextSchema).max(32),
    evidenceSummary: evidenceSummarySchema,
    outcomeProjection: outcomeProjectionSchema.nullable(),
    sizing: sizingSchema.nullable(),
    realizedOutcome: realizedOutcomeSchema.nullable(),
    trackRecordActionToken: actionTokenSchema.nullable(),
  })
  .strict()
  .superRefine((analysis, context) => {
    if (
      analysis.recommendation.kind !== "action" &&
      (analysis.priceSummary.actionRanges !== null ||
        analysis.outcomeProjection !== null ||
        analysis.sizing !== null)
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
    const scenarios = analysis.priceSummary.scenarios
    const actionRanges = analysis.priceSummary.actionRanges
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

export type InvestmentAnalysis = z.infer<typeof investmentAnalysisSchema>
export type InvestmentAnalysisLocator = z.infer<
  typeof investmentAnalysisLocatorSchema
>
export type InvestmentAnalysisPage = z.infer<typeof investmentAnalysisPageSchema>
export type RecommendationTrackRecord = z.infer<
  typeof recommendationTrackRecordSchema
>
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
    throw new Error("This saved screen could not be opened.")
  }
  return parsed.data.data
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
