import { z } from "zod"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.string().datetime({ offset: true }).max(64)
const outputTimestampSchema = timestampSchema.regex(
  /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$/,
)
const dateSchema = z.string().date().refine((value) => !value.startsWith("0000-"))
const monthSchema = z.string().regex(/^[0-9]{4}-(?:0[1-9]|1[0-2])$/)
  .refine((value) => dateSchema.safeParse(`${value}-01`).success)
const decimalSchema = z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)
const observedDecimalSchema = decimalSchema.regex(/^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/)
export const macroContextCutoffsSchema = z.union([
  z.object({ knowledgeCutoff: z.literal(""), effectiveDateCutoff: z.literal("") }).strict(),
  z
    .object({ knowledgeCutoff: timestampSchema, effectiveDateCutoff: dateSchema })
    .strict()
    .superRefine((cutoffs, refinement) => {
      const knowledgeDate = canonicalTimestamp(cutoffs.knowledgeCutoff)?.slice(0, 10) ?? null
      if (knowledgeDate === null || cutoffs.effectiveDateCutoff > knowledgeDate) {
        refinement.addIssue({
          code: "custom",
          path: ["effectiveDateCutoff"],
          message: "The effective date cannot follow the knowledge cutoff date.",
        })
      }
    }),
])
const confidenceSchema = z
  .object({
    level: z.enum(["moderate", "limited", "unavailable"]),
    summary: z.string().min(1).max(512),
  })
  .strict()
const recordedSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("known"), date: dateSchema }).strict(),
  z.object({ state: z.literal("not_supplied") }).strict(),
])
const valueSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("observed"), decimal: observedDecimalSchema }).strict(),
  z
    .object({
      state: z.literal("missing"),
      reason: z.enum(["not_reported", "unavailable"]),
      explanation: z.string().min(1).max(512),
    })
    .strict(),
])

const indicatorIds = [
  "us-government-yield-1m",
  "us-government-yield-3m",
  "us-government-yield-6m",
  "us-government-yield-1y",
  "us-government-yield-2y",
  "us-government-yield-3y",
  "us-government-yield-5y",
  "us-government-yield-7y",
  "us-government-yield-10y",
  "us-government-yield-20y",
  "us-government-yield-30y",
  "us-unemployment-rate",
  "us-residential-electricity-price",
] as const

const indicatorCount = indicatorIds.length

const observationSchema = z
  .object({
    indicatorId: z.enum(indicatorIds),
    label: z.string().min(1).max(128),
    category: z.enum(["interest_rates", "labor_market", "energy_prices"]),
    frequency: z.enum(["business_daily", "monthly"]),
    seasonalAdjustment: z.enum(["not_applicable", "seasonally_adjusted", "not_supplied"]),
    unit: z
      .object({
        code: z.enum(["percent_per_year", "percent_of_labor_force", "native_energy_price"]),
        label: z.string().min(1).max(32 * 1024),
        symbol: z.string().min(1).max(8).nullable(),
      })
      .strict(),
    effectiveDate: dateSchema.nullable(),
    effectivePeriod: monthSchema.optional(),
    recorded: recordedSchema,
    availableAt: outputTimestampSchema.nullable(),
    revision: z.number().int().positive().max(4_294_967_295).nullable(),
    supersededAfter: dateSchema.nullable(),
    value: valueSchema,
    availability: z.enum(["available", "missing", "unavailable"]),
    confidence: confidenceSchema,
  })
  .strict()

export const macroContextSchema = z
  .object({
    availability: z.enum(["available", "partial", "unavailable"]),
    selection: z
      .object({
        knowledgeCutoff: outputTimestampSchema,
        effectiveDateCutoff: dateSchema,
        effectiveMonthCutoff: monthSchema.nullable(),
        evaluatedAt: outputTimestampSchema,
        complete: z.boolean(),
      })
      .strict(),
    confidence: confidenceSchema,
    coverage: z
      .object({
        requested: z.literal(indicatorCount),
        observed: z.number().int().min(0).max(indicatorCount),
        missing: z.number().int().min(0).max(indicatorCount),
        unavailable: z.number().int().min(0).max(indicatorCount),
      })
      .strict(),
    observations: z.array(observationSchema).length(indicatorCount),
  })
  .strict()
  .superRefine((context, refinement) => {
    if (context.selection.effectiveMonthCutoff !== completedMonth(context.selection.effectiveDateCutoff)) {
      refinement.addIssue({
        code: "custom",
        path: ["selection", "effectiveMonthCutoff"],
        message: "The month cutoff must be the last complete month within the selected date.",
      })
    }
    context.observations.forEach((observation, index) => {
      if (observation.indicatorId !== indicatorIds[index]) {
        refinement.addIssue({
          code: "custom",
          path: ["observations", index, "indicatorId"],
          message: "Indicators must retain the canonical application order.",
        })
      }
      const energy = index === indicatorCount - 1
      const interestRate = index < 11
      const expectedCategory = index < 11 ? "interest_rates" : energy ? "energy_prices" : "labor_market"
      const expectedFrequency = index < 11 ? "business_daily" : "monthly"
      const expectedSeasonality = index < 11 ? "not_applicable" : energy ? "not_supplied" : "seasonally_adjusted"
      const expectedUnit = index < 11 ? "percent_per_year" : energy ? "native_energy_price" : "percent_of_labor_force"
      const expectedUnitLabel = interestRate ? "Percent per year" : "Percent of labor force"
      if (
        observation.category !== expectedCategory ||
        observation.frequency !== expectedFrequency ||
        observation.seasonalAdjustment !== expectedSeasonality ||
        observation.unit.code !== expectedUnit ||
        (energy ? observation.unit.symbol !== null :
          observation.unit.label !== expectedUnitLabel || observation.unit.symbol !== "%") ||
        (energy ? observation.effectiveDate !== null : observation.effectivePeriod !== undefined) ||
        (energy && observation.effectivePeriod !== undefined &&
          (context.selection.effectiveMonthCutoff === null ||
            observation.effectivePeriod > context.selection.effectiveMonthCutoff)) ||
        (energy && (observation.availability === "unavailable") !== (observation.effectivePeriod === undefined))
      ) {
        refinement.addIssue({
          code: "custom",
          path: ["observations", index],
          message: "Indicator semantics do not match the canonical application contract.",
        })
      }
      const expectedAvailability =
        observation.value.state === "observed"
          ? "available"
          : observation.value.reason === "not_reported"
            ? "missing"
            : "unavailable"
      if (observation.availability !== expectedAvailability) {
        refinement.addIssue({
          code: "custom",
          path: ["observations", index, "availability"],
          message: "Indicator availability and value state disagree.",
        })
      }
    })
    const counts = context.observations.reduce(
      (total, observation) => ({
        ...total,
        [observation.availability]: total[observation.availability] + 1,
      }),
      { available: 0, missing: 0, unavailable: 0 },
    )
    if (
      context.coverage.observed !== counts.available ||
      context.coverage.missing !== counts.missing ||
      context.coverage.unavailable !== counts.unavailable
    ) {
      refinement.addIssue({
        code: "custom",
        path: ["coverage"],
        message: "Coverage does not match the returned indicators.",
      })
    }
  })

export type MacroContextData = z.infer<typeof macroContextSchema>
export type MacroContextObservation = z.infer<typeof observationSchema>

export function parseMacroContext(
  result: ApplicationResult,
  requestedCutoffs?: { knowledgeCutoff: string; effectiveDateCutoff: string },
): MacroContextData {
  const application = applicationResultSchema.parse(result)
  if (
    application.metadata.completeness !== "complete" ||
    application.metadata.returnedItems !== indicatorCount ||
    application.metadata.availableItems !== indicatorCount
  ) {
    throw new Error("Economic context is incomplete.")
  }
  const context = macroContextSchema.parse(application.data)
  if (
    requestedCutoffs &&
    (context.selection.knowledgeCutoff !== canonicalTimestamp(requestedCutoffs.knowledgeCutoff) ||
      context.selection.effectiveDateCutoff !== requestedCutoffs.effectiveDateCutoff)
  ) {
    throw new Error("Economic context does not match the requested dates.")
  }
  return context
}

// Convert the whole second independently so Date never rounds away input nanoseconds.
function canonicalTimestamp(value: string): string | null {
  const match = /^(.*T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?(Z|[+-][0-9]{2}:[0-9]{2})$/.exec(value)
  if (!match || !timestampSchema.safeParse(value).success) return null
  const milliseconds = Date.parse(`${match[1]}${match[3]}`)
  if (!Number.isFinite(milliseconds)) return null
  const wholeSecond = new Date(milliseconds).toISOString().slice(0, 19)
  return `${wholeSecond}.${(match[2] ?? "").padEnd(9, "0")}Z`
}

function completedMonth(date: string): string | null {
  const year = Number(date.slice(0, 4))
  const month = Number(date.slice(5, 7))
  const nextDay = String(Number(date.slice(8, 10)) + 1).padStart(2, "0")
  if (!dateSchema.safeParse(`${date.slice(0, 7)}-${nextDay}`).success) return date.slice(0, 7)
  if (month > 1) return `${date.slice(0, 4)}-${String(month - 1).padStart(2, "0")}`
  return year > 1 ? `${String(year - 1).padStart(4, "0")}-12` : null
}
