import { z } from "zod"

import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.string().datetime({ offset: true })
const dateSchema = z.string().date()
const decimalSchema = z.string().regex(/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/)
export const macroContextCutoffsSchema = z.union([
  z.object({ knowledgeCutoff: z.literal(""), effectiveDateCutoff: z.literal("") }).strict(),
  z
    .object({ knowledgeCutoff: timestampSchema, effectiveDateCutoff: dateSchema })
    .strict()
    .superRefine((cutoffs, refinement) => {
      const milliseconds = Date.parse(cutoffs.knowledgeCutoff)
      const knowledgeDate = Number.isFinite(milliseconds)
        ? new Date(milliseconds).toISOString().slice(0, 10)
        : null
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
  z.object({ state: z.literal("observed"), decimal: decimalSchema }).strict(),
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
] as const

const observationSchema = z
  .object({
    indicatorId: z.enum(indicatorIds),
    label: z.string().min(1).max(128),
    category: z.enum(["interest_rates", "labor_market"]),
    frequency: z.enum(["business_daily", "monthly"]),
    seasonalAdjustment: z.enum(["not_applicable", "seasonally_adjusted"]),
    unit: z
      .object({
        code: z.enum(["percent_per_year", "percent_of_labor_force"]),
        label: z.string().min(1).max(64),
        symbol: z.string().min(1).max(8).nullable(),
      })
      .strict(),
    effectiveDate: dateSchema.nullable(),
    recorded: recordedSchema,
    availableAt: timestampSchema.nullable(),
    revision: z.number().int().positive().nullable(),
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
        knowledgeCutoff: timestampSchema,
        effectiveDateCutoff: dateSchema,
        evaluatedAt: timestampSchema,
        complete: z.boolean(),
      })
      .strict(),
    confidence: confidenceSchema,
    coverage: z
      .object({
        requested: z.literal(12),
        observed: z.number().int().min(0).max(12),
        missing: z.number().int().min(0).max(12),
        unavailable: z.number().int().min(0).max(12),
      })
      .strict(),
    observations: z.array(observationSchema).length(12),
  })
  .strict()
  .superRefine((context, refinement) => {
    context.observations.forEach((observation, index) => {
      if (observation.indicatorId !== indicatorIds[index]) {
        refinement.addIssue({
          code: "custom",
          path: ["observations", index, "indicatorId"],
          message: "Indicators must retain the canonical application order.",
        })
      }
      const expectedCategory = index < 11 ? "interest_rates" : "labor_market"
      const expectedFrequency = index < 11 ? "business_daily" : "monthly"
      const expectedSeasonality = index < 11 ? "not_applicable" : "seasonally_adjusted"
      const expectedUnit = index < 11 ? "percent_per_year" : "percent_of_labor_force"
      const expectedUnitLabel = index < 11 ? "Percent per year" : "Percent of labor force"
      if (
        observation.category !== expectedCategory ||
        observation.frequency !== expectedFrequency ||
        observation.seasonalAdjustment !== expectedSeasonality ||
        observation.unit.code !== expectedUnit ||
        observation.unit.label !== expectedUnitLabel ||
        observation.unit.symbol !== "%"
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
    application.metadata.returnedItems !== 12 ||
    application.metadata.availableItems !== 12
  ) {
    throw new Error("Economic context is incomplete.")
  }
  const context = macroContextSchema.parse(application.data)
  if (
    requestedCutoffs &&
    (context.selection.knowledgeCutoff !== requestedCutoffs.knowledgeCutoff ||
      context.selection.effectiveDateCutoff !== requestedCutoffs.effectiveDateCutoff)
  ) {
    throw new Error("Economic context does not match the requested dates.")
  }
  return context
}
